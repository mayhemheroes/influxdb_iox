//! Namespace level data buffer structures.

use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use data_types::{NamespaceId, PartitionKey, SequenceNumber, ShardId};
use dml::DmlOperation;
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use metric::U64Counter;
use parking_lot::RwLock;
use snafu::{OptionExt, ResultExt};
use write_summary::ShardProgress;

#[cfg(test)]
use super::triggers::TestTriggers;
use super::{partition::PersistingBatch, table::TableData};
use crate::lifecycle::LifecycleHandle;

/// Data of a Namespace that belongs to a given Shard
#[derive(Debug)]
pub struct NamespaceData {
    namespace_id: NamespaceId,

    /// The catalog ID of the shard this namespace is being populated from.
    shard_id: ShardId,

    tables: RwLock<BTreeMap<String, Arc<tokio::sync::RwLock<TableData>>>>,
    table_count: U64Counter,

    /// The sequence number being actively written, if any.
    ///
    /// This is used to know when a sequence number is only partially
    /// buffered for readability reporting. For example, in the
    /// following diagram a write for SequenceNumber 10 is only
    /// partially readable because it has been written into partitions
    /// A and B but not yet C. The max buffered number on each
    /// PartitionData is not sufficient to determine if the write is
    /// complete.
    ///
    /// ```text
    /// ╔═══════════════════════════════════════════════╗
    /// ║                                               ║   DML Operation (write)
    /// ║  ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━┓  ║   SequenceNumber = 10
    /// ║  ┃ Data for C  ┃ Data for B  ┃ Data for A  ┃  ║
    /// ║  ┗━━━━━━━━━━━━━┻━━━━━━━━━━━━━┻━━━━━━━━━━━━━┛  ║
    /// ║         │             │             │         ║
    /// ╚═══════════════════════╬═════════════╬═════════╝
    ///           │             │             │           ┌──────────────────────────────────┐
    ///                         │             │           │           Partition A            │
    ///           │             │             └──────────▶│        max buffered = 10         │
    ///                         │                         └──────────────────────────────────┘
    ///           │             │
    ///                         │                         ┌──────────────────────────────────┐
    ///           │             │                         │           Partition B            │
    ///                         └────────────────────────▶│        max buffered = 10         │
    ///           │                                       └──────────────────────────────────┘
    ///
    ///           │
    ///                                                   ┌──────────────────────────────────┐
    ///           │                                       │           Partition C            │
    ///            ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ▶│         max buffered = 7         │
    ///                                                   └──────────────────────────────────┘
    ///           Write is partially buffered. It has been
    ///            written to Partitions A and B, but not
    ///                  yet written to Partition C
    ///                                                               PartitionData
    ///                                                       (Ingester state per partition)
    ///```
    buffering_sequence_number: RwLock<Option<SequenceNumber>>,

    /// Control the flow of ingest, for testing purposes
    #[cfg(test)]
    pub(crate) test_triggers: TestTriggers,
}

impl NamespaceData {
    /// Initialize new tables with default partition template of daily
    pub fn new(namespace_id: NamespaceId, shard_id: ShardId, metrics: &metric::Registry) -> Self {
        let table_count = metrics
            .register_metric::<U64Counter>(
                "ingester_tables_total",
                "Number of tables known to the ingester",
            )
            .recorder(&[]);

        Self {
            namespace_id,
            shard_id,
            tables: Default::default(),
            table_count,
            buffering_sequence_number: RwLock::new(None),
            #[cfg(test)]
            test_triggers: TestTriggers::new(),
        }
    }

    /// Buffer the operation in the cache, adding any new partitions or delete tombstones to the
    /// catalog. Returns true if ingest should be paused due to memory limits set in the passed
    /// lifecycle manager.
    pub(super) async fn buffer_operation(
        &self,
        dml_operation: DmlOperation,
        catalog: &dyn Catalog,
        lifecycle_handle: &dyn LifecycleHandle,
        executor: &Executor,
    ) -> Result<bool, super::Error> {
        let sequence_number = dml_operation
            .meta()
            .sequence()
            .expect("must have sequence number")
            .sequence_number;

        // Note that this namespace is actively writing this sequence
        // number. Since there is no namespace wide lock held during a
        // write, this number is used to detect and update reported
        // progress during a write
        let _sequence_number_guard =
            ScopedSequenceNumber::new(sequence_number, &self.buffering_sequence_number);

        match dml_operation {
            DmlOperation::Write(write) => {
                let mut pause_writes = false;

                // Extract the partition key derived by the router.
                let partition_key = write
                    .partition_key()
                    .expect("no partition key in dml write")
                    .clone();

                for (t, b) in write.into_tables() {
                    let table_data = match self.table_data(&t) {
                        Some(t) => t,
                        None => self.insert_table(&t, catalog).await?,
                    };

                    {
                        // lock scope
                        let mut table_data = table_data.write().await;
                        let should_pause = table_data
                            .buffer_table_write(
                                sequence_number,
                                b,
                                partition_key.clone(),
                                catalog,
                                lifecycle_handle,
                            )
                            .await?;
                        pause_writes = pause_writes || should_pause;
                    }
                    #[cfg(test)]
                    self.test_triggers.on_write().await;
                }

                Ok(pause_writes)
            }
            DmlOperation::Delete(delete) => {
                let table_name = delete.table_name().context(super::TableNotPresentSnafu)?;
                let table_data = match self.table_data(table_name) {
                    Some(t) => t,
                    None => self.insert_table(table_name, catalog).await?,
                };

                let mut table_data = table_data.write().await;

                table_data
                    .buffer_delete(delete.predicate(), sequence_number, catalog, executor)
                    .await?;

                // don't pause writes since deletes don't count towards memory limits
                Ok(false)
            }
        }
    }

    /// Snapshots the mutable buffer for the partition, which clears it out and moves it over to
    /// snapshots. Then return a vec of the snapshots and the optional persisting batch.
    #[cfg(test)] // Only used in tests
    pub(crate) async fn snapshot(
        &self,
        table_name: &str,
        partition_key: &PartitionKey,
    ) -> Option<(
        Vec<Arc<super::partition::SnapshotBatch>>,
        Option<Arc<PersistingBatch>>,
    )> {
        if let Some(t) = self.table_data(table_name) {
            let mut t = t.write().await;

            return t.partition_data.get_mut(partition_key).map(|p| {
                p.data
                    .generate_snapshot()
                    .expect("snapshot on mutable batch should never fail");
                (p.data.snapshots.to_vec(), p.data.persisting.clone())
            });
        }

        None
    }

    /// Snapshots the mutable buffer for the partition, which clears it out and then moves all
    /// snapshots over to a persisting batch, which is returned. If there is no data to snapshot
    /// or persist, None will be returned.
    pub(crate) async fn snapshot_to_persisting(
        &self,
        table_name: &str,
        partition_key: &PartitionKey,
    ) -> Option<Arc<PersistingBatch>> {
        if let Some(table_data) = self.table_data(table_name) {
            let mut table_data = table_data.write().await;

            return table_data
                .partition_data
                .get_mut(partition_key)
                .and_then(|partition_data| partition_data.snapshot_to_persisting_batch());
        }

        None
    }

    /// Gets the buffered table data
    pub(crate) fn table_data(
        &self,
        table_name: &str,
    ) -> Option<Arc<tokio::sync::RwLock<TableData>>> {
        let t = self.tables.read();
        t.get(table_name).cloned()
    }

    /// Inserts the table or returns it if it happens to be inserted by some other thread
    async fn insert_table(
        &self,
        table_name: &str,
        catalog: &dyn Catalog,
    ) -> Result<Arc<tokio::sync::RwLock<TableData>>, super::Error> {
        let mut repos = catalog.repositories().await;
        let info = repos
            .tables()
            .get_table_persist_info(self.shard_id, self.namespace_id, table_name)
            .await
            .context(super::CatalogSnafu)?
            .context(super::TableNotFoundSnafu { table_name })?;

        let mut t = self.tables.write();

        let data = match t.entry(table_name.to_string()) {
            Entry::Vacant(v) => {
                let v = v.insert(Arc::new(tokio::sync::RwLock::new(TableData::new(
                    info.table_id,
                    table_name,
                    self.shard_id,
                    info.tombstone_max_sequence_number,
                ))));
                self.table_count.inc(1);
                Arc::clone(v)
            }
            Entry::Occupied(v) => Arc::clone(v.get()),
        };

        Ok(data)
    }

    /// Walks down the table and partition and clears the persisting batch. The sequence number is
    /// the max_sequence_number for the persisted parquet file, which should be kept in the table
    /// data buffer.
    pub(crate) async fn mark_persisted(
        &self,
        table_name: &str,
        partition_key: &PartitionKey,
        sequence_number: SequenceNumber,
    ) {
        if let Some(t) = self.table_data(table_name) {
            let mut t = t.write().await;
            let partition = t.partition_data.get_mut(partition_key);

            if let Some(p) = partition {
                p.mark_persisted(sequence_number);
            }
        }
    }

    /// Return progress from this Namespace
    pub(crate) async fn progress(&self) -> ShardProgress {
        let tables: Vec<_> = self.tables.read().values().map(Arc::clone).collect();

        // Consolidate progtress across partitions.
        let mut progress = ShardProgress::new()
            // Properly account for any sequence number that is
            // actively buffering and thus not yet completely
            // readable.
            .actively_buffering(*self.buffering_sequence_number.read());

        for table_data in tables {
            progress = progress.combine(table_data.read().await.progress())
        }
        progress
    }

    /// Return the [`NamespaceId`] this [`NamespaceData`] belongs to.
    pub(super) fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }

    #[cfg(test)]
    pub(super) fn table_count(&self) -> &U64Counter {
        &self.table_count
    }
}

/// RAAI struct that sets buffering sequence number on creation and clears it on free
struct ScopedSequenceNumber<'a> {
    sequence_number: SequenceNumber,
    buffering_sequence_number: &'a RwLock<Option<SequenceNumber>>,
}

impl<'a> ScopedSequenceNumber<'a> {
    fn new(
        sequence_number: SequenceNumber,
        buffering_sequence_number: &'a RwLock<Option<SequenceNumber>>,
    ) -> Self {
        *buffering_sequence_number.write() = Some(sequence_number);

        Self {
            sequence_number,
            buffering_sequence_number,
        }
    }
}

impl<'a> Drop for ScopedSequenceNumber<'a> {
    fn drop(&mut self) {
        // clear write on drop
        let mut buffering_sequence_number = self.buffering_sequence_number.write();
        assert_eq!(
            *buffering_sequence_number,
            Some(self.sequence_number),
            "multiple operations are being buffered concurrently"
        );
        *buffering_sequence_number = None;
    }
}
