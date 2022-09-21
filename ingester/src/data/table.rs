//! Table level data buffer structures.

use std::{collections::BTreeMap, sync::Arc};

use data_types::{DeletePredicate, PartitionKey, SequenceNumber, ShardId, TableId, Timestamp};
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use mutable_batch::MutableBatch;
use snafu::ResultExt;
use write_summary::ShardProgress;

use super::partition::{PartitionData, PartitionStatus, UnpersistedPartitionData};
use crate::lifecycle::LifecycleHandle;

/// Data of a Table in a given Namesapce that belongs to a given Shard
#[derive(Debug)]
pub(crate) struct TableData {
    table_id: TableId,
    table_name: Arc<str>,

    /// The catalog ID of the shard this table is being populated from.
    shard_id: ShardId,

    // the max sequence number for a tombstone associated with this table
    tombstone_max_sequence_number: Option<SequenceNumber>,
    // Map pf partition key to its data
    pub(super) partition_data: BTreeMap<PartitionKey, PartitionData>,
}

impl TableData {
    /// Initialize new table buffer
    pub fn new(
        table_id: TableId,
        table_name: &str,
        shard_id: ShardId,
        tombstone_max_sequence_number: Option<SequenceNumber>,
    ) -> Self {
        Self {
            table_id,
            table_name: table_name.into(),
            shard_id,
            tombstone_max_sequence_number,
            partition_data: Default::default(),
        }
    }

    /// Return parquet_max_sequence_number
    pub(super) fn parquet_max_sequence_number(&self) -> Option<SequenceNumber> {
        self.partition_data
            .values()
            .map(|p| p.max_persisted_sequence_number())
            .max()
            .flatten()
    }

    /// Return tombstone_max_sequence_number
    #[allow(dead_code)] // Used in tests
    pub(super) fn tombstone_max_sequence_number(&self) -> Option<SequenceNumber> {
        self.tombstone_max_sequence_number
    }

    // buffers the table write and returns true if the lifecycle manager indicates that
    // ingest should be paused.
    pub(super) async fn buffer_table_write(
        &mut self,
        sequence_number: SequenceNumber,
        batch: MutableBatch,
        partition_key: PartitionKey,
        catalog: &dyn Catalog,
        lifecycle_handle: &dyn LifecycleHandle,
    ) -> Result<bool, super::Error> {
        let partition_data = match self.partition_data.get_mut(&partition_key) {
            Some(p) => p,
            None => {
                self.insert_partition(partition_key.clone(), self.shard_id, catalog)
                    .await?;
                self.partition_data.get_mut(&partition_key).unwrap()
            }
        };

        // skip the write if it has already been persisted
        if let Some(max) = partition_data.max_persisted_sequence_number() {
            if max >= sequence_number {
                return Ok(false);
            }
        }

        let should_pause = lifecycle_handle.log_write(
            partition_data.id(),
            self.shard_id,
            sequence_number,
            batch.size(),
            batch.rows(),
        );
        partition_data.buffer_write(sequence_number, batch)?;

        Ok(should_pause)
    }

    pub(super) async fn buffer_delete(
        &mut self,
        predicate: &DeletePredicate,
        sequence_number: SequenceNumber,
        catalog: &dyn Catalog,
        executor: &Executor,
    ) -> Result<(), super::Error> {
        let min_time = Timestamp::new(predicate.range.start());
        let max_time = Timestamp::new(predicate.range.end());

        let mut repos = catalog.repositories().await;
        let tombstone = repos
            .tombstones()
            .create_or_get(
                self.table_id,
                self.shard_id,
                sequence_number,
                min_time,
                max_time,
                &predicate.expr_sql_string(),
            )
            .await
            .context(super::CatalogSnafu)?;

        // remember "persisted" state
        self.tombstone_max_sequence_number = Some(sequence_number);

        // modify one partition at a time
        for data in self.partition_data.values_mut() {
            data.buffer_tombstone(executor, tombstone.clone()).await;
        }

        Ok(())
    }

    pub(crate) fn unpersisted_partition_data(&self) -> Vec<UnpersistedPartitionData> {
        self.partition_data
            .values()
            .map(|p| UnpersistedPartitionData {
                partition_id: p.id(),
                non_persisted: p
                    .get_non_persisting_data()
                    .expect("get_non_persisting should always work"),
                persisting: p.get_persisting_data(),
                partition_status: PartitionStatus {
                    parquet_max_sequence_number: p.max_persisted_sequence_number(),
                    tombstone_max_sequence_number: self.tombstone_max_sequence_number,
                },
            })
            .collect()
    }

    async fn insert_partition(
        &mut self,
        partition_key: PartitionKey,
        shard_id: ShardId,
        catalog: &dyn Catalog,
    ) -> Result<(), super::Error> {
        let partition = catalog
            .repositories()
            .await
            .partitions()
            .create_or_get(partition_key, shard_id, self.table_id)
            .await
            .context(super::CatalogSnafu)?;

        self.partition_data.insert(
            partition.partition_key,
            PartitionData::new(
                partition.id,
                shard_id,
                self.table_id,
                Arc::clone(&self.table_name),
                partition.persisted_sequence_number,
            ),
        );

        Ok(())
    }

    /// Return progress from this Table
    pub(crate) fn progress(&self) -> ShardProgress {
        let progress = ShardProgress::new();
        let progress = match self.parquet_max_sequence_number() {
            Some(n) => progress.with_persisted(n),
            None => progress,
        };

        self.partition_data
            .values()
            .fold(progress, |progress, partition_data| {
                progress.combine(partition_data.progress())
            })
    }

    #[cfg(test)]
    pub(crate) fn table_id(&self) -> TableId {
        self.table_id
    }
}
