//! Data for the lifecycle of the Ingester

use std::{collections::BTreeMap, pin::Pin, sync::Arc};

use arrow::{error::ArrowError, record_batch::RecordBatch};
use arrow_util::optimize::{optimize_record_batch, optimize_schema};
use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{PartitionId, SequenceNumber, ShardId, ShardIndex};
use datafusion::physical_plan::SendableRecordBatchStream;
use dml::DmlOperation;
use futures::{Stream, StreamExt};
use iox_catalog::interface::{get_table_schema_by_id, Catalog};
use iox_query::exec::Executor;
use iox_time::SystemProvider;
use metric::{Attributes, Metric, U64Histogram, U64HistogramOptions};
use object_store::DynObjectStore;
use observability_deps::tracing::{debug, warn};
use parquet_file::storage::ParquetStorage;
use snafu::{OptionExt, Snafu};
use write_summary::ShardProgress;

use crate::{
    compact::{compact_persisting_batch, CompactedStream},
    lifecycle::LifecycleHandle,
};

pub mod namespace;
pub mod partition;
mod query_dedup;
pub mod shard;
pub mod table;

use self::{partition::PartitionStatus, shard::ShardData};

#[cfg(test)]
mod triggers;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Shard {} not found in data map", shard_id))]
    ShardNotFound { shard_id: ShardId },

    #[snafu(display("Namespace {} not found in catalog", namespace))]
    NamespaceNotFound { namespace: String },

    #[snafu(display("Table {} not found in buffer", table_name))]
    TableNotFound { table_name: String },

    #[snafu(display("Table must be specified in delete"))]
    TableNotPresent,

    #[snafu(display("Error accessing catalog: {}", source))]
    Catalog {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Snapshot error: {}", source))]
    Snapshot { source: mutable_batch::Error },

    #[snafu(display("Error while filtering columns from snapshot: {}", source))]
    FilterColumn { source: arrow::error::ArrowError },

    #[snafu(display("Error while copying buffer to snapshot: {}", source))]
    BufferToSnapshot { source: mutable_batch::Error },

    #[snafu(display("Error adding to buffer in mutable batch: {}", source))]
    BufferWrite { source: mutable_batch::Error },
}

/// A specialized `Error` for Ingester Data errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Contains all buffered and cached data for the ingester.
#[derive(Debug)]
pub struct IngesterData {
    /// Object store for persistence of parquet files
    store: ParquetStorage,

    /// The global catalog for schema, parquet files and tombstones
    catalog: Arc<dyn Catalog>,

    /// This map gets set up on initialization of the ingester so it won't ever be modified.
    /// The content of each ShardData will get changed when more namespaces and tables
    /// get ingested.
    shards: BTreeMap<ShardId, ShardData>,

    /// Executor for running queries and compacting and persisting
    exec: Arc<Executor>,

    /// Backoff config
    backoff_config: BackoffConfig,

    /// Metrics for file size of persisted Parquet files
    persisted_file_size_bytes: Metric<U64Histogram>,
}

impl IngesterData {
    /// Create new instance.
    pub fn new(
        object_store: Arc<DynObjectStore>,
        catalog: Arc<dyn Catalog>,
        shards: BTreeMap<ShardId, ShardData>,
        exec: Arc<Executor>,
        backoff_config: BackoffConfig,
        metrics: Arc<metric::Registry>,
    ) -> Self {
        let persisted_file_size_bytes = metrics.register_metric_with_options(
            "ingester_persisted_file_size_bytes",
            "Size of files persisted by the ingester",
            || {
                U64HistogramOptions::new([
                    500 * 1024,       // 500 KB
                    1024 * 1024,      // 1 MB
                    3 * 1024 * 1024,  // 3 MB
                    10 * 1024 * 1024, // 10 MB
                    30 * 1024 * 1024, // 30 MB
                    u64::MAX,         // Inf
                ])
            },
        );

        Self {
            store: ParquetStorage::new(object_store),
            catalog,
            shards,
            exec,
            backoff_config,
            persisted_file_size_bytes,
        }
    }

    /// Executor for running queries and compacting and persisting
    pub(crate) fn exec(&self) -> &Arc<Executor> {
        &self.exec
    }

    /// Get shard data for specific shard.
    #[allow(dead_code)] // Used in tests
    pub(crate) fn shard(&self, shard_id: ShardId) -> Option<&ShardData> {
        self.shards.get(&shard_id)
    }

    /// Get iterator over shards (ID and data).
    pub(crate) fn shards(&self) -> impl Iterator<Item = (&ShardId, &ShardData)> {
        self.shards.iter()
    }

    /// Store the write or delete in the in memory buffer. Deletes will
    /// be written into the catalog before getting stored in the buffer.
    /// Any writes that create new IOx partitions will have those records
    /// created in the catalog before putting into the buffer. Writes will
    /// get logged in the lifecycle manager. If it indicates ingest should
    /// be paused, this function will return true.
    pub async fn buffer_operation(
        &self,
        shard_id: ShardId,
        dml_operation: DmlOperation,
        lifecycle_handle: &dyn LifecycleHandle,
    ) -> Result<bool> {
        let shard_data = self
            .shards
            .get(&shard_id)
            .context(ShardNotFoundSnafu { shard_id })?;
        shard_data
            .buffer_operation(
                dml_operation,
                shard_id,
                &self.catalog,
                lifecycle_handle,
                &self.exec,
            )
            .await
    }

    /// Return the ingestion progress for the specified shards
    /// Returns an empty `ShardProgress` for any shards that this ingester doesn't know about.
    pub(crate) async fn progresses(
        &self,
        shard_indexes: Vec<ShardIndex>,
    ) -> BTreeMap<ShardIndex, ShardProgress> {
        let mut progresses = BTreeMap::new();
        for shard_index in shard_indexes {
            let shard_data = self
                .shards
                .iter()
                .map(|(_, shard_data)| shard_data)
                .find(|shard_data| shard_data.shard_index() == shard_index);

            let progress = match shard_data {
                Some(shard_data) => shard_data.progress().await,
                None => ShardProgress::new(), // don't know about this shard
            };

            progresses.insert(shard_index, progress);
        }
        progresses
    }
}

/// The Persister has a function to persist a given partition ID and to update the
/// associated shard's `min_unpersisted_sequence_number`.
#[async_trait]
pub trait Persister: Send + Sync + 'static {
    /// Persits the partition ID. Will retry forever until it succeeds.
    async fn persist(&self, partition_id: PartitionId);

    /// Updates the shard's `min_unpersisted_sequence_number` in the catalog.
    /// This number represents the minimum that might be unpersisted, which is the
    /// farthest back the ingester would need to read in the write buffer to ensure
    /// that all data would be correctly replayed on startup.
    async fn update_min_unpersisted_sequence_number(
        &self,
        shard_id: ShardId,
        sequence_number: SequenceNumber,
    );
}

#[async_trait]
impl Persister for IngesterData {
    async fn persist(&self, partition_id: PartitionId) {
        // lookup the partition_info from the catalog
        let partition_info = Backoff::new(&self.backoff_config)
            .retry_all_errors("get partition_info_by_id", || async {
                let mut repos = self.catalog.repositories().await;
                repos.partitions().partition_info_by_id(partition_id).await
            })
            .await
            .expect("retry forever");

        // lookup the state from the ingester data. If something isn't found, it's unexpected. Crash
        // so someone can take a look.
        let partition_info = partition_info
            .unwrap_or_else(|| panic!("partition {} not found in catalog", partition_id));
        let shard_data = self
            .shards
            .get(&partition_info.partition.shard_id)
            .unwrap_or_else(|| {
                panic!(
                    "shard state for {} not in ingester data",
                    partition_info.partition.shard_id
                )
            }); //{
        let namespace = shard_data
            .namespace(&partition_info.namespace_name)
            .unwrap_or_else(|| {
                panic!(
                    "namespace {} not in shard {} state",
                    partition_info.namespace_name, partition_info.partition.shard_id
                )
            });
        debug!(?partition_id, ?partition_info, "persisting partition");

        // lookup column IDs from catalog
        // TODO: this can be removed once the ingester uses column IDs internally as well
        let table_schema = Backoff::new(&self.backoff_config)
            .retry_all_errors("get table schema", || async {
                let mut repos = self.catalog.repositories().await;
                let table = repos
                    .tables()
                    .get_by_namespace_and_name(namespace.namespace_id(), &partition_info.table_name)
                    .await?
                    .expect("table not found in catalog");
                get_table_schema_by_id(table.id, repos.as_mut()).await
            })
            .await
            .expect("retry forever");

        let persisting_batch = namespace.snapshot_to_persisting(&partition_info).await;

        if let Some(persisting_batch) = persisting_batch {
            // do the CPU intensive work of compaction, de-duplication and sorting
            let compacted_stream = match compact_persisting_batch(
                Arc::new(SystemProvider::new()),
                &self.exec,
                namespace.namespace_id().get(),
                &partition_info,
                Arc::clone(&persisting_batch),
            )
            .await
            {
                Err(e) => {
                    // this should never error out. if it does, we need to crash hard so
                    // someone can take a look.
                    panic!("unable to compact persisting batch with error: {:?}", e);
                }
                Ok(Some(r)) => r,
                Ok(None) => {
                    warn!("persist called with no data");
                    return;
                }
            };
            let CompactedStream {
                stream: record_stream,
                iox_metadata,
                sort_key_update,
            } = compacted_stream;

            // Save the compacted data to a parquet file in object storage.
            //
            // This call retries until it completes.
            let (md, file_size) = self
                .store
                .upload(record_stream, &iox_metadata)
                .await
                .expect("unexpected fatal persist error");

            // Update the sort key in the catalog if there are
            // additional columns BEFORE adding parquet file to the
            // catalog. If the order is reversed, the querier or
            // compactor may see a parquet file with an inconsistent
            // sort key. https://github.com/influxdata/influxdb_iox/issues/5090
            if let Some(new_sort_key) = sort_key_update {
                let sort_key = new_sort_key.to_columns().collect::<Vec<_>>();
                Backoff::new(&self.backoff_config)
                    .retry_all_errors("update_sort_key", || async {
                        let mut repos = self.catalog.repositories().await;
                        let _partition = repos
                            .partitions()
                            .update_sort_key(partition_id, &sort_key)
                            .await?;
                        // compiler insisted on getting told the type of the error :shrug:
                        Ok(()) as Result<(), iox_catalog::interface::Error>
                    })
                    .await
                    .expect("retry forever");
                debug!(
                    ?partition_id,
                    table = partition_info.table_name,
                    ?new_sort_key,
                    "adjusted sort key during batch compact & persist"
                );
            }

            // Add the parquet file to the catalog until succeed
            let parquet_file = iox_metadata.to_parquet_file(partition_id, file_size, &md, |name| {
                table_schema.columns.get(name).expect("Unknown column").id
            });

            // Assert partitions are persisted in-order.
            //
            // It is an invariant that partitions are persisted in order so that
            // both the per-shard, and per-partition watermarks are correctly
            // advanced and accurate.
            if let Some(last_persist) = partition_info.partition.persisted_sequence_number {
                assert!(
                    parquet_file.max_sequence_number > last_persist,
                    "out of order partition persistence, persisting {}, previously persisted {}",
                    parquet_file.max_sequence_number.get(),
                    last_persist.get(),
                );
            }

            // Add the parquet file to the catalog.
            //
            // This has the effect of allowing the queriers to "discover" the
            // parquet file by polling / querying the catalog.
            Backoff::new(&self.backoff_config)
                .retry_all_errors("add parquet file to catalog", || async {
                    let mut repos = self.catalog.repositories().await;
                    let parquet_file = repos.parquet_files().create(parquet_file.clone()).await?;
                    debug!(
                        ?partition_id,
                        table_id=?parquet_file.table_id,
                        parquet_file_id=?parquet_file.id,
                        table_name=%iox_metadata.table_name,
                        "parquet file written to catalog"
                    );
                    // compiler insisted on getting told the type of the error :shrug:
                    Ok(()) as Result<(), iox_catalog::interface::Error>
                })
                .await
                .expect("retry forever");

            // Update the per-partition persistence watermark, so that new
            // ingester instances skip the just-persisted ops during replay.
            //
            // This could be transactional with the above parquet insert to
            // maintain catalog consistency, though in practice it is an
            // unnecessary overhead - the system can tolerate replaying the ops
            // that lead to this parquet file being generated, and tolerate
            // creating a parquet file containing duplicate data (remedied by
            // compaction).
            //
            // This means it is possible to observe a parquet file with a
            // max_persisted_sequence_number >
            // partition.persisted_sequence_number, either in-between these
            // catalog updates, or for however long it takes a crashed ingester
            // to restart and replay the ops, and re-persist a file containing
            // the same (or subset of) data.
            //
            // The above is also true of the per-shard persist marker that
            // governs the ingester's replay start point, which is
            // non-transactionally updated after all partitions have persisted.
            Backoff::new(&self.backoff_config)
                .retry_all_errors("set partition persist marker", || async {
                    self.catalog
                        .repositories()
                        .await
                        .partitions()
                        .update_persisted_sequence_number(
                            parquet_file.partition_id,
                            parquet_file.max_sequence_number,
                        )
                        .await
                })
                .await
                .expect("retry forever");

            // Record metrics
            let attributes = Attributes::from([(
                "shard_id",
                format!("{}", partition_info.partition.shard_id).into(),
            )]);
            self.persisted_file_size_bytes
                .recorder(attributes)
                .record(file_size as u64);

            // and remove the persisted data from memory
            namespace
                .mark_persisted(
                    &partition_info.table_name,
                    &partition_info.partition.partition_key,
                    iox_metadata.max_sequence_number,
                )
                .await;
            debug!(
                ?partition_id,
                table_name=%partition_info.table_name,
                partition_key=%partition_info.partition.partition_key,
                max_sequence_number=%iox_metadata.max_sequence_number.get(),
                "marked partition as persisted"
            );
        }
    }

    async fn update_min_unpersisted_sequence_number(
        &self,
        shard_id: ShardId,
        sequence_number: SequenceNumber,
    ) {
        Backoff::new(&self.backoff_config)
            .retry_all_errors("updating min_unpersisted_sequence_number", || async {
                self.catalog
                    .repositories()
                    .await
                    .shards()
                    .update_min_unpersisted_sequence_number(shard_id, sequence_number)
                    .await
            })
            .await
            .expect("retry forever")
    }
}

/// Stream of snapshots.
///
/// Every snapshot is a dedicated [`SendableRecordBatchStream`].
pub(crate) type SnapshotStream =
    Pin<Box<dyn Stream<Item = Result<SendableRecordBatchStream, ArrowError>> + Send>>;

/// Response data for a single partition.
pub(crate) struct IngesterQueryPartition {
    /// Stream of snapshots.
    snapshots: SnapshotStream,

    /// Partition ID.
    id: PartitionId,

    /// Partition persistence status.
    status: PartitionStatus,
}

impl std::fmt::Debug for IngesterQueryPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngesterQueryPartition")
            .field("snapshots", &"<SNAPSHOT STREAM>")
            .field("id", &self.id)
            .field("status", &self.status)
            .finish()
    }
}

impl IngesterQueryPartition {
    pub(crate) fn new(snapshots: SnapshotStream, id: PartitionId, status: PartitionStatus) -> Self {
        Self {
            snapshots,
            id,
            status,
        }
    }
}

/// Stream of partitions in this response.
pub(crate) type IngesterQueryPartitionStream =
    Pin<Box<dyn Stream<Item = Result<IngesterQueryPartition, ArrowError>> + Send>>;

/// Response streams for querier<>ingester requests.
///
/// The data structure is constructed to allow lazy/streaming data generation. For easier
/// consumption according to the wire protocol, use the [`flatten`](Self::flatten) method.
pub struct IngesterQueryResponse {
    /// Stream of partitions.
    partitions: IngesterQueryPartitionStream,
}

impl std::fmt::Debug for IngesterQueryResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngesterQueryResponse")
            .field("partitions", &"<PARTITION STREAM>")
            .finish()
    }
}

impl IngesterQueryResponse {
    /// Make a response
    pub(crate) fn new(partitions: IngesterQueryPartitionStream) -> Self {
        Self { partitions }
    }

    /// Flattens the data according to the wire protocol.
    pub fn flatten(self) -> FlatIngesterQueryResponseStream {
        self.partitions
            .flat_map(|partition_res| match partition_res {
                Ok(partition) => {
                    let head = futures::stream::once(async move {
                        Ok(FlatIngesterQueryResponse::StartPartition {
                            partition_id: partition.id,
                            status: partition.status,
                        })
                    });
                    let tail = partition
                        .snapshots
                        .flat_map(|snapshot_res| match snapshot_res {
                            Ok(snapshot) => {
                                let schema = Arc::new(optimize_schema(&snapshot.schema()));

                                let schema_captured = Arc::clone(&schema);
                                let head = futures::stream::once(async {
                                    Ok(FlatIngesterQueryResponse::StartSnapshot {
                                        schema: schema_captured,
                                    })
                                });

                                let tail = snapshot.map(move |batch_res| match batch_res {
                                    Ok(batch) => Ok(FlatIngesterQueryResponse::RecordBatch {
                                        batch: optimize_record_batch(&batch, Arc::clone(&schema))?,
                                    }),
                                    Err(e) => Err(e),
                                });

                                head.chain(tail).boxed()
                            }
                            Err(e) => futures::stream::once(async { Err(e) }).boxed(),
                        });

                    head.chain(tail).boxed()
                }
                Err(e) => futures::stream::once(async { Err(e) }).boxed(),
            })
            .boxed()
    }
}

/// Flattened version of [`IngesterQueryResponse`].
pub type FlatIngesterQueryResponseStream =
    Pin<Box<dyn Stream<Item = Result<FlatIngesterQueryResponse, ArrowError>> + Send>>;

/// Element within the flat wire protocol.
#[derive(Debug, PartialEq)]
pub enum FlatIngesterQueryResponse {
    /// Start a new partition.
    StartPartition {
        /// Partition ID.
        partition_id: PartitionId,

        /// Partition persistence status.
        status: PartitionStatus,
    },

    /// Start a new snapshot.
    ///
    /// The snapshot belongs to the partition of the last [`StartPartition`](Self::StartPartition)
    /// message.
    StartSnapshot {
        /// Snapshot schema.
        schema: Arc<arrow::datatypes::Schema>,
    },

    /// Add a record batch to the snapshot that was announced by the last
    /// [`StartSnapshot`](Self::StartSnapshot) message.
    RecordBatch {
        /// Record batch.
        batch: RecordBatch,
    },
}

#[cfg(test)]
mod tests {
    use std::{
        ops::DerefMut,
        task::{Context, Poll},
        time::Duration,
    };

    use arrow::datatypes::SchemaRef;
    use assert_matches::assert_matches;
    use data_types::{
        ColumnId, ColumnSet, CompactionLevel, DeletePredicate, NamespaceSchema, NonEmptyString,
        ParquetFileParams, Sequence, Timestamp, TimestampRange,
    };
    use datafusion::physical_plan::RecordBatchStream;
    use dml::{DmlDelete, DmlMeta, DmlWrite};
    use futures::TryStreamExt;
    use iox_catalog::{mem::MemCatalog, validate_or_insert_schema};
    use iox_time::Time;
    use metric::{MetricObserver, Observation};
    use mutable_batch_lp::{lines_to_batches, test_helpers::lp_to_mutable_batch};
    use object_store::memory::InMemory;
    use schema::selection::Selection;
    use uuid::Uuid;

    use super::*;
    use crate::{
        data::namespace::NamespaceData,
        lifecycle::{LifecycleConfig, LifecycleManager},
    };

    #[tokio::test]
    async fn buffer_write_updates_lifecycle_manager_indicates_pause() {
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("whatevs").await.unwrap();
        let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();
        let shard_index = ShardIndex::new(0);
        let namespace = repos
            .namespaces()
            .create("foo", "inf", topic.id, query_pool.id)
            .await
            .unwrap();
        let shard1 = repos
            .shards()
            .create_or_get(&topic, shard_index)
            .await
            .unwrap();

        let mut shards = BTreeMap::new();
        let shard_index = ShardIndex::new(0);
        shards.insert(shard1.id, ShardData::new(shard_index, Arc::clone(&metrics)));

        let object_store: Arc<DynObjectStore> = Arc::new(InMemory::new());

        let data = Arc::new(IngesterData::new(
            Arc::clone(&object_store),
            Arc::clone(&catalog),
            shards,
            Arc::new(Executor::new(1)),
            BackoffConfig::default(),
            Arc::clone(&metrics),
        ));

        let schema = NamespaceSchema::new(namespace.id, topic.id, query_pool.id);

        let ignored_ts = Time::from_timestamp_millis(42);

        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(1)),
                ignored_ts,
                None,
                50,
            ),
        );

        let _ = validate_or_insert_schema(w1.tables(), &schema, repos.deref_mut())
            .await
            .unwrap()
            .unwrap();

        std::mem::drop(repos);
        let pause_size = w1.size() + 1;
        let manager = LifecycleManager::new(
            LifecycleConfig::new(
                pause_size,
                0,
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                1000000,
            ),
            metrics,
            Arc::new(SystemProvider::new()),
        );
        let should_pause = data
            .buffer_operation(
                shard1.id,
                DmlOperation::Write(w1.clone()),
                &manager.handle(),
            )
            .await
            .unwrap();
        assert!(!should_pause);
        let should_pause = data
            .buffer_operation(shard1.id, DmlOperation::Write(w1), &manager.handle())
            .await
            .unwrap();
        assert!(should_pause);
    }

    #[tokio::test]
    async fn persist_row_count_trigger() {
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("whatevs").await.unwrap();
        let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();
        let shard_index = ShardIndex::new(0);
        let namespace = repos
            .namespaces()
            .create("foo", "inf", topic.id, query_pool.id)
            .await
            .unwrap();
        let shard1 = repos
            .shards()
            .create_or_get(&topic, shard_index)
            .await
            .unwrap();
        let mut shards = BTreeMap::new();
        shards.insert(
            shard1.id,
            ShardData::new(shard1.shard_index, Arc::clone(&metrics)),
        );

        let object_store: Arc<DynObjectStore> = Arc::new(InMemory::new());

        let data = Arc::new(IngesterData::new(
            Arc::clone(&object_store),
            Arc::clone(&catalog),
            shards,
            Arc::new(Executor::new(1)),
            BackoffConfig::default(),
            Arc::clone(&metrics),
        ));

        let schema = NamespaceSchema::new(namespace.id, topic.id, query_pool.id);

        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10\nmem foo=1 11", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(1)),
                Time::from_timestamp_millis(42),
                None,
                50,
            ),
        );
        let _schema = validate_or_insert_schema(w1.tables(), &schema, repos.deref_mut())
            .await
            .unwrap()
            .unwrap();

        // drop repos so the mem catalog won't deadlock.
        std::mem::drop(repos);

        let manager = LifecycleManager::new(
            LifecycleConfig::new(
                1000000000,
                0,
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                1, // This row count will be hit
            ),
            Arc::clone(&metrics),
            Arc::new(SystemProvider::new()),
        );

        let should_pause = data
            .buffer_operation(shard1.id, DmlOperation::Write(w1), &manager.handle())
            .await
            .unwrap();
        // Exceeding the row count doesn't pause ingest (like other partition
        // limits)
        assert!(!should_pause);

        let partition_id = {
            let sd = data.shards.get(&shard1.id).unwrap();
            let n = sd.namespace("foo").unwrap();
            let mem_table = n.table_data("mem").unwrap();
            assert!(n.table_data("mem").is_some());
            let mem_table = mem_table.write().await;
            let p = mem_table.partition_data.get(&"1970-01-01".into()).unwrap();
            p.id()
        };

        data.persist(partition_id).await;

        // verify that a file got put into object store
        let file_paths: Vec<_> = object_store
            .list(None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(file_paths.len(), 1);
    }

    #[tokio::test]
    async fn persist() {
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("whatevs").await.unwrap();
        let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();
        let shard_index = ShardIndex::new(0);
        let namespace = repos
            .namespaces()
            .create("foo", "inf", topic.id, query_pool.id)
            .await
            .unwrap();
        let shard1 = repos
            .shards()
            .create_or_get(&topic, shard_index)
            .await
            .unwrap();
        let shard2 = repos
            .shards()
            .create_or_get(&topic, shard_index)
            .await
            .unwrap();
        let mut shards = BTreeMap::new();
        shards.insert(
            shard1.id,
            ShardData::new(shard1.shard_index, Arc::clone(&metrics)),
        );
        shards.insert(
            shard2.id,
            ShardData::new(shard2.shard_index, Arc::clone(&metrics)),
        );

        let object_store: Arc<DynObjectStore> = Arc::new(InMemory::new());

        let data = Arc::new(IngesterData::new(
            Arc::clone(&object_store),
            Arc::clone(&catalog),
            shards,
            Arc::new(Executor::new(1)),
            BackoffConfig::default(),
            Arc::clone(&metrics),
        ));

        let schema = NamespaceSchema::new(namespace.id, topic.id, query_pool.id);

        let ignored_ts = Time::from_timestamp_millis(42);

        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(1)),
                ignored_ts,
                None,
                50,
            ),
        );
        let schema = validate_or_insert_schema(w1.tables(), &schema, repos.deref_mut())
            .await
            .unwrap()
            .unwrap();

        let w2 = DmlWrite::new(
            "foo",
            lines_to_batches("cpu foo=1 10", 1).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(2), SequenceNumber::new(1)),
                ignored_ts,
                None,
                50,
            ),
        );
        let _ = validate_or_insert_schema(w2.tables(), &schema, repos.deref_mut())
            .await
            .unwrap()
            .unwrap();

        // drop repos so the mem catalog won't deadlock.
        std::mem::drop(repos);
        let w3 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 30", 2).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(2)),
                ignored_ts,
                None,
                50,
            ),
        );

        let manager = LifecycleManager::new(
            LifecycleConfig::new(
                1,
                0,
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                1000000,
            ),
            Arc::clone(&metrics),
            Arc::new(SystemProvider::new()),
        );

        data.buffer_operation(shard1.id, DmlOperation::Write(w1), &manager.handle())
            .await
            .unwrap();
        data.buffer_operation(shard2.id, DmlOperation::Write(w2), &manager.handle())
            .await
            .unwrap();
        data.buffer_operation(shard1.id, DmlOperation::Write(w3), &manager.handle())
            .await
            .unwrap();

        let expected_progress = ShardProgress::new()
            .with_buffered(SequenceNumber::new(1))
            .with_buffered(SequenceNumber::new(2));
        assert_progress(&data, shard_index, expected_progress).await;

        let sd = data.shards.get(&shard1.id).unwrap();
        let n = sd.namespace("foo").unwrap();
        let partition_id;
        let table_id;
        {
            let mem_table = n.table_data("mem").unwrap();
            assert!(n.table_data("cpu").is_some());
            let mem_table = mem_table.write().await;
            let p = mem_table.partition_data.get(&"1970-01-01".into()).unwrap();

            table_id = mem_table.table_id();
            partition_id = p.id();
        }
        {
            // verify the partition doesn't have a sort key before any data has been persisted
            let mut repos = catalog.repositories().await;
            let partition_info = repos
                .partitions()
                .partition_info_by_id(partition_id)
                .await
                .unwrap()
                .unwrap();
            assert!(partition_info.partition.sort_key.is_empty());
        }

        data.persist(partition_id).await;

        // verify that a file got put into object store
        let file_paths: Vec<_> = object_store
            .list(None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(file_paths.len(), 1);

        let mut repos = catalog.repositories().await;
        // verify it put the record in the catalog
        let parquet_files = repos
            .parquet_files()
            .list_by_shard_greater_than(shard1.id, SequenceNumber::new(0))
            .await
            .unwrap();
        assert_eq!(parquet_files.len(), 1);
        let pf = parquet_files.first().unwrap();
        assert_eq!(pf.partition_id, partition_id);
        assert_eq!(pf.table_id, table_id);
        assert_eq!(pf.min_time, Timestamp::new(10));
        assert_eq!(pf.max_time, Timestamp::new(30));
        assert_eq!(pf.max_sequence_number, SequenceNumber::new(2));
        assert_eq!(pf.shard_id, shard1.id);
        assert!(pf.to_delete.is_none());

        // Verify the per-partition persist mark was updated to a value
        // inclusive of the persisted data, forming the exclusive lower-bound
        // from which a partition should start applying ops to ingest new data.
        let partition = repos
            .partitions()
            .get_by_id(partition_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            partition.persisted_sequence_number,
            Some(SequenceNumber::new(2))
        );

        // This value should be recorded in the metrics asserted next;
        // it is less than 500 KB
        //
        // note that since the file has metadata with timestamps
        // embedded in it, and those timestamps may compress slightly
        // different, the file may change slightly from time to time
        //
        // https://github.com/influxdata/influxdb_iox/issues/5434
        let expected_size = 1252;
        let allowable_delta = 10;
        let size_delta = (pf.file_size_bytes - expected_size).abs();
        assert!(
            size_delta < allowable_delta,
            "Unexpected parquet file size. Expected {} +/- {} bytes, got {}",
            expected_size,
            allowable_delta,
            pf.file_size_bytes
        );

        // verify metrics
        let persisted_file_size_bytes: Metric<U64Histogram> = metrics
            .get_instrument("ingester_persisted_file_size_bytes")
            .unwrap();

        let observation = persisted_file_size_bytes
            .get_observer(&Attributes::from([(
                "shard_id",
                format!("{}", shard1.id).into(),
            )]))
            .unwrap()
            .fetch();
        assert_eq!(observation.sample_count(), 1);
        let buckets_with_counts: Vec<_> = observation
            .buckets
            .iter()
            .filter_map(|o| if o.count == 0 { None } else { Some(o.le) })
            .collect();
        // Only the < 500 KB bucket has a count
        assert_eq!(buckets_with_counts, &[500 * 1024]);

        // verify it set a sort key on the partition in the catalog
        let partition_info = repos
            .partitions()
            .partition_info_by_id(partition_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(partition_info.partition.sort_key, vec!["time"]);

        let mem_table = n.table_data("mem").unwrap();
        let mem_table = mem_table.read().await;

        // verify that the parquet_max_sequence_number got updated
        assert_eq!(
            mem_table.parquet_max_sequence_number(),
            Some(SequenceNumber::new(2))
        );

        // check progresses after persist
        let expected_progress = ShardProgress::new()
            .with_buffered(SequenceNumber::new(1))
            .with_persisted(SequenceNumber::new(2));
        assert_progress(&data, shard_index, expected_progress).await;
    }

    #[tokio::test]
    async fn partial_write_progress() {
        test_helpers::maybe_start_logging();
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("whatevs").await.unwrap();
        let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();
        let shard_index = ShardIndex::new(0);
        let namespace = repos
            .namespaces()
            .create("foo", "inf", topic.id, query_pool.id)
            .await
            .unwrap();
        let shard1 = repos
            .shards()
            .create_or_get(&topic, shard_index)
            .await
            .unwrap();
        let shard2 = repos
            .shards()
            .create_or_get(&topic, shard_index)
            .await
            .unwrap();
        let mut shards = BTreeMap::new();
        shards.insert(
            shard1.id,
            ShardData::new(shard1.shard_index, Arc::clone(&metrics)),
        );
        shards.insert(
            shard2.id,
            ShardData::new(shard2.shard_index, Arc::clone(&metrics)),
        );

        let object_store: Arc<DynObjectStore> = Arc::new(InMemory::new());

        let data = Arc::new(IngesterData::new(
            Arc::clone(&object_store),
            Arc::clone(&catalog),
            shards,
            Arc::new(Executor::new(1)),
            BackoffConfig::default(),
            Arc::clone(&metrics),
        ));

        let schema = NamespaceSchema::new(namespace.id, topic.id, query_pool.id);

        let ignored_ts = Time::from_timestamp_millis(42);

        // write with sequence number 1
        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(1)),
                ignored_ts,
                None,
                50,
            ),
        );
        let _ = validate_or_insert_schema(w1.tables(), &schema, repos.deref_mut())
            .await
            .unwrap()
            .unwrap();

        // write with sequence number 2
        let w2 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 30\ncpu bar=1 20", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(2)),
                ignored_ts,
                None,
                50,
            ),
        );
        let _ = validate_or_insert_schema(w2.tables(), &schema, repos.deref_mut())
            .await
            .unwrap()
            .unwrap();

        drop(repos); // release catalog transaction

        let manager = LifecycleManager::new(
            LifecycleConfig::new(
                1,
                0,
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                1000000,
            ),
            metrics,
            Arc::new(SystemProvider::new()),
        );

        // buffer operation 1, expect progress buffered sequence number should be 1
        data.buffer_operation(shard1.id, DmlOperation::Write(w1), &manager.handle())
            .await
            .unwrap();

        // Get the namespace
        let sd = data.shards.get(&shard1.id).unwrap();
        let n = sd.namespace("foo").unwrap();

        let expected_progress = ShardProgress::new().with_buffered(SequenceNumber::new(1));
        assert_progress(&data, shard_index, expected_progress).await;

        // configure the the namespace to wait after each insert.
        n.test_triggers.enable_pause_after_write().await;

        // now, buffer operation 2 which has two tables,
        let captured_data = Arc::clone(&data);
        let task = tokio::task::spawn(async move {
            captured_data
                .buffer_operation(shard1.id, DmlOperation::Write(w2), &manager.handle())
                .await
                .unwrap();
        });

        n.test_triggers.wait_for_pause_after_write().await;

        // Check that while the write is only partially complete, the
        // buffered sequence number hasn't increased
        let expected_progress = ShardProgress::new()
            // sequence 2 hasn't been buffered yet
            .with_buffered(SequenceNumber::new(1));
        assert_progress(&data, shard_index, expected_progress).await;

        // allow the write to complete
        n.test_triggers.release_pause_after_write().await;
        task.await.expect("task completed unsuccessfully");

        // check progresses after the write completes
        let expected_progress = ShardProgress::new()
            .with_buffered(SequenceNumber::new(1))
            .with_buffered(SequenceNumber::new(2));
        assert_progress(&data, shard_index, expected_progress).await;
    }

    #[tokio::test]
    async fn buffer_operation_ignores_already_persisted_data() {
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("whatevs").await.unwrap();
        let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();
        let shard_index = ShardIndex::new(0);
        let namespace = repos
            .namespaces()
            .create("foo", "inf", topic.id, query_pool.id)
            .await
            .unwrap();
        let shard = repos
            .shards()
            .create_or_get(&topic, shard_index)
            .await
            .unwrap();

        let schema = NamespaceSchema::new(namespace.id, topic.id, query_pool.id);

        let ignored_ts = Time::from_timestamp_millis(42);

        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(1)),
                ignored_ts,
                None,
                50,
            ),
        );
        let w2 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(2)),
                ignored_ts,
                None,
                50,
            ),
        );

        let _ = validate_or_insert_schema(w1.tables(), &schema, repos.deref_mut())
            .await
            .unwrap()
            .unwrap();

        // create some persisted state
        let table = repos
            .tables()
            .create_or_get("mem", namespace.id)
            .await
            .unwrap();
        let partition = repos
            .partitions()
            .create_or_get("1970-01-01".into(), shard.id, table.id)
            .await
            .unwrap();
        repos
            .partitions()
            .update_persisted_sequence_number(partition.id, SequenceNumber::new(1))
            .await
            .unwrap();
        let partition2 = repos
            .partitions()
            .create_or_get("1970-01-02".into(), shard.id, table.id)
            .await
            .unwrap();

        let parquet_file_params = ParquetFileParams {
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: table.id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(1),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(1),
            file_size_bytes: 0,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };
        repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();

        // now create a parquet file in another partition with a much higher sequence persisted
        // sequence number. We want to make sure that this doesn't cause our write in the other
        // partition to get ignored.
        let other_file_params = ParquetFileParams {
            max_sequence_number: SequenceNumber::new(15),
            object_store_id: Uuid::new_v4(),
            partition_id: partition2.id,
            ..parquet_file_params
        };
        repos
            .parquet_files()
            .create(other_file_params)
            .await
            .unwrap();
        std::mem::drop(repos);

        let manager = LifecycleManager::new(
            LifecycleConfig::new(
                1,
                0,
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                1000000,
            ),
            Arc::clone(&metrics),
            Arc::new(SystemProvider::new()),
        );
        let exec = Executor::new(1);

        let data = NamespaceData::new(namespace.id, &*metrics);

        // w1 should be ignored because the per-partition replay offset is set
        // to 1 already, so it shouldn't be buffered and the buffer should
        // remain empty.
        let should_pause = data
            .buffer_operation(
                DmlOperation::Write(w1),
                shard.id,
                &catalog,
                &manager.handle(),
                &exec,
            )
            .await
            .unwrap();
        {
            let table_data = data.table_data("mem").unwrap();
            let table = table_data.read().await;
            let p = table.partition_data.get(&"1970-01-01".into()).unwrap();
            assert_eq!(
                p.max_persisted_sequence_number(),
                Some(SequenceNumber::new(1))
            );
            assert!(p.data.buffer.is_none());
        }
        assert!(!should_pause);

        // w2 should be in the buffer
        data.buffer_operation(
            DmlOperation::Write(w2),
            shard.id,
            &catalog,
            &manager.handle(),
            &exec,
        )
        .await
        .unwrap();

        let table_data = data.table_data("mem").unwrap();
        let table = table_data.read().await;
        let partition = table.partition_data.get(&"1970-01-01".into()).unwrap();
        assert_eq!(
            partition.data.buffer.as_ref().unwrap().min_sequence_number,
            SequenceNumber::new(2)
        );

        assert_matches!(data.table_count().observe(), Observation::U64Counter(v) => {
            assert_eq!(v, 1, "unexpected table count metric value");
        });
    }

    #[tokio::test]
    async fn buffer_deletes_updates_tombstone_watermark() {
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("whatevs").await.unwrap();
        let query_pool = repos.query_pools().create_or_get("whatevs").await.unwrap();
        let shard_index = ShardIndex::new(0);
        let namespace = repos
            .namespaces()
            .create("foo", "inf", topic.id, query_pool.id)
            .await
            .unwrap();
        let shard1 = repos
            .shards()
            .create_or_get(&topic, shard_index)
            .await
            .unwrap();

        let mut shards = BTreeMap::new();
        let shard_index = ShardIndex::new(0);
        shards.insert(shard1.id, ShardData::new(shard_index, Arc::clone(&metrics)));

        let object_store: Arc<DynObjectStore> = Arc::new(InMemory::new());

        let data = Arc::new(IngesterData::new(
            Arc::clone(&object_store),
            Arc::clone(&catalog),
            shards,
            Arc::new(Executor::new(1)),
            BackoffConfig::default(),
            Arc::clone(&metrics),
        ));

        let schema = NamespaceSchema::new(namespace.id, topic.id, query_pool.id);

        let ignored_ts = Time::from_timestamp_millis(42);

        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(1)),
                ignored_ts,
                None,
                50,
            ),
        );

        let _ = validate_or_insert_schema(w1.tables(), &schema, repos.deref_mut())
            .await
            .unwrap()
            .unwrap();

        std::mem::drop(repos);
        let pause_size = w1.size() + 1;
        let manager = LifecycleManager::new(
            LifecycleConfig::new(
                pause_size,
                0,
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                1000000,
            ),
            metrics,
            Arc::new(SystemProvider::new()),
        );
        data.buffer_operation(
            shard1.id,
            DmlOperation::Write(w1.clone()),
            &manager.handle(),
        )
        .await
        .unwrap();

        assert_eq!(
            data.shard(shard1.id)
                .unwrap()
                .namespace(&namespace.name)
                .unwrap()
                .table_data("mem")
                .unwrap()
                .read()
                .await
                .tombstone_max_sequence_number(),
            None,
        );

        let predicate = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };
        let d1 = DmlDelete::new(
            "foo",
            predicate,
            Some(NonEmptyString::new("mem").unwrap()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(1), SequenceNumber::new(2)),
                ignored_ts,
                None,
                1337,
            ),
        );
        data.buffer_operation(shard1.id, DmlOperation::Delete(d1), &manager.handle())
            .await
            .unwrap();

        assert_eq!(
            data.shard(shard1.id)
                .unwrap()
                .namespace(&namespace.name)
                .unwrap()
                .table_data("mem")
                .unwrap()
                .read()
                .await
                .tombstone_max_sequence_number(),
            Some(SequenceNumber::new(2)),
        );
    }

    /// Verifies that the progress in data is the same as expected_progress
    async fn assert_progress(
        data: &IngesterData,
        shard_index: ShardIndex,
        expected_progress: ShardProgress,
    ) {
        let progresses = data.progresses(vec![shard_index]).await;
        let expected_progresses = [(shard_index, expected_progress)]
            .into_iter()
            .collect::<BTreeMap<_, _>>();

        assert_eq!(progresses, expected_progresses);
    }

    #[tokio::test]
    async fn test_ingester_query_response_flatten() {
        let batch_1_1 = lp_to_batch("table x=1 0");
        let batch_1_2 = lp_to_batch("table x=2 1");
        let batch_2 = lp_to_batch("table y=1 10");
        let batch_3 = lp_to_batch("table z=1 10");

        let schema_1 = batch_1_1.schema();
        let schema_2 = batch_2.schema();
        let schema_3 = batch_3.schema();

        let response = IngesterQueryResponse::new(Box::pin(futures::stream::iter([
            Ok(IngesterQueryPartition::new(
                Box::pin(futures::stream::iter([
                    Ok(Box::pin(TestRecordBatchStream::new(
                        vec![
                            Ok(batch_1_1.clone()),
                            Err(ArrowError::NotYetImplemented("not yet implemeneted".into())),
                            Ok(batch_1_2.clone()),
                        ],
                        Arc::clone(&schema_1),
                    )) as _),
                    Err(ArrowError::InvalidArgumentError("invalid arg".into())),
                    Ok(Box::pin(TestRecordBatchStream::new(
                        vec![Ok(batch_2.clone())],
                        Arc::clone(&schema_2),
                    )) as _),
                    Ok(Box::pin(TestRecordBatchStream::new(vec![], Arc::clone(&schema_3))) as _),
                ])),
                PartitionId::new(2),
                PartitionStatus {
                    parquet_max_sequence_number: None,
                    tombstone_max_sequence_number: Some(SequenceNumber::new(1)),
                },
            )),
            Err(ArrowError::IoError("some io error".into())),
            Ok(IngesterQueryPartition::new(
                Box::pin(futures::stream::iter([])),
                PartitionId::new(1),
                PartitionStatus {
                    parquet_max_sequence_number: None,
                    tombstone_max_sequence_number: None,
                },
            )),
        ])));

        let actual: Vec<_> = response.flatten().collect().await;
        let expected = vec![
            Ok(FlatIngesterQueryResponse::StartPartition {
                partition_id: PartitionId::new(2),
                status: PartitionStatus {
                    parquet_max_sequence_number: None,
                    tombstone_max_sequence_number: Some(SequenceNumber::new(1)),
                },
            }),
            Ok(FlatIngesterQueryResponse::StartSnapshot { schema: schema_1 }),
            Ok(FlatIngesterQueryResponse::RecordBatch { batch: batch_1_1 }),
            Err(ArrowError::NotYetImplemented("not yet implemeneted".into())),
            Ok(FlatIngesterQueryResponse::RecordBatch { batch: batch_1_2 }),
            Err(ArrowError::InvalidArgumentError("invalid arg".into())),
            Ok(FlatIngesterQueryResponse::StartSnapshot { schema: schema_2 }),
            Ok(FlatIngesterQueryResponse::RecordBatch { batch: batch_2 }),
            Ok(FlatIngesterQueryResponse::StartSnapshot { schema: schema_3 }),
            Err(ArrowError::IoError("some io error".into())),
            Ok(FlatIngesterQueryResponse::StartPartition {
                partition_id: PartitionId::new(1),
                status: PartitionStatus {
                    parquet_max_sequence_number: None,
                    tombstone_max_sequence_number: None,
                },
            }),
        ];

        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.into_iter().zip(expected) {
            match (actual, expected) {
                (Ok(actual), Ok(expected)) => {
                    assert_eq!(actual, expected);
                }
                (Err(_), Err(_)) => {
                    // cannot compare `ArrowError`, but it's unlikely that someone changed the error
                }
                (Ok(_), Err(_)) => panic!("Actual is Ok but expected is Err"),
                (Err(_), Ok(_)) => panic!("Actual is Err but expected is Ok"),
            }
        }
    }

    fn lp_to_batch(lp: &str) -> RecordBatch {
        lp_to_mutable_batch(lp).1.to_arrow(Selection::All).unwrap()
    }

    pub struct TestRecordBatchStream {
        schema: SchemaRef,
        batches: Vec<Result<RecordBatch, ArrowError>>,
    }

    impl TestRecordBatchStream {
        pub fn new(batches: Vec<Result<RecordBatch, ArrowError>>, schema: SchemaRef) -> Self {
            Self { schema, batches }
        }
    }

    impl RecordBatchStream for TestRecordBatchStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    impl futures::Stream for TestRecordBatchStream {
        type Item = Result<RecordBatch, ArrowError>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            if self.batches.is_empty() {
                Poll::Ready(None)
            } else {
                Poll::Ready(Some(self.batches.remove(0)))
            }
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.batches.len(), Some(self.batches.len()))
        }
    }
}
