//! Collect highest hot candidates and compact them

use crate::{
    compact::{self, Compactor},
    compact_candidates_with_memory_budget, compact_in_parallel,
    utils::get_candidates_with_retry,
    PartitionCompactionCandidateWithInfo,
};
use data_types::{CompactionLevel, PartitionParam, ShardId, Timestamp};
use iox_catalog::interface::Catalog;
use metric::Attributes;
use observability_deps::tracing::*;
use std::{sync::Arc, time::Duration};

/// Hot compaction. Returns the number of compacted partitions.
pub async fn compact(compactor: Arc<Compactor>) -> usize {
    let compaction_type = "hot";

    let candidates = get_candidates_with_retry(
        Arc::clone(&compactor),
        compaction_type,
        |compactor_for_retry| async move { hot_partitions_to_compact(compactor_for_retry).await },
    )
    .await;

    let n_candidates = candidates.len();
    if n_candidates == 0 {
        debug!(compaction_type, "no compaction candidates found");
        return 0;
    } else {
        debug!(n_candidates, compaction_type, "found compaction candidates");
    }

    let start_time = compactor.time_provider.now();

    compact_candidates_with_memory_budget(
        Arc::clone(&compactor),
        compaction_type,
        CompactionLevel::Initial,
        compact_in_parallel,
        true, // split
        candidates.into(),
    )
    .await;

    // Done compacting all candidates in the cycle, record its time
    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let attributes = Attributes::from(&[("partition_type", compaction_type)]);
        let duration = compactor.compaction_cycle_duration.recorder(attributes);
        duration.record(delta);
    }

    n_candidates
}

/// Return a list of the most recent highest ingested throughput partitions.
/// The highest throughput partitions are prioritized as follows:
///  1. If there are partitions with new ingested files within the last 4 hours, pick them.
///  2. If no new ingested files in the last 4 hours, will look for partitions with new writes
///     within the last 24 hours.
///  3. If there are no ingested files within the last 24 hours, will look for partitions
///     with any new ingested files in the past.
///
/// * New ingested files means non-deleted L0 files
/// * In all cases above, for each shard, N partitions with the most new ingested files
///   will be selected and the return list will include at most, P = N * S, partitions where S
///   is the number of shards this compactor handles.
async fn hot_partitions_to_compact(
    compactor: Arc<Compactor>,
) -> Result<Vec<Arc<PartitionCompactionCandidateWithInfo>>, compact::Error> {
    let compaction_type = "hot";

    let min_number_recent_ingested_files_per_partition = compactor
        .config
        .min_number_recent_ingested_files_per_partition;
    let max_number_partitions_per_shard = compactor.config.max_number_partitions_per_shard;

    let mut candidates =
        Vec::with_capacity(compactor.shards.len() * max_number_partitions_per_shard);

    // Get the most recent highest ingested throughput partitions within the last 4 hours. If not,
    // increase to 24 hours. Query using `now() - num_hours` in nanoseconds.
    let query_times = query_times(compactor.time_provider.now());

    for &shard_id in &compactor.shards {
        let mut partitions = hot_partitions_for_shard(
            Arc::clone(&compactor.catalog),
            shard_id,
            &query_times,
            max_number_partitions_per_shard,
            min_number_recent_ingested_files_per_partition,
        )
        .await?;

        // Record metric for candidates per shard
        let num_partitions = partitions.len();
        debug!(
            shard_id = shard_id.get(),
            n = num_partitions,
            compaction_type,
            "compaction candidates",
        );
        let attributes = Attributes::from([
            ("shard_id", format!("{}", shard_id).into()),
            ("partition_type", compaction_type.into()),
        ]);
        let number_gauge = compactor.compaction_candidate_gauge.recorder(attributes);
        number_gauge.set(num_partitions as u64);

        candidates.append(&mut partitions);
    }

    // Get extra needed information for selected partitions
    let start_time = compactor.time_provider.now();

    // Column types and their counts of the tables of the partition candidates
    debug!(
        num_candidates=?candidates.len(),
        compaction_type,
        "start getting column types for the partition candidates"
    );
    let table_columns = compactor.table_columns(&candidates).await?;

    // Add other compaction-needed info into selected partitions
    debug!(
        num_candidates=?candidates.len(),
        compaction_type,
        "start getting additional info for the partition candidates"
    );
    let candidates = compactor
        .add_info_to_partitions(&candidates, &table_columns)
        .await?;

    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let attributes = Attributes::from(&[("partition_type", compaction_type)]);
        let duration = compactor
            .partitions_extra_info_reading_duration
            .recorder(attributes);
        duration.record(delta);
    }

    Ok(candidates)
}

async fn hot_partitions_for_shard(
    catalog: Arc<dyn Catalog>,
    shard_id: ShardId,
    query_times: &[(u64, Timestamp)],
    // Minimum number of the most recent writes per partition we want to count
    // to prioritize partitions
    min_number_recent_ingested_files_per_partition: usize,
    // Max number of the most recent highest ingested throughput partitions
    // per shard we want to read
    max_number_partitions_per_shard: usize,
) -> Result<Vec<PartitionParam>, compact::Error> {
    let mut repos = catalog.repositories().await;

    for &(hours_ago, hours_ago_in_ns) in query_times {
        let partitions = repos
            .parquet_files()
            .recent_highest_throughput_partitions(
                shard_id,
                hours_ago_in_ns,
                min_number_recent_ingested_files_per_partition,
                max_number_partitions_per_shard,
            )
            .await
            .map_err(|e| compact::Error::HighestThroughputPartitions {
                shard_id,
                source: e,
            })?;
        if !partitions.is_empty() {
            debug!(
                shard_id = shard_id.get(),
                hours_ago,
                n = partitions.len(),
                "found high-throughput partitions"
            );
            return Ok(partitions);
        }
    }

    Ok(Vec::new())
}

fn query_times(now: iox_time::Time) -> Vec<(u64, Timestamp)> {
    [4, 24]
        .iter()
        .map(|&num_hours| {
            (
                num_hours,
                Timestamp::new((now - Duration::from_secs(60 * 60 * num_hours)).timestamp_nanos()),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compact::Compactor,
        compact_one_partition,
        handler::CompactorConfig,
        parquet_file_filtering, parquet_file_lookup,
        tests::{test_setup, TestSetup},
        ParquetFilesForCompaction,
    };
    use arrow_util::assert_batches_sorted_eq;
    use backoff::BackoffConfig;
    use data_types::{
        ColumnId, ColumnSet, ColumnType, CompactionLevel, ParquetFileId, ParquetFileParams,
        SequenceNumber, ShardIndex,
    };
    use iox_query::exec::Executor;
    use iox_tests::util::{TestCatalog, TestParquetFileBuilder};
    use iox_time::{SystemProvider, TimeProvider};
    use parquet_file::storage::ParquetStorage;
    use std::{collections::HashMap, sync::Arc, time::Duration};
    use uuid::Uuid;

    fn make_compactor_config() -> CompactorConfig {
        CompactorConfig {
            max_desired_file_size_bytes: 10_000,
            percentage_max_file_size: 30,
            split_percentage: 80,
            max_number_partitions_per_shard: 1,
            min_number_recent_ingested_files_per_partition: 1,
            hot_multiple: 4,
            memory_budget_bytes: 10 * 1024 * 1024,
        }
    }

    #[tokio::test]
    async fn test_hot_partitions_to_compact() {
        let catalog = TestCatalog::new();

        // Create a db with 2 shards, one with 4 empty partitions and the other one with one
        // empty partition
        let mut txn = catalog.catalog.start_transaction().await.unwrap();

        let topic = txn.topics().create_or_get("foo").await.unwrap();
        let pool = txn.query_pools().create_or_get("foo").await.unwrap();
        let namespace = txn
            .namespaces()
            .create(
                "namespace_hot_partitions_to_compact",
                "inf",
                topic.id,
                pool.id,
            )
            .await
            .unwrap();
        let table = txn
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let shard = txn
            .shards()
            .create_or_get(&topic, ShardIndex::new(1))
            .await
            .unwrap();
        let partition1 = txn
            .partitions()
            .create_or_get("one".into(), shard.id, table.id)
            .await
            .unwrap();
        let partition2 = txn
            .partitions()
            .create_or_get("two".into(), shard.id, table.id)
            .await
            .unwrap();
        let partition3 = txn
            .partitions()
            .create_or_get("three".into(), shard.id, table.id)
            .await
            .unwrap();
        let partition4 = txn
            .partitions()
            .create_or_get("four".into(), shard.id, table.id)
            .await
            .unwrap();
        // other shard
        let another_table = txn
            .tables()
            .create_or_get("another_test_table", namespace.id)
            .await
            .unwrap();
        let another_shard = txn
            .shards()
            .create_or_get(&topic, ShardIndex::new(2))
            .await
            .unwrap();
        let another_partition = txn
            .partitions()
            .create_or_get(
                "another_partition".into(),
                another_shard.id,
                another_table.id,
            )
            .await
            .unwrap();
        // update sort key for this another_partition
        let another_partition = txn
            .partitions()
            .update_sort_key(another_partition.id, &["tag1", "time"])
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // Create a compactor
        let time_provider = Arc::new(SystemProvider::new());
        let config = make_compactor_config();
        let compactor = Arc::new(Compactor::new(
            vec![shard.id, another_shard.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            time_provider,
            BackoffConfig::default(),
            config,
            Arc::new(metric::Registry::new()),
        ));

        // Some times in the past to set to created_at of the files
        let time_now = Timestamp::new(compactor.time_provider.now().timestamp_nanos());
        let time_three_minutes_ago = Timestamp::new(
            (compactor.time_provider.now() - Duration::from_secs(60 * 3)).timestamp_nanos(),
        );
        let time_five_hour_ago = Timestamp::new(
            (compactor.time_provider.now() - Duration::from_secs(60 * 60 * 5)).timestamp_nanos(),
        );
        let time_38_hour_ago = Timestamp::new(
            (compactor.time_provider.now() - Duration::from_secs(60 * 60 * 38)).timestamp_nanos(),
        );

        // Basic parquet info
        let p1 = ParquetFileParams {
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: table.id,
            partition_id: partition1.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(100),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(5),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial, // level of file of new writes
            created_at: time_now,
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };

        // Note: The order of the test cases below is important and should not be changed
        // because they depend on the order of the writes and their content. For example,
        // in order to test `Case 3`, we do not need to add asserts for `Case 1` and `Case 2`,
        // but all the writes, deletes and updates in Cases 1 and 2 are a must for testing Case 3.
        // In order words, the last Case needs all content of previous tests.
        // This shows the priority of selecting compaction candidates

        // --------------------------------------
        // Case 1: no files yet --> no partition candidates
        //
        let candidates = hot_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        assert!(candidates.is_empty());

        // --------------------------------------
        // Case 2: no non-deleleted L0 files -->  no partition candidates
        //
        // partition1 has a deleted L0
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let pf1 = txn.parquet_files().create(p1.clone()).await.unwrap();
        txn.parquet_files().flag_for_delete(pf1.id).await.unwrap();
        //
        // partition2 has a non-L0 file
        let p2 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition2.id,
            ..p1.clone()
        };
        let pf2 = txn.parquet_files().create(p2).await.unwrap();
        txn.parquet_files()
            .update_compaction_level(&[pf2.id], CompactionLevel::FileNonOverlapped)
            .await
            .unwrap();
        txn.commit().await.unwrap();
        // No non-deleted level 0 files yet --> no candidates
        let candidates = hot_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        assert!(candidates.is_empty());

        // --------------------------------------
        // Case 3: no new recent writes (within the last 24 hours) --> no partition candidates
        // (the cold case will pick them up)
        //
        // partition2 has an old (more than 8 hours ago) non-deleted level 0 file
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p3 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition2.id,
            created_at: time_38_hour_ago,
            ..p1.clone()
        };
        let _pf3 = txn.parquet_files().create(p3).await.unwrap();
        txn.commit().await.unwrap();

        // No hot candidates
        let candidates = hot_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        assert!(candidates.is_empty());

        // --------------------------------------
        // Case 4: has one partition with recent writes (5 hours ago) --> return that partition
        //
        // partition4 has a new write 5 hours ago
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p4 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition4.id,
            created_at: time_five_hour_ago,
            ..p1.clone()
        };
        let _pf4 = txn.parquet_files().create(p4).await.unwrap();
        txn.commit().await.unwrap();
        //
        // Has at least one partition with a recent write --> make it a candidate
        let candidates = hot_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].id(), partition4.id);

        // --------------------------------------
        // Case 5: has 2 partitions with 2 different groups of recent writes:
        //  1. Within the last 4 hours
        //  2. Within the last 24 hours but older than 4 hours ago
        // When we have group 1, we will ignore partitions in group 2
        //
        // partition3 has a new write 3 hours ago
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p5 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: partition3.id,
            created_at: time_three_minutes_ago,
            ..p1.clone()
        };
        let _pf5 = txn.parquet_files().create(p5).await.unwrap();
        txn.commit().await.unwrap();
        //
        // make partitions in the most recent group candidates
        let candidates = hot_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].id(), partition3.id);

        // --------------------------------------
        // Case 6: has partition candidates for 2 shards
        //
        // The another_shard now has non-deleted level-0 file ingested 5 hours ago
        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let p6 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            shard_id: another_shard.id,
            table_id: another_table.id,
            partition_id: another_partition.id,
            created_at: time_five_hour_ago,
            ..p1.clone()
        };
        let _pf6 = txn.parquet_files().create(p6).await.unwrap();
        txn.commit().await.unwrap();
        //
        // Will have 2 candidates, one for each shard
        let mut candidates = hot_partitions_to_compact(Arc::clone(&compactor))
            .await
            .unwrap();
        candidates.sort_by_key(|c| c.candidate);
        assert_eq!(candidates.len(), 2);

        assert_eq!(candidates[0].id(), partition3.id);
        assert_eq!(candidates[0].shard_id(), shard.id);
        assert_eq!(*candidates[0].namespace, namespace);
        assert_eq!(*candidates[0].table, table);
        assert_eq!(candidates[0].partition_key, partition3.partition_key);
        assert_eq!(candidates[0].sort_key, partition3.sort_key()); // this sort key is None

        assert_eq!(candidates[1].id(), another_partition.id);
        assert_eq!(candidates[1].shard_id(), another_shard.id);
        assert_eq!(*candidates[1].namespace, namespace);
        assert_eq!(*candidates[1].table, another_table);
        assert_eq!(candidates[1].partition_key, another_partition.partition_key);
        assert_eq!(candidates[1].sort_key, another_partition.sort_key()); // this sort key is Some(tag1, time)
    }

    #[tokio::test]
    async fn test_compact_hot_partition_candidates() {
        test_helpers::maybe_start_logging();

        let TestSetup {
            compactor,
            mock_compactor,
            shard,
            table,
            ..
        } = test_setup().await;

        // Some times in the past to set to created_at of the files
        let hot_time_one_hour_ago =
            (compactor.time_provider.now() - Duration::from_secs(60 * 60)).timestamp_nanos();

        // P1:
        //   L0 2 rows. bytes: 1125 * 2 = 2,250
        //   L1 2 rows. bytes: 1125 * 2 = 2,250
        // total = 2,250 + 2,250 = 4,500
        let partition1 = table.with_shard(&shard).create_partition("one").await;

        let pf1_1 = TestParquetFileBuilder::default()
            .with_min_time(1)
            .with_max_time(5)
            .with_row_count(2)
            .with_compaction_level(CompactionLevel::Initial)
            .with_creation_time(hot_time_one_hour_ago);
        partition1.create_parquet_file_catalog_record(pf1_1).await;

        let pf1_2 = TestParquetFileBuilder::default()
            .with_min_time(4) // overlapped with pf1_1
            .with_max_time(6)
            .with_row_count(2)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_creation_time(hot_time_one_hour_ago);
        partition1.create_parquet_file_catalog_record(pf1_2).await;

        // P2:
        //   L0 2 rows. bytes: 1125 * 2 = 2,250
        //   L1 2 rows. bytes: 1125 * 2 = 2,250
        // total = 2,250 + 2,250 = 4,500
        let partition2 = table.with_shard(&shard).create_partition("two").await;

        let pf2_1 = TestParquetFileBuilder::default()
            .with_min_time(1)
            .with_max_time(5)
            .with_row_count(2)
            .with_compaction_level(CompactionLevel::Initial)
            .with_creation_time(hot_time_one_hour_ago);
        partition2.create_parquet_file_catalog_record(pf2_1).await;

        let pf2_2 = TestParquetFileBuilder::default()
            .with_min_time(4) // overlapped with pf2_1
            .with_max_time(6)
            .with_row_count(2)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_creation_time(hot_time_one_hour_ago);
        partition2.create_parquet_file_catalog_record(pf2_2).await;

        // P3: bytes >= 90% of full budget = 90% * 13,500 = 12,150
        //   L0 6 rows. bytes: 1125 * 6 = 6,750
        //   L1 4 rows. bytes: 1125 * 4 = 4,500
        // total = 6,700 + 4,500 = 12,150
        let partition3 = table.with_shard(&shard).create_partition("three").await;
        let pf3_1 = TestParquetFileBuilder::default()
            .with_min_time(1)
            .with_max_time(5)
            .with_row_count(6)
            .with_compaction_level(CompactionLevel::Initial)
            .with_creation_time(hot_time_one_hour_ago);
        partition3.create_parquet_file_catalog_record(pf3_1).await;

        let pf3_2 = TestParquetFileBuilder::default()
            .with_min_time(4) // overlapped with pf3_1
            .with_max_time(6)
            .with_row_count(4)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_creation_time(hot_time_one_hour_ago);
        partition3.create_parquet_file_catalog_record(pf3_2).await;

        // P4: Over the full budget
        // L0 with 8 rows.bytes =  1125 * 8 = 9,000
        // L1 with 6 rows.bytes =  1125 * 6 = 6,750
        // total = 15,750
        let partition4 = table.with_shard(&shard).create_partition("four").await;
        let pf4_1 = TestParquetFileBuilder::default()
            .with_min_time(1)
            .with_max_time(5)
            .with_row_count(8)
            .with_compaction_level(CompactionLevel::Initial)
            .with_creation_time(hot_time_one_hour_ago);
        partition4.create_parquet_file_catalog_record(pf4_1).await;

        let pf4_2 = TestParquetFileBuilder::default()
            .with_min_time(4) // overlapped with pf4_1
            .with_max_time(6)
            .with_row_count(6)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_creation_time(hot_time_one_hour_ago);
        partition4.create_parquet_file_catalog_record(pf4_2).await;

        // P5:
        // L0 with 2 rows.bytes =  1125 * 2 = 2,250
        // L1 with 2 rows.bytes =  1125 * 2 = 2,250
        // total = 4,500
        let partition5 = table.with_shard(&shard).create_partition("five").await;
        let pf5_1 = TestParquetFileBuilder::default()
            .with_min_time(1)
            .with_max_time(5)
            .with_row_count(2)
            .with_compaction_level(CompactionLevel::Initial)
            .with_creation_time(hot_time_one_hour_ago);
        partition5.create_parquet_file_catalog_record(pf5_1).await;

        let pf5_2 = TestParquetFileBuilder::default()
            .with_min_time(4) // overlapped with pf5_1
            .with_max_time(6)
            .with_row_count(2)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_creation_time(hot_time_one_hour_ago);
        partition5.create_parquet_file_catalog_record(pf5_2).await;

        // P6:
        // L0 with 2 rows.bytes =  1125 * 2 = 2,250
        // L1 with 2 rows.bytes =  1125 * 2 = 2,250
        // total = 4,500
        let partition6 = table.with_shard(&shard).create_partition("six").await;
        let pf6_1 = TestParquetFileBuilder::default()
            .with_min_time(1)
            .with_max_time(5)
            .with_row_count(2)
            .with_compaction_level(CompactionLevel::Initial)
            .with_creation_time(hot_time_one_hour_ago);
        partition6.create_parquet_file_catalog_record(pf6_1).await;

        let pf6_2 = TestParquetFileBuilder::default()
            .with_min_time(4) // overlapped with pf6_1
            .with_max_time(6)
            .with_row_count(2)
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
            .with_creation_time(hot_time_one_hour_ago);
        partition6.create_parquet_file_catalog_record(pf6_2).await;

        let query_times = query_times(compactor.catalog.time_provider().now());

        // partition candidates: partitions with L0 and overlapped L1
        let mut candidates = hot_partitions_for_shard(
            Arc::clone(&compactor.catalog),
            shard.shard.id,
            &query_times,
            1,
            100,
        )
        .await
        .unwrap();
        assert_eq!(candidates.len(), 6);
        candidates.sort_by_key(|c| c.partition_id);
        {
            let mut repos = compactor.catalog.repositories().await;
            let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
            assert!(
                skipped_compactions.is_empty(),
                "Expected no skipped compactions, got: {skipped_compactions:?}"
            );
        }

        let table_columns = compactor.table_columns(&candidates).await.unwrap();
        let candidates = compactor
            .add_info_to_partitions(&candidates, &table_columns)
            .await
            .unwrap();

        // There are 3 rounds of parallel compaction:
        //
        // * Round 1: 3 candidates [P1, P2, P5] and total needed budget 13,500
        // * Round 2: 1 candidate [P6] and total needed budget 4,500
        // * Round 3: 1 candidate [P3] and total needed budget 11,250
        //
        // P4 is not compacted due to overbudget.
        compact_candidates_with_memory_budget(
            Arc::clone(&compactor),
            "hot",
            CompactionLevel::Initial,
            mock_compactor.compaction_function(),
            true,
            candidates.into(),
        )
        .await;

        let compaction_groups = mock_compactor.results();

        // 3 rounds of parallel compaction
        assert_eq!(compaction_groups.len(), 3);

        // Round 1
        let group1 = &compaction_groups[0];
        assert_eq!(group1.len(), 3);

        let g1_candidate1 = &group1[0];
        assert_eq!(g1_candidate1.partition.id(), partition1.partition.id);
        let g1_candidate1_pf_ids: Vec<_> =
            g1_candidate1.files.iter().map(|pf| pf.id().get()).collect();
        assert_eq!(g1_candidate1_pf_ids, vec![2, 1]);

        let g1_candidate2 = &group1[1];
        assert_eq!(g1_candidate2.partition.id(), partition2.partition.id);
        let g1_candidate2_pf_ids: Vec<_> =
            g1_candidate2.files.iter().map(|pf| pf.id().get()).collect();
        assert_eq!(g1_candidate2_pf_ids, vec![4, 3]);

        let g1_candidate3 = &group1[2];
        assert_eq!(g1_candidate3.partition.id(), partition5.partition.id);
        let g1_candidate3_pf_ids: Vec<_> =
            g1_candidate3.files.iter().map(|pf| pf.id().get()).collect();
        assert_eq!(g1_candidate3_pf_ids, vec![10, 9]);

        // Round 2
        let group2 = &compaction_groups[1];
        assert_eq!(group2.len(), 1);

        let g2_candidate1 = &group2[0];
        assert_eq!(g2_candidate1.partition.id(), partition6.partition.id);
        let g2_candidate1_pf_ids: Vec<_> =
            g2_candidate1.files.iter().map(|pf| pf.id().get()).collect();
        assert_eq!(g2_candidate1_pf_ids, vec![12, 11]);

        // Round 3
        let group3 = &compaction_groups[2];
        assert_eq!(group3.len(), 1);

        let g3_candidate1 = &group3[0];
        assert_eq!(g3_candidate1.partition.id(), partition3.partition.id);
        let g3_candidate1_pf_ids: Vec<_> =
            g3_candidate1.files.iter().map(|pf| pf.id().get()).collect();
        assert_eq!(g3_candidate1_pf_ids, vec![6, 5]);

        {
            let mut repos = compactor.catalog.repositories().await;
            let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
            assert_eq!(skipped_compactions.len(), 1);
            assert_eq!(skipped_compactions[0].partition_id, partition4.partition.id);
            assert_eq!(
                skipped_compactions[0].reason,
                "over memory budget. Needed budget = 15750, memory budget = 13500"
            );
        }
    }

    // A quite sophisticated integration test
    // Beside lp data, every value min/max sequence numbers and min/max time are important
    // to have a combination of needed tests in this test function
    #[tokio::test]
    async fn test_compact_hot_partition_many_files() {
        test_helpers::maybe_start_logging();
        let catalog = TestCatalog::new();

        // lp1 does not overlap with any other level 0
        let lp1 = vec![
            "table,tag1=WA field_int=1000i 10",
            "table,tag1=VT field_int=10i 20",
        ]
        .join("\n");

        // lp2 overlaps with lp3
        let lp2 = vec![
            "table,tag1=WA field_int=1000i 8000", // will be eliminated due to duplicate
            "table,tag1=VT field_int=10i 10000",
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");

        // lp3 overlaps with lp2
        let lp3 = vec![
            "table,tag1=WA field_int=1500i 8000", // latest duplicate and kept
            "table,tag1=VT field_int=10i 6000",
            "table,tag1=UT field_int=270i 25000",
        ]
        .join("\n");

        // lp4 does not overlap with any
        let lp4 = vec![
            "table,tag2=WA,tag3=10 field_int=1600i 28000",
            "table,tag2=VT,tag3=20 field_int=20i 26000",
        ]
        .join("\n");

        // lp5 overlaps with lp1
        let lp5 = vec![
            "table,tag2=PA,tag3=15 field_int=1601i 9",
            "table,tag2=OH,tag3=21 field_int=21i 25",
        ]
        .join("\n");

        // lp6 does not overlap with any
        let lp6 = vec![
            "table,tag2=PA,tag3=15 field_int=81601i 90000",
            "table,tag2=OH,tag3=21 field_int=421i 91000",
        ]
        .join("\n");

        let ns = catalog.create_namespace("ns").await;
        let shard = ns.create_shard(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;

        let partition = table.with_shard(&shard).create_partition("part").await;
        let time = Arc::new(SystemProvider::new());
        let config = CompactorConfig {
            max_desired_file_size_bytes: 10_000,
            percentage_max_file_size: 30,
            split_percentage: 80,
            max_number_partitions_per_shard: 1,
            min_number_recent_ingested_files_per_partition: 1,
            hot_multiple: 4,
            memory_budget_bytes: 100_000_000,
        };

        let metrics = Arc::new(metric::Registry::new());
        let compactor = Arc::new(Compactor::new(
            vec![shard.shard.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        ));

        // parquet files that are all in the same partition
        let mut size_overrides = HashMap::<ParquetFileId, i64>::default();

        // pf1 does not overlap with any other level 0
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_max_seq(3)
            .with_min_time(10)
            .with_max_time(20)
            .with_creation_time(20);
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(
            f.parquet_file.id,
            compactor.config.max_desired_file_size_bytes as i64 + 10,
        );

        // pf2 overlaps with pf3
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp2)
            .with_max_seq(5)
            .with_min_time(8_000)
            .with_max_time(20_000)
            .with_creation_time(time.now().timestamp_nanos());
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(f.parquet_file.id, 100); // small file

        // pf3 overlaps with pf2
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp3)
            .with_max_seq(10)
            .with_min_time(6_000)
            .with_max_time(25_000)
            .with_creation_time(time.now().timestamp_nanos());
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(f.parquet_file.id, 100); // small file

        // pf4 does not overlap with any but is small
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp4)
            .with_max_seq(18)
            .with_min_time(26_000)
            .with_max_time(28_000)
            .with_creation_time(time.now().timestamp_nanos());
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(f.parquet_file.id, 100); // small file

        // pf5 was created in a previous compaction cycle; overlaps with pf1
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp5)
            .with_max_seq(1)
            .with_min_time(9)
            .with_max_time(25)
            .with_creation_time(time.now().timestamp_nanos())
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(f.parquet_file.id, 100); // small file

        // pf6 was created in a previous compaction cycle; does not overlap with any
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp6)
            .with_max_seq(20)
            .with_min_time(90000)
            .with_max_time(91000)
            .with_creation_time(time.now().timestamp_nanos())
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let f = partition.create_parquet_file(builder).await;
        size_overrides.insert(f.parquet_file.id, 100); // small file

        // should have 4 level-0 files before compacting
        let count = catalog.count_level_0_files(shard.shard.id).await;
        assert_eq!(count, 4);

        let query_times = query_times(compactor.catalog.time_provider().now());

        // ------------------------------------------------
        // Compact
        let partition_candidates = hot_partitions_for_shard(
            Arc::clone(&compactor.catalog),
            shard.shard.id,
            &query_times,
            1,
            1,
        )
        .await
        .unwrap();

        assert_eq!(partition_candidates.len(), 1);

        let table_columns = compactor
            .table_columns(&partition_candidates)
            .await
            .unwrap();
        let mut partition_candidates = compactor
            .add_info_to_partitions(&partition_candidates, &table_columns)
            .await
            .unwrap();
        let partition = partition_candidates.pop().unwrap();

        let parquet_files_for_compaction =
            parquet_file_lookup::ParquetFilesForCompaction::for_partition_with_size_overrides(
                Arc::clone(&compactor.catalog),
                Arc::clone(&partition),
                &size_overrides,
            )
            .await
            .unwrap();

        let ParquetFilesForCompaction {
            level_0,
            level_1,
            .. // Ignore other levels
        } = parquet_files_for_compaction;

        let to_compact = parquet_file_filtering::filter_parquet_files(
            partition,
            level_0,
            level_1,
            compactor.config.memory_budget_bytes,
            &compactor.parquet_file_candidate_gauge,
            &compactor.parquet_file_candidate_bytes,
        );

        let to_compact = to_compact.into();

        compact_one_partition(&compactor, to_compact, "hot", true)
            .await
            .unwrap();

        // Should have 3 non-soft-deleted files:
        //
        // - the level 1 file that didn't overlap with anything
        // - the two newly created after compacting and splitting pf1, pf2, pf3, pf4, pf5
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 3);
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        assert_eq!(
            files_and_levels,
            vec![
                (6, CompactionLevel::FileNonOverlapped),
                (7, CompactionLevel::FileNonOverlapped),
                (8, CompactionLevel::FileNonOverlapped),
            ]
        );

        // ------------------------------------------------
        // Verify the parquet file content

        // Later compacted file
        let file1 = files.pop().unwrap();
        let batches = table.read_parquet_file(file1).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 1600      |      | WA   | 10   | 1970-01-01T00:00:00.000028Z |",
                "| 20        |      | VT   | 20   | 1970-01-01T00:00:00.000026Z |",
                "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );

        // Earlier compacted file
        let file0 = files.pop().unwrap();
        let batches = table.read_parquet_file(file0).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+--------------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                           |",
                "+-----------+------+------+------+--------------------------------+",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000000020Z |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z    |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000010Z    |",
                "| 1000      | WA   |      |      | 1970-01-01T00:00:00.000000010Z |",
                "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z    |",
                "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000000009Z |",
                "| 21        |      | OH   | 21   | 1970-01-01T00:00:00.000000025Z |",
                "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z    |",
                "+-----------+------+------+------+--------------------------------+",
            ],
            &batches
        );
    }
}
