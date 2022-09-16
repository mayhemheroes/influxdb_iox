//! Compact partitions that are cold because they have not gotten writes recently and they're not
//! fully compacted.

use crate::{
    compact::Compactor, compact_candidates_with_memory_budget, compact_in_parallel,
    parquet_file_combining, parquet_file_lookup,
};
use backoff::Backoff;
use data_types::CompactionLevel;
use metric::Attributes;
use observability_deps::tracing::*;
use snafu::Snafu;
use std::sync::Arc;

/// Cold compaction. Returns the number of compacted partitions.
pub async fn compact(compactor: Arc<Compactor>) -> usize {
    let compaction_type = "cold";
    // Select cold partition candidates
    debug!(compaction_type, "start collecting partitions to compact");
    let attributes = Attributes::from(&[("partition_type", compaction_type)]);
    let start_time = compactor.time_provider.now();
    let candidates = Backoff::new(&compactor.backoff_config)
        .retry_all_errors("cold_partitions_to_compact", || async {
            compactor
                .cold_partitions_to_compact(compactor.config.max_number_partitions_per_shard)
                .await
        })
        .await
        .expect("retry forever");
    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let duration = compactor
            .candidate_selection_duration
            .recorder(attributes.clone());
        duration.record(delta);
    }

    let n_candidates = candidates.len();
    if n_candidates == 0 {
        debug!(compaction_type, "no compaction candidates found");
        return 0;
    } else {
        debug!(n_candidates, compaction_type, "found compaction candidates");
    }

    let start_time = compactor.time_provider.now();

    // Compact any remaining level 0 files in parallel
    compact_candidates_with_memory_budget(
        Arc::clone(&compactor),
        compaction_type,
        CompactionLevel::Initial,
        compact_in_parallel,
        true, // split
        candidates.clone().into(),
    )
    .await;

    // Compact level 1 files in parallel ("full compaction")
    compact_candidates_with_memory_budget(
        Arc::clone(&compactor),
        compaction_type,
        CompactionLevel::FileNonOverlapped,
        compact_in_parallel,
        false, // don't split
        candidates.into(),
    )
    .await;

    // Done compacting all candidates in the cycle, record its time
    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let duration = compactor.compaction_cycle_duration.recorder(attributes);
        duration.record(delta);
    }

    n_candidates
}

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub(crate) enum Error {
    #[snafu(display("{}", source))]
    Lookup {
        source: parquet_file_lookup::PartitionFilesFromPartitionError,
    },

    #[snafu(display("{}", source))]
    Combining {
        source: Box<parquet_file_combining::Error>,
    },

    #[snafu(display("{}", source))]
    Upgrading {
        source: iox_catalog::interface::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compact_one_partition, handler::CompactorConfig, parquet_file_filtering,
        ParquetFilesForCompaction,
    };
    use ::parquet_file::storage::ParquetStorage;
    use arrow_util::assert_batches_sorted_eq;
    use backoff::BackoffConfig;
    use data_types::{ColumnType, CompactionLevel, ParquetFileId};
    use iox_query::exec::Executor;
    use iox_tests::util::{TestCatalog, TestParquetFileBuilder};
    use iox_time::{SystemProvider, TimeProvider};
    use std::{collections::HashMap, time::Duration};

    #[tokio::test]
    async fn test_compact_remaining_level_0_files_many_files() {
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
        let time_38_hour_ago = (time.now() - Duration::from_secs(60 * 60 * 38)).timestamp_nanos();
        let config = make_compactor_config();
        let metrics = Arc::new(metric::Registry::new());
        let compactor = Compactor::new(
            vec![shard.shard.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        );

        // parquet files that are all in the same partition
        let mut size_overrides = HashMap::<ParquetFileId, i64>::default();

        // pf1 does not overlap with any other level 0
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_max_seq(3)
            .with_min_time(10)
            .with_max_time(20)
            .with_creation_time(time_38_hour_ago);
        let pf1_no_overlap = partition.create_parquet_file(builder).await;
        size_overrides.insert(
            pf1_no_overlap.parquet_file.id,
            compactor.config.max_desired_file_size_bytes as i64 + 10,
        );

        // pf2 overlaps with pf3
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp2)
            .with_max_seq(5)
            .with_min_time(8_000)
            .with_max_time(20_000)
            .with_creation_time(time_38_hour_ago);
        let pf2 = partition.create_parquet_file(builder).await;
        size_overrides.insert(pf2.parquet_file.id, 100); // small file

        // pf3 overlaps with pf2
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp3)
            .with_max_seq(10)
            .with_min_time(6_000)
            .with_max_time(25_000)
            .with_creation_time(time_38_hour_ago);
        let pf3 = partition.create_parquet_file(builder).await;
        size_overrides.insert(pf3.parquet_file.id, 100); // small file

        // pf4 does not overlap with any but is small
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp4)
            .with_max_seq(18)
            .with_min_time(26_000)
            .with_max_time(28_000)
            .with_creation_time(time_38_hour_ago);
        let pf4 = partition.create_parquet_file(builder).await;
        size_overrides.insert(pf4.parquet_file.id, 100); // small file

        // pf5 was created in a previous compaction cycle; overlaps with pf1
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp5)
            .with_max_seq(1)
            .with_min_time(9)
            .with_max_time(25)
            .with_creation_time(time_38_hour_ago)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let pf5 = partition.create_parquet_file(builder).await;
        size_overrides.insert(pf5.parquet_file.id, 100); // small file

        // pf6 was created in a previous compaction cycle; does not overlap with any
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp6)
            .with_max_seq(20)
            .with_min_time(90000)
            .with_max_time(91000)
            .with_creation_time(time_38_hour_ago)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let pf6 = partition.create_parquet_file(builder).await;
        size_overrides.insert(pf6.parquet_file.id, 100); // small file

        // should have 4 level-0 files before compacting
        let count = catalog.count_level_0_files(shard.shard.id).await;
        assert_eq!(count, 4);

        // ------------------------------------------------
        // Compact
        let mut partition_candidates = compactor
            .cold_partitions_to_compact(compactor.config.max_number_partitions_per_shard)
            .await
            .unwrap();

        assert_eq!(partition_candidates.len(), 1);
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

        compact_one_partition(&compactor, to_compact, "cold", true)
            .await
            .unwrap();

        // Should have 3 non-soft-deleted files:
        //
        // - pf6, the level 1 file that didn't overlap with anything
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
                (
                    pf6.parquet_file.id.get(),
                    CompactionLevel::FileNonOverlapped
                ),
                (7, CompactionLevel::FileNonOverlapped),
                (8, CompactionLevel::FileNonOverlapped),
            ]
        );

        // ------------------------------------------------
        // Verify the parquet file content

        // Data from pf3 and pf4, later times
        let file2 = files.pop().unwrap();
        let batches = table.read_parquet_file(file2).await;
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

        // Data from pf1, pf2, pf3, and pf5, earlier times
        let file1 = files.pop().unwrap();
        let batches = table.read_parquet_file(file1).await;
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

        // Data from pf6 that didn't overlap with anything, left unchanged
        let file0 = files.pop().unwrap();
        let batches = table.read_parquet_file(file0).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+-----------------------------+",
                "| field_int | tag2 | tag3 | time                        |",
                "+-----------+------+------+-----------------------------+",
                "| 421       | OH   | 21   | 1970-01-01T00:00:00.000091Z |",
                "| 81601     | PA   | 15   | 1970-01-01T00:00:00.000090Z |",
                "+-----------+------+------+-----------------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn test_compact_remaining_level_0_files_one_level_0_without_overlap() {
        test_helpers::maybe_start_logging();
        let catalog = TestCatalog::new();

        // lp1 does not overlap with any other level 0 or level 1
        let lp1 = vec![
            "table,tag1=WA field_int=1000i 10",
            "table,tag1=VT field_int=10i 20",
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
        let time_38_hour_ago = (time.now() - Duration::from_secs(60 * 60 * 38)).timestamp_nanos();
        let config = make_compactor_config();
        let metrics = Arc::new(metric::Registry::new());
        let compactor = Compactor::new(
            vec![shard.shard.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        );

        // parquet files that are all in the same partition
        let mut size_overrides = HashMap::<ParquetFileId, i64>::default();

        // pf1 does not overlap with any other level 0
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_max_seq(3)
            .with_min_time(10)
            .with_max_time(20)
            .with_creation_time(time_38_hour_ago);
        let pf1 = partition.create_parquet_file(builder).await;
        size_overrides.insert(pf1.parquet_file.id, 100); // small file

        // pf6 was created in a previous compaction cycle; does not overlap with any
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp6)
            .with_max_seq(20)
            .with_min_time(90000)
            .with_max_time(91000)
            .with_creation_time(time_38_hour_ago)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let pf6 = partition.create_parquet_file(builder).await;
        size_overrides.insert(pf6.parquet_file.id, 100); // small file

        // should have 1 level-0 file before compacting
        let count = catalog.count_level_0_files(shard.shard.id).await;
        assert_eq!(count, 1);

        // ------------------------------------------------
        // Compact
        let mut partition_candidates = compactor
            .cold_partitions_to_compact(compactor.config.max_number_partitions_per_shard)
            .await
            .unwrap();

        assert_eq!(partition_candidates.len(), 1);
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
            Arc::clone(&partition),
            level_0,
            level_1,
            compactor.config.memory_budget_bytes,
            &compactor.parquet_file_candidate_gauge,
            &compactor.parquet_file_candidate_bytes,
        );

        let to_compact = to_compact.into();

        compact_one_partition(&compactor, to_compact, "cold", true)
            .await
            .unwrap();

        // Should have 2 non-soft-deleted files:
        //
        // - pf1, the newly created level 1 file that was only upgraded from level 0
        // - pf6, the level 1 file that didn't overlap with anything
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 2);
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        assert_eq!(
            files_and_levels,
            vec![
                (
                    pf1.parquet_file.id.get(),
                    CompactionLevel::FileNonOverlapped
                ),
                (
                    pf6.parquet_file.id.get(),
                    CompactionLevel::FileNonOverlapped
                ),
            ]
        );

        // ------------------------------------------------
        // Verify the parquet file content

        // pf6
        let file1 = files.pop().unwrap();
        let batches = table.read_parquet_file(file1).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+-----------------------------+",
                "| field_int | tag2 | tag3 | time                        |",
                "+-----------+------+------+-----------------------------+",
                "| 421       | OH   | 21   | 1970-01-01T00:00:00.000091Z |",
                "| 81601     | PA   | 15   | 1970-01-01T00:00:00.000090Z |",
                "+-----------+------+------+-----------------------------+",
            ],
            &batches
        );

        // pf1
        let file0 = files.pop().unwrap();
        let batches = table.read_parquet_file(file0).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+--------------------------------+",
                "| field_int | tag1 | time                           |",
                "+-----------+------+--------------------------------+",
                "| 10        | VT   | 1970-01-01T00:00:00.000000020Z |",
                "| 1000      | WA   | 1970-01-01T00:00:00.000000010Z |",
                "+-----------+------+--------------------------------+",
            ],
            &batches
        );

        // Full compaction will now combine the two level 1 files into one level 2 file
        let parquet_files_for_compaction =
            parquet_file_lookup::ParquetFilesForCompaction::for_partition_with_size_overrides(
                Arc::clone(&compactor.catalog),
                Arc::clone(&partition),
                &size_overrides,
            )
            .await
            .unwrap();

        let ParquetFilesForCompaction {
            level_1,
            level_2,
            .. // Ignore other levels
        } = parquet_files_for_compaction;

        let to_compact = parquet_file_filtering::filter_parquet_files(
            Arc::clone(&partition),
            level_1,
            level_2,
            compactor.config.memory_budget_bytes,
            &compactor.parquet_file_candidate_gauge,
            &compactor.parquet_file_candidate_bytes,
        );

        let to_compact = to_compact.into();

        compact_one_partition(&compactor, to_compact, "cold", false)
            .await
            .unwrap();

        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 1);
        let file = files.pop().unwrap();
        assert_eq!(file.id.get(), 3);
        assert_eq!(file.compaction_level, CompactionLevel::Final);

        // ------------------------------------------------
        // Verify the parquet file content

        let batches = table.read_parquet_file(file).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+--------------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                           |",
                "+-----------+------+------+------+--------------------------------+",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000000020Z |",
                "| 1000      | WA   |      |      | 1970-01-01T00:00:00.000000010Z |",
                "| 421       |      | OH   | 21   | 1970-01-01T00:00:00.000091Z    |",
                "| 81601     |      | PA   | 15   | 1970-01-01T00:00:00.000090Z    |",
                "+-----------+------+------+------+--------------------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn test_full_cold_compaction_upgrades_one_level_0() {
        test_helpers::maybe_start_logging();
        let catalog = TestCatalog::new();

        // Create one cold level 0 file that will get upgraded to level 1 then upgraded to level 2
        let lp1 = vec![
            "table,tag1=WA field_int=1000i 10",
            "table,tag1=VT field_int=10i 20",
        ]
        .join("\n");

        let ns = catalog.create_namespace("ns").await;
        let shard = ns.create_shard(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;
        let partition = table.with_shard(&shard).create_partition("part").await;
        let time = Arc::new(SystemProvider::new());
        let time_38_hour_ago = (time.now() - Duration::from_secs(60 * 60 * 38)).timestamp_nanos();
        let config = make_compactor_config();
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

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_creation_time(time_38_hour_ago);
        partition.create_parquet_file(builder).await;

        // should have 1 level-0 file before compacting
        let count = catalog.count_level_0_files(shard.shard.id).await;
        assert_eq!(count, 1);

        // ------------------------------------------------
        // Compact
        compact(compactor).await;

        // Should have 1 non-soft-deleted files:
        //
        // - the newly created file that was upgraded to level 1 then to level 2
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 1);
        let file = files.pop().unwrap();
        assert_eq!(file.id.get(), 1); // ID doesn't change because the file doesn't get rewritten
        assert_eq!(file.compaction_level, CompactionLevel::Final);

        // ------------------------------------------------
        // Verify the parquet file content

        let batches = table.read_parquet_file(file).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+--------------------------------+",
                "| field_int | tag1 | time                           |",
                "+-----------+------+--------------------------------+",
                "| 10        | VT   | 1970-01-01T00:00:00.000000020Z |",
                "| 1000      | WA   | 1970-01-01T00:00:00.000000010Z |",
                "+-----------+------+--------------------------------+",
            ],
            &batches
        );
    }

    fn make_compactor_config() -> CompactorConfig {
        CompactorConfig {
            max_desired_file_size_bytes: 10_000,
            percentage_max_file_size: 30,
            split_percentage: 80,
            max_number_partitions_per_shard: 1,
            min_number_recent_ingested_files_per_partition: 1,
            hot_multiple: 4,
            memory_budget_bytes: 100_000_000,
        }
    }

    #[tokio::test]
    async fn full_cold_compaction_many_files() {
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
        let time_38_hour_ago = (time.now() - Duration::from_secs(60 * 60 * 38)).timestamp_nanos();
        let config = make_compactor_config();
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
            .with_creation_time(time_38_hour_ago);
        let pf1 = partition.create_parquet_file(builder).await;
        size_overrides.insert(
            pf1.parquet_file.id,
            compactor.config.max_desired_file_size_bytes as i64 + 10,
        );

        // pf2 overlaps with pf3
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp2)
            .with_max_seq(5)
            .with_min_time(8_000)
            .with_max_time(20_000)
            .with_creation_time(time_38_hour_ago);
        let pf2 = partition.create_parquet_file(builder).await;
        size_overrides.insert(
            pf2.parquet_file.id,
            100, // small file
        );

        // pf3 overlaps with pf2
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp3)
            .with_max_seq(10)
            .with_min_time(6_000)
            .with_max_time(25_000)
            .with_creation_time(time_38_hour_ago);
        let pf3 = partition.create_parquet_file(builder).await;
        size_overrides.insert(
            pf3.parquet_file.id,
            100, // small file
        );

        // pf4 does not overlap with any but is small
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp4)
            .with_max_seq(18)
            .with_min_time(26_000)
            .with_max_time(28_000)
            .with_creation_time(time_38_hour_ago);
        let pf4 = partition.create_parquet_file(builder).await;
        size_overrides.insert(
            pf4.parquet_file.id,
            100, // small file
        );

        // pf5 was created in a previous compaction cycle; overlaps with pf1
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp5)
            .with_max_seq(1)
            .with_min_time(9)
            .with_max_time(25)
            .with_creation_time(time_38_hour_ago)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let pf5 = partition.create_parquet_file(builder).await;
        size_overrides.insert(
            pf5.parquet_file.id,
            100, // small file
        );

        // pf6 was created in a previous compaction cycle; does not overlap with any
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp6)
            .with_max_seq(20)
            .with_min_time(90000)
            .with_max_time(91000)
            .with_creation_time(time_38_hour_ago)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        let pf6 = partition.create_parquet_file(builder).await;
        size_overrides.insert(
            pf6.parquet_file.id,
            100, // small file
        );

        // should have 4 level-0 files before compacting
        let count = catalog.count_level_0_files(shard.shard.id).await;
        assert_eq!(count, 4);

        // ------------------------------------------------
        // Compact

        compact(compactor).await;

        // Should have 1 non-soft-deleted file:
        //
        // - the level 2 file created after combining all 3 level 1 files created by the first step
        //   of compaction to compact remaining level 0 files
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 1, "{files:?}");
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();

        // The initial files are: L0 1-4, L1 5-6. The first step of cold compaction took files 1-5
        // and compacted them and split them into files 7 and 8. The second step of cold compaction
        // took 6, 7, and 8 and combined them all into file 9.
        assert_eq!(files_and_levels, vec![(9, CompactionLevel::Final)]);

        // ------------------------------------------------
        // Verify the parquet file content
        let file = files.pop().unwrap();
        let batches = table.read_parquet_file(file).await;
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
                "| 1600      |      | WA   | 10   | 1970-01-01T00:00:00.000028Z    |",
                "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000000009Z |",
                "| 20        |      | VT   | 20   | 1970-01-01T00:00:00.000026Z    |",
                "| 21        |      | OH   | 21   | 1970-01-01T00:00:00.000000025Z |",
                "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z    |",
                "| 421       |      | OH   | 21   | 1970-01-01T00:00:00.000091Z    |",
                "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z    |",
                "| 81601     |      | PA   | 15   | 1970-01-01T00:00:00.000090Z    |",
                "+-----------+------+------+------+--------------------------------+",
            ],
            &batches
        );
    }
}
