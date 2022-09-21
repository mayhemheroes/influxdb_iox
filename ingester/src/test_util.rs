//! Test setups and data for ingester crate

#![allow(missing_docs)]

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use arrow::record_batch::RecordBatch;
use arrow_util::assert_batches_eq;
use bitflags::bitflags;
use data_types::{
    CompactionLevel, NamespaceId, NonEmptyString, PartitionId, PartitionKey, Sequence,
    SequenceNumber, ShardId, ShardIndex, TableId, Timestamp, Tombstone, TombstoneId,
};
use dml::{DmlDelete, DmlMeta, DmlOperation, DmlWrite};
use iox_catalog::{interface::Catalog, mem::MemCatalog};
use iox_query::test::{raw_data, TestChunk};
use iox_time::{SystemProvider, Time, TimeProvider};
use mutable_batch_lp::lines_to_batches;
use object_store::memory::InMemory;
use parquet_file::metadata::IoxMetadata;
use predicate::delete_predicate::parse_delete_predicate;
use schema::sort::SortKey;
use uuid::Uuid;

use crate::{
    data::{
        partition::{PersistingBatch, SnapshotBatch},
        shard::ShardData,
        IngesterData,
    },
    lifecycle::{LifecycleConfig, LifecycleHandle, LifecycleManager},
    query::QueryableBatch,
};

/// Create a persisting batch, some tombstones and corresponding metadata for them after compaction
pub async fn make_persisting_batch_with_meta() -> (Arc<PersistingBatch>, Vec<Tombstone>, IoxMetadata)
{
    // record batches of input data
    let batches = create_batches_with_influxtype_different_columns_different_order().await;

    // tombstones
    let tombstones = vec![
        create_tombstone(
            1,
            1,
            1,
            100,                          // delete's seq_number
            0,                            // min time of data to get deleted
            200000,                       // max time of data to get deleted
            "tag2=CT and field_int=1000", // delete predicate
        ),
        create_tombstone(
            1, 1, 1, 101,        // delete's seq_number
            0,          // min time of data to get deleted
            200000,     // max time of data to get deleted
            "tag1!=MT", // delete predicate
        ),
    ];

    // IDs set to the persisting batch and its compacted metadata
    let uuid = Uuid::new_v4();
    let namespace_name = "test_namespace";
    let partition_key = "test_partition_key";
    let table_name = "test_table";
    let shard_id = 1;
    let seq_num_start: i64 = 1;
    let seq_num_end: i64 = seq_num_start + 1; // 2 batches
    let namespace_id = 1;
    let table_id = 1;
    let partition_id = 1;

    // make the persisting batch
    let persisting_batch = make_persisting_batch(
        shard_id,
        seq_num_start,
        table_id,
        table_name,
        partition_id,
        uuid,
        batches,
        tombstones.clone(),
    );

    // make metadata
    let time_provider = Arc::new(SystemProvider::new());
    let meta = make_meta(
        uuid,
        time_provider.now(),
        shard_id,
        namespace_id,
        namespace_name,
        table_id,
        table_name,
        partition_id,
        partition_key,
        seq_num_end,
        CompactionLevel::Initial,
        Some(SortKey::from_columns(vec!["tag1", "tag2", "time"])),
    );

    (persisting_batch, tombstones, meta)
}

/// Create tombstone for testing
pub fn create_tombstone(
    id: i64,
    table_id: i64,
    shard_id: i64,
    seq_num: i64,
    min_time: i64,
    max_time: i64,
    predicate: &str,
) -> Tombstone {
    Tombstone {
        id: TombstoneId::new(id),
        table_id: TableId::new(table_id),
        shard_id: ShardId::new(shard_id),
        sequence_number: SequenceNumber::new(seq_num),
        min_time: Timestamp::new(min_time),
        max_time: Timestamp::new(max_time),
        serialized_predicate: predicate.to_string(),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn make_meta(
    object_store_id: Uuid,
    creation_timestamp: Time,
    shard_id: i64,
    namespace_id: i64,
    namespace_name: &str,
    table_id: i64,
    table_name: &str,
    partition_id: i64,
    partition_key: &str,
    max_sequence_number: i64,
    compaction_level: CompactionLevel,
    sort_key: Option<SortKey>,
) -> IoxMetadata {
    IoxMetadata {
        object_store_id,
        creation_timestamp,
        shard_id: ShardId::new(shard_id),
        namespace_id: NamespaceId::new(namespace_id),
        namespace_name: Arc::from(namespace_name),
        table_id: TableId::new(table_id),
        table_name: Arc::from(table_name),
        partition_id: PartitionId::new(partition_id),
        partition_key: PartitionKey::from(partition_key),
        max_sequence_number: SequenceNumber::new(max_sequence_number),
        compaction_level,
        sort_key,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn make_persisting_batch(
    shard_id: i64,
    seq_num_start: i64,
    table_id: i64,
    table_name: &str,
    partition_id: i64,
    object_store_id: Uuid,
    batches: Vec<Arc<RecordBatch>>,
    tombstones: Vec<Tombstone>,
) -> Arc<PersistingBatch> {
    let queryable_batch = make_queryable_batch_with_deletes(
        table_name,
        partition_id,
        seq_num_start,
        batches,
        tombstones,
    );

    Arc::new(PersistingBatch {
        shard_id: ShardId::new(shard_id),
        table_id: TableId::new(table_id),
        partition_id: PartitionId::new(partition_id),
        object_store_id,
        data: queryable_batch,
    })
}

pub fn make_queryable_batch(
    table_name: &str,
    partition_id: i64,
    seq_num_start: i64,
    batches: Vec<Arc<RecordBatch>>,
) -> Arc<QueryableBatch> {
    make_queryable_batch_with_deletes(table_name, partition_id, seq_num_start, batches, vec![])
}

pub fn make_queryable_batch_with_deletes(
    table_name: &str,
    partition_id: i64,
    seq_num_start: i64,
    batches: Vec<Arc<RecordBatch>>,
    tombstones: Vec<Tombstone>,
) -> Arc<QueryableBatch> {
    // make snapshots for the batches
    let mut snapshots = vec![];
    let mut seq_num = seq_num_start;
    for batch in batches {
        let seq = SequenceNumber::new(seq_num);
        snapshots.push(Arc::new(make_snapshot_batch(batch, seq, seq)));
        seq_num += 1;
    }

    Arc::new(QueryableBatch::new(
        table_name.into(),
        PartitionId::new(partition_id),
        snapshots,
        tombstones,
    ))
}

pub fn make_snapshot_batch(
    batch: Arc<RecordBatch>,
    min: SequenceNumber,
    max: SequenceNumber,
) -> SnapshotBatch {
    SnapshotBatch {
        min_sequence_number: min,
        max_sequence_number: max,
        data: batch,
    }
}

pub async fn create_one_row_record_batch_with_influxtype() -> Vec<Arc<RecordBatch>> {
    let chunk1 = Arc::new(
        TestChunk::new("t")
            .with_id(1)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_one_row_of_data(),
    );
    let batches = raw_data(&[chunk1]).await;

    // Make sure all data in one record batch
    assert_eq!(batches.len(), 1);

    // verify data
    let expected = vec![
        "+-----------+------+-----------------------------+",
        "| field_int | tag1 | time                        |",
        "+-----------+------+-----------------------------+",
        "| 1000      | MA   | 1970-01-01T00:00:00.000001Z |",
        "+-----------+------+-----------------------------+",
    ];
    assert_batches_eq!(&expected, &batches);

    let batches: Vec<_> = batches.iter().map(|r| Arc::new(r.clone())).collect();
    batches
}

pub async fn create_one_record_batch_with_influxtype_no_duplicates() -> Vec<Arc<RecordBatch>> {
    let chunk1 = Arc::new(
        TestChunk::new("t")
            .with_id(1)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_three_rows_of_data(),
    );
    let batches = raw_data(&[chunk1]).await;

    // Make sure all data in one record batch
    assert_eq!(batches.len(), 1);

    // verify data
    let expected = vec![
        "+-----------+------+-----------------------------+",
        "| field_int | tag1 | time                        |",
        "+-----------+------+-----------------------------+",
        "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
        "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
        "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
        "+-----------+------+-----------------------------+",
    ];
    assert_batches_eq!(&expected, &batches);

    let batches: Vec<_> = batches.iter().map(|r| Arc::new(r.clone())).collect();
    batches
}

pub async fn create_one_record_batch_with_influxtype_duplicates() -> Vec<Arc<RecordBatch>> {
    let chunk1 = Arc::new(
        TestChunk::new("t")
            .with_id(1)
            .with_time_column() //_with_full_stats(
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_ten_rows_of_data_some_duplicates(),
    );
    let batches = raw_data(&[chunk1]).await;

    // Make sure all data in one record batch
    assert_eq!(batches.len(), 1);

    // verify data
    let expected = vec![
        "+-----------+------+--------------------------------+",
        "| field_int | tag1 | time                           |",
        "+-----------+------+--------------------------------+",
        "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |",
        "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
        "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
        "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
        "+-----------+------+--------------------------------+",
    ];
    assert_batches_eq!(&expected, &batches);

    let batches: Vec<_> = batches.iter().map(|r| Arc::new(r.clone())).collect();
    batches
}

/// RecordBatches with knowledge of influx metadata
pub async fn create_batches_with_influxtype() -> Vec<Arc<RecordBatch>> {
    // Use the available TestChunk to create chunks and then convert them to raw RecordBatches
    let mut batches = vec![];

    // chunk1 with 10 rows and 3 columns
    let chunk1 = Arc::new(
        TestChunk::new("t")
            .with_id(1)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_ten_rows_of_data_some_duplicates(),
    );
    let batch1 = raw_data(&[chunk1]).await[0].clone();
    let expected = vec![
        "+-----------+------+--------------------------------+",
        "| field_int | tag1 | time                           |",
        "+-----------+------+--------------------------------+",
        "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |",
        "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
        "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
        "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
        "+-----------+------+--------------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch1.clone()]);
    batches.push(Arc::new(batch1));

    // chunk2 having duplicate data with chunk 1
    let chunk2 = Arc::new(
        TestChunk::new("t")
            .with_id(2)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_five_rows_of_data(),
    );
    let batch2 = raw_data(&[chunk2]).await[0].clone();
    let expected = vec![
        "+-----------+------+--------------------------------+",
        "| field_int | tag1 | time                           |",
        "+-----------+------+--------------------------------+",
        "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
        "+-----------+------+--------------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch2.clone()]);
    batches.push(Arc::new(batch2));

    // verify data from both batches
    let expected = vec![
        "+-----------+------+--------------------------------+",
        "| field_int | tag1 | time                           |",
        "+-----------+------+--------------------------------+",
        "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |",
        "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
        "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
        "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
        "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
        "+-----------+------+--------------------------------+",
    ];
    let b: Vec<_> = batches.iter().map(|b| (**b).clone()).collect();
    assert_batches_eq!(&expected, &b);

    batches
}

/// RecordBatches with knowledge of influx metadata
pub async fn create_batches_with_influxtype_different_columns() -> Vec<Arc<RecordBatch>> {
    // Use the available TestChunk to create chunks and then convert them to raw RecordBatches
    let mut batches = vec![];

    // chunk1 with 10 rows and 3 columns
    let chunk1 = Arc::new(
        TestChunk::new("t")
            .with_id(1)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_ten_rows_of_data_some_duplicates(),
    );
    let batch1 = raw_data(&[chunk1]).await[0].clone();
    let expected = vec![
        "+-----------+------+--------------------------------+",
        "| field_int | tag1 | time                           |",
        "+-----------+------+--------------------------------+",
        "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |",
        "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
        "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
        "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
        "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
        "+-----------+------+--------------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch1.clone()]);
    batches.push(Arc::new(batch1));

    // chunk2 having duplicate data with chunk 1
    // mmore columns
    let chunk2 = Arc::new(
        TestChunk::new("t")
            .with_id(2)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_tag_column("tag2")
            .with_i64_field_column("field_int2")
            .with_five_rows_of_data(),
    );
    let batch2 = raw_data(&[chunk2]).await[0].clone();
    let expected = vec![
        "+-----------+------------+------+------+--------------------------------+",
        "| field_int | field_int2 | tag1 | tag2 | time                           |",
        "+-----------+------------+------+------+--------------------------------+",
        "| 1000      | 1000       | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | 10         | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | 70         | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | 100        | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | 5          | MT   | AL   | 1970-01-01T00:00:00.000005Z    |",
        "+-----------+------------+------+------+--------------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch2.clone()]);
    batches.push(Arc::new(batch2));

    batches
}

/// RecordBatches with knowledge of influx metadata
pub async fn create_batches_with_influxtype_different_columns_different_order(
) -> Vec<Arc<RecordBatch>> {
    // Use the available TestChunk to create chunks and then convert them to raw RecordBatches
    let mut batches = vec![];

    // chunk1 with 10 rows and 3 columns
    let chunk1 = Arc::new(
        TestChunk::new("t")
            .with_id(1)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_tag_column("tag2")
            .with_ten_rows_of_data_some_duplicates(),
    );
    let batch1 = raw_data(&[chunk1]).await[0].clone();
    let expected = vec![
        "+-----------+------+------+--------------------------------+",
        "| field_int | tag1 | tag2 | time                           |",
        "+-----------+------+------+--------------------------------+",
        "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | MT   | AL   | 1970-01-01T00:00:00.000000005Z |",
        "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000002Z    |",
        "| 20        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000500Z |",
        "| 10        | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
        "| 30        | MT   | AL   | 1970-01-01T00:00:00.000000005Z |",
        "+-----------+------+------+--------------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch1.clone()]);
    batches.push(Arc::new(batch1.clone()));

    // chunk2 having duplicate data with chunk 1
    // mmore columns
    let chunk2 = Arc::new(
        TestChunk::new("t")
            .with_id(2)
            .with_time_column()
            .with_tag_column("tag2")
            .with_i64_field_column("field_int")
            .with_five_rows_of_data(),
    );
    let batch2 = raw_data(&[chunk2]).await[0].clone();
    let expected = vec![
        "+-----------+------+--------------------------------+",
        "| field_int | tag2 | time                           |",
        "+-----------+------+--------------------------------+",
        "| 1000      | CT   | 1970-01-01T00:00:00.000001Z    |",
        "| 10        | AL   | 1970-01-01T00:00:00.000007Z    |",
        "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
        "| 100       | MA   | 1970-01-01T00:00:00.000000050Z |",
        "| 5         | AL   | 1970-01-01T00:00:00.000005Z    |",
        "+-----------+------+--------------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch2.clone()]);
    batches.push(Arc::new(batch2));

    batches
}

/// Has 2 tag columns; tag1 has a lower cardinality (3) than tag3 (4)
pub async fn create_batches_with_influxtype_different_cardinality() -> Vec<Arc<RecordBatch>> {
    // Use the available TestChunk to create chunks and then convert them to raw RecordBatches
    let mut batches = vec![];

    let chunk1 = Arc::new(
        TestChunk::new("t")
            .with_id(1)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_tag_column("tag3")
            .with_four_rows_of_data(),
    );
    let batch1 = raw_data(&[chunk1]).await[0].clone();
    let expected = vec![
        "+-----------+------+------+-----------------------------+",
        "| field_int | tag1 | tag3 | time                        |",
        "+-----------+------+------+-----------------------------+",
        "| 1000      | WA   | TX   | 1970-01-01T00:00:00.000028Z |",
        "| 10        | VT   | PR   | 1970-01-01T00:00:00.000210Z |",
        "| 70        | UT   | OR   | 1970-01-01T00:00:00.000220Z |",
        "| 50        | VT   | AL   | 1970-01-01T00:00:00.000210Z |",
        "+-----------+------+------+-----------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch1.clone()]);
    batches.push(Arc::new(batch1.clone()));

    let chunk2 = Arc::new(
        TestChunk::new("t")
            .with_id(2)
            .with_time_column()
            .with_tag_column("tag1")
            .with_tag_column("tag3")
            .with_i64_field_column("field_int")
            .with_four_rows_of_data(),
    );
    let batch2 = raw_data(&[chunk2]).await[0].clone();
    let expected = vec![
        "+-----------+------+------+-----------------------------+",
        "| field_int | tag1 | tag3 | time                        |",
        "+-----------+------+------+-----------------------------+",
        "| 1000      | WA   | TX   | 1970-01-01T00:00:00.000028Z |",
        "| 10        | VT   | PR   | 1970-01-01T00:00:00.000210Z |",
        "| 70        | UT   | OR   | 1970-01-01T00:00:00.000220Z |",
        "| 50        | VT   | AL   | 1970-01-01T00:00:00.000210Z |",
        "+-----------+------+------+-----------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch2.clone()]);
    batches.push(Arc::new(batch2));

    batches
}

/// RecordBatches with knowledge of influx metadata
pub async fn create_batches_with_influxtype_same_columns_different_type() -> Vec<Arc<RecordBatch>> {
    // Use the available TestChunk to create chunks and then convert them to raw RecordBatches
    let mut batches = vec![];

    // chunk1
    let chunk1 = Arc::new(
        TestChunk::new("t")
            .with_id(1)
            .with_time_column()
            .with_tag_column("tag1")
            .with_i64_field_column("field_int")
            .with_three_rows_of_data(),
    );
    let batch1 = raw_data(&[chunk1]).await[0].clone();
    let expected = vec![
        "+-----------+------+-----------------------------+",
        "| field_int | tag1 | time                        |",
        "+-----------+------+-----------------------------+",
        "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
        "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
        "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
        "+-----------+------+-----------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch1.clone()]);
    batches.push(Arc::new(batch1));

    // chunk2 having duplicate data with chunk 1
    // mmore columns
    let chunk2 = Arc::new(
        TestChunk::new("t")
            .with_id(2)
            .with_time_column()
            .with_u64_column("field_int") //  u64 type but on existing name "field_int" used for i64 in chunk 1
            .with_tag_column("tag2")
            .with_three_rows_of_data(),
    );
    let batch2 = raw_data(&[chunk2]).await[0].clone();
    let expected = vec![
        "+-----------+------+-----------------------------+",
        "| field_int | tag2 | time                        |",
        "+-----------+------+-----------------------------+",
        "| 1000      | SC   | 1970-01-01T00:00:00.000008Z |",
        "| 10        | NC   | 1970-01-01T00:00:00.000010Z |",
        "| 70        | RI   | 1970-01-01T00:00:00.000020Z |",
        "+-----------+------+-----------------------------+",
    ];
    assert_batches_eq!(&expected, &[batch2.clone()]);
    batches.push(Arc::new(batch2));

    batches
}

pub const TEST_NAMESPACE: &str = "test_namespace";
pub const TEST_NAMESPACE_EMPTY: &str = "test_namespace_empty";
pub const TEST_TABLE: &str = "test_table";
pub const TEST_TABLE_EMPTY: &str = "test_table_empty";
pub const TEST_PARTITION_1: &str = "test+partition_1";
pub const TEST_PARTITION_2: &str = "test+partition_2";

bitflags! {
    /// Make the same in-memory data but data are split between:
    ///    . one or two partition
    ///    . The first partition will have a choice to have data in either
    ///       . buffer only
    ///       . snapshot only
    ///       . persisting only
    ///       . buffer + snapshot
    ///       . buffer + persisting
    ///       . snapshot + persisting
    ///       . buffer + snapshot + persisting
    ///    . If the second partittion exists, it only has data in its buffer
    pub struct DataLocation: u8 {
        const BUFFER = 0b001;
        const SNAPSHOT = 0b010;
        const PERSISTING = 0b100;
        const BUFFER_SNAPSHOT = Self::BUFFER.bits | Self::SNAPSHOT.bits;
        const BUFFER_PERSISTING = Self::BUFFER.bits | Self::PERSISTING.bits;
        const SNAPSHOT_PERSISTING = Self::SNAPSHOT.bits | Self::PERSISTING.bits;
        const BUFFER_SNAPSHOT_PERSISTING = Self::BUFFER.bits | Self::SNAPSHOT.bits | Self::PERSISTING.bits;
    }
}

/// This function produces one scenario but with the parameter combination (2*7),
/// you will be able to produce 14 scenarios by calling it in 2 loops
pub async fn make_ingester_data(two_partitions: bool, loc: DataLocation) -> IngesterData {
    // Whatever data because they won't be used in the tests
    let metrics: Arc<metric::Registry> = Default::default();
    let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
    let object_store = Arc::new(InMemory::new());
    let exec = Arc::new(iox_query::exec::Executor::new(1));
    let lifecycle = LifecycleManager::new(
        LifecycleConfig::new(
            200_000_000,
            100_000_000,
            100_000_000,
            Duration::from_secs(100_000_000),
            Duration::from_secs(100_000_000),
            100_000_000,
        ),
        Arc::clone(&metrics),
        Arc::new(SystemProvider::default()),
    );

    // Make data for one shard and two tables
    let shard_index = ShardIndex::new(1);
    let shard_id = populate_catalog(&*catalog).await;

    let mut shards = BTreeMap::new();
    shards.insert(
        shard_id,
        ShardData::new(shard_index, shard_id, Arc::clone(&metrics)),
    );

    let ingester = IngesterData::new(
        object_store,
        catalog,
        shards,
        exec,
        backoff::BackoffConfig::default(),
        metrics,
    );

    // Make partitions per requested
    let ops = make_partitions(two_partitions, shard_index);

    // Apply all ops
    for op in ops {
        ingester
            .buffer_operation(shard_id, op, &lifecycle.handle())
            .await
            .unwrap();
    }

    if loc.contains(DataLocation::PERSISTING) {
        // Move partition 1 data to persisting
        let _ignored = ingester
            .shard(shard_id)
            .unwrap()
            .namespace(TEST_NAMESPACE)
            .unwrap()
            .snapshot_to_persisting(TEST_TABLE, &PartitionKey::from(TEST_PARTITION_1))
            .await;
    } else if loc.contains(DataLocation::SNAPSHOT) {
        // move partition 1 data to snapshot
        let _ignored = ingester
            .shard(shard_id)
            .unwrap()
            .namespace(TEST_NAMESPACE)
            .unwrap()
            .snapshot(TEST_TABLE, &PartitionKey::from(TEST_PARTITION_1))
            .await;
    }

    ingester
}

pub async fn make_ingester_data_with_tombstones(loc: DataLocation) -> IngesterData {
    // Whatever data because they won't be used in the tests
    let metrics: Arc<metric::Registry> = Default::default();
    let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
    let object_store = Arc::new(InMemory::new());
    let exec = Arc::new(iox_query::exec::Executor::new(1));
    let lifecycle = LifecycleManager::new(
        LifecycleConfig::new(
            200_000_000,
            100_000_000,
            100_000_000,
            Duration::from_secs(100_000_000),
            Duration::from_secs(100_000_000),
            100_000_000,
        ),
        Arc::clone(&metrics),
        Arc::new(SystemProvider::default()),
    );

    // Make data for one shard and two tables
    let shard_index = ShardIndex::new(0);
    let shard_id = populate_catalog(&*catalog).await;

    let mut shards = BTreeMap::new();
    shards.insert(
        shard_id,
        ShardData::new(shard_index, shard_id, Arc::clone(&metrics)),
    );

    let ingester = IngesterData::new(
        object_store,
        catalog,
        shards,
        exec,
        backoff::BackoffConfig::default(),
        metrics,
    );

    // Make partitions per requested
    make_one_partition_with_tombstones(&ingester, &lifecycle.handle(), loc, shard_index, shard_id)
        .await;

    ingester
}

/// Make data for one or two partitions per requested
pub(crate) fn make_partitions(two_partitions: bool, shard_index: ShardIndex) -> Vec<DmlOperation> {
    // In-memory data includes these rows but split between 4 groups go into
    // different batches of parittion 1 or partittion 2  as requeted
    // let expected = vec![
    //         "+------------+-----+------+--------------------------------+",
    //         "| city       | day | temp | time                           |",
    //         "+------------+-----+------+--------------------------------+",
    //         "| Andover    | tue | 56   | 1970-01-01T00:00:00.000000030Z |", // in group 1 - seq_num: 2
    //         "| Andover    | mon |      | 1970-01-01T00:00:00.000000046Z |", // in group 2 - seq_num: 3
    //         "| Boston     | sun | 60   | 1970-01-01T00:00:00.000000036Z |", // in group 1 - seq_num: 1
    //         "| Boston     | mon |      | 1970-01-01T00:00:00.000000038Z |", // in group 3 - seq_num: 5
    //         "| Medford    | sun | 55   | 1970-01-01T00:00:00.000000022Z |", // in group 4 - seq_num: 7
    //         "| Medford    | wed |      | 1970-01-01T00:00:00.000000026Z |", // in group 2 - seq_num: 4
    //         "| Reading    | mon | 58   | 1970-01-01T00:00:00.000000040Z |", // in group 4 - seq_num: 8
    //         "| Wilmington | mon |      | 1970-01-01T00:00:00.000000035Z |", // in group 3 - seq_num: 6
    //         "+------------+-----+------+--------------------------------+",
    //     ];

    // ------------------------------------------
    // Build the first partition
    let (mut ops, seq_num) =
        make_first_partition_data(&PartitionKey::from(TEST_PARTITION_1), shard_index);

    // ------------------------------------------
    // Build the second partition if asked

    let mut seq_num = seq_num.get();
    if two_partitions {
        // Group 4: in buffer of p2
        ops.push(DmlOperation::Write(make_write_op(
            &PartitionKey::from(TEST_PARTITION_2),
            shard_index,
            seq_num,
            r#"test_table,city=Medford day="sun",temp=55 22"#,
        )));
        seq_num += 1;

        ops.push(DmlOperation::Write(make_write_op(
            &PartitionKey::from(TEST_PARTITION_2),
            shard_index,
            seq_num,
            r#"test_table,city=Reading day="mon",temp=58 40"#,
        )));
    } else {
        // Group 4: in buffer of p1
        ops.push(DmlOperation::Write(make_write_op(
            &PartitionKey::from(TEST_PARTITION_1),
            shard_index,
            seq_num,
            r#"test_table,city=Medford day="sun",temp=55 22"#,
        )));
        seq_num += 1;

        ops.push(DmlOperation::Write(make_write_op(
            &PartitionKey::from(TEST_PARTITION_1),
            shard_index,
            seq_num,
            r#"test_table,city=Reading day="mon",temp=58 40"#,
        )));
    }

    ops
}

/// Make data for one partition with tombstones
async fn make_one_partition_with_tombstones(
    ingester: &IngesterData,
    lifecycle_handle: &dyn LifecycleHandle,
    loc: DataLocation,
    shard_index: ShardIndex,
    shard_id: ShardId,
) {
    // In-memory data includes these rows but split between 4 groups go into
    // different batches of parittion 1 or partittion 2  as requeted
    // let expected = vec![
    //         "+------------+-----+------+--------------------------------+",
    //         "| city       | day | temp | time                           |",
    //         "+------------+-----+------+--------------------------------+",
    //         "| Andover    | tue | 56   | 1970-01-01T00:00:00.000000030Z |", // in group 1 - seq_num: 2
    //         "| Andover    | mon |      | 1970-01-01T00:00:00.000000046Z |", // in group 2 - seq_num: 3
    //         "| Boston     | sun | 60   | 1970-01-01T00:00:00.000000036Z |", // in group 1 - seq_num: 1  --> will get deleted
    //         "| Boston     | mon |      | 1970-01-01T00:00:00.000000038Z |", // in group 3 - seq_num: 5  --> will get deleted
    //         "| Medford    | sun | 55   | 1970-01-01T00:00:00.000000022Z |", // in group 4 - seq_num: 8  (after the tombstone's seq num)
    //         "| Medford    | wed |      | 1970-01-01T00:00:00.000000026Z |", // in group 2 - seq_num: 4
    //         "| Reading    | mon | 58   | 1970-01-01T00:00:00.000000040Z |", // in group 4 - seq_num: 9
    //         "| Wilmington | mon |      | 1970-01-01T00:00:00.000000035Z |", // in group 3 - seq_num: 6
    //         "+------------+-----+------+--------------------------------+",
    //     ];

    let (ops, seq_num) =
        make_first_partition_data(&PartitionKey::from(TEST_PARTITION_1), shard_index);

    // Apply all ops
    for op in ops {
        ingester
            .buffer_operation(shard_id, op, lifecycle_handle)
            .await
            .unwrap();
    }

    if loc.contains(DataLocation::PERSISTING) {
        // Move partition 1 data to persisting
        let _ignored = ingester
            .shard(shard_id)
            .unwrap()
            .namespace(TEST_NAMESPACE)
            .unwrap()
            .snapshot_to_persisting(TEST_TABLE, &PartitionKey::from(TEST_PARTITION_1))
            .await;
    } else if loc.contains(DataLocation::SNAPSHOT) {
        // move partition 1 data to snapshot
        let _ignored = ingester
            .shard(shard_id)
            .unwrap()
            .namespace(TEST_NAMESPACE)
            .unwrap()
            .snapshot(TEST_TABLE, &PartitionKey::from(TEST_PARTITION_1))
            .await;
    }

    // Add tombstones
    // Depending on where the existing data is, they (buffer & snapshot) will be either moved to a new snapshot after
    // applying the tombstone or (persisting) stay where they are and the tombstones is kept to get applied later
    // ------------------------------------------
    // Delete
    let mut seq_num = seq_num.get();
    seq_num += 1;

    let delete = parse_delete_predicate(
        "1970-01-01T00:00:00.000000010Z",
        "1970-01-01T00:00:00.000000050Z",
        "city=Boston",
    )
    .unwrap();

    ingester
        .buffer_operation(
            shard_id,
            DmlOperation::Delete(DmlDelete::new(
                TEST_NAMESPACE.to_string(),
                delete,
                NonEmptyString::new(TEST_TABLE),
                DmlMeta::sequenced(
                    Sequence {
                        shard_index,
                        sequence_number: SequenceNumber::new(seq_num),
                    },
                    Time::MIN,
                    None,
                    42,
                ),
            )),
            lifecycle_handle,
        )
        .await
        .unwrap();

    // Group 4: in buffer of p1 after the tombstone

    ingester
        .buffer_operation(
            shard_id,
            DmlOperation::Write(make_write_op(
                &PartitionKey::from(TEST_PARTITION_1),
                shard_index,
                seq_num,
                r#"test_table,city=Medford day="sun",temp=55 22"#,
            )),
            lifecycle_handle,
        )
        .await
        .unwrap();
    seq_num += 1;

    ingester
        .buffer_operation(
            shard_id,
            DmlOperation::Write(make_write_op(
                &PartitionKey::from(TEST_PARTITION_1),
                shard_index,
                seq_num,
                r#"test_table,city=Reading day="mon",temp=58 40"#,
            )),
            lifecycle_handle,
        )
        .await
        .unwrap();
}

fn make_write_op(
    partition_key: &PartitionKey,
    shard_index: ShardIndex,
    sequence_number: i64,
    lines: &str,
) -> DmlWrite {
    DmlWrite::new(
        TEST_NAMESPACE.to_string(),
        lines_to_batches(lines, 0).unwrap(),
        Some(partition_key.clone()),
        DmlMeta::sequenced(
            Sequence {
                shard_index,
                sequence_number: SequenceNumber::new(sequence_number),
            },
            Time::MIN,
            None,
            42,
        ),
    )
}

fn make_first_partition_data(
    partition_key: &PartitionKey,
    shard_index: ShardIndex,
) -> (Vec<DmlOperation>, SequenceNumber) {
    // In-memory data includes these rows but split between 3 groups go into
    // different batches of parittion p1
    // let expected = vec![
    //         "+------------+-----+------+--------------------------------+",
    //         "| city       | day | temp | time                           |",
    //         "+------------+-----+------+--------------------------------+",
    //         "| Andover    | tue | 56   | 1970-01-01T00:00:00.000000030Z |", // in group 1 - seq_num: 2
    //         "| Andover    | mon |      | 1970-01-01T00:00:00.000000046Z |", // in group 2 - seq_num: 3
    //         "| Boston     | sun | 60   | 1970-01-01T00:00:00.000000036Z |", // in group 1 - seq_num: 1
    //         "| Boston     | mon |      | 1970-01-01T00:00:00.000000038Z |", // in group 3 - seq_num: 5
    //         "| Medford    | wed |      | 1970-01-01T00:00:00.000000026Z |", // in group 2 - seq_num: 4
    //         "| Wilmington | mon |      | 1970-01-01T00:00:00.000000035Z |", // in group 3 - seq_num: 6
    //         "+------------+-----+------+--------------------------------+",
    //     ];

    let mut out = Vec::default();

    // ------------------------------------------
    // Build the first partition
    let mut seq_num = 0;

    // --------------------
    // Group 1
    out.push(DmlOperation::Write(make_write_op(
        partition_key,
        shard_index,
        seq_num,
        r#"test_table,city=Boston day="sun",temp=60 36"#,
    )));
    seq_num += 1;

    out.push(DmlOperation::Write(make_write_op(
        partition_key,
        shard_index,
        seq_num,
        r#"test_table,city=Andover day="tue",temp=56 30"#,
    )));
    seq_num += 1;

    // --------------------
    // Group 2
    out.push(DmlOperation::Write(make_write_op(
        partition_key,
        shard_index,
        seq_num,
        r#"test_table,city=Andover day="mon" 46"#,
    )));
    seq_num += 1;

    out.push(DmlOperation::Write(make_write_op(
        partition_key,
        shard_index,
        seq_num,
        r#"test_table,city=Medford day="wed" 26"#,
    )));
    seq_num += 1;

    // --------------------
    // Group 3: always in buffer
    // Fill `buffer`
    out.push(DmlOperation::Write(make_write_op(
        partition_key,
        shard_index,
        seq_num,
        r#"test_table,city=Boston day="mon" 38"#,
    )));
    seq_num += 1;

    out.push(DmlOperation::Write(make_write_op(
        partition_key,
        shard_index,
        seq_num,
        r#"test_table,city=Wilmington day="mon" 35"#,
    )));
    seq_num += 1;

    (out, SequenceNumber::new(seq_num))
}

async fn populate_catalog(catalog: &dyn Catalog) -> ShardId {
    let mut c = catalog.repositories().await;
    let topic = c.topics().create_or_get("whatevs").await.unwrap();
    let query_pool = c.query_pools().create_or_get("whatevs").await.unwrap();
    let shard_index = ShardIndex::new(0);
    let ns_id = c
        .namespaces()
        .create(
            TEST_NAMESPACE,
            iox_catalog::INFINITE_RETENTION_POLICY,
            topic.id,
            query_pool.id,
        )
        .await
        .unwrap()
        .id;
    c.tables().create_or_get(TEST_TABLE, ns_id).await.unwrap();
    c.shards()
        .create_or_get(&topic, shard_index)
        .await
        .unwrap()
        .id
}
