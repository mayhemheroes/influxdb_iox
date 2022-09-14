
//! This file is auto generated by query_tests/generate.
//! Do not edit manually --> will result in sadness
use std::path::Path;
use crate::runner::Runner;

#[tokio::test]
// Tests from "basic.sql",
async fn test_cases_basic_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("basic.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}

#[tokio::test]
// Tests from "delete_all.sql",
async fn test_cases_delete_all_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("delete_all.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}

#[tokio::test]
// Tests from "delete_multi_expr_one_chunk.sql",
async fn test_cases_delete_multi_expr_one_chunk_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("delete_multi_expr_one_chunk.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}

#[tokio::test]
// Tests from "delete_simple_pred_one_chunk.sql",
async fn test_cases_delete_simple_pred_one_chunk_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("delete_simple_pred_one_chunk.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}

#[tokio::test]
// Tests from "delete_three_chunks_1.sql",
async fn test_cases_delete_three_chunks_1_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("delete_three_chunks_1.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}

#[tokio::test]
// Tests from "delete_three_chunks_2.sql",
async fn test_cases_delete_three_chunks_2_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("delete_three_chunks_2.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}

#[tokio::test]
// Tests from "delete_three_chunks_3.sql",
async fn test_cases_delete_three_chunks_3_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("delete_three_chunks_3.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}

#[tokio::test]
// Tests from "delete_three_chunks_4.sql",
async fn test_cases_delete_three_chunks_4_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("delete_three_chunks_4.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}

#[tokio::test]
// Tests from "delete_two_del_multi_expr_one_chunk.sql",
async fn test_cases_delete_two_del_multi_expr_one_chunk_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("delete_two_del_multi_expr_one_chunk.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}

#[tokio::test]
// Tests from "duplicates_ingester.sql",
async fn test_cases_duplicates_ingester_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("duplicates_ingester.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}

#[tokio::test]
// Tests from "duplicates_parquet.sql",
async fn test_cases_duplicates_parquet_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("duplicates_parquet.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}

#[tokio::test]
// Tests from "new_sql_system_tables.sql",
async fn test_cases_new_sql_system_tables_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("new_sql_system_tables.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}

#[tokio::test]
// Tests from "pushdown.sql",
async fn test_cases_pushdown_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("pushdown.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}

#[tokio::test]
// Tests from "several_chunks.sql",
async fn test_cases_several_chunks_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("several_chunks.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}

#[tokio::test]
// Tests from "sql_information_schema.sql",
async fn test_cases_sql_information_schema_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("sql_information_schema.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}

#[tokio::test]
// Tests from "timestamps.sql",
async fn test_cases_timestamps_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("timestamps.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}

#[tokio::test]
// Tests from "two_chunks.sql",
async fn test_cases_two_chunks_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("two_chunks.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}

#[tokio::test]
// Tests from "two_chunks_missing_columns.sql",
async fn test_cases_two_chunks_missing_columns_sql() {
    test_helpers::maybe_start_logging();

    let input_path = Path::new("cases").join("in").join("two_chunks_missing_columns.sql");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}