// Tests for NamespaceBackedDatabase with DirectoryNamespace

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lancedb::connect_namespace;
use lancedb::database::CreateTableMode;
use lancedb::query::ExecutableQuery;
use tempfile::tempdir;

/// Helper function to create test data
fn create_test_data(
) -> RecordBatchIterator<std::vec::IntoIter<Result<RecordBatch, arrow_schema::ArrowError>>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(name_array)],
    )
    .unwrap();
    RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema)
}

#[tokio::test]
async fn test_namespace_create_table_basic() {
    // Setup: Create a temporary directory for the namespace
    let tmp_dir = tempdir().unwrap();
    let root_path = tmp_dir.path().to_str().unwrap().to_string();

    // Connect to namespace using DirectoryNamespace
    let mut properties = HashMap::new();
    properties.insert("root".to_string(), root_path.clone());

    let conn = connect_namespace("dir", properties)
        .await
        .expect("Failed to connect to namespace");

    // Test: Create a table
    let test_data = create_test_data();
    let table = conn
        .create_table("test_table", test_data)
        .execute()
        .await
        .expect("Failed to create table");

    // Verify: Table was created and can be queried
    let results = table
        .query()
        .execute()
        .await
        .expect("Failed to query table")
        .try_collect::<Vec<_>>()
        .await
        .expect("Failed to collect results");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 5);

    // Verify: Table appears in table_names
    let table_names = conn
        .table_names()
        .execute()
        .await
        .expect("Failed to list tables");
    assert!(table_names.contains(&"test_table".to_string()));
}

#[tokio::test]
async fn test_namespace_describe_table() {
    // Setup: Create a temporary directory for the namespace
    let tmp_dir = tempdir().unwrap();
    let root_path = tmp_dir.path().to_str().unwrap().to_string();

    // Connect to namespace
    let mut properties = HashMap::new();
    properties.insert("root".to_string(), root_path.clone());

    let conn = connect_namespace("dir", properties)
        .await
        .expect("Failed to connect to namespace");

    // Create a table first
    let test_data = create_test_data();
    let _table = conn
        .create_table("describe_test", test_data)
        .execute()
        .await
        .expect("Failed to create table");

    // Test: Open the table (which internally uses describe_table)
    let opened_table = conn
        .open_table("describe_test")
        .execute()
        .await
        .expect("Failed to open table");

    // Verify: Can query the opened table
    let results = opened_table
        .query()
        .execute()
        .await
        .expect("Failed to query table")
        .try_collect::<Vec<_>>()
        .await
        .expect("Failed to collect results");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 5);

    // Verify schema matches
    let schema = opened_table.schema().await.expect("Failed to get schema");
    assert_eq!(schema.fields.len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
}

#[tokio::test]
async fn test_namespace_create_table_overwrite_mode() {
    // Setup: Create a temporary directory for the namespace
    let tmp_dir = tempdir().unwrap();
    let root_path = tmp_dir.path().to_str().unwrap().to_string();

    let mut properties = HashMap::new();
    properties.insert("root".to_string(), root_path.clone());

    let conn = connect_namespace("dir", properties)
        .await
        .expect("Failed to connect to namespace");

    // Create initial table with 5 rows
    let test_data1 = create_test_data();
    let _table1 = conn
        .create_table("overwrite_test", test_data1)
        .execute()
        .await
        .expect("Failed to create table");

    // Create new data with 3 rows
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let id_array = Int32Array::from(vec![10, 20, 30]);
    let name_array = StringArray::from(vec!["New1", "New2", "New3"]);
    let test_data2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(name_array)],
    )
    .unwrap();

    // Test: Overwrite the table
    let table2 = conn
        .create_table(
            "overwrite_test",
            RecordBatchIterator::new(vec![Ok(test_data2)].into_iter(), schema),
        )
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .expect("Failed to overwrite table");

    // Verify: Table has new data (3 rows instead of 5)
    let results = table2
        .query()
        .execute()
        .await
        .expect("Failed to query table")
        .try_collect::<Vec<_>>()
        .await
        .expect("Failed to collect results");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 3);

    // Verify the data is actually the new data
    let id_col = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(id_col.value(0), 10);
    assert_eq!(id_col.value(1), 20);
    assert_eq!(id_col.value(2), 30);
}

#[tokio::test]
async fn test_namespace_create_table_exist_ok_mode() {
    // Setup: Create a temporary directory for the namespace
    let tmp_dir = tempdir().unwrap();
    let root_path = tmp_dir.path().to_str().unwrap().to_string();

    let mut properties = HashMap::new();
    properties.insert("root".to_string(), root_path.clone());

    let conn = connect_namespace("dir", properties)
        .await
        .expect("Failed to connect to namespace");

    // Create initial table with test data
    let test_data1 = create_test_data();
    let _table1 = conn
        .create_table("exist_ok_test", test_data1)
        .execute()
        .await
        .expect("Failed to create table");

    // Try to create again with exist_ok mode
    let test_data2 = create_test_data();
    let table2 = conn
        .create_table("exist_ok_test", test_data2)
        .mode(CreateTableMode::exist_ok(|req| req))
        .execute()
        .await
        .expect("Failed with exist_ok mode");

    // Verify: Table still has original data (5 rows)
    let results = table2
        .query()
        .execute()
        .await
        .expect("Failed to query table")
        .try_collect::<Vec<_>>()
        .await
        .expect("Failed to collect results");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 5);
}

#[tokio::test]
async fn test_namespace_create_multiple_tables() {
    // Setup: Create a temporary directory for the namespace
    let tmp_dir = tempdir().unwrap();
    let root_path = tmp_dir.path().to_str().unwrap().to_string();

    let mut properties = HashMap::new();
    properties.insert("root".to_string(), root_path.clone());

    let conn = connect_namespace("dir", properties)
        .await
        .expect("Failed to connect to namespace");

    // Create first table
    let test_data1 = create_test_data();
    let _table1 = conn
        .create_table("table1", test_data1)
        .execute()
        .await
        .expect("Failed to create first table");

    // Create second table
    let test_data2 = create_test_data();
    let _table2 = conn
        .create_table("table2", test_data2)
        .execute()
        .await
        .expect("Failed to create second table");

    // Verify: Both tables appear in table list
    let table_names = conn
        .table_names()
        .execute()
        .await
        .expect("Failed to list tables");

    assert!(table_names.contains(&"table1".to_string()));
    assert!(table_names.contains(&"table2".to_string()));

    // Verify: Can open both tables
    let opened_table1 = conn
        .open_table("table1")
        .execute()
        .await
        .expect("Failed to open table1");

    let opened_table2 = conn
        .open_table("table2")
        .execute()
        .await
        .expect("Failed to open table2");

    // Verify both tables work
    let count1 = opened_table1
        .count_rows(None)
        .await
        .expect("Failed to count rows in table1");
    assert_eq!(count1, 5);

    let count2 = opened_table2
        .count_rows(None)
        .await
        .expect("Failed to count rows in table2");
    assert_eq!(count2, 5);
}

#[tokio::test]
async fn test_namespace_table_not_found() {
    // Setup: Create a temporary directory for the namespace
    let tmp_dir = tempdir().unwrap();
    let root_path = tmp_dir.path().to_str().unwrap().to_string();

    let mut properties = HashMap::new();
    properties.insert("root".to_string(), root_path.clone());

    let conn = connect_namespace("dir", properties)
        .await
        .expect("Failed to connect to namespace");

    // Test: Try to open a non-existent table
    let result = conn.open_table("non_existent_table").execute().await;

    // Verify: Should return an error
    assert!(result.is_err());
}

#[tokio::test]
async fn test_namespace_drop_table() {
    // Setup: Create a temporary directory for the namespace
    let tmp_dir = tempdir().unwrap();
    let root_path = tmp_dir.path().to_str().unwrap().to_string();

    let mut properties = HashMap::new();
    properties.insert("root".to_string(), root_path.clone());

    let conn = connect_namespace("dir", properties)
        .await
        .expect("Failed to connect to namespace");

    // Create a table first
    let test_data = create_test_data();
    let _table = conn
        .create_table("drop_test", test_data)
        .execute()
        .await
        .expect("Failed to create table");

    // Verify table exists
    let table_names_before = conn
        .table_names()
        .execute()
        .await
        .expect("Failed to list tables");
    assert!(table_names_before.contains(&"drop_test".to_string()));

    // Test: Drop the table
    conn.drop_table("drop_test", &[])
        .await
        .expect("Failed to drop table");

    // Verify: Table no longer exists
    let table_names_after = conn
        .table_names()
        .execute()
        .await
        .expect("Failed to list tables");
    assert!(!table_names_after.contains(&"drop_test".to_string()));

    // Verify: Cannot open dropped table
    let open_result = conn.open_table("drop_test").execute().await;
    assert!(open_result.is_err());
}
