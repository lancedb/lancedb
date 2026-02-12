// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Schema evolution operations for LanceDB tables.
//!
//! This module provides functionality to modify the schema of existing tables:
//! - [`add_columns`](execute_add_columns): Add new columns using SQL expressions
//! - [`alter_columns`](execute_alter_columns): Rename columns, change types, or modify nullability
//! - [`drop_columns`](execute_drop_columns): Remove columns from the table

use lance::dataset::{ColumnAlteration, NewColumnTransform};
use serde::{Deserialize, Serialize};

use super::NativeTable;
use crate::Result;

/// The result of an add columns operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AddColumnsResult {
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
}

/// The result of an alter columns operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AlterColumnsResult {
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
}

/// The result of a drop columns operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DropColumnsResult {
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
}

/// Internal implementation of the add columns logic.
///
/// Adds new columns to the table using the provided transforms.
pub(crate) async fn execute_add_columns(
    table: &NativeTable,
    transforms: NewColumnTransform,
    read_columns: Option<Vec<String>>,
) -> Result<AddColumnsResult> {
    table.dataset.ensure_mutable()?;
    let mut dataset = (*table.dataset.get().await?).clone();
    dataset.add_columns(transforms, read_columns, None).await?;
    let version = dataset.version().version;
    table.dataset.update(dataset);
    Ok(AddColumnsResult { version })
}

/// Internal implementation of the alter columns logic.
///
/// Alters existing columns in the table (rename, change type, or modify nullability).
pub(crate) async fn execute_alter_columns(
    table: &NativeTable,
    alterations: &[ColumnAlteration],
) -> Result<AlterColumnsResult> {
    table.dataset.ensure_mutable()?;
    let mut dataset = (*table.dataset.get().await?).clone();
    dataset.alter_columns(alterations).await?;
    let version = dataset.version().version;
    table.dataset.update(dataset);
    Ok(AlterColumnsResult { version })
}

/// Internal implementation of the drop columns logic.
///
/// Removes columns from the table.
pub(crate) async fn execute_drop_columns(
    table: &NativeTable,
    columns: &[&str],
) -> Result<DropColumnsResult> {
    table.dataset.ensure_mutable()?;
    let mut dataset = (*table.dataset.get().await?).clone();
    dataset.drop_columns(columns).await?;
    let version = dataset.version().version;
    table.dataset.update(dataset);
    Ok(DropColumnsResult { version })
}

#[cfg(test)]
mod tests {
    use arrow_array::{record_batch, Int32Array, RecordBatchIterator, StringArray};
    use arrow_schema::DataType;
    use futures::TryStreamExt;
    use lance::dataset::ColumnAlteration;

    use crate::connect;
    use crate::query::{ExecutableQuery, QueryBase, Select};
    use crate::table::NewColumnTransform;

    // Add Columns Tests

    #[tokio::test]
    async fn test_add_columns_with_sql_expression() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("id", Int32, [1, 2, 3, 4, 5])).unwrap();
        let schema = batch.schema();

        let table = conn
            .create_table(
                "test_add_columns",
                RecordBatchIterator::new(vec![Ok(batch)], schema),
            )
            .execute()
            .await
            .unwrap();

        let initial_version = table.version().await.unwrap();

        // Add a computed column
        let result = table
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![("doubled".into(), "id * 2".into())]),
                None,
            )
            .await
            .unwrap();

        // Version should increment
        assert!(result.version > initial_version);

        // Verify the new column exists with correct values
        let batches = table
            .query()
            .select(Select::columns(&["id", "doubled"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let batch = &batches[0];
        let ids: Vec<i32> = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect();
        let doubled: Vec<i32> = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect();

        for (id, d) in ids.iter().zip(doubled.iter()) {
            assert_eq!(*d, id * 2);
        }
    }

    #[tokio::test]
    async fn test_add_multiple_columns() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("x", Int32, [10, 20, 30])).unwrap();
        let schema = batch.schema();

        let table = conn
            .create_table(
                "test_add_multi_columns",
                RecordBatchIterator::new(vec![Ok(batch)], schema),
            )
            .execute()
            .await
            .unwrap();

        // Add multiple columns at once
        table
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![
                    ("y".into(), "x + 1".into()),
                    ("z".into(), "x * x".into()),
                ]),
                None,
            )
            .await
            .unwrap();

        // Verify schema has all columns
        let schema = table.schema().await.unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert!(schema.field_with_name("x").is_ok());
        assert!(schema.field_with_name("y").is_ok());
        assert!(schema.field_with_name("z").is_ok());
    }

    #[tokio::test]
    async fn test_add_column_with_constant_expression() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        let schema = batch.schema();

        let table = conn
            .create_table(
                "test_add_const_column",
                RecordBatchIterator::new(vec![Ok(batch)], schema),
            )
            .execute()
            .await
            .unwrap();

        // Add a column with a constant value
        table
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![("constant".into(), "42".into())]),
                None,
            )
            .await
            .unwrap();

        let schema = table.schema().await.unwrap();
        assert!(schema.field_with_name("constant").is_ok());

        // Verify all values are 42
        let batches = table
            .query()
            .select(Select::columns(&["constant"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let batch = &batches[0];
        let values = batch["constant"]
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .values();
        assert!(values.iter().all(|&v| v == 42));
    }

    // Alter Columns Tests

    #[tokio::test]
    async fn test_alter_column_rename() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("old_name", Int32, [1, 2, 3])).unwrap();
        let schema = batch.schema();

        let table = conn
            .create_table(
                "test_alter_rename",
                RecordBatchIterator::new(vec![Ok(batch)], schema),
            )
            .execute()
            .await
            .unwrap();

        let initial_version = table.version().await.unwrap();

        // Rename the column
        let result = table
            .alter_columns(&[ColumnAlteration::new("old_name".into()).rename("new_name".into())])
            .await
            .unwrap();

        // Version should increment
        assert!(result.version > initial_version);

        // Verify rename
        let schema = table.schema().await.unwrap();
        assert!(schema.field_with_name("old_name").is_err());
        assert!(schema.field_with_name("new_name").is_ok());
    }

    #[tokio::test]
    async fn test_alter_column_set_nullable() {
        use arrow_array::RecordBatch;
        use arrow_schema::{Field, Schema};
        use std::sync::Arc;

        let conn = connect("memory://").execute().await.unwrap();

        // Create a schema with a non-nullable field
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let table = conn
            .create_table(
                "test_alter_nullable",
                RecordBatchIterator::new(vec![Ok(batch)], schema),
            )
            .execute()
            .await
            .unwrap();

        // Initially non-nullable
        let schema = table.schema().await.unwrap();
        assert!(!schema.field_with_name("value").unwrap().is_nullable());

        // Make it nullable
        table
            .alter_columns(&[ColumnAlteration::new("value".into()).set_nullable(true)])
            .await
            .unwrap();

        // Verify it's now nullable
        let schema = table.schema().await.unwrap();
        assert!(schema.field_with_name("value").unwrap().is_nullable());
    }

    #[tokio::test]
    async fn test_alter_column_cast_type() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("num", Int32, [1, 2, 3])).unwrap();
        let schema = batch.schema();

        let table = conn
            .create_table(
                "test_cast_type",
                RecordBatchIterator::new(vec![Ok(batch)], schema),
            )
            .execute()
            .await
            .unwrap();

        // Cast Int32 to Int64 (a supported cast)
        table
            .alter_columns(&[ColumnAlteration::new("num".into()).cast_to(DataType::Int64)])
            .await
            .unwrap();

        // Verify type changed
        let schema = table.schema().await.unwrap();
        assert_eq!(
            schema.field_with_name("num").unwrap().data_type(),
            &DataType::Int64
        );

        // Query the data and verify the returned type is correct
        let batches = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let batch = &batches[0];
        let values = batch["num"]
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .values();
        assert_eq!(values.as_ref(), &[1i64, 2, 3]);
    }

    #[tokio::test]
    async fn test_alter_column_invalid_cast_fails() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("num", Int32, [1, 2, 3])).unwrap();
        let schema = batch.schema();

        let table = conn
            .create_table(
                "test_invalid_cast",
                RecordBatchIterator::new(vec![Ok(batch)], schema),
            )
            .execute()
            .await
            .unwrap();

        // Casting Int32 to Float64 is not supported
        let result = table
            .alter_columns(&[ColumnAlteration::new("num".into()).cast_to(DataType::Float64)])
            .await;
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("cast"),
            "Expected error message to contain 'cast', got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_alter_multiple_columns() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("a", Int32, [1, 2, 3]), ("b", Int32, [4, 5, 6])).unwrap();
        let schema = batch.schema();

        let table = conn
            .create_table(
                "test_alter_multi",
                RecordBatchIterator::new(vec![Ok(batch)], schema),
            )
            .execute()
            .await
            .unwrap();

        // Alter multiple columns at once
        table
            .alter_columns(&[
                ColumnAlteration::new("a".into()).rename("alpha".into()),
                ColumnAlteration::new("b".into()).set_nullable(true),
            ])
            .await
            .unwrap();

        let schema = table.schema().await.unwrap();
        assert!(schema.field_with_name("alpha").is_ok());
        assert!(schema.field_with_name("a").is_err());
        assert!(schema.field_with_name("b").unwrap().is_nullable());
    }

    // Drop Columns Tests

    #[tokio::test]
    async fn test_drop_single_column() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch =
            record_batch!(("keep", Int32, [1, 2, 3]), ("remove", Int32, [4, 5, 6])).unwrap();
        let schema = batch.schema();

        let table = conn
            .create_table(
                "test_drop_single",
                RecordBatchIterator::new(vec![Ok(batch)], schema),
            )
            .execute()
            .await
            .unwrap();

        let initial_version = table.version().await.unwrap();

        // Drop a column
        let result = table.drop_columns(&["remove"]).await.unwrap();

        // Version should increment
        assert!(result.version > initial_version);

        // Verify column was dropped
        let schema = table.schema().await.unwrap();
        assert_eq!(schema.fields().len(), 1);
        assert!(schema.field_with_name("keep").is_ok());
        assert!(schema.field_with_name("remove").is_err());
    }

    #[tokio::test]
    async fn test_drop_multiple_columns() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(
            ("a", Int32, [1, 2]),
            ("b", Int32, [3, 4]),
            ("c", Int32, [5, 6]),
            ("d", Int32, [7, 8])
        )
        .unwrap();
        let schema = batch.schema();

        let table = conn
            .create_table(
                "test_drop_multi",
                RecordBatchIterator::new(vec![Ok(batch)], schema),
            )
            .execute()
            .await
            .unwrap();

        // Drop multiple columns
        table.drop_columns(&["b", "d"]).await.unwrap();

        // Verify only a and c remain
        let schema = table.schema().await.unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert!(schema.field_with_name("a").is_ok());
        assert!(schema.field_with_name("c").is_ok());
        assert!(schema.field_with_name("b").is_err());
        assert!(schema.field_with_name("d").is_err());
    }

    #[tokio::test]
    async fn test_drop_column_preserves_data() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(
            ("id", Int32, [1, 2, 3]),
            ("name", Utf8, ["a", "b", "c"]),
            ("extra", Int32, [10, 20, 30])
        )
        .unwrap();
        let schema = batch.schema();

        let table = conn
            .create_table(
                "test_drop_preserves",
                RecordBatchIterator::new(vec![Ok(batch)], schema),
            )
            .execute()
            .await
            .unwrap();

        // Drop the extra column
        table.drop_columns(&["extra"]).await.unwrap();

        // Verify remaining data is intact
        let batches = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);

        let ids: Vec<i32> = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect();
        assert_eq!(ids, vec![1, 2, 3]);

        let names: Vec<&str> = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    // Error Case Tests

    #[tokio::test]
    async fn test_drop_nonexistent_column_fails() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("existing", Int32, [1, 2, 3])).unwrap();
        let schema = batch.schema();

        let table = conn
            .create_table(
                "test_drop_nonexistent",
                RecordBatchIterator::new(vec![Ok(batch)], schema),
            )
            .execute()
            .await
            .unwrap();

        // Try to drop a column that doesn't exist
        let result = table.drop_columns(&["nonexistent"]).await;
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("nonexistent"),
            "Expected error message to contain column name 'nonexistent', got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_alter_nonexistent_column_fails() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("existing", Int32, [1, 2, 3])).unwrap();
        let schema = batch.schema();

        let table = conn
            .create_table(
                "test_alter_nonexistent",
                RecordBatchIterator::new(vec![Ok(batch)], schema),
            )
            .execute()
            .await
            .unwrap();

        // Try to alter a column that doesn't exist
        let result = table
            .alter_columns(&[ColumnAlteration::new("nonexistent".into()).rename("new".into())])
            .await;
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("nonexistent"),
            "Expected error message to contain column name 'nonexistent', got: {}",
            err
        );
    }

    // Version Tracking Tests

    #[tokio::test]
    async fn test_schema_operations_increment_version() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("a", Int32, [1, 2, 3]), ("b", Int32, [4, 5, 6])).unwrap();
        let schema = batch.schema();

        let table = conn
            .create_table(
                "test_version_increment",
                RecordBatchIterator::new(vec![Ok(batch)], schema),
            )
            .execute()
            .await
            .unwrap();

        let v1 = table.version().await.unwrap();

        // Add column increments version
        let add_result = table
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![("c".into(), "a + b".into())]),
                None,
            )
            .await
            .unwrap();
        assert!(add_result.version > v1);
        let v2 = table.version().await.unwrap();
        assert_eq!(add_result.version, v2);

        // Alter column increments version
        let alter_result = table
            .alter_columns(&[ColumnAlteration::new("c".into()).rename("sum".into())])
            .await
            .unwrap();
        assert!(alter_result.version > v2);
        let v3 = table.version().await.unwrap();
        assert_eq!(alter_result.version, v3);

        // Drop column increments version
        let drop_result = table.drop_columns(&["b"]).await.unwrap();
        assert!(drop_result.version > v3);
        let v4 = table.version().await.unwrap();
        assert_eq!(drop_result.version, v4);
    }
}
