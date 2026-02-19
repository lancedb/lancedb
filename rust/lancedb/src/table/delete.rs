// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
use serde::{Deserialize, Serialize};

use super::NativeTable;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DeleteResult {
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
}

/// Internal implementation of the delete logic
///
/// This logic was moved from NativeTable::delete to keep table.rs clean.
pub(crate) async fn execute_delete(table: &NativeTable, predicate: &str) -> Result<DeleteResult> {
    table.dataset.ensure_mutable()?;
    let mut dataset = (*table.dataset.get().await?).clone();
    dataset.delete(predicate).await?;
    let version = dataset.version().version;
    table.dataset.update(dataset);
    Ok(DeleteResult { version })
}

#[cfg(test)]
mod tests {
    use crate::connect;
    use arrow_array::{record_batch, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    use crate::query::ExecutableQuery;
    use futures::TryStreamExt;
    #[tokio::test]
    async fn test_delete_simple() {
        let conn = connect("memory://").execute().await.unwrap();

        // 1. Create a table with values 0 to 9
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap();

        let table = conn
            .create_table("test_delete", batch)
            .execute()
            .await
            .unwrap();

        // 2. Verify initial state
        assert_eq!(table.count_rows(None).await.unwrap(), 10);

        // 3. Execute Delete (removes values > 5)
        table.delete("i > 5").await.unwrap();

        // 4. Verify results
        assert_eq!(table.count_rows(None).await.unwrap(), 6); // 0, 1, 2, 3, 4, 5 remain

        // 5. Verify specific data consistency
        let batches = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let batch = &batches[0];
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Ensure no value > 5 exists
        for val in array.iter() {
            assert!(val.unwrap() <= 5);
        }
    }
    #[tokio::test]
    async fn rows_removed_schema_same() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(
            ("id", Int32, [1, 2, 3, 4, 5]),
            ("name", Utf8, ["a", "b", "c", "d", "e"])
        )
        .unwrap();
        let original_schema = batch.schema();

        let table = conn
            .create_table("test_delete_all", batch)
            .execute()
            .await
            .unwrap();

        table.delete("true").await.unwrap();

        assert_eq!(table.count_rows(None).await.unwrap(), 0);

        let current_schema = table.schema().await.unwrap();
        //check if the original schema is the same as current
        assert_eq!(current_schema, original_schema);
    }

    #[tokio::test]
    async fn test_delete_false_increments_version() {
        let conn = connect("memory://").execute().await.unwrap();

        // Create a table with 5 rows
        let batch = record_batch!(("id", Int32, [1, 2, 3, 4, 5])).unwrap();

        let table = conn
            .create_table("test_delete_noop", batch)
            .execute()
            .await
            .unwrap();

        // Capture the initial state (Rows = 5, Version = 1)
        let initial_rows = table.count_rows(None).await.unwrap();
        let initial_version = table.version().await.unwrap();

        assert_eq!(initial_rows, 5);
        table.delete("false").await.unwrap();

        // Rows should still be 5
        let current_rows = table.count_rows(None).await.unwrap();
        assert_eq!(
            current_rows, initial_rows,
            "Data should not change when predicate is false"
        );

        // version check
        let current_version = table.version().await.unwrap();
        assert!(
            current_version > initial_version,
            "Table version must increment after delete operation"
        );
    }
}
