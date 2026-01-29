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
    // We access the dataset from the table. Since this is in the same module hierarchy (super),
    // and 'dataset' is pub(crate), we can access it.
    let mut dataset = table.dataset.get_mut().await?;

    // Perform the actual delete on the Lance dataset
    dataset.delete(predicate).await?;

    // Return the result with the new version
    Ok(DeleteResult {
        version: dataset.version().version,
    })
}

#[cfg(test)]
mod tests {
    use crate::connect;
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    use crate::query::ExecutableQuery;
    use futures::TryStreamExt;
    #[tokio::test]
    async fn test_delete_simple() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        // 1. Create a table with values 0 to 9
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap();

        let table = conn
            .create_table(
                "test_delete",
                RecordBatchIterator::new(vec![Ok(batch)], schema),
            )
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
}
