use std::sync::Arc;

use futures::FutureExt;
use lance::dataset::DeleteBuilder;
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
use serde::{Deserialize, Serialize};

use super::{NativeTable, Predicate};
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DeleteResult {
    /// The number of rows that were deleted.
    #[serde(default)]
    pub num_deleted_rows: u64,
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
}

/// Internal implementation of the delete logic
///
/// This logic was moved from NativeTable::delete to keep table.rs clean.
pub(crate) async fn execute_delete(
    table: &NativeTable,
    predicate: Predicate<'_>,
) -> Result<DeleteResult> {
    table.dataset.ensure_mutable()?;
    table.ensure_not_a_view("delete from").await?;
    match predicate {
        Predicate::String(s) => {
            let predicate = crate::expr::canonicalize_sql_predicate(s)?;
            let mut dataset = (*table.dataset.get().await?).clone();
            let delete_result = dataset.delete(&predicate).boxed().await?;
            let num_deleted_rows = delete_result.num_deleted_rows;
            let version = dataset.version().version;
            table.dataset.update(dataset);
            Ok(DeleteResult {
                num_deleted_rows,
                version,
            })
        }
        Predicate::Expr(expr) => {
            let dataset = table.dataset.get().await?;
            let delete_result = DeleteBuilder::from_expr(Arc::clone(&dataset), expr.clone())
                .execute()
                .await?;
            let num_deleted_rows = delete_result.num_deleted_rows;
            let version = delete_result.new_dataset.version().version;
            table.dataset.update(
                Arc::try_unwrap(delete_result.new_dataset).unwrap_or_else(|arc| (*arc).clone()),
            );
            Ok(DeleteResult {
                num_deleted_rows,
                version,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        Array, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, StringArray, record_batch,
    };
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use lance_index::scalar::FullTextSearchQuery;

    use crate::Table;
    use crate::connect;
    use crate::index::vector::IvfFlatIndexBuilder;
    use crate::index::{Index, IndexType};
    use crate::query::{ExecutableQuery, QueryBase};
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
    async fn test_delete_returns_num_deleted_rows() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("id", Int32, [1, 2, 3, 4, 5])).unwrap();
        let table = conn
            .create_table("test_num_deleted", batch)
            .execute()
            .await
            .unwrap();

        // Delete 2 rows (id > 3 means id=4 and id=5)
        let result = table.delete("id > 3").await.unwrap();
        assert_eq!(result.num_deleted_rows, 2);
        assert_eq!(table.count_rows(None).await.unwrap(), 3);

        // Delete 0 rows (no rows match)
        let result = table.delete("id > 100").await.unwrap();
        assert_eq!(result.num_deleted_rows, 0);
        assert_eq!(table.count_rows(None).await.unwrap(), 3);

        // Delete remaining rows
        let result = table.delete("true").await.unwrap();
        assert_eq!(result.num_deleted_rows, 3);
        assert_eq!(table.count_rows(None).await.unwrap(), 0);
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

    #[tokio::test]
    async fn test_delete_expr() {
        use datafusion_expr::{col, lit};

        let conn = connect("memory://").execute().await.unwrap();

        // 1. Create a table with values 0 to 9
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap();

        let table = conn
            .create_table("test_delete_expr", batch)
            .execute()
            .await
            .unwrap();

        // 2. Verify initial state
        assert_eq!(table.count_rows(None).await.unwrap(), 10);
        let initial_version = table.version().await.unwrap();

        // 3. Execute Delete with Expr (removes values > 5)
        let expr = col("i").gt(lit(5));
        table.delete(&expr).await.unwrap();

        // 4. Verify results
        assert_eq!(table.count_rows(None).await.unwrap(), 6); // 0, 1, 2, 3, 4, 5 remain
        let current_version = table.version().await.unwrap();
        assert!(
            current_version > initial_version,
            "Table version must increment after delete_expr operation"
        );

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
    async fn test_delete_expr_increments_version() {
        use datafusion_expr::lit;

        let conn = connect("memory://").execute().await.unwrap();

        // Create a table with 5 rows
        let batch = record_batch!(("id", Int32, [1, 2, 3, 4, 5])).unwrap();

        let table = conn
            .create_table("test_delete_expr_noop", batch)
            .execute()
            .await
            .unwrap();

        // Capture the initial state (Rows = 5, Version = 1)
        let initial_rows = table.count_rows(None).await.unwrap();
        let initial_version = table.version().await.unwrap();

        assert_eq!(initial_rows, 5);
        let expr = lit(false);
        table.delete(&expr).await.unwrap();

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
            "Table version must increment after delete_expr operation"
        );
    }

    async fn listed(table: &Table) -> Vec<(String, IndexType)> {
        let mut indices = table.list_indices().await.unwrap();
        indices.sort_by(|left, right| left.name.cmp(&right.name));
        indices
            .into_iter()
            .map(|index| (index.name, index.index_type))
            .collect()
    }

    async fn add_index(table: &Table, column: &str, index: Index, name: &str) {
        table
            .create_index(&[column], index)
            .name(name.to_string())
            .execute()
            .await
            .unwrap();
    }

    async fn ids(stream: crate::arrow::SendableRecordBatchStream) -> Vec<i32> {
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let mut values = Vec::new();
        for batch in batches {
            let column = batch.column_by_name("id").unwrap();
            let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
            values.extend(array.values().iter().copied());
        }
        values.sort_unstable();
        values
    }

    async fn filtered(table: &Table, predicate: &str) -> Vec<i32> {
        let query = table.query().only_if(predicate).limit(100);
        ids(query.execute().await.unwrap()).await
    }

    async fn matched(table: &Table, text: &str) -> Vec<i32> {
        let query = FullTextSearchQuery::new(text.into());
        let query = query.with_column("text".into()).unwrap();
        let search = table.query().full_text_search(query).limit(100);
        ids(search.execute().await.unwrap()).await
    }

    async fn nearest(table: &Table, id: i32) -> Vec<i32> {
        let vector = vec![id as f32, 0.0, 0.0, 0.0];
        let query = table.query().nearest_to(vector).unwrap().limit(1);
        ids(query.execute().await.unwrap()).await
    }

    fn rows(ids: &[i32], category_of: impl Fn(i32) -> &'static str) -> RecordBatch {
        let flat = ids
            .iter()
            .flat_map(|id| [*id as f32, 0.0, 0.0, 0.0])
            .collect::<Vec<_>>();
        let vector_values = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            4,
            Arc::new(Float32Array::from(flat)),
            None,
        )
        .unwrap();
        let categories: Vec<&str> = ids.iter().copied().map(category_of).collect();
        let texts: Vec<String> = ids.iter().map(|id| format!("row{id}")).collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("category", DataType::Utf8, false),
                Field::new("text", DataType::Utf8, false),
                Field::new("vector", vector_values.data_type().clone(), false),
            ])),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(categories)),
                Arc::new(StringArray::from(texts)),
                Arc::new(vector_values),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn delete_keeps_vector_scalar_and_fts_indices() {
        let conn = connect("memory://").execute().await.unwrap();
        let ids: Vec<i32> = (0..16).collect();
        let batch = rows(&ids, |id| if id < 8 { "keep" } else { "drop" });
        let table = conn
            .create_table("indexed_delete", batch)
            .execute()
            .await
            .unwrap();
        let ivf = Index::IvfFlat(IvfFlatIndexBuilder::default().num_partitions(1));
        let btree = Index::BTree(Default::default());
        let fts = Index::FTS(Default::default());
        add_index(&table, "vector", ivf, "vector_idx").await;
        add_index(&table, "category", btree, "category_idx").await;
        add_index(&table, "text", fts, "text_idx").await;
        let expected = vec![
            ("category_idx".to_string(), IndexType::BTree),
            ("text_idx".to_string(), IndexType::FTS),
            ("vector_idx".to_string(), IndexType::IvfFlat),
        ];

        let deleted = table.delete("category = 'drop'").await.unwrap();
        assert_eq!(deleted.num_deleted_rows, 8);
        assert_eq!(listed(&table).await, expected);
        for index in table.list_indices().await.unwrap() {
            assert!(index.num_indexed_rows.unwrap_or(0) > 0);
            let stats = table.index_stats(&index.name).await.unwrap().unwrap();
            assert!(stats.num_indexed_rows > 0);
        }
        let kept = (0..8).collect::<Vec<_>>();
        assert_eq!(filtered(&table, "category = 'keep'").await, kept);
        assert_eq!(matched(&table, "row0").await, vec![0]);
        assert!(matched(&table, "row8").await.is_empty());
        assert_eq!(nearest(&table, 0).await, vec![0]);

        let deleted = table.delete("true").await.unwrap();
        assert_eq!(deleted.num_deleted_rows, 8);
        assert_eq!(table.count_rows(None).await.unwrap(), 0);
        assert_eq!(listed(&table).await, expected);

        table.add(rows(&[20], |_| "fresh")).execute().await.unwrap();
        assert_eq!(listed(&table).await, expected);
        assert_eq!(filtered(&table, "category = 'fresh'").await, vec![20]);
        assert_eq!(matched(&table, "row20").await, vec![20]);
        assert!(matched(&table, "row0").await.is_empty());
        assert_eq!(nearest(&table, 20).await, vec![20]);
    }
}
