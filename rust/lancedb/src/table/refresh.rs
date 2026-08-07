// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Filling computed columns.

use arrow_schema::Schema as ArrowSchema;
use lance::dataset::UpdateBuilder as LanceUpdateBuilder;
use serde::{Deserialize, Serialize};

use super::NativeTable;
use super::computed_columns::{ComputedColumnKind, computed_column_from_field};
use crate::{Error, Result};

/// The result of refreshing a computed column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RefreshColumnResult {
    /// Rows that had a value computed.
    #[serde(default)]
    pub rows_filled: u64,
    /// The commit version associated with the operation.
    #[serde(default)]
    pub version: u64,
}

/// Internal implementation of the refresh logic.
pub(crate) async fn execute_refresh_column(
    table: &NativeTable,
    column: &str,
) -> Result<RefreshColumnResult> {
    table.dataset.ensure_mutable()?;
    let dataset = table.dataset.get().await?;

    let schema = ArrowSchema::from(dataset.schema());
    let field = schema
        .field_with_name(column)
        .map_err(|_| Error::ColumnNotFound {
            name: column.to_string(),
        })?;
    let declaration =
        computed_column_from_field(field).ok_or_else(|| Error::NotAComputedColumn {
            name: column.to_string(),
        })?;
    let expression = match &declaration.kind {
        ComputedColumnKind::Sql { expression } => expression,
        ComputedColumnKind::Unrecognized { kind } => {
            return Err(Error::NotSupported {
                message: format!(
                    "computed column '{column}' is defined by '{kind}', which this version of \
                     lancedb cannot evaluate"
                ),
            });
        }
    };

    // Rows still holding no value are the ones to fill. A row whose expression
    // evaluates to null is indistinguishable from an unfilled one and is
    // recomputed, which costs work but cannot change the result.
    let builder = LanceUpdateBuilder::new(dataset)
        .update_where(&format!("{column} IS NULL"))?
        .set(column, expression)?;
    let result = builder.build()?.execute().await?;

    let version = result.new_dataset.version().version;
    table.dataset.update(result.new_dataset.as_ref().clone());
    Ok(RefreshColumnResult {
        rows_filled: result.rows_updated,
        version,
    })
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, record_batch};
    use futures::TryStreamExt;

    use crate::connect;
    use crate::query::{ExecutableQuery, QueryBase, Select};
    use crate::{Error, Result, Table};

    async fn table_with(name: &str, values: Vec<i32>) -> Table {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("x", Int32, values)).unwrap();
        conn.create_table(name, batch).execute().await.unwrap()
    }

    async fn declare_doubled(table: &Table) -> Result<u64> {
        Ok(table
            .add_columns()
            .computed("doubled", "x * 2")
            .execute()
            .await?
            .version)
    }

    async fn read(table: &Table, column: &str) -> Vec<Option<i32>> {
        let batches = table
            .query()
            .select(Select::columns(&[column]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut values: Vec<Option<i32>> = batches
            .iter()
            .flat_map(|batch| {
                batch[column]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect();
        values.sort();
        values
    }

    async fn append(table: &Table, values: Vec<i32>) {
        let batch = record_batch!(("x", Int32, values)).unwrap();
        table.add(batch).execute().await.unwrap();
    }

    #[tokio::test]
    async fn test_refresh_fills_a_declared_column() {
        let table = table_with("refresh_fills", vec![1, 2, 3]).await;
        let declared = declare_doubled(&table).await.unwrap();
        assert_eq!(read(&table, "doubled").await, vec![None, None, None]);

        let result = table.refresh_column("doubled").await.unwrap();
        assert!(result.version > declared);
        assert_eq!(result.rows_filled, 3);
        assert_eq!(
            read(&table, "doubled").await,
            vec![Some(2), Some(4), Some(6)]
        );
    }

    /// Values written after the last refresh must be reachable by another one.
    #[tokio::test]
    async fn test_refresh_fills_rows_appended_since_the_last_refresh() {
        let table = table_with("refresh_appended", vec![1, 2]).await;
        declare_doubled(&table).await.unwrap();
        table.refresh_column("doubled").await.unwrap();

        append(&table, vec![5, 6]).await;
        assert_eq!(
            read(&table, "doubled").await,
            vec![None, None, Some(2), Some(4)]
        );

        let result = table.refresh_column("doubled").await.unwrap();
        assert_eq!(result.rows_filled, 2);
        assert_eq!(
            read(&table, "doubled").await,
            vec![Some(2), Some(4), Some(10), Some(12)]
        );
    }

    #[tokio::test]
    async fn test_refresh_with_nothing_to_fill() {
        let table = table_with("refresh_noop", vec![1, 2, 3]).await;
        declare_doubled(&table).await.unwrap();
        table.refresh_column("doubled").await.unwrap();

        let again = table.refresh_column("doubled").await.unwrap();
        assert_eq!(again.rows_filled, 0);
        assert_eq!(
            read(&table, "doubled").await,
            vec![Some(2), Some(4), Some(6)]
        );
    }

    #[tokio::test]
    async fn test_refresh_leaves_deleted_rows_alone() {
        let table = table_with("refresh_deleted", vec![1, 2, 3, 4]).await;
        declare_doubled(&table).await.unwrap();
        table.delete("x = 2").await.unwrap();

        let result = table.refresh_column("doubled").await.unwrap();
        assert_eq!(result.rows_filled, 3);
        assert_eq!(
            read(&table, "doubled").await,
            vec![Some(2), Some(6), Some(8)]
        );
    }

    #[tokio::test]
    async fn test_refresh_a_constant_expression() {
        let table = table_with("refresh_constant", vec![1, 2, 3]).await;
        table
            .add_columns()
            .computed("answer", "42")
            .execute()
            .await
            .unwrap();

        let result = table.refresh_column("answer").await.unwrap();
        assert_eq!(result.rows_filled, 3);
    }

    #[tokio::test]
    async fn test_refresh_rejects_a_plain_column() {
        let table = table_with("refresh_plain", vec![1, 2, 3]).await;
        let err = table.refresh_column("x").await.unwrap_err();
        assert!(matches!(err, Error::NotAComputedColumn { name } if name == "x"));
    }

    #[tokio::test]
    async fn test_refresh_rejects_an_unknown_column() {
        let table = table_with("refresh_missing", vec![1, 2, 3]).await;
        let err = table.refresh_column("nope").await.unwrap_err();
        assert!(matches!(err, Error::ColumnNotFound { name } if name == "nope"));
    }

    /// A declaration of a kind this version cannot evaluate is refused by
    /// name, rather than mistaken for a plain column or fed to the SQL path.
    #[tokio::test]
    async fn test_refresh_rejects_a_kind_it_cannot_evaluate() {
        let table = table_with("refresh_foreign", vec![1, 2, 3]).await;
        super::super::computed_columns::add_foreign_kind(&table, "embedding", "udf").await;

        let err = table.refresh_column("embedding").await.unwrap_err();
        assert!(matches!(err, Error::NotSupported { message } if message.contains("udf")));
    }
}
