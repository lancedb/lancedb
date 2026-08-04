// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Builder for adding columns to a table.

use std::sync::Arc;

use lance::dataset::NewColumnTransform;

use super::BaseTable;
use super::schema_evolution::AddColumnsResult;
use crate::{Error, Result};

/// Adds columns to a table. See [`Table::add_columns`](super::Table::add_columns).
pub struct AddColumnsBuilder {
    parent: Arc<dyn BaseTable>,
    transform: Option<NewColumnTransform>,
    read_columns: Option<Vec<String>>,
}

impl std::fmt::Debug for AddColumnsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddColumnsBuilder")
            .field("parent", &self.parent)
            .field("has_transform", &self.transform.is_some())
            .field("read_columns", &self.read_columns)
            .finish()
    }
}

impl AddColumnsBuilder {
    pub(crate) fn new(parent: Arc<dyn BaseTable>) -> Self {
        Self {
            parent,
            transform: None,
            read_columns: None,
        }
    }

    /// Set how the new columns' values are produced. Required.
    pub fn transform(mut self, transform: NewColumnTransform) -> Self {
        self.transform = Some(transform);
        self
    }

    /// Limit which existing columns a [`NewColumnTransform::BatchUDF`] mapper
    /// receives. Every other transform determines what it reads, so setting
    /// this alongside one is an error rather than a silent no-op.
    pub fn read_columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.read_columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Add the columns.
    pub async fn execute(self) -> Result<AddColumnsResult> {
        let Self {
            parent,
            transform,
            read_columns,
        } = self;

        let Some(transform) = transform else {
            return Err(Error::InvalidInput {
                message: "add_columns requires a transform".into(),
            });
        };

        if read_columns.is_some() && !matches!(transform, NewColumnTransform::BatchUDF(_)) {
            return Err(Error::InvalidInput {
                message: "read_columns applies only to a BatchUDF transform; \
                          every other transform determines what it reads"
                    .into(),
            });
        }

        parent.add_columns(transform, read_columns).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, record_batch};
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::{BatchUDF, NewColumnTransform};

    use crate::Table;
    use crate::connect;

    async fn table_with_two_columns(name: &str) -> Table {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("x", Int32, [1, 2, 3]), ("y", Int32, [10, 20, 30])).unwrap();
        conn.create_table(name, batch).execute().await.unwrap()
    }

    #[tokio::test]
    async fn test_requires_a_transform() {
        let table = table_with_two_columns("no_transform").await;
        let err = table.add_columns().execute().await.unwrap_err();
        assert!(
            err.to_string().contains("requires a transform"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn test_read_columns_with_sql_expressions_is_rejected() {
        let table = table_with_two_columns("read_cols_sql").await;
        let err = table
            .add_columns()
            .transform(NewColumnTransform::SqlExpressions(vec![(
                "doubled".into(),
                "x * 2".into(),
            )]))
            .read_columns(["x"])
            .execute()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("BatchUDF"), "got: {err}");

        let schema = table.schema().await.unwrap();
        assert!(
            schema.field_with_name("doubled").is_err(),
            "a rejected call must not commit"
        );
    }

    #[tokio::test]
    async fn test_read_columns_limits_what_a_batch_udf_sees() {
        let table = table_with_two_columns("read_cols_udf").await;

        let output_schema = Arc::new(Schema::new(vec![Field::new("sum", DataType::Int32, true)]));
        let mapper_schema = output_schema.clone();
        let udf = BatchUDF {
            mapper: Box::new(move |batch: &RecordBatch| {
                assert!(batch.column_by_name("x").is_some());
                assert!(batch.column_by_name("y").is_none(), "y was not requested");
                let x = batch["x"].as_any().downcast_ref::<Int32Array>().unwrap();
                let doubled: Int32Array = x.iter().map(|v| v.map(|v| v * 2)).collect();
                Ok(RecordBatch::try_new(
                    mapper_schema.clone(),
                    vec![Arc::new(doubled)],
                )?)
            }),
            output_schema,
            result_checkpoint: None,
        };

        table
            .add_columns()
            .transform(NewColumnTransform::BatchUDF(udf))
            .read_columns(["x"])
            .execute()
            .await
            .unwrap();

        let schema = table.schema().await.unwrap();
        assert!(schema.field_with_name("sum").is_ok());
    }
}
