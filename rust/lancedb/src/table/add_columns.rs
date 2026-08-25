// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Builder for adding columns to a table.

use std::sync::Arc;

use lance::dataset::NewColumnTransform;

use super::BaseTable;
use super::schema_evolution::AddColumnsResult;
use crate::function::FunctionApplication;
use crate::{Error, Result};

/// Adds columns to a table. See [`Table::add_columns`](super::Table::add_columns).
pub struct AddColumnsBuilder {
    parent: Arc<dyn BaseTable>,
    transform: Option<NewColumnTransform>,
    computed: Vec<(String, String)>,
    function: Option<(FunctionApplication, Option<String>)>,
    read_columns: Option<Vec<String>>,
}

impl std::fmt::Debug for AddColumnsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddColumnsBuilder")
            .field("parent", &self.parent)
            .field("has_transform", &self.transform.is_some())
            .field("computed", &self.computed)
            .field("has_function", &self.function.is_some())
            .field("read_columns", &self.read_columns)
            .finish()
    }
}

impl AddColumnsBuilder {
    pub(crate) fn new(parent: Arc<dyn BaseTable>) -> Self {
        Self {
            parent,
            transform: None,
            computed: Vec::new(),
            function: None,
            read_columns: None,
        }
    }

    /// Set how the new columns' values are produced.
    pub fn transform(mut self, transform: NewColumnTransform) -> Self {
        self.transform = Some(transform);
        self
    }

    /// Add a column defined by `expression`, evaluated by a later refresh
    /// rather than by this commit. Its type and inputs are derived from the
    /// expression.
    ///
    /// The column is committed with no values, so declaring one costs the same
    /// on an empty table as on a large one. Rows get values from
    /// [`Table::refresh_column`](super::Table::refresh_column), which fills
    /// every fragment that has none -- including fragments appended since the
    /// last refresh.
    ///
    /// Refresh does not revisit a fragment it has filled, so mutating an input
    /// leaves the value computed at fill time; recomputing means dropping the
    /// column and declaring it again. An input cannot be renamed, retyped or
    /// dropped while a declaration reads it, since the expression names it.
    ///
    /// On LanceDB Cloud and Enterprise the expression is planned by the
    /// server, and the refresh runs as a server job -- see
    /// [`Table::refresh_column_async`](super::Table::refresh_column_async).
    ///
    /// ```
    /// # use lancedb::Table;
    /// # async fn declare(table: &Table) -> Result<(), Box<dyn std::error::Error>> {
    /// table
    ///     .add_columns()
    ///     .computed("doubled", "x * 2")
    ///     .execute()
    ///     .await?;
    /// let filled = table.refresh_column("doubled").await?;
    /// println!("filled {} rows", filled.rows_filled);
    /// # Ok(())
    /// # }
    /// ```
    pub fn computed(mut self, name: impl Into<String>, expression: impl Into<String>) -> Self {
        self.computed.push((name.into(), expression.into()));
        self
    }

    /// Declare every field of a named-struct Function result as one atomic
    /// binding. Result-field aliases come from
    /// [`FunctionApplication::columns`](crate::function::FunctionApplication::columns).
    ///
    /// ```
    /// # use lancedb::Table;
    /// # use lancedb::function::FunctionApplication;
    /// # async fn declare(table: &Table, application: FunctionApplication) -> lancedb::Result<()> {
    /// table.add_columns().function(application).execute().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn function(mut self, application: FunctionApplication) -> Self {
        self.function = Some((application, None));
        self
    }

    /// Declare a scalar or entire named-struct Function result as one table
    /// column. The physical column starts all-null and is materialized by the
    /// remote Function refresh path.
    ///
    /// ```
    /// # use lancedb::Table;
    /// # use lancedb::function::FunctionApplication;
    /// # async fn declare(table: &Table, application: FunctionApplication) -> lancedb::Result<()> {
    /// table
    ///     .add_columns()
    ///     .function_as("embedding", application)
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn function_as(
        mut self,
        name: impl Into<String>,
        application: FunctionApplication,
    ) -> Self {
        self.function = Some((application, Some(name.into())));
        self
    }

    /// Limit which existing columns a [`NewColumnTransform::BatchUDF`] mapper
    /// receives. Every other transform, and a computed column, determines what
    /// it reads, so setting this alongside one is an error rather than a silent
    /// no-op.
    pub fn read_columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.read_columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Add the columns.
    pub async fn execute(self) -> Result<AddColumnsResult> {
        let Self {
            parent,
            transform,
            computed,
            function,
            read_columns,
        } = self;

        let declaration_count = usize::from(!computed.is_empty()) + usize::from(function.is_some());
        if transform.is_some() && declaration_count != 0 || declaration_count > 1 {
            return Err(Error::InvalidInput {
                message: "add_columns cannot mix transforms, SQL computed columns, and a Function application; they cannot be added atomically in one call"
                    .into(),
            });
        }

        match (transform, computed.is_empty(), function) {
            (None, true, None) => Err(Error::InvalidInput {
                message: "add_columns requires a transform or a computed column".into(),
            }),
            (Some(transform), true, None) => {
                if read_columns.is_some() && !matches!(transform, NewColumnTransform::BatchUDF(_)) {
                    return Err(Error::InvalidInput {
                        message: "read_columns applies only to a BatchUDF transform; \
                                  every other transform determines what it reads"
                            .into(),
                    });
                }
                parent.add_columns(transform, read_columns).await
            }
            (None, false, None) => {
                if read_columns.is_some() {
                    return Err(Error::InvalidInput {
                        message: "read_columns applies only to a BatchUDF transform; \
                                  a computed column's inputs come from its expression"
                            .into(),
                    });
                }
                parent.add_computed_columns(&computed).await
            }
            (None, true, Some((application, output_name))) => {
                if read_columns.is_some() {
                    return Err(Error::InvalidInput {
                        message: "read_columns does not apply to a Function application; its inputs are already bound"
                            .into(),
                    });
                }
                parent
                    .add_function_columns(&application, output_name.as_deref())
                    .await
            }
            _ => unreachable!("mixed add_columns modes were rejected above"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, record_batch};
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::{BatchUDF, NewColumnTransform};

    use crate::connect;
    use crate::{Error, Table};

    async fn table_with_two_columns(name: &str) -> Table {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("x", Int32, [1, 2, 3]), ("y", Int32, [10, 20, 30])).unwrap();
        conn.create_table(name, batch).execute().await.unwrap()
    }

    #[tokio::test]
    async fn test_requires_a_transform() {
        let table = table_with_two_columns("no_transform").await;
        let err = table.add_columns().execute().await.unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));
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
        assert!(matches!(err, Error::InvalidInput { .. }));

        let schema = table.schema().await.unwrap();
        assert!(
            schema.field_with_name("doubled").is_err(),
            "a rejected call must not commit"
        );
    }

    #[tokio::test]
    async fn test_mixing_transform_and_computed_is_rejected() {
        let table = table_with_two_columns("mixed_add").await;
        let err = table
            .add_columns()
            .transform(NewColumnTransform::SqlExpressions(vec![(
                "eager".into(),
                "x * 2".into(),
            )]))
            .computed("lazy", "x * 3")
            .execute()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));

        let schema = table.schema().await.unwrap();
        assert!(schema.field_with_name("eager").is_err());
        assert!(schema.field_with_name("lazy").is_err());
    }

    #[tokio::test]
    async fn test_read_columns_with_computed_is_rejected() {
        let table = table_with_two_columns("read_cols_computed").await;
        let err = table
            .add_columns()
            .computed("doubled", "x * 2")
            .read_columns(["x"])
            .execute()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));
        assert!(
            table
                .schema()
                .await
                .unwrap()
                .field_with_name("doubled")
                .is_err()
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
