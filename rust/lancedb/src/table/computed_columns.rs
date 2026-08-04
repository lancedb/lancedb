// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Expression-backed computed columns.
//!
//! A computed column is defined by a SQL expression rather than by values
//! supplied at write time. Declaring one commits the column carrying its
//! expression in field metadata but no data, so the cost does not scale with
//! the table; a later refresh fills the rows.
//!
//! The expression is the whole definition: both the result type and the input
//! columns are derived from it, so a caller writes neither.
//!
//! [`computed_columns`] and [`computed_column_from_field`] read declarations
//! back off a schema.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{Field as ArrowField, Schema as ArrowSchema, SchemaRef};
use lance::dataset::NewColumnTransform;
use lance_datafusion::planner::Planner;

use crate::{Error, Result};

/// Field metadata key marking a column as computed. The value is `"true"`.
pub const COMPUTED_COLUMN_META_KEY: &str = "computed_column";

/// Field metadata key holding the SQL expression that defines the column.
pub const EXPRESSION_META_KEY: &str = "computed_column.expression";

/// Field metadata key holding the column's inputs, as a JSON array of names.
pub const INPUTS_META_KEY: &str = "computed_column.inputs";

/// A computed column's declaration, as read back from field metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputedColumn {
    /// Name of the computed column.
    pub name: String,
    /// The SQL expression that defines it.
    pub expression: String,
    /// Columns the expression reads, parsed from it at declaration time.
    pub inputs: Vec<String>,
}

/// Build the field metadata recording a binding.
fn computed_column_metadata(expression: &str, inputs: &[String]) -> HashMap<String, String> {
    HashMap::from([
        (COMPUTED_COLUMN_META_KEY.to_string(), "true".to_string()),
        (EXPRESSION_META_KEY.to_string(), expression.to_string()),
        (
            INPUTS_META_KEY.to_string(),
            serde_json::to_string(inputs).unwrap_or_else(|_| "[]".to_string()),
        ),
    ])
}

/// Read a field's computed-column declaration, if it carries one.
///
/// A field flagged computed but missing its expression is not a computed
/// column here: without the binding there is nothing to refresh from, so it is
/// reported as absent rather than as a half-formed declaration.
pub fn computed_column_from_field(field: &ArrowField) -> Option<ComputedColumn> {
    let metadata = field.metadata();
    if metadata.get(COMPUTED_COLUMN_META_KEY).map(String::as_str) != Some("true") {
        return None;
    }
    let expression = metadata.get(EXPRESSION_META_KEY)?;
    let inputs = metadata
        .get(INPUTS_META_KEY)
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .unwrap_or_default();
    Some(ComputedColumn {
        name: field.name().clone(),
        expression: expression.clone(),
        inputs,
    })
}

/// Read every computed-column declaration carried by `schema`, in field order.
///
/// Introspection is a pure read of the schema the caller already holds, the
/// way a SQL catalog reports a generation expression as another column of
/// `information_schema.columns`.
pub fn computed_columns(schema: &ArrowSchema) -> Vec<ComputedColumn> {
    schema
        .fields()
        .iter()
        .filter_map(|field| computed_column_from_field(field))
        .collect()
}

/// Resolve `(name, expression)` pairs against `schema` into fields carrying
/// their bindings.
///
/// Everything that can be known statically is checked here rather than at
/// refresh time: that the expression parses, that every column it reads
/// exists, and that the target name is free. A declaration that survives this
/// is one a refresh can always act on.
pub(crate) fn plan(schema: SchemaRef, columns: &[(String, String)]) -> Result<Vec<ArrowField>> {
    if columns.is_empty() {
        return Err(Error::InvalidInput {
            message: "at least one computed column is required".into(),
        });
    }

    let planner = Planner::new(schema.clone());
    let mut fields = Vec::with_capacity(columns.len());
    let mut declared: Vec<&str> = Vec::with_capacity(columns.len());

    for (name, expression) in columns {
        if schema.field_with_name(name).is_ok() || declared.contains(&name.as_str()) {
            return Err(Error::ColumnAlreadyExists { name: name.clone() });
        }

        let expr = planner
            .parse_expr(expression)
            .and_then(|expr| planner.optimize_expr(expr))
            .map_err(|e| Error::InvalidExpression {
                column: name.clone(),
                message: e.to_string(),
            })?;

        let mut inputs = Planner::column_names_in_expr(&expr);
        inputs.sort();
        inputs.dedup();

        // Resolved here rather than left to the planner so an unknown column
        // names itself in the error instead of surfacing as a plan failure.
        let mut indices = Vec::with_capacity(inputs.len());
        for input in &inputs {
            let index = schema
                .index_of(input)
                .map_err(|_| Error::InvalidExpression {
                    column: name.clone(),
                    message: format!("unknown column '{input}'"),
                })?;
            indices.push(index);
        }

        // Physical expressions address columns by position, so the planner
        // that types the expression has to be built on the projected schema
        // the refresh will actually read.
        let read_schema =
            Arc::new(
                schema
                    .project(&indices)
                    .map_err(|e| Error::InvalidExpression {
                        column: name.clone(),
                        message: e.to_string(),
                    })?,
            );
        let physical = Planner::new(read_schema.clone())
            .create_physical_expr(&expr)
            .map_err(|e| Error::InvalidExpression {
                column: name.clone(),
                message: e.to_string(),
            })?;
        let data_type =
            physical
                .data_type(read_schema.as_ref())
                .map_err(|e| Error::InvalidExpression {
                    column: name.clone(),
                    message: e.to_string(),
                })?;

        // Declared columns start entirely null, so nullability is a property
        // of the declaration rather than of what the expression yields.
        fields.push(
            ArrowField::new(name, data_type, true)
                .with_metadata(computed_column_metadata(expression, &inputs)),
        );
        declared.push(name);
    }

    Ok(fields)
}

/// Build the transform that declares `columns` against `schema`.
///
/// An all-null column is how a binding with no values yet is carried into a
/// commit; that it is spelled `AllNulls` is a detail of the commit, not of the
/// column, which is why this is internal and
/// [`AddColumnsBuilder::computed`](super::AddColumnsBuilder::computed) is the
/// public way in.
pub(crate) fn declare(
    schema: SchemaRef,
    columns: &[(String, String)],
) -> Result<NewColumnTransform> {
    let fields = plan(schema, columns)?;
    Ok(NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(
        fields,
    ))))
}

#[cfg(test)]
mod tests {
    use arrow_array::record_batch;
    use arrow_schema::DataType;
    use futures::TryStreamExt;

    use super::*;
    use crate::connect;
    use crate::query::{ExecutableQuery, QueryBase, Select};
    use crate::{Error, Table};

    async fn table_with_ints(name: &str) -> Table {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("x", Int32, [1, 2, 3])).unwrap();
        conn.create_table(name, batch).execute().await.unwrap()
    }

    /// Declare `columns` the way a caller would: plan the expressions, then
    /// add them through the ordinary column API.
    async fn add_computed(table: &Table, columns: &[(String, String)]) -> Result<u64> {
        let mut builder = table.add_columns();
        for (name, expression) in columns {
            builder = builder.computed(name, expression);
        }
        Ok(builder.execute().await?.version)
    }

    async fn declared(table: &Table) -> Vec<ComputedColumn> {
        computed_columns(table.schema().await.unwrap().as_ref())
    }

    #[tokio::test]
    async fn test_declare_infers_type_and_inputs() {
        let table = table_with_ints("declare_infers").await;
        let initial = table.version().await.unwrap();

        let version = add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();
        assert!(version > initial);

        let schema = table.schema().await.unwrap();
        let field = schema.field_with_name("doubled").unwrap();
        assert_eq!(field.data_type(), &DataType::Int32);
        assert!(field.is_nullable());

        assert_eq!(
            declared(&table).await,
            vec![ComputedColumn {
                name: "doubled".into(),
                expression: "x * 2".into(),
                inputs: vec!["x".into()],
            }]
        );
    }

    /// The binding reaches the schema only if `AllNulls` carries per-field
    /// metadata through the commit. The whole representation rests on it.
    #[tokio::test]
    async fn test_all_nulls_preserves_field_metadata() {
        let table = table_with_ints("metadata_survives").await;
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        let schema = table.schema().await.unwrap();
        let metadata = schema.field_with_name("doubled").unwrap().metadata();
        assert_eq!(
            metadata.get(COMPUTED_COLUMN_META_KEY).map(String::as_str),
            Some("true")
        );
        assert_eq!(
            metadata.get(EXPRESSION_META_KEY).map(String::as_str),
            Some("x * 2")
        );
        assert_eq!(
            metadata.get(INPUTS_META_KEY).map(String::as_str),
            Some(r#"["x"]"#)
        );
    }

    #[tokio::test]
    async fn test_declared_column_is_all_null() {
        let table = table_with_ints("declare_is_null").await;
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        let batches = table
            .query()
            .select(Select::columns(&["doubled"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
        for batch in &batches {
            assert_eq!(batch["doubled"].null_count(), batch.num_rows());
        }
    }

    #[tokio::test]
    async fn test_unknown_column_fails_at_declare_time() {
        let table = table_with_ints("unknown_input").await;
        let err = add_computed(&table, &[("bad".into(), "missing + 1".into())])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidExpression { column, .. } if column == "bad"));

        let schema = table.schema().await.unwrap();
        assert!(schema.field_with_name("bad").is_err());
    }

    #[tokio::test]
    async fn test_unparsable_expression_fails_at_declare_time() {
        let table = table_with_ints("bad_syntax").await;
        let err = add_computed(&table, &[("bad".into(), "x *".into())])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidExpression { column, .. } if column == "bad"));
        assert!(
            table
                .schema()
                .await
                .unwrap()
                .field_with_name("bad")
                .is_err()
        );
    }

    /// A user-defined function is an expression like any other; only its
    /// resolution is missing. When a registry-aware planner exists this
    /// becomes a supported declaration rather than a new API.
    #[tokio::test]
    async fn test_unregistered_function_is_rejected_for_now() {
        let table = table_with_ints("udf_not_yet").await;
        let err = add_computed(&table, &[("vec".into(), "embed(x)".into())])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidExpression { column, .. } if column == "vec"));
        assert!(
            table
                .schema()
                .await
                .unwrap()
                .field_with_name("vec")
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_existing_column_name_is_rejected() {
        let table = table_with_ints("name_taken").await;
        let err = add_computed(&table, &[("x".into(), "x * 2".into())])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ColumnAlreadyExists { name } if name == "x"));
        assert!(declared(&table).await.is_empty());
    }

    #[tokio::test]
    async fn test_constant_expression_needs_no_inputs() {
        let table = table_with_ints("constant").await;
        add_computed(&table, &[("answer".into(), "42".into())])
            .await
            .unwrap();

        let declared = declared(&table).await;
        assert_eq!(declared.len(), 1);
        assert!(declared[0].inputs.is_empty());
    }

    #[tokio::test]
    async fn test_multiple_columns_in_one_commit() {
        let table = table_with_ints("multi").await;
        let initial = table.version().await.unwrap();

        add_computed(
            &table,
            &[
                ("plus".into(), "x + 1".into()),
                ("squared".into(), "x * x".into()),
            ],
        )
        .await
        .unwrap();

        assert_eq!(table.version().await.unwrap(), initial + 1);
        let declared = declared(&table).await;
        assert_eq!(declared.len(), 2);
        assert_eq!(declared[0].name, "plus");
        assert_eq!(declared[1].name, "squared");
    }

    #[tokio::test]
    async fn test_duplicate_declaration_in_one_call_is_rejected() {
        let table = table_with_ints("dupe").await;
        let err = add_computed(
            &table,
            &[
                ("dup".into(), "x + 1".into()),
                ("dup".into(), "x + 2".into()),
            ],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::ColumnAlreadyExists { name } if name == "dup"));
        assert!(declared(&table).await.is_empty());
    }

    /// A column added by an ordinary transform is materialized, not bound, so
    /// it carries no declaration to report.
    #[tokio::test]
    async fn test_ordinary_columns_are_not_reported_as_computed() {
        let table = table_with_ints("plain").await;
        assert!(declared(&table).await.is_empty());

        table
            .add_columns()
            .transform(NewColumnTransform::SqlExpressions(vec![(
                "eager".into(),
                "x * 2".into(),
            )]))
            .execute()
            .await
            .unwrap();
        assert!(declared(&table).await.is_empty());
    }

    /// Built-in functions type the column the same way an operator does.
    #[tokio::test]
    async fn test_builtin_function_inference() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("name", Utf8, ["ada", "grace"]), ("n", Int32, [-1, 2])).unwrap();
        let table = conn
            .create_table("builtins", batch)
            .execute()
            .await
            .unwrap();

        add_computed(
            &table,
            &[
                ("shout".into(), "upper(name)".into()),
                ("width".into(), "length(name)".into()),
                ("magnitude".into(), "abs(n)".into()),
            ],
        )
        .await
        .unwrap();

        let schema = table.schema().await.unwrap();
        assert_eq!(
            schema.field_with_name("shout").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("magnitude").unwrap().data_type(),
            &DataType::Int32
        );
        // length() returns a width-dependent integer type; assert it is one
        // rather than pinning which.
        assert!(
            schema
                .field_with_name("width")
                .unwrap()
                .data_type()
                .is_integer()
        );

        let declared = declared(&table).await;
        assert_eq!(declared.len(), 3);
        assert_eq!(declared[0].inputs, vec!["name".to_string()]);
        assert_eq!(declared[2].inputs, vec!["n".to_string()]);
    }

    #[tokio::test]
    async fn test_inputs_are_deduplicated_and_sorted() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("b", Int32, [1, 2]), ("a", Int32, [3, 4])).unwrap();
        let table = conn.create_table("dedupe", batch).execute().await.unwrap();

        add_computed(&table, &[("total".into(), "b + a + b".into())])
            .await
            .unwrap();

        assert_eq!(
            declared(&table).await[0].inputs,
            vec!["a".to_string(), "b".to_string()]
        );
    }
}
