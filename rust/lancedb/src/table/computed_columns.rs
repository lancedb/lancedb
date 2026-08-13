// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Computed columns.
//!
//! A computed column is defined by a rule rather than by values supplied at
//! write time. Declaring one commits the column carrying that rule in field
//! metadata but no data, so the cost does not scale with the table; a later
//! refresh fills the rows.
//!
//! The rule is tagged by kind ([`ComputedColumnKind`]) because kinds differ in
//! where the column's type and inputs come from. A SQL expression is
//! self-describing -- both are derived from the expression, so a caller writes
//! neither -- while a kind resolved through a registry cannot be typed without
//! consulting it. Only SQL exists today; the tag is what lets another kind be
//! added without a second reading of the same key.
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

/// Field metadata key naming the kind of rule that defines the column.
pub const KIND_META_KEY: &str = "computed_column.kind";

/// Field metadata key holding the SQL expression that defines the column.
pub const EXPRESSION_META_KEY: &str = "computed_column.expression";

/// Field metadata key holding the column's inputs, as a JSON array of names.
pub const INPUTS_META_KEY: &str = "computed_column.inputs";

/// Value of [`KIND_META_KEY`] for a column defined by a SQL expression.
pub const SQL_KIND: &str = "sql";

/// The rule that defines a computed column's values.
///
/// Non-exhaustive: a kind added later is an additive change, and a caller that
/// only handles the kinds it knows keeps compiling.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ComputedColumnKind {
    /// A SQL expression evaluated by DataFusion. It is the whole definition:
    /// the column's type and its inputs are both derived from it.
    Sql {
        /// The expression.
        expression: String,
    },
    /// A kind this version does not understand, written by a newer one.
    ///
    /// Reported rather than hidden so a caller can tell a column it cannot
    /// refresh apart from one that was never computed. Nothing produces this.
    Unrecognized {
        /// The kind as it was found in the metadata.
        kind: String,
    },
}

/// A computed column's declaration, as read back from field metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputedColumn {
    /// Name of the computed column.
    pub name: String,
    /// The rule that defines it.
    pub kind: ComputedColumnKind,
    /// Columns the rule reads, recorded at declaration time.
    ///
    /// Outside the kind because every kind has inputs and the consumers that
    /// use them -- refresh planning, dependency ordering -- do not care which
    /// kind produced them. Where they come from does differ, and that is
    /// settled at declaration: derived from a SQL expression, supplied by the
    /// caller for a kind that cannot be parsed.
    pub inputs: Vec<String>,
}

/// Build the field metadata recording a SQL binding.
fn computed_column_metadata(expression: &str, inputs: &[String]) -> HashMap<String, String> {
    HashMap::from([
        (COMPUTED_COLUMN_META_KEY.to_string(), "true".to_string()),
        (KIND_META_KEY.to_string(), SQL_KIND.to_string()),
        (EXPRESSION_META_KEY.to_string(), expression.to_string()),
        (
            INPUTS_META_KEY.to_string(),
            serde_json::to_string(inputs).unwrap_or_else(|_| "[]".to_string()),
        ),
    ])
}

/// Read a field's computed-column declaration, if it carries one.
///
/// A field flagged computed but carrying no kind, or a SQL one missing its
/// expression, is not a computed column here: without the rule there is
/// nothing to refresh from, so it is reported as absent rather than as a
/// half-formed declaration. An unrecognized kind is different -- the rule is
/// there and intact, this version just cannot act on it -- and comes back as
/// [`ComputedColumnKind::Unrecognized`].
pub fn computed_column_from_field(field: &ArrowField) -> Option<ComputedColumn> {
    let metadata = field.metadata();
    if metadata.get(COMPUTED_COLUMN_META_KEY).map(String::as_str) != Some("true") {
        return None;
    }
    let kind = match metadata.get(KIND_META_KEY)?.as_str() {
        SQL_KIND => ComputedColumnKind::Sql {
            expression: metadata.get(EXPRESSION_META_KEY)?.clone(),
        },
        other => ComputedColumnKind::Unrecognized {
            kind: other.to_string(),
        },
    };
    let inputs = metadata
        .get(INPUTS_META_KEY)
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .unwrap_or_default();
    Some(ComputedColumn {
        name: field.name().clone(),
        kind,
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

/// Reject a schema change to a column some declaration reads.
///
/// A binding is SQL text naming its inputs, so renaming, retyping or dropping
/// one leaves an expression that no longer resolves. Refusing the change keeps
/// a declaration that survived [`plan`] evaluable for as long as it exists.
///
/// Paths are compared at their root: a declaration reading `metadata` is
/// invalidated by a change to `metadata.age` just as surely.
pub(crate) fn ensure_not_an_input(schema: &ArrowSchema, paths: &[&str]) -> Result<()> {
    let root = |path: &str| path.split('.').next().unwrap_or(path).to_string();
    for declaration in computed_columns(schema) {
        for path in paths {
            // A declaration does not read itself, so it is free to be dropped
            // or renamed along with its binding.
            if declaration.name == root(path) {
                continue;
            }
            if declaration
                .inputs
                .iter()
                .any(|input| root(input) == root(path))
            {
                return Err(Error::InvalidInput {
                    message: format!(
                        "column '{}' is read by computed column '{}'; drop that column first",
                        path, declaration.name
                    ),
                });
            }
        }
    }
    Ok(())
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

/// Commit a declaration of a kind this version does not produce, the way a
/// newer lancedb would leave one behind. Shared with the refresh tests, which
/// need the same column to check that refresh refuses it.
#[cfg(test)]
pub(super) async fn add_foreign_kind(table: &crate::Table, name: &str, kind: &str) {
    use arrow_schema::DataType;

    let field = ArrowField::new(name, DataType::Int32, true).with_metadata(HashMap::from([
        (COMPUTED_COLUMN_META_KEY.to_string(), "true".to_string()),
        (KIND_META_KEY.to_string(), kind.to_string()),
        (INPUTS_META_KEY.to_string(), r#"["x"]"#.to_string()),
    ]));
    table
        .add_columns()
        .transform(NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(
            vec![field],
        ))))
        .execute()
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    use arrow_array::record_batch;
    use arrow_schema::DataType;
    use futures::TryStreamExt;
    use lance::dataset::ColumnAlteration;

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
                kind: ComputedColumnKind::Sql {
                    expression: "x * 2".into()
                },
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
        assert_eq!(metadata.get(KIND_META_KEY).map(String::as_str), Some("sql"));
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

    /// The reason the kind is tagged: a declaration written by a newer version
    /// has to read back as a computed column this one cannot evaluate, not as
    /// an ordinary column. Reported as absent it would be refreshable by
    /// nothing and redeclarable over, silently.
    #[tokio::test]
    async fn test_unrecognized_kind_is_reported_rather_than_hidden() {
        let table = table_with_ints("foreign_kind").await;
        super::add_foreign_kind(&table, "embedding", "udf").await;

        assert_eq!(
            declared(&table).await,
            vec![ComputedColumn {
                name: "embedding".into(),
                kind: ComputedColumnKind::Unrecognized { kind: "udf".into() },
                inputs: vec!["x".into()],
            }]
        );

        let err = add_computed(&table, &[("embedding".into(), "x * 2".into())])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ColumnAlreadyExists { name } if name == "embedding"));
    }

    /// A kind is what makes a declaration readable at all, so the flag alone
    /// is half-formed in the same way a missing expression is.
    #[test]
    fn test_flag_without_a_kind_is_not_a_declaration() {
        let field =
            ArrowField::new("half", DataType::Int32, true).with_metadata(HashMap::from([(
                COMPUTED_COLUMN_META_KEY.to_string(),
                "true".to_string(),
            )]));
        assert_eq!(computed_column_from_field(&field), None);
    }

    /// A SQL declaration is its expression; without one there is nothing to
    /// refresh from.
    #[test]
    fn test_sql_kind_without_an_expression_is_not_a_declaration() {
        let field = ArrowField::new("half", DataType::Int32, true).with_metadata(HashMap::from([
            (COMPUTED_COLUMN_META_KEY.to_string(), "true".to_string()),
            (KIND_META_KEY.to_string(), SQL_KIND.to_string()),
        ]));
        assert_eq!(computed_column_from_field(&field), None);
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

    #[tokio::test]
    async fn test_dropping_an_input_is_refused() {
        let table = table_with_ints("drop_input").await;
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        let err = table.drop_columns(&["x"]).await.unwrap_err();
        assert!(
            matches!(&err, Error::InvalidInput { message } if message.contains("doubled")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn test_renaming_an_input_is_refused() {
        let table = table_with_ints("rename_input").await;
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        let err = table
            .alter_columns(&[ColumnAlteration::new("x".into()).rename("y".into())])
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::InvalidInput { message } if message.contains("doubled")),
            "{err:?}"
        );
    }

    /// Nothing resolves against nullability, so it is not a rebinding.
    #[tokio::test]
    async fn test_altering_an_input_nullability_is_allowed() {
        let table = table_with_ints("nullable_input").await;
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        table
            .alter_columns(&[ColumnAlteration::new("x".into()).set_nullable(true)])
            .await
            .unwrap();
    }

    /// A declaration does not read itself, so it travels with its binding.
    #[tokio::test]
    async fn test_dropping_the_computed_column_is_allowed() {
        let table = table_with_ints("drop_computed").await;
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        table.drop_columns(&["doubled"]).await.unwrap();
        assert!(declared(&table).await.is_empty());
    }
}
