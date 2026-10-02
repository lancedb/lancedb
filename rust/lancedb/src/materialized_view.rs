// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Materialized views.
//!
//! A materialized view is a table whose contents are defined by a query over
//! one source table and maintained by refresh rather than by writes. Creation
//! commits an empty table carrying the defining query in schema metadata; a
//! query this version cannot maintain reads back as unrefreshable, not as a
//! plain table. Queries, indexes and search work on the view unchanged.

mod grouped;
pub use grouped::IVF_PARTITION;
mod query;
pub mod refresh;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field as ArrowField, FieldRef, Schema as ArrowSchema, SchemaRef};
use datafusion_common::ScalarValue;
use lance_datafusion::planner::Planner;
use serde::{Deserialize, Serialize};

use crate::connection::Connection;
use crate::database::{CreateTableRequest, Database, OpenTableRequest};
use crate::embeddings::EmbeddingDefinition;
use crate::function::FunctionBinding;
use crate::job::Job;
use crate::table::Table;
use crate::table::computed_columns::{
    FUNCTION_BINDINGS_META_KEY, computed_column_from_field, computed_columns,
    ensure_declarations_are_planned, function_bindings_metadata,
};
use crate::table::{ColumnDefinition, ColumnKind};
use crate::{Error, Result};

pub use refresh::{RefreshMaterializedViewResult, RefreshMode};

/// Schema metadata key holding the view definition; see [`DEFINITION_FORMAT`].
pub const DEFINITION_META_KEY: &str = "mv.definition";

/// Field metadata namespace for declarations about schema structure, such as
/// an unenforced primary key.
const SCHEMA_DECLARATION_META_PREFIX: &str = "lance-schema:";

/// A field's identity in its own schema, which is not the view's.
const LANCE_FIELD_ID_KEY: &str = "lance:field_id";

/// Schema metadata key holding embedding-function configuration. It describes
/// columns rather than storage, so a view carries it through.
const EMBEDDING_FUNCTIONS_META_KEY: &str = "embedding_functions";

/// Schema metadata key holding lancedb's own column definitions, one per
/// field in schema order. It marks which columns an embedding function
/// produces, which is what lets a query embed its own text.
const COLUMN_DEFINITIONS_META_KEY: &str = "lancedb::column_definitions";

/// The newest layout this version reads under [`DEFINITION_META_KEY`]:
/// `{"format": N, "query": "<SQL>"}`, the query as
/// [`MaterializedViewDefinition::to_sql`] renders it. Format 2 identifies a
/// query with `GROUP BY`, and format 1 an ordinary query. A reader refuses a newer format rather than
/// guess at it. The layout also carries `"kind": "query"`, which readers
/// older than the format number report as an unrefreshable view instead of
/// failing to read the metadata.
pub const DEFINITION_FORMAT: u64 = 2;

/// The format `definition` is written in; see [`DEFINITION_FORMAT`].
fn format_of(definition: &MaterializedViewDefinition) -> u64 {
    if definition.is_grouped() { 2 } else { 1 }
}

/// Legacy `kind` tag of the structured layout written before
/// [`DEFINITION_FORMAT`] existed; still read, never written.
pub const SELECT_KIND: &str = "select";

/// Legacy `kind` tag of the structured layout over a namespaced source;
/// still read, never written.
pub const NAMESPACED_SELECT_KIND: &str = "namespaced_select";

/// Which view outputs each source column is projected to directly. A column
/// may be projected more than once, so each carries every name the view gives
/// it, in projection order.
type Lineage = HashMap<String, Vec<String>>;

/// One projected output column of a view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewProjection {
    /// Name of the column in the view.
    pub output: String,
    /// SQL expression over the source table that computes it.
    pub expression: String,
}

impl ViewProjection {
    /// `SELECT *`: every source column, expanded when the view is planned.
    /// A definition selecting it holds this projection alone.
    ///
    /// ```
    /// use lancedb::materialized_view::{MaterializedViewDefinition, ViewProjection};
    ///
    /// let definition = MaterializedViewDefinition::from_sql("SELECT * FROM docs")?;
    /// assert_eq!(definition.projections, [ViewProjection::star()]);
    /// assert!(definition.selects_star());
    /// # Ok::<(), lancedb::Error>(())
    /// ```
    pub fn star() -> Self {
        Self {
            output: "*".to_string(),
            expression: "*".to_string(),
        }
    }
}

/// A `FROM` item computed per source row: each source row yields one view
/// row per element, and projections read the element as `alias`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewLateral {
    /// Where the elements come from.
    pub source: LateralSource,
    /// The name the element is read through.
    pub alias: String,
}

/// What a [`ViewLateral`] expands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LateralSource {
    /// `UNNEST(column)`: a list column of the source table.
    Unnest {
        /// The list column.
        column: String,
    },
    /// `name(args)`: a Function in `FROM` position, returning rows. Local
    /// databases do not execute table functions during refresh.
    Function {
        /// The Function's name.
        name: String,
        /// Its arguments, as SQL expressions over the source table.
        args: Vec<String>,
    },
}

/// The engine's form of a [`ViewLateral`]: the list column it unnests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ViewUnnest {
    pub column: String,
    pub alias: String,
}

/// The list column refresh unnests for `definition`. `None` when the query
/// has no lateral item.
pub(crate) fn physical_unnest(
    definition: &MaterializedViewDefinition,
) -> Result<Option<ViewUnnest>> {
    let Some(lateral) = &definition.lateral else {
        return Ok(None);
    };
    let column = match &lateral.source {
        LateralSource::Unnest { column } => column.clone(),
        LateralSource::Function { name, .. } => {
            return Err(Error::NotSupported {
                message: format!(
                    "'{name}' in FROM position is a Function; views over Function rows are \
                     supported only on LanceDB Cloud and Enterprise"
                ),
            });
        }
    };
    Ok(Some(ViewUnnest {
        column,
        alias: lateral.alias.clone(),
    }))
}

/// The query that defines a materialized view, in the relational shape
/// refresh maintains. Stored as SQL; see [`MaterializedViewDefinition::from_sql`]
/// for the shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedViewDefinition {
    /// Name of the source table, in the same database as the view.
    pub source_table: String,
    /// Namespace path holding the source table; empty is the root namespace.
    pub source_namespace: Vec<String>,
    /// The `FROM` item computed per source row, if any.
    pub lateral: Option<ViewLateral>,
    /// The projected output columns, in view schema order;
    /// [`ViewProjection::star`] alone selects every source column. Empty is
    /// a declaration that projects nothing yet, which
    /// [`PreparedDeclaration::input_column`] can still add to.
    pub projections: Vec<ViewProjection>,
    /// SQL predicate selecting the rows the view holds.
    pub filter: Option<String>,
    /// `GROUP BY` expressions. A grouped view holds one row per group and is
    /// recomputed in full whenever its source changes.
    pub group_by: Vec<String>,
    /// Cap on the number of rows the view holds, in materialization order.
    pub limit: Option<u64>,
}

impl MaterializedViewDefinition {
    /// Parse the defining query:
    ///
    /// ```sql
    /// SELECT <column | expr AS name | *>, ...
    /// FROM [ns.]table [, function(args) AS alias | , UNNEST(column) AS alias]
    /// [WHERE predicate] [GROUP BY expr, ...] [LIMIT n]
    /// ```
    ///
    /// A Function in `FROM` position yields one row per element it returns;
    /// `CROSS JOIN [LATERAL]` spells the same relation. Any other clause is
    /// refused: this engine cannot maintain it, and a definition it does not
    /// fully understand must not be materialized.
    ///
    /// ```
    /// use lancedb::materialized_view::MaterializedViewDefinition;
    ///
    /// let definition = MaterializedViewDefinition::from_sql(
    ///     "select id, c.text from docs cross join lateral chunk(body) as c",
    /// )?;
    /// assert_eq!(definition.to_sql(), "SELECT id, c.text FROM docs, chunk(body) AS c");
    /// # Ok::<(), lancedb::Error>(())
    /// ```
    pub fn from_sql(sql: &str) -> Result<Self> {
        query::parse(sql)
    }

    /// The defining query in its canonical spelling, which is what is
    /// stored and what [`Self::from_sql`] reads back equal.
    pub fn to_sql(&self) -> String {
        query::render(self)
    }

    /// The definition in its stored layout (see [`DEFINITION_FORMAT`]), as
    /// the language bindings hand it across.
    pub fn to_json(&self) -> Result<String> {
        definition_to_metadata(self)
    }

    /// Whether the query has a `GROUP BY`.
    pub fn is_grouped(&self) -> bool {
        !self.group_by.is_empty()
    }

    /// Whether the query is `SELECT *`.
    pub fn selects_star(&self) -> bool {
        matches!(self.projections.as_slice(), [p] if *p == ViewProjection::star())
    }
}

/// A view definition as read back from schema metadata. Non-exhaustive so
/// a later outcome is additive.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StoredDefinition {
    /// A query this version can maintain.
    Query(MaterializedViewDefinition),
    /// Written by a newer version, reported so a caller can tell an
    /// unrefreshable view apart from a plain table. `format` is the tag as
    /// found: a format number, or a legacy `kind`.
    Newer {
        /// The format tag as stored.
        format: String,
    },
}

/// The backend-independent metadata needed to open a materialized view.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedViewInfo {
    /// The parsed view definition.
    pub definition: MaterializedViewDefinition,
}

/// The backend-independent request used to create a remote materialized view.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateMaterializedViewRequest {
    /// Name of the new view.
    pub name: String,
    /// Namespace in which to create the view.
    pub namespace_path: Vec<String>,
    /// Defining SELECT query.
    pub query: String,
    /// Whether to skip the initial population job.
    pub with_no_data: bool,
}

/// Prefix of the internal columns holding source columns a computed column
/// reads without the view projecting them; see
/// [`PreparedDeclaration::input_column`].
pub const INPUT_COLUMN_PREFIX: &str = "__input_";

/// The internal view column holding a copy of `source_column`; a nested
/// path's separators become `__`, since a top-level name cannot hold `.`.
pub fn input_column_name(source_column: &str) -> String {
    format!("{INPUT_COLUMN_PREFIX}{}", source_column.replace('.', "__"))
}

/// The structured layout written before [`DEFINITION_FORMAT`]. Read only;
/// refresh rewrites such a view in the current layout.
#[derive(Deserialize)]
struct LegacyDefinition {
    source_table: String,
    #[serde(default)]
    source_namespace: Vec<String>,
    projections: Vec<ViewProjection>,
    #[serde(default)]
    filter: Option<String>,
    #[serde(default)]
    limit: Option<u64>,
}

/// Serialize `definition` into the layout stored under
/// [`DEFINITION_META_KEY`].
pub(crate) fn definition_to_metadata(definition: &MaterializedViewDefinition) -> Result<String> {
    Ok(definition_metadata_from_sql_with_format(
        &definition.to_sql(),
        format_of(definition),
    ))
}

/// Serialize an arbitrary SQL definition into the materialized-view metadata
/// envelope. Sophon uses this for SQL queries that are broader than the local
/// refresh engine's supported shape.
pub fn definition_metadata_from_sql(sql: &str) -> String {
    definition_metadata_from_sql_with_format(sql, 1)
}

fn definition_metadata_from_sql_with_format(sql: &str, format: u64) -> String {
    serde_json::json!({
        "kind": "query",
        "format": format,
        "query": sql,
    })
    .to_string()
}

/// Return the defining SQL stored in [`DEFINITION_META_KEY`], without
/// requiring the local refresh parser to understand it.
pub fn read_definition_sql(metadata: &HashMap<String, String>) -> Result<Option<String>> {
    let Some(raw) = metadata.get(DEFINITION_META_KEY) else {
        return Ok(None);
    };
    let value: serde_json::Value = serde_json::from_str(raw).map_err(|e| Error::Runtime {
        message: format!("unreadable materialized view definition: {e}"),
    })?;
    value
        .get("query")
        .and_then(|query| query.as_str())
        .map(ToOwned::to_owned)
        .ok_or_else(|| Error::Runtime {
            message: "unreadable materialized view definition: missing query".into(),
        })
        .map(Some)
}

/// Read a view definition off a schema metadata map, if it carries one.
/// `Ok(None)` for a plain table; a definition that does not parse is an
/// error, because treating a view as plain would let it be rewritten.
pub fn read_definition(metadata: &HashMap<String, String>) -> Result<Option<StoredDefinition>> {
    let Some(raw) = metadata.get(DEFINITION_META_KEY) else {
        return Ok(None);
    };
    let unreadable = |e: &dyn std::fmt::Display| Error::Runtime {
        message: format!("unreadable materialized view definition: {e}"),
    };
    let value: serde_json::Value = serde_json::from_str(raw).map_err(|e| unreadable(&e))?;
    if let Some(format) = value.get("format") {
        let Some(format) = format.as_u64() else {
            return Err(unreadable(&format!("format tag {format} is not a number")));
        };
        if format > DEFINITION_FORMAT {
            return Ok(Some(StoredDefinition::Newer {
                format: format.to_string(),
            }));
        }
        let Some(sql) = value.get("query").and_then(|q| q.as_str()) else {
            return Err(unreadable(&"missing query"));
        };
        let definition = MaterializedViewDefinition::from_sql(sql).map_err(|e| unreadable(&e))?;
        // No correct writer tags a query below the format its shape needs.
        if format < format_of(&definition) {
            return Err(unreadable(&format!(
                "format {format} cannot carry this query; it needs {}",
                format_of(&definition)
            )));
        }
        return Ok(Some(StoredDefinition::Query(definition)));
    }
    let kind = value
        .get("kind")
        .and_then(|k| k.as_str())
        .ok_or_else(|| unreadable(&"missing format tag"))?
        .to_string();
    if kind != SELECT_KIND && kind != NAMESPACED_SELECT_KIND {
        return Ok(Some(StoredDefinition::Newer {
            format: format!("kind '{kind}'"),
        }));
    }
    let legacy: LegacyDefinition = serde_json::from_value(value).map_err(|e| unreadable(&e))?;
    // No correct writer produced a tag that disagrees with the definition.
    if (kind == SELECT_KIND) != legacy.source_namespace.is_empty() {
        return Err(unreadable(&format!(
            "kind '{kind}' does not match its source namespace {:?}",
            legacy.source_namespace
        )));
    }
    Ok(Some(StoredDefinition::Query(MaterializedViewDefinition {
        source_table: legacy.source_table,
        source_namespace: legacy.source_namespace,
        lateral: None,
        projections: legacy.projections,
        filter: legacy.filter,
        group_by: Vec::new(),
        limit: legacy.limit,
    })))
}

pub(crate) fn materialized_view_info_from_metadata(
    name: &str,
    metadata: &HashMap<String, String>,
) -> Result<MaterializedViewInfo> {
    match read_definition(metadata)? {
        Some(StoredDefinition::Query(definition)) => Ok(MaterializedViewInfo { definition }),
        Some(StoredDefinition::Newer { format }) => Err(Error::NotSupported {
            message: format!(
                "materialized view '{name}' is stored in format {format}, which this version \
                 of lancedb cannot refresh"
            ),
        }),
        None => Err(Error::NotAMaterializedView {
            name: name.to_string(),
        }),
    }
}

/// Resolve a definition against the source schema into the view's projected
/// fields, with `inputs` filled in and every expression in its canonical
/// spelling. Everything statically checkable is checked here rather than at
/// refresh time. Empty `projections` selects every column as the schema
/// stands now.
#[derive(Debug)]
pub(crate) struct Planned {
    /// The definition with every expression in its canonical spelling and
    /// `SELECT *` expanded.
    pub definition: MaterializedViewDefinition,
    /// The view's projected fields, in order.
    pub fields: Vec<ArrowField>,
    pub lineage: Lineage,
    /// Source columns the query reads; a read through the unnest alias is
    /// recorded as the list column present in the source schema.
    pub inputs: Vec<String>,
}

pub(crate) fn plan(
    source_schema: SchemaRef,
    definition: &MaterializedViewDefinition,
) -> Result<Planned> {
    let filter = definition
        .filter
        .as_deref()
        .map(query::canonical_expr)
        .transpose()
        .map_err(|err| match err {
            Error::InvalidInput { message } => Error::InvalidInput {
                message: format!("invalid view filter: {message}"),
            },
            err => err,
        })?;
    if definition.is_grouped() {
        return grouped::plan(source_schema, definition, filter);
    }
    // Projections are typed against the source, or for an unnested view
    // against the source with the list column replaced by its element
    // under the alias, where `c.chunk` is an ordinary nested path.
    let unnest = physical_unnest(definition)?;
    let source_schema = match &unnest {
        None => source_schema,
        Some(unnest) => {
            // A scan limit counts source rows, not the elements they expand to.
            if definition.limit.is_some() {
                return Err(Error::InvalidInput {
                    message: "LIMIT is not supported together with UNNEST".to_string(),
                });
            }
            flattened_schema(&source_schema, unnest)?
        }
    };
    let projections: Vec<(String, String)> = if definition.selects_star() {
        source_schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), query::ident_sql(f.name())))
            .collect()
    } else {
        definition
            .projections
            .iter()
            .map(|p| {
                let expression =
                    query::canonical_expr(&p.expression).map_err(|e| Error::InvalidExpression {
                        column: p.output.clone(),
                        message: e.to_string(),
                    })?;
                Ok((p.output.clone(), expression))
            })
            .collect::<Result<_>>()?
    };
    let limit = definition.limit;

    // A scan takes the cap as i64. Rejecting it here keeps creation and
    // refresh from disagreeing about whether a view is valid.
    if let Some(limit) = limit
        && i64::try_from(limit).is_err()
    {
        return Err(Error::InvalidInput {
            message: format!("view limit {limit} exceeds the maximum of {}", i64::MAX),
        });
    }

    let planner = Planner::new(source_schema.clone());
    let mut fields = Vec::with_capacity(projections.len());
    let mut inputs = Vec::new();
    let mut declared: Vec<&str> = Vec::with_capacity(projections.len());
    let mut lineage: Lineage = HashMap::new();

    for (output, expression) in &projections {
        if declared.contains(&output.as_str()) {
            return Err(Error::ColumnAlreadyExists {
                name: output.clone(),
            });
        }
        let parsed = planner
            .parse_expr(expression)
            .map_err(|e| Error::InvalidExpression {
                column: output.clone(),
                message: e.to_string(),
            })?;
        let expr = planner
            .optimize_expr(parsed)
            .map_err(|e| Error::InvalidExpression {
                column: output.clone(),
                message: e.to_string(),
            })?;
        let expr_inputs =
            resolve_inputs(&source_schema, &expr, |message| Error::InvalidExpression {
                column: output.clone(),
                message,
            })?;

        // Physical expressions address columns by position, so the planner
        // that types the expression is built on the projected schema.
        let read_schema = project_schema(&source_schema, &expr_inputs);
        let physical = Planner::new(read_schema.clone())
            .create_physical_expr(&expr)
            .map_err(|e| Error::InvalidExpression {
                column: output.clone(),
                message: e.to_string(),
            })?;
        let data_type =
            physical
                .data_type(read_schema.as_ref())
                .map_err(|e| Error::InvalidExpression {
                    column: output.clone(),
                    message: e.to_string(),
                })?;

        // A projected column keeps its nullability; a computed value is
        // nullable whatever the evaluator reports for a given batch.
        let nullable = match projected_path(&expr).as_deref() {
            Some([column]) => source_schema
                .field_with_name(column)
                .map(|f| f.is_nullable())
                .unwrap_or(true),
            _ => true,
        };
        let mut field = ArrowField::new(output, data_type, nullable);
        // Identity projections keep descriptive field metadata (blob markers);
        // computed values carry none. Structural declarations never come along.
        if let Some(source_field) = projected_field(&expr, &source_schema) {
            field = field.with_metadata(source_field.metadata().clone());
        }
        if let Some(path) = projected_path(&expr)
            && let [column] = path.as_slice()
        {
            lineage
                .entry(column.clone())
                .or_default()
                .push(output.clone());
        }
        fields.push(without_declarations(&field));
        inputs.extend(expr_inputs);
        declared.push(output);
    }

    if let Some(filter) = filter.as_deref() {
        inputs.extend(plan_filter(&source_schema, filter)?);
    }

    let definition = MaterializedViewDefinition {
        source_table: definition.source_table.clone(),
        source_namespace: definition.source_namespace.clone(),
        lateral: definition.lateral.clone(),
        projections: projections
            .into_iter()
            .map(|(output, expression)| ViewProjection { output, expression })
            .collect(),
        filter,
        group_by: Vec::new(),
        limit,
    };
    let mut inputs: Vec<String> = inputs
        .iter()
        .map(|input| recorded_input(unnest.as_ref(), input))
        .collect();
    inputs.sort();
    inputs.dedup();
    Ok(Planned {
        definition,
        fields,
        lineage,
        inputs,
    })
}

/// Check that `filter` is a boolean predicate over `source_schema`, returning
/// the columns it reads.
pub(crate) fn plan_filter(source_schema: &SchemaRef, filter: &str) -> Result<Vec<String>> {
    let expr = Planner::new(source_schema.clone())
        .parse_filter(filter)
        .map_err(|e| Error::InvalidInput {
            message: format!("invalid view filter: {e}"),
        })?;
    let filter_inputs = resolve_inputs(source_schema, &expr, |message| Error::InvalidInput {
        message: format!("invalid view filter: {message}"),
    })?;
    // A committed filter has to be usable as a predicate.
    let read_schema = project_schema(source_schema, &filter_inputs);
    let data_type = Planner::new(read_schema.clone())
        .create_physical_expr(&expr)
        .map_err(|e| Error::InvalidInput {
            message: format!("invalid view filter: {e}"),
        })?
        .data_type(read_schema.as_ref())
        .map_err(|e| Error::InvalidInput {
            message: format!("invalid view filter: {e}"),
        })?;
    if data_type != DataType::Boolean {
        return Err(Error::InvalidInput {
            message: format!("view filter must be a boolean predicate, not {data_type}"),
        });
    }
    Ok(filter_inputs)
}

/// The schema a projection over an unnested view is planned against: the
/// source's, with the list column replaced by its element type under the
/// alias.
pub(crate) fn flattened_schema(
    source_schema: &ArrowSchema,
    unnest: &ViewUnnest,
) -> Result<SchemaRef> {
    let field = source_schema
        .field_with_name(&unnest.column)
        .map_err(|_| Error::InvalidInput {
            message: format!(
                "UNNEST column '{}' is not a column of the source",
                unnest.column
            ),
        })?;
    let DataType::List(element) = field.data_type() else {
        return Err(Error::InvalidInput {
            message: format!(
                "UNNEST column '{}' is {}, not a list",
                unnest.column,
                field.data_type()
            ),
        });
    };
    if source_schema.field_with_name(&unnest.alias).is_ok() {
        return Err(Error::InvalidInput {
            message: format!(
                "UNNEST alias '{}' collides with a source column",
                unnest.alias
            ),
        });
    }
    let fields: Vec<ArrowField> = source_schema
        .fields()
        .iter()
        .map(|f| {
            if f.name() == &unnest.column {
                ArrowField::new(&unnest.alias, element.data_type().clone(), true)
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Ok(Arc::new(ArrowSchema::new(fields)))
}

/// The root of a possibly-dotted column path: `metadata.age` -> `metadata`.
fn root(path: &str) -> &str {
    path.split('.').next().unwrap_or(path)
}

/// The field a dotted `path` names, walking struct children.
fn field_at_path(schema: &ArrowSchema, path: &str) -> Option<ArrowField> {
    let mut parts = path.split('.');
    let mut field = schema.field_with_name(parts.next()?).ok()?.clone();
    for part in parts {
        let DataType::Struct(children) = field.data_type() else {
            return None;
        };
        field = children.iter().find(|f| f.name() == part)?.as_ref().clone();
    }
    Some(field)
}

/// The source column recorded as read for `path`: for an unnested view a
/// read through the alias is a read of the list column.
fn recorded_input(unnest: Option<&ViewUnnest>, path: &str) -> String {
    match unnest {
        Some(unnest) if root(path) == unnest.alias => unnest.column.clone(),
        _ => path.to_string(),
    }
}

/// The columns `expr` reads, kept as the planner reports them (a nested
/// reference stays a dotted path) but resolved by root field.
/// Embedding configuration rewritten for the view: entries whose columns the
/// view projects directly are kept under the view's names; the rest describe
/// a table that does not exist and are dropped.
fn embedding_config_for_view(raw: &str, lineage: &Lineage) -> Option<String> {
    // Every representation the writers use: the Python bindings name the
    // destination `vector_column`, the Rust definition `dest_column`, and the
    // Node bindings spell both halves in camelCase.
    const SOURCE_KEYS: [&str; 2] = ["source_column", "sourceColumn"];
    const DEST_KEYS: [&str; 4] = ["vector_column", "dest_column", "vectorColumn", "destColumn"];

    let entries: Vec<serde_json::Value> = serde_json::from_str(raw).ok()?;
    let mut kept = Vec::new();
    for entry in &entries {
        let Some(object) = entry.as_object() else {
            continue;
        };
        let named = |keys: &[&str]| {
            let key = keys.iter().find(|key| object.contains_key(**key))?;
            let outputs = lineage.get(object.get(*key)?.as_str()?)?;
            Some(((*key).to_string(), outputs))
        };
        let (Some((source_key, sources)), Some((dest_key, dests))) =
            (named(&SOURCE_KEYS), named(&DEST_KEYS))
        else {
            continue;
        };
        // A projection may give one source column several names, and every
        // pairing of the two is a real relationship in the view.
        for source in sources {
            for dest in dests {
                let mut object = object.clone();
                object.insert(source_key.clone(), source.clone().into());
                object.insert(dest_key.clone(), dest.clone().into());
                kept.push(serde_json::Value::Object(object));
            }
        }
    }
    (!kept.is_empty()).then(|| serde_json::Value::Array(kept).to_string())
}

/// Lancedb's column definitions rewritten for the view: positional, one per
/// view field. Directly projected embedding columns keep their definition
/// under the view's names; everything else is physical. `None` = no key.
fn column_definitions_for_view(
    raw: &str,
    source_schema: &ArrowSchema,
    view_fields: &[ArrowField],
    lineage: &Lineage,
) -> Option<String> {
    let source_definitions: Vec<ColumnDefinition> = serde_json::from_str(raw).ok()?;
    // The definition sits on the column the function writes, so the source
    // schema's field name at that position is the embedding's destination.
    let embeddings: HashMap<&str, &EmbeddingDefinition> = source_schema
        .fields()
        .iter()
        .zip(&source_definitions)
        .filter_map(|(field, definition)| match &definition.kind {
            ColumnKind::Embedding(embedding) => Some((field.name().as_str(), embedding)),
            ColumnKind::Physical => None,
        })
        .collect();
    let sources: HashMap<&str, &str> = lineage
        .iter()
        .flat_map(|(source, outputs)| outputs.iter().map(move |o| (o.as_str(), source.as_str())))
        .collect();

    let mut kept = false;
    let definitions: Vec<ColumnDefinition> = view_fields
        .iter()
        .map(|field| {
            let kind = embedding_for_output(field.name(), &embeddings, &sources, lineage)
                .map(|embedding| {
                    kept = true;
                    ColumnKind::Embedding(embedding)
                })
                .unwrap_or(ColumnKind::Physical);
            ColumnDefinition { kind }
        })
        .collect();
    kept.then(|| serde_json::to_string(&definitions).ok())?
}

/// The embedding `output` inherits, renamed to the view's columns. `None`
/// unless the view projects both the function's input and its output
/// directly: anything else advertises a column the view cannot recompute.
fn embedding_for_output(
    output: &str,
    embeddings: &HashMap<&str, &EmbeddingDefinition>,
    sources: &HashMap<&str, &str>,
    lineage: &Lineage,
) -> Option<EmbeddingDefinition> {
    let embedding = embeddings.get(sources.get(output)?)?;
    // The input may be projected several times; the first name the view gives
    // it is the one this column is defined against.
    let input = lineage.get(&embedding.source_column)?.first()?;
    Some(EmbeddingDefinition {
        source_column: input.clone(),
        dest_column: Some(output.to_string()),
        embedding_name: embedding.embedding_name.clone(),
    })
}

/// The source field a projection reads directly, if it reads one: a bare
/// column, or a path of struct field accesses over one. Anything computed
/// produces a new value and has no source field.
fn projected_field<'a>(
    expr: &datafusion_expr::Expr,
    schema: &'a ArrowSchema,
) -> Option<&'a ArrowField> {
    let path = projected_path(expr)?;
    let mut segments = path.iter();
    let mut field = schema.field_with_name(segments.next()?).ok()?;
    for segment in segments {
        let DataType::Struct(children) = field.data_type() else {
            return None;
        };
        field = children.iter().find(|c| c.name() == segment)?;
    }
    Some(field)
}

/// The dotted path a projection reads directly, root first.
fn projected_path(expr: &datafusion_expr::Expr) -> Option<Vec<String>> {
    let mut path = Vec::new();
    let mut node = expr;
    loop {
        match node {
            datafusion_expr::Expr::Column(column) => {
                path.push(column.name.clone());
                break;
            }
            // `a.b` parses to get_field(a, "b"), nested for deeper paths.
            datafusion_expr::Expr::ScalarFunction(call) if call.func.name() == "get_field" => {
                let [
                    inner,
                    datafusion_expr::Expr::Literal(ScalarValue::Utf8(Some(name)), _),
                ] = call.args.as_slice()
                else {
                    return None;
                };
                path.push(name.clone());
                node = inner;
            }
            _ => return None,
        }
    }

    path.reverse();
    Some(path)
}

/// `field` without the metadata that declares how a column is written, at
/// every depth; descriptive metadata (blob markers) stays. A view is written
/// by refresh alone, and its always-nullable fields contradict declarations.
fn is_declaration(key: &str) -> bool {
    key.starts_with(SCHEMA_DECLARATION_META_PREFIX)
        || key == LANCE_FIELD_ID_KEY
        || crate::table::computed_columns::is_declaration_key(key)
}

fn without_declarations(field: &ArrowField) -> ArrowField {
    let metadata: HashMap<String, String> = field
        .metadata()
        .iter()
        .filter(|(key, _)| !is_declaration(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let strip = |child: &FieldRef| Arc::new(without_declarations(child));
    // Every Arrow variant that carries a field carries that field's metadata
    // with it, so all of them are descended.
    let data_type = match field.data_type() {
        DataType::Struct(children) => DataType::Struct(children.iter().map(strip).collect()),
        DataType::List(child) => DataType::List(strip(child)),
        DataType::ListView(child) => DataType::ListView(strip(child)),
        DataType::LargeList(child) => DataType::LargeList(strip(child)),
        DataType::LargeListView(child) => DataType::LargeListView(strip(child)),
        DataType::Map(entries, sorted) => DataType::Map(strip(entries), *sorted),
        DataType::FixedSizeList(child, len) => DataType::FixedSizeList(strip(child), *len),
        DataType::Union(variants, mode) => DataType::Union(
            variants
                .iter()
                .map(|(id, child)| (id, strip(child)))
                .collect(),
            *mode,
        ),
        DataType::RunEndEncoded(run_ends, values) => {
            DataType::RunEndEncoded(strip(run_ends), strip(values))
        }
        other => other.clone(),
    };
    ArrowField::new(field.name(), data_type, field.is_nullable()).with_metadata(metadata)
}

fn resolve_inputs(
    schema: &ArrowSchema,
    expr: &datafusion_expr::Expr,
    error: impl Fn(String) -> Error,
) -> Result<Vec<String>> {
    let mut inputs = Planner::column_names_in_expr(expr);
    inputs.sort();
    inputs.dedup();
    for input in &inputs {
        if schema.field_with_name(root(input)).is_err() {
            return Err(error(format!("unknown column '{input}'")));
        }
    }
    Ok(inputs)
}

/// Project the root fields of `columns`, deduplicated, in schema order.
fn project_schema(schema: &ArrowSchema, columns: &[String]) -> SchemaRef {
    let roots: std::collections::HashSet<&str> = columns.iter().map(|c| root(c)).collect();
    let fields: Vec<ArrowField> = schema
        .fields()
        .iter()
        .filter(|f| roots.contains(f.name().as_str()))
        .map(|f| f.as_ref().clone())
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

/// A validated view declaration, ready to become a table: the projected
/// fields with the definition stamped in metadata.
/// Produced only by [`prepare_declaration`].
#[derive(Clone)]
pub struct PreparedDeclaration {
    schema: SchemaRef,
    definition: MaterializedViewDefinition,
    /// The source schema and the projection lineage, for placing a computed
    /// column's inputs; `internal_inputs` counts the projections
    /// [`PreparedDeclaration::input_column`] added after the declared ones.
    source_schema: SchemaRef,
    lineage: Lineage,
    internal_inputs: usize,
    /// The source's own database: the only place
    /// [`PreparedDeclaration::create`] will put the view, because refresh
    /// resolves the recorded source coordinate through the view's database.
    database: Arc<dyn Database>,
}

impl std::fmt::Debug for PreparedDeclaration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedDeclaration")
            .field("definition", &self.definition)
            .finish_non_exhaustive()
    }
}

impl PreparedDeclaration {
    /// The query the declaration records.
    pub fn definition(&self) -> &MaterializedViewDefinition {
        &self.definition
    }

    /// The schema the view will have: the declared columns in order, any
    /// internal projections added by [`PreparedDeclaration::input_column`].
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The view column that holds `source_column` for a computed column to
    /// read: the column the view projects it to, if any, otherwise an
    /// internal projection added here, named by [`input_column_name`].
    pub fn input_column(&mut self, source_column: &str) -> Result<String> {
        // A grouped view's rows are groups; no source row carries a value into one.
        if self.definition.is_grouped() {
            return Err(Error::InvalidInput {
                message: format!(
                    "a computed column on a grouped view cannot read source column \
                     '{source_column}'; declare it on a view over the grouped view"
                ),
            });
        }
        if let Some(output) = self.lineage.get(source_column).and_then(|o| o.first()) {
            return Ok(output.clone());
        }
        let name = input_column_name(source_column);
        let field = field_at_path(&self.source_schema, source_column).ok_or_else(|| {
            Error::InvalidInput {
                message: format!("the source has no column '{source_column}' to read"),
            }
        })?;
        if self.schema.field_with_name(&name).is_ok() {
            return Err(Error::ColumnAlreadyExists { name });
        }
        let mut fields: Vec<ArrowField> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(without_declarations(&field.with_name(name.clone())));
        self.definition.projections.push(ViewProjection {
            output: name.clone(),
            expression: source_column
                .split('.')
                .map(query::ident_sql)
                .collect::<Vec<_>>()
                .join("."),
        });
        self.lineage
            .entry(source_column.to_string())
            .or_default()
            .push(name.clone());
        self.internal_inputs += 1;
        let mut metadata = self.schema.metadata().clone();
        rewrite_column_definitions(&mut metadata, self.schema.as_ref(), &fields)?;
        metadata.insert(
            DEFINITION_META_KEY.to_string(),
            definition_to_metadata(&self.definition)?,
        );
        self.schema = Arc::new(ArrowSchema::new_with_metadata(fields, metadata));
        Ok(name)
    }

    /// Add computed columns, each at its position among the declared
    /// columns, with the bindings any of them name.
    ///
    /// Refresh never computes such a column: every row it writes carries
    /// NULL there, and the declaration's owner fills it, `refresh_column`
    /// for a SQL declaration. A commit that fills only computed columns is
    /// the one commit on a view refresh does not treat as drift. Declarations
    /// are validated over the assembled schema, and read only columns the
    /// view holds (see [`PreparedDeclaration::input_column`]).
    ///
    /// ```no_run
    /// # #![recursion_limit = "256"]
    /// # use std::collections::HashMap;
    /// # use arrow_schema::{DataType, Field};
    /// # use lancedb::materialized_view::prepare_declaration;
    /// # use lancedb::table::computed_columns::{
    /// #     COMPUTED_COLUMN_META_KEY, EXPRESSION_META_KEY, INPUTS_META_KEY, KIND_META_KEY, SQL_KIND,
    /// # };
    /// # async fn declare(source: &lancedb::Table) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut prepared = prepare_declaration(
    ///     source,
    ///     Some(&[("id".into(), "id".into())]),
    ///     None,
    ///     None,
    /// )
    /// .await?;
    /// // `text` is not projected; the view holds it internally for the column to read.
    /// let text = prepared.input_column("text")?;
    /// let length = Field::new("length", DataType::Int32, true).with_metadata(HashMap::from([
    ///     (COMPUTED_COLUMN_META_KEY.into(), "true".into()),
    ///     (KIND_META_KEY.into(), SQL_KIND.into()),
    ///     (EXPRESSION_META_KEY.into(), format!("length({text})")),
    ///     (INPUTS_META_KEY.into(), format!("[\"{text}\"]")),
    /// ]));
    /// let view = prepared
    ///     .with_computed_columns(vec![(1, length)], &[])?
    ///     .create("lengths")
    ///     .await?;
    /// view.refresh().execute().await?; // rows land with `length` NULL
    /// view.table().refresh_column("length").await?; // filled
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_computed_columns(
        mut self,
        columns: Vec<(usize, ArrowField)>,
        bindings: &[FunctionBinding],
    ) -> Result<Self> {
        let invalid = |message: String| Error::InvalidInput { message };
        if columns.is_empty() {
            return Err(invalid("at least one computed column is needed".into()));
        }
        if !computed_columns(&self.schema).is_empty() {
            return Err(invalid(
                "computed columns were already declared on this view".into(),
            ));
        }
        if self.definition.projections.is_empty() {
            return Err(invalid(
                "a view of computed columns alone must read at least one source column".into(),
            ));
        }
        let visible_count = self.visible_count();
        let mut fields: Vec<ArrowField> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let mut columns = columns;
        columns.sort_by_key(|(position, _)| *position);
        for (inserted, (position, field)) in columns.iter().enumerate() {
            let name = field.name().as_str();
            if name.starts_with(INPUT_COLUMN_PREFIX) {
                return Err(invalid(format!("view column name '{name}' is reserved")));
            }
            if fields.iter().any(|f| f.name() == name) {
                return Err(Error::ColumnAlreadyExists {
                    name: name.to_string(),
                });
            }
            if !field.is_nullable() {
                return Err(invalid(format!(
                    "computed column '{name}' must be nullable until a refresh fills it"
                )));
            }
            if computed_column_from_field(field).is_none() {
                return Err(invalid(format!(
                    "column '{name}' does not carry a computed-column declaration"
                )));
            }
            let limit = visible_count + inserted;
            if *position > limit {
                return Err(invalid(format!(
                    "computed column '{name}' is placed at {position}, past the view's {limit} columns"
                )));
            }
            // Positions index the select list, which counts the computed
            // columns already inserted before this one.
            fields.insert(*position, field.clone());
        }
        let mut metadata = self.schema.metadata().clone();
        if !bindings.is_empty() {
            metadata.insert(
                FUNCTION_BINDINGS_META_KEY.to_string(),
                function_bindings_metadata(bindings)?,
            );
        }
        rewrite_column_definitions(&mut metadata, self.schema.as_ref(), &fields)?;
        let schema = ArrowSchema::new_with_metadata(fields, metadata);
        ensure_declarations_are_planned(&schema)?;
        self.schema = Arc::new(schema);
        Ok(self)
    }

    /// Columns the declaration lists: everything before the internal
    /// projections.
    fn visible_count(&self) -> usize {
        self.definition.projections.len() - self.internal_inputs
            + computed_columns(&self.schema).len()
    }

    /// Create the view table and verify it, consuming the declaration.
    ///
    /// The view goes at the root of the source's own database, where refresh
    /// resolves the recorded source coordinate.
    pub async fn create(self, name: &str) -> Result<MaterializedView> {
        self.create_in(&[], name).await
    }

    /// Create the view in `namespace_path`, empty for the root namespace.
    /// Otherwise [`PreparedDeclaration::create`].
    pub async fn create_in(
        self,
        namespace_path: &[String],
        name: &str,
    ) -> Result<MaterializedView> {
        let empty: Vec<std::result::Result<arrow_array::RecordBatch, arrow_schema::ArrowError>> =
            vec![];
        let reader: Box<dyn arrow_array::RecordBatchReader + Send> =
            Box::new(arrow_array::RecordBatchIterator::new(empty, self.schema));
        let mut request = CreateTableRequest::new(name.to_string(), Box::new(reader));
        request.namespace_path = namespace_path.to_vec();
        let table = self.database.clone().create_table(request).await?;
        let table = Table::new(table, self.database);
        Ok(MaterializedView {
            table,
            definition: self.definition,
        })
    }
}

/// Column definitions are positional over the view schema: carry each
/// field's entry to its place in `fields`, physical for a field that had none.
fn rewrite_column_definitions(
    metadata: &mut HashMap<String, String>,
    previous: &ArrowSchema,
    fields: &[ArrowField],
) -> Result<()> {
    let Some(raw) = metadata.get(COLUMN_DEFINITIONS_META_KEY).cloned() else {
        return Ok(());
    };
    let definitions: Vec<ColumnDefinition> =
        serde_json::from_str(&raw).map_err(|e| Error::Runtime {
            message: format!("unreadable column definitions on the view: {e}"),
        })?;
    let by_name: HashMap<&str, &ColumnDefinition> = previous
        .fields()
        .iter()
        .zip(&definitions)
        .map(|(field, definition)| (field.name().as_str(), definition))
        .collect();
    let rewritten: Vec<ColumnDefinition> = fields
        .iter()
        .map(|field| {
            by_name
                .get(field.name().as_str())
                .map(|d| (*d).clone())
                .unwrap_or(ColumnDefinition {
                    kind: ColumnKind::Physical,
                })
        })
        .collect();
    metadata.insert(
        COLUMN_DEFINITIONS_META_KEY.to_string(),
        serde_json::to_string(&rewritten).map_err(|e| Error::Runtime {
            message: format!("failed to serialize column definitions: {e}"),
        })?,
    );
    Ok(())
}

/// Validate a view declaration over `source`: `projections` as
/// `(name, SQL expression)` pairs, `None` selecting every source column.
/// See [`MaterializedViewDefinition::from_sql`] for the query shape;
/// [`prepare_definition`] takes a parsed query directly.
pub async fn prepare_declaration(
    source: &Table,
    projections: Option<&[(String, String)]>,
    filter: Option<&str>,
    limit: Option<u64>,
) -> Result<PreparedDeclaration> {
    let definition = MaterializedViewDefinition {
        source_table: source.name().to_string(),
        source_namespace: source.namespace().to_vec(),
        lateral: None,
        projections: match projections {
            None => vec![ViewProjection::star()],
            Some(projections) => projections
                .iter()
                .map(|(output, expression)| ViewProjection {
                    output: output.clone(),
                    expression: expression.clone(),
                })
                .collect(),
        },
        filter: filter.map(str::to_string),
        group_by: Vec::new(),
        limit,
    };
    prepare_definition(source, definition).await
}

/// Validate `definition` over `source`, the table it names. The declaration
/// is planned against the source as refresh will reach it, and the result
/// creates the view with [`PreparedDeclaration::create`]. A query calling a
/// Function in `FROM` position is not supported by local refresh.
///
/// ```
/// # #![recursion_limit = "256"]
/// use lancedb::materialized_view::{MaterializedViewDefinition, prepare_definition};
///
/// # async fn declare(events: &lancedb::Table) -> Result<(), Box<dyn std::error::Error>> {
/// let definition = MaterializedViewDefinition::from_sql(
///     "SELECT id, t.tag AS tag FROM events, UNNEST(tags) AS t WHERE id > 0",
/// )?;
/// let view = prepare_definition(events, definition)
///     .await?
///     .create("event_tags")
///     .await?;
/// view.refresh().execute().await?;
/// # Ok(())
/// # }
/// ```
///
/// A grouped view holds one row per group and is recomputed in full on
/// every refresh after a source change:
///
/// ```
/// # #![recursion_limit = "256"]
/// use lancedb::materialized_view::{MaterializedViewDefinition, prepare_definition};
///
/// # async fn declare(events: &lancedb::Table) -> Result<(), Box<dyn std::error::Error>> {
/// let definition = MaterializedViewDefinition::from_sql(
///     "SELECT kind, count(*) AS n, array_agg(id) AS ids FROM events GROUP BY kind",
/// )?;
/// let view = prepare_definition(events, definition)
///     .await?
///     .create("events_by_kind")
///     .await?;
/// view.refresh().execute().await?;
/// # Ok(())
/// # }
/// ```
pub async fn prepare_definition(
    source: &Table,
    definition: MaterializedViewDefinition,
) -> Result<PreparedDeclaration> {
    if definition.source_table != source.name() || definition.source_namespace != source.namespace()
    {
        return Err(Error::InvalidInput {
            message: format!(
                "the query reads '{}' in namespace {:?}, but the source handle is '{}' in {:?}",
                definition.source_table,
                definition.source_namespace,
                source.name(),
                source.namespace()
            ),
        });
    }
    prepare_with(source, definition).await
}

async fn prepare_with(
    source: &Table,
    definition: MaterializedViewDefinition,
) -> Result<PreparedDeclaration> {
    let Some(caller_native) = source.as_native() else {
        return Err(Error::NotSupported {
            message: "materialized views are supported only on local databases".into(),
        });
    };
    let source_namespace = source.namespace().to_vec();
    let database = source
        .database_opt()
        .ok_or_else(|| Error::InvalidInput {
            message: "the source was not opened through a database connection".into(),
        })?
        .clone();

    // Canonicalize: resolve the recorded coordinate exactly the way a
    // refresh will, and plan from what it reaches. A handle that does not
    // resolve back to itself must not be declared under this name.
    let resolved = database
        .open_table(OpenTableRequest {
            name: source.name().to_string(),
            namespace_path: source_namespace.clone(),
            index_cache_size: None,
            lance_read_params: None,
            location: None,
            namespace_client: None,
            managed_versioning: None,
        })
        .await?;
    let resolved = Table::new(resolved, database.clone());
    let Some(native) = resolved.as_native() else {
        return Err(Error::NotSupported {
            message: "materialized views are supported only on local databases".into(),
        });
    };
    let caller_uri = caller_native.dataset.get().await?.uri().to_string();
    let resolved_uri = native.dataset.get().await?.uri().to_string();
    if caller_uri != resolved_uri {
        return Err(Error::InvalidInput {
            message: format!(
                "the source handle does not resolve to itself through its \
                 database: '{}' resolves to '{resolved_uri}', but the handle \
                 reads '{caller_uri}'",
                source.name()
            ),
        });
    }
    refresh::ensure_no_mem_wal(
        native.dataset.get().await?.as_ref(),
        "source table",
        resolved.name(),
    )
    .await?;
    // The internal-input prefix belongs to the declaration alone; the
    // replan at refresh sees those projections and must accept them.
    if let Some(reserved) = definition
        .projections
        .iter()
        .find(|p| p.output.starts_with(INPUT_COLUMN_PREFIX))
    {
        return Err(Error::InvalidInput {
            message: format!("view column name '{}' is reserved", reserved.output),
        });
    }
    let source_schema = resolved.schema().await?;
    let source_metadata = source_schema.metadata().clone();
    let Planned {
        definition,
        fields,
        lineage,
        ..
    } = plan(source_schema.clone(), &definition)?;
    if definition.is_grouped() {
        grouped::check(native.dataset.get().await?.as_ref(), &definition).await?;
    }
    // What later projections (`input_column`) are planned against: for an
    // unnested view the flattened schema, where the alias is a column.
    let planning_schema = match physical_unnest(&definition)? {
        None => source_schema.clone(),
        Some(unnest) => flattened_schema(&source_schema, &unnest)?,
    };
    // Only column-describing metadata comes along: structural declarations
    // describe how a table is written, and a view is written by refresh alone.
    let mut metadata: HashMap<String, String> = HashMap::new();
    if let Some(raw) = source_metadata.get(EMBEDDING_FUNCTIONS_META_KEY)
        && let Some(rewritten) = embedding_config_for_view(raw, &lineage)
    {
        metadata.insert(EMBEDDING_FUNCTIONS_META_KEY.to_string(), rewritten);
    }
    if let Some(raw) = source_metadata.get(COLUMN_DEFINITIONS_META_KEY)
        && let Some(rewritten) = column_definitions_for_view(raw, &source_schema, &fields, &lineage)
    {
        metadata.insert(COLUMN_DEFINITIONS_META_KEY.to_string(), rewritten);
    }
    metadata.insert(
        DEFINITION_META_KEY.to_string(),
        definition_to_metadata(&definition)?,
    );
    Ok(PreparedDeclaration {
        schema: Arc::new(ArrowSchema::new_with_metadata(fields, metadata)),
        definition,
        source_schema: planning_schema,
        lineage,
        internal_inputs: 0,
        database,
    })
}

/// Builds a materialized view. Created by
/// [`Connection::create_materialized_view`].
pub struct CreateMaterializedViewBuilder {
    connection: Connection,
    name: String,
    namespace: Vec<String>,
    source: String,
    source_namespace: Vec<String>,
    projections: Vec<(String, String)>,
    filter: Option<String>,
    limit: Option<u64>,
    with_no_data: bool,
}

impl CreateMaterializedViewBuilder {
    pub(crate) fn new(connection: Connection, name: String, source: String) -> Self {
        Self {
            connection,
            name,
            namespace: Vec::new(),
            source,
            source_namespace: Vec::new(),
            projections: Vec::new(),
            filter: None,
            limit: None,
            with_no_data: false,
        }
    }

    /// The namespace to create the view in. Defaults to the root namespace.
    pub fn namespace(mut self, namespace_path: Vec<String>) -> Self {
        self.namespace = namespace_path;
        self
    }

    /// The namespace holding the source table; recorded in the definition
    /// for refresh to resolve. Defaults to the root namespace.
    pub fn source_namespace(mut self, namespace_path: Vec<String>) -> Self {
        self.source_namespace = namespace_path;
        self
    }

    /// The view's columns, as `(name, SQL expression)` pairs. Not calling
    /// this selects every source column, expanded at creation time.
    pub fn select(
        mut self,
        columns: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.projections = columns
            .into_iter()
            .map(|(output, expression)| (output.into(), expression.into()))
            .collect();
        self
    }

    /// Only source rows matching the SQL predicate appear in the view.
    pub fn only_if(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Cap the view at `limit` rows, in materialization order.
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Create only the definition and empty backing table. By default create
    /// also waits for the initial refresh so the returned view is populated.
    pub fn with_no_data(mut self, with_no_data: bool) -> Self {
        self.with_no_data = with_no_data;
        self
    }

    fn query(&self) -> Result<String> {
        fn quote(name: &str) -> String {
            format!("\"{}\"", name.replace('"', "\"\""))
        }

        let projection = if self.projections.is_empty() {
            "*".to_string()
        } else {
            self.projections
                .iter()
                .map(|(output, expression)| format!("{expression} AS {}", quote(output)))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let source = self
            .source_namespace
            .iter()
            .chain(std::iter::once(&self.source))
            .map(|part| quote(part))
            .collect::<Vec<_>>()
            .join(".");
        let mut query = format!("SELECT {projection} FROM {source}");
        if let Some(filter) = &self.filter {
            query.push_str(" WHERE ");
            query.push_str(filter);
        }
        if let Some(limit) = self.limit {
            query.push_str(&format!(" LIMIT {limit}"));
        }
        Ok(query)
    }

    /// Submit creation and initial population, returning a [`Job`] that
    /// settles when the view is ready.
    pub async fn execute_async(self) -> Result<Job> {
        if self.connection.uri().starts_with("db://") {
            return self
                .connection
                .database()
                .create_materialized_view_async(CreateMaterializedViewRequest {
                    name: self.name.clone(),
                    namespace_path: self.namespace.clone(),
                    query: self.query()?,
                    with_no_data: self.with_no_data,
                })
                .await;
        }
        Ok(Job::spawned(tokio::spawn(async move {
            self.execute_native().await.map(|_| ())
        })))
    }

    /// Create and populate the view, waiting until it is ready.
    pub async fn execute(self) -> Result<MaterializedView> {
        if !self.connection.uri().starts_with("db://") {
            return self.execute_native().await;
        }
        let connection = self.connection.clone();
        let name = self.name.clone();
        let namespace = self.namespace.clone();
        self.execute_async().await?.wait().await?;
        let table = connection
            .open_table(name)
            .namespace(namespace)
            .execute()
            .await?;
        MaterializedView::from_table(table).await
    }

    async fn execute_native(self) -> Result<MaterializedView> {
        let source = self
            .connection
            .open_table(&self.source)
            .namespace(self.source_namespace.clone())
            .execute()
            .await?;
        let prepared = prepare_declaration(
            &source,
            (!self.projections.is_empty()).then_some(self.projections.as_slice()),
            self.filter.as_deref(),
            self.limit,
        )
        .await?;
        let view = prepared.create_in(&self.namespace, &self.name).await?;
        if !self.with_no_data {
            view.refresh().execute().await?;
        }
        Ok(view)
    }
}

/// A handle on a materialized view: the view table plus its parsed definition.
#[derive(Debug, Clone)]
pub struct MaterializedView {
    table: Table,
    definition: MaterializedViewDefinition,
}

impl MaterializedView {
    /// Interpret `table` as a materialized view: [`Error::NotAMaterializedView`]
    /// for a plain table, [`Error::NotSupported`] for a query this version
    /// cannot refresh.
    pub async fn from_table(table: Table) -> Result<Self> {
        let info = table.base_table().materialized_view_info().await?;
        Ok(Self {
            table,
            definition: info.definition,
        })
    }

    /// The view, as the table it is. Queries, indexes and search all apply.
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// The view's table name.
    pub fn name(&self) -> &str {
        self.table.name()
    }

    /// The query that defines the view.
    pub fn definition(&self) -> &MaterializedViewDefinition {
        &self.definition
    }

    /// Recompute the view from its source.
    ///
    /// Every refresh rebuilds the view from the selected source version.
    ///
    /// ```no_run
    /// # #![recursion_limit = "256"]
    /// # use lancedb::materialized_view::MaterializedView;
    /// # async fn refresh(view: &MaterializedView) -> Result<(), Box<dyn std::error::Error>> {
    /// let result = view.refresh().execute().await?;
    /// println!("{:?}: {} rows", result.mode, result.rows_written);
    /// # Ok(())
    /// # }
    /// ```
    pub fn refresh(&self) -> RefreshMaterializedViewBuilder {
        RefreshMaterializedViewBuilder {
            view: self.clone(),
            source_version: None,
        }
    }
}

/// Builds a refresh. Created by [`MaterializedView::refresh`].
pub struct RefreshMaterializedViewBuilder {
    view: MaterializedView,
    source_version: Option<u64>,
}

impl RefreshMaterializedViewBuilder {
    /// Refresh to this source table version instead of the latest.
    pub fn source_version(mut self, version: u64) -> Self {
        self.source_version = Some(version);
        self
    }

    /// Submit the refresh and return a job that settles with its result.
    pub async fn execute_async(self) -> Result<Job<RefreshMaterializedViewResult>> {
        if self.view.table.as_native().is_none() {
            return self
                .view
                .table
                .base_table()
                .refresh_materialized_view_async(self.source_version)
                .await;
        }
        Ok(Job::spawned(tokio::spawn(async move {
            refresh::execute_refresh(&self.view.table, self.source_version).await
        })))
    }

    /// Refresh the view, waiting for the job to finish.
    pub async fn execute(self) -> Result<RefreshMaterializedViewResult> {
        if self.view.table.as_native().is_some() {
            return refresh::execute_refresh(&self.view.table, self.source_version).await;
        }
        self.execute_async().await?.wait().await
    }
}

impl Connection {
    /// Define a materialized view named `name` over `source`.
    ///
    /// The definition is recorded in schema metadata and the initial refresh
    /// is completed before this method returns. Use
    /// [`CreateMaterializedViewBuilder::with_no_data`] to skip population.
    ///
    /// ```no_run
    /// # #![recursion_limit = "256"]
    /// # use lancedb::Connection;
    /// # async fn create(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let view = conn
    ///     .create_materialized_view("loud_adults", "people")
    ///     .select([("name", "upper(name)"), ("age", "age")])
    ///     .only_if("age >= 18")
    ///     .execute()
    ///     .await?;
    /// assert_eq!(view.table().count_rows(None).await?, 1);
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_materialized_view(
        &self,
        name: impl Into<String>,
        source: impl Into<String>,
    ) -> CreateMaterializedViewBuilder {
        CreateMaterializedViewBuilder::new(self.clone(), name.into(), source.into())
    }

    /// Open the materialized view named `name`.
    pub async fn open_materialized_view(
        &self,
        name: impl Into<String>,
    ) -> Result<MaterializedView> {
        let table = self.open_table(name).execute().await?;
        MaterializedView::from_table(table).await
    }

    /// The names of materialized views in the root namespace.
    pub async fn list_materialized_views(&self) -> Result<Vec<String>> {
        self.database().list_materialized_views(&[]).await
    }

    /// Drop a materialized view.
    ///
    /// The view may become unavailable before its physical data is removed.
    /// Use [`Connection::drop_materialized_view_async`] to retain the cleanup
    /// job and wait for it explicitly.
    pub async fn drop_materialized_view(
        &self,
        name: impl AsRef<str>,
        namespace_path: &[String],
    ) -> Result<()> {
        let name = name.as_ref();
        if self.uri().starts_with("db://") {
            return self
                .database()
                .drop_materialized_view_async(name, namespace_path)
                .await
                .map(|_| ());
        }
        let table = self
            .open_table(name)
            .namespace(namespace_path.to_vec())
            .execute()
            .await?;
        MaterializedView::from_table(table).await?;
        self.drop_table(name, namespace_path).await
    }

    /// Start dropping a materialized view and return its cleanup job.
    ///
    /// This validates that the named resource is a materialized view rather
    /// than an ordinary table. Call [`Job::wait`] before assuming physical
    /// cleanup has finished. When the backend performs cleanup inline, the
    /// returned job is already finished and has no job ID.
    ///
    /// ```no_run
    /// # use lancedb::Connection;
    /// # async fn drop_view(conn: &Connection) -> lancedb::Result<()> {
    /// let job = conn.drop_materialized_view_async("daily_sales", &[]).await?;
    /// job.wait().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn drop_materialized_view_async(
        &self,
        name: impl AsRef<str>,
        namespace_path: &[String],
    ) -> Result<Job> {
        let name = name.as_ref();
        if self.uri().starts_with("db://") {
            return self
                .database()
                .drop_materialized_view_async(name, namespace_path)
                .await;
        }
        let table = self
            .open_table(name)
            .namespace(namespace_path.to_vec())
            .execute()
            .await?;
        MaterializedView::from_table(table).await?;
        self.drop_table_async(name, namespace_path).await
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::record_batch;

    use super::*;
    use crate::connect;

    #[tokio::test]
    async fn refresh_always_rebuilds_without_source_row_ids() {
        let conn = connect("memory://").execute().await.unwrap();
        let source = conn
            .create_table("src", record_batch!(("x", Int32, [1, 2])).unwrap())
            .execute()
            .await
            .unwrap();
        let view = conn
            .create_materialized_view("copy", "src")
            .execute()
            .await
            .unwrap();

        assert_eq!(view.table().count_rows(None).await.unwrap(), 2);
        assert!(
            view.table()
                .schema()
                .await
                .unwrap()
                .field_with_name("__source_row_id")
                .is_err()
        );

        source
            .add(record_batch!(("x", Int32, [3])).unwrap())
            .execute()
            .await
            .unwrap();
        let refreshed = view.refresh().execute().await.unwrap();
        assert_eq!(refreshed.mode, RefreshMode::Rebuild);
        assert_eq!(refreshed.rows_written, 3);
        assert_eq!(view.table().count_rows(None).await.unwrap(), 3);

        let unchanged = view.refresh().execute().await.unwrap();
        assert_eq!(unchanged.mode, RefreshMode::Rebuild);
        assert_eq!(unchanged.rows_written, 3);
    }
}
