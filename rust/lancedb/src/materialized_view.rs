// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Materialized views.
//!
//! A materialized view is a table whose contents are defined by a query over
//! one source table and maintained by refresh rather than by writes. Creation
//! commits an empty table carrying the kind-tagged definition in schema
//! metadata; a kind added later reads back as unrefreshable, not as a plain
//! table. Queries, indexes and search work on the view unchanged.

pub mod refresh;

#[cfg(test)]
mod differential;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field as ArrowField, FieldRef, Schema as ArrowSchema, SchemaRef};
use datafusion_common::ScalarValue;
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::{CommitBuilder, WriteDestination};
use lance_core::ROW_ID;
use lance_datafusion::planner::Planner;
use serde::{Deserialize, Serialize};

use crate::connection::Connection;
use crate::database::listing::OPT_NEW_TABLE_ENABLE_STABLE_ROW_IDS;
use crate::database::{CreateTableRequest, Database, OpenTableRequest};
use crate::embeddings::EmbeddingDefinition;
use crate::function::FunctionBinding;
use crate::table::Table;
use crate::table::computed_columns::{
    ComputedColumnKind, FUNCTION_BINDINGS_META_KEY, computed_column_from_field,
    function_bindings_metadata,
};
use crate::table::refresh::quote_identifier;
use crate::table::{ColumnDefinition, ColumnKind};
use crate::{Error, Result};

pub use refresh::{RefreshMaterializedViewResult, RefreshMode};

/// Schema metadata key holding the view definition, as kind-tagged JSON.
pub const DEFINITION_META_KEY: &str = "mv.definition";

/// Schema metadata key holding the view's incarnation: a token minted at each
/// physical creation of a view table, so a view dropped and recreated under
/// the same name and definition is still told apart from the one a caller
/// captured. A view whose metadata was replaced wholesale, or one declared
/// before tokens existed, carries none until its next refresh mints one.
pub const INCARNATION_META_KEY: &str = "mv.incarnation";

/// Schema metadata key holding the source table version the view was last
/// refreshed to. Absent until the first refresh.
pub const SOURCE_VERSION_META_KEY: &str = "mv.source_version";

/// Schema metadata key holding the wall-clock time of the last refresh,
/// in milliseconds since the epoch.
pub const REFRESHED_AT_MS_META_KEY: &str = "mv.refreshed_at_ms";

/// Column recording which source row produced each view row: the source's
/// stable `_rowid` at refresh time, which is why sources must keep stable
/// row ids.
pub const SOURCE_ROW_ID_COLUMN: &str = "__source_row_id";

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

/// Value of the definition's `kind` tag for the projected `select` form.
/// Reserved for root-namespace sources; see [`NAMESPACED_SELECT_KIND`].
pub const SELECT_KIND: &str = "select";

/// The `select` form over a namespaced source: its own kind, because released
/// readers drop unknown fields and resolve a `select` source at the root, so
/// this routes them to the [`MaterializedViewKind::Unrecognized`] refusal
/// instead of a wrong-table refresh.
pub const NAMESPACED_SELECT_KIND: &str = "namespaced_select";

/// The `select` form with function columns: outputs a registered Function
/// fills after each refresh (see [`PreparedDeclaration::with_function_columns`]).
/// Its own kind, so a reader without the concept refuses the view instead of
/// reporting its schema as not matching its definition.
pub const FUNCTION_SELECT_KIND: &str = "function_select";

/// [`FUNCTION_SELECT_KIND`] over a namespaced source.
pub const NAMESPACED_FUNCTION_SELECT_KIND: &str = "namespaced_function_select";

const KNOWN_KINDS: [&str; 4] = [
    SELECT_KIND,
    NAMESPACED_SELECT_KIND,
    FUNCTION_SELECT_KIND,
    NAMESPACED_FUNCTION_SELECT_KIND,
];

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

/// The query that defines a materialized view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializedViewDefinition {
    /// Name of the source table, in the same database as the view.
    pub source_table: String,
    /// Namespace path holding the source table; empty is the root namespace.
    /// A definition written before namespaced sources reads as root.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_namespace: Vec<String>,
    /// The projected output columns, in view schema order.
    pub projections: Vec<ViewProjection>,
    /// SQL predicate selecting the source rows the view holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    /// Cap on the number of rows the view holds, in materialization order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    /// Source columns the projections and filter read, derived at creation.
    #[serde(default)]
    pub inputs: Vec<String>,
    /// View columns a registered Function fills after each refresh, in view
    /// schema order after the projections. Refresh writes them as NULL and
    /// never reads them; the binding lives in the view's field and schema
    /// metadata exactly as a table's Function column does.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub function_columns: Vec<String>,
}

/// The kind tag a definition serializes under. Each combination of namespaced
/// source and function columns is its own kind, so a reader that predates
/// either concept refuses the view rather than misreading it.
fn definition_kind(definition: &MaterializedViewDefinition) -> &'static str {
    match (
        definition.source_namespace.is_empty(),
        definition.function_columns.is_empty(),
    ) {
        (true, true) => SELECT_KIND,
        (false, true) => NAMESPACED_SELECT_KIND,
        (true, false) => FUNCTION_SELECT_KIND,
        (false, false) => NAMESPACED_FUNCTION_SELECT_KIND,
    }
}

/// A view definition as read back from schema metadata. Non-exhaustive so a
/// kind added later is additive.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MaterializedViewKind {
    /// The projected `select` form.
    Select(MaterializedViewDefinition),
    /// A kind written by a newer version, reported so a caller can tell an
    /// unrefreshable view apart from a plain table. Nothing produces this.
    Unrecognized {
        /// The kind as it was found in the metadata.
        kind: String,
    },
}

/// Serialize `definition` into the kind-tagged form stored under
/// [`DEFINITION_META_KEY`].
pub(crate) fn definition_to_metadata(definition: &MaterializedViewDefinition) -> Result<String> {
    let mut value = serde_json::to_value(definition).map_err(|e| Error::Runtime {
        message: format!("failed to serialize view definition: {e}"),
    })?;
    value["kind"] = serde_json::Value::String(definition_kind(definition).to_string());
    Ok(value.to_string())
}

/// Read a view declaration off a schema metadata map, if it carries one.
/// `Ok(None)` for a plain table; a declaration that does not parse is an
/// error, because treating a view as plain would let it be rewritten.
pub fn materialized_view_kind(
    metadata: &HashMap<String, String>,
) -> Result<Option<MaterializedViewKind>> {
    let Some(raw) = metadata.get(DEFINITION_META_KEY) else {
        return Ok(None);
    };
    let unreadable = |e: &dyn std::fmt::Display| Error::Runtime {
        message: format!("unreadable materialized view definition: {e}"),
    };
    let value: serde_json::Value = serde_json::from_str(raw).map_err(|e| unreadable(&e))?;
    let kind = value
        .get("kind")
        .and_then(|k| k.as_str())
        .ok_or_else(|| unreadable(&"missing kind tag"))?;
    if !KNOWN_KINDS.contains(&kind) {
        return Ok(Some(MaterializedViewKind::Unrecognized {
            kind: kind.to_string(),
        }));
    }
    let kind = kind.to_string();
    let definition: MaterializedViewDefinition =
        serde_json::from_value(value).map_err(|e| unreadable(&e))?;
    // No correct writer produces a kind that disagrees with its definition.
    if kind != definition_kind(&definition) {
        return Err(unreadable(&format!(
            "kind '{kind}' does not match its source namespace {:?} and function columns {:?}",
            definition.source_namespace, definition.function_columns
        )));
    }
    Ok(Some(MaterializedViewKind::Select(definition)))
}

/// Resolve a definition against the source schema into the view's projected
/// fields, with `inputs` filled in. Everything statically checkable is
/// checked here rather than at refresh time. Empty `projections` selects
/// every source column as the schema stands now.
pub(crate) fn plan(
    source_schema: SchemaRef,
    source_table: &str,
    source_namespace: &[String],
    projections: &[(String, String)],
    filter: Option<&str>,
    limit: Option<u64>,
) -> Result<(MaterializedViewDefinition, Vec<ArrowField>, Lineage)> {
    let filter = filter
        .map(crate::expr::canonicalize_sql_predicate)
        .transpose()
        .map_err(|err| match err {
            Error::InvalidInput { message } => Error::InvalidInput {
                message: format!("invalid view filter: {message}"),
            },
            err => err,
        })?;
    let projections: Vec<(String, String)> = if projections.is_empty() {
        source_schema
            .fields()
            .iter()
            // A source that is itself a view carries its own provenance
            // column; the new view records its own, not a copy.
            .filter(|f| f.name() != SOURCE_ROW_ID_COLUMN)
            .map(|f| (f.name().clone(), quote_identifier(f.name())))
            .collect()
    } else {
        projections.to_vec()
    };

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
        if output == SOURCE_ROW_ID_COLUMN || output == ROW_ID {
            return Err(Error::InvalidInput {
                message: format!("view column name '{output}' is reserved"),
            });
        }

        let parsed = planner
            .parse_expr(expression)
            .map_err(|e| Error::InvalidExpression {
                column: output.clone(),
                message: e.to_string(),
            })?;
        // Before optimization: the simplifier folds a stable-but-not-immutable
        // call like now() into a literal, hiding it from the check while the
        // stored definition keeps the call.
        ensure_immutable(&parsed, |message| Error::InvalidExpression {
            column: output.clone(),
            message,
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

        // Always nullable: what a refresh appends must fit the declared field
        // whatever nullability the evaluator reports for a given batch.
        let mut field = ArrowField::new(output, data_type, true);
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
        let expr = planner
            .parse_filter(filter)
            .map_err(|e| Error::InvalidInput {
                message: format!("invalid view filter: {e}"),
            })?;
        ensure_immutable(&expr, |message| Error::InvalidInput {
            message: format!("invalid view filter: {message}"),
        })?;
        let filter_inputs = resolve_inputs(&source_schema, &expr, |message| Error::InvalidInput {
            message: format!("invalid view filter: {message}"),
        })?;
        // A committed filter has to be usable as a predicate.
        let read_schema = project_schema(&source_schema, &filter_inputs);
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
        inputs.extend(filter_inputs);
    }

    inputs.sort();
    inputs.dedup();

    let definition = MaterializedViewDefinition {
        source_table: source_table.to_string(),
        source_namespace: source_namespace.to_vec(),
        projections: projections
            .into_iter()
            .map(|(output, expression)| ViewProjection { output, expression })
            .collect(),
        filter,
        limit,
        inputs,
        function_columns: Vec::new(),
    };
    Ok((definition, fields, lineage))
}

/// Reject any function that is not immutable: a view definition has to
/// evaluate identically across refreshes, or incremental maintenance would
/// mix rows from different evaluations of the same definition.
fn ensure_immutable(expr: &datafusion_expr::Expr, error: impl Fn(String) -> Error) -> Result<()> {
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion_expr::Volatility;

    // Labeled immutable but not determined by row values alone: version()
    // depends on the build, the arrow_* introspectors on schema state.
    const NOT_VALUE_DETERMINED: &[&str] =
        &["version", "arrow_typeof", "arrow_field", "arrow_metadata"];

    let mut offending: Option<String> = None;
    expr.apply(|node| {
        if let datafusion_expr::Expr::ScalarFunction(function) = node {
            let name = function.func.name();
            if function.func.signature().volatility != Volatility::Immutable
                || NOT_VALUE_DETERMINED.contains(&name)
            {
                offending = Some(name.to_string());
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map_err(|e| error(e.to_string()))?;
    match offending {
        Some(name) => Err(error(format!(
            "function '{name}' is not immutable and would evaluate differently \
             across refreshes"
        ))),
        None => Ok(()),
    }
}

/// The root of a possibly-dotted column path: `metadata.age` -> `metadata`.
fn root(path: &str) -> &str {
    path.split('.').next().unwrap_or(path)
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
/// fields plus [`SOURCE_ROW_ID_COLUMN`], definition stamped in metadata.
/// Produced only by [`prepare_declaration`].
#[derive(Clone)]
pub struct PreparedDeclaration {
    schema: SchemaRef,
    /// The tail of `schema` that a registered Function fills: created by a
    /// merge after the table exists, since a create schema may not carry
    /// computed-column declarations.
    function_fields: Vec<ArrowField>,
    definition: MaterializedViewDefinition,
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

    /// The schema the view will have: the projected fields,
    /// [`SOURCE_ROW_ID_COLUMN`], then any function columns.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Add columns a registered Function fills after each refresh.
    ///
    /// Each field carries a table Function column's declaration (see
    /// [`crate::table::computed_columns::function_computed_column_metadata`])
    /// naming one of `bindings`, and
    /// every binding input reads a projected view column. Refresh writes
    /// these columns as NULL; a fill rewrites only them, and is the one
    /// commit on a view that refresh does not treat as drift.
    pub fn with_function_columns(
        mut self,
        fields: Vec<ArrowField>,
        bindings: &[FunctionBinding],
    ) -> Result<Self> {
        let invalid = |message: String| Error::InvalidInput { message };
        if fields.is_empty() {
            return Err(invalid(
                "a view needs at least one function column to declare".into(),
            ));
        }
        if !self.definition.function_columns.is_empty() {
            return Err(invalid(
                "function columns were already declared on this view".into(),
            ));
        }
        let projected: Vec<ArrowField> = self
            .schema
            .fields()
            .iter()
            .filter(|f| f.name() != SOURCE_ROW_ID_COLUMN)
            .map(|f| f.as_ref().clone())
            .collect();
        let binding_ids: HashSet<&str> = bindings.iter().map(FunctionBinding::binding_id).collect();
        let mut declared: HashSet<&str> = projected.iter().map(|f| f.name().as_str()).collect();
        let mut bound: HashSet<&str> = HashSet::new();
        for field in &fields {
            let name = field.name().as_str();
            if name == SOURCE_ROW_ID_COLUMN || name == ROW_ID {
                return Err(invalid(format!("view column name '{name}' is reserved")));
            }
            if !declared.insert(name) {
                return Err(Error::ColumnAlreadyExists {
                    name: name.to_string(),
                });
            }
            if !field.is_nullable() {
                return Err(invalid(format!(
                    "function column '{name}' must be nullable until a refresh fills it"
                )));
            }
            match computed_column_from_field(field).map(|column| column.kind) {
                Some(ComputedColumnKind::Function { binding_id, .. })
                    if binding_ids.contains(binding_id.as_str()) =>
                {
                    bound.insert(field.name().as_str());
                }
                _ => {
                    return Err(invalid(format!(
                        "function column '{name}' does not carry a declared Function binding"
                    )));
                }
            }
        }
        let bound_ids: HashSet<String> = fields
            .iter()
            .filter_map(computed_column_from_field)
            .filter_map(|column| match column.kind {
                ComputedColumnKind::Function { binding_id, .. } => Some(binding_id),
                _ => None,
            })
            .collect();
        for binding in bindings {
            if !bound_ids.contains(binding.binding_id()) {
                return Err(invalid(format!(
                    "Function binding '{}' declares no view column",
                    binding.binding_id()
                )));
            }
            for input in binding.inputs() {
                let root = input.field_path.split('.').next().unwrap_or_default();
                if !projected.iter().any(|f| f.name() == root) {
                    return Err(invalid(format!(
                        "Function binding '{}' reads '{}', which the view does not project",
                        binding.binding_id(),
                        input.field_path
                    )));
                }
            }
        }
        debug_assert_eq!(bound.len(), fields.len());

        let mut all = projected;
        all.push(ArrowField::new(
            SOURCE_ROW_ID_COLUMN,
            DataType::UInt64,
            false,
        ));
        let insert_at = all.len();
        all.extend(fields.iter().cloned());
        let mut metadata = self.schema.metadata().clone();
        metadata.insert(
            FUNCTION_BINDINGS_META_KEY.to_string(),
            function_bindings_metadata(bindings)?,
        );
        // Column definitions are positional over the view schema.
        if let Some(raw) = metadata.get(COLUMN_DEFINITIONS_META_KEY).cloned() {
            let mut definitions: Vec<ColumnDefinition> =
                serde_json::from_str(&raw).map_err(|e| Error::Runtime {
                    message: format!("unreadable column definitions on the view: {e}"),
                })?;
            for offset in 0..fields.len() {
                definitions.insert(
                    insert_at + offset,
                    ColumnDefinition {
                        kind: ColumnKind::Physical,
                    },
                );
            }
            metadata.insert(
                COLUMN_DEFINITIONS_META_KEY.to_string(),
                serde_json::to_string(&definitions).map_err(|e| Error::Runtime {
                    message: format!("failed to serialize column definitions: {e}"),
                })?,
            );
        }
        self.definition.function_columns = fields.iter().map(|f| f.name().clone()).collect();
        metadata.insert(
            DEFINITION_META_KEY.to_string(),
            definition_to_metadata(&self.definition)?,
        );
        self.schema = Arc::new(ArrowSchema::new_with_metadata(all, metadata));
        self.function_fields = fields;
        Ok(self)
    }

    /// Create the view table and verify it, consuming the declaration.
    ///
    /// The view goes at the root of the source's own database, where refresh
    /// resolves the recorded source coordinate. Stable row ids are requested
    /// at both levels and verified rather than trusted; nothing is rolled
    /// back on failure.
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
        // Minted here, not at preparation: a declaration can be cloned and
        // create more than one physical table, and each needs its own token.
        let incarnation = uuid::Uuid::new_v4().to_string();
        let mut metadata = self.schema.metadata().clone();
        metadata.insert(INCARNATION_META_KEY.to_string(), incarnation.clone());
        // Function columns and their bindings are merged in below.
        let bindings = metadata.remove(FUNCTION_BINDINGS_META_KEY);
        let created: Vec<FieldRef> = self
            .schema
            .fields()
            .iter()
            .filter(|f| !self.function_fields.iter().any(|g| g.name() == f.name()))
            .cloned()
            .collect();
        let schema = Arc::new(ArrowSchema::new_with_metadata(created, metadata));
        let reader: Box<dyn arrow_array::RecordBatchReader + Send> =
            Box::new(arrow_array::RecordBatchIterator::new(empty, schema));
        let mut request = CreateTableRequest::new(name.to_string(), Box::new(reader));
        request.namespace_path = namespace_path.to_vec();
        let write_params = request
            .write_options
            .lance_write_params
            .get_or_insert_with(Default::default);
        write_params.enable_stable_row_ids = true;
        let store_params = write_params
            .store_params
            .get_or_insert_with(Default::default);
        crate::connection::merge_storage_options(
            store_params,
            [(
                OPT_NEW_TABLE_ENABLE_STABLE_ROW_IDS.to_string(),
                "true".to_string(),
            )],
        );
        let table = self.database.clone().create_table(request).await?;
        let table = Table::new(table, self.database);
        let stable = match table.as_native() {
            Some(native) => native.dataset.get().await?.manifest.uses_stable_row_ids(),
            None => false,
        };
        if !stable {
            return Err(Error::Runtime {
                message: format!(
                    "view '{name}' was created without stable row ids: the database \
                     ignored the creation option; the table remains and is not \
                     usable as a materialized view"
                ),
            });
        }
        if let Some(bindings) = bindings {
            merge_function_columns(&table, &self.function_fields, bindings).await?;
        }
        Ok(MaterializedView {
            table,
            definition: self.definition,
            incarnation: Some(incarnation),
        })
    }
}

/// Validate a view declaration against its live source and hold what its
/// creation needs. The declaration is canonicalized through the coordinate a
/// refresh will resolve -- name and namespace both -- so a handle that does
/// not resolve back to itself is rejected. Same creation-time checks as
/// [`Connection::create_materialized_view`].
///
/// ```no_run
/// # #![recursion_limit = "256"]
/// # use lancedb::materialized_view::prepare_declaration;
/// # async fn declare(source: &lancedb::Table) -> Result<(), Box<dyn std::error::Error>> {
/// let prepared = prepare_declaration(
///     source,
///     &[("id".into(), "id".into()), ("double".into(), "value * 2".into())],
///     Some("value > 0"),
///     None,
/// )
/// .await?;
/// let view = prepared.create("doubles").await?;
/// # Ok(())
/// # }
/// ```
pub async fn prepare_declaration(
    source: &Table,
    projections: &[(String, String)],
    filter: Option<&str>,
    limit: Option<u64>,
) -> Result<PreparedDeclaration> {
    let Some(caller_native) = source.as_native() else {
        return Err(Error::NotSupported {
            message: "materialized views are supported only on local databases".into(),
        });
    };
    // Refresh resolves the source at exactly this coordinate, so the
    // definition records the namespace alongside the name.
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
    if !native.dataset.get().await?.manifest.uses_stable_row_ids() {
        return Err(Error::InvalidInput {
            message: format!(
                "materialized views require stable row ids on the source table; \
                 create '{}' with storage option new_table_enable_stable_row_ids=true",
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
    let source_schema = resolved.schema().await?;
    let source_metadata = source_schema.metadata().clone();
    let (definition, mut fields, lineage) = plan(
        source_schema.clone(),
        resolved.name(),
        &source_namespace,
        projections,
        filter,
        limit,
    )?;
    fields.push(ArrowField::new(
        SOURCE_ROW_ID_COLUMN,
        DataType::UInt64,
        false,
    ));
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
        function_fields: Vec::new(),
        definition,
        database,
    })
}

/// Declare `fields` on the freshly created view the way the server declares
/// Function columns on a table: one merge commit carrying the fields and the
/// schema-level binding envelope.
async fn merge_function_columns(
    table: &Table,
    fields: &[ArrowField],
    bindings: String,
) -> Result<()> {
    let Some(native) = table.as_native() else {
        return Err(Error::NotSupported {
            message: "materialized views are supported only on local databases".into(),
        });
    };
    let dataset = native.dataset.get().await?.as_ref().clone();
    let declaration = ArrowSchema::new(fields.to_vec());
    let mut merged = dataset.schema().merge(&declaration)?;
    merged
        .metadata
        .insert(FUNCTION_BINDINGS_META_KEY.to_string(), bindings);
    merged.set_field_id(Some(dataset.manifest.max_field_id()));
    let transaction = Transaction::new(
        dataset.manifest.version,
        Operation::Merge {
            fragments: Vec::new(),
            schema: merged,
            preserves_nullability: true,
        },
        None,
    );
    CommitBuilder::new(WriteDestination::Dataset(Arc::new(dataset)))
        .execute(transaction)
        .await?;
    native.dataset.reload().await?;
    Ok(())
}

/// One row of [`Connection::list_materialized_views`]: a view's name and its
/// definition kind, which may be one this version cannot refresh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedViewEntry {
    /// Name of the view's table.
    pub name: String,
    /// The view's definition as stored.
    pub kind: MaterializedViewKind,
}

/// Materialized views are local-only; refuse a remote connection before any
/// request is made.
fn ensure_local(connection: &Connection) -> Result<()> {
    if connection.uri().starts_with("db://") {
        return Err(Error::NotSupported {
            message: "materialized views are supported only on local databases".into(),
        });
    }
    Ok(())
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

    /// Create the view: an empty table carrying the definition; refresh
    /// computes the rows. The source must keep stable row ids -- they hold
    /// provenance across compaction, and cannot be enabled later.
    pub async fn execute(self) -> Result<MaterializedView> {
        ensure_local(&self.connection)?;
        let source = self
            .connection
            .open_table(&self.source)
            .namespace(self.source_namespace.clone())
            .execute()
            .await?;
        let prepared = prepare_declaration(
            &source,
            &self.projections,
            self.filter.as_deref(),
            self.limit,
        )
        .await?;
        prepared.create_in(&self.namespace, &self.name).await
    }
}

/// A handle on a materialized view: the view table plus its parsed definition.
#[derive(Debug, Clone)]
pub struct MaterializedView {
    table: Table,
    definition: MaterializedViewDefinition,
    incarnation: Option<String>,
}

impl MaterializedView {
    /// Interpret `table` as a materialized view: [`Error::NotAMaterializedView`]
    /// for a plain table, [`Error::NotSupported`] for a kind this version
    /// cannot refresh.
    pub async fn from_table(table: Table) -> Result<Self> {
        // Same local-only boundary the connection-level entry points hold,
        // applied before the schema read so a remote table costs no request.
        if table.as_native().is_none() {
            return Err(Error::NotSupported {
                message: "materialized views are supported only on local databases".into(),
            });
        }
        let schema = table.schema().await?;
        let incarnation = schema.metadata().get(INCARNATION_META_KEY).cloned();
        match materialized_view_kind(schema.metadata())? {
            Some(MaterializedViewKind::Select(definition)) => Ok(Self {
                table,
                definition,
                incarnation,
            }),
            Some(MaterializedViewKind::Unrecognized { kind }) => Err(Error::NotSupported {
                message: format!(
                    "materialized view '{}' is defined by '{kind}', which this version of \
                     lancedb cannot refresh",
                    table.name()
                ),
            }),
            None => Err(Error::NotAMaterializedView {
                name: table.name().to_string(),
            }),
        }
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

    /// The view's incarnation token as of when this handle was opened; see
    /// [`RefreshMaterializedViewBuilder::expect_incarnation`]. `None` for a
    /// view that has none yet (see [`INCARNATION_META_KEY`]).
    pub fn incarnation(&self) -> Option<&str> {
        self.incarnation.as_deref()
    }

    /// Recompute the view from its source.
    ///
    /// By default the refresh is incremental when the source's changes can be
    /// reconciled into the view, and otherwise rebuilds; see
    /// [`RefreshMaterializedViewBuilder`].
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
            full: false,
            source_version: None,
            expected_incarnation: None,
        }
    }
}

/// Builds a refresh. Created by [`MaterializedView::refresh`].
pub struct RefreshMaterializedViewBuilder {
    view: MaterializedView,
    full: bool,
    source_version: Option<u64>,
    expected_incarnation: Option<String>,
}

impl RefreshMaterializedViewBuilder {
    /// Rebuild the view even where an incremental refresh would do.
    pub fn full(mut self, full: bool) -> Self {
        self.full = full;
        self
    }

    /// Refresh to this source table version instead of the latest.
    pub fn source_version(mut self, version: u64) -> Self {
        self.source_version = Some(version);
        self
    }

    /// Refresh only if the view is still the incarnation that minted `token`
    /// (see [`MaterializedView::incarnation`]): a refresh requested against
    /// one declaration must not land in a view dropped and recreated since,
    /// even under the same name and definition.
    ///
    /// Best effort. The token is read from the latest stored manifest before
    /// planning and again immediately before every commit, but it is not part
    /// of the commit's own condition, so a recreation that lands between that
    /// final read and the commit is not caught.
    pub fn expect_incarnation(mut self, token: impl Into<String>) -> Self {
        self.expected_incarnation = Some(token.into());
        self
    }

    pub async fn execute(self) -> Result<RefreshMaterializedViewResult> {
        refresh::execute_refresh(
            &self.view.table,
            self.full,
            self.source_version,
            self.expected_incarnation.as_deref(),
        )
        .await
    }
}

impl Connection {
    /// Define a materialized view named `name` over `source`.
    ///
    /// The view is created empty, with the definition recorded in its schema
    /// metadata; refresh computes the rows. Local databases only.
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
    /// view.refresh().execute().await?;
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
        ensure_local(self)?;
        let table = self.open_table(name).execute().await?;
        MaterializedView::from_table(table).await
    }

    /// The materialized views in this database, unrefreshable kinds included.
    /// Costs a table open per table; one that cannot be opened is skipped
    /// rather than failing the listing.
    pub async fn list_materialized_views(&self) -> Result<Vec<MaterializedViewEntry>> {
        ensure_local(self)?;
        let names = self.table_names().execute().await?;
        let mut views = Vec::new();
        for name in names {
            let Ok(table) = self.open_table(&name).execute().await else {
                continue;
            };
            let schema = table.schema().await?;
            if let Some(kind) = materialized_view_kind(schema.metadata())? {
                views.push(MaterializedViewEntry { name, kind });
            }
        }
        Ok(views)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::record_batch;

    use super::*;
    use crate::connect;
    use crate::table::WriteOptions;

    async fn people_db() -> Connection {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(
            ("name", Utf8, ["ada", "grace", "alan"]),
            ("age", Int32, [36, 85, 41])
        )
        .unwrap();
        conn.create_table("people", batch)
            .write_options(stable_row_ids())
            .execute()
            .await
            .unwrap();
        conn
    }

    /// Sources must keep stable row ids; see the create-time gate.
    pub(super) fn stable_row_ids() -> WriteOptions {
        WriteOptions {
            lance_write_params: Some(lance::dataset::WriteParams {
                enable_stable_row_ids: true,
                ..Default::default()
            }),
        }
    }

    /// The error a doomed declaration against `people` produces.
    async fn declare_err(
        cfg: impl FnOnce(CreateMaterializedViewBuilder) -> CreateMaterializedViewBuilder,
    ) -> Error {
        let conn = people_db().await;
        cfg(conn.create_materialized_view("bad", "people"))
            .execute()
            .await
            .unwrap_err()
    }

    #[tokio::test]
    async fn test_create_records_the_definition() {
        let conn = people_db().await;
        let view = conn
            .create_materialized_view("adults", "people")
            .select([("name", "name"), ("shout", "upper(name)")])
            .only_if("age >= 18")
            .limit(10)
            .execute()
            .await
            .unwrap();

        assert_eq!(view.name(), "adults");
        assert_eq!(
            view.definition(),
            &MaterializedViewDefinition {
                source_table: "people".into(),
                source_namespace: Vec::new(),
                projections: vec![
                    ViewProjection {
                        output: "name".into(),
                        expression: "name".into()
                    },
                    ViewProjection {
                        output: "shout".into(),
                        expression: "upper(name)".into()
                    },
                ],
                filter: Some("age >= 18".into()),
                limit: Some(10),
                inputs: vec!["age".into(), "name".into()],
                function_columns: Vec::new(),
            }
        );

        // The definition round-trips off the stored schema, not the handle.
        let reopened = conn.open_materialized_view("adults").await.unwrap();
        assert_eq!(reopened.definition(), view.definition());
    }

    #[tokio::test]
    async fn test_view_schema_is_derived_from_the_query() {
        let conn = people_db().await;
        let view = conn
            .create_materialized_view("shapes", "people")
            .select([("shout", "upper(name)"), ("next_age", "age + 1")])
            .execute()
            .await
            .unwrap();

        let schema = view.table().schema().await.unwrap();
        assert_eq!(
            schema.field_with_name("shout").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("next_age").unwrap().data_type(),
            &DataType::Int32
        );
        assert_eq!(
            schema
                .field_with_name(SOURCE_ROW_ID_COLUMN)
                .unwrap()
                .data_type(),
            &DataType::UInt64
        );
        assert_eq!(view.table().count_rows(None).await.unwrap(), 0);
    }

    /// No projection selects every source column, expanded now: the schema
    /// captured at creation is the definition.
    #[tokio::test]
    async fn test_default_projection_captures_the_source_schema() {
        let conn = people_db().await;
        let view = conn
            .create_materialized_view("copy", "people")
            .execute()
            .await
            .unwrap();
        assert_eq!(
            view.definition()
                .projections
                .iter()
                .map(|p| p.output.as_str())
                .collect::<Vec<_>>(),
            vec!["name", "age"]
        );
        assert_eq!(view.definition().inputs, vec!["age", "name"]);
    }

    #[tokio::test]
    async fn test_unknown_column_fails_at_create_time() {
        let conn = people_db().await;
        let err = conn
            .create_materialized_view("bad", "people")
            .select([("x", "missing + 1")])
            .execute()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidExpression { column, .. } if column == "x"));
        let names = conn.table_names().execute().await.unwrap();
        assert!(!names.contains(&"bad".to_string()));
    }

    #[tokio::test]
    async fn test_unknown_filter_column_fails_at_create_time() {
        let err = declare_err(|b| b.only_if("missing > 1")).await;
        assert!(matches!(err, Error::InvalidInput { message } if message.contains("missing")));
    }

    #[tokio::test]
    async fn test_duplicate_output_is_rejected() {
        let err = declare_err(|b| b.select([("dup", "age"), ("dup", "age + 1")])).await;
        assert!(matches!(err, Error::ColumnAlreadyExists { name } if name == "dup"));
    }

    #[tokio::test]
    async fn test_reserved_output_name_is_rejected() {
        let err = declare_err(|b| b.select([(SOURCE_ROW_ID_COLUMN, "age")])).await;
        assert!(matches!(err, Error::InvalidInput { message } if message.contains("reserved")));
    }

    #[tokio::test]
    async fn test_missing_source_fails() {
        let conn = connect("memory://").execute().await.unwrap();
        let err = conn
            .create_materialized_view("v", "nope")
            .execute()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::TableNotFound { .. }));
    }

    /// Provenance has to survive source compactions and updates, and stable
    /// row ids cannot be enabled after a table exists -- so the requirement
    /// is checked at the last moment the caller can still act on it.
    #[tokio::test]
    async fn test_source_without_stable_row_ids_is_refused() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("x", Int32, [1, 2])).unwrap();
        conn.create_table("plain", batch).execute().await.unwrap();

        let err = conn
            .create_materialized_view("v", "plain")
            .execute()
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::InvalidInput { message } if message.contains("stable row ids"))
        );
        assert!(
            !conn
                .table_names()
                .execute()
                .await
                .unwrap()
                .contains(&"v".to_string())
        );
    }

    #[tokio::test]
    async fn test_name_collision_fails() {
        let conn = people_db().await;
        let err = conn
            .create_materialized_view("people", "people")
            .execute()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::TableAlreadyExists { .. }));
    }

    #[tokio::test]
    async fn test_a_plain_table_is_not_a_view() {
        let conn = people_db().await;
        let table = conn.open_table("people").execute().await.unwrap();
        let err = MaterializedView::from_table(table).await.unwrap_err();
        assert!(matches!(err, Error::NotAMaterializedView { name } if name == "people"));

        let err = conn.open_materialized_view("people").await.unwrap_err();
        assert!(matches!(err, Error::NotAMaterializedView { .. }));
    }

    /// The reason the kind is tagged: a definition written by a newer version
    /// reads back as a view this one cannot refresh, not as a plain table.
    #[tokio::test]
    async fn test_unrecognized_kind_is_refused_by_name() {
        let conn = people_db().await;
        conn.create_materialized_view("v", "people")
            .execute()
            .await
            .unwrap();
        let table = conn.open_table("v").execute().await.unwrap();
        table
            .as_native()
            .unwrap()
            .replace_schema_metadata(HashMap::from([(
                DEFINITION_META_KEY.to_string(),
                r#"{"kind": "join"}"#.to_string(),
            )]))
            .await
            .unwrap();

        let err = conn.open_materialized_view("v").await.unwrap_err();
        assert!(matches!(err, Error::NotSupported { message } if message.contains("join")));
    }

    #[tokio::test]
    async fn test_list_reports_views_and_only_views() {
        let conn = people_db().await;
        conn.create_materialized_view("adults", "people")
            .only_if("age >= 18")
            .execute()
            .await
            .unwrap();

        let views = conn.list_materialized_views().await.unwrap();
        assert_eq!(
            views.iter().map(|v| v.name.as_str()).collect::<Vec<_>>(),
            vec!["adults"]
        );
        let MaterializedViewKind::Select(definition) = &views[0].kind else {
            panic!("expected a select view");
        };
        assert_eq!(definition.filter.as_deref(), Some("age >= 18"));
    }

    /// The creation option outranks a connection configured to create
    /// unstable tables: the view still gets stable row ids, on the same
    /// store (no fork -- the table must be reachable through the
    /// connection afterwards).
    #[tokio::test]
    async fn test_view_is_stable_despite_connection_override() {
        let conn = connect("memory://")
            .storage_options([("new_table_enable_stable_row_ids", "false")])
            .execute()
            .await
            .unwrap();
        let batch = record_batch!(("x", Int32, [1])).unwrap();
        conn.create_table("src", batch)
            .storage_option("new_table_enable_stable_row_ids", "true")
            .execute()
            .await
            .unwrap();

        let view = conn
            .create_materialized_view("v", "src")
            .execute()
            .await
            .unwrap();
        let stable = view
            .table()
            .as_native()
            .unwrap()
            .dataset
            .get()
            .await
            .unwrap()
            .manifest
            .uses_stable_row_ids();
        assert!(stable);
        conn.open_materialized_view("v").await.unwrap();
    }

    /// A committed filter has to be usable as a predicate.
    #[tokio::test]
    async fn test_non_boolean_filter_is_rejected() {
        let err = declare_err(|b| b.only_if("age + 1")).await;
        assert!(matches!(err, Error::InvalidInput { message } if message.contains("boolean")));
    }

    /// Nested references stay dotted paths; resolution is by root field.
    #[tokio::test]
    async fn test_struct_columns_can_be_declared() {
        use arrow_array::{ArrayRef, Int32Array, StructArray};

        let conn = connect("memory://").execute().await.unwrap();
        let ages = StructArray::from(vec![(
            Arc::new(ArrowField::new("age", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![36, 17])) as ArrayRef,
        )]);
        let batch =
            arrow_array::RecordBatch::try_from_iter(vec![("metadata", Arc::new(ages) as ArrayRef)])
                .unwrap();
        conn.create_table("people", batch)
            .write_options(stable_row_ids())
            .execute()
            .await
            .unwrap();

        let view = conn
            .create_materialized_view("ages", "people")
            .select([("age", "metadata.age")])
            .only_if("metadata.age >= 18")
            .execute()
            .await
            .unwrap();
        assert_eq!(view.definition().inputs, vec!["metadata.age"]);
        let schema = view.table().schema().await.unwrap();
        assert_eq!(
            schema.field_with_name("age").unwrap().data_type(),
            &DataType::Int32
        );
    }

    /// A newer-kind view must not disappear from the listing.
    #[tokio::test]
    async fn test_unrecognized_kind_is_listed_with_its_kind() {
        let conn = people_db().await;
        conn.create_materialized_view("v", "people")
            .execute()
            .await
            .unwrap();
        let table = conn.open_table("v").execute().await.unwrap();
        table
            .as_native()
            .unwrap()
            .replace_schema_metadata(HashMap::from([(
                DEFINITION_META_KEY.to_string(),
                r#"{"kind": "join"}"#.to_string(),
            )]))
            .await
            .unwrap();

        let views = conn.list_materialized_views().await.unwrap();
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].name, "v");
        assert_eq!(
            views[0].kind,
            MaterializedViewKind::Unrecognized {
                kind: "join".into()
            }
        );
    }

    /// Remote connections are refused before any request is made.
    #[cfg(feature = "remote")]
    #[tokio::test]
    async fn test_remote_connection_is_refused_up_front() {
        let conn = connect("db://nowhere")
            .api_key("sk_test")
            .region("us-east-1")
            .execute()
            .await
            .unwrap();
        let err = conn
            .create_materialized_view("v", "src")
            .execute()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotSupported { .. }));
        let err = conn.open_materialized_view("v").await.unwrap_err();
        assert!(matches!(err, Error::NotSupported { .. }));
        let err = conn.list_materialized_views().await.unwrap_err();
        assert!(matches!(err, Error::NotSupported { .. }));
    }

    /// A definition must evaluate identically across refreshes; anything
    /// less makes incremental maintenance a mix of evaluations.
    #[tokio::test]
    async fn test_volatile_and_unstable_expressions_are_rejected() {
        let conn = people_db().await;
        for expression in [
            "random()",
            "now()",
            "version()",
            "arrow_typeof(age)",
            "arrow_metadata(age, 'k')",
        ] {
            let err = conn
                .create_materialized_view("bad", "people")
                .select([("x", expression)])
                .execute()
                .await
                .unwrap_err();
            assert!(
                matches!(err, Error::InvalidExpression { message, .. }
                    if message.contains("not immutable")),
                "{expression} was not rejected"
            );
        }
        for filter in ["age > random() * 100", "age >= 0 and now() is not null"] {
            let err = conn
                .create_materialized_view("bad", "people")
                .only_if(filter)
                .execute()
                .await
                .unwrap_err();
            assert!(
                matches!(err, Error::InvalidInput { message } if message.contains("not immutable")),
                "{filter} was not rejected"
            );
        }
    }

    /// A column projected as itself stays the column it was: blob discovery
    /// and the blob APIs key off field metadata, which a bare rebuild of the
    /// field would drop.
    #[tokio::test]
    async fn test_identity_projection_keeps_field_metadata() {
        let conn = connect("memory://").execute().await.unwrap();
        let schema = Arc::new(ArrowSchema::new_with_metadata(
            vec![
                ArrowField::new("id", DataType::Int32, true),
                crate::blob("payload", true),
            ],
            HashMap::new(),
        ));
        conn.create_empty_table("src", schema)
            .write_options(stable_row_ids())
            .execute()
            .await
            .unwrap();

        let view = conn
            .create_materialized_view("v", "src")
            .execute()
            .await
            .unwrap();
        let view_schema = view.table().schema().await.unwrap();

        let payload = view_schema.field_with_name("payload").unwrap();
        assert!(
            crate::blob::is_blob(payload),
            "default projection dropped the blob marker: {:?}",
            payload.metadata()
        );
        assert_eq!(
            view.table().blob_columns().await.unwrap(),
            vec!["payload".to_string()],
            "blob discovery no longer finds the projected column"
        );
        assert!(view_schema.metadata().contains_key(DEFINITION_META_KEY));
        // Structural declarations describe how a table is written; a view is
        // written by refresh, and its fields are always nullable.
        assert!(!view_schema.metadata().contains_key("lance:primary_key"));

        // A computed column is a new value and carries no source metadata.
        let computed = conn
            .create_materialized_view("c", "src")
            .select([("payload", "payload"), ("n", "id + 1")])
            .execute()
            .await
            .unwrap();
        let computed_schema = computed.table().schema().await.unwrap();
        assert!(crate::blob::is_blob(
            computed_schema.field_with_name("payload").unwrap()
        ));
        assert!(
            computed_schema
                .field_with_name("n")
                .unwrap()
                .metadata()
                .is_empty()
        );
    }

    /// A nested column projected straight through is still that column, and a
    /// declaration buried in a struct child binds as hard as one on top.
    #[tokio::test]
    async fn test_nested_projection_metadata_and_declarations() {
        let conn = connect("memory://").execute().await.unwrap();
        let payload = crate::blob("payload", true).with_metadata(HashMap::from([
            ("lance-encoding:blob".to_string(), "true".to_string()),
            (
                "lance-schema:unenforced-primary-key".to_string(),
                "0".to_string(),
            ),
        ]));
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int32, true),
            ArrowField::new("meta", DataType::Struct(vec![payload].into()), true),
        ]));
        conn.create_empty_table("src", schema)
            .write_options(stable_row_ids())
            .execute()
            .await
            .unwrap();

        // A nested path is a direct projection: the leaf's metadata comes with
        // it, so the blob stays a blob rather than a plain struct.
        let lifted = conn
            .create_materialized_view("lifted", "src")
            .select([("payload", "meta.payload")])
            .execute()
            .await
            .unwrap();
        let field = lifted.table().schema().await.unwrap();
        let field = field.field_with_name("payload").unwrap().clone();
        assert_eq!(
            field.metadata().get("lance-encoding:blob"),
            Some(&"true".to_string()),
            "nested projection lost the leaf's metadata"
        );
        assert!(
            !field
                .metadata()
                .contains_key("lance-schema:unenforced-primary-key"),
            "a structural declaration rode along"
        );

        // Projecting the struct whole must not carry the child's declaration
        // out to a view whose fields are nullable.
        let whole = conn
            .create_materialized_view("whole", "src")
            .select([("meta", "meta")])
            .execute()
            .await
            .unwrap();
        let schema = whole.table().schema().await.unwrap();
        let DataType::Struct(children) = schema.field_with_name("meta").unwrap().data_type() else {
            panic!("meta is not a struct");
        };
        let child = children.iter().find(|c| c.name() == "payload").unwrap();
        assert!(
            !child
                .metadata()
                .contains_key("lance-schema:unenforced-primary-key"),
            "a nested declaration survived: {:?}",
            child.metadata()
        );
        assert_eq!(
            child.metadata().get("lance-encoding:blob"),
            Some(&"true".to_string())
        );
    }

    /// A map's entries are fields like any other, and a declaration on one
    /// binds the view's writes just as hard as one on top.
    #[tokio::test]
    async fn test_map_declarations_are_stripped() {
        let conn = connect("memory://").execute().await.unwrap();
        let value =
            ArrowField::new("value", DataType::Utf8, false).with_metadata(HashMap::from([(
                "lance-schema:unenforced-clustering-key:position".to_string(),
                "1".to_string(),
            )]));
        let entries = ArrowField::new(
            "entries",
            DataType::Struct(vec![ArrowField::new("key", DataType::Utf8, false), value].into()),
            false,
        );
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "props",
            DataType::Map(Arc::new(entries), false),
            true,
        )]));
        conn.create_empty_table("src", schema)
            .write_options(stable_row_ids())
            .execute()
            .await
            .unwrap();

        let view = conn
            .create_materialized_view("view", "src")
            .execute()
            .await
            .unwrap();
        let schema = view.table().schema().await.unwrap();
        let DataType::Map(entries, _) = schema.field_with_name("props").unwrap().data_type() else {
            panic!("props is not a map");
        };
        let DataType::Struct(children) = entries.data_type() else {
            panic!("map entries are not a struct");
        };
        let value = children.iter().find(|c| c.name() == "value").unwrap();
        assert!(
            !value
                .metadata()
                .contains_key("lance-schema:unenforced-clustering-key:position"),
            "a declaration survived inside a map: {:?}",
            value.metadata()
        );
    }

    /// A computed column is declared by field metadata. Projecting one --
    /// as itself or under an alias -- must carry its description without its
    /// declaration, which the target table would reject as foreign.
    #[tokio::test]
    async fn test_view_over_a_computed_column() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("id", Int32, [1, 2])).unwrap();
        let source = conn
            .create_table("src", batch)
            .write_options(stable_row_ids())
            .execute()
            .await
            .unwrap();
        source
            .add_columns()
            .computed("doubled", "id * 2")
            .execute()
            .await
            .unwrap();

        // Default projection reaches the computed column too.
        let whole = conn
            .create_materialized_view("whole", "src")
            .execute()
            .await
            .unwrap();
        let schema = whole.table().schema().await.unwrap();
        let field = schema.field_with_name("doubled").unwrap();
        assert!(
            !field
                .metadata()
                .keys()
                .any(|k| k.starts_with("computed_column")),
            "a computed-column declaration rode along: {:?}",
            field.metadata()
        );

        // And under an alias.
        conn.create_materialized_view("aliased", "src")
            .select([("twice", "doubled")])
            .execute()
            .await
            .unwrap();
    }

    /// Embedding configuration names columns. It comes along only for the
    /// columns a view actually projects, under the names the view gives them.
    #[tokio::test]
    async fn test_embedding_config_follows_the_projection() {
        let config = r#"[{"name":"f","model":{},"source_column":"text","vector_column":"vec"}]"#;
        let conn = connect("memory://").execute().await.unwrap();
        let schema = Arc::new(ArrowSchema::new_with_metadata(
            vec![
                ArrowField::new("text", DataType::Utf8, true),
                ArrowField::new("vec", DataType::Float32, true),
            ],
            HashMap::from([("embedding_functions".to_string(), config.to_string())]),
        ));
        conn.create_empty_table("src", schema)
            .write_options(stable_row_ids())
            .execute()
            .await
            .unwrap();

        let carried = |view: &MaterializedView| {
            let view = view.table().clone();
            async move {
                view.schema()
                    .await
                    .unwrap()
                    .metadata()
                    .get("embedding_functions")
                    .cloned()
            }
        };

        // Both columns projected as themselves: kept as it stands.
        let whole = conn
            .create_materialized_view("whole", "src")
            .execute()
            .await
            .unwrap();
        let kept = carried(&whole).await.expect("config dropped");
        assert!(kept.contains(r#""source_column":"text""#), "{kept}");
        assert!(kept.contains(r#""vector_column":"vec""#), "{kept}");

        // Only the source column: the configuration names a vector column the
        // view does not have, so it describes nothing and goes.
        let partial = conn
            .create_materialized_view("partial", "src")
            .select([("text", "text")])
            .execute()
            .await
            .unwrap();
        assert_eq!(carried(&partial).await, None);

        // Renamed: the configuration follows the names the view uses.
        let renamed = conn
            .create_materialized_view("renamed", "src")
            .select([("body", "text"), ("embedding", "vec")])
            .execute()
            .await
            .unwrap();
        let remapped = carried(&renamed).await.expect("config dropped");
        assert!(remapped.contains(r#""source_column":"body""#), "{remapped}");
        assert!(
            remapped.contains(r#""vector_column":"embedding""#),
            "{remapped}"
        );

        // The Node bindings spell the same configuration in camelCase, and
        // the Rust definition names the destination `dest_column`.
        for (config, source_key, dest_key) in [
            (
                r#"[{"name":"f","model":{},"sourceColumn":"text","vectorColumn":"vec"}]"#,
                "sourceColumn",
                "vectorColumn",
            ),
            (
                r#"[{"name":"f","model":{},"source_column":"text","dest_column":"vec"}]"#,
                "source_column",
                "dest_column",
            ),
        ] {
            let schema = Arc::new(ArrowSchema::new_with_metadata(
                vec![
                    ArrowField::new("text", DataType::Utf8, true),
                    ArrowField::new("vec", DataType::Float32, true),
                ],
                HashMap::from([("embedding_functions".to_string(), config.to_string())]),
            ));
            let name = format!("src_{source_key}");
            conn.create_empty_table(&name, schema)
                .write_options(stable_row_ids())
                .execute()
                .await
                .unwrap();
            let view = conn
                .create_materialized_view(format!("v_{source_key}"), &name)
                .select([("body", "text"), ("embedding", "vec")])
                .execute()
                .await
                .unwrap();
            let carried = carried(&view).await.expect("config dropped");
            assert!(
                carried.contains(&format!(r#""{source_key}":"body""#)),
                "{carried}"
            );
            assert!(
                carried.contains(&format!(r#""{dest_key}":"embedding""#)),
                "{carried}"
            );
        }

        // A computed column is not the source column under another name.
        let computed = conn
            .create_materialized_view("computed", "src")
            .select([("body", "upper(text)"), ("embedding", "vec")])
            .execute()
            .await
            .unwrap();
        assert_eq!(carried(&computed).await, None);

        // One column projected twice is two columns in the view, and the
        // configuration has to describe both rather than whichever came last.
        let twice = conn
            .create_materialized_view("twice", "src")
            .select([("body", "text"), ("a", "vec"), ("b", "vec")])
            .execute()
            .await
            .unwrap();
        let carried = carried(&twice).await.expect("config dropped");
        let entries: Vec<serde_json::Value> = serde_json::from_str(&carried).unwrap();
        let mut vectors: Vec<&str> = entries
            .iter()
            .filter_map(|e| e["vector_column"].as_str())
            .collect();
        vectors.sort_unstable();
        assert_eq!(vectors, ["a", "b"], "{carried}");
    }

    /// The native Rust producer records embeddings as column definitions
    /// rather than as `embedding_functions`, and a query embeds its own text
    /// through them. They are positional, so the view's list covers every one
    /// of its fields.
    #[tokio::test]
    async fn test_native_column_definitions_follow_the_projection() {
        let conn = connect("memory://").execute().await.unwrap();
        let rich = crate::table::TableDefinition::new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("text", DataType::Utf8, true),
                ArrowField::new("vector", DataType::Float32, true),
            ])),
            vec![
                ColumnDefinition {
                    kind: ColumnKind::Physical,
                },
                ColumnDefinition {
                    kind: ColumnKind::Embedding(EmbeddingDefinition::new(
                        "text",
                        "model",
                        Some("vector"),
                    )),
                },
            ],
        )
        .into_rich_schema();
        conn.create_empty_table("src", rich)
            .write_options(stable_row_ids())
            .execute()
            .await
            .unwrap();

        let view = conn
            .create_materialized_view("view", "src")
            .select([("body", "text"), ("embedding", "vector")])
            .execute()
            .await
            .unwrap();
        let schema = view.table().schema().await.unwrap();
        let raw = schema
            .metadata()
            .get(COLUMN_DEFINITIONS_META_KEY)
            .expect("the view dropped the native column definitions");
        let definitions: Vec<ColumnDefinition> = serde_json::from_str(raw).unwrap();
        assert_eq!(
            definitions.len(),
            schema.fields().len(),
            "column definitions are positional"
        );
        let ColumnKind::Embedding(embedding) = &definitions[1].kind else {
            panic!("the embedding column came back physical: {raw}");
        };
        assert_eq!(embedding.source_column, "body");
        assert_eq!(embedding.dest_column.as_deref(), Some("embedding"));
        assert_eq!(embedding.embedding_name, "model");
        assert!(matches!(definitions[0].kind, ColumnKind::Physical));
        assert!(matches!(definitions[2].kind, ColumnKind::Physical));

        // Without the column the function reads, the view cannot recompute
        // the embedding, so it carries no definition for it.
        let partial = conn
            .create_materialized_view("partial", "src")
            .select([("embedding", "vector")])
            .execute()
            .await
            .unwrap();
        assert_eq!(
            partial
                .table()
                .schema()
                .await
                .unwrap()
                .metadata()
                .get(COLUMN_DEFINITIONS_META_KEY),
            None
        );
    }

    /// A scan takes the cap as i64, so a larger one is refused where it is
    /// declared rather than at the refresh that cannot run it. What a cap of
    /// zero means is a refresh question, tested there.
    #[tokio::test]
    async fn test_limit_above_i64_max_is_refused_at_creation() {
        let conn = people_db().await;
        let err = conn
            .create_materialized_view("too_big", "people")
            .limit(i64::MAX as u64 + 1)
            .execute()
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::InvalidInput { message } if message.contains("exceeds the maximum")),
            "got {err:?}"
        );

        // The boundary itself is accepted.
        conn.create_materialized_view("at_max", "people")
            .limit(i64::MAX as u64)
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_drop_is_drop_table() {
        let conn = people_db().await;
        conn.create_materialized_view("v", "people")
            .execute()
            .await
            .unwrap();
        conn.drop_table("v", &[]).await.unwrap();
        assert!(conn.list_materialized_views().await.unwrap().is_empty());
    }

    /// The public declaration contract: prepare validates the source and
    /// create consumes the declaration into a verified view table; a
    /// source that cannot anchor refresh and a target outside the
    /// source's database are both refused.
    #[tokio::test]
    async fn prepare_and_create_bind_the_declaration_lifecycle() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("id", Int32, [1, 2]), ("value", Int32, [3, 4])).unwrap();
        let source = conn
            .create_table("src", batch.clone())
            .write_options(stable_row_ids())
            .execute()
            .await
            .unwrap();

        let projections = [
            ("id".to_string(), "id".to_string()),
            ("double".to_string(), "value * 2".to_string()),
        ];
        let prepared = prepare_declaration(&source, &projections, Some("value > 0"), None)
            .await
            .unwrap();
        assert_eq!(prepared.definition().source_table, "src");

        let view = prepared.create("v").await.unwrap();
        let schema = view.table().schema().await.unwrap();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["id", "double", SOURCE_ROW_ID_COLUMN]);
        assert!(schema.metadata().contains_key(DEFINITION_META_KEY));

        // The same call rejects a source without stable row ids, so an
        // external creation path cannot skip the check.
        conn.create_table("plain", batch).execute().await.unwrap();
        let plain = conn.open_table("plain").execute().await.unwrap();
        let err = prepare_declaration(&plain, &[], None, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("stable row ids"), "{err}");

        // A handle whose location does not resolve back through its name is
        // refused: the definition would record a name reaching other data.
        let plain_uri = plain
            .as_native()
            .unwrap()
            .dataset
            .get()
            .await
            .unwrap()
            .uri()
            .to_string();
        let masquerade = conn
            .open_table("src")
            .location(plain_uri)
            .execute()
            .await
            .unwrap();
        let err = prepare_declaration(&masquerade, &[], None, None)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("does not resolve to itself"),
            "{err}"
        );

        // A table created at a custom location is refused outright: its
        // recorded name reaches nothing at the database root, so the
        // canonical reopen fails before any URI comparison.
        let custom = conn
            .create_table(
                "custom_loc",
                record_batch!(("id", Int32, [1, 2]), ("value", Int32, [3, 4])).unwrap(),
            )
            .location("memory://elsewhere/custom_loc")
            .write_options(stable_row_ids())
            .execute()
            .await
            .unwrap();
        let err = prepare_declaration(&custom, &[], None, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("custom_loc"), "{err}");
    }

    /// A view declared over a namespaced source records that namespace, and
    /// refresh resolves the source through it -- the coordinate round-trips.
    #[tokio::test]
    async fn a_namespaced_source_round_trips_through_refresh() {
        use lance_namespace::models::CreateNamespaceRequest;

        let tmp = tempfile::tempdir().unwrap();
        let mut properties = std::collections::HashMap::new();
        properties.insert("root".to_string(), tmp.path().to_str().unwrap().to_string());
        let conn = crate::connect_namespace("dir", properties)
            .execute()
            .await
            .unwrap();
        conn.create_namespace(CreateNamespaceRequest {
            id: Some(vec!["ns".into()]),
            ..Default::default()
        })
        .await
        .unwrap();

        let batch = record_batch!(
            ("name", Utf8, ["ada", "grace", "alan"]),
            ("age", Int32, [36, 85, 41])
        )
        .unwrap();
        conn.create_table("people", batch)
            .namespace(vec!["ns".to_string()])
            .write_options(stable_row_ids())
            .execute()
            .await
            .unwrap();

        // A decoy of the same name at the root: resolving the source at the
        // wrong namespace materializes one row here instead of three.
        let decoy = record_batch!(("name", Utf8, ["mallory"]), ("age", Int32, [42])).unwrap();
        conn.create_table("people", decoy)
            .write_options(stable_row_ids())
            .execute()
            .await
            .unwrap();

        let view = conn
            .create_materialized_view("adults", "people")
            .namespace(vec!["ns".to_string()])
            .source_namespace(vec!["ns".to_string()])
            .select([("name", "name")])
            .only_if("age >= 18")
            .execute()
            .await
            .unwrap();

        assert_eq!(view.definition().source_table, "people");
        assert_eq!(view.definition().source_namespace, vec!["ns".to_string()]);
        assert_eq!(view.table().namespace(), &["ns"]);

        // Refresh resolves the source at the recorded namespace, not at root.
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.rows_written, 3);
    }

    /// A definition stored before namespaced sources existed carries no
    /// namespace key and must read as the root namespace.
    #[test]
    fn a_definition_without_a_namespace_reads_as_root() {
        let stored =
            r#"{"source_table":"people","projections":[{"output":"name","expression":"name"}]}"#;
        let definition: MaterializedViewDefinition = serde_json::from_str(stored).unwrap();
        assert!(definition.source_namespace.is_empty());
    }

    fn definition(source_namespace: Vec<String>) -> MaterializedViewDefinition {
        MaterializedViewDefinition {
            source_table: "people".to_string(),
            source_namespace,
            projections: vec![ViewProjection {
                output: "name".to_string(),
                expression: "name".to_string(),
            }],
            filter: None,
            limit: None,
            inputs: vec!["name".to_string()],
            function_columns: Vec::new(),
        }
    }

    /// A root definition keeps the pre-namespace `select` form byte-stably;
    /// a namespaced one moves off `select`, which sends pre-namespace readers
    /// to the `Unrecognized` refusal instead of a root resolve.
    #[test]
    fn a_namespaced_definition_is_refused_by_the_pre_namespace_reader() {
        let root = definition_to_metadata(&definition(Vec::new())).unwrap();
        let root: serde_json::Value = serde_json::from_str(&root).unwrap();
        assert_eq!(root["kind"], "select");
        assert!(
            root.get("source_namespace").is_none(),
            "a root definition must not grow new keys: {root}"
        );

        let stored = definition_to_metadata(&definition(vec!["ns".to_string()])).unwrap();
        let value: serde_json::Value = serde_json::from_str(&stored).unwrap();
        // The pre-namespace discriminator is `kind == "select"`; anything
        // else lands in its Unrecognized refusal rather than in a root open.
        assert_eq!(value["kind"], "namespaced_select");

        // The current reader round-trips the coordinate.
        let metadata = HashMap::from([(DEFINITION_META_KEY.to_string(), stored)]);
        match materialized_view_kind(&metadata).unwrap() {
            Some(MaterializedViewKind::Select(read)) => {
                assert_eq!(read.source_namespace, vec!["ns".to_string()])
            }
            other => panic!("expected the namespaced select form, got {other:?}"),
        }
    }

    /// A kind that disagrees with its namespace is an error, not a view:
    /// under `select` it is the shape old readers would resolve at the root.
    #[test]
    fn a_kind_namespace_mismatch_is_refused() {
        for (kind, namespace) in [
            (SELECT_KIND, vec!["ns".to_string()]),
            (NAMESPACED_SELECT_KIND, Vec::new()),
        ] {
            let mut value = serde_json::to_value(definition(namespace)).unwrap();
            value["kind"] = serde_json::Value::String(kind.to_string());
            let metadata = HashMap::from([(DEFINITION_META_KEY.to_string(), value.to_string())]);
            let err = materialized_view_kind(&metadata).unwrap_err();
            assert!(
                err.to_string()
                    .contains("does not match its source namespace"),
                "kind '{kind}': {err}"
            );
        }
    }

    /// A binding as the server records it: one input over `input`, one
    /// scalar output named `output`.
    pub fn test_binding(binding_id: &str, input: &str, output: &str) -> FunctionBinding {
        FunctionBinding::from_json(
            &serde_json::json!({
                "binding_id": binding_id,
                "function": {"name": "embed", "version": "fv_test"},
                "inputs": [{
                    "parameter": "text", "field_id": -1, "field_path": input,
                    "arrow_type": "utf8", "nullable": true,
                }],
                "outputs": [{
                    "result_field": "$value", "output_name": output, "output_field_id": -1,
                    "output_ordinal": 0, "arrow_type": "int32", "nullable": true,
                }],
            })
            .to_string(),
        )
        .unwrap()
    }

    /// A function column as the server declares it on a table.
    pub fn function_field(name: &str, binding_id: &str, input: &str) -> ArrowField {
        ArrowField::new(name, DataType::Int32, true).with_metadata(
            crate::table::computed_columns::function_computed_column_metadata(
                binding_id,
                0,
                &[input.to_string()],
            ),
        )
    }

    pub async fn people(conn: &Connection) -> Table {
        let batch =
            record_batch!(("id", Int32, [1, 2, 3]), ("name", Utf8, ["a", "b", "c"])).unwrap();
        conn.create_table("people", batch)
            .write_options(stable_row_ids())
            .execute()
            .await
            .unwrap()
    }

    async fn prepared_people(conn: &Connection) -> PreparedDeclaration {
        let source = people(conn).await;
        prepare_declaration(
            &source,
            &[
                ("id".to_string(), "id".to_string()),
                ("name".to_string(), "name".to_string()),
            ],
            None,
            None,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_function_column_is_declared_null_with_its_binding() {
        let conn = connect("memory://").execute().await.unwrap();
        let view = prepared_people(&conn)
            .await
            .with_function_columns(
                vec![function_field("emb", "fb_1", "name")],
                &[test_binding("fb_1", "name", "emb")],
            )
            .unwrap()
            .create("v")
            .await
            .unwrap();

        let schema = view.table().schema().await.unwrap();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["id", "name", SOURCE_ROW_ID_COLUMN, "emb"]);
        let emb = schema.field_with_name("emb").unwrap();
        assert!(emb.is_nullable());
        assert_eq!(
            computed_column_from_field(emb).map(|c| c.kind),
            Some(ComputedColumnKind::Function {
                binding_id: "fb_1".into(),
                output_ordinal: 0
            })
        );
        let bindings = crate::table::computed_columns::function_bindings(&schema).unwrap();
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].binding_id(), "fb_1");
        assert_eq!(view.definition().function_columns, ["emb"]);
        let stored: serde_json::Value =
            serde_json::from_str(&schema.metadata()[DEFINITION_META_KEY]).unwrap();
        assert_eq!(stored["kind"], FUNCTION_SELECT_KIND);
        assert_eq!(view.table().count_rows(None).await.unwrap(), 0);

        let reopened = conn.open_materialized_view("v").await.unwrap();
        assert_eq!(reopened.definition().function_columns, ["emb"]);
    }

    #[tokio::test]
    async fn function_column_declarations_are_validated() {
        let conn = connect("memory://").execute().await.unwrap();
        let prepared = prepared_people(&conn).await;
        let binding = test_binding("fb_1", "name", "emb");
        let fails = |prepared: PreparedDeclaration,
                     fields: Vec<ArrowField>,
                     bindings: &[FunctionBinding]| {
            prepared
                .with_function_columns(fields, bindings)
                .err()
                .map(|e| e.to_string())
                .expect("the declaration should be refused")
        };

        let not_nullable = function_field("emb", "fb_1", "name").with_nullable(false);
        let err = fails(
            prepared.clone(),
            vec![not_nullable],
            std::slice::from_ref(&binding),
        );
        assert!(err.contains("must be nullable"), "{err}");

        let plain = ArrowField::new("emb", DataType::Int32, true);
        let err = fails(
            prepared.clone(),
            vec![plain],
            std::slice::from_ref(&binding),
        );
        assert!(
            err.contains("does not carry a declared Function binding"),
            "{err}"
        );

        let other_binding = function_field("emb", "fb_other", "name");
        let err = fails(
            prepared.clone(),
            vec![other_binding],
            std::slice::from_ref(&binding),
        );
        assert!(
            err.contains("does not carry a declared Function binding"),
            "{err}"
        );

        let err = fails(
            prepared.clone(),
            vec![function_field("emb", "fb_1", "name")],
            &[test_binding("fb_1", "bio", "emb")],
        );
        assert!(
            err.contains("reads 'bio', which the view does not project"),
            "{err}"
        );

        let err = fails(
            prepared.clone(),
            vec![function_field("name", "fb_1", "name")],
            std::slice::from_ref(&binding),
        );
        assert!(err.contains("already exists"), "{err}");

        let err = fails(
            prepared.clone(),
            vec![function_field(SOURCE_ROW_ID_COLUMN, "fb_1", "name")],
            std::slice::from_ref(&binding),
        );
        assert!(err.contains("reserved"), "{err}");

        let err = fails(
            prepared.clone(),
            vec![function_field("emb", "fb_1", "name")],
            &[binding.clone(), test_binding("fb_2", "name", "emb2")],
        );
        assert!(err.contains("'fb_2' declares no view column"), "{err}");

        let err = fails(prepared, Vec::new(), std::slice::from_ref(&binding));
        assert!(err.contains("at least one function column"), "{err}");
    }

    /// A definition without function columns serializes as it always has, so
    /// released readers keep refreshing it. One with function columns takes a
    /// kind they route to the same refusal an unknown kind gets, instead of
    /// reporting the view's schema as not matching its definition.
    #[test]
    fn a_function_definition_is_refused_by_the_pre_function_reader() {
        let root: serde_json::Value =
            serde_json::from_str(&definition_to_metadata(&definition(Vec::new())).unwrap())
                .unwrap();
        assert_eq!(root["kind"], SELECT_KIND);
        assert!(
            root.get("function_columns").is_none(),
            "a definition without function columns must not grow new keys"
        );

        let mut with_functions = definition(Vec::new());
        with_functions.function_columns = vec!["emb".to_string()];
        let stored = definition_to_metadata(&with_functions).unwrap();
        let value: serde_json::Value = serde_json::from_str(&stored).unwrap();
        assert_eq!(value["kind"], FUNCTION_SELECT_KIND);
        // The pre-function reader's discriminator.
        let kind = value["kind"].as_str().unwrap();
        assert!(kind != SELECT_KIND && kind != NAMESPACED_SELECT_KIND);

        let mut namespaced = definition(vec!["ns".to_string()]);
        namespaced.function_columns = vec!["emb".to_string()];
        let value: serde_json::Value =
            serde_json::from_str(&definition_to_metadata(&namespaced).unwrap()).unwrap();
        assert_eq!(value["kind"], NAMESPACED_FUNCTION_SELECT_KIND);

        let metadata = HashMap::from([(DEFINITION_META_KEY.to_string(), stored)]);
        match materialized_view_kind(&metadata).unwrap() {
            Some(MaterializedViewKind::Select(read)) => assert_eq!(read, with_functions),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn a_kind_function_mismatch_is_refused() {
        for (kind, function_columns) in [
            (SELECT_KIND, vec!["emb".to_string()]),
            (FUNCTION_SELECT_KIND, Vec::new()),
        ] {
            let mut definition = definition(Vec::new());
            definition.function_columns = function_columns;
            let mut value = serde_json::to_value(&definition).unwrap();
            value["kind"] = serde_json::Value::String(kind.to_string());
            let metadata = HashMap::from([(DEFINITION_META_KEY.to_string(), value.to_string())]);
            let err = materialized_view_kind(&metadata).unwrap_err().to_string();
            assert!(err.contains("does not match its source namespace"), "{err}");
        }
    }
}
