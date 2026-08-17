// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Materialized views.
//!
//! A materialized view is a table whose contents are defined by a query over
//! one source table and maintained by refresh rather than by writes. Creation
//! commits an empty table carrying the kind-tagged definition in schema
//! metadata; a kind added later reads back as unrefreshable, not as a plain
//! table. Queries, indexes and search work on the view unchanged.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field as ArrowField, FieldRef, Schema as ArrowSchema, SchemaRef};
use datafusion_common::ScalarValue;
use lance_core::ROW_ID;
use lance_datafusion::planner::Planner;
use serde::{Deserialize, Serialize};

use crate::connection::Connection;
use crate::database::listing::OPT_NEW_TABLE_ENABLE_STABLE_ROW_IDS;
use crate::database::{CreateTableRequest, Database, OpenTableRequest};
use crate::embeddings::EmbeddingDefinition;
use crate::table::Table;
use crate::table::refresh::quote_identifier;
use crate::table::{ColumnDefinition, ColumnKind};
use crate::{Error, Result};

/// Schema metadata key holding the view definition, as kind-tagged JSON.
pub const DEFINITION_META_KEY: &str = "mv.definition";

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
pub const SELECT_KIND: &str = "select";

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
    value["kind"] = serde_json::Value::String(SELECT_KIND.to_string());
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
    if kind != SELECT_KIND {
        return Ok(Some(MaterializedViewKind::Unrecognized {
            kind: kind.to_string(),
        }));
    }
    let definition = serde_json::from_value(value).map_err(|e| unreadable(&e))?;
    Ok(Some(MaterializedViewKind::Select(definition)))
}

/// Resolve a definition against the source schema into the view's projected
/// fields, with `inputs` filled in. Everything statically checkable is
/// checked here rather than at refresh time. Empty `projections` selects
/// every source column as the schema stands now.
pub(crate) fn plan(
    source_schema: SchemaRef,
    source_table: &str,
    projections: &[(String, String)],
    filter: Option<&str>,
    limit: Option<u64>,
) -> Result<(MaterializedViewDefinition, Vec<ArrowField>, Lineage)> {
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

    if let Some(filter) = filter {
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
        projections: projections
            .into_iter()
            .map(|(output, expression)| ViewProjection { output, expression })
            .collect(),
        filter: filter.map(String::from),
        limit,
        inputs,
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
    definition: MaterializedViewDefinition,
    /// The source's own database: the only place
    /// [`PreparedDeclaration::create`] will put the view, because refresh
    /// resolves the recorded source name through the view's database.
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

    /// Create the view table and verify it, consuming the declaration.
    ///
    /// The view goes in the source's own database, where refresh resolves the
    /// recorded source name. Stable row ids are requested at both levels and
    /// verified rather than trusted; nothing is rolled back on failure.
    pub async fn create(self, name: &str) -> Result<MaterializedView> {
        let empty: Vec<std::result::Result<arrow_array::RecordBatch, arrow_schema::ArrowError>> =
            vec![];
        let reader: Box<dyn arrow_array::RecordBatchReader + Send> =
            Box::new(arrow_array::RecordBatchIterator::new(empty, self.schema));
        let mut request = CreateTableRequest::new(name.to_string(), Box::new(reader));
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
        Ok(MaterializedView {
            table,
            definition: self.definition,
        })
    }
}

/// Validate a view declaration against its live source and hold what its
/// creation needs. The declaration is canonicalized through the coordinate a
/// refresh will resolve, so a handle that does not resolve back to itself is
/// rejected, as is a namespaced source. Same creation-time checks as
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
    // The definition records the source by bare name; any other source
    // form would be recorded as a name its refresh cannot resolve.
    if !source.namespace().is_empty() {
        return Err(Error::NotSupported {
            message: format!(
                "a namespaced source cannot be recorded in a view definition; \
                 '{}' must be a root-namespace table",
                source.name()
            ),
        });
    }
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
            namespace_path: vec![],
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
    let source_schema = resolved.schema().await?;
    let source_metadata = source_schema.metadata().clone();
    let (definition, mut fields, lineage) = plan(
        source_schema.clone(),
        resolved.name(),
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
        definition,
        database,
    })
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
    source: String,
    projections: Vec<(String, String)>,
    filter: Option<String>,
    limit: Option<u64>,
}

impl CreateMaterializedViewBuilder {
    pub(crate) fn new(connection: Connection, name: String, source: String) -> Self {
        Self {
            connection,
            name,
            source,
            projections: Vec::new(),
            filter: None,
            limit: None,
        }
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
        let source = self.connection.open_table(&self.source).execute().await?;
        let prepared = prepare_declaration(
            &source,
            &self.projections,
            self.filter.as_deref(),
            self.limit,
        )
        .await?;
        prepared.create(&self.name).await
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
        match materialized_view_kind(schema.metadata())? {
            Some(MaterializedViewKind::Select(definition)) => Ok(Self { table, definition }),
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
}

impl Connection {
    /// Define a materialized view named `name` over `source`.
    ///
    /// The view is created empty, with the definition recorded in its schema
    /// metadata; refresh computes the rows. Local databases only.
    ///
    /// ```no_run
    /// # use lancedb::Connection;
    /// # async fn create(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let view = conn
    ///     .create_materialized_view("loud_adults", "people")
    ///     .select([("name", "upper(name)"), ("age", "age")])
    ///     .only_if("age >= 18")
    ///     .execute()
    ///     .await?;
    /// println!("{}", view.definition().source_table);
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

        // A namespaced source cannot be recorded in the definition: the
        // bare name refresh resolves would reach a different table or none.
        let namespaced = crate::table::NativeTable::create(
            "memory://ns_src",
            "ns_src",
            vec!["ns".to_string()],
            Box::new(arrow_array::RecordBatchIterator::new(
                vec![],
                std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                    "id",
                    arrow_schema::DataType::Int32,
                    true,
                )])),
            )) as Box<dyn arrow_array::RecordBatchReader + Send>,
            None,
            None,
            None,
            None,
            std::collections::HashSet::new(),
        )
        .await
        .unwrap();
        let namespaced = Table::new(std::sync::Arc::new(namespaced), conn.database().clone());
        let err = prepare_declaration(&namespaced, &[], None, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("namespaced source"), "{err}");
    }
}
