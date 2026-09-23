// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Views with `GROUP BY`: one row per group, planned and executed by
//! DataFusion, and recomputed in full whenever the source changes, since a
//! group spans fragments.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::cast::AsArray;
use arrow_array::{Array, FixedSizeListArray, UInt32Array};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef};
use datafusion::catalog::default_table_source::provider_as_source;
use datafusion::datasource::empty::EmptyTable;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use datafusion_common::DataFusionError;
use datafusion_common::TableReference;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::planner::{ContextProvider, ExprPlanner};
use datafusion_expr::{
    AggregateUDF, ColumnarValue, Expr, HigherOrderUDF, LogicalPlan, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TableSource, Volatility, WindowUDF,
};
use datafusion_sql::planner::SqlToRel;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;
use futures::StreamExt;
use lance::Dataset;
use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};
use lance_core::ROW_ID;
use lance_datafusion::exec::SessionContextExt;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::vector::ivf::{IvfTransformer, new_ivf_transformer};
use lance_linalg::distance::DistanceType;
use lance_linalg::kernels::normalize_fsl;
use uuid::Uuid;

use super::refresh::to_view_batch;
use super::{
    MaterializedViewDefinition, Planned, SOURCE_ROW_ID_COLUMN, ViewProjection, ensure_immutable,
    plan_filter, query, without_declarations,
};
use crate::{Error, Result};

/// The name the source is planned under; never visible to the user.
const SOURCE: &str = "__source";

/// `ivf_partition(column)`: the partition of the IVF index on `column` that
/// each vector falls in, assigned by that index's centroids and distance
/// type. Bound per refresh, so a retrained index regroups the view.
pub const IVF_PARTITION: &str = "ivf_partition";

/// The index `ivf_partition(column)` assigns by.
#[derive(Debug, Clone)]
struct IvfBinding {
    index: Uuid,
    transformer: Arc<IvfTransformer>,
    /// A cosine index assigns L2 over unit vectors, as lance's index path does.
    normalize: bool,
}

/// `bindings` maps a source column to its index; planning runs unbound.
#[derive(Debug)]
struct IvfPartition {
    signature: Signature,
    bindings: HashMap<String, IvfBinding>,
}

impl PartialEq for IvfPartition {
    fn eq(&self, other: &Self) -> bool {
        self.bindings.len() == other.bindings.len()
            && self.bindings.iter().all(|(column, binding)| {
                other
                    .bindings
                    .get(column)
                    .is_some_and(|b| b.index == binding.index)
            })
    }
}

impl Eq for IvfPartition {}

impl std::hash::Hash for IvfPartition {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let mut bound: Vec<_> = self.bindings.iter().map(|(c, b)| (c, b.index)).collect();
        bound.sort();
        bound.hash(state);
    }
}

impl ScalarUDFImpl for IvfPartition {
    fn name(&self) -> &str {
        IVF_PARTITION
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        // Float vectors under L2/dot/cosine, byte vectors under Hamming; the
        // binding checks the index's metric against the element type.
        match arg_types {
            [DataType::FixedSizeList(element, _)]
                if element.data_type().is_floating() || *element.data_type() == DataType::UInt8 =>
            {
                Ok(DataType::UInt32)
            }
            other => datafusion_common::plan_err!(
                "{IVF_PARTITION} takes one vector column, not {other:?}"
            ),
        }
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        // The argument is a bare column (checked at planning), so its field
        // names the column the binding was made for.
        let column = args.arg_fields[0].name();
        let Some(binding) = self.bindings.get(column) else {
            return datafusion_common::exec_err!("{IVF_PARTITION}({column}) has no index bound");
        };
        let vectors = args.args[0].to_array(args.number_rows)?;
        let vectors = vectors.as_fixed_size_list();
        let normalized;
        let vectors = if binding.normalize {
            normalized =
                normalize_fsl(vectors).map_err(|e| DataFusionError::External(Box::new(e)))?;
            &normalized
        } else {
            vectors
        };
        let partitions = binding
            .transformer
            .compute_partitions(vectors)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        // A null vector falls in no partition, and neither does one the
        // assigner could not place (a zero vector under cosine): a NULL bucket
        // is honest, membership in bucket 0 is not.
        let partitions = UInt32Array::new(
            partitions.values().clone(),
            NullBuffer::union(vectors.nulls(), partitions.nulls()),
        );
        Ok(ColumnarValue::Array(Arc::new(partitions)))
    }
}

fn session(bindings: HashMap<String, IvfBinding>) -> SessionState {
    let mut state = SessionStateBuilder::new().with_default_features().build();
    let ivf = IvfPartition {
        signature: Signature::any(1, Volatility::Immutable),
        bindings,
    };
    state
        .register_udf(Arc::new(ScalarUDF::new_from_impl(ivf)))
        .expect("registering a scalar function cannot fail");
    state
}

/// The columns `plan` calls `ivf_partition` on. The argument must be a bare
/// column: its field is how the bound function finds the column's index.
fn ivf_columns(plan: &LogicalPlan) -> Result<Vec<String>> {
    let mut columns = Vec::new();
    let mut misuse = None;
    plan.apply(|node| {
        for expr in node.expressions() {
            expr.apply(|e| {
                if let Expr::ScalarFunction(call) = e
                    && call.name() == IVF_PARTITION
                {
                    match call.args.as_slice() {
                        [Expr::Column(column)] => columns.push(column.name.clone()),
                        _ => misuse = Some(e.to_string()),
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map_err(|e| Error::InvalidInput {
        message: format!("invalid grouped view: {e}"),
    })?;
    if let Some(call) = misuse {
        return Err(Error::InvalidInput {
            message: format!("{IVF_PARTITION} takes a vector column of the source, not `{call}`"),
        });
    }
    columns.sort();
    columns.dedup();
    Ok(columns)
}

/// Refuse a grouped `definition` whose functions cannot be bound over
/// `source`, such as `ivf_partition` on a column without an IVF index.
pub(super) async fn check(source: &Dataset, definition: &MaterializedViewDefinition) -> Result<()> {
    bind(source, definition).await.map(|_| ())
}

/// Bind every `ivf_partition(column)` in `definition` to the IVF index on
/// that column of `source`.
async fn bind(
    source: &Dataset,
    definition: &MaterializedViewDefinition,
) -> Result<HashMap<String, IvfBinding>> {
    let schema = Arc::new(ArrowSchema::from(source.schema()));
    let planned = logical_plan(&session(HashMap::new()), empty_source(&schema), definition)?;
    let mut bindings = HashMap::new();
    for column in ivf_columns(&planned)? {
        bindings.insert(column.clone(), bind_column(source, &column).await?);
    }
    Ok(bindings)
}

async fn bind_column(source: &Dataset, column: &str) -> Result<IvfBinding> {
    let field = source
        .schema()
        .field(column)
        .ok_or_else(|| Error::InvalidInput {
            message: format!("{IVF_PARTITION}: the source has no column '{column}'"),
        })?;
    // One index, whose segments all assign by one model. A segment trained
    // on its own fragments carries its own centroids, and grouping every row
    // by one segment's model would bucket the other segments' rows wrongly.
    let mut found: Option<(String, Uuid, DistanceType, FixedSizeListArray)> = None;
    for index in source.load_indices().await?.iter() {
        if index.fields != [field.id] {
            continue;
        }
        let Ok(vector) = source
            .open_vector_index(column, &index.uuid, &NoOpMetricsCollector)
            .await
        else {
            continue;
        };
        let Some(centroids) = vector.ivf_model().centroids.clone() else {
            continue;
        };
        let metric = vector.metric_type();
        match &found {
            None => found = Some((index.name.clone(), index.uuid, metric, centroids)),
            Some((name, _, seen_metric, seen)) if *name == index.name => {
                if *seen_metric != metric || seen.to_data() != centroids.to_data() {
                    return Err(Error::InvalidInput {
                        message: format!(
                            "{IVF_PARTITION}({column}): the segments of index '{name}' were \
                             trained separately and assign by different models; rebuild \
                             the index before grouping by it"
                        ),
                    });
                }
            }
            Some((name, ..)) => {
                return Err(Error::InvalidInput {
                    message: format!(
                        "{IVF_PARTITION}({column}) is ambiguous: indices '{name}' and '{}' \
                         both cover it",
                        index.name
                    ),
                });
            }
        }
    }
    let byte_vectors = matches!(
        field.data_type(),
        DataType::FixedSizeList(element, _) if *element.data_type() == DataType::UInt8
    );
    let Some((_, uuid, metric, centroids)) = found else {
        return Err(Error::InvalidInput {
            message: format!(
                "{IVF_PARTITION}({column}) needs an IVF vector index on '{column}'; create one first"
            ),
        });
    };
    // The assigner pairs byte vectors with Hamming and float vectors with the
    // rest; an index of the other kind cannot place this column's values.
    if byte_vectors != (metric == DistanceType::Hamming) {
        return Err(Error::InvalidInput {
            message: format!(
                "{IVF_PARTITION}({column}): the index assigns by {metric}, which does not \
                 apply to {}",
                field.data_type()
            ),
        });
    }
    // Lance's index path assigns cosine by L2 over unit vectors.
    let normalize = metric == DistanceType::Cosine;
    let distance = if normalize { DistanceType::L2 } else { metric };
    Ok(IvfBinding {
        index: uuid,
        transformer: Arc::new(new_ivf_transformer(centroids, distance, vec![])),
        normalize,
    })
}

fn empty_source(source_schema: &SchemaRef) -> Arc<dyn TableSource> {
    let mut fields = source_schema.fields().to_vec();
    fields.push(Arc::new(ArrowField::new(ROW_ID, DataType::UInt64, false)));
    provider_as_source(Arc::new(EmptyTable::new(Arc::new(ArrowSchema::new(
        fields,
    )))))
}

/// The query DataFusion runs: the view's projections plus the group's
/// smallest source row id, which stands as the row's provenance. The filter
/// is not here; the lance scan applies it, as for any other view.
fn sql(definition: &MaterializedViewDefinition) -> String {
    let items: Vec<String> = definition
        .projections
        .iter()
        .map(|p| format!("{} AS {}", p.expression, query::ident_sql(&p.output)))
        .collect();
    format!(
        "SELECT {}, min({ROW_ID}) AS {ROW_ID} FROM {SOURCE} GROUP BY {}",
        items.join(", "),
        definition.group_by.join(", ")
    )
}

struct Provider<'a> {
    state: &'a SessionState,
    source: Arc<dyn TableSource>,
}

impl ContextProvider for Provider<'_> {
    fn get_table_source(
        &self,
        name: TableReference,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        if name.schema().is_none() && name.table() == SOURCE {
            Ok(self.source.clone())
        } else {
            datafusion_common::plan_err!("a grouped view reads only its source, not '{name}'")
        }
    }
    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        self.state.expr_planners()
    }
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions().get(name).cloned()
    }
    fn get_higher_order_meta(&self, name: &str) -> Option<Arc<HigherOrderUDF>> {
        self.state.higher_order_functions().get(name).cloned()
    }
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions().get(name).cloned()
    }
    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.state.window_functions().get(name).cloned()
    }
    fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
        None
    }
    fn options(&self) -> &ConfigOptions {
        self.state.config_options()
    }
    fn udf_names(&self) -> Vec<String> {
        self.state.scalar_functions().keys().cloned().collect()
    }
    fn higher_order_function_names(&self) -> Vec<String> {
        self.state
            .higher_order_functions()
            .keys()
            .cloned()
            .collect()
    }
    fn udaf_names(&self) -> Vec<String> {
        self.state.aggregate_functions().keys().cloned().collect()
    }
    fn udwf_names(&self) -> Vec<String> {
        self.state.window_functions().keys().cloned().collect()
    }
}

fn logical_plan(
    state: &SessionState,
    source: Arc<dyn TableSource>,
    definition: &MaterializedViewDefinition,
) -> Result<LogicalPlan> {
    let invalid = |e: &dyn std::fmt::Display| Error::InvalidInput {
        message: format!("invalid grouped view: {e}"),
    };
    let statement = Parser::parse_sql(&GenericDialect {}, &sql(definition))
        .map_err(|e| invalid(&e))?
        .pop()
        .ok_or_else(|| invalid(&"empty query"))?;
    SqlToRel::new(&Provider { state, source })
        .sql_statement_to_plan(statement)
        .map_err(|e| invalid(&e))
}

/// Plan a grouped definition against the source schema. `filter` is the
/// canonical filter, already spelled the way [`super::plan`] stores it.
pub(super) fn plan(
    source_schema: SchemaRef,
    definition: &MaterializedViewDefinition,
    filter: Option<String>,
) -> Result<Planned> {
    query::check_grouping(definition)?;
    let mut inputs = match &filter {
        Some(filter) => plan_filter(&source_schema, filter)?,
        None => Vec::new(),
    };
    let mut projections: Vec<ViewProjection> = Vec::with_capacity(definition.projections.len());
    for p in &definition.projections {
        if p.output == SOURCE_ROW_ID_COLUMN || p.output == ROW_ID {
            return Err(Error::InvalidInput {
                message: format!("view column name '{}' is reserved", p.output),
            });
        }
        if projections.iter().any(|seen| seen.output == p.output) {
            return Err(Error::ColumnAlreadyExists {
                name: p.output.clone(),
            });
        }
        let expression =
            query::canonical_expr(&p.expression).map_err(|e| Error::InvalidExpression {
                column: p.output.clone(),
                message: e.to_string(),
            })?;
        projections.push(ViewProjection {
            output: p.output.clone(),
            expression,
        });
    }
    let group_by = definition
        .group_by
        .iter()
        .map(|key| query::canonical_expr(key))
        .collect::<Result<Vec<_>>>()?;
    let definition = MaterializedViewDefinition {
        projections,
        filter,
        group_by,
        ..definition.clone()
    };

    let planned = logical_plan(
        &session(HashMap::new()),
        empty_source(&source_schema),
        &definition,
    )?;
    ivf_columns(&planned)?;

    let mut exprs = Vec::new();
    planned
        .apply(|node| {
            exprs.extend(node.expressions());
            Ok(TreeNodeRecursion::Continue)
        })
        .map_err(|e| Error::InvalidInput {
            message: format!("invalid grouped view: {e}"),
        })?;
    for expr in &exprs {
        ensure_immutable(expr, |message| Error::InvalidInput {
            message: format!("invalid grouped view: {message}"),
        })?;
        inputs.extend(
            expr.column_refs()
                .into_iter()
                .map(|c| c.name.clone())
                .filter(|name| name != ROW_ID && source_schema.field_with_name(name).is_ok()),
        );
    }
    inputs.sort();
    inputs.dedup();

    let fields = planned
        .schema()
        .fields()
        .iter()
        .filter(|f| f.name() != ROW_ID)
        .map(|f| without_declarations(f))
        .collect();
    Ok(Planned {
        definition,
        fields,
        lineage: HashMap::new(),
        inputs,
    })
}

/// Every group of `source`, as view rows in `schema`.
pub(super) async fn stream(
    source: &Dataset,
    definition: &MaterializedViewDefinition,
    inputs: &[String],
    schema: SchemaRef,
    rows_written: Arc<AtomicU64>,
) -> Result<SendableRecordBatchStream> {
    let mut scanner = source.scan();
    scanner.with_row_id();
    if let Some(filter) = &definition.filter {
        scanner.filter(filter)?;
    }
    scanner.project(inputs)?;
    let scan: SendableRecordBatchStream = scanner.try_into_stream().await?.into();

    let state = session(bind(source, definition).await?);
    let ctx = SessionContext::new_with_state(state.clone());
    let source = ctx.read_one_shot(scan)?.into_view();
    let planned = logical_plan(&state, provider_as_source(source), definition)?;
    let groups = ctx
        .execute_logical_plan(planned)
        .await?
        .execute_stream()
        .await?;

    let out_schema = schema.clone();
    let mapped = groups.map(move |batch| {
        let batch = to_view_batch(&batch?, &out_schema)?;
        rows_written.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        Ok(batch)
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mapped)))
}
