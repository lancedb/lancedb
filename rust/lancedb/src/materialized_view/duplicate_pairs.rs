// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Native pair generation as partitioned materialized-view refresh work.

use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, TableFunctionArgs, Value,
};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::transaction::Operation;
use lance::dataset::{InsertBuilder, WriteDestination, WriteMode, WriteParams};
use lance_table::format::Fragment;
use serde::{Deserialize, Serialize};

use super::grouped_units::GroupedRefreshPlan;
use super::refresh::{
    RefreshMaterializedViewResult, ensure_incarnation, ensure_no_mem_wal, open_source,
};
use super::{MaterializedViewDefinition, Planned, ViewProjection};
use crate::table::Table;
use crate::table::datafusion::udtf::duplicate_pairs::{
    DUPLICATE_PAIRS_OPERATOR_VERSION, DuplicatePairTask, DuplicatePairsConfig, DuplicatePairsExec,
    duplicate_pair_schema, plan_duplicate_pairs,
};
use crate::{Error, Result};

pub(super) const FUNCTION_NAME: &str = "vector_duplicate_pairs";
pub(super) const DEDUP_FUNCTION_NAME: &str = "vector_dedup";

/// The result of a pinned native vector source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorSourceKind {
    /// The qualifying pair relation.
    Pairs,
    /// Original source rows retained by greedy direct representatives.
    Dedup,
}

/// A native table-function source, pinned for the lifetime of the view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorSource {
    pub kind: VectorSourceKind,
    pub dataset_version: u64,
    pub column: String,
    /// Validated numeric SQL spelling; execution and task identity use f32 bits.
    pub distance_threshold: String,
}

impl VectorSource {
    pub(super) fn function_name(&self) -> &'static str {
        match self.kind {
            VectorSourceKind::Pairs => FUNCTION_NAME,
            VectorSourceKind::Dedup => DEDUP_FUNCTION_NAME,
        }
    }
    pub(crate) fn config(&self) -> Result<DuplicatePairsConfig> {
        let threshold: f32 = self
            .distance_threshold
            .parse()
            .map_err(|_| invalid("invalid pair threshold"))?;
        if self.dataset_version == 0 || self.column.is_empty() || !threshold.is_finite() {
            return Err(invalid(
                "duplicate pairs require a positive snapshot, a column and a finite threshold",
            ));
        }
        Ok(DuplicatePairsConfig {
            dataset_version: self.dataset_version,
            column: self.column.clone(),
            distance_threshold: threshold,
        })
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidInput {
        message: message.into(),
    }
}

pub(super) fn source(
    args: &TableFunctionArgs,
    kind: VectorSourceKind,
) -> Result<(Vec<String>, String, VectorSource)> {
    if args.settings.is_some() {
        return Err(invalid("duplicate pairs do not accept function settings"));
    }
    let exprs = args
        .args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
            _ => Err(invalid(
                "duplicate pairs require four positional literal arguments",
            )),
        })
        .collect::<Result<Vec<_>>>()?;
    let [table, version, column, threshold] = exprs.as_slice() else {
        return Err(invalid(
            "native vector sources require (table, dataset_version, column, distance_threshold)",
        ));
    };
    let string = |expr: &Expr| match expr {
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(value) => Ok(value.clone()),
            _ => Err(invalid("table and column must be string literals")),
        },
        _ => Err(invalid("table and column must be string literals")),
    };
    let table = string(table)?;
    let plain = MaterializedViewDefinition::from_sql(&format!("SELECT * FROM {table}"))?;
    if plain.vector_source.is_some()
        || plain.lateral.is_some()
        || plain.filter.is_some()
        || plain.is_grouped()
        || plain.limit.is_some()
    {
        return Err(invalid("duplicate pairs must name one source table"));
    }
    let dataset_version = match version {
        Expr::Value(value) => match &value.value {
            Value::Number(number, _) => number
                .parse()
                .map_err(|_| invalid("invalid source version"))?,
            _ => return Err(invalid("source version must be an integer literal")),
        },
        _ => return Err(invalid("source version must be an integer literal")),
    };
    let config = VectorSource {
        kind,
        dataset_version,
        column: string(column)?,
        distance_threshold: threshold.to_string(),
    };
    config.config()?;
    Ok((plain.source_namespace, plain.source_table, config))
}

pub(super) fn check_shape(definition: &MaterializedViewDefinition) -> Result<()> {
    if definition
        .vector_source
        .as_ref()
        .is_some_and(|s| s.kind == VectorSourceKind::Dedup)
    {
        if !definition.selects_star()
            || definition.filter.is_some()
            || definition.limit.is_some()
            || definition.lateral.is_some()
            || definition.is_grouped()
        {
            return Err(invalid(
                "a vector_dedup view must select * without other clauses",
            ));
        }
        return Ok(());
    }
    let schema = duplicate_pair_schema();
    let projections = schema
        .fields()
        .iter()
        .map(|field| ViewProjection {
            output: field.name().clone(),
            expression: field.name().clone(),
        })
        .collect::<Vec<_>>();
    if (!definition.selects_star() && definition.projections != projections)
        || definition.filter.is_some()
        || definition.limit.is_some()
        || definition.lateral.is_some()
        || definition.is_grouped()
    {
        return Err(invalid(
            "a native pair view selects *, or row_id_a, row_id_b, distance, without other clauses",
        ));
    }
    Ok(())
}

pub(super) fn plan(
    source_schema: SchemaRef,
    definition: &MaterializedViewDefinition,
) -> Result<Planned> {
    check_shape(definition)?;
    let config = definition.vector_source.as_ref().expect("native source");
    config.config()?;
    source_schema.field_with_name(&config.column)?;
    if config.kind == VectorSourceKind::Dedup {
        return Ok(Planned {
            definition: definition.clone(),
            fields: source_schema
                .fields()
                .iter()
                .map(|f| super::without_declarations(f))
                .collect(),
            lineage: source_schema
                .fields()
                .iter()
                .map(|f| (f.name().clone(), vec![f.name().clone()]))
                .collect(),
            inputs: source_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
        });
    }
    let schema = duplicate_pair_schema();
    let projections = schema
        .fields()
        .iter()
        .map(|field| ViewProjection {
            output: field.name().clone(),
            expression: field.name().clone(),
        })
        .collect();
    let mut definition = definition.clone();
    let inputs = vec![config.column.clone()];
    definition.projections = projections;
    Ok(Planned {
        definition,
        fields: schema.fields().iter().map(|f| f.as_ref().clone()).collect(),
        lineage: Default::default(),
        inputs,
    })
}

/// A saved native refresh plan, including every physical partition identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuplicatePairsRefreshPlan {
    pub snapshot: GroupedRefreshPlan,
    pub source_uri: String,
    pub definition_sql: String,
    pub operator_version: u32,
    pub threshold_bits: u32,
    pub tasks: Vec<DuplicatePairTask>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WrittenPairsPartition {
    pub(super) unit: u32,
    pub(super) plan_id: String,
    pub(super) task: DuplicatePairTask,
    pub(super) fragments: Vec<Fragment>,
    pub(super) rows: u64,
}

impl DuplicatePairsRefreshPlan {
    pub(super) fn identity(&self) -> Result<String> {
        use sha2::{Digest, Sha256};
        let encoded = serde_json::to_vec(&(
            &self.snapshot,
            &self.source_uri,
            &self.definition_sql,
            self.operator_version,
            self.threshold_bits,
        ))
        .map_err(|e| invalid(e.to_string()))?;
        Ok(format!("{:x}", Sha256::digest(encoded)))
    }
}

async fn open(view: &Table) -> Result<(Dataset, MaterializedViewDefinition, Arc<Dataset>)> {
    let native = view
        .as_native()
        .ok_or_else(|| invalid("native pair refresh requires a local view"))?;
    native.dataset.ensure_mutable()?;
    native.dataset.reload().await?;
    let view_ds = native.dataset.get().await?.as_ref().clone();
    let Some(super::StoredDefinition::Query(definition)) =
        super::read_definition(&view_ds.schema().metadata)?
    else {
        return Err(invalid("view does not carry a supported definition"));
    };
    let config = definition
        .vector_source
        .as_ref()
        .ok_or_else(|| invalid("not a native pair view"))?
        .config()?;
    let source = Arc::new(
        open_source(view, &definition, None)
            .await?
            .checkout_version(config.dataset_version)
            .await?,
    );
    ensure_no_mem_wal(&source, "source table", &definition.source_table).await?;
    let planned = plan(Arc::new(Schema::from(source.schema())), &definition)?;
    let expected = Schema::new(planned.fields);
    let physical = Schema::from(view_ds.schema());
    if physical.fields().len() != expected.fields().len()
        || physical
            .fields()
            .iter()
            .zip(expected.fields())
            .any(|(a, b)| {
                a.name() != b.name()
                    || a.data_type() != b.data_type()
                    || a.is_nullable() != b.is_nullable()
            })
    {
        return Err(invalid("native pair view schema changed since creation"));
    }
    Ok((view_ds, planned.definition, source))
}

pub(super) async fn plan_refresh(
    view: &Table,
    pinned: Option<u64>,
) -> Result<DuplicatePairsRefreshPlan> {
    let (view_ds, definition, source) = open(view).await?;
    let config = definition
        .vector_source
        .as_ref()
        .expect("native")
        .config()?;
    if pinned.is_some_and(|v| v != config.dataset_version) {
        return Err(invalid(
            "refresh source version differs from the native pair view's fixed snapshot",
        ));
    }
    let tasks = plan_duplicate_pairs(source.clone(), &config).await?;
    let units = u32::try_from(tasks.len()).map_err(|_| invalid("too many index partitions"))?;
    let incarnation = view_ds
        .schema()
        .metadata
        .get(super::INCARNATION_META_KEY)
        .cloned()
        .ok_or_else(|| invalid("native pair view has no incarnation"))?;
    Ok(DuplicatePairsRefreshPlan {
        snapshot: GroupedRefreshPlan {
            units,
            source_version: config.dataset_version,
            view_version: view_ds.version().version,
            incarnation,
        },
        source_uri: source.uri().to_string(),
        definition_sql: definition.to_sql(),
        operator_version: DUPLICATE_PAIRS_OPERATOR_VERSION,
        threshold_bits: config.distance_threshold.to_bits(),
        tasks,
    })
}

pub(super) async fn open_planned(
    view: &Table,
    plan: &DuplicatePairsRefreshPlan,
) -> Result<(Dataset, MaterializedViewDefinition, Arc<Dataset>)> {
    let (view_ds, definition, source) = open(view).await?;
    ensure_incarnation(&view_ds, Some(&plan.snapshot.incarnation), view.name()).await?;
    let config = definition
        .vector_source
        .as_ref()
        .expect("native")
        .config()?;
    if source.uri() != plan.source_uri
        || source.version().version != plan.snapshot.source_version
        || definition.to_sql() != plan.definition_sql
        || config.distance_threshold.to_bits() != plan.threshold_bits
        || plan.operator_version != DUPLICATE_PAIRS_OPERATOR_VERSION
        || plan.tasks.len() != plan.snapshot.units as usize
    {
        return Err(invalid(
            "native pair task belongs to a different snapshot, configuration or operator",
        ));
    }
    Ok((view_ds, definition, source))
}

pub(super) async fn write_unit(
    view: &Table,
    unit: u32,
    plan: &DuplicatePairsRefreshPlan,
) -> Result<WrittenPairsPartition> {
    let (view_ds, definition, source) = open_planned(view, plan).await?;
    let task = plan
        .tasks
        .get(unit as usize)
        .ok_or_else(|| invalid("pair partition is outside the plan"))?
        .clone();
    let config = definition
        .vector_source
        .as_ref()
        .expect("native")
        .config()?;
    let exec = DuplicatePairsExec::try_new(source, config, vec![task.clone()])?;
    let stream = exec.execute(0, Arc::new(TaskContext::default()))?;
    let rows = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let count = rows.clone();
    let stream = stream.inspect_ok(move |batch| {
        count.fetch_add(
            batch.num_rows() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    });
    let stream: SendableRecordBatchStream = Box::pin(
        datafusion_physical_plan::stream::RecordBatchStreamAdapter::new(
            duplicate_pair_schema(),
            stream,
        ),
    );
    let txn = InsertBuilder::new(WriteDestination::Dataset(Arc::new(view_ds)))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute_uncommitted_stream(stream)
        .await?;
    let Operation::Append { fragments } = txn.operation else {
        return Err(invalid(
            "native pair partition did not stage append fragments",
        ));
    };
    Ok(WrittenPairsPartition {
        unit,
        plan_id: plan.identity()?,
        task,
        fragments,
        rows: rows.load(std::sync::atomic::Ordering::Relaxed),
    })
}

pub(super) async fn commit(
    view: &Table,
    plan: &DuplicatePairsRefreshPlan,
    units: Vec<WrittenPairsPartition>,
    expected: Option<&str>,
) -> Result<RefreshMaterializedViewResult> {
    let (_, definition, source) = open_planned(view, plan).await?;
    let config = definition
        .vector_source
        .as_ref()
        .expect("native")
        .config()?;
    if plan_duplicate_pairs(source.clone(), &config).await? != plan.tasks {
        return Err(invalid(
            "pair plan does not cover the pinned index partitions exactly",
        ));
    }
    let identity = plan.identity()?;
    let mut ordered = vec![None; plan.snapshot.units as usize];
    for written in units {
        if written.plan_id != identity
            || plan.tasks.get(written.unit as usize) != Some(&written.task)
        {
            return Err(invalid("pair partition receipt belongs to another plan"));
        }
        let slot = ordered
            .get_mut(written.unit as usize)
            .ok_or_else(|| invalid("unknown pair partition receipt"))?;
        if slot.is_some() {
            return Err(invalid("duplicate pair partition receipt"));
        }
        *slot = Some(written);
    }
    let units = ordered
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| invalid("missing pair partition receipt"))?;
    let rows = units.iter().map(|u| u.rows).sum();
    let fragments = units.into_iter().flat_map(|u| u.fragments).collect();
    super::grouped_units::commit_fragments(
        view,
        &plan.snapshot,
        &definition,
        source.manifest.timestamp_nanos,
        fragments,
        rows,
        expected,
    )
    .await
}
