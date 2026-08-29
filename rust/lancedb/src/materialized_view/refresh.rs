// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Refreshing materialized views.
//!
//! A refresh pins one source version and brings the view to exactly the
//! definition's result at that version. Incremental refresh evaluates the
//! definition over inserted rows and both images of updated rows, consolidates
//! the signed output into a bag delta, and applies that delta atomically.
//! Source deletions rebuild until DatasetDelta can return their before-images.
//!
//! The watermark ([`SOURCE_VERSION_META_KEY`]) lands in a follow-up commit; a
//! crash or race between the two leaves the view visibly unstamped and the
//! next refresh rebuilds. In-process refreshes serialize on a per-view lock;
//! across processes the commit's inserted-rows filter carries a shared token,
//! so two refreshes of one view conflict and only one lands.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{RecordBatch, RecordBatchOptions, UInt64Array};
use arrow_schema::{DataType, Schema as ArrowSchema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::{col, lit};
use futures::{StreamExt, TryStreamExt};
use lance::Dataset;
use lance::dataset::mem_wal::DatasetMemWalExt;
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::write::delete::DeleteBuilder;
use lance::dataset::write::merge_insert::inserted_rows::{FilterType, KeyExistenceFilter};
use lance::dataset::{
    CommitBuilder, InsertBuilder, ProjectionRequest, WriteDestination, WriteMode, WriteParams,
};
use lance_core::{ROW_ID, WILDCARD};
use lance_datafusion::planner::Planner;
use lance_file::version::ConcreteFileVersion;
use lance_table::format::Fragment;
use serde::{Deserialize, Serialize};

use super::{
    DEFINITION_META_KEY, INCARNATION_META_KEY, MaterializedViewDefinition,
    REFRESHED_AT_MS_META_KEY, SOURCE_VERSION_META_KEY, definition_to_metadata,
};
use crate::database::OpenTableRequest;
use crate::table::{NativeTable, NativeTableExt, Table};
use crate::{Error, Result};

/// How a refresh brought the view up to date.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshMode {
    /// The view was recomputed from scratch.
    Rebuild,
    /// A DatasetDelta was evaluated and reconciled against the view.
    Incremental,
    /// The view was already at the requested source version.
    NoOp,
}

/// The result of refreshing a materialized view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshMaterializedViewResult {
    /// How the view was brought up to date.
    pub mode: RefreshMode,
    /// Rows appended to the view: everything on a rebuild, and the net
    /// positive bag-delta rows on an incremental refresh. Rows removed by
    /// deletion vectors are not included.
    pub rows_written: u64,
    /// The source table version the view now reflects.
    pub source_version: u64,
    /// The view table version after the refresh.
    pub version: u64,
}

/// Schema metadata key holding the view table version a successful refresh
/// left behind. Any other commit on the view is drift, and refresh rebuilds.
pub const VIEW_VERSION_META_KEY: &str = "mv.view_version";

/// Schema metadata key holding the commit timestamp of the watermark's source
/// manifest. A dropped and recreated source reuses version numbers but never
/// their timestamps, so a mismatch means the watermark describes a different
/// incarnation and refresh rebuilds.
pub const SOURCE_VERSION_TS_META_KEY: &str = "mv.source_version_ts";

/// One refresh per view at a time within this process.
fn refresh_lock(uri: &str) -> Arc<tokio::sync::Mutex<()>> {
    static LOCKS: OnceLock<StdMutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
        OnceLock::new();
    LOCKS
        .get_or_init(Default::default)
        .lock()
        .expect("refresh lock registry poisoned")
        .entry(uri.to_string())
        .or_default()
        .clone()
}

/// Internal implementation of the refresh logic.
pub(crate) async fn execute_refresh(
    view: &Table,
    full: bool,
    pinned: Option<u64>,
    expected_incarnation: Option<&str>,
) -> Result<RefreshMaterializedViewResult> {
    let view_native = view.as_native().ok_or_else(|| Error::NotSupported {
        message: "materialized views are supported only on local tables".into(),
    })?;
    view_native.dataset.ensure_mutable()?;
    let lock = refresh_lock(view_native.dataset.get().await?.uri());
    let _guard = lock.lock().await;
    // Force-load the latest view state under the lock: each handle caches
    // lazily, and a second handle would otherwise plan from a snapshot taken
    // before another handle's commit -- appending the same rows again or
    // reporting NoOp over a mutated view.
    view_native.dataset.reload().await?;
    let view_ds = view_native.dataset.get().await?.as_ref().clone();

    ensure_incarnation(&view_ds, expected_incarnation, view.name()).await?;

    // The definition a handle cached at open may since have been replaced;
    // what refresh executes and what it stamps must be one generation.
    let definition = match super::materialized_view_kind(&view_ds.schema().metadata)? {
        Some(super::MaterializedViewKind::Select(definition)) => definition,
        Some(super::MaterializedViewKind::Unrecognized { kind }) => {
            return Err(Error::NotSupported {
                message: format!(
                    "materialized view '{}' is defined by '{kind}', which this \
                     version of lancedb cannot refresh",
                    view.name()
                ),
            });
        }
        None => {
            return Err(Error::NotAMaterializedView {
                name: view.name().to_string(),
            });
        }
    };
    let definition = &definition;
    ensure_no_mem_wal(&view_ds, "materialized view", view.name()).await?;

    let source_ds = open_source(view, definition).await?;
    let source_ds = match pinned {
        Some(version) => source_ds.checkout_version(version).await?,
        None => source_ds,
    };
    ensure_no_mem_wal(&source_ds, "source table", &definition.source_table).await?;
    let source_version = source_ds.version().version;
    let source_ts = source_ds.manifest.timestamp_nanos;

    // Re-plan the persisted definition against the current source schema and
    // require its planned output to be exactly the view's physical schema: a
    // definition the stored table cannot represent must not be certified.
    let source_schema = Arc::new(ArrowSchema::from(source_ds.schema()));
    let projections: Vec<(String, String)> = definition
        .projections
        .iter()
        .map(|p| (p.output.clone(), p.expression.clone()))
        .collect();
    validate_inputs(&source_ds, definition)?;
    let (replanned, planned_fields, _renames) = super::plan(
        source_schema,
        &definition.source_table,
        &projections,
        definition.filter.as_deref(),
        definition.limit,
    )?;
    let physical = ArrowSchema::from(view_ds.schema());
    let planned_shape: Vec<_> = planned_fields
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
        .collect();
    let physical_shape: Vec<_> = physical
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
        .collect();
    if planned_shape != physical_shape {
        return Err(Error::Schema {
            message: format!(
                "the stored definition of view '{}' does not produce this \
                 view's schema; recreate the view",
                view.name()
            ),
        });
    }
    let definition_changed =
        definition.filter != replanned.filter || definition.inputs != replanned.inputs;
    let definition = &replanned;

    // A watermark written for a legacy raw filter certifies the rows that
    // filter produced, not the canonical predicate above. Rebuild instead of
    // accepting or advancing it, and persist the migrated definition in the
    // same metadata commit that certifies the replacement rows.
    if definition_changed {
        return rebuild(
            view_native,
            &view_ds,
            &source_ds,
            source_version,
            source_ts,
            definition,
            true,
            expected_incarnation,
        )
        .await;
    }

    let metadata = &view_ds.schema().metadata;
    let watermark: Option<u64> = metadata
        .get(SOURCE_VERSION_META_KEY)
        .and_then(|raw| raw.parse().ok());
    let recorded_ts: Option<u128> = metadata
        .get(SOURCE_VERSION_TS_META_KEY)
        .and_then(|raw| raw.parse().ok());
    // The watermark speaks only for the view state its refresh left behind;
    // any other commit on the view since then is drift.
    let view_intact = metadata
        .get(VIEW_VERSION_META_KEY)
        .and_then(|raw| raw.parse::<u64>().ok())
        == Some(view_ds.version().version);

    if !full && watermark == Some(source_version) && view_intact && recorded_ts == Some(source_ts) {
        return Ok(RefreshMaterializedViewResult {
            mode: RefreshMode::NoOp,
            rows_written: 0,
            source_version,
            version: view_ds.version().version,
        });
    }

    let watermark = watermark.filter(|_| view_intact);
    match incremental_base(&source_ds, source_version, watermark, recorded_ts, full).await {
        Some(base) => {
            let reconciled = incremental(
                view_native,
                &view_ds,
                &source_ds,
                source_version,
                source_ts,
                &base,
                definition,
                expected_incarnation,
            )
            .await?;
            match reconciled {
                Some(result) => Ok(result),
                // The delta was too large to reconcile in bounded memory.
                None => {
                    rebuild(
                        view_native,
                        &view_ds,
                        &source_ds,
                        source_version,
                        source_ts,
                        definition,
                        false,
                        expected_incarnation,
                    )
                    .await
                }
            }
        }
        None => {
            rebuild(
                view_native,
                &view_ds,
                &source_ds,
                source_version,
                source_ts,
                definition,
                false,
                expected_incarnation,
            )
            .await
        }
    }
}

/// Return the source snapshot named by a trustworthy watermark. Incremental
/// evaluation needs it for updated-row before-images.
async fn incremental_base(
    source_ds: &Dataset,
    source_version: u64,
    watermark: Option<u64>,
    recorded_ts: Option<u128>,
    full: bool,
) -> Option<Dataset> {
    if full {
        return None;
    }
    if source_ds.manifest.data_storage_format.lance_file_format() == ConcreteFileVersion::V1 {
        return None;
    }
    let watermark = watermark?;
    if watermark > source_version {
        return None;
    }
    let old = source_ds.checkout_version(watermark).await.ok()?;
    if recorded_ts != Some(old.manifest.timestamp_nanos) {
        return None;
    }
    Some(old)
}

/// Error if a column the view reads no longer exists in the source.
fn validate_inputs(source: &Dataset, definition: &MaterializedViewDefinition) -> Result<()> {
    for input in &definition.inputs {
        if source.schema().field(input).is_none() {
            return Err(Error::Schema {
                message: format!(
                    "source column '{input}' read by the view no longer exists \
                     (dropped or renamed in '{}')",
                    definition.source_table
                ),
            });
        }
    }
    Ok(())
}

/// Reject MemWAL/LSM state on a refresh participant: un-compacted tiers are
/// invisible to DatasetDelta and the rebuild scan. An active write spec and
/// retained rows both disqualify; shard directories on storage are the
/// durable evidence of the latter.
pub(crate) async fn ensure_no_mem_wal(dataset: &Dataset, role: &str, name: &str) -> Result<()> {
    let retained = !dataset.list_mem_wal_latest_shard_ids().await?.is_empty();
    if retained || dataset.mem_wal_index_details().await?.is_some() {
        return Err(Error::NotSupported {
            message: format!(
                "{role} '{name}' has an LSM write spec or retained un-compacted \
                 rows: rows in un-compacted tiers are invisible to refresh"
            ),
        });
    }
    Ok(())
}

async fn open_source(view: &Table, definition: &MaterializedViewDefinition) -> Result<Dataset> {
    let database = view.database_opt().ok_or_else(|| Error::InvalidInput {
        message: "the view was not opened through a database connection".into(),
    })?;
    let source = database
        .open_table(OpenTableRequest {
            name: definition.source_table.clone(),
            namespace_path: Vec::new(),
            index_cache_size: None,
            lance_read_params: None,
            location: None,
            namespace_client: None,
            managed_versioning: None,
        })
        .await?;
    let native = source.as_native().ok_or_else(|| Error::NotSupported {
        message: "materialized views are supported only on local tables".into(),
    })?;
    let dataset = native.dataset.get().await?.as_ref().clone();
    if !dataset.manifest.uses_stable_row_ids() {
        return Err(Error::InvalidInput {
            message: format!(
                "source table '{}' does not have stable row ids; it is not the \
                 table this view was declared over",
                definition.source_table
            ),
        });
    }
    Ok(dataset)
}

#[allow(clippy::too_many_arguments)]
async fn incremental(
    view_native: &NativeTable,
    view_ds: &Dataset,
    source_ds: &Dataset,
    source_version: u64,
    source_ts: u128,
    base: &Dataset,
    definition: &MaterializedViewDefinition,
    expected_incarnation: Option<&str>,
) -> Result<Option<RefreshMaterializedViewResult>> {
    let base_inputs = super::project_schema(&ArrowSchema::from(base.schema()), &definition.inputs);
    let current_inputs =
        super::project_schema(&ArrowSchema::from(source_ds.schema()), &definition.inputs);
    if base_inputs != current_inputs {
        return Ok(None);
    }
    let delta = source_ds
        .delta()
        .with_begin_version(base.version().version)
        .with_end_version(source_version)
        .build()?;
    let version_gap = source_version.saturating_sub(base.version().version);
    if version_gap > MAX_TRANSACTION_WALK {
        return Ok(None);
    }
    let Ok(transactions) = delta.list_transactions().await else {
        return Ok(None);
    };
    if transactions.len() != version_gap as usize
        || transactions
            .iter()
            .any(|transaction| delta_operation_requires_rebuild(&transaction.operation))
    {
        return Ok(None);
    }

    let mut deleted = delta.get_deleted_row_ids().await?;
    while let Some(batch) = deleted.try_next().await? {
        if batch.num_rows() > 0 {
            return Ok(None);
        }
    }

    let mut inserted = delta.get_inserted_rows().await?;
    let mut updated = delta.get_updated_rows().await?;
    if definition.limit.is_some() {
        while let Some(batch) = inserted.try_next().await? {
            if batch.num_rows() > 0 {
                return Ok(None);
            }
        }
        while let Some(batch) = updated.try_next().await? {
            if batch.num_rows() > 0 {
                return Ok(None);
            }
        }
    }

    let evaluator = DeltaEvaluator::new(source_ds, definition, view_ds)?;
    let cap = delta_rebuild_cap();
    let byte_cap = delta_byte_rebuild_cap();
    let mut evaluated_rows = 0usize;
    let mut evaluated_bytes = 0usize;
    let mut weights = HashMap::new();

    while let Some(batch) = inserted.try_next().await? {
        evaluated_rows = evaluated_rows.saturating_add(batch.num_rows());
        if evaluated_rows > cap {
            return Ok(None);
        }
        if !accumulate(
            &mut weights,
            &evaluator.evaluate(&batch)?,
            1,
            &mut evaluated_bytes,
            byte_cap,
        )? {
            return Ok(None);
        }
    }

    let mut updated_ids = Vec::new();
    while let Some(batch) = updated.try_next().await? {
        evaluated_rows = evaluated_rows.saturating_add(batch.num_rows().saturating_mul(2));
        if evaluated_rows > cap {
            return Ok(None);
        }
        updated_ids.extend(row_ids_of(&batch)?.values().iter().copied());
        if !accumulate(
            &mut weights,
            &evaluator.evaluate(&batch)?,
            1,
            &mut evaluated_bytes,
            byte_cap,
        )? {
            return Ok(None);
        }
    }

    let before_columns: Vec<&str> = evaluator
        .read_schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    let before_schema = Arc::new(base.schema().project(&before_columns)?);
    for ids in updated_ids.chunks(8192) {
        let before = base
            .take_rows(ids, ProjectionRequest::Schema(before_schema.clone()))
            .await?;
        if !accumulate(
            &mut weights,
            &evaluator.evaluate(&before)?,
            -1,
            &mut evaluated_bytes,
            byte_cap,
        )? {
            return Ok(None);
        }
    }

    weights.retain(|_, weight| *weight != 0);
    let mut removals: HashMap<Vec<ScalarValue>, usize> = weights
        .iter()
        .filter_map(|(row, weight)| (*weight < 0).then_some((row.clone(), (-*weight) as usize)))
        .collect();
    if !removals.is_empty() {
        let view_rows = view_ds.count_rows(None).await?;
        let schema = ArrowSchema::from(view_ds.schema());
        if view_reconciliation_exceeds_budget(&schema, view_rows) {
            return Ok(None);
        }
    }
    let Some(evicted_ids) = select_view_rows(view_ds, &mut removals).await? else {
        return Ok(None);
    };
    let mut eviction = Eviction::new(view_ds, EVICTION_CHUNK);
    eviction.push_slice(&evicted_ids).await?;
    let eviction = eviction.finish().await?;
    let additions = positive_batch(&weights, Arc::new(ArrowSchema::from(view_ds.schema())))?;
    let rows_written = additions
        .as_ref()
        .map_or(0, |batch| batch.num_rows() as u64);

    let mut result = RefreshMaterializedViewResult {
        mode: RefreshMode::Incremental,
        rows_written,
        source_version,
        version: view_ds.version().version,
    };
    if additions.is_none() && eviction.is_none() {
        result.version = stamp_watermark(
            view_native,
            view_ds.clone(),
            source_version,
            source_ts,
            None,
            expected_incarnation,
        )
        .await?;
        return Ok(Some(result));
    }
    let new_fragments = stage_additions(view_ds, additions).await?;
    let appended = publish(
        view_ds,
        eviction,
        new_fragments,
        Some(refresh_filter()),
        expected_incarnation,
    )
    .await?;
    result.version = stamp_watermark(
        view_native,
        appended,
        source_version,
        source_ts,
        None,
        expected_incarnation,
    )
    .await?;
    Ok(Some(result))
}

fn delta_operation_requires_rebuild(operation: &Operation) -> bool {
    match operation {
        Operation::Append { .. }
        | Operation::CreateIndex { .. }
        | Operation::Delete { .. }
        | Operation::Merge { .. }
        | Operation::Project { .. }
        | Operation::ReserveFragments { .. }
        | Operation::Rewrite { .. }
        | Operation::Update { .. }
        | Operation::UpdateBases { .. }
        | Operation::UpdateConfig { .. }
        | Operation::UpdateMemWalState { .. } => false,
        Operation::Clone { .. }
        | Operation::DataOverlay { .. }
        | Operation::DataReplacement { .. }
        | Operation::Overwrite { .. }
        | Operation::Restore { .. } => true,
    }
}

struct DeltaEvaluator {
    read_schema: SchemaRef,
    output_schema: SchemaRef,
    filter: Option<Arc<dyn PhysicalExpr>>,
    projections: Vec<Arc<dyn PhysicalExpr>>,
}

impl DeltaEvaluator {
    fn new(
        source: &Dataset,
        definition: &MaterializedViewDefinition,
        view: &Dataset,
    ) -> Result<Self> {
        let source_schema = Arc::new(ArrowSchema::from(source.schema()));
        let read_schema = super::project_schema(&source_schema, &definition.inputs);
        let source_planner = Planner::new(source_schema);
        let read_planner = Planner::new(read_schema.clone());
        let bind = |expression: &str, filter: bool| -> Result<Arc<dyn PhysicalExpr>> {
            let logical = if filter {
                source_planner.parse_filter(expression)
            } else {
                source_planner.parse_expr(expression)
            }
            .and_then(|expr| source_planner.optimize_expr(expr))
            .map_err(|error| Error::Runtime {
                message: format!("failed to plan materialized-view delta expression: {error}"),
            })?;
            read_planner
                .create_physical_expr(&logical)
                .map_err(|error| Error::Runtime {
                    message: format!("failed to bind materialized-view delta expression: {error}"),
                })
        };
        let filter = definition
            .filter
            .as_deref()
            .map(|expression| bind(expression, true))
            .transpose()?;
        let projections = definition
            .projections
            .iter()
            .map(|projection| bind(&projection.expression, false))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            read_schema,
            output_schema: Arc::new(ArrowSchema::from(view.schema())),
            filter,
            projections,
        })
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let columns = self
            .read_schema
            .fields()
            .iter()
            .map(|field| {
                batch
                    .column_by_name(field.name())
                    .cloned()
                    .ok_or_else(|| Error::Runtime {
                        message: format!(
                            "source delta is missing materialized-view input '{}'",
                            field.name()
                        ),
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut input = RecordBatch::try_new_with_options(
            self.read_schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?;
        if let Some(filter) = &self.filter {
            let mask = filter
                .evaluate(&input)
                .and_then(|value| value.into_array(input.num_rows()))
                .map_err(|error| Error::Runtime {
                    message: format!("failed to evaluate materialized-view delta filter: {error}"),
                })?;
            let mask = mask.as_boolean_opt().ok_or_else(|| Error::Runtime {
                message: "materialized-view delta filter did not produce booleans".into(),
            })?;
            input = arrow_select::filter::filter_record_batch(&input, mask)?;
        }
        let columns = self
            .projections
            .iter()
            .map(|projection| {
                projection
                    .evaluate(&input)
                    .and_then(|value| value.into_array(input.num_rows()))
                    .map_err(|error| Error::Runtime {
                        message: format!(
                            "failed to evaluate materialized-view delta projection: {error}"
                        ),
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new_with_options(
            self.output_schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(input.num_rows())),
        )?)
    }
}

fn accumulate(
    weights: &mut HashMap<Vec<ScalarValue>, i64>,
    batch: &RecordBatch,
    sign: i64,
    evaluated_bytes: &mut usize,
    byte_cap: usize,
) -> Result<bool> {
    for row in 0..batch.num_rows() {
        let key = batch
            .columns()
            .iter()
            .map(|column| {
                ScalarValue::try_from_array(column.as_ref(), row).map_err(|error| Error::Runtime {
                    message: format!("failed to encode materialized-view delta row: {error}"),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        *evaluated_bytes = (*evaluated_bytes).saturating_add(ScalarValue::size_of_vec(&key));
        if *evaluated_bytes > byte_cap {
            return Ok(false);
        }
        *weights.entry(key).or_default() += sign;
    }
    Ok(true)
}

fn positive_batch(
    weights: &HashMap<Vec<ScalarValue>, i64>,
    schema: SchemaRef,
) -> Result<Option<RecordBatch>> {
    let rows: Vec<&Vec<ScalarValue>> = weights
        .iter()
        .flat_map(|(row, weight)| std::iter::repeat_n(row, (*weight).max(0) as usize))
        .collect();
    if rows.is_empty() {
        return Ok(None);
    }
    let columns = (0..schema.fields().len())
        .map(|column| {
            ScalarValue::iter_to_array(rows.iter().map(|row| row[column].clone())).map_err(
                |error| Error::Runtime {
                    message: format!("failed to materialize positive view delta: {error}"),
                },
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(RecordBatch::try_new(schema, columns)?))
}

async fn select_view_rows(
    view: &Dataset,
    needed: &mut HashMap<Vec<ScalarValue>, usize>,
) -> Result<Option<Vec<u64>>> {
    if needed.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let schema = ArrowSchema::from(view.schema());
    let mut scanner = view.scan();
    scanner.with_row_id().project(&[WILDCARD, ROW_ID])?;
    let mut stream = scanner.try_into_stream().await?;
    let mut selected = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        let row_ids = row_ids_of(&batch)?;
        for row in 0..batch.num_rows() {
            let key = schema
                .fields()
                .iter()
                .map(|field| {
                    let column =
                        batch
                            .column_by_name(field.name())
                            .ok_or_else(|| Error::Runtime {
                                message: format!("view scan is missing output '{}'", field.name()),
                            })?;
                    ScalarValue::try_from_array(column.as_ref(), row).map_err(|error| {
                        Error::Runtime {
                            message: format!("failed to encode materialized-view row: {error}"),
                        }
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            if let Some(count) = needed.get_mut(&key)
                && *count > 0
            {
                selected.push(row_ids.value(row));
                *count -= 1;
            }
        }
        needed.retain(|_, count| *count > 0);
        if needed.is_empty() {
            return Ok(Some(selected));
        }
    }
    Ok(None)
}

async fn stage_additions(view: &Dataset, batch: Option<RecordBatch>) -> Result<Vec<Fragment>> {
    let Some(batch) = batch else {
        return Ok(Vec::new());
    };
    let schema = batch.schema();
    let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter([Ok(batch)]),
    ));
    let transaction = InsertBuilder::new(WriteDestination::Dataset(Arc::new(view.clone())))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute_uncommitted_stream(stream)
        .await?;
    let Operation::Append { fragments } = transaction.operation else {
        return Err(Error::Runtime {
            message: "expected an append when staging the view's delta rows".into(),
        });
    };
    Ok(fragments)
}

#[allow(clippy::too_many_arguments)]
async fn rebuild(
    view_native: &NativeTable,
    view_ds: &Dataset,
    source_ds: &Dataset,
    source_version: u64,
    source_ts: u128,
    definition: &MaterializedViewDefinition,
    persist_definition: bool,
    expected_incarnation: Option<&str>,
) -> Result<RefreshMaterializedViewResult> {
    let rows_written = Arc::new(AtomicU64::new(0));
    let schema = Arc::new(ArrowSchema::from(view_ds.schema()));
    let stream = compute_stream(
        source_ds,
        definition,
        RowScope {
            limit: definition.limit,
        },
        schema,
        rows_written.clone(),
    )
    .await?;
    // Every rebuild is one fragment swap, indexed or not: an Update commit
    // carries no schema metadata, so it cannot erase a definition update
    // that raced in the way an overwrite (which adopts its stream's schema)
    // durably would -- and it must land on the planned generation or abort.
    let replaced = replace_retaining_indices(view_ds.clone(), stream, expected_incarnation).await?;
    let version = stamp_watermark(
        view_native,
        replaced,
        source_version,
        source_ts,
        persist_definition.then_some(definition),
        expected_incarnation,
    )
    .await?;
    Ok(RefreshMaterializedViewResult {
        mode: RefreshMode::Rebuild,
        rows_written: rows_written.load(Ordering::Relaxed),
        source_version,
        version,
    })
}

/// Replace all of the view's data in one commit that retains its index
/// definitions: new fragments staged uncommitted, one `Update` removing every
/// old fragment. `Update` prunes index bitmaps only for modified fields and
/// none are modified here, so readers never see the view unindexed or empty.
async fn replace_retaining_indices(
    view_ds: Dataset,
    stream: SendableRecordBatchStream,
    expected_incarnation: Option<&str>,
) -> Result<Dataset> {
    let ds = Arc::new(view_ds);
    let read_version = ds.version().version;
    #[cfg(test)]
    tests::hold_before_publish(ds.uri()).await;
    ensure_incarnation(&ds, expected_incarnation, ds.uri()).await?;
    let removed_fragment_ids: Vec<u64> = ds.get_fragments().iter().map(|f| f.id() as u64).collect();

    let write_txn = InsertBuilder::new(WriteDestination::Dataset(ds.clone()))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute_uncommitted_stream(stream)
        .await?;
    let Operation::Append {
        fragments: new_fragments,
    } = write_txn.operation
    else {
        return Err(Error::Runtime {
            message: "expected an append when staging the view's replacement rows".into(),
        });
    };

    let transaction = Transaction::new(
        read_version,
        Operation::Update {
            removed_fragment_ids,
            updated_fragments: Vec::new(),
            new_fragments,
            fields_modified: Vec::new(),
            compacted_sstables: Vec::new(),
            fields_for_preserving_frag_bitmap: Vec::new(),
            update_mode: None,
            // Two refreshes that materialized the same source rows must not
            // both land -- a raced first rebuild would double the view.
            inserted_rows_filter: Some(refresh_filter()),
            updated_fragment_offsets: None,
        },
        None,
    );
    let committed = CommitBuilder::new(WriteDestination::Dataset(ds))
        .execute(transaction)
        .await?;
    if committed.version().version != read_version + 1 {
        return Err(Error::Runtime {
            message: format!(
                "a concurrent commit raced this refresh (view version {}); the 
                 refresh is unrecorded and the next one will rebuild",
                committed.version().version
            ),
        });
    }
    Ok(committed)
}

/// Record that the view now reflects `source_version`, including the view
/// Refuse to act on a view that is not `expected`'s incarnation, judged from
/// the latest stored manifest. Not a commit condition; see
/// `RefreshMaterializedViewBuilder::expect_incarnation`.
async fn ensure_incarnation(view_ds: &Dataset, expected: Option<&str>, what: &str) -> Result<()> {
    let Some(expected) = expected else {
        return Ok(());
    };
    let mut latest = view_ds.clone();
    latest.checkout_latest().await?;
    match latest.schema().metadata.get(INCARNATION_META_KEY) {
        Some(actual) if actual == expected => Ok(()),
        Some(_) => Err(Error::Runtime {
            message: format!(
                "materialized view '{what}' is not the incarnation this refresh was \
                 requested for: it was dropped and recreated"
            ),
        }),
        None => Err(Error::Runtime {
            message: format!(
                "materialized view '{what}' carries no incarnation token: its schema \
                 metadata was replaced since the token was captured"
            ),
        }),
    }
}

/// version this very commit produces. The version is predicted and then
/// verified; on a mismatch another commit raced in between, and the stamp
/// ABORTS rather than certify that commit as the refresh's own generation.
/// The view is left visibly unstamped, so the next refresh rebuilds.
async fn stamp_watermark(
    view_native: &NativeTable,
    mut dataset: Dataset,
    source_version: u64,
    source_ts: u128,
    definition: Option<&MaterializedViewDefinition>,
    expected_incarnation: Option<&str>,
) -> Result<u64> {
    ensure_incarnation(&dataset, expected_incarnation, dataset.uri()).await?;
    let predicted = dataset.version().version + 1;
    // A view with no token (declared before tokens existed, or its metadata
    // replaced wholesale) starts a new incarnation here.
    let incarnation = dataset
        .schema()
        .metadata
        .get(INCARNATION_META_KEY)
        .cloned()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let mut metadata = vec![(INCARNATION_META_KEY.to_string(), Some(incarnation))];
    if let Some(definition) = definition {
        metadata.push((
            DEFINITION_META_KEY.to_string(),
            Some(definition_to_metadata(definition)?),
        ));
    }
    metadata.extend([
        (
            SOURCE_VERSION_META_KEY.to_string(),
            Some(source_version.to_string()),
        ),
        (
            SOURCE_VERSION_TS_META_KEY.to_string(),
            Some(source_ts.to_string()),
        ),
        (
            REFRESHED_AT_MS_META_KEY.to_string(),
            Some(now_ms().to_string()),
        ),
        (
            VIEW_VERSION_META_KEY.to_string(),
            Some(predicted.to_string()),
        ),
    ]);
    dataset.update_schema_metadata(metadata).await?;
    let actual = dataset.version().version;
    if actual != predicted {
        return Err(Error::Runtime {
            message: format!(
                "a concurrent commit raced this refresh (view version {actual}, \
                 expected {predicted}); the refresh is unrecorded and the next \
                 one will rebuild"
            ),
        });
    }
    view_native.dataset.update(dataset);
    Ok(predicted)
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Evaluate the definition over the source as batches in the view's schema.
#[derive(Default)]
struct RowScope {
    /// Stop after this many rows.
    limit: Option<u64>,
}

async fn compute_stream(
    source: &Dataset,
    definition: &MaterializedViewDefinition,
    scope: RowScope,
    schema: SchemaRef,
    rows_written: Arc<AtomicU64>,
) -> Result<SendableRecordBatchStream> {
    let RowScope { limit } = scope;
    let mut scanner = source.scan();
    if let Some(filter) = &definition.filter {
        scanner.filter(filter)?;
    }
    let transforms: Vec<(&str, &str)> = definition
        .projections
        .iter()
        .map(|p| (p.output.as_str(), p.expression.as_str()))
        .collect();
    scanner.project_with_transform(&transforms)?;
    // A scan reads a limit of zero as no limit at all, so a view capped at
    // nothing is answered without one.
    if limit == Some(0) {
        return Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::empty(),
        )));
    }
    if let Some(limit) = limit {
        let limit = i64::try_from(limit).map_err(|_| Error::InvalidInput {
            message: format!("view limit {limit} exceeds the maximum of {}", i64::MAX),
        })?;
        scanner.limit(Some(limit), None)?;
    }

    let out_schema = schema.clone();
    let mapped = scanner.try_into_stream().await?.map(move |batch| {
        let batch = batch.map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut columns = Vec::with_capacity(out_schema.fields().len());
        for field in out_schema.fields() {
            let column = batch.column_by_name(field.name()).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "view column '{}' is not produced by the view's definition",
                    field.name()
                ))
            })?;
            columns.push(column.clone());
        }
        rows_written.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        Ok(RecordBatch::try_new(out_schema.clone(), columns)?)
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mapped)))
}

/// Commit the view's removals and additions as one change, on the exact
/// generation the refresh planned from. A shared refresh token rejects a
/// concurrent refresh; the generation check catches every other view write.
async fn publish(
    view_ds: &Dataset,
    eviction: Option<(Vec<Fragment>, Vec<u64>)>,
    new_fragments: Vec<Fragment>,
    keys: Option<KeyExistenceFilter>,
    expected_incarnation: Option<&str>,
) -> Result<Dataset> {
    let planned = view_ds.version().version;
    #[cfg(test)]
    tests::hold_before_publish(view_ds.uri()).await;
    #[cfg(test)]
    tests::hold_until_peers_planned();
    ensure_incarnation(view_ds, expected_incarnation, view_ds.uri()).await?;
    let (updated_fragments, removed_fragment_ids) = eviction.unwrap_or_default();
    let committed = CommitBuilder::new(WriteDestination::Dataset(Arc::new(view_ds.clone())))
        .execute(Transaction::new(
            planned,
            Operation::Update {
                removed_fragment_ids,
                updated_fragments,
                new_fragments,
                fields_modified: Vec::new(),
                compacted_sstables: Vec::new(),
                fields_for_preserving_frag_bitmap: Vec::new(),
                update_mode: None,
                inserted_rows_filter: keys,
                updated_fragment_offsets: None,
            },
            None,
        ))
        .await?;
    if committed.version().version != planned + 1 {
        return Err(Error::Runtime {
            message: format!(
                "a concurrent commit raced this refresh (view version {}); 
                 the refresh is unrecorded and the next one will rebuild",
                committed.version().version
            ),
        });
    }
    Ok(committed)
}

/// A delta batch's row id column, borrowed: a delta batch is as large as one
/// fragment's deletions, so copying it out would be the unbounded step the
/// chunking exists to avoid.
fn row_ids_of(batch: &RecordBatch) -> Result<&UInt64Array> {
    let column = batch.column_by_name(ROW_ID).ok_or_else(|| Error::Runtime {
        message: format!("'{ROW_ID}' is missing from a delta batch"),
    })?;
    column
        .as_primitive_opt::<UInt64Type>()
        .ok_or_else(|| Error::Runtime {
            message: "row ids are not UInt64".into(),
        })
}

/// A shared token carried by every refresh commit so concurrent refreshes
/// conflict even though the view has no physical reconciliation key.
const REFRESH_TOKEN_ID: u64 = u64::MAX;

fn refresh_filter() -> KeyExistenceFilter {
    KeyExistenceFilter {
        field_ids: Vec::new(),
        filter: FilterType::ExactSet(HashSet::from([REFRESH_TOKEN_ID])),
    }
}

/// Delta rows past this fall back to the streamed rebuild, whose memory
/// does not grow with the delta.
fn delta_rebuild_cap() -> usize {
    #[cfg(test)]
    if let Some(cap) = tests::delta_cap_override() {
        return cap;
    }
    4 * 1024 * 1024
}

/// Version gap beyond which reading every transaction is more expensive than
/// rebuilding and more likely to encounter cleaned history.
const MAX_TRANSACTION_WALK: u64 = 512;

/// Bound the encoded output values held while consolidating a delta. The
/// positive batch and hash table temporarily duplicate some of this memory.
fn delta_byte_rebuild_cap() -> usize {
    128 * 1024 * 1024
}

/// Exact bag subtraction decodes and hashes view values row-wise. Past this
/// estimated decoded-value budget the columnar rebuild is the bounded path.
const VIEW_RECONCILIATION_BYTE_CAP: usize = 128 * 1024 * 1024;

fn view_reconciliation_exceeds_budget(schema: &ArrowSchema, rows: usize) -> bool {
    let row_bytes = schema.fields().iter().fold(0usize, |total, field| {
        total.saturating_add(
            std::mem::size_of::<ScalarValue>()
                .saturating_add(estimated_value_width(field.data_type())),
        )
    });
    rows.saturating_mul(row_bytes) > VIEW_RECONCILIATION_BYTE_CAP
}

fn estimated_value_width(data_type: &DataType) -> usize {
    if let Some(width) = data_type.primitive_width() {
        return width;
    }
    match data_type {
        DataType::Null => 0,
        DataType::Boolean => 1,
        DataType::FixedSizeBinary(width) => (*width).max(0) as usize,
        DataType::FixedSizeList(child, len) => {
            ((*len).max(0) as usize).saturating_mul(estimated_value_width(child.data_type()))
        }
        DataType::Struct(fields) => fields.iter().fold(0usize, |total, field| {
            total.saturating_add(estimated_value_width(field.data_type()))
        }),
        DataType::Union(fields, _) => fields
            .iter()
            .map(|(_, field)| estimated_value_width(field.data_type()))
            .max()
            .unwrap_or_default(),
        DataType::Dictionary(_, values) => estimated_value_width(values),
        DataType::RunEndEncoded(_, values) => estimated_value_width(values.data_type()),
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::List(_)
        | DataType::ListView(_)
        | DataType::LargeList(_)
        | DataType::LargeListView(_)
        | DataType::Map(_, _) => 256,
        _ => 64,
    }
}

/// View row ids per staged eviction: the delete predicate carries one
/// literal per id, so a whole delta at once is unbounded. Each chunk costs a
/// pass over the view's virtual row id, which is why it is not smaller.
const EVICTION_CHUNK: usize = 64 * 1024;

/// Accumulates the view's removals from bounded chunks of virtual row ids into
/// the one set of fragment changes the refresh publishes.
struct Eviction {
    /// The view as the chunks staged so far leave it. A chunk's delete has to
    /// see the deletion vectors the earlier ones wrote, or it stages a
    /// fragment that drops them.
    snapshot: Dataset,
    chunk: usize,
    updated: HashMap<u64, Fragment>,
    removed: Vec<u64>,
    pending: Vec<u64>,
    staged: bool,
    /// Largest the buffer ever got, which is the bound under test.
    #[cfg(test)]
    peak: usize,
}

impl Eviction {
    fn new(view_ds: &Dataset, chunk: usize) -> Self {
        Self {
            snapshot: view_ds.clone(),
            chunk,
            updated: HashMap::new(),
            removed: Vec::new(),
            pending: Vec::with_capacity(chunk),
            staged: false,
            #[cfg(test)]
            peak: 0,
        }
    }

    /// Fill a chunk at a time. Taking the batch whole and splitting it would
    /// hold a fragment's worth of ids and recopy the tail per chunk.
    #[cfg(test)]
    async fn push(&mut self, ids: &UInt64Array) -> Result<()> {
        self.push_slice(ids.values()).await
    }

    async fn push_slice(&mut self, ids: &[u64]) -> Result<()> {
        for id in ids {
            self.pending.push(*id);
            #[cfg(test)]
            {
                self.peak = self.peak.max(self.pending.len());
            }
            if self.pending.len() == self.chunk {
                self.flush().await?;
            }
        }
        Ok(())
    }

    /// Stage what is buffered, keeping the buffer's allocation.
    async fn flush(&mut self) -> Result<()> {
        let mut chunk = std::mem::take(&mut self.pending);
        let staged = self.stage(&chunk).await;
        chunk.clear();
        self.pending = chunk;
        staged
    }

    /// The staged fragment changes, or `None` where nothing was evicted.
    async fn finish(mut self) -> Result<Option<(Vec<Fragment>, Vec<u64>)>> {
        if !self.pending.is_empty() {
            self.flush().await?;
        }
        if !self.staged {
            return Ok(None);
        }
        let mut updated: Vec<Fragment> = self.updated.into_values().collect();
        updated.sort_unstable_by_key(|f| f.id);
        Ok(Some((updated, self.removed)))
    }

    async fn stage(&mut self, ids: &[u64]) -> Result<()> {
        let (updated, removed) = stage_eviction(&self.snapshot, ids).await?;
        self.snapshot = advance(&self.snapshot, &updated);
        for fragment in updated {
            self.updated.insert(fragment.id, fragment);
        }
        self.removed.extend(removed);
        self.staged = true;
        Ok(())
    }
}

/// The view as staged removals leave it, without committing them. Fragments
/// are replaced in place so the fragment bitmap stays in step; an emptied one
/// is left alone, since the selected virtual row ids are unique.
fn advance(view_ds: &Dataset, updated: &[Fragment]) -> Dataset {
    if updated.is_empty() {
        return view_ds.clone();
    }
    let by_id: HashMap<u64, &Fragment> = updated.iter().map(|f| (f.id, f)).collect();
    let fragments = view_ds
        .manifest
        .fragments
        .iter()
        .map(|f| by_id.get(&f.id).map_or_else(|| f.clone(), |u| (*u).clone()))
        .collect();
    let mut manifest = view_ds.manifest.as_ref().clone();
    manifest.fragments = Arc::new(fragments);
    let mut snapshot = view_ds.clone();
    snapshot.manifest = Arc::new(manifest);
    snapshot
}

/// The fragment changes that remove the view's rows for `ids`, staged rather
/// than committed so they can ride in the refresh's single data commit.
async fn stage_eviction(view_ds: &Dataset, ids: &[u64]) -> Result<(Vec<Fragment>, Vec<u64>)> {
    // An expression rather than SQL text: the id list is a value here, not a
    // predicate string that grows with the delta and has to be parsed.
    let predicate = col(ROW_ID).in_list(
        ids.iter()
            .map(|id| lit(ScalarValue::UInt64(Some(*id))))
            .collect(),
        false,
    );
    let staged = DeleteBuilder::from_expr(Arc::new(view_ds.clone()), predicate)
        .execute_uncommitted()
        .await?;
    let Operation::Delete {
        updated_fragments,
        deleted_fragment_ids,
        ..
    } = staged.transaction.operation
    else {
        return Err(Error::Runtime {
            message: "expected a delete when staging the view's evictions".into(),
        });
    };
    Ok((updated_fragments, deleted_fragment_ids))
}

#[cfg(test)]
mod tests {

    /// Park a refresh between planning and publication so a test can move
    /// the view underneath it. Inert unless [`DRIFT_TARGET`] names this view.
    pub(super) async fn hold_before_publish(uri: &str) {
        {
            let mut target = DRIFT_TARGET.lock().unwrap();
            if target.as_deref() != Some(uri) {
                return;
            }
            // Take it: memory:// uris are relative and repeat across tests, so
            // leaving it armed would park an unrelated refresh forever.
            *target = None;
        }
        DRIFT_PLANNED.notify_one();
        DRIFT_RELEASED.notified().await;
    }

    /// The rendezvous below is one global pair, so the cases that use it run
    /// one at a time rather than trading each other's signals.
    pub(super) static DELTA_CAP: StdMutex<Option<usize>> = StdMutex::new(None);
    pub(super) fn delta_cap_override() -> Option<usize> {
        *DELTA_CAP.lock().unwrap()
    }

    pub(super) static DRIFT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    pub(super) static DRIFT_TARGET: StdMutex<Option<String>> = StdMutex::new(None);
    pub(super) static DRIFT_PLANNED: tokio::sync::Notify = tokio::sync::Notify::const_new();
    pub(super) static DRIFT_RELEASED: tokio::sync::Notify = tokio::sync::Notify::const_new();

    /// Block until every participant in a cross-process race has planned and
    /// staged its write, so the commits they then attempt genuinely contend
    /// rather than depending on the scheduler to overlap them. Inert unless
    /// `MV_RACE_SYNC` names a directory shared by the participants.
    pub(super) fn hold_until_peers_planned() {
        let (Ok(dir), Ok(tag), Ok(peers)) = (
            std::env::var("MV_RACE_SYNC"),
            std::env::var("MV_RACE_TAG"),
            std::env::var("MV_RACE_PEERS"),
        ) else {
            return;
        };
        let dir = std::path::PathBuf::from(dir);
        let peers: usize = peers.parse().unwrap();
        std::fs::write(dir.join(format!("planned-{tag}")), b"1").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        while planned_count(&dir) < peers {
            assert!(
                std::time::Instant::now() < deadline,
                "peers never reached the commit boundary"
            );
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }

    fn planned_count(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .filter(|e| e.file_name().to_string_lossy().starts_with("planned-"))
                    .count()
            })
            .unwrap_or(0)
    }
    use arrow_array::{Float64Array, Int32Array, record_batch};
    use futures::TryStreamExt;
    use lance::dataset::NewColumnTransform;
    use lance_file::version::LanceFileVersion;

    use super::*;
    use crate::connect;
    use crate::connection::Connection;
    use crate::index::Index;
    use crate::index::scalar::BTreeIndexBuilder;
    use crate::materialized_view::MaterializedView;
    use crate::query::{ExecutableQuery, QueryBase, Select};
    use crate::table::{CompactionOptions, OptimizeAction};

    async fn db_with_source(values: Vec<i32>) -> (Connection, Table) {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("x", Int32, values)).unwrap();
        let table = conn
            .create_table("src", batch)
            .write_options(crate::materialized_view::tests::stable_row_ids())
            .execute()
            .await
            .unwrap();
        (conn, table)
    }

    async fn doubled_view(conn: &Connection) -> MaterializedView {
        conn.create_materialized_view("doubled", "src")
            .select([("x", "x"), ("twice", "x * 2")])
            .execute()
            .await
            .unwrap()
    }

    /// A refreshed doubled view over a source holding `values`.
    async fn refreshed_doubled(values: Vec<i32>) -> (Connection, Table, MaterializedView) {
        let (conn, source) = db_with_source(values).await;
        let view = doubled_view(&conn).await;
        view.refresh().execute().await.unwrap();
        (conn, source, view)
    }

    async fn read(table: &Table, column: &str) -> Vec<i32> {
        let batches = table
            .query()
            .select(Select::columns(&[column]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut values: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                batch[column]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .flatten()
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

    #[test]
    fn operations_invisible_to_row_deltas_require_rebuild() {
        assert!(delta_operation_requires_rebuild(&Operation::DataOverlay {
            groups: Vec::new(),
        }));
        assert!(delta_operation_requires_rebuild(&Operation::Restore {
            version: 1,
        }));
        assert!(!delta_operation_requires_rebuild(&Operation::Append {
            fragments: Vec::new(),
        }));
    }

    #[test]
    fn bag_delta_respects_the_byte_budget() {
        let batch = record_batch!(("x", Int32, [1])).unwrap();
        let mut weights = HashMap::new();
        let mut evaluated_bytes = 0;

        assert!(!accumulate(&mut weights, &batch, 1, &mut evaluated_bytes, 1,).unwrap());
    }

    #[test]
    fn wide_views_use_the_columnar_rebuild_path() {
        let item = Arc::new(arrow_schema::Field::new("item", DataType::Float32, true));
        let wide = ArrowSchema::new(vec![arrow_schema::Field::new(
            "vector",
            DataType::FixedSizeList(item, 768),
            true,
        )]);
        let narrow = ArrowSchema::new(vec![arrow_schema::Field::new(
            "value",
            DataType::Int32,
            true,
        )]);

        assert!(view_reconciliation_exceeds_budget(&wide, 1_000_000));
        assert!(!view_reconciliation_exceeds_budget(&narrow, 1_000));
    }

    #[tokio::test]
    async fn test_first_refresh_materializes_the_view() {
        let (conn, _) = db_with_source(vec![1, 2, 3]).await;
        let view = doubled_view(&conn).await;

        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        assert_eq!(result.rows_written, 3);
        assert_eq!(read(view.table(), "twice").await, vec![2, 4, 6]);

        // The watermark survives on the stored schema, not just the handle.
        let reopened = conn.open_materialized_view("doubled").await.unwrap();
        let again = reopened.refresh().execute().await.unwrap();
        assert_eq!(again.mode, RefreshMode::NoOp);
        assert_eq!(again.rows_written, 0);
    }

    #[tokio::test]
    async fn test_filter_selects_the_source_rows() {
        let (conn, _) = db_with_source(vec![1, 20, 3, 40]).await;
        let view = conn
            .create_materialized_view("big", "src")
            .select([("x", "x")])
            .only_if("x > 10")
            .execute()
            .await
            .unwrap();

        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.rows_written, 2);
        assert_eq!(read(view.table(), "x").await, vec![20, 40]);
    }

    #[tokio::test]
    async fn test_mixed_case_filter_is_canonicalized_for_lineage_and_refresh() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(
            ("id", Int32, [1, 2, 3]),
            ("PartyAbbrev", Utf8, ["D", "R", "D"])
        )
        .unwrap();
        conn.create_table("src", batch)
            .write_options(crate::materialized_view::tests::stable_row_ids())
            .execute()
            .await
            .unwrap();
        conn.create_materialized_view("democrats", "src")
            .select([("id", "id")])
            .only_if(r#""PartyAbbrev" = 'D'"#)
            .execute()
            .await
            .unwrap();

        // Reopen from schema metadata so these assertions cover the stored
        // predicate and lineage, not only the declaration-time handle.
        let view = conn.open_materialized_view("democrats").await.unwrap();
        assert_eq!(
            view.definition().filter.as_deref(),
            Some("`PartyAbbrev` = 'D'")
        );
        assert_eq!(view.definition().inputs, ["PartyAbbrev", "id"]);

        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.rows_written, 2);
        assert_eq!(read(view.table(), "id").await, vec![1, 3]);
    }

    #[tokio::test]
    async fn test_legacy_raw_filter_rebuilds_and_persists_canonical_definition() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(
            ("id", Int32, [1, 2, 3]),
            ("PartyAbbrev", Utf8, ["D", "R", "D"])
        )
        .unwrap();
        conn.create_table("legacy_src", batch)
            .write_options(crate::materialized_view::tests::stable_row_ids())
            .execute()
            .await
            .unwrap();
        let view = conn
            .create_materialized_view("legacy_view", "legacy_src")
            .select([("id", "id")])
            .only_if(r#""PartyAbbrev" = 'X'"#)
            .execute()
            .await
            .unwrap();
        assert_eq!(view.refresh().execute().await.unwrap().rows_written, 0);

        // Model a definition and up-to-date watermark written before filter
        // canonicalization was applied to materialized views.
        let mut legacy = view.definition().clone();
        legacy.filter = Some(r#""PartyAbbrev" = 'D'"#.into());
        legacy.inputs = vec!["id".into()];
        let native = view.table().as_native().unwrap();
        let mut dataset = native.dataset.get().await.unwrap().as_ref().clone();
        let predicted = dataset.version().version + 1;
        dataset
            .update_schema_metadata([
                (
                    DEFINITION_META_KEY.to_string(),
                    Some(definition_to_metadata(&legacy).unwrap()),
                ),
                (
                    VIEW_VERSION_META_KEY.to_string(),
                    Some(predicted.to_string()),
                ),
            ])
            .await
            .unwrap();
        native.dataset.update(dataset);

        let reopened = conn.open_materialized_view("legacy_view").await.unwrap();
        let result = reopened.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        assert_eq!(result.rows_written, 2);
        assert_eq!(read(reopened.table(), "id").await, vec![1, 3]);

        // A fresh handle proves the migration was stored alongside the new
        // watermark and therefore happens only once.
        let migrated = conn.open_materialized_view("legacy_view").await.unwrap();
        assert_eq!(
            migrated.definition().filter.as_deref(),
            Some("`PartyAbbrev` = 'D'")
        );
        assert_eq!(migrated.definition().inputs, ["PartyAbbrev", "id"]);
        assert_eq!(
            migrated.refresh().execute().await.unwrap().mode,
            RefreshMode::NoOp
        );
        assert_eq!(read(migrated.table(), "id").await, vec![1, 3]);
    }

    #[tokio::test]
    async fn test_append_refreshes_incrementally() {
        let (_conn, source, view) = refreshed_doubled(vec![1, 2]).await;

        append(&source, vec![5]).await;
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Incremental);
        assert_eq!(result.rows_written, 1);
        assert_eq!(read(view.table(), "twice").await, vec![2, 4, 10]);
    }

    #[tokio::test]
    async fn test_incremental_applies_the_filter() {
        let (conn, source) = db_with_source(vec![1, 20]).await;
        let view = conn
            .create_materialized_view("big", "src")
            .select([("x", "x")])
            .only_if("x > 10")
            .execute()
            .await
            .unwrap();
        view.refresh().execute().await.unwrap();

        append(&source, vec![3, 30]).await;
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Incremental);
        assert_eq!(result.rows_written, 1);
        assert_eq!(read(view.table(), "x").await, vec![20, 30]);
    }

    /// The watermark has to advance even when no appended row matches, or the
    /// same fragments would be rescanned by every later refresh.
    #[tokio::test]
    async fn test_incremental_with_nothing_matching_advances_the_watermark() {
        let (conn, source) = db_with_source(vec![20]).await;
        let view = conn
            .create_materialized_view("big", "src")
            .select([("x", "x")])
            .only_if("x > 10")
            .execute()
            .await
            .unwrap();
        view.refresh().execute().await.unwrap();

        append(&source, vec![1, 2]).await;
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Incremental);
        assert_eq!(result.rows_written, 0);

        let again = view.refresh().execute().await.unwrap();
        assert_eq!(again.mode, RefreshMode::NoOp);
    }

    /// Unlike a computed column, a view reflects source mutation: an update
    /// rebuilds rather than going stale.
    #[tokio::test]
    async fn test_update_replaces_the_rows_it_changed() {
        let (_conn, source, view) = refreshed_doubled(vec![1, 2, 3]).await;

        // An update changes rows in place, so the view replaces exactly
        // those rows and leaves the rest of what it holds alone.
        source
            .update()
            .column("x", "20")
            .only_if("x = 2")
            .execute()
            .await
            .unwrap();
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Incremental);
        assert_eq!(result.rows_written, 1, "only the changed row is recomputed");
        assert_eq!(read(view.table(), "twice").await, vec![2, 6, 40]);
    }

    #[tokio::test]
    async fn test_update_reconciles_duplicate_output_rows_as_a_bag() {
        let (conn, source) = db_with_source(vec![1, 1, 2]).await;
        let view = conn
            .create_materialized_view("parity", "src")
            .select([("bucket", "x % 2")])
            .execute()
            .await
            .unwrap();
        view.refresh().execute().await.unwrap();
        assert_eq!(read(view.table(), "bucket").await, vec![0, 1, 1]);

        source
            .update()
            .column("x", "4")
            .only_if("x = 1")
            .execute()
            .await
            .unwrap();
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Incremental);
        assert_eq!(result.rows_written, 2);
        assert_eq!(read(view.table(), "bucket").await, vec![0, 0, 0]);
    }

    #[tokio::test]
    async fn test_update_reconciles_filter_transitions() {
        let (conn, source) = db_with_source(vec![1, 20]).await;
        let view = conn
            .create_materialized_view("big_updates", "src")
            .select([("x", "x")])
            .only_if("x > 10")
            .execute()
            .await
            .unwrap();
        view.refresh().execute().await.unwrap();

        source
            .update()
            .column("x", "30")
            .only_if("x = 1")
            .execute()
            .await
            .unwrap();
        let entered = view.refresh().execute().await.unwrap();
        assert_eq!(entered.mode, RefreshMode::Incremental);
        assert_eq!(read(view.table(), "x").await, vec![20, 30]);

        source
            .update()
            .column("x", "2")
            .only_if("x = 20")
            .execute()
            .await
            .unwrap();
        let left = view.refresh().execute().await.unwrap();
        assert_eq!(left.mode, RefreshMode::Incremental);
        assert_eq!(left.rows_written, 0);
        assert_eq!(read(view.table(), "x").await, vec![30]);
    }

    #[tokio::test]
    async fn test_update_reconciles_null_and_float_keys() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(
            ("id", Int32, [1, 2, 3]),
            ("value", Float64, [Some(f64::NAN), None, Some(-0.0)])
        )
        .unwrap();
        let source = conn
            .create_table("float_src", batch)
            .write_options(crate::materialized_view::tests::stable_row_ids())
            .execute()
            .await
            .unwrap();
        let view = conn
            .create_materialized_view("float_view", "float_src")
            .select([("value", "value")])
            .execute()
            .await
            .unwrap();
        view.refresh().execute().await.unwrap();

        for (id, value) in [(1, "1.0"), (2, "2.0"), (3, "0.0")] {
            source
                .update()
                .column("value", value)
                .only_if(format!("id = {id}"))
                .execute()
                .await
                .unwrap();
        }
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Incremental);
        assert_eq!(result.rows_written, 3);

        let batches = view
            .table()
            .query()
            .select(Select::columns(&["value"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut values: Vec<f64> = batches
            .iter()
            .flat_map(|batch| {
                batch["value"]
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .iter()
                    .flatten()
            })
            .collect();
        values.sort_by(f64::total_cmp);
        assert_eq!(values, vec![0.0, 1.0, 2.0]);
    }

    /// Legacy storage cannot serve DatasetDelta row streams, so it rebuilds
    /// rather than retaining the former fragment-classification path.
    #[tokio::test]
    async fn test_a_legacy_storage_source_is_reconciled() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("x", Int32, [1, 2, 3])).unwrap();
        let source = conn
            .create_table("legacy_src", batch)
            .write_options(crate::table::WriteOptions {
                lance_write_params: Some(lance::dataset::WriteParams {
                    enable_stable_row_ids: true,
                    data_storage_version: Some(LanceFileVersion::Legacy),
                    ..Default::default()
                }),
            })
            .execute()
            .await
            .unwrap();
        let view = conn
            .create_materialized_view("legacy_doubled", "legacy_src")
            .select([("x", "x"), ("twice", "x * 2")])
            .execute()
            .await
            .unwrap();
        view.refresh().execute().await.unwrap();

        source
            .add(record_batch!(("x", Int32, [4])).unwrap())
            .execute()
            .await
            .unwrap();
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        assert_eq!(read(view.table(), "twice").await, vec![2, 4, 6, 8]);

        source
            .update()
            .column("x", "20")
            .only_if("x = 2")
            .execute()
            .await
            .unwrap();
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        assert_eq!(read(view.table(), "twice").await, vec![2, 6, 8, 40]);
    }

    #[tokio::test]
    async fn test_delete_rebuilds_without_before_images() {
        let (_conn, source, view) = refreshed_doubled(vec![1, 2, 3]).await;

        source.delete("x = 2").await.unwrap();
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        assert_eq!(read(view.table(), "twice").await, vec![2, 6]);

        // A delete and an append in one span: both are applied.
        source.delete("x = 1").await.unwrap();
        source
            .add(record_batch!(("x", Int32, vec![4])).unwrap())
            .execute()
            .await
            .unwrap();
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        assert_eq!(read(view.table(), "twice").await, vec![6, 8]);
    }

    #[tokio::test]
    async fn test_restore_rebuilds_rows_hidden_from_the_delta_streams() {
        let (_conn, source, view) = refreshed_doubled(vec![1, 2, 3]).await;
        let original_version = source.version().await.unwrap();

        source.delete("x = 2").await.unwrap();
        view.refresh().execute().await.unwrap();
        assert_eq!(read(view.table(), "twice").await, vec![2, 6]);

        source.checkout(original_version).await.unwrap();
        source.restore().await.unwrap();
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        assert_eq!(read(view.table(), "twice").await, vec![2, 4, 6]);
    }

    /// merge_insert's by-source arm removes rows, which rebuilds until the
    /// delta API can return deleted-row before-images.
    #[tokio::test]
    async fn test_merge_insert_by_source_delete_evicts_the_view_rows() {
        let (_conn, source, view) = refreshed_doubled(vec![1, 2, 3]).await;

        let batch = record_batch!(("x", Int32, vec![1, 3])).unwrap();
        let reader = arrow_array::RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let mut merge = source.merge_insert(&["x"]);
        merge.when_not_matched_by_source_delete(None);
        merge.execute(Box::new(reader)).await.unwrap();

        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        assert_eq!(read(view.table(), "twice").await, vec![2, 6]);
    }

    /// A refresh may only certify the generation it planned from. A write
    /// that lands between planning and publication is drift the refresh did
    /// not account for, so it aborts rather than stamp it as materialized.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_refresh_aborts_rather_than_certify_view_drift() {
        let _serial = DRIFT_LOCK.lock().await;
        let (conn, source) = db_with_source(vec![1, 2, 3]).await;
        let view = conn
            .create_materialized_view("drifting_view", "src")
            .select([("x", "x"), ("twice", "x * 2")])
            .execute()
            .await
            .unwrap();
        view.refresh().execute().await.unwrap();
        let certified = source_version_of(view.table()).await;

        // The source loses a row, so the next refresh plans an eviction.
        source.delete("x = 2").await.unwrap();

        let uri = view
            .table()
            .as_native()
            .unwrap()
            .dataset
            .get()
            .await
            .unwrap()
            .uri()
            .to_string();
        *DRIFT_TARGET.lock().unwrap() = Some(uri);
        let refreshing = tokio::spawn(async move { view.refresh().execute().await });

        // Move the view once the refresh has planned against it.
        tokio::time::timeout(std::time::Duration::from_secs(30), DRIFT_PLANNED.notified())
            .await
            .expect("the refresh never reached the publication boundary");
        let drifted = conn.open_table("drifting_view").execute().await.unwrap();
        drifted.delete("twice = 6").await.unwrap();
        DRIFT_RELEASED.notify_one();

        // Publishing removals and additions as one change makes the drift a
        // conflict lance itself rejects; a pure append, which touches no
        // existing fragment, still relies on the generation check.
        let err = refreshing.await.unwrap().unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("raced this refresh") || message.contains("preempted by concurrent"),
            "got {err:?}"
        );
        // The watermark still names the generation that was actually proven.
        assert_eq!(source_version_of(&drifted).await, certified);
    }

    async fn source_version_of(table: &Table) -> Option<String> {
        table
            .schema()
            .await
            .unwrap()
            .metadata()
            .get(SOURCE_VERSION_META_KEY)
            .cloned()
    }

    /// A transaction file carries placeholder ids for the fragments it
    /// creates. Into an empty source those collide with the ids the commit
    /// assigns, so treating them as already-materialized drops the very
    /// first rows while the watermark still advances past them.
    #[tokio::test]
    async fn test_merge_into_an_empty_source_is_materialized() {
        let (_conn, source, view) = refreshed_doubled(vec![]).await;
        assert_eq!(read(view.table(), "twice").await, Vec::<i32>::new());

        let batch = record_batch!(("x", Int32, vec![1, 2])).unwrap();
        let reader = arrow_array::RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let mut merge = source.merge_insert(&["x"]);
        merge
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        merge.execute(Box::new(reader)).await.unwrap();

        view.refresh().execute().await.unwrap();
        assert_eq!(read(view.table(), "twice").await, vec![2, 4]);
        // A second refresh must not double them either.
        view.refresh().execute().await.unwrap();
        assert_eq!(read(view.table(), "twice").await, vec![2, 4]);
    }

    /// A refresh publishes what it removes and what it adds as one change,
    /// so an update never exposes the view without the rows it is replacing.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_an_update_is_never_visible_as_a_gap() {
        let _serial = DRIFT_LOCK.lock().await;
        let (conn, source) = db_with_source(vec![1, 2, 3]).await;
        let view = conn
            .create_materialized_view("atomic_view", "src")
            .select([("x", "x"), ("twice", "x * 2")])
            .execute()
            .await
            .unwrap();
        view.refresh().execute().await.unwrap();

        source
            .update()
            .column("x", "x + 10")
            .execute()
            .await
            .unwrap();

        let uri = view
            .table()
            .as_native()
            .unwrap()
            .dataset
            .get()
            .await
            .unwrap()
            .uri()
            .to_string();
        *DRIFT_TARGET.lock().unwrap() = Some(uri);
        let refreshing = tokio::spawn(async move { view.refresh().execute().await });

        // Read the view while the refresh is staged but not yet published.
        tokio::time::timeout(std::time::Duration::from_secs(30), DRIFT_PLANNED.notified())
            .await
            .expect("the refresh never reached the publication boundary");
        let midway = conn.open_table("atomic_view").execute().await.unwrap();
        assert_eq!(
            read(&midway, "twice").await,
            vec![2, 4, 6],
            "the pre-refresh rows must still be there in full"
        );
        DRIFT_RELEASED.notify_one();

        refreshing.await.unwrap().unwrap();
        let after = conn.open_table("atomic_view").execute().await.unwrap();
        assert_eq!(read(&after, "twice").await, vec![22, 24, 26]);
    }

    async fn row_id_by_x(table: &Table) -> HashMap<i32, u64> {
        let dataset = table
            .as_native()
            .unwrap()
            .dataset
            .get()
            .await
            .unwrap()
            .as_ref()
            .clone();
        let mut scanner = dataset.scan();
        scanner.with_row_id().project(&["x", ROW_ID]).unwrap();
        let batches = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        batches
            .iter()
            .flat_map(|batch| {
                let xs = batch["x"].as_any().downcast_ref::<Int32Array>().unwrap();
                let ids = batch[ROW_ID].as_primitive::<UInt64Type>();
                (0..batch.num_rows())
                    .map(|i| (xs.value(i), ids.value(i)))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// One delta batch is as large as a fragment's deletions, so it arrives
    /// well past a chunk: it has to be drained a chunk at a time without ever
    /// holding the batch, and the passes' deletion vectors have to accumulate
    /// rather than replace each other.
    #[tokio::test]
    async fn test_a_chunked_eviction_removes_every_row_it_names() {
        let (_conn, _, view) = refreshed_doubled(vec![1, 2, 3, 4, 5, 6]).await;

        let native = view.table().as_native().unwrap();
        let view_ds = native.dataset.get().await.unwrap().as_ref().clone();
        let row_ids = row_id_by_x(view.table()).await;

        let mut eviction = Eviction::new(&view_ds, 2);
        let batch = UInt64Array::from_iter_values([1, 2, 3, 4].map(|x| row_ids[&x]));
        eviction.push(&batch).await.unwrap();
        assert_eq!(
            eviction.peak, 2,
            "a batch past the chunk was buffered whole"
        );
        let staged = eviction.finish().await.unwrap();
        assert!(staged.is_some(), "four ids over a chunk of two stage twice");
        publish(&view_ds, staged, Vec::new(), None, None)
            .await
            .unwrap();
        native.dataset.reload().await.unwrap();

        assert_eq!(read(view.table(), "x").await, vec![5, 6]);
    }

    /// Two first rebuilds materialize the same source rows; the loser must
    /// key-conflict at commit and land nothing, or the view doubles.
    #[tokio::test]
    async fn test_a_raced_first_rebuild_lands_nothing() {
        let _guard = DRIFT_LOCK.lock().await;
        let (conn, _) = db_with_source(vec![1, 2, 3]).await;
        let view = conn
            .create_materialized_view("raced_rebuild", "src")
            .select([("x", "x"), ("twice", "x * 2")])
            .execute()
            .await
            .unwrap();
        let native = view.table().as_native().unwrap();
        let view_ds = native.dataset.get().await.unwrap().as_ref().clone();
        let uri = view_ds.uri().to_string();
        *DRIFT_TARGET.lock().unwrap() = Some(uri);
        let racing = tokio::spawn(async move { view.refresh().execute().await });
        tokio::time::timeout(std::time::Duration::from_secs(30), DRIFT_PLANNED.notified())
            .await
            .expect("the rebuild never reached the publication boundary");

        // The other process's first rebuild, reduced to its commit. Its
        // source rows are DISJOINT from ours -- the sentinel alone must make
        // the two rebuilds conflict.
        let batch = record_batch!(("x", Int32, [8, 9]), ("twice", Int32, [16, 18])).unwrap();
        let schema = Arc::new(ArrowSchema::from(view_ds.schema()));
        let batch = RecordBatch::try_new(
            schema.clone(),
            schema
                .fields()
                .iter()
                .map(|f| batch.column_by_name(f.name()).unwrap().clone())
                .collect(),
        )
        .unwrap();
        let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter([Ok(batch)]),
        ));
        let write_txn = InsertBuilder::new(WriteDestination::Dataset(Arc::new(view_ds.clone())))
            .with_params(&WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            })
            .execute_uncommitted_stream(stream)
            .await
            .unwrap();
        let Operation::Append { fragments } = write_txn.operation else {
            panic!("expected an append");
        };
        CommitBuilder::new(WriteDestination::Dataset(Arc::new(view_ds.clone())))
            .execute(Transaction::new(
                view_ds.version().version,
                Operation::Update {
                    removed_fragment_ids: Vec::new(),
                    updated_fragments: Vec::new(),
                    new_fragments: fragments,
                    fields_modified: Vec::new(),
                    compacted_sstables: Vec::new(),
                    fields_for_preserving_frag_bitmap: Vec::new(),
                    update_mode: None,
                    inserted_rows_filter: Some(refresh_filter()),
                    updated_fragment_offsets: None,
                },
                None,
            ))
            .await
            .unwrap();
        DRIFT_RELEASED.notify_one();

        let err = racing.await.unwrap().unwrap_err();
        // The loser lands nothing: only the winner's rows remain.
        let raced = conn.open_table("raced_rebuild").execute().await.unwrap();
        assert_eq!(
            read(&raced, "twice").await,
            vec![16, 18],
            "the losing rebuild must not union with the winner ({err})"
        );
    }

    /// An incremental refresh racing any other refresh must land nothing,
    /// even when their planned rows are disjoint: the shared token makes the
    /// commits conflict.
    #[tokio::test]
    async fn test_a_raced_incremental_lands_nothing() {
        let _guard = DRIFT_LOCK.lock().await;
        let (conn, source) = db_with_source(vec![1, 2, 3]).await;
        let view = conn
            .create_materialized_view("raced_incremental", "src")
            .select([("x", "x"), ("twice", "x * 2")])
            .execute()
            .await
            .unwrap();
        view.refresh().execute().await.unwrap();
        append(&source, vec![4]).await;

        let native = view.table().as_native().unwrap();
        native.dataset.reload().await.unwrap();
        let view_ds = native.dataset.get().await.unwrap().as_ref().clone();
        *DRIFT_TARGET.lock().unwrap() = Some(view_ds.uri().to_string());
        let racing = tokio::spawn(async move { view.refresh().execute().await });
        tokio::time::timeout(std::time::Duration::from_secs(30), DRIFT_PLANNED.notified())
            .await
            .expect("the refresh never reached the publication boundary");

        // The concurrent refresh, reduced to its commit: disjoint rows, the
        // shared token alone must collide.
        let batch = record_batch!(("x", Int32, [9]), ("twice", Int32, [18])).unwrap();
        let schema = Arc::new(ArrowSchema::from(view_ds.schema()));
        let batch = RecordBatch::try_new(
            schema.clone(),
            schema
                .fields()
                .iter()
                .map(|f| batch.column_by_name(f.name()).unwrap().clone())
                .collect(),
        )
        .unwrap();
        let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter([Ok(batch)]),
        ));
        let write_txn = InsertBuilder::new(WriteDestination::Dataset(Arc::new(view_ds.clone())))
            .with_params(&WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            })
            .execute_uncommitted_stream(stream)
            .await
            .unwrap();
        let Operation::Append { fragments } = write_txn.operation else {
            panic!("expected an append");
        };
        CommitBuilder::new(WriteDestination::Dataset(Arc::new(view_ds.clone())))
            .execute(Transaction::new(
                view_ds.version().version,
                Operation::Update {
                    removed_fragment_ids: Vec::new(),
                    updated_fragments: Vec::new(),
                    new_fragments: fragments,
                    fields_modified: Vec::new(),
                    compacted_sstables: Vec::new(),
                    fields_for_preserving_frag_bitmap: Vec::new(),
                    update_mode: None,
                    inserted_rows_filter: Some(refresh_filter()),
                    updated_fragment_offsets: None,
                },
                None,
            ))
            .await
            .unwrap();
        DRIFT_RELEASED.notify_one();

        let err = racing.await.unwrap().unwrap_err();
        let raced = conn
            .open_table("raced_incremental")
            .execute()
            .await
            .unwrap();
        assert_eq!(
            read(&raced, "twice").await,
            vec![2, 4, 6, 18],
            "the losing increment must not land its rows ({err})"
        );
    }

    /// Past the eviction cap the refresh falls back to the streamed rebuild,
    /// and the result is identical either way.
    #[tokio::test]
    async fn test_oversized_delta_falls_back_to_rebuild() {
        let (conn, source) = db_with_source((1..=20).collect()).await;
        let view = doubled_view(&conn).await;
        view.refresh().execute().await.unwrap();

        append(&source, (21..=30).collect()).await;
        *tests::DELTA_CAP.lock().unwrap() = Some(4);
        let result = view.refresh().execute().await;
        *tests::DELTA_CAP.lock().unwrap() = None;
        let result = result.unwrap();
        assert_eq!(
            result.mode,
            RefreshMode::Rebuild,
            "ten inserted rows, cap of four"
        );
        assert_eq!(read(view.table(), "x").await, (1..=30).collect::<Vec<_>>());

        // Under the cap the same shape stays incremental.
        append(&source, vec![31]).await;
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Incremental);
        assert_eq!(read(view.table(), "x").await, (1..=31).collect::<Vec<_>>());
    }

    /// A cap of zero is a view that holds nothing, not a view without a cap.
    #[tokio::test]
    async fn test_zero_limit_holds_no_rows() {
        let (conn, source) = db_with_source(vec![1, 2, 3]).await;
        let view = conn
            .create_materialized_view("empty", "src")
            .select([("x", "x")])
            .limit(0)
            .execute()
            .await
            .unwrap();

        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.rows_written, 0);
        assert_eq!(view.table().count_rows(None).await.unwrap(), 0);

        // Still nothing after the source grows, and the watermark advances.
        append(&source, vec![4]).await;
        view.refresh().execute().await.unwrap();
        assert_eq!(view.table().count_rows(None).await.unwrap(), 0);
        assert_eq!(
            view.refresh().execute().await.unwrap().mode,
            RefreshMode::NoOp
        );
    }

    /// An update rewrites a whole fragment. When it lands on one appended
    /// since the watermark, that fragment also holds rows the update never
    /// touched -- rows the recompute does not cover and the append set no
    /// longer reaches.
    #[tokio::test]
    async fn test_update_touching_a_new_fragment_keeps_its_untouched_rows() {
        let (_conn, source, view) = refreshed_doubled(vec![1, 2]).await;

        // One fragment, appended after the watermark, holding both a row the
        // update will change and a row it will not.
        append(&source, vec![3, 40]).await;
        source
            .update()
            .column("x", "99")
            .only_if("x = 3")
            .execute()
            .await
            .unwrap();

        view.refresh().execute().await.unwrap();
        assert_eq!(
            read(view.table(), "twice").await,
            vec![2, 4, 80, 198],
            "a row appended into the updated fragment went missing"
        );
    }

    async fn compact(source: &Table) {
        source
            .optimize(OptimizeAction::Compact {
                options: CompactionOptions::default(),
                remap_options: None,
            })
            .await
            .unwrap();
    }

    /// A compaction rearranges rows without changing them, so it costs the
    /// view nothing: the watermark advances and no row is recomputed.
    #[tokio::test]
    async fn test_compaction_alone_refreshes_incrementally() {
        let (_conn, source, view) = refreshed_doubled(vec![1, 2]).await;
        append(&source, vec![3]).await;
        view.refresh().execute().await.unwrap();

        compact(&source).await;
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Incremental);
        assert_eq!(result.rows_written, 0);
        assert_eq!(read(view.table(), "twice").await, vec![2, 4, 6]);
        assert_eq!(
            view.refresh().execute().await.unwrap().mode,
            RefreshMode::NoOp
        );
    }

    /// Rows appended after a compaction are separable through the transaction
    /// log: only the appended fragments are computed.
    #[tokio::test]
    async fn test_append_after_compaction_stays_incremental() {
        let (_conn, source, view) = refreshed_doubled(vec![1]).await;
        append(&source, vec![2]).await;
        view.refresh().execute().await.unwrap();

        compact(&source).await;
        append(&source, vec![3]).await;
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Incremental);
        assert_eq!(result.rows_written, 1);
        assert_eq!(read(view.table(), "twice").await, vec![2, 4, 6]);
    }

    /// DatasetDelta identifies an append even when a later compaction folds
    /// its fragment into existing data.
    #[tokio::test]
    async fn test_append_folded_into_compaction_stays_incremental() {
        let (_conn, source, view) = refreshed_doubled(vec![1]).await;

        append(&source, vec![2]).await;
        compact(&source).await;
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Incremental);
        assert_eq!(read(view.table(), "twice").await, vec![2, 4]);
    }

    /// The create-time gate holds across a drop-and-recreate of the source
    /// under the same name.
    #[tokio::test]
    async fn test_refresh_refuses_a_recreated_source_without_stable_row_ids() {
        let (conn, _, view) = refreshed_doubled(vec![1]).await;

        conn.drop_table("src", &[]).await.unwrap();
        let batch = record_batch!(("x", Int32, [9])).unwrap();
        conn.create_table("src", batch).execute().await.unwrap();

        let err = view.refresh().execute().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidInput { message } if message.contains("stable row ids"))
        );
    }

    /// A change to a column the view does not read is not a reason to
    /// rebuild: the exact signature check is scoped to the view's inputs.
    #[tokio::test]
    async fn test_unrelated_column_change_does_not_rebuild() {
        let (_conn, source, view) = refreshed_doubled(vec![1, 2]).await;

        source
            .add_columns()
            .transform(NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(
                vec![arrow_schema::Field::new(
                    "unrelated",
                    arrow_schema::DataType::Int32,
                    true,
                )],
            ))))
            .execute()
            .await
            .unwrap();

        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Incremental);
        assert_eq!(result.rows_written, 0);
        assert_eq!(read(view.table(), "twice").await, vec![2, 4]);
    }

    #[tokio::test]
    async fn test_full_forces_a_rebuild() {
        let (_conn, source, view) = refreshed_doubled(vec![1]).await;

        append(&source, vec![2]).await;
        let result = view.refresh().full(true).execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        assert_eq!(result.rows_written, 2);
        assert_eq!(read(view.table(), "twice").await, vec![2, 4]);
    }

    /// A cap and incremental reconciliation do not compose: rows skipped at
    /// the cap fall behind the watermark, so room freed later cannot be
    /// refilled from any delta. A capped view rebuilds instead, which its
    /// own cap keeps cheap.
    #[tokio::test]
    async fn test_limited_view_rebuilds_rather_than_reconcile() {
        let (conn, source) = db_with_source(vec![1, 2]).await;
        let view = conn
            .create_materialized_view("capped", "src")
            .select([("x", "x")])
            .limit(2)
            .execute()
            .await
            .unwrap();
        view.refresh().execute().await.unwrap();
        assert_eq!(read(view.table(), "x").await, vec![1, 2]);

        append(&source, vec![3, 4]).await;
        source
            .update()
            .column("x", "11")
            .only_if("x = 1")
            .execute()
            .await
            .unwrap();
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        let held = read(view.table(), "x").await;
        assert_eq!(held.len(), 2, "the cap holds: {held:?}");
        let selectable = read(&source, "x").await;
        assert!(
            held.iter().all(|x| selectable.contains(x)),
            "{held:?} is not a subset of {selectable:?}"
        );
    }

    #[tokio::test]
    async fn test_limit_caps_the_view() {
        let (conn, source) = db_with_source(vec![1, 2, 3]).await;
        let view = conn
            .create_materialized_view("capped", "src")
            .select([("x", "x")])
            .limit(4)
            .execute()
            .await
            .unwrap();

        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.rows_written, 3);

        // A changed capped view rebuilds because rows previously skipped at
        // the cap are outside the incremental watermark.
        append(&source, vec![4, 5, 6]).await;
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        assert_eq!(result.rows_written, 4);
        assert_eq!(view.table().count_rows(None).await.unwrap(), 4);

        // Further row changes rebuild for the same reason.
        append(&source, vec![7]).await;
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        assert_eq!(result.rows_written, 4);
        assert_eq!(
            view.refresh().execute().await.unwrap().mode,
            RefreshMode::NoOp
        );
    }

    /// The whole point of the fragment-swap commit: a rebuild must never
    /// leave the view without its index definitions.
    #[tokio::test]
    async fn test_rebuild_retains_indexes() {
        let (_conn, source, view) = refreshed_doubled(vec![1, 2, 3]).await;

        view.table()
            .create_index(&["twice"], Index::BTree(BTreeIndexBuilder::default()))
            .execute()
            .await
            .unwrap();
        assert_eq!(view.table().list_indices().await.unwrap().len(), 1);

        source
            .update()
            .column("x", "x + 10")
            .execute()
            .await
            .unwrap();
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        assert_eq!(view.table().list_indices().await.unwrap().len(), 1);
        assert_eq!(read(view.table(), "twice").await, vec![22, 24, 26]);

        // The swapped-in rows are reachable through an indexed query.
        let batches = view
            .table()
            .query()
            .only_if("twice = 24")
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    #[tokio::test]
    async fn test_rebuild_of_an_empty_result_is_an_empty_view() {
        let (conn, _) = db_with_source(vec![1, 2]).await;
        let view = conn
            .create_materialized_view("none", "src")
            .select([("x", "x")])
            .only_if("x > 100")
            .execute()
            .await
            .unwrap();

        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        assert_eq!(result.rows_written, 0);
        assert_eq!(view.table().count_rows(None).await.unwrap(), 0);
        assert_eq!(
            view.refresh().execute().await.unwrap().mode,
            RefreshMode::NoOp
        );
    }

    #[tokio::test]
    async fn test_view_schema_has_no_source_row_id() {
        let (_conn, _, view) = refreshed_doubled(vec![1, 2, 3]).await;
        let schema = view.table().schema().await.unwrap();
        assert!(schema.field_with_name("__source_row_id").is_err());
        assert_eq!(schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_dropping_a_source_input_fails_the_refresh() {
        let (conn, source) = db_with_source(vec![1]).await;
        let view = conn
            .create_materialized_view("v", "src")
            .select([("twice", "x * 2")])
            .execute()
            .await
            .unwrap();
        view.refresh().execute().await.unwrap();

        append(&source, vec![2]).await;
        source
            .add_columns()
            .transform(NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(
                vec![arrow_schema::Field::new(
                    "y",
                    arrow_schema::DataType::Int32,
                    true,
                )],
            ))))
            .execute()
            .await
            .unwrap();
        source.drop_columns(&["x"]).await.unwrap();

        let err = view.refresh().execute().await.unwrap_err();
        assert!(matches!(err, Error::Schema { message } if message.contains("'x'")));
    }

    /// A pinned refresh materializes the source as of `version`; catching up
    /// to the appends beyond it stays incremental.
    #[tokio::test]
    async fn test_pinned_refresh_and_catch_up() {
        let (conn, source) = db_with_source(vec![1]).await;
        let view = doubled_view(&conn).await;
        let pinned = source.version().await.unwrap();

        append(&source, vec![2]).await;
        let result = view
            .refresh()
            .source_version(pinned)
            .execute()
            .await
            .unwrap();
        assert_eq!(result.source_version, pinned);
        assert_eq!(read(view.table(), "twice").await, vec![2]);

        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Incremental);
        assert_eq!(read(view.table(), "twice").await, vec![2, 4]);
    }

    /// Views chain: a view's stable row ids make it a source like any other.
    #[tokio::test]
    async fn test_a_view_can_source_another_view() {
        let (conn, source) = db_with_source(vec![1, 2, 30]).await;
        let first = doubled_view(&conn).await;
        first.refresh().execute().await.unwrap();

        let second = conn
            .create_materialized_view("second", "doubled")
            .only_if("twice > 10")
            .execute()
            .await
            .unwrap();
        assert_eq!(second.definition().projections.len(), 2);
        let result = second.refresh().execute().await.unwrap();
        assert_eq!(result.rows_written, 1);
        assert_eq!(read(second.table(), "twice").await, vec![60]);

        append(&source, vec![50]).await;
        first.refresh().execute().await.unwrap();
        let result = second.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Incremental);
        assert_eq!(read(second.table(), "twice").await, vec![60, 100]);
    }

    /// The watermark speaks only for the state a refresh left behind: a
    /// direct write to the view is drift, and the next refresh rebuilds
    /// rather than preserving it as current.
    #[tokio::test]
    async fn test_direct_view_mutation_forces_a_rebuild() {
        let (_conn, _, view) = refreshed_doubled(vec![1, 2]).await;

        view.table().delete("x = 1").await.unwrap();
        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        assert_eq!(read(view.table(), "twice").await, vec![2, 4]);
        assert_eq!(
            view.refresh().execute().await.unwrap().mode,
            RefreshMode::NoOp
        );
    }

    /// A dropped and recreated source reuses version numbers but never their
    /// timestamps; the watermark must not vouch for the replacement's rows.
    #[tokio::test]
    async fn test_source_recreation_forces_a_rebuild() {
        let (conn, _, view) = refreshed_doubled(vec![1]).await;

        conn.drop_table("src", &[]).await.unwrap();
        let batch = record_batch!(("x", Int32, [7])).unwrap();
        conn.create_table("src", batch)
            .write_options(crate::materialized_view::tests::stable_row_ids())
            .execute()
            .await
            .unwrap();

        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        assert_eq!(read(view.table(), "twice").await, vec![14]);
    }

    /// A refresh bound to an incarnation refuses a view dropped and recreated
    /// since, even under the same name and definition; the recreated view's
    /// own token is accepted, and the token survives a refresh's stamp.
    #[tokio::test]
    async fn test_refresh_refuses_a_recreated_view_incarnation() {
        let (conn, _, view) = refreshed_doubled(vec![1]).await;
        let token = view.incarnation().unwrap().to_string();
        view.refresh()
            .expect_incarnation(&token)
            .execute()
            .await
            .unwrap();
        let reopened = conn.open_materialized_view("doubled").await.unwrap();
        assert_eq!(reopened.incarnation(), Some(token.as_str()));

        conn.drop_table("doubled", &[]).await.unwrap();
        let recreated = doubled_view(&conn).await;
        assert_ne!(recreated.incarnation(), Some(token.as_str()));

        let err = recreated
            .refresh()
            .expect_incarnation(&token)
            .execute()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("dropped and recreated"), "{err}");
        assert_eq!(read(recreated.table(), "twice").await, Vec::<i32>::new());

        recreated
            .refresh()
            .expect_incarnation(recreated.incarnation().unwrap())
            .execute()
            .await
            .unwrap();
        assert_eq!(read(recreated.table(), "twice").await, vec![2]);
    }

    /// A cloned declaration creates two physical tables; each gets its own
    /// token.
    #[tokio::test]
    async fn test_cloned_declaration_mints_a_fresh_incarnation_per_create() {
        let (conn, source) = db_with_source(vec![1]).await;
        let prepared = crate::materialized_view::prepare_declaration(
            &source,
            &[("x".into(), "x".into()), ("twice".into(), "x * 2".into())],
            None,
            None,
        )
        .await
        .unwrap();
        let replacement = prepared.clone();
        let first = prepared.create("cloned").await.unwrap();
        let first_token = first.incarnation().unwrap().to_string();

        conn.drop_table("cloned", &[]).await.unwrap();
        let second = replacement.create("cloned").await.unwrap();
        assert_ne!(second.incarnation(), Some(first_token.as_str()));
    }

    /// A recreation that lands after planning but before publication is
    /// caught by the pre-commit read: the stale refresh fails and the
    /// replacement stays empty under its own token.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_bound_refresh_cannot_publish_into_a_raced_recreation() {
        let _serial = DRIFT_LOCK.lock().await;
        let (conn, _) = db_with_source(vec![1]).await;
        let view = doubled_view(&conn).await;
        let token = view.incarnation().unwrap().to_string();
        let uri = view
            .table()
            .as_native()
            .unwrap()
            .dataset
            .get()
            .await
            .unwrap()
            .uri()
            .to_string();

        *DRIFT_TARGET.lock().unwrap() = Some(uri);
        let refreshing =
            tokio::spawn(async move { view.refresh().expect_incarnation(token).execute().await });
        tokio::time::timeout(std::time::Duration::from_secs(30), DRIFT_PLANNED.notified())
            .await
            .expect("refresh never reached publication");

        conn.drop_table("doubled", &[]).await.unwrap();
        let replacement = doubled_view(&conn).await;
        let replacement_token = replacement.incarnation().unwrap().to_string();
        DRIFT_RELEASED.notify_one();

        let result = refreshing.await.unwrap();
        assert!(result.is_err(), "the stale refresh unexpectedly succeeded");
        let reopened = conn.open_materialized_view("doubled").await.unwrap();
        assert_eq!(reopened.incarnation(), Some(replacement_token.as_str()));
        assert_eq!(read(reopened.table(), "twice").await, Vec::<i32>::new());
    }

    /// Replacing the schema metadata wholesale drops the token. A refresh
    /// bound to the old token is refused for that reason, not as a
    /// recreation; an unbound refresh mints the view a fresh one.
    #[tokio::test]
    async fn test_a_view_whose_metadata_was_replaced_starts_a_new_incarnation() {
        let (conn, _, view) = refreshed_doubled(vec![1]).await;
        let token = view.incarnation().unwrap().to_string();
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::materialized_view::DEFINITION_META_KEY.to_string(),
            crate::materialized_view::definition_to_metadata(view.definition()).unwrap(),
        );
        view.table()
            .as_native()
            .unwrap()
            .replace_schema_metadata(metadata)
            .await
            .unwrap();
        assert_eq!(
            conn.open_materialized_view("doubled")
                .await
                .unwrap()
                .incarnation(),
            None
        );

        let err = view
            .refresh()
            .expect_incarnation(&token)
            .execute()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no incarnation token"), "{err}");

        view.refresh().execute().await.unwrap();
        let reopened = conn.open_materialized_view("doubled").await.unwrap();
        assert!(reopened.incarnation().is_some());
        assert_ne!(reopened.incarnation(), Some(token.as_str()));
    }

    /// In-process refreshes of one view serialize: the loser of the race
    /// observes the winner's watermark instead of appending the same rows.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_refreshes_do_not_duplicate() {
        let (_conn, source, view) = refreshed_doubled(vec![1]).await;

        append(&source, vec![2, 3]).await;
        let (a, b) = tokio::join!(view.refresh().execute(), view.refresh().execute());
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(read(view.table(), "twice").await, vec![2, 4, 6]);
        let modes = [a.mode, b.mode];
        assert!(modes.contains(&RefreshMode::Incremental));
        assert!(modes.contains(&RefreshMode::NoOp));
    }

    /// A second handle's lazy cache must not defeat the lock: after another
    /// handle's refresh commits, the stale handle plans from the reloaded
    /// state and no-ops instead of appending the same fragments again.
    #[tokio::test]
    async fn test_a_second_handle_does_not_double_append() {
        let (conn, source, view) = refreshed_doubled(vec![1, 2, 3]).await;
        let stale = conn.open_materialized_view("doubled").await.unwrap();

        append(&source, vec![4]).await;
        view.refresh().execute().await.unwrap();

        let result = stale.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::NoOp);
        assert_eq!(read(view.table(), "twice").await, vec![2, 4, 6, 8]);
    }

    /// A commit racing between a refresh's data commit and its stamp must
    /// not be certified as the refresh's generation: the stamp aborts, and
    /// the next refresh rebuilds from the drifted state.
    #[tokio::test]
    async fn test_stamp_aborts_on_a_racing_commit() {
        let (_conn, _, view) = refreshed_doubled(vec![1, 2]).await;

        let view_native = view.table().as_native().unwrap();
        let stale = view_native.dataset.get().await.unwrap().as_ref().clone();
        view.table().delete("x = 1").await.unwrap();

        let err = stamp_watermark(view_native, stale, 99, 99, None, None).await;
        assert!(err.is_err());

        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        assert_eq!(read(view.table(), "twice").await, vec![2, 4]);
    }

    /// What refresh executes and what it stamps must be one generation: a
    /// replaced definition wins over whatever a stale handle cached.
    #[tokio::test]
    async fn test_refresh_uses_the_latest_persisted_definition() {
        let (_conn, _, view) = refreshed_doubled(vec![1, 2]).await;

        let replacement = crate::materialized_view::MaterializedViewDefinition {
            source_table: "src".into(),
            projections: vec![
                crate::materialized_view::ViewProjection {
                    output: "x".into(),
                    expression: "x".into(),
                },
                crate::materialized_view::ViewProjection {
                    output: "twice".into(),
                    expression: "x * 3".into(),
                },
            ],
            filter: None,
            limit: None,
            inputs: vec!["x".into()],
        };
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::materialized_view::DEFINITION_META_KEY.to_string(),
            crate::materialized_view::definition_to_metadata(&replacement).unwrap(),
        );
        view.table()
            .as_native()
            .unwrap()
            .replace_schema_metadata(metadata)
            .await
            .unwrap();

        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.mode, RefreshMode::Rebuild);
        assert_eq!(read(view.table(), "twice").await, vec![3, 6]);
    }

    /// A persisted definition that does not produce this view's schema must
    /// not refresh at all, let alone be certified.
    #[tokio::test]
    async fn test_definition_view_schema_mismatch_is_refused() {
        let (_conn, _, view) = refreshed_doubled(vec![1, 2]).await;

        let narrower = crate::materialized_view::MaterializedViewDefinition {
            source_table: "src".into(),
            projections: vec![crate::materialized_view::ViewProjection {
                output: "x".into(),
                expression: "x".into(),
            }],
            filter: None,
            limit: None,
            inputs: vec!["x".into()],
        };
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::materialized_view::DEFINITION_META_KEY.to_string(),
            crate::materialized_view::definition_to_metadata(&narrower).unwrap(),
        );
        view.table()
            .as_native()
            .unwrap()
            .replace_schema_metadata(metadata)
            .await
            .unwrap();

        let err = view.refresh().execute().await.unwrap_err();
        assert!(matches!(err, Error::Schema { message } if message.contains("does not produce")),);
    }

    /// An output whose name needs quoting flows through as a projection
    /// alias, never spliced into SQL text.
    #[tokio::test]
    async fn test_output_names_needing_quotes() {
        let (conn, _) = db_with_source(vec![1, 2]).await;
        let view = conn
            .create_materialized_view("v", "src")
            .select([("double value", "x * 2")])
            .execute()
            .await
            .unwrap();

        let result = view.refresh().execute().await.unwrap();
        assert_eq!(result.rows_written, 2);
        assert_eq!(read(view.table(), "double value").await, vec![2, 4]);
    }

    /// MemWAL tiers are visible to reads but not to the refresh scan, so LSM
    /// state disqualifies every participant: the source at create, either
    /// side at refresh, and the view can never accept a spec. Retained
    /// un-compacted rows (the catch-up flag outlives unset) count as state.
    #[tokio::test]
    async fn lsm_state_disqualifies_source_and_view() {
        use crate::table::LsmWriteSpec;
        use arrow_array::RecordBatchIterator;

        let tmp_dir = tempfile::tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        // Hand-rolled: the LSM primary key must be non-nullable, which
        // record_batch! cannot express.
        let schema = Arc::new(ArrowSchema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("x", arrow_schema::DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![1, 2])) as _,
                Arc::new(Int32Array::from(vec![1, 2])) as _,
            ],
        )
        .unwrap();
        let table = conn
            .create_table("src", batch.clone())
            .write_options(crate::materialized_view::tests::stable_row_ids())
            .execute()
            .await
            .unwrap();
        table.set_unenforced_primary_key(["id"]).await.unwrap();
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();

        // An active-LSM source is refused at create.
        let err = conn
            .create_materialized_view("v", "src")
            .execute()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("un-compacted"), "{err}");

        // Unset with nothing written clears the state: the view creates,
        // and a spec can never be installed over it.
        table.unset_lsm_write_spec().await.unwrap();
        let view = conn
            .create_materialized_view("v", "src")
            .execute()
            .await
            .unwrap();
        let err = view
            .table()
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("materialized view"), "{err}");

        // A source that acquires a spec after create fails refresh.
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();
        let err = view.refresh().execute().await.unwrap_err();
        assert!(err.to_string().contains("source table 'src'"), "{err}");

        // Retained rows outlive unset: write through the WAL, unset, and
        // refresh still refuses.
        let mut merge = table.merge_insert(&["id"]);
        merge
            .when_matched_update_all(None)
            .when_not_matched_insert_all()
            .use_lsm(true);
        merge
            .execute(Box::new(RecordBatchIterator::new(
                vec![Ok(batch.clone())],
                batch.schema(),
            )))
            .await
            .unwrap();
        table.unset_lsm_write_spec().await.unwrap();
        let err = view.refresh().execute().await.unwrap_err();
        assert!(err.to_string().contains("source table 'src'"), "{err}");
    }
}
