// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Full refresh for materialized views.
//!
//! A refresh evaluates the definition at one source version, stages the
//! complete result, and atomically replaces every fragment of the view.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::{RecordBatch, new_null_array};
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use lance::Dataset;
use lance::dataset::mem_wal::DatasetMemWalExt;
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::{CommitBuilder, InsertBuilder, WriteDestination, WriteMode, WriteParams};
use lance_datafusion::planner::Planner;
use serde::{Deserialize, Serialize};

use super::{MaterializedViewDefinition, StoredDefinition};
use crate::database::OpenTableRequest;
use crate::table::computed_columns::{computed_column_from_field, ensure_declarations_are_planned};
use crate::table::{NativeTableExt, Table};
use crate::{Error, Result};

/// How a refresh brought the view up to date.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshMode {
    /// The view was recomputed from scratch.
    Rebuild,
}

/// The result of refreshing a materialized view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshMaterializedViewResult {
    /// How the view was brought up to date.
    pub mode: RefreshMode,
    /// Rows written to the replacement view.
    pub rows_written: u64,
    /// The source table version the view reflects.
    pub source_version: u64,
    /// The view table version after the refresh.
    pub version: u64,
}

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

/// Recompute a local materialized view from one source version.
pub(crate) async fn execute_refresh(
    view: &Table,
    pinned: Option<u64>,
) -> Result<RefreshMaterializedViewResult> {
    let native = view.as_native().ok_or_else(|| Error::NotSupported {
        message: "materialized views are supported only on local tables".into(),
    })?;
    native.dataset.ensure_mutable()?;
    let lock = refresh_lock(native.dataset.get().await?.uri());
    let _guard = lock.lock().await;
    native.dataset.reload().await?;
    let view_ds = native.dataset.get().await?.as_ref().clone();
    ensure_no_mem_wal(&view_ds, "materialized view", view.name()).await?;

    let definition = match super::read_definition(&view_ds.schema().metadata)? {
        Some(StoredDefinition::Query(definition)) => definition,
        Some(StoredDefinition::Newer { format }) => {
            return Err(Error::NotSupported {
                message: format!(
                    "materialized view '{}' is stored in format {format}, which this version of lancedb cannot refresh",
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
    let source = open_source(view, &definition).await?;
    let source = match pinned {
        Some(version) => source.checkout_version(version).await?,
        None => source,
    };
    ensure_no_mem_wal(&source, "source table", &definition.source_table).await?;
    let source_version = source.version().version;

    let source_schema = Arc::new(ArrowSchema::from(source.schema()));
    let super::Planned {
        definition,
        fields: planned_fields,
        inputs,
        ..
    } = super::plan(source_schema, &definition).map_err(|error| match error {
        Error::InvalidExpression { column, message } => Error::Schema {
            message: format!(
                "view column '{column}' no longer plans against '{}' (a source column was dropped or renamed): {message}",
                definition.source_table
            ),
        },
        Error::InvalidInput { message } => Error::Schema {
            message: format!(
                "the stored query no longer plans against '{}' (a source column was dropped or renamed): {message}",
                definition.source_table
            ),
        },
        error => error,
    })?;

    let physical = Arc::new(ArrowSchema::from(view_ds.schema()));
    ensure_declarations_are_planned(&physical)?;
    if let Some(field) = physical
        .fields()
        .iter()
        .find(|field| computed_column_from_field(field).is_some() && !field.is_nullable())
    {
        return Err(Error::Schema {
            message: format!(
                "computed column '{}' of view '{}' cannot hold NULL; recreate the view",
                field.name(),
                view.name()
            ),
        });
    }
    let stored_fields: Vec<_> = physical
        .fields()
        .iter()
        .filter(|field| computed_column_from_field(field).is_none())
        .collect();
    let matches = planned_fields.len() == stored_fields.len()
        && planned_fields
            .iter()
            .zip(stored_fields)
            .all(|(planned, stored)| {
                planned.name() == stored.name()
                    && planned.data_type() == stored.data_type()
                    && (stored.is_nullable() || !planned.is_nullable())
            });
    if !matches {
        return Err(Error::Schema {
            message: format!(
                "the stored definition of view '{}' does not produce this view's schema; recreate the view",
                view.name()
            ),
        });
    }

    let rows_written = Arc::new(AtomicU64::new(0));
    let unnest = super::physical_unnest(&definition)?;
    let stream = compute_stream(
        &source,
        &definition,
        &inputs,
        unnest.as_ref(),
        physical,
        rows_written.clone(),
    )
    .await?;
    let committed = replace_fragments(view_ds, stream).await?;
    let version = committed.version().version;
    native.dataset.update(committed);
    Ok(RefreshMaterializedViewResult {
        mode: RefreshMode::Rebuild,
        rows_written: rows_written.load(Ordering::Relaxed),
        source_version,
        version,
    })
}

/// Atomically replace all fragments of a materialized view with a complete
/// query result. This is shared with Sophon's SQL refresh job.
pub async fn replace_materialized_view_fragments(
    view: &Table,
    stream: SendableRecordBatchStream,
) -> Result<u64> {
    let native = view.as_native().ok_or_else(|| Error::NotSupported {
        message: "materialized views are supported only on local tables".into(),
    })?;
    native.dataset.ensure_mutable()?;
    let lock = refresh_lock(native.dataset.get().await?.uri());
    let _guard = lock.lock().await;
    native.dataset.reload().await?;
    let dataset = native.dataset.get().await?.as_ref().clone();
    if super::read_definition_sql(&dataset.schema().metadata)?.is_none() {
        return Err(Error::NotAMaterializedView {
            name: view.name().to_string(),
        });
    }
    let committed = replace_fragments(dataset, stream).await?;
    let version = committed.version().version;
    native.dataset.update(committed);
    Ok(version)
}

async fn replace_fragments(dataset: Dataset, stream: SendableRecordBatchStream) -> Result<Dataset> {
    let dataset = Arc::new(dataset);
    let read_version = dataset.version().version;
    let removed_fragment_ids = dataset
        .get_fragments()
        .iter()
        .map(|fragment| fragment.id() as u64)
        .collect();
    let write = InsertBuilder::new(WriteDestination::Dataset(dataset.clone()))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute_uncommitted_stream(stream)
        .await?;
    let Operation::Append {
        fragments: new_fragments,
    } = write.operation
    else {
        return Err(Error::Runtime {
            message: "expected an append while staging replacement rows".into(),
        });
    };
    let committed = CommitBuilder::new(WriteDestination::Dataset(dataset))
        .execute(Transaction::new(
            read_version,
            Operation::Update {
                removed_fragment_ids,
                updated_fragments: Vec::new(),
                new_fragments,
                fields_modified: Vec::new(),
                compacted_sstables: Vec::new(),
                fields_for_preserving_frag_bitmap: Vec::new(),
                update_mode: None,
                inserted_rows_filter: None,
                updated_fragment_offsets: None,
            },
            None,
        ))
        .await?;
    if committed.version().version != read_version + 1 {
        return Err(Error::Runtime {
            message: format!(
                "a concurrent commit raced this refresh (view version {}, expected {})",
                committed.version().version,
                read_version + 1
            ),
        });
    }
    Ok(committed)
}

/// Reject MemWAL/LSM state because an ordinary dataset scan cannot see its
/// un-compacted tiers.
pub(crate) async fn ensure_no_mem_wal(dataset: &Dataset, role: &str, name: &str) -> Result<()> {
    let retained = !dataset.list_mem_wal_latest_shard_ids().await?.is_empty();
    if retained || dataset.mem_wal_index_details().await?.is_some() {
        return Err(Error::NotSupported {
            message: format!("{role} '{name}' has an LSM write spec or retained un-compacted rows"),
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
            namespace_path: definition.source_namespace.clone(),
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
    Ok(native.dataset.get().await?.as_ref().clone())
}

async fn compute_stream(
    source: &Dataset,
    definition: &MaterializedViewDefinition,
    inputs: &[String],
    unnest: Option<&super::ViewUnnest>,
    schema: SchemaRef,
    rows_written: Arc<AtomicU64>,
) -> Result<SendableRecordBatchStream> {
    if definition.is_grouped() {
        return super::grouped::stream(source, definition, inputs, schema, rows_written).await;
    }
    let mut scanner = source.scan();
    if let Some(filter) = definition.filter.as_deref().filter(|_| unnest.is_none()) {
        scanner.filter(filter)?;
    }
    let expanded = match unnest {
        Some(unnest) => Some(UnnestPlan::new(source, definition, inputs, unnest)?),
        None => {
            let transforms: Vec<(&str, &str)> = definition
                .projections
                .iter()
                .map(|projection| (projection.output.as_str(), projection.expression.as_str()))
                .collect();
            scanner.project_with_transform(&transforms)?;
            None
        }
    };
    if let Some(expanded) = &expanded {
        scanner.project(&expanded.raw_inputs)?;
    }
    if definition.limit == Some(0) {
        return Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::empty(),
        )));
    }
    if let Some(limit) = definition.limit {
        let limit = i64::try_from(limit).map_err(|_| Error::InvalidInput {
            message: format!("view limit {limit} exceeds the maximum of {}", i64::MAX),
        })?;
        scanner.limit(Some(limit), None)?;
    }
    let output_schema = schema.clone();
    let mapped = scanner.try_into_stream().await?.map(move |batch| {
        let batch = batch.map_err(|error| DataFusionError::External(Box::new(error)))?;
        let batch = match &expanded {
            Some(expanded) => expanded.apply(&batch)?,
            None => batch,
        };
        let batch = to_view_batch(&batch, &output_schema)?;
        rows_written.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        Ok(batch)
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mapped)))
}

pub(super) fn to_view_batch(
    batch: &RecordBatch,
    output_schema: &SchemaRef,
) -> datafusion::common::Result<RecordBatch> {
    let mut columns = Vec::with_capacity(output_schema.fields().len());
    for field in output_schema.fields() {
        if computed_column_from_field(field).is_some() {
            columns.push(new_null_array(field.data_type(), batch.num_rows()));
            continue;
        }
        let column = batch.column_by_name(field.name()).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "view column '{}' is not produced by the view's definition",
                field.name()
            ))
        })?;
        columns.push(column.clone());
    }
    Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

struct UnnestPlan {
    column: String,
    raw_inputs: Vec<String>,
    read_schema: SchemaRef,
    projections: Vec<(String, Arc<dyn PhysicalExpr>)>,
    filter: Option<Arc<dyn PhysicalExpr>>,
}

impl UnnestPlan {
    fn new(
        source: &Dataset,
        definition: &MaterializedViewDefinition,
        inputs: &[String],
        unnest: &super::ViewUnnest,
    ) -> Result<Self> {
        let mut raw_inputs: Vec<String> = inputs
            .iter()
            .map(|input| super::root(input).to_string())
            .chain(std::iter::once(unnest.column.clone()))
            .collect();
        raw_inputs.sort();
        raw_inputs.dedup();
        let flattened = super::flattened_schema(&ArrowSchema::from(source.schema()), unnest)?;
        let mut read_fields = Vec::with_capacity(raw_inputs.len());
        for name in &raw_inputs {
            let name = if *name == unnest.column {
                &unnest.alias
            } else {
                name
            };
            let field = flattened
                .field_with_name(name)
                .map_err(|_| Error::Runtime {
                    message: format!("source column '{name}' read by the view is missing"),
                })?;
            read_fields.push(field.clone());
        }
        let read_schema = Arc::new(ArrowSchema::new(read_fields));
        let planner = Planner::new(read_schema.clone());
        let physical = |what: &str, sql: &str| -> Result<Arc<dyn PhysicalExpr>> {
            let error = |error: lance::Error| Error::Runtime {
                message: format!("{what}: {error}"),
            };
            let parsed = planner.parse_expr(sql).map_err(error)?;
            let optimized = planner.optimize_expr(parsed).map_err(error)?;
            planner.create_physical_expr(&optimized).map_err(error)
        };
        let projections = definition
            .projections
            .iter()
            .map(|projection| {
                physical(
                    &format!("view column '{}'", projection.output),
                    &projection.expression,
                )
                .map(|expression| (projection.output.clone(), expression))
            })
            .collect::<Result<_>>()?;
        let filter = definition
            .filter
            .as_deref()
            .map(|sql| physical("view filter", sql))
            .transpose()?;
        Ok(Self {
            column: unnest.column.clone(),
            raw_inputs,
            read_schema,
            projections,
            filter,
        })
    }

    fn apply(&self, batch: &RecordBatch) -> datafusion::common::Result<RecordBatch> {
        let unnested = unnest_batch(batch, &self.column)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let unnested = RecordBatch::try_new(self.read_schema.clone(), unnested.columns().to_vec())?;
        let unnested = match &self.filter {
            None => unnested,
            Some(filter) => {
                let keep = filter
                    .evaluate(&unnested)?
                    .into_array(unnested.num_rows())?;
                let keep = keep.as_boolean_opt().ok_or_else(|| {
                    DataFusionError::Internal("view filter did not evaluate to a boolean".into())
                })?;
                arrow_select::filter::filter_record_batch(&unnested, keep)?
            }
        };
        let columns = self
            .projections
            .iter()
            .map(|(output, expression)| {
                expression
                    .evaluate(&unnested)?
                    .into_array(unnested.num_rows())
                    .map(|value| (output.clone(), value))
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        Ok(RecordBatch::try_from_iter(columns)?)
    }
}

fn unnest_batch(batch: &RecordBatch, list_column: &str) -> Result<RecordBatch> {
    use arrow_array::{Array, ListArray, UInt32Array};
    use arrow_select::take::take;

    let (list_index, _) = batch
        .schema()
        .column_with_name(list_column)
        .ok_or_else(|| Error::Runtime {
            message: format!("expansion column '{list_column}' is not in the batch"),
        })?;
    let list = batch
        .column(list_index)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| Error::Runtime {
            message: format!(
                "expansion column '{list_column}' is {}, not a list",
                batch.column(list_index).data_type()
            ),
        })?;
    let offsets = list.value_offsets();
    let mut repeat = Vec::with_capacity(list.values().len());
    for row in 0..list.len() {
        if list.is_valid(row) {
            repeat.extend(std::iter::repeat_n(
                row as u32,
                (offsets[row + 1] - offsets[row]) as usize,
            ));
        }
    }
    let repeat = UInt32Array::from(repeat);
    let mut ranges = Vec::new();
    for row in 0..list.len() {
        if list.is_valid(row) {
            ranges.extend((offsets[row] as u32)..(offsets[row + 1] as u32));
        }
    }
    let elements = take(list.values().as_ref(), &UInt32Array::from(ranges), None)?;
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if index == list_index {
            let element = match field.data_type() {
                arrow_schema::DataType::List(element) => element.clone(),
                other => {
                    return Err(Error::Runtime {
                        message: format!("expansion column '{list_column}' is {other}, not a list"),
                    });
                }
            };
            fields.push(Arc::new(
                arrow_schema::Field::new(field.name(), element.data_type().clone(), true)
                    .with_metadata(field.metadata().clone()),
            ));
            columns.push(elements.clone());
        } else {
            fields.push(field.clone());
            columns.push(take(batch.column(index).as_ref(), &repeat, None)?);
        }
    }
    let schema = Arc::new(ArrowSchema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    Ok(RecordBatch::try_new(schema, columns)?)
}
