// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! A grouped view keyed by one `ivf_partition(col)` refreshed in units, one
//! per index partition: the index already holds each partition's row ids, so
//! a unit reads only its rows, computes only its groups and writes its own
//! fragments, and a single commit replaces the view. Nothing is shuffled and
//! no worker holds more than one partition's groups.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use datafusion::catalog::default_table_source::provider_as_source;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use futures::{StreamExt, TryStreamExt};
use lance::Dataset;
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::{
    CommitBuilder, InsertBuilder, ProjectionRequest, WriteDestination, WriteMode, WriteParams,
};
use lance_core::ROW_ID;
use lance_core::utils::address::RowAddress;
use lance_datafusion::exec::SessionContextExt;
use lance_index::metrics::NoOpMetricsCollector;
use lance_table::format::Fragment;
use serde::{Deserialize, Serialize};

use super::grouped::{self, IVF_PARTITION};
use super::refresh::{
    RefreshMaterializedViewResult, RefreshMode, ensure_incarnation, ensure_no_mem_wal, open_source,
    refresh_lock, stamp_watermark, to_view_batch,
};
use super::{MaterializedViewDefinition, StoredDefinition};
use crate::table::Table;
use crate::{Error, Result};

/// Row ids per `take`: bounds one unit's memory while it reads a partition.
const TAKE_CHUNK: usize = 8192;

/// Rows per partition, and in total, the plan reads to see whether the index
/// assigns by its own centroids. A sample: [`write_grouped_unit`] proves the
/// rows it takes, and this only keeps a doomed refresh off the fleet.
const PROBE_PER_UNIT: usize = 8;
const PROBE_ROWS: usize = 256;

/// How a grouped view can be refreshed in units.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupedRefreshPlan {
    /// Units `0..units`: unit `p` before the last holds index partition
    /// `p`'s groups; the last holds the rows the index cannot place (null or
    /// unplaceable vectors), grouped under a NULL key. One per index
    /// partition, plus that one.
    pub units: u32,
    /// The source version every unit reads and the commit records.
    pub source_version: u64,
    /// The view version the plan was made against; the commit lands on it
    /// or not at all.
    pub view_version: u64,
    /// The view this plan belongs to: its incarnation, which a drop and
    /// recreate replaces. Two views of the same shape reach the same
    /// counters, so without it a result written for one could be published
    /// into the other.
    pub incarnation: String,
}

/// The column a grouped view's single `ivf_partition(column)` key names, if
/// that is its whole `GROUP BY`.
fn ivf_key_column(definition: &MaterializedViewDefinition) -> Option<String> {
    let [key] = definition.group_by.as_slice() else {
        return None;
    };
    let inner = key
        .strip_prefix(&format!("{IVF_PARTITION}("))?
        .strip_suffix(')')?;
    let column = inner.trim().trim_matches('`');
    (!column.is_empty() && !column.contains(',')).then(|| column.to_string())
}

/// What a grouped refresh of `view` reads: its definition, source and binding.
struct Grouped {
    view_ds: Dataset,
    definition: MaterializedViewDefinition,
    source: Dataset,
    column: String,
    inputs: Vec<String>,
    binding: grouped::IvfBinding,
    segments: grouped::IvfSegments,
}

async fn open_grouped(view: &Table, pinned: Option<u64>) -> Result<Option<Grouped>> {
    let view_native = view.as_native().ok_or_else(|| Error::NotSupported {
        message: "materialized views are supported only on local tables".into(),
    })?;
    view_native.dataset.reload().await?;
    let view_ds = view_native.dataset.get().await?.as_ref().clone();
    let Some(StoredDefinition::Query(definition)) =
        super::read_definition(&view_ds.schema().metadata)?
    else {
        return Ok(None);
    };
    let Some(column) = ivf_key_column(&definition) else {
        return Ok(None);
    };
    let staging = super::read_staging(&view_ds.schema().metadata)?;
    let source = open_source(view, &definition, staging.as_ref()).await?;
    let source = match pinned {
        Some(version) => source.checkout_version(version).await?,
        None => source,
    };
    ensure_no_mem_wal(&source, "source table", &definition.source_table).await?;
    let source_schema = Arc::new(ArrowSchema::from(source.schema()));
    let planned = super::plan(source_schema, &definition, staging.as_ref())?;
    let (binding, segments) = grouped::bind_column_with_segments(&source, &column).await?;
    Ok(Some(Grouped {
        view_ds,
        definition: planned.definition,
        source,
        column,
        inputs: planned.inputs,
        binding,
        segments,
    }))
}

/// The units `view` can be refreshed in against `source_version` (the
/// latest when `None`), or `None` when it is not a grouped view keyed by one
/// `ivf_partition(column)`.
pub async fn plan_grouped_refresh(
    view: &Table,
    source_version: Option<u64>,
) -> Result<Option<GroupedRefreshPlan>> {
    let Some(grouped) = open_grouped(view, source_version).await? else {
        return Ok(None);
    };
    // The split reads indexed rows through the index and the rest by
    // scanning what it does not cover, so an index that does not say what it
    // covers cannot be split at all. The caller refreshes single-pass.
    if grouped.segments.covered.is_none() {
        return Ok(None);
    }
    // Without one there is nothing to bind a unit's result to, so the view
    // is refreshed in one pass, where the rows never leave the process.
    let Some(incarnation) = grouped
        .view_ds
        .schema()
        .metadata
        .get(super::INCARNATION_META_KEY)
        .cloned()
    else {
        return Ok(None);
    };
    // The split is the index's posting lists and the key is recomputed, so a
    // view whose index assigns by something else has no units at all.
    if !assigns_by_its_centroids(&grouped).await? {
        return Ok(None);
    }
    Ok(Some(GroupedRefreshPlan {
        units: grouped.binding.partitions + 1,
        source_version: grouped.source.version().version,
        view_version: grouped.view_ds.version().version,
        incarnation,
    }))
}

/// Fragments one unit wrote, uncommitted, for [`commit_grouped_refresh`].
/// Only [`write_grouped_unit`] makes one: it names the unit and the plan,
/// and the commit publishes a set of them as the view's whole contents, so
/// a result belonging to another plan, another view, or to a unit already
/// accounted for has to be refusable. Travels to whoever commits, so it
/// serializes, but its fields are not a caller's to write.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WrittenUnit {
    unit: u32,
    plan: GroupedRefreshPlan,
    fragments: Vec<Fragment>,
    rows: u64,
}

impl WrittenUnit {
    /// The unit of the plan this result holds.
    pub fn unit(&self) -> u32 {
        self.unit
    }

    /// The plan it was computed for.
    pub fn plan(&self) -> &GroupedRefreshPlan {
        &self.plan
    }

    /// View rows this unit wrote, its groups.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// The fragments it staged, uncommitted until the whole set is published.
    pub fn fragments(&self) -> &[Fragment] {
        &self.fragments
    }
}

/// The grouped view `plan` was made for, opened at the source version it
/// pins. Every holder of a plan asks the same two questions: is this a
/// grouped view, and is it the view the plan names.
///
/// The pinned version is also what holds every unit to one index: indices
/// are part of the source manifest, so a rebuild lands in a later version
/// that a unit reading this one never sees, and units written on either
/// side of it still group by the same centroids.
async fn open_planned(view: &Table, plan: &GroupedRefreshPlan) -> Result<Grouped> {
    let grouped = open_grouped(view, Some(plan.source_version))
        .await?
        .ok_or_else(|| Error::InvalidInput {
            message: format!(
                "'{}' is not a grouped view keyed by {IVF_PARTITION}",
                view.name()
            ),
        })?;
    ensure_incarnation(&grouped.view_ds, Some(&plan.incarnation), view.name()).await?;
    Ok(grouped)
}

/// Compute unit `unit` of `plan` against source version `source_version`
/// and write its groups as uncommitted fragments of the view.
pub async fn write_grouped_unit(
    view: &Table,
    unit: u32,
    plan: &GroupedRefreshPlan,
) -> Result<WrittenUnit> {
    let grouped = open_planned(view, plan).await?;
    if unit >= plan.units {
        return Err(Error::InvalidInput {
            message: format!(
                "unit {unit} is not in the plan of '{}' ({} units)",
                view.name(),
                plan.units
            ),
        });
    }
    let schema: SchemaRef = Arc::new(ArrowSchema::from(grouped.view_ds.schema()));
    let rows_written = Arc::new(AtomicU64::new(0));
    let rows = unit_rows(&grouped, unit).await?;
    let stream = group_rows(&grouped, rows, schema.clone(), rows_written.clone()).await?;
    let write_txn = InsertBuilder::new(WriteDestination::Dataset(Arc::new(grouped.view_ds)))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute_uncommitted_stream(stream)
        .await?;
    let Operation::Append { fragments } = write_txn.operation else {
        return Err(Error::Runtime {
            message: "expected an append when writing a grouped unit".into(),
        });
    };
    Ok(WrittenUnit {
        unit,
        plan: plan.clone(),
        fragments,
        rows: rows_written.load(Ordering::Relaxed),
    })
}

/// The row ids the index places in partition `unit`, at most `limit` of them.
/// A segment's postings outlive its ownership -- a column rewrite attaches a
/// new file and takes the fragment out of its bitmap while leaving the rows
/// listed -- so each segment's rows are kept only while it still holds their
/// fragment. The fragment [`unit_rows`] scans them from instead is the one
/// the rewrite left them in.
async fn indexed_row_ids(grouped: &Grouped, unit: u32, limit: usize) -> Result<Vec<u64>> {
    let addresses = lance::dataset::rowids::get_row_id_index(&grouped.source).await?;
    let fragment_of = |row_id: u64| -> Result<Option<u32>> {
        Ok(match &addresses {
            Some(index) => index.get(row_id)?.map(|address| address.fragment_id()),
            None => Some(RowAddress::new_from_u64(row_id).fragment_id()),
        })
    };
    let mut row_ids = Vec::new();
    for segment in &grouped.segments.segments {
        let owned = segment.fragments.as_ref().ok_or_else(|| Error::Runtime {
            message: "the index stopped naming the fragments it covers mid-refresh".into(),
        })?;
        let mut stream = segment
            .index
            .partition_reader(unit as usize, false, &NoOpMetricsCollector)
            .await?;
        while let Some(batch) = stream.try_next().await? {
            for row_id in batch[ROW_ID].as_primitive::<UInt64Type>().values() {
                if row_ids.len() >= limit {
                    return Ok(row_ids);
                }
                if fragment_of(*row_id)?.is_some_and(|fragment| owned.contains(fragment)) {
                    row_ids.push(*row_id);
                }
            }
        }
    }
    Ok(row_ids)
}

/// The first row of `batch` that `unit` holds but `ivf_partition` does not
/// put there, if there is one. `batch` carries the key column.
fn assigned_elsewhere(grouped: &Grouped, batch: &RecordBatch, unit: u32) -> Result<Option<usize>> {
    let vectors = batch[grouped.column.as_str()].as_fixed_size_list();
    let assigned = grouped.binding.assign(vectors)?;
    Ok((0..batch.num_rows()).find(|&row| !(assigned.is_valid(row) && assigned.value(row) == unit)))
}

/// Whether the index's posting lists are `ivf_partition`'s buckets, over a
/// sample of each partition. Lance also builds an index from precomputed
/// assignments, and records nowhere that it did, so the only way to ask is
/// to recompute some of them.
async fn assigns_by_its_centroids(grouped: &Grouped) -> Result<bool> {
    let mut expected = HashMap::new();
    for unit in 0..grouped.binding.partitions {
        if expected.len() >= PROBE_ROWS {
            break;
        }
        for row_id in indexed_row_ids(grouped, unit, PROBE_PER_UNIT).await? {
            expected.insert(row_id, unit);
        }
    }
    if expected.is_empty() {
        return Ok(true);
    }
    let sample: Vec<u64> = expected.keys().copied().collect();
    let columns = [ROW_ID, grouped.column.as_str()];
    let projection = ProjectionRequest::from_columns(columns, grouped.source.schema());
    let batch = grouped.source.take_rows(&sample, projection).await?;
    let row_ids = batch[ROW_ID].as_primitive::<UInt64Type>().clone();
    let assigned = grouped
        .binding
        .assign(batch[grouped.column.as_str()].as_fixed_size_list())?;
    Ok((0..batch.num_rows()).all(|row| {
        assigned.is_valid(row) && Some(&assigned.value(row)) == expected.get(&row_ids.value(row))
    }))
}

/// The source rows of one unit: the index's rows for partition `unit` plus
/// the unindexed rows that assign to it, or for the last unit every row the
/// assigner cannot place. Each batch carries the inputs and `_rowid`.
async fn unit_rows(grouped: &Grouped, unit: u32) -> Result<Vec<RecordBatch>> {
    let mut columns: Vec<&str> = vec![ROW_ID];
    columns.extend(grouped.inputs.iter().map(String::as_str));
    if !grouped.inputs.contains(&grouped.column) {
        columns.push(&grouped.column);
    }
    let source = Arc::new(grouped.source.clone());
    let mut batches = Vec::new();
    let unassigned = unit == grouped.binding.partitions;

    if !unassigned {
        // Rows the index placed in this partition; a deleted row is dropped
        // by the take.
        let row_ids = indexed_row_ids(grouped, unit, usize::MAX).await?;
        let projection = ProjectionRequest::from_columns(&columns, source.schema());
        for chunk in row_ids.chunks(TAKE_CHUNK) {
            let batch = source.take_rows(chunk, projection.clone()).await?;
            if batch.num_rows() > 0 {
                // What the plan sampled, this unit proves: a row grouped
                // here that belongs to another key would be aggregated in
                // both units and published as two rows of one group.
                if let Some(row) = assigned_elsewhere(grouped, &batch, unit)? {
                    let row_id = batch[ROW_ID].as_primitive::<UInt64Type>().value(row);
                    return Err(Error::NotSupported {
                        message: format!(
                            "{IVF_PARTITION}({}): the index lists row {row_id} under \
                             partition {unit}, which it does not assign to; this view \
                             is refreshed in one pass",
                            grouped.column
                        ),
                    });
                }
                batches.push(in_column_order(&batch, &columns)?);
            }
        }
    }

    // Rows the index has not seen assign here; the last unit also takes the
    // rows no assignment places, from every fragment.
    let mut fragments = Vec::new();
    for fragment in source.get_fragments() {
        let unindexed = grouped
            .segments
            .is_unindexed(fragment.id() as u64)
            .ok_or_else(|| Error::Runtime {
                message: "the index stopped naming the fragments it covers mid-refresh".into(),
            })?;
        if unassigned || unindexed {
            fragments.push(fragment.metadata().clone());
        }
    }
    if !fragments.is_empty() {
        let mut scanner = source.scan();
        scanner.with_fragments(fragments);
        scanner.with_row_id();
        scanner.project(&columns)?;
        let mut stream = scanner.try_into_stream().await?;
        while let Some(batch) = stream.try_next().await? {
            let vectors = batch[grouped.column.as_str()].as_fixed_size_list();
            let assigned = grouped.binding.assign(vectors)?;
            let keep: arrow_array::BooleanArray = (0..batch.num_rows())
                .map(|row| {
                    Some(if unassigned {
                        assigned.is_null(row)
                    } else {
                        assigned.is_valid(row) && assigned.value(row) == unit
                    })
                })
                .collect();
            let kept = arrow_select::filter::filter_record_batch(&batch, &keep)?;
            if kept.num_rows() > 0 {
                batches.push(in_column_order(&kept, &columns)?);
            }
        }
    }
    Ok(batches)
}

/// `batch` with its columns in `columns` order: the take and the scan place
/// `_rowid` differently, and the query stream coalesces by position.
fn in_column_order(batch: &RecordBatch, columns: &[&str]) -> Result<RecordBatch> {
    let indices = columns
        .iter()
        .map(|column| batch.schema().index_of(column))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(batch.project(&indices)?)
}

/// The grouped query over `rows`, in the view's schema.
async fn group_rows(
    grouped: &Grouped,
    rows: Vec<RecordBatch>,
    schema: SchemaRef,
    rows_written: Arc<AtomicU64>,
) -> Result<SendableRecordBatchStream> {
    let input_schema = match rows.first() {
        Some(batch) => batch.schema(),
        None => {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::empty(),
            )));
        }
    };
    let rows: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        input_schema,
        futures::stream::iter(rows.into_iter().map(Ok)),
    ));
    let state = grouped::session(grouped::bind(&grouped.source, &grouped.definition).await?);
    let ctx = SessionContext::new_with_state(state.clone());
    let source = ctx.read_one_shot(rows)?.into_view();
    let planned = grouped::logical_plan(
        &state,
        provider_as_source(source),
        &grouped.definition,
        // Neither the indexed take nor the fragment scan filters: the rows
        // reach this query raw, so the predicate belongs in it.
        true,
    )?;
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

/// Replace the view's rows with the fragments every unit wrote, in one
/// commit, and record `plan.source_version` as what the view reflects.
///
/// The commit is refused if the view moved since the plan: a refresh that
/// landed meanwhile replaced rows the units never saw. Concurrent refreshes
/// of one view are serialized by the caller, as for a single-pass rebuild;
/// this check catches the ones that slipped through.
pub async fn commit_grouped_refresh(
    view: &Table,
    plan: &GroupedRefreshPlan,
    units: Vec<WrittenUnit>,
    expected_incarnation: Option<&str>,
) -> Result<RefreshMaterializedViewResult> {
    let view_native = view.as_native().ok_or_else(|| Error::NotSupported {
        message: "materialized views are supported only on local tables".into(),
    })?;
    let lock = refresh_lock(view_native.dataset.get().await?.uri());
    let _guard = lock.lock().await;
    if let Some(expected) = expected_incarnation
        && expected != plan.incarnation
    {
        return Err(Error::Runtime {
            message: format!(
                "the refresh of '{}' was requested for another incarnation than the one \
                 its units were planned against",
                view.name()
            ),
        });
    }
    let grouped = open_planned(view, plan).await?;
    let source_ts = grouped.source.manifest.timestamp_nanos;

    let ds = Arc::new(grouped.view_ds);
    if ds.version().version != plan.view_version {
        return Err(Error::Runtime {
            message: format!(
                "'{}' moved from version {} to {} while its units were written; the \
                 refresh is unrecorded and must be replanned",
                view.name(),
                plan.view_version,
                ds.version().version
            ),
        });
    }
    let units = complete_set(view.name(), plan, units)?;
    let removed_fragment_ids: Vec<u64> = ds.get_fragments().iter().map(|f| f.id() as u64).collect();
    // Fragment ids are assigned at commit; the units' fragments all carry 0.
    let rows_written = units.iter().map(|unit| unit.rows).sum();
    let new_fragments = units.into_iter().flat_map(|unit| unit.fragments).collect();
    let transaction = Transaction::new(
        plan.view_version,
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
    );
    #[cfg(test)]
    super::refresh::tests::hold_before_publish(ds.uri()).await;
    let committed = CommitBuilder::new(WriteDestination::Dataset(ds))
        .execute(transaction)
        .await?;
    // The version check above is not the fence: lance rebases this `Update`
    // over a concurrent append rather than refusing it, which would leave
    // the appended rows in a view certified as the units' whole output.
    if committed.version().version != plan.view_version + 1 {
        return Err(Error::Runtime {
            message: format!(
                "a concurrent commit raced the refresh of '{}' (view version {}); the \
                 refresh is unrecorded and must be replanned",
                view.name(),
                committed.version().version
            ),
        });
    }
    let version = stamp_watermark(
        view_native,
        committed,
        plan.source_version,
        source_ts,
        Some(&grouped.definition),
        expected_incarnation,
    )
    .await?;
    Ok(RefreshMaterializedViewResult {
        mode: RefreshMode::Rebuild,
        rows_written,
        source_version: plan.source_version,
        version,
    })
}

/// The units in plan order, exactly one per unit of `plan`. The commit
/// publishes them as the view's whole contents, so a missing, repeated or
/// foreign result would certify a view nobody computed.
fn complete_set(
    view: &str,
    plan: &GroupedRefreshPlan,
    units: Vec<WrittenUnit>,
) -> Result<Vec<WrittenUnit>> {
    let mut ordered: Vec<Option<WrittenUnit>> = vec![None; plan.units as usize];
    for written in units {
        if &written.plan != plan {
            return Err(Error::InvalidInput {
                message: format!(
                    "a unit of another plan of '{view}' was offered to this refresh; the \
                     refresh is unrecorded and must be replanned"
                ),
            });
        }
        let slot = ordered
            .get_mut(written.unit as usize)
            .ok_or_else(|| Error::InvalidInput {
                message: format!(
                    "unit {} is not in the plan of '{view}' ({} units)",
                    written.unit, plan.units
                ),
            })?;
        if slot.is_some() {
            return Err(Error::InvalidInput {
                message: format!("unit {} of '{view}' was written twice", written.unit),
            });
        }
        // A unit holds one key, so it writes that key's one group.
        if written.rows > 1 {
            return Err(Error::InvalidInput {
                message: format!(
                    "unit {} of '{view}' wrote {} groups; the split did not match the \
                     grouping and the refresh must be replanned",
                    written.unit, written.rows
                ),
            });
        }
        *slot = Some(written);
    }
    ordered
        .into_iter()
        .enumerate()
        .map(|(unit, written)| {
            written.ok_or_else(|| Error::InvalidInput {
                message: format!(
                    "unit {unit} of '{view}' is missing; the refresh holds {} units and \
                     publishes all of them or none",
                    plan.units
                ),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::refresh::tests::{clustered_source, declare, rows};
    use super::*;
    use crate::connect;
    use crate::index::Index;
    use crate::index::vector::IvfFlatIndexBuilder;
    use arrow_array::types::Float32Type;
    use arrow_array::{ArrayRef, FixedSizeListArray, Int32Array, RecordBatchIterator};
    use lance::index::DatasetIndexExt;
    use lance::index::vector::VectorIndexParams;
    use lance_index::IndexType;
    use lance_index::vector::ivf::IvfBuildParams;

    const BUCKETS: &str = "SELECT ivf_partition(vec) AS bucket, count(*) AS n, min(id) AS lo, \
         max(id) AS hi FROM images GROUP BY ivf_partition(vec)";

    /// The clustered source under a two-partition cosine index, then edited
    /// behind the index: a row deleted from each cluster, a row appended to
    /// each, and one zero vector cosine cannot place.
    async fn edited_source(conn: &crate::Connection) -> Table {
        let source = clustered_source(conn).await;
        source
            .create_index(
                &["vec"],
                Index::IvfFlat(
                    IvfFlatIndexBuilder::default()
                        .num_partitions(2)
                        .distance_type(crate::DistanceType::Cosine),
                ),
            )
            .execute()
            .await
            .unwrap();
        source.delete("id IN (3, 13)").await.unwrap();
        let appended = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            [
                Some(vec![Some(1.5), Some(0.0)]),
                Some(vec![Some(101.5), Some(100.0)]),
                Some(vec![Some(0.0), Some(0.0)]),
            ],
            2,
        );
        source
            .add(
                RecordBatch::try_from_iter(vec![
                    (
                        "id",
                        Arc::new(Int32Array::from(vec![20, 21, 22])) as ArrayRef,
                    ),
                    ("vec", Arc::new(appended) as ArrayRef),
                ])
                .unwrap(),
            )
            .execute()
            .await
            .unwrap();
        source
    }

    async fn refresh_in_units(
        view: &Table,
        expected_incarnation: Option<&str>,
    ) -> Result<(
        GroupedRefreshPlan,
        Vec<WrittenUnit>,
        RefreshMaterializedViewResult,
    )> {
        let plan = plan_grouped_refresh(view, None)
            .await?
            .expect("an ivf-keyed view");
        let mut units = Vec::new();
        for unit in 0..plan.units {
            units.push(write_grouped_unit(view, unit, &plan).await?);
        }
        let result =
            commit_grouped_refresh(view, &plan, units.clone(), expected_incarnation).await?;
        Ok((plan, units, result))
    }

    /// Every partition's unit reads its indexed rows plus the appended rows
    /// that assign to it, the deleted rows are gone, and the last unit holds
    /// the unplaceable row: together they are the single-pass result.
    #[tokio::test]
    async fn units_compose_to_the_single_pass_result() {
        let conn = connect("memory://").execute().await.unwrap();
        let source = edited_source(&conn).await;
        let single = declare(&source, "single", BUCKETS).await.unwrap();
        single.refresh().execute().await.unwrap();
        let expected = rows(single.table(), &["bucket", "n", "lo", "hi"]).await;
        assert_eq!(expected.len(), 3, "{expected:?}");
        assert!(expected.iter().any(|row| row == " 1 22 22"), "{expected:?}");

        let view = declare(&source, "buckets", BUCKETS).await.unwrap();
        let (plan, units, result) = refresh_in_units(view.table(), None).await.unwrap();
        assert_eq!(plan.units, 3, "two partitions plus the unplaceable rows");
        assert_eq!(
            rows(view.table(), &["bucket", "n", "lo", "hi"]).await,
            expected
        );
        assert_eq!(units.iter().map(|u| u.rows).sum::<u64>(), 3);
        assert_eq!(units.iter().map(|u| u.rows).collect::<Vec<_>>(), [1, 1, 1]);
        assert_eq!(result.rows_written, 3);
        assert_eq!(result.source_version, plan.source_version);
        assert_eq!(view.table().version().await.unwrap(), result.version);

        // The commit is a refresh: the view reflects the source version it
        // read and a second pass finds nothing to do.
        let again = view.refresh().execute().await.unwrap();
        assert_eq!(again.rows_written, 0, "{again:?}");
        assert_eq!(
            rows(view.table(), &["bucket", "n", "lo", "hi"]).await,
            expected
        );
    }

    /// A second refresh in units replaces the first one's rows rather than
    /// appending to them.
    #[tokio::test]
    async fn a_refresh_in_units_replaces_the_previous_rows() {
        let conn = connect("memory://").execute().await.unwrap();
        let source = edited_source(&conn).await;
        let view = declare(&source, "buckets", BUCKETS).await.unwrap();
        refresh_in_units(view.table(), None).await.unwrap();
        source.delete("id = 22").await.unwrap();
        refresh_in_units(view.table(), None).await.unwrap();
        let mut groups = rows(view.table(), &["n", "lo", "hi"]).await;
        groups.sort();
        assert_eq!(groups, ["10 0 20", "10 10 21"]);
    }

    #[tokio::test]
    async fn a_view_not_keyed_by_ivf_partition_has_no_units() {
        let conn = connect("memory://").execute().await.unwrap();
        let source = clustered_source(&conn).await;
        let view = declare(
            &source,
            "by_id",
            "SELECT id % 2 AS k, count(*) AS n FROM images GROUP BY id % 2",
        )
        .await
        .unwrap();
        assert_eq!(
            plan_grouped_refresh(view.table(), None).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn a_unit_outside_the_plan_is_refused() {
        let conn = connect("memory://").execute().await.unwrap();
        let source = edited_source(&conn).await;
        let view = declare(&source, "buckets", BUCKETS).await.unwrap();
        let plan = plan_grouped_refresh(view.table(), None)
            .await
            .unwrap()
            .unwrap();
        let err = write_grouped_unit(view.table(), plan.units, &plan)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not in the plan"), "{err}");
    }

    /// A commit that lands on a view someone else refreshed meanwhile is
    /// refused rather than recorded over their rows.
    #[tokio::test]
    async fn a_raced_commit_is_refused() {
        let conn = connect("memory://").execute().await.unwrap();
        let source = edited_source(&conn).await;
        let view = declare(&source, "buckets", BUCKETS).await.unwrap();
        let plan = plan_grouped_refresh(view.table(), None)
            .await
            .unwrap()
            .unwrap();
        let units = vec![write_grouped_unit(view.table(), 0, &plan).await.unwrap()];
        view.refresh().execute().await.unwrap();
        let before = rows(view.table(), &["bucket", "n", "lo", "hi"]).await;
        let raced = commit_grouped_refresh(view.table(), &plan, units, None).await;
        assert!(raced.is_err(), "{raced:?}");
        assert_eq!(
            rows(view.table(), &["bucket", "n", "lo", "hi"]).await,
            before
        );
    }

    /// A view with a predicate holds the rows it selects however it is
    /// refreshed: the units read their rows through the index and by
    /// scanning fragments, neither of which filters, so the predicate has
    /// to be in the grouped query itself.
    #[tokio::test]
    async fn a_filtered_view_refreshes_in_units_like_a_single_pass() {
        const FILTERED: &str = "SELECT ivf_partition(vec) AS bucket, count(*) AS n, \
             min(id) AS lo, max(id) AS hi FROM images WHERE id < 5 \
             GROUP BY ivf_partition(vec)";
        let conn = connect("memory://").execute().await.unwrap();
        let source = edited_source(&conn).await;
        let single = declare(&source, "single", FILTERED).await.unwrap();
        single.refresh().execute().await.unwrap();
        let expected = rows(single.table(), &["bucket", "n", "lo", "hi"]).await;

        let view = declare(&source, "buckets", FILTERED).await.unwrap();
        refresh_in_units(view.table(), None).await.unwrap();
        assert_eq!(
            rows(view.table(), &["bucket", "n", "lo", "hi"]).await,
            expected
        );
    }

    /// The commit publishes its units as the view's whole contents, so it
    /// takes the plan's units exactly once each and nothing else.
    #[tokio::test]
    async fn only_a_complete_set_of_this_plan_s_units_is_published() {
        let conn = connect("memory://").execute().await.unwrap();
        let source = edited_source(&conn).await;
        let view = declare(&source, "buckets", BUCKETS).await.unwrap();
        let plan = plan_grouped_refresh(view.table(), None)
            .await
            .unwrap()
            .unwrap();
        let mut units = Vec::new();
        for unit in 0..plan.units {
            units.push(write_grouped_unit(view.table(), unit, &plan).await.unwrap());
        }

        let missing = commit_grouped_refresh(view.table(), &plan, units[..2].to_vec(), None).await;
        assert!(
            missing.unwrap_err().to_string().contains("is missing"),
            "a partial set was published"
        );
        let mut twice = units.clone();
        twice[1] = units[0].clone();
        let repeated = commit_grouped_refresh(view.table(), &plan, twice, None).await;
        assert!(
            repeated.unwrap_err().to_string().contains("written twice"),
            "a repeated unit was published"
        );
        let mut foreign = units.clone();
        foreign[0].plan.source_version += 1;
        let stale = commit_grouped_refresh(view.table(), &plan, foreign, None).await;
        assert!(
            stale.unwrap_err().to_string().contains("another plan"),
            "a unit of another plan was published"
        );
        assert_eq!(rows(view.table(), &["bucket"]).await, Vec::<String>::new());

        // The complete set still lands.
        commit_grouped_refresh(view.table(), &plan, units, None)
            .await
            .unwrap();
        assert_eq!(rows(view.table(), &["bucket"]).await.len(), 3);
    }

    /// A column rewrite attaches a new file to the row's fragment and takes
    /// that fragment out of the index segment's bitmap, but the segment's
    /// postings still name its rows. Taking them anyway, while the fragment
    /// is also scanned as unindexed, counts those rows twice and buckets the
    /// stale copy by the value the rewrite replaced.
    #[tokio::test]
    async fn a_rewritten_fragment_is_read_once() {
        use lance::dataset::{
            MergeInsertBuilder, MergeInsertWriteMode, WhenMatched, WhenNotMatched,
        };

        let conn = connect("memory://").execute().await.unwrap();
        // A column the rewrite leaves alone: RewriteColumns is only for a
        // source that covers some of the row, not all of it.
        let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            (0..20).map(|i| {
                let base = if i < 10 { 0.0 } else { 100.0 };
                Some(vec![Some(base + 1.0 + i as f32 * 0.1), Some(base)])
            }),
            2,
        );
        let source = conn
            .create_table(
                "images",
                RecordBatch::try_from_iter(vec![
                    (
                        "id",
                        Arc::new(Int32Array::from_iter_values(0..20)) as ArrayRef,
                    ),
                    ("vec", Arc::new(vectors) as ArrayRef),
                    (
                        "tag",
                        Arc::new(Int32Array::from_iter_values((0..20).map(|i| i * 10))) as ArrayRef,
                    ),
                ])
                .unwrap(),
            )
            .write_options(super::super::tests::stable_row_ids())
            .execute()
            .await
            .unwrap();
        source
            .create_index(
                &["vec"],
                Index::IvfFlat(IvfFlatIndexBuilder::default().num_partitions(2)),
            )
            .execute()
            .await
            .unwrap();

        // Row 0 keeps its id and its fragment; its vector is replaced in a
        // file the index segment does not cover.
        let batch = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![0])) as ArrayRef),
            (
                "vec",
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        [Some(vec![Some(101.0), Some(100.0)])],
                        2,
                    ),
                ) as ArrayRef,
            ),
        ])
        .unwrap();
        let schema = batch.schema();
        let dataset = source.as_native().unwrap().dataset.get().await.unwrap();
        let mut builder =
            MergeInsertBuilder::try_new(Arc::new(dataset.as_ref().clone()), vec!["id".to_string()])
                .unwrap();
        builder
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::DoNothing)
            .write_mode(MergeInsertWriteMode::RewriteColumns);
        builder
            .try_build()
            .unwrap()
            .execute_reader(arrow_array::RecordBatchIterator::new([Ok(batch)], schema))
            .await
            .unwrap();

        let single = declare(&source, "single", BUCKETS).await.unwrap();
        single.refresh().execute().await.unwrap();
        let expected = rows(single.table(), &["bucket", "n", "lo", "hi"]).await;

        let view = declare(&source, "buckets", BUCKETS).await.unwrap();
        refresh_in_units(view.table(), None).await.unwrap();
        assert_eq!(
            rows(view.table(), &["bucket", "n", "lo", "hi"]).await,
            expected,
            "the rewritten row was counted twice or bucketed by its old vector"
        );
    }

    /// Units written on either side of an index rebuild still group by one
    /// model: the plan pins a source version, indices live in the source
    /// manifest, and a rebuild lands in a later version the units never
    /// read. Without that, two units of one plan could assign their rows by
    /// different centroids.
    #[tokio::test]
    async fn a_rebuilt_index_does_not_reach_the_units_of_a_plan() {
        let conn = connect("memory://").execute().await.unwrap();
        let source = edited_source(&conn).await;
        let view = declare(&source, "buckets", BUCKETS).await.unwrap();
        let plan = plan_grouped_refresh(view.table(), None)
            .await
            .unwrap()
            .unwrap();
        let first = write_grouped_unit(view.table(), 0, &plan).await.unwrap();

        source
            .create_index(
                &["vec"],
                Index::IvfFlat(
                    IvfFlatIndexBuilder::default()
                        .num_partitions(2)
                        .distance_type(crate::DistanceType::Cosine),
                ),
            )
            .replace(true)
            .execute()
            .await
            .unwrap();
        let rebuilt = plan_grouped_refresh(view.table(), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rebuilt.units, plan.units, "same shape");
        assert!(
            rebuilt.source_version > plan.source_version,
            "the rebuild is a later source version"
        );

        // The remaining units read the version the plan pinned, so they are
        // grouped by the index that version holds, not the new one.
        let mut units = vec![first];
        for unit in 1..plan.units {
            units.push(write_grouped_unit(view.table(), unit, &plan).await.unwrap());
        }
        commit_grouped_refresh(view.table(), &plan, units, None)
            .await
            .unwrap();
        assert_eq!(
            rows(view.table(), &["bucket", "n", "lo", "hi"]).await.len(),
            3
        );
    }

    /// Two views of the same shape over the same source reach the same plan
    /// counters, so the results of one must not satisfy the other: they are
    /// fragments written into a different dataset.
    #[tokio::test]
    async fn units_written_for_another_view_are_refused() {
        let conn = connect("memory://").execute().await.unwrap();
        let source = edited_source(&conn).await;
        let mine = declare(&source, "buckets", BUCKETS).await.unwrap();
        let theirs = declare(&source, "twin", BUCKETS).await.unwrap();
        let my_plan = plan_grouped_refresh(mine.table(), None)
            .await
            .unwrap()
            .unwrap();
        let their_plan = plan_grouped_refresh(theirs.table(), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (my_plan.units, my_plan.view_version, my_plan.source_version),
            (
                their_plan.units,
                their_plan.view_version,
                their_plan.source_version
            ),
            "the counters alone do not tell these views apart"
        );

        let mut mine_units = Vec::new();
        for unit in 0..my_plan.units {
            mine_units.push(
                write_grouped_unit(mine.table(), unit, &my_plan)
                    .await
                    .unwrap(),
            );
        }
        let misrouted =
            commit_grouped_refresh(theirs.table(), &their_plan, mine_units.clone(), None).await;
        assert!(
            misrouted.unwrap_err().to_string().contains("another plan"),
            "another view's units were published"
        );
        assert_eq!(
            rows(theirs.table(), &["bucket"]).await,
            Vec::<String>::new()
        );

        // And a unit cannot be written for a view the plan does not name.
        let foreign = write_grouped_unit(theirs.table(), 0, &my_plan).await;
        assert!(
            foreign
                .unwrap_err()
                .to_string()
                .contains("not the incarnation"),
            "a unit was written under another view's plan"
        );
    }

    /// A write that lands on the view between the plan check and the commit
    /// is not a conflict lance refuses -- the update rebases over it -- so
    /// the publication checks the generation it actually landed on.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_write_racing_the_publication_is_refused() {
        use super::super::refresh::tests::{
            DRIFT_LOCK, DRIFT_PLANNED, DRIFT_RELEASED, DRIFT_TARGET,
        };
        let _serial = DRIFT_LOCK.lock().await;
        let conn = connect("memory://").execute().await.unwrap();
        let source = edited_source(&conn).await;
        // Its own name: memory:// uris repeat across tests, so a view called
        // like its siblings would arm the hook for one of their commits.
        let view = declare(&source, "racing_buckets", BUCKETS).await.unwrap();
        let plan = plan_grouped_refresh(view.table(), None)
            .await
            .unwrap()
            .unwrap();
        let mut units = Vec::new();
        for unit in 0..plan.units {
            units.push(write_grouped_unit(view.table(), unit, &plan).await.unwrap());
        }
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

        let publishing = {
            let table = view.table().clone();
            let plan = plan.clone();
            tokio::spawn(async move { commit_grouped_refresh(&table, &plan, units, None).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(30), DRIFT_PLANNED.notified())
            .await
            .expect("the commit never reached the publication boundary");
        view.table()
            .add(
                RecordBatch::try_from_iter(vec![
                    ("bucket", Arc::new(Int32Array::from(vec![7])) as ArrayRef),
                    ("n", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
                    ("lo", Arc::new(Int32Array::from(vec![0])) as ArrayRef),
                    ("hi", Arc::new(Int32Array::from(vec![0])) as ArrayRef),
                    (
                        super::super::SOURCE_ROW_ID_COLUMN,
                        Arc::new(arrow_array::UInt64Array::from(vec![0u64])) as ArrayRef,
                    ),
                ])
                .unwrap(),
            )
            .execute()
            .await
            .unwrap();
        DRIFT_RELEASED.notify_one();

        let err = publishing.await.unwrap().unwrap_err();
        assert!(err.to_string().contains("raced the refresh"), "{err}");
    }

    #[test]
    fn the_key_column_is_read_off_a_single_ivf_key() {
        let mut definition = MaterializedViewDefinition::from_sql(
            "SELECT ivf_partition(vec) AS b, count(*) AS n FROM t GROUP BY ivf_partition(vec)",
        )
        .unwrap();
        assert_eq!(ivf_key_column(&definition).as_deref(), Some("vec"));
        definition.group_by = vec!["k".to_string()];
        assert_eq!(ivf_key_column(&definition), None);
        definition.group_by = vec!["ivf_partition(vec)".to_string(), "k".to_string()];
        assert_eq!(ivf_key_column(&definition), None);
    }

    /// A source whose index does not assign by its own centroids: lance
    /// takes precomputed partitions, so row 1 is stored under partition 1
    /// and `ivf_partition` puts it in 0.
    async fn precomputed_source(conn: &crate::Connection, dir: &std::path::Path) -> Table {
        let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            [
                Some(vec![Some(1.0), Some(0.0)]),
                Some(vec![Some(2.0), Some(0.0)]),
                Some(vec![Some(100.0), Some(100.0)]),
            ],
            2,
        );
        let source = conn
            .create_table(
                "images",
                RecordBatch::try_from_iter(vec![
                    ("id", Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef),
                    ("vec", Arc::new(vectors) as ArrayRef),
                ])
                .unwrap(),
            )
            .write_options(crate::materialized_view::tests::stable_row_ids())
            .execute()
            .await
            .unwrap();

        let assignments = arrow_array::record_batch!(
            ("row_id", UInt64, [0, 1, 2]),
            ("partition", UInt32, [0, 1, 1])
        )
        .unwrap();
        let file = dir.join("partitions.lance");
        let file = file.to_str().unwrap();
        lance::Dataset::write(
            RecordBatchIterator::new(vec![Ok(assignments.clone())], assignments.schema()),
            file,
            None,
        )
        .await
        .unwrap();

        let centroids = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            [
                Some(vec![Some(0.0), Some(0.0)]),
                Some(vec![Some(100.0), Some(100.0)]),
            ],
            2,
        );
        let mut ivf = IvfBuildParams::try_with_centroids(2, Arc::new(centroids)).unwrap();
        ivf.precomputed_partitions_file = Some(file.to_string());
        let mut dataset = source
            .as_native()
            .unwrap()
            .dataset
            .get()
            .await
            .unwrap()
            .as_ref()
            .clone();
        dataset
            .create_index(
                &["vec"],
                IndexType::Vector,
                None,
                &VectorIndexParams::with_ivf_flat_params(
                    lance_linalg::distance::DistanceType::L2,
                    ivf,
                ),
                true,
            )
            .await
            .unwrap();
        conn.open_table("images").execute().await.unwrap()
    }

    async fn incarnation(view: &Table) -> String {
        view.as_native()
            .unwrap()
            .dataset
            .get()
            .await
            .unwrap()
            .schema()
            .metadata
            .get(crate::materialized_view::INCARNATION_META_KEY)
            .cloned()
            .unwrap()
    }

    /// The split is the index's posting lists and the key is
    /// `ivf_partition`. Where they disagree one group is aggregated in two
    /// units and published twice, so such a view has no units at all.
    #[tokio::test]
    async fn an_index_that_does_not_assign_by_its_centroids_has_no_units() {
        let dir = tempfile::tempdir().unwrap();
        let conn = connect("memory://").execute().await.unwrap();
        let source = precomputed_source(&conn, dir.path()).await;
        let view = declare(&source, "buckets", BUCKETS).await.unwrap();
        assert_eq!(
            plan_grouped_refresh(view.table(), None).await.unwrap(),
            None
        );
        view.refresh().execute().await.unwrap();
        assert_eq!(
            rows(view.table(), &["bucket", "n", "lo", "hi"]).await,
            ["0 2 0 1", "1 1 2 2"]
        );
    }

    /// The publication is the last place the split can be caught: a unit
    /// that holds one key writes one group, and a set that says otherwise is
    /// not published however it was produced.
    #[tokio::test]
    async fn a_unit_that_wrote_more_than_one_group_is_not_published() {
        let conn = connect("memory://").execute().await.unwrap();
        let source = edited_source(&conn).await;
        let view = declare(&source, "buckets", BUCKETS).await.unwrap();
        let plan = plan_grouped_refresh(view.table(), None)
            .await
            .unwrap()
            .unwrap();
        let mut units = Vec::new();
        for unit in 0..plan.units {
            units.push(write_grouped_unit(view.table(), unit, &plan).await.unwrap());
        }
        units[1].rows = 2;
        let err = commit_grouped_refresh(view.table(), &plan, units, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("wrote 2 groups"), "{err}");
        assert_eq!(rows(view.table(), &["bucket"]).await, Vec::<String>::new());
    }

    /// The plan reads a sample, so the unit proves the rest: it holds only
    /// rows that assign to it, or it refuses rather than write a group
    /// another unit holds too.
    #[tokio::test]
    async fn a_unit_holding_a_row_that_assigns_elsewhere_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let conn = connect("memory://").execute().await.unwrap();
        let source = precomputed_source(&conn, dir.path()).await;
        let view = declare(&source, "buckets", BUCKETS).await.unwrap();
        let plan = GroupedRefreshPlan {
            units: 3,
            source_version: source.version().await.unwrap(),
            view_version: view.table().version().await.unwrap(),
            incarnation: incarnation(view.table()).await,
        };
        assert_eq!(
            write_grouped_unit(view.table(), 0, &plan)
                .await
                .unwrap()
                .rows,
            1
        );
        let err = write_grouped_unit(view.table(), 1, &plan)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("does not assign to"), "{err}");
    }
}
