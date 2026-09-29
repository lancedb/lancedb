// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Two dependent waves in the ordinary refresh fleet: index partition selection,
//! then source fragment materialization. Only the latter publishes data files.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::{SessionConfig, SessionContext, col};
use datafusion_common::DataFusionError;
use datafusion_execution::{
    SendableRecordBatchStream, TaskContext, runtime_env::RuntimeEnvBuilder,
};
use datafusion_physical_plan::{ExecutionPlan, stream::RecordBatchStreamAdapter};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::rowids::{get_row_id_index, load_row_id_sequence};
use lance::dataset::scanner::{RowAddrMask, RowAddrTreeMap};
use lance::dataset::transaction::Operation;
use lance::dataset::{InsertBuilder, WriteDestination, WriteMode, WriteParams};
use lance_core::datatypes::BlobHandling;
use lance_core::utils::address::RowAddress;
use lance_datafusion::exec::{OneShotExec, SessionContextExt};
use lance_table::format::Fragment;
use roaring::{RoaringBitmap, RoaringTreemap};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::duplicate_pairs::{self, DuplicatePairsRefreshPlan, VectorSourceKind};
use super::{GroupedRefreshPlan, RefreshMaterializedViewResult};
use crate::table::Table;
use crate::table::datafusion::udtf::duplicate_pairs::{DuplicatePairsExec, plan_duplicate_pairs};
use crate::{Error, Result};

const POLICY_VERSION: u32 = 1;
const SORT_MEMORY_BYTES: usize = 256 * 1024 * 1024;
const MATERIALIZE_BATCH_ROWS: usize = 1024;
const MASK_FILE: &str = "dedup.bitmap";
const MASK_MAGIC: &[u8] = b"LANCEDB_DEDUP_1";

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidInput {
        message: message.into(),
    }
}
fn io_error(error: std::io::Error) -> Error {
    invalid(format!("invalid dedup bitmap: {error}"))
}

/// Immutable snapshot, partition tasks and source fragments for both waves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorDedupPlan {
    pub snapshot: GroupedRefreshPlan,
    native: DuplicatePairsRefreshPlan,
    source_fragments: Vec<u32>,
    policy_version: u32,
}
impl VectorDedupPlan {
    pub(super) fn dependencies(&self) -> u32 {
        self.native.snapshot.units
    }
    fn identity(&self) -> Result<String> {
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(self).map_err(|e| invalid(e.to_string()))?)
        ))
    }
}

/// Small receipt for a durable selection mask. Row IDs and pairs are not stored
/// in the host's task registry. UUID directories use ordinary index-file GC.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MaskFile {
    source_fragment: u32,
    object_id: Uuid,
    bytes: usize,
    digest: String,
}

/// A selection task produces masks; a materialization task produces data files.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WrittenDedupUnit {
    unit: u32,
    plan_id: String,
    masks: Vec<MaskFile>,
    pub(super) fragments: Vec<Fragment>,
    pub(super) rows: u64,
}

pub(super) async fn plan_refresh(view: &Table, pinned: Option<u64>) -> Result<VectorDedupPlan> {
    let native = duplicate_pairs::plan_refresh(view, pinned).await?;
    let (_, _, source) = duplicate_pairs::open_planned(view, &native).await?;
    let source_fragments = source
        .get_fragments()
        .iter()
        .map(|f| f.id() as u32)
        .collect::<Vec<_>>();
    let mut snapshot = native.snapshot.clone();
    snapshot.units = snapshot
        .units
        .checked_add(
            u32::try_from(source_fragments.len())
                .map_err(|_| invalid("too many source fragments"))?,
        )
        .ok_or_else(|| invalid("too many dedup refresh units"))?;
    Ok(VectorDedupPlan {
        snapshot,
        native,
        source_fragments,
        policy_version: POLICY_VERSION,
    })
}

async fn open(
    view: &Table,
    plan: &VectorDedupPlan,
) -> Result<(Dataset, super::MaterializedViewDefinition, Arc<Dataset>)> {
    let (view_ds, definition, source) = duplicate_pairs::open_planned(view, &plan.native).await?;
    let mut snapshot = plan.native.snapshot.clone();
    snapshot.units = snapshot
        .units
        .checked_add(
            u32::try_from(plan.source_fragments.len())
                .map_err(|_| invalid("too many dedup fragments"))?,
        )
        .ok_or_else(|| invalid("too many dedup units"))?;
    if plan.policy_version != POLICY_VERSION
        || snapshot != plan.snapshot
        || definition.vector_source.as_ref().map(|s| s.kind) != Some(VectorSourceKind::Dedup)
        || source
            .get_fragments()
            .iter()
            .map(|f| f.id() as u32)
            .collect::<Vec<_>>()
            != plan.source_fragments
    {
        return Err(invalid("dedup task belongs to another snapshot or policy"));
    }
    Ok((view_ds, definition, source))
}

fn edge_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt64, false),
        Field::new("b", DataType::UInt64, false),
    ]))
}

/// Sorting canonical edges makes the first unremoved endpoint the smallest
/// unassigned representative. The sort spills; only vertex masks stay in RAM.
async fn select_representatives(
    stream: SendableRecordBatchStream,
    memory: usize,
) -> Result<(RoaringTreemap, RoaringTreemap)> {
    let schema = edge_schema();
    let batch_schema = schema.clone();
    let canonical = stream
        .map_ok(move |batch| {
            let a = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("native row id");
            let b = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("native row id");
            RecordBatch::try_new(
                batch_schema.clone(),
                vec![
                    Arc::new(UInt64Array::from_iter_values(
                        a.values().iter().zip(b.values()).map(|(&a, &b)| a.min(b)),
                    )),
                    Arc::new(UInt64Array::from_iter_values(
                        a.values().iter().zip(b.values()).map(|(&a, &b)| a.max(b)),
                    )),
                ],
            )
            .map_err(DataFusionError::from)
        })
        .and_then(futures::future::ready);
    let canonical = Box::pin(RecordBatchStreamAdapter::new(schema, canonical));
    let mut config = SessionConfig::default().with_batch_size(MATERIALIZE_BATCH_ROWS);
    // Leave enough room to merge spilled runs while the final in-memory run
    // is still resident. Small batches also bound each merge input buffer.
    config.options_mut().execution.sort_spill_reservation_bytes = memory / 2;
    let ctx = SessionContext::new_with_config_rt(
        config,
        RuntimeEnvBuilder::new()
            .with_memory_limit(memory, 1.0)
            .build_arc()?,
    );
    let mut sorted = ctx
        .read_one_shot(canonical)?
        .sort_by(vec![col("a"), col("b")])?
        .execute_stream()
        .await?;
    let mut removed = RoaringTreemap::new();
    let mut representatives = RoaringTreemap::new();
    while let Some(batch) = sorted.try_next().await? {
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("sorted id");
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("sorted id");
        for (&a, &b) in a.values().iter().zip(b.values()) {
            if a != b && !removed.contains(a) && !removed.contains(b) {
                representatives.insert(a);
                removed.insert(b);
            }
        }
    }
    Ok((removed, representatives))
}

#[derive(Default)]
struct Masks {
    removed: RoaringBitmap,
    representatives: RoaringBitmap,
}

async fn persist_masks(
    view: &Dataset,
    source: &Dataset,
    removed: RoaringTreemap,
    representatives: RoaringTreemap,
) -> Result<Vec<MaskFile>> {
    let row_index = get_row_id_index(source).await?;
    let mut masks: BTreeMap<u32, Masks> = BTreeMap::new();
    for (ids, is_removed) in [(removed, true), (representatives, false)] {
        for id in ids {
            let address = match &row_index {
                Some(index) => index
                    .get(id)?
                    .ok_or_else(|| invalid("pair row id missing from pinned source"))?,
                None => RowAddress::new_from_u64(id),
            };
            let mask = masks.entry(address.fragment_id()).or_default();
            if is_removed {
                mask.removed.insert(address.row_offset());
            } else {
                mask.representatives.insert(address.row_offset());
            }
        }
    }
    let store = view.object_store(None).await?;
    let mut files = Vec::new();
    for (source_fragment, mask) in masks {
        if source.get_fragment(source_fragment as usize).is_none() {
            return Err(invalid("pair row address is outside the pinned snapshot"));
        }
        let mut bytes = MASK_MAGIC.to_vec();
        mask.removed.serialize_into(&mut bytes).map_err(io_error)?;
        mask.representatives
            .serialize_into(&mut bytes)
            .map_err(io_error)?;
        let object_id = Uuid::new_v4();
        store
            .put(
                &view
                    .indices_dir()
                    .join(object_id.to_string())
                    .join(MASK_FILE),
                &bytes,
            )
            .await?;
        files.push(MaskFile {
            source_fragment,
            object_id,
            bytes: bytes.len(),
            digest: format!("{:x}", Sha256::digest(&bytes)),
        });
    }
    Ok(files)
}

fn validate_receipts<'a>(
    plan: &VectorDedupPlan,
    receipts: &'a [WrittenDedupUnit],
    count: u32,
) -> Result<Vec<&'a WrittenDedupUnit>> {
    let identity = plan.identity()?;
    let mut ordered = vec![None; count as usize];
    for receipt in receipts {
        if receipt.plan_id != identity {
            return Err(invalid("dedup receipt belongs to another plan"));
        }
        let slot = ordered
            .get_mut(receipt.unit as usize)
            .ok_or_else(|| invalid("unexpected dedup receipt"))?;
        if slot.is_some() {
            return Err(invalid("duplicate dedup receipt"));
        }
        if receipt.unit < plan.dependencies() {
            if !receipt.fragments.is_empty()
                || receipt.rows != 0
                || receipt
                    .masks
                    .iter()
                    .any(|m| !plan.source_fragments.contains(&m.source_fragment))
            {
                return Err(invalid("invalid selection receipt"));
            }
        } else if !receipt.masks.is_empty() {
            return Err(invalid("invalid materialization receipt"));
        }
        *slot = Some(receipt);
    }
    ordered
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| invalid("missing dedup dependency receipt"))
}

async fn load_masks(
    view: &Dataset,
    source_fragment: u32,
    dependencies: &[&WrittenDedupUnit],
) -> Result<RoaringBitmap> {
    let store = view.object_store(None).await?;
    let mut masks = Masks::default();
    for file in dependencies
        .iter()
        .flat_map(|u| &u.masks)
        .filter(|m| m.source_fragment == source_fragment)
    {
        let reader = store
            .open(
                &view
                    .indices_dir()
                    .join(file.object_id.to_string())
                    .join(MASK_FILE),
            )
            .await?;
        if reader.size().await? != file.bytes {
            return Err(invalid("dedup mask length mismatch"));
        }
        let bytes = reader.get_range(0..file.bytes).await?;
        if !bytes.starts_with(MASK_MAGIC) || format!("{:x}", Sha256::digest(&bytes)) != file.digest
        {
            return Err(invalid("dedup mask checksum mismatch"));
        }
        let mut cursor = Cursor::new(&bytes[MASK_MAGIC.len()..]);
        masks.removed |= RoaringBitmap::deserialize_from(&mut cursor).map_err(io_error)?;
        masks.representatives |= RoaringBitmap::deserialize_from(&mut cursor).map_err(io_error)?;
        if cursor.position() as usize != bytes.len() - MASK_MAGIC.len() {
            return Err(invalid("dedup mask has trailing data"));
        }
    }
    // Native index scopes normally have disjoint rows. Fail closed if an
    // overlapping scope would delete a representative used by another scope.
    if !masks.removed.is_disjoint(&masks.representatives) {
        return Err(invalid(
            "overlapping index scopes disagree on retained representatives",
        ));
    }
    Ok(masks.removed)
}

pub(super) async fn write_unit(
    view: &Table,
    unit: u32,
    plan: &VectorDedupPlan,
    dependencies: &[WrittenDedupUnit],
) -> Result<WrittenDedupUnit> {
    let (view_ds, definition, source) = open(view, plan).await?;
    let mut written = WrittenDedupUnit {
        unit,
        plan_id: plan.identity()?,
        masks: Vec::new(),
        fragments: Vec::new(),
        rows: 0,
    };
    if unit < plan.dependencies() {
        let config = definition
            .vector_source
            .as_ref()
            .expect("dedup source")
            .config()?;
        let exec = DuplicatePairsExec::try_new(
            source.clone(),
            config,
            vec![plan.native.tasks[unit as usize].clone()],
        )?;
        let stream = exec.execute(0, Arc::new(TaskContext::default()))?;
        let (removed, representatives) = select_representatives(stream, SORT_MEMORY_BYTES).await?;
        written.masks = persist_masks(&view_ds, &source, removed, representatives).await?;
        return Ok(written);
    }
    let source_fragment = *plan
        .source_fragments
        .get((unit - plan.dependencies()) as usize)
        .ok_or_else(|| invalid("dedup unit is outside the plan"))?;
    let dependencies = validate_receipts(plan, dependencies, plan.dependencies())?;
    let removed = load_masks(&view_ds, source_fragment, &dependencies).await?;
    let fragment = source
        .get_fragment(source_fragment as usize)
        .ok_or_else(|| invalid("source fragment missing"))?;
    // The scanner prefilter uses snapshot row IDs. Our persisted masks use
    // physical offsets so each writer only loads its own fragment's artifacts.
    let blocked = if source.manifest.uses_stable_row_ids() {
        let sequence = load_row_id_sequence(&source, fragment.metadata()).await?;
        if removed
            .max()
            .is_some_and(|offset| offset as u64 >= sequence.len())
        {
            return Err(invalid("dedup mask offset is outside the source fragment"));
        }
        RowAddrTreeMap::from_iter(sequence.select(removed.iter().map(|offset| offset as usize)))
    } else {
        RowAddrTreeMap::from_iter(
            removed
                .iter()
                .map(|offset| u64::from(RowAddress::new_from_parts(source_fragment, offset))),
        )
    };
    let mut scanner = source.scan();
    scanner
        .with_fragments(vec![fragment.metadata().clone()])
        .with_row_addr_prefilter(RowAddrMask::from_block(blocked))
        .blob_handling(BlobHandling::AllBinary)
        .batch_size(MATERIALIZE_BATCH_ROWS)
        .strict_batch_size(true);
    let stream = scanner.try_into_stream().await?;
    let schema = lance_io::stream::RecordBatchStream::schema(&stream);
    let rows = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let count = rows.clone();
    let stream = stream
        .map_err(|e| DataFusionError::External(Box::new(e)))
        .inspect_ok(move |batch| {
            count.fetch_add(
                batch.num_rows() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        });
    let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(schema, stream));
    // Materialized binary payloads must become the destination's logical blob
    // structs before the Lance writer encodes them. Reuse the normal add path's
    // coercions, including nulls and nested fields.
    let coerced = crate::table::datafusion::cast::cast_to_table_schema(
        Arc::new(OneShotExec::new(stream)),
        &Schema::from(view_ds.schema()),
    )?;
    let stream = coerced.execute(0, Arc::new(TaskContext::default()))?;
    let txn = InsertBuilder::new(WriteDestination::Dataset(Arc::new(view_ds)))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute_uncommitted_stream(stream)
        .await?;
    let Operation::Append { fragments } = txn.operation else {
        return Err(invalid(
            "dedup materialization did not stage append fragments",
        ));
    };
    written.fragments = fragments;
    written.rows = rows.load(std::sync::atomic::Ordering::Relaxed);
    Ok(written)
}

pub(super) async fn commit(
    view: &Table,
    plan: &VectorDedupPlan,
    units: Vec<WrittenDedupUnit>,
    expected: Option<&str>,
) -> Result<RefreshMaterializedViewResult> {
    let (_, definition, source) = open(view, plan).await?;
    let config = definition
        .vector_source
        .as_ref()
        .expect("dedup source")
        .config()?;
    if plan_duplicate_pairs(source.clone(), &config).await? != plan.native.tasks {
        return Err(invalid("dedup plan does not cover the pinned index"));
    }
    validate_receipts(plan, &units, plan.snapshot.units)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn edges(values: &[(u64, u64)], batch_size: usize) -> SendableRecordBatchStream {
        let schema = edge_schema();
        let batches = values
            .chunks(batch_size)
            .map(|edges| {
                Ok(RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt64Array::from_iter_values(edges.iter().map(|e| e.0))),
                        Arc::new(UInt64Array::from_iter_values(edges.iter().map(|e| e.1))),
                    ],
                )
                .unwrap())
            })
            .collect::<Vec<_>>();
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches),
        ))
    }

    #[tokio::test]
    async fn direct_representatives_handle_chains_orientation_and_uint64() {
        let high = u64::MAX - 2;
        let pairs = [
            (2, 1),
            (1, 0),
            (high + 2, high + 1),
            (high, high + 1),
            (1, 0),
        ];
        let (removed, representatives) =
            select_representatives(edges(&pairs, 1), SORT_MEMORY_BYTES)
                .await
                .unwrap();
        assert_eq!(removed.iter().collect::<Vec<_>>(), [1, high + 1]);
        assert_eq!(representatives.iter().collect::<Vec<_>>(), [0, high]);
    }

    #[tokio::test]
    async fn masks_reject_overlapping_representatives_and_corrupt_artifacts() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![0, 1, 2]))],
        )
        .unwrap();
        let ds = Dataset::write(
            arrow_array::RecordBatchIterator::new(vec![Ok(batch)], schema),
            dir.path().join("source.lance").to_str().unwrap(),
            None,
        )
        .await
        .unwrap();
        let first = persist_masks(
            &ds,
            &ds,
            [1].into_iter().collect(),
            [0].into_iter().collect(),
        )
        .await
        .unwrap();
        let second = persist_masks(
            &ds,
            &ds,
            [2].into_iter().collect(),
            [1].into_iter().collect(),
        )
        .await
        .unwrap();
        let receipt = |masks| WrittenDedupUnit {
            unit: 0,
            plan_id: "test".into(),
            masks,
            fragments: vec![],
            rows: 0,
        };
        let first = receipt(first);
        let second = receipt(second);
        assert_eq!(
            load_masks(&ds, 0, &[&first]).await.unwrap(),
            [1].into_iter().collect()
        );
        assert!(
            load_masks(&ds, 0, &[&first, &second])
                .await
                .unwrap_err()
                .to_string()
                .contains("overlapping")
        );
        let file = &first.masks[0];
        ds.object_store(None)
            .await
            .unwrap()
            .put(
                &ds.indices_dir()
                    .join(file.object_id.to_string())
                    .join(MASK_FILE),
                &vec![0; file.bytes],
            )
            .await
            .unwrap();
        assert!(
            load_masks(&ds, 0, &[&first])
                .await
                .unwrap_err()
                .to_string()
                .contains("checksum")
        );
    }

    #[tokio::test]
    async fn external_sort_handles_more_edges_than_its_budget() {
        let pairs = (0..262144)
            .rev()
            .map(|i| ((i + 1) as u64, i as u64))
            .collect::<Vec<_>>();
        let (removed, reps) = select_representatives(edges(&pairs, 4096), 2 * 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(removed.len(), 131072);
        assert_eq!(reps.len(), 131072);
        assert!(removed.iter().all(|id| id % 2 == 1));
        assert!(reps.iter().all(|id| id % 2 == 0));
    }
}
