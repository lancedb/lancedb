// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Protobuf serialization for [`FilteredReadExec`] and related types.
//!
//! Proto message definitions live in `lance-datafusion` (see `pb`).
//! Conversion functions live here because they need access to `FilteredReadExec`
//! and `Dataset`, which are defined in this crate.
//!
//! A datafusion `PhysicalExtensionCodec` can call these functions in `try_encode`
//! and `try_decode` to support distributed execution (planner → executor).

use std::collections::HashMap;
use std::io::Cursor;
use std::ops::Range;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use datafusion::execution::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use lance_core::datatypes::{BlobHandling, Projection};
use lance_core::{Error, Result};
use lance_datafusion::pb;
use lance_datafusion::substrait::{encode_substrait, parse_substrait, prune_schema_for_substrait};
use lance_select::RowAddrTreeMap;
use lance_table::format::Fragment;

use crate::Dataset;

use super::filtered_read::{
    FilteredReadExec, FilteredReadOptions, FilteredReadPlan, FilteredReadThreadingMode,
};
use super::table_identifier::{resolve_dataset, table_identifier_from_dataset};

// =============================================================================
// FilteredReadExec <-> Proto
// =============================================================================

/// Convert a [`FilteredReadExec`] to proto for serialization.
///
/// Uses `table_identifier_from_dataset` by default (no manifest bytes).
/// The caller can replace the `table` field with
/// [`table_identifier_from_dataset_with_manifest`] if desired.
pub async fn filtered_read_exec_to_proto(
    exec: &FilteredReadExec,
    state: &SessionState,
) -> Result<pb::FilteredReadExecProto> {
    let table = table_identifier_from_dataset(exec.dataset()).await?;
    // Use the pruned dataset schema for filter encoding — filters can reference columns
    // outside the projection (e.g. SELECT name WHERE age > 10), and some dataset columns
    // may have types that Substrait cannot serialize (e.g. FixedSizeList, Float16).
    let filter_schema = Arc::new(prune_schema_for_substrait(&exec.dataset().schema().into()));
    let options = fr_options_to_proto(exec.options(), &filter_schema, state)?;

    let plan = match exec.plan() {
        Some(plan) => Some(plan_to_proto(&plan, &filter_schema, state)?),
        None => None,
    };

    Ok(pb::FilteredReadExecProto {
        table: Some(table),
        options: Some(options),
        plan,
    })
}

/// Reconstruct a [`FilteredReadExec`] from proto.
pub async fn filtered_read_exec_from_proto(
    proto: pb::FilteredReadExecProto,
    dataset: Option<Arc<Dataset>>,
    index_input: Option<Arc<dyn ExecutionPlan>>,
    state: &SessionState,
) -> Result<FilteredReadExec> {
    let dataset = resolve_dataset(dataset, proto.table.as_ref()).await?;

    let options_proto = proto.options.ok_or_else(|| {
        Error::invalid_input_source("Missing options in FilteredReadExecProto".into())
    })?;

    let options = fr_options_from_proto(options_proto, &dataset, state).await?;
    let exec = FilteredReadExec::try_new(dataset.clone(), options, index_input)?;

    // Apply pre-computed plan if present
    if let Some(plan_proto) = proto.plan {
        let plan = plan_from_proto(plan_proto, &dataset, state).await?;
        exec.with_plan(plan).await
    } else {
        Ok(exec)
    }
}

// =============================================================================
// FilteredReadOptions <-> Proto
// =============================================================================

fn fr_options_to_proto(
    options: &FilteredReadOptions,
    filter_schema: &Arc<ArrowSchema>,
    state: &SessionState,
) -> Result<pb::FilteredReadOptionsProto> {
    let refine_filter_substrait = match &options.refine_filter {
        Some(expr) => Some(encode_substrait(
            expr.clone(),
            filter_schema.clone(),
            state,
        )?),
        None => None,
    };

    let full_filter_substrait = match &options.full_filter {
        Some(expr) => Some(encode_substrait(
            expr.clone(),
            filter_schema.clone(),
            state,
        )?),
        None => None,
    };

    // Serialize the filter schema as Arrow IPC if we have filters
    let filter_schema_ipc = if refine_filter_substrait.is_some() || full_filter_substrait.is_some()
    {
        Some(schema_to_bytes(filter_schema)?)
    } else {
        None
    };

    Ok(pb::FilteredReadOptionsProto {
        scan_range_before_filter: options
            .scan_range_before_filter
            .as_ref()
            .map(range_to_proto),
        scan_range_after_filter: options.scan_range_after_filter.as_ref().map(range_to_proto),
        with_deleted_rows: options.with_deleted_rows,
        batch_size: options.batch_size,
        fragment_readahead: options.fragment_readahead.map(|v| v as u64),
        fragment_ids: options
            .fragments
            .as_ref()
            .map(|frags| frags.iter().map(|f| f.id).collect())
            .unwrap_or_default(),
        projection: Some(projection_to_proto(&options.projection)),
        refine_filter_substrait,
        full_filter_substrait,
        threading_mode: Some(threading_mode_to_proto(&options.threading_mode)),
        io_buffer_size_bytes: options.io_buffer_size_bytes,
        filter_schema_ipc,
    })
}

async fn fr_options_from_proto(
    proto: pb::FilteredReadOptionsProto,
    dataset: &Arc<Dataset>,
    state: &SessionState,
) -> Result<FilteredReadOptions> {
    let projection = projection_from_proto(
        proto.projection.as_ref(),
        dataset.clone() as Arc<dyn lance_core::datatypes::Projectable>,
    )?;
    let mut options = FilteredReadOptions::new(projection);

    // Fragments
    if !proto.fragment_ids.is_empty() {
        let fragments = fragments_from_proto(&proto.fragment_ids, dataset)?;
        options = options.with_fragments(Arc::new(fragments));
    }

    // Scan ranges
    if let Some(range) = proto.scan_range_before_filter {
        options = options
            .with_scan_range_before_filter(range_from_proto(&range))
            .map_err(|e| Error::internal(e.to_string()))?;
    }
    if let Some(range) = proto.scan_range_after_filter {
        options = options
            .with_scan_range_after_filter(range_from_proto(&range))
            .map_err(|e| Error::internal(e.to_string()))?;
    }

    // Deleted rows
    if proto.with_deleted_rows {
        options = options
            .with_deleted_rows()
            .map_err(|e| Error::internal(e.to_string()))?;
    }

    // Performance tuning
    if let Some(batch_size) = proto.batch_size {
        options = options.with_batch_size(batch_size);
    }
    if let Some(readahead) = proto.fragment_readahead {
        options = options.with_fragment_readahead(readahead as usize);
    }
    if let Some(io_buffer) = proto.io_buffer_size_bytes {
        options = options.with_io_buffer_size(io_buffer);
    }
    if let Some(mode) = proto.threading_mode {
        options.threading_mode = threading_mode_from_proto(&mode)?;
    }

    // Filters — require filter_schema_ipc when filters are present
    let has_filters =
        proto.refine_filter_substrait.is_some() || proto.full_filter_substrait.is_some();
    if has_filters {
        let filter_schema =
            schema_from_bytes(proto.filter_schema_ipc.as_ref().ok_or_else(|| {
                Error::invalid_input_source(
                    "missing filter_schema_ipc but filters are present".into(),
                )
            })?)?;

        if let Some(bytes) = &proto.refine_filter_substrait {
            options.refine_filter =
                Some(parse_substrait(bytes, filter_schema.clone(), state).await?);
        }
        if let Some(bytes) = &proto.full_filter_substrait {
            options.full_filter = Some(parse_substrait(bytes, filter_schema, state).await?);
        }
    }

    Ok(options)
}

// =============================================================================
// FilteredReadPlan <-> Proto
// =============================================================================

/// Convert a [`FilteredReadPlan`] to proto.
///
/// Deduplicates filter expressions: many fragments often share the same `Arc<Expr>`.
/// We detect sharing via `Arc::as_ptr()` and encode each unique expression only once.
pub fn plan_to_proto(
    plan: &FilteredReadPlan,
    filter_schema: &Arc<ArrowSchema>,
    state: &SessionState,
) -> Result<pb::FilteredReadPlanProto> {
    let mut buf = Vec::with_capacity(plan.rows.serialized_size());
    plan.rows.serialize_into(&mut buf)?;

    // Deduplicate filter expressions by Arc pointer identity.
    let mut ptr_to_id: HashMap<*const Expr, u32> = HashMap::new();
    let mut filter_expressions: Vec<Vec<u8>> = Vec::new();
    let mut fragment_filter_ids: HashMap<u32, u32> = HashMap::new();

    for (frag_id, expr) in &plan.filters {
        let ptr = Arc::as_ptr(expr);
        let id = match ptr_to_id.get(&ptr) {
            Some(&id) => id,
            None => {
                let id = filter_expressions.len() as u32;
                let encoded =
                    encode_substrait(expr.as_ref().clone(), filter_schema.clone(), state)?;
                filter_expressions.push(encoded);
                ptr_to_id.insert(ptr, id);
                id
            }
        };
        fragment_filter_ids.insert(*frag_id, id);
    }

    let filter_schema_ipc = if fragment_filter_ids.is_empty() {
        None
    } else {
        Some(schema_to_bytes(filter_schema)?)
    };

    Ok(pb::FilteredReadPlanProto {
        row_addr_tree_map: buf,
        scan_range_after_filter: plan.scan_range_after_filter.as_ref().map(range_to_proto),
        filter_schema_ipc,
        fragment_filter_ids,
        filter_expressions,
    })
}

async fn plan_from_proto(
    proto: pb::FilteredReadPlanProto,
    _dataset: &Arc<Dataset>,
    state: &SessionState,
) -> Result<FilteredReadPlan> {
    let rows = RowAddrTreeMap::deserialize_from(Cursor::new(&proto.row_addr_tree_map))?;

    let mut filters = HashMap::new();
    if !proto.fragment_filter_ids.is_empty() {
        let filter_schema =
            schema_from_bytes(proto.filter_schema_ipc.as_ref().ok_or_else(|| {
                Error::invalid_input_source("missing filter_schema_ipc but plan has filters".into())
            })?)?;

        // Decode each unique expression once, then share via Arc.
        let mut decoded: Vec<Arc<Expr>> = Vec::with_capacity(proto.filter_expressions.len());
        for bytes in &proto.filter_expressions {
            let expr = parse_substrait(bytes, filter_schema.clone(), state).await?;
            decoded.push(Arc::new(expr));
        }

        for (frag_id, expr_id) in &proto.fragment_filter_ids {
            let expr = decoded.get(*expr_id as usize).ok_or_else(|| {
                Error::invalid_input_source(
                    format!(
                        "filter expression index {} out of bounds (have {})",
                        expr_id,
                        decoded.len()
                    )
                    .into(),
                )
            })?;
            filters.insert(*frag_id, Arc::clone(expr));
        }
    }

    Ok(FilteredReadPlan {
        rows,
        filters,
        scan_range_after_filter: proto.scan_range_after_filter.map(|r| range_from_proto(&r)),
    })
}

// =============================================================================
// Projection <-> Proto
// =============================================================================

fn projection_to_proto(proj: &Projection) -> pb::ProjectionProto {
    pb::ProjectionProto {
        field_ids: proj.field_ids.iter().copied().collect(),
        with_row_id: proj.with_row_id,
        with_row_addr: proj.with_row_addr,
        with_row_last_updated_at_version: proj.with_row_last_updated_at_version,
        with_row_created_at_version: proj.with_row_created_at_version,
        blob_handling: Some(blob_handling_to_proto(&proj.blob_handling)),
    }
}

fn blob_handling_to_proto(bh: &BlobHandling) -> pb::BlobHandlingProto {
    use pb::blob_handling_proto::Mode;
    let mode = match bh {
        BlobHandling::AllBinary => Some(Mode::AllBinary(true)),
        BlobHandling::BlobsDescriptions => Some(Mode::BlobsDescriptions(true)),
        BlobHandling::AllDescriptions => Some(Mode::AllDescriptions(true)),
        BlobHandling::SomeBlobsBinary(ids) => Some(Mode::SomeBlobsBinary(pb::FieldIdSet {
            field_ids: ids.iter().copied().collect(),
        })),
        BlobHandling::SomeBinary(ids) => Some(Mode::SomeBinary(pb::FieldIdSet {
            field_ids: ids.iter().copied().collect(),
        })),
    };
    pb::BlobHandlingProto { mode }
}

fn blob_handling_from_proto(proto: Option<&pb::BlobHandlingProto>) -> BlobHandling {
    use pb::blob_handling_proto::Mode;
    match proto.and_then(|p| p.mode.as_ref()) {
        Some(Mode::AllBinary(_)) => BlobHandling::AllBinary,
        Some(Mode::BlobsDescriptions(_)) => BlobHandling::BlobsDescriptions,
        Some(Mode::AllDescriptions(_)) => BlobHandling::AllDescriptions,
        Some(Mode::SomeBlobsBinary(ids)) => {
            BlobHandling::SomeBlobsBinary(ids.field_ids.iter().copied().collect())
        }
        Some(Mode::SomeBinary(ids)) => {
            BlobHandling::SomeBinary(ids.field_ids.iter().copied().collect())
        }
        // Default for backwards compatibility with protos that don't have blob_handling
        None => BlobHandling::default(),
    }
}

fn projection_from_proto(
    proto: Option<&pb::ProjectionProto>,
    base: Arc<dyn lance_core::datatypes::Projectable>,
) -> Result<Projection> {
    let proto =
        proto.ok_or_else(|| Error::invalid_input_source("Missing projection in proto".into()))?;

    let mut projection = Projection::empty(base);
    for field_id in &proto.field_ids {
        projection.field_ids.insert(*field_id);
    }
    if proto.with_row_id {
        projection = projection.with_row_id();
    }
    if proto.with_row_addr {
        projection = projection.with_row_addr();
    }
    if proto.with_row_last_updated_at_version {
        projection = projection.with_row_last_updated_at_version();
    }
    if proto.with_row_created_at_version {
        projection = projection.with_row_created_at_version();
    }
    projection =
        projection.with_blob_handling(blob_handling_from_proto(proto.blob_handling.as_ref()));
    Ok(projection)
}

// =============================================================================
// Threading mode <-> Proto
// =============================================================================

fn threading_mode_to_proto(mode: &FilteredReadThreadingMode) -> pb::FilteredReadThreadingModeProto {
    let mode_oneof = match mode {
        FilteredReadThreadingMode::OnePartitionMultipleThreads(n) => {
            pb::filtered_read_threading_mode_proto::Mode::OnePartitionMultipleThreads(*n as u64)
        }
        FilteredReadThreadingMode::MultiplePartitions(n) => {
            pb::filtered_read_threading_mode_proto::Mode::MultiplePartitions(*n as u64)
        }
    };
    pb::FilteredReadThreadingModeProto {
        mode: Some(mode_oneof),
    }
}

fn threading_mode_from_proto(
    proto: &pb::FilteredReadThreadingModeProto,
) -> Result<FilteredReadThreadingMode> {
    match &proto.mode {
        Some(pb::filtered_read_threading_mode_proto::Mode::OnePartitionMultipleThreads(n)) => Ok(
            FilteredReadThreadingMode::OnePartitionMultipleThreads(*n as usize),
        ),
        Some(pb::filtered_read_threading_mode_proto::Mode::MultiplePartitions(n)) => {
            Ok(FilteredReadThreadingMode::MultiplePartitions(*n as usize))
        }
        None => Err(Error::invalid_input_source(
            "Missing threading mode in proto".into(),
        )),
    }
}

// =============================================================================
// Helpers
// =============================================================================

fn range_to_proto(range: &Range<u64>) -> pb::U64Range {
    pb::U64Range {
        start: range.start,
        end: range.end,
    }
}

fn range_from_proto(proto: &pb::U64Range) -> Range<u64> {
    proto.start..proto.end
}

fn fragments_from_proto(fragment_ids: &[u64], dataset: &Arc<Dataset>) -> Result<Vec<Fragment>> {
    fragment_ids
        .iter()
        .map(|id| {
            dataset
                .manifest
                .fragments
                .iter()
                .find(|f| f.id == *id)
                .cloned()
                .ok_or_else(|| {
                    Error::invalid_input_source(
                        format!("Fragment {} not found in dataset", id).into(),
                    )
                })
        })
        .collect()
}

fn schema_to_bytes(schema: &ArrowSchema) -> Result<Vec<u8>> {
    let options =
        arrow_ipc::writer::IpcWriteOptions::try_new(8, false, arrow_ipc::MetadataVersion::V5)
            .map_err(|e| Error::internal(format!("Failed to create IPC write options: {}", e)))?;
    let generator = arrow_ipc::writer::IpcDataGenerator::default();
    let mut tracker = arrow_ipc::writer::DictionaryTracker::new(false);
    let encoded = generator.schema_to_bytes_with_dictionary_tracker(schema, &mut tracker, &options);
    Ok(encoded.ipc_message.to_vec())
}

fn schema_from_bytes(bytes: &[u8]) -> Result<Arc<ArrowSchema>> {
    let message = arrow_ipc::root_as_message(bytes)
        .map_err(|e| Error::internal(format!("Failed to parse IPC schema message: {}", e)))?;
    let ipc_schema = message
        .header_as_schema()
        .ok_or_else(|| Error::internal("IPC message does not contain a schema".to_string()))?;
    let schema = arrow_ipc::convert::fb_to_schema(ipc_schema);
    Ok(Arc::new(schema))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::UInt32Type;
    use arrow_schema::{DataType, Field};
    use datafusion::prelude::SessionContext;
    use lance_core::datatypes::OnMissing;
    use lance_datagen::{array, gen_batch};
    use lance_select::RowAddrTreeMap;
    use roaring::RoaringBitmap;
    use std::collections::HashMap;
    use std::collections::HashSet;

    use crate::utils::test::{DatagenExt, FragmentCount, FragmentRowCount};

    #[test]
    fn test_range_roundtrip() {
        let range = 10u64..42u64;
        let proto = range_to_proto(&range);
        let back = range_from_proto(&proto);
        assert_eq!(range, back);
    }

    #[test]
    fn test_threading_mode_roundtrip() {
        let mode = FilteredReadThreadingMode::OnePartitionMultipleThreads(8);
        let proto = threading_mode_to_proto(&mode);
        let back = threading_mode_from_proto(&proto).unwrap();
        assert_eq!(mode, back);

        let mode = FilteredReadThreadingMode::MultiplePartitions(4);
        let proto = threading_mode_to_proto(&mode);
        let back = threading_mode_from_proto(&proto).unwrap();
        assert_eq!(mode, back);
    }

    #[test]
    fn test_schema_roundtrip() {
        let schema = ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let bytes = schema_to_bytes(&schema).unwrap();
        let back = schema_from_bytes(&bytes).unwrap();
        assert_eq!(schema, *back);
    }

    #[test]
    fn test_projection_roundtrip() {
        let schema = lance_core::datatypes::Schema::try_from(&ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true),
        ]))
        .unwrap();

        let base: Arc<dyn lance_core::datatypes::Projectable> = Arc::new(schema);

        let mut projection = Projection::empty(base.clone());
        projection.field_ids = HashSet::from([0, 2]);
        projection = projection
            .with_row_id()
            .with_row_addr()
            .with_row_last_updated_at_version()
            .with_row_created_at_version()
            .with_blob_handling(BlobHandling::SomeBlobsBinary(HashSet::from([1, 3])));

        let proto = projection_to_proto(&projection);
        let back = projection_from_proto(Some(&proto), base).unwrap();

        assert_eq!(projection.field_ids, back.field_ids);
        assert_eq!(projection.with_row_id, back.with_row_id);
        assert_eq!(projection.with_row_addr, back.with_row_addr);
        assert_eq!(
            projection.with_row_last_updated_at_version,
            back.with_row_last_updated_at_version
        );
        assert_eq!(
            projection.with_row_created_at_version,
            back.with_row_created_at_version
        );
        assert_eq!(projection.blob_handling, back.blob_handling);
    }

    #[test]
    fn test_row_addr_tree_map_roundtrip_in_plan_proto() {
        let mut rows = RowAddrTreeMap::new();
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert_range(0..100);
        rows.insert_bitmap(0, bitmap);
        rows.insert_fragment(1); // Full fragment

        let mut buf = Vec::with_capacity(rows.serialized_size());
        rows.serialize_into(&mut buf).unwrap();
        let back = RowAddrTreeMap::deserialize_from(Cursor::new(&buf)).unwrap();
        assert_eq!(rows, back);
    }

    async fn make_test_dataset() -> Arc<Dataset> {
        let dataset = gen_batch()
            .col("x", array::step::<UInt32Type>())
            .col("y", array::step::<UInt32Type>())
            .into_ram_dataset(FragmentCount::from(2), FragmentRowCount::from(50))
            .await
            .unwrap();
        Arc::new(dataset)
    }

    #[tokio::test]
    async fn test_options_roundtrip_basic() {
        let dataset = make_test_dataset().await;
        let ctx = SessionContext::new();
        let state = ctx.state();
        let filter_schema = Arc::new(prune_schema_for_substrait(&dataset.schema().into()));

        let options = FilteredReadOptions::basic_full_read(&dataset)
            .with_scan_range_before_filter(10..90)
            .unwrap()
            .with_batch_size(64)
            .with_fragment_readahead(4)
            .with_io_buffer_size(1024 * 1024);

        let proto = fr_options_to_proto(&options, &filter_schema, &state).unwrap();
        let back = fr_options_from_proto(proto, &dataset, &state)
            .await
            .unwrap();

        assert_eq!(
            options.scan_range_before_filter,
            back.scan_range_before_filter
        );
        assert_eq!(options.batch_size, back.batch_size);
        assert_eq!(options.fragment_readahead, back.fragment_readahead);
        assert_eq!(options.io_buffer_size_bytes, back.io_buffer_size_bytes);
        assert_eq!(options.threading_mode, back.threading_mode);
        assert_eq!(options.with_deleted_rows, back.with_deleted_rows);
        assert_eq!(options.projection.field_ids, back.projection.field_ids);
        assert_eq!(options.projection.with_row_id, back.projection.with_row_id);
        assert_eq!(
            options.projection.with_row_addr,
            back.projection.with_row_addr
        );
    }

    #[tokio::test]
    async fn test_options_roundtrip_with_filter() {
        let dataset = make_test_dataset().await;
        let ctx = SessionContext::new();
        let state = ctx.state();
        let filter_schema = Arc::new(prune_schema_for_substrait(&dataset.schema().into()));

        let filter_expr = datafusion_expr::col("x").gt(datafusion_expr::lit(5i32));
        let refine_expr = datafusion_expr::col("x").lt(datafusion_expr::lit(100i32));
        let projection = dataset
            .empty_projection()
            .union_column("x", OnMissing::Error)
            .unwrap()
            .with_row_id();
        let mut options = FilteredReadOptions::new(projection)
            .with_deleted_rows()
            .unwrap();
        options.full_filter = Some(filter_expr);
        options.refine_filter = Some(refine_expr);
        options.threading_mode = FilteredReadThreadingMode::MultiplePartitions(4);

        let proto = fr_options_to_proto(&options, &filter_schema, &state).unwrap();

        // Verify filter schema IPC was generated
        assert!(proto.filter_schema_ipc.is_some());
        assert!(proto.full_filter_substrait.is_some());
        assert!(proto.refine_filter_substrait.is_some());

        let back = fr_options_from_proto(proto, &dataset, &state)
            .await
            .unwrap();

        assert!(back.full_filter.is_some());
        assert!(back.refine_filter.is_some());
        assert!(back.with_deleted_rows);
        assert_eq!(options.threading_mode, back.threading_mode);
        assert_eq!(options.projection.field_ids, back.projection.field_ids);
        assert!(back.projection.with_row_id);
    }

    #[tokio::test]
    async fn test_options_roundtrip_with_fragments() {
        let dataset = make_test_dataset().await;
        let ctx = SessionContext::new();
        let state = ctx.state();
        let filter_schema = Arc::new(prune_schema_for_substrait(&dataset.schema().into()));

        let frags = dataset.get_fragments();
        let first_frag = vec![frags[0].metadata().clone()];
        let options =
            FilteredReadOptions::basic_full_read(&dataset).with_fragments(Arc::new(first_frag));

        let proto = fr_options_to_proto(&options, &filter_schema, &state).unwrap();
        assert_eq!(proto.fragment_ids.len(), 1);

        let back = fr_options_from_proto(proto, &dataset, &state)
            .await
            .unwrap();
        assert!(back.fragments.is_some());
        assert_eq!(back.fragments.as_ref().unwrap().len(), 1);
        assert_eq!(
            back.fragments.as_ref().unwrap()[0].id,
            options.fragments.as_ref().unwrap()[0].id
        );
    }

    #[tokio::test]
    async fn test_exec_to_proto_roundtrip() {
        let dataset = make_test_dataset().await;
        let ctx = SessionContext::new();
        let state = ctx.state();

        let options = FilteredReadOptions::basic_full_read(&dataset)
            .with_batch_size(32)
            .with_scan_range_before_filter(0..50)
            .unwrap();

        let exec = FilteredReadExec::try_new(dataset.clone(), options, None).unwrap();

        let proto = filtered_read_exec_to_proto(&exec, &state).await.unwrap();

        // Check table identifier
        let table = proto.table.as_ref().unwrap();
        assert_eq!(table.uri, dataset.uri());
        assert_eq!(table.version, dataset.manifest.version);
        assert!(table.serialized_manifest.is_none());

        // Roundtrip back
        let back = filtered_read_exec_from_proto(proto, Some(dataset.clone()), None, &state)
            .await
            .unwrap();

        assert_eq!(exec.options().batch_size, back.options().batch_size);
        assert_eq!(
            exec.options().scan_range_before_filter,
            back.options().scan_range_before_filter
        );
        assert_eq!(
            exec.options().projection.field_ids,
            back.options().projection.field_ids
        );
    }

    /// A row-stream (take) exec serializes like any other: the input plan
    /// travels as the node's child through the plan codec, and decoding
    /// re-derives the row-stream selector from the child's schema
    #[tokio::test]
    async fn test_exec_to_proto_roundtrip_row_stream() {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use lance_core::{ROW_ID, ROW_ID_FIELD};
        use lance_datafusion::exec::OneShotExec;

        fn keys_input() -> Arc<dyn ExecutionPlan> {
            let schema = Arc::new(ArrowSchema::new(vec![ROW_ID_FIELD.clone()]));
            let batch = arrow_array::RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow_array::UInt64Array::from(vec![3u64, 1, 4]))],
            )
            .unwrap();
            let stream = futures::stream::iter(vec![Ok(batch)]);
            Arc::new(OneShotExec::new(Box::pin(RecordBatchStreamAdapter::new(
                schema, stream,
            ))))
        }

        let dataset = make_test_dataset().await;
        let ctx = SessionContext::new();
        let state = ctx.state();

        let options = FilteredReadOptions::basic_full_read(&dataset);
        let exec = FilteredReadExec::try_new(dataset.clone(), options, Some(keys_input())).unwrap();
        assert!(exec.row_stream_input().is_some());

        let proto = filtered_read_exec_to_proto(&exec, &state).await.unwrap();

        // The codec hands the decoded child back; the selector re-derives
        // from its schema
        let back =
            filtered_read_exec_from_proto(proto, Some(dataset.clone()), Some(keys_input()), &state)
                .await
                .unwrap();
        assert!(back.row_stream_input().is_some());
        assert_eq!(exec.schema(), back.schema());
        assert_eq!(
            exec.options().projection.field_ids,
            back.options().projection.field_ids
        );
        assert!(
            back.row_stream_input()
                .unwrap()
                .schema()
                .column_with_name(ROW_ID)
                .is_some()
        );
    }

    #[tokio::test]
    async fn test_plan_proto_roundtrip() {
        let dataset = make_test_dataset().await;
        let ctx = SessionContext::new();
        let state = ctx.state();

        let mut rows = RowAddrTreeMap::new();
        let mut bitmap0 = RoaringBitmap::new();
        bitmap0.insert_range(0..25);
        rows.insert_bitmap(0, bitmap0);
        let mut bitmap1 = RoaringBitmap::new();
        bitmap1.insert_range(0..30);
        rows.insert_bitmap(1, bitmap1);

        // Two fragments share the same Arc<Expr> — dedup should encode it once.
        let shared_filter = Arc::new(datafusion_expr::col("x").gt(datafusion_expr::lit(10i32)));
        let mut filters = HashMap::new();
        filters.insert(0u32, Arc::clone(&shared_filter));
        filters.insert(1u32, Arc::clone(&shared_filter));

        let plan = FilteredReadPlan {
            rows,
            filters,
            scan_range_after_filter: Some(5..20),
        };

        let filter_schema = Arc::new(prune_schema_for_substrait(&dataset.schema().into()));
        let proto = plan_to_proto(&plan, &filter_schema, &state).unwrap();

        // Verify dedup: 2 fragments but only 1 unique expression
        assert_eq!(proto.fragment_filter_ids.len(), 2);
        assert_eq!(
            proto.filter_expressions.len(),
            1,
            "shared Arc<Expr> should be deduplicated into a single expression"
        );

        let back = plan_from_proto(proto, &dataset, &state).await.unwrap();

        assert_eq!(plan.rows, back.rows);
        assert_eq!(plan.scan_range_after_filter, back.scan_range_after_filter);
        assert_eq!(back.filters.len(), 2);
        assert!(back.filters.contains_key(&0));
        assert!(back.filters.contains_key(&1));
        // After roundtrip, the decoded expressions should be shared via Arc too
        assert!(Arc::ptr_eq(&back.filters[&0], &back.filters[&1]));
    }
}
