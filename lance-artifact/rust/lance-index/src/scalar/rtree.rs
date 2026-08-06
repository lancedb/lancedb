// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::metrics::{MetricsCollector, NoOpMetricsCollector};
use crate::scalar::expression::{GeoQueryParser, ScalarQueryParser};
use crate::scalar::lance_format::LanceIndexStore;
use crate::scalar::registry::{
    BasicTrainer, ScalarIndexPlugin, TrainingCriteria, TrainingOrdering, TrainingRequest,
};
use crate::scalar::rtree::sort::Sorter;
use crate::scalar::{
    AnyQuery, BuiltinIndexType, CreatedIndex, GeoQuery, IndexFile, IndexReader, IndexStore,
    IndexWriter, OldIndexDataFilter, RowIdRemapper, ScalarIndex, ScalarIndexParams, SearchResult,
    UpdateCriteria,
};
use crate::{Index, IndexType, pb};
use arrow_array::UInt32Array;
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{Array, BinaryArray, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::DataFusionError;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt, stream};
use geoarrow_array::array::{RectArray, from_arrow_array};
use geoarrow_array::builder::RectBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow};
use geoarrow_schema::{Dimension, RectType};
use lance_arrow::RecordBatchExt;
use lance_core::cache::{CacheKey, CacheKeySchema, KeyBuilder, LanceCache, WeakLanceCache};
use lance_core::deepsize::DeepSizeOf;
use lance_core::utils::address::RowAddress;
use lance_core::utils::row_addr_remap::RowAddrRemap;
use lance_core::utils::tempfile::TempDir;
use lance_core::{Error, ROW_ID, Result};
use lance_datafusion::chunker::chunk_concat_stream;
pub use lance_geo::bbox::{BoundingBox, bounding_box, total_bounds};
use lance_io::object_store::ObjectStore;
use lance_select::{NullableRowAddrSet, RowAddrTreeMap, RowSetOps};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use sort::hilbert_sort::HilbertSorter;
use std::any::Any;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, LazyLock};

mod sort;

pub const DEFAULT_RTREE_PAGE_SIZE: u32 = 4096;
const RTREE_INDEX_VERSION: u32 = 0;
const RTREE_PAGES_NAME: &str = "page_data.lance";
const RTREE_NULLS_NAME: &str = "nulls.lance";

fn validate_page_size(page_size: u32) -> Result<()> {
    if page_size < 2 {
        return Err(Error::invalid_input(
            "RTree page_size must be at least 2".to_string(),
        ));
    }
    Ok(())
}

fn validate_stored_page_size(page_size: u32, num_items: usize) -> Result<()> {
    if page_size == 0 || (page_size == 1 && num_items > 1) {
        return Err(Error::invalid_input(format!(
            "stored RTree page_size {page_size} cannot represent {num_items} items"
        )));
    }
    Ok(())
}

static BBOX_FIELD: LazyLock<Arc<ArrowField>> = LazyLock::new(|| {
    let bbox_type = RectType::new(Dimension::XY, Default::default());
    Arc::new(bbox_type.to_field("bbox", false))
});
static BBOX_ROWID_SCHEMA: LazyLock<Arc<ArrowSchema>> = LazyLock::new(|| {
    let rowid_field = ArrowField::new(ROW_ID, DataType::UInt64, false);
    Arc::new(ArrowSchema::new(vec![
        BBOX_FIELD.clone(),
        rowid_field.into(),
    ]))
});
static RTREE_PAGE_SCHEMA: LazyLock<Arc<ArrowSchema>> = LazyLock::new(|| {
    let id_field = ArrowField::new("id", DataType::UInt64, false);
    Arc::new(ArrowSchema::new(vec![BBOX_FIELD.clone(), id_field.into()]))
});

static RTREE_NULLS_SCHEMA: LazyLock<Arc<ArrowSchema>> = LazyLock::new(|| {
    Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "nulls",
        DataType::Binary,
        false,
    )]))
});

/// A stream that reads the original training data back out of the index
struct IndexReaderStream {
    reader: Arc<dyn IndexReader>,
    batch_size: u64,
    offset: u64,
    limit: u64,
}

impl IndexReaderStream {
    async fn new(reader: Arc<dyn IndexReader>, batch_size: u64) -> Self {
        let limit = reader.num_rows() as u64;
        Self::new_with_limit(reader, batch_size, limit).await
    }

    async fn new_with_limit(reader: Arc<dyn IndexReader>, batch_size: u64, limit: u64) -> Self {
        Self {
            reader,
            batch_size,
            offset: 0,
            limit,
        }
    }
}

impl Stream for IndexReaderStream {
    type Item = BoxFuture<'static, Result<RecordBatch>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.offset >= this.limit {
            return std::task::Poll::Ready(None);
        }
        let read_start = this.offset;
        let read_end = this.limit.min(this.offset + this.batch_size);
        this.offset = read_end;
        let reader_copy = this.reader.clone();

        let read_task = async move {
            reader_copy
                .read_range(read_start as usize..read_end as usize, None)
                .await
        }
        .boxed();
        std::task::Poll::Ready(Some(read_task))
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RTreeMetadata {
    pub(crate) page_size: u32,
    pub(crate) num_pages: u64,
    pub(crate) num_items: usize,
    pub(crate) bbox: BoundingBox,
    pub(crate) page_offsets: Vec<usize>,
}

impl RTreeMetadata {
    pub fn new(page_size: u32, num_pages: u64, num_items: usize, bbox: BoundingBox) -> Self {
        let page_offsets = Self::calculate_page_offsets(num_items, page_size);
        if page_size >= 2 {
            debug_assert_eq!(page_offsets.len(), num_pages as usize);
        }
        Self {
            page_size,
            num_pages,
            num_items,
            bbox,
            page_offsets,
        }
    }

    fn calculate_page_offsets(num_items: usize, page_size: u32) -> Vec<usize> {
        if page_size < 2 {
            return Vec::new();
        }
        let mut page_offsets = vec![];
        let mut cur_level_items = num_items;
        let mut cur_offset = 0;
        while cur_level_items > 0 {
            if cur_level_items <= page_size as usize {
                page_offsets.push(cur_offset);
                break;
            }
            for off in (0..cur_level_items).step_by(page_size as usize) {
                page_offsets.push(cur_offset + off);
            }
            cur_offset += cur_level_items;
            cur_level_items = cur_level_items.div_ceil(page_size as usize);
        }

        page_offsets
    }

    fn into_map(self) -> HashMap<String, String> {
        HashMap::from_iter(vec![
            ("page_size".to_owned(), self.page_size.to_string()),
            ("num_pages".to_owned(), self.num_pages.to_string()),
            ("num_items".to_owned(), self.num_items.to_string()),
            ("bbox".to_owned(), serde_json::json!(self.bbox).to_string()),
        ])
    }
}

impl From<&HashMap<String, String>> for RTreeMetadata {
    fn from(metadata: &HashMap<String, String>) -> Self {
        let page_size = metadata
            .get("page_size")
            .map(|bs| bs.parse().unwrap_or(DEFAULT_RTREE_PAGE_SIZE))
            .unwrap_or(DEFAULT_RTREE_PAGE_SIZE);
        let num_pages = metadata
            .get("num_pages")
            .map(|bs| bs.parse().unwrap_or(0))
            .unwrap_or(0);
        let num_items = metadata
            .get("num_items")
            .map(|bs| bs.parse().unwrap_or(0))
            .unwrap_or(0);
        let bbox = metadata
            .get("bbox")
            .map(|bs| serde_json::from_str(bs).unwrap_or_default())
            .unwrap_or_default();
        Self::new(page_size, num_pages, num_items, bbox)
    }
}

/// Extract bounding boxes from geometry columns
pub fn extract_bounding_boxes(
    geometry_array: &dyn Array,
    geometry_field: &ArrowField,
) -> Result<RectArray> {
    let geo_array = from_arrow_array(geometry_array, geometry_field).map_err(|e| {
        Error::index(format!(
            "Construct GeoArrowArray from an Arrow Array failed: {}",
            e
        ))
    })?;
    let rect_array = bounding_box(geo_array.as_ref())?;

    Ok(rect_array)
}

struct BboxStreamStats {
    null_map: RowAddrTreeMap,
    total_bbox: BoundingBox,
    // Number of non-null items
    num_items: usize,
}

#[derive(Debug, Clone)]
pub enum RTreeCacheKey {
    Page(u64),
    Nulls,
}

#[derive(Debug)]
pub struct RTreeCacheValue(Arc<RecordBatch>);

impl DeepSizeOf for RTreeCacheValue {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.0.deep_size_of_children(context)
    }
}

impl CacheKey for RTreeCacheKey {
    type ValueType = RTreeCacheValue;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        match self {
            Self::Page(page_id) => format!("page-{}", page_id).into(),
            Self::Nulls => "nulls".into(),
        }
    }

    fn type_name() -> &'static str {
        "RTree"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.scalar.rtree-entry-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        match self {
            Self::Page(page_id) => {
                builder.write_variant(0);
                builder.write_u64(*page_id);
            }
            Self::Nulls => builder.write_variant(1),
        }
    }
}

#[derive(Clone)]
pub struct RTreeIndex {
    pub(crate) metadata: Arc<RTreeMetadata>,
    store: Arc<dyn IndexStore>,
    frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
    index_cache: WeakLanceCache,
    pages_reader: Arc<dyn IndexReader>,
    nulls_reader: Arc<dyn IndexReader>,
}

impl std::fmt::Debug for RTreeIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RTreeIndex")
            .field("metadata", &self.metadata)
            .field("store", &self.store)
            .finish()
    }
}

impl RTreeIndex {
    pub async fn load(
        store: Arc<dyn IndexStore>,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        index_cache: &LanceCache,
    ) -> Result<Arc<Self>> {
        let pages_reader = store.open_index_file(RTREE_PAGES_NAME).await?;
        let metadata = RTreeMetadata::from(&pages_reader.schema().metadata);
        validate_stored_page_size(metadata.page_size, metadata.num_items)?;
        let nulls_reader = store.open_index_file(RTREE_NULLS_NAME).await?;

        Ok(Arc::new(Self {
            metadata: Arc::new(metadata),
            store,
            frag_reuse_index,
            index_cache: WeakLanceCache::from(index_cache),
            pages_reader,
            nulls_reader,
        }))
    }

    async fn page_range(&self, page_idx: u64) -> Result<Range<usize>> {
        let start = match self.metadata.page_offsets.get(page_idx as usize) {
            None => self.pages_reader.num_rows(),
            Some(start) => *start,
        };
        let end = match self.metadata.page_offsets.get((page_idx + 1) as usize) {
            None => self.pages_reader.num_rows(),
            Some(end) => *end,
        };
        Ok(start..end)
    }

    async fn search_bbox(
        &self,
        bbox: BoundingBox,
        metrics: &dyn MetricsCollector,
    ) -> Result<RowAddrTreeMap> {
        if self.metadata.num_items == 0 || !self.metadata.bbox.rect_intersects(&bbox) {
            return Ok(RowAddrTreeMap::default());
        }

        let mut row_addrs = RowAddrTreeMap::new();
        let mut stack = vec![self.metadata.num_pages - 1];

        while let Some(page_idx) = stack.pop() {
            let range = self.page_range(page_idx).await?;
            let is_leaf = range.start < self.metadata.num_items;
            let result = self
                .index_cache
                .get_or_insert_with_key_hit(RTreeCacheKey::Page(page_idx), move || async move {
                    let batch = self.pages_reader.read_range(range, None).await?;
                    metrics.record_part_load();
                    Ok(RTreeCacheValue(Arc::new(batch)))
                })
                .await;
            match &result {
                Ok((_, true)) => metrics.record_index_cache_hit(),
                _ => metrics.record_index_cache_miss(),
            }
            let batch = result.map(|(v, _)| v.0.clone())?;

            let bbox_array =
                extract_bounding_boxes(batch.column(0).as_ref(), batch.schema().field(0))?;
            let rowaddr_or_pageid_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            for i in 0..bbox_array.len() {
                let rect = bbox_array.value(i).unwrap();
                if bbox.rect_intersects(&rect) {
                    if is_leaf {
                        let row_addr = rowaddr_or_pageid_array.value(i);
                        row_addrs.insert(row_addr);
                    } else {
                        let page_id = rowaddr_or_pageid_array.value(i);
                        stack.push(page_id);
                    }
                }
            }
        }

        Ok(row_addrs)
    }

    async fn search_null(&self, metrics: &dyn MetricsCollector) -> Result<RowAddrTreeMap> {
        let result = self
            .index_cache
            .get_or_insert_with_key_hit(RTreeCacheKey::Nulls, move || async move {
                // Only one row
                let batch = self.nulls_reader.read_range(0..1, None).await?;
                metrics.record_part_load();
                Ok(RTreeCacheValue(Arc::new(batch)))
            })
            .await;
        match &result {
            Ok((_, true)) => metrics.record_index_cache_hit(),
            _ => metrics.record_index_cache_miss(),
        }
        let batch = result.map(|(v, _)| v.0.clone())?;

        let null_map = match batch.num_rows() {
            0 => RowAddrTreeMap::default(),
            1 => {
                let bytes = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(0);
                RowAddrTreeMap::deserialize_from(bytes)?
            }
            _ => {
                unreachable!()
            }
        };
        Ok(null_map)
    }

    /// Create a stream of all the data in the index, in the format (bbox, row_id)
    async fn into_data_stream(self) -> Result<SendableRecordBatchStream> {
        let reader = self.store.open_index_file(RTREE_PAGES_NAME).await?;
        let reader_stream = IndexReaderStream::new_with_limit(
            reader,
            self.metadata.page_size as u64,
            self.metadata.num_items as u64,
        )
        .await;
        let batches = reader_stream
            .map(|fut| {
                fut.map_ok(|batch| {
                    RecordBatch::try_new(BBOX_ROWID_SCHEMA.clone(), batch.columns().into()).unwrap()
                })
            })
            .map(|fut| fut.map_err(DataFusionError::from))
            .buffered(self.store.io_parallelism())
            .boxed();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            BBOX_ROWID_SCHEMA.clone(),
            batches,
        )))
    }

    async fn combine_old_new(
        self,
        new_input: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        let old_input = self.into_data_stream().await?;
        debug_assert_eq!(
            old_input.schema().flattened_fields().len(),
            new_input.schema().flattened_fields().len()
        );

        let merged = futures::stream::select(old_input, new_input);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            BBOX_ROWID_SCHEMA.clone(),
            merged,
        )))
    }
}

fn filter_keeps_nothing(filter: &Option<OldIndexDataFilter>) -> bool {
    match filter {
        Some(OldIndexDataFilter::Fragments { to_keep, .. }) => to_keep.is_empty(),
        Some(OldIndexDataFilter::RowIds(valid)) => valid.is_empty(),
        None => false,
    }
}

fn filter_rtree_data(
    data: SendableRecordBatchStream,
    filter: OldIndexDataFilter,
) -> SendableRecordBatchStream {
    let schema = data.schema();
    let filtered = data.map(move |batch_result| {
        let batch = batch_result?;
        let row_ids = batch
            .column_by_name(ROW_ID)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| Error::internal("expected UInt64Array for RTree row ids"))?;
        let mask = filter.filter_row_ids(row_ids);
        Ok(arrow_select::filter::filter_record_batch(&batch, &mask)?)
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, filtered))
}

fn remap_rtree_data(
    data: SendableRecordBatchStream,
    remapper: Arc<dyn RowIdRemapper>,
) -> SendableRecordBatchStream {
    let schema = data.schema();
    let remapped = data.map(move |batch_result| {
        let batch = batch_result?;
        // The row ID is column 1 in BBOX_ROWID_SCHEMA.
        Ok(remapper.remap_row_ids_record_batch(batch, 1)?)
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, remapped))
}

/// Merge caller-selected RTree segments into one self-contained segment.
///
/// Each source may supply a filter for rows that are still live. The merged index recomputes its
/// bounding box from the retained, remapped rows and preserves retained null row IDs.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
///
/// use lance_core::Result;
/// use lance_index::scalar::OldIndexDataFilter;
/// use lance_index::scalar::lance_format::LanceIndexStore;
/// use lance_index::scalar::rtree::{RTreeIndex, merge_rtree_indices};
///
/// async fn merge(
///     segments: &[Arc<RTreeIndex>],
///     destination: &LanceIndexStore,
///     filters: &[Option<OldIndexDataFilter>],
/// ) -> Result<()> {
///     merge_rtree_indices(segments, destination, filters).await?;
///     Ok(())
/// }
/// ```
pub async fn merge_rtree_indices(
    source_indices: &[Arc<RTreeIndex>],
    dest_store: &dyn IndexStore,
    old_data_filters: &[Option<OldIndexDataFilter>],
) -> Result<CreatedIndex> {
    if source_indices.is_empty() {
        return Err(Error::invalid_input(
            "merge_rtree_indices requires at least one source index",
        ));
    }
    if source_indices.len() != old_data_filters.len() {
        return Err(Error::invalid_input(format!(
            "merge_rtree_indices received {} source indices but {} filters",
            source_indices.len(),
            old_data_filters.len()
        )));
    }

    let first_contributing = source_indices
        .iter()
        .zip(old_data_filters)
        .find(|(source, filter)| source.metadata.num_items > 0 && !filter_keeps_nothing(filter))
        .or_else(|| {
            source_indices
                .iter()
                .zip(old_data_filters)
                .find(|(_, filter)| !filter_keeps_nothing(filter))
        })
        .map(|(source, _)| source)
        .unwrap_or(&source_indices[0]);
    let page_size = first_contributing.metadata.page_size;
    validate_page_size(page_size)?;
    let mut data_streams = Vec::with_capacity(source_indices.len());
    let mut null_map = RowAddrTreeMap::new();

    for (source, filter) in source_indices.iter().zip(old_data_filters) {
        if filter_keeps_nothing(filter) {
            continue;
        }
        if source.metadata.num_items > 0 && source.metadata.page_size != page_size {
            return Err(Error::invalid_input(format!(
                "cannot merge RTree segments with different page sizes: {} and {}",
                page_size, source.metadata.page_size
            )));
        }
        let mut source_nulls = source.search_null(&NoOpMetricsCollector).await?;
        if let Some(remapper) = &source.frag_reuse_index {
            source_nulls = remapper.remap_row_addrs_tree_map(&source_nulls);
        }
        if let Some(filter) = filter {
            filter.retain_old_rows(&mut source_nulls);
        }
        null_map |= &source_nulls;

        let mut data = source.as_ref().clone().into_data_stream().await?;
        if let Some(remapper) = source.frag_reuse_index.clone() {
            data = remap_rtree_data(data, remapper);
        }
        data_streams.push(match filter {
            Some(filter) => filter_rtree_data(data, filter.clone()),
            None => data,
        });
    }

    let combined = Box::pin(RecordBatchStreamAdapter::new(
        BBOX_ROWID_SCHEMA.clone(),
        stream::select_all(data_streams),
    ));
    let tmpdir = Arc::new(TempDir::default());
    let spill_store = Arc::new(LanceIndexStore::new(
        Arc::new(ObjectStore::local()),
        tmpdir.obj_path(),
        Arc::new(LanceCache::no_cache()),
    ));
    let (bbox_data, mut stats) =
        RTreeIndexPlugin::process_and_analyze_bbox_stream(combined, page_size, spill_store).await?;
    stats.null_map = null_map;
    let files =
        RTreeIndexPlugin::train_rtree_index(bbox_data, stats, page_size, dest_store).await?;

    Ok(CreatedIndex {
        index_details: prost_types::Any::from_msg(&pb::RTreeIndexDetails::default())?,
        index_version: RTREE_INDEX_VERSION,
        files,
    })
}

impl DeepSizeOf for RTreeIndex {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        let mut total_size = 0;

        total_size += self.store.deep_size_of_children(context);

        total_size
    }
}

#[async_trait]
impl Index for RTreeIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
        self
    }

    fn statistics(&self) -> Result<serde_json::Value> {
        serde_json::to_value(self.metadata.clone())
            .map_err(|e| Error::internal(format!("Error serializing statistics: {}", e)))
    }

    async fn prewarm(&self) -> Result<()> {
        for page_id in 0..self.metadata.num_pages {
            let range = self.page_range(page_id).await?;
            let batch = Arc::new(self.pages_reader.read_range(range, None).await?);
            self.index_cache
                .insert_with_key(
                    &RTreeCacheKey::Page(page_id),
                    Arc::new(RTreeCacheValue(batch.clone())),
                )
                .await;
        }

        let batch = self.nulls_reader.read_range(0..1, None).await?;
        self.index_cache
            .insert_with_key(
                &RTreeCacheKey::Nulls,
                Arc::new(RTreeCacheValue(Arc::new(batch))),
            )
            .await;

        Ok(())
    }

    fn index_type(&self) -> IndexType {
        IndexType::RTree
    }

    async fn calculate_included_frags(&self) -> Result<RoaringBitmap> {
        let mut frag_ids = RoaringBitmap::default();

        let mut reader_stream = self.clone().into_data_stream().await?;
        while let Some(page) = reader_stream.try_next().await? {
            let mut page_frag_ids = page
                .column(1)
                .as_primitive::<UInt64Type>()
                .iter()
                .flatten()
                .map(|row_addr| RowAddress::from(row_addr).fragment_id())
                .collect::<Vec<_>>();
            page_frag_ids.sort();
            page_frag_ids.dedup();
            frag_ids |= RoaringBitmap::from_sorted_iter(page_frag_ids).unwrap();
        }
        Ok(frag_ids)
    }
}

#[async_trait]
impl ScalarIndex for RTreeIndex {
    async fn search(
        &self,
        query: &dyn AnyQuery,
        metrics: &dyn MetricsCollector,
    ) -> Result<SearchResult> {
        let query = query.as_any().downcast_ref::<GeoQuery>().unwrap();
        match query {
            GeoQuery::IntersectQuery(query) => {
                let geo_array =
                    extract_bounding_boxes(query.value.to_array()?.as_ref(), &query.field)?;
                let bbox = total_bounds(&geo_array)?;
                let mut rowids = self.search_bbox(bbox, metrics).await?;
                let mut null_map = self.search_null(metrics).await?;

                if let Some(fri) = &self.frag_reuse_index {
                    rowids = fri.remap_row_addrs_tree_map(&rowids);
                    null_map = fri.remap_row_addrs_tree_map(&null_map);
                }
                Ok(SearchResult::AtMost(NullableRowAddrSet::new(
                    rowids, null_map,
                )))
            }
            GeoQuery::IsNull => {
                let mut null_map = self.search_null(metrics).await?;

                if let Some(fri) = &self.frag_reuse_index {
                    null_map = fri.remap_row_addrs_tree_map(&null_map);
                }
                Ok(SearchResult::Exact(NullableRowAddrSet::new(
                    null_map,
                    RowAddrTreeMap::default(),
                )))
            }
        }
    }

    fn can_remap(&self) -> bool {
        false
    }

    async fn remap(
        &self,
        _mapping: &RowAddrRemap,
        _dest_store: &dyn IndexStore,
    ) -> Result<CreatedIndex> {
        Err(Error::invalid_input_source(
            "RTree does not support remap".into(),
        ))
    }

    async fn update(
        &self,
        new_data: SendableRecordBatchStream,
        dest_store: &dyn IndexStore,
        _old_data_filter: Option<super::OldIndexDataFilter>,
    ) -> Result<CreatedIndex> {
        let bbox_data = RTreeIndexPlugin::convert_bbox_stream(new_data)?;
        let tmpdir = Arc::new(TempDir::default());
        let spill_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));
        let (new_bbox_data, stats) = RTreeIndexPlugin::process_and_analyze_bbox_stream(
            bbox_data,
            self.metadata.page_size,
            spill_store.clone(),
        )
        .await?;

        let merged_bbox_data = self.clone().combine_old_new(new_bbox_data).await?;

        let null_map = self.search_null(&NoOpMetricsCollector).await?;

        let mut new_bbox = BoundingBox::new();
        new_bbox.add_rect(&stats.total_bbox);
        new_bbox.add_rect(&self.metadata.bbox);

        let merge_stats = BboxStreamStats {
            null_map: RowAddrTreeMap::union_all(&[&null_map, &stats.null_map]),
            total_bbox: new_bbox,
            num_items: self.metadata.num_items + stats.num_items,
        };

        let files = RTreeIndexPlugin::train_rtree_index(
            merged_bbox_data,
            merge_stats,
            self.metadata.page_size,
            dest_store,
        )
        .await?;

        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&pb::RTreeIndexDetails::default())?,
            index_version: RTREE_INDEX_VERSION,
            files,
        })
    }

    fn update_criteria(&self) -> UpdateCriteria {
        UpdateCriteria::only_new_data(TrainingCriteria::new(TrainingOrdering::None).with_row_id())
    }

    fn derive_index_params(&self) -> Result<ScalarIndexParams> {
        let params = serde_json::to_value(RTreeParameters {
            page_size: Some(self.metadata.page_size),
        })?;
        Ok(ScalarIndexParams::for_builtin(BuiltinIndexType::RTree).with_params(&params))
    }
}

/// Parameters for a rtree index
#[derive(Debug, Serialize, Deserialize, Clone)]
struct RTreeParameters {
    /// The number of rows to include in each page
    pub page_size: Option<u32>,
}

pub struct RTreeTrainingRequest {
    parameters: RTreeParameters,
    criteria: TrainingCriteria,
}

impl RTreeTrainingRequest {
    fn new(parameters: RTreeParameters) -> Self {
        Self {
            parameters,
            criteria: TrainingCriteria::new(TrainingOrdering::None).with_row_id(),
        }
    }
}

impl Default for RTreeTrainingRequest {
    fn default() -> Self {
        Self::new(RTreeParameters {
            page_size: Some(DEFAULT_RTREE_PAGE_SIZE),
        })
    }
}

impl TrainingRequest for RTreeTrainingRequest {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn criteria(&self) -> &TrainingCriteria {
        &self.criteria
    }
}

#[derive(Debug, Default)]
pub struct RTreeIndexPlugin;

impl RTreeIndexPlugin {
    fn validate_schema(schema: &ArrowSchema) -> Result<()> {
        if schema.fields().len() != 2 {
            return Err(Error::invalid_input_source(
                "RTree index schema must have exactly two fields".into(),
            ));
        }

        let row_id_field = schema.field_with_name(ROW_ID)?;
        if *row_id_field.data_type() != DataType::UInt64 {
            return Err(Error::invalid_input_source(
                "Second field in RTree index schema must be of type UInt64".into(),
            ));
        }
        Ok(())
    }

    fn convert_bbox_stream(source: SendableRecordBatchStream) -> Result<SendableRecordBatchStream> {
        let bbox_stream = source
            .map_err(DataFusionError::into)
            .and_then(move |batch| async move {
                let schema = batch.schema();
                let geometry_field = schema.field(0);
                let geometry_array = batch.column(0);
                let bbox_array = extract_bounding_boxes(geometry_array, geometry_field)?;

                let bbox_schema = Arc::new(ArrowSchema::new(vec![
                    bbox_array.extension_type().clone().to_field("bbox", true),
                    ArrowField::new(ROW_ID, DataType::UInt64, false),
                ]));
                RecordBatch::try_new(
                    bbox_schema,
                    vec![bbox_array.into_array_ref(), batch.column(1).clone()],
                )
                .map_err(DataFusionError::from)
            });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            BBOX_ROWID_SCHEMA.clone(),
            bbox_stream,
        )))
    }

    /// Processes a bounding box data stream, separating null and non-null elements, and collects
    /// statistics about non-null elements.
    async fn process_and_analyze_bbox_stream(
        mut data: SendableRecordBatchStream,
        page_size: u32,
        spill_store: Arc<LanceIndexStore>,
    ) -> Result<(SendableRecordBatchStream, BboxStreamStats)> {
        let mut null_rowaddrs = RowAddrTreeMap::new();
        let mut total_bbox = BoundingBox::new();
        let mut num_non_null_rows = 0;

        let schema = data.schema();

        let mut writer = spill_store
            .new_index_file("analyze.tmp", BBOX_ROWID_SCHEMA.clone())
            .await?;

        while let Some(batch) = data.try_next().await? {
            let bbox_array = extract_bounding_boxes(&batch.column(0), batch.schema().field(0))?;
            let rowaddr_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            total_bbox.add_geo_arrow_array(&bbox_array)?;

            let num_rows = bbox_array.len();

            let mut non_null_indexes = vec![];

            for i in 0..num_rows {
                if bbox_array.is_null(i) {
                    let rowaddr = rowaddr_array.value(i);
                    null_rowaddrs.insert(rowaddr);
                } else {
                    non_null_indexes.push(i as u32);
                }
            }

            let new_batch = if non_null_indexes.is_empty() {
                // all nulls, skip write
                continue;
            } else if non_null_indexes.len() == num_rows {
                batch
            } else {
                batch.take(&UInt32Array::from(non_null_indexes))?
            };

            num_non_null_rows += new_batch.num_rows();
            writer.write_record_batch(new_batch).await?;
        }
        writer.finish().await?;
        let reader = spill_store.open_index_file("analyze.tmp").await?;
        let stream = IndexReaderStream::new(reader, page_size as u64)
            .await
            .map(|fut| fut.map_err(DataFusionError::from))
            .buffered(spill_store.io_parallelism())
            .boxed();
        let new_data = RecordBatchStreamAdapter::new(schema.clone(), stream);

        Ok((
            Box::pin(new_data),
            BboxStreamStats {
                null_map: null_rowaddrs,
                total_bbox,
                num_items: num_non_null_rows,
            },
        ))
    }

    async fn train_rtree_page(
        batch: RecordBatch,
        page_id: u64,
        writer: &mut dyn IndexWriter,
    ) -> Result<EncodedBatch> {
        let geo_array = extract_bounding_boxes(batch.column(0).as_ref(), batch.schema().field(0))?;
        let bbox = total_bounds(&geo_array)?;
        let new_batch = RecordBatch::try_new(
            RTREE_PAGE_SCHEMA.clone(),
            vec![batch.column(0).clone(), batch.column(1).clone()],
        )?;
        writer.write_record_batch(new_batch).await?;
        Ok(EncodedBatch { bbox, page_id })
    }

    fn encoded_batches_into_batch_stream(
        batches: Vec<EncodedBatch>,
        batch_size: u32,
    ) -> SendableRecordBatchStream {
        let batches = batches
            .chunks(batch_size as usize)
            .map(|chunk| {
                let bbox_type = RectType::new(Dimension::XY, Default::default());
                let mut bbox_builder = RectBuilder::with_capacity(bbox_type, chunk.len());
                let mut page_ids = UInt64Array::builder(chunk.len());

                for item in chunk {
                    bbox_builder.push_rect(Some(&item.bbox));
                    page_ids.append_value(item.page_id);
                }

                RecordBatch::try_new(
                    RTREE_PAGE_SCHEMA.clone(),
                    vec![
                        bbox_builder.finish().into_array_ref(),
                        Arc::new(page_ids.finish()),
                    ],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        Box::pin(RecordBatchStreamAdapter::new(
            RTREE_PAGE_SCHEMA.clone(),
            stream::iter(batches).map(Ok).boxed(),
        ))
    }

    pub async fn write_index(
        sorted_data: SendableRecordBatchStream,
        num_items: usize,
        total_bbox: BoundingBox,
        store: &dyn IndexStore,
        page_size: u32,
    ) -> Result<IndexFile> {
        validate_page_size(page_size)?;
        let mut page_idx: u64 = 0;
        let mut writer = store
            .new_index_file(RTREE_PAGES_NAME, RTREE_PAGE_SCHEMA.clone())
            .await?;

        if num_items > 0 {
            let mut current_level = Some((sorted_data, num_items));
            while let Some((mut data, num_items)) = current_level.take() {
                if num_items <= page_size as usize {
                    while let Some(batch) = data.try_next().await? {
                        Self::train_rtree_page(batch, page_idx, writer.as_mut()).await?;
                        page_idx += 1;
                    }
                } else {
                    let mut next_level = vec![];
                    let mut paged_source = chunk_concat_stream(data, page_size as usize);
                    while let Some(batch) = paged_source.try_next().await? {
                        let encoded_batch =
                            Self::train_rtree_page(batch, page_idx, writer.as_mut()).await?;
                        page_idx += 1;
                        next_level.push(encoded_batch);
                    }
                    if !next_level.is_empty() {
                        let next_num_items = next_level.len();
                        current_level = Some((
                            Self::encoded_batches_into_batch_stream(next_level, page_size),
                            next_num_items,
                        ));
                    }
                }
            }
        }

        writer
            .finish_with_metadata(
                RTreeMetadata::new(page_size, page_idx, num_items, total_bbox).into_map(),
            )
            .await
    }

    pub async fn write_nulls(
        store: &dyn IndexStore,
        null_map: RowAddrTreeMap,
    ) -> Result<IndexFile> {
        let mut writer = store
            .new_index_file(RTREE_NULLS_NAME, RTREE_NULLS_SCHEMA.clone())
            .await?;
        let mut bytes = Vec::new();
        null_map.serialize_into(&mut bytes)?;
        let batch = RecordBatch::try_new(
            RTREE_NULLS_SCHEMA.clone(),
            vec![Arc::new(BinaryArray::from_vec(vec![&bytes]))],
        )?;

        writer.write_record_batch(batch).await?;
        writer.finish().await
    }

    async fn train_rtree_index(
        bbox_data: SendableRecordBatchStream,
        stats: BboxStreamStats,
        page_size: u32,
        store: &dyn IndexStore,
    ) -> Result<Vec<IndexFile>> {
        // new sorted stream
        let sorter = HilbertSorter::new(stats.total_bbox);
        let sorted_data = sorter.sort(bbox_data).await?;

        let page_file = Self::write_index(
            sorted_data,
            stats.num_items,
            stats.total_bbox,
            store,
            page_size,
        )
        .await?;

        let nulls_file = Self::write_nulls(store, stats.null_map).await?;

        Ok(vec![page_file, nulls_file])
    }
}

#[async_trait]
impl BasicTrainer for RTreeIndexPlugin {
    fn new_training_request(
        &self,
        params: &str,
        _field: &ArrowField,
    ) -> Result<Box<dyn TrainingRequest>> {
        let params = serde_json::from_str::<RTreeParameters>(params)?;
        if let Some(page_size) = params.page_size {
            validate_page_size(page_size)?;
        }
        Ok(Box::new(RTreeTrainingRequest::new(params)))
    }

    async fn train_index(
        &self,
        data: SendableRecordBatchStream,
        index_store: &dyn IndexStore,
        request: Box<dyn TrainingRequest>,
        _fragment_ids: Option<Vec<u32>>,
        _progress: Arc<dyn crate::progress::IndexBuildProgress>,
    ) -> Result<CreatedIndex> {
        Self::validate_schema(&data.schema())?;

        let request = request
            .as_any()
            .downcast_ref::<RTreeTrainingRequest>()
            .unwrap();
        let page_size = request
            .parameters
            .page_size
            .unwrap_or(DEFAULT_RTREE_PAGE_SIZE);
        validate_page_size(page_size)?;

        let bbox_data = Self::convert_bbox_stream(data)?;
        let tmpdir = Arc::new(TempDir::default());
        let spill_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));
        let (bbox_data, stats) =
            Self::process_and_analyze_bbox_stream(bbox_data, page_size, spill_store.clone())
                .await?;

        let files = Self::train_rtree_index(bbox_data, stats, page_size, index_store).await?;

        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&pb::RTreeIndexDetails::default())?,
            index_version: RTREE_INDEX_VERSION,
            files,
        })
    }
}

#[async_trait]
impl ScalarIndexPlugin for RTreeIndexPlugin {
    fn basic_trainer(&self) -> Option<&dyn BasicTrainer> {
        Some(self)
    }

    fn name(&self) -> &str {
        "RTree"
    }

    fn provides_exact_answer(&self) -> bool {
        false
    }

    fn version(&self) -> u32 {
        RTREE_INDEX_VERSION
    }

    fn new_query_parser(
        &self,
        index_name: String,
        _index_details: &prost_types::Any,
    ) -> Option<Box<dyn ScalarQueryParser>> {
        Some(Box::new(GeoQueryParser::new(
            index_name,
            self.name().to_string(),
        )))
    }

    async fn load_index(
        &self,
        index_store: Arc<dyn IndexStore>,
        _index_details: &prost_types::Any,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        cache: &LanceCache,
    ) -> Result<Arc<dyn ScalarIndex>> {
        Ok(RTreeIndex::load(index_store, frag_reuse_index, cache).await? as Arc<dyn ScalarIndex>)
    }
}

struct EncodedBatch {
    bbox: BoundingBox,
    page_id: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frag_reuse::{FragReuseIndex, FragReuseIndexDetails, FragReuseIndexHandle};
    use crate::metrics::NoOpMetricsCollector;
    use crate::scalar::registry::VALUE_COLUMN_NAME;
    use arrow_array::ArrayRef;
    use arrow_schema::Schema;
    use geo_types::{Rect, coord};
    use geoarrow_array::builder::{PointBuilder, RectBuilder};
    use geoarrow_schema::{Dimension, PointType, RectType};
    use lance_core::utils::tempfile::TempObjDir;
    use rand::Rng;

    fn expected_num_pages(num_items: usize, page_size: u32) -> u64 {
        RTreeMetadata::calculate_page_offsets(num_items, page_size).len() as u64
    }

    #[test]
    fn test_rejects_page_size_that_cannot_reduce_tree_levels() {
        let plugin = RTreeIndexPlugin;
        let field = ArrowField::new("geometry", DataType::Null, true);
        let error = plugin
            .new_training_request(r#"{"page_size":1}"#, &field)
            .err()
            .unwrap();
        assert!(error.to_string().contains("page_size must be at least 2"));
    }

    #[test]
    fn test_stored_page_size_preserves_single_item_compatibility() {
        assert!(validate_stored_page_size(1, 1).is_ok());
        assert!(validate_stored_page_size(1, 0).is_ok());
        assert!(validate_stored_page_size(1, 2).is_err());
        assert!(validate_stored_page_size(0, 0).is_err());
    }

    fn convert_bbox_rowid_batch_stream(
        geo_array: &dyn GeoArrowArray,
        row_id_array: ArrayRef,
    ) -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::new(vec![
            geo_array.data_type().to_field(VALUE_COLUMN_NAME, true),
            ArrowField::new(ROW_ID, DataType::UInt64, false),
        ]));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![geo_array.to_array_ref(), row_id_array])
                .unwrap();

        let stream = stream::once(async move { Ok(batch) });
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }

    async fn train_index(
        geo_array: &dyn GeoArrowArray,
        page_size: Option<u32>,
    ) -> (Arc<RTreeIndex>, Arc<LanceIndexStore>, TempObjDir) {
        let page_size = page_size.unwrap_or(DEFAULT_RTREE_PAGE_SIZE);
        let mut num_items = 0;
        for i in 0..geo_array.len() {
            if !geo_array.is_null(i) {
                num_items += 1;
            }
        }

        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let stream = convert_bbox_rowid_batch_stream(
            geo_array,
            Arc::new(UInt64Array::from(
                (0..geo_array.len() as u64).collect::<Vec<_>>(),
            )),
        );

        let plugin = RTreeIndexPlugin;
        plugin
            .train_index(
                stream,
                store.as_ref(),
                Box::new(RTreeTrainingRequest::new(RTreeParameters {
                    page_size: Some(page_size),
                })),
                None,
                crate::progress::noop_progress(),
            )
            .await
            .unwrap();

        let pages_reader = store.open_index_file(RTREE_PAGES_NAME).await.unwrap();
        let metadata = RTreeMetadata::from(&pages_reader.schema().metadata);
        assert_eq!(metadata.num_items, num_items);
        assert_eq!(metadata.num_pages, expected_num_pages(num_items, page_size));

        (
            RTreeIndex::load(store.clone(), None, &LanceCache::no_cache())
                .await
                .unwrap(),
            store,
            tmpdir,
        )
    }

    #[tokio::test]
    async fn test_search_bbox() {
        let bbox_type = RectType::new(Dimension::XY, Default::default());

        let mut rng = rand::rng();
        let mut rect_builder = RectBuilder::new(bbox_type.clone());
        let num_items = 10000;
        let page_size = 16;

        for _ in 0..num_items {
            let x1 = rng.random_range(-1000.0..1000.0);
            let y1 = rng.random_range(-1000.0..1000.0);
            let x2 = rng.random_range(x1..x1 + 10.0);
            let y2 = rng.random_range(y1..y1 + 10.0);

            rect_builder.push_rect(Some(&Rect::new(
                coord! { x: x1, y: y1 },
                coord! { x: x2, y: y2 },
            )));
        }
        let rect_arr = rect_builder.finish();

        let (rtree_index, _store, _tmpdir) = train_index(&rect_arr, Some(page_size)).await;

        let mut search_bbox = BoundingBox::new();
        search_bbox.add_rect(&Rect::new(
            coord! { x: 10.5, y: 1.5 },
            coord! { x: 99.5, y: 200.5 },
        ));
        let row_ids = rtree_index
            .search_bbox(search_bbox, &NoOpMetricsCollector)
            .await
            .unwrap();

        let mut expected_row_ids = RowAddrTreeMap::new();
        for i in 0..rect_arr.len() {
            let mut bbox = BoundingBox::new();
            bbox.add_rect(&rect_arr.value(i).unwrap());
            if search_bbox.rect_intersects(&bbox) {
                expected_row_ids.insert(i as u64);
            }
        }
        assert_eq!(row_ids, expected_row_ids);
    }

    #[tokio::test]
    async fn test_search_null() {
        let point_type = PointType::new(Dimension::XY, Default::default());

        let mut rng = rand::rng();
        let num_points = 10000;
        let null_probability = 0.001; // 0.1%

        let mut expected_nulls = Vec::new();
        let mut point_builder = PointBuilder::new(point_type.clone());

        for i in 0..num_points {
            if rng.random_bool(null_probability) {
                point_builder.push_null();
                expected_nulls.push(RowAddress::new_from_parts(0, i as u32));
            } else {
                let x = rng.random_range(-1000.0..1000.0);
                let y = rng.random_range(-1000.0..1000.0);
                point_builder.push_point(Some(&geo_types::point!(x: x, y: y)));
            }
        }
        let point_arr = point_builder.finish();

        let (rtree_index, _store, _tmpdir) = train_index(&point_arr, None).await;
        let row_addrs = rtree_index
            .search_null(&NoOpMetricsCollector)
            .await
            .unwrap();

        let mut actual_nulls = row_addrs.row_addrs().unwrap().collect::<Vec<_>>();
        actual_nulls.sort();
        expected_nulls.sort();

        assert_eq!(actual_nulls, expected_nulls);
    }

    #[tokio::test]
    async fn test_merge_rtree_indices_filters_rows_and_nulls() {
        let point_type = PointType::new(Dimension::XY, Default::default());
        let mut first_builder = PointBuilder::new(point_type.clone());
        first_builder.push_point(Some(&geo_types::point!(x: 10.0, y: 10.0)));
        first_builder.push_null();
        let first = first_builder.finish();

        let mut second_builder = PointBuilder::new(point_type);
        second_builder.push_point(Some(&geo_types::point!(x: 100.0, y: 100.0)));
        second_builder.push_null();
        let second = second_builder.finish();
        let empty = PointBuilder::new(PointType::new(Dimension::XY, Default::default())).finish();

        let (empty_index, _empty_store, _empty_tmpdir) = train_index(&empty, Some(8)).await;
        let (first_index, _first_store, _first_tmpdir) = train_index(&first, Some(4)).await;
        let (second_index, _second_store, _second_tmpdir) = train_index(&second, Some(4)).await;
        let mut near_origin = BoundingBox::new();
        near_origin.add_rect(&Rect::new(
            coord! { x: 9.0, y: 9.0 },
            coord! { x: 11.0, y: 11.0 },
        ));
        let mut expected_geometry = RowAddrTreeMap::new();
        expected_geometry.insert(0);
        assert_eq!(
            first_index
                .search_bbox(near_origin, &NoOpMetricsCollector)
                .await
                .unwrap(),
            expected_geometry
        );
        let merged_tmpdir = TempObjDir::default();
        let merged_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            merged_tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let remapped_geometry = RowAddress::new_from_parts(2, 0).into();
        let remapped_null = RowAddress::new_from_parts(2, 1).into();
        expected_geometry = RowAddrTreeMap::new();
        expected_geometry.insert(remapped_geometry);
        let remapper = FragReuseIndexHandle(Arc::new(FragReuseIndex::new(
            uuid::Uuid::new_v4(),
            vec![HashMap::from([
                (0, Some(remapped_geometry)),
                (1, Some(remapped_null)),
            ])],
            FragReuseIndexDetails { versions: vec![] },
        )));
        let mut first_index = first_index.as_ref().clone();
        first_index.frag_reuse_index = Some(Arc::new(remapper));

        let mut keep_first_rows = RowAddrTreeMap::new();
        keep_first_rows.insert(remapped_geometry);
        keep_first_rows.insert(remapped_null);
        merge_rtree_indices(
            &[empty_index, Arc::new(first_index), second_index],
            merged_store.as_ref(),
            &[
                None,
                Some(OldIndexDataFilter::RowIds(keep_first_rows)),
                Some(OldIndexDataFilter::RowIds(RowAddrTreeMap::new())),
            ],
        )
        .await
        .unwrap();

        let merged = RTreeIndex::load(merged_store, None, &LanceCache::no_cache())
            .await
            .unwrap();
        assert_eq!(merged.metadata.num_items, 1);
        assert_eq!(merged.metadata.bbox.minx(), 10.0);
        assert_eq!(merged.metadata.bbox.miny(), 10.0);
        assert_eq!(merged.metadata.bbox.maxx(), 10.0);
        assert_eq!(merged.metadata.bbox.maxy(), 10.0);
        assert!(
            merged.metadata.bbox.rect_intersects(&near_origin),
            "merged bounds {:?} do not intersect {:?}",
            merged.metadata.bbox,
            near_origin
        );
        assert_eq!(
            merged
                .search_bbox(near_origin, &NoOpMetricsCollector)
                .await
                .unwrap(),
            expected_geometry
        );
        let mut expected_null = RowAddrTreeMap::new();
        expected_null.insert(remapped_null);
        assert_eq!(
            merged.search_null(&NoOpMetricsCollector).await.unwrap(),
            expected_null
        );
    }

    #[tokio::test]
    async fn test_update_and_search() {
        fn gen_data(num_items: u32, frag_id: u32, nulls_addrs: &mut RowAddrTreeMap) -> RectArray {
            let bbox_type = RectType::new(Dimension::XY, Default::default());

            let mut rng = rand::rng();
            let null_probability = 0.001;
            let mut rect_builder = RectBuilder::new(bbox_type);

            for i in 0..num_items {
                if rng.random_bool(null_probability) {
                    rect_builder.push_null();
                    nulls_addrs.insert(RowAddress::new_from_parts(frag_id, i).into());
                } else {
                    let x1 = rng.random_range(-1000.0..1000.0);
                    let y1 = rng.random_range(-1000.0..1000.0);
                    let x2 = rng.random_range(x1..x1 + 10.0);
                    let y2 = rng.random_range(y1..y1 + 10.0);

                    rect_builder.push_rect(Some(&Rect::new(
                        coord! { x: x1, y: y1 },
                        coord! { x: x2, y: y2 },
                    )));
                }
            }
            rect_builder.finish()
        }

        let mut nulls_addrs = RowAddrTreeMap::default();

        let frag_id = 0;
        let rect_arr = gen_data(10000, frag_id, &mut nulls_addrs);

        let (rtree_index, _store, _tmpdir) = train_index(&rect_arr, Some(16)).await;

        let tmpdir = TempObjDir::default();
        let new_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let new_frag_id = 1;
        let new_rect_arr = gen_data(10000, 1, &mut nulls_addrs);
        let new_rowaddr_arr = (0..new_rect_arr.len())
            .map(|off| RowAddress::new_from_parts(new_frag_id, off as u32).into())
            .collect::<Vec<_>>();
        let stream = convert_bbox_rowid_batch_stream(
            &new_rect_arr,
            Arc::new(UInt64Array::from(new_rowaddr_arr.clone())),
        );
        rtree_index
            .update(stream, new_store.as_ref(), None)
            .await
            .unwrap();

        let new_rtree_index = RTreeIndex::load(new_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        let mut search_bbox = BoundingBox::new();
        search_bbox.add_rect(&Rect::new(
            coord! { x: 10.5, y: 1.5 },
            coord! { x: 99.5, y: 200.5 },
        ));
        let row_addrs = new_rtree_index
            .search_bbox(search_bbox, &NoOpMetricsCollector)
            .await
            .unwrap();

        let mut expected_row_addrs = RowAddrTreeMap::new();
        for i in 0..rect_arr.len() {
            if !rect_arr.is_null(i) {
                let bbox = BoundingBox::new_with_rect(&rect_arr.value(i).unwrap());
                if search_bbox.rect_intersects(&bbox) {
                    expected_row_addrs.insert(i as u64);
                }
            }
        }
        for i in 0..new_rect_arr.len() {
            if !new_rect_arr.is_null(i) {
                let bbox = BoundingBox::new_with_rect(&new_rect_arr.value(i).unwrap());
                if search_bbox.rect_intersects(&bbox) {
                    expected_row_addrs.insert(new_rowaddr_arr.get(i).copied().unwrap());
                }
            }
        }

        assert_eq!(row_addrs, expected_row_addrs);

        let actual_nulls = new_rtree_index
            .search_null(&NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(actual_nulls, nulls_addrs);
    }

    #[tokio::test]
    async fn test_prewarm() {
        let point_type = PointType::new(Dimension::XY, Default::default());

        let mut rng = rand::rng();
        let num_points = 1000;
        let null_probability = 0.1;

        let mut point_builder = PointBuilder::new(point_type.clone());

        for _ in 0..num_points {
            if rng.random_bool(null_probability) {
                point_builder.push_null();
            } else {
                let x = rng.random_range(-1000.0..1000.0);
                let y = rng.random_range(-1000.0..1000.0);
                point_builder.push_point(Some(&geo_types::point!(x: x, y: y)));
            }
        }
        let point_arr = point_builder.finish();

        let (_, store, _tmpdir) = train_index(&point_arr, Some(32)).await;

        let cache = LanceCache::with_capacity(10 << 20);
        let rtree_index = RTreeIndex::load(store, None, &cache).await.unwrap();

        // Call prewarm
        rtree_index.prewarm().await.unwrap();

        for page_id in 0..rtree_index.metadata.num_pages {
            assert!(
                rtree_index
                    .index_cache
                    .get_with_key(&RTreeCacheKey::Page(page_id))
                    .await
                    .is_some()
            )
        }

        assert!(
            rtree_index
                .index_cache
                .get_with_key(&RTreeCacheKey::Nulls)
                .await
                .is_some()
        )
    }
}
