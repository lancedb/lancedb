// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::utils::row_addr_remap::RowAddrRemap;
use std::{
    any::Any,
    collections::HashMap,
    fmt::Debug,
    pin::Pin,
    sync::{Arc, Mutex},
};

use arrow::array::AsArray;
use arrow_array::{Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
use datafusion_common::ScalarValue;
use futures::{StreamExt, TryStream, TryStreamExt, stream::BoxStream};
use lance_core::cache::{
    CacheCodec, CacheCodecImpl, CacheEntryReader, CacheEntryWriter, CacheKey, CacheKeySchema,
    KeyBuilder, LanceCache,
};
use lance_core::deepsize::DeepSizeOf;
use lance_core::error::LanceOptionExt;
use lance_core::{Error, ROW_ID, Result};
use lance_select::{NullableRowAddrSet, RowAddrTreeMap, RowSetOps};
use roaring::RoaringBitmap;
use tracing::instrument;

use super::{
    AnyQuery, IndexFile, IndexStore, LabelListQuery, OldIndexDataFilter, ScalarIndex,
    bitmap::BitmapIndex,
};
use super::{BuiltinIndexType, SargableQuery, ScalarIndexParams};
use super::{MetricsCollector, SearchResult};
use crate::pbold;
use crate::scalar::bitmap::{BitmapIndexPlugin, BitmapIndexState};
use crate::scalar::expression::{LabelListQueryParser, ScalarQueryParser};
use crate::scalar::registry::{
    BasicTrainer, DefaultTrainingRequest, ScalarIndexLoad, ScalarIndexPlugin, TrainingCriteria,
    TrainingOrdering, TrainingRequest, VALUE_COLUMN_NAME, single_flight_open,
};
use crate::scalar::{CreatedIndex, RowIdRemapper, UpdateCriteria};
use crate::{Index, IndexType};

pub const BITMAP_LOOKUP_NAME: &str = "bitmap_page_lookup.lance";
pub const LABEL_LIST_NULLS_METADATA_KEY: &str = "lance:label_list_nulls";
pub const LABEL_LIST_NULLS_MIN_VERSION: i32 = 1;
const LABEL_LIST_INDEX_VERSION: u32 = 1;

#[async_trait]
trait LabelListSubIndex: ScalarIndex + DeepSizeOf {
    async fn search_exact(
        &self,
        query: &dyn AnyQuery,
        metrics: &dyn MetricsCollector,
    ) -> Result<NullableRowAddrSet> {
        let result = self.search(query, metrics).await?;
        match result {
            SearchResult::Exact(row_ids) => {
                // Label list semantics treat NULL elements as non-matches, so only TRUE/FALSE
                // results should remain for array_has_any/array_has_all when the list itself
                // is non-NULL. Clear nulls to avoid propagating element-level NULLs.
                Ok(row_ids.with_nulls(RowAddrTreeMap::new()))
            }
            _ => Err(Error::internal(
                "Label list sub-index should return exact results".to_string(),
            )),
        }
    }
}

impl<T: ScalarIndex + DeepSizeOf> LabelListSubIndex for T {}

/// A scalar index that can be used on `List<T>` columns to
/// accelerate list membership filters such as `array_has_all`, `array_has_any`,
/// and `array_has` / `array_contains`, using an underlying bitmap index.
#[derive(Clone, Debug, DeepSizeOf)]
pub struct LabelListIndex {
    values_index: Arc<BitmapIndex>,
    list_nulls: Arc<RowAddrTreeMap>,
}

impl LabelListIndex {
    fn new(values_index: Arc<BitmapIndex>, list_nulls: Arc<RowAddrTreeMap>) -> Self {
        Self {
            values_index,
            list_nulls,
        }
    }

    async fn load(
        store: Arc<dyn IndexStore>,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        index_cache: &LanceCache,
    ) -> Result<Arc<Self>> {
        let values_index =
            BitmapIndex::load(store.clone(), frag_reuse_index.clone(), index_cache).await?;
        let list_nulls = read_list_nulls(store, frag_reuse_index).await?;
        Ok(Arc::new(Self::new(values_index, Arc::new(list_nulls))))
    }
}

#[async_trait]
impl Index for LabelListIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
        self
    }

    async fn prewarm(&self) -> Result<()> {
        self.values_index.prewarm().await
    }

    fn index_type(&self) -> IndexType {
        IndexType::LabelList
    }

    fn statistics(&self) -> Result<serde_json::Value> {
        self.values_index.statistics()
    }

    async fn calculate_included_frags(&self) -> Result<RoaringBitmap> {
        unimplemented!()
    }
}

impl LabelListIndex {
    fn search_values<'a>(
        &'a self,
        values: &'a Vec<ScalarValue>,
        metrics: &'a dyn MetricsCollector,
    ) -> BoxStream<'a, Result<NullableRowAddrSet>> {
        futures::stream::iter(values)
            .then(move |value| {
                let value_query = SargableQuery::Equals(value.clone());
                async move { self.values_index.search_exact(&value_query, metrics).await }
            })
            .boxed()
    }

    async fn set_union<'a>(
        &'a self,
        mut sets: impl TryStream<Ok = NullableRowAddrSet, Error = Error> + 'a + Unpin,
        single_set: bool,
    ) -> Result<NullableRowAddrSet> {
        let mut union_bitmap = sets.try_next().await?.unwrap();
        if single_set {
            return Ok(union_bitmap);
        }
        while let Some(next) = sets.try_next().await? {
            union_bitmap |= &next;
        }
        Ok(union_bitmap)
    }

    async fn set_intersection<'a>(
        &'a self,
        mut sets: impl TryStream<Ok = NullableRowAddrSet, Error = Error> + 'a + Unpin,
        single_set: bool,
    ) -> Result<NullableRowAddrSet> {
        let mut intersect_bitmap = sets.try_next().await?.unwrap();
        if single_set {
            return Ok(intersect_bitmap);
        }
        while let Some(next) = sets.try_next().await? {
            intersect_bitmap &= &next;
        }
        Ok(intersect_bitmap)
    }
}

#[async_trait]
impl ScalarIndex for LabelListIndex {
    #[instrument(skip_all, level = "debug")]
    async fn search(
        &self,
        query: &dyn AnyQuery,
        metrics: &dyn MetricsCollector,
    ) -> Result<SearchResult> {
        let query = query.as_any().downcast_ref::<LabelListQuery>().unwrap();

        let row_ids = match query {
            LabelListQuery::HasAllLabels(labels) => {
                let values_results = self.search_values(labels, metrics);
                self.set_intersection(values_results, labels.len() == 1)
                    .await
            }
            LabelListQuery::HasAnyLabel(labels) => {
                let values_results = self.search_values(labels, metrics);
                self.set_union(values_results, labels.len() == 1).await
            }
        }?;
        let row_ids = if self.list_nulls.as_ref().is_empty() {
            row_ids
        } else {
            let mut nulls = row_ids.null_rows().clone();
            nulls |= self.list_nulls.as_ref();
            row_ids.with_nulls(nulls)
        };
        Ok(SearchResult::Exact(row_ids))
    }

    fn can_remap(&self) -> bool {
        true
    }

    /// Remap the row ids, creating a new remapped version of this index in `dest_store`
    async fn remap(
        &self,
        mapping: &RowAddrRemap,
        dest_store: &dyn IndexStore,
    ) -> Result<CreatedIndex> {
        let state = self.values_index.load_bitmap_index_state().await?;
        let remapped_state = BitmapIndexPlugin::remap_bitmap_state(state, mapping);
        let remapped_nulls =
            RowAddrTreeMap::from_iter(self.list_nulls.row_addrs().unwrap().filter_map(|addr| {
                let addr_as_u64 = u64::from(addr);
                mapping.get(addr_as_u64).unwrap_or(Some(addr_as_u64))
            }));
        let file = write_label_list_bitmap_index(
            remapped_state,
            dest_store,
            self.values_index.value_type(),
            &remapped_nulls,
        )
        .await?;

        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&pbold::LabelListIndexDetails::default())
                .unwrap(),
            index_version: LABEL_LIST_INDEX_VERSION,
            files: vec![file],
        })
    }

    /// Add the new data into the index, creating an updated version of the index in `dest_store`
    async fn update(
        &self,
        new_data: SendableRecordBatchStream,
        dest_store: &dyn IndexStore,
        old_data_filter: Option<super::OldIndexDataFilter>,
    ) -> Result<CreatedIndex> {
        let state = self.values_index.load_bitmap_index_state().await?;
        let list_nulls = Arc::new(Mutex::new(RowAddrTreeMap::new()));
        let new_data = track_list_nulls(new_data, list_nulls.clone());
        let (merged_state, value_type) =
            BitmapIndexPlugin::build_bitmap_index_state(unnest_chunks(new_data)?, state).await?;
        let _ = old_data_filter;
        let mut merged_nulls = (*self.list_nulls).clone();
        let new_nulls = list_nulls.lock().unwrap().clone();
        if !new_nulls.is_empty() {
            merged_nulls |= &new_nulls;
        }
        let file =
            write_label_list_bitmap_index(merged_state, dest_store, &value_type, &merged_nulls)
                .await?;

        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&pbold::LabelListIndexDetails::default())
                .unwrap(),
            index_version: LABEL_LIST_INDEX_VERSION,
            files: vec![file],
        })
    }

    fn update_criteria(&self) -> UpdateCriteria {
        UpdateCriteria::only_new_data(TrainingCriteria::new(TrainingOrdering::None).with_row_id())
    }

    fn derive_index_params(&self) -> Result<ScalarIndexParams> {
        Ok(ScalarIndexParams::for_builtin(BuiltinIndexType::LabelList))
    }
}

fn extract_flatten_indices(list_arr: &dyn Array) -> UInt64Array {
    if let Some(list_arr) = list_arr.as_list_opt::<i32>() {
        let mut indices = Vec::with_capacity(list_arr.values().len());
        let offsets = list_arr.value_offsets();
        for (offset_idx, w) in offsets.windows(2).enumerate() {
            let size = (w[1] - w[0]) as u64;
            indices.extend((0..size).map(|_| offset_idx as u64));
        }
        UInt64Array::from(indices)
    } else if let Some(list_arr) = list_arr.as_list_opt::<i64>() {
        let mut indices = Vec::with_capacity(list_arr.values().len());
        let offsets = list_arr.value_offsets();
        for (offset_idx, w) in offsets.windows(2).enumerate() {
            let size = (w[1] - w[0]) as u64;
            indices.extend((0..size).map(|_| offset_idx as u64));
        }
        UInt64Array::from(indices)
    } else {
        unreachable!(
            "Should verify that the first column is a list earlier. Got array of type: {}",
            list_arr.data_type()
        )
    }
}

/// Collect row_ids for list-level NULLs before unnest; unnest drops NULL lists entirely.
fn track_list_nulls(
    source: SendableRecordBatchStream,
    list_nulls: Arc<Mutex<RowAddrTreeMap>>,
) -> SendableRecordBatchStream {
    let schema = source.schema();
    let stream = source.try_filter_map(move |batch| {
        let list_nulls = list_nulls.clone();
        async move {
            record_list_nulls(&batch, &list_nulls)?;
            Ok(Some(batch))
        }
    });

    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

fn record_list_nulls(
    batch: &RecordBatch,
    list_nulls: &Arc<Mutex<RowAddrTreeMap>>,
) -> datafusion_common::Result<()> {
    let values = batch.column_by_name(VALUE_COLUMN_NAME).expect_ok()?;
    let row_ids = batch.column_by_name(ROW_ID).expect_ok()?;
    let row_ids = row_ids.as_any().downcast_ref::<UInt64Array>().unwrap();

    let mut local_nulls = RowAddrTreeMap::new();
    for i in 0..values.len() {
        if values.is_null(i) {
            local_nulls.insert(row_ids.value(i));
        }
    }
    if !local_nulls.is_empty() {
        let mut guard = list_nulls.lock().unwrap();
        *guard |= &local_nulls;
    }
    Ok(())
}

fn unnest_schema(schema: &Schema) -> SchemaRef {
    let mut fields_iter = schema.fields.iter().cloned();
    let key_field = fields_iter.next().unwrap();
    let remaining_fields = fields_iter.collect::<Vec<_>>();

    let new_key_field = match key_field.data_type() {
        DataType::List(item_field) | DataType::LargeList(item_field) => Field::new(
            key_field.name(),
            item_field.data_type().clone(),
            item_field.is_nullable() || key_field.is_nullable(),
        ),
        other_type => {
            unreachable!(
                "The first field in the schema must be a List or LargeList type. \
                Found: {}. This should have been verified earlier in the code.",
                other_type
            )
        }
    };

    let all_fields = vec![Arc::new(new_key_field)]
        .into_iter()
        .chain(remaining_fields)
        .collect::<Vec<_>>();

    Arc::new(Schema::new(Fields::from(all_fields)))
}

fn unnest_batch(
    batch: arrow::record_batch::RecordBatch,
    unnest_schema: SchemaRef,
) -> datafusion_common::Result<RecordBatch> {
    let mut columns_iter = batch.columns().iter().cloned();
    let key_col = columns_iter.next().unwrap();
    let remaining_cols = columns_iter.collect::<Vec<_>>();

    let remaining_fields = unnest_schema
        .fields
        .iter()
        .skip(1)
        .cloned()
        .collect::<Vec<_>>();

    let remaining_batch = RecordBatch::try_new(
        Arc::new(Schema::new(Fields::from(remaining_fields))),
        remaining_cols,
    )?;

    let flatten_indices = extract_flatten_indices(key_col.as_ref());

    let flattened_remaining =
        arrow_select::take::take_record_batch(&remaining_batch, &flatten_indices)?;

    let new_key_values = if let Some(key_list) = key_col.as_list_opt::<i32>() {
        let value_start = key_list.value_offsets()[key_list.offset()] as usize;
        let value_stop = key_list.value_offsets()[key_list.len()] as usize;
        key_list
            .values()
            .slice(value_start, value_stop - value_start)
            .clone()
    } else if let Some(key_list) = key_col.as_list_opt::<i64>() {
        let value_start = key_list.value_offsets()[key_list.offset()] as usize;
        let value_stop = key_list.value_offsets()[key_list.len()] as usize;
        key_list
            .values()
            .slice(value_start, value_stop - value_start)
            .clone()
    } else {
        unreachable!("Should verify that the first column is a list earlier")
    };

    let all_columns = vec![new_key_values]
        .into_iter()
        .chain(flattened_remaining.columns().iter().cloned())
        .collect::<Vec<_>>();

    datafusion_common::Result::Ok(arrow::record_batch::RecordBatch::try_new(
        unnest_schema,
        all_columns,
    )?)
}

fn unnest_chunks(
    source: Pin<Box<dyn RecordBatchStream + Send>>,
) -> Result<SendableRecordBatchStream> {
    let unnest_schema = unnest_schema(source.schema().as_ref());
    let unnest_schema_copy = unnest_schema.clone();
    let source = source.try_filter_map(move |batch| {
        std::future::ready(Some(unnest_batch(batch, unnest_schema.clone())).transpose())
    });

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        unnest_schema_copy,
        source,
    )))
}

async fn read_list_nulls(
    store: Arc<dyn IndexStore>,
    frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
) -> Result<RowAddrTreeMap> {
    let reader = store.open_index_file(BITMAP_LOOKUP_NAME).await?;
    if let Some(buffer_idx_str) = reader.schema().metadata.get(LABEL_LIST_NULLS_METADATA_KEY) {
        let buffer_idx = buffer_idx_str.parse::<u32>().map_err(|err| {
            Error::internal(format!(
                "LabelList metadata key {} had invalid global buffer index {}: {}",
                LABEL_LIST_NULLS_METADATA_KEY, buffer_idx_str, err
            ))
        })?;
        let bytes = reader.read_global_buffer(buffer_idx).await?;
        let null_map = RowAddrTreeMap::deserialize_from(bytes.as_ref())?;
        return if let Some(frag_reuse_index) = frag_reuse_index {
            Ok(frag_reuse_index.remap_row_addrs_tree_map(&null_map))
        } else {
            Ok(null_map)
        };
    }
    Ok(RowAddrTreeMap::default())
}

fn serialize_list_nulls(null_map: &RowAddrTreeMap) -> Result<Bytes> {
    let mut bytes = Vec::new();
    null_map.serialize_into(&mut bytes)?;
    Ok(Bytes::from(bytes))
}

async fn write_label_list_bitmap_index(
    state: HashMap<ScalarValue, RowAddrTreeMap>,
    store: &dyn IndexStore,
    value_type: &DataType,
    list_nulls: &RowAddrTreeMap,
) -> Result<IndexFile> {
    BitmapIndexPlugin::write_bitmap_index_with_extras(
        state,
        store,
        value_type,
        HashMap::new(),
        vec![(
            LABEL_LIST_NULLS_METADATA_KEY.to_string(),
            serialize_list_nulls(list_nulls)?,
        )],
    )
    .await
}

/// Merge multiple LabelList index segments into a single index.
///
/// A [`LabelListIndex`] is a [`BitmapIndex`] over the unnested list values plus a
/// separate `list_nulls` row set. Because distributed segments cover disjoint rows
/// (distinct fragments), merging is a cheap union of the underlying bitmap states
/// and of the `list_nulls` sets — no re-scan of source data is required. This
/// mirrors [`crate::scalar::bitmap::merge_bitmap_indices`] but also carries the
/// per-segment `list_nulls`. When `old_data_filter` is provided, rows from
/// retired fragments are removed from both the value bitmaps and `list_nulls`.
pub async fn merge_label_list_indices(
    source_indices: &[Arc<LabelListIndex>],
    dest_store: &dyn IndexStore,
    old_data_filter: Option<OldIndexDataFilter>,
    progress: Arc<dyn crate::progress::IndexBuildProgress>,
) -> Result<CreatedIndex> {
    if source_indices.is_empty() {
        return Err(Error::invalid_input(
            "LabelList segment merge requires at least one source segment".to_string(),
        ));
    }

    let value_type = source_indices[0].values_index.value_type().clone();
    let mut merged_state = HashMap::<ScalarValue, RowAddrTreeMap>::new();
    let mut merged_nulls = RowAddrTreeMap::new();

    progress
        .stage_start(
            "merge_label_list_segments",
            Some(source_indices.len() as u64),
            "segments",
        )
        .await?;
    for (idx, source_index) in source_indices.iter().enumerate() {
        if source_index.values_index.value_type() != &value_type {
            return Err(Error::invalid_input(format!(
                "LabelList segment has value type {:?}, expected {:?}",
                source_index.values_index.value_type(),
                value_type
            )));
        }

        let state = source_index.values_index.load_bitmap_index_state().await?;
        for (key, mut bitmap) in state {
            if let Some(filter) = old_data_filter.as_ref() {
                filter.retain_old_rows(&mut bitmap);
            }
            if bitmap.is_empty() {
                continue;
            }
            merged_state
                .entry(key)
                .and_modify(|existing| *existing |= &bitmap)
                .or_insert(bitmap);
        }
        let mut list_nulls = source_index.list_nulls.as_ref().clone();
        if let Some(filter) = old_data_filter.as_ref() {
            filter.retain_old_rows(&mut list_nulls);
        }
        merged_nulls |= &list_nulls;
        progress
            .stage_progress("merge_label_list_segments", (idx + 1) as u64)
            .await?;
    }
    progress.stage_complete("merge_label_list_segments").await?;

    progress
        .stage_start("write_label_list_index", Some(1), "files")
        .await?;
    let file =
        write_label_list_bitmap_index(merged_state, dest_store, &value_type, &merged_nulls).await?;
    progress.stage_progress("write_label_list_index", 1).await?;
    progress.stage_complete("write_label_list_index").await?;

    Ok(CreatedIndex {
        index_details: prost_types::Any::from_msg(&pbold::LabelListIndexDetails::default())
            .unwrap(),
        index_version: LABEL_LIST_INDEX_VERSION,
        files: vec![file],
    })
}

/// The serializable state of a [`LabelListIndex`].
///
/// `LabelListIndex` is a thin wrapper around a [`BitmapIndex`] plus a separate
/// row bitmap tracking which list values were `NULL` (lost by unnest at build
/// time). Its cache state is the corresponding [`BitmapIndexState`] plus the
/// already-loaded `list_nulls`.
#[derive(Debug, Clone)]
pub struct LabelListIndexState {
    bitmap_state: BitmapIndexState,
    list_nulls: Arc<RowAddrTreeMap>,
}

impl DeepSizeOf for LabelListIndexState {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.bitmap_state.deep_size_of_children(context)
            + self.list_nulls.deep_size_of_children(context)
    }
}

impl LabelListIndexState {
    fn from_index(index: &LabelListIndex) -> Result<Self> {
        Ok(Self {
            bitmap_state: BitmapIndexState::from_index(&index.values_index)?,
            list_nulls: index.list_nulls.clone(),
        })
    }

    fn from_scalar_index(index: &dyn ScalarIndex) -> Result<Self> {
        let label_list = index
            .as_any()
            .downcast_ref::<LabelListIndex>()
            .ok_or_else(|| {
                Error::internal(
                    "LabelListIndexState::from_scalar_index called with a non-label-list index",
                )
            })?;
        Self::from_index(label_list)
    }

    fn into_label_list_index(
        self,
        store: Arc<dyn IndexStore>,
        index_cache: &LanceCache,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
    ) -> Result<Arc<LabelListIndex>> {
        let bitmap = self
            .bitmap_state
            .to_bitmap_index(store, index_cache, frag_reuse_index)?;
        Ok(Arc::new(LabelListIndex::new(bitmap, self.list_nulls)))
    }
}

impl CacheCodecImpl for LabelListIndexState {
    const TYPE_ID: &'static str = "lance.scalar.LabelListIndexState";
    const CURRENT_VERSION: u32 = 1;

    /// Wire format:
    /// ```text
    /// RAW_BLOB : list_nulls (roaring tree map, portable encoding)
    /// <nested BitmapIndexState body (self-delimiting)>
    /// ```
    fn serialize(&self, w: &mut CacheEntryWriter<'_>) -> Result<()> {
        let mut nulls_bytes = Vec::with_capacity(self.list_nulls.serialized_size());
        self.list_nulls.serialize_into(&mut nulls_bytes)?;
        w.write_raw(&nulls_bytes)?;
        // The bitmap state writes its own self-delimiting body inline.
        self.bitmap_state.serialize(w)?;
        Ok(())
    }

    fn deserialize(r: &mut CacheEntryReader<'_>) -> Result<Self> {
        let nulls_bytes = r.read_raw()?;
        let list_nulls = Arc::new(RowAddrTreeMap::deserialize_from(nulls_bytes.as_ref())?);
        // The bitmap state is self-delimiting (length-prefixed null map +
        // Arrow IPC stream with EOS marker); it continues reading the body
        // from where the null map left off.
        let bitmap_state = BitmapIndexState::deserialize(r)?;
        Ok(Self {
            bitmap_state,
            list_nulls,
        })
    }
}

struct LabelListIndexStateKey;

impl CacheKey for LabelListIndexStateKey {
    type ValueType = LabelListIndexState;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        "state".into()
    }

    fn type_name() -> &'static str {
        "LabelListIndexState"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.scalar.label-list-index-state-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_variant(0);
    }

    fn codec() -> Option<CacheCodec> {
        Some(CacheCodec::from_impl::<LabelListIndexState>())
    }
}

#[derive(Debug, Default)]
pub struct LabelListIndexPlugin;

pub(super) fn validate_label_list_data_type(data_type: &DataType) -> Result<()> {
    let item_type = match data_type {
        DataType::List(item_field) | DataType::LargeList(item_field) => item_field.data_type(),
        _ => {
            return Err(Error::invalid_input_source(
                format!(
                    "LabelList index can only be created on List or LargeList type columns. Column has type {:?}",
                    data_type
                )
                .into(),
            ));
        }
    };

    if item_type.is_nested() {
        return Err(Error::invalid_input_source(
            format!(
                "LabelList index item type must be non-nested. Column has type {:?}",
                data_type
            )
            .into(),
        ));
    }

    Ok(())
}

#[async_trait]
impl BasicTrainer for LabelListIndexPlugin {
    fn new_training_request(
        &self,
        _params: &str,
        field: &Field,
    ) -> Result<Box<dyn TrainingRequest>> {
        validate_label_list_data_type(field.data_type())?;

        Ok(Box::new(DefaultTrainingRequest::new(
            TrainingCriteria::new(TrainingOrdering::None).with_row_id(),
        )))
    }

    /// Train a new index
    ///
    /// The provided data must fulfill all the criteria returned by `training_criteria`
    /// and the plugin can rely on this fact.
    async fn train_index(
        &self,
        data: SendableRecordBatchStream,
        index_store: &dyn IndexStore,
        _request: Box<dyn TrainingRequest>,
        // Training over a fragment subset is supported for distributed builds: the
        // provided `data` stream is already scoped to those fragments, so a partial
        // index covering exactly those rows is produced. Segments are recombined by
        // `merge_label_list_indices`.
        _fragment_ids: Option<Vec<u32>>,
        _progress: Arc<dyn crate::progress::IndexBuildProgress>,
    ) -> Result<CreatedIndex> {
        let schema = data.schema();
        let field = schema
            .column_with_name(VALUE_COLUMN_NAME)
            .ok_or_else(|| {
                Error::invalid_input_source(
                    "Index training data missing value column"
                        .to_string()
                        .into(),
                )
            })?
            .1;

        validate_label_list_data_type(field.data_type())?;

        let list_nulls = Arc::new(Mutex::new(RowAddrTreeMap::new()));
        let data = track_list_nulls(data, list_nulls.clone());
        let data = unnest_chunks(data)?;
        let (state, value_type) =
            BitmapIndexPlugin::build_bitmap_index_state(data, HashMap::new()).await?;
        let list_nulls = list_nulls.lock().unwrap().clone();
        let file =
            write_label_list_bitmap_index(state, index_store, &value_type, &list_nulls).await?;
        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&pbold::LabelListIndexDetails::default())
                .unwrap(),
            index_version: LABEL_LIST_INDEX_VERSION,
            files: vec![file],
        })
    }
}

#[async_trait]
impl ScalarIndexPlugin for LabelListIndexPlugin {
    fn basic_trainer(&self) -> Option<&dyn BasicTrainer> {
        Some(self)
    }

    fn name(&self) -> &str {
        "LabelList"
    }

    fn provides_exact_answer(&self) -> bool {
        true
    }

    fn version(&self) -> u32 {
        LABEL_LIST_INDEX_VERSION
    }

    fn new_query_parser(
        &self,
        index_name: String,
        _index_details: &prost_types::Any,
    ) -> Option<Box<dyn ScalarQueryParser>> {
        Some(Box::new(LabelListQueryParser::new(
            index_name,
            self.name().to_string(),
        )))
    }

    /// Load an index from storage
    async fn load_index(
        &self,
        index_store: Arc<dyn IndexStore>,
        _index_details: &prost_types::Any,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        cache: &LanceCache,
    ) -> Result<Arc<dyn ScalarIndex>> {
        Ok(
            LabelListIndex::load(index_store, frag_reuse_index, cache).await?
                as Arc<dyn ScalarIndex>,
        )
    }

    async fn get_from_cache(
        &self,
        index_store: Arc<dyn IndexStore>,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        cache: &LanceCache,
    ) -> Result<Option<Arc<dyn ScalarIndex>>> {
        let Some(state) = cache.get_with_key(&LabelListIndexStateKey).await else {
            return Ok(None);
        };
        let state = (*state).clone();
        let index = state.into_label_list_index(index_store, cache, frag_reuse_index)?;
        Ok(Some(index as Arc<dyn ScalarIndex>))
    }

    async fn put_in_cache(&self, cache: &LanceCache, index: Arc<dyn ScalarIndex>) -> Result<()> {
        let state = LabelListIndexState::from_scalar_index(index.as_ref())?;
        cache
            .insert_with_key(&LabelListIndexStateKey, Arc::new(state))
            .await;
        Ok(())
    }

    async fn get_or_insert_in_cache(
        &self,
        index_store: Arc<dyn IndexStore>,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        cache: &LanceCache,
        load: ScalarIndexLoad<'_>,
    ) -> Result<Arc<dyn ScalarIndex>> {
        single_flight_open(
            cache,
            LabelListIndexStateKey,
            load,
            LabelListIndexState::from_scalar_index,
            move |state| {
                Ok((*state)
                    .clone()
                    .into_label_list_index(index_store, cache, frag_reuse_index)?
                    as Arc<dyn ScalarIndex>)
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use datafusion_common::ScalarValue;
    use lance_core::cache::CacheCodec;
    use lance_core::utils::address::RowAddress;
    use rstest::rstest;

    use super::super::bitmap::BitmapIndexState;
    use super::super::btree::OrderableScalarValue;
    use super::*;

    #[rstest]
    #[case::list(DataType::List(Arc::new(Field::new(
        "item",
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    ))))]
    #[case::large_list(DataType::LargeList(Arc::new(Field::new(
        "item",
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    ))))]
    fn test_rejects_nested_item_type(#[case] data_type: DataType) {
        let field = Field::new(VALUE_COLUMN_NAME, data_type, true);
        let error = LabelListIndexPlugin
            .new_training_request("", &field)
            .err()
            .expect("nested item type should be rejected");

        assert!(
            matches!(error, Error::InvalidInput { .. }),
            "expected invalid input error, got: {error}"
        );
        assert!(
            error
                .to_string()
                .contains("LabelList index item type must be non-nested"),
            "unexpected error: {error}"
        );
    }

    fn sample_state() -> LabelListIndexState {
        let mut index_map = BTreeMap::new();
        for k in 0..32i32 {
            index_map.insert(
                OrderableScalarValue(ScalarValue::Int32(Some(k))),
                k as usize,
            );
        }
        let mut bitmap_nulls = RowAddrTreeMap::new();
        bitmap_nulls.insert(RowAddress::new_from_parts(0, 3).into());
        let bitmap_state =
            BitmapIndexState::new_for_test(index_map, bitmap_nulls, DataType::Int32).unwrap();

        let mut list_nulls = RowAddrTreeMap::new();
        list_nulls.insert(RowAddress::new_from_parts(0, 9).into());
        LabelListIndexState {
            bitmap_state,
            list_nulls: Arc::new(list_nulls),
        }
    }

    #[test]
    fn test_label_list_state_codec_roundtrip() {
        let state = sample_state();
        let mut buf = Vec::new();
        state
            .serialize(&mut CacheEntryWriter::new(&mut buf))
            .unwrap();
        let data = Bytes::from(buf);
        let mut reader = CacheEntryReader::new(&data, 0, LabelListIndexState::CURRENT_VERSION);
        let restored = LabelListIndexState::deserialize(&mut reader).unwrap();

        assert_eq!(&*restored.list_nulls, &*state.list_nulls);
        assert_eq!(
            restored.bitmap_state.lookup_batch(),
            state.bitmap_state.lookup_batch()
        );
        assert_eq!(
            restored.bitmap_state.null_map(),
            state.bitmap_state.null_map()
        );
    }

    /// The nested bitmap lookup batch must decode zero-copy through the full
    /// envelope, proving the leading `list_nulls` RAW_BLOB does not knock the
    /// nested IPC section off its 64-byte boundary.
    #[test]
    fn test_label_list_nested_lookup_is_zero_copy() {
        const ALIGN: usize = 64;
        let codec = CacheCodec::from_impl::<LabelListIndexState>();
        let any: Arc<dyn std::any::Any + Send + Sync> = Arc::new(sample_state());
        let mut buf = Vec::new();
        codec.serialize(&any, &mut buf).unwrap();

        let mut v = vec![0u8; buf.len() + ALIGN];
        let pad = (ALIGN - (v.as_ptr() as usize % ALIGN)) % ALIGN;
        v[pad..pad + buf.len()].copy_from_slice(&buf);
        let data = Bytes::from(v).slice(pad..pad + buf.len());

        let restored = codec.deserialize(&data).hit().unwrap();
        let restored = restored.downcast::<LabelListIndexState>().unwrap();

        let base = data.as_ptr() as usize;
        let end = base + data.len();
        for col in restored.bitmap_state.lookup_batch().columns() {
            for buffer in col.to_data().buffers() {
                let ptr = buffer.as_ptr() as usize;
                assert!(
                    ptr >= base && ptr < end,
                    "nested bitmap lookup buffer was realigned — misaligned IPC section",
                );
            }
        }
    }
}
