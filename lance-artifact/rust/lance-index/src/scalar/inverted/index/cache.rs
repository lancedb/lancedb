// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

/// New type just to allow Positions implement DeepSizeOf so it can be put
/// in the cache.
#[derive(Clone)]
pub struct Positions(pub(in super::super) CompressedPositionStorage);

/// Slice-aware cache-size charge for the Arrow array shapes stored in posting
/// caches. [`Array::get_buffer_memory_size`] reports the full capacity of shared
/// backing buffers; cached posting lists often reference only a small slice of a
/// group read. Count the referenced span for the known posting-list types and
/// fall back to Arrow's full-buffer size for anything else.
pub(super) fn sliced_cache_bytes(array: &dyn Array) -> usize {
    let validity_bytes = array
        .nulls()
        .map(|nulls| nulls.len().div_ceil(8))
        .unwrap_or(0);
    match array.data_type() {
        DataType::LargeBinary => {
            let array = array.as_binary::<i64>();
            let data_bytes = if array.is_empty() {
                0
            } else {
                let offsets = array.value_offsets();
                (offsets[array.len()] - offsets[0]) as usize
            };
            data_bytes + (array.len() + 1) * std::mem::size_of::<i64>() + validity_bytes
        }
        DataType::List(_) => {
            let array = array.as_list::<i32>();
            let (child_start, child_end) = if array.is_empty() {
                (0, 0)
            } else {
                let offsets = array.value_offsets();
                (offsets[0] as usize, offsets[array.len()] as usize)
            };
            let offset_bytes = (array.len() + 1) * std::mem::size_of::<i32>();
            let child = array.values().slice(child_start, child_end - child_start);
            offset_bytes + validity_bytes + sliced_cache_bytes(child.as_ref())
        }
        // Fixed-width primitives hold exactly `len * width` bytes regardless of
        // buffer capacity, so this is already slice-aware. Any other type falls
        // back to the full-buffer size.
        other => match other.primitive_width() {
            Some(width) => array.len() * width + validity_bytes,
            None => array.get_buffer_memory_size(),
        },
    }
}

impl DeepSizeOf for Positions {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.0.deep_size_of_children(context)
    }
}

// Cache key implementations for type-safe cache access
#[derive(Debug, Clone)]
pub struct PostingListKey {
    pub token_id: u32,
}

impl CacheKey for PostingListKey {
    type ValueType = PostingList;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        format!("postings-{}", self.token_id).into()
    }

    fn type_name() -> &'static str {
        "PostingList"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.scalar.inverted.posting-list-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u32(self.token_id);
    }

    fn codec() -> Option<CacheCodec> {
        Some(CacheCodec::from_impl::<PostingList>())
    }
}

/// Cache key for a group of consecutive posting lists stored as a single
/// entry, covering rows `[start, end)` (issue #7040). The range, not a token
/// id, is the key so a runtime group-size change simply misses old entries
/// instead of serving a differently-shaped group.
#[derive(Debug, Clone)]
pub struct PostingListGroupKey {
    pub start: u32,
    pub end: u32,
}

impl CacheKey for PostingListGroupKey {
    type ValueType = PostingListGroup;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        format!("postings-{}-{}", self.start, self.end).into()
    }

    fn type_name() -> &'static str {
        "PostingListGroup"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.scalar.inverted.posting-list-group-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u32(self.start);
        builder.write_u32(self.end);
    }

    fn codec() -> Option<CacheCodec> {
        Some(CacheCodec::from_impl::<PostingListGroup>())
    }
}

/// Internal cache-key decorator that isolates impact-bearing posting values
/// without changing the source-compatible public posting key structs.
#[derive(Debug, Clone)]
pub(super) struct ImpactAwareCacheKey<K> {
    inner: K,
    has_impacts: bool,
}

impl<K: CacheKey> CacheKey for ImpactAwareCacheKey<K> {
    type ValueType = K::ValueType;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        if self.has_impacts {
            format!("{}-impacts", self.inner.key()).into()
        } else {
            self.inner.key()
        }
    }

    fn type_name() -> &'static str {
        K::type_name()
    }

    fn stable_type_id() -> &'static str {
        K::stable_type_id()
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.scalar.inverted.impact-aware-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        let inner_schema = K::schema();
        builder.write_str(K::stable_type_id());
        builder.write_str(inner_schema.id());
        builder.write_u32(inner_schema.version());
        builder.write_variant(if self.has_impacts { 1 } else { 0 });
        self.inner.write_key(builder);
    }

    fn codec() -> Option<CacheCodec> {
        K::codec()
    }
}

pub(super) fn posting_list_cache_key(
    token_id: u32,
    has_impacts: bool,
) -> ImpactAwareCacheKey<PostingListKey> {
    ImpactAwareCacheKey {
        inner: PostingListKey { token_id },
        has_impacts,
    }
}

pub(super) fn posting_list_group_cache_key(
    start: u32,
    end: u32,
    has_impacts: bool,
) -> ImpactAwareCacheKey<PostingListGroupKey> {
    ImpactAwareCacheKey {
        inner: PostingListGroupKey { start, end },
        has_impacts,
    }
}

#[derive(Debug, Clone, DeepSizeOf)]
pub(super) struct PostingMetadataValue {
    pub(super) max_score: f32,
    pub(super) length: u32,
}

#[derive(Debug, Clone)]
pub(super) struct PostingMetadataKey {
    pub(super) token_id: u32,
}

impl CacheKey for PostingMetadataKey {
    type ValueType = PostingMetadataValue;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        format!("posting-metadata-{}", self.token_id).into()
    }

    fn type_name() -> &'static str {
        "PostingMetadata"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.scalar.inverted.posting-metadata-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u32(self.token_id);
    }
}

#[derive(Debug, Clone)]
pub struct PositionKey {
    pub token_id: u32,
}

impl CacheKey for PositionKey {
    type ValueType = Positions;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        format!("positions-{}", self.token_id).into()
    }

    fn type_name() -> &'static str {
        "Position"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.scalar.inverted.position-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u32(self.token_id);
    }

    fn codec() -> Option<CacheCodec> {
        Some(CacheCodec::from_impl::<Positions>())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompressedPositionStorage {
    LegacyPerDoc(ListArray),
    SharedStream(SharedPositionStream),
}

impl DeepSizeOf for CompressedPositionStorage {
    fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
        match self {
            Self::LegacyPerDoc(positions) => sliced_cache_bytes(positions),
            Self::SharedStream(stream) => stream.size(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SharedPositionStream {
    codec: PositionStreamCodec,
    block_offsets: Arc<[u32]>,
    // Stored with shared ownership so cache hits can clone position streams
    // without copying either offsets or bytes.
    bytes: bytes::Bytes,
}

impl SharedPositionStream {
    pub fn new(codec: PositionStreamCodec, block_offsets: Vec<u32>, bytes: bytes::Bytes) -> Self {
        Self {
            codec,
            block_offsets: Arc::from(block_offsets.into_boxed_slice()),
            bytes,
        }
    }

    pub fn codec(&self) -> PositionStreamCodec {
        self.codec
    }

    pub fn block_count(&self) -> usize {
        self.block_offsets.len()
    }

    pub fn block_range(&self, index: usize) -> Range<usize> {
        let start = self.block_offsets[index] as usize;
        let end = self
            .block_offsets
            .get(index + 1)
            .map(|offset| *offset as usize)
            .unwrap_or(self.bytes.len());
        start..end
    }

    pub fn block(&self, index: usize) -> &[u8] {
        let range = self.block_range(index);
        &self.bytes[range]
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn block_offsets(&self) -> &[u32] {
        self.block_offsets.as_ref()
    }

    pub fn size(&self) -> usize {
        self.block_offsets.len() * std::mem::size_of::<u32>() + self.bytes.len()
    }
}

/// A group of consecutive posting lists held in a single cache entry, in row
/// order (issue #7040). Prewarmed modern groups without positions retain only
/// the compact Arrow posting rows read from `invert.lance`; max-score/length
/// metadata stays in the reader and is injected when a query creates a
/// posting-list view. Cold-loaded groups may keep inline metadata to preserve
/// one-read query loading. Legacy and position-bearing prewarm paths use the
/// materialized fallback.
#[derive(Debug, Clone)]
pub struct PostingListGroup {
    pub(in super::super) storage: PostingListGroupStorage,
}

#[derive(Debug, Clone)]
pub(in super::super) enum PostingListGroupStorage {
    Packed(PackedPostingListGroup),
    Materialized(Vec<PostingList>),
}

#[derive(Debug, Clone)]
pub(in super::super) struct PackedPostingListGroup {
    pub(in super::super) batch: RecordBatch,
    pub(in super::super) posting_tail_codec: PostingTailCodec,
    pub(in super::super) block_size: usize,
    pub(super) first_docs_states: Arc<[OnceLock<Box<[u32]>>]>,
    pub(super) first_docs_state_capacity_bytes: usize,
    pub(super) impact_states: Option<Arc<[OnceLock<Box<ImpactSkipData>>]>>,
    pub(super) impact_state_capacity_bytes: usize,
}

impl DeepSizeOf for PostingListGroup {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        match &self.storage {
            PostingListGroupStorage::Packed(group) => group
                .batch
                .columns()
                .iter()
                .map(|column| sliced_cache_bytes(column.as_ref()))
                .sum::<usize>()
                .saturating_add(group.first_docs_state_capacity_bytes)
                .saturating_add(group.impact_state_capacity_bytes),
            PostingListGroupStorage::Materialized(posting_lists) => {
                posting_lists.deep_size_of_children(context)
            }
        }
    }
}

impl PostingListGroup {
    pub(in super::super) fn new(posting_lists: Vec<PostingList>) -> Self {
        Self {
            storage: PostingListGroupStorage::Materialized(posting_lists),
        }
    }

    pub(in super::super) fn new_packed(
        batch: RecordBatch,
        posting_tail_codec: PostingTailCodec,
    ) -> Result<Self> {
        let block_size = parse_posting_block_size(batch.schema_ref().metadata())?;
        Self::new_packed_with_block_size(batch, posting_tail_codec, block_size)
    }

    pub(super) fn new_packed_with_block_size(
        batch: RecordBatch,
        posting_tail_codec: PostingTailCodec,
        block_size: usize,
    ) -> Result<Self> {
        validate_block_size(block_size)?;
        if let Some(encoded_block_size) = batch.schema_ref().metadata().get(POSTING_BLOCK_SIZE_KEY)
        {
            let encoded_block_size = encoded_block_size.parse::<usize>().map_err(|err| {
                Error::index(format!(
                    "invalid {POSTING_BLOCK_SIZE_KEY} metadata value {encoded_block_size:?}: {err}"
                ))
            })?;
            if encoded_block_size != block_size {
                return Err(Error::index(format!(
                    "packed posting group {POSTING_BLOCK_SIZE_KEY}={encoded_block_size} does not match block_size={block_size}"
                )));
            }
        }

        // Projected reads may drop schema metadata. Restore the reader's
        // validated block size before the batch enters the packed cache so IPC
        // roundtrips remain self-describing. Older packed cache entries omit
        // the key and enter through new_packed with the legacy 128-doc default.
        let mut schema = batch.schema().as_ref().clone();
        schema
            .metadata
            .insert(POSTING_BLOCK_SIZE_KEY.to_owned(), block_size.to_string());
        let batch = batch.with_schema(Arc::new(schema))?;
        let postings = batch
            .column_by_name(POSTING_COL)
            .and_then(|column| column.as_list_opt::<i32>())
            .ok_or_else(|| {
                Error::index(format!(
                    "packed posting group column {POSTING_COL} must be List<LargeBinary>"
                ))
            })?;
        if postings.values().data_type() != &DataType::LargeBinary {
            return Err(Error::index(format!(
                "packed posting group column {POSTING_COL} must contain LargeBinary values, got {}",
                postings.values().data_type()
            )));
        }
        if postings.null_count() != 0 {
            return Err(Error::index(
                "packed posting group column must not contain nulls".to_string(),
            ));
        }
        let total_posting_blocks = (0..batch.num_rows())
            .map(|slot| postings.value_length(slot) as usize)
            .sum::<usize>();
        let first_docs_states: Arc<[OnceLock<Box<[u32]>>]> = (0..batch.num_rows())
            .map(|_| OnceLock::new())
            .collect::<Vec<_>>()
            .into();
        // Reserve the compact per-slot state slab and the block-head arrays it
        // can lazily retain, so warming these derived values cannot grow the
        // cache beyond its admission charge.
        let first_docs_state_capacity_bytes = first_docs_states
            .len()
            .saturating_mul(std::mem::size_of::<OnceLock<Box<[u32]>>>())
            .saturating_add(total_posting_blocks.saturating_mul(std::mem::size_of::<u32>()));
        let (impact_states, impact_state_capacity_bytes) = if let Some(impacts) =
            batch.column_by_name(IMPACT_COL)
        {
            let impacts = impacts.as_list_opt::<i32>().ok_or_else(|| {
                Error::index(format!(
                    "packed posting group column {IMPACT_COL} must be List<LargeBinary>"
                ))
            })?;
            if impacts.values().data_type() != &DataType::LargeBinary {
                return Err(Error::index(format!(
                    "packed posting group column {IMPACT_COL} must contain LargeBinary values, got {}",
                    impacts.values().data_type()
                )));
            }
            if impacts.null_count() != 0 {
                return Err(Error::index(format!(
                    "packed posting group column {IMPACT_COL} must not contain nulls"
                )));
            }
            let mut derived_cache_bytes = 0usize;
            for slot in 0..batch.num_rows() {
                let posting_blocks = postings.value_length(slot) as usize;
                let impact_entries = impacts.value_length(slot) as usize;
                let expected_impact_entries =
                    posting_blocks.saturating_add(posting_blocks.div_ceil(IMPACT_LEVEL1_BLOCKS));
                if impact_entries != expected_impact_entries {
                    return Err(Error::index(format!(
                        "packed posting group impact slot {slot} has {impact_entries} entries, expected {expected_impact_entries} for {posting_blocks} posting blocks"
                    )));
                }
                derived_cache_bytes = derived_cache_bytes.saturating_add(
                    ImpactSkipData::derived_cache_bytes_for_entries(impact_entries),
                );
            }

            let states: Arc<[OnceLock<Box<ImpactSkipData>>]> = (0..batch.num_rows())
                .map(|_| OnceLock::new())
                .collect::<Vec<_>>()
                .into();
            // Account up front for every allocation that the lazy states can
            // eventually retain. The impact entry bytes themselves remain in
            // `batch` and are already charged exactly once above.
            let per_slot_bytes = std::mem::size_of::<OnceLock<Box<ImpactSkipData>>>()
                .saturating_add(std::mem::size_of::<ImpactSkipData>());
            let capacity_bytes = states
                .len()
                .saturating_mul(per_slot_bytes)
                .saturating_add(derived_cache_bytes);
            (Some(states), capacity_bytes)
        } else {
            (None, 0)
        };
        match (
            batch.column_by_name(MAX_SCORE_COL),
            batch.column_by_name(LENGTH_COL),
        ) {
            (None, None) => {}
            (Some(max_scores), Some(lengths)) => {
                let max_scores = max_scores
                    .as_primitive_opt::<Float32Type>()
                    .ok_or_else(|| {
                        Error::index(format!(
                            "packed posting group column {MAX_SCORE_COL} must be Float32"
                        ))
                    })?;
                let lengths = lengths.as_primitive_opt::<UInt32Type>().ok_or_else(|| {
                    Error::index(format!(
                        "packed posting group column {LENGTH_COL} must be UInt32"
                    ))
                })?;
                if max_scores.null_count() != 0 || lengths.null_count() != 0 {
                    return Err(Error::index(
                        "packed posting group metadata columns must not contain nulls".to_string(),
                    ));
                }
            }
            _ => {
                return Err(Error::index(format!(
                    "packed posting group must contain both {MAX_SCORE_COL} and {LENGTH_COL}, or neither"
                )));
            }
        }

        Ok(Self {
            storage: PostingListGroupStorage::Packed(PackedPostingListGroup {
                batch,
                posting_tail_codec,
                block_size,
                first_docs_states,
                first_docs_state_capacity_bytes,
                impact_states,
                impact_state_capacity_bytes,
            }),
        })
    }

    pub(in super::super) fn len(&self) -> usize {
        match &self.storage {
            PostingListGroupStorage::Packed(group) => group.batch.num_rows(),
            PostingListGroupStorage::Materialized(posting_lists) => posting_lists.len(),
        }
    }

    #[cfg(test)]
    pub(in super::super) fn is_packed(&self) -> bool {
        matches!(&self.storage, PostingListGroupStorage::Packed(_))
    }

    pub(super) fn needs_external_metadata(&self) -> bool {
        match &self.storage {
            PostingListGroupStorage::Packed(group) => {
                group.batch.column_by_name(MAX_SCORE_COL).is_none()
            }
            PostingListGroupStorage::Materialized(_) => false,
        }
    }

    /// Build an owned posting-list view for `slot`. Packed groups clone only
    /// Arrow array metadata; the compressed posting bytes remain shared with
    /// the group's `List<LargeBinary>` child buffers.
    pub(in super::super) fn posting_list(
        &self,
        slot: usize,
        max_score: Option<f32>,
        length: Option<u32>,
    ) -> Result<Option<PostingList>> {
        match &self.storage {
            PostingListGroupStorage::Materialized(posting_lists) => {
                Ok(posting_lists.get(slot).cloned())
            }
            PostingListGroupStorage::Packed(group) => {
                if slot >= group.batch.num_rows() {
                    return Ok(None);
                }
                let postings = group
                    .batch
                    .column_by_name(POSTING_COL)
                    .and_then(|column| column.as_list_opt::<i32>())
                    .ok_or_else(|| {
                        Error::index(format!(
                            "packed posting group column {POSTING_COL} must be List<LargeBinary>"
                        ))
                    })?;
                let blocks = postings.value(slot);
                let blocks = blocks.as_binary_opt::<i64>().ok_or_else(|| {
                    Error::index(format!(
                        "packed posting group slot {slot} is not LargeBinary"
                    ))
                })?;
                let max_score = match group.batch.column_by_name(MAX_SCORE_COL) {
                    Some(column) => column
                        .as_primitive_opt::<Float32Type>()
                        .expect("packed group metadata was validated at construction")
                        .value(slot),
                    None => max_score.ok_or_else(|| {
                        Error::index("packed posting group requires max-score metadata".to_string())
                    })?,
                };
                let length = match group.batch.column_by_name(LENGTH_COL) {
                    Some(column) => column
                        .as_primitive_opt::<UInt32Type>()
                        .expect("packed group metadata was validated at construction")
                        .value(slot),
                    None => length.ok_or_else(|| {
                        Error::index("packed posting group requires length metadata".to_string())
                    })?,
                };
                let impacts = match (
                    group.impact_states.as_ref(),
                    group.batch.column_by_name(IMPACT_COL),
                ) {
                    (Some(states), Some(column)) => {
                        let state = states.get(slot).ok_or_else(|| {
                            Error::index(format!(
                                "packed posting group impact state missing slot {slot}"
                            ))
                        })?;
                        let impact_lists = column.as_list_opt::<i32>().ok_or_else(|| {
                            Error::index(format!(
                                "packed posting group column {IMPACT_COL} must be List<LargeBinary>"
                            ))
                        })?;
                        let entries = impact_lists.value(slot);
                        let entries = entries.as_binary_opt::<i64>().ok_or_else(|| {
                            Error::index(format!(
                                "packed posting group impact slot {slot} is not LargeBinary"
                            ))
                        })?;
                        let impacts =
                            state.get_or_init(|| {
                                Box::new(ImpactSkipData::new(entries.clone(), blocks.len()).expect(
                                    "packed impact entry count was validated at construction",
                                ))
                            });
                        Some(impacts.as_ref().clone())
                    }
                    (None, None) => None,
                    _ => {
                        return Err(Error::internal(
                            "packed posting group impact column/state mismatch".to_string(),
                        ));
                    }
                };
                Ok(Some(PostingList::Compressed(
                    CompressedPostingList::new(
                        blocks.clone(),
                        max_score,
                        length,
                        group.posting_tail_codec,
                        group.block_size,
                        None,
                        impacts,
                    )
                    .with_packed_first_docs(group.first_docs_states.clone(), slot),
                )))
            }
        }
    }
}
