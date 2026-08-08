// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

#[derive(Debug, Clone, DeepSizeOf, Copy)]
pub enum DocInfo {
    Located(LocatedDocInfo),
    Raw(RawDocInfo),
}

impl DocInfo {
    pub fn doc_id(&self) -> u64 {
        match self {
            Self::Raw(info) => info.doc_id as u64,
            Self::Located(info) => info.row_id,
        }
    }

    pub fn frequency(&self) -> u32 {
        match self {
            Self::Raw(info) => info.frequency,
            Self::Located(info) => info.frequency as u32,
        }
    }
}

impl Eq for DocInfo {}

impl PartialEq for DocInfo {
    fn eq(&self, other: &Self) -> bool {
        self.doc_id() == other.doc_id()
    }
}

impl PartialOrd for DocInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DocInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.doc_id().cmp(&other.doc_id())
    }
}

#[derive(Debug, Clone, Default, DeepSizeOf, Copy)]
pub struct LocatedDocInfo {
    pub row_id: u64,
    pub frequency: f32,
}

impl LocatedDocInfo {
    pub fn new(row_id: u64, frequency: f32) -> Self {
        Self { row_id, frequency }
    }
}

impl Eq for LocatedDocInfo {}

impl PartialEq for LocatedDocInfo {
    fn eq(&self, other: &Self) -> bool {
        self.row_id == other.row_id
    }
}

impl PartialOrd for LocatedDocInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LocatedDocInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.row_id.cmp(&other.row_id)
    }
}

#[derive(Debug, Clone, Default, DeepSizeOf, Copy)]
pub struct RawDocInfo {
    pub doc_id: u32,
    pub frequency: u32,
}

impl RawDocInfo {
    pub fn new(doc_id: u32, frequency: u32) -> Self {
        Self { doc_id, frequency }
    }
}

impl Eq for RawDocInfo {}

impl PartialEq for RawDocInfo {
    fn eq(&self, other: &Self) -> bool {
        self.doc_id == other.doc_id
    }
}

impl PartialOrd for RawDocInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RawDocInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.doc_id.cmp(&other.doc_id)
    }
}

/// Lucene SmallFloat-style document-length quantization for 256-document-block scoring and impact
/// norms: a 4-mantissa-bit float-like byte code. Values 0-7 are exact; larger
/// values keep their top four significand bits (relative error <= 6.25%) and
/// decode to their bucket floor. The floor only ever shortens a doc, so impact
/// bounds remain conservative for exact scoring as well as quantized scoring.
pub(in super::super) fn quantize_doc_length(value: u32) -> u8 {
    let num_bits = 32 - value.leading_zeros();
    if num_bits < 4 {
        value as u8
    } else {
        let shift = num_bits - 4;
        (((value >> shift) as u8) & 0x07) | (((shift + 1) as u8) << 3)
    }
}

#[inline]
pub(in super::super) fn dequantize_doc_length(code: u8) -> u32 {
    DEQUANTIZED_DOC_LENGTHS[code as usize]
}

pub(in super::super) static DEQUANTIZED_DOC_LENGTHS: [u32; 256] = build_dequantized_doc_lengths();

pub(super) const fn build_dequantized_doc_lengths() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut code = 0usize;
    while code < 256 {
        let bits = (code & 0x07) as u64;
        let shift = (code >> 3) as i64 - 1;
        let decoded = if shift < 0 {
            bits
        } else {
            (bits | 0x08) << shift
        };
        // Codes past the largest u32 encoding are never produced; saturate so
        // the table stays total.
        table[code] = if decoded > u32::MAX as u64 {
            u32::MAX
        } else {
            decoded as u32
        };
        code += 1;
    }
    table
}

#[derive(Debug, Clone)]
pub(super) enum NumTokens {
    Owned(Vec<u32>),
    Shared(ScalarBuffer<u32>),
}

impl Default for NumTokens {
    fn default() -> Self {
        Self::Owned(Vec::new())
    }
}

impl std::ops::Deref for NumTokens {
    type Target = [u32];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(values) => values,
            Self::Shared(values) => values,
        }
    }
}

impl DeepSizeOf for NumTokens {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        match self {
            Self::Owned(values) => values.deep_size_of_children(context),
            Self::Shared(values) => values.deep_size_of_children(context),
        }
    }
}

impl NumTokens {
    fn with_capacity(capacity: usize) -> Self {
        Self::Owned(Vec::with_capacity(capacity))
    }

    fn into_owned(self) -> Vec<u32> {
        match self {
            Self::Owned(values) => values,
            Self::Shared(values) => values.to_vec(),
        }
    }

    fn push(&mut self, value: u32) {
        match self {
            Self::Owned(values) => values.push(value),
            Self::Shared(values) => {
                let mut owned = values.to_vec();
                owned.push(value);
                *self = Self::Owned(owned);
            }
        }
    }

    fn memory_size(&self) -> usize {
        match self {
            Self::Owned(values) => values.capacity() * std::mem::size_of::<u32>(),
            Self::Shared(values) => values.inner().capacity(),
        }
    }
}

// DocSet is a mapping from row ids to the number of tokens in the document
// It's used to sort the documents by the bm25 score
#[derive(Debug, Clone, Default)]
pub struct DocSet {
    pub(super) row_ids: Vec<u64>,
    pub(super) num_tokens: NumTokens,
    // One flat u32 column per list boundary. This avoids a Vec allocation per
    // document while preserving the full logical document coordinate.
    pub(super) doc_indices: Vec<Vec<u32>>,
    // (row_id, doc_id) pairs sorted by row_id
    pub(super) inv: Vec<(u64, u32)>,

    pub(super) total_tokens: u64,

    // 256-document-block partitions score with quantized document lengths: the
    // flag is set at partition load and the byte-norm slab bakes lazily on
    // first scoring use (shared by clones of the loaded set). 128-block
    // partitions never set the flag and keep exact scoring.
    pub(super) scoring_quantized: bool,
    pub(super) norms: Arc<std::sync::OnceLock<Box<[u8]>>>,
}

impl DeepSizeOf for DocSet {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.row_ids.deep_size_of_children(context)
            + self.num_tokens.deep_size_of_children(context)
            + self.doc_indices.deep_size_of_children(context)
            + self.inv.deep_size_of_children(context)
            + self
                .norms
                .get()
                .map(|slab| std::mem::size_of_val(slab.as_ref()))
                .unwrap_or(0)
    }
}

impl DocSet {
    pub(crate) fn with_coordinate_rank(coordinate_rank: usize) -> Self {
        Self {
            doc_indices: (0..coordinate_rank).map(|_| Vec::new()).collect(),
            ..Default::default()
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        // Use num_tokens instead of row_ids so the deferred-row_ids
        // scoring path (which constructs a DocSet via
        // [`Self::from_num_tokens_only`]) still reports the right doc
        // count.
        self.num_tokens.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// True iff the per-doc `row_id` array is populated. The
    /// deferred-row_id scoring path constructs DocSets with the array
    /// left empty so wand can skip the load; callers that need to do
    /// row_id lookups in the inner loop must check this and fall back
    /// to async resolution otherwise.
    #[inline]
    pub fn has_row_ids(&self) -> bool {
        !self.row_ids.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&u64, &u32)> {
        self.row_ids.iter().zip(self.num_tokens.iter())
    }

    pub fn row_id(&self, doc_id: u32) -> u64 {
        self.row_ids[doc_id as usize]
    }

    pub fn doc_index(&self, doc_id: u32) -> Vec<u32> {
        self.doc_indices
            .iter()
            .map(|coordinates| coordinates[doc_id as usize])
            .collect()
    }

    pub fn coordinate(&self, doc_id: u32, rank: usize) -> u32 {
        self.doc_indices[rank][doc_id as usize]
    }

    pub fn coordinate_rank(&self) -> usize {
        self.doc_indices.len()
    }

    /// Resolve a `row_id` to every `doc_id` it owns.
    ///
    /// Row-document indexes map each row to a single document. Element-document
    /// indexes (and older list indexes) can map one row to several documents,
    /// so a single `row_id` may own multiple `doc_id`s sharing that key in `inv`.
    /// The prefilter path (`flat_search`) walks an allow-list of row_ids and
    /// must evaluate all legacy documents for that row.
    pub fn doc_ids(&self, row_id: u64) -> impl Iterator<Item = u64> + '_ {
        if self.inv.is_empty() {
            // in legacy format, the row id is doc id (one document per row)
            let found = self.row_ids.binary_search(&row_id).is_ok();
            Either::Left(found.then_some(row_id).into_iter())
        } else {
            // `inv` is sorted by row_id, so the entries sharing this key form a
            // contiguous run; yield the doc_id of each.
            let lo = self.inv.partition_point(|entry| entry.0 < row_id);
            let hi = self.inv.partition_point(|entry| entry.0 <= row_id);
            Either::Right(self.inv[lo..hi].iter().map(|entry| entry.1 as u64))
        }
    }
    pub fn total_tokens_num(&self) -> u64 {
        self.total_tokens
    }

    #[inline]
    pub fn average_length(&self) -> f32 {
        self.total_tokens as f32 / self.len() as f32
    }

    pub fn calculate_block_max_scores<'a>(
        &self,
        doc_ids: impl Iterator<Item = &'a u32>,
        freqs: impl Iterator<Item = &'a u32>,
    ) -> Vec<f32> {
        self.calculate_block_max_scores_with_block_size(doc_ids, freqs, LEGACY_BLOCK_SIZE)
    }

    pub fn calculate_block_max_scores_with_block_size<'a>(
        &self,
        doc_ids: impl Iterator<Item = &'a u32>,
        freqs: impl Iterator<Item = &'a u32>,
        block_size: usize,
    ) -> Vec<f32> {
        validate_block_size(block_size).expect("invalid posting list block size");
        let avgdl = self.average_length();
        let length = doc_ids.size_hint().0;
        let num_blocks = length.div_ceil(block_size);
        let mut block_max_scores = Vec::with_capacity(num_blocks);
        let idf_scale = idf(length, self.len()) * (K1 + 1.0);
        let mut max_score = f32::MIN;
        for (i, (doc_id, freq)) in doc_ids.zip(freqs).enumerate() {
            let doc_norm = K1 * (1.0 - B + B * self.num_tokens(*doc_id) as f32 / avgdl);
            let freq = *freq as f32;
            let score = freq / (freq + doc_norm);
            if score > max_score {
                max_score = score;
            }
            if (i + 1) % block_size == 0 {
                max_score *= idf_scale;
                block_max_scores.push(max_score);
                max_score = f32::MIN;
            }
        }
        if !length.is_multiple_of(block_size) {
            max_score *= idf_scale;
            block_max_scores.push(max_score);
        }
        block_max_scores
    }

    pub fn to_batch(&self) -> Result<RecordBatch> {
        let row_id_col = UInt64Array::from_iter_values(self.row_ids.iter().cloned());
        let num_tokens_col = UInt32Array::from_iter_values(self.num_tokens.iter().cloned());

        let mut fields = vec![
            arrow_schema::Field::new(ROW_ID, DataType::UInt64, false),
            arrow_schema::Field::new(NUM_TOKEN_COL, DataType::UInt32, false),
        ];
        let mut columns = vec![
            Arc::new(row_id_col) as ArrayRef,
            Arc::new(num_tokens_col) as ArrayRef,
        ];
        for (rank, coordinates) in self.doc_indices.iter().enumerate() {
            fields.push(arrow_schema::Field::new(
                doc_index_storage_column(rank),
                DataType::UInt32,
                false,
            ));
            columns.push(
                Arc::new(UInt32Array::from_iter_values(coordinates.iter().copied())) as ArrayRef,
            );
        }
        let schema = arrow_schema::Schema::new(fields);

        let batch = RecordBatch::try_new(Arc::new(schema), columns)?;
        Ok(batch)
    }

    pub async fn load(
        reader: Arc<dyn IndexReader>,
        is_legacy: bool,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
    ) -> Result<Self> {
        let batch = reader.read_range(0..reader.num_rows(), None).await?;
        let row_id_col = batch[ROW_ID].as_primitive::<datatypes::UInt64Type>();
        let num_tokens_col = batch[NUM_TOKEN_COL].as_primitive::<datatypes::UInt32Type>();
        let mut doc_indices = Vec::new();
        for rank in 0.. {
            let column_name = doc_index_storage_column(rank);
            let Some(column) = batch.column_by_name(&column_name) else {
                break;
            };
            doc_indices.push(column.as_primitive::<datatypes::UInt32Type>());
        }
        Self::from_columns_with_doc_indices(
            row_id_col,
            num_tokens_col,
            &doc_indices,
            is_legacy,
            frag_reuse_index,
        )
    }

    /// Build a `DocSet` carrying only the per-doc `num_tokens` array;
    /// `row_ids` and `inv` are left empty. Used by the deferred-row_id
    /// scoring path: wand checks `has_row_ids()` to skip `row_id` /
    /// `num_tokens_by_row_id` calls, and the per-partition caller
    /// resolves doc_id → row_id for the surviving top-K post-wand.
    pub fn from_num_tokens_only(num_tokens_col: &arrow_array::UInt32Array) -> Self {
        let total_tokens = num_tokens_col.values().iter().map(|&n| n as u64).sum();
        Self::from_cached_num_tokens(num_tokens_col, total_tokens)
    }

    /// Build a zero-copy num-tokens-only view from an Arrow column and its
    /// already-computed total. The caller must guarantee that `total_tokens`
    /// is the sum of `num_tokens_col`.
    pub(crate) fn from_cached_num_tokens(
        num_tokens_col: &arrow_array::UInt32Array,
        total_tokens: u64,
    ) -> Self {
        Self {
            row_ids: Vec::new(),
            num_tokens: NumTokens::Shared(num_tokens_col.values().clone()),
            doc_indices: Vec::new(),
            inv: Vec::new(),
            total_tokens,
            scoring_quantized: false,
            norms: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Build a `DocSet` from already-loaded `row_id` and `num_tokens`
    /// Arrow columns without re-reading either column.
    pub fn from_columns(
        row_id_col: &UInt64Array,
        num_tokens_col: &arrow_array::UInt32Array,
        is_legacy: bool,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
    ) -> Result<Self> {
        Self::from_columns_with_doc_indices(
            row_id_col,
            num_tokens_col,
            &[],
            is_legacy,
            frag_reuse_index,
        )
    }

    pub fn from_columns_with_doc_indices(
        row_id_col: &UInt64Array,
        num_tokens_col: &arrow_array::UInt32Array,
        doc_index_cols: &[&arrow_array::UInt32Array],
        is_legacy: bool,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
    ) -> Result<Self> {
        if doc_index_cols
            .iter()
            .any(|column| column.len() != row_id_col.len())
        {
            return Err(Error::index(
                "FTS document coordinate columns must have the same length as row ids".to_string(),
            ));
        }
        let doc_indices = doc_index_cols
            .iter()
            .map(|column| column.values().to_vec())
            .collect::<Vec<_>>();
        // for legacy format, the row id is doc id; sorting keeps binary search viable
        if is_legacy {
            let (row_ids, num_tokens): (Vec<_>, Vec<_>) = row_id_col
                .values()
                .iter()
                .filter_map(|id| {
                    if let Some(frag_reuse_index_ref) = frag_reuse_index.as_ref() {
                        frag_reuse_index_ref.remap_row_id(*id)
                    } else {
                        Some(*id)
                    }
                })
                .zip(num_tokens_col.values().iter())
                .sorted_unstable_by_key(|x| x.0)
                .unzip();

            let total_tokens = num_tokens.iter().map(|&x| x as u64).sum();
            return Ok(Self {
                row_ids,
                num_tokens: NumTokens::Owned(num_tokens),
                doc_indices,
                inv: Vec::new(),
                total_tokens,
                scoring_quantized: false,
                norms: Arc::new(std::sync::OnceLock::new()),
            });
        }

        // If frag reuse happened, remap the row_ids through it. Crucially we
        // must NOT drop the rows the reuse index deleted, because the posting
        // lists reference doc_ids *positionally* (a doc_id is an index into
        // these arrays, fixed at build time). Dropping deleted rows would
        // renumber every later doc_id and desync the posting lists, so wand
        // would index `num_tokens`/`row_ids` out of bounds or score the wrong
        // doc. Instead we tombstone deleted rows in place: their slot survives
        // (so doc_ids stay aligned with the posting lists) carrying
        // `RowAddress::TOMBSTONE_ROW`, which wand skips, and they are left out
        // of `inv` so a row_id lookup never resolves to a deleted doc. The
        // heavyweight physical remap (`DocSet::remap`) is what actually
        // renumbers and compacts; this load-time path only has to stay
        // consistent until then.
        if let Some(frag_reuse_index_ref) = frag_reuse_index.as_ref() {
            let mut row_ids = Vec::with_capacity(row_id_col.len());
            let num_tokens = num_tokens_col.values().to_vec();
            let mut inv = Vec::with_capacity(row_id_col.len());
            for (doc_id, row_id) in row_id_col.values().iter().enumerate() {
                match frag_reuse_index_ref.remap_row_id(*row_id) {
                    Some(new_row_id) => {
                        row_ids.push(new_row_id);
                        inv.push((new_row_id, doc_id as u32));
                    }
                    None => {
                        // Deleted: keep the slot (doc_ids must not shift) but
                        // tombstone it and leave it out of `inv`.
                        row_ids.push(RowAddress::TOMBSTONE_ROW);
                    }
                }
            }
            inv.sort_unstable_by_key(|entry| entry.0);

            let total_tokens = num_tokens.iter().map(|&x| x as u64).sum();
            return Ok(Self {
                row_ids,
                num_tokens: NumTokens::Owned(num_tokens),
                doc_indices,
                inv,
                total_tokens,
                scoring_quantized: false,
                norms: Arc::new(std::sync::OnceLock::new()),
            });
        }

        let row_ids = row_id_col.values().to_vec();
        let num_tokens = num_tokens_col.values().to_vec();
        let mut inv: Vec<(u64, u32)> = row_ids
            .iter()
            .enumerate()
            .map(|(doc_id, row_id)| (*row_id, doc_id as u32))
            .collect();
        if !row_ids.is_sorted() {
            inv.sort_unstable_by_key(|entry| entry.0);
        }
        let total_tokens = num_tokens.iter().map(|&x| x as u64).sum();
        Ok(Self {
            row_ids,
            num_tokens: NumTokens::Owned(num_tokens),
            doc_indices,
            inv,
            total_tokens,
            scoring_quantized: false,
            norms: Arc::new(std::sync::OnceLock::new()),
        })
    }

    // remap the row ids to the new row ids
    // returns the removed doc ids
    pub fn remap(&mut self, mapping: &RowAddrRemap) -> Vec<u32> {
        let mut removed = Vec::new();
        let len = self.len();
        let row_ids = std::mem::replace(&mut self.row_ids, Vec::with_capacity(len));
        let num_tokens =
            std::mem::replace(&mut self.num_tokens, NumTokens::with_capacity(len)).into_owned();
        let doc_indices = std::mem::take(&mut self.doc_indices);
        self.doc_indices = doc_indices
            .iter()
            .map(|_| Vec::with_capacity(len))
            .collect();
        self.invalidate_norms();
        self.total_tokens = 0;
        for (doc_id, (row_id, num_token)) in std::iter::zip(row_ids, num_tokens).enumerate() {
            match mapping.get(row_id) {
                Some(Some(new_row_id)) => {
                    self.row_ids.push(new_row_id);
                    self.num_tokens.push(num_token);
                    for (new_coordinates, old_coordinates) in
                        self.doc_indices.iter_mut().zip(&doc_indices)
                    {
                        new_coordinates.push(old_coordinates[doc_id]);
                    }
                    self.total_tokens += num_token as u64;
                }
                Some(None) => {
                    removed.push(doc_id as u32);
                }
                None => {
                    self.row_ids.push(row_id);
                    self.num_tokens.push(num_token);
                    for (new_coordinates, old_coordinates) in
                        self.doc_indices.iter_mut().zip(&doc_indices)
                    {
                        new_coordinates.push(old_coordinates[doc_id]);
                    }
                    self.total_tokens += num_token as u64;
                }
            }
        }
        removed
    }

    #[inline]
    pub fn num_tokens(&self, doc_id: u32) -> u32 {
        self.num_tokens[doc_id as usize]
    }

    /// Enable quantized document-length scoring for 256-document-block partitions.
    pub fn set_quantized_scoring(&mut self, quantized: bool) {
        self.scoring_quantized = quantized;
    }

    /// The quantized document-length slab when this set scores quantized,
    /// baked on first use; `None` for exact-scoring sets.
    pub fn scoring_norms(&self) -> Option<&[u8]> {
        if !self.scoring_quantized {
            return None;
        }
        Some(
            self.norms
                .get_or_init(|| {
                    self.num_tokens
                        .iter()
                        .map(|&n| quantize_doc_length(n))
                        .collect()
                })
                .as_ref(),
        )
    }

    /// Document length as scoring sees it: the quantized bucket floor for
    /// 256-document-block partitions, the exact value otherwise.
    #[inline]
    pub fn scoring_num_tokens(&self, doc_id: u32) -> u32 {
        match self.scoring_norms() {
            Some(norms) => dequantize_doc_length(norms[doc_id as usize]),
            None => self.num_tokens[doc_id as usize],
        }
    }

    // this can be used only if it's a legacy format,
    // which store the sorted row ids so that we can use binary search
    #[inline]
    pub fn num_tokens_by_row_id(&self, row_id: u64) -> u32 {
        self.row_ids
            .binary_search(&row_id)
            .map(|idx| self.num_tokens[idx])
            .unwrap_or(0)
    }

    // append a document to the doc set
    // returns the doc_id (the number of documents before appending)
    pub fn append(&mut self, row_id: u64, num_tokens: u32) -> u32 {
        self.row_ids.push(row_id);
        self.num_tokens.push(num_tokens);
        self.total_tokens += num_tokens as u64;
        self.invalidate_norms();
        self.row_ids.len() as u32 - 1
    }

    pub fn append_with_doc_index(
        &mut self,
        row_id: u64,
        num_tokens: u32,
        doc_index: &[u32],
    ) -> Result<u32> {
        if self.row_ids.is_empty() && self.doc_indices.is_empty() {
            self.doc_indices = (0..doc_index.len()).map(|_| Vec::new()).collect();
        }
        if self.doc_indices.len() != doc_index.len() {
            return Err(Error::index(format!(
                "all documents in an FTS partition must have the same coordinate rank: expected {}, got {}",
                self.doc_indices.len(),
                doc_index.len()
            )));
        }
        self.row_ids.push(row_id);
        self.num_tokens.push(num_tokens);
        for (coordinates, value) in self.doc_indices.iter_mut().zip(doc_index) {
            coordinates.push(*value);
        }
        self.total_tokens += num_tokens as u64;
        self.invalidate_norms();
        Ok(self.row_ids.len() as u32 - 1)
    }

    // Drop the baked norm slab after a mutation; it re-bakes on the next
    // scoring use.
    fn invalidate_norms(&mut self) {
        if self.norms.get().is_some() {
            self.norms = Arc::new(std::sync::OnceLock::new());
        }
    }

    pub(crate) fn memory_size(&self) -> usize {
        self.row_ids.capacity() * std::mem::size_of::<u64>()
            + self.num_tokens.memory_size()
            + self
                .doc_indices
                .iter()
                .map(|coordinates| coordinates.capacity() * std::mem::size_of::<u32>())
                .sum::<usize>()
            + self.inv.capacity() * std::mem::size_of::<(u64, u32)>()
    }
}
