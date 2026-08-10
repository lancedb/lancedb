// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

#[derive(Debug, Clone, DeepSizeOf)]
#[allow(clippy::large_enum_variant)]
pub enum PostingList {
    Plain(PlainPostingList),
    Compressed(CompressedPostingList),
}

impl PostingList {
    pub fn from_batch(
        batch: &RecordBatch,
        max_score: Option<f32>,
        length: Option<u32>,
    ) -> Result<Self> {
        let posting_tail_codec = parse_posting_tail_codec(batch.schema_ref().metadata())?;
        let block_size = parse_posting_block_size(batch.schema_ref().metadata())?;
        Self::from_batch_with_tail_codec(batch, max_score, length, posting_tail_codec, block_size)
    }

    pub fn from_batch_with_tail_codec(
        batch: &RecordBatch,
        max_score: Option<f32>,
        length: Option<u32>,
        posting_tail_codec: PostingTailCodec,
        block_size: usize,
    ) -> Result<Self> {
        let positions_layout = if batch.column_by_name(COMPRESSED_POSITION_COL).is_some() {
            PositionsLayout::SharedStream(parse_shared_position_codec(
                batch.schema_ref().metadata(),
            )?)
        } else if batch.column_by_name(POSITION_COL).is_some() {
            PositionsLayout::LegacyPerDoc
        } else {
            PositionsLayout::None
        };
        Self::from_batch_with_tail_codec_and_positions_layout(
            batch,
            max_score,
            length,
            posting_tail_codec,
            block_size,
            positions_layout,
        )
    }

    pub(super) fn from_batch_with_tail_codec_and_positions_layout(
        batch: &RecordBatch,
        max_score: Option<f32>,
        length: Option<u32>,
        posting_tail_codec: PostingTailCodec,
        block_size: usize,
        positions_layout: PositionsLayout,
    ) -> Result<Self> {
        match batch.column_by_name(POSTING_COL) {
            Some(_) => {
                debug_assert!(max_score.is_some() && length.is_some());
                let shared_position_codec = match positions_layout {
                    PositionsLayout::SharedStream(codec) => Some(codec),
                    _ => None,
                };
                let posting = CompressedPostingList::from_batch(
                    batch,
                    max_score.unwrap(),
                    length.unwrap(),
                    posting_tail_codec,
                    block_size,
                    shared_position_codec,
                )?;
                Ok(Self::Compressed(posting))
            }
            None => {
                let posting = PlainPostingList::from_batch(batch, max_score);
                Ok(Self::Plain(posting))
            }
        }
    }

    pub fn iter(&self) -> PostingListIterator<'_> {
        PostingListIterator::new(self)
    }

    pub fn has_position(&self) -> bool {
        match self {
            Self::Plain(posting) => posting.positions.is_some(),
            Self::Compressed(posting) => posting.positions.is_some(),
        }
    }

    pub fn has_impacts(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            Self::Compressed(posting) => posting.impacts.is_some(),
        }
    }

    pub fn set_positions(&mut self, positions: CompressedPositionStorage) {
        match self {
            Self::Plain(posting) => match positions {
                CompressedPositionStorage::LegacyPerDoc(positions) => {
                    posting.positions = Some(positions)
                }
                CompressedPositionStorage::SharedStream(_) => {
                    unreachable!("shared position stream is not supported for plain postings")
                }
            },
            Self::Compressed(posting) => {
                posting.positions = Some(positions);
            }
        }
    }

    pub fn take_positions(&mut self) -> Option<CompressedPositionStorage> {
        match self {
            Self::Plain(posting) => posting
                .positions
                .take()
                .map(CompressedPositionStorage::LegacyPerDoc),
            Self::Compressed(posting) => posting.positions.take(),
        }
    }

    pub fn max_score(&self) -> Option<f32> {
        match self {
            Self::Plain(posting) => posting.max_score,
            Self::Compressed(posting) => Some(posting.max_score),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Plain(posting) => posting.len(),
            Self::Compressed(posting) => posting.length as usize,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_builder(self, docs: &DocSet) -> PostingListBuilder {
        let posting_tail_codec = match &self {
            Self::Plain(_) => PostingTailCodec::Fixed32,
            Self::Compressed(posting) => posting.posting_tail_codec,
        };
        let block_size = match &self {
            Self::Plain(_) => LEGACY_BLOCK_SIZE,
            Self::Compressed(posting) => posting.block_size,
        };
        let mut builder = PostingListBuilder::new_with_posting_tail_codec_and_block_size(
            self.has_position(),
            posting_tail_codec,
            block_size,
        );
        match self {
            // legacy format
            Self::Plain(posting) => {
                // convert the posting list to the new format:
                // 1. map row ids to doc ids
                // 2. sort the posting list by doc ids
                struct Item {
                    doc_id: u32,
                    positions: PositionRecorder,
                }
                let doc_ids = docs
                    .row_ids
                    .iter()
                    .enumerate()
                    .map(|(doc_id, row_id)| (*row_id, doc_id as u32))
                    .collect::<HashMap<_, _>>();
                let mut items = Vec::with_capacity(posting.len());
                for (row_id, freq, positions) in posting.iter() {
                    let freq = freq as u32;
                    let positions = match positions {
                        Some(positions) => {
                            PositionRecorder::Position(positions.collect::<Vec<_>>().into())
                        }
                        None => PositionRecorder::Count(freq),
                    };
                    items.push(Item {
                        doc_id: doc_ids[&row_id],
                        positions,
                    });
                }
                items.sort_unstable_by_key(|item| item.doc_id);
                for item in items {
                    builder.add(item.doc_id, item.positions);
                }
            }
            Self::Compressed(posting) => {
                posting.iter().for_each(|(doc_id, freq, positions)| {
                    let positions = match positions {
                        Some(positions) => {
                            PositionRecorder::Position(positions.collect::<Vec<_>>().into())
                        }
                        None => PositionRecorder::Count(freq),
                    };
                    builder.add(doc_id, positions);
                });
            }
        }
        builder
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct PlainPostingList {
    pub row_ids: ScalarBuffer<u64>,
    pub frequencies: ScalarBuffer<f32>,
    pub max_score: Option<f32>,
    pub positions: Option<ListArray>, // List of Int32
}

impl DeepSizeOf for PlainPostingList {
    fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
        self.row_ids.len() * std::mem::size_of::<u64>()
            + self.frequencies.len() * std::mem::size_of::<f32>()
            + self
                .positions
                .as_ref()
                .map(|positions| sliced_cache_bytes(positions))
                .unwrap_or(0)
    }
}

impl PlainPostingList {
    pub fn new(
        row_ids: ScalarBuffer<u64>,
        frequencies: ScalarBuffer<f32>,
        max_score: Option<f32>,
        positions: Option<ListArray>,
    ) -> Self {
        Self {
            row_ids,
            frequencies,
            max_score,
            positions,
        }
    }

    pub fn from_batch(batch: &RecordBatch, max_score: Option<f32>) -> Self {
        let row_ids = batch[ROW_ID].as_primitive::<UInt64Type>().values().clone();
        let frequencies = batch[FREQUENCY_COL]
            .as_primitive::<Float32Type>()
            .values()
            .clone();
        let positions = batch
            .column_by_name(POSITION_COL)
            .map(|col| col.as_list::<i32>().clone());

        Self::new(row_ids, frequencies, max_score, positions)
    }

    pub fn len(&self) -> usize {
        self.row_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> PlainPostingListIterator<'_> {
        Box::new(
            self.row_ids
                .iter()
                .zip(self.frequencies.iter())
                .enumerate()
                .map(|(idx, (doc_id, freq))| {
                    (
                        *doc_id,
                        *freq,
                        self.positions.as_ref().map(|p| {
                            let start = p.value_offsets()[idx] as usize;
                            let end = p.value_offsets()[idx + 1] as usize;
                            Box::new(
                                p.values().as_primitive::<Int32Type>().values()[start..end]
                                    .iter()
                                    .map(|pos| *pos as u32),
                            ) as _
                        }),
                    )
                }),
        )
    }

    #[inline]
    pub fn doc(&self, i: usize) -> LocatedDocInfo {
        LocatedDocInfo::new(self.row_ids[i], self.frequencies[i])
    }

    pub fn positions(&self, index: usize) -> Option<Arc<dyn Array>> {
        self.positions
            .as_ref()
            .map(|positions| positions.value(index))
    }

    pub fn max_score(&self) -> Option<f32> {
        self.max_score
    }

    pub fn row_id(&self, i: usize) -> u64 {
        self.row_ids[i]
    }
}

#[derive(Debug, Clone)]
pub(super) enum FirstDocsState {
    Standalone(Arc<OnceLock<Box<[u32]>>>),
    Packed {
        states: Arc<[OnceLock<Box<[u32]>>]>,
        slot: usize,
    },
}

impl FirstDocsState {
    fn standalone() -> Self {
        Self::Standalone(Arc::new(OnceLock::new()))
    }

    fn state(&self) -> &OnceLock<Box<[u32]>> {
        match self {
            Self::Standalone(state) => state,
            Self::Packed { states, slot } => &states[*slot],
        }
    }

    fn get_or_init(&self, initialize: impl FnOnce() -> Box<[u32]>) -> &[u32] {
        self.state().get_or_init(initialize)
    }

    fn capacity_bytes(
        &self,
        block_count: usize,
        context: &mut lance_core::deepsize::Context,
    ) -> usize {
        if context.mark_seen(self.state() as *const _ as usize) {
            std::mem::size_of::<OnceLock<Box<[u32]>>>()
                .saturating_add(block_count.saturating_mul(std::mem::size_of::<u32>()))
        } else {
            0
        }
    }

    #[cfg(test)]
    fn shares_state_with(&self, other: &Self) -> bool {
        std::ptr::eq(self.state(), other.state())
    }
}

#[derive(Debug, Clone)]
pub struct CompressedPostingList {
    pub max_score: f32,
    pub length: u32,
    // each binary is a block of compressed data
    // that contains `block_size` doc ids and then `block_size` frequencies,
    // packed by the physical bitpacker matching that block size.
    pub blocks: LargeBinaryArray,
    pub posting_tail_codec: PostingTailCodec,
    pub block_size: usize,
    pub positions: Option<CompressedPositionStorage>,
    pub(crate) impacts: Option<ImpactSkipData>,
    // First doc id per block, baked lazily and shared across per-query clones
    // of the cached list. See `block_first_docs`.
    first_docs: FirstDocsState,
}

impl PartialEq for CompressedPostingList {
    fn eq(&self, other: &Self) -> bool {
        self.max_score == other.max_score
            && self.length == other.length
            && self.blocks == other.blocks
            && self.posting_tail_codec == other.posting_tail_codec
            && self.block_size == other.block_size
            && self.positions == other.positions
            && self.impacts == other.impacts
    }
}

impl DeepSizeOf for CompressedPostingList {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        sliced_cache_bytes(&self.blocks)
            + self
                .positions
                .as_ref()
                .map(|positions| positions.deep_size_of_children(context))
                .unwrap_or(0)
            + self
                .impacts
                .as_ref()
                .map(|impacts| {
                    sliced_cache_bytes(impacts.entries())
                        .saturating_add(impacts.derived_cache_bytes())
                })
                .unwrap_or(0)
            + self.first_docs.capacity_bytes(self.blocks.len(), context)
    }
}

impl CompressedPostingList {
    pub(crate) fn new(
        blocks: LargeBinaryArray,
        max_score: f32,
        length: u32,
        posting_tail_codec: PostingTailCodec,
        block_size: usize,
        positions: Option<CompressedPositionStorage>,
        impacts: Option<ImpactSkipData>,
    ) -> Self {
        debug_assert!(block_size.is_power_of_two());
        Self {
            max_score,
            length,
            blocks,
            posting_tail_codec,
            block_size,
            positions,
            impacts,
            first_docs: FirstDocsState::standalone(),
        }
    }

    pub(super) fn with_packed_first_docs(
        mut self,
        states: Arc<[OnceLock<Box<[u32]>>]>,
        slot: usize,
    ) -> Self {
        debug_assert!(slot < states.len());
        self.first_docs = FirstDocsState::Packed { states, slot };
        self
    }

    /// Block sizes are validated powers of two, so per-doc hot loops derive
    /// block indices with shift/mask instead of runtime division, which is
    /// measurably slower in the iterator advance path.
    #[inline]
    pub(crate) fn block_shift(&self) -> u32 {
        self.block_size.trailing_zeros()
    }

    #[inline]
    pub(crate) fn block_mask(&self) -> usize {
        self.block_size - 1
    }

    pub fn from_batch(
        batch: &RecordBatch,
        max_score: f32,
        length: u32,
        posting_tail_codec: PostingTailCodec,
        block_size: usize,
        shared_position_codec: Option<PositionStreamCodec>,
    ) -> Result<Self> {
        debug_assert_eq!(batch.num_rows(), 1);
        let blocks = batch[POSTING_COL]
            .as_list::<i32>()
            .value(0)
            .as_binary::<i64>()
            .clone();
        let positions = if let Some(col) = batch.column_by_name(COMPRESSED_POSITION_COL) {
            let bytes = bytes::Bytes::from(col.as_binary::<i64>().value(0).to_vec());
            let block_offsets = batch[POSITION_BLOCK_OFFSET_COL]
                .as_list::<i32>()
                .value(0)
                .as_primitive::<UInt32Type>()
                .values()
                .to_vec();
            let codec = shared_position_codec.unwrap_or_else(|| {
                parse_shared_position_codec(batch.schema_ref().metadata())
                    .expect("shared position stream codec metadata should be valid")
            });
            Some(CompressedPositionStorage::SharedStream(
                SharedPositionStream::new(codec, block_offsets, bytes),
            ))
        } else {
            batch.column_by_name(POSITION_COL).map(|col| {
                CompressedPositionStorage::LegacyPerDoc(
                    col.as_list::<i32>().value(0).as_list::<i32>().clone(),
                )
            })
        };
        let impacts = batch
            .column_by_name(IMPACT_COL)
            .map(|col| {
                let entries = col.as_list::<i32>().value(0).as_binary::<i64>().clone();
                ImpactSkipData::new(entries, blocks.len())
            })
            .transpose()?;

        Ok(Self {
            max_score,
            length,
            blocks,
            posting_tail_codec,
            block_size,
            positions,
            impacts,
            first_docs: FirstDocsState::standalone(),
        })
    }

    pub fn iter(&self) -> CompressedPostingListIterator {
        CompressedPostingListIterator::new(
            self.length as usize,
            self.blocks.clone(),
            self.posting_tail_codec,
            self.positions.clone(),
            self.block_size,
        )
    }

    pub fn block_max_score(&self, block_idx: usize) -> f32 {
        // 256-document blocks store no per-block max score: their impact
        // skip data supplies the tight per-block bound, so callers on that
        // path never reach here. Fall back to the list-level max, which is
        // still a valid (looser) bound for any block.
        if super::super::encoding::posting_block_score_prefix_len(self.block_size) == 0 {
            return self.max_score;
        }
        let block = self.blocks.value(block_idx);
        block[0..4].try_into().map(f32::from_le_bytes).unwrap()
    }

    #[inline]
    pub fn block_least_doc_id(&self, block_idx: usize) -> u32 {
        self.block_first_docs()[block_idx]
    }

    /// First doc id of every block, decoded once per cached list and shared by
    /// the per-query clones. Block boundary lookups (window bounds, block
    /// binary searches) are hot enough that re-reading the block headers —
    /// and re-decoding the tail block — shows up in profiles.
    pub(crate) fn block_first_docs(&self) -> &[u32] {
        self.first_docs.get_or_init(|| {
            (0..self.blocks.len())
                .map(|block_idx| {
                    let block = self.blocks.value(block_idx);
                    let remainder = self.length as usize % self.block_size;
                    if block_idx + 1 == self.blocks.len() && remainder > 0 {
                        return super::super::encoding::read_posting_tail_first_doc(
                            block,
                            self.posting_tail_codec,
                            self.block_size,
                        );
                    }
                    let prefix =
                        super::super::encoding::posting_block_score_prefix_len(self.block_size);
                    block[prefix..prefix + 4]
                        .try_into()
                        .map(u32::from_le_bytes)
                        .unwrap()
                })
                .collect::<Vec<_>>()
                .into_boxed_slice()
        })
    }

    #[cfg(test)]
    pub(super) fn shares_first_docs_with(&self, other: &Self) -> bool {
        self.first_docs.shares_state_with(&other.first_docs)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct EncodedBlocks {
    offsets: Vec<u32>,
    bytes: Vec<u8>,
}

impl EncodedBlocks {
    pub(super) fn len(&self) -> usize {
        self.offsets.len()
    }

    pub(super) fn size(&self) -> usize {
        self.offsets.capacity() * std::mem::size_of::<u32>() + self.bytes.capacity()
    }

    pub(super) fn push_full_block(
        &mut self,
        doc_ids: &[u32],
        frequencies: &[u32],
    ) -> Result<usize> {
        let start = self.bytes.len();
        self.offsets.push(start as u32);
        super::super::encoding::encode_full_posting_block_into(
            doc_ids,
            frequencies,
            &mut self.bytes,
        )?;
        Ok(self.bytes.len() - start)
    }

    pub(super) fn block(&self, index: usize) -> &[u8] {
        let (start, end) = self.block_range(index);
        &self.bytes[start..end]
    }

    fn block_range(&self, index: usize) -> (usize, usize) {
        let start = self.offsets[index] as usize;
        let end = self
            .offsets
            .get(index + 1)
            .map(|offset| *offset as usize)
            .unwrap_or(self.bytes.len());
        (start, end)
    }

    pub(super) fn set_block_score(&mut self, index: usize, score: f32) {
        let (start, _) = self.block_range(index);
        self.bytes[start..start + 4].copy_from_slice(&score.to_le_bytes());
    }

    pub(super) fn append_remainder_block_with_codec(
        &mut self,
        doc_ids: &[u32],
        frequencies: &[u32],
        codec: PostingTailCodec,
        block_size: usize,
    ) -> Result<()> {
        self.offsets.push(self.bytes.len() as u32);
        super::super::encoding::encode_remainder_posting_block_into(
            doc_ids,
            frequencies,
            codec,
            block_size,
            &mut self.bytes,
        )
    }

    pub(super) fn into_array(mut self) -> LargeBinaryArray {
        let mut offsets = Vec::with_capacity(self.offsets.len() + 1);
        offsets.extend(self.offsets.into_iter().map(i64::from));
        offsets.push(self.bytes.len() as i64);
        LargeBinaryArray::new(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Buffer::from_vec(std::mem::take(&mut self.bytes)),
            None,
        )
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &[u8]> {
        (0..self.len()).map(|index| self.block(index))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct EncodedPositionBlocks {
    offsets: Vec<u32>,
    bytes: Vec<u8>,
}

impl EncodedPositionBlocks {
    pub(super) fn size(&self) -> usize {
        self.offsets.capacity() * std::mem::size_of::<u32>() + self.bytes.capacity()
    }

    pub(super) fn block(&self, index: usize) -> &[u8] {
        let start = self.offsets[index] as usize;
        let end = self
            .offsets
            .get(index + 1)
            .map(|offset| *offset as usize)
            .unwrap_or(self.bytes.len());
        &self.bytes[start..end]
    }

    pub(super) fn push_encoded_block(&mut self, block: &[u8]) -> usize {
        let start = self.bytes.len();
        self.offsets.push(start as u32);
        self.bytes.extend_from_slice(block);
        self.bytes.len() - start
    }

    pub(super) fn into_stream(self) -> SharedPositionStream {
        SharedPositionStream::new(
            PositionStreamCodec::PackedDelta,
            self.offsets,
            bytes::Bytes::from(self.bytes),
        )
    }
}
