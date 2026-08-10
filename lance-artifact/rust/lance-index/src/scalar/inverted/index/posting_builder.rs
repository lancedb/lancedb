// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

#[derive(Debug)]
pub struct PostingListBuilder {
    pub(super) with_positions: bool,
    pub(super) posting_tail_codec: PostingTailCodec,
    pub(super) encoded_blocks: Option<Box<EncodedBlocks>>,
    pub(super) encoded_position_blocks: Option<Box<EncodedPositionBlocks>>,
    pub(super) tail_entries: Vec<RawDocInfo>,
    pub(super) tail_positions: PositionBlockBuilder,
    pub(super) open_doc_id: Option<u32>,
    pub(super) open_doc_frequency: u32,
    pub(super) open_doc_last_position: Option<u32>,
    pub(super) block_size: usize,
    pub(super) memory_size_bytes: u32,
    pub(super) len: u32,
}

impl PostingListBuilder {
    pub fn size(&self) -> u64 {
        self.memory_size_bytes as u64
    }

    pub fn has_positions(&self) -> bool {
        self.with_positions
    }

    pub fn new(with_position: bool) -> Self {
        Self::new_with_posting_tail_codec_and_block_size(
            with_position,
            current_fts_format_version().posting_tail_codec(),
            LEGACY_BLOCK_SIZE,
        )
    }

    pub fn new_with_posting_tail_codec(
        with_position: bool,
        posting_tail_codec: PostingTailCodec,
    ) -> Self {
        Self::new_with_posting_tail_codec_and_block_size(
            with_position,
            posting_tail_codec,
            LEGACY_BLOCK_SIZE,
        )
    }

    pub fn new_with_block_size(with_position: bool, block_size: usize) -> Self {
        Self::new_with_posting_tail_codec_and_block_size(
            with_position,
            current_fts_format_version().posting_tail_codec(),
            block_size,
        )
    }

    pub fn new_with_posting_tail_codec_and_block_size(
        with_position: bool,
        posting_tail_codec: PostingTailCodec,
        block_size: usize,
    ) -> Self {
        validate_block_size(block_size).expect("invalid posting list block size");
        Self {
            with_positions: with_position,
            posting_tail_codec,
            encoded_blocks: None,
            encoded_position_blocks: None,
            tail_entries: Vec::new(),
            tail_positions: PositionBlockBuilder::default(),
            open_doc_id: None,
            open_doc_frequency: 0,
            open_doc_last_position: None,
            block_size,
            len: 0,
            memory_size_bytes: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> std::vec::IntoIter<(u32, u32, Option<Vec<u32>>)> {
        self.collect_entries().into_iter()
    }

    pub fn for_each_entry<E>(
        &self,
        mut visit: impl FnMut(u32, u32, Option<Vec<u32>>) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        let mut doc_ids = Vec::with_capacity(self.block_size);
        let mut frequencies = Vec::with_capacity(self.block_size);
        let mut decoded_positions = Vec::new();
        let mut position_block_index = 0usize;

        if let Some(encoded_blocks) = self.encoded_blocks.as_deref() {
            for block in encoded_blocks.iter() {
                doc_ids.clear();
                frequencies.clear();
                super::super::encoding::decode_full_posting_block(
                    block,
                    &mut doc_ids,
                    &mut frequencies,
                    self.block_size,
                );
                decoded_positions.clear();
                if self.with_positions {
                    let position_blocks = self
                        .encoded_position_blocks
                        .as_deref()
                        .expect("positions must exist for posting list");
                    super::super::encoding::decode_position_stream_block(
                        position_blocks.block(position_block_index),
                        &frequencies,
                        PositionStreamCodec::PackedDelta,
                        &mut decoded_positions,
                    )
                    .expect("position stream decoding should succeed");
                    position_block_index += 1;
                }
                let mut offset = 0usize;
                for (doc_id, frequency) in doc_ids.iter().copied().zip(frequencies.iter().copied())
                {
                    let positions = self.with_positions.then(|| {
                        let end = offset + frequency as usize;
                        let doc_positions = decoded_positions[offset..end].to_vec();
                        offset = end;
                        doc_positions
                    });
                    visit(doc_id, frequency, positions)?;
                }
            }
        }

        let mut decoded_tail_positions = Vec::new();
        if self.with_positions && !self.tail_entries.is_empty() {
            let tail_frequencies = self
                .tail_entries
                .iter()
                .map(|entry| entry.frequency)
                .collect::<Vec<_>>();
            self.tail_positions
                .decode_into(tail_frequencies.as_slice(), &mut decoded_tail_positions)
                .expect("tail position stream decoding should succeed");
        }
        let mut tail_offset = 0usize;
        for entry in &self.tail_entries {
            let positions = self.with_positions.then(|| {
                let end = tail_offset + entry.frequency as usize;
                let doc_positions = decoded_tail_positions[tail_offset..end].to_vec();
                tail_offset = end;
                doc_positions
            });
            visit(entry.doc_id, entry.frequency, positions)?;
        }

        Ok(())
    }

    pub fn add(&mut self, doc_id: u32, term_positions: PositionRecorder) {
        debug_assert!(
            self.open_doc_id.is_none(),
            "cannot add closed doc while a positions doc is still open"
        );
        let tail_entries_capacity_before = self.tail_entries.capacity();
        self.tail_entries
            .push(RawDocInfo::new(doc_id, term_positions.len()));
        let tail_entries_capacity_after = self.tail_entries.capacity();
        if tail_entries_capacity_after > tail_entries_capacity_before {
            self.add_memory_bytes(
                (tail_entries_capacity_after - tail_entries_capacity_before)
                    * std::mem::size_of::<RawDocInfo>(),
            );
        }
        if let PositionRecorder::Position(positions_in_doc) = term_positions {
            debug_assert!(self.with_positions);
            let old_size = self.tail_positions.size();
            self.tail_positions
                .append_doc_positions(positions_in_doc.as_slice())
                .expect("position stream encoding should succeed");
            self.adjust_tail_positions_size(old_size);
        }
        self.len += 1;

        if self.tail_entries.len() == self.block_size {
            self.flush_tail_block()
                .expect("posting list block compression should succeed");
        }
    }

    pub fn add_occurrence(&mut self, doc_id: u32, position: u32) -> Result<bool> {
        if !self.with_positions {
            return Err(Error::index(
                "cannot append streamed positions to a posting list without positions".to_owned(),
            ));
        }

        match self.open_doc_id {
            Some(open_doc_id) if open_doc_id == doc_id => {
                let old_size = self.tail_positions.size();
                self.tail_positions
                    .append_position(position, self.open_doc_last_position)?;
                self.adjust_tail_positions_size(old_size);
                self.open_doc_frequency += 1;
                self.open_doc_last_position = Some(position);
                Ok(false)
            }
            Some(open_doc_id) => Err(Error::index(format!(
                "posting list received doc {} before finishing open doc {}",
                doc_id, open_doc_id
            ))),
            None => {
                let old_size = self.tail_positions.size();
                self.tail_positions.append_position(position, None)?;
                self.adjust_tail_positions_size(old_size);
                self.open_doc_id = Some(doc_id);
                self.open_doc_frequency = 1;
                self.open_doc_last_position = Some(position);
                self.len += 1;
                Ok(true)
            }
        }
    }

    pub fn finish_open_doc(&mut self, doc_id: u32) -> Result<()> {
        if !self.with_positions {
            return Ok(());
        }
        match self.open_doc_id {
            Some(open_doc_id) if open_doc_id == doc_id => {
                let tail_entries_capacity_before = self.tail_entries.capacity();
                self.tail_entries
                    .push(RawDocInfo::new(doc_id, self.open_doc_frequency));
                let tail_entries_capacity_after = self.tail_entries.capacity();
                if tail_entries_capacity_after > tail_entries_capacity_before {
                    self.add_memory_bytes(
                        (tail_entries_capacity_after - tail_entries_capacity_before)
                            * std::mem::size_of::<RawDocInfo>(),
                    );
                }
                self.open_doc_id = None;
                self.open_doc_frequency = 0;
                self.open_doc_last_position = None;
                if self.tail_entries.len() == self.block_size {
                    self.flush_tail_block()?;
                }
                Ok(())
            }
            Some(open_doc_id) => Err(Error::index(format!(
                "attempted to finish doc {} while doc {} is still open",
                doc_id, open_doc_id
            ))),
            None => Ok(()),
        }
    }

    fn collect_entries(&self) -> Vec<(u32, u32, Option<Vec<u32>>)> {
        let mut entries = Vec::with_capacity(self.len());
        self.for_each_entry(|doc_id, frequency, positions| {
            entries.push((doc_id, frequency, positions));
            Ok::<(), ()>(())
        })
        .expect("collecting posting list entries should not fail");
        entries
    }

    fn encoded_blocks_mut(&mut self) -> &mut EncodedBlocks {
        if self.encoded_blocks.is_none() {
            self.encoded_blocks = Some(Box::default());
            self.add_memory_bytes(std::mem::size_of::<EncodedBlocks>());
        }
        self.encoded_blocks
            .as_deref_mut()
            .expect("encoded blocks must exist")
    }

    fn encoded_position_blocks_mut(&mut self) -> &mut EncodedPositionBlocks {
        if self.encoded_position_blocks.is_none() {
            self.encoded_position_blocks = Some(Box::default());
            self.add_memory_bytes(std::mem::size_of::<EncodedPositionBlocks>());
        }
        self.encoded_position_blocks
            .as_deref_mut()
            .expect("encoded position blocks must exist")
    }

    fn flush_tail_block(&mut self) -> Result<()> {
        if self.tail_entries.is_empty() {
            return Ok(());
        }
        debug_assert!(
            self.open_doc_id.is_none(),
            "cannot flush a posting block while a document is still open"
        );
        debug_assert_eq!(self.tail_entries.len(), self.block_size);
        let doc_ids = self
            .tail_entries
            .iter()
            .map(|entry| entry.doc_id)
            .collect::<Vec<_>>();
        let frequencies = self
            .tail_entries
            .iter()
            .map(|entry| entry.frequency)
            .collect::<Vec<_>>();
        let encoded_blocks_size_before = self
            .encoded_blocks
            .as_ref()
            .map(|encoded_blocks| encoded_blocks.size())
            .unwrap_or(0usize);
        self.encoded_blocks_mut()
            .push_full_block(&doc_ids, &frequencies)?;
        let encoded_blocks_size_after = self
            .encoded_blocks
            .as_ref()
            .map(|encoded_blocks| encoded_blocks.size())
            .unwrap_or(0usize);
        if encoded_blocks_size_after > encoded_blocks_size_before {
            self.add_memory_bytes(encoded_blocks_size_after - encoded_blocks_size_before);
        }
        if self.with_positions {
            let encoded_positions_size_before = self
                .encoded_position_blocks
                .as_ref()
                .map(|encoded| encoded.size())
                .unwrap_or(0usize);
            let released_tail_positions_bytes = self.tail_positions.size();
            let tail_position_block = std::mem::take(&mut self.tail_positions).finish();
            self.encoded_position_blocks_mut()
                .push_encoded_block(tail_position_block.as_slice());
            let encoded_positions_size_after = self
                .encoded_position_blocks
                .as_ref()
                .map(|encoded| encoded.size())
                .unwrap_or(0usize);
            if released_tail_positions_bytes > 0 {
                self.subtract_memory_bytes(released_tail_positions_bytes);
            }
            if encoded_positions_size_after > encoded_positions_size_before {
                self.add_memory_bytes(encoded_positions_size_after - encoded_positions_size_before);
            }
        }
        self.tail_entries.clear();
        Ok(())
    }

    fn adjust_tail_positions_size(&mut self, old_size: usize) {
        let new_size = self.tail_positions.size();
        if new_size > old_size {
            self.add_memory_bytes(new_size - old_size);
        } else if old_size > new_size {
            self.subtract_memory_bytes(old_size - new_size);
        }
    }

    fn add_memory_bytes(&mut self, bytes: usize) {
        self.memory_size_bytes = self
            .memory_size_bytes
            .checked_add(
                u32::try_from(bytes).expect("posting list memory size delta overflowed u32"),
            )
            .expect("posting list memory size overflowed u32");
    }

    fn subtract_memory_bytes(&mut self, bytes: usize) {
        self.memory_size_bytes = self
            .memory_size_bytes
            .checked_sub(
                u32::try_from(bytes).expect("posting list memory size delta overflowed u32"),
            )
            .expect("posting list memory size underflowed u32");
    }

    fn build_position_columns(
        positions: Option<CompressedPositionStorage>,
    ) -> Result<Vec<ArrayRef>> {
        let Some(positions) = positions else {
            return Ok(Vec::new());
        };
        match positions {
            CompressedPositionStorage::LegacyPerDoc(positions) => {
                Ok(vec![Arc::new(ListArray::try_new(
                    Arc::new(Field::new("item", positions.data_type().clone(), true)),
                    OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, positions.len() as i32])),
                    Arc::new(positions) as ArrayRef,
                    None,
                )?) as ArrayRef])
            }
            CompressedPositionStorage::SharedStream(positions) => {
                let mut columns = Vec::with_capacity(2);
                columns.push(
                    Arc::new(LargeBinaryArray::from(vec![Some(positions.bytes())])) as ArrayRef,
                );

                let mut offsets_builder = ListBuilder::new(UInt32Builder::new());
                for &offset in positions.block_offsets() {
                    offsets_builder.values().append_value(offset);
                }
                offsets_builder.append(true);
                columns.push(Arc::new(offsets_builder.finish()) as ArrayRef);
                Ok(columns)
            }
        }
    }

    fn build_batch(
        self,
        compressed: LargeBinaryArray,
        impacts: Option<ImpactSkipData>,
        max_score: f32,
        schema: SchemaRef,
        positions: Option<CompressedPositionStorage>,
    ) -> Result<RecordBatch> {
        let length = self.len();
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, compressed.len() as i32]));
        let mut columns = vec![
            Arc::new(ListArray::try_new(
                Arc::new(Field::new("item", datatypes::DataType::LargeBinary, true)),
                offsets,
                Arc::new(compressed),
                None,
            )?) as ArrayRef,
            Arc::new(Float32Array::from_iter_values(std::iter::once(max_score))) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(std::iter::once(
                length as u32,
            ))) as ArrayRef,
        ];
        if schema.field_with_name(IMPACT_COL).is_ok() {
            let impacts = impacts.ok_or_else(|| {
                Error::index(format!(
                    "impact column requested without impact data for posting length {}",
                    length
                ))
            })?;
            let impact_offsets =
                OffsetBuffer::new(ScalarBuffer::from(vec![0, impacts.entries().len() as i32]));
            columns.push(Arc::new(ListArray::try_new(
                Arc::new(Field::new("item", datatypes::DataType::LargeBinary, true)),
                impact_offsets,
                Arc::new(impacts.entries().clone()),
                None,
            )?) as ArrayRef);
        }
        columns.extend(Self::build_position_columns(positions)?);

        let batch = RecordBatch::try_new(schema, columns)?;
        Ok(batch)
    }

    fn build_legacy_positions(&self) -> Result<ListArray> {
        let mut positions_builder = ListBuilder::new(LargeBinaryBuilder::new());
        self.for_each_entry(|_doc_id, frequency, positions| {
            let positions = positions.ok_or_else(|| {
                Error::index(format!(
                    "legacy position writer missing positions for frequency {}",
                    frequency
                ))
            })?;
            let compressed = super::super::encoding::compress_positions(positions.as_slice())?;
            for block_idx in 0..compressed.len() {
                positions_builder
                    .values()
                    .append_value(compressed.value(block_idx));
            }
            positions_builder.append(true);
            Ok::<(), Error>(())
        })?;
        Ok(positions_builder.finish())
    }

    pub(in super::super) fn append_to_batch_with_docs(
        self,
        docs: &DocSet,
        batch_builder: &mut PostingListBatchBuilder,
        format_version: InvertedListFormatVersion,
    ) -> Result<()> {
        let legacy_positions =
            if self.with_positions && !format_version.uses_shared_position_stream() {
                Some(self.build_legacy_positions()?)
            } else {
                None
            };
        let Self {
            with_positions,
            posting_tail_codec,
            encoded_blocks,
            encoded_position_blocks,
            tail_entries,
            tail_positions,
            open_doc_id,
            open_doc_frequency,
            open_doc_last_position,
            block_size,
            len,
            ..
        } = self;
        debug_assert!(open_doc_id.is_none());
        debug_assert_eq!(open_doc_frequency, 0);
        debug_assert!(open_doc_last_position.is_none());
        let parts = PostingListParts {
            with_positions,
            posting_tail_codec,
            block_size,
            length: len as usize,
            encoded_blocks: encoded_blocks
                .map(|encoded_blocks| *encoded_blocks)
                .unwrap_or_default(),
            encoded_position_blocks: encoded_position_blocks
                .map(|encoded_positions| *encoded_positions)
                .unwrap_or_default(),
            tail_entries: tail_entries.as_slice(),
            tail_position_block: with_positions.then(|| tail_positions.finish()),
        };
        let (compressed, shared_positions, max_score, impacts) =
            Self::build_compressed_with_scores_from_parts(parts, docs)?;
        let positions = match legacy_positions {
            Some(positions) => Some(CompressedPositionStorage::LegacyPerDoc(positions)),
            None => shared_positions.map(CompressedPositionStorage::SharedStream),
        };
        batch_builder.append(
            compressed,
            Some(&impacts),
            max_score,
            len,
            positions.as_ref(),
        )
    }

    fn extend_tail_components(
        tail_entries: &[RawDocInfo],
        doc_ids: &mut Vec<u32>,
        frequencies: &mut Vec<u32>,
    ) {
        doc_ids.clear();
        frequencies.clear();
        doc_ids.extend(tail_entries.iter().map(|entry| entry.doc_id));
        frequencies.extend(tail_entries.iter().map(|entry| entry.frequency));
    }

    fn build_compressed_with_scores_from_parts(
        parts: PostingListParts<'_>,
        docs: &DocSet,
    ) -> Result<(
        LargeBinaryArray,
        Option<SharedPositionStream>,
        f32,
        ImpactSkipData,
    )> {
        let PostingListParts {
            with_positions,
            posting_tail_codec,
            length,
            block_size,
            mut encoded_blocks,
            mut encoded_position_blocks,
            tail_entries,
            tail_position_block,
        } = parts;
        let avgdl = docs.average_length();
        let idf_scale = idf(length, docs.len()) * (K1 + 1.0);
        let mut max_score = f32::MIN;
        let mut doc_ids = Vec::with_capacity(block_size);
        let mut frequencies = Vec::with_capacity(block_size);
        let mut impact_block = Vec::with_capacity(block_size);
        let mut impact_builder =
            ImpactSkipDataBuilder::with_capacity(length.div_ceil(block_size), block_size);

        for index in 0..encoded_blocks.len() {
            let block = encoded_blocks.block(index);
            doc_ids.clear();
            frequencies.clear();
            super::super::encoding::decode_full_posting_block(
                block,
                &mut doc_ids,
                &mut frequencies,
                block_size,
            );
            let block_score = compute_block_score_and_impact_block(
                docs,
                avgdl,
                idf_scale,
                doc_ids.iter().copied(),
                frequencies.iter().copied(),
                &mut impact_block,
            );
            impact_builder.append_block(impact_block.as_slice())?;
            max_score = max_score.max(block_score);
            if super::super::encoding::posting_block_score_prefix_len(block_size) > 0 {
                encoded_blocks.set_block_score(index, block_score);
            }
        }

        if !tail_entries.is_empty() {
            Self::extend_tail_components(tail_entries, &mut doc_ids, &mut frequencies);
            let block_score = compute_block_score_and_impact_block(
                docs,
                avgdl,
                idf_scale,
                doc_ids.iter().copied(),
                frequencies.iter().copied(),
                &mut impact_block,
            );
            impact_builder.append_block(impact_block.as_slice())?;
            max_score = max_score.max(block_score);
            encoded_blocks.append_remainder_block_with_codec(
                doc_ids.as_slice(),
                frequencies.as_slice(),
                posting_tail_codec,
                block_size,
            )?;
            if super::super::encoding::posting_block_score_prefix_len(block_size) > 0 {
                encoded_blocks.set_block_score(encoded_blocks.len() - 1, block_score);
            }
            if with_positions {
                encoded_position_blocks.push_encoded_block(
                    tail_position_block
                        .as_deref()
                        .expect("tail position block must exist for postings with positions"),
                );
            }
        }

        let impacts = impact_builder.finish()?;
        Ok((
            encoded_blocks.into_array(),
            with_positions.then(|| encoded_position_blocks.into_stream()),
            max_score,
            impacts,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn build_compressed_with_block_scores_from_parts(
        with_positions: bool,
        posting_tail_codec: PostingTailCodec,
        block_size: usize,
        mut encoded_blocks: EncodedBlocks,
        mut encoded_position_blocks: EncodedPositionBlocks,
        tail_entries: &[RawDocInfo],
        tail_position_block: Option<Vec<u8>>,
        mut block_max_scores: impl Iterator<Item = f32>,
    ) -> Result<(LargeBinaryArray, Option<SharedPositionStream>, f32)> {
        let has_score_prefix =
            super::super::encoding::posting_block_score_prefix_len(block_size) > 0;
        let mut max_score = f32::MIN;
        let mut doc_ids = Vec::with_capacity(BLOCK_SIZE);
        let mut frequencies = Vec::with_capacity(BLOCK_SIZE);

        for index in 0..encoded_blocks.len() {
            let block_score = block_max_scores
                .next()
                .ok_or_else(|| Error::index("missing block max score".to_owned()))?;
            max_score = max_score.max(block_score);
            if has_score_prefix {
                encoded_blocks.set_block_score(index, block_score);
            }
        }

        if !tail_entries.is_empty() {
            let block_score = block_max_scores
                .next()
                .ok_or_else(|| Error::index("missing tail block max score".to_owned()))?;
            max_score = max_score.max(block_score);
            Self::extend_tail_components(tail_entries, &mut doc_ids, &mut frequencies);
            encoded_blocks.append_remainder_block_with_codec(
                doc_ids.as_slice(),
                frequencies.as_slice(),
                posting_tail_codec,
                block_size,
            )?;
            if has_score_prefix {
                encoded_blocks.set_block_score(encoded_blocks.len() - 1, block_score);
            }
            if with_positions {
                encoded_position_blocks.push_encoded_block(
                    tail_position_block
                        .as_deref()
                        .expect("tail position block must exist for postings with positions"),
                );
            }
        }

        Ok((
            encoded_blocks.into_array(),
            with_positions.then(|| encoded_position_blocks.into_stream()),
            max_score,
        ))
    }

    pub fn to_batch(self, block_max_scores: Vec<f32>) -> Result<RecordBatch> {
        let format_version = InvertedListFormatVersion::from_posting_tail_codec_and_block_size(
            self.posting_tail_codec,
            self.block_size,
        )?;
        let schema = inverted_list_schema_for_version_with_block_size_and_impacts(
            self.has_positions(),
            format_version,
            self.block_size,
            false,
        );
        let legacy_positions =
            if self.with_positions && !format_version.uses_shared_position_stream() {
                Some(self.build_legacy_positions()?)
            } else {
                None
            };
        let Self {
            with_positions,
            posting_tail_codec,
            encoded_blocks,
            encoded_position_blocks,
            tail_entries,
            tail_positions,
            open_doc_id,
            open_doc_frequency,
            open_doc_last_position,
            block_size,
            len,
            ..
        } = self;
        debug_assert!(open_doc_id.is_none());
        debug_assert_eq!(open_doc_frequency, 0);
        debug_assert!(open_doc_last_position.is_none());
        let (compressed, shared_positions, max_score) =
            Self::build_compressed_with_block_scores_from_parts(
                with_positions,
                posting_tail_codec,
                block_size,
                encoded_blocks
                    .map(|encoded_blocks| *encoded_blocks)
                    .unwrap_or_default(),
                encoded_position_blocks
                    .map(|encoded_positions| *encoded_positions)
                    .unwrap_or_default(),
                tail_entries.as_slice(),
                with_positions.then(|| tail_positions.finish()),
                block_max_scores.into_iter(),
            )?;
        let builder = Self {
            with_positions,
            posting_tail_codec,
            encoded_blocks: None,
            encoded_position_blocks: None,
            tail_entries: Vec::new(),
            tail_positions: PositionBlockBuilder::default(),
            open_doc_id: None,
            open_doc_frequency: 0,
            open_doc_last_position: None,
            block_size,
            memory_size_bytes: 0,
            len,
        };
        let positions = match legacy_positions {
            Some(positions) => Some(CompressedPositionStorage::LegacyPerDoc(positions)),
            None => shared_positions.map(CompressedPositionStorage::SharedStream),
        };
        builder.build_batch(compressed, None, max_score, schema, positions)
    }

    pub fn to_batch_with_docs(self, docs: &DocSet, schema: SchemaRef) -> Result<RecordBatch> {
        let format_version = parse_format_version_from_metadata(schema.metadata())?;
        let legacy_positions =
            if self.with_positions && !format_version.uses_shared_position_stream() {
                Some(self.build_legacy_positions()?)
            } else {
                None
            };
        let Self {
            with_positions,
            posting_tail_codec,
            encoded_blocks,
            encoded_position_blocks,
            tail_entries,
            tail_positions,
            open_doc_id,
            open_doc_frequency,
            open_doc_last_position,
            block_size,
            len,
            ..
        } = self;
        debug_assert!(open_doc_id.is_none());
        debug_assert_eq!(open_doc_frequency, 0);
        debug_assert!(open_doc_last_position.is_none());
        let parts = PostingListParts {
            with_positions,
            posting_tail_codec,
            block_size,
            length: len as usize,
            encoded_blocks: encoded_blocks
                .map(|encoded_blocks| *encoded_blocks)
                .unwrap_or_default(),
            encoded_position_blocks: encoded_position_blocks
                .map(|encoded_positions| *encoded_positions)
                .unwrap_or_default(),
            tail_entries: tail_entries.as_slice(),
            tail_position_block: with_positions.then(|| tail_positions.finish()),
        };
        let (compressed, shared_positions, max_score, impacts) =
            Self::build_compressed_with_scores_from_parts(parts, docs)?;
        let builder = Self {
            with_positions,
            posting_tail_codec,
            encoded_blocks: None,
            encoded_position_blocks: None,
            tail_entries: Vec::new(),
            tail_positions: PositionBlockBuilder::default(),
            open_doc_id: None,
            open_doc_frequency: 0,
            open_doc_last_position: None,
            block_size,
            memory_size_bytes: 0,
            len,
        };
        let positions = match legacy_positions {
            Some(positions) => Some(CompressedPositionStorage::LegacyPerDoc(positions)),
            None => shared_positions.map(CompressedPositionStorage::SharedStream),
        };
        builder.build_batch(compressed, Some(impacts), max_score, schema, positions)
    }

    pub fn remap(&mut self, removed: &[u32]) {
        let mut cursor = 0;
        let mut new_builder = Self::new_with_posting_tail_codec_and_block_size(
            self.has_positions(),
            self.posting_tail_codec,
            self.block_size,
        );
        for (doc_id, freq, positions) in self.iter() {
            while cursor < removed.len() && removed[cursor] < doc_id {
                cursor += 1;
            }
            if cursor < removed.len() && removed[cursor] == doc_id {
                continue;
            }
            let positions = match positions {
                Some(positions) => PositionRecorder::Position(positions.into()),
                None => PositionRecorder::Count(freq),
            };
            new_builder.add(doc_id - cursor as u32, positions);
        }

        *self = new_builder;
    }
}

pub(super) fn compute_block_score_and_impact_block(
    docs: &DocSet,
    avgdl: f32,
    idf_scale: f32,
    doc_ids: impl Iterator<Item = u32>,
    frequencies: impl Iterator<Item = u32>,
    impact_block: &mut Vec<(u32, u32, u32)>,
) -> f32 {
    impact_block.clear();
    let mut block_max_score = f32::MIN;
    for (doc_id, freq) in doc_ids.zip(frequencies) {
        let doc_len = docs.num_tokens(doc_id);
        let doc_norm = K1 * (1.0 - B + B * doc_len as f32 / avgdl);
        let freq_f32 = freq as f32;
        let score = freq_f32 / (freq_f32 + doc_norm);
        block_max_score = block_max_score.max(score);
        impact_block.push((doc_id, freq, doc_len));
    }
    block_max_score * idf_scale
}
