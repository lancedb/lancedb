// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

pub(in super::super) struct PostingListBatchBuilder {
    schema: SchemaRef,
    postings: ListBuilder<LargeBinaryBuilder>,
    impacts: Option<ListBuilder<LargeBinaryBuilder>>,
    max_scores: Float32Builder,
    lengths: UInt32Builder,
    positions: BatchPositionsBuilder,
    len: usize,
}

pub(super) enum BatchPositionsBuilder {
    None,
    Legacy(ListBuilder<ListBuilder<LargeBinaryBuilder>>),
    Shared {
        bytes: LargeBinaryBuilder,
        block_offsets: ListBuilder<UInt32Builder>,
    },
}

pub(super) struct PostingListParts<'a> {
    pub(super) with_positions: bool,
    pub(super) posting_tail_codec: PostingTailCodec,
    pub(super) block_size: usize,
    pub(super) length: usize,
    pub(super) encoded_blocks: EncodedBlocks,
    pub(super) encoded_position_blocks: EncodedPositionBlocks,
    pub(super) tail_entries: &'a [RawDocInfo],
    pub(super) tail_position_block: Option<Vec<u8>>,
}

impl PostingListBatchBuilder {
    pub fn new(
        schema: SchemaRef,
        with_positions: bool,
        format_version: InvertedListFormatVersion,
        capacity: usize,
    ) -> Self {
        let positions = if !with_positions {
            BatchPositionsBuilder::None
        } else if format_version.uses_shared_position_stream() {
            BatchPositionsBuilder::Shared {
                bytes: LargeBinaryBuilder::with_capacity(capacity, 0),
                block_offsets: ListBuilder::with_capacity(UInt32Builder::new(), capacity),
            }
        } else {
            BatchPositionsBuilder::Legacy(ListBuilder::with_capacity(
                ListBuilder::new(LargeBinaryBuilder::new()),
                capacity,
            ))
        };
        let impacts = schema
            .field_with_name(IMPACT_COL)
            .ok()
            .map(|_| ListBuilder::with_capacity(LargeBinaryBuilder::new(), capacity));
        Self {
            schema,
            postings: ListBuilder::with_capacity(LargeBinaryBuilder::new(), capacity),
            impacts,
            max_scores: Float32Builder::with_capacity(capacity),
            lengths: UInt32Builder::with_capacity(capacity),
            positions,
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(super) fn append(
        &mut self,
        compressed: LargeBinaryArray,
        impacts: Option<&ImpactSkipData>,
        max_score: f32,
        length: u32,
        positions: Option<&CompressedPositionStorage>,
    ) -> Result<()> {
        {
            let values = self.postings.values();
            for index in 0..compressed.len() {
                values.append_value(compressed.value(index));
            }
        }
        self.postings.append(true);
        if let Some(impacts_builder) = &mut self.impacts {
            let impacts = impacts.ok_or_else(|| {
                Error::index(format!(
                    "impacts builder missing impact data for posting length {}",
                    length
                ))
            })?;
            let values = impacts_builder.values();
            for index in 0..impacts.entries().len() {
                values.append_value(impacts.entries().value(index));
            }
            impacts_builder.append(true);
        }
        self.max_scores.append_value(max_score);
        self.lengths.append_value(length);

        match &mut self.positions {
            BatchPositionsBuilder::None => {}
            BatchPositionsBuilder::Shared {
                bytes,
                block_offsets,
            } => {
                let positions = positions.ok_or_else(|| {
                    Error::index(format!(
                        "positions builder missing position data for posting length {}",
                        length
                    ))
                })?;
                let CompressedPositionStorage::SharedStream(positions) = positions else {
                    return Err(Error::index(
                        "shared positions builder received legacy positions".to_owned(),
                    ));
                };
                bytes.append_value(positions.bytes());
                let offsets_builder = block_offsets.values();
                for &offset in positions.block_offsets() {
                    offsets_builder.append_value(offset);
                }
                block_offsets.append(true);
            }
            BatchPositionsBuilder::Legacy(position_lists) => {
                let positions = positions.ok_or_else(|| {
                    Error::index(format!(
                        "positions builder missing position data for posting length {}",
                        length
                    ))
                })?;
                let CompressedPositionStorage::LegacyPerDoc(positions) = positions else {
                    return Err(Error::index(
                        "legacy positions builder received shared position stream".to_owned(),
                    ));
                };
                let docs_builder = position_lists.values();
                for doc_idx in 0..positions.len() {
                    let doc_positions = positions.value(doc_idx);
                    let compressed_positions = doc_positions.as_binary::<i64>();
                    for block_idx in 0..compressed_positions.len() {
                        docs_builder
                            .values()
                            .append_value(compressed_positions.value(block_idx));
                    }
                    docs_builder.append(true);
                }
                position_lists.append(true);
            }
        }

        self.len += 1;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<RecordBatch> {
        let mut columns = vec![
            Arc::new(self.postings.finish()) as ArrayRef,
            Arc::new(self.max_scores.finish()) as ArrayRef,
            Arc::new(self.lengths.finish()) as ArrayRef,
        ];
        if let Some(impacts) = &mut self.impacts {
            columns.push(Arc::new(impacts.finish()) as ArrayRef);
        }
        match &mut self.positions {
            BatchPositionsBuilder::None => {}
            BatchPositionsBuilder::Legacy(position_lists) => {
                columns.push(Arc::new(position_lists.finish()) as ArrayRef);
            }
            BatchPositionsBuilder::Shared {
                bytes,
                block_offsets,
            } => {
                columns.push(Arc::new(bytes.finish()) as ArrayRef);
                columns.push(Arc::new(block_offsets.finish()) as ArrayRef);
            }
        }
        self.len = 0;
        RecordBatch::try_new(self.schema.clone(), columns).map_err(Error::from)
    }
}
