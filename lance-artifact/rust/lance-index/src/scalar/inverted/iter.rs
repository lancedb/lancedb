// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow::array::AsArray;
use arrow_array::{Array, LargeBinaryArray};

use super::{
    CompressedPositionStorage, PostingList, PostingTailCodec,
    encoding::{
        MAX_POSTING_BLOCK_SIZE, decode_position_stream_block, decompress_positions,
        decompress_posting_block, decompress_posting_remainder,
    },
};

pub enum PostingListIterator<'a> {
    Plain(PlainPostingListIterator<'a>),
    Compressed(Box<CompressedPostingListIterator>),
}

impl<'a> PostingListIterator<'a> {
    pub fn new(posting: &'a PostingList) -> Self {
        match posting {
            PostingList::Plain(posting) => Self::Plain(posting.iter()),
            PostingList::Compressed(posting) => {
                Self::Compressed(Box::new(CompressedPostingListIterator::new(
                    posting.length as usize,
                    posting.blocks.clone(),
                    posting.posting_tail_codec,
                    posting.positions.clone(),
                    posting.block_size,
                )))
            }
        }
    }
}

impl<'a> Iterator for PostingListIterator<'a> {
    type Item = (u64, u32, Option<Box<dyn Iterator<Item = u32> + 'a>>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            PostingListIterator::Plain(iter) => iter
                .next()
                .map(|(doc_id, freq, pos)| (doc_id, freq as u32, pos)),
            PostingListIterator::Compressed(iter) => iter
                .next()
                .map(|(doc_id, freq, pos)| (doc_id as u64, freq, pos)),
        }
    }
}

pub type PlainPostingListIterator<'a> =
    Box<dyn Iterator<Item = (u64, f32, Option<Box<dyn Iterator<Item = u32> + 'a>>)> + 'a>;

struct OwnedPositionsIter {
    positions: Box<[u32]>,
    index: usize,
}

impl OwnedPositionsIter {
    fn new(positions: &[u32]) -> Self {
        Self {
            positions: Box::<[u32]>::from(positions),
            index: 0,
        }
    }
}

impl Iterator for OwnedPositionsIter {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        let position = self.positions.get(self.index).copied()?;
        self.index += 1;
        Some(position)
    }
}

pub struct CompressedPostingListIterator {
    remainder: usize,
    blocks: LargeBinaryArray,
    next_block_idx: usize,
    posting_tail_codec: PostingTailCodec,
    positions: Option<CompressedPositionStorage>,
    block_size: usize,
    idx: usize,
    doc_ids: Vec<u32>,
    frequencies: Vec<u32>,
    doc_idx_in_block: usize,
    decoded_positions: Vec<u32>,
    position_offsets: Vec<usize>,
    buffer: [u32; MAX_POSTING_BLOCK_SIZE],
}

impl CompressedPostingListIterator {
    pub fn new(
        length: usize,
        blocks: LargeBinaryArray,
        posting_tail_codec: PostingTailCodec,
        positions: Option<CompressedPositionStorage>,
        block_size: usize,
    ) -> Self {
        debug_assert!(length > 0, "length: {}", length);
        debug_assert_eq!(
            length.div_ceil(block_size),
            blocks.len(),
            "length: {}, num_blocks: {}",
            length,
            blocks.len(),
        );

        Self {
            remainder: length % block_size,
            blocks,
            next_block_idx: 0,
            posting_tail_codec,
            positions,
            block_size,
            idx: 0,
            doc_ids: Vec::new(),
            frequencies: Vec::new(),
            doc_idx_in_block: 0,
            decoded_positions: Vec::new(),
            position_offsets: Vec::new(),
            buffer: [0; MAX_POSTING_BLOCK_SIZE],
        }
    }
}

impl Iterator for CompressedPostingListIterator {
    type Item = (u32, u32, Option<Box<dyn Iterator<Item = u32>>>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.doc_idx_in_block < self.doc_ids.len() {
            let doc_id = self.doc_ids[self.doc_idx_in_block];
            let freq = self.frequencies[self.doc_idx_in_block];
            let positions = self.positions.as_ref().map(|storage| match storage {
                CompressedPositionStorage::LegacyPerDoc(list) => {
                    let compressed = list.value(self.idx);
                    let positions = decompress_positions(compressed.as_binary());
                    Box::new(positions.into_iter()) as Box<dyn Iterator<Item = u32>>
                }
                CompressedPositionStorage::SharedStream(_) => {
                    let start = self.position_offsets[self.doc_idx_in_block];
                    let end = self.position_offsets[self.doc_idx_in_block + 1];
                    Box::new(OwnedPositionsIter::new(&self.decoded_positions[start..end]))
                        as Box<dyn Iterator<Item = u32>>
                }
            });
            self.idx += 1;
            self.doc_idx_in_block += 1;
            return Some((doc_id, freq, positions));
        }

        // move to the next block
        if self.next_block_idx >= self.blocks.len() {
            return None;
        }
        let compressed = self.blocks.value(self.next_block_idx);
        self.next_block_idx += 1;

        self.doc_ids.clear();
        self.frequencies.clear();
        if self.next_block_idx == self.blocks.len() && self.remainder > 0 {
            decompress_posting_remainder(
                compressed,
                self.remainder,
                self.posting_tail_codec,
                self.block_size,
                &mut self.doc_ids,
                &mut self.frequencies,
            );
        } else {
            decompress_posting_block(
                compressed,
                &mut self.buffer,
                &mut self.doc_ids,
                &mut self.frequencies,
                self.block_size,
            );
        }
        self.doc_idx_in_block = 0;
        self.decoded_positions.clear();
        self.position_offsets.clear();
        if let Some(CompressedPositionStorage::SharedStream(stream)) = self.positions.as_ref() {
            decode_position_stream_block(
                stream.block(self.next_block_idx - 1),
                self.frequencies.as_slice(),
                stream.codec(),
                &mut self.decoded_positions,
            )
            .expect("shared position stream decoding should succeed");
            self.position_offsets.reserve(self.frequencies.len() + 1);
            self.position_offsets.push(0);
            let mut offset = 0usize;
            for &frequency in &self.frequencies {
                offset += frequency as usize;
                self.position_offsets.push(offset);
            }
        }
        self.next()
    }
}

pub fn take_fst_keys<'f, I, S>(s: I, dst: &mut Vec<String>, max_expansion: usize)
where
    I: for<'a> fst::IntoStreamer<'a, Into = S, Item = (&'a [u8], u64)>,
    S: 'f + for<'a> fst::Streamer<'a, Item = (&'a [u8], u64)>,
{
    let mut stream = s.into_stream();
    while let Some((token, _)) = stream.next() {
        dst.push(String::from_utf8_lossy(token).into_owned());
        if dst.len() >= max_expansion {
            break;
        }
    }
}
