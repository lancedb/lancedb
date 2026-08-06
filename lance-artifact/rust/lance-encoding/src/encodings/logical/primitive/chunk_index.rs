// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Compact per-page chunk index for the mini-block structural encoding.
//!
//! The chunk index is stored on disk in an extremely compressed form that
//! requires a lot of CPU to work with.  However, extracting it out to its full
//! width can be RAM-intensive.  As a compromise we extract into a prefix-sum
//! array that we fit into `u32` if possible and we avoid storing per-block row
//! counts when those are redundant.  The scheduler looks chunks up by index
//! (byte range, leaf value count) and by row (which chunk holds a row).
//!
//! ```text
//! MiniBlockChunkIndex
//! |- base: u64                      absolute file position of the value buffer
//! |- byte_starts: PrefixSums        cumulative chunk byte sizes (all pages)
//! `- rows: RowMapping
//!    |- UniformFlat { .. }          flat page, uniform leaf chunking (arithmetic)
//!    |- Flat { value_starts }       flat page, non-uniform leaf chunking
//!    `- Nested { row_starts, .. }   repetition present; rows tracked as prefix sums
//! ```

use std::ops::Range;

use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use lance_core::cache::{Context, DeepSizeOf};

/// Cumulative (prefix-sum) array of length `num_chunks + 1` (entry `0` is `0`,
/// the last entry is the grand total).  Stored as `u32` when the total fits,
/// else `u64`.
#[derive(Debug, DeepSizeOf)]
pub enum PrefixSums {
    U32(Vec<u32>),
    U64(Vec<u64>),
}

impl PrefixSums {
    /// Builds the cumulative array from per-chunk `deltas`.  `total` selects the
    /// storage width (callers must pass the true sum); `num_chunks` only pre-sizes.
    pub fn from_deltas(deltas: impl Iterator<Item = u64>, num_chunks: usize, total: u64) -> Self {
        if total <= u32::MAX as u64 {
            let mut values = Vec::with_capacity(num_chunks + 1);
            let mut acc = 0u32;
            values.push(0);
            for delta in deltas {
                acc += delta as u32;
                values.push(acc);
            }
            debug_assert_eq!(values.len(), num_chunks + 1);
            debug_assert_eq!(acc as u64, total);
            Self::U32(values)
        } else {
            let mut values = Vec::with_capacity(num_chunks + 1);
            let mut acc = 0u64;
            values.push(0);
            for delta in deltas {
                acc += delta;
                values.push(acc);
            }
            debug_assert_eq!(values.len(), num_chunks + 1);
            debug_assert_eq!(acc, total);
            Self::U64(values)
        }
    }

    /// Builds a `PrefixSums` from an already-cumulative array (`[0, .., total]`),
    /// narrowing to `u32` when the total fits.  Avoids the deltas buffer
    /// [`Self::from_deltas`] would need.
    fn from_prefix(prefix: Vec<u64>) -> Self {
        debug_assert!(!prefix.is_empty());
        debug_assert_eq!(prefix[0], 0);
        let total = prefix.last().copied().unwrap_or(0);
        if total <= u32::MAX as u64 {
            Self::U32(prefix.into_iter().map(|v| v as u32).collect())
        } else {
            Self::U64(prefix)
        }
    }

    /// Cumulative value at position `i` (i.e. the start of chunk `i`).
    pub fn get(&self, i: usize) -> u64 {
        match self {
            Self::U32(values) => values[i] as u64,
            Self::U64(values) => values[i],
        }
    }

    /// Start and end of chunk `i` (positions `i`, `i + 1`) behind one width
    /// match -- halves the branching of two `get` calls on the hot per-chunk path.
    pub fn get_pair(&self, i: usize) -> (u64, u64) {
        match self {
            Self::U32(values) => (values[i] as u64, values[i + 1] as u64),
            Self::U64(values) => (values[i], values[i + 1]),
        }
    }

    /// Number of chunks (array length minus the trailing total).
    pub fn num_chunks(&self) -> usize {
        match self {
            Self::U32(values) => values.len() - 1,
            Self::U64(values) => values.len() - 1,
        }
    }

    /// Size of chunk `i` (the delta between consecutive cumulative values).
    pub fn delta(&self, i: usize) -> u64 {
        let (start, end) = self.get_pair(i);
        end - start
    }

    /// Index of the chunk whose half-open span `[get(i), get(i+1))` contains
    /// `value`.  On an exact hit against a chunk start, returns the *first* chunk
    /// with that start (chunks can share a start row).
    pub fn find(&self, value: u64) -> usize {
        // Match the width once, then binary-search only the starts (not the
        // trailing total).  `partition_point` already yields the first of any
        // duplicated starts; the `idx - 1` fallback is safe since `get(0) == 0`.
        match self {
            Self::U32(values) => {
                let starts = &values[..values.len() - 1];
                let idx = starts.partition_point(|&start| (start as u64) < value);
                if idx < starts.len() && starts[idx] as u64 == value {
                    idx
                } else {
                    idx - 1
                }
            }
            Self::U64(values) => {
                let starts = &values[..values.len() - 1];
                let idx = starts.partition_point(|&start| start < value);
                if idx < starts.len() && starts[idx] == value {
                    idx
                } else {
                    idx - 1
                }
            }
        }
    }
}

/// Leaf value counts per chunk, needed to decode.  Tracked only for nested
/// pages; flat pages read items off the row mapping (rows == items).
#[derive(Debug, DeepSizeOf)]
pub enum ItemCounts {
    /// Every non-last chunk holds the same number of values.
    Uniform {
        values_per_chunk: u64,
        last_chunk_values: u64,
    },
    /// `log2` of each chunk's value count, stored as one byte per chunk rather
    /// than the full count because this index stays cached in RAM; the last
    /// chunk is handled via `last_chunk_values`.
    PerChunkLog {
        logs: Vec<u8>,
        last_chunk_values: u64,
    },
}

impl ItemCounts {
    fn get(&self, i: usize, num_chunks: usize) -> u64 {
        match self {
            Self::Uniform {
                values_per_chunk,
                last_chunk_values,
            } => {
                if i == num_chunks - 1 {
                    *last_chunk_values
                } else {
                    *values_per_chunk
                }
            }
            Self::PerChunkLog {
                logs,
                last_chunk_values,
            } => {
                if i == num_chunks - 1 {
                    *last_chunk_values
                } else {
                    1u64 << logs[i]
                }
            }
        }
    }
}

/// How row ranges map onto chunks.
///
/// Flat pages have row == value index and no preamble/trailer.  Nested pages
/// track rows separately from leaf items and store a trailer bit per chunk; a
/// chunk's preamble is the previous chunk's trailer.
#[derive(Debug)]
pub enum RowMapping {
    /// Flat page whose non-last chunks all hold `values_per_chunk` values, so
    /// row->chunk is pure arithmetic.
    UniformFlat {
        values_per_chunk: u64,
        last_chunk_values: u64,
        num_chunks: usize,
    },
    /// Flat page with non-uniform chunk sizes; `value_starts` are the cumulative
    /// value counts (final entry == number of items in the page).
    Flat { value_starts: PrefixSums },
    /// Nested page.  `row_starts` are cumulative row counts (final entry ==
    /// number of rows in the page); `has_trailer[i]` is set when chunk `i` ends
    /// with a partial list.
    Nested {
        row_starts: PrefixSums,
        has_trailer: BooleanBuffer,
        item_counts: ItemCounts,
    },
}

impl DeepSizeOf for RowMapping {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        match self {
            Self::UniformFlat { .. } => 0,
            Self::Flat { value_starts } => value_starts.deep_size_of_children(context),
            Self::Nested {
                row_starts,
                has_trailer,
                item_counts,
            } => {
                row_starts.deep_size_of_children(context)
                    + has_trailer.len().div_ceil(8)
                    + item_counts.deep_size_of_children(context)
            }
        }
    }
}

/// Compact per-page chunk index that avoids fully materializing the repetition
/// index into u64's to save RAM.  See the module docs for the layout.
#[derive(Debug)]
pub struct MiniBlockChunkIndex {
    base: u64,
    byte_starts: PrefixSums,
    rows: RowMapping,
}

impl MiniBlockChunkIndex {
    pub fn new(base: u64, byte_starts: PrefixSums, rows: RowMapping) -> Self {
        Self {
            base,
            byte_starts,
            rows,
        }
    }

    /// Number of chunks in the page.
    pub fn num_chunks(&self) -> usize {
        self.byte_starts.num_chunks()
    }

    /// Absolute byte range of chunk `i` within the file.
    pub fn byte_range(&self, i: usize) -> Range<u64> {
        let (start, end) = self.byte_starts.get_pair(i);
        (self.base + start)..(self.base + end)
    }

    /// Number of leaf values in chunk `i` (passed to the value decompressor).
    pub fn items_in_chunk(&self, i: usize) -> u64 {
        let num_chunks = self.num_chunks();
        match &self.rows {
            RowMapping::UniformFlat {
                values_per_chunk,
                last_chunk_values,
                ..
            } => {
                if i == num_chunks - 1 {
                    *last_chunk_values
                } else {
                    *values_per_chunk
                }
            }
            RowMapping::Flat { value_starts } => value_starts.delta(i),
            RowMapping::Nested { item_counts, .. } => item_counts.get(i, num_chunks),
        }
    }

    /// Index of the chunk that contains `row`.
    pub fn find_chunk(&self, row: u64) -> usize {
        match &self.rows {
            RowMapping::UniformFlat {
                values_per_chunk,
                num_chunks,
                ..
            } => ((row / values_per_chunk) as usize).min(num_chunks - 1),
            RowMapping::Flat { value_starts } => value_starts.find(row),
            RowMapping::Nested { row_starts, .. } => row_starts.find(row),
        }
    }

    /// First row (relative to the page) that begins in chunk `i`.
    pub fn first_row(&self, i: usize) -> u64 {
        match &self.rows {
            RowMapping::UniformFlat {
                values_per_chunk, ..
            } => i as u64 * values_per_chunk,
            RowMapping::Flat { value_starts } => value_starts.get(i),
            RowMapping::Nested { row_starts, .. } => row_starts.get(i),
        }
    }

    /// Number of rows that start in chunk `i`, including a trailer but not a
    /// preamble (the previous `starts_including_trailer`).
    pub fn rows_in_chunk(&self, i: usize) -> u64 {
        let num_chunks = self.num_chunks();
        match &self.rows {
            RowMapping::UniformFlat {
                values_per_chunk,
                last_chunk_values,
                ..
            } => {
                if i == num_chunks - 1 {
                    *last_chunk_values
                } else {
                    *values_per_chunk
                }
            }
            RowMapping::Flat { value_starts } => value_starts.delta(i),
            RowMapping::Nested { row_starts, .. } => row_starts.delta(i),
        }
    }

    /// Whether chunk `i` begins with a preamble (a continuation of the previous
    /// chunk's list).  Always false for flat pages; for nested pages this is the
    /// previous chunk's trailer.
    pub fn has_preamble(&self, i: usize) -> bool {
        match &self.rows {
            RowMapping::Nested { has_trailer, .. } => i > 0 && has_trailer.value(i - 1),
            _ => false,
        }
    }

    /// Whether chunk `i` ends with a trailer (a partial list continued in the
    /// next chunk).  Always false for flat pages.
    pub fn has_trailer(&self, i: usize) -> bool {
        match &self.rows {
            RowMapping::Nested { has_trailer, .. } => has_trailer.value(i),
            _ => false,
        }
    }

    /// Name of the active row-mapping variant, used to assert detection in tests.
    #[cfg(test)]
    pub fn row_mapping_debug(&self) -> &'static str {
        match &self.rows {
            RowMapping::UniformFlat { .. } => "uniform_flat",
            RowMapping::Flat { .. } => "flat",
            RowMapping::Nested { .. } => "nested",
        }
    }

    /// Builds a nested index from raw repetition-index bytes, using placeholder
    /// byte offsets and item counts.  Only the row axis is populated, which is
    /// all the scheduler exercises.
    #[cfg(test)]
    pub fn new_nested_for_test(rep_bytes: &[u8], stride: usize) -> Self {
        let (row_starts, has_trailer) = parse_nested_rep(rep_bytes, stride);
        let num_chunks = row_starts.num_chunks();
        let byte_starts = PrefixSums::from_deltas(
            std::iter::repeat_n(8u64, num_chunks),
            num_chunks,
            8 * num_chunks as u64,
        );
        Self {
            base: 0,
            byte_starts,
            rows: RowMapping::Nested {
                row_starts,
                has_trailer,
                item_counts: ItemCounts::Uniform {
                    values_per_chunk: 1,
                    last_chunk_values: 1,
                },
            },
        }
    }
}

impl DeepSizeOf for MiniBlockChunkIndex {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        self.byte_starts.deep_size_of_children(context) + self.rows.deep_size_of_children(context)
    }
}

/// Parses a mini-block repetition index into the compact nested row mapping.
///
/// Bytes are `u64`s in groups of `stride`; the first two are `ends` (lists
/// finishing in the chunk) and `partial` (leftover items).  Only cumulative row
/// starts and a trailer bit are kept: `has_preamble[i] = has_trailer[i-1]` and
/// `starts_including_trailer = ends + has_trailer - has_preamble`.
pub fn parse_nested_rep(rep_bytes: &[u8], stride: usize) -> (PrefixSums, BooleanBuffer) {
    // Read the two `u64`s per group straight from the little-endian bytes rather
    // than copying the buffer to reinterpret it.  The caller guarantees
    // `rep_bytes.len() % 8 == 0`, so the 8-byte windows stay in bounds.
    const WORD: usize = std::mem::size_of::<u64>();
    let read_word = |word_idx: usize| -> u64 {
        let byte = word_idx * WORD;
        u64::from_le_bytes(rep_bytes[byte..byte + WORD].try_into().unwrap())
    };
    let num_chunks = (rep_bytes.len() / WORD) / stride;

    let mut has_trailer_builder = BooleanBufferBuilder::new(num_chunks);
    // Accumulate the cumulative row starts in a single pass (entry 0 is 0, the
    // trailing entry is the total) so there is no separate deltas buffer.
    let mut row_starts = Vec::with_capacity(num_chunks + 1);
    row_starts.push(0u64);
    let mut acc = 0u64;
    let mut chunk_has_preamble = false;

    for i in 0..num_chunks {
        let base_idx = i * stride;
        let ends = read_word(base_idx);
        let partial = read_word(base_idx + 1);

        let has_trailer = partial > 0;
        let starts_including_trailer = ends + (has_trailer as u64) - (chunk_has_preamble as u64);

        has_trailer_builder.append(has_trailer);
        acc += starts_including_trailer;
        row_starts.push(acc);

        chunk_has_preamble = has_trailer;
    }

    (
        PrefixSums::from_prefix(row_starts),
        has_trailer_builder.finish(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::LanceBuffer;

    /// Reference decode: the previous per-block repetition index, used as an
    /// oracle for the compact `parse_nested_rep`.
    struct RefBlock {
        first_row: u64,
        starts_including_trailer: u64,
        has_preamble: bool,
        has_trailer: bool,
    }

    fn reference_decode(rep_bytes: &[u8], stride: usize) -> Vec<RefBlock> {
        let buffer = LanceBuffer::from(rep_bytes.to_vec());
        let u64_slice = buffer.borrow_to_typed_slice::<u64>();
        let n = u64_slice.len() / stride;
        let mut blocks = Vec::with_capacity(n);
        let mut chunk_has_preamble = false;
        let mut offset = 0u64;
        for i in 0..n {
            let base_idx = i * stride;
            let ends = u64_slice[base_idx];
            let partial = u64_slice[base_idx + 1];
            let has_trailer = partial > 0;
            let starts_including_trailer =
                ends + (has_trailer as u64) - (chunk_has_preamble as u64);
            blocks.push(RefBlock {
                first_row: offset,
                starts_including_trailer,
                has_preamble: chunk_has_preamble,
                has_trailer,
            });
            chunk_has_preamble = has_trailer;
            offset += starts_including_trailer;
        }
        blocks
    }

    fn rep_bytes(values: &[u64]) -> Vec<u8> {
        values.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    #[test]
    fn test_prefix_sums_u32() {
        let sums = PrefixSums::from_deltas([2u64, 3, 5].into_iter(), 3, 10);
        assert!(matches!(sums, PrefixSums::U32(_)));
        assert_eq!(sums.num_chunks(), 3);
        assert_eq!(sums.get(0), 0);
        assert_eq!(sums.get(1), 2);
        assert_eq!(sums.get(3), 10);
        assert_eq!(sums.delta(1), 3);
    }

    #[test]
    fn test_prefix_sums_u64_selected_by_total() {
        let big = u32::MAX as u64 + 1;
        let sums = PrefixSums::from_deltas([big].into_iter(), 1, big);
        assert!(matches!(sums, PrefixSums::U64(_)));
        assert_eq!(sums.get(1), big);
    }

    #[test]
    fn test_prefix_sums_find() {
        // Chunk starts: 0, 5, 5, 12 (a zero-width chunk creates a duplicate start)
        let sums = PrefixSums::from_deltas([5u64, 0, 7].into_iter(), 3, 12);
        // Inside the first chunk
        assert_eq!(sums.find(3), 0);
        // Exact match on a duplicated start returns the first such chunk
        assert_eq!(sums.find(5), 1);
        // Inside the last chunk
        assert_eq!(sums.find(11), 2);
        // Start of the first chunk
        assert_eq!(sums.find(0), 0);
    }

    #[test]
    fn test_parse_nested_rep_matches_reference() {
        let cases: Vec<Vec<u64>> = vec![
            vec![5, 2, 3, 0, 4, 7, 2, 0],
            vec![5, 2, 3, 3, 20, 0],
            vec![0, 5, 0, 3, 10, 0],
            vec![1, 0],
        ];
        for values in cases {
            let bytes = rep_bytes(&values);
            let reference = reference_decode(&bytes, 2);
            let (row_starts, has_trailer) = parse_nested_rep(&bytes, 2);
            assert_eq!(row_starts.num_chunks(), reference.len());
            for (i, block) in reference.iter().enumerate() {
                assert_eq!(row_starts.get(i), block.first_row, "first_row[{i}]");
                assert_eq!(
                    row_starts.delta(i),
                    block.starts_including_trailer,
                    "starts_including_trailer[{i}]"
                );
                assert_eq!(has_trailer.value(i), block.has_trailer, "has_trailer[{i}]");
                let derived_preamble = i > 0 && has_trailer.value(i - 1);
                assert_eq!(derived_preamble, block.has_preamble, "has_preamble[{i}]");
            }
        }
    }
}
