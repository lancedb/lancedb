// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::ops::RangeInclusive;
use std::sync::Arc;

use super::{RowIdSequence, U64Segment};
use lance_core::deepsize::DeepSizeOf;
use lance_core::utils::address::RowAddress;
use lance_core::utils::deletion::DeletionVector;
use lance_core::{Error, Result};
use rangemap::RangeInclusiveMap;

/// An index of row ids
///
/// This index is used to map row ids to their corresponding addresses. These
/// addresses correspond to physical positions in the dataset. See [RowAddress].
///
/// This structure only contains rows that physically exist. However, it may
/// map to addresses that have been tombstoned. A separate tombstone index is
/// used to track tombstoned rows.
// (Implementation)
// Disjoint ranges of row ids are stored as the keys of the map. The values are
// a pair of segments. The first segment is the row ids, and the second segment
// is the addresses.
#[derive(Debug)]
pub struct RowIdIndex(RangeInclusiveMap<u64, (U64Segment, U64Segment)>);

pub struct FragmentRowIdIndex {
    pub fragment_id: u32,
    pub row_id_sequence: Arc<RowIdSequence>,
    pub deletion_vector: Arc<DeletionVector>,
}

impl RowIdIndex {
    /// Create a new index from a list of fragment ids and their corresponding row id sequences.
    pub fn new(fragment_indices: &[FragmentRowIdIndex]) -> Result<Self> {
        let chunks = fragment_indices
            .iter()
            .flat_map(decompose_sequence)
            .collect::<Vec<_>>();

        let mut final_chunks = Vec::new();
        for processed_chunk in prep_index_chunks(chunks) {
            match processed_chunk {
                RawIndexChunk::NonOverlapping(chunk) => {
                    final_chunks.push(chunk);
                }
                RawIndexChunk::Overlapping(_range, overlapping_chunks) => {
                    // Intersecting row-id ranges don't imply intersecting id sets;
                    // sparse ids and deletion holes leave the union short of the span.
                    // The real invariant (no id in two fragments) is checked in the merge.
                    let merged_chunk = merge_overlapping_chunks(overlapping_chunks)?;
                    final_chunks.push(merged_chunk);
                }
            }
        }

        Ok(Self(RangeInclusiveMap::from_iter(final_chunks)))
    }

    /// Get the address for a given row id.
    ///
    /// Will return None if the row id does not exist in the index.
    pub fn get(&self, row_id: u64) -> Option<RowAddress> {
        let (row_id_segment, address_segment) = self.0.get(&row_id)?;
        let pos = row_id_segment.position(row_id)?;
        let address = address_segment.get(pos)?;
        Some(RowAddress::from(address))
    }

    /// Get addresses for many row ids in one pass over the index.
    ///
    /// Returns one entry per input id, in input order (`None` for missing).
    /// Sorts a working copy of the input internally so the chunk iterator
    /// is advanced at most once per chunk, amortizing the per-id tree walk
    /// from O(N · log F) to O(F + N).
    pub fn get_many(&self, row_ids: &[u64]) -> Vec<Option<RowAddress>> {
        let n = row_ids.len();
        let mut out = vec![None; n];
        if n == 0 {
            return out;
        }

        let mut sorted: Vec<(u64, usize)> = row_ids.iter().copied().zip(0..n).collect();
        sorted.sort_unstable_by_key(|&(id, _)| id);

        let mut chunks = self.0.iter().peekable();
        for (id, orig_idx) in sorted {
            // Advance past chunks that end before this id.
            while let Some((range, _)) = chunks.peek() {
                if *range.end() < id {
                    chunks.next();
                } else {
                    break;
                }
            }
            let Some((range, (row_id_seg, addr_seg))) = chunks.peek() else {
                break;
            };
            if id < *range.start() {
                continue; // falls in a gap between chunks
            }
            if let Some(pos) = row_id_seg.position(id)
                && let Some(addr) = addr_seg.get(pos)
            {
                out[orig_idx] = Some(RowAddress::from(addr));
            }
        }
        out
    }
}

impl DeepSizeOf for RowIdIndex {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.0
            .iter()
            .map(|(_, (row_id_segment, address_segment))| {
                (2 * std::mem::size_of::<u64>())
                    + std::mem::size_of::<(U64Segment, U64Segment)>()
                    + row_id_segment.deep_size_of_children(context)
                    + address_segment.deep_size_of_children(context)
            })
            .sum()
    }
}

fn decompose_sequence(
    frag_index: &FragmentRowIdIndex,
) -> Vec<(RangeInclusive<u64>, (U64Segment, U64Segment))> {
    let mut start_address: u64 = RowAddress::first_row(frag_index.fragment_id).into();
    let mut current_offset = 0u32;
    let no_deletions = frag_index.deletion_vector.is_empty();

    frag_index
        .row_id_sequence
        .0
        .iter()
        .filter_map(|segment| {
            let segment_len = segment.len();

            let result = if no_deletions {
                decompose_segment_no_deletions(segment, start_address)
            } else {
                decompose_segment_with_deletions(
                    segment,
                    start_address,
                    current_offset,
                    &frag_index.deletion_vector,
                )
            };

            current_offset += segment_len as u32;
            start_address += segment_len as u64;

            result
        })
        .collect()
}

/// Build an IndexChunk from a list of (row_id, address) pairs.
fn build_chunk_from_pairs(pairs: Vec<(u64, u64)>) -> Option<IndexChunk> {
    if pairs.is_empty() {
        return None;
    }
    let (row_ids, addresses): (Vec<u64>, Vec<u64>) = pairs.into_iter().unzip();
    let row_id_segment = U64Segment::from_iter(row_ids);
    let address_segment = U64Segment::from_iter(addresses);
    let coverage = row_id_segment.range()?;
    Some((coverage, (row_id_segment, address_segment)))
}

/// Fast path: no deletions. O(1) for Range segments.
fn decompose_segment_no_deletions(segment: &U64Segment, start_address: u64) -> Option<IndexChunk> {
    match segment {
        U64Segment::Range(range) if !range.is_empty() => {
            let len = range.end - range.start;
            let row_id_segment = U64Segment::Range(range.clone());
            let address_segment = U64Segment::Range(start_address..start_address + len);
            let coverage = range.start..=range.end - 1;
            Some((coverage, (row_id_segment, address_segment)))
        }
        _ if segment.is_empty() => None,
        _ => {
            // Non-Range segments: must iterate to build address mapping.
            let pairs: Vec<(u64, u64)> = segment
                .iter()
                .enumerate()
                .map(|(i, row_id)| (row_id, start_address + i as u64))
                .collect();
            build_chunk_from_pairs(pairs)
        }
    }
}

/// Slow path: has deletions, must check each row.
fn decompose_segment_with_deletions(
    segment: &U64Segment,
    start_address: u64,
    current_offset: u32,
    deletion_vector: &DeletionVector,
) -> Option<IndexChunk> {
    let pairs: Vec<(u64, u64)> = segment
        .iter()
        .enumerate()
        .filter_map(|(i, row_id)| {
            let row_offset = current_offset + i as u32;
            if !deletion_vector.contains(row_offset) {
                Some((row_id, start_address + i as u64))
            } else {
                None
            }
        })
        .collect();
    build_chunk_from_pairs(pairs)
}

type IndexChunk = (RangeInclusive<u64>, (U64Segment, U64Segment));

#[derive(Debug)]
enum RawIndexChunk {
    NonOverlapping(IndexChunk),
    Overlapping(RangeInclusive<u64>, Vec<IndexChunk>),
}

impl RawIndexChunk {
    fn range_end(&self) -> u64 {
        match self {
            Self::NonOverlapping((range, _)) => *range.end(),
            Self::Overlapping(range, _) => *range.end(),
        }
    }
}

/// Given a vector of index chunks, sort them and return an iterator of index chunks.
///
/// The iterator will yield chunks that are non-overlapping or a set of chunks
/// that are overlapping.
fn prep_index_chunks(mut chunks: Vec<IndexChunk>) -> impl Iterator<Item = RawIndexChunk> {
    chunks.sort_by_key(|(range, _)| u64::MAX - *range.start());

    let mut output = Vec::new();

    // Start assuming non-overlapping in first chunk.
    if let Some(first_chunk) = chunks.pop() {
        output.push(RawIndexChunk::NonOverlapping(first_chunk));
    } else {
        // Early return for empty.
        return output.into_iter();
    }

    let mut current_range = 0..=0;
    let mut current_overlap = Vec::new();
    while let Some(chunk) = chunks.pop() {
        debug_assert_eq!(
            current_overlap
                .iter()
                .map(|(range, _): &IndexChunk| *range.start())
                .min()
                .unwrap_or_default(),
            *current_range.start(),
        );
        debug_assert_eq!(
            current_overlap
                .iter()
                .map(|(range, _): &IndexChunk| *range.end())
                .max()
                .unwrap_or_default(),
            *current_range.end(),
        );

        if current_overlap.is_empty() {
            // We haven't found overlap yet.
            let last_chunk_end = output.last().unwrap().range_end();
            if *chunk.0.start() <= last_chunk_end {
                // We have found overlap.
                match output.pop().unwrap() {
                    RawIndexChunk::NonOverlapping(chunk) => {
                        current_overlap.push(chunk);
                    }
                    _ => unreachable!(),
                }
                current_overlap.push(chunk);

                let range_start = *current_overlap.first().unwrap().0.start();
                let range_end = *current_overlap
                    .last()
                    .unwrap()
                    .0
                    .end()
                    .max(current_overlap.first().unwrap().0.end());
                current_range = range_start..=range_end;
            } else {
                // We are still in non-overlapping space.
                output.push(RawIndexChunk::NonOverlapping(chunk));
            }
        } else {
            // We are making an overlap chunk
            if chunk.0.start() <= current_range.end() {
                // We are still in overlap.
                let range_end = *chunk.0.end().max(current_range.end());
                current_range = *current_range.start()..=range_end;

                current_overlap.push(chunk);
            } else {
                // We have exited overlap.
                output.push(RawIndexChunk::Overlapping(
                    std::mem::replace(&mut current_range, 0..=0),
                    std::mem::take(&mut current_overlap),
                ));
                output.push(RawIndexChunk::NonOverlapping(chunk));
            }
        }
    }
    debug_assert_eq!(
        current_overlap
            .iter()
            .map(|(range, _): &IndexChunk| *range.start())
            .min()
            .unwrap_or_default(),
        *current_range.start(),
    );
    debug_assert_eq!(
        current_overlap
            .iter()
            .map(|(range, _): &IndexChunk| *range.end())
            .max()
            .unwrap_or_default(),
        *current_range.end(),
    );

    if !current_overlap.is_empty() {
        output.push(RawIndexChunk::Overlapping(
            current_range.clone(),
            current_overlap,
        ));
    }

    output.into_iter()
}

fn merge_overlapping_chunks(overlapping_chunks: Vec<IndexChunk>) -> Result<IndexChunk> {
    let total_capacity = overlapping_chunks
        .iter()
        .map(|(_, (row_ids, _))| row_ids.len())
        .sum();
    let mut values = Vec::with_capacity(total_capacity);
    for (_, (row_ids, row_addrs)) in overlapping_chunks.iter() {
        values.extend(row_ids.iter().zip(row_addrs.iter()));
    }
    values.sort_by_key(|(row_id, _)| *row_id);
    // A duplicate row id here means two fragments claim the same live id: a
    // corrupt index, not a resolvable sparse-coverage case.
    if let Some(w) = values.windows(2).find(|w| w[0].0 == w[1].0) {
        return Err(Error::internal(format!(
            "row id index corrupt: stable row id {} is live in multiple fragments",
            w[0].0
        )));
    }
    let row_id_segment = U64Segment::from_iter(values.iter().map(|(row_id, _)| *row_id));
    let address_segment = U64Segment::from_iter(values.iter().map(|(_, row_addr)| *row_addr));

    let range = row_id_segment.range().unwrap();

    Ok((range, (row_id_segment, address_segment)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::{
        prelude::{Just, Strategy, any},
        prop_assert, prop_assert_eq,
    };

    #[test]
    fn test_new_index() {
        let fragment_indices = vec![
            FragmentRowIdIndex {
                fragment_id: 10,
                row_id_sequence: Arc::new(RowIdSequence(vec![
                    U64Segment::Range(0..10),
                    U64Segment::RangeWithHoles {
                        range: 10..17,
                        holes: vec![12, 15].into(),
                    },
                    U64Segment::SortedArray(vec![20, 25, 30].into()),
                ])),
                deletion_vector: Arc::new(DeletionVector::default()),
            },
            FragmentRowIdIndex {
                fragment_id: 20,
                row_id_sequence: Arc::new(RowIdSequence(vec![
                    U64Segment::RangeWithBitmap {
                        range: 17..20,
                        bitmap: [true, false, true].as_slice().into(),
                    },
                    U64Segment::Array(vec![40, 50, 60].into()),
                ])),
                deletion_vector: Arc::new(DeletionVector::default()),
            },
        ];

        let index = RowIdIndex::new(&fragment_indices).unwrap();

        // Check various queries.
        assert_eq!(index.get(0), Some(RowAddress::new_from_parts(10, 0)));
        assert_eq!(index.get(15), None);
        assert_eq!(index.get(16), Some(RowAddress::new_from_parts(10, 14)));
        assert_eq!(index.get(17), Some(RowAddress::new_from_parts(20, 0)));
        assert_eq!(index.get(25), Some(RowAddress::new_from_parts(10, 16)));
        assert_eq!(index.get(40), Some(RowAddress::new_from_parts(20, 2)));
        assert_eq!(index.get(60), Some(RowAddress::new_from_parts(20, 4)));
        assert_eq!(index.get(61), None);
    }

    #[test]
    fn test_new_index_overlap() {
        let fragment_indices = vec![
            FragmentRowIdIndex {
                fragment_id: 23,
                row_id_sequence: Arc::new(RowIdSequence(vec![U64Segment::SortedArray(
                    vec![3, 6, 9].into(),
                )])),
                deletion_vector: Arc::new(DeletionVector::default()),
            },
            FragmentRowIdIndex {
                fragment_id: 42,
                row_id_sequence: Arc::new(RowIdSequence(vec![U64Segment::SortedArray(
                    vec![2, 5, 8].into(),
                )])),
                deletion_vector: Arc::new(DeletionVector::default()),
            },
            FragmentRowIdIndex {
                fragment_id: 10,
                row_id_sequence: Arc::new(RowIdSequence(vec![U64Segment::SortedArray(
                    vec![1, 4, 7].into(),
                )])),
                deletion_vector: Arc::new(DeletionVector::default()),
            },
        ];

        let index = RowIdIndex::new(&fragment_indices).unwrap();

        // Check various queries.
        assert_eq!(index.get(1), Some(RowAddress::new_from_parts(10, 0)));
        assert_eq!(index.get(2), Some(RowAddress::new_from_parts(42, 0)));
        assert_eq!(index.get(3), Some(RowAddress::new_from_parts(23, 0)));
        assert_eq!(index.get(4), Some(RowAddress::new_from_parts(10, 1)));
        assert_eq!(index.get(5), Some(RowAddress::new_from_parts(42, 1)));
        assert_eq!(index.get(6), Some(RowAddress::new_from_parts(23, 1)));
        assert_eq!(index.get(7), Some(RowAddress::new_from_parts(10, 2)));
        assert_eq!(index.get(8), Some(RowAddress::new_from_parts(42, 2)));
        assert_eq!(index.get(9), Some(RowAddress::new_from_parts(23, 2)));
    }

    #[test]
    fn test_new_index_unsorted_row_ids() {
        // Test case with unsorted row ids within fragments
        let fragment_indices = vec![
            FragmentRowIdIndex {
                fragment_id: 10,
                row_id_sequence: Arc::new(RowIdSequence(vec![U64Segment::Array(
                    vec![9, 3, 6].into(), // Unsorted array
                )])),
                deletion_vector: Arc::new(DeletionVector::default()),
            },
            FragmentRowIdIndex {
                fragment_id: 20,
                row_id_sequence: Arc::new(RowIdSequence(vec![U64Segment::Array(
                    vec![8, 2, 5].into(), // Unsorted array
                )])),
                deletion_vector: Arc::new(DeletionVector::default()),
            },
            FragmentRowIdIndex {
                fragment_id: 30,
                row_id_sequence: Arc::new(RowIdSequence(vec![U64Segment::Array(
                    vec![7, 1, 4].into(), // Unsorted array
                )])),
                deletion_vector: Arc::new(DeletionVector::default()),
            },
        ];

        let index = RowIdIndex::new(&fragment_indices).unwrap();

        // Check that all row ids can be found regardless of their order in the segments
        assert_eq!(index.get(1), Some(RowAddress::new_from_parts(30, 1)));
        assert_eq!(index.get(2), Some(RowAddress::new_from_parts(20, 1)));
        assert_eq!(index.get(3), Some(RowAddress::new_from_parts(10, 1)));
        assert_eq!(index.get(4), Some(RowAddress::new_from_parts(30, 2)));
        assert_eq!(index.get(5), Some(RowAddress::new_from_parts(20, 2)));
        assert_eq!(index.get(6), Some(RowAddress::new_from_parts(10, 2)));
        assert_eq!(index.get(7), Some(RowAddress::new_from_parts(30, 0)));
        assert_eq!(index.get(8), Some(RowAddress::new_from_parts(20, 0)));
        assert_eq!(index.get(9), Some(RowAddress::new_from_parts(10, 0)));

        // Check that non-existent row ids return None
        assert_eq!(index.get(0), None);
        assert_eq!(index.get(10), None);
    }

    #[test]
    fn test_new_index_partial_overlap() {
        let fragment_indices = vec![
            FragmentRowIdIndex {
                fragment_id: 0,
                row_id_sequence: Arc::new(RowIdSequence(vec![U64Segment::RangeWithHoles {
                    range: 0..100,
                    holes: vec![50].into(),
                }])),
                deletion_vector: Arc::new(DeletionVector::default()),
            },
            FragmentRowIdIndex {
                fragment_id: 1,
                row_id_sequence: Arc::new(RowIdSequence(vec![U64Segment::Range(50..51)])),
                deletion_vector: Arc::new(DeletionVector::default()),
            },
        ];

        let index = RowIdIndex::new(&fragment_indices).unwrap();

        // Check various queries.
        assert_eq!(index.get(0), Some(RowAddress::new_from_parts(0, 0)));
        assert_eq!(index.get(49), Some(RowAddress::new_from_parts(0, 49)));
        assert_eq!(index.get(50), Some(RowAddress::new_from_parts(1, 0)));
        assert_eq!(index.get(51), Some(RowAddress::new_from_parts(0, 50)));
        assert_eq!(index.get(99), Some(RowAddress::new_from_parts(0, 98)));
    }

    #[test]
    fn test_overlapping_chunks_sparse_with_deletions() {
        // Interleaved (overlapping) id ranges plus a deletion that leaves a hole,
        // so the union doesn't tile the span. Every live id must still resolve.
        let fragment_indices = vec![
            FragmentRowIdIndex {
                fragment_id: 10,
                row_id_sequence: Arc::new(RowIdSequence(vec![U64Segment::SortedArray(
                    vec![1, 3, 5, 7, 9].into(),
                )])),
                deletion_vector: Arc::new(DeletionVector::default()),
            },
            FragmentRowIdIndex {
                fragment_id: 20,
                row_id_sequence: Arc::new(RowIdSequence(vec![U64Segment::SortedArray(
                    vec![0, 2, 4, 6, 8].into(),
                )])),
                // Delete offset 2 (id 4) -> a hole in the span.
                deletion_vector: Arc::new(DeletionVector::from_iter(vec![2])),
            },
        ];

        let index = RowIdIndex::new(&fragment_indices).unwrap();

        assert_eq!(index.get(0), Some(RowAddress::new_from_parts(20, 0)));
        assert_eq!(index.get(1), Some(RowAddress::new_from_parts(10, 0)));
        assert_eq!(index.get(2), Some(RowAddress::new_from_parts(20, 1)));
        assert_eq!(index.get(3), Some(RowAddress::new_from_parts(10, 1)));
        assert_eq!(index.get(4), None);
        // Surviving ids keep their original offsets (the hole is not compacted).
        assert_eq!(index.get(6), Some(RowAddress::new_from_parts(20, 3)));
        assert_eq!(index.get(8), Some(RowAddress::new_from_parts(20, 4)));
        assert_eq!(index.get(9), Some(RowAddress::new_from_parts(10, 4)));
    }

    #[test]
    fn test_index_with_deletion_vector() {
        let deletion_vector = DeletionVector::from_iter(vec![2, 3]);

        let fragment_indices = vec![FragmentRowIdIndex {
            fragment_id: 10,
            row_id_sequence: Arc::new(RowIdSequence(vec![U64Segment::Range(0..6)])),
            deletion_vector: Arc::new(deletion_vector),
        }];

        let index = RowIdIndex::new(&fragment_indices).unwrap();

        assert_eq!(index.get(0), Some(RowAddress::new_from_parts(10, 0)));
        assert_eq!(index.get(1), Some(RowAddress::new_from_parts(10, 1)));
        assert_eq!(index.get(4), Some(RowAddress::new_from_parts(10, 4)));
        assert_eq!(index.get(5), Some(RowAddress::new_from_parts(10, 5)));

        assert_eq!(index.get(2), None);
        assert_eq!(index.get(3), None);
    }

    #[test]
    fn test_empty_fragment_sequences() {
        let fragment_indices = vec![
            FragmentRowIdIndex {
                fragment_id: 10,
                row_id_sequence: Arc::new(RowIdSequence(vec![])),
                deletion_vector: Arc::new(DeletionVector::default()),
            },
            FragmentRowIdIndex {
                fragment_id: 20,
                row_id_sequence: Arc::new(RowIdSequence(vec![U64Segment::Range(5..8)])),
                deletion_vector: Arc::new(DeletionVector::default()),
            },
        ];

        let index = RowIdIndex::new(&fragment_indices).unwrap();

        assert_eq!(index.get(5), Some(RowAddress::new_from_parts(20, 0)));
        assert_eq!(index.get(7), Some(RowAddress::new_from_parts(20, 2)));
        assert_eq!(index.get(4), None);
    }

    #[test]
    fn test_completely_empty_index() {
        let fragment_indices = vec![];
        let index = RowIdIndex::new(&fragment_indices).unwrap();

        assert_eq!(index.get(0), None);
        assert_eq!(index.get(100), None);
    }

    #[test]
    fn test_non_overlapping_ranges() {
        let fragment_indices = vec![
            FragmentRowIdIndex {
                fragment_id: 10,
                row_id_sequence: Arc::new(RowIdSequence(vec![U64Segment::Range(0..5)])),
                deletion_vector: Arc::new(DeletionVector::default()),
            },
            FragmentRowIdIndex {
                fragment_id: 20,
                row_id_sequence: Arc::new(RowIdSequence(vec![U64Segment::Range(5..10)])),
                deletion_vector: Arc::new(DeletionVector::default()),
            },
            FragmentRowIdIndex {
                fragment_id: 30,
                row_id_sequence: Arc::new(RowIdSequence(vec![U64Segment::Range(10..15)])),
                deletion_vector: Arc::new(DeletionVector::default()),
            },
        ];

        let index = RowIdIndex::new(&fragment_indices).unwrap();

        assert_eq!(index.get(0), Some(RowAddress::new_from_parts(10, 0)));
        assert_eq!(index.get(4), Some(RowAddress::new_from_parts(10, 4)));
        assert_eq!(index.get(5), Some(RowAddress::new_from_parts(20, 0)));
        assert_eq!(index.get(9), Some(RowAddress::new_from_parts(20, 4)));
        assert_eq!(index.get(10), Some(RowAddress::new_from_parts(30, 0)));
        assert_eq!(index.get(14), Some(RowAddress::new_from_parts(30, 4)));
    }

    fn arbitrary_row_ids(
        num_fragments_range: std::ops::Range<usize>,
        frag_size_range: std::ops::Range<usize>,
    ) -> impl Strategy<Value = Vec<(u32, Arc<RowIdSequence>)>> {
        let fragment_sizes = proptest::collection::vec(frag_size_range, num_fragments_range);
        fragment_sizes.prop_flat_map(|fragment_sizes| {
            let num_rows = fragment_sizes.iter().sum::<usize>() as u64;
            let row_ids = 0..num_rows;
            let row_ids = row_ids.collect::<Vec<_>>();
            let row_ids_shuffled = proptest::strategy::Just(row_ids).prop_shuffle();
            row_ids_shuffled.prop_map(move |row_ids| {
                let mut sequences = Vec::with_capacity(fragment_sizes.len());
                let mut i = 0;
                for size in &fragment_sizes {
                    let end = i + size;
                    let sequence =
                        RowIdSequence(vec![U64Segment::from_slice(row_ids[i..end].into())]);
                    sequences.push((i as u32, Arc::new(sequence)));
                    i = end;
                }
                sequences
            })
        })
    }

    fn arbitrary_row_ids_with_deletions(
        num_fragments_range: std::ops::Range<usize>,
        frag_size_range: std::ops::Range<usize>,
    ) -> impl Strategy<Value = Vec<(u32, Arc<RowIdSequence>, Arc<DeletionVector>)>> {
        arbitrary_row_ids(num_fragments_range, frag_size_range)
            .prop_flat_map(|row_ids| {
                let num_rows = row_ids
                    .iter()
                    .map(|(_, sequence)| sequence.len() as usize)
                    .sum::<usize>();
                (
                    Just(row_ids),
                    proptest::collection::vec(any::<bool>(), num_rows),
                )
            })
            .prop_map(|(row_ids, deleted_rows)| {
                let mut deleted_rows = deleted_rows.into_iter();
                row_ids
                    .into_iter()
                    .map(|(fragment_id, sequence)| {
                        let mut deletion_bitmap = roaring::RoaringBitmap::new();
                        for offset in 0..sequence.len() as u32 {
                            if deleted_rows.next().unwrap() {
                                deletion_bitmap.insert(offset);
                            }
                        }
                        (
                            fragment_id,
                            sequence,
                            Arc::new(DeletionVector::Bitmap(deletion_bitmap)),
                        )
                    })
                    .collect()
            })
    }

    #[test]
    fn test_large_range_segments_no_deletions() {
        // Simulates a real-world scenario: many fragments with large Range segments
        // and no deletions. Before optimization, this would iterate over all rows
        // (O(total_rows)). After optimization, it's O(num_fragments).
        let rows_per_fragment = 250_000u64;
        let num_fragments = 100u32;
        let mut offset = 0u64;

        let fragment_indices: Vec<FragmentRowIdIndex> = (0..num_fragments)
            .map(|frag_id| {
                let start = offset;
                offset += rows_per_fragment;
                FragmentRowIdIndex {
                    fragment_id: frag_id,
                    row_id_sequence: Arc::new(RowIdSequence(vec![U64Segment::Range(
                        start..start + rows_per_fragment,
                    )])),
                    deletion_vector: Arc::new(DeletionVector::default()),
                }
            })
            .collect();

        let start = std::time::Instant::now();
        let index = RowIdIndex::new(&fragment_indices).unwrap();
        let elapsed = start.elapsed();

        // Verify correctness at boundaries
        assert_eq!(index.get(0), Some(RowAddress::new_from_parts(0, 0)));
        assert_eq!(
            index.get(rows_per_fragment - 1),
            Some(RowAddress::new_from_parts(0, rows_per_fragment as u32 - 1))
        );
        assert_eq!(
            index.get(rows_per_fragment),
            Some(RowAddress::new_from_parts(1, 0))
        );
        let last_row = num_fragments as u64 * rows_per_fragment - 1;
        assert_eq!(
            index.get(last_row),
            Some(RowAddress::new_from_parts(
                num_fragments - 1,
                rows_per_fragment as u32 - 1
            ))
        );
        assert_eq!(index.get(last_row + 1), None);

        // With the optimization, building an index for 25M rows across 100 fragments
        // should complete in well under 1 second (typically < 1ms).
        assert!(
            elapsed.as_secs() < 1,
            "Index build took {:?} for {} fragments x {} rows = {} total rows. \
             This suggests the O(rows) -> O(fragments) optimization is not working.",
            elapsed,
            num_fragments,
            rows_per_fragment,
            num_fragments as u64 * rows_per_fragment,
        );
    }

    #[test]
    fn test_large_range_segments_with_deletions() {
        let rows_per_fragment = 1_000u64;
        let num_fragments = 10u32;
        let mut offset = 0u64;

        let fragment_indices: Vec<FragmentRowIdIndex> = (0..num_fragments)
            .map(|frag_id| {
                let start = offset;
                offset += rows_per_fragment;

                // Delete every 3rd row (offsets 0, 3, 6, ...) within each fragment.
                let mut deleted = roaring::RoaringBitmap::new();
                for i in (0..rows_per_fragment as u32).step_by(3) {
                    deleted.insert(i);
                }

                FragmentRowIdIndex {
                    fragment_id: frag_id,
                    row_id_sequence: Arc::new(RowIdSequence(vec![U64Segment::Range(
                        start..start + rows_per_fragment,
                    )])),
                    deletion_vector: Arc::new(DeletionVector::Bitmap(deleted)),
                }
            })
            .collect();

        let index = RowIdIndex::new(&fragment_indices).unwrap();

        // Deleted rows (offset 0, 3, 6, ...) should not be found.
        // Row ID 0 has offset 0 in fragment 0 -> deleted.
        assert_eq!(index.get(0), None);
        // Row ID 3 has offset 3 in fragment 0 -> deleted.
        assert_eq!(index.get(3), None);

        // Non-deleted rows should resolve correctly.
        // Row ID 1 has offset 1 in fragment 0 -> address (frag=0, row=1).
        assert_eq!(index.get(1), Some(RowAddress::new_from_parts(0, 1)));
        // Row ID 2 has offset 2 in fragment 0 -> address (frag=0, row=2).
        assert_eq!(index.get(2), Some(RowAddress::new_from_parts(0, 2)));
        // Row ID 4 has offset 4 in fragment 0 -> address (frag=0, row=4).
        assert_eq!(index.get(4), Some(RowAddress::new_from_parts(0, 4)));

        // Check second fragment: row IDs start at 1000.
        // Row ID 1000 has offset 0 in fragment 1 -> deleted.
        assert_eq!(index.get(rows_per_fragment), None);
        // Row ID 1001 has offset 1 in fragment 1 -> address (frag=1, row=1).
        assert_eq!(
            index.get(rows_per_fragment + 1),
            Some(RowAddress::new_from_parts(1, 1))
        );

        // Last fragment, last non-deleted row.
        // Row ID 9999 has offset 999 in fragment 9 -> 999 % 3 == 0 -> deleted.
        let last_row = num_fragments as u64 * rows_per_fragment - 1;
        assert_eq!(index.get(last_row), None);
        // Row ID 9998 has offset 998 -> 998 % 3 == 2 -> not deleted.
        assert_eq!(
            index.get(last_row - 1),
            Some(RowAddress::new_from_parts(num_fragments - 1, 998))
        );

        // Out of range.
        assert_eq!(index.get(last_row + 1), None);
    }

    proptest::proptest! {
        #[test]
        fn test_new_index_robustness(
            row_ids in arbitrary_row_ids_with_deletions(0..5, 0..32)
        ) {
            let fragment_indices: Vec<FragmentRowIdIndex> = row_ids
                .iter()
                .map(|(frag_id, sequence, deletion_vector)| FragmentRowIdIndex {
                    fragment_id: *frag_id,
                    row_id_sequence: sequence.clone(),
                    deletion_vector: deletion_vector.clone(),
                })
                .collect();

            let index = RowIdIndex::new(&fragment_indices).unwrap();
            for (frag_id, sequence, deletion_vector) in row_ids.iter() {
                for (local_offset, row_id) in sequence.iter().enumerate() {
                    let expected = if deletion_vector.contains(local_offset as u32) {
                        None
                    } else {
                        Some(RowAddress::new_from_parts(*frag_id, local_offset as u32))
                    };
                    prop_assert_eq!(
                        index.get(row_id),
                        expected,
                        "Row id {} in sequence {:?} not found in index {:?}",
                        row_id,
                        sequence,
                        index
                    );
                }
            }
        }

        #[test]
        fn test_new_index_moved_row_id(
            row_id in any::<u64>(),
            source_fragment in 0u32..1024,
            fragment_delta in 1u32..1024,
        ) {
            let target_fragment = source_fragment + fragment_delta;
            let fragment_indices = [
                FragmentRowIdIndex {
                    fragment_id: source_fragment,
                    row_id_sequence: Arc::new(RowIdSequence::from(&[row_id][..])),
                    deletion_vector: Arc::new(DeletionVector::Bitmap(
                        roaring::RoaringBitmap::from_iter([0]),
                    )),
                },
                FragmentRowIdIndex {
                    fragment_id: target_fragment,
                    row_id_sequence: Arc::new(RowIdSequence::from(&[row_id][..])),
                    deletion_vector: Arc::new(DeletionVector::default()),
                },
            ];

            let index = RowIdIndex::new(&fragment_indices).unwrap();
            prop_assert_eq!(
                index.get(row_id),
                Some(RowAddress::new_from_parts(target_fragment, 0))
            );
        }

        #[test]
        fn test_new_index_rejects_duplicate_live_row_id(
            row_id in any::<u64>(),
            first_fragment in 0u32..1024,
            fragment_delta in 1u32..1024,
        ) {
            let second_fragment = first_fragment + fragment_delta;
            let fragment_indices = [
                FragmentRowIdIndex {
                    fragment_id: first_fragment,
                    row_id_sequence: Arc::new(RowIdSequence::from(&[row_id][..])),
                    deletion_vector: Arc::new(DeletionVector::default()),
                },
                FragmentRowIdIndex {
                    fragment_id: second_fragment,
                    row_id_sequence: Arc::new(RowIdSequence::from(&[row_id][..])),
                    deletion_vector: Arc::new(DeletionVector::default()),
                },
            ];

            let error = RowIdIndex::new(&fragment_indices).unwrap_err();
            let is_internal = matches!(&error, Error::Internal { .. });
            let expected_message =
                format!("stable row id {row_id} is live in multiple fragments");
            let error_message = error.to_string();
            prop_assert!(is_internal);
            prop_assert!(error_message.contains(&expected_message));
        }
    }
}
