// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Shared Zone Training Utilities
//!
//! This module provides common infrastructure for building zone-based scalar indexes.
//! It handles chunking data streams into fixed-size zones while respecting fragment
//! boundaries and computing zone bounds that remain valid after row deletions.

use arrow_array::{ArrayRef, UInt64Array};
use datafusion::execution::SendableRecordBatchStream;
use futures::TryStreamExt;
use lance_core::error::Error;
use lance_core::utils::address::RowAddress;
use lance_core::{ROW_ADDR, Result};
use lance_datafusion::chunker::chunk_concat_stream;
use lance_select::RowAddrTreeMap;

//
// Example: Suppose we have two fragments, each with 4 rows.
// Fragment 0: start = 0, length = 4  // covers rows 0, 1, 2, 3 in fragment 0
// The row addresses for fragment 0 are: 0, 1, 2, 3
// Fragment 1: start = 0, length = 4  // covers rows 0, 1, 2, 3 in fragment 1
// The row addresses for fragment 1 are: (1<<32), (1<<32)+1, (1<<32)+2, (1<<32)+3
//
// Deletion is 0 index based. We delete the 0th and 1st row in fragment 0,
// and the 1st and 2nd row in fragment 1,
// Fragment 0: start = 2, length = 2 // covers rows 2, 3 in fragment 0
// The row addresses for fragment 0 are: 2, 3
// Fragment 1: start = 0, length = 4  // covers rows 0, 3 in fragment 1
// The row addresses for fragment 1 are: (1<<32), (1<<32)+3
/// Zone bound within a fragment
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZoneBound {
    pub fragment_id: u64,
    // start is start row of the zone in the fragment, also known
    // as the local offset. To get the actual first row address,
    // use `(fragment_id << 32) | start`.
    pub start: u64,
    // length is the span of row offsets between the first and last row in the zone,
    // calculated as (last_row_offset - first_row_offset + 1). It is not the count
    // of physical rows, since deletions may create gaps within the span.
    pub length: usize,
}

/// Index-specific logic used while building zones.
pub trait ZoneProcessor {
    type ZoneStatistics;

    /// Process a slice of values that belongs to the current zone.
    fn process_chunk(&mut self, values: &ArrayRef) -> Result<()>;

    /// Emit statistics when the zone is full or the fragment changes.
    fn finish_zone(&mut self, bound: ZoneBound) -> Result<Self::ZoneStatistics>;

    /// Reset state so the processor can handle the next zone.
    fn reset(&mut self) -> Result<()>;
}

/// Trainer that handles chunking, fragment boundaries, and zone flushing.
#[derive(Debug)]
pub struct ZoneTrainer<P> {
    processor: P,
    zone_capacity: u64,
}

impl<P> ZoneTrainer<P>
where
    P: ZoneProcessor,
{
    /// Create a new trainer that buffers at most `zone_capacity` rows per zone.
    pub fn new(processor: P, zone_capacity: u64) -> Result<Self> {
        if zone_capacity == 0 {
            return Err(Error::invalid_input(
                "zone capacity must be greater than zero",
            ));
        }
        Ok(Self {
            processor,
            zone_capacity,
        })
    }

    /// Consume the `_rowaddr`-annotated stream, split it into zones, and let the
    /// processor compute zone statistics.
    ///
    /// The caller must provide record batches where the first column is the
    /// value array that the zone processor understands, and the schema includes
    /// the `_rowaddr` column with physical row addresses. Future zone-based
    /// indexes should maintain this ordering or extend the trainer to accept an
    /// explicit column index.
    pub async fn train(
        mut self,
        stream: SendableRecordBatchStream,
    ) -> Result<(Vec<P::ZoneStatistics>, RowAddrTreeMap)> {
        let zone_size = usize::try_from(self.zone_capacity).map_err(|_| {
            Error::invalid_input("zone capacity does not fit into usize on this platform")
        })?;

        let mut batches = chunk_concat_stream(stream, zone_size);
        let mut zones = Vec::new();
        let mut null_rows = RowAddrTreeMap::new();
        let mut current_fragment_id: Option<u64> = None;
        let mut current_zone_len: usize = 0;
        let mut zone_start_offset: Option<u64> = None;
        let mut zone_end_offset: Option<u64> = None;

        self.processor.reset()?;

        while let Some(batch) = batches.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }

            let values = batch.column(0);
            let row_addr_col = batch
                .column_by_name(ROW_ADDR)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            let mut batch_offset = 0usize;
            while batch_offset < batch.num_rows() {
                let row_addr = row_addr_col.value(batch_offset);
                let fragment_id = row_addr >> 32;

                // Zones cannot span fragments; flush current zone (if non-empty) at boundary
                match current_fragment_id {
                    Some(current) if current != fragment_id => {
                        if current_zone_len > 0 {
                            Self::flush_zone(
                                &mut self.processor,
                                &mut zones,
                                current,
                                &mut current_zone_len,
                                &mut zone_start_offset,
                                &mut zone_end_offset,
                            )?;
                        }
                        current_fragment_id = Some(fragment_id);
                    }
                    None => {
                        current_fragment_id = Some(fragment_id);
                    }
                    _ => {}
                }

                // Count consecutive rows in the same fragment
                let run_len = (batch_offset..batch.num_rows())
                    .take_while(|&idx| (row_addr_col.value(idx) >> 32) == fragment_id)
                    .count();
                let capacity = zone_size - current_zone_len;
                let take = run_len.min(capacity);

                self.processor
                    .process_chunk(&values.slice(batch_offset, take))?;

                // Record exact row addresses for null values in this chunk.
                let chunk = values.slice(batch_offset, take);
                if chunk.null_count() > 0 {
                    for i in 0..take {
                        if values.is_null(batch_offset + i) {
                            null_rows.insert(row_addr_col.value(batch_offset + i));
                        }
                    }
                }

                // Track the first and last row offsets to handle non-contiguous offsets
                // after deletions. Zone length (offset span) is computed as (last - first + 1),
                // not the actual row count.
                let first_offset =
                    RowAddress::new_from_u64(row_addr_col.value(batch_offset)).row_offset() as u64;
                let last_offset =
                    RowAddress::new_from_u64(row_addr_col.value(batch_offset + take - 1))
                        .row_offset() as u64;

                if zone_start_offset.is_none() {
                    zone_start_offset = Some(first_offset);
                }
                zone_end_offset = Some(last_offset);

                current_zone_len += take;
                batch_offset += take;

                if current_zone_len == zone_size {
                    Self::flush_zone(
                        &mut self.processor,
                        &mut zones,
                        fragment_id,
                        &mut current_zone_len,
                        &mut zone_start_offset,
                        &mut zone_end_offset,
                    )?;
                }
            }
        }

        if current_zone_len > 0 {
            if let Some(fragment_id) = current_fragment_id {
                Self::flush_zone(
                    &mut self.processor,
                    &mut zones,
                    fragment_id,
                    &mut current_zone_len,
                    &mut zone_start_offset,
                    &mut zone_end_offset,
                )?;
            } else {
                self.processor.reset()?;
            }
        }

        Ok((zones, null_rows))
    }

    /// Flushes a non-empty zone and resets the processor state.
    fn flush_zone(
        processor: &mut P,
        zones: &mut Vec<P::ZoneStatistics>,
        fragment_id: u64,
        current_zone_len: &mut usize,
        zone_start_offset: &mut Option<u64>,
        zone_end_offset: &mut Option<u64>,
    ) -> Result<()> {
        let start = zone_start_offset.unwrap_or(0);
        let inferred_end =
            zone_end_offset.unwrap_or_else(|| start + (*current_zone_len as u64).saturating_sub(1));
        if inferred_end < start {
            return Err(Error::invalid_input("zone row offsets are out of order"));
        }
        let bound = ZoneBound {
            fragment_id,
            start,
            length: (inferred_end - start + 1) as usize,
        };
        let stats = processor.finish_zone(bound)?;
        zones.push(stats);
        *current_zone_len = 0;
        *zone_start_offset = None;
        *zone_end_offset = None;
        processor.reset()?;
        Ok(())
    }
}

/// Shared search helper that loops over zones, records metrics, and
/// collects row address ranges for matching zones. The result is always
/// returned as `SearchResult::AtMost` because zone-level pruning can only
/// guarantee a superset of the true matches.
pub fn search_zones<T, F>(
    zones: &[T],
    metrics: &dyn crate::metrics::MetricsCollector,
    mut zone_matches: F,
) -> Result<crate::scalar::SearchResult>
where
    T: AsRef<ZoneBound>,
    F: FnMut(&T) -> Result<bool>,
{
    metrics.record_comparisons(zones.len());
    let mut row_addr_tree_map = RowAddrTreeMap::new();

    // For each zone, check if it might contain the queried value
    for zone in zones {
        if zone_matches(zone)? {
            let bound = zone.as_ref();
            // Calculate the range of row addresses for this zone
            let zone_start_addr = (bound.fragment_id << 32) + bound.start;
            let zone_end_addr = zone_start_addr + bound.length as u64;

            // Add all row addresses in this zone to the result
            row_addr_tree_map.insert_range(zone_start_addr..zone_end_addr);
        }
    }

    Ok(crate::scalar::SearchResult::at_most(row_addr_tree_map))
}

/// Helper that retrains zones from `stream` and appends them to the existing
/// statistics. Returns the combined zone list and the null-row bitmap for the
/// new data only — callers are responsible for merging with any existing bitmap.
pub async fn rebuild_zones<P>(
    existing: &[P::ZoneStatistics],
    trainer: ZoneTrainer<P>,
    stream: SendableRecordBatchStream,
) -> Result<(Vec<P::ZoneStatistics>, RowAddrTreeMap)>
where
    P: ZoneProcessor,
    P::ZoneStatistics: Clone,
{
    let mut combined = existing.to_vec();
    let (mut new_zones, null_rows) = trainer.train(stream).await?;
    combined.append(&mut new_zones);
    Ok((combined, null_rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{metrics::LocalMetricsCollector, scalar::SearchResult};
    use arrow_array::{ArrayRef, Int32Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream;
    use lance_core::ROW_ADDR;
    use std::sync::Arc;

    #[derive(Debug, Clone, PartialEq)]
    struct MockStats {
        sum: i32,
        bound: ZoneBound,
    }

    #[derive(Debug)]
    struct MockProcessor {
        current_sum: i32,
    }

    impl MockProcessor {
        fn new() -> Self {
            Self { current_sum: 0 }
        }
    }

    impl ZoneProcessor for MockProcessor {
        type ZoneStatistics = MockStats;

        fn process_chunk(&mut self, values: &ArrayRef) -> Result<()> {
            let arr = values.as_any().downcast_ref::<Int32Array>().unwrap();
            self.current_sum += arr.iter().map(|v| v.unwrap_or(0)).sum::<i32>();
            Ok(())
        }

        fn finish_zone(&mut self, bound: ZoneBound) -> Result<Self::ZoneStatistics> {
            Ok(MockStats {
                sum: self.current_sum,
                bound,
            })
        }

        fn reset(&mut self) -> Result<()> {
            self.current_sum = 0;
            Ok(())
        }
    }

    fn batch(values: Vec<i32>, fragments: Vec<u64>, offsets: Vec<u64>) -> RecordBatch {
        let val_array = Arc::new(Int32Array::from(values));
        let row_addrs: Vec<u64> = fragments
            .into_iter()
            .zip(offsets)
            .map(|(frag, off)| (frag << 32) | off)
            .collect();
        let addr_array = Arc::new(UInt64Array::from(row_addrs));
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
            Field::new(ROW_ADDR, DataType::UInt64, false),
        ]));
        RecordBatch::try_new(schema, vec![val_array, addr_array]).unwrap()
    }

    #[tokio::test]
    async fn splits_single_fragment() {
        // Single fragment with 10 rows, zone capacity = 4.
        // Expect three zones with lengths [4, 4, 2].
        let values = vec![1; 10];
        let offsets: Vec<u64> = (0..10).collect();
        let batch = batch(values, vec![0; 10], offsets);
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            stream::once(async { Ok(batch) }),
        ));

        let processor = MockProcessor::new();
        let trainer = ZoneTrainer::new(processor, 4).unwrap();
        let (stats, _) = trainer.train(stream).await.unwrap();

        // Three zones: offsets [0..=3], [4..=7], [8..=9]
        assert_eq!(stats.len(), 3);
        assert_eq!(stats[0].bound.start, 0);
        assert_eq!(stats[0].bound.length, 4);
        assert_eq!(stats[1].bound.start, 4);
        assert_eq!(stats[1].bound.length, 4);
        assert_eq!(stats[2].bound.start, 8);
        assert_eq!(stats[2].bound.length, 2); // Last zone has only 2 rows
        assert_eq!(
            stats.iter().map(|s| s.sum).collect::<Vec<_>>(),
            vec![4, 4, 2]
        );
    }

    #[tokio::test]
    async fn flushes_on_fragment_boundary() {
        // Two fragments back to back, capacity is large enough that only fragment
        // boundaries cause zone flushes. Expect two zones (one per fragment).
        let values = vec![1, 1, 1, 2, 2, 2];
        let fragments = vec![0, 0, 0, 1, 1, 1];
        let offsets = vec![0, 1, 2, 0, 1, 2];
        let batch = batch(values, fragments, offsets);
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            stream::once(async { Ok(batch) }),
        ));

        let processor = MockProcessor::new();
        let trainer = ZoneTrainer::new(processor, 10).unwrap();
        let (stats, _) = trainer.train(stream).await.unwrap();

        // Two zones, one per fragment (capacity=10 is large enough)
        assert_eq!(stats.len(), 2);
        assert_eq!(stats[0].bound.fragment_id, 0);
        assert_eq!(stats[0].bound.length, 3); // Fragment 0: offsets 0,1,2 → length = 2-0+1 = 3
        assert_eq!(stats[1].bound.fragment_id, 1);
        assert_eq!(stats[1].bound.length, 3); // Fragment 1: offsets 0,1,2 → length = 2-0+1 = 3
    }

    #[tokio::test]
    async fn errors_on_out_of_order_offsets() {
        // Offsets go backwards (5 -> 3). Trainer should treat this as invalid input
        // rather than silently emitting a zero-length zone.
        let values = vec![1, 2, 3];
        let fragments = vec![0, 0, 0];
        let offsets = vec![5, 3, 4];
        let batch = batch(values, fragments, offsets);
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            stream::once(async { Ok(batch) }),
        ));

        let processor = MockProcessor::new();
        let trainer = ZoneTrainer::new(processor, 10).unwrap();
        let err = trainer.train(stream).await.unwrap_err();
        assert!(
            format!("{}", err).contains("zone row offsets are out of order"),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn handles_empty_batches() {
        // Empty batches in the stream should be properly skipped without affecting zones.
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
            Field::new(ROW_ADDR, DataType::UInt64, false),
        ]));

        let empty_batch = RecordBatch::new_empty(schema.clone());
        let valid_batch = batch(vec![1, 2, 3], vec![0, 0, 0], vec![0, 1, 2]);

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![
                Ok(empty_batch.clone()),
                Ok(valid_batch),
                Ok(empty_batch),
            ]),
        ));

        let processor = MockProcessor::new();
        let trainer = ZoneTrainer::new(processor, 10).unwrap();
        let (stats, _) = trainer.train(stream).await.unwrap();

        // One zone containing the 3 valid rows (empty batches skipped)
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].sum, 6);
        assert_eq!(stats[0].bound.fragment_id, 0);
        assert_eq!(stats[0].bound.length, 3);
    }

    #[tokio::test]
    async fn handles_zone_capacity_one() {
        // Each row becomes its own zone when capacity is 1.
        let values = vec![10, 20, 30];
        let offsets = vec![0, 1, 2];
        let batch = batch(values.clone(), vec![0, 0, 0], offsets.clone());
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            stream::once(async { Ok(batch) }),
        ));

        let processor = MockProcessor::new();
        let trainer = ZoneTrainer::new(processor, 1).unwrap();
        let (stats, _) = trainer.train(stream).await.unwrap();

        // Three zones, one per row (capacity=1)
        assert_eq!(stats.len(), 3);
        for (i, stat) in stats.iter().enumerate() {
            assert_eq!(stat.bound.fragment_id, 0);
            assert_eq!(stat.bound.start, offsets[i]);
            assert_eq!(stat.bound.length, 1); // Each zone contains exactly one row
            assert_eq!(stat.sum, values[i]);
        }
    }

    #[tokio::test]
    async fn handles_large_capacity() {
        // When capacity >> data size, all data fits in one zone.
        let values = vec![1; 100];
        let offsets: Vec<u64> = (0..100).collect();
        let batch = batch(values, vec![0; 100], offsets);
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            stream::once(async { Ok(batch) }),
        ));

        let processor = MockProcessor::new();
        let trainer = ZoneTrainer::new(processor, 10000).unwrap();
        let (stats, _) = trainer.train(stream).await.unwrap();

        // One zone containing all 100 rows (capacity is large enough)
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].sum, 100);
        assert_eq!(stats[0].bound.start, 0);
        assert_eq!(stats[0].bound.length, 100);
    }

    #[tokio::test]
    async fn rejects_zero_capacity() {
        let processor = MockProcessor::new();
        let result = ZoneTrainer::new(processor, 0);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("zone capacity must be greater than zero")
        );
    }

    #[tokio::test]
    async fn handles_multiple_batches_same_fragment() {
        // Multiple batches from the same fragment should be properly accumulated into zones.
        let b1 = batch(vec![1, 1], vec![0, 0], vec![0, 1]);
        let b2 = batch(vec![1, 1], vec![0, 0], vec![2, 3]);
        let b3 = batch(vec![1, 1], vec![0, 0], vec![4, 5]);

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            b1.schema(),
            stream::iter(vec![Ok(b1), Ok(b2), Ok(b3)]),
        ));

        let processor = MockProcessor::new();
        let trainer = ZoneTrainer::new(processor, 4).unwrap();
        let (stats, _) = trainer.train(stream).await.unwrap();

        // Two zones: first 4 rows, then remaining 2 rows
        assert_eq!(stats.len(), 2);
        // First zone: offsets [0..=3]
        assert_eq!(stats[0].bound.fragment_id, 0);
        assert_eq!(stats[0].bound.start, 0);
        assert_eq!(stats[0].bound.length, 4);
        assert_eq!(stats[0].sum, 4);
        // Second zone: offsets [4..=5]
        assert_eq!(stats[1].bound.fragment_id, 0);
        assert_eq!(stats[1].bound.start, 4);
        assert_eq!(stats[1].bound.length, 2);
        assert_eq!(stats[1].sum, 2);
    }

    #[tokio::test]
    async fn handles_multi_batch_with_fragment_change() {
        // Complex scenario: multiple batches with fragment changes mid-batch.
        // This tests that zones flush correctly at fragment boundaries.
        let b1 = batch(vec![1, 1], vec![0, 0], vec![0, 1]);
        // b2 has fragment change: starts with frag 0, switches to frag 1
        let b2 = batch(vec![1, 1, 2, 2], vec![0, 0, 1, 1], vec![2, 3, 0, 1]);

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            b1.schema(),
            stream::iter(vec![Ok(b1), Ok(b2)]),
        ));

        let processor = MockProcessor::new();
        let trainer = ZoneTrainer::new(processor, 3).unwrap();
        let (stats, _) = trainer.train(stream).await.unwrap();

        // Three zones: frag 0 full zone, frag 0 partial (flushed at boundary), frag 1
        assert_eq!(stats.len(), 3);

        // Zone 0: Fragment 0, offsets [0..=2] (fills capacity)
        assert_eq!(stats[0].bound.fragment_id, 0);
        assert_eq!(stats[0].bound.start, 0);
        assert_eq!(stats[0].bound.length, 3);
        assert_eq!(stats[0].sum, 3);

        // Zone 1: Fragment 0, offset 3 (partial, flushed at fragment boundary)
        assert_eq!(stats[1].bound.fragment_id, 0);
        assert_eq!(stats[1].bound.start, 3);
        assert_eq!(stats[1].bound.length, 1);
        assert_eq!(stats[1].sum, 1);

        // Zone 2: Fragment 1, offsets [0..=1]
        assert_eq!(stats[2].bound.fragment_id, 1);
        assert_eq!(stats[2].bound.start, 0);
        assert_eq!(stats[2].bound.length, 2);
        assert_eq!(stats[2].sum, 4);
    }

    #[tokio::test]
    async fn handles_non_contiguous_offsets_after_deletion() {
        // CRITICAL: Test deletion scenario with non-contiguous row offsets.
        // This is the main reason for tracking first/last offsets.
        // Simulate a zone where rows 2, 3, 4, 6 have been deleted.
        let values = vec![1, 1, 1, 1, 1, 1]; // 6 actual rows
        let fragments = vec![0, 0, 0, 0, 0, 0];
        let offsets = vec![0, 1, 5, 7, 8, 9]; // Non-contiguous!

        let batch = batch(values, fragments, offsets);
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            stream::once(async { Ok(batch) }),
        ));

        let processor = MockProcessor::new();
        let trainer = ZoneTrainer::new(processor, 4).unwrap();
        let (stats, _) = trainer.train(stream).await.unwrap();

        // Should create 2 zones (capacity=4):
        // Zone 0: rows at offsets [0, 1, 5, 7] (4 rows)
        // Zone 1: rows at offsets [8, 9] (2 rows)
        assert_eq!(stats.len(), 2);

        // First zone: 4 rows, but offset span is [0..=7] so length=8 (due to gaps)
        assert_eq!(stats[0].sum, 4);
        assert_eq!(stats[0].bound.fragment_id, 0);
        assert_eq!(stats[0].bound.start, 0);
        assert_eq!(stats[0].bound.length, 8); // Address span: 7 - 0 + 1

        // Second zone: 2 rows, offset span is [8..=9] so length=2
        assert_eq!(stats[1].sum, 2);
        assert_eq!(stats[1].bound.fragment_id, 0);
        assert_eq!(stats[1].bound.start, 8);
        assert_eq!(stats[1].bound.length, 2); // Address span: 9 - 8 + 1
    }

    #[tokio::test]
    async fn handles_deletion_with_large_gaps() {
        // Extreme deletion scenario: very large gaps between consecutive rows.
        let values = vec![1, 1, 1];
        let fragments = vec![0, 0, 0];
        let offsets = vec![0, 100, 200]; // Huge gaps!

        let batch = batch(values, fragments, offsets);
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            stream::once(async { Ok(batch) }),
        ));

        let processor = MockProcessor::new();
        let trainer = ZoneTrainer::new(processor, 10).unwrap();
        let (stats, _) = trainer.train(stream).await.unwrap();

        // One zone with 3 rows, but offset span [0..=200] so length=201 due to large gaps
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].sum, 3);
        assert_eq!(stats[0].bound.start, 0);
        assert_eq!(stats[0].bound.length, 201); // Span: 200 - 0 + 1
    }

    #[tokio::test]
    async fn handles_non_contiguous_fragment_ids() {
        // CRITICAL: Test fragment IDs that are not consecutive (e.g., after fragment deletion).
        // Original code assumed fragment_id + 1, which would fail here.
        // Fragment IDs: 0, 5, 10 (non-consecutive!)
        let values = vec![1, 1, 2, 2, 3, 3];
        let fragments = vec![0, 0, 5, 5, 10, 10]; // Gaps in fragment IDs
        let offsets = vec![0, 1, 0, 1, 0, 1];

        let batch = batch(values, fragments, offsets);
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            stream::once(async { Ok(batch) }),
        ));

        let processor = MockProcessor::new();
        let trainer = ZoneTrainer::new(processor, 10).unwrap();
        let (stats, _) = trainer.train(stream).await.unwrap();

        // Should create 3 zones (one per fragment)
        assert_eq!(stats.len(), 3);

        // Fragment 0
        assert_eq!(stats[0].bound.fragment_id, 0);
        assert_eq!(stats[0].bound.start, 0);
        assert_eq!(stats[0].bound.length, 2);
        assert_eq!(stats[0].sum, 2);

        // Fragment 5 (not 1!)
        assert_eq!(stats[1].bound.fragment_id, 5);
        assert_eq!(stats[1].bound.start, 0);
        assert_eq!(stats[1].bound.length, 2);
        assert_eq!(stats[1].sum, 4);

        // Fragment 10 (not 2!)
        assert_eq!(stats[2].bound.fragment_id, 10);
        assert_eq!(stats[2].bound.start, 0);
        assert_eq!(stats[2].bound.length, 2);
        assert_eq!(stats[2].sum, 6);
    }

    #[test]
    fn search_zones_collects_row_ranges() {
        // Ensure the shared helper converts matching zones into the correct row-id
        // ranges (fragment upper bits + local offsets) while skipping non-matching
        // zones. This protects the helper if we modify how RowAddrTreeMap ranges are
        // inserted in the future.
        #[derive(Debug)]
        struct DummyZone {
            bound: ZoneBound,
            matches: bool,
        }

        impl AsRef<ZoneBound> for DummyZone {
            fn as_ref(&self) -> &ZoneBound {
                &self.bound
            }
        }

        let zones = vec![
            DummyZone {
                bound: ZoneBound {
                    fragment_id: 0,
                    start: 0,
                    length: 2,
                },
                matches: true,
            },
            DummyZone {
                bound: ZoneBound {
                    fragment_id: 1,
                    start: 5,
                    length: 3,
                },
                matches: false,
            },
            DummyZone {
                bound: ZoneBound {
                    fragment_id: 2,
                    start: 10,
                    length: 1,
                },
                matches: true,
            },
        ];

        let metrics = LocalMetricsCollector::default();
        let result = search_zones(&zones, &metrics, |zone| Ok(zone.matches)).unwrap();
        let SearchResult::AtMost(map) = result else {
            panic!("search_zones should return AtMost for dummy zones");
        };

        // Fragment 0, offsets 0 and 1
        assert!(map.selected(0));
        assert!(map.selected(1));
        // Fragment 1 should be skipped entirely
        assert!(!map.selected((1_u64 << 32) + 5));
        assert!(!map.selected((1_u64 << 32) + 7));
        // Fragment 2 includes only the single offset 10
        assert!(map.selected((2_u64 << 32) + 10));
        assert!(!map.selected((2_u64 << 32) + 11));
    }

    #[test]
    fn search_zones_returns_empty_when_no_match() {
        #[derive(Debug)]
        struct DummyZone {
            bound: ZoneBound,
            matches: bool,
        }

        impl AsRef<ZoneBound> for DummyZone {
            fn as_ref(&self) -> &ZoneBound {
                &self.bound
            }
        }

        // Both zones are marked as non-matching. The helper should return an empty map.
        let zones = vec![
            DummyZone {
                bound: ZoneBound {
                    fragment_id: 0,
                    start: 0,
                    length: 4,
                },
                matches: false,
            },
            DummyZone {
                bound: ZoneBound {
                    fragment_id: 1,
                    start: 10,
                    length: 2,
                },
                matches: false,
            },
        ];

        let metrics = LocalMetricsCollector::default();
        let result = search_zones(&zones, &metrics, |zone| Ok(zone.matches)).unwrap();
        let SearchResult::AtMost(map) = result else {
            panic!("expected AtMost result");
        };
        // No zones should be inserted when every predicate evaluates to false
        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn rebuild_zones_appends_new_stats() {
        let existing = vec![MockStats {
            sum: 50,
            bound: ZoneBound {
                fragment_id: 0,
                start: 0,
                length: 2,
            },
        }];

        let batch = batch(vec![3, 4], vec![1, 1], vec![0, 1]);
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            stream::once(async { Ok(batch) }),
        ));

        let trainer = ZoneTrainer::new(MockProcessor::new(), 2).unwrap();
        let (rebuilt, _) = rebuild_zones(&existing, trainer, stream).await.unwrap();
        // Existing zone should remain unchanged and new stats appended afterwards
        assert_eq!(rebuilt.len(), 2);
        assert_eq!(rebuilt[0].sum, 50);
        assert_eq!(rebuilt[1].sum, 7);
        assert_eq!(rebuilt[1].bound.fragment_id, 1);
        assert_eq!(rebuilt[1].bound.start, 0);
        assert_eq!(rebuilt[1].bound.length, 2);
    }

    #[tokio::test]
    async fn rebuild_zones_handles_multi_fragment_stream() {
        let existing = vec![MockStats {
            sum: 10,
            bound: ZoneBound {
                fragment_id: 0,
                start: 0,
                length: 1,
            },
        }];

        // Construct a stream with two fragments. Trainer should emit two zones that
        // get appended after the existing entries.
        let batch = batch(vec![5, 5, 6, 6], vec![1, 1, 2, 2], vec![0, 1, 0, 1]);
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            stream::once(async { Ok(batch) }),
        ));

        let trainer = ZoneTrainer::new(MockProcessor::new(), 2).unwrap();
        let (rebuilt, _) = rebuild_zones(&existing, trainer, stream).await.unwrap();
        // Existing zone plus two new fragments should yield three total zones
        assert_eq!(rebuilt.len(), 3);
        assert_eq!(rebuilt[0].bound.fragment_id, 0);
        assert_eq!(rebuilt[1].bound.fragment_id, 1);
        assert_eq!(rebuilt[2].bound.fragment_id, 2);
        assert_eq!(rebuilt[1].sum, 10);
        assert_eq!(rebuilt[2].sum, 12);
    }
}
