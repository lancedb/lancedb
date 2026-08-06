// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Utilities for working with streams of [`RecordBatch`].

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use futures::stream::{self, Stream, StreamExt};
use std::pin::Pin;

use crate::deepcopy::deep_copy_batch_sliced;

/// Rechunks a stream of [`RecordBatch`] so that each output batch has
/// approximately `target_bytes` of array data.
///
/// Small input batches are accumulated (by concatenation) until at least
/// `min_bytes` of data has been collected. If the resulting batch exceeds
/// `max_bytes`, it is sliced into roughly equal pieces of ~`max_bytes`
/// (assuming uniform row sizes).
pub fn rechunk_stream_by_size<S, E>(
    input: S,
    input_schema: SchemaRef,
    min_bytes: usize,
    max_bytes: usize,
) -> impl Stream<Item = Result<RecordBatch, E>>
where
    S: Stream<Item = Result<RecordBatch, E>>,
    E: From<ArrowError>,
{
    rechunk_stream_by_size_inner(input, input_schema, min_bytes, max_bytes, false)
}

/// Like [`rechunk_stream_by_size`] but deep-copies slices so that
/// `get_array_memory_size` reflects the true size of each output batch.
///
/// After a normal `RecordBatch::slice`, the backing buffers are shared with
/// the original batch, so `get_array_memory_size` still reports the full
/// parent size.  This variant deep-copies every slice produced during the
/// splitting phase, which allows the stream to detect and re-split slices
/// that still exceed `max_bytes` (e.g. because a single row is much larger
/// than average).
///
/// The deep copy is a last resort and potentially expensive for large
/// batches.  However, it is only performed when a batch actually needs to be
/// sliced — batches that are already within the target range pass through at
/// zero cost.  Use this only when the hard cap on `max_bytes` is a
/// correctness requirement, not merely a performance hint.
pub fn rechunk_stream_by_size_deep_copy<S, E>(
    input: S,
    input_schema: SchemaRef,
    min_bytes: usize,
    max_bytes: usize,
) -> impl Stream<Item = Result<RecordBatch, E>>
where
    S: Stream<Item = Result<RecordBatch, E>>,
    E: From<ArrowError>,
{
    rechunk_stream_by_size_inner(input, input_schema, min_bytes, max_bytes, true)
}

fn rechunk_stream_by_size_inner<S, E>(
    input: S,
    input_schema: SchemaRef,
    min_bytes: usize,
    max_bytes: usize,
    deep_copy: bool,
) -> impl Stream<Item = Result<RecordBatch, E>>
where
    S: Stream<Item = Result<RecordBatch, E>>,
    E: From<ArrowError>,
{
    stream::try_unfold(
        RechunkState {
            input: Box::pin(input),
            accumulated: Vec::new(),
            acc_bytes: 0,
            done: false,
            input_schema,
            min_bytes,
            max_bytes,
            deep_copy,
        },
        |mut state| async move {
            if state.done && state.accumulated.is_empty() {
                return Ok(None);
            }

            // Pull batches until we reach the byte target or exhaust input.
            // Always pull at least one batch so that min_bytes=0 works.
            while !state.done && (state.accumulated.is_empty() || state.acc_bytes < state.min_bytes)
            {
                match state.input.next().await {
                    Some(Ok(batch)) => {
                        state.acc_bytes += batch.get_array_memory_size();
                        state.accumulated.push(batch);
                    }
                    Some(Err(e)) => return Err(e),
                    None => {
                        state.done = true;
                    }
                }
            }

            if state.accumulated.is_empty() {
                return Ok(None);
            }

            // Fast path: if the first accumulated batch already meets the
            // byte threshold, deliver it directly instead of concatenating
            // everything together (which would just get sliced back apart).
            if state.accumulated.len() > 1
                && state.accumulated[0].get_array_memory_size() >= state.min_bytes
            {
                let b = state.accumulated.remove(0);
                state.acc_bytes -= b.get_array_memory_size();
                return Ok(Some((b, state)));
            }

            let batch = if state.accumulated.len() == 1 {
                state.accumulated.pop().unwrap()
            } else {
                let b =
                    arrow_select::concat::concat_batches(&state.input_schema, &state.accumulated)
                        .map_err(E::from)?;
                state.accumulated.clear();
                b
            };
            state.acc_bytes = 0;

            // Slice the batch into ~max_bytes pieces assuming uniform row sizes.
            let mut slices =
                slice_batch(batch, state.max_bytes, state.deep_copy).map_err(E::from)?;

            if slices.len() == 1 {
                Ok(Some((slices.pop().unwrap(), state)))
            } else {
                let first = slices.remove(0);

                // Stash leftover slices for subsequent iterations.
                for a in &slices {
                    state.acc_bytes += a.get_array_memory_size();
                }
                state.accumulated = slices;

                Ok(Some((first, state)))
            }
        },
    )
}

/// Slice a batch into pieces of at most `max_bytes`.
///
/// When `deep_copy` is false, slices share buffers with the original batch
/// and `get_array_memory_size` will still report the parent buffer size.
/// This is fine when the caller only needs approximate sizing.
///
/// When `deep_copy` is true, each slice is deep-copied so that
/// `get_array_memory_size` reflects the true size.  If a deep-copied slice
/// still exceeds `max_bytes` (due to non-uniform row sizes), it is
/// recursively split until every piece is within budget or contains only a
/// single row.
fn slice_batch(
    batch: RecordBatch,
    max_bytes: usize,
    deep_copy: bool,
) -> Result<Vec<RecordBatch>, ArrowError> {
    let batch_bytes = batch.get_array_memory_size();
    let num_rows = batch.num_rows();

    if batch_bytes <= max_bytes || num_rows <= 1 {
        return Ok(vec![batch]);
    }

    let rows_per_chunk = (max_bytes as u64 * num_rows as u64 / batch_bytes as u64).max(1) as usize;

    let mut result = Vec::new();
    let mut offset = 0;
    while offset < num_rows {
        let len = rows_per_chunk.min(num_rows - offset);
        let slice = batch.slice(offset, len);
        if deep_copy {
            let copied = deep_copy_batch_sliced(&slice)?;
            // Recurse: the deep-copied slice has accurate sizes, so if it
            // still exceeds max_bytes we can split further.
            result.extend(slice_batch(copied, max_bytes, true)?);
        } else {
            result.push(slice);
        }
        offset += len;
    }

    Ok(result)
}

/// Internal state for [`rechunk_stream`].
///
/// Kept as a named struct so the `try_unfold` closure stays readable.
struct RechunkState<S> {
    input: Pin<Box<S>>,
    accumulated: Vec<RecordBatch>,
    acc_bytes: usize,
    done: bool,
    input_schema: SchemaRef,
    min_bytes: usize,
    max_bytes: usize,
    deep_copy: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use futures::executor::block_on;

    fn make_batch(num_rows: usize) -> RecordBatch {
        let schema = test_schema();
        let values: Vec<i32> = (0..num_rows as i32).collect();
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn collect_rechunked(
        batches: Vec<RecordBatch>,
        min_bytes: usize,
        max_bytes: usize,
    ) -> Vec<RecordBatch> {
        let input = stream::iter(batches.into_iter().map(Ok::<_, ArrowError>));
        let rechunked = rechunk_stream_by_size(input, test_schema(), min_bytes, max_bytes);
        block_on(rechunked.collect::<Vec<_>>())
            .into_iter()
            .map(|r| r.unwrap())
            .collect()
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    #[test]
    fn test_empty_stream() {
        let result = collect_rechunked(vec![], 100, 200);
        assert!(result.is_empty());
    }

    #[test]
    fn test_single_batch_passthrough() {
        let batch = make_batch(100);
        let bytes = batch.get_array_memory_size();
        // Batch is between min and max — should pass through as-is.
        let result = collect_rechunked(vec![batch], bytes / 2, bytes * 2);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 100);
    }

    #[test]
    fn test_small_batches_concatenated() {
        let one_batch_bytes = make_batch(10).get_array_memory_size();
        let batches: Vec<_> = (0..8).map(|_| make_batch(10)).collect();
        // min = 5 batches worth, max = 10 batches worth.
        let result = collect_rechunked(batches, one_batch_bytes * 5, one_batch_bytes * 10);
        assert_eq!(total_rows(&result), 80);
        // Should have been concatenated into fewer batches than the 8 inputs.
        assert!(
            result.len() < 8,
            "expected fewer output batches, got {}",
            result.len()
        );
    }

    #[test]
    fn test_large_batch_sliced() {
        let batch = make_batch(1000);
        let bytes = batch.get_array_memory_size();
        let result = collect_rechunked(vec![batch], bytes / 8, bytes / 4);
        assert_eq!(total_rows(&result), 1000);
        assert!(
            result.len() >= 4,
            "expected at least 4 slices, got {}",
            result.len()
        );
    }

    #[test]
    fn test_sliced_leftovers_are_not_recombined() {
        // Key test for the fast-path optimisation. When a large batch is
        // sliced, leftover slices should be delivered one-at-a-time without
        // being concatenated back together.  We verify this by checking that
        // every output buffer pointer falls inside the original batch's
        // allocation (i.e. they are all zero-copy slices, not fresh copies).
        let batch = make_batch(1000);
        let bytes = batch.get_array_memory_size();
        let orig_data = batch.column(0).to_data();
        let orig_buf = &orig_data.buffers()[0];
        let orig_start = orig_buf.as_ptr() as usize;
        let orig_end = orig_start + orig_buf.len();

        let result = collect_rechunked(vec![batch], bytes / 8, bytes / 4);

        assert_eq!(total_rows(&result), 1000);
        assert!(result.len() >= 4);

        for (i, b) in result.iter().enumerate() {
            let ptr = b.column(0).to_data().buffers()[0].as_ptr() as usize;
            assert!(
                ptr >= orig_start && ptr < orig_end,
                "slice {i} buffer at {ptr:#x} is outside the original allocation \
                 [{orig_start:#x}, {orig_end:#x}) — it was re-concatenated"
            );
        }
    }

    #[test]
    fn test_flush_remainder_on_stream_end() {
        // Data below min_bytes should still be flushed when the stream ends.
        let batch = make_batch(10);
        let bytes = batch.get_array_memory_size();
        let result = collect_rechunked(vec![batch], bytes * 100, bytes * 200);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 10);
    }

    #[test]
    fn test_large_then_small_batches() {
        // After a large batch is fully drained, subsequent small batches
        // should be accumulated normally.
        let large = make_batch(1000);
        let small_bytes = make_batch(10).get_array_memory_size();
        let batches = vec![
            large,
            make_batch(10),
            make_batch(10),
            make_batch(10),
            make_batch(10),
            make_batch(10),
        ];
        let result = collect_rechunked(batches, small_bytes * 3, small_bytes * 100);
        assert_eq!(total_rows(&result), 1050);
        // The large batch should appear (possibly sliced) followed by
        // concatenated small batches, so we should have fewer output batches
        // than the 6 inputs.
        assert!(result.len() < 6);
    }

    #[test]
    fn test_row_preservation_across_slicing() {
        // Verify that every input row appears exactly once in the output
        // and in the correct order after slicing.
        let batch = make_batch(237); // odd count to exercise remainder slice
        let bytes = batch.get_array_memory_size();
        let result = collect_rechunked(vec![batch], bytes / 8, bytes / 5);

        assert_eq!(total_rows(&result), 237);

        let values: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        let expected: Vec<i32> = (0..237).collect();
        assert_eq!(values, expected);
    }

    #[test]
    fn test_min_bytes_zero_still_yields_all_rows() {
        // When min_bytes=0, the stream should still yield every batch.
        // This is the "chop only, don't coalesce" use case.
        let batches: Vec<_> = (0..5).map(|_| make_batch(100)).collect();
        let batch_bytes = batches[0].get_array_memory_size();
        let result = collect_rechunked(batches, 0, batch_bytes * 2);
        assert_eq!(total_rows(&result), 500);
    }

    #[test]
    fn test_min_bytes_zero_slices_oversized() {
        // min_bytes=0 with a small max_bytes should still slice large batches.
        let batch = make_batch(1000);
        let bytes = batch.get_array_memory_size();
        let result = collect_rechunked(vec![batch], 0, bytes / 4);
        assert_eq!(total_rows(&result), 1000);
        assert!(
            result.len() >= 4,
            "expected at least 4 slices, got {}",
            result.len()
        );
    }

    /// Build a batch with one variable-length string column.
    /// Every row is `small_size` bytes except the row at index `big_row_idx`
    /// which is `big_size` bytes.
    fn make_variable_batch(
        num_rows: usize,
        small_size: usize,
        big_row_idx: usize,
        big_size: usize,
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let values: Vec<String> = (0..num_rows)
            .map(|i| {
                if i == big_row_idx {
                    "X".repeat(big_size)
                } else {
                    "x".repeat(small_size)
                }
            })
            .collect();
        let array = arrow_array::StringArray::from(values);
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    fn variable_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]))
    }

    fn collect_rechunked_variable(
        batches: Vec<RecordBatch>,
        min_bytes: usize,
        max_bytes: usize,
    ) -> Vec<RecordBatch> {
        let input = stream::iter(batches.into_iter().map(Ok::<_, ArrowError>));
        let rechunked =
            rechunk_stream_by_size_deep_copy(input, variable_schema(), min_bytes, max_bytes);
        block_on(rechunked.collect::<Vec<_>>())
            .into_iter()
            .map(|r| r.unwrap())
            .collect()
    }

    #[test]
    fn test_oversized_row_at_end() {
        // 100 rows: 99 small (64 bytes each) + 1 large (100KiB) at the end.
        let batch = make_variable_batch(100, 64, 99, 100 * 1024);
        let max_bytes = 64 * 1024;
        let result = collect_rechunked_variable(vec![batch], 0, max_bytes);
        assert_eq!(total_rows(&result), 100);
        for (i, b) in result.iter().enumerate() {
            let size = b.get_array_memory_size();
            assert!(
                size <= max_bytes || b.num_rows() == 1,
                "batch {i} has {size} bytes (max {max_bytes}) and {} rows",
                b.num_rows()
            );
        }
    }

    #[test]
    fn test_oversized_row_at_start() {
        // 100 rows: 1 large (100KiB) at the start + 99 small (64 bytes each).
        let batch = make_variable_batch(100, 64, 0, 100 * 1024);
        let max_bytes = 64 * 1024;
        let result = collect_rechunked_variable(vec![batch], 0, max_bytes);
        assert_eq!(total_rows(&result), 100);
        for (i, b) in result.iter().enumerate() {
            let size = b.get_array_memory_size();
            assert!(
                size <= max_bytes || b.num_rows() == 1,
                "batch {i} has {size} bytes (max {max_bytes}) and {} rows",
                b.num_rows()
            );
        }
    }

    #[test]
    fn test_oversized_row_in_middle() {
        // 100 rows: 1 large (100KiB) in the middle + 99 small (64 bytes each).
        let batch = make_variable_batch(100, 64, 50, 100 * 1024);
        let max_bytes = 64 * 1024;
        let result = collect_rechunked_variable(vec![batch], 0, max_bytes);
        assert_eq!(total_rows(&result), 100);
        for (i, b) in result.iter().enumerate() {
            let size = b.get_array_memory_size();
            assert!(
                size <= max_bytes || b.num_rows() == 1,
                "batch {i} has {size} bytes (max {max_bytes}) and {} rows",
                b.num_rows()
            );
        }
    }

    #[test]
    fn test_error_propagation() {
        let input = stream::iter(vec![
            Ok(make_batch(10)),
            Err(ArrowError::ComputeError("boom".into())),
            Ok(make_batch(10)),
        ]);
        let rechunked = rechunk_stream_by_size(input, test_schema(), 1, usize::MAX);
        let results: Vec<Result<RecordBatch, ArrowError>> = block_on(rechunked.collect());
        assert!(results.iter().any(|r| r.is_err()));
    }
}
