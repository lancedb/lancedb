// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::pin::Pin;
use std::task::Poll;
use std::{collections::VecDeque, task::Context};

use arrow::compute::kernels;
use arrow_array::RecordBatch;
use datafusion::physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
use datafusion_common::DataFusionError;
use futures::{Stream, StreamExt, TryStreamExt, ready};

use lance_core::Result;
use lance_core::error::DataFusionResult;

/// Wraps a [`SendableRecordBatchStream`] into a stream of RecordBatch chunks of
/// a given size.  This slices but does not copy any buffers.
struct BatchReaderChunker {
    /// The inner stream
    inner: SendableRecordBatchStream,
    /// The batches that have been read from the inner stream but not yet fully yielded
    buffered: VecDeque<RecordBatch>,
    /// The number of rows to yield in each chunk
    output_size: usize,
    /// The position within the first batch in the buffer to start yielding from
    i: usize,
}

impl BatchReaderChunker {
    fn new(inner: SendableRecordBatchStream, output_size: usize) -> Self {
        Self {
            inner,
            buffered: VecDeque::new(),
            output_size,
            i: 0,
        }
    }

    fn buffered_len(&self) -> usize {
        let buffer_total: usize = self.buffered.iter().map(|batch| batch.num_rows()).sum();
        buffer_total - self.i
    }

    async fn fill_buffer(&mut self) -> Result<()> {
        while self.buffered_len() < self.output_size {
            match self.inner.next().await {
                Some(Ok(batch)) => self.buffered.push_back(batch),
                Some(Err(e)) => return Err(e.into()),
                None => break,
            }
        }
        Ok(())
    }

    async fn next(&mut self) -> Option<Result<Vec<RecordBatch>>> {
        match self.fill_buffer().await {
            Ok(_) => {}
            Err(e) => return Some(Err(e)),
        };

        let mut batches = Vec::new();

        let mut rows_collected = 0;

        while rows_collected < self.output_size {
            if let Some(batch) = self.buffered.pop_front() {
                // Skip empty batch
                if batch.num_rows() == 0 {
                    continue;
                }

                let rows_remaining_in_batch = batch.num_rows() - self.i;
                let rows_to_take =
                    std::cmp::min(rows_remaining_in_batch, self.output_size - rows_collected);

                if rows_to_take == rows_remaining_in_batch {
                    // We're taking the whole batch, so we can just move it
                    let batch = if self.i == 0 {
                        batch
                    } else {
                        // We are taking the remainder of the batch, so we need to slice it
                        batch.slice(self.i, rows_to_take)
                    };
                    batches.push(batch);
                    self.i = 0;
                } else {
                    // We're taking a slice of the batch, so we need to copy it
                    batches.push(batch.slice(self.i, rows_to_take));
                    // And then we need to push the remainder back onto the front of the queue
                    self.i += rows_to_take;
                    self.buffered.push_front(batch);
                }

                rows_collected += rows_to_take;
            } else {
                break;
            }
        }

        if batches.is_empty() {
            None
        } else {
            Some(Ok(batches))
        }
    }
}

struct BreakStreamState {
    max_rows: usize,
    rows_seen: usize,
    rows_remaining: usize,
    batch: Option<RecordBatch>,
}

impl BreakStreamState {
    fn next(mut self) -> Option<(Result<RecordBatch>, Self)> {
        if self.rows_remaining == 0 {
            return None;
        }
        if self.rows_remaining + self.rows_seen <= self.max_rows {
            self.rows_seen = (self.rows_seen + self.rows_remaining) % self.max_rows;
            self.rows_remaining = 0;
            let next = self.batch.take().unwrap();
            Some((Ok(next), self))
        } else {
            let rows_to_emit = self.max_rows - self.rows_seen;
            self.rows_seen = 0;
            self.rows_remaining -= rows_to_emit;
            let batch = self.batch.as_mut().unwrap();
            let next = batch.slice(0, rows_to_emit);
            *batch = batch.slice(rows_to_emit, batch.num_rows() - rows_to_emit);
            Some((Ok(next), self))
        }
    }
}

// Given a stream of record batches, and a desired break point, this will
// make sure that a new record batch is emitted every time `break_point` rows
// have passed.
//
// This method will not combine record batches in any way.  For example, if
// the input lengths are [3, 5, 8, 3, 5], and the break point is 10 then the
// output batches will be [3, 5, 2 (break inserted) 6, 3, 1 (break inserted) 4]
pub fn break_stream(
    stream: SendableRecordBatchStream,
    max_chunk_size: usize,
) -> Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>> {
    let mut rows_already_seen = 0;
    stream
        .map_ok(move |batch| {
            let state = BreakStreamState {
                rows_remaining: batch.num_rows(),
                max_rows: max_chunk_size,
                rows_seen: rows_already_seen,
                batch: Some(batch),
            };
            rows_already_seen = (state.rows_seen + state.rows_remaining) % state.max_rows;

            futures::stream::unfold(state, move |state| std::future::ready(state.next()))
                .fuse()
                .boxed()
        })
        .try_flatten()
        .boxed()
}

/// Given a stream of record batches, this will yield batches of a fixed size.
///
/// In order to avoid copying data the batches will be converted into a stream of
/// `Vec<RecordBatch>` where each item is a `Vec` of batches whose total size is
/// `chunk_size`.
pub fn chunk_stream(
    stream: SendableRecordBatchStream,
    chunk_size: usize,
) -> Pin<Box<dyn Stream<Item = Result<Vec<RecordBatch>>> + Send>> {
    let chunker = BatchReaderChunker::new(stream, chunk_size);
    futures::stream::unfold(chunker, |mut chunker| async move {
        match chunker.next().await {
            Some(Ok(batches)) => Some((Ok(batches), chunker)),
            Some(Err(e)) => Some((Err(e), chunker)),
            None => None,
        }
    })
    .fuse()
    .boxed()
}

/// Given a stream of record batches, this will yield batches of a fixed size.
///
/// This stream _will_ combine record batches and so it can be fairly expensive as it will
/// likely force a copy of incoming data.  However, it can be useful when users require
/// precise batch sizing.
pub fn chunk_concat_stream(
    stream: SendableRecordBatchStream,
    chunk_size: usize,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let schema_copy = schema.clone();
    let chunked = chunk_stream(stream, chunk_size);
    let chunk_concat = chunked
        .and_then(move |batches| {
            std::future::ready(
                // chunk_stream is zero-copy and so it gives us pieces of batches.  However, the btree
                // index needs 1 batch-per-page and so we concatenate here.
                kernels::concat::concat_batches(&schema, batches.iter()).map_err(|e| e.into()),
            )
        })
        .map_err(DataFusionError::from)
        .boxed();
    Box::pin(RecordBatchStreamAdapter::new(schema_copy, chunk_concat))
}

/// Given a stream of record batches, this will yield batches of a fixed size.
///
/// This stream _will_ combine record batches and so it can be fairly expensive as it will
/// likely force a copy of all incoming data.  However, it can be useful when users require
/// precise batch sizing.
pub struct StrictBatchSizeStream<S> {
    inner: S,
    batch_size: usize,
    residual: Option<RecordBatch>,
}

impl<S: Stream<Item = DataFusionResult<RecordBatch>> + Unpin> StrictBatchSizeStream<S> {
    pub fn new(inner: S, batch_size: usize) -> Self {
        Self {
            inner,
            batch_size,
            residual: None,
        }
    }
}

/// Internal polling method for strict batch size enforcement.
///
/// # Use Case
/// When precise batch sizing is required (e.g., ML batch processing), this method guarantees
/// output batches exactly match batch_size until final partial batch. Maintains data integrity
/// across splits using row-aware splitting.
///
/// # Example
/// With batch_size=5 and input sequence:
/// - Fragment 1: 7 rows → splits into `[5,2]`
///   (queues 5, carries 2)
/// - Fragment 2: 4 rows → combines carried 2 + 4 = 6
///   splits into `[5,1]`
///
/// - Output batches: `[5]`, `[5]`, `[1]`
impl<S> Stream for StrictBatchSizeStream<S>
where
    S: Stream<Item = DataFusionResult<RecordBatch>> + Unpin,
{
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // Process residual first if present
            if let Some(residual) = self.residual.take() {
                if residual.num_rows() >= self.batch_size {
                    let split_at = self.batch_size;
                    let chunk = residual.slice(0, split_at);
                    let new_residual = residual.slice(split_at, residual.num_rows() - split_at);
                    self.residual = Some(new_residual);
                    return Poll::Ready(Some(Ok(chunk)));
                } else {
                    // Keep residual and proceed to get more data
                    self.residual = Some(residual);
                }
            }

            // Poll the inner stream for next batch
            match ready!(Pin::new(&mut self.inner).poll_next(cx)) {
                Some(Ok(batch)) => {
                    // Combine with residual if any
                    let current_batch = if let Some(residual) = self.residual.take() {
                        arrow::compute::concat_batches(&residual.schema(), &[residual, batch])
                            .map_err(|e| DataFusionError::External(Box::new(e)))?
                    } else {
                        batch
                    };

                    if current_batch.num_rows() >= self.batch_size {
                        let split_at = self.batch_size;
                        let chunk = current_batch.slice(0, split_at);
                        let new_residual =
                            current_batch.slice(split_at, current_batch.num_rows() - split_at);
                        if new_residual.num_rows() > 0 {
                            self.residual = Some(new_residual);
                        }
                        return Poll::Ready(Some(Ok(chunk)));
                    } else {
                        // Not enough rows, store as residual
                        self.residual = Some(current_batch);
                        continue;
                    }
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => {
                    return Poll::Ready(
                        self.residual
                            .take()
                            .filter(|r| r.num_rows() > 0)
                            .map(Ok::<_, DataFusionError>),
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{Int32Type, Int64Type};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::{StreamExt, TryStreamExt};
    use lance_datagen::{BatchCount, RowCount, array};

    use crate::datagen::DatafusionDatagenExt;

    #[tokio::test]
    async fn test_chunkers() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("", arrow::datatypes::DataType::Int32, false),
        ]));

        let make_batch = |num_rows: u32| {
            lance_datagen::gen_batch()
                .anon_col(lance_datagen::array::step::<Int32Type>())
                .into_batch_rows(RowCount::from(num_rows as u64))
                .unwrap()
        };

        let batches = vec![make_batch(10), make_batch(5), make_batch(13), make_batch(0)];

        let make_stream = || {
            let stream = futures::stream::iter(
                batches
                    .clone()
                    .into_iter()
                    .map(datafusion_common::Result::Ok),
            )
            .boxed();
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), stream))
        };

        let chunked = super::chunk_stream(make_stream(), 10)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(chunked.len(), 3);
        assert_eq!(chunked[0].len(), 1);
        assert_eq!(chunked[0][0].num_rows(), 10);
        assert_eq!(chunked[1].len(), 2);
        assert_eq!(chunked[1][0].num_rows(), 5);
        assert_eq!(chunked[1][1].num_rows(), 5);
        assert_eq!(chunked[2].len(), 1);
        assert_eq!(chunked[2][0].num_rows(), 8);

        let chunked = super::chunk_concat_stream(make_stream(), 10)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(chunked.len(), 3);
        assert_eq!(chunked[0].num_rows(), 10);
        assert_eq!(chunked[1].num_rows(), 10);
        assert_eq!(chunked[2].num_rows(), 8);

        let chunked = super::break_stream(make_stream(), 10)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(chunked.len(), 4);
        assert_eq!(chunked[0].num_rows(), 10);
        assert_eq!(chunked[1].num_rows(), 5);
        assert_eq!(chunked[2].num_rows(), 5);
        assert_eq!(chunked[3].num_rows(), 8);
    }

    #[tokio::test]
    async fn test_strict_batch_size_stream() {
        let batches = lance_datagen::gen_batch()
            .anon_col(array::step::<Int32Type>())
            .anon_col(array::step::<Int64Type>())
            .into_df_stream(RowCount::from(7), BatchCount::from(10));

        let stream = super::StrictBatchSizeStream::new(batches, 10);

        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batches.len(), 7);

        for batch in batches {
            assert_eq!(batch.num_rows(), 10);
        }
    }
}
