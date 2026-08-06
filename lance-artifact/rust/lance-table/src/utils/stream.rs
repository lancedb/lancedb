// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{fmt, sync::Arc};

use arrow_array::{BooleanArray, RecordBatch, RecordBatchOptions, UInt64Array, make_array};
use arrow_buffer::NullBuffer;
use futures::{
    FutureExt, Stream, StreamExt,
    future::{BoxFuture, Shared},
    stream::{BoxStream, FuturesOrdered},
};
use lance_arrow::RecordBatchExt;
use lance_core::{
    Error, ROW_ADDR, ROW_ADDR_FIELD, ROW_CREATED_AT_VERSION_FIELD, ROW_ID, ROW_ID_FIELD,
    ROW_LAST_UPDATED_AT_VERSION_FIELD, Result,
    utils::{address::RowAddress, deletion::DeletionVector},
};
use lance_io::ReadBatchParams;
use tracing::instrument;

use crate::rowids::RowIdSequence;

pub type ReadBatchFut = BoxFuture<'static, Result<RecordBatch>>;
/// A task, emitted by a file reader, that will produce a batch (of the
/// given size)
pub struct ReadBatchTask {
    pub task: ReadBatchFut,
    pub num_rows: u32,
}
pub type ReadBatchTaskStream = BoxStream<'static, ReadBatchTask>;
pub type ReadBatchFutStream = BoxStream<'static, ReadBatchFut>;

type SharedReadBatchFut = Shared<BoxFuture<'static, std::result::Result<RecordBatch, Arc<Error>>>>;

#[derive(Debug)]
struct SharedReadError(Arc<Error>);

impl fmt::Display for SharedReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for SharedReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

struct PendingReadBatch {
    task: Option<ReadBatchFut>,
    shared_task: Option<SharedReadBatchFut>,
    offset: u32,
    num_rows: u32,
}

impl PendingReadBatch {
    fn new(task: ReadBatchTask) -> Self {
        Self {
            task: Some(task.task),
            shared_task: None,
            offset: 0,
            num_rows: task.num_rows,
        }
    }

    fn take(&mut self, num_rows: u32) -> ReadBatchFut {
        debug_assert!(num_rows <= self.num_rows);

        if self.offset == 0 && num_rows == self.num_rows && self.shared_task.is_none() {
            self.num_rows = 0;
            let Some(task) = self.task.take() else {
                return async {
                    Err(Error::internal(
                        "missing read task while merging aligned streams".to_string(),
                    ))
                }
                .boxed();
            };
            return task;
        }

        let shared_task = self
            .shared_task
            .get_or_insert_with(|| {
                let task = self.task.take();
                async move {
                    let Some(task) = task else {
                        return Err(Arc::new(Error::internal(
                            "missing read task while splitting a merged stream".to_string(),
                        )));
                    };
                    task.await.map_err(Arc::new)
                }
                .boxed()
                .shared()
            })
            .clone();
        let offset = self.offset;
        self.offset += num_rows;
        self.num_rows -= num_rows;

        async move {
            match shared_task.await {
                Ok(batch) => Ok(batch.slice(offset as usize, num_rows as usize)),
                Err(error) => Err(Error::wrapped(Box::new(SharedReadError(error)))),
            }
        }
        .boxed()
    }
}

struct MergeStream {
    streams: Vec<ReadBatchTaskStream>,
    pending: Vec<Option<PendingReadBatch>>,
    index: usize,
}

impl MergeStream {
    fn emit(&mut self) -> ReadBatchTask {
        let num_rows = self
            .pending
            .iter()
            .filter_map(|pending| pending.as_ref().map(|pending| pending.num_rows))
            .min()
            .unwrap_or_default();
        let mut batches = FuturesOrdered::new();
        for pending in &mut self.pending {
            let Some(pending_batch) = pending.as_mut() else {
                continue;
            };
            batches.push_back(pending_batch.take(num_rows));
            if pending_batch.num_rows == 0 {
                *pending = None;
            }
        }
        let task = async move {
            let Some(first) = batches.next().await else {
                return Err(Error::internal(
                    "cannot merge an empty set of read batches".to_string(),
                ));
            };
            let mut batch = first?;
            while let Some(next) = batches.next().await {
                let next = next?;
                batch = batch.merge(&next)?;
            }
            Ok(batch)
        }
        .boxed();
        ReadBatchTask { task, num_rows }
    }
}

impl Stream for MergeStream {
    type Item = ReadBatchTask;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            if self.pending.iter().all(Option::is_some) {
                return std::task::Poll::Ready(Some(self.emit()));
            }

            let index = self.index;
            if self.pending[index].is_some() {
                self.index = (index + 1) % self.streams.len();
                continue;
            }
            match self.streams[index].poll_next_unpin(cx) {
                std::task::Poll::Ready(Some(batch_task)) => {
                    self.pending[index] = Some(PendingReadBatch::new(batch_task));
                    self.index = (index + 1) % self.streams.len();
                }
                std::task::Poll::Ready(None) => {
                    return std::task::Poll::Ready(None);
                }
                std::task::Poll::Pending => {
                    return std::task::Poll::Pending;
                }
            }
        }
    }
}

/// Given multiple streams of batch tasks, merge them into a single stream
///
/// This pulls one batch from each stream and then combines the columns from
/// all of the batches into a single batch.  The order of the batches in the
/// streams is maintained and the merged batch columns will be in order from first
/// to last stream. If the streams use different batch boundaries then batches are
/// sliced so each merged output remains row-aligned.
///
/// This stream ends as soon as any of the input streams ends (we do not
/// verify that the other input streams are finished as well)
pub fn merge_streams(streams: Vec<ReadBatchTaskStream>) -> ReadBatchTaskStream {
    if streams.is_empty() {
        return futures::stream::empty().boxed();
    }
    let pending = (0..streams.len()).map(|_| None).collect();
    MergeStream {
        streams,
        pending,
        index: 0,
    }
    .boxed()
}

/// Apply a mask to the batch, where rows are "deleted" by the _rowid column null.
///
/// This is used partly as a performance optimization (cheaper to null than to filter)
/// but also because there are cases where we want to load the physical rows.  For example,
/// we may be replacing a column based on some UDF and we want to provide a value for the
/// deleted rows to ensure the fragments are aligned.
fn apply_deletions_as_nulls(batch: RecordBatch, mask: &BooleanArray) -> Result<RecordBatch> {
    // Transform mask into null buffer. Null means deleted, though note that
    // null buffers are actually validity buffers, so True means not null
    // and thus not deleted.
    let mask_buffer = NullBuffer::new(mask.values().clone());

    if mask_buffer.null_count() == 0 {
        // No rows are deleted
        return Ok(batch);
    }

    // For each column convert to data
    let new_columns = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, col)| {
            if field.name() == ROW_ID || field.name() == ROW_ADDR {
                let col_data = col.to_data();
                // If it already has a validity bitmap, then AND it with the mask.
                // Otherwise, use the boolean buffer as the mask.
                let null_buffer = NullBuffer::union(col_data.nulls(), Some(&mask_buffer));

                Ok(col_data
                    .into_builder()
                    .null_bit_buffer(null_buffer.map(|b| b.buffer().clone()))
                    .build()
                    .map(make_array)?)
            } else {
                Ok(col.clone())
            }
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        new_columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}

/// Extract version values for a batch selection by binary-searching over
/// precomputed RLE run offsets. Single-run fragments (the common case)
/// take the O(1) fast path.
fn version_values_for_selection(
    sequence: &crate::rowids::version::RowDatasetVersionSequence,
    params: &ReadBatchParams,
    batch_offset: u32,
    num_rows: u32,
) -> Result<Vec<u64>> {
    let selection = params
        .slice(batch_offset as usize, num_rows as usize)
        .unwrap()
        .to_ranges()
        .unwrap();

    if sequence.runs.len() == 1 {
        return Ok(vec![sequence.runs[0].version(); num_rows as usize]);
    }

    let mut versions = Vec::with_capacity(num_rows as usize);
    let run_offsets: Vec<usize> = sequence
        .runs
        .iter()
        .scan(0usize, |acc, run| {
            let start = *acc;
            *acc += run.len();
            Some(start)
        })
        .collect();
    let total_len: usize = sequence.runs.iter().map(|r| r.len()).sum();

    for r in &selection {
        for pos in r.start..r.end {
            let pos = pos as usize;
            if pos >= total_len {
                return Err(lance_core::Error::internal(format!(
                    "version column position {} out of range (total_len={})",
                    pos, total_len
                )));
            }
            let run_idx = match run_offsets.binary_search(&pos) {
                Ok(idx) => idx,
                Err(idx) => idx - 1,
            };
            versions.push(sequence.runs[run_idx].version());
        }
    }
    Ok(versions)
}

/// Configuration needed to apply row ids and deletions to a batch
#[derive(Debug)]
pub struct RowIdAndDeletesConfig {
    /// The row ids that were requested
    pub params: ReadBatchParams,
    /// Whether to include the row id column in the final batch
    pub with_row_id: bool,
    /// Whether to include the row address column in the final batch
    pub with_row_addr: bool,
    /// Whether to include the last updated at version column in the final batch
    pub with_row_last_updated_at_version: bool,
    /// Whether to include the created at version column in the final batch
    pub with_row_created_at_version: bool,
    /// An optional deletion vector to apply to the batch
    pub deletion_vector: Option<Arc<DeletionVector>>,
    /// An optional row id sequence to use for the row id column.
    pub row_id_sequence: Option<Arc<RowIdSequence>>,
    /// The last_updated_at version sequence
    pub last_updated_at_sequence: Option<Arc<crate::rowids::version::RowDatasetVersionSequence>>,
    /// The created_at version sequence
    pub created_at_sequence: Option<Arc<crate::rowids::version::RowDatasetVersionSequence>>,
    /// Whether to make deleted rows null instead of filtering them out
    pub make_deletions_null: bool,
    /// The total number of rows that will be loaded
    ///
    /// This is needed to convert ReadbatchParams::RangeTo into a valid range
    pub total_num_rows: u32,
}

impl RowIdAndDeletesConfig {
    fn has_system_cols(&self) -> bool {
        self.with_row_id
            || self.with_row_addr
            || self.with_row_last_updated_at_version
            || self.with_row_created_at_version
    }
}

#[instrument(level = "debug", skip_all)]
pub fn apply_row_id_and_deletes(
    batch: RecordBatch,
    batch_offset: u32,
    fragment_id: u32,
    config: &RowIdAndDeletesConfig,
) -> Result<RecordBatch> {
    let mut deletion_vector = config.deletion_vector.as_ref();
    // Convert Some(NoDeletions) into None to simplify logic below
    if let Some(deletion_vector_inner) = deletion_vector
        && matches!(deletion_vector_inner.as_ref(), DeletionVector::NoDeletions)
    {
        deletion_vector = None;
    }
    let has_deletions = deletion_vector.is_some();
    debug_assert!(batch.num_columns() > 0 || config.has_system_cols() || has_deletions);

    // If row id sequence is None, then row id IS row address.
    let should_fetch_row_addr = config.with_row_addr
        || (config.with_row_id && config.row_id_sequence.is_none())
        || has_deletions;

    let num_rows = batch.num_rows() as u32;

    let row_addrs =
        if should_fetch_row_addr {
            let _rowaddrs = tracing::span!(tracing::Level::DEBUG, "fetch_row_addrs").entered();
            let mut row_addrs = Vec::with_capacity(num_rows as usize);
            for offset_range in config
                .params
                .slice(batch_offset as usize, num_rows as usize)
                .unwrap()
                .iter_offset_ranges()?
            {
                row_addrs.extend(offset_range.map(|row_offset| {
                    u64::from(RowAddress::new_from_parts(fragment_id, row_offset))
                }));
            }

            Some(Arc::new(UInt64Array::from(row_addrs)))
        } else {
            None
        };

    let row_ids = if config.with_row_id {
        let _rowids = tracing::span!(tracing::Level::DEBUG, "fetch_row_ids").entered();
        if let Some(row_id_sequence) = &config.row_id_sequence {
            let selection = config
                .params
                .slice(batch_offset as usize, num_rows as usize)
                .unwrap()
                .to_ranges()
                .unwrap();
            let row_ids = row_id_sequence
                .select(
                    selection
                        .iter()
                        .flat_map(|r| r.start as usize..r.end as usize),
                )
                .collect::<UInt64Array>();
            Some(Arc::new(row_ids))
        } else {
            // If we don't have a row id sequence, can assume the row ids are
            // the same as the row addresses.
            row_addrs.clone()
        }
    } else {
        None
    };

    let span = tracing::span!(tracing::Level::DEBUG, "apply_deletions");
    let _enter = span.enter();
    let deletion_mask = deletion_vector.and_then(|v| {
        let row_addrs: &[u64] = row_addrs.as_ref().unwrap().values();
        v.build_predicate(row_addrs.iter())
    });

    let batch = if config.with_row_id {
        let row_id_arr = row_ids.unwrap();
        batch.try_with_column(ROW_ID_FIELD.clone(), row_id_arr)?
    } else {
        batch
    };

    let batch = if config.with_row_addr {
        let row_addr_arr = row_addrs.unwrap();
        batch.try_with_column(ROW_ADDR_FIELD.clone(), row_addr_arr)?
    } else {
        batch
    };

    // Add version columns if requested
    let batch = if config.with_row_last_updated_at_version || config.with_row_created_at_version {
        let mut batch = batch;

        if config.with_row_last_updated_at_version {
            let version_arr = if let Some(sequence) = &config.last_updated_at_sequence {
                Arc::new(UInt64Array::from(version_values_for_selection(
                    sequence,
                    &config.params,
                    batch_offset,
                    num_rows,
                )?))
            } else {
                // Default to version 1 if sequence not provided
                Arc::new(UInt64Array::from(vec![1u64; num_rows as usize]))
            };
            batch =
                batch.try_with_column(ROW_LAST_UPDATED_AT_VERSION_FIELD.clone(), version_arr)?;
        }

        if config.with_row_created_at_version {
            let version_arr = if let Some(sequence) = &config.created_at_sequence {
                Arc::new(UInt64Array::from(version_values_for_selection(
                    sequence,
                    &config.params,
                    batch_offset,
                    num_rows,
                )?))
            } else {
                // Default to version 1 if sequence not provided
                Arc::new(UInt64Array::from(vec![1u64; num_rows as usize]))
            };
            batch = batch.try_with_column(ROW_CREATED_AT_VERSION_FIELD.clone(), version_arr)?;
        }

        batch
    } else {
        batch
    };

    match (deletion_mask, config.make_deletions_null) {
        (None, _) => Ok(batch),
        (Some(mask), false) => Ok(arrow::compute::filter_record_batch(&batch, &mask)?),
        (Some(mask), true) => Ok(apply_deletions_as_nulls(batch, &mask)?),
    }
}

/// Given a stream of batch tasks this function will add a row ids column (if requested)
/// and also apply a deletions vector to the batch.
///
/// This converts from BatchTaskStream to BatchFutStream because, if we are applying a
/// deletion vector, it is impossible to know how many output rows we will have.
pub fn wrap_with_row_id_and_delete(
    stream: ReadBatchTaskStream,
    fragment_id: u32,
    config: RowIdAndDeletesConfig,
) -> ReadBatchFutStream {
    let config = Arc::new(config);
    let mut offset = 0;
    stream
        .map(move |batch_task| {
            let config = config.clone();
            let this_offset = offset;
            let num_rows = batch_task.num_rows;
            offset += num_rows;
            batch_task
                .task
                .map(move |batch| {
                    apply_row_id_and_deletes(batch?, this_offset, fragment_id, config.as_ref())
                })
                .boxed()
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{array::AsArray, datatypes::UInt64Type};
    use arrow_array::{RecordBatch, UInt32Array, types::Int32Type};
    use arrow_schema::ArrowError;
    use futures::{
        FutureExt, StreamExt, TryStreamExt,
        stream::{self, BoxStream},
    };
    use lance_core::{
        ROW_ID,
        utils::{address::RowAddress, deletion::DeletionVector},
    };
    use lance_datagen::{BatchCount, RowCount};
    use lance_io::{ReadBatchParams, stream::arrow_stream_to_lance_stream};
    use roaring::RoaringBitmap;

    use crate::utils::stream::ReadBatchTask;

    use super::RowIdAndDeletesConfig;

    fn batch_task_stream(
        datagen_stream: BoxStream<'static, std::result::Result<RecordBatch, ArrowError>>,
    ) -> super::ReadBatchTaskStream {
        arrow_stream_to_lance_stream(datagen_stream)
            .map(|batch| ReadBatchTask {
                num_rows: batch.as_ref().unwrap().num_rows() as u32,
                task: std::future::ready(batch).boxed(),
            })
            .boxed()
    }

    #[tokio::test]
    async fn test_basic_zip() {
        let left = batch_task_stream(
            lance_datagen::gen_batch()
                .col("x", lance_datagen::array::step::<Int32Type>())
                .into_reader_stream(RowCount::from(100), BatchCount::from(10))
                .0,
        );
        let right = batch_task_stream(
            lance_datagen::gen_batch()
                .col("y", lance_datagen::array::step::<Int32Type>())
                .into_reader_stream(RowCount::from(100), BatchCount::from(10))
                .0,
        );

        let merged = super::merge_streams(vec![left, right])
            .map(|batch_task| batch_task.task)
            .buffered(1)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let expected = lance_datagen::gen_batch()
            .col("x", lance_datagen::array::step::<Int32Type>())
            .col("y", lance_datagen::array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(100), BatchCount::from(10))
            .collect::<Result<Vec<_>, ArrowError>>()
            .unwrap();
        assert_eq!(merged, expected);
    }

    #[tokio::test]
    async fn test_zip_with_different_batch_boundaries() {
        let left_batch =
            arrow_array::record_batch!(("x", Int32, (0..10).collect::<Vec<_>>())).unwrap();
        let right_batch =
            arrow_array::record_batch!(("y", Int32, (10..20).collect::<Vec<_>>())).unwrap();
        let left = batch_task_stream(
            stream::iter([Ok(left_batch.slice(0, 6)), Ok(left_batch.slice(6, 4))]).boxed(),
        );
        let right = batch_task_stream(
            stream::iter([Ok(right_batch.slice(0, 4)), Ok(right_batch.slice(4, 6))]).boxed(),
        );

        let merged = super::merge_streams(vec![left, right])
            .map(|batch_task| batch_task.task)
            .buffered(3)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let expected = vec![
            arrow_array::record_batch!(
                ("x", Int32, (0..4).collect::<Vec<_>>()),
                ("y", Int32, (10..14).collect::<Vec<_>>())
            )
            .unwrap(),
            arrow_array::record_batch!(
                ("x", Int32, (4..6).collect::<Vec<_>>()),
                ("y", Int32, (14..16).collect::<Vec<_>>())
            )
            .unwrap(),
            arrow_array::record_batch!(
                ("x", Int32, (6..10).collect::<Vec<_>>()),
                ("y", Int32, (16..20).collect::<Vec<_>>())
            )
            .unwrap(),
        ];
        assert_eq!(merged, expected);
    }

    async fn check_row_id(params: ReadBatchParams, expected: impl IntoIterator<Item = u32>) {
        let expected = Vec::from_iter(expected);

        for has_columns in [false, true] {
            for fragment_id in [0, 10] {
                // 100 rows across 10 batches of 10 rows
                let mut datagen = lance_datagen::gen_batch();
                if has_columns {
                    datagen = datagen.col("x", lance_datagen::array::rand::<Int32Type>());
                }
                let data = batch_task_stream(
                    datagen
                        .into_reader_stream(RowCount::from(10), BatchCount::from(10))
                        .0,
                );

                let config = RowIdAndDeletesConfig {
                    params: params.clone(),
                    with_row_id: true,
                    with_row_addr: false,
                    with_row_last_updated_at_version: false,
                    with_row_created_at_version: false,
                    deletion_vector: None,
                    row_id_sequence: None,
                    last_updated_at_sequence: None,
                    created_at_sequence: None,
                    make_deletions_null: false,
                    total_num_rows: 100,
                };
                let stream = super::wrap_with_row_id_and_delete(data, fragment_id, config);
                let batches = stream.buffered(1).try_collect::<Vec<_>>().await.unwrap();

                let mut offset = 0;
                let expected = expected.clone();
                for batch in batches {
                    let actual_row_ids =
                        batch[ROW_ID].as_primitive::<UInt64Type>().values().to_vec();
                    let expected_row_ids = expected[offset..offset + 10]
                        .iter()
                        .map(|row_offset| {
                            RowAddress::new_from_parts(fragment_id, *row_offset).into()
                        })
                        .collect::<Vec<u64>>();
                    assert_eq!(actual_row_ids, expected_row_ids);
                    offset += batch.num_rows();
                }
            }
        }
    }

    #[tokio::test]
    async fn test_row_id() {
        let some_indices = (0..100).rev().collect::<Vec<u32>>();
        let some_indices_arr = UInt32Array::from(some_indices.clone());
        check_row_id(ReadBatchParams::RangeFull, 0..100).await;
        check_row_id(ReadBatchParams::Indices(some_indices_arr), some_indices).await;
        check_row_id(ReadBatchParams::Range(1000..1100), 1000..1100).await;
        check_row_id(
            ReadBatchParams::RangeFrom(std::ops::RangeFrom { start: 1000 }),
            1000..1100,
        )
        .await;
        check_row_id(
            ReadBatchParams::RangeTo(std::ops::RangeTo { end: 1000 }),
            0..100,
        )
        .await;
    }

    #[tokio::test]
    async fn test_deletes() {
        let no_deletes: Option<Arc<DeletionVector>> = None;
        let no_deletes_2 = Some(Arc::new(DeletionVector::NoDeletions));
        let delete_some_bitmap = Some(Arc::new(DeletionVector::Bitmap(RoaringBitmap::from_iter(
            0..35,
        ))));
        let delete_some_set = Some(Arc::new(DeletionVector::Set((0..35).collect())));

        for deletion_vector in [
            no_deletes,
            no_deletes_2,
            delete_some_bitmap,
            delete_some_set,
        ] {
            for has_columns in [false, true] {
                for with_row_id in [false, true] {
                    for make_deletions_null in [false, true] {
                        for frag_id in [0, 1] {
                            let has_deletions = if let Some(dv) = &deletion_vector {
                                !matches!(dv.as_ref(), DeletionVector::NoDeletions)
                            } else {
                                false
                            };
                            if !has_columns && !has_deletions && !with_row_id {
                                // This is an invalid case and should be prevented upstream,
                                // no meaningful work is being done!
                                continue;
                            }
                            if make_deletions_null && !with_row_id {
                                // This is an invalid case and should be prevented upstream
                                // we cannot make the row_id column null if it isn't present
                                continue;
                            }

                            let mut datagen = lance_datagen::gen_batch();
                            if has_columns {
                                datagen =
                                    datagen.col("x", lance_datagen::array::rand::<Int32Type>());
                            }
                            // 100 rows across 10 batches of 10 rows
                            let data = batch_task_stream(
                                datagen
                                    .into_reader_stream(RowCount::from(10), BatchCount::from(10))
                                    .0,
                            );

                            let config = RowIdAndDeletesConfig {
                                params: ReadBatchParams::RangeFull,
                                with_row_id,
                                with_row_addr: false,
                                with_row_last_updated_at_version: false,
                                with_row_created_at_version: false,
                                deletion_vector: deletion_vector.clone(),
                                row_id_sequence: None,
                                last_updated_at_sequence: None,
                                created_at_sequence: None,
                                make_deletions_null,
                                total_num_rows: 100,
                            };
                            let stream = super::wrap_with_row_id_and_delete(data, frag_id, config);
                            let batches = stream
                                .buffered(1)
                                .filter_map(|batch| {
                                    std::future::ready(
                                        batch
                                            .map(|batch| {
                                                if batch.num_rows() == 0 {
                                                    None
                                                } else {
                                                    Some(batch)
                                                }
                                            })
                                            .transpose(),
                                    )
                                })
                                .try_collect::<Vec<_>>()
                                .await
                                .unwrap();

                            let total_num_rows =
                                batches.iter().map(|b| b.num_rows()).sum::<usize>();
                            let total_num_nulls = if make_deletions_null {
                                batches
                                    .iter()
                                    .map(|b| b[ROW_ID].null_count())
                                    .sum::<usize>()
                            } else {
                                0
                            };
                            let total_actually_deleted = total_num_nulls + (100 - total_num_rows);

                            let expected_deletions = match &deletion_vector {
                                None => 0,
                                Some(deletion_vector) => match deletion_vector.as_ref() {
                                    DeletionVector::NoDeletions => 0,
                                    DeletionVector::Bitmap(b) => b.len() as usize,
                                    DeletionVector::Set(s) => s.len(),
                                },
                            };
                            assert_eq!(total_actually_deleted, expected_deletions);
                            if expected_deletions > 0 && with_row_id {
                                if make_deletions_null {
                                    // If we make deletions null we get 3 batches of all-null and then
                                    // a batch of half-null
                                    assert_eq!(
                                        batches[3][ROW_ID].as_primitive::<UInt64Type>().value(0),
                                        u64::from(RowAddress::new_from_parts(frag_id, 30))
                                    );
                                    assert_eq!(batches[3][ROW_ID].null_count(), 5);
                                } else {
                                    // If we materialize deletions the first row will be 35
                                    assert_eq!(
                                        batches[0][ROW_ID].as_primitive::<UInt64Type>().value(0),
                                        u64::from(RowAddress::new_from_parts(frag_id, 35))
                                    );
                                }
                            }
                            if !with_row_id {
                                assert!(batches[0].column_by_name(ROW_ID).is_none());
                            }
                        }
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_version_column_with_deletions() {
        use crate::rowids::segment::U64Segment;
        use crate::rowids::version::{RowDatasetVersionRun, RowDatasetVersionSequence};

        let seq = Arc::new(RowDatasetVersionSequence {
            runs: vec![RowDatasetVersionRun {
                span: U64Segment::Range(0..100),
                version: 42,
            }],
        });

        let data = batch_task_stream(
            lance_datagen::gen_batch()
                .col("x", lance_datagen::array::rand::<Int32Type>())
                .into_reader_stream(RowCount::from(10), BatchCount::from(10))
                .0,
        );

        let config = RowIdAndDeletesConfig {
            params: ReadBatchParams::RangeFull,
            with_row_id: true,
            with_row_addr: false,
            with_row_last_updated_at_version: false,
            with_row_created_at_version: true,
            deletion_vector: Some(Arc::new(DeletionVector::Bitmap(RoaringBitmap::from_iter(
                0..35,
            )))),
            row_id_sequence: None,
            last_updated_at_sequence: None,
            created_at_sequence: Some(seq),
            make_deletions_null: false,
            total_num_rows: 100,
        };
        let stream = super::wrap_with_row_id_and_delete(data, 0, config);
        let batches: Vec<_> = stream
            .buffered(1)
            .try_filter(|b| std::future::ready(b.num_rows() > 0))
            .try_collect()
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 65);

        for batch in &batches {
            let versions = batch
                .column_by_name("_row_created_at_version")
                .unwrap()
                .as_primitive::<UInt64Type>()
                .values();
            assert!(versions.iter().all(|&v| v == 42));
        }
    }

    #[tokio::test]
    async fn test_version_column_multi_run() {
        use crate::rowids::segment::U64Segment;
        use crate::rowids::version::{RowDatasetVersionRun, RowDatasetVersionSequence};

        // 3 runs: 0..40 v1, 40..70 v2, 70..100 v3
        let seq = Arc::new(RowDatasetVersionSequence {
            runs: vec![
                RowDatasetVersionRun {
                    span: U64Segment::Range(0..40),
                    version: 1,
                },
                RowDatasetVersionRun {
                    span: U64Segment::Range(40..70),
                    version: 2,
                },
                RowDatasetVersionRun {
                    span: U64Segment::Range(70..100),
                    version: 3,
                },
            ],
        });

        // Delete 0..20 and 60..80 (spans run boundary).
        // Survivors: 20..40 (v1), 40..60 (v2), 80..100 (v3) = 60 rows
        let mut deletions = RoaringBitmap::from_iter(0..20);
        deletions.extend(60..80);

        let data = batch_task_stream(
            lance_datagen::gen_batch()
                .col("x", lance_datagen::array::rand::<Int32Type>())
                .into_reader_stream(RowCount::from(10), BatchCount::from(10))
                .0,
        );

        let config = RowIdAndDeletesConfig {
            params: ReadBatchParams::RangeFull,
            with_row_id: true,
            with_row_addr: false,
            with_row_last_updated_at_version: false,
            with_row_created_at_version: true,
            deletion_vector: Some(Arc::new(DeletionVector::Bitmap(deletions))),
            row_id_sequence: None,
            last_updated_at_sequence: None,
            created_at_sequence: Some(seq),
            make_deletions_null: false,
            total_num_rows: 100,
        };
        let stream = super::wrap_with_row_id_and_delete(data, 0, config);
        let batches: Vec<_> = stream
            .buffered(1)
            .try_filter(|b| std::future::ready(b.num_rows() > 0))
            .try_collect()
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 60);

        let all_versions: Vec<u64> = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("_row_created_at_version")
                    .unwrap()
                    .as_primitive::<UInt64Type>()
                    .values()
                    .to_vec()
            })
            .collect();

        assert!(all_versions[..20].iter().all(|&v| v == 1));
        assert!(all_versions[20..40].iter().all(|&v| v == 2));
        assert!(all_versions[40..60].iter().all(|&v| v == 3));
    }
}
