// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{
    io::{BufReader, BufWriter},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Schema, SchemaRef};
use datafusion::{
    catalog::{TableProvider, streaming::StreamingTable},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{stream::RecordBatchStreamAdapter, streaming::PartitionStream},
};
use datafusion_common::DataFusionError;
use futures::StreamExt;
use lance_arrow::memory::MemoryAccumulator;
use lance_core::error::LanceOptionExt;
use lance_core::utils::tempfile::TempDir;

/// Start a spill of Arrow data to a file that can be read later multiple times.
///
/// Up to `memory_limit` bytes of data can be buffered in memory before a spill
/// is created. If the memory limit is never reached before [`SpillSender::finish()`]
/// is called, then the data will simply be kept in memory and no spill will be
/// created.
///
/// `path` is the path to the file that may be created. It should not already
/// exist. It is the responsibility of the caller to delete the file after it is
/// no longer needed.
///
/// The [`SpillSender`] allows you to write batches to the spill.
///
/// The [`SpillReceiver`] can open a [`SendableRecordBatchStream`] that reads
/// batches from the spill. This can be opened before, during, or after batches
/// have been written to the spill.
///
/// Once [`SpillSender`] is dropped, the temporary file is deleted. This will
/// cause the [`SpillReceiver`] to return an error if it is still open.
pub fn create_replay_spill(
    path: std::path::PathBuf,
    schema: Arc<Schema>,
    memory_limit: usize,
) -> (SpillSender, SpillReceiver) {
    let initial_status = WriteStatus::default();
    let (status_sender, status_receiver) = tokio::sync::watch::channel(initial_status);
    let sender = SpillSender {
        memory_limit,
        path: path.clone(),
        schema: schema.clone(),
        state: SpillState::default(),
        status_sender,
    };

    let receiver = SpillReceiver {
        status_receiver,
        path,
        schema,
    };

    (sender, receiver)
}

/// Wrap a one-shot [`SendableRecordBatchStream`] in a re-scannable [`TableProvider`].
///
/// The source is drained in the background into a replayable spill. Two properties
/// keep this cheap for the common case:
///
/// - **Memory-first.** Up to `memory_limit` bytes are buffered in memory; the spill
///   only touches disk once that budget is exceeded. A source that fits under the
///   limit never hits the filesystem.
/// - **Streaming replay.** A scan can start consuming batches as soon as they land,
///   before the source has finished draining — the first reader is not blocked
///   waiting for the whole source to buffer.
///
/// Each scan of the returned provider replays the full source, which is what makes a
/// one-shot stream usable in the write retry loop.
///
/// The provider reports no statistics — the source size is not known until it has
/// been fully drained — so callers that need source statistics (e.g. to drive join
/// ordering) should prefer a materialized or file-backed provider instead.
///
/// # Examples
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{Int32Array, RecordBatch};
/// # use arrow_schema::{DataType, Field, Schema};
/// # use datafusion::execution::SendableRecordBatchStream;
/// # use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
/// # use futures::TryStreamExt;
/// # use lance_datafusion::exec::provider_to_stream;
/// # use lance_datafusion::spill::spilling_table_provider;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
/// let batch =
///     RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])?;
/// // A one-shot stream can only be consumed once.
/// let source: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
///     schema.clone(),
///     futures::stream::iter(vec![Ok(batch)]),
/// ));
///
/// // Wrapping it makes it re-scannable: each scan replays the full source.
/// let provider = spilling_table_provider(source, 100 * 1024 * 1024).await?;
/// let first: Vec<RecordBatch> = provider_to_stream(provider.clone()).await?.try_collect().await?;
/// let second: Vec<RecordBatch> = provider_to_stream(provider).await?.try_collect().await?;
/// assert_eq!(first.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
/// assert_eq!(second.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
/// # Ok(())
/// # }
/// ```
pub async fn spilling_table_provider(
    mut source: SendableRecordBatchStream,
    memory_limit: usize,
) -> Result<Arc<dyn TableProvider>, DataFusionError> {
    let schema = source.schema();
    let tmp_dir = tokio::task::spawn_blocking(TempDir::try_new)
        .await
        .map_err(|e| DataFusionError::Execution(format!("Failed to spawn temp dir task: {e}")))?
        .map_err(|e| DataFusionError::Execution(format!("Failed to create temp dir: {e}")))?;
    let tmp_path = tmp_dir.std_path().join("spill.arrows");
    let (mut sender, receiver) = create_replay_spill(tmp_path, schema.clone(), memory_limit);

    // Drain the one-shot source into the spill once, in the background. The spill
    // tees to memory/disk so the first reader can consume batches as they arrive
    // while later readers replay the complete source.
    let drain_handle = tokio::task::spawn(async move {
        let mut errored = false;
        while let Some(res) = source.next().await {
            match res {
                Ok(batch) => {
                    if let Err(e) = sender.write(batch).await {
                        sender.send_error(e);
                        errored = true;
                        break;
                    }
                }
                Err(e) => {
                    sender.send_error(e);
                    errored = true;
                    break;
                }
            }
        }
        // Only finish on a clean drain. Calling finish() after an error would
        // overwrite the original (replayable) error with a generic one, losing
        // the source error's type (e.g. an external error from user code).
        if !errored && let Err(err) = sender.finish().await {
            sender.send_error(err);
        }
        sender
    });

    let partition = Arc::new(SpillPartition {
        schema: schema.clone(),
        receiver,
        _tmp_dir: Arc::new(tmp_dir),
        _drain_handle: Arc::new(drain_handle),
    });
    Ok(Arc::new(StreamingTable::try_new(schema, vec![partition])?))
}

/// A [`PartitionStream`] backed by a replayable spill.
///
/// Each call to [`PartitionStream::execute`] opens a fresh stream over the spill,
/// so the partition can be scanned repeatedly. The spill file and the background
/// task draining the source are kept alive for as long as this partition exists.
struct SpillPartition {
    schema: SchemaRef,
    receiver: SpillReceiver,
    // The spilled data lives in this temp dir; dropping it deletes the spill file.
    _tmp_dir: Arc<TempDir>,
    // Keeps the background drain task (which owns the `SpillSender`) alive. The
    // `SpillSender` must outlive the readers or they error out, so we hold the
    // handle rather than detaching it.
    _drain_handle: Arc<tokio::task::JoinHandle<SpillSender>>,
}

impl std::fmt::Debug for SpillPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpillPartition")
            .field("schema", &self.schema)
            .finish()
    }
}

impl PartitionStream for SpillPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        self.receiver.read()
    }
}

#[derive(Clone)]
pub struct SpillReceiver {
    status_receiver: tokio::sync::watch::Receiver<WriteStatus>,
    path: PathBuf,
    schema: Arc<Schema>,
}

impl SpillReceiver {
    /// Returns a stream of batches from the spill. The stream will emit
    /// batches as they are written to the spill. If the spill has already
    /// been finished, the stream will emit all batches in the spill.
    ///
    /// The stream will not complete until [`SpillSender::finish()`] is called.
    ///
    /// If the spill has been dropped, an error will be returned.
    pub fn read(&self) -> SendableRecordBatchStream {
        let rx = self.status_receiver.clone();
        let reader = SpillReader::new(rx, self.path.clone());

        let stream = futures::stream::try_unfold(reader, move |mut reader| async move {
            match reader.read().await {
                Ok(None) => Ok(None),
                Ok(Some(batch)) => Ok(Some((batch, reader))),
                Err(err) => Err(err),
            }
        });

        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

struct SpillReader {
    pub batches_read: usize,
    receiver: tokio::sync::watch::Receiver<WriteStatus>,
    state: SpillReaderState,
}

enum SpillReaderState {
    Buffered { spill_path: PathBuf },
    Reader { reader: AsyncStreamReader },
}

impl SpillReader {
    fn new(receiver: tokio::sync::watch::Receiver<WriteStatus>, spill_path: PathBuf) -> Self {
        Self {
            batches_read: 0,
            receiver,
            state: SpillReaderState::Buffered { spill_path },
        }
    }

    async fn wait_for_more_data(&mut self) -> Result<Option<Arc<[RecordBatch]>>, DataFusionError> {
        let status = self
            .receiver
            .wait_for(|status| {
                status.error.is_some()
                    || status.finished
                    || status.batches_written() > self.batches_read
            })
            .await
            .map_err(|_| {
                DataFusionError::Execution(
                    "Spill has been dropped before reader has finish.".into(),
                )
            })?;

        if let Some(error) = &status.error {
            let mut guard = error.lock().ok().expect_ok()?;
            return Err(DataFusionError::from(&mut (*guard)));
        }

        if let DataLocation::Buffered { batches } = &status.data_location {
            Ok(Some(batches.clone()))
        } else {
            Ok(None)
        }
    }

    async fn get_reader(&mut self) -> Result<&AsyncStreamReader, ArrowError> {
        if let SpillReaderState::Buffered { spill_path } = &self.state {
            let reader = AsyncStreamReader::open(spill_path.clone()).await?;
            // Skip batches we've already read before the writer started spilling.
            // The read batches were spilled to the file for the benefit of
            // future readers, as the spill is replay-able.
            for _ in 0..self.batches_read {
                reader.read().await?;
            }
            self.state = SpillReaderState::Reader { reader };
        }

        if let SpillReaderState::Reader { reader } = &mut self.state {
            Ok(reader)
        } else {
            unreachable!()
        }
    }

    async fn read(&mut self) -> Result<Option<RecordBatch>, DataFusionError> {
        let maybe_data = self.wait_for_more_data().await?;

        if let Some(batches) = maybe_data {
            if self.batches_read < batches.len() {
                let batch = batches[self.batches_read].clone();
                self.batches_read += 1;
                Ok(Some(batch))
            } else {
                Ok(None)
            }
        } else {
            let reader = self.get_reader().await?;
            let batch = reader.read().await?;
            if batch.is_some() {
                self.batches_read += 1;
            }
            Ok(batch)
        }
    }
}

/// The sender side of the spill. This is used to write batches to the spill.
///
/// Note: this must be kept alive until after the readers are done reading the
/// spill. Otherwise, they will return an error.
pub struct SpillSender {
    memory_limit: usize,
    schema: Arc<Schema>,
    path: PathBuf,
    state: SpillState,
    status_sender: tokio::sync::watch::Sender<WriteStatus>,
}

enum SpillState {
    Buffering {
        batches: Vec<RecordBatch>,
        memory_accumulator: MemoryAccumulator,
    },
    Spilling {
        writer: AsyncStreamWriter,
        batches_written: usize,
    },
    Finished {
        batches: Option<Arc<[RecordBatch]>>,
        batches_written: usize,
    },
    Errored {
        error: Arc<Mutex<SpillError>>,
    },
}

impl Default for SpillState {
    fn default() -> Self {
        Self::Buffering {
            batches: Vec::new(),
            memory_accumulator: MemoryAccumulator::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct WriteStatus {
    error: Option<Arc<Mutex<SpillError>>>,
    finished: bool,
    data_location: DataLocation,
}

impl WriteStatus {
    fn batches_written(&self) -> usize {
        match &self.data_location {
            DataLocation::Buffered { batches } => batches.len(),
            DataLocation::Spilled {
                batches_written, ..
            } => *batches_written,
        }
    }
}

#[derive(Clone, Debug)]
enum DataLocation {
    Buffered { batches: Arc<[RecordBatch]> },
    Spilled { batches_written: usize },
}

impl Default for DataLocation {
    fn default() -> Self {
        Self::Buffered {
            batches: Arc::new([]),
        }
    }
}

/// A DataFusion error that be be emitted multiple times. We provide the
/// Original error first, and subsequent conversions provide a copy with a
/// string representation of the original error.
#[derive(Debug)]
enum SpillError {
    Original(DataFusionError),
    Copy(DataFusionError),
}

impl From<DataFusionError> for SpillError {
    fn from(err: DataFusionError) -> Self {
        Self::Original(err)
    }
}

impl From<&mut SpillError> for DataFusionError {
    fn from(err: &mut SpillError) -> Self {
        match err {
            SpillError::Original(inner) => {
                let copy = Self::Execution(inner.to_string());
                let original = std::mem::replace(err, SpillError::Copy(copy));
                if let SpillError::Original(inner) = original {
                    inner
                } else {
                    unreachable!()
                }
            }
            SpillError::Copy(Self::Execution(message)) => Self::Execution(message.clone()),
            _ => unreachable!(),
        }
    }
}

impl From<&SpillState> for WriteStatus {
    fn from(state: &SpillState) -> Self {
        match state {
            SpillState::Buffering { batches, .. } => Self {
                finished: false,
                data_location: DataLocation::Buffered {
                    batches: batches.clone().into(),
                },
                error: None,
            },
            SpillState::Spilling {
                batches_written, ..
            } => Self {
                finished: false,
                data_location: DataLocation::Spilled {
                    batches_written: *batches_written,
                },
                error: None,
            },
            SpillState::Finished {
                batches_written,
                batches,
            } => {
                let data_location = if let Some(batches) = batches {
                    DataLocation::Buffered {
                        batches: batches.clone(),
                    }
                } else {
                    DataLocation::Spilled {
                        batches_written: *batches_written,
                    }
                };
                Self {
                    finished: true,
                    data_location,
                    error: None,
                }
            }
            SpillState::Errored { error } => Self {
                finished: true,
                data_location: DataLocation::default(), // Doesn't matter.
                error: Some(error.clone()),
            },
        }
    }
}

impl SpillSender {
    /// Write a batch to the spill.  
    ///  
    /// If there is room in the `memory_limit` then the batch is queued.  
    /// If `memory_limit` is first encountered then all queued batches, and this one,  
    /// will be written to disk as part of this call.  
    /// If we are already spilling then the batch will be written to disk as part of this  
    /// call.
    pub async fn write(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        if let SpillState::Finished { .. } = self.state {
            return Err(DataFusionError::Execution(
                "Spill has already been finished".to_string(),
            ));
        }

        if let SpillState::Errored { .. } = &self.state {
            return Err(DataFusionError::Execution(
                "Spill has sent an error".to_string(),
            ));
        }

        let (writer, batches_written) = match &mut self.state {
            SpillState::Buffering {
                batches,
                memory_accumulator,
            } => {
                memory_accumulator.record_batch(&batch);

                if memory_accumulator.total() > self.memory_limit {
                    let writer =
                        AsyncStreamWriter::open(self.path.clone(), self.schema.clone()).await?;
                    let batches_written = batches.len();
                    for batch in batches.drain(..) {
                        writer.write(batch).await?;
                    }
                    self.state = SpillState::Spilling {
                        writer,
                        batches_written,
                    };
                    if let SpillState::Spilling {
                        writer,
                        batches_written,
                    } = &mut self.state
                    {
                        (writer, batches_written)
                    } else {
                        unreachable!()
                    }
                } else {
                    batches.push(batch);
                    self.status_sender
                        .send_replace(WriteStatus::from(&self.state));
                    return Ok(());
                }
            }
            SpillState::Spilling {
                writer,
                batches_written,
            } => (writer, batches_written),
            _ => unreachable!(),
        };

        writer.write(batch).await?;
        *batches_written += 1;
        self.status_sender
            .send_replace(WriteStatus::from(&self.state));

        Ok(())
    }

    /// Send an error to the spill. This will be sent to all readers of the
    /// spill.
    pub fn send_error(&mut self, err: DataFusionError) {
        let error = Arc::new(Mutex::new(err.into()));
        self.state = SpillState::Errored { error };
        self.status_sender
            .send_replace(WriteStatus::from(&self.state));
    }

    /// Complete the spill write. This will finalize the Arrow IPC stream file.
    /// The file will remain available for reading until the spill is dropped.
    pub async fn finish(&mut self) -> Result<(), DataFusionError> {
        // We create a temporary state to get an owned copy of current state.
        // Since we hold an exclusive reference to `self`, no one should be
        // able to see this temporary state.
        let tmp_state = SpillState::Finished {
            batches_written: 0,
            batches: None,
        };
        match std::mem::replace(&mut self.state, tmp_state) {
            SpillState::Buffering { batches, .. } => {
                let batches_written = batches.len();
                self.state = SpillState::Finished {
                    batches_written,
                    batches: Some(batches.into()),
                };
                self.status_sender
                    .send_replace(WriteStatus::from(&self.state));
            }
            SpillState::Spilling {
                writer,
                batches_written,
            } => {
                writer.finish().await?;
                self.state = SpillState::Finished {
                    batches_written,
                    batches: None,
                };
                self.status_sender
                    .send_replace(WriteStatus::from(&self.state));
            }
            SpillState::Finished { .. } => {
                return Err(DataFusionError::Execution(
                    "Spill has already been finished".to_string(),
                ));
            }
            SpillState::Errored { .. } => {
                return Err(DataFusionError::Execution(
                    "Spill has sent an error".to_string(),
                ));
            }
        };

        Ok(())
    }
}

/// An async wrapper around [`StreamWriter`]. Each call uses [`tokio::task::spawn_blocking`]
/// to spawn a blocking task to write the batch.
struct AsyncStreamWriter {
    writer: Arc<Mutex<StreamWriter<BufWriter<std::fs::File>>>>,
}

impl AsyncStreamWriter {
    pub async fn open(path: PathBuf, schema: Arc<Schema>) -> Result<Self, ArrowError> {
        let writer = tokio::task::spawn_blocking(move || {
            let file = std::fs::File::create(&path).map_err(ArrowError::from)?;
            let writer = BufWriter::new(file);
            StreamWriter::try_new(writer, &schema)
        })
        .await
        .unwrap()?;
        let writer = Arc::new(Mutex::new(writer));
        Ok(Self { writer })
    }

    pub async fn write(&self, batch: RecordBatch) -> Result<(), ArrowError> {
        let writer = self.writer.clone();
        tokio::task::spawn_blocking(move || {
            let mut writer = writer.lock().unwrap();
            writer.write(&batch)?;
            writer.flush()
        })
        .await
        .unwrap()
    }

    pub async fn finish(self) -> Result<(), ArrowError> {
        let writer = self.writer.clone();
        tokio::task::spawn_blocking(move || {
            let mut writer = writer.lock().unwrap();
            writer.finish()
        })
        .await
        .unwrap()
    }
}

struct AsyncStreamReader {
    reader: Arc<Mutex<StreamReader<BufReader<std::fs::File>>>>,
}

impl AsyncStreamReader {
    pub async fn open(path: PathBuf) -> Result<Self, ArrowError> {
        let reader = tokio::task::spawn_blocking(move || {
            let file = std::fs::File::open(&path).map_err(ArrowError::from)?;
            let reader = BufReader::new(file);
            StreamReader::try_new(reader, None)
        })
        .await
        .unwrap()?;
        let reader = Arc::new(Mutex::new(reader));
        Ok(Self { reader })
    }

    pub async fn read(&self) -> Result<Option<RecordBatch>, ArrowError> {
        let reader = self.reader.clone();
        tokio::task::spawn_blocking(move || {
            let mut reader = reader.lock().unwrap();
            reader.next()
        })
        .await
        .unwrap()
        .transpose()
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field};
    use futures::{StreamExt, TryStreamExt, poll};
    use lance_core::utils::tempfile::{TempStdFile, TempStdPath};

    use super::*;

    #[tokio::test]
    async fn test_spill() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batches = [
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![4, 5, 6]))],
            )
            .unwrap(),
        ];

        // Create a stream
        let path = TempStdFile::default();
        let (mut spill, receiver) = create_replay_spill(path.to_owned(), schema.clone(), 0);

        // We can open a reader prior to writing any data. No batches will be ready.
        let mut stream_before = receiver.read();
        let mut stream_before_next = stream_before.next();
        let poll_res = poll!(&mut stream_before_next);
        assert!(poll_res.is_pending());

        // If we write a batch, the existing reader can now receive it.
        spill.write(batches[0].clone()).await.unwrap();
        let stream_before_batch1 = stream_before_next
            .await
            .expect("Expected a batch")
            .expect("Expected no error");
        assert_eq!(&stream_before_batch1, &batches[0]);
        let mut stream_before_next = stream_before.next();
        let poll_res = poll!(&mut stream_before_next);
        assert!(poll_res.is_pending());

        // We can also open a ready while the spill is being written to. We can
        // retrieve batches written so far immediately.
        let mut stream_during = receiver.read();
        let stream_during_batch1 = stream_during
            .next()
            .await
            .expect("Expected a batch")
            .expect("Expected no error");
        assert_eq!(&stream_during_batch1, &batches[0]);
        let mut stream_during_next = stream_during.next();
        let poll_res = poll!(&mut stream_during_next);
        assert!(poll_res.is_pending());

        // Once we finish the spill, readers can get remaining batches and will
        // reach the end of the stream.
        spill.write(batches[1].clone()).await.unwrap();
        spill.finish().await.unwrap();

        let stream_before_batch2 = stream_before_next
            .await
            .expect("Expected a batch")
            .expect("Expected no error");
        assert_eq!(&stream_before_batch2, &batches[1]);
        assert!(stream_before.next().await.is_none());

        let stream_during_batch2 = stream_during_next
            .await
            .expect("Expected a batch")
            .expect("Expected no error");
        assert_eq!(&stream_during_batch2, &batches[1]);
        assert!(stream_during.next().await.is_none());

        // Can also start a reader after finishing.
        let stream_after = receiver.read();
        let stream_after_batches = stream_after.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(&stream_after_batches, &batches);

        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn test_spill_error() {
        // Create a spill
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let path = TempStdFile::default();
        let (mut spill, receiver) =
            create_replay_spill(path.as_ref().to_owned(), schema.clone(), 0);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        spill.write(batch.clone()).await.unwrap();

        let mut stream = receiver.read();
        let stream_batch = stream
            .next()
            .await
            .expect("Expected a batch")
            .expect("Expected no error");
        assert_eq!(&stream_batch, &batch);

        spill.send_error(DataFusionError::ResourcesExhausted("🥱".into()));
        let stream_error = stream
            .next()
            .await
            .expect("Expected an error")
            .expect_err("Expected an error");
        assert!(matches!(
            stream_error,
            DataFusionError::ResourcesExhausted(message) if message == "🥱"
        ));

        // If we try to write after sending an error, it should return an error.
        let err = spill.write(batch).await;
        assert!(matches!(
            err,
            Err(DataFusionError::Execution(message)) if message == "Spill has sent an error"
        ));

        // If we try to finish after sending an error, it should return an error.
        let err = spill.finish().await;
        assert!(matches!(
            err,
            Err(DataFusionError::Execution(message)) if message == "Spill has sent an error"
        ));

        // If we try to read after sending an error, it should return an error.
        let mut stream = receiver.read();
        let stream_error = stream
            .next()
            .await
            .expect("Expected an error")
            .expect_err("Expected an error");
        assert!(matches!(
            stream_error,
            DataFusionError::Execution(message) if message.contains("🥱")
        ));

        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn test_spill_buffered() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let path = TempStdPath::default();
        let memory_limit = 1024 * 1024; // 1 MiB
        let (mut spill, receiver) = create_replay_spill(path.clone(), schema.clone(), memory_limit);

        // 0.5 MB batch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1; (512 * 1024) / 4]))],
        )
        .unwrap();
        spill.write(batch.clone()).await.unwrap();
        assert!(!std::fs::exists(&path).unwrap());

        spill.finish().await.unwrap();
        assert!(!std::fs::exists(&path).unwrap());

        let mut stream = receiver.read();
        let stream_batch = stream
            .next()
            .await
            .expect("Expected a batch")
            .expect("Expected no error");
        assert_eq!(&stream_batch, &batch);

        assert!(!std::fs::exists(&path).unwrap());
    }

    #[tokio::test]
    async fn test_spill_buffered_transition() {
        // Starts as buffered, then spills, then finished.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let path = TempStdPath::default();
        let memory_limit = 1024 * 1024; // 1 MiB
        let (mut spill, receiver) = create_replay_spill(path.clone(), schema.clone(), memory_limit);

        // 0.7 MB batch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1; (768 * 1024) / 4]))],
        )
        .unwrap();
        spill.write(batch.clone()).await.unwrap();
        assert!(!std::fs::exists(&path).unwrap());

        let mut stream = receiver.read();
        let stream_batch = stream
            .next()
            .await
            .expect("Expected a batch")
            .expect("Expected no error");
        assert_eq!(&stream_batch, &batch);
        assert!(!std::fs::exists(&path).unwrap());

        // 0.5 MB batch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1; (512 * 1024) / 4]))],
        )
        .unwrap();
        spill.write(batch.clone()).await.unwrap();
        assert!(std::fs::exists(&path).unwrap());

        let stream_batch = stream
            .next()
            .await
            .expect("Expected a batch")
            .expect("Expected no error");
        assert_eq!(&stream_batch, &batch);
        assert!(std::fs::exists(&path).unwrap());

        spill.finish().await.unwrap();

        assert!(stream.next().await.is_none());

        std::fs::remove_file(path).unwrap();
    }
}
