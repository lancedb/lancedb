// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! A pull-driven synchronous reader over a query's record batch stream.
//!
//! The synchronous Python API used to drive one future per batch through the
//! shared background event loop. This reader keeps the stream in Rust and
//! fetches exactly one batch from the caller's thread for each `__next__`,
//! while the caller's GIL is released.
//!
//! The stream is stored only while the reader is idle. Taking it out for a
//! poll means an error, EOF, interrupt, or a dropped last owner releases the
//! execution plan immediately instead of leaving a reader task behind. A
//! successful batch puts the stream back so an early `break` can resume
//! reading from the same cursor.

use std::sync::Mutex;

use arrow::datatypes::SchemaRef;
use arrow::pyarrow::{IntoPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use lancedb::arrow::SendableRecordBatchStream;
use pyo3::{Py, PyAny, PyRef, PyResult, Python, pyclass, pymethods};

use crate::error::PythonErrorExt;
use crate::runtime::block_on_interruptible;

/// Synchronous iterator over the batches of one query execution.
///
/// Python wraps this in [`pyarrow.RecordBatchReader.from_batches`] so the
/// public return type, schema capsule, and exception types stay the same as
/// the existing sync reader. Creating the iterator does not poll the stream;
/// the first `__next__` starts any configured query timeout.
#[pyclass]
pub struct SyncRecordBatchStream {
    schema: SchemaRef,
    stream: Mutex<Option<SendableRecordBatchStream>>,
}

impl SyncRecordBatchStream {
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        let schema = stream.schema();
        Self {
            schema,
            stream: Mutex::new(Some(stream)),
        }
    }

    /// Take the stream and hand it to one owned, one-shot read future.
    ///
    /// The future owns the stream so an interrupt aborts it and drops the
    /// execution plan with the future. A successful batch returns the stream
    /// to the slot, which keeps an early `break` resumable. EOF and read
    /// errors drop the stream before returning. The lock is held only while
    /// taking or storing the stream, never while waiting, so another Python
    /// thread cannot deadlock against a read that needs the GIL.
    fn pull(&mut self) -> PyResult<Option<RecordBatch>> {
        match self.pull_with(|mut stream| {
            block_on_interruptible(async move {
                let next = stream.next().await;
                Ok::<_, pyo3::PyErr>((stream, next))
            })
        })? {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(error)) => Err(into_py_error(error)),
            None => Ok(None),
        }
    }

    /// Shared state machine for one pull, with the wait injected for tests.
    fn pull_with<F>(
        &mut self,
        fetch: F,
    ) -> Result<Option<Result<RecordBatch, lancedb::Error>>, pyo3::PyErr>
    where
        F: FnOnce(
            SendableRecordBatchStream,
        ) -> PyResult<(
            SendableRecordBatchStream,
            Option<Result<RecordBatch, lancedb::Error>>,
        )>,
    {
        let Some(stream) = self
            .stream
            .lock()
            .expect("sync reader mutex poisoned")
            .take()
        else {
            return Ok(None);
        };
        let (stream, next) = fetch(stream)?;
        match next {
            Some(Ok(batch)) => {
                *self.stream.lock().expect("sync reader mutex poisoned") = Some(stream);
                Ok(Some(Ok(batch)))
            }
            Some(Err(error)) => {
                drop(stream);
                Ok(Some(Err(error)))
            }
            None => {
                drop(stream);
                Ok(None)
            }
        }
    }

    fn release(&self) {
        *self.stream.lock().expect("sync reader mutex poisoned") = None;
    }
}

#[pymethods]
impl SyncRecordBatchStream {
    #[getter]
    pub fn schema(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        (*self.schema)
            .clone()
            .into_pyarrow(py)
            .map(|obj| obj.unbind())
    }

    fn __iter__(self_: PyRef<'_, Self>) -> PyRef<'_, Self> {
        self_
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let batch = py.detach(|| self.pull())?;
        let Some(batch) = batch else {
            return Ok(None);
        };
        let batch = match batch.to_pyarrow(py) {
            Ok(batch) => batch,
            Err(error) => {
                self.release();
                return Err(error);
            }
        };
        // A signal arriving after the fetch still terminates the reader, so
        // no execution plan outlives the KeyboardInterrupt.
        if let Err(error) = py.check_signals() {
            self.release();
            return Err(error);
        }
        Ok(Some(batch.unbind()))
    }
}

fn into_py_error(error: lancedb::Error) -> pyo3::PyErr {
    match Err::<(), _>(error).infer_error() {
        Ok(()) => unreachable!("infer_error returned Ok for an Err value"),
        Err(error) => error,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::Stream;
    use lancedb::arrow::RecordBatchStream as LanceRecordBatchStream;

    use super::*;

    struct TestStream {
        schema: SchemaRef,
        batches: VecDeque<Result<RecordBatch, lancedb::Error>>,
        polls: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    }

    impl TestStream {
        fn new(batches: Vec<Result<RecordBatch, lancedb::Error>>) -> Self {
            let schema = test_schema();
            Self {
                schema,
                batches: batches.into(),
                polls: Arc::new(AtomicUsize::new(0)),
                dropped: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    impl Stream for TestStream {
        type Item = Result<RecordBatch, lancedb::Error>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(self.batches.pop_front())
        }
    }

    impl LanceRecordBatchStream for TestStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    impl Drop for TestStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn test_batch(value: i32) -> RecordBatch {
        RecordBatch::try_new(test_schema(), vec![Arc::new(Int32Array::from(vec![value]))]).unwrap()
    }

    fn boxed(stream: TestStream) -> SendableRecordBatchStream {
        Box::pin(stream)
    }

    fn pull_fake(
        reader: &mut SyncRecordBatchStream,
    ) -> Result<Option<Result<RecordBatch, lancedb::Error>>, pyo3::PyErr> {
        reader.pull_with(|mut stream| {
            let next = futures::executor::block_on(stream.next());
            Ok((stream, next))
        })
    }

    #[test]
    fn creation_does_not_poll() {
        let stream = TestStream::new(vec![Ok(test_batch(1))]);
        let polls = Arc::clone(&stream.polls);
        let _reader = SyncRecordBatchStream::new(boxed(stream));
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn one_pull_reads_one_batch_and_keeps_the_stream_resumable() {
        let stream = TestStream::new(vec![
            Ok(test_batch(1)),
            Ok(test_batch(2)),
            Ok(test_batch(3)),
        ]);
        let polls = Arc::clone(&stream.polls);
        let dropped = Arc::clone(&stream.dropped);
        let mut reader = SyncRecordBatchStream::new(boxed(stream));

        assert!(pull_fake(&mut reader).unwrap().unwrap().is_ok());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert!(!dropped.load(Ordering::SeqCst));

        assert!(pull_fake(&mut reader).unwrap().unwrap().is_ok());
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert!(!dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn eof_drops_the_stream_before_returning() {
        let stream = TestStream::new(vec![Ok(test_batch(1))]);
        let dropped = Arc::clone(&stream.dropped);
        let mut reader = SyncRecordBatchStream::new(boxed(stream));

        assert!(pull_fake(&mut reader).unwrap().unwrap().is_ok());
        assert!(!dropped.load(Ordering::SeqCst));
        assert!(pull_fake(&mut reader).unwrap().is_none());
        assert!(dropped.load(Ordering::SeqCst));
        assert!(pull_fake(&mut reader).unwrap().is_none());
    }

    #[test]
    fn error_drops_the_stream_before_returning() {
        let stream = TestStream::new(vec![Err(lancedb::Error::Runtime {
            message: "read failed".to_string(),
        })]);
        let dropped = Arc::clone(&stream.dropped);
        let mut reader = SyncRecordBatchStream::new(boxed(stream));

        assert!(pull_fake(&mut reader).unwrap().unwrap().is_err());
        assert!(dropped.load(Ordering::SeqCst));
        assert!(pull_fake(&mut reader).unwrap().is_none());
    }

    #[test]
    fn dropping_an_unread_reader_drops_the_stream_without_polling() {
        let stream = TestStream::new(vec![Ok(test_batch(1))]);
        let polls = Arc::clone(&stream.polls);
        let dropped = Arc::clone(&stream.dropped);
        let reader = SyncRecordBatchStream::new(boxed(stream));

        drop(reader);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert!(dropped.load(Ordering::SeqCst));
    }
}
