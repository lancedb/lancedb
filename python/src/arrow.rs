// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;
use std::time::Duration;

use crate::error::PythonErrorExt;
use crate::runtime::future_into_py;
use arrow::{
    array::RecordBatch,
    datatypes::SchemaRef,
    pyarrow::{IntoPyArrow, ToPyArrow},
    record_batch::{RecordBatchIterator, RecordBatchReader as ArrowRecordBatchReader},
};
use futures::{TryStreamExt, stream::StreamExt};
use lancedb::{
    arrow::SendableRecordBatchStream,
    query::{ExecutableQuery, QueryExecutionOptions},
};
use pyo3::{
    Bound, Py, PyAny, PyRef, PyResult, Python,
    exceptions::{PyRuntimeError, PyStopAsyncIteration},
    pyclass, pymethods,
};

#[pyclass]
pub struct RecordBatchStream {
    schema: SchemaRef,
    inner: Arc<tokio::sync::Mutex<Option<SendableRecordBatchStream>>>,
}

impl RecordBatchStream {
    pub fn new(inner: SendableRecordBatchStream) -> Self {
        let schema = inner.schema().clone();
        Self {
            schema,
            inner: Arc::new(tokio::sync::Mutex::new(Some(inner))),
        }
    }

    async fn next_batch(
        inner: &tokio::sync::Mutex<Option<SendableRecordBatchStream>>,
    ) -> PyResult<RecordBatch> {
        let mut slot = inner.lock().await;
        // Serialize reads with the mutex, but let this future own the stream.
        // Cancellation must drop the parked execution and its cache loader guards
        // even if Python keeps the reader alive. An empty slot marks cancellation.
        let mut stream = slot
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("Record batch stream was cancelled"))?;
        let next = stream.next().await;
        *slot = Some(stream);
        drop(slot);
        next.ok_or_else(|| PyStopAsyncIteration::new_err(""))?
            .infer_error()
    }
}

#[pymethods]
impl RecordBatchStream {
    #[getter]
    pub fn schema(&self, py: Python) -> PyResult<Py<PyAny>> {
        (*self.schema)
            .clone()
            .into_pyarrow(py)
            .map(|obj| obj.unbind())
    }

    pub fn __aiter__(self_: PyRef<'_, Self>) -> PyRef<'_, Self> {
        self_
    }

    pub fn __anext__(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let batch = Self::next_batch(&inner).await?;
            Python::attach(|py| batch.to_pyarrow(py).map(|obj| obj.unbind()))
        })
    }
}

/// Execute `query` and collect every batch in Rust before returning to Python.
///
/// `to_arrow()` needs the complete table, so the default 1024-row output
/// slicing only adds work. Returning an Arrow C Stream reader also avoids a
/// Python future per batch while python materializes the table.
///
/// The stream is collected inside the returned future, so timeout and
/// cancellation keep the same observable behavior as reading the stream from
/// Python: errors surface from the awaited call and dropping the future drops
/// the underlying stream.
pub fn execute_to_reader<Q>(
    py: Python<'_>,
    query: Q,
    max_batch_length: Option<u32>,
    timeout: Option<Duration>,
) -> PyResult<Bound<'_, PyAny>>
where
    Q: ExecutableQuery + Send + 'static,
{
    future_into_py(py, async move {
        let mut options = QueryExecutionOptions::default();
        // `None` means the caller wants one eager result, not the core's
        // default batch size. An explicit value is still honored.
        options.max_batch_length = max_batch_length.unwrap_or(0);
        options.timeout = timeout;
        let stream = query.execute_with_options(options).await.infer_error()?;
        let schema = stream.schema();
        let batches = stream.try_collect::<Vec<_>>().await.infer_error()?;
        let reader: Box<dyn ArrowRecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema,
        ));
        Python::attach(|py| reader.into_pyarrow(py).map(|obj| obj.unbind()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use futures::{FutureExt, stream};
    use lancedb::arrow::SimpleRecordBatchStream;
    use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods};
    use tokio::sync::oneshot;

    struct NotifyOnDrop(Py<PyAny>);

    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            Python::attach(|py| {
                self.0.bind(py).call_method0("set").unwrap();
            });
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_read_drops_stream_while_reader_is_retained() {
        Python::initialize();
        Python::attach(|py| {
            let threading = py.import("threading").unwrap();
            let polled = threading.call_method0("Event").unwrap();
            let released = threading.call_method0("Event").unwrap();
            let on_poll = polled.clone().unbind();
            let on_drop = NotifyOnDrop(released.clone().unbind());
            // Model a cache loader guard held inside the stream's first poll.
            let stream = stream::once(async move {
                let _guard = on_drop;
                Python::attach(|py| {
                    on_poll.bind(py).call_method0("set").unwrap();
                });
                futures::future::pending::<lancedb::Result<RecordBatch>>().await
            });
            let reader = RecordBatchStream::new(Box::pin(SimpleRecordBatchStream::new(
                stream,
                Arc::new(Schema::empty()),
            )));
            let locals = PyDict::new(py);
            locals
                .set_item("reader", Py::new(py, reader).unwrap())
                .unwrap();
            locals.set_item("polled", polled).unwrap();
            locals.set_item("released", released).unwrap();
            py.run(
                c"import asyncio

async def read():
    return await reader.__anext__()

async def check():
    task = asyncio.create_task(read())
    assert await asyncio.to_thread(polled.wait, 5), 'stream was never polled'
    assert task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError('read was not cancelled')
    # Keep both reader and task alive: cancellation must release the parked
    # execution without relying on Python reference counting or collection.
    assert await asyncio.to_thread(released.wait, 5), 'stream was retained after cancellation'
    try:
        await reader.__anext__()
    except RuntimeError as error:
        assert 'cancelled' in str(error)
    else:
        raise AssertionError('cancelled stream did not raise RuntimeError')

asyncio.run(check())",
                Some(&locals),
                None,
            )
            .unwrap();
        });
    }

    #[tokio::test]
    async fn cancelling_a_waiting_read_preserves_the_active_read() {
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(schema.clone());
        let expected = batch.clone();
        let (send, receive) = oneshot::channel();
        let stream = stream::once(async move {
            receive.await.unwrap();
            Ok(batch)
        });
        let reader = RecordBatchStream::new(Box::pin(SimpleRecordBatchStream::new(stream, schema)));

        let mut active = Box::pin(RecordBatchStream::next_batch(&reader.inner));
        assert!(futures::poll!(active.as_mut()).is_pending());
        let mut waiting = Box::pin(RecordBatchStream::next_batch(&reader.inner));
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        drop(waiting);

        send.send(()).unwrap();
        assert_eq!(active.await.unwrap(), expected);
    }

    #[tokio::test]
    async fn completed_reads_preserve_remaining_batches_and_exhaustion() {
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(schema.clone());
        let stream = stream::iter([Ok(batch.clone()), Ok(batch.clone())]);
        let reader = RecordBatchStream::new(Box::pin(SimpleRecordBatchStream::new(stream, schema)));

        for _ in 0..2 {
            assert_eq!(
                RecordBatchStream::next_batch(&reader.inner).await.unwrap(),
                batch
            );
        }
        Python::initialize();
        for _ in 0..2 {
            let error = RecordBatchStream::next_batch(&reader.inner)
                .now_or_never()
                .expect("an exhausted stream must not wait")
                .unwrap_err();
            Python::attach(|py| assert!(error.is_instance_of::<PyStopAsyncIteration>(py)));
        }
    }
}
