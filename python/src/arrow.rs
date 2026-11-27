// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;
use std::time::Duration;

use crate::error::PythonErrorExt;
use crate::runtime::{block_on_interruptible, future_into_py};
use arrow::{
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
    Bound, Py, PyAny, PyRef, PyResult, Python, exceptions::PyStopAsyncIteration, pyclass, pymethods,
};

#[pyclass]
pub struct RecordBatchStream {
    schema: SchemaRef,
    inner: Arc<tokio::sync::Mutex<SendableRecordBatchStream>>,
}

impl RecordBatchStream {
    pub fn new(inner: SendableRecordBatchStream) -> Self {
        let schema = inner.schema().clone();
        Self {
            schema,
            inner: Arc::new(tokio::sync::Mutex::new(inner)),
        }
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
            let inner_next = inner
                .lock()
                .await
                .next()
                .await
                .ok_or_else(|| PyStopAsyncIteration::new_err(""))?;
            Python::attach(|py| {
                inner_next
                    .infer_error()?
                    .to_pyarrow(py)
                    .map(|obj| obj.unbind())
            })
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
        let reader = collect_to_reader(query, max_batch_length, timeout).await?;
        Python::attach(|py| reader.into_pyarrow(py).map(|obj| obj.unbind()))
    })
}

/// Synchronous counterpart of [`execute_to_reader`].
///
/// The returned reader owns the collected batches; the caller converts it to
/// Python after reattaching to the interpreter. The wait releases the GIL and
/// keeps Python signal handlers on the calling thread.
pub fn execute_to_reader_sync<Q>(
    query: Q,
    max_batch_length: Option<u32>,
    timeout: Option<Duration>,
) -> PyResult<Box<dyn ArrowRecordBatchReader + Send>>
where
    Q: ExecutableQuery + Send + 'static,
{
    block_on_interruptible(collect_to_reader(query, max_batch_length, timeout))
}

async fn collect_to_reader<Q>(
    query: Q,
    max_batch_length: Option<u32>,
    timeout: Option<Duration>,
) -> PyResult<Box<dyn ArrowRecordBatchReader + Send>>
where
    Q: ExecutableQuery + Send + 'static,
{
    let mut options = QueryExecutionOptions::default();
    // `None` means the caller wants one eager result, not the core's
    // default batch size. An explicit value is still honored.
    options.max_batch_length = max_batch_length.unwrap_or(0);
    options.timeout = timeout;
    let stream = query.execute_with_options(options).await.infer_error()?;
    let schema = stream.schema();
    let batches = stream.try_collect::<Vec<_>>().await.infer_error()?;
    Ok(Box::new(RecordBatchIterator::new(
        batches.into_iter().map(Ok),
        schema,
    )))
}
