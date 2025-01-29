// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow::{
    datatypes::SchemaRef,
    pyarrow::{IntoPyArrow, ToPyArrow},
};
use futures::stream::StreamExt;
use lancedb::arrow::SendableRecordBatchStream;
use pyo3::{
    exceptions::PyStopAsyncIteration, pyclass, pymethods, Bound, PyAny, PyObject, PyRef, PyResult,
    Python,
};
use pyo3_async_runtimes::tokio::future_into_py;

use crate::error::PythonErrorExt;

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
    pub fn schema(&self, py: Python) -> PyResult<PyObject> {
        (*self.schema).clone().into_pyarrow(py)
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
            Python::with_gil(|py| inner_next.infer_error()?.to_pyarrow(py))
        })
    }
}
