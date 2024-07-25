// use arrow::datatypes::SchemaRef;
// use lancedb::arrow::SendableRecordBatchStream;

use std::sync::Arc;

use arrow::{
    datatypes::SchemaRef,
    pyarrow::{IntoPyArrow, ToPyArrow},
};
use futures::stream::StreamExt;
use lancedb::arrow::SendableRecordBatchStream;
use pyo3::{pyclass, pymethods, Bound, PyAny, PyObject, PyRef, PyResult, Python};
use pyo3_asyncio_0_21::tokio::future_into_py;

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
    pub fn schema(&self, py: Python) -> PyResult<PyObject> {
        (*self.schema).clone().into_pyarrow(py)
    }

    pub fn next(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let inner_next = inner.lock().await.next().await;
            inner_next
                .map(|item| {
                    let item = item.infer_error()?;
                    Python::with_gil(|py| item.to_pyarrow(py))
                })
                .transpose()
        })
    }
}
