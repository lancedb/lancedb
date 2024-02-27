use std::sync::Arc;

use arrow::pyarrow::ToPyArrow;
use lancedb::table::Table as LanceTable;
use pyo3::{pyclass, pymethods, PyAny, PyRef, PyResult, Python};
use pyo3_asyncio::tokio::future_into_py;

use crate::error::PythonErrorExt;

#[pyclass]
pub struct Table {
    inner: Arc<dyn LanceTable>,
}

impl Table {
    pub(crate) fn new(inner: Arc<dyn LanceTable>) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl Table {
    pub fn name(&self) -> String {
        self.inner.name().to_string()
    }

    pub fn schema(self_: PyRef<'_, Self>) -> PyResult<&PyAny> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let schema = inner.schema().await.infer_error()?;
            Python::with_gil(|py| schema.to_pyarrow(py))
        })
    }
}
