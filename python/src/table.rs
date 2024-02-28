use arrow::{
    ffi_stream::ArrowArrayStreamReader,
    pyarrow::{FromPyArrow, ToPyArrow},
};
use lancedb::table::{AddDataMode, Table as LanceDbTable};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyAny, PyRef, PyResult, Python};
use pyo3_asyncio::tokio::future_into_py;

use crate::error::PythonErrorExt;

#[pyclass]
pub struct Table {
    inner: LanceDbTable,
}

impl Table {
    pub(crate) fn new(inner: LanceDbTable) -> Self {
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

    pub fn add<'a>(self_: PyRef<'a, Self>, data: &PyAny, mode: String) -> PyResult<&'a PyAny> {
        let batches = Box::new(ArrowArrayStreamReader::from_pyarrow(data)?);
        let mut op = self_.inner.add(batches);
        if mode == "append" {
            op = op.mode(AddDataMode::Append);
        } else if mode == "overwrite" {
            op = op.mode(AddDataMode::Overwrite);
        } else {
            return Err(PyValueError::new_err(format!("Invalid mode: {}", mode)));
        }

        future_into_py(self_.py(), async move {
            op.execute().await.infer_error()?;
            Ok(())
        })
    }

    pub fn count_rows(self_: PyRef<'_, Self>, filter: Option<String>) -> PyResult<&PyAny> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner.count_rows(filter).await.infer_error()
        })
    }
}
