// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow::pyarrow::ToPyArrow;
use pyo3::{Bound, Py, PyAny, PyResult, Python, pyclass, pymethods, types::PyTuple};

/// Immutable first-class Function handle backed by the exact Rust value.
#[pyclass(frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct Function {
    inner: lancedb::function::Function,
}

impl Function {
    pub(crate) fn new(inner: lancedb::function::Function) -> Self {
        Self { inner }
    }

    /// Crate-private accessor for later call-authoring slices.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> &lancedb::function::Function {
        &self.inner
    }
}

#[pymethods]
impl Function {
    #[getter]
    fn id(&self) -> &str {
        self.inner.id().as_str()
    }

    #[getter]
    fn parameters<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let parameters = self.inner.signature().parameters();
        let mut pairs = Vec::with_capacity(parameters.len());
        for parameter in parameters {
            let data_type = parameter.data_type().to_pyarrow(py)?;
            pairs.push((parameter.name(), data_type));
        }
        PyTuple::new(py, pairs)
    }

    #[getter]
    fn output_type(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner
            .signature()
            .output()
            .data_type()
            .to_pyarrow(py)
            .map(|obj| obj.unbind())
    }

    #[getter]
    fn output_nullable(&self) -> bool {
        self.inner.signature().output().nullable()
    }

    fn __repr__(&self) -> String {
        format!("Function(id={:?})", self.inner.id().as_str())
    }
}
