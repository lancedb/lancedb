// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use pyo3::{
    exceptions::{PyIOError, PyNotImplementedError, PyOSError, PyRuntimeError, PyValueError},
    intern,
    types::{PyAnyMethods, PyNone},
    PyErr, PyResult, Python,
};

use lancedb::error::Error as LanceError;

pub trait PythonErrorExt<T> {
    /// Convert to a python error based on the Lance error type
    fn infer_error(self) -> PyResult<T>;
    /// Convert to OSError
    fn os_error(self) -> PyResult<T>;
    /// Convert to RuntimeError
    fn runtime_error(self) -> PyResult<T>;
    /// Convert to ValueError
    fn value_error(self) -> PyResult<T>;
}

impl<T> PythonErrorExt<T> for std::result::Result<T, LanceError> {
    fn infer_error(self) -> PyResult<T> {
        match &self {
            Ok(_) => Ok(self.unwrap()),
            Err(err) => match err {
                LanceError::InvalidInput { .. }
                | LanceError::InvalidTableName { .. }
                | LanceError::TableNotFound { .. }
                | LanceError::Schema { .. }
                | LanceError::TableAlreadyExists { .. } => self.value_error(),
                LanceError::CreateDir { .. } => self.os_error(),
                LanceError::ObjectStore { .. } => Err(PyIOError::new_err(err.to_string())),
                LanceError::NotSupported { .. } => {
                    Err(PyNotImplementedError::new_err(err.to_string()))
                }
                LanceError::Http {
                    request_id,
                    source,
                    status_code,
                } => Python::with_gil(|py| {
                    let message = err.to_string();
                    let http_err_cls = py
                        .import_bound(intern!(py, "lancedb.remote.errors"))?
                        .getattr(intern!(py, "HttpError"))?;
                    let err = http_err_cls.call1((
                        message,
                        request_id,
                        status_code.map(|s| s.as_u16()),
                    ))?;

                    if let Some(cause) = source.source() {
                        // The HTTP error already includes the first cause. But
                        // we can add the rest of the chain if there is any more.
                        let cause_err = http_from_rust_error(
                            py,
                            cause,
                            request_id,
                            status_code.map(|s| s.as_u16()),
                        )?;
                        err.setattr(intern!(py, "__cause__"), cause_err)?;
                    }

                    Err(PyErr::from_value_bound(err))
                }),
                LanceError::Retry {
                    request_id,
                    request_failures,
                    max_request_failures,
                    connect_failures,
                    max_connect_failures,
                    read_failures,
                    max_read_failures,
                    source,
                    status_code,
                } => Python::with_gil(|py| {
                    let cause_err = http_from_rust_error(
                        py,
                        source.as_ref(),
                        request_id,
                        status_code.map(|s| s.as_u16()),
                    )?;

                    let message = err.to_string();
                    let retry_error_cls = py
                        .import_bound(intern!(py, "lancedb.remote.errors"))?
                        .getattr("RetryError")?;
                    let err = retry_error_cls.call1((
                        message,
                        request_id,
                        *request_failures,
                        *connect_failures,
                        *read_failures,
                        *max_request_failures,
                        *max_connect_failures,
                        *max_read_failures,
                        status_code.map(|s| s.as_u16()),
                    ))?;

                    err.setattr(intern!(py, "__cause__"), cause_err)?;
                    Err(PyErr::from_value_bound(err))
                }),
                _ => self.runtime_error(),
            },
        }
    }

    fn os_error(self) -> PyResult<T> {
        self.map_err(|err| PyOSError::new_err(err.to_string()))
    }

    fn runtime_error(self) -> PyResult<T> {
        self.map_err(|err| PyRuntimeError::new_err(err.to_string()))
    }

    fn value_error(self) -> PyResult<T> {
        self.map_err(|err| PyValueError::new_err(err.to_string()))
    }
}

fn http_from_rust_error(
    py: Python<'_>,
    err: &dyn std::error::Error,
    request_id: &str,
    status_code: Option<u16>,
) -> PyResult<PyErr> {
    let message = err.to_string();
    let http_err_cls = py.import("lancedb.remote.errors")?.getattr("HttpError")?;
    let py_err = http_err_cls.call1((message, request_id, status_code))?;

    // Reset the traceback since it doesn't provide additional information.
    let py_err = py_err.call_method1(intern!(py, "with_traceback"), (PyNone::get_bound(py),))?;

    if let Some(cause) = err.source() {
        let cause_err = http_from_rust_error(py, cause, request_id, status_code)?;
        py_err.setattr(intern!(py, "__cause__"), cause_err)?;
    }

    Ok(PyErr::from_value(py_err))
}
