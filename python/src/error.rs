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
    PyResult,
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
                LanceError::InvalidInput { .. } => self.value_error(),
                LanceError::InvalidTableName { .. } => self.value_error(),
                LanceError::TableNotFound { .. } => self.value_error(),
                LanceError::Schema { .. } => self.value_error(),
                LanceError::CreateDir { .. } => self.os_error(),
                LanceError::TableAlreadyExists { .. } => self.runtime_error(),
                LanceError::ObjectStore { .. } => Err(PyIOError::new_err(err.to_string())),
                LanceError::Lance { .. } => self.runtime_error(),
                LanceError::Runtime { .. } => self.runtime_error(),
                LanceError::Http { .. } => self.runtime_error(),
                LanceError::Arrow { .. } => self.runtime_error(),
                LanceError::NotSupported { .. } => {
                    Err(PyNotImplementedError::new_err(err.to_string()))
                }
                LanceError::Other { .. } => self.runtime_error(),
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
