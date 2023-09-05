// Copyright 2023 Lance Developers.
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

//! Data cleaning and sanitization functions.

use std::sync::Arc;

use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::pyarrow::{FromPyArrow, IntoPyArrow};
use arrow_schema::Schema;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use vectordb::data::sanitize::coerce_schema;

#[pyfunction(name = "_sanitize_table")]
pub fn sanitize_table(py: Python<'_>, data: &PyAny, schema: Option<&PyAny>) -> PyResult<PyObject> {
    let batches = ArrowArrayStreamReader::from_pyarrow(data)?;
    let schema: Option<Schema> = schema.map(|s| Schema::from_pyarrow(s)).transpose()?;
    if let Some(schema) = schema {
        let batches = ArrowArrayStreamReader::from_pyarrow(data)?;
        let boxed = coerce_schema(batches, Arc::new(schema)).map_err(|e| {
            PyValueError::new_err(format!("Failed to sanitize data: {}", e.to_string()))
        })?;
        // TODO(lei): wait for arrow-rs 47.0 to be released to run boxed.into_pyarrow(py)
        let ffi_stream = FFI_ArrowArrayStream::new(boxed);
        let arrow_stream_reader = ArrowArrayStreamReader::try_new(ffi_stream).map_err(|e| {
            PyValueError::new_err(format!("Failed to sanitize data: {}", e.to_string()))
        })?;
        arrow_stream_reader.into_pyarrow(py)
    } else {
        batches.into_pyarrow(py)
    }
}

#[cfg(test)]
mod tests {}
