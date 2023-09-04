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

/// Data cleaning and sanitization functions.

use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::FromPyArrow;
use arrow_schema::Schema;
use vectordb::data::sanitize::coerce_schema;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyfunction(name = "_sanitize_table")]
pub fn sanitize_table(data: &PyAny, schema: Option<&PyAny>) -> PyResult<()> {
    let batches = ArrowArrayStreamReader::from_pyarrow(data)?;
    let schema: Option<Schema> = schema.map(|s| Schema::from_pyarrow(s)).transpose()?;
    if let Some(schema) = schema {
        for batch in batches {
            let batch = batch.map_err(|e| {
                PyErr::new::<PyValueError>(format!(
                    "Error reading batch: {}",
                    e
                ))
            })?;
            let batch_schema = batch.schema();
            let batch = coerce_schema(batch, schema.clone())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {

}