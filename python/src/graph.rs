// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Property graph schemas, between pyarrow and the JSON form a graph's
//! definition carries.

use arrow::{datatypes::Schema, pyarrow::PyArrowType};
use lance_namespace::models::JsonArrowSchema;
use pyo3::{PyResult, exceptions::PyValueError, pyfunction};

/// A pyarrow schema as the JSON Arrow schema of a node or edge type.
#[pyfunction]
pub fn graph_schema_to_json(schema: PyArrowType<Schema>) -> PyResult<String> {
    let json = lance_namespace::schema::arrow_schema_to_json(&schema.0)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serde_json::to_string(&json).map_err(|error| PyValueError::new_err(error.to_string()))
}

/// A node or edge type's JSON Arrow schema as a pyarrow schema.
#[pyfunction]
pub fn graph_schema_from_json(json: String) -> PyResult<PyArrowType<Schema>> {
    let json: JsonArrowSchema =
        serde_json::from_str(&json).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let schema = lance_namespace::schema::convert_json_arrow_schema(&json)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    Ok(PyArrowType(schema))
}
