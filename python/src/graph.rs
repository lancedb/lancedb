// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Property graph schemas, between pyarrow and the JSON form a graph's
//! definition carries.

use arrow::{datatypes::Schema, pyarrow::PyArrowType};
use lance_namespace::models::JsonArrowSchema;
use pyo3::{PyResult, exceptions::PyValueError, pyfunction};

/// A kind of graph, spelled as its routes and catalog objects are.
#[derive(Debug, Clone, Copy)]
pub enum GraphKind {
    Property,
    Virtual,
    MaterializedVirtual,
}

impl GraphKind {
    pub fn parse(kind: &str) -> PyResult<Self> {
        match kind {
            "property_graph" => Ok(Self::Property),
            "virtual_property_graph" => Ok(Self::Virtual),
            "materialized_virtual_property_graph" => Ok(Self::MaterializedVirtual),
            other => Err(PyValueError::new_err(format!(
                "'{other}' is not a kind of property graph"
            ))),
        }
    }
}

/// A pyarrow schema as the JSON Arrow schema of a node or edge type, refused
/// when that form would not read back as the same schema.
#[pyfunction]
pub fn graph_schema_to_json(schema: PyArrowType<Schema>) -> PyResult<String> {
    let json = lancedb::graph::schema_to_json(&schema.0)
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
