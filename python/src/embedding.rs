// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! PyO3 wrappers for Python embedding functions and registry.
//!
//! This module allows Rust to call Python's embedding functions by wrapping
//! Python's EmbeddingFunctionRegistry. The registry parses the `embedding_functions`
//! metadata format used by Python tables.

use std::borrow::Cow;
use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayData};
use arrow::datatypes::{DataType, Field};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use lancedb::embeddings::{EmbeddingDefinition, EmbeddingFunction, EmbeddingRegistry};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyDict};

/// Wraps a Python EmbeddingFunction to implement the Rust EmbeddingFunction trait
pub struct PyEmbeddingFunction {
    py_func: Py<PyAny>,
    name: String,
    ndims: i32,
}

impl std::fmt::Debug for PyEmbeddingFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PyEmbeddingFunction({})", self.name)
    }
}

impl PyEmbeddingFunction {
    pub fn new(py: Python<'_>, py_func: Py<PyAny>) -> PyResult<Self> {
        let name: String = py_func.bind(py).get_type().name()?.to_string();
        let ndims: i32 = py_func.call_method0(py, "ndims")?.extract(py)?;
        Ok(Self {
            py_func,
            name,
            ndims,
        })
    }
}

impl EmbeddingFunction for PyEmbeddingFunction {
    fn name(&self) -> &str {
        &self.name
    }

    fn source_type(&self) -> lancedb::Result<Cow<'_, DataType>> {
        // Most embedding functions take text input
        Ok(Cow::Owned(DataType::Utf8))
    }

    fn dest_type(&self) -> lancedb::Result<Cow<'_, DataType>> {
        Ok(Cow::Owned(DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)),
            self.ndims,
        )))
    }

    fn compute_source_embeddings(&self, source: Arc<dyn Array>) -> lancedb::Result<Arc<dyn Array>> {
        #[allow(deprecated)]
        Python::with_gil(|py| {
            // Convert Arrow array to PyArrow
            let py_array =
                source
                    .into_data()
                    .to_pyarrow(py)
                    .map_err(|e| lancedb::Error::Runtime {
                        message: format!("Failed to convert to PyArrow: {}", e),
                    })?;

            // Call Python embedding function
            // Python embedding functions return list[np.array], not PyArrow arrays
            let result = self
                .py_func
                .call_method1(py, "compute_source_embeddings_with_retry", (py_array,))
                .map_err(|e| lancedb::Error::Runtime {
                    message: format!("Python embedding failed: {}", e),
                })?;

            // Convert list[np.array] to PyArrow FixedSizeList array
            // Using the same approach as Python: pa.array(col_data, type=fsl_type)
            let pyarrow = py.import("pyarrow").map_err(|e| lancedb::Error::Runtime {
                message: format!("Failed to import pyarrow: {}", e),
            })?;

            // Create the FixedSizeList type: list(float32, ndims)
            let float32_type =
                pyarrow
                    .call_method0("float32")
                    .map_err(|e| lancedb::Error::Runtime {
                        message: format!("Failed to get float32 type: {}", e),
                    })?;
            let list_type = pyarrow
                .call_method1("list_", (float32_type, self.ndims))
                .map_err(|e| lancedb::Error::Runtime {
                    message: format!("Failed to create list type: {}", e),
                })?;

            // Convert the result to PyArrow array with the correct type
            let kwargs =
                [("type", list_type)]
                    .into_py_dict(py)
                    .map_err(|e| lancedb::Error::Runtime {
                        message: format!("Failed to create kwargs dict: {}", e),
                    })?;
            let arrow_array = pyarrow
                .call_method("array", (result.bind(py),), Some(&kwargs))
                .map_err(|e| lancedb::Error::Runtime {
                    message: format!("Failed to create PyArrow array: {}", e),
                })?;

            let array_data = ArrayData::from_pyarrow_bound(&arrow_array).map_err(|e| {
                lancedb::Error::Runtime {
                    message: format!("Failed to convert from PyArrow: {}", e),
                }
            })?;
            Ok(make_array(array_data))
        })
    }

    fn compute_query_embeddings(&self, input: Arc<dyn Array>) -> lancedb::Result<Arc<dyn Array>> {
        #[allow(deprecated)]
        Python::with_gil(|py| {
            // Convert Arrow array to PyArrow
            let py_array =
                input
                    .into_data()
                    .to_pyarrow(py)
                    .map_err(|e| lancedb::Error::Runtime {
                        message: format!("Failed to convert to PyArrow: {}", e),
                    })?;

            // Call Python embedding function
            // Python embedding functions return list[np.array], not PyArrow arrays
            let result = self
                .py_func
                .call_method1(py, "compute_query_embeddings_with_retry", (py_array,))
                .map_err(|e| lancedb::Error::Runtime {
                    message: format!("Python query embedding failed: {}", e),
                })?;

            // Convert list[np.array] to PyArrow FixedSizeList array
            let pyarrow = py.import("pyarrow").map_err(|e| lancedb::Error::Runtime {
                message: format!("Failed to import pyarrow: {}", e),
            })?;

            // Create the FixedSizeList type: list(float32, ndims)
            let float32_type =
                pyarrow
                    .call_method0("float32")
                    .map_err(|e| lancedb::Error::Runtime {
                        message: format!("Failed to get float32 type: {}", e),
                    })?;
            let list_type = pyarrow
                .call_method1("list_", (float32_type, self.ndims))
                .map_err(|e| lancedb::Error::Runtime {
                    message: format!("Failed to create list type: {}", e),
                })?;

            // Convert the result to PyArrow array with the correct type
            let kwargs =
                [("type", list_type)]
                    .into_py_dict(py)
                    .map_err(|e| lancedb::Error::Runtime {
                        message: format!("Failed to create kwargs dict: {}", e),
                    })?;
            let arrow_array = pyarrow
                .call_method("array", (result.bind(py),), Some(&kwargs))
                .map_err(|e| lancedb::Error::Runtime {
                    message: format!("Failed to create PyArrow array: {}", e),
                })?;

            let array_data = ArrayData::from_pyarrow_bound(&arrow_array).map_err(|e| {
                lancedb::Error::Runtime {
                    message: format!("Failed to convert from PyArrow: {}", e),
                }
            })?;
            Ok(make_array(array_data))
        })
    }
}

/// Wraps Python's EmbeddingFunctionRegistry singleton.
///
/// This registry can look up embedding functions from Python and parse the
/// `embedding_functions` metadata format used by Python tables.
#[pyclass]
pub struct PyEmbeddingRegistry {
    py_registry: Py<PyAny>,
}

impl Clone for PyEmbeddingRegistry {
    fn clone(&self) -> Self {
        #[allow(deprecated)]
        Python::with_gil(|py| Self {
            py_registry: self.py_registry.clone_ref(py),
        })
    }
}

#[pymethods]
impl PyEmbeddingRegistry {
    /// Create a new PyEmbeddingRegistry from Python's global singleton.
    #[staticmethod]
    pub fn from_singleton(py: Python<'_>) -> PyResult<Self> {
        let module = py.import("lancedb.embeddings.registry")?;
        let registry_class = module.getattr("EmbeddingFunctionRegistry")?;
        let py_registry = registry_class.call_method0("get_instance")?.into();
        Ok(Self { py_registry })
    }
}

impl std::fmt::Debug for PyEmbeddingRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PyEmbeddingRegistry")
    }
}

impl EmbeddingRegistry for PyEmbeddingRegistry {
    fn functions(&self) -> std::collections::HashSet<String> {
        // Return empty - we look up dynamically from Python
        std::collections::HashSet::new()
    }

    fn register(&self, _name: &str, _function: Arc<dyn EmbeddingFunction>) -> lancedb::Result<()> {
        // Registration happens on Python side
        Err(lancedb::Error::Other {
            message: "Cannot register functions in PyEmbeddingRegistry from Rust".to_string(),
            source: None,
        })
    }

    fn get(&self, name: &str) -> Option<Arc<dyn EmbeddingFunction>> {
        #[allow(deprecated)]
        Python::with_gil(|py| {
            // Look up function class by name in Python registry
            let func_class = self.py_registry.call_method1(py, "get", (name,)).ok()?;

            // Call create() to instantiate the function
            let func_instance = func_class.call_method0(py, "create").ok()?;

            let wrapper = PyEmbeddingFunction::new(py, func_instance).ok()?;
            Some(Arc::new(wrapper) as Arc<dyn EmbeddingFunction>)
        })
    }

    fn parse_metadata_embeddings(
        &self,
        metadata: &std::collections::HashMap<String, String>,
    ) -> lancedb::Result<Vec<(EmbeddingDefinition, Arc<dyn EmbeddingFunction>)>> {
        // Delegate to our parse_metadata helper method
        self.parse_metadata(metadata)
    }
}

impl PyEmbeddingRegistry {
    /// Parse Python's embedding_functions metadata and return embedding definitions.
    ///
    /// This reads the `embedding_functions` key from schema metadata (in Python's JSON
    /// format) and returns the parsed embedding definitions with their associated
    /// embedding functions.
    pub fn parse_metadata(
        &self,
        metadata: &std::collections::HashMap<String, String>,
    ) -> lancedb::Result<Vec<(EmbeddingDefinition, Arc<dyn EmbeddingFunction>)>> {
        #[allow(deprecated)]
        Python::with_gil(|py| {
            // Convert metadata to Python dict with bytes keys/values (pyarrow format)
            let py_metadata = PyDict::new(py);
            for (k, v) in metadata {
                py_metadata
                    .set_item(k.as_bytes(), v.as_bytes())
                    .map_err(|e| lancedb::Error::Runtime {
                        message: format!("Failed to set metadata item: {}", e),
                    })?;
            }

            // Call parse_functions on Python registry
            let parsed = self
                .py_registry
                .call_method1(py, "parse_functions", (py_metadata,))
                .map_err(|e| lancedb::Error::Runtime {
                    message: format!("Failed to parse embedding functions: {}", e),
                })?;

            // Convert result to Rust types
            // parse_functions returns Dict[str, EmbeddingFunctionConfig]
            let parsed_dict =
                parsed
                    .bind(py)
                    .downcast::<PyDict>()
                    .map_err(|e| lancedb::Error::Runtime {
                        message: format!("Expected dict from parse_functions: {}", e),
                    })?;

            let mut result = Vec::new();
            for (vector_col, conf) in parsed_dict.iter() {
                let vector_column: String =
                    vector_col.extract().map_err(|e| lancedb::Error::Runtime {
                        message: format!("Failed to extract vector column name: {}", e),
                    })?;
                let source_column: String = conf
                    .getattr("source_column")
                    .map_err(|e| lancedb::Error::Runtime {
                        message: format!("Failed to get source_column: {}", e),
                    })?
                    .extract()
                    .map_err(|e| lancedb::Error::Runtime {
                        message: format!("Failed to extract source_column: {}", e),
                    })?;
                let py_func: Py<PyAny> = conf
                    .getattr("function")
                    .map_err(|e| lancedb::Error::Runtime {
                        message: format!("Failed to get function: {}", e),
                    })?
                    .into();

                // Use vector_column as the embedding name for lookup
                let def = EmbeddingDefinition::new(
                    &source_column,
                    &vector_column, // embedding_name
                    Some(&vector_column),
                );

                let wrapper =
                    PyEmbeddingFunction::new(py, py_func).map_err(|e| lancedb::Error::Runtime {
                        message: format!("Failed to wrap embedding function: {}", e),
                    })?;
                result.push((def, Arc::new(wrapper) as Arc<dyn EmbeddingFunction>));
            }

            Ok(result)
        })
    }
}

// Make PyEmbeddingRegistry Send + Sync by ensuring GIL is held for all operations
// that access Python objects
unsafe impl Send for PyEmbeddingRegistry {}
unsafe impl Sync for PyEmbeddingRegistry {}
