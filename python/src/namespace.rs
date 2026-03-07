// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Namespace utilities for Python bindings

use std::sync::Arc;

use lance_namespace::LanceNamespace as LanceNamespaceTrait;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

/// Extract an Arc<dyn LanceNamespace> from a Python namespace object.
///
/// This function handles REST namespaces by checking for the `_inner` attribute
/// which holds the Rust namespace client. For other namespace types, it attempts
/// to use the namespace's connection builder pattern.
pub fn extract_namespace_arc(
    py: Python<'_>,
    ns: Py<PyAny>,
) -> PyResult<Arc<dyn LanceNamespaceTrait>> {
    let ns_ref = ns.bind(py);

    // Check if it has _inner attribute (REST namespace wrapper)
    if let Ok(_inner) = ns_ref.getattr("_inner") {
        // Try to get the inner client - this is a Rust object wrapped in Python
        // For REST namespaces, we need to reconstruct from properties
        if let Ok(props_dict) = ns_ref.getattr("_properties") {
            let props = props_dict.extract::<std::collections::HashMap<String, String>>()?;
            let rt = Runtime::new().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to create tokio runtime: {}",
                    e
                ))
            })?;
            let ns_client = rt
                .block_on(
                    lance_namespace_impls::ConnectBuilder::new("rest")
                        .properties(props)
                        .connect(),
                )
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to connect to namespace: {}",
                        e
                    ))
                })?;
            return Ok(ns_client);
        }
    }

    // Fallback: try to use namespace_impl and namespace_properties if available
    if let (Ok(impl_name), Ok(props)) = (
        ns_ref.getattr("namespace_impl"),
        ns_ref.getattr("namespace_properties"),
    ) {
        let impl_str: String = impl_name.extract()?;
        let props_map: std::collections::HashMap<String, String> = props.extract()?;

        let rt = Runtime::new().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to create tokio runtime: {}",
                e
            ))
        })?;
        let ns_client = rt
            .block_on(
                lance_namespace_impls::ConnectBuilder::new(&impl_str)
                    .properties(props_map)
                    .connect(),
            )
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to connect to namespace: {}",
                    e
                ))
            })?;
        return Ok(ns_client);
    }

    Err(pyo3::exceptions::PyTypeError::new_err(
        "Cannot extract namespace client from Python object. \
         Expected a LanceNamespace with _inner or namespace_impl/namespace_properties attributes.",
    ))
}
