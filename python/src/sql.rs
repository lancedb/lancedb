// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;

use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyAnyMethods, PyDict, PyList, PyListMethods, PyModule};
use pyo3::{Bound, Py, PyAny, PyResult, Python, pyfunction};

use crate::connection::PyClientConfig;
use crate::error::PythonErrorExt;

/// Execute a SQL statement through LanceDB's Rust Flight SQL client.
///
/// `default_database` is resolved as `db://<name>`. Unqualified table names use
/// `default_namespace_path`, which defaults to `["public"]`. Fully qualified
/// SQL names can reference other databases and namespaces exposed by the same
/// server. Authentication, endpoint discovery, TLS, timeouts, and result
/// streaming are handled in Rust.
///
/// Returns
/// -------
/// pyarrow.Table
///     The combined result from all Flight endpoints.
#[pyfunction]
#[pyo3(signature = (
    query,
    *,
    default_database="lancedb",
    default_namespace_path=None,
    api_key=None,
    region="us-east-1",
    host_override=None,
    flight_sql_uri=None,
    client_config=None,
    storage_options=None,
))]
#[allow(clippy::too_many_arguments)]
pub fn sql(
    py: Python<'_>,
    query: String,
    default_database: &str,
    default_namespace_path: Option<Bound<'_, PyAny>>,
    api_key: Option<String>,
    region: &str,
    host_override: Option<String>,
    flight_sql_uri: Option<String>,
    client_config: Option<Bound<'_, PyAny>>,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<Py<PyAny>> {
    validate_database(default_database)?;
    let default_namespace_path = match default_namespace_path {
        Some(path) => {
            if !path.is_instance_of::<PyList>() {
                return Err(PyValueError::new_err(
                    "lancedb.sql default_namespace_path must be a list",
                ));
            }
            path.extract::<Vec<String>>().map_err(|_| {
                PyValueError::new_err(
                    "lancedb.sql default_namespace_path components must be strings",
                )
            })?
        }
        None => vec!["public".to_string()],
    };

    let rust_client_config: Option<lancedb::remote::ClientConfig> = client_config
        .map(|config| normalize_client_config(py, config))
        .transpose()?
        .map(Into::into);
    let mut resolved_api_key = api_key.or_else(|| std::env::var("LANCEDB_API_KEY").ok());
    if resolved_api_key.is_none()
        && rust_client_config.as_ref().is_some_and(|config| {
            config.header_provider.is_some()
                || config.extra_headers.keys().any(|key| {
                    key.eq_ignore_ascii_case("authorization")
                        || key.eq_ignore_ascii_case("x-api-key")
                })
        })
    {
        resolved_api_key = Some(String::new());
    }

    let mut builder = lancedb::connect(&format!("db://{default_database}")).region(region);
    if let Some(api_key) = resolved_api_key {
        builder = builder.api_key(&api_key);
    }
    if let Some(host_override) = host_override {
        builder = builder.host_override(&host_override);
    }
    if let Some(client_config) = rust_client_config {
        builder = builder.client_config(client_config);
    }
    if let Some(storage_options) = storage_options {
        builder = builder.storage_options(storage_options);
    }

    let batches = py
        .detach(move || {
            crate::runtime::block_on(async move {
                let connection = builder.execute().await?;
                let mut operation = connection
                    .sql(query)
                    .default_namespace_path(default_namespace_path);
                if let Some(uri) = flight_sql_uri {
                    operation = operation.flight_sql_uri(uri);
                }
                operation.execute().await
            })
        })
        .infer_error()?;

    let pyarrow = PyModule::import(py, "pyarrow")?;
    if batches.is_empty() {
        return Ok(pyarrow
            .call_method1("table", (pyo3::types::PyDict::new(py),))?
            .unbind());
    }
    let py_batches = PyList::empty(py);
    for batch in batches {
        py_batches.append(batch.to_pyarrow(py)?)?;
    }
    Ok(pyarrow
        .getattr("Table")?
        .call_method1("from_batches", (py_batches,))?
        .unbind())
}

fn normalize_client_config(py: Python<'_>, config: Bound<'_, PyAny>) -> PyResult<PyClientConfig> {
    if config.is_instance_of::<PyDict>() {
        let kwargs = config.cast::<PyDict>()?;
        return PyModule::import(py, "lancedb.remote")?
            .getattr("ClientConfig")?
            .call((), Some(kwargs))?
            .extract();
    }
    config.extract()
}

fn validate_database(database: &str) -> PyResult<()> {
    let invalid = database.is_empty()
        || !database.is_ascii()
        || database.bytes().any(|byte| {
            byte.is_ascii_whitespace() || b"/:@?#[]\\".contains(&byte) || byte.is_ascii_control()
        });
    if invalid {
        return Err(PyValueError::new_err(
            "lancedb.sql default_database must be a database name, not a URI or path",
        ));
    }
    Ok(())
}
