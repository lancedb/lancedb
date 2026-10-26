// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{StreamExt, future::poll_fn};

use lancedb::catalog::{CatalogConnection, CreateDatabaseRequest, DropDatabaseRequest};
use pyo3::exceptions::{PyStopAsyncIteration, PyValueError};
use pyo3::{Bound, PyAny, PyRef, PyResult, Python, pyclass, pyfunction, pymethods};

use crate::connection::{Connection, PyClientConfig};
use crate::error::PythonErrorExt;
use crate::runtime::future_into_py;

#[pyclass]
pub struct Catalog {
    inner: CatalogConnection,
}

#[pymethods]
impl Catalog {
    #[getter]
    fn authz(&self) -> PyResult<crate::authz::AuthorizationClient> {
        Ok(crate::authz::AuthorizationClient {
            inner: self.inner.authz().infer_error()?,
        })
    }

    #[getter]
    fn uri(&self) -> &str {
        self.inner.uri()
    }

    #[pyo3(signature = (name, *, exist_ok=false))]
    fn create_database<'py>(
        self_: PyRef<'py, Self>,
        name: String,
        exist_ok: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner
                .create_database(CreateDatabaseRequest::new(name).exist_ok(exist_ok))
                .await
                .map(Connection::new)
                .infer_error()
        })
    }

    fn connect_database<'py>(self_: PyRef<'py, Self>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner
                .connect_database(name)
                .await
                .map(Connection::new)
                .infer_error()
        })
    }

    #[pyo3(signature = (name, *, ignore_missing=false))]
    fn drop_database<'py>(
        self_: PyRef<'py, Self>,
        name: String,
        ignore_missing: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner
                .drop_database(DropDatabaseRequest::new(name).ignore_missing(ignore_missing))
                .await
                .infer_error()
        })
    }

    #[pyo3(signature = (*, page_token=None, page_limit=None))]
    fn list_databases(&self, page_token: Option<String>, page_limit: Option<u32>) -> DatabaseNames {
        DatabaseNames {
            inner: Arc::new(Mutex::new(
                self.inner.list_databases(page_token, page_limit),
            )),
            next_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}

#[pyclass]
pub struct DatabaseNames {
    inner: Arc<Mutex<lancedb::catalog::DatabaseNames>>,
    next_lock: Arc<tokio::sync::Mutex<()>>,
}

#[pymethods]
impl DatabaseNames {
    fn num_page_results(&self, py: Python<'_>) -> usize {
        py.detach(|| self.inner.lock().unwrap().num_page_results())
    }

    fn page_token(&self, py: Python<'_>) -> Option<String> {
        py.detach(|| self.inner.lock().unwrap().page_token().map(str::to_owned))
    }

    fn __aiter__(self_: PyRef<'_, Self>) -> PyRef<'_, Self> {
        self_
    }

    fn __anext__(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        let next_lock = self_.next_lock.clone();
        future_into_py(self_.py(), async move {
            // Serialize advances while allowing synchronous state inspection
            // between polls, including while a REST request is pending.
            let _guard = next_lock.lock().await;
            poll_fn(|cx| inner.lock().unwrap().poll_next_unpin(cx))
                .await
                .ok_or_else(|| PyStopAsyncIteration::new_err(""))?
                .infer_error()
        })
    }
}

#[pyfunction]
#[pyo3(signature = (endpoint, *, api_key=None, client_config=None, sql_host_override=None, read_consistency_interval=None, oauth_config=None))]
pub fn connect_catalog(
    py: Python<'_>,
    endpoint: String,
    api_key: Option<String>,
    client_config: Option<PyClientConfig>,
    sql_host_override: Option<String>,
    read_consistency_interval: Option<f64>,
    oauth_config: Option<crate::oauth::PyOAuthConfig>,
) -> PyResult<Bound<'_, PyAny>> {
    let interval = read_consistency_interval
        .map(Duration::try_from_secs_f64)
        .transpose()
        .map_err(|err| {
            PyValueError::new_err(format!("Invalid read consistency interval: {err}"))
        })?;
    future_into_py(py, async move {
        let mut builder = lancedb::connect_catalog(endpoint);
        if let Some(api_key) = api_key {
            builder = builder.api_key(api_key);
        }
        if let Some(config) = client_config {
            builder = builder.client_config(config.into());
        }
        if let Some(endpoint) = sql_host_override {
            builder = builder.sql_host_override(endpoint);
        }
        if let Some(interval) = interval {
            builder = builder.read_consistency_interval(interval);
        }
        if let Some(config) = oauth_config {
            builder = builder.oauth_config(config.try_into().infer_error()?);
        }
        Ok(Catalog {
            inner: builder.execute().await.infer_error()?,
        })
    })
}
