// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::time::Duration;

use lancedb::catalog::{
    CatalogConnection, CreateDatabaseRequest, DropDatabaseRequest, ListDatabasesRequest,
};
use pyo3::exceptions::PyValueError;
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

    #[pyo3(signature = (*, limit=None, page_token=None))]
    fn list_databases<'py>(
        self_: PyRef<'py, Self>,
        limit: Option<u32>,
        page_token: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let mut request = ListDatabasesRequest::default();
            request.limit = limit;
            request.page_token = page_token;
            let response = inner.list_databases(request).await.infer_error()?;
            Ok((response.databases, response.page_token))
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
