// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use crate::error::PythonErrorExt;
use crate::runtime::future_into_py;
use futures::{StreamExt, future::poll_fn};
use lancedb::listing::{Listing, ListingOptions};
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::{Bound, PyAny, PyRef, PyResult, Python, pyclass, pymethods};
use std::sync::{Arc, Mutex};

pub fn options(page_token: Option<String>, page_limit: Option<u32>) -> ListingOptions {
    let mut options = ListingOptions::default();
    options.page_token = page_token;
    options.page_limit = page_limit;
    options
}

#[pyclass]
pub struct NameListing {
    inner: Arc<Mutex<Listing<String>>>,
    next_lock: Arc<tokio::sync::Mutex<()>>,
}
impl NameListing {
    pub fn new(inner: Listing<String>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
            next_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}
#[pymethods]
impl NameListing {
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
            let _guard = next_lock.lock().await;
            let value = poll_fn(|cx| inner.lock().unwrap().poll_next_unpin(cx))
                .await
                .ok_or_else(|| PyStopAsyncIteration::new_err(""))?
                .infer_error()?;
            Ok(value)
        })
    }
}

#[pyclass]
pub struct FunctionListing {
    inner: Arc<Mutex<Listing<lancedb::function::FunctionVersion>>>,
    next_lock: Arc<tokio::sync::Mutex<()>>,
}
impl FunctionListing {
    pub fn new(inner: Listing<lancedb::function::FunctionVersion>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
            next_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}
#[pymethods]
impl FunctionListing {
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
            let _guard = next_lock.lock().await;
            let value = poll_fn(|cx| inner.lock().unwrap().poll_next_unpin(cx))
                .await
                .ok_or_else(|| PyStopAsyncIteration::new_err(""))?
                .infer_error()?;
            value.to_canonical_json().infer_error()
        })
    }
}

#[pyclass]
pub struct JobListing {
    inner: Arc<Mutex<Listing<lancedb::database::JobInfo>>>,
    next_lock: Arc<tokio::sync::Mutex<()>>,
}
impl JobListing {
    pub fn new(inner: Listing<lancedb::database::JobInfo>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
            next_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}
#[pymethods]
impl JobListing {
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
            let _guard = next_lock.lock().await;
            let value = poll_fn(|cx| inner.lock().unwrap().poll_next_unpin(cx))
                .await
                .ok_or_else(|| PyStopAsyncIteration::new_err(""))?
                .infer_error()?;
            Ok(crate::job::JobInfo::from(value))
        })
    }
}
