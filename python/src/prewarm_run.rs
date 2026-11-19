// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;
use std::time::Duration;

use lancedb::prewarm_run::{RowRange, WaveAnnouncement};
use pyo3::{
    Bound, PyAny, PyRef, PyResult, exceptions::PyRuntimeError, exceptions::PyValueError, pyclass,
    pymethods,
};

use crate::error::PythonErrorExt;
use crate::runtime::future_into_py;

/// A row range as Python passes it: `(fragment_id, start, end)`.
type PyRowRange = (u64, u64, u64);

fn row_ranges(ranges: Vec<PyRowRange>) -> Vec<RowRange> {
    ranges
        .into_iter()
        .map(|(fragment_id, start, end)| RowRange {
            fragment_id,
            start,
            end,
        })
        .collect()
}

#[pyclass]
pub struct PrewarmRun {
    inner: lancedb::prewarm_run::PrewarmRun,
}

impl PrewarmRun {
    pub(crate) fn new(inner: lancedb::prewarm_run::PrewarmRun) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PrewarmRun {
    #[getter]
    pub fn run_id(&self) -> String {
        self.inner.run_id().to_string()
    }

    #[getter]
    pub fn version(&self) -> u64 {
        self.inner.version()
    }

    pub fn reader(&self, rank: u32) -> PrewarmRunReader {
        PrewarmRunReader {
            inner: self.inner.reader(rank),
        }
    }

    pub fn join(self_: PyRef<'_, Self>, rank: u32, first_wave: u64) -> PyResult<Bound<'_, PyAny>> {
        let run = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let rank = run.join(rank, first_wave).await.infer_error()?;
            Ok(PrewarmRunRank {
                inner: Arc::new(rank),
            })
        })
    }
}

#[pyclass]
pub struct PrewarmRunRank {
    inner: Arc<lancedb::prewarm_run::PrewarmRunRank>,
}

#[pymethods]
impl PrewarmRunRank {
    #[getter]
    pub fn rank(&self) -> u32 {
        self.inner.rank()
    }

    /// The run as the server last reported it, as JSON.
    pub fn view(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner.view())
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    pub fn announce(
        self_: PyRef<'_, Self>,
        waves: Vec<(u64, Vec<PyRowRange>)>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        let waves = waves
            .into_iter()
            .map(|(wave, ranges)| WaveAnnouncement {
                wave,
                ranges: row_ranges(ranges),
            })
            .collect();
        future_into_py(self_.py(), async move {
            inner.announce(waves).await.infer_error()
        })
    }

    pub fn wait_ready(
        self_: PyRef<'_, Self>,
        wave: u64,
        timeout_s: f64,
    ) -> PyResult<Bound<'_, PyAny>> {
        let timeout = Duration::try_from_secs_f64(timeout_s)
            .map_err(|error| PyValueError::new_err(format!("invalid timeout: {error}")))?;
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner.wait_ready(wave, timeout).await.infer_error()
        })
    }

    pub fn report_wave(&self, wave: u64) {
        self.inner.report_wave(wave);
    }

    pub fn rewarm(
        self_: PyRef<'_, Self>,
        wave: u64,
        ranges: Vec<PyRowRange>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        let ranges = row_ranges(ranges);
        future_into_py(self_.py(), async move {
            inner.rewarm(wave, ranges).await.infer_error()
        })
    }

    pub fn close(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move { inner.close().await.infer_error() })
    }
}

#[pyclass]
pub struct PrewarmRunReader {
    inner: lancedb::prewarm_run::PrewarmRunReader,
}

#[pymethods]
impl PrewarmRunReader {
    #[getter]
    pub fn rank(&self) -> u32 {
        self.inner.rank()
    }

    pub fn wait_ready(
        self_: PyRef<'_, Self>,
        wave: u64,
        timeout_s: f64,
    ) -> PyResult<Bound<'_, PyAny>> {
        let timeout = Duration::try_from_secs_f64(timeout_s)
            .map_err(|error| PyValueError::new_err(format!("invalid timeout: {error}")))?;
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner.wait_ready(wave, timeout).await.infer_error()
        })
    }

    pub fn rewarm(
        self_: PyRef<'_, Self>,
        wave: u64,
        ranges: Vec<PyRowRange>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        let ranges = row_ranges(ranges);
        future_into_py(self_.py(), async move {
            inner.rewarm(wave, ranges).await.infer_error()
        })
    }
}
