// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Python-facing wrappers over [`lancedb::metrics_otel`].
//!
//! The aggregation, catalog, and histogram bucketing all live in the LanceDB
//! core crate; this module only converts the core snapshot types into PyO3
//! classes and exposes the three entry points to Python, where
//! `lancedb/otel.py` bridges them into the user's OpenTelemetry `MeterProvider`.

use std::collections::HashMap;

use lancedb::metrics_otel::{MetricPoint, MetricValue};
use pyo3::prelude::*;

/// One metric data point exposed to Python. For counters and gauges only
/// `value` is set; for histograms `buckets` (cumulative `le` counts), `count`,
/// and `sum` are set.
#[pyclass(name = "MetricPoint", get_all)]
pub struct PyMetricPoint {
    name: String,
    kind: String,
    attributes: HashMap<String, String>,
    value: Option<f64>,
    buckets: Option<Vec<(String, u64)>>,
    count: Option<u64>,
    sum: Option<f64>,
}

impl From<MetricPoint> for PyMetricPoint {
    fn from(point: MetricPoint) -> Self {
        let kind = point.kind.as_str().to_string();
        let (value, buckets, count, sum) = match point.value {
            MetricValue::Scalar(v) => (Some(v), None, None, None),
            MetricValue::Histogram {
                buckets,
                count,
                sum,
            } => (None, Some(buckets), Some(count), Some(sum)),
        };
        Self {
            name: point.name,
            kind,
            attributes: point.attributes,
            value,
            buckets,
            count,
            sum,
        }
    }
}

/// A described metric, used by the Python layer to create instruments up front.
#[pyclass(name = "MetricDescription", get_all)]
pub struct PyMetricDescription {
    name: String,
    kind: String,
    unit: Option<String>,
    description: String,
}

/// Install the LanceDB metrics recorder as the process-global `metrics` recorder.
///
/// Returns `True` if the recorder is installed (now or previously). Returns
/// `False` if a *different* recorder is already installed — `metrics` allows
/// only one global recorder per process, so LanceDB cannot coexist with another.
#[pyfunction]
pub fn register_lancedb_metrics_recorder() -> bool {
    lancedb::metrics_otel::register_metrics_recorder()
}

/// The catalog of described LanceDB metrics. Empty until the recorder is installed.
#[pyfunction]
pub fn lancedb_metrics_catalog() -> Vec<PyMetricDescription> {
    lancedb::metrics_otel::metrics_catalog()
        .into_iter()
        .map(|desc| PyMetricDescription {
            name: desc.name,
            kind: desc.kind.as_str().to_string(),
            unit: desc.unit,
            description: desc.description,
        })
        .collect()
}

/// A point-in-time snapshot of every recorded metric. Empty until the recorder
/// is installed. The lock-free read runs with the GIL released.
#[pyfunction]
pub fn snapshot_lancedb_metrics(py: Python<'_>) -> Vec<PyMetricPoint> {
    let points = py.detach(lancedb::metrics_otel::snapshot_metrics);
    points.into_iter().map(PyMetricPoint::from).collect()
}
