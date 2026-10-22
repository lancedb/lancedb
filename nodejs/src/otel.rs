// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Node.js bindings over [`lancedb::metrics_otel`].
//!
//! The aggregation, catalog, and histogram bucketing all live in the LanceDB
//! core crate; this module only converts the core snapshot types into napi
//! objects and exposes the three entry points to JavaScript, where
//! `lancedb/otel.ts` bridges them into the user's OpenTelemetry `MeterProvider`.

use std::collections::HashMap;

use lancedb::metrics_otel::{MetricPoint as CoreMetricPoint, MetricValue};
use napi_derive::napi;

/// One cumulative histogram bucket: all samples with value `<= le`.
#[napi(object)]
pub struct MetricBucket {
    /// The inclusive upper bound of the bucket, or `"+Inf"` for the final bucket.
    pub le: String,
    /// Cumulative number of samples less than or equal to `le`.
    pub cumulative_count: f64,
}

/// One aggregated metric data point. For counters and gauges only `value` is
/// set; for histograms `buckets` (cumulative `le` counts), `count`, and `sum`
/// are set.
#[napi(object)]
pub struct MetricPoint {
    pub name: String,
    pub kind: String,
    pub attributes: HashMap<String, String>,
    pub value: Option<f64>,
    pub buckets: Option<Vec<MetricBucket>>,
    pub count: Option<f64>,
    pub sum: Option<f64>,
}

impl From<CoreMetricPoint> for MetricPoint {
    fn from(point: CoreMetricPoint) -> Self {
        let kind = point.kind.as_str().to_string();
        let (value, buckets, count, sum) = match point.value {
            MetricValue::Scalar(v) => (Some(v), None, None, None),
            MetricValue::Histogram {
                buckets,
                count,
                sum,
            } => (
                None,
                Some(
                    buckets
                        .into_iter()
                        // Counts stay well within the f64-exact integer range
                        // (2^53), so this cast is lossless in practice and keeps
                        // the values plain JS numbers for OpenTelemetry.
                        .map(|(le, cumulative_count)| MetricBucket {
                            le,
                            cumulative_count: cumulative_count as f64,
                        })
                        .collect(),
                ),
                Some(count as f64),
                Some(sum),
            ),
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

/// A described metric, used by the JavaScript layer to create instruments up front.
#[napi(object)]
pub struct MetricDescription {
    pub name: String,
    pub kind: String,
    pub unit: Option<String>,
    pub description: String,
}

/// Install the LanceDB metrics recorder as the process-global `metrics` recorder.
///
/// Returns `true` if the recorder is installed (now or previously). Returns
/// `false` if a *different* recorder is already installed — `metrics` allows
/// only one global recorder per process, so LanceDB cannot coexist with another.
#[napi]
pub fn register_lancedb_metrics_recorder() -> bool {
    lancedb::metrics_otel::register_metrics_recorder()
}

/// The catalog of described LanceDB metrics. Empty until the recorder is installed.
#[napi]
pub fn lancedb_metrics_catalog() -> Vec<MetricDescription> {
    lancedb::metrics_otel::metrics_catalog()
        .into_iter()
        .map(|desc| MetricDescription {
            name: desc.name,
            kind: desc.kind.as_str().to_string(),
            unit: desc.unit,
            description: desc.description,
        })
        .collect()
}

/// A point-in-time snapshot of every recorded metric. Empty until the recorder
/// is installed.
#[napi]
pub fn snapshot_lancedb_metrics() -> Vec<MetricPoint> {
    lancedb::metrics_otel::snapshot_metrics()
        .into_iter()
        .map(MetricPoint::from)
        .collect()
}
