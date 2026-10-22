// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! A pull-based adapter over the [`metrics`] crate facade.
//!
//! LanceDB (through Lance core) publishes metrics — currently object store
//! request counts, bytes, latency, errors, and throttles — through the global
//! [`metrics`] facade without choosing a backend. This module installs a
//! process-global [`metrics::Recorder`] that aggregates those metrics into
//! lock-free cumulative storage and exposes that state as a snapshot, so callers
//! can feed it into a pull-based exporter such as OpenTelemetry.
//!
//! The language bindings build their OpenTelemetry integrations on top of the
//! three public entry points here: [`register_metrics_recorder`],
//! [`metrics_catalog`], and [`snapshot_metrics`].
//!
//! The recorder is *generic*: it records any metric emitted through the facade,
//! keyed by name and labels. Object store metrics are the first producer, but
//! nothing here is specific to them. New metrics flow through automatically;
//! they only need to be described (via the `metrics` `describe_*!` macros) so
//! callers can discover their name, kind, and unit up front.
//!
//! ## Why pull, not push
//!
//! OpenTelemetry collects on its own schedule and invokes observable-instrument
//! callbacks at collection time. Cumulative counters map directly onto OTel's
//! `ObservableCounter` semantics. So the adapter aggregates in Rust and lets the
//! collection thread pull a [`snapshot`](snapshot_metrics) on demand.
//!
//! ## Histograms
//!
//! OpenTelemetry has no asynchronous histogram instrument, so histograms cannot
//! be pulled as-is. Instead each histogram is aggregated into fixed buckets
//! (Prometheus style) and exposed as cumulative `le` bucket counts plus a count
//! and sum, which the caller can surface as observable counters.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock, RwLock};

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_util::registry::{Registry, Storage};

/// Bucket boundaries used when a histogram has no registered bounds. Covers a
/// broad latency range so unknown histograms still produce useful buckets.
const DEFAULT_BOUNDS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
];

/// The kind of a metric, mirroring the three `metrics` instrument types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

impl MetricKind {
    /// The lowercase name of this kind (`"counter"`, `"gauge"`, `"histogram"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

/// A described metric, used to create one exporter instrument per metric up front.
#[derive(Debug, Clone)]
pub struct MetricDescription {
    /// The metric name (e.g. `lance_object_store_requests_total`).
    pub name: String,
    /// Whether the metric is a counter, gauge, or histogram.
    pub kind: MetricKind,
    /// The canonical unit label, if the producer described one.
    pub unit: Option<String>,
    /// Human-readable help text describing the metric.
    pub description: String,
}

/// The aggregated value of a metric at snapshot time.
#[derive(Debug, Clone)]
pub enum MetricValue {
    /// A counter or gauge value.
    Scalar(f64),
    /// A histogram, decomposed into cumulative `le` buckets plus count and sum.
    Histogram {
        /// Cumulative `(le, count)` buckets, ending in the implicit `+Inf` bucket.
        buckets: Vec<(String, u64)>,
        /// Total number of recorded samples.
        count: u64,
        /// Sum of all recorded sample values.
        sum: f64,
    },
}

/// One aggregated metric data point exposed to a caller.
#[derive(Debug, Clone)]
pub struct MetricPoint {
    /// The metric name.
    pub name: String,
    /// Whether the point is a counter, gauge, or histogram.
    pub kind: MetricKind,
    /// The label set for this point (e.g. `operation`, `base`).
    pub attributes: HashMap<String, String>,
    /// The aggregated value.
    pub value: MetricValue,
}

/// Catalog of described metrics, keyed by metric name.
static CATALOG: LazyLock<Mutex<HashMap<String, CatalogEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

struct CatalogEntry {
    kind: MetricKind,
    unit: Option<String>,
    description: String,
}

/// Per-metric histogram bucket boundaries, keyed by metric name. Producers
/// register their recommended bounds before any metric is recorded.
static HISTOGRAM_BOUNDS: LazyLock<RwLock<HashMap<String, Arc<[f64]>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// The installed recorder's registry, available once installation succeeds.
static REGISTRY: OnceLock<Arc<Registry<Key, LanceStorage>>> = OnceLock::new();

fn bounds_for(name: &str) -> Arc<[f64]> {
    HISTOGRAM_BOUNDS
        .read()
        .unwrap()
        .get(name)
        .cloned()
        .unwrap_or_else(|| Arc::from(DEFAULT_BOUNDS))
}

/// A histogram that buckets samples at record time into fixed boundaries,
/// keeping a cumulative count and sum. Bucketing eagerly keeps memory bounded
/// (unlike retaining raw samples) and produces Prometheus-style `le` buckets.
struct BucketedHistogram {
    /// Sorted, finite upper bounds. A sample `v` falls in the first bucket whose
    /// bound is `>= v`; samples above all bounds fall in the implicit `+Inf`
    /// bucket stored as the final entry of `counts`.
    bounds: Arc<[f64]>,
    /// Per-bucket (non-cumulative) counts; length is `bounds.len() + 1`.
    counts: Box<[AtomicU64]>,
    count: AtomicU64,
    /// Running sum of recorded values, stored as `f64` bits (there is no atomic
    /// f64, so the bit pattern is held in a `u64`; see [`Self::add_to_sum`]).
    sum_bits: AtomicU64,
}

// All atomics here use `Ordering::Relaxed`: each metric counter is independent,
// so no happens-before relationship is needed between them, and a snapshot
// reader tolerates slightly stale values. This matches `metrics_util`'s
// `AtomicStorage`.

impl BucketedHistogram {
    fn new(bounds: Arc<[f64]>) -> Self {
        let counts = (0..bounds.len() + 1)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            bounds,
            counts,
            count: AtomicU64::new(0),
            sum_bits: AtomicU64::new(0),
        }
    }

    fn add_to_sum(&self, value: f64) {
        // No atomic offers an f64 add, so read the current bit pattern, add in
        // float space, and CAS it back, retrying if another thread won the race.
        let mut current = self.sum_bits.load(Ordering::Relaxed);
        loop {
            let updated = (f64::from_bits(current) + value).to_bits();
            match self.sum_bits.compare_exchange_weak(
                current,
                updated,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Cumulative `le` buckets, total count, and sum at this instant.
    fn snapshot(&self) -> MetricValue {
        let mut cumulative = 0u64;
        let mut buckets = Vec::with_capacity(self.bounds.len() + 1);
        for (i, bound) in self.bounds.iter().enumerate() {
            cumulative += self.counts[i].load(Ordering::Relaxed);
            buckets.push((format!("{}", bound), cumulative));
        }
        cumulative += self.counts[self.bounds.len()].load(Ordering::Relaxed);
        buckets.push(("+Inf".to_string(), cumulative));
        MetricValue::Histogram {
            buckets,
            count: self.count.load(Ordering::Relaxed),
            sum: f64::from_bits(self.sum_bits.load(Ordering::Relaxed)),
        }
    }
}

impl metrics::HistogramFn for BucketedHistogram {
    fn record(&self, value: f64) {
        let idx = self.bounds.partition_point(|&bound| bound < value);
        self.counts[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.add_to_sum(value);
    }
}

/// Storage backing the registry. Counters and gauges are plain atomics (as in
/// `metrics_util`'s `AtomicStorage`); histograms use [`BucketedHistogram`].
struct LanceStorage;

impl Storage<Key> for LanceStorage {
    type Counter = Arc<AtomicU64>;
    type Gauge = Arc<AtomicU64>;
    type Histogram = Arc<BucketedHistogram>;

    fn counter(&self, _key: &Key) -> Self::Counter {
        Arc::new(AtomicU64::new(0))
    }

    fn gauge(&self, _key: &Key) -> Self::Gauge {
        // The `metrics` facade writes the f64 bit pattern into this `u64` (the
        // snapshot decodes it with `f64::from_bits`), matching `AtomicStorage`.
        // `0` decodes to `0.0`, the correct initial value.
        Arc::new(AtomicU64::new(0))
    }

    fn histogram(&self, key: &Key) -> Self::Histogram {
        Arc::new(BucketedHistogram::new(bounds_for(key.name())))
    }
}

struct LanceRecorder {
    registry: Arc<Registry<Key, LanceStorage>>,
}

impl LanceRecorder {
    fn describe(
        &self,
        key: KeyName,
        kind: MetricKind,
        unit: Option<Unit>,
        description: SharedString,
    ) {
        CATALOG.lock().unwrap().insert(
            key.as_str().to_string(),
            CatalogEntry {
                kind,
                unit: unit.map(|u| u.as_canonical_label().to_string()),
                description: description.into_owned(),
            },
        );
    }
}

impl Recorder for LanceRecorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(key, MetricKind::Counter, unit, description);
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(key, MetricKind::Gauge, unit, description);
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(key, MetricKind::Histogram, unit, description);
    }

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        self.registry
            .get_or_create_counter(key, |c| Counter::from_arc(c.clone()))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        self.registry
            .get_or_create_gauge(key, |g| Gauge::from_arc(g.clone()))
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        self.registry
            .get_or_create_histogram(key, |h| Histogram::from_arc(h.clone()))
    }
}

/// Register the recommended histogram bounds for every metric-emitting
/// subsystem. New subsystems add their `histogram_bounds()` here.
fn register_bounds() {
    let mut bounds = HISTOGRAM_BOUNDS.write().unwrap();
    for (name, values) in lance_io::object_store::metrics::histogram_bounds() {
        bounds.insert((*name).to_string(), Arc::from(*values));
    }
}

/// Describe every metric-emitting subsystem so the catalog is populated. Must
/// run after the recorder is installed. New subsystems add their
/// `describe_metrics()` here.
fn describe_all() {
    lance_io::object_store::metrics::describe_metrics();
}

fn labels(key: &Key) -> HashMap<String, String> {
    key.labels()
        .map(|label| (label.key().to_string(), label.value().to_string()))
        .collect()
}

fn collect_points(registry: &Registry<Key, LanceStorage>) -> Vec<MetricPoint> {
    let mut points = Vec::new();
    for (key, handle) in registry.get_counter_handles() {
        points.push(MetricPoint {
            name: key.name().to_string(),
            kind: MetricKind::Counter,
            attributes: labels(&key),
            // OpenTelemetry observations are float; counts stay well within the
            // f64-exact integer range (2^53), so this cast is lossless in practice.
            value: MetricValue::Scalar(handle.load(Ordering::Relaxed) as f64),
        });
    }
    for (key, handle) in registry.get_gauge_handles() {
        points.push(MetricPoint {
            name: key.name().to_string(),
            kind: MetricKind::Gauge,
            attributes: labels(&key),
            value: MetricValue::Scalar(f64::from_bits(handle.load(Ordering::Relaxed))),
        });
    }
    for (key, handle) in registry.get_histogram_handles() {
        points.push(MetricPoint {
            name: key.name().to_string(),
            kind: MetricKind::Histogram,
            attributes: labels(&key),
            value: handle.snapshot(),
        });
    }
    points
}

/// Install the LanceDB metrics recorder as the process-global `metrics` recorder.
///
/// Returns `true` if the recorder is installed (now or previously). Returns
/// `false` if a *different* recorder is already installed — `metrics` allows
/// only one global recorder per process, so LanceDB cannot coexist with another.
pub fn register_metrics_recorder() -> bool {
    if REGISTRY.get().is_some() {
        return true;
    }
    let registry = Arc::new(Registry::new(LanceStorage));
    let recorder = LanceRecorder {
        registry: registry.clone(),
    };
    // Register bounds *before* installing the recorder. Bounds don't depend on
    // the recorder, and once it is installed a concurrent histogram emission
    // could otherwise create a handle with the fallback bounds and keep them for
    // the process lifetime.
    register_bounds();
    match metrics::set_global_recorder(recorder) {
        Ok(()) => {
            let _ = REGISTRY.set(registry);
            // Describe metrics only after install so the `describe_*!` macros
            // route through this recorder and populate the catalog.
            describe_all();
            true
        }
        Err(_) => false,
    }
}

/// The catalog of described LanceDB metrics. Empty until the recorder is installed.
pub fn metrics_catalog() -> Vec<MetricDescription> {
    CATALOG
        .lock()
        .unwrap()
        .iter()
        .map(|(name, entry)| MetricDescription {
            name: name.clone(),
            kind: entry.kind,
            unit: entry.unit.clone(),
            description: entry.description.clone(),
        })
        .collect()
}

/// A point-in-time snapshot of every recorded metric. Empty until the recorder
/// is installed. The read is lock-free.
pub fn snapshot_metrics() -> Vec<MetricPoint> {
    let Some(registry) = REGISTRY.get() else {
        return Vec::new();
    };
    collect_points(registry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::HistogramFn;

    fn bucket_count(buckets: &[(String, u64)], le: &str) -> u64 {
        buckets
            .iter()
            .find(|(b, _)| b == le)
            .map(|(_, c)| *c)
            .unwrap_or_else(|| panic!("no bucket with le={le}"))
    }

    #[test]
    fn bucketed_histogram_records_cumulative_buckets() {
        let hist = BucketedHistogram::new(Arc::from([0.1f64, 1.0, 10.0].as_slice()));
        hist.record(0.05); // le=0.1
        hist.record(0.5); // le=1
        hist.record(0.5); // le=1
        hist.record(50.0); // +Inf

        let MetricValue::Histogram {
            buckets,
            count,
            sum,
        } = hist.snapshot()
        else {
            panic!("expected histogram");
        };

        // Buckets are cumulative (Prometheus `le` semantics).
        assert_eq!(bucket_count(&buckets, "0.1"), 1);
        assert_eq!(bucket_count(&buckets, "1"), 3);
        assert_eq!(bucket_count(&buckets, "10"), 3);
        assert_eq!(bucket_count(&buckets, "+Inf"), 4);
        assert_eq!(count, 4);
        assert!((sum - 51.05).abs() < 1e-9);
    }

    #[test]
    fn bucketed_histogram_boundary_is_inclusive() {
        let hist = BucketedHistogram::new(Arc::from([1.0f64].as_slice()));
        hist.record(1.0); // exactly the bound -> le=1, not +Inf
        let MetricValue::Histogram { buckets, .. } = hist.snapshot() else {
            panic!("expected histogram");
        };
        assert_eq!(bucket_count(&buckets, "1"), 1);
        assert_eq!(bucket_count(&buckets, "+Inf"), 1);
    }

    #[test]
    fn bucketed_histogram_boundary_is_inclusive_mid_range() {
        // A value equal to a middle bound lands in that bucket, not the next.
        let hist = BucketedHistogram::new(Arc::from([0.1f64, 1.0, 10.0].as_slice()));
        hist.record(1.0);
        let MetricValue::Histogram { buckets, .. } = hist.snapshot() else {
            panic!("expected histogram");
        };
        assert_eq!(bucket_count(&buckets, "0.1"), 0);
        assert_eq!(bucket_count(&buckets, "1"), 1);
        assert_eq!(bucket_count(&buckets, "10"), 1); // cumulative, so still 1
        assert_eq!(bucket_count(&buckets, "+Inf"), 1);
    }

    #[test]
    fn recorder_aggregates_counters_with_labels() {
        let registry = Arc::new(Registry::new(LanceStorage));
        let recorder = LanceRecorder {
            registry: registry.clone(),
        };
        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("test_requests_total", "operation" => "get", "scheme" => "s3")
                .increment(2);
            metrics::counter!("test_requests_total", "operation" => "get", "scheme" => "s3")
                .increment(3);
            // A distinct label set must produce a separate point, not merge.
            metrics::counter!("test_requests_total", "operation" => "put", "scheme" => "gs")
                .increment(7);
        });

        let scalar = |attrs: &[(&str, &str)]| {
            let points = collect_points(&registry);
            let point = points
                .into_iter()
                .find(|p| {
                    p.name == "test_requests_total"
                        && attrs
                            .iter()
                            .all(|(k, v)| p.attributes.get(*k).map(String::as_str) == Some(*v))
                })
                .expect("counter recorded for label set");
            assert_eq!(point.kind, MetricKind::Counter);
            match point.value {
                MetricValue::Scalar(v) => v,
                _ => panic!("expected scalar"),
            }
        };

        // Same labels aggregate; distinct labels stay separate.
        assert!((scalar(&[("operation", "get"), ("scheme", "s3")]) - 5.0).abs() < 1e-9);
        assert!((scalar(&[("operation", "put"), ("scheme", "gs")]) - 7.0).abs() < 1e-9);
    }

    #[test]
    fn recorder_records_gauges() {
        let registry = Arc::new(Registry::new(LanceStorage));
        let recorder = LanceRecorder {
            registry: registry.clone(),
        };
        // Gauges store the f64 bit pattern in a u64; the snapshot must decode it.
        metrics::with_local_recorder(&recorder, || {
            metrics::gauge!("test_gauge", "scheme" => "s3").set(3.5);
        });

        let points = collect_points(&registry);
        let point = points
            .iter()
            .find(|p| p.name == "test_gauge")
            .expect("gauge recorded");
        assert_eq!(point.kind, MetricKind::Gauge);
        assert!(matches!(point.value, MetricValue::Scalar(v) if (v - 3.5).abs() < 1e-9));
    }

    #[test]
    fn recorder_falls_back_to_default_bounds() {
        // A histogram with no registered bounds uses DEFAULT_BOUNDS.
        let name = "test_unregistered_histogram";
        assert!(!HISTOGRAM_BOUNDS.read().unwrap().contains_key(name));

        let registry = Arc::new(Registry::new(LanceStorage));
        let recorder = LanceRecorder {
            registry: registry.clone(),
        };
        metrics::with_local_recorder(&recorder, || {
            metrics::histogram!(name).record(0.02);
        });

        let points = collect_points(&registry);
        let point = points.iter().find(|p| p.name == name).expect("recorded");
        let MetricValue::Histogram { buckets, count, .. } = &point.value else {
            panic!("expected histogram");
        };
        assert_eq!(*count, 1);
        // DEFAULT_BOUNDS yields one bucket per bound plus the implicit `+Inf`.
        assert_eq!(buckets.len(), DEFAULT_BOUNDS.len() + 1);
        // 0.02 falls in the le=0.025 bucket (the third DEFAULT_BOUNDS entry).
        assert_eq!(bucket_count(buckets, "0.025"), 1);
        assert_eq!(bucket_count(buckets, "0.01"), 0);
        assert_eq!(bucket_count(buckets, "+Inf"), 1);
    }

    #[test]
    fn recorder_uses_registered_histogram_bounds() {
        let name = "test_recorder_bounds_seconds";
        HISTOGRAM_BOUNDS
            .write()
            .unwrap()
            .insert(name.to_string(), Arc::from([0.1f64, 1.0].as_slice()));

        let registry = Arc::new(Registry::new(LanceStorage));
        let recorder = LanceRecorder {
            registry: registry.clone(),
        };
        metrics::with_local_recorder(&recorder, || {
            metrics::histogram!(name).record(0.05);
            metrics::histogram!(name).record(5.0);
        });

        let points = collect_points(&registry);
        let point = points.iter().find(|p| p.name == name).expect("recorded");
        let MetricValue::Histogram { buckets, count, .. } = &point.value else {
            panic!("expected histogram");
        };
        assert_eq!(*count, 2);
        assert_eq!(bucket_count(buckets, "0.1"), 1);
        assert_eq!(bucket_count(buckets, "+Inf"), 2);
    }

    #[test]
    fn describe_populates_catalog() {
        let name = "test_describe_catalog_total";
        let registry = Arc::new(Registry::new(LanceStorage));
        let recorder = LanceRecorder { registry };
        metrics::with_local_recorder(&recorder, || {
            metrics::describe_counter!(name, Unit::Count, "a test counter");
        });

        let catalog = CATALOG.lock().unwrap();
        let entry = catalog.get(name).expect("described");
        assert_eq!(entry.kind, MetricKind::Counter);
        assert_eq!(entry.description, "a test counter");
    }
}
