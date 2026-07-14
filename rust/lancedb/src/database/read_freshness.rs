// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Read-freshness signaling for the lance-namespace path.
//!
//! Against a server that serves cached table metadata up to some staleness
//! window, a handle that just wrote (or asked for the latest version via
//! `checkout_latest`) can still read a stale snapshot. To prevent that, reads
//! routed through the namespace client carry an `x-lancedb-min-timestamp`
//! header naming the oldest snapshot the caller will accept.
//!
//! This mirrors `remote::table`: a per-table baseline is bumped to "now" on
//! every write and on `checkout_latest()`, and reads send
//! `max(baseline, now - read_consistency_interval)`. Since the namespace client
//! takes no headers directly, a [`DynamicContextProvider`] injects it per request.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use lance_namespace_impls::{DynamicContextProvider, OperationInfo};

/// Provider context keys prefixed with `headers.` become HTTP headers (prefix
/// stripped), so this emits the `x-lancedb-min-timestamp` header.
const MIN_TIMESTAMP_CONTEXT_KEY: &str = "headers.x-lancedb-min-timestamp";

/// Per-table freshness baselines (keyed by namespace object id), shared between
/// the provider that reads them and the table handles that bump them.
pub type FreshnessBaselines = Arc<Mutex<HashMap<String, SystemTime>>>;

/// `max(baseline, now - interval)`, or `None` when neither constraint applies.
fn compute_min_timestamp(
    baseline: Option<SystemTime>,
    interval: Option<Duration>,
    now: SystemTime,
) -> Option<SystemTime> {
    let interval_based = match interval {
        None => None,
        Some(d) if d.is_zero() => Some(now),
        Some(d) => Some(now.checked_sub(d).unwrap_or(now)),
    };
    match (interval_based, baseline) {
        (None, None) => None,
        (Some(t), None) | (None, Some(t)) => Some(t),
        (Some(a), Some(b)) => Some(a.max(b)),
    }
}

/// Advance the baseline to `now`, never backwards, so a concurrent handle's
/// write can't lower a floor another handle already set.
fn next_freshness_baseline(prev: Option<SystemTime>, now: SystemTime) -> SystemTime {
    match prev {
        Some(p) => p.max(now),
        None => now,
    }
}

/// A handle's view of the shared baseline map for a single table.
#[derive(Clone, Debug)]
pub struct TableFreshness {
    baselines: FreshnessBaselines,
    /// Namespace object id for this table (matches the read's `object_id`).
    key: String,
}

impl TableFreshness {
    pub fn new(baselines: FreshnessBaselines, key: String) -> Self {
        Self { baselines, key }
    }

    pub fn bump(&self) {
        let now = SystemTime::now();
        let mut baselines = self.baselines.lock().unwrap();
        let prev = baselines.get(&self.key).copied();
        baselines.insert(self.key.clone(), next_freshness_baseline(prev, now));
    }
}

/// Read ops that can be served stale and so carry the freshness floor.
/// `list_table_versions` resolves "latest" for managed-versioning tables, so it
/// is what makes `checkout_latest()` observe a prior write.
fn is_read_operation(operation: &str) -> bool {
    matches!(
        operation,
        "describe_table" | "list_table_versions" | "query_table" | "list_tables"
    )
}

/// Injects `x-lancedb-min-timestamp` on namespace reads, per addressed table.
#[derive(Debug)]
pub struct ReadFreshnessContextProvider {
    baselines: FreshnessBaselines,
    read_consistency_interval: Option<Duration>,
}

impl ReadFreshnessContextProvider {
    pub fn new(baselines: FreshnessBaselines, read_consistency_interval: Option<Duration>) -> Self {
        Self {
            baselines,
            read_consistency_interval,
        }
    }
}

impl DynamicContextProvider for ReadFreshnessContextProvider {
    fn provide_context(&self, info: &OperationInfo) -> HashMap<String, String> {
        if !is_read_operation(&info.operation) {
            return HashMap::new();
        }

        let baseline = self.baselines.lock().unwrap().get(&info.object_id).copied();
        match compute_min_timestamp(baseline, self.read_consistency_interval, SystemTime::now()) {
            Some(ts) => {
                let dt: chrono::DateTime<chrono::Utc> = ts.into();
                HashMap::from([(MIN_TIMESTAMP_CONTEXT_KEY.to_string(), dt.to_rfc3339())])
            }
            None => HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Allowed slop when comparing a header timestamp against a locally
    /// captured wall-clock bound. Tests run fast enough that 1s is plenty.
    const TOLERANCE: Duration = Duration::from_secs(1);

    fn parse_header_ts(headers: &HashMap<String, String>) -> SystemTime {
        let value = headers
            .get(MIN_TIMESTAMP_CONTEXT_KEY)
            .expect("expected min-timestamp context key");
        chrono::DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&chrono::Utc)
            .into()
    }

    #[test]
    fn test_compute_min_timestamp_combines_baseline_and_interval() {
        let now = SystemTime::now();
        let baseline = now - Duration::from_secs(60);

        // No interval, no baseline -> no header.
        assert_eq!(compute_min_timestamp(None, None, now), None);

        // Baseline only -> baseline.
        assert_eq!(
            compute_min_timestamp(Some(baseline), None, now),
            Some(baseline)
        );

        // ZERO interval, no baseline -> now (strong consistency).
        assert_eq!(
            compute_min_timestamp(None, Some(Duration::ZERO), now),
            Some(now)
        );

        // Positive interval, no baseline -> now - interval.
        assert_eq!(
            compute_min_timestamp(None, Some(Duration::from_secs(10)), now),
            Some(now - Duration::from_secs(10))
        );

        // Both: pick the more-recent (tighter) constraint.
        // baseline = now-60, now-interval = now-10. now-10 is newer.
        assert_eq!(
            compute_min_timestamp(Some(baseline), Some(Duration::from_secs(10)), now),
            Some(now - Duration::from_secs(10))
        );

        // Both, baseline newer: pick baseline.
        let recent_baseline = now - Duration::from_secs(5);
        assert_eq!(
            compute_min_timestamp(Some(recent_baseline), Some(Duration::from_secs(60)), now),
            Some(recent_baseline)
        );
    }

    #[test]
    fn test_next_freshness_baseline_is_monotonic() {
        let now = SystemTime::now();
        let earlier = now - Duration::from_secs(30);
        let later = now + Duration::from_secs(30);

        // No prior baseline -> now.
        assert_eq!(next_freshness_baseline(None, now), now);
        // Prior baseline older than now -> now.
        assert_eq!(next_freshness_baseline(Some(earlier), now), now);
        // Prior baseline newer than now -> keep the newer baseline.
        assert_eq!(next_freshness_baseline(Some(later), now), later);
    }

    fn provider_with(
        entries: &[(&str, SystemTime)],
        interval: Option<Duration>,
    ) -> ReadFreshnessContextProvider {
        let map: HashMap<String, SystemTime> =
            entries.iter().map(|(k, v)| (k.to_string(), *v)).collect();
        ReadFreshnessContextProvider::new(Arc::new(Mutex::new(map)), interval)
    }

    #[test]
    fn test_provider_emits_header_at_or_after_bumped_baseline() {
        // A baseline set "now" with no interval: every read op must carry a
        // floor at or after that baseline. `list_table_versions` is the hook
        // that makes managed-versioning `checkout_latest()` observe a write.
        let baseline = SystemTime::now();
        let provider = provider_with(&[("ns$tbl", baseline)], None);

        // These ops are keyed by the table id, so they pick up the per-table
        // baseline. (`list_tables` is keyed by the namespace, so it is covered
        // separately by the interval-floor test.)
        for op in ["describe_table", "list_table_versions", "query_table"] {
            let ctx = provider.provide_context(&OperationInfo::new(op, "ns$tbl"));
            let sent = parse_header_ts(&ctx);
            assert!(
                sent >= baseline - TOLERANCE && sent <= baseline + TOLERANCE,
                "operation {op} should carry a floor at the bumped baseline"
            );
        }
    }

    #[test]
    fn test_provider_list_tables_uses_interval_floor_not_table_baseline() {
        // `list_tables` is addressed by the namespace id, which never matches a
        // per-table baseline key, so a bumped table baseline must not leak onto
        // it. With no interval it sends nothing; with one it sends now-interval.
        let provider = provider_with(&[("ns$tbl", SystemTime::now())], None);
        let ctx = provider.provide_context(&OperationInfo::new("list_tables", "ns"));
        assert!(
            ctx.is_empty(),
            "list_tables must not inherit a per-table baseline"
        );

        let interval = Duration::from_secs(30);
        let provider = provider_with(&[("ns$tbl", SystemTime::now())], Some(interval));
        let before = SystemTime::now();
        let ctx = provider.provide_context(&OperationInfo::new("list_tables", "ns"));
        let after = SystemTime::now();
        let sent = parse_header_ts(&ctx);
        assert!(
            sent >= before - interval - TOLERANCE && sent <= after - interval + TOLERANCE,
            "list_tables should carry the interval floor"
        );
    }

    #[test]
    fn test_provider_no_header_for_empty_baseline_and_no_interval() {
        // Manual consistency (no interval) on a table that was never bumped:
        // no floor, so the server may serve from cache.
        let provider = provider_with(&[], None);
        let ctx = provider.provide_context(&OperationInfo::new("describe_table", "ns$tbl"));
        assert!(ctx.is_empty());
    }

    #[test]
    fn test_provider_interval_floor_applies_without_baseline() {
        // With a consistency interval and no baseline, the floor is now-interval.
        let interval = Duration::from_secs(30);
        let provider = provider_with(&[], Some(interval));

        let before = SystemTime::now();
        let ctx = provider.provide_context(&OperationInfo::new("query_table", "ns$tbl"));
        let after = SystemTime::now();

        let sent = parse_header_ts(&ctx);
        assert!(
            sent >= before - interval - TOLERANCE && sent <= after - interval + TOLERANCE,
            "expected floor at roughly now - interval"
        );
    }

    #[test]
    fn test_provider_non_read_ops_emit_nothing() {
        // Even with a fresh baseline and a zero interval, a non-read operation
        // (which establishes rather than consumes a baseline) sends no header.
        let provider = provider_with(&[("ns$tbl", SystemTime::now())], Some(Duration::ZERO));
        for op in [
            "create_table",
            "register_table",
            "drop_table",
            "rename_table",
            // Pinned to an immutable version, so it cannot be served stale.
            "describe_table_version",
        ] {
            let ctx = provider.provide_context(&OperationInfo::new(op, "ns$tbl"));
            assert!(
                ctx.is_empty(),
                "operation {op} must not send a freshness header"
            );
        }
    }

    #[test]
    fn test_provider_uses_per_table_baseline() {
        // The floor is looked up by object id, so an unrelated table's baseline
        // does not leak onto another table's read.
        let baseline = SystemTime::now();
        let provider = provider_with(&[("ns$has_baseline", baseline)], None);

        // The bumped table gets a header.
        let hit =
            provider.provide_context(&OperationInfo::new("describe_table", "ns$has_baseline"));
        assert!(!hit.is_empty());

        // A different table with no baseline (and no interval) gets nothing.
        let miss = provider.provide_context(&OperationInfo::new("describe_table", "ns$other"));
        assert!(miss.is_empty());
    }
}
