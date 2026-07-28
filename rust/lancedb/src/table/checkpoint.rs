// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Converging a table's LSM write path into its base table.
//!
//! `checkpoint_lsm` is `flush` then `compact`, repeated until the fresh
//! tier is empty — and the loop runs **here, in the client**, not on the
//! server. Putting it server-side would mean a background task, which
//! means a single-flight intent, an intent that leaks on panic, a
//! bounded-iteration policy, an "is it done" observable, and a story for
//! every way a client can vanish mid-operation. None of that exists in
//! this shape.
//!
//! Three properties follow:
//!
//! * **No held socket.** Each request does a bounded unit of work and
//!   returns. Nothing sits on a connection for minutes waiting on a merge.
//! * **Termination is structural.** Each `compact` drains at most one
//!   prefix per bucket and reports what is left. A caller that wants to
//!   stop, stops.
//! * **Completion is carried in the responses, not inferred.** A poll-a
//!   -shared-counter design cannot distinguish "converged" from "hasn't
//!   started yet"; being *told* `generations_remaining == 0` can.
//!
//! **Best-effort by construction.** Nothing is frozen, so a checkpoint
//! converges the fresh tier *as of some instant*; with writes flowing
//! there may be new rows by the time it returns. That is the correct
//! contract for a checkpoint and exactly Postgres's. It is idempotent,
//! abandonable at any point with zero consequence, and safe to run on a
//! cadence.

use std::collections::HashMap;
use std::time::Duration;

use serde::Deserialize;

/// Options for [`crate::Table::checkpoint_lsm`].
#[derive(Debug, Clone)]
pub struct CheckpointOptions {
    /// Stop and report `converged: false` once this much wall-clock has
    /// elapsed. `None` runs until the table converges or a terminal error
    /// arrives — safe only on a table you know is not under write load.
    pub deadline: Option<Duration>,
    /// Per-request, per-bucket bound on generations merged. `None` uses the
    /// server's `compact_prefix_max`. Note this is **per bucket**: an
    /// N-bucket table admits `N × max_generations_per_bucket` merges per
    /// request. [`LsmStats::bucket_count`] reports N.
    pub max_generations_per_bucket: Option<usize>,
    /// Collect a [`LsmStats`] block into the report when the loop ends.
    pub include_stats: bool,
}

impl Default for CheckpointOptions {
    fn default() -> Self {
        Self {
            deadline: Some(Duration::from_secs(300)),
            max_generations_per_bucket: None,
            include_stats: false,
        }
    }
}

/// Why a checkpoint stopped short of convergence. Absent ⇒ it converged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointStopReason {
    /// The caller's deadline elapsed with generations still in L0. A
    /// *result*, not an error: a table under sustained write load may
    /// legitimately never reach zero. Callers on a cadence ignore it;
    /// callers about to disable the WAL check it.
    DeadlineExceeded,
    /// The owning node is draining. **Terminal** — the drain gate is a
    /// one-way latch that never releases until the process restarts, so
    /// retrying against that node spins until the deadline and then reports
    /// failure when the truthful answer was available on the first
    /// response. It resolves only when the ring changes.
    ///
    /// Stopping is also the *correct* answer: a node drain is a checkpoint
    /// of every claimed table on the pod, driven by something that will
    /// finish it.
    NodeDraining,
    /// The registry entry vanished repeatedly under pod restarts. A pod in
    /// a crash loop would otherwise turn flush → compact → 404 → flush into
    /// a spin that burns the whole deadline making no progress.
    RepeatedlyUnclaimed,
}

/// Outcome of a [`crate::Table::checkpoint_lsm`] run.
#[derive(Debug, Clone)]
pub struct CheckpointReport {
    /// L0 was empty across every bucket as of the last pass.
    ///
    /// Precisely: the loop seals once at the top, so rows written *during*
    /// the loop sit in the active memtable and are not sealed again.
    /// Writes are never blocked to make this look better — a caller who
    /// wants a quiescent table quiesces it.
    pub converged: bool,
    /// Set when `converged` is false.
    pub stop_reason: Option<CheckpointStopReason>,
    /// Rows sealed out of the memtables by the single opening flush.
    pub rows_sealed: u64,
    /// L0 generations merged into base across every pass.
    pub generations_compacted: usize,
    /// Rows merged into base across every pass.
    pub rows_merged: u64,
    /// Generations still in L0 as of the last response.
    pub generations_remaining: usize,
    /// Number of `compact` round trips issued. Zero when the opening flush
    /// landed in an empty L0 — the common case on a cadence.
    pub compact_calls: usize,
    /// Present when [`CheckpointOptions::include_stats`] was set.
    pub stats: Option<LsmStats>,
}

/// Per-bucket result of one flush.
#[derive(Debug, Clone, Deserialize)]
pub struct FlushBucketReport {
    pub shard_id: String,
    /// `None` when the bucket's active memtable was already empty.
    #[serde(default)]
    pub sealed_generation: Option<u64>,
    pub rows_sealed: u64,
    pub generations_remaining: usize,
}

/// Result of [`crate::Table::flush_lsm`]: everything written before the
/// seal is now in L0.
#[derive(Debug, Clone, Deserialize)]
pub struct FlushReport {
    pub buckets: Vec<FlushBucketReport>,
    pub rows_sealed: u64,
    /// Summed across buckets, so `== 0` means every bucket is empty. This
    /// is what lets an already-converged table cost one round trip and zero
    /// compaction passes.
    pub generations_remaining: usize,
}

/// Options for [`crate::Table::compact_lsm`].
#[derive(Debug, Clone, Default)]
pub struct CompactOptions {
    /// See [`CheckpointOptions::max_generations_per_bucket`].
    pub max_generations_per_bucket: Option<usize>,
}

/// Per-bucket result of one compaction pass.
#[derive(Debug, Clone, Deserialize)]
pub struct CompactBucketReport {
    pub shard_id: String,
    pub generations_consumed: usize,
    pub rows_merged: u64,
    pub generations_remaining: usize,
}

/// Result of [`crate::Table::compact_lsm`]: one bounded L0 → base pass per
/// bucket.
///
/// The per-bucket list is authoritative; the table-level fields are a
/// convenience. Consumed/merged/remaining are **sums**;
/// `highest_wal_entry_position` is a **max**, since it is a watermark.
#[derive(Debug, Clone, Deserialize)]
pub struct CompactReport {
    pub buckets: Vec<CompactBucketReport>,
    pub generations_consumed: usize,
    pub rows_merged: u64,
    pub generations_remaining: usize,
    pub highest_wal_entry_position: u64,
}

/// Options for [`crate::Table::get_lsm_stats`].
#[derive(Debug, Clone, Default)]
pub struct LsmStatsOptions {
    /// Report `rows` per L0 generation. Off by default: the shard manifest
    /// stores only `{generation, path}`, so rows cost one Lance manifest
    /// read per generation.
    pub include_generation_rows: bool,
}

/// One flushed L0 generation.
#[derive(Debug, Clone, Deserialize)]
pub struct GenerationStats {
    pub generation: u64,
    pub path: String,
    pub bytes: u64,
    /// Present only when [`LsmStatsOptions::include_generation_rows`] was set.
    #[serde(default)]
    pub rows: Option<u64>,
}

/// One in-memory memtable.
#[derive(Debug, Clone, Deserialize)]
pub struct MemtableStats {
    pub generation: u64,
    pub rows: u64,
    pub bytes: u64,
    pub batches: u64,
}

/// An index the memtable carries. An absent `hnsw` entry on a vector
/// column is the whole answer to "why is my fresh-tier vector search
/// brute-force".
#[derive(Debug, Clone, Deserialize)]
pub struct MemIndexStats {
    pub name: String,
    /// `btree` | `hnsw` | `fts`.
    pub kind: String,
    pub column: String,
}

/// Live state of one bucket. A table is N buckets on one node; flattening
/// to a single number hides the one hot bucket that is usually why someone
/// opened this endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct BucketStats {
    pub shard_id: String,
    /// `Active` | `Sealed` (drop-table 2PC in flight).
    pub status: String,
    pub writer_epoch: u64,
    pub manifest_version: u64,
    pub current_generation: u64,
    pub replay_after_wal_entry_position: u64,
    pub wal_entry_position_last_seen: u64,
    /// Accepted WAL entries not yet covered by a flush.
    pub wal_lag: u64,
    pub generations: Vec<GenerationStats>,
    pub l0_bytes: u64,
    pub fenced: bool,
    pub compaction_in_progress: bool,
    /// Absent for a `Sealed` bucket, whose in-memory state is torn down.
    #[serde(default)]
    pub active_memtable: Option<MemtableStats>,
    #[serde(default)]
    pub frozen_memtables: Option<Vec<MemtableStats>>,
    #[serde(default)]
    pub memtable_indexes: Option<Vec<MemIndexStats>>,
}

/// Live LSM state: per-bucket detail plus a table-level aggregate.
///
/// Every field is measured. There is no "WAL is off" shape here — that
/// case is `None` from [`crate::Table::get_lsm_stats`], because a struct of
/// zeros would read as measurements.
#[derive(Debug, Clone, Deserialize)]
pub struct LsmStats {
    pub buckets: Vec<BucketStats>,
    pub bucket_count: usize,
    pub generations_total: usize,
    pub l0_bytes_total: u64,
    pub memtable_rows_total: u64,
}

/// Server-side JSON envelope for `get_lsm_stats`. `lsm_stats` is null when
/// the table has no LSM write path.
#[derive(Debug, Deserialize)]
pub(crate) struct GetLsmStatsResponse {
    #[serde(default)]
    pub lsm_stats: Option<LsmStats>,
}

/// How a failed LSM request should be handled by the checkpoint loop.
///
/// Five distinct conditions used to arrive at a client as one 503. This is
/// the classification that keeps them apart, and it reads the body's `code`
/// on 503 only — every other status is already unambiguous.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LsmFault {
    /// 429, or 503 with `ServiceUnavailable`: contention, a fenced writer,
    /// no ready slot, or transport. Retry with backoff.
    ///
    /// Fenced and slot-unavailable are deliberately not distinguished from
    /// contention here: the client action is identical.
    Retry,
    /// 404: the registry entry vanished under a node restart mid-loop.
    /// `flush` is the call that re-claims and replays, so re-issue from
    /// there — with a cap, or a crash-looping node turns the loop into a
    /// spin.
    ReissueFromFlush,
    /// 503 with `InvalidTableState`: the owning node is draining. Terminal.
    Draining,
    /// 409 (dropping), 400 (not WAL-backed), or anything else. Terminal.
    Fatal,
}

/// Lance-namespace `ErrorCode::InvalidTableState`. Nothing else in the
/// server maps to it, which is what makes it usable as the draining signal:
/// a `WalDraining` variant that mapped to the generic service-unavailable
/// code would be byte-identical to every other 503 on the wire, leaving
/// this classifier matching message strings — exactly what a string `code`
/// field was rejected to avoid.
pub(crate) const CODE_INVALID_TABLE_STATE: u64 = 19;

/// Classify an LSM-route failure from its status and response body.
///
/// The body must be inspected **before** any helper that folds it into a
/// string and keeps only the status, or the discrimination designed across
/// three crates is lost at the last hop.
pub(crate) fn classify_lsm_fault(status: u16, body: &str) -> LsmFault {
    match status {
        429 => LsmFault::Retry,
        404 => LsmFault::ReissueFromFlush,
        503 => {
            let code = serde_json::from_str::<HashMap<String, serde_json::Value>>(body)
                .ok()
                .and_then(|m| m.get("code").and_then(|c| c.as_u64()));
            if code == Some(CODE_INVALID_TABLE_STATE) {
                LsmFault::Draining
            } else {
                LsmFault::Retry
            }
        }
        _ => LsmFault::Fatal,
    }
}

/// What the checkpoint loop should do next after a failed request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckpointControl {
    /// Re-issue the same call after a backoff.
    Retry,
    /// Restart from `flush`, which is what re-claims and replays.
    ReissueFromFlush,
    /// Stop; `report.stop_reason` has been set.
    Stop,
}

/// Base backoff between retries, doubled per consecutive retry up to
/// [`RETRY_BACKOFF_MAX`]. Latch contention clears in about the time one
/// compaction pass takes, so starting small is right; a saturated pool
/// wants the ceiling.
const RETRY_BACKOFF_BASE: Duration = Duration::from_millis(100);
const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// Decide what a failed LSM request means for the loop, recording the stop
/// reason on `report` when it is terminal. Returns `Err` only for genuinely
/// fatal faults (dropping table, not WAL-backed, anything unrecognized) —
/// those are errors the caller should see, not outcomes.
pub(crate) async fn checkpoint_fault(
    e: crate::Error,
    report: &mut CheckpointReport,
    attempt: usize,
    max_reissues: usize,
) -> crate::Result<CheckpointControl> {
    #[cfg(feature = "remote")]
    let fault = match &e {
        crate::Error::LsmRoute { fault, .. } => Some(*fault),
        _ => None,
    };
    #[cfg(not(feature = "remote"))]
    let fault: Option<LsmFault> = None;

    match fault {
        Some(LsmFault::Retry) => {
            let backoff = RETRY_BACKOFF_BASE
                .saturating_mul(1u32 << attempt.min(8) as u32)
                .min(RETRY_BACKOFF_MAX);
            tokio::time::sleep(backoff).await;
            Ok(CheckpointControl::Retry)
        }
        Some(LsmFault::ReissueFromFlush) if attempt < max_reissues => {
            Ok(CheckpointControl::ReissueFromFlush)
        }
        Some(LsmFault::ReissueFromFlush) => {
            report.stop_reason = Some(CheckpointStopReason::RepeatedlyUnclaimed);
            Ok(CheckpointControl::Stop)
        }
        Some(LsmFault::Draining) => {
            report.stop_reason = Some(CheckpointStopReason::NodeDraining);
            Ok(CheckpointControl::Stop)
        }
        Some(LsmFault::Fatal) | None => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every row of the taxonomy, asserted on the `(status, code)` pair the
    /// client actually receives. A `_ =>` arm added later passes any test
    /// that only asserts "it errored".
    #[test]
    fn taxonomy_round_trips() {
        assert_eq!(classify_lsm_fault(429, r#"{"code":21}"#), LsmFault::Retry);
        assert_eq!(
            classify_lsm_fault(503, r#"{"code":19}"#),
            LsmFault::Draining,
            "a draining node must be told apart from every other 503"
        );
        assert_eq!(
            classify_lsm_fault(503, r#"{"code":17}"#),
            LsmFault::Retry,
            "fenced / no-slot / transport stay retryable"
        );
        assert_eq!(
            classify_lsm_fault(404, r#"{"code":4}"#),
            LsmFault::ReissueFromFlush
        );
        assert_eq!(classify_lsm_fault(409, r#"{"code":14}"#), LsmFault::Fatal);
        assert_eq!(classify_lsm_fault(400, r#"{"code":13}"#), LsmFault::Fatal);
    }

    /// A 503 whose body is missing, truncated, or not the expected JSON must
    /// fall back to *retryable*, never to terminal: mistaking a healthy node
    /// for a draining one aborts the checkpoint on a lie.
    #[test]
    fn unparseable_503_body_is_retryable() {
        assert_eq!(classify_lsm_fault(503, ""), LsmFault::Retry);
        assert_eq!(classify_lsm_fault(503, "gateway timeout"), LsmFault::Retry);
        assert_eq!(classify_lsm_fault(503, r#"{"code":"19"}"#), LsmFault::Retry);
    }
}
