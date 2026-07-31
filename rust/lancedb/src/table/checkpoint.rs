// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Converging a table's LSM write path into its base table.
//!
//! `checkpoint_lsm` seals once, then triggers compaction and watches
//! generation numbers until the L0 that existed at the start is gone. The
//! loop runs **here, in the client**, not on the server: putting it
//! server-side would mean a background task, a single-flight intent, an
//! intent that leaks on panic, an "is it done" observable, and a story for
//! every way a client can vanish mid-operation.
//!
//! Three properties follow:
//!
//! * **No held socket.** `compact_lsm` dispatches a pass and returns; the
//!   merge runs on the server's compactor pool. Nothing sits on a
//!   connection for minutes.
//! * **The predicate is durable state, not a response body.** Completion
//!   is read from generation numbers in the shard manifest via
//!   `get_lsm_stats`. A count carried back in a compact response is stale
//!   the moment a concurrent write lands; a generation number is not.
//! * **Termination is reachable under write load.** The target set is
//!   fixed at the start, so generations created *during* the checkpoint
//!   are ignored. A predicate of "L0 is empty" chases a moving target on a
//!   table taking writes and only ever exits on a timeout.
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

/// One flushed L0 generation.
#[derive(Debug, Clone, Deserialize)]
pub struct GenerationStats {
    pub generation: u64,
    pub bytes: u64,
    /// Present only when `include_generation_rows` was requested. Off by
    /// default because each count opens an uncached Lance dataset, and the
    /// checkpoint loop polls this route needing only generation numbers.
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
    /// Names of the indexes this memtable carries. An absent name is the
    /// whole answer to "why is my fresh-tier search on that column
    /// brute-force"; cross-reference `list_indices` for its kind and column.
    pub indexes: Vec<String>,
}

/// Live state of one bucket. A table is N buckets on one node; flattening
/// to a single number hides the one hot bucket that is usually why someone
/// opened this endpoint.
///
/// Nothing here is derived — sums and differences (total L0 bytes, WAL lag)
/// are the caller's to compute from measured fields.
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
    pub generations: Vec<GenerationStats>,
    /// Whether a pass owns this bucket's compaction latch right now. Says
    /// *a* driver is running, not *whose*: the server's periodic trigger
    /// competes for the same latch, so a caller that dispatched a compact
    /// cannot read this as "mine is progressing", only as "do not pile on".
    pub compacting: bool,
    /// Oldest first, active last. Absent for a `Sealed` bucket, whose
    /// in-memory state is torn down.
    #[serde(default)]
    pub memtables: Option<Vec<MemtableStats>>,
}

impl BucketStats {
    /// The newest flushed generation, or `None` when L0 is empty.
    pub(crate) fn newest_generation(&self) -> Option<u64> {
        self.generations.iter().map(|g| g.generation).max()
    }

    /// How many generations at or below `target` are still in L0.
    ///
    /// A *count*, not a boolean: it is the checkpoint's progress metric,
    /// and one pass drains a bounded prefix rather than the whole target
    /// set. A per-bucket boolean would read as "no progress" for every
    /// pass but the last, so a bucket needing more passes than the idle
    /// bound would fail a checkpoint that was working correctly.
    /// Compaction drains oldest-first, so this decreases monotonically.
    pub(crate) fn outstanding_generations(&self, target: u64) -> usize {
        self.generations
            .iter()
            .filter(|g| g.generation <= target)
            .count()
    }
}

/// Live LSM state, one entry per bucket.
///
/// Every field is measured. There is no "WAL is off" shape here — that
/// case is `None` from [`crate::Table::get_lsm_stats`], because a struct of
/// zeros would read as measurements.
#[derive(Debug, Clone, Deserialize)]
pub struct LsmStats {
    pub buckets: Vec<BucketStats>,
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
        // A 404 is a registry miss only when the body says so. Left
        // unchecked, a typo'd table name — also a 404 — is read as "the
        // node lost its claim", and the loop re-issues from flush until the
        // cap and then blames a crash loop for a name that never existed.
        // `send_lsm_route` turns the other 404 into `TableNotFound` before
        // this is reached; the check stays so a direct caller cannot
        // reintroduce the confusion.
        404 if body_code(body) == Some(CODE_INVALID_TABLE_STATE) => LsmFault::ReissueFromFlush,
        404 => LsmFault::Fatal,
        503 if body_code(body) == Some(CODE_INVALID_TABLE_STATE) => LsmFault::Draining,
        503 => LsmFault::Retry,
        _ => LsmFault::Fatal,
    }
}

/// The lance-namespace error code from a JSON error body, if it parses.
/// Absent, truncated, or non-numeric ⇒ `None`, which every caller reads as
/// the *less* terminal branch.
fn body_code(body: &str) -> Option<u64> {
    serde_json::from_str::<HashMap<String, serde_json::Value>>(body)
        .ok()
        .and_then(|m| m.get("code").and_then(|c| c.as_u64()))
}

/// How long the loop waits between `get_lsm_stats` polls. A pass over a
/// `prefix_max` prefix takes seconds, so a shorter interval only adds
/// round trips.
pub(crate) const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Consecutive polls tolerated with no progress *and* no pass running
/// before the checkpoint gives up. This is the bound in place of a
/// deadline: a slow table waits (a running pass resets the counter) while
/// a stuck one — a fenced bucket, a saturated compactor pool, a failing
/// merge — fails loudly instead of spinning.
pub(crate) const MAX_IDLE_POLLS: usize = 10;

/// Cap on re-issues from `flush` after a 404. A node in a crash loop would
/// otherwise turn flush → compact → 404 → flush into a spin.
pub(crate) const MAX_REISSUES: usize = 3;

/// Base backoff between retries, doubled per consecutive retry up to
/// [`RETRY_BACKOFF_MAX`]. Latch contention clears in about the time one
/// compaction pass takes, so starting small is right; a saturated pool
/// wants the ceiling.
pub(crate) const RETRY_BACKOFF_BASE: Duration = Duration::from_millis(100);
pub(crate) const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// Sleep before re-issuing a retryable request.
pub(crate) async fn backoff(attempt: usize) {
    let delay = RETRY_BACKOFF_BASE
        .saturating_mul(1u32 << attempt.min(8) as u32)
        .min(RETRY_BACKOFF_MAX);
    tokio::time::sleep(delay).await;
}

/// What the checkpoint loop should do next after a failed request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckpointControl {
    /// Re-issue the same call after a backoff.
    Retry,
    /// Restart from `flush`, which is what re-claims and replays.
    ReissueFromFlush,
}

/// Whether the drain loop finished or needs the table re-claimed first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckpointOutcome {
    Done,
    ReissueFromFlush,
}

/// Decide what a failed LSM request means for the loop. Returns `Err` with
/// the original error for genuinely terminal faults — a draining node, a
/// dropping table, a missing table, anything unrecognized — which the
/// caller should see rather than keep looping on.
///
/// Takes the error by value so a terminal one propagates *as itself*.
/// Classifying from a borrow meant rebuilding it, and anything that was
/// not an `LsmRoute` (a `TableNotFound`, say) got flattened to a generic
/// runtime message on the way out.
///
/// Draining is terminal by design: the drain gate is a one-way latch that
/// never releases until the process restarts, so retrying against that
/// node spins forever when the truthful answer was in the first response.
pub(crate) fn checkpoint_fault(
    e: crate::Error,
    attempt: usize,
) -> crate::Result<CheckpointControl> {
    #[cfg(feature = "remote")]
    let fault = match &e {
        crate::Error::LsmRoute { fault, .. } => Some(*fault),
        _ => None,
    };
    #[cfg(not(feature = "remote"))]
    let fault: Option<LsmFault> = None;

    match fault {
        Some(LsmFault::Retry) => Ok(CheckpointControl::Retry),
        Some(LsmFault::ReissueFromFlush) if attempt < MAX_REISSUES => {
            Ok(CheckpointControl::ReissueFromFlush)
        }
        _ => Err(e),
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
            classify_lsm_fault(404, r#"{"code":19}"#),
            LsmFault::ReissueFromFlush,
            "a registry miss says InvalidTableState and must re-claim"
        );
        assert_eq!(
            classify_lsm_fault(404, r#"{"code":4}"#),
            LsmFault::Fatal,
            "a table that does not exist must not be retried as a lost claim"
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

    fn bucket(shard: &str, generations: &[u64], compacting: bool) -> BucketStats {
        BucketStats {
            shard_id: shard.into(),
            status: "Active".into(),
            writer_epoch: 1,
            manifest_version: 1,
            current_generation: generations.iter().max().copied().unwrap_or(0) + 1,
            replay_after_wal_entry_position: 0,
            wal_entry_position_last_seen: 0,
            generations: generations
                .iter()
                .map(|g| GenerationStats {
                    generation: *g,
                    bytes: 1,
                    rows: None,
                })
                .collect(),
            compacting,
            memtables: None,
        }
    }

    /// The target watermark is the newest generation at the start, and a
    /// generation created *after* it must not hold the loop open — that is
    /// the whole reason the predicate terminates under write load.
    #[test]
    fn newer_generations_do_not_extend_the_target() {
        let start = bucket("b0", &[7, 8], false);
        let target = start.newest_generation().expect("L0 is non-empty");
        assert_eq!(target, 8);

        // Compaction drained 7 and 8; 9 and 10 arrived while it ran.
        let later = bucket("b0", &[9, 10], false);
        assert_eq!(
            later.outstanding_generations(target),
            0,
            "generations above the target are somebody else's problem"
        );

        // Still holding 8 means still outstanding.
        assert_eq!(
            bucket("b0", &[8, 9], false).outstanding_generations(target),
            1
        );
    }

    /// The progress metric counts generations, not buckets. A pass drains
    /// a bounded prefix, so one bucket going 3 → 2 → 1 → 0 must read as
    /// three steps of progress, not three idle polls.
    #[test]
    fn progress_is_measured_in_generations() {
        let target = 3;
        let counts: Vec<usize> = [&[1u64, 2, 3][..], &[2, 3][..], &[3][..], &[][..]]
            .iter()
            .map(|gens| bucket("b0", gens, false).outstanding_generations(target))
            .collect();
        assert_eq!(counts, vec![3, 2, 1, 0]);
    }

    #[test]
    fn empty_l0_has_no_target() {
        assert!(bucket("b0", &[], false).newest_generation().is_none());
    }
}
