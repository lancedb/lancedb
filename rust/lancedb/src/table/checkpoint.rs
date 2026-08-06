// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Converging a table's LSM write path into its base table.
//!
//! `checkpoint_lsm` seals once, then triggers compaction and watches
//! generation numbers until the L0 that existed at the start is gone.
//!
//! The loop runs in the client, not the server: `compact_lsm` dispatches a
//! pass and returns, so nothing holds a socket and a client can vanish
//! mid-operation with nothing to reconcile. Completion is read from
//! generation numbers in the shard manifest — durable state, unlike a count
//! in a compact response, which a concurrent write invalidates.
//!
//! The target set is fixed at the start, so generations created *during* the
//! checkpoint are ignored. That is what lets it terminate under write load,
//! and what makes it best-effort: it converges the fresh tier as of some
//! instant. Idempotent, abandonable at any point, safe on a cadence.
//!
//! No liveness bound — the caller owns the deadline. The compactor pool is
//! shared pod-wide, so a checkpoint queued behind unrelated tables looks
//! exactly like one that is merging.

use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use crate::{Error, Result, Table};

/// How a failed LSM request should be handled by the checkpoint loop.
///
/// Keyed on status, which the server assigns one meaning apiece. The body's
/// `code` is read on 503 alone, to tell a server 503 from a proxy's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LsmFault {
    /// 429 (latch held, pool saturated, or the pod replaying its WAL), or a
    /// 503 that did not come from the server. Fenced and slot-unavailable are
    /// not distinguished from contention: the client action is identical.
    Retry,
    /// 421: the owning node holds no claim. `flush` re-claims and replays, so
    /// re-issue from there — capped, or a crash-looping node spins.
    ReissueFromFlush,
    /// 503 with `InvalidTableState`: the owning node is draining. Terminal.
    Draining,
    /// 404 (no such table), 409 (dropping), 400 (not WAL-backed), or anything
    /// unrecognized. Terminal.
    Fatal,
}

/// Lance-namespace `ErrorCode::InvalidTableState`, which the server attaches
/// to a draining 503. Tells a server 503 from an ingress or proxy one, which
/// carries no code at all and is retryable.
pub(crate) const CODE_INVALID_TABLE_STATE: u64 = 19;

/// Classify an LSM-route failure from its status and response body.
pub(crate) fn classify_lsm_fault(status: u16, body: &str) -> LsmFault {
    match status {
        429 => LsmFault::Retry,
        // `flush` is what re-claims and replays.
        421 => LsmFault::ReissueFromFlush,
        // The body check asks whether this 503 came from phalanx at all: a
        // proxy 503 carries no code, and reading it as a draining node would
        // abort a checkpoint on a transient hop failure.
        503 if body_code(body) == Some(CODE_INVALID_TABLE_STATE) => LsmFault::Draining,
        503 => LsmFault::Retry,
        // `send_lsm_route` turns 404 into `TableNotFound` before this runs.
        _ => LsmFault::Fatal,
    }
}

/// The lance-namespace error code from a JSON error body. Absent or
/// unparseable ⇒ `None`, which callers read as the *less* terminal branch.
fn body_code(body: &str) -> Option<u64> {
    serde_json::from_str::<HashMap<String, serde_json::Value>>(body)
        .ok()
        .and_then(|m| m.get("code").and_then(|c| c.as_u64()))
}

/// Interval between `get_lsm_stats` polls. One interval is roughly one
/// compaction pass, the granularity at which the answer can change.
///
/// Fixed rather than configurable, matching `wait_for_index`. It costs
/// nothing on an already-converged table and at most one interval of tail
/// latency after the final pass lands.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Cap on re-issues from `flush` after a 421, so a crash-looping node cannot
/// turn flush → compact → 421 → flush into a spin.
///
/// Deliberately not shared with [`MAX_RETRIES`]: a claim that keeps
/// evaporating is a broken node, while contention is routine and wants a real
/// budget. One shared counter let a merely contended table exhaust this cap
/// and then blame a claim it never lost.
const MAX_REISSUES: usize = 3;

/// Retryable faults tolerated on a *single* request, reset on every success —
/// scattered contention across a long checkpoint must not accumulate toward a
/// cap. Roughly 16s of retrying against the backoff below.
const MAX_RETRIES: usize = 8;

/// Backoff between retries, doubling up to [`RETRY_BACKOFF_MAX`]. Latch
/// contention clears in about the time one pass takes, so start small; a
/// saturated pool wants the ceiling.
const RETRY_BACKOFF_BASE: Duration = Duration::from_millis(100);
const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// Sleep before re-issuing a retryable request.
async fn backoff(attempt: usize) {
    let delay = RETRY_BACKOFF_BASE
        .saturating_mul(1u32 << attempt.min(8) as u32)
        .min(RETRY_BACKOFF_MAX);
    tokio::time::sleep(delay).await;
}

/// Whether the drain loop finished or needs the table re-claimed first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckpointOutcome {
    Done,
    ReissueFromFlush,
}

/// What one LSM request produced: its value, or word that the owning node
/// holds no claim and only `flush` can get it back.
enum Attempt<T> {
    Ok(T),
    ReissueFromFlush,
}

/// The fault an LSM route reported, or `None` for anything else — a
/// `TableNotFound`, a transport failure — which is terminal by definition.
fn lsm_fault(e: &Error) -> Option<LsmFault> {
    #[cfg(feature = "remote")]
    {
        match e {
            Error::LsmRoute { fault, .. } => Some(*fault),
            _ => None,
        }
    }
    #[cfg(not(feature = "remote"))]
    {
        let _ = e;
        None
    }
}

/// Issue one LSM request, retrying in place while the fault is retryable.
///
/// The two recoverable faults have separate budgets: contention clears on its
/// own and retries here against [`MAX_RETRIES`], while a 421 needs `flush` to
/// re-claim, which only the caller can drive.
///
/// An exhausted budget propagates the last error *as itself* rather than a
/// synthesized one — "429 after nine tries" beats "checkpoint failed".
/// Draining is terminal: the drain gate never releases until the process
/// restarts, so retrying that node spins forever.
async fn issue<T, F, Fut>(mut call: F) -> Result<Attempt<T>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut retries = 0;
    loop {
        match call().await {
            Ok(value) => return Ok(Attempt::Ok(value)),
            Err(e) => match lsm_fault(&e) {
                Some(LsmFault::ReissueFromFlush) => return Ok(Attempt::ReissueFromFlush),
                Some(LsmFault::Retry) if retries < MAX_RETRIES => {
                    backoff(retries).await;
                    retries += 1;
                }
                _ => return Err(e),
            },
        }
    }
}

/// Drive [`Table::checkpoint_lsm`]: seal once, fix the target watermark
/// from the resulting L0, then trigger and poll until it drains.
pub(crate) async fn checkpoint_lsm(table: &Table) -> Result<()> {
    for reissue in 0..=MAX_REISSUES {
        // The seal turns everything written before this call into a
        // generation, so the watermark has to be read after it. Idempotent:
        // sealing an empty memtable is a no-op, so a re-issue does not churn
        // empty generations.
        match issue(|| table.flush_lsm()).await? {
            Attempt::Ok(()) => {}
            Attempt::ReissueFromFlush => {
                backoff(reissue).await;
                continue;
            }
        }

        let stats = match issue(|| table.get_lsm_stats(false)).await? {
            Attempt::Ok(stats) => stats,
            Attempt::ReissueFromFlush => {
                backoff(reissue).await;
                continue;
            }
        };
        let Some(stats) = stats else {
            // Not WAL-backed; `flush_lsm` would have errored first but for a race.
            return Ok(());
        };
        let targets: HashMap<String, u64> = stats
            .buckets
            .iter()
            .filter_map(|b| Some((b.shard_id.clone(), b.newest_generation()?)))
            .collect();
        if targets.is_empty() {
            return Ok(());
        }

        match drain_to_targets(table, &targets).await? {
            CheckpointOutcome::Done => return Ok(()),
            CheckpointOutcome::ReissueFromFlush => {
                backoff(reissue).await;
                continue;
            }
        }
    }
    Err(Error::Runtime {
        message: "checkpoint_lsm: the owning node kept losing its claim; \
                  re-issued from flush the maximum number of times"
            .into(),
    })
}

/// Trigger and poll until no bucket holds a generation at or below its
/// target.
///
/// No liveness bound, deliberately. The pod-wide compactor pool (a semaphore
/// of 2 by default, shared across every table on the node) is taken *inside*
/// the pass, after the bucket latch, so a checkpoint queued behind unrelated
/// tables is indistinguishable from one that is merging. An idle-poll counter
/// here could only ever have fired on a table that would have finished.
async fn drain_to_targets(
    table: &Table,
    targets: &HashMap<String, u64>,
) -> Result<CheckpointOutcome> {
    loop {
        let stats = match issue(|| table.get_lsm_stats(false)).await? {
            Attempt::Ok(stats) => stats,
            Attempt::ReissueFromFlush => return Ok(CheckpointOutcome::ReissueFromFlush),
        };
        let Some(stats) = stats else {
            return Ok(CheckpointOutcome::Done);
        };
        // `compacting` is the bucket's compaction latch, held from dispatch
        // until the pass ends — including while it waits on the pod-wide
        // permit. So it answers one question only: do not pile on. Buckets
        // with nothing outstanding are skipped, not counted as idle.
        let mut outstanding = 0;
        let mut all_compacting = true;
        for b in &stats.buckets {
            let Some(target) = targets.get(&b.shard_id) else {
                continue;
            };
            let n = b.outstanding_generations(*target);
            if n > 0 {
                outstanding += n;
                all_compacting &= b.compacting;
            }
        }
        if outstanding == 0 {
            return Ok(CheckpointOutcome::Done);
        }

        // Not retried in place: the server answers 429 only when it could
        // latch no bucket at all, which the poll above already handles. Fall
        // through and re-read; `POLL_INTERVAL` is the backoff.
        if !all_compacting {
            match table.compact_lsm().await {
                Ok(()) => {}
                Err(e) => match lsm_fault(&e) {
                    Some(LsmFault::Retry) => {}
                    Some(LsmFault::ReissueFromFlush) => {
                        return Ok(CheckpointOutcome::ReissueFromFlush);
                    }
                    _ => return Err(e),
                },
            }
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every row of the taxonomy, on the `(status, code)` pair the client
    /// receives. A `_ =>` arm added later passes any test that only asserts
    /// "it errored".
    #[test]
    fn taxonomy_round_trips() {
        assert_eq!(
            classify_lsm_fault(429, r#"{"code":21}"#),
            LsmFault::Retry,
            "contention, saturation, and a pod replaying its WAL all arrive here"
        );
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
            classify_lsm_fault(421, r#"{"code":19}"#),
            LsmFault::ReissueFromFlush,
            "a lost claim has its own status and must re-claim"
        );
        assert_eq!(
            classify_lsm_fault(404, r#"{"code":4}"#),
            LsmFault::Fatal,
            "a table that does not exist must not be retried as a lost claim"
        );
        assert_eq!(classify_lsm_fault(409, r#"{"code":14}"#), LsmFault::Fatal);
        assert_eq!(classify_lsm_fault(400, r#"{"code":13}"#), LsmFault::Fatal);
    }

    /// A 503 whose body is missing, truncated, or not the expected JSON falls
    /// back to retryable, never terminal: an ingress 503 carries no code, and
    /// mistaking it for a draining node aborts the checkpoint on a lie.
    #[test]
    fn unparseable_503_body_is_retryable() {
        assert_eq!(classify_lsm_fault(503, ""), LsmFault::Retry);
        assert_eq!(classify_lsm_fault(503, "gateway timeout"), LsmFault::Retry);
        assert_eq!(classify_lsm_fault(503, r#"{"code":"19"}"#), LsmFault::Retry);
    }
}
