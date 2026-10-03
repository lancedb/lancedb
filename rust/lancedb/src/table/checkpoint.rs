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

/// The HTTP status a failed request carried, if it carried one.
///
/// `None` for anything with no retry story: a `TableNotFound` that
/// `check_table_response` already translated, or a connection failure that
/// never reached the server. Both are terminal.
fn status_of(e: &Error) -> Option<u16> {
    #[cfg(feature = "remote")]
    {
        match e {
            Error::Http {
                status_code: Some(status),
                ..
            } => Some(status.as_u16()),
            _ => None,
        }
    }
    #[cfg(not(feature = "remote"))]
    {
        let _ = e;
        None
    }
}

/// 429 (latch held, pool saturated, or the pod replaying its WAL) and 503 (a
/// draining node, or a proxy between here and it).
///
/// The status is the whole signal: the server deliberately keeps contention
/// off 503, so a latch collision is a 429. A draining node *is* terminal, but
/// it is also a 503 that stays a 503, so retrying spends one budget and then
/// reports the server's own message — cheaper than parsing the body for the
/// namespace code it would take to tell the two apart.
fn is_retryable(e: &Error) -> bool {
    matches!(status_of(e), Some(429 | 503))
}

/// 421: the owning node holds no claim. Only `flush` re-claims and replays,
/// so this cannot be retried in place — the caller has to start over.
fn is_lost_claim(e: &Error) -> bool {
    status_of(e) == Some(421)
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

/// Issue one LSM request, retrying in place while the fault is retryable.
///
/// The two recoverable faults have separate budgets: contention clears on its
/// own and retries here against [`MAX_RETRIES`], while a 421 needs `flush` to
/// re-claim, which only the caller can drive.
///
/// An exhausted budget propagates the last error *as itself* rather than a
/// synthesized one — "429 after nine tries" beats "checkpoint failed", and a
/// draining node arrives carrying the server's own message.
async fn issue<T, F, Fut>(mut call: F) -> Result<Attempt<T>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut retries = 0;
    loop {
        let e = match call().await {
            Ok(value) => return Ok(Attempt::Ok(value)),
            Err(e) => e,
        };
        if is_lost_claim(&e) {
            return Ok(Attempt::ReissueFromFlush);
        }
        if !is_retryable(&e) || retries >= MAX_RETRIES {
            return Err(e);
        }
        backoff(retries).await;
        retries += 1;
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

        if !all_compacting {
            match table.compact_lsm().await {
                Ok(()) => {}
                Err(e) if is_lost_claim(&e) => return Ok(CheckpointOutcome::ReissueFromFlush),
                Err(e) if !is_retryable(&e) => return Err(e),
                // A 429 here means the server could latch no bucket at all,
                // which the poll above already handles. Not retried in place:
                // the latch it would contend for is the one doing the work, so
                // fall through and re-read — `POLL_INTERVAL` is the backoff.
                Err(_) => {}
            }
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[cfg(all(test, feature = "remote"))]
mod tests {
    use super::*;

    fn http(status: u16) -> Error {
        Error::Http {
            source: "server said no".into(),
            request_id: "rid".into(),
            status_code: reqwest::StatusCode::from_u16(status).ok(),
        }
    }

    /// Every status the loop acts on. The two predicates are checked together
    /// because their overlap is what would be wrong: a status must never be
    /// both, and 421 in particular must not read as retryable — retrying it in
    /// place re-issues the call that just said the node holds no claim.
    #[test]
    fn taxonomy_round_trips() {
        for status in [429, 503] {
            assert!(is_retryable(&http(status)), "{status} must retry");
            assert!(
                !is_lost_claim(&http(status)),
                "{status} is not a lost claim"
            );
        }
        assert!(is_lost_claim(&http(421)), "a lost claim must re-claim");
        assert!(
            !is_retryable(&http(421)),
            "retrying a lost claim in place only asks the same node again"
        );
        for status in [400, 404, 409, 500] {
            assert!(!is_retryable(&http(status)), "{status} is terminal");
            assert!(!is_lost_claim(&http(status)), "{status} is terminal");
        }
    }

    /// An error carrying no status has no retry story and must be terminal —
    /// a connection that never reached the server, or a `TableNotFound` that
    /// `check_table_response` translated before the loop saw it.
    #[test]
    fn errors_without_a_status_are_terminal() {
        let no_status = Error::Http {
            source: "connection reset".into(),
            request_id: "rid".into(),
            status_code: None,
        };
        assert!(!is_retryable(&no_status));
        assert!(!is_lost_claim(&no_status));

        let translated = Error::TableNotFound {
            name: "t".into(),
            source: "gone".into(),
        };
        assert!(!is_retryable(&translated));
        assert!(!is_lost_claim(&translated));
    }
}
