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

use crate::{Error, Result, Table};

/// How a failed LSM request should be handled by the checkpoint loop.
///
/// Reads the status, which the server assigns one meaning apiece and phalanx
/// relays. The body's `code` is consulted on 503 alone, and only to tell a
/// server 503 from a proxy's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LsmFault {
    /// 429 (latch held, pool saturated, or the pod replaying its WAL), or a
    /// 503 that did not come from the server. Retry with backoff.
    ///
    /// Fenced and slot-unavailable are deliberately not distinguished from
    /// contention here: the client action is identical.
    Retry,
    /// 421: the owning node holds no claim on the table — its registry entry
    /// vanished under a restart mid-loop. `flush` is the call that re-claims
    /// and replays, so re-issue from there — with a cap, or a crash-looping
    /// node turns the loop into a spin.
    ReissueFromFlush,
    /// 503 with `InvalidTableState`: the owning node is draining. Terminal.
    Draining,
    /// 404 (no such table), 409 (dropping), 400 (not WAL-backed), or anything
    /// unrecognized. Terminal.
    Fatal,
}

/// Lance-namespace `ErrorCode::InvalidTableState`, which the server attaches
/// to a draining 503. Its job here is to distinguish a 503 the server sent
/// from one an ingress or proxy sent on its behalf — the latter carries no
/// code at all, and is retryable.
pub(crate) const CODE_INVALID_TABLE_STATE: u64 = 19;

/// Classify an LSM-route failure from its status and response body.
///
/// The status carries the condition: the server assigns exactly one meaning
/// per status and phalanx relays it. The body is read for one thing only —
/// see the 503 arm.
pub(crate) fn classify_lsm_fault(status: u16, body: &str) -> LsmFault {
    match status {
        // Latch held, compactor pool saturated, or the pod replaying its WAL.
        429 => LsmFault::Retry,
        // The owning node holds no claim; `flush` re-claims and replays.
        421 => LsmFault::ReissueFromFlush,
        // The body check is *not* disambiguating phalanx's own 503s — it asks
        // whether this 503 came from phalanx at all. An ingress or proxy 503
        // between here and the server carries no code, and treating that as a
        // draining node would abort a checkpoint on a transient hop failure.
        503 if body_code(body) == Some(CODE_INVALID_TABLE_STATE) => LsmFault::Draining,
        503 => LsmFault::Retry,
        // 404 (no such table), 409 (dropping), 400 (malformed). `send_lsm_route`
        // turns 404 into `TableNotFound` before this is reached.
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
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Consecutive polls tolerated with no progress *and* no pass running
/// before the checkpoint gives up. This is the bound in place of a
/// deadline: a slow table waits (a running pass resets the counter) while
/// a stuck one — a fenced bucket, a saturated compactor pool, a failing
/// merge — fails loudly instead of spinning.
const MAX_IDLE_POLLS: usize = 10;

/// Cap on re-issues from `flush` after a 421. A node in a crash loop would
/// otherwise turn flush → compact → 421 → flush into a spin.
const MAX_REISSUES: usize = 3;

/// Base backoff between retries, doubled per consecutive retry up to
/// [`RETRY_BACKOFF_MAX`]. Latch contention clears in about the time one
/// compaction pass takes, so starting small is right; a saturated pool
/// wants the ceiling.
const RETRY_BACKOFF_BASE: Duration = Duration::from_millis(100);
const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// Sleep before re-issuing a retryable request.
async fn backoff(attempt: usize) {
    let delay = RETRY_BACKOFF_BASE
        .saturating_mul(1u32 << attempt.min(8) as u32)
        .min(RETRY_BACKOFF_MAX);
    tokio::time::sleep(delay).await;
}

/// What the checkpoint loop should do next after a failed request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckpointControl {
    /// Re-issue the same call after a backoff.
    Retry,
    /// Restart from `flush`, which is what re-claims and replays.
    ReissueFromFlush,
}

/// Whether the drain loop finished or needs the table re-claimed first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckpointOutcome {
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
fn checkpoint_fault(e: Error, attempt: usize) -> Result<CheckpointControl> {
    #[cfg(feature = "remote")]
    let fault = match &e {
        Error::LsmRoute { fault, .. } => Some(*fault),
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

/// Drive [`Table::checkpoint_lsm`]: seal once, fix the target watermark
/// from the resulting L0, then trigger and poll until it drains.
pub(crate) async fn checkpoint_lsm(table: &Table) -> Result<()> {
    for attempt in 0..=MAX_REISSUES {
        // The seal is what turns everything written before this call into a
        // generation, so the watermark has to be read after it. It is also
        // idempotent: sealing an empty active memtable is a no-op, so a
        // re-issue does not churn empty generations.
        match table.flush_lsm().await {
            Ok(()) => {}
            Err(e) => match checkpoint_fault(e, attempt)? {
                CheckpointControl::Retry | CheckpointControl::ReissueFromFlush => {
                    backoff(attempt).await;
                    continue;
                }
            },
        }

        let Some(stats) = table.get_lsm_stats(false).await? else {
            // Not WAL-backed: nothing to converge, and `flush_lsm` would
            // have errored first on every path but a race.
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

        match drain_to_targets(table, &targets, attempt).await? {
            CheckpointOutcome::Done => return Ok(()),
            CheckpointOutcome::ReissueFromFlush => continue,
        }
    }
    Err(Error::Runtime {
        message: "checkpoint_lsm: the owning node kept losing its claim; \
                  re-issued from flush the maximum number of times"
            .into(),
    })
}

/// Trigger and poll until no bucket holds a generation at or below its
/// target. Split out so the flush re-issue path above stays readable.
async fn drain_to_targets(
    table: &Table,
    targets: &HashMap<String, u64>,
    attempt: usize,
) -> Result<CheckpointOutcome> {
    let mut idle_polls = 0;
    let mut last_outstanding = usize::MAX;
    loop {
        let Some(stats) = table.get_lsm_stats(false).await? else {
            return Ok(CheckpointOutcome::Done);
        };
        let mut outstanding = 0;
        let mut buckets_left = 0;
        let mut all_compacting = true;
        for b in &stats.buckets {
            let Some(target) = targets.get(&b.shard_id) else {
                continue;
            };
            let n = b.outstanding_generations(*target);
            if n > 0 {
                outstanding += n;
                buckets_left += 1;
                all_compacting &= b.compacting;
            }
        }
        if outstanding == 0 {
            return Ok(CheckpointOutcome::Done);
        }

        // Progress, not time, is the bound. A pass already running on
        // every outstanding bucket counts as progress: piling on would
        // only collect 429s, and the latch it would contend for is the
        // one doing the work.
        if outstanding < last_outstanding || all_compacting {
            idle_polls = 0;
        } else {
            idle_polls += 1;
            if idle_polls >= MAX_IDLE_POLLS {
                return Err(Error::Runtime {
                    message: format!(
                        "checkpoint_lsm: {outstanding} generation(s) across {buckets_left} \
                         bucket(s) stopped making progress with no compaction running; the \
                         compactor pool may be saturated or the writer fenced"
                    ),
                });
            }
        }
        last_outstanding = outstanding;

        if !all_compacting {
            match table.compact_lsm().await {
                Ok(()) => {}
                Err(e) => match checkpoint_fault(e, attempt)? {
                    // Every bucket busy (429) is the state the poll above
                    // already handles; fall through and re-read.
                    CheckpointControl::Retry => {}
                    CheckpointControl::ReissueFromFlush => {
                        return Ok(CheckpointOutcome::ReissueFromFlush);
                    }
                },
            }
        }
        tokio::time::sleep(POLL_INTERVAL).await;
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

    /// A 503 whose body is missing, truncated, or not the expected JSON must
    /// fall back to *retryable*, never to terminal. This is the proxy case:
    /// an ingress 503 carries no code, and mistaking it for a draining node
    /// aborts the checkpoint on a lie.
    #[test]
    fn unparseable_503_body_is_retryable() {
        assert_eq!(classify_lsm_fault(503, ""), LsmFault::Retry);
        assert_eq!(classify_lsm_fault(503, "gateway timeout"), LsmFault::Retry);
        assert_eq!(classify_lsm_fault(503, r#"{"code":"19"}"#), LsmFault::Retry);
    }
}
