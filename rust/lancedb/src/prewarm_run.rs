// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Prewarm runs: warm a training job's data a wave at a time, ahead of reads.
//!
//! A training job opens one run per table. Each rank joins it, rank 0
//! announces the rows of upcoming waves, and every rank waits for a wave to
//! be warm before reading it. While a rank is joined, a background task sends
//! heartbeats that keep it live and tell LanceDB Enterprise which wave it is
//! reading, so the server warms the waves ahead and lets consumed ones go.
//!
//! Runs are only supported by LanceDB Enterprise.

use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::error::{Error, Result};

/// How long a call keeps retrying while the server cannot reach the run's
/// coordinator. It must outlast the coordinator being replaced after a
/// failure; the value is provisional until that has been measured.
pub const DEFAULT_RUN_CALL_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// The longest a single `wave_status` call waits on the server. Kept below
/// common proxy idle timeouts; [`PrewarmRunRank::wait_ready`] loops it.
const MAX_WAVE_STATUS_WAIT: Duration = Duration::from_secs(20);

const INITIAL_RETRY_BACKOFF: Duration = Duration::from_millis(100);
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(2);

/// Error codes the server returns for run calls.
pub mod error_codes {
    pub const UNKNOWN_RUN: &str = "unknown_run";
    pub const RANK_OUT_OF_RANGE: &str = "rank_out_of_range";
    pub const NOT_ANNOUNCER: &str = "not_announcer";
    pub const NOT_JOINED: &str = "not_joined";
    pub const RUN_KEY_CONFLICT: &str = "run_key_conflict";
    pub const WAVE_NOT_ANNOUNCED: &str = "wave_not_announced";
    pub const WAVE_OUT_OF_ORDER: &str = "wave_out_of_order";
    pub const ANNOUNCEMENT_CONFLICT: &str = "announcement_conflict";
    pub const BEYOND_HORIZON: &str = "beyond_horizon";
    pub const RUN_RELEASED: &str = "run_released";
    pub const RUN_FAILED: &str = "run_failed";
    pub const NOT_READY: &str = "not_ready";
    /// A cache-only query needed data the cache does not hold.
    pub const NOT_RESIDENT: &str = "not_resident";
    /// Client-side: the rank asked about a wave every live rank has passed,
    /// so it was dropped from the run or fell behind.
    pub const WAVE_CONSUMED: &str = "wave_consumed";
}

/// What a run warms and how many ranks read it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrewarmRunOptions {
    /// Chosen by the caller and identical on every rank of one training
    /// attempt. Every rank that opens a run with the same key gets the same
    /// run; a key whose run has ended never opens a second one.
    pub run_key: String,
    /// The table version the run reads. The run holds it until released.
    pub version: u64,
    /// Columns to warm. Empty means all columns.
    pub columns: Vec<String>,
    /// Number of ranks. Fixed for the life of the run.
    pub world_size: u32,
}

/// Rows `[start, end)` of one fragment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RowRange {
    pub fragment_id: u64,
    pub start: u64,
    pub end: u64,
}

/// The rows one wave reads, across every split.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaveAnnouncement {
    pub wave: u64,
    pub ranges: Vec<RowRange>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Open,
    Released,
    Failed,
    /// A status this client version does not know.
    #[serde(other)]
    Unknown,
}

/// The run as the server last reported it to this rank.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunView {
    pub status: RunStatus,
    /// The lowest wave any live rank is reading.
    pub reading_wave: u64,
    pub highest_announced: Option<u64>,
    pub live_ranks: u32,
    /// How many waves past `reading_wave` rank 0 may announce.
    pub horizon: u32,
    /// Permutation positions of each split that one wave holds.
    pub wave_positions: u64,
    pub heartbeat_interval_ms: u64,
}

impl RunView {
    /// The highest wave rank 0 may announce now.
    pub fn announce_limit(&self) -> u64 {
        self.reading_wave + u64::from(self.horizon)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaveStatus {
    NotAnnounced,
    Pending,
    Warming,
    Ready,
    Consumed,
}

/// A failed run call, as the transport saw it.
#[derive(Debug)]
pub(crate) enum CallError {
    /// The server answered with a run error code.
    Run {
        status: u16,
        code: String,
        message: String,
    },
    /// Retryable: the request did not reach a server that could answer it.
    Transient(Error),
    /// Anything else, returned to the caller as is.
    Other(Error),
}

/// Sends run calls for one table. Implemented by the remote table.
#[async_trait]
pub(crate) trait PrewarmRunTransport: Send + Sync + Debug {
    async fn call(
        &self,
        run_id: &str,
        call: &str,
        body: Value,
    ) -> std::result::Result<Value, CallError>;
}

/// An open prewarm run. Join it with each rank's [`PrewarmRun::join`].
#[derive(Clone, Debug)]
pub struct PrewarmRun {
    transport: Arc<dyn PrewarmRunTransport>,
    run_id: Arc<str>,
    version: u64,
}

impl PrewarmRun {
    pub(crate) fn new(
        transport: Arc<dyn PrewarmRunTransport>,
        run_id: String,
        version: u64,
    ) -> Self {
        Self {
            transport,
            run_id: run_id.into(),
            version,
        }
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// The table version the run reads.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Join the run as `rank` and start sending heartbeats. Joining again
    /// with the same rank is harmless.
    pub async fn join(&self, rank: u32) -> Result<PrewarmRunRank> {
        let deadline = Instant::now() + DEFAULT_RUN_CALL_TIMEOUT;
        let view: RunView = call(self, "join", json!({ "rank": rank }), deadline).await?;
        let shared = Arc::new(RankShared {
            run: self.clone(),
            rank,
            current_wave: AtomicU64::new(view.reading_wave),
            view: Mutex::new(view),
        });
        let heartbeat = tokio::spawn(heartbeat_loop(shared.clone()));
        Ok(PrewarmRunRank {
            shared,
            heartbeat: Mutex::new(Some(heartbeat)),
        })
    }
}

#[derive(Debug)]
struct RankShared {
    run: PrewarmRun,
    rank: u32,
    /// The wave this rank is reading, as heartbeats report it.
    current_wave: AtomicU64,
    view: Mutex<RunView>,
}

impl RankShared {
    fn record(&self, view: RunView) {
        *self.view.lock().unwrap_or_else(|e| e.into_inner()) = view;
    }

    fn view(&self) -> RunView {
        self.view.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Run a rank call, joining again once if the server dropped this rank.
    async fn call<T: DeserializeOwned>(
        &self,
        name: &str,
        body: Value,
        deadline: Instant,
    ) -> Result<T> {
        match call(&self.run, name, body.clone(), deadline).await {
            Err(Error::PrewarmRun { code, .. }) if code == error_codes::NOT_JOINED => {
                let view: RunView =
                    call(&self.run, "join", json!({ "rank": self.rank }), deadline).await?;
                self.record(view);
                call(&self.run, name, body, deadline).await
            }
            result => result,
        }
    }
}

/// One rank's membership in a prewarm run. Dropping it stops heartbeats
/// without closing; call [`PrewarmRunRank::close`] when the rank is done.
#[derive(Debug)]
pub struct PrewarmRunRank {
    shared: Arc<RankShared>,
    heartbeat: Mutex<Option<JoinHandle<()>>>,
}

impl PrewarmRunRank {
    pub fn run(&self) -> &PrewarmRun {
        &self.shared.run
    }

    pub fn rank(&self) -> u32 {
        self.shared.rank
    }

    /// The run as the server last reported it.
    pub fn view(&self) -> RunView {
        self.shared.view()
    }

    /// Announce upcoming waves. Only rank 0 may announce; waves must follow
    /// the last announced one and stay within [`RunView::announce_limit`].
    /// Repeating an announced wave with the same rows is harmless. Returns
    /// the highest wave announced so far.
    pub async fn announce(&self, waves: Vec<WaveAnnouncement>) -> Result<Option<u64>> {
        #[derive(Deserialize)]
        struct Reply {
            highest_announced: Option<u64>,
        }
        let deadline = Instant::now() + DEFAULT_RUN_CALL_TIMEOUT;
        let body = json!({ "rank": self.shared.rank, "waves": waves });
        let reply: Reply = self.shared.call("announce", body, deadline).await?;
        Ok(reply.highest_announced)
    }

    /// Wait until `wave` is warm, or `timeout` passes. Marks `wave` as the
    /// one this rank is reading, so the server lets earlier waves go.
    pub async fn wait_ready(&self, wave: u64, timeout: Duration) -> Result<()> {
        #[derive(Deserialize)]
        struct Reply {
            status: WaveStatus,
        }
        let deadline = Instant::now() + timeout;
        self.shared.current_wave.fetch_max(wave, Ordering::Relaxed);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Timeout {
                    message: format!(
                        "prewarm run {}: wave {wave} was not warm within {timeout:?}",
                        self.shared.run.run_id
                    ),
                });
            }
            let wait_ms = remaining.min(MAX_WAVE_STATUS_WAIT).as_millis() as u64;
            let body = json!({ "rank": self.shared.rank, "wave": wave, "wait_ms": wait_ms });
            let reply: Reply = self.shared.call("wave_status", body, deadline).await?;
            match reply.status {
                WaveStatus::Ready => return Ok(()),
                WaveStatus::Consumed => {
                    return Err(Error::PrewarmRun {
                        run_id: self.shared.run.run_id.to_string(),
                        code: error_codes::WAVE_CONSUMED.to_string(),
                        message: format!(
                            "wave {wave} is behind every live rank; rank {} was dropped \
                             from the run or fell behind",
                            self.shared.rank
                        ),
                    });
                }
                WaveStatus::NotAnnounced | WaveStatus::Pending | WaveStatus::Warming => {}
            }
        }
    }

    /// Ask the server to warm `ranges` of `wave` again after a read found
    /// them missing. Follow with [`PrewarmRunRank::wait_ready`].
    pub async fn rewarm(&self, wave: u64, ranges: Vec<RowRange>) -> Result<()> {
        let deadline = Instant::now() + DEFAULT_RUN_CALL_TIMEOUT;
        let body = json!({ "rank": self.shared.rank, "wave": wave, "ranges": ranges });
        let _: Value = self.shared.call("rewarm", body, deadline).await?;
        Ok(())
    }

    /// Stop heartbeats and leave the run. A run that has already ended
    /// counts as closed.
    pub async fn close(&self) -> Result<()> {
        self.stop_heartbeat();
        let deadline = Instant::now() + DEFAULT_RUN_CALL_TIMEOUT;
        let body = json!({ "rank": self.shared.rank });
        match call::<Value>(&self.shared.run, "close", body, deadline).await {
            Ok(_) => Ok(()),
            Err(Error::PrewarmRun { code, .. })
                if code == error_codes::RUN_RELEASED || code == error_codes::NOT_JOINED =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn stop_heartbeat(&self) {
        if let Some(handle) = self
            .heartbeat
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            handle.abort();
        }
    }
}

impl Drop for PrewarmRunRank {
    fn drop(&mut self) {
        self.stop_heartbeat();
    }
}

async fn heartbeat_loop(shared: Arc<RankShared>) {
    loop {
        let interval = Duration::from_millis(shared.view().heartbeat_interval_ms.max(1));
        tokio::time::sleep(interval).await;
        let body = json!({
            "rank": shared.rank,
            "current_wave": shared.current_wave.load(Ordering::Relaxed),
        });
        // A missed beat is retried on the next tick, so one interval bounds it.
        let deadline = Instant::now() + interval;
        match shared.call::<RunView>("heartbeat", body, deadline).await {
            Ok(view) => {
                let open = view.status == RunStatus::Open;
                shared.record(view);
                if !open {
                    return;
                }
            }
            Err(Error::PrewarmRun { code, .. })
                if code == error_codes::RUN_RELEASED
                    || code == error_codes::RUN_FAILED
                    || code == error_codes::UNKNOWN_RUN =>
            {
                return;
            }
            Err(error) => {
                log::warn!(
                    "prewarm run {} rank {}: heartbeat failed: {error}",
                    shared.run.run_id,
                    shared.rank
                );
            }
        }
    }
}

/// Send one run call, retrying transient failures with backoff until
/// `deadline`.
async fn call<T: DeserializeOwned>(
    run: &PrewarmRun,
    name: &str,
    body: Value,
    deadline: Instant,
) -> Result<T> {
    let mut backoff = INITIAL_RETRY_BACKOFF;
    loop {
        let error = match run.transport.call(&run.run_id, name, body.clone()).await {
            Ok(reply) => {
                return serde_json::from_value(reply).map_err(|error| Error::Runtime {
                    message: format!(
                        "prewarm run {}: unexpected `{name}` reply: {error}",
                        run.run_id
                    ),
                });
            }
            Err(CallError::Run { status, code, .. }) if code == error_codes::NOT_READY => {
                format!("{status} {code}")
            }
            Err(CallError::Run { code, message, .. }) => {
                return Err(Error::PrewarmRun {
                    run_id: run.run_id.to_string(),
                    code,
                    message,
                });
            }
            Err(CallError::Transient(error)) => error.to_string(),
            Err(CallError::Other(error)) => return Err(error),
        };
        let now = Instant::now();
        if now + backoff >= deadline {
            return Err(Error::Timeout {
                message: format!(
                    "prewarm run {}: `{name}` did not succeed before its deadline; \
                     last error: {error}",
                    run.run_id
                ),
            });
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_RETRY_BACKOFF);
    }
}
