// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

/// Per-table state driving the freshness headers (`x-lancedb-min-version`,
/// `x-lancedb-min-timestamp`, and `x-lancedb-min-read-version`) sent on table
/// requests.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct FreshnessState {
    /// Identifies the handle timeline that produced this state. Explicit
    /// checkout operations advance the generation so responses from older
    /// in-flight requests cannot repopulate the new timeline's constraints.
    pub(super) generation: u64,
    /// Exact-version, tag, and snapshot handles must not carry latest-timeline
    /// constraints. Their request body already selects the precise version.
    pub(super) pinned: bool,
    /// Provides read-your-write within a single handle: writes that return a
    /// version update this, and reads send it as `x-lancedb-min-version`.
    pub(super) min_version: Option<u64>,
    /// Highest committed dataset version advertised by a successful table
    /// response on this handle. Later requests send it as
    /// `x-lancedb-min-read-version` so a load-balanced query
    /// node whose cache is behind this version must refresh before serving,
    /// giving monotonic reads across nodes regardless of which one the load
    /// balancer routes to. Unlike write result bodies, this is sourced only
    /// from the server's committed dataset-version response header or other
    /// typed dataset-version fields, so WAL entry ids cannot enter it.
    pub(super) min_read_version: Option<u64>,
    /// Wall-clock time captured at the last [`BaseTable::checkout_latest`]
    /// call. Subsequent reads send
    /// `max(baseline, now - read_consistency_interval)` as
    /// `x-lancedb-min-timestamp`.
    ///
    /// Without this, `checkout_latest()` would have no effect on subsequent
    /// reads when `read_consistency_interval` is unset (the default): a
    /// server-side cache could still serve a snapshot older than the moment
    /// the user explicitly asked for "latest". The baseline forces the
    /// server to skip any cache entry older than the checkout time, so the
    /// `checkout_latest()` signal is preserved across reads on the same
    /// handle regardless of the configured consistency interval.
    pub(super) checkout_baseline: Option<SystemTime>,
}

/// Snapshot of the headers that should be attached to a single table request.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct FreshnessHeaders {
    generation: u64,
    min_version: Option<u64>,
    pub(super) min_timestamp: Option<SystemTime>,
    min_read_version: Option<u64>,
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct ReadSnapshot {
    pub(super) version: Option<u64>,
    pub(super) freshness_state: FreshnessState,
    pub(super) freshness: FreshnessHeaders,
}

impl FreshnessHeaders {
    pub(super) fn apply(self, mut request: RequestBuilder) -> RequestBuilder {
        if let Some(v) = self.min_version {
            request = request.header(MIN_VERSION_HEADER, v.to_string());
        }
        if let Some(ts) = self.min_timestamp {
            let dt: chrono::DateTime<chrono::Utc> = ts.into();
            request = request.header(MIN_TIMESTAMP_HEADER, dt.to_rfc3339());
        }
        if let Some(v) = self.min_read_version {
            request = request.header(MIN_READ_VERSION_HEADER, v.to_string());
        }
        request
    }

    pub(super) fn observe_version(self, freshness: &Mutex<FreshnessState>, version: u64) {
        track_read_version_for_generation(freshness, self.generation, version);
    }

    pub(super) fn observe_headers(
        self,
        freshness: &Mutex<FreshnessState>,
        headers: &reqwest::header::HeaderMap,
    ) {
        if let Some(version) = headers
            .get(&VERSION_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
        {
            self.observe_version(freshness, version);
        }
    }

    pub(super) fn update_if_current(
        self,
        freshness: &Mutex<FreshnessState>,
        update: impl FnOnce(&mut FreshnessState),
    ) {
        let mut state = freshness.lock().unwrap();
        if state.generation == self.generation {
            update(&mut state);
        }
    }

    pub(super) fn is_current(self, freshness: &Mutex<FreshnessState>) -> bool {
        freshness.lock().unwrap().generation == self.generation
    }
}

pub(super) fn track_read_version(freshness: &Mutex<FreshnessState>, version: u64) {
    if version == 0 {
        return;
    }
    let mut state = freshness.lock().unwrap();
    state.min_read_version = Some(state.min_read_version.map_or(version, |v| v.max(version)));
}

pub(super) fn track_read_version_for_generation(
    freshness: &Mutex<FreshnessState>,
    generation: u64,
    version: u64,
) {
    if version == 0 {
        return;
    }
    let mut state = freshness.lock().unwrap();
    if state.generation == generation {
        state.min_read_version = Some(state.min_read_version.map_or(version, |v| v.max(version)));
    }
}

/// A backfill job whose successful wait establishes a read-freshness
/// baseline on the submitting handle, so a later read cannot be served
/// from a cache older than the completed fill. A handle pinned by checkout
/// at completion keeps its time-travel view instead.
pub(super) struct FreshnessJob<S: HttpSend> {
    pub(super) inner: RemoteJob<S>,
    pub(super) freshness: Arc<Mutex<FreshnessState>>,
    pub(super) version: Arc<RwLock<Option<u64>>>,
    pub(super) tracked_result: TrackedJobResult,
    pub(super) freshness_request: FreshnessHeaders,
}

#[derive(Clone, Copy)]
pub(super) enum TrackedJobResult {
    None,
    RefreshColumn,
}

#[async_trait]
impl<S: HttpSend> crate::job::JobHandle for FreshnessJob<S> {
    fn id(&self) -> Option<&str> {
        crate::job::JobHandle::id(&self.inner)
    }

    async fn status(&self) -> Result<String> {
        crate::job::JobHandle::status(&self.inner).await
    }

    async fn describe(&self) -> Result<crate::database::JobDescription> {
        crate::job::JobHandle::describe(&self.inner).await
    }

    async fn events(
        &self,
        request: crate::job::JobEventsRequest,
    ) -> Result<Vec<arrow_array::RecordBatch>> {
        crate::job::JobHandle::events(&self.inner, request).await
    }

    async fn wait(&self) -> Result<crate::job::TerminalResult> {
        let result = crate::job::JobHandle::wait(&self.inner).await?;
        let version = self.version.read().await;
        if version.is_none() {
            let result_version = match self.tracked_result {
                TrackedJobResult::None => None,
                TrackedJobResult::RefreshColumn => result.value().and_then(|value| {
                    serde_json::from_value::<crate::function::RefreshColumnResult>(value.clone())
                        .ok()
                        .map(|result| {
                            result
                                .published_version
                                .map_or(result.source_version, |version| {
                                    version.max(result.source_version)
                                })
                        })
                }),
            }
            .filter(|version| *version != 0);
            if let Some(version) = result_version {
                self.freshness_request
                    .observe_version(&self.freshness, version);
            } else {
                self.freshness_request
                    .update_if_current(&self.freshness, |state| {
                        state.checkout_baseline = Some(SystemTime::now());
                    });
            }
        }
        Ok(result)
    }

    async fn cancel(&self) -> Result<()> {
        crate::job::JobHandle::cancel(&self.inner).await
    }
}

pub(super) fn compute_min_timestamp(
    state: &FreshnessState,
    interval: Option<Duration>,
    now: SystemTime,
) -> Option<SystemTime> {
    let interval_based = match interval {
        None => None,
        Some(d) if d.is_zero() => Some(now),
        Some(d) => Some(now.checked_sub(d).unwrap_or(now)),
    };
    match (interval_based, state.checkout_baseline) {
        (None, None) => None,
        (Some(t), None) | (None, Some(t)) => Some(t),
        (Some(a), Some(b)) => Some(a.max(b)),
    }
}

pub(super) fn freshness_headers_snapshot(
    freshness: &Mutex<FreshnessState>,
    interval: Option<Duration>,
) -> FreshnessHeaders {
    freshness_state_snapshot(freshness, interval).1
}

pub(super) fn freshness_state_snapshot(
    freshness: &Mutex<FreshnessState>,
    interval: Option<Duration>,
) -> (FreshnessState, FreshnessHeaders) {
    let state = *freshness.lock().unwrap();
    if state.pinned {
        return (
            state,
            FreshnessHeaders {
                generation: state.generation,
                ..FreshnessHeaders::default()
            },
        );
    }
    (
        state,
        FreshnessHeaders {
            generation: state.generation,
            min_version: state.min_version,
            min_timestamp: compute_min_timestamp(&state, interval, SystemTime::now()),
            min_read_version: state.min_read_version,
        },
    )
}
