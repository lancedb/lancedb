// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! On-demand profiling of a Function worker's Python execution session.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub(super) fn validate_profile_id(profile_id: &str) -> Result<()> {
    if profile_id.is_empty() || profile_id.len() > 256 {
        return Err(Error::InvalidInput {
            message: "profile_id must contain between 1 and 256 bytes".into(),
        });
    }
    Ok(())
}

/// What to record in one Function worker process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum FunctionProfileKind {
    /// Python stack samples weighted by thread CPU time, exported as speedscope JSON.
    Cpu,
    /// PyTorch CUDA kernels, transfers and available CUDA host API activities.
    Gpu,
    /// Python allocations made during the capture and retained at its end.
    Memory,
}

impl FunctionProfileKind {
    fn maximum_duration(self) -> Duration {
        Duration::from_secs(if self == Self::Gpu { 5 } else { 30 })
    }
}

/// Server-reported state of a profile capture, independent of its parent job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum FunctionProfileStatus {
    /// Waiting for the Function supervisor to dispatch the capture.
    Pending,
    /// Recording the selected execution session.
    Capturing,
    /// The artifact can be retrieved until expiry or scratch reclamation.
    Complete,
    /// Capture failed; inspect [`FunctionProfile::error`] for the server's reason.
    Failed,
}

/// A snapshot of a capture's identity, status and retention information.
///
/// It describes a diagnostic capture on an existing Function refresh job, not
/// another job. Use its profile ID with [`super::Job::profile_status`] or
/// [`super::Job::profile_artifact`], including after the refresh finishes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct FunctionProfile {
    /// Retain this ID to retrieve the capture later.
    pub profile_id: String,
    /// The parent Function refresh job.
    pub job_id: String,
    /// The selected Function worker.
    pub worker_id: String,
    /// Execution epoch pinned by the server when accepting the capture.
    pub execution_epoch: i64,
    /// Python session slot; zero is the first session.
    pub session_slot: usize,
    /// The kind of evidence collected.
    pub kind: FunctionProfileKind,
    /// Requested recording window, excluding dispatch delay.
    pub duration_seconds: f64,
    /// Current capture state.
    pub status: FunctionProfileStatus,
    /// Acceptance time, in milliseconds since the Unix epoch.
    pub requested_at_millis: i64,
    /// Server deadline for an unfinished capture, in epoch milliseconds.
    pub deadline_at_millis: i64,
    /// Retention bound in epoch milliseconds; scratch reclamation may remove it sooner.
    pub expires_at_millis: i64,
    /// Completion time, when reported.
    pub completed_at_millis: Option<i64>,
    /// Server-reported failure reason, including unsupported-session errors.
    pub error: Option<String>,
    /// Server artifact identity, when the capture completes.
    pub artifact_id: Option<String>,
}

impl FunctionProfile {
    pub(crate) fn same_capture(&self, other: &Self) -> bool {
        self.profile_id == other.profile_id
            && self.job_id == other.job_id
            && self.worker_id == other.worker_id
            && self.execution_epoch == other.execution_epoch
            && self.session_slot == other.session_slot
            && self.kind == other.kind
            && self.duration_seconds == other.duration_seconds
    }

    pub(crate) fn valid_for(&self, job_id: &str, profile_id: Option<&str>) -> bool {
        self.job_id == job_id
            && !self.profile_id.is_empty()
            && self.profile_id.len() <= 256
            && profile_id.is_none_or(|id| id == self.profile_id)
            && !self.worker_id.is_empty()
            && self.worker_id.len() <= 256
            && self.duration_seconds.is_finite()
            && (1.0..=self.kind.maximum_duration().as_secs_f64()).contains(&self.duration_seconds)
            && (self.status != FunctionProfileStatus::Complete
                || self.artifact_id.as_ref().is_some_and(|id| !id.is_empty()))
    }
}

/// Options for [`super::Job::capture_profile`].
///
/// CPU and memory captures allow 1–30 seconds; GPU captures allow 1–5.
/// Omitting the duration selects 10 seconds for CPU/memory and 5 for GPU.
#[derive(Debug, Clone)]
pub struct FunctionProfileOptions {
    pub(crate) kind: FunctionProfileKind,
    pub(crate) duration: Option<Duration>,
    pub(crate) session_slot: usize,
}

impl Default for FunctionProfileOptions {
    fn default() -> Self {
        Self::new(FunctionProfileKind::Cpu)
    }
}

impl FunctionProfileOptions {
    /// Select the kind of capture, using its default duration and session zero.
    pub fn new(kind: FunctionProfileKind) -> Self {
        Self {
            kind,
            duration: None,
            session_slot: 0,
        }
    }

    /// Set the recording window. Dispatch time is additional.
    pub fn duration(mut self, duration: Duration) -> Self {
        self.duration = Some(duration);
        self
    }

    /// Select the Python session within the worker.
    pub fn session_slot(mut self, slot: usize) -> Self {
        self.session_slot = slot;
        self
    }

    pub(crate) fn duration_seconds(&self) -> f64 {
        self.duration
            .unwrap_or_else(|| {
                Duration::from_secs(if self.kind == FunctionProfileKind::Gpu {
                    5
                } else {
                    10
                })
            })
            .as_secs_f64()
    }

    pub(crate) fn validate(&self, worker_id: &str) -> Result<()> {
        if worker_id.is_empty() || worker_id.len() > 256 {
            return Err(Error::InvalidInput {
                message: "worker_id must contain between 1 and 256 bytes".into(),
            });
        }
        if !(1.0..=self.kind.maximum_duration().as_secs_f64()).contains(&self.duration_seconds()) {
            return Err(Error::InvalidInput {
                message: format!(
                    "profile duration must be between 1 and {} seconds",
                    self.kind.maximum_duration().as_secs()
                ),
            });
        }
        Ok(())
    }
}

/// Client controls for [`super::Job::wait_for_profile`].
///
/// Defaults to a 60-second timeout and two seconds between status requests.
/// The timeout includes requests and sleeps, and never cancels the capture or job.
#[derive(Debug, Clone)]
pub struct FunctionProfileWaitOptions {
    pub(crate) timeout: Duration,
    pub(crate) poll_interval: Duration,
}

impl Default for FunctionProfileWaitOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            poll_interval: Duration::from_secs(2),
        }
    }
}

impl FunctionProfileWaitOptions {
    /// Use the default timeout and polling interval.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bound this wait without cancelling server-side work.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Delay between status requests; must be greater than zero.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }
}
