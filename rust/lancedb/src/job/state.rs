// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/// Detailed lifecycle of one job attempt.
///
/// Unknown server values are retained and are never considered successful or
/// terminal. Legacy client and server labels are accepted by [`Self::from`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum JobState {
    Created,
    Queued,
    InProgress,
    Succeeded,
    Failed,
    Canceled,
    TimedOut,
    Unknown(String),
}

impl JobState {
    /// The canonical wire spelling, or the unrecognized value unchanged.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Created => "created",
            Self::Queued => "queued",
            Self::InProgress => "in_progress",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::TimedOut => "timed_out",
            Self::Unknown(value) => value,
        }
    }

    /// Whether this is a recognized terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Canceled | Self::TimedOut
        )
    }

    /// Only recognized success establishes that the operation completed.
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Succeeded)
    }
}

impl From<&str> for JobState {
    fn from(value: &str) -> Self {
        match value {
            "created" | "CREATED" => Self::Created,
            "queued" | "QUEUED" => Self::Queued,
            "running" | "in_progress" | "IN_PROGRESS" => Self::InProgress,
            "finished" | "done" | "DONE" | "succeeded" | "SUCCEEDED" => Self::Succeeded,
            "failed" | "FAILED" => Self::Failed,
            "canceled" | "CANCELED" | "cancelled" | "CANCELLED" => Self::Canceled,
            "timed_out" | "TIMED_OUT" => Self::TimedOut,
            other => Self::Unknown(other.to_string()),
        }
    }
}
