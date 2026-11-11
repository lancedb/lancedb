// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Read-only status for Function-backed table columns.

use serde::{Deserialize, Serialize};

use crate::function::FunctionVersionRef;
use crate::{Error, Result};

/// Stable HTTP error code for a status read waiting on its matching finalize.
pub const FINALIZATION_PENDING_ERROR_CODE: &str = "function_column_status_finalization_pending";

/// One exact immutable Function revision attached to a table output.
pub type FunctionColumnVersionStatus = FunctionVersionRef;

/// Latest matching refresh Job projected for a Function output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionColumnLastRefreshJob {
    pub job_id: String,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub automatic: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_millis: Option<i64>,
}

/// Automatic-refresh scheduling state that the server can prove.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FunctionColumnAutoRefreshState {
    Disabled,
    Running,
    UpToDate,
    Ready,
    WaitingInterval,
    WaitingBackoff,
    Blocked,
    Unobservable,
}

/// Why an enabled automatic refresh is not currently schedulable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FunctionColumnAutoRefreshBlockedReason {
    TableHasMemWal,
    InputsNotSettled,
    DependencyInFlight,
    TableCapacity,
    Cancelled,
    PreparationBackoff,
}

/// Automatic-refresh projection for a Function output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionColumnAutoRefreshStatus {
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_job_id: Option<String>,
    pub state: FunctionColumnAutoRefreshState,
    pub failure_streak: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_cooldown_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<FunctionColumnAutoRefreshBlockedReason>,
    /// Null is intentional until the Lance Agent exposes its scheduler decision.
    pub next_run_at_millis: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_run_at_reason: Option<String>,
}

/// Exact status of one Function output at one table version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionColumnStatus {
    pub column: String,
    pub binding_id: String,
    pub function: FunctionColumnVersionStatus,
    pub table_version: u64,
    pub rows: u64,
    pub fresh: u64,
    pub stale: u64,
    pub never_computed: u64,
    pub failed: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_refresh_job: Option<FunctionColumnLastRefreshJob>,
    pub auto_refresh: FunctionColumnAutoRefreshStatus,
}

impl FunctionColumnStatus {
    /// Whether this status is safe to expose as an exact four-way count.
    pub fn validate(&self) -> Result<()> {
        let classified = self
            .fresh
            .checked_add(self.stale)
            .and_then(|count| count.checked_add(self.never_computed))
            .and_then(|count| count.checked_add(self.failed));
        if classified != Some(self.rows) {
            return Err(Error::Runtime {
                message: format!(
                    "Function column '{}' status counts do not partition {} rows",
                    self.column, self.rows
                ),
            });
        }
        if self.auto_refresh.state == FunctionColumnAutoRefreshState::Blocked
            && self.auto_refresh.blocked_reason.is_none()
        {
            return Err(Error::Runtime {
                message: format!(
                    "Function column '{}' reported a blocked scheduler state without a reason",
                    self.column
                ),
            });
        }
        if self.auto_refresh.state == FunctionColumnAutoRefreshState::Unobservable
            && self.auto_refresh.next_run_at_millis.is_none()
            && self
                .auto_refresh
                .next_run_at_reason
                .as_deref()
                .is_none_or(str::is_empty)
        {
            return Err(Error::Runtime {
                message: format!(
                    "Function column '{}' omitted a reason for an unavailable next run",
                    self.column
                ),
            });
        }
        Ok(())
    }
}

/// Validate one GET response, whose objects must all describe one snapshot.
pub fn validate_statuses(statuses: &[FunctionColumnStatus]) -> Result<()> {
    let mut table_versions = statuses.iter().map(|status| status.table_version);
    let Some(first) = table_versions.next() else {
        return Ok(());
    };
    if table_versions.any(|version| version != first) {
        return Err(Error::Runtime {
            message: "Function column statuses came from different table versions".to_string(),
        });
    }
    statuses.iter().try_for_each(FunctionColumnStatus::validate)
}

/// Whether an error is the typed transient response used while the data
/// version's matching freshness finalization is still in progress.
pub fn is_finalization_pending(error: &Error) -> bool {
    #[cfg(feature = "remote")]
    {
        error.status_code() == Some(reqwest::StatusCode::SERVICE_UNAVAILABLE)
            && error
                .remote_error_metadata()
                .and_then(|metadata| metadata.error_code.as_deref())
                == Some(FINALIZATION_PENDING_ERROR_CODE)
    }
    #[cfg(not(feature = "remote"))]
    {
        let _ = error;
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(column: &str, table_version: u64) -> FunctionColumnStatus {
        FunctionColumnStatus {
            column: column.to_string(),
            binding_id: "fb_01".to_string(),
            function: FunctionVersionRef {
                name: "embed".to_string(),
                namespace_path: Vec::new(),
                object_id: "embed".to_string(),
                location: "objects/embed".to_string(),
                version: "7".to_string(),
                manifest_digest: "sha256:abc".to_string(),
            },
            table_version,
            rows: 10,
            fresh: 6,
            stale: 2,
            never_computed: 1,
            failed: 1,
            last_refresh_job: None,
            auto_refresh: FunctionColumnAutoRefreshStatus {
                enabled: true,
                interval_seconds: Some(60),
                active_job_id: None,
                state: FunctionColumnAutoRefreshState::Unobservable,
                failure_streak: 0,
                effective_cooldown_millis: None,
                blocked_reason: None,
                next_run_at_millis: None,
                next_run_at_reason: Some(
                    "authoritative_scheduler_projection_unavailable".to_string(),
                ),
            },
        }
    }

    #[test]
    fn validates_a_single_version_and_four_way_partition() {
        validate_statuses(&[status("a", 42), status("b", 42)]).unwrap();

        let error = validate_statuses(&[status("a", 42), status("b", 43)]).unwrap_err();
        assert!(error.to_string().contains("different table versions"));

        let mut invalid = status("a", 42);
        invalid.fresh += 1;
        let error = validate_statuses(&[invalid]).unwrap_err();
        assert!(error.to_string().contains("do not partition 10 rows"));
    }

    #[test]
    fn wire_keeps_job_and_auto_refresh_metadata() {
        let mut value = status("a", 42);
        value.last_refresh_job = Some(FunctionColumnLastRefreshJob {
            job_id: "job-7".to_string(),
            state: "finished".to_string(),
            automatic: Some(true),
            completed_at_millis: Some(1_790_000_000_000),
        });
        let encoded = serde_json::to_value(value).unwrap();
        assert_eq!(encoded["last_refresh_job"]["job_id"], "job-7");
        assert_eq!(encoded["auto_refresh"]["enabled"], true);
        assert_eq!(
            encoded["auto_refresh"]["next_run_at_reason"],
            "authoritative_scheduler_projection_unavailable"
        );
    }

    #[test]
    fn up_to_date_wire_is_stable() {
        assert_eq!(
            serde_json::to_string(&FunctionColumnAutoRefreshState::UpToDate).unwrap(),
            "\"up_to_date\""
        );
    }

    #[test]
    fn blocked_auto_refresh_wire_keeps_its_reason() {
        let mut value = status("a", 42);
        value.auto_refresh.state = FunctionColumnAutoRefreshState::Blocked;
        value.auto_refresh.failure_streak = 3;
        value.auto_refresh.effective_cooldown_millis = Some(8000);
        value.auto_refresh.blocked_reason =
            Some(FunctionColumnAutoRefreshBlockedReason::PreparationBackoff);
        value.auto_refresh.next_run_at_reason = None;

        let encoded = serde_json::to_value(&value).unwrap();
        assert_eq!(encoded["auto_refresh"]["state"], "blocked");
        assert_eq!(
            encoded["auto_refresh"]["blocked_reason"],
            "preparation_backoff"
        );
        assert_eq!(encoded["auto_refresh"]["failure_streak"], 3);
        assert_eq!(encoded["auto_refresh"]["effective_cooldown_millis"], 8000);
        value.validate().unwrap();

        value.auto_refresh.blocked_reason = None;
        assert!(
            value
                .validate()
                .unwrap_err()
                .to_string()
                .contains("blocked scheduler state without a reason")
        );
    }
}
