// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Client-side job listing and diagnostic records. Job IDs and page tokens are opaque.
use serde::{Deserialize, Serialize};
use std::fmt::Display;

pub const CHILD_AVAILABILITY_KEY: &str = "lancedb.job_coordinator.history_availability";
pub const JOB_EVENT_PAGE_TOKEN_METADATA_KEY: &str = "lancedb.job_coordinator.page_token";
/// Maximum event rows supported by a server page.
pub const MAX_JOB_EVENT_QUERY_RESULTS: u32 = 10_000;

/// Table identity used in job filters and descriptions.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableRef {
    pub db_name: String,
    pub table_id: TableId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_name: Option<String>,
}
/// Namespace path and table name serialized by the job API.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableId {
    #[serde(default)]
    pub namespace_path: Vec<String>,
    pub table_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
}
impl TableRef {
    pub fn from_parts(
        db_name: &str,
        namespace_path: Vec<String>,
        table_name: &str,
        branch_name: Option<String>,
        delimiter: Option<String>,
    ) -> crate::Result<Self> {
        crate::utils::validate_database_name(db_name)?;
        crate::utils::validate_namespace(&namespace_path)?;
        crate::utils::validate_table_name(table_name)?;
        if delimiter.as_deref().is_some_and(|d| d != "$") {
            return Err(crate::Error::InvalidInput {
                message: "job table filters use the $ delimiter".into(),
            });
        }
        let namespace_path = if namespace_path == ["public"] {
            vec![]
        } else {
            namespace_path
        };
        let branch_name = branch_name.filter(|b| b != "main");
        Ok(Self {
            db_name: db_name.into(),
            table_id: TableId {
                namespace_path,
                table_name: table_name.into(),
                delimiter: None,
            },
            branch_name,
        })
    }
    pub fn table_name(&self) -> &str {
        &self.table_id.table_name
    }
    pub fn db_table_branch_id(&self) -> crate::Result<String> {
        let mut parts = self.table_id.namespace_path.clone();
        parts.push(self.table_id.table_name.clone());
        let mut id = format!("{}:{}", self.db_name, parts.join("$"));
        if let Some(branch) = &self.branch_name {
            id.push(':');
            id.push_str(branch);
        }
        Ok(id)
    }
}
fn milliseconds_since_epoch_to_str(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap_or_else(chrono::Utc::now)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

/// Cancellation options; an optional reason is recorded in the event history.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct JobCancelOptions {
    pub reason: Option<String>,
}

/// Paginated event query. Limit applies to the entire walk, not to each page.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct JobEventOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<JobEventOrder>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

/// Read the opaque continuation token carried in an event page schema.
pub fn job_event_page_token(batches: &[arrow_array::RecordBatch]) -> Option<String> {
    batches
        .first()?
        .schema()
        .metadata()
        .get(JOB_EVENT_PAGE_TOKEN_METADATA_KEY)
        .cloned()
}
/// Server-reported job failure, independent of the worker implementation.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct JobFailureInfo {
    /// The stage the job failed in.
    pub phase: String,

    /// What went wrong.
    pub message: String,

    /// Whether a retry could clear the failure.
    pub retryable: bool,
}

/// Complete server job record, retaining original state names for diagnostics.
#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct JobDetails {
    /// The unique job id.
    pub job_id: String,

    /// The type of job
    pub job_type: String,

    /// The current job state.
    pub job_state: String,

    /// When the job was created, in milliseconds since the epoch.
    pub creation_ms: i64,

    /// A job-type-specific specification string.
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub spec: serde_json::Value,

    /// Why the job failed, if it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<JobFailureInfo>,

    /// The typed terminal result, present only for successful jobs whose API
    /// contract defines one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,

    /// When the job was paused, if it is.
    ///
    /// A paused job reports `IN_PROGRESS`, like every other job that has not
    /// finished -- listing derives its state from the registry's path layout,
    /// which has one bucket for all of them, and a `job_state` describe could
    /// return but list could never produce would be a value no filter matches.
    /// This is what says the job is parked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused_at_millis: Option<i64>,

    /// The table the job targets. `None` for database- and cluster-scoped jobs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_ref: Option<TableRef>,

    /// Principal that asked for the job, when it was user-initiated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initiated_by: Option<String>,

    /// When the job reached a terminal state, else `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_millis: Option<i64>,

    /// Columns the job is scoped to, e.g. the indexed column of a reindex.
    /// Empty for table-, database- and cluster-scoped jobs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
}

impl Display for JobDetails {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeJobResponse(")?;
        write!(f, "job_id = {}", self.job_id)?;
        write!(f, ", job_type = {}", self.job_type)?;
        write!(f, ", job_state = {}", self.job_state)?;
        write!(
            f,
            ", creation_ms = {}",
            milliseconds_since_epoch_to_str(self.creation_ms)
        )?;
        if !self.spec.is_null() {
            write!(f, ", spec = {}", self.spec)?;
        }
        if let Some(paused_at_millis) = self.paused_at_millis {
            write!(
                f,
                ", paused_at = {}",
                milliseconds_since_epoch_to_str(paused_at_millis)
            )?;
        }
        if let Some(table_ref) = &self.table_ref {
            write!(
                f,
                ", table = {}.{}",
                table_ref.db_name,
                table_ref.table_name()
            )?;
        }
        if let Some(initiated_by) = &self.initiated_by {
            write!(f, ", initiated_by = {}", initiated_by)?;
        }
        if let Some(completed_at_millis) = self.completed_at_millis {
            write!(
                f,
                ", completed_at = {}",
                milliseconds_since_epoch_to_str(completed_at_millis)
            )?;
        }
        if !self.columns.is_empty() {
            write!(f, ", columns = [{}]", self.columns.join(", "))?;
        }
        if let Some(failure) = &self.failure {
            write!(
                f,
                ", failure = {} in {}{}",
                failure.message,
                failure.phase,
                if failure.retryable {
                    " (retryable)"
                } else {
                    ""
                }
            )?;
        }
        write!(f, ")")?;
        Ok(())
    }
}

/// Optional filters and pagination for a server job listing.
#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct JobListOptions {
    /// The maximum number of responses to return. The server may choose
    /// to return fewer than this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,

    /// If this is non-None, only list jobs for this table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_ref: Option<TableRef>,

    /// If this is non-None, only list jobs belonging to this database.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,

    /// If this is non-None, only list jobs of this type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_type: Option<String>,

    /// If this is non-None, only list jobs in this state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,

    /// Pagination token from a previous list jobs request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

impl Display for JobListOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JobListOptions(")?;
        let mut prefix = "";
        if let Some(table_ref) = &self.table_ref {
            write!(
                f,
                "{}table_ref = {}",
                prefix,
                table_ref
                    .db_table_branch_id()
                    .map_err(|_| std::fmt::Error)?
            )?;
            prefix = ", ";
        }
        if let Some(database) = &self.database {
            write!(f, "{}database = {}", prefix, database)?;
            prefix = ", ";
        }
        if let Some(job_type) = &self.job_type {
            write!(f, "{}job_type = {}", prefix, job_type)?;
            prefix = ", ";
        }
        if let Some(state) = &self.state {
            write!(f, "{}state = {}", prefix, state)?;
            prefix = ", ";
        }
        if let Some(page_token) = &self.page_token {
            write!(f, "{}page_token = {}", prefix, page_token)?;
        }
        write!(f, ")")?;
        Ok(())
    }
}

/// One diagnostic listing row, retaining the server state vocabulary.
#[derive(Clone, Serialize, Deserialize, Debug, Default, PartialEq, Eq)]
pub struct JobListRow {
    /// The job id.
    pub job_id: String,

    /// The table name, without URI or namespace.
    pub table: String,

    /// The job type.
    pub job_type: String,

    /// Design-style lowercase state.
    pub state: String,

    /// The date at which the job was created.
    pub created_at_millis: i64,

    /// Whether an operator has parked this job. `state` stays the coarse
    /// bucket the registry's path layout gives every unfinished job, so this
    /// is what distinguishes a paused one from a queued one.
    #[serde(default)]
    pub paused: bool,

    /// When the job reached a terminal state. `None` while it is unfinished,
    /// or when its record could not be read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_millis: Option<i64>,

    /// Principal that asked for the job, when it was user-initiated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initiated_by: Option<String>,

    /// Columns the job is scoped to, e.g. the indexed column of a reindex.
    /// Empty for table-, database- and cluster-scoped jobs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
}

impl Display for JobListRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(")?;
        write!(f, "table = {}", self.table)?;
        write!(f, ", job_id = {}", self.job_id)?;
        write!(f, ", job_type = {}", self.job_type)?;
        write!(
            f,
            ", created_at_millis = {}",
            milliseconds_since_epoch_to_str(self.created_at_millis)
        )?;
        write!(f, ", state = {}", self.state)?;
        if self.paused {
            write!(f, ", paused = true")?;
        }
        if let Some(completed_at_millis) = self.completed_at_millis {
            write!(
                f,
                ", completed_at_millis = {}",
                milliseconds_since_epoch_to_str(completed_at_millis)
            )?;
        }
        if let Some(initiated_by) = &self.initiated_by {
            write!(f, ", initiated_by = {}", initiated_by)?;
        }
        if !self.columns.is_empty() {
            write!(f, ", columns = [{}]", self.columns.join(", "))?;
        }
        write!(f, ")")?;
        Ok(())
    }
}

/// A single server job page and its opaque continuation token.
#[derive(Clone, Serialize, Deserialize, Debug, Default, PartialEq, Eq)]
pub struct JobListPage {
    /// Registry-derived list rows.
    pub jobs: Vec<JobListRow>,

    /// If this is set, clients should request another page with this token.
    /// Composite registries may return an empty page while crossing from live to
    /// archived jobs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

impl Display for JobListPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JobListPage(")?;
        write!(f, "jobs = [")?;
        let mut prefix = "";
        for job in &self.jobs {
            write!(f, "{}{}", prefix, job)?;
            prefix = ", ";
        }
        write!(f, "]")?;
        if let Some(page_token) = &self.page_token {
            write!(f, ", page_token = {}", page_token)?;
        }
        write!(f, ")")?;
        Ok(())
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobEventOrder {
    #[default]
    NewestFirst,
    OldestFirst,
}

impl JobEventOrder {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NewestFirst => "newest_first",
            Self::OldestFirst => "oldest_first",
        }
    }
}

impl Display for JobEventOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether child history has been recorded and is still retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildHistoryAvailability {
    Available,
    NotYetRecorded,
    Unavailable,
}
impl ChildHistoryAvailability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::NotYetRecorded => "not_yet_recorded",
            Self::Unavailable => "unavailable",
        }
    }
}
/// Pagination for child diagnostics. Job::children supplies the parent ID.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobChildrenRequest {
    pub job_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}
/// An opaque child ID and its diagnostic summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobChildRow {
    pub job_id: String,
    #[serde(flatten)]
    pub details: JobChildDetails,
}
/// Typed child diagnostic summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "summary", rename_all = "snake_case")]
pub enum JobChildDetails {
    FunctionRefreshFragment(JobFragmentRow),
}
/// Child diagnostics, aggregate parent progress, and history availability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobChildrenPage {
    pub job_id: String,
    pub history_availability: ChildHistoryAvailability,
    pub children: Vec<JobChildRow>,
    pub progress: Option<FunctionRefreshJobProgress>,
    pub progress_updated_at_millis: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

/// Progress of one Function refresh fragment, independent of recovery state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobFragmentRow {
    pub fragment_id: u64,
    /// Null when available diagnostics do not establish a phase, even if row counts exist.
    /// Completed means fragment finalization finished; parent publication may still be pending.
    pub phase: Option<JobFragmentPhase>,
    pub rows_planned: Option<u64>,
    pub rows_computed: Option<u64>,
    pub rows_skipped: Option<u64>,
    pub execution_epoch: Option<i64>,
    pub attempt_ordinal: Option<u32>,
    pub last_progress_at_millis: Option<i64>,
}

impl JobFragmentRow {
    pub fn unknown(fragment_id: u64) -> Self {
        Self {
            fragment_id,
            phase: None,
            rows_planned: None,
            rows_computed: None,
            rows_skipped: None,
            execution_epoch: None,
            attempt_ordinal: None,
            last_progress_at_millis: None,
        }
    }
}

/// Fragment finalization state; completion does not imply parent publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobFragmentPhase {
    Preparing,
    Running,
    Finalizing,
    /// Whole-fragment finalization finished; this does not establish parent-job success.
    Completed,
}

impl JobFragmentPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "preparing",
            Self::Running => "running",
            Self::Finalizing => "finalizing",
            Self::Completed => "completed",
        }
    }
}

/// Existing parent aggregate. Planned work can grow while planning continues.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionRefreshJobProgress {
    pub rows_planned: u64,
    pub rows_computed: u64,
    pub rows_skipped: u64,
    pub rows_remaining: u64,
    pub fragments_planned: u64,
    pub fragments_completed: u64,
    pub active_workers: u64,
    pub active_fragments: u64,
    pub last_progress_at_millis: Option<i64>,
}

impl JobListOptions {
    /// Limit the server page size.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
    /// Resume with a server-issued opaque token.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }
    /// Restrict the listing to one database.
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }
    /// Restrict the listing to a table and optional branch.
    pub fn table(mut self, table: TableRef) -> Self {
        self.table_ref = Some(table);
        self
    }
    /// Filter using the server's job type name.
    pub fn job_type(mut self, job_type: impl Into<String>) -> Self {
        self.job_type = Some(job_type.into());
        self
    }
    /// Filter using the server's state name.
    pub fn state(mut self, state: impl Into<String>) -> Self {
        self.state = Some(state.into());
        self
    }
}
impl JobCancelOptions {
    /// Record a human-readable cancellation reason.
    pub fn reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }
}
impl JobEventOptions {
    /// Maximum rows across the complete event walk.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
    /// Server-side event filter expression.
    pub fn filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }
    /// Select traversal order; continuation tokens retain their original order.
    pub fn order(mut self, order: JobEventOrder) -> Self {
        self.order = Some(order);
        self
    }
    /// Resume a previous walk.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }
}
impl JobChildrenRequest {
    /// Limit the number of children in one response.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
    /// Resume a child listing.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }
}
