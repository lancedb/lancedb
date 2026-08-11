// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Tracking for server-side jobs through the `/v1/jobs` API.

use std::time::Duration;

use async_trait::async_trait;
use tokio::time::sleep;

use serde::{Deserialize, Deserializer};

use crate::error::{Error, FunctionErrorCode, JobFailure, Result};
use crate::job::{JobHandle, JobResult};
use crate::remote::client::{HttpSend, RequestResultExt, RestfulLanceDbClient};

/// Delay before the second job-state poll; doubles up to [`MAX_POLL_INTERVAL`].
const INITIAL_POLL_INTERVAL: Duration = Duration::from_millis(200);
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
enum JobState {
    InProgress,
    Cancelled,
    Failed,
    Done,
    /// A state this client version does not know; treated as still running
    /// and reported as-is if the job never settles.
    Other(String),
}

impl<'de> Deserialize<'de> for JobState {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Ok(Self::from(String::deserialize(deserializer)?.as_str()))
    }
}

impl JobState {
    /// The client vocabulary label for this state.
    fn client_label(&self) -> String {
        match self {
            Self::InProgress => "running".to_string(),
            Self::Done => "finished".to_string(),
            Self::Failed => "failed".to_string(),
            Self::Cancelled => "cancelled".to_string(),
            Self::Other(state) => state.clone(),
        }
    }
}

impl From<&str> for JobState {
    fn from(state: &str) -> Self {
        match state {
            "IN_PROGRESS" => Self::InProgress,
            "CANCELLED" => Self::Cancelled,
            // The server reports a timed-out job as FAILED on describe;
            // accept the raw registry state too in case a future server
            // stops folding it.
            "FAILED" | "TIMED_OUT" => Self::Failed,
            "DONE" => Self::Done,
            other => Self::Other(other.to_string()),
        }
    }
}

/// Closed current no-result job_type vocabulary.
const KNOWN_NO_RESULT_JOB_TYPES: &[&str] = &[
    "create_index",
    "reindex_ivf_pq",
    "reindex_ivf_flat",
    "reindex_ivf_rq",
    "reindex_ivf_hnsw_sq",
    "reindex_btree",
    "reindex_fts",
    "reindex_fm",
    "reindex_bitmap",
    "reindex_label_list",
    "reindex_zonemap",
    "reindex_ngram",
    "reindex_bloom_filter",
    "reindex_rtree",
    "compact",
    "cleanup",
    "index_remap",
    "spfresh_merge",
    "prewarm_page_cache",
    "prewarm_index_cache",
    "job_registry_archive",
];

const REGISTER_FUNCTION_JOB_TYPE: &str = "register_function";

/// Closed success-result expectation for a describe `job_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpectedSuccessResult {
    Function,
    None,
    /// Historical empty/missing job_type: only missing/null wire result.
    AbsentResultOnly,
}

/// The server's account of why a job failed. Absent from older servers, which
/// report only the terminal state.
#[derive(Deserialize)]
struct ReportedFailure {
    /// Stable Function error category when the server supplied one.
    #[serde(default)]
    error_code: Option<FunctionErrorCode>,
    #[serde(default)]
    phase: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    retryable: Option<bool>,
}

/// Outer `/v1/jobs/describe` envelope. Unknown informational fields are ignored;
/// `result` stays raw so lifecycle reads do not depend on JobResult decoding.
#[derive(Deserialize)]
struct DescribeJobResponse {
    job_state: JobState,
    #[serde(default)]
    job_type: String,
    #[serde(default)]
    result: Option<serde_json::Value>,
    #[serde(default)]
    failure: Option<ReportedFailure>,
}

fn protocol_http(request_id: String, message: impl Into<String>) -> Error {
    Error::Http {
        source: message.into().into(),
        request_id,
        status_code: None,
    }
}

fn expected_success_result(job_type: &str) -> Option<ExpectedSuccessResult> {
    if job_type == REGISTER_FUNCTION_JOB_TYPE {
        return Some(ExpectedSuccessResult::Function);
    }
    if job_type.is_empty() {
        return Some(ExpectedSuccessResult::AbsentResultOnly);
    }
    if KNOWN_NO_RESULT_JOB_TYPES.contains(&job_type) {
        return Some(ExpectedSuccessResult::None);
    }
    None
}

/// Strict DONE projection of a describe payload into [`JobResult`].
fn project_done_job_result(
    job_type: &str,
    raw_result: Option<&serde_json::Value>,
    request_id: String,
) -> Result<JobResult> {
    let Some(expected) = expected_success_result(job_type) else {
        return Err(protocol_http(
            request_id,
            "unknown job_type for success result projection",
        ));
    };

    match (expected, raw_result) {
        (ExpectedSuccessResult::Function, None) => Err(protocol_http(
            request_id,
            "register_function DONE response missing Function result",
        )),
        (ExpectedSuccessResult::None, None) | (ExpectedSuccessResult::AbsentResultOnly, None) => {
            Ok(JobResult::None)
        }
        (ExpectedSuccessResult::AbsentResultOnly, Some(_)) => Err(protocol_http(
            request_id,
            "historical empty job_type cannot carry an explicit success result",
        )),
        (ExpectedSuccessResult::Function, Some(raw)) | (ExpectedSuccessResult::None, Some(raw)) => {
            let Ok(decoded) = serde_json::from_value::<JobResult>(raw.clone()) else {
                return Err(protocol_http(request_id, "failed to decode job result"));
            };
            match (expected, decoded) {
                (ExpectedSuccessResult::Function, JobResult::Function(function)) => {
                    Ok(JobResult::Function(function))
                }
                (ExpectedSuccessResult::None, JobResult::None) => Ok(JobResult::None),
                _ => Err(protocol_http(
                    request_id,
                    "job result kind does not match job_type expectation",
                )),
            }
        }
    }
}

pub struct RemoteJob<S: HttpSend> {
    client: RestfulLanceDbClient<S>,
    job_id: String,
}

impl<S: HttpSend> RemoteJob<S> {
    pub fn new(client: RestfulLanceDbClient<S>, job_id: String) -> Self {
        Self { client, job_id }
    }

    /// One `/v1/jobs/describe` round trip.
    async fn describe(&self) -> Result<(String, DescribeJobResponse)> {
        let request = self
            .client
            .post("/v1/jobs/describe")
            .json(&serde_json::json!({ "job_id": self.job_id }));
        let (request_id, response) = self.client.send(request).await?;
        let response = self.client.check_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let description: DescribeJobResponse =
            serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("failed to parse job description: {}", e).into(),
                request_id: request_id.clone(),
                status_code: None,
            })?;
        Ok((request_id, description))
    }
}

#[async_trait]
impl<S: HttpSend> JobHandle for RemoteJob<S> {
    fn id(&self) -> Option<&str> {
        Some(&self.job_id)
    }

    async fn status(&self) -> Result<String> {
        Ok(self.describe().await?.1.job_state.client_label())
    }

    async fn wait(&self) -> Result<JobResult> {
        let mut interval = INITIAL_POLL_INTERVAL;
        loop {
            let (request_id, description) = self.describe().await?;
            if !matches!(description.job_state, JobState::Done) && description.result.is_some() {
                return Err(protocol_http(
                    request_id,
                    "non-DONE job describe response carried a success result",
                ));
            }
            match description.job_state {
                JobState::Done => {
                    return project_done_job_result(
                        &description.job_type,
                        description.result.as_ref(),
                        request_id,
                    );
                }
                JobState::Failed => {
                    return Err(Error::JobFailed {
                        job_id: Some(self.job_id.clone()),
                        failure: description
                            .failure
                            .map(|reported| JobFailure {
                                error_code: reported.error_code,
                                phase: reported.phase,
                                message: reported.message,
                                retryable: reported.retryable,
                                source: None,
                            })
                            .unwrap_or_default(),
                    });
                }
                JobState::Cancelled => {
                    return Err(Error::JobCancelled {
                        job_id: Some(self.job_id.clone()),
                    });
                }
                JobState::InProgress => {}
                JobState::Other(ref state) => {
                    log::debug!("job {} is in unrecognized state {state}", self.job_id)
                }
            }
            sleep(interval).await;
            interval = (interval * 2).min(MAX_POLL_INTERVAL);
        }
    }

    async fn cancel(&self) -> Result<()> {
        let request = self
            .client
            .post("/v1/jobs/cancel")
            .json(&serde_json::json!({ "job_id": self.job_id }));
        let (request_id, response) = self.client.send(request).await?;
        self.client
            .check_response(&request_id, response)
            .await
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::FunctionErrorCode;
    use crate::function::{
        Function, FunctionId, FunctionOutput, FunctionParameter, FunctionSignature,
    };
    use crate::job::JobResult;
    use crate::remote::client::test_utils::client_with_handler;
    use arrow_schema::DataType;
    use serde_json::{Value, json};

    /// Terminal DONE with no result field projects as [`JobResult::None`].
    #[tokio::test]
    async fn local_job_result_remote_done_without_result_projects_none() {
        let client = client_with_handler(|_| {
            http::Response::builder()
                .status(200)
                .body(r#"{"job_id":"job-done","job_state":"DONE"}"#)
                .unwrap()
        });
        let job = RemoteJob::new(client, "job-done".into());
        let result = job.wait().await.expect("DONE with no result must succeed");
        assert_eq!(result, JobResult::None);
    }

    #[tokio::test]
    async fn wait_decodes_known_failure_error_code() {
        let client = client_with_handler(|_| {
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"job_id":"job-err","job_state":"FAILED","failure":{"error_code":"stale_or_conflicting_input","phase":"commit","message":"looks like udf_execution_failure","retryable":true}}"#,
                )
                .unwrap()
        });
        let job = RemoteJob::new(client, "job-err".into());
        let err = job.wait().await.expect_err("FAILED must surface JobFailed");
        match err {
            Error::JobFailed { failure, .. } => {
                match &failure.error_code {
                    Some(code) => {
                        assert_eq!(code, &FunctionErrorCode::StaleOrConflictingInput);
                        assert_ne!(code, &FunctionErrorCode::UdfExecutionFailure);
                    }
                    None => panic!("known error_code must be decoded"),
                }
                assert_eq!(failure.phase.as_deref(), Some("commit"));
                assert_eq!(failure.retryable, Some(true));
            }
            other => panic!("expected Error::JobFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn wait_preserves_unrecognized_failure_error_code() {
        let client = client_with_handler(|_| {
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"job_id":"job-err","job_state":"FAILED","failure":{"error_code":"enterprise_future_category_xyz","phase":"execute","message":"new category","retryable":false}}"#,
                )
                .unwrap()
        });
        let job = RemoteJob::new(client, "job-err".into());
        let err = job.wait().await.expect_err("FAILED must surface JobFailed");
        match err {
            Error::JobFailed { failure, .. } => match &failure.error_code {
                Some(FunctionErrorCode::Unrecognized(raw)) => {
                    assert_eq!(raw, "enterprise_future_category_xyz");
                }
                Some(other) => panic!("unknown error_code must stay Unrecognized, got {other:?}"),
                None => panic!("unknown error_code must not be dropped"),
            },
            other => panic!("expected Error::JobFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn wait_allows_older_failure_payload_without_error_code() {
        let client = client_with_handler(|_| {
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"job_id":"job-err","job_state":"FAILED","failure":{"phase":"execute","message":"generated_column_incomplete in logs","retryable":true}}"#,
                )
                .unwrap()
        });
        let job = RemoteJob::new(client, "job-err".into());
        let err = job.wait().await.expect_err("FAILED must surface JobFailed");
        match err {
            Error::JobFailed { failure, .. } => {
                assert!(
                    failure.error_code.is_none(),
                    "older payloads without error_code must not invent a category from message/phase/retryable: {failure:?}"
                );
                assert_eq!(failure.phase.as_deref(), Some("execute"));
                assert_eq!(failure.retryable, Some(true));
            }
            other => panic!("expected Error::JobFailed, got {other:?}"),
        }
    }

    /// Closed current no-result job_type vocabulary.
    const KNOWN_NO_RESULT_JOB_TYPES: &[&str] = &[
        "create_index",
        "reindex_ivf_pq",
        "reindex_ivf_flat",
        "reindex_ivf_rq",
        "reindex_ivf_hnsw_sq",
        "reindex_btree",
        "reindex_fts",
        "reindex_fm",
        "reindex_bitmap",
        "reindex_label_list",
        "reindex_zonemap",
        "reindex_ngram",
        "reindex_bloom_filter",
        "reindex_rtree",
        "compact",
        "cleanup",
        "index_remap",
        "spfresh_merge",
        "prewarm_page_cache",
        "prewarm_index_cache",
        "job_registry_archive",
    ];

    #[derive(Clone)]
    enum JsonField {
        Absent,
        Null,
        Present(Value),
    }

    fn sample_remote_function() -> Function {
        let id = FunctionId::try_new("fn.exact.remote-job-result").expect("valid FunctionId");
        let signature = FunctionSignature::try_new(
            vec![
                FunctionParameter::new("x", DataType::Int32),
                FunctionParameter::new("label", DataType::Utf8),
            ],
            FunctionOutput::new(DataType::Int32, true),
        )
        .expect("valid FunctionSignature");
        Function::new(id, signature)
    }

    fn assert_exact_function(actual: &Function, expected: &Function) {
        assert_eq!(actual.id(), expected.id());
        assert_eq!(actual.signature(), expected.signature());
    }

    fn assert_http_protocol_err(err: Error) {
        match err {
            Error::Http { .. } => {}
            other => panic!("expected Error::Http protocol failure, got {other:?}"),
        }
    }

    fn job_result_none_wire() -> Value {
        serde_json::to_value(JobResult::None).expect("serialize JobResult::None wire")
    }

    fn job_result_function_wire(function: &Function) -> Value {
        serde_json::to_value(JobResult::Function(function.clone()))
            .expect("serialize JobResult::Function wire")
    }

    fn describe_body(
        job_id: &str,
        job_state: &str,
        job_type: JsonField,
        result: JsonField,
    ) -> String {
        let mut body = json!({
            "job_id": job_id,
            "job_state": job_state,
        });
        let object = body
            .as_object_mut()
            .expect("describe body must be a JSON object");
        match job_type {
            JsonField::Absent => {}
            JsonField::Null => {
                object.insert("job_type".into(), Value::Null);
            }
            JsonField::Present(value) => {
                object.insert("job_type".into(), value);
            }
        }
        match result {
            JsonField::Absent => {}
            JsonField::Null => {
                object.insert("result".into(), Value::Null);
            }
            JsonField::Present(value) => {
                object.insert("result".into(), value);
            }
        }
        body.to_string()
    }

    fn remote_job_with_describe_body(job_id: &str, body: String) -> RemoteJob<impl HttpSend> {
        let client = client_with_handler(move |_| {
            http::Response::builder()
                .status(200)
                .body(body.clone())
                .unwrap()
        });
        RemoteJob::new(client, job_id.into())
    }

    async fn wait_expect_none(job: &RemoteJob<impl HttpSend>) {
        let result = job
            .wait()
            .await
            .expect("expected successful None projection");
        assert_eq!(result, JobResult::None);
    }

    async fn wait_expect_http(job: &RemoteJob<impl HttpSend>) {
        let err = job
            .wait()
            .await
            .expect_err("expected remote protocol Error::Http");
        assert_http_protocol_err(err);
    }

    /// register_function DONE with a Function result returns the exact ID and signature.
    #[tokio::test]
    async fn remote_job_result_register_function_done_returns_exact_function() {
        let expected = sample_remote_function();
        let body = describe_body(
            "job-register",
            "DONE",
            JsonField::Present(Value::String("register_function".into())),
            JsonField::Present(job_result_function_wire(&expected)),
        );
        let job = remote_job_with_describe_body("job-register", body);
        let result = job
            .wait()
            .await
            .expect("register_function DONE with Function must succeed");
        match result {
            JobResult::Function(function) => assert_exact_function(&function, &expected),
            JobResult::None => panic!("register_function success must not project as None"),
        }
    }

    /// register_function DONE with missing, null, or explicit None is protocol Http.
    #[tokio::test]
    async fn remote_job_result_register_function_missing_null_or_explicit_none_is_http() {
        let cases = [
            ("absent", JsonField::Absent),
            ("null", JsonField::Null),
            ("explicit_none", JsonField::Present(job_result_none_wire())),
        ];
        let mut unexpected = Vec::new();
        for (label, result_field) in cases {
            let body = describe_body(
                "job-register-missing",
                "DONE",
                JsonField::Present(Value::String("register_function".into())),
                result_field,
            );
            let job = remote_job_with_describe_body("job-register-missing", body);
            match job.wait().await {
                Err(Error::Http { .. }) => {}
                Ok(value) => unexpected.push(format!("{label}: Ok({value:?})")),
                Err(other) => unexpected.push(format!("{label}: Err({other:?})")),
            }
        }
        assert!(
            unexpected.is_empty(),
            "register_function without Function must be Error::Http for every case: {unexpected:?}"
        );
    }

    /// create_index accepts explicit None or missing; every known no-result type accepts missing.
    #[tokio::test]
    async fn remote_job_result_known_no_result_types_project_none() {
        let create_index_cases = [
            JsonField::Absent,
            JsonField::Present(job_result_none_wire()),
        ];
        for result_field in create_index_cases {
            let body = describe_body(
                "job-create-index",
                "DONE",
                JsonField::Present(Value::String("create_index".into())),
                result_field,
            );
            let job = remote_job_with_describe_body("job-create-index", body);
            wait_expect_none(&job).await;
        }

        for job_type in KNOWN_NO_RESULT_JOB_TYPES {
            let body = describe_body(
                "job-no-result",
                "DONE",
                JsonField::Present(Value::String((*job_type).into())),
                JsonField::Absent,
            );
            let job = remote_job_with_describe_body("job-no-result", body);
            wait_expect_none(&job).await;
        }
    }

    /// A known no-result job_type carrying Function is protocol Http.
    #[tokio::test]
    async fn remote_job_result_known_no_result_with_function_is_http() {
        let function = sample_remote_function();
        let body = describe_body(
            "job-create-index-function",
            "DONE",
            JsonField::Present(Value::String("create_index".into())),
            JsonField::Present(job_result_function_wire(&function)),
        );
        let job = remote_job_with_describe_body("job-create-index-function", body);
        wait_expect_http(&job).await;
    }

    /// Unknown job_type fails wait; empty historical job_type succeeds only without an explicit result.
    #[tokio::test]
    async fn remote_job_result_unknown_or_empty_job_type_expectation() {
        // Historical empty job_type without an explicit result remains None.
        for result_field in [JsonField::Absent, JsonField::Null] {
            let body = describe_body(
                "job-empty-type",
                "DONE",
                JsonField::Present(Value::String("".into())),
                result_field,
            );
            let job = remote_job_with_describe_body("job-empty-type", body);
            wait_expect_none(&job).await;
        }

        let mut unexpected = Vec::new();
        let reject_cases = [
            (
                "unknown_missing_result",
                JsonField::Present(Value::String("future_job_type_xyz".into())),
                JsonField::Absent,
            ),
            (
                "unknown_explicit_none",
                JsonField::Present(Value::String("future_job_type_xyz".into())),
                JsonField::Present(job_result_none_wire()),
            ),
            (
                "empty_explicit_none",
                JsonField::Present(Value::String("".into())),
                JsonField::Present(job_result_none_wire()),
            ),
            (
                "empty_explicit_function",
                JsonField::Present(Value::String("".into())),
                JsonField::Present(job_result_function_wire(&sample_remote_function())),
            ),
        ];
        for (label, job_type, result_field) in reject_cases {
            let body = describe_body("job-type-expectation", "DONE", job_type, result_field);
            let job = remote_job_with_describe_body("job-type-expectation", body);
            match job.wait().await {
                Err(Error::Http { .. }) => {}
                Ok(value) => unexpected.push(format!("{label}: Ok({value:?})")),
                Err(other) => unexpected.push(format!("{label}: Err({other:?})")),
            }
        }
        assert!(
            unexpected.is_empty(),
            "unknown/empty job_type expectation mismatches must be Error::Http: {unexpected:?}"
        );
    }

    /// Unknown or malformed result kind, version, outer field, or nested Function fails wait.
    #[tokio::test]
    async fn remote_job_result_malformed_or_unknown_result_is_http() {
        let function = sample_remote_function();
        let function_wire = job_result_function_wire(&function);
        let none_wire = job_result_none_wire();

        let mut unknown_kind = none_wire.clone();
        unknown_kind["kind"] = Value::String("artifact".into());

        let mut unknown_version = none_wire.clone();
        unknown_version["format_version"] = Value::from(2);

        let mut unknown_outer_field = none_wire.clone();
        unknown_outer_field
            .as_object_mut()
            .unwrap()
            .insert("unexpected_field".into(), Value::Bool(true));

        let mut empty_function_id = function_wire.clone();
        empty_function_id["function"]["id"] = Value::String("".into());

        let mut unknown_nested_field = function_wire.clone();
        unknown_nested_field["function"]
            .as_object_mut()
            .unwrap()
            .insert("unexpected_field".into(), Value::Bool(true));

        let mut malformed_nested_version = function_wire.clone();
        malformed_nested_version["function"]["format_version"] = Value::from(2);

        let malformed_results = [
            ("unknown_kind", unknown_kind),
            ("unknown_version", unknown_version),
            ("unknown_outer_field", unknown_outer_field),
            ("empty_function_id", empty_function_id),
            ("unknown_nested_field", unknown_nested_field),
            ("malformed_nested_version", malformed_nested_version),
        ];
        let mut unexpected = Vec::new();
        for (label, result) in malformed_results {
            let body = describe_body(
                "job-bad-result",
                "DONE",
                JsonField::Present(Value::String("register_function".into())),
                JsonField::Present(result),
            );
            let job = remote_job_with_describe_body("job-bad-result", body);
            match job.wait().await {
                Err(Error::Http { .. }) => {}
                Ok(value) => unexpected.push(format!("{label}: Ok({value:?})")),
                Err(other) => unexpected.push(format!("{label}: Err({other:?})")),
            }
        }
        assert!(
            unexpected.is_empty(),
            "malformed/unknown result must be Error::Http for every case: {unexpected:?}"
        );
    }

    /// For unknown result or job_type, status reports finished while wait fails on a separate handle.
    #[tokio::test]
    async fn remote_job_result_status_observes_finished_while_wait_rejects_unknown() {
        let cases = [
            (
                "job-unknown-result",
                JsonField::Present(Value::String("create_index".into())),
                JsonField::Present(json!({
                    "format_version": 1,
                    "kind": "future_result_kind",
                    "raw": {"keep": true}
                })),
            ),
            (
                "job-unknown-type",
                JsonField::Present(Value::String("future_job_type_xyz".into())),
                JsonField::Present(job_result_none_wire()),
            ),
        ];

        let mut unexpected = Vec::new();
        for (job_id, job_type, result) in cases {
            let body = describe_body(job_id, "DONE", job_type, result);

            let status_job = remote_job_with_describe_body(job_id, body.clone());
            let status = status_job
                .status()
                .await
                .expect("status must observe terminal DONE");
            assert_eq!(status, "finished");

            let wait_job = remote_job_with_describe_body(job_id, body);
            match wait_job.wait().await {
                Err(Error::Http { .. }) => {}
                Ok(value) => unexpected.push(format!("{job_id}: Ok({value:?})")),
                Err(other) => unexpected.push(format!("{job_id}: Err({other:?})")),
            }
        }
        assert!(
            unexpected.is_empty(),
            "wait must be Error::Http while status stayed finished: {unexpected:?}"
        );
    }

    /// non-DONE FAILED or CANCELLED carrying a success result is protocol Http.
    #[tokio::test]
    async fn remote_job_result_non_done_carrying_success_result_is_http() {
        let function_wire = job_result_function_wire(&sample_remote_function());
        let none_wire = job_result_none_wire();
        let cases = [
            (
                "job-failed-function",
                "FAILED",
                JsonField::Present(Value::String("register_function".into())),
                JsonField::Present(function_wire),
            ),
            (
                "job-cancelled-none",
                "CANCELLED",
                JsonField::Present(Value::String("create_index".into())),
                JsonField::Present(none_wire),
            ),
        ];

        let mut unexpected = Vec::new();
        for (job_id, job_state, job_type, result) in cases {
            let body = describe_body(job_id, job_state, job_type, result);
            let job = remote_job_with_describe_body(job_id, body);
            match job.wait().await {
                Err(Error::Http { .. }) => {}
                Err(Error::JobFailed { .. }) => {
                    unexpected.push(format!("{job_id}: JobFailed"));
                }
                Err(Error::JobCancelled { .. }) => {
                    unexpected.push(format!("{job_id}: JobCancelled"));
                }
                Ok(value) => unexpected.push(format!("{job_id}: Ok({value:?})")),
                Err(other) => unexpected.push(format!("{job_id}: Err({other:?})")),
            }
        }
        assert!(
            unexpected.is_empty(),
            "non-DONE success result must be Error::Http, not lifecycle errors: {unexpected:?}"
        );
    }

    /// Unknown informational fields on the describe outer envelope are tolerated.
    #[tokio::test]
    async fn remote_job_result_unknown_outer_envelope_fields_tolerated() {
        let create_index_body = json!({
            "job_id": "job-create-index-extra",
            "job_state": "DONE",
            "job_type": "create_index",
            "creation_ms": 7,
            "server_note": "informational-only",
        });
        let create_index_job =
            remote_job_with_describe_body("job-create-index-extra", create_index_body.to_string());
        wait_expect_none(&create_index_job).await;

        let expected = sample_remote_function();
        let register_body = json!({
            "job_id": "job-register-extra",
            "job_state": "DONE",
            "job_type": "register_function",
            "result": job_result_function_wire(&expected),
            "creation_ms": 42,
            "server_note": "informational-only",
            "spec": {"ignored": true},
        });
        let register_job =
            remote_job_with_describe_body("job-register-extra", register_body.to_string());
        let result = register_job
            .wait()
            .await
            .expect("unknown outer fields must not block Function projection");
        match result {
            JobResult::Function(function) => assert_exact_function(&function, &expected),
            JobResult::None => panic!("register_function success must not project as None"),
        }
    }
}
