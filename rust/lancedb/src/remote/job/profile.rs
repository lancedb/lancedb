// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::RemoteJob;
use crate::error::{Error, Result};
use crate::job::{
    FunctionProfile, FunctionProfileKind, FunctionProfileOptions, FunctionProfileStatus,
};
use crate::remote::client::{HttpSend, RequestResultExt};

impl<S: HttpSend> RemoteJob<S> {
    async fn profile_request<T: DeserializeOwned>(
        &self,
        action: &str,
        body: Value,
    ) -> Result<(String, T)> {
        let request = self
            .client
            .post(&format!("/v1/jobs/profile/{action}"))
            .json(&body);
        let (request_id, response) = self.client.send(request).await?;
        let response = self.client.check_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let value = serde_json::from_str(&body).map_err(|error| Error::Http {
            source: format!("invalid Function profile {action} response: {error}").into(),
            request_id: request_id.clone(),
            status_code: None,
        })?;
        Ok((request_id, value))
    }

    pub(super) async fn capture_profile_inner(
        &self,
        worker_id: &str,
        options: FunctionProfileOptions,
    ) -> Result<FunctionProfile> {
        let duration = options.duration_seconds();
        let (request_id, profile): (_, FunctionProfile) = self
            .profile_request(
                "capture",
                json!({
                    "job_id": self.job_id,
                    "worker_id": worker_id,
                    "session_slot": options.session_slot,
                    "kind": options.kind,
                    "duration_seconds": duration,
                }),
            )
            .await?;
        if !profile.valid_for(&self.job_id, None)
            || profile.worker_id != worker_id
            || profile.session_slot != options.session_slot
            || profile.kind != options.kind
            || profile.duration_seconds != duration
        {
            return Err(Error::Http {
                source: "Function profile response does not match the requested capture".into(),
                request_id,
                status_code: None,
            });
        }
        Ok(profile)
    }

    pub(super) async fn profile_status_inner(&self, profile_id: &str) -> Result<FunctionProfile> {
        let (request_id, profile): (_, FunctionProfile) = self
            .profile_request(
                "status",
                json!({"job_id": self.job_id, "profile_id": profile_id}),
            )
            .await?;
        if !profile.valid_for(&self.job_id, Some(profile_id)) {
            return Err(Error::Http {
                source: "Function profile response does not match the requested identity".into(),
                request_id,
                status_code: None,
            });
        }
        Ok(profile)
    }

    pub(super) async fn profile_artifact_inner(&self, profile_id: &str) -> Result<Value> {
        let profile = self.profile_status_inner(profile_id).await?;
        if profile.status != FunctionProfileStatus::Complete {
            return Err(Error::Runtime {
                message: format!(
                    "Function worker profile {profile_id} is not complete ({:?}): {}",
                    profile.status,
                    profile
                        .error
                        .as_deref()
                        .unwrap_or("artifact is not available")
                ),
            });
        }
        let (request_id, artifact): (_, Value) = self
            .profile_request(
                "artifact",
                json!({"job_id": self.job_id, "profile_id": profile_id}),
            )
            .await?;
        let valid = match profile.kind {
            FunctionProfileKind::Cpu => {
                artifact.get("$schema").and_then(Value::as_str)
                    == Some("https://www.speedscope.app/file-format-schema.json")
                    && artifact
                        .pointer("/shared/frames")
                        .is_some_and(Value::is_array)
                    && artifact.get("profiles").is_some_and(Value::is_array)
            }
            FunctionProfileKind::Memory => {
                artifact.get("format").and_then(Value::as_str) == Some("lancedb.tracemalloc.v1")
                    && artifact.get("allocations").is_some_and(Value::is_array)
            }
            FunctionProfileKind::Gpu => {
                artifact.get("format").and_then(Value::as_str) == Some("lancedb.cuda.v1")
                    && artifact.get("traceEvents").is_some_and(Value::is_array)
                    && artifact.get("truncated").is_some_and(Value::is_boolean)
            }
        };
        if !valid {
            return Err(Error::Http {
                source: "Function profile artifact does not match its declared format".into(),
                request_id,
                status_code: None,
            });
        }
        Ok(artifact)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use serde_json::{Value, json};

    use crate::error::Error;
    use crate::job::{
        FunctionProfileKind, FunctionProfileOptions, FunctionProfileStatus,
        FunctionProfileWaitOptions, Job,
    };
    use crate::remote::client::test_utils::client_with_handler;
    use crate::remote::job::RemoteJob;

    fn metadata(kind: &str, status: &str) -> Value {
        json!({
            "profile_id": "profile-1", "job_id": "job-1", "worker_id": "worker-1",
            "execution_epoch": 3, "session_slot": 0, "kind": kind,
            "duration_seconds": 2.0, "status": status,
            "requested_at_millis": 1000, "deadline_at_millis": 46000,
            "expires_at_millis": 3601000, "completed_at_millis": null,
            "error": if status == "failed" { Some("gpu_unavailable") } else { None },
            "artifact_id": if status == "complete" { Some("profile-1") } else { None },
        })
    }

    #[tokio::test]
    async fn capture_sends_selected_identity_and_kind_specific_defaults() {
        for (kind, name, duration) in [
            (FunctionProfileKind::Cpu, "cpu", 10.0),
            (FunctionProfileKind::Memory, "memory", 10.0),
            (FunctionProfileKind::Gpu, "gpu", 5.0),
        ] {
            let client = client_with_handler(move |request| {
                assert_eq!(request.url().path(), "/v1/jobs/profile/capture");
                let body: Value =
                    serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
                assert_eq!(
                    body,
                    json!({
                        "job_id":"job-1", "worker_id":"worker-1", "kind":name,
                        "session_slot":2, "duration_seconds":duration,
                    })
                );
                let mut value = metadata(name, "pending");
                value["session_slot"] = json!(2);
                value["duration_seconds"] = json!(duration);
                http::Response::builder()
                    .status(202)
                    .body(value.to_string())
                    .unwrap()
            });
            let job = Job::new(Box::new(RemoteJob::new(client, "job-1".into())));
            let profile = job
                .capture_profile(
                    "worker-1",
                    FunctionProfileOptions::new(kind).session_slot(2),
                )
                .await
                .unwrap();
            assert_eq!(profile.kind, kind);
            assert_eq!(profile.execution_epoch, 3);
            assert_eq!(profile.status, FunctionProfileStatus::Pending);
            assert_eq!(job.state(), None);
        }
    }

    #[tokio::test]
    async fn invalid_capture_options_never_send_a_request() {
        let client =
            client_with_handler::<String>(|_| panic!("invalid capture sent an HTTP request"));
        let job = Job::new(Box::new(RemoteJob::new(client, "job-1".into())));
        for (kind, duration) in [
            (FunctionProfileKind::Cpu, Duration::from_millis(999)),
            (FunctionProfileKind::Memory, Duration::from_secs(31)),
            (FunctionProfileKind::Gpu, Duration::from_millis(5001)),
        ] {
            assert!(matches!(
                job.capture_profile(
                    "worker-1",
                    FunctionProfileOptions::new(kind).duration(duration)
                )
                .await,
                Err(Error::InvalidInput { .. })
            ));
        }
        assert!(
            job.capture_profile("", FunctionProfileOptions::default())
                .await
                .is_err()
        );
        assert!(
            job.capture_profile("é".repeat(129), FunctionProfileOptions::default())
                .await
                .is_err()
        );
        assert!(job.profile_status("").await.is_err());
        assert!(job.profile_artifact("p".repeat(257)).await.is_err());
    }

    #[tokio::test]
    async fn response_identity_and_unsupported_servers_are_not_accepted_as_captures() {
        for mutation in [
            "profile_id",
            "job_id",
            "worker_id",
            "kind",
            "session_slot",
            "duration_seconds",
        ] {
            let client = client_with_handler(move |_| {
                let mut value = metadata("gpu", "pending");
                value[mutation] = match mutation {
                    "profile_id" => json!(""),
                    "session_slot" => json!(1),
                    "duration_seconds" => json!(3),
                    "kind" => json!("cpu"),
                    _ => json!("other"),
                };
                http::Response::builder()
                    .status(200)
                    .body(value.to_string())
                    .unwrap()
            });
            let job = Job::new(Box::new(RemoteJob::new(client, "job-1".into())));
            assert!(matches!(
                job.capture_profile(
                    "worker-1",
                    FunctionProfileOptions::new(FunctionProfileKind::Gpu)
                        .duration(Duration::from_secs(2))
                )
                .await,
                Err(Error::Http { .. })
            ));
        }
        let client =
            client_with_handler(|_| http::Response::builder().status(200).body("{}").unwrap());
        let job = Job::new(Box::new(RemoteJob::new(client, "job-1".into())));
        assert!(matches!(
            job.profile_status("profile-1").await,
            Err(Error::Http { .. })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn wait_timeout_is_resumable_and_does_not_cancel_or_refresh_the_job() {
        let complete = Arc::new(AtomicBool::new(false));
        let status = complete.clone();
        let client = client_with_handler(move |request| {
            assert_eq!(request.url().path(), "/v1/jobs/profile/status");
            let value = metadata(
                "gpu",
                if status.load(Ordering::SeqCst) {
                    "complete"
                } else {
                    "pending"
                },
            );
            http::Response::builder()
                .status(200)
                .body(value.to_string())
                .unwrap()
        });
        let job = Job::new(Box::new(RemoteJob::new(client, "job-1".into())));
        let error = job
            .wait_for_profile(
                "profile-1",
                FunctionProfileWaitOptions::new().timeout(Duration::from_millis(50)),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Timeout { .. }));
        assert_eq!(job.state(), None);
        complete.store(true, Ordering::SeqCst);
        assert_eq!(
            job.wait_for_profile("profile-1", FunctionProfileWaitOptions::new())
                .await
                .unwrap()
                .status,
            FunctionProfileStatus::Complete
        );
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_honors_poll_interval_and_rejects_execution_changes() {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let client = client_with_handler(move |_| {
            let next = count.fetch_add(1, Ordering::SeqCst);
            let mut value = metadata("gpu", if next == 0 { "pending" } else { "complete" });
            if next > 0 {
                value["execution_epoch"] = json!(4);
            }
            http::Response::builder()
                .status(200)
                .body(value.to_string())
                .unwrap()
        });
        let job = Job::new(Box::new(RemoteJob::new(client, "job-1".into())));
        let started = tokio::time::Instant::now();
        let error = job
            .wait_for_profile(
                "profile-1",
                FunctionProfileWaitOptions::new().poll_interval(Duration::from_secs(5)),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("execution identity"));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(started.elapsed() >= Duration::from_secs(5));
    }

    #[tokio::test]
    async fn failed_capture_keeps_its_reason_and_is_not_a_failed_refresh() {
        let client = client_with_handler(|request| {
            assert_eq!(request.url().path(), "/v1/jobs/profile/status");
            http::Response::builder()
                .status(200)
                .body(metadata("gpu", "failed").to_string())
                .unwrap()
        });
        let job = Job::new(Box::new(RemoteJob::new(client, "job-1".into())));
        let profile = job.profile_status("profile-1").await.unwrap();
        assert_eq!(profile.status, FunctionProfileStatus::Failed);
        assert_eq!(profile.error.as_deref(), Some("gpu_unavailable"));
        let error = job
            .wait_for_profile("profile-1", FunctionProfileWaitOptions::new())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Runtime { .. }));
        assert!(error.to_string().contains("gpu_unavailable"));
        assert_eq!(job.state(), None);
    }

    #[tokio::test]
    async fn artifacts_preserve_all_supported_formats_and_truncation_after_refresh() {
        for (kind, artifact) in [
            (
                "cpu",
                json!({"$schema":"https://www.speedscope.app/file-format-schema.json","shared":{"frames":[{"name":"work"}]},"profiles":[],"future_field":7}),
            ),
            (
                "memory",
                json!({"format":"lancedb.tracemalloc.v1","allocations":[{"size_bytes":4096,"frames":[]}],"truncated":false}),
            ),
            (
                "gpu",
                json!({"format":"lancedb.cuda.v1","traceEvents":[{"cat":"kernel","name":"compute","ph":"X","ts":10,"dur":2}],"truncated":true}),
            ),
        ] {
            let output = artifact.clone();
            let client = client_with_handler(move |request| {
                let body: Value =
                    serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
                assert_eq!(body, json!({"job_id":"job-1","profile_id":"profile-1"}));
                let response = match request.url().path() {
                    "/v1/jobs/profile/status" => metadata(kind, "complete"),
                    "/v1/jobs/profile/artifact" => output.clone(),
                    other => panic!("retrieval must not describe or restart refresh: {other}"),
                };
                http::Response::builder()
                    .status(200)
                    .body(response.to_string())
                    .unwrap()
            });
            let job = Job::new(Box::new(RemoteJob::new(client, "job-1".into())));
            assert_eq!(job.profile_artifact("profile-1").await.unwrap(), artifact);
        }
    }

    #[tokio::test]
    async fn permission_missing_and_expired_errors_are_preserved_without_retry() {
        for code in [403, 404, 500] {
            for operation in ["capture", "status", "artifact"] {
                let calls = Arc::new(AtomicUsize::new(0));
                let seen = calls.clone();
                let client = client_with_handler(move |request| {
                    let path = request.url().path();
                    if operation == "artifact" && path.ends_with("/status") {
                        return http::Response::builder()
                            .status(200)
                            .body(metadata("gpu", "complete").to_string())
                            .unwrap();
                    }
                    assert_eq!(path, format!("/v1/jobs/profile/{operation}"));
                    seen.fetch_add(1, Ordering::SeqCst);
                    http::Response::builder()
                        .status(code)
                        .body("profile is unavailable".to_owned())
                        .unwrap()
                });
                let job = Job::new(Box::new(RemoteJob::new(client, "job-1".into())));
                let error = match operation {
                    "capture" => job
                        .capture_profile("worker-1", FunctionProfileOptions::default())
                        .await
                        .unwrap_err(),
                    "status" => job.profile_status("profile-1").await.unwrap_err(),
                    _ => job.profile_artifact("profile-1").await.unwrap_err(),
                };
                assert!(
                    matches!(error, Error::Http { status_code: Some(status), .. } if status.as_u16() == code)
                );
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            }
        }
    }

    #[tokio::test]
    async fn unrelated_or_incomplete_artifacts_are_not_exported_as_profiles() {
        let client = client_with_handler(|request| {
            let value = if request.url().path().ends_with("/status") {
                metadata("gpu", "complete")
            } else {
                json!({"traceEvents":[]})
            };
            http::Response::builder()
                .status(200)
                .body(value.to_string())
                .unwrap()
        });
        let job = Job::new(Box::new(RemoteJob::new(client, "job-1".into())));
        assert!(matches!(
            job.profile_artifact("profile-1").await,
            Err(Error::Http { .. })
        ));
        let client = client_with_handler(|request| {
            assert!(request.url().path().ends_with("/status"));
            http::Response::builder()
                .status(200)
                .body(metadata("gpu", "capturing").to_string())
                .unwrap()
        });
        let job = Job::new(Box::new(RemoteJob::new(client, "job-1".into())));
        assert!(matches!(
            job.profile_artifact("profile-1").await,
            Err(Error::Runtime { .. })
        ));
    }
}
