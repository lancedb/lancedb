// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;

use super::{FreshnessHeaders, FreshnessState, MaterializedViewRefreshJob};
use crate::job::{
    FunctionProfileKind, FunctionProfileOptions, FunctionProfileStatus, FunctionProfileWaitOptions,
    Job,
};
use crate::materialized_view::RefreshMaterializedViewResult;
use crate::remote::client::test_utils::client_with_handler;
use crate::remote::job::RemoteJob;

#[tokio::test]
async fn materialized_view_refresh_handles_forward_profile_operations_without_finishing_refresh() {
    let client = client_with_handler(|request| {
        let body: serde_json::Value =
            serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
        assert_eq!(body["job_id"], "job-1");
        let action = request.url().path();
        let value = if action == "/v1/jobs/profile/artifact" {
            json!({"format":"lancedb.cuda.v1", "traceEvents":[], "truncated":true})
        } else {
            assert!(matches!(
                action,
                "/v1/jobs/profile/capture" | "/v1/jobs/profile/status"
            ));
            if action.ends_with("/capture") {
                assert_eq!(body["worker_id"], "worker-1");
                assert_eq!(body["kind"], "gpu");
            } else {
                assert_eq!(body["profile_id"], "profile-1");
            }
            json!({
                "profile_id":"profile-1", "job_id":"job-1", "worker_id":"worker-1",
                "execution_epoch":1, "session_slot":0, "kind":"gpu", "duration_seconds":2,
                "status":if action.ends_with("/capture") { "pending" } else { "complete" },
                "requested_at_millis":1000, "deadline_at_millis":46000,
                "expires_at_millis":3601000, "artifact_id":"profile-1"
            })
        };
        http::Response::builder()
            .status(200)
            .body(value.to_string())
            .unwrap()
    });
    let freshness = Arc::new(Mutex::new(FreshnessState::default()));
    let job: Job<RefreshMaterializedViewResult> =
        Job::new_typed(Box::new(MaterializedViewRefreshJob {
            inner: RemoteJob::new(client.clone(), "job-1".into()),
            client,
            db_name: "analytics".into(),
            name: "view".into(),
            namespace: vec![],
            identifier: "view".into(),
            server_version: Default::default(),
            sql_client: None,
            freshness: freshness.clone(),
            freshness_request: FreshnessHeaders::default(),
        }));
    let capture = job
        .capture_profile(
            "worker-1",
            FunctionProfileOptions::new(FunctionProfileKind::Gpu).duration(Duration::from_secs(2)),
        )
        .await
        .unwrap();
    assert_eq!(capture.status, FunctionProfileStatus::Pending);
    assert_eq!(
        job.wait_for_profile(&capture.profile_id, FunctionProfileWaitOptions::new())
            .await
            .unwrap()
            .status,
        FunctionProfileStatus::Complete
    );
    assert_eq!(
        job.profile_artifact(&capture.profile_id).await.unwrap()["truncated"],
        true
    );
    assert_eq!(job.state(), None);
    assert!(freshness.lock().unwrap().checkout_baseline.is_none());
}
