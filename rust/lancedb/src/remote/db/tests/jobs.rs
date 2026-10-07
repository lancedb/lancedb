// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[tokio::test]
async fn test_list_jobs_paginates() {
    let page = Arc::new(AtomicUsize::new(0));
    let conn = Connection::new_with_handler(move |request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/jobs/list");
        let body: serde_json::Value =
            serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
        match page.fetch_add(1, Ordering::SeqCst) {
            0 => {
                assert!(body.get("page_token").is_none());
                http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"jobs": [{"job_id": "job-1", "table": "t1", "job_type": "create_index", "state": "in_progress", "created_at_millis": 1000}], "page_token": "next"}"#,
                    )
                    .unwrap()
            }
            _ => {
                assert_eq!(body["page_token"], "next");
                http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"jobs": [{"job_id": "job-2", "table": "t2", "job_type": "create_index", "state": "succeeded", "created_at_millis": 2000}, {"job_id": "job-3", "table": "t3", "job_type": "create_index", "state": "timed_out", "created_at_millis": 3000}]}"#,
                    )
                    .unwrap()
            }
        }
    });
    let jobs = conn.list_jobs().await.unwrap();
    assert_eq!(jobs.len(), 3);
    assert_eq!(jobs[0].job_id, "job-1");
    assert_eq!(jobs[0].table, "t1");
    assert_eq!(jobs[0].state, "running");
    assert_eq!(jobs[1].job_id, "job-2");
    assert_eq!(jobs[1].state, "finished");
    assert_eq!(jobs[1].created_at_millis, 2000);
    assert_eq!(jobs[2].job_id, "job-3");
    assert_eq!(jobs[2].state, "failed");
}

#[tokio::test]
async fn test_list_jobs_rejects_a_page_token_cycle() {
    let requests = Arc::new(AtomicUsize::new(0));
    let seen = requests.clone();
    let conn = Connection::new_with_handler(move |request| {
        let body: serde_json::Value =
            serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
        match seen.fetch_add(1, Ordering::SeqCst) {
            0 => assert!(body.get("page_token").is_none()),
            _ => assert_eq!(body["page_token"], "loop"),
        }
        http::Response::builder()
            .status(200)
            .body(r#"{"jobs": [], "page_token": "loop"}"#)
            .unwrap()
    });

    let error = conn.list_jobs().await.unwrap_err();
    assert!(
        matches!(
            &error,
            Error::Http {
                status_code: Some(http::StatusCode::OK),
                ..
            }
        ),
        "got {error:?}"
    );
    assert_eq!(requests.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_open_job() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/jobs/describe");
        let body: serde_json::Value =
            serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
        assert_eq!(body["job_id"], "job-1");
        http::Response::builder()
            .status(200)
            .body(
                r#"{"job_id": "job-1", "job_type": "create_index", "job_state": "FAILED", "creation_ms": 1000, "spec": {"column": "vec"}, "failure": {"phase": "execute", "message": "worker died", "retryable": true}}"#,
            )
            .unwrap()
    });
    // Opening populates the handle, so the accessors answer without a
    // second round trip.
    let job = conn.open_job("job-1").await.unwrap();
    assert_eq!(job.id(), Some("job-1"));
    assert_eq!(job.job_type().as_deref(), Some("create_index"));
    assert_eq!(job.state().as_deref(), Some("failed"));
    assert_eq!(job.creation_ms(), Some(1000));
    assert_eq!(job.spec().unwrap()["column"], "vec");
    assert!(job.result().is_none());
    let failure = job.failure().unwrap();
    assert_eq!(failure.phase.as_deref(), Some("execute"));
    assert_eq!(failure.message.as_deref(), Some("worker died"));
    assert_eq!(failure.retryable, Some(true));
}

#[tokio::test]
async fn test_open_job_reports_the_terminal_result() {
    let conn = Connection::new_with_handler(|_| {
        http::Response::builder()
            .status(200)
            .body(
                r#"{"job_id": "job-1", "job_type": "refresh_column", "job_state": "DONE", "creation_ms": 1000, "result": {"rows_assigned": 1000000, "rows_failed": 0}}"#,
            )
            .unwrap()
    });
    let job = conn.open_job("job-1").await.unwrap();
    assert_eq!(job.state().as_deref(), Some("finished"));
    let result = job.result().unwrap();
    assert_eq!(result["rows_assigned"], 1_000_000);
    assert_eq!(result["rows_failed"], 0);
}

#[tokio::test]
async fn test_open_job_missing_fails() {
    let conn = Connection::new_with_handler(|_| {
        http::Response::builder()
            .status(404)
            .body("no such job")
            .unwrap()
    });
    let err = conn.open_job("nope").await.unwrap_err();
    assert!(
        matches!(&err, Error::JobNotFound { job_id } if job_id == "nope"),
        "{err:?}"
    );
}

#[tokio::test]
async fn test_pause_and_resume_job() {
    use crate::database::{PauseJobStatus, ResumeJobStatus};
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.url().path(), "/v1/jobs/pause");
        http::Response::builder()
            .status(200)
            .body(r#"{"job_id": "job-1", "paused": true}"#)
            .unwrap()
    });
    assert_eq!(
        conn.pause_job("job-1").await.unwrap(),
        PauseJobStatus::Pausing
    );

    let conn = Connection::new_with_handler(|_| {
        http::Response::builder()
            .status(200)
            .body(r#"{"job_id": "job-1", "paused": false, "committing": true}"#)
            .unwrap()
    });
    assert_eq!(
        conn.pause_job("job-1").await.unwrap(),
        PauseJobStatus::Committing
    );

    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.url().path(), "/v1/jobs/resume");
        http::Response::builder()
            .status(200)
            .body(r#"{"job_id": "job-1", "resumed": false, "still_pausing": true}"#)
            .unwrap()
    });
    assert_eq!(
        conn.resume_job("job-1").await.unwrap(),
        ResumeJobStatus::StillPausing
    );
}

#[tokio::test]
async fn test_job_events_scope_to_that_job() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "state",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow_array::StringArray::from(vec![
            "claim_complete",
        ]))],
    )
    .unwrap();
    let mut events = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut events, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let conn = Connection::new_with_handler(move |request| {
        let body: serde_json::Value =
            serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
        if request.url().path() == "/v1/jobs/describe" {
            return http::Response::builder()
                .status(200)
                .body(
                    r#"{"job_id": "job-1", "job_type": "refresh_column", "job_state": "IN_PROGRESS", "creation_ms": 1}"#
                        .as_bytes()
                        .to_vec(),
                )
                .unwrap();
        }
        assert_eq!(request.url().path(), "/v1/jobs/query_events");
        // The handle supplies job_id; the caller only narrows the query.
        assert_eq!(body["job_id"], "job-1");
        assert_eq!(body["limit"], 500);
        assert_eq!(body["filter"], "state = 'claim_complete'");
        http::Response::builder()
            .status(200)
            .body(events.clone())
            .unwrap()
    });
    let job = conn.open_job("job-1").await.unwrap();
    let batches = job
        .events(
            JobEventsRequest::default()
                .limit(500)
                .filter("state = 'claim_complete'"),
        )
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn test_job_events_keep_the_schema_when_nothing_matches() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "state",
        DataType::Utf8,
        false,
    )]));
    let mut events = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut events, &schema).unwrap();
        writer.finish().unwrap();
    }
    let conn = Connection::new_with_handler(move |request| {
        if request.url().path() == "/v1/jobs/describe" {
            return http::Response::builder()
                .status(200)
                .body(
                    r#"{"job_id": "job-1", "job_type": "refresh_column", "job_state": "IN_PROGRESS", "creation_ms": 1}"#
                        .as_bytes()
                        .to_vec(),
                )
                .unwrap();
        }
        let body: serde_json::Value =
            serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
        // Only the job id when the caller narrows nothing.
        assert_eq!(body, serde_json::json!({ "job_id": "job-1" }));
        http::Response::builder()
            .status(200)
            .body(events.clone())
            .unwrap()
    });
    let job = conn.open_job("job-1").await.unwrap();
    let batches = job.events(JobEventsRequest::default()).await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 0);
    assert_eq!(batches[0].schema(), schema);
}

/// A component that is not a legal Secret component never reaches a
/// transport. Before the identifier was checked here, each of these decided
/// the route instead of the name: `a/b` and `../jobs` left `/v1/secret/`
/// entirely, carrying a create body that holds a credential, and `a$b` read
/// as the namespace `a` and the name `b`.
#[tokio::test]
async fn test_an_illegal_component_never_reaches_the_transport() {
    use std::sync::{Arc, Mutex};
    for name in [
        "../jobs",
        "a/b",
        "a$b",
        "with space",
        "q?x",
        "a#b",
        "a%2Fb",
        "",
    ] {
        let reached = Arc::new(Mutex::new(false));
        let flag = reached.clone();
        let conn = Connection::new_with_handler(move |_| {
            *flag.lock().unwrap() = true;
            http::Response::builder().status(200).body("{}").unwrap()
        });
        let error = conn
            .create_secret(name, "sk-live-0001", &[])
            .await
            .expect_err("an illegal component must be refused");
        assert!(!*reached.lock().unwrap(), "{name:?} reached the transport");
        assert!(
            error.to_string().contains("Secret name"),
            "{name:?}: {error}"
        );
    }
}

/// `.` and `..` pass the character set and still cannot address anything:
/// URL parsing resolves them as relative segments, and after
/// percent-decoding, so no spelling of either survives.
#[tokio::test]
async fn test_a_relative_segment_component_is_refused() {
    use std::sync::{Arc, Mutex};
    // The percent-encoded spellings are caught a step earlier, by the
    // character set: `%` is not a character a Secret component may hold.
    // They reach the relative-segment rule only where there is no charset
    // to catch them first -- see the Function case below.
    for component in [".", ".."] {
        let reached = Arc::new(Mutex::new(false));
        let flag = reached.clone();
        let conn = Connection::new_with_handler(move |_| {
            *flag.lock().unwrap() = true;
            http::Response::builder()
                .status(200)
                .body(r#"{"secrets":[]}"#)
                .unwrap()
        });
        let by_name = conn
            .drop_secret(component, &[])
            .await
            .expect_err("a dot-only name must be refused");
        assert!(
            by_name.to_string().contains("relative path segments"),
            "{by_name}"
        );

        let by_segment = conn
            .list_secrets(&[component.to_string()])
            .await
            .expect_err("a dot-only namespace segment must be refused");
        assert!(
            by_segment.to_string().contains("relative path segments"),
            "{by_segment}"
        );
        assert!(
            !*reached.lock().unwrap(),
            "{component:?} reached the transport"
        );
    }
}

/// An identifier the service accepts is untouched by the encoding, so the
/// route reads like the table and Function routes beside it.
#[tokio::test]
async fn test_an_admissible_identifier_is_not_encoded() {
    use std::sync::{Arc, Mutex};
    let seen = Arc::new(Mutex::new(String::new()));
    let captured = seen.clone();
    let conn = Connection::new_with_handler(move |request| {
        *captured.lock().unwrap() = request.url().path().to_string();
        http::Response::builder().status(200).body("{}").unwrap()
    });
    conn.drop_secret(
        "openai-prod.v1",
        &["prod".to_string(), "vision_2".to_string()],
    )
    .await
    .unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        "/v1/secret/prod$vision_2$openai-prod.v1/drop"
    );
}

/// A table name is joined into the route the same way a Secret's is, so the
/// same two failures are reachable: a dot-only name leaves the table route
/// entirely, and a name holding the delimiter is indistinguishable from a
/// namespace boundary.
#[tokio::test]
async fn test_a_table_name_cannot_choose_its_own_route() {
    use std::sync::{Arc, Mutex};
    for name in ["..", ".", "../jobs", "a/b", "a$b"] {
        let reached = Arc::new(Mutex::new(false));
        let flag = reached.clone();
        let conn = Connection::new_with_handler(move |_| {
            *flag.lock().unwrap() = true;
            http::Response::builder().status(200).body("{}").unwrap()
        });
        let error = conn
            .drop_table(name, &[])
            .await
            .expect_err("an unaddressable table name must be refused");
        assert!(!*reached.lock().unwrap(), "{name:?} reached the transport");
        assert!(!error.to_string().is_empty(), "{name:?}");
    }
}

/// The namespace half of the same join.
#[tokio::test]
async fn test_a_namespace_segment_cannot_choose_its_own_route() {
    use std::sync::{Arc, Mutex};
    for segment in ["..", "a$b", ""] {
        let reached = Arc::new(Mutex::new(false));
        let flag = reached.clone();
        let conn = Connection::new_with_handler(move |_| {
            *flag.lock().unwrap() = true;
            http::Response::builder().status(200).body("{}").unwrap()
        });
        let error = conn
            .drop_table("t", &[segment.to_string()])
            .await
            .expect_err("an unaddressable namespace segment must be refused");
        assert!(
            !*reached.lock().unwrap(),
            "{segment:?} reached the transport"
        );
        assert!(!error.to_string().is_empty(), "{segment:?}");
    }
}

/// A segment outside the table charset still addresses one segment: the
/// service decides whether it may exist, and percent-encoding is what keeps
/// the question reaching the right route. A catalog database is named this
/// way.
#[tokio::test]
async fn test_a_namespace_segment_outside_the_charset_is_encoded_not_refused() {
    use std::sync::{Arc, Mutex};
    let seen = Arc::new(Mutex::new(String::new()));
    let path = seen.clone();
    let conn = Connection::new_with_handler(move |request| {
        *path.lock().unwrap() = request.url().path().to_string();
        http::Response::builder().status(200).body("{}").unwrap()
    });
    conn.drop_table("t", &["team/search".to_string()])
        .await
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), "/v1/table/team%2Fsearch$t/drop/");
}

/// A Function name is percent-encoded, which covers everything but the
/// relative segment: `..` is unreserved, so it survives encoding and is
/// then resolved away, posting a registration body to `/v1/create`.
#[tokio::test]
async fn test_a_relative_segment_function_name_is_refused() {
    use std::sync::{Arc, Mutex};
    for name in [".", "..", "%2E%2E"] {
        let reached = Arc::new(Mutex::new(false));
        let flag = reached.clone();
        let conn = Connection::new_with_handler(move |_| {
            *flag.lock().unwrap() = true;
            http::Response::builder().status(200).body("{}").unwrap()
        });
        let error = conn
            .drop_function(name, "fv_1", &[])
            .await
            .expect_err("a dot-only Function name must be refused");
        assert!(
            error.to_string().contains("relative path segments"),
            "{error}"
        );
        assert!(!*reached.lock().unwrap(), "{name:?} reached the transport");
    }
}

/// Only `.` and `..` are relative segments. `...` and longer runs are
/// ordinary and address perfectly well, so refusing them would make an
/// object that works today stop working on upgrade. This pins that.
#[tokio::test]
async fn test_a_longer_run_of_periods_is_an_ordinary_name() {
    use std::sync::{Arc, Mutex};
    for name in ["...", "....", "a.", ".a", "a..b"] {
        let seen = Arc::new(Mutex::new(String::new()));
        let captured = seen.clone();
        let conn = Connection::new_with_handler(move |request| {
            *captured.lock().unwrap() = request.url().path().to_string();
            http::Response::builder().status(200).body("{}").unwrap()
        });
        conn.drop_secret(name, &[])
            .await
            .unwrap_or_else(|error| panic!("{name:?} must remain addressable: {error}"));
        assert_eq!(
            *seen.lock().unwrap(),
            format!("/v1/secret/{name}/drop"),
            "{name:?} did not reach its own route"
        );
    }
}
