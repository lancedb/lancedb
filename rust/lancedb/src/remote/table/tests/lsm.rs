// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[tokio::test]
async fn test_prewarm_index() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(
            request.url().path(),
            "/v1/table/my_table/index/my_index/prewarm/"
        );
        http::Response::builder().status(200).body("{}").unwrap()
    });
    table.prewarm_index("my_index").await.unwrap();
}

#[tokio::test]
async fn test_prewarm_index_not_found() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(
            request.url().path(),
            "/v1/table/my_table/index/my_index/prewarm/"
        );
        http::Response::builder().status(404).body("{}").unwrap()
    });
    let e = table.prewarm_index("my_index").await.unwrap_err();
    assert!(matches!(e, Error::IndexNotFound { .. }));
}

#[tokio::test]
async fn test_prewarm_data() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(
            request.url().path(),
            "/v1/table/my_table/page_cache/prewarm/"
        );
        http::Response::builder().status(200).body("{}").unwrap()
    });
    table.prewarm_data(None).await.unwrap();
}

#[tokio::test]
async fn test_prewarm_data_with_columns() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(
            request.url().path(),
            "/v1/table/my_table/page_cache/prewarm/"
        );
        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(body["columns"], serde_json::json!(["col_a", "col_b"]));
        http::Response::builder().status(200).body("{}").unwrap()
    });
    table
        .prewarm_data(Some(vec!["col_a".into(), "col_b".into()]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_drop_index() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(
            request.url().path(),
            "/v1/table/my_table/index/my_index/drop/"
        );
        http::Response::builder().status(200).body("{}").unwrap()
    });
    table.drop_index("my_index").await.unwrap();
}

#[tokio::test]
async fn test_drop_index_not_exists() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(
            request.url().path(),
            "/v1/table/my_table/index/my_index/drop/"
        );
        http::Response::builder().status(404).body("{}").unwrap()
    });

    // Assert that the error is IndexNotFound
    let e = table.drop_index("my_index").await.unwrap_err();
    assert!(matches!(e, Error::IndexNotFound { .. }));
}

/// Index names are unvalidated, so reserved characters must be
/// percent-encoded or they restructure the request path.
#[tokio::test]
async fn test_per_index_paths_encode_reserved_characters() {
    const NAME: &str = "my/index?a#b c";
    const PREFIX: &str = "/v1/table/my_table/index/my%2Findex%3Fa%23b%20c";

    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.url().path(), format!("{PREFIX}/stats/"));
        let body = serde_json::json!({
          "num_indexed_rows": 1,
          "num_unindexed_rows": 0,
          "index_type": "IVF_PQ",
          "distance_type": "l2"
        });
        http::Response::builder()
            .status(200)
            .body(serde_json::to_string(&body).unwrap())
            .unwrap()
    });
    assert!(table.index_stats(NAME).await.unwrap().is_some());

    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.url().path(), format!("{PREFIX}/drop/"));
        http::Response::builder().status(200).body("{}").unwrap()
    });
    table.drop_index(NAME).await.unwrap();

    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.url().path(), format!("{PREFIX}/prewarm/"));
        http::Response::builder().status(200).body("{}").unwrap()
    });
    table.prewarm_index(NAME).await.unwrap();
}

#[tokio::test]
async fn test_set_lsm_write_spec_unsharded() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(
            request.url().path(),
            "/v1/table/my_table/set_lsm_write_spec/"
        );
        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(body["sharding"], serde_json::json!({ "mode": "unsharded" }));
        assert_eq!(body["maintained_indexes"], serde_json::json!(["id_idx"]));
        assert_eq!(
            body["writer_config_defaults"],
            serde_json::json!({ "max_memtable_rows": "1000" })
        );
        http::Response::builder()
            .status(200)
            .body(r#"{"maintained_indexes":["id_idx"]}"#)
            .unwrap()
    });
    let spec = crate::table::LsmWriteSpec::unsharded()
        .with_maintained_indexes(vec!["id_idx".to_string()])
        .with_writer_config_defaults([("max_memtable_rows", "1000")]);
    table.set_lsm_write_spec(spec).await.unwrap();
}

#[tokio::test]
async fn test_set_lsm_write_spec_bucket() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(
            request.url().path(),
            "/v1/table/my_table/set_lsm_write_spec/"
        );
        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(
            body["sharding"],
            serde_json::json!({ "mode": "bucket", "column": "id", "num_buckets": 16 })
        );
        // An unpinned maintained set sends null: resolve server-side.
        assert_eq!(body["maintained_indexes"], serde_json::Value::Null);
        http::Response::builder().status(200).body("{}").unwrap()
    });
    table
        .set_lsm_write_spec(crate::table::LsmWriteSpec::bucket("id", 16))
        .await
        .unwrap();
}

/// `[]` (none) must stay distinguishable on the wire from null (all).
#[tokio::test]
async fn test_set_lsm_write_spec_no_maintained_indexes() {
    let table = Table::new_with_handler("my_table", |request| {
        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(body["maintained_indexes"], serde_json::json!([]));
        http::Response::builder().status(200).body("{}").unwrap()
    });
    table
        .set_lsm_write_spec(
            crate::table::LsmWriteSpec::bucket("id", 16).with_maintained_indexes(Vec::new()),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_set_lsm_write_spec_identity() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(
            request.url().path(),
            "/v1/table/my_table/set_lsm_write_spec/"
        );
        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(
            body["sharding"],
            serde_json::json!({ "mode": "identity", "column": "tenant" })
        );
        http::Response::builder().status(200).body("{}").unwrap()
    });
    table
        .set_lsm_write_spec(crate::table::LsmWriteSpec::identity("tenant"))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_unset_lsm_write_spec() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(
            request.url().path(),
            "/v1/table/my_table/unset_lsm_write_spec/"
        );
        http::Response::builder().status(200).body("{}").unwrap()
    });
    table.unset_lsm_write_spec().await.unwrap();
}

#[tokio::test]
async fn test_get_lsm_write_spec() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(
            request.url().path(),
            "/v1/table/my_table/get_lsm_write_spec/"
        );

        // The server resolves the spec and re-encodes it into the same
        // sophon-owned shape the set endpoint accepts (`Sharding` internally
        // tagged on `mode`, wrapped in `lsm_write_spec`).
        let response = serde_json::json!({
            "lsm_write_spec": {
                "sharding": { "mode": "bucket", "column": "id", "num_buckets": 4 },
                "maintained_indexes": ["id_idx"],
                "writer_config_defaults": { "durable_write": "false" },
            }
        });
        http::Response::builder()
            .status(200)
            .body(response.to_string())
            .unwrap()
    });

    let spec = table
        .get_lsm_write_spec()
        .await
        .unwrap()
        .expect("a spec should be reported");
    match spec {
        crate::table::LsmWriteSpec::Bucket {
            column,
            num_buckets,
            maintained_indexes,
            writer_config_defaults,
        } => {
            assert_eq!(column, "id");
            assert_eq!(num_buckets, 4);
            assert_eq!(maintained_indexes, Some(vec!["id_idx".to_string()]));
            assert_eq!(
                writer_config_defaults
                    .get("durable_write")
                    .map(String::as_str),
                Some("false")
            );
        }
        other => panic!("expected a bucket spec, got {:?}", other),
    }
}

/// Every selection reads back as the server reported it: every index
/// (`null`), none (`[]`), and a named list.
#[rstest::rstest]
#[case::every_index(serde_json::Value::Null, None)]
#[case::no_index(serde_json::json!([]), Some(vec![]))]
#[case::named(serde_json::json!(["id_idx"]), Some(vec!["id_idx".to_string()]))]
#[tokio::test]
async fn test_get_lsm_write_spec_round_trips_the_selection(
    #[case] reported: serde_json::Value,
    #[case] expected: Option<Vec<String>>,
) {
    let table = Table::new_with_handler("my_table", move |_| {
        let response = serde_json::json!({
            "lsm_write_spec": {
                "sharding": { "mode": "unsharded" },
                "maintained_indexes": reported,
                "writer_config_defaults": {},
            }
        });
        http::Response::builder()
            .status(200)
            .body(response.to_string())
            .unwrap()
    });

    let spec = table
        .get_lsm_write_spec()
        .await
        .unwrap()
        .expect("a spec should be reported");
    assert_eq!(
        spec.maintained_indexes().map(<[String]>::to_vec),
        expected,
        "the selection the server reported must survive the read"
    );
}

#[tokio::test]
async fn test_get_lsm_write_spec_absent() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(
            request.url().path(),
            "/v1/table/my_table/get_lsm_write_spec/"
        );
        // Null spec → the LSM write path is not enabled.
        let response = serde_json::json!({ "lsm_write_spec": null });
        http::Response::builder()
            .status(200)
            .body(response.to_string())
            .unwrap()
    });
    assert!(table.get_lsm_write_spec().await.unwrap().is_none());
}

/// `flush_lsm` / `compact_lsm` answer 202 with no body at all.
fn accepted() -> http::Response<String> {
    http::Response::builder()
        .status(202)
        .body(String::new())
        .unwrap()
}

fn ok_json(body: String) -> http::Response<String> {
    http::Response::builder().status(200).body(body).unwrap()
}

/// A flush landing in an empty L0 finishes on the opening stats read
/// alone. Asserting zero compacts is the point: "it returned Ok" is also
/// true of a loop that ran a pointless pass.
#[tokio::test(start_paused = true)]
async fn test_checkpoint_short_circuits_on_empty_l0() {
    let compacts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = compacts.clone();
    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path().to_string();
        if path.contains("compact_lsm") {
            seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            panic!("an already-converged table must issue no compact calls");
        }
        if path.contains("flush_lsm") {
            return accepted();
        }
        assert_eq!(path, "/v1/table/my_table/get_lsm_stats/");
        ok_json(stats_body(&[], false))
    });

    table.checkpoint_lsm().await.unwrap();
    assert_eq!(compacts.load(std::sync::atomic::Ordering::SeqCst), 0);
}

/// The loop triggers compaction until every generation that existed at
/// the start is gone, one bounded prefix per pass.
#[tokio::test(start_paused = true)]
async fn test_checkpoint_triggers_until_targets_are_drained() {
    let compacts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = compacts.clone();
    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path().to_string();
        if path.contains("flush_lsm") {
            return accepted();
        }
        if path.contains("compact_lsm") {
            seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return accepted();
        }
        // Each pass drains the oldest generation.
        let drained = seen.load(std::sync::atomic::Ordering::SeqCst);
        let left: Vec<u64> = [1u64, 2, 3].into_iter().skip(drained).collect();
        ok_json(stats_body(&left, false))
    });

    table.checkpoint_lsm().await.unwrap();
    assert_eq!(
        compacts.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "one trigger per generation prefix, then stop"
    );
}

/// Generations created *during* the checkpoint are not waited on, which
/// is what lets the loop terminate on a table taking writes where "L0 is
/// empty" never becomes true.
#[tokio::test(start_paused = true)]
async fn test_checkpoint_ignores_generations_created_while_it_runs() {
    let compacts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = compacts.clone();
    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path().to_string();
        if path.contains("flush_lsm") {
            return accepted();
        }
        if path.contains("compact_lsm") {
            seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return accepted();
        }
        // Target is 5. One pass drains it; a writer keeps adding above.
        let n = seen.load(std::sync::atomic::Ordering::SeqCst);
        let body = if n == 0 {
            stats_body(&[5], false)
        } else {
            stats_body(&[6, 7], false)
        };
        ok_json(body)
    });

    table.checkpoint_lsm().await.unwrap();
    assert_eq!(
        compacts.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the loop must not chase generations written after it started"
    );
}

/// Contention is a 429 and must be retried. The server keeps it off 503
/// precisely so the client can act on the status alone — reading it as
/// terminal stops the checkpoint early on a healthy node.
#[tokio::test(start_paused = true)]
async fn test_checkpoint_retries_contention() {
    let compacts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = compacts.clone();
    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path().to_string();
        if path.contains("flush_lsm") {
            return accepted();
        }
        if path.contains("compact_lsm") {
            // First two triggers: every bucket already latched.
            if seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 2 {
                return http::Response::builder()
                    .status(429)
                    .body(r#"{"code":21,"error":"Too many concurrent writes"}"#.to_string())
                    .unwrap();
            }
            return accepted();
        }
        let accepted_triggers = seen
            .load(std::sync::atomic::Ordering::SeqCst)
            .saturating_sub(2);
        let left: Vec<u64> = if accepted_triggers == 0 {
            vec![1]
        } else {
            vec![]
        };
        ok_json(stats_body(&left, false))
    });

    table
        .checkpoint_lsm()
        .await
        .expect("contention must not abort the checkpoint");
    assert_eq!(
        compacts.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "assert the retry count, not just the outcome"
    );
}

/// A transient fault on the poll must not abort the checkpoint. This route
/// meets the most contention — it runs every `POLL_INTERVAL` for the
/// checkpoint's whole life, with the transport retry layer disabled — yet
/// was the one call reached with a bare `?`.
#[tokio::test(start_paused = true)]
async fn test_checkpoint_retries_a_contended_stats_poll() {
    let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = polls.clone();
    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path().to_string();
        if path.contains("flush_lsm") || path.contains("compact_lsm") {
            return accepted();
        }
        // The opening read lands; the next two polls are latched out.
        let n = seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if (1..3).contains(&n) {
            return http::Response::builder()
                .status(429)
                .body(r#"{"code":21,"error":"Too many concurrent writes"}"#.to_string())
                .unwrap();
        }
        ok_json(stats_body(if n < 4 { &[1] } else { &[] }, false))
    });

    table
        .checkpoint_lsm()
        .await
        .expect("a contended poll must be retried, not surfaced");
    assert_eq!(
        polls.load(std::sync::atomic::Ordering::SeqCst),
        5,
        "the two rejected polls must be re-issued, not skipped"
    );
}

/// Contention and a lost claim draw on separate budgets: five straight
/// 429s on `flush`, more than `MAX_REISSUES`, must still converge. On one
/// shared counter this spent the re-issue cap and then reported a lost
/// claim nothing had ever reported.
#[tokio::test(start_paused = true)]
async fn test_contention_does_not_exhaust_the_reissue_budget() {
    let flushes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = flushes.clone();
    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path().to_string();
        if path.contains("flush_lsm") {
            if seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 5 {
                return http::Response::builder()
                    .status(429)
                    .body(r#"{"code":21,"error":"Too many concurrent writes"}"#.to_string())
                    .unwrap();
            }
            return accepted();
        }
        if path.contains("compact_lsm") {
            return accepted();
        }
        ok_json(stats_body(&[], false))
    });

    table
        .checkpoint_lsm()
        .await
        .expect("contention must not be reported as a lost claim");
    assert_eq!(
        flushes.load(std::sync::atomic::Ordering::SeqCst),
        6,
        "five retries against one seal, then it lands"
    );
}

/// An exhausted retry budget surfaces the fault that consumed it, not a
/// message the loop invented: "429, nine times" points an operator at a
/// saturated pool, a generic runtime error points them nowhere.
#[tokio::test(start_paused = true)]
async fn test_exhausted_retries_surface_the_underlying_fault() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = calls.clone();
    let table = Table::new_with_handler("my_table", move |_request| {
        seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        http::Response::builder()
            .status(429)
            .body(r#"{"code":21,"error":"Too many concurrent writes"}"#.to_string())
            .unwrap()
    });

    let err = table.checkpoint_lsm().await.unwrap_err();
    assert!(
        matches!(&err, Error::Http { status_code: Some(s), .. } if s.as_u16() == 429),
        "the fault that spent the budget must be the one reported: {err:?}"
    );
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        9,
        "one call plus MAX_RETRIES — the re-issue budget is not spent on top"
    );
}

/// A draining node is terminal, but the client does not know that from the
/// status: draining and a proxy blip are both 503, and telling them apart
/// takes parsing the body for a namespace code. So it spends the retry
/// budget and then reports what the server said — the drain gate never
/// releases, so the answer does not change, and the operator still reads
/// "WAL node draining" in the error.
#[tokio::test(start_paused = true)]
async fn test_draining_surfaces_after_the_retry_budget() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = calls.clone();
    let table = Table::new_with_handler("my_table", move |_request| {
        seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        http::Response::builder()
            .status(503)
            .body(r#"{"code":19,"error":"WAL node draining"}"#.to_string())
            .unwrap()
    });

    let err = table.checkpoint_lsm().await.unwrap_err();
    let message = err.to_string();
    assert!(
        matches!(&err, Error::Http { status_code: Some(s), .. } if s.as_u16() == 503),
        "the 503 must surface as itself: {err:?}"
    );
    assert!(
        message.contains("WAL node draining"),
        "the server's own diagnosis must survive to the caller: {message}"
    );
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        9,
        "one call plus MAX_RETRIES, then it reports rather than spinning"
    );
}

/// A long stall with nothing compacting must keep waiting, not fail. The
/// client cannot judge this: a checkpoint queued behind unrelated tables
/// on the pod-wide compactor pool reports exactly these numbers — flat
/// generations, an idle latch — as one whose merges are failing. The
/// deadline is the caller's.
#[tokio::test(start_paused = true)]
async fn test_checkpoint_waits_out_a_long_stall_rather_than_failing() {
    let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = polls.clone();
    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path().to_string();
        if path.contains("flush_lsm") || path.contains("compact_lsm") {
            return accepted();
        }
        // Flat for far longer than any bound this loop ever had, with
        // `compacting: false` throughout — then it drains.
        let n = seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        ok_json(stats_body(if n < 40 { &[1, 2] } else { &[] }, false))
    });

    table
        .checkpoint_lsm()
        .await
        .expect("a stall is the server being slow, not the client's call to make");
    assert!(
        polls.load(std::sync::atomic::Ordering::SeqCst) > 40,
        "the loop must have kept polling well past the old ten-poll bound"
    );
}

/// A pass already owns the latch on every outstanding bucket, so the loop
/// waits rather than piling on triggers it would only refuse. This is the
/// sole thing `compacting` is read for.
#[tokio::test(start_paused = true)]
async fn test_checkpoint_waits_while_a_pass_is_running() {
    let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let compacts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen_polls = polls.clone();
    let seen_compacts = compacts.clone();
    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path().to_string();
        if path.contains("flush_lsm") {
            return accepted();
        }
        if path.contains("compact_lsm") {
            seen_compacts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return accepted();
        }
        // Latched for many polls, then done.
        let n = seen_polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        ok_json(if n > 15 {
            stats_body(&[], false)
        } else {
            stats_body(&[1], true)
        })
    });

    table
        .checkpoint_lsm()
        .await
        .expect("a running pass is progress, not a stall");
    assert_eq!(
        compacts.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "never trigger against a bucket already compacting"
    );
}

/// WAL off ⇒ `None`; WAL on ⇒ a fully populated `Some` with no field
/// defaulting to a zero it did not measure. `include_generation_rows`
/// rides in the body and is off unless asked for.
#[tokio::test]
async fn test_get_lsm_stats_round_trip() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.url().path(), "/v1/table/my_table/get_lsm_stats/");
        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(
            body["include_generation_rows"], true,
            "the flag must reach the server, not be silently dropped"
        );
        let response = serde_json::json!({
            "lsm_stats": {
                "buckets": [{
                    "shard_id": "b0",
                    "status": "Active",
                    "writer_epoch": 3,
                    "manifest_version": 11,
                    "current_generation": 9,
                    "replay_after_wal_entry_position": 100,
                    "wal_entry_position_last_seen": 140,
                    "generations": [{ "generation": 8, "bytes": 4096, "rows": 30 }],
                    "compacting": false,
                    "memtables": [
                        { "generation": 9, "rows": 12, "bytes": 900, "batches": 2,
                          "indexes": ["vec_idx"] }
                    ],
                }],
            }
        });
        http::Response::builder()
            .status(200)
            .body(response.to_string())
            .unwrap()
    });

    let stats = table
        .get_lsm_stats(true)
        .await
        .unwrap()
        .expect("a WAL-backed table reports Some");
    let bucket = &stats.buckets[0];
    assert_eq!(bucket.replay_after_wal_entry_position, 100);
    assert_eq!(bucket.wal_entry_position_last_seen, 140);
    assert!(!bucket.compacting);
    assert_eq!(bucket.generations[0].generation, 8);
    assert_eq!(bucket.generations[0].rows, Some(30));
    // The line that answers "why is my fresh-tier vector search
    // brute-force" — an absent index name is the whole explanation.
    let memtables = bucket.memtables.as_ref().unwrap();
    assert_eq!(memtables[0].indexes, vec!["vec_idx".to_string()]);
}

/// A 404 arrives as `TableNotFound`, not as a lost claim the loop
/// re-issues from flush until its cap. The two are distinguished by
/// status: 404 is "no such table", 421 is "this node holds no claim".
/// They shared 404 once, and the loop chased a name that never existed.
#[tokio::test(start_paused = true)]
async fn test_missing_table_is_not_read_as_a_lost_claim() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = calls.clone();
    let table = Table::new_with_handler("my_table", move |_request| {
        seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        http::Response::builder()
            .status(404)
            .body(r#"{"code":4,"error":"Not found: Table not found: my_table"}"#.to_string())
            .unwrap()
    });

    let err = table.checkpoint_lsm().await.unwrap_err();
    assert!(
        matches!(err, Error::TableNotFound { .. }),
        "a missing table must say so: {err:?}"
    );
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "no point re-claiming a table that does not exist"
    );
}

/// A lost claim — 421, not 404 — does re-issue from flush, the call that
/// re-claims and replays.
#[tokio::test(start_paused = true)]
async fn test_registry_miss_reissues_from_flush() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = calls.clone();
    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path().to_string();
        let n = seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if path.contains("flush_lsm") {
            // First flush lands; the claim is then lost, and the
            // re-issued flush succeeds.
            return accepted();
        }
        if path.contains("compact_lsm") {
            if n < 4 {
                return http::Response::builder()
                    .status(421)
                    .body(r#"{"code":19,"error":"table not claimed"}"#.to_string())
                    .unwrap();
            }
            return accepted();
        }
        ok_json(stats_body(if n < 6 { &[1] } else { &[] }, false))
    });

    table
        .checkpoint_lsm()
        .await
        .expect("a lost claim must be recovered by re-flushing, not surfaced");
}

#[tokio::test]
async fn test_get_lsm_stats_absent_when_wal_off() {
    let table = Table::new_with_handler("my_table", |_request| {
        http::Response::builder()
            .status(200)
            .body(serde_json::json!({ "lsm_stats": null }).to_string())
            .unwrap()
    });
    assert!(table.get_lsm_stats(false).await.unwrap().is_none());
}

#[tokio::test]
async fn test_wait_for_index() {
    let table = _make_table_with_indices(0);
    table
        .wait_for_index(&["vector_idx", "my_idx"], Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_wait_for_index_timeout() {
    let table = _make_table_with_indices(100);
    let e = table
        .wait_for_index(&["vector_idx", "my_idx"], Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(
        e.to_string(),
        "Timeout error: timed out waiting for indices: [\"vector_idx\", \"my_idx\"] after 1s"
    );
}

#[tokio::test]
async fn test_wait_for_index_timeout_never_created() {
    let table = _make_table_with_indices(0);
    let e = table
        .wait_for_index(&["doesnt_exist_idx"], Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(
        e.to_string(),
        "Timeout error: timed out waiting for indices: [\"doesnt_exist_idx\"] after 1s"
    );
}

fn _make_table_with_indices(unindexed_rows: usize) -> Table {
    Table::new_with_handler("my_table", move |request| {
        assert_eq!(request.method(), "POST");

        let response_body = match request.url().path() {
            "/v1/table/my_table/describe/" => {
                let schema = Schema::new(vec![
                    Field::new(
                        "vector",
                        DataType::FixedSizeList(
                            Arc::new(Field::new("item", DataType::Float32, true)),
                            8,
                        ),
                        false,
                    ),
                    Field::new("my_column", DataType::Utf8, false),
                ]);
                serde_json::from_str::<serde_json::Value>(&describe_response(&schema)).unwrap()
            }
            "/v1/table/my_table/index/list/" => {
                serde_json::json!({
                    "indexes": [
                        {
                            "index_name": "vector_idx",
                            "index_uuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                            "columns": ["vector"],
                            "index_status": "done",
                        },
                        {
                            "index_name": "my_idx",
                            "index_uuid": "34255f64-5717-4562-b3fc-2c963f66afa6",
                            "columns": ["my_column"],
                            "index_status": "done",
                        },
                    ]
                })
            }
            "/v1/table/my_table/index/vector_idx/stats/" => {
                serde_json::json!({
                    "num_indexed_rows": 100000,
                    "num_unindexed_rows": unindexed_rows,
                    "index_type": "IVF_PQ",
                    "distance_type": "l2"
                })
            }
            "/v1/table/my_table/index/my_idx/stats/" => {
                serde_json::json!({
                    "num_indexed_rows": 100000,
                    "num_unindexed_rows": unindexed_rows,
                    "index_type": "LABEL_LIST"
                })
            }
            _path => {
                serde_json::json!(None::<String>)
            }
        };
        let body = serde_json::to_string(&response_body).unwrap();
        let status = if body == "null" { 404 } else { 200 };
        http::Response::builder().status(status).body(body).unwrap()
    })
}
