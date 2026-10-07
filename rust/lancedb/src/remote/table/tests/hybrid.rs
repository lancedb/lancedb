// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

// WAL-PK-FUSION: delete everything from here to the end of `mod tests`.

/// Verbatim from sophon's `LSM_WITH_ROW_ID_UNSUPPORTED`; the fallback keys
/// off this sentence, so a test that paraphrased it would prove nothing.
const WAL_ROW_ID_REFUSAL_BODY: &str = r#"{"code":13,"error":"Bad request: the MemWAL LSM scanner does not support with_row_id (the LSM scanner exposes _rowaddr, not a stable _rowid); set use_lsm(false) to read the base table only (results will exclude un-compacted MemWAL data)"}"#;

fn pk_schema() -> Schema {
    use lance_core::datatypes::LANCE_UNENFORCED_PRIMARY_KEY_POSITION;
    Schema::new(vec![
        Field::new("id", DataType::Utf8, false).with_metadata(
            [(
                LANCE_UNENFORCED_PRIMARY_KEY_POSITION.to_string(),
                "1".to_string(),
            )]
            .into_iter()
            .collect(),
        ),
        Field::new("text", DataType::Utf8, true),
    ])
}

/// A server that refuses `_rowid` the way a MemWAL table does, recording
/// every query body so a test can see which key each attempt asked for.
fn wal_refusing_table(bodies: Arc<std::sync::Mutex<Vec<serde_json::Value>>>) -> Table {
    Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/describe/" => http::Response::builder()
            .status(200)
            .body(describe_response(&pk_schema()).into_bytes())
            .unwrap(),
        "/v1/table/my_table/query/" => {
            let body = request_body_json(&request);
            let asked_for_row_id = body["with_row_id"] == serde_json::Value::Bool(true);
            let is_fts = body.get("full_text_query").is_some_and(|v| !v.is_null());
            bodies.lock().unwrap().push(body);
            if asked_for_row_id {
                return http::Response::builder()
                    .status(400)
                    .body(WAL_ROW_ID_REFUSAL_BODY.as_bytes().to_vec())
                    .unwrap();
            }
            let batch = if is_fts {
                leg("_score", vec!["a", "b"], false)
            } else {
                leg("_distance", vec!["a"], false)
            };
            http::Response::builder()
                .status(200)
                .body(write_ipc_file(&batch))
                .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    })
}

fn hybrid(table: &Table) -> crate::query::VectorQuery {
    table
        .query()
        .full_text_search(FullTextSearchQuery::new("a".to_string()))
        .nearest_to(&[0.0, 0.0])
        .unwrap()
}

/// The optimistic path: ask for `_rowid`, and when a MemWAL table refuses,
/// fall back to the primary key rather than failing.
#[tokio::test]
async fn test_hybrid_falls_back_to_the_primary_key_when_row_id_is_refused() {
    let bodies = Arc::new(std::sync::Mutex::new(Vec::new()));
    let table = wal_refusing_table(bodies.clone());

    let results = hybrid(&table)
        .select(Select::columns(&["text"]))
        .limit(10)
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let bodies = bodies.lock().unwrap();
    let (asked_row_id, asked_pk): (Vec<_>, Vec<_>) = bodies
        .iter()
        .partition(|b| b["with_row_id"] == serde_json::Value::Bool(true));
    // The legs run concurrently under `try_join!`, which abandons the
    // sibling as soon as one errors — so the refused attempt is one or two
    // requests, never zero, and the retry always sends both.
    assert!(
        !asked_row_id.is_empty(),
        "`_rowid` has to be tried before the fallback means anything"
    );
    assert_eq!(asked_pk.len(), 2, "both legs are retried on the key");
    for body in asked_pk {
        let columns = body["columns"].as_array().expect("a column projection");
        assert!(
            columns.iter().any(|c| c == "id"),
            "the retry has to project the key: {body}"
        );
    }

    let batch = &results[0];
    assert!(batch.column_by_name(ROW_ID).is_none());
    assert!(batch.column_by_name("id").is_none(), "injected key leaked");
    assert_eq!(batch.num_rows(), 2, "`a` is in both legs and fuses");
}

/// A hybrid query that matches nothing must return nothing, not fail.
/// When both legs come back empty, `query_schemas` synthesizes a schema
/// that already carries `_rowid`, which the stamping has to tolerate.
#[tokio::test]
async fn test_hybrid_with_no_matches_returns_an_empty_result() {
    let table = Table::new_with_handler("my_table", move |request| {
        match request.url().path() {
            "/v1/table/my_table/describe/" => http::Response::builder()
                .status(200)
                .body(describe_response(&pk_schema()).into_bytes())
                .unwrap(),
            "/v1/table/my_table/query/" => {
                let body = request_body_json(&request);
                if body["with_row_id"] == serde_json::Value::Bool(true) {
                    return http::Response::builder()
                        .status(400)
                        .body(WAL_ROW_ID_REFUSAL_BODY.as_bytes().to_vec())
                        .unwrap();
                }
                // A leg that matched nothing: schema, no batches.
                let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
                let mut body = Vec::new();
                {
                    let mut writer =
                        arrow_ipc::writer::FileWriter::try_new(&mut body, &schema).unwrap();
                    writer.finish().unwrap();
                }
                http::Response::builder().status(200).body(body).unwrap()
            }
            path => panic!("unexpected request path: {path}"),
        }
    });

    let results = hybrid(&table)
        .limit(10)
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(results.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

/// The refusal is paid once: the second query goes straight to the key.
#[tokio::test]
async fn test_hybrid_remembers_the_refusal() {
    let bodies = Arc::new(std::sync::Mutex::new(Vec::new()));
    let table = wal_refusing_table(bodies.clone());

    for _ in 0..2 {
        hybrid(&table)
            .limit(10)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
    }

    let refused = bodies
        .lock()
        .unwrap()
        .iter()
        .filter(|b| b["with_row_id"] == serde_json::Value::Bool(true))
        .count();
    assert!(
        (1..=2).contains(&refused),
        "only the first query should pay the refusal, saw {refused} attempts"
    );
}

/// A table that never refuses must never pay for the fallback existing.
#[tokio::test]
async fn test_hybrid_on_a_base_table_sends_no_extra_requests() {
    let paths = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = paths.clone();
    let table = Table::new_with_handler("my_table", move |request| {
        seen.lock().unwrap().push(request.url().path().to_string());
        let batch = if request_body_json(&request)
            .get("full_text_query")
            .is_some_and(|v| !v.is_null())
        {
            leg("_score", vec!["a", "b"], true)
        } else {
            leg("_distance", vec!["a"], true)
        };
        http::Response::builder()
            .status(200)
            .body(write_ipc_file(&batch))
            .unwrap()
    });

    hybrid(&table)
        .limit(10)
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let paths = paths.lock().unwrap();
    assert!(
        paths.iter().all(|p| p.ends_with("/query/")),
        "a table that never refuses must not be probed, got {paths:?}"
    );
    assert_eq!(paths.len(), 2, "one request per leg, got {paths:?}");
}

/// A caller who asked for `_rowid` gets the server's refusal, not a
/// surrogate dressed up as one.
#[tokio::test]
async fn test_hybrid_does_not_fall_back_when_the_caller_asked_for_row_id() {
    let bodies = Arc::new(std::sync::Mutex::new(Vec::new()));
    let table = wal_refusing_table(bodies.clone());

    let err = hybrid(&table)
        .with_row_id()
        .limit(10)
        .execute()
        .await
        .err()
        .expect("the refusal must reach the caller");

    // Translated, not passed through: the caller gets the reason and the
    // way out, which a bare 400 does not carry.
    assert!(matches!(err, Error::NotSupported { .. }), "{err:?}");
    assert!(err.to_string().contains("use_lsm(false)"), "{err}");
    assert!(
        bodies
            .lock()
            .unwrap()
            .iter()
            .all(|b| b["with_row_id"] == serde_json::Value::Bool(true)),
        "no retry should have been attempted"
    );
}
