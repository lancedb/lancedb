// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
use clap::Parser;
use lancedb_cli::load::{run_load, LoadArgs};
use std::sync::Arc;

#[tokio::test]
async fn load_lance_into_local_database_and_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.lance");
    let destination = dir.path().join("db");
    let batch = RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int32Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    lance::Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
        source.to_str().unwrap(),
        None,
    )
    .await
    .unwrap();
    let args = || {
        LoadArgs::try_parse_from([
            "load",
            "--source",
            "lance",
            "--dataset",
            source.to_str().unwrap(),
            "--table",
            "events",
            "--destination",
            destination.to_str().unwrap(),
        ])
        .unwrap()
    };
    run_load(args()).await.unwrap();
    assert!(run_load(args())
        .await
        .unwrap_err()
        .to_string()
        .contains("Failed to load data"));
    let mut overwrite = args();
    overwrite.overwrite = true;
    run_load(overwrite).await.unwrap();
    let connection = lancedb::connect(destination.to_str().unwrap())
        .execute()
        .await
        .unwrap();
    assert_eq!(
        connection
            .open_table("events")
            .execute()
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap(),
        3
    );
}

#[test]
fn load_accepts_destination_and_http_server_url_alias() {
    let args = [
        "load",
        "--source",
        "lance",
        "--dataset",
        "source.lance",
        "--table",
        "events",
    ];
    let destination = LoadArgs::try_parse_from(
        args.into_iter()
            .chain(["--destination", "https://example.com"]),
    )
    .unwrap();
    let alias = LoadArgs::try_parse_from(
        args.into_iter()
            .chain(["--http-server-url", "https://example.com"]),
    )
    .unwrap();
    assert_eq!(
        destination.destination.as_deref(),
        Some("https://example.com")
    );
    assert_eq!(
        alias.connection.http_server_url.as_deref(),
        Some("https://example.com")
    );
    assert!(LoadArgs::try_parse_from(args.into_iter().chain([
        "--destination",
        "https://example.com",
        "--http-server-url",
        "https://example.com",
    ]))
    .is_err());
}
