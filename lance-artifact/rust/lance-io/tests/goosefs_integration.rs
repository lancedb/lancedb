// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! GooseFS integration tests via OpenDAL.
//!
//! Covers Stage 2 (OpenDAL direct), Stage 3 (Lance ObjectStore I/O),
//! and diagnostic tests (OpenDAL via lance-io ObjectStore).
//!
//! Run:
//!   cargo test -p lance-io --features "goosefs goosefs-test" --test goosefs_integration -- --ignored --nocapture --test-threads=1
#![cfg(feature = "goosefs-test")]
#![allow(clippy::print_stderr)]

use std::sync::Arc;

use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use opendal::{Operator, services::GooseFs};
use std::collections::HashMap;

fn get_operator() -> Operator {
    let addr = std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or("127.0.0.1:9200".into());
    let auth_type = std::env::var("GOOSEFS_AUTH_TYPE").unwrap_or("simple".into());
    let mut cfg = HashMap::new();
    cfg.insert("master_addr".to_string(), addr);
    cfg.insert("root".to_string(), "/lance-test/opendal".to_string());
    cfg.insert("auth_type".to_string(), auth_type);
    Operator::from_iter::<GooseFs>(cfg).unwrap()
}

// ============================================================
// Stage 2: OpenDAL GooseFs Service tests
// ============================================================

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_opendal_write_read() {
    let op = get_operator();
    // Cleanup any leftover from previous runs
    let _ = op.delete("hello.txt").await;
    op.write("hello.txt", "Hello from OpenDAL").await.unwrap();
    let data = op.read("hello.txt").await.unwrap();
    assert_eq!(data.to_vec(), b"Hello from OpenDAL");
    op.delete("hello.txt").await.unwrap();
}

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_opendal_list() {
    let op = get_operator();
    // Write files directly (GooseFS may have h2 issues with newly-created subdirs)
    let _ = op.delete("list_a.txt").await;
    let _ = op.delete("list_b.txt").await;
    op.write("list_a.txt", "aaa").await.unwrap();
    op.write("list_b.txt", "bbb").await.unwrap();
    let entries: Vec<_> = op.list("/").await.unwrap();
    let names: Vec<String> = entries.iter().map(|e| e.name().to_string()).collect();
    eprintln!("Listed entries: {:?}", names);
    assert!(
        entries.len() >= 2,
        "Expected at least 2 entries, got {}",
        entries.len()
    );
    op.delete("list_a.txt").await.unwrap();
    op.delete("list_b.txt").await.unwrap();
}

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_opendal_stat() {
    let op = get_operator();
    // Cleanup leftover from previous runs
    let _ = op.delete("stat_test.txt").await;
    op.write("stat_test.txt", "12345").await.unwrap();
    let meta = op.stat("stat_test.txt").await.unwrap();
    assert_eq!(meta.content_length(), 5);
    op.delete("stat_test.txt").await.unwrap();
}

// ============================================================
// Stage 3: Lance ObjectStore I/O tests
// ============================================================

use lance_io::object_store::ObjectStore;

async fn get_lance_store() -> Arc<ObjectStore> {
    let addr = std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or("127.0.0.1:9200".into());
    let uri = format!("goosefs://{}/lance-test/lance-io", addr);
    ObjectStore::from_uri(&uri).await.unwrap().0
}

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_objectstore_put_get() {
    let store = get_lance_store().await;
    let path = object_store::path::Path::from("test_put_get.bin");

    // Cleanup
    let _ = store.inner.delete(&path).await;

    // Write
    store
        .inner
        .put(&path, (&b"lance-goosefs-test"[..]).into())
        .await
        .unwrap();

    // Read
    let result = store.inner.get(&path).await.unwrap();
    let bytes = result.bytes().await.unwrap();
    assert_eq!(&bytes[..], b"lance-goosefs-test");

    // Cleanup
    store.inner.delete(&path).await.unwrap();
}

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_objectstore_list() {
    let store = get_lance_store().await;

    let file_a = object_store::path::Path::from("list_a.bin");
    let file_b = object_store::path::Path::from("list_b.bin");

    // Cleanup leftovers
    let _ = store.inner.delete(&file_a).await;
    let _ = store.inner.delete(&file_b).await;

    store
        .inner
        .put(&file_a, (&b"aaa"[..]).into())
        .await
        .unwrap();
    store
        .inner
        .put(&file_b, (&b"bbb"[..]).into())
        .await
        .unwrap();

    let entries: Vec<_> = store.inner.list(None).try_collect().await.unwrap();
    eprintln!("Listed {} entries", entries.len());
    assert!(
        entries.len() >= 2,
        "Expected at least 2 entries, got {}",
        entries.len()
    );

    store.inner.delete(&file_a).await.unwrap();
    store.inner.delete(&file_b).await.unwrap();
}

#[ignore = "Requires GooseFS cluster"]
#[tokio::test]
async fn test_lance_objectstore_large_file() {
    let store = get_lance_store().await;
    let path = object_store::path::Path::from("large_file.bin");
    let _ = store.inner.delete(&path).await;

    // Write 5MB file
    let data = vec![42u8; 5 * 1024 * 1024];
    store.inner.put(&path, data.clone().into()).await.unwrap();

    let result = store.inner.get(&path).await.unwrap();
    let bytes = result.bytes().await.unwrap();
    assert_eq!(bytes.len(), 5 * 1024 * 1024);
    assert_eq!(&bytes[..10], &[42u8; 10]);

    store.inner.delete(&path).await.unwrap();
}

// ============================================================
// Diagnostic: lance-io ObjectStore advanced write modes
// ============================================================

use lance_io::object_store::{ObjectStoreParams, ObjectStoreRegistry};

#[tokio::test]
#[ignore = "Requires GooseFS cluster"]
async fn test_diag_lance_io_write_modes() {
    let addr = std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".into());
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let root = format!("goosefs://{}/lance-test/lance_io_direct_{}", addr, ts);

    eprintln!("[DIAG] Creating ObjectStore at: {}", root);

    let params = ObjectStoreParams::default();
    let registry = Arc::new(ObjectStoreRegistry::default());
    let (object_store, _path) = ObjectStore::from_uri_and_params(registry, &root, &params)
        .await
        .expect("Failed to create ObjectStore");

    // Best-effort cleanup of leftover test files. object_store_opendal maps
    // relative paths to the GooseFS root, so prior runs leave files at /
    // even when the ObjectStore URI uses a per-run subdirectory. The
    // concurrent exactly-one-winner race now lives in its own
    // `test_diag_concurrent_put_create_exactly_one_winner` test, which
    // uses a per-run filename so it does not share state with this test.
    for name in ["test_file.txt", "test_create.txt"].iter() {
        let p = object_store::path::Path::parse(name).unwrap();
        let _ = object_store.inner.delete(&p).await;
    }

    // Test 1: Basic put + get
    let test_path = object_store::path::Path::parse("test_file.txt").unwrap();
    let test_data = bytes::Bytes::from("Hello from lance-io ObjectStore!");

    eprintln!(
        "[DIAG] Writing test_file.txt ({} bytes)...",
        test_data.len()
    );
    match object_store
        .inner
        .put(&test_path, test_data.clone().into())
        .await
    {
        Ok(_) => eprintln!("[DIAG] Write succeeded! ✅"),
        Err(e) => {
            eprintln!("[DIAG] Write FAILED: {:?}", e);
            eprintln!("[DIAG] Error source: {:?}", std::error::Error::source(&e));
            return;
        }
    }

    eprintln!("[DIAG] Reading test_file.txt...");
    match object_store.inner.get(&test_path).await {
        Ok(result) => {
            let bytes = result.bytes().await.unwrap();
            let content = String::from_utf8_lossy(&bytes);
            eprintln!("[DIAG] Read content: '{}' ({} bytes)", content, bytes.len());
            assert_eq!(bytes, test_data);
        }
        Err(e) => eprintln!("[DIAG] Read FAILED: {:?}", e),
    }

    // PutMode::Create (if_not_exists) — required by
    // ConditionalPutCommitHandler for concurrent-safe manifest commits.
    let create_path = object_store::path::Path::parse("test_create.txt").unwrap();
    eprintln!("[DIAG] Writing with PutMode::Create (if_not_exists)...");
    object_store
        .inner
        .put_opts(
            &create_path,
            bytes::Bytes::from("conditional write!").into(),
            object_store::PutOptions {
                mode: object_store::PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .expect("PutMode::Create should succeed for a new path");
    eprintln!("[DIAG] PutMode::Create succeeded! ✅");

    eprintln!("[DIAG] Second PutMode::Create on same path (expect AlreadyExists)...");
    let conflict = object_store
        .inner
        .put_opts(
            &create_path,
            bytes::Bytes::from("should not overwrite").into(),
            object_store::PutOptions {
                mode: object_store::PutMode::Create,
                ..Default::default()
            },
        )
        .await;
    assert!(
        matches!(
            conflict,
            Err(object_store::Error::AlreadyExists { .. })
                | Err(object_store::Error::Precondition { .. })
        ),
        "second PutMode::Create must fail with AlreadyExists/Precondition, got: {conflict:?}"
    );
    eprintln!("[DIAG] PutMode::Create conflict correctly rejected! ✅");

    // Test 3: rename_if_not_exists
    eprintln!("[DIAG] Testing rename_if_not_exists...");
    let tmp_path = object_store::path::Path::parse("_tmp_rename.txt").unwrap();
    let dest_path = object_store::path::Path::parse("renamed.txt").unwrap();
    match object_store
        .inner
        .put(&tmp_path, bytes::Bytes::from("rename me!").into())
        .await
    {
        Ok(_) => {
            eprintln!("[DIAG] Tmp file written ✅");
            match object_store
                .inner
                .rename_if_not_exists(&tmp_path, &dest_path)
                .await
            {
                Ok(_) => eprintln!("[DIAG] rename_if_not_exists succeeded! ✅"),
                Err(e) => eprintln!("[DIAG] rename_if_not_exists FAILED: {:?}", e),
            }
        }
        Err(e) => eprintln!("[DIAG] Tmp file write FAILED: {:?}", e),
    }

    eprintln!("[DIAG] lance-io direct write test complete ✅");
}

/// Concurrency regression: two `PutMode::Create` writers racing on the
/// same fresh path must produce exactly one winner, and the stored bytes
/// must match the winner's payload byte-for-byte. This is the property
/// that makes `ConditionalPutCommitHandler` safe under concurrent
/// manifest commits: every writer either commits its manifest or observes
/// the loser's precondition failure and retries against a fresh version.
///
/// Split out of `test_diag_lance_io_write_modes` so the race assertions
/// cannot be silently skipped — the basic-write `return` on error in the
/// diagnostic test no longer covers the race, and the race is now a
/// first-class ignored test with its own setup/teardown invariants:
///
/// 1. **Per-run path.** The race filename embeds a nanosecond timestamp so
///    concurrent or back-to-back runs in the same process never share
///    state. `object_store_opendal` maps the relative filename to the
///    GooseFS root regardless of the ObjectStore URI path, so a
///    per-run URI subdirectory would not isolate runs on its own.
/// 2. **Explicit pre-cleanup.** A leftover from a prior crashed run is
///    expected (`NotFound` is the only accepted pre-cleanup error); any
///    other error fails the test rather than being silently ignored.
/// 3. **Strict setup.** Every setup step uses `expect` so a transient
///    `ObjectStore` construction or filename-parse error is surfaced
///    rather than logged and skipped.
/// 4. **Post-cleanup.** The winning path is removed on success; cleanup
///    must report `Ok` or `NotFound`.
///
/// Requires a live GooseFS cluster.
#[tokio::test]
#[ignore = "Requires GooseFS cluster"]
async fn test_diag_concurrent_put_create_exactly_one_winner() {
    let addr = std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".into());
    // Nanosecond timestamp gives effectively unique filenames even for
    // back-to-back runs in the same process. The race path is *not* put
    // under the per-run URI subdirectory because `object_store_opendal`
    // ignores the URL path when resolving relative keys.
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let race_filename = format!("test_create_race_{}.txt", ts);
    let root = format!("goosefs://{}/lance-test", addr);
    eprintln!(
        "[DIAG] Creating ObjectStore at: {} (race path: {})",
        root, race_filename
    );

    let params = ObjectStoreParams::default();
    let registry = Arc::new(ObjectStoreRegistry::default());
    let (object_store, _path) = ObjectStore::from_uri_and_params(registry.clone(), &root, &params)
        .await
        .expect("Failed to create ObjectStore");

    let race_path = object_store::path::Path::parse(&race_filename).unwrap();

    // Pre-flight cleanup: the race path is per-run, so a leftover can
    // only come from a prior run that crashed before its post-cleanup
    // completed. `Ok` and `NotFound` are both expected; any other error
    // is a setup failure that would otherwise silently skip the race.
    match object_store.inner.delete(&race_path).await {
        Ok(_) => {}
        Err(object_store::Error::NotFound { .. }) => {}
        Err(e) => panic!("pre-cleanup of race path {race_filename} failed: {e:?}"),
    }

    let store_a = object_store.clone();
    let store_b = object_store.clone();
    let path_a = race_path.clone();
    let path_b = race_path.clone();
    let payload_a = bytes::Bytes::from("writer-A");
    let payload_b = bytes::Bytes::from("writer-B");

    eprintln!("[DIAG] Launching two concurrent PutMode::Create on fresh path...");
    let fut_a = tokio::spawn(async move {
        store_a
            .inner
            .put_opts(
                &path_a,
                payload_a.clone().into(),
                object_store::PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map(|_| payload_a)
    });
    let fut_b = tokio::spawn(async move {
        store_b
            .inner
            .put_opts(
                &path_b,
                payload_b.clone().into(),
                object_store::PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map(|_| payload_b)
    });
    let (res_a, res_b) = tokio::join!(fut_a, fut_b);
    let res_a = res_a.expect("writer A join");
    let res_b = res_b.expect("writer B join");

    let mut wins = 0usize;
    let mut conflicts = 0usize;
    let mut other_err: Option<String> = None;
    let mut winner_payload: Option<bytes::Bytes> = None;
    match (&res_a, &res_b) {
        (Ok(p), Err(_)) => {
            wins = 1;
            conflicts = 1;
            winner_payload = Some(p.clone());
        }
        (Err(_), Ok(p)) => {
            wins = 1;
            conflicts = 1;
            winner_payload = Some(p.clone());
        }
        (Ok(_), Ok(_)) => {
            other_err = Some("both writers reported success — if-not-exists violated".into());
        }
        (Err(ea), Err(eb)) => {
            other_err = Some(format!("both writers failed: a={ea:?} b={eb:?}"));
        }
    }
    for r in [&res_a, &res_b] {
        if let Err(e) = r {
            let s = format!("{e:?}").to_lowercase();
            assert!(
                s.contains("already exists")
                    || s.contains("precondition")
                    || s.contains("conditionnotmatch")
                    || s.contains("if_not_exists"),
                "loser must report AlreadyExists/Precondition, got: {e:?}"
            );
        }
    }
    assert!(
        other_err.is_none(),
        "exactly-one-winner violated: {}",
        other_err.unwrap()
    );
    assert_eq!(wins, 1, "exactly one writer must win (got {wins})");
    assert_eq!(
        conflicts, 1,
        "exactly one writer must conflict (got {conflicts})"
    );

    // Read back — stored bytes must match the winner's payload exactly.
    let stored = object_store
        .inner
        .get(&race_path)
        .await
        .expect("get winning payload")
        .bytes()
        .await
        .expect("read winning payload");
    let expected = winner_payload.expect("winner payload recorded");
    assert_eq!(
        stored, expected,
        "stored bytes must match the winner's payload"
    );
    eprintln!(
        "[DIAG] concurrent exactly-one-winner ok ✅ stored={} bytes",
        stored.len()
    );

    // Post-flight cleanup: must succeed. `NotFound` after a winning write
    // would mean the file never actually persisted, contradicting the
    // assertion above, so we surface it as a failure rather than masking
    // a real regression.
    match object_store.inner.delete(&race_path).await {
        Ok(_) => {}
        Err(object_store::Error::NotFound { .. }) => {
            panic!(
                "post-cleanup of race path {race_filename} reported NotFound after a winning write"
            )
        }
        Err(e) => panic!("post-cleanup of race path {race_filename} failed: {e:?}"),
    }
}
