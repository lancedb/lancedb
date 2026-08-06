// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
//! These integration tests can only be run against a real GCS bucket.  
//! They do not work against any local emulator right now.
#![cfg(feature = "gcs-test")]

use std::sync::Arc;

// TODO: Once we re-use this logic for S3, we can instead use tests against
// Minio to validate the multipart upload logic.
use lance_io::object_store::ObjectStore;
use object_store::{ObjectStoreExt, path::Path};
use tokio::io::AsyncWriteExt;

async fn get_store() -> Arc<ObjectStore> {
    let bucket_name = std::env::var("OBJECT_STORE_BUCKET").unwrap_or_else(|_| "test-bucket".into());
    ObjectStore::from_uri(&format!("gs://{}/object", bucket_name))
        .await
        .unwrap()
        .0
}

#[ignore = "Must be run manually on GCS"]
#[tokio::test]
async fn test_small_upload() {
    let store = get_store().await;

    let path = Path::from("test_small_upload");
    if store.exists(&path).await.unwrap() {
        store.delete(&path).await.unwrap();
    }

    // Write an empty file.
    let mut writer = store.create(&path).await.unwrap();
    writer.shutdown().await.unwrap();
    let meta = store.inner.head(&path).await.unwrap();
    assert_eq!(meta.size, 0);
    store.delete(&path).await.unwrap();

    // Write a small file, with two small writes.
    let mut writer = store.create(&path).await.unwrap();
    writer.write_all(b"hello").await.unwrap();
    writer.write_all(b"world").await.unwrap();
    writer.shutdown().await.unwrap();
    let meta = store.inner.head(&path).await.unwrap();
    assert_eq!(meta.size, 10);
    store.delete(&path).await.unwrap();
}

#[ignore = "Must be run manually on GCS"]
#[tokio::test]
async fn test_large_upload() {
    let store = get_store().await;

    let path = Path::from("test_large_upload");
    if store.exists(&path).await.unwrap() {
        store.delete(&path).await.unwrap();
    }

    let mut writer = store.create(&path).await.unwrap();

    // Write a few 3MB buffers
    writer
        .write_all(&vec![b'a'; 3 * 1024 * 1024])
        .await
        .unwrap();
    writer
        .write_all(&vec![b'b'; 3 * 1024 * 1024])
        .await
        .unwrap();
    writer
        .write_all(&vec![b'c'; 3 * 1024 * 1024])
        .await
        .unwrap();
    writer
        .write_all(&vec![b'd'; 3 * 1024 * 1024])
        .await
        .unwrap();

    // Write a 40MB buffer
    writer
        .write_all(&vec![b'e'; 40 * 1024 * 1024])
        .await
        .unwrap();

    writer.flush().await.unwrap();
    writer.shutdown().await.unwrap();

    let meta = store.inner.head(&path).await.unwrap();
    assert_eq!(meta.size, 52 * 1024 * 1024);

    let data = store.inner.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(data.len(), 52 * 1024 * 1024);
    assert_eq!(data[0], b'a');
    assert_eq!(data[3 * 1024 * 1024], b'b');
    assert_eq!(data[6 * 1024 * 1024], b'c');
    assert_eq!(data[9 * 1024 * 1024], b'd');
    assert_eq!(data[12 * 1024 * 1024], b'e');
}
