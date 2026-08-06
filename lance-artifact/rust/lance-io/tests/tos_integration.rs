// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
//! These integration tests can only be run against a real Volcengine TOS bucket.

#![cfg(feature = "tos-test")]

use futures::TryStreamExt;
use lance_io::object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use tokio::io::AsyncWriteExt;

fn tos_bucket() -> String {
    std::env::var("TOS_BUCKET").expect("TOS_BUCKET must be set")
}

async fn delete_prefix(store: &ObjectStore, prefix: &str) {
    let prefix_path = Path::from(prefix);
    let locations = store
        .inner
        .list(Some(&prefix_path))
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await
        .unwrap_or_default();

    for location in locations {
        let _ = store.inner.delete(&location).await;
    }
}

#[ignore = "Must be run manually on Volcengine TOS"]
#[tokio::test]
async fn test_tos_write_read_list_delete() {
    let prefix = format!("lance-tos-{}-{}", std::process::id(), rand::random::<u64>());
    let bucket = tos_bucket();
    let (store, base_path) = ObjectStore::from_uri(&format!("tos://{bucket}/{prefix}"))
        .await
        .unwrap();
    assert_eq!(base_path, Path::from(prefix.as_str()));

    let path = Path::from(format!("{prefix}/small.txt"));
    delete_prefix(&store, &prefix).await;

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let mut writer = store.create(&path).await?;
        writer.write_all(b"hello").await?;
        writer.write_all(b" tos").await?;
        writer.shutdown().await?;

        let meta = store.inner.head(&path).await?;
        if meta.size != 9 {
            return Err(format!("expected object size 9, got {}", meta.size).into());
        }

        let data = store.inner.get(&path).await?.bytes().await?;
        if data.as_ref() != b"hello tos" {
            return Err("downloaded TOS object content did not match".into());
        }

        let listed = store
            .inner
            .list(Some(&Path::from(prefix.as_str())))
            .try_collect::<Vec<_>>()
            .await?;
        if !listed.iter().any(|meta| meta.location == path) {
            return Err("uploaded TOS object was not returned by list".into());
        }

        store.inner.delete(&path).await?;
        if store.exists(&path).await? {
            return Err("deleted TOS object still exists".into());
        }

        Ok(())
    }
    .await;

    delete_prefix(&store, &prefix).await;
    result.unwrap();
}
