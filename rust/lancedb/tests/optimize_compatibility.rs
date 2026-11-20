// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Compatibility with the Optimize APIs published before the naming cleanup.
#![allow(deprecated)]

use std::any::TypeId;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use lancedb::table::{
    CompactionMode, CompactionOptions, CompactionOverrides, CompactionResult, IndexOptimizeOptions,
    OptimizeAction, OptimizeDataMode, OptimizeOptions, OptimizeResult,
};

#[test]
fn released_type_paths_keep_their_meaning() {
    assert_eq!(
        TypeId::of::<OptimizeOptions>(),
        TypeId::of::<IndexOptimizeOptions>()
    );
    assert_eq!(
        TypeId::of::<CompactionOptions>(),
        TypeId::of::<lance::dataset::optimize::CompactionOptions>()
    );
    assert_eq!(
        TypeId::of::<CompactionMode>(),
        TypeId::of::<OptimizeDataMode>()
    );
    assert_eq!(
        TypeId::of::<CompactionResult>(),
        TypeId::of::<OptimizeResult>()
    );
    let _ = OptimizeAction::Index(OptimizeOptions::default());
}

#[tokio::test]
async fn released_compact_action_rewrites_without_pruning() {
    let batch =
        RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef)])
            .unwrap();
    let db = lancedb::connect("memory://").execute().await.unwrap();
    let table = db
        .create_table("legacy", batch.clone())
        .execute()
        .await
        .unwrap();
    table.add(batch).execute().await.unwrap();
    let stats = table
        .optimize(OptimizeAction::Compact {
            options: CompactionOptions {
                target_rows_per_fragment: 100,
                ..Default::default()
            },
            remap_options: None,
        })
        .await
        .unwrap();
    assert_eq!(stats.compaction.unwrap().fragments_removed, 2);
    assert!(stats.prune.is_none());
    assert_eq!(table.count_rows(None).await.unwrap(), 4);
    table.checkout(1).await.unwrap();
    assert_eq!(table.count_rows(None).await.unwrap(), 2);
    table.checkout_latest().await.unwrap();
    assert!(
        table
            .compact(CompactionOverrides::default())
            .await
            .unwrap()
            .prune
            .is_none()
    );
    assert!(matches!(
        table.compact_async(CompactionOverrides::default()).await,
        Err(lancedb::Error::NotSupported { .. })
    ));
}
