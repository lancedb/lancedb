// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use lancedb::{
    Table,
    index::{Index, scalar::BTreeIndexBuilder},
    table::{OptimizeAction, optimize::OptimizeOptions},
};
use std::sync::Arc;

fn rows(ids: Vec<i32>) -> RecordBatch {
    RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(ids)) as ArrayRef)]).unwrap()
}
async fn table() -> Table {
    let db = lancedb::connect("memory://").execute().await.unwrap();
    db.create_table("events", rows(vec![1]))
        .execute()
        .await
        .unwrap()
}

#[tokio::test]
async fn optimize_options_run_full_cycle_while_rewrite_is_explicit() {
    let table = table().await;
    table
        .create_index(&["id"], Index::BTree(BTreeIndexBuilder::default()))
        .execute()
        .await
        .unwrap();
    table.add(rows(vec![2])).execute().await.unwrap();
    // The configured target prevents a rewrite so index maintenance is observable.
    table
        .as_native()
        .unwrap()
        .update_config([(
            "lance.compaction.target_rows_per_fragment".into(),
            "1".into(),
        )])
        .await
        .unwrap();
    let index = table.list_indices().await.unwrap()[0].name.clone();
    let stats = table
        .optimize(OptimizeAction::RewriteData {
            options: OptimizeOptions::default(),
            remap_options: None,
        })
        .await
        .unwrap();
    assert!(stats.prune.is_none());
    assert_eq!(
        table
            .index_stats(&index)
            .await
            .unwrap()
            .unwrap()
            .num_unindexed_rows,
        1
    );
    let stats = table.optimize(OptimizeOptions::default()).await.unwrap();
    assert!(stats.prune.is_some());
    assert_eq!(
        table
            .index_stats(&index)
            .await
            .unwrap()
            .unwrap()
            .num_unindexed_rows,
        0
    );
    assert_eq!(table.count_rows(None).await.unwrap(), 2);
}

#[tokio::test]
#[allow(deprecated)]
async fn optimize_inherits_table_policy_and_invocation_overrides_it() {
    let table = table().await;
    table.add(rows(vec![2])).execute().await.unwrap();
    table
        .as_native()
        .unwrap()
        .update_config([
            (
                "lance.compaction.target_rows_per_fragment".into(),
                "1".into(),
            ),
            ("lance.compaction.io_buffer_size".into(), "4096".into()),
        ])
        .await
        .unwrap();
    let stats = table.optimize(OptimizeOptions::default()).await.unwrap();
    assert_eq!(stats.compaction.unwrap().fragments_removed, 0);
    let legacy = table
        .compact(lancedb::table::CompactionOverrides::default())
        .await
        .unwrap();
    assert_eq!(legacy.compaction.unwrap().fragments_removed, 0);
    assert!(legacy.prune.is_none());
    let stats = table
        .optimize(OptimizeOptions {
            target_rows_per_fragment: Some(100),
            io_buffer_size: Some(0),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(stats.compaction.unwrap().fragments_removed, 2);
    assert_eq!(table.count_rows(None).await.unwrap(), 2);
    let config = table.as_native().unwrap().manifest().await.unwrap().config;
    assert_eq!(
        config
            .get("lance.compaction.target_rows_per_fragment")
            .unwrap(),
        "1"
    );
}
