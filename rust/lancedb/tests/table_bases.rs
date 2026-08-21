// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::{WriteMode, WriteParams};
use lancedb::{Result, TableBase, connect, connect_namespace, table::WriteOptions};
use tempfile::tempdir;
use url::Url;

fn empty_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn file_uri(path: &std::path::Path) -> String {
    Url::from_file_path(path)
        .unwrap_or_else(|_| panic!("not an absolute path: {}", path.display()))
        .to_string()
}

#[tokio::test]
async fn test_add_bases_accepts_named_and_dataset_root_entries() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().join("db").to_str().unwrap())
        .execute()
        .await?;
    let table = db.create_empty_table("t", empty_schema()).execute().await?;
    let media = tmp.path().join("media");
    let parent = tmp.path().join("parent");
    std::fs::create_dir_all(&media).unwrap();
    std::fs::create_dir_all(&parent).unwrap();

    table
        .add_bases([
            TableBase {
                path: file_uri(&media),
                name: Some("media".into()),
                is_dataset_root: false,
            },
            TableBase {
                path: file_uri(&parent),
                name: Some("parent".into()),
                is_dataset_root: true,
            },
        ])
        .await
}

#[tokio::test]
async fn test_add_bases_accepts_two_unnamed_paths() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().join("db").to_str().unwrap())
        .execute()
        .await?;
    let table = db.create_empty_table("t", empty_schema()).execute().await?;
    let media = tmp.path().join("media");
    let other = tmp.path().join("other");
    std::fs::create_dir_all(&media).unwrap();
    std::fs::create_dir_all(&other).unwrap();
    table
        .add_bases([&file_uri(&media), &file_uri(&other)])
        .await
}

#[tokio::test]
async fn test_add_bases_write_and_read_through_registered_base() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().join("db").to_str().unwrap())
        .execute()
        .await?;
    let table = db.create_empty_table("t", empty_schema()).execute().await?;
    let media = tmp.path().join("media");
    std::fs::create_dir_all(&media).unwrap();
    let media_uri = file_uri(&media);
    table.add_bases([&media_uri]).await?;

    let batch = RecordBatch::try_new(
        empty_schema(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    table
        .add(batch)
        .write_options(WriteOptions {
            lance_write_params: Some(WriteParams {
                mode: WriteMode::Append,
                target_base_names_or_paths: Some(vec![media_uri.clone()]),
                ..Default::default()
            }),
        })
        .execute()
        .await?;

    assert_eq!(table.count_rows(None).await?, 3);

    let dataset = table.dataset().unwrap().get().await?;
    let registered = dataset
        .manifest()
        .base_paths
        .values()
        .find(|base| base.path == media_uri)
        .expect("registered base");
    assert_ne!(registered.id, 0);
    assert!(registered.name.is_none());
    assert!(
        dataset.get_fragments().iter().any(|fragment| {
            fragment
                .metadata()
                .files
                .iter()
                .any(|file| file.base_id == Some(registered.id))
        }),
        "written fragment should reference the registered base"
    );
    assert!(
        std::fs::read_dir(&media)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .any(|entry| entry.path().extension().is_some_and(|ext| ext == "lance")),
        "data file should land under the registered base"
    );
    Ok(())
}

#[tokio::test]
async fn test_memory_add_bases_accepts_a_file_uri() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect("memory://").execute().await?;
    let table = db.create_empty_table("t", empty_schema()).execute().await?;
    let media = tmp.path().join("media");
    std::fs::create_dir_all(&media).unwrap();
    table.add_bases([file_uri(&media)]).await
}

#[tokio::test]
async fn test_namespace_add_bases_accepts_a_file_uri() -> Result<()> {
    let tmp = tempdir().unwrap();
    let mut properties = std::collections::HashMap::new();
    properties.insert("root".to_string(), tmp.path().to_str().unwrap().to_string());
    let db = connect_namespace("dir", properties).execute().await?;
    let table = db.create_empty_table("t", empty_schema()).execute().await?;
    let media = tmp.path().join("media");
    std::fs::create_dir_all(&media).unwrap();
    table.add_bases([file_uri(&media)]).await
}
