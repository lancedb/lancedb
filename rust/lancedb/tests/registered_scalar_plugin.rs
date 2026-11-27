// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_array::{RecordBatchIterator, record_batch};
use lance::{Dataset, index::DatasetIndexExt};
use lance_index::scalar::ScalarIndexParams;
use lancedb::index::IndexType;

// MinHashLSH is a registered scalar plugin without a LanceDB IndexType variant.
// It exercises the same metadata-only fallback as a downstream plugin.
#[tokio::test]
async fn list_registered_scalar_plugin_with_its_type_name() {
    let directory = tempfile::tempdir().unwrap();
    let batch = record_batch!((
        "value",
        Utf8,
        ["alpha beta", "beta gamma", "gamma delta", "delta alpha"]
    ))
    .unwrap();
    let mut dataset = Dataset::write(
        RecordBatchIterator::new([Ok(batch.clone())], batch.schema()),
        directory.path().join("test.lance").to_str().unwrap(),
        None,
    )
    .await
    .unwrap();
    dataset
        .create_index_builder(
            &["value"],
            lance_index::IndexType::Scalar,
            &ScalarIndexParams::new("minhashlsh".into()),
        )
        .name("profile".into())
        .train(false)
        .await
        .unwrap();

    let connection = lancedb::connect(directory.path().to_str().unwrap())
        .execute()
        .await
        .unwrap();
    let table = connection.open_table("test").execute().await.unwrap();
    let indices = table.list_indices().await.unwrap();
    assert_eq!(indices.len(), 1);
    assert_eq!(indices[0].name, "profile");
    assert_eq!(
        indices[0].index_type,
        IndexType::Custom("MinHashLsh".into())
    );
    assert_eq!(indices[0].index_type.to_string(), "MinHashLsh");
    assert_eq!(indices[0].columns, ["value"]);
    assert!(
        indices[0]
            .type_url
            .as_deref()
            .unwrap()
            .ends_with("MinHashLshIndexDetails")
    );
    let statistics = table.index_stats("profile").await.unwrap().unwrap();
    assert_eq!(
        statistics.index_type,
        IndexType::Custom("MinHashLsh".into())
    );
    assert_eq!(statistics.num_indexed_rows, 0);
    assert_eq!(statistics.num_unindexed_rows, 4);
}
