// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Helpers for converting between [`Dataset`] and [`TableIdentifier`](pb::TableIdentifier) proto.

use std::sync::Arc;

use lance_core::Result;
use lance_datafusion::pb;
use lance_io::object_store::StorageOptions;
use prost::Message;

use crate::Dataset;
use crate::dataset::builder::DatasetBuilder;

/// Build a [`TableIdentifier`] from a [`Dataset`].
///
/// Default: lightweight mode (uri + version + etag only, no serialized manifest).
/// Includes the dataset's latest storage options (if any) so the remote executor
/// can open or cache the dataset with the correct storage configuration.
pub async fn table_identifier_from_dataset(dataset: &Dataset) -> Result<pb::TableIdentifier> {
    Ok(pb::TableIdentifier {
        uri: dataset.uri().to_string(),
        version: dataset.manifest.version,
        manifest_etag: dataset.manifest_location.e_tag.clone(),
        serialized_manifest: None,
        storage_options: dataset
            .latest_storage_options()
            .await?
            .map(|StorageOptions(m)| m)
            .unwrap_or_default(),
    })
}

/// Build a [`TableIdentifier`] with serialized manifest bytes included.
///
/// Fast path: remote executor skips manifest read from storage.
pub async fn table_identifier_from_dataset_with_manifest(
    dataset: &Dataset,
) -> Result<pb::TableIdentifier> {
    let manifest_proto = lance_table::format::pb::Manifest::from(dataset.manifest.as_ref());
    Ok(pb::TableIdentifier {
        uri: dataset.uri().to_string(),
        version: dataset.manifest.version,
        manifest_etag: dataset.manifest_location.e_tag.clone(),
        serialized_manifest: Some(manifest_proto.encode_to_vec()),
        storage_options: dataset
            .latest_storage_options()
            .await?
            .map(|StorageOptions(m)| m)
            .unwrap_or_default(),
    })
}

/// Open a dataset from a table identifier proto.
pub async fn open_dataset_from_table_identifier(
    table_id: &pb::TableIdentifier,
) -> Result<Arc<Dataset>> {
    let mut builder = DatasetBuilder::from_uri(&table_id.uri).with_version(table_id.version);
    if let Some(manifest_bytes) = &table_id.serialized_manifest {
        builder = builder.with_serialized_manifest(manifest_bytes)?;
    }
    if !table_id.storage_options.is_empty() {
        builder = builder.with_storage_options(table_id.storage_options.clone());
    }
    Ok(Arc::new(builder.load().await?))
}

/// Resolve a dataset from an optional pre-loaded instance or from a table identifier.
///
/// If `dataset` is `Some`, returns it directly. Otherwise, opens the dataset
/// from the table identifier proto.
pub async fn resolve_dataset(
    dataset: Option<Arc<Dataset>>,
    table_id: Option<&pb::TableIdentifier>,
) -> Result<Arc<Dataset>> {
    use lance_core::Error;
    match dataset {
        Some(ds) => Ok(ds),
        None => {
            let table_id = table_id.ok_or_else(|| {
                Error::invalid_input_source("Missing TableIdentifier in proto".into())
            })?;
            open_dataset_from_table_identifier(table_id).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::RecordBatchIterator;
    use arrow_array::types::UInt32Type;
    use lance_datagen::{array, gen_batch};
    use std::collections::HashMap;

    async fn make_test_dataset() -> (Arc<Dataset>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let batch = gen_batch()
            .col("x", array::step::<UInt32Type>())
            .col("y", array::step::<UInt32Type>())
            .into_batch_rows(lance_datagen::RowCount::from(100))
            .unwrap();
        let path = dir.path().join("test.lance");
        let ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
            path.to_str().unwrap(),
            None,
        )
        .await
        .unwrap();
        (Arc::new(ds), dir)
    }

    #[test]
    fn test_table_identifier_proto_roundtrip() {
        let id = pb::TableIdentifier {
            uri: "s3://bucket/table.lance".to_string(),
            version: 42,
            manifest_etag: Some("etag123".to_string()),
            serialized_manifest: None,
            storage_options: HashMap::new(),
        };
        let bytes = id.encode_to_vec();
        let back = pb::TableIdentifier::decode(bytes.as_slice()).unwrap();
        assert_eq!(id.uri, back.uri);
        assert_eq!(id.version, back.version);
        assert_eq!(id.manifest_etag, back.manifest_etag);
        assert!(back.serialized_manifest.is_none());
    }

    #[test]
    fn test_table_identifier_proto_with_storage_options() {
        let mut opts = HashMap::new();
        opts.insert("region".to_string(), "us-east-1".to_string());
        opts.insert("endpoint".to_string(), "https://s3.example.com".to_string());

        let id = pb::TableIdentifier {
            uri: "s3://bucket/table.lance".to_string(),
            version: 7,
            manifest_etag: None,
            serialized_manifest: None,
            storage_options: opts.clone(),
        };
        let bytes = id.encode_to_vec();
        let back = pb::TableIdentifier::decode(bytes.as_slice()).unwrap();
        assert_eq!(back.storage_options, opts);
    }

    #[tokio::test]
    async fn test_table_identifier_from_dataset_roundtrip() {
        let (dataset, _dir) = make_test_dataset().await;

        let id = table_identifier_from_dataset(&dataset).await.unwrap();
        assert_eq!(id.uri, dataset.uri());
        assert_eq!(id.version, dataset.manifest.version);
        assert!(id.serialized_manifest.is_none());

        // Roundtrip: open the dataset back from the identifier
        let back = open_dataset_from_table_identifier(&id).await.unwrap();
        assert_eq!(back.uri(), dataset.uri());
        assert_eq!(back.manifest.version, dataset.manifest.version);
    }

    #[tokio::test]
    async fn test_table_identifier_with_manifest_roundtrip() {
        let (dataset, _dir) = make_test_dataset().await;

        let id = table_identifier_from_dataset_with_manifest(&dataset)
            .await
            .unwrap();
        assert_eq!(id.uri, dataset.uri());
        assert_eq!(id.version, dataset.manifest.version);
        assert!(id.serialized_manifest.is_some());

        // Verify the serialized manifest bytes decode
        let manifest_bytes = id.serialized_manifest.as_ref().unwrap();
        let _manifest_proto =
            lance_table::format::pb::Manifest::decode(manifest_bytes.as_slice()).unwrap();

        // Roundtrip: open the dataset back from the identifier (with manifest)
        let back = open_dataset_from_table_identifier(&id).await.unwrap();
        assert_eq!(back.uri(), dataset.uri());
        assert_eq!(back.manifest.version, dataset.manifest.version);
    }
}
