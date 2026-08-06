// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;

use crate::dataset::transaction::{Operation, Transaction, UpdateMap, UpdateMapEntry};

use super::Dataset;
use crate::Result;
use futures::future::BoxFuture;
use lance_core::datatypes::FieldRef;
use lance_core::datatypes::Schema;

/// Execute a metadata update operation on a dataset.
/// This is moved from Dataset::update_op to keep metadata logic in this module.
pub async fn execute_metadata_update(dataset: &mut Dataset, operation: Operation) -> Result<()> {
    let transaction = Transaction::new(dataset.manifest.version, operation, None);
    dataset
        .apply_commit(transaction, &Default::default(), &Default::default())
        .await?;
    Ok(())
}

/// Builder for metadata update operations that supports optional replace semantics.
/// This provides backward compatibility while adding new functionality.
pub struct UpdateMetadataBuilder<'a> {
    dataset: &'a mut Dataset,
    values: Vec<UpdateMapEntry>,
    replace: bool,
    metadata_type: MetadataType,
}

/// Type of metadata being updated
pub enum MetadataType {
    Config,
    TableMetadata,
    SchemaMetadata,
}

impl<'a> UpdateMetadataBuilder<'a> {
    pub fn new(
        dataset: &'a mut Dataset,
        values: impl IntoIterator<Item = impl Into<UpdateMapEntry>>,
        metadata_type: MetadataType,
    ) -> Self {
        Self {
            dataset,
            values: values.into_iter().map(Into::into).collect(),
            replace: false,
            metadata_type,
        }
    }

    /// Set the replace flag to true, causing the entire metadata map to be replaced
    /// instead of merged.
    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }

    fn create_update_map(values: Vec<UpdateMapEntry>, replace: bool) -> UpdateMap {
        UpdateMap {
            update_entries: values,
            replace,
        }
    }
}

impl<'a> std::future::IntoFuture for UpdateMetadataBuilder<'a> {
    type Output = Result<HashMap<String, String>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let update_map = Self::create_update_map(self.values, self.replace);

            let operation = match self.metadata_type {
                MetadataType::Config => Operation::UpdateConfig {
                    config_updates: Some(update_map),
                    table_metadata_updates: None,
                    schema_metadata_updates: None,
                    field_metadata_updates: HashMap::new(),
                },
                MetadataType::TableMetadata => Operation::UpdateConfig {
                    config_updates: None,
                    table_metadata_updates: Some(update_map),
                    schema_metadata_updates: None,
                    field_metadata_updates: HashMap::new(),
                },
                MetadataType::SchemaMetadata => Operation::UpdateConfig {
                    config_updates: None,
                    table_metadata_updates: None,
                    schema_metadata_updates: Some(update_map),
                    field_metadata_updates: HashMap::new(),
                },
            };

            execute_metadata_update(self.dataset, operation).await?;

            // Get result after the update
            let result = match self.metadata_type {
                MetadataType::Config => self.dataset.manifest.config.clone(),
                MetadataType::TableMetadata => self.dataset.manifest.table_metadata.clone(),
                MetadataType::SchemaMetadata => self.dataset.manifest.schema.metadata.clone(),
            };

            Ok(result)
        })
    }
}

#[derive(Debug)]
pub struct UpdateFieldMetadataBuilder<'a> {
    dataset: &'a mut Dataset,
    field_metadata_updates: HashMap<i32, UpdateMap>,
}

impl<'a> UpdateFieldMetadataBuilder<'a> {
    pub fn new(dataset: &'a mut Dataset) -> Self {
        Self {
            dataset,
            field_metadata_updates: HashMap::new(),
        }
    }

    fn apply<'b>(
        mut self,
        field: impl Into<FieldRef<'b>> + 'b,
        values: impl IntoIterator<Item = impl Into<UpdateMapEntry>>,
        replace: bool,
    ) -> Result<Self> {
        let field_id = field.into().into_id(self.dataset.schema())?;
        let values = UpdateMap {
            update_entries: values.into_iter().map(Into::into).collect(),
            replace,
        };
        self.field_metadata_updates.insert(field_id, values);
        Ok(self)
    }
    pub fn update<'b>(
        self,
        field: impl Into<FieldRef<'b>> + 'b,
        values: impl IntoIterator<Item = impl Into<UpdateMapEntry>>,
    ) -> Result<Self> {
        self.apply(field, values, false)
    }

    pub fn replace<'b>(
        self,
        field: impl Into<FieldRef<'b>> + 'b,
        values: impl IntoIterator<Item = impl Into<UpdateMapEntry>>,
    ) -> Result<Self> {
        self.apply(field, values, true)
    }
}

impl<'a> std::future::IntoFuture for UpdateFieldMetadataBuilder<'a> {
    type Output = Result<&'a Schema>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            execute_metadata_update(
                self.dataset,
                Operation::UpdateConfig {
                    config_updates: None,
                    table_metadata_updates: None,
                    schema_metadata_updates: None,
                    field_metadata_updates: self.field_metadata_updates,
                },
            )
            .await?;
            Ok(self.dataset.schema())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use lance_core::Error;
    use lance_datagen::{BatchCount, RowCount, array, gen_batch};
    use rstest::rstest;

    use super::*;
    use arrow_array::{
        ArrayRef, Int32Array, RecordBatch, RecordBatchIterator, UInt32Array, types::Int32Type,
    };
    use arrow_schema::{DataType, Field as ArrowField, Fields, Schema as ArrowSchema};

    #[rstest]
    #[tokio::test]
    async fn test_update_config() {
        let data = gen_batch()
            .col("i", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));
        let mut dataset = Dataset::write(data, "memory://", None).await.unwrap();

        // Insert
        let mut desired_config = dataset.manifest.config.clone();
        desired_config.insert("lance.test".to_string(), "value".to_string());
        desired_config.insert("other-key".to_string(), "other-value".to_string());

        dataset
            .update_config([("lance.test", "value"), ("other-key", "other-value")])
            .await
            .unwrap();
        assert_eq!(dataset.manifest.config, desired_config);
        assert_eq!(dataset.config(), &desired_config);

        // Update and delete
        let mut desired_config = dataset.manifest.config.clone();
        desired_config.insert("other-key".to_string(), "new-value".to_string());
        desired_config.remove("lance.test");

        dataset
            .update_config([("other-key", Some("new-value")), ("lance.test", None)])
            .await
            .unwrap();

        // Replace
        let desired_config = HashMap::from_iter([
            ("k1".to_string(), "v1".to_string()),
            ("k2".to_string(), "v2".to_string()),
        ]);
        dataset
            .update_config([("k1", "v1"), ("k2", "v2")])
            .replace()
            .await
            .unwrap();
        assert_eq!(dataset.config(), &desired_config);

        // Clear
        dataset
            .update_config([] as [UpdateMapEntry; 0])
            .replace()
            .await
            .unwrap();
        assert!(dataset.config().is_empty());
    }

    #[tokio::test]
    async fn test_update_table_metadata() {
        let data = gen_batch()
            .col("i", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));
        let mut dataset = Dataset::write(data, "memory://", None).await.unwrap();

        // Insert
        let mut desired_table_meta = dataset.manifest.table_metadata.clone();
        desired_table_meta.insert("lance.table".to_string(), "value".to_string());
        desired_table_meta.insert(
            "other-table-key".to_string(),
            "other-table-value".to_string(),
        );

        dataset
            .update_metadata([
                ("lance.table", "value"),
                ("other-table-key", "other-table-value"),
            ])
            .await
            .unwrap();
        assert_eq!(dataset.manifest.table_metadata, desired_table_meta);

        // Update and delete
        let mut desired_table_meta = dataset.manifest.table_metadata.clone();
        desired_table_meta.insert("other-table-key".to_string(), "new-table-value".to_string());
        desired_table_meta.remove("lance.table");

        dataset
            .update_metadata([
                ("other-table-key", Some("new-table-value")),
                ("lance.table", None),
            ])
            .await
            .unwrap();

        // Replace
        let desired_table_meta = HashMap::from_iter([
            ("k1".to_string(), "v1".to_string()),
            ("k2".to_string(), "v2".to_string()),
        ]);
        dataset
            .update_metadata([("k1", "v1"), ("k2", "v2")])
            .replace()
            .await
            .unwrap();
        assert_eq!(dataset.manifest.table_metadata, desired_table_meta);

        // Clear
        dataset
            .update_metadata([] as [UpdateMapEntry; 0])
            .replace()
            .await
            .unwrap();
        assert!(dataset.manifest.table_metadata.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn test_replace_schema_metadata_preserves_fragments() {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "i",
            DataType::UInt32,
            false,
        )]));

        let data = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt32Array::from_iter_values(0..100))],
        );

        let reader = RecordBatchIterator::new(vec![data.unwrap()].into_iter().map(Ok), schema);
        let mut dataset = Dataset::write(reader, "memory://", None).await.unwrap();

        let manifest_before = dataset.manifest.clone();

        let mut new_schema_meta = HashMap::new();
        new_schema_meta.insert("new_key".to_string(), "new_value".to_string());
        #[allow(deprecated)]
        dataset
            .replace_schema_metadata(new_schema_meta.clone())
            .await
            .unwrap();

        let manifest_after = dataset.manifest.clone();

        assert_eq!(manifest_before.fragments, manifest_after.fragments);
    }

    #[rstest]
    #[tokio::test]
    async fn test_replace_fragment_metadata_preserves_fragments() {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "i",
            DataType::UInt32,
            false,
        )]));

        let data = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt32Array::from_iter_values(0..100))],
        );

        let reader = RecordBatchIterator::new(vec![data.unwrap()].into_iter().map(Ok), schema);
        let mut dataset = Dataset::write(reader, "memory://", None).await.unwrap();

        let manifest_before = dataset.manifest.clone();

        let mut new_field_meta = HashMap::new();
        new_field_meta.insert("new_key".to_string(), "new_value".to_string());
        dataset
            .replace_field_metadata(vec![(0, new_field_meta.clone())])
            .await
            .unwrap();

        let manifest_after = dataset.manifest.clone();

        assert_eq!(manifest_before.fragments, manifest_after.fragments);
    }

    async fn test_dataset_nested() -> Dataset {
        let schema = Arc::new(ArrowSchema::new_with_metadata(
            vec![
                ArrowField::new("id", DataType::Int32, false),
                ArrowField::new("name", DataType::Utf8, true),
                ArrowField::new(
                    "nested",
                    DataType::Struct(Fields::from(vec![
                        ArrowField::new("sub_field", DataType::Int32, true),
                        ArrowField::new("another_field", DataType::Float32, false),
                    ])),
                    true,
                ),
            ],
            Default::default(),
        ));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(arrow_array::StringArray::from(vec!["a", "b", "c"])),
                Arc::new(arrow_array::StructArray::from(vec![
                    (
                        Arc::new(ArrowField::new("sub_field", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("another_field", DataType::Float32, false)),
                        Arc::new(arrow_array::Float32Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
                    ),
                ])),
            ],
        )
        .unwrap();

        Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
            "memory://test",
            None,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_update_field_metadata_by_path() {
        let mut dataset = test_dataset_nested().await;

        // Test updating metadata by field path
        dataset
            .update_field_metadata()
            .update("name", [("key1", "value1"), ("key2", "value2")])
            .unwrap()
            .await
            .unwrap();

        // Verify metadata was updated
        let field = dataset.schema().field("name").unwrap();
        assert_eq!(field.metadata.get("key1"), Some(&"value1".to_string()));
        assert_eq!(field.metadata.get("key2"), Some(&"value2".to_string()));

        // Test updating nested field by path
        dataset
            .update_field_metadata()
            .update("nested.sub_field", [("nested_key", "nested_value")])
            .unwrap()
            .await
            .unwrap();

        let nested_field = dataset.schema().field("nested.sub_field").unwrap();
        assert_eq!(
            nested_field.metadata.get("nested_key"),
            Some(&"nested_value".to_string())
        );
    }

    #[tokio::test]
    async fn test_update_field_metadata_by_id() {
        let mut dataset = test_dataset_nested().await;

        // Get field IDs first
        let id_field_id = dataset.schema().field("id").unwrap().id;
        let name_field_id = dataset.schema().field("name").unwrap().id;

        // Test updating metadata by field ID
        dataset
            .update_field_metadata()
            .update(id_field_id, [("id_key", "id_value")])
            .unwrap()
            .await
            .unwrap();

        let field = dataset.schema().field_by_id(id_field_id).unwrap();
        assert_eq!(field.metadata.get("id_key"), Some(&"id_value".to_string()));

        // Update another field by ID
        dataset
            .update_field_metadata()
            .update(name_field_id, [("val_key", "val_value")])
            .unwrap()
            .await
            .unwrap();

        let field = dataset.schema().field_by_id(name_field_id).unwrap();
        assert_eq!(
            field.metadata.get("val_key"),
            Some(&"val_value".to_string())
        );
    }

    #[tokio::test]
    async fn test_update_field_metadata_replace() {
        let mut dataset = test_dataset_nested().await;

        // First, add some metadata using update
        dataset
            .update_field_metadata()
            .update("id", [("key1", "value1"), ("key2", "value2")])
            .unwrap()
            .await
            .unwrap();

        let field = dataset.schema().field("id").unwrap();
        assert_eq!(field.metadata.get("key1"), Some(&"value1".to_string()));
        assert_eq!(field.metadata.get("key2"), Some(&"value2".to_string()));

        // Now replace the metadata
        dataset
            .update_field_metadata()
            .replace("id", [("new_key", "new_value")])
            .unwrap()
            .await
            .unwrap();

        let field = dataset.schema().field("id").unwrap();
        // Old keys should be gone
        assert_eq!(field.metadata.get("key1"), None);
        assert_eq!(field.metadata.get("key2"), None);
        // New key should be present
        assert_eq!(
            field.metadata.get("new_key"),
            Some(&"new_value".to_string())
        );

        // Test clearing metadata completely by replacing with empty array
        dataset
            .update_field_metadata()
            .replace("id", [] as [(&str, &str); 0])
            .unwrap()
            .await
            .unwrap();

        let field = dataset.schema().field("id").unwrap();
        assert!(field.metadata.is_empty());
    }

    #[tokio::test]
    async fn test_update_field_metadata_invalid_path() {
        let mut dataset = test_dataset_nested().await;

        // Test updating non-existent field by path
        let result = dataset
            .update_field_metadata()
            .update("non_existent_field", [("key", "value")]);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::FieldNotFound { .. }));
        assert!(
            err.to_string()
                .contains("Field 'non_existent_field' not found"),
            "Expected error message to contain field name, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_update_field_metadata_syncs_unenforced_primary_key_position() {
        // Installing the unenforced primary key via field metadata must keep
        // the cached `unenforced_primary_key_position` in sync with the
        // metadata HashMap, otherwise the next commit drops the marker because
        // the protobuf is encoded from the cached option.
        use lance_core::datatypes::LANCE_UNENFORCED_PRIMARY_KEY_POSITION;

        let tmp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let uri = tmp_dir.as_str();
        let data = gen_batch()
            .col("a", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let mut dataset = Dataset::write(data, uri, None).await.unwrap();
        assert!(dataset.schema().unenforced_primary_key().is_empty());

        dataset
            .update_field_metadata()
            .update("a", [(LANCE_UNENFORCED_PRIMARY_KEY_POSITION, "1")])
            .unwrap()
            .await
            .unwrap();
        let a_field = dataset.schema().field("a").unwrap();
        assert_eq!(a_field.unenforced_primary_key_position, Some(1));

        // The marker is encoded from the cached option, so it must round-trip
        // through reopen.
        let reopened = Dataset::open(uri).await.unwrap();
        let pk = reopened.schema().unenforced_primary_key();
        assert_eq!(pk.len(), 1);
        assert_eq!(pk[0].name, "a");
    }

    #[tokio::test]
    async fn test_update_field_metadata_unenforced_primary_key_legacy_flag() {
        // The legacy boolean-flag form installs the primary key and syncs the
        // cached option; all accepted truthy spellings are recognized.
        use lance_core::datatypes::LANCE_UNENFORCED_PRIMARY_KEY;

        for truthy in ["true", "1", "yes", "TRUE", "Yes"] {
            let data = gen_batch()
                .col("a", array::step::<Int32Type>())
                .into_reader_rows(RowCount::from(10), BatchCount::from(1));
            let mut dataset = Dataset::write(data, "memory://", None).await.unwrap();
            dataset
                .update_field_metadata()
                .replace("a", [(LANCE_UNENFORCED_PRIMARY_KEY, truthy)])
                .unwrap()
                .await
                .unwrap();
            let a_field = dataset.schema().field("a").unwrap();
            assert_eq!(
                a_field.unenforced_primary_key_position,
                Some(0),
                "value {:?} should be treated as a PK marker",
                truthy
            );
        }
    }

    #[tokio::test]
    async fn test_update_field_metadata_unenforced_primary_key_non_numeric_position() {
        // A non-numeric position value falls back to the boolean-flag path
        // rather than panicking on parse.
        use lance_core::datatypes::{
            LANCE_UNENFORCED_PRIMARY_KEY, LANCE_UNENFORCED_PRIMARY_KEY_POSITION,
        };

        let data = gen_batch()
            .col("a", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let mut dataset = Dataset::write(data, "memory://", None).await.unwrap();
        dataset
            .update_field_metadata()
            .replace(
                "a",
                [
                    (LANCE_UNENFORCED_PRIMARY_KEY_POSITION, "not-a-number"),
                    (LANCE_UNENFORCED_PRIMARY_KEY, "true"),
                ],
            )
            .unwrap()
            .await
            .unwrap();
        let a_field = dataset.schema().field("a").unwrap();
        assert_eq!(a_field.unenforced_primary_key_position, Some(0));
    }

    #[tokio::test]
    async fn test_unenforced_primary_key_is_immutable() {
        // Once set, the unenforced primary key cannot be changed, re-set, or
        // removed: any commit that writes its reserved metadata keys, or that
        // alters the set of primary key columns, is rejected.
        use lance_core::datatypes::LANCE_UNENFORCED_PRIMARY_KEY_POSITION;

        let data = gen_batch()
            .col("a", array::step::<Int32Type>())
            .col("b", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let mut dataset = Dataset::write(data, "memory://", None).await.unwrap();

        // The first install of the primary key is allowed.
        dataset
            .update_field_metadata()
            .update("a", [(LANCE_UNENFORCED_PRIMARY_KEY_POSITION, "1")])
            .unwrap()
            .await
            .unwrap();
        assert_eq!(dataset.schema().unenforced_primary_key().len(), 1);

        // Re-applying the primary key, even to the identical column, is
        // rejected: the reserved key cannot be written once a key is set.
        let err = dataset
            .update_field_metadata()
            .update("a", [(LANCE_UNENFORCED_PRIMARY_KEY_POSITION, "1")])
            .unwrap()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "got {:?}", err);

        // Adding a second primary key column is rejected.
        let err = dataset
            .update_field_metadata()
            .update("b", [(LANCE_UNENFORCED_PRIMARY_KEY_POSITION, "2")])
            .unwrap()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "got {:?}", err);

        // Removing the primary key is rejected.
        let err = dataset
            .update_field_metadata()
            .replace("a", [] as [UpdateMapEntry; 0])
            .unwrap()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "got {:?}", err);

        // The primary key is unchanged after the rejected commits.
        let pk = dataset.schema().unenforced_primary_key();
        assert_eq!(pk.len(), 1);
        assert_eq!(pk[0].name, "a");
    }

    #[tokio::test]
    async fn test_unenforced_primary_key_rejects_invalid_marker() {
        // Writing a reserved primary key metadata key with a value that is not
        // a valid marker (e.g. a non-truthy flag) is rejected rather than
        // silently ignored.
        use lance_core::datatypes::LANCE_UNENFORCED_PRIMARY_KEY;

        let data = gen_batch()
            .col("a", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let mut dataset = Dataset::write(data, "memory://", None).await.unwrap();

        for invalid in ["no", "false", "0", "anything-else"] {
            let err = dataset
                .update_field_metadata()
                .replace("a", [(LANCE_UNENFORCED_PRIMARY_KEY, invalid)])
                .unwrap()
                .await
                .unwrap_err();
            assert!(
                matches!(err, Error::InvalidInput { .. }),
                "value {:?}: got {:?}",
                invalid,
                err
            );
            assert!(dataset.schema().unenforced_primary_key().is_empty());
        }
    }

    #[tokio::test]
    async fn test_update_field_metadata_invalid_id() {
        let mut dataset = test_dataset_nested().await;

        // Test updating with invalid field ID
        // Use an ID that's definitely invalid
        let invalid_id = 99999;

        // Create a builder and try to execute - this should eventually fail somewhere
        let result = async {
            dataset
                .update_field_metadata()
                .update(invalid_id, [("key", "value")])?
                .await
        }
        .await;

        assert!(matches!(result, Err(Error::InvalidInput { .. })));
    }

    #[tokio::test]
    async fn test_update_field_metadata_syncs_unenforced_clustering_key_position() {
        // Installing the unenforced clustering key via field metadata must keep
        // the cached `unenforced_clustering_key_position` in sync with the
        // metadata HashMap, otherwise the next commit drops the marker because
        // the protobuf is encoded from the cached option.
        use lance_core::datatypes::LANCE_UNENFORCED_CLUSTERING_KEY_POSITION;

        let tmp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let uri = tmp_dir.as_str();
        let data = gen_batch()
            .col("a", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let mut dataset = Dataset::write(data, uri, None).await.unwrap();
        assert!(dataset.schema().unenforced_clustering_key().is_empty());

        dataset
            .update_field_metadata()
            .update("a", [(LANCE_UNENFORCED_CLUSTERING_KEY_POSITION, "1")])
            .unwrap()
            .await
            .unwrap();
        let a_field = dataset.schema().field("a").unwrap();
        assert_eq!(a_field.unenforced_clustering_key_position, Some(1));

        // The marker is encoded from the cached option, so it must round-trip
        // through reopen.
        let reopened = Dataset::open(uri).await.unwrap();
        let ck = reopened.schema().unenforced_clustering_key();
        assert_eq!(ck.len(), 1);
        assert_eq!(ck[0].name, "a");
    }

    #[tokio::test]
    async fn test_update_field_metadata_unenforced_clustering_key_compound() {
        // A compound clustering key installs all of its columns, ordered by
        // position, and round-trips through reopen.
        use lance_core::datatypes::LANCE_UNENFORCED_CLUSTERING_KEY_POSITION;

        let tmp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let uri = tmp_dir.as_str();
        let data = gen_batch()
            .col("a", array::step::<Int32Type>())
            .col("b", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let mut dataset = Dataset::write(data, uri, None).await.unwrap();

        dataset
            .update_field_metadata()
            .update("b", [(LANCE_UNENFORCED_CLUSTERING_KEY_POSITION, "1")])
            .unwrap()
            .update("a", [(LANCE_UNENFORCED_CLUSTERING_KEY_POSITION, "2")])
            .unwrap()
            .await
            .unwrap();

        let reopened = Dataset::open(uri).await.unwrap();
        let ck = reopened.schema().unenforced_clustering_key();
        assert_eq!(ck.len(), 2);
        assert_eq!(ck[0].name, "b");
        assert_eq!(ck[1].name, "a");
    }

    #[tokio::test]
    async fn test_unenforced_clustering_key_is_immutable() {
        // Once set, the unenforced clustering key cannot be changed, re-set, or
        // removed: any commit that writes its reserved metadata key, or that
        // alters the set of clustering key columns, is rejected.
        use lance_core::datatypes::LANCE_UNENFORCED_CLUSTERING_KEY_POSITION;

        let data = gen_batch()
            .col("a", array::step::<Int32Type>())
            .col("b", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let mut dataset = Dataset::write(data, "memory://", None).await.unwrap();

        // The first install of the clustering key is allowed.
        dataset
            .update_field_metadata()
            .update("a", [(LANCE_UNENFORCED_CLUSTERING_KEY_POSITION, "1")])
            .unwrap()
            .await
            .unwrap();
        assert_eq!(dataset.schema().unenforced_clustering_key().len(), 1);

        // Re-applying the clustering key, even to the identical column, is
        // rejected: the reserved key cannot be written once a key is set.
        let err = dataset
            .update_field_metadata()
            .update("a", [(LANCE_UNENFORCED_CLUSTERING_KEY_POSITION, "1")])
            .unwrap()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "got {:?}", err);

        // Adding a second clustering key column is rejected.
        let err = dataset
            .update_field_metadata()
            .update("b", [(LANCE_UNENFORCED_CLUSTERING_KEY_POSITION, "2")])
            .unwrap()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "got {:?}", err);

        // Removing the clustering key is rejected.
        let err = dataset
            .update_field_metadata()
            .replace("a", [] as [UpdateMapEntry; 0])
            .unwrap()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "got {:?}", err);

        // The clustering key is unchanged after the rejected commits.
        let ck = dataset.schema().unenforced_clustering_key();
        assert_eq!(ck.len(), 1);
        assert_eq!(ck[0].name, "a");
    }

    #[tokio::test]
    async fn test_unenforced_clustering_key_rejects_invalid_marker() {
        // Writing the reserved clustering key metadata key with a value that is
        // not a valid position is rejected rather than silently ignored.
        use lance_core::datatypes::LANCE_UNENFORCED_CLUSTERING_KEY_POSITION;

        let data = gen_batch()
            .col("a", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let mut dataset = Dataset::write(data, "memory://", None).await.unwrap();

        for invalid in ["not-a-number", "", "1.5"] {
            let err = dataset
                .update_field_metadata()
                .replace("a", [(LANCE_UNENFORCED_CLUSTERING_KEY_POSITION, invalid)])
                .unwrap()
                .await
                .unwrap_err();
            assert!(
                matches!(err, Error::InvalidInput { .. }),
                "value {:?}: got {:?}",
                invalid,
                err
            );
            assert!(dataset.schema().unenforced_clustering_key().is_empty());
        }
    }
}
