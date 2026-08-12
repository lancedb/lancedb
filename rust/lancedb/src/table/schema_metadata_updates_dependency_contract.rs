// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Dependency-contract test for Lance A4 / A4u / A4d schema metadata attachment (B4p).
//!
//! Pins the exact generic Lance API shape LanceDB B4 will consume:
//! [`SchemaMetadataUpdates`], [`UpdateMap`], [`UpdateMapEntry`],
//! [`Transaction::with_schema_metadata_updates`], and the public
//! `with_schema_metadata_updates` methods on insert/update/delete builders.
//!
//! Also pins:
//! - A4u Update no-op: an attached field metadata patch must accompany a real
//!   data change; when a predicate matches zero rows, `rows_updated == 0` and
//!   the patch must not be published.
//! - A4d Delete no-op: when a predicate scans but deletes zero rows,
//!   `num_deleted_rows == 0` and the attached patch must not be published.
//!
//! Neutral metadata keys only. No Function / UDF / Job semantics.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use lance::Result;
use lance::dataset::transaction::{
    Operation, SchemaMetadataUpdates, Transaction, UpdateMap, UpdateMapEntry,
};
use lance::dataset::{Dataset, DeleteBuilder, InsertBuilder, UpdateBuilder};
use lance_table::format::Fragment;

const FIELD_ID: i32 = 7;
const META_KEY: &str = "b4p.dependency.meta";
const META_VALUE: &str = "neutral-value";

fn field_metadata_patch(field_id: i32) -> SchemaMetadataUpdates {
    SchemaMetadataUpdates {
        schema_metadata_updates: None,
        field_metadata_updates: HashMap::from([(
            field_id,
            UpdateMap {
                update_entries: vec![UpdateMapEntry {
                    key: META_KEY.to_string(),
                    value: Some(META_VALUE.to_string()),
                }],
                replace: false,
            },
        )]),
    }
}

async fn write_neutral_fixture(uri: &str) -> Dataset {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();
    Dataset::write(RecordBatchIterator::new(vec![Ok(batch)], schema), uri, None)
        .await
        .expect("fixture dataset must write")
}

/// Compile-time proof that InsertBuilder exposes the A4 attachment method.
#[allow(dead_code)]
fn typecheck_insert_builder_attachment<'a>(
    builder: InsertBuilder<'a>,
    updates: SchemaMetadataUpdates,
) -> Result<InsertBuilder<'a>> {
    builder.with_schema_metadata_updates(updates)
}

/// Compile-time proof that UpdateBuilder exposes the A4 attachment method.
#[allow(dead_code)]
fn typecheck_update_builder_attachment(
    builder: UpdateBuilder,
    updates: SchemaMetadataUpdates,
) -> Result<UpdateBuilder> {
    builder.with_schema_metadata_updates(updates)
}

/// Compile-time proof that DeleteBuilder exposes the A4 attachment method.
#[allow(dead_code)]
fn typecheck_delete_builder_attachment(
    builder: DeleteBuilder,
    updates: SchemaMetadataUpdates,
) -> Result<DeleteBuilder> {
    builder.with_schema_metadata_updates(updates)
}

#[test]
fn append_transaction_retains_schema_metadata_updates_patch() {
    let updates = field_metadata_patch(FIELD_ID);
    assert!(
        !updates.is_empty(),
        "fixture must be a substantive non-empty field metadata patch"
    );

    let transaction = Transaction::new(
        0,
        Operation::Append {
            fragments: vec![Fragment::new(1)],
        },
        None,
    )
    .with_schema_metadata_updates(updates.clone())
    .expect("non-empty field metadata patch must attach to Append");

    assert_eq!(transaction.schema_metadata_updates.as_ref(), Some(&updates));

    let field_map = transaction
        .schema_metadata_updates
        .as_ref()
        .expect("attached patch must be present")
        .field_metadata_updates
        .get(&FIELD_ID)
        .expect("stable field id 7 must be present");
    assert!(!field_map.replace);
    assert_eq!(field_map.update_entries.len(), 1);
    assert_eq!(field_map.update_entries[0].key, META_KEY);
    assert_eq!(
        field_map.update_entries[0].value.as_deref(),
        Some(META_VALUE)
    );
}

/// A4u dependency: a no-op Update (predicate matches zero rows) must not
/// publish an attached field metadata patch. Manifest version advancement is
/// unconstrained.
#[tokio::test]
async fn noop_update_does_not_publish_attached_field_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let uri = tmp.path().join("noop_update.lance");
    let uri = uri.to_str().unwrap();

    let dataset = write_neutral_fixture(uri).await;

    let field = dataset
        .schema()
        .field("value")
        .expect("value column must exist");
    let field_id = field.id;
    assert!(
        !field.metadata.contains_key(META_KEY),
        "{META_KEY} must be initially absent, got {:?}",
        field.metadata
    );

    let updates = field_metadata_patch(field_id);
    assert!(
        !updates.is_empty(),
        "fixture must be a substantive non-empty field metadata patch"
    );

    let before_count = dataset.count_rows(None).await.unwrap();
    assert_eq!(before_count, 3);

    let result = UpdateBuilder::new(Arc::new(dataset))
        .update_where("id < 0")
        .unwrap()
        .set("value", "'changed'")
        .unwrap()
        .with_schema_metadata_updates(updates)
        .expect("Update attachment must construct")
        .build()
        .unwrap()
        .execute()
        .await
        .expect("no-op attached Update must complete");

    assert_eq!(result.rows_updated, 0, "predicate must match zero rows");
    assert_eq!(
        result.new_dataset.count_rows(None).await.unwrap(),
        before_count,
        "row count must remain unchanged"
    );
    assert_eq!(
        result
            .new_dataset
            .count_rows(Some("value = 'changed'".into()))
            .await
            .unwrap(),
        0,
        "SET expression must not rewrite any rows"
    );
    assert_eq!(
        result
            .new_dataset
            .count_rows(Some("value IN ('a', 'b', 'c')".into()))
            .await
            .unwrap(),
        before_count,
        "original values must remain unchanged"
    );

    let reopened = Dataset::open(uri).await.unwrap();
    assert_eq!(reopened.count_rows(None).await.unwrap(), before_count);
    assert_eq!(
        reopened
            .count_rows(Some("value = 'changed'".into()))
            .await
            .unwrap(),
        0
    );
    let reopened_field = reopened
        .schema()
        .field_by_id(field_id)
        .expect("stable field id must still exist");
    assert!(
        !reopened_field.metadata.contains_key(META_KEY),
        "no-op Update must not publish attached field metadata; got {:?}",
        reopened_field.metadata.get(META_KEY)
    );
}

/// A4d dependency: a no-op Delete (predicate scans but matches zero rows) must
/// not publish an attached field metadata patch. Manifest version advancement
/// is unconstrained.
#[tokio::test]
async fn noop_delete_does_not_publish_attached_field_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let uri = tmp.path().join("noop_delete.lance");
    let uri = uri.to_str().unwrap();

    let dataset = write_neutral_fixture(uri).await;

    let field = dataset
        .schema()
        .field("value")
        .expect("value column must exist");
    let field_id = field.id;
    assert!(
        !field.metadata.contains_key(META_KEY),
        "{META_KEY} must be initially absent, got {:?}",
        field.metadata
    );

    let updates = field_metadata_patch(field_id);
    assert!(
        !updates.is_empty(),
        "fixture must be a substantive non-empty field metadata patch"
    );

    let before_count = dataset.count_rows(None).await.unwrap();
    assert_eq!(before_count, 3);

    let result = DeleteBuilder::new(Arc::new(dataset), "id < 0")
        .with_schema_metadata_updates(updates)
        .expect("Delete attachment must construct")
        .execute()
        .await
        .expect("no-op attached Delete must complete");

    assert_eq!(result.num_deleted_rows, 0, "predicate must match zero rows");
    assert_eq!(
        result.new_dataset.count_rows(None).await.unwrap(),
        before_count,
        "row count must remain unchanged"
    );
    assert_eq!(
        result
            .new_dataset
            .count_rows(Some("value IN ('a', 'b', 'c')".into()))
            .await
            .unwrap(),
        before_count,
        "original values must remain unchanged"
    );

    let returned_field = result
        .new_dataset
        .schema()
        .field_by_id(field_id)
        .expect("stable field id must still exist on returned dataset");
    assert!(
        !returned_field.metadata.contains_key(META_KEY),
        "no-op Delete must not publish attached field metadata on returned dataset; got {:?}",
        returned_field.metadata.get(META_KEY)
    );

    let reopened = Dataset::open(uri).await.unwrap();
    assert_eq!(reopened.count_rows(None).await.unwrap(), before_count);
    assert_eq!(
        reopened
            .count_rows(Some("value IN ('a', 'b', 'c')".into()))
            .await
            .unwrap(),
        before_count,
        "fresh open must preserve all original rows"
    );
    let reopened_field = reopened
        .schema()
        .field_by_id(field_id)
        .expect("stable field id must still exist");
    assert!(
        !reopened_field.metadata.contains_key(META_KEY),
        "no-op Delete must not publish attached field metadata; got {:?}",
        reopened_field.metadata.get(META_KEY)
    );
}
