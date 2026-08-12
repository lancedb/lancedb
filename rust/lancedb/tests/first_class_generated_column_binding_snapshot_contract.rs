// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Contract tests for GeneratedColumnBindingSnapshot (FF-029).
//!
//! Pins the hidden value projection and Table seam used by generated-column
//! call binding. These tests intentionally fail to compile until that API
//! exists. They do not submit Jobs, mutate generated-column state, or resolve
//! authored Function calls.

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lancedb::connect;
use lancedb::function::{GeneratedColumnBindingEntry, GeneratedColumnBindingSnapshot};
use lancedb::{Error, Result};
use tempfile::tempdir;

fn sample_fields() -> Vec<arrow_schema::FieldRef> {
    vec![
        Arc::new(Field::new("text", DataType::Utf8, true)),
        Arc::new(Field::new("score", DataType::Int32, false)),
        Arc::new(Field::new("a.b", DataType::Utf8, true)),
    ]
}

#[test]
fn try_new_preserves_version_order_and_exact_lookup() -> Result<()> {
    let fields = sample_fields();
    let snapshot = GeneratedColumnBindingSnapshot::try_new(7, fields.clone(), vec![3, 5, 9])?;

    assert_eq!(snapshot.version(), 7);
    let entries = snapshot.entries();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].field_id(), 3);
    assert_eq!(entries[0].field().name(), "text");
    assert_eq!(entries[0].field().data_type(), &DataType::Utf8);
    assert_eq!(entries[1].field_id(), 5);
    assert_eq!(entries[1].field().name(), "score");
    assert_eq!(entries[2].field_id(), 9);
    assert_eq!(entries[2].field().name(), "a.b");

    let by_name = snapshot.field("score").expect("exact name");
    assert_eq!(by_name.field_id(), 5);
    assert!(snapshot.field("Score").is_none());
    assert!(snapshot.field("a").is_none());
    let dotted = snapshot
        .field("a.b")
        .expect("literal dotted top-level name");
    assert_eq!(dotted.field_id(), 9);
    assert_eq!(dotted.field().as_ref(), fields[2].as_ref());

    // Type existence pin for the entry surface used by the next binding slice.
    let _: &GeneratedColumnBindingEntry = by_name;
    Ok(())
}

#[test]
fn try_new_rejects_invalid_projections() {
    let fields = sample_fields();

    assert!(matches!(
        GeneratedColumnBindingSnapshot::try_new(1, fields.clone(), vec![1, 2]),
        Err(Error::InvalidInput { .. })
    ));
    assert!(matches!(
        GeneratedColumnBindingSnapshot::try_new(1, fields.clone(), vec![1, 2, -1]),
        Err(Error::InvalidInput { .. })
    ));
    assert!(matches!(
        GeneratedColumnBindingSnapshot::try_new(1, fields.clone(), vec![1, 2, 1]),
        Err(Error::InvalidInput { .. })
    ));

    let duplicate_names = vec![
        Arc::new(Field::new("text", DataType::Utf8, true)),
        Arc::new(Field::new("text", DataType::Int32, false)),
    ];
    assert!(matches!(
        GeneratedColumnBindingSnapshot::try_new(1, duplicate_names, vec![1, 2]),
        Err(Error::InvalidInput { .. })
    ));
}

#[tokio::test]
async fn table_seam_returns_atomic_native_snapshot() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, true),
        Field::new("score", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("a")])),
            Arc::new(Int32Array::from(vec![1])),
        ],
    )?;
    let table = db.create_table("binding", batch).execute().await?;

    let snapshot = table.generated_column_binding_snapshot().await?;
    let public_schema = table.schema().await?;
    let version = table.version().await?;

    assert_eq!(snapshot.version(), version);
    assert_eq!(snapshot.entries().len(), public_schema.fields().len());
    for (entry, field) in snapshot.entries().iter().zip(public_schema.fields()) {
        assert_eq!(entry.field().name(), field.name());
        assert_eq!(entry.field().data_type(), field.data_type());
        assert!(entry.field_id() >= 0);
        assert!(!field.metadata().contains_key("lance:field_id"));
        assert!(!entry.field().metadata().contains_key("lance:field_id"));
    }
    Ok(())
}
