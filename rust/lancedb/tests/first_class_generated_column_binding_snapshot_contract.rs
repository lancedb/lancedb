// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Contract tests for GeneratedColumnBindingSnapshot (FF-029 / FF-030).
//!
//! Pins the hidden value projection, Table seam, and bound-call field
//! validation used by generated-column call binding. These tests intentionally
//! fail to compile until that API exists. They do not submit Jobs, mutate
//! generated-column state, or resolve authored Function calls.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::NewColumnTransform;
use lancedb::connect;
use lancedb::function::{
    Function, FunctionArgument, FunctionCall, FunctionId, FunctionOutput, FunctionParameter,
    FunctionSignature, GeneratedColumnBindingEntry, GeneratedColumnBindingSnapshot,
};
use lancedb::table::ColumnAlteration;
use lancedb::{Error, Result};
use tempfile::tempdir;

fn sample_fields() -> Vec<arrow_schema::FieldRef> {
    vec![
        Arc::new(Field::new("text", DataType::Utf8, true)),
        Arc::new(Field::new("score", DataType::Int32, false)),
        Arc::new(Field::new("a.b", DataType::Utf8, true)),
    ]
}

fn sample_output() -> FunctionOutput {
    FunctionOutput::new(DataType::Int32, true)
}

/// Parameter names intentionally differ from table column names so any
/// name-based validation would fail these fixtures.
fn two_field_function() -> Result<Function> {
    let id = FunctionId::try_new("fn.exact.binding.validate")?;
    let signature = FunctionSignature::try_new(
        vec![
            FunctionParameter::new("input_payload", DataType::Utf8),
            FunctionParameter::new("metric_value", DataType::Int32),
        ],
        sample_output(),
    )?;
    Ok(Function::new(id, signature))
}

fn one_field_function() -> Result<Function> {
    let id = FunctionId::try_new("fn.exact.binding.one-field")?;
    let signature = FunctionSignature::try_new(
        vec![FunctionParameter::new("payload_arg", DataType::Utf8)],
        sample_output(),
    )?;
    Ok(Function::new(id, signature))
}

fn literal_only_function() -> Result<Function> {
    let id = FunctionId::try_new("fn.exact.binding.literal-only")?;
    let signature = FunctionSignature::try_new(
        vec![FunctionParameter::new("constant_arg", DataType::Int32)],
        sample_output(),
    )?;
    Ok(Function::new(id, signature))
}

fn mixed_function() -> Result<Function> {
    let id = FunctionId::try_new("fn.exact.binding.mixed")?;
    let signature = FunctionSignature::try_new(
        vec![
            FunctionParameter::new("payload_arg", DataType::Utf8),
            FunctionParameter::new("constant_arg", DataType::Int32),
        ],
        sample_output(),
    )?;
    Ok(Function::new(id, signature))
}

fn int_literal(value: Option<i32>) -> Result<FunctionArgument> {
    FunctionArgument::try_literal(Arc::new(Int32Array::from(vec![value])) as ArrayRef)
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

#[test]
fn validate_field_arguments_accepts_valid_and_mixed_bindings() -> Result<()> {
    let snapshot = GeneratedColumnBindingSnapshot::try_new(3, sample_fields(), vec![3, 5, 9])?;

    let one = one_field_function()?;
    let valid = FunctionCall::try_new(
        &one,
        vec![(
            "payload_arg".to_string(),
            FunctionArgument::try_field(3, DataType::Utf8)?,
        )],
    )?;
    snapshot.validate_field_arguments(&valid)?;

    let two = two_field_function()?;
    let multi = FunctionCall::try_new(
        &two,
        vec![
            (
                "input_payload".to_string(),
                FunctionArgument::try_field(3, DataType::Utf8)?,
            ),
            (
                "metric_value".to_string(),
                FunctionArgument::try_field(5, DataType::Int32)?,
            ),
        ],
    )?;
    snapshot.validate_field_arguments(&multi)?;

    let mixed_fn = mixed_function()?;
    let mixed = FunctionCall::try_new(
        &mixed_fn,
        vec![
            (
                "payload_arg".to_string(),
                FunctionArgument::try_field(3, DataType::Utf8)?,
            ),
            ("constant_arg".to_string(), int_literal(Some(42))?),
        ],
    )?;
    snapshot.validate_field_arguments(&mixed)?;
    Ok(())
}

#[test]
fn validate_field_arguments_rejects_missing_id_and_type_mismatch() -> Result<()> {
    let snapshot = GeneratedColumnBindingSnapshot::try_new(3, sample_fields(), vec![3, 5, 9])?;
    let one = one_field_function()?;

    let missing = FunctionCall::try_new(
        &one,
        vec![(
            "payload_arg".to_string(),
            FunctionArgument::try_field(99, DataType::Utf8)?,
        )],
    )?;
    let err = snapshot
        .validate_field_arguments(&missing)
        .expect_err("missing stable field id");
    assert!(matches!(err, Error::InvalidInput { .. }));
    let message = err.to_string();
    assert!(
        message.contains("99"),
        "diagnostics may name field id: {message}"
    );
    assert!(
        !message.contains("text") && !message.contains("score") && !message.contains("a.b"),
        "diagnostics must not invent or use a column name: {message}"
    );

    // Same stable ID, different Arrow type: exact-type equality must reject.
    // This covers Remote/other producer projections that keep the ID.
    let type_mismatch = FunctionCall::try_new(
        &one,
        vec![(
            "payload_arg".to_string(),
            FunctionArgument::try_field(5, DataType::Utf8)?,
        )],
    )?;
    let err = snapshot
        .validate_field_arguments(&type_mismatch)
        .expect_err("same-id exact type mismatch");
    assert!(matches!(err, Error::InvalidInput { .. }));
    let message = err.to_string();
    assert!(
        message.contains("5"),
        "diagnostics may name field id: {message}"
    );
    assert!(
        message.contains("Utf8") && message.contains("Int32"),
        "diagnostics may identify expected/current types: {message}"
    );
    assert!(
        !message.contains("score") && !message.contains("text"),
        "diagnostics must not invent or use a column name: {message}"
    );
    Ok(())
}

#[test]
fn validate_field_arguments_literal_only_ignores_table_fields() -> Result<()> {
    // Snapshot has no field that a name-based binder could match to "constant_arg".
    let snapshot = GeneratedColumnBindingSnapshot::try_new(
        1,
        vec![Arc::new(Field::new("unrelated", DataType::Utf8, true))],
        vec![11],
    )?;
    let function = literal_only_function()?;
    let call = FunctionCall::try_new(
        &function,
        vec![("constant_arg".to_string(), int_literal(Some(7))?)],
    )?;
    snapshot.validate_field_arguments(&call)?;
    Ok(())
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

#[tokio::test]
async fn validate_field_arguments_survives_rename_on_real_table() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    // Column names deliberately differ from Function parameter names.
    let schema = Arc::new(Schema::new(vec![
        Field::new("source_text", DataType::Utf8, true),
        Field::new("source_score", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("hello")])),
            Arc::new(Int32Array::from(vec![7])),
        ],
    )?;
    let table = db.create_table("binding_rename", batch).execute().await?;

    let before = table.generated_column_binding_snapshot().await?;
    let text_entry = before.field("source_text").expect("source_text");
    let score_entry = before.field("source_score").expect("source_score");
    let text_id = text_entry.field_id();
    let score_id = score_entry.field_id();
    assert_eq!(text_entry.field().data_type(), &DataType::Utf8);
    assert_eq!(score_entry.field().data_type(), &DataType::Int32);

    let function = two_field_function()?;
    let call = FunctionCall::try_new(
        &function,
        vec![
            (
                "input_payload".to_string(),
                FunctionArgument::try_field(text_id, DataType::Utf8)?,
            ),
            (
                "metric_value".to_string(),
                FunctionArgument::try_field(score_id, DataType::Int32)?,
            ),
        ],
    )?;
    before.validate_field_arguments(&call)?;

    table
        .alter_columns(&[ColumnAlteration::new("source_text".into()).rename("renamed_text".into())])
        .await?;

    let after = table.generated_column_binding_snapshot().await?;
    assert!(after.field("source_text").is_none());
    let renamed = after.field("renamed_text").expect("renamed_text");
    assert_eq!(renamed.field_id(), text_id);
    assert_eq!(renamed.field().data_type(), &DataType::Utf8);
    assert_eq!(
        after
            .field("source_score")
            .expect("source_score")
            .field_id(),
        score_id
    );
    after.validate_field_arguments(&call)?;
    Ok(())
}

#[tokio::test]
async fn validate_field_arguments_rejects_drop_recreate_same_name_type() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("keep_col", DataType::Int32, false),
        Field::new("bound_col", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("v")])),
        ],
    )?;
    let table = db
        .create_table("binding_drop_recreate", batch)
        .execute()
        .await?;

    let before = table.generated_column_binding_snapshot().await?;
    let bound = before.field("bound_col").expect("bound_col");
    let old_id = bound.field_id();
    assert_eq!(bound.field().data_type(), &DataType::Utf8);

    let function = one_field_function()?;
    let call = FunctionCall::try_new(
        &function,
        vec![(
            "payload_arg".to_string(),
            FunctionArgument::try_field(old_id, DataType::Utf8)?,
        )],
    )?;
    before.validate_field_arguments(&call)?;

    table.drop_columns(&["bound_col"]).await?;
    table
        .add_columns()
        .transform(NewColumnTransform::SqlExpressions(vec![(
            "bound_col".into(),
            "cast(NULL as string)".into(),
        )]))
        .execute()
        .await?;

    let after = table.generated_column_binding_snapshot().await?;
    let recreated = after.field("bound_col").expect("recreated bound_col");
    assert_eq!(recreated.field().data_type(), &DataType::Utf8);
    assert_ne!(
        recreated.field_id(),
        old_id,
        "drop/recreate must allocate a new stable field id"
    );
    let err = after
        .validate_field_arguments(&call)
        .expect_err("old call must not bind by name");
    assert!(matches!(err, Error::InvalidInput { .. }));
    let message = err.to_string();
    assert!(
        message.contains(&old_id.to_string()),
        "diagnostics may name missing field id: {message}"
    );
    assert!(
        !message.contains("bound_col") && !message.contains("keep_col"),
        "diagnostics must not invent or use a column name: {message}"
    );
    Ok(())
}

#[tokio::test]
async fn validate_field_arguments_rejects_cast_that_allocates_new_field_id() -> Result<()> {
    // Native Lance cast_to allocates a new stable field ID. The old bound call
    // must fail because that ID is absent. Same-ID exact-type mismatch is proved
    // separately via manually constructed snapshots (Remote/other producers).
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("label_col", DataType::Utf8, true),
        Field::new("metric_col", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("x")])),
            Arc::new(Int32Array::from(vec![3])),
        ],
    )?;
    let table = db
        .create_table("binding_type_change", batch)
        .execute()
        .await?;

    let before = table.generated_column_binding_snapshot().await?;
    let metric = before.field("metric_col").expect("metric_col");
    let old_metric_id = metric.field_id();
    assert_eq!(metric.field().data_type(), &DataType::Int32);

    let id = FunctionId::try_new("fn.exact.binding.type-change")?;
    let function = Function::new(
        id,
        FunctionSignature::try_new(
            vec![FunctionParameter::new("metric_value", DataType::Int32)],
            sample_output(),
        )?,
    );
    let call = FunctionCall::try_new(
        &function,
        vec![(
            "metric_value".to_string(),
            FunctionArgument::try_field(old_metric_id, DataType::Int32)?,
        )],
    )?;
    before.validate_field_arguments(&call)?;

    table
        .alter_columns(&[ColumnAlteration::new("metric_col".into()).cast_to(DataType::Int64)])
        .await?;

    let after = table.generated_column_binding_snapshot().await?;
    let casted = after.field("metric_col").expect("metric_col");
    assert_eq!(casted.field().data_type(), &DataType::Int64);
    assert_ne!(
        casted.field_id(),
        old_metric_id,
        "Native Lance cast_to must allocate a new stable field id"
    );
    let err = after
        .validate_field_arguments(&call)
        .expect_err("old call must fail because the prior stable field id is absent");
    assert!(matches!(err, Error::InvalidInput { .. }));
    let message = err.to_string();
    assert!(
        message.contains(&old_metric_id.to_string()),
        "diagnostics may name missing field id: {message}"
    );
    assert!(
        !message.contains("metric_col") && !message.contains("label_col"),
        "diagnostics must not invent or use a column name: {message}"
    );
    Ok(())
}
