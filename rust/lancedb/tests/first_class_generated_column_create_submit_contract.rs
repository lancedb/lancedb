// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Contract tests for the hidden create-generated-column table submit seam
//! (FF-031).
//!
//! Scope is the Native/default `BaseTable`/`Table` submit path only: already-
//! bound `(source_table_version, CreateGeneratedColumnJobSpec)` must return
//! [`lancedb::Error::NotSupported`] without mutating dataset version or
//! schema. Remote transport and Job result decoding live in crate unit tests.

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lancedb::Error;
use lancedb::connect;
use lancedb::function::{
    CreateGeneratedColumnJobSpec, Function, FunctionArgument, FunctionCall, FunctionId,
    FunctionOutput, FunctionParameter, FunctionSignature,
};

fn sample_function() -> Function {
    let id = FunctionId::try_new("fn.exact.create-submit.native").expect("valid FunctionId");
    let signature = FunctionSignature::try_new(
        vec![
            FunctionParameter::new("x", DataType::Int32),
            FunctionParameter::new("label", DataType::Utf8),
        ],
        FunctionOutput::new(DataType::Int32, true),
    )
    .expect("valid FunctionSignature");
    Function::new(id, signature)
}

fn sample_spec() -> CreateGeneratedColumnJobSpec {
    let function = sample_function();
    let call = FunctionCall::try_new(
        &function,
        vec![
            (
                "x".to_string(),
                FunctionArgument::try_field(1, DataType::Int32).expect("field arg"),
            ),
            (
                "label".to_string(),
                FunctionArgument::try_literal(Arc::new(StringArray::from(vec![Some(
                    "SENSITIVE_CREATE_GEN_COL_LITERAL_MARKER",
                )])) as _)
                .expect("literal arg"),
            ),
        ],
    )
    .expect("valid FunctionCall");
    CreateGeneratedColumnJobSpec::try_new("normalized", &function, call).expect("valid spec")
}

/// Native tables reject create-generated-column submit without dataset change.
#[tokio::test]
async fn native_submit_create_generated_column_returns_not_supported_without_mutation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let conn = connect(dir.path().to_str().unwrap())
        .execute()
        .await
        .expect("local connect");

    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int32, false),
        Field::new("label", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("a")])),
        ],
    )
    .expect("batch");
    let table = conn
        .create_table("create_submit_native", batch)
        .execute()
        .await
        .expect("create table");

    let version_before = table.version().await.expect("version before");
    let schema_before = table.schema().await.expect("schema before");
    let field_names_before: Vec<_> = schema_before
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    let err = table
        .submit_create_generated_column(version_before, sample_spec())
        .await
        .expect_err("native submit must be unsupported");
    assert!(
        matches!(err, Error::NotSupported { .. }),
        "expected NotSupported, got {err:?}"
    );

    let version_after = table.version().await.expect("version after");
    let schema_after = table.schema().await.expect("schema after");
    let field_names_after: Vec<_> = schema_after
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        version_before, version_after,
        "unsupported submit must not advance dataset version"
    );
    assert_eq!(
        field_names_before, field_names_after,
        "unsupported submit must not change schema field names"
    );
    assert_eq!(
        schema_before.fields().len(),
        schema_after.fields().len(),
        "unsupported submit must not add schema fields"
    );
}
