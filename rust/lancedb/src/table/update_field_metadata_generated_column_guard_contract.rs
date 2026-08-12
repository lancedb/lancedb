// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Runtime contract tests for the B4f reserved generated-metadata update guard.
//!
//! Pins that the general-purpose [`crate::table::Table::update_field_metadata`]
//! API cannot create, replace, or remove `GENERATED_COLUMN_METADATA_KEY`, and
//! that Native `replace()` cannot wipe an existing generated definition by
//! omitting the reserved key. Remote explicit-key attempts must reject before
//! transport.

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use tempfile::TempDir;

use crate::connection::ConnectBuilder;
use crate::error::Error;
use crate::function::{
    Function, FunctionArgument, FunctionCall, FunctionId, FunctionOutput, FunctionParameter,
    FunctionSignature, GENERATED_COLUMN_METADATA_KEY, GeneratedColumnDefinition,
};
use crate::query::{ExecutableQuery, QueryBase, Select};
use crate::table::Table;
use crate::table::schema_evolution::FieldMetadataUpdate;

const GEN_OUT: &str = "gen_out";
const ORDINARY: &str = "ordinary";
const CATEGORY: &str = "category";
const FN_ID: &str = "fn.exact.b4f.guard.literal";
const MALFORMED_MARKER: &str = "SENSITIVE_B4F_GUARD_METADATA_MARKER_7c1e_d04b";

struct Fixture {
    _tmp: TempDir,
    table: Table,
    uri: String,
}

fn literal_definition(
    output_field_id: i32,
    dependency_epoch: u64,
    materialized_epoch: u64,
) -> GeneratedColumnDefinition {
    let function = Function::new(
        FunctionId::try_new(FN_ID).unwrap(),
        FunctionSignature::try_new(
            vec![FunctionParameter::new("label", DataType::Utf8)],
            FunctionOutput::new(DataType::Int32, true),
        )
        .unwrap(),
    );
    let call = FunctionCall::try_new(
        &function,
        vec![(
            "label".to_string(),
            FunctionArgument::try_literal(
                Arc::new(StringArray::from(vec![Some("b4f-guard")])) as arrow_array::ArrayRef
            )
            .unwrap(),
        )],
    )
    .unwrap();
    GeneratedColumnDefinition::try_new(output_field_id, call, dependency_epoch, materialized_epoch)
        .unwrap()
}

async fn create_ordinary_table(name: &str) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let uri = tmp.path().to_str().unwrap().to_string();
    let conn = ConnectBuilder::new(&uri).execute().await.unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new(GEN_OUT, DataType::Int32, true),
        Field::new(ORDINARY, DataType::Utf8, true),
        Field::new(CATEGORY, DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("seed")])),
            Arc::new(StringArray::from(vec![Some("A")])),
        ],
    )
    .unwrap();
    let table = conn.create_table(name, batch).execute().await.unwrap();
    Fixture {
        _tmp: tmp,
        table,
        uri,
    }
}

async fn plant_generated_raw(table: &Table, column: &str, raw: String) {
    crate::table::schema_evolution::install_raw_generated_column_metadata_for_tests(
        table
            .as_native()
            .expect("generated-column fixture planting requires a Native table"),
        column,
        raw,
    )
    .await
    .expect("fixture raw generated-column metadata install must succeed");
}

async fn plant_valid_generated(table: &Table, column: &str) -> String {
    let snapshot = table.generated_column_binding_snapshot().await.unwrap();
    let field_id = snapshot.field(column).expect(column).field_id();
    let raw = literal_definition(field_id, 3, 3)
        .to_metadata_json()
        .unwrap();
    plant_generated_raw(table, column, raw.clone()).await;
    raw
}

async fn read_raw_generated_metadata(table: &Table, column: &str) -> Option<String> {
    let snapshot = table.generated_column_binding_snapshot().await.unwrap();
    snapshot
        .field(column)
        .expect(column)
        .field()
        .metadata()
        .get(GENERATED_COLUMN_METADATA_KEY)
        .cloned()
}

async fn ordinary_values(table: &Table) -> Vec<String> {
    let batches = table
        .query()
        .select(Select::columns(&[ORDINARY]))
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap().to_string())
        })
        .collect()
}

async fn reopen(uri: &str, name: &str) -> Table {
    ConnectBuilder::new(uri)
        .execute()
        .await
        .unwrap()
        .open_table(name)
        .execute()
        .await
        .unwrap()
}

fn assert_not_supported_redacted(err: &Error, label: &str, forbidden_substrings: &[&str]) {
    match err {
        Error::NotSupported { message } => {
            let rendered = format!("{err}\n{err:?}\n{message}");
            assert!(
                !rendered.contains(GENERATED_COLUMN_METADATA_KEY),
                "{label}: leaked metadata wire key: {rendered}"
            );
            assert!(
                !rendered.contains(FN_ID),
                "{label}: leaked Function ID: {rendered}"
            );
            assert!(
                !rendered.contains(MALFORMED_MARKER),
                "{label}: leaked malformed marker: {rendered}"
            );
            for needle in forbidden_substrings {
                assert!(
                    !rendered.contains(needle),
                    "{label}: leaked forbidden substring `{needle}`: {rendered}"
                );
            }
            assert!(
                message.to_lowercase().contains("generated")
                    || message.to_lowercase().contains("job"),
                "{label}: message must describe Job-owned generated-column boundary: {message}"
            );
        }
        other => panic!("{label}: expected Error::NotSupported, got {other:?}"),
    }
}

#[tokio::test]
async fn native_ordinary_field_explicit_reserved_key_set_rejects_and_preserves_state() {
    let fixture = create_ordinary_table("b4f_ordinary_set").await;
    let version_before = fixture.table.version().await.unwrap();
    let rows_before = ordinary_values(&fixture.table).await;
    let schema_before = fixture.table.schema().await.unwrap();
    let category_md_before = schema_before
        .field_with_name(CATEGORY)
        .unwrap()
        .metadata()
        .clone();

    let snapshot = fixture
        .table
        .generated_column_binding_snapshot()
        .await
        .unwrap();
    let field_id = snapshot.field(CATEGORY).expect(CATEGORY).field_id();
    let payload = literal_definition(field_id, 1, 1)
        .to_metadata_json()
        .unwrap();

    let err = fixture
        .table
        .update_field_metadata(&[
            FieldMetadataUpdate::new(CATEGORY).set(GENERATED_COLUMN_METADATA_KEY, payload.clone())
        ])
        .await
        .expect_err("explicit reserved-key set on ordinary field must reject");
    assert_not_supported_redacted(&err, "ordinary reserved set", &[&payload]);

    assert_eq!(fixture.table.version().await.unwrap(), version_before);
    assert_eq!(ordinary_values(&fixture.table).await, rows_before);
    let schema_after = fixture.table.schema().await.unwrap();
    assert_eq!(
        schema_after.field_with_name(CATEGORY).unwrap().metadata(),
        &category_md_before
    );
    assert!(
        read_raw_generated_metadata(&fixture.table, CATEGORY)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn native_generated_field_explicit_remove_rejects_and_preserves_raw() {
    let fixture = create_ordinary_table("b4f_gen_remove").await;
    let planted = plant_valid_generated(&fixture.table, GEN_OUT).await;
    let version_before = fixture.table.version().await.unwrap();

    let err = fixture
        .table
        .update_field_metadata(&[
            FieldMetadataUpdate::new(GEN_OUT).remove(GENERATED_COLUMN_METADATA_KEY)
        ])
        .await
        .expect_err("explicit reserved-key remove must reject");
    assert_not_supported_redacted(&err, "generated remove", &[&planted]);

    assert_eq!(fixture.table.version().await.unwrap(), version_before);
    assert_eq!(
        read_raw_generated_metadata(&fixture.table, GEN_OUT)
            .await
            .as_deref(),
        Some(planted.as_str())
    );
    let fresh = reopen(&fixture.uri, "b4f_gen_remove").await;
    assert_eq!(
        read_raw_generated_metadata(&fresh, GEN_OUT)
            .await
            .as_deref(),
        Some(planted.as_str())
    );
}

#[tokio::test]
async fn native_generated_field_explicit_replacement_rejects_and_preserves_raw() {
    let fixture = create_ordinary_table("b4f_gen_replace_value").await;
    let planted = plant_valid_generated(&fixture.table, GEN_OUT).await;
    let version_before = fixture.table.version().await.unwrap();
    let replacement = literal_definition(
        fixture
            .table
            .generated_column_binding_snapshot()
            .await
            .unwrap()
            .field(GEN_OUT)
            .unwrap()
            .field_id(),
        9,
        1,
    )
    .to_metadata_json()
    .unwrap();
    assert_ne!(replacement, planted);

    let err = fixture
        .table
        .update_field_metadata(&[FieldMetadataUpdate::new(GEN_OUT)
            .set(GENERATED_COLUMN_METADATA_KEY, replacement.clone())])
        .await
        .expect_err("explicit reserved-key replacement must reject");
    assert_not_supported_redacted(&err, "generated replace value", &[&planted, &replacement]);

    assert_eq!(fixture.table.version().await.unwrap(), version_before);
    let fresh = reopen(&fixture.uri, "b4f_gen_replace_value").await;
    assert_eq!(
        read_raw_generated_metadata(&fresh, GEN_OUT)
            .await
            .as_deref(),
        Some(planted.as_str())
    );
}

#[tokio::test]
async fn native_generated_field_replace_with_ordinary_metadata_rejects_and_preserves_raw() {
    let fixture = create_ordinary_table("b4f_gen_replace_map").await;
    let planted = plant_valid_generated(&fixture.table, GEN_OUT).await;
    let version_before = fixture.table.version().await.unwrap();

    let err = fixture
        .table
        .update_field_metadata(&[FieldMetadataUpdate::new(GEN_OUT)
            .replace()
            .set("unit", "label")])
        .await
        .expect_err("replace() that would wipe generated definition must reject");
    assert_not_supported_redacted(&err, "generated replace map", &[&planted]);

    assert_eq!(fixture.table.version().await.unwrap(), version_before);
    let fresh = reopen(&fixture.uri, "b4f_gen_replace_map").await;
    assert_eq!(
        read_raw_generated_metadata(&fresh, GEN_OUT)
            .await
            .as_deref(),
        Some(planted.as_str())
    );
}

#[tokio::test]
async fn native_mixed_batch_rejects_atomically_no_partial_commit() {
    let fixture = create_ordinary_table("b4f_mixed_batch").await;
    let planted = plant_valid_generated(&fixture.table, GEN_OUT).await;
    let version_before = fixture.table.version().await.unwrap();

    let err = fixture
        .table
        .update_field_metadata(&[
            FieldMetadataUpdate::new(CATEGORY).set("unit", "label"),
            FieldMetadataUpdate::new(GEN_OUT).remove(GENERATED_COLUMN_METADATA_KEY),
        ])
        .await
        .expect_err("mixed batch with forbidden update must reject all-or-none");
    assert_not_supported_redacted(&err, "mixed forbidden second", &[&planted]);

    assert_eq!(fixture.table.version().await.unwrap(), version_before);
    let schema = fixture.table.schema().await.unwrap();
    assert!(
        !schema
            .field_with_name(CATEGORY)
            .unwrap()
            .metadata()
            .contains_key("unit"),
        "ordinary metadata must not partially commit"
    );

    let err = fixture
        .table
        .update_field_metadata(&[
            FieldMetadataUpdate::new(GEN_OUT).set(GENERATED_COLUMN_METADATA_KEY, planted.clone()),
            FieldMetadataUpdate::new(CATEGORY).set("unit", "label"),
        ])
        .await
        .expect_err("mixed batch with forbidden update first must reject all-or-none");
    assert_not_supported_redacted(&err, "mixed forbidden first", &[&planted]);

    let fresh = reopen(&fixture.uri, "b4f_mixed_batch").await;
    assert_eq!(fresh.version().await.unwrap(), version_before);
    assert_eq!(
        read_raw_generated_metadata(&fresh, GEN_OUT)
            .await
            .as_deref(),
        Some(planted.as_str())
    );
    let fresh_schema = fresh.schema().await.unwrap();
    assert!(
        !fresh_schema
            .field_with_name(CATEGORY)
            .unwrap()
            .metadata()
            .contains_key("unit")
    );
}

#[tokio::test]
async fn native_malformed_generated_raw_replace_rejects_redacted_and_preserves_raw() {
    let fixture = create_ordinary_table("b4f_malformed_replace").await;
    let field_id = fixture
        .table
        .generated_column_binding_snapshot()
        .await
        .unwrap()
        .field(GEN_OUT)
        .unwrap()
        .field_id();
    let planted_raw = format!(
        r#"{{"format_version":1,"output_field_id":{field_id},"function_call":{MALFORMED_MARKER},"dependency_epoch":1,"materialized_epoch":1}}"#
    );
    assert!(planted_raw.contains(MALFORMED_MARKER));
    plant_generated_raw(&fixture.table, GEN_OUT, planted_raw.clone()).await;
    let version_before = fixture.table.version().await.unwrap();

    let err = fixture
        .table
        .update_field_metadata(&[FieldMetadataUpdate::new(GEN_OUT)
            .replace()
            .set("unit", "label")])
        .await
        .expect_err("malformed generated raw must still block replace()");
    assert_not_supported_redacted(&err, "malformed replace", &[&planted_raw]);

    assert_eq!(fixture.table.version().await.unwrap(), version_before);
    let fresh = reopen(&fixture.uri, "b4f_malformed_replace").await;
    assert_eq!(
        read_raw_generated_metadata(&fresh, GEN_OUT)
            .await
            .as_deref(),
        Some(planted_raw.as_str())
    );
}

#[tokio::test]
async fn native_ordinary_metadata_merge_set_remove_replace_still_work() {
    let fixture = create_ordinary_table("b4f_ordinary_controls").await;

    fixture
        .table
        .update_field_metadata(&[FieldMetadataUpdate::new(CATEGORY)
            .set("unit", "label")
            .set("pii", "false")])
        .await
        .unwrap();
    let md = fixture
        .table
        .schema()
        .await
        .unwrap()
        .field_with_name(CATEGORY)
        .unwrap()
        .metadata()
        .clone();
    assert_eq!(md.get("unit").map(String::as_str), Some("label"));
    assert_eq!(md.get("pii").map(String::as_str), Some("false"));

    fixture
        .table
        .update_field_metadata(&[FieldMetadataUpdate::new(CATEGORY)
            .set("source", "import")
            .remove("pii")])
        .await
        .unwrap();
    let md = fixture
        .table
        .schema()
        .await
        .unwrap()
        .field_with_name(CATEGORY)
        .unwrap()
        .metadata()
        .clone();
    assert_eq!(md.get("unit").map(String::as_str), Some("label"));
    assert_eq!(md.get("source").map(String::as_str), Some("import"));
    assert!(!md.contains_key("pii"));

    fixture
        .table
        .update_field_metadata(&[FieldMetadataUpdate::new(CATEGORY)
            .replace()
            .set("only", "kept")])
        .await
        .unwrap();
    let md = fixture
        .table
        .schema()
        .await
        .unwrap()
        .field_with_name(CATEGORY)
        .unwrap()
        .metadata()
        .clone();
    assert_eq!(md.len(), 1);
    assert_eq!(md.get("only").map(String::as_str), Some("kept"));
    assert!(
        read_raw_generated_metadata(&fixture.table, CATEGORY)
            .await
            .is_none()
    );
}

#[cfg(feature = "remote")]
mod remote_explicit_key_guard {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::Error;
    use crate::remote::{ClientConfig, HeaderProvider};

    #[derive(Debug)]
    struct CountingHeaderProvider {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl HeaderProvider for CountingHeaderProvider {
        async fn get_headers(&self) -> crate::Result<HashMap<String, String>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(HashMap::from([(
                "X-Test-Header".to_string(),
                "must-not-be-requested".to_string(),
            )]))
        }
    }

    fn panic_handler(
        calls: Arc<AtomicUsize>,
    ) -> impl Fn(reqwest::Request) -> http::Response<String> + Clone + Send + Sync + 'static {
        move |_request| {
            calls.fetch_add(1, Ordering::SeqCst);
            panic!("remote reserved-key update must not invoke the HTTP handler");
        }
    }

    #[tokio::test]
    async fn remote_explicit_set_rejects_before_handler_and_header_provider() {
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let header_calls = Arc::new(AtomicUsize::new(0));
        let config = ClientConfig {
            header_provider: Some(Arc::new(CountingHeaderProvider {
                calls: header_calls.clone(),
            }) as Arc<dyn HeaderProvider>),
            ..Default::default()
        };
        let table = Table::new_with_handler_and_config(
            "my_table",
            panic_handler(handler_calls.clone()),
            config,
        );

        let err = table
            .update_field_metadata(&[FieldMetadataUpdate::new(CATEGORY)
                .set(GENERATED_COLUMN_METADATA_KEY, r#"{"format_version":1}"#)])
            .await
            .expect_err("remote explicit reserved-key set must reject");
        assert!(
            matches!(err, Error::NotSupported { .. }),
            "expected NotSupported, got {err:?}"
        );
        assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
        assert_eq!(header_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn remote_explicit_remove_rejects_before_handler_and_header_provider() {
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let header_calls = Arc::new(AtomicUsize::new(0));
        let config = ClientConfig {
            header_provider: Some(Arc::new(CountingHeaderProvider {
                calls: header_calls.clone(),
            }) as Arc<dyn HeaderProvider>),
            ..Default::default()
        };
        let table = Table::new_with_handler_and_config(
            "my_table",
            panic_handler(handler_calls.clone()),
            config,
        );

        let err = table
            .update_field_metadata(&[
                FieldMetadataUpdate::new(CATEGORY).remove(GENERATED_COLUMN_METADATA_KEY)
            ])
            .await
            .expect_err("remote explicit reserved-key remove must reject");
        assert!(
            matches!(err, Error::NotSupported { .. }),
            "expected NotSupported, got {err:?}"
        );
        assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
        assert_eq!(header_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn remote_ordinary_metadata_update_sends_exact_body_and_succeeds() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/update_field_metadata/"
            );
            let body = request
                .body()
                .expect("ordinary update must send a body")
                .as_bytes()
                .expect("body is in-memory");
            let parsed: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(
                parsed,
                serde_json::json!({
                    "updates": [{
                        "path": "category",
                        "metadata": { "unit": "label" },
                        "replace": false
                    }]
                })
            );
            http::Response::builder()
                .status(200)
                .body(r#"{"version": 7}"#.to_string())
                .unwrap()
        });

        let result = table
            .update_field_metadata(&[FieldMetadataUpdate::new(CATEGORY).set("unit", "label")])
            .await
            .unwrap();
        assert_eq!(result.version, 7);
    }
}
