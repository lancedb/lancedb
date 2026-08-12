// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! RED runtime contract tests for Native merge-insert fail-closed guard (B4e).
//!
//! Tables with generated-column definitions cannot carry dependency-epoch
//! metadata updates through Native merge-insert in this slice. Both the
//! standard and MemWAL/LSM routes must reject before consuming source input or
//! mutating the table. Ordinary tables keep existing merge-insert semantics.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::{Int32Array, RecordBatch, RecordBatchReader, StringArray};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use tempfile::TempDir;

use crate::connection::ConnectBuilder;
use crate::error::Error;
use crate::function::{
    Function, FunctionArgument, FunctionCall, FunctionId, FunctionOutput, FunctionParameter,
    FunctionSignature, GENERATED_COLUMN_METADATA_KEY, GeneratedColumnDefinition,
    GeneratedColumnStatus,
};
use crate::query::{ExecutableQuery, QueryBase, Select};
use crate::table::Table;

const ID: &str = "id";
const ORDINARY: &str = "ordinary";
const GEN_OUT: &str = "gen_out";
const INITIAL_DEPENDENCY_EPOCH: u64 = 3;
const INITIAL_MATERIALIZED_EPOCH: u64 = 3;
const FN_ID: &str = "fn.exact.b4e.merge.literal";
const MALFORMED_MARKER: &str = "SENSITIVE_B4E_MERGE_METADATA_MARKER_7c91_e2ab";

struct Fixture {
    _tmp: TempDir,
    table: Table,
    table_name: String,
    uri: String,
}

/// RecordBatchReader that counts how many times [`Self::next`] is called.
struct ObservableReader {
    inner: Box<dyn RecordBatchReader + Send>,
    next_calls: Arc<AtomicUsize>,
}

impl ObservableReader {
    fn wrap(
        inner: Box<dyn RecordBatchReader + Send>,
        next_calls: Arc<AtomicUsize>,
    ) -> Box<dyn RecordBatchReader + Send> {
        Box::new(Self { inner, next_calls })
    }
}

impl Iterator for ObservableReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.next()
    }
}

impl RecordBatchReader for ObservableReader {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

fn literal_only_function() -> Function {
    Function::new(
        FunctionId::try_new(FN_ID).unwrap(),
        FunctionSignature::try_new(
            vec![FunctionParameter::new("label", DataType::Utf8)],
            FunctionOutput::new(DataType::Int32, true),
        )
        .unwrap(),
    )
}

fn literal_only_definition(output_field_id: i32) -> GeneratedColumnDefinition {
    let function = literal_only_function();
    let call = FunctionCall::try_new(
        &function,
        vec![(
            "label".to_string(),
            FunctionArgument::try_literal(
                Arc::new(StringArray::from(vec![Some("literal-only")])) as arrow_array::ArrayRef
            )
            .unwrap(),
        )],
    )
    .unwrap();
    GeneratedColumnDefinition::try_new(
        output_field_id,
        call,
        INITIAL_DEPENDENCY_EPOCH,
        INITIAL_MATERIALIZED_EPOCH,
    )
    .unwrap()
}

fn seed_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(ID, DataType::Int32, false),
        Field::new(ORDINARY, DataType::Utf8, true),
        Field::new(GEN_OUT, DataType::Int32, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            Arc::new(Int32Array::from(vec![10, 20])),
        ],
    )
    .unwrap()
}

fn source_batch(ids: &[i32], ordinary: &[&str], gen_values: &[i32]) -> RecordBatch {
    assert_eq!(ids.len(), ordinary.len());
    assert_eq!(ids.len(), gen_values.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new(ID, DataType::Int32, false),
        Field::new(ORDINARY, DataType::Utf8, true),
        Field::new(GEN_OUT, DataType::Int32, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(
                ordinary
                    .iter()
                    .map(|value| Some(*value))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(gen_values.to_vec())),
        ],
    )
    .unwrap()
}

fn boxed_reader(batch: RecordBatch) -> Box<dyn RecordBatchReader + Send> {
    let schema = batch.schema();
    Box::new(arrow_array::RecordBatchIterator::new(
        vec![Ok(batch)].into_iter(),
        schema,
    ))
}

fn empty_reader() -> Box<dyn RecordBatchReader + Send> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(ID, DataType::Int32, false),
        Field::new(ORDINARY, DataType::Utf8, true),
        Field::new(GEN_OUT, DataType::Int32, true),
    ]));
    Box::new(arrow_array::RecordBatchIterator::new(
        std::iter::empty::<Result<RecordBatch, ArrowError>>(),
        schema,
    ))
}

async fn create_ordinary_table(name: &str) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let uri = tmp.path().to_str().unwrap().to_string();
    let conn = ConnectBuilder::new(&uri).execute().await.unwrap();
    let table = conn
        .create_table(name, seed_batch())
        .execute()
        .await
        .unwrap();
    Fixture {
        _tmp: tmp,
        table,
        table_name: name.to_string(),
        uri,
    }
}

async fn create_table_with_complete_literal_generated(name: &str) -> Fixture {
    let fixture = create_ordinary_table(name).await;
    let snapshot = fixture
        .table
        .generated_column_binding_snapshot()
        .await
        .unwrap();
    let field_id = snapshot.field(GEN_OUT).expect(GEN_OUT).field_id();
    let definition = literal_only_definition(field_id);
    let json = definition.to_metadata_json().unwrap();
    crate::table::schema_evolution::install_raw_generated_column_metadata_for_tests(
        fixture
            .table
            .as_native()
            .expect("generated-column fixture planting requires a Native table"),
        GEN_OUT,
        json,
    )
    .await
    .unwrap();
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_OUT)
            .await
            .unwrap(),
        GeneratedColumnStatus::Complete
    );
    fixture
}

async fn read_generated_definition(table: &Table) -> GeneratedColumnDefinition {
    let snapshot = table.generated_column_binding_snapshot().await.unwrap();
    snapshot
        .field(GEN_OUT)
        .expect(GEN_OUT)
        .generated_column_definition()
        .expect("generated metadata must decode")
        .expect("generated metadata must be present")
}

async fn read_raw_generated_metadata(table: &Table) -> String {
    let snapshot = table.generated_column_binding_snapshot().await.unwrap();
    snapshot
        .field(GEN_OUT)
        .expect(GEN_OUT)
        .field()
        .metadata()
        .get(GENERATED_COLUMN_METADATA_KEY)
        .expect("generated metadata key must be present")
        .clone()
}

async fn ordinary_rows(table: &Table) -> Vec<(i32, String)> {
    let batches = table
        .query()
        .select(Select::columns(&[ID, ORDINARY]))
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let ordinary = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((ids.value(index), ordinary.value(index).to_string()));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    rows
}

fn assert_not_supported(err: &Error, label: &str) {
    assert!(
        matches!(err, Error::NotSupported { .. }),
        "{label}: expected NotSupported, got {err:?}"
    );
}

fn assert_invalid_input_redacted(err: &Error, planted_raw: &str, label: &str) {
    assert!(
        matches!(err, Error::InvalidInput { .. }),
        "{label}: expected InvalidInput, got {err:?}"
    );
    let rendered = format!("{err}\n{err:?}");
    assert!(
        !rendered.contains(MALFORMED_MARKER),
        "{label}: diagnostic echoed unique metadata marker: {rendered}"
    );
    assert!(
        !rendered.contains(FN_ID),
        "{label}: diagnostic echoed Function ID: {rendered}"
    );
    assert!(
        !rendered.contains(GENERATED_COLUMN_METADATA_KEY),
        "{label}: diagnostic echoed metadata wire key: {rendered}"
    );
    assert!(
        !rendered.contains(planted_raw),
        "{label}: diagnostic echoed raw metadata JSON: {rendered}"
    );
}

fn configure_standard_merge(builder: &mut crate::table::merge::MergeInsertBuilder) {
    builder
        .when_matched_update_all(None)
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete(None);
}

#[tokio::test]
async fn standard_merge_insert_rejects_when_generated_column_present_before_input_consumption() {
    let fixture = create_table_with_complete_literal_generated("b4e_standard_reject").await;
    let version_before = fixture.table.version().await.unwrap();
    let rows_before = ordinary_rows(&fixture.table).await;
    let definition_before = read_generated_definition(&fixture.table).await;
    let raw_before = read_raw_generated_metadata(&fixture.table).await;
    assert_eq!(
        definition_before.function_call().function_id().as_str(),
        FN_ID
    );

    let next_calls = Arc::new(AtomicUsize::new(0));
    let reader = ObservableReader::wrap(
        boxed_reader(source_batch(&[1, 3], &["updated", "inserted"], &[11, 30])),
        next_calls.clone(),
    );

    let mut builder = fixture.table.merge_insert(&[ID]);
    configure_standard_merge(&mut builder);
    let err = builder
        .execute(reader)
        .await
        .expect_err("generated-column table must reject standard merge_insert");
    assert_not_supported(&err, "standard merge_insert generated reject");
    assert_eq!(
        next_calls.load(Ordering::SeqCst),
        0,
        "rejection must occur before consuming the RecordBatchReader"
    );

    assert_eq!(fixture.table.version().await.unwrap(), version_before);
    assert_eq!(ordinary_rows(&fixture.table).await, rows_before);
    assert_eq!(
        read_generated_definition(&fixture.table).await,
        definition_before
    );
    assert_eq!(
        read_raw_generated_metadata(&fixture.table).await,
        raw_before
    );
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_OUT)
            .await
            .unwrap(),
        GeneratedColumnStatus::Complete
    );
}

#[tokio::test]
async fn empty_standard_merge_insert_rejects_when_generated_column_present() {
    let fixture = create_table_with_complete_literal_generated("b4e_empty_reject").await;
    let version_before = fixture.table.version().await.unwrap();
    let rows_before = ordinary_rows(&fixture.table).await;
    let raw_before = read_raw_generated_metadata(&fixture.table).await;

    let next_calls = Arc::new(AtomicUsize::new(0));
    let reader = ObservableReader::wrap(empty_reader(), next_calls.clone());

    let mut builder = fixture.table.merge_insert(&[ID]);
    configure_standard_merge(&mut builder);
    let err = builder
        .execute(reader)
        .await
        .expect_err("empty merge_insert must still reject on generated-column tables");
    assert_not_supported(&err, "empty standard merge_insert generated reject");
    assert_eq!(next_calls.load(Ordering::SeqCst), 0);

    assert_eq!(fixture.table.version().await.unwrap(), version_before);
    assert_eq!(ordinary_rows(&fixture.table).await, rows_before);
    assert_eq!(
        read_raw_generated_metadata(&fixture.table).await,
        raw_before
    );
}

#[tokio::test]
async fn forced_lsm_without_spec_rejects_generated_before_missing_spec_and_input() {
    let fixture = create_table_with_complete_literal_generated("b4e_lsm_force_reject").await;
    let version_before = fixture.table.version().await.unwrap();
    let rows_before = ordinary_rows(&fixture.table).await;
    let raw_before = read_raw_generated_metadata(&fixture.table).await;

    let next_calls = Arc::new(AtomicUsize::new(0));
    let reader = ObservableReader::wrap(
        boxed_reader(source_batch(&[1], &["must-not-land"], &[11])),
        next_calls.clone(),
    );

    let mut builder = fixture.table.merge_insert(&[ID]);
    builder
        .when_matched_update_all(None)
        .when_not_matched_insert_all()
        .use_lsm(true);
    let err = builder
        .execute(reader)
        .await
        .expect_err("generated-column guard must run before LSM missing-spec validation");
    assert_not_supported(&err, "forced LSM generated reject");
    assert_eq!(next_calls.load(Ordering::SeqCst), 0);

    assert_eq!(fixture.table.version().await.unwrap(), version_before);
    assert_eq!(ordinary_rows(&fixture.table).await, rows_before);
    assert_eq!(
        read_raw_generated_metadata(&fixture.table).await,
        raw_before
    );
}

#[tokio::test]
async fn malformed_generated_metadata_rejects_merge_insert_before_mutation_and_redacts() {
    let fixture = create_ordinary_table("b4e_malformed_preflight").await;
    let snapshot = fixture
        .table
        .generated_column_binding_snapshot()
        .await
        .unwrap();
    let field_id = snapshot.field(GEN_OUT).expect(GEN_OUT).field_id();
    let planted_raw = format!(
        r#"{{"format_version":1,"output_field_id":{field_id},"function_call":{{"function_id":"{FN_ID}","marker":"{MALFORMED_MARKER}"}},"dependency_epoch":1,"materialized_epoch":1}}"#
    );
    assert!(planted_raw.contains(MALFORMED_MARKER));
    assert!(planted_raw.contains(FN_ID));
    crate::table::schema_evolution::install_raw_generated_column_metadata_for_tests(
        fixture
            .table
            .as_native()
            .expect("generated-column fixture planting requires a Native table"),
        GEN_OUT,
        planted_raw.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        read_raw_generated_metadata(&fixture.table).await,
        planted_raw,
        "planted malformed raw metadata must round-trip byte-for-byte"
    );

    let version_before = fixture.table.version().await.unwrap();
    let rows_before = ordinary_rows(&fixture.table).await;
    let next_calls = Arc::new(AtomicUsize::new(0));
    let reader = ObservableReader::wrap(
        boxed_reader(source_batch(&[1], &["must-not-land"], &[99])),
        next_calls.clone(),
    );

    let mut builder = fixture.table.merge_insert(&[ID]);
    configure_standard_merge(&mut builder);
    let err = builder
        .execute(reader)
        .await
        .expect_err("malformed generated metadata must fail closed before merge_insert");
    assert_invalid_input_redacted(&err, &planted_raw, "malformed merge_insert preflight");
    assert_eq!(next_calls.load(Ordering::SeqCst), 0);

    let fresh = ConnectBuilder::new(&fixture.uri)
        .execute()
        .await
        .unwrap()
        .open_table(&fixture.table_name)
        .execute()
        .await
        .unwrap();
    assert_eq!(fresh.version().await.unwrap(), version_before);
    assert_eq!(ordinary_rows(&fresh).await, rows_before);
    assert_eq!(read_raw_generated_metadata(&fresh).await, planted_raw);
}

#[tokio::test]
async fn ordinary_table_standard_merge_insert_preserves_result_semantics() {
    let fixture = create_ordinary_table("b4e_ordinary_standard").await;
    let mut builder = fixture.table.merge_insert(&[ID]);
    configure_standard_merge(&mut builder);
    let result = builder
        .execute(boxed_reader(source_batch(
            &[1, 3],
            &["updated", "inserted"],
            &[11, 30],
        )))
        .await
        .expect("ordinary-table standard merge_insert must succeed");

    assert_eq!(result.num_inserted_rows, 1);
    assert_eq!(result.num_updated_rows, 1);
    assert_eq!(result.num_deleted_rows, 1);
    assert_eq!(result.num_attempts, 1);
    assert_eq!(result.num_rows, 2);
    assert!(result.version > 0);

    assert_eq!(
        ordinary_rows(&fixture.table).await,
        vec![(1, "updated".to_string()), (3, "inserted".to_string()),]
    );
}

#[tokio::test]
async fn ordinary_table_forced_lsm_without_spec_keeps_missing_spec_error() {
    let fixture = create_ordinary_table("b4e_ordinary_lsm_missing_spec").await;
    let version_before = fixture.table.version().await.unwrap();
    let rows_before = ordinary_rows(&fixture.table).await;

    let mut builder = fixture.table.merge_insert(&[ID]);
    builder
        .when_matched_update_all(None)
        .when_not_matched_insert_all()
        .use_lsm(true);
    let err = builder
        .execute(boxed_reader(source_batch(&[1], &["x"], &[1])))
        .await
        .expect_err("ordinary table without MemWAL spec must keep missing-spec InvalidInput");
    match err {
        Error::InvalidInput { message } => {
            assert!(
                message.contains("no MemWAL write spec"),
                "expected missing-spec message, got {message}"
            );
        }
        other => panic!("expected InvalidInput missing-spec, got {other:?}"),
    }

    assert_eq!(fixture.table.version().await.unwrap(), version_before);
    assert_eq!(ordinary_rows(&fixture.table).await, rows_before);
}
