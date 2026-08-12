// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! RED runtime contract tests for Native add-columns schema admission (B4h).
//!
//! Caller-authored Arrow field metadata under
//! [`crate::function::GENERATED_COLUMN_METADATA_KEY`] must not enter table
//! schema state through general-purpose Native `add_columns`. Generated
//! definitions are Job-owned. Schema-bearing transforms (`BatchUDF`, `Stream`,
//! `Reader`, `AllNulls`) currently accept and persist reserved top-level field
//! metadata; these tests pin the missing pre-consumption admission guard.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::{TryStreamExt, stream};
use lance::dataset::{BatchUDF, NewColumnTransform};
use tempfile::TempDir;

use crate::connection::ConnectBuilder;
use crate::error::Error;
use crate::function::{
    Function, FunctionArgument, FunctionCall, FunctionId, FunctionOutput, FunctionParameter,
    FunctionSignature, GENERATED_COLUMN_METADATA_KEY, GeneratedColumnDefinition,
};
use crate::query::{ExecutableQuery, QueryBase, Select};
use crate::table::Table;

const ID: &str = "id";
const ORDINARY: &str = "ordinary";
const GEN_OUT: &str = "gen_out";
const ORDINARY_META_KEY: &str = "unit";
const ORDINARY_META_VALUE: &str = "label";
const FN_ID: &str = "fn.exact.b4h.add_columns.literal";
const MALFORMED_MARKER: &str = "SENSITIVE_B4H_ADD_COLUMNS_METADATA_MARKER_4f8a_c3e2";

struct Fixture {
    _tmp: TempDir,
    table: Table,
}

/// Counts [`RecordBatchReader::next`] calls. [`RecordBatchReader::schema`] is free.
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

fn literal_definition(output_field_id: i32) -> GeneratedColumnDefinition {
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
                Arc::new(StringArray::from(vec![Some("literal-only")])) as arrow_array::ArrayRef
            )
            .unwrap(),
        )],
    )
    .unwrap();
    GeneratedColumnDefinition::try_new(output_field_id, call, 1, 1).unwrap()
}

fn valid_reserved_payload() -> String {
    literal_definition(1).to_metadata_json().unwrap()
}

fn malformed_reserved_payload() -> String {
    format!(
        r#"{{"format_version":1,"output_field_id":1,"function_call":"{MALFORMED_MARKER}","dependency_epoch":1,"materialized_epoch":1}}"#
    )
}

fn seed_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(ID, DataType::Int32, false),
        Field::new(ORDINARY, DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
        ],
    )
    .unwrap()
}

fn field_with_metadata(metadata: HashMap<String, String>) -> Field {
    Field::new(GEN_OUT, DataType::Int32, true).with_metadata(metadata)
}

fn reserved_field(payload: &str) -> Field {
    field_with_metadata(
        [(
            GENERATED_COLUMN_METADATA_KEY.to_string(),
            payload.to_string(),
        )]
        .into(),
    )
}

fn ordinary_metadata_field() -> Field {
    field_with_metadata(
        [(
            ORDINARY_META_KEY.to_string(),
            ORDINARY_META_VALUE.to_string(),
        )]
        .into(),
    )
}

fn values_batch(schema: SchemaRef, values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
}

fn boxed_reader(batch: RecordBatch) -> Box<dyn RecordBatchReader + Send> {
    let schema = batch.schema();
    Box::new(RecordBatchIterator::new(
        vec![Ok(batch)].into_iter(),
        schema,
    ))
}

fn observable_stream(
    batch: RecordBatch,
    yield_calls: Arc<AtomicUsize>,
) -> datafusion_physical_plan::SendableRecordBatchStream {
    let schema = batch.schema();
    let counter = yield_calls.clone();
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        stream::once(async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(batch)
        }),
    ))
}

fn assert_not_supported_redacted(err: &Error, label: &str, payload: &str) {
    match err {
        Error::NotSupported { message } => {
            let rendered = format!("{err}\n{err:?}\n{message}");
            assert!(
                !rendered.contains(GENERATED_COLUMN_METADATA_KEY),
                "{label}: leaked metadata wire key: {rendered}"
            );
            assert!(
                !rendered.contains(payload),
                "{label}: leaked raw payload: {rendered}"
            );
            assert!(
                !rendered.contains(FN_ID),
                "{label}: leaked Function ID: {rendered}"
            );
            assert!(
                !rendered.contains(GEN_OUT),
                "{label}: leaked output field name: {rendered}"
            );
            assert!(
                !rendered.contains(MALFORMED_MARKER),
                "{label}: leaked malformed marker: {rendered}"
            );
            assert!(
                message.to_lowercase().contains("generated")
                    || message.to_lowercase().contains("job"),
                "{label}: message must describe Job-owned generated-column boundary: {message}"
            );
        }
        other => panic!("{label}: expected Error::NotSupported, got {other:?}"),
    }
}

async fn create_table(name: &str) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let uri = tmp.path().to_str().unwrap().to_string();
    let conn = ConnectBuilder::new(&uri).execute().await.unwrap();
    let table = conn
        .create_table(name, seed_batch())
        .execute()
        .await
        .unwrap();
    Fixture { _tmp: tmp, table }
}

async fn snapshot_rows(table: &Table) -> Vec<(i32, String)> {
    let batches: Vec<RecordBatch> = table
        .query()
        .select(Select::columns(&[ID, ORDINARY]))
        .execute()
        .await
        .unwrap()
        .try_collect()
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
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), ordinary.value(i).to_string()));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    rows
}

async fn assert_table_unchanged(
    table: &Table,
    version_before: u64,
    schema_before: &Schema,
    rows_before: &[(i32, String)],
) {
    assert_eq!(table.version().await.unwrap(), version_before);
    let schema_after = table.schema().await.unwrap();
    assert_eq!(schema_after.as_ref(), schema_before);
    assert!(
        schema_after.field_with_name(GEN_OUT).is_err(),
        "rejected add_columns must leave column `{GEN_OUT}` absent"
    );
    assert_eq!(snapshot_rows(table).await, rows_before);
}

#[tokio::test]
async fn batch_udf_rejects_valid_reserved_before_mapper() {
    let fixture = create_table("b4h_batch_udf").await;
    let table = &fixture.table;
    let version_before = table.version().await.unwrap();
    let schema_before = table.schema().await.unwrap();
    let rows_before = snapshot_rows(table).await;

    let payload = valid_reserved_payload();
    let output_schema = Arc::new(Schema::new(vec![reserved_field(&payload)]));
    let mapper_schema = output_schema.clone();
    let mapper_calls = Arc::new(AtomicUsize::new(0));
    let calls = mapper_calls.clone();
    let udf = BatchUDF {
        mapper: Box::new(move |batch: &RecordBatch| {
            calls.fetch_add(1, Ordering::SeqCst);
            let values = Int32Array::from(vec![Some(10); batch.num_rows()]);
            Ok(RecordBatch::try_new(
                mapper_schema.clone(),
                vec![Arc::new(values)],
            )?)
        }),
        output_schema,
        result_checkpoint: None,
    };

    // Public Table::add_columns builder path.
    let err = table
        .add_columns()
        .transform(NewColumnTransform::BatchUDF(udf))
        .execute()
        .await
        .expect_err("BatchUDF must reject reserved generated-column metadata");
    assert_not_supported_redacted(&err, "BatchUDF reserved admission", &payload);
    assert_eq!(
        mapper_calls.load(Ordering::SeqCst),
        0,
        "rejection must occur before invoking the BatchUDF mapper"
    );
    assert_table_unchanged(table, version_before, schema_before.as_ref(), &rows_before).await;
}

#[tokio::test]
async fn stream_rejects_malformed_reserved_before_yield() {
    let fixture = create_table("b4h_stream").await;
    let table = &fixture.table;
    let version_before = table.version().await.unwrap();
    let schema_before = table.schema().await.unwrap();
    let rows_before = snapshot_rows(table).await;

    let payload = malformed_reserved_payload();
    assert!(payload.contains(MALFORMED_MARKER));
    let output_schema = Arc::new(Schema::new(vec![reserved_field(&payload)]));
    let yield_calls = Arc::new(AtomicUsize::new(0));
    let stream = observable_stream(
        values_batch(output_schema, vec![10, 20]),
        yield_calls.clone(),
    );

    // Direct experimental BaseTable::add_columns path.
    let err = table
        .base_table()
        .add_columns(NewColumnTransform::Stream(stream), None)
        .await
        .expect_err("Stream must reject reserved generated-column metadata");
    assert_not_supported_redacted(&err, "Stream reserved admission", &payload);
    assert_eq!(
        yield_calls.load(Ordering::SeqCst),
        0,
        "rejection must occur before polling/yielding the user Stream"
    );
    assert_table_unchanged(table, version_before, schema_before.as_ref(), &rows_before).await;
}

#[tokio::test]
async fn reader_rejects_valid_reserved_before_next() {
    let fixture = create_table("b4h_reader").await;
    let table = &fixture.table;
    let version_before = table.version().await.unwrap();
    let schema_before = table.schema().await.unwrap();
    let rows_before = snapshot_rows(table).await;

    let payload = valid_reserved_payload();
    let output_schema = Arc::new(Schema::new(vec![reserved_field(&payload)]));
    let next_calls = Arc::new(AtomicUsize::new(0));
    let reader = ObservableReader::wrap(
        boxed_reader(values_batch(output_schema, vec![10, 20])),
        next_calls.clone(),
    );

    let err = table
        .base_table()
        .add_columns(NewColumnTransform::Reader(reader), None)
        .await
        .expect_err("Reader must reject reserved generated-column metadata");
    assert_not_supported_redacted(&err, "Reader reserved admission", &payload);
    assert_eq!(
        next_calls.load(Ordering::SeqCst),
        0,
        "rejection must occur before RecordBatchReader::next"
    );
    assert_table_unchanged(table, version_before, schema_before.as_ref(), &rows_before).await;
}

#[tokio::test]
async fn all_nulls_rejects_malformed_reserved_before_commit() {
    let fixture = create_table("b4h_all_nulls").await;
    let table = &fixture.table;
    let version_before = table.version().await.unwrap();
    let schema_before = table.schema().await.unwrap();
    let rows_before = snapshot_rows(table).await;

    let payload = malformed_reserved_payload();
    let output_schema = Arc::new(Schema::new(vec![reserved_field(&payload)]));

    let err = table
        .add_columns()
        .transform(NewColumnTransform::AllNulls(output_schema))
        .execute()
        .await
        .expect_err("AllNulls must reject reserved generated-column metadata");
    assert_not_supported_redacted(&err, "AllNulls reserved admission", &payload);
    assert_table_unchanged(table, version_before, schema_before.as_ref(), &rows_before).await;
}

#[tokio::test]
async fn sql_expressions_add_columns_still_succeeds() {
    let fixture = create_table("b4h_sql_control").await;
    let table = &fixture.table;

    table
        .add_columns()
        .transform(NewColumnTransform::SqlExpressions(vec![(
            "doubled".into(),
            "id * 2".into(),
        )]))
        .execute()
        .await
        .expect("ordinary SqlExpressions add_columns must remain supported");

    let schema = table.schema().await.unwrap();
    assert!(schema.field_with_name("doubled").is_ok());
    assert!(schema.field_with_name(GEN_OUT).is_err());
    assert!(
        !schema
            .field_with_name("doubled")
            .unwrap()
            .metadata()
            .contains_key(GENERATED_COLUMN_METADATA_KEY)
    );
}

#[tokio::test]
async fn schema_bearing_ordinary_metadata_is_preserved() {
    let fixture = create_table("b4h_ordinary_meta").await;
    let table = &fixture.table;
    let output_schema = Arc::new(Schema::new(vec![ordinary_metadata_field()]));

    // AllNulls is schema-bearing and metadata-only; proves ordinary metadata
    // remains accepted so a later guard cannot reject every field metadata map.
    table
        .add_columns()
        .transform(NewColumnTransform::AllNulls(output_schema))
        .execute()
        .await
        .expect("ordinary non-reserved field metadata must remain accepted");

    let schema = table.schema().await.unwrap();
    let md = schema.field_with_name(GEN_OUT).unwrap().metadata();
    assert_eq!(
        md.get(ORDINARY_META_KEY).map(String::as_str),
        Some(ORDINARY_META_VALUE)
    );
    assert!(!md.contains_key(GENERATED_COLUMN_METADATA_KEY));
}
