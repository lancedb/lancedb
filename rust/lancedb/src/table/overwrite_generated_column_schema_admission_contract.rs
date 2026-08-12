// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! RED runtime contract tests for overwrite schema admission (B4i).
//!
//! Caller-authored Arrow field metadata under
//! [`crate::function::GENERATED_COLUMN_METADATA_KEY`] must not enter table
//! schema state through general-purpose overwrite. Generated definitions are
//! Job-owned. Both `AddDataBuilder` effective overwrite and direct/DataFusion
//! `BaseTable::create_insert_exec(..., WriteMode::Overwrite)` may currently
//! adopt caller input/plan schema on ordinary tables; these tests pin the
//! missing pre-consumption admission guard.
//!
//! Append is not schema replacement: caller field metadata discarded by
//! cast-to-table-schema must remain accepted.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use futures::{StreamExt, TryStreamExt};
use lance::dataset::{WriteMode, WriteParams};
use tempfile::TempDir;

use crate::arrow::SendableRecordBatchStream as LanceSendableRecordBatchStream;
use crate::connection::ConnectBuilder;
use crate::data::scannable::Scannable;
use crate::error::Error;
use crate::function::{
    Function, FunctionArgument, FunctionCall, FunctionId, FunctionOutput, FunctionParameter,
    FunctionSignature, GENERATED_COLUMN_METADATA_KEY, GeneratedColumnDefinition,
};
use crate::query::{ExecutableQuery, QueryBase, Select};
use crate::table::{AddDataMode, AddResult, Table, WriteOptions};

const ID: &str = "id";
const ORDINARY: &str = "ordinary";
const GEN_OUT: &str = "gen_out";
const FN_ID: &str = "fn.exact.b4i.overwrite.literal";
const MALFORMED_MARKER: &str = "SENSITIVE_B4I_OVERWRITE_METADATA_MARKER_7c1e_b9a4";

struct Fixture {
    _tmp: TempDir,
    table: Table,
}

/// Counts [`Scannable::scan_as_stream`] calls. [`Scannable::schema`] is free.
struct ObservableScannable {
    batch: RecordBatch,
    scan_calls: Arc<AtomicUsize>,
}

impl ObservableScannable {
    fn new(batch: RecordBatch, scan_calls: Arc<AtomicUsize>) -> Self {
        Self { batch, scan_calls }
    }
}

impl Scannable for ObservableScannable {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    fn scan_as_stream(&mut self) -> LanceSendableRecordBatchStream {
        self.scan_calls.fetch_add(1, Ordering::SeqCst);
        self.batch.scan_as_stream()
    }

    fn num_rows(&self) -> Option<usize> {
        Some(self.batch.num_rows())
    }

    fn rescannable(&self) -> bool {
        true
    }
}

/// Minimal single-partition plan that counts [`ExecutionPlan::execute`] calls.
#[derive(Debug)]
struct CountingExec {
    batch: RecordBatch,
    execute_calls: Arc<AtomicUsize>,
    properties: Arc<PlanProperties>,
}

impl CountingExec {
    fn new(batch: RecordBatch, execute_calls: Arc<AtomicUsize>) -> Self {
        let schema = batch.schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            batch,
            execute_calls,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for CountingExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CountingExec")
    }
}

impl ExecutionPlan for CountingExec {
    fn name(&self) -> &str {
        "CountingExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "CountingExec only supports partition 0, got {partition}"
            )));
        }
        self.execute_calls.fetch_add(1, Ordering::SeqCst);
        let batch = self.batch.clone();
        let schema = batch.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Ok(batch) }),
        )))
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

fn overwrite_schema_with_metadata(metadata: HashMap<String, String>) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(ID, DataType::Int32, false),
        Field::new(GEN_OUT, DataType::Int32, true).with_metadata(metadata),
    ]))
}

fn reserved_overwrite_batch(payload: &str, rows: Option<Vec<i32>>) -> RecordBatch {
    let schema = overwrite_schema_with_metadata(
        [(
            GENERATED_COLUMN_METADATA_KEY.to_string(),
            payload.to_string(),
        )]
        .into(),
    );
    match rows {
        Some(values) => {
            let n = values.len();
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from((0..n as i32).collect::<Vec<_>>())),
                    Arc::new(Int32Array::from(values)),
                ],
            )
            .unwrap()
        }
        None => RecordBatch::new_empty(schema),
    }
}

fn plain_overwrite_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(ID, DataType::Int32, false),
        Field::new(GEN_OUT, DataType::Int32, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![10])),
            Arc::new(Int32Array::from(vec![99])),
        ],
    )
    .unwrap()
}

fn append_batch_with_reserved_on_ordinary(payload: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(ID, DataType::Int32, false),
        Field::new(ORDINARY, DataType::Utf8, true).with_metadata(
            [(
                GENERATED_COLUMN_METADATA_KEY.to_string(),
                payload.to_string(),
            )]
            .into(),
        ),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![3])),
            Arc::new(StringArray::from(vec![Some("c")])),
        ],
    )
    .unwrap()
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

fn assert_schema_forged_reserved(schema: &Schema, payload: &str, label: &str) {
    let field = schema
        .field_with_name(GEN_OUT)
        .unwrap_or_else(|_| panic!("{label}: expected forged field `{GEN_OUT}`"));
    assert_eq!(
        field
            .metadata()
            .get(GENERATED_COLUMN_METADATA_KEY)
            .map(String::as_str),
        Some(payload),
        "{label}: overwrite must have persisted caller reserved metadata for RED evidence"
    );
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
        "rejected overwrite must leave column `{GEN_OUT}` absent"
    );
    assert_eq!(snapshot_rows(table).await, rows_before);
}

async fn execute_plan_to_completion(plan: Arc<dyn ExecutionPlan>) {
    let ctx = Arc::new(TaskContext::default());
    let mut stream = plan
        .execute(0, ctx)
        .expect("returned write plan must be executable");
    while let Some(item) = stream.next().await {
        item.expect("returned write plan must complete without stream error");
    }
}

/// A.1 Native AddDataMode::Overwrite with non-empty reserved source.
#[tokio::test]
async fn native_add_mode_overwrite_rejects_valid_reserved_before_scan() {
    let fixture = create_table("b4i_native_add_mode").await;
    let table = &fixture.table;
    let version_before = table.version().await.unwrap();
    let schema_before = table.schema().await.unwrap();
    let rows_before = snapshot_rows(table).await;

    let payload = valid_reserved_payload();
    let scan_calls = Arc::new(AtomicUsize::new(0));
    let data = ObservableScannable::new(
        reserved_overwrite_batch(&payload, Some(vec![10, 20])),
        scan_calls.clone(),
    );

    match table.add(data).mode(AddDataMode::Overwrite).execute().await {
        Err(err) => {
            assert_not_supported_redacted(&err, "native AddDataMode overwrite", &payload);
            assert_eq!(
                scan_calls.load(Ordering::SeqCst),
                0,
                "rejection must occur before Scannable::scan_as_stream"
            );
            assert_table_unchanged(table, version_before, schema_before.as_ref(), &rows_before)
                .await;
        }
        Ok(_) => {
            table.checkout_latest().await.unwrap();
            let forged = table.schema().await.unwrap();
            assert_schema_forged_reserved(
                forged.as_ref(),
                &payload,
                "native AddDataMode overwrite",
            );
            assert!(
                scan_calls.load(Ordering::SeqCst) > 0,
                "RED path scanned the reserved source while forging schema"
            );
            panic!(
                "RED: Native AddDataMode::Overwrite forged reserved generated-column metadata \
                 into table schema (scan_calls={})",
                scan_calls.load(Ordering::SeqCst)
            );
        }
    }
}

/// A.2 Native WriteParams Overwrite with empty/schema-only malformed reserved source.
#[tokio::test]
async fn native_add_write_params_overwrite_rejects_malformed_empty_before_scan() {
    let fixture = create_table("b4i_native_write_params").await;
    let table = &fixture.table;
    let version_before = table.version().await.unwrap();
    let schema_before = table.schema().await.unwrap();
    let rows_before = snapshot_rows(table).await;

    let payload = malformed_reserved_payload();
    assert!(payload.contains(MALFORMED_MARKER));
    let scan_calls = Arc::new(AtomicUsize::new(0));
    let data =
        ObservableScannable::new(reserved_overwrite_batch(&payload, None), scan_calls.clone());

    match table
        .add(data)
        .mode(AddDataMode::Append)
        .write_options(WriteOptions {
            lance_write_params: Some(WriteParams {
                mode: WriteMode::Overwrite,
                ..Default::default()
            }),
        })
        .execute()
        .await
    {
        Err(err) => {
            assert_not_supported_redacted(&err, "native WriteParams overwrite", &payload);
            assert_eq!(
                scan_calls.load(Ordering::SeqCst),
                0,
                "rejection must occur before Scannable::scan_as_stream"
            );
            assert_table_unchanged(table, version_before, schema_before.as_ref(), &rows_before)
                .await;
        }
        Ok(_) => {
            table.checkout_latest().await.unwrap();
            let forged = table.schema().await.unwrap();
            assert_schema_forged_reserved(
                forged.as_ref(),
                &payload,
                "native WriteParams empty overwrite",
            );
            panic!(
                "RED: Native WriteParams Overwrite forged reserved generated-column metadata \
                 into table schema from empty/schema-only input (scan_calls={})",
                scan_calls.load(Ordering::SeqCst)
            );
        }
    }
}

/// C.6 Native ordinary overwrite without reserved metadata still succeeds.
#[tokio::test]
async fn native_ordinary_overwrite_without_reserved_still_succeeds() {
    let fixture = create_table("b4i_native_ordinary_overwrite").await;
    let table = &fixture.table;
    let batch = plain_overwrite_batch();

    table
        .add(batch.clone())
        .mode(AddDataMode::Overwrite)
        .execute()
        .await
        .expect("ordinary overwrite without reserved metadata must remain supported");

    table.checkout_latest().await.unwrap();
    assert_eq!(table.count_rows(None).await.unwrap(), 1);
    let schema = table.schema().await.unwrap();
    assert_eq!(schema.as_ref(), batch.schema().as_ref());
    assert!(
        !schema
            .field_with_name(GEN_OUT)
            .unwrap()
            .metadata()
            .contains_key(GENERATED_COLUMN_METADATA_KEY)
    );
}

/// C.8 Native append with reserved input metadata on matching ordinary field.
#[tokio::test]
async fn native_append_reserved_input_metadata_is_discarded_and_accepted() {
    let fixture = create_table("b4i_native_append_control").await;
    let table = &fixture.table;
    let payload = valid_reserved_payload();
    let rows_before = snapshot_rows(table).await;

    table
        .add(append_batch_with_reserved_on_ordinary(&payload))
        .mode(AddDataMode::Append)
        .execute()
        .await
        .expect(
            "append with reserved input field metadata matching an ordinary table field must remain accepted",
        );

    table.checkout_latest().await.unwrap();
    let mut expected = rows_before;
    expected.push((3, "c".to_string()));
    assert_eq!(snapshot_rows(table).await, expected);

    let schema = table.schema().await.unwrap();
    assert!(
        !schema
            .field_with_name(ORDINARY)
            .unwrap()
            .metadata()
            .contains_key(GENERATED_COLUMN_METADATA_KEY),
        "append must leave persisted table schema free of the reserved key"
    );
    assert!(schema.field_with_name(GEN_OUT).is_err());
}

/// B.4 Native direct create_insert_exec Overwrite.
#[tokio::test]
async fn native_create_insert_exec_overwrite_rejects_reserved_before_plan_execution() {
    let fixture = create_table("b4i_native_insert_exec").await;
    let table = &fixture.table;
    let version_before = table.version().await.unwrap();
    let schema_before = table.schema().await.unwrap();
    let rows_before = snapshot_rows(table).await;

    let payload = valid_reserved_payload();
    let execute_calls = Arc::new(AtomicUsize::new(0));
    let input: Arc<dyn ExecutionPlan> = Arc::new(CountingExec::new(
        reserved_overwrite_batch(&payload, Some(vec![7])),
        execute_calls.clone(),
    ));
    let write_params = WriteParams {
        mode: WriteMode::Overwrite,
        ..Default::default()
    };

    match table
        .base_table()
        .create_insert_exec(input, write_params)
        .await
    {
        Err(err) => {
            assert_not_supported_redacted(&err, "native create_insert_exec overwrite", &payload);
            assert_eq!(
                execute_calls.load(Ordering::SeqCst),
                0,
                "rejection must occur before input plan execution"
            );
            assert_table_unchanged(table, version_before, schema_before.as_ref(), &rows_before)
                .await;
        }
        Ok(plan) => {
            assert_eq!(
                execute_calls.load(Ordering::SeqCst),
                0,
                "create_insert_exec itself must not execute the input plan"
            );
            execute_plan_to_completion(plan).await;
            assert!(
                execute_calls.load(Ordering::SeqCst) > 0,
                "RED must execute the returned overwrite plan against the input"
            );
            table.checkout_latest().await.unwrap();
            let forged = table.schema().await.unwrap();
            assert_schema_forged_reserved(
                forged.as_ref(),
                &payload,
                "native create_insert_exec overwrite",
            );
            panic!(
                "RED: Native create_insert_exec(Overwrite) returned and executed a plan that \
                 forged reserved generated-column metadata into table schema"
            );
        }
    }
}

/// C.9 Native direct create_insert_exec Append with reserved input metadata.
#[tokio::test]
async fn native_create_insert_exec_append_reserved_input_remains_supported() {
    let fixture = create_table("b4i_native_insert_exec_append").await;
    let table = &fixture.table;
    let payload = malformed_reserved_payload();
    let execute_calls = Arc::new(AtomicUsize::new(0));
    let input: Arc<dyn ExecutionPlan> = Arc::new(CountingExec::new(
        append_batch_with_reserved_on_ordinary(&payload),
        execute_calls.clone(),
    ));
    let write_params = WriteParams {
        mode: WriteMode::Append,
        ..Default::default()
    };

    let plan = table
        .base_table()
        .create_insert_exec(input, write_params)
        .await
        .expect(
            "direct Append create_insert_exec with reserved input metadata must remain supported",
        );
    execute_plan_to_completion(plan).await;
    assert!(execute_calls.load(Ordering::SeqCst) > 0);

    table.checkout_latest().await.unwrap();
    let schema = table.schema().await.unwrap();
    assert!(
        !schema
            .field_with_name(ORDINARY)
            .unwrap()
            .metadata()
            .contains_key(GENERATED_COLUMN_METADATA_KEY),
        "Append create_insert_exec must leave persisted table schema free of the reserved key"
    );
    let rows = snapshot_rows(table).await;
    assert!(
        rows.iter()
            .any(|(id, ordinary)| *id == 3 && ordinary == "c"),
        "Append create_insert_exec must append the reserved-metadata input rows; got {rows:?}"
    );
}

#[cfg(feature = "remote")]
mod remote_admission {
    use super::*;
    use std::io::Cursor;

    use arrow_ipc::reader::StreamReader;
    use async_trait::async_trait;
    use lance::arrow::json::JsonSchema;
    use serde_json::json;

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
                "X-B4i-Test".to_string(),
                "must-not-be-requested".to_string(),
            )]))
        }
    }

    fn ordinary_describe_body() -> String {
        let schema = Schema::new(vec![
            Field::new(ID, DataType::Int32, false),
            Field::new(ORDINARY, DataType::Utf8, true),
        ]);
        let json_schema = JsonSchema::try_from(&schema).unwrap();
        serde_json::to_string(&json!({
            "version": 1,
            "schema": json_schema,
        }))
        .unwrap()
    }

    fn extract_reserved_from_insert_body(body: &[u8]) -> Option<String> {
        let reader = StreamReader::try_new(Cursor::new(body), None).ok()?;
        let schema = reader.schema();
        schema
            .field_with_name(GEN_OUT)
            .ok()?
            .metadata()
            .get(GENERATED_COLUMN_METADATA_KEY)
            .cloned()
    }

    async fn assert_remote_effective_overwrite_reserved(
        use_add_data_mode: bool,
        empty_input: bool,
        payload: &str,
        table_name: &str,
        label: &str,
    ) {
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let header_calls = Arc::new(AtomicUsize::new(0));
        let scan_calls = Arc::new(AtomicUsize::new(0));
        let describe_calls = Arc::new(AtomicUsize::new(0));
        let insert_calls = Arc::new(AtomicUsize::new(0));
        let forged_payload = Arc::new(Mutex::new(None::<String>));

        let describe_body = ordinary_describe_body();
        let handler_calls_h = handler_calls.clone();
        let describe_calls_h = describe_calls.clone();
        let insert_calls_h = insert_calls.clone();
        let forged_payload_h = forged_payload.clone();
        let table_name_owned = table_name.to_string();
        let label_owned = label.to_string();
        let describe_path = format!("/v1/table/{table_name}/describe/");
        let insert_path = format!("/v1/table/{table_name}/insert/");

        let config = ClientConfig {
            header_provider: Some(Arc::new(CountingHeaderProvider {
                calls: header_calls.clone(),
            }) as Arc<dyn HeaderProvider>),
            ..Default::default()
        };

        let table = Table::new_with_handler_and_config(
            table_name_owned,
            move |request| {
                handler_calls_h.fetch_add(1, Ordering::SeqCst);
                let path = request.url().path();
                if path == describe_path {
                    describe_calls_h.fetch_add(1, Ordering::SeqCst);
                    return http::Response::builder()
                        .status(200)
                        .body(describe_body.clone())
                        .unwrap();
                }
                if path == insert_path {
                    insert_calls_h.fetch_add(1, Ordering::SeqCst);
                    let query = request.url().query().unwrap_or("");
                    assert!(
                        query.contains("mode=overwrite"),
                        "{}: expected mode=overwrite, got {query}",
                        label_owned
                    );
                    if let Some(body) = request.body().and_then(|b| b.as_bytes()) {
                        *forged_payload_h.lock().unwrap() = extract_reserved_from_insert_body(body);
                    }
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 2}"#.to_string())
                        .unwrap();
                }
                panic!("{}: unexpected HTTP path {path}", label_owned);
            },
            config,
        );

        let rows = if empty_input {
            None
        } else {
            Some(vec![11, 22])
        };
        let data =
            ObservableScannable::new(reserved_overwrite_batch(payload, rows), scan_calls.clone());

        let result: crate::Result<AddResult> = if use_add_data_mode {
            table.add(data).mode(AddDataMode::Overwrite).execute().await
        } else {
            table
                .add(data)
                .mode(AddDataMode::Append)
                .write_options(WriteOptions {
                    lance_write_params: Some(WriteParams {
                        mode: WriteMode::Overwrite,
                        ..Default::default()
                    }),
                })
                .execute()
                .await
        };

        match result {
            Err(err) => {
                assert_not_supported_redacted(&err, label, payload);
                assert_eq!(
                    scan_calls.load(Ordering::SeqCst),
                    0,
                    "{label}: rejection must occur before scan_as_stream"
                );
                assert_eq!(
                    header_calls.load(Ordering::SeqCst),
                    0,
                    "{label}: rejection must occur before header-provider invocation"
                );
                assert_eq!(
                    describe_calls.load(Ordering::SeqCst),
                    0,
                    "{label}: rejection must occur before schema/describe request"
                );
                assert_eq!(
                    insert_calls.load(Ordering::SeqCst),
                    0,
                    "{label}: rejection must occur before insert/multipart request"
                );
                assert_eq!(
                    handler_calls.load(Ordering::SeqCst),
                    0,
                    "{label}: rejection must occur before any HTTP handler call"
                );
            }
            Ok(_) => {
                let transmitted = forged_payload.lock().unwrap().clone().unwrap_or_else(|| {
                    panic!(
                        "{label}: RED overwrite succeeded but insert body did not carry \
                             reserved field metadata (handler_calls={}, describe={}, insert={}, \
                             scan={})",
                        handler_calls.load(Ordering::SeqCst),
                        describe_calls.load(Ordering::SeqCst),
                        insert_calls.load(Ordering::SeqCst),
                        scan_calls.load(Ordering::SeqCst)
                    )
                });
                assert_eq!(
                    transmitted.as_str(),
                    payload,
                    "{label}: insert Arrow IPC must transmit the reserved payload"
                );
                panic!(
                    "RED: {label} transmitted reserved generated-column metadata over overwrite \
                     HTTP (scan={}, header={}, describe={}, insert={})",
                    scan_calls.load(Ordering::SeqCst),
                    header_calls.load(Ordering::SeqCst),
                    describe_calls.load(Ordering::SeqCst),
                    insert_calls.load(Ordering::SeqCst)
                );
            }
        }
    }

    /// A.3 Remote AddDataMode overwrite (non-empty).
    #[tokio::test]
    async fn remote_add_mode_overwrite_rejects_valid_reserved_before_scan_headers_and_http() {
        assert_remote_effective_overwrite_reserved(
            true,
            false,
            &valid_reserved_payload(),
            "b4i_remote_add_mode",
            "remote AddDataMode overwrite reserved admission",
        )
        .await;
    }

    /// A.3 Remote WriteParams overwrite (empty/schema-only).
    #[tokio::test]
    async fn remote_write_params_overwrite_rejects_malformed_empty_before_scan_headers_and_http() {
        let payload = malformed_reserved_payload();
        assert!(payload.contains(MALFORMED_MARKER));
        assert_remote_effective_overwrite_reserved(
            false,
            true,
            &payload,
            "b4i_remote_write_params",
            "remote WriteParams empty overwrite reserved admission",
        )
        .await;
    }

    /// C.7 Remote ordinary overwrite without reserved metadata still succeeds.
    #[tokio::test]
    async fn remote_ordinary_overwrite_without_reserved_still_succeeds() {
        let insert_calls = Arc::new(AtomicUsize::new(0));
        let insert_calls_h = insert_calls.clone();
        let describe_body = ordinary_describe_body();
        let table = Table::new_with_handler("b4i_remote_ordinary", move |request| {
            let path = request.url().path();
            if path == "/v1/table/b4i_remote_ordinary/describe/" {
                return http::Response::builder()
                    .status(200)
                    .body(describe_body.clone())
                    .unwrap();
            }
            if path == "/v1/table/b4i_remote_ordinary/insert/" {
                let query = request.url().query().unwrap_or("");
                assert!(
                    query.contains("mode=overwrite"),
                    "ordinary remote overwrite must send mode=overwrite, got {query}"
                );
                if let Some(body) = request.body().and_then(|b| b.as_bytes()) {
                    let reader = StreamReader::try_new(Cursor::new(body), None).unwrap();
                    let schema = reader.schema();
                    assert!(
                        !schema
                            .field_with_name(GEN_OUT)
                            .unwrap()
                            .metadata()
                            .contains_key(GENERATED_COLUMN_METADATA_KEY)
                    );
                }
                insert_calls_h.fetch_add(1, Ordering::SeqCst);
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 2}"#.to_string())
                    .unwrap();
            }
            panic!("unexpected path {path}");
        });

        table
            .add(plain_overwrite_batch())
            .mode(AddDataMode::Overwrite)
            .execute()
            .await
            .expect("ordinary remote overwrite without reserved metadata must remain supported");
        assert_eq!(insert_calls.load(Ordering::SeqCst), 1);
    }

    /// B.5 Remote direct create_insert_exec Overwrite.
    #[tokio::test]
    async fn remote_create_insert_exec_overwrite_rejects_reserved_before_plan_execution() {
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let header_calls = Arc::new(AtomicUsize::new(0));
        let execute_calls = Arc::new(AtomicUsize::new(0));
        let insert_calls = Arc::new(AtomicUsize::new(0));
        let forged_payload = Arc::new(Mutex::new(None::<String>));

        let payload = valid_reserved_payload();
        let handler_calls_h = handler_calls.clone();
        let insert_calls_h = insert_calls.clone();
        let forged_payload_h = forged_payload.clone();
        let describe_body = ordinary_describe_body();

        let config = ClientConfig {
            header_provider: Some(Arc::new(CountingHeaderProvider {
                calls: header_calls.clone(),
            }) as Arc<dyn HeaderProvider>),
            ..Default::default()
        };

        let table = Table::new_with_handler_and_config(
            "b4i_remote_insert_exec",
            move |request| {
                handler_calls_h.fetch_add(1, Ordering::SeqCst);
                let path = request.url().path();
                if path == "/v1/table/b4i_remote_insert_exec/describe/" {
                    return http::Response::builder()
                        .status(200)
                        .body(describe_body.clone())
                        .unwrap();
                }
                if path == "/v1/table/b4i_remote_insert_exec/insert/" {
                    insert_calls_h.fetch_add(1, Ordering::SeqCst);
                    let query = request.url().query().unwrap_or("");
                    assert!(
                        query.contains("mode=overwrite"),
                        "expected mode=overwrite, got {query}"
                    );
                    if let Some(body) = request.body().and_then(|b| b.as_bytes()) {
                        *forged_payload_h.lock().unwrap() = extract_reserved_from_insert_body(body);
                    }
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 2}"#.to_string())
                        .unwrap();
                }
                panic!("unexpected HTTP path {path}");
            },
            config,
        );

        let input: Arc<dyn ExecutionPlan> = Arc::new(CountingExec::new(
            reserved_overwrite_batch(&payload, Some(vec![5])),
            execute_calls.clone(),
        ));
        let write_params = WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        };

        match table
            .base_table()
            .create_insert_exec(input, write_params)
            .await
        {
            Err(err) => {
                assert_not_supported_redacted(
                    &err,
                    "remote create_insert_exec overwrite",
                    &payload,
                );
                assert_eq!(execute_calls.load(Ordering::SeqCst), 0);
                assert_eq!(header_calls.load(Ordering::SeqCst), 0);
                assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
                assert_eq!(insert_calls.load(Ordering::SeqCst), 0);
            }
            Ok(plan) => {
                assert_eq!(
                    execute_calls.load(Ordering::SeqCst),
                    0,
                    "create_insert_exec itself must not execute the input plan"
                );
                assert_eq!(
                    handler_calls.load(Ordering::SeqCst),
                    0,
                    "create_insert_exec must not HTTP before plan execution in RED setup"
                );
                execute_plan_to_completion(plan).await;
                assert!(execute_calls.load(Ordering::SeqCst) > 0);
                assert!(insert_calls.load(Ordering::SeqCst) > 0);
                let transmitted = forged_payload.lock().unwrap().clone().unwrap_or_else(|| {
                    panic!(
                        "RED: remote create_insert_exec overwrite executed but insert body lacked \
                         reserved field metadata"
                    )
                });
                assert_eq!(transmitted.as_str(), payload);
                panic!(
                    "RED: Remote create_insert_exec(Overwrite) returned and executed a plan that \
                     transmitted reserved generated-column metadata over overwrite HTTP \
                     (execute_calls={}, insert_calls={}, header_calls={})",
                    execute_calls.load(Ordering::SeqCst),
                    insert_calls.load(Ordering::SeqCst),
                    header_calls.load(Ordering::SeqCst)
                );
            }
        }
    }
}
