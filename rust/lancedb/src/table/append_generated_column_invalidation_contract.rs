// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! RED runtime contract tests for Native append invalidation (B4b).
//!
//! These tests pin Native Table API and DataFusion SQL insert behavior for
//! generated-column dependency-epoch invalidation. They use real local Native
//! tables and existing public/internal APIs; Lance commits and query guards are
//! not mocked.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use lance::dataset::{WriteMode, WriteParams};
use tempfile::TempDir;

use crate::connection::ConnectBuilder;
use crate::error::{Error, FunctionErrorCode};
use crate::function::{
    Function, FunctionArgument, FunctionCall, FunctionId, FunctionOutput, FunctionParameter,
    FunctionSignature, GENERATED_COLUMN_METADATA_KEY, GeneratedColumnDefinition,
    GeneratedColumnStatus,
};
use crate::query::{ExecutableQuery, QueryBase, Select};
use crate::table::datafusion::BaseTableAdapter;
use crate::table::{AddDataMode, Table, WriteOptions};

const GEN_OUT: &str = "gen_out";
const ORDINARY: &str = "ordinary";
const INITIAL_DEPENDENCY_EPOCH: u64 = 3;
const INITIAL_MATERIALIZED_EPOCH: u64 = 3;
const MALFORMED_MARKER: &str = "SENSITIVE_B4B_APPEND_METADATA_MARKER_9f2c_a81d";

struct Fixture {
    _tmp: TempDir,
    table: Table,
    table_name: String,
    uri: String,
}

fn literal_only_function() -> Function {
    Function::new(
        FunctionId::try_new("fn.exact.b4b.append.literal").unwrap(),
        FunctionSignature::try_new(
            vec![FunctionParameter::new("label", DataType::Utf8)],
            FunctionOutput::new(DataType::Int32, true),
        )
        .unwrap(),
    )
}

fn literal_only_definition(
    output_field_id: i32,
    dependency_epoch: u64,
    materialized_epoch: u64,
) -> GeneratedColumnDefinition {
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
    assert!(
        call.arguments()
            .iter()
            .all(|(_, argument)| argument.field_id().is_none()),
        "fixture must be literal-only so row-set coverage, not field dependency, drives invalidation"
    );
    GeneratedColumnDefinition::try_new(output_field_id, call, dependency_epoch, materialized_epoch)
        .unwrap()
}

async fn create_table_with_complete_literal_generated(name: &str) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let uri = tmp.path().to_str().unwrap().to_string();
    let conn = ConnectBuilder::new(&uri).execute().await.unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new(GEN_OUT, DataType::Int32, true),
        Field::new(ORDINARY, DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("seed")])),
        ],
    )
    .unwrap();
    let table = conn.create_table(name, batch).execute().await.unwrap();

    let snapshot = table.generated_column_binding_snapshot().await.unwrap();
    let field_id = snapshot.field(GEN_OUT).expect(GEN_OUT).field_id();
    let definition = literal_only_definition(
        field_id,
        INITIAL_DEPENDENCY_EPOCH,
        INITIAL_MATERIALIZED_EPOCH,
    );
    let json = definition.to_metadata_json().unwrap();
    crate::table::schema_evolution::install_raw_generated_column_metadata_for_tests(
        table
            .as_native()
            .expect("generated-column fixture planting requires a Native table"),
        GEN_OUT,
        json,
    )
    .await
    .unwrap();

    assert_eq!(
        table.generated_column_status(GEN_OUT).await.unwrap(),
        GeneratedColumnStatus::Complete
    );
    let planted = read_generated_definition(&table).await;
    assert!(
        planted
            .function_call()
            .arguments()
            .iter()
            .all(|(_, argument)| argument.field_id().is_none()),
        "planted metadata must remain literal-only"
    );

    Fixture {
        _tmp: tmp,
        table,
        table_name: name.to_string(),
        uri,
    }
}

async fn create_ordinary_table(name: &str) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let uri = tmp.path().to_str().unwrap().to_string();
    let conn = ConnectBuilder::new(&uri).execute().await.unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new(GEN_OUT, DataType::Int32, true),
        Field::new(ORDINARY, DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("seed")])),
        ],
    )
    .unwrap();
    let table = conn.create_table(name, batch).execute().await.unwrap();
    Fixture {
        _tmp: tmp,
        table,
        table_name: name.to_string(),
        uri,
    }
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

fn ordinary_rows_batch(values: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        ORDINARY,
        DataType::Utf8,
        true,
    )]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(
            values.iter().map(|value| Some(*value)).collect::<Vec<_>>(),
        ))],
    )
    .unwrap()
}

fn full_rows_batch(gen_values: &[Option<i32>], ordinary_values: &[&str]) -> RecordBatch {
    assert_eq!(gen_values.len(), ordinary_values.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new(GEN_OUT, DataType::Int32, true),
        Field::new(ORDINARY, DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(gen_values.to_vec())),
            Arc::new(StringArray::from(
                ordinary_values
                    .iter()
                    .map(|value| Some(*value))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn ordinary_values(table: &Table) -> HashSet<String> {
    let batches = table
        .query()
        .select(Select::columns(&[ORDINARY]))
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut values = HashSet::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..column.len() {
            if !column.is_null(index) {
                values.insert(column.value(index).to_string());
            }
        }
    }
    values
}

fn assert_generated_column_incomplete(err: &Error, label: &str) {
    match err {
        Error::Function {
            code: FunctionErrorCode::GeneratedColumnIncomplete,
            ..
        } => {}
        other => panic!("{label}: expected generated_column_incomplete, got {other:?}"),
    }
}

fn assert_not_supported(err: &Error, label: &str) {
    assert!(
        matches!(err, Error::NotSupported { .. }),
        "{label}: expected NotSupported, got {err:?}"
    );
}

fn assert_invalid_input_redacted(err: &Error, label: &str) {
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
        !rendered.contains(GENERATED_COLUMN_METADATA_KEY),
        "{label}: diagnostic echoed metadata wire key: {rendered}"
    );
}

fn assert_conflict_error(err: &Error, label: &str) {
    match err {
        Error::Lance { source } => {
            assert!(
                matches!(
                    source,
                    lance::Error::IncompatibleTransaction { .. }
                        | lance::Error::RetryableCommitConflict { .. }
                        | lance::Error::CommitConflict { .. }
                ),
                "{label}: expected Lance commit conflict category, got {source:?}"
            );
        }
        Error::Function {
            code: FunctionErrorCode::StaleOrConflictingInput,
            ..
        } => {}
        other => panic!("{label}: expected conflict error category, got {other:?}"),
    }
}

fn from_datafusion_error(err: datafusion_common::DataFusionError) -> Error {
    Error::from(err)
}

async fn sql_ctx_for(table: &Table, name: &str) -> SessionContext {
    let ctx = SessionContext::new();
    let provider = BaseTableAdapter::try_new(table.base_table().clone())
        .await
        .unwrap();
    ctx.register_table(name, Arc::new(provider)).unwrap();
    ctx
}

async fn run_sql(ctx: &SessionContext, sql: &str) -> Result<(), Error> {
    match ctx.sql(sql).await {
        Err(err) => Err(from_datafusion_error(err)),
        Ok(df) => match df.collect().await {
            Ok(_) => Ok(()),
            Err(err) => Err(from_datafusion_error(err)),
        },
    }
}

#[tokio::test]
async fn nonempty_table_api_append_invalidates_literal_only_generated_column() {
    let fixture = create_table_with_complete_literal_generated("b4b_table_append").await;
    let before = read_generated_definition(&fixture.table).await;

    fixture
        .table
        .add(ordinary_rows_batch(&["appended"]))
        .execute()
        .await
        .expect("non-empty Table API append must commit");

    let values = ordinary_values(&fixture.table).await;
    assert!(values.contains("seed"));
    assert!(values.contains("appended"));

    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_OUT)
            .await
            .unwrap(),
        GeneratedColumnStatus::Incomplete
    );
    let after = read_generated_definition(&fixture.table).await;
    assert_eq!(after.dependency_epoch(), before.dependency_epoch() + 1);
    assert_eq!(after.materialized_epoch(), before.materialized_epoch());
    assert_eq!(after.output_field_id(), before.output_field_id());
    assert_eq!(after.function_call(), before.function_call());

    let Err(err) = fixture
        .table
        .query()
        .select(Select::columns(&[GEN_OUT]))
        .execute()
        .await
    else {
        panic!("incomplete generated column query must fail");
    };
    assert_generated_column_incomplete(&err, "table api append query");
}

#[tokio::test]
async fn nonempty_table_api_append_atomic_version_visibility() {
    let fixture = create_table_with_complete_literal_generated("b4b_atomic_visibility").await;
    let previous_version = fixture.table.version().await.unwrap();
    let previous_rows = ordinary_values(&fixture.table).await;
    let previous_definition = read_generated_definition(&fixture.table).await;
    assert_eq!(
        previous_definition.status(),
        GeneratedColumnStatus::Complete
    );

    fixture
        .table
        .add(ordinary_rows_batch(&["atomic-new"]))
        .execute()
        .await
        .expect("non-empty append must commit");
    let new_version = fixture.table.version().await.unwrap();
    assert_ne!(new_version, previous_version);

    // Exact new version: new rows + incomplete metadata together.
    let new_rows = ordinary_values(&fixture.table).await;
    assert!(new_rows.contains("atomic-new"));
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_OUT)
            .await
            .unwrap(),
        GeneratedColumnStatus::Incomplete
    );
    let new_definition = read_generated_definition(&fixture.table).await;
    assert_eq!(
        new_definition.dependency_epoch(),
        previous_definition.dependency_epoch() + 1
    );

    // Immediately previous version: neither new rows nor incomplete metadata.
    fixture.table.checkout(previous_version).await.unwrap();
    assert_eq!(ordinary_values(&fixture.table).await, previous_rows);
    assert!(!ordinary_values(&fixture.table).await.contains("atomic-new"));
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_OUT)
            .await
            .unwrap(),
        GeneratedColumnStatus::Complete
    );
    let checked_out = read_generated_definition(&fixture.table).await;
    assert_eq!(checked_out, previous_definition);
    fixture
        .table
        .query()
        .select(Select::columns(&[GEN_OUT]))
        .execute()
        .await
        .expect("previous complete version must remain readable");
}

#[tokio::test]
async fn empty_table_api_append_leaves_complete_generated_column() {
    let fixture = create_table_with_complete_literal_generated("b4b_empty_table_append").await;
    let before = read_generated_definition(&fixture.table).await;
    let rows_before = ordinary_values(&fixture.table).await;

    fixture
        .table
        .add(RecordBatch::new_empty(Arc::new(Schema::new(vec![
            Field::new(ORDINARY, DataType::Utf8, true),
        ]))))
        .execute()
        .await
        .expect("empty Table API append is a supported path");

    assert_eq!(ordinary_values(&fixture.table).await, rows_before);
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_OUT)
            .await
            .unwrap(),
        GeneratedColumnStatus::Complete
    );
    let after = read_generated_definition(&fixture.table).await;
    assert_eq!(after, before);
}

#[tokio::test]
async fn multipartition_table_api_append_advances_dependency_epoch_once() {
    let fixture = create_table_with_complete_literal_generated("b4b_multipartition").await;
    let before = read_generated_definition(&fixture.table).await;

    fixture
        .table
        .add(ordinary_rows_batch(&["p0", "p1", "p2", "p3"]))
        .write_parallelism(2)
        .execute()
        .await
        .expect("multi-partition append must commit");

    let values = ordinary_values(&fixture.table).await;
    assert!(values.contains("p0"));
    assert!(values.contains("p3"));
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_OUT)
            .await
            .unwrap(),
        GeneratedColumnStatus::Incomplete
    );
    let after = read_generated_definition(&fixture.table).await;
    assert_eq!(
        after.dependency_epoch(),
        before.dependency_epoch() + 1,
        "multi-partition append must attach one whole-transaction patch"
    );
    assert_eq!(after.materialized_epoch(), before.materialized_epoch());
    assert_eq!(after.function_call(), before.function_call());
}

#[tokio::test]
async fn table_api_overwrite_rejects_before_mutation_when_generated_column_present() {
    let fixture = create_table_with_complete_literal_generated("b4b_table_overwrite").await;
    let version_before = fixture.table.version().await.unwrap();
    let rows_before = ordinary_values(&fixture.table).await;
    let definition_before = read_generated_definition(&fixture.table).await;

    let err = fixture
        .table
        .add(full_rows_batch(&[Some(9)], &["overwrite"]))
        .mode(AddDataMode::Overwrite)
        .execute()
        .await
        .expect_err("overwrite must reject when any generated column is present");
    assert_not_supported(&err, "table api overwrite");

    assert_eq!(fixture.table.version().await.unwrap(), version_before);
    assert_eq!(ordinary_values(&fixture.table).await, rows_before);
    assert_eq!(
        read_generated_definition(&fixture.table).await,
        definition_before
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
async fn table_api_effective_overwrite_from_add_data_mode_rejects_when_lance_params_append() {
    let fixture =
        create_table_with_complete_literal_generated("b4b_table_effective_overwrite").await;
    let version_before = fixture.table.version().await.unwrap();
    let rows_before = ordinary_values(&fixture.table).await;
    let definition_before = read_generated_definition(&fixture.table).await;
    assert_eq!(definition_before.status(), GeneratedColumnStatus::Complete);

    let err = fixture
        .table
        .add(full_rows_batch(&[Some(9)], &["effective-overwrite"]))
        .mode(AddDataMode::Overwrite)
        .write_options(WriteOptions {
            lance_write_params: Some(WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            }),
        })
        .execute()
        .await
        .expect_err(
            "AddDataMode::Overwrite must reject generated-table writes even when \
             explicit lance WriteParams.mode is Append",
        );
    assert_not_supported(&err, "table api effective overwrite");

    assert_eq!(fixture.table.version().await.unwrap(), version_before);
    assert_eq!(ordinary_values(&fixture.table).await, rows_before);
    assert_eq!(
        read_generated_definition(&fixture.table).await,
        definition_before
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
async fn ordinary_table_api_overwrite_still_supported() {
    let fixture = create_ordinary_table("b4b_ordinary_overwrite_control").await;

    fixture
        .table
        .add(full_rows_batch(&[Some(42)], &["replaced"]))
        .mode(AddDataMode::Overwrite)
        .execute()
        .await
        .expect("ordinary tables must keep overwrite support");

    let values = ordinary_values(&fixture.table).await;
    assert_eq!(values, HashSet::from(["replaced".to_string()]));
    assert_eq!(fixture.table.count_rows(None).await.unwrap(), 1);
}

#[tokio::test]
async fn nonempty_sql_insert_invalidates_generated_column() {
    let fixture = create_table_with_complete_literal_generated("b4b_sql_insert").await;
    let before = read_generated_definition(&fixture.table).await;
    let ctx = sql_ctx_for(&fixture.table, &fixture.table_name).await;

    run_sql(
        &ctx,
        &format!(
            "INSERT INTO {} VALUES (CAST(NULL AS INT), 'sql-appended')",
            fixture.table_name
        ),
    )
    .await
    .expect("non-empty SQL INSERT must commit");

    fixture.table.checkout_latest().await.unwrap();
    let values = ordinary_values(&fixture.table).await;
    assert!(values.contains("sql-appended"));
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_OUT)
            .await
            .unwrap(),
        GeneratedColumnStatus::Incomplete
    );
    let after = read_generated_definition(&fixture.table).await;
    assert_eq!(after.dependency_epoch(), before.dependency_epoch() + 1);
    assert_eq!(after.materialized_epoch(), before.materialized_epoch());
    assert_eq!(after.function_call(), before.function_call());

    let Err(err) = fixture
        .table
        .query()
        .select(Select::columns(&[GEN_OUT]))
        .execute()
        .await
    else {
        panic!("SQL INSERT invalidation must trip generated query guard");
    };
    assert_generated_column_incomplete(&err, "sql insert query");
}

#[tokio::test]
async fn empty_sql_insert_leaves_complete_generated_column() {
    let fixture = create_table_with_complete_literal_generated("b4b_empty_sql_insert").await;
    let before = read_generated_definition(&fixture.table).await;
    let rows_before = ordinary_values(&fixture.table).await;

    let conn = ConnectBuilder::new(&fixture.uri).execute().await.unwrap();
    let source_schema = Arc::new(Schema::new(vec![
        Field::new(GEN_OUT, DataType::Int32, true),
        Field::new(ORDINARY, DataType::Utf8, true),
    ]));
    let empty_reader: Box<dyn arrow_array::RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(
            std::iter::empty::<Result<RecordBatch, arrow_schema::ArrowError>>(),
            source_schema,
        ));
    let source = conn
        .create_table("empty_source", empty_reader)
        .execute()
        .await
        .unwrap();

    let ctx = sql_ctx_for(&fixture.table, &fixture.table_name).await;
    let source_provider = BaseTableAdapter::try_new(source.base_table().clone())
        .await
        .unwrap();
    ctx.register_table("empty_source", Arc::new(source_provider))
        .unwrap();

    run_sql(
        &ctx,
        &format!(
            "INSERT INTO {} SELECT * FROM empty_source",
            fixture.table_name
        ),
    )
    .await
    .expect("empty SQL INSERT is a supported path");

    fixture.table.checkout_latest().await.unwrap();
    assert_eq!(ordinary_values(&fixture.table).await, rows_before);
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_OUT)
            .await
            .unwrap(),
        GeneratedColumnStatus::Complete
    );
    assert_eq!(read_generated_definition(&fixture.table).await, before);
}

#[tokio::test]
async fn sql_insert_overwrite_rejects_before_mutation_when_generated_column_present() {
    let fixture = create_table_with_complete_literal_generated("b4b_sql_overwrite").await;
    let version_before = fixture.table.version().await.unwrap();
    let rows_before = ordinary_values(&fixture.table).await;
    let definition_before = read_generated_definition(&fixture.table).await;
    let ctx = sql_ctx_for(&fixture.table, &fixture.table_name).await;

    let err = run_sql(
        &ctx,
        &format!(
            "INSERT OVERWRITE INTO {} VALUES (10, 'sql-overwrite')",
            fixture.table_name
        ),
    )
    .await
    .expect_err("SQL INSERT OVERWRITE must reject when any generated column is present");
    assert_not_supported(&err, "sql insert overwrite");

    fixture.table.checkout_latest().await.unwrap();
    assert_eq!(fixture.table.version().await.unwrap(), version_before);
    assert_eq!(ordinary_values(&fixture.table).await, rows_before);
    assert_eq!(
        read_generated_definition(&fixture.table).await,
        definition_before
    );
}

#[tokio::test]
async fn malformed_generated_metadata_rejects_append_before_mutation_and_redacts_marker() {
    let fixture = create_ordinary_table("b4b_malformed_preflight").await;
    let snapshot = fixture
        .table
        .generated_column_binding_snapshot()
        .await
        .unwrap();
    let field_id = snapshot.field(GEN_OUT).expect(GEN_OUT).field_id();
    let raw = format!(
        r#"{{"format_version":1,"output_field_id":{field_id},"function_call":{MALFORMED_MARKER},"dependency_epoch":1,"materialized_epoch":1}}"#
    );
    assert!(raw.contains(MALFORMED_MARKER));
    crate::table::schema_evolution::install_raw_generated_column_metadata_for_tests(
        fixture
            .table
            .as_native()
            .expect("generated-column fixture planting requires a Native table"),
        GEN_OUT,
        raw.clone(),
    )
    .await
    .unwrap();

    let version_before = fixture.table.version().await.unwrap();
    let rows_before = ordinary_values(&fixture.table).await;

    let err = fixture
        .table
        .add(ordinary_rows_batch(&["must-not-land"]))
        .execute()
        .await
        .expect_err("malformed generated metadata must fail closed before append visibility");
    assert_invalid_input_redacted(&err, "malformed append preflight");

    assert_eq!(fixture.table.version().await.unwrap(), version_before);
    assert_eq!(ordinary_values(&fixture.table).await, rows_before);
    assert!(
        !ordinary_values(&fixture.table)
            .await
            .contains("must-not-land")
    );
}

#[tokio::test]
async fn concurrent_same_field_append_one_winner_one_conflict() {
    let fixture = create_table_with_complete_literal_generated("b4b_concurrent_append").await;
    let conn = ConnectBuilder::new(&fixture.uri)
        .read_consistency_interval(Duration::from_secs(3600))
        .execute()
        .await
        .unwrap();
    let table_a = conn
        .open_table(&fixture.table_name)
        .execute()
        .await
        .unwrap();
    let table_b = conn
        .open_table(&fixture.table_name)
        .execute()
        .await
        .unwrap();
    let basis_version = table_a.version().await.unwrap();
    assert_eq!(table_b.version().await.unwrap(), basis_version);

    let (result_a, result_b) = tokio::join!(
        table_a.add(ordinary_rows_batch(&["winner-a"])).execute(),
        table_b.add(ordinary_rows_batch(&["winner-b"])).execute(),
    );

    let outcomes = [result_a, result_b];
    let wins = outcomes.iter().filter(|result| result.is_ok()).count();
    let losses = outcomes.iter().filter(|result| result.is_err()).count();
    assert_eq!(wins, 1, "exactly one same-basis append may publish");
    assert_eq!(losses, 1, "exactly one same-basis append must conflict");
    for result in &outcomes {
        if let Err(err) = result {
            assert_conflict_error(err, "concurrent same-field append loser");
        }
    }

    let fresh = conn
        .open_table(&fixture.table_name)
        .execute()
        .await
        .unwrap();
    let values = ordinary_values(&fresh).await;
    assert!(values.contains("seed"));
    let has_a = values.contains("winner-a");
    let has_b = values.contains("winner-b");
    assert!(
        has_a ^ has_b,
        "only winner rows may be visible, got {values:?}"
    );
    assert_eq!(
        fresh.generated_column_status(GEN_OUT).await.unwrap(),
        GeneratedColumnStatus::Incomplete
    );
    let definition = read_generated_definition(&fresh).await;
    assert_eq!(definition.dependency_epoch(), INITIAL_DEPENDENCY_EPOCH + 1);
    assert_eq!(definition.materialized_epoch(), INITIAL_MATERIALIZED_EPOCH);
}
