// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! RED runtime contract tests for Native update invalidation (B4c).
//!
//! These tests pin Native Table update-builder behavior for generated-column
//! dependency-epoch invalidation. They use real local Native tables and existing
//! public APIs; Lance commits, the B4a planner, query guards, and concurrency are
//! not mocked.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use tempfile::TempDir;

use crate::connection::ConnectBuilder;
use crate::error::{Error, FunctionErrorCode};
use crate::function::{
    Function, FunctionArgument, FunctionCall, FunctionId, FunctionOutput, FunctionParameter,
    FunctionSignature, GENERATED_COLUMN_METADATA_KEY, GeneratedColumnDefinition,
    GeneratedColumnStatus,
};
use crate::query::{ExecutableQuery, QueryBase, Select};
use crate::table::Table;

const ID: &str = "id";
const INPUT_A: &str = "input_a";
const INPUT_B: &str = "input_b";
const ORDINARY: &str = "ordinary";
const GEN_DIRECT: &str = "gen_direct";
const GEN_TRANSITIVE: &str = "gen_transitive";
const GEN_UNRELATED: &str = "gen_unrelated";

const INITIAL_DEPENDENCY_EPOCH: u64 = 3;
const INITIAL_MATERIALIZED_EPOCH: u64 = 3;
const MALFORMED_MARKER: &str = "SENSITIVE_B4C_UPDATE_METADATA_MARKER_3e81_c4f0";

const SEED_ID: i32 = 1;
const SEED_INPUT_A: i32 = 10;
const SEED_INPUT_B: i32 = 20;
const SEED_ORDINARY: &str = "seed";
const SEED_GEN_DIRECT: i32 = 100;
const SEED_GEN_TRANSITIVE: i32 = 200;
const SEED_GEN_UNRELATED: i32 = 300;

struct Fixture {
    _tmp: TempDir,
    table: Table,
    table_name: String,
    uri: String,
    input_a_field_id: i32,
    input_b_field_id: i32,
    gen_direct_field_id: i32,
    gen_transitive_field_id: i32,
    gen_unrelated_field_id: i32,
}

fn int_field_function(function_id: &str) -> Function {
    Function::new(
        FunctionId::try_new(function_id).unwrap(),
        FunctionSignature::try_new(
            vec![FunctionParameter::new("upstream", DataType::Int32)],
            FunctionOutput::new(DataType::Int32, true),
        )
        .unwrap(),
    )
}

fn int_field_bound_call(function_id: &str, input_field_id: i32) -> FunctionCall {
    FunctionCall::try_new(
        &int_field_function(function_id),
        vec![(
            "upstream".to_string(),
            FunctionArgument::try_field(input_field_id, DataType::Int32).unwrap(),
        )],
    )
    .unwrap()
}

fn field_bound_definition(
    function_id: &str,
    output_field_id: i32,
    input_field_id: i32,
) -> GeneratedColumnDefinition {
    GeneratedColumnDefinition::try_new(
        output_field_id,
        int_field_bound_call(function_id, input_field_id),
        INITIAL_DEPENDENCY_EPOCH,
        INITIAL_MATERIALIZED_EPOCH,
    )
    .unwrap()
}

fn sole_field_argument_id(definition: &GeneratedColumnDefinition) -> i32 {
    let args = definition.function_call().arguments();
    assert_eq!(
        args.len(),
        1,
        "fixture calls must bind exactly one argument"
    );
    args[0]
        .1
        .field_id()
        .expect("fixture calls must be field-bound")
}

async fn create_dependent_generated_fixture(name: &str) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let uri = tmp.path().to_str().unwrap().to_string();
    let conn = ConnectBuilder::new(&uri).execute().await.unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new(ID, DataType::Int32, false),
        Field::new(INPUT_A, DataType::Int32, true),
        Field::new(INPUT_B, DataType::Int32, true),
        Field::new(ORDINARY, DataType::Utf8, true),
        Field::new(GEN_DIRECT, DataType::Int32, true),
        Field::new(GEN_TRANSITIVE, DataType::Int32, true),
        Field::new(GEN_UNRELATED, DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![SEED_ID])),
            Arc::new(Int32Array::from(vec![SEED_INPUT_A])),
            Arc::new(Int32Array::from(vec![SEED_INPUT_B])),
            Arc::new(StringArray::from(vec![Some(SEED_ORDINARY)])),
            Arc::new(Int32Array::from(vec![SEED_GEN_DIRECT])),
            Arc::new(Int32Array::from(vec![SEED_GEN_TRANSITIVE])),
            Arc::new(Int32Array::from(vec![SEED_GEN_UNRELATED])),
        ],
    )
    .unwrap();
    let table = conn.create_table(name, batch).execute().await.unwrap();

    let snapshot = table.generated_column_binding_snapshot().await.unwrap();
    let input_a_field_id = snapshot.field(INPUT_A).expect(INPUT_A).field_id();
    let input_b_field_id = snapshot.field(INPUT_B).expect(INPUT_B).field_id();
    let gen_direct_field_id = snapshot.field(GEN_DIRECT).expect(GEN_DIRECT).field_id();
    let gen_transitive_field_id = snapshot
        .field(GEN_TRANSITIVE)
        .expect(GEN_TRANSITIVE)
        .field_id();
    let gen_unrelated_field_id = snapshot
        .field(GEN_UNRELATED)
        .expect(GEN_UNRELATED)
        .field_id();

    let direct =
        field_bound_definition("fn.exact.b4c.direct", gen_direct_field_id, input_a_field_id);
    let transitive = field_bound_definition(
        "fn.exact.b4c.transitive",
        gen_transitive_field_id,
        gen_direct_field_id,
    );
    let unrelated = field_bound_definition(
        "fn.exact.b4c.unrelated",
        gen_unrelated_field_id,
        input_b_field_id,
    );

    let native = table
        .as_native()
        .expect("generated-column fixture planting requires a Native table");
    for (column, definition) in [
        (GEN_DIRECT, &direct),
        (GEN_TRANSITIVE, &transitive),
        (GEN_UNRELATED, &unrelated),
    ] {
        crate::table::schema_evolution::install_raw_generated_column_metadata_for_tests(
            native,
            column,
            definition.to_metadata_json().unwrap(),
        )
        .await
        .unwrap();
    }

    let planted_direct = read_generated_definition(&table, GEN_DIRECT).await;
    let planted_transitive = read_generated_definition(&table, GEN_TRANSITIVE).await;
    let planted_unrelated = read_generated_definition(&table, GEN_UNRELATED).await;

    assert_eq!(sole_field_argument_id(&planted_direct), input_a_field_id);
    assert_eq!(
        sole_field_argument_id(&planted_transitive),
        gen_direct_field_id
    );
    assert_eq!(sole_field_argument_id(&planted_unrelated), input_b_field_id);
    assert_eq!(planted_direct.output_field_id(), gen_direct_field_id);
    assert_eq!(
        planted_transitive.output_field_id(),
        gen_transitive_field_id
    );
    assert_eq!(planted_unrelated.output_field_id(), gen_unrelated_field_id);
    assert_eq!(
        planted_direct.function_call().function_id().as_str(),
        "fn.exact.b4c.direct"
    );
    assert_eq!(
        planted_transitive.function_call().function_id().as_str(),
        "fn.exact.b4c.transitive"
    );
    assert_eq!(
        planted_unrelated.function_call().function_id().as_str(),
        "fn.exact.b4c.unrelated"
    );
    assert_eq!(
        table.generated_column_status(GEN_DIRECT).await.unwrap(),
        GeneratedColumnStatus::Complete
    );
    assert_eq!(
        table.generated_column_status(GEN_TRANSITIVE).await.unwrap(),
        GeneratedColumnStatus::Complete
    );
    assert_eq!(
        table.generated_column_status(GEN_UNRELATED).await.unwrap(),
        GeneratedColumnStatus::Complete
    );
    assert_eq!(planted_direct.status(), GeneratedColumnStatus::Complete);
    assert_eq!(planted_transitive.status(), GeneratedColumnStatus::Complete);
    assert_eq!(planted_unrelated.status(), GeneratedColumnStatus::Complete);
    assert_ne!(INITIAL_DEPENDENCY_EPOCH, 0);
    assert_eq!(INITIAL_DEPENDENCY_EPOCH, INITIAL_MATERIALIZED_EPOCH);

    Fixture {
        _tmp: tmp,
        table,
        table_name: name.to_string(),
        uri,
        input_a_field_id,
        input_b_field_id,
        gen_direct_field_id,
        gen_transitive_field_id,
        gen_unrelated_field_id,
    }
}

async fn read_generated_definition(table: &Table, column: &str) -> GeneratedColumnDefinition {
    let snapshot = table.generated_column_binding_snapshot().await.unwrap();
    snapshot
        .field(column)
        .unwrap_or_else(|| panic!("missing field {column}"))
        .generated_column_definition()
        .expect("generated metadata must decode")
        .expect("generated metadata must be present")
}

async fn read_int_column(table: &Table, column: &str) -> Vec<i32> {
    let batches = table
        .query()
        .select(Select::columns(&[column]))
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut values = Vec::new();
    for batch in batches {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for index in 0..array.len() {
            assert!(
                !array.is_null(index),
                "{column} must be non-null in fixture"
            );
            values.push(array.value(index));
        }
    }
    values
}

async fn read_ordinary(table: &Table) -> Vec<String> {
    let batches = table
        .query()
        .select(Select::columns(&[ORDINARY]))
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut values = Vec::new();
    for batch in batches {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..array.len() {
            assert!(!array.is_null(index));
            values.push(array.value(index).to_string());
        }
    }
    values
}

async fn safe_row_projection(table: &Table) -> (Vec<i32>, Vec<i32>, Vec<i32>, Vec<String>) {
    // Avoid projecting intentionally incomplete generated outputs.
    let batches = table
        .query()
        .select(Select::columns(&[ID, INPUT_A, INPUT_B, ORDINARY]))
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut ids = Vec::new();
    let mut input_a = Vec::new();
    let mut input_b = Vec::new();
    let mut ordinary = Vec::new();
    for batch in batches {
        let id = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let a = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let b = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let o = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            ids.push(id.value(index));
            input_a.push(a.value(index));
            input_b.push(b.value(index));
            ordinary.push(o.value(index).to_string());
        }
    }
    (ids, input_a, input_b, ordinary)
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

fn assert_invalidated_once(
    before: &GeneratedColumnDefinition,
    after: &GeneratedColumnDefinition,
    label: &str,
) {
    assert_eq!(
        after.dependency_epoch(),
        before.dependency_epoch() + 1,
        "{label}: dependency_epoch must advance exactly once"
    );
    assert_eq!(
        after.materialized_epoch(),
        before.materialized_epoch(),
        "{label}: materialized_epoch must be preserved"
    );
    assert_eq!(
        after.output_field_id(),
        before.output_field_id(),
        "{label}: output field id must be preserved"
    );
    assert_eq!(
        after.function_call(),
        before.function_call(),
        "{label}: embedded function call must be preserved"
    );
    assert_eq!(
        after.status(),
        GeneratedColumnStatus::Incomplete,
        "{label}: status must become Incomplete"
    );
}

#[tokio::test]
async fn dependent_update_invalidates_direct_and_transitive_preserves_unrelated() {
    let fixture = create_dependent_generated_fixture("b4c_dependent_update").await;
    let before_direct = read_generated_definition(&fixture.table, GEN_DIRECT).await;
    let before_transitive = read_generated_definition(&fixture.table, GEN_TRANSITIVE).await;
    let before_unrelated = read_generated_definition(&fixture.table, GEN_UNRELATED).await;
    assert_eq!(
        sole_field_argument_id(&before_direct),
        fixture.input_a_field_id
    );
    assert_eq!(
        sole_field_argument_id(&before_transitive),
        fixture.gen_direct_field_id
    );
    assert_eq!(
        sole_field_argument_id(&before_unrelated),
        fixture.input_b_field_id
    );

    let result = fixture
        .table
        .update()
        .column(INPUT_A, "111")
        .execute()
        .await
        .expect("dependent input update must commit");
    assert_eq!(result.rows_updated, 1);

    let (_, input_a, input_b, ordinary) = safe_row_projection(&fixture.table).await;
    assert_eq!(input_a, vec![111]);
    assert_eq!(input_b, vec![SEED_INPUT_B]);
    assert_eq!(ordinary, vec![SEED_ORDINARY.to_string()]);

    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_DIRECT)
            .await
            .unwrap(),
        GeneratedColumnStatus::Incomplete
    );
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_TRANSITIVE)
            .await
            .unwrap(),
        GeneratedColumnStatus::Incomplete
    );
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_UNRELATED)
            .await
            .unwrap(),
        GeneratedColumnStatus::Complete
    );

    let after_direct = read_generated_definition(&fixture.table, GEN_DIRECT).await;
    let after_transitive = read_generated_definition(&fixture.table, GEN_TRANSITIVE).await;
    let after_unrelated = read_generated_definition(&fixture.table, GEN_UNRELATED).await;
    assert_invalidated_once(&before_direct, &after_direct, GEN_DIRECT);
    assert_invalidated_once(&before_transitive, &after_transitive, GEN_TRANSITIVE);
    assert_eq!(
        after_unrelated, before_unrelated,
        "independent generated column must remain byte-for-byte complete"
    );

    let Err(err_direct) = fixture
        .table
        .query()
        .select(Select::columns(&[GEN_DIRECT]))
        .execute()
        .await
    else {
        panic!("impacted gen_direct query must trip incomplete guard");
    };
    assert_generated_column_incomplete(&err_direct, "gen_direct query");

    let Err(err_transitive) = fixture
        .table
        .query()
        .select(Select::columns(&[GEN_TRANSITIVE]))
        .execute()
        .await
    else {
        panic!("impacted gen_transitive query must trip incomplete guard");
    };
    assert_generated_column_incomplete(&err_transitive, "gen_transitive query");

    fixture
        .table
        .query()
        .select(Select::columns(&[GEN_UNRELATED]))
        .execute()
        .await
        .expect("unrelated complete generated column must remain readable");
    assert_eq!(
        read_int_column(&fixture.table, GEN_UNRELATED).await,
        vec![SEED_GEN_UNRELATED]
    );
}

#[tokio::test]
async fn unrelated_ordinary_update_preserves_all_generated_definitions() {
    let fixture = create_dependent_generated_fixture("b4c_unrelated_ordinary").await;
    let before_direct = read_generated_definition(&fixture.table, GEN_DIRECT).await;
    let before_transitive = read_generated_definition(&fixture.table, GEN_TRANSITIVE).await;
    let before_unrelated = read_generated_definition(&fixture.table, GEN_UNRELATED).await;
    let before_rows = safe_row_projection(&fixture.table).await;

    let result = fixture
        .table
        .update()
        .column(ORDINARY, "'touched'")
        .execute()
        .await
        .expect("ordinary-field update must commit");
    assert_eq!(result.rows_updated, 1);

    let (ids, input_a, input_b, ordinary) = safe_row_projection(&fixture.table).await;
    assert_eq!(ids, before_rows.0);
    assert_eq!(input_a, before_rows.1);
    assert_eq!(input_b, before_rows.2);
    assert_eq!(ordinary, vec!["touched".to_string()]);

    assert_eq!(
        read_generated_definition(&fixture.table, GEN_DIRECT).await,
        before_direct
    );
    assert_eq!(
        read_generated_definition(&fixture.table, GEN_TRANSITIVE).await,
        before_transitive
    );
    assert_eq!(
        read_generated_definition(&fixture.table, GEN_UNRELATED).await,
        before_unrelated
    );
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_DIRECT)
            .await
            .unwrap(),
        GeneratedColumnStatus::Complete
    );
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_TRANSITIVE)
            .await
            .unwrap(),
        GeneratedColumnStatus::Complete
    );
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_UNRELATED)
            .await
            .unwrap(),
        GeneratedColumnStatus::Complete
    );

    for column in [GEN_DIRECT, GEN_TRANSITIVE, GEN_UNRELATED] {
        fixture
            .table
            .query()
            .select(Select::columns(&[column]))
            .execute()
            .await
            .unwrap_or_else(|err| panic!("{column} must remain readable: {err:?}"));
    }
}

#[tokio::test]
async fn zero_row_dependent_update_preserves_definitions_and_data() {
    let fixture = create_dependent_generated_fixture("b4c_zero_row_update").await;
    let before_direct = read_generated_definition(&fixture.table, GEN_DIRECT).await;
    let before_transitive = read_generated_definition(&fixture.table, GEN_TRANSITIVE).await;
    let before_unrelated = read_generated_definition(&fixture.table, GEN_UNRELATED).await;
    let before_rows = safe_row_projection(&fixture.table).await;
    let before_gen_unrelated = read_int_column(&fixture.table, GEN_UNRELATED).await;
    let before_gen_direct = read_int_column(&fixture.table, GEN_DIRECT).await;
    let before_gen_transitive = read_int_column(&fixture.table, GEN_TRANSITIVE).await;

    let result = fixture
        .table
        .update()
        .only_if("id < 0")
        .column(INPUT_A, "999")
        .execute()
        .await
        .expect("zero-row dependent update must complete");
    assert_eq!(result.rows_updated, 0);

    assert_eq!(safe_row_projection(&fixture.table).await, before_rows);
    assert_eq!(
        read_int_column(&fixture.table, GEN_DIRECT).await,
        before_gen_direct
    );
    assert_eq!(
        read_int_column(&fixture.table, GEN_TRANSITIVE).await,
        before_gen_transitive
    );
    assert_eq!(
        read_int_column(&fixture.table, GEN_UNRELATED).await,
        before_gen_unrelated
    );
    assert_eq!(
        read_generated_definition(&fixture.table, GEN_DIRECT).await,
        before_direct
    );
    assert_eq!(
        read_generated_definition(&fixture.table, GEN_TRANSITIVE).await,
        before_transitive
    );
    assert_eq!(
        read_generated_definition(&fixture.table, GEN_UNRELATED).await,
        before_unrelated
    );
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_DIRECT)
            .await
            .unwrap(),
        GeneratedColumnStatus::Complete
    );
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_TRANSITIVE)
            .await
            .unwrap(),
        GeneratedColumnStatus::Complete
    );
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_UNRELATED)
            .await
            .unwrap(),
        GeneratedColumnStatus::Complete
    );
}

#[tokio::test]
async fn dependent_update_atomic_old_new_version_visibility() {
    let fixture = create_dependent_generated_fixture("b4c_atomic_visibility").await;
    let previous_version = fixture.table.version().await.unwrap();
    let previous_rows = safe_row_projection(&fixture.table).await;
    let previous_direct = read_generated_definition(&fixture.table, GEN_DIRECT).await;
    let previous_transitive = read_generated_definition(&fixture.table, GEN_TRANSITIVE).await;
    let previous_unrelated = read_generated_definition(&fixture.table, GEN_UNRELATED).await;
    assert_eq!(previous_direct.status(), GeneratedColumnStatus::Complete);
    assert_eq!(
        previous_transitive.status(),
        GeneratedColumnStatus::Complete
    );

    let result = fixture
        .table
        .update()
        .column(INPUT_A, "77")
        .execute()
        .await
        .expect("dependent update must commit");
    assert_eq!(result.rows_updated, 1);
    let new_version = fixture.table.version().await.unwrap();
    assert_eq!(
        new_version,
        previous_version
            .checked_add(1)
            .expect("previous_version + 1 must not overflow"),
        "successful update must create exactly one new version"
    );
    assert_eq!(result.version, new_version);

    // Exact new version: changed input + impacted Incomplete metadata together.
    let (_, input_a, _, _) = safe_row_projection(&fixture.table).await;
    assert_eq!(input_a, vec![77]);
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_DIRECT)
            .await
            .unwrap(),
        GeneratedColumnStatus::Incomplete
    );
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_TRANSITIVE)
            .await
            .unwrap(),
        GeneratedColumnStatus::Incomplete
    );
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_UNRELATED)
            .await
            .unwrap(),
        GeneratedColumnStatus::Complete
    );
    let new_direct = read_generated_definition(&fixture.table, GEN_DIRECT).await;
    let new_transitive = read_generated_definition(&fixture.table, GEN_TRANSITIVE).await;
    assert_invalidated_once(&previous_direct, &new_direct, "new gen_direct");
    assert_invalidated_once(&previous_transitive, &new_transitive, "new gen_transitive");
    assert_eq!(
        read_generated_definition(&fixture.table, GEN_UNRELATED).await,
        previous_unrelated
    );

    // Immediately previous version: old values + original complete definitions.
    fixture.table.checkout(previous_version).await.unwrap();
    assert_eq!(safe_row_projection(&fixture.table).await, previous_rows);
    assert_eq!(
        read_generated_definition(&fixture.table, GEN_DIRECT).await,
        previous_direct
    );
    assert_eq!(
        read_generated_definition(&fixture.table, GEN_TRANSITIVE).await,
        previous_transitive
    );
    assert_eq!(
        read_generated_definition(&fixture.table, GEN_UNRELATED).await,
        previous_unrelated
    );
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_DIRECT)
            .await
            .unwrap(),
        GeneratedColumnStatus::Complete
    );
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_TRANSITIVE)
            .await
            .unwrap(),
        GeneratedColumnStatus::Complete
    );
    for column in [GEN_DIRECT, GEN_TRANSITIVE, GEN_UNRELATED] {
        fixture
            .table
            .query()
            .select(Select::columns(&[column]))
            .execute()
            .await
            .unwrap_or_else(|err| {
                panic!("previous complete version must keep {column} readable: {err:?}")
            });
    }
}

#[tokio::test]
async fn multi_input_update_invalidates_overlapping_closure_once() {
    let fixture = create_dependent_generated_fixture("b4c_multi_input").await;
    let before_direct = read_generated_definition(&fixture.table, GEN_DIRECT).await;
    let before_transitive = read_generated_definition(&fixture.table, GEN_TRANSITIVE).await;
    let before_unrelated = read_generated_definition(&fixture.table, GEN_UNRELATED).await;
    assert_eq!(fixture.gen_direct_field_id, before_direct.output_field_id());
    assert_eq!(
        fixture.gen_transitive_field_id,
        before_transitive.output_field_id()
    );
    assert_eq!(
        fixture.gen_unrelated_field_id,
        before_unrelated.output_field_id()
    );

    let result = fixture
        .table
        .update()
        .column(INPUT_A, "31")
        .column(INPUT_B, "32")
        .execute()
        .await
        .expect("multi-input update must commit");
    assert_eq!(result.rows_updated, 1);

    let (_, input_a, input_b, _) = safe_row_projection(&fixture.table).await;
    assert_eq!(input_a, vec![31]);
    assert_eq!(input_b, vec![32]);

    let after_direct = read_generated_definition(&fixture.table, GEN_DIRECT).await;
    let after_transitive = read_generated_definition(&fixture.table, GEN_TRANSITIVE).await;
    let after_unrelated = read_generated_definition(&fixture.table, GEN_UNRELATED).await;
    assert_invalidated_once(&before_direct, &after_direct, GEN_DIRECT);
    assert_invalidated_once(&before_transitive, &after_transitive, GEN_TRANSITIVE);
    assert_invalidated_once(&before_unrelated, &after_unrelated, GEN_UNRELATED);

    for column in [GEN_DIRECT, GEN_TRANSITIVE, GEN_UNRELATED] {
        let Err(err) = fixture
            .table
            .query()
            .select(Select::columns(&[column]))
            .execute()
            .await
        else {
            panic!("{column} must trip incomplete guard after multi-input update");
        };
        assert_generated_column_incomplete(&err, column);
    }
}

#[tokio::test]
async fn direct_generated_output_update_rejects_before_mutation() {
    let fixture = create_dependent_generated_fixture("b4c_direct_output_reject").await;
    let version_before = fixture.table.version().await.unwrap();
    let rows_before = safe_row_projection(&fixture.table).await;
    let before_direct = read_generated_definition(&fixture.table, GEN_DIRECT).await;
    let before_transitive = read_generated_definition(&fixture.table, GEN_TRANSITIVE).await;
    let before_unrelated = read_generated_definition(&fixture.table, GEN_UNRELATED).await;
    let gen_direct_before = read_int_column(&fixture.table, GEN_DIRECT).await;

    let err = fixture
        .table
        .update()
        .column(GEN_DIRECT, "999")
        .execute()
        .await
        .expect_err("direct generated-output update must reject before mutation");
    assert_not_supported(&err, "direct generated-output update");

    assert_eq!(fixture.table.version().await.unwrap(), version_before);
    assert_eq!(safe_row_projection(&fixture.table).await, rows_before);
    assert_eq!(
        read_int_column(&fixture.table, GEN_DIRECT).await,
        gen_direct_before
    );
    assert_eq!(
        read_generated_definition(&fixture.table, GEN_DIRECT).await,
        before_direct
    );
    assert_eq!(
        read_generated_definition(&fixture.table, GEN_TRANSITIVE).await,
        before_transitive
    );
    assert_eq!(
        read_generated_definition(&fixture.table, GEN_UNRELATED).await,
        before_unrelated
    );
    assert_eq!(
        fixture
            .table
            .generated_column_status(GEN_DIRECT)
            .await
            .unwrap(),
        GeneratedColumnStatus::Complete
    );
}

#[tokio::test]
async fn unrelated_update_rejects_malformed_generated_metadata_before_mutation() {
    let fixture = create_dependent_generated_fixture("b4c_malformed_preflight").await;
    let raw = format!(
        r#"{{"format_version":1,"output_field_id":{},"function_call":{},"dependency_epoch":1,"materialized_epoch":1}}"#,
        fixture.gen_unrelated_field_id, MALFORMED_MARKER
    );
    assert!(raw.contains(MALFORMED_MARKER));
    crate::table::schema_evolution::install_raw_generated_column_metadata_for_tests(
        fixture
            .table
            .as_native()
            .expect("generated-column fixture planting requires a Native table"),
        GEN_UNRELATED,
        raw,
    )
    .await
    .unwrap();

    let version_before = fixture.table.version().await.unwrap();
    let rows_before = safe_row_projection(&fixture.table).await;
    let before_direct = read_generated_definition(&fixture.table, GEN_DIRECT).await;
    let before_transitive = read_generated_definition(&fixture.table, GEN_TRANSITIVE).await;
    let ordinary_before = read_ordinary(&fixture.table).await;

    let err = fixture
        .table
        .update()
        .column(ORDINARY, "'must-not-land'")
        .execute()
        .await
        .expect_err(
            "malformed generated metadata must fail closed before unrelated update visibility",
        );
    assert_invalid_input_redacted(&err, "malformed update preflight");

    assert_eq!(fixture.table.version().await.unwrap(), version_before);
    assert_eq!(safe_row_projection(&fixture.table).await, rows_before);
    assert_eq!(read_ordinary(&fixture.table).await, ordinary_before);
    assert!(
        !read_ordinary(&fixture.table)
            .await
            .contains(&"must-not-land".to_string())
    );
    assert_eq!(
        read_generated_definition(&fixture.table, GEN_DIRECT).await,
        before_direct
    );
    assert_eq!(
        read_generated_definition(&fixture.table, GEN_TRANSITIVE).await,
        before_transitive
    );
}

#[tokio::test]
async fn concurrent_same_basis_dependent_updates_one_winner_one_conflict() {
    let fixture = create_dependent_generated_fixture("b4c_concurrent_update").await;
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
        table_a.update().column(INPUT_A, "111").execute(),
        table_b.update().column(INPUT_A, "222").execute(),
    );

    let outcomes = [result_a, result_b];
    let wins = outcomes.iter().filter(|result| result.is_ok()).count();
    let losses = outcomes.iter().filter(|result| result.is_err()).count();
    assert_eq!(wins, 1, "exactly one same-basis update may publish");
    assert_eq!(losses, 1, "exactly one same-basis update must conflict");
    for result in &outcomes {
        if let Err(err) = result {
            assert_conflict_error(err, "concurrent same-basis update loser");
        }
    }

    let fresh = conn
        .open_table(&fixture.table_name)
        .execute()
        .await
        .unwrap();
    let (_, input_a, _, _) = safe_row_projection(&fresh).await;
    assert_eq!(input_a.len(), 1);
    assert!(
        input_a[0] == 111 || input_a[0] == 222,
        "only a winner value may be visible, got {input_a:?}"
    );
    assert!(
        !(input_a.contains(&111) && input_a.contains(&222)),
        "loser values must not be visible, got {input_a:?}"
    );

    assert_eq!(
        fresh.generated_column_status(GEN_DIRECT).await.unwrap(),
        GeneratedColumnStatus::Incomplete
    );
    assert_eq!(
        fresh.generated_column_status(GEN_TRANSITIVE).await.unwrap(),
        GeneratedColumnStatus::Incomplete
    );
    assert_eq!(
        fresh.generated_column_status(GEN_UNRELATED).await.unwrap(),
        GeneratedColumnStatus::Complete
    );
    let direct = read_generated_definition(&fresh, GEN_DIRECT).await;
    let transitive = read_generated_definition(&fresh, GEN_TRANSITIVE).await;
    assert_eq!(direct.dependency_epoch(), INITIAL_DEPENDENCY_EPOCH + 1);
    assert_eq!(transitive.dependency_epoch(), INITIAL_DEPENDENCY_EPOCH + 1);
    assert_eq!(direct.materialized_epoch(), INITIAL_MATERIALIZED_EPOCH);
    assert_eq!(transitive.materialized_epoch(), INITIAL_MATERIALIZED_EPOCH);
    let unrelated = read_generated_definition(&fresh, GEN_UNRELATED).await;
    assert_eq!(unrelated.dependency_epoch(), INITIAL_DEPENDENCY_EPOCH);
    assert_eq!(unrelated.status(), GeneratedColumnStatus::Complete);
}
