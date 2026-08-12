// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! RED runtime contract tests for Native delete invalidation (B4d).
//!
//! These tests pin Native Table delete behavior for generated-column
//! dependency-epoch invalidation. They use real local Native tables and existing
//! public APIs; Lance commits, the B4a planner, query guards, and concurrency are
//! not mocked. String and DataFusion Expr predicates are proven independently.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion_expr::{col, lit};
use futures::TryStreamExt;
use lance::dataset::NewColumnTransform;
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
use crate::table::schema_evolution::FieldMetadataUpdate;

const ID: &str = "id";
const INPUT_A: &str = "input_a";
const INPUT_B: &str = "input_b";
const ORDINARY: &str = "ordinary";
const GEN_DIRECT: &str = "gen_direct";
const GEN_TRANSITIVE: &str = "gen_transitive";
const GEN_INDEPENDENT: &str = "gen_independent";
const GEN_LITERAL: &str = "gen_literal";
const ADDED_ORDINARY: &str = "added_ordinary";

const INITIAL_DEPENDENCY_EPOCH: u64 = 3;
const INITIAL_MATERIALIZED_EPOCH: u64 = 3;
const MALFORMED_MARKER: &str = "SENSITIVE_B4D_DELETE_METADATA_MARKER_7c2a_91e4";

const FN_DIRECT: &str = "fn.exact.b4d.direct";
const FN_TRANSITIVE: &str = "fn.exact.b4d.transitive";
const FN_INDEPENDENT: &str = "fn.exact.b4d.independent";
const FN_LITERAL: &str = "fn.exact.b4d.literal";
const LITERAL_LABEL: &str = "literal-only-b4d";

const ROW_ONE: i32 = 1;
const ROW_TWO: i32 = 2;
const ROW_THREE: i32 = 3;

struct Fixture {
    _tmp: TempDir,
    table: Table,
    table_name: String,
    uri: String,
    input_a_field_id: i32,
    input_b_field_id: i32,
    gen_direct_field_id: i32,
    gen_transitive_field_id: i32,
    gen_independent_field_id: i32,
    gen_literal_field_id: i32,
    literal_argument: FunctionArgument,
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

fn literal_only_function() -> Function {
    Function::new(
        FunctionId::try_new(FN_LITERAL).unwrap(),
        FunctionSignature::try_new(
            vec![FunctionParameter::new("label", DataType::Utf8)],
            FunctionOutput::new(DataType::Int32, true),
        )
        .unwrap(),
    )
}

fn literal_only_argument() -> FunctionArgument {
    FunctionArgument::try_literal(Arc::new(StringArray::from(vec![Some(LITERAL_LABEL)])) as ArrayRef)
        .unwrap()
}

fn literal_only_definition(output_field_id: i32) -> GeneratedColumnDefinition {
    let call = FunctionCall::try_new(
        &literal_only_function(),
        vec![("label".to_string(), literal_only_argument())],
    )
    .unwrap();
    assert!(
        call.arguments()
            .iter()
            .all(|(_, argument)| argument.field_id().is_none()),
        "literal fixture call must not bind any field id"
    );
    GeneratedColumnDefinition::try_new(
        output_field_id,
        call,
        INITIAL_DEPENDENCY_EPOCH,
        INITIAL_MATERIALIZED_EPOCH,
    )
    .unwrap()
}

fn sole_field_argument(definition: &GeneratedColumnDefinition) -> &FunctionArgument {
    let args = definition.function_call().arguments();
    assert_eq!(
        args.len(),
        1,
        "fixture calls must bind exactly one argument"
    );
    &args[0].1
}

fn sole_field_argument_id(definition: &GeneratedColumnDefinition) -> i32 {
    sole_field_argument(definition)
        .field_id()
        .expect("fixture call must be field-bound")
}

async fn create_row_set_generated_fixture(name: &str) -> Fixture {
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
        Field::new(GEN_INDEPENDENT, DataType::Int32, true),
        Field::new(GEN_LITERAL, DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![ROW_ONE, ROW_TWO, ROW_THREE])),
            Arc::new(Int32Array::from(vec![10, 11, 12])),
            Arc::new(Int32Array::from(vec![20, 21, 22])),
            Arc::new(StringArray::from(vec![
                Some("seed-1"),
                Some("seed-2"),
                Some("seed-3"),
            ])),
            Arc::new(Int32Array::from(vec![100, 101, 102])),
            Arc::new(Int32Array::from(vec![200, 201, 202])),
            Arc::new(Int32Array::from(vec![300, 301, 302])),
            Arc::new(Int32Array::from(vec![400, 401, 402])),
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
    let gen_independent_field_id = snapshot
        .field(GEN_INDEPENDENT)
        .expect(GEN_INDEPENDENT)
        .field_id();
    let gen_literal_field_id = snapshot.field(GEN_LITERAL).expect(GEN_LITERAL).field_id();

    let direct = field_bound_definition(FN_DIRECT, gen_direct_field_id, input_a_field_id);
    let transitive =
        field_bound_definition(FN_TRANSITIVE, gen_transitive_field_id, gen_direct_field_id);
    let independent =
        field_bound_definition(FN_INDEPENDENT, gen_independent_field_id, input_b_field_id);
    let literal = literal_only_definition(gen_literal_field_id);
    let literal_argument = literal_only_argument();

    table
        .update_field_metadata(&[
            FieldMetadataUpdate::new(GEN_DIRECT).set(
                GENERATED_COLUMN_METADATA_KEY,
                direct.to_metadata_json().unwrap(),
            ),
            FieldMetadataUpdate::new(GEN_TRANSITIVE).set(
                GENERATED_COLUMN_METADATA_KEY,
                transitive.to_metadata_json().unwrap(),
            ),
            FieldMetadataUpdate::new(GEN_INDEPENDENT).set(
                GENERATED_COLUMN_METADATA_KEY,
                independent.to_metadata_json().unwrap(),
            ),
            FieldMetadataUpdate::new(GEN_LITERAL).set(
                GENERATED_COLUMN_METADATA_KEY,
                literal.to_metadata_json().unwrap(),
            ),
        ])
        .await
        .unwrap();

    let planted_direct = read_generated_definition(&table, GEN_DIRECT).await;
    let planted_transitive = read_generated_definition(&table, GEN_TRANSITIVE).await;
    let planted_independent = read_generated_definition(&table, GEN_INDEPENDENT).await;
    let planted_literal = read_generated_definition(&table, GEN_LITERAL).await;

    assert_eq!(sole_field_argument_id(&planted_direct), input_a_field_id);
    assert_eq!(
        sole_field_argument(&planted_direct).data_type(),
        &DataType::Int32
    );
    assert_eq!(
        sole_field_argument_id(&planted_transitive),
        gen_direct_field_id
    );
    assert_eq!(
        sole_field_argument(&planted_transitive).data_type(),
        &DataType::Int32
    );
    assert_eq!(
        sole_field_argument_id(&planted_independent),
        input_b_field_id
    );
    assert_eq!(
        sole_field_argument(&planted_independent).data_type(),
        &DataType::Int32
    );
    assert_eq!(planted_direct.output_field_id(), gen_direct_field_id);
    assert_eq!(
        planted_transitive.output_field_id(),
        gen_transitive_field_id
    );
    assert_eq!(
        planted_independent.output_field_id(),
        gen_independent_field_id
    );
    assert_eq!(planted_literal.output_field_id(), gen_literal_field_id);
    assert_eq!(
        planted_direct.function_call().function_id().as_str(),
        FN_DIRECT
    );
    assert_eq!(
        planted_transitive.function_call().function_id().as_str(),
        FN_TRANSITIVE
    );
    assert_eq!(
        planted_independent.function_call().function_id().as_str(),
        FN_INDEPENDENT
    );
    assert_eq!(
        planted_literal.function_call().function_id().as_str(),
        FN_LITERAL
    );

    let planted_literal_arg = sole_field_argument(&planted_literal);
    assert!(
        planted_literal_arg.field_id().is_none(),
        "planted literal call must remain literal-only"
    );
    assert_eq!(planted_literal_arg.data_type(), &DataType::Utf8);
    assert_eq!(planted_literal_arg, &literal_argument);

    let planted_snapshot = table.generated_column_binding_snapshot().await.unwrap();
    assert_eq!(
        planted_snapshot.field(INPUT_A).expect(INPUT_A).field_id(),
        input_a_field_id
    );
    assert_eq!(
        planted_snapshot.field(INPUT_B).expect(INPUT_B).field_id(),
        input_b_field_id
    );
    assert_eq!(
        planted_snapshot
            .field(GEN_DIRECT)
            .expect(GEN_DIRECT)
            .field_id(),
        gen_direct_field_id
    );
    assert_eq!(
        planted_snapshot
            .field(GEN_TRANSITIVE)
            .expect(GEN_TRANSITIVE)
            .field_id(),
        gen_transitive_field_id
    );
    assert_eq!(
        planted_snapshot
            .field(GEN_INDEPENDENT)
            .expect(GEN_INDEPENDENT)
            .field_id(),
        gen_independent_field_id
    );
    assert_eq!(
        planted_snapshot
            .field(GEN_LITERAL)
            .expect(GEN_LITERAL)
            .field_id(),
        gen_literal_field_id
    );

    for column in [GEN_DIRECT, GEN_TRANSITIVE, GEN_INDEPENDENT, GEN_LITERAL] {
        assert_eq!(
            table.generated_column_status(column).await.unwrap(),
            GeneratedColumnStatus::Complete
        );
        table
            .query()
            .select(Select::columns(&[column]))
            .execute()
            .await
            .unwrap_or_else(|err| panic!("initial {column} must be query-readable: {err:?}"));
    }
    assert_eq!(planted_direct.status(), GeneratedColumnStatus::Complete);
    assert_eq!(planted_transitive.status(), GeneratedColumnStatus::Complete);
    assert_eq!(
        planted_independent.status(),
        GeneratedColumnStatus::Complete
    );
    assert_eq!(planted_literal.status(), GeneratedColumnStatus::Complete);
    assert_ne!(INITIAL_DEPENDENCY_EPOCH, 0);
    assert_eq!(INITIAL_DEPENDENCY_EPOCH, INITIAL_MATERIALIZED_EPOCH);
    assert_eq!(table.count_rows(None).await.unwrap(), 3);

    Fixture {
        _tmp: tmp,
        table,
        table_name: name.to_string(),
        uri,
        input_a_field_id,
        input_b_field_id,
        gen_direct_field_id,
        gen_transitive_field_id,
        gen_independent_field_id,
        gen_literal_field_id,
        literal_argument,
    }
}

async fn create_ordinary_table(name: &str) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let uri = tmp.path().to_str().unwrap().to_string();
    let conn = ConnectBuilder::new(&uri).execute().await.unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new(ID, DataType::Int32, false),
        Field::new(ORDINARY, DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![ROW_ONE, ROW_TWO, ROW_THREE])),
            Arc::new(StringArray::from(vec![
                Some("seed-1"),
                Some("seed-2"),
                Some("seed-3"),
            ])),
        ],
    )
    .unwrap();
    let table = conn.create_table(name, batch).execute().await.unwrap();
    Fixture {
        _tmp: tmp,
        table,
        table_name: name.to_string(),
        uri,
        input_a_field_id: -1,
        input_b_field_id: -1,
        gen_direct_field_id: -1,
        gen_transitive_field_id: -1,
        gen_independent_field_id: -1,
        gen_literal_field_id: -1,
        literal_argument: literal_only_argument(),
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

async fn read_raw_generated_metadata(table: &Table, column: &str) -> String {
    let snapshot = table.generated_column_binding_snapshot().await.unwrap();
    snapshot
        .field(column)
        .unwrap_or_else(|| panic!("missing field {column}"))
        .field()
        .metadata()
        .get(GENERATED_COLUMN_METADATA_KEY)
        .unwrap_or_else(|| panic!("missing generated metadata key on {column}"))
        .clone()
}

async fn read_ids(table: &Table) -> Vec<i32> {
    let batches = table
        .query()
        .select(Select::columns(&[ID]))
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
            assert!(!array.is_null(index));
            values.push(array.value(index));
        }
    }
    values.sort_unstable();
    values
}

async fn safe_row_projection(table: &Table) -> (Vec<i32>, Vec<i32>, Vec<i32>, Vec<String>) {
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
    let mut paired: Vec<_> = ids
        .into_iter()
        .zip(input_a)
        .zip(input_b)
        .zip(ordinary)
        .map(|(((id, a), b), o)| (id, a, b, o))
        .collect();
    paired.sort_by_key(|(id, _, _, _)| *id);
    let mut ids = Vec::new();
    let mut input_a = Vec::new();
    let mut input_b = Vec::new();
    let mut ordinary = Vec::new();
    for (id, a, b, o) in paired {
        ids.push(id);
        input_a.push(a);
        input_b.push(b);
        ordinary.push(o);
    }
    (ids, input_a, input_b, ordinary)
}

async fn read_all_definitions(
    table: &Table,
) -> (
    GeneratedColumnDefinition,
    GeneratedColumnDefinition,
    GeneratedColumnDefinition,
    GeneratedColumnDefinition,
) {
    (
        read_generated_definition(table, GEN_DIRECT).await,
        read_generated_definition(table, GEN_TRANSITIVE).await,
        read_generated_definition(table, GEN_INDEPENDENT).await,
        read_generated_definition(table, GEN_LITERAL).await,
    )
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
                        | lance::Error::TooMuchWriteContention { .. }
                ),
                "{label}: expected Lance conflict/contention category, got {source:?}"
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

fn assert_fixture_bindings(fixture: &Fixture) {
    let stable_ids = [
        ("input_a", fixture.input_a_field_id),
        ("input_b", fixture.input_b_field_id),
        ("gen_direct", fixture.gen_direct_field_id),
        ("gen_transitive", fixture.gen_transitive_field_id),
        ("gen_independent", fixture.gen_independent_field_id),
        ("gen_literal", fixture.gen_literal_field_id),
    ];
    for (left_index, (left_name, left_id)) in stable_ids.iter().enumerate() {
        for (right_name, right_id) in stable_ids.iter().skip(left_index + 1) {
            assert_ne!(
                left_id, right_id,
                "stable field IDs must be pairwise unique: {left_name}={left_id}, {right_name}={right_id}"
            );
        }
    }
    assert_eq!(
        fixture.literal_argument.data_type(),
        &DataType::Utf8,
        "literal fixture argument must keep typed Utf8 payload"
    );
}

async fn assert_all_four_incomplete(table: &Table) {
    for column in [GEN_DIRECT, GEN_TRANSITIVE, GEN_INDEPENDENT, GEN_LITERAL] {
        assert_eq!(
            table.generated_column_status(column).await.unwrap(),
            GeneratedColumnStatus::Incomplete,
            "{column} must be Incomplete"
        );
        let Err(err) = table
            .query()
            .select(Select::columns(&[column]))
            .execute()
            .await
        else {
            panic!("{column} projection must fail closed as incomplete");
        };
        assert_generated_column_incomplete(&err, column);
    }
}

async fn assert_all_four_complete_and_readable(table: &Table) {
    for column in [GEN_DIRECT, GEN_TRANSITIVE, GEN_INDEPENDENT, GEN_LITERAL] {
        assert_eq!(
            table.generated_column_status(column).await.unwrap(),
            GeneratedColumnStatus::Complete,
            "{column} must remain Complete"
        );
        table
            .query()
            .select(Select::columns(&[column]))
            .execute()
            .await
            .unwrap_or_else(|err| panic!("{column} must remain query-readable: {err:?}"));
    }
}

async fn assert_nonempty_delete_contract(
    table: &Table,
    previous_version: u64,
    previous_direct: &GeneratedColumnDefinition,
    previous_transitive: &GeneratedColumnDefinition,
    previous_independent: &GeneratedColumnDefinition,
    previous_literal: &GeneratedColumnDefinition,
) {
    let new_version = table.version().await.unwrap();
    assert_eq!(
        new_version,
        previous_version
            .checked_add(1)
            .expect("previous_version + 1 must not overflow"),
        "successful delete must create exactly one new version"
    );

    let (ids, input_a, input_b, ordinary) = safe_row_projection(table).await;
    assert_eq!(ids, vec![ROW_TWO, ROW_THREE]);
    assert_eq!(input_a, vec![11, 12]);
    assert_eq!(input_b, vec![21, 22]);
    assert_eq!(ordinary, vec!["seed-2".to_string(), "seed-3".to_string()]);
    assert!(!ids.contains(&ROW_ONE));

    let (after_direct, after_transitive, after_independent, after_literal) =
        read_all_definitions(table).await;
    assert_invalidated_once(previous_direct, &after_direct, GEN_DIRECT);
    assert_invalidated_once(previous_transitive, &after_transitive, GEN_TRANSITIVE);
    assert_invalidated_once(previous_independent, &after_independent, GEN_INDEPENDENT);
    assert_invalidated_once(previous_literal, &after_literal, GEN_LITERAL);
    assert_eq!(
        after_direct.dependency_epoch(),
        INITIAL_DEPENDENCY_EPOCH + 1
    );
    assert_eq!(
        after_transitive.dependency_epoch(),
        INITIAL_DEPENDENCY_EPOCH + 1
    );
    assert_eq!(
        after_independent.dependency_epoch(),
        INITIAL_DEPENDENCY_EPOCH + 1
    );
    assert_eq!(
        after_literal.dependency_epoch(),
        INITIAL_DEPENDENCY_EPOCH + 1
    );
    assert_eq!(
        after_direct.materialized_epoch(),
        INITIAL_MATERIALIZED_EPOCH
    );
    assert_eq!(
        after_transitive.materialized_epoch(),
        INITIAL_MATERIALIZED_EPOCH
    );
    assert_eq!(
        after_independent.materialized_epoch(),
        INITIAL_MATERIALIZED_EPOCH
    );
    assert_eq!(
        after_literal.materialized_epoch(),
        INITIAL_MATERIALIZED_EPOCH
    );
    assert_all_four_incomplete(table).await;

    // Exact previous version retains all rows and all four complete definitions.
    table.checkout(previous_version).await.unwrap();
    assert_eq!(
        safe_row_projection(table).await.0,
        vec![ROW_ONE, ROW_TWO, ROW_THREE]
    );
    assert_eq!(
        read_generated_definition(table, GEN_DIRECT).await,
        *previous_direct
    );
    assert_eq!(
        read_generated_definition(table, GEN_TRANSITIVE).await,
        *previous_transitive
    );
    assert_eq!(
        read_generated_definition(table, GEN_INDEPENDENT).await,
        *previous_independent
    );
    assert_eq!(
        read_generated_definition(table, GEN_LITERAL).await,
        *previous_literal
    );
    assert_all_four_complete_and_readable(table).await;
    table.checkout_latest().await.unwrap();
}

#[tokio::test]
async fn string_nonempty_delete_invalidates_all_four_atomic() {
    let fixture = create_row_set_generated_fixture("b4d_string_nonempty").await;
    assert_fixture_bindings(&fixture);
    let previous_version = fixture.table.version().await.unwrap();
    let (before_direct, before_transitive, before_independent, before_literal) =
        read_all_definitions(&fixture.table).await;
    assert_eq!(
        sole_field_argument_id(&before_direct),
        fixture.input_a_field_id
    );
    assert_eq!(
        sole_field_argument_id(&before_transitive),
        fixture.gen_direct_field_id
    );
    assert_eq!(
        sole_field_argument_id(&before_independent),
        fixture.input_b_field_id
    );
    assert_eq!(
        sole_field_argument(&before_literal),
        &fixture.literal_argument
    );

    let result = fixture
        .table
        .delete("id = 1")
        .await
        .expect("string non-empty delete must commit");
    assert_eq!(result.num_deleted_rows, 1);
    assert_eq!(
        result.version,
        previous_version
            .checked_add(1)
            .expect("previous_version + 1 must not overflow")
    );

    assert_nonempty_delete_contract(
        &fixture.table,
        previous_version,
        &before_direct,
        &before_transitive,
        &before_independent,
        &before_literal,
    )
    .await;
}

#[tokio::test]
async fn expr_nonempty_delete_invalidates_all_four_atomic() {
    let fixture = create_row_set_generated_fixture("b4d_expr_nonempty").await;
    assert_fixture_bindings(&fixture);
    let previous_version = fixture.table.version().await.unwrap();
    let (before_direct, before_transitive, before_independent, before_literal) =
        read_all_definitions(&fixture.table).await;
    assert_eq!(before_direct.output_field_id(), fixture.gen_direct_field_id);
    assert_eq!(
        before_transitive.output_field_id(),
        fixture.gen_transitive_field_id
    );
    assert_eq!(
        before_independent.output_field_id(),
        fixture.gen_independent_field_id
    );
    assert_eq!(
        before_literal.output_field_id(),
        fixture.gen_literal_field_id
    );

    let expr = col(ID).eq(lit(ROW_ONE));
    let result = fixture
        .table
        .delete(&expr)
        .await
        .expect("expr non-empty delete must commit");
    assert_eq!(result.num_deleted_rows, 1);
    assert_eq!(
        result.version,
        previous_version
            .checked_add(1)
            .expect("previous_version + 1 must not overflow")
    );

    assert_nonempty_delete_contract(
        &fixture.table,
        previous_version,
        &before_direct,
        &before_transitive,
        &before_independent,
        &before_literal,
    )
    .await;
}

#[tokio::test]
async fn string_zero_row_delete_preserves_definitions_and_data() {
    let fixture = create_row_set_generated_fixture("b4d_string_zero_row").await;
    let before_rows = safe_row_projection(&fixture.table).await;
    let (before_direct, before_transitive, before_independent, before_literal) =
        read_all_definitions(&fixture.table).await;
    let before_json = [
        before_direct.to_metadata_json().unwrap(),
        before_transitive.to_metadata_json().unwrap(),
        before_independent.to_metadata_json().unwrap(),
        before_literal.to_metadata_json().unwrap(),
    ];

    let result = fixture
        .table
        .delete("id < 0")
        .await
        .expect("string zero-row delete must complete");
    assert_eq!(result.num_deleted_rows, 0);

    assert_eq!(safe_row_projection(&fixture.table).await, before_rows);
    let (after_direct, after_transitive, after_independent, after_literal) =
        read_all_definitions(&fixture.table).await;
    assert_eq!(after_direct, before_direct);
    assert_eq!(after_transitive, before_transitive);
    assert_eq!(after_independent, before_independent);
    assert_eq!(after_literal, before_literal);
    assert_eq!(
        [
            after_direct.to_metadata_json().unwrap(),
            after_transitive.to_metadata_json().unwrap(),
            after_independent.to_metadata_json().unwrap(),
            after_literal.to_metadata_json().unwrap(),
        ],
        before_json
    );
    assert_all_four_complete_and_readable(&fixture.table).await;
}

#[tokio::test]
async fn expr_zero_row_delete_preserves_definitions_and_data() {
    let fixture = create_row_set_generated_fixture("b4d_expr_zero_row").await;
    let before_rows = safe_row_projection(&fixture.table).await;
    let (before_direct, before_transitive, before_independent, before_literal) =
        read_all_definitions(&fixture.table).await;
    let before_json = [
        before_direct.to_metadata_json().unwrap(),
        before_transitive.to_metadata_json().unwrap(),
        before_independent.to_metadata_json().unwrap(),
        before_literal.to_metadata_json().unwrap(),
    ];

    let expr = col(ID).lt(lit(0));
    let result = fixture
        .table
        .delete(&expr)
        .await
        .expect("expr zero-row delete must complete");
    assert_eq!(result.num_deleted_rows, 0);

    assert_eq!(safe_row_projection(&fixture.table).await, before_rows);
    let (after_direct, after_transitive, after_independent, after_literal) =
        read_all_definitions(&fixture.table).await;
    assert_eq!(after_direct, before_direct);
    assert_eq!(after_transitive, before_transitive);
    assert_eq!(after_independent, before_independent);
    assert_eq!(after_literal, before_literal);
    assert_eq!(
        [
            after_direct.to_metadata_json().unwrap(),
            after_transitive.to_metadata_json().unwrap(),
            after_independent.to_metadata_json().unwrap(),
            after_literal.to_metadata_json().unwrap(),
        ],
        before_json
    );
    assert_all_four_complete_and_readable(&fixture.table).await;
}

#[tokio::test]
async fn string_malformed_generated_metadata_rejects_before_mutation() {
    let fixture = create_row_set_generated_fixture("b4d_string_malformed").await;
    let planted_raw = format!(
        r#"{{"format_version":1,"output_field_id":{},"function_call":{},"dependency_epoch":1,"materialized_epoch":1}}"#,
        fixture.gen_independent_field_id, MALFORMED_MARKER
    );
    assert!(planted_raw.contains(MALFORMED_MARKER));
    fixture
        .table
        .update_field_metadata(&[FieldMetadataUpdate::new(GEN_INDEPENDENT)
            .set(GENERATED_COLUMN_METADATA_KEY, planted_raw.clone())])
        .await
        .unwrap();
    assert_eq!(
        read_raw_generated_metadata(&fixture.table, GEN_INDEPENDENT).await,
        planted_raw,
        "planted malformed raw metadata must round-trip byte-for-byte"
    );

    let version_before = fixture.table.version().await.unwrap();
    let rows_before = safe_row_projection(&fixture.table).await;
    let before_direct = read_generated_definition(&fixture.table, GEN_DIRECT).await;
    let before_transitive = read_generated_definition(&fixture.table, GEN_TRANSITIVE).await;
    let before_literal = read_generated_definition(&fixture.table, GEN_LITERAL).await;

    let err = fixture
        .table
        .delete("id = 1")
        .await
        .expect_err("malformed generated metadata must fail closed before string delete");
    assert_invalid_input_redacted(&err, "string malformed delete preflight");

    let fresh = ConnectBuilder::new(&fixture.uri)
        .execute()
        .await
        .unwrap()
        .open_table(&fixture.table_name)
        .execute()
        .await
        .unwrap();
    assert_eq!(fresh.version().await.unwrap(), version_before);
    assert_eq!(safe_row_projection(&fresh).await, rows_before);
    assert_eq!(read_ids(&fresh).await, vec![ROW_ONE, ROW_TWO, ROW_THREE]);
    assert_eq!(
        read_raw_generated_metadata(&fresh, GEN_INDEPENDENT).await,
        planted_raw,
        "fresh storage must retain the exact planted malformed raw string"
    );
    assert_eq!(
        read_generated_definition(&fresh, GEN_DIRECT).await,
        before_direct
    );
    assert_eq!(
        read_generated_definition(&fresh, GEN_TRANSITIVE).await,
        before_transitive
    );
    assert_eq!(
        read_generated_definition(&fresh, GEN_LITERAL).await,
        before_literal
    );
}

#[tokio::test]
async fn expr_malformed_generated_metadata_rejects_before_mutation() {
    let fixture = create_row_set_generated_fixture("b4d_expr_malformed").await;
    let planted_raw = format!(
        r#"{{"format_version":1,"output_field_id":{},"function_call":{},"dependency_epoch":1,"materialized_epoch":1}}"#,
        fixture.gen_literal_field_id, MALFORMED_MARKER
    );
    assert!(planted_raw.contains(MALFORMED_MARKER));
    fixture
        .table
        .update_field_metadata(&[FieldMetadataUpdate::new(GEN_LITERAL)
            .set(GENERATED_COLUMN_METADATA_KEY, planted_raw.clone())])
        .await
        .unwrap();
    assert_eq!(
        read_raw_generated_metadata(&fixture.table, GEN_LITERAL).await,
        planted_raw,
        "planted malformed raw metadata must round-trip byte-for-byte"
    );

    let version_before = fixture.table.version().await.unwrap();
    let rows_before = safe_row_projection(&fixture.table).await;
    let before_direct = read_generated_definition(&fixture.table, GEN_DIRECT).await;
    let before_transitive = read_generated_definition(&fixture.table, GEN_TRANSITIVE).await;
    let before_independent = read_generated_definition(&fixture.table, GEN_INDEPENDENT).await;

    let expr = col(ID).eq(lit(ROW_ONE));
    let err = fixture
        .table
        .delete(&expr)
        .await
        .expect_err("malformed generated metadata must fail closed before expr delete");
    assert_invalid_input_redacted(&err, "expr malformed delete preflight");

    let fresh = ConnectBuilder::new(&fixture.uri)
        .execute()
        .await
        .unwrap()
        .open_table(&fixture.table_name)
        .execute()
        .await
        .unwrap();
    assert_eq!(fresh.version().await.unwrap(), version_before);
    assert_eq!(safe_row_projection(&fresh).await, rows_before);
    assert_eq!(read_ids(&fresh).await, vec![ROW_ONE, ROW_TWO, ROW_THREE]);
    assert_eq!(
        read_raw_generated_metadata(&fresh, GEN_LITERAL).await,
        planted_raw,
        "fresh storage must retain the exact planted malformed raw string"
    );
    assert_eq!(
        read_generated_definition(&fresh, GEN_DIRECT).await,
        before_direct
    );
    assert_eq!(
        read_generated_definition(&fresh, GEN_TRANSITIVE).await,
        before_transitive
    );
    assert_eq!(
        read_generated_definition(&fresh, GEN_INDEPENDENT).await,
        before_independent
    );
}

#[tokio::test]
async fn concurrent_same_basis_deletes_one_winner_one_conflict() {
    let fixture = create_row_set_generated_fixture("b4d_concurrent_delete").await;
    let (before_direct, before_transitive, before_independent, before_literal) =
        read_all_definitions(&fixture.table).await;
    let basis_version = fixture.table.version().await.unwrap();

    let conn = ConnectBuilder::new(&fixture.uri)
        .read_consistency_interval(Duration::from_secs(3600))
        .execute()
        .await
        .unwrap();
    // String request deletes row 1; Expr request deletes row 2.
    let string_table = conn
        .open_table(&fixture.table_name)
        .execute()
        .await
        .unwrap();
    let expr_table = conn
        .open_table(&fixture.table_name)
        .execute()
        .await
        .unwrap();
    assert_eq!(string_table.version().await.unwrap(), basis_version);
    assert_eq!(expr_table.version().await.unwrap(), basis_version);

    let expr = col(ID).eq(lit(ROW_TWO));
    let (string_result, expr_result) =
        tokio::join!(string_table.delete("id = 1"), expr_table.delete(&expr),);

    let string_won = string_result.is_ok();
    let expr_won = expr_result.is_ok();
    assert_eq!(
        usize::from(string_won) + usize::from(expr_won),
        1,
        "exactly one same-basis delete may publish"
    );
    assert_eq!(
        usize::from(string_result.is_err()) + usize::from(expr_result.is_err()),
        1,
        "exactly one same-basis delete must conflict"
    );

    for (label, result) in [
        ("string row-1 delete", &string_result),
        ("expr row-2 delete", &expr_result),
    ] {
        match result {
            Ok(ok) => {
                assert_eq!(
                    ok.num_deleted_rows, 1,
                    "{label}: winner must delete exactly one row"
                );
                assert_eq!(
                    ok.version,
                    basis_version
                        .checked_add(1)
                        .expect("basis_version + 1 must not overflow"),
                    "{label}: winner must publish exactly one new version"
                );
            }
            Err(err) => {
                assert_conflict_error(err, label);
                assert!(
                    !matches!(err, Error::InvalidInput { .. }),
                    "{label}: loser must be a stable conflict, not a rewritten zero-row/latest success path: {err:?}"
                );
            }
        }
    }

    let expected_ids = if string_won {
        vec![ROW_TWO, ROW_THREE]
    } else {
        vec![ROW_ONE, ROW_THREE]
    };

    let fresh = ConnectBuilder::new(&fixture.uri)
        .execute()
        .await
        .unwrap()
        .open_table(&fixture.table_name)
        .execute()
        .await
        .unwrap();
    assert_eq!(
        fresh.version().await.unwrap(),
        basis_version
            .checked_add(1)
            .expect("basis_version + 1 must not overflow"),
        "fresh storage must advance exactly one version"
    );

    let ids = read_ids(&fresh).await;
    assert_eq!(
        ids, expected_ids,
        "fresh storage must equal the exact complementary row set for the winning request"
    );

    let (after_direct, after_transitive, after_independent, after_literal) =
        read_all_definitions(&fresh).await;
    assert_invalidated_once(&before_direct, &after_direct, GEN_DIRECT);
    assert_invalidated_once(&before_transitive, &after_transitive, GEN_TRANSITIVE);
    assert_invalidated_once(&before_independent, &after_independent, GEN_INDEPENDENT);
    assert_invalidated_once(&before_literal, &after_literal, GEN_LITERAL);
    assert_all_four_incomplete(&fresh).await;
}

#[tokio::test]
async fn stale_schema_basis_delete_conflicts_without_auto_retry() {
    let fixture = create_row_set_generated_fixture("b4d_stale_schema_basis").await;
    let (before_direct, before_transitive, before_independent, before_literal) =
        read_all_definitions(&fixture.table).await;
    let before_json = [
        before_direct.to_metadata_json().unwrap(),
        before_transitive.to_metadata_json().unwrap(),
        before_independent.to_metadata_json().unwrap(),
        before_literal.to_metadata_json().unwrap(),
    ];
    let basis_version = fixture.table.version().await.unwrap();

    let conn = ConnectBuilder::new(&fixture.uri)
        .read_consistency_interval(Duration::from_secs(3600))
        .execute()
        .await
        .unwrap();
    let stale = conn
        .open_table(&fixture.table_name)
        .execute()
        .await
        .unwrap();
    let mutator = conn
        .open_table(&fixture.table_name)
        .execute()
        .await
        .unwrap();
    assert_eq!(stale.version().await.unwrap(), basis_version);
    assert_eq!(mutator.version().await.unwrap(), basis_version);

    mutator
        .add_columns()
        .transform(NewColumnTransform::SqlExpressions(vec![(
            ADDED_ORDINARY.into(),
            "cast(NULL as string)".into(),
        )]))
        .execute()
        .await
        .expect("ordinary add-column schema commit must succeed");
    let schema_version = mutator.version().await.unwrap();
    assert_eq!(
        schema_version,
        basis_version
            .checked_add(1)
            .expect("basis_version + 1 must not overflow")
    );
    assert_eq!(
        stale.version().await.unwrap(),
        basis_version,
        "lazy stale handle must remain on the pre-schema version"
    );

    let err = stale.delete("id = 1").await.expect_err(
        "stale generated-table delete must conflict instead of auto-retrying on latest",
    );
    assert_conflict_error(&err, "stale schema-basis delete");

    let fresh = ConnectBuilder::new(&fixture.uri)
        .execute()
        .await
        .unwrap()
        .open_table(&fixture.table_name)
        .execute()
        .await
        .unwrap();
    assert_eq!(
        fresh.version().await.unwrap(),
        schema_version,
        "fresh storage must contain only the schema commit"
    );
    assert_eq!(
        read_ids(&fresh).await,
        vec![ROW_ONE, ROW_TWO, ROW_THREE],
        "stale delete must not remove rows after schema commit"
    );
    let schema = fresh.schema().await.unwrap();
    assert!(
        schema.field_with_name(ADDED_ORDINARY).is_ok(),
        "fresh storage must contain the added ordinary column"
    );

    let (after_direct, after_transitive, after_independent, after_literal) =
        read_all_definitions(&fresh).await;
    assert_eq!(after_direct, before_direct);
    assert_eq!(after_transitive, before_transitive);
    assert_eq!(after_independent, before_independent);
    assert_eq!(after_literal, before_literal);
    assert_eq!(
        [
            after_direct.to_metadata_json().unwrap(),
            after_transitive.to_metadata_json().unwrap(),
            after_independent.to_metadata_json().unwrap(),
            after_literal.to_metadata_json().unwrap(),
        ],
        before_json
    );
    assert_all_four_complete_and_readable(&fresh).await;
}

#[tokio::test]
async fn ordinary_table_string_and_expr_deletes_succeed() {
    let fixture = create_ordinary_table("b4d_ordinary_control").await;
    assert_eq!(
        fixture.table.count_rows(None).await.unwrap(),
        3,
        "ordinary control fixture must start with three rows"
    );

    let string_result = fixture
        .table
        .delete("id = 1")
        .await
        .expect("ordinary string delete must succeed");
    assert_eq!(string_result.num_deleted_rows, 1);
    assert_eq!(read_ids(&fixture.table).await, vec![ROW_TWO, ROW_THREE]);

    let expr = col(ID).eq(lit(ROW_TWO));
    let expr_result = fixture
        .table
        .delete(&expr)
        .await
        .expect("ordinary expr delete must succeed");
    assert_eq!(expr_result.num_deleted_rows, 1);
    assert_eq!(read_ids(&fixture.table).await, vec![ROW_THREE]);
    assert_eq!(fixture.table.count_rows(None).await.unwrap(), 1);
}
