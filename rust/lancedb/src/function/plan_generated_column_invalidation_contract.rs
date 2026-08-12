// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Contract tests for the crate-private generated-column invalidation planner (B4a).
//!
//! These tests pin the pure planning surface implemented by
//! [`super::plan_generated_column_invalidation`]. No runtime append/update/delete
//! path is exercised.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array};
use arrow_schema::{DataType, Field, FieldRef};

use super::plan_generated_column_invalidation::{
    GeneratedColumnMutationImpact, PlannedGeneratedColumnMetadataUpdate,
    plan_generated_column_invalidation,
};
use super::{
    Function, FunctionArgument, FunctionCall, FunctionId, FunctionOutput, FunctionParameter,
    FunctionSignature, GENERATED_COLUMN_METADATA_KEY, GeneratedColumnBindingSnapshot,
    GeneratedColumnDefinition,
};
use crate::Error;

fn utf8_field_function() -> Function {
    Function::new(
        FunctionId::try_new("fn.exact.b4a.utf8").unwrap(),
        FunctionSignature::try_new(
            vec![FunctionParameter::new("payload", DataType::Utf8)],
            FunctionOutput::new(DataType::Int32, true),
        )
        .unwrap(),
    )
}

fn literal_only_function() -> Function {
    Function::new(
        FunctionId::try_new("fn.exact.b4a.literal").unwrap(),
        FunctionSignature::try_new(
            vec![FunctionParameter::new("constant", DataType::Int32)],
            FunctionOutput::new(DataType::Int32, true),
        )
        .unwrap(),
    )
}

fn int_field_function() -> Function {
    Function::new(
        FunctionId::try_new("fn.exact.b4a.int").unwrap(),
        FunctionSignature::try_new(
            vec![FunctionParameter::new("upstream", DataType::Int32)],
            FunctionOutput::new(DataType::Int32, true),
        )
        .unwrap(),
    )
}

fn field_bound_call(input_field_id: i32) -> FunctionCall {
    FunctionCall::try_new(
        &utf8_field_function(),
        vec![(
            "payload".to_string(),
            FunctionArgument::try_field(input_field_id, DataType::Utf8).unwrap(),
        )],
    )
    .unwrap()
}

fn literal_only_call() -> FunctionCall {
    FunctionCall::try_new(
        &literal_only_function(),
        vec![(
            "constant".to_string(),
            FunctionArgument::try_literal(Arc::new(Int32Array::from(vec![Some(7)])) as ArrayRef)
                .unwrap(),
        )],
    )
    .unwrap()
}

fn int_field_bound_call(input_field_id: i32) -> FunctionCall {
    FunctionCall::try_new(
        &int_field_function(),
        vec![(
            "upstream".to_string(),
            FunctionArgument::try_field(input_field_id, DataType::Int32).unwrap(),
        )],
    )
    .unwrap()
}

fn definition(
    output_field_id: i32,
    call: FunctionCall,
    dependency_epoch: u64,
    materialized_epoch: u64,
) -> GeneratedColumnDefinition {
    GeneratedColumnDefinition::try_new(output_field_id, call, dependency_epoch, materialized_epoch)
        .unwrap()
}

fn ordinary_field(name: &str, data_type: DataType) -> FieldRef {
    Arc::new(Field::new(name, data_type, true))
}

fn generated_field(name: &str, def: &GeneratedColumnDefinition) -> FieldRef {
    let json = def.to_metadata_json().unwrap();
    Arc::new(
        Field::new(name, DataType::Int32, true)
            .with_metadata([(GENERATED_COLUMN_METADATA_KEY.to_string(), json)].into()),
    )
}

fn generated_field_with_raw_metadata(name: &str, raw: &str) -> FieldRef {
    Arc::new(
        Field::new(name, DataType::Int32, true)
            .with_metadata([(GENERATED_COLUMN_METADATA_KEY.to_string(), raw.to_string())].into()),
    )
}

fn snapshot(
    version: u64,
    fields: Vec<FieldRef>,
    field_ids: Vec<i32>,
) -> GeneratedColumnBindingSnapshot {
    GeneratedColumnBindingSnapshot::try_new(version, fields, field_ids).unwrap()
}

fn expected_invalidated(def: &GeneratedColumnDefinition) -> GeneratedColumnDefinition {
    let mut next = def.clone();
    next.invalidate().unwrap();
    next
}

fn assert_planned_definition(
    update: &PlannedGeneratedColumnMetadataUpdate,
    expected: &GeneratedColumnDefinition,
) {
    assert_eq!(update.output_field_id(), expected.output_field_id());
    let decoded = GeneratedColumnDefinition::from_metadata_json(
        update.metadata_json(),
        expected.output_field_id(),
    )
    .expect("planned metadata must decode");
    assert_eq!(&decoded, expected);
    assert_eq!(
        update.metadata_json(),
        expected.to_metadata_json().unwrap(),
        "planned metadata JSON must be canonical"
    );
}

#[test]
fn no_generated_columns_returns_empty_plan() {
    let snap = snapshot(
        1,
        vec![
            ordinary_field("text", DataType::Utf8),
            ordinary_field("score", DataType::Int32),
        ],
        vec![1, 2],
    );
    let before = snap.clone();

    let plan =
        plan_generated_column_invalidation(&snap, &GeneratedColumnMutationImpact::RowSetChanged)
            .expect("planner must succeed when no generated columns are present");
    assert!(plan.is_empty());
    assert_eq!(snap, before, "planner must not mutate the binding snapshot");

    let plan = plan_generated_column_invalidation(
        &snap,
        &GeneratedColumnMutationImpact::UpdatedFields(BTreeSet::from([1])),
    )
    .expect("field update with no generated columns must succeed");
    assert!(plan.is_empty());
    assert_eq!(snap, before, "planner must not mutate the binding snapshot");
}

#[test]
fn row_set_change_invalidates_field_bound_and_literal_only_exactly_once() {
    let text_id = 10;
    let field_bound_id = 20;
    let literal_id = 30;
    let field_bound = definition(field_bound_id, field_bound_call(text_id), 3, 3);
    let literal_only = definition(literal_id, literal_only_call(), 4, 4);
    let snap = snapshot(
        2,
        vec![
            ordinary_field("text", DataType::Utf8),
            generated_field("gen_field", &field_bound),
            generated_field("gen_literal", &literal_only),
        ],
        vec![text_id, field_bound_id, literal_id],
    );
    let before = snap.clone();

    let plan =
        plan_generated_column_invalidation(&snap, &GeneratedColumnMutationImpact::RowSetChanged)
            .expect("row-set change must plan invalidation");
    assert_eq!(snap, before, "planner must not mutate the binding snapshot");
    assert_eq!(
        plan.len(),
        2,
        "each generated column invalidates exactly once"
    );
    assert_eq!(plan[0].output_field_id(), field_bound_id);
    assert_eq!(plan[1].output_field_id(), literal_id);
    assert_planned_definition(&plan[0], &expected_invalidated(&field_bound));
    assert_planned_definition(&plan[1], &expected_invalidated(&literal_only));
}

#[test]
fn already_incomplete_advances_dependency_epoch_and_preserves_materialized_epoch() {
    let text_id = 11;
    let gen_id = 21;
    let incomplete = definition(gen_id, field_bound_call(text_id), 9, 2);
    assert_eq!(incomplete.dependency_epoch(), 9);
    assert_eq!(incomplete.materialized_epoch(), 2);
    let snap = snapshot(
        3,
        vec![
            ordinary_field("text", DataType::Utf8),
            generated_field("gen_incomplete", &incomplete),
        ],
        vec![text_id, gen_id],
    );

    let plan =
        plan_generated_column_invalidation(&snap, &GeneratedColumnMutationImpact::RowSetChanged)
            .expect("incomplete definition must still advance");
    assert_eq!(plan.len(), 1);
    let decoded =
        GeneratedColumnDefinition::from_metadata_json(plan[0].metadata_json(), gen_id).unwrap();
    assert_eq!(decoded.dependency_epoch(), 10);
    assert_eq!(decoded.materialized_epoch(), 2);
    assert_eq!(
        decoded.function_call(),
        incomplete.function_call(),
        "invalidation must preserve the embedded function call"
    );
}

#[test]
fn direct_field_update_invalidates_only_dependent_generated_column() {
    let text_id = 12;
    let score_id = 13;
    let dependent_id = 22;
    let unrelated_gen_id = 23;
    let dependent = definition(dependent_id, field_bound_call(text_id), 5, 5);
    let unrelated_gen = definition(unrelated_gen_id, literal_only_call(), 6, 6);
    let snap = snapshot(
        4,
        vec![
            ordinary_field("text", DataType::Utf8),
            ordinary_field("score", DataType::Int32),
            generated_field("gen_dependent", &dependent),
            generated_field("gen_unrelated", &unrelated_gen),
        ],
        vec![text_id, score_id, dependent_id, unrelated_gen_id],
    );
    let before = snap.clone();

    let plan = plan_generated_column_invalidation(
        &snap,
        &GeneratedColumnMutationImpact::UpdatedFields(BTreeSet::from([text_id])),
    )
    .expect("dependent update must plan a single invalidation");
    assert_eq!(snap, before);
    assert_eq!(plan.len(), 1);
    assert_planned_definition(&plan[0], &expected_invalidated(&dependent));
}

#[test]
fn unrelated_field_update_returns_empty_plan() {
    let text_id = 14;
    let score_id = 15;
    let gen_id = 24;
    let dependent = definition(gen_id, field_bound_call(text_id), 2, 2);
    let snap = snapshot(
        5,
        vec![
            ordinary_field("text", DataType::Utf8),
            ordinary_field("score", DataType::Int32),
            generated_field("gen_text", &dependent),
        ],
        vec![text_id, score_id, gen_id],
    );
    let before = snap.clone();

    let plan = plan_generated_column_invalidation(
        &snap,
        &GeneratedColumnMutationImpact::UpdatedFields(BTreeSet::from([score_id])),
    )
    .expect("unrelated update must not invent invalidation");
    assert!(plan.is_empty());
    assert_eq!(snap, before);
}

#[test]
fn transitive_dependency_propagation_follows_snapshot_order() {
    // A (ordinary) -> B (generated) -> C (generated). Update A invalidates B and C.
    let a_id = 30;
    let b_id = 40;
    let c_id = 50;
    let b = definition(b_id, field_bound_call(a_id), 1, 1);
    let c = definition(c_id, int_field_bound_call(b_id), 1, 1);
    // Schema order places C before B so the plan must follow snapshot order, not
    // dependency discovery order.
    let snap = snapshot(
        6,
        vec![
            ordinary_field("a", DataType::Utf8),
            generated_field("gen_c", &c),
            generated_field("gen_b", &b),
        ],
        vec![a_id, c_id, b_id],
    );
    let before = snap.clone();

    let plan = plan_generated_column_invalidation(
        &snap,
        &GeneratedColumnMutationImpact::UpdatedFields(BTreeSet::from([a_id])),
    )
    .expect("transitive dependents must invalidate");
    assert_eq!(snap, before);
    assert_eq!(plan.len(), 2);
    assert_eq!(plan[0].output_field_id(), c_id);
    assert_eq!(plan[1].output_field_id(), b_id);
    assert_planned_definition(&plan[0], &expected_invalidated(&c));
    assert_planned_definition(&plan[1], &expected_invalidated(&b));
}

#[test]
fn malformed_metadata_fails_closed_for_unrelated_update_without_echoing_payload() {
    const MARKER: &str = "SENSITIVE_B4A_METADATA_MARKER_7c91_e2aa";
    let text_id = 16;
    let score_id = 17;
    let bad_id = 25;
    let raw = format!(
        r#"{{"format_version":1,"output_field_id":{bad_id},"function_call":{MARKER},"dependency_epoch":1,"materialized_epoch":1}}"#
    );
    assert!(raw.contains(MARKER));
    let snap = snapshot(
        7,
        vec![
            ordinary_field("text", DataType::Utf8),
            ordinary_field("score", DataType::Int32),
            generated_field_with_raw_metadata("gen_bad", &raw),
        ],
        vec![text_id, score_id, bad_id],
    );

    let err = plan_generated_column_invalidation(
        &snap,
        &GeneratedColumnMutationImpact::UpdatedFields(BTreeSet::from([score_id])),
    )
    .expect_err("malformed metadata must fail closed even for an unrelated update");
    assert!(
        matches!(err, Error::InvalidInput { .. }),
        "expected InvalidInput, got {err:?}"
    );
    let text = format!("{err}\n{err:?}");
    assert!(
        !text.contains(MARKER),
        "diagnostics must not echo raw metadata marker: {text}"
    );
    assert!(
        !text.contains(&raw),
        "diagnostics must not echo raw metadata payload: {text}"
    );
}

#[test]
fn missing_input_field_id_fails_closed() {
    let missing_input_id = 99;
    let gen_id = 26;
    let orphan = definition(gen_id, field_bound_call(missing_input_id), 1, 1);
    let snap = snapshot(
        8,
        vec![
            ordinary_field("score", DataType::Int32),
            generated_field("gen_orphan", &orphan),
        ],
        vec![18, gen_id],
    );

    let err = plan_generated_column_invalidation(
        &snap,
        &GeneratedColumnMutationImpact::UpdatedFields(BTreeSet::from([18])),
    )
    .expect_err("missing stable input field id must fail closed");
    assert!(
        matches!(err, Error::InvalidInput { .. }),
        "expected InvalidInput, got {err:?}"
    );
    let message = err.to_string();
    assert!(
        message.contains("99") || message.contains("missing"),
        "diagnostic should identify the missing field id: {message}"
    );
}

#[test]
fn field_type_mismatch_fails_closed() {
    let text_id = 19;
    let gen_id = 27;
    // Definition claims Utf8 for field 19, but the snapshot entry is Int32.
    let mismatched = definition(gen_id, field_bound_call(text_id), 1, 1);
    let snap = snapshot(
        9,
        vec![
            ordinary_field("text", DataType::Int32),
            generated_field("gen_mismatch", &mismatched),
        ],
        vec![text_id, gen_id],
    );

    let err = plan_generated_column_invalidation(
        &snap,
        &GeneratedColumnMutationImpact::UpdatedFields(BTreeSet::from([text_id])),
    )
    .expect_err("field type mismatch must fail closed");
    assert!(
        matches!(err, Error::InvalidInput { .. }),
        "expected InvalidInput, got {err:?}"
    );
    let message = err.to_string();
    assert!(
        message.contains("mismatch")
            || (message.contains("Utf8") && message.contains("Int32"))
            || message.contains(&text_id.to_string()),
        "diagnostic should identify the type mismatch: {message}"
    );
}

#[test]
fn epoch_overflow_fails_atomically_with_stable_sanitized_diagnostic() {
    let text_id = 31;
    let overflow_id = 41;
    let other_id = 42;
    let at_max = definition(overflow_id, field_bound_call(text_id), u64::MAX, u64::MAX);
    let other = definition(other_id, literal_only_call(), 1, 1);
    let snap = snapshot(
        10,
        vec![
            ordinary_field("text", DataType::Utf8),
            generated_field("gen_max", &at_max),
            generated_field("gen_other", &other),
        ],
        vec![text_id, overflow_id, other_id],
    );

    // Row-set change impacts every generated column, including the overflowed one.
    let err =
        plan_generated_column_invalidation(&snap, &GeneratedColumnMutationImpact::RowSetChanged)
            .expect_err("dependency_epoch overflow must fail closed");
    match err {
        Error::InvalidInput { message } => {
            assert_eq!(
                message, "dependency_epoch overflow",
                "overflow must use the existing sanitized InvalidInput diagnostic"
            );
        }
        other => panic!("expected InvalidInput overflow, got {other:?}"),
    }

    // Direct update that impacts only the overflowed definition must also fail
    // atomically and must not return a partial plan for sibling columns.
    let err = plan_generated_column_invalidation(
        &snap,
        &GeneratedColumnMutationImpact::UpdatedFields(BTreeSet::from([text_id])),
    )
    .expect_err("impacted overflow must fail with no partial plan");
    match err {
        Error::InvalidInput { message } => {
            assert_eq!(message, "dependency_epoch overflow");
        }
        other => panic!("expected InvalidInput overflow, got {other:?}"),
    }
}
