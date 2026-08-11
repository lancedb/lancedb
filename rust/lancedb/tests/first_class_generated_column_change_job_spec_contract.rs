// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Contract tests for ChangeGeneratedColumnJobSpec (FF-011 / generated-column change Job).
//!
//! These tests pin the intended public surface under [`lancedb::function`] for
//! change-generated-column Job operation input only. They intentionally fail to
//! compile until that API exists.
//!
//! Rejection cases are judged by `Result` structure (`is_err` / `is_ok`), never
//! by diagnostic message substrings. Catalog execution, materialization, CAS
//! commit against current field metadata, and Job lifecycle are out of scope.
//!
//! Intended minimal public API (exact names pinned here):
//! - [`ChangeGeneratedColumnJobSpec::try_new`]`(expected_definition, &new_function, new_call) -> Result<Self>`
//! - [`ChangeGeneratedColumnJobSpec::format_version`] /
//!   [`expected_generated_column_definition`] / [`new_function_call`]
//! - [`ChangeGeneratedColumnJobSpec::validate_against`]`(&Function) -> Result<()>`
//!
//! Strict wire v1 outer object keys are exactly `format_version`,
//! `expected_generated_column_definition`, and `new_function_call`. Sophon
//! JobMetadata alone owns required `table_ref` and `source_table_version`.
//! The constructor validates only the new call against the new Function; the
//! expected definition is an opaque exact CAS precondition and is not
//! catalog-validated against an old Function handle.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lancedb::Result;
use lancedb::function::{
    ChangeGeneratedColumnJobSpec, Function, FunctionArgument, FunctionCall, FunctionId,
    FunctionOutput, FunctionParameter, FunctionSignature, GeneratedColumnDefinition,
};
use lancedb::ipc::schema_to_ipc_file;
use serde_json::Value;

/// Distinctive UTF-8 sentinel for the old call nested in the expected definition.
const OLD_LITERAL_SENTINEL: &str = "REDTEST_CHANGE_OLD_LITERAL_SENTINEL_π_🔒_v1";

/// Distinctive UTF-8 sentinel for the new FunctionCall on the change wire.
const NEW_LITERAL_SENTINEL: &str = "REDTEST_CHANGE_NEW_LITERAL_SENTINEL_Ω_🔓_v1";

/// Stable output field id used across change fixtures.
const OUTPUT_FIELD_ID: i32 = 17;

/// Distinctive dependency / materialized epochs for incomplete change fixtures.
const DEPENDENCY_EPOCH: u64 = 41;
const MATERIALIZED_EPOCH_INCOMPLETE: u64 = 37;

const OLD_FUNCTION_ID: &str = "fn.exact.generated-column.change.old";
const NEW_FUNCTION_ID: &str = "fn.exact.generated-column.change.new";
const OTHER_NEW_FUNCTION_ID: &str = "fn.other.exact.change.new.id";

fn compatible_input_parameters() -> Vec<FunctionParameter> {
    vec![
        FunctionParameter::new("x", DataType::Int32),
        FunctionParameter::new("label", DataType::Utf8),
    ]
}

fn old_signature() -> Result<FunctionSignature> {
    FunctionSignature::try_new(
        compatible_input_parameters(),
        FunctionOutput::new(DataType::Int32, true),
    )
}

/// New Function with the same input parameters but a different output type and
/// nullability (Utf8, non-nullable).
fn new_signature_type_changing() -> Result<FunctionSignature> {
    FunctionSignature::try_new(
        compatible_input_parameters(),
        FunctionOutput::new(DataType::Utf8, false),
    )
}

fn old_function() -> Result<Function> {
    let id = FunctionId::try_new(OLD_FUNCTION_ID)?;
    Ok(Function::new(id, old_signature()?))
}

fn new_function() -> Result<Function> {
    let id = FunctionId::try_new(NEW_FUNCTION_ID)?;
    Ok(Function::new(id, new_signature_type_changing()?))
}

fn other_new_function_same_signature() -> Result<Function> {
    let id = FunctionId::try_new(OTHER_NEW_FUNCTION_ID)?;
    Ok(Function::new(id, new_signature_type_changing()?))
}

fn field_arg(field_id: i32, data_type: DataType) -> Result<FunctionArgument> {
    FunctionArgument::try_field(field_id, data_type)
}

fn int_literal(value: Option<i32>) -> Result<FunctionArgument> {
    FunctionArgument::try_literal(Arc::new(Int32Array::from(vec![value])) as ArrayRef)
}

fn utf8_literal(value: Option<&str>) -> Result<FunctionArgument> {
    FunctionArgument::try_literal(Arc::new(StringArray::from(vec![value])) as ArrayRef)
}

/// Representative call: stable field-ID binding plus typed literal binding.
fn sample_mixed_call(function: &Function) -> Result<FunctionCall> {
    FunctionCall::try_new(
        function,
        vec![
            ("x".to_string(), field_arg(7, DataType::Int32)?),
            ("label".to_string(), utf8_literal(Some("ok"))?),
        ],
    )
}

fn old_literal_sentinel_call(function: &Function) -> Result<FunctionCall> {
    FunctionCall::try_new(
        function,
        vec![
            ("x".to_string(), int_literal(Some(42))?),
            (
                "label".to_string(),
                utf8_literal(Some(OLD_LITERAL_SENTINEL))?,
            ),
        ],
    )
}

fn new_literal_sentinel_call(function: &Function) -> Result<FunctionCall> {
    FunctionCall::try_new(
        function,
        vec![
            ("x".to_string(), int_literal(Some(7))?),
            (
                "label".to_string(),
                utf8_literal(Some(NEW_LITERAL_SENTINEL))?,
            ),
        ],
    )
}

fn complete_definition(function: &Function) -> Result<GeneratedColumnDefinition> {
    let call = sample_mixed_call(function)?;
    GeneratedColumnDefinition::try_new(OUTPUT_FIELD_ID, call, 3, 3)
}

fn incomplete_definition(function: &Function) -> Result<GeneratedColumnDefinition> {
    let call = sample_mixed_call(function)?;
    GeneratedColumnDefinition::try_new(
        OUTPUT_FIELD_ID,
        call,
        DEPENDENCY_EPOCH,
        MATERIALIZED_EPOCH_INCOMPLETE,
    )
}

fn old_sentinel_definition(function: &Function) -> Result<GeneratedColumnDefinition> {
    let call = old_literal_sentinel_call(function)?;
    GeneratedColumnDefinition::try_new(
        OUTPUT_FIELD_ID,
        call,
        DEPENDENCY_EPOCH,
        MATERIALIZED_EPOCH_INCOMPLETE,
    )
}

fn sample_spec() -> Result<ChangeGeneratedColumnJobSpec> {
    let old = old_function()?;
    let new = new_function()?;
    let expected = incomplete_definition(&old)?;
    let new_call = sample_mixed_call(&new)?;
    ChangeGeneratedColumnJobSpec::try_new(expected, &new, new_call)
}

fn assert_json_object_keys_exact(value: &Value, expected: &[&str]) {
    let object = value
        .as_object()
        .unwrap_or_else(|| panic!("expected JSON object, got {value}"));
    let keys: BTreeSet<&str> = object.keys().map(|k| k.as_str()).collect();
    let expected: BTreeSet<&str> = expected.iter().copied().collect();
    assert_eq!(
        keys, expected,
        "JSON object key set must match exactly (iteration order is not a contract); got {keys:?}, expected {expected:?} in {value}"
    );
}

/// Outer-object keys forbidden on ChangeGeneratedColumnJobSpec wire.
///
/// Exact key matching on the outer spec object only. Nested
/// [`GeneratedColumnDefinition`] / [`FunctionCall`] keys such as
/// `output_field_id`, `dependency_epoch`, `materialized_epoch`, `function_id`,
/// `field_id`, `parameter`, and `data_type_ipc` are legitimate and must not be
/// treated as forbidden by recursive scans.
const FORBIDDEN_OUTER_SPEC_KEYS: &[&str] = &[
    "column_name",
    "name",
    "function_name",
    "function",
    "function_call",
    "generated_column_definition",
    "old_function",
    "new_function",
    "old_function_call",
    "old_function_id",
    "new_function_id",
    "signature",
    "parameters",
    "output",
    "output_type",
    "data_type",
    "data_type_ipc",
    "output_field_id",
    "dependency_epoch",
    "materialized_epoch",
    "target_epoch",
    "target_dependency_epoch",
    "target_materialized_epoch",
    "change_mode",
    "refresh_mode",
    "mode",
    "generation",
    "row_range",
    "row_ids",
    "table",
    "table_name",
    "table_ref",
    "source_table_version",
    "version",
    "artifact",
    "artifact_digest",
    "digest",
    "storage",
    "storage_location",
    "location",
    "worker",
    "executor",
    "environment",
    "scheduler",
    "replica",
    "placement",
    "id",
    "job_id",
    "jobId",
    "state",
    "status",
    "attempt",
    "attempt_id",
    "timestamp",
    "created_at",
    "updated_at",
    "lineage",
    "idempotency_key",
    "retry_key",
    "idempotency",
    "commit_token",
    "retry",
];

fn assert_outer_forbidden_keys_absent(value: &Value) {
    assert_json_object_keys_exact(
        value,
        &[
            "format_version",
            "expected_generated_column_definition",
            "new_function_call",
        ],
    );
    let object = value
        .as_object()
        .unwrap_or_else(|| panic!("expected JSON object, got {value}"));
    for key in object.keys() {
        assert!(
            !FORBIDDEN_OUTER_SPEC_KEYS.contains(&key.as_str()),
            "ChangeGeneratedColumnJobSpec wire must not contain forbidden outer key `{key}`: {value}"
        );
    }
}

/// Minimal RFC 4648 base64 encoder for test-only type IPC fixtures used when
/// structurally mutating `data_type_ipc` for catalog-invalid call bindings.
fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((triple >> 18) & 0x3F) as usize] as char);
        out.push(TABLE[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn schema_only_type_ipc_b64(data_type: DataType) -> Result<String> {
    let schema = Schema::new(vec![Field::new("", data_type, true)]);
    Ok(base64_encode(&schema_to_ipc_file(&schema)?))
}

fn utf8_literal_value(argument: &FunctionArgument) -> &str {
    let array = argument.literal_array().expect("literal array");
    let values = array
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8 literal");
    values.value(0)
}

/// Structural decode of a GeneratedColumnDefinition after mutating nested call JSON.
///
/// Public constructors cannot build a catalog-invalid nested FunctionCall; this
/// helper only bypasses that for feeding malformed expected definitions into
/// [`ChangeGeneratedColumnJobSpec::try_new`].
fn structurally_decoded_definition_with_mutated_json(
    mutate: impl FnOnce(&mut Value),
) -> Result<GeneratedColumnDefinition> {
    let old = old_function()?;
    let definition = incomplete_definition(&old)?;
    let mut definition_json =
        serde_json::to_value(&definition).expect("serialize GeneratedColumnDefinition");
    mutate(&mut definition_json);
    Ok(serde_json::from_value(definition_json).expect("structural definition decode"))
}

fn structurally_decoded_new_call_with_mutated_json(
    mutate: impl FnOnce(&mut Value),
) -> Result<FunctionCall> {
    let new = new_function()?;
    let call = sample_mixed_call(&new)?;
    let mut call_json = serde_json::to_value(&call).expect("serialize FunctionCall");
    mutate(&mut call_json);
    Ok(serde_json::from_value(call_json).expect("structural FunctionCall decode"))
}

fn structurally_decoded_change_spec_with_mutated_json(
    mutate: impl FnOnce(&mut Value),
) -> Result<ChangeGeneratedColumnJobSpec> {
    let spec = sample_spec()?;
    let mut spec_json = serde_json::to_value(&spec).expect("serialize change spec");
    mutate(&mut spec_json);
    Ok(serde_json::from_value(spec_json).expect("structural change decode"))
}

#[test]
fn change_round_trip_pins_expected_definition_and_new_function_call() -> Result<()> {
    let old = old_function()?;
    let new = new_function()?;
    assert_ne!(old.id().as_str(), new.id().as_str());
    assert_ne!(
        old.signature().output().data_type(),
        new.signature().output().data_type()
    );
    assert_ne!(
        old.signature().output().nullable(),
        new.signature().output().nullable()
    );

    let expected = incomplete_definition(&old)?;
    let new_call = sample_mixed_call(&new)?;
    let spec = ChangeGeneratedColumnJobSpec::try_new(expected.clone(), &new, new_call.clone())?;

    assert_eq!(spec.format_version(), 1);
    assert_eq!(spec.expected_generated_column_definition(), &expected);
    assert_eq!(
        spec.expected_generated_column_definition()
            .output_field_id(),
        OUTPUT_FIELD_ID
    );
    assert_eq!(
        spec.expected_generated_column_definition()
            .dependency_epoch(),
        DEPENDENCY_EPOCH
    );
    assert_eq!(
        spec.expected_generated_column_definition()
            .materialized_epoch(),
        MATERIALIZED_EPOCH_INCOMPLETE
    );
    assert_eq!(
        spec.expected_generated_column_definition()
            .function_call()
            .function_id()
            .as_str(),
        OLD_FUNCTION_ID
    );
    assert_eq!(spec.new_function_call(), &new_call);
    assert_eq!(
        spec.new_function_call().function_id().as_str(),
        NEW_FUNCTION_ID
    );
    assert_eq!(spec.new_function_call().arguments().len(), 2);
    assert_eq!(spec.new_function_call().arguments()[0].0, "x");
    assert_eq!(
        spec.new_function_call().arguments()[0].1.field_id(),
        Some(7)
    );
    assert_eq!(spec.new_function_call().arguments()[1].0, "label");
    assert_eq!(
        spec.new_function_call().arguments()[1].1.data_type(),
        &DataType::Utf8
    );

    let json = serde_json::to_value(&spec).expect("serialize change spec");
    assert_eq!(json["format_version"], 1);
    assert_json_object_keys_exact(
        &json,
        &[
            "format_version",
            "expected_generated_column_definition",
            "new_function_call",
        ],
    );
    assert_json_object_keys_exact(
        &json["expected_generated_column_definition"],
        &[
            "format_version",
            "output_field_id",
            "function_call",
            "dependency_epoch",
            "materialized_epoch",
        ],
    );
    assert_json_object_keys_exact(&json["new_function_call"], &["function_id", "arguments"]);
    assert_eq!(
        json["expected_generated_column_definition"]["output_field_id"],
        Value::from(OUTPUT_FIELD_ID)
    );
    assert_eq!(
        json["expected_generated_column_definition"]["function_call"]["function_id"],
        Value::String(OLD_FUNCTION_ID.into())
    );
    assert_eq!(
        json["new_function_call"]["function_id"],
        Value::String(NEW_FUNCTION_ID.into())
    );

    let restored: ChangeGeneratedColumnJobSpec =
        serde_json::from_value(json.clone()).expect("deserialize change spec");
    assert_eq!(restored.format_version(), 1);
    assert_eq!(
        restored.expected_generated_column_definition(),
        spec.expected_generated_column_definition()
    );
    assert_eq!(restored.new_function_call(), spec.new_function_call());

    let encoded_a = serde_json::to_string(&spec).expect("encode a");
    let encoded_b = serde_json::to_string(&spec).expect("encode b");
    assert_eq!(encoded_a, encoded_b);
    assert_eq!(
        serde_json::to_value(&restored).expect("re-serialize restored"),
        json
    );
    Ok(())
}

#[test]
fn constructor_accepts_complete_incomplete_same_call_and_type_changing_new_function() -> Result<()>
{
    let old = old_function()?;
    let new = new_function()?;

    let complete = complete_definition(&old)?;
    let incomplete = incomplete_definition(&old)?;
    assert_ne!(
        incomplete.materialized_epoch(),
        incomplete.dependency_epoch(),
        "incomplete fixture must not invent an incomplete-only constructor rule"
    );

    let complete_new_call = sample_mixed_call(&new)?;
    let incomplete_new_call = sample_mixed_call(&new)?;
    assert!(
        ChangeGeneratedColumnJobSpec::try_new(complete, &new, complete_new_call).is_ok(),
        "complete expected definition must construct"
    );
    assert!(
        ChangeGeneratedColumnJobSpec::try_new(incomplete, &new, incomplete_new_call).is_ok(),
        "incomplete expected definition must construct"
    );

    // Same-call change: new_call equals the call already nested in expected.
    // No no-op / same-call restriction may reject this.
    let same_call_expected = incomplete_definition(&old)?;
    let same_call = same_call_expected.function_call().clone();
    assert!(
        ChangeGeneratedColumnJobSpec::try_new(same_call_expected, &old, same_call).is_ok(),
        "same-call change must construct without a no-op restriction"
    );

    // Type-changing new Function (distinct output type/nullability) is valid.
    assert_ne!(
        old.signature().output().data_type(),
        new.signature().output().data_type()
    );
    assert_ne!(
        old.signature().output().nullable(),
        new.signature().output().nullable()
    );
    let type_change_expected = incomplete_definition(&old)?;
    let type_change_call = sample_mixed_call(&new)?;
    assert!(
        ChangeGeneratedColumnJobSpec::try_new(type_change_expected, &new, type_change_call).is_ok(),
        "new Function with different output type/nullability must construct"
    );
    Ok(())
}

#[test]
fn constructor_does_not_validate_old_call_against_catalog() -> Result<()> {
    let new = new_function()?;
    let new_call = sample_mixed_call(&new)?;

    // Structurally mutate the expected definition's nested call so it is
    // catalog-invalid against the old Function (parameter order), but still
    // internally decodable. Constructor must accept it as an opaque CAS
    // precondition without requiring an old Function handle.
    let reordered_expected =
        structurally_decoded_definition_with_mutated_json(|definition_json| {
            definition_json["function_call"]["arguments"]
                .as_array_mut()
                .expect("arguments")
                .swap(0, 1);
        })?;
    assert!(
        ChangeGeneratedColumnJobSpec::try_new(reordered_expected, &new, new_call.clone()).is_ok(),
        "catalog-invalid old call parameter order must still construct as CAS precondition"
    );

    // Type-mutated old call is likewise accepted without catalog validation.
    let type_mismatch_expected =
        structurally_decoded_definition_with_mutated_json(|definition_json| {
            definition_json["function_call"]["arguments"][0]["value"]["data_type_ipc"] =
                Value::String(schema_only_type_ipc_b64(DataType::Utf8).expect("type ipc"));
        })?;
    assert!(
        ChangeGeneratedColumnJobSpec::try_new(type_mismatch_expected, &new, new_call).is_ok(),
        "catalog-invalid old call argument type must still construct as CAS precondition"
    );
    Ok(())
}

#[test]
fn constructor_and_validate_against_reject_new_call_mismatches() -> Result<()> {
    let old = old_function()?;
    let new = new_function()?;
    let expected = incomplete_definition(&old)?;

    let matching_call = sample_mixed_call(&new)?;
    assert!(
        ChangeGeneratedColumnJobSpec::try_new(expected.clone(), &new, matching_call).is_ok(),
        "matching new Function and new FunctionCall must construct"
    );

    // Exact new Function identity mismatch: call pinned to another Function ID.
    let other = other_new_function_same_signature()?;
    let other_call = sample_mixed_call(&other)?;
    assert!(
        ChangeGeneratedColumnJobSpec::try_new(expected.clone(), &new, other_call).is_err(),
        "new Function ID mismatch must be rejected by constructor"
    );

    // Constructor must rerun full FunctionCall::validate_against for the new call.
    let reordered_call = structurally_decoded_new_call_with_mutated_json(|call_json| {
        call_json["arguments"]
            .as_array_mut()
            .expect("arguments")
            .swap(0, 1);
    })?;
    assert!(
        ChangeGeneratedColumnJobSpec::try_new(expected.clone(), &new, reordered_call).is_err(),
        "reordered new-call parameter bindings must be rejected by constructor"
    );

    let renamed_call = structurally_decoded_new_call_with_mutated_json(|call_json| {
        call_json["arguments"][0]["parameter"] = Value::String("renamed".into());
    })?;
    assert!(
        ChangeGeneratedColumnJobSpec::try_new(expected.clone(), &new, renamed_call).is_err(),
        "new-call parameter name mismatch must be rejected by constructor"
    );

    let type_mismatch_call = structurally_decoded_new_call_with_mutated_json(|call_json| {
        call_json["arguments"][0]["value"]["data_type_ipc"] =
            Value::String(schema_only_type_ipc_b64(DataType::Utf8).expect("type ipc"));
    })?;
    assert!(
        ChangeGeneratedColumnJobSpec::try_new(expected, &new, type_mismatch_call).is_err(),
        "new-call Arrow type mismatch must be rejected by constructor"
    );

    // Separate execution-consumer guarantee: structural decode of the outer
    // spec bypasses try_new; validate_against must still reject the same
    // new-call mutations.
    let reordered_spec = structurally_decoded_change_spec_with_mutated_json(|spec_json| {
        spec_json["new_function_call"]["arguments"]
            .as_array_mut()
            .expect("arguments")
            .swap(0, 1);
    })?;
    assert!(
        reordered_spec.validate_against(&new).is_err(),
        "reordered new-call parameter bindings must fail validate_against"
    );

    let renamed_spec = structurally_decoded_change_spec_with_mutated_json(|spec_json| {
        spec_json["new_function_call"]["arguments"][0]["parameter"] =
            Value::String("renamed".into());
    })?;
    assert!(
        renamed_spec.validate_against(&new).is_err(),
        "new-call parameter name mismatch must fail validate_against"
    );

    let type_mismatch_spec = structurally_decoded_change_spec_with_mutated_json(|spec_json| {
        spec_json["new_function_call"]["arguments"][0]["value"]["data_type_ipc"] =
            Value::String(schema_only_type_ipc_b64(DataType::Utf8).expect("type ipc"));
    })?;
    assert!(
        type_mismatch_spec.validate_against(&new).is_err(),
        "new-call Arrow type mismatch must fail validate_against"
    );

    let id_mismatch_spec = structurally_decoded_change_spec_with_mutated_json(|spec_json| {
        spec_json["new_function_call"]["function_id"] = Value::String(OTHER_NEW_FUNCTION_ID.into());
    })?;
    assert!(
        id_mismatch_spec.validate_against(&new).is_err(),
        "new Function ID mismatch must fail validate_against"
    );
    Ok(())
}

#[test]
fn negative_output_id_reversed_epochs_empty_function_ids_and_unknowns_fail_closed() -> Result<()> {
    let old = old_function()?;
    let call = sample_mixed_call(&old)?;

    assert!(
        GeneratedColumnDefinition::try_new(-1, call.clone(), 1, 1).is_err(),
        "negative output_field_id must be rejected"
    );
    assert!(
        GeneratedColumnDefinition::try_new(OUTPUT_FIELD_ID, call, 4, 5).is_err(),
        "materialized_epoch > dependency_epoch must be rejected"
    );

    let definition = incomplete_definition(&old)?;
    let definition_json =
        serde_json::to_value(&definition).expect("serialize GeneratedColumnDefinition");

    let mut negative_id = definition_json.clone();
    negative_id["output_field_id"] = Value::from(-1);
    assert!(
        serde_json::from_value::<GeneratedColumnDefinition>(negative_id).is_err(),
        "generic deserialize must reject negative output_field_id"
    );

    let mut reversed_epochs = definition_json.clone();
    reversed_epochs["dependency_epoch"] = Value::from(4);
    reversed_epochs["materialized_epoch"] = Value::from(5);
    assert!(
        serde_json::from_value::<GeneratedColumnDefinition>(reversed_epochs).is_err(),
        "generic deserialize must reject materialized_epoch > dependency_epoch"
    );

    let mut empty_old_function_id = definition_json.clone();
    empty_old_function_id["function_call"]["function_id"] = Value::String("".into());
    assert!(
        serde_json::from_value::<GeneratedColumnDefinition>(empty_old_function_id).is_err(),
        "generic deserialize must reject empty nested old Function ID"
    );

    let mut unknown_definition_field = definition_json.clone();
    unknown_definition_field
        .as_object_mut()
        .unwrap()
        .insert("unexpected_field".into(), Value::Bool(true));
    assert!(
        serde_json::from_value::<GeneratedColumnDefinition>(unknown_definition_field).is_err(),
        "unknown expected-definition field must fail closed"
    );

    let mut unknown_definition_version = definition_json.clone();
    unknown_definition_version["format_version"] = Value::from(2);
    assert!(
        serde_json::from_value::<GeneratedColumnDefinition>(unknown_definition_version).is_err(),
        "definition format_version other than 1 must fail closed"
    );

    let spec = sample_spec()?;
    let json = serde_json::to_value(&spec).expect("serialize change spec");

    let mut empty_new_function_id = json.clone();
    empty_new_function_id["new_function_call"]["function_id"] = Value::String("".into());
    assert!(
        serde_json::from_value::<ChangeGeneratedColumnJobSpec>(empty_new_function_id).is_err(),
        "decode must reject empty new Function ID"
    );

    let mut unknown_new_call_field = json.clone();
    unknown_new_call_field["new_function_call"]
        .as_object_mut()
        .unwrap()
        .insert("unexpected_field".into(), Value::Bool(true));
    assert!(
        serde_json::from_value::<ChangeGeneratedColumnJobSpec>(unknown_new_call_field).is_err(),
        "unknown new-call field must fail closed"
    );

    let mut unknown_outer_field = json.clone();
    unknown_outer_field
        .as_object_mut()
        .unwrap()
        .insert("unexpected_field".into(), Value::Bool(true));
    assert!(
        serde_json::from_value::<ChangeGeneratedColumnJobSpec>(unknown_outer_field).is_err(),
        "unknown change outer field must fail closed"
    );

    let mut unknown_outer_version = json.clone();
    unknown_outer_version["format_version"] = Value::from(2);
    assert!(
        serde_json::from_value::<ChangeGeneratedColumnJobSpec>(unknown_outer_version).is_err(),
        "change format_version other than 1 must fail closed"
    );

    let mut unknown_expected_field = json.clone();
    unknown_expected_field["expected_generated_column_definition"]
        .as_object_mut()
        .unwrap()
        .insert("unexpected_field".into(), Value::Bool(true));
    assert!(
        serde_json::from_value::<ChangeGeneratedColumnJobSpec>(unknown_expected_field).is_err(),
        "unknown nested expected-definition field must fail closed"
    );
    Ok(())
}

#[test]
fn outer_wire_excludes_forbidden_fields_and_keeps_ids_epochs_nested() -> Result<()> {
    let old = old_function()?;
    let new = new_function()?;
    let expected = incomplete_definition(&old)?;
    let new_call = sample_mixed_call(&new)?;
    let spec = ChangeGeneratedColumnJobSpec::try_new(expected, &new, new_call)?;
    let json = serde_json::to_value(&spec).expect("serialize change spec");
    assert_outer_forbidden_keys_absent(&json);

    // Nested expected definition carries stable field identity and epochs; outer must not.
    assert!(json.get("output_field_id").is_none());
    assert!(json.get("dependency_epoch").is_none());
    assert!(json.get("materialized_epoch").is_none());
    assert!(json.get("column_name").is_none());
    assert!(json.get("target_epoch").is_none());
    assert!(json.get("table").is_none());
    assert!(json.get("table_ref").is_none());
    assert!(json.get("source_table_version").is_none());
    assert!(json.get("version").is_none());
    assert!(json.get("state").is_none());
    assert!(json.get("status").is_none());
    assert!(json.get("job_id").is_none());
    assert!(json.get("function_call").is_none());
    assert!(json.get("generated_column_definition").is_none());
    assert!(json.get("function_id").is_none());

    assert_eq!(
        json["expected_generated_column_definition"]["output_field_id"],
        Value::from(OUTPUT_FIELD_ID)
    );
    assert_eq!(
        json["expected_generated_column_definition"]["dependency_epoch"],
        Value::from(DEPENDENCY_EPOCH)
    );
    assert_eq!(
        json["expected_generated_column_definition"]["materialized_epoch"],
        Value::from(MATERIALIZED_EPOCH_INCOMPLETE)
    );
    assert_eq!(
        json["expected_generated_column_definition"]["function_call"]["function_id"],
        Value::String(OLD_FUNCTION_ID.into())
    );
    assert_eq!(
        json["expected_generated_column_definition"]["function_call"]["arguments"][0]["value"]["field_id"],
        Value::from(7)
    );
    assert_eq!(
        json["new_function_call"]["function_id"],
        Value::String(NEW_FUNCTION_ID.into())
    );
    assert_eq!(
        json["new_function_call"]["arguments"][0]["value"]["field_id"],
        Value::from(7)
    );

    let literal_expected = old_sentinel_definition(&old)?;
    let literal_new_call = new_literal_sentinel_call(&new)?;
    let literal_spec =
        ChangeGeneratedColumnJobSpec::try_new(literal_expected, &new, literal_new_call)?;
    let literal_json = serde_json::to_value(&literal_spec).expect("serialize literal change spec");
    assert_outer_forbidden_keys_absent(&literal_json);
    Ok(())
}

#[test]
fn structural_decode_then_validate_against_matching_and_mismatching_new_function() -> Result<()> {
    let new = new_function()?;
    let spec = sample_spec()?;
    let json = serde_json::to_value(&spec).expect("serialize change spec");

    let decoded: ChangeGeneratedColumnJobSpec =
        serde_json::from_value(json).expect("structural decode without catalog/table");
    assert_eq!(
        decoded
            .expected_generated_column_definition()
            .function_call()
            .function_id()
            .as_str(),
        OLD_FUNCTION_ID
    );
    assert_eq!(
        decoded.new_function_call().function_id().as_str(),
        new.id().as_str()
    );

    decoded.validate_against(&new)?;

    let other = other_new_function_same_signature()?;
    assert!(
        decoded.validate_against(&other).is_err(),
        "validate_against must reject a catalog Function with a different exact ID"
    );
    Ok(())
}

#[test]
fn debug_redacts_literal_payloads_while_trusted_getters_retain_them() -> Result<()> {
    let old = old_function()?;
    let new = new_function()?;
    let expected = old_sentinel_definition(&old)?;
    let new_call = new_literal_sentinel_call(&new)?;
    let spec = ChangeGeneratedColumnJobSpec::try_new(expected, &new, new_call)?;

    // Trusted getters retain both distinct typed literal payloads.
    let old_label = &spec
        .expected_generated_column_definition()
        .function_call()
        .arguments()[1]
        .1;
    assert_eq!(utf8_literal_value(old_label), OLD_LITERAL_SENTINEL);

    let new_label = &spec.new_function_call().arguments()[1].1;
    assert_eq!(utf8_literal_value(new_label), NEW_LITERAL_SENTINEL);

    let json = serde_json::to_value(&spec).expect("serialize change spec");
    let restored: ChangeGeneratedColumnJobSpec =
        serde_json::from_value(json).expect("deserialize change spec");
    let restored_old_label = &restored
        .expected_generated_column_definition()
        .function_call()
        .arguments()[1]
        .1;
    assert_eq!(utf8_literal_value(restored_old_label), OLD_LITERAL_SENTINEL);
    let restored_new_label = &restored.new_function_call().arguments()[1].1;
    assert_eq!(utf8_literal_value(restored_new_label), NEW_LITERAL_SENTINEL);

    let debug = format!("{spec:?}");
    assert!(
        debug.contains(&OUTPUT_FIELD_ID.to_string()),
        "Debug may show stable output field ID: {debug}"
    );
    assert!(
        debug.contains(OLD_FUNCTION_ID),
        "Debug may show old exact Function ID: {debug}"
    );
    assert!(
        debug.contains(NEW_FUNCTION_ID),
        "Debug may show new exact Function ID: {debug}"
    );
    assert!(
        debug.contains(&DEPENDENCY_EPOCH.to_string()),
        "Debug may show dependency_epoch: {debug}"
    );
    assert!(
        debug.contains(&MATERIALIZED_EPOCH_INCOMPLETE.to_string()),
        "Debug may show materialized_epoch: {debug}"
    );
    assert!(
        !debug.contains(OLD_LITERAL_SENTINEL),
        "ChangeGeneratedColumnJobSpec Debug must not reveal old typed literal payload: {debug}"
    );
    assert!(
        !debug.contains(NEW_LITERAL_SENTINEL),
        "ChangeGeneratedColumnJobSpec Debug must not reveal new typed literal payload: {debug}"
    );
    assert!(
        !debug.contains("FunctionCall"),
        "ChangeGeneratedColumnJobSpec Debug must not format nested FunctionCall: {debug}"
    );
    assert!(
        !debug.contains("GeneratedColumnDefinition"),
        "ChangeGeneratedColumnJobSpec Debug must not format nested GeneratedColumnDefinition: {debug}"
    );
    Ok(())
}
