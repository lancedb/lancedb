// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Contract tests for CreateGeneratedColumnJobSpec (FF-009 / generated-column create Job).
//!
//! These tests pin the intended public surface under [`lancedb::function`] for
//! create-generated-column Job operation input only. They intentionally fail to
//! compile until that API exists.
//!
//! Rejection cases are judged by `Result` structure (`is_err` / `is_ok`), never
//! by diagnostic message substrings. Catalog execution, materialization, and
//! Job lifecycle are out of scope.
//!
//! Intended minimal public API (exact names pinned here):
//! - [`CreateGeneratedColumnJobSpec::try_new`]`(column_name, &function, call) -> Result<Self>`
//! - [`CreateGeneratedColumnJobSpec::format_version`] / [`column_name`] /
//!   [`function_call`]
//! - [`CreateGeneratedColumnJobSpec::validate_against`]`(&Function) -> Result<()>`
//!
//! Strict wire v1 outer object keys are exactly `format_version`, `column_name`,
//! and `function_call`. Sophon JobMetadata owns table/version/lifecycle fields.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lancedb::Result;
use lancedb::function::{
    CreateGeneratedColumnJobSpec, Function, FunctionArgument, FunctionCall, FunctionId,
    FunctionOutput, FunctionParameter, FunctionSignature,
};
use lancedb::ipc::schema_to_ipc_file;
use serde_json::Value;

/// Distinctive UTF-8 sentinel used only for Debug redaction vs trusted wire.
const LITERAL_SENTINEL: &str = "REDTEST_LITERAL_SENTINEL_π_🔒_v1";

fn sample_signature() -> Result<FunctionSignature> {
    FunctionSignature::try_new(
        vec![
            FunctionParameter::new("x", DataType::Int32),
            FunctionParameter::new("label", DataType::Utf8),
        ],
        FunctionOutput::new(DataType::Int32, true),
    )
}

fn sample_function() -> Result<Function> {
    let id = FunctionId::try_new("fn.exact.generated-column.create")?;
    Ok(Function::new(id, sample_signature()?))
}

fn other_function_same_signature() -> Result<Function> {
    let id = FunctionId::try_new("fn.other.exact.id")?;
    Ok(Function::new(id, sample_signature()?))
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

fn sample_literal_sentinel_call(function: &Function) -> Result<FunctionCall> {
    FunctionCall::try_new(
        function,
        vec![
            ("x".to_string(), int_literal(Some(42))?),
            ("label".to_string(), utf8_literal(Some(LITERAL_SENTINEL))?),
        ],
    )
}

fn sample_spec() -> Result<CreateGeneratedColumnJobSpec> {
    let function = sample_function()?;
    let call = sample_mixed_call(&function)?;
    CreateGeneratedColumnJobSpec::try_new("score_normalized", &function, call)
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

/// Outer-object keys forbidden on CreateGeneratedColumnJobSpec wire.
///
/// Exact key matching on the outer spec object only. Nested FunctionCall keys
/// such as `function_id`, `field_id`, `parameter`, and `data_type_ipc` are
/// legitimate and must not be treated as forbidden by recursive scans.
const FORBIDDEN_OUTER_SPEC_KEYS: &[&str] = &[
    "name",
    "function_name",
    "function",
    "signature",
    "parameters",
    "output",
    "output_type",
    "data_type",
    "data_type_ipc",
    "output_field_id",
    "dependency_epoch",
    "materialized_epoch",
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
    assert_json_object_keys_exact(value, &["format_version", "column_name", "function_call"]);
    let object = value
        .as_object()
        .unwrap_or_else(|| panic!("expected JSON object, got {value}"));
    for key in object.keys() {
        assert!(
            !FORBIDDEN_OUTER_SPEC_KEYS.contains(&key.as_str()),
            "CreateGeneratedColumnJobSpec wire must not contain forbidden outer key `{key}`: {value}"
        );
    }
}

/// Minimal RFC 4648 base64 encoder for test-only type IPC fixtures.
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

/// Minimal RFC 4648 base64 decoder for test-only wire inspection.
fn base64_decode(input: &str) -> std::result::Result<Vec<u8>, String> {
    fn decode_char(c: u8) -> std::result::Result<u8, String> {
        match c {
            b'A'..=b'Z' => Ok(c - b'A'),
            b'a'..=b'z' => Ok(c - b'a' + 26),
            b'0'..=b'9' => Ok(c - b'0' + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(format!("invalid base64 byte: {c}")),
        }
    }

    let bytes = input.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err("base64 length must be a multiple of 4".into());
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        let pad = chunk.iter().filter(|&&b| b == b'=').count();
        let c0 = decode_char(chunk[0])?;
        let c1 = decode_char(chunk[1])?;
        let c2 = if chunk[2] == b'=' {
            0
        } else {
            decode_char(chunk[2])?
        };
        let c3 = if chunk[3] == b'=' {
            0
        } else {
            decode_char(chunk[3])?
        };
        let triple = ((c0 as u32) << 18) | ((c1 as u32) << 12) | ((c2 as u32) << 6) | (c3 as u32);
        out.push(((triple >> 16) & 0xFF) as u8);
        if pad < 2 {
            out.push(((triple >> 8) & 0xFF) as u8);
        }
        if pad < 1 {
            out.push((triple & 0xFF) as u8);
        }
    }
    Ok(out)
}

fn schema_only_type_ipc_b64(data_type: DataType) -> Result<String> {
    let schema = Schema::new(vec![Field::new("", data_type, true)]);
    Ok(base64_encode(&schema_to_ipc_file(&schema)?))
}

fn bytes_contain(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn first_literal_ipc(spec_json: &Value) -> String {
    let args = spec_json
        .pointer("/function_call/arguments")
        .and_then(Value::as_array)
        .expect("function_call.arguments array");
    for arg in args {
        let value = arg
            .get("value")
            .and_then(Value::as_object)
            .expect("argument.value object");
        if value.get("kind").and_then(Value::as_str) == Some("literal") {
            return value
                .get("ipc")
                .and_then(Value::as_str)
                .expect("literal value.ipc string")
                .to_string();
        }
    }
    panic!("expected at least one literal argument with ipc");
}

fn structurally_decoded_call_with_mutated_json(
    mutate: impl FnOnce(&mut Value),
) -> Result<FunctionCall> {
    let function = sample_function()?;
    let call = sample_mixed_call(&function)?;
    let mut call_json = serde_json::to_value(&call).expect("serialize FunctionCall");
    mutate(&mut call_json);
    Ok(serde_json::from_value(call_json).expect("structural FunctionCall decode"))
}

#[test]
fn create_round_trip_pins_column_name_and_nested_function_call() -> Result<()> {
    let function = sample_function()?;
    let call = sample_mixed_call(&function)?;
    let spec = CreateGeneratedColumnJobSpec::try_new("score_normalized", &function, call)?;

    assert_eq!(spec.format_version(), 1);
    assert_eq!(spec.column_name(), "score_normalized");
    assert_eq!(
        spec.function_call().function_id().as_str(),
        "fn.exact.generated-column.create"
    );
    assert_eq!(spec.function_call().arguments().len(), 2);
    assert_eq!(spec.function_call().arguments()[0].0, "x");
    assert_eq!(spec.function_call().arguments()[0].1.field_id(), Some(7));
    assert_eq!(spec.function_call().arguments()[1].0, "label");
    assert_eq!(
        spec.function_call().arguments()[1].1.data_type(),
        &DataType::Utf8
    );

    let json = serde_json::to_value(&spec).expect("serialize create spec");
    assert_eq!(json["format_version"], 1);
    assert_eq!(
        json["column_name"],
        Value::String("score_normalized".into())
    );
    assert_eq!(
        json["function_call"]["function_id"],
        Value::String("fn.exact.generated-column.create".into())
    );
    assert_json_object_keys_exact(&json, &["format_version", "column_name", "function_call"]);
    assert_json_object_keys_exact(&json["function_call"], &["function_id", "arguments"]);

    let restored: CreateGeneratedColumnJobSpec =
        serde_json::from_value(json.clone()).expect("deserialize create spec");
    assert_eq!(restored.format_version(), 1);
    assert_eq!(restored.column_name(), "score_normalized");
    assert_eq!(
        restored.function_call().function_id().as_str(),
        spec.function_call().function_id().as_str()
    );
    assert_eq!(
        restored.function_call().arguments().len(),
        spec.function_call().arguments().len()
    );
    assert_eq!(
        restored.function_call().arguments()[0].1.field_id(),
        spec.function_call().arguments()[0].1.field_id()
    );
    assert_eq!(
        restored.function_call().arguments()[1].1.data_type(),
        spec.function_call().arguments()[1].1.data_type()
    );

    // Same value serializes byte-identically.
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
fn constructor_validates_exact_function_identity_parameter_order_and_types() -> Result<()> {
    let function = sample_function()?;
    let matching_call = sample_mixed_call(&function)?;
    assert!(
        CreateGeneratedColumnJobSpec::try_new("score_normalized", &function, matching_call).is_ok(),
        "matching Function and FunctionCall must construct"
    );

    // Exact Function identity mismatch: call pinned to another Function ID.
    let other = other_function_same_signature()?;
    let other_call = sample_mixed_call(&other)?;
    assert!(
        CreateGeneratedColumnJobSpec::try_new("score_normalized", &function, other_call).is_err(),
        "Function ID mismatch must be rejected by constructor"
    );

    // Public FunctionCall::try_new cannot produce a malformed call; use structural
    // JSON fixtures for parameter name/order and Arrow type mismatches.
    let reordered_call = structurally_decoded_call_with_mutated_json(|call_json| {
        call_json["arguments"]
            .as_array_mut()
            .expect("arguments")
            .swap(0, 1);
    })?;
    assert!(
        CreateGeneratedColumnJobSpec::try_new("score_normalized", &function, reordered_call)
            .is_err(),
        "reordered parameter bindings must be rejected by constructor"
    );

    let renamed_call = structurally_decoded_call_with_mutated_json(|call_json| {
        call_json["arguments"][0]["parameter"] = Value::String("renamed".into());
    })?;
    assert!(
        CreateGeneratedColumnJobSpec::try_new("score_normalized", &function, renamed_call).is_err(),
        "parameter name mismatch must be rejected by constructor"
    );

    let type_mismatch_call = structurally_decoded_call_with_mutated_json(|call_json| {
        call_json["arguments"][0]["value"]["data_type_ipc"] =
            Value::String(schema_only_type_ipc_b64(DataType::Utf8).expect("type ipc"));
    })?;
    assert!(
        CreateGeneratedColumnJobSpec::try_new("score_normalized", &function, type_mismatch_call)
            .is_err(),
        "Arrow type mismatch must be rejected by constructor"
    );
    Ok(())
}

#[test]
fn empty_name_unknown_outer_field_and_unknown_version_fail_closed() -> Result<()> {
    let function = sample_function()?;
    let call = sample_mixed_call(&function)?;

    assert!(
        CreateGeneratedColumnJobSpec::try_new("", &function, call).is_err(),
        "empty column_name must be rejected by constructor"
    );

    let spec = sample_spec()?;
    let json = serde_json::to_value(&spec).expect("serialize create spec");

    let mut empty_name = json.clone();
    empty_name["column_name"] = Value::String("".into());
    assert!(
        serde_json::from_value::<CreateGeneratedColumnJobSpec>(empty_name).is_err(),
        "decode must reject empty column_name"
    );

    let mut unknown_field = json.clone();
    unknown_field
        .as_object_mut()
        .unwrap()
        .insert("unexpected_field".into(), Value::Bool(true));
    assert!(
        serde_json::from_value::<CreateGeneratedColumnJobSpec>(unknown_field).is_err(),
        "unknown outer field must fail closed"
    );

    let mut unknown_version = json.clone();
    unknown_version["format_version"] = Value::from(2);
    assert!(
        serde_json::from_value::<CreateGeneratedColumnJobSpec>(unknown_version).is_err(),
        "format_version other than 1 must fail closed"
    );
    Ok(())
}

#[test]
fn outer_wire_excludes_forbidden_operation_and_lifecycle_fields() -> Result<()> {
    let function = sample_function()?;
    let field_and_literal = sample_mixed_call(&function)?;
    let mixed =
        CreateGeneratedColumnJobSpec::try_new("score_normalized", &function, field_and_literal)?;
    let mixed_json = serde_json::to_value(&mixed).expect("serialize mixed spec");
    assert_outer_forbidden_keys_absent(&mixed_json);

    // Nested call may carry exact Function ID and field_id; outer must not.
    assert!(mixed_json.get("function_id").is_none());
    assert!(mixed_json.get("field_id").is_none());
    assert!(mixed_json.get("output_field_id").is_none());
    assert!(mixed_json.get("dependency_epoch").is_none());
    assert!(mixed_json.get("materialized_epoch").is_none());
    assert_eq!(
        mixed_json["function_call"]["function_id"],
        Value::String("fn.exact.generated-column.create".into())
    );
    assert_eq!(
        mixed_json["function_call"]["arguments"][0]["value"]["field_id"],
        Value::from(7)
    );

    let literal_only = sample_literal_sentinel_call(&function)?;
    let literal_spec =
        CreateGeneratedColumnJobSpec::try_new("label_copy", &function, literal_only)?;
    let literal_json = serde_json::to_value(&literal_spec).expect("serialize literal spec");
    assert_outer_forbidden_keys_absent(&literal_json);
    Ok(())
}

#[test]
fn structural_decode_then_validate_against_matching_and_mismatching_catalog_function() -> Result<()>
{
    let function = sample_function()?;
    let call = sample_mixed_call(&function)?;
    let spec = CreateGeneratedColumnJobSpec::try_new("score_normalized", &function, call)?;
    let json = serde_json::to_value(&spec).expect("serialize create spec");

    let decoded: CreateGeneratedColumnJobSpec =
        serde_json::from_value(json).expect("structural decode without catalog");
    assert_eq!(decoded.column_name(), "score_normalized");
    assert_eq!(
        decoded.function_call().function_id().as_str(),
        function.id().as_str()
    );

    decoded.validate_against(&function)?;

    let other = other_function_same_signature()?;
    assert!(
        decoded.validate_against(&other).is_err(),
        "validate_against must reject a catalog Function with a different exact ID"
    );

    // Structurally decoded call with reordered bindings must fail validate_against.
    let mut mutated = serde_json::to_value(&spec).expect("serialize for mutation");
    mutated["function_call"]["arguments"]
        .as_array_mut()
        .expect("arguments")
        .swap(0, 1);
    let reordered: CreateGeneratedColumnJobSpec =
        serde_json::from_value(mutated).expect("structural decode of reordered call");
    assert!(
        reordered.validate_against(&function).is_err(),
        "reordered nested bindings must fail validate_against"
    );
    Ok(())
}

#[test]
fn debug_redacts_literal_payload_while_trusted_wire_retains_it() -> Result<()> {
    let function = sample_function()?;
    let call = sample_literal_sentinel_call(&function)?;
    let spec = CreateGeneratedColumnJobSpec::try_new("label_copy", &function, call)?;

    // Trusted getters retain the typed literal payload.
    let label = &spec.function_call().arguments()[1].1;
    let array = label.literal_array().expect("label literal array");
    let values = array
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8 literal");
    assert_eq!(values.value(0), LITERAL_SENTINEL);

    let json = serde_json::to_value(&spec).expect("serialize create spec");
    // Both literals are present; the sentinel lives in the Utf8 label binding.
    assert!(!first_literal_ipc(&json).is_empty());
    let label_ipc = json["function_call"]["arguments"][1]["value"]["ipc"]
        .as_str()
        .expect("label literal ipc");
    let ipc_bytes = base64_decode(label_ipc).expect("literal ipc base64");
    assert!(
        bytes_contain(&ipc_bytes, LITERAL_SENTINEL.as_bytes()),
        "trusted wire IPC payload must retain the UTF-8 literal sentinel"
    );

    let debug = format!("{spec:?}");
    assert!(
        debug.contains("label_copy"),
        "Debug may show column_name: {debug}"
    );
    assert!(
        debug.contains("fn.exact.generated-column.create"),
        "Debug may show exact Function ID: {debug}"
    );
    assert!(
        !debug.contains(LITERAL_SENTINEL),
        "CreateGeneratedColumnJobSpec Debug must not reveal literal payload: {debug}"
    );
    Ok(())
}
