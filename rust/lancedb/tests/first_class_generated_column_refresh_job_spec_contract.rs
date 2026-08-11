// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Contract tests for RefreshGeneratedColumnJobSpec (FF-010 / generated-column refresh Job).
//!
//! These tests pin the intended public surface under [`lancedb::function`] for
//! refresh-generated-column Job operation input only. They intentionally fail to
//! compile until that API exists, and until [`GeneratedColumnDefinition`] gains
//! generic strict Serialize/Deserialize.
//!
//! Rejection cases are judged by `Result` structure (`is_err` / `is_ok`), never
//! by diagnostic message substrings. Catalog execution, materialization, and
//! Job lifecycle are out of scope.
//!
//! Intended minimal public API (exact names pinned here):
//! - [`RefreshGeneratedColumnJobSpec::try_new`]`(&function, generated_column_definition) -> Result<Self>`
//! - [`RefreshGeneratedColumnJobSpec::format_version`] /
//!   [`generated_column_definition`]
//! - [`RefreshGeneratedColumnJobSpec::validate_against`]`(&Function) -> Result<()>`
//!
//! Strict wire v1 outer object keys are exactly `format_version` and
//! `generated_column_definition`. Sophon JobMetadata owns table/version fields.
//! No user-provided target epoch: a later executor clones the pinned definition
//! and marks materialized to its same dependency epoch.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lancedb::Result;
use lancedb::function::{
    Function, FunctionArgument, FunctionCall, FunctionId, FunctionOutput, FunctionParameter,
    FunctionSignature, GeneratedColumnDefinition, RefreshGeneratedColumnJobSpec,
};
use lancedb::ipc::schema_to_ipc_file;
use serde_json::Value;

/// Distinctive UTF-8 sentinel used only for Debug redaction vs trusted wire.
const LITERAL_SENTINEL: &str = "REDTEST_REFRESH_LITERAL_SENTINEL_π_🔒_v1";

/// Stable output field id used across refresh fixtures.
const OUTPUT_FIELD_ID: i32 = 17;

/// Distinctive dependency / materialized epochs for incomplete refresh fixtures.
const DEPENDENCY_EPOCH: u64 = 41;
const MATERIALIZED_EPOCH_INCOMPLETE: u64 = 37;

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
    let id = FunctionId::try_new("fn.exact.generated-column.refresh")?;
    Ok(Function::new(id, sample_signature()?))
}

fn other_function_same_signature() -> Result<Function> {
    let id = FunctionId::try_new("fn.other.exact.refresh.id")?;
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

fn sentinel_definition(function: &Function) -> Result<GeneratedColumnDefinition> {
    let call = sample_literal_sentinel_call(function)?;
    GeneratedColumnDefinition::try_new(
        OUTPUT_FIELD_ID,
        call,
        DEPENDENCY_EPOCH,
        MATERIALIZED_EPOCH_INCOMPLETE,
    )
}

fn sample_spec() -> Result<RefreshGeneratedColumnJobSpec> {
    let function = sample_function()?;
    let definition = incomplete_definition(&function)?;
    RefreshGeneratedColumnJobSpec::try_new(&function, definition)
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

/// Outer-object keys forbidden on RefreshGeneratedColumnJobSpec wire.
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
    "refresh_mode",
    "mode",
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
    assert_json_object_keys_exact(value, &["format_version", "generated_column_definition"]);
    let object = value
        .as_object()
        .unwrap_or_else(|| panic!("expected JSON object, got {value}"));
    for key in object.keys() {
        assert!(
            !FORBIDDEN_OUTER_SPEC_KEYS.contains(&key.as_str()),
            "RefreshGeneratedColumnJobSpec wire must not contain forbidden outer key `{key}`: {value}"
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

fn label_literal_ipc(spec_json: &Value) -> String {
    let args = spec_json
        .pointer("/generated_column_definition/function_call/arguments")
        .and_then(Value::as_array)
        .expect("generated_column_definition.function_call.arguments array");
    for arg in args {
        if arg.get("parameter").and_then(Value::as_str) != Some("label") {
            continue;
        }
        let value = arg
            .get("value")
            .and_then(Value::as_object)
            .expect("argument.value object");
        assert_eq!(
            value.get("kind").and_then(Value::as_str),
            Some("literal"),
            "label binding must be a typed literal"
        );
        return value
            .get("ipc")
            .and_then(Value::as_str)
            .expect("literal value.ipc string")
            .to_string();
    }
    panic!("expected label literal argument with ipc");
}

fn structurally_decoded_refresh_spec_with_mutated_json(
    mutate: impl FnOnce(&mut Value),
) -> Result<RefreshGeneratedColumnJobSpec> {
    let spec = sample_spec()?;
    let mut spec_json = serde_json::to_value(&spec).expect("serialize refresh spec");
    mutate(&mut spec_json);
    Ok(serde_json::from_value(spec_json).expect("structural refresh decode"))
}

/// Structural decode of a GeneratedColumnDefinition after mutating nested call JSON.
///
/// Public constructors cannot build a catalog-invalid nested FunctionCall; this
/// helper only bypasses that for feeding malformed definitions into
/// [`RefreshGeneratedColumnJobSpec::try_new`].
fn structurally_decoded_definition_with_mutated_json(
    mutate: impl FnOnce(&mut Value),
) -> Result<GeneratedColumnDefinition> {
    let function = sample_function()?;
    let definition = incomplete_definition(&function)?;
    let mut definition_json =
        serde_json::to_value(&definition).expect("serialize GeneratedColumnDefinition");
    mutate(&mut definition_json);
    Ok(serde_json::from_value(definition_json).expect("structural definition decode"))
}

#[test]
fn generated_column_definition_generic_serde_matches_metadata_json() -> Result<()> {
    let function = sample_function()?;
    let definition = sentinel_definition(&function)?;

    let via_serde =
        serde_json::to_string(&definition).expect("Serialize GeneratedColumnDefinition");
    let via_metadata = definition.to_metadata_json()?;
    assert_eq!(
        via_serde, via_metadata,
        "generic Serialize bytes must be identical to to_metadata_json()"
    );

    let restored: GeneratedColumnDefinition =
        serde_json::from_str(&via_serde).expect("Deserialize GeneratedColumnDefinition");
    assert_eq!(restored, definition);
    assert_eq!(restored.to_metadata_json()?, via_metadata);
    assert_eq!(
        serde_json::to_string(&restored).expect("re-serialize restored"),
        via_serde
    );
    Ok(())
}

#[test]
fn from_metadata_json_rejects_field_id_mismatch_while_generic_deserialize_succeeds() -> Result<()> {
    let function = sample_function()?;
    let definition = complete_definition(&function)?;
    let json = serde_json::to_string(&definition).expect("Serialize GeneratedColumnDefinition");

    let structural: GeneratedColumnDefinition =
        serde_json::from_str(&json).expect("generic deserialize must succeed structurally");
    assert_eq!(structural.output_field_id(), OUTPUT_FIELD_ID);
    assert_eq!(structural, definition);

    assert!(
        GeneratedColumnDefinition::from_metadata_json(&json, OUTPUT_FIELD_ID + 1).is_err(),
        "from_metadata_json must still reject expected_output_field_id mismatch"
    );
    assert!(
        GeneratedColumnDefinition::from_metadata_json(&json, OUTPUT_FIELD_ID).is_ok(),
        "from_metadata_json must accept matching expected_output_field_id"
    );
    Ok(())
}

#[test]
fn incomplete_and_complete_definitions_round_trip_exact_v1_outer_wire() -> Result<()> {
    let function = sample_function()?;

    let complete = complete_definition(&function)?;
    let incomplete = incomplete_definition(&function)?;
    assert_ne!(
        incomplete.materialized_epoch(),
        incomplete.dependency_epoch(),
        "incomplete fixture must not invent an incomplete-only constructor rule"
    );

    let complete_spec = RefreshGeneratedColumnJobSpec::try_new(&function, complete.clone())?;
    let incomplete_spec = RefreshGeneratedColumnJobSpec::try_new(&function, incomplete.clone())?;

    assert_eq!(complete_spec.format_version(), 1);
    assert_eq!(incomplete_spec.format_version(), 1);
    assert_eq!(complete_spec.generated_column_definition(), &complete);
    assert_eq!(incomplete_spec.generated_column_definition(), &incomplete);
    assert_eq!(
        complete_spec
            .generated_column_definition()
            .output_field_id(),
        OUTPUT_FIELD_ID
    );
    assert_eq!(
        incomplete_spec
            .generated_column_definition()
            .dependency_epoch(),
        DEPENDENCY_EPOCH
    );
    assert_eq!(
        incomplete_spec
            .generated_column_definition()
            .materialized_epoch(),
        MATERIALIZED_EPOCH_INCOMPLETE
    );

    for spec in [&complete_spec, &incomplete_spec] {
        let json = serde_json::to_value(spec).expect("serialize refresh spec");
        assert_eq!(json["format_version"], 1);
        assert_json_object_keys_exact(&json, &["format_version", "generated_column_definition"]);
        assert_json_object_keys_exact(
            &json["generated_column_definition"],
            &[
                "format_version",
                "output_field_id",
                "function_call",
                "dependency_epoch",
                "materialized_epoch",
            ],
        );
        assert_eq!(
            json["generated_column_definition"]["output_field_id"],
            Value::from(OUTPUT_FIELD_ID)
        );
        assert_eq!(
            json["generated_column_definition"]["function_call"]["function_id"],
            Value::String("fn.exact.generated-column.refresh".into())
        );

        let restored: RefreshGeneratedColumnJobSpec =
            serde_json::from_value(json.clone()).expect("deserialize refresh spec");
        assert_eq!(restored.format_version(), 1);
        assert_eq!(
            restored.generated_column_definition(),
            spec.generated_column_definition()
        );

        let encoded_a = serde_json::to_string(spec).expect("encode a");
        let encoded_b = serde_json::to_string(spec).expect("encode b");
        assert_eq!(encoded_a, encoded_b);
        assert_eq!(
            serde_json::to_value(&restored).expect("re-serialize restored"),
            json
        );
    }
    Ok(())
}

#[test]
fn constructor_rejects_exact_function_id_mismatch_and_validate_against_rejects_mutated_bindings()
-> Result<()> {
    let function = sample_function()?;
    let matching = incomplete_definition(&function)?;
    assert!(
        RefreshGeneratedColumnJobSpec::try_new(&function, matching).is_ok(),
        "matching Function and GeneratedColumnDefinition must construct"
    );

    // Exact Function identity mismatch: definition call pinned to another Function ID.
    let other = other_function_same_signature()?;
    let other_definition = incomplete_definition(&other)?;
    assert!(
        RefreshGeneratedColumnJobSpec::try_new(&function, other_definition).is_err(),
        "Function ID mismatch must be rejected by constructor"
    );

    // Constructor must rerun full FunctionCall::validate_against, not ID-only.
    // Public APIs cannot build a catalog-invalid nested call; mutate definition
    // JSON, structurally decode, then pass into try_new.
    let reordered_definition =
        structurally_decoded_definition_with_mutated_json(|definition_json| {
            definition_json["function_call"]["arguments"]
                .as_array_mut()
                .expect("arguments")
                .swap(0, 1);
        })?;
    assert!(
        RefreshGeneratedColumnJobSpec::try_new(&function, reordered_definition).is_err(),
        "reordered parameter bindings must be rejected by constructor"
    );

    let renamed_definition =
        structurally_decoded_definition_with_mutated_json(|definition_json| {
            definition_json["function_call"]["arguments"][0]["parameter"] =
                Value::String("renamed".into());
        })?;
    assert!(
        RefreshGeneratedColumnJobSpec::try_new(&function, renamed_definition).is_err(),
        "parameter name mismatch must be rejected by constructor"
    );

    let type_mismatch_definition =
        structurally_decoded_definition_with_mutated_json(|definition_json| {
            definition_json["function_call"]["arguments"][0]["value"]["data_type_ipc"] =
                Value::String(schema_only_type_ipc_b64(DataType::Utf8).expect("type ipc"));
        })?;
    assert!(
        RefreshGeneratedColumnJobSpec::try_new(&function, type_mismatch_definition).is_err(),
        "Arrow type mismatch must be rejected by constructor"
    );

    // Separate execution-consumer guarantee: structural decode of the outer spec
    // bypasses try_new; validate_against must still reject the same mutations.
    let reordered_spec = structurally_decoded_refresh_spec_with_mutated_json(|spec_json| {
        spec_json["generated_column_definition"]["function_call"]["arguments"]
            .as_array_mut()
            .expect("arguments")
            .swap(0, 1);
    })?;
    assert!(
        reordered_spec.validate_against(&function).is_err(),
        "reordered parameter bindings must fail validate_against"
    );

    let renamed_spec = structurally_decoded_refresh_spec_with_mutated_json(|spec_json| {
        spec_json["generated_column_definition"]["function_call"]["arguments"][0]["parameter"] =
            Value::String("renamed".into());
    })?;
    assert!(
        renamed_spec.validate_against(&function).is_err(),
        "parameter name mismatch must fail validate_against"
    );

    let type_mismatch_spec = structurally_decoded_refresh_spec_with_mutated_json(|spec_json| {
        spec_json["generated_column_definition"]["function_call"]["arguments"][0]["value"]["data_type_ipc"] =
            Value::String(schema_only_type_ipc_b64(DataType::Utf8).expect("type ipc"));
    })?;
    assert!(
        type_mismatch_spec.validate_against(&function).is_err(),
        "Arrow type mismatch must fail validate_against"
    );
    Ok(())
}

#[test]
fn negative_output_id_reversed_epochs_empty_function_id_and_unknowns_fail_closed() -> Result<()> {
    let function = sample_function()?;
    let call = sample_mixed_call(&function)?;

    assert!(
        GeneratedColumnDefinition::try_new(-1, call.clone(), 1, 1).is_err(),
        "negative output_field_id must be rejected"
    );
    assert!(
        GeneratedColumnDefinition::try_new(OUTPUT_FIELD_ID, call, 4, 5).is_err(),
        "materialized_epoch > dependency_epoch must be rejected"
    );

    let definition = incomplete_definition(&function)?;
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

    let mut empty_function_id = definition_json.clone();
    empty_function_id["function_call"]["function_id"] = Value::String("".into());
    assert!(
        serde_json::from_value::<GeneratedColumnDefinition>(empty_function_id).is_err(),
        "generic deserialize must reject empty nested Function ID"
    );

    let mut unknown_definition_field = definition_json.clone();
    unknown_definition_field
        .as_object_mut()
        .unwrap()
        .insert("unexpected_field".into(), Value::Bool(true));
    assert!(
        serde_json::from_value::<GeneratedColumnDefinition>(unknown_definition_field).is_err(),
        "unknown definition field must fail closed"
    );

    let mut unknown_definition_version = definition_json.clone();
    unknown_definition_version["format_version"] = Value::from(2);
    assert!(
        serde_json::from_value::<GeneratedColumnDefinition>(unknown_definition_version).is_err(),
        "definition format_version other than 1 must fail closed"
    );

    let spec = sample_spec()?;
    let json = serde_json::to_value(&spec).expect("serialize refresh spec");

    let mut unknown_outer_field = json.clone();
    unknown_outer_field
        .as_object_mut()
        .unwrap()
        .insert("unexpected_field".into(), Value::Bool(true));
    assert!(
        serde_json::from_value::<RefreshGeneratedColumnJobSpec>(unknown_outer_field).is_err(),
        "unknown refresh outer field must fail closed"
    );

    let mut unknown_outer_version = json.clone();
    unknown_outer_version["format_version"] = Value::from(2);
    assert!(
        serde_json::from_value::<RefreshGeneratedColumnJobSpec>(unknown_outer_version).is_err(),
        "refresh format_version other than 1 must fail closed"
    );
    Ok(())
}

#[test]
fn outer_wire_excludes_forbidden_fields_and_keeps_epochs_nested() -> Result<()> {
    let function = sample_function()?;
    let definition = incomplete_definition(&function)?;
    let spec = RefreshGeneratedColumnJobSpec::try_new(&function, definition)?;
    let json = serde_json::to_value(&spec).expect("serialize refresh spec");
    assert_outer_forbidden_keys_absent(&json);

    // Nested definition carries stable field identity and epochs; outer must not.
    assert!(json.get("output_field_id").is_none());
    assert!(json.get("dependency_epoch").is_none());
    assert!(json.get("materialized_epoch").is_none());
    assert!(json.get("column_name").is_none());
    assert!(json.get("target_epoch").is_none());
    assert!(json.get("target_dependency_epoch").is_none());
    assert!(json.get("target_materialized_epoch").is_none());
    assert!(json.get("table").is_none());
    assert!(json.get("table_ref").is_none());
    assert!(json.get("source_table_version").is_none());
    assert!(json.get("version").is_none());
    assert!(json.get("state").is_none());
    assert!(json.get("status").is_none());
    assert!(json.get("job_id").is_none());
    assert!(json.get("function_call").is_none());

    assert_eq!(
        json["generated_column_definition"]["output_field_id"],
        Value::from(OUTPUT_FIELD_ID)
    );
    assert_eq!(
        json["generated_column_definition"]["dependency_epoch"],
        Value::from(DEPENDENCY_EPOCH)
    );
    assert_eq!(
        json["generated_column_definition"]["materialized_epoch"],
        Value::from(MATERIALIZED_EPOCH_INCOMPLETE)
    );
    assert_eq!(
        json["generated_column_definition"]["function_call"]["arguments"][0]["value"]["field_id"],
        Value::from(7)
    );

    let literal_definition = sentinel_definition(&function)?;
    let literal_spec = RefreshGeneratedColumnJobSpec::try_new(&function, literal_definition)?;
    let literal_json = serde_json::to_value(&literal_spec).expect("serialize literal refresh spec");
    assert_outer_forbidden_keys_absent(&literal_json);
    Ok(())
}

#[test]
fn structural_decode_then_validate_against_matching_and_mismatching_catalog_function() -> Result<()>
{
    let function = sample_function()?;
    let definition = incomplete_definition(&function)?;
    let spec = RefreshGeneratedColumnJobSpec::try_new(&function, definition)?;
    let json = serde_json::to_value(&spec).expect("serialize refresh spec");

    let decoded: RefreshGeneratedColumnJobSpec =
        serde_json::from_value(json).expect("structural decode without catalog/table");
    assert_eq!(
        decoded
            .generated_column_definition()
            .function_call()
            .function_id()
            .as_str(),
        function.id().as_str()
    );

    decoded.validate_against(&function)?;

    let other = other_function_same_signature()?;
    assert!(
        decoded.validate_against(&other).is_err(),
        "validate_against must reject a catalog Function with a different exact ID"
    );
    Ok(())
}

#[test]
fn debug_redacts_literal_payload_while_trusted_wire_retains_it() -> Result<()> {
    let function = sample_function()?;
    let definition = sentinel_definition(&function)?;
    let spec = RefreshGeneratedColumnJobSpec::try_new(&function, definition)?;

    // Trusted getters retain the typed literal payload.
    let label = &spec
        .generated_column_definition()
        .function_call()
        .arguments()[1]
        .1;
    let array = label.literal_array().expect("label literal array");
    let values = array
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8 literal");
    assert_eq!(values.value(0), LITERAL_SENTINEL);

    let json = serde_json::to_value(&spec).expect("serialize refresh spec");
    let label_ipc = label_literal_ipc(&json);
    let ipc_bytes = base64_decode(&label_ipc).expect("literal ipc base64");
    assert!(
        bytes_contain(&ipc_bytes, LITERAL_SENTINEL.as_bytes()),
        "trusted wire IPC payload must retain the UTF-8 literal sentinel"
    );

    let restored: RefreshGeneratedColumnJobSpec =
        serde_json::from_value(json).expect("deserialize refresh spec");
    let restored_label = &restored
        .generated_column_definition()
        .function_call()
        .arguments()[1]
        .1;
    let restored_values = restored_label
        .literal_array()
        .expect("restored label literal")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8 literal");
    assert_eq!(restored_values.value(0), LITERAL_SENTINEL);

    let debug = format!("{spec:?}");
    assert!(
        debug.contains(&OUTPUT_FIELD_ID.to_string()),
        "Debug may show stable output field ID: {debug}"
    );
    assert!(
        debug.contains("fn.exact.generated-column.refresh"),
        "Debug may show exact Function ID: {debug}"
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
        !debug.contains(LITERAL_SENTINEL),
        "RefreshGeneratedColumnJobSpec Debug must not reveal typed literal payload: {debug}"
    );
    assert!(
        !debug.contains("FunctionCall"),
        "RefreshGeneratedColumnJobSpec Debug must not format nested FunctionCall: {debug}"
    );
    assert!(
        !debug.contains("GeneratedColumnDefinition"),
        "RefreshGeneratedColumnJobSpec Debug must not format nested GeneratedColumnDefinition: {debug}"
    );
    Ok(())
}
