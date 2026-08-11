// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Contract tests for LanceDB Enterprise first-class functions.
//!
//! These tests pin the intended public surface under [`lancedb::function`].
//! They intentionally fail to compile until that module exists.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lancedb::Result;
use lancedb::function::{
    Function, FunctionArgument, FunctionCall, FunctionId, FunctionOutput, FunctionParameter,
    FunctionSignature, GENERATED_COLUMN_METADATA_KEY, GeneratedColumnDefinition,
    GeneratedColumnStatus,
};
use lancedb::ipc::{batches_to_ipc_file, schema_to_ipc_file};
use serde_json::Value;

fn sample_output() -> FunctionOutput {
    FunctionOutput::new(DataType::Int32, true)
}

fn sample_signature() -> Result<FunctionSignature> {
    FunctionSignature::try_new(
        vec![
            FunctionParameter::new("x", DataType::Int32),
            FunctionParameter::new("label", DataType::Utf8),
        ],
        sample_output(),
    )
}

fn sample_function() -> Result<Function> {
    let id = FunctionId::try_new("fn.exact.example")?;
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

fn sample_call(function: &Function) -> Result<FunctionCall> {
    FunctionCall::try_new(
        function,
        vec![
            ("x".to_string(), field_arg(7, DataType::Int32)?),
            ("label".to_string(), utf8_literal(Some("ok"))?),
        ],
    )
}

fn assert_json_object_keys_subset(value: &Value, allowed: &[&str]) {
    let object = value
        .as_object()
        .unwrap_or_else(|| panic!("expected JSON object, got {value}"));
    for key in object.keys() {
        assert!(
            allowed.contains(&key.as_str()),
            "unexpected JSON key `{key}` in {value}"
        );
    }
}

fn assert_forbidden_function_keys_absent(value: &Value) {
    // Function transport forbids catalog/name/version/lineage-style fields on the
    // function (or function_call) object itself. Parameter objects still use `name`.
    let forbidden = [
        "name",
        "catalog",
        "catalog_name",
        "version",
        "function_version",
        "FunctionVersion",
        "lineage",
        "user_version",
        "idempotency_key",
        "digest",
        "storage",
        "storage_location",
        "deterministic",
        "null_policy",
        "nullPolicy",
    ];
    let object = value
        .as_object()
        .unwrap_or_else(|| panic!("expected JSON object, got {value}"));
    for key in forbidden {
        assert!(
            !object.contains_key(key),
            "function JSON must not contain forbidden key `{key}`: {value}"
        );
    }
}

/// Minimal RFC 4648 base64 encoder for test-only wire mutation.
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

/// Minimal RFC 4648 base64 decoder for test-only wire mutation.
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

fn first_literal_ipc(metadata: &Value) -> String {
    let args = metadata
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

fn set_first_literal_ipc(metadata: &mut Value, ipc: String) {
    let args = metadata
        .pointer_mut("/function_call/arguments")
        .and_then(Value::as_array_mut)
        .expect("function_call.arguments array");
    for arg in args {
        let value = arg
            .get_mut("value")
            .and_then(Value::as_object_mut)
            .expect("argument.value object");
        if value.get("kind").and_then(Value::as_str) == Some("literal") {
            value.insert("ipc".into(), Value::String(ipc));
            return;
        }
    }
    panic!("expected at least one literal argument with ipc");
}

fn metadata_with_literal_call() -> Result<(String, i32)> {
    let function = sample_function()?;
    let call = FunctionCall::try_new(
        &function,
        vec![
            ("x".to_string(), int_literal(Some(7))?),
            ("label".to_string(), utf8_literal(Some("ok"))?),
        ],
    )?;
    let definition = GeneratedColumnDefinition::try_new(13, call, 1, 1)?;
    Ok((definition.to_metadata_json()?, 13))
}

fn int32_batch_ipc(values: &[Option<i32>]) -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        true,
    )]));
    let array = Arc::new(Int32Array::from(values.to_vec())) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
    batches_to_ipc_file(&[batch])
}

fn int32_two_one_row_batches_ipc() -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        true,
    )]));
    let batch_a = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef],
    )
    .expect("batch a");
    let batch_b = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![Some(2)])) as ArrayRef],
    )
    .expect("batch b");
    batches_to_ipc_file(&[batch_a, batch_b])
}

fn int32_two_column_one_row_ipc() -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(2)])) as ArrayRef,
        ],
    )
    .expect("two-column batch");
    batches_to_ipc_file(&[batch])
}

/// Mutate the first signature `data_type_ipc` string in a Function JSON value.
fn set_first_parameter_data_type_ipc(function_json: &mut Value, ipc: String) {
    let parameters = function_json
        .pointer_mut("/signature/parameters")
        .and_then(Value::as_array_mut)
        .expect("signature.parameters array");
    let first = parameters
        .first_mut()
        .expect("at least one signature parameter");
    first
        .as_object_mut()
        .expect("parameter object")
        .insert("data_type_ipc".into(), Value::String(ipc));
}

#[test]
fn function_id_rejects_empty() {
    let err = FunctionId::try_new("").expect_err("empty FunctionId must be rejected");
    let message = err.to_string();
    assert!(
        message.to_lowercase().contains("empty") || message.to_lowercase().contains("non-empty"),
        "unexpected error: {message}"
    );
}

#[test]
fn function_id_preserves_exact_opaque_value() -> Result<()> {
    let id = FunctionId::try_new("fn.exact.opaque-value")?;
    assert_eq!(id.as_str(), "fn.exact.opaque-value");
    Ok(())
}

#[test]
fn function_signature_rejects_duplicate_or_empty_parameter_names() {
    let duplicate = FunctionSignature::try_new(
        vec![
            FunctionParameter::new("x", DataType::Int32),
            FunctionParameter::new("x", DataType::Utf8),
        ],
        sample_output(),
    );
    assert!(
        duplicate.is_err(),
        "duplicate parameter names must be rejected"
    );

    let empty = FunctionSignature::try_new(
        vec![FunctionParameter::new("", DataType::Int32)],
        sample_output(),
    );
    assert!(empty.is_err(), "empty parameter names must be rejected");
}

#[test]
fn function_json_round_trip_pins_output_and_ipc_wire_shape() -> Result<()> {
    let function = sample_function()?;
    let json = serde_json::to_value(&function).expect("serialize Function");
    // Function transport JSON is a strict format_version = 1 envelope (wire mechanism).
    assert_json_object_keys_subset(&json, &["format_version", "id", "signature"]);
    assert_eq!(json["format_version"], 1);
    assert_forbidden_function_keys_absent(&json);

    let signature = json
        .get("signature")
        .and_then(Value::as_object)
        .expect("signature object");
    assert_json_object_keys_subset(&Value::Object(signature.clone()), &["parameters", "output"]);

    let parameters = signature
        .get("parameters")
        .and_then(Value::as_array)
        .expect("parameters array");
    assert_eq!(parameters.len(), 2);
    for parameter in parameters {
        assert_json_object_keys_subset(parameter, &["name", "data_type_ipc"]);
        let ipc = parameter
            .get("data_type_ipc")
            .and_then(Value::as_str)
            .expect("parameter data_type_ipc");
        assert!(!ipc.is_empty(), "parameter data_type_ipc must be base64");
        base64_decode(ipc).expect("parameter data_type_ipc must be valid base64");
    }

    let output = signature
        .get("output")
        .and_then(Value::as_object)
        .expect("output object");
    assert_json_object_keys_subset(
        &Value::Object(output.clone()),
        &["data_type_ipc", "nullable"],
    );
    let output_ipc = output
        .get("data_type_ipc")
        .and_then(Value::as_str)
        .expect("output data_type_ipc");
    base64_decode(output_ipc).expect("output data_type_ipc must be valid base64");
    assert_eq!(output.get("nullable"), Some(&Value::Bool(true)));

    assert_eq!(json["id"], Value::String("fn.exact.example".to_string()));

    let restored: Function = serde_json::from_value(json.clone()).expect("deserialize Function");
    assert_eq!(restored.id().as_str(), function.id().as_str());
    assert_eq!(
        restored.signature().parameters().len(),
        function.signature().parameters().len()
    );
    assert_eq!(
        restored.signature().parameters()[0].name(),
        function.signature().parameters()[0].name()
    );
    assert_eq!(
        restored.signature().parameters()[0].data_type(),
        function.signature().parameters()[0].data_type()
    );
    assert_eq!(
        restored.signature().parameters()[1].name(),
        function.signature().parameters()[1].name()
    );
    assert_eq!(
        restored.signature().parameters()[1].data_type(),
        function.signature().parameters()[1].data_type()
    );
    assert_eq!(
        restored.signature().output().data_type(),
        function.signature().output().data_type()
    );
    assert_eq!(
        restored.signature().output().nullable(),
        function.signature().output().nullable()
    );
    assert_eq!(restored.signature().output().data_type(), &DataType::Int32);
    assert!(restored.signature().output().nullable());

    // Handle / wire identity is the opaque FunctionId only.
    assert_eq!(
        serde_json::to_value(&restored).expect("re-serialize Function"),
        json
    );

    let mut unknown = json.clone();
    unknown
        .as_object_mut()
        .unwrap()
        .insert("unexpected_field".into(), Value::Bool(true));
    assert!(
        serde_json::from_value::<Function>(unknown).is_err(),
        "Function JSON must reject unknown fields"
    );

    let mut unknown_version = json.clone();
    unknown_version["format_version"] = Value::from(2);
    assert!(
        serde_json::from_value::<Function>(unknown_version).is_err(),
        "Function JSON must reject unknown format_version"
    );
    Ok(())
}

#[test]
fn function_signature_data_type_ipc_decode_is_fail_closed() -> Result<()> {
    let function = sample_function()?;
    let json = serde_json::to_value(&function).expect("serialize Function");
    let original_ipc = json["signature"]["parameters"][0]["data_type_ipc"]
        .as_str()
        .expect("parameter data_type_ipc")
        .to_string();
    let original_bytes = base64_decode(&original_ipc).expect("parameter data_type_ipc base64");

    let mut invalid_base64 = json.clone();
    set_first_parameter_data_type_ipc(&mut invalid_base64, "!!!".into());
    assert!(
        serde_json::from_value::<Function>(invalid_base64).is_err(),
        "invalid base64 data_type_ipc must be rejected"
    );

    let multi_field_schema = Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    let multi_field_ipc = schema_to_ipc_file(&multi_field_schema)?;
    let mut multi_field = json.clone();
    set_first_parameter_data_type_ipc(&mut multi_field, base64_encode(&multi_field_ipc));
    assert!(
        serde_json::from_value::<Function>(multi_field).is_err(),
        "schema-only IPC with more than one field must be rejected"
    );

    let mut trailing = original_bytes;
    trailing.extend_from_slice(b"extra");
    let mut trailing_ipc = json.clone();
    set_first_parameter_data_type_ipc(&mut trailing_ipc, base64_encode(&trailing));
    assert!(
        serde_json::from_value::<Function>(trailing_ipc).is_err(),
        "single-field IPC with trailing bytes must be rejected"
    );

    // Type encoding is schema-only: a valid one-field IPC file that also carries a
    // record batch must be rejected (data-bearing IPC is not a type encoding).
    let data_bearing_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        true,
    )]));
    let data_bearing_batch = RecordBatch::try_new(
        data_bearing_schema,
        vec![Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef],
    )
    .expect("one-field one-row batch");
    let data_bearing_ipc = batches_to_ipc_file(&[data_bearing_batch])?;
    let mut data_bearing = json.clone();
    set_first_parameter_data_type_ipc(&mut data_bearing, base64_encode(&data_bearing_ipc));
    assert!(
        serde_json::from_value::<Function>(data_bearing).is_err(),
        "one-field IPC that contains a record batch must be rejected for data_type_ipc"
    );
    Ok(())
}

fn sample_definition_metadata() -> Result<(Function, String, i32)> {
    let function = sample_function()?;
    let call = sample_call(&function)?;
    let definition = GeneratedColumnDefinition::try_new(11, call, 3, 3)?;
    Ok((function, definition.to_metadata_json()?, 11))
}

fn schema_only_type_ipc_b64(data_type: DataType) -> Result<String> {
    let schema = Schema::new(vec![Field::new("", data_type, true)]);
    Ok(base64_encode(&schema_to_ipc_file(&schema)?))
}

#[test]
fn function_call_validate_against_requires_exact_identity_and_signature() -> Result<()> {
    let (function, metadata_json, output_field_id) = sample_definition_metadata()?;

    // try_new and a normal metadata round-trip must validate against the catalog Function.
    let call = sample_call(&function)?;
    call.validate_against(&function)?;
    let restored = GeneratedColumnDefinition::from_metadata_json(&metadata_json, output_field_id)?;
    restored.function_call().validate_against(&function)?;

    let value: Value = serde_json::from_str(&metadata_json).expect("metadata JSON");

    // Reordered wire bindings decode structurally, then fail validation.
    let mut reordered = value.clone();
    let args = reordered
        .pointer_mut("/function_call/arguments")
        .and_then(Value::as_array_mut)
        .expect("function_call.arguments");
    args.swap(0, 1);
    let reordered_def =
        GeneratedColumnDefinition::from_metadata_json(&reordered.to_string(), output_field_id)?;
    assert!(
        reordered_def
            .function_call()
            .validate_against(&function)
            .is_err(),
        "reordered wire bindings must fail validate_against"
    );

    // Changed parameter name.
    let mut renamed = value.clone();
    renamed["function_call"]["arguments"][0]["parameter"] = Value::String("renamed".into());
    let renamed_def =
        GeneratedColumnDefinition::from_metadata_json(&renamed.to_string(), output_field_id)?;
    assert!(
        renamed_def
            .function_call()
            .validate_against(&function)
            .is_err(),
        "changed parameter name must fail validate_against"
    );

    // Missing binding.
    let mut missing = value.clone();
    missing["function_call"]["arguments"]
        .as_array_mut()
        .expect("arguments")
        .pop();
    let missing_def =
        GeneratedColumnDefinition::from_metadata_json(&missing.to_string(), output_field_id)?;
    assert!(
        missing_def
            .function_call()
            .validate_against(&function)
            .is_err(),
        "missing binding must fail validate_against"
    );

    // Extra binding.
    let mut extra = value.clone();
    let extra_binding = serde_json::json!({
        "parameter": "extra",
        "value": {
            "kind": "field",
            "field_id": 99,
            "data_type_ipc": schema_only_type_ipc_b64(DataType::Int32)?
        }
    });
    extra["function_call"]["arguments"]
        .as_array_mut()
        .expect("arguments")
        .push(extra_binding);
    let extra_def =
        GeneratedColumnDefinition::from_metadata_json(&extra.to_string(), output_field_id)?;
    assert!(
        extra_def
            .function_call()
            .validate_against(&function)
            .is_err(),
        "extra binding must fail validate_against"
    );

    // Changed argument Arrow type (field argument data_type_ipc).
    let mut type_changed = value.clone();
    type_changed["function_call"]["arguments"][0]["value"]["data_type_ipc"] =
        Value::String(schema_only_type_ipc_b64(DataType::Utf8)?);
    let type_changed_def =
        GeneratedColumnDefinition::from_metadata_json(&type_changed.to_string(), output_field_id)?;
    assert!(
        type_changed_def
            .function_call()
            .validate_against(&function)
            .is_err(),
        "changed argument type must fail validate_against"
    );

    // Different Function ID.
    let mut different_id = value.clone();
    different_id["function_call"]["function_id"] = Value::String("fn.other".into());
    let different_id_def =
        GeneratedColumnDefinition::from_metadata_json(&different_id.to_string(), output_field_id)?;
    assert!(
        different_id_def
            .function_call()
            .validate_against(&function)
            .is_err(),
        "different FunctionId must fail validate_against"
    );

    Ok(())
}

#[test]
fn function_call_requires_complete_unique_ordered_typed_bindings() -> Result<()> {
    let function = sample_function()?;

    let missing = FunctionCall::try_new(
        &function,
        vec![("x".to_string(), field_arg(1, DataType::Int32)?)],
    );
    assert!(missing.is_err(), "missing parameter must fail");

    let duplicate = FunctionCall::try_new(
        &function,
        vec![
            ("x".to_string(), field_arg(1, DataType::Int32)?),
            ("label".to_string(), utf8_literal(Some("a"))?),
            ("x".to_string(), field_arg(2, DataType::Int32)?),
        ],
    );
    assert!(duplicate.is_err(), "duplicate parameter name must fail");

    let unknown = FunctionCall::try_new(
        &function,
        vec![
            ("x".to_string(), field_arg(1, DataType::Int32)?),
            ("label".to_string(), utf8_literal(Some("a"))?),
            ("extra".to_string(), int_literal(Some(1))?),
        ],
    );
    assert!(unknown.is_err(), "unknown parameter name must fail");

    let type_mismatch = FunctionCall::try_new(
        &function,
        vec![
            ("x".to_string(), field_arg(1, DataType::Utf8)?),
            ("label".to_string(), utf8_literal(Some("a"))?),
        ],
    );
    assert!(type_mismatch.is_err(), "argument type mismatch must fail");

    // Binding is by explicit parameter name: wrong order is accepted then normalized.
    let wrong_order_input = FunctionCall::try_new(
        &function,
        vec![
            ("label".to_string(), utf8_literal(Some("a"))?),
            ("x".to_string(), field_arg(9, DataType::Int32)?),
        ],
    )?;
    let args = wrong_order_input.arguments();
    assert_eq!(args.len(), 2);
    assert_eq!(args[0].0, "x");
    assert_eq!(args[1].0, "label");
    assert_eq!(wrong_order_input.function_id().as_str(), "fn.exact.example");
    Ok(())
}

#[test]
fn field_argument_stores_stable_field_id_not_column_name() -> Result<()> {
    let function = sample_function()?;
    let call = FunctionCall::try_new(
        &function,
        vec![
            ("x".to_string(), field_arg(42, DataType::Int32)?),
            ("label".to_string(), utf8_literal(Some("n"))?),
        ],
    )?;

    let (name, arg) = &call.arguments()[0];
    assert_eq!(name, "x");
    assert_eq!(arg.field_id(), Some(42));
    assert_eq!(arg.data_type(), &DataType::Int32);
    assert!(arg.literal_array().is_none());

    let json = serde_json::to_value(&call).expect("serialize FunctionCall");
    let encoded = json.to_string();
    assert!(
        !encoded.contains("column_name")
            && !encoded.contains("columnName")
            && !encoded.to_lowercase().contains("\"column\""),
        "field argument must not persist a column name: {json}"
    );
    assert!(
        encoded.contains("42") && (encoded.contains("field_id") || encoded.contains("fieldId")),
        "field argument must persist stable field id: {json}"
    );

    let arguments = json
        .get("arguments")
        .and_then(Value::as_array)
        .expect("arguments array");
    assert_eq!(arguments.len(), 2);
    assert_json_object_keys_subset(&arguments[0], &["parameter", "value"]);
    assert_eq!(arguments[0]["parameter"], Value::String("x".into()));
    assert_eq!(arguments[0]["value"]["kind"], Value::String("field".into()));
    assert_json_object_keys_subset(&arguments[1], &["parameter", "value"]);
    assert_eq!(arguments[1]["parameter"], Value::String("label".into()));
    assert_eq!(
        arguments[1]["value"]["kind"],
        Value::String("literal".into())
    );
    let literal_ipc = arguments[1]["value"]["ipc"]
        .as_str()
        .expect("literal value.ipc");
    base64_decode(literal_ipc).expect("literal ipc must be valid base64");
    Ok(())
}

#[test]
fn field_argument_rejects_negative_field_id() {
    let err = FunctionArgument::try_field(-1, DataType::Int32)
        .expect_err("negative field id must be rejected");
    let message = err.to_string().to_lowercase();
    assert!(
        message.contains("field") || message.contains("negative"),
        "unexpected error: {message}"
    );
}

#[test]
fn typed_null_literal_round_trips_through_generated_metadata_json() -> Result<()> {
    let function = sample_function()?;
    let null_literal = int_literal(None)?;
    assert_eq!(null_literal.data_type(), &DataType::Int32);
    assert!(null_literal.is_typed_null());

    let call = FunctionCall::try_new(
        &function,
        vec![
            ("x".to_string(), null_literal),
            ("label".to_string(), utf8_literal(Some("ok"))?),
        ],
    )?;
    let definition = GeneratedColumnDefinition::try_new(3, call, 2, 2)?;
    let metadata_json = definition.to_metadata_json()?;
    let restored = GeneratedColumnDefinition::from_metadata_json(&metadata_json, 3)?;
    let decoded = &restored.function_call().arguments()[0].1;
    assert!(decoded.is_typed_null());
    assert_eq!(decoded.data_type(), &DataType::Int32);

    let array = decoded
        .literal_array()
        .expect("typed NULL must expose a one-row array");
    assert_eq!(array.len(), 1);
    assert!(array.is_null(0));
    assert_eq!(restored.to_metadata_json()?, metadata_json);
    Ok(())
}

#[test]
fn generated_column_metadata_round_trips_exact_function_id() -> Result<()> {
    assert_eq!(GENERATED_COLUMN_METADATA_KEY, "lancedb::generated_column");
    assert!(!GENERATED_COLUMN_METADATA_KEY.is_empty());

    let function = sample_function()?;
    let call = sample_call(&function)?;
    let definition = GeneratedColumnDefinition::try_new(/* output_field_id */ 11, call, 3, 3)?;

    assert_eq!(definition.format_version(), 1);
    assert_eq!(definition.output_field_id(), 11);
    assert_eq!(definition.dependency_epoch(), 3);
    assert_eq!(definition.materialized_epoch(), 3);
    assert_eq!(definition.status(), GeneratedColumnStatus::Complete);
    assert_eq!(
        definition.function_call().function_id().as_str(),
        "fn.exact.example"
    );

    let metadata_json = definition.to_metadata_json()?;
    let value: Value = serde_json::from_str(&metadata_json).expect("metadata JSON");
    assert_json_object_keys_subset(
        &value,
        &[
            "format_version",
            "output_field_id",
            "function_call",
            "dependency_epoch",
            "materialized_epoch",
        ],
    );
    assert_eq!(value["format_version"], 1);
    assert_eq!(value["output_field_id"], 11);
    assert_eq!(
        value["function_call"]["function_id"],
        Value::String("fn.exact.example".to_string())
    );
    assert_forbidden_function_keys_absent(&value["function_call"]);

    let arguments = value["function_call"]["arguments"]
        .as_array()
        .expect("function_call.arguments");
    for argument in arguments {
        assert_json_object_keys_subset(argument, &["parameter", "value"]);
        let kind = argument["value"]["kind"]
            .as_str()
            .expect("argument value.kind");
        assert!(
            kind == "field" || kind == "literal",
            "unexpected argument kind `{kind}`"
        );
        if kind == "literal" {
            base64_decode(argument["value"]["ipc"].as_str().expect("literal ipc"))
                .expect("literal ipc base64");
        }
    }

    let restored = GeneratedColumnDefinition::from_metadata_json(&metadata_json, 11)?;
    assert_eq!(restored.output_field_id(), definition.output_field_id());
    assert_eq!(
        restored.function_call().function_id().as_str(),
        definition.function_call().function_id().as_str()
    );
    assert_eq!(restored.dependency_epoch(), definition.dependency_epoch());
    assert_eq!(
        restored.materialized_epoch(),
        definition.materialized_epoch()
    );
    assert_eq!(restored.status(), definition.status());
    assert_eq!(restored.to_metadata_json()?, metadata_json);
    Ok(())
}

#[test]
fn metadata_decode_is_fail_closed_for_unknown_field_variant_and_version() -> Result<()> {
    let function = sample_function()?;
    let call = sample_call(&function)?;
    let definition = GeneratedColumnDefinition::try_new(5, call, 1, 1)?;
    let value: Value =
        serde_json::from_str(&definition.to_metadata_json()?).expect("metadata JSON");

    let mut unknown_field = value.clone();
    unknown_field
        .as_object_mut()
        .unwrap()
        .insert("unexpected_field".into(), Value::Bool(true));
    assert!(
        GeneratedColumnDefinition::from_metadata_json(&unknown_field.to_string(), 5).is_err(),
        "unknown metadata field must be rejected"
    );

    let mut unknown_version = value.clone();
    unknown_version["format_version"] = Value::from(2);
    assert!(
        GeneratedColumnDefinition::from_metadata_json(&unknown_version.to_string(), 5).is_err(),
        "unknown format_version must be rejected"
    );

    let mut unknown_variant = value.clone();
    let argument_value = unknown_variant
        .pointer_mut("/function_call/arguments/0/value")
        .and_then(Value::as_object_mut)
        .expect("argument value object");
    argument_value.clear();
    argument_value.insert("kind".into(), Value::String("expression".into()));
    argument_value.insert("sql".into(), Value::String("x + 1".into()));
    assert!(
        GeneratedColumnDefinition::from_metadata_json(&unknown_variant.to_string(), 5).is_err(),
        "unknown argument variant must be rejected"
    );
    Ok(())
}

#[test]
fn literal_ipc_rejects_invalid_trailing_schema_only_zero_and_multi_row_payloads() -> Result<()> {
    let multi_row =
        FunctionArgument::try_literal(Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef);
    assert!(multi_row.is_err(), "multi-row literal must be rejected");

    let zero_row =
        FunctionArgument::try_literal(Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef);
    assert!(zero_row.is_err(), "zero-row literal must be rejected");

    let (metadata_json, output_field_id) = metadata_with_literal_call()?;
    let mut value: Value = serde_json::from_str(&metadata_json).expect("metadata JSON");
    let original_bytes = base64_decode(&first_literal_ipc(&value)).expect("literal ipc base64");

    set_first_literal_ipc(&mut value, base64_encode(b"not-arrow-ipc"));
    assert!(
        GeneratedColumnDefinition::from_metadata_json(&value.to_string(), output_field_id).is_err(),
        "invalid Arrow IPC must be rejected"
    );

    let mut trailing = original_bytes;
    trailing.extend_from_slice(b"extra");
    value = serde_json::from_str(&metadata_json).expect("metadata JSON");
    set_first_literal_ipc(&mut value, base64_encode(&trailing));
    assert!(
        GeneratedColumnDefinition::from_metadata_json(&value.to_string(), output_field_id).is_err(),
        "trailing IPC payload must be rejected"
    );

    let schema = Schema::new(vec![Field::new("value", DataType::Int32, true)]);
    let schema_only = schema_to_ipc_file(&schema)?;
    value = serde_json::from_str(&metadata_json).expect("metadata JSON");
    set_first_literal_ipc(&mut value, base64_encode(&schema_only));
    assert!(
        GeneratedColumnDefinition::from_metadata_json(&value.to_string(), output_field_id).is_err(),
        "schema-only IPC must be rejected for literal decode"
    );

    let zero_row_ipc = int32_batch_ipc(&[])?;
    value = serde_json::from_str(&metadata_json).expect("metadata JSON");
    set_first_literal_ipc(&mut value, base64_encode(&zero_row_ipc));
    assert!(
        GeneratedColumnDefinition::from_metadata_json(&value.to_string(), output_field_id).is_err(),
        "zero-row literal IPC must be rejected"
    );

    let multi_row_ipc = int32_batch_ipc(&[Some(1), Some(2)])?;
    value = serde_json::from_str(&metadata_json).expect("metadata JSON");
    set_first_literal_ipc(&mut value, base64_encode(&multi_row_ipc));
    assert!(
        GeneratedColumnDefinition::from_metadata_json(&value.to_string(), output_field_id).is_err(),
        "multi-row literal IPC must be rejected"
    );

    value = serde_json::from_str(&metadata_json).expect("metadata JSON");
    set_first_literal_ipc(&mut value, "!!!".into());
    assert!(
        GeneratedColumnDefinition::from_metadata_json(&value.to_string(), output_field_id).is_err(),
        "syntactically invalid base64 literal ipc must be rejected"
    );

    let two_batches_ipc = int32_two_one_row_batches_ipc()?;
    value = serde_json::from_str(&metadata_json).expect("metadata JSON");
    set_first_literal_ipc(&mut value, base64_encode(&two_batches_ipc));
    assert!(
        GeneratedColumnDefinition::from_metadata_json(&value.to_string(), output_field_id).is_err(),
        "literal IPC with two one-row batches must be rejected"
    );

    let two_column_ipc = int32_two_column_one_row_ipc()?;
    value = serde_json::from_str(&metadata_json).expect("metadata JSON");
    set_first_literal_ipc(&mut value, base64_encode(&two_column_ipc));
    assert!(
        GeneratedColumnDefinition::from_metadata_json(&value.to_string(), output_field_id).is_err(),
        "literal IPC with one row but two columns must be rejected"
    );
    Ok(())
}

#[test]
fn generated_metadata_rejects_output_field_id_mismatch() -> Result<()> {
    let function = sample_function()?;
    let call = sample_call(&function)?;
    let definition = GeneratedColumnDefinition::try_new(19, call, 2, 2)?;
    let metadata_json = definition.to_metadata_json()?;

    let err = GeneratedColumnDefinition::from_metadata_json(&metadata_json, 20)
        .expect_err("output field id mismatch must fail closed");
    let message = err.to_string().to_lowercase();
    assert!(
        message.contains("field") || message.contains("mismatch"),
        "unexpected error: {message}"
    );
    Ok(())
}

#[test]
fn epochs_complete_to_incomplete_to_complete_and_overflow() -> Result<()> {
    let function = sample_function()?;

    let mut complete = GeneratedColumnDefinition::try_new(1, sample_call(&function)?, 4, 4)?;
    assert_eq!(complete.status(), GeneratedColumnStatus::Complete);

    let incomplete = GeneratedColumnDefinition::try_new(1, sample_call(&function)?, 5, 4)?;
    assert_eq!(incomplete.status(), GeneratedColumnStatus::Incomplete);

    let invalid = GeneratedColumnDefinition::try_new(1, sample_call(&function)?, 4, 5);
    assert!(
        invalid.is_err(),
        "materialized_epoch greater than dependency_epoch must be invalid"
    );

    // Complete -> Incomplete via checked dependency invalidation.
    complete.invalidate()?;
    assert_eq!(complete.dependency_epoch(), 5);
    assert_eq!(complete.materialized_epoch(), 4);
    assert_eq!(complete.status(), GeneratedColumnStatus::Incomplete);

    // Incomplete -> Complete via materialization at the current dependency epoch.
    complete.mark_materialized();
    assert_eq!(complete.dependency_epoch(), 5);
    assert_eq!(complete.materialized_epoch(), 5);
    assert_eq!(complete.status(), GeneratedColumnStatus::Complete);

    let mut at_max =
        GeneratedColumnDefinition::try_new(1, sample_call(&function)?, u64::MAX, u64::MAX)?;
    assert!(
        at_max.invalidate().is_err(),
        "dependency_epoch overflow must fail"
    );

    // Typed NULL materialization state is distinct from Incomplete projection.
    let null_call = FunctionCall::try_new(
        &function,
        vec![
            ("x".to_string(), int_literal(None)?),
            ("label".to_string(), utf8_literal(None)?),
        ],
    )?;
    let complete_with_nulls = GeneratedColumnDefinition::try_new(1, null_call, 9, 9)?;
    assert_eq!(
        complete_with_nulls.status(),
        GeneratedColumnStatus::Complete
    );
    assert!(
        complete_with_nulls.function_call().arguments()[0]
            .1
            .is_typed_null()
    );
    assert_ne!(
        complete_with_nulls.status(),
        GeneratedColumnStatus::Incomplete
    );
    Ok(())
}

#[test]
fn arrow_type_and_literal_encoding_is_deterministic_through_serde_json() -> Result<()> {
    let data_type = DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("UTC".into()));
    let signature_a = FunctionSignature::try_new(
        vec![FunctionParameter::new("ts", data_type.clone())],
        FunctionOutput::new(data_type.clone(), false),
    )?;
    let signature_b = FunctionSignature::try_new(
        vec![FunctionParameter::new("ts", data_type.clone())],
        FunctionOutput::new(data_type, false),
    )?;
    let function_a = Function::new(FunctionId::try_new("fn.deterministic")?, signature_a);
    let function_b = Function::new(FunctionId::try_new("fn.deterministic")?, signature_b);
    let json_a = serde_json::to_value(&function_a).expect("serialize function_a");
    let json_b = serde_json::to_value(&function_b).expect("serialize function_b");
    assert_eq!(
        json_a, json_b,
        "Function Arrow IPC wire encoding must be deterministic"
    );
    assert_eq!(
        json_a["signature"]["parameters"][0]["data_type_ipc"],
        json_b["signature"]["parameters"][0]["data_type_ipc"]
    );
    assert_eq!(
        json_a["signature"]["output"]["data_type_ipc"],
        json_b["signature"]["output"]["data_type_ipc"]
    );

    let utf8_function = Function::new(
        FunctionId::try_new("fn.literal.deterministic")?,
        FunctionSignature::try_new(
            vec![FunctionParameter::new("label", DataType::Utf8)],
            FunctionOutput::new(DataType::Utf8, true),
        )?,
    );
    let call_a = FunctionCall::try_new(
        &utf8_function,
        vec![("label".to_string(), utf8_literal(Some("deterministic"))?)],
    )?;
    let call_b = FunctionCall::try_new(
        &utf8_function,
        vec![("label".to_string(), utf8_literal(Some("deterministic"))?)],
    )?;
    let def_a = GeneratedColumnDefinition::try_new(1, call_a, 1, 1)?;
    let def_b = GeneratedColumnDefinition::try_new(1, call_b, 1, 1)?;
    assert_eq!(
        def_a.to_metadata_json()?,
        def_b.to_metadata_json()?,
        "literal IPC encoding through metadata JSON must be deterministic"
    );

    let metadata: Value = serde_json::from_str(&def_a.to_metadata_json()?).expect("metadata JSON");
    let literal_ipc = metadata["function_call"]["arguments"][0]["value"]["ipc"]
        .as_str()
        .expect("literal ipc");
    base64_decode(literal_ipc).expect("literal ipc base64");

    // Contract: no untyped JSON / Lance JsonDataType substitute for Arrow types.
    let encoded = json_a.to_string();
    assert!(
        !encoded.contains("JsonDataType") && !encoded.contains("json_type"),
        "signature types must not use Lance JsonDataType: {json_a}"
    );
    Ok(())
}
