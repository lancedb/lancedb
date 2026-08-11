// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Contract tests for JobResult value/wire (FF-012).
//!
//! These tests pin the intended public non-resource [`lancedb::JobResult`]
//! surface under [`lancedb::job`]. They intentionally fail to compile until
//! that API exists.
//!
//! Scope is JobResult value and JSON wire only. Job::wait behavior, remote
//! describe shape, missing-result handling, local outcome, Python, Node, and
//! Sophon are out of scope.
//!
//! Rejection cases are judged by `Result` structure (`is_err` / `is_ok`) or
//! serde decode failure, never by diagnostic message substrings.
//! JSON map iteration order is not a contract; exact key sets are compared
//! independently. Byte reproducibility means repeated encoding of the same
//! in-memory value.

use std::collections::BTreeSet;

use arrow_schema::DataType;
use lancedb::JobResult;
use lancedb::Result;
use lancedb::function::{
    Function, FunctionId, FunctionOutput, FunctionParameter, FunctionSignature,
};
use serde_json::Value;

fn sample_function() -> Result<Function> {
    let id = FunctionId::try_new("fn.exact.job-result")?;
    let signature = FunctionSignature::try_new(
        vec![
            FunctionParameter::new("x", DataType::Int32),
            FunctionParameter::new("label", DataType::Utf8),
        ],
        FunctionOutput::new(DataType::Int32, true),
    )?;
    Ok(Function::new(id, signature))
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

/// Outer JobResult object keys that must not appear. Nested Function signature
/// keys (including parameter `name` and Function `id`) are legitimate and are
/// not scanned here. Opaque string contents are not recursively searched.
const FORBIDDEN_OUTER_RESULT_KEYS: &[&str] = &[
    "name",
    "definition",
    "FunctionDefinition",
    "source",
    "runtime",
    "packages",
    "capability",
    "capabilities",
    "artifact",
    "artifact_digest",
    "digest",
    "storage",
    "storage_location",
    "location",
    "table",
    "table_name",
    "table_ref",
    "version",
    "function_version",
    "FunctionVersion",
    "user_version",
    "lineage",
    "id",
    "job_id",
    "jobId",
    "type",
    "job_type",
    "state",
    "status",
    "lifecycle",
    "failure",
    "attempt",
    "attempt_id",
    "timestamp",
    "created_at",
    "updated_at",
    "retry",
    "retry_key",
    "idempotency",
    "idempotency_key",
    "commit_token",
    "secret",
    "compatibility",
    "deterministic",
    "null_policy",
    "nullPolicy",
];

fn assert_outer_forbidden_keys_absent(value: &Value) {
    let object = value
        .as_object()
        .unwrap_or_else(|| panic!("expected outer JobResult JSON object, got {value}"));
    for key in object.keys() {
        assert!(
            !FORBIDDEN_OUTER_RESULT_KEYS.contains(&key.as_str()),
            "outer JobResult wire must not contain forbidden key `{key}`: {value}"
        );
    }
}

fn assert_function_handle_exact(actual: &Function, expected: &Function) {
    assert_eq!(actual.id().as_str(), expected.id().as_str());
    assert_eq!(
        actual.signature().parameters().len(),
        expected.signature().parameters().len()
    );
    for (actual_param, expected_param) in actual
        .signature()
        .parameters()
        .iter()
        .zip(expected.signature().parameters().iter())
    {
        assert_eq!(actual_param.name(), expected_param.name());
        assert_eq!(actual_param.data_type(), expected_param.data_type());
    }
    assert_eq!(
        actual.signature().output().data_type(),
        expected.signature().output().data_type()
    );
    assert_eq!(
        actual.signature().output().nullable(),
        expected.signature().output().nullable()
    );
}

#[test]
fn none_and_function_exact_key_sets_helpers_round_trip_and_bytes() -> Result<()> {
    let none = JobResult::None;
    assert_eq!(none.format_version(), 1);
    assert!(none.function().is_none());
    assert!(matches!(none, JobResult::None));

    let none_json = serde_json::to_value(&none).expect("serialize JobResult::None");
    assert_json_object_keys_exact(&none_json, &["format_version", "kind"]);
    assert_eq!(none_json["format_version"], 1);
    assert_eq!(none_json["kind"], Value::String("none".into()));
    assert!(none_json.get("function").is_none());

    let none_restored: JobResult =
        serde_json::from_value(none_json.clone()).expect("deserialize JobResult::None");
    assert_eq!(none_restored.format_version(), 1);
    assert!(none_restored.function().is_none());
    assert!(matches!(none_restored, JobResult::None));
    assert_eq!(none_restored, none);
    assert_eq!(
        serde_json::to_value(&none_restored).expect("re-serialize None"),
        none_json
    );

    let none_a = serde_json::to_string(&none).expect("encode None a");
    let none_b = serde_json::to_string(&none).expect("encode None b");
    assert_eq!(
        none_a, none_b,
        "repeated None encoding must be byte-identical"
    );

    let function = sample_function()?;
    let expected_function_wire =
        serde_json::to_value(&function).expect("serialize nested Function");
    let function_result = JobResult::Function(function.clone());
    assert_eq!(function_result.format_version(), 1);
    assert!(matches!(function_result, JobResult::Function(_)));
    assert_function_handle_exact(
        function_result.function().expect("Function variant"),
        &function,
    );

    let function_json =
        serde_json::to_value(&function_result).expect("serialize JobResult::Function");
    assert_json_object_keys_exact(&function_json, &["format_version", "kind", "function"]);
    assert_eq!(function_json["format_version"], 1);
    assert_eq!(function_json["kind"], Value::String("function".into()));
    assert_eq!(
        function_json["function"], expected_function_wire,
        "nested function must be the exact existing Function wire"
    );
    assert_json_object_keys_exact(
        &function_json["function"],
        &["format_version", "id", "signature"],
    );
    assert_eq!(
        function_json["function"]["id"],
        Value::String("fn.exact.job-result".into())
    );
    assert_eq!(function_json["function"]["format_version"], 1);

    let function_restored: JobResult =
        serde_json::from_value(function_json.clone()).expect("deserialize JobResult::Function");
    assert_eq!(function_restored.format_version(), 1);
    assert_function_handle_exact(
        function_restored.function().expect("Function variant"),
        &function,
    );
    assert_eq!(
        function_restored
            .function()
            .expect("Function variant")
            .id()
            .as_str(),
        "fn.exact.job-result"
    );
    assert_eq!(function_restored, function_result);
    assert_eq!(
        serde_json::to_value(&function_restored).expect("re-serialize Function"),
        function_json
    );

    let function_a = serde_json::to_string(&function_result).expect("encode Function a");
    let function_b = serde_json::to_string(&function_result).expect("encode Function b");
    assert_eq!(
        function_a, function_b,
        "repeated Function encoding must be byte-identical"
    );
    Ok(())
}

#[test]
fn function_and_into_function_accessors_for_both_variants() -> Result<()> {
    let none = JobResult::None;
    assert!(none.function().is_none());
    assert!(none.into_function().is_none());

    let function = sample_function()?;
    let function_result = JobResult::Function(function.clone());
    assert_function_handle_exact(
        function_result.function().expect("borrowed Function"),
        &function,
    );
    let owned = function_result
        .into_function()
        .expect("owned Function from Function variant");
    assert_function_handle_exact(&owned, &function);
    assert_eq!(owned.id().as_str(), "fn.exact.job-result");
    Ok(())
}

#[test]
fn unknown_kind_field_version_and_malformed_function_fail_closed() -> Result<()> {
    let none = JobResult::None;
    let none_json = serde_json::to_value(&none).expect("serialize None");

    let mut unknown_kind = none_json.clone();
    unknown_kind["kind"] = Value::String("artifact".into());
    assert!(
        serde_json::from_value::<JobResult>(unknown_kind).is_err(),
        "unknown kind must fail closed and must not become None"
    );

    let mut unknown_field = none_json.clone();
    unknown_field
        .as_object_mut()
        .unwrap()
        .insert("unexpected_field".into(), Value::Bool(true));
    assert!(
        serde_json::from_value::<JobResult>(unknown_field).is_err(),
        "unknown outer field must fail closed"
    );

    let mut unknown_version = none_json.clone();
    unknown_version["format_version"] = Value::from(2);
    assert!(
        serde_json::from_value::<JobResult>(unknown_version).is_err(),
        "unsupported format_version must fail closed"
    );

    let mut unexpected_function_on_none = none_json.clone();
    unexpected_function_on_none.as_object_mut().unwrap().insert(
        "function".into(),
        serde_json::to_value(&sample_function()?).expect("nested Function"),
    );
    assert!(
        serde_json::from_value::<JobResult>(unexpected_function_on_none).is_err(),
        "kind=none with unexpected function field must fail closed"
    );

    let function = sample_function()?;
    let function_json =
        serde_json::to_value(&JobResult::Function(function)).expect("serialize Function");

    let mut missing_function = function_json.clone();
    missing_function.as_object_mut().unwrap().remove("function");
    assert!(
        serde_json::from_value::<JobResult>(missing_function).is_err(),
        "kind=function without function field must fail closed"
    );

    let mut empty_function_id = function_json.clone();
    empty_function_id["function"]["id"] = Value::String("".into());
    assert!(
        serde_json::from_value::<JobResult>(empty_function_id).is_err(),
        "empty nested Function ID must fail closed"
    );

    let mut unknown_nested_function_field = function_json.clone();
    unknown_nested_function_field["function"]
        .as_object_mut()
        .unwrap()
        .insert("unexpected_field".into(), Value::Bool(true));
    assert!(
        serde_json::from_value::<JobResult>(unknown_nested_function_field).is_err(),
        "unknown nested Function field must fail closed"
    );

    let mut malformed_nested_version = function_json.clone();
    malformed_nested_version["function"]["format_version"] = Value::from(2);
    assert!(
        serde_json::from_value::<JobResult>(malformed_nested_version).is_err(),
        "malformed nested Function must fail closed"
    );

    // Unknown wire must not be preserved as a public variant or downgraded to None.
    let unknown_raw = serde_json::json!({
        "format_version": 1,
        "kind": "future_result_kind",
        "raw": {"keep": true}
    });
    assert!(
        serde_json::from_value::<JobResult>(unknown_raw).is_err(),
        "unknown kind must fail closed without a public raw/unknown variant"
    );
    Ok(())
}

#[test]
fn outer_result_excludes_forbidden_fields() -> Result<()> {
    let none_json = serde_json::to_value(&JobResult::None).expect("serialize None");
    assert_json_object_keys_exact(&none_json, &["format_version", "kind"]);
    assert_outer_forbidden_keys_absent(&none_json);

    let function = sample_function()?;
    let function_json =
        serde_json::to_value(&JobResult::Function(function)).expect("serialize Function");
    assert_json_object_keys_exact(&function_json, &["format_version", "kind", "function"]);
    assert_outer_forbidden_keys_absent(&function_json);

    // Nested Function may carry `id` and signature parameter `name`; those are
    // not outer result keys and must remain present on the nested object.
    assert_eq!(
        function_json["function"]["id"],
        Value::String("fn.exact.job-result".into())
    );
    assert!(
        function_json["function"]["signature"]["parameters"][0]
            .get("name")
            .is_some(),
        "nested signature parameter `name` remains legitimate"
    );
    Ok(())
}
