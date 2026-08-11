// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Contract tests for RegisterFunctionJobSpec (FF-008 / B1d).
//!
//! These tests pin the intended public surface under [`lancedb::function`] for
//! registration Job operation input only. They intentionally fail to compile
//! until that API exists.
//!
//! Rejection cases are judged by `Result` structure (`is_err` / `is_ok`), never
//! by diagnostic message substrings. Catalog execution, typed Job wait, and
//! result Function publication are out of scope.

use std::collections::BTreeSet;

use arrow_schema::DataType;
use lancedb::Result;
use lancedb::function::{
    FunctionCapability, FunctionDefinition, FunctionId, FunctionOutput, FunctionParameter,
    FunctionSignature, PythonFunctionDefinition, RegisterFunctionJobSpec,
};
use serde_json::Value;

fn sample_signature() -> Result<FunctionSignature> {
    FunctionSignature::try_new(
        vec![
            FunctionParameter::new("text", DataType::Utf8),
            FunctionParameter::new("limit", DataType::Int32),
        ],
        FunctionOutput::new(DataType::Utf8, true),
    )
}

fn sample_source() -> &'static str {
    "def normalize(text, limit):\n    return text[:limit]\n"
}

fn sample_python_definition() -> Result<PythonFunctionDefinition> {
    PythonFunctionDefinition::try_new(
        "normalize_mod",
        "normalize",
        sample_source(),
        "3.12",
        vec!["Unidecode==1.3.8".to_string()],
    )
}

fn sample_capabilities() -> Result<Vec<FunctionCapability>> {
    Ok(vec![
        FunctionCapability::try_network("https://api.example.com")?,
        FunctionCapability::try_secret("secret://team/api-token", "API_TOKEN")?,
    ])
}

fn sample_definition() -> Result<FunctionDefinition> {
    FunctionDefinition::try_new(
        sample_signature()?,
        sample_python_definition()?,
        sample_capabilities()?,
    )
}

fn sample_create_spec() -> Result<RegisterFunctionJobSpec> {
    RegisterFunctionJobSpec::try_new("text.normalize", sample_definition()?, None)
}

fn sample_replace_spec() -> Result<RegisterFunctionJobSpec> {
    RegisterFunctionJobSpec::try_new(
        "text.normalize",
        sample_definition()?,
        Some(FunctionId::try_new("fn.existing.exact")?),
    )
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

/// Lifecycle / identity / artifact / runtime keys that must not appear as public
/// object keys on RegisterFunctionJobSpec wire. Exact key matching only: do not
/// substring-scan encoded JSON (source text, parameter `name`, and
/// `expected_current_function_id` must not false-match).
const FORBIDDEN_SPEC_KEYS: &[&str] = &[
    "id",
    "function_id",
    "FunctionId",
    "new_function_id",
    "generated_function_id",
    "result_function_id",
    "version",
    "function_version",
    "FunctionVersion",
    "user_version",
    "lineage",
    "idempotency_key",
    "retry_key",
    "idempotency",
    "job_id",
    "jobId",
    "state",
    "status",
    "attempt",
    "attempt_id",
    "timestamp",
    "created_at",
    "updated_at",
    "table",
    "table_name",
    "table_ref",
    "executor",
    "environment",
    "artifact",
    "artifact_digest",
    "digest",
    "storage",
    "storage_location",
    "location",
    "worker",
    "scheduler",
    "replica",
    "placement",
];

fn assert_object_keys_not_forbidden(value: &Value, context: &str) {
    let object = value.as_object().unwrap_or_else(|| {
        panic!("expected JSON object at {context}, got {value}");
    });
    for key in object.keys() {
        assert!(
            !FORBIDDEN_SPEC_KEYS.contains(&key.as_str()),
            "RegisterFunctionJobSpec wire must not contain forbidden key `{key}` at {context}: {value}"
        );
    }
}

fn assert_forbidden_spec_keys_absent(value: &Value) {
    assert_json_object_keys_exact(
        value,
        &[
            "format_version",
            "name",
            "definition",
            "expected_current_function_id",
        ],
    );
    assert_object_keys_not_forbidden(value, "RegisterFunctionJobSpec");

    // Precondition field is allowed; a generated/new Function ID key is not.
    assert!(
        value.get("function_id").is_none(),
        "spec must not carry generated/new `function_id`; use expected_current_function_id only: {value}"
    );
    assert!(
        value.get("id").is_none(),
        "spec must not carry generated/new Function `id`: {value}"
    );

    let definition = value.get("definition").expect("definition object");
    assert_json_object_keys_exact(
        definition,
        &[
            "format_version",
            "signature",
            "implementation",
            "capabilities",
        ],
    );
    assert_object_keys_not_forbidden(definition, "definition");
    // Catalog/function identity name belongs on the spec, not the nested definition.
    assert!(
        definition.get("name").is_none(),
        "nested definition must not contain catalog name: {definition}"
    );

    let implementation = definition
        .get("implementation")
        .expect("implementation object");
    assert_json_object_keys_exact(
        implementation,
        &["kind", "module", "callable", "source", "python", "packages"],
    );
    assert_object_keys_not_forbidden(implementation, "definition.implementation");

    let capabilities = definition
        .get("capabilities")
        .and_then(Value::as_array)
        .expect("capabilities array");
    for (idx, capability) in capabilities.iter().enumerate() {
        let context = format!("definition.capabilities[{idx}]");
        assert_object_keys_not_forbidden(capability, &context);
    }

    let signature = definition.get("signature").expect("signature object");
    assert_object_keys_not_forbidden(signature, "definition.signature");
    if let Some(parameters) = signature.get("parameters").and_then(Value::as_array) {
        for (idx, parameter) in parameters.iter().enumerate() {
            let context = format!("definition.signature.parameters[{idx}]");
            assert_object_keys_not_forbidden(parameter, &context);
            // Parameter object key `name` is legitimate and must not be treated
            // as a forbidden catalog/function identity field.
            assert!(
                parameter.get("name").is_some(),
                "{context} must include parameter `name`"
            );
        }
    }
}

#[test]
fn create_and_replace_round_trip_pins_name_definition_and_precondition() -> Result<()> {
    let create = sample_create_spec()?;
    assert_eq!(create.format_version(), 1);
    assert_eq!(create.name(), "text.normalize");
    assert!(create.expected_current_function_id().is_none());
    assert_eq!(
        create.definition().python_definition().source(),
        sample_source()
    );
    assert_eq!(create.definition().capabilities().len(), 2);
    assert_eq!(
        create.definition().capabilities()[0].origin(),
        Some("https://api.example.com")
    );
    assert_eq!(
        create.definition().capabilities()[1].reference(),
        Some("secret://team/api-token")
    );

    let create_json = serde_json::to_value(&create).expect("serialize create spec");
    assert_eq!(create_json["format_version"], 1);
    assert_eq!(create_json["name"], Value::String("text.normalize".into()));
    assert_eq!(create_json["expected_current_function_id"], Value::Null);

    let create_restored: RegisterFunctionJobSpec =
        serde_json::from_value(create_json.clone()).expect("deserialize create spec");
    assert_eq!(create_restored.format_version(), 1);
    assert_eq!(create_restored.name(), "text.normalize");
    assert!(create_restored.expected_current_function_id().is_none());
    assert_eq!(
        create_restored.definition().python_definition().module(),
        create.definition().python_definition().module()
    );
    assert_eq!(
        create_restored.definition().python_definition().callable(),
        create.definition().python_definition().callable()
    );
    assert_eq!(
        create_restored.definition().python_definition().source(),
        create.definition().python_definition().source()
    );
    assert_eq!(
        create_restored.definition().python_definition().python(),
        create.definition().python_definition().python()
    );
    assert_eq!(
        create_restored.definition().python_definition().packages(),
        create.definition().python_definition().packages()
    );
    assert_eq!(
        create_restored.definition().capabilities()[1].reference(),
        create.definition().capabilities()[1].reference()
    );
    assert_eq!(
        create_restored.definition().capabilities()[1].environment_variable(),
        create.definition().capabilities()[1].environment_variable()
    );

    let replace = sample_replace_spec()?;
    assert_eq!(replace.name(), "text.normalize");
    assert_eq!(
        replace
            .expected_current_function_id()
            .map(FunctionId::as_str),
        Some("fn.existing.exact")
    );

    let replace_json = serde_json::to_value(&replace).expect("serialize replace spec");
    assert_eq!(
        replace_json["expected_current_function_id"],
        Value::String("fn.existing.exact".into())
    );
    let replace_restored: RegisterFunctionJobSpec =
        serde_json::from_value(replace_json.clone()).expect("deserialize replace spec");
    assert_eq!(
        replace_restored
            .expected_current_function_id()
            .map(FunctionId::as_str),
        Some("fn.existing.exact")
    );
    assert_eq!(replace_restored.name(), replace.name());
    assert_eq!(
        replace_restored.definition().python_definition().source(),
        replace.definition().python_definition().source()
    );

    // Byte-for-byte repeated serde_json encoding for the same value.
    let create_a = serde_json::to_string(&create).expect("encode create a");
    let create_b = serde_json::to_string(&create).expect("encode create b");
    assert_eq!(create_a, create_b);
    let replace_a = serde_json::to_string(&replace).expect("encode replace a");
    let replace_b = serde_json::to_string(&replace).expect("encode replace b");
    assert_eq!(replace_a, replace_b);
    assert_eq!(
        serde_json::to_value(&create_restored).expect("re-serialize create"),
        create_json
    );
    assert_eq!(
        serde_json::to_value(&replace_restored).expect("re-serialize replace"),
        replace_json
    );
    Ok(())
}

#[test]
fn create_wire_pins_exact_key_set_and_null_precondition() -> Result<()> {
    let create = sample_create_spec()?;
    let json = serde_json::to_value(&create).expect("serialize create spec");

    // Exact object key set; Map iteration order is not part of the contract.
    assert_json_object_keys_exact(
        &json,
        &[
            "format_version",
            "name",
            "definition",
            "expected_current_function_id",
        ],
    );
    assert_eq!(json["format_version"], 1);
    assert_eq!(json["name"], Value::String("text.normalize".into()));
    // Create-if-absent always serializes the precondition key as JSON null.
    assert_eq!(json["expected_current_function_id"], Value::Null);
    assert!(json["expected_current_function_id"].is_null());

    let replace = sample_replace_spec()?;
    let replace_json = serde_json::to_value(&replace).expect("serialize replace spec");
    assert_json_object_keys_exact(
        &replace_json,
        &[
            "format_version",
            "name",
            "definition",
            "expected_current_function_id",
        ],
    );
    assert_eq!(
        replace_json["expected_current_function_id"],
        Value::String("fn.existing.exact".into())
    );
    Ok(())
}

#[test]
fn empty_name_and_expected_id_unknown_field_and_version_fail_closed() -> Result<()> {
    let definition = sample_definition()?;

    assert!(
        RegisterFunctionJobSpec::try_new("", definition.clone(), None).is_err(),
        "empty name must be rejected by constructor"
    );
    assert!(
        RegisterFunctionJobSpec::try_new(
            "text.normalize",
            definition,
            Some(FunctionId::try_new("fn.existing.exact")?),
        )
        .is_ok(),
        "non-empty name with exact expected ID must construct"
    );

    let create = sample_create_spec()?;
    let json = serde_json::to_value(&create).expect("serialize create spec");

    let mut empty_name = json.clone();
    empty_name["name"] = Value::String("".into());
    assert!(
        serde_json::from_value::<RegisterFunctionJobSpec>(empty_name).is_err(),
        "decode must reject empty name"
    );

    let mut empty_expected_id = json.clone();
    empty_expected_id["expected_current_function_id"] = Value::String("".into());
    assert!(
        serde_json::from_value::<RegisterFunctionJobSpec>(empty_expected_id).is_err(),
        "decode must reject empty expected_current_function_id"
    );

    let mut unknown_field = json.clone();
    unknown_field
        .as_object_mut()
        .unwrap()
        .insert("unexpected_field".into(), Value::Bool(true));
    assert!(
        serde_json::from_value::<RegisterFunctionJobSpec>(unknown_field).is_err(),
        "unknown outer field must fail closed"
    );

    let mut unknown_version = json.clone();
    unknown_version["format_version"] = Value::from(2);
    assert!(
        serde_json::from_value::<RegisterFunctionJobSpec>(unknown_version).is_err(),
        "format_version other than 1 must fail closed"
    );
    Ok(())
}

#[test]
fn spec_wire_excludes_generated_identity_job_lifecycle_and_artifact_fields() -> Result<()> {
    let create = sample_create_spec()?;
    let create_json = serde_json::to_value(&create).expect("serialize create spec");
    assert_forbidden_spec_keys_absent(&create_json);

    let replace = sample_replace_spec()?;
    let replace_json = serde_json::to_value(&replace).expect("serialize replace spec");
    assert_forbidden_spec_keys_absent(&replace_json);
    // Replace may carry the exact opaque precondition string only.
    assert_eq!(
        replace_json["expected_current_function_id"],
        Value::String("fn.existing.exact".into())
    );
    Ok(())
}

#[test]
fn debug_redacts_source_and_secret_reference_while_nested_getters_remain_exact() -> Result<()> {
    let source = sample_source();
    let secret_reference = "secret://team/api-token";

    let create = sample_create_spec()?;
    assert_eq!(create.definition().python_definition().source(), source);
    assert_eq!(
        create.definition().capabilities()[1].reference(),
        Some(secret_reference)
    );
    let create_debug = format!("{create:?}");
    assert!(
        !create_debug.contains(source),
        "create RegisterFunctionJobSpec Debug must not contain source body: {create_debug}"
    );
    assert!(
        !create_debug.contains("return text[:limit]"),
        "create RegisterFunctionJobSpec Debug must not leak source fragments: {create_debug}"
    );
    assert!(
        !create_debug.contains(secret_reference),
        "create RegisterFunctionJobSpec Debug must not contain secret reference: {create_debug}"
    );

    let replace = sample_replace_spec()?;
    assert_eq!(replace.definition().python_definition().source(), source);
    assert_eq!(
        replace.definition().capabilities()[1].reference(),
        Some(secret_reference)
    );
    assert_eq!(
        replace
            .expected_current_function_id()
            .map(FunctionId::as_str),
        Some("fn.existing.exact")
    );
    let replace_debug = format!("{replace:?}");
    assert!(
        !replace_debug.contains(source),
        "replace RegisterFunctionJobSpec Debug must not contain source body: {replace_debug}"
    );
    assert!(
        !replace_debug.contains(secret_reference),
        "replace RegisterFunctionJobSpec Debug must not contain secret reference: {replace_debug}"
    );
    Ok(())
}

#[test]
fn nested_definition_wire_is_exact_ff007_function_definition() -> Result<()> {
    let definition = sample_definition()?;
    let expected_definition_wire =
        serde_json::to_value(&definition).expect("serialize FunctionDefinition");

    let create = RegisterFunctionJobSpec::try_new("text.normalize", definition.clone(), None)?;
    let create_json = serde_json::to_value(&create).expect("serialize create spec");
    assert_eq!(
        create_json["definition"], expected_definition_wire,
        "nested definition must be the exact FF-007 FunctionDefinition wire"
    );
    assert_json_object_keys_exact(
        &create_json["definition"],
        &[
            "format_version",
            "signature",
            "implementation",
            "capabilities",
        ],
    );
    assert_eq!(
        create_json["definition"]["implementation"]["source"],
        Value::String(sample_source().into())
    );
    assert_eq!(
        create_json["definition"]["capabilities"][1]["reference"],
        Value::String("secret://team/api-token".into())
    );
    // Not a summarized / digested / artifact reference indirection.
    assert!(create_json["definition"].get("digest").is_none());
    assert!(create_json["definition"].get("artifact").is_none());
    assert!(create_json["definition"].get("artifact_digest").is_none());
    assert!(create_json["definition"].get("storage").is_none());
    assert!(
        create_json["definition"]["implementation"]
            .get("digest")
            .is_none()
    );
    assert!(
        create_json["definition"]["implementation"]
            .get("artifact")
            .is_none()
    );

    let replace = RegisterFunctionJobSpec::try_new(
        "text.normalize",
        definition,
        Some(FunctionId::try_new("fn.existing.exact")?),
    )?;
    let replace_json = serde_json::to_value(&replace).expect("serialize replace spec");
    assert_eq!(
        replace_json["definition"], expected_definition_wire,
        "replace nested definition must remain the exact FF-007 wire"
    );
    Ok(())
}
