// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Contract tests for FunctionDefinition registration input (FF-007 / B1c).
//!
//! These tests pin the intended public surface under [`lancedb::function`] for
//! Python definition transport only. They intentionally fail to compile until
//! that API exists.
//!
//! Rejection cases are judged by `Result` structure (`is_err` / `is_ok`), never
//! by diagnostic message substrings.

use std::collections::BTreeSet;

use arrow_schema::DataType;
use lancedb::Result;
use lancedb::function::{
    Function, FunctionCapability, FunctionDefinition, FunctionId, FunctionOutput,
    FunctionParameter, FunctionSignature, PythonFunctionDefinition,
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

/// Identity / lineage / artifact / runtime fields that must not appear as public
/// object keys on FunctionDefinition wire. Parameter object key `name` is not
/// listed here: FunctionSignature legitimately uses it under `signature.parameters`.
const FORBIDDEN_DEFINITION_KEYS: &[&str] = &[
    "id",
    "function_id",
    "FunctionId",
    "catalog",
    "catalog_name",
    "version",
    "function_version",
    "FunctionVersion",
    "lineage",
    "user_version",
    "idempotency_key",
    "digest",
    "artifact",
    "artifact_digest",
    "storage",
    "storage_location",
    "location",
    "deterministic",
    "null_policy",
    "nullPolicy",
    "timestamp",
    "created_at",
    "updated_at",
    "worker",
    "scheduler",
    "attempt",
    "attempt_id",
    "replica",
    "placement",
];

fn assert_object_keys_not_forbidden(value: &Value, context: &str) {
    let object = value.as_object().unwrap_or_else(|| {
        panic!("expected JSON object at {context}, got {value}");
    });
    for key in object.keys() {
        assert!(
            !FORBIDDEN_DEFINITION_KEYS.contains(&key.as_str()),
            "FunctionDefinition wire must not contain forbidden key `{key}` at {context}: {value}"
        );
    }
}

fn assert_forbidden_definition_keys_absent(value: &Value) {
    // Top-level definition key set (order-independent).
    assert_json_object_keys_exact(
        value,
        &[
            "format_version",
            "signature",
            "implementation",
            "capabilities",
        ],
    );
    assert_object_keys_not_forbidden(value, "definition");
    // Catalog / function identity name is absent at definition root; parameter
    // `name` is allowed only under signature.parameters.
    assert!(
        value.get("name").is_none(),
        "top-level FunctionDefinition wire must not contain catalog/function identity key `name`: {value}"
    );
    assert!(
        value.get("catalog_name").is_none(),
        "top-level FunctionDefinition wire must not contain `catalog_name`: {value}"
    );

    let implementation = value.get("implementation").expect("implementation object");
    assert_json_object_keys_exact(
        implementation,
        &["kind", "module", "callable", "source", "python", "packages"],
    );
    assert_object_keys_not_forbidden(implementation, "implementation");
    assert!(
        implementation.get("name").is_none(),
        "implementation must not contain catalog/function identity key `name`: {implementation}"
    );
    assert!(
        implementation.get("catalog_name").is_none(),
        "implementation must not contain `catalog_name`: {implementation}"
    );

    let capabilities = value
        .get("capabilities")
        .and_then(Value::as_array)
        .expect("capabilities array");
    for (idx, capability) in capabilities.iter().enumerate() {
        let kind = capability
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("capabilities[{idx}] missing kind"));
        match kind {
            "network" => assert_json_object_keys_exact(capability, &["kind", "origin"]),
            "secret" => assert_json_object_keys_exact(
                capability,
                &["kind", "reference", "environment_variable"],
            ),
            other => panic!("unexpected capability kind `{other}` in contract fixture"),
        }
        let context = format!("capabilities[{idx}]");
        assert_object_keys_not_forbidden(capability, &context);
        assert!(
            capability.get("name").is_none(),
            "{context} must not contain catalog/function identity key `name`: {capability}"
        );
        assert!(
            capability.get("catalog_name").is_none(),
            "{context} must not contain `catalog_name`: {capability}"
        );
    }

    // Signature may carry parameter objects with key `name`. Still reject
    // identity/lineage/runtime keys and function-identity `name`/`catalog_name`
    // on the signature and output objects themselves.
    let signature = value.get("signature").expect("signature object");
    assert_object_keys_not_forbidden(signature, "signature");
    assert!(
        signature.get("name").is_none(),
        "signature object must not contain catalog/function identity key `name`: {signature}"
    );
    assert!(
        signature.get("catalog_name").is_none(),
        "signature object must not contain `catalog_name`: {signature}"
    );
    if let Some(parameters) = signature.get("parameters").and_then(Value::as_array) {
        for (idx, parameter) in parameters.iter().enumerate() {
            let context = format!("signature.parameters[{idx}]");
            assert_object_keys_not_forbidden(parameter, &context);
            assert!(
                parameter.get("catalog_name").is_none(),
                "{context} must not contain `catalog_name`: {parameter}"
            );
            // `name` is intentionally allowed on parameter objects.
            assert!(
                parameter.get("name").is_some(),
                "{context} must include parameter `name`"
            );
        }
    }
    if let Some(output) = signature.get("output") {
        assert_object_keys_not_forbidden(output, "signature.output");
        assert!(
            output.get("name").is_none(),
            "signature.output must not contain catalog/function identity key `name`: {output}"
        );
        assert!(
            output.get("catalog_name").is_none(),
            "signature.output must not contain `catalog_name`: {output}"
        );
    }
}

#[test]
fn definition_json_round_trip_pins_exact_wire_shape_and_order() -> Result<()> {
    let definition = sample_definition()?;

    assert_eq!(definition.signature().parameters().len(), 2);
    assert_eq!(definition.signature().parameters()[0].name(), "text");
    assert_eq!(
        definition.signature().parameters()[0].data_type(),
        &DataType::Utf8
    );
    assert_eq!(definition.signature().parameters()[1].name(), "limit");
    assert_eq!(
        definition.signature().parameters()[1].data_type(),
        &DataType::Int32
    );
    assert_eq!(definition.signature().output().data_type(), &DataType::Utf8);
    assert!(definition.signature().output().nullable());

    let python = definition.python_definition();
    assert_eq!(python.module(), "normalize_mod");
    assert_eq!(python.callable(), "normalize");
    assert_eq!(python.source(), sample_source());
    assert_eq!(python.python(), "3.12");
    assert_eq!(python.packages(), &["Unidecode==1.3.8".to_string()]);

    let capabilities = definition.capabilities();
    assert_eq!(capabilities.len(), 2);
    assert_eq!(capabilities[0].origin(), Some("https://api.example.com"));
    assert_eq!(capabilities[0].reference(), None);
    assert_eq!(capabilities[0].environment_variable(), None);
    assert_eq!(capabilities[1].reference(), Some("secret://team/api-token"));
    assert_eq!(capabilities[1].environment_variable(), Some("API_TOKEN"));
    assert_eq!(capabilities[1].origin(), None);

    let json = serde_json::to_value(&definition).expect("serialize FunctionDefinition");
    assert_json_object_keys_exact(
        &json,
        &[
            "format_version",
            "signature",
            "implementation",
            "capabilities",
        ],
    );
    assert_eq!(json["format_version"], 1);

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
    assert_eq!(parameters[0]["name"], Value::String("text".into()));
    assert_eq!(parameters[1]["name"], Value::String("limit".into()));
    for parameter in parameters {
        assert_json_object_keys_subset(parameter, &["name", "data_type_ipc"]);
        assert!(
            parameter
                .get("data_type_ipc")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty()),
            "parameter data_type_ipc must be non-empty base64"
        );
    }
    let output = signature
        .get("output")
        .and_then(Value::as_object)
        .expect("output object");
    assert_json_object_keys_subset(
        &Value::Object(output.clone()),
        &["data_type_ipc", "nullable"],
    );
    assert_eq!(output.get("nullable"), Some(&Value::Bool(true)));

    let implementation = json.get("implementation").expect("implementation object");
    assert_json_object_keys_exact(
        implementation,
        &["kind", "module", "callable", "source", "python", "packages"],
    );
    assert_eq!(implementation["kind"], Value::String("python".into()));
    assert_eq!(
        implementation["module"],
        Value::String("normalize_mod".into())
    );
    assert_eq!(
        implementation["callable"],
        Value::String("normalize".into())
    );
    assert_eq!(
        implementation["source"],
        Value::String(sample_source().into())
    );
    assert_eq!(implementation["python"], Value::String("3.12".into()));
    assert_eq!(
        implementation["packages"],
        Value::Array(vec![Value::String("Unidecode==1.3.8".into())])
    );

    let capabilities_json = json
        .get("capabilities")
        .and_then(Value::as_array)
        .expect("capabilities array");
    assert_eq!(capabilities_json.len(), 2);
    assert_json_object_keys_exact(&capabilities_json[0], &["kind", "origin"]);
    assert_eq!(
        capabilities_json[0]["kind"],
        Value::String("network".into())
    );
    assert_eq!(
        capabilities_json[0]["origin"],
        Value::String("https://api.example.com".into())
    );
    assert_json_object_keys_exact(
        &capabilities_json[1],
        &["kind", "reference", "environment_variable"],
    );
    assert_eq!(capabilities_json[1]["kind"], Value::String("secret".into()));
    assert_eq!(
        capabilities_json[1]["reference"],
        Value::String("secret://team/api-token".into())
    );
    assert_eq!(
        capabilities_json[1]["environment_variable"],
        Value::String("API_TOKEN".into())
    );

    // Same ordered signature IPC representation as Function handle transport.
    let function = Function::new(FunctionId::try_new("fn.wire.compare")?, sample_signature()?);
    let function_json = serde_json::to_value(&function).expect("serialize Function");
    assert_eq!(json["signature"], function_json["signature"]);

    let restored: FunctionDefinition =
        serde_json::from_value(json.clone()).expect("deserialize FunctionDefinition");
    assert_eq!(
        restored.signature().parameters()[0].name(),
        definition.signature().parameters()[0].name()
    );
    assert_eq!(
        restored.signature().parameters()[0].data_type(),
        definition.signature().parameters()[0].data_type()
    );
    assert_eq!(
        restored.signature().parameters()[1].name(),
        definition.signature().parameters()[1].name()
    );
    assert_eq!(
        restored.signature().parameters()[1].data_type(),
        definition.signature().parameters()[1].data_type()
    );
    assert_eq!(
        restored.signature().output().data_type(),
        definition.signature().output().data_type()
    );
    assert_eq!(
        restored.signature().output().nullable(),
        definition.signature().output().nullable()
    );
    assert_eq!(
        restored.python_definition().module(),
        definition.python_definition().module()
    );
    assert_eq!(
        restored.python_definition().callable(),
        definition.python_definition().callable()
    );
    assert_eq!(
        restored.python_definition().source(),
        definition.python_definition().source()
    );
    assert_eq!(
        restored.python_definition().python(),
        definition.python_definition().python()
    );
    assert_eq!(
        restored.python_definition().packages(),
        definition.python_definition().packages()
    );
    assert_eq!(restored.capabilities().len(), 2);
    assert_eq!(
        restored.capabilities()[0].origin(),
        definition.capabilities()[0].origin()
    );
    assert_eq!(
        restored.capabilities()[1].reference(),
        definition.capabilities()[1].reference()
    );
    assert_eq!(
        restored.capabilities()[1].environment_variable(),
        definition.capabilities()[1].environment_variable()
    );

    // Package and capability order are part of the structural wire.
    let multi_pkg = FunctionDefinition::try_new(
        sample_signature()?,
        PythonFunctionDefinition::try_new(
            "normalize_mod",
            "normalize",
            sample_source(),
            "3.12",
            vec![
                "Unidecode==1.3.8".to_string(),
                "requests==2.32.3".to_string(),
            ],
        )?,
        vec![
            FunctionCapability::try_secret("secret://team/api-token", "API_TOKEN")?,
            FunctionCapability::try_network("https://api.example.com")?,
            FunctionCapability::try_network("https://other.example.com")?,
        ],
    )?;
    let multi_json = serde_json::to_value(&multi_pkg).expect("serialize multi-order definition");
    assert_eq!(
        multi_json["implementation"]["packages"],
        Value::Array(vec![
            Value::String("Unidecode==1.3.8".into()),
            Value::String("requests==2.32.3".into()),
        ])
    );
    assert_eq!(
        multi_json["capabilities"][0]["kind"],
        Value::String("secret".into())
    );
    assert_eq!(
        multi_json["capabilities"][1]["origin"],
        Value::String("https://api.example.com".into())
    );
    assert_eq!(
        multi_json["capabilities"][2]["origin"],
        Value::String("https://other.example.com".into())
    );
    let multi_restored: FunctionDefinition =
        serde_json::from_value(multi_json.clone()).expect("deserialize multi-order definition");
    assert_eq!(
        multi_restored.python_definition().packages(),
        &[
            "Unidecode==1.3.8".to_string(),
            "requests==2.32.3".to_string()
        ]
    );
    assert_eq!(
        multi_restored.capabilities()[0].reference(),
        Some("secret://team/api-token")
    );
    assert_eq!(
        multi_restored.capabilities()[1].origin(),
        Some("https://api.example.com")
    );
    assert_eq!(
        multi_restored.capabilities()[2].origin(),
        Some("https://other.example.com")
    );

    // Byte-for-byte repeated serde_json encoding for the same value.
    let encoded_a = serde_json::to_string(&definition).expect("encode a");
    let encoded_b = serde_json::to_string(&definition).expect("encode b");
    assert_eq!(encoded_a, encoded_b);
    assert_eq!(
        serde_json::to_value(&restored).expect("re-serialize restored"),
        json
    );
    assert_eq!(
        serde_json::to_string(&multi_restored).expect("re-encode multi"),
        serde_json::to_string(&multi_pkg).expect("encode multi")
    );
    Ok(())
}

#[test]
fn definition_wire_excludes_identity_lineage_artifact_and_runtime_fields() -> Result<()> {
    let definition = sample_definition()?;
    let json = serde_json::to_value(&definition).expect("serialize FunctionDefinition");
    // Forbidden public fields are object keys at structural levels only.
    // Do not substring-scan encoded JSON: user source/reference/package text
    // may legitimately contain those tokens.
    assert_forbidden_definition_keys_absent(&json);
    Ok(())
}

#[test]
fn definition_decode_fails_closed_for_unknown_version_fields_and_kinds() -> Result<()> {
    let definition = sample_definition()?;
    let json = serde_json::to_value(&definition).expect("serialize FunctionDefinition");

    let mut unknown_version = json.clone();
    unknown_version["format_version"] = Value::from(2);
    assert!(
        serde_json::from_value::<FunctionDefinition>(unknown_version).is_err(),
        "format_version other than 1 must fail closed"
    );

    let mut unknown_outer = json.clone();
    unknown_outer
        .as_object_mut()
        .unwrap()
        .insert("unexpected_field".into(), Value::Bool(true));
    assert!(
        serde_json::from_value::<FunctionDefinition>(unknown_outer).is_err(),
        "unknown outer field must fail closed"
    );

    let mut unknown_implementation_field = json.clone();
    unknown_implementation_field["implementation"]
        .as_object_mut()
        .unwrap()
        .insert("entrypoint".into(), Value::String("main".into()));
    assert!(
        serde_json::from_value::<FunctionDefinition>(unknown_implementation_field).is_err(),
        "unknown nested implementation field must fail closed"
    );

    let mut unknown_capability_field = json.clone();
    unknown_capability_field["capabilities"][0]
        .as_object_mut()
        .unwrap()
        .insert("headers".into(), Value::Object(Default::default()));
    assert!(
        serde_json::from_value::<FunctionDefinition>(unknown_capability_field).is_err(),
        "unknown nested capability field must fail closed"
    );

    let mut unknown_implementation_kind = json.clone();
    unknown_implementation_kind["implementation"]["kind"] = Value::String("builtin".into());
    assert!(
        serde_json::from_value::<FunctionDefinition>(unknown_implementation_kind).is_err(),
        "unknown implementation kind must fail closed"
    );

    let mut unknown_capability_kind = json.clone();
    unknown_capability_kind["capabilities"][0]["kind"] = Value::String("filesystem".into());
    assert!(
        serde_json::from_value::<FunctionDefinition>(unknown_capability_kind).is_err(),
        "unknown capability kind must fail closed"
    );
    Ok(())
}

#[test]
fn constructors_and_decode_reject_empty_fields_and_duplicate_packages() -> Result<()> {
    let signature = sample_signature()?;
    let packages = vec!["Unidecode==1.3.8".to_string()];
    let capabilities = sample_capabilities()?;

    assert!(
        PythonFunctionDefinition::try_new(
            "",
            "normalize",
            sample_source(),
            "3.12",
            packages.clone()
        )
        .is_err(),
        "empty module must be rejected"
    );
    assert!(
        PythonFunctionDefinition::try_new(
            "normalize_mod",
            "",
            sample_source(),
            "3.12",
            packages.clone()
        )
        .is_err(),
        "empty callable must be rejected"
    );
    assert!(
        PythonFunctionDefinition::try_new(
            "normalize_mod",
            "normalize",
            "",
            "3.12",
            packages.clone()
        )
        .is_err(),
        "empty source must be rejected"
    );
    assert!(
        PythonFunctionDefinition::try_new(
            "normalize_mod",
            "normalize",
            sample_source(),
            "",
            packages.clone()
        )
        .is_err(),
        "empty python runtime request must be rejected"
    );
    assert!(
        PythonFunctionDefinition::try_new(
            "normalize_mod",
            "normalize",
            sample_source(),
            "3.12",
            vec!["".to_string()],
        )
        .is_err(),
        "empty package requirement must be rejected"
    );
    assert!(
        PythonFunctionDefinition::try_new(
            "normalize_mod",
            "normalize",
            sample_source(),
            "3.12",
            vec![
                "Unidecode==1.3.8".to_string(),
                "Unidecode==1.3.8".to_string(),
            ],
        )
        .is_err(),
        "duplicate package requirements must be rejected"
    );

    assert!(
        FunctionCapability::try_network("").is_err(),
        "empty network origin must be rejected"
    );
    assert!(
        FunctionCapability::try_secret("", "API_TOKEN").is_err(),
        "empty secret reference must be rejected"
    );
    assert!(
        FunctionCapability::try_secret("secret://team/api-token", "").is_err(),
        "empty secret environment variable must be rejected"
    );

    // Decode path must enforce the same emptiness / uniqueness rules.
    let definition = FunctionDefinition::try_new(
        signature.clone(),
        sample_python_definition()?,
        capabilities.clone(),
    )?;
    let json = serde_json::to_value(&definition).expect("serialize FunctionDefinition");

    for (pointer, empty) in [
        ("/implementation/module", ""),
        ("/implementation/callable", ""),
        ("/implementation/source", ""),
        ("/implementation/python", ""),
        ("/capabilities/0/origin", ""),
        ("/capabilities/1/reference", ""),
        ("/capabilities/1/environment_variable", ""),
    ] {
        let mut invalid = json.clone();
        let target = invalid
            .pointer_mut(pointer)
            .unwrap_or_else(|| panic!("missing pointer {pointer}"));
        *target = Value::String(empty.into());
        assert!(
            serde_json::from_value::<FunctionDefinition>(invalid).is_err(),
            "decode must reject empty value at {pointer}"
        );
    }

    let mut empty_package = json.clone();
    empty_package["implementation"]["packages"] = Value::Array(vec![Value::String("".into())]);
    assert!(
        serde_json::from_value::<FunctionDefinition>(empty_package).is_err(),
        "decode must reject empty package requirement"
    );

    let mut duplicate_packages = json.clone();
    duplicate_packages["implementation"]["packages"] = Value::Array(vec![
        Value::String("Unidecode==1.3.8".into()),
        Value::String("Unidecode==1.3.8".into()),
    ]);
    assert!(
        serde_json::from_value::<FunctionDefinition>(duplicate_packages).is_err(),
        "decode must reject duplicate package requirements"
    );

    // Keep the constructor path for FunctionDefinition itself structurally valid
    // when children are valid; emptiness is owned by child constructors above.
    assert!(
        FunctionDefinition::try_new(signature, sample_python_definition()?, capabilities).is_ok()
    );
    Ok(())
}

#[test]
fn secret_capability_wire_rejects_plaintext_value_fields() -> Result<()> {
    let definition = sample_definition()?;
    let json = serde_json::to_value(&definition).expect("serialize FunctionDefinition");
    let secret = &json["capabilities"][1];
    assert_json_object_keys_exact(secret, &["kind", "reference", "environment_variable"]);
    assert!(secret.get("value").is_none());
    assert!(secret.get("plaintext_secret").is_none());

    let mut with_value = json.clone();
    with_value["capabilities"][1]
        .as_object_mut()
        .unwrap()
        .insert("value".into(), Value::String("super-secret".into()));
    assert!(
        serde_json::from_value::<FunctionDefinition>(with_value).is_err(),
        "secret capability must reject `value`"
    );

    let mut with_plaintext = json.clone();
    with_plaintext["capabilities"][1]
        .as_object_mut()
        .unwrap()
        .insert(
            "plaintext_secret".into(),
            Value::String("super-secret".into()),
        );
    assert!(
        serde_json::from_value::<FunctionDefinition>(with_plaintext).is_err(),
        "secret capability must reject `plaintext_secret`"
    );
    Ok(())
}

#[test]
fn debug_redacts_source_and_secret_reference_while_getters_remain_exact() -> Result<()> {
    let python = sample_python_definition()?;
    let source = sample_source();
    assert_eq!(python.source(), source);
    let python_debug = format!("{python:?}");
    assert!(
        !python_debug.contains(source),
        "PythonFunctionDefinition Debug must not contain source body: {python_debug}"
    );
    assert!(
        !python_debug.contains("return text[:limit]"),
        "PythonFunctionDefinition Debug must not leak source fragments: {python_debug}"
    );

    let secret = FunctionCapability::try_secret("secret://team/api-token", "API_TOKEN")?;
    assert_eq!(secret.reference(), Some("secret://team/api-token"));
    assert_eq!(secret.environment_variable(), Some("API_TOKEN"));
    let secret_debug = format!("{secret:?}");
    assert!(
        !secret_debug.contains("secret://team/api-token"),
        "FunctionCapability secret Debug must not contain reference: {secret_debug}"
    );

    let definition = sample_definition()?;
    assert_eq!(definition.python_definition().source(), source);
    assert_eq!(
        definition.capabilities()[1].reference(),
        Some("secret://team/api-token")
    );
    let definition_debug = format!("{definition:?}");
    assert!(
        !definition_debug.contains(source),
        "FunctionDefinition Debug must not contain source body: {definition_debug}"
    );
    assert!(
        !definition_debug.contains("secret://team/api-token"),
        "FunctionDefinition Debug must not contain secret reference: {definition_debug}"
    );
    Ok(())
}

#[test]
fn definition_has_no_identity_before_registration_and_is_not_a_function_handle() -> Result<()> {
    let definition = sample_definition()?;
    let json = serde_json::to_value(&definition).expect("serialize FunctionDefinition");

    assert!(json.get("id").is_none());
    assert!(json.get("function_id").is_none());
    assert_json_object_keys_exact(
        &json,
        &[
            "format_version",
            "signature",
            "implementation",
            "capabilities",
        ],
    );

    // Identity exists only on the immutable Function handle after registration.
    // Definition remains a separate authoring value and does not borrow or mint an ID.
    let registered = Function::new(
        FunctionId::try_new("fn.published.after.registration")?,
        definition.signature().clone(),
    );
    assert_eq!(registered.id().as_str(), "fn.published.after.registration");
    assert_eq!(
        registered.signature().parameters().len(),
        definition.signature().parameters().len()
    );

    let definition_again = serde_json::to_value(&definition).expect("re-serialize definition");
    assert!(definition_again.get("id").is_none());
    assert!(definition_again.get("function_id").is_none());
    assert_ne!(
        serde_json::to_value(&registered).expect("serialize Function"),
        definition_again,
        "Function handle wire must remain distinct from FunctionDefinition wire"
    );
    Ok(())
}
