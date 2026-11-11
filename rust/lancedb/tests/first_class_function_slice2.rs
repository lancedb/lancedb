// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::fs;
use std::path::PathBuf;

use lancedb::Error;
use lancedb::function::FunctionRegistrationRequest;
use lancedb::secrets::{SecretBinding, SecretReference};
use serde_json::Value;

fn fixture(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/first_class_functions/v1")
        .join(name);
    fs::read_to_string(path).expect("fixture must be readable")
}

/// A registration request never models a resolved credential, at any depth.
fn assert_no_secret_values(value: &Value) {
    match value {
        Value::Object(values) => {
            for (key, value) in values {
                assert!(
                    !matches!(
                        key.as_str(),
                        "secret_value" | "secret_values" | "resolved_secret" | "resolved_secrets"
                    ),
                    "registration requests must not model resolved secret material"
                );
                assert_no_secret_values(value);
            }
        }
        Value::Array(values) => values.iter().for_each(assert_no_secret_values),
        _ => {}
    }
}

#[test]
fn registration_request_matches_shared_canonical_golden() {
    let request = FunctionRegistrationRequest::from_json(&fixture(
        "remote_function_registration_request.json",
    ))
    .expect("registration request");
    assert_eq!(request.name, "normalize_score");
    assert_eq!(request.artifact.adapter.kind, "scalar_to_arrow_batch");
    // The unchanged path: a Function that binds nothing serializes today's
    // bytes, with no `secret_bindings` key at all.
    assert!(request.secret_bindings.is_empty());
    assert_eq!(
        request.to_canonical_json().expect("canonical request"),
        fixture("remote_function_registration_request.canonical.json").trim()
    );

    let value: Value =
        serde_json::from_str(&request.to_canonical_json().expect("canonical request"))
            .expect("request JSON");
    assert_no_secret_values(&value);
}

#[test]
fn execution_controls_match_shared_canonical_golden() {
    let request = FunctionRegistrationRequest::from_json(&fixture(
        "remote_function_controlled_registration_request.json",
    ))
    .expect("controlled registration request");
    assert_eq!(
        request.to_canonical_json().expect("canonical request"),
        fixture("remote_function_controlled_registration_request.canonical.json").trim()
    );

    let value: Value = serde_json::from_str(&request.to_canonical_json().unwrap()).unwrap();
    assert_eq!(value["batch_size"], 2);
    assert_eq!(value["row_timeout_seconds"], 0.75);
}

#[test]
fn execution_controls_require_their_positive_ranges() {
    let source = fixture("remote_function_registration_request.json");
    for (field, value) in [
        ("batch_size", Value::from(0)),
        ("batch_size", Value::from(4_294_967_296_u64)),
        ("row_timeout_seconds", Value::from(0)),
        ("row_timeout_seconds", Value::from(-0.25)),
    ] {
        let mut request: Value = serde_json::from_str(&source).unwrap();
        request[field] = value;
        let error = FunctionRegistrationRequest::from_json(&request.to_string()).unwrap_err();
        assert!(
            error.to_string().contains(field),
            "{field} validation error was {error}"
        );
    }
}

#[test]
fn execution_controls_are_validated_before_canonical_serialization() {
    let source = fixture("remote_function_controlled_registration_request.json");
    for timeout in [f64::NAN, f64::INFINITY, -0.25] {
        let mut request = FunctionRegistrationRequest::from_json(&source).unwrap();
        request.row_timeout_seconds = Some(timeout);
        let error = request.to_canonical_json().unwrap_err();
        assert!(error.to_string().contains("row_timeout_seconds"), "{error}");
    }

    let mut request = FunctionRegistrationRequest::from_json(&source).unwrap();
    request.batch_size = Some(0);
    let error = request.to_canonical_json().unwrap_err();
    assert!(error.to_string().contains("batch_size"), "{error}");
}

/// The same shared golden as the Python suite builds from `@function(secrets=...)`
/// plus `bind_secrets`, so both clients agree byte for byte on a bound request.
#[test]
fn secret_bound_registration_request_matches_shared_canonical_golden() {
    let request = FunctionRegistrationRequest::from_json(&fixture(
        "remote_function_secret_registration_request.json",
    ))
    .expect("registration request");
    assert_eq!(request.name, "analyze_caption");
    assert_eq!(
        request.secret_bindings,
        [SecretBinding::Env {
            variable: "OPENAI_API_KEY".to_string(),
            secret_ref: SecretReference::new("openai-prod"),
        }]
    );
    assert_eq!(
        request.to_canonical_json().expect("canonical request"),
        fixture("remote_function_secret_registration_request.canonical.json").trim()
    );

    let value: Value =
        serde_json::from_str(&request.to_canonical_json().expect("canonical request"))
            .expect("request JSON");
    assert_no_secret_values(&value);
}

#[tokio::test]
async fn local_function_catalog_operations_return_stable_not_supported() {
    let directory = tempfile::tempdir().unwrap();
    let connection = lancedb::connect(directory.path().to_str().unwrap())
        .execute()
        .await
        .unwrap();
    let request = FunctionRegistrationRequest::from_json(&fixture(
        "remote_function_registration_request.json",
    ))
    .unwrap();

    let create_error = connection
        .create_function_async(request, &[])
        .await
        .unwrap_err();
    let lookup_error = connection
        .get_function("normalize_score", "1", &[])
        .await
        .unwrap_err();
    let latest_lookup_error = connection
        .get_function_latest("normalize_score", &[])
        .await
        .unwrap_err();
    let drop_error = connection
        .drop_function("normalize_score", "1", &[])
        .await
        .unwrap_err();
    for error in [create_error, lookup_error, latest_lookup_error, drop_error] {
        assert!(matches!(
            error,
            Error::NotSupported { message }
                if message.starts_with("Function catalog operations are not supported by this database")
                    && message.contains("utm_campaign=functions")
        ));
    }
}
