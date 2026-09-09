// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::fs;
use std::path::PathBuf;

use lancedb::Error;
use lancedb::function::{
    FunctionRegistrationRequest, MAX_FUNCTION_SECRET_ENV_BINDINGS, MAX_SECRET_VALUE_BYTES,
};
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
    // bytes, with no `secret_env_bindings` key at all.
    assert!(request.secret_env_bindings.is_empty());
    assert_eq!(
        request.to_canonical_json().expect("canonical request"),
        fixture("remote_function_registration_request.canonical.json").trim()
    );

    let value: Value =
        serde_json::from_str(&request.to_canonical_json().expect("canonical request"))
            .expect("request JSON");
    assert_no_secret_values(&value);
}

/// The same shared golden as the Python suite builds from `@udf(secrets=...)`
/// plus `bind_secrets`, so both clients agree byte for byte on a bound request.
#[test]
fn secret_bound_registration_request_matches_shared_canonical_golden() {
    let request = FunctionRegistrationRequest::from_json(&fixture(
        "remote_function_secret_registration_request.json",
    ))
    .expect("registration request");
    assert_eq!(request.name, "analyze_caption");
    assert_eq!(
        request.secret_env_bindings,
        std::collections::BTreeMap::from([(
            "OPENAI_API_KEY".to_string(),
            "openai-prod".to_string()
        )])
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

    let create_error = connection.create_function_async(request).await.unwrap_err();
    let lookup_error = connection
        .get_function("normalize_score", "fv_exact")
        .await
        .unwrap_err();
    let drop_error = connection
        .drop_function("normalize_score", "fv_exact")
        .await
        .unwrap_err();
    for error in [create_error, lookup_error, drop_error] {
        assert!(matches!(
            error,
            Error::NotSupported { message }
                if message == "Function catalog operations are not supported by this database"
        ));
    }
}

/// The cap is enforced above the backend, so every database and every language
/// surface rejects the same envelope. A local connection would otherwise answer
/// `NotSupported` first, which is what makes it the honest probe here.
#[tokio::test]
async fn a_function_binds_at_most_sixteen_secrets() {
    let directory = tempfile::tempdir().unwrap();
    let connection = lancedb::connect(directory.path().to_str().unwrap())
        .execute()
        .await
        .unwrap();
    let mut request = FunctionRegistrationRequest::from_json(&fixture(
        "remote_function_registration_request.json",
    ))
    .unwrap();
    request.secret_env_bindings = (0..=MAX_FUNCTION_SECRET_ENV_BINDINGS)
        .map(|index| (format!("TOKEN_{index}"), format!("secret-{index}")))
        .collect();

    let error = connection.create_function_async(request).await.unwrap_err();
    assert!(matches!(
        error,
        Error::InvalidInput { message } if message.contains("at most 16 secrets")
    ));
}

/// The binding contract is enforced above the backend in full, not just its
/// count: a caller that skips a language binding still cannot register a name
/// the runtime could not deliver.
#[tokio::test]
async fn binding_names_are_validated_before_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let connection = lancedb::connect(directory.path().to_str().unwrap())
        .execute()
        .await
        .unwrap();

    let mut invalid_name = FunctionRegistrationRequest::from_json(&fixture(
        "remote_function_registration_request.json",
    ))
    .unwrap();
    invalid_name.secret_env_bindings = [("BAD=NAME".to_string(), "openai-prod".to_string())].into();
    let error = connection
        .create_function_async(invalid_name)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::InvalidInput { message } if message.contains("portable")
    ));

    // `env` is readable wherever the Function's record is; a bound Secret is
    // not. The same name cannot mean both.
    let mut overlapping = FunctionRegistrationRequest::from_json(&fixture(
        "remote_function_registration_request.json",
    ))
    .unwrap();
    let bound = overlapping
        .runtime
        .env()
        .and_then(|env| env.keys().next().cloned())
        .expect("fixture runtime declares env");
    overlapping.secret_env_bindings = [(bound.clone(), "openai-prod".to_string())].into();
    let error = connection
        .create_function_async(overlapping)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::InvalidInput { message } if message.contains("already set by runtime.env")
    ));
}

/// An oversized credential is refused before a body is built, so it is never
/// serialized or uploaded to be refused by the service instead.
#[tokio::test]
async fn an_oversized_secret_value_is_refused_before_the_wire() {
    let directory = tempfile::tempdir().unwrap();
    let connection = lancedb::connect(directory.path().to_str().unwrap())
        .execute()
        .await
        .unwrap();

    for value in ["", &"x".repeat(MAX_SECRET_VALUE_BYTES + 1)] {
        let error = connection
            .create_secret("openai-prod", value)
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::InvalidInput { .. }),
            "expected InvalidInput, got {error:?}"
        );
    }

    // A local database refuses the verb outright, which is what proves the
    // size check ran ahead of the backend rather than instead of it.
    let error = connection
        .create_secret("openai-prod", "x".repeat(MAX_SECRET_VALUE_BYTES))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::NotSupported { .. }));
}
