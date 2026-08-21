// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::fs;
use std::path::PathBuf;

use lancedb::function::{
    FunctionApplication, FunctionBinding, FunctionVersion, RefreshColumnResult,
};
use serde_json::Value;

fn fixture(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/first_class_functions/v1")
        .join(name);
    fs::read_to_string(path).expect("fixture must be readable")
}

fn job_result(name: &str) -> Value {
    serde_json::from_str::<Value>(&fixture(name)).expect("remote Job fixture")["result"].clone()
}

fn assert_no_secret_values(value: &Value) {
    match value {
        Value::Object(values) => {
            for (key, value) in values {
                assert!(
                    !matches!(
                        key.as_str(),
                        "secret_value" | "secret_values" | "resolved_secret" | "resolved_secrets"
                    ),
                    "client canonical value must not model resolved secret material"
                );
                assert_no_secret_values(value);
            }
        }
        Value::Array(values) => values.iter().for_each(assert_no_secret_values),
        _ => {}
    }
}

#[test]
fn function_version_job_result_matches_shared_canonical_golden() {
    let result = job_result("remote_function_job.json");
    let version = FunctionVersion::from_json(&result.to_string()).expect("FunctionVersion result");

    assert_eq!(version.name(), "embed");
    assert_eq!(version.version(), "fv_01K3EXACT");
    assert_eq!(version.runtime_digest(), "sha256:runtime");
    assert_eq!(version.required_secrets(), &["HF_TOKEN"]);
    assert_eq!(
        version.to_canonical_json().expect("canonical JSON"),
        fixture("remote_function_version.canonical.json").trim()
    );
}

#[test]
fn version_identity_is_immutable_and_exact() {
    let original = job_result("remote_function_job.json");
    let version =
        FunctionVersion::from_json(&original.to_string()).expect("FunctionVersion result");
    let reopened = version.clone();
    assert_eq!(reopened, version);
    assert_eq!(reopened.name(), version.name());
    assert_eq!(reopened.version(), version.version());

    let mut changed = original;
    changed["version"] = Value::String("fv_01K3DIFFERENT".to_string());
    let changed = FunctionVersion::from_json(&changed.to_string()).expect("changed version");
    assert_ne!(changed, version);
}

#[test]
fn application_and_binding_match_shared_remote_goldens() {
    let application = FunctionApplication::from_json(&fixture("remote_function_application.json"))
        .expect("application fixture");
    assert_eq!(application.function().version, "fv_01K3TEXT");
    assert_eq!(application.output().kind, "named_struct");
    assert_eq!(application.inputs().len(), 2);
    assert_eq!(
        application.to_canonical_json().expect("canonical JSON"),
        fixture("remote_function_application.canonical.json").trim()
    );

    let binding = FunctionBinding::from_json(&fixture("remote_function_binding.json"))
        .expect("binding fixture");
    assert_eq!(binding.revision(), 3);
    assert_eq!(binding.function().version, "fv_01K3TEXT");
    assert_eq!(binding.outputs()[0].output_ordinal, 0);
    assert_eq!(binding.outputs()[1].output_ordinal, 1);
    assert_eq!(
        binding.to_canonical_json().expect("canonical JSON"),
        fixture("remote_function_binding.canonical.json").trim()
    );
}

#[test]
fn refresh_job_result_matches_shared_canonical_golden() {
    let result = job_result("remote_refresh_job.json");
    let result = RefreshColumnResult::from_json(&result.to_string()).expect("refresh result");
    assert_eq!(result.rows_assigned, 999_998_800);
    assert_eq!(result.rows_filled(), result.rows_assigned);
    assert_eq!(result.version(), result.published_version);
    assert_eq!(
        result.to_canonical_json().expect("canonical JSON"),
        fixture("remote_refresh_result.canonical.json").trim()
    );
}

#[test]
fn unknown_fields_and_discriminators_are_forward_decodable() {
    let mut result = job_result("remote_function_job.json");
    result["future_version_metadata"] = serde_json::json!({"retention_class": "catalog"});
    result["runtime"]["kind"] = Value::String("future_python_runtime".to_string());
    result["runtime"]["environment"]["kind"] =
        Value::String("future_environment_source".to_string());
    result["signature"]["output"]["kind"] = Value::String("future_output_shape".to_string());

    let version = FunctionVersion::from_json(&result.to_string()).expect("future remote value");
    assert_eq!(version.runtime().kind, "future_python_runtime");
    assert_eq!(
        version.runtime().environment.kind,
        "future_environment_source"
    );
    assert_eq!(version.signature().output.kind, "future_output_shape");
}

#[test]
fn canonical_client_values_contain_secret_names_only() {
    let result = job_result("remote_function_job.json");
    let version = FunctionVersion::from_json(&result.to_string()).expect("FunctionVersion result");
    let canonical: Value = serde_json::from_str(
        &version
            .to_canonical_json()
            .expect("canonical FunctionVersion"),
    )
    .expect("canonical JSON");

    assert_eq!(
        canonical["required_secrets"],
        serde_json::json!(["HF_TOKEN"])
    );
    assert_no_secret_values(&canonical);
}
