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

#[test]
fn function_version_job_result_matches_shared_canonical_golden() {
    let result = job_result("remote_function_job.json");
    let version = FunctionVersion::from_json(&result.to_string()).expect("FunctionVersion result");

    assert_eq!(version.name(), "embed");
    assert_eq!(version.version(), "fv_01K3EXACT");
    assert_eq!(version.runtime_digest(), "sha256:runtime");
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
    assert_eq!(binding.function().version, "fv_01K3TEXT");
    assert_eq!(binding.outputs()[0].output_ordinal, 0);
    assert_eq!(binding.outputs()[1].output_ordinal, 1);
    assert!(binding.input_schema().is_some());
    assert!(binding.output_schema().is_some());
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

    let result = RefreshColumnResult::from_json(&fixture(
        "remote_refresh_result_without_published_version.json",
    ))
    .expect("optional version");
    assert_eq!(result.published_version, None);
    assert_eq!(
        result
            .to_canonical_json()
            .expect("canonical result without version"),
        fixture("remote_refresh_result_without_published_version.canonical.json").trim()
    );
    assert_eq!(
        RefreshColumnResult::from_json(
            &result
                .to_canonical_json()
                .expect("canonical result without version")
        )
        .expect("round-trip result without version"),
        result
    );
}

#[test]
fn unknown_fields_and_discriminators_are_forward_decodable() {
    let mut result = job_result("remote_function_job.json");
    result["future_version_metadata"] = serde_json::json!({"retention_class": "catalog"});
    result["runtime"] = serde_json::json!({
        "kind": "wasm",
        "module_digest": "sha256:wasm"
    });
    result["signature"]["output"]["kind"] = Value::String("future_output_shape".to_string());

    let version = FunctionVersion::from_json(&result.to_string()).expect("future remote value");
    assert_eq!(version.runtime().kind(), "wasm");
    assert_eq!(version.runtime().python_version(), None);
    assert_eq!(version.signature().output.kind, "future_output_shape");
    assert_eq!(
        serde_json::from_str::<Value>(
            &version.to_canonical_json().expect("canonical future value")
        )
        .expect("canonical JSON")["runtime"],
        serde_json::json!({"kind": "wasm"})
    );
}

#[test]
fn floating_point_application_literals_are_rejected_consistently() {
    let error = FunctionApplication::from_json(&fixture("remote_function_application_float.json"))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("floating-point Function literals")
    );
}
