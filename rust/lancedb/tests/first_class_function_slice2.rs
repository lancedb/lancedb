// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::fs;
use std::path::PathBuf;

use lancedb::Error;
use lancedb::function::FunctionRegistrationRequest;

fn fixture(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/first_class_functions/v1")
        .join(name);
    fs::read_to_string(path).expect("fixture must be readable")
}

#[test]
fn registration_request_matches_shared_canonical_golden() {
    let request = FunctionRegistrationRequest::from_json(&fixture(
        "remote_function_registration_request.json",
    ))
    .expect("registration request");
    assert_eq!(request.name, "normalize_score");
    assert_eq!(request.artifact.adapter.kind, "scalar_to_arrow_batch");
    assert_eq!(
        request.to_canonical_json().expect("canonical request"),
        fixture("remote_function_registration_request.canonical.json").trim()
    );
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
    for error in [create_error, lookup_error] {
        assert!(matches!(
            error,
            Error::NotSupported { message }
                if message == "Function catalog operations are not supported by this database"
        ));
    }
}
