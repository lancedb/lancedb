// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Public contract tests for first-class Function error codes (FF-006).
//!
//! These tests pin the stable `FunctionErrorCode` wire strings, direct
//! `Error::Function` projection, and optional `JobFailure.error_code`.
//! They intentionally fail to compile until that public API exists.
//!
//! Categories are judged only by structural enum matching / equality, never
//! by parsing diagnostic message text.

use lancedb::error::FunctionErrorCode;
use lancedb::{Error, JobFailure};
use serde_json::{Value, json};

/// Exact stable wire strings for the eight known Function error categories.
const KNOWN_WIRE_CODES: &[(&str, FunctionErrorCode)] = &[
    (
        "definition_validation_failure",
        FunctionErrorCode::DefinitionValidationFailure,
    ),
    (
        "name_or_function_not_found",
        FunctionErrorCode::NameOrFunctionNotFound,
    ),
    ("name_conflict", FunctionErrorCode::NameConflict),
    (
        "unsupported_runtime_or_capability",
        FunctionErrorCode::UnsupportedRuntimeOrCapability,
    ),
    ("revoked_function", FunctionErrorCode::RevokedFunction),
    (
        "udf_execution_failure",
        FunctionErrorCode::UdfExecutionFailure,
    ),
    (
        "generated_column_incomplete",
        FunctionErrorCode::GeneratedColumnIncomplete,
    ),
    (
        "stale_or_conflicting_input",
        FunctionErrorCode::StaleOrConflictingInput,
    ),
];

fn assert_known_variant(code: &FunctionErrorCode, expected: &FunctionErrorCode) {
    assert_eq!(
        code, expected,
        "FunctionErrorCode must match structurally; got {code:?}, expected {expected:?}"
    );
    assert!(
        !matches!(code, FunctionErrorCode::Unrecognized(_)),
        "known wire string must not deserialize as Unrecognized: {code:?}"
    );
}

#[test]
fn function_error_code_known_variants_use_exact_stable_json_strings() {
    for (wire, expected) in KNOWN_WIRE_CODES {
        let encoded = serde_json::to_value(expected).expect("serialize FunctionErrorCode");
        assert_eq!(
            encoded,
            Value::String((*wire).to_string()),
            "stable JSON string for {expected:?}"
        );

        let decoded: FunctionErrorCode = serde_json::from_value(Value::String((*wire).to_string()))
            .unwrap_or_else(|e| panic!("deserialize `{wire}`: {e}"));
        assert_known_variant(&decoded, expected);

        let round_trip = serde_json::to_value(&decoded).expect("re-serialize");
        assert_eq!(round_trip, Value::String((*wire).to_string()));
    }
}

#[test]
fn unrecognized_error_code_preserves_exact_string_and_does_not_become_known() {
    let raw = "enterprise_future_category_xyz";
    let decoded: FunctionErrorCode = serde_json::from_value(json!(raw))
        .unwrap_or_else(|e| panic!("unknown code must deserialize, not fail: {e}"));

    match &decoded {
        FunctionErrorCode::Unrecognized(preserved) => {
            assert_eq!(preserved, raw, "unknown code must be preserved verbatim");
        }
        other => panic!("expected FunctionErrorCode::Unrecognized, got {other:?}"),
    }

    for (_, known) in KNOWN_WIRE_CODES {
        assert_ne!(
            &decoded, known,
            "unrecognized code must not equal known variant {known:?}"
        );
    }

    let encoded = serde_json::to_value(&decoded).expect("serialize Unrecognized");
    assert_eq!(encoded, json!(raw));

    let again: FunctionErrorCode =
        serde_json::from_value(encoded).expect("Unrecognized must round-trip");
    match again {
        FunctionErrorCode::Unrecognized(preserved) => assert_eq!(preserved, raw),
        other => panic!("round-trip must stay Unrecognized, got {other:?}"),
    }
}

#[test]
fn error_function_carries_code_plus_diagnostic_message() {
    let err = Error::Function {
        code: FunctionErrorCode::NameConflict,
        message: "sanitized diagnostic only".to_string(),
    };

    match err {
        Error::Function { code, message } => {
            assert_known_variant(&code, &FunctionErrorCode::NameConflict);
            assert_eq!(message, "sanitized diagnostic only");
        }
        other => panic!("expected Error::Function, got {other:?}"),
    }
}

#[test]
fn error_function_category_is_the_code_field_not_the_message() {
    // Message text deliberately names a different category; structural code wins.
    let err = Error::Function {
        code: FunctionErrorCode::GeneratedColumnIncomplete,
        message: "looks like udf_execution_failure to a string parser".to_string(),
    };

    match err {
        Error::Function { code, .. } => {
            assert_known_variant(&code, &FunctionErrorCode::GeneratedColumnIncomplete);
            assert_ne!(code, FunctionErrorCode::UdfExecutionFailure);
        }
        other => panic!("expected Error::Function, got {other:?}"),
    }
}

#[test]
fn job_failure_has_optional_error_code() {
    let with_code = JobFailure {
        error_code: Some(FunctionErrorCode::RevokedFunction),
        phase: Some("execute".to_string()),
        message: Some("revoked".to_string()),
        retryable: Some(false),
        source: None,
    };
    match &with_code.error_code {
        Some(code) => assert_known_variant(code, &FunctionErrorCode::RevokedFunction),
        None => panic!("error_code must be present when set"),
    }

    let without_code = JobFailure {
        phase: Some("execute".to_string()),
        message: Some("older backend failure without a category".to_string()),
        retryable: Some(true),
        ..Default::default()
    };
    assert!(
        without_code.error_code.is_none(),
        "missing error_code must stay None; diagnostics must not invent a category"
    );
}

#[test]
fn job_failure_diagnostics_do_not_overwrite_error_code() {
    let failure = JobFailure {
        error_code: Some(FunctionErrorCode::StaleOrConflictingInput),
        phase: Some("commit".to_string()),
        message: Some("definition_validation_failure in worker logs".to_string()),
        retryable: Some(true),
        source: None,
    };

    match &failure.error_code {
        Some(code) => {
            assert_known_variant(code, &FunctionErrorCode::StaleOrConflictingInput);
            assert_ne!(code, &FunctionErrorCode::DefinitionValidationFailure);
        }
        None => panic!("explicit error_code must remain set"),
    }
    assert_eq!(failure.phase.as_deref(), Some("commit"));
    assert_eq!(failure.retryable, Some(true));
}
