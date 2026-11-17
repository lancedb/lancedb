// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Shared golden data for downstream compatibility tests.
//!
//! Enable the `test-fixtures` feature to access these fixtures from another crate.

/// The version 1 Function Arrow type golden as UTF-8 JSON.
///
/// Contains supported declaration spellings, their canonical JSON Arrow types,
/// and declarations that must be rejected. The contents match the golden used by
/// LanceDB's Rust and Python tests; parsing is left to the consumer.
///
/// ```
/// let golden: serde_json::Value =
///     serde_json::from_str(lancedb::test_fixtures::FUNCTION_ARROW_TYPES_JSON).unwrap();
/// assert!(golden["valid"].is_array());
/// assert!(golden["invalid"].is_array());
/// ```
pub const FUNCTION_ARROW_TYPES_JSON: &str =
    include_str!("../tests/fixtures/first_class_functions/v1/arrow_types.json");
