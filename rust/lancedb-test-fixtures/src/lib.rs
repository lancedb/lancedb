// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Immutable test fixtures shared by LanceDB clients and downstream consumers.
//!
//! Fixture contents are committed verbatim. Parsing and interpreting a fixture
//! remains the consumer's responsibility.

/// Version 1 fixtures.
pub mod v1 {
    /// The exact bytes of the Function Arrow type golden.
    ///
    /// The golden pairs every supported declaration spelling with its
    /// canonical JSON Arrow type and lists declarations that must be rejected.
    ///
    /// ```
    /// let golden_json = lancedb_test_fixtures::v1::FUNCTION_ARROW_TYPES_JSON;
    /// assert!(golden_json.starts_with('{'));
    /// ```
    pub const FUNCTION_ARROW_TYPES_JSON: &str =
        include_str!("../fixtures/first_class_functions/v1/arrow_types.json");
}
