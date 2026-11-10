// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Flight SQL metadata for remote compaction compatibility.
//!
//! Vendor SqlInfo IDs are part of the wire contract. Keep the companion SDK
//! and `optimize-capabilities.json` in sync. Missing values mean unknown.

use arrow_array::{Array, BooleanArray, RecordBatch, UInt32Array, UnionArray};

/// Supports canonical `OPTIMIZE <table> REWRITE DATA`, including branch targeting.
pub const OPTIMIZE_REWRITE_DATA: u32 = 10_000;
/// Supports target_rows_per_fragment, max_source_rows, io_buffer_size and batch_size.
pub const OPTIMIZE_INVOCATION_OPTIONS: u32 = 10_001;

pub const UNSUPPORTED_OPTIMIZE: &str = "Remote compaction requires an Enterprise server with OPTIMIZE <table> REWRITE DATA job submission enabled. Enable server-side job submission or upgrade to a compatible release.";
pub const UNSUPPORTED_OPTIONS: &str = "This Enterprise server does not support the requested compaction invocation options. Upgrade the server; options will not be removed or retried.";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OptimizeCapabilities {
    pub rewrite_data: Option<bool>,
    pub invocation_options: Option<bool>,
}

impl OptimizeCapabilities {
    pub fn from_batches(batches: &[RecordBatch]) -> Result<Self, String> {
        let mut result = Self::default();
        for batch in batches {
            let names = batch
                .column_by_name("info_name")
                .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
                .ok_or("invalid optimize SqlInfo: expected UInt32 info_name")?;
            let values = batch
                .column_by_name("value")
                .and_then(|column| column.as_any().downcast_ref::<UnionArray>())
                .ok_or("invalid optimize SqlInfo: expected union value")?;
            for row in 0..batch.num_rows() {
                if names.is_null(row) {
                    return Err("invalid optimize SqlInfo: null info_name".into());
                }
                let slot = match names.value(row) {
                    OPTIMIZE_REWRITE_DATA => &mut result.rewrite_data,
                    OPTIMIZE_INVOCATION_OPTIONS => &mut result.invocation_options,
                    _ => continue,
                };
                let child = values.child(values.type_id(row));
                let boolean = child
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or("invalid optimize SqlInfo: capability must be boolean")?;
                let offset = values.value_offset(row);
                if boolean.is_null(offset) || slot.is_some() {
                    return Err("invalid optimize SqlInfo: null or duplicate capability".into());
                }
                *slot = Some(boolean.value(offset));
            }
        }
        Ok(result)
    }

    pub fn check(&self, has_options: bool) -> Result<(), &'static str> {
        if self.rewrite_data == Some(false) {
            return Err(UNSUPPORTED_OPTIMIZE);
        }
        if has_options && self.invocation_options == Some(false) {
            return Err(UNSUPPORTED_OPTIONS);
        }
        Ok(())
    }
}

/// Historical servers cannot advertise support. Only recognize grammar errors
/// for statements the client constructs itself, never arbitrary SQL errors.
pub fn legacy_optimize_error_message(message: &str, has_options: bool) -> Option<&'static str> {
    let message = message.to_ascii_lowercase();
    let parser_error = message.contains("parsererror");
    if (parser_error
        && has_options
        && (message.contains("expected: end of statement, found: with")
            || message.contains("expected: ), found: ,")))
        || [
            "target_rows_per_fragment",
            "max_source_rows",
            "io_buffer_size",
            "batch_size",
        ]
        .iter()
        .any(|name| message.contains(&format!("unknown optimize option '{name}'")))
    {
        Some(UNSUPPORTED_OPTIONS)
    } else if parser_error
        && (message.contains("expected: end of statement, found: rewrite")
            || message.contains("expected: table, found:"))
    {
        Some(UNSUPPORTED_OPTIMIZE)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optimize_capability_wire_contract_and_validation() {
        use arrow_flight::sql::metadata::SqlInfoDataBuilder;
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("optimize-capabilities.json")).unwrap();
        assert_eq!(fixture[0]["info_name"], OPTIMIZE_REWRITE_DATA);
        assert_eq!(fixture[1]["info_name"], OPTIMIZE_INVOCATION_OPTIONS);
        let mut builder = SqlInfoDataBuilder::new();
        for row in fixture.as_array().unwrap() {
            builder.append(
                row["info_name"].as_u64().unwrap() as u32,
                row["value"].as_bool().unwrap(),
            );
        }
        let batch = builder.build().unwrap().record_batch(None).unwrap();
        assert_eq!(
            OptimizeCapabilities::from_batches(std::slice::from_ref(&batch)).unwrap(),
            OptimizeCapabilities {
                rewrite_data: Some(true),
                invocation_options: Some(true)
            }
        );
        assert!(OptimizeCapabilities::from_batches(&[batch.clone(), batch]).is_err());
        let mut malformed = SqlInfoDataBuilder::new();
        malformed.append(OPTIMIZE_REWRITE_DATA, "true");
        assert!(
            OptimizeCapabilities::from_batches(&[malformed
                .build()
                .unwrap()
                .record_batch(None)
                .unwrap()])
            .is_err()
        );
    }

    #[test]
    fn optimize_support_is_explicit_and_unknown_is_legacy_compatible() {
        assert!(OptimizeCapabilities::default().check(true).is_ok());
        let base_only = OptimizeCapabilities {
            rewrite_data: Some(true),
            invocation_options: Some(false),
        };
        assert!(base_only.check(false).is_ok());
        assert_eq!(base_only.check(true), Err(UNSUPPORTED_OPTIONS));
        assert_eq!(
            OptimizeCapabilities {
                rewrite_data: Some(false),
                invocation_options: Some(true)
            }
            .check(false),
            Err(UNSUPPORTED_OPTIMIZE)
        );
    }

    #[test]
    fn optimize_legacy_errors_do_not_classify_other_failures() {
        assert_eq!(
            legacy_optimize_error_message(
                "ParserError: Expected: end of statement, found: REWRITE",
                false
            ),
            Some(UNSUPPORTED_OPTIMIZE)
        );
        assert_eq!(
            legacy_optimize_error_message("ParserError: Expected: TABLE, found: events", false),
            Some(UNSUPPORTED_OPTIMIZE)
        );
        assert_eq!(
            legacy_optimize_error_message("Unknown OPTIMIZE option 'max_source_rows'", true),
            Some(UNSUPPORTED_OPTIONS)
        );
        assert_eq!(
            legacy_optimize_error_message("ParserError: Expected: ), found: ,", true),
            Some(UNSUPPORTED_OPTIONS)
        );
        assert!(
            legacy_optimize_error_message("ParserError: Expected: ), found: ,", false).is_none()
        );
        for message in [
            "permission denied",
            "table not found",
            "connection reset",
            "ParserError: Expected: identifier",
            "Unknown OPTIMIZE option 'future_option'",
        ] {
            assert!(
                legacy_optimize_error_message(message, true).is_none(),
                "{message}"
            );
        }
    }
}
