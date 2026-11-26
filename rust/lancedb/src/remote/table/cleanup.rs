// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! SQL VACUUM submission; never replay an ambiguous acceptance response.

use crate::{Error, Result, table::CleanupOptions};
use arrow_array::{Array, RecordBatch, StringArray};

pub(super) fn statement(
    table: &str,
    branch: Option<&str>,
    options: &CleanupOptions,
) -> Result<String> {
    let mut settings = Vec::new();
    if let Some(branch) = branch {
        if branch.trim().is_empty() {
            return Err(Error::InvalidInput {
                message: "cleanup branch must not be empty".into(),
            });
        }
        settings.push(format!("branch = {}", super::quote_sql_string(branch)));
    }
    if let Some(age) = options.min_age_seconds {
        if i64::try_from(age)
            .ok()
            .and_then(chrono::Duration::try_seconds)
            .and_then(|duration| chrono::Utc::now().checked_sub_signed(duration))
            .is_none()
        {
            return Err(Error::InvalidInput {
                message: "min_age_seconds is out of range".into(),
            });
        }
        settings.push(format!("min_age_seconds = {age}"));
    }
    let mut sql = format!("VACUUM {}", super::quote_sql_identifier(table));
    if !settings.is_empty() {
        sql.push_str(&format!(" WITH ({})", settings.join(", ")));
    }
    Ok(sql)
}

pub(super) fn plan(batches: &[RecordBatch]) -> Result<crate::table::CleanupPlan> {
    let invalid = || {
        Error::Runtime { message: "EXPLAIN VACUUM did not return a valid versioned preview; a compatible EXPLAIN (FORMAT JSON) VACUUM server is required. No execution fallback was attempted".into() }
    };
    if batches.iter().map(RecordBatch::num_rows).sum::<usize>() != 1 {
        return Err(invalid());
    }
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() == 1)
        .ok_or_else(invalid)?;
    let kind = batch
        .column_by_name("plan_type")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(invalid)?;
    let payload = batch
        .column_by_name("plan")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(invalid)?;
    if kind.is_null(0) || kind.value(0) != "logical_plan" || payload.is_null(0) {
        return Err(invalid());
    }
    serde_json::from_str(payload.value(0)).map_err(|_| invalid())
}

pub(super) fn map_error(error: Error) -> Error {
    fn unsupported(error: &Error) -> Option<&Error> {
        match error {
            Error::Sql {
                message, grpc_code, ..
            } => {
                let message = message.to_ascii_lowercase();
                (*grpc_code == tonic::Code::Unimplemented as i32
                    || (*grpc_code == tonic::Code::Internal as i32
                        && message.contains("invalid explain format")
                        && message.contains("json"))
                    || (*grpc_code == tonic::Code::InvalidArgument as i32
                        && (message.contains("parsererror")
                            || message.contains("unknown vacuum option")
                            || message.contains("vacuum is not supported by this deployment"))))
                .then_some(error)
            }
            Error::JobFailed { failure, .. } => failure.source.as_deref().and_then(unsupported),
            _ => None,
        }
    }
    if let Some(source) = unsupported(&error) {
        let diagnostic = if matches!(error, Error::Sql { .. }) {
            error.to_string()
        } else {
            format!("{error}; {source}")
        };
        Error::NotSupported {
            message: format!(
                "Manual cleanup requires a compatible SQL VACUUM server; upgrade the server. No options were removed and no REST fallback was attempted. Server diagnostic: {diagnostic}"
            ),
        }
    } else {
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn wrapped_cleanup_compatibility_keeps_job_and_request_identity() {
        let sql = Error::Sql {
            message: "ParserError: found VACUUM".into(),
            request_id: "cleanup-request".into(),
            grpc_code: tonic::Code::InvalidArgument as i32,
            metadata: Default::default(),
        };
        let error = Error::JobFailed {
            job_id: Some("query-job".into()),
            failure: Box::new(crate::error::JobFailure {
                phase: Some("planning".into()),
                message: Some("statement failed".into()),
                source: Some(Arc::new(sql)),
                ..Default::default()
            }),
        };
        let mapped = map_error(error);
        assert!(matches!(mapped, Error::NotSupported { .. }));
        let diagnostic = mapped.to_string();
        assert!(diagnostic.contains("query-job"));
        assert!(diagnostic.contains("cleanup-request"));
    }

    #[test]
    fn cleanup_statement_preserves_inheritance_zero_and_quoting() {
        assert_eq!(
            statement("events", None, &CleanupOptions::new()).unwrap(),
            "VACUUM \"events\""
        );
        assert_eq!(
            statement(
                "a\"b",
                Some("o'brien"),
                &CleanupOptions::new().min_age_seconds(0)
            )
            .unwrap(),
            "VACUUM \"a\"\"b\" WITH (branch = 'o''brien', min_age_seconds = 0)"
        );
        assert!(statement("t", None, &CleanupOptions::new().min_age_seconds(u64::MAX)).is_err());
    }

    #[test]
    fn cleanup_compatibility_preserves_permissions_and_transport_errors() {
        for (code, diagnostic, expected) in [
            (
                tonic::Code::InvalidArgument,
                "ParserError: found VACUUM",
                true,
            ),
            (
                tonic::Code::InvalidArgument,
                "Unknown VACUUM option future",
                true,
            ),
            (tonic::Code::PermissionDenied, "ParserError: denied", false),
            (tonic::Code::Unavailable, "transport unavailable", false),
            (
                tonic::Code::InvalidArgument,
                "min_age_seconds is out of range",
                false,
            ),
        ] {
            let error = Error::Sql {
                message: diagnostic.into(),
                request_id: "cleanup-request".into(),
                grpc_code: code as i32,
                metadata: Default::default(),
            };
            let mapped = map_error(error);
            assert_eq!(matches!(mapped, Error::NotSupported { .. }), expected);
            assert!(mapped.to_string().contains("cleanup-request"));
        }
    }
}
