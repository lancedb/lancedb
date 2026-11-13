// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Compaction options supported by the remote SQL submission protocol.
use crate::error::{Error, Result};

pub(super) fn compaction_sql_options(
    options: &crate::table::optimize::CompactionOverrides,
) -> Result<Vec<String>> {
    for (name, value) in [
        ("target_rows_per_fragment", options.target_rows_per_fragment),
        ("max_source_rows", options.max_source_rows),
        ("batch_size", options.batch_size),
    ] {
        if value == Some(0) {
            return Err(Error::InvalidInput {
                message: format!("{name} must be greater than 0"),
            });
        }
    }
    let mut result = Vec::new();
    for (name, value) in [
        (
            "target_rows_per_fragment",
            options.target_rows_per_fragment.map(|v| v as u64),
        ),
        ("max_source_rows", options.max_source_rows.map(|v| v as u64)),
        ("io_buffer_size", options.io_buffer_size),
        ("batch_size", options.batch_size.map(|v| v as u64)),
    ] {
        if let Some(value) = value {
            result.push(format!("{name} = {value}"));
        }
    }
    Ok(result)
}

const UNSUPPORTED_OPTIMIZE: &str = "Compaction on an Enterprise table requires support for OPTIMIZE <table> REWRITE DATA. Upgrade LanceDB Enterprise to a compatible release.";
const UNSUPPORTED_OPTIONS: &str = "The requested compaction options are not supported by this version of LanceDB Enterprise. Upgrade to a compatible release; requested options will not be removed or retried.";

/// The client generates the whole statement, so parser errors indicate a
/// client/server syntax mismatch. Match option errors without enumerating names.
fn unsupported_optimize_message(message: &str) -> Option<&'static str> {
    let message = message.to_ascii_lowercase();
    if message.contains("parsererror") {
        Some(UNSUPPORTED_OPTIMIZE)
    } else if message.contains("unknown optimize option") {
        Some(UNSUPPORTED_OPTIONS)
    } else {
        None
    }
}

/// Apply compatibility handling only to this constructed operation.
/// Preserve every unrelated SQL/authentication/transport error unchanged.
pub(super) fn map_optimize_error(error: Error) -> Error {
    fn unsupported(error: &Error) -> Option<(&Error, &'static str)> {
        match error {
            Error::Sql {
                message, grpc_code, ..
            } if *grpc_code == tonic::Code::InvalidArgument as i32 => {
                unsupported_optimize_message(message).map(|reason| (error, reason))
            }
            Error::JobFailed { failure, .. } => failure
                .source
                .as_deref()
                .and_then(|source| unsupported(source)),
            _ => None,
        }
    }
    if let Some((source, reason)) = unsupported(&error) {
        let diagnostic = match &error {
            Error::Sql { .. } => error.to_string(),
            // JobFailure's Display omits its source, including the SQL request ID.
            _ => format!("{error}; {source}"),
        };
        Error::NotSupported {
            message: format!("{reason} Server diagnostic: {diagnostic}"),
        }
    } else {
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::JobFailure;
    use crate::table::{CompactionOptions, CompactionOverrides};
    use std::sync::Arc;

    fn sql_error(code: tonic::Code, message: &str) -> Error {
        Error::Sql {
            message: message.into(),
            request_id: "optimize-request".into(),
            grpc_code: code as i32,
            metadata: Default::default(),
        }
    }

    #[test]
    fn direct_optimize_error_prints_its_diagnostic_once() {
        let diagnostic = "ParserError: Expected: end of statement, found: REWRITE";
        let Error::NotSupported { message } =
            map_optimize_error(sql_error(tonic::Code::InvalidArgument, diagnostic))
        else {
            panic!("known optimize syntax must be unsupported");
        };
        assert_eq!(message.matches(diagnostic).count(), 1);
        assert_eq!(message.matches("request_id=optimize-request").count(), 1);
    }

    #[test]
    fn wrapped_optimize_error_keeps_job_context_and_sql_diagnostic() {
        let diagnostic = "ParserError: Expected: end of statement, found: REWRITE";
        let error = Error::JobFailed {
            job_id: Some("optimize-job".into()),
            failure: Box::new(JobFailure {
                phase: Some("planning".into()),
                message: Some("SQL planning failed".into()),
                source: Some(Arc::new(sql_error(
                    tonic::Code::InvalidArgument,
                    diagnostic,
                ))),
                ..Default::default()
            }),
        };
        let Error::NotSupported { message } = map_optimize_error(error) else {
            panic!("wrapped unsupported optimize SQL must be translated");
        };
        assert!(message.contains(UNSUPPORTED_OPTIMIZE));
        assert!(message.contains("optimize-job"));
        assert!(message.contains("SQL planning failed (in planning)"));
        assert_eq!(message.matches(diagnostic).count(), 1);
        assert_eq!(message.matches("request_id=optimize-request").count(), 1);
    }

    #[test]
    fn unrelated_wrapped_sql_errors_pass_through_unchanged() {
        let unsupported = "ParserError: Expected: end of statement, found: REWRITE";
        for (code, diagnostic) in [
            (tonic::Code::PermissionDenied, unsupported),
            (tonic::Code::Unavailable, unsupported),
            (tonic::Code::InvalidArgument, "invalid table configuration"),
        ] {
            let source = Arc::new(sql_error(code, diagnostic));
            let error = Error::JobFailed {
                job_id: Some("optimize-job".into()),
                failure: Box::new(JobFailure {
                    phase: Some("planning".into()),
                    message: Some("SQL planning failed".into()),
                    sql_job_id: Some("sql-job".into()),
                    retryable: Some(false),
                    source: Some(source.clone()),
                    ..Default::default()
                }),
            };
            let Error::JobFailed { job_id, failure } = map_optimize_error(error) else {
                panic!("{code:?}: unrelated SQL failure must keep its classification");
            };
            assert_eq!(job_id.as_deref(), Some("optimize-job"));
            assert_eq!(failure.phase.as_deref(), Some("planning"));
            assert_eq!(failure.message.as_deref(), Some("SQL planning failed"));
            assert_eq!(failure.sql_job_id.as_deref(), Some("sql-job"));
            assert_eq!(failure.retryable, Some(false));
            assert!(Arc::ptr_eq(failure.source.as_ref().unwrap(), &source));
        }
    }

    #[test]
    fn generated_optimize_syntax_errors_indicate_incompatible_servers() {
        for diagnostic in [
            "ParserError: Expected: end of statement, found: REWRITE",
            "ParserError: Expected: identifier",
            "SQL error: ParserError: future grammar diagnostic",
            "parsererror: lowercase diagnostic",
        ] {
            assert_eq!(
                unsupported_optimize_message(diagnostic),
                Some(UNSUPPORTED_OPTIMIZE)
            );
        }
        for diagnostic in [
            "Unknown OPTIMIZE option 'io_buffer_size'",
            "Error during planning: Unknown OPTIMIZE option 'future_option'",
        ] {
            assert_eq!(
                unsupported_optimize_message(diagnostic),
                Some(UNSUPPORTED_OPTIONS)
            );
        }
        for diagnostic in [
            "permission denied",
            "table not found",
            "connection reset",
            "invalid table configuration",
        ] {
            assert!(
                unsupported_optimize_message(diagnostic).is_none(),
                "{diagnostic}"
            );
        }
    }

    #[test]
    fn omissions_and_explicit_default_target_are_distinct() {
        assert!(
            compaction_sql_options(&CompactionOverrides::default())
                .unwrap()
                .is_empty()
        );
        let target = CompactionOptions::default().target_rows_per_fragment;
        assert_eq!(
            compaction_sql_options(&CompactionOverrides {
                target_rows_per_fragment: Some(target),
                io_buffer_size: Some(0),
                ..Default::default()
            })
            .unwrap(),
            [
                format!("target_rows_per_fragment = {target}"),
                "io_buffer_size = 0".into()
            ]
        );
    }

    #[test]
    fn unsupported_settings_are_rejected() {
        assert!(matches!(
            CompactionOverrides::try_from(CompactionOptions {
                max_source_bytes: Some(1024),
                ..Default::default()
            }),
            Err(Error::NotSupported { .. })
        ));
    }

    #[test]
    fn invalid_row_settings_are_rejected() {
        for options in [
            CompactionOverrides {
                target_rows_per_fragment: Some(0),
                ..Default::default()
            },
            CompactionOverrides {
                max_source_rows: Some(0),
                ..Default::default()
            },
            CompactionOverrides {
                batch_size: Some(0),
                ..Default::default()
            },
        ] {
            assert!(matches!(
                compaction_sql_options(&options),
                Err(Error::InvalidInput { .. })
            ));
        }
    }
}
