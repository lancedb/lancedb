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

/// Apply legacy compatibility handling only to this constructed operation.
/// Preserve every unrelated SQL/authentication/transport error unchanged.
pub(super) fn map_legacy_optimize_error(error: Error, has_options: bool) -> Error {
    use crate::remote::optimize_capabilities::legacy_optimize_error_message;
    fn unsupported(error: &Error, has_options: bool) -> Option<(&Error, &'static str)> {
        match error {
            Error::Sql {
                message, grpc_code, ..
            } if *grpc_code == tonic::Code::InvalidArgument as i32 => {
                legacy_optimize_error_message(message, has_options).map(|reason| (error, reason))
            }
            Error::JobFailed { failure, .. } => failure
                .source
                .as_deref()
                .and_then(|source| unsupported(source, has_options)),
            _ => None,
        }
    }
    if let Some((source, reason)) = unsupported(&error, has_options) {
        Error::NotSupported {
            message: format!("{reason} Server diagnostic: {error}; {source}"),
        }
    } else {
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{CompactionOptions, CompactionOverrides};

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
