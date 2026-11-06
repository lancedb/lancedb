// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Compaction options supported by the remote SQL submission protocol.
use crate::error::{Error, Result};
use crate::table::CompactionOptions;

pub(super) fn compaction_sql_options(options: &CompactionOptions) -> Result<Vec<String>> {
    let defaults = CompactionOptions::default();
    // Reset only the supported fields. Comparing everything else also rejects
    // future non-default options until the SQL protocol can represent them.
    let mut remaining = options.clone();
    remaining.target_rows_per_fragment = defaults.target_rows_per_fragment;
    remaining.max_source_rows = defaults.max_source_rows;
    remaining.io_buffer_size = defaults.io_buffer_size;
    remaining.batch_size = defaults.batch_size;
    if remaining != defaults {
        return Err(Error::NotSupported {
            message: "remote CompactionOptions support only target_rows_per_fragment, max_source_rows, io_buffer_size, and batch_size; other options must retain their defaults".into(),
        });
    }
    for (name, value) in [
        (
            "target_rows_per_fragment",
            Some(options.target_rows_per_fragment as u64),
        ),
        ("max_source_rows", options.max_source_rows.map(|v| v as u64)),
        ("batch_size", options.batch_size.map(|v| v as u64)),
    ] {
        if value == Some(0) {
            return Err(Error::InvalidInput {
                message: format!("{name} must be greater than 0"),
            });
        }
    }
    let mut result = Vec::new();
    if options.target_rows_per_fragment != defaults.target_rows_per_fragment {
        result.push(format!(
            "target_rows_per_fragment = {}",
            options.target_rows_per_fragment
        ));
    }
    for (name, value) in [
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_omitted_and_zero_buffer_is_preserved() {
        assert!(
            compaction_sql_options(&CompactionOptions::default())
                .unwrap()
                .is_empty()
        );
        let options = CompactionOptions {
            target_rows_per_fragment: 100,
            max_source_rows: Some(500),
            io_buffer_size: Some(0),
            batch_size: Some(16),
            ..Default::default()
        };
        assert_eq!(
            compaction_sql_options(&options).unwrap(),
            [
                "target_rows_per_fragment = 100",
                "max_source_rows = 500",
                "io_buffer_size = 0",
                "batch_size = 16"
            ]
        );
    }

    #[test]
    fn unsupported_settings_are_rejected_even_with_supported_options() {
        let supported = CompactionOptions {
            max_source_rows: Some(500),
            ..Default::default()
        };
        for options in [
            CompactionOptions {
                max_source_bytes: Some(1024),
                ..supported.clone()
            },
            CompactionOptions {
                num_threads: Some(2),
                ..supported.clone()
            },
            CompactionOptions {
                defer_index_remap: true,
                ..supported.clone()
            },
            CompactionOptions {
                max_rows_per_group: 64,
                ..supported.clone()
            },
        ] {
            assert!(matches!(
                compaction_sql_options(&options),
                Err(Error::NotSupported { .. })
            ));
        }
    }

    #[test]
    fn invalid_row_settings_are_rejected() {
        for options in [
            CompactionOptions {
                target_rows_per_fragment: 0,
                ..Default::default()
            },
            CompactionOptions {
                max_source_rows: Some(0),
                ..Default::default()
            },
            CompactionOptions {
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
