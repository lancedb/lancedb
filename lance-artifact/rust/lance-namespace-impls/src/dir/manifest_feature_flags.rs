// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Reader/writer feature flags for the directory-catalog `__manifest` dataset.
//!
//! Forward-compatibility infrastructure for the `__manifest` Lance dataset,
//! analogous to the Lance table format's `reader_feature_flags` /
//! `writer_feature_flags` but describing the *catalog manifest* format (schema
//! and semantics) rather than the underlying Lance file format. The flags are
//! persisted in the `__manifest` dataset's `table_metadata` map.
//!
//! Each manifest feature owns one bit in a `u64` bitmask. A build may read a
//! `__manifest` only if it understands every set reader-flag bit, and may write
//! it only if it understands every set writer-flag bit; otherwise it fails fast
//! with a clear "please upgrade" error instead of silently misreading data. The
//! set of bits a build understands is `READER_KNOWN_FLAGS` / `WRITER_KNOWN_FLAGS`.
//!
//! This is the mechanism only: no manifest feature is defined yet, so the known
//! masks are `0` and nothing is ever set — every current manifest reads and
//! writes unchanged. The first format change that needs forward-compatibility
//! protection adds its bit to the known masks and stamps it on write; from then
//! on, builds without that bit refuse the new format rather than misreading it.
//! Manifests written before this mechanism carry no flag keys, which parse as
//! `0` and stay compatible with every build.

use std::collections::HashMap;

use lance_core::{Error, Result};
use lance_namespace::error::NamespaceError;

/// `table_metadata` key holding the reader feature-flag bitmask (decimal `u64`).
pub const READER_FEATURE_FLAGS_KEY: &str = "lance.namespace.manifest.reader_feature_flags";
/// `table_metadata` key holding the writer feature-flag bitmask (decimal `u64`).
pub const WRITER_FEATURE_FLAGS_KEY: &str = "lance.namespace.manifest.writer_feature_flags";

/// Reader feature-flag bits this build understands. No manifest feature is
/// defined yet, so this build understands none and refuses any non-zero reader
/// flag. A future format change adds its bit here.
const READER_KNOWN_FLAGS: u64 = 0;
/// Writer feature-flag bits this build understands.
const WRITER_KNOWN_FLAGS: u64 = 0;

/// Whether this build can read a `__manifest` whose persisted reader feature
/// flags are `reader_flags` — i.e. it understands every set bit.
pub fn can_read_manifest(reader_flags: u64) -> bool {
    (reader_flags & !READER_KNOWN_FLAGS) == 0
}

/// Whether this build can write a `__manifest` whose persisted writer feature
/// flags are `writer_flags` — i.e. it understands every set bit.
pub fn can_write_manifest(writer_flags: u64) -> bool {
    (writer_flags & !WRITER_KNOWN_FLAGS) == 0
}

fn parse_flags(table_metadata: &HashMap<String, String>, key: &str) -> Result<u64> {
    match table_metadata.get(key) {
        None => Ok(0),
        Some(raw) => raw.parse::<u64>().map_err(|e| {
            Error::from(NamespaceError::Unsupported {
                message: format!(
                    "The __manifest dataset has an unparsable feature-flag value '{raw}' for \
                     '{key}': {e}. This likely means it was written by a newer, incompatible \
                     version of Lance; please upgrade Lance to use this catalog."
                ),
            })
        }),
    }
}

/// Reader feature flags persisted in the `__manifest` `table_metadata` (`0` if absent).
pub fn reader_flags(table_metadata: &HashMap<String, String>) -> Result<u64> {
    parse_flags(table_metadata, READER_FEATURE_FLAGS_KEY)
}

/// Writer feature flags persisted in the `__manifest` `table_metadata` (`0` if absent).
pub fn writer_flags(table_metadata: &HashMap<String, String>) -> Result<u64> {
    parse_flags(table_metadata, WRITER_FEATURE_FLAGS_KEY)
}

/// Validate that this build can READ the `__manifest` described by `table_metadata`,
/// returning a clear "please upgrade" error otherwise.
pub fn ensure_readable(table_metadata: &HashMap<String, String>) -> Result<()> {
    let flags = reader_flags(table_metadata)?;
    if !can_read_manifest(flags) {
        return Err(Error::from(NamespaceError::Unsupported {
            message: format!(
                "The __manifest dataset was written with reader feature flags {flags}, which this \
                 version of Lance does not understand (known reader flags: {READER_KNOWN_FLAGS}). \
                 Please upgrade Lance to read this catalog."
            ),
        }));
    }
    Ok(())
}

/// Validate that this build can WRITE the `__manifest` described by `table_metadata`,
/// returning a clear "please upgrade" error otherwise.
pub fn ensure_writable(table_metadata: &HashMap<String, String>) -> Result<()> {
    let flags = writer_flags(table_metadata)?;
    if !can_write_manifest(flags) {
        return Err(Error::from(NamespaceError::Unsupported {
            message: format!(
                "The __manifest dataset was written with writer feature flags {flags}, which this \
                 version of Lance does not understand (known writer flags: {WRITER_KNOWN_FLAGS}). \
                 Please upgrade Lance to modify this catalog."
            ),
        }));
    }
    Ok(())
}

/// Whether `err` indicates the `__manifest` is in a format this build cannot
/// handle — i.e. it carries an unknown reader/writer feature flag, surfaced by
/// [`ensure_readable`] / [`ensure_writable`] as a [`NamespaceError::Unsupported`].
///
/// Catalog initialization uses this to refuse opening such a manifest rather
/// than silently degrading to a directory-listing view that ignores it. The
/// `__manifest` open path raises no other `Unsupported` error, so matching the
/// code is sufficient and avoids brittle message matching.
pub fn is_incompatible_manifest_error(err: &Error) -> bool {
    matches!(
        err,
        Error::Namespace { source, .. }
            if source
                .downcast_ref::<NamespaceError>()
                .is_some_and(|e| matches!(e, NamespaceError::Unsupported { .. }))
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn unflagged_is_compatible() {
        assert!(can_read_manifest(0));
        assert!(can_write_manifest(0));
        let empty = HashMap::new();
        assert!(ensure_readable(&empty).is_ok());
        assert!(ensure_writable(&empty).is_ok());
        assert_eq!(reader_flags(&empty).unwrap(), 0);
        assert_eq!(writer_flags(&empty).unwrap(), 0);
        // Explicit zeroes are also compatible.
        let zeroed = meta(&[
            (READER_FEATURE_FLAGS_KEY, "0"),
            (WRITER_FEATURE_FLAGS_KEY, "0"),
        ]);
        assert!(ensure_readable(&zeroed).is_ok());
        assert!(ensure_writable(&zeroed).is_ok());
    }

    #[test]
    fn any_unknown_flag_is_refused() {
        // This build understands no feature flags, so any non-zero bit is refused.
        assert!(!can_read_manifest(1));
        assert!(!can_write_manifest(1));
        assert!(!can_read_manifest(1 << 30));
        assert!(!can_write_manifest(1 << 63));

        let reader = meta(&[(READER_FEATURE_FLAGS_KEY, "1")]);
        let err = ensure_readable(&reader).unwrap_err();
        assert!(err.to_string().to_lowercase().contains("upgrade"));
        assert!(is_incompatible_manifest_error(&err));
        // A reader flag does not block writers that the writer mask allows.
        assert!(ensure_writable(&reader).is_ok());

        let writer = meta(&[(WRITER_FEATURE_FLAGS_KEY, "2")]);
        let err = ensure_writable(&writer).unwrap_err();
        assert!(err.to_string().to_lowercase().contains("upgrade"));
        assert!(is_incompatible_manifest_error(&err));
    }

    #[test]
    fn unparsable_value_is_refused() {
        let m = meta(&[(READER_FEATURE_FLAGS_KEY, "not-a-number")]);
        assert!(reader_flags(&m).is_err());
        assert!(ensure_readable(&m).is_err());
    }

    #[test]
    fn unrelated_error_is_not_an_incompatibility() {
        let other = Error::from(NamespaceError::TableNotFound {
            message: "x".to_string(),
        });
        assert!(!is_incompatible_manifest_error(&other));
    }
}
