// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use lance_arrow::DataTypeExt;
use lance_core::datatypes::Field;
use lance_core::deepsize::{Context, DeepSizeOf};
use lance_core::{Error, Result};

pub const LEGACY_FORMAT_VERSION: &str = "0.1";
pub const V2_FORMAT_2_0: &str = "2.0";
pub const V2_FORMAT_2_1: &str = "2.1";
pub const V2_FORMAT_2_2: &str = "2.2";
pub const V2_FORMAT_2_3: &str = "2.3";

/// A caller-facing Lance file-version request.
///
/// This selector remains separate from [`ConcreteFileVersion`] because `Stable`
/// and `Next` are release policy rather than persisted identities.
#[derive(Debug, Default, PartialEq, Eq, Clone, Copy, Ord, PartialOrd)]
pub enum LanceFileVersion {
    /// The legacy v1 format.
    Legacy,
    /// Exact v2.0.
    V2_0,
    /// Exact v2.1 and the current default.
    #[default]
    V2_1,
    /// The latest stable release.
    Stable,
    /// Exact v2.2.
    V2_2,
    /// The latest unstable release.
    Next,
    /// Exact v2.3.
    V2_3,
}

impl DeepSizeOf for LanceFileVersion {
    fn deep_size_of_children(&self, _context: &mut Context) -> usize {
        0
    }
}

impl LanceFileVersion {
    /// Resolve `Stable` or `Next` to the current exact selector.
    pub fn resolve(&self) -> Self {
        match self {
            Self::Stable => Self::default(),
            Self::Next => Self::V2_3,
            _ => *self,
        }
    }

    pub fn is_unstable(&self) -> bool {
        self >= &Self::Next
    }

    pub fn iter_non_legacy() -> impl Iterator<Item = Self> {
        [Self::V2_0, Self::V2_1, Self::V2_2, Self::V2_3].into_iter()
    }

    pub fn support_add_sub_column(&self) -> bool {
        self > &Self::V2_1
    }

    pub fn support_remove_sub_column(&self, field: &Field) -> bool {
        if self <= &Self::V2_1 {
            field.data_type().is_struct()
        } else {
            field.data_type().is_nested()
        }
    }
}

impl Display for LanceFileVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Legacy => LEGACY_FORMAT_VERSION,
            Self::V2_0 => V2_FORMAT_2_0,
            Self::V2_1 => V2_FORMAT_2_1,
            Self::V2_2 => V2_FORMAT_2_2,
            Self::V2_3 => V2_FORMAT_2_3,
            Self::Stable => "stable",
            Self::Next => "next",
        })
    }
}

impl FromStr for LanceFileVersion {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.to_lowercase().as_str() {
            LEGACY_FORMAT_VERSION | "legacy" => Ok(Self::Legacy),
            V2_FORMAT_2_0 | "0.3" => Ok(Self::V2_0),
            V2_FORMAT_2_1 => Ok(Self::V2_1),
            V2_FORMAT_2_2 => Ok(Self::V2_2),
            V2_FORMAT_2_3 => Ok(Self::V2_3),
            "stable" => Ok(Self::Stable),
            "next" => Ok(Self::Next),
            _ => Err(unknown_version(value)),
        }
    }
}

/// The exact persisted identity of a Lance file format.
///
/// Unlike [`LanceFileVersion`], this type cannot represent release selectors such as
/// `stable` or `next`. Exact versions deliberately have no ordering because format
/// capabilities are not implied by release order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConcreteFileVersion {
    /// The legacy v1 file format.
    V1,
    /// The v2.0 file format.
    V2_0,
    /// The v2.1 file format.
    V2_1,
    /// The v2.2 file format.
    V2_2,
    /// The v2.3 file format.
    V2_3,
}

impl DeepSizeOf for ConcreteFileVersion {
    fn deep_size_of_children(&self, _context: &mut Context) -> usize {
        0
    }
}

impl ConcreteFileVersion {
    /// Decode the exact version string stored in a dataset manifest.
    ///
    /// Public selector aliases such as `legacy`, `0.3`, `stable`, and `next` are
    /// intentionally rejected because manifests only store canonical exact versions.
    pub fn from_manifest_string(value: &str) -> Result<Self> {
        match value {
            LEGACY_FORMAT_VERSION => Ok(Self::V1),
            V2_FORMAT_2_0 => Ok(Self::V2_0),
            V2_FORMAT_2_1 => Ok(Self::V2_1),
            V2_FORMAT_2_2 => Ok(Self::V2_2),
            V2_FORMAT_2_3 => Ok(Self::V2_3),
            _ => Err(unknown_version(value)),
        }
    }

    /// Encode this exact version as the canonical string stored in a dataset manifest.
    pub const fn to_manifest_string(self) -> &'static str {
        match self {
            Self::V1 => LEGACY_FORMAT_VERSION,
            Self::V2_0 => V2_FORMAT_2_0,
            Self::V2_1 => V2_FORMAT_2_1,
            Self::V2_2 => V2_FORMAT_2_2,
            Self::V2_3 => V2_FORMAT_2_3,
        }
    }

    /// Decode the major/minor version stored in `DataFile` metadata.
    ///
    /// Legacy manifests may omit these fields and decode to `(0, 0)`, so all legacy
    /// v1 number pairs accepted by the historical decoder remain valid inputs. The
    /// historical generic decoder also accepted the standard v2.0 footer pair `(0, 3)`;
    /// decoding retains that compatibility while encoding always emits `(2, 0)`.
    pub fn from_data_file_numbers(major: u32, minor: u32) -> Result<Self> {
        match (major, minor) {
            (0, 0..=2) => Ok(Self::V1),
            (0, 3) | (2, 0) => Ok(Self::V2_0),
            (2, 1) => Ok(Self::V2_1),
            (2, 2) => Ok(Self::V2_2),
            (2, 3) => Ok(Self::V2_3),
            _ => Err(unknown_version(format_args!("{}.{}", major, minor))),
        }
    }

    /// Encode the canonical major/minor pair stored in `DataFile` metadata.
    pub const fn to_data_file_numbers(self) -> (u32, u32) {
        match self {
            Self::V1 => (0, 2),
            Self::V2_0 => (2, 0),
            Self::V2_1 => (2, 1),
            Self::V2_2 => (2, 2),
            Self::V2_3 => (2, 3),
        }
    }

    /// Decode the major/minor version stored in a Lance file footer.
    ///
    /// V2.0 has two accepted representations: `(0, 3)` from the standard file writer
    /// and `(2, 0)` from self-described and mini-lance writers.
    pub fn from_footer_numbers(major: u16, minor: u16) -> Result<Self> {
        match (major, minor) {
            (0, 0..=2) => Ok(Self::V1),
            (0, 3) | (2, 0) => Ok(Self::V2_0),
            (2, 1) => Ok(Self::V2_1),
            (2, 2) => Ok(Self::V2_2),
            (2, 3) => Ok(Self::V2_3),
            _ => Err(unknown_version(format_args!("{}.{}", major, minor))),
        }
    }

    /// Encode the footer numbers emitted by the standard Lance file writer.
    pub const fn to_standard_footer_numbers(self) -> (u16, u16) {
        match self {
            Self::V1 => (0, 2),
            Self::V2_0 => (0, 3),
            Self::V2_1 => (2, 1),
            Self::V2_2 => (2, 2),
            Self::V2_3 => (2, 3),
        }
    }

    /// Encode the footer numbers emitted by self-described and mini-lance writers.
    pub const fn to_embedded_footer_numbers(self) -> (u16, u16) {
        match self {
            Self::V1 => (0, 2),
            Self::V2_0 => (2, 0),
            Self::V2_1 => (2, 1),
            Self::V2_2 => (2, 2),
            Self::V2_3 => (2, 3),
        }
    }
}

impl Display for ConcreteFileVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_manifest_string())
    }
}

impl From<ConcreteFileVersion> for LanceFileVersion {
    fn from(value: ConcreteFileVersion) -> Self {
        match value {
            ConcreteFileVersion::V1 => Self::Legacy,
            ConcreteFileVersion::V2_0 => Self::V2_0,
            ConcreteFileVersion::V2_1 => Self::V2_1,
            ConcreteFileVersion::V2_2 => Self::V2_2,
            ConcreteFileVersion::V2_3 => Self::V2_3,
        }
    }
}

impl From<LanceFileVersion> for ConcreteFileVersion {
    fn from(value: LanceFileVersion) -> Self {
        match value.resolve() {
            LanceFileVersion::Legacy => Self::V1,
            LanceFileVersion::V2_0 => Self::V2_0,
            LanceFileVersion::V2_1 => Self::V2_1,
            LanceFileVersion::V2_2 => Self::V2_2,
            LanceFileVersion::V2_3 => Self::V2_3,
            LanceFileVersion::Stable | LanceFileVersion::Next => {
                unreachable!("resolved file-version selector must be exact")
            }
        }
    }
}

fn unknown_version(value: impl Display) -> Error {
    Error::invalid_input_source(format!("Unknown Lance storage version: {}", value).into())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use lance_io::object_store::ObjectStore;
    use object_store::path::Path;

    use super::*;

    const EXACT_VERSIONS: [ConcreteFileVersion; 5] = [
        ConcreteFileVersion::V1,
        ConcreteFileVersion::V2_0,
        ConcreteFileVersion::V2_1,
        ConcreteFileVersion::V2_2,
        ConcreteFileVersion::V2_3,
    ];

    #[test]
    fn selector_resolution_is_exact() {
        let cases = [
            (LanceFileVersion::Legacy, ConcreteFileVersion::V1),
            (LanceFileVersion::V2_0, ConcreteFileVersion::V2_0),
            (LanceFileVersion::V2_1, ConcreteFileVersion::V2_1),
            (LanceFileVersion::Stable, ConcreteFileVersion::V2_1),
            (LanceFileVersion::V2_2, ConcreteFileVersion::V2_2),
            (LanceFileVersion::Next, ConcreteFileVersion::V2_3),
            (LanceFileVersion::V2_3, ConcreteFileVersion::V2_3),
        ];

        for (selector, expected) in cases {
            assert_eq!(ConcreteFileVersion::from(selector), expected);
            assert_eq!(LanceFileVersion::from(expected), selector.resolve());
        }
    }

    #[test]
    fn public_selector_aliases_remain_unchanged() {
        let cases = [
            ("0.1", LanceFileVersion::Legacy),
            ("legacy", LanceFileVersion::Legacy),
            ("2.0", LanceFileVersion::V2_0),
            ("0.3", LanceFileVersion::V2_0),
            ("2.1", LanceFileVersion::V2_1),
            ("stable", LanceFileVersion::Stable),
            ("2.2", LanceFileVersion::V2_2),
            ("next", LanceFileVersion::Next),
            ("2.3", LanceFileVersion::V2_3),
        ];

        for (value, expected) in cases {
            assert_eq!(LanceFileVersion::from_str(value).unwrap(), expected);
        }
    }

    #[test]
    fn manifest_codec_only_accepts_canonical_exact_versions() {
        for version in EXACT_VERSIONS {
            let encoded = version.to_manifest_string();
            assert_eq!(
                ConcreteFileVersion::from_manifest_string(encoded).unwrap(),
                version
            );
        }

        for selector_or_alias in ["legacy", "0.3", "stable", "next"] {
            assert!(ConcreteFileVersion::from_manifest_string(selector_or_alias).is_err());
        }
    }

    #[test]
    fn data_file_codec_preserves_wire_numbers() {
        let cases = [
            (ConcreteFileVersion::V1, (0, 2)),
            (ConcreteFileVersion::V2_0, (2, 0)),
            (ConcreteFileVersion::V2_1, (2, 1)),
            (ConcreteFileVersion::V2_2, (2, 2)),
            (ConcreteFileVersion::V2_3, (2, 3)),
        ];

        for (version, encoded) in cases {
            assert_eq!(version.to_data_file_numbers(), encoded);
            assert_eq!(
                ConcreteFileVersion::from_data_file_numbers(encoded.0, encoded.1).unwrap(),
                version
            );
        }
        for minor in 0..=2 {
            assert_eq!(
                ConcreteFileVersion::from_data_file_numbers(0, minor).unwrap(),
                ConcreteFileVersion::V1
            );
        }
        assert_eq!(
            ConcreteFileVersion::from_data_file_numbers(0, 3).unwrap(),
            ConcreteFileVersion::V2_0
        );
    }

    #[test]
    fn footer_codec_preserves_both_v2_0_writer_representations() {
        let standard_cases = [
            (ConcreteFileVersion::V1, (0, 2)),
            (ConcreteFileVersion::V2_0, (0, 3)),
            (ConcreteFileVersion::V2_1, (2, 1)),
            (ConcreteFileVersion::V2_2, (2, 2)),
            (ConcreteFileVersion::V2_3, (2, 3)),
        ];
        let embedded_cases = [
            (ConcreteFileVersion::V1, (0, 2)),
            (ConcreteFileVersion::V2_0, (2, 0)),
            (ConcreteFileVersion::V2_1, (2, 1)),
            (ConcreteFileVersion::V2_2, (2, 2)),
            (ConcreteFileVersion::V2_3, (2, 3)),
        ];

        for (version, encoded) in standard_cases {
            assert_eq!(version.to_standard_footer_numbers(), encoded);
            assert_eq!(
                ConcreteFileVersion::from_footer_numbers(encoded.0, encoded.1).unwrap(),
                version
            );
        }
        for (version, encoded) in embedded_cases {
            assert_eq!(version.to_embedded_footer_numbers(), encoded);
            assert_eq!(
                ConcreteFileVersion::from_footer_numbers(encoded.0, encoded.1).unwrap(),
                version
            );
        }
        for minor in 0..=2 {
            assert_eq!(
                ConcreteFileVersion::from_footer_numbers(0, minor).unwrap(),
                ConcreteFileVersion::V1
            );
        }
    }

    #[tokio::test]
    async fn file_version_detection_accepts_all_legacy_footer_aliases() {
        let object_store = ObjectStore::memory();
        for minor in 0u16..=2 {
            let path = Path::from(format!("legacy-{minor}.lance"));
            let mut footer = Vec::with_capacity(8);
            footer.extend_from_slice(&0u16.to_le_bytes());
            footer.extend_from_slice(&minor.to_le_bytes());
            footer.extend_from_slice(crate::format::MAGIC);
            object_store.put(&path, &footer).await.unwrap();

            assert_eq!(
                crate::determine_file_version(&object_store, &path, Some(footer.len()))
                    .await
                    .unwrap(),
                ConcreteFileVersion::V1
            );
        }
    }
}
