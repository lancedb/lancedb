// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

pub mod datatypes;
pub mod format;
pub(crate) mod io;
pub mod reader;
pub mod testing;
pub mod version;
pub mod versions;
pub mod writer;

#[cfg(test)]
mod compatibility_tests;

pub use io::LanceEncodingsIo;

use format::MAGIC;
use lance_core::{Error, Result};
use lance_io::object_store::ObjectStore;
use object_store::path::Path;
use version::ConcreteFileVersion;

pub async fn determine_file_version(
    store: &ObjectStore,
    path: &Path,
    known_size: Option<usize>,
) -> Result<ConcreteFileVersion> {
    let size = match known_size {
        None => usize::try_from(store.size(path).await?).map_err(|_| {
            Error::invalid_input(format!("file {} is too large for this platform", path))
        })?,
        Some(size) => size,
    };
    if size < 8 {
        return Err(Error::invalid_input_source(
            format!(
                "the file {} does not appear to be a lance file (too small)",
                path
            )
            .into(),
        ));
    }
    let reader = store.open_with_size(path, size).await?;
    let footer = reader.get_range((size - 8)..size).await?;
    if &footer[4..] != MAGIC {
        return Err(Error::invalid_input_source(
            format!(
                "the file {} does not appear to be a lance file (magic mismatch)",
                path
            )
            .into(),
        ));
    }
    let major_version = u16::from_le_bytes([footer[0], footer[1]]);
    let minor_version = u16::from_le_bytes([footer[2], footer[3]]);

    ConcreteFileVersion::from_footer_numbers(major_version, minor_version)
}
