// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_buffer::ToByteSlice;
use uuid::Uuid;

mod fragment;
mod index;
mod manifest;
pub mod overlay;
mod transaction;

pub use crate::rowids::version::{
    RowDatasetVersionMeta, RowDatasetVersionRun, RowDatasetVersionSequence,
};
pub use fragment::*;
pub use index::{IndexFile, IndexMetadata, index_metadata_codec, list_index_files_with_sizes};

pub use manifest::{
    BasePath, DETACHED_VERSION_MASK, DataStorageFormat, Manifest, SelfDescribingFileReader,
    WriterVersion, is_detached_version, populate_manifest_schema_dictionaries,
};
pub use transaction::Transaction;

use lance_core::{Error, Result};

// In 0.36.1 we renamed Index to IndexMetadata because Index conflicted too much with the
// Index trait.  This is left in for backward compatibility.
#[deprecated(since = "0.36.1", note = "Use IndexMetadata instead")]
pub type Index = IndexMetadata;

/// Protobuf definitions for Lance Format
pub mod pb {
    #![allow(clippy::all)]
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(unused)]
    #![allow(improper_ctypes)]
    #![allow(clippy::upper_case_acronyms)]
    #![allow(clippy::use_self)]
    include!(concat!(env!("OUT_DIR"), "/lance.table.rs"));
}

/// These version/magic values are written at the end of manifest files (e.g. versions/1.version)
pub const MAJOR_VERSION: i16 = 0;
pub const MINOR_VERSION: i16 = 1;
pub const MAGIC: &[u8; 4] = b"LANC";

impl TryFrom<&pb::Uuid> for Uuid {
    type Error = Error;

    fn try_from(p: &pb::Uuid) -> Result<Self> {
        if p.uuid.len() != 16 {
            return Err(Error::invalid_input(
                "Protobuf UUID is malformed".to_string(),
            ));
        }
        let mut buf: [u8; 16] = [0; 16];
        buf.copy_from_slice(p.uuid.to_byte_slice());
        Ok(Self::from_bytes(buf))
    }
}

impl From<&Uuid> for pb::Uuid {
    fn from(value: &Uuid) -> Self {
        Self {
            uuid: value.into_bytes().to_vec(),
        }
    }
}
