// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

// Discriminants are the dictionary keys used in the `tracked_files` output
// schema; they must stay in sync with `FILE_TYPE_DICT_ARRAY` in `arrow.rs`.
#[repr(i8)]
#[derive(Debug, Clone, Copy)]
pub enum FileType {
    Manifest = 0,
    DataFile = 1,
    DeletionFile = 2,
    TransactionFile = 3,
    IndexFile = 4,
}

impl std::fmt::Display for FileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Manifest => "manifest",
            Self::DataFile => "data file",
            Self::DeletionFile => "deletion file",
            Self::TransactionFile => "transaction file",
            Self::IndexFile => "index file",
        };
        write!(f, "{s}")
    }
}

impl From<FileType> for i8 {
    fn from(file_type: FileType) -> Self {
        file_type as Self
    }
}
