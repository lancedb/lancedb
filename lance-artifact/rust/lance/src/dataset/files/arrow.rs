// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayBuilder, Int8Builder};
use arrow::datatypes::Int8Type;
use arrow_array::builder::{Int64Builder, StringBuilder, StringDictionaryBuilder};
use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, DictionaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use lance_core::Result;

use super::FileRow;
use super::file_types::FileType;

pub static FILE_TYPE_DICT_ARRAY: LazyLock<ArrayRef> = LazyLock::new(|| {
    let mut builder = StringBuilder::with_capacity(5, 20);
    builder.append_value(FileType::Manifest.to_string());
    builder.append_value(FileType::DataFile.to_string());
    builder.append_value(FileType::DeletionFile.to_string());
    builder.append_value(FileType::TransactionFile.to_string());
    builder.append_value(FileType::IndexFile.to_string());
    Arc::new(builder.finish())
});

pub struct FileTypeArrayBuilder {
    builder: Int8Builder,
}

impl FileTypeArrayBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: Int8Builder::with_capacity(capacity),
        }
    }

    pub fn append_value(&mut self, file_type: FileType) {
        let value = file_type.into();
        self.builder.append_value(value);
    }

    pub fn finish(mut self) -> DictionaryArray<Int8Type> {
        let indices = self.builder.finish();
        DictionaryArray::new(indices, FILE_TYPE_DICT_ARRAY.clone())
    }
}

pub(super) static TRACKED_FILES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("version", DataType::Int64, false),
        Field::new(
            "base_uri",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("path", DataType::Utf8, false),
        Field::new(
            "type",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
        ),
    ]))
});

pub(super) static ALL_FILES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new(
            "base_uri",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("path", DataType::Utf8, false),
        Field::new("size_bytes", DataType::Int64, false),
        Field::new(
            "last_modified",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]))
});

/// Arrow batch builder for the `tracked_files` schema.
///
/// Construct with [`with_capacity`](Self::with_capacity) to pre-size the
/// underlying buffers, then call [`extend`](Self::extend) to fill rows in bulk.
pub(super) struct TrackedFileBatch {
    version: Int64Builder,
    base_uri: StringDictionaryBuilder<Int32Type>,
    path: StringBuilder,
    file_type: FileTypeArrayBuilder,
}

impl TrackedFileBatch {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            version: Int64Builder::with_capacity(capacity),
            // Most of the time, there is only 1 base_uri
            base_uri: StringDictionaryBuilder::with_capacity(capacity, 1, 20),
            path: StringBuilder::with_capacity(capacity, capacity * 50),
            file_type: FileTypeArrayBuilder::with_capacity(capacity),
        }
    }

    pub fn append(&mut self, row: &FileRow) {
        self.version.append_value(row.version as i64);
        self.base_uri.append_value(&row.base_uri);
        self.path.append_value(&row.path);
        self.file_type.append_value(row.file_type);
    }

    pub fn len(&self) -> usize {
        self.version.len()
    }

    pub fn finish(mut self) -> Result<RecordBatch> {
        RecordBatch::try_new(
            TRACKED_FILES_SCHEMA.clone(),
            vec![
                Arc::new(self.version.finish()),
                Arc::new(self.base_uri.finish()),
                Arc::new(self.path.finish()),
                Arc::new(self.file_type.finish()),
            ],
        )
        .map_err(Into::into)
    }
}
