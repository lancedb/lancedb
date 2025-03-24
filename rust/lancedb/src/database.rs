// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! The database module defines the `Database` trait and related types.
//!
//! A "database" is a generic concept for something that manages tables and their metadata.
//!
//! We provide a basic implementation of a database that requires no additional infrastructure
//! and is based off listing directories in a filesystem.
//!
//! Users may want to provider their own implementations for a variety of reasons:
//!  * Tables may be arranged in a different order on the S3 filesystem
//!  * Tables may be managed by some kind of independent application (e.g. some database)
//!  * Tables may be managed by a database system (e.g. Postgres)
//!  * A custom table implementation (e.g. remote table, etc.) may be used

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatchReader;
use async_trait::async_trait;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use lance::dataset::ReadParams;
use lance_datafusion::utils::StreamingWriteSource;

use crate::arrow::{SendableRecordBatchStream, SendableRecordBatchStreamExt};
use crate::error::Result;
use crate::table::{BaseTable, TableDefinition, WriteOptions};

pub mod listing;

pub trait DatabaseOptions {
    fn serialize_into_map(&self, map: &mut HashMap<String, String>);
}

/// A request to list names of tables in the database
#[derive(Clone, Debug, Default)]
pub struct TableNamesRequest {
    /// If present, only return names that come lexicographically after the supplied
    /// value.
    ///
    /// This can be combined with limit to implement pagination by setting this to
    /// the last table name from the previous page.
    pub start_after: Option<String>,
    /// The maximum number of table names to return
    pub limit: Option<u32>,
}

/// A request to open a table
#[derive(Clone, Debug)]
pub struct OpenTableRequest {
    pub name: String,
    pub index_cache_size: Option<u32>,
    pub lance_read_params: Option<ReadParams>,
}

pub type TableBuilderCallback = Box<dyn FnOnce(OpenTableRequest) -> OpenTableRequest + Send>;

/// Describes what happens when creating a table and a table with
/// the same name already exists
pub enum CreateTableMode {
    /// If the table already exists, an error is returned
    Create,
    /// If the table already exists, it is opened.  Any provided data is
    /// ignored.  The function will be passed an OpenTableBuilder to customize
    /// how the table is opened
    ExistOk(TableBuilderCallback),
    /// If the table already exists, it is overwritten
    Overwrite,
}

impl CreateTableMode {
    pub fn exist_ok(
        callback: impl FnOnce(OpenTableRequest) -> OpenTableRequest + Send + 'static,
    ) -> Self {
        Self::ExistOk(Box::new(callback))
    }
}

impl Default for CreateTableMode {
    fn default() -> Self {
        Self::Create
    }
}

/// The data to start a table or a schema to create an empty table
pub enum CreateTableData {
    /// Creates a table using an iterator of data, the schema will be obtained from the data
    Data(Box<dyn RecordBatchReader + Send>),
    /// Creates a table using a stream of data, the schema will be obtained from the data
    StreamingData(SendableRecordBatchStream),
    /// Creates an empty table, the definition / schema must be provided separately
    Empty(TableDefinition),
}

impl CreateTableData {
    pub fn schema(&self) -> Arc<arrow_schema::Schema> {
        match self {
            Self::Data(reader) => reader.schema(),
            Self::StreamingData(stream) => stream.schema(),
            Self::Empty(definition) => definition.schema.clone(),
        }
    }
}

#[async_trait]
impl StreamingWriteSource for CreateTableData {
    fn arrow_schema(&self) -> Arc<arrow_schema::Schema> {
        self.schema()
    }
    fn into_stream(self) -> datafusion_physical_plan::SendableRecordBatchStream {
        match self {
            Self::Data(reader) => reader.into_stream(),
            Self::StreamingData(stream) => stream.into_df_stream(),
            Self::Empty(table_definition) => {
                let schema = table_definition.schema.clone();
                Box::pin(RecordBatchStreamAdapter::new(schema, stream::empty()))
            }
        }
    }
}

/// A request to create a table
pub struct CreateTableRequest {
    /// The name of the new table
    pub name: String,
    /// Initial data to write to the table, can be None to create an empty table
    pub data: CreateTableData,
    /// The mode to use when creating the table
    pub mode: CreateTableMode,
    /// Options to use when writing data (only used if `data` is not None)
    pub write_options: WriteOptions,
}

impl CreateTableRequest {
    pub fn new(name: String, data: CreateTableData) -> Self {
        Self {
            name,
            data,
            mode: CreateTableMode::default(),
            write_options: WriteOptions::default(),
        }
    }
}

/// The `Database` trait defines the interface for database implementations.
///
/// A database is responsible for managing tables and their metadata.
#[async_trait::async_trait]
pub trait Database:
    Send + Sync + std::any::Any + std::fmt::Debug + std::fmt::Display + 'static
{
    /// List the names of tables in the database
    async fn table_names(&self, request: TableNamesRequest) -> Result<Vec<String>>;
    /// Create a table in the database
    async fn create_table(&self, request: CreateTableRequest) -> Result<Arc<dyn BaseTable>>;
    /// Open a table in the database
    async fn open_table(&self, request: OpenTableRequest) -> Result<Arc<dyn BaseTable>>;
    /// Rename a table in the database
    async fn rename_table(&self, old_name: &str, new_name: &str) -> Result<()>;
    /// Drop a table in the database
    async fn drop_table(&self, name: &str) -> Result<()>;
    /// Drop all tables in the database
    async fn drop_all_tables(&self) -> Result<()>;
    fn as_any(&self) -> &dyn std::any::Any;
}
