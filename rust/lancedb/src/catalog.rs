// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Catalog implementation for managing databases

pub mod listing;

use std::collections::HashMap;
use std::sync::Arc;

use crate::database::Database;
use crate::error::Result;
use async_trait::async_trait;

pub trait CatalogOptions {
    fn serialize_into_map(&self, map: &mut HashMap<String, String>);
}

/// Request parameters for listing databases
#[derive(Clone, Debug, Default)]
pub struct DatabaseNamesRequest {
    /// Start listing after this name (exclusive)
    pub start_after: Option<String>,
    /// Maximum number of names to return
    pub limit: Option<u32>,
}

/// Request to open an existing database
#[derive(Clone, Debug)]
pub struct OpenDatabaseRequest {
    /// The name of the database to open
    pub name: String,
    /// A map of database-specific options
    ///
    /// Consult the catalog / database implementation to determine which options are available
    pub database_options: HashMap<String, String>,
}

/// Database creation mode
///
/// The default behavior is Create
pub enum CreateDatabaseMode {
    /// Create new database, error if exists
    Create,
    /// Open existing database if present
    ExistOk,
    /// Overwrite existing database
    Overwrite,
}

impl Default for CreateDatabaseMode {
    fn default() -> Self {
        Self::Create
    }
}

/// Request to create a new database
pub struct CreateDatabaseRequest {
    /// The name of the database to create
    pub name: String,
    /// The creation mode
    pub mode: CreateDatabaseMode,
    /// A map of catalog-specific options, consult your catalog implementation to determine what's available
    pub options: HashMap<String, String>,
}

#[async_trait]
pub trait Catalog: Send + Sync + std::fmt::Debug + 'static {
    /// List database names with pagination
    async fn database_names(&self, request: DatabaseNamesRequest) -> Result<Vec<String>>;

    /// Create a new database
    async fn create_database(&self, request: CreateDatabaseRequest) -> Result<Arc<dyn Database>>;

    /// Open existing database
    async fn open_database(&self, request: OpenDatabaseRequest) -> Result<Arc<dyn Database>>;

    /// Rename database
    async fn rename_database(&self, old_name: &str, new_name: &str) -> Result<()>;

    /// Delete database
    async fn drop_database(&self, name: &str) -> Result<()>;

    /// Delete all databases
    async fn drop_all_databases(&self) -> Result<()>;
}
