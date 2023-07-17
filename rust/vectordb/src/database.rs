// Copyright 2023 LanceDB Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs::create_dir_all;
use std::path::Path;

use arrow_array::RecordBatchReader;
use lance::dataset::WriteParams;
use lance::io::object_store::ObjectStore;
use snafu::prelude::*;

use crate::error::{CreateDirSnafu, Result};
use crate::table::{OpenTableParams, Table};

pub struct Database {
    object_store: ObjectStore,

    pub(crate) uri: String,
    pub(crate) base_path: object_store::path::Path,
}

const LANCE_EXTENSION: &str = "lance";

/// A connection to LanceDB
impl Database {
    /// Connects to LanceDB
    ///
    /// # Arguments
    ///
    /// * `path` - URI where the database is located, can be a local file or a supported remote cloud storage
    ///
    /// # Returns
    ///
    /// * A [Database] object.
    pub async fn connect(uri: &str) -> Result<Database> {
        let (object_store, base_path) = ObjectStore::from_uri(uri).await?;
        if object_store.is_local() {
            Self::try_create_dir(uri).context(CreateDirSnafu { path: uri })?;
        }
        Ok(Database {
            uri: uri.to_string(),
            base_path,
            object_store,
        })
    }

    /// Try to create a local directory to store the lancedb dataset
    fn try_create_dir(path: &str) -> core::result::Result<(), std::io::Error> {
        let path = Path::new(path);
        if !path.try_exists()? {
            create_dir_all(&path)?;
        }
        Ok(())
    }

    /// Get the names of all tables in the database.
    ///
    /// # Returns
    ///
    /// * A [Vec<String>] with all table names.
    pub async fn table_names(&self) -> Result<Vec<String>> {
        let f = self
            .object_store
            .read_dir(self.base_path.clone())
            .await?
            .iter()
            .map(|fname| Path::new(fname))
            .filter(|path| {
                let is_lance = path
                    .extension()
                    .map(|e| e.to_str().map(|e| e == LANCE_EXTENSION))
                    .flatten();
                is_lance.unwrap_or(false)
            })
            .map(|p| {
                p.file_stem()
                    .map(|s| s.to_str().map(|s| String::from(s)))
                    .flatten()
            })
            .flatten()
            .collect();
        Ok(f)
    }

    /// Create a new table in the database.
    ///
    /// # Arguments
    /// * `name` - The name of the table.
    /// * `batches` - The initial data to write to the table.
    /// * `params` - Optional [`WriteParams`] to create the table.
    pub async fn create_table(
        &self,
        name: &str,
        batches: impl RecordBatchReader + Send + 'static,
        params: Option<WriteParams>,
    ) -> Result<Table> {
        Table::create(&self.uri, name, batches, params).await
    }

    /// Open a table in the database.
    ///
    /// # Arguments
    /// * `name` - The name of the table.
    ///
    /// # Returns
    ///
    /// * A [Table] object.
    pub async fn open_table(&self, name: &str) -> Result<Table> {
        self.open_table_with_params(name, OpenTableParams::default())
            .await
    }

    /// Open a table in the database.
    ///
    /// # Arguments
    /// * `name` - The name of the table.
    /// * `params` - The parameters to open the table.
    ///
    /// # Returns
    ///
    /// * A [Table] object.
    pub async fn open_table_with_params(
        &self,
        name: &str,
        params: OpenTableParams,
    ) -> Result<Table> {
        Table::open_with_params(&self.uri, name, params).await
    }

    /// Drop a table in the database.
    ///
    /// # Arguments
    /// * `name` - The name of the table.
    pub async fn drop_table(&self, name: &str) -> Result<()> {
        let dir_name = format!("{}.{}", name, LANCE_EXTENSION);
        let full_path = self.base_path.child(dir_name.clone());
        self.object_store.remove_dir_all(full_path).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::create_dir_all;
    use tempfile::tempdir;

    use crate::database::Database;

    #[tokio::test]
    async fn test_connect() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let db = Database::connect(uri).await.unwrap();

        assert_eq!(db.uri, uri);
    }

    #[tokio::test]
    async fn test_table_names() {
        let tmp_dir = tempdir().unwrap();
        create_dir_all(tmp_dir.path().join("table1.lance")).unwrap();
        create_dir_all(tmp_dir.path().join("table2.lance")).unwrap();
        create_dir_all(tmp_dir.path().join("invalidlance")).unwrap();

        let uri = tmp_dir.path().to_str().unwrap();
        let db = Database::connect(uri).await.unwrap();
        let tables = db.table_names().await.unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&String::from("table1")));
        assert!(tables.contains(&String::from("table2")));
    }

    #[tokio::test]
    async fn test_connect_s3() {
        // let db = Database::connect("s3://bucket/path/to/database").await.unwrap();
    }

    #[tokio::test]
    async fn drop_table() {
        let tmp_dir = tempdir().unwrap();
        create_dir_all(tmp_dir.path().join("table1.lance")).unwrap();

        let uri = tmp_dir.path().to_str().unwrap();
        let db = Database::connect(uri).await.unwrap();
        db.drop_table("table1").await.unwrap();

        let tables = db.table_names().await.unwrap();
        assert_eq!(tables.len(), 0);
    }
}
