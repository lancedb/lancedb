// Copyright 2023 Lance Developers.
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
use lance::io::object_store::ObjectStore;
use snafu::prelude::*;

use crate::error::{CreateDirSnafu, Result};
use crate::table::Table;

pub struct Database {
    object_store: ObjectStore,

    pub(crate) uri: String,
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
        let (object_store, _) = ObjectStore::from_uri(uri).await?;
        if object_store.is_local() {
            Self::try_create_dir(uri).context(CreateDirSnafu { path: uri })?;
        }
        Ok(Database {
            uri: uri.to_string(),
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
            .read_dir(self.uri.as_str())
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

    pub async fn create_table(
        &self,
        name: &str,
        batches: Box<dyn RecordBatchReader>,
    ) -> Result<Table> {
        Table::create(&self.uri, name, batches).await
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
        Table::open(&self.uri, name).await
    }

    /// Drops a Table and delete all data stored in it.
    ///
    /// # Arguments
    /// * `name` - The name of the table.
    ///
    pub async fn drop_table(&self, name: &str) -> Result<()> {
        self.open_table(name).await?.drop_table().await
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, RecordBatch, RecordBatchReader};
    use arrow_schema::{DataType, Field, Schema};
    use lance::arrow::RecordBatchBuffer;
    use std::fs::create_dir_all;
    use std::sync::Arc;
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
    async fn test_drop_table() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let buffer = RecordBatchBuffer::new(vec![RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap()]);
        let batches: Box<dyn RecordBatchReader> = Box::new(buffer);

        let uri = tmp_dir.path().to_str().unwrap();
        let db = Database::connect(uri).await.unwrap();
        db.create_table("test", batches).await.unwrap();
        assert_eq!(db.table_names().await.unwrap()[0], "test");
        db.drop_table("test").await.unwrap();
        assert!(db.table_names().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_connect_s3() {
        // let db = Database::connect("s3://bucket/path/to/database").await.unwrap();
    }
}
