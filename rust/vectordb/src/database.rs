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

use crate::error::Result;
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
        let object_store = ObjectStore::new(uri).await?;
        if matches!(object_store.scheme.as_str(), "file") {
            let path = Path::new(uri);
            if !path.try_exists()? {
                create_dir_all(&path)?;
            }
        }
        Ok(Database {
            uri: uri.to_string(),
            object_store,
        })
    }

    /// Get the names of all tables in the database.
    ///
    /// # Returns
    ///
    /// * A [Vec<String>] with all table names.
    pub async fn table_names(&self) -> Result<Vec<String>> {
        let f = self
            .object_store
            .read_dir("/")
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
}
