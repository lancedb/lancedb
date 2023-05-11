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
use std::path::{Path, PathBuf};
use std::sync::Arc;
use arrow_array::RecordBatchReader;

use crate::error::Result;
use crate::table::Table;

pub struct Database {
    pub(crate) path: Arc<PathBuf>,
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
    pub fn connect<P: AsRef<Path>>(path: P) -> Result<Database> {
        if !path.as_ref().try_exists()? {
            create_dir_all(&path)?;
        }
        Ok(Database {
            path: Arc::new(path.as_ref().to_path_buf()),
        })
    }

    /// Get the names of all tables in the database.
    ///
    /// # Returns
    ///
    /// * A [Vec<String>] with all table names.
    pub fn table_names(&self) -> Result<Vec<String>> {
        let f = self
            .path
            .read_dir()?
            .flatten()
            .map(|dir_entry| dir_entry.path())
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

    pub async fn create_table(&self, name: String,batches: &mut Box<dyn RecordBatchReader + Send>,
    ) -> Result<Table> {
        Table::create(self.path.clone(), name, batches).await
    }

    /// Open a table in the database.
    ///
    /// # Arguments
    /// * `name` - The name of the table.
    ///
    /// # Returns
    ///
    /// * A [Table] object.
    pub async fn open_table(&self, name: String) -> Result<Table> {
        Table::new(self.path.clone(), name).await
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
        let path_buf = tmp_dir.into_path();
        let db = Database::connect(&path_buf);

        assert_eq!(db.unwrap().path.as_path(), path_buf.as_path())
    }

    #[tokio::test]
    async fn test_table_names() {
        let tmp_dir = tempdir().unwrap();
        create_dir_all(tmp_dir.path().join("table1.lance")).unwrap();
        create_dir_all(tmp_dir.path().join("table2.lance")).unwrap();
        create_dir_all(tmp_dir.path().join("invalidlance")).unwrap();

        let db = Database::connect(&tmp_dir.into_path()).unwrap();
        let tables = db.table_names().unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&String::from("table1")));
        assert!(tables.contains(&String::from("table2")));
    }
}
