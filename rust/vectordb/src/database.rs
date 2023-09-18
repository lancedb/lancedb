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

use crate::error::{CreateDirSnafu, InvalidTableNameSnafu, Result};
use crate::table::{ReadParams, Table};

pub const LANCE_FILE_EXTENSION: &str = "lance";

pub struct Database {
    object_store: ObjectStore,
    query_string: Option<String>,

    pub(crate) uri: String,
    pub(crate) base_path: object_store::path::Path,
}

const LANCE_EXTENSION: &str = "lance";
const ENGINE: &str = "engine";

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
        let parse_res = url::Url::parse(uri);

        match parse_res {
            Ok(url) if url.scheme().len() == 1 && cfg!(windows) => Self::open_path(uri).await,
            Ok(mut url) => {
                // iter thru the query params and extract the commit store param
                let mut engine = None;
                let mut filtered_querys = vec![];

                // WARNING: specifying engine is NOT a publicly supported feature in lancedb yet
                // THE API WILL CHANGE
                for (key, value) in url.query_pairs() {
                    if key == ENGINE {
                        engine = Some(value.to_string());
                    } else {
                        // to owned so we can modify the url
                        filtered_querys.push((key.to_string(), value.to_string()));
                    }
                }

                // Filter out the commit store query param -- it's a lancedb param
                url.query_pairs_mut().clear();
                url.query_pairs_mut().extend_pairs(filtered_querys);
                // Take a copy of the query string so we can propagate it to lance
                let query_string = url.query().map(|s| s.to_string());
                // clear the query string so we can use the url as the base uri
                // use .set_query(None) instead of .set_query("") because the latter
                // will add a trailing '?' to the url
                url.set_query(None);

                let table_base_uri = if let Some(store) = engine {
                    static WARN_ONCE: std::sync::Once = std::sync::Once::new();
                    WARN_ONCE.call_once(|| {
                        log::warn!("Specifing engine is not a publicly supported feature in lancedb yet. THE API WILL CHANGE");
                    });
                    let old_scheme = url.scheme().to_string();
                    let new_scheme = format!("{}+{}", old_scheme, store);
                    url.to_string().replacen(&old_scheme, &new_scheme, 1)
                } else {
                    url.to_string()
                };

                let plain_uri = url.to_string();
                let (object_store, base_path) = ObjectStore::from_uri(&plain_uri).await?;
                if object_store.is_local() {
                    Self::try_create_dir(&plain_uri).context(CreateDirSnafu { path: plain_uri })?;
                }

                Ok(Database {
                    uri: table_base_uri,
                    query_string,
                    base_path,
                    object_store,
                })
            }
            Err(_) => Self::open_path(uri).await,
        }
    }

    async fn open_path(path: &str) -> Result<Database> {
        let (object_store, base_path) = ObjectStore::from_uri(path).await?;
        if object_store.is_local() {
            Self::try_create_dir(path).context(CreateDirSnafu { path: path })?;
        }
        Ok(Self {
            uri: path.to_string(),
            query_string: None,
            base_path,
            object_store,
        })
    }

    /// Try to create a local directory to store the lancedb dataset
    fn try_create_dir(path: &str) -> core::result::Result<(), std::io::Error> {
        let path = Path::new(path);
        if !path.try_exists()? {
            create_dir_all(path)?;
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
            .map(Path::new)
            .filter(|path| {
                let is_lance = path
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(|e| e == LANCE_EXTENSION);
                is_lance.unwrap_or(false)
            })
            .filter_map(|p| p.file_stem().and_then(|s| s.to_str().map(String::from)))
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
        let table_uri = self.table_uri(name)?;
        Table::create(&table_uri, name, batches, params).await
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
        self.open_table_with_params(name, &ReadParams::default())
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
    pub async fn open_table_with_params(&self, name: &str, params: &ReadParams) -> Result<Table> {
        let table_uri = self.table_uri(name)?;
        Table::open_with_params(&table_uri, name, params).await
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

    /// Get the URI of a table in the database.
    fn table_uri(&self, name: &str) -> Result<String> {
        let path = Path::new(&self.uri);
        let table_uri = path.join(format!("{}.{}", name, LANCE_FILE_EXTENSION));

        let mut uri = table_uri
            .as_path()
            .to_str()
            .context(InvalidTableNameSnafu { name })?
            .to_string();

        // If there are query string set on the connection, propagate to lance
        if let Some(query) = self.query_string.as_ref() {
            uri.push('?');
            uri.push_str(query.as_str());
        }

        Ok(uri)
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

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_connect_relative() {
        let tmp_dir = tempdir().unwrap();
        let uri = std::fs::canonicalize(tmp_dir.path().to_str().unwrap()).unwrap();

        let mut relative_anacestors = vec![];
        let current_dir = std::env::current_dir().unwrap();
        let mut ancestors = current_dir.ancestors();
        while let Some(_) = ancestors.next() {
            relative_anacestors.push("..");
        }
        let relative_root = std::path::PathBuf::from(relative_anacestors.join("/"));
        let relative_uri = relative_root.join(&uri);

        let db = Database::connect(relative_uri.to_str().unwrap())
            .await
            .unwrap();

        assert_eq!(db.uri, relative_uri.to_str().unwrap().to_string());
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
