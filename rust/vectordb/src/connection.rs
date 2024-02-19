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

//! LanceDB Database

use std::fs::create_dir_all;
use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatchReader;
use arrow_schema::SchemaRef;
use lance::dataset::ReadParams;
use lance::io::{ObjectStore, ObjectStoreParams, WrappingObjectStore};
use object_store::{
    aws::AwsCredential, local::LocalFileSystem, CredentialProvider, StaticCredentialProvider,
};
use snafu::prelude::*;

use crate::error::{CreateDirSnafu, Error, InvalidTableNameSnafu, Result};
use crate::io::object_store::MirroringObjectStoreWrapper;
use crate::table::{NativeTable, TableRef, WriteTableOptions};

pub const LANCE_FILE_EXTENSION: &str = "lance";

/// Describes what happens when creating a table and a table with
/// the same name already exists
#[derive(Clone, Debug)]
pub enum CreateTableMode {
    /// If the table already exists, an error is returned
    Create,
    /// If the table already exists, it is opened (with the provided options)
    /// and returned.  Any provided data is ignored
    ExistOk(OpenTableOptions),
    /// If the table already exists, it is overwritten
    Overwrite,
}

impl Default for CreateTableMode {
    fn default() -> Self {
        Self::Create
    }
}

/// Describes what happens when a vector either contains NaN or
/// does not have enough values
#[derive(Clone, Debug)]
enum BadVectorHandling {
    /// An error is returned
    Error,
    #[allow(dead_code)] // https://github.com/lancedb/lancedb/issues/992
    /// The offending row is droppped
    Drop,
    #[allow(dead_code)] // https://github.com/lancedb/lancedb/issues/992
    /// The invalid/missing items are replaced by fill_value
    Fill(f32),
}

impl Default for BadVectorHandling {
    fn default() -> Self {
        Self::Error
    }
}

#[derive(Clone, Debug, Default)]
pub struct CreateTableOptions {
    /// What behavior to take if a table with the given name already exists
    pub mode: CreateTableMode,
    /// Options to use when writing any initial data
    pub write_options: WriteTableOptions,
}

#[derive(Clone, Debug)]
pub struct OpenTableOptions {
    /// The size of the index cache, specified as a number of entries
    ///
    /// The default value is 256
    ///
    /// The exact meaning of an "entry" will depend on the type of index:
    /// * IVF - there is one entry for each IVF partition
    /// * BTREE - there is one entry for the entire index
    ///
    /// This cache applies to the entire opened table, across all indices.
    /// Setting this value higher will increase performance on larger datasets
    /// at the expense of more RAM
    pub index_cache_size: u64,
    /// Advanced parameters that can be used to customize table reads
    ///
    /// If set, these will take precedence over any overlapping `OpenTableOptions` options
    pub lance_read_params: Option<ReadParams>,
}

impl Default for OpenTableOptions {
    fn default() -> Self {
        Self {
            index_cache_size: 256,
            lance_read_params: None,
        }
    }
}

/// A connection to LanceDB
#[async_trait::async_trait]
pub trait Connection: Send + Sync + std::fmt::Debug + 'static {
    /// Get the names of all tables in the database.
    async fn table_names(&self) -> Result<Vec<String>>;

    /// Create a new table from data
    ///
    /// # Parameters
    ///
    /// * `name` - The name of the table
    /// * `initial_data` - The initial data to write to the table
    /// * `params` - Optional [`CreateTableOptions`] to create the table
    ///
    /// # Returns
    /// Created [`TableRef`], or [`Error::TableAlreadyExists`] if the table already exists
    /// with that name
    async fn create_table(
        &self,
        name: &str,
        initial_data: Box<dyn RecordBatchReader + Send>,
        params: CreateTableOptions,
    ) -> Result<TableRef>;

    /// Create an empty table with a given schema
    ///
    /// # Parameters
    ///
    /// * `name` - The name of the table
    /// * `schema` - The schema of the table
    /// * `params` - Optional [`CreateTableOptions`] to create the table
    ///
    /// # Returns
    /// Created [`TableRef`], or [`Error::TableAlreadyExists`] if the table already exists
    /// with that name
    async fn create_empty_table(
        &self,
        name: &str,
        schema: SchemaRef,
        params: CreateTableOptions,
    ) -> Result<TableRef>;

    /// Open an existing table in the database
    ///
    /// # Arguments
    /// * `name` - The name of the table
    ///
    /// # Returns
    /// Created [`TableRef`], or [`Error::TableNotFound`] if the table does not exist.
    async fn open_table(&self, name: &str, options: OpenTableOptions) -> Result<TableRef>;
    /// Drop a table in the database.
    ///
    /// # Arguments
    /// * `name` - The name of the table to drop
    async fn drop_table(&self, name: &str) -> Result<()>;

    /// Drop the database
    ///
    /// This is the same as dropping all of the tables
    async fn drop_db(&self) -> Result<()>;
}

#[derive(Debug)]
pub struct ConnectOptions {
    /// Database URI
    ///
    /// ### Accpeted URI formats
    ///
    /// - `/path/to/database` - local database on file system.
    /// - `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud object store
    /// - `db://dbname` - LanceDB Cloud
    pub uri: String,

    /// LanceDB Cloud API key, required if using Lance Cloud
    pub api_key: Option<String>,
    /// LanceDB Cloud region, required if using Lance Cloud
    pub region: Option<String>,
    /// LanceDB Cloud host override, only required if using an on-premises Lance Cloud instance
    pub host_override: Option<String>,

    /// User provided AWS credentials
    pub aws_creds: Option<AwsCredential>,
}

impl ConnectOptions {
    /// Create a new [`ConnectOptions`] with the given database URI.
    pub fn new(uri: &str) -> Self {
        Self {
            uri: uri.to_string(),
            api_key: None,
            region: None,
            host_override: None,
            aws_creds: None,
        }
    }

    pub fn api_key(mut self, api_key: &str) -> Self {
        self.api_key = Some(api_key.to_string());
        self
    }

    pub fn region(mut self, region: &str) -> Self {
        self.region = Some(region.to_string());
        self
    }

    pub fn host_override(mut self, host_override: &str) -> Self {
        self.host_override = Some(host_override.to_string());
        self
    }

    /// [`AwsCredential`] to use when connecting to S3.
    pub fn aws_creds(mut self, aws_creds: AwsCredential) -> Self {
        self.aws_creds = Some(aws_creds);
        self
    }
}

/// Connect to a LanceDB database.
///
/// # Arguments
///
/// * `uri` - URI where the database is located, can be a local directory, supported remote cloud storage,
///           or a LanceDB Cloud database.  See [ConnectOptions::uri] for a list of accepted formats
pub async fn connect(uri: &str) -> Result<Arc<dyn Connection>> {
    let options = ConnectOptions::new(uri);
    connect_with_options(&options).await
}

/// Connect to a LanceDB database with [`ConnectOptions`].
///
/// # Arguments
/// - `options` - [`ConnectOptions`] to connect to the database.
pub async fn connect_with_options(options: &ConnectOptions) -> Result<Arc<dyn Connection>> {
    let db = Database::connect_with_options(options).await?;
    Ok(Arc::new(db))
}

#[derive(Debug)]
struct Database {
    object_store: ObjectStore,
    query_string: Option<String>,

    pub(crate) uri: String,
    pub(crate) base_path: object_store::path::Path,

    // the object store wrapper to use on write path
    pub(crate) store_wrapper: Option<Arc<dyn WrappingObjectStore>>,
}

const LANCE_EXTENSION: &str = "lance";
const ENGINE: &str = "engine";
const MIRRORED_STORE: &str = "mirroredStore";

/// A connection to LanceDB
impl Database {
    async fn connect_with_options(options: &ConnectOptions) -> Result<Self> {
        let uri = &options.uri;
        let parse_res = url::Url::parse(uri);

        match parse_res {
            Ok(url) if url.scheme().len() == 1 && cfg!(windows) => Self::open_path(uri).await,
            Ok(mut url) => {
                // iter thru the query params and extract the commit store param
                let mut engine = None;
                let mut mirrored_store = None;
                let mut filtered_querys = vec![];

                // WARNING: specifying engine is NOT a publicly supported feature in lancedb yet
                // THE API WILL CHANGE
                for (key, value) in url.query_pairs() {
                    if key == ENGINE {
                        engine = Some(value.to_string());
                    } else if key == MIRRORED_STORE {
                        if cfg!(windows) {
                            return Err(Error::Lance {
                                message: "mirrored store is not supported on windows".into(),
                            });
                        }
                        mirrored_store = Some(value.to_string());
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
                let os_params: ObjectStoreParams = if let Some(aws_creds) = &options.aws_creds {
                    let credential_provider: Arc<
                        dyn CredentialProvider<Credential = AwsCredential>,
                    > = Arc::new(StaticCredentialProvider::new(AwsCredential {
                        key_id: aws_creds.key_id.clone(),
                        secret_key: aws_creds.secret_key.clone(),
                        token: aws_creds.token.clone(),
                    }));
                    ObjectStoreParams::with_aws_credentials(
                        Some(credential_provider),
                        options.region.clone(),
                    )
                } else {
                    ObjectStoreParams::default()
                };
                let (object_store, base_path) =
                    ObjectStore::from_uri_and_params(&plain_uri, &os_params).await?;
                if object_store.is_local() {
                    Self::try_create_dir(&plain_uri).context(CreateDirSnafu { path: plain_uri })?;
                }

                let write_store_wrapper = match mirrored_store {
                    Some(path) => {
                        let mirrored_store = Arc::new(LocalFileSystem::new_with_prefix(path)?);
                        let wrapper = MirroringObjectStoreWrapper::new(mirrored_store);
                        Some(Arc::new(wrapper) as Arc<dyn WrappingObjectStore>)
                    }
                    None => None,
                };

                Ok(Self {
                    uri: table_base_uri,
                    query_string,
                    base_path,
                    object_store,
                    store_wrapper: write_store_wrapper,
                })
            }
            Err(_) => Self::open_path(uri).await,
        }
    }

    async fn open_path(path: &str) -> Result<Self> {
        let (object_store, base_path) = ObjectStore::from_uri(path).await?;
        if object_store.is_local() {
            Self::try_create_dir(path).context(CreateDirSnafu { path })?;
        }
        Ok(Self {
            uri: path.to_string(),
            query_string: None,
            base_path,
            object_store,
            store_wrapper: None,
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

#[async_trait::async_trait]
impl Connection for Database {
    async fn table_names(&self) -> Result<Vec<String>> {
        let mut f = self
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
            .collect::<Vec<String>>();
        f.sort();
        Ok(f)
    }

    async fn create_table(
        &self,
        name: &str,
        batches: Box<dyn RecordBatchReader + Send>,
        params: CreateTableOptions,
    ) -> Result<TableRef> {
        let table_uri = self.table_uri(name)?;

        Ok(Arc::new(
            NativeTable::create(
                &table_uri,
                name,
                batches,
                self.store_wrapper.clone(),
                params,
            )
            .await?,
        ))
    }

    async fn create_empty_table(
        &self,
        name: &str,
        schema: SchemaRef,
        params: CreateTableOptions,
    ) -> Result<TableRef> {
        let table_uri = self.table_uri(name)?;

        Ok(Arc::new(
            NativeTable::create_empty(&table_uri, name, schema, self.store_wrapper.clone(), params)
                .await?,
        ))
    }

    async fn open_table(&self, name: &str, params: OpenTableOptions) -> Result<TableRef> {
        let table_uri = self.table_uri(name)?;
        Ok(Arc::new(
            NativeTable::open_with_params(&table_uri, name, self.store_wrapper.clone(), params)
                .await?,
        ))
    }

    async fn drop_table(&self, name: &str) -> Result<()> {
        let dir_name = format!("{}.{}", name, LANCE_EXTENSION);
        let full_path = self.base_path.child(dir_name.clone());
        self.object_store.remove_dir_all(full_path).await?;
        Ok(())
    }

    async fn drop_db(&self) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::fs::create_dir_all;

    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn test_connect() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let db = Database::connect_with_options(&ConnectOptions::new(uri))
            .await
            .unwrap();

        assert_eq!(db.uri, uri);
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_connect_relative() {
        let tmp_dir = tempdir().unwrap();
        let uri = std::fs::canonicalize(tmp_dir.path().to_str().unwrap()).unwrap();

        let current_dir = std::env::current_dir().unwrap();
        let ancestors = current_dir.ancestors();
        let relative_ancestors = vec![".."; ancestors.count()];

        let relative_root = std::path::PathBuf::from(relative_ancestors.join("/"));
        let relative_uri = relative_root.join(&uri);

        let db =
            Database::connect_with_options(&ConnectOptions::new(relative_uri.to_str().unwrap()))
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
        let db = connect(uri).await.unwrap();
        let tables = db.table_names().await.unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables[0].eq(&String::from("table1")));
        assert!(tables[1].eq(&String::from("table2")));
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
        let db = connect(uri).await.unwrap();
        db.drop_table("table1").await.unwrap();

        let tables = db.table_names().await.unwrap();
        assert_eq!(tables.len(), 0);
    }
}
