// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

pub const OPT_REMOTE_PREFIX: &str = "remote_database_";
pub const OPT_REMOTE_API_KEY: &str = "remote_database_api_key";
pub const OPT_REMOTE_REGION: &str = "remote_database_region";
pub const OPT_REMOTE_HOST_OVERRIDE: &str = "remote_database_host_override";
pub const OPT_REMOTE_SQL_HOST_OVERRIDE: &str = "remote_database_sql_host_override";
// TODO: add support for configuring client config via key/value options

#[derive(Clone, Debug, Default)]
pub struct RemoteDatabaseOptions {
    /// The LanceDB Cloud API key
    pub api_key: Option<String>,
    /// The LanceDB Cloud region
    pub region: Option<String>,
    /// The LanceDB Enterprise host override
    ///
    /// This is required when connecting to LanceDB Enterprise and should be
    /// provided if using an on-premises LanceDB Enterprise instance.
    pub host_override: Option<String>,
    /// Storage options configure the storage layer (e.g. S3, GCS, Azure, etc.)
    ///
    /// See available options at <https://docs.lancedb.com/storage/>
    ///
    /// These options are only used for LanceDB Enterprise and only a subset of options
    /// are supported.
    pub storage_options: HashMap<String, String>,
}

impl RemoteDatabaseOptions {
    pub fn builder() -> RemoteDatabaseOptionsBuilder {
        RemoteDatabaseOptionsBuilder::new()
    }

    pub(crate) fn parse_from_map(map: &HashMap<String, String>) -> Result<Self> {
        let api_key = map.get(OPT_REMOTE_API_KEY).cloned();
        let region = map.get(OPT_REMOTE_REGION).cloned();
        let host_override = map.get(OPT_REMOTE_HOST_OVERRIDE).cloned();
        let storage_options = map
            .iter()
            .filter(|(key, _)| !key.starts_with(OPT_REMOTE_PREFIX))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Ok(Self {
            api_key,
            region,
            host_override,
            storage_options,
        })
    }
}

impl DatabaseOptions for RemoteDatabaseOptions {
    fn serialize_into_map(&self, map: &mut HashMap<String, String>) {
        for (key, value) in &self.storage_options {
            map.insert(key.clone(), value.clone());
        }
        if let Some(api_key) = &self.api_key {
            map.insert(OPT_REMOTE_API_KEY.to_string(), api_key.clone());
        }
        if let Some(region) = &self.region {
            map.insert(OPT_REMOTE_REGION.to_string(), region.clone());
        }
        if let Some(host_override) = &self.host_override {
            map.insert(OPT_REMOTE_HOST_OVERRIDE.to_string(), host_override.clone());
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RemoteDatabaseOptionsBuilder {
    options: RemoteDatabaseOptions,
}

impl RemoteDatabaseOptionsBuilder {
    pub fn new() -> Self {
        Self {
            options: RemoteDatabaseOptions::default(),
        }
    }

    /// Set the LanceDB Cloud API key
    ///
    /// # Arguments
    ///
    /// * `api_key` - The LanceDB Cloud API key
    pub fn api_key(mut self, api_key: String) -> Self {
        self.options.api_key = Some(api_key);
        self
    }

    /// Set the LanceDB Cloud region
    ///
    /// # Arguments
    ///
    /// * `region` - The LanceDB Cloud region
    pub fn region(mut self, region: String) -> Self {
        self.options.region = Some(region);
        self
    }

    /// Set the LanceDB Enterprise host override
    ///
    /// # Arguments
    ///
    /// * `host_override` - The LanceDB Enterprise host override
    pub fn host_override(mut self, host_override: String) -> Self {
        self.options.host_override = Some(host_override);
        self
    }
}

/// RemoteOptions contains a subset of StorageOptions that are compatible with Remote LanceDB connections
#[derive(Clone, Debug, Default)]
pub struct RemoteOptions(pub HashMap<String, String>);

impl RemoteOptions {
    pub fn new(options: HashMap<String, String>) -> Self {
        Self(options)
    }
}

impl From<StorageOptions> for RemoteOptions {
    fn from(options: StorageOptions) -> Self {
        let supported_opts = vec!["account_name", "azure_storage_account_name"];
        let mut filtered = HashMap::new();
        for opt in supported_opts {
            if let Some(v) = options.0.get(opt) {
                filtered.insert(opt.to_string(), v.clone());
            }
        }
        Self::new(filtered)
    }
}
