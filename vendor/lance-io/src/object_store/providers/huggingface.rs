// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::sync::Arc;

use object_store::ObjectStore as OSObjectStore;
use object_store::path::Path;
use object_store_opendal::OpendalStore;
use opendal::{Operator, services::Huggingface};
use url::Url;

use crate::object_store::dynamic_opendal::DynamicOpenDalStore;
use crate::object_store::parse_hf_repo_id;
use crate::object_store::{
    DEFAULT_CLOUD_BLOCK_SIZE, DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_MAX_IOP_SIZE, ObjectStore,
    ObjectStoreParams, ObjectStoreProvider, StorageOptions,
};
use lance_core::error::{Error, Result};

/// Hugging Face object store provider backed by OpenDAL.
#[derive(Default, Debug)]
pub struct HuggingfaceStoreProvider;

/// Parsed components from a Hugging Face URL.
#[derive(Debug, PartialEq, Eq)]
struct ParsedHfUrl {
    repo_type: String,
    repo_id: String,
    relative_path: String,
}

fn parse_hf_url(url: &Url) -> Result<ParsedHfUrl> {
    let mut repo_type = url
        .host_str()
        .ok_or_else(|| Error::invalid_input("Huggingface URL must contain repo type"))?
        .to_string();
    // OpenDAL expects `dataset` instead of `datasets`; keep the workaround here and adapt tests.
    if repo_type == "datasets" {
        repo_type = "dataset".to_string();
    }

    let mut segments = url.path().trim_start_matches('/').split('/');
    let owner = segments
        .next()
        .ok_or_else(|| Error::invalid_input("Huggingface URL must contain owner"))?;
    let repo_name = segments
        .next()
        .ok_or_else(|| Error::invalid_input("Huggingface URL must contain repository name"))?;

    let relative_path = segments.collect::<Vec<_>>().join("/");

    Ok(ParsedHfUrl {
        repo_type,
        repo_id: format!("{owner}/{repo_name}"),
        relative_path,
    })
}

fn build_hf_base_options(
    repo_type: &str,
    repo_id: &str,
    storage_options: &StorageOptions,
) -> HashMap<String, String> {
    let mut options = storage_options.0.clone();
    options.insert("repo_type".to_string(), repo_type.to_string());
    options.insert("repo_id".to_string(), repo_id.to_string());
    options
}

fn normalize_download_mode(download_mode: String) -> Result<String> {
    match download_mode.to_lowercase().as_str() {
        "xet" => Ok("xet".to_string()),
        "http" => Ok("http".to_string()),
        _ => Err(Error::invalid_input(format!(
            "Invalid Huggingface download_mode: {download_mode}. Expected one of: xet, http"
        ))),
    }
}

fn normalize_hf_config(options: &HashMap<String, String>) -> Result<HashMap<String, String>> {
    let mut config_map = HashMap::new();

    let repo_type = options
        .get("repo_type")
        .cloned()
        .ok_or_else(|| Error::invalid_input("Huggingface repo_type is required"))?;
    let repo_id = options
        .get("repo_id")
        .cloned()
        .ok_or_else(|| Error::invalid_input("Huggingface repo_id is required"))?;

    config_map.insert("repo_type".to_string(), repo_type);
    config_map.insert("repo_id".to_string(), repo_id);

    if let Some(revision) = options
        .get("hf_revision")
        .cloned()
        .or_else(|| options.get("revision").cloned())
    {
        config_map.insert("revision".to_string(), revision);
    }

    if let Some(root) = options
        .get("hf_root")
        .cloned()
        .or_else(|| options.get("root").cloned())
        && !root.is_empty()
    {
        config_map.insert("root".to_string(), root);
    }

    if let Some(token) = options
        .get("hf_token")
        .cloned()
        .or_else(|| options.get("token").cloned())
        && !token.is_empty()
    {
        config_map.insert("token".to_string(), token);
    }

    let download_mode = options
        .get("hf_download_mode")
        .filter(|download_mode| !download_mode.is_empty())
        .cloned()
        .or_else(|| {
            options
                .get("download_mode")
                .filter(|download_mode| !download_mode.is_empty())
                .cloned()
        })
        .unwrap_or_else(|| "http".to_string());
    config_map.insert(
        "download_mode".to_string(),
        normalize_download_mode(download_mode)?,
    );

    Ok(config_map)
}

fn build_hf_store(config_map: HashMap<String, String>) -> Result<OpendalStore> {
    let repo_type = config_map
        .get("repo_type")
        .ok_or_else(|| Error::invalid_input("Huggingface repo_type is required"))?;
    let repo_id = config_map
        .get("repo_id")
        .ok_or_else(|| Error::invalid_input("Huggingface repo_id is required"))?;

    let mut builder = Huggingface::default().repo_type(repo_type).repo_id(repo_id);
    if let Some(revision) = config_map.get("revision") {
        builder = builder.revision(revision);
    }
    if let Some(root) = config_map.get("root") {
        builder = builder.root(root);
    }
    if let Some(token) = config_map.get("token") {
        builder = builder.token(token);
    }
    if let Some(download_mode) = config_map.get("download_mode") {
        builder = builder.download_mode(download_mode);
    }

    let operator = Operator::new(builder).map_err(|e| {
        Error::invalid_input(format!("Failed to create Huggingface operator: {:?}", e))
    })?;

    Ok(OpendalStore::new(operator))
}

#[async_trait::async_trait]
impl ObjectStoreProvider for HuggingfaceStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let ParsedHfUrl {
            repo_type, repo_id, ..
        } = parse_hf_url(&base_path)?;

        let block_size = params.block_size.unwrap_or(DEFAULT_CLOUD_BLOCK_SIZE);
        let storage_options = StorageOptions(params.storage_options().cloned().unwrap_or_default());
        let download_retry_count = storage_options.download_retry_count();

        let mut base_options = build_hf_base_options(&repo_type, &repo_id, &storage_options);
        if !base_options.contains_key("hf_token") && !base_options.contains_key("token") {
            if let Ok(token) = std::env::var("HF_TOKEN") {
                base_options.insert("hf_token".to_string(), token);
            } else if let Ok(token) = std::env::var("HUGGINGFACE_TOKEN") {
                base_options.insert("hf_token".to_string(), token);
            }
        }

        let accessor = params.get_accessor();
        let inner: Arc<dyn OSObjectStore> =
            if let Some(accessor) = accessor.filter(|a| a.has_provider()) {
                Arc::new(
                    DynamicOpenDalStore::new(
                        format!("hf:{}", base_path),
                        base_options,
                        accessor,
                        normalize_hf_config,
                        build_hf_store,
                    )
                    .with_protected_keys(["repo_type", "repo_id"]),
                )
            } else {
                Arc::new(build_hf_store(normalize_hf_config(&base_options)?)?)
            };

        Ok(ObjectStore {
            scheme: "hf".to_string(),
            inner,
            block_size,
            max_iop_size: *DEFAULT_MAX_IOP_SIZE,
            use_constant_size_upload_parts: params.use_constant_size_upload_parts,
            list_is_lexically_ordered: params.list_is_lexically_ordered.unwrap_or(true),
            io_parallelism: DEFAULT_CLOUD_IO_PARALLELISM,
            download_retry_count,
            io_tracker: Default::default(),
            store_prefix: self
                .calculate_object_store_prefix(&base_path, params.storage_options())?,
        })
    }

    fn extract_path(&self, url: &Url) -> Result<Path> {
        let parsed = parse_hf_url(url)?;
        Path::from_url_path(&parsed.relative_path).map_err(|_| {
            Error::invalid_input(format!("Invalid path in Huggingface URL: {}", url.path()))
        })
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        let repo_id = parse_hf_repo_id(url)?;
        Ok(format!("{}${}@{}", url.scheme(), url.authority(), repo_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::object_store::StorageOptionsAccessor;
    use crate::object_store::dynamic_opendal::DynamicOpenDalStore;
    use crate::object_store::test_utils::StaticMockStorageOptionsProvider;

    #[test]
    fn parse_basic_url() {
        let url = Url::parse("hf://datasets/acme/repo/path/to/table.lance").unwrap();
        let parsed = parse_hf_url(&url).unwrap();
        assert_eq!(
            parsed,
            ParsedHfUrl {
                repo_type: "dataset".to_string(),
                repo_id: "acme/repo".to_string(),
                relative_path: "path/to/table.lance".to_string(),
            }
        );
    }

    #[test]
    fn storage_option_revision_takes_precedence() {
        use crate::object_store::StorageOptionsAccessor;
        use std::sync::Arc;
        let url = Url::parse("hf://datasets/acme/repo/data/file").unwrap();
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([(String::from("hf_revision"), String::from("stable"))]),
            ))),
            ..Default::default()
        };
        // new_store should accept without creating operator; test precedence via builder config
        let ParsedHfUrl {
            repo_type, repo_id, ..
        } = parse_hf_url(&url).unwrap();

        // Build config map the same way new_store would to assert precedence logic.
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert("repo_type".to_string(), repo_type);
        config_map.insert("repo".to_string(), repo_id);
        if let Some(rev) = params
            .storage_options()
            .unwrap()
            .get("hf_revision")
            .cloned()
        {
            config_map.insert("revision".to_string(), rev);
        }
        assert_eq!(config_map.get("revision").unwrap(), "stable");
    }

    #[test]
    fn storage_options_cannot_override_url_repo_identity() {
        let config = normalize_hf_config(&build_hf_base_options(
            "dataset",
            "acme/repo",
            &crate::object_store::StorageOptions(HashMap::from([
                ("repo_type".to_string(), "model".to_string()),
                ("repo_id".to_string(), "other/repo".to_string()),
                ("hf_revision".to_string(), "stable".to_string()),
            ])),
        ))
        .unwrap();

        assert_eq!(config.get("repo_type").unwrap(), "dataset");
        assert_eq!(config.get("repo_id").unwrap(), "acme/repo");
        assert_eq!(config.get("revision").unwrap(), "stable");
    }

    #[test]
    fn storage_option_download_mode_takes_hf_prefix_precedence() {
        let config = normalize_hf_config(&build_hf_base_options(
            "dataset",
            "acme/repo",
            &crate::object_store::StorageOptions(HashMap::from([
                ("download_mode".to_string(), "xet".to_string()),
                ("hf_download_mode".to_string(), "http".to_string()),
            ])),
        ))
        .unwrap();

        assert_eq!(config.get("download_mode").unwrap(), "http");
    }

    #[test]
    fn storage_option_download_mode_defaults_to_http() {
        let config = normalize_hf_config(&build_hf_base_options(
            "dataset",
            "acme/repo",
            &crate::object_store::StorageOptions(HashMap::new()),
        ))
        .unwrap();

        assert_eq!(config.get("download_mode").unwrap(), "http");
    }

    #[test]
    fn storage_option_download_mode_rejects_invalid_value() {
        let err = normalize_hf_config(&build_hf_base_options(
            "dataset",
            "acme/repo",
            &crate::object_store::StorageOptions(HashMap::from([(
                "hf_download_mode".to_string(),
                "invalid".to_string(),
            )])),
        ))
        .unwrap_err();

        assert!(
            err.to_string().contains("download_mode"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn parse_hf_repo_id_with_type_and_owner_repo() {
        let url = Url::parse("hf://models/owner/repo/path/to/file").unwrap();
        let repo = crate::object_store::parse_hf_repo_id(&url).unwrap();
        assert_eq!(repo, "owner/repo");
    }

    #[test]
    fn parse_hf_repo_id_legacy_without_type() {
        let url = Url::parse("hf://owner/repo/path/to/file").unwrap();
        let repo = crate::object_store::parse_hf_repo_id(&url).unwrap();
        assert_eq!(repo, "owner/repo");
    }

    #[test]
    fn parse_hf_repo_id_strips_revision() {
        let url = Url::parse("hf://datasets/owner/repo@main/data").unwrap();
        let repo = crate::object_store::parse_hf_repo_id(&url).unwrap();
        assert_eq!(repo, "owner/repo");
    }

    #[test]
    fn parse_hf_repo_id_missing_segments_errors() {
        let url = Url::parse("hf://datasets/only-owner").unwrap();
        let err = crate::object_store::parse_hf_repo_id(&url).unwrap_err();
        assert!(
            err.to_string().contains("owner/repo"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn extract_path_returns_relative() {
        let url = Url::parse("hf://datasets/acme/repo/sub/dir/table.lance").unwrap();
        let provider = HuggingfaceStoreProvider;
        let path = provider.extract_path(&url).unwrap();
        assert_eq!(path.to_string(), "sub/dir/table.lance");
    }

    #[test]
    fn calculate_prefix_uses_repo_id() {
        let provider = HuggingfaceStoreProvider;
        let url = Url::parse("hf://datasets/acme/repo/path").unwrap();
        let prefix = provider.calculate_object_store_prefix(&url, None).unwrap();
        assert_eq!(prefix, "hf$datasets@acme/repo");
    }

    #[test]
    fn parse_invalid_url_errors() {
        let url = Url::parse("hf://datasets/only-owner").unwrap();
        let err = parse_hf_url(&url).unwrap_err();
        assert!(err.to_string().contains("repository name"));
    }

    #[tokio::test]
    async fn test_dynamic_opendal_hf_store_uses_provider_token() {
        let parsed = parse_hf_url(&Url::parse("hf://datasets/acme/repo/path").unwrap()).unwrap();
        let accessor = Arc::new(StorageOptionsAccessor::with_provider(Arc::new(
            StaticMockStorageOptionsProvider {
                options: HashMap::from([("hf_token".to_string(), "dynamic-token".to_string())]),
            },
        )));

        let store = DynamicOpenDalStore::new(
            "hf",
            build_hf_base_options(
                &parsed.repo_type,
                &parsed.repo_id,
                &crate::object_store::StorageOptions(HashMap::new()),
            ),
            accessor,
            normalize_hf_config,
            build_hf_store,
        );

        let current_store = store
            .current_store()
            .await
            .expect("dynamic OpenDAL HuggingFace store should build");

        assert!(current_store.to_string().contains("Opendal"));
    }
}
