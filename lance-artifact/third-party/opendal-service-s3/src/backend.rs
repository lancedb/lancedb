// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Write;
use std::sync::Arc;
use std::sync::LazyLock;

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use constants::X_AMZ_META_PREFIX;
use constants::X_AMZ_VERSION_ID;
use http::StatusCode;
use log::debug;
use log::warn;
use md5::Digest;
use md5::Md5;
use reqsign_aws_v4::AssumeRoleCredentialProvider;
use reqsign_aws_v4::Credential;
use reqsign_aws_v4::DefaultCredentialProvider;
use reqsign_aws_v4::RequestSigner as AwsV4Signer;
use reqsign_aws_v4::StaticCredentialProvider;
use reqsign_core::Context;
use reqsign_core::OsEnv;
use reqsign_core::ProvideCredential;
use reqsign_core::ProvideCredentialChain;
use reqsign_core::ProvideCredentialDyn;
use reqsign_core::Signer;
use reqsign_file_read_tokio::TokioFileRead;
use url::Url;

use crate::S3_SCHEME;
use crate::config::S3Config;
use crate::copier::S3Copiers;
use crate::copier::new_s3_copier;
use crate::core::parse_error;
use crate::core::*;
use crate::deleter::S3Deleter;
use crate::lister::S3ListerV1;
use crate::lister::S3ListerV2;
use crate::lister::S3Listers;
use crate::lister::S3ObjectVersionsLister;
use crate::reader::*;
use crate::writer::S3Writer;
use crate::writer::S3Writers;
use opendal_core::raw::*;
use opendal_core::*;

/// Allow constructing correct region endpoint if user gives a global endpoint.
static ENDPOINT_TEMPLATES: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    // AWS S3 Service.
    m.insert(
        "https://s3.amazonaws.com",
        "https://s3.{region}.amazonaws.com",
    );
    m
});

const DEFAULT_BATCH_MAX_OPERATIONS: usize = 1000;

/// Aws S3 and compatible services (including minio, digitalocean space, Tencent Cloud Object Storage(COS) and so on) support.
/// For more information about s3-compatible services, refer to [Compatible Services](#compatible-services).
#[doc = include_str!("docs.md")]
#[doc = include_str!("compatible_services.md")]
#[derive(Default)]
pub struct S3Builder {
    pub(super) config: S3Config,
    pub(super) credential_provider: Option<Arc<dyn ProvideCredentialDyn<Credential = Credential>>>,
}

impl Debug for S3Builder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Builder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl S3Builder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set bucket name of this backend.
    pub fn bucket(mut self, bucket: &str) -> Self {
        self.config.bucket = bucket.to_string();

        self
    }

    /// Set endpoint of this backend.
    ///
    /// Endpoint must be full uri, e.g.
    ///
    /// - AWS S3: `https://s3.amazonaws.com` or `https://s3.{region}.amazonaws.com`
    /// - Cloudflare R2: `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`
    /// - Aliyun OSS: `https://{region}.aliyuncs.com`
    /// - Tencent COS: `https://cos.{region}.myqcloud.com`
    /// - Minio: `http://127.0.0.1:9000`
    ///
    /// If user inputs endpoint without scheme like "s3.amazonaws.com", we
    /// will prepend "https://" before it.
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.config.endpoint = Some(endpoint.trim_end_matches('/').to_string())
        }

        self
    }

    /// Region represent the signing region of this endpoint. This is required
    /// if you are using the default AWS S3 endpoint.
    ///
    /// If using a custom endpoint,
    /// - If region is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn region(mut self, region: &str) -> Self {
        if !region.is_empty() {
            self.config.region = Some(region.to_string())
        }

        self
    }

    /// Set access_key_id of this backend.
    ///
    /// - If access_key_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn access_key_id(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.access_key_id = Some(v.to_string())
        }

        self
    }

    /// Set secret_access_key of this backend.
    ///
    /// - If secret_access_key is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn secret_access_key(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.secret_access_key = Some(v.to_string())
        }

        self
    }

    /// Set role_arn for this backend.
    ///
    /// If `role_arn` is set, we will use already known config as source
    /// credential to assume role with `role_arn`.
    pub fn role_arn(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.role_arn = Some(v.to_string())
        }

        self
    }

    /// Set external_id for this backend.
    pub fn external_id(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.external_id = Some(v.to_string())
        }

        self
    }

    /// Set role_session_name for this backend.
    pub fn role_session_name(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.role_session_name = Some(v.to_string())
        }

        self
    }

    /// Set assume_role_duration_seconds for this backend.
    pub fn assume_role_duration_seconds(mut self, v: u32) -> Self {
        self.config.assume_role_duration_seconds = Some(v);
        self
    }

    /// Set assume_role_session_tags for this backend.
    pub fn assume_role_session_tags(mut self, tags: HashMap<String, String>) -> Self {
        self.config.assume_role_session_tags = Some(tags);
        self
    }

    /// Set default storage_class for this backend.
    ///
    /// Available values:
    /// - `DEEP_ARCHIVE`
    /// - `GLACIER`
    /// - `GLACIER_IR`
    /// - `INTELLIGENT_TIERING`
    /// - `ONEZONE_IA`
    /// - `OUTPOSTS`
    /// - `REDUCED_REDUNDANCY`
    /// - `STANDARD`
    /// - `STANDARD_IA`
    pub fn default_storage_class(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.default_storage_class = Some(v.to_string())
        }

        self
    }

    /// Set server_side_encryption for this backend.
    ///
    /// Available values: `AES256`, `aws:kms`.
    ///
    /// # Note
    ///
    /// This function is the low-level setting for SSE related features.
    ///
    /// SSE related options should be set carefully to make them works.
    /// Please use `server_side_encryption_with_*` helpers if even possible.
    pub fn server_side_encryption(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.server_side_encryption = Some(v.to_string())
        }

        self
    }

    /// Set server_side_encryption_aws_kms_key_id for this backend
    ///
    /// - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`
    ///   is not set, S3 will use aws managed kms key to encrypt data.
    /// - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`
    ///   is a valid kms key id, S3 will use the provided kms key to encrypt data.
    /// - If the `server_side_encryption_aws_kms_key_id` is invalid or not found, an error will be
    ///   returned.
    /// - If `server_side_encryption` is not `aws:kms`, setting `server_side_encryption_aws_kms_key_id` is a noop.
    ///
    /// # Note
    ///
    /// This function is the low-level setting for SSE related features.
    ///
    /// SSE related options should be set carefully to make them works.
    /// Please use `server_side_encryption_with_*` helpers if even possible.
    pub fn server_side_encryption_aws_kms_key_id(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.server_side_encryption_aws_kms_key_id = Some(v.to_string())
        }

        self
    }

    /// Set server_side_encryption_customer_algorithm for this backend.
    ///
    /// Available values: `AES256`.
    ///
    /// # Note
    ///
    /// This function is the low-level setting for SSE related features.
    ///
    /// SSE related options should be set carefully to make them works.
    /// Please use `server_side_encryption_with_*` helpers if even possible.
    pub fn server_side_encryption_customer_algorithm(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.server_side_encryption_customer_algorithm = Some(v.to_string())
        }

        self
    }

    /// Set server_side_encryption_customer_key for this backend.
    ///
    /// # Args
    ///
    /// `v`: base64 encoded key that matches algorithm specified in
    /// `server_side_encryption_customer_algorithm`.
    ///
    /// # Note
    ///
    /// This function is the low-level setting for SSE related features.
    ///
    /// SSE related options should be set carefully to make them works.
    /// Please use `server_side_encryption_with_*` helpers if even possible.
    pub fn server_side_encryption_customer_key(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.server_side_encryption_customer_key = Some(v.to_string())
        }

        self
    }

    /// Set server_side_encryption_customer_key_md5 for this backend.
    ///
    /// # Args
    ///
    /// `v`: MD5 digest of key specified in `server_side_encryption_customer_key`.
    ///
    /// # Note
    ///
    /// This function is the low-level setting for SSE related features.
    ///
    /// SSE related options should be set carefully to make them works.
    /// Please use `server_side_encryption_with_*` helpers if even possible.
    pub fn server_side_encryption_customer_key_md5(mut self, v: &str) -> Self {
        if !v.is_empty() {
            self.config.server_side_encryption_customer_key_md5 = Some(v.to_string())
        }

        self
    }

    /// Enable server side encryption with aws managed kms key
    ///
    /// As known as: SSE-KMS
    ///
    /// NOTE: This function should not be used along with other `server_side_encryption_with_` functions.
    pub fn server_side_encryption_with_aws_managed_kms_key(mut self) -> Self {
        self.config.server_side_encryption = Some("aws:kms".to_string());
        self
    }

    /// Enable server side encryption with customer managed kms key
    ///
    /// As known as: SSE-KMS
    ///
    /// NOTE: This function should not be used along with other `server_side_encryption_with_` functions.
    pub fn server_side_encryption_with_customer_managed_kms_key(
        mut self,
        aws_kms_key_id: &str,
    ) -> Self {
        self.config.server_side_encryption = Some("aws:kms".to_string());
        self.config.server_side_encryption_aws_kms_key_id = Some(aws_kms_key_id.to_string());
        self
    }

    /// Enable server side encryption with s3 managed key
    ///
    /// As known as: SSE-S3
    ///
    /// NOTE: This function should not be used along with other `server_side_encryption_with_` functions.
    pub fn server_side_encryption_with_s3_key(mut self) -> Self {
        self.config.server_side_encryption = Some("AES256".to_string());
        self
    }

    /// Enable server side encryption with customer key.
    ///
    /// As known as: SSE-C
    ///
    /// NOTE: This function should not be used along with other `server_side_encryption_with_` functions.
    pub fn server_side_encryption_with_customer_key(mut self, algorithm: &str, key: &[u8]) -> Self {
        self.config.server_side_encryption_customer_algorithm = Some(algorithm.to_string());
        self.config.server_side_encryption_customer_key = Some(BASE64_STANDARD.encode(key));
        let key_md5 = Md5::digest(key);
        self.config.server_side_encryption_customer_key_md5 = Some(BASE64_STANDARD.encode(key_md5));
        self
    }

    /// Set temporary credential used in AWS S3 connections
    ///
    /// # Warning
    ///
    /// session token's lifetime is short and requires users to refresh in time.
    pub fn session_token(mut self, token: &str) -> Self {
        if !token.is_empty() {
            self.config.session_token = Some(token.to_string());
        }
        self
    }

    /// Disable config load so that opendal will not load config from
    /// environment.
    ///
    /// For examples:
    ///
    /// - envs like `AWS_ACCESS_KEY_ID`
    /// - files like `~/.aws/config`
    pub fn disable_config_load(mut self) -> Self {
        self.config.disable_config_load = true;
        self
    }

    /// Disable list objects v2 so that opendal will fall back to the older
    /// List Objects V1 to list objects.
    ///
    /// By default, OpenDAL uses List Objects V2 to list objects. However,
    /// some legacy services do not yet support V2.
    pub fn disable_list_objects_v2(mut self) -> Self {
        self.config.disable_list_objects_v2 = true;
        self
    }

    /// Enable request payer so that OpenDAL will send requests with `x-amz-request-payer` header.
    ///
    /// With this option the client accepts to pay for the request and data transfer costs.
    pub fn enable_request_payer(mut self) -> Self {
        self.config.enable_request_payer = true;
        self
    }

    /// Disable load credential from ec2 metadata.
    ///
    /// This option is used to disable the default behavior of opendal
    /// to load credential from ec2 metadata, a.k.a, IMDSv2
    pub fn disable_ec2_metadata(mut self) -> Self {
        self.config.disable_ec2_metadata = true;
        self
    }

    /// Skip signature will skip loading credentials and signing requests.
    pub fn skip_signature(mut self) -> Self {
        self.config.skip_signature = true;
        self
    }

    /// Allow anonymous will allow opendal to send request without signing
    /// when credential is not loaded.
    #[deprecated(
        since = "0.57.0",
        note = "Please use `skip_signature` instead of `allow_anonymous`"
    )]
    pub fn allow_anonymous(self) -> Self {
        self.skip_signature()
    }

    /// Enable virtual host style so that opendal will send API requests
    /// in virtual host style instead of path style.
    ///
    /// - By default, opendal will send API to `https://s3.us-east-1.amazonaws.com/bucket_name`
    /// - Enabled, opendal will send API to `https://bucket_name.s3.us-east-1.amazonaws.com`
    pub fn enable_virtual_host_style(mut self) -> Self {
        self.config.enable_virtual_host_style = true;
        self
    }

    /// Deprecated: S3 stat override capabilities are enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "S3 stat override capabilities are enabled by default and this option is no longer needed."
    )]
    pub fn disable_stat_with_override(self) -> Self {
        self
    }

    /// Deprecated: S3 versioning capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "S3 versioning capability is enabled by default and this option is no longer needed."
    )]
    pub fn enable_versioning(self, _enabled: bool) -> Self {
        self
    }

    /// Replace the credential providers with a custom chain.
    pub fn credential_provider_chain(mut self, chain: ProvideCredentialChain<Credential>) -> Self {
        self.credential_provider = Some(Arc::new(chain));
        self
    }

    /// Replace the credential providers with one custom provider.
    ///
    /// Unlike [`S3Builder::credential_provider_chain`], this method preserves errors from the
    /// provider instead of applying [`ProvideCredentialChain`]'s continue-on-error semantics.
    pub fn credential_provider(
        mut self,
        provider: impl ProvideCredential<Credential = Credential>,
    ) -> Self {
        self.credential_provider = Some(Arc::new(provider));
        self
    }

    /// Check if `bucket` is valid.
    /// `bucket` must be not empty and if `enable_virtual_host_style` is true
    /// it could not contain dot (.) character.
    fn is_bucket_valid(config: &S3Config) -> bool {
        if config.bucket.is_empty() {
            return false;
        }
        // If enable virtual host style, `bucket` will reside in domain part,
        // for example `https://bucket_name.s3.us-east-1.amazonaws.com`,
        // so `bucket` with dot can't be recognized correctly for this format.
        if config.enable_virtual_host_style && config.bucket.contains('.') {
            return false;
        }
        true
    }

    /// Build endpoint with given region.
    fn build_endpoint(config: &S3Config, region: &str) -> String {
        let bucket = {
            debug_assert!(Self::is_bucket_valid(config), "bucket must be valid");

            config.bucket.as_str()
        };

        let mut endpoint = match &config.endpoint {
            Some(endpoint) => {
                if endpoint.starts_with("http") {
                    endpoint.to_string()
                } else {
                    // Prefix https if endpoint doesn't start with scheme.
                    format!("https://{endpoint}")
                }
            }
            None => "https://s3.amazonaws.com".to_string(),
        };

        // If endpoint contains bucket name, we should trim them.
        endpoint = endpoint.replace(&format!("//{bucket}."), "//");

        // Omit default ports if specified.
        if let Ok(url) = Url::parse(&endpoint) {
            // Remove the trailing `/` of root path.
            endpoint = url.to_string().trim_end_matches('/').to_string();
        }

        // Update with endpoint templates.
        endpoint = if let Some(template) = ENDPOINT_TEMPLATES.get(endpoint.as_str()) {
            template.replace("{region}", region)
        } else {
            // If we don't know where about this endpoint, just leave
            // them as it.
            endpoint.to_string()
        };

        // Apply virtual host style.
        if config.enable_virtual_host_style {
            endpoint = endpoint.replace("//", &format!("//{bucket}."))
        } else {
            write!(endpoint, "/{bucket}").expect("write into string must succeed");
        };

        endpoint
    }

    /// Deprecated: S3 delete batch capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "S3 delete batch capability is enabled by default and this option is no longer needed."
    )]
    pub fn batch_max_operations(self, _batch_max_operations: usize) -> Self {
        self
    }

    /// Deprecated: S3 delete batch capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "S3 delete batch capability is enabled by default and this option is no longer needed."
    )]
    pub fn delete_max_size(self, _delete_max_size: usize) -> Self {
        self
    }

    /// Set checksum algorithm of this backend.
    /// This is necessary when writing to AWS S3 Buckets with Object Lock enabled for example.
    ///
    /// Available options:
    /// - "crc32c"
    /// - "md5"
    pub fn checksum_algorithm(mut self, checksum_algorithm: &str) -> Self {
        self.config.checksum_algorithm = Some(checksum_algorithm.to_string());

        self
    }

    /// Deprecated: S3 write with If-Match capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "S3 write with If-Match capability is enabled by default and this option is no longer needed."
    )]
    pub fn disable_write_with_if_match(self) -> Self {
        self
    }

    /// Deprecated: S3 append capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "S3 append capability is enabled by default and this option is no longer needed."
    )]
    pub fn enable_write_with_append(self) -> Self {
        self
    }

    /// Detect region of S3 bucket.
    ///
    /// # Args
    ///
    /// - endpoint: the endpoint of S3 service
    /// - bucket: the bucket of S3 service
    ///
    /// # Return
    ///
    /// - `Some(region)` means we detect the region successfully
    /// - `None` means we can't detect the region or meeting errors.
    ///
    /// # Notes
    ///
    /// We will try to detect region by the following methods.
    ///
    /// - Match endpoint with given rules to get region
    ///   - Cloudflare R2
    ///   - AWS S3
    ///   - Aliyun OSS
    /// - Send a `HEAD` request to endpoint with bucket name to get `x-amz-bucket-region`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use opendal_service_s3::S3;
    ///
    /// # async fn example() {
    /// let region: Option<String> = S3::detect_region("https://s3.amazonaws.com", "example").await;
    /// # }
    /// ```
    ///
    /// # Reference
    ///
    /// - [Amazon S3 HeadBucket API](https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_HeadBucket.html)
    pub async fn detect_region(endpoint: &str, bucket: &str) -> Option<String> {
        // Remove the possible trailing `/` in endpoint.
        let endpoint = endpoint.trim_end_matches('/');

        // Make sure the endpoint contains the scheme.
        let mut endpoint = if endpoint.starts_with("http") {
            endpoint.to_string()
        } else {
            // Prefix https if endpoint doesn't start with scheme.
            format!("https://{endpoint}")
        };

        // Remove bucket name from endpoint.
        endpoint = endpoint.replace(&format!("//{bucket}."), "//");
        let url = format!("{endpoint}/{bucket}");

        debug!("detect region with url: {url}");

        // Try to detect region by endpoint.

        // If this bucket is R2, we can return auto directly.
        //
        // Reference: <https://developers.cloudflare.com/r2/api/s3/api/>
        if endpoint.ends_with("r2.cloudflarestorage.com") {
            return Some("auto".to_string());
        }

        // If this bucket is AWS, we can try to match the endpoint.
        if endpoint == "https://s3.amazonaws.com" {
            return Some("us-east-1".to_string());
        }

        if let Some(region) = endpoint
            .strip_prefix("https://s3.")
            .and_then(|v| v.strip_suffix(".amazonaws.com"))
        {
            return Some(region.to_string());
        }

        // If this bucket is OSS, we can try to match the endpoint.
        //
        // - `oss-ap-southeast-1.aliyuncs.com` => `oss-ap-southeast-1`
        // - `oss-cn-hangzhou-internal.aliyuncs.com` => `oss-cn-hangzhou`
        if let Some(v) = endpoint.strip_prefix("https://") {
            if let Some(region) = v.strip_suffix("-internal.aliyuncs.com") {
                return Some(region.to_string());
            }
            if let Some(region) = v.strip_suffix(".aliyuncs.com") {
                return Some(region.to_string());
            }
        }

        // Try to detect region by HeadBucket.
        let req = http::Request::head(&url).body(Buffer::new()).ok()?;

        let client = HttpTransporter::default();
        let res = client
            .send(req)
            .await
            .map_err(|err| warn!("detect region failed for: {err:?}"))
            .ok()?;

        debug!(
            "auto detect region got response: status {:?}, header: {:?}",
            res.status(),
            res.headers()
        );

        // Get region from response header no matter status code.
        if let Some(region) = res
            .headers()
            .get("x-amz-bucket-region")
            .and_then(|header| header.to_str().ok())
        {
            return Some(region.to_string());
        }

        // Status code is 403 or 200 means we already visit the correct
        // region, we can use the default region directly.
        if res.status() == StatusCode::FORBIDDEN || res.status() == StatusCode::OK {
            return Some("us-east-1".to_string());
        }

        None
    }

    /// Set default ACL for new objects.
    pub fn default_acl(mut self, acl: &str) -> Self {
        self.config.default_acl = Some(acl.to_string());
        self
    }
}

impl Builder for S3Builder {
    type Config = S3Config;

    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", self);

        let S3Builder {
            mut config,
            credential_provider,
        } = self;

        #[allow(deprecated)]
        if config.allow_anonymous {
            config.skip_signature = true;
        }

        let root = normalize_root(&config.root.clone().unwrap_or_default());
        debug!("backend use root {}", root);

        // Handle bucket name.
        let bucket = if Self::is_bucket_valid(&config) {
            Ok(&config.bucket)
        } else {
            Err(
                Error::new(ErrorKind::ConfigInvalid, "The bucket is misconfigured")
                    .with_context("service", S3_SCHEME),
            )
        }?;
        debug!("backend use bucket {}", bucket);

        let default_storage_class = match &config.default_storage_class {
            None => None,
            Some(v) => Some(
                build_header_value(v).map_err(|err| err.with_context("key", "storage_class"))?,
            ),
        };

        let server_side_encryption = match &config.server_side_encryption {
            None => None,
            Some(v) => Some(
                build_header_value(v)
                    .map_err(|err| err.with_context("key", "server_side_encryption"))?,
            ),
        };

        let server_side_encryption_aws_kms_key_id =
            match &config.server_side_encryption_aws_kms_key_id {
                None => None,
                Some(v) => Some(build_header_value(v).map_err(|err| {
                    err.with_context("key", "server_side_encryption_aws_kms_key_id")
                })?),
            };

        let server_side_encryption_customer_algorithm =
            match &config.server_side_encryption_customer_algorithm {
                None => None,
                Some(v) => Some(build_header_value(v).map_err(|err| {
                    err.with_context("key", "server_side_encryption_customer_algorithm")
                })?),
            };

        let server_side_encryption_customer_key =
            match &config.server_side_encryption_customer_key {
                None => None,
                Some(v) => Some(build_header_value(v).map_err(|err| {
                    err.with_context("key", "server_side_encryption_customer_key")
                })?),
            };

        let server_side_encryption_customer_key_md5 =
            match &config.server_side_encryption_customer_key_md5 {
                None => None,
                Some(v) => Some(build_header_value(v).map_err(|err| {
                    err.with_context("key", "server_side_encryption_customer_key_md5")
                })?),
            };

        let checksum_algorithm = match config.checksum_algorithm.as_deref() {
            Some("crc32c") => Some(ChecksumAlgorithm::Crc32c),
            Some("md5") => Some(ChecksumAlgorithm::Md5),
            None => None,
            v => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    format!("{v:?} is not a supported checksum_algorithm."),
                ));
            }
        };

        // Determine the region
        let region = if let Some(ref v) = config.region {
            v.to_string()
        } else {
            std::env::var("AWS_REGION")
                .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                .map_err(|_| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        "region is missing. Please find it by S3::detect_region() or set them in env.",
                    )
                    .with_operation("Builder::build")
                    .with_context("service", S3_SCHEME)
                })?
        };
        debug!("backend use region: {region}");

        if config.endpoint.is_none() && !config.disable_config_load {
            let endpoint_from_env = std::env::var("AWS_ENDPOINT_URL")
                .or_else(|_| std::env::var("AWS_ENDPOINT"))
                .or_else(|_| std::env::var("AWS_S3_ENDPOINT"))
                .ok();
            if let Some(endpoint) = endpoint_from_env {
                let normalized = endpoint.trim_end_matches('/').to_string();
                config.endpoint = Some(normalized);
            }
        }

        // Building endpoint.
        let endpoint = Self::build_endpoint(&config, &region);
        debug!("backend use endpoint: {endpoint}");

        // The base signer context only carries local config readers. HTTP
        // sending is injected from OperationContext when S3Core signs each
        // operation.
        let ctx = Context::new().with_file_read(TokioFileRead).with_env(OsEnv);

        let mut provider = {
            let mut builder = DefaultCredentialProvider::builder();

            if config.disable_config_load {
                builder = builder.no_env().no_profile();
            }

            if config.disable_ec2_metadata {
                builder = builder.no_imds();
            }

            ProvideCredentialChain::new().push(builder.build())
        };

        // Insert static key if user provided.
        if let (Some(ak), Some(sk)) = (&config.access_key_id, &config.secret_access_key) {
            let static_provider = if let Some(token) = config.session_token.as_deref() {
                StaticCredentialProvider::new(ak, sk).with_session_token(token)
            } else {
                StaticCredentialProvider::new(ak, sk)
            };
            provider = provider.push_front(static_provider);
        }

        // Insert assume role provider if user provided.
        if let Some(role_arn) = &config.role_arn {
            // The assume-role provider owns its STS signer, so give it a
            // concrete HTTP sender instead of relying on a future operation
            // context.
            let sts_ctx = ctx.clone().with_http_send(HttpTransporter::default());
            let sts_request_signer = AwsV4Signer::new("sts", &region);
            let sts_signer = Signer::new(sts_ctx, provider, sts_request_signer);
            let mut assume_role_provider =
                AssumeRoleCredentialProvider::new(role_arn.clone(), sts_signer)
                    .with_region(region.clone())
                    .with_regional_sts_endpoint();

            if let Some(external_id) = &config.external_id {
                assume_role_provider = assume_role_provider.with_external_id(external_id.clone());
            }
            if let Some(role_session_name) = &config.role_session_name {
                assume_role_provider =
                    assume_role_provider.with_role_session_name(role_session_name.clone());
            }
            if let Some(duration_seconds) = config.assume_role_duration_seconds {
                assume_role_provider = assume_role_provider.with_duration_seconds(duration_seconds);
            }
            if let Some(tags) = &config.assume_role_session_tags {
                assume_role_provider = assume_role_provider
                    .with_tags(tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
            }
            provider = ProvideCredentialChain::new().push(assume_role_provider);
        }

        // Replace provider if user provide their own.
        let provider: Arc<dyn ProvideCredentialDyn<Credential = Credential>> =
            credential_provider.unwrap_or_else(|| Arc::new(provider));

        // Create request signer for S3
        let request_signer = AwsV4Signer::new("s3", &region);

        // Create the signer
        let signer = Signer::new(ctx, provider, request_signer);

        Ok(S3Backend {
            core: Arc::new(S3Core {
                info: ServiceInfo::new(S3_SCHEME, &root, bucket),
                capability: Capability {
                    stat: true,
                    stat_with_if_match: true,
                    stat_with_if_none_match: true,
                    stat_with_if_modified_since: true,
                    stat_with_if_unmodified_since: true,
                    stat_with_override_cache_control: true,
                    stat_with_override_content_disposition: true,
                    stat_with_override_content_type: true,
                    stat_with_version: true,

                    read: true,
                    read_with_if_match: true,
                    read_with_if_none_match: true,
                    read_with_if_modified_since: true,
                    read_with_if_unmodified_since: true,
                    read_with_override_cache_control: true,
                    read_with_override_content_disposition: true,
                    read_with_override_content_type: true,
                    read_with_version: true,
                    read_with_suffix: true,

                    write: true,
                    write_can_empty: true,
                    write_can_multi: true,
                    write_can_append: true,

                    write_with_cache_control: true,
                    write_with_content_type: true,
                    write_with_content_disposition: true,
                    write_with_content_encoding: true,
                    write_with_if_match: true,
                    write_with_if_not_exists: true,
                    write_with_user_metadata: true,

                    // The min multipart size of S3 is 5 MiB.
                    //
                    // ref: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
                    write_multi_min_size: Some(5 * 1024 * 1024),
                    // The max multipart size of S3 is 5 GiB.
                    //
                    // ref: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
                    write_multi_max_size: if cfg!(target_pointer_width = "64") {
                        Some(5 * 1024 * 1024 * 1024)
                    } else {
                        Some(usize::MAX)
                    },
                    // S3 allows at most 10,000 parts and 5 GiB for each part.
                    //
                    // ref: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
                    write_total_max_size: if cfg!(target_pointer_width = "64") {
                        Some(10_000 * 5 * 1024 * 1024 * 1024)
                    } else {
                        None
                    },

                    delete: true,
                    delete_max_size: Some(DEFAULT_BATCH_MAX_OPERATIONS),
                    delete_with_version: true,

                    copy: true,
                    copy_can_multi: true,
                    copy_with_if_not_exists: true,
                    copy_with_if_match: true,
                    copy_with_source_version: true,
                    // The min multipart size of S3 is 5 MiB.
                    //
                    // ref: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
                    copy_multi_min_size: Some(5 * 1024 * 1024),
                    // The max multipart size of S3 is 5 GiB.
                    //
                    // ref: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
                    copy_multi_max_size: if cfg!(target_pointer_width = "64") {
                        Some(5 * 1024 * 1024 * 1024)
                    } else {
                        Some(usize::MAX)
                    },

                    list: true,
                    list_with_limit: true,
                    list_with_start_after: true,
                    list_with_recursive: true,
                    list_with_versions: true,
                    list_with_deleted: true,

                    presign: true,
                    presign_stat: true,
                    presign_read: true,
                    presign_write: true,
                    presign_delete: true,

                    shared: true,

                    ..Default::default()
                },
                bucket: bucket.to_string(),
                endpoint,
                root,
                server_side_encryption,
                server_side_encryption_aws_kms_key_id,
                server_side_encryption_customer_algorithm,
                server_side_encryption_customer_key,
                server_side_encryption_customer_key_md5,
                default_storage_class,
                skip_signature: config.skip_signature,
                disable_list_objects_v2: config.disable_list_objects_v2,
                enable_request_payer: config.enable_request_payer,
                signer,
                checksum_algorithm,
                default_acl: config.default_acl,
            }),
        })
    }
}

/// Backend for s3 services.
#[derive(Debug, Clone)]
pub struct S3Backend {
    pub(crate) core: Arc<S3Core>,
}

impl Service for S3Backend {
    type Reader = oio::StreamReader<S3Reader>;
    type Writer = S3Writers;
    type Lister = S3Listers;
    type Deleter = oio::BatchDeleter<S3Deleter>;
    type Copier = S3Copiers;

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let resp = self.core.s3_head_object(ctx, path, args).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let headers = resp.headers();
                let mut meta = parse_into_metadata(path, headers)?;

                let user_meta = parse_prefixed_headers(headers, X_AMZ_META_PREFIX);
                if !user_meta.is_empty() {
                    meta = meta.with_user_metadata(user_meta);
                }

                if let Some(v) = parse_header_to_str(headers, X_AMZ_VERSION_ID)? {
                    meta.set_version(v);
                }

                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp)),
        }
    }
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<S3Reader> = {
            Ok(oio::StreamReader::new(S3Reader::new(
                self.clone(),
                ctx.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let output: S3Writers = {
            let writer = S3Writer::new(self.core.clone(), ctx.clone(), path, args.clone());

            let w = if args.append() {
                S3Writers::Two(oio::AppendWriter::new(writer))
            } else {
                // Multipart uploads schedule work through the operation
                // executor supplied by the caller.
                S3Writers::One(oio::MultipartWriter::new(
                    ctx.executor().clone(),
                    writer,
                    args.concurrent(),
                ))
            };

            Ok(w)
        }?;

        Ok(output)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::BatchDeleter<S3Deleter> = {
            Ok(oio::BatchDeleter::new(
                S3Deleter::new(self.core.clone(), ctx.clone()),
                self.core.capability.delete_max_size,
            ))
        }?;

        Ok(output)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let output: S3Listers = {
            let l = if args.versions() || args.deleted() {
                ThreeWays::Three(oio::PageLister::new(S3ObjectVersionsLister::new(
                    self.core.clone(),
                    ctx.clone(),
                    path,
                    args,
                )))
            } else if self.core.disable_list_objects_v2 {
                ThreeWays::One(oio::PageLister::new(S3ListerV1::new(
                    self.core.clone(),
                    ctx.clone(),
                    path,
                    args,
                )))
            } else {
                ThreeWays::Two(oio::PageLister::new(S3ListerV2::new(
                    self.core.clone(),
                    ctx.clone(),
                    path,
                    args,
                )))
            };

            Ok(l)
        }?;

        Ok(output)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<Self::Copier> {
        let output: S3Copiers = {
            let copier = new_s3_copier(self.core.clone(), ctx, from, to, args, opts)?;
            Ok(copier)
        }?;

        Ok(output)
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        let (expire, op) = args.into_parts();
        // We will not send this request out, just for signing.
        let req = match op {
            PresignOperation::Stat(v) => self.core.s3_head_object_request(path, v),
            PresignOperation::Read(range, v) => self.core.s3_get_object_request(path, range, &v),
            PresignOperation::Write(v) => {
                self.core
                    .s3_put_object_request(path, None, &v, Buffer::new())
            }
            PresignOperation::Delete(v) => self.core.s3_delete_object_request(path, &v),
            _ => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        };
        let req = req?;

        let req = self.core.sign_query(ctx, req, expire).await?;

        // We don't need this request anymore, consume it directly.
        let (parts, _) = req.into_parts();

        Ok(RpPresign::new(PresignedRequest::new(
            parts.method,
            parts.uri,
            parts.headers,
        )))
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_bucket() {
        let bucket_cases = vec![
            ("", false, false),
            ("test", false, true),
            ("test.xyz", false, true),
            ("", true, false),
            ("test", true, true),
            ("test.xyz", true, false),
        ];

        for (bucket, enable_virtual_host_style, expected) in bucket_cases {
            let mut b = S3Builder::default();
            b = b.bucket(bucket);
            if enable_virtual_host_style {
                b = b.enable_virtual_host_style();
            }
            assert_eq!(S3Builder::is_bucket_valid(&b.config), expected)
        }
    }

    #[test]
    fn test_build_endpoint() {
        let endpoint_cases = vec![
            Some("s3.amazonaws.com"),
            Some("https://s3.amazonaws.com"),
            Some("https://s3.us-east-2.amazonaws.com"),
            None,
        ];

        for endpoint in &endpoint_cases {
            let mut b = S3Builder::default().bucket("test");
            if let Some(endpoint) = endpoint {
                b = b.endpoint(endpoint);
            }

            let endpoint = S3Builder::build_endpoint(&b.config, "us-east-2");
            assert_eq!(endpoint, "https://s3.us-east-2.amazonaws.com/test");
        }

        for endpoint in &endpoint_cases {
            let mut b = S3Builder::default()
                .bucket("test")
                .enable_virtual_host_style();
            if let Some(endpoint) = endpoint {
                b = b.endpoint(endpoint);
            }

            let endpoint = S3Builder::build_endpoint(&b.config, "us-east-2");
            assert_eq!(endpoint, "https://test.s3.us-east-2.amazonaws.com");
        }
    }

    #[tokio::test]
    async fn test_detect_region() {
        let cases = vec![
            (
                "aws s3 without region in endpoint",
                "https://s3.amazonaws.com",
                "example",
                Some("us-east-1"),
            ),
            (
                "aws s3 with region in endpoint",
                "https://s3.us-east-1.amazonaws.com",
                "example",
                Some("us-east-1"),
            ),
            (
                "oss with public endpoint",
                "https://oss-ap-southeast-1.aliyuncs.com",
                "example",
                Some("oss-ap-southeast-1"),
            ),
            (
                "oss with internal endpoint",
                "https://oss-cn-hangzhou-internal.aliyuncs.com",
                "example",
                Some("oss-cn-hangzhou"),
            ),
            (
                "r2",
                "https://abc.xxxxx.r2.cloudflarestorage.com",
                "example",
                Some("auto"),
            ),
            (
                "invalid service",
                "https://opendal.apache.org",
                "example",
                None,
            ),
        ];

        for (name, endpoint, bucket, expected) in cases {
            let region = S3Builder::detect_region(endpoint, bucket).await;
            assert_eq!(region.as_deref(), expected, "{name}");
        }
    }

    #[tokio::test]
    async fn test_presign_write_preserves_content_type() {
        let backend = S3Builder::default()
            .bucket("test")
            .region("us-east-1")
            .skip_signature()
            .disable_config_load()
            .disable_ec2_metadata()
            .build()
            .expect("build");

        let op = OpWrite::default().with_content_type("application/json");
        let args = OpPresign::new(op, Duration::from_secs(3600));
        let ctx = OperationContext::new();
        let presigned = backend
            .presign(&ctx, "test.txt", args)
            .await
            .expect("presign")
            .into_presigned_request();

        assert_eq!(
            presigned.header().get(http::header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
    }

    #[tokio::test]
    async fn test_presign_stat_encodes_version_id() {
        let backend = S3Builder::default()
            .bucket("test")
            .region("us-east-1")
            .skip_signature()
            .disable_config_load()
            .disable_ec2_metadata()
            .build()
            .expect("build");

        let op = OpStat::default().with_version("a+b/c=d%25&e");
        let args = OpPresign::new(op, Duration::from_secs(3600));
        let ctx = OperationContext::new();
        let presigned = backend
            .presign(&ctx, "test.txt", args)
            .await
            .expect("presign")
            .into_presigned_request();

        assert_eq!(
            presigned.uri().to_string(),
            "https://s3.us-east-1.amazonaws.com/test/test.txt?versionId=a%2Bb/c%3Dd%2525%26e"
        );
    }

    #[tokio::test]
    async fn test_presign_read_encodes_version_id() {
        let backend = S3Builder::default()
            .bucket("test")
            .region("us-east-1")
            .skip_signature()
            .disable_config_load()
            .disable_ec2_metadata()
            .build()
            .expect("build");

        let op = OpRead::default().with_version("a+b/c=d%25&e");
        let args = OpPresign::new(
            PresignOperation::Read(BytesRange::default(), op),
            Duration::from_secs(3600),
        );
        let ctx = OperationContext::new();
        let presigned = backend
            .presign(&ctx, "test.txt", args)
            .await
            .expect("presign")
            .into_presigned_request();

        assert_eq!(
            presigned.uri().to_string(),
            "https://s3.us-east-1.amazonaws.com/test/test.txt?versionId=a%2Bb/c%3Dd%2525%26e"
        );
    }
}
