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

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/opendal/main/website/static/img/logo.svg"
)]
#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]
#![deny(missing_docs)]

pub use opendal_core::*;

#[cfg(feature = "tests")]
pub extern crate opendal_testkit as tests;

/// Install the global defaults provided by the facade crate.
///
/// This function is safe to call multiple times. It registers enabled services
/// and installs the default HTTP transport when the corresponding feature is
/// enabled.
///
/// # Process-wide HTTP transport
///
/// When `auto-register-services` is enabled, a process constructor calls this
/// function before `main`. If a default HTTP transport feature is also enabled,
/// that transport occupies the process-wide default because
/// [`HttpTransporter::install_default`] uses first-installed-wins semantics.
///
/// Applications that need to install their own process-wide transport should
/// disable `auto-register-services`, call [`init_default_registry`] explicitly
/// when URI-based construction is needed, and install the transport themselves.
/// Per-operator transports can be configured regardless of this setting.
pub fn install_default() {
    init_default_registry();

    #[cfg(feature = "http-transport-reqwest")]
    opendal_http_transport_reqwest::install_default();
}

/// Initialize the global [`OperatorRegistry`] with enabled services.
///
/// This function is safe to call multiple times and will only perform
/// initialization once.
///
/// # Notes
///
/// Some bindings link `opendal` as a `staticlib`, where `#[ctor::ctor]`
/// initializers may not be executed due to linker behavior. Those bindings
/// should call this function explicitly before using `Operator::from_uri` or
/// `Operator::via_iter`.
pub fn init_default_registry() {
    static DEFAULT_REGISTRY_INIT: std::sync::Once = std::sync::Once::new();
    DEFAULT_REGISTRY_INIT.call_once(|| init_default_registry_inner(OperatorRegistry::get()));
}

fn init_default_registry_inner(registry: &OperatorRegistry) {
    opendal_core::services::register_memory_service(registry);

    #[cfg(feature = "services-aliyun-drive")]
    opendal_service_aliyun_drive::register_aliyun_drive_service(registry);

    #[cfg(feature = "services-alluxio")]
    opendal_service_alluxio::register_alluxio_service(registry);

    #[cfg(feature = "services-azblob")]
    opendal_service_azblob::register_azblob_service(registry);

    #[cfg(feature = "services-azdls")]
    opendal_service_azdls::register_azdls_service(registry);

    #[cfg(feature = "services-azfile")]
    opendal_service_azfile::register_azfile_service(registry);

    #[cfg(feature = "services-b2")]
    opendal_service_b2::register_b2_service(registry);

    #[cfg(feature = "services-cacache")]
    opendal_service_cacache::register_cacache_service(registry);

    #[cfg(feature = "services-cloudflare-kv")]
    opendal_service_cloudflare_kv::register_cloudflare_kv_service(registry);

    #[cfg(feature = "services-compfs")]
    opendal_service_compfs::register_compfs_service(registry);

    #[cfg(feature = "services-cos")]
    opendal_service_cos::register_cos_service(registry);

    #[cfg(feature = "services-d1")]
    opendal_service_d1::register_d1_service(registry);

    #[cfg(feature = "services-dashmap")]
    opendal_service_dashmap::register_dashmap_service(registry);

    #[cfg(feature = "services-dbfs")]
    opendal_service_dbfs::register_dbfs_service(registry);

    #[cfg(feature = "services-dropbox")]
    opendal_service_dropbox::register_dropbox_service(registry);

    #[cfg(feature = "services-etcd")]
    opendal_service_etcd::register_etcd_service(registry);

    #[cfg(feature = "services-foundationdb")]
    opendal_service_foundationdb::register_foundationdb_service(registry);

    #[cfg(feature = "services-foyer")]
    opendal_service_foyer::register_foyer_service(registry);

    #[cfg(feature = "services-fs")]
    opendal_service_fs::register_fs_service(registry);

    #[cfg(feature = "services-ftp")]
    opendal_service_ftp::register_ftp_service(registry);

    #[cfg(feature = "services-gcs")]
    opendal_service_gcs::register_gcs_service(registry);

    #[cfg(feature = "services-gdrive")]
    opendal_service_gdrive::register_gdrive_service(registry);

    #[cfg(feature = "services-ghac")]
    opendal_service_ghac::register_ghac_service(registry);

    #[cfg(feature = "services-github")]
    opendal_service_github::register_github_service(registry);

    #[cfg(feature = "services-goosefs")]
    opendal_service_goosefs::register_goosefs_service(registry);

    #[cfg(feature = "services-gridfs")]
    opendal_service_gridfs::register_gridfs_service(registry);

    #[cfg(feature = "services-hdfs")]
    opendal_service_hdfs::register_hdfs_service(registry);

    #[cfg(feature = "services-hdfs-native")]
    opendal_service_hdfs_native::register_hdfs_native_service(registry);

    #[cfg(feature = "services-http")]
    opendal_service_http::register_http_service(registry);

    #[cfg(feature = "services-hf")]
    opendal_service_hf::register_hf_service(registry);

    #[cfg(feature = "services-ipfs")]
    opendal_service_ipfs::register_ipfs_service(registry);

    #[cfg(feature = "services-ipmfs")]
    opendal_service_ipmfs::register_ipmfs_service(registry);

    #[cfg(feature = "services-koofr")]
    opendal_service_koofr::register_koofr_service(registry);

    #[cfg(feature = "services-lakefs")]
    opendal_service_lakefs::register_lakefs_service(registry);

    #[cfg(feature = "services-memcached")]
    opendal_service_memcached::register_memcached_service(registry);

    #[cfg(feature = "services-mini-moka")]
    opendal_service_mini_moka::register_mini_moka_service(registry);

    #[cfg(feature = "services-moka")]
    opendal_service_moka::register_moka_service(registry);

    #[cfg(feature = "services-mongodb")]
    opendal_service_mongodb::register_mongodb_service(registry);

    #[cfg(feature = "services-monoiofs")]
    opendal_service_monoiofs::register_monoiofs_service(registry);

    #[cfg(feature = "services-mysql")]
    opendal_service_mysql::register_mysql_service(registry);

    #[cfg(feature = "services-obs")]
    opendal_service_obs::register_obs_service(registry);

    #[cfg(feature = "services-onedrive")]
    opendal_service_onedrive::register_onedrive_service(registry);

    #[cfg(feature = "services-oss")]
    opendal_service_oss::register_oss_service(registry);

    #[cfg(feature = "services-pcloud")]
    opendal_service_pcloud::register_pcloud_service(registry);

    #[cfg(feature = "services-persy")]
    opendal_service_persy::register_persy_service(registry);

    #[cfg(feature = "services-postgresql")]
    opendal_service_postgresql::register_postgresql_service(registry);

    #[cfg(feature = "services-redb")]
    opendal_service_redb::register_redb_service(registry);

    #[cfg(feature = "services-rocksdb")]
    opendal_service_rocksdb::register_rocksdb_service(registry);

    #[cfg(feature = "services-s3")]
    opendal_service_s3::register_s3_service(registry);

    #[cfg(feature = "services-seafile")]
    opendal_service_seafile::register_seafile_service(registry);

    #[cfg(feature = "services-sftp")]
    opendal_service_sftp::register_sftp_service(registry);

    #[cfg(feature = "services-sled")]
    opendal_service_sled::register_sled_service(registry);

    #[cfg(feature = "services-sqlite")]
    opendal_service_sqlite::register_sqlite_service(registry);

    #[cfg(feature = "services-surrealdb")]
    opendal_service_surrealdb::register_surrealdb_service(registry);

    #[cfg(feature = "services-swift")]
    opendal_service_swift::register_swift_service(registry);

    #[cfg(feature = "services-tikv")]
    opendal_service_tikv::register_tikv_service(registry);

    #[cfg(feature = "services-tos")]
    opendal_service_tos::register_tos_service(registry);

    #[cfg(feature = "services-upyun")]
    opendal_service_upyun::register_upyun_service(registry);

    #[cfg(feature = "services-vercel-artifacts")]
    opendal_service_vercel_artifacts::register_vercel_artifacts_service(registry);

    #[cfg(feature = "services-vercel-blob")]
    opendal_service_vercel_blob::register_vercel_blob_service(registry);

    #[cfg(feature = "services-webdav")]
    opendal_service_webdav::register_webdav_service(registry);

    #[cfg(feature = "services-webhdfs")]
    opendal_service_webhdfs::register_webhdfs_service(registry);

    #[cfg(feature = "services-yandex-disk")]
    opendal_service_yandex_disk::register_yandex_disk_service(registry);

    #[cfg(all(target_arch = "wasm32", feature = "services-opfs"))]
    opendal_service_opfs::register_opfs_service(registry);

    #[cfg(any(feature = "services-redis", feature = "services-redis-native-tls"))]
    opendal_service_redis::register_redis_service(registry);
}

#[cfg(feature = "auto-register-services")]
#[ctor::ctor(unsafe)]
fn register_default_operator_registry() {
    install_default();
}

/// Service builders enabled through `services-*` Cargo features.
///
/// [`Memory`](services::Memory) is always available. Other builders are
/// re-exported when their matching facade feature is enabled; for example,
/// `services-s3` enables [`S3`](services::S3).
///
/// Pass a configured builder to [`Operator::new`]. The facade automatically
/// registers enabled services for [`Operator::from_uri`] and
/// [`Operator::via_iter`] when the `auto-register-services` feature is enabled.
pub mod services {
    pub use opendal_core::services::*;
    #[cfg(feature = "services-aliyun-drive")]
    pub use opendal_service_aliyun_drive::*;
    #[cfg(feature = "services-alluxio")]
    pub use opendal_service_alluxio::*;
    #[cfg(feature = "services-azblob")]
    pub use opendal_service_azblob::*;
    #[cfg(feature = "services-azdls")]
    pub use opendal_service_azdls::*;
    #[cfg(feature = "services-azfile")]
    pub use opendal_service_azfile::*;
    #[cfg(feature = "services-b2")]
    pub use opendal_service_b2::*;
    #[cfg(feature = "services-cacache")]
    pub use opendal_service_cacache::*;
    #[cfg(feature = "services-cloudflare-kv")]
    pub use opendal_service_cloudflare_kv::*;
    #[cfg(feature = "services-compfs")]
    pub use opendal_service_compfs::*;
    #[cfg(feature = "services-cos")]
    pub use opendal_service_cos::*;
    #[cfg(feature = "services-d1")]
    pub use opendal_service_d1::*;
    #[cfg(feature = "services-dashmap")]
    pub use opendal_service_dashmap::*;
    #[cfg(feature = "services-dbfs")]
    pub use opendal_service_dbfs::*;
    #[cfg(feature = "services-dropbox")]
    pub use opendal_service_dropbox::*;
    #[cfg(feature = "services-etcd")]
    pub use opendal_service_etcd::*;
    #[cfg(feature = "services-foundationdb")]
    pub use opendal_service_foundationdb::*;
    #[cfg(feature = "services-foyer")]
    pub use opendal_service_foyer::*;
    #[cfg(feature = "services-fs")]
    pub use opendal_service_fs::*;
    #[cfg(feature = "services-ftp")]
    pub use opendal_service_ftp::*;
    #[cfg(feature = "services-gcs")]
    pub use opendal_service_gcs::*;
    #[cfg(feature = "services-gdrive")]
    pub use opendal_service_gdrive::*;
    #[cfg(feature = "services-ghac")]
    pub use opendal_service_ghac::*;
    #[cfg(feature = "services-github")]
    pub use opendal_service_github::*;
    #[cfg(feature = "services-goosefs")]
    pub use opendal_service_goosefs::*;
    #[cfg(feature = "services-gridfs")]
    pub use opendal_service_gridfs::*;
    #[cfg(feature = "services-hdfs")]
    pub use opendal_service_hdfs::*;
    #[cfg(feature = "services-hdfs-native")]
    pub use opendal_service_hdfs_native::*;
    #[cfg(feature = "services-hf")]
    pub use opendal_service_hf::*;
    #[cfg(feature = "services-http")]
    pub use opendal_service_http::*;
    #[cfg(feature = "services-ipfs")]
    pub use opendal_service_ipfs::*;
    #[cfg(feature = "services-ipmfs")]
    pub use opendal_service_ipmfs::*;
    #[cfg(feature = "services-koofr")]
    pub use opendal_service_koofr::*;
    #[cfg(feature = "services-lakefs")]
    pub use opendal_service_lakefs::*;
    #[cfg(feature = "services-memcached")]
    pub use opendal_service_memcached::*;
    #[cfg(feature = "services-mini-moka")]
    pub use opendal_service_mini_moka::*;
    #[cfg(feature = "services-moka")]
    pub use opendal_service_moka::*;
    #[cfg(feature = "services-mongodb")]
    pub use opendal_service_mongodb::*;
    #[cfg(feature = "services-monoiofs")]
    pub use opendal_service_monoiofs::*;
    #[cfg(feature = "services-mysql")]
    pub use opendal_service_mysql::*;
    #[cfg(feature = "services-obs")]
    pub use opendal_service_obs::*;
    #[cfg(feature = "services-onedrive")]
    pub use opendal_service_onedrive::*;
    #[cfg(all(target_arch = "wasm32", feature = "services-opfs"))]
    pub use opendal_service_opfs::*;
    #[cfg(feature = "services-oss")]
    pub use opendal_service_oss::*;
    #[cfg(feature = "services-pcloud")]
    pub use opendal_service_pcloud::*;
    #[cfg(feature = "services-persy")]
    pub use opendal_service_persy::*;
    #[cfg(feature = "services-postgresql")]
    pub use opendal_service_postgresql::*;
    #[cfg(feature = "services-redb")]
    pub use opendal_service_redb::*;
    #[cfg(feature = "services-redis")]
    pub use opendal_service_redis::*;
    #[cfg(feature = "services-rocksdb")]
    pub use opendal_service_rocksdb::*;
    #[cfg(feature = "services-s3")]
    pub use opendal_service_s3::*;
    #[cfg(feature = "services-seafile")]
    pub use opendal_service_seafile::*;
    #[cfg(feature = "services-sftp")]
    pub use opendal_service_sftp::*;
    #[cfg(feature = "services-sled")]
    pub use opendal_service_sled::*;
    #[cfg(feature = "services-sqlite")]
    pub use opendal_service_sqlite::*;
    #[cfg(feature = "services-surrealdb")]
    pub use opendal_service_surrealdb::*;
    #[cfg(feature = "services-swift")]
    pub use opendal_service_swift::*;
    #[cfg(feature = "services-tikv")]
    pub use opendal_service_tikv::*;
    #[cfg(feature = "services-tos")]
    pub use opendal_service_tos::*;
    #[cfg(feature = "services-upyun")]
    pub use opendal_service_upyun::*;
    #[cfg(feature = "services-vercel-artifacts")]
    pub use opendal_service_vercel_artifacts::*;
    #[cfg(feature = "services-vercel-blob")]
    pub use opendal_service_vercel_blob::*;
    #[cfg(feature = "services-webdav")]
    pub use opendal_service_webdav::*;
    #[cfg(feature = "services-webhdfs")]
    pub use opendal_service_webhdfs::*;
    #[cfg(feature = "services-yandex-disk")]
    pub use opendal_service_yandex_disk::*;
}

/// Layers enabled through `layers-*` Cargo features.
///
/// A layer adds cross-service behavior to an [`Operator`]. Enable the matching
/// facade feature, construct the layer, and pass it to [`Operator::layer`].
/// Layer order is observable, so review each layer's composition notes when
/// combining retries, timeouts, caching, or observability.
pub mod layers {
    pub use opendal_core::layers::*;
    #[cfg(feature = "layers-async-backtrace")]
    pub use opendal_layer_async_backtrace::*;
    #[cfg(feature = "layers-await-tree")]
    pub use opendal_layer_await_tree::*;
    #[cfg(feature = "layers-capability-check")]
    pub use opendal_layer_capability_check::*;
    #[cfg(feature = "layers-chaos")]
    pub use opendal_layer_chaos::*;
    #[cfg(feature = "layers-concurrent-limit")]
    pub use opendal_layer_concurrent_limit::*;
    #[cfg(all(target_os = "linux", feature = "layers-dtrace"))]
    pub use opendal_layer_dtrace::*;
    #[cfg(feature = "layers-fastmetrics")]
    pub use opendal_layer_fastmetrics::*;
    #[cfg(feature = "layers-fastrace")]
    pub use opendal_layer_fastrace::*;
    #[cfg(feature = "layers-foyer")]
    pub use opendal_layer_foyer::*;
    #[cfg(feature = "layers-hotpath")]
    pub use opendal_layer_hotpath::*;
    #[cfg(feature = "layers-immutable-index")]
    pub use opendal_layer_immutable_index::*;
    #[cfg(feature = "layers-logging")]
    pub use opendal_layer_logging::*;
    #[cfg(feature = "layers-metrics")]
    pub use opendal_layer_metrics::*;
    #[cfg(feature = "layers-mime-guess")]
    pub use opendal_layer_mime_guess::*;
    #[cfg(feature = "layers-otel-metrics")]
    pub use opendal_layer_otelmetrics::*;
    #[cfg(feature = "layers-otel-trace")]
    pub use opendal_layer_oteltrace::*;
    #[cfg(feature = "layers-prometheus")]
    pub use opendal_layer_prometheus::*;
    #[cfg(feature = "layers-prometheus-client")]
    pub use opendal_layer_prometheus_client::*;
    #[cfg(feature = "layers-retry")]
    pub use opendal_layer_retry::*;
    #[cfg(feature = "layers-route")]
    pub use opendal_layer_route::*;
    #[cfg(feature = "layers-tail-cut")]
    pub use opendal_layer_tail_cut::*;
    #[cfg(feature = "layers-throttle")]
    pub use opendal_layer_throttle::*;
    #[cfg(feature = "layers-timeout")]
    pub use opendal_layer_timeout::*;
    #[cfg(feature = "layers-tracing")]
    pub use opendal_layer_tracing::*;
}
