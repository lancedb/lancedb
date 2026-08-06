// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Dataset API extensions for MemWAL.
//!
//! This module provides the user-facing API for initializing and using MemWAL
//! on a Dataset.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::DataType;
use async_trait::async_trait;
use lance_core::{Error, Result};
use lance_index::mem_wal::{MEM_WAL_INDEX_NAME, MemWalIndexDetails, ShardingField, ShardingSpec};
use lance_index::vector::hnsw::builder::HnswBuildParams;
use uuid::Uuid;

use crate::Dataset;
use crate::dataset::CommitBuilder;
use crate::dataset::transaction::{Operation, Transaction};
use crate::index::DatasetIndexExt;
use crate::index::DatasetIndexInternalExt;
use crate::index::mem_wal::{load_mem_wal_index_details, new_mem_wal_index_meta};

use super::ShardWriterConfig;
use super::index::{MemIndexKind, unsupported_index_type};
use super::scanner::sstable_cache::open_sstable;
use super::scanner::{DatasetCache, ShardSnapshot};
use super::util::derived_store_params;
use super::write::MemIndexConfig;
use super::write::ShardWriter;

/// Spec id of the sole sharding spec installed by [`InitializeMemWalBuilder`].
const SHARDING_SPEC_ID: u32 = 1;

/// Field id, within the sharding spec, of the derived shard-routing value.
const SHARDING_FIELD_ID: &str = "bucket";

/// Result type of the derived shard-routing value.
const SHARDING_RESULT_TYPE: &str = "int32";

/// Transform name for [`InitializeMemWalBuilder::bucket_sharding`]. Matches
/// Iceberg's `bucket(col, N)` partition transform name.
const BUCKET_TRANSFORM: &str = "bucket";

/// Transform name for [`InitializeMemWalBuilder::unsharded`]: every row maps to
/// a single shard.
const UNSHARDED_TRANSFORM: &str = "unsharded";

/// Transform name for [`InitializeMemWalBuilder::identity_sharding`]: the shard
/// value is the raw value of the source column.
const IDENTITY_TRANSFORM: &str = "identity";

/// Parameter key holding the bucket count `N` on the bucket transform.
const NUM_BUCKETS_PARAM: &str = "num_buckets";

/// Inclusive upper bound for `num_buckets`. Bounds the number of distinct
/// MemWAL shards a single bucket spec can address, which caps how many shard
/// manifests the dataset has to manage.
const MAX_NUM_BUCKETS: u32 = 1024;

/// How writes are partitioned into MemWAL shards.
#[derive(Debug)]
enum Sharding {
    /// No sharding spec is recorded; shards are managed manually.
    Manual,
    /// A single shard; every row is routed to it.
    Unsharded,
    /// Hash-bucket a shard key into `num_buckets` shards.
    Bucket { column: String, num_buckets: u32 },
    /// Shard by the raw value of `column` (identity transform).
    Identity { column: String },
}

/// Builder for initializing MemWAL on a [`Dataset`].
///
/// Created by [`DatasetMemWalExt::initialize_mem_wal`]. Choose a sharding
/// strategy and the indexes to maintain, then call [`execute`](Self::execute).
///
/// # Example
///
/// ```ignore
/// use lance::dataset::mem_wal::DatasetMemWalExt;
///
/// dataset
///     .initialize_mem_wal()
///     .bucket_sharding("id", 16)
///     .maintained_indexes(["id_btree"])
///     .execute()
///     .await?;
/// ```
#[must_use = "InitializeMemWalBuilder does nothing unless `.execute()` is awaited"]
pub struct InitializeMemWalBuilder<'a> {
    dataset: &'a mut Dataset,
    sharding: Sharding,
    maintained_indexes: Vec<String>,
    writer_config_defaults: HashMap<String, String>,
}

impl<'a> InitializeMemWalBuilder<'a> {
    fn new(dataset: &'a mut Dataset) -> Self {
        Self {
            dataset,
            sharding: Sharding::Manual,
            maintained_indexes: Vec::new(),
            writer_config_defaults: HashMap::new(),
        }
    }

    /// Route every row to a single MemWAL shard.
    pub fn unsharded(mut self) -> Self {
        self.sharding = Sharding::Unsharded;
        self
    }

    /// Hash-bucket `column` into `num_buckets` shards.
    ///
    /// `column` must name a scalar dataset column that can be hash-bucketed.
    /// `num_buckets` must be in `[1, 1024]`. These constraints are validated
    /// by [`execute`](Self::execute).
    pub fn bucket_sharding(mut self, column: impl Into<String>, num_buckets: u32) -> Self {
        self.sharding = Sharding::Bucket {
            column: column.into(),
            num_buckets,
        };
        self
    }

    /// Shard by the raw value of `column` (the identity transform).
    ///
    /// Each distinct value of `column` becomes its own shard; use this when the
    /// data is already partitioned by that column. For primary-key tables, the
    /// caller is responsible for ensuring every primary key maps consistently
    /// to a single value of `column`. `column` must be a scalar column that
    /// exists on the dataset; it is validated by [`execute`](Self::execute).
    pub fn identity_sharding(mut self, column: impl Into<String>) -> Self {
        self.sharding = Sharding::Identity {
            column: column.into(),
        };
        self
    }

    /// Set the base-table indexes to maintain in MemTables, replacing any
    /// previously set list.
    ///
    /// Each name must reference an index that already exists on the dataset.
    /// The primary key btree, when present, is maintained implicitly and must
    /// not be listed.
    pub fn maintained_indexes<I, S>(mut self, indexes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.maintained_indexes = indexes.into_iter().map(Into::into).collect();
        self
    }

    /// Record `config` as the default `ShardWriter` configuration.
    ///
    /// Every tunable field is persisted into the MemWAL index so that all
    /// writers — across processes and restarts — start from the same
    /// defaults. Shard identity (`shard_id`, `shard_spec_id`) is not a
    /// configuration default and is not recorded. These remain defaults only:
    /// an individual writer may still override any value at runtime in its own
    /// (non-persisted) `ShardWriterConfig`.
    ///
    /// Merges into any defaults already set; a key set via
    /// [`add_writer_config_default`](Self::add_writer_config_default) afterwards wins.
    pub fn writer_config_defaults(mut self, config: ShardWriterConfig) -> Self {
        self.writer_config_defaults
            .extend(writer_config_to_defaults(&config));
        self
    }

    /// Record a single arbitrary writer-configuration default.
    ///
    /// Use this for keys not covered by
    /// [`writer_config_defaults`](Self::writer_config_defaults).
    pub fn add_writer_config_default(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.writer_config_defaults.insert(key.into(), value.into());
        self
    }

    /// Initialize MemWAL on the dataset, committing the MemWAL system index.
    ///
    /// Fails if any maintained index does not exist, if the selected sharding
    /// configuration is invalid, or if MemWAL is already initialized.
    pub async fn execute(self) -> Result<()> {
        let Self {
            dataset,
            sharding,
            maintained_indexes,
            writer_config_defaults,
        } = self;

        // Resolve (and validate) the sharding choice before any I/O.
        let (sharding_specs, num_shards) = resolve_sharding(dataset, sharding)?;

        let indices = dataset.load_indices().await?;
        for index_name in &maintained_indexes {
            if !indices.iter().any(|idx| &idx.name == index_name) {
                return Err(Error::invalid_input(format!(
                    "Index '{}' not found on dataset. maintained_indexes must reference existing indexes.",
                    index_name
                )));
            }
        }
        if indices.iter().any(|idx| idx.name == MEM_WAL_INDEX_NAME) {
            return Err(Error::invalid_input(
                "MemWAL is already initialized on this dataset.",
            ));
        }

        let details = MemWalIndexDetails {
            num_shards,
            sharding_specs,
            maintained_indexes,
            writer_config_defaults,
            ..Default::default()
        };

        let index_meta = new_mem_wal_index_meta(dataset.manifest.version, details)?;
        let transaction = Transaction::new(
            dataset.manifest.version,
            Operation::CreateIndex {
                new_indices: vec![index_meta],
                removed_indices: vec![],
            },
            None,
        );

        let new_dataset = CommitBuilder::new(Arc::new(dataset.clone()))
            .execute(transaction)
            .await?;
        *dataset = new_dataset;

        Ok(())
    }
}

/// Resolve a [`Sharding`] choice into the sharding specs and shard count to
/// persist in [`MemWalIndexDetails`].
fn resolve_sharding(dataset: &Dataset, sharding: Sharding) -> Result<(Vec<ShardingSpec>, u32)> {
    match sharding {
        Sharding::Manual => Ok((Vec::new(), 0)),
        Sharding::Unsharded => Ok((vec![unsharded_sharding_spec()], 1)),
        Sharding::Bucket {
            column,
            num_buckets,
        } => Ok((
            vec![bucket_sharding_spec(dataset, &column, num_buckets)?],
            num_buckets,
        )),
        Sharding::Identity { column } => Ok((vec![identity_sharding_spec(dataset, &column)?], 0)),
    }
}

/// Build the sharding spec for [`InitializeMemWalBuilder::unsharded`].
fn unsharded_sharding_spec() -> ShardingSpec {
    ShardingSpec {
        spec_id: SHARDING_SPEC_ID,
        fields: vec![ShardingField {
            field_id: SHARDING_FIELD_ID.to_string(),
            source_ids: Vec::new(),
            transform: Some(UNSHARDED_TRANSFORM.to_string()),
            expression: None,
            result_type: SHARDING_RESULT_TYPE.to_string(),
            parameters: HashMap::new(),
        }],
    }
}

/// Build the sharding spec for [`InitializeMemWalBuilder::bucket_sharding`].
fn bucket_sharding_spec(dataset: &Dataset, column: &str, num_buckets: u32) -> Result<ShardingSpec> {
    if num_buckets == 0 || num_buckets > MAX_NUM_BUCKETS {
        return Err(Error::invalid_input(format!(
            "bucket_sharding: num_buckets must be in [1, {}], got {}",
            MAX_NUM_BUCKETS, num_buckets
        )));
    }

    let source_field = dataset.schema().field(column).ok_or_else(|| {
        Error::invalid_input(format!(
            "bucket_sharding: column '{}' not found on the dataset",
            column
        ))
    })?;

    let data_type = source_field.data_type();
    if !is_bucket_sharding_supported_type(&data_type) {
        return Err(Error::invalid_input(format!(
            "bucket_sharding: column '{}' has type {:?}, which cannot be used as a shard key",
            column, data_type
        )));
    }

    Ok(ShardingSpec {
        spec_id: SHARDING_SPEC_ID,
        fields: vec![ShardingField {
            field_id: SHARDING_FIELD_ID.to_string(),
            source_ids: vec![source_field.id],
            transform: Some(BUCKET_TRANSFORM.to_string()),
            expression: None,
            result_type: SHARDING_RESULT_TYPE.to_string(),
            parameters: HashMap::from([(NUM_BUCKETS_PARAM.to_string(), num_buckets.to_string())]),
        }],
    })
}

/// Build the sharding spec for [`InitializeMemWalBuilder::identity_sharding`].
fn identity_sharding_spec(dataset: &Dataset, column: &str) -> Result<ShardingSpec> {
    let field = dataset.schema().field(column).ok_or_else(|| {
        Error::invalid_input(format!(
            "identity_sharding: column '{}' not found on the dataset",
            column
        ))
    })?;

    let data_type = field.data_type();
    let result_type = scalar_result_type(&data_type).ok_or_else(|| {
        Error::invalid_input(format!(
            "identity_sharding: column '{}' has type {:?}, which cannot be used as a shard key",
            column, data_type
        ))
    })?;

    Ok(ShardingSpec {
        spec_id: SHARDING_SPEC_ID,
        fields: vec![ShardingField {
            field_id: SHARDING_FIELD_ID.to_string(),
            source_ids: vec![field.id],
            transform: Some(IDENTITY_TRANSFORM.to_string()),
            expression: None,
            result_type: result_type.to_string(),
            parameters: HashMap::new(),
        }],
    })
}

fn is_bucket_sharding_supported_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
    )
}

/// The Arrow type name for a scalar column usable as a shard key, or `None`
/// for types that cannot be a shard key.
fn scalar_result_type(data_type: &DataType) -> Option<&'static str> {
    Some(match data_type {
        DataType::Int8 => "int8",
        DataType::Int16 => "int16",
        DataType::Int32 => "int32",
        DataType::Int64 => "int64",
        DataType::UInt8 => "uint8",
        DataType::UInt16 => "uint16",
        DataType::UInt32 => "uint32",
        DataType::UInt64 => "uint64",
        DataType::Utf8 => "utf8",
        DataType::LargeUtf8 => "large_utf8",
        DataType::Boolean => "boolean",
        _ => return None,
    })
}

/// Extract the tunable defaults from a [`ShardWriterConfig`] into the persisted
/// string map. Shard identity (`shard_id`, `shard_spec_id`) is not a default.
/// `Duration` knobs are recorded in milliseconds with a `_ms` key suffix.
fn writer_config_to_defaults(config: &ShardWriterConfig) -> HashMap<String, String> {
    let mut defaults = HashMap::from([
        (
            "durable_write".to_string(),
            config.durable_write.to_string(),
        ),
        (
            "max_wal_buffer_size".to_string(),
            config.max_wal_buffer_size.to_string(),
        ),
        (
            "max_wal_persist_retries".to_string(),
            config.max_wal_persist_retries.to_string(),
        ),
        (
            "wal_persist_retry_base_delay_ms".to_string(),
            config.wal_persist_retry_base_delay.as_millis().to_string(),
        ),
        (
            "max_memtable_size".to_string(),
            config.max_memtable_size.to_string(),
        ),
        (
            "max_memtable_rows".to_string(),
            config.max_memtable_rows.to_string(),
        ),
        (
            "max_memtable_batches".to_string(),
            config.max_memtable_batches.to_string(),
        ),
        (
            "manifest_scan_batch_size".to_string(),
            config.manifest_scan_batch_size.to_string(),
        ),
        (
            "max_unflushed_memtable_bytes".to_string(),
            config.max_unflushed_memtable_bytes.to_string(),
        ),
        (
            "backpressure_log_interval_ms".to_string(),
            config.backpressure_log_interval.as_millis().to_string(),
        ),
        (
            "enable_memtable".to_string(),
            config.enable_memtable.to_string(),
        ),
    ]);
    if let Some(interval) = config.max_wal_flush_interval {
        defaults.insert(
            "max_wal_flush_interval_ms".to_string(),
            interval.as_millis().to_string(),
        );
    }
    if let Some(interval) = config.stats_log_interval {
        defaults.insert(
            "stats_log_interval_ms".to_string(),
            interval.as_millis().to_string(),
        );
    }
    // Per-index HNSW build params are recorded under `hnsw.<index>.<field>` keys.
    for (index_name, params) in &config.hnsw_params {
        defaults.insert(format!("hnsw.{index_name}.num_edges"), params.m.to_string());
        defaults.insert(
            format!("hnsw.{index_name}.ef_construction"),
            params.ef_construction.to_string(),
        );
        defaults.insert(
            format!("hnsw.{index_name}.max_level"),
            params.max_level.to_string(),
        );
    }
    defaults
}

/// Extension trait for Dataset to support MemWAL operations.
#[async_trait]
pub trait DatasetMemWalExt {
    /// Begin initializing MemWAL on this dataset.
    ///
    /// Returns an [`InitializeMemWalBuilder`]; configure the sharding strategy
    /// and maintained indexes, then call [`InitializeMemWalBuilder::execute`].
    fn initialize_mem_wal(&mut self) -> InitializeMemWalBuilder<'_>;

    /// Return the MemWAL index details for this dataset, if MemWAL is initialized.
    async fn mem_wal_index_details(&self) -> Result<Option<MemWalIndexDetails>> {
        Ok(None)
    }

    /// List current MemWAL shard IDs from object storage directory listing.
    async fn list_mem_wal_latest_shard_ids(&self) -> Result<Vec<Uuid>> {
        Ok(Vec::new())
    }

    /// Prewarm the SSTables of the given MemWAL shards into this
    /// dataset's session caches.
    ///
    /// For every SSTable in `snapshots`, opens the generation's
    /// on-disk dataset (populating the session's metadata/index caches, and the
    /// optional `cache` of opened `Arc<Dataset>`s) and prewarms each of its
    /// indexes. Opens run concurrently.
    ///
    /// The caller chooses how to enumerate the shards — list the `_mem_wal`
    /// directory (e.g. [`Self::list_mem_wal_latest_shard_ids`] then read each
    /// shard manifest), or read the MemWAL index shard snapshots — and passes
    /// the resulting [`ShardSnapshot`]s here. Prewarming is purely a cache
    /// optimization; correctness never depends on it, so passing a generation
    /// that has since been retired is harmless.
    async fn prewarm_mem_wal(
        &self,
        _snapshots: &[ShardSnapshot],
        _cache: Option<&Arc<dyn DatasetCache>>,
    ) -> Result<()> {
        Ok(())
    }

    /// Get a ShardWriter for the specified shard.
    ///
    /// Automatically loads index configurations from the MemWalIndex
    /// and creates the appropriate in-memory indexes.
    ///
    /// # Arguments
    ///
    /// * `shard_id` - UUID identifying this shard
    /// * `config` - Writer configuration (durability, buffer sizes, etc.)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let writer = dataset.mem_wal_writer(
    ///     Uuid::new_v4(),
    ///     ShardWriterConfig::default(),
    /// ).await?;
    /// writer.put(vec![batch1, batch2]).await?;
    /// ```
    async fn mem_wal_writer(
        &self,
        shard_id: Uuid,
        config: ShardWriterConfig,
    ) -> Result<ShardWriter>;
}

/// Prewarm every index of `dataset` into its session caches. A no-op when the
/// dataset has no indexes; duplicate index names are warmed once.
async fn prewarm_all_indexes(dataset: &Dataset) -> Result<()> {
    let indices = dataset.load_indices().await?;
    let mut seen = std::collections::HashSet::new();
    for index in indices.iter() {
        if seen.insert(index.name.as_str()) {
            dataset.prewarm_index(&index.name).await?;
        }
    }
    Ok(())
}

#[async_trait]
impl DatasetMemWalExt for Dataset {
    fn initialize_mem_wal(&mut self) -> InitializeMemWalBuilder<'_> {
        InitializeMemWalBuilder::new(self)
    }

    async fn mem_wal_index_details(&self) -> Result<Option<MemWalIndexDetails>> {
        let Some(index_meta) = self.load_index_by_name(MEM_WAL_INDEX_NAME).await? else {
            return Ok(None);
        };

        load_mem_wal_index_details(index_meta).map(Some)
    }

    async fn list_mem_wal_latest_shard_ids(&self) -> Result<Vec<Uuid>> {
        let prefix = super::util::mem_wal_path(&self.branch_location().path);
        let object_store = self.object_store(None).await?;
        let list_result = object_store
            .inner
            .list_with_delimiter(Some(&prefix))
            .await
            .map_err(|e| {
                Error::io(format!(
                    "failed to list MemWAL shard directories at {}: {}",
                    prefix, e
                ))
            })?;
        let mut ids = Vec::new();
        for shard_prefix in list_result.common_prefixes {
            if let Some(name) = shard_prefix.filename()
                && let Ok(shard_id) = Uuid::parse_str(name)
            {
                ids.push(shard_id);
            }
        }
        ids.sort();
        Ok(ids)
    }

    async fn prewarm_mem_wal(
        &self,
        snapshots: &[ShardSnapshot],
        cache: Option<&Arc<dyn DatasetCache>>,
    ) -> Result<()> {
        let session = self.session();
        // Every open below targets a generation URI, never the base's own.
        let store_params = self.store_params().map(derived_store_params);
        // Resolve SSTable paths exactly as the LSM collector does, so the
        // session/cache entries we warm key-match the paths later lookups open.
        let base_path = self.uri().trim_end_matches('/').to_string();
        let opens = snapshots
            .iter()
            .flat_map(|snapshot| {
                let shard_id = snapshot.shard_id;
                let base_path = &base_path;
                let session = &session;
                let store_params = &store_params;
                snapshot.sstables.iter().map(move |sstable| {
                    let path = format!("{}/_mem_wal/{}/{}", base_path, shard_id, sstable.path);
                    async move {
                        let dataset =
                            open_sstable(&path, Some(session), store_params.as_ref(), cache, None)
                                .await?;
                        prewarm_all_indexes(&dataset).await
                    }
                })
            })
            .collect::<Vec<_>>();
        futures::future::try_join_all(opens).await?;
        Ok(())
    }

    async fn mem_wal_writer(
        &self,
        shard_id: Uuid,
        mut config: ShardWriterConfig,
    ) -> Result<ShardWriter> {
        use lance_index::metrics::NoOpMetricsCollector;

        // Load MemWalIndex to get maintained_indexes
        let mem_wal_index = self
            .open_mem_wal_index(&NoOpMetricsCollector)
            .await?
            .ok_or_else(|| {
                Error::invalid_input(
                    "MemWAL is not initialized on this dataset. Call initialize_mem_wal() first.",
                )
            })?;

        // Get maintained_indexes from the MemWalIndex details
        let maintained_indexes = &mem_wal_index.details.maintained_indexes;

        // Load index configs for each maintained index
        let mut index_configs = Vec::new();
        for index_name in maintained_indexes {
            // A maintained index can split into multiple physical segments
            // (e.g. `optimize_indices(append)` deltas), which the singular
            // `load_index_by_name` rejects. Every segment carries the same
            // type and params, so take the first match.
            let index_meta = self
                .load_indices_by_name(index_name)
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    Error::invalid_input(format!(
                        "Index '{}' from maintained_indexes not found on dataset",
                        index_name
                    ))
                })?;

            // Detect index kind and create appropriate config
            let type_url = index_meta
                .index_details
                .as_ref()
                .map(|d| d.type_url.as_str())
                .unwrap_or("");

            let kind = MemIndexKind::from_type_url(type_url)
                .ok_or_else(|| unsupported_index_type(type_url))?;

            // Exhaustive: a new kind must be built here, or callers filtering on
            // `is_maintainable_index_type` would admit an index this writer
            // cannot open, failing every memtable claim.
            match kind {
                MemIndexKind::BTree => {
                    index_configs.push(MemIndexConfig::btree_from_metadata(
                        &index_meta,
                        self.schema(),
                    )?);
                }
                MemIndexKind::Fts => {
                    index_configs.push(MemIndexConfig::fts_from_metadata(
                        &index_meta,
                        self.schema(),
                    )?);
                }
                MemIndexKind::Hnsw => {
                    let hnsw_params = config.hnsw_params.get(index_name).cloned();
                    let vector_config =
                        load_vector_index_config(self, index_name, &index_meta, hnsw_params)
                            .await?;
                    index_configs.push(vector_config);
                }
            };
        }

        // Set shard_id in config
        config.shard_id = shard_id;

        // Inject the dataset's store params + session so the flusher opens the
        // base + generations with the same store the base was resolved with.
        config.store_params = self.store_params().cloned();
        config.session = Some(self.session());

        // Reuse the dataset's own object store + base path; `ObjectStore::from_uri`
        // would discard the store params the dataset was opened with, signing WAL
        // writes with the ambient identity. Mirrors `list_mem_wal_latest_shard_ids`.
        let base_uri = self.uri();
        let store = self.object_store(None).await?;
        let base_path = self.branch_location().path;

        // Create ShardWriter
        ShardWriter::open(
            store,
            base_path,
            base_uri,
            config,
            Arc::new(self.schema().into()),
            index_configs,
        )
        .await
    }
}

/// Build an in-memory HNSW vector index configuration from a base-table
/// vector index entry.
///
/// HNSW does not require any centroids/codebook from the base table — it is
/// self-contained. The only thing we read from the base index is the distance
/// type (so the in-memory index uses the same metric as the base). If the
/// base index is unreadable for some reason, we default to L2.
async fn load_vector_index_config(
    dataset: &Dataset,
    index_name: &str,
    index_meta: &lance_table::format::IndexMetadata,
    hnsw_params: Option<HnswBuildParams>,
) -> Result<MemIndexConfig> {
    use lance_index::metrics::NoOpMetricsCollector;

    let field_id = index_meta.fields.first().ok_or_else(|| {
        Error::invalid_input(format!("Vector index '{}' has no fields", index_name))
    })?;

    let field = dataset.schema().field_by_id(*field_id).ok_or_else(|| {
        Error::invalid_input(format!("Field not found for vector index '{}'", index_name))
    })?;
    let column = field.name.clone();

    // Inherit the base table's distance type so the in-memory index and the
    // base index produce comparable distances. Surface the open error
    // instead of silently defaulting to L2 — flushed `IVF_HNSW_SQ` files
    // bake this metric into their on-disk metadata, so a wrong default would
    // be durable corruption.
    let distance_type = dataset
        .open_vector_index(&column, &index_meta.uuid, &NoOpMetricsCollector)
        .await
        .map_err(|e| {
            Error::invalid_input(format!(
                "Failed to open base vector index '{}' to inherit distance type: {}",
                index_name, e
            ))
        })?
        .metric_type();

    Ok(match hnsw_params {
        Some(params) => MemIndexConfig::hnsw_with_params(
            index_name.to_string(),
            *field_id,
            column,
            distance_type,
            params,
        ),
        None => MemIndexConfig::hnsw(index_name.to_string(), *field_id, column, distance_type),
    })
}

#[cfg(test)]
mod tests {
    use super::super::scanner::SsTableCache;
    use super::*;

    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use lance_index::IndexType;
    use lance_index::scalar::ScalarIndexParams;

    use crate::dataset::WriteParams;

    fn id_v_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::Int32, true),
        ]))
    }

    fn id_v_batch(schema: &Arc<ArrowSchema>, ids: &[i32]) -> RecordBatch {
        let vs: Vec<i32> = ids.iter().map(|i| i * 10).collect();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(Int32Array::from(vs)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_prewarm_mem_wal_opens_and_warms_indexes() {
        // `prewarm_mem_wal` opens each SSTable (into the base
        // dataset's session + the supplied cache) and warms its indexes. We
        // place an SSTable dataset with a BTree index at the
        // canonical `{base}/_mem_wal/{shard}/{folder}` path, prewarm it via a
        // snapshot, and assert the generation is cached and its index loadable.
        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let schema = id_v_schema();

        // Base dataset (1-row sentinel).
        let reader = RecordBatchIterator::new([Ok(id_v_batch(&schema, &[-1]))], schema.clone());
        let base = Dataset::write(reader, &base_uri, Some(WriteParams::default()))
            .await
            .unwrap();

        // SSTable with a BTree index on `id`.
        let shard_id = Uuid::new_v4();
        let folder = "deadbeef_gen_1";
        let gen_uri = format!("{}/_mem_wal/{}/{}", base_uri, shard_id, folder);
        let reader =
            RecordBatchIterator::new([Ok(id_v_batch(&schema, &[1, 2, 3]))], schema.clone());
        let mut gen_ds = Dataset::write(reader, &gen_uri, Some(WriteParams::default()))
            .await
            .unwrap();
        gen_ds
            .create_index(
                &["id"],
                IndexType::BTree,
                Some("id_idx".to_string()),
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();

        let snapshot = ShardSnapshot::new(shard_id)
            .with_current_generation(2)
            .with_sstable(1, folder.to_string());

        let cache: Arc<dyn DatasetCache> = Arc::new(SsTableCache::new(4));
        base.prewarm_mem_wal(std::slice::from_ref(&snapshot), Some(&cache))
            .await
            .expect("prewarm must open the generation and warm its index");

        // The generation is resident in the cache (same session), with its
        // index loadable — a later lookup that opens this path is a pure hit.
        let warmed = cache
            .get_or_open(&gen_uri, Some(base.session()), base.store_params().cloned())
            .await
            .unwrap();
        assert_eq!(warmed.load_indices().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_prewarm_mem_wal_empty_is_noop() {
        // No snapshots / no SSTables: prewarm is a clean no-op.
        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let schema = id_v_schema();
        let reader = RecordBatchIterator::new([Ok(id_v_batch(&schema, &[-1]))], schema.clone());
        let base = Dataset::write(reader, &base_uri, Some(WriteParams::default()))
            .await
            .unwrap();

        base.prewarm_mem_wal(&[], None).await.unwrap();

        let empty = ShardSnapshot::new(Uuid::new_v4()).with_current_generation(1);
        base.prewarm_mem_wal(std::slice::from_ref(&empty), None)
            .await
            .unwrap();
    }
}
