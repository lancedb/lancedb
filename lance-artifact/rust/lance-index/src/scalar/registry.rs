// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::borrow::Cow;
use std::sync::Arc;

use arrow_schema::{DataType, Field};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use futures::future::BoxFuture;
use lance_core::{
    Result,
    cache::{CacheKey, CacheKeySchema, KeyBuilder, LanceCache, UnsizedCacheKey},
    deepsize::DeepSizeOf,
};

use crate::progress::IndexBuildProgress;
use crate::registry::IndexPluginRegistry;
use crate::scalar::RowIdRemapper;
use crate::scalar::{CreatedIndex, IndexStore, ScalarIndex, expression::ScalarQueryParser};
// Re-export training types that were previously defined here
pub use crate::scalar::{TrainingCriteria, TrainingOrdering};

pub const VALUE_COLUMN_NAME: &str = "value";

/// A trait object for plugin-specific training parameters and data requirements.
///
/// Returned by [`BasicTrainer::new_training_request`]. The caller uses
/// [`criteria`](Self::criteria) to prepare the training data stream, then passes
/// the request back to [`BasicTrainer::train_index`], which may downcast
/// it to the plugin-specific concrete type to recover parsed parameters.
pub trait TrainingRequest: std::any::Any + Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;
    fn criteria(&self) -> &TrainingCriteria;
}

/// A default training request impl for indexes that don't need any parameters
pub(crate) struct DefaultTrainingRequest {
    criteria: TrainingCriteria,
}

impl DefaultTrainingRequest {
    pub fn new(criteria: TrainingCriteria) -> Self {
        Self { criteria }
    }
}

impl TrainingRequest for DefaultTrainingRequest {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn criteria(&self) -> &TrainingCriteria {
        &self.criteria
    }
}

/// Implemented by indexes that can train on a stream of column data.
///
/// The training process has two stages. In the first stage, the caller provides
/// index parameters and receives a [`TrainingRequest`] that describes what criteria
/// the training data must satisfy (e.g. sort order, row-ID availability). In the
/// second stage, the caller prepares the data accordingly and calls
/// [`train_index`](Self::train_index).
///
/// Any scalar index plugin that builds from a column data stream should implement
/// this trait.
#[async_trait]
pub trait BasicTrainer: Send + Sync {
    /// Creates a new training request from the given parameters.
    ///
    /// The returned request specifies the criteria the training data must satisfy.
    /// It is the caller's responsibility to prepare data that meets those criteria
    /// before calling [`train_index`](Self::train_index).
    fn new_training_request(&self, params: &str, field: &Field)
    -> Result<Box<dyn TrainingRequest>>;

    /// Train a new index from a prepared data stream.
    ///
    /// The provided data must fulfill all the criteria returned by
    /// [`new_training_request`](Self::new_training_request). It is the caller's
    /// responsibility to ensure this.
    ///
    /// Returns index details describing the index. These details may be useful for
    /// planning and must be provided when loading the index. It is the caller's
    /// responsibility to store them.
    async fn train_index(
        &self,
        data: SendableRecordBatchStream,
        index_store: &dyn IndexStore,
        request: Box<dyn TrainingRequest>,
        fragment_ids: Option<Vec<u32>>,
        progress: Arc<dyn IndexBuildProgress>,
    ) -> Result<CreatedIndex>;
}

/// A trait for scalar index plugins
#[async_trait]
pub trait ScalarIndexPlugin: Send + Sync + std::fmt::Debug {
    /// Returns this plugin's [`BasicTrainer`] implementation, if any.
    ///
    /// Training an index can be a complex process.  For example, a btree index might
    /// be trained using a shuffler from a distributed OLAP system such as
    /// Spark or Ray.  A vector index can be trained by sampling the column to create
    /// a kmeans model and then streaming the vectors to assign partitions.  Encapsulating
    /// the entire set of possible approaches is beyond what this trait can model.
    /// This is especially true because this is a low-level crate with no concept of a table
    /// or a dataset.
    ///
    /// However, in many cases, an index can be trained on a (potentially sorted) stream
    /// of column data.  There is also significant utility in being able to provide users
    /// with a simple generic "create an index" API.
    ///
    /// This method is a compromise.  Indexes that support training on a stream of column
    /// data should override this to return `Some(self)`.  Indexes that need their own
    /// individualized training approaches should return `None` and provide their own
    /// methods for training.
    ///
    /// An index can take both approaches.  Providing a simple (but maybe less
    /// efficient) stream-based trainer while also providing more specialized index
    /// creation methods elsewhere.
    fn basic_trainer(&self) -> Option<&dyn BasicTrainer> {
        None
    }

    /// A short name for the index
    ///
    /// This is a friendly name for display purposes and also can be used as an alias for
    /// the index type URL.  If multiple plugins have the same name, then the first one
    /// found will be used.
    ///
    /// By convention this is MixedCase with no spaces.  When used as an alias, it will be
    /// compared case-insensitively.
    fn name(&self) -> &str;

    /// Returns true if the index returns an exact answer (e.g. not AtMost)
    fn provides_exact_answer(&self) -> bool;

    /// The version of the index plugin
    ///
    /// We assume that indexes are not forwards compatible.  If an index was written with a
    /// newer version than this, it cannot be read
    fn version(&self) -> u32;

    /// Returns a new query parser for the index
    ///
    /// Can return None if this index cannot participate in query optimization
    fn new_query_parser(
        &self,
        index_name: String,
        index_details: &prost_types::Any,
    ) -> Option<Box<dyn ScalarQueryParser>>;

    /// Load an index from storage
    ///
    /// The index details should match the details that were returned when the index was
    /// originally trained.
    async fn load_index(
        &self,
        index_store: Arc<dyn IndexStore>,
        index_details: &prost_types::Any,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        cache: &LanceCache,
    ) -> Result<Arc<dyn ScalarIndex>>;

    /// Look up a previously-opened index in the cache.
    ///
    /// `cache` is already per-index namespaced by the caller, so a plugin's key
    /// only needs to disambiguate entries within a single index.
    ///
    /// The default implementation reads an in-memory `Arc<dyn ScalarIndex>` entry.
    /// Plugins whose index has a serializable representation should override this
    /// (together with [`put_in_cache`](Self::put_in_cache)) to store that
    /// representation under a sized [`CacheKey`] with
    /// a codec, and reconstruct the index here. `index_store` and
    /// `frag_reuse_index` are provided so the override can rebuild the index
    /// without re-reading metadata.
    async fn get_from_cache(
        &self,
        _index_store: Arc<dyn IndexStore>,
        _frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        cache: &LanceCache,
    ) -> Result<Option<Arc<dyn ScalarIndex>>> {
        Ok(cache.get_unsized_with_key(&ScalarIndexCacheKey).await)
    }

    /// Store a freshly-opened index in the cache.
    ///
    /// `cache` is already per-index namespaced; see
    /// [`get_from_cache`](Self::get_from_cache).
    ///
    /// The default implementation stores the `Arc<dyn ScalarIndex>` in-memory.
    async fn put_in_cache(&self, cache: &LanceCache, index: Arc<dyn ScalarIndex>) -> Result<()> {
        cache
            .insert_unsized_with_key(&ScalarIndexCacheKey, index)
            .await;
        Ok(())
    }

    /// Open an index through the cache, awaiting `load` only on a miss.
    ///
    /// The default read-through / write-back over
    /// [`get_from_cache`](Self::get_from_cache) and
    /// [`put_in_cache`](Self::put_in_cache) does not coalesce concurrent cold
    /// opens; plugins with a serializable form should override with
    /// [`single_flight_open`] so one shared load populates the sized state key.
    async fn get_or_insert_in_cache(
        &self,
        index_store: Arc<dyn IndexStore>,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        cache: &LanceCache,
        load: ScalarIndexLoad<'_>,
    ) -> Result<Arc<dyn ScalarIndex>> {
        if let Some(index) = self
            .get_from_cache(index_store, frag_reuse_index, cache)
            .await?
        {
            return Ok(index);
        }
        let index = load.await?;
        self.put_in_cache(cache, index.clone()).await?;
        Ok(index)
    }

    /// Optional hook allowing a plugin to provide statistics without loading the index.
    async fn load_statistics(
        &self,
        _index_store: Arc<dyn IndexStore>,
        _index_details: &prost_types::Any,
    ) -> Result<Option<serde_json::Value>> {
        Ok(None)
    }

    /// Optional hook that plugins can use if they need to be aware of the registry
    fn attach_registry(&self, _registry: Arc<IndexPluginRegistry>) {}

    /// Returns a JSON string representation of the provided index details
    ///
    /// These details will be user-visible and should be considered part of the public
    /// API.  As a result, efforts should be made to ensure the information is backwards
    /// compatible and avoid breaking changes.
    fn details_as_json(&self, _details: &prost_types::Any) -> Result<serde_json::Value> {
        // Return an empty JSON object as the default implementation
        Ok(serde_json::json!({}))
    }

    /// Optionally create a seed writer for the given column.
    ///
    /// A seed writer observes column values during data file writes, accumulates
    /// compact statistics in memory, and serializes them as a global buffer
    /// embedded in the data file footer. The buffer is later harvested during
    /// index updates to skip a full column scan.
    ///
    /// All parameters needed to construct the writer must be derivable from
    /// `index_details` — this method must not perform any I/O. Return `Ok(None)`
    /// if this index type does not support seed writing.
    async fn create_seed_writer(
        &self,
        _field_path: &str,
        _data_type: &DataType,
        _index_details: &prost_types::Any,
    ) -> Result<Option<Box<dyn super::seed::IndexSeedWriter>>> {
        Ok(None)
    }

    /// Returns true if this index type may have seed buffers embedded in data
    /// files for the given index configuration.
    ///
    /// When false the caller can skip opening data files to look for seeds
    /// entirely, avoiding I/O for index types or configurations that never
    /// write seeds.
    fn might_use_seeds(&self, _index_details: &prost_types::Any) -> bool {
        false
    }

    /// Attempt to update `reference_index` using pre-harvested `seeds` instead
    /// of re-scanning column data.
    ///
    /// Each [`FragmentSeed`](super::seed::FragmentSeed) carries the raw bytes
    /// written by the corresponding [`IndexSeedWriter`](super::seed::IndexSeedWriter)
    /// and the original `metadata_value` stored in the data file, which the plugin
    /// can use for compatibility validation (e.g. confirming `rows_per_zone`).
    ///
    /// Return `Ok(Some(created))` if the seed-based update succeeded, or
    /// `Ok(None)` to signal that the caller should fall back to a full column scan.
    async fn update_from_seeds(
        &self,
        _seeds: Vec<super::seed::FragmentSeed>,
        _reference_index: Arc<dyn ScalarIndex>,
        _index_details: &prost_types::Any,
        _dest_store: &dyn IndexStore,
    ) -> Result<Option<CreatedIndex>> {
        Ok(None)
    }
}

/// A boxed, `Send` future performing the storage-level load of a scalar index
/// (compat checks, `load_index`, metrics).
///
/// Passed to [`ScalarIndexPlugin::get_or_insert_in_cache`], which awaits it at
/// most once on a cache miss and drops it un-awaited on a warm hit.
pub type ScalarIndexLoad<'a> = BoxFuture<'a, Result<Arc<dyn ScalarIndex>>>;

/// Single-flight open helper for plugins with a serializable form.
///
/// Concurrent cold opens of the same index coalesce onto one `load`: it runs
/// once, `to_state` converts the opened index to its sized state, and the state
/// is cached under `state_key` (persisted via the key's
/// [`CacheCodec`](lance_core::cache::CacheCodec)). Every caller — warm hits
/// included — then rebuilds the index from the shared state via `from_state`, an
/// IO-free reconstruct.
///
/// `to_state` / `from_state` mirror the plugin's
/// [`put_in_cache`](ScalarIndexPlugin::put_in_cache) /
/// [`get_from_cache`](ScalarIndexPlugin::get_from_cache), keeping the state
/// representation defined in one place.
pub async fn single_flight_open<K, ToState, FromState>(
    cache: &LanceCache,
    state_key: K,
    load: ScalarIndexLoad<'_>,
    to_state: ToState,
    from_state: FromState,
) -> Result<Arc<dyn ScalarIndex>>
where
    K: CacheKey + Send,
    K::ValueType: DeepSizeOf + Send + Sync + 'static,
    ToState: FnOnce(&dyn ScalarIndex) -> Result<K::ValueType> + Send,
    FromState: FnOnce(Arc<K::ValueType>) -> Result<Arc<dyn ScalarIndex>> + Send,
{
    let state = cache
        .get_or_insert_with_key(state_key, move || async move {
            let index = load.await?;
            to_state(index.as_ref())
        })
        .await?;
    from_state(state)
}

/// In-memory cache key for a whole `Arc<dyn ScalarIndex>`.
///
/// Used by the default [`ScalarIndexPlugin::get_from_cache`] /
/// [`ScalarIndexPlugin::put_in_cache`] implementations. The cache is already
/// per-index namespaced by the caller, so a constant key suffices. Trait objects
/// cannot be serialized, so this is an [`UnsizedCacheKey`] with no codec —
/// plugins that want a persistable cache entry override those methods with a
/// sized key.
pub struct ScalarIndexCacheKey;

impl UnsizedCacheKey for ScalarIndexCacheKey {
    type ValueType = dyn ScalarIndex;

    fn key(&self) -> Cow<'_, str> {
        Cow::Borrowed("scalar_index")
    }

    fn type_name() -> &'static str {
        "ScalarIndex"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.scalar.registry.scalar-index-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_variant(0);
    }
}
