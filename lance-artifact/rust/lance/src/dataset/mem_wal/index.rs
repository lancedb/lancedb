// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Index store for MemTable write path.
//!
//! Maintains in-memory indexes that are updated synchronously with writes:
//! - BTree: Primary key and scalar field lookups
//! - HNSW: Vector similarity search (built incrementally, queryable while
//!   building, flushed as Lance HNSW + FLAT)
//! - FTS: Full-text search
//!
//! Other index types log a warning and are skipped.

#![allow(clippy::print_stderr)]
#![allow(clippy::type_complexity)]

mod arena_skiplist;
mod btree;
mod fts;
mod hnsw;
mod pk_key;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use datafusion::common::ScalarValue;

use super::memtable::batch_store::StoredBatch;
use super::wal::WriterCursors;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema as ArrowSchema};
use lance_core::datatypes::Schema as LanceSchema;
use lance_core::{Error, Result};
use lance_index::pbold;
use lance_index::scalar::InvertedIndexParams;
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_linalg::distance::DistanceType;
use lance_table::format::IndexMetadata;
use prost::Message as _;
use tracing::instrument;

/// Row position in MemTable.
///
/// This is the absolute row position across all batches in the MemTable.
/// When flushed to a single Lance file, this becomes the row ID directly.
pub type RowPosition = u64;

// Re-export public types used externally
pub use btree::{BTreeIndexConfig, BTreeMemIndex};
pub use fts::{FtsIndexConfig, FtsMemIndex, FtsQueryExpr, SearchOptions};
pub use hnsw::{HnswIndexConfig, HnswMemIndex};
pub use pk_key::encode_pk_tuple;

use pk_key::encode_pk_batch;

/// Synthetic column the composite PK index is keyed on: the order-preserving
/// encoded tuple (see [`encode_pk_tuple`]), stored as `Binary` so a
/// [`BTreeMemIndex`]'s byte backend indexes it directly.
const PK_KEY_COLUMN: &str = "__pk_key__";

/// Row count at or below which [`IndexStore::insert_batches`] indexes inline
/// rather than spawning a thread per index.
///
/// The spawn is one OS thread *per index* — tens of microseconds each, and a table can
/// carry several BTrees alongside its HNSW and FTS — so for a small batch it costs more
/// than the indexing it parallelizes. Small batches are not the exceptional case: a
/// durable put triggers a WAL flush covering only the batch it just inserted, so this
/// path is routinely called with a single short batch.
///
/// The crossover depends on per-row HNSW cost, which varies with dimension and
/// `ef_construction`; tune against `benches/mem_wal/vector/mem_wal_index_micro.rs`.
const PARALLEL_INDEX_MIN_ROWS: usize = 64;

/// The memtable's primary-key index, used to answer "newest visible version of
/// this key" for dedup. Single-column PKs reuse the column's compact typed
/// [`BTreeMemIndex`] (no second copy); composite PKs key a `BTreeMemIndex` on
/// the order-preserving encoded tuple ([`encode_pk_tuple`]) instead. Either way
/// the lookup is a single seek on one `BTreeMemIndex`.
enum PkIndex {
    /// Arity 1: aliases a `btree_indexes` entry, so the insert loop maintains it.
    Single(Arc<BTreeMemIndex>),
    /// Arity >= 2: a `BTreeMemIndex` over the encoded-tuple `Binary` key,
    /// maintained explicitly in the insert paths (the original batch lacks the
    /// synthetic key column). `columns` are the PK columns in order, resolved
    /// against each batch's schema at insert time.
    Composite {
        index: Arc<BTreeMemIndex>,
        columns: Vec<String>,
    },
}

// ============================================================================
// Index Store
// ============================================================================

/// Validate every configured in-memory index, and the composite primary key,
/// against the shard schema. Call once at shard open, before any write can land.
///
/// This is what makes poison-and-replay *terminating*. An index insert that
/// fails deterministically on a row that is already WAL-durable cannot be
/// recovered from: the writer poisons, the operator reopens, replay re-reads the
/// same WAL rows, the same insert fails again, and `open()` propagates it — a
/// shard that never comes back. Every such failure is an index *config*
/// disagreeing with the schema, never a property of the data, so one pass here
/// closes the whole class before a single row is accepted.
///
/// The data-dependent errors inside the index layer are already unreachable
/// through `put`: `MemTable::insert_batches_only` does a full `Arc<Schema>`
/// equality check, so a batch that would trip one is rejected before it reaches
/// the batch store, let alone the WAL.
///
/// It also rejects a config whose `field_id` names a different column than its
/// `column`. Index *selection* keys off `field_id` — a single-column PK reuses
/// the BTree whose `field_id` matches its key — so a config resolved only by name
/// could be bound under the wrong identity, serving stale reads and flushing the
/// wrong column into the durable PK sidecar. `lance_schema` supplies the
/// authoritative name→id mapping.
pub fn validate_index_configs(
    configs: &[MemIndexConfig],
    schema: &ArrowSchema,
    lance_schema: &LanceSchema,
    pk_columns: &[String],
) -> Result<()> {
    for config in configs {
        let column = config.column();
        if let MemIndexConfig::Fts(config) = config {
            let resolved = crate::index::scalar::inverted::resolve_fts_field(
                lance_schema,
                column,
                config.params.get_document_granularity(),
            )
            .map_err(|error| {
                Error::invalid_input(format!(
                    "FTS index '{}' is invalid for field path '{}': {error}",
                    config.name, column
                ))
            })?;
            if resolved.final_field_id != config.field_id {
                return Err(Error::invalid_input(format!(
                    "index '{}' is configured with field_id {} but its field path '{}' has \
                     final field_id {} in the shard schema",
                    config.name, config.field_id, column, resolved.final_field_id,
                )));
            }
            continue;
        }

        let field = schema.field_with_name(column).map_err(|_| {
            Error::invalid_input(format!(
                "index '{}' is configured on column '{}', which is not in the shard schema; \
                 available columns: [{}]",
                config.name(),
                column,
                schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;

        match config {
            // BTree falls back to per-row `ScalarValue` extraction, so it
            // accepts any column type the schema can hold. Existence is the
            // only precondition.
            MemIndexConfig::BTree(_) => {}
            MemIndexConfig::Fts(_) => unreachable!("FTS configs are validated by schema path"),
            MemIndexConfig::Hnsw(_) => match field.data_type() {
                DataType::FixedSizeList(item, dim) => {
                    if item.data_type() != &DataType::Float32 {
                        return Err(Error::invalid_input(format!(
                            "HNSW index '{}' requires a FixedSizeList<Float32> column; \
                             column '{}' has item type {:?}",
                            config.name(),
                            column,
                            item.data_type()
                        )));
                    }
                    // `HnswMemIndex.dim` is a placeholder until the first batch
                    // pins it (`hnsw.rs`), so a zero-width vector would only
                    // surface at insert time — i.e. on already-durable data.
                    if *dim <= 0 {
                        return Err(Error::invalid_input(format!(
                            "HNSW index '{}' requires a vector dimension > 0; column '{}' has \
                             dimension {dim}",
                            config.name(),
                            column,
                        )));
                    }
                }
                other => {
                    return Err(Error::invalid_input(format!(
                        "HNSW index '{}' requires a FixedSizeList<Float32> column; \
                         column '{}' is {:?}",
                        config.name(),
                        column,
                        other
                    )));
                }
            },
        }

        // The column resolves, but index selection keys off `field_id`, not name.
        // A config whose `field_id` identifies a *different* column would be bound
        // under the wrong identity (e.g. reused as the single-column PK index), so
        // reject any `field_id` that does not name the resolved column.
        let resolved_field_id = lance_schema
            .field(column)
            .ok_or_else(|| {
                Error::invalid_input(format!(
                    "index '{}' is configured on column '{}', which is present in the Arrow \
                     schema but absent from the Lance schema",
                    config.name(),
                    column,
                ))
            })?
            .id;
        if resolved_field_id != config.field_id() {
            return Err(Error::invalid_input(format!(
                "index '{}' is configured with field_id {} but its column '{}' has field_id {} \
                 in the shard schema",
                config.name(),
                config.field_id(),
                column,
                resolved_field_id,
            )));
        }
    }

    // Every PK column must exist in the schema. A single-column PK aliases a
    // BTree entry (any type); only a *composite* PK builds an order-preserving
    // encoded key, and only some types encode.
    for column in pk_columns {
        let field = schema.field_with_name(column).map_err(|_| {
            Error::invalid_input(format!(
                "primary-key column '{column}' is not in the shard schema"
            ))
        })?;
        if pk_columns.len() > 1 && !is_encodable_pk_type(field.data_type()) {
            return Err(Error::invalid_input(format!(
                "composite primary-key column '{column}' has type {:?}, which has no \
                 order-preserving key encoding",
                field.data_type()
            )));
        }
    }

    Ok(())
}

/// Types `pk_key::encode_value` can encode into an order-preserving composite key.
fn is_encodable_pk_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Date32
            | DataType::Date64
            | DataType::Boolean
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
    )
}

/// The index kinds a MemTable can maintain — the registry of MemWAL index
/// support. Data-free because indexes are identified by type url before any
/// [`MemIndexConfig`] exists.
///
/// Adding a variant is a compile error in [`details_suffix`](Self::details_suffix),
/// `MemIndexConfig::kind`, and `Dataset::mem_wal_writer` until each handles it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemIndexKind {
    /// BTree index for scalar fields (point lookups, range queries).
    BTree,
    /// HNSW vector index built incrementally, queryable while building.
    Hnsw,
    /// Full-text search index.
    Fts,
}

impl MemIndexKind {
    /// Every maintainable kind. A kind missing here is never detected, so it
    /// goes unmaintained rather than reaching a memtable that cannot build it.
    pub const ALL: &'static [Self] = &[Self::BTree, Self::Hnsw, Self::Fts];

    /// Suffix of the protobuf details message identifying this kind.
    ///
    /// Only the suffix: the prefix varies by dataset version
    /// (`/lance.table.`, `/lance.index.pb.`, and the `type.googleapis.com/`
    /// form MemWAL flush once wrote), and all must resolve.
    pub const fn details_suffix(self) -> &'static str {
        match self {
            Self::BTree => "BTreeIndexDetails",
            Self::Hnsw => "VectorIndexDetails",
            Self::Fts => "InvertedIndexDetails",
        }
    }

    /// The kind a base-table index of this protobuf type maps to, or `None`
    /// when a memtable cannot maintain it.
    pub fn from_type_url(type_url: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|kind| type_url.ends_with(kind.details_suffix()))
    }
}

/// Configuration for an index in MemWAL. Pairs 1:1 with [`MemIndexKind`] via
/// [`kind`](Self::kind).
///
/// `Hnsw` is boxed because `HnswBuildParams` is small but the variant may
/// grow with future config (e.g. shard-specific tuning).
#[derive(Debug, Clone)]
pub enum MemIndexConfig {
    /// BTree index for scalar fields (point lookups, range queries).
    BTree(BTreeIndexConfig),
    /// HNSW vector index built incrementally, queryable while building.
    Hnsw(Box<HnswIndexConfig>),
    /// Full-text search index.
    Fts(FtsIndexConfig),
}

impl MemIndexConfig {
    /// The kind this config builds. Links the config enum to the registry, so
    /// a new variant must declare its kind.
    pub const fn kind(&self) -> MemIndexKind {
        match self {
            Self::BTree(_) => MemIndexKind::BTree,
            Self::Hnsw(_) => MemIndexKind::Hnsw,
            Self::Fts(_) => MemIndexKind::Fts,
        }
    }

    /// Get the index name.
    pub fn name(&self) -> &str {
        match self {
            Self::BTree(c) => &c.name,
            Self::Hnsw(c) => &c.name,
            Self::Fts(c) => &c.name,
        }
    }

    /// Get the field ID.
    pub fn field_id(&self) -> i32 {
        match self {
            Self::BTree(c) => c.field_id,
            Self::Hnsw(c) => c.field_id,
            Self::Fts(c) => c.field_id,
        }
    }

    /// Get the column name.
    pub fn column(&self) -> &str {
        match self {
            Self::BTree(c) => &c.column,
            Self::Hnsw(c) => &c.column,
            Self::Fts(c) => &c.column,
        }
    }

    /// Create a BTree index config from base table IndexMetadata.
    pub fn btree_from_metadata(index_meta: &IndexMetadata, schema: &LanceSchema) -> Result<Self> {
        let (field_id, column) = Self::extract_field_info(index_meta, schema)?;
        Ok(Self::BTree(BTreeIndexConfig {
            name: index_meta.name.clone(),
            field_id,
            column,
        }))
    }

    /// Create an FTS index config from base table IndexMetadata.
    pub fn fts_from_metadata(index_meta: &IndexMetadata, schema: &LanceSchema) -> Result<Self> {
        let (field_id, _) = Self::extract_field_info(index_meta, schema)?;

        // Extract InvertedIndexParams from index_details if available
        let details = if let Some(details_any) = &index_meta.index_details {
            pbold::InvertedIndexDetails::decode(details_any.value.as_slice()).map_err(|err| {
                Error::io(format!(
                    "failed to decode InvertedIndexDetails for MemWAL FTS index '{}': {}",
                    index_meta.name, err
                ))
            })?
        } else {
            pbold::InvertedIndexDetails::default()
        };
        let details =
            crate::index::scalar::inverted::normalize_inverted_details(index_meta, details)?;
        let params = InvertedIndexParams::try_from(&details)?;
        let resolved = crate::index::scalar::inverted::resolve_fts_field_by_id(
            schema,
            field_id,
            params.get_document_granularity(),
        )?;

        Ok(Self::Fts(
            FtsIndexConfig::try_with_params(
                index_meta.name.clone(),
                field_id,
                resolved.canonical_path.clone(),
                params,
            )?
            .with_resolved_field(resolved),
        ))
    }

    /// Create an HNSW vector index config.
    pub fn hnsw(name: String, field_id: i32, column: String, distance_type: DistanceType) -> Self {
        Self::Hnsw(Box::new(HnswIndexConfig::new(
            name,
            field_id,
            column,
            distance_type,
        )))
    }

    /// Create an HNSW vector index config with explicit build parameters.
    pub fn hnsw_with_params(
        name: String,
        field_id: i32,
        column: String,
        distance_type: DistanceType,
        build_params: HnswBuildParams,
    ) -> Self {
        Self::Hnsw(Box::new(
            HnswIndexConfig::new(name, field_id, column, distance_type)
                .with_build_params(build_params),
        ))
    }

    /// Extract field ID and column name from index metadata.
    fn extract_field_info(
        index_meta: &IndexMetadata,
        schema: &LanceSchema,
    ) -> Result<(i32, String)> {
        let field_id = index_meta.fields.first().ok_or_else(|| {
            Error::invalid_input(format!("Index '{}' has no fields", index_meta.name))
        })?;

        let column = schema
            .field_by_id(*field_id)
            .map(|f| f.name.clone())
            .ok_or_else(|| {
                Error::invalid_input(format!("Field with id {} not found in schema", field_id))
            })?;

        Ok((*field_id, column))
    }
}

/// Whether the MemWAL can maintain an index of this protobuf type.
///
/// Opening a shard writer rejects anything outside this set, which makes the
/// table unwritable — so filter on this before committing a maintained set,
/// not at claim time.
pub fn is_maintainable_index_type(type_url: &str) -> bool {
    MemIndexKind::from_type_url(type_url).is_some()
}

/// Shared by the detection and writer paths so both report the same thing.
pub(crate) fn unsupported_index_type(type_url: &str) -> Error {
    Error::invalid_input(format!(
        "Unsupported index type for MemWAL: {}. Supported: BTree, Inverted, Vector",
        type_url
    ))
}

/// Registry managing all in-memory indexes for a MemTable.
///
/// Indexes are keyed by index name. Each index stores its field_id for
/// stable column-to-index resolution (column name → field_id → index).
///
/// The store also carries the MemTable's two cursors: `indexed_count` (what the
/// index layer has ingested) and `visible_count` (what is indexed *and* durable,
/// and therefore safe for scanners to read). Scanners snapshot the latter at plan
/// construction time so every plan keys on a stable MVCC cursor.
pub struct IndexStore {
    /// BTree indexes keyed by index name. `Arc` so the primary-key BTrees can be
    /// shared into [`Self::pk_btrees`] without a second copy or a second insert.
    btree_indexes: HashMap<String, Arc<BTreeMemIndex>>,
    /// HNSW vector indexes keyed by index name.
    hnsw_indexes: HashMap<String, HnswMemIndex>,
    /// FTS indexes keyed by index name.
    fts_indexes: HashMap<String, FtsMemIndex>,
    /// The primary-key index (single-column or composite), or `None` without a
    /// primary key. Queried via [`Self::pk_newest_visible`] (see
    /// [`Self::enable_pk_index`]).
    pk_index: Option<PkIndex>,
    /// How many batches of this memtable have been fully indexed. An exclusive
    /// count: 0 means none.
    ///
    /// This has only ever been an *indexed* cursor — it is advanced at the end of
    /// `insert_batches`, once every index insert for the batch has completed, and
    /// never before. It was named `max_visible_batch_position` and treated as a
    /// visibility cursor by five read sites, which is how rows became readable
    /// before they were durable. Publishing is a separate step, and it is the
    /// writer's to make — see `visible_count`.
    indexed_count: AtomicUsize,

    /// The writer's cursors, and this memtable's coordinate within them. `None`
    /// for a bare `IndexStore` (tests, benches), where visibility is just the
    /// indexed prefix.
    ///
    /// Visibility is **derived, never stored**: see `visible_count`.
    durability: Option<(Arc<WriterCursors>, usize)>,
    /// Conservative flag set once this memtable has observed any primary-key
    /// rewrite while maintaining a search index. Search planners can push top-k
    /// into HNSW/FTS for append-only PK data, but must switch to
    /// newest-before-top-k search after an overwrite.
    pk_has_overrides: AtomicBool,
}

impl Default for IndexStore {
    fn default() -> Self {
        Self {
            btree_indexes: HashMap::new(),
            hnsw_indexes: HashMap::new(),
            fts_indexes: HashMap::new(),
            pk_index: None,
            indexed_count: AtomicUsize::new(0),
            durability: None,
            pk_has_overrides: AtomicBool::new(false),
        }
    }
}

impl std::fmt::Debug for IndexStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexStore")
            .field(
                "btree_indexes",
                &self.btree_indexes.keys().collect::<Vec<_>>(),
            )
            .field(
                "hnsw_indexes",
                &self.hnsw_indexes.keys().collect::<Vec<_>>(),
            )
            .field("fts_indexes", &self.fts_indexes.keys().collect::<Vec<_>>())
            .field(
                "pk_index",
                &match &self.pk_index {
                    None => "none".to_string(),
                    Some(PkIndex::Single(b)) => format!("single({})", b.column_name()),
                    Some(PkIndex::Composite { columns, .. }) => {
                        format!("composite({})", columns.join(", "))
                    }
                },
            )
            .field("indexed_count", &self.indexed_count.load(Ordering::Acquire))
            .field(
                "pk_has_overrides",
                &self.pk_has_overrides.load(Ordering::Acquire),
            )
            .finish()
    }
}

impl IndexStore {
    /// Create a new empty index registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an index registry from index configurations.
    ///
    /// # Arguments
    ///
    /// * `configs` - Index configurations
    /// * `max_rows` - Maximum vectors / rows in memtable. Used to size the
    ///   pre-allocated HNSW graph and storage capacity.
    /// * `max_batches` - Maximum number of write batches the HNSW storage
    ///   can hold by reference (matches the writer's
    ///   `ShardWriterConfig::max_memtable_batches`).
    pub fn from_configs(
        configs: &[MemIndexConfig],
        max_rows: usize,
        max_batches: usize,
    ) -> Result<Self> {
        let mut registry = Self::new();

        for config in configs {
            match config {
                MemIndexConfig::BTree(c) => {
                    let index = Arc::new(BTreeMemIndex::new(c.field_id, c.column.clone()));
                    registry.btree_indexes.insert(c.name.clone(), index);
                }
                MemIndexConfig::Hnsw(c) => {
                    let index = HnswMemIndex::with_capacity(
                        c.field_id,
                        c.column.clone(),
                        c.distance_type,
                        c.build_params.clone(),
                        max_rows,
                        max_batches,
                    );
                    registry.hnsw_indexes.insert(c.name.clone(), index);
                }
                MemIndexConfig::Fts(c) => {
                    let index = match c.resolved_field.as_deref() {
                        Some(resolved) => FtsMemIndex::try_with_resolved_field(
                            c.field_id,
                            c.column.clone(),
                            c.params.clone(),
                            resolved.clone(),
                        )?,
                        None => FtsMemIndex::try_with_params(
                            c.field_id,
                            c.column.clone(),
                            c.params.clone(),
                        )?,
                    };
                    registry.fts_indexes.insert(c.name.clone(), index);
                }
            }
        }

        Ok(registry)
    }

    /// Add a BTree/scalar index (skip-list backed). Low-level / test helper;
    /// the production memtable path goes through [`Self::from_configs`].
    pub fn add_btree(&mut self, name: String, field_id: i32, column: String) {
        self.btree_indexes
            .insert(name, Arc::new(BTreeMemIndex::new(field_id, column)));
    }

    /// Add an HNSW vector index with default build parameters.
    ///
    /// HNSW indexes must be configured before rows are inserted into a
    /// PK-indexed memtable. The vector planner's append-only fast path relies
    /// on `pk_has_overrides` being maintained for every row visible to HNSW.
    pub fn add_hnsw(
        &mut self,
        name: String,
        field_id: i32,
        column: String,
        distance_type: DistanceType,
        capacity: usize,
        max_batches: usize,
    ) {
        assert!(
            self.pk_index.is_none() || self.pk_is_empty(),
            "HNSW indexes must be configured before inserting rows into a PK memtable"
        );
        self.hnsw_indexes.insert(
            name,
            HnswMemIndex::with_capacity(
                field_id,
                column,
                distance_type,
                HnswBuildParams::default(),
                capacity,
                max_batches,
            ),
        );
    }

    /// Add an HNSW vector index with explicit build parameters.
    ///
    /// See [`Self::add_hnsw`] for the PK-indexed memtable lifecycle invariant.
    #[allow(clippy::too_many_arguments)]
    pub fn add_hnsw_with_params(
        &mut self,
        name: String,
        field_id: i32,
        column: String,
        distance_type: DistanceType,
        build_params: HnswBuildParams,
        capacity: usize,
        max_batches: usize,
    ) {
        assert!(
            self.pk_index.is_none() || self.pk_is_empty(),
            "HNSW indexes must be configured before inserting rows into a PK memtable"
        );
        self.hnsw_indexes.insert(
            name,
            HnswMemIndex::with_capacity(
                field_id,
                column,
                distance_type,
                build_params,
                capacity,
                max_batches,
            ),
        );
    }

    /// Add an FTS index with default tokenizer parameters.
    ///
    /// FTS indexes must be configured before rows are inserted into a
    /// PK-indexed memtable. FTS top-k pushdown relies on `pk_has_overrides`
    /// being maintained for every row visible to the index.
    pub fn add_fts(&mut self, name: String, field_id: i32, column: String) {
        assert!(
            self.pk_index.is_none() || self.pk_is_empty(),
            "FTS indexes must be configured before inserting rows into a PK memtable"
        );
        self.fts_indexes
            .insert(name, FtsMemIndex::new(field_id, column));
    }

    /// Add an FTS index with custom tokenizer parameters.
    pub fn add_fts_with_params(
        &mut self,
        name: String,
        field_id: i32,
        column: String,
        params: InvertedIndexParams,
    ) -> Result<()> {
        assert!(
            self.pk_index.is_none() || self.pk_is_empty(),
            "FTS indexes must be configured before inserting rows into a PK memtable"
        );
        self.fts_indexes.insert(
            name,
            FtsMemIndex::try_with_params(field_id, column, params)?,
        );
        Ok(())
    }

    /// Maintain a primary-key index so the memtable can answer "newest visible
    /// version of this key" (see [`Self::pk_newest_visible`]).
    ///
    /// Single-column PKs reuse an existing BTree on the field, else auto-create
    /// one under a `__pk__*` name so the normal insert loop maintains it (no
    /// second copy). Composite (arity >= 2) PKs key a `BTreeMemIndex` on the
    /// order-preserving encoded tuple (synthetic `PK_KEY_COLUMN`), maintained
    /// explicitly in the insert paths. Call once at construction, after
    /// [`Self::from_configs`] and before any inserts; a no-op when `pk_columns`
    /// is empty. Search indexes (HNSW/FTS) must also still be empty so every
    /// search-visible row participates in PK override tracking.
    pub fn enable_pk_index(&mut self, pk_columns: &[(String, i32)]) {
        if !pk_columns.is_empty() {
            assert!(
                self.hnsw_indexes.values().all(|idx| idx.is_empty())
                    && self.fts_indexes.values().all(|idx| idx.is_empty()),
                "Primary-key indexes must be configured before inserting rows into a search-indexed memtable"
            );
        }
        self.pk_index = match pk_columns {
            [] => None,
            [(column, field_id)] => {
                let btree = match self
                    .btree_indexes
                    .values()
                    .find(|b| b.field_id() == *field_id)
                {
                    Some(existing) => existing.clone(),
                    None => {
                        let btree = Arc::new(BTreeMemIndex::new(*field_id, column.clone()));
                        self.btree_indexes
                            .insert(format!("__pk__{column}"), btree.clone());
                        btree
                    }
                };
                Some(PkIndex::Single(btree))
            }
            multi => Some(PkIndex::Composite {
                // Synthetic field id (-1): the composite index is held directly,
                // never resolved by field id.
                index: Arc::new(BTreeMemIndex::new(-1, PK_KEY_COLUMN.to_string())),
                columns: multi.iter().map(|(c, _)| c.clone()).collect(),
            }),
        };
    }

    /// Whether the memtable has a primary-key index.
    pub fn has_pk_index(&self) -> bool {
        self.pk_index.is_some()
    }

    /// Sorted `(value, row_id)` training batches for the flushed on-disk PK
    /// BTree (the sidecar dedup index). Single-column emits the typed PK value;
    /// composite emits the order-preserving `Binary` encoded tuple. Empty when
    /// there is no primary key. Row positions line up 1:1 with the forward-
    /// written data file, so they are the SSTable row ids directly.
    pub fn pk_training_batches(&self, batch_size: usize) -> Result<Vec<RecordBatch>> {
        match &self.pk_index {
            None => Ok(Vec::new()),
            Some(PkIndex::Single(btree)) => btree.to_training_batches(batch_size),
            Some(PkIndex::Composite { index, .. }) => index.to_training_batches(batch_size),
        }
    }

    /// Resolve the PK columns' positions in `batch` (composite insert helper).
    fn pk_batch_indices(batch: &RecordBatch, columns: &[String]) -> Result<Vec<usize>> {
        columns
            .iter()
            .map(|c| {
                batch
                    .schema()
                    .column_with_name(c)
                    .map(|(i, _)| i)
                    .ok_or_else(|| {
                        Error::invalid_input(format!("PK column '{c}' not found in batch"))
                    })
            })
            .collect()
    }

    /// Maintain the composite PK index for `batch` (no-op for single/no PK):
    /// encode the PK columns into the synthetic `PK_KEY_COLUMN` `Binary` column
    /// and feed that to the keyed `BTreeMemIndex`.
    fn insert_composite_pk(
        &self,
        batch: &RecordBatch,
        row_offset: u64,
        report_existing: bool,
    ) -> Result<bool> {
        if let Some(PkIndex::Composite { index, columns }) = &self.pk_index {
            let pk_indices = Self::pk_batch_indices(batch, columns)?;
            let encoded = encode_pk_batch(batch, &pk_indices)?;
            let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                PK_KEY_COLUMN,
                arrow_schema::DataType::Binary,
                false,
            )]));
            let key_batch = RecordBatch::try_new(schema, vec![Arc::new(encoded)])
                .map_err(|e| Error::invalid_input(e.to_string()))?;
            if report_existing {
                return index.insert_and_report_existing(&key_batch, row_offset);
            }
            index.insert(&key_batch, row_offset)?;
        }
        Ok(false)
    }

    /// The newest row position of the primary-key tuple `values` (in PK order)
    /// visible at `max_visible_row`, or `None`. A single seek either way:
    /// single-column probes the typed BTree; composite probes the encoded-tuple
    /// index. Collision-free, since `position` is the row identity.
    pub fn pk_newest_visible(
        &self,
        values: &[ScalarValue],
        max_visible_row: RowPosition,
    ) -> Option<RowPosition> {
        match &self.pk_index {
            None => None,
            Some(PkIndex::Single(btree)) => btree.get_newest_visible(&values[0], max_visible_row),
            Some(PkIndex::Composite { index, .. }) => {
                // An unsupported PK type would have failed at insert, so the
                // index can't hold a tuple this fails to encode. The probe key is
                // the same `Binary`-encoded tuple the insert path indexed.
                let key = encode_pk_tuple(values).ok()?;
                index.get_newest_visible(&ScalarValue::Binary(Some(key)), max_visible_row)
            }
        }
    }

    /// Whether `position` is the newest visible row of `values` — the recency
    /// check the active index-search arms apply to drop predicate-crossing
    /// stale hits. Callers gate on [`Self::has_pk_index`] first, since this is
    /// `false` (drop) when the memtable has no primary-key index.
    pub fn pk_is_newest(
        &self,
        values: &[ScalarValue],
        position: RowPosition,
        max_visible_row: RowPosition,
    ) -> bool {
        self.pk_newest_visible(values, max_visible_row) == Some(position)
    }

    /// Whether `key` has any version visible at `max_visible_row` — the
    /// cross-source block-list's existence query, snapshot-bounded so a
    /// not-yet-visible write can't shadow an older visible copy.
    ///
    /// `key` is already in the index's key space: the typed PK value for a
    /// single-column key, the `Binary`-encoded tuple for a composite one (built
    /// by `block_list::on_disk_pk_key`, the same key the flushed on-disk index is
    /// probed with). Both arities forward it straight to the keyed BTree.
    pub fn pk_contains_key(&self, key: &ScalarValue, max_visible_row: RowPosition) -> bool {
        match &self.pk_index {
            None => false,
            Some(PkIndex::Single(btree)) | Some(PkIndex::Composite { index: btree, .. }) => {
                btree.get_newest_visible(key, max_visible_row).is_some()
            }
        }
    }

    /// Whether the primary-key index holds no rows (or doesn't exist).
    pub fn pk_is_empty(&self) -> bool {
        match &self.pk_index {
            None => true,
            Some(PkIndex::Single(btree)) => btree.is_empty(),
            Some(PkIndex::Composite { index, .. }) => index.is_empty(),
        }
    }

    /// Whether this memtable has observed at least one PK rewrite.
    ///
    /// This is intentionally conservative: once true, it never resets for the
    /// lifetime of the memtable. That is enough for query planning because a
    /// memtable is flushed as a unit, and any rewrite means search-index top-k
    /// pushdown can be polluted by stale entries that must be removed before
    /// top-k. Scalar-only PK tables skip tracking because no search index uses
    /// the flag.
    pub fn pk_has_overrides(&self) -> bool {
        self.pk_has_overrides.load(Ordering::Acquire)
    }

    fn should_track_pk_overrides(&self) -> bool {
        (!self.hnsw_indexes.is_empty() || !self.fts_indexes.is_empty()) && !self.pk_has_overrides()
    }

    fn is_single_pk_btree(&self, index: &Arc<BTreeMemIndex>) -> bool {
        matches!(&self.pk_index, Some(PkIndex::Single(pk)) if Arc::ptr_eq(pk, index))
    }

    fn mark_pk_overrides_if_needed(&self, had_existing_pk: bool) {
        if had_existing_pk {
            self.pk_has_overrides.store(true, Ordering::Release);
        }
    }

    /// Insert a batch into all indexes.
    pub fn insert(&self, batch: &RecordBatch, row_offset: u64) -> Result<()> {
        self.insert_with_batch_position(batch, row_offset, None)
    }

    /// Insert a batch into all indexes with batch position tracking.
    #[instrument(name = "idx_insert_batch", level = "debug", skip_all, fields(num_rows = batch.num_rows(), row_offset, batch_position))]
    pub fn insert_with_batch_position(
        &self,
        batch: &RecordBatch,
        row_offset: u64,
        batch_position: Option<usize>,
    ) -> Result<()> {
        let track_pk_overrides = self.should_track_pk_overrides();
        for index in self.btree_indexes.values() {
            if track_pk_overrides && self.is_single_pk_btree(index) {
                let had_existing = index.insert_and_report_existing(batch, row_offset)?;
                self.mark_pk_overrides_if_needed(had_existing);
            } else {
                index.insert(batch, row_offset)?;
            }
        }
        for index in self.hnsw_indexes.values() {
            index.insert(batch, row_offset)?;
        }
        for index in self.fts_indexes.values() {
            index.insert(batch, row_offset)?;
        }
        // Single-column PK aliases a `btree_indexes` entry (maintained above);
        // a composite PK has its own index, maintained here.
        let had_existing = self.insert_composite_pk(batch, row_offset, track_pk_overrides)?;
        self.mark_pk_overrides_if_needed(had_existing);

        // Update the indexed prefix after every index has been updated.
        if let Some(bp) = batch_position {
            self.advance_indexed_count(bp + 1);
        }

        Ok(())
    }

    /// Advance the indexed prefix to at least `count` batches.
    ///
    /// Only ever moves forward (idempotent max). The vector planner relies on the
    /// insert paths setting `pk_has_overrides` before this is called, so any
    /// snapshot that can see a PK rewrite also observes `pk_has_overrides == true`.
    pub(crate) fn advance_indexed_count(&self, count: usize) {
        let mut current = self.indexed_count.load(Ordering::Acquire);
        while count > current {
            match self.indexed_count.compare_exchange_weak(
                current,
                count,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Insert multiple batches into every index.
    ///
    /// Above `PARALLEL_INDEX_MIN_ROWS` rows each index runs on its own thread, which
    /// maximizes parallelism when several indexes are maintained. At or below it they run
    /// inline on the calling thread: the spawn is one OS thread *per index*, and for a
    /// handful of rows that costs more than the indexing itself.
    ///
    /// Returns a map of index names to their update durations for performance tracking.
    #[instrument(name = "idx_insert_batches", level = "debug", skip_all, fields(batch_count = batches.len()))]
    pub fn insert_batches(
        &self,
        batches: &[StoredBatch],
    ) -> Result<std::collections::HashMap<String, std::time::Duration>> {
        if batches.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let track_pk_overrides = self.should_track_pk_overrides();

        // One task per index, boxed so the inline and the threaded path drive the very
        // same closures. Each reports whether it saw an already-present PK.
        type IndexTask<'a> = Box<dyn Fn() -> Result<bool> + Send + Sync + 'a>;
        let mut tasks: Vec<(&str, IndexTask<'_>)> = Vec::new();

        for (name, index) in &self.btree_indexes {
            let track_this_index = track_pk_overrides && self.is_single_pk_btree(index);
            tasks.push((
                name.as_str(),
                Box::new(move || {
                    let mut had_existing = false;
                    for stored in batches {
                        if track_this_index {
                            had_existing |= index
                                .insert_and_report_existing(&stored.data, stored.row_offset)?;
                        } else {
                            index.insert(&stored.data, stored.row_offset)?;
                        }
                    }
                    Ok(had_existing)
                }),
            ));
        }

        for (name, index) in &self.hnsw_indexes {
            tasks.push((
                name.as_str(),
                Box::new(move || index.insert_batches(batches).map(|_| false)),
            ));
        }

        for (name, index) in &self.fts_indexes {
            tasks.push((
                name.as_str(),
                Box::new(move || {
                    for stored in batches {
                        index.insert(&stored.data, stored.row_offset)?;
                    }
                    Ok(false)
                }),
            ));
        }

        // Keep the raw `Duration` so sub-millisecond timings (the steady state for BTree
        // updates) survive instead of truncating to 0.
        let total_rows: usize = batches.iter().map(|b| b.num_rows).sum();
        let results: Vec<(&str, std::time::Duration, Result<bool>)> =
            if tasks.len() < 2 || total_rows <= PARALLEL_INDEX_MIN_ROWS {
                tasks
                    .iter()
                    .map(|(name, task)| {
                        let start = Instant::now();
                        let result = task();
                        (*name, start.elapsed(), result)
                    })
                    .collect()
            } else {
                std::thread::scope(|scope| {
                    let handles: Vec<_> = tasks
                        .iter()
                        .map(|(name, task)| {
                            let handle = scope.spawn(move || {
                                let start = Instant::now();
                                let result = task();
                                (start.elapsed(), result)
                            });
                            (*name, handle)
                        })
                        .collect();

                    handles
                        .into_iter()
                        .map(|(name, handle)| match handle.join() {
                            Ok((duration, result)) => (name, duration, result),
                            Err(_) => (
                                name,
                                std::time::Duration::ZERO,
                                Err(Error::internal(format!("Index '{}' thread panicked", name))),
                            ),
                        })
                        .collect()
                })
            };

        // Every task ran to completion whether or not a peer failed (the threaded path
        // joins all handles unconditionally). Keep the first error; there is no rollback,
        // so a failure here is terminal for the writer.
        let mut first_error: Option<Error> = None;
        let mut had_existing_pk = false;
        let mut duration_map =
            std::collections::HashMap::<String, std::time::Duration>::with_capacity(results.len());

        for (name, duration, result) in results {
            duration_map.insert(name.to_string(), duration);
            match result {
                Ok(had_existing) => had_existing_pk |= had_existing,
                Err(e) if first_error.is_none() => first_error = Some(e),
                Err(_) => {}
            }
        }

        if let Some(e) = first_error {
            return Err(e);
        }
        self.mark_pk_overrides_if_needed(had_existing_pk);

        // Single-column PK aliases a `btree_indexes` entry — its task above already
        // maintained it. A composite PK has its own index; maintain it here before the
        // watermark advances so the visible prefix is fully indexed.
        let mut had_existing = false;
        for stored in batches {
            had_existing |=
                self.insert_composite_pk(&stored.data, stored.row_offset, track_pk_overrides)?;
        }
        self.mark_pk_overrides_if_needed(had_existing);

        // The indexed prefix now covers every batch up to and including the
        // highest position in this call, so the count is that position plus one.
        let max_bp = batches.iter().map(|b| b.batch_position).max().unwrap();
        self.advance_indexed_count(max_bp + 1);

        Ok(duration_map)
    }

    /// Get a BTree index by name.
    pub fn get_btree(&self, name: &str) -> Option<&BTreeMemIndex> {
        self.btree_indexes.get(name).map(Arc::as_ref)
    }

    /// Get an HNSW vector index by name.
    pub fn get_hnsw(&self, name: &str) -> Option<&HnswMemIndex> {
        self.hnsw_indexes.get(name)
    }

    /// Get an FTS index by name.
    pub fn get_fts(&self, name: &str) -> Option<&FtsMemIndex> {
        self.fts_indexes.get(name)
    }

    /// Get a BTree index by field ID.
    ///
    /// Searches through all BTree indexes to find one matching the field_id.
    /// Use this for column-to-index resolution (column → field_id → index).
    pub fn get_btree_by_field_id(&self, field_id: i32) -> Option<&BTreeMemIndex> {
        self.btree_indexes
            .values()
            .find(|idx| idx.field_id() == field_id)
            .map(Arc::as_ref)
    }

    /// Get an HNSW vector index by field ID.
    pub fn get_hnsw_by_field_id(&self, field_id: i32) -> Option<&HnswMemIndex> {
        self.hnsw_indexes
            .values()
            .find(|idx| idx.field_id() == field_id)
    }

    /// Get an FTS index by field ID.
    ///
    /// Searches through all FTS indexes to find one matching the field_id.
    /// Use this for column-to-index resolution (column → field_id → index).
    pub fn get_fts_by_field_id(&self, field_id: i32) -> Option<&FtsMemIndex> {
        self.get_fts_by_field_id_and_granularity(
            field_id,
            lance_index::scalar::inverted::DocumentGranularity::Row,
        )
    }

    pub fn get_fts_by_field_id_and_granularity(
        &self,
        field_id: i32,
        document_granularity: lance_index::scalar::inverted::DocumentGranularity,
    ) -> Option<&FtsMemIndex> {
        self.fts_indexes.values().find(|idx| {
            idx.field_id() == field_id && idx.document_granularity() == document_granularity
        })
    }

    /// Get a BTree index by column name.
    pub fn get_btree_by_column(&self, column: &str) -> Option<&BTreeMemIndex> {
        self.btree_indexes
            .values()
            .find(|idx| idx.column_name() == column)
            .map(Arc::as_ref)
    }

    /// Get an HNSW vector index by column name.
    pub fn get_hnsw_by_column(&self, column: &str) -> Option<&HnswMemIndex> {
        self.hnsw_indexes
            .values()
            .find(|idx| idx.column_name() == column)
    }

    /// Get an FTS index by column name.
    pub fn get_fts_by_column(&self, column: &str) -> Option<&FtsMemIndex> {
        self.get_fts_by_column_and_granularity(
            column,
            lance_index::scalar::inverted::DocumentGranularity::Row,
        )
    }

    pub fn get_fts_by_column_and_granularity(
        &self,
        column: &str,
        document_granularity: lance_index::scalar::inverted::DocumentGranularity,
    ) -> Option<&FtsMemIndex> {
        self.fts_indexes.values().find(|idx| {
            idx.column_name() == column && idx.document_granularity() == document_granularity
        })
    }

    /// Return the distinct persisted document granularities for FTS indexes on
    /// `column`, ordered from row to list-element.
    pub fn fts_document_granularities_by_column(
        &self,
        column: &str,
    ) -> Vec<lance_index::scalar::inverted::DocumentGranularity> {
        let mut granularities = self
            .fts_indexes
            .values()
            .filter(|index| index.column_name() == column)
            .map(|index| index.document_granularity())
            .collect::<Vec<_>>();
        granularities.sort_by_key(|document_granularity| match document_granularity {
            lance_index::scalar::inverted::DocumentGranularity::Row => 0,
            lance_index::scalar::inverted::DocumentGranularity::ListElement => 1,
        });
        granularities.dedup();
        granularities
    }

    /// Check if the registry has any indexes.
    pub fn is_empty(&self) -> bool {
        self.btree_indexes.is_empty() && self.hnsw_indexes.is_empty() && self.fts_indexes.is_empty()
    }

    /// Name every index this memtable carries, for diagnostics.
    ///
    /// Answers "is my fresh-tier vector search brute-force" — an absent
    /// name is the whole explanation, and there is no other way to see it
    /// from outside. Sorted so repeated calls compare cleanly; `HashMap`
    /// iteration order alone would not.
    pub fn index_names(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .btree_indexes
            .keys()
            .chain(self.hnsw_indexes.keys())
            .chain(self.fts_indexes.keys())
            .cloned()
            .collect();
        out.sort();
        out
    }

    /// Get the total number of indexes.
    pub fn len(&self) -> usize {
        self.btree_indexes.len() + self.hnsw_indexes.len() + self.fts_indexes.len()
    }

    /// How many batches of this memtable have been fully indexed (exclusive
    /// count; 0 before any batch is indexed).
    ///
    /// This is the *indexed* cursor, not the visibility watermark: it advances
    /// once every index insert for a batch completes, regardless of WAL
    /// durability. Readers must snapshot [`Self::visible_count`], which derives
    /// what is safe to read from this cursor and the writer's durability cursor.
    pub fn indexed_count(&self) -> usize {
        self.indexed_count.load(Ordering::Acquire)
    }

    /// The prefix of this memtable that readers may see. Snapshot this, never
    /// `indexed_count`.
    ///
    /// Derived on every call from the two cursors rather than cached, so there is
    /// no published value that can be left stale by a race between the two tasks
    /// that advance them. A bare `IndexStore` has no writer, so its visible
    /// prefix is simply what has been indexed.
    pub fn visible_count(&self) -> usize {
        let indexed = self.indexed_count();
        match &self.durability {
            Some((cursors, global_offset)) => cursors.visible_count(indexed, *global_offset),
            None => indexed,
        }
    }

    /// Bind this memtable's indexes to the writer's cursors. Called once at
    /// construction, before the memtable is published.
    pub(crate) fn set_durability(&mut self, cursors: Arc<WriterCursors>, global_offset: usize) {
        self.durability = Some((cursors, global_offset));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema};
    use lance_index::scalar::inverted::InvertedListFormatVersion;
    use rstest::rstest;
    use std::sync::Arc;
    use uuid::Uuid;

    /// Matching is on the message-name suffix, not the whole url: `Any::from_msg`
    /// emits the package (`/lance.table.`, `/lance.index.pb.`), while MemWAL flush
    /// used to hand-write a `type.googleapis.com/` url that existing datasets
    /// still carry.
    #[rstest]
    #[case::btree("/lance.table.BTreeIndexDetails", Some(MemIndexKind::BTree))]
    #[case::fts("/lance.table.InvertedIndexDetails", Some(MemIndexKind::Fts))]
    #[case::fts_legacy("/lance.index.pb.InvertedIndexDetails", Some(MemIndexKind::Fts))]
    #[case::vector("/lance.index.pb.VectorIndexDetails", Some(MemIndexKind::Hnsw))]
    // What MemWAL flush wrote before it switched to `Any::from_msg`.
    #[case::vector_legacy_flush(
        "type.googleapis.com/lance.index.VectorIndexDetails",
        Some(MemIndexKind::Hnsw)
    )]
    #[case::bitmap("/lance.table.BitmapIndexDetails", None)]
    #[case::label_list("/lance.table.LabelListIndexDetails", None)]
    #[case::ngram("/lance.table.NGramIndexDetails", None)]
    #[case::zone_map("/lance.table.ZoneMapIndexDetails", None)]
    #[case::bloom_filter("/lance.index.pb.BloomFilterIndexDetails", None)]
    #[case::json("/lance.index.pb.JsonIndexDetails", None)]
    #[case::fm("/lance.index.pb.FMIndexDetails", None)]
    #[case::absent("", None)]
    fn type_urls_resolve_to_the_kind_the_writer_builds(
        #[case] type_url: &str,
        #[case] expected: Option<MemIndexKind>,
    ) {
        assert_eq!(MemIndexKind::from_type_url(type_url), expected);
        assert_eq!(is_maintainable_index_type(type_url), expected.is_some());
    }

    /// `ALL` is hand-maintained, so a kind left out of it stops resolving.
    #[test]
    fn every_kind_is_registered_and_uniquely_identified() {
        for kind in MemIndexKind::ALL {
            assert_eq!(
                MemIndexKind::from_type_url(&format!("/lance.table.{}", kind.details_suffix())),
                Some(*kind),
                "{kind:?} does not resolve from its own suffix",
            );
        }
        let suffixes: std::collections::HashSet<_> = MemIndexKind::ALL
            .iter()
            .map(|k| k.details_suffix())
            .collect();
        assert_eq!(
            suffixes.len(),
            MemIndexKind::ALL.len(),
            "two kinds share a details suffix, so one can never be resolved",
        );
    }

    fn create_test_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
        ]))
    }

    fn create_test_batch(schema: &ArrowSchema, start_id: i32) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![start_id, start_id + 1, start_id + 2])),
                Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
                Arc::new(StringArray::from(vec![
                    "hello world",
                    "goodbye world",
                    "hello again",
                ])),
            ],
        )
        .unwrap()
    }

    fn create_sized_batch(schema: &ArrowSchema, start_id: i32, num_rows: usize) -> RecordBatch {
        let ids: Vec<i32> = (0..num_rows as i32).map(|i| start_id + i).collect();
        let names: Vec<String> = ids.iter().map(|id| format!("name-{id}")).collect();
        let descriptions: Vec<String> = ids.iter().map(|id| format!("hello world {id}")).collect();
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(descriptions)),
            ],
        )
        .unwrap()
    }

    fn fts_index_metadata(index_version: i32) -> IndexMetadata {
        fts_index_metadata_with_details(index_version, None)
    }

    fn fts_index_metadata_with_details(
        index_version: i32,
        details: Option<pbold::InvertedIndexDetails>,
    ) -> IndexMetadata {
        let index_details = details.map(|details| {
            let mut value = Vec::new();
            details.encode(&mut value).unwrap();
            Arc::new(prost_types::Any {
                type_url: "type.googleapis.com/lance.index.InvertedIndexDetails".to_string(),
                value,
            })
        });

        IndexMetadata {
            uuid: Uuid::new_v4(),
            fields: vec![2],
            name: "desc_idx".to_string(),
            dataset_version: 1,
            fragment_bitmap: None,
            index_details,
            index_version,
            created_at: None,
            base_id: None,
            files: None,
        }
    }

    /// Single-column `id` batch for primary-key lookup tests.
    fn id_batch(ids: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "id",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(ids.to_vec()))],
        )
        .unwrap()
    }

    fn id_vector_batch(ids: &[i32]) -> RecordBatch {
        use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                false,
            ),
        ]));
        let mut vectors = FixedSizeListBuilder::new(Float32Builder::new(), 2);
        for id in ids {
            vectors.values().append_value(*id as f32);
            vectors.values().append_value(*id as f32 + 0.5);
            vectors.append(true);
        }
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(vectors.finish()),
            ],
        )
        .unwrap()
    }

    fn id_name_vector_batch(rows: &[(i32, &str)]) -> RecordBatch {
        use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                false,
            ),
        ]));
        let mut ids = Vec::with_capacity(rows.len());
        let mut names = Vec::with_capacity(rows.len());
        let mut vectors = FixedSizeListBuilder::new(Float32Builder::new(), 2);
        for (id, name) in rows {
            ids.push(*id);
            names.push(*name);
            vectors.values().append_value(*id as f32);
            vectors.values().append_value(name.len() as f32);
            vectors.append(true);
        }
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(vectors.finish()),
            ],
        )
        .unwrap()
    }

    #[test]
    fn pk_newest_visible_single_column() {
        let mut store = IndexStore::new();
        store.enable_pk_index(&[("id".to_string(), 0)]);
        // id=1 at positions 0 and 2 (an update), id=2 at position 1.
        store.insert(&id_batch(&[1, 2]), 0).unwrap();
        store.insert(&id_batch(&[1]), 2).unwrap();

        let one = [ScalarValue::Int32(Some(1))];
        // Watermark above the update sees the newest position; below it, the older.
        assert_eq!(store.pk_newest_visible(&one, 5), Some(2));
        assert_eq!(store.pk_newest_visible(&one, 1), Some(0));
        assert!(store.pk_is_newest(&one, 2, 5));
        assert!(!store.pk_is_newest(&one, 0, 5));
        // Absent key (probed by the typed value, as the block-list does).
        assert!(!store.pk_contains_key(&ScalarValue::Int32(Some(9)), 5));
    }

    #[test]
    fn pk_has_overrides_tracks_single_column_rewrites() {
        let mut store = IndexStore::new();
        store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );
        store.enable_pk_index(&[("id".to_string(), 0)]);

        store.insert(&id_vector_batch(&[1, 2]), 0).unwrap();
        assert!(
            !store.pk_has_overrides(),
            "append-only PK inserts should keep HNSW eligible"
        );

        store.insert(&id_vector_batch(&[3, 3]), 2).unwrap();
        assert!(
            store.pk_has_overrides(),
            "duplicate PKs within one insert must disable HNSW"
        );
    }

    #[test]
    #[should_panic(
        expected = "Primary-key indexes must be configured before inserting rows into a search-indexed memtable"
    )]
    fn enable_pk_index_after_search_rows_panics() {
        let mut store = IndexStore::new();
        store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );
        store.insert(&id_vector_batch(&[1, 2]), 0).unwrap();

        store.enable_pk_index(&[("id".to_string(), 0)]);
    }

    #[test]
    fn pk_has_overrides_tracks_single_column_rewrites_across_inserts() {
        let mut store = IndexStore::new();
        store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );
        store.enable_pk_index(&[("id".to_string(), 0)]);

        store.insert(&id_vector_batch(&[1, 2]), 0).unwrap();
        assert!(
            !store.pk_has_overrides(),
            "append-only PK inserts should keep HNSW eligible"
        );

        store.insert(&id_vector_batch(&[1]), 2).unwrap();
        assert!(
            store.pk_has_overrides(),
            "single-column PK rewrites across inserts must disable HNSW"
        );
    }

    #[test]
    fn pk_has_overrides_skips_scalar_only_tables() {
        let mut store = IndexStore::new();
        store.enable_pk_index(&[("id".to_string(), 0)]);

        store.insert(&id_batch(&[1, 1]), 0).unwrap();
        assert!(
            !store.pk_has_overrides(),
            "scalar-only PK tables should not pay override tracking cost"
        );
    }

    #[test]
    fn pk_has_overrides_tracks_fts_rewrites() {
        let mut store = IndexStore::new();
        store.enable_pk_index(&[("id".to_string(), 0)]);
        store.add_fts("text_fts".to_string(), 1, "text".to_string());

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(StringArray::from(vec!["alpha", "beta"])),
            ],
        )
        .unwrap();
        store.insert(&batch, 0).unwrap();
        assert!(
            store.pk_has_overrides(),
            "FTS PK rewrites must disable index-level FTS limit/WAND pushdown"
        );
    }

    #[test]
    fn pk_newest_visible_composite_seeks_encoded_tuple() {
        let mut store = IndexStore::new();
        store.enable_pk_index(&[("id".to_string(), 0), ("name".to_string(), 1)]);
        // Rows: (1,"a")@0, (1,"b")@1, (1,"a")@2 — an update of (1,"a").
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1])),
                Arc::new(StringArray::from(vec!["a", "b", "a"])),
            ],
        )
        .unwrap();
        store.insert(&batch, 0).unwrap();

        let tuple_1a = [ScalarValue::Int32(Some(1)), ScalarValue::from("a")];
        let tuple_1b = [ScalarValue::Int32(Some(1)), ScalarValue::from("b")];
        // (1,"a")'s newest visible row is its re-write at position 2.
        assert_eq!(store.pk_newest_visible(&tuple_1a, 5), Some(2));
        assert!(store.pk_is_newest(&tuple_1a, 2, 5));
        assert!(!store.pk_is_newest(&tuple_1a, 0, 5));
        // (1,"b") only exists at position 1.
        assert_eq!(store.pk_newest_visible(&tuple_1b, 5), Some(1));
        // Watermark below the re-write: the older (1,"a")@0 is the newest visible.
        assert_eq!(store.pk_newest_visible(&tuple_1a, 1), Some(0));
        // An absent tuple (probed by its Binary-encoded key, as the block-list
        // does).
        let tuple_2a = [ScalarValue::Int32(Some(2)), ScalarValue::from("a")];
        let key_2a = ScalarValue::Binary(Some(encode_pk_tuple(&tuple_2a).unwrap()));
        assert!(!store.pk_contains_key(&key_2a, 5));
    }

    #[test]
    fn pk_has_overrides_tracks_composite_rewrites() {
        let mut store = IndexStore::new();
        store.add_hnsw(
            "vector_hnsw".to_string(),
            2,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );
        store.enable_pk_index(&[("id".to_string(), 0), ("name".to_string(), 1)]);
        let first = id_name_vector_batch(&[(1, "a"), (1, "b")]);
        store.insert(&first, 0).unwrap();
        assert!(!store.pk_has_overrides());

        let rewrite = id_name_vector_batch(&[(1, "a")]);
        store.insert(&rewrite, 2).unwrap();
        assert!(
            store.pk_has_overrides(),
            "repeated composite PK must disable HNSW"
        );
    }

    #[test]
    fn test_index_registry() {
        let schema = create_test_schema();
        let mut registry = IndexStore::new();

        // field_id 0 for "id" column, field_id 2 for "description" column
        registry.add_btree("id_idx".to_string(), 0, "id".to_string());
        registry.add_fts("desc_idx".to_string(), 2, "description".to_string());

        assert_eq!(registry.len(), 2);

        let batch = create_test_batch(&schema, 0);
        registry.insert(&batch, 0).unwrap();

        let btree = registry.get_btree("id_idx").unwrap();
        assert_eq!(btree.len(), 3);

        let fts = registry.get_fts("desc_idx").unwrap();
        assert_eq!(fts.doc_count(), 3);
    }

    #[test]
    fn fts_registry_routes_row_and_element_targets_independently() {
        let mut registry = IndexStore::new();
        registry
            .add_fts_with_params(
                "tags_idx".to_string(),
                1,
                "tags".to_string(),
                InvertedIndexParams::default(),
            )
            .unwrap();
        registry
            .add_fts_with_params(
                "tags_element_idx".to_string(),
                1,
                "tags".to_string(),
                InvertedIndexParams::default().document_granularity(
                    lance_index::scalar::inverted::DocumentGranularity::ListElement,
                ),
            )
            .unwrap();

        assert_eq!(
            registry.get_fts_by_column("tags").unwrap().column_name(),
            "tags"
        );
        assert_eq!(
            registry
                .get_fts_by_column_and_granularity(
                    "tags",
                    lance_index::scalar::inverted::DocumentGranularity::ListElement,
                )
                .unwrap()
                .column_name(),
            "tags"
        );
    }

    #[test]
    fn fts_from_metadata_preserves_format_version() {
        let arrow_schema = create_test_schema();
        let schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();

        for (index_version, expected_format_version) in [
            (0, InvertedListFormatVersion::V1),
            (1, InvertedListFormatVersion::V1),
            (2, InvertedListFormatVersion::V2),
            (3, InvertedListFormatVersion::V3),
        ] {
            let config =
                MemIndexConfig::fts_from_metadata(&fts_index_metadata(index_version), &schema)
                    .unwrap();

            match config {
                MemIndexConfig::Fts(config) => {
                    assert_eq!(
                        config.params.resolved_format_version(),
                        expected_format_version
                    );
                }
                _ => unreachable!("fts metadata should create an FTS config"),
            }
        }
    }

    #[test]
    fn fts_from_metadata_rejects_unsupported_format_version() {
        let arrow_schema = create_test_schema();
        let schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();

        let err = MemIndexConfig::fts_from_metadata(&fts_index_metadata(4), &schema).unwrap_err();
        assert!(
            err.to_string().contains("unsupported index_version 4"),
            "{err}"
        );
    }

    #[test]
    fn fts_from_metadata_accepts_element_document_v3_capability() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]));
        let schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();
        let tags = schema.field("tags").unwrap();
        for (block_size, expected_format_version) in [
            (128, InvertedListFormatVersion::V2),
            (256, InvertedListFormatVersion::V3),
        ] {
            let params = InvertedIndexParams::default()
                .block_size(block_size)
                .unwrap()
                .document_granularity(
                    lance_index::scalar::inverted::DocumentGranularity::ListElement,
                );
            let details = pbold::InvertedIndexDetails::try_from(&params).unwrap();
            let mut metadata = fts_index_metadata_with_details(3, Some(details));
            metadata.fields = vec![tags.id];
            let config = MemIndexConfig::fts_from_metadata(&metadata, &schema).unwrap();

            let MemIndexConfig::Fts(config) = config else {
                unreachable!("fts metadata should create an FTS config")
            };
            assert_eq!(config.field_id, tags.id);
            assert_eq!(config.column, "tags");
            assert_eq!(
                config.params.get_document_granularity(),
                lance_index::scalar::inverted::DocumentGranularity::ListElement
            );
            assert_eq!(
                config.params.resolved_format_version(),
                expected_format_version
            );
        }
    }

    #[test]
    fn fts_from_metadata_accepts_v3_with_legacy_block_size() {
        let arrow_schema = create_test_schema();
        let schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();
        let mut legacy_details =
            pbold::InvertedIndexDetails::try_from(&InvertedIndexParams::default()).unwrap();
        legacy_details.posting_format_version = None;

        for metadata in [
            fts_index_metadata(3),
            fts_index_metadata_with_details(3, Some(legacy_details)),
        ] {
            let config = MemIndexConfig::fts_from_metadata(&metadata, &schema).unwrap();
            let MemIndexConfig::Fts(config) = config else {
                unreachable!("FTS metadata should create an FTS config");
            };
            assert_eq!(
                config.params.resolved_format_version(),
                InvertedListFormatVersion::V3
            );
            assert_eq!(config.params.posting_block_size(), 128);
        }
    }

    #[test]
    fn fts_from_metadata_accepts_v3_with_256_block_size() {
        let arrow_schema = create_test_schema();
        let schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();
        let params = InvertedIndexParams::default().block_size(256).unwrap();
        let details = pbold::InvertedIndexDetails::try_from(&params).unwrap();

        let config = MemIndexConfig::fts_from_metadata(
            &fts_index_metadata_with_details(3, Some(details)),
            &schema,
        )
        .unwrap();

        match config {
            MemIndexConfig::Fts(config) => {
                assert_eq!(
                    config.params.resolved_format_version(),
                    InvertedListFormatVersion::V3
                );
                assert_eq!(config.params.posting_block_size(), 256);
            }
            _ => unreachable!("fts metadata should create an FTS config"),
        }
    }

    #[test]
    fn test_from_configs() {
        let configs = vec![
            MemIndexConfig::BTree(BTreeIndexConfig {
                name: "pk_idx".to_string(),
                field_id: 0,
                column: "id".to_string(),
            }),
            MemIndexConfig::Fts(FtsIndexConfig::new(
                "search_idx".to_string(),
                2,
                "description".to_string(),
            )),
        ];

        let registry = IndexStore::from_configs(&configs, 100_000, 1_000).unwrap();
        assert_eq!(registry.len(), 2);
        assert!(registry.get_btree("pk_idx").is_some());
        assert!(registry.get_fts("search_idx").is_some());
        // Also test field_id lookup
        assert!(registry.get_btree_by_field_id(0).is_some());
        assert!(registry.get_fts_by_field_id(2).is_some());
    }

    fn vector_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("description", DataType::Utf8, true),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                true,
            ),
            Field::new(
                "f64_vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 4),
                true,
            ),
        ]))
    }

    /// Every index config that would fail *deterministically* on insert must be
    /// rejected at open instead. Such a config also fails on WAL replay, so once
    /// a row is durable the shard could never reopen — poison-and-replay would
    /// not terminate.
    #[rstest]
    #[case::btree_ok(MemIndexConfig::BTree(BTreeIndexConfig {
        name: "idx".into(), field_id: 0, column: "id".into(),
    }), None)]
    #[case::btree_missing_column(MemIndexConfig::BTree(BTreeIndexConfig {
        name: "idx".into(), field_id: 9, column: "nope".into(),
    }), Some("not in the shard schema"))]
    // Column exists, but its field_id names a *different* column ("id" is 0, not 1).
    #[case::btree_field_id_column_mismatch(MemIndexConfig::BTree(BTreeIndexConfig {
        name: "idx".into(), field_id: 1, column: "id".into(),
    }), Some("has field_id 0"))]
    #[case::fts_ok(MemIndexConfig::Fts(FtsIndexConfig::new(
        "idx".into(), 1, "description".into(),
    )), None)]
    #[case::fts_non_utf8(MemIndexConfig::Fts(FtsIndexConfig::new(
        "idx".into(), 0, "id".into(),
    )), Some("must resolve to Utf8, LargeUtf8, Utf8View, or JSON"))]
    #[case::fts_missing_column(MemIndexConfig::Fts(FtsIndexConfig::new(
        "idx".into(), 9, "nope".into(),
    )), Some("does not exist in the dataset schema"))]
    #[case::hnsw_ok(MemIndexConfig::Hnsw(Box::new(HnswIndexConfig::new(
        "idx".into(), 2, "vector".into(), DistanceType::L2,
    ))), None)]
    #[case::hnsw_not_a_vector(MemIndexConfig::Hnsw(Box::new(HnswIndexConfig::new(
        "idx".into(), 0, "id".into(), DistanceType::L2,
    ))), Some("requires a FixedSizeList<Float32> column"))]
    #[case::hnsw_wrong_item_type(MemIndexConfig::Hnsw(Box::new(HnswIndexConfig::new(
        "idx".into(), 3, "f64_vector".into(), DistanceType::L2,
    ))), Some("item type Float64"))]
    #[case::hnsw_missing_column(MemIndexConfig::Hnsw(Box::new(HnswIndexConfig::new(
        "idx".into(), 9, "nope".into(), DistanceType::L2,
    ))), Some("not in the shard schema"))]
    fn test_validate_index_configs(
        #[case] config: MemIndexConfig,
        #[case] expected_error: Option<&str>,
    ) {
        let schema = vector_schema();
        let lance_schema = LanceSchema::try_from(schema.as_ref()).unwrap();
        let result = validate_index_configs(&[config], &schema, &lance_schema, &[]);
        match expected_error {
            None => result.expect("valid config must pass validation"),
            Some(fragment) => {
                let message = result
                    .expect_err("invalid config must be rejected")
                    .to_string();
                assert!(
                    message.contains(fragment),
                    "error must explain the mismatch; wanted {fragment:?}, got {message:?}"
                );
            }
        }
    }

    #[test]
    fn test_validate_nested_fts_index_config() {
        let content_fields = Fields::from(vec![Field::new("content", DataType::Utf8, true)]);
        let doc_item = Arc::new(Field::new("item", DataType::Struct(content_fields), true));
        let group_fields = Fields::from(vec![Field::new("docs", DataType::List(doc_item), true)]);
        let group_item = Arc::new(Field::new("item", DataType::Struct(group_fields), true));
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "groups",
            DataType::List(group_item),
            true,
        )]));
        let lance_schema = LanceSchema::try_from(schema.as_ref()).unwrap();
        let resolved = crate::index::scalar::inverted::resolve_fts_field(
            &lance_schema,
            "groups.docs.content",
            lance_index::scalar::inverted::DocumentGranularity::ListElement,
        )
        .unwrap();

        let params = InvertedIndexParams::default()
            .document_granularity(lance_index::scalar::inverted::DocumentGranularity::ListElement);
        let config = MemIndexConfig::Fts(FtsIndexConfig::with_params(
            "idx".into(),
            resolved.final_field_id,
            "groups.docs.content".into(),
            params.clone(),
        ));
        validate_index_configs(&[config], &schema, &lance_schema, &[]).unwrap();

        let wrong_field_id = MemIndexConfig::Fts(FtsIndexConfig::with_params(
            "idx".into(),
            resolved.final_field_id + 1,
            "groups.docs.content".into(),
            params,
        ));
        let error =
            validate_index_configs(&[wrong_field_id], &schema, &lance_schema, &[]).unwrap_err();
        assert!(error.to_string().contains("final field_id"), "{error}");
    }

    #[test]
    fn test_validate_index_configs_rejects_diverged_lance_schema() {
        let arrow_schema = ArrowSchema::new(vec![Field::new("id", DataType::Int32, false)]);
        let lance_schema = LanceSchema::try_from(&ArrowSchema::new(vec![Field::new(
            "other",
            DataType::Int32,
            false,
        )]))
        .expect("test Lance schema must be valid");
        let config = MemIndexConfig::BTree(BTreeIndexConfig {
            name: "idx".into(),
            field_id: 0,
            column: "id".into(),
        });

        let error = validate_index_configs(&[config], &arrow_schema, &lance_schema, &[])
            .expect_err("diverged Arrow and Lance schemas must be rejected");
        assert!(
            matches!(error, Error::InvalidInput { .. }),
            "expected InvalidInput, got {error:?}"
        );
        let message = error.to_string();
        assert!(
            message.contains("index 'idx'"),
            "error must name the index: {message}"
        );
        assert!(
            message.contains("column 'id'"),
            "error must name the column: {message}"
        );
        assert!(
            message.contains("absent from the Lance schema"),
            "error must explain the schema divergence: {message}"
        );
    }

    /// A composite PK builds an order-preserving encoded key, so its columns must
    /// be encodable. A single-column PK aliases a BTree entry, which accepts any
    /// type — so it must *not* be rejected here.
    #[test]
    fn test_validate_composite_pk_column_types() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "coords",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            ),
        ]));
        let lance_schema = LanceSchema::try_from(schema.as_ref()).unwrap();

        validate_index_configs(&[], &schema, &lance_schema, &["id".into(), "name".into()])
            .expect("Int32 + Utf8 composite PK must be encodable");

        let err =
            validate_index_configs(&[], &schema, &lance_schema, &["id".into(), "coords".into()])
                .expect_err("a FixedSizeList PK column has no order-preserving encoding");
        assert!(
            err.to_string().contains("order-preserving key encoding"),
            "error must name the reason, got {err}"
        );

        // A single-column PK of the same type is fine: it aliases a BTree.
        validate_index_configs(&[], &schema, &lance_schema, &["coords".into()])
            .expect("single-column PK aliases a BTree and accepts any type");

        // But every PK column must exist. A single-column PK naming an absent
        // column is rejected here, not left to fail deterministically on every
        // later index build and WAL replay.
        let err = validate_index_configs(&[], &schema, &lance_schema, &["missing".into()])
            .expect_err("a single-column PK on an absent column must be rejected");
        assert!(
            err.to_string().contains("not in the shard schema"),
            "error must name the missing column, got {err}"
        );
    }

    #[test]
    fn test_index_store_indexed_count() {
        let schema = create_test_schema();
        let mut registry = IndexStore::new();

        // field_id 0 for "id" column, field_id 2 for "description" column
        registry.add_btree("id_idx".to_string(), 0, "id".to_string());
        registry.add_fts("desc_idx".to_string(), 2, "description".to_string());

        // Initial watermark should be 0 (no data indexed yet)
        assert_eq!(registry.indexed_count(), 0);

        // Insert with batch position tracking
        let batch = create_test_batch(&schema, 0);
        registry
            .insert_with_batch_position(&batch, 0, Some(5))
            .unwrap();

        // Indexing batch position 5 means the prefix [0, 6) is indexed.
        assert_eq!(registry.indexed_count(), 6);

        // Insert with higher batch position
        registry
            .insert_with_batch_position(&batch, 3, Some(10))
            .unwrap();

        // Advances to cover batch position 10.
        assert_eq!(registry.indexed_count(), 11);

        // Insert without batch position shouldn't change the cursor
        registry.insert(&batch, 6).unwrap();
        assert_eq!(registry.indexed_count(), 11);
    }

    /// `insert_batches` picks the inline or the threaded path by row count, so
    /// exercise both and assert they leave the same index state: every row indexed
    /// exactly once, in every index, with a timing reported for each.
    #[rstest]
    #[case::inline(8)]
    #[case::threaded(PARALLEL_INDEX_MIN_ROWS + 64)]
    fn test_insert_batches_indexes_every_row_once(#[case] num_rows: usize) {
        let schema = create_test_schema();
        let mut registry = IndexStore::new();
        registry.add_btree("id_idx".to_string(), 0, "id".to_string());
        registry.add_fts("desc_idx".to_string(), 2, "description".to_string());

        let batch = create_sized_batch(&schema, 0, num_rows);
        let durations = registry
            .insert_batches(&[StoredBatch::new(batch, 0, 2)])
            .unwrap();

        assert_eq!(durations.len(), 2, "expected one timing per index");
        assert!(durations.contains_key("id_idx"));
        assert!(durations.contains_key("desc_idx"));

        let btree = registry.get_btree("id_idx").unwrap();
        for id in 0..num_rows as i32 {
            let positions = btree.get(&ScalarValue::Int32(Some(id)));
            assert_eq!(
                positions.len(),
                1,
                "id={id} should be indexed exactly once, got {positions:?}"
            );
        }
        assert_eq!(registry.get_fts("desc_idx").unwrap().doc_count(), num_rows);
        assert_eq!(registry.indexed_count(), 3);
    }

    #[test]
    fn test_get_index_by_name_and_field_id() {
        let mut registry = IndexStore::new();
        // field_id 0 for "id" column, field_id 2 for "description" column
        registry.add_btree("id_idx".to_string(), 0, "id".to_string());
        registry.add_fts("desc_idx".to_string(), 2, "description".to_string());

        // Lookup by name
        assert!(registry.get_btree("id_idx").is_some());
        assert!(registry.get_btree("nonexistent").is_none());
        assert!(registry.get_fts("desc_idx").is_some());
        assert!(registry.get_fts("id_idx").is_none());

        // Lookup by field ID
        assert!(registry.get_btree_by_field_id(0).is_some());
        assert!(registry.get_btree_by_field_id(999).is_none());
        assert!(registry.get_fts_by_field_id(2).is_some());
        assert!(registry.get_fts_by_field_id(0).is_none());

        // Lookup by column name
        assert!(registry.get_btree_by_column("id").is_some());
        assert!(registry.get_btree_by_column("nonexistent").is_none());
        assert!(registry.get_fts_by_column("description").is_some());
    }
}
