// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! MemWAL LSM write path for `merge_insert`.
//!
//! [`set_lsm_write_spec`] installs an [`LsmWriteSpec`] on a table by creating
//! Lance's MemWAL index; [`unset_lsm_write_spec`] removes it. Once a spec is
//! installed, `merge_insert` upsert calls are dispatched through Lance's MemWAL
//! `ShardWriter` (LSM-style append) instead of the standard merge path — see
//! [`lsm_dispatch_decision`] and [`execute_lsm_merge_insert`].
//!
//! Each `merge_insert` call must target a single shard: every row must route
//! to the same shard under the installed sharding spec (bucket / identity /
//! unsharded). [`MergeInsertBuilder::validate_single_shard`] controls whether
//! every row is checked or only the first. A dataset writes to one shard at a
//! time; its writer is cached in the [`ShardWriterCache`] held alongside the
//! dataset, and [`close_lsm_writers`] closes it.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, RecordBatch, RecordBatchReader};
use arrow_schema::{DataType, Schema as ArrowSchema, SchemaRef};
use lance::Dataset;
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardWriter, ShardWriterConfig};
use lance::index::DatasetIndexExt;
use lance_index::mem_wal::{MemWalIndexDetails, ShardingField};
use murmur3::murmur3_32;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::table::merge::{MergeInsertBuilder, MergeResult};
use crate::table::{LsmWriteSpec, NativeTable};

/// Spec id of the sole sharding spec installed by [`set_lsm_write_spec`].
/// Must match Lance's `InitializeMemWalBuilder` (`SHARDING_SPEC_ID`).
const SHARDING_SPEC_ID: u32 = 1;

/// Transform name recorded by `bucket_sharding`.
const BUCKET_TRANSFORM: &str = "bucket";
/// Transform name recorded by `identity_sharding`.
const IDENTITY_TRANSFORM: &str = "identity";
/// Transform name recorded by `unsharded`.
const UNSHARDED_TRANSFORM: &str = "unsharded";

/// Parameter key holding the bucket count on the bucket transform.
const NUM_BUCKETS_PARAM: &str = "num_buckets";

/// Murmur3-x86-32 seed. Iceberg's `bucket` transform uses 0; matching it keeps
/// the row-to-bucket assignment stable and Iceberg-compatible.
const MURMUR3_SEED: u32 = 0;

/// Fixed namespace UUID for deriving deterministic shard ids. Hardcoded so
/// derivations stay stable across processes.
const SHARD_NAMESPACE: Uuid = Uuid::from_u128(0x4c53_4d57_5249_5445_5f53_4841_5244_3031);

// =============================================================================
// set_lsm_write_spec
// =============================================================================

/// Install an [`LsmWriteSpec`] on the table.
///
/// The bucket / identity / unsharded sharding spec is constructed and validated
/// by Lance's
/// [`InitializeMemWalBuilder`](lance::dataset::mem_wal::InitializeMemWalBuilder).
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn set_lsm_write_spec(table: &NativeTable, spec: LsmWriteSpec) -> Result<()> {
    table.dataset.ensure_mutable()?;

    {
        let dataset = table.dataset.get().await?;
        if dataset.mem_wal_index_details().await?.is_some() {
            return Err(Error::InvalidInput {
                message: "set_lsm_write_spec: an LSM write spec is already set on this table; mutation is not supported".into(),
            });
        }
    }

    let mut dataset = (*table.dataset.get().await?).clone();
    let mut builder = dataset.initialize_mem_wal();
    let (maintained_indexes, writer_config_defaults) = match spec {
        LsmWriteSpec::Bucket {
            column,
            num_buckets,
            maintained_indexes,
            writer_config_defaults,
        } => {
            builder = builder.bucket_sharding(column, num_buckets);
            (maintained_indexes, writer_config_defaults)
        }
        LsmWriteSpec::Identity {
            column,
            maintained_indexes,
            writer_config_defaults,
        } => {
            builder = builder.identity_sharding(column);
            (maintained_indexes, writer_config_defaults)
        }
        LsmWriteSpec::Unsharded {
            maintained_indexes,
            writer_config_defaults,
        } => {
            builder = builder.unsharded();
            (maintained_indexes, writer_config_defaults)
        }
    };
    builder = builder.maintained_indexes(maintained_indexes);
    for (key, value) in writer_config_defaults {
        builder = builder.add_writer_config_default(key, value);
    }
    builder.execute().await?;
    table.dataset.update(dataset);
    Ok(())
}

// =============================================================================
// unset_lsm_write_spec
// =============================================================================

/// Remove the [`LsmWriteSpec`] from the table by dropping the MemWAL index.
///
/// Any cached shard writers are drained and closed first. Errors if no spec is
/// currently set.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn unset_lsm_write_spec(table: &NativeTable) -> Result<()> {
    table.dataset.ensure_mutable()?;

    {
        let dataset = table.dataset.get().await?;
        if dataset.mem_wal_index_details().await?.is_none() {
            return Err(Error::InvalidInput {
                message: "unset_lsm_write_spec: no LSM write spec is set on this table".into(),
            });
        }
    }

    table.dataset.shard_writer().drain_and_close().await?;

    let mut dataset = (*table.dataset.get().await?).clone();
    dataset
        .drop_index(lance_index::mem_wal::MEM_WAL_INDEX_NAME)
        .await?;
    table.dataset.update(dataset);
    Ok(())
}

// =============================================================================
// close_lsm_writers
// =============================================================================

/// Drain and close every cached MemWAL shard writer for the table.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn close_lsm_writers(table: &NativeTable) -> Result<()> {
    table.dataset.shard_writer().drain_and_close().await
}

// =============================================================================
// ShardWriter cache
// =============================================================================

/// Per-table cache holding the single open MemWAL `ShardWriter`.
///
/// Held by [`DatasetConsistencyWrapper`](crate::table::dataset::DatasetConsistencyWrapper)
/// so the writer lives where the dataset lives — cached for the session and
/// reused across `merge_insert` calls. A dataset writes to one shard at a
/// time; routing a `merge_insert` to a different shard requires closing the
/// current writer first via [`close_lsm_writers`]. `ShardWriter::put` takes
/// `&self`, so concurrent puts on the cached writer are safe; `close` consumes
/// the writer, so the entry wraps it in `RwLock<Option<ShardWriter>>`.
#[derive(Default)]
#[allow(clippy::redundant_pub_crate)]
pub(crate) struct ShardWriterCache {
    /// `Some((shard_id, entry))` once a writer has been opened for the session.
    slot: RwLock<Option<(Uuid, Arc<ShardWriterEntry>)>>,
}

impl std::fmt::Debug for ShardWriterCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardWriterCache").finish_non_exhaustive()
    }
}

struct ShardWriterEntry {
    inner: RwLock<Option<ShardWriter>>,
}

impl ShardWriterEntry {
    fn new(writer: ShardWriter) -> Self {
        Self {
            inner: RwLock::new(Some(writer)),
        }
    }

    async fn put(&self, batches: Vec<RecordBatch>) -> Result<()> {
        let guard = self.inner.read().await;
        let writer = guard.as_ref().ok_or_else(|| Error::Runtime {
            message: "merge_insert: shard writer was closed before this write".to_string(),
        })?;
        writer.put(batches).await.map_err(|e| Error::Runtime {
            message: format!("merge_insert: shard writer put failed: {}", e),
        })?;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        let writer = { self.inner.write().await.take() };
        if let Some(writer) = writer {
            writer.close().await.map_err(|e| Error::Runtime {
                message: format!("merge_insert: shard writer close failed: {}", e),
            })?;
        }
        Ok(())
    }
}

impl ShardWriterCache {
    /// Return the cached writer, opening one for `shard_id` with `config` if
    /// the slot is empty. Errors if a writer is already open for a *different*
    /// shard — the caller must close it first.
    async fn writer_for_shard(
        &self,
        dataset: &Dataset,
        shard_id: Uuid,
        config: ShardWriterConfig,
    ) -> Result<Arc<ShardWriterEntry>> {
        {
            let guard = self.slot.read().await;
            if let Some((cached, entry)) = guard.as_ref() {
                check_shard_match(*cached, shard_id)?;
                return Ok(entry.clone());
            }
        }
        let mut guard = self.slot.write().await;
        // Re-check: another caller may have opened the writer meanwhile.
        if let Some((cached, entry)) = guard.as_ref() {
            check_shard_match(*cached, shard_id)?;
            return Ok(entry.clone());
        }
        let writer = dataset
            .mem_wal_writer(shard_id, config)
            .await
            .map_err(|e| Error::Runtime {
                message: format!(
                    "merge_insert: failed to open MemWAL shard writer for shard {}: {}",
                    shard_id, e
                ),
            })?;
        let entry = Arc::new(ShardWriterEntry::new(writer));
        *guard = Some((shard_id, entry.clone()));
        Ok(entry)
    }

    /// Close the cached writer, if any, and clear the slot.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) async fn drain_and_close(&self) -> Result<()> {
        let cached = { self.slot.write().await.take() };
        if let Some((_, entry)) = cached {
            entry.close().await?;
        }
        Ok(())
    }
}

/// Error if a cached writer is open for a shard other than the one needed.
fn check_shard_match(cached: Uuid, wanted: Uuid) -> Result<()> {
    if cached == wanted {
        return Ok(());
    }
    Err(Error::InvalidInput {
        message: format!(
            "merge_insert: a shard writer is already open for shard {} but this input routes to shard {}; call close_lsm_writers before writing to a different shard",
            cached, wanted
        ),
    })
}

// =============================================================================
// merge_insert LSM dispatch
// =============================================================================

/// How the installed sharding spec routes rows to shards.
#[derive(Debug, Clone)]
enum LsmMode {
    /// Hash-bucket the routing column into `num_buckets` shards.
    Bucket {
        routing_col: String,
        num_buckets: u32,
    },
    /// Shard by the raw value of the routing column.
    Identity { routing_col: String },
    /// Route every row to a single shard.
    Unsharded,
}

/// Resolved plan for routing a `merge_insert` through the MemWAL write path.
#[derive(Debug)]
#[allow(clippy::redundant_pub_crate)]
pub(crate) struct LsmPlan {
    mode: LsmMode,
    writer_config_defaults: HashMap<String, String>,
}

/// Outcome of [`lsm_dispatch_decision`].
#[allow(clippy::redundant_pub_crate)]
pub(crate) enum LsmDispatch {
    /// No LSM write spec applies; use the standard `merge_insert` path.
    Standard,
    /// Route the `merge_insert` through the MemWAL shard writer.
    Lsm(LsmPlan),
}

/// Decide whether a `merge_insert` should be routed through the MemWAL write
/// path, validating the builder against the installed spec.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn lsm_dispatch_decision(
    table: &NativeTable,
    params: &MergeInsertBuilder,
) -> Result<LsmDispatch> {
    // `Some(false)` is an explicit opt-out: use the standard path.
    if params.use_lsm_write == Some(false) {
        return Ok(LsmDispatch::Standard);
    }

    let dataset = table.dataset.get().await?;
    let Some(details) = dataset.mem_wal_index_details().await? else {
        // No LSM write spec installed. `Some(true)` explicitly asked for the
        // LSM path, which is meaningless without a spec; `None` (the default)
        // just falls back to the standard path.
        if params.use_lsm_write == Some(true) {
            return Err(Error::InvalidInput {
                message: "merge_insert: use_lsm_write(true) requires an LSM write spec on the table; call set_lsm_write_spec first".to_string(),
            });
        }
        return Ok(LsmDispatch::Standard);
    };

    let pk_cols: Vec<String> = dataset
        .schema()
        .unenforced_primary_key()
        .iter()
        .map(|f| f.name.clone())
        .collect();
    if pk_cols.is_empty() {
        return Err(Error::Runtime {
            message: "merge_insert: table has a MemWAL index but no unenforced primary key"
                .to_string(),
        });
    }
    if !params.on.is_empty() && params.on != pk_cols {
        return Err(Error::InvalidInput {
            message: format!(
                "merge_insert: `on` columns {:?} must match the table's unenforced primary key {:?} when an LSM write spec is set; pass an empty `on` to default to the primary key",
                params.on, pk_cols
            ),
        });
    }

    if !is_upsert_only(params) {
        return Err(Error::InvalidInput {
            message: "merge_insert: when an LSM write spec is set, only the upsert form (when_matched_update_all without a filter + when_not_matched_insert_all, no by-source delete) is supported; call use_lsm_write(false) to use the standard merge_insert path".to_string(),
        });
    }

    let mode = resolve_lsm_mode(&details, dataset.as_ref())?;
    Ok(LsmDispatch::Lsm(LsmPlan {
        mode,
        writer_config_defaults: details.writer_config_defaults,
    }))
}

/// Returns true if the builder requests the upsert-only shape the LSM write
/// path can honor.
fn is_upsert_only(params: &MergeInsertBuilder) -> bool {
    params.when_matched_update_all
        && params.when_matched_update_all_filt.is_none()
        && params.when_not_matched_insert_all
        && !params.when_not_matched_by_source_delete
        && params.when_not_matched_by_source_delete_filt.is_none()
}

/// Read the sharding mode from the MemWAL index details.
fn resolve_lsm_mode(details: &MemWalIndexDetails, dataset: &Dataset) -> Result<LsmMode> {
    let field = details
        .sharding_specs
        .first()
        .and_then(|spec| spec.fields.first())
        .ok_or_else(|| Error::Runtime {
            message: "merge_insert: MemWAL index has no sharding spec".to_string(),
        })?;
    match field.transform.as_deref() {
        Some(BUCKET_TRANSFORM) => {
            let num_buckets = field
                .parameters
                .get(NUM_BUCKETS_PARAM)
                .and_then(|s| s.parse::<u32>().ok())
                .filter(|n| *n > 0)
                .ok_or_else(|| Error::Runtime {
                    message: "merge_insert: MemWAL bucket spec has a missing or invalid num_buckets parameter".to_string(),
                })?;
            Ok(LsmMode::Bucket {
                routing_col: routing_column(field, dataset)?,
                num_buckets,
            })
        }
        Some(IDENTITY_TRANSFORM) => Ok(LsmMode::Identity {
            routing_col: routing_column(field, dataset)?,
        }),
        Some(UNSHARDED_TRANSFORM) => Ok(LsmMode::Unsharded),
        other => Err(Error::Runtime {
            message: format!(
                "merge_insert: MemWAL index has an unsupported sharding transform {:?}",
                other
            ),
        }),
    }
}

/// Resolve the routing column name from a sharding field's source id.
fn routing_column(field: &ShardingField, dataset: &Dataset) -> Result<String> {
    let field_id = *field.source_ids.first().ok_or_else(|| Error::Runtime {
        message: "merge_insert: MemWAL sharding spec has no source field".to_string(),
    })?;
    let schema_field = dataset
        .schema()
        .field_by_id(field_id)
        .ok_or_else(|| Error::Runtime {
            message: format!(
                "merge_insert: MemWAL sharding spec references field id {} which is not in the schema",
                field_id
            ),
        })?;
    Ok(schema_field.name.clone())
}

// =============================================================================
// LSM merge_insert execution
// =============================================================================

/// Execute a `merge_insert` through the MemWAL shard writer cache.
///
/// The entire input is collected, schema-aligned, and shard-validated before
/// anything is written, then issued as a single atomic `ShardWriter::put` — so
/// a validation failure (e.g. input spanning shards) never leaves a partial
/// write behind. When `validate_single_shard` is set, every row is checked to
/// route to one shard; when disabled, only the first row of each batch is.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn execute_lsm_merge_insert(
    table: &NativeTable,
    plan: LsmPlan,
    validate_single_shard: bool,
    new_data: Box<dyn RecordBatchReader + Send>,
) -> Result<MergeResult> {
    let dataset = table.dataset.get().await?;
    let target_schema: SchemaRef = Arc::new(ArrowSchema::from(dataset.schema()));

    // Collect, align and shard-validate the whole input before writing
    // anything. `ShardWriter::put` is atomic over the batch vector, so any
    // failure raised here leaves the MemWAL untouched.
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut shard_id: Option<Uuid> = None;
    let mut total_rows: u64 = 0;

    for batch in new_data {
        let batch = batch.map_err(|e| Error::Arrow { source: e })?;
        if batch.num_rows() == 0 {
            continue;
        }
        let batch = align_batch_schema(batch, &target_schema)?;
        let batch_shard = resolve_batch_shard(&plan.mode, &batch, validate_single_shard)?;
        match shard_id {
            Some(seen) if seen != batch_shard => {
                return Err(Error::InvalidInput {
                    message: "merge_insert: input batches route to multiple shards; each merge_insert call must target a single shard".to_string(),
                });
            }
            _ => shard_id = Some(batch_shard),
        }
        total_rows += batch.num_rows() as u64;
        batches.push(batch);
    }

    // Empty input (or only empty batches): nothing to write.
    let Some(shard_id) = shard_id else {
        return Ok(lsm_merge_result(0));
    };

    let config = shard_writer_config_from_defaults(&plan.writer_config_defaults);
    let writer = table
        .dataset
        .shard_writer()
        .writer_for_shard(dataset.as_ref(), shard_id, config)
        .await?;
    writer.put(batches).await?;

    Ok(lsm_merge_result(total_rows))
}

/// Compute the target shard id for a non-empty batch. When
/// `validate_single_shard` is set, every row is checked to route to the same
/// shard; otherwise only the first row is inspected.
fn resolve_batch_shard(
    mode: &LsmMode,
    batch: &RecordBatch,
    validate_single_shard: bool,
) -> Result<Uuid> {
    match mode {
        LsmMode::Unsharded => Ok(unsharded_shard_id()),
        LsmMode::Bucket {
            routing_col,
            num_buckets,
        } => {
            let array = routing_array(batch, routing_col)?;
            let first = bucket_for_row(array, 0, *num_buckets)?;
            if validate_single_shard {
                for row in 1..batch.num_rows() {
                    let bucket = bucket_for_row(array, row, *num_buckets)?;
                    if bucket != first {
                        return Err(Error::InvalidInput {
                            message: format!(
                                "merge_insert: input row 0 hashes to bucket {} but row {} hashes to bucket {}; each merge_insert call must target a single bucket (pre-shard the input, or set validate_single_shard(false) to route by the first row only)",
                                first, row, bucket
                            ),
                        });
                    }
                }
            }
            Ok(bucket_shard_id(first))
        }
        LsmMode::Identity { routing_col } => {
            let array = routing_array(batch, routing_col)?;
            let first = encode_scalar(array, 0)?;
            if validate_single_shard {
                for row in 1..batch.num_rows() {
                    if encode_scalar(array, row)? != first {
                        return Err(Error::InvalidInput {
                            message: format!(
                                "merge_insert: input rows have differing values for identity-sharding column '{}'; each merge_insert call must target a single shard (pre-shard the input, or set validate_single_shard(false) to route by the first row only)",
                                routing_col
                            ),
                        });
                    }
                }
            }
            Ok(identity_shard_id(&first))
        }
    }
}

/// Borrow a routing column from a batch, rejecting a missing column or nulls.
fn routing_array<'a>(batch: &'a RecordBatch, column: &str) -> Result<&'a dyn Array> {
    let array = batch
        .column_by_name(column)
        .ok_or_else(|| Error::InvalidInput {
            message: format!(
                "merge_insert: input is missing the routing column '{}'",
                column
            ),
        })?;
    if array.null_count() > 0 {
        return Err(Error::InvalidInput {
            message: format!(
                "merge_insert: input has null values in the routing column '{}'",
                column
            ),
        });
    }
    Ok(array.as_ref())
}

/// Bucket index for one row under `bucket(col, num_buckets)`.
fn bucket_for_row(array: &dyn Array, row: usize, num_buckets: u32) -> Result<u32> {
    Ok((murmur3_hash_value(array, row)? & 0x7FFF_FFFF) % num_buckets)
}

/// Murmur3-x86-32 hash of one cell, matching Iceberg's `bucket` transform.
fn murmur3_hash_value(array: &dyn Array, row: usize) -> Result<u32> {
    let buf: Vec<u8> = match array.data_type() {
        // Iceberg widens int to long before hashing, so `Int32` value `v` and
        // `Int64` value `v` hash identically.
        DataType::Int32 => (array.as_primitive::<Int32Type>().value(row) as i64)
            .to_le_bytes()
            .to_vec(),
        DataType::Int64 => array
            .as_primitive::<Int64Type>()
            .value(row)
            .to_le_bytes()
            .to_vec(),
        DataType::Utf8 => array.as_string::<i32>().value(row).as_bytes().to_vec(),
        DataType::LargeUtf8 => array.as_string::<i64>().value(row).as_bytes().to_vec(),
        DataType::Binary => array.as_binary::<i32>().value(row).to_vec(),
        DataType::LargeBinary => array.as_binary::<i64>().value(row).to_vec(),
        DataType::FixedSizeBinary(_) => array.as_fixed_size_binary().value(row).to_vec(),
        other => {
            return Err(Error::InvalidInput {
                message: format!(
                    "merge_insert: bucket sharding does not support primary key dtype {:?}",
                    other
                ),
            });
        }
    };
    murmur3_32(&mut Cursor::new(&buf), MURMUR3_SEED).map_err(|e| Error::Runtime {
        message: format!("merge_insert: murmur3 hash failed: {}", e),
    })
}

/// Encode one cell of an identity-sharding column to comparable bytes.
fn encode_scalar(array: &dyn Array, row: usize) -> Result<Vec<u8>> {
    Ok(match array.data_type() {
        DataType::Int8 => array
            .as_primitive::<Int8Type>()
            .value(row)
            .to_le_bytes()
            .to_vec(),
        DataType::Int16 => array
            .as_primitive::<Int16Type>()
            .value(row)
            .to_le_bytes()
            .to_vec(),
        DataType::Int32 => array
            .as_primitive::<Int32Type>()
            .value(row)
            .to_le_bytes()
            .to_vec(),
        DataType::Int64 => array
            .as_primitive::<Int64Type>()
            .value(row)
            .to_le_bytes()
            .to_vec(),
        DataType::UInt8 => array
            .as_primitive::<UInt8Type>()
            .value(row)
            .to_le_bytes()
            .to_vec(),
        DataType::UInt16 => array
            .as_primitive::<UInt16Type>()
            .value(row)
            .to_le_bytes()
            .to_vec(),
        DataType::UInt32 => array
            .as_primitive::<UInt32Type>()
            .value(row)
            .to_le_bytes()
            .to_vec(),
        DataType::UInt64 => array
            .as_primitive::<UInt64Type>()
            .value(row)
            .to_le_bytes()
            .to_vec(),
        DataType::Utf8 => array.as_string::<i32>().value(row).as_bytes().to_vec(),
        DataType::LargeUtf8 => array.as_string::<i64>().value(row).as_bytes().to_vec(),
        DataType::Boolean => vec![u8::from(array.as_boolean().value(row))],
        other => {
            return Err(Error::InvalidInput {
                message: format!(
                    "merge_insert: identity sharding does not support column dtype {:?}",
                    other
                ),
            });
        }
    })
}

/// Deterministic shard id for a bucket index.
fn bucket_shard_id(bucket: u32) -> Uuid {
    Uuid::new_v5(&SHARD_NAMESPACE, format!("bucket-{}", bucket).as_bytes())
}

/// Deterministic shard id for an identity value.
fn identity_shard_id(value: &[u8]) -> Uuid {
    let mut name = b"identity-".to_vec();
    name.extend_from_slice(value);
    Uuid::new_v5(&SHARD_NAMESPACE, &name)
}

/// Deterministic shard id for the single unsharded shard.
fn unsharded_shard_id() -> Uuid {
    Uuid::new_v5(&SHARD_NAMESPACE, b"unsharded")
}

/// Build a [`ShardWriterConfig`] from the persisted `writer_config_defaults`.
///
/// Unknown or unparseable keys are ignored; absent keys keep the
/// [`ShardWriterConfig`] default. The shard id is set by `mem_wal_writer`.
fn shard_writer_config_from_defaults(defaults: &HashMap<String, String>) -> ShardWriterConfig {
    let mut config = ShardWriterConfig::default().with_shard_spec_id(SHARDING_SPEC_ID);
    let bool_of = |key: &str| defaults.get(key).and_then(|s| s.parse::<bool>().ok());
    let usize_of = |key: &str| defaults.get(key).and_then(|s| s.parse::<usize>().ok());
    let millis_of = |key: &str| {
        defaults
            .get(key)
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_millis)
    };

    if let Some(v) = bool_of("durable_write") {
        config = config.with_durable_write(v);
    }
    if let Some(v) = bool_of("sync_indexed_write") {
        config = config.with_sync_indexed_write(v);
    }
    if let Some(v) = usize_of("max_wal_buffer_size") {
        config = config.with_max_wal_buffer_size(v);
    }
    if let Some(v) = usize_of("max_memtable_size") {
        config = config.with_max_memtable_size(v);
    }
    if let Some(v) = usize_of("max_memtable_rows") {
        config = config.with_max_memtable_rows(v);
    }
    if let Some(v) = usize_of("max_memtable_batches") {
        config = config.with_max_memtable_batches(v);
    }
    if let Some(v) = usize_of("manifest_scan_batch_size") {
        config = config.with_manifest_scan_batch_size(v);
    }
    if let Some(v) = usize_of("max_unflushed_memtable_bytes") {
        config = config.with_max_unflushed_memtable_bytes(v);
    }
    if let Some(v) = millis_of("backpressure_log_interval_ms") {
        config = config.with_backpressure_log_interval(v);
    }
    if let Some(v) = usize_of("async_index_buffer_rows") {
        config = config.with_async_index_buffer_rows(v);
    }
    if let Some(v) = millis_of("async_index_interval_ms") {
        config = config.with_async_index_interval(v);
    }
    if let Some(v) = bool_of("enable_memtable") {
        config = config.with_enable_memtable(v);
    }
    if let Some(v) = millis_of("max_wal_flush_interval_ms") {
        config = config.with_max_wal_flush_interval(v);
    }
    if let Some(v) = millis_of("stats_log_interval_ms") {
        config = config.with_stats_log_interval(Some(v));
    }
    config
}

/// Re-attach the dataset's Arrow schema (including field metadata) to a
/// user-supplied input batch. The MemWAL `ShardWriter` checks batch schemas
/// against the dataset schema by exact equality, so input readers built
/// without the primary-key metadata must be rewrapped before being put.
///
/// Columns are matched by name; column order in the input is irrelevant.
fn align_batch_schema(batch: RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    if batch.schema() == *target {
        return Ok(batch);
    }
    let mut columns = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let column = batch
            .column_by_name(field.name())
            .ok_or_else(|| Error::InvalidInput {
                message: format!(
                    "merge_insert: input is missing column '{}' required by the table schema",
                    field.name()
                ),
            })?;
        if column.data_type() != field.data_type() {
            return Err(Error::InvalidInput {
                message: format!(
                    "merge_insert: input column '{}' has dtype {:?}, expected {:?}",
                    field.name(),
                    column.data_type(),
                    field.data_type()
                ),
            });
        }
        columns.push(column.clone());
    }
    RecordBatch::try_new(target.clone(), columns).map_err(|e| Error::Arrow { source: e })
}

/// Build the [`MergeResult`] for an LSM-path `merge_insert`.
///
/// The insert/update breakdown is not known until LSM compaction, so only the
/// total row count is reported.
fn lsm_merge_result(num_rows: u64) -> MergeResult {
    MergeResult {
        version: 0,
        num_inserted_rows: 0,
        num_updated_rows: 0,
        num_deleted_rows: 0,
        num_attempts: 0,
        num_rows,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, Int64Array, StringArray};
    use arrow_schema::Field;

    #[test]
    fn bucket_assignments_are_pinned() {
        // Regression guard: changing the hash or seed moves row -> bucket
        // assignments and breaks upserts on existing tables.
        let array = StringArray::from(vec!["alpha", "beta", "gamma", "delta"]);
        let buckets: Vec<u32> = (0..4)
            .map(|r| bucket_for_row(&array, r, 4).unwrap())
            .collect();
        assert_eq!(buckets, vec![1, 1, 2, 0]);
    }

    #[test]
    fn bucket_int32_matches_int64_after_widening() {
        for v in [1_i64, 7, -3, 12345] {
            let b32 = bucket_for_row(&Int32Array::from(vec![v as i32]), 0, 8).unwrap();
            let b64 = bucket_for_row(&Int64Array::from(vec![v]), 0, 8).unwrap();
            assert_eq!(b32, b64, "Int32({0}) and Int64({0}) must agree", v);
        }
    }

    #[test]
    fn shard_ids_are_deterministic_and_distinct() {
        assert_eq!(bucket_shard_id(3), bucket_shard_id(3));
        assert_ne!(bucket_shard_id(3), bucket_shard_id(4));
        assert_ne!(bucket_shard_id(0), unsharded_shard_id());
        assert_eq!(
            identity_shard_id(b"tenant-a"),
            identity_shard_id(b"tenant-a")
        );
        assert_ne!(
            identity_shard_id(b"tenant-a"),
            identity_shard_id(b"tenant-b")
        );
    }

    #[test]
    fn encode_scalar_distinguishes_values() {
        let ints = Int64Array::from(vec![1, 2]);
        assert_ne!(
            encode_scalar(&ints, 0).unwrap(),
            encode_scalar(&ints, 1).unwrap()
        );
        let strs = StringArray::from(vec!["x", "y"]);
        assert_ne!(
            encode_scalar(&strs, 0).unwrap(),
            encode_scalar(&strs, 1).unwrap()
        );
    }

    #[test]
    fn writer_config_from_defaults_parses_known_keys() {
        let defaults = HashMap::from([
            ("durable_write".to_string(), "false".to_string()),
            ("max_memtable_rows".to_string(), "4096".to_string()),
            ("async_index_interval_ms".to_string(), "250".to_string()),
            ("unknown_key".to_string(), "ignored".to_string()),
        ]);
        let config = shard_writer_config_from_defaults(&defaults);
        assert!(!config.durable_write);
        assert_eq!(config.max_memtable_rows, 4096);
        assert_eq!(config.async_index_interval, Duration::from_millis(250));
        assert_eq!(config.shard_spec_id, SHARDING_SPEC_ID);
    }

    #[test]
    fn align_batch_schema_reorders_columns() {
        let target: SchemaRef = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let source = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("v", DataType::Int64, false),
                Field::new("id", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        let aligned = align_batch_schema(source, &target).unwrap();
        assert_eq!(aligned.schema(), target);
        assert_eq!(
            aligned.column(0).as_primitive::<Int64Type>().values(),
            &[1, 2]
        );
    }

    #[test]
    fn align_batch_schema_rejects_missing_column() {
        let target: SchemaRef = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let source = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "id",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        assert!(matches!(
            align_batch_schema(source, &target),
            Err(Error::InvalidInput { .. })
        ));
    }

    fn utf8_batch(col: &str, values: Vec<&str>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                col,
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(values))],
        )
        .unwrap()
    }

    #[test]
    fn resolve_batch_shard_bucket_same_bucket() {
        // alpha and beta both hash to bucket 1 under num_buckets = 4.
        let mode = LsmMode::Bucket {
            routing_col: "id".to_string(),
            num_buckets: 4,
        };
        let batch = utf8_batch("id", vec!["alpha", "beta"]);
        assert_eq!(
            resolve_batch_shard(&mode, &batch, true).unwrap(),
            bucket_shard_id(1)
        );
    }

    #[test]
    fn resolve_batch_shard_bucket_rejects_mixed() {
        // alpha -> bucket 1, gamma -> bucket 2.
        let mode = LsmMode::Bucket {
            routing_col: "id".to_string(),
            num_buckets: 4,
        };
        let batch = utf8_batch("id", vec!["alpha", "gamma"]);
        // validate_single_shard rejects a batch that spans buckets.
        assert!(matches!(
            resolve_batch_shard(&mode, &batch, true),
            Err(Error::InvalidInput { .. })
        ));
        // With validation off, only row 0 is inspected, so it is accepted.
        assert_eq!(
            resolve_batch_shard(&mode, &batch, false).unwrap(),
            bucket_shard_id(1)
        );
    }

    #[test]
    fn resolve_batch_shard_rejects_nulls() {
        let mode = LsmMode::Bucket {
            routing_col: "id".to_string(),
            num_buckets: 4,
        };
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "id",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        assert!(matches!(
            resolve_batch_shard(&mode, &batch, true),
            Err(Error::InvalidInput { .. })
        ));
    }

    #[test]
    fn resolve_batch_shard_rejects_missing_routing_column() {
        let mode = LsmMode::Bucket {
            routing_col: "id".to_string(),
            num_buckets: 4,
        };
        let batch = utf8_batch("other", vec!["a"]);
        assert!(matches!(
            resolve_batch_shard(&mode, &batch, true),
            Err(Error::InvalidInput { .. })
        ));
    }

    #[test]
    fn resolve_batch_shard_identity_groups_by_value() {
        let mode = LsmMode::Identity {
            routing_col: "region".to_string(),
        };
        let same = utf8_batch("region", vec!["us", "us"]);
        let mixed = utf8_batch("region", vec!["us", "eu"]);
        assert!(resolve_batch_shard(&mode, &same, true).is_ok());
        assert!(matches!(
            resolve_batch_shard(&mode, &mixed, true),
            Err(Error::InvalidInput { .. })
        ));
        // With validation off, the mixed batch is accepted (row 0 only).
        assert!(resolve_batch_shard(&mode, &mixed, false).is_ok());
    }

    #[test]
    fn resolve_batch_shard_unsharded_is_constant() {
        let batch = utf8_batch("anything", vec!["a", "b", "c"]);
        assert_eq!(
            resolve_batch_shard(&LsmMode::Unsharded, &batch, true).unwrap(),
            unsharded_shard_id()
        );
    }
}
