// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Caching layer for [`PermutationReader`] that avoids repeated remote reads.
//!
//! [`CachingPermutationReader`] wraps a [`PermutationReader`] and stores
//! fetched rows in a local LanceDB table so that expensive remote reads are
//! not repeated across epochs.

use crate::arrow::SendableRecordBatchStream;
use crate::connection::Connection;
use crate::dataloader::permutation::reader::PermutationReader;
use crate::query::{ExecutableQuery, QueryBase, QueryExecutionOptions, Select};
use crate::table::BaseTable;
use crate::{Result, Table, connect};
use arrow::compute::concat_batches;
use arrow::datatypes::UInt64Type;
use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field};
use futures::TryStreamExt;
use lance_arrow::RecordBatchExt;
use std::collections::HashMap;
use std::sync::Arc;

/// The column name used to store the original remote logical offset in the local cache table.
const REMOTE_OFFSET_COL: &str = "_remote_offset";

/// FNV-1a hash – stable across runs, no extra dependencies.
fn fnv1a_hash(s: &str) -> u64 {
    let mut h: u64 = 14695981039346656037;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    h
}

/// Replace any character that is not alphanumeric or `_` with `_`.
fn sanitize_table_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Compute a 16-char hex string key for the given `Select`.
fn selection_key(selection: &Select) -> String {
    let canonical = match selection {
        Select::All => "ALL".to_string(),
        Select::Columns(cols) => {
            let mut sorted = cols.clone();
            sorted.sort();
            sorted.join(",")
        }
        Select::Dynamic(pairs) => {
            let mut sorted = pairs.clone();
            sorted.sort_by(|a, b| a.0.cmp(&b.0));
            sorted
                .iter()
                .map(|(k, v)| format!("{}:{}", k, v))
                .collect::<Vec<_>>()
                .join(";")
        }
        Select::Expr(pairs) => {
            let mut sorted = pairs.clone();
            sorted.sort_by(|a, b| a.0.cmp(&b.0));
            sorted
                .iter()
                .map(|(alias, expr)| format!("{}:{}", alias, expr))
                .collect::<Vec<_>>()
                .join(";")
        }
    };
    format!("{:016x}", fnv1a_hash(&canonical))
}

/// Per-selection cache state stored inside [`SharedOffsetCache`].
struct CacheEntry {
    /// Maps remote logical offset → physical row offset in the local cache table.
    offset_map: HashMap<u64, u64>,
    /// The local LanceDB table backing this cache entry (None until first write).
    cache_table: Option<Arc<dyn BaseTable>>,
    /// Monotonic count of rows written so far (== next available local offset).
    next_cache_offset: u64,
}

impl CacheEntry {
    fn empty() -> Self {
        Self {
            offset_map: HashMap::new(),
            cache_table: None,
            next_cache_offset: 0,
        }
    }
}

/// Shared offset cache shared across multiple [`CachingPermutationReader`]s.
///
/// Each selection variant gets its own sub-table (keyed by a hash of the
/// selection descriptor) so that cached batches fetched with different column
/// projections don't collide.
pub struct SharedOffsetCache {
    conn: Connection,
    table_name_prefix: String,
    entries: tokio::sync::Mutex<HashMap<String, CacheEntry>>,
}

impl SharedOffsetCache {
    /// Open (or create) a local LanceDB connection at `cache_dir` and
    /// initialise the cache for a specific table version.
    pub async fn new(cache_dir: &str, table_name: &str, table_version: u64) -> Result<Self> {
        let conn = connect(cache_dir).execute().await?;
        let table_name_prefix = format!("{}_v{}", sanitize_table_name(table_name), table_version);
        Ok(Self {
            conn,
            table_name_prefix,
            entries: tokio::sync::Mutex::new(HashMap::new()),
        })
    }

    fn cache_table_name(&self, sel_key: &str) -> String {
        format!("{}__{}", self.table_name_prefix, sel_key)
    }
}

/// Try to load an existing cache entry from the local LanceDB connection.
///
/// * If the table does not exist → returns an empty [`CacheEntry`].
/// * If the table is corrupted (missing `_remote_offset`) → drops and returns empty.
/// * Otherwise → reconstructs the `offset_map` from the stored data.
async fn init_cache_entry(conn: &Connection, table_name: &str) -> Result<CacheEntry> {
    match conn.open_table(table_name).execute().await {
        Err(_) => {
            // Table does not exist yet.
            Ok(CacheEntry::empty())
        }
        Ok(table) => {
            let base = table.base_table().clone();
            let schema = base.schema().await?;
            if schema.column_with_name(REMOTE_OFFSET_COL).is_none() {
                // Corrupted cache – drop and start fresh.
                conn.drop_table(table_name, &[]).await?;
                return Ok(CacheEntry::empty());
            }

            // Rebuild the offset map: we need _rowoffset and _remote_offset.
            let mut offset_map = HashMap::new();
            let mut stream = Table::from(base.clone())
                .query()
                .select(Select::Columns(vec![
                    "_rowoffset".to_string(),
                    REMOTE_OFFSET_COL.to_string(),
                ]))
                .execute()
                .await?;

            while let Some(batch) = stream.try_next().await? {
                let rowoffset_col = batch.column_by_name("_rowoffset").ok_or_else(|| {
                    crate::error::Error::InvalidInput {
                        message: "Cache table missing _rowoffset column".to_string(),
                    }
                })?;
                let remote_col = batch.column_by_name(REMOTE_OFFSET_COL).ok_or_else(|| {
                    crate::error::Error::InvalidInput {
                        message: "Cache table missing _remote_offset column".to_string(),
                    }
                })?;

                use arrow::array::AsArray;
                let rowoffsets = rowoffset_col.as_primitive::<UInt64Type>().values();
                let remote_offsets = remote_col.as_primitive::<UInt64Type>().values();

                for (remote, local) in remote_offsets.iter().zip(rowoffsets.iter()) {
                    offset_map.insert(*remote, *local);
                }
            }

            let next_cache_offset = base.count_rows(None).await? as u64;

            Ok(CacheEntry {
                offset_map,
                cache_table: Some(base),
                next_cache_offset,
            })
        }
    }
}

/// Add a `_remote_offset` column to a [`RecordBatch`].
///
/// We build the new batch manually to avoid any lifetime issues with helper
/// traits.
fn add_remote_offset_column(batch: &RecordBatch, offsets: &[u64]) -> Result<RecordBatch> {
    let remote_field = Field::new(REMOTE_OFFSET_COL, DataType::UInt64, false);
    let mut new_fields: Vec<arrow_schema::FieldRef> =
        batch.schema().fields().iter().cloned().collect();
    new_fields.push(Arc::new(remote_field));
    let new_schema = Arc::new(arrow_schema::Schema::new_with_metadata(
        new_fields,
        batch.schema().metadata().clone(),
    ));

    let mut new_columns: Vec<Arc<dyn arrow_array::Array>> = batch.columns().to_vec();
    new_columns.push(Arc::new(UInt64Array::from(offsets.to_vec())));

    Ok(RecordBatch::try_new(new_schema, new_columns)?)
}

/// A [`PermutationReader`] that caches fetched rows in a local LanceDB table.
///
/// Calls to [`take_offsets`] first check the cache; only offsets that are not
/// yet cached are forwarded to the wrapped `inner` reader.  Fetched rows are
/// written back to the local cache table so future calls are served locally.
#[derive(Clone)]
pub struct CachingPermutationReader {
    pub inner: PermutationReader,
    pub cache: Arc<SharedOffsetCache>,
}

impl CachingPermutationReader {
    /// Wrap an existing [`PermutationReader`] with the given shared cache.
    pub fn new(inner: PermutationReader, cache: Arc<SharedOffsetCache>) -> Self {
        Self { inner, cache }
    }

    /// Fetch rows by logical offset, serving cache hits locally and fetching
    /// misses from the wrapped reader before storing them in the cache.
    pub async fn take_offsets(&self, offsets: &[u64], selection: Select) -> Result<RecordBatch> {
        if offsets.is_empty() {
            return Ok(RecordBatch::new_empty(
                self.inner.output_schema(selection).await?,
            ));
        }

        let sel_key = selection_key(&selection);
        let cache_table_name = self.cache.cache_table_name(&sel_key);

        // ── Step 1: initialise the cache entry (double-checked locking) ──────
        {
            let entries = self.cache.entries.lock().await;
            if !entries.contains_key(&cache_table_name) {
                drop(entries);
                // Init outside the lock to avoid blocking other tasks.
                let new_entry = init_cache_entry(&self.cache.conn, &cache_table_name).await?;
                let mut entries = self.cache.entries.lock().await;
                entries.entry(cache_table_name.clone()).or_insert(new_entry);
            }
        }

        // ── Step 2: classify offsets into hits and misses ─────────────────────
        let (hits, misses, hit_cache_table) = {
            let entries = self.cache.entries.lock().await;
            let entry = entries.get(&cache_table_name).unwrap();

            let mut hits: Vec<(usize, u64, u64)> = Vec::new(); // (orig_idx, remote_off, local_off)
            let mut misses: Vec<(usize, u64)> = Vec::new(); // (orig_idx, remote_off)

            for (orig_idx, &remote_off) in offsets.iter().enumerate() {
                if let Some(&local_off) = entry.offset_map.get(&remote_off) {
                    hits.push((orig_idx, remote_off, local_off));
                } else {
                    misses.push((orig_idx, remote_off));
                }
            }

            let hit_cache_table = entry.cache_table.clone();
            (hits, misses, hit_cache_table)
        };

        // ── Step 3: fetch hits from the local cache ───────────────────────────
        let hit_batch_opt: Option<RecordBatch> = if !hits.is_empty() {
            let local_offsets: Vec<u64> = hits.iter().map(|(_, _, local_off)| *local_off).collect();
            let cache_arc = hit_cache_table.unwrap();
            let mut stream = Table::from(cache_arc)
                .take_offsets(local_offsets)
                .select(Select::All)
                .execute()
                .await?;

            let mut batches = Vec::new();
            while let Some(batch) = stream.try_next().await? {
                batches.push(batch);
            }

            if batches.is_empty() {
                None
            } else {
                let schema = batches[0].schema();
                Some(concat_batches(&schema, &batches)?)
            }
        } else {
            None
        };

        // ── Step 4: fetch misses from the remote reader ───────────────────────
        let (miss_batch_opt, miss_batch_with_remote_opt) = if !misses.is_empty() {
            let miss_remote_offsets: Vec<u64> = misses.iter().map(|(_, ro)| *ro).collect();
            let miss_batch = self
                .inner
                .take_offsets(&miss_remote_offsets, selection.clone())
                .await?;
            let miss_batch_with_remote =
                add_remote_offset_column(&miss_batch, &miss_remote_offsets)?;
            (Some(miss_batch), Some(miss_batch_with_remote))
        } else {
            (None, None)
        };

        // ── Step 5: write misses back to the local cache ──────────────────────
        if let Some(miss_batch_with_remote) = miss_batch_with_remote_opt {
            let n_misses = misses.len() as u64;
            let mut entries = self.cache.entries.lock().await;
            let entry = entries.get_mut(&cache_table_name).unwrap();

            if entry.cache_table.is_none() {
                // Create the cache table with the first batch.
                let table = self
                    .cache
                    .conn
                    .create_table(&cache_table_name, miss_batch_with_remote.clone())
                    .execute()
                    .await?;
                entry.cache_table = Some(table.base_table().clone());
            } else {
                // Append to the existing cache table.
                Table::from(entry.cache_table.clone().unwrap())
                    .add(miss_batch_with_remote.clone())
                    .execute()
                    .await?;
            }

            for (i, (_, remote_off)) in misses.iter().enumerate() {
                entry
                    .offset_map
                    .insert(*remote_off, entry.next_cache_offset + i as u64);
            }
            entry.next_cache_offset += n_misses;
        }

        // ── Step 6: assemble the result ───────────────────────────────────────
        match (hit_batch_opt, miss_batch_opt) {
            (None, None) => {
                // Shouldn't happen (we'd have returned early above), but be safe.
                Ok(RecordBatch::new_empty(
                    self.inner.output_schema(selection).await?,
                ))
            }
            (Some(hit_batch), None) => {
                // Drop the _remote_offset column that the cache table stores.
                Ok(hit_batch.drop_column(REMOTE_OFFSET_COL)?)
            }
            (None, Some(miss_batch)) => Ok(miss_batch),
            (Some(hit_batch), Some(miss_batch)) => {
                let hit_batch = hit_batch.drop_column(REMOTE_OFFSET_COL)?;
                let schema = miss_batch.schema();
                Ok(concat_batches(&schema, &[hit_batch, miss_batch])?)
            }
        }
    }

    /// Delegate to the inner reader's `output_schema`.
    pub async fn output_schema(&self, selection: Select) -> Result<arrow_schema::SchemaRef> {
        self.inner.output_schema(selection).await
    }

    /// Delegate to the inner reader's `count_rows`.
    pub fn count_rows(&self) -> u64 {
        self.inner.count_rows()
    }

    /// Return a new reader with the given offset applied.
    pub async fn with_offset(self, offset: u64) -> Result<Self> {
        Ok(Self {
            inner: self.inner.with_offset(offset).await?,
            cache: self.cache,
        })
    }

    /// Return a new reader with the given limit applied.
    pub async fn with_limit(self, limit: u64) -> Result<Self> {
        Ok(Self {
            inner: self.inner.with_limit(limit).await?,
            cache: self.cache,
        })
    }

    /// Stream all rows from the inner reader (caching is not applied here).
    pub async fn read(
        &self,
        selection: Select,
        execution_options: QueryExecutionOptions,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.read(selection, execution_options).await
    }
}
