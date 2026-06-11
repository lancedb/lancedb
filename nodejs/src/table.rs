// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use lancedb::ipc::{ipc_file_to_batches, ipc_file_to_schema};
use lancedb::table::{
    AddDataMode, ColumnAlteration as LanceColumnAlteration, Duration,
    FieldMetadataUpdate as LanceFieldMetadataUpdate, NewColumnTransform, OptimizeAction,
    OptimizeOptions, Ref, Table as LanceDbTable,
};
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;

use crate::error::NapiErrorExt;
use crate::index::Index;
use crate::merge::NativeMergeInsertBuilder;
use crate::query::{Query, TakeQuery, VectorQuery};
use crate::util::schema_to_buffer;

#[napi]
pub struct Table {
    // We keep a duplicate of the table name so we can use it for error
    // messages even if the table has been closed
    pub name: String,
    pub(crate) inner: Option<LanceDbTable>,
}

impl Table {
    pub(crate) fn inner_ref(&self) -> napi::Result<&LanceDbTable> {
        self.inner
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason(format!("Table {} is closed", self.name)))
    }
}

#[napi]
impl Table {
    pub(crate) fn new(table: LanceDbTable) -> Self {
        Self {
            name: table.name().to_string(),
            inner: Some(table),
        }
    }

    #[napi]
    pub fn display(&self) -> String {
        match &self.inner {
            None => format!("ClosedTable({})", self.name),
            Some(inner) => inner.to_string(),
        }
    }

    #[napi]
    pub fn is_open(&self) -> bool {
        self.inner.is_some()
    }

    #[napi]
    pub fn close(&mut self) {
        self.inner.take();
    }

    /// Return Schema as empty Arrow IPC file.
    #[napi(catch_unwind)]
    pub async fn schema(&self) -> napi::Result<Buffer> {
        let schema = self.inner_ref()?.schema().await.default_error()?;
        schema_to_buffer(&schema)
    }

    #[napi(
        catch_unwind,
        ts_args_type = "buf: Buffer, mode: string, progressCallback?: (progress: WriteProgressInfo) => void"
    )]
    pub async fn add(
        &self,
        buf: Buffer,
        mode: String,
        progress_callback: Option<ProgressFn>,
    ) -> napi::Result<AddResult> {
        let batches = ipc_file_to_batches(buf.to_vec())
            .map_err(|e| napi::Error::from_reason(format!("Failed to read IPC file: {}", e)))?;
        let batches = batches
            .into_iter()
            .map(|batch| {
                batch.map_err(|e| {
                    napi::Error::from_reason(format!(
                        "Failed to read record batch from IPC file: {}",
                        e
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut op = self.inner_ref()?.add(batches);

        op = if mode == "append" {
            op.mode(AddDataMode::Append)
        } else if mode == "overwrite" {
            op.mode(AddDataMode::Overwrite)
        } else {
            return Err(napi::Error::from_reason(format!("Invalid mode: {}", mode)));
        };

        if let Some(tsfn) = progress_callback {
            op = op.progress(move |p| {
                // NonBlocking: dispatch onto the JS event loop without
                // blocking the writer thread.  With napi-rs's default
                // unbounded queue, events are not dropped — a slow JS
                // callback will just queue them.
                tsfn.call(
                    WriteProgressInfo::from(p),
                    ThreadsafeFunctionCallMode::NonBlocking,
                );
            });
        }

        let res = op.execute().await.default_error()?;
        Ok(res.into())
    }

    #[napi(catch_unwind)]
    pub async fn count_rows(&self, filter: Option<String>) -> napi::Result<i64> {
        self.inner_ref()?
            .count_rows(filter)
            .await
            .map(|val| val as i64)
            .default_error()
    }

    #[napi(catch_unwind)]
    pub async fn delete(&self, predicate: String) -> napi::Result<DeleteResult> {
        let res = self.inner_ref()?.delete(&predicate).await.default_error()?;
        Ok(res.into())
    }

    #[napi(catch_unwind)]
    pub async fn create_index(
        &self,
        index: Option<&Index>,
        column: String,
        replace: Option<bool>,
        wait_timeout_s: Option<i64>,
        name: Option<String>,
        train: Option<bool>,
    ) -> napi::Result<()> {
        let lancedb_index = if let Some(index) = index {
            index.consume()?
        } else {
            lancedb::index::Index::Auto
        };
        let mut builder = self.inner_ref()?.create_index(&[column], lancedb_index);
        if let Some(replace) = replace {
            builder = builder.replace(replace);
        }
        if let Some(timeout) = wait_timeout_s {
            builder =
                builder.wait_timeout(std::time::Duration::from_secs(timeout.try_into().unwrap()));
        }
        if let Some(name) = name {
            builder = builder.name(name);
        }
        if let Some(train) = train {
            builder = builder.train(train);
        }
        builder.execute().await.default_error()
    }

    #[napi(catch_unwind)]
    pub async fn drop_index(&self, index_name: String) -> napi::Result<()> {
        self.inner_ref()?
            .drop_index(&index_name)
            .await
            .default_error()
    }

    #[napi(catch_unwind)]
    pub async fn prewarm_index(&self, index_name: String) -> napi::Result<()> {
        self.inner_ref()?
            .prewarm_index(&index_name)
            .await
            .default_error()
    }

    #[napi(catch_unwind)]
    pub async fn prewarm_data(&self, columns: Option<Vec<String>>) -> napi::Result<()> {
        self.inner_ref()?
            .prewarm_data(columns)
            .await
            .default_error()
    }

    #[napi(catch_unwind)]
    pub async fn wait_for_index(&self, index_names: Vec<String>, timeout_s: i64) -> Result<()> {
        let timeout = std::time::Duration::from_secs(timeout_s.try_into().unwrap());
        let index_names: Vec<&str> = index_names.iter().map(|s| s.as_str()).collect();
        let slice: &[&str] = &index_names;

        self.inner_ref()?
            .wait_for_index(slice, timeout)
            .await
            .default_error()
    }

    #[napi(catch_unwind)]
    pub async fn stats(&self) -> Result<TableStatistics> {
        let stats = self.inner_ref()?.stats().await.default_error()?;
        Ok(stats.into())
    }

    #[napi(catch_unwind)]
    pub async fn initial_storage_options(&self) -> napi::Result<Option<HashMap<String, String>>> {
        Ok(self.inner_ref()?.initial_storage_options().await)
    }

    #[napi(catch_unwind)]
    pub async fn latest_storage_options(&self) -> napi::Result<Option<HashMap<String, String>>> {
        self.inner_ref()?
            .latest_storage_options()
            .await
            .default_error()
    }

    #[napi(catch_unwind)]
    pub async fn update(
        &self,
        only_if: Option<String>,
        columns: Vec<(String, String)>,
    ) -> napi::Result<UpdateResult> {
        let mut op = self.inner_ref()?.update();
        if let Some(only_if) = only_if {
            op = op.only_if(only_if);
        }
        for (column_name, value) in columns {
            op = op.column(column_name, value);
        }
        let res = op.execute().await.default_error()?;
        Ok(res.into())
    }

    #[napi(catch_unwind)]
    pub fn query(&self) -> napi::Result<Query> {
        Ok(Query::new(self.inner_ref()?.query()))
    }

    #[napi(catch_unwind)]
    pub fn take_offsets(&self, offsets: Vec<i64>) -> napi::Result<TakeQuery> {
        Ok(TakeQuery::new(
            self.inner_ref()?.take_offsets(
                offsets
                    .into_iter()
                    .map(|o| {
                        u64::try_from(o).map_err(|e| {
                            napi::Error::from_reason(format!(
                                "Failed to convert offset to u64: {}",
                                e
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
        ))
    }

    #[napi(catch_unwind)]
    pub fn take_row_ids(&self, row_ids: Vec<BigInt>) -> napi::Result<TakeQuery> {
        Ok(TakeQuery::new(
            self.inner_ref()?.take_row_ids(
                row_ids
                    .into_iter()
                    .map(|id| {
                        let (negative, value, lossless) = id.get_u64();
                        if negative {
                            Err(napi::Error::from_reason(
                                "Row id cannot be negative".to_string(),
                            ))
                        } else if !lossless {
                            Err(napi::Error::from_reason(
                                "Row id is too large to fit in u64".to_string(),
                            ))
                        } else {
                            Ok(value)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
        ))
    }

    #[napi(catch_unwind)]
    pub fn vector_search(&self, vector: Float32Array) -> napi::Result<VectorQuery> {
        self.query()?.nearest_to(vector)
    }

    #[napi(catch_unwind)]
    pub async fn add_columns(
        &self,
        transforms: Vec<AddColumnsSql>,
    ) -> napi::Result<AddColumnsResult> {
        let transforms = transforms
            .into_iter()
            .map(|sql| (sql.name, sql.value_sql))
            .collect::<Vec<_>>();
        let transforms = NewColumnTransform::SqlExpressions(transforms);
        let res = self
            .inner_ref()?
            .add_columns(transforms, None)
            .await
            .default_error()?;
        Ok(res.into())
    }

    #[napi(catch_unwind)]
    pub async fn add_columns_with_schema(
        &self,
        schema_buf: Buffer,
    ) -> napi::Result<AddColumnsResult> {
        let schema = ipc_file_to_schema(schema_buf.to_vec())
            .map_err(|e| napi::Error::from_reason(format!("Failed to read IPC schema: {}", e)))?;

        let transforms = NewColumnTransform::AllNulls(schema);
        let res = self
            .inner_ref()?
            .add_columns(transforms, None)
            .await
            .default_error()?;
        Ok(res.into())
    }

    #[napi(catch_unwind)]
    pub async fn alter_columns(
        &self,
        alterations: Vec<ColumnAlteration>,
    ) -> napi::Result<AlterColumnsResult> {
        for alteration in &alterations {
            if alteration.rename.is_none()
                && alteration.nullable.is_none()
                && alteration.data_type.is_none()
            {
                return Err(napi::Error::from_reason(
                    "Alteration must have a 'rename', 'dataType', or 'nullable' field.",
                ));
            }
        }
        let alterations = alterations
            .into_iter()
            .map(LanceColumnAlteration::try_from)
            .collect::<std::result::Result<Vec<_>, String>>()
            .map_err(napi::Error::from_reason)?;

        let res = self
            .inner_ref()?
            .alter_columns(&alterations)
            .await
            .default_error()?;
        Ok(res.into())
    }

    #[napi(catch_unwind)]
    pub async fn update_field_metadata(
        &self,
        updates: Vec<FieldMetadataUpdate>,
    ) -> napi::Result<UpdateFieldMetadataResult> {
        let updates = updates
            .into_iter()
            .map(LanceFieldMetadataUpdate::from)
            .collect::<Vec<_>>();
        let res = self
            .inner_ref()?
            .update_field_metadata(&updates)
            .await
            .default_error()?;
        Ok(res.into())
    }

    #[napi(catch_unwind)]
    pub async fn drop_columns(&self, columns: Vec<String>) -> napi::Result<DropColumnsResult> {
        let col_refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
        let res = self
            .inner_ref()?
            .drop_columns(&col_refs)
            .await
            .default_error()?;
        Ok(res.into())
    }

    #[napi(catch_unwind)]
    pub async fn set_unenforced_primary_key(&self, columns: Vec<String>) -> napi::Result<()> {
        self.inner_ref()?
            .set_unenforced_primary_key(columns)
            .await
            .default_error()
    }

    #[napi(catch_unwind)]
    pub async fn set_lsm_write_spec(&self, spec: LsmWriteSpec) -> napi::Result<()> {
        let native_spec = lancedb::table::LsmWriteSpec::try_from(spec)?;
        self.inner_ref()?
            .set_lsm_write_spec(native_spec)
            .await
            .default_error()
    }

    #[napi(catch_unwind)]
    pub async fn unset_lsm_write_spec(&self) -> napi::Result<()> {
        self.inner_ref()?
            .unset_lsm_write_spec()
            .await
            .default_error()
    }

    #[napi(catch_unwind)]
    pub async fn close_lsm_writers(&self) -> napi::Result<()> {
        self.inner_ref()?.close_lsm_writers().await.default_error()
    }

    #[napi(catch_unwind)]
    pub async fn version(&self) -> napi::Result<i64> {
        self.inner_ref()?
            .version()
            .await
            .map(|val| val as i64)
            .default_error()
    }

    #[napi(catch_unwind)]
    pub async fn checkout(&self, version: i64) -> napi::Result<()> {
        self.inner_ref()?
            .checkout(version as u64)
            .await
            .default_error()
    }

    #[napi(catch_unwind)]
    pub async fn checkout_tag(&self, tag: String) -> napi::Result<()> {
        self.inner_ref()?
            .checkout_tag(tag.as_str())
            .await
            .default_error()
    }

    #[napi(catch_unwind)]
    pub async fn checkout_latest(&self) -> napi::Result<()> {
        self.inner_ref()?.checkout_latest().await.default_error()
    }

    #[napi(catch_unwind)]
    pub async fn list_versions(&self) -> napi::Result<Vec<Version>> {
        self.inner_ref()?
            .list_versions()
            .await
            .map(|versions| {
                versions
                    .iter()
                    .map(|version| Version {
                        version: version.version as i64,
                        timestamp: version.timestamp.timestamp_micros(),
                        metadata: version
                            .metadata
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect(),
                    })
                    .collect()
            })
            .default_error()
    }

    #[napi(catch_unwind)]
    pub async fn restore(&self) -> napi::Result<()> {
        self.inner_ref()?.restore().await.default_error()
    }

    #[napi(catch_unwind)]
    pub async fn tags(&self) -> napi::Result<Tags> {
        Ok(Tags {
            inner: self.inner_ref()?.clone(),
        })
    }

    #[napi(catch_unwind)]
    pub async fn branches(&self) -> napi::Result<Branches> {
        Ok(Branches {
            inner: self.inner_ref()?.clone(),
        })
    }

    #[napi(catch_unwind)]
    pub async fn optimize(
        &self,
        older_than_ms: Option<i64>,
        delete_unverified: Option<bool>,
    ) -> napi::Result<OptimizeStats> {
        let inner = self.inner_ref()?;

        let older_than = if let Some(ms) = older_than_ms {
            if ms == i64::MIN {
                return Err(napi::Error::from_reason(format!(
                    "older_than_ms can not be {}",
                    i32::MIN,
                )));
            }
            Duration::try_milliseconds(ms)
        } else {
            None
        };

        let compaction_stats = inner
            .optimize(OptimizeAction::Compact {
                options: lancedb::table::CompactionOptions::default(),
                remap_options: None,
            })
            .await
            .default_error()?
            .compaction
            .unwrap();
        let prune_stats = inner
            .optimize(OptimizeAction::Prune {
                older_than,
                delete_unverified,
                error_if_tagged_old_versions: None,
            })
            .await
            .default_error()?
            .prune
            .unwrap();
        inner
            .optimize(lancedb::table::OptimizeAction::Index(
                OptimizeOptions::default(),
            ))
            .await
            .default_error()?;
        Ok(OptimizeStats {
            compaction: CompactionStats {
                files_added: compaction_stats.files_added as i64,
                files_removed: compaction_stats.files_removed as i64,
                fragments_added: compaction_stats.fragments_added as i64,
                fragments_removed: compaction_stats.fragments_removed as i64,
            },
            prune: RemovalStats {
                bytes_removed: prune_stats.bytes_removed as i64,
                old_versions_removed: prune_stats.old_versions as i64,
            },
        })
    }

    #[napi(catch_unwind)]
    pub async fn list_indices(&self) -> napi::Result<Vec<IndexConfig>> {
        Ok(self
            .inner_ref()?
            .list_indices()
            .await
            .default_error()?
            .into_iter()
            .map(IndexConfig::from)
            .collect::<Vec<_>>())
    }

    #[napi(catch_unwind)]
    pub async fn index_stats(&self, index_name: String) -> napi::Result<Option<IndexStatistics>> {
        let tbl = self.inner_ref()?;
        let stats = tbl.index_stats(&index_name).await.default_error()?;
        Ok(stats.map(IndexStatistics::from))
    }

    #[napi(catch_unwind)]
    pub fn merge_insert(&self, on: Vec<String>) -> napi::Result<NativeMergeInsertBuilder> {
        let on: Vec<_> = on.iter().map(String::as_str).collect();
        Ok(self.inner_ref()?.merge_insert(on.as_slice()).into())
    }

    #[napi(catch_unwind)]
    pub async fn uses_v2_manifest_paths(&self) -> napi::Result<bool> {
        self.inner_ref()?
            .as_native()
            .ok_or_else(|| napi::Error::from_reason("This cannot be run on a remote table"))?
            .uses_v2_manifest_paths()
            .await
            .default_error()
    }

    #[napi(catch_unwind)]
    pub async fn migrate_manifest_paths_v2(&self) -> napi::Result<()> {
        self.inner_ref()?
            .as_native()
            .ok_or_else(|| napi::Error::from_reason("This cannot be run on a remote table"))?
            .migrate_manifest_paths_v2()
            .await
            .default_error()
    }
}

#[napi(object)]
/// A description of an index currently configured on a column
pub struct IndexConfig {
    /// The name of the index
    pub name: String,
    /// The type of the index
    pub index_type: String,
    /// The columns in the index
    ///
    /// Currently this is always an array of size 1. In the future there may
    /// be more columns to represent composite indices.
    pub columns: Vec<String>,
    /// The UUID of the first segment of the index.
    ///
    /// `undefined` for remote tables, which do not yet surface this.
    pub index_uuid: Option<String>,
    /// The protobuf type URL, a precise type identifier for the index.
    ///
    /// `undefined` for remote tables.
    pub type_url: Option<String>,
    /// When the index was created.
    ///
    /// `undefined` for remote tables or indices created before timestamps were tracked.
    pub created_at: Option<DateTime<Utc>>,
    /// The number of rows indexed, across all segments.
    ///
    /// `undefined` for remote tables.
    pub num_indexed_rows: Option<i64>,
    /// The number of rows not yet covered by this index.
    ///
    /// `undefined` for remote tables.
    pub num_unindexed_rows: Option<i64>,
    /// The total size in bytes of all index files across all segments.
    ///
    /// `undefined` for remote tables or indices without size tracking.
    pub size_bytes: Option<i64>,
    /// The number of segments that make up the index.
    ///
    /// `undefined` for remote tables.
    pub num_segments: Option<i32>,
    /// The on-disk index format version.
    ///
    /// `undefined` for remote tables.
    pub index_version: Option<i32>,
    /// Index-type-specific details parsed as a JavaScript object.
    ///
    /// Falls back to a raw string if JSON parsing fails. `undefined` for
    /// remote tables or when details are unavailable.
    pub index_details: Option<serde_json::Value>,
}

impl From<lancedb::index::IndexConfig> for IndexConfig {
    fn from(value: lancedb::index::IndexConfig) -> Self {
        let index_type = format!("{:?}", value.index_type);
        Self {
            index_type,
            columns: value.columns,
            name: value.name,
            index_uuid: value.index_uuid,
            type_url: value.type_url,
            created_at: value.created_at,
            num_indexed_rows: value.num_indexed_rows.map(|n| n as i64),
            num_unindexed_rows: value.num_unindexed_rows.map(|n| n as i64),
            size_bytes: value.size_bytes.map(|n| n as i64),
            num_segments: value.num_segments.map(|n| n as i32),
            index_version: value.index_version,
            index_details: value
                .index_details
                .and_then(|s| serde_json::from_str(&s).ok()),
        }
    }
}

/// Specification selecting Lance's MemWAL LSM-style write path for
/// `mergeInsert`.
///
/// `specType` must be `"bucket"`, `"identity"`, or `"unsharded"`. For
/// `"bucket"`, `column` and `numBuckets` are required; for `"identity"`,
/// `column` is required.
#[napi(object)]
#[derive(Clone, Debug)]
pub struct LsmWriteSpec {
    /// One of `"bucket"`, `"identity"`, or `"unsharded"`.
    pub spec_type: String,
    /// Bucket and identity variants: the sharding column.
    pub column: Option<String>,
    /// Bucket variant: the number of buckets, in `[1, 1024]`.
    pub num_buckets: Option<u32>,
    /// Names of indexes the MemWAL should keep up to date during writes.
    pub maintained_indexes: Option<Vec<String>>,
    /// Default `ShardWriter` configuration recorded in the MemWAL index.
    pub writer_config_defaults: Option<HashMap<String, String>>,
}

impl TryFrom<LsmWriteSpec> for lancedb::table::LsmWriteSpec {
    type Error = napi::Error;

    fn try_from(value: LsmWriteSpec) -> napi::Result<Self> {
        let maintained = value.maintained_indexes.unwrap_or_default();
        let writer_config_defaults = value.writer_config_defaults.unwrap_or_default();
        let spec = match value.spec_type.as_str() {
            "bucket" => {
                let column = value.column.ok_or_else(|| {
                    napi::Error::from_reason("LsmWriteSpec bucket requires `column`")
                })?;
                let num_buckets = value.num_buckets.ok_or_else(|| {
                    napi::Error::from_reason("LsmWriteSpec bucket requires `numBuckets`")
                })?;
                Self::bucket(column, num_buckets)
            }
            "identity" => {
                let column = value.column.ok_or_else(|| {
                    napi::Error::from_reason("LsmWriteSpec identity requires `column`")
                })?;
                Self::identity(column)
            }
            "unsharded" => Self::unsharded(),
            other => {
                return Err(napi::Error::from_reason(format!(
                    "LsmWriteSpec `specType` must be 'bucket', 'identity', or 'unsharded', got '{}'",
                    other
                )));
            }
        };
        Ok(spec
            .with_maintained_indexes(maintained)
            .with_writer_config_defaults(writer_config_defaults))
    }
}

/// Statistics about a compaction operation.
#[napi(object)]
#[derive(Clone, Debug)]
pub struct CompactionStats {
    /// The number of fragments removed
    pub fragments_removed: i64,
    /// The number of new, compacted fragments added
    pub fragments_added: i64,
    /// The number of data files removed
    pub files_removed: i64,
    /// The number of new, compacted data files added
    pub files_added: i64,
}

/// Statistics about a cleanup operation
#[napi(object)]
#[derive(Clone, Debug)]
pub struct RemovalStats {
    /// The number of bytes removed
    pub bytes_removed: i64,
    /// The number of old versions removed
    pub old_versions_removed: i64,
}

/// Statistics about an optimize operation
#[napi(object)]
#[derive(Clone, Debug)]
pub struct OptimizeStats {
    /// Statistics about the compaction operation
    pub compaction: CompactionStats,
    /// Statistics about the removal operation
    pub prune: RemovalStats,
}

/// Progress snapshot for a write operation, delivered to the JS callback
/// passed to `Table.add`.
#[napi(object)]
#[derive(Clone, Debug)]
pub struct WriteProgressInfo {
    /// Number of rows written so far.
    pub output_rows: i64,
    /// Number of bytes written so far.
    pub output_bytes: i64,
    /// Total rows expected, if the input source reports it.
    /// Always set on the final callback (where `done` is `true`).
    pub total_rows: Option<i64>,
    /// Wall-clock seconds since monitoring started.
    pub elapsed_seconds: f64,
    /// Number of parallel write tasks currently in flight.
    pub active_tasks: i64,
    /// Total number of parallel write tasks (the write parallelism).
    pub total_tasks: i64,
    /// `true` for the final callback; `false` otherwise.
    pub done: bool,
}

impl From<&lancedb::table::write_progress::WriteProgress> for WriteProgressInfo {
    fn from(p: &lancedb::table::write_progress::WriteProgress) -> Self {
        Self {
            output_rows: p.output_rows() as i64,
            output_bytes: p.output_bytes() as i64,
            total_rows: p.total_rows().map(|n| n as i64),
            elapsed_seconds: p.elapsed().as_secs_f64(),
            active_tasks: p.active_tasks() as i64,
            total_tasks: p.total_tasks() as i64,
            done: p.done(),
        }
    }
}

type ProgressFn = ThreadsafeFunction<WriteProgressInfo, (), WriteProgressInfo, Status, false>;

///  A definition of a column alteration. The alteration changes the column at
/// `path` to have the new name `name`, to be nullable if `nullable` is true,
/// and to have the data type `data_type`. At least one of `rename` or `nullable`
/// must be provided.
#[napi(object)]
pub struct ColumnAlteration {
    /// The path to the column to alter. This is a dot-separated path to the column.
    /// If it is a top-level column then it is just the name of the column. If it is
    /// a nested column then it is the path to the column, e.g. "a.b.c" for a column
    /// `c` nested inside a column `b` nested inside a column `a`.
    pub path: String,
    /// The new name of the column. If not provided then the name will not be changed.
    /// This must be distinct from the names of all other columns in the table.
    pub rename: Option<String>,
    /// A new data type for the column. If not provided then the data type will not be changed.
    /// Changing data types is limited to casting to the same general type. For example, these
    /// changes are valid:
    /// * `int32` -> `int64` (integers)
    /// * `double` -> `float` (floats)
    /// * `string` -> `large_string` (strings)
    /// But these changes are not:
    /// * `int32` -> `double` (mix integers and floats)
    /// * `string` -> `int32` (mix strings and integers)
    pub data_type: Option<String>,
    /// Set the new nullability. Note that a nullable column cannot be made non-nullable.
    pub nullable: Option<bool>,
}

/// A per-field metadata update, addressed by dot-path. Merges into the field's
/// existing metadata by default; a `null` value deletes a key, and `replace`
/// swaps the field's entire metadata map.
#[napi(object)]
pub struct FieldMetadataUpdate {
    /// Dot-separated path to the field (e.g. "embedding" or "a.b.c").
    pub path: String,
    /// Metadata keys to set; a `null` value deletes that key.
    pub metadata: HashMap<String, Option<String>>,
    /// If true, replace the field's entire metadata map instead of merging.
    pub replace: Option<bool>,
}

impl From<FieldMetadataUpdate> for LanceFieldMetadataUpdate {
    fn from(js: FieldMetadataUpdate) -> Self {
        Self {
            path: js.path,
            metadata: js.metadata,
            replace: js.replace.unwrap_or(false),
        }
    }
}

impl TryFrom<ColumnAlteration> for LanceColumnAlteration {
    type Error = String;
    fn try_from(js: ColumnAlteration) -> std::result::Result<Self, Self::Error> {
        let ColumnAlteration {
            path,
            rename,
            nullable,
            data_type,
        } = js;
        let data_type = if let Some(data_type) = data_type {
            Some(
                lancedb::utils::string_to_datatype(&data_type)
                    .ok_or_else(|| format!("Invalid data type: {}", data_type))?,
            )
        } else {
            None
        };
        Ok(Self {
            path,
            rename,
            nullable,
            data_type,
        })
    }
}

/// A definition of a new column to add to a table.
#[napi(object)]
pub struct AddColumnsSql {
    /// The name of the new column.
    pub name: String,
    /// The values to populate the new column with, as a SQL expression.
    /// The expression can reference other columns in the table.
    pub value_sql: String,
}

#[napi(object)]
pub struct IndexStatistics {
    /// The number of rows indexed by the index
    pub num_indexed_rows: f64,
    /// The number of rows not indexed
    pub num_unindexed_rows: f64,
    /// The type of the index
    pub index_type: String,
    /// The type of the distance function used by the index. This is only
    /// present for vector indices. Scalar and full text search indices do
    /// not have a distance function.
    pub distance_type: Option<String>,
    /// The number of parts this index is split into.
    pub num_indices: Option<u32>,
}
impl From<lancedb::index::IndexStatistics> for IndexStatistics {
    fn from(value: lancedb::index::IndexStatistics) -> Self {
        Self {
            num_indexed_rows: value.num_indexed_rows as f64,
            num_unindexed_rows: value.num_unindexed_rows as f64,
            index_type: value.index_type.to_string(),
            distance_type: value.distance_type.map(|d| d.to_string()),
            num_indices: value.num_indices,
        }
    }
}

#[napi(object)]
pub struct TableStatistics {
    /// The total number of bytes in the table
    pub total_bytes: i64,

    /// The number of rows in the table
    pub num_rows: i64,

    /// The number of indices in the table
    pub num_indices: i64,

    /// Statistics on table fragments
    pub fragment_stats: FragmentStatistics,
}

#[napi(object)]
pub struct FragmentStatistics {
    /// The number of fragments in the table
    pub num_fragments: i64,

    /// The number of uncompacted fragments in the table
    pub num_small_fragments: i64,

    /// Statistics on the number of rows in the table fragments
    pub lengths: FragmentSummaryStats,
}

#[napi(object)]
pub struct FragmentSummaryStats {
    /// The number of rows in the fragment with the fewest rows
    pub min: i64,

    /// The number of rows in the fragment with the most rows
    pub max: i64,

    /// The mean number of rows in the fragments
    pub mean: i64,

    /// The 25th percentile of number of rows in the fragments
    pub p25: i64,

    /// The 50th percentile of number of rows in the fragments
    pub p50: i64,

    /// The 75th percentile of number of rows in the fragments
    pub p75: i64,

    /// The 99th percentile of number of rows in the fragments
    pub p99: i64,
}

impl From<lancedb::table::TableStatistics> for TableStatistics {
    fn from(v: lancedb::table::TableStatistics) -> Self {
        Self {
            total_bytes: v.total_bytes as i64,
            num_rows: v.num_rows as i64,
            num_indices: v.num_indices as i64,
            fragment_stats: FragmentStatistics {
                num_fragments: v.fragment_stats.num_fragments as i64,
                num_small_fragments: v.fragment_stats.num_small_fragments as i64,
                lengths: FragmentSummaryStats {
                    min: v.fragment_stats.lengths.min as i64,
                    max: v.fragment_stats.lengths.max as i64,
                    mean: v.fragment_stats.lengths.mean as i64,
                    p25: v.fragment_stats.lengths.p25 as i64,
                    p50: v.fragment_stats.lengths.p50 as i64,
                    p75: v.fragment_stats.lengths.p75 as i64,
                    p99: v.fragment_stats.lengths.p99 as i64,
                },
            },
        }
    }
}

#[napi(object)]
pub struct Version {
    pub version: i64,
    pub timestamp: i64,
    pub metadata: HashMap<String, String>,
}

#[napi(object)]
pub struct UpdateResult {
    pub rows_updated: i64,
    pub version: i64,
}

impl From<lancedb::table::UpdateResult> for UpdateResult {
    fn from(value: lancedb::table::UpdateResult) -> Self {
        Self {
            rows_updated: value.rows_updated as i64,
            version: value.version as i64,
        }
    }
}

#[napi(object)]
pub struct AddResult {
    pub version: i64,
}

impl From<lancedb::table::AddResult> for AddResult {
    fn from(value: lancedb::table::AddResult) -> Self {
        Self {
            version: value.version as i64,
        }
    }
}

#[napi(object)]
pub struct DeleteResult {
    pub num_deleted_rows: i64,
    pub version: i64,
}

impl From<lancedb::table::DeleteResult> for DeleteResult {
    fn from(value: lancedb::table::DeleteResult) -> Self {
        Self {
            num_deleted_rows: value.num_deleted_rows as i64,
            version: value.version as i64,
        }
    }
}

#[napi(object)]
pub struct MergeResult {
    pub version: i64,
    pub num_inserted_rows: i64,
    pub num_updated_rows: i64,
    pub num_deleted_rows: i64,
    pub num_attempts: i64,
    pub num_rows: i64,
}

impl From<lancedb::table::MergeResult> for MergeResult {
    fn from(value: lancedb::table::MergeResult) -> Self {
        Self {
            version: value.version as i64,
            num_inserted_rows: value.num_inserted_rows as i64,
            num_updated_rows: value.num_updated_rows as i64,
            num_deleted_rows: value.num_deleted_rows as i64,
            num_attempts: value.num_attempts as i64,
            num_rows: value.num_rows as i64,
        }
    }
}

#[napi(object)]
pub struct AddColumnsResult {
    pub version: i64,
}

impl From<lancedb::table::AddColumnsResult> for AddColumnsResult {
    fn from(value: lancedb::table::AddColumnsResult) -> Self {
        Self {
            version: value.version as i64,
        }
    }
}

#[napi(object)]
pub struct AlterColumnsResult {
    pub version: i64,
}

impl From<lancedb::table::AlterColumnsResult> for AlterColumnsResult {
    fn from(value: lancedb::table::AlterColumnsResult) -> Self {
        Self {
            version: value.version as i64,
        }
    }
}

#[napi(object)]
pub struct UpdateFieldMetadataResult {
    pub version: i64,
}

impl From<lancedb::table::UpdateFieldMetadataResult> for UpdateFieldMetadataResult {
    fn from(value: lancedb::table::UpdateFieldMetadataResult) -> Self {
        Self {
            version: value.version as i64,
        }
    }
}

#[napi(object)]
pub struct DropColumnsResult {
    pub version: i64,
}

impl From<lancedb::table::DropColumnsResult> for DropColumnsResult {
    fn from(value: lancedb::table::DropColumnsResult) -> Self {
        Self {
            version: value.version as i64,
        }
    }
}

#[napi]
pub struct TagContents {
    pub version: i64,
    pub manifest_size: i64,
}

#[napi]
pub struct BranchContents {
    pub parent_branch: Option<String>,
    pub parent_version: i64,
    pub manifest_size: i64,
}

#[napi]
pub struct Tags {
    inner: LanceDbTable,
}

#[napi]
impl Tags {
    #[napi]
    pub async fn list(&self) -> napi::Result<HashMap<String, TagContents>> {
        let rust_tags = self.inner.tags().await.default_error()?;
        let tag_list = rust_tags.as_ref().list().await.default_error()?;
        let tag_contents = tag_list
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    TagContents {
                        version: v.version as i64,
                        manifest_size: v.manifest_size as i64,
                    },
                )
            })
            .collect();

        Ok(tag_contents)
    }

    #[napi]
    pub async fn get_version(&self, tag: String) -> napi::Result<i64> {
        let rust_tags = self.inner.tags().await.default_error()?;
        rust_tags
            .as_ref()
            .get_version(tag.as_str())
            .await
            .map(|v| v as i64)
            .default_error()
    }

    #[napi]
    pub async unsafe fn create(&mut self, tag: String, version: i64) -> napi::Result<()> {
        let mut rust_tags = self.inner.tags().await.default_error()?;
        rust_tags
            .as_mut()
            .create(tag.as_str(), version as u64)
            .await
            .default_error()
    }

    #[napi]
    pub async unsafe fn delete(&mut self, tag: String) -> napi::Result<()> {
        let mut rust_tags = self.inner.tags().await.default_error()?;
        rust_tags
            .as_mut()
            .delete(tag.as_str())
            .await
            .default_error()
    }

    #[napi]
    pub async unsafe fn update(&mut self, tag: String, version: i64) -> napi::Result<()> {
        let mut rust_tags = self.inner.tags().await.default_error()?;
        rust_tags
            .as_mut()
            .update(tag.as_str(), version as u64)
            .await
            .default_error()
    }
}

#[napi]
pub struct Branches {
    inner: LanceDbTable,
}

#[napi]
impl Branches {
    #[napi]
    pub async fn list(&self) -> napi::Result<HashMap<String, BranchContents>> {
        let branches = self.inner.list_branches().await.default_error()?;
        let result = branches
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    BranchContents {
                        parent_branch: v.parent_branch,
                        parent_version: v.parent_version as i64,
                        manifest_size: v.manifest_size as i64,
                    },
                )
            })
            .collect();
        Ok(result)
    }

    #[napi]
    pub async fn create(
        &self,
        name: String,
        from_ref: Option<String>,
        from_version: Option<i64>,
    ) -> napi::Result<Table> {
        let from_ref = from_ref.filter(|b| b != "main");
        let from_version = from_version
            .map(|v| {
                u64::try_from(v).map_err(|_| {
                    napi::Error::from_reason("from_version must be a non-negative integer")
                })
            })
            .transpose()?;
        let from = Ref::Version(from_ref, from_version);
        let table = self
            .inner
            .create_branch(&name, from)
            .await
            .default_error()?;
        Ok(Table::new(table))
    }

    #[napi]
    pub async fn checkout(&self, name: String, version: Option<i64>) -> napi::Result<Table> {
        let version = version
            .map(|v| {
                u64::try_from(v)
                    .map_err(|_| napi::Error::from_reason("version must be a non-negative integer"))
            })
            .transpose()?;
        let table = self
            .inner
            .checkout_branch(&name, version)
            .await
            .default_error()?;
        Ok(Table::new(table))
    }

    #[napi]
    pub async fn delete(&self, name: String) -> napi::Result<()> {
        self.inner.delete_branch(&name).await.default_error()
    }
}
