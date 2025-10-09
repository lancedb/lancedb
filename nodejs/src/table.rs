// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;

use arrow_ipc::writer::FileWriter;
use lancedb::ipc::ipc_file_to_batches;
use lancedb::table::{
    AddDataMode, ColumnAlteration as LanceColumnAlteration, Duration, NewColumnTransform,
    OptimizeAction, OptimizeOptions, Table as LanceDbTable,
};
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::error::NapiErrorExt;
use crate::index::Index;
use crate::merge::NativeMergeInsertBuilder;
use crate::query::{Query, TakeQuery, VectorQuery};

#[napi]
pub struct Table {
    // We keep a duplicate of the table name so we can use it for error
    // messages even if the table has been closed
    pub name: String,
    pub(crate) inner: Option<LanceDbTable>,
}

impl Table {
    fn inner_ref(&self) -> napi::Result<&LanceDbTable> {
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
        let mut writer = FileWriter::try_new(vec![], &schema)
            .map_err(|e| napi::Error::from_reason(format!("Failed to create IPC file: {}", e)))?;
        writer
            .finish()
            .map_err(|e| napi::Error::from_reason(format!("Failed to finish IPC file: {}", e)))?;
        Ok(Buffer::from(writer.into_inner().map_err(|e| {
            napi::Error::from_reason(format!("Failed to get IPC file: {}", e))
        })?))
    }

    #[napi(catch_unwind)]
    pub async fn add(&self, buf: Buffer, mode: String) -> napi::Result<AddResult> {
        let batches = ipc_file_to_batches(buf.to_vec())
            .map_err(|e| napi::Error::from_reason(format!("Failed to read IPC file: {}", e)))?;
        let mut op = self.inner_ref()?.add(batches);

        op = if mode == "append" {
            op.mode(AddDataMode::Append)
        } else if mode == "overwrite" {
            op.mode(AddDataMode::Overwrite)
        } else {
            return Err(napi::Error::from_reason(format!("Invalid mode: {}", mode)));
        };

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
    pub fn take_row_ids(&self, row_ids: Vec<i64>) -> napi::Result<TakeQuery> {
        Ok(TakeQuery::new(
            self.inner_ref()?.take_row_ids(
                row_ids
                    .into_iter()
                    .map(|o| {
                        u64::try_from(o).map_err(|e| {
                            napi::Error::from_reason(format!(
                                "Failed to convert row id to u64: {}",
                                e
                            ))
                        })
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

    /// Update table metadata
    ///
    /// Pass `null` for a value to remove that key.
    /// Pass `replace=true` to replace the entire metadata map.
    ///
    /// Returns the updated metadata map after the operation.
    #[napi(catch_unwind)]
    pub async fn update_metadata(
        &self,
        values: Vec<UpdateMapEntry>,
        replace: bool,
    ) -> napi::Result<HashMap<String, String>> {
        use lancedb::table::UpdateMapEntry as LanceUpdateMapEntry;

        let lance_entries: Vec<LanceUpdateMapEntry> = values
            .into_iter()
            .map(|entry| LanceUpdateMapEntry {
                key: entry.key,
                value: entry.value,
            })
            .collect();

        let mut builder = self.inner_ref()?.update_metadata(lance_entries);
        if replace {
            builder = builder.replace();
        }
        builder.await.default_error()
    }

    /// Update schema metadata
    ///
    /// Pass `null` for a value to remove that key.
    /// Pass `replace=true` to replace the entire schema metadata map.
    ///
    /// Returns the updated schema metadata map after the operation.
    #[napi(catch_unwind)]
    pub async fn update_schema_metadata(
        &self,
        values: Vec<UpdateMapEntry>,
        replace: bool,
    ) -> napi::Result<HashMap<String, String>> {
        use lancedb::table::UpdateMapEntry as LanceUpdateMapEntry;

        let lance_entries: Vec<LanceUpdateMapEntry> = values
            .into_iter()
            .map(|entry| LanceUpdateMapEntry {
                key: entry.key,
                value: entry.value,
            })
            .collect();

        let mut builder = self.inner_ref()?.update_schema_metadata(lance_entries);
        if replace {
            builder = builder.replace();
        }
        builder.await.default_error()
    }
}

/// An entry for updating metadata maps
#[napi(object)]
pub struct UpdateMapEntry {
    /// The key of the map entry to update
    pub key: String,
    /// The value to set for the key. Use `null` to remove the key.
    pub value: Option<String>,
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
}

impl From<lancedb::index::IndexConfig> for IndexConfig {
    fn from(value: lancedb::index::IndexConfig) -> Self {
        let index_type = format!("{:?}", value.index_type);
        Self {
            index_type,
            columns: value.columns,
            name: value.name,
        }
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
    /// The KMeans loss value of the index,
    /// it is only present for vector indices.
    pub loss: Option<f64>,
}
impl From<lancedb::index::IndexStatistics> for IndexStatistics {
    fn from(value: lancedb::index::IndexStatistics) -> Self {
        Self {
            num_indexed_rows: value.num_indexed_rows as f64,
            num_unindexed_rows: value.num_unindexed_rows as f64,
            index_type: value.index_type.to_string(),
            distance_type: value.distance_type.map(|d| d.to_string()),
            num_indices: value.num_indices,
            loss: value.loss,
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
    pub version: i64,
}

impl From<lancedb::table::DeleteResult> for DeleteResult {
    fn from(value: lancedb::table::DeleteResult) -> Self {
        Self {
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
}

impl From<lancedb::table::MergeResult> for MergeResult {
    fn from(value: lancedb::table::MergeResult) -> Self {
        Self {
            version: value.version as i64,
            num_inserted_rows: value.num_inserted_rows as i64,
            num_updated_rows: value.num_updated_rows as i64,
            num_deleted_rows: value.num_deleted_rows as i64,
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
