// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
use crate::query::{Query, VectorQuery};

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
        let schema =
            self.inner_ref()?.schema().await.map_err(|e| {
                napi::Error::from_reason(format!("Failed to create IPC file: {}", e))
            })?;
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
    pub async fn add(&self, buf: Buffer, mode: String) -> napi::Result<()> {
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

        op.execute().await.map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to add batches to table {}: {}",
                self.name, e
            ))
        })
    }

    #[napi(catch_unwind)]
    pub async fn count_rows(&self, filter: Option<String>) -> napi::Result<i64> {
        self.inner_ref()?
            .count_rows(filter)
            .await
            .map(|val| val as i64)
            .map_err(|e| {
                napi::Error::from_reason(format!(
                    "Failed to count rows in table {}: {}",
                    self.name, e
                ))
            })
    }

    #[napi(catch_unwind)]
    pub async fn delete(&self, predicate: String) -> napi::Result<()> {
        self.inner_ref()?.delete(&predicate).await.map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to delete rows in table {}: predicate={}",
                self.name, e
            ))
        })
    }

    #[napi(catch_unwind)]
    pub async fn create_index(
        &self,
        index: Option<&Index>,
        column: String,
        replace: Option<bool>,
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
        builder.execute().await.default_error()
    }

    #[napi(catch_unwind)]
    pub async fn update(
        &self,
        only_if: Option<String>,
        columns: Vec<(String, String)>,
    ) -> napi::Result<()> {
        let mut op = self.inner_ref()?.update();
        if let Some(only_if) = only_if {
            op = op.only_if(only_if);
        }
        for (column_name, value) in columns {
            op = op.column(column_name, value);
        }
        op.execute().await.default_error()
    }

    #[napi(catch_unwind)]
    pub fn query(&self) -> napi::Result<Query> {
        Ok(Query::new(self.inner_ref()?.query()))
    }

    #[napi(catch_unwind)]
    pub fn vector_search(&self, vector: Float32Array) -> napi::Result<VectorQuery> {
        self.query()?.nearest_to(vector)
    }

    #[napi(catch_unwind)]
    pub async fn add_columns(&self, transforms: Vec<AddColumnsSql>) -> napi::Result<()> {
        let transforms = transforms
            .into_iter()
            .map(|sql| (sql.name, sql.value_sql))
            .collect::<Vec<_>>();
        let transforms = NewColumnTransform::SqlExpressions(transforms);
        self.inner_ref()?
            .add_columns(transforms, None)
            .await
            .map_err(|err| {
                napi::Error::from_reason(format!(
                    "Failed to add columns to table {}: {}",
                    self.name, err
                ))
            })?;
        Ok(())
    }

    #[napi(catch_unwind)]
    pub async fn alter_columns(&self, alterations: Vec<ColumnAlteration>) -> napi::Result<()> {
        for alteration in &alterations {
            if alteration.rename.is_none() && alteration.nullable.is_none() {
                return Err(napi::Error::from_reason(
                    "Alteration must have a 'rename' or 'nullable' field.",
                ));
            }
        }
        let alterations = alterations
            .into_iter()
            .map(LanceColumnAlteration::from)
            .collect::<Vec<_>>();

        self.inner_ref()?
            .alter_columns(&alterations)
            .await
            .map_err(|err| {
                napi::Error::from_reason(format!(
                    "Failed to alter columns in table {}: {}",
                    self.name, err
                ))
            })?;
        Ok(())
    }

    #[napi(catch_unwind)]
    pub async fn drop_columns(&self, columns: Vec<String>) -> napi::Result<()> {
        let col_refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
        self.inner_ref()?
            .drop_columns(&col_refs)
            .await
            .map_err(|err| {
                napi::Error::from_reason(format!(
                    "Failed to drop columns from table {}: {}",
                    self.name, err
                ))
            })?;
        Ok(())
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
    pub async fn checkout_latest(&self) -> napi::Result<()> {
        self.inner_ref()?.checkout_latest().await.default_error()
    }

    #[napi(catch_unwind)]
    pub async fn restore(&self) -> napi::Result<()> {
        self.inner_ref()?.restore().await.default_error()
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
        let tbl = self.inner_ref()?.as_native().unwrap();
        let stats = tbl.index_stats(&index_name).await.default_error()?;
        Ok(stats.map(IndexStatistics::from))
    }

    #[napi(catch_unwind)]
    pub fn merge_insert(&self, on: Vec<String>) -> napi::Result<NativeMergeInsertBuilder> {
        let on: Vec<_> = on.iter().map(String::as_str).collect();
        Ok(self.inner_ref()?.merge_insert(on.as_slice()).into())
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
    /// Set the new nullability. Note that a nullable column cannot be made non-nullable.
    pub nullable: Option<bool>,
}

impl From<ColumnAlteration> for LanceColumnAlteration {
    fn from(js: ColumnAlteration) -> Self {
        let ColumnAlteration {
            path,
            rename,
            nullable,
        } = js;
        Self {
            path,
            rename,
            nullable,
            // TODO: wire up this field
            data_type: None,
        }
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
    pub index_type: Option<String>,
    /// The metadata for each index
    pub indices: Vec<IndexMetadata>,
}
impl From<lancedb::index::IndexStatistics> for IndexStatistics {
    fn from(value: lancedb::index::IndexStatistics) -> Self {
        Self {
            num_indexed_rows: value.num_indexed_rows as f64,
            num_unindexed_rows: value.num_unindexed_rows as f64,
            index_type: value.index_type.map(|t| format!("{:?}", t)),
            indices: value.indices.into_iter().map(Into::into).collect(),
        }
    }
}

#[napi(object)]
pub struct IndexMetadata {
    pub metric_type: Option<String>,
    pub index_type: Option<String>,
}

impl From<lancedb::index::IndexMetadata> for IndexMetadata {
    fn from(value: lancedb::index::IndexMetadata) -> Self {
        Self {
            metric_type: value.metric_type,
            index_type: value.index_type,
        }
    }
}
