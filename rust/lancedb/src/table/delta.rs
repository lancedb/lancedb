// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Streaming row changes between two persisted table versions.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use datafusion_common::DataFusionError;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use lance::Dataset;
use lance::dataset::delta::{DatasetDelta, DatasetDeltaBuilder};
use lance::dataset::scanner::DatasetRecordBatchStream;
use lance::dataset::transaction::{Operation, Transaction};
use lance_core::{ROW_CREATED_AT_VERSION_FIELD, ROW_ID_FIELD, ROW_LAST_UPDATED_AT_VERSION_FIELD};

use crate::error::{Error, Result};

/// A version or tag that identifies one end of a [`TableDelta`].
///
/// Version numbers start at 1. Tags are resolved on the table handle's current
/// branch; a tag that belongs to another branch is rejected instead of being
/// followed to that branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaRef {
    /// A persisted table version on the current branch.
    Version(u64),
    /// A tag that points at a version on the current branch.
    Tag(String),
}

impl From<u64> for DeltaRef {
    fn from(version: u64) -> Self {
        Self::Version(version)
    }
}

impl From<&str> for DeltaRef {
    fn from(tag: &str) -> Self {
        Self::Tag(tag.to_string())
    }
}

impl From<String> for DeltaRef {
    fn from(tag: String) -> Self {
        Self::Tag(tag)
    }
}

/// Streaming row changes between two versions of an embedded LanceDB table.
///
/// A delta is created by [`crate::Table::delta`]. The two endpoints are
/// resolved when the delta is created, so moving a tag or advancing the table
/// afterwards does not change what the delta compares.
///
/// Row-level readers require stable row ids at both endpoints. Tables created
/// with the default local settings do not enable them; create the table with
/// `storage_options={"new_table_enable_stable_row_ids": "true"}` to use
/// [`Self::inserted_rows`], [`Self::updated_rows`], [`Self::upserted_rows`],
/// and [`Self::deleted_row_ids`]. [`Self::list_transactions`] works without
/// stable row ids.
///
/// A nonempty range that contains a `Restore` rejects the row-level readers,
/// because a restored snapshot can retain older row-version metadata that the
/// delta readers would omit. The same happens when transaction history in the
/// range is incomplete. Recompute the checkpoint with a full rescan and
/// reinitialize it at the end version; [`Self::list_transactions`] remains
/// available.
pub struct TableDelta {
    /// The resolved start endpoint. Kept alongside the numeric version so the
    /// handle remains pinned even if the source table is checked out or moves
    /// forward later.
    begin: Dataset,
    /// The resolved end endpoint. Kept for schema construction and to pin the
    /// endpoint independently of the table handle.
    end: Dataset,
    begin_version: u64,
    end_version: u64,
    delta: DatasetDelta,
    transactions: Vec<Transaction>,
    row_change_error: Option<String>,
    row_schema: SchemaRef,
    deleted_schema: SchemaRef,
}

impl std::fmt::Debug for TableDelta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableDelta")
            .field("begin_version", &self.begin_version)
            .field("end_version", &self.end_version)
            .finish_non_exhaustive()
    }
}

impl TableDelta {
    /// Resolve both endpoints from the snapshot currently visible through
    /// `visible`. The table wrapper is not touched, so its checkout state and
    /// version stay exactly as they were.
    pub(crate) async fn from_visible_snapshot(
        visible: &Dataset,
        from_version: DeltaRef,
        to_version: Option<DeltaRef>,
    ) -> Result<Self> {
        let begin = resolve_endpoint(visible, from_version, "from_version").await?;
        let end = match to_version {
            Some(to_version) => resolve_endpoint(visible, to_version, "to_version").await?,
            // The visible snapshot is the implicit target, including when the
            // table handle is in time-travel mode.
            None => visible.clone(),
        };

        let begin_version = begin.version().version;
        let end_version = end.version().version;
        if begin_version > end_version {
            return Err(Error::InvalidInput {
                message: format!(
                    "delta begin version {begin_version} is newer than end version {end_version}; \
                     from_version must be less than or equal to to_version"
                ),
            });
        }

        let delta = DatasetDeltaBuilder::new(end.clone())
            .with_begin_version(begin_version)
            .with_end_version(end_version)
            .build()?;

        // Read the pinned range once so every row reader shares the same
        // Restore/history check instead of scanning it again.
        let transactions = if begin_version == end_version {
            Vec::new()
        } else {
            delta.list_transactions().await?
        };
        let row_change_error = row_change_error(begin_version, end_version, &transactions);

        Ok(Self {
            row_schema: row_change_schema(&end),
            deleted_schema: deleted_row_ids_schema(),
            begin,
            end,
            begin_version,
            end_version,
            delta,
            transactions,
            row_change_error,
        })
    }

    /// The version this delta starts from (exclusive).
    pub fn begin_version(&self) -> u64 {
        self.begin_version
    }

    /// The version this delta ends at (inclusive).
    pub fn end_version(&self) -> u64 {
        self.end_version
    }

    /// Stream rows inserted in `(begin_version, end_version]`.
    ///
    /// Each batch carries the table columns plus `_rowid`,
    /// `_row_created_at_version`, and `_row_last_updated_at_version`, read as
    /// of the end version.
    pub async fn inserted_rows(&self) -> Result<DatasetRecordBatchStream> {
        self.scan_rows(DeltaRowChange::Inserted).await
    }

    /// Stream rows updated in `(begin_version, end_version]`.
    ///
    /// Newly inserted rows are excluded; see [`Self::upserted_rows`] for both.
    /// Each batch carries the table columns plus `_rowid`,
    /// `_row_created_at_version`, and `_row_last_updated_at_version`, read as
    /// of the end version.
    pub async fn updated_rows(&self) -> Result<DatasetRecordBatchStream> {
        self.scan_rows(DeltaRowChange::Updated).await
    }

    /// Stream inserted and updated rows in `(begin_version, end_version]`.
    ///
    /// Each batch carries the table columns plus `_rowid`,
    /// `_row_created_at_version`, and `_row_last_updated_at_version`, read as
    /// of the end version.
    pub async fn upserted_rows(&self) -> Result<DatasetRecordBatchStream> {
        self.scan_rows(DeltaRowChange::Upserted).await
    }

    /// Stream the ids of rows deleted in `(begin_version, end_version]`.
    ///
    /// Each batch carries one `_rowid` column of type `UInt64`. Deleted rows
    /// are not reconstructed from earlier versions.
    pub async fn deleted_row_ids(&self) -> Result<DatasetRecordBatchStream> {
        if self.begin_version == self.end_version {
            return Ok(empty_stream(self.deleted_schema.clone()));
        }
        self.ensure_row_changes_allowed()?;
        Ok(self.delta.get_deleted_row_ids().await?)
    }

    /// List the transactions committed in `(begin_version, end_version]`.
    ///
    /// Unlike the row readers, this does not require stable row ids. A version
    /// can have no readable transaction and is skipped by Lance.
    pub async fn list_transactions(&self) -> Result<Vec<Transaction>> {
        Ok(self.transactions.clone())
    }

    async fn scan_rows(&self, change: DeltaRowChange) -> Result<DatasetRecordBatchStream> {
        if self.begin_version == self.end_version {
            return Ok(empty_stream(self.row_schema.clone()));
        }
        self.ensure_row_changes_allowed()?;
        let rows = match change {
            DeltaRowChange::Inserted => self.delta.get_inserted_rows().await?,
            DeltaRowChange::Updated => self.delta.get_updated_rows().await?,
            DeltaRowChange::Upserted => self.delta.get_upserted_rows().await?,
        };
        Ok(rows)
    }

    fn ensure_row_changes_allowed(&self) -> Result<()> {
        if let Some(message) = &self.row_change_error {
            return Err(Error::InvalidInput {
                message: message.clone(),
            });
        }
        self.ensure_stable_row_ids()
    }

    fn ensure_stable_row_ids(&self) -> Result<()> {
        if self.begin.manifest().uses_stable_row_ids() && self.end.manifest().uses_stable_row_ids()
        {
            return Ok(());
        }
        Err(Error::InvalidInput {
            message: format!(
                "row-level delta requires stable row ids at both endpoints (versions {} and {}); \
                 create the table with storage_options={{\"new_table_enable_stable_row_ids\": \"true\"}} \
                 to use inserted_rows(), updated_rows(), upserted_rows(), or deleted_row_ids(). \
                 list_transactions() is available without stable row ids",
                self.begin_version, self.end_version
            ),
        })
    }
}

fn row_change_error(
    begin_version: u64,
    end_version: u64,
    transactions: &[Transaction],
) -> Option<String> {
    if begin_version == end_version {
        return None;
    }
    if transactions
        .iter()
        .any(|transaction| matches!(&transaction.operation, Operation::Restore { .. }))
    {
        return Some(format!(
            "row-level table.delta cannot verify versions ({begin_version}, {end_version}] \
             because the range contains a Restore; a restore can reintroduce rows with older \
             row-version metadata that the delta readers omit. Recompute the checkpoint with a \
             full rescan and reinitialize it at version {end_version}, or use \
             list_transactions() to inspect the range"
        ));
    }

    // A normal commit in the range records one transaction. Lance's
    // `DatasetDelta::list_transactions` skips versions whose transaction entry
    // is absent, so a short result means we cannot rule out a hidden Restore.
    let expected_transactions = (end_version - begin_version) as usize;
    if transactions.len() != expected_transactions {
        let missing = expected_transactions.saturating_sub(transactions.len());
        return Some(format!(
            "row-level table.delta cannot verify versions ({begin_version}, {end_version}] \
             because transaction history is missing for {missing} of {expected_transactions} \
             versions; a Restore may be hidden. Recompute the checkpoint with a full rescan and \
             reinitialize it at version {end_version}, or use list_transactions() to inspect the \
             available history"
        ));
    }
    None
}

#[derive(Clone, Copy)]
enum DeltaRowChange {
    Inserted,
    Updated,
    Upserted,
}

async fn resolve_endpoint(visible: &Dataset, endpoint: DeltaRef, field: &str) -> Result<Dataset> {
    let current_branch = visible.manifest().branch.clone();
    match endpoint {
        DeltaRef::Version(version) => {
            if version == 0 {
                return Err(Error::InvalidInput {
                    message: format!(
                        "{field} version 0 is not a persisted table version; delta versions start at 1"
                    ),
                });
            }
            visible
                .checkout_version(version)
                .await
                .map_err(|error| endpoint_error(error, field, current_branch.as_deref()))
        }
        DeltaRef::Tag(tag) => {
            let tag_contents = visible
                .tags()
                .get(&tag)
                .await
                .map_err(|error| endpoint_error(error, field, current_branch.as_deref()))?;
            if tag_contents.version == 0 {
                return Err(Error::InvalidInput {
                    message: format!(
                        "{field} tag '{tag}' points at version 0, which is not a persisted table version"
                    ),
                });
            }
            let tag_branch = normalize_branch(tag_contents.branch.as_deref());
            if tag_branch != current_branch {
                return Err(Error::InvalidInput {
                    message: format!(
                        "{field} tag '{tag}' belongs to branch '{}', but this table is on branch '{}'; \
                         table.delta only compares endpoints on the current branch",
                        branch_label(tag_branch.as_deref()),
                        branch_label(current_branch.as_deref())
                    ),
                });
            }
            visible
                .checkout_version(tag_contents.version)
                .await
                .map_err(|error| endpoint_error(error, field, current_branch.as_deref()))
        }
    }
}

fn endpoint_error(error: lance::Error, field: &str, branch: Option<&str>) -> Error {
    match error {
        lance::Error::VersionNotFound { message } => Error::InvalidInput {
            message: format!(
                "{field} does not exist on branch '{}': {message}",
                branch_label(branch)
            ),
        },
        lance::Error::DatasetNotFound { source, .. } => Error::InvalidInput {
            message: format!(
                "{field} does not exist on branch '{}': {source}",
                branch_label(branch)
            ),
        },
        lance::Error::RefNotFound { message } => Error::InvalidInput {
            message: format!("{field} tag does not exist: {message}"),
        },
        other => other.into(),
    }
}

fn normalize_branch(branch: Option<&str>) -> Option<String> {
    match branch {
        None | Some("main") => None,
        Some(name) => Some(name.to_string()),
    }
}

fn branch_label(branch: Option<&str>) -> &str {
    branch.unwrap_or("main")
}

fn row_change_schema(end: &Dataset) -> SchemaRef {
    let base = ArrowSchema::from(end.schema());
    let mut fields = base.fields().to_vec();
    fields.push(Arc::new((*ROW_ID_FIELD).clone()));
    fields.push(Arc::new((*ROW_CREATED_AT_VERSION_FIELD).clone()));
    fields.push(Arc::new((*ROW_LAST_UPDATED_AT_VERSION_FIELD).clone()));
    Arc::new(ArrowSchema::new_with_metadata(
        fields,
        base.metadata().clone(),
    ))
}

fn deleted_row_ids_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![Arc::new((*ROW_ID_FIELD).clone())]))
}

fn empty_stream(schema: SchemaRef) -> DatasetRecordBatchStream {
    let batches = stream::empty::<std::result::Result<RecordBatch, DataFusionError>>();
    DatasetRecordBatchStream::new(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Int32Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use futures::TryStreamExt;
    use lance::dataset::WriteParams;
    use lance::io::RecordBatchStream;
    use lance_file::format::{MAGIC, MAJOR_VERSION, MINOR_VERSION};
    use lance_io::object_writer::ObjectWriter;
    use lance_io::traits::{WriteExt, Writer};
    use lance_table::format::pb;
    use tempfile::tempdir;

    use super::*;
    use crate::connect;
    use crate::query::{ExecutableQuery, QueryBase};
    use crate::table::{BaseTable, NativeTable, WriteOptions};

    fn rows(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(Int32Array::from(vec![0; ids.len()])),
            ],
        )
        .unwrap()
    }

    async fn collect(stream: DatasetRecordBatchStream) -> Vec<RecordBatch> {
        stream.try_collect().await.unwrap()
    }

    fn i32_column(batches: &[RecordBatch], name: &str) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn u64_column(batches: &[RecordBatch], name: &str) -> Vec<u64> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn schema_names(schema: &ArrowSchema) -> Vec<String> {
        schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }

    fn stable_row_ids() -> WriteOptions {
        WriteOptions {
            lance_write_params: Some(WriteParams {
                enable_stable_row_ids: true,
                ..Default::default()
            }),
        }
    }

    #[tokio::test]
    async fn test_delta_streams_versions_tags_and_checkout() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap());
        let conn = conn.execute().await.unwrap();

        let table = conn
            .create_table("t", rows(&[0, 1]))
            .write_options(stable_row_ids())
            .execute()
            .await
            .unwrap();
        assert_eq!(table.version().await.unwrap(), 1);

        table.add(rows(&[2])).execute().await.unwrap();
        assert_eq!(table.version().await.unwrap(), 2);
        let mut tags = table.tags().await.unwrap();
        tags.create("baseline", 2).await.unwrap();

        table
            .update()
            .column("value", "value + 10")
            .execute()
            .await
            .unwrap();
        assert_eq!(table.version().await.unwrap(), 3);

        // Remember the stable row id before deletion so the deleted reader can
        // be checked against a value derived from the table, not the delta.
        let row_id_batches = table
            .query()
            .only_if("id = 1")
            .with_row_id()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let deleted_row_id = u64_column(&row_id_batches, ROW_ID_FIELD.name())[0];

        table.delete("id = 1").await.unwrap();
        assert_eq!(table.version().await.unwrap(), 4);

        let delta = table.delta(DeltaRef::Version(1), None).await.unwrap();
        assert_eq!(delta.begin_version(), 1);
        assert_eq!(delta.end_version(), 4);

        let inserted = collect(delta.inserted_rows().await.unwrap()).await;
        assert_eq!(i32_column(&inserted, "id"), vec![2]);
        assert_eq!(
            schema_names(&inserted[0].schema()),
            vec![
                "id".to_string(),
                "value".to_string(),
                "_rowid".to_string(),
                "_row_created_at_version".to_string(),
                "_row_last_updated_at_version".to_string(),
            ]
        );

        let updated = collect(delta.updated_rows().await.unwrap()).await;
        assert_eq!(i32_column(&updated, "id"), vec![0]);

        let upserted = collect(delta.upserted_rows().await.unwrap()).await;
        let mut upserted_ids = i32_column(&upserted, "id");
        upserted_ids.sort_unstable();
        assert_eq!(upserted_ids, vec![0, 2]);

        let deleted = collect(delta.deleted_row_ids().await.unwrap()).await;
        assert_eq!(
            u64_column(&deleted, ROW_ID_FIELD.name()),
            vec![deleted_row_id]
        );
        assert_eq!(
            schema_names(&deleted[0].schema()),
            vec!["_rowid".to_string()]
        );
        assert_eq!(
            deleted[0]
                .column_by_name(ROW_ID_FIELD.name())
                .unwrap()
                .data_type(),
            &DataType::UInt64
        );

        let transactions = delta.list_transactions().await.unwrap();
        assert_eq!(
            transactions
                .iter()
                .map(|transaction| transaction.operation.name())
                .collect::<Vec<_>>(),
            vec!["Append", "Update", "Delete"]
        );

        // A tag is resolved on the current branch and can be either endpoint.
        let from_tag = table.delta("baseline", None).await.unwrap();
        assert_eq!(from_tag.begin_version(), 2);
        assert_eq!(from_tag.end_version(), 4);
        let mut updated_ids =
            i32_column(&collect(from_tag.updated_rows().await.unwrap()).await, "id");
        updated_ids.sort_unstable();
        // Row 2 was created exactly at the baseline, so the later update to it
        // counts as an update in this range.
        assert_eq!(updated_ids, vec![0, 2]);

        let to_tag = table
            .delta(1u64, Some(DeltaRef::Tag("baseline".to_string())))
            .await
            .unwrap();
        assert_eq!(to_tag.end_version(), 2);
        let inserted = collect(to_tag.inserted_rows().await.unwrap()).await;
        assert_eq!(i32_column(&inserted, "id"), vec![2]);

        // Moving a tag does not change an existing delta; a new delta sees the
        // new target.
        tags.update("baseline", 3).await.unwrap();
        assert_eq!(from_tag.begin_version(), 2);
        assert_eq!(to_tag.end_version(), 2);
        let moved_tag = table.delta("baseline", None).await.unwrap();
        assert_eq!(moved_tag.begin_version(), 3);

        // Equal endpoints are empty but keep the reader schemas, without
        // needing to scan or require stable row ids again.
        let equal = table.delta(2u64, Some(DeltaRef::Version(2))).await.unwrap();
        let inserted = collect(equal.inserted_rows().await.unwrap()).await;
        assert!(inserted.is_empty());
        let inserted_schema = equal.inserted_rows().await.unwrap().schema();
        assert_eq!(
            schema_names(&inserted_schema),
            vec![
                "id".to_string(),
                "value".to_string(),
                "_rowid".to_string(),
                "_row_created_at_version".to_string(),
                "_row_last_updated_at_version".to_string(),
            ]
        );
        let deleted = collect(equal.deleted_row_ids().await.unwrap()).await;
        assert!(deleted.is_empty());
        assert_eq!(
            schema_names(&equal.deleted_row_ids().await.unwrap().schema()),
            vec!["_rowid".to_string()]
        );

        // Invalid and reversed endpoints are actionable input errors.
        for result in [
            table.delta(0u64, None).await,
            table.delta(1u64, Some(DeltaRef::Version(0))).await,
            table.delta(4u64, Some(DeltaRef::Version(2))).await,
            table.delta(999u64, None).await,
            table.delta(1u64, Some(DeltaRef::Version(999))).await,
            table.delta("missing-tag", None).await,
            table
                .delta(1u64, Some(DeltaRef::Tag("missing-tag".to_string())))
                .await,
        ] {
            assert!(
                matches!(result, Err(Error::InvalidInput { .. })),
                "expected an invalid input error, got {result:?}"
            );
        }

        // An omitted target follows the visible checkout and leaves the shared
        // table checkout exactly where it was.
        table.checkout(2).await.unwrap();
        let checked_out = table.delta(1u64, None).await.unwrap();
        assert_eq!(checked_out.end_version(), 2);
        let inserted = collect(checked_out.inserted_rows().await.unwrap()).await;
        assert_eq!(i32_column(&inserted, "id"), vec![2]);
        assert_eq!(table.version().await.unwrap(), 2);

        // An explicit target still works from a pinned handle without moving it.
        let explicit = table.delta(1u64, Some(DeltaRef::Version(3))).await.unwrap();
        let mut updated_ids =
            i32_column(&collect(explicit.updated_rows().await.unwrap()).await, "id");
        updated_ids.sort_unstable();
        assert_eq!(updated_ids, vec![0, 1]);
        assert_eq!(table.version().await.unwrap(), 2);
        table.checkout_latest().await.unwrap();
        assert_eq!(table.version().await.unwrap(), 4);
    }

    #[tokio::test]
    async fn test_delta_resolves_endpoints_on_the_current_branch() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .read_consistency_interval(std::time::Duration::ZERO)
            .execute()
            .await
            .unwrap();

        let main = conn
            .create_table("t", rows(&[0]))
            .write_options(stable_row_ids())
            .execute()
            .await
            .unwrap();
        let fork = main.version().await.unwrap();
        let branch = main.create_branch("exp", fork).await.unwrap();

        branch.add(rows(&[1])).execute().await.unwrap();
        main.add(rows(&[100, 101, 102])).execute().await.unwrap();
        assert_eq!(
            branch.version().await.unwrap(),
            main.version().await.unwrap(),
            "branch and main must share a version number for this test"
        );

        let mut main_tags = main.tags().await.unwrap();
        main_tags
            .create("main-tag", main.version().await.unwrap())
            .await
            .unwrap();
        let mut branch_tags = branch.tags().await.unwrap();
        branch_tags
            .create("exp-tag", branch.version().await.unwrap())
            .await
            .unwrap();

        // The same-branch tag resolves through the branch, not main's data at
        // the same version number.
        let branch_delta = branch
            .delta(1u64, Some(DeltaRef::Tag("exp-tag".to_string())))
            .await
            .unwrap();
        let inserted = collect(branch_delta.inserted_rows().await.unwrap()).await;
        assert_eq!(i32_column(&inserted, "id"), vec![1]);

        // A tag from another branch is rejected even though its version number
        // also exists on this branch.
        let wrong_branch = branch.delta("main-tag", None).await;
        match wrong_branch {
            Err(Error::InvalidInput { message }) => {
                assert!(message.contains("main-tag"), "{message}");
                assert!(message.contains("exp"), "{message}");
                assert!(message.contains("main"), "{message}");
            }
            other => panic!("expected cross-branch tag rejection, got {other:?}"),
        }

        let wrong_branch = main.delta("exp-tag", None).await;
        assert!(
            matches!(wrong_branch, Err(Error::InvalidInput { .. })),
            "main must reject a branch tag"
        );
    }

    #[tokio::test]
    async fn test_delta_rejects_restore_ranges() {
        for mutation in ["update", "delete"] {
            let tmp_dir = tempdir().unwrap();
            let conn = connect(tmp_dir.path().to_str().unwrap())
                .execute()
                .await
                .unwrap();
            let table = conn
                .create_table("t", rows(&[0]))
                .write_options(stable_row_ids())
                .execute()
                .await
                .unwrap();
            assert_eq!(table.version().await.unwrap(), 1);

            if mutation == "update" {
                table.update().column("value", "1").execute().await.unwrap();
            } else {
                table.delete("id = 0").await.unwrap();
            }
            assert_eq!(table.version().await.unwrap(), 2);

            table.checkout(1).await.unwrap();
            table.restore().await.unwrap();
            assert_eq!(table.version().await.unwrap(), 3);
            assert_eq!(table.count_rows(None).await.unwrap(), 1);

            let restore_range = table.delta(2u64, Some(DeltaRef::Version(3))).await.unwrap();
            assert_eq!(
                restore_range
                    .list_transactions()
                    .await
                    .unwrap()
                    .iter()
                    .map(|transaction| transaction.operation.name())
                    .collect::<Vec<_>>(),
                vec!["Restore"]
            );
            for result in [
                restore_range.inserted_rows().await,
                restore_range.updated_rows().await,
                restore_range.upserted_rows().await,
                restore_range.deleted_row_ids().await,
            ] {
                match result {
                    Err(Error::InvalidInput { message }) => {
                        assert!(message.contains("Restore"), "{mutation}: {message}");
                        assert!(message.contains("full rescan"), "{mutation}: {message}");
                        assert!(message.contains("reinitialize"), "{mutation}: {message}");
                    }
                    Ok(_) => panic!("{mutation}: restore range must be rejected"),
                    Err(other) => panic!("{mutation}: expected invalid input, got {other:?}"),
                }
            }

            // A restore in the middle of the range is still unverifiable.
            table.add(rows(&[1])).execute().await.unwrap();
            assert_eq!(table.version().await.unwrap(), 4);
            match table
                .delta(2u64, Some(DeltaRef::Version(4)))
                .await
                .unwrap()
                .upserted_rows()
                .await
            {
                Err(Error::InvalidInput { message }) => {
                    assert!(message.contains("Restore"), "{mutation}: {message}");
                }
                Ok(_) => panic!("{mutation}: restore inside the range must be rejected"),
                Err(other) => panic!("{mutation}: expected invalid input, got {other:?}"),
            }

            // A restore at the exclusive boundary is the base snapshot, so
            // changes after it are usable.
            let after_restore = table.delta(3u64, Some(DeltaRef::Version(4))).await.unwrap();
            let inserted = collect(after_restore.inserted_rows().await.unwrap()).await;
            assert_eq!(i32_column(&inserted, "id"), vec![1]);
            assert_eq!(
                after_restore
                    .list_transactions()
                    .await
                    .unwrap()
                    .iter()
                    .map(|transaction| transaction.operation.name())
                    .collect::<Vec<_>>(),
                vec!["Append"]
            );

            // Equal endpoints remain empty and do not run the restore guard.
            let equal = table.delta(3u64, Some(DeltaRef::Version(3))).await.unwrap();
            assert!(
                collect(equal.upserted_rows().await.unwrap())
                    .await
                    .is_empty()
            );
        }
    }

    #[test]
    fn test_row_change_guard_requires_complete_restore_free_history() {
        let append = |read_version| {
            Transaction::new(
                read_version,
                Operation::Append {
                    fragments: Vec::new(),
                },
                None,
            )
        };
        let restore = Transaction::new(2, Operation::Restore { version: 1 }, None);

        let missing =
            row_change_error(1, 3, &[append(1)]).expect("missing history must be rejected");
        assert!(
            missing.contains("transaction history is missing"),
            "{missing}"
        );

        let restored =
            row_change_error(1, 3, &[append(1), restore]).expect("a restore must be rejected");
        assert!(restored.contains("Restore"), "{restored}");

        assert!(row_change_error(1, 3, &[append(1), append(2)]).is_none());
        assert!(row_change_error(1, 1, &[]).is_none());
    }

    #[tokio::test]
    async fn test_delta_rejects_missing_transaction_history() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("t", rows(&[0]))
            .write_options(stable_row_ids())
            .execute()
            .await
            .unwrap();
        table.add(rows(&[1])).execute().await.unwrap();
        let uri = table.uri().await.unwrap();

        // Simulate a supported dataset whose latest version has no transaction
        // entry. Lance cannot then tell us whether that version was a Restore,
        // so row readers must fail closed instead of treating it as restore-free.
        let dataset = Dataset::open(&uri).await.unwrap();
        let manifest_path = dataset.manifest_location().path.clone();
        let store = dataset.object_store(None).await.unwrap();
        let mut writer = ObjectWriter::new(store.as_ref(), &manifest_path)
            .await
            .unwrap();
        let mut manifest = pb::Manifest::from(dataset.manifest());
        manifest.transaction_section = None;
        manifest.transaction_section_deprecated = None;
        manifest.transaction_file = String::new();
        let pos = writer.write_protobuf(&manifest).await.unwrap();
        writer
            .write_magics(pos, MAJOR_VERSION, MINOR_VERSION, MAGIC)
            .await
            .unwrap();
        Writer::shutdown(&mut writer).await.unwrap();
        drop(dataset);
        drop(table);

        let table = NativeTable::open(&uri).await.unwrap();
        let delta = table.delta(DeltaRef::Version(1), None).await.unwrap();
        assert!(delta.list_transactions().await.unwrap().is_empty());
        match delta.inserted_rows().await {
            Err(Error::InvalidInput { message }) => {
                assert!(
                    message.contains("transaction history is missing"),
                    "{message}"
                );
                assert!(message.contains("full rescan"), "{message}");
            }
            Ok(_) => panic!("missing transaction history must fail closed"),
            Err(other) => panic!("expected invalid input, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_delta_without_stable_row_ids_guides_but_lists_transactions() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();

        let table = conn.create_table("t", rows(&[0])).execute().await.unwrap();
        table.add(rows(&[1])).execute().await.unwrap();
        let delta = table.delta(1u64, None).await.unwrap();

        for result in [
            delta.inserted_rows().await,
            delta.updated_rows().await,
            delta.upserted_rows().await,
            delta.deleted_row_ids().await,
        ] {
            match result {
                Err(Error::InvalidInput { message }) => {
                    assert!(
                        message.contains("new_table_enable_stable_row_ids"),
                        "{message}"
                    );
                    assert!(message.contains("list_transactions"), "{message}");
                }
                Ok(_) => panic!("row-level delta must require stable row ids"),
                Err(other) => panic!("expected invalid input, got {other:?}"),
            }
        }

        // Transactions remain available without stable row ids.
        let transactions = delta.list_transactions().await.unwrap();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].operation.name(), "Append");

        // Equal endpoints need no row lineage, so their readers are empty.
        let version = table.version().await.unwrap();
        let equal = table
            .delta(version, Some(DeltaRef::Version(version)))
            .await
            .unwrap();
        assert!(
            collect(equal.inserted_rows().await.unwrap())
                .await
                .is_empty()
        );
        assert!(
            collect(equal.deleted_row_ids().await.unwrap())
                .await
                .is_empty()
        );
    }
}
