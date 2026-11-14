// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Row-level differences between two versions of a table.
//!
//! A [`TableDelta`] answers "what changed since version N" without scanning the
//! whole table: it reports the rows inserted, updated or deleted between two
//! versions. It is a bounded comparison, not a continuous change feed.
//!
//! ```no_run
//! # use futures::TryStreamExt;
//! # async fn example(table: &lancedb::Table, checkpoint: u64) -> lancedb::Result<()> {
//! let delta = table.delta().compared_against_version(checkpoint).build().await?;
//! let mut upserted = delta.upserted_rows().await?;
//! while let Some(batch) = upserted.try_next().await? {
//!     // `_rowid`, `_row_created_at_version`, `_row_last_updated_at_version`
//!     // and the table columns.
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Inserted, updated and upserted rows need stable row ids on the table. Create
//! the table with the `new_table_enable_stable_row_ids` storage option set to
//! `true`.

use std::sync::Arc;

use crate::arrow::SendableRecordBatchStream;
use crate::error::{Error, Result};

use super::BaseTable;

/// Which rows of a delta to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaRows {
    /// Rows created in the version range.
    Inserted,
    /// Rows that existed before the range and were updated in it.
    Updated,
    /// Rows inserted or updated in the range.
    Upserted,
    /// The `_rowid` of every row that was removed in the range.
    DeletedRowIds,
}

/// The fixed version range a delta compares: `begin` is exclusive and `end` is
/// inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaRange {
    pub begin: u64,
    pub end: u64,
}

/// Builds a [`TableDelta`].
///
/// Obtained from [`Table::delta`](crate::Table::delta). Choose the range with
/// [`compared_against_version`](Self::compared_against_version) or with both of
/// [`with_begin_version`](Self::with_begin_version) and
/// [`with_end_version`](Self::with_end_version).
pub struct DeltaBuilder {
    table: Arc<dyn BaseTable>,
    compared_against: Option<u64>,
    begin: Option<u64>,
    end: Option<u64>,
}

impl DeltaBuilder {
    pub(crate) fn new(table: Arc<dyn BaseTable>) -> Self {
        Self {
            table,
            compared_against: None,
            begin: None,
            end: None,
        }
    }

    /// Compare the table's current version against `version`.
    ///
    /// Cannot be combined with an explicit begin or end version.
    pub fn compared_against_version(mut self, version: u64) -> Self {
        self.compared_against = Some(version);
        self
    }

    /// Set the start of the range (exclusive). Requires an end version.
    pub fn with_begin_version(mut self, version: u64) -> Self {
        self.begin = Some(version);
        self
    }

    /// Set the end of the range (inclusive). Requires a begin version.
    pub fn with_end_version(mut self, version: u64) -> Self {
        self.end = Some(version);
        self
    }

    /// Validate the range and build the delta.
    ///
    /// The version pair is fixed here. With
    /// [`compared_against_version`](Self::compared_against_version) the table's
    /// current version is read once, so every reader of the delta, and every
    /// retry, compares the same two versions even if the table is written to
    /// afterwards.
    pub async fn build(self) -> Result<TableDelta> {
        let range = match (self.compared_against, self.begin, self.end) {
            (Some(checkpoint), None, None) => {
                let current = self.table.version().await?;
                DeltaRange {
                    begin: checkpoint.min(current),
                    end: checkpoint.max(current),
                }
            }
            (None, Some(begin), Some(end)) => {
                if begin > end {
                    return Err(Error::InvalidInput {
                        message: format!("begin version {begin} is newer than end version {end}"),
                    });
                }
                DeltaRange { begin, end }
            }
            (Some(_), _, _) => {
                return Err(Error::InvalidInput {
                    message: "compared_against_version cannot be combined with an explicit \
                              begin or end version"
                        .to_string(),
                });
            }
            (None, Some(_), None) | (None, None, Some(_)) => {
                return Err(Error::InvalidInput {
                    message: "a delta range needs both a begin version and an end version"
                        .to_string(),
                });
            }
            (None, None, None) => {
                return Err(Error::InvalidInput {
                    message: "specify compared_against_version, or both a begin and an end \
                              version"
                        .to_string(),
                });
            }
        };
        Ok(TableDelta {
            table: self.table,
            range,
        })
    }
}

/// The rows that changed between two versions of a table.
///
/// Each method returns a stream of record batches over the same fixed version
/// range, chosen when the delta was built; later writes do not change it.
pub struct TableDelta {
    table: Arc<dyn BaseTable>,
    range: DeltaRange,
}

impl TableDelta {
    /// The version range this delta compares.
    pub fn range(&self) -> DeltaRange {
        self.range
    }

    /// Rows created in the range, with all table columns plus `_rowid`,
    /// `_row_created_at_version` and `_row_last_updated_at_version`.
    pub async fn inserted_rows(&self) -> Result<SendableRecordBatchStream> {
        self.table.delta_rows(self.range, DeltaRows::Inserted).await
    }

    /// Rows that existed before the range and were updated in it, with the same
    /// columns as [`inserted_rows`](Self::inserted_rows).
    pub async fn updated_rows(&self) -> Result<SendableRecordBatchStream> {
        self.table.delta_rows(self.range, DeltaRows::Updated).await
    }

    /// Rows inserted or updated in the range, with the same columns as
    /// [`inserted_rows`](Self::inserted_rows).
    pub async fn upserted_rows(&self) -> Result<SendableRecordBatchStream> {
        self.table.delta_rows(self.range, DeltaRows::Upserted).await
    }

    /// The `_rowid` of every row removed in the range.
    ///
    /// Needs stable row ids at both endpoints of the range (version 0 is
    /// exempt).
    pub async fn deleted_row_ids(&self) -> Result<SendableRecordBatchStream> {
        self.table
            .delta_rows(self.range, DeltaRows::DeletedRowIds)
            .await
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, Int32Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;

    use super::*;
    use crate::Table;
    use crate::connect;
    use crate::database::listing::OPT_NEW_TABLE_ENABLE_STABLE_ROW_IDS;

    fn batch(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
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

    /// Version 1 holds ids 1-3, version 2 appends 4-5, version 3 updates id 2
    /// and version 4 deletes id 1.
    async fn history_table(stable_row_ids: bool) -> Table {
        let conn = connect("memory://").execute().await.unwrap();
        let table = conn
            .create_table("history", batch(&[1, 2, 3]))
            .storage_option(
                OPT_NEW_TABLE_ENABLE_STABLE_ROW_IDS,
                if stable_row_ids { "true" } else { "false" },
            )
            .execute()
            .await
            .unwrap();
        table.add(batch(&[4, 5])).execute().await.unwrap();
        table
            .update()
            .only_if("id = 2")
            .column("value", "99")
            .execute()
            .await
            .unwrap();
        table.delete("id = 1").await.unwrap();
        assert_eq!(table.version().await.unwrap(), 4);
        table
    }

    async fn sorted_ids(stream: SendableRecordBatchStream) -> Vec<i32> {
        let mut ids = stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .flat_map(|batch| {
                let column = batch.column_by_name("id").unwrap();
                let column = column.as_any().downcast_ref::<Int32Array>().unwrap();
                (0..column.len())
                    .map(|i| column.value(i))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        ids.sort();
        ids
    }

    #[tokio::test]
    async fn test_delta_reports_inserted_updated_and_upserted_rows() {
        let table = history_table(true).await;
        let delta = table
            .delta()
            .compared_against_version(1)
            .build()
            .await
            .unwrap();

        assert_eq!(
            sorted_ids(delta.inserted_rows().await.unwrap()).await,
            [4, 5]
        );
        assert_eq!(sorted_ids(delta.updated_rows().await.unwrap()).await, [2]);
        assert_eq!(
            sorted_ids(delta.upserted_rows().await.unwrap()).await,
            [2, 4, 5]
        );
    }

    #[tokio::test]
    async fn test_delta_rows_carry_version_metadata() {
        let table = history_table(true).await;
        let delta = table
            .delta()
            .compared_against_version(1)
            .build()
            .await
            .unwrap();

        let batches = delta
            .inserted_rows()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let schema = batches[0].schema();
        for name in [
            "_rowid",
            "_row_created_at_version",
            "_row_last_updated_at_version",
        ] {
            assert!(schema.column_with_name(name).is_some(), "missing {name}");
        }
        let created = batches[0]
            .column_by_name("_row_created_at_version")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert!(created.iter().all(|version| version == Some(2)));
    }

    #[tokio::test]
    async fn test_delta_reports_deleted_row_ids() {
        let table = history_table(true).await;
        let delta = table
            .delta()
            .compared_against_version(3)
            .build()
            .await
            .unwrap();

        let deleted = delta
            .deleted_row_ids()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            deleted.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
        assert!(deleted[0].column_by_name("_rowid").is_some());
    }

    #[tokio::test]
    async fn test_delta_between_explicit_versions() {
        let table = history_table(true).await;
        let delta = table
            .delta()
            .with_begin_version(1)
            .with_end_version(2)
            .build()
            .await
            .unwrap();

        assert_eq!(delta.range(), DeltaRange { begin: 1, end: 2 });
        assert_eq!(
            sorted_ids(delta.inserted_rows().await.unwrap()).await,
            [4, 5]
        );
        assert!(
            sorted_ids(delta.updated_rows().await.unwrap())
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_delta_requires_stable_row_ids_for_row_changes() {
        let table = history_table(false).await;
        let delta = table
            .delta()
            .compared_against_version(1)
            .build()
            .await
            .unwrap();

        let err = delta.inserted_rows().await.err().unwrap();
        assert!(err.to_string().contains("stable row id"), "{err}");
    }

    #[tokio::test]
    async fn test_delta_builder_rejects_invalid_ranges() {
        let table = history_table(true).await;
        for builder in [
            table.delta(),
            table.delta().with_begin_version(1),
            table.delta().with_end_version(3),
            table
                .delta()
                .compared_against_version(1)
                .with_end_version(3),
            table.delta().with_begin_version(3).with_end_version(1),
        ] {
            assert!(matches!(
                builder.build().await.err().unwrap(),
                Error::InvalidInput { .. }
            ));
        }
    }

    #[tokio::test]
    async fn test_delta_is_pinned_when_built() {
        let table = history_table(true).await;
        let delta = table
            .delta()
            .compared_against_version(2)
            .build()
            .await
            .unwrap();
        assert_eq!(delta.range(), DeltaRange { begin: 2, end: 4 });

        // A later write must not change what the delta reads.
        table.delete("id = 5").await.unwrap();
        assert_eq!(table.version().await.unwrap(), 5);
        let deleted = delta
            .deleted_row_ids()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(deleted.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert_eq!(sorted_ids(delta.upserted_rows().await.unwrap()).await, [2]);
    }
}
