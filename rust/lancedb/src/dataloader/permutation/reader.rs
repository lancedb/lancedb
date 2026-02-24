// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Row ID-based views for LanceDB tables
//!
//! This module provides functionality for creating views that are based on specific row IDs.
//! The `IdView` allows you to create a virtual table that contains only
//! the rows from a source table that correspond to row IDs stored in a separate table.

use crate::arrow::{SendableRecordBatchStream, SimpleRecordBatchStream};
use crate::dataloader::permutation::builder::SRC_ROW_ID_COL;
use crate::dataloader::permutation::split::SPLIT_ID_COLUMN;
use crate::error::Error;
use crate::query::{
    ExecutableQuery, QueryBase, QueryExecutionOptions, QueryFilter, QueryRequest, Select,
};
use crate::table::{AnyQuery, BaseTable, Filter};
use crate::{Result, Table};
use arrow::array::AsArray;
use arrow::compute::concat_batches;
use arrow::datatypes::UInt64Type;
use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::SchemaRef;
use futures::{StreamExt, TryStreamExt};
use lance::dataset::scanner::DatasetRecordBatchStream;
use lance::io::RecordBatchStream;
use lance_arrow::RecordBatchExt;
use lance_core::error::LanceOptionExt;
use lance_core::ROW_ID;
use std::collections::HashMap;
use std::sync::Arc;

/// Reads a permutation of a source table based on row IDs stored in a separate table
#[derive(Clone)]
pub struct PermutationReader {
    base_table: Arc<dyn BaseTable>,
    permutation_table: Option<Arc<dyn BaseTable>>,
    offset: Option<u64>,
    limit: Option<u64>,
    available_rows: u64,
    split: u64,
    // Cached map of offset to row id for the split
    #[allow(clippy::type_complexity)]
    offset_map: Arc<tokio::sync::Mutex<Option<Arc<HashMap<u64, u64>>>>>,
}

impl std::fmt::Debug for PermutationReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PermutationReader(base={}, permutation={}, split={}, offset={:?}, limit={:?})",
            self.base_table.name(),
            self.permutation_table
                .as_ref()
                .map(|t| t.name())
                .unwrap_or("--"),
            self.split,
            self.offset,
            self.limit,
        )
    }
}

impl PermutationReader {
    /// Create a new PermutationReader
    pub async fn inner_new(
        base_table: Arc<dyn BaseTable>,
        permutation_table: Option<Arc<dyn BaseTable>>,
        split: u64,
    ) -> Result<Self> {
        let mut slf = Self {
            base_table,
            permutation_table,
            offset: None,
            limit: None,
            available_rows: 0,
            split,
            offset_map: Arc::new(tokio::sync::Mutex::new(None)),
        };
        slf.validate().await?;
        // Calculate the number of available rows
        slf.available_rows = slf.verify_limit_offset(None, None).await?;
        if slf.available_rows == 0 {
            return Err(Error::InvalidInput {
                message: "No rows found in the permutation table for the given split".to_string(),
            });
        }
        Ok(slf)
    }

    pub async fn try_from_tables(
        base_table: Arc<dyn BaseTable>,
        permutation_table: Arc<dyn BaseTable>,
        split: u64,
    ) -> Result<Self> {
        Self::inner_new(base_table, Some(permutation_table), split).await
    }

    pub async fn identity(base_table: Arc<dyn BaseTable>) -> Self {
        Self::inner_new(base_table, None, 0).await.unwrap()
    }

    /// Validates the limit and offset and returns the number of rows that will be read
    fn validate_limit_offset(
        limit: Option<u64>,
        offset: Option<u64>,
        available_rows: u64,
    ) -> Result<u64> {
        match (limit, offset) {
            (Some(limit), Some(offset)) => {
                if offset + limit > available_rows {
                    Err(Error::InvalidInput {
                        message: "Offset + limit is greater than the number of rows in the permutation table"
                            .to_string(),
                    })
                } else {
                    Ok(limit)
                }
            }
            (None, Some(offset)) => {
                if offset > available_rows {
                    Err(Error::InvalidInput {
                        message:
                            "Offset is greater than the number of rows in the permutation table"
                                .to_string(),
                    })
                } else {
                    Ok(available_rows - offset)
                }
            }
            (Some(limit), None) => {
                if limit > available_rows {
                    Err(Error::InvalidInput {
                        message:
                            "Limit is greater than the number of rows in the permutation table"
                                .to_string(),
                    })
                } else {
                    Ok(limit)
                }
            }
            (None, None) => Ok(available_rows),
        }
    }

    async fn verify_limit_offset(&self, limit: Option<u64>, offset: Option<u64>) -> Result<u64> {
        let available_rows = if let Some(permutation_table) = &self.permutation_table {
            permutation_table
                .count_rows(Some(Filter::Sql(format!(
                    "{} = {}",
                    SPLIT_ID_COLUMN, self.split
                ))))
                .await? as u64
        } else {
            self.base_table.count_rows(None).await? as u64
        };
        Self::validate_limit_offset(limit, offset, available_rows)
    }

    pub async fn with_offset(mut self, offset: u64) -> Result<Self> {
        let available_rows = self.verify_limit_offset(self.limit, Some(offset)).await?;
        self.offset = Some(offset);
        self.available_rows = available_rows;
        self.offset_map = Arc::new(tokio::sync::Mutex::new(None));
        Ok(self)
    }

    pub async fn with_limit(mut self, limit: u64) -> Result<Self> {
        let available_rows = self.verify_limit_offset(Some(limit), self.offset).await?;
        self.available_rows = available_rows;
        self.limit = Some(limit);
        self.offset_map = Arc::new(tokio::sync::Mutex::new(None));
        Ok(self)
    }

    fn is_sorted_already<'a, T: Iterator<Item = &'a u64>>(iter: T) -> bool {
        for (expected, idx) in iter.enumerate() {
            if *idx != expected as u64 {
                return false;
            }
        }
        true
    }

    async fn load_batch(
        base_table: &Arc<dyn BaseTable>,
        row_ids: RecordBatch,
        selection: Select,
    ) -> Result<RecordBatch> {
        let has_row_id = Self::has_row_id(&selection)?;

        let num_rows = row_ids.num_rows();
        let row_ids = row_ids
            .column(0)
            .as_primitive_opt::<UInt64Type>()
            .expect_ok()?
            .values();

        let filter = format!(
            "_rowid in ({})",
            row_ids
                .iter()
                .map(|o| o.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );

        let base_query = QueryRequest {
            filter: Some(QueryFilter::Sql(filter)),
            select: selection,
            with_row_id: true,
            ..Default::default()
        };

        let data = base_table
            .query(
                &AnyQuery::Query(base_query),
                QueryExecutionOptions {
                    max_batch_length: num_rows as u32,
                    ..Default::default()
                },
            )
            .await?;
        let schema = data.schema();

        let batches = data.try_collect::<Vec<_>>().await?;

        if batches.is_empty() {
            return Err(Error::InvalidInput {
                message: "Base table returned no batches".to_string(),
            });
        }

        if batches.iter().map(|b| b.num_rows()).sum::<usize>() != num_rows {
            return Err(Error::InvalidInput {
                message: "Base table returned different number of rows than the number of row IDs"
                    .to_string(),
            });
        }

        let batch = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            concat_batches(&schema, &batches)?
        };

        // There is no guarantee the result order will match the order provided
        // so may need to restore order
        let actual_row_ids = batch
            .column_by_name(ROW_ID)
            .expect_ok()?
            .as_primitive_opt::<UInt64Type>()
            .expect_ok()?
            .values();

        // Map from row id to order in batch, used to restore original ordering
        let ordering = actual_row_ids
            .iter()
            .copied()
            .enumerate()
            .map(|(i, o)| (o, i as u64))
            .collect::<HashMap<_, _>>();

        let desired_idx_order = row_ids
            .iter()
            .map(|o| ordering.get(o).copied().expect_ok().map_err(Error::from))
            .collect::<Result<Vec<_>>>()?;

        let ordered_batch = if Self::is_sorted_already(desired_idx_order.iter()) {
            // Fast path if already sorted, important as data may be large and
            // re-ordering could be expensive
            batch
        } else {
            let desired_idx_order = UInt64Array::from(desired_idx_order);

            arrow_select::take::take_record_batch(&batch, &desired_idx_order)?
        };

        if has_row_id {
            Ok(ordered_batch)
        } else {
            // The user didn't ask for row id, we needed it for ordering the data, but now we drop it
            Ok(ordered_batch.drop_column(ROW_ID)?)
        }
    }

    async fn row_ids_to_batches(
        base_table: Arc<dyn BaseTable>,
        row_ids: DatasetRecordBatchStream,
        selection: Select,
    ) -> Result<SendableRecordBatchStream> {
        let mut stream = row_ids
            .map_err(Error::from)
            .try_filter_map(move |batch| {
                let selection = selection.clone();
                let base_table = base_table.clone();
                async move {
                    Self::load_batch(&base_table, batch, selection)
                        .await
                        .map(Some)
                }
            })
            .boxed();

        // Need to read out first batch to get schema
        let Some(first_batch) = stream.try_next().await? else {
            return Err(Error::InvalidInput {
                message: "Permutation was empty".to_string(),
            });
        };
        let schema = first_batch.schema();

        let stream = futures::stream::once(std::future::ready(Ok(first_batch))).chain(stream);

        Ok(Box::pin(SimpleRecordBatchStream::new(stream, schema)))
    }

    fn has_row_id(selection: &Select) -> Result<bool> {
        match selection {
            Select::All => {
                // _rowid is a system column and is not included in Select::All
                Ok(false)
            }
            Select::Columns(columns) => Ok(columns.contains(&ROW_ID.to_string())),
            Select::Dynamic(columns) => {
                for column in columns {
                    if column.0 == ROW_ID {
                        if column.1 == ROW_ID {
                            return Ok(true);
                        } else {
                            return Err(Error::InvalidInput {
                                message: format!(
                                    "Dynamic column {} cannot be used to select _rowid",
                                    column.1
                                ),
                            });
                        }
                    }
                }
                Ok(false)
            }
        }
    }

    async fn validate(&self) -> Result<()> {
        if let Some(permutation_table) = &self.permutation_table {
            let schema = permutation_table.schema().await?;
            if schema.column_with_name(SRC_ROW_ID_COL).is_none() {
                return Err(Error::InvalidInput {
                    message: "Permutation table must contain a column named row_id".to_string(),
                });
            }
            if schema.column_with_name(SPLIT_ID_COLUMN).is_none() {
                return Err(Error::InvalidInput {
                    message: "Permutation table must contain a column named split_id".to_string(),
                });
            }
        }
        let avail_rows = if let Some(permutation_table) = &self.permutation_table {
            permutation_table.count_rows(None).await? as u64
        } else {
            self.base_table.count_rows(None).await? as u64
        };
        Self::validate_limit_offset(self.limit, self.offset, avail_rows)?;
        Ok(())
    }

    pub async fn read(
        &self,
        selection: Select,
        execution_options: QueryExecutionOptions,
    ) -> Result<SendableRecordBatchStream> {
        // Note: this relies on the row ids query here being returned in consistent order
        let row_ids = if let Some(permutation_table) = &self.permutation_table {
            permutation_table
                .query(
                    &AnyQuery::Query(QueryRequest {
                        select: Select::Columns(vec![SRC_ROW_ID_COL.to_string()]),
                        filter: Some(QueryFilter::Sql(format!(
                            "{} = {}",
                            SPLIT_ID_COLUMN, self.split
                        ))),
                        offset: self.offset.map(|o| o as usize),
                        limit: self.limit.map(|l| l as usize),
                        ..Default::default()
                    }),
                    execution_options,
                )
                .await?
        } else {
            self.base_table
                .query(
                    &AnyQuery::Query(QueryRequest {
                        select: Select::Columns(vec![ROW_ID.to_string()]),
                        offset: self.offset.map(|o| o as usize),
                        limit: self.limit.map(|l| l as usize),
                        ..Default::default()
                    }),
                    execution_options,
                )
                .await?
        };
        Self::row_ids_to_batches(self.base_table.clone(), row_ids, selection).await
    }

    /// If we are going to use `take` then we load the offset -> row id map once for the split and cache it
    ///
    /// This method fetches the map with find-or-create semantics.
    async fn get_offset_map(
        &self,
        permutation_table: &Arc<dyn BaseTable>,
    ) -> Result<Arc<HashMap<u64, u64>>> {
        let mut offset_map_ref = self.offset_map.lock().await;
        if let Some(offset_map) = &*offset_map_ref {
            return Ok(offset_map.clone());
        }
        let mut offset_map = HashMap::new();
        let mut row_ids_query = Table::from(permutation_table.clone())
            .query()
            .select(Select::Columns(vec![SRC_ROW_ID_COL.to_string()]))
            .only_if(format!("{} = {}", SPLIT_ID_COLUMN, self.split));
        if let Some(offset) = self.offset {
            row_ids_query = row_ids_query.offset(offset as usize);
        }
        if let Some(limit) = self.limit {
            row_ids_query = row_ids_query.limit(limit as usize);
        }
        let mut row_ids = row_ids_query.execute().await?;
        let mut idx_offset = 0;
        while let Some(batch) = row_ids.try_next().await? {
            let row_ids = batch
                .column(0)
                .as_primitive::<UInt64Type>()
                .values()
                .to_vec();
            for (i, row_id) in row_ids.iter().enumerate() {
                offset_map.insert(i as u64 + idx_offset, *row_id);
            }
            idx_offset += batch.num_rows() as u64;
        }
        let offset_map = Arc::new(offset_map);
        *offset_map_ref = Some(offset_map.clone());
        Ok(offset_map)
    }

    pub async fn take_offsets(&self, offsets: &[u64], selection: Select) -> Result<RecordBatch> {
        if let Some(permutation_table) = &self.permutation_table {
            let offset_map = self.get_offset_map(permutation_table).await?;
            let row_ids = offsets
                .iter()
                .map(|o| offset_map.get(o).copied().expect_ok().map_err(Error::from))
                .collect::<Result<Vec<_>>>()?;
            let row_ids = RecordBatch::try_new(
                Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                    "row_id",
                    arrow_schema::DataType::UInt64,
                    false,
                )])),
                vec![Arc::new(UInt64Array::from(row_ids))],
            )?;
            Self::load_batch(&self.base_table, row_ids, selection).await
        } else {
            let table = Table::from(self.base_table.clone());
            let batches = table
                .take_offsets(offsets.to_vec())
                .select(selection.clone())
                .execute()
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            if let Some(first_batch) = batches.first() {
                let schema = first_batch.schema();
                let batch = arrow::compute::concat_batches(&schema, &batches)?;
                Ok(batch)
            } else {
                Ok(RecordBatch::try_new(
                    self.output_schema(selection).await?,
                    vec![],
                )?)
            }
        }
    }

    pub async fn output_schema(&self, selection: Select) -> Result<SchemaRef> {
        let table = Table::from(self.base_table.clone());
        table.query().select(selection).output_schema().await
    }

    pub fn count_rows(&self) -> u64 {
        self.available_rows
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Int32Type;
    use arrow_array::{ArrowPrimitiveType, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use lance_datagen::{BatchCount, RowCount};
    use rand::seq::SliceRandom;

    use crate::{
        arrow::SendableRecordBatchStream,
        query::{ExecutableQuery, QueryBase},
        test_utils::datagen::{virtual_table, LanceDbDatagenExt},
        Table,
    };

    use super::*;

    async fn collect_from_stream<T: ArrowPrimitiveType>(
        mut stream: SendableRecordBatchStream,
        column: &str,
    ) -> Vec<T::Native> {
        let mut row_ids = Vec::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            let col_idx = batch.schema().index_of(column).unwrap();
            row_ids.extend(batch.column(col_idx).as_primitive::<T>().values().to_vec());
        }
        row_ids
    }

    async fn collect_column<T: ArrowPrimitiveType>(table: &Table, column: &str) -> Vec<T::Native> {
        collect_from_stream::<T>(
            table
                .query()
                .select(Select::Columns(vec![column.to_string()]))
                .execute()
                .await
                .unwrap(),
            column,
        )
        .await
    }

    #[tokio::test]
    async fn test_permutation_reader() {
        let base_table = lance_datagen::gen_batch()
            .col("idx", lance_datagen::array::step::<Int32Type>())
            .col("other_col", lance_datagen::array::step::<UInt64Type>())
            .into_mem_table("tbl", RowCount::from(9), BatchCount::from(1))
            .await;

        let mut row_ids = collect_column::<UInt64Type>(&base_table, "_rowid").await;
        row_ids.shuffle(&mut rand::rng());
        // Put the last two rows in split 1
        let split_ids = UInt64Array::from_iter_values(
            std::iter::repeat_n(0, row_ids.len() - 2).chain(std::iter::repeat_n(1, 2)),
        );
        let permutation_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("row_id", DataType::UInt64, false),
                Field::new(SPLIT_ID_COLUMN, DataType::UInt64, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(row_ids.clone())),
                Arc::new(split_ids),
            ],
        )
        .unwrap();
        let row_ids_table = virtual_table("row_ids", &permutation_batch).await;

        let reader = PermutationReader::try_from_tables(
            base_table.base_table().clone(),
            row_ids_table.base_table().clone(),
            0,
        )
        .await
        .unwrap();

        // Read split 0
        let mut stream = reader
            .read(
                Select::All,
                QueryExecutionOptions {
                    max_batch_length: 3,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(stream.schema(), base_table.schema().await.unwrap());

        let check_batch = async |stream: &mut SendableRecordBatchStream,
                                 expected_values: &[u64]| {
            let batch = stream.try_next().await.unwrap().unwrap();
            assert_eq!(batch.num_rows(), expected_values.len());
            assert_eq!(
                batch.column(0).as_primitive::<Int32Type>().values(),
                &expected_values
                    .iter()
                    .map(|o| *o as i32)
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                batch.column(1).as_primitive::<UInt64Type>().values(),
                &expected_values
            );
        };

        check_batch(&mut stream, &row_ids[0..3]).await;
        check_batch(&mut stream, &row_ids[3..6]).await;
        check_batch(&mut stream, &row_ids[6..7]).await;
        assert!(stream.try_next().await.unwrap().is_none());

        // Read split 1
        let reader = PermutationReader::try_from_tables(
            base_table.base_table().clone(),
            row_ids_table.base_table().clone(),
            1,
        )
        .await
        .unwrap();

        let mut stream = reader
            .read(
                Select::All,
                QueryExecutionOptions {
                    max_batch_length: 3,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        check_batch(&mut stream, &row_ids[7..9]).await;
        assert!(stream.try_next().await.unwrap().is_none());
    }

    /// Helper to create a base table and permutation table for take_offsets tests.
    /// Returns (base_table, row_ids_table, shuffled_row_ids).
    async fn setup_permutation_tables(num_rows: usize) -> (Table, Table, Vec<u64>) {
        let base_table = lance_datagen::gen_batch()
            .col("idx", lance_datagen::array::step::<Int32Type>())
            .col("other_col", lance_datagen::array::step::<UInt64Type>())
            .into_mem_table("tbl", RowCount::from(num_rows as u64), BatchCount::from(1))
            .await;

        let mut row_ids = collect_column::<UInt64Type>(&base_table, "_rowid").await;
        row_ids.shuffle(&mut rand::rng());

        let split_ids = UInt64Array::from_iter_values(std::iter::repeat_n(0u64, row_ids.len()));
        let permutation_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("row_id", DataType::UInt64, false),
                Field::new(SPLIT_ID_COLUMN, DataType::UInt64, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(row_ids.clone())),
                Arc::new(split_ids),
            ],
        )
        .unwrap();
        let row_ids_table = virtual_table("row_ids", &permutation_batch).await;

        (base_table, row_ids_table, row_ids)
    }

    #[tokio::test]
    async fn test_take_offsets_with_permutation_table() {
        let (base_table, row_ids_table, row_ids) = setup_permutation_tables(10).await;

        let reader = PermutationReader::try_from_tables(
            base_table.base_table().clone(),
            row_ids_table.base_table().clone(),
            0,
        )
        .await
        .unwrap();

        // Take specific offsets and verify the returned rows match the permutation order
        let offsets = vec![0, 2, 4];
        let batch = reader.take_offsets(&offsets, Select::All).await.unwrap();

        assert_eq!(batch.num_rows(), 3);

        let idx_values = batch
            .column(0)
            .as_primitive::<Int32Type>()
            .values()
            .to_vec();
        let expected: Vec<i32> = offsets
            .iter()
            .map(|&o| row_ids[o as usize] as i32)
            .collect();
        assert_eq!(idx_values, expected);
    }

    #[tokio::test]
    async fn test_take_offsets_preserves_order() {
        let (base_table, row_ids_table, row_ids) = setup_permutation_tables(10).await;

        let reader = PermutationReader::try_from_tables(
            base_table.base_table().clone(),
            row_ids_table.base_table().clone(),
            0,
        )
        .await
        .unwrap();

        // Take offsets in reverse order and verify returned rows match that order
        let offsets = vec![5, 3, 1, 0];
        let batch = reader.take_offsets(&offsets, Select::All).await.unwrap();

        assert_eq!(batch.num_rows(), 4);

        let idx_values = batch
            .column(0)
            .as_primitive::<Int32Type>()
            .values()
            .to_vec();
        let expected: Vec<i32> = offsets
            .iter()
            .map(|&o| row_ids[o as usize] as i32)
            .collect();
        assert_eq!(idx_values, expected);
    }

    #[tokio::test]
    async fn test_take_offsets_with_column_selection() {
        let (base_table, row_ids_table, row_ids) = setup_permutation_tables(10).await;

        let reader = PermutationReader::try_from_tables(
            base_table.base_table().clone(),
            row_ids_table.base_table().clone(),
            0,
        )
        .await
        .unwrap();

        let offsets = vec![1, 3];
        let batch = reader
            .take_offsets(&offsets, Select::Columns(vec!["idx".to_string()]))
            .await
            .unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "idx");

        let idx_values = batch
            .column(0)
            .as_primitive::<Int32Type>()
            .values()
            .to_vec();
        let expected: Vec<i32> = offsets
            .iter()
            .map(|&o| row_ids[o as usize] as i32)
            .collect();
        assert_eq!(idx_values, expected);
    }

    #[tokio::test]
    async fn test_take_offsets_invalid_offset() {
        let (base_table, row_ids_table, _) = setup_permutation_tables(5).await;

        let reader = PermutationReader::try_from_tables(
            base_table.base_table().clone(),
            row_ids_table.base_table().clone(),
            0,
        )
        .await
        .unwrap();

        // Offset 999 doesn't exist in the offset map
        let result = reader.take_offsets(&[0, 999], Select::All).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_take_offsets_identity_reader() {
        let base_table = lance_datagen::gen_batch()
            .col("idx", lance_datagen::array::step::<Int32Type>())
            .into_mem_table("tbl", RowCount::from(10), BatchCount::from(1))
            .await;

        let reader = PermutationReader::identity(base_table.base_table().clone()).await;

        // With no permutation table, take_offsets uses the base table directly
        let offsets = vec![0, 2, 4, 6];
        let batch = reader.take_offsets(&offsets, Select::All).await.unwrap();

        assert_eq!(batch.num_rows(), 4);

        let idx_values = batch
            .column(0)
            .as_primitive::<Int32Type>()
            .values()
            .to_vec();
        assert_eq!(idx_values, vec![0, 2, 4, 6]);
    }

    #[tokio::test]
    async fn test_take_offsets_caches_offset_map() {
        let (base_table, row_ids_table, row_ids) = setup_permutation_tables(10).await;

        let reader = PermutationReader::try_from_tables(
            base_table.base_table().clone(),
            row_ids_table.base_table().clone(),
            0,
        )
        .await
        .unwrap();

        // First call populates the cache
        let batch1 = reader.take_offsets(&[0, 1], Select::All).await.unwrap();

        // Second call should use the cached offset map and produce consistent results
        let batch2 = reader.take_offsets(&[0, 1], Select::All).await.unwrap();

        let values1 = batch1
            .column(0)
            .as_primitive::<Int32Type>()
            .values()
            .to_vec();
        let values2 = batch2
            .column(0)
            .as_primitive::<Int32Type>()
            .values()
            .to_vec();
        assert_eq!(values1, values2);

        let expected: Vec<i32> = vec![row_ids[0] as i32, row_ids[1] as i32];
        assert_eq!(values1, expected);
    }

    #[tokio::test]
    async fn test_take_offsets_single_offset() {
        let (base_table, row_ids_table, row_ids) = setup_permutation_tables(5).await;

        let reader = PermutationReader::try_from_tables(
            base_table.base_table().clone(),
            row_ids_table.base_table().clone(),
            0,
        )
        .await
        .unwrap();

        let batch = reader.take_offsets(&[2], Select::All).await.unwrap();

        assert_eq!(batch.num_rows(), 1);
        let idx_values = batch
            .column(0)
            .as_primitive::<Int32Type>()
            .values()
            .to_vec();
        assert_eq!(idx_values, vec![row_ids[2] as i32]);
    }

    #[tokio::test]
    async fn test_filtered_permutation_full_iteration() {
        use crate::dataloader::permutation::builder::PermutationBuilder;

        // Create a base table with 10000 rows where idx goes 0..10000.
        // Filter to even values only, giving 5000 rows in the permutation.
        let base_table = lance_datagen::gen_batch()
            .col("idx", lance_datagen::array::step::<Int32Type>())
            .into_mem_table("tbl", RowCount::from(10000), BatchCount::from(1))
            .await;

        let permutation_table = PermutationBuilder::new(base_table.clone())
            .with_filter("idx % 2 = 0".to_string())
            .build()
            .await
            .unwrap();

        assert_eq!(permutation_table.count_rows(None).await.unwrap(), 5000);

        let reader = PermutationReader::try_from_tables(
            base_table.base_table().clone(),
            permutation_table.base_table().clone(),
            0,
        )
        .await
        .unwrap();

        assert_eq!(reader.count_rows(), 5000);

        // Iterate through all batches using a batch size that doesn't evenly divide
        // the row count (5000 / 128 = 39 full batches + 1 batch of 8 rows).
        let batch_size = 128;
        let mut stream = reader
            .read(
                Select::All,
                QueryExecutionOptions {
                    max_batch_length: batch_size,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let mut total_rows = 0u64;
        let mut all_idx_values = Vec::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            assert!(batch.num_rows() <= batch_size as usize);
            total_rows += batch.num_rows() as u64;
            let idx_col = batch.column(0).as_primitive::<Int32Type>().values();
            all_idx_values.extend(idx_col.iter().copied());
        }

        assert_eq!(total_rows, 5000);
        assert_eq!(all_idx_values.len(), 5000);

        // Every value should be even (from the filter)
        assert!(all_idx_values.iter().all(|v| v % 2 == 0));

        // Should have 5000 unique values
        let unique: std::collections::HashSet<i32> = all_idx_values.iter().copied().collect();
        assert_eq!(unique.len(), 5000);

        // Use take_offsets to fetch rows from the beginning, middle, and end
        // of the permutation. The values should match what we saw during iteration.

        // Beginning
        let batch = reader.take_offsets(&[0, 1, 2], Select::All).await.unwrap();
        assert_eq!(batch.num_rows(), 3);
        let idx_values = batch
            .column(0)
            .as_primitive::<Int32Type>()
            .values()
            .to_vec();
        assert_eq!(idx_values, &all_idx_values[0..3]);

        // Middle
        let batch = reader
            .take_offsets(&[2499, 2500, 2501], Select::All)
            .await
            .unwrap();
        assert_eq!(batch.num_rows(), 3);
        let idx_values = batch
            .column(0)
            .as_primitive::<Int32Type>()
            .values()
            .to_vec();
        assert_eq!(idx_values, &all_idx_values[2499..2502]);

        // End (last 3 rows)
        let batch = reader
            .take_offsets(&[4997, 4998, 4999], Select::All)
            .await
            .unwrap();
        assert_eq!(batch.num_rows(), 3);
        let idx_values = batch
            .column(0)
            .as_primitive::<Int32Type>()
            .values()
            .to_vec();
        assert_eq!(idx_values, &all_idx_values[4997..5000]);
    }
}
