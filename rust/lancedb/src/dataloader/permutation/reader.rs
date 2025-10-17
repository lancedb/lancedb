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
use lance_core::error::LanceOptionExt;
use lance_core::ROW_ID;
use std::collections::HashMap;
use std::sync::Arc;

/// Reads a permutation of a source table based on row IDs stored in a separate table
#[derive(Clone)]
pub struct PermutationReader {
    base_table: Arc<dyn BaseTable>,
    permutation_table: Arc<dyn BaseTable>,
    offset: Option<u64>,
    limit: Option<u64>,
    available_rows: u64,
    split: u64,
}

impl std::fmt::Debug for PermutationReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PermutationReader(base={}, permutation={}, split={}, offset={:?}, limit={:?})",
            self.base_table.name(),
            self.permutation_table.name(),
            self.split,
            self.offset,
            self.limit,
        )
    }
}

impl PermutationReader {
    /// Create a new PermutationReader
    pub async fn try_new(
        base_table: Arc<dyn BaseTable>,
        permutation_table: Arc<dyn BaseTable>,
        split: u64,
    ) -> Result<Self> {
        let mut slf = Self {
            base_table,
            permutation_table,
            offset: None,
            limit: None,
            available_rows: 0,
            split,
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

    async fn verify_limit_offset(&self, limit: Option<u64>, offset: Option<u64>) -> Result<u64> {
        let available_rows = self
            .permutation_table
            .count_rows(Some(Filter::Sql(format!(
                "{} = {}",
                SPLIT_ID_COLUMN, self.split
            ))))
            .await? as u64;
        if let Some(offset) = offset {
            if let Some(limit) = limit {
                if offset + limit > available_rows {
                    Err(Error::InvalidInput {
                        message: "Offset + limit is greater than the number of rows in the permutation table"
                            .to_string(),
                    })
                } else {
                    Ok(limit)
                }
            } else {
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
        } else if let Some(limit) = limit {
            if limit > available_rows {
                return Err(Error::InvalidInput {
                    message: "Limit is greater than the number of rows in the permutation table"
                        .to_string(),
                });
            } else {
                Ok(limit)
            }
        } else {
            Ok(available_rows)
        }
    }

    pub async fn with_offset(mut self, offset: u64) -> Result<Self> {
        let available_rows = self.verify_limit_offset(self.limit, Some(offset)).await?;
        self.offset = Some(offset);
        self.available_rows = available_rows;
        Ok(self)
    }

    pub async fn with_limit(mut self, limit: u64) -> Result<Self> {
        let available_rows = self.verify_limit_offset(Some(limit), self.offset).await?;
        self.available_rows = available_rows;
        self.limit = Some(limit);
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
        has_row_id: bool,
    ) -> Result<RecordBatch> {
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
        let has_row_id = Self::has_row_id(&selection)?;
        let mut stream = row_ids
            .map_err(Error::from)
            .try_filter_map(move |batch| {
                let selection = selection.clone();
                let base_table = base_table.clone();
                async move {
                    Self::load_batch(&base_table, batch, selection, has_row_id)
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
        let schema = self.permutation_table.schema().await?;
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
        let avail_rows = self.permutation_table.count_rows(None).await? as u64;
        if let Some(offset) = self.offset {
            if let Some(limit) = self.limit {
                if offset + limit > avail_rows {
                    return Err(Error::InvalidInput {
                        message: "Offset + limit is greater than the number of rows in the permutation table"
                            .to_string(),
                    });
                }
            } else {
                if offset > avail_rows {
                    return Err(Error::InvalidInput {
                        message:
                            "Offset is greater than the number of rows in the permutation table"
                                .to_string(),
                    });
                }
            }
        } else if let Some(limit) = self.limit {
            if limit > avail_rows {
                return Err(Error::InvalidInput {
                    message: "Limit is greater than the number of rows in the permutation table"
                        .to_string(),
                });
            }
        }
        Ok(())
    }

    pub async fn read(
        &self,
        selection: Select,
        execution_options: QueryExecutionOptions,
    ) -> Result<SendableRecordBatchStream> {
        let row_ids = self
            .permutation_table
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
            .await?;

        Self::row_ids_to_batches(self.base_table.clone(), row_ids, selection).await
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

        let reader = PermutationReader::try_new(
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
        let reader = PermutationReader::try_new(
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
}
