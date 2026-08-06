// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_array::RecordBatch;
use arrow_schema::Field;
use lance_arrow::RecordBatchExt;
use lance_core::Error;
use tracing::instrument;

use crate::vector::transform::Transformer;

use super::storage::FLAT_COLUMN;

#[derive(Debug)]
pub struct FlatTransformer {
    input_column: String,
}

impl FlatTransformer {
    pub fn new(input_column: impl AsRef<str>) -> Self {
        Self {
            input_column: input_column.as_ref().to_owned(),
        }
    }
}

impl Transformer for FlatTransformer {
    #[instrument(name = "FlatTransformer::transform", level = "debug", skip_all)]
    fn transform(&self, batch: &RecordBatch) -> lance_core::Result<RecordBatch> {
        let input_arr = batch
            .column_by_name(&self.input_column)
            .ok_or(Error::index(format!(
                "FlatTransform: column {} not found in batch",
                self.input_column
            )))?;
        let field = Field::new(
            FLAT_COLUMN,
            input_arr.data_type().clone(),
            input_arr.is_nullable(),
        );
        // rename the column to FLAT_COLUMN
        let batch = batch
            .drop_column(&self.input_column)?
            .try_with_column(field, input_arr.clone())?;
        Ok(batch)
    }
}
