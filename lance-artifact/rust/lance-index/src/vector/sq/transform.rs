// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

use arrow::array::AsArray;
use arrow_array::{
    RecordBatch,
    types::{Float16Type, Float32Type, Float64Type},
};
use arrow_schema::{DataType, Field};
use tracing::instrument;

use crate::vector::transform::Transformer;

use lance_arrow::RecordBatchExt;
use lance_core::{Error, Result};

use super::ScalarQuantizer;

pub struct SQTransformer {
    quantizer: ScalarQuantizer,
    input_column: String,
    output_column: String,
}

impl SQTransformer {
    pub fn new(quantizer: ScalarQuantizer, input_column: String, output_column: String) -> Self {
        Self {
            quantizer,
            input_column,
            output_column,
        }
    }
}

impl Debug for SQTransformer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SQTransformer(input={}, output={})",
            self.input_column, self.output_column
        )
    }
}

impl Transformer for SQTransformer {
    #[instrument(name = "SQTransformer::transform", level = "debug", skip_all)]
    fn transform(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let input = batch
            .column_by_name(&self.input_column)
            .ok_or(Error::index(format!(
                "SQ Transform: column {} not found in batch",
                self.input_column
            )))?;
        let fsl = input
            .as_fixed_size_list_opt()
            .ok_or(Error::index("input column is not vector type".to_string()))?;
        let sq_code = match fsl.value_type() {
            DataType::Float16 => self.quantizer.transform::<Float16Type>(input)?,
            DataType::Float32 => self.quantizer.transform::<Float32Type>(input)?,
            DataType::Float64 => self.quantizer.transform::<Float64Type>(input)?,
            _ => {
                return Err(Error::index(format!(
                    "unsupported data type: {}",
                    fsl.value_type()
                )));
            }
        };

        let sq_field = Field::new(&self.output_column, sq_code.data_type().clone(), false);
        let batch = batch
            .try_with_column(sq_field, Arc::new(sq_code))?
            .drop_column(&self.input_column)?;
        Ok(batch)
    }
}
