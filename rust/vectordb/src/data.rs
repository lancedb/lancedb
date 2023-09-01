// Copyright 2023 Lance Developers.
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

//! Data types, schema coercion, and data cleaning and etc.

use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, Schema};

use crate::error::Result;

fn coerce_schema_batch(
    batch: RecordBatch,
    schema: Arc<Schema>,
) -> std::result::Result<RecordBatch, ArrowError> {
    if batch.schema() == schema {
        return Ok(batch);
    }
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            batch
                .column_by_name(field.name())
                .map(|c| {
                    if c.data_type() == field.data_type() {
                        return Ok(c.clone());
                    } else {
                        todo!()
                    }
                })
                .ok_or(|| {
                    ArrowError::SchemaError(format!("Column {} not found in batch", field.name()))
                })
        })
        .flatten()
        .collect::<std::result::Result<Vec<_>, ArrowError>>()?;
    RecordBatch::try_new(schema, columns)
}

/// Coerce the batch reader schema, to match the given [Schema].
///
pub fn coerce_schema(
    reader: impl RecordBatchReader,
    schema: Arc<Schema>,
) -> Result<impl RecordBatchReader> {
    if reader.schema() == schema {
        return Ok(RecordBatchIterator::new(
            reader.into_iter().collect::<Vec<_>>(),
            schema,
        ));
    }
    let batches = reader
        .map(|batch| coerce_schema_batch(batch?, schema.clone()))
        .collect::<Vec<_>>();
    Ok(RecordBatchIterator::new(batches, schema))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow_array::{
        FixedSizeListArray, Float16Array, RecordBatch, RecordBatchIterator, StringArray,
    };
    use arrow_schema::{DataType, Field};
    use half::f16;
    use lance::arrow::FixedSizeListArrayExt;

    #[test]
    fn test_coerce_list_to_fixed_size_list() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "fl",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float16, true)), 64),
                true,
            ),
            Field::new("s", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(
                    FixedSizeListArray::try_new_from_values(
                        Float16Array::from_iter_values((0..256).map(|v| f16::from_f32(v as f32))),
                        64,
                    )
                    .unwrap(),
                ),
                Arc::new(StringArray::from(vec![
                    Some("hello"),
                    Some("world"),
                    Some("from"),
                    Some("lance"),
                ])),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());

        let expected_schema = Arc::new(Schema::new(vec![
            Field::new(
                "fl",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 64),
                true,
            ),
            Field::new("s", DataType::Utf8, true),
        ]));
        let stream = coerce_schema(reader, expected_schema).unwrap();
    }
}
