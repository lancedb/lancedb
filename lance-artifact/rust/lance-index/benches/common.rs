// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Common utilities and data generation for scalar index benchmarks.
use std::sync::Arc;

use arrow::datatypes::{Int64Type, UInt64Type};
use arrow_array::{Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion::physical_plan::SendableRecordBatchStream;
use lance_datafusion::datagen::DatafusionDatagenExt;
use lance_datagen::{BatchCount, RowCount, array, gen_batch};

/// Total number of rows in the dataset
pub const TOTAL_ROWS: u64 = 1_000_000;

/// Number of unique values for low cardinality tests
pub const LOW_CARDINALITY_COUNT: usize = 100;

/// Batch size for streaming data
pub const BATCH_SIZE: u64 = 10_000;

/// Number of batches in the dataset
pub const NUM_BATCHES: u64 = TOTAL_ROWS / BATCH_SIZE;

/// Generate a stream of int64 data with unique values (sequential)
pub fn generate_int_unique_stream() -> SendableRecordBatchStream {
    gen_batch()
        .col("value", array::step::<Int64Type>())
        .col("_rowid", array::step::<UInt64Type>())
        .into_df_stream(
            RowCount::from(BATCH_SIZE),
            BatchCount::from(NUM_BATCHES as u32),
        )
}

/// Generate sorted int64 data with low cardinality (100 unique values)
/// Each value appears 10,000 times consecutively
pub fn generate_int_low_cardinality_stream() -> SendableRecordBatchStream {
    let rows_per_value = TOTAL_ROWS / LOW_CARDINALITY_COUNT as u64;
    let mut batches = Vec::new();
    let mut current_row = 0u64;

    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
        Field::new("_rowid", DataType::UInt64, false),
    ]));

    for value_idx in 0..LOW_CARDINALITY_COUNT {
        let value = value_idx as i64;
        let value_end_row = current_row + rows_per_value;

        while current_row < value_end_row {
            let batch_end = (current_row + BATCH_SIZE).min(value_end_row);
            let batch_size = (batch_end - current_row) as usize;

            // Manually create arrays with proper row IDs
            let values = vec![value; batch_size];
            let row_ids: Vec<u64> = (current_row..batch_end).collect();

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(values)),
                    Arc::new(UInt64Array::from(row_ids)),
                ],
            )
            .unwrap();

            batches.push(Ok(batch));
            current_row = batch_end;
        }
    }

    let stream = futures::stream::iter(batches);
    Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream))
}

/// Generate a stream of string data with unique values
/// Strings are zero-padded to 10 digits for proper lexicographic sorting
pub fn generate_string_unique_stream() -> SendableRecordBatchStream {
    let mut batches = Vec::new();
    let mut current_row = 0u64;

    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("_rowid", DataType::UInt64, false),
    ]));

    while current_row < TOTAL_ROWS {
        let batch_end = (current_row + BATCH_SIZE).min(TOTAL_ROWS);

        // Generate zero-padded strings for proper lexicographic sorting
        let values: Vec<String> = (current_row..batch_end)
            .map(|i| format!("string_{:010}", i))
            .collect();
        let row_ids: Vec<u64> = (current_row..batch_end).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(values)),
                Arc::new(UInt64Array::from(row_ids)),
            ],
        )
        .unwrap();

        batches.push(Ok(batch));
        current_row = batch_end;
    }

    let stream = futures::stream::iter(batches);
    Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream))
}

/// Generate sorted string data with low cardinality (100 unique values)
pub fn generate_string_low_cardinality_stream() -> SendableRecordBatchStream {
    let rows_per_value = TOTAL_ROWS / LOW_CARDINALITY_COUNT as u64;
    let mut batches = Vec::new();
    let mut current_row = 0u64;

    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("_rowid", DataType::UInt64, false),
    ]));

    for value_idx in 0..LOW_CARDINALITY_COUNT {
        let value = format!("value_{:03}", value_idx);
        let value_end_row = current_row + rows_per_value;

        while current_row < value_end_row {
            let batch_end = (current_row + BATCH_SIZE).min(value_end_row);
            let batch_size = (batch_end - current_row) as usize;

            // Manually create arrays with proper row IDs
            let values = vec![value.as_str(); batch_size];
            let row_ids: Vec<u64> = (current_row..batch_end).collect();

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(values)),
                    Arc::new(UInt64Array::from(row_ids)),
                ],
            )
            .unwrap();

            batches.push(Ok(batch));
            current_row = batch_end;
        }
    }

    let stream = futures::stream::iter(batches);
    Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream))
}
