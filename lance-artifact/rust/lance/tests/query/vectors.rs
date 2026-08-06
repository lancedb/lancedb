// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::{test_ann, test_scan, test_take};
use crate::utils::DatasetTestCases;
use arrow::datatypes::{Date32Type, Float32Type, Int32Type};
use arrow_array::RecordBatch;
use lance::Dataset;
use lance_datagen::{ArrayGeneratorExt, Dimension, RowCount, array, gen_batch};
use lance_index::IndexType;

fn date_as_i32(date: &str) -> i32 {
    // Return as i32 days since unix epoch.
    use chrono::{NaiveDate, TimeZone, Utc};

    let parsed_date =
        NaiveDate::parse_from_str(date, "%Y-%m-%d").expect("Date should be in YYYY-MM-DD format");

    let unix_epoch = Utc.timestamp_opt(0, 0).unwrap().date_naive();

    (parsed_date - unix_epoch).num_days() as i32
}

#[tokio::test]
async fn test_query_prefilter_date() {
    let batch = gen_batch()
        .col("id", array::step::<Int32Type>())
        .col(
            "value",
            array::step_custom::<Date32Type>(date_as_i32("2020-01-01"), 1).with_random_nulls(0.1),
        )
        .col("vec", array::rand_vec::<Float32Type>(Dimension::from(16)))
        .into_batch_rows(RowCount::from(256))
        .unwrap();
    DatasetTestCases::from_data(batch)
        .with_index_types("value", [None, Some(IndexType::BTree)])
        .with_index_types(
            "vec",
            [
                None,
                Some(IndexType::IvfPq),
                Some(IndexType::IvfSq),
                Some(IndexType::IvfFlat),
                // TODO: HNSW results are very flakey.
                // Some(IndexType::IvfHnswFlat),
                // Some(IndexType::IvfHnswPq),
                // Some(IndexType::IvfHnswSq),
            ],
        )
        .run(|ds: Dataset, original: RecordBatch| async move {
            test_scan(&original, &ds).await;
            test_take(&original, &ds).await;
            test_ann(&original, &ds, "vec", None).await;
            test_ann(&original, &ds, "vec", Some("value is not null")).await;
            test_ann(
                &original,
                &ds,
                "vec",
                Some("value >= DATE '2020-01-03' AND value <= DATE '2020-01-25'"),
            )
            .await;
        })
        .await
}
