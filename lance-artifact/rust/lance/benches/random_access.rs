// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use criterion::{Criterion, criterion_group, criterion_main};
use lance::dataset::{Dataset, ProjectionRequest, WriteParams};
use lance_file::version::LanceFileVersion;
use std::collections::HashMap;
use tokio::runtime::Runtime;
use uuid::Uuid;

const TOTAL_ROWS: usize = 500_000;
const BATCH_SIZE: usize = 1024;
const LIMIT: i64 = 10_000;
const SHIP_MODES: [&str; 5] = ["FOB", "RAIL", "AIR", "MAIL", "TRUCK"];
const ROW_IDS: [u64; 5] = [1, 40, 100, 130, 200];

fn bench_random_access(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");

    let dataset_v2_0 = runtime.block_on(prepare_dataset(LanceFileVersion::V2_0, true));
    let dataset_v2_1_fsst = runtime.block_on(prepare_dataset(LanceFileVersion::V2_1, true));
    let dataset_v2_1_no_fsst = runtime.block_on(prepare_dataset(LanceFileVersion::V2_1, false));

    benchmark_dataset(&runtime, c, dataset_v2_0, "V2_0");
    benchmark_dataset(&runtime, c, dataset_v2_1_fsst, "V2_1 (FSST)");
    benchmark_dataset(&runtime, c, dataset_v2_1_no_fsst, "V2_1 (FSST disabled)");
}

fn benchmark_dataset(rt: &Runtime, c: &mut Criterion, dataset: Dataset, label: &str) {
    let dataset = Arc::new(dataset);
    bench_filtered_scan(rt, c, dataset.clone(), label);
    bench_random_take(rt, c, dataset, label);
}

fn bench_filtered_scan(rt: &Runtime, c: &mut Criterion, dataset: Arc<Dataset>, label: &str) {
    let bench_name = format!("{label} Filtered Scan ({LIMIT} limit)");
    c.bench_function(&bench_name, |b| {
        let dataset = dataset.clone();
        b.to_async(rt).iter(move || {
            let dataset = dataset.clone();
            async move {
                let batch = dataset
                    .scan()
                    .filter("l_shipmode = 'FOB'")
                    .expect("failed to apply filter")
                    .limit(Some(LIMIT), None)
                    .expect("failed to set limit")
                    .try_into_batch()
                    .await
                    .expect("scan execution failed");
                assert_eq!(batch.num_rows(), LIMIT as usize);
            }
        });
    });
}

fn bench_random_take(rt: &Runtime, c: &mut Criterion, dataset: Arc<Dataset>, label: &str) {
    let bench_name = format!("{label} Random Take {} rows", ROW_IDS.len());
    let projection = Arc::new(dataset.schema().clone());
    c.bench_function(&bench_name, |b| {
        let dataset = dataset.clone();
        let projection = projection.clone();
        b.to_async(rt).iter(move || {
            let dataset = dataset.clone();
            let projection = projection.clone();
            async move {
                let batch = dataset
                    .take_rows(&ROW_IDS, ProjectionRequest::Schema(projection.clone()))
                    .await
                    .expect("take_rows failed");
                assert_eq!(batch.num_rows(), ROW_IDS.len());
            }
        });
    });
}

fn utf8_field_without_fsst(name: &str) -> Field {
    let mut metadata = HashMap::new();
    metadata.insert("lance-encoding:compression".to_string(), "none".to_string());
    Field::new(name, DataType::Utf8, false).with_metadata(metadata)
}

fn utf8_field_for(version: LanceFileVersion, enable_fsst: bool, name: &str) -> Field {
    if enable_fsst && version >= LanceFileVersion::V2_1 {
        Field::new(name, DataType::Utf8, false)
    } else {
        utf8_field_without_fsst(name)
    }
}

async fn prepare_dataset(version: LanceFileVersion, enable_fsst: bool) -> Dataset {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        utf8_field_for(version, enable_fsst, "l_shipmode"),
        Field::new("l_extendedprice", DataType::Float64, false),
        utf8_field_for(version, enable_fsst, "l_comment"),
    ]));

    let batches = generate_batches(schema.clone());
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

    let params = WriteParams {
        data_storage_version: Some(version),
        ..Default::default()
    };

    let uri = format!(
        "memory://random-access-{}-{}",
        version_label(version),
        Uuid::new_v4()
    );

    Dataset::write(reader, uri.as_str(), Some(params))
        .await
        .expect("failed to write dataset")
}

fn generate_batches(schema: Arc<ArrowSchema>) -> Vec<RecordBatch> {
    let mut batches = Vec::with_capacity(TOTAL_ROWS.div_ceil(BATCH_SIZE));
    let mut start = 0usize;

    while start < TOTAL_ROWS {
        let end = usize::min(start + BATCH_SIZE, TOTAL_ROWS);
        let order_key = Int64Array::from_iter_values((start as i64)..(end as i64));
        let ship_mode = StringArray::from_iter_values(
            (start..end).map(|idx| SHIP_MODES[idx % SHIP_MODES.len()].to_string()),
        );
        let extended_price = Float64Array::from_iter_values((start..end).map(|idx| {
            let base = (idx % 10_000) as f64;
            base * 1.5 + 42.0
        }));
        let comment = StringArray::from_iter_values(
            (start..end).map(|idx| format!("Shipment comment #{idx}")),
        );

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(order_key),
                Arc::new(ship_mode),
                Arc::new(extended_price),
                Arc::new(comment),
            ],
        )
        .expect("failed to build record batch");

        batches.push(batch);
        start = end;
    }

    batches
}

fn version_label(version: LanceFileVersion) -> &'static str {
    match version {
        LanceFileVersion::V2_0 => "v2_0",
        LanceFileVersion::V2_1 => "v2_1",
        _ => "other",
    }
}

criterion_group!(benches, bench_random_access);
criterion_main!(benches);
