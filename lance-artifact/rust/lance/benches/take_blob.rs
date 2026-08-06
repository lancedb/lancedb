// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{LargeBinaryArray, RecordBatch, RecordBatchIterator, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use lance::blob::{BlobArrayBuilder, blob_field};
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::{Dataset, ProjectionRequest, ReadParams, WriteParams};
use lance_arrow::BLOB_META_KEY;
use lance_encoding::decoder::DecoderConfig;
use lance_file::reader::FileReaderOptions;
use lance_file::version::LanceFileVersion;
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};
use tokio::runtime::Runtime;
use uuid::Uuid;

const TOTAL_ROWS: usize = 128 * 1024;
const ROWS_PER_BATCH: usize = 1024;
const BLOB_COLUMN: &str = "video_blob";
const ID_COLUMN: &str = "id";

fn bench_take_blob(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");

    let cases = [
        (LanceFileVersion::V2_0, false, "V2_0 cache_off"),
        (LanceFileVersion::V2_1, false, "V2_1 cache_off"),
        (LanceFileVersion::V2_1, true, "V2_1 cache_on"),
        (LanceFileVersion::V2_2, false, "V2_2 cache_off"),
        (LanceFileVersion::V2_2, true, "V2_2 cache_on"),
    ];

    for (version, cache_repetition_index, label) in cases {
        let dataset = Arc::new(runtime.block_on(prepare_dataset(version, cache_repetition_index)));
        bench_take_blobs_by_indices(&runtime, c, dataset.clone(), label, 1);
        bench_take_blobs_by_indices(&runtime, c, dataset.clone(), label, 16);
        bench_take_blob_descriptors(&runtime, c, dataset.clone(), label, 1);
        bench_take_blob_descriptors(&runtime, c, dataset.clone(), label, 16);
        bench_take_blob_descriptors_with_row_addr(&runtime, c, dataset.clone(), label, 1);
        bench_take_blob_descriptors_with_row_addr(&runtime, c, dataset.clone(), label, 16);
        bench_take_id_column(&runtime, c, dataset.clone(), label, 1);
        bench_take_id_column(&runtime, c, dataset, label, 16);
    }
}

fn bench_take_blobs_by_indices(
    runtime: &Runtime,
    c: &mut Criterion,
    dataset: Arc<Dataset>,
    label: &str,
    take_rows: usize,
) {
    let indices = Arc::new(build_indices(take_rows));
    let bench_name = format!("{label} take_blobs_by_indices ({take_rows} rows)");

    c.bench_function(&bench_name, |b| {
        let dataset = dataset.clone();
        let indices = indices.clone();
        b.to_async(runtime).iter(move || {
            let dataset = dataset.clone();
            let indices = indices.clone();
            async move {
                let blobs = dataset
                    .take_blobs_by_indices(indices.as_slice(), BLOB_COLUMN)
                    .await
                    .expect("take_blobs_by_indices failed");
                black_box(blobs.len());
            }
        });
    });
}

fn bench_take_blob_descriptors(
    runtime: &Runtime,
    c: &mut Criterion,
    dataset: Arc<Dataset>,
    label: &str,
    take_rows: usize,
) {
    let indices = Arc::new(build_indices(take_rows));
    let projection = ProjectionRequest::from_columns([BLOB_COLUMN], dataset.schema());
    let bench_name = format!("{label} take_rows(blob descriptor) ({take_rows} rows)");

    c.bench_function(&bench_name, |b| {
        let dataset = dataset.clone();
        let indices = indices.clone();
        let projection = projection.clone();
        b.to_async(runtime).iter(move || {
            let dataset = dataset.clone();
            let indices = indices.clone();
            let projection = projection.clone();
            async move {
                let batch = dataset
                    .take_rows(indices.as_slice(), projection.clone())
                    .await
                    .expect("take_rows on blob column failed");
                black_box(batch.num_rows());
            }
        });
    });
}

fn bench_take_blob_descriptors_with_row_addr(
    runtime: &Runtime,
    c: &mut Criterion,
    dataset: Arc<Dataset>,
    label: &str,
    take_rows: usize,
) {
    let indices = Arc::new(build_indices(take_rows));
    let projection = ProjectionRequest::from_columns([BLOB_COLUMN], dataset.schema());
    let bench_name = format!("{label} take_builder(blob+rowaddr) ({take_rows} rows)");

    c.bench_function(&bench_name, |b| {
        let dataset = dataset.clone();
        let indices = indices.clone();
        let projection = projection.clone();
        b.to_async(runtime).iter(move || {
            let dataset = dataset.clone();
            let indices = indices.clone();
            let projection = projection.clone();
            async move {
                let batch = dataset
                    .take_builder(indices.as_slice(), projection.clone())
                    .expect("failed to create take_builder")
                    .with_row_address(true)
                    .execute()
                    .await
                    .expect("take_builder execute failed");
                black_box(batch.num_rows());
            }
        });
    });
}

fn bench_take_id_column(
    runtime: &Runtime,
    c: &mut Criterion,
    dataset: Arc<Dataset>,
    label: &str,
    take_rows: usize,
) {
    let indices = Arc::new(build_indices(take_rows));
    let projection = ProjectionRequest::from_columns([ID_COLUMN], dataset.schema());
    let bench_name = format!("{label} take_rows(id column) ({take_rows} rows)");

    c.bench_function(&bench_name, |b| {
        let dataset = dataset.clone();
        let indices = indices.clone();
        let projection = projection.clone();
        b.to_async(runtime).iter(move || {
            let dataset = dataset.clone();
            let indices = indices.clone();
            let projection = projection.clone();
            async move {
                let batch = dataset
                    .take_rows(indices.as_slice(), projection.clone())
                    .await
                    .expect("take_rows on id column failed");
                black_box(batch.num_rows());
            }
        });
    });
}

fn build_indices(take_rows: usize) -> Vec<u64> {
    if take_rows == 1 {
        return vec![(TOTAL_ROWS / 2) as u64];
    }

    let step = TOTAL_ROWS / take_rows;
    (0..take_rows).map(|i| (i * step) as u64).collect()
}

async fn prepare_dataset(version: LanceFileVersion, cache_repetition_index: bool) -> Dataset {
    let label = match version {
        LanceFileVersion::V2_0 => "v2_0",
        LanceFileVersion::V2_1 => "v2_1",
        LanceFileVersion::V2_2 => "v2_2",
        LanceFileVersion::V2_3 => "v2_3",
        LanceFileVersion::Legacy => "legacy",
        LanceFileVersion::Stable => "stable",
        LanceFileVersion::Next => "next",
    };
    let cache_label = if cache_repetition_index {
        "cache_on"
    } else {
        "cache_off"
    };
    let uri = std::env::temp_dir()
        .join(format!(
            "take-blob-{label}-{cache_label}-{}",
            Uuid::new_v4()
        ))
        .to_string_lossy()
        .into_owned();
    write_blob_dataset(&uri, version, cache_repetition_index).await
}

async fn write_blob_dataset(
    uri: &str,
    version: LanceFileVersion,
    cache_repetition_index: bool,
) -> Dataset {
    let batches = if version >= LanceFileVersion::V2_2 {
        make_blob_v2_batches()
    } else {
        make_legacy_blob_batches()
    };
    let schema = batches[0].schema();

    let write_params = WriteParams {
        data_storage_version: Some(version),
        max_rows_per_file: TOTAL_ROWS,
        max_rows_per_group: ROWS_PER_BATCH,
        ..Default::default()
    };

    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    Dataset::write(reader, uri, Some(write_params))
        .await
        .expect("failed to write benchmark dataset");

    let mut read_params = ReadParams::default();
    if cache_repetition_index {
        read_params.file_reader_options(FileReaderOptions {
            decoder_config: DecoderConfig {
                cache_repetition_index: true,
                ..Default::default()
            },
            ..Default::default()
        });
    }

    DatasetBuilder::from_uri(uri)
        .with_read_params(read_params)
        .load()
        .await
        .expect("failed to reopen benchmark dataset")
}

fn make_blob_v2_batches() -> Vec<RecordBatch> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::UInt64, false),
        blob_field(BLOB_COLUMN, true),
    ]));

    let mut batches = Vec::with_capacity(TOTAL_ROWS.div_ceil(ROWS_PER_BATCH));
    let mut start = 0usize;

    while start < TOTAL_ROWS {
        let end = usize::min(start + ROWS_PER_BATCH, TOTAL_ROWS);
        let ids = Arc::new(UInt64Array::from_iter_values(start as u64..end as u64));

        let mut blobs = BlobArrayBuilder::new(end - start);
        for idx in start..end {
            blobs
                .push_bytes(format!("blob-payload-{idx}").as_bytes())
                .expect("failed to append blob payload");
        }

        let batch = RecordBatch::try_new(schema.clone(), vec![ids, blobs.finish().unwrap()])
            .expect("failed to build v2 blob batch");
        batches.push(batch);
        start = end;
    }

    batches
}

fn make_legacy_blob_batches() -> Vec<RecordBatch> {
    let mut metadata = HashMap::new();
    metadata.insert(BLOB_META_KEY.to_string(), "true".to_string());

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new(BLOB_COLUMN, DataType::LargeBinary, true).with_metadata(metadata),
    ]));

    let mut batches = Vec::with_capacity(TOTAL_ROWS.div_ceil(ROWS_PER_BATCH));
    let mut start = 0usize;

    while start < TOTAL_ROWS {
        let end = usize::min(start + ROWS_PER_BATCH, TOTAL_ROWS);
        let ids = Arc::new(UInt64Array::from_iter_values(start as u64..end as u64));
        let payloads = Arc::new(LargeBinaryArray::from_iter_values(
            (start..end).map(|idx| format!("blob-payload-{idx}").into_bytes()),
        ));

        let batch = RecordBatch::try_new(schema.clone(), vec![ids, payloads])
            .expect("failed to build legacy blob batch");
        batches.push(batch);
        start = end;
    }

    batches
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().sample_size(50).with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_take_blob
);
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().sample_size(30);
    targets = bench_take_blob
);
criterion_main!(benches);
