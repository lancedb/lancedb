// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmark of HNSW graph.
//!
//!
#![allow(clippy::print_stdout)]
use std::collections::HashSet;
use std::sync::Arc;

use all_asserts::assert_gt;
use arrow::array::AsArray;
use arrow::array::{Array, LargeStringArray, RecordBatch, RecordBatchIterator, UInt64Array};
use arrow::datatypes::UInt64Type;
use arrow_schema::{DataType, Field, Schema};
use itertools::Itertools;
use lance::Dataset;
use lance::index::DatasetIndexExt;
use lance_datagen::{RowCount, array};
use lance_index::scalar::inverted::flat_full_text_search;
use lance_index::scalar::{FullTextSearchQuery, InvertedIndexParams};
use object_store::path::Path;

#[tokio::main]
async fn main() {
    env_logger::init();

    const TOTAL: usize = 10_000;
    const DOC_ID: &str = "__example_doc_id";

    let tempdir = tempfile::tempdir().unwrap();
    let dataset_dir = Path::from_filesystem_path(tempdir.path()).unwrap();

    let create_index = true;
    if create_index {
        let row_id_col = Arc::new(UInt64Array::from(
            (0..TOTAL).map(|i| i as u64).collect_vec(),
        ));

        // Generate random words using lance-datagen
        let mut words_gen = array::random_sentence(1, 100, true);
        let doc_col = words_gen
            .generate_default(RowCount::from(TOTAL as u64))
            .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("doc", DataType::LargeUtf8, false),
                Field::new(DOC_ID, DataType::UInt64, false),
            ])),
            vec![doc_col.clone(), row_id_col.clone()],
        )
        .unwrap();

        let batches = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
        let mut dataset = Dataset::write(batches, dataset_dir.as_ref(), None)
            .await
            .unwrap();
        let params = InvertedIndexParams::default();
        let start = std::time::Instant::now();
        dataset
            .create_index(
                &["doc"],
                lance_index::IndexType::Inverted,
                None,
                &params,
                true,
            )
            .await
            .unwrap();
        println!("create_index: {:?}", start.elapsed());
    }

    let dataset = Dataset::open(dataset_dir.as_ref()).await.unwrap();
    // Use a sample word for query - fetch first doc and pick a word from it
    let sample_batch = dataset
        .scan()
        .project(&["doc"])
        .unwrap()
        .limit(Some(1), None)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    let sample_doc = sample_batch["doc"]
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .value(0);
    let query_string = sample_doc.split_whitespace().next().unwrap();
    let query = FullTextSearchQuery::new(query_string.to_owned()).limit(Some(10));
    println!("query: {:?}", query);
    let batch = dataset
        .scan()
        .full_text_search(query.clone())
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    let index_results = batch[DOC_ID]
        .as_primitive::<UInt64Type>()
        .iter()
        .map(|v| v.unwrap())
        .collect::<HashSet<_>>();

    let start = std::time::Instant::now();
    dataset
        .scan()
        .full_text_search(query.clone())
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    println!("full_text_search: {:?}", start.elapsed());

    let batch = dataset
        .scan()
        .project(&["doc"])
        .unwrap()
        .with_row_id()
        .try_into_batch()
        .await
        .unwrap();
    let flat_results = flat_full_text_search(&[&batch], "doc", query_string, None)
        .unwrap()
        .into_iter()
        .collect::<HashSet<_>>();
    assert_gt!(index_results.len(), 0);
    assert_eq!(index_results, flat_results);
}
