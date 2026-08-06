// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmark of HNSW graph.
//!
//!

use std::{sync::Arc, time::Duration};

use arrow_array::{LargeStringArray, RecordBatch, UInt64Array};
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use itertools::Itertools;
use lance_core::ROW_ID;
use lance_core::cache::LanceCache;
use lance_index::prefilter::NoFilter;
use lance_index::scalar::inverted::document_tokenizer::DocType;
use lance_index::scalar::inverted::query::{FtsSearchParams, Operator, Tokens};
use lance_index::scalar::inverted::{InvertedIndex, InvertedIndexBuilder};
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::{
    metrics::NoOpMetricsCollector, scalar::inverted::tokenizer::InvertedIndexParams,
};
use lance_io::object_store::ObjectStore;
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};
use object_store::path::Path;
use rand::{Rng, SeedableRng, rngs::StdRng};
use rand_distr::Zipf;

fn bench_inverted(c: &mut Criterion) {
    const TOTAL: usize = 1_000_000;

    let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();

    let make_store = |path: &std::path::Path| {
        let index_dir = Path::from_filesystem_path(path).unwrap();
        rt.block_on(async {
            Arc::new(LanceIndexStore::new(
                Arc::new(ObjectStore::local()),
                index_dir,
                Arc::new(LanceCache::no_cache()),
            ))
        })
    };
    let indexing_tempdir = tempfile::tempdir().unwrap();
    let indexing_store = make_store(indexing_tempdir.path());
    let indexing_with_positions_tempdir = tempfile::tempdir().unwrap();
    let indexing_with_positions_store = make_store(indexing_with_positions_tempdir.path());
    let phrase_search_tempdir = tempfile::tempdir().unwrap();
    let phrase_search_store = make_store(phrase_search_tempdir.path());

    let row_id_col = Arc::new(UInt64Array::from(
        (0..TOTAL).map(|i| i as u64).collect_vec(),
    ));

    // Generate Zipf-distributed words to better reflect real-world term frequency.
    const VOCAB_SIZE: usize = 100_000;
    const MIN_WORDS: usize = 1;
    const MAX_WORDS: usize = 100;
    const ZIPF_EXPONENT: f64 = 1.1;
    let vocab: Vec<String> = (0..VOCAB_SIZE).map(|i| format!("term{i:05}")).collect();
    let word_zipf = Zipf::new(VOCAB_SIZE as f64, ZIPF_EXPONENT).unwrap();
    let mut rng = StdRng::seed_from_u64(42);
    let mut docs = Vec::with_capacity(TOTAL);
    for _ in 0..TOTAL {
        let num_words = rng.random_range(MIN_WORDS..=MAX_WORDS);
        let mut doc = String::with_capacity(num_words * 8);
        for i in 0..num_words {
            let idx = (rng.sample(word_zipf) as usize).clamp(1, VOCAB_SIZE) - 1;
            if i > 0 {
                doc.push(' ');
            }
            doc.push_str(&vocab[idx]);
        }
        docs.push(doc);
    }
    let doc_col = Arc::new(LargeStringArray::from(docs));
    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("doc", arrow_schema::DataType::LargeUtf8, false),
            arrow_schema::Field::new(ROW_ID, arrow_schema::DataType::UInt64, false),
        ])
        .into(),
        vec![doc_col.clone(), row_id_col],
    )
    .unwrap();

    c.bench_function(format!("invert_indexing({TOTAL})").as_str(), |b| {
        b.to_async(&rt).iter(|| async {
            let stream = RecordBatchStreamAdapter::new(
                batch.schema(),
                stream::iter(vec![Ok(batch.clone())]),
            );
            let stream = Box::pin(stream);
            let mut builder =
                InvertedIndexBuilder::new(InvertedIndexParams::default().with_position(false));
            black_box({
                builder
                    .update(stream, indexing_store.as_ref(), None)
                    .await
                    .unwrap();
                builder
            });
        })
    });

    c.bench_function(
        format!("invert_indexing_with_positions({TOTAL})").as_str(),
        |b| {
            b.to_async(&rt).iter(|| async {
                let stream = RecordBatchStreamAdapter::new(
                    batch.schema(),
                    stream::iter(vec![Ok(batch.clone())]),
                );
                let stream = Box::pin(stream);
                let mut builder =
                    InvertedIndexBuilder::new(InvertedIndexParams::default().with_position(true));
                black_box({
                    builder
                        .update(stream, indexing_with_positions_store.as_ref(), None)
                        .await
                        .unwrap();
                    builder
                });
            })
        },
    );

    rt.block_on(async {
        let stream =
            RecordBatchStreamAdapter::new(batch.schema(), stream::iter(vec![Ok(batch.clone())]));
        let stream = Box::pin(stream);
        let mut builder =
            InvertedIndexBuilder::new(InvertedIndexParams::default().with_position(true));
        builder
            .update(stream, phrase_search_store.as_ref(), None)
            .await
            .unwrap();
    });
    let invert_index = rt
        .block_on(InvertedIndex::load(
            phrase_search_store,
            None,
            &LanceCache::no_cache(),
        ))
        .unwrap();

    let params = FtsSearchParams::new().with_limit(Some(10));
    let no_filter = Arc::new(NoFilter);

    // Get some sample words from the generated documents for search
    let sample_doc = doc_col.value(0);
    let sample_words: Vec<String> = sample_doc
        .split_whitespace()
        .map(|s| s.to_owned())
        .collect();
    let sample_words_len = sample_words.len();
    const TOKENS_PER_QUERY: usize = 15;
    const QUERY_SET_SIZE: usize = 1024;
    let mut query_rng = StdRng::seed_from_u64(7);
    let mut queries = Vec::with_capacity(QUERY_SET_SIZE);
    for _ in 0..QUERY_SET_SIZE {
        let mut query_tokens = Vec::with_capacity(TOKENS_PER_QUERY);
        for _ in 0..TOKENS_PER_QUERY {
            let word_idx = query_rng.random_range(0..sample_words_len);
            query_tokens.push(sample_words[word_idx].clone());
        }
        queries.push(Arc::new(Tokens::new(query_tokens, DocType::Text)));
    }
    let mut query_idx = 0usize;

    c.bench_function(format!("invert_search({TOTAL})").as_str(), |b| {
        b.to_async(&rt).iter(|| {
            // Cycle through pre-generated queries to avoid skewing benchmark results.
            let query = queries[query_idx % queries.len()].clone();
            query_idx = query_idx.wrapping_add(1);
            let invert_index = invert_index.clone();
            let params = params.clone();
            let no_filter = no_filter.clone();
            async move {
                black_box(
                    invert_index
                        .bm25_search(
                            query,
                            params.clone().into(),
                            Operator::Or,
                            no_filter.clone(),
                            Arc::new(NoOpMetricsCollector),
                            None,
                        )
                        .await
                        .unwrap(),
                );
            }
        })
    });

    let phrase_params = FtsSearchParams::new()
        .with_limit(Some(10))
        .with_phrase_slop(Some(0));
    let phrase_pairs = sample_words
        .windows(2)
        .map(|pair| {
            Arc::new(Tokens::new(
                pair.iter().map(|s| s.to_string()).collect(),
                DocType::Text,
            ))
        })
        .collect_vec();
    let mut phrase_query_idx = 0usize;

    c.bench_function(format!("invert_phrase_search({TOTAL})").as_str(), |b| {
        b.to_async(&rt).iter(|| {
            let query = phrase_pairs[phrase_query_idx % phrase_pairs.len()].clone();
            phrase_query_idx = phrase_query_idx.wrapping_add(1);
            let invert_index = invert_index.clone();
            let params = phrase_params.clone();
            let no_filter = no_filter.clone();
            async move {
                black_box(
                    invert_index
                        .bm25_search(
                            query,
                            params.clone().into(),
                            Operator::And,
                            no_filter.clone(),
                            Arc::new(NoOpMetricsCollector),
                            None,
                        )
                        .await
                        .unwrap(),
                );
            }
        })
    });
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_inverted);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(10);
    targets = bench_inverted);

criterion_main!(benches);
