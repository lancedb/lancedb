// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Integration tests for vector-only and FTS-only reranking functionality.

use std::sync::Arc;

use arrow_array::{Float32Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::ROW_ID;
use lancedb::rerankers::rrf::RRFReranker;
use lancedb::rerankers::Reranker;

const RELEVANCE_SCORE: &str = "_relevance_score";

#[tokio::test]
async fn test_rrf_reranker_vector_only() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new(ROW_ID, DataType::UInt64, false),
    ]));

    let vec_results = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(UInt64Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap();

    let reranker = RRFReranker::new(1.0);
    let result = reranker
        .rerank_vector("test query", vec_results)
        .await
        .unwrap();

    // Should have name, _rowid, _relevance_score
    assert_eq!(3, result.schema().fields().len());
    assert_eq!(
        RELEVANCE_SCORE,
        result.schema().fields().get(2).unwrap().name()
    );

    // Scores should be: 1/1=1.0, 1/2=0.5, 1/3=0.333...
    let scores = result
        .column(2)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(scores.value(0), 1.0);
    assert_eq!(scores.value(1), 0.5);
    assert!((scores.value(2) - 0.333333).abs() < 0.0001);
}

#[tokio::test]
async fn test_rrf_reranker_fts_only() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, false),
        Field::new(ROW_ID, DataType::UInt64, false),
    ]));

    let fts_results = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["alpha", "beta"])),
            Arc::new(UInt64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();

    let reranker = RRFReranker::new(60.0); // Default k
    let result = reranker
        .rerank_fts("search query", fts_results)
        .await
        .unwrap();

    // Should have text, _rowid, _relevance_score
    assert_eq!(3, result.schema().fields().len());

    // Scores should be descending
    let scores = result
        .column(2)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert!(scores.value(0) > scores.value(1));

    // First score = 1/(0+60) = 0.0166...
    assert!((scores.value(0) - 1.0 / 60.0).abs() < 0.0001);
}

#[tokio::test]
async fn test_rrf_reranker_vector_only_with_default_k() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("content", DataType::Utf8, false),
        Field::new(ROW_ID, DataType::UInt64, false),
    ]));

    let vec_results = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["first", "second", "third", "fourth"])),
            Arc::new(UInt64Array::from(vec![100, 200, 300, 400])),
        ],
    )
    .unwrap();

    let reranker = RRFReranker::default(); // Uses k=60
    let result = reranker
        .rerank_vector("test", vec_results)
        .await
        .unwrap();

    // Verify all results are present
    assert_eq!(4, result.num_rows());

    // Verify schema includes relevance score
    assert!(result
        .schema()
        .column_with_name(RELEVANCE_SCORE)
        .is_some());

    // Verify scores are in descending order
    let scores = result
        .column_by_name(RELEVANCE_SCORE)
        .unwrap()
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();

    for i in 0..scores.len() - 1 {
        assert!(scores.value(i) >= scores.value(i + 1));
    }
}

#[tokio::test]
async fn test_rrf_reranker_fts_only_single_result() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, false),
        Field::new(ROW_ID, DataType::UInt64, false),
    ]));

    let fts_results = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["single"])),
            Arc::new(UInt64Array::from(vec![1])),
        ],
    )
    .unwrap();

    let reranker = RRFReranker::default();
    let result = reranker.rerank_fts("query", fts_results).await.unwrap();

    // Should handle single result correctly
    assert_eq!(1, result.num_rows());

    let scores = result
        .column_by_name(RELEVANCE_SCORE)
        .unwrap()
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();

    // First (and only) score should be 1/(0+60)
    assert!((scores.value(0) - 1.0 / 60.0).abs() < 0.0001);
}
