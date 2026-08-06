// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    ArrayRef, FixedSizeListArray, Float32Array, Int32Array, ListArray, RecordBatch,
    RecordBatchIterator, StringArray, StructArray, UInt32Array,
    builder::{ListBuilder, StringBuilder},
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field as ArrowField, Fields as ArrowFields};
use lance::Dataset;
use lance::dataset::optimize::{CompactionOptions, compact_files};
use lance::dataset::scanner::{ColumnOrdering, QueryFilter};
use lance::dataset::{ColumnAlteration, InsertBuilder, WriteMode, WriteParams};
use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};
use lance_arrow::FixedSizeListArrayExt;
use lance_index::IndexType;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::optimize::OptimizeOptions;
use lance_index::prefilter::NoFilter;
use lance_index::scalar::inverted::query::{
    BooleanQuery, BoostQuery, FtsQuery, FtsSearchParams, MatchQuery, MultiMatchQuery, Occur,
    Operator, PhraseQuery, collect_query_tokens,
};
use lance_index::scalar::inverted::{DocumentGranularity, Language};
use lance_index::scalar::{FullTextSearchQuery, InvertedIndexParams};
use lance_table::format::IndexMetadata;

use super::{strip_score_column, test_fts, test_scan, test_take};
use crate::utils::DatasetTestCases;

// Build baseline inverted index parameters for tests, toggling token positions.
fn base_inverted_params(with_position: bool) -> InvertedIndexParams {
    InvertedIndexParams::new("simple".to_string(), Language::English)
        .with_position(with_position)
        .lower_case(true)
        .stem(false)
        .remove_stop_words(false)
        .ascii_folding(false)
        .max_token_length(None)
}

fn params_for(base_tokenizer: &str, lower_case: bool, with_position: bool) -> InvertedIndexParams {
    InvertedIndexParams::new(base_tokenizer.to_string(), Language::English)
        .with_position(with_position)
        .lower_case(lower_case)
        .stem(false)
        .remove_stop_words(false)
        .ascii_folding(false)
        .max_token_length(None)
}

fn list_element_params(with_position: bool) -> InvertedIndexParams {
    base_inverted_params(with_position).document_granularity(DocumentGranularity::ListElement)
}

fn list_element_match_node(column: &str, terms: &str) -> MatchQuery {
    MatchQuery::new(terms.to_string())
        .with_column(Some(column.to_string()))
        .with_document_granularity(DocumentGranularity::ListElement)
}

fn list_element_match(column: &str, terms: &str) -> FullTextSearchQuery {
    FullTextSearchQuery::new_query(FtsQuery::Match(list_element_match_node(column, terms)))
}

fn list_element_phrase(column: &str, terms: &str) -> FullTextSearchQuery {
    FullTextSearchQuery::new_query(FtsQuery::Phrase(
        PhraseQuery::new(terms.to_string())
            .with_column(Some(column.to_string()))
            .with_document_granularity(DocumentGranularity::ListElement),
    ))
}

fn row_match_node(column: &str, terms: &str) -> MatchQuery {
    MatchQuery::new(terms.to_string())
        .with_column(Some(column.to_string()))
        .with_document_granularity(DocumentGranularity::Row)
}

fn row_match(column: &str, terms: &str) -> FullTextSearchQuery {
    FullTextSearchQuery::new_query(FtsQuery::Match(row_match_node(column, terms)))
}

fn row_phrase(column: &str, terms: &str) -> FullTextSearchQuery {
    FullTextSearchQuery::new_query(FtsQuery::Phrase(
        PhraseQuery::new(terms.to_string())
            .with_column(Some(column.to_string()))
            .with_document_granularity(DocumentGranularity::Row),
    ))
}

// Execute a full-text search with optional filter and deterministic id ordering.
async fn run_fts(ds: &Dataset, query: FullTextSearchQuery, filter: Option<&str>) -> RecordBatch {
    let mut scanner = ds.scan();
    scanner.full_text_search(query).unwrap();
    if let Some(predicate) = filter {
        scanner.filter(predicate).unwrap();
    }
    scanner
        .order_by(Some(vec![ColumnOrdering::asc_nulls_first(
            "id".to_string(),
        )]))
        .unwrap();
    scanner.try_into_batch().await.unwrap()
}

// Run an FTS query and assert results match a deterministic expected batch.
async fn assert_fts_expected(
    original: &RecordBatch,
    ds: &Dataset,
    query: FullTextSearchQuery,
    filter: Option<&str>,
    expected_ids: &[i32],
) {
    let scanned = run_fts(ds, query, filter).await;
    let scanned = strip_score_column(&scanned, original.schema().as_ref());

    let indices_u32: Vec<u32> = expected_ids.iter().map(|&i| i as u32).collect();
    let indices_array = UInt32Array::from(indices_u32);
    let expected = arrow::compute::take_record_batch(original, &indices_array).unwrap();

    // Ensure ordering is deterministic (id asc) and matches the expected rows.
    assert_eq!(&expected, &scanned);
}

fn string_lists(values: &[Option<Vec<Option<&str>>>]) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for value in values {
        match value {
            Some(elements) => {
                for element in elements {
                    match element {
                        Some(element) => builder.values().append_value(element),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            None => builder.append(false),
        }
    }
    Arc::new(builder.finish())
}

fn element_hits(batch: &RecordBatch) -> Vec<(i32, Vec<u32>)> {
    let ids = batch["id"].as_primitive::<arrow_array::types::Int32Type>();
    let coordinates = batch["_doc_index"]
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let mut hits = (0..batch.num_rows())
        .map(|row| {
            let coordinate = coordinates.value(row);
            (
                ids.value(row),
                coordinate
                    .as_primitive::<arrow_array::types::UInt32Type>()
                    .values()
                    .to_vec(),
            )
        })
        .collect::<Vec<_>>();
    hits.sort_unstable();
    hits
}

fn element_scored_hits(batch: &RecordBatch) -> Vec<(i32, Vec<u32>, f32)> {
    let ids = batch["id"].as_primitive::<arrow_array::types::Int32Type>();
    let coordinates = batch["_doc_index"].as_list::<i32>();
    let scores = batch["_score"].as_primitive::<arrow_array::types::Float32Type>();
    let mut hits = (0..batch.num_rows())
        .map(|row| {
            (
                ids.value(row),
                coordinates
                    .value(row)
                    .as_primitive::<arrow_array::types::UInt32Type>()
                    .values()
                    .to_vec(),
                scores.value(row),
            )
        })
        .collect::<Vec<_>>();
    hits.sort_unstable_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    hits
}

fn assert_same_element_scores(left: &RecordBatch, right: &RecordBatch) {
    let left = element_scored_hits(left);
    let right = element_scored_hits(right);
    assert_eq!(left.len(), right.len());
    for (left, right) in left.iter().zip(&right) {
        assert_eq!((&left.0, &left.1), (&right.0, &right.1));
        assert!((left.2 - right.2).abs() < 1e-5, "{left:?} != {right:?}");
    }
}

fn assert_element_coordinates_point_to(batch: &RecordBatch, column: &str, term: &str) {
    let values = batch[column].as_list::<i32>();
    let coordinates = batch["_doc_index"].as_list::<i32>();
    for row in 0..batch.num_rows() {
        let coordinate = coordinates
            .value(row)
            .as_primitive::<arrow_array::types::UInt32Type>()
            .value(0) as usize;
        let elements = values.value(row);
        let element = elements.as_string::<i32>().value(coordinate);
        assert!(
            element.contains(term),
            "coordinate {coordinate} selected {element:?}"
        );
    }
}

fn expected_bm25_score(
    num_docs: usize,
    token_docs: usize,
    total_tokens: u32,
    doc_tokens: u32,
) -> f32 {
    let num_docs = num_docs as f32;
    let idf = ((num_docs - token_docs as f32 + 0.5) / (token_docs as f32 + 0.5) + 1.0).ln();
    let avg_doc_length = total_tokens as f32 / num_docs;
    let doc_norm = 1.2 * (1.0 - 0.75 + 0.75 * doc_tokens as f32 / avg_doc_length);
    idf * 2.2 / (1.0 + doc_norm)
}

#[tokio::test]
async fn test_row_document_raw_list_is_consistent_across_index_coverage() {
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![0])) as ArrayRef),
        ("tags", string_lists(&[Some(vec![Some("a"), Some("b")])])),
    ])
    .unwrap();
    let test_dir = tempfile::tempdir().unwrap();
    let mut ds = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
        test_dir.path().to_str().unwrap(),
        None,
    )
    .await
    .unwrap();

    ds.create_index(
        &["tags"],
        IndexType::Inverted,
        None,
        &params_for("raw", false, false),
        true,
    )
    .await
    .unwrap();

    let appended = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
        ("tags", string_lists(&[Some(vec![Some("a"), Some("b")])])),
    ])
    .unwrap();
    ds = InsertBuilder::new(Arc::new(ds))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute(vec![appended])
        .await
        .unwrap();

    let joined = run_fts(&ds, row_match("tags", "a b"), None).await;
    assert_eq!(
        joined["id"]
            .as_primitive::<arrow_array::types::Int32Type>()
            .values(),
        &[0, 1]
    );
    let element = run_fts(&ds, row_match("tags", "a"), None).await;
    assert_eq!(element.num_rows(), 0);
}

#[tokio::test]
async fn test_element_document_fts_flat_indexed_and_mixed() {
    let ids = Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5]));
    let tags = string_lists(&[
        Some(vec![
            Some("alpha beta"),
            Some("gamma alpha"),
            None,
            Some(""),
            Some("delta"),
        ]),
        Some(vec![Some("beta"), Some("gamma")]),
        None,
        Some(vec![]),
        Some(vec![None, None]),
        Some(vec![Some("!!!"), Some("epsilon")]),
    ]);
    let batch = RecordBatch::try_from_iter(vec![("id", ids as ArrayRef), ("tags", tags)]).unwrap();
    let schema = batch.schema();
    let test_dir = tempfile::tempdir().unwrap();
    let mut ds = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        test_dir.path().to_str().unwrap(),
        None,
    )
    .await
    .unwrap();

    let element_query = list_element_match("tags", "alpha");
    let element_and_query = FullTextSearchQuery::new_query(FtsQuery::Match(
        list_element_match_node("tags", "alpha beta").with_operator(Operator::And),
    ));
    let flat = run_fts(&ds, element_query.clone(), None).await;
    assert_eq!(element_hits(&flat), vec![(0, vec![0]), (0, vec![1])]);
    let flat_and = run_fts(&ds, element_and_query.clone(), None).await;
    assert_eq!(element_hits(&flat_and), vec![(0, vec![0])]);
    let flat_phrase = run_fts(&ds, list_element_phrase("tags", "alpha beta"), None).await;
    assert_eq!(element_hits(&flat_phrase), vec![(0, vec![0])]);
    let flat_cross_element_phrase =
        run_fts(&ds, list_element_phrase("tags", "beta gamma"), None).await;
    assert_eq!(flat_cross_element_phrase.num_rows(), 0);
    let row_flat_phrase = run_fts(
        &ds,
        FullTextSearchQuery::new_query(FtsQuery::Phrase(
            PhraseQuery::new("beta gamma".to_string()).with_column(Some("tags".to_string())),
        )),
        None,
    )
    .await;
    assert_eq!(
        row_flat_phrase["id"]
            .as_primitive::<arrow_array::types::Int32Type>()
            .values(),
        &[0, 1]
    );
    assert_element_coordinates_point_to(&flat, "tags", "alpha");
    let params = list_element_params(true);
    let err = ds
        .create_index(&["id"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("must resolve to Utf8"), "{err}");
    let err = ds
        .create_index(&["tags[*]"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("tags[*]"), "{err}");
    ds.create_index(&["tags"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();
    let mut element_only_auto = ds.scan();
    element_only_auto
        .full_text_search(FullTextSearchQuery::new("alpha".to_string()))
        .unwrap();
    let err = element_only_auto.try_into_batch().await.unwrap_err();
    assert!(
        err.to_string()
            .contains("unless an INVERTED index has been created"),
        "{err}"
    );
    let inferred_element = run_fts(
        &ds,
        FullTextSearchQuery::new_query(FtsQuery::Match(
            MatchQuery::new("alpha".to_string()).with_column(Some("tags".to_string())),
        )),
        None,
    )
    .await;
    assert_eq!(
        element_hits(&inferred_element),
        vec![(0, vec![0]), (0, vec![1])]
    );
    let mut mismatched_row = ds.scan();
    mismatched_row
        .full_text_search(row_match("tags", "alpha"))
        .unwrap();
    let err = mismatched_row.try_into_batch().await.unwrap_err();
    assert!(
        err.to_string().contains("requested Row") && err.to_string().contains("ListElement"),
        "{err}"
    );

    ds.create_index(
        &["tags"],
        IndexType::Inverted,
        None,
        &base_inverted_params(true),
        true,
    )
    .await
    .unwrap();
    let names = ds
        .load_indices()
        .await
        .unwrap()
        .iter()
        .map(|index| index.name.clone())
        .collect::<Vec<_>>();
    assert!(names.contains(&"tags_idx".to_string()));
    assert!(names.contains(&"tags_list_element_idx".to_string()));

    let auto_row = run_fts(&ds, FullTextSearchQuery::new("alpha".to_string()), None).await;
    assert_eq!(auto_row.num_rows(), 1);
    assert!(auto_row.column_by_name("_doc_index").is_none());
    let mut ambiguous = ds.scan();
    ambiguous
        .full_text_search(
            FullTextSearchQuery::new("alpha".to_string())
                .with_column("tags".to_string())
                .unwrap(),
        )
        .unwrap();
    let err = ambiguous.try_into_batch().await.unwrap_err();
    assert!(
        err.to_string().contains("ambiguous")
            && err.to_string().contains("specify document_granularity"),
        "{err}"
    );
    let mut row_projection = ds.scan();
    row_projection
        .full_text_search(row_match("tags", "alpha"))
        .unwrap();
    row_projection.project(&["_doc_index"]).unwrap();
    let err = row_projection.try_into_batch().await.unwrap_err();
    assert!(err.to_string().contains("_doc_index"), "{err}");

    let indexed = run_fts(&ds, element_query.clone(), None).await;
    assert_eq!(element_hits(&indexed), vec![(0, vec![0]), (0, vec![1])]);
    let indexed_and = run_fts(&ds, element_and_query, None).await;
    assert_eq!(element_hits(&indexed_and), vec![(0, vec![0])]);
    assert_element_coordinates_point_to(&indexed, "tags", "alpha");
    let element_index = ds
        .load_indices_by_name("tags_list_element_idx")
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        element_index.index_version,
        lance_index::scalar::inverted::INVERTED_INDEX_VERSION_V3 as i32
    );
    let element_index = ds
        .open_scalar_index("tags", &element_index.uuid, &NoOpMetricsCollector)
        .await
        .unwrap();
    assert_eq!(element_index.statistics().unwrap()["num_docs"], 11);
    let element_index = element_index
        .as_any()
        .downcast_ref::<lance_index::scalar::inverted::InvertedIndex>()
        .unwrap();
    let (total_tokens, num_docs, token_docs) = element_index
        .bm25_stats_for_terms(&["alpha".to_string()], None)
        .await
        .unwrap();
    assert_eq!((total_tokens, num_docs, token_docs), (8, 11, vec![2]));
    let mut tokenizer = element_index.tokenizer();
    let tokens = Arc::new(collect_query_tokens("alpha", &mut tokenizer));
    let documents = element_index
        .bm25_search_documents(
            tokens,
            Arc::new(FtsSearchParams::default()),
            Operator::Or,
            Arc::new(NoFilter),
            Arc::new(NoOpMetricsCollector),
            None,
        )
        .await
        .unwrap();
    let expected_score = expected_bm25_score(11, 2, 8, 2);
    assert!(
        documents
            .iter()
            .all(|document| (document.score.0 - expected_score).abs() < 1e-5),
        "{documents:?}"
    );
    assert_same_element_scores(&flat, &indexed);
    let indexed_phrase = run_fts(&ds, list_element_phrase("tags", "alpha beta"), None).await;
    assert_eq!(element_hits(&indexed_phrase), vec![(0, vec![0])]);
    let filtered = run_fts(&ds, element_query.clone(), Some("id = 0")).await;
    assert_eq!(element_hits(&filtered), vec![(0, vec![0]), (0, vec![1])]);
    let ordinal = run_fts(&ds, list_element_match("tags", "delta"), None).await;
    assert_eq!(element_hits(&ordinal), vec![(0, vec![4])]);
    let punctuation_gap = run_fts(&ds, list_element_match("tags", "epsilon"), None).await;
    assert_eq!(element_hits(&punctuation_gap), vec![(5, vec![1])]);
    let limited = run_fts(&ds, element_query.clone().limit(Some(1)), None).await;
    assert_eq!(limited.num_rows(), 1);
    assert_eq!(
        limited["id"]
            .as_primitive::<arrow_array::types::Int32Type>()
            .value(0),
        0
    );
    assert_eq!(element_hits(&limited).len(), 1);

    let element_match = |terms: &str| FtsQuery::Match(list_element_match_node("tags", terms));
    let boolean = BooleanQuery::new([
        (Occur::Must, element_match("alpha")),
        (Occur::Must, element_match("beta")),
    ]);
    let boolean = run_fts(
        &ds,
        FullTextSearchQuery::new_query(FtsQuery::Boolean(boolean)),
        None,
    )
    .await;
    assert_eq!(element_hits(&boolean), vec![(0, vec![0])]);

    let boost = BoostQuery::new(element_match("alpha"), element_match("gamma"), Some(0.5));
    let boost = run_fts(
        &ds,
        FullTextSearchQuery::new_query(FtsQuery::Boost(boost)),
        None,
    )
    .await;
    assert_eq!(element_hits(&boost), vec![(0, vec![0]), (0, vec![1])]);

    let phrase = PhraseQuery::new("beta gamma".to_string())
        .with_column(Some("tags".to_string()))
        .with_document_granularity(DocumentGranularity::ListElement);
    let element_phrase = run_fts(
        &ds,
        FullTextSearchQuery::new_query(FtsQuery::Phrase(phrase)),
        None,
    )
    .await;
    assert_eq!(element_phrase.num_rows(), 0);

    let row_phrase = run_fts(&ds, row_phrase("tags", "beta gamma"), None).await;
    assert_eq!(
        row_phrase["id"]
            .as_primitive::<arrow_array::types::Int32Type>()
            .values(),
        &[0, 1]
    );
    assert!(row_phrase.column_by_name("_doc_index").is_none());

    let cross_target = BooleanQuery::new([
        (Occur::Must, element_match("alpha")),
        (Occur::Must, FtsQuery::Match(row_match_node("tags", "beta"))),
    ]);
    let mut scanner = ds.scan();
    scanner
        .full_text_search(FullTextSearchQuery::new_query(FtsQuery::Boolean(
            cross_target,
        )))
        .unwrap();
    let err = scanner
        .try_into_batch()
        .await
        .expect_err("mixed document granularities must be rejected");
    assert!(
        err.to_string()
            .contains("cannot mix Row and ListElement document granularities"),
        "{err}"
    );

    let mut multi_match =
        MultiMatchQuery::try_new("alpha".to_string(), vec!["tags".to_string()]).unwrap();
    multi_match.match_queries[0].document_granularity = Some(DocumentGranularity::ListElement);
    let mut scanner = ds.scan();
    scanner
        .full_text_search(FullTextSearchQuery::new_query(FtsQuery::MultiMatch(
            multi_match,
        )))
        .unwrap();
    let err = scanner
        .try_into_batch()
        .await
        .expect_err("ListElement MultiMatch must be rejected");
    assert!(
        err.to_string()
            .contains("MultiMatch does not support ListElement document granularity"),
        "{err}"
    );

    let mut row_multi_match =
        MultiMatchQuery::try_new("beta".to_string(), vec!["tags".to_string()]).unwrap();
    row_multi_match.match_queries[0].document_granularity = Some(DocumentGranularity::Row);
    let mixed_multi_match = BooleanQuery::new([
        (Occur::Must, element_match("alpha")),
        (Occur::Must, FtsQuery::MultiMatch(row_multi_match)),
    ]);
    let mut scanner = ds.scan();
    scanner
        .full_text_search(FullTextSearchQuery::new_query(FtsQuery::Boolean(
            mixed_multi_match,
        )))
        .unwrap();
    let err = scanner
        .try_into_batch()
        .await
        .expect_err("mixed document granularities must be rejected");
    assert!(
        err.to_string()
            .contains("cannot mix Row and ListElement document granularities"),
        "{err}"
    );

    let appended = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![6])) as ArrayRef),
        (
            "tags",
            string_lists(&[Some(vec![Some("alpha beta"), Some("alpha again")])]),
        ),
    ])
    .unwrap();
    ds = InsertBuilder::new(Arc::new(ds))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute(vec![appended])
        .await
        .unwrap();

    let mixed = run_fts(&ds, element_query, None).await;
    assert_eq!(
        element_hits(&mixed),
        vec![(0, vec![0]), (0, vec![1]), (6, vec![0]), (6, vec![1])]
    );
    assert_element_coordinates_point_to(&mixed, "tags", "alpha");
    let mixed_phrase = run_fts(&ds, list_element_phrase("tags", "alpha beta"), None).await;
    assert_eq!(
        element_hits(&mixed_phrase),
        vec![(0, vec![0]), (6, vec![0])]
    );
    let all_data = ds.scan().try_into_batch().await.unwrap();
    let flat_reference_dir = tempfile::tempdir().unwrap();
    let flat_reference_ds = Dataset::write(
        RecordBatchIterator::new(vec![Ok(all_data.clone())], all_data.schema()),
        flat_reference_dir.path().to_str().unwrap(),
        None,
    )
    .await
    .unwrap();
    let flat_reference = run_fts(
        &flat_reference_ds,
        list_element_match("tags", "alpha"),
        None,
    )
    .await;
    assert_same_element_scores(&mixed, &flat_reference);
    let flat_phrase_reference = run_fts(
        &flat_reference_ds,
        list_element_phrase("tags", "alpha beta"),
        None,
    )
    .await;
    assert_same_element_scores(&mixed_phrase, &flat_phrase_reference);

    ds.optimize_indices(&OptimizeOptions::append())
        .await
        .unwrap();
    assert_eq!(
        ds.load_indices_by_name("tags_list_element_idx")
            .await
            .unwrap()
            .len(),
        2
    );
    let optimized = run_fts(&ds, list_element_match("tags", "alpha"), None).await;
    assert_same_element_scores(&optimized, &flat_reference);
    assert_element_coordinates_point_to(&optimized, "tags", "alpha");

    ds.optimize_indices(
        &OptimizeOptions::merge(2).index_names(vec!["tags_list_element_idx".to_string()]),
    )
    .await
    .unwrap();
    assert_eq!(
        ds.load_indices_by_name("tags_list_element_idx")
            .await
            .unwrap()
            .len(),
        1
    );
    let merged = run_fts(&ds, list_element_match("tags", "alpha"), None).await;
    assert_same_element_scores(&merged, &flat_reference);
    assert_element_coordinates_point_to(&merged, "tags", "alpha");

    ds.delete("id = 0").await.unwrap();
    compact_files(&mut ds, CompactionOptions::default(), None)
        .await
        .unwrap();
    let compacted = run_fts(&ds, list_element_match("tags", "alpha"), None).await;
    assert_eq!(element_hits(&compacted), vec![(6, vec![0]), (6, vec![1])]);

    ds.alter_columns(&[ColumnAlteration::new("tags".into()).rename("labels".into())])
        .await
        .unwrap();
    let renamed = run_fts(&ds, list_element_match("labels", "alpha"), None).await;
    assert_eq!(element_hits(&renamed), vec![(6, vec![0]), (6, vec![1])]);
    let mut old_name_scanner = ds.scan();
    old_name_scanner
        .full_text_search(list_element_match("tags", "alpha"))
        .unwrap();
    let err = old_name_scanner
        .try_into_batch()
        .await
        .expect_err("renamed element target must reject the old path");
    assert!(err.to_string().contains("tags"), "{err}");
}

#[tokio::test]
async fn test_element_document_persists_empty_and_zero_token_corpora() {
    async fn assert_empty_query(tags: ArrayRef, expected_documents: usize, with_position: bool) {
        let ids = Arc::new(Int32Array::from_iter_values(0..tags.len() as i32));
        let batch =
            RecordBatch::try_from_iter(vec![("id", ids as ArrayRef), ("tags", tags)]).unwrap();
        let test_dir = tempfile::tempdir().unwrap();
        let mut ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
            test_dir.path().to_str().unwrap(),
            None,
        )
        .await
        .unwrap();

        ds.create_index(
            &["tags"],
            IndexType::Inverted,
            None,
            &list_element_params(with_position),
            true,
        )
        .await
        .unwrap();

        let result = run_fts(&ds, list_element_match("tags", "alpha"), None).await;
        assert_eq!(result.num_rows(), 0);

        let metadata = ds
            .load_indices_by_name("tags_list_element_idx")
            .await
            .unwrap()
            .pop()
            .unwrap();
        let index = ds
            .open_scalar_index("tags", &metadata.uuid, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(index.statistics().unwrap()["num_docs"], expected_documents);
    }

    for with_position in [false, true] {
        assert_empty_query(string_lists(&[None, Some(vec![])]), 0, with_position).await;
        assert_empty_query(
            string_lists(&[Some(vec![None, Some(""), Some("!!!")])]),
            3,
            with_position,
        )
        .await;
    }
}

#[tokio::test]
async fn test_element_document_nested_lists_use_deepest_boundary() {
    let doc_fields = ArrowFields::from(vec![ArrowField::new("content", DataType::Utf8, true)]);
    let doc_values = StructArray::new(
        doc_fields.clone(),
        vec![Arc::new(StringArray::from(vec![
            Some("alpha"),
            Some("beta"),
            Some("gamma"),
            Some("alpha delta"),
            Some("alpha"),
        ])) as ArrayRef],
        None,
    );
    let doc_item = Arc::new(ArrowField::new("item", DataType::Struct(doc_fields), true));
    let docs_type = DataType::List(doc_item.clone());
    let docs = ListArray::new(
        doc_item,
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 4, 4, 5])),
        Arc::new(doc_values),
        None,
    );
    let group_fields = ArrowFields::from(vec![ArrowField::new("docs", docs_type, true)]);
    let group_values =
        StructArray::new(group_fields.clone(), vec![Arc::new(docs) as ArrayRef], None);
    let group_item = Arc::new(ArrowField::new(
        "item",
        DataType::Struct(group_fields),
        true,
    ));
    let groups = ListArray::new(
        group_item,
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 4])),
        Arc::new(group_values),
        None,
    );
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef),
        ("groups", Arc::new(groups) as ArrayRef),
    ])
    .unwrap();
    let test_dir = tempfile::tempdir().unwrap();
    let mut ds = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
        test_dir.path().to_str().unwrap(),
        None,
    )
    .await
    .unwrap();

    let path = "groups.docs.content";
    let expected = vec![(0, vec![0, 0]), (0, vec![1, 1]), (1, vec![1, 0])];
    let flat = run_fts(&ds, list_element_match(path, "alpha"), None).await;
    assert_eq!(element_hits(&flat), expected);

    let row_flat = run_fts(
        &ds,
        FullTextSearchQuery::new("alpha".to_string())
            .with_column(path.to_string())
            .unwrap(),
        None,
    )
    .await;
    assert_eq!(
        row_flat["id"]
            .as_primitive::<arrow_array::types::Int32Type>()
            .values(),
        &[0, 1]
    );
    assert!(row_flat.column_by_name("_doc_index").is_none());

    ds.create_index(
        &[path],
        IndexType::Inverted,
        None,
        &list_element_params(true),
        true,
    )
    .await
    .unwrap();
    ds.create_index(
        &[path],
        IndexType::Inverted,
        None,
        &base_inverted_params(true),
        true,
    )
    .await
    .unwrap();

    let indexed = run_fts(&ds, list_element_match(path, "alpha"), None).await;
    assert_eq!(element_hits(&indexed), expected);
    let indices = ds.load_indices().await.unwrap();
    let row = indices
        .iter()
        .find(|index| index.name == "groups.docs.content_idx")
        .unwrap();
    let elements = indices
        .iter()
        .find(|index| index.name == "groups.docs.content_list_element_idx")
        .unwrap();
    let groups = ds.schema().field("groups").unwrap();
    let group_item = groups.children.first().unwrap();
    let docs = group_item
        .children
        .iter()
        .find(|field| field.name == "docs")
        .unwrap();
    let doc_item = docs.children.first().unwrap();
    let content = doc_item
        .children
        .iter()
        .find(|field| field.name == "content")
        .unwrap();
    assert_eq!(row.fields, elements.fields);
    assert_eq!(row.fields, vec![content.id]);
    assert_ne!(row.fields, vec![docs.children[0].id]);
}

#[tokio::test]
async fn test_element_document_bm25_uses_element_corpus() {
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef),
        (
            "tags",
            string_lists(&[
                Some(vec![
                    Some("needle"),
                    Some("filler filler filler filler filler filler filler filler filler"),
                ]),
                Some(vec![Some("needle filler")]),
            ]),
        ),
    ])
    .unwrap();
    let schema = batch.schema();
    let test_dir = tempfile::tempdir().unwrap();
    let mut ds = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        test_dir.path().to_str().unwrap(),
        None,
    )
    .await
    .unwrap();
    let params = base_inverted_params(false);
    ds.create_index(&["tags"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();
    ds.create_index(
        &["tags"],
        IndexType::Inverted,
        None,
        &list_element_params(false),
        true,
    )
    .await
    .unwrap();

    let row = run_fts(&ds, row_match("tags", "needle"), None).await;
    let element = run_fts(&ds, list_element_match("tags", "needle"), None).await;
    let row_scores = row["_score"].as_primitive::<arrow_array::types::Float32Type>();
    let element_scores = element["_score"].as_primitive::<arrow_array::types::Float32Type>();

    let expected_row_long = expected_bm25_score(2, 2, 12, 10);
    let expected_row_short = expected_bm25_score(2, 2, 12, 2);
    let expected_element_short = expected_bm25_score(3, 2, 12, 1);
    let expected_element_long = expected_bm25_score(3, 2, 12, 2);
    assert!((row_scores.value(0) - expected_row_long).abs() < 1e-5);
    assert!((row_scores.value(1) - expected_row_short).abs() < 1e-5);
    assert!((element_scores.value(0) - expected_element_short).abs() < 1e-5);
    assert!((element_scores.value(1) - expected_element_long).abs() < 1e-5);
    assert!(row_scores.value(1) > row_scores.value(0));
    assert!(element_scores.value(0) > element_scores.value(1));
}

#[tokio::test]
async fn test_element_document_fts_vector_prefilter_deduplicates_parent_rows() {
    let vectors =
        FixedSizeListArray::try_new_from_values(Float32Array::from(vec![0.0, 0.0, 1.0, 1.0]), 2)
            .unwrap();
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef),
        (
            "tags",
            string_lists(&[
                Some(vec![Some("alpha"), Some("alpha again")]),
                Some(vec![Some("beta")]),
            ]),
        ),
        ("vector", Arc::new(vectors) as ArrayRef),
    ])
    .unwrap();
    let schema = batch.schema();
    let test_dir = tempfile::tempdir().unwrap();
    let mut ds = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        test_dir.path().to_str().unwrap(),
        None,
    )
    .await
    .unwrap();
    ds.create_index(
        &["tags"],
        IndexType::Inverted,
        None,
        &list_element_params(false),
        true,
    )
    .await
    .unwrap();

    let query_vector = Float32Array::from(vec![0.0, 0.0]);
    let mut scanner = ds.scan();
    scanner
        .nearest("vector", &query_vector, 10)
        .unwrap()
        .filter_query(QueryFilter::Fts(list_element_match("tags", "alpha")))
        .unwrap()
        .prefilter(true);
    let result = scanner.try_into_batch().await.unwrap();

    assert_eq!(
        result["id"]
            .as_primitive::<arrow_array::types::Int32Type>()
            .values(),
        &[0]
    );
    assert!(result.column_by_name("_doc_index").is_none());

    ds.create_index(
        &["tags"],
        IndexType::Inverted,
        None,
        &base_inverted_params(false),
        true,
    )
    .await
    .unwrap();
    let mut scanner = ds.scan();
    scanner
        .nearest("vector", &query_vector, 10)
        .unwrap()
        .filter_query(QueryFilter::Fts(FullTextSearchQuery::new(
            "alpha".to_string(),
        )))
        .unwrap()
        .prefilter(true);
    let result = scanner.try_into_batch().await.unwrap();
    assert_eq!(
        result["id"]
            .as_primitive::<arrow_array::types::Int32Type>()
            .values(),
        &[0]
    );
    assert!(result.column_by_name("_doc_index").is_none());
}

#[tokio::test]
// Ensure indexed and non-indexed full-text search return the same ids.
async fn test_inverted_basic_equivalence() {
    let ids = Arc::new(Int32Array::from((0..10).collect::<Vec<i32>>()));
    let text_values = vec![
        Some("hello world"),
        Some("world hello"),
        Some("hello"),
        Some("lance database"),
        Some(""),
        None,
        Some("hello lance"),
        Some("lance"),
        Some("database"),
        Some("world"),
    ];
    let text = Arc::new(StringArray::from(text_values)) as ArrayRef;
    let batch = RecordBatch::try_from_iter(vec![("id", ids as ArrayRef), ("text", text)]).unwrap();

    DatasetTestCases::from_data(batch.clone())
        .run(|ds, original| async move {
            let mut ds = ds;
            let query = FullTextSearchQuery::new("hello".to_string())
                .with_column("text".to_string())
                .unwrap();

            let expected_ids = vec![0, 1, 2, 6];
            assert_fts_expected(&original, &ds, query.clone(), None, &expected_ids).await;

            let params = base_inverted_params(false);
            ds.create_index(&["text"], IndexType::Inverted, None, &params, true)
                .await
                .unwrap();
            assert_fts_expected(&original, &ds, query.clone(), None, &expected_ids).await;
            test_fts(&original, &ds, "text", "hello", None, true, false).await;

            test_scan(&original, &ds).await;
            test_take(&original, &ds).await;
        })
        .await;
}

#[tokio::test]
// Verify phrase queries require token positions and match contiguous terms.
async fn test_inverted_phrase_query_with_positions() {
    let ids = Arc::new(Int32Array::from((0..6).collect::<Vec<i32>>()));
    let text_values = vec![
        Some("lance database"),
        Some("lance and database"),
        Some("database lance"),
        Some("lance database test"),
        Some("lance database"),
        None,
    ];
    let text = Arc::new(StringArray::from(text_values)) as ArrayRef;
    let batch = RecordBatch::try_from_iter(vec![("id", ids as ArrayRef), ("text", text)]).unwrap();

    DatasetTestCases::from_data(batch.clone())
        .run(|ds, original| async move {
            let mut ds = ds;
            let params = base_inverted_params(true);
            ds.create_index(&["text"], IndexType::Inverted, None, &params, true)
                .await
                .unwrap();

            let phrase = PhraseQuery::new("lance database".to_string())
                .with_column(Some("text".to_string()));
            let query = FullTextSearchQuery::new_query(FtsQuery::Phrase(phrase));

            assert_fts_expected(&original, &ds, query, None, &[0, 3, 4]).await;
            test_fts(&original, &ds, "text", "lance database", None, true, true).await;
        })
        .await;
}

#[tokio::test]
async fn test_segmented_inverted_match_query() {
    let test_dir = tempfile::tempdir().unwrap();
    let test_uri = test_dir.path().to_str().unwrap();

    let batches = vec![
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef),
            (
                "text",
                Arc::new(StringArray::from(vec![Some("alpha lance"), Some("beta")])) as ArrayRef,
            ),
        ])
        .unwrap(),
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![2, 3])) as ArrayRef),
            (
                "text",
                Arc::new(StringArray::from(vec![Some("lance delta"), Some("gamma")])) as ArrayRef,
            ),
        ])
        .unwrap(),
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![4, 5])) as ArrayRef),
            (
                "text",
                Arc::new(StringArray::from(vec![Some("omega"), Some("lance omega")])) as ArrayRef,
            ),
        ])
        .unwrap(),
    ];
    let schema = batches[0].schema();
    let original = arrow_select::concat::concat_batches(&schema, &batches).unwrap();

    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let mut ds = Dataset::write(
        reader,
        test_uri,
        Some(WriteParams {
            max_rows_per_file: 2,
            max_rows_per_group: 2,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let params = base_inverted_params(false);
    let fragment_ids = ds
        .get_fragments()
        .iter()
        .map(|fragment| fragment.id() as u32)
        .collect::<Vec<_>>();
    let mut metadatas = Vec::<IndexMetadata>::with_capacity(fragment_ids.len());
    for fragment_id in fragment_ids {
        let mut builder = ds
            .create_index_builder(&["text"], IndexType::Inverted, &params)
            .name("segmented_fts".to_string())
            .fragments(vec![fragment_id]);
        metadatas.push(builder.execute_uncommitted().await.unwrap());
    }
    ds.commit_existing_index_segments("segmented_fts", "text", metadatas.clone())
        .await
        .unwrap();
    assert!(metadatas.len() >= 2);
    assert_eq!(
        ds.load_indices_by_name("segmented_fts")
            .await
            .unwrap()
            .len(),
        metadatas.len()
    );

    let query = FullTextSearchQuery::new("lance".to_string())
        .with_column("text".to_string())
        .unwrap();
    assert_fts_expected(&original, &ds, query.clone(), None, &[0, 2, 5]).await;
    test_fts(&original, &ds, "text", "lance", None, true, false).await;
}

#[tokio::test]
async fn test_segmented_inverted_fuzzy_match_uses_global_idf() {
    let test_dir = tempfile::tempdir().unwrap();
    let test_uri = test_dir.path().to_str().unwrap();

    let batches = vec![
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![0])) as ArrayRef),
            (
                "text",
                Arc::new(StringArray::from(vec![Some("lance")])) as ArrayRef,
            ),
        ])
        .unwrap(),
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
            (
                "text",
                Arc::new(StringArray::from(vec![Some("lance lance lance")])) as ArrayRef,
            ),
        ])
        .unwrap(),
    ];
    let schema = batches[0].schema();
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    let mut ds = Dataset::write(
        reader,
        test_uri,
        Some(WriteParams {
            max_rows_per_file: 1,
            max_rows_per_group: 1,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let params = base_inverted_params(false);
    let fragment_ids = ds
        .get_fragments()
        .iter()
        .map(|fragment| fragment.id() as u32)
        .collect::<Vec<_>>();
    let mut metadatas = Vec::<IndexMetadata>::with_capacity(fragment_ids.len());
    for fragment_id in fragment_ids {
        let mut builder = ds
            .create_index_builder(&["text"], IndexType::Inverted, &params)
            .name("segmented_fuzzy".to_string())
            .fragments(vec![fragment_id]);
        metadatas.push(builder.execute_uncommitted().await.unwrap());
    }
    ds.commit_existing_index_segments("segmented_fuzzy", "text", metadatas)
        .await
        .unwrap();

    let batch = ds
        .scan()
        .full_text_search(
            FullTextSearchQuery::new_fuzzy("lnce".to_string(), Some(1))
                .with_column("text".to_string())
                .unwrap()
                .limit(Some(1)),
        )
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    let ids = batch["id"].as_primitive::<arrow_array::types::Int32Type>();
    assert_eq!(ids.values(), &[1]);
}

#[tokio::test]
async fn test_segmented_inverted_phrase_query() {
    let test_dir = tempfile::tempdir().unwrap();
    let test_uri = test_dir.path().to_str().unwrap();

    let batches = vec![
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef),
            (
                "text",
                Arc::new(StringArray::from(vec![
                    Some("lance database"),
                    Some("database lance"),
                ])) as ArrayRef,
            ),
        ])
        .unwrap(),
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![2, 3])) as ArrayRef),
            (
                "text",
                Arc::new(StringArray::from(vec![
                    Some("lance database query"),
                    Some("lance and database"),
                ])) as ArrayRef,
            ),
        ])
        .unwrap(),
    ];
    let schema = batches[0].schema();
    let original = arrow_select::concat::concat_batches(&schema, &batches).unwrap();

    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let mut ds = Dataset::write(
        reader,
        test_uri,
        Some(WriteParams {
            max_rows_per_file: 2,
            max_rows_per_group: 2,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let params = base_inverted_params(true);
    let fragment_ids = ds
        .get_fragments()
        .iter()
        .map(|fragment| fragment.id() as u32)
        .collect::<Vec<_>>();
    let mut metadatas = Vec::<IndexMetadata>::with_capacity(fragment_ids.len());
    for fragment_id in fragment_ids {
        let mut builder = ds
            .create_index_builder(&["text"], IndexType::Inverted, &params)
            .name("segmented_phrase_fts".to_string())
            .fragments(vec![fragment_id]);
        metadatas.push(builder.execute_uncommitted().await.unwrap());
    }
    ds.commit_existing_index_segments("segmented_phrase_fts", "text", metadatas)
        .await
        .unwrap();

    let phrase =
        PhraseQuery::new("lance database".to_string()).with_column(Some("text".to_string()));
    let query = FullTextSearchQuery::new_query(FtsQuery::Phrase(phrase));
    assert_fts_expected(&original, &ds, query, None, &[0, 2]).await;
    test_fts(&original, &ds, "text", "lance database", None, true, true).await;
}

#[tokio::test]
async fn test_segmented_inverted_match_query_with_unindexed_fragments() {
    let test_dir = tempfile::tempdir().unwrap();
    let test_uri = test_dir.path().to_str().unwrap();

    let initial_batches = vec![
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef),
            (
                "text",
                Arc::new(StringArray::from(vec![Some("lance zero"), Some("alpha")])) as ArrayRef,
            ),
        ])
        .unwrap(),
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![2, 3])) as ArrayRef),
            (
                "text",
                Arc::new(StringArray::from(vec![Some("beta"), Some("lance three")])) as ArrayRef,
            ),
        ])
        .unwrap(),
    ];
    let schema = initial_batches[0].schema();
    let reader =
        RecordBatchIterator::new(initial_batches.clone().into_iter().map(Ok), schema.clone());
    let mut ds = Dataset::write(
        reader,
        test_uri,
        Some(WriteParams {
            max_rows_per_file: 2,
            max_rows_per_group: 2,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let params = base_inverted_params(false);
    let fragment_ids = ds
        .get_fragments()
        .iter()
        .map(|fragment| fragment.id() as u32)
        .collect::<Vec<_>>();
    let mut metadatas = Vec::<IndexMetadata>::with_capacity(fragment_ids.len());
    for fragment_id in fragment_ids {
        let mut builder = ds
            .create_index_builder(&["text"], IndexType::Inverted, &params)
            .name("segmented_mixed_fts".to_string())
            .fragments(vec![fragment_id]);
        metadatas.push(builder.execute_uncommitted().await.unwrap());
    }
    ds.commit_existing_index_segments("segmented_mixed_fts", "text", metadatas)
        .await
        .unwrap();

    let appended = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![4, 5])) as ArrayRef),
        (
            "text",
            Arc::new(StringArray::from(vec![Some("lance four"), Some("omega")])) as ArrayRef,
        ),
    ])
    .unwrap();
    let appended_reader = RecordBatchIterator::new(vec![Ok(appended.clone())], appended.schema());
    ds.append(appended_reader, None).await.unwrap();

    let original = arrow_select::concat::concat_batches(
        &schema,
        &[
            initial_batches[0].clone(),
            initial_batches[1].clone(),
            appended,
        ],
    )
    .unwrap();
    let query = FullTextSearchQuery::new("lance".to_string())
        .with_column("text".to_string())
        .unwrap();
    assert_fts_expected(&original, &ds, query.clone(), None, &[0, 3, 4]).await;
    test_fts(&original, &ds, "text", "lance", None, true, false).await;
}

#[tokio::test]
// Validate filters are applied alongside inverted index search results.
async fn test_inverted_with_filter() {
    let ids = Arc::new(Int32Array::from((0..5).collect::<Vec<i32>>()));
    let text_values = vec![
        Some("lance database"),
        Some("lance vector"),
        Some("random text"),
        Some("lance"),
        None,
    ];
    let categories = vec![
        Some("keep"),
        Some("drop"),
        Some("keep"),
        Some("keep"),
        Some("keep"),
    ];
    let text = Arc::new(StringArray::from(text_values)) as ArrayRef;
    let category = Arc::new(StringArray::from(categories)) as ArrayRef;
    let batch = RecordBatch::try_from_iter(vec![
        ("id", ids as ArrayRef),
        ("text", text),
        ("category", category),
    ])
    .unwrap();

    DatasetTestCases::from_data(batch.clone())
        .with_index_types(
            "category",
            [
                None,
                Some(IndexType::Bitmap),
                Some(IndexType::BTree),
                Some(IndexType::BloomFilter),
                Some(IndexType::ZoneMap),
            ],
        )
        .run(|ds, original| async move {
            let mut ds = ds;
            let params = base_inverted_params(false);
            ds.create_index(&["text"], IndexType::Inverted, None, &params, true)
                .await
                .unwrap();

            let query = FullTextSearchQuery::new("lance".to_string())
                .with_column("text".to_string())
                .unwrap();
            assert_fts_expected(&original, &ds, query, Some("category = 'keep'"), &[0, 3]).await;
            test_fts(
                &original,
                &ds,
                "text",
                "lance",
                Some("category = 'keep'"),
                true,
                false,
            )
            .await;
        })
        .await;
}

#[tokio::test]
// Validate tokenizer/lowercase/position parameter combinations against expected matches.
async fn test_inverted_params_combinations() {
    let ids = Arc::new(Int32Array::from((0..5).collect::<Vec<i32>>()));
    let text_values = vec![
        Some("Hello there, this is a longer sentence about Lance."),
        Some("In this longer sentence we say hello to the database."),
        Some("Another line: hello world appears in a longer phrase."),
        Some("Saying HELLO loudly in a long sentence for testing."),
        None,
    ];
    let text = Arc::new(StringArray::from(text_values)) as ArrayRef;
    let batch = RecordBatch::try_from_iter(vec![("id", ids as ArrayRef), ("text", text)]).unwrap();

    let cases = vec![
        (
            "simple_lc_pos",
            params_for("simple", true, true),
            vec![0, 1, 2, 3],
            true,
        ),
        (
            "simple_no_lc",
            params_for("simple", false, false),
            vec![1, 2],
            false,
        ),
        (
            "whitespace_lc",
            params_for("whitespace", true, false),
            vec![0, 1, 2, 3],
            true,
        ),
        (
            "whitespace_no_lc_pos",
            params_for("whitespace", false, true),
            vec![1, 2],
            false,
        ),
    ];

    for (_name, params, expected, lower_case) in cases {
        let params = params.clone();
        let expected = expected.clone();
        DatasetTestCases::from_data(batch.clone())
            .with_index_types_and_inverted_index_params("text", [Some(IndexType::Inverted)], params)
            .run(|ds, original| {
                let expected = expected.clone();
                async move {
                    let query = FullTextSearchQuery::new("hello".to_string())
                        .with_column("text".to_string())
                        .unwrap();
                    assert_fts_expected(&original, &ds, query.clone(), None, &expected).await;
                    test_fts(&original, &ds, "text", "hello", None, lower_case, false).await;
                }
            })
            .await;
    }
}

/// Regression test: FTS query after deleting rows should not crash with
/// "Attempt to merge two RecordBatch with different sizes".
///
/// When stable row IDs are enabled, the FTS index may return row IDs for
/// deleted rows. The row ID index excludes deleted rows, so get_row_addrs()
/// must filter the input batch to match. Without this filtering, the
/// downstream merge in TakeExec fails with a size mismatch.
#[tokio::test]
async fn test_fts_after_delete_with_stable_row_ids() {
    let ids = Arc::new(Int32Array::from((0..20).collect::<Vec<i32>>()));
    // Give each row a unique word + a common word "shared"
    let texts: Vec<Option<&str>> = (0..20)
        .map(|i| match i % 4 {
            0 => Some("alpha shared"),
            1 => Some("beta shared"),
            2 => Some("gamma shared"),
            _ => Some("delta shared"),
        })
        .collect();
    let text_col = Arc::new(StringArray::from(texts));
    let batch = RecordBatch::try_from_iter(vec![
        ("id", ids as ArrayRef),
        ("text", text_col as ArrayRef),
    ])
    .unwrap();

    // Create dataset with stable row IDs
    let mut ds = InsertBuilder::new("memory://")
        .with_params(&WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        })
        .execute(vec![batch])
        .await
        .unwrap();

    // Create FTS index
    let params = InvertedIndexParams::default();
    ds.create_index_builder(&["text"], IndexType::Inverted, &params)
        .await
        .unwrap();

    // Delete some rows — these will still be referenced by the FTS index
    ds.delete("id IN (0, 1, 2, 3, 4)").await.unwrap();

    // FTS query for "shared" — matches ALL rows including deleted ones.
    // Before the fix, this would crash with a merge size mismatch.
    let query = FullTextSearchQuery::new("shared".to_string())
        .with_column("text".to_string())
        .unwrap();
    let mut scanner = ds.scan();
    scanner.full_text_search(query).unwrap();
    scanner
        .order_by(Some(vec![ColumnOrdering::asc_nulls_first(
            "id".to_string(),
        )]))
        .unwrap();
    let result = scanner.try_into_batch().await.unwrap();

    // Should only have 15 rows (20 - 5 deleted)
    assert_eq!(result.num_rows(), 15);

    // Verify no deleted IDs are present
    let result_ids = result
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    for id in result_ids.values().iter() {
        assert!(*id >= 5, "Deleted row id {} should not appear", id);
    }
}
