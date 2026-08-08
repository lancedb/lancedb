// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

#[tokio::test]
async fn flat_bm25_search_stream_with_metrics_records_elapsed_compute() {
    use crate::scalar::inverted::tokenizer::document_tokenizer::TextTokenizer;
    use arrow_array::{StringArray, UInt64Array};
    use lance_tokenizer::{SimpleTokenizer, TextAnalyzer};

    // Tiny stream of one batch containing the query term in two rows.
    let schema = Arc::new(Schema::new(vec![
        ROW_ID_FIELD.clone(),
        Field::new("text", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![0u64, 1, 2, 3])),
            Arc::new(StringArray::from(vec![
                "the quick brown fox",
                "lazy dog sleeps",
                "the brown fox jumps over",
                "completely unrelated text",
            ])),
        ],
    )
    .unwrap();

    let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        schema.clone(),
        stream::iter(vec![Ok(batch)]),
    ));

    let tokenizer: Box<dyn LanceTokenizer> = Box::new(TextTokenizer::new(
        TextAnalyzer::builder(SimpleTokenizer::default()).build(),
    ));

    let elapsed_compute = Time::default();
    let result_stream = flat_bm25_search_stream_with_metrics(
        input,
        "text".to_string(),
        "fox".to_string(),
        tokenizer,
        None,
        100,
        Some(elapsed_compute.clone()),
    )
    .await
    .unwrap();

    let batches: Vec<_> = result_stream.try_collect().await.unwrap();
    assert!(!batches.is_empty(), "expected at least one scored batch");

    // Both phase 1 (tokenize_and_count's spawn_cpu) and phase 2 (sync
    // scoring) call `add_duration` on the metric; verify the handle
    // was actually populated.
    assert!(
        elapsed_compute.value() > 0,
        "elapsed_compute should have been populated; got 0"
    );
}

#[tokio::test]
async fn flat_bm25_phrase_honors_positions_slop_and_repeated_terms() {
    async fn search(query: &str, slop: u32) -> Vec<u64> {
        let schema = Arc::new(Schema::new(vec![
            ROW_ID_FIELD.clone(),
            Field::new("text", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(0..5)),
                Arc::new(StringArray::from(vec![
                    "alpha beta",
                    "alpha gap beta",
                    "alpha gap gap beta",
                    "alpha alpha beta",
                    "beta alpha",
                ])),
            ],
        )
        .unwrap();
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![Ok(batch)]),
        ));
        let tokenizer: Box<dyn LanceTokenizer> = Box::new(TextTokenizer::new(
            TextAnalyzer::builder(SimpleTokenizer::default()).build(),
        ));
        let (stream, _) = flat_bm25_search_stream_with_options_and_scorer(
            input,
            "text".to_string(),
            query.to_string(),
            tokenizer,
            None,
            FlatBm25SearchOptions {
                target_batch_size: 100,
                elapsed_compute: None,
                document_granularity: DocumentGranularity::Row,
                operator: Operator::And,
                boost: 1.0,
                phrase_slop: Some(slop),
            },
        )
        .await
        .unwrap();
        stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .flat_map(|batch| batch[ROW_ID].as_primitive::<UInt64Type>().values())
            .copied()
            .collect()
    }

    assert_eq!(search("alpha beta", 0).await, vec![0, 3]);
    assert_eq!(search("alpha beta", 1).await, vec![0, 1, 3]);
    assert_eq!(search("alpha alpha", 0).await, vec![3]);
}

#[test]
fn flat_full_text_search_supports_phrase_queries() {
    let schema = Arc::new(Schema::new(vec![
        ROW_ID_FIELD.clone(),
        Field::new("text", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from_iter_values(0..4)),
            Arc::new(StringArray::from(vec![
                "alpha beta",
                "alpha gap beta",
                "alpha alpha beta",
                "beta alpha",
            ])),
        ],
    )
    .unwrap();

    assert_eq!(
        flat_full_text_search(&[&batch], "text", "\"alpha beta\"", None).unwrap(),
        vec![0, 2]
    );
    assert_eq!(
        flat_full_text_search(&[&batch], "text", "\"alpha alpha\"", None).unwrap(),
        vec![2]
    );
    assert!(!is_phrase_query("\""));
}

#[tokio::test]
async fn flat_bm25_skips_zero_token_documents_from_corpus_stats() {
    let schema = Arc::new(Schema::new(vec![
        ROW_ID_FIELD.clone(),
        Field::new("text", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![0_u64, 1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some(""),
                Some("   "),
                Some("the"),
                Some("overlength"),
                None,
                Some("hello"),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let params = InvertedIndexParams::new("whitespace".to_string(), Language::English)
        .remove_stop_words(true)
        .stem(false)
        .max_token_length(Some(6));
    let query_tokens = Arc::new(Tokens::new(vec!["hello".to_string()], DocType::Text));

    let counted_input = tokenize_and_count(
        stream::iter(vec![Ok(batch)]),
        params.build().unwrap(),
        query_tokens.clone(),
        1,
        None,
        0,
        None,
    )
    .await
    .unwrap();

    assert_eq!(counted_input.num_rows(), 1);
    assert_eq!(
        counted_input[ROW_ID].as_primitive::<UInt64Type>().values(),
        &[5]
    );
    let scorer = initialize_scorer(None, query_tokens.as_ref(), &counted_input);
    let expected_scorer = MemBM25Scorer::new(1, 1, HashMap::from([("hello".to_string(), 1)]));
    assert_eq!(scorer.total_tokens, 1);
    assert_eq!(scorer.num_docs(), 1);
    assert_eq!(scorer.num_docs_containing_token("hello"), 1);
    assert_eq!(scorer.avg_doc_length(), expected_scorer.avg_doc_length());
    assert_eq!(
        scorer.query_weight("hello"),
        expected_scorer.query_weight("hello")
    );
}

#[tokio::test]
async fn flat_bm25_preserves_zero_token_list_element_documents() {
    let coordinate_column = doc_index_storage_column(0);
    let schema = Arc::new(Schema::new(vec![
        ROW_ID_FIELD.clone(),
        Field::new(&coordinate_column, DataType::UInt32, false),
        Field::new("text", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![7_u64; 6])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0, 1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                None,
                Some(""),
                Some("   "),
                Some("the"),
                Some("overlength"),
                Some("hello"),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let params = InvertedIndexParams::new("whitespace".to_string(), Language::English)
        .remove_stop_words(true)
        .stem(false)
        .max_token_length(Some(6));
    let query_tokens = Arc::new(Tokens::new(vec!["hello".to_string()], DocType::Text));

    let counted_input = tokenize_and_count(
        stream::iter(vec![Ok(batch)]),
        params.build().unwrap(),
        query_tokens.clone(),
        2,
        None,
        1,
        None,
    )
    .await
    .unwrap();

    assert_eq!(counted_input.num_rows(), 6);
    assert_eq!(
        counted_input[&coordinate_column]
            .as_primitive::<UInt32Type>()
            .values(),
        &[0, 1, 2, 3, 4, 5]
    );
    let scorer = initialize_scorer(None, query_tokens.as_ref(), &counted_input);
    assert_eq!(scorer.total_tokens, 1);
    assert_eq!(scorer.num_docs(), 6);
    assert_eq!(scorer.num_docs_containing_token("hello"), 1);
}

#[tokio::test]
async fn flat_bm25_search_uses_full_document_length_for_normalization() {
    let schema = Arc::new(Schema::new(vec![
        ROW_ID_FIELD.clone(),
        Field::new("text", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![0u64, 1])),
            Arc::new(StringArray::from(vec![
                "alpha",
                "alpha filler filler filler filler filler filler filler filler filler",
            ])),
        ],
    )
    .unwrap();

    let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        schema.clone(),
        stream::iter(vec![Ok(batch)]),
    ));
    let tokenizer: Box<dyn LanceTokenizer> = Box::new(TextTokenizer::new(
        TextAnalyzer::builder(SimpleTokenizer::default()).build(),
    ));

    let result_stream = flat_bm25_search_stream_with_metrics(
        input,
        "text".to_string(),
        "alpha".to_string(),
        tokenizer,
        None,
        100,
        None,
    )
    .await
    .unwrap();
    let batches: Vec<_> = result_stream.try_collect().await.unwrap();
    let scored = arrow::compute::concat_batches(&FTS_SCHEMA, &batches).unwrap();
    let row_ids = scored[ROW_ID].as_primitive::<UInt64Type>();
    let scores = scored[SCORE_COL].as_primitive::<Float32Type>();

    assert_eq!(row_ids.values(), &[0, 1]);
    assert!(
        scores.value(0) > scores.value(1),
        "same term frequency should score shorter document higher; short={}, long={}",
        scores.value(0),
        scores.value(1)
    );
}

#[tokio::test]
async fn flat_bm25_search_treats_string_lists_as_row_documents() {
    let mut docs_builder = GenericListBuilder::<i32, _>::new(GenericStringBuilder::<i32>::new());
    docs_builder.values().append_value("alpha");
    docs_builder.values().append_value("alpha beta");
    docs_builder.append(true);
    docs_builder.values().append_value("beta");
    docs_builder.append(true);
    docs_builder.append(true);
    docs_builder.values().append_null();
    docs_builder.append(true);
    docs_builder.append(false);

    let docs = Arc::new(docs_builder.finish()) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        ROW_ID_FIELD.clone(),
        Field::new("text", docs.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![0u64, 1, 2, 3, 4])) as ArrayRef,
            docs,
        ],
    )
    .unwrap();

    let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        schema.clone(),
        stream::iter(vec![Ok(batch)]),
    ));
    let tokenizer: Box<dyn LanceTokenizer> = Box::new(TextTokenizer::new(
        TextAnalyzer::builder(SimpleTokenizer::default()).build(),
    ));

    let result_stream = flat_bm25_search_stream_with_metrics(
        input,
        "text".to_string(),
        "alpha".to_string(),
        tokenizer,
        None,
        100,
        None,
    )
    .await
    .unwrap();
    let batches: Vec<_> = result_stream.try_collect().await.unwrap();
    let scored = arrow::compute::concat_batches(&FTS_SCHEMA, &batches).unwrap();
    let row_ids = scored[ROW_ID].as_primitive::<UInt64Type>();

    assert_eq!(row_ids.values(), &[0]);
}

#[tokio::test]
async fn flat_bm25_search_code_and_uses_position_groups() {
    let schema = Arc::new(Schema::new(vec![
        ROW_ID_FIELD.clone(),
        Field::new("code", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![0u64, 1, 2, 3])),
            Arc::new(StringArray::from(vec![
                "get user name",
                "getUserName",
                "get user",
                "username",
            ])),
        ],
    )
    .unwrap();

    let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        schema.clone(),
        stream::iter(vec![Ok(batch)]),
    ));
    let tokenizer = InvertedIndexParams::code()
        .split_identifiers(true)
        .build()
        .unwrap();

    let result_stream = flat_bm25_search_stream_with_metrics_and_operator(
        input,
        "code".to_string(),
        "getUserName".to_string(),
        tokenizer,
        None,
        100,
        Operator::And,
        None,
    )
    .await
    .unwrap();

    let batches: Vec<_> = result_stream.try_collect().await.unwrap();
    let scored = arrow::compute::concat_batches(&FTS_SCHEMA, &batches).unwrap();
    let mut row_ids = scored[ROW_ID]
        .as_primitive::<UInt64Type>()
        .values()
        .to_vec();
    row_ids.sort_unstable();

    assert_eq!(row_ids, vec![0, 1]);
}

#[tokio::test]
async fn flat_bm25_search_code_and_counts_repeated_subwords() {
    let schema = Arc::new(Schema::new(vec![
        ROW_ID_FIELD.clone(),
        Field::new("code", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![0u64, 1])),
                Arc::new(StringArray::from(vec![
                    "pub fn edge_flat_generic_return<T>() -> Result<T, EdgeFlatError> where T: TryFrom<String> { todo!() }",
                    "pub fn edge_flat_generic_return<T>() -> Result<T> { todo!() }",
                ])),
            ],
        )
        .unwrap();

    let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        schema.clone(),
        stream::iter(vec![Ok(batch)]),
    ));
    let tokenizer = InvertedIndexParams::code().build().unwrap();

    let result_stream = flat_bm25_search_stream_with_metrics_and_operator(
        input,
        "code".to_string(),
        "edge_flat_generic_return TryFrom EdgeFlatError Result".to_string(),
        tokenizer,
        None,
        100,
        Operator::And,
        None,
    )
    .await
    .unwrap();

    let batches: Vec<_> = result_stream.try_collect().await.unwrap();
    let scored = arrow::compute::concat_batches(&FTS_SCHEMA, &batches).unwrap();
    let row_ids = scored[ROW_ID].as_primitive::<UInt64Type>().values();

    assert_eq!(row_ids, &[0]);
}
