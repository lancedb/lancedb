// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

// Enough distinct tokens that `write_posting_lists` emits several posting-list
// batches (the default batch size is 256 rows), exercising the restructured
// producer and async send path.
const MANY_BATCH_TOKENS: u64 = 1000;
const MANY_BATCH_ROW_ID_BASE: u64 = 1000;

// Writes a single partition whose posting lists span many output batches. Each
// token `tok{i:05}` maps to row id `MANY_BATCH_ROW_ID_BASE + i`.
async fn write_partition_spanning_many_batches(store: &dyn IndexStore) {
    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    for i in 0..MANY_BATCH_TOKENS {
        // Zero-padded so tokens are inserted in sorted order, as the set expects.
        builder.tokens.add(format!("tok{i:05}"));
        let doc_id = builder.docs.append(MANY_BATCH_ROW_ID_BASE + i, 1);
        let mut posting_list = PostingListBuilder::new(false);
        posting_list.add(doc_id, PositionRecorder::Count(1));
        builder.posting_lists.push(posting_list);
    }
    builder
        .write(store)
        .await
        .expect("writing posting lists should succeed");
}

// Correctness guard for the restructured posting-list writer. The producer now
// builds each batch in its own `spawn_cpu` call, handing the builder and the
// remaining posting lists back out so state (the cross-batch cache-group
// accumulator) is preserved, and dispatches with an async `send().await`. This
// verifies that path over many batches by checking representative tokens after
// they cross the producer/consumer boundary.
//
// Note: this does not reproduce the single-thread-pool deadlock the async send
// fixes -- that requires a 1-thread CPU pool (a process-global singleton) plus
// ~8MB of buffered posting data to trigger a consumer-side encoder flush, which
// is impractical as a lightweight unit test.
#[tokio::test]
async fn test_write_many_posting_list_batches_preserves_all_batches() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    write_partition_spanning_many_batches(store.as_ref()).await;

    write_test_metadata(&store, vec![0], InvertedIndexParams::default()).await;
    let cache = Arc::new(LanceCache::with_capacity(4096));
    let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
        .await
        .unwrap();

    // Probe tokens from the first, a middle, and the last batch to confirm they
    // remain queryable after crossing the producer/consumer boundary.
    for token_idx in [0u64, MANY_BATCH_TOKENS / 2, MANY_BATCH_TOKENS - 1] {
        let tokens = Arc::new(Tokens::new(
            vec![format!("tok{token_idx:05}")],
            DocType::Text,
        ));
        let params = Arc::new(FtsSearchParams::new().with_limit(Some(10)));
        let (row_ids, _) = index
            .bm25_search(
                tokens,
                params,
                Operator::Or,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            row_ids,
            vec![MANY_BATCH_ROW_ID_BASE + token_idx],
            "token tok{token_idx:05} should map to its single document"
        );
    }
}

#[tokio::test]
async fn test_and_query_skips_partition_missing_required_term() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let mut builder0 = InnerBuilder::new(0, false, TokenSetFormat::default());
    builder0.tokens.add("alpha".to_owned());
    builder0.posting_lists.push(PostingListBuilder::new(false));
    builder0.posting_lists[0].add(0, PositionRecorder::Count(1));
    builder0.docs.append(100, 1);
    builder0.write(store.as_ref()).await.unwrap();

    let mut builder1 = InnerBuilder::new(1, false, TokenSetFormat::default());
    builder1.tokens.add("alpha".to_owned());
    builder1.tokens.add("beta".to_owned());
    builder1.posting_lists.push(PostingListBuilder::new(false));
    builder1.posting_lists.push(PostingListBuilder::new(false));
    builder1.posting_lists[0].add(0, PositionRecorder::Count(1));
    builder1.posting_lists[1].add(0, PositionRecorder::Count(1));
    builder1.docs.append(200, 2);
    builder1.write(store.as_ref()).await.unwrap();

    write_test_metadata(&store, vec![0, 1], InvertedIndexParams::default()).await;
    let cache = Arc::new(LanceCache::with_capacity(4096));
    let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
        .await
        .unwrap();

    let tokens = Arc::new(Tokens::new(
        vec!["alpha".to_owned(), "beta".to_owned()],
        DocType::Text,
    ));
    let params = Arc::new(FtsSearchParams::new().with_limit(Some(10)));
    let (mut row_ids, _) = index
        .bm25_search(
            tokens,
            params,
            Operator::And,
            Arc::new(NoFilter),
            Arc::new(NoOpMetricsCollector),
            None,
        )
        .await
        .unwrap();
    row_ids.sort_unstable();
    assert_eq!(
        row_ids,
        vec![200],
        "partition missing beta must not contribute alpha-only hits"
    );
}

#[tokio::test]
async fn test_fuzzy_and_groups_expansions_by_original_position() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    builder.tokens.add("alpha".to_owned());
    builder.tokens.add("alphi".to_owned());
    builder.tokens.add("beta".to_owned());
    builder.posting_lists.push(PostingListBuilder::new(false));
    builder.posting_lists.push(PostingListBuilder::new(false));
    builder.posting_lists.push(PostingListBuilder::new(false));
    builder.posting_lists[0].add(0, PositionRecorder::Count(1));
    builder.posting_lists[1].add(1, PositionRecorder::Count(1));
    builder.posting_lists[2].add(0, PositionRecorder::Count(1));
    builder.posting_lists[2].add(1, PositionRecorder::Count(1));
    builder.docs.append(100, 2);
    builder.docs.append(101, 2);
    builder.write(store.as_ref()).await.unwrap();

    write_test_metadata(&store, vec![0], InvertedIndexParams::default()).await;
    let cache = Arc::new(LanceCache::with_capacity(4096));
    let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
        .await
        .unwrap();
    let params = Arc::new(
        FtsSearchParams::new()
            .with_limit(Some(10))
            .with_fuzziness(Some(1)),
    );

    let missing_position_tokens = Arc::new(Tokens::new(
        vec!["betx".to_owned(), "zzzzz".to_owned()],
        DocType::Text,
    ));
    let (missing_and_row_ids, _) = index
        .bm25_search(
            missing_position_tokens.clone(),
            params.clone(),
            Operator::And,
            Arc::new(NoFilter),
            Arc::new(NoOpMetricsCollector),
            None,
        )
        .await
        .unwrap();
    assert!(
        missing_and_row_ids.is_empty(),
        "fuzzy AND must require at least one expansion for every original position"
    );

    let (mut or_row_ids, _) = index
        .bm25_search(
            missing_position_tokens,
            params.clone(),
            Operator::Or,
            Arc::new(NoFilter),
            Arc::new(NoOpMetricsCollector),
            None,
        )
        .await
        .unwrap();
    or_row_ids.sort_unstable();
    assert_eq!(
        or_row_ids,
        vec![100, 101],
        "OR should still match present fuzzy expansions"
    );

    let grouped_tokens = Arc::new(Tokens::new(
        vec!["alphx".to_owned(), "betx".to_owned()],
        DocType::Text,
    ));
    let (mut grouped_row_ids, _) = index
        .bm25_search(
            grouped_tokens,
            params,
            Operator::And,
            Arc::new(NoFilter),
            Arc::new(NoOpMetricsCollector),
            None,
        )
        .await
        .unwrap();
    grouped_row_ids.sort_unstable();
    assert_eq!(
        grouped_row_ids,
        vec![100, 101],
        "each original fuzzy position should match any one of its expansions"
    );
}

#[tokio::test]
async fn test_fuzzy_expansion_cap_applies_to_whole_query() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    for token in ["alpha", "alphi", "beta", "beti"] {
        builder.tokens.add(token.to_owned());
        builder.posting_lists.push(PostingListBuilder::new(false));
    }
    for token_id in 0..4 {
        builder.posting_lists[token_id].add(token_id as u32, PositionRecorder::Count(1));
        builder.docs.append(100 + token_id as u64, 1);
    }
    builder.write(store.as_ref()).await.unwrap();

    write_test_metadata(&store, vec![0], InvertedIndexParams::default()).await;
    let cache = Arc::new(LanceCache::with_capacity(4096));
    let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
        .await
        .unwrap();
    let partition = index.partitions[0].clone();
    let params = FtsSearchParams::new()
        .with_fuzziness(Some(1))
        .with_max_expansions(3);
    let tokens = Tokens::new(vec!["alphx".to_owned(), "betx".to_owned()], DocType::Text);

    let expanded = partition.expand_fuzzy(&tokens, &params).unwrap();
    let expanded_terms = (0..expanded.len())
        .map(|idx| (expanded.get_token(idx).to_owned(), expanded.position(idx)))
        .collect::<Vec<_>>();

    assert_eq!(
        expanded_terms,
        vec![
            ("alpha".to_owned(), 0),
            ("alphi".to_owned(), 0),
            ("beta".to_owned(), 1),
        ],
        "max_expansions should cap the whole fuzzy query, not each token"
    );
}

/// Write one partition holding `variants` in order, with one
/// single-token doc per variant taken from `row_ids`.
async fn write_variant_partition(
    store: &Arc<LanceIndexStore>,
    partition_id: u64,
    variants: &[&str],
    row_ids: &[u64],
) {
    let mut builder = InnerBuilder::new(partition_id, false, TokenSetFormat::default());
    for token in variants {
        builder.tokens.add((*token).to_owned());
        builder.posting_lists.push(PostingListBuilder::new(false));
    }
    for (local_idx, row_id) in row_ids.iter().enumerate() {
        builder.posting_lists[local_idx].add(local_idx as u32, PositionRecorder::Count(1));
        builder.docs.append(*row_id, 1);
    }
    builder.write(store.as_ref()).await.unwrap();
}

#[tokio::test]
async fn test_fuzzy_expansion_cap_is_global_across_partitions() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    write_variant_partition(&store, 0, &["alpha", "alphb"], &[100, 101]).await;
    write_variant_partition(&store, 1, &["alphc", "alphd"], &[102, 103]).await;
    write_test_metadata(&store, vec![0, 1], InvertedIndexParams::default()).await;
    let cache = Arc::new(LanceCache::with_capacity(4096));
    let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
        .await
        .unwrap();

    let params = FtsSearchParams::new()
        .with_fuzziness(Some(1))
        .with_max_expansions(3);
    let tokens = Tokens::new(vec!["alphx".to_owned()], DocType::Text);

    let expanded = index.expand_fuzzy_tokens(&tokens, &params).unwrap();
    let expanded_terms = (0..expanded.len())
        .map(|idx| expanded.get_token(idx).to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        expanded_terms,
        vec!["alpha".to_owned(), "alphb".to_owned(), "alphc".to_owned()],
        "max_expansions must cap the whole query across partitions, \
             in lexicographic order"
    );
}

#[tokio::test]
async fn test_fuzzy_results_independent_of_partition_shape() {
    // The same four single-variant docs, laid out as one partition and
    // as two. With a binding max_expansions the two shapes must still
    // match the same documents with the same scores.
    let single_dir = TempObjDir::default();
    let single_store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        single_dir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));
    write_variant_partition(
        &single_store,
        0,
        &["alpha", "alphb", "alphc", "alphd"],
        &[100, 101, 102, 103],
    )
    .await;
    write_test_metadata(&single_store, vec![0], InvertedIndexParams::default()).await;

    let split_dir = TempObjDir::default();
    let split_store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        split_dir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));
    write_variant_partition(&split_store, 0, &["alpha", "alphb"], &[100, 101]).await;
    write_variant_partition(&split_store, 1, &["alphc", "alphd"], &[102, 103]).await;
    write_test_metadata(&split_store, vec![0, 1], InvertedIndexParams::default()).await;

    let params = Arc::new(
        FtsSearchParams::new()
            .with_limit(Some(10))
            .with_fuzziness(Some(1))
            .with_max_expansions(3),
    );

    let mut results = Vec::new();
    for store in [single_store, split_store] {
        let cache = LanceCache::with_capacity(4096);
        let index = InvertedIndex::load(store, None, &cache).await.unwrap();
        let tokens = Arc::new(Tokens::new(vec!["alphx".to_owned()], DocType::Text));
        let (row_ids, scores) = index
            .bm25_search(
                tokens,
                params.clone(),
                Operator::Or,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();
        let mut scored = row_ids.into_iter().zip(scores).collect::<Vec<_>>();
        scored.sort_unstable_by_key(|(row_id, _)| *row_id);
        results.push(scored);
    }

    assert_eq!(
        results[0]
            .iter()
            .map(|(row_id, _)| *row_id)
            .collect::<Vec<_>>(),
        vec![100, 101, 102],
        "a binding cap keeps the three lexicographically smallest variants"
    );
    assert_eq!(
        results[0], results[1],
        "fuzzy results must not depend on the partition shape"
    );
}

#[tokio::test]
async fn test_fuzzy_and_scores_grouped_expansions_by_matched_token() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    builder.tokens.add("alpha".to_owned());
    builder.tokens.add("alphi".to_owned());
    builder.tokens.add("beta".to_owned());
    builder.posting_lists.push(PostingListBuilder::new(false));
    builder.posting_lists.push(PostingListBuilder::new(false));
    builder.posting_lists.push(PostingListBuilder::new(false));
    builder.posting_lists[0].add(0, PositionRecorder::Count(1));
    builder.posting_lists[0].add(2, PositionRecorder::Count(1));
    builder.posting_lists[0].add(3, PositionRecorder::Count(1));
    builder.posting_lists[0].add(4, PositionRecorder::Count(1));
    builder.posting_lists[0].add(5, PositionRecorder::Count(1));
    builder.posting_lists[1].add(1, PositionRecorder::Count(1));
    builder.posting_lists[2].add(0, PositionRecorder::Count(1));
    builder.posting_lists[2].add(1, PositionRecorder::Count(1));
    builder.docs.append(100, 2);
    builder.docs.append(101, 2);
    builder.docs.append(102, 1);
    builder.docs.append(103, 1);
    builder.docs.append(104, 1);
    builder.docs.append(105, 1);
    builder.write(store.as_ref()).await.unwrap();

    write_test_metadata(&store, vec![0], InvertedIndexParams::default()).await;
    let cache = Arc::new(LanceCache::with_capacity(4096));
    let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
        .await
        .unwrap();

    let tokens = Arc::new(Tokens::new(
        vec!["alphx".to_owned(), "betx".to_owned()],
        DocType::Text,
    ));
    let params = Arc::new(
        FtsSearchParams::new()
            .with_limit(Some(1))
            .with_fuzziness(Some(1)),
    );
    let (row_ids, _scores) = index
        .bm25_search(
            tokens,
            params,
            Operator::And,
            Arc::new(NoFilter),
            Arc::new(NoOpMetricsCollector),
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        row_ids,
        vec![101],
        "the rare matched expansion should outrank the common expansion"
    );
}

#[rstest::rstest]
#[case::and(Operator::And)]
#[case::or(Operator::Or)]
#[tokio::test]
async fn test_grouped_scoring_keeps_exact_winner_outside_proxy_window(#[case] operator: Operator) {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    builder.tokens.add("common".to_owned());
    builder.tokens.add("rare".to_owned());
    builder.posting_lists.push(PostingListBuilder::new(false));
    builder.posting_lists.push(PostingListBuilder::new(false));
    for doc_id in 0..3 {
        builder.posting_lists[0].add(doc_id, PositionRecorder::Count(1));
        builder.docs.append(100 + doc_id as u64, 1);
    }
    builder.posting_lists[1].add(3, PositionRecorder::Count(1));
    builder.docs.append(103, 2);
    builder.write(store.as_ref()).await.unwrap();

    write_test_metadata(&store, vec![0], InvertedIndexParams::default()).await;
    let cache = Arc::new(LanceCache::with_capacity(4096));
    let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
        .await
        .unwrap();

    let tokens = Arc::new(Tokens::with_positions(
        vec!["common".to_owned(), "rare".to_owned()],
        vec![0, 0],
        DocType::Text,
    ));
    let params = Arc::new(FtsSearchParams::new().with_limit(Some(1)));
    let (row_ids, _scores) = index
        .bm25_search(
            tokens,
            params,
            operator,
            Arc::new(NoFilter),
            Arc::new(NoOpMetricsCollector),
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        row_ids,
        vec![103],
        "the rare term's exact IDF must win even when proxy scoring ranks it outside the old candidate cushion"
    );
}

#[tokio::test]
async fn test_fuzzy_and_grouped_rescore_keeps_wand_limit_bounded() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let num_docs = BLOCK_SIZE * 2 + 4;
    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    builder.tokens.add("alpha".to_owned());
    builder.tokens.add("alphi".to_owned());
    builder.tokens.add("beta".to_owned());
    builder.posting_lists.push(PostingListBuilder::new(false));
    builder.posting_lists.push(PostingListBuilder::new(false));
    builder.posting_lists.push(PostingListBuilder::new(false));

    builder.posting_lists[0].add(0, PositionRecorder::Count(1));
    builder.posting_lists[1].add(1, PositionRecorder::Count(1));
    for doc_id in 0..num_docs {
        builder.posting_lists[2].add(doc_id as u32, PositionRecorder::Count(1));
        if doc_id >= 2 {
            builder.posting_lists[0].add(doc_id as u32, PositionRecorder::Count(1));
        }
        let num_tokens = if doc_id < 2 { 2 } else { 100 };
        builder.docs.append(100 + doc_id as u64, num_tokens);
    }
    builder.write(store.as_ref()).await.unwrap();

    write_test_metadata(&store, vec![0], InvertedIndexParams::default()).await;
    let cache = Arc::new(LanceCache::with_capacity(4096));
    let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
        .await
        .unwrap();

    let tokens = Arc::new(Tokens::new(
        vec!["alphx".to_owned(), "betx".to_owned()],
        DocType::Text,
    ));
    let params = Arc::new(
        FtsSearchParams::new()
            .with_limit(Some(1))
            .with_fuzziness(Some(1)),
    );
    let metrics = Arc::new(LocalMetricsCollector::default());
    let (row_ids, _scores) = index
        .bm25_search(
            tokens,
            params,
            Operator::And,
            Arc::new(NoFilter),
            metrics.clone(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        row_ids,
        vec![101],
        "final rescoring should still rank by the matched expansion"
    );
    let comparisons = metrics.comparisons.load(Ordering::Relaxed);
    assert!(
        comparisons < num_docs,
        "grouped fuzzy AND should not clear the WAND top-k bound and scan every candidate; comparisons={comparisons}, num_docs={num_docs}"
    );
}

#[tokio::test]
async fn test_phrase_query_reads_legacy_per_doc_positions() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let mut builder = InnerBuilder::new_with_format_version(
        0,
        true,
        TokenSetFormat::default(),
        InvertedListFormatVersion::V1,
    );
    builder.tokens.add("hello".to_owned());
    builder.tokens.add("world".to_owned());
    builder
        .posting_lists
        .push(PostingListBuilder::new_with_posting_tail_codec(
            true,
            PostingTailCodec::Fixed32,
        ));
    builder
        .posting_lists
        .push(PostingListBuilder::new_with_posting_tail_codec(
            true,
            PostingTailCodec::Fixed32,
        ));
    builder.posting_lists[0].add(0, PositionRecorder::Position(vec![0].into()));
    builder.posting_lists[1].add(0, PositionRecorder::Position(vec![1].into()));
    builder.posting_lists[0].add(1, PositionRecorder::Position(vec![0].into()));
    builder.posting_lists[1].add(1, PositionRecorder::Position(vec![2].into()));
    builder.docs.append(100, 2);
    builder.docs.append(101, 2);
    builder.write(store.as_ref()).await.unwrap();

    let metadata = std::collections::HashMap::from_iter(vec![
        (
            "partitions".to_owned(),
            serde_json::to_string(&vec![0_u64]).unwrap(),
        ),
        (
            "params".to_owned(),
            serde_json::to_string(&InvertedIndexParams::default().with_position(true)).unwrap(),
        ),
        (
            TOKEN_SET_FORMAT_KEY.to_owned(),
            TokenSetFormat::default().to_string(),
        ),
    ]);
    let mut writer = store
        .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
        .await
        .unwrap();
    writer.finish_with_metadata(metadata).await.unwrap();

    let cache = Arc::new(LanceCache::with_capacity(4096));
    let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
        .await
        .unwrap();

    let tokens = Arc::new(Tokens::new(
        vec!["hello".to_owned(), "world".to_owned()],
        DocType::Text,
    ));
    let params = Arc::new(
        FtsSearchParams::new()
            .with_limit(Some(10))
            .with_phrase_slop(Some(0)),
    );
    let prefilter = Arc::new(NoFilter);
    let metrics = Arc::new(NoOpMetricsCollector);

    let (row_ids, _scores) = index
        .bm25_search(tokens, params, Operator::And, prefilter, metrics, None)
        .await
        .unwrap();

    assert_eq!(row_ids, vec![100]);
}
