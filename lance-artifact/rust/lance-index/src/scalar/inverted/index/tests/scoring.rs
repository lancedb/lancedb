// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

#[tokio::test]
async fn test_bm25_search_uses_global_idf() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    // Partition 0: 3 docs, only one contains "alpha".
    let mut builder0 = InnerBuilder::new(0, false, TokenSetFormat::default());
    builder0.tokens.add("alpha".to_owned());
    builder0.tokens.add("beta".to_owned());
    builder0.posting_lists.push(PostingListBuilder::new(false));
    builder0.posting_lists.push(PostingListBuilder::new(false));
    builder0.posting_lists[0].add(0, PositionRecorder::Count(1));
    builder0.posting_lists[1].add(1, PositionRecorder::Count(1));
    builder0.posting_lists[1].add(2, PositionRecorder::Count(1));
    builder0.docs.append(100, 1);
    builder0.docs.append(101, 1);
    builder0.docs.append(102, 1);
    builder0.write(store.as_ref()).await.unwrap();

    // Partition 1: 1 doc, contains "alpha".
    let mut builder1 = InnerBuilder::new(1, false, TokenSetFormat::default());
    builder1.tokens.add("alpha".to_owned());
    builder1.posting_lists.push(PostingListBuilder::new(false));
    builder1.posting_lists[0].add(0, PositionRecorder::Count(1));
    builder1.docs.append(200, 1);
    builder1.write(store.as_ref()).await.unwrap();

    let metadata = std::collections::HashMap::from_iter(vec![
        (
            "partitions".to_owned(),
            serde_json::to_string(&vec![0u64, 1u64]).unwrap(),
        ),
        (
            "params".to_owned(),
            serde_json::to_string(&InvertedIndexParams::default()).unwrap(),
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

    let tokens = Arc::new(Tokens::new(vec!["alpha".to_string()], DocType::Text));
    let params = Arc::new(FtsSearchParams::new().with_limit(Some(10)));
    let prefilter = Arc::new(NoFilter);
    let metrics = Arc::new(NoOpMetricsCollector);

    let (row_ids, scores) = index
        .bm25_search(tokens, params, Operator::Or, prefilter, metrics, None)
        .await
        .unwrap();

    assert_eq!(row_ids.len(), 2);
    assert!(row_ids.contains(&100));
    assert!(row_ids.contains(&200));
    assert_eq!(row_ids.len(), scores.len());

    let expected_idf = idf(2, 4);
    for score in scores {
        assert!(
            (score - expected_idf).abs() < 1e-6,
            "score: {}, expected: {}",
            score,
            expected_idf
        );
    }
}

async fn write_test_partition_with_optional_impacts(
    store: &Arc<LanceIndexStore>,
    partition_id: u64,
    mut builder: InnerBuilder,
    token_set_format: TokenSetFormat,
    with_impacts: bool,
) {
    let format_version = InvertedListFormatVersion::V1;
    let block_size = LEGACY_BLOCK_SIZE;
    let docs = std::mem::take(&mut builder.docs);
    let schema = inverted_list_schema_for_version_with_block_size_and_impacts(
        false,
        format_version,
        block_size,
        with_impacts,
    );

    let mut posting_writer = store
        .new_index_file(&posting_file_path(partition_id), schema.clone())
        .await
        .unwrap();
    for posting_list in std::mem::take(&mut builder.posting_lists) {
        let batch = posting_list
            .to_batch_with_docs(&docs, schema.clone())
            .unwrap();
        posting_writer.write_record_batch(batch).await.unwrap();
    }
    posting_writer.finish().await.unwrap();

    let token_batch = std::mem::take(&mut builder.tokens)
        .to_batch(token_set_format)
        .unwrap();
    let mut token_writer = store
        .new_index_file(&token_file_path(partition_id), token_batch.schema())
        .await
        .unwrap();
    token_writer.write_record_batch(token_batch).await.unwrap();
    token_writer.finish().await.unwrap();

    let doc_batch = docs.to_batch().unwrap();
    let mut doc_writer = store
        .new_index_file(&doc_file_path(partition_id), doc_batch.schema())
        .await
        .unwrap();
    doc_writer.write_record_batch(doc_batch).await.unwrap();
    doc_writer.finish().await.unwrap();
}

async fn load_global_scoring_test_index(
    first_partition_has_impacts: bool,
    second_partition_has_impacts: bool,
) -> (TempObjDir, Arc<LanceCache>, Arc<InvertedIndex>) {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));
    let partition_specs = [
        (0, 100, 5_000, 101..111, 5_000, first_partition_has_impacts),
        (1, 200, 1_000, 201..301, 1, second_partition_has_impacts),
    ];
    for (
        partition_id,
        matching_row_id,
        matching_doc_length,
        other_row_ids,
        other_doc_length,
        with_impacts,
    ) in partition_specs
    {
        let mut builder = InnerBuilder::new_with_format_version(
            partition_id,
            false,
            TokenSetFormat::default(),
            InvertedListFormatVersion::V1,
        );
        builder.tokens.add("alpha".to_owned());
        builder
            .posting_lists
            .push(PostingListBuilder::new_with_posting_tail_codec(
                false,
                InvertedListFormatVersion::V1.posting_tail_codec(),
            ));
        builder.posting_lists[0].add(0, PositionRecorder::Count(1));
        builder.docs.append(matching_row_id, matching_doc_length);
        for row_id in other_row_ids {
            builder.docs.append(row_id, other_doc_length);
        }
        write_test_partition_with_optional_impacts(
            &store,
            partition_id,
            builder,
            TokenSetFormat::default(),
            with_impacts,
        )
        .await;
    }

    write_test_metadata(&store, vec![0, 1], InvertedIndexParams::default()).await;
    let cache = Arc::new(LanceCache::with_backend(Arc::new(
        QuickCacheBackend::with_capacity(4096),
    )));
    let index = InvertedIndex::load(store, None, cache.as_ref())
        .await
        .unwrap();
    (tmpdir, cache, index)
}

#[tokio::test]
async fn test_chunked_modern_search_preserves_cold_and_prewarmed_results() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));
    let matching_partitions = 17_u64;
    for partition_id in 0..matching_partitions {
        let mut builder = InnerBuilder::new(partition_id, false, TokenSetFormat::default());
        builder.tokens.add("pipeline".to_owned());
        builder.posting_lists.push(PostingListBuilder::new(false));
        builder.posting_lists[0].add(0, PositionRecorder::Count(1));
        builder.docs.append(partition_id * 1_000 + 7, 1);
        builder.write(store.as_ref()).await.unwrap();
    }
    let unmatched_partition = matching_partitions;
    let mut builder = InnerBuilder::new(unmatched_partition, false, TokenSetFormat::default());
    builder.tokens.add("unrelated".to_owned());
    builder.posting_lists.push(PostingListBuilder::new(false));
    builder.posting_lists[0].add(0, PositionRecorder::Count(1));
    builder.docs.append(999_999, 1);
    builder.write(store.as_ref()).await.unwrap();

    write_test_metadata(
        &store,
        (0..=unmatched_partition).collect(),
        InvertedIndexParams::default(),
    )
    .await;
    let cache = Arc::new(LanceCache::with_capacity(64 * 1024 * 1024));
    let index = InvertedIndex::load(store, None, cache.as_ref())
        .await
        .unwrap();
    let tokens = Arc::new(Tokens::new(vec!["pipeline".to_owned()], DocType::Text));
    let params = Arc::new(FtsSearchParams::new().with_limit(Some(matching_partitions as usize)));

    let search = || {
        index.bm25_search(
            tokens.clone(),
            params.clone(),
            Operator::Or,
            Arc::new(NoFilter),
            Arc::new(NoOpMetricsCollector),
            None,
        )
    };
    let (mut cold_row_ids, cold_scores) = search().await.unwrap();
    cold_row_ids.sort_unstable();
    let expected = (0..matching_partitions)
        .map(|partition_id| partition_id * 1_000 + 7)
        .collect::<Vec<_>>();
    assert_eq!(cold_row_ids, expected);
    assert_eq!(cold_scores.len(), expected.len());

    index
        .prewarm_with_options(&FtsPrewarmOptions::default())
        .await
        .unwrap();
    let (mut prewarmed_row_ids, prewarmed_scores) = search().await.unwrap();
    prewarmed_row_ids.sort_unstable();
    assert_eq!(prewarmed_row_ids, expected);
    assert_eq!(prewarmed_scores, cold_scores);
}

#[tokio::test]
async fn test_prewarmed_modern_search_uses_resident_address_projection() {
    let (_tmpdir, cache, index) = load_global_scoring_test_index(true, true).await;
    let tokens = Arc::new(Tokens::new(vec!["alpha".to_owned()], DocType::Text));
    let params = Arc::new(FtsSearchParams::new().with_limit(Some(2)));

    assert!(!index.has_resident_document_projections());
    let deferred = index
        .bm25_search(
            tokens.clone(),
            params.clone(),
            Operator::Or,
            Arc::new(NoFilter),
            Arc::new(NoOpMetricsCollector),
            None,
        )
        .await
        .unwrap();
    assert!(!index.has_resident_document_projections());

    index.partitions[0]
        .docs
        .modern()
        .unwrap()
        .prewarm()
        .await
        .unwrap();
    assert!(index.partitions[0].docs.query_ready());
    assert!(!index.has_resident_document_projections());
    let partially_resident = index
        .bm25_search(
            tokens.clone(),
            params.clone(),
            Operator::Or,
            Arc::new(NoFilter),
            Arc::new(NoOpMetricsCollector),
            None,
        )
        .await
        .unwrap();
    assert_eq!(partially_resident, deferred);

    let prewarm_options = FtsPrewarmOptions::default();
    futures::future::join_all((0..8).map(|_| index.prewarm_with_options(&prewarm_options)))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert!(index.document_projections_resident.load(Ordering::Acquire));
    assert!(index.has_resident_document_projections());
    assert!(index.corpus_stats.initialized());
    assert!(index.partitions.iter().all(|partition| {
        partition.docs.query_ready() && partition.inverted_list.modern_posting_validation_ready()
    }));
    assert!(index.prewarm_state.lock().await.satisfies(false));

    let resident = index
        .bm25_search(
            tokens.clone(),
            params.clone(),
            Operator::Or,
            Arc::new(NoFilter),
            Arc::new(NoOpMetricsCollector),
            None,
        )
        .await
        .unwrap();
    assert_eq!(resident, deferred);
    assert_eq!(resident.0.len(), 2);
    assert!(resident.0.contains(&100));
    assert!(resident.0.contains(&200));

    cache.clear().await;
    assert!(index.document_projections_resident.load(Ordering::Acquire));
    assert_eq!(cache.size().await, 0);
    let resident_address_owners = index
        .partitions
        .iter()
        .map(|partition| {
            partition
                .docs
                .modern()
                .unwrap()
                .address_buffer_handle()
                .strong_count()
        })
        .collect::<Vec<_>>();
    assert_eq!(resident_address_owners, vec![0, 0]);
    assert!(
        index
            .partitions
            .iter()
            .all(|partition| { !partition.docs.modern().unwrap().projection_resident() })
    );

    let after_eviction = index
        .bm25_search(
            tokens.clone(),
            params.clone(),
            Operator::Or,
            Arc::new(NoFilter),
            Arc::new(NoOpMetricsCollector),
            None,
        )
        .await
        .unwrap();
    assert_eq!(after_eviction, deferred);
    assert!(!index.document_projections_resident.load(Ordering::Acquire));

    cache.clear().await;
    index.prewarm_with_options(&prewarm_options).await.unwrap();
    assert!(index.document_projections_resident_now());
    assert!(index.document_projections_resident.load(Ordering::Acquire));

    let re_prewarms_after_eviction = index
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
    assert_eq!(re_prewarms_after_eviction, deferred);
}

#[tokio::test]
async fn test_resident_modern_search_loads_partition_stats_without_global_stats() {
    let (_tmpdir, _cache, index) = load_global_scoring_test_index(true, false).await;
    assert!(index.corpus_stats.get().is_none());
    assert!(
        index
            .partitions
            .iter()
            .all(|partition| partition.docs.cached_stats().is_none())
    );

    for partition in &index.partitions {
        partition
            .docs
            .modern()
            .unwrap()
            .address_projection()
            .await
            .unwrap();
    }
    assert!(index.has_resident_document_projections());

    let scorer = MemBM25Scorer::new(56_100, 112, HashMap::from([("alpha".to_owned(), 2)]));
    let result = index
        .bm25_search(
            Arc::new(Tokens::new(vec!["alpha".to_owned()], DocType::Text)),
            Arc::new(FtsSearchParams::new().with_limit(Some(2))),
            Operator::Or,
            Arc::new(NoFilter),
            Arc::new(NoOpMetricsCollector),
            Some(&scorer),
        )
        .await
        .unwrap();

    assert_eq!(result.0.len(), 2);
    assert!(result.0.contains(&100));
    assert!(result.0.contains(&200));
    assert!(index.corpus_stats.get().is_none());
    assert!(
        index
            .partitions
            .iter()
            .all(|partition| partition.docs.cached_stats().is_some())
    );
}

async fn search_test_impact_partition(
    partition: &InvertedPartition,
    tokens: &Tokens,
    params: &FtsSearchParams,
    scorer: Arc<MemBM25Scorer>,
    shared_threshold: Arc<AtomicU32>,
) -> Vec<DocCandidate<DocId>> {
    let LoadedPostings {
        postings,
        grouped_expansions,
        impact_safe,
        exact_scoring_required,
    } = partition
        .load_posting_lists(
            tokens,
            params,
            Operator::Or,
            scorer.as_ref(),
            &NoOpMetricsCollector,
            false,
        )
        .await
        .unwrap();
    assert!(impact_safe);
    assert!(!exact_scoring_required);
    assert!(grouped_expansions.is_empty());

    let documents = partition.docs.modern().unwrap();
    let lengths = documents.lengths().await.unwrap();
    let visibility = documents.visibility(NoFilter.mask(), false).await.unwrap();
    partition
        .bm25_search_modern(
            lengths.as_ref(),
            &visibility,
            params,
            Operator::Or,
            postings,
            Some(scorer),
            &NoOpMetricsCollector,
            shared_threshold,
        )
        .unwrap()
}

#[tokio::test]
async fn test_impact_partitions_share_global_threshold_without_pruning_winner() {
    // Partition 0 wins under its local corpus statistics but loses under
    // the global statistics. If its local score escapes into the shared
    // floor, partition 1 will incorrectly prune the real global winner.
    let (_tmpdir, _cache, index) = load_global_scoring_test_index(true, true).await;
    let first_partition = index
        .partitions
        .iter()
        .find(|partition| partition.id() == 0)
        .unwrap();
    let second_partition = index
        .partitions
        .iter()
        .find(|partition| partition.id() == 1)
        .unwrap();

    let tokens = Arc::new(Tokens::new(vec!["alpha".to_owned()], DocType::Text));
    let params = Arc::new(FtsSearchParams::new().with_limit(Some(1)));
    let scorer = Arc::new(
        index
            .bm25_base_scorer(tokens.as_ref(), params.as_ref(), None)
            .await
            .unwrap(),
    );
    first_partition
        .inverted_list
        .ensure_metadata_loaded()
        .await
        .unwrap();
    second_partition
        .inverted_list
        .ensure_metadata_loaded()
        .await
        .unwrap();
    let first_local_scorer = IndexBM25Scorer::new(std::iter::once(first_partition.as_ref()));
    let second_local_scorer = IndexBM25Scorer::new(std::iter::once(second_partition.as_ref()));
    let first_local_score =
        first_local_scorer.query_weight("alpha") * first_local_scorer.doc_weight(1, 5_000);
    let second_local_score =
        second_local_scorer.query_weight("alpha") * second_local_scorer.doc_weight(1, 1_000);
    assert!(first_local_score > second_local_score);
    let shared_threshold = Arc::new(AtomicU32::new(f32::NEG_INFINITY.to_bits()));

    // Search sequentially so partition 0 deterministically publishes its
    // score before partition 1 evaluates its impact upper bound.
    let first_candidates = search_test_impact_partition(
        first_partition,
        tokens.as_ref(),
        params.as_ref(),
        scorer.clone(),
        shared_threshold.clone(),
    )
    .await;
    assert_eq!(first_candidates.len(), 1);
    assert_eq!(first_candidates[0].document, DocId::new(0));
    let first_score =
        scorer.query_weight("alpha") * scorer.doc_weight(1, first_candidates[0].doc_length);
    let published_threshold = f32::from_bits(shared_threshold.load(Ordering::Relaxed));
    assert!(
        (published_threshold - first_score).abs() < 1e-6,
        "published threshold: {published_threshold}, expected global score: {first_score}"
    );

    let second_candidates = search_test_impact_partition(
        second_partition,
        tokens.as_ref(),
        params.as_ref(),
        scorer.clone(),
        shared_threshold.clone(),
    )
    .await;
    assert_eq!(second_candidates.len(), 1);
    assert_eq!(second_candidates[0].document, DocId::new(0));
    let second_score =
        scorer.query_weight("alpha") * scorer.doc_weight(1, second_candidates[0].doc_length);
    assert!(
        second_score > first_score,
        "second score: {second_score}, first score: {first_score}"
    );
    assert!((f32::from_bits(shared_threshold.load(Ordering::Relaxed)) - second_score).abs() < 1e-6);

    let (row_ids, scores) = index
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
    assert_eq!(row_ids, vec![200]);
    assert_eq!(scores.len(), 1);
    assert!((scores[0] - second_score).abs() < 1e-6);
}

#[tokio::test]
async fn test_mixed_impact_and_legacy_partitions_use_global_final_scores() {
    let (_tmpdir, _cache, index) = load_global_scoring_test_index(true, false).await;

    let impact_partition = index
        .partitions
        .iter()
        .find(|partition| partition.id() == 0)
        .unwrap();
    let legacy_partition = index
        .partitions
        .iter()
        .find(|partition| partition.id() == 1)
        .unwrap();

    let impact_posting = impact_partition
        .inverted_list
        .posting_list(0, false, &NoOpMetricsCollector)
        .await
        .unwrap();
    assert!(impact_posting.has_impacts());

    let legacy_posting = legacy_partition
        .inverted_list
        .posting_list(0, false, &NoOpMetricsCollector)
        .await
        .unwrap();
    assert!(!legacy_posting.has_impacts());

    let tokens = Arc::new(Tokens::new(vec!["alpha".to_string()], DocType::Text));
    let params = Arc::new(FtsSearchParams::new().with_limit(Some(1)));
    let (row_ids, scores) = index
        .bm25_search(
            tokens.clone(),
            params.clone(),
            Operator::Or,
            Arc::new(NoFilter),
            Arc::new(NoOpMetricsCollector),
            None,
        )
        .await
        .unwrap();

    assert_eq!(row_ids, vec![200]);
    assert_eq!(row_ids.len(), scores.len());

    let scorer = index
        .bm25_base_scorer(tokens.as_ref(), params.as_ref(), None)
        .await
        .unwrap();
    let expected_score = scorer.query_weight("alpha") * scorer.doc_weight(1, 1_000);
    assert!(
        (scores[0] - expected_score).abs() < 1e-6,
        "score: {}, expected: {}",
        scores[0],
        expected_score
    );
}

#[tokio::test]
async fn test_two_legacy_partitions_keep_private_thresholds() {
    // Legacy BM25 scores use partition-local statistics, so sharing one
    // pruning floor across partitions can discard the global winner.
    let (_tmpdir, _cache, index) = load_global_scoring_test_index(false, false).await;
    for partition in index.partitions.iter() {
        let posting = partition
            .inverted_list
            .posting_list(0, false, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert!(!posting.has_impacts());
    }

    let tokens = Arc::new(Tokens::new(vec!["alpha".to_string()], DocType::Text));
    let params = Arc::new(FtsSearchParams::new().with_limit(Some(1)));
    let (row_ids, scores) = index
        .bm25_search(
            tokens.clone(),
            params.clone(),
            Operator::Or,
            Arc::new(NoFilter),
            Arc::new(NoOpMetricsCollector),
            None,
        )
        .await
        .unwrap();

    assert_eq!(row_ids, vec![200]);
    assert_eq!(scores.len(), 1);
    let scorer = index
        .bm25_base_scorer(tokens.as_ref(), params.as_ref(), None)
        .await
        .unwrap();
    let expected_score = scorer.query_weight("alpha") * scorer.doc_weight(1, 1_000);
    assert!(
        (scores[0] - expected_score).abs() < 1e-6,
        "score: {}, expected global score: {}",
        scores[0],
        expected_score
    );
}

#[tokio::test]
async fn test_and_query_returns_empty_when_exact_term_missing() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    builder.tokens.add("alpha".to_owned());
    builder.posting_lists.push(PostingListBuilder::new(false));
    builder.posting_lists[0].add(0, PositionRecorder::Count(1));
    builder.docs.append(100, 1);
    builder.write(store.as_ref()).await.unwrap();

    write_test_metadata(&store, vec![0], InvertedIndexParams::default()).await;
    let cache = Arc::new(LanceCache::with_capacity(4096));
    let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
        .await
        .unwrap();

    let tokens = Arc::new(Tokens::new(
        vec!["alpha".to_owned(), "missing".to_owned()],
        DocType::Text,
    ));
    let params = Arc::new(FtsSearchParams::new().with_limit(Some(10)));
    let prefilter = Arc::new(NoFilter);
    let metrics = Arc::new(NoOpMetricsCollector);

    let (and_row_ids, _) = index
        .bm25_search(
            tokens.clone(),
            params.clone(),
            Operator::And,
            prefilter.clone(),
            metrics.clone(),
            None,
        )
        .await
        .unwrap();
    assert!(
        and_row_ids.is_empty(),
        "AND must not match when any required term is missing"
    );

    let (or_row_ids, _) = index
        .bm25_search(tokens, params, Operator::Or, prefilter, metrics, None)
        .await
        .unwrap();
    assert_eq!(
        or_row_ids,
        vec![100],
        "OR should still match the present term"
    );
}

#[tokio::test]
async fn test_and_query_accepts_same_position_alternatives() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    for token in ["getusername", "get", "user", "name"] {
        builder.tokens.add(token.to_owned());
        builder.posting_lists.push(PostingListBuilder::new(false));
    }
    // Doc 0 only has the split words. Doc 1 has both the complete
    // identifier and split words. A grouped AND query should accept either
    // `getusername` or `get` at position 0.
    builder.posting_lists[1].add(0, PositionRecorder::Count(1));
    builder.posting_lists[2].add(0, PositionRecorder::Count(1));
    builder.posting_lists[3].add(0, PositionRecorder::Count(1));
    builder.docs.append(100, 3);

    builder.posting_lists[0].add(1, PositionRecorder::Count(1));
    builder.posting_lists[1].add(1, PositionRecorder::Count(1));
    builder.posting_lists[2].add(1, PositionRecorder::Count(1));
    builder.posting_lists[3].add(1, PositionRecorder::Count(1));
    builder.docs.append(101, 4);
    builder.write(store.as_ref()).await.unwrap();

    write_test_metadata(&store, vec![0], InvertedIndexParams::code()).await;
    let index = InvertedIndex::load(store.clone(), None, &LanceCache::no_cache())
        .await
        .unwrap();

    let tokens = Arc::new(Tokens::with_positions(
        vec![
            "getusername".to_string(),
            "get".to_string(),
            "user".to_string(),
            "name".to_string(),
        ],
        vec![0, 0, 1, 2],
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
    assert_eq!(row_ids, vec![100, 101]);
}

#[tokio::test]
async fn test_phrase_query_accepts_same_position_alternatives() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let mut builder = InnerBuilder::new(0, true, TokenSetFormat::default());
    for token in ["getusername", "get", "user", "name"] {
        builder.tokens.add(token.to_owned());
        builder.posting_lists.push(PostingListBuilder::new(true));
    }
    // Doc 0 only has split words. Doc 1 has both the complete identifier
    // and split words at the same position. Doc 2 has the terms but not as
    // an exact phrase.
    builder.posting_lists[1].add(0, PositionRecorder::Position(vec![0].into()));
    builder.posting_lists[2].add(0, PositionRecorder::Position(vec![1].into()));
    builder.posting_lists[3].add(0, PositionRecorder::Position(vec![2].into()));
    builder.docs.append(100, 3);

    builder.posting_lists[0].add(1, PositionRecorder::Position(vec![0].into()));
    builder.posting_lists[1].add(1, PositionRecorder::Position(vec![0].into()));
    builder.posting_lists[2].add(1, PositionRecorder::Position(vec![1].into()));
    builder.posting_lists[3].add(1, PositionRecorder::Position(vec![2].into()));
    builder.docs.append(101, 3);

    builder.posting_lists[0].add(2, PositionRecorder::Position(vec![0].into()));
    builder.posting_lists[2].add(2, PositionRecorder::Position(vec![2].into()));
    builder.posting_lists[3].add(2, PositionRecorder::Position(vec![3].into()));
    builder.docs.append(102, 3);

    builder.write(store.as_ref()).await.unwrap();

    write_test_metadata(
        &store,
        vec![0],
        InvertedIndexParams::code().with_position(true),
    )
    .await;
    let index = InvertedIndex::load(store.clone(), None, &LanceCache::no_cache())
        .await
        .unwrap();

    let tokens = Arc::new(Tokens::with_positions(
        vec![
            "getusername".to_string(),
            "get".to_string(),
            "user".to_string(),
            "name".to_string(),
        ],
        vec![0, 0, 1, 2],
        DocType::Text,
    ));
    let params = Arc::new(
        FtsSearchParams::new()
            .with_limit(Some(10))
            .with_phrase_slop(Some(0)),
    );
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
    assert_eq!(row_ids, vec![100, 101]);
}
