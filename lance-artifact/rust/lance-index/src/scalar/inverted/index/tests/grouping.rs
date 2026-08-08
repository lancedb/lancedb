// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

fn posting_entries(posting: &PostingList) -> Vec<(u64, u32)> {
    posting.iter().map(|(doc, freq, _)| (doc, freq)).collect()
}

#[tokio::test]
async fn test_modern_posting_validation_is_cached_per_token() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    builder.tokens.add("term".to_owned());
    let mut valid_builder = PostingListBuilder::new(false);
    valid_builder.add(0, PositionRecorder::Count(1));
    builder.posting_lists.push(valid_builder);
    builder.docs.append(1000, 1);
    builder.write(store.as_ref()).await.unwrap();

    let reader = store.open_index_file(&posting_file_path(0)).await.unwrap();
    let mut posting_reader = PostingListReader::try_new(reader, &LanceCache::no_cache())
        .await
        .unwrap();
    posting_reader.modern_num_docs = Some(1);
    let validation = &posting_reader
        .modern_doc_id_validations
        .as_ref()
        .expect("modern readers have per-token validation state")[0];
    assert!(validation.get().is_none());
    assert!(!posting_reader.modern_posting_is_validated(0).unwrap());

    let mut corrupt_builder = PostingListBuilder::new(false);
    corrupt_builder.add(1, PositionRecorder::Count(1));
    let corrupt_batch = corrupt_builder.to_batch(vec![1.0]).unwrap();
    let corrupt_posting = PostingList::from_batch(&corrupt_batch, Some(1.0), Some(1)).unwrap();
    let error = posting_reader
        .ensure_modern_posting_validated(0, &corrupt_posting)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Index { .. }));
    assert!(error.to_string().contains("DocId 1"));
    assert!(error.to_string().contains("[0, 1)"));
    assert!(validation.get().is_none());
    assert!(!posting_reader.modern_posting_is_validated(0).unwrap());

    let first = posting_reader
        .posting_list(0, false, &NoOpMetricsCollector)
        .await
        .unwrap();
    assert_eq!(posting_entries(&first), vec![(0, 1)]);
    assert!(validation.get().is_some());
    assert!(posting_reader.modern_posting_is_validated(0).unwrap());

    let second = posting_reader
        .posting_list(0, false, &NoOpMetricsCollector)
        .await
        .unwrap();
    assert_eq!(posting_entries(&second), vec![(0, 1)]);
    assert!(validation.get().is_some());
}

/// Runtime synthetic grouping must return correct posting lists for every
/// token, including across synthetic group boundaries.
#[tokio::test]
async fn test_posting_list_synthetic_grouping_reads_group_boundaries() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let num_tokens = runtime_posting_group_tokens() as u32 + 4;
    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    for t in 0..num_tokens {
        builder.tokens.add(format!("t{t}"));
        let mut pl = PostingListBuilder::new(false);
        pl.add(t, PositionRecorder::Count(1));
        builder.posting_lists.push(pl);
        builder.docs.append(1000 + t as u64, 1);
    }
    builder.write(store.as_ref()).await.unwrap();

    let reader = store.open_index_file(&posting_file_path(0)).await.unwrap();
    let cache = LanceCache::no_cache();
    let mut posting_reader = PostingListReader::try_new(reader, &cache).await.unwrap();
    posting_reader.modern_num_docs = Some(num_tokens as usize);
    assert!(
        matches!(
            &posting_reader.grouping,
            PostingGrouping::SyntheticFixed { .. }
        ),
        "v2 reader must synthesize runtime posting groups",
    );

    let metrics = NoOpMetricsCollector;
    for token in 0..num_tokens {
        let posting = posting_reader
            .posting_list(token, false, &metrics)
            .await
            .unwrap();
        assert_eq!(
            posting_entries(&posting),
            vec![(token as u64, 1)],
            "synthetic grouping mismatch for token {token}",
        );
        assert_eq!(posting.len(), 1, "length mismatch for token {token}");
    }
}

/// Prewarm must populate exactly the `PostingListGroupKey`s the read path
/// looks up — in particular the final group, whose `end` both paths derive
/// from `self.len()`. If those derivations drifted (e.g. one used
/// `num_rows()` and the other the loaded posting count), the last group's
/// warm entry would be missing and prewarm silently wasted (issue #7040).
#[tokio::test]
async fn test_prewarm_group_keys_match_read_path() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let num_tokens = runtime_posting_group_tokens() as u32 + 4;
    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    for t in 0..num_tokens {
        builder.tokens.add(format!("t{t}"));
        let mut pl = PostingListBuilder::new(false);
        pl.add(t, PositionRecorder::Count(1));
        builder.posting_lists.push(pl);
        builder.docs.append(1000 + t as u64, 1);
    }
    builder.write(store.as_ref()).await.unwrap();

    let reader = store.open_index_file(&posting_file_path(0)).await.unwrap();
    // A real (strong) cache must outlive the reader's weak handle so the
    // prewarmed entries are still resolvable below.
    let cache = LanceCache::with_capacity(1 << 20);
    let mut posting_reader = PostingListReader::try_new(reader, &cache).await.unwrap();
    posting_reader.modern_num_docs = Some(num_tokens as usize);
    assert!(
        matches!(
            &posting_reader.grouping,
            PostingGrouping::SyntheticFixed { .. }
        ),
        "v2 reader should use runtime synthetic groups",
    );

    posting_reader
        .prewarm_posting_lists(false, 2)
        .await
        .unwrap();
    assert!(posting_reader.modern_posting_validation_ready());
    assert!(
        posting_reader
            .modern_postings_validated
            .load(Ordering::Acquire)
    );

    for token in 0..num_tokens {
        let (start, end) = posting_reader.group_range_for_token(token).unwrap();
        assert!(
            posting_reader
                .index_cache
                .get_with_key(&posting_list_group_cache_key(
                    start,
                    end,
                    posting_reader.has_impacts,
                ))
                .await
                .is_some(),
            "prewarm did not populate group [{start}, {end}) that the read \
                 path requests for token {token}",
        );
    }

    let (_, last_end) = posting_reader
        .group_range_for_token(num_tokens - 1)
        .unwrap();
    assert_eq!(
        last_end, num_tokens,
        "the last group must end at the posting count ({num_tokens})",
    );
}

/// An empty partition has no synthetic groups because there are no token
/// rows to cache.
#[tokio::test]
async fn test_empty_partition_has_no_synthetic_groups() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    builder.write(store.as_ref()).await.unwrap();

    let reader = store.open_index_file(&posting_file_path(0)).await.unwrap();
    let posting_reader = PostingListReader::try_new(reader, &LanceCache::no_cache())
        .await
        .unwrap();
    assert!(
        matches!(&posting_reader.grouping, PostingGrouping::None),
        "reader for an empty partition must not create cache groups",
    );
    assert!(posting_reader.is_empty());
}

/// A large posting list can share a runtime synthetic group with neighbors;
/// grouping is token-count based and should still read every member intact.
#[tokio::test]
async fn test_large_posting_reads_inside_synthetic_group() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    let big_docs = (BLOCK_SIZE * 3 + 5) as u32;
    builder.tokens.add("big".to_owned());
    let mut big = PostingListBuilder::new(false);
    for d in 0..big_docs {
        big.add(d, PositionRecorder::Count(1));
    }
    builder.posting_lists.push(big);
    for t in 1..5u32 {
        builder.tokens.add(format!("t{t}"));
        let mut pl = PostingListBuilder::new(false);
        pl.add(0, PositionRecorder::Count(1));
        builder.posting_lists.push(pl);
    }
    for d in 0..big_docs as u64 {
        builder.docs.append(1000 + d, 1);
    }
    builder.write(store.as_ref()).await.unwrap();

    let reader = store.open_index_file(&posting_file_path(0)).await.unwrap();
    let posting_reader = PostingListReader::try_new(reader, &LanceCache::no_cache())
        .await
        .unwrap();
    let expected_end = runtime_posting_group_tokens().min(5) as u32;

    assert_eq!(
        posting_reader.group_range_for_token(0),
        Some((0, expected_end)),
        "runtime synthetic grouping should group by token count, not posting bytes",
    );
    let big = posting_reader
        .posting_list(0, false, &NoOpMetricsCollector)
        .await
        .unwrap();
    assert_eq!(big.len(), big_docs as usize);
    // A trailing tiny term (in the next, multi-token group) still reads back.
    let tiny = posting_reader
        .posting_list(2, false, &NoOpMetricsCollector)
        .await
        .unwrap();
    assert_eq!(tiny.len(), 1);
}

/// Non-empty v2 indexes should prewarm synthetic `PostingListGroupKey`
/// entries, matching what the read path then looks up without persisted
/// grouping metadata.
#[tokio::test]
async fn test_prewarm_synthetic_grouping_populates_group_entries() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let num_tokens = 3u32;
    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    for t in 0..num_tokens {
        builder.tokens.add(format!("t{t}"));
        let mut pl = PostingListBuilder::new(false);
        pl.add(t, PositionRecorder::Count(1));
        builder.posting_lists.push(pl);
        builder.docs.append(1000 + t as u64, 1);
    }
    builder.write(store.as_ref()).await.unwrap();

    let reader = store.open_index_file(&posting_file_path(0)).await.unwrap();
    let cache = LanceCache::with_capacity(1 << 20);
    let posting_reader = PostingListReader::try_new(reader, &cache).await.unwrap();
    assert!(matches!(
        &posting_reader.grouping,
        PostingGrouping::SyntheticFixed { .. }
    ));

    posting_reader
        .prewarm_posting_lists(false, 2)
        .await
        .unwrap();

    for token_id in 0..num_tokens {
        let (start, end) = posting_reader.group_range_for_token(token_id).unwrap();
        let group = posting_reader
            .index_cache
            .get_with_key(&posting_list_group_cache_key(
                start,
                end,
                posting_reader.has_impacts,
            ))
            .await
            .unwrap_or_else(|| {
                panic!(
                    "synthetic prewarm should populate group [{start}, {end}) for token {token_id}"
                )
            });
        assert!(
            group.is_packed(),
            "no-position synthetic prewarm should insert a packed group"
        );
        assert!(
            posting_reader
                .index_cache
                .get_with_key(&posting_list_cache_key(
                    token_id,
                    posting_reader.has_impacts,
                ))
                .await
                .is_none(),
            "synthetic prewarm should not populate per-token entry {token_id}",
        );
    }
}

/// End-to-end BM25 search over a grouped multi-group index must return the
/// correct documents, and a warm-cache query must match the cold-cache
/// result exactly (issue #7040).
#[tokio::test]
async fn test_grouped_bm25_search_correct_and_cache_stable() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    // Rare tokens (one doc each) plus one common token in every doc. The
    // token count exceeds the runtime group size so scoring must index
    // into the right synthetic group slot.
    let num_rare = runtime_posting_group_tokens() as u32 + 2;
    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    for t in 0..num_rare {
        builder.tokens.add(format!("t{t}"));
        builder.posting_lists.push(PostingListBuilder::new(false));
    }
    let common_id = builder.tokens.add("common".to_owned());
    builder.posting_lists.push(PostingListBuilder::new(false));
    for d in 0..num_rare {
        builder.posting_lists[d as usize].add(d, PositionRecorder::Count(1));
        builder.posting_lists[common_id as usize].add(d, PositionRecorder::Count(1));
        builder.docs.append(1000 + d as u64, 2);
    }
    builder.write(store.as_ref()).await.unwrap();

    let metadata = HashMap::from([
        (
            "partitions".to_owned(),
            serde_json::to_string(&vec![0u64]).unwrap(),
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

    let cache = Arc::new(LanceCache::with_capacity(1 << 20));
    let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
        .await
        .unwrap();

    // A rare token in the middle of a group must resolve to its one doc.
    let query = |term: &str| {
        let index = index.clone();
        let term = term.to_string();
        async move {
            index
                .bm25_search(
                    Arc::new(Tokens::new(vec![term], DocType::Text)),
                    Arc::new(FtsSearchParams::new().with_limit(Some(num_rare as usize))),
                    Operator::Or,
                    Arc::new(NoFilter),
                    Arc::new(NoOpMetricsCollector),
                    None,
                )
                .await
                .unwrap()
        }
    };

    let rare_query_id = num_rare / 2;
    let (rare_rows, _) = query(&format!("t{rare_query_id}")).await;
    assert_eq!(
        rare_rows,
        vec![1000 + rare_query_id as u64],
        "rare token must map to its single doc",
    );

    // Cold vs warm cache must agree for the common (large) token.
    let (cold_rows, cold_scores) = query("common").await;
    let (warm_rows, warm_scores) = query("common").await;
    assert_eq!(cold_rows.len(), num_rare as usize);
    assert_eq!(cold_rows, warm_rows, "warm-cache rows must match cold");
    assert_eq!(
        cold_scores, warm_scores,
        "warm-cache scores must match cold"
    );
}

#[tokio::test]
async fn flat_bm25_search_stop_word_query_over_unindexed_rows_returns_empty() {
    let schema = Arc::new(Schema::new(vec![
        ROW_ID_FIELD.clone(),
        Field::new("text", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![0u64, 1, 2])),
            Arc::new(StringArray::from(vec![
                "the quick brown fox",
                "a lazy dog",
                "for the win",
            ])),
        ],
    )
    .unwrap();

    let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        schema.clone(),
        stream::iter(vec![Ok(batch)]),
    ));

    // Analyzer with an English stop-word filter, so the query "the"
    // tokenizes to zero terms -- exactly the production trigger.
    let tokenizer: Box<dyn LanceTokenizer> = Box::new(TextTokenizer::new(
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(StopWordFilter::new(Language::English).unwrap())
            .build(),
    ));

    let result_stream = flat_bm25_search_stream_with_metrics(
        input,
        "text".to_string(),
        "the".to_string(),
        tokenizer,
        None,
        100,
        None,
    )
    .await
    .unwrap();

    let batches: Vec<_> = result_stream.try_collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 0,
        "a stop-word-only query has no searchable terms and must match nothing"
    );
}
