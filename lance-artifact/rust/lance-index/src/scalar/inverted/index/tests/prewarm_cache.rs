// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

#[tokio::test]
async fn test_posting_cache_conflict_across_partitions() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    // Create first partition with one token and posting list length 1
    let mut builder1 = InnerBuilder::new(0, false, TokenSetFormat::default());
    builder1.tokens.add("test".to_owned());
    builder1.posting_lists.push(PostingListBuilder::new(false));
    builder1.posting_lists[0].add(0, PositionRecorder::Count(1));
    builder1.docs.append(100, 1); // row_id=100, num_tokens=1
    builder1.write(store.as_ref()).await.unwrap();

    // Create second partition with one token and posting list length 4
    let mut builder2 = InnerBuilder::new(1, false, TokenSetFormat::default());
    builder2.tokens.add("test".to_owned()); // Use same token to test cache prefix fix
    builder2.posting_lists.push(PostingListBuilder::new(false));
    builder2.posting_lists[0].add(0, PositionRecorder::Count(2));
    builder2.posting_lists[0].add(1, PositionRecorder::Count(1));
    builder2.posting_lists[0].add(2, PositionRecorder::Count(3));
    builder2.posting_lists[0].add(3, PositionRecorder::Count(1));
    builder2.docs.append(200, 2); // row_id=200, num_tokens=2
    builder2.docs.append(201, 1); // row_id=201, num_tokens=1
    builder2.docs.append(202, 3); // row_id=202, num_tokens=3
    builder2.docs.append(203, 1); // row_id=203, num_tokens=1
    builder2.write(store.as_ref()).await.unwrap();

    // Create metadata file with both partitions
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

    // Load the inverted index
    let cache = Arc::new(LanceCache::with_capacity(4096));
    let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
        .await
        .unwrap();

    // Verify the index structure
    assert_eq!(index.partitions.len(), 2);
    assert_eq!(index.partitions[0].tokens.len(), 1);
    assert_eq!(index.partitions[1].tokens.len(), 1);

    // Verify the partitions were loaded correctly

    // Verify posting list lengths (note: partition order may differ from creation order).
    // `posting_len_for_token` works for both legacy and v2 layouts without
    // forcing the V2-only bulk metadata load.
    let pl_0_0 = index.partitions[0]
        .inverted_list
        .posting_len_for_token(0, None)
        .await
        .unwrap();
    let pl_1_0 = index.partitions[1]
        .inverted_list
        .posting_len_for_token(0, None)
        .await
        .unwrap();
    if index.partitions[0].id() == 0 {
        assert_eq!(pl_0_0, 1);
        assert_eq!(pl_1_0, 4);
        assert_eq!(index.partitions[0].docs.len(), 1);
        assert_eq!(index.partitions[1].docs.len(), 4);
    } else {
        assert_eq!(pl_0_0, 4);
        assert_eq!(pl_1_0, 1);
        assert_eq!(index.partitions[0].docs.len(), 4);
        assert_eq!(index.partitions[1].docs.len(), 1);
    }

    // Prewarm the inverted index (this loads posting lists into cache)
    index.prewarm().await.unwrap();

    let tokens = Arc::new(Tokens::new(vec!["test".to_string()], DocType::Text));
    let params = Arc::new(FtsSearchParams::new().with_limit(Some(10)));
    let prefilter = Arc::new(NoFilter);
    let metrics = Arc::new(NoOpMetricsCollector);

    let (row_ids, scores) = index
        .bm25_search(tokens, params, Operator::Or, prefilter, metrics, None)
        .await
        .unwrap();

    // Verify that we got search results
    // Expected to find 5 documents: 1 from first partition, 4 from second partition
    assert_eq!(row_ids.len(), 5, "row_ids: {:?}", row_ids);
    assert!(!row_ids.is_empty(), "Should find at least some documents");
    assert_eq!(row_ids.len(), scores.len());

    // All scores should be positive since all documents contain the search token
    for &score in &scores {
        assert!(score > 0.0, "All scores should be positive");
    }

    // Check that we got results from both partitions
    assert!(
        row_ids.contains(&100),
        "Should contain row_id from partition 0"
    );
    assert!(
        row_ids.iter().any(|&id| id >= 200),
        "Should contain row_id from partition 1"
    );
}

#[tokio::test]
async fn test_modern_prewarm_packs_group_with_shared_posting_buffer() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    builder.tokens.add("alpha".to_owned());
    builder.tokens.add("beta".to_owned());
    builder.posting_lists.push(PostingListBuilder::new(false));
    builder.posting_lists.push(PostingListBuilder::new(false));
    builder.posting_lists[0].add(0, PositionRecorder::Count(1));
    builder.posting_lists[0].add(1, PositionRecorder::Count(2));
    builder.posting_lists[1].add(2, PositionRecorder::Count(3));
    builder.posting_lists[1].add(3, PositionRecorder::Count(4));
    builder.docs.append(100, 1);
    builder.docs.append(101, 2);
    builder.docs.append(102, 3);
    builder.docs.append(103, 4);
    builder.write(store.as_ref()).await.unwrap();

    let metadata = std::collections::HashMap::from_iter(vec![
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

    let cache = Arc::new(LanceCache::with_capacity(4096));
    let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
        .await
        .unwrap();
    let inverted_list = &index.partitions[0].inverted_list;
    assert!(
        !inverted_list.is_legacy_layout(),
        "test should use modern posting layout"
    );
    assert!(
        inverted_list.has_impacts,
        "modern posting fixture should include impact skip data"
    );

    inverted_list.prewarm_posting_lists(false, 2).await.unwrap();

    // The two tiny tokens land in a single cache group [0, 2) (issue
    // #7040); both postings are read out of that group entry.
    let (start, end) = inverted_list.group_range_for_token(0).unwrap();
    let group = inverted_list
        .index_cache
        .get_with_key(&posting_list_group_cache_key(
            start,
            end,
            inverted_list.has_impacts,
        ))
        .await
        .unwrap();

    assert!(
        group.is_packed(),
        "no-position prewarm should pack v2 groups"
    );
    assert!(
        group.needs_external_metadata(),
        "prewarmed packed groups must not duplicate reader score/length metadata"
    );
    let (alpha_score, alpha_len) = inverted_list.bulk_metadata_for_token(0);
    let PostingList::Compressed(alpha) = group
        .posting_list(0, alpha_score, alpha_len)
        .unwrap()
        .unwrap()
    else {
        panic!("expected compressed posting list for token 0");
    };
    let PostingList::Compressed(alpha_again) = group
        .posting_list(0, alpha_score, alpha_len)
        .unwrap()
        .unwrap()
    else {
        panic!("expected compressed posting list for repeated token 0 access");
    };
    let (beta_score, beta_len) = inverted_list.bulk_metadata_for_token(1);
    let PostingList::Compressed(beta) = group
        .posting_list(1, beta_score, beta_len)
        .unwrap()
        .unwrap()
    else {
        panic!("expected compressed posting list for token 1");
    };

    assert!(
        alpha.impacts.is_some() && beta.impacts.is_some(),
        "packed prewarm must preserve impact skip data"
    );
    assert!(
        alpha
            .impacts
            .as_ref()
            .unwrap()
            .shares_derived_state_with(alpha_again.impacts.as_ref().unwrap()),
        "repeated packed slot access must share decoded impact state"
    );
    assert!(
        alpha.shares_first_docs_with(&alpha_again),
        "repeated packed slot access must share decoded block heads"
    );
    assert_eq!(
        alpha.block_first_docs().as_ptr(),
        alpha_again.block_first_docs().as_ptr(),
        "packed block heads should be decoded only once per slot"
    );
    assert_eq!(
        alpha.blocks.values().as_ptr(),
        beta.blocks.values().as_ptr(),
        "packed posting views should share the group's values buffer"
    );
}

#[tokio::test]
async fn test_packed_prewarm_groups_do_not_retain_the_full_chunk() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    for token_id in 0..4u32 {
        builder.tokens.add(format!("t{token_id}"));
        let mut posting = PostingListBuilder::new(false);
        posting.add(token_id, PositionRecorder::Count(1));
        builder.posting_lists.push(posting);
        builder.docs.append(1000 + token_id as u64, 1);
    }
    builder.write(store.as_ref()).await.unwrap();

    let reader = store.open_index_file(&posting_file_path(0)).await.unwrap();
    let cache = LanceCache::with_capacity(1 << 20);
    let mut posting_reader = PostingListReader::try_new(reader, &cache).await.unwrap();
    posting_reader.grouping = PostingGrouping::SyntheticFixed { group_size: 2 };

    assert_eq!(
        posting_reader
            .prewarm_posting_lists_chunked(false, Some(4), 1)
            .await
            .unwrap(),
        1,
        "the test must read both groups in one prewarm chunk"
    );

    let first_group = posting_reader
        .index_cache
        .get_with_key(&posting_list_group_cache_key(
            0,
            2,
            posting_reader.has_impacts,
        ))
        .await
        .unwrap();
    let second_group = posting_reader
        .index_cache
        .get_with_key(&posting_list_group_cache_key(
            2,
            4,
            posting_reader.has_impacts,
        ))
        .await
        .unwrap();
    let (first_score, first_len) = posting_reader.bulk_metadata_for_token(0);
    let PostingList::Compressed(first) = first_group
        .posting_list(0, first_score, first_len)
        .unwrap()
        .unwrap()
    else {
        panic!("expected compressed posting list in first group");
    };
    let (neighbor_score, neighbor_len) = posting_reader.bulk_metadata_for_token(1);
    let PostingList::Compressed(first_neighbor) = first_group
        .posting_list(1, neighbor_score, neighbor_len)
        .unwrap()
        .unwrap()
    else {
        panic!("expected compressed posting list in first group");
    };
    let (second_score, second_len) = posting_reader.bulk_metadata_for_token(2);
    let PostingList::Compressed(second) = second_group
        .posting_list(0, second_score, second_len)
        .unwrap()
        .unwrap()
    else {
        panic!("expected compressed posting list in second group");
    };

    assert_eq!(
        first.blocks.values().as_ptr(),
        first_neighbor.blocks.values().as_ptr(),
        "postings in one group should share the group's values buffer"
    );
    assert_ne!(
        first.blocks.values().as_ptr(),
        second.blocks.values().as_ptr(),
        "each group must own a compact buffer instead of retaining the full chunk"
    );
}

#[test]
fn test_prewarm_chunk_ranges_preserve_group_boundaries() {
    let grouping = PostingGrouping::SyntheticFixed { group_size: 4 };
    assert_eq!(
        prewarm_chunk_ranges(&grouping, 13, 5),
        vec![(0, 4), (4, 8), (8, 13)],
        "grouped chunks may contain multiple groups but must never split one"
    );
    assert_eq!(
        prewarm_chunk_ranges(&PostingGrouping::None, 13, 5),
        vec![(0, 5), (5, 10), (10, 13)],
        "ungrouped chunk ranges should use plain token ranges"
    );
}

#[test]
fn test_synthetic_grouping_preserves_fixed_boundaries() {
    let grouping = PostingGrouping::SyntheticFixed { group_size: 4 };
    assert_eq!(
        grouping.range_for_token(5, 10),
        Some((4, 8)),
        "synthetic token groups should be fixed-size ranges"
    );
    assert_eq!(
        grouping.range_for_token(9, 10),
        Some((8, 10)),
        "the final synthetic group should end at token_count"
    );
    assert_eq!(
        prewarm_chunk_ranges(&grouping, 10, 6),
        vec![(0, 4), (4, 10)],
        "prewarm chunks may contain multiple synthetic groups but must not split one"
    );
    assert_eq!(
        grouping.ranges_for_chunk(4, 10, 10),
        vec![(4, 8), (8, 10)],
        "publish selection should enumerate synthetic groups in a chunk"
    );
}

/// Prewarming a large partition in multiple chunks must end up holding exactly the
/// same per-token posting lists (doc ids and frequencies) as the whole-file path.
/// Parametrized over layout: the legacy-v1 chunk path rebases global offsets to
/// chunk-local rows, while the modern one-row-per-token path covers both
/// legacy-sized v2 and 256-doc v3 posting blocks.
#[rstest::rstest]
#[case::v1(InvertedListFormatVersion::V1, LEGACY_BLOCK_SIZE)]
#[case::v2(InvertedListFormatVersion::V2, LEGACY_BLOCK_SIZE)]
#[case::v3(InvertedListFormatVersion::V3, 256)]
#[tokio::test]
async fn test_prewarm_streams_in_chunks_preserves_content(
    #[case] format_version: InvertedListFormatVersion,
    #[case] block_size: usize,
) {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    // One partition with enough tokens to span multiple runtime synthetic
    // groups and several docs per token.
    let num_tokens = runtime_posting_group_tokens() as u32 + 4;
    const DOCS_PER_TOKEN: u32 = 3;
    let posting_tail_codec = format_version.posting_tail_codec();
    let mut builder = InnerBuilder::new_with_format_version_and_block_size(
        0,
        false,
        TokenSetFormat::default(),
        format_version,
        block_size,
    );
    // expected[token] = [(doc_id, frequency)] in stored (doc-id) order.
    let mut expected: Vec<Vec<(u32, u32)>> = Vec::new();
    let mut doc_id = 0u64;
    for t in 0..num_tokens {
        builder.tokens.add(format!("tok_{t:03}"));
        let mut posting = PostingListBuilder::new_with_posting_tail_codec_and_block_size(
            false,
            posting_tail_codec,
            block_size,
        );
        let mut docs = Vec::new();
        for _ in 0..DOCS_PER_TOKEN {
            posting.add(doc_id as u32, PositionRecorder::Count(1));
            builder.docs.append(doc_id, 1);
            docs.push((doc_id as u32, 1));
            doc_id += 1;
        }
        expected.push(docs);
        builder.posting_lists.push(posting);
    }
    builder.write(store.as_ref()).await.unwrap();

    let params = InvertedIndexParams::default()
        .block_size(block_size)
        .unwrap();
    let metadata = std::collections::HashMap::from_iter(vec![
        (
            "partitions".to_owned(),
            serde_json::to_string(&vec![0u64]).unwrap(),
        ),
        ("params".to_owned(), serde_json::to_string(&params).unwrap()),
        (
            TOKEN_SET_FORMAT_KEY.to_owned(),
            TokenSetFormat::default().to_string(),
        ),
        (
            POSTING_TAIL_CODEC_KEY.to_owned(),
            posting_tail_codec.as_str().to_owned(),
        ),
        (
            FTS_FORMAT_VERSION_KEY.to_owned(),
            format_version.index_version().to_string(),
        ),
        (POSTING_BLOCK_SIZE_KEY.to_owned(), block_size.to_string()),
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
    let inverted_list = &index.partitions[0].inverted_list;
    assert_eq!(inverted_list.len(), num_tokens as usize);
    assert_eq!(inverted_list.block_size(), block_size);

    // Force a small target chunk. Since CHUNK_TOKENS is below the runtime
    // group size, synthetic group alignment should still split only at
    // group boundaries.
    const CHUNK_TOKENS: usize = 6;
    let chunk_count = inverted_list
        .prewarm_posting_lists_chunked(false, Some(CHUNK_TOKENS), 2)
        .await
        .unwrap();

    // (1) The partition was streamed in multiple chunks. The exact count is
    // group-alignment-dependent (chunks snap to whole groups), so just
    // require more than one.
    assert!(
        chunk_count > 1,
        "single partition must be streamed in more than one chunk, got {chunk_count}"
    );

    if block_size == 256 {
        let (start, end) = inverted_list.group_range_for_token(0).unwrap();
        let group = inverted_list
            .index_cache
            .get_with_key(&posting_list_group_cache_key(
                start,
                end,
                inverted_list.has_impacts,
            ))
            .await
            .expect("256-document blocks should populate the packed group cache");
        assert!(group.is_packed());
        let (max_score, length) = inverted_list.bulk_metadata_for_token(0);
        let PostingList::Compressed(posting) =
            group.posting_list(0, max_score, length).unwrap().unwrap()
        else {
            panic!("expected compressed posting list");
        };
        assert_eq!(posting.block_size, 256);
        assert!(
            posting.impacts.is_some(),
            "packed prewarm must preserve impact skip data"
        );
    }

    // (2) Correctness: every token's posting list round-trips with exactly
    // the doc ids and frequencies of the whole-file path.
    for token_id in 0..num_tokens {
        let actual = inverted_list
            .posting_list(token_id, false, &NoOpMetricsCollector)
            .await
            .unwrap()
            .iter()
            .map(|(doc_id, freq, _positions)| (doc_id as u32, freq))
            .collect::<Vec<_>>();
        assert_eq!(
            actual, expected[token_id as usize],
            "token {token_id} posting list mismatch after chunked prewarm"
        );
    }
}

/// With positions, the chunked prewarm must strip positions into their own
/// per-token cache entries (leaving the posting cache positions-free) and still
/// round-trip exact doc ids, frequencies, and positions across chunk boundaries.
#[tokio::test]
async fn test_prewarm_streams_in_chunks_with_positions() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let format_version = InvertedListFormatVersion::V2;
    let posting_tail_codec = format_version.posting_tail_codec();
    let num_tokens = runtime_posting_group_tokens() as u32 + 4;
    const DOCS_PER_TOKEN: u32 = 3;
    let mut builder =
        InnerBuilder::new_with_format_version(0, true, TokenSetFormat::default(), format_version);
    // expected[token] = [(doc_id, frequency, positions)].
    let mut expected: Vec<Vec<(u32, u32, Vec<u32>)>> = Vec::new();
    let mut doc_id = 0u64;
    for t in 0..num_tokens {
        builder.tokens.add(format!("tok_{t:03}"));
        let mut posting = PostingListBuilder::new_with_posting_tail_codec(true, posting_tail_codec);
        let mut docs = Vec::new();
        for _ in 0..DOCS_PER_TOKEN {
            let positions = vec![t % 3, t % 3 + 2, t % 3 + 5];
            posting.add(
                doc_id as u32,
                PositionRecorder::Position(positions.clone().into()),
            );
            builder.docs.append(doc_id, positions.len() as u32);
            docs.push((doc_id as u32, positions.len() as u32, positions));
            doc_id += 1;
        }
        expected.push(docs);
        builder.posting_lists.push(posting);
    }
    builder.write(store.as_ref()).await.unwrap();

    let metadata = std::collections::HashMap::from_iter(vec![
        (
            "partitions".to_owned(),
            serde_json::to_string(&vec![0u64]).unwrap(),
        ),
        (
            "params".to_owned(),
            serde_json::to_string(&InvertedIndexParams::default().with_position(true)).unwrap(),
        ),
        (
            TOKEN_SET_FORMAT_KEY.to_owned(),
            TokenSetFormat::default().to_string(),
        ),
        (
            POSTING_TAIL_CODEC_KEY.to_owned(),
            posting_tail_codec.as_str().to_owned(),
        ),
        (
            POSITIONS_LAYOUT_KEY.to_owned(),
            POSITIONS_LAYOUT_SHARED_STREAM_V2.to_owned(),
        ),
        (
            POSITIONS_CODEC_KEY.to_owned(),
            PositionStreamCodec::PackedDelta.as_str().to_owned(),
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
    let inverted_list = &index.partitions[0].inverted_list;

    const CHUNK_TOKENS: usize = 5;
    let chunk_count = inverted_list
        .prewarm_posting_lists_chunked(true, Some(CHUNK_TOKENS), 2)
        .await
        .unwrap();
    assert!(
        chunk_count > 1,
        "partition must be streamed in more than one chunk, got {chunk_count}"
    );

    for token_id in 0..num_tokens {
        // The prewarmed posting cache entry is positions-free.
        let (start, end) = inverted_list.group_range_for_token(token_id).unwrap();
        let group = inverted_list
            .index_cache
            .get_with_key(&posting_list_group_cache_key(
                start,
                end,
                inverted_list.has_impacts,
            ))
            .await
            .unwrap();
        let slot = (token_id - start) as usize;
        assert!(
            !group.is_packed(),
            "with-position prewarm should retain the materialized fallback"
        );
        assert!(
            !group
                .posting_list(slot, None, None)
                .unwrap()
                .unwrap()
                .has_position(),
            "token {token_id} posting cache entry must be positions-free after prewarm"
        );

        // Full content (doc ids, frequencies, positions) round-trips; the
        // positions come from the dedicated per-token cache prewarm populated.
        let actual = inverted_list
            .posting_list(token_id, true, &NoOpMetricsCollector)
            .await
            .unwrap()
            .iter()
            .map(|(doc_id, freq, positions)| {
                (doc_id as u32, freq, positions.unwrap().collect::<Vec<_>>())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual, expected[token_id as usize],
            "token {token_id} posting list / positions mismatch after chunked prewarm"
        );
    }
}

#[tokio::test]
async fn test_strict_modern_prewarm_fails_when_index_cache_cannot_hold_all_partitions() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));
    let params = InvertedIndexParams::default().format_version(InvertedListFormatVersion::V2);
    let format_version = params.resolved_format_version();
    let partition_count = 6_u64;
    let docs_per_partition = 512_u32;

    for partition_id in 0..partition_count {
        let mut builder = InnerBuilder::new_with_format_version_and_block_size(
            partition_id,
            false,
            TokenSetFormat::default(),
            format_version,
            params.posting_block_size(),
        );
        builder.tokens.add(format!("token_{partition_id}"));
        let mut posting = PostingListBuilder::new_with_posting_tail_codec_and_block_size(
            false,
            format_version.posting_tail_codec(),
            params.posting_block_size(),
        );
        for doc_id in 0..docs_per_partition {
            posting.add(doc_id, PositionRecorder::Count(1));
            builder
                .docs
                .append(partition_id * 1_000_000 + doc_id as u64, 1);
        }
        builder.posting_lists.push(posting);
        builder.write(store.as_ref()).await.unwrap();
    }
    write_test_metadata(&store, (0..partition_count).collect(), params).await;

    let cache = Arc::new(LanceCache::with_backend(Arc::new(
        QuickCacheBackend::with_capacity(8 * 1024),
    )));
    let index = InvertedIndex::load(store, None, cache.as_ref())
        .await
        .unwrap();

    let err = index
        .prewarm_with_options(&FtsPrewarmOptions::default())
        .await
        .unwrap_err();
    let err = err.to_string();
    assert!(err.contains("partition(s) are not fully resident"));
    assert!(err.contains("resident row-address projection"));
    assert!(
        index
            .partitions
            .iter()
            .any(|partition| !partition.docs.query_ready()),
        "strict prewarm should fail because at least one partition lost query-ready state"
    );
    assert!(
        index
            .partitions
            .iter()
            .all(|partition| partition.inverted_list.modern_posting_validation_ready()),
        "posting validation should complete; the strict failure should be the final residency postcondition"
    );
    assert!(
        index.partitions.iter().any(|partition| {
            let docs = partition.docs.modern().unwrap();
            !docs.projection_resident()
        }),
        "at least one modern document projection should be non-resident"
    );

    let result = index
        .prewarm_with_options_result(&FtsPrewarmOptions::default().best_effort())
        .await
        .unwrap();
    assert!(!result.fully_resident);
    let diagnostics = result
        .diagnostics
        .expect("best-effort partial prewarm should report diagnostics");
    assert_eq!(diagnostics.partition_count, partition_count as usize);
    assert!(!diagnostics.failing_partitions.is_empty());
    assert!(
        diagnostics
            .failing_partitions
            .iter()
            .all(|partition| partition.posting_validation_ready),
        "best-effort should not suppress posting validation failures"
    );
}
