// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

/// Build a multi-partition inverted index in `store` with `num_partitions`
/// partitions, each carrying a handful of tokens/docs.
async fn build_multi_partition_index(
    store: &Arc<LanceIndexStore>,
    num_partitions: u64,
) -> (Arc<InvertedIndex>, Arc<LanceCache>) {
    for id in 0..num_partitions {
        let mut builder = InnerBuilder::new_with_format_version(
            id,
            false,
            TokenSetFormat::default(),
            InvertedListFormatVersion::V1,
        );
        // A few distinct tokens per partition so each posting file has real
        // content to read and materialize during prewarm.
        for t in 0..4u32 {
            builder.tokens.add(format!("tok_{id}_{t}"));
            let mut posting =
                PostingListBuilder::new_with_posting_tail_codec(false, PostingTailCodec::Fixed32);
            let base = id * 1000 + t as u64 * 10;
            for d in 0..5u32 {
                posting.add(d, PositionRecorder::Count(1));
                builder.docs.append(base + d as u64, 4);
            }
            builder.posting_lists.push(posting);
        }
        builder.write(store.as_ref()).await.unwrap();
    }

    let partition_ids: Vec<u64> = (0..num_partitions).collect();
    let metadata = std::collections::HashMap::from_iter(vec![
        (
            "partitions".to_owned(),
            serde_json::to_string(&partition_ids).unwrap(),
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

    // Keep the cache alive and return it: the partition readers hold only a
    // WeakLanceCache, so the prewarmed entries vanish if this Arc is dropped.
    let cache = Arc::new(LanceCache::with_capacity(1 << 20));
    let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
        .await
        .unwrap();
    (index, cache)
}

/// The prewarm cost estimate must come from cheap object metadata (the
/// posting file length) without reading the posting data, and must be
/// monotonic in the partition's content.
#[tokio::test]
async fn test_posting_data_size_bytes_uses_file_length() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));
    let (index, _cache) = build_multi_partition_index(&store, 3).await;
    for part in &index.partitions {
        // File length is reported by object metadata at open time; it must be
        // non-trivial for a partition that actually holds postings.
        let est = part.inverted_list.posting_data_size_bytes();
        assert!(
            est > 0,
            "expected a non-zero posting-data size estimate, got {est}"
        );
    }
}

/// Each partition must read through the shared scheduler at a distinct base
/// priority. Tied priorities (every partition at 0) break the scheduler's
/// backpressure deadlock-break — which admits the lowest-priority in-flight
/// request — because there is no unique lowest request to advance, so a
/// concurrent multi-partition read (e.g. prewarm) can wedge. Distinct
/// per-partition priorities keep the in-flight set totally ordered.
#[tokio::test]
async fn test_partitions_load_with_distinct_priorities() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));
    let (index, _cache) = build_multi_partition_index(&store, 5).await;

    let mut priorities: Vec<u64> = index
        .partitions
        .iter()
        .map(|part| {
            part.store
                .as_any()
                .downcast_ref::<LanceIndexStore>()
                .expect("partition store should be a LanceIndexStore")
                .io_priority()
        })
        .collect();

    // Distinct and dense (0..N): every partition reads at its own priority,
    // so the shared scheduler sees a total order across all partitions. The
    // partitions may finish loading in any order, so sort before comparing —
    // what matters is that the priorities form a contiguous, collision-free
    // set, not which partition ended up at which slot.
    priorities.sort_unstable();
    assert_eq!(
        priorities,
        (0..index.partitions.len() as u64).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_update_preserves_v2_format_version() -> Result<()> {
    let src_dir = TempObjDir::default();
    let dest_dir = TempObjDir::default();
    let src_store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        src_dir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));
    let dest_store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        dest_dir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let format_version = InvertedListFormatVersion::V2;
    let posting_tail_codec = format_version.posting_tail_codec();
    let mut partition =
        InnerBuilder::new_with_format_version(0, false, TokenSetFormat::default(), format_version);
    partition.tokens.add("hello".to_owned());
    let mut posting_list =
        PostingListBuilder::new_with_posting_tail_codec(false, posting_tail_codec);
    posting_list.add(0, PositionRecorder::Count(1));
    partition.posting_lists.push(posting_list);
    partition.docs.append(100, 1);
    partition.write(src_store.as_ref()).await?;

    let metadata = HashMap::from([
        (
            "partitions".to_owned(),
            serde_json::to_string(&vec![0_u64]).unwrap(),
        ),
        (
            "params".to_owned(),
            serde_json::to_string(&InvertedIndexParams::default()).unwrap(),
        ),
        (
            TOKEN_SET_FORMAT_KEY.to_owned(),
            TokenSetFormat::default().to_string(),
        ),
        (
            POSTING_TAIL_CODEC_KEY.to_owned(),
            posting_tail_codec.as_str().to_owned(),
        ),
    ]);
    let mut writer = src_store
        .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
        .await
        .unwrap();
    writer.finish_with_metadata(metadata).await.unwrap();

    let index = InvertedIndex::load(src_store, None, &LanceCache::no_cache()).await?;
    assert_eq!(index.format_version(), format_version);
    assert_eq!(index.index_version(), INVERTED_INDEX_VERSION_V2);

    let schema = Arc::new(Schema::new(vec![
        Field::new("doc", DataType::Utf8, true),
        Field::new(ROW_ID, DataType::UInt64, false),
    ]));
    let docs = Arc::new(StringArray::from(vec![Some("hello again")]));
    let row_ids = Arc::new(UInt64Array::from(vec![101u64]));
    let batch = RecordBatch::try_new(schema.clone(), vec![docs, row_ids])?;
    let stream = RecordBatchStreamAdapter::new(schema, stream::iter(vec![Ok(batch)]));
    let created = index
        .update(Box::pin(stream), dest_store.as_ref(), None)
        .await?;

    assert_eq!(created.index_version, INVERTED_INDEX_VERSION_V2);

    let updated = InvertedIndex::load(dest_store, None, &LanceCache::no_cache()).await?;
    assert_eq!(updated.format_version(), format_version);
    assert_eq!(updated.index_version(), INVERTED_INDEX_VERSION_V2);
    assert_eq!(updated.partitions.len(), 2);
    for partition in &updated.partitions {
        assert_eq!(
            partition.inverted_list.posting_tail_codec(),
            posting_tail_codec
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_block_size_256_writes_v3_metadata_and_index_version() -> Result<()> {
    let src_dir = TempObjDir::default();
    let dest_dir = TempObjDir::default();
    let src_store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        src_dir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));
    let dest_store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        dest_dir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let params = InvertedIndexParams::default().block_size(256)?;
    let format_version = params.resolved_format_version();
    assert_eq!(format_version, InvertedListFormatVersion::V3);

    let mut partition = InnerBuilder::new_with_format_version_and_block_size(
        0,
        false,
        TokenSetFormat::default(),
        format_version,
        params.posting_block_size(),
    );
    partition.tokens.add("hello".to_owned());
    let mut posting_list = PostingListBuilder::new_with_posting_tail_codec_and_block_size(
        false,
        format_version.posting_tail_codec(),
        params.posting_block_size(),
    );
    posting_list.add(0, PositionRecorder::Count(1));
    partition.posting_lists.push(posting_list);
    partition.docs.append(100, 1);
    partition.write(src_store.as_ref()).await?;

    write_test_metadata(&src_store, vec![0], params).await;

    let index = InvertedIndex::load(src_store, None, &LanceCache::no_cache()).await?;
    assert_eq!(index.format_version(), InvertedListFormatVersion::V3);
    assert_eq!(index.index_version(), INVERTED_INDEX_VERSION_V3);

    let created = index
        .update(empty_doc_stream(), dest_store.as_ref(), None)
        .await?;
    assert_eq!(created.index_version, INVERTED_INDEX_VERSION_V3);

    let updated = InvertedIndex::load(dest_store, None, &LanceCache::no_cache()).await?;
    assert_eq!(updated.format_version(), InvertedListFormatVersion::V3);
    assert_eq!(updated.index_version(), INVERTED_INDEX_VERSION_V3);

    Ok(())
}

#[tokio::test]
async fn test_merge_segments_preserves_arrow_token_set_format() -> Result<()> {
    let src_dir = TempObjDir::default();
    let dest_dir = TempObjDir::default();
    let src_store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        src_dir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));
    let dest_store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        dest_dir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let index = write_single_partition_index(
        src_store,
        InvertedIndexParams::default().format_version(InvertedListFormatVersion::V2),
        TokenSetFormat::Arrow,
        "hello",
        100,
    )
    .await?;
    assert_eq!(index.index_version(), 0);
    let created = InvertedIndex::merge_segments(
        &[index],
        empty_doc_stream(),
        dest_store.as_ref(),
        None,
        crate::progress::noop_progress(),
    )
    .await?;

    assert_eq!(created.index_version, 0);
    let merged = InvertedIndex::load(dest_store, None, &LanceCache::no_cache()).await?;
    assert_eq!(merged.index_version(), 0);
    assert_eq!(merged.token_set_format, TokenSetFormat::Arrow);

    let tokens = Arc::new(Tokens::new(vec!["hello".to_string()], DocType::Text));
    let params = Arc::new(FtsSearchParams::new().with_limit(Some(10)));
    let prefilter = Arc::new(NoFilter);
    let metrics = Arc::new(NoOpMetricsCollector);
    let (row_ids, _) = merged
        .bm25_search(tokens, params, Operator::Or, prefilter, metrics, None)
        .await?;
    assert_eq!(row_ids, vec![100]);

    Ok(())
}

#[rstest::rstest]
#[case::v1(InvertedListFormatVersion::V1, LEGACY_BLOCK_SIZE)]
#[case::v2(InvertedListFormatVersion::V2, LEGACY_BLOCK_SIZE)]
#[case::v3_128(InvertedListFormatVersion::V3, LEGACY_BLOCK_SIZE)]
#[case::v3_256(InvertedListFormatVersion::V3, 256)]
#[tokio::test]
async fn test_merge_segments_preserves_format_version(
    #[case] format_version: InvertedListFormatVersion,
    #[case] block_size: usize,
) -> Result<()> {
    let src_dir = TempObjDir::default();
    let dest_dir = TempObjDir::default();
    let src_store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        src_dir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));
    let dest_store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        dest_dir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));
    let params = InvertedIndexParams::default()
        .block_size(block_size)?
        .format_version(format_version);

    let index =
        write_single_partition_index(src_store, params, TokenSetFormat::Fst, "hello", 100).await?;
    assert_eq!(index.format_version(), format_version);

    let created = InvertedIndex::merge_segments(
        &[index],
        empty_doc_stream(),
        dest_store.as_ref(),
        None,
        crate::progress::noop_progress(),
    )
    .await?;
    assert_eq!(created.index_version, format_version.index_version());

    let merged = InvertedIndex::load(dest_store, None, &LanceCache::no_cache()).await?;
    assert_eq!(merged.format_version(), format_version);
    assert_eq!(merged.index_version(), format_version.index_version());

    Ok(())
}

#[tokio::test]
async fn test_merge_segments_uses_memory_limit_for_old_partitions() -> Result<()> {
    let src_dir_1 = TempObjDir::default();
    let src_dir_2 = TempObjDir::default();
    let dest_dir = TempObjDir::default();
    let src_store_1 = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        src_dir_1.clone(),
        Arc::new(LanceCache::no_cache()),
    ));
    let src_store_2 = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        src_dir_2.clone(),
        Arc::new(LanceCache::no_cache()),
    ));
    let dest_store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        dest_dir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let params = InvertedIndexParams::default().memory_limit_mb(0);
    let first = write_single_partition_index(
        src_store_1,
        params.clone(),
        TokenSetFormat::default(),
        "alpha",
        100,
    )
    .await?;
    let second =
        write_single_partition_index(src_store_2, params, TokenSetFormat::default(), "beta", 200)
            .await?;

    let mut builder = InvertedIndexBuilder::new(InvertedIndexParams::default().memory_limit_mb(0))
        .with_token_set_format(TokenSetFormat::default());
    builder
        .update_from_segments(
            empty_doc_stream(),
            dest_store.as_ref(),
            &[first, second],
            None,
        )
        .await?;

    let merged = InvertedIndex::load(dest_store, None, &LanceCache::no_cache()).await?;
    assert_eq!(merged.partitions.len(), 2);
    let mut partition_ids = merged
        .partitions
        .iter()
        .map(|partition| partition.id())
        .collect::<Vec<_>>();
    partition_ids.sort_unstable();
    assert_eq!(partition_ids, vec![0, 1]);

    Ok(())
}

#[tokio::test]
async fn test_modern_index_without_deleted_col_has_empty_bitmap() {
    // An index created before the deleted_fragments feature was added
    // will have a metadata file with num_rows=0 (no record batch data).
    // The load path should gracefully handle this with an empty bitmap.
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    builder.tokens.add("test".to_owned());
    builder.posting_lists.push(PostingListBuilder::new(false));
    builder.posting_lists[0].add(0, PositionRecorder::Count(1));
    builder.docs.append(100, 1);
    builder.write(store.as_ref()).await.unwrap();

    // Write a metadata file WITHOUT the deleted_fragments column
    // (simulates an older index version)
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

    let index = InvertedIndex::load(store, None, &LanceCache::no_cache())
        .await
        .unwrap();
    assert!(
        index.deleted_fragments().is_empty(),
        "index without deleted_fragments column should have empty bitmap"
    );
}
