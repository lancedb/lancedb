// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;
use async_trait::async_trait;
use lance_core::cache::{CacheBackend, CacheCodec, CacheEntry, InternalCacheKey};
use std::future::Future;
use std::pin::Pin;

#[derive(Debug)]
struct RejectPositionsCacheBackend {
    inner: QuickCacheBackend,
}

impl RejectPositionsCacheBackend {
    fn new(capacity: usize) -> Self {
        Self {
            inner: QuickCacheBackend::with_capacity(capacity),
        }
    }

    fn rejects(codec: Option<&CacheCodec>) -> bool {
        codec.is_some_and(|codec| codec.type_id() == "lance.fts.Positions")
    }
}

#[async_trait]
impl CacheBackend for RejectPositionsCacheBackend {
    async fn get(&self, key: &InternalCacheKey, codec: Option<CacheCodec>) -> Option<CacheEntry> {
        self.inner.get(key, codec).await
    }

    async fn insert(
        &self,
        key: &InternalCacheKey,
        entry: CacheEntry,
        size_bytes: usize,
        codec: Option<CacheCodec>,
    ) {
        if Self::rejects(codec.as_ref()) {
            return;
        }
        self.inner.insert(key, entry, size_bytes, codec).await;
    }

    async fn get_or_insert<'a>(
        &self,
        key: &InternalCacheKey,
        loader: Pin<Box<dyn Future<Output = Result<(CacheEntry, usize)>> + Send + 'a>>,
        codec: Option<CacheCodec>,
    ) -> Result<(CacheEntry, bool)> {
        if Self::rejects(codec.as_ref()) {
            return loader.await.map(|(entry, _)| (entry, false));
        }
        self.inner.get_or_insert(key, loader, codec).await
    }

    async fn clear(&self) {
        self.inner.clear().await;
    }

    async fn num_entries(&self) -> usize {
        self.inner.num_entries().await
    }

    async fn size_bytes(&self) -> usize {
        self.inner.size_bytes().await
    }

    fn approx_num_entries(&self) -> usize {
        self.inner.approx_num_entries()
    }

    fn approx_size_bytes(&self) -> usize {
        self.inner.approx_size_bytes()
    }
}

#[tokio::test]
async fn test_prewarm_with_positions_populates_separate_position_cache() {
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

    let cache = Arc::new(LanceCache::with_backend(Arc::new(
        QuickCacheBackend::with_capacity(4096),
    )));
    let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
        .await
        .unwrap();

    index
        .prewarm_with_options(&FtsPrewarmOptions::new().with_position(true))
        .await
        .unwrap();

    let inverted_list = &index.partitions[0].inverted_list;
    // The posting cache entry is grouped (issue #7040); the group holds
    // positions-free lists while positions live in their own per-token
    // entries.
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
        !group.is_packed(),
        "with-position prewarm should retain the materialized fallback"
    );
    assert!(
        !group
            .posting_list(0, None, None)
            .unwrap()
            .unwrap()
            .has_position(),
        "posting cache should remain positions-free after prewarm"
    );

    let positions = inverted_list
        .index_cache
        .get_with_key(&PositionKey { token_id: 0 })
        .await
        .unwrap();
    assert!(
        matches!(
            positions.as_ref().0,
            CompressedPositionStorage::LegacyPerDoc(_)
        ),
        "positions should be stored in the dedicated position cache"
    );

    drop(positions);
    drop(group);
    cache.clear().await;
    assert!(
        inverted_list
            .index_cache
            .get_with_key(&PositionKey { token_id: 0 })
            .await
            .is_none()
    );

    index
        .prewarm_with_options(&FtsPrewarmOptions::default())
        .await
        .unwrap();
    assert!(index.prewarm_state.lock().await.satisfies(true));
    assert!(
        inverted_list
            .index_cache
            .get_with_key(&PositionKey { token_id: 0 })
            .await
            .is_some(),
        "re-prewarm after eviction must preserve the strongest requested mode"
    );
}

#[tokio::test]
async fn test_best_effort_prewarm_reports_missing_requested_positions() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let params = InvertedIndexParams::default()
        .with_position(true)
        .format_version(InvertedListFormatVersion::V2);
    let format_version = params.resolved_format_version();
    let mut builder = InnerBuilder::new_with_format_version_and_block_size(
        0,
        true,
        TokenSetFormat::default(),
        format_version,
        params.posting_block_size(),
    );
    builder.tokens.add("alpha".to_owned());
    builder.tokens.add("beta".to_owned());
    for token_id in 0..2 {
        let mut posting = PostingListBuilder::new_with_posting_tail_codec_and_block_size(
            true,
            format_version.posting_tail_codec(),
            params.posting_block_size(),
        );
        posting.add(0, PositionRecorder::Position(vec![token_id].into()));
        builder.posting_lists.push(posting);
    }
    builder.docs.append(100, 2);
    builder.write(store.as_ref()).await.unwrap();
    write_test_metadata(&store, vec![0], params).await;

    let cache = Arc::new(LanceCache::with_backend(Arc::new(
        RejectPositionsCacheBackend::new(1 << 20),
    )));
    let index = InvertedIndex::load(store, None, cache.as_ref())
        .await
        .unwrap();

    let result = index
        .prewarm_with_options_result(&FtsPrewarmOptions::new().with_position(true).best_effort())
        .await
        .unwrap();
    assert!(
        !result.fully_resident,
        "prewarm must not report full residency when requested positions were rejected"
    );
    let diagnostics = result
        .diagnostics
        .expect("missing requested positions should produce diagnostics");
    assert_eq!(diagnostics.partition_count, 1);
    assert_eq!(diagnostics.failing_partitions.len(), 1);
    let partition = &diagnostics.failing_partitions[0];
    assert!(partition.documents.query_ready());
    assert!(partition.posting_validation_ready);
    assert!(partition.posting_resident);
    assert_eq!(partition.position_resident, Some(false));
}

#[tokio::test]
async fn test_prewarm_with_v2_positions_preserves_shared_stream_codec() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let format_version = InvertedListFormatVersion::V2;
    let posting_tail_codec = format_version.posting_tail_codec();
    let mut builder =
        InnerBuilder::new_with_format_version(0, true, TokenSetFormat::default(), format_version);
    builder.tokens.add("body".to_owned());

    let mut posting_list =
        PostingListBuilder::new_with_posting_tail_codec(true, posting_tail_codec);
    let expected = (0..(BLOCK_SIZE + 5) as u32)
        .map(|doc_id| {
            let positions = vec![doc_id % 3, doc_id % 3 + 2, doc_id % 3 + 5];
            posting_list.add(doc_id, PositionRecorder::Position(positions.clone().into()));
            builder.docs.append(30_000 + doc_id as u64, 20 + doc_id % 7);
            (doc_id, positions.len() as u32, positions)
        })
        .collect::<Vec<_>>();
    builder.posting_lists.push(posting_list);
    builder.write(store.as_ref()).await.unwrap();

    let metadata = HashMap::from([
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

    let cache = Arc::new(LanceCache::with_capacity(4096));
    let index = InvertedIndex::load(store, None, cache.as_ref())
        .await
        .unwrap();
    index
        .prewarm_with_options(&FtsPrewarmOptions::new().with_position(true))
        .await
        .unwrap();

    let actual = index.partitions[0]
        .inverted_list
        .posting_list(0, true, &NoOpMetricsCollector)
        .await
        .unwrap()
        .iter()
        .map(|(doc_id, freq, positions)| {
            (doc_id as u32, freq, positions.unwrap().collect::<Vec<_>>())
        })
        .collect::<Vec<_>>();

    assert_eq!(actual, expected);
}

#[test]
fn test_block_max_scores_capacity_matches_block_count() {
    let mut docs = DocSet::default();
    let num_docs = BLOCK_SIZE * 3 + 7;
    let doc_ids = (0..num_docs as u32).collect::<Vec<_>>();
    for doc_id in &doc_ids {
        docs.append(*doc_id as u64, 1);
    }

    let freqs = vec![1_u32; doc_ids.len()];
    let block_max_scores = docs.calculate_block_max_scores(doc_ids.iter(), freqs.iter());
    let expected_blocks = doc_ids.len().div_ceil(BLOCK_SIZE);

    assert_eq!(block_max_scores.len(), expected_blocks);
    assert_eq!(block_max_scores.capacity(), expected_blocks);
}
