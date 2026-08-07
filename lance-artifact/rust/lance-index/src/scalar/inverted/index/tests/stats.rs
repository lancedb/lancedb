// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

/// IO accounting for the IO-counting stats test below: tracks bytes
/// pulled from the posting file so we can assert that the stats path is
/// O(1) in num_unique_tokens.
#[derive(Debug, Default)]
struct PostingMetadataCounter {
    rows_read: std::sync::atomic::AtomicUsize,
    metadata_rows_read: std::sync::atomic::AtomicUsize,
    read_range_calls: std::sync::atomic::AtomicUsize,
}

impl PostingMetadataCounter {
    fn rows_read(&self) -> usize {
        self.rows_read.load(std::sync::atomic::Ordering::Relaxed)
    }
    fn metadata_rows_read(&self) -> usize {
        self.metadata_rows_read
            .load(std::sync::atomic::Ordering::Relaxed)
    }
    fn read_range_calls(&self) -> usize {
        self.read_range_calls
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

struct CountingPostingReader {
    inner: Arc<dyn IndexReader>,
    counter: Arc<PostingMetadataCounter>,
}

#[async_trait]
impl IndexReader for CountingPostingReader {
    async fn read_record_batch(&self, n: u64, batch_size: u64) -> Result<RecordBatch> {
        self.inner.read_record_batch(n, batch_size).await
    }
    async fn read_global_buffer(&self, index: u32) -> Result<bytes::Bytes> {
        self.inner.read_global_buffer(index).await
    }
    async fn read_range(
        &self,
        range: std::ops::Range<usize>,
        projection: Option<&[&str]>,
    ) -> Result<RecordBatch> {
        let n = range.end - range.start;
        self.counter
            .read_range_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.counter
            .rows_read
            .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
        let touches_metadata = projection
            .map(|cols| cols.contains(&MAX_SCORE_COL) || cols.contains(&LENGTH_COL))
            .unwrap_or(false);
        if touches_metadata {
            self.counter
                .metadata_rows_read
                .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
        }
        self.inner.read_range(range, projection).await
    }
    async fn num_batches(&self, batch_size: u64) -> u32 {
        self.inner.num_batches(batch_size).await
    }
    fn num_rows(&self) -> usize {
        self.inner.num_rows()
    }
    fn schema(&self) -> &lance_core::datatypes::Schema {
        self.inner.schema()
    }
}

#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn IndexStore>,
    posting_file: String,
    counter: Arc<PostingMetadataCounter>,
}

impl DeepSizeOf for CountingStore {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.inner.deep_size_of_children(context)
    }
}

#[async_trait]
impl IndexStore for CountingStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn clone_arc(&self) -> Arc<dyn IndexStore> {
        Arc::new(Self {
            inner: self.inner.clone(),
            posting_file: self.posting_file.clone(),
            counter: self.counter.clone(),
        })
    }
    fn io_parallelism(&self) -> usize {
        self.inner.io_parallelism()
    }
    fn with_io_priority(&self, io_priority: u64) -> Arc<dyn IndexStore> {
        Arc::new(Self {
            inner: self.inner.with_io_priority(io_priority),
            posting_file: self.posting_file.clone(),
            counter: self.counter.clone(),
        })
    }
    async fn new_index_file(
        &self,
        name: &str,
        schema: Arc<arrow_schema::Schema>,
    ) -> Result<Box<dyn crate::scalar::IndexWriter>> {
        self.inner.new_index_file(name, schema).await
    }
    async fn open_index_file(&self, name: &str) -> Result<Arc<dyn IndexReader>> {
        let reader = self.inner.open_index_file(name).await?;
        if name == self.posting_file {
            Ok(Arc::new(CountingPostingReader {
                inner: reader,
                counter: self.counter.clone(),
            }))
        } else {
            Ok(reader)
        }
    }
    async fn copy_index_file(
        &self,
        name: &str,
        dest_store: &dyn IndexStore,
    ) -> Result<crate::scalar::IndexFile> {
        self.inner.copy_index_file(name, dest_store).await
    }
    async fn copy_index_file_to(
        &self,
        name: &str,
        new_name: &str,
        dest_store: &dyn IndexStore,
    ) -> Result<crate::scalar::IndexFile> {
        self.inner
            .copy_index_file_to(name, new_name, dest_store)
            .await
    }
    async fn rename_index_file(
        &self,
        name: &str,
        new_name: &str,
    ) -> Result<crate::scalar::IndexFile> {
        self.inner.rename_index_file(name, new_name).await
    }
    async fn delete_index_file(&self, name: &str) -> Result<()> {
        self.inner.delete_index_file(name).await
    }
    async fn list_files_with_sizes(&self) -> Result<Vec<crate::scalar::IndexFile>> {
        self.inner.list_files_with_sizes().await
    }
}

// Returns the `TempObjDir` guard so callers keep the backing store alive
// for the index's lifetime: the deferred DocSet re-opens the docs file on
// demand (it does not pin an open handle), so the files must still exist
// when the test exercises a scoring path.
async fn load_counted_v2_index(
    num_tokens: usize,
    cache: LanceCache,
) -> (Arc<InvertedIndex>, Arc<PostingMetadataCounter>, TempObjDir) {
    let tmpdir = TempObjDir::default();
    let inner_store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    for i in 0..num_tokens {
        builder.tokens.add(format!("t{}", i));
        let mut pl = PostingListBuilder::new(false);
        pl.add(i as u32, PositionRecorder::Count(1));
        builder.posting_lists.push(pl);
        builder.docs.append(i as u64, 1);
    }
    builder.write(inner_store.as_ref()).await.unwrap();

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
    let mut writer = inner_store
        .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
        .await
        .unwrap();
    writer.finish_with_metadata(metadata).await.unwrap();

    let counter = Arc::new(PostingMetadataCounter::default());
    let counting_store: Arc<dyn IndexStore> = Arc::new(CountingStore {
        inner: inner_store,
        posting_file: posting_file_path(0),
        counter: counter.clone(),
    });
    let index = InvertedIndex::load(counting_store, None, &cache)
        .await
        .unwrap();
    (index, counter, tmpdir)
}

/// IO regression test for the lazy posting-metadata refactor. Builds a
/// v2 InvertedIndex with `num_tokens` tokens in a single partition,
/// wraps the IndexStore so reads against the posting file are counted,
/// then asserts:
///
/// * `InvertedIndex::load` does not touch the posting file at all
///   (`InvertedPartition::load` only needs the token file and docs file).
/// * `bm25_stats_for_terms(["t0"])` reads exactly one metadata row from
///   the posting file for token 0 regardless of how many unique tokens the
///   partition has.
///
/// Before this refactor, `PostingListReader::try_new` did
/// `read_range(0..num_rows, [MAX_SCORE_COL, LENGTH_COL])`, so the
/// `metadata_rows_read` figure scaled linearly with `num_tokens` even
/// when nobody asked for those stats. The cases below exercise that
/// scaling explicitly.
#[rstest::rstest]
#[case::tokens_10(10)]
#[case::tokens_100(100)]
#[case::tokens_1000(1000)]
#[tokio::test]
async fn test_bm25_stats_for_terms_is_lazy(#[case] num_tokens: usize) {
    let (index, counter, _tmpdir) = load_counted_v2_index(num_tokens, LanceCache::no_cache()).await;
    assert!(
        !index.partitions[0].inverted_list.is_legacy_layout(),
        "this test only proves the lazy path for v2 indexes",
    );

    // Opening the partition must not pull anything from the posting file.
    // Pre-fix, `PostingListReader::try_new` issued one read_range here for
    // [MAX_SCORE_COL, LENGTH_COL] covering every unique token.
    assert_eq!(
        counter.read_range_calls(),
        0,
        "InvertedIndex::load must not read the posting file (was {} calls)",
        counter.read_range_calls(),
    );
    assert_eq!(counter.rows_read(), 0);

    let (total_tokens, num_docs, dfs) = index
        .bm25_stats_for_terms(&["t0".to_string()], None)
        .await
        .unwrap();
    assert_eq!(total_tokens, num_tokens as u64);
    assert_eq!(num_docs, num_tokens);
    assert_eq!(dfs, vec![1]);

    // Stats must pull a constant number of metadata rows from the posting
    // file regardless of how many tokens the partition has. One term, one
    // partition, one row.
    assert_eq!(
        counter.metadata_rows_read(),
        1,
        "stats path should read exactly 1 metadata row per (term, partition); \
             got {} (read_range_calls={}, rows_read={}, num_tokens={})",
        counter.metadata_rows_read(),
        counter.read_range_calls(),
        counter.rows_read(),
        num_tokens,
    );
}

#[tokio::test]
async fn test_bm25_stats_for_terms_reuses_posting_metadata_cache() {
    let cache = LanceCache::with_capacity(1024 * 1024);
    let (index, counter, _tmpdir) = load_counted_v2_index(100, cache.clone()).await;

    let terms = ["t0".to_string()];
    let first = index.bm25_stats_for_terms(&terms, None).await.unwrap();
    assert_eq!(first, (100, 100, vec![1]));
    assert_eq!(counter.metadata_rows_read(), 1);

    let second = index.bm25_stats_for_terms(&terms, None).await.unwrap();
    assert_eq!(second, first);
    assert_eq!(
        counter.metadata_rows_read(),
        1,
        "repeated stats for the same token should reuse cached posting metadata",
    );
}

#[tokio::test]
async fn test_bm25_stats_for_terms_records_metadata_cache_stats() {
    let cache = LanceCache::with_capacity(1024 * 1024);
    let (index, _counter, _tmpdir) = load_counted_v2_index(100, cache.clone()).await;
    assert!(
        !index.partitions[0].inverted_list.is_legacy_layout(),
        "this test only proves the v2 metadata boundary",
    );

    let terms = ["t0".to_string(), "t1".to_string(), "t2".to_string()];
    let cold = LocalMetricsCollector::default();
    let cold_stats = index
        .bm25_stats_for_terms(&terms, Some(&cold))
        .await
        .unwrap();
    assert_eq!(cold_stats.2, vec![1, 1, 1]);
    assert_eq!(cold.index_cache_misses(), terms.len());
    assert_eq!(cold.index_cache_hits(), 0);

    let warm = LocalMetricsCollector::default();
    let warm_stats = index
        .bm25_stats_for_terms(&terms, Some(&warm))
        .await
        .unwrap();
    assert_eq!(warm_stats, cold_stats);
    assert_eq!(warm.index_cache_misses(), 0);
    assert_eq!(warm.index_cache_hits(), terms.len());
}

#[tokio::test]
async fn test_aggregate_corpus_stats_reuses_cached_value() {
    let (index, _counter, _tmpdir) = load_counted_v2_index(100, LanceCache::no_cache()).await;
    assert!(index.corpus_stats.get().is_none());

    let first = index.aggregate_corpus_stats().await.unwrap();
    assert_eq!(first, (100, 100));
    assert_eq!(index.corpus_stats.get().copied(), Some(first));

    let second = index.aggregate_corpus_stats().await.unwrap();
    assert_eq!(second, first);
}

#[tokio::test]
async fn test_persisted_stats_do_not_load_document_columns() {
    let (index, _counter, _tmpdir) = load_counted_v2_index(100, LanceCache::no_cache()).await;
    assert!(!index.is_legacy());
    let partition = index.partitions[0].clone();
    let documents = partition.docs.modern().unwrap();

    assert_eq!(index.aggregate_corpus_stats().await.unwrap(), (100, 100));
    assert_eq!(documents.cached_stats().unwrap().total_tokens, 100);
    assert!(!documents.lengths_loaded());
    assert!(!documents.projection_loaded());

    let views = futures::future::join_all((0..8).map(|_| documents.lengths()))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let first = &views[0];
    assert!(views.iter().all(|view| Arc::ptr_eq(first, view)));
    assert_eq!(first.total_tokens(), 100);

    let all_rows = RowAddrMask::all_rows();
    assert!(matches!(
        documents
            .visibility(Arc::new(all_rows), false)
            .await
            .unwrap(),
        DocVisibility::All
    ));
    assert!(!documents.projection_loaded());

    let filtered = RowAddrMask::allow_nothing();
    let visibility = documents
        .visibility(Arc::new(filtered), true)
        .await
        .unwrap();
    assert!(visibility.is_empty());
    assert!(!documents.projection_loaded());
    assert_eq!(
        documents
            .resolve_addresses(&[DocId::new(0), DocId::new(99)])
            .await
            .unwrap(),
        [0, 99]
    );
}

#[tokio::test]
async fn test_no_hit_partition_does_not_load_document_columns() {
    let (index, _counter, _tmpdir) = load_counted_v2_index(100, LanceCache::no_cache()).await;
    let documents = index.partitions[0].docs.modern().unwrap();
    assert!(!documents.lengths_loaded());
    assert!(!documents.projection_loaded());

    let tokens = Arc::new(Tokens::new(vec!["missing-token".to_owned()], DocType::Text));
    let params = Arc::new(FtsSearchParams::new().with_limit(Some(10)));
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

    assert!(row_ids.is_empty());
    assert!(scores.is_empty());
    assert!(!documents.lengths_loaded());
    assert!(!documents.projection_loaded());
}

#[tokio::test]
async fn test_concurrent_stats_and_lengths_initialization() {
    let (index, _counter, _tmpdir) = load_counted_v2_index(100, LanceCache::no_cache()).await;
    let docs = index.partitions[0].docs.modern().unwrap().clone();

    let stats = futures::future::join_all((0..8).map(|_| docs.stats()));
    let views = futures::future::join_all((0..8).map(|_| docs.lengths()));
    let (stats, views) = tokio::join!(stats, views);

    let stats = stats.into_iter().collect::<Result<Vec<_>>>().unwrap();
    assert!(stats.iter().all(|stats| stats.total_tokens == 100));
    let views = views.into_iter().collect::<Result<Vec<_>>>().unwrap();
    let first = &views[0];
    assert!(views.iter().all(|view| Arc::ptr_eq(first, view)));
    assert_eq!(docs.cached_stats().unwrap().total_tokens, 100);
}

#[tokio::test]
async fn test_grouped_posting_lists_read_one_group_per_neighborhood() {
    // Cold-start scoring must not bulk-read the full `0..num_tokens`
    // metadata table. With small-posting grouping (issue #7040), scoring
    // K adjacent cold tokens shares a single group cache entry: one
    // read_range bounded by the group size, independent of the partition's
    // total token count.
    let runtime_group_size = runtime_posting_group_tokens().max(1);
    let queried_token_count = runtime_group_size.min(4);
    let queried_tokens = (0..queried_token_count as u32).collect::<Vec<_>>();
    let num_tokens = runtime_group_size
        .saturating_mul(2)
        .max(queried_token_count + 1)
        .min(1024);
    let (index, counter, _tmpdir) = load_counted_v2_index(num_tokens, LanceCache::no_cache()).await;
    let inverted_list = index.partitions[0].inverted_list.clone();
    assert!(
        !inverted_list.is_legacy_layout(),
        "this test only proves the lazy path for v2 indexes",
    );
    assert!(
        matches!(
            &inverted_list.grouping,
            PostingGrouping::SyntheticFixed { .. }
        ),
        "freshly written v2 index should use runtime synthetic groups",
    );

    // This fixture uses a no-op cache, so each call re-reads; that isolates
    // the per-query read shape. Each posting_list call reads exactly its
    // own group — bounded by the group size, never the full token table.
    let metrics = Arc::new(NoOpMetricsCollector);
    for &token_id in &queried_tokens {
        inverted_list
            .posting_list(token_id, false, metrics.as_ref())
            .await
            .unwrap();
    }

    let (start, end) = inverted_list.group_range_for_token(0).unwrap();
    let group_len = (end - start) as usize;
    assert!(
        (queried_tokens.len()..=num_tokens).contains(&group_len),
        "group [{start}, {end}) should cover the queried neighborhood and \
             stay bounded by the {num_tokens}-token table",
    );
    assert_eq!(
        counter.read_range_calls(),
        queried_tokens.len(),
        "each cold token should read exactly its own group, no bulk read",
    );
    assert_eq!(
        counter.metadata_rows_read(),
        queried_tokens.len() * group_len,
        "each query reads one group's metadata rows ({group_len}), not the \
             full {num_tokens}-row table",
    );
}

/// Build a single-partition v2 index where every token's posting list spans
/// `docs_per_token` docs. Runtime grouping packs consecutive token rows
/// into shared cache groups.
async fn load_v2_index_with_grouped_postings(
    num_tokens: usize,
    docs_per_token: usize,
) -> (Arc<InvertedIndex>, Arc<LanceCache>) {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let num_docs = num_tokens * docs_per_token;
    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
    for token_id in 0..num_tokens {
        builder.tokens.add(format!("t{token_id}"));
        let mut pl = PostingListBuilder::new(false);
        for d in 0..docs_per_token {
            let doc_id = (token_id * docs_per_token + d) as u32;
            pl.add(doc_id, PositionRecorder::Count(1));
        }
        builder.posting_lists.push(pl);
    }
    for doc in 0..num_docs {
        builder.docs.append(doc as u64, 1);
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

    // The inverted list keeps only a `WeakLanceCache`, so the caller must
    // hold this `Arc<LanceCache>` alive for the cache to stay usable.
    let cache = Arc::new(LanceCache::with_capacity(1 << 30));
    let index = InvertedIndex::load(store, None, cache.as_ref())
        .await
        .unwrap();
    (index, cache)
}

/// Packed groups charge their Arrow buffers and contiguous metadata once,
/// avoiding the per-member enum/array object graph of a materialized group.
#[tokio::test]
async fn test_packed_group_deep_size_is_smaller_than_materialized_graph() {
    let (index, _cache) = load_v2_index_with_grouped_postings(512, 1).await;
    let inverted_list = index.partitions[0].inverted_list.clone();
    assert!(!inverted_list.is_legacy_layout(), "expected v2 layout");
    assert!(
        matches!(
            &inverted_list.grouping,
            PostingGrouping::SyntheticFixed { .. }
        ),
        "expected grouped posting lists"
    );

    // Populate the group cache via the same path a query uses.
    inverted_list
        .posting_list(0, false, &NoOpMetricsCollector)
        .await
        .unwrap();
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
    assert!(group.is_packed(), "cold v2 group should use packed storage");
    inverted_list.ensure_metadata_loaded().await.unwrap();

    let mut distinct_buffers = std::collections::HashSet::new();
    let mut materialized = Vec::with_capacity(group.len());
    for slot in 0..group.len() {
        let (max_score, length) = inverted_list.bulk_metadata_for_token(start + slot as u32);
        let posting = group
            .posting_list(slot, max_score, length)
            .unwrap()
            .unwrap();
        let PostingList::Compressed(compressed) = posting else {
            panic!("expected compressed posting lists");
        };
        distinct_buffers.insert(compressed.blocks.values().as_ptr());
        materialized.push(PostingList::Compressed(compressed));
    }
    let posting_count = materialized.len();

    assert!(
        posting_count > 1,
        "default grouping should pack multiple tiny postings into one group"
    );
    assert_eq!(
        distinct_buffers.len(),
        1,
        "read-path postings in a group should share one backing buffer"
    );
    let packed_size = group.deep_size_of();
    let materialized_size = PostingListGroup::new(materialized).deep_size_of();
    assert!(
        packed_size * 4 < materialized_size * 3,
        "packed group deep_size_of {packed_size}B should be at least 25% smaller than the \
             {materialized_size}B materialized graph for {posting_count} postings"
    );
}

// ===========================================================================
// Regression tests for index-cache size accounting of cached posting lists.
//
// A cached posting list is a *slice* of a buffer read for a whole posting-list
// group, so its `DeepSizeOf` impl must charge only the bytes the slice
// references, not the full shared backing buffer. These lock that in: each
// builds an array that references a small slice of a much larger buffer and
// asserts `deep_size_of()` tracks the slice, not the buffer.
// ===========================================================================

/// Build a `List<Int32>` of `num_sublists` x `ints_per_sublist`, then return
/// the slice `[off, off + len)`. The returned array shares the full backing
/// buffers, so `values().get_buffer_memory_size()` still reports the whole
/// thing — the slicing-unaware over-count the fix targets.
fn sliced_int32_list(
    num_sublists: usize,
    ints_per_sublist: usize,
    off: usize,
    len: usize,
) -> ListArray {
    let mut builder = ListBuilder::new(Int32Builder::new());
    for s in 0..num_sublists {
        for i in 0..ints_per_sublist {
            builder
                .values()
                .append_value((s * ints_per_sublist + i) as i32);
        }
        builder.append(true);
    }
    builder.finish().slice(off, len)
}

#[test]
fn test_compressed_posting_deep_size_counts_only_referenced_blocks_slice() {
    const ELEM_BYTES: usize = 256;
    const TOTAL_ELEMS: usize = 64;
    const SLICE_OFF: usize = 10;
    const SLICE_LEN: usize = 2;

    let mut builder = LargeBinaryBuilder::new();
    for _ in 0..TOTAL_ELEMS {
        builder.append_value(vec![7u8; ELEM_BYTES]);
    }
    let full = builder.finish();
    let blocks = full.slice(SLICE_OFF, SLICE_LEN);

    let posting = CompressedPostingList::new(
        blocks,
        1.0,
        SLICE_LEN as u32,
        PostingTailCodec::Fixed32,
        LEGACY_BLOCK_SIZE,
        None,
        None,
    );

    let full_backing = full.get_buffer_memory_size();
    let slice_bytes = SLICE_LEN * ELEM_BYTES;
    let reported = posting.deep_size_of();

    assert!(
        reported < full_backing / 4,
        "deep_size_of {reported}B must not count the {full_backing}B shared buffer"
    );
    assert!(
        reported <= slice_bytes * 2,
        "deep_size_of {reported}B should track the ~{slice_bytes}B referenced slice"
    );
}

#[test]
fn test_plain_posting_deep_size_counts_only_referenced_positions_slice() {
    const SUBLISTS: usize = 64;
    const INTS: usize = 64;
    const SLICE_LEN: usize = 2;

    let positions = sliced_int32_list(SUBLISTS, INTS, 10, SLICE_LEN);
    let row_ids = ScalarBuffer::from(vec![0u64, 1]);
    let frequencies = ScalarBuffer::from(vec![1.0f32, 1.0]);
    let posting = PlainPostingList::new(row_ids, frequencies, Some(1.0), Some(positions.clone()));

    let full_backing = positions.values().get_buffer_memory_size();
    let slice_bytes = SLICE_LEN * INTS * std::mem::size_of::<i32>();
    let reported = posting.deep_size_of();

    assert!(
        reported < full_backing / 4,
        "deep_size_of {reported}B must not count the {full_backing}B shared positions buffer"
    );
    assert!(
        reported <= slice_bytes * 2 + 64,
        "deep_size_of {reported}B should track the ~{slice_bytes}B referenced slice"
    );
}

#[test]
fn test_legacy_per_doc_positions_deep_size_counts_only_referenced_slice() {
    const SUBLISTS: usize = 64;
    const INTS: usize = 64;
    const SLICE_LEN: usize = 2;

    let positions = sliced_int32_list(SUBLISTS, INTS, 10, SLICE_LEN);
    let full_backing = positions.values().get_buffer_memory_size();
    let slice_bytes = SLICE_LEN * INTS * std::mem::size_of::<i32>();

    let storage = CompressedPositionStorage::LegacyPerDoc(positions);
    let reported = storage.deep_size_of();
    assert!(
        reported < full_backing / 4,
        "CompressedPositionStorage deep_size_of {reported}B must not count the \
             {full_backing}B shared buffer"
    );
    assert!(
        reported <= slice_bytes * 2 + 64,
        "deep_size_of {reported}B should track the ~{slice_bytes}B referenced slice"
    );

    // The `Positions` cache wrapper must report the same slice-aware size.
    let wrapped = Positions(storage).deep_size_of();
    assert!(
        wrapped < full_backing / 4,
        "Positions deep_size_of {wrapped}B must not count the {full_backing}B shared buffer"
    );
}
