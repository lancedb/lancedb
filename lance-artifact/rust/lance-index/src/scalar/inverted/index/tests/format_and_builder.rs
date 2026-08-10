// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

#[test]
fn address_read_concurrency_respects_payload_budget() {
    assert_eq!(address_read_concurrency(64, 0), 64);
    assert_eq!(address_read_concurrency(64, 8 * 1024 * 1024), 8);
    assert_eq!(address_read_concurrency(64, 16 * 1024 * 1024), 4);
    assert_eq!(
        address_read_concurrency(64, 2 * MAX_CONCURRENT_ADDRESS_READ_BYTES),
        1
    );
}

#[derive(Debug)]
struct MetadataAccessDeniedStore {
    inner: Arc<dyn IndexStore>,
}

impl DeepSizeOf for MetadataAccessDeniedStore {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.inner.deep_size_of_children(context)
    }
}

#[async_trait]
impl IndexStore for MetadataAccessDeniedStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn clone_arc(&self) -> Arc<dyn IndexStore> {
        Arc::new(Self {
            inner: self.inner.clone(),
        })
    }

    fn io_parallelism(&self) -> usize {
        self.inner.io_parallelism()
    }

    async fn new_index_file(
        &self,
        name: &str,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn crate::scalar::IndexWriter>> {
        self.inner.new_index_file(name, schema).await
    }

    async fn open_index_file(&self, name: &str) -> Result<Arc<dyn IndexReader>> {
        if name == METADATA_FILE {
            Err(Error::io("metadata access denied"))
        } else {
            self.inner.open_index_file(name).await
        }
    }

    fn with_io_priority(&self, io_priority: u64) -> Arc<dyn IndexStore> {
        Arc::new(Self {
            inner: self.inner.with_io_priority(io_priority),
        })
    }

    async fn copy_index_file(
        &self,
        name: &str,
        dest_store: &dyn IndexStore,
    ) -> Result<crate::scalar::IndexFile> {
        self.inner.copy_index_file(name, dest_store).await
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

#[tokio::test]
async fn params_legacy_fallback_probes_tokens_after_metadata_access_denied() {
    let tmpdir = TempObjDir::default();
    let inner: Arc<dyn IndexStore> = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));
    let expected = InvertedIndexParams::default();
    let metadata = HashMap::from([(
        "tokenizer".to_owned(),
        serde_json::to_string(&expected).unwrap(),
    )]);
    let mut writer = inner
        .new_index_file(TOKENS_FILE, Arc::new(Schema::empty()))
        .await
        .unwrap();
    writer.finish_with_metadata(metadata).await.unwrap();
    let store = MetadataAccessDeniedStore { inner };

    let actual = InvertedIndex::load_params(&store).await.unwrap();
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
}

#[tokio::test]
async fn params_legacy_probe_preserves_metadata_error_when_tokens_are_missing() {
    let tmpdir = TempObjDir::default();
    let inner: Arc<dyn IndexStore> = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));
    let store = MetadataAccessDeniedStore { inner };

    let error = InvertedIndex::load_params(&store).await.unwrap_err();
    assert!(matches!(error, Error::IO { .. }));
    assert!(error.to_string().contains("metadata access denied"));
}

#[tokio::test]
async fn params_metadata_ignores_unknown_fields() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));
    let expected = InvertedIndexParams::default();
    let mut params = serde_json::to_value(&expected).unwrap();
    let params = params.as_object_mut().unwrap();
    params.insert("skip_merge".to_owned(), true.into());
    params.insert(
        "future_parameter".to_owned(),
        serde_json::json!({ "enabled": true }),
    );
    let metadata = HashMap::from([("params".to_owned(), serde_json::to_string(params).unwrap())]);
    let mut writer = store
        .new_index_file(METADATA_FILE, Arc::new(Schema::empty()))
        .await
        .unwrap();
    writer.finish_with_metadata(metadata).await.unwrap();

    let actual = InvertedIndex::load_params(store.as_ref()).await.unwrap();
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
}

#[test]
fn test_posting_block_size_schema_metadata() {
    assert_eq!(parse_posting_block_size(&HashMap::new()).unwrap(), 128);

    let metadata = HashMap::from([(POSTING_BLOCK_SIZE_KEY.to_owned(), "512".to_owned())]);
    let err = parse_posting_block_size(&metadata).unwrap_err();
    assert!(err.to_string().contains("block_size"));

    let metadata = HashMap::from([(POSTING_BLOCK_SIZE_KEY.to_owned(), "129".to_owned())]);
    let err = parse_posting_block_size(&metadata).unwrap_err();
    assert!(err.to_string().contains("block_size"));
}

#[test]
fn test_num_tokens_only_reuses_sliced_arrow_storage() {
    let docs = {
        let source = UInt32Array::from(vec![999, 7, 16, 1024, 888]);
        let sliced = source.slice(1, 3);
        let mut docs = DocSet::from_num_tokens_only(&sliced);

        let NumTokens::Shared(values) = &docs.num_tokens else {
            panic!("num-tokens-only DocSet must retain shared Arrow storage");
        };
        assert!(values.ptr_eq(sliced.values()));
        assert_eq!(values.as_ref(), &[7, 16, 1024]);
        assert_eq!(docs.total_tokens_num(), 1047);
        docs.set_quantized_scoring(true);
        assert_eq!(docs.scoring_norms().unwrap().len(), 3);
        assert_eq!(
            docs.scoring_num_tokens(0),
            dequantize_doc_length(quantize_doc_length(7))
        );
        assert_eq!(
            docs.scoring_num_tokens(2),
            dequantize_doc_length(quantize_doc_length(1024))
        );
        docs
    };

    assert_eq!(docs.len(), 3);
    assert_eq!(docs.num_tokens(0), 7);
    assert_eq!(docs.num_tokens(2), 1024);
}

#[test]
fn test_cached_num_tokens_uses_supplied_total_and_full_stays_owned() {
    const CACHED_TOTAL_MARKER: u64 = 123_456;

    let num_tokens = UInt32Array::from(vec![3, 5, 8]);
    let docs = DocSet::from_cached_num_tokens(&num_tokens, CACHED_TOTAL_MARKER);
    assert_eq!(docs.total_tokens_num(), CACHED_TOTAL_MARKER);
    assert!(matches!(&docs.num_tokens, NumTokens::Shared(_)));

    let row_ids = UInt64Array::from(vec![10, 20, 30]);
    let full = DocSet::from_columns(&row_ids, &num_tokens, false, None).unwrap();
    assert!(matches!(&full.num_tokens, NumTokens::Owned(_)));
    assert_eq!(full.total_tokens_num(), 16);
    assert_eq!(full.row_id(1), 20);
}

#[test]
fn test_posting_builder_writes_impacts_for_supported_block_sizes() {
    for block_size in [128, 256] {
        let format_version = default_fts_format_version_for_block_size(block_size).unwrap();
        let num_docs = block_size * 33 + 1;
        let mut docs = DocSet::default();
        let mut posting = PostingListBuilder::new_with_posting_tail_codec_and_block_size(
            false,
            format_version.posting_tail_codec(),
            block_size,
        );
        for doc_id in 0..num_docs {
            docs.append(doc_id as u64, (doc_id % 5 + 1) as u32);
            posting.add(
                doc_id as u32,
                PositionRecorder::Count((doc_id % 3 + 1) as u32),
            );
        }
        let schema =
            inverted_list_schema_for_version_with_block_size(false, format_version, block_size);
        let batch = posting.to_batch_with_docs(&docs, schema).unwrap();
        assert!(batch.column_by_name(IMPACT_COL).is_some());
        let max_score = batch[MAX_SCORE_COL].as_primitive::<Float32Type>().value(0);
        let length = batch[LENGTH_COL].as_primitive::<UInt32Type>().value(0);
        let posting = PostingList::from_batch(&batch, Some(max_score), Some(length)).unwrap();
        let PostingList::Compressed(posting) = posting else {
            panic!("expected compressed posting list");
        };
        let impacts = posting.impacts.expect("posting should include impacts");
        assert_eq!(impacts.level0_len(), posting.blocks.len());
        assert_eq!(impacts.level1_len(), posting.blocks.len().div_ceil(32));
        assert_eq!(
            impacts.entries().len(),
            impacts.level0_len() + impacts.level1_len()
        );
    }
}

#[test]
fn test_posting_builder_without_impact_column_roundtrips_without_impacts() {
    let mut posting = PostingListBuilder::new(false);
    for doc_id in 0..BLOCK_SIZE + 3 {
        posting.add(doc_id as u32, PositionRecorder::Count(1));
    }
    let batch = posting.to_batch(vec![1.0, 1.0]).unwrap();
    assert!(batch.column_by_name(IMPACT_COL).is_none());
    let posting =
        PostingList::from_batch(&batch, Some(1.0), Some((BLOCK_SIZE + 3) as u32)).unwrap();
    assert!(!posting.has_impacts());
}

#[tokio::test]
async fn test_build_search_uses_configured_posting_block_size() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let params = InvertedIndexParams::default().block_size(256).unwrap();
    let format_version = params.resolved_format_version();
    let block_size = params.posting_block_size();
    let num_docs = block_size + 7;

    let mut builder = InnerBuilder::new_with_format_version_and_block_size(
        0,
        false,
        TokenSetFormat::default(),
        format_version,
        block_size,
    );
    builder.tokens.add("needle".to_owned());
    let mut posting_list = PostingListBuilder::new_with_posting_tail_codec_and_block_size(
        false,
        format_version.posting_tail_codec(),
        block_size,
    );
    for doc_id in 0..num_docs {
        posting_list.add(doc_id as u32, PositionRecorder::Count(1));
        builder.docs.append(1_000 + doc_id as u64, 1);
    }
    builder.posting_lists.push(posting_list);
    builder.write(store.as_ref()).await.unwrap();
    write_test_metadata(&store, vec![0], params).await;

    let cache = Arc::new(LanceCache::with_capacity(4096));
    let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
        .await
        .unwrap();
    assert_eq!(index.partitions[0].inverted_list.block_size(), block_size);

    let posting = index.partitions[0]
        .inverted_list
        .posting_list(0, false, &NoOpMetricsCollector)
        .await
        .unwrap();
    let PostingList::Compressed(posting) = posting else {
        panic!("expected compressed posting list");
    };
    assert_eq!(posting.block_size, block_size);
    assert_eq!(posting.blocks.len(), num_docs.div_ceil(block_size));
    let impacts = posting
        .impacts
        .as_ref()
        .expect("newly written posting list should include impacts");
    assert_eq!(impacts.level0_len(), posting.blocks.len());
    assert_eq!(impacts.level1_len(), posting.blocks.len().div_ceil(32));
    assert_eq!(
        impacts.entries().len(),
        impacts.level0_len() + impacts.level1_len()
    );

    let tokens = Arc::new(Tokens::new(vec!["needle".to_owned()], DocType::Text));
    let params = Arc::new(FtsSearchParams::new().with_limit(Some(10)));
    let prefilter = Arc::new(NoFilter);
    let metrics = Arc::new(NoOpMetricsCollector);
    let (row_ids, scores) = index
        .bm25_search(tokens, params, Operator::Or, prefilter, metrics, None)
        .await
        .unwrap();

    assert_eq!(row_ids.len(), 10);
    assert_eq!(scores.len(), 10);
    assert!(row_ids.iter().all(|row_id| *row_id >= 1_000));
}

#[tokio::test]
async fn test_posting_builder_remap() {
    let posting_tail_codec = PostingTailCodec::Fixed32;
    let mut builder = PostingListBuilder::new_with_posting_tail_codec(false, posting_tail_codec);
    let n = BLOCK_SIZE + 3;
    for i in 0..n {
        builder.add(i as u32, PositionRecorder::Count(1));
    }
    let removed = vec![5, 7];
    builder.remap(&removed);

    let mut expected = PostingListBuilder::new_with_posting_tail_codec(false, posting_tail_codec);
    for i in 0..n - removed.len() {
        expected.add(i as u32, PositionRecorder::Count(1));
    }
    let expected_entries = expected.iter().collect::<Vec<_>>();
    let actual_entries = builder.iter().collect::<Vec<_>>();
    assert_eq!(actual_entries, expected_entries);

    // BLOCK_SIZE + 3 elements should be reduced to BLOCK_SIZE + 1,
    // there are still 2 blocks.
    let batch = builder.to_batch(vec![1.0, 2.0]).unwrap();
    let (doc_ids, freqs) = decompress_posting_list_with_tail_codec(
        (n - removed.len()) as u32,
        batch[POSTING_COL]
            .as_list::<i32>()
            .value(0)
            .as_binary::<i64>(),
        posting_tail_codec,
    )
    .unwrap();
    assert!(
        doc_ids
            .iter()
            .zip(expected_entries.iter().map(|(doc_id, _, _)| doc_id))
            .all(|(a, b)| a == b)
    );
    assert!(
        freqs
            .iter()
            .zip(expected_entries.iter().map(|(_, freq, _)| freq))
            .all(|(a, b)| a == b)
    );
}

#[test]
fn test_posting_builder_size_tracking_matches_structure() {
    fn tracked_memory_size(builder: &PostingListBuilder) -> u64 {
        let encoded_blocks_size = builder
            .encoded_blocks
            .iter()
            .map(|encoded_blocks| std::mem::size_of::<EncodedBlocks>() + encoded_blocks.size())
            .sum::<usize>();
        let encoded_positions_size = builder
            .encoded_position_blocks
            .as_ref()
            .map(|positions| std::mem::size_of::<EncodedPositionBlocks>() + positions.size())
            .unwrap_or(0usize);
        (encoded_blocks_size
            + builder.tail_entries.capacity() * std::mem::size_of::<RawDocInfo>()
            + builder.tail_positions.size()
            + encoded_positions_size) as u64
    }

    let mut builder = PostingListBuilder::new(true);
    for doc_id in 0..(BLOCK_SIZE + 5) as u32 {
        builder.add(
            doc_id,
            PositionRecorder::Position(smallvec::smallvec![1, 3, 5]),
        );
    }

    assert_eq!(builder.size(), tracked_memory_size(&builder));
}

#[test]
fn test_posting_builder_flush_releases_tail_position_capacity() {
    let mut builder = PostingListBuilder::new(true);
    let positions = smallvec::SmallVec::<[u32; 2]>::from_vec((0..1024).collect());
    for doc_id in 0..BLOCK_SIZE as u32 {
        builder.add(doc_id, PositionRecorder::Position(positions.clone()));
    }

    assert_eq!(builder.tail_positions.size(), 0);
    assert_eq!(builder.size(), {
        let encoded_blocks_size = builder
            .encoded_blocks
            .iter()
            .map(|encoded_blocks| std::mem::size_of::<EncodedBlocks>() + encoded_blocks.size())
            .sum::<usize>();
        let encoded_positions_size = builder
            .encoded_position_blocks
            .as_ref()
            .map(|positions| std::mem::size_of::<EncodedPositionBlocks>() + positions.size())
            .unwrap_or(0usize);
        (encoded_blocks_size
            + builder.tail_entries.capacity() * std::mem::size_of::<RawDocInfo>()
            + builder.tail_positions.size()
            + encoded_positions_size) as u64
    });
}

#[test]
fn test_posting_builder_streamed_positions_roundtrip() {
    let mut builder = PostingListBuilder::new(true);
    assert!(builder.add_occurrence(0, 1).unwrap());
    assert!(!builder.add_occurrence(0, 4).unwrap());
    assert!(!builder.add_occurrence(0, 9).unwrap());
    builder.finish_open_doc(0).unwrap();

    assert!(builder.add_occurrence(2, 3).unwrap());
    builder.finish_open_doc(2).unwrap();

    let entries = builder.iter().collect::<Vec<_>>();
    assert_eq!(
        entries,
        vec![
            (0_u32, 3_u32, Some(vec![1_u32, 4_u32, 9_u32])),
            (2_u32, 1_u32, Some(vec![3_u32])),
        ]
    );
}

#[test]
fn test_shared_position_stream_clone_shares_block_offsets() {
    let stream = SharedPositionStream::new(
        PositionStreamCodec::PackedDelta,
        vec![0_u32, 4, 11],
        bytes::Bytes::from_static(b"shared position bytes"),
    );
    let original_offsets = stream.block_offsets().as_ptr();

    let cloned = stream.clone();

    assert_eq!(cloned.block_offsets(), stream.block_offsets());
    assert_eq!(cloned.block_offsets().as_ptr(), original_offsets);
}

#[test]
fn test_posting_builder_roundtrip_shared_positions() {
    let entries = vec![
        (0_u32, vec![1_u32, 5]),
        (2, vec![0, 4, 9]),
        (4, vec![7]),
        (8, vec![3, 10]),
        (13, vec![2, 11, 30]),
    ];
    let mut builder =
        PostingListBuilder::new_with_posting_tail_codec(true, PostingTailCodec::VarintDelta);
    for (doc_id, positions) in &entries {
        builder.add(
            *doc_id,
            PositionRecorder::Position(positions.clone().into()),
        );
    }

    let batch = builder.to_batch(vec![1.0]).unwrap();
    assert!(batch.column_by_name(COMPRESSED_POSITION_COL).is_some());
    assert!(batch.column_by_name(POSITION_COL).is_none());
    assert_eq!(
        batch.schema_ref().metadata().get(POSTING_TAIL_CODEC_KEY),
        Some(&PostingTailCodec::VarintDelta.as_str().to_owned())
    );
    assert_eq!(
        batch.schema_ref().metadata().get(POSITIONS_LAYOUT_KEY),
        Some(&POSITIONS_LAYOUT_SHARED_STREAM_V2.to_owned())
    );
    assert_eq!(
        batch.schema_ref().metadata().get(POSITIONS_CODEC_KEY),
        Some(&PositionStreamCodec::PackedDelta.as_str().to_owned())
    );

    let posting = PostingList::from_batch(&batch, Some(1.0), Some(entries.len() as u32)).unwrap();
    let actual = posting
        .iter()
        .map(|(doc_id, freq, positions)| {
            (doc_id as u32, freq, positions.unwrap().collect::<Vec<_>>())
        })
        .collect::<Vec<_>>();
    let expected = entries
        .iter()
        .map(|(doc_id, positions)| (*doc_id, positions.len() as u32, positions.clone()))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[test]
fn test_posting_builder_roundtrip_legacy_positions() {
    let entries = vec![(0_u32, vec![1_u32, 5]), (2, vec![0, 4, 9]), (4, vec![7])];
    let mut builder =
        PostingListBuilder::new_with_posting_tail_codec(true, PostingTailCodec::Fixed32);
    for (doc_id, positions) in &entries {
        builder.add(
            *doc_id,
            PositionRecorder::Position(positions.clone().into()),
        );
    }

    let batch = builder.to_batch(vec![1.0]).unwrap();
    assert!(batch.column_by_name(POSITION_COL).is_some());
    assert!(batch.column_by_name(COMPRESSED_POSITION_COL).is_none());
    assert_eq!(
        batch.schema_ref().metadata().get(POSTING_TAIL_CODEC_KEY),
        None
    );
    assert_eq!(
        batch.schema_ref().metadata().get(POSITIONS_LAYOUT_KEY),
        None
    );
    assert_eq!(batch.schema_ref().metadata().get(POSITIONS_CODEC_KEY), None);

    let posting = PostingList::from_batch(&batch, Some(1.0), Some(entries.len() as u32)).unwrap();
    let actual = posting
        .iter()
        .map(|(doc_id, freq, positions)| {
            (doc_id as u32, freq, positions.unwrap().collect::<Vec<_>>())
        })
        .collect::<Vec<_>>();
    let expected = entries
        .iter()
        .map(|(doc_id, positions)| (*doc_id, positions.len() as u32, positions.clone()))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[test]
fn test_resolve_fts_format_version_defaults_to_v2() {
    assert_eq!(
        resolve_fts_format_version(None).unwrap(),
        InvertedListFormatVersion::V2
    );
    assert_eq!(
        resolve_fts_format_version(Some("2")).unwrap(),
        InvertedListFormatVersion::V2
    );
    assert_eq!(
        resolve_fts_format_version(Some("3")).unwrap(),
        InvertedListFormatVersion::V3
    );
    assert!(resolve_fts_format_version(Some("4")).is_err());
}

#[test]
fn test_block_size_256_metadata_resolves_to_v3() {
    let metadata = HashMap::from([(POSTING_BLOCK_SIZE_KEY.to_owned(), "256".to_owned())]);
    assert_eq!(
        parse_format_version_from_metadata(&metadata).unwrap(),
        InvertedListFormatVersion::V3
    );
}

#[test]
fn test_legacy_compressed_positions_still_readable() {
    let doc_ids = [1_u32, 3_u32];
    let frequencies = [2_u32, 3_u32];
    let posting = compress_posting_list_with_tail_codec(
        doc_ids.len(),
        doc_ids.iter(),
        frequencies.iter(),
        std::iter::once(1.0_f32),
        PostingTailCodec::Fixed32,
    )
    .unwrap();

    let mut posting_builder = ListBuilder::new(LargeBinaryBuilder::new());
    for idx in 0..posting.len() {
        posting_builder.values().append_value(posting.value(idx));
    }
    posting_builder.append(true);

    let mut positions_builder = ListBuilder::new(ListBuilder::new(LargeBinaryBuilder::new()));
    for positions in [vec![1_u32, 5_u32], vec![0_u32, 4_u32, 9_u32]] {
        let compressed = compress_positions(&positions).unwrap();
        let doc_builder = positions_builder.values();
        for idx in 0..compressed.len() {
            doc_builder.values().append_value(compressed.value(idx));
        }
        doc_builder.append(true);
    }
    positions_builder.append(true);

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            POSTING_COL,
            DataType::List(Arc::new(Field::new("item", DataType::LargeBinary, true))),
            false,
        ),
        Field::new(MAX_SCORE_COL, DataType::Float32, false),
        Field::new(LENGTH_COL, DataType::UInt32, false),
        Field::new(
            POSITION_COL,
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::LargeBinary, true))),
                true,
            ))),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(posting_builder.finish()) as ArrayRef,
            Arc::new(Float32Array::from(vec![1.0])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![doc_ids.len() as u32])) as ArrayRef,
            Arc::new(positions_builder.finish()) as ArrayRef,
        ],
    )
    .unwrap();

    let posting = PostingList::from_batch(&batch, Some(1.0), Some(doc_ids.len() as u32)).unwrap();
    let actual = posting
        .iter()
        .map(|(doc_id, freq, positions)| {
            (doc_id as u32, freq, positions.unwrap().collect::<Vec<_>>())
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, vec![(1, 2, vec![1, 5]), (3, 3, vec![0, 4, 9]),]);
}

#[test]
fn test_shared_stream_v2_without_codec_still_readable() {
    let doc_ids = [1_u32, 3_u32];
    let frequencies = [2_u32, 3_u32];
    let posting = compress_posting_list_with_tail_codec(
        doc_ids.len(),
        doc_ids.iter(),
        frequencies.iter(),
        std::iter::once(1.0_f32),
        PostingTailCodec::Fixed32,
    )
    .unwrap();

    let mut posting_builder = ListBuilder::new(LargeBinaryBuilder::new());
    for idx in 0..posting.len() {
        posting_builder.values().append_value(posting.value(idx));
    }
    posting_builder.append(true);

    let positions = vec![1_u32, 5_u32, 0_u32, 4_u32, 9_u32];
    let mut encoded_positions = Vec::new();
    encode_position_stream_block_into(
        &positions,
        &frequencies,
        PositionStreamCodec::VarintDocDelta,
        &mut encoded_positions,
    )
    .unwrap();

    let mut position_offsets = ListBuilder::new(UInt32Builder::new());
    position_offsets.values().append_value(0);
    position_offsets.append(true);

    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new(
                POSTING_COL,
                DataType::List(Arc::new(Field::new("item", DataType::LargeBinary, true))),
                false,
            ),
            Field::new(MAX_SCORE_COL, DataType::Float32, false),
            Field::new(LENGTH_COL, DataType::UInt32, false),
            Field::new(COMPRESSED_POSITION_COL, DataType::LargeBinary, false),
            Field::new(
                POSITION_BLOCK_OFFSET_COL,
                DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
                false,
            ),
        ],
        HashMap::from([(
            POSITIONS_LAYOUT_KEY.to_owned(),
            POSITIONS_LAYOUT_SHARED_STREAM_V2.to_owned(),
        )]),
    ));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(posting_builder.finish()) as ArrayRef,
            Arc::new(Float32Array::from(vec![1.0])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![doc_ids.len() as u32])) as ArrayRef,
            Arc::new(arrow_array::LargeBinaryArray::from(vec![Some(
                encoded_positions.as_slice(),
            )])) as ArrayRef,
            Arc::new(position_offsets.finish()) as ArrayRef,
        ],
    )
    .unwrap();

    let posting = PostingList::from_batch(&batch, Some(1.0), Some(doc_ids.len() as u32)).unwrap();
    let actual = posting
        .iter()
        .map(|(doc_id, freq, positions)| {
            (doc_id as u32, freq, positions.unwrap().collect::<Vec<_>>())
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, vec![(1, 2, vec![1, 5]), (3, 3, vec![0, 4, 9]),]);
}

#[test]
fn test_shared_position_stream_is_smaller_for_sparse_positions() {
    let mut builder =
        PostingListBuilder::new_with_posting_tail_codec(true, PostingTailCodec::VarintDelta);
    let mut legacy_positions = Vec::with_capacity(BLOCK_SIZE * 4);
    for doc_id in 0..(BLOCK_SIZE * 4) as u32 {
        let mut positions = vec![doc_id * 3 + 1];
        if doc_id % 8 == 0 {
            positions.push(doc_id * 3 + 2);
        }
        builder.add(doc_id, PositionRecorder::Position(positions.clone().into()));
        legacy_positions.push(positions);
    }

    let batch = builder.to_batch(vec![1.0; 4]).unwrap();
    let shared_positions_size = batch[COMPRESSED_POSITION_COL].get_buffer_memory_size()
        + batch[POSITION_BLOCK_OFFSET_COL].get_buffer_memory_size();

    let mut positions_builder = ListBuilder::new(ListBuilder::new(LargeBinaryBuilder::new()));
    for positions in legacy_positions {
        let compressed = compress_positions(&positions).unwrap();
        let doc_builder = positions_builder.values();
        for idx in 0..compressed.len() {
            doc_builder.values().append_value(compressed.value(idx));
        }
        doc_builder.append(true);
    }
    positions_builder.append(true);
    let legacy_positions_size = positions_builder.finish().get_buffer_memory_size();

    assert!(
        shared_positions_size < legacy_positions_size,
        "expected shared position stream to be smaller than legacy per-doc storage, shared={shared_positions_size}, legacy={legacy_positions_size}",
    );
}

#[test]
fn test_posting_list_batch_matches_docset_scoring() {
    let mut docs = DocSet::default();
    let num_docs = BLOCK_SIZE + 3;
    for doc_id in 0..num_docs as u32 {
        docs.append(doc_id as u64, doc_id % 7 + 1);
    }

    let doc_ids = (0..num_docs as u32).collect::<Vec<_>>();
    let freqs = doc_ids
        .iter()
        .map(|doc_id| doc_id % 5 + 1)
        .collect::<Vec<_>>();

    let mut builder_scores = PostingListBuilder::new(false);
    let mut builder_docs = PostingListBuilder::new(false);
    for (&doc_id, &freq) in doc_ids.iter().zip(freqs.iter()) {
        builder_scores.add(doc_id, PositionRecorder::Count(freq));
        builder_docs.add(doc_id, PositionRecorder::Count(freq));
    }

    let block_max_scores = docs.calculate_block_max_scores(doc_ids.iter(), freqs.iter());
    let batch_scores = builder_scores.to_batch(block_max_scores).unwrap();
    let batch_docs = builder_docs
        .to_batch_with_docs(&docs, inverted_list_schema(false))
        .unwrap();

    let scores_posting = batch_scores[POSTING_COL].as_list::<i32>().value(0);
    let scores_posting = scores_posting.as_binary::<i64>();
    let docs_posting = batch_docs[POSTING_COL].as_list::<i32>().value(0);
    let docs_posting = docs_posting.as_binary::<i64>();
    assert_eq!(scores_posting, docs_posting);

    let score_left = batch_scores[MAX_SCORE_COL]
        .as_primitive::<Float32Type>()
        .value(0);
    let score_right = batch_docs[MAX_SCORE_COL]
        .as_primitive::<Float32Type>()
        .value(0);
    assert!((score_left - score_right).abs() < 1e-6);

    let len_left = batch_scores[LENGTH_COL]
        .as_primitive::<UInt32Type>()
        .value(0);
    let len_right = batch_docs[LENGTH_COL].as_primitive::<UInt32Type>().value(0);
    assert_eq!(len_left, len_right);
}

#[tokio::test]
async fn test_remap_to_empty_posting_list() {
    let tmpdir = TempObjDir::default();
    let store = Arc::new(LanceIndexStore::new(
        ObjectStore::local().into(),
        tmpdir.clone(),
        Arc::new(LanceCache::no_cache()),
    ));

    let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());

    // index of docs:
    // 0: lance
    // 1: lake lake
    // 2: lake lake lake
    builder.tokens.add("lance".to_owned());
    builder.tokens.add("lake".to_owned());
    builder.posting_lists.push(PostingListBuilder::new(false));
    builder.posting_lists.push(PostingListBuilder::new(false));
    builder.posting_lists[0].add(0, PositionRecorder::Count(1));
    builder.posting_lists[1].add(1, PositionRecorder::Count(2));
    builder.posting_lists[1].add(2, PositionRecorder::Count(3));
    builder.docs.append(0, 1);
    builder.docs.append(1, 1);
    builder.docs.append(2, 1);
    builder.write(store.as_ref()).await.unwrap();

    let index = InvertedPartition::load(
        store.clone(),
        0,
        None,
        &LanceCache::no_cache(),
        TokenSetFormat::default(),
    )
    .await
    .unwrap();
    let mut builder = index.into_builder().await.unwrap();

    let mapping = HashMap::from([(0, None), (2, Some(3))]);
    builder.remap(&RowAddrRemap::direct(mapping)).await.unwrap();

    // after remap, the doc 0 is removed, and the doc 2 is updated to 3
    assert_eq!(builder.tokens.len(), 1);
    assert_eq!(builder.tokens.get("lake"), Some(0));
    assert_eq!(builder.posting_lists.len(), 1);
    assert_eq!(builder.posting_lists[0].len(), 2);
    assert_eq!(builder.docs.len(), 2);
    assert_eq!(builder.docs.row_id(0), 1);
    assert_eq!(builder.docs.row_id(1), 3);

    builder.write(store.as_ref()).await.unwrap();

    // remap to delete all docs
    let mapping = HashMap::from([(1, None), (3, None)]);
    builder.remap(&RowAddrRemap::direct(mapping)).await.unwrap();

    assert_eq!(builder.tokens.len(), 0);
    assert_eq!(builder.posting_lists.len(), 0);
    assert_eq!(builder.docs.len(), 0);

    builder.write(store.as_ref()).await.unwrap();
}

#[test]
fn test_docset_remap_preserves_element_coordinates() {
    let mut docs = DocSet::default();
    docs.append_with_doc_index(10, 2, &[0]).unwrap();
    docs.append_with_doc_index(10, 3, &[3]).unwrap();
    docs.append_with_doc_index(11, 1, &[1]).unwrap();

    let removed = docs.remap(&RowAddrRemap::direct(HashMap::from([
        (10, Some(20)),
        (11, None),
    ])));

    assert_eq!(removed, vec![2]);
    assert_eq!(docs.len(), 2);
    assert_eq!(docs.row_id(0), 20);
    assert_eq!(docs.row_id(1), 20);
    assert_eq!(docs.doc_index(0), vec![0]);
    assert_eq!(docs.doc_index(1), vec![3]);
}
