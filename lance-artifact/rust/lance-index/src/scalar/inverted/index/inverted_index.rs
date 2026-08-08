// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

#[derive(Debug, Default)]
pub(in super::super) struct InvertedPrewarmState {
    query_ready: bool,
    positions_ready: bool,
}

impl InvertedPrewarmState {
    pub(super) fn satisfies(&self, with_position: bool) -> bool {
        self.query_ready && (!with_position || self.positions_ready)
    }
}

#[derive(Clone)]
pub struct InvertedIndex {
    pub(super) params: InvertedIndexParams,
    pub(super) store: Arc<dyn IndexStore>,
    pub(super) tokenizer: Box<dyn LanceTokenizer>,
    pub(super) token_set_format: TokenSetFormat,
    pub(super) format_version: InvertedListFormatVersion,
    pub(crate) partitions: Vec<Arc<InvertedPartition>>,
    pub(super) corpus_stats: Arc<OnceCell<(u64, usize)>>,
    pub(super) prewarm_state: Arc<Mutex<InvertedPrewarmState>>,
    /// Optimistic fast-path hint. Cache eviction can make it stale; the
    /// resident resolver clears it when a weak projection upgrade misses.
    pub(super) document_projections_resident: Arc<AtomicBool>,
    // Fragments which are contained in the index, but no longer in the dataset.
    // These should be pruned at search time since we don't prune them at update time.
    pub(super) deleted_fragments: RoaringBitmap,
}

impl Debug for InvertedIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InvertedIndex")
            .field("params", &self.params)
            .field("token_set_format", &self.token_set_format)
            .field("format_version", &self.format_version)
            .field("partitions", &self.partitions)
            .field("deleted_fragments", &self.deleted_fragments)
            .finish()
    }
}

impl DeepSizeOf for InvertedIndex {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.partitions.deep_size_of_children(context)
    }
}

impl InvertedIndex {
    pub(super) fn format_version(&self) -> InvertedListFormatVersion {
        self.format_version
    }

    pub(super) fn index_version(&self) -> u32 {
        if self.params.get_document_granularity().is_list_element() {
            return INVERTED_INDEX_VERSION_V3;
        }
        match (self.token_set_format, self.format_version()) {
            (
                TokenSetFormat::Arrow,
                InvertedListFormatVersion::V1 | InvertedListFormatVersion::V2,
            ) => 0,
            (_, format_version) => format_version.index_version(),
        }
    }

    fn posting_tail_codec(&self) -> PostingTailCodec {
        self.partitions
            .first()
            .map(|partition| partition.inverted_list.posting_tail_codec())
            .unwrap_or_default()
    }

    fn to_builder(&self) -> InvertedIndexBuilder {
        self.to_builder_with_offset(None)
    }

    fn to_builder_with_offset(&self, fragment_mask: Option<u64>) -> InvertedIndexBuilder {
        if self.is_legacy() {
            // for legacy format, we re-create the index in the new format
            InvertedIndexBuilder::from_existing_index(
                self.params.clone(),
                None,
                Vec::new(),
                self.token_set_format,
                fragment_mask,
                self.deleted_fragments.clone(),
            )
            .with_posting_tail_codec(self.posting_tail_codec())
        } else {
            let partitions = match fragment_mask {
                Some(fragment_mask) => self
                    .partitions
                    .iter()
                    // Filter partitions that belong to the specified fragment
                    // The mask contains fragment_id in high 32 bits, we check if partition's
                    // fragment_id matches by comparing the masked result with the original mask
                    .filter(|part| part.belongs_to_fragment(fragment_mask))
                    .map(|part| part.id())
                    .collect(),
                None => self.partitions.iter().map(|part| part.id()).collect(),
            };

            InvertedIndexBuilder::from_existing_index(
                self.params.clone(),
                Some(self.store.clone()),
                partitions,
                self.token_set_format,
                fragment_mask,
                self.deleted_fragments.clone(),
            )
            .with_format_version(self.format_version())
        }
    }

    pub fn tokenizer(&self) -> Box<dyn LanceTokenizer> {
        self.tokenizer.clone()
    }

    pub fn params(&self) -> &InvertedIndexParams {
        &self.params
    }

    /// Returns the number of partitions in this inverted index.
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }
    /// Returns the set of fragments which are contained in the index, but no longer in the dataset.
    ///
    /// Most other indices remove data from deleted fragments when the index updates (copy-on-write).
    /// However, this would require an expensive copy of the FTS index.  Instead, we track the deleted
    /// fragments and prune them at search time (merge-on-read).
    pub fn deleted_fragments(&self) -> &RoaringBitmap {
        &self.deleted_fragments
    }

    pub async fn merge_segments(
        segments: &[Arc<Self>],
        new_data: SendableRecordBatchStream,
        dest_store: &dyn IndexStore,
        old_data_filter: Option<OldIndexDataFilter>,
        progress: Arc<dyn IndexBuildProgress>,
    ) -> Result<CreatedIndex> {
        let Some(first) = segments.first() else {
            return Err(Error::invalid_input(
                "cannot merge inverted index without at least one source segment".to_string(),
            ));
        };

        for segment in segments.iter().skip(1) {
            if segment.params != first.params {
                return Err(Error::index(
                    "cannot merge inverted index segments with different parameters".to_string(),
                ));
            }
            if segment.token_set_format != first.token_set_format {
                return Err(Error::index(
                    "cannot merge inverted index segments with different token set formats"
                        .to_string(),
                ));
            }
            if segment.format_version() != first.format_version() {
                return Err(Error::index(
                    "cannot merge inverted index segments with different format versions"
                        .to_string(),
                ));
            }
            if segment.posting_tail_codec() != first.posting_tail_codec() {
                return Err(Error::index(
                    "cannot merge inverted index segments with different posting tail codecs"
                        .to_string(),
                ));
            }
        }

        let mut builder = InvertedIndexBuilder::new(first.params.clone()).with_progress(progress);
        builder = builder
            .with_token_set_format(first.token_set_format)
            .with_format_version(first.format_version());
        let files = builder
            .update_from_segments(new_data, dest_store, segments, old_data_filter)
            .await?;

        let details = pbold::InvertedIndexDetails::try_from(&first.params)?;

        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&details).unwrap(),
            index_version: first.index_version(),
            files,
        })
    }
}

impl InvertedIndex {
    async fn load_legacy_index(
        store: Arc<dyn IndexStore>,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        index_cache: &LanceCache,
    ) -> Result<Arc<Self>> {
        log::warn!("loading legacy FTS index");
        let tokens_fut = tokio::spawn({
            let store = store.clone();
            async move {
                let token_reader = store.open_index_file(TOKENS_FILE).await?;
                let tokenizer = token_reader
                    .schema()
                    .metadata
                    .get("tokenizer")
                    .map(|s| serde_json::from_str::<InvertedIndexParams>(s))
                    .transpose()?
                    .unwrap_or_default();
                let tokens = TokenSet::load(token_reader, TokenSetFormat::Arrow).await?;
                Result::Ok((tokenizer, tokens))
            }
        });
        let invert_list_fut = tokio::spawn({
            let store = store.clone();
            let index_cache_clone = index_cache.clone();
            async move {
                let invert_list_reader = store.open_index_file(INVERT_LIST_FILE).await?;
                let invert_list =
                    PostingListReader::try_new(invert_list_reader, &index_cache_clone).await?;
                Result::Ok(Arc::new(invert_list))
            }
        });
        let docs_fut = tokio::spawn({
            let store = store.clone();
            async move {
                let docs_reader = store.open_index_file(DOCS_FILE).await?;
                let docs = DocSet::load(docs_reader, true, frag_reuse_index).await?;
                Result::Ok(docs)
            }
        });

        let (tokenizer_config, tokens) = tokens_fut.await??;
        let inverted_list = invert_list_fut.await??;
        let docs = docs_fut.await??;

        let tokenizer = tokenizer_config.build()?;

        Ok(Arc::new(Self {
            params: tokenizer_config,
            store: store.clone(),
            tokenizer,
            token_set_format: TokenSetFormat::Arrow,
            format_version: InvertedListFormatVersion::V1,
            partitions: vec![Arc::new(InvertedPartition {
                id: 0,
                store,
                tokens,
                inverted_list,
                docs: PartitionDocumentStore::Legacy(Arc::new(docs)),
                token_set_format: TokenSetFormat::Arrow,
            })],
            corpus_stats: Arc::new(OnceCell::new()),
            prewarm_state: Arc::new(Mutex::new(InvertedPrewarmState::default())),
            document_projections_resident: Arc::new(AtomicBool::new(false)),
            deleted_fragments: RoaringBitmap::new(),
        }))
    }

    pub fn is_legacy(&self) -> bool {
        self.partitions.len() == 1 && self.partitions[0].docs.legacy().is_some()
    }

    /// Read only the index's [`InvertedIndexParams`],
    /// Contains more complete info than manifest's lossy `InvertedIndexDetails`.
    pub async fn load_params(store: &dyn IndexStore) -> Result<InvertedIndexParams> {
        match store.open_index_file(METADATA_FILE).await {
            Ok(reader) => {
                let params = reader
                    .schema()
                    .metadata
                    .get("params")
                    .ok_or(Error::index("params not found in metadata".to_owned()))?;
                Ok(serde_json::from_str::<InvertedIndexParams>(params)?)
            }
            Err(metadata_error) => {
                // Legacy format: params live in the tokens file (see
                // `load_legacy_index`). Some S3 configurations return 403 for
                // a missing object, so the readable legacy file is the
                // authoritative format probe.
                let Ok(reader) = store.open_index_file(TOKENS_FILE).await else {
                    return Err(metadata_error);
                };
                Ok(reader
                    .schema()
                    .metadata
                    .get("tokenizer")
                    .map(|s| serde_json::from_str::<InvertedIndexParams>(s))
                    .transpose()?
                    .unwrap_or_default())
            }
        }
    }

    pub async fn load(
        store: Arc<dyn IndexStore>,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        index_cache: &LanceCache,
    ) -> Result<Arc<Self>>
    where
        Self: Sized,
    {
        // for new index format, there is a metadata file and multiple partitions,
        // each partition is a separate index containing tokens, inverted list and docs.
        // for old index format, there is no metadata file, and it's just like a single partition

        match store.open_index_file(METADATA_FILE).await {
            Ok(reader) => {
                let params = reader
                    .schema()
                    .metadata
                    .get("params")
                    .ok_or(Error::index("params not found in metadata".to_owned()))?;
                let mut params = serde_json::from_str::<InvertedIndexParams>(params)?;
                let partitions = reader
                    .schema()
                    .metadata
                    .get("partitions")
                    .ok_or(Error::index("partitions not found in metadata".to_owned()))?;
                let partitions: Vec<u64> = serde_json::from_str(partitions)?;
                let token_set_format = reader
                    .schema()
                    .metadata
                    .get(TOKEN_SET_FORMAT_KEY)
                    .map(|name| TokenSetFormat::from_str(name))
                    .transpose()?
                    .unwrap_or(TokenSetFormat::Arrow);
                let format_version = parse_format_version_from_metadata(&reader.schema().metadata)?;

                // Load deleted_fragments if present (optional for backward compatibility)
                let deleted_fragments = if reader.num_rows() > 0 {
                    let metadata_batch = reader.read_range(0..1, None).await?;
                    if let Some(col) = metadata_batch.column_by_name(DELETED_FRAGMENTS_COL) {
                        let arr = col.as_binary_opt::<i32>().expect_ok()?;
                        RoaringBitmap::deserialize_from(arr.value(0))?
                    } else {
                        RoaringBitmap::new()
                    }
                } else {
                    RoaringBitmap::new()
                };

                let format = token_set_format;
                let partitions = partitions.into_iter().enumerate().map(|(priority, id)| {
                    let store = store.with_io_priority(priority as u64);
                    let frag_reuse_index_clone = frag_reuse_index.clone();
                    let index_cache_for_part =
                        index_cache.with_key_prefix(format!("part-{}", id).as_str());
                    let token_set_format = format;
                    async move {
                        Result::Ok(Arc::new(
                            InvertedPartition::load(
                                store,
                                id,
                                frag_reuse_index_clone,
                                &index_cache_for_part,
                                token_set_format,
                            )
                            .await?,
                        ))
                    }
                });
                let partitions = stream::iter(partitions)
                    .buffer_unordered(store.io_parallelism())
                    .try_collect::<Vec<_>>()
                    .await?;

                let coordinate_rank = partitions
                    .first()
                    .map(|partition| partition.docs.coordinate_rank())
                    .unwrap_or(0);
                if partitions
                    .iter()
                    .any(|partition| partition.docs.coordinate_rank() != coordinate_rank)
                {
                    return Err(Error::index(
                        "FTS partitions have inconsistent document coordinate ranks".to_string(),
                    ));
                }
                params.document_granularity = if coordinate_rank == 0 {
                    DocumentGranularity::Row
                } else {
                    DocumentGranularity::ListElement
                };

                let tokenizer = params.build()?;
                Ok(Arc::new(Self {
                    params,
                    store,
                    tokenizer,
                    token_set_format,
                    format_version,
                    partitions,
                    corpus_stats: Arc::new(OnceCell::new()),
                    prewarm_state: Arc::new(Mutex::new(InvertedPrewarmState::default())),
                    document_projections_resident: Arc::new(AtomicBool::new(false)),
                    deleted_fragments,
                }))
            }
            Err(_) => {
                // old index format
                Self::load_legacy_index(store, frag_reuse_index, index_cache).await
            }
        }
    }
}

#[async_trait]
impl Index for InvertedIndex {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
        self
    }

    fn statistics(&self) -> Result<serde_json::Value> {
        let num_tokens = self
            .partitions
            .iter()
            .map(|part| part.tokens.len())
            .sum::<usize>();
        let num_docs = self
            .partitions
            .iter()
            .map(|part| part.docs.len())
            .sum::<usize>();
        Ok(serde_json::json!({
            "params": self.params,
            "num_tokens": num_tokens,
            "num_docs": num_docs,
        }))
    }

    async fn prewarm(&self) -> Result<()> {
        self.prewarm_with_options(&FtsPrewarmOptions::default())
            .await
    }

    fn index_type(&self) -> crate::IndexType {
        crate::IndexType::Inverted
    }

    async fn calculate_included_frags(&self) -> Result<RoaringBitmap> {
        unimplemented!()
    }
}

impl InvertedIndex {
    pub async fn prewarm_with_options(&self, options: &FtsPrewarmOptions) -> Result<()> {
        self.prewarm_with_options_result(options).await.map(|_| ())
    }

    pub async fn prewarm_with_options_result(
        &self,
        options: &FtsPrewarmOptions,
    ) -> Result<FtsPrewarmResult> {
        let mut state = self.prewarm_state.lock().await;
        if state.satisfies(options.with_position)
            && self
                .prewarm_diagnostics(options.with_position)
                .await
                .fully_resident()
        {
            return Ok(FtsPrewarmResult::fully_resident());
        }
        let with_position = options.with_position || state.positions_ready;
        state.query_ready = false;
        state.positions_ready = false;
        self.document_projections_resident
            .store(false, Ordering::Release);
        let result = self
            .prewarm_query_state(with_position, options.mode.is_best_effort())
            .await?;
        if result.fully_resident {
            state.query_ready = true;
            state.positions_ready = with_position;
        }
        Ok(result)
    }

    async fn prewarm_query_state(
        &self,
        with_position: bool,
        best_effort: bool,
    ) -> Result<FtsPrewarmResult> {
        let chunk_concurrency = self.store.io_parallelism().max(1);
        let prewarm_started = Instant::now();
        info!(
            partition_count = self.partitions.len(),
            with_position, best_effort, chunk_concurrency, "fts index prewarm started"
        );
        for part in &self.partitions {
            let partition_started = Instant::now();
            info!(
                partition_id = part.id(),
                token_count = part.tokens.len(),
                with_position,
                chunk_concurrency,
                "fts partition prewarm started"
            );
            if let Err(err) = part
                .inverted_list
                .prewarm_posting_lists(with_position, chunk_concurrency)
                .await
            {
                warn!(
                    partition_id = part.id(),
                    error = %err,
                    elapsed_ms = partition_started.elapsed().as_millis() as u64,
                    "fts partition posting list prewarm failed"
                );
                return Err(err);
            }
            info!(
                partition_id = part.id(),
                elapsed_ms = partition_started.elapsed().as_millis() as u64,
                "fts partition posting lists prewarmed"
            );
            let docs_started = Instant::now();
            if let Err(err) = part.docs.prewarm().await {
                warn!(
                    partition_id = part.id(),
                    error = %err,
                    elapsed_ms = docs_started.elapsed().as_millis() as u64,
                    total_elapsed_ms = partition_started.elapsed().as_millis() as u64,
                    "fts partition docset prewarm failed"
                );
                return Err(err);
            }
            info!(
                partition_id = part.id(),
                docset_elapsed_ms = docs_started.elapsed().as_millis() as u64,
                elapsed_ms = partition_started.elapsed().as_millis() as u64,
                "fts partition prewarm finished"
            );
        }
        self.aggregate_corpus_stats().await?;
        let diagnostics = self.prewarm_diagnostics(with_position).await;
        if !diagnostics.fully_resident() {
            if best_effort {
                warn!(
                    partition_count = diagnostics.partition_count,
                    failing_partition_count = diagnostics.failing_partitions.len(),
                    diagnostics = %diagnostics,
                    elapsed_ms = prewarm_started.elapsed().as_millis() as u64,
                    "fts index prewarm finished with partial residency"
                );
                return Ok(FtsPrewarmResult::partial(diagnostics));
            }
            return Err(Error::internal(diagnostics.to_string()));
        }
        self.document_projections_resident
            .store(true, Ordering::Release);
        info!(
            partition_count = self.partitions.len(),
            query_ready = true,
            elapsed_ms = prewarm_started.elapsed().as_millis() as u64,
            "fts index prewarm finished"
        );
        Ok(FtsPrewarmResult::fully_resident())
    }

    pub async fn prewarm_residency_result(&self, with_position: bool) -> FtsPrewarmResult {
        let diagnostics = self.prewarm_diagnostics(with_position).await;
        if diagnostics.fully_resident() {
            FtsPrewarmResult::fully_resident()
        } else {
            FtsPrewarmResult::partial(diagnostics)
        }
    }

    async fn prewarm_diagnostics(&self, with_position: bool) -> FtsPrewarmDiagnostics {
        let statuses = futures::future::join_all(self.partitions.iter().map(|partition| async {
            let (posting_resident, position_resident) = partition
                .inverted_list
                .prewarm_residency_status(with_position)
                .await;
            FtsPrewarmPartitionStatus {
                segment_id: None,
                partition_id: partition.id(),
                documents: partition.docs.prewarm_status(),
                posting_validation_ready: partition.inverted_list.modern_posting_validation_ready(),
                posting_resident,
                position_resident,
            }
        }))
        .await;
        let failing_partitions = statuses
            .into_iter()
            .filter(|status| !status.query_ready())
            .collect();
        FtsPrewarmDiagnostics {
            partition_count: self.partitions.len(),
            failing_segments: Vec::new(),
            failing_partitions,
        }
    }
    /// Search docs match the input text.
    async fn do_search(&self, text: &str) -> Result<RecordBatch> {
        let params = FtsSearchParams::new();
        let mut tokenizer = self.tokenizer.clone();
        let tokens = collect_query_tokens(text, &mut tokenizer);

        let (doc_ids, _) = self
            .bm25_search(
                Arc::new(tokens),
                params.into(),
                Operator::And,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .boxed()
            .await?;

        Ok(RecordBatch::try_new(
            ROW_ID_SCHEMA.clone(),
            vec![Arc::new(UInt64Array::from(doc_ids))],
        )?)
    }
}

#[async_trait]
impl ScalarIndex for InvertedIndex {
    // return the row ids of the documents that contain the query
    #[instrument(level = "debug", skip_all)]
    async fn search(
        &self,
        query: &dyn AnyQuery,
        _metrics: &dyn MetricsCollector,
    ) -> Result<SearchResult> {
        let query = query.as_any().downcast_ref::<TokenQuery>().unwrap();

        match query {
            TokenQuery::TokensContains(text) => {
                let records = self.do_search(text).await?;
                let row_ids = records
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                let row_ids = row_ids.iter().flatten().collect_vec();
                Ok(SearchResult::at_most(RowAddrTreeMap::from_iter(row_ids)))
            }
        }
    }

    fn can_remap(&self) -> bool {
        true
    }

    async fn remap(
        &self,
        mapping: &RowAddrRemap,
        dest_store: &dyn IndexStore,
    ) -> Result<CreatedIndex> {
        let files = self
            .to_builder()
            .remap(mapping, self.store.clone(), dest_store)
            .await?;

        let details = pbold::InvertedIndexDetails::try_from(&self.params)?;

        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&details).unwrap(),
            index_version: self.index_version(),
            files,
        })
    }

    async fn update(
        &self,
        new_data: SendableRecordBatchStream,
        dest_store: &dyn IndexStore,
        old_data_filter: Option<crate::scalar::OldIndexDataFilter>,
    ) -> Result<CreatedIndex> {
        let files = self
            .to_builder()
            .update(new_data, dest_store, old_data_filter)
            .await?;

        let details = pbold::InvertedIndexDetails::try_from(&self.params)?;

        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&details).unwrap(),
            index_version: self.index_version(),
            files,
        })
    }

    fn update_criteria(&self) -> UpdateCriteria {
        let criteria = TrainingCriteria::new(TrainingOrdering::None).with_row_id();
        if self.is_legacy() {
            UpdateCriteria::requires_old_data(criteria)
        } else {
            UpdateCriteria::only_new_data(criteria)
        }
    }

    fn derive_index_params(&self) -> Result<ScalarIndexParams> {
        let mut params = self.params.clone();
        if params.base_tokenizer.is_empty() {
            // Empty tokenizer metadata only appears in legacy simple-tokenizer indexes.
            params.base_tokenizer = "simple".to_string();
        }
        params = params.format_version(self.format_version());

        let params_json = params.to_training_json()?.to_string();

        Ok(ScalarIndexParams {
            index_type: BuiltinIndexType::Inverted.as_str().to_string(),
            params: Some(params_json),
        })
    }
}
