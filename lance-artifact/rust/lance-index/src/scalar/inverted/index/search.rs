// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

impl InvertedIndex {
    /// Build a single-segment [`MemBM25Scorer`] whose per-term IDF table
    /// covers every token that the per-partition scoring loop will look
    /// up. For fuzzy queries that means the union of Levenshtein
    /// expansions, not just the raw query tokens — otherwise
    /// `query_weight(expanded_token)` returns 0 and the BM25 contribution
    /// of every expanded match is discarded.
    pub async fn bm25_base_scorer(
        &self,
        query_tokens: &Tokens,
        params: &FtsSearchParams,
        metrics: Option<&dyn MetricsCollector>,
    ) -> Result<MemBM25Scorer> {
        if matches!(params.fuzziness, Some(n) if n != 0) {
            let expanded = self.expand_fuzzy_tokens(query_tokens, params)?;
            self.bm25_scorer_for_final_tokens(&expanded, metrics).await
        } else {
            self.bm25_scorer_for_final_tokens(query_tokens, metrics)
                .await
        }
    }

    /// Scorer for a token list that needs no further fuzzy expansion: dedup
    /// the terms and pull their document frequencies. `bm25_search` calls
    /// this with the tokens it already expanded, so the expansion runs once
    /// per query rather than once for the scorer and once per partition.
    async fn bm25_scorer_for_final_tokens(
        &self,
        tokens: &Tokens,
        metrics: Option<&dyn MetricsCollector>,
    ) -> Result<MemBM25Scorer> {
        let (total_tokens, num_docs) = self.aggregate_corpus_stats().await?;
        let mut terms: Vec<String> = Vec::new();
        let mut seen = HashSet::new();
        for token in tokens {
            if seen.insert(token.to_string()) {
                terms.push(token.to_string());
            }
        }
        let mut token_docs = HashMap::with_capacity(terms.len());
        for term in &terms {
            let df = self.df_for_term(term, metrics).await?;
            token_docs.insert(term.clone(), df);
        }
        Ok(MemBM25Scorer::new(total_tokens, num_docs, token_docs))
    }

    pub async fn bm25_stats_for_terms(
        &self,
        terms: &[String],
        metrics: Option<&dyn MetricsCollector>,
    ) -> Result<(u64, usize, Vec<usize>)> {
        let (total_tokens, num_docs) = self.aggregate_corpus_stats().await?;
        let token_docs =
            futures::future::try_join_all(terms.iter().map(|term| self.df_for_term(term, metrics)))
                .await?;
        Ok((total_tokens, num_docs, token_docs))
    }

    /// Aggregate immutable per-partition corpus statistics.  New modern files
    /// read both values from the already-opened docs footer; older partitioned
    /// files scan `_num_tokens` once as a compatibility fallback.
    pub(super) async fn aggregate_corpus_stats(&self) -> Result<(u64, usize)> {
        self.corpus_stats
            .get_or_try_init(|| async {
                let io_parallelism = self.store.io_parallelism();
                let futures = self
                    .partitions
                    .iter()
                    .map(|p| {
                        let part = p.clone();
                        async move { part.docs.stats().await }
                    })
                    .collect::<Vec<_>>();
                let stats = stream::iter(futures)
                    .buffer_unordered(io_parallelism)
                    .try_collect::<Vec<_>>()
                    .await?;
                let mut total_tokens = 0_u64;
                let mut num_docs = 0_usize;
                for stat in stats {
                    total_tokens = total_tokens
                        .checked_add(stat.total_tokens)
                        .ok_or_else(|| Error::index("FTS corpus token count overflows u64"))?;
                    num_docs = num_docs
                        .checked_add(stat.num_docs)
                        .ok_or_else(|| Error::index("FTS corpus document count overflows usize"))?;
                }
                Ok((total_tokens, num_docs))
            })
            .await
            .copied()
    }

    /// Sum the posting-list length for `term` across this index's partitions
    /// via single-row reads, with partition lookups bounded by the store's
    /// `io_parallelism()`.
    async fn df_for_term(
        &self,
        term: &str,
        metrics: Option<&dyn MetricsCollector>,
    ) -> Result<usize> {
        let io_parallelism = self.store.io_parallelism();
        let futures = self
            .partitions
            .iter()
            .map(|part| {
                let part = part.clone();
                async move {
                    match part.tokens.get(term) {
                        Some(token_id) => {
                            part.inverted_list
                                .posting_len_for_token(token_id, metrics)
                                .await
                        }
                        None => Ok(0),
                    }
                }
            })
            .collect::<Vec<_>>();
        let dfs: Vec<usize> = stream::iter(futures)
            .buffer_unordered(io_parallelism)
            .try_collect()
            .await?;
        Ok(dfs.into_iter().sum())
    }

    /// Expand fuzzy query tokens against all partitions in this segment.
    ///
    /// `params.max_expansions` caps the whole query's expansion, not any
    /// single partition's: for each query token the per-partition candidates
    /// (each streamed in FST key order) merge into one lexicographically
    /// ordered set, and the remaining budget takes a prefix of it. The
    /// selected terms are a pure function of the segment's vocabulary, so
    /// splitting the same corpus into more partitions cannot change which
    /// terms a fuzzy query matches.
    pub fn expand_fuzzy_tokens(&self, tokens: &Tokens, params: &FtsSearchParams) -> Result<Tokens> {
        let mut expanded_tokens = Vec::new();
        let mut expanded_positions = Vec::new();
        let mut seen = HashSet::new();
        for token_idx in 0..tokens.len() {
            let remaining = params.max_expansions.saturating_sub(expanded_tokens.len());
            if remaining == 0 {
                break;
            }
            let token = tokens.get_token(token_idx);
            let position = tokens.position(token_idx);
            // Each partition contributes at most its `remaining`
            // lexicographically smallest candidates, so the global
            // lex-smallest `remaining` selection below is unaffected by the
            // per-partition truncation.
            let mut candidates = BTreeSet::new();
            let base_prefix_len = tokens.token_type().prefix_len(token) as u32;
            for partition in &self.partitions {
                partition.collect_fuzzy_candidates(
                    token,
                    base_prefix_len,
                    params,
                    remaining,
                    &mut candidates,
                )?;
            }
            for candidate in candidates {
                if expanded_tokens.len() >= params.max_expansions {
                    break;
                }
                if seen.insert((candidate.clone(), position)) {
                    expanded_tokens.push(candidate);
                    expanded_positions.push(position);
                }
            }
        }
        Ok(Tokens::with_positions(
            expanded_tokens,
            expanded_positions,
            tokens.token_type().clone(),
        ))
    }

    /// Search documents that match the query and return row ids sorted by BM25 score.
    ///
    /// When `base_scorer` is provided, search uses those corpus-level BM25 statistics
    /// instead of deriving them from this segment alone.
    #[instrument(level = "debug", skip_all)]
    pub async fn bm25_search(
        &self,
        tokens: Arc<Tokens>,
        params: Arc<FtsSearchParams>,
        operator: Operator,
        prefilter: Arc<dyn PreFilter>,
        metrics: Arc<dyn MetricsCollector>,
        base_scorer: Option<&MemBM25Scorer>,
    ) -> Result<(Vec<u64>, Vec<f32>)> {
        let documents = self
            .bm25_search_documents(tokens, params, operator, prefilter, metrics, base_scorer)
            .await?;
        Ok(documents
            .into_iter()
            .map(|document| (document.row_id, document.score.0))
            .unzip())
    }

    /// Search logical FTS documents, retaining element coordinates when present.
    #[instrument(level = "debug", skip_all)]
    pub async fn bm25_search_documents(
        &self,
        tokens: Arc<Tokens>,
        params: Arc<FtsSearchParams>,
        operator: Operator,
        prefilter: Arc<dyn PreFilter>,
        metrics: Arc<dyn MetricsCollector>,
        base_scorer: Option<&MemBM25Scorer>,
    ) -> Result<Vec<ScoredDoc>> {
        // Fuzzy expansion runs once here, with the global `max_expansions`
        // budget, instead of once per partition: partitions receive the
        // final token list, so the matched terms cannot depend on how the
        // corpus happens to be partitioned.
        let tokens = if matches!(params.fuzziness, Some(n) if n != 0) {
            let expanded = Arc::new(self.expand_fuzzy_tokens(tokens.as_ref(), params.as_ref())?);
            if operator == Operator::And || params.phrase_slop.is_some() {
                // AND/phrase semantics require every original token position
                // to keep at least one expansion; a position that expands to
                // nothing anywhere in the segment can never be matched.
                let surviving = (0..expanded.len())
                    .map(|idx| expanded.position(idx))
                    .collect::<HashSet<_>>();
                if (0..tokens.len()).any(|idx| !surviving.contains(&tokens.position(idx))) {
                    return Ok(Vec::new());
                }
            }
            expanded
        } else {
            tokens
        };

        // The wand only consults `scorer.doc_weight`, which is metadata-free.
        // The outer aggregation below consults `scorer.query_weight`, which
        // hits per-token `posting_len`; building a `MemBM25Scorer` with
        // precomputed per-term IDFs avoids the v2 bulk metadata pull.
        let local_scorer;
        let scorer: &MemBM25Scorer = if let Some(base_scorer) = base_scorer {
            base_scorer
        } else {
            local_scorer = self
                .bm25_scorer_for_final_tokens(tokens.as_ref(), Some(metrics.as_ref()))
                .await?;
            &local_scorer
        };
        let impact_scorer = Arc::new(scorer.clone());

        let limit = params.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mask = prefilter.mask();
        if self.is_legacy() {
            let (row_ids, scores) = self
                .bm25_search_legacy(
                    tokens,
                    params,
                    operator,
                    mask,
                    metrics,
                    scorer,
                    impact_scorer,
                    limit,
                )
                .await?;
            Ok(row_ids
                .into_iter()
                .zip(scores)
                .map(|(row_id, score)| ScoredDoc::new(row_id, score))
                .collect())
        } else {
            self.bm25_search_modern(ModernSearchRequest {
                tokens,
                params,
                operator,
                mask,
                metrics,
                scorer,
                impact_scorer,
                limit,
            })
            .await
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn bm25_search_legacy(
        &self,
        tokens: Arc<Tokens>,
        params: Arc<FtsSearchParams>,
        operator: Operator,
        mask: Arc<RowAddrMask>,
        metrics: Arc<dyn MetricsCollector>,
        scorer: &MemBM25Scorer,
        impact_scorer: Arc<MemBM25Scorer>,
        limit: usize,
    ) -> Result<(Vec<u64>, Vec<f32>)> {
        let impact_shared_threshold = Arc::new(AtomicU32::new(f32::NEG_INFINITY.to_bits()));
        let io_parallelism = self.store.io_parallelism();
        let parts = self
            .partitions
            .chunks(fts_search_chunk())
            .map(|chunk| {
                let chunk = chunk.to_vec();
                let tokens = tokens.clone();
                let params = params.clone();
                let mask = mask.clone();
                let metrics = metrics.clone();
                let impact_scorer = impact_scorer.clone();
                let impact_shared_threshold = impact_shared_threshold.clone();
                async move {
                    let loads = chunk.into_iter().map(|part| {
                        let tokens = tokens.clone();
                        let params = params.clone();
                        let metrics = metrics.clone();
                        let impact_scorer = impact_scorer.clone();
                        let impact_shared_threshold = impact_shared_threshold.clone();
                        async move {
                            let LoadedPostings {
                                postings,
                                grouped_expansions,
                                impact_safe,
                                exact_scoring_required,
                            } = part
                                .load_posting_lists(
                                    tokens.as_ref(),
                                    params.as_ref(),
                                    operator,
                                    impact_scorer.as_ref(),
                                    metrics.as_ref(),
                                    false,
                                )
                                .await?;
                            if postings.is_empty() {
                                return Result::Ok(None);
                            }
                            let max_position = postings
                                .iter()
                                .map(|posting| posting.term_index() as usize)
                                .max()
                                .unwrap_or_default();
                            let mut tokens_by_position = vec![String::new(); max_position + 1];
                            for posting in &postings {
                                tokens_by_position[posting.term_index() as usize] =
                                    posting.token().to_owned();
                            }
                            let docs = part.docs.legacy().cloned().ok_or_else(|| {
                                Error::internal("legacy index contains modern partition documents")
                            })?;
                            let use_global_scorer = impact_safe || exact_scoring_required;
                            let threshold = if use_global_scorer {
                                impact_shared_threshold
                            } else {
                                Arc::new(AtomicU32::new(f32::NEG_INFINITY.to_bits()))
                            };
                            let wand_scorer = use_global_scorer.then(|| impact_scorer.clone());
                            Result::Ok(Some((
                                part,
                                docs,
                                postings,
                                wand_scorer,
                                threshold,
                                tokens_by_position,
                                grouped_expansions,
                            )))
                        }
                    });
                    let loaded = stream::iter(loads)
                        .buffer_unordered(io_parallelism)
                        .try_collect::<Vec<_>>()
                        .await?
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>();
                    if loaded.is_empty() {
                        return Result::Ok(Vec::new());
                    }

                    let results = spawn_cpu(move || {
                        let mut results = Vec::with_capacity(loaded.len());
                        for (
                            part,
                            docs,
                            postings,
                            wand_scorer,
                            threshold,
                            tokens_by_position,
                            grouped_expansions,
                        ) in loaded
                        {
                            let candidates = part.bm25_search_legacy(
                                docs.as_ref(),
                                params.as_ref(),
                                operator,
                                mask.as_ref(),
                                postings,
                                wand_scorer,
                                metrics.as_ref(),
                                threshold,
                            )?;
                            results.push(PartitionCandidates {
                                tokens_by_position,
                                grouped_expansions,
                                candidates,
                            });
                        }
                        Result::Ok(results)
                    })
                    .await?;
                    Result::Ok(results)
                }
            })
            .collect::<Vec<_>>();

        let mut ranked = BinaryHeap::new();
        let mut idf_cache = HashMap::new();
        let mut parts = stream::iter(parts)
            .buffer_unordered(get_num_compute_intensive_cpus().min(32))
            .map_ok(|results| stream::iter(results.into_iter().map(Result::Ok)))
            .try_flatten();
        while let Some(partition) = parts.try_next().await? {
            for (row_id, score) in rescore_partition_candidates(partition, scorer, &mut idf_cache) {
                push_scored_key(&mut ranked, limit, row_id, score);
            }
        }
        Ok(ranked
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse(doc)| (doc.row_id, doc.score.0))
            .unzip())
    }

    pub(super) async fn bm25_search_modern(
        &self,
        request: ModernSearchRequest<'_>,
    ) -> Result<Vec<ScoredDoc>> {
        // Select a concrete completion path before candidate search.  The
        // fully resident future never builds deferred address-read state, while
        // a cold query keeps DocIds until its final bounded I/O phase.
        if self.has_resident_document_projections() {
            self.bm25_search_modern_resident(request).await
        } else {
            self.bm25_search_modern_deferred(request).await
        }
    }

    pub(super) fn has_resident_document_projections(&self) -> bool {
        if self.document_projections_resident.load(Ordering::Acquire) {
            return true;
        }
        let resident = self.document_projections_resident_now();
        if resident {
            self.document_projections_resident
                .store(true, Ordering::Release);
        }
        resident
    }

    pub(super) fn document_projections_resident_now(&self) -> bool {
        self.partitions.iter().all(|partition| {
            partition
                .docs
                .modern()
                .is_some_and(|documents| documents.projection_resident())
        })
    }

    async fn bm25_search_modern_resident(
        &self,
        request: ModernSearchRequest<'_>,
    ) -> Result<Vec<ScoredDoc>> {
        let ranked = self.bm25_search_modern_candidates(request).await?;
        if let Some(result) = self.resolve_resident_modern_candidates(&ranked)? {
            return Ok(result);
        }
        self.document_projections_resident
            .store(false, Ordering::Release);
        self.resolve_deferred_modern_candidates(ranked).await
    }

    async fn bm25_search_modern_deferred(
        &self,
        request: ModernSearchRequest<'_>,
    ) -> Result<Vec<ScoredDoc>> {
        // Old partitioned files without persisted stats populate their
        // fallback stats before deferred candidate orchestration.  A resident
        // search can skip this full-index synchronization: standard prewarm
        // has already initialized it, while any independently resident
        // partition loads its lengths before constructing a local scorer.
        if self.corpus_stats.get().is_none() {
            self.aggregate_corpus_stats().await?;
        }
        // For new-format indexes, aggregate_corpus_stats reads corpus stats from
        // persisted schema metadata (O(1)) without loading doc lengths as a side
        // effect. Pre-load lengths in parallel now for partitions that contain at
        // least one query token, so the scoring phase gets cache hits instead of
        // issuing sequential per-partition IO.  Partitions with no matching terms
        // are skipped to preserve the no-load optimization for no-hit queries.
        let io_parallelism = self.store.io_parallelism();
        let uncached_lengths = self
            .partitions
            .iter()
            .filter_map(|part| {
                let docs = part.docs.modern()?.clone();
                if docs.cached_lengths().is_some() {
                    return None;
                }
                let has_match = (0..request.tokens.len())
                    .any(|i| part.tokens.get(request.tokens.get_token(i)).is_some());
                has_match.then_some(async move { docs.lengths().await.map(|_| ()) })
            })
            .collect::<Vec<_>>();
        if !uncached_lengths.is_empty() {
            stream::iter(uncached_lengths)
                .buffer_unordered(io_parallelism)
                .try_collect::<Vec<_>>()
                .await?;
        }
        let ranked = self.bm25_search_modern_candidates(request).await?;
        self.resolve_deferred_modern_candidates(ranked).await
    }

    async fn bm25_search_modern_candidates(
        &self,
        request: ModernSearchRequest<'_>,
    ) -> Result<Vec<Reverse<ScoredPartitionDoc>>> {
        let ModernSearchRequest {
            tokens,
            params,
            operator,
            mask,
            metrics,
            scorer,
            impact_scorer,
            limit,
        } = request;
        if self.partitions.len() > u32::MAX as usize {
            return Err(Error::index(format!(
                "FTS partition count {} exceeds candidate identity capacity",
                self.partitions.len()
            )));
        }
        let impact_shared_threshold = Arc::new(AtomicU32::new(f32::NEG_INFINITY.to_bits()));
        let io_parallelism = self.store.io_parallelism();
        let parts = self
            .partitions
            .chunks(fts_search_chunk())
            .enumerate()
            .map(|(chunk_ordinal, chunk)| {
                let first_partition_ordinal = chunk_ordinal * fts_search_chunk();
                let chunk = chunk
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(offset, part)| (first_partition_ordinal + offset, part))
                    .collect::<Vec<_>>();
                let tokens = tokens.clone();
                let params = params.clone();
                let mask = mask.clone();
                let metrics = metrics.clone();
                let impact_scorer = impact_scorer.clone();
                let impact_shared_threshold = impact_shared_threshold.clone();
                async move {
                    let loads = chunk.into_iter().map(|(partition_ordinal, part)| {
                        let tokens = tokens.clone();
                        let params = params.clone();
                        let mask = mask.clone();
                        let metrics = metrics.clone();
                        let impact_scorer = impact_scorer.clone();
                        let impact_shared_threshold = impact_shared_threshold.clone();
                        async move {
                            let LoadedPostings {
                                postings,
                                grouped_expansions,
                                impact_safe,
                                exact_scoring_required,
                            } = part
                                .load_posting_lists(
                                    tokens.as_ref(),
                                    params.as_ref(),
                                    operator,
                                    impact_scorer.as_ref(),
                                    metrics.as_ref(),
                                    false,
                                )
                                .await?;
                            if postings.is_empty() {
                                return Result::Ok(None);
                            }
                            let documents = part.docs.modern().cloned().ok_or_else(|| {
                                Error::internal("modern index contains legacy partition documents")
                            })?;
                            let materialize_selected = operator == Operator::Or
                                && mask.max_len().is_some_and(|selected| {
                                    u128::from(selected).saturating_mul(100)
                                        <= u128::from(*FLAT_SEARCH_PERCENT_THRESHOLD)
                                            .saturating_mul(documents.len() as u128)
                                });
                            let visibility = match documents
                                .immediate_visibility(mask.clone(), materialize_selected)
                            {
                                Some(visibility) => visibility,
                                None => {
                                    documents
                                        .visibility(mask.clone(), materialize_selected)
                                        .await?
                                }
                            };
                            if visibility.is_empty() {
                                return Result::Ok(None);
                            }
                            let lengths = match documents.cached_lengths() {
                                Some(lengths) => lengths,
                                None => documents.lengths().await?,
                            };
                            let max_position = postings
                                .iter()
                                .map(|posting| posting.term_index() as usize)
                                .max()
                                .unwrap_or_default();
                            let mut tokens_by_position = vec![String::new(); max_position + 1];
                            for posting in &postings {
                                tokens_by_position[posting.term_index() as usize] =
                                    posting.token().to_owned();
                            }
                            let use_global_scorer = impact_safe || exact_scoring_required;
                            let threshold = if use_global_scorer {
                                impact_shared_threshold
                            } else {
                                Arc::new(AtomicU32::new(f32::NEG_INFINITY.to_bits()))
                            };
                            let wand_scorer = use_global_scorer.then(|| impact_scorer.clone());
                            Result::Ok(Some((
                                partition_ordinal,
                                part,
                                lengths,
                                visibility,
                                postings,
                                wand_scorer,
                                threshold,
                                tokens_by_position,
                                grouped_expansions,
                            )))
                        }
                    });
                    let loaded = stream::iter(loads)
                        .buffer_unordered(io_parallelism)
                        .try_collect::<Vec<_>>()
                        .await?
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>();
                    if loaded.is_empty() {
                        return Result::Ok(Vec::new());
                    }

                    let results = spawn_cpu(move || {
                        let mut results = Vec::with_capacity(loaded.len());
                        for (
                            partition_ordinal,
                            part,
                            lengths,
                            visibility,
                            postings,
                            wand_scorer,
                            threshold,
                            tokens_by_position,
                            grouped_expansions,
                        ) in loaded
                        {
                            let candidates = part.bm25_search_modern(
                                lengths.as_ref(),
                                &visibility,
                                params.as_ref(),
                                operator,
                                postings,
                                wand_scorer,
                                metrics.as_ref(),
                                threshold,
                            )?;
                            results.push((
                                partition_ordinal,
                                PartitionCandidates {
                                    tokens_by_position,
                                    grouped_expansions,
                                    candidates,
                                },
                            ));
                        }
                        Result::Ok(results)
                    })
                    .await?;
                    Result::Ok(results)
                }
            })
            .collect::<Vec<_>>();

        let mut ranked = BinaryHeap::new();
        let mut idf_cache = HashMap::new();
        let mut parts = stream::iter(parts)
            .buffer_unordered(get_num_compute_intensive_cpus().min(32))
            .map_ok(|results| stream::iter(results.into_iter().map(Result::Ok)))
            .try_flatten();
        while let Some((partition_ordinal, partition)) = parts.try_next().await? {
            for (doc_id, score) in rescore_partition_candidates(partition, scorer, &mut idf_cache) {
                push_scored_partition_doc(
                    &mut ranked,
                    limit,
                    PartitionDocId::try_new(partition_ordinal, doc_id)?,
                    score,
                );
            }
        }

        Ok(ranked.into_sorted_vec())
    }

    fn resolve_resident_modern_candidates(
        &self,
        ranked: &[Reverse<ScoredPartitionDoc>],
    ) -> Result<Option<Vec<ScoredDoc>>> {
        if self.partitions.iter().any(|partition| {
            partition
                .docs
                .modern()
                .is_some_and(|documents| documents.coordinate_rank() > 0)
        }) {
            return Ok(None);
        }
        let mut resolved_documents = ranked
            .iter()
            .map(|Reverse(candidate)| ScoredDoc::new(0, candidate.score.0))
            .collect::<Vec<_>>();
        let mut by_partition = BTreeMap::<usize, Vec<(usize, DocId)>>::new();
        for (rank, Reverse(candidate)) in ranked.iter().enumerate() {
            let partition_ordinal = candidate.document.partition_ordinal();
            let doc_id = candidate.document.doc_id;
            by_partition
                .entry(partition_ordinal)
                .or_default()
                .push((rank, doc_id));
        }
        for (partition_ordinal, entries) in by_partition {
            let documents = self
                .partitions
                .get(partition_ordinal)
                .and_then(|partition| partition.docs.modern())
                .ok_or_else(|| {
                    Error::internal(format!(
                        "resident FTS candidates reference missing modern partition ordinal {partition_ordinal}"
                    ))
                })?;
            let doc_ids = entries
                .iter()
                .map(|(_, doc_id)| *doc_id)
                .collect::<Vec<_>>();
            let Some(resolved) = documents.cached_row_addresses(&doc_ids)? else {
                return Ok(None);
            };
            for ((rank, _), address) in entries.into_iter().zip(resolved) {
                resolved_documents[rank].row_id = address;
            }
        }
        Ok(Some(resolved_documents))
    }

    async fn resolve_deferred_modern_candidates(
        &self,
        ranked: Vec<Reverse<ScoredPartitionDoc>>,
    ) -> Result<Vec<ScoredDoc>> {
        let mut resolved_documents = ranked
            .iter()
            .map(|Reverse(candidate)| ScoredDoc::new(0, candidate.score.0))
            .collect::<Vec<_>>();
        let mut by_partition = BTreeMap::<usize, Vec<(usize, DocId)>>::new();
        for (rank, Reverse(candidate)) in ranked.iter().enumerate() {
            let partition_ordinal = candidate.document.partition_ordinal();
            let doc_id = candidate.document.doc_id;
            by_partition
                .entry(partition_ordinal)
                .or_default()
                .push((rank, doc_id));
        }
        let mut address_reads = Vec::with_capacity(by_partition.len());
        let mut largest_read_bytes = 0;
        for (partition_ordinal, entries) in by_partition {
            let documents = self
                .partitions
                .get(partition_ordinal)
                .and_then(|partition| partition.docs.modern())
                .cloned()
                .ok_or_else(|| {
                    Error::internal(format!(
                        "deferred FTS candidates reference missing modern partition ordinal {partition_ordinal}"
                    ))
                })?;
            let doc_ids = entries
                .iter()
                .map(|(_, doc_id)| *doc_id)
                .collect::<Vec<_>>();
            largest_read_bytes =
                largest_read_bytes.max(documents.estimated_address_read_bytes(&doc_ids));
            address_reads.push(async move {
                let resolved = documents.resolve_document_keys(&doc_ids).await?;
                Result::Ok((entries, resolved))
            });
        }
        let concurrency = address_read_concurrency(self.store.io_parallelism(), largest_read_bytes);
        let mut address_reads = stream::iter(address_reads).buffer_unordered(concurrency);
        while let Some((entries, resolved)) = address_reads.try_next().await? {
            for ((rank, _), (row_id, doc_index)) in entries.into_iter().zip(resolved) {
                resolved_documents[rank].row_id = row_id;
                resolved_documents[rank].doc_index = doc_index;
            }
        }
        Ok(resolved_documents)
    }
}
