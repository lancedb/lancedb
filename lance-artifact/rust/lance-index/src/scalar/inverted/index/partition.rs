// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

#[derive(Debug, Clone, DeepSizeOf)]
pub struct InvertedPartition {
    // 0 for legacy format
    pub(super) id: u64,
    pub(super) store: Arc<dyn IndexStore>,
    pub(crate) tokens: TokenSet,
    pub(crate) inverted_list: Arc<PostingListReader>,
    /// Legacy documents stay in their original complete `DocSet`; modern
    /// documents use typed, independently-loaded lengths and addresses.
    pub(in super::super) docs: PartitionDocumentStore,
    pub(super) token_set_format: TokenSetFormat,
}

impl InvertedPartition {
    /// Check if this partition belongs to the specified fragment.
    ///
    /// This method encapsulates the bit manipulation logic for fragment filtering
    /// in distributed indexing scenarios.
    ///
    /// # Arguments
    /// * `fragment_mask` - A mask with fragment_id in high 32 bits
    ///
    /// # Returns
    /// * `true` if the partition belongs to the fragment, `false` otherwise
    pub fn belongs_to_fragment(&self, fragment_mask: u64) -> bool {
        (self.id() & fragment_mask) == fragment_mask
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn store(&self) -> &dyn IndexStore {
        self.store.as_ref()
    }

    pub fn is_legacy(&self) -> bool {
        self.inverted_list.is_legacy_layout()
    }

    pub async fn load(
        store: Arc<dyn IndexStore>,
        id: u64,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        index_cache: &LanceCache,
        token_set_format: TokenSetFormat,
    ) -> Result<Self> {
        let token_file = store.open_index_file(&token_file_path(id)).await?;
        let tokens = TokenSet::load(token_file, token_set_format).await?;
        let invert_list_file = store.open_index_file(&posting_file_path(id)).await?;
        let mut inverted_list = PostingListReader::try_new(invert_list_file, index_cache).await?;
        let docs_path = doc_file_path(id);
        let docs_reader = store.open_index_file(&docs_path).await?;
        let docs = PartitionDocuments::try_new(
            store.clone(),
            docs_path,
            id,
            WeakLanceCache::from(index_cache),
            docs_reader.as_ref(),
            frag_reuse_index,
            // 256-document blocks score with quantized document lengths.
            inverted_list.block_size() == MAX_POSTING_BLOCK_SIZE,
        )?;
        inverted_list.modern_num_docs = Some(docs.len());

        Ok(Self {
            id,
            store,
            tokens,
            inverted_list: Arc::new(inverted_list),
            docs: PartitionDocumentStore::Modern(Arc::new(docs)),
            token_set_format,
        })
    }

    fn map(&self, token: &str) -> Option<u32> {
        self.tokens.get(token)
    }

    pub fn expand_fuzzy(&self, tokens: &Tokens, params: &FtsSearchParams) -> Result<Tokens> {
        let mut new_tokens = Vec::with_capacity(min(tokens.len(), params.max_expansions));
        let mut new_positions = Vec::with_capacity(new_tokens.capacity());
        let mut seen = HashSet::new();
        for token_idx in 0..tokens.len() {
            let remaining = params.max_expansions.saturating_sub(new_tokens.len());
            if remaining == 0 {
                break;
            }
            let token = tokens.get_token(token_idx);
            let position = tokens.position(token_idx);
            let base_prefix_len = tokens.token_type().prefix_len(token) as u32;
            let mut candidates = BTreeSet::new();
            self.collect_fuzzy_candidates(
                token,
                base_prefix_len,
                params,
                remaining,
                &mut candidates,
            )?;
            for candidate in candidates {
                if new_tokens.len() >= params.max_expansions {
                    break;
                }
                if seen.insert((candidate.clone(), position)) {
                    new_tokens.push(candidate);
                    new_positions.push(position);
                }
            }
        }
        Ok(Tokens::with_positions(
            new_tokens,
            new_positions,
            tokens.token_type().clone(),
        ))
    }

    /// Collect up to `limit` fuzzy candidates for one query token from this
    /// partition's token FST, in key (lexicographic) order. Callers merge
    /// candidates across partitions and apply the query-wide
    /// `max_expansions` budget; truncating each partition at `limit` is
    /// lossless for that selection because any term among the merged
    /// lexicographically-smallest `limit` is also among its own partition's
    /// smallest `limit`.
    pub(super) fn collect_fuzzy_candidates(
        &self,
        token: &str,
        base_prefix_len: u32,
        params: &FtsSearchParams,
        limit: usize,
        candidates: &mut BTreeSet<String>,
    ) -> Result<()> {
        let fuzziness = match params.fuzziness {
            Some(fuzziness) => fuzziness,
            None => MatchQuery::auto_fuzziness(token),
        };
        let lev = fst::automaton::Levenshtein::new(token, fuzziness)
            .map_err(|e| Error::index(format!("failed to construct the fuzzy query: {}", e)))?;

        if let TokenMap::Fst(ref map) = self.tokens.tokens {
            let mut expanded = Vec::new();
            match base_prefix_len + params.prefix_length {
                0 => take_fst_keys(map.search(lev), &mut expanded, limit),
                prefix_length => {
                    let prefix = &token[..min(prefix_length as usize, token.len())];
                    let prefix = fst::automaton::Str::new(prefix).starts_with();
                    take_fst_keys(map.search(lev.intersection(prefix)), &mut expanded, limit)
                }
            }
            candidates.extend(expanded);
            Ok(())
        } else {
            Err(Error::index(
                "tokens is not fst, which is not expected".to_owned(),
            ))
        }
    }

    #[inline]
    fn grouped_score_upper_bound(
        query_weight: f32,
        union_freq: u32,
        doc_length: u32,
        scorer: &MemBM25Scorer,
    ) -> f32 {
        // BM25's document weight is monotonic in frequency and every IDF is
        // non-negative. Scoring the summed frequency with the summed IDF is
        // therefore an upper bound on the sum of the individual term scores.
        query_weight * scorer.doc_weight(union_freq, doc_length)
    }

    fn grouped_block_max_scores(
        doc_ids: &[u32],
        frequencies: &[u32],
        block_size: usize,
        docs: &LoadedDocLengths,
        query_weight: f32,
        scorer: &MemBM25Scorer,
    ) -> Vec<f32> {
        doc_ids
            .chunks(block_size)
            .zip(frequencies.chunks(block_size))
            .map(|(doc_ids, frequencies)| {
                doc_ids
                    .iter()
                    .zip(frequencies)
                    .map(|(doc_id, freq)| {
                        Self::grouped_score_upper_bound(
                            query_weight,
                            *freq,
                            docs.scoring_num_tokens(*doc_id),
                            scorer,
                        )
                    })
                    .fold(0.0, f32::max)
            })
            .collect()
    }

    fn union_plain_posting_lists(
        postings: Vec<PostingList>,
        docs: &LoadedDocLengths,
        query_weight: f32,
        scorer: &MemBM25Scorer,
    ) -> Result<PostingList> {
        let mut freqs_by_row_id = BTreeMap::new();
        for posting in postings {
            for (row_id, freq, _) in posting.iter() {
                let entry = freqs_by_row_id.entry(row_id).or_insert(0u32);
                *entry = entry.checked_add(freq).ok_or_else(|| {
                    Error::index(format!("posting frequency overflow for row id {}", row_id))
                })?;
            }
        }
        let mut row_ids = Vec::with_capacity(freqs_by_row_id.len());
        let mut frequencies = Vec::with_capacity(freqs_by_row_id.len());
        let mut max_score = 0.0_f32;
        for (row_id, freq) in freqs_by_row_id {
            max_score = max_score.max(Self::grouped_score_upper_bound(
                query_weight,
                freq,
                docs.num_tokens_by_row_id(row_id),
                scorer,
            ));
            row_ids.push(row_id);
            frequencies.push(freq as f32);
        }
        Ok(PostingList::Plain(PlainPostingList::new(
            ScalarBuffer::from(row_ids),
            ScalarBuffer::from(frequencies),
            Some(max_score),
            None,
        )))
    }

    fn union_plain_posting_lists_with_positions(
        postings: Vec<PostingList>,
        docs: &LoadedDocLengths,
        query_weight: f32,
        scorer: &MemBM25Scorer,
    ) -> Result<PostingList> {
        let mut positions_by_row_id = BTreeMap::<u64, Vec<u32>>::new();
        for posting in postings {
            for (row_id, _, positions) in posting.iter() {
                let positions = positions.ok_or_else(|| {
                    Error::index("cannot union grouped phrase terms without positions".to_string())
                })?;
                positions_by_row_id
                    .entry(row_id)
                    .or_default()
                    .extend(positions);
            }
        }
        if positions_by_row_id.is_empty() {
            return Ok(PostingList::Plain(PlainPostingList::new(
                ScalarBuffer::from(Vec::<u64>::new()),
                ScalarBuffer::from(Vec::<f32>::new()),
                None,
                None,
            )));
        }

        let mut row_ids = Vec::with_capacity(positions_by_row_id.len());
        let mut frequencies = Vec::with_capacity(positions_by_row_id.len());
        let mut positions_builder = ListBuilder::new(Int32Builder::new());
        let mut max_score = 0.0_f32;
        for (row_id, mut positions) in positions_by_row_id {
            positions.sort_unstable();
            let frequency = positions.len() as u32;
            max_score = max_score.max(Self::grouped_score_upper_bound(
                query_weight,
                frequency,
                docs.num_tokens_by_row_id(row_id),
                scorer,
            ));
            row_ids.push(row_id);
            frequencies.push(frequency as f32);
            for position in positions {
                positions_builder.values().append_value(position as i32);
            }
            positions_builder.append(true);
        }

        Ok(PostingList::Plain(PlainPostingList::new(
            ScalarBuffer::from(row_ids),
            ScalarBuffer::from(frequencies),
            Some(max_score),
            Some(positions_builder.finish()),
        )))
    }

    fn union_compressed_posting_lists(
        postings: Vec<PostingList>,
        docs: &LoadedDocLengths,
        query_weight: f32,
        scorer: &MemBM25Scorer,
    ) -> Result<PostingList> {
        let block_size = postings
            .iter()
            .find_map(|posting| match posting {
                PostingList::Compressed(posting) => Some(posting.block_size),
                PostingList::Plain(_) => None,
            })
            .unwrap_or(LEGACY_BLOCK_SIZE);
        let mut freqs_by_doc_id = BTreeMap::new();
        for posting in postings {
            for (doc_id, freq, _) in posting.iter() {
                let doc_id = u32::try_from(doc_id).map_err(|_| {
                    Error::index(format!(
                        "compressed posting doc id {} exceeds u32::MAX",
                        doc_id
                    ))
                })?;
                let entry = freqs_by_doc_id.entry(doc_id).or_insert(0u32);
                *entry = entry.checked_add(freq).ok_or_else(|| {
                    Error::index(format!("posting frequency overflow for doc id {}", doc_id))
                })?;
            }
        }
        if freqs_by_doc_id.is_empty() {
            return Ok(PostingList::Plain(PlainPostingList::new(
                ScalarBuffer::from(Vec::<u64>::new()),
                ScalarBuffer::from(Vec::<f32>::new()),
                None,
                None,
            )));
        }

        let mut builder = PostingListBuilder::new_with_block_size(false, block_size);
        let mut doc_ids = Vec::with_capacity(freqs_by_doc_id.len());
        let mut frequencies = Vec::with_capacity(freqs_by_doc_id.len());
        for (doc_id, freq) in freqs_by_doc_id {
            builder.add(doc_id, PositionRecorder::Count(freq));
            doc_ids.push(doc_id);
            frequencies.push(freq);
        }
        let block_max_scores = Self::grouped_block_max_scores(
            &doc_ids,
            &frequencies,
            block_size,
            docs,
            query_weight,
            scorer,
        );
        let batch = builder.to_batch(block_max_scores)?;
        let max_score = batch[MAX_SCORE_COL].as_primitive::<Float32Type>().value(0);
        let length = batch[LENGTH_COL].as_primitive::<UInt32Type>().value(0);
        PostingList::from_batch(&batch, Some(max_score), Some(length))
    }

    fn union_compressed_posting_lists_with_positions(
        postings: Vec<PostingList>,
        docs: &LoadedDocLengths,
        query_weight: f32,
        scorer: &MemBM25Scorer,
    ) -> Result<PostingList> {
        let block_size = postings
            .iter()
            .find_map(|posting| match posting {
                PostingList::Compressed(posting) => Some(posting.block_size),
                PostingList::Plain(_) => None,
            })
            .unwrap_or(LEGACY_BLOCK_SIZE);
        let mut positions_by_doc_id = BTreeMap::<u32, Vec<u32>>::new();
        for posting in postings {
            for (doc_id, _, positions) in posting.iter() {
                let doc_id = u32::try_from(doc_id).map_err(|_| {
                    Error::index(format!(
                        "compressed posting doc id {} exceeds u32::MAX",
                        doc_id
                    ))
                })?;
                let positions = positions.ok_or_else(|| {
                    Error::index("cannot union grouped phrase terms without positions".to_string())
                })?;
                positions_by_doc_id
                    .entry(doc_id)
                    .or_default()
                    .extend(positions);
            }
        }
        if positions_by_doc_id.is_empty() {
            return Ok(PostingList::Plain(PlainPostingList::new(
                ScalarBuffer::from(Vec::<u64>::new()),
                ScalarBuffer::from(Vec::<f32>::new()),
                None,
                None,
            )));
        }

        let mut builder = PostingListBuilder::new_with_block_size(true, block_size);
        let mut doc_ids = Vec::with_capacity(positions_by_doc_id.len());
        let mut frequencies = Vec::with_capacity(positions_by_doc_id.len());
        for (doc_id, mut positions) in positions_by_doc_id {
            positions.sort_unstable();
            let frequency = positions.len() as u32;
            builder.add(doc_id, PositionRecorder::Position(positions.into()));
            doc_ids.push(doc_id);
            frequencies.push(frequency);
        }
        let block_max_scores = Self::grouped_block_max_scores(
            &doc_ids,
            &frequencies,
            block_size,
            docs,
            query_weight,
            scorer,
        );
        let batch = builder.to_batch(block_max_scores)?;
        let max_score = batch[MAX_SCORE_COL].as_primitive::<Float32Type>().value(0);
        let length = batch[LENGTH_COL].as_primitive::<UInt32Type>().value(0);
        PostingList::from_batch(&batch, Some(max_score), Some(length))
    }

    fn union_posting_lists(
        postings: Vec<PostingList>,
        docs: &LoadedDocLengths,
        with_positions: bool,
        query_weight: f32,
        scorer: &MemBM25Scorer,
    ) -> Result<PostingList> {
        let has_plain = postings
            .iter()
            .any(|posting| matches!(posting, PostingList::Plain(_)));
        let has_compressed = postings
            .iter()
            .any(|posting| matches!(posting, PostingList::Compressed(_)));
        match (has_plain, has_compressed) {
            (true, true) => Err(Error::index(
                "cannot union mixed plain and compressed posting lists".to_owned(),
            )),
            (true, false) if with_positions => {
                Self::union_plain_posting_lists_with_positions(postings, docs, query_weight, scorer)
            }
            (true, false) => Self::union_plain_posting_lists(postings, docs, query_weight, scorer),
            (false, true) if with_positions => Self::union_compressed_posting_lists_with_positions(
                postings,
                docs,
                query_weight,
                scorer,
            ),
            (false, true) => {
                Self::union_compressed_posting_lists(postings, docs, query_weight, scorer)
            }
            (false, false) => Ok(PostingList::Plain(PlainPostingList::new(
                ScalarBuffer::from(Vec::<u64>::new()),
                ScalarBuffer::from(Vec::<f32>::new()),
                None,
                None,
            ))),
        }
    }

    // search the documents that contain the query
    // return the doc info and the doc length
    // ref: https://en.wikipedia.org/wiki/Okapi_BM25
    //
    // `force_global_scorer` is used by compound search, where leaf scores and
    // bounds must share corpus-level statistics before the global collector
    // can safely propagate its threshold. Old posting formats without impacts
    // fall back to a scorer-derived global upper bound in that mode.
    #[instrument(level = "debug", skip_all)]
    pub(in super::super) async fn load_posting_lists(
        &self,
        tokens: &Tokens,
        params: &FtsSearchParams,
        operator: Operator,
        impact_scorer: &MemBM25Scorer,
        metrics: &dyn MetricsCollector,
        force_global_scorer: bool,
    ) -> Result<LoadedPostings> {
        let is_phrase_query = params.phrase_slop.is_some();
        let is_and_query = operator == Operator::And;
        let required_positions = (is_and_query || is_phrase_query).then(|| {
            (0..tokens.len())
                .map(|index| tokens.position(index))
                .collect::<HashSet<_>>()
        });
        // Fuzzy expansion already ran once at the index level (see
        // `InvertedIndex::bm25_search`) under the global `max_expansions`
        // budget. Positions identify alternatives that must share one posting
        // iterator, including code identifier subwords and fuzzy expansions.
        let tokens = tokens.clone();
        let token_positions = (0..tokens.len())
            .map(|index| tokens.position(index))
            .collect::<Vec<_>>();
        let mut seen_positions = HashSet::with_capacity(token_positions.len());
        let exact_scoring_required = token_positions
            .iter()
            .any(|position| !seen_positions.insert(*position));
        let mut token_ids = Vec::with_capacity(tokens.len());
        let mut matched_positions = required_positions.as_ref().map(|_| HashSet::new());
        for (index, token) in tokens.into_iter().enumerate() {
            let token_id = self.map(&token);
            if let Some(token_id) = token_id {
                let position = token_positions[index];
                if let Some(matched_positions) = matched_positions.as_mut() {
                    matched_positions.insert(position);
                }
                token_ids.push((token_id, token, position));
            }
        }
        if token_ids.is_empty() {
            return Ok(LoadedPostings::empty());
        }
        if let Some(required_positions) = required_positions.as_ref()
            && let Some(matched_positions) = matched_positions.as_ref()
            && !required_positions.is_subset(matched_positions)
        {
            return Ok(LoadedPostings::empty());
        }

        token_ids.sort_unstable_by_key(|(token_id, _, position)| (*position, *token_id));
        token_ids.dedup_by(|lhs, rhs| lhs.0 == rhs.0 && lhs.2 == rhs.2);

        let num_docs = self.docs.len();
        let loaded_postings = stream::iter(token_ids)
            .map(|(token_id, token, position)| async move {
                let posting = self
                    .inverted_list
                    .posting_list(token_id, is_phrase_query, metrics)
                    .await?;

                Result::Ok((token_id, token, position, posting))
            })
            .buffered(self.store.io_parallelism())
            .try_collect::<Vec<_>>()
            .await?;

        let needs_union = loaded_postings
            .windows(2)
            .any(|window| window[0].2 == window[1].2);
        if (is_and_query || is_phrase_query)
            && !needs_union
            && loaded_postings
                .iter()
                .any(|(_, _, _, posting)| posting.is_empty())
        {
            return Ok(LoadedPostings::empty());
        }

        if !needs_union {
            let impact_safe = loaded_postings
                .iter()
                .all(|(_, _, _, posting)| posting.has_impacts());
            return Ok(LoadedPostings {
                postings: loaded_postings
                    .into_iter()
                    .map(|(token_id, token, position, posting)| {
                        let needs_scorer_upper_bound = (exact_scoring_required
                            || force_global_scorer)
                            && !posting.has_impacts();
                        let query_weight =
                            if impact_safe || exact_scoring_required || force_global_scorer {
                                impact_scorer.query_weight(&token)
                            } else {
                                idf(posting.len(), num_docs)
                            };
                        let posting = PostingIterator::with_query_weight(
                            token,
                            token_id,
                            position,
                            query_weight,
                            posting,
                            num_docs,
                        );
                        if needs_scorer_upper_bound {
                            posting.with_scorer_upper_bound()
                        } else {
                            posting
                        }
                    })
                    .collect(),
                grouped_expansions: Vec::new(),
                impact_safe,
                exact_scoring_required,
            });
        }

        let docs_for_union = if needs_union {
            Some(match &self.docs {
                PartitionDocumentStore::Legacy(docs) => LoadedDocLengths::Legacy(docs.clone()),
                PartitionDocumentStore::Modern(documents) => {
                    LoadedDocLengths::Modern(documents.lengths().await?)
                }
            })
        } else {
            None
        };

        // WAND's AND mode treats every iterator as required, so expansions from
        // one original query position must be merged before scoring.
        let mut grouped_postings = Vec::new();
        let mut grouped_expansions = Vec::new();
        let mut iter = loaded_postings.into_iter().peekable();
        while let Some((token_id, token, position, posting)) = iter.next() {
            let mut group = vec![(token_id, token, posting)];
            while matches!(iter.peek(), Some((_, _, next_position, _)) if *next_position == position)
            {
                let (token_id, token, _, posting) = iter.next().expect("peeked item must exist");
                group.push((token_id, token, posting));
            }

            let (token_id, token, posting) = if group.len() == 1 {
                group.pop().expect("single-item group must exist")
            } else {
                let token_id = group[0].0;
                let token = group[0].1.clone();
                let terms = group
                    .iter()
                    .map(|(_, token, posting)| {
                        GroupedTermScorer::new(impact_scorer.query_weight(token), posting)
                    })
                    .collect::<Vec<_>>();
                let terms = Arc::<[GroupedTermScorer]>::from(terms);
                let query_weight = terms.iter().map(GroupedTermScorer::query_weight).sum();
                grouped_expansions.push(GroupedExpansionTerms {
                    position,
                    terms: terms.clone(),
                });
                let postings = group
                    .into_iter()
                    .map(|(_, _, posting)| posting)
                    .collect::<Vec<_>>();
                let docs = docs_for_union.as_ref().ok_or_else(|| {
                    Error::index("union docs were not loaded for grouped query terms".to_string())
                })?;
                let posting = Self::union_posting_lists(
                    postings,
                    docs,
                    is_phrase_query,
                    query_weight,
                    impact_scorer,
                )?;
                if posting.is_empty() && (is_and_query || is_phrase_query) {
                    return Ok(LoadedPostings::empty());
                }
                grouped_postings.push(
                    PostingIterator::with_query_weight(
                        token,
                        token_id,
                        position,
                        query_weight,
                        posting,
                        num_docs,
                    )
                    .with_grouped_terms(terms),
                );
                continue;
            };
            if posting.is_empty() {
                if is_and_query || is_phrase_query {
                    return Ok(LoadedPostings::empty());
                }
                continue;
            }

            let query_weight = impact_scorer.query_weight(&token);
            let needs_scorer_upper_bound = !posting.has_impacts();
            let posting = PostingIterator::with_query_weight(
                token,
                token_id,
                position,
                query_weight,
                posting,
                num_docs,
            );
            grouped_postings.push(if needs_scorer_upper_bound {
                posting.with_scorer_upper_bound()
            } else {
                posting
            });
        }

        Ok(LoadedPostings {
            postings: grouped_postings,
            grouped_expansions,
            impact_safe: false,
            exact_scoring_required: true,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn bm25_search_legacy(
        &self,
        docs: &DocSet,
        params: &FtsSearchParams,
        operator: Operator,
        mask: &RowAddrMask,
        postings: Vec<PostingIterator>,
        impact_scorer: Option<Arc<MemBM25Scorer>>,
        metrics: &dyn MetricsCollector,
        shared_threshold: Arc<AtomicU32>,
    ) -> Result<Vec<DocCandidate<u64>>> {
        let documents = LegacyWandDocuments::new(docs, mask);
        self.bm25_search_with_documents(
            &documents,
            params,
            operator,
            postings,
            impact_scorer,
            metrics,
            shared_threshold,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn bm25_search_modern(
        &self,
        lengths: &DocLengths,
        visibility: &DocVisibility,
        params: &FtsSearchParams,
        operator: Operator,
        postings: Vec<PostingIterator>,
        impact_scorer: Option<Arc<MemBM25Scorer>>,
        metrics: &dyn MetricsCollector,
        shared_threshold: Arc<AtomicU32>,
    ) -> Result<Vec<DocCandidate<DocId>>> {
        if visibility.is_all() {
            let documents = ModernWandDocuments::all(lengths);
            self.bm25_search_with_documents(
                &documents,
                params,
                operator,
                postings,
                impact_scorer,
                metrics,
                shared_threshold,
            )
        } else {
            let documents = ModernWandDocuments::filtered(lengths, visibility);
            self.bm25_search_with_documents(
                &documents,
                params,
                operator,
                postings,
                impact_scorer,
                metrics,
                shared_threshold,
            )
        }
    }

    #[instrument(level = "debug", skip_all)]
    #[allow(clippy::too_many_arguments)]
    fn bm25_search_with_documents<D: WandDocuments>(
        &self,
        documents: &D,
        params: &FtsSearchParams,
        operator: Operator,
        postings: Vec<PostingIterator>,
        impact_scorer: Option<Arc<MemBM25Scorer>>,
        metrics: &dyn MetricsCollector,
        shared_threshold: Arc<AtomicU32>,
    ) -> Result<Vec<DocCandidate<D::Candidate>>> {
        if postings.is_empty() {
            return Ok(Vec::new());
        }

        let hits = if let Some(scorer) = impact_scorer {
            let mut wand = Wand::new(operator, postings.into_iter(), documents, scorer)
                .with_shared_threshold(shared_threshold);
            wand.search(params, metrics)?
        } else {
            let scorer = IndexBM25Scorer::new(std::iter::once(self));
            let mut wand = Wand::new(operator, postings.into_iter(), documents, scorer)
                .with_shared_threshold(shared_threshold);
            wand.search(params, metrics)?
        };
        Ok(hits)
    }

    pub async fn into_builder(self) -> Result<InnerBuilder> {
        let mut builder = InnerBuilder::new_with_posting_tail_codec_and_block_size(
            self.id,
            self.inverted_list.has_positions(),
            self.token_set_format,
            self.inverted_list.posting_tail_codec(),
            self.inverted_list.block_size(),
        );
        builder.tokens = self.tokens.into_mutable();
        builder.docs = self.docs.load_build_docset().await?;

        builder
            .posting_lists
            .reserve_exact(self.inverted_list.len());
        for posting_list in self
            .inverted_list
            .read_all(self.inverted_list.has_positions())
            .await?
        {
            let posting_list = posting_list?;
            builder
                .posting_lists
                .push(posting_list.into_builder(&builder.docs));
        }
        Ok(builder)
    }
}
