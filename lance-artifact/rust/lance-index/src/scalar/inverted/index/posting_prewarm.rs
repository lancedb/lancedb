// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

impl PostingListReader {
    pub(super) async fn prewarm_residency_status(
        &self,
        with_position: bool,
    ) -> (bool, Option<bool>) {
        let postings_resident = self.postings_resident_now().await;
        let positions_resident = if with_position {
            Some(self.positions_resident_now().await)
        } else {
            None
        };
        (postings_resident, positions_resident)
    }

    async fn postings_resident_now(&self) -> bool {
        if self.is_empty() {
            return true;
        }

        let mut seen_groups = BTreeSet::new();
        for token_id in 0..self.len() as u32 {
            if let Some((start, end)) = self.group_range_for_token(token_id) {
                if seen_groups.insert((start, end))
                    && self
                        .index_cache
                        .get_with_key(&posting_list_group_cache_key(start, end, self.has_impacts))
                        .await
                        .is_none()
                {
                    return false;
                }
            } else if self
                .index_cache
                .get_with_key(&posting_list_cache_key(token_id, self.has_impacts))
                .await
                .is_none()
            {
                return false;
            }
        }
        true
    }

    async fn positions_resident_now(&self) -> bool {
        for token_id in 0..self.len() as u32 {
            if self
                .index_cache
                .get_with_key(&PositionKey { token_id })
                .await
                .is_none()
            {
                return false;
            }
        }
        true
    }

    /// Build posting lists for one chunk's token range from `chunk_batch`, rebasing
    /// global offsets to chunk-local rows. Returns `(global token_id, PostingList)`
    /// pairs identical to the whole-file path, only bounded to one chunk.
    fn build_prewarm_posting_lists_chunk(
        chunk_batch: RecordBatch,
        chunk: PrewarmChunk<'_>,
        ctx: &PrewarmBuildCtx<'_>,
    ) -> Result<Vec<(u32, PostingList)>> {
        let mut posting_lists = Vec::with_capacity(chunk.token_count);
        for local in 0..chunk.token_count {
            let global = chunk.tok_start + local;
            let row_batch = if let Some(chunk_offsets) = chunk.offsets {
                // Legacy v1: rebase global offsets to chunk row 0; the last token
                // ends at `chunk.end_row` (no trailing sentinel in chunk_offsets).
                let base = chunk_offsets[0];
                let start = chunk_offsets[local] - base;
                let end = if local + 1 < chunk_offsets.len() {
                    chunk_offsets[local + 1] - base
                } else {
                    chunk.end_row - base
                };
                chunk_batch.slice(start, end - start)
            } else {
                // V2: one posting row per token; row `local` within the chunk.
                chunk_batch.slice(local, 1)
            };
            let row_batch = row_batch.shrink_to_fit()?;
            let posting_list = Self::posting_list_from_batch_parts(
                &row_batch,
                ctx.max_scores.map(|scores| scores[global]),
                ctx.lengths.map(|lengths| lengths[global]),
                ctx.posting_tail_codec,
                ctx.block_size,
                ctx.positions_layout,
            )?;
            posting_lists.push((global as u32, posting_list));
        }

        Ok(posting_lists)
    }

    /// Read the posting rows for token ids `[tok_start, tok_end)` into one RecordBatch.
    /// For v2 the token range is the row range; for v1 it's derived from the offsets.
    async fn read_chunk_batch(
        &self,
        tok_start: usize,
        tok_end: usize,
        with_position: bool,
    ) -> Result<RecordBatch> {
        let columns = self.posting_columns(with_position);
        let row_range = match &self.metadata {
            PostingMetadata::LegacyV1 { offsets, .. } => {
                let start = offsets[tok_start];
                let end = offsets
                    .get(tok_end)
                    .copied()
                    .unwrap_or_else(|| self.reader.num_rows());
                start..end
            }
            PostingMetadata::V2 { .. } => tok_start..tok_end,
        };
        let batch = self.reader.read_range(row_range, Some(&columns)).await?;
        Ok(batch)
    }

    pub(super) async fn prewarm_posting_lists(
        &self,
        with_position: bool,
        chunk_concurrency: usize,
    ) -> Result<()> {
        self.prewarm_posting_lists_chunked(with_position, None, chunk_concurrency)
            .await?;
        Ok(())
    }

    /// Stream the partition's posting lists into the cache in bounded token-row chunks
    /// (read -> build -> insert -> drop), so peak resident set is ~one chunk. Returns
    /// the chunk count (tests assert it split). `chunk_tokens_override` is test-only.
    pub(super) async fn prewarm_posting_lists_chunked(
        &self,
        with_position: bool,
        chunk_tokens_override: Option<usize>,
        chunk_concurrency: usize,
    ) -> Result<usize> {
        if with_position && !self.has_positions() {
            return Err(Error::invalid_input(
                "cannot prewarm positions for an inverted index that was built without positions; recreate the index with with_position=true".to_owned(),
            ));
        }

        // Make max_scores/lengths available for query-local packed views. The
        // materialized fallback also clones them into its blocking build task.
        self.ensure_metadata_loaded().await?;

        // With grouping the cache stores one entry per group, so a group's
        // posting lists must all be resident at once: align chunk boundaries to
        // whole groups. Without grouping, chunks are plain token ranges.
        let grouping = self.grouping.clone();
        let use_packed_groups = grouping.is_grouped() && !with_position;
        // Packed groups reuse the reader's bulk metadata at query time, so they
        // do not need the temporary full-partition metadata clones used by the
        // materialized fallback.
        let state = (!use_packed_groups).then(|| self.chunk_build_state());
        let token_count = self.len();
        let posting_data_size_bytes = self.posting_data_size_bytes();
        let chunk_tokens = chunk_tokens_override
            .unwrap_or_else(|| prewarm_chunk_tokens(token_count, posting_data_size_bytes))
            .max(1);
        let chunk_ranges = prewarm_chunk_ranges(&grouping, token_count, chunk_tokens);
        let chunk_count = chunk_ranges.len();
        let chunk_concurrency = chunk_concurrency.max(1);

        let read_build_start = Instant::now();
        stream::iter(chunk_ranges)
            .map(|(tok_start, tok_end)| {
                let state = state.as_ref();
                let grouping = &grouping;
                async move {
                    if use_packed_groups {
                        let groups = self
                            .build_packed_chunk_groups(tok_start, tok_end, token_count, grouping)
                            .await?;
                        for (start, end, group) in groups {
                            self.index_cache
                                .insert_with_key(
                                    &posting_list_group_cache_key(start, end, self.has_impacts),
                                    Arc::new(group),
                                )
                                .await;
                        }
                    } else {
                        let state = state.expect(
                            "materialized prewarm must initialize posting-list build state",
                        );
                        let posting_lists = self
                            .build_chunk_postings(tok_start, tok_end, with_position, state)
                            .await?;
                        self.publish_chunk_postings(
                            posting_lists,
                            grouping,
                            tok_start,
                            tok_end,
                            token_count,
                            with_position,
                        )
                        .await;
                    }
                    Result::Ok(())
                }
            })
            .buffer_unordered(chunk_concurrency)
            .try_collect::<()>()
            .await?;
        let read_build_elapsed = read_build_start.elapsed();

        info!(
            legacy_layout = self.is_legacy_layout(),
            with_position,
            token_count,
            chunk_count,
            chunk_tokens,
            chunk_concurrency,
            posting_data_size_bytes,
            read_build_ms = read_build_elapsed.as_secs_f64() * 1000.0,
            "posting list prewarm timing"
        );

        Ok(chunk_count)
    }

    /// Loop-invariant inputs shared by every chunk build: the metadata vecs
    /// (`Arc`d so chunks share them without re-cloning) plus codec/layout.
    fn chunk_build_state(&self) -> ChunkBuildState {
        let (offsets, max_scores, lengths) = match &self.metadata {
            PostingMetadata::LegacyV1 {
                offsets,
                max_scores,
            } => (Some(offsets.clone()), max_scores.clone(), None),
            PostingMetadata::V2 { metadata } => (
                None,
                metadata.get().map(|loaded| loaded.max_scores.clone()),
                metadata.get().map(|loaded| loaded.lengths.clone()),
            ),
        };
        ChunkBuildState {
            offsets: offsets.map(Arc::new),
            max_scores: max_scores.map(Arc::new),
            lengths: lengths.map(Arc::new),
            posting_tail_codec: self.posting_tail_codec,
            block_size: self.block_size,
            positions_layout: self.positions_layout,
        }
    }

    /// Read one token-row chunk and build its posting lists off the runtime thread.
    /// The large batch is dropped inside the blocking task once built, bounding
    /// resident memory to one chunk.
    async fn build_chunk_postings(
        &self,
        tok_start: usize,
        tok_end: usize,
        with_position: bool,
        state: &ChunkBuildState,
    ) -> Result<Vec<(u32, PostingList)>> {
        let chunk_token_count = tok_end - tok_start;
        let chunk_batch = self
            .read_chunk_batch(tok_start, tok_end, with_position)
            .await?;

        let (chunk_offsets, chunk_end_row) = match state.offsets.as_ref() {
            Some(offsets) => {
                let end_row = offsets
                    .get(tok_end)
                    .copied()
                    .unwrap_or_else(|| self.reader.num_rows());
                (Some(offsets[tok_start..tok_end].to_vec()), end_row)
            }
            // V2 doesn't use chunk_end_row (one row per token); pass tok_end.
            None => (None, tok_end),
        };
        let max_scores = state.max_scores.clone();
        let lengths = state.lengths.clone();
        let posting_tail_codec = state.posting_tail_codec;
        let block_size = state.block_size;
        let positions_layout = state.positions_layout;
        let num_docs = self.modern_num_docs;
        let posting_lists = spawn_blocking(move || {
            let ctx = PrewarmBuildCtx {
                max_scores: max_scores.as_deref().map(|v| v.as_slice()),
                lengths: lengths.as_deref().map(|v| v.as_slice()),
                posting_tail_codec,
                block_size,
                positions_layout,
            };
            let chunk = PrewarmChunk {
                tok_start,
                token_count: chunk_token_count,
                offsets: chunk_offsets.as_deref(),
                end_row: chunk_end_row,
            };
            let posting_lists = Self::build_prewarm_posting_lists_chunk(chunk_batch, chunk, &ctx)?;
            if let Some(num_docs) = num_docs {
                for (token_id, posting) in &posting_lists {
                    Self::validate_modern_posting(*token_id, posting, num_docs)?;
                }
            }
            Result::Ok(posting_lists)
        })
        .await
        .map_err(|err| {
            Error::internal(format!(
                "Failed to build prewarm posting lists in blocking task: {err}"
            ))
        })??;
        for (token_id, _) in &posting_lists {
            self.publish_modern_posting_validated(*token_id).await?;
        }
        // The chunk yields its token range as contiguous ascending ids from
        // `tok_start`; the group publish path relies on this to index the lists.
        debug_assert_eq!(posting_lists.len(), chunk_token_count);
        debug_assert!(
            posting_lists
                .iter()
                .enumerate()
                .all(|(i, (token_id, _))| *token_id as usize == tok_start + i)
        );
        Ok(posting_lists)
    }

    /// Build compact v2 groups directly from one posting-row chunk. Each group
    /// slice is deep-copied once, so it owns only its Arrow buffers without
    /// materializing a `Vec<PostingList>` or retaining the full chunk.
    async fn build_packed_chunk_groups(
        &self,
        tok_start: usize,
        tok_end: usize,
        token_count: usize,
        grouping: &PostingGrouping,
    ) -> Result<Vec<(u32, u32, PostingListGroup)>> {
        debug_assert!(grouping.is_grouped());
        debug_assert!(!self.is_legacy_layout());

        let chunk_batch = self.read_chunk_batch(tok_start, tok_end, false).await?;
        let ranges = grouping.ranges_for_chunk(tok_start, tok_end, token_count);
        let posting_tail_codec = self.posting_tail_codec;
        let block_size = self.block_size;
        let num_docs = self.modern_num_docs;
        let (chunk_max_scores, chunk_lengths) = match &self.metadata {
            PostingMetadata::V2 { metadata } => {
                let loaded = metadata.get().ok_or_else(|| {
                    Error::internal("packed prewarm requires loaded posting metadata".to_owned())
                })?;
                (
                    loaded.max_scores[tok_start..tok_end].to_vec(),
                    loaded.lengths[tok_start..tok_end].to_vec(),
                )
            }
            PostingMetadata::LegacyV1 { .. } => {
                return Err(Error::internal(
                    "packed prewarm is not supported for legacy posting metadata".to_owned(),
                ));
            }
        };

        let groups = spawn_blocking(move || {
            let mut groups = Vec::with_capacity(ranges.len());
            for (start, end) in ranges {
                let start_usize = start as usize;
                let end_usize = end as usize;
                let local_start = start_usize - tok_start;
                let group_len = end_usize - start_usize;
                let group_batch = chunk_batch.slice(local_start, group_len).shrink_to_fit()?;
                let group = PostingListGroup::new_packed_with_block_size(
                    group_batch,
                    posting_tail_codec,
                    block_size,
                )?;
                if let Some(num_docs) = num_docs {
                    for token_id in start..end {
                        let chunk_slot = token_id as usize - tok_start;
                        let posting = group
                            .posting_list(
                                (token_id - start) as usize,
                                Some(chunk_max_scores[chunk_slot]),
                                Some(chunk_lengths[chunk_slot]),
                            )?
                            .ok_or_else(|| {
                                Error::index(format!(
                                    "token {token_id} is missing from prewarm posting group [{start}, {end})"
                                ))
                            })?;
                        Self::validate_modern_posting(token_id, &posting, num_docs)?;
                    }
                }
                groups.push((start, end, group));
            }
            Result::Ok(groups)
        })
        .await
        .map_err(|err| {
            Error::internal(format!(
                "Failed to build packed prewarm posting groups in blocking task: {err}"
            ))
        })??;
        for (start, end, _) in &groups {
            for token_id in *start..*end {
                self.publish_modern_posting_validated(token_id).await?;
            }
        }
        Ok(groups)
    }

    /// Strip positions into their own per-token cache entries (the posting cache
    /// holds positions-free lists), then populate the same cache keys the read
    /// path uses: grouped entries when grouping is active, per-token entries
    /// otherwise. Called once per chunk; the chunk's lists drop on return.
    async fn publish_chunk_postings(
        &self,
        posting_lists: Vec<(u32, PostingList)>,
        grouping: &PostingGrouping,
        tok_start: usize,
        tok_end: usize,
        token_count: usize,
        with_position: bool,
    ) {
        match grouping {
            PostingGrouping::None => {
                for (token_id, mut posting_list) in posting_lists {
                    self.cache_positions(&mut posting_list, token_id, with_position)
                        .await;
                    self.index_cache
                        .insert_with_key(
                            &posting_list_cache_key(token_id, self.has_impacts),
                            Arc::new(posting_list),
                        )
                        .await;
                }
            }
            PostingGrouping::SyntheticFixed { .. } => {
                let mut chunk_postings = Vec::with_capacity(posting_lists.len());
                for (token_id, mut posting_list) in posting_lists {
                    self.cache_positions(&mut posting_list, token_id, with_position)
                        .await;
                    chunk_postings.push(posting_list);
                }
                // Chunk is group-aligned, so every group starting in it also ends
                // in it; `chunk_postings[i]` is token `tok_start + i`. The last
                // group's `end` derives from `token_count`, matching the read path
                // so both produce identical `PostingListGroupKey`s.
                for (start, end) in grouping.ranges_for_chunk(tok_start, tok_end, token_count) {
                    let start_usize = start as usize;
                    let lo = start_usize - tok_start;
                    let hi = end as usize - tok_start;
                    let group = PostingListGroup::new(chunk_postings[lo..hi].to_vec());
                    self.index_cache
                        .insert_with_key(
                            &posting_list_group_cache_key(start, end, self.has_impacts),
                            Arc::new(group),
                        )
                        .await;
                }
            }
        }
    }

    /// Move a posting list's positions (when present and requested) into the
    /// dedicated per-token position cache, leaving the posting list positions-free.
    async fn cache_positions(
        &self,
        posting_list: &mut PostingList,
        token_id: u32,
        with_position: bool,
    ) {
        if with_position && let Some(positions) = posting_list.take_positions() {
            self.index_cache
                .insert_with_key(&PositionKey { token_id }, Arc::new(Positions(positions)))
                .await;
        }
    }

    /// Cheap `invert.lance` size estimate (file length from object metadata, no
    /// data read), used only to size prewarm chunks. Falls back to a row-count
    /// proxy when the reader can't surface the length (legacy v1).
    pub(crate) fn posting_data_size_bytes(&self) -> u64 {
        if let Some(size) = self.reader.file_size_bytes() {
            return size;
        }
        // Fallback proxy for readers that don't cache their file length: just needs
        // to be monotonic in partition size.
        const ESTIMATED_BYTES_PER_ROW: u64 = 16;
        (self.reader.num_rows() as u64).saturating_mul(ESTIMATED_BYTES_PER_ROW)
    }

    pub(crate) async fn read_batch(&self, with_position: bool) -> Result<RecordBatch> {
        let columns = self.posting_columns(with_position);
        let batch = self
            .reader
            .read_range(0..self.reader.num_rows(), Some(&columns))
            .await?;
        Ok(batch)
    }

    pub(crate) async fn read_all(
        &self,
        with_position: bool,
    ) -> Result<impl Iterator<Item = Result<PostingList>> + '_> {
        // read_all walks every posting list; the bulk metadata is paid for
        // unconditionally, so just load it once up front and index into it
        // synchronously below.
        self.ensure_metadata_loaded().await?;
        let batch = self.read_batch(with_position).await?;
        Ok((0..self.len()).map(move |i| {
            let token_id = i as u32;
            let range = self.posting_list_range(token_id);
            let batch = batch.slice(i, range.end - range.start);
            let (max_score, length) = self.bulk_metadata_for_token(token_id);
            self.posting_list_from_batch(&batch, max_score, length)
        }))
    }

    /// Sync lookup of `(max_score, length)` from the bulk-loaded metadata.
    /// Only safe after [`Self::ensure_metadata_loaded`]; callers that hold
    /// the OnceCell-loaded reference (e.g. read_all, prewarm) use this to
    /// avoid the per-token IO path.
    pub(super) fn bulk_metadata_for_token(&self, token_id: u32) -> (Option<f32>, Option<u32>) {
        match &self.metadata {
            PostingMetadata::LegacyV1 { max_scores, .. } => {
                (max_scores.as_ref().map(|m| m[token_id as usize]), None)
            }
            PostingMetadata::V2 { metadata } => {
                let loaded = metadata.get().expect(
                    "v2 metadata must be bulk-loaded before bulk_metadata_for_token; call ensure_metadata_loaded first",
                );
                (
                    Some(loaded.max_scores[token_id as usize]),
                    Some(loaded.lengths[token_id as usize]),
                )
            }
        }
    }

    pub(super) async fn read_positions(
        &self,
        token_id: u32,
        metrics: &dyn MetricsCollector,
    ) -> Result<CompressedPositionStorage> {
        let result = self.index_cache.get_or_insert_with_key_hit(PositionKey { token_id }, || async move {
            let positions = match self.positions_layout {
                PositionsLayout::None => {
                    return Err(Error::invalid_input(
                        "position is not found but required for phrase queries, try recreating the index with position".to_owned(),
                    ));
                }
                PositionsLayout::LegacyPerDoc => {
                    let batch = self
                        .reader
                        .read_range(self.posting_list_range(token_id), Some(&[POSITION_COL]))
                        .await
                        .map_err(|e| match e {
                            Error::Schema { .. } => Error::invalid_input("position is not found but required for phrase queries, try recreating the index with position".to_owned()),
                            e => e,
                        })?;
                    CompressedPositionStorage::LegacyPerDoc(
                        batch[POSITION_COL].as_list::<i32>().value(0).as_list::<i32>().clone(),
                    )
                }
                PositionsLayout::SharedStream(codec) => {
                    let batch = self
                        .reader
                        .read_range(
                            self.posting_list_range(token_id),
                            Some(&[COMPRESSED_POSITION_COL, POSITION_BLOCK_OFFSET_COL]),
                        )
                        .await
                        .map_err(|e| match e {
                            Error::Schema { .. } => Error::invalid_input("position is not found but required for phrase queries, try recreating the index with position".to_owned()),
                            e => e,
                        })?;
                    let bytes = bytes::Bytes::from(
                        batch[COMPRESSED_POSITION_COL]
                            .as_binary::<i64>()
                            .value(0)
                            .to_vec(),
                    );
                    let block_offsets = batch[POSITION_BLOCK_OFFSET_COL]
                        .as_list::<i32>()
                        .value(0)
                        .as_primitive::<UInt32Type>()
                        .values()
                        .to_vec();
                    CompressedPositionStorage::SharedStream(SharedPositionStream::new(
                        codec,
                        block_offsets,
                        bytes,
                    ))
                }
            };
            Result::Ok(Positions(positions))
        }).await;
        match &result {
            Ok((_, true)) => metrics.record_index_cache_hit(),
            _ => metrics.record_index_cache_miss(),
        }
        let (positions, _) = result?;
        Ok(positions.0.clone())
    }

    fn posting_list_range(&self, token_id: u32) -> Range<usize> {
        match &self.metadata {
            PostingMetadata::LegacyV1 { offsets, .. } => {
                let offset = offsets[token_id as usize];
                let posting_len = self.posting_len(token_id);
                offset..offset + posting_len
            }
            PostingMetadata::V2 { .. } => {
                let token_id = token_id as usize;
                token_id..token_id + 1
            }
        }
    }

    fn posting_columns(&self, with_position: bool) -> Vec<&'static str> {
        let mut base_columns = if self.is_legacy_layout() {
            vec![ROW_ID, FREQUENCY_COL]
        } else {
            vec![POSTING_COL]
        };
        if with_position {
            match self.positions_layout {
                PositionsLayout::None => {}
                PositionsLayout::LegacyPerDoc => base_columns.push(POSITION_COL),
                PositionsLayout::SharedStream(_) => {
                    base_columns.push(COMPRESSED_POSITION_COL);
                    base_columns.push(POSITION_BLOCK_OFFSET_COL);
                }
            }
        }
        if self.has_impacts {
            base_columns.push(IMPACT_COL);
        }
        base_columns
    }
}

/// Loop-invariant state for [`InvertedPartition::build_chunk_postings`]. The
/// metadata vecs are `Arc`d so each chunk's blocking build shares them cheaply.
pub(super) struct ChunkBuildState {
    offsets: Option<Arc<Vec<usize>>>,
    max_scores: Option<Arc<Vec<f32>>>,
    lengths: Option<Arc<Vec<u32>>>,
    posting_tail_codec: PostingTailCodec,
    block_size: usize,
    positions_layout: PositionsLayout,
}

/// Chunk-invariant inputs to [`InvertedPartition::build_prewarm_posting_lists_chunk`]:
/// the per-partition codec/layout and the (shared, whole-partition) metadata
/// slices indexed by global token id. These don't change across chunks.
pub(super) struct PrewarmBuildCtx<'a> {
    max_scores: Option<&'a [f32]>,
    lengths: Option<&'a [u32]>,
    posting_tail_codec: PostingTailCodec,
    block_size: usize,
    positions_layout: PositionsLayout,
}

/// Per-chunk inputs to [`InvertedPartition::build_prewarm_posting_lists_chunk`]:
/// the token sub-range `[tok_start, tok_start + token_count)` and, for legacy
/// v1, the rebased offset slice plus the chunk's end row.
pub(super) struct PrewarmChunk<'a> {
    tok_start: usize,
    token_count: usize,
    /// Legacy v1 only: `offsets[tok_start..tok_start+token_count]` (no sentinel).
    offsets: Option<&'a [usize]>,
    /// Legacy v1 only: global row at which this chunk's posting rows end.
    end_row: usize,
}
