// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

pub struct PostingListReader {
    pub(super) reader: Arc<dyn IndexReader>,

    /// Layout-specific metadata. V2 keeps its per-token max-score and
    /// length columns lazy so opening a partition doesn't drag O(num_tokens)
    /// bytes off cold storage when the caller only needs `df` for a few terms.
    pub(super) metadata: PostingMetadata,

    pub(super) has_position: bool,
    pub(super) has_impacts: bool,
    pub(super) posting_tail_codec: PostingTailCodec,
    pub(super) block_size: usize,
    pub(super) positions_layout: PositionsLayout,

    /// Runtime posting-list cache grouping. Non-empty v2 indexes use synthetic
    /// fixed groups so prewarm can improve cache density without rebuilding the
    /// index or relying on persisted grouping metadata.
    pub(super) grouping: PostingGrouping,

    /// Modern postings contain dense DocIds into the partition document table.
    /// Cache successful boundary validation per immutable token so repeated
    /// queries do not decode the final posting block again.
    pub(super) modern_doc_id_validations: Option<Arc<[OnceCell<()>]>>,
    /// Skips per-token readiness checks once the whole immutable table is validated.
    pub(super) modern_postings_validated: AtomicBool,
    pub(super) modern_num_docs: Option<usize>,

    pub(super) index_cache: WeakLanceCache,
}

/// Per-token metadata (max_score, length) needed by the BM25 query and stats
/// paths. The legacy and v2 formats store this metadata in different
/// places, with very different cost profiles for cold-load: the variants
/// surface that asymmetry so callers can choose a per-token or bulk access
/// pattern.
pub(super) enum PostingMetadata {
    /// Legacy v1: offsets and max_scores are encoded in the file's schema
    /// metadata, so they are already in memory by the time `try_new` returns.
    LegacyV1 {
        offsets: Vec<usize>,
        max_scores: Option<Vec<f32>>,
    },
    /// V2: per-token `max_score` and `length` live as columns in the
    /// posting file. The bulk vectors are filled lazily by
    /// `ensure_metadata_loaded`, and the stats path can also fetch a single
    /// token via `posting_len_for_token` without forcing the bulk load.
    V2 {
        metadata: OnceCell<LoadedPostingMetadata>,
    },
}

#[derive(Debug, Clone)]
pub(super) struct LoadedPostingMetadata {
    pub(super) max_scores: Vec<f32>,
    pub(super) lengths: Vec<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PositionsLayout {
    None,
    LegacyPerDoc,
    SharedStream(PositionStreamCodec),
}

impl std::fmt::Debug for PostingListReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("InvertedListReader");
        match &self.metadata {
            PostingMetadata::LegacyV1 {
                offsets,
                max_scores,
            } => {
                s.field("layout", &"legacy_v1")
                    .field("offsets", offsets)
                    .field("max_scores", max_scores);
            }
            PostingMetadata::V2 { metadata } => {
                s.field("layout", &"v2")
                    .field("metadata_loaded", &metadata.initialized());
            }
        }
        s.finish()
    }
}

impl DeepSizeOf for PostingListReader {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        let metadata_size = match &self.metadata {
            PostingMetadata::LegacyV1 {
                offsets,
                max_scores,
            } => offsets.deep_size_of_children(context) + max_scores.deep_size_of_children(context),
            PostingMetadata::V2 { metadata } => metadata
                .get()
                .map(|loaded| {
                    loaded.max_scores.deep_size_of_children(context)
                        + loaded.lengths.deep_size_of_children(context)
                })
                .unwrap_or(0),
        };
        let validation_size = self
            .modern_doc_id_validations
            .as_ref()
            .map(|validations| {
                validations
                    .len()
                    .saturating_mul(std::mem::size_of::<OnceCell<()>>())
            })
            .unwrap_or(0);
        metadata_size + self.grouping.deep_size_of_children(context) + validation_size
    }
}

impl PostingListReader {
    pub(crate) async fn try_new(
        reader: Arc<dyn IndexReader>,
        index_cache: &LanceCache,
    ) -> Result<Self> {
        let positions_layout = if reader.schema().field(COMPRESSED_POSITION_COL).is_some() {
            PositionsLayout::SharedStream(parse_shared_position_codec(&reader.schema().metadata)?)
        } else if reader.schema().field(POSITION_COL).is_some() {
            PositionsLayout::LegacyPerDoc
        } else {
            PositionsLayout::None
        };
        let posting_tail_codec = parse_posting_tail_codec(&reader.schema().metadata)?;
        let block_size = parse_posting_block_size(&reader.schema().metadata)?;
        let has_position = positions_layout != PositionsLayout::None;
        let has_impacts = reader.schema().field(IMPACT_COL).is_some();
        let metadata = if reader.schema().field(POSTING_COL).is_none() {
            let (offsets, max_scores) = Self::load_metadata(reader.schema())?;
            PostingMetadata::LegacyV1 {
                offsets,
                max_scores,
            }
        } else {
            PostingMetadata::V2 {
                metadata: OnceCell::new(),
            }
        };

        let is_legacy_layout = matches!(&metadata, PostingMetadata::LegacyV1 { .. });
        let grouping = PostingGrouping::for_reader(is_legacy_layout, reader.num_rows());
        let modern_doc_id_validations = (!is_legacy_layout).then(|| {
            (0..reader.num_rows())
                .map(|_| OnceCell::new())
                .collect::<Vec<_>>()
                .into()
        });

        Ok(Self {
            reader,
            metadata,
            has_position,
            has_impacts,
            posting_tail_codec,
            block_size,
            positions_layout,
            grouping,
            modern_doc_id_validations,
            modern_postings_validated: AtomicBool::new(false),
            modern_num_docs: None,
            index_cache: WeakLanceCache::from(index_cache),
        })
    }

    // for legacy format
    // returns the offsets and max scores
    fn load_metadata(
        schema: &lance_core::datatypes::Schema,
    ) -> Result<(Vec<usize>, Option<Vec<f32>>)> {
        let offsets = schema
            .metadata
            .get("offsets")
            .ok_or(Error::index("offsets not found in metadata".to_owned()))?;
        let offsets = serde_json::from_str(offsets)?;

        let max_scores = schema
            .metadata
            .get("max_scores")
            .map(|max_scores| serde_json::from_str(max_scores))
            .transpose()?;
        Ok((offsets, max_scores))
    }

    // the number of posting lists
    pub fn len(&self) -> usize {
        match &self.metadata {
            PostingMetadata::LegacyV1 { offsets, .. } => offsets.len(),
            PostingMetadata::V2 { .. } => self.reader.num_rows(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn has_positions(&self) -> bool {
        self.has_position
    }

    pub(crate) fn posting_tail_codec(&self) -> PostingTailCodec {
        self.posting_tail_codec
    }

    pub(crate) fn block_size(&self) -> usize {
        self.block_size
    }

    pub(super) fn is_legacy_layout(&self) -> bool {
        matches!(self.metadata, PostingMetadata::LegacyV1 { .. })
    }

    /// Sync access to `posting_len`. Requires v2 metadata to already be
    /// loaded via [`ensure_metadata_loaded`]; the bm25 scoring path enforces
    /// that contract before kicking off wand. The stats path uses
    /// [`Self::posting_len_for_token`] instead, which avoids the bulk load.
    pub(crate) fn posting_len(&self, token_id: u32) -> usize {
        let token_id = token_id as usize;
        match &self.metadata {
            PostingMetadata::LegacyV1 { offsets, .. } => {
                let next_offset = offsets
                    .get(token_id + 1)
                    .copied()
                    .unwrap_or(self.reader.num_rows());
                next_offset - offsets[token_id]
            }
            PostingMetadata::V2 { metadata } => {
                let metadata = metadata
                    .get()
                    .expect("v2 posting metadata must be bulk-loaded before sync posting_len; call ensure_metadata_loaded first");
                metadata.lengths[token_id] as usize
            }
        }
    }

    /// Async access to a single token's posting list length. For v2
    /// indexes this reads one row of posting metadata if the bulk metadata has
    /// not been loaded yet, and never triggers the bulk load itself. The stats
    /// path uses this so a single-term `df` lookup costs O(1) bytes rather
    /// than O(num_unique_tokens).
    pub(crate) async fn posting_len_for_token(
        &self,
        token_id: u32,
        metrics: Option<&dyn MetricsCollector>,
    ) -> Result<usize> {
        match &self.metadata {
            PostingMetadata::LegacyV1 { .. } => Ok(self.posting_len(token_id)),
            PostingMetadata::V2 { metadata } => {
                if let Some(metadata) = metadata.get() {
                    return Ok(metadata.lengths[token_id as usize] as usize);
                }
                let (_, length) = self.posting_metadata_for_token(token_id, metrics).await?;
                length
                    .map(|len| len as usize)
                    .ok_or_else(|| Error::index("posting length metadata missing".to_string()))
            }
        }
    }

    /// Async access to a single token's `(max_score, length)` pair. Mirrors
    /// [`Self::posting_len_for_token`] but covers both columns the scoring
    /// path needs, in one read. For v2 indexes that have not been
    /// bulk-loaded this issues one `read_range(token..token+1, [MAX_SCORE,
    /// LENGTH])`; for legacy v1 the values come from in-memory schema
    /// metadata.
    pub(crate) async fn posting_metadata_for_token(
        &self,
        token_id: u32,
        metrics: Option<&dyn MetricsCollector>,
    ) -> Result<(Option<f32>, Option<u32>)> {
        match &self.metadata {
            PostingMetadata::LegacyV1 { max_scores, .. } => {
                Ok((max_scores.as_ref().map(|m| m[token_id as usize]), None))
            }
            PostingMetadata::V2 { metadata } => {
                if let Some(loaded) = metadata.get() {
                    return Ok((
                        Some(loaded.max_scores[token_id as usize]),
                        Some(loaded.lengths[token_id as usize]),
                    ));
                }
                let result = self
                    .index_cache
                    .get_or_insert_with_key_hit(PostingMetadataKey { token_id }, || async move {
                        let token_id = token_id as usize;
                        let batch = self
                            .reader
                            .read_range(token_id..token_id + 1, Some(&[MAX_SCORE_COL, LENGTH_COL]))
                            .await?;
                        let max_score = batch[MAX_SCORE_COL].as_primitive::<Float32Type>().value(0);
                        let length = batch[LENGTH_COL].as_primitive::<UInt32Type>().value(0);
                        Ok(PostingMetadataValue { max_score, length })
                    })
                    .await;
                if let Some(metrics) = metrics {
                    match &result {
                        Ok((_, true)) => metrics.record_index_cache_hit(),
                        _ => metrics.record_index_cache_miss(),
                    }
                }
                let metadata = result.map(|(value, _)| value)?;
                Ok((Some(metadata.max_score), Some(metadata.length)))
            }
        }
    }

    /// Force the v2 bulk metadata (`max_scores`, `lengths`) into
    /// memory. Cheap to call repeatedly; no-op for legacy v1 indexes whose
    /// metadata is already populated from schema metadata at `try_new` time.
    pub(crate) async fn ensure_metadata_loaded(&self) -> Result<()> {
        let PostingMetadata::V2 { metadata } = &self.metadata else {
            return Ok(());
        };
        metadata
            .get_or_try_init(|| async {
                let batch = self
                    .reader
                    .read_range(
                        0..self.reader.num_rows(),
                        Some(&[MAX_SCORE_COL, LENGTH_COL]),
                    )
                    .await?;
                let max_scores = batch[MAX_SCORE_COL]
                    .as_primitive::<Float32Type>()
                    .values()
                    .to_vec();
                let lengths = batch[LENGTH_COL]
                    .as_primitive::<UInt32Type>()
                    .values()
                    .to_vec();
                Ok::<LoadedPostingMetadata, Error>(LoadedPostingMetadata {
                    max_scores,
                    lengths,
                })
            })
            .await?;
        Ok(())
    }

    pub(crate) async fn posting_batch(
        &self,
        token_id: u32,
        with_position: bool,
    ) -> Result<RecordBatch> {
        if self.is_legacy_layout() {
            self.posting_batch_legacy(token_id, with_position).await
        } else {
            let token_id = token_id as usize;
            let mut columns = if with_position {
                match self.positions_layout {
                    PositionsLayout::SharedStream(_) => {
                        vec![
                            POSTING_COL,
                            COMPRESSED_POSITION_COL,
                            POSITION_BLOCK_OFFSET_COL,
                        ]
                    }
                    PositionsLayout::LegacyPerDoc => vec![POSTING_COL, POSITION_COL],
                    PositionsLayout::None => vec![POSTING_COL],
                }
            } else {
                vec![POSTING_COL]
            };
            if self.has_impacts {
                columns.push(IMPACT_COL);
            }
            let batch = self
                .reader
                .read_range(token_id..token_id + 1, Some(&columns))
                .await?;
            Ok(batch)
        }
    }

    async fn posting_batch_legacy(
        &self,
        token_id: u32,
        with_position: bool,
    ) -> Result<RecordBatch> {
        let mut columns = vec![ROW_ID, FREQUENCY_COL];
        if with_position {
            columns.push(POSITION_COL);
        }

        let length = self.posting_len(token_id);
        let PostingMetadata::LegacyV1 { offsets, .. } = &self.metadata else {
            unreachable!("posting_batch_legacy is only reachable on legacy v1 layout");
        };
        let token_id = token_id as usize;
        let offset = offsets[token_id];
        let batch = self
            .reader
            .read_range(offset..offset + length, Some(&columns))
            .await?;
        Ok(batch)
    }

    #[instrument(level = "debug", skip(self, metrics))]
    pub(crate) async fn posting_list(
        &self,
        token_id: u32,
        is_phrase_query: bool,
        metrics: &dyn MetricsCollector,
    ) -> Result<PostingList> {
        let mut posting = match self.group_range_for_token(token_id) {
            // Grouped path (issue #7040): one cache entry covers rows
            // [start, end), so neighbouring rare terms share a single read.
            Some((start, end)) => {
                let result = self
                    .index_cache
                    .get_or_insert_with_key_hit(
                        posting_list_group_cache_key(start, end, self.has_impacts),
                        || async move {
                            metrics.record_part_load();
                            info!(target: TRACE_IO_EVENTS, r#type=IO_TYPE_LOAD_SCALAR_PART, index_type="inverted", part_id=start);
                            self.load_posting_list_group(start, end).await
                        },
                    )
                    .await;
                match &result {
                    Ok((_, true)) => metrics.record_index_cache_hit(),
                    _ => metrics.record_index_cache_miss(),
                }
                let (group, _) = result?;
                let (max_score, length) = if group.needs_external_metadata() {
                    self.posting_metadata_for_token(token_id, Some(metrics))
                        .await?
                } else {
                    (None, None)
                };
                let slot = (token_id - start) as usize;
                group
                    .posting_list(slot, max_score, length)?
                    .ok_or_else(|| {
                        Error::index(format!(
                            "token {token_id} maps to slot {slot} outside posting group [{start}, {end})"
                        ))
                    })?
            }
            // Fallback for layouts that cannot use row-based groups: one cache
            // entry per token.
            None => {
                let result = self
                    .index_cache
                    .get_or_insert_with_key_hit(
                        posting_list_cache_key(token_id, self.has_impacts),
                        || async move {
                            metrics.record_part_load();
                            info!(target: TRACE_IO_EVENTS, r#type=IO_TYPE_LOAD_SCALAR_PART, index_type="inverted", part_id=token_id);
                            // Fetch the posting batch and this token's (max_score,
                            // length) in parallel; for cold v2 partitions this is one
                            // single-row metadata read plus one posting-row read,
                            // instead of pulling the full per-token metadata table.
                            let (batch, (max_score, length)) = futures::try_join!(
                                self.posting_batch(token_id, false),
                                self.posting_metadata_for_token(token_id, Some(metrics)),
                            )?;
                            self.posting_list_from_batch(&batch, max_score, length)
                        },
                    )
                    .await;
                match &result {
                    Ok((_, true)) => metrics.record_index_cache_hit(),
                    _ => metrics.record_index_cache_miss(),
                }
                result?.0.as_ref().clone()
            }
        };

        if !self.modern_posting_is_validated(token_id)? {
            self.ensure_modern_posting_validated(token_id, &posting)
                .await?;
        }

        if is_phrase_query && !posting.has_position() {
            // hit the cache and when the cache was populated, the positions column was not loaded
            let positions = self.read_positions(token_id, metrics).await?;
            posting.set_positions(positions);
        }

        Ok(posting)
    }

    pub(super) async fn ensure_modern_posting_validated(
        &self,
        token_id: u32,
        posting: &PostingList,
    ) -> Result<()> {
        let (Some(validations), Some(num_docs)) =
            (&self.modern_doc_id_validations, self.modern_num_docs)
        else {
            return Ok(());
        };
        let validation = validations.get(token_id as usize).ok_or_else(|| {
            Error::index(format!(
                "modern FTS token id {token_id} is outside validation state [0, {})",
                validations.len()
            ))
        })?;
        validation
            .get_or_try_init(|| async {
                Self::validate_modern_posting(token_id, posting, num_docs)
            })
            .await
            .map(|_| ())
    }

    #[inline]
    pub(super) fn modern_posting_is_validated(&self, token_id: u32) -> Result<bool> {
        if self.modern_postings_validated.load(Ordering::Acquire) {
            return Ok(true);
        }
        let (Some(validations), Some(_)) = (&self.modern_doc_id_validations, self.modern_num_docs)
        else {
            return Ok(true);
        };
        let validation = validations.get(token_id as usize).ok_or_else(|| {
            Error::index(format!(
                "modern FTS token id {token_id} is outside validation state [0, {})",
                validations.len()
            ))
        })?;
        Ok(validation.get().is_some())
    }

    pub(super) fn validate_modern_posting(
        token_id: u32,
        posting: &PostingList,
        num_docs: usize,
    ) -> Result<()> {
        validate_modern_posting_doc_ids(posting, &format!("token id {token_id}"), num_docs)
    }

    pub(super) async fn publish_modern_posting_validated(&self, token_id: u32) -> Result<()> {
        let Some(validations) = &self.modern_doc_id_validations else {
            return Ok(());
        };
        let validation = validations.get(token_id as usize).ok_or_else(|| {
            Error::index(format!(
                "modern FTS token id {token_id} is outside validation state [0, {})",
                validations.len()
            ))
        })?;
        validation
            .get_or_try_init(|| async { Result::Ok(()) })
            .await
            .map(|_| ())
    }

    pub(super) fn modern_posting_validation_ready(&self) -> bool {
        if self.modern_postings_validated.load(Ordering::Acquire) {
            return true;
        }
        let ready = self
            .modern_doc_id_validations
            .as_ref()
            .is_none_or(|validations| validations.iter().all(|state| state.get().is_some()));
        if ready {
            self.modern_postings_validated
                .store(true, Ordering::Release);
        }
        ready
    }

    /// Map a token id to its cache group's row range `[start, end)`, or `None`
    /// when grouping is not available so the caller falls back to the per-token
    /// path. In v2 the token id is the row offset, so the group range is also
    /// the physical row range.
    pub(super) fn group_range_for_token(&self, token_id: u32) -> Option<(u32, u32)> {
        self.grouping.range_for_token(token_id, self.len())
    }

    /// Read rows `[start, end)` into one compact Arrow-backed cache value.
    /// Positions are excluded; phrase queries load them on demand via
    /// [`Self::read_positions`].
    async fn load_posting_list_group(&self, start: u32, end: u32) -> Result<PostingListGroup> {
        let mut columns = vec![POSTING_COL, MAX_SCORE_COL, LENGTH_COL];
        if self.has_impacts {
            columns.push(IMPACT_COL);
        }
        let batch = self
            .reader
            .read_range(start as usize..end as usize, Some(&columns))
            .await?;
        PostingListGroup::new_packed_with_block_size(
            batch.shrink_to_fit()?,
            self.posting_tail_codec,
            self.block_size,
        )
    }

    pub(super) fn posting_list_from_batch_parts(
        batch: &RecordBatch,
        max_score: Option<f32>,
        length: Option<u32>,
        posting_tail_codec: PostingTailCodec,
        block_size: usize,
        positions_layout: PositionsLayout,
    ) -> Result<PostingList> {
        let posting_list = PostingList::from_batch_with_tail_codec_and_positions_layout(
            batch,
            max_score,
            length,
            posting_tail_codec,
            block_size,
            positions_layout,
        )?;
        Ok(posting_list)
    }

    pub(crate) fn posting_list_from_batch(
        &self,
        batch: &RecordBatch,
        max_score: Option<f32>,
        length: Option<u32>,
    ) -> Result<PostingList> {
        Self::posting_list_from_batch_parts(
            batch,
            max_score,
            length,
            self.posting_tail_codec,
            self.block_size,
            self.positions_layout,
        )
    }
}
