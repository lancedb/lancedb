// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

/// Target on-disk size of one prewarm chunk. Keep this large enough that cloud
/// stores do not spend prewarm time on thousands of tiny range reads, but still
/// bounded so one large partition is not materialized all at once.
pub(super) const PREWARM_CHUNK_TARGET_BYTES: u64 = 128 << 20;

/// Cap on token rows per chunk, bounding the built `Vec` when posting lists are tiny.
pub(super) const PREWARM_MAX_CHUNK_TOKENS: usize = 256 * 1024;

/// Floor on token rows per chunk, so a partition always makes progress.
pub(super) const PREWARM_MIN_CHUNK_TOKENS: usize = 1;

/// Maximum number of posting lists in a runtime synthetic cache group. This is
/// deliberately token-count based so grouping works for old v2 indexes without
/// scanning posting lengths or requiring index rebuilds.
pub(super) static LANCE_FTS_POSTING_GROUP_MAX_TOKENS: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("LANCE_FTS_POSTING_GROUP_MAX_TOKENS")
        .unwrap_or_else(|_| "128".to_string())
        .parse()
        .expect("failed to parse LANCE_FTS_POSTING_GROUP_MAX_TOKENS")
});

pub(super) fn runtime_posting_group_tokens() -> usize {
    (*LANCE_FTS_POSTING_GROUP_MAX_TOKENS).max(1)
}

/// Runtime posting-list cache grouping. Non-empty v2 indexes synthesize fixed
/// groups at read time so prewarm and queries share group cache entries without
/// persisted grouping metadata or index rebuilds.
#[derive(Debug, Clone, DeepSizeOf)]
pub(super) enum PostingGrouping {
    /// Leaves legacy or empty partitions ungrouped.
    None,
    /// Uses a fixed runtime cache group size measured in token rows, not posting bytes.
    SyntheticFixed { group_size: u32 },
}

impl PostingGrouping {
    pub(super) fn for_reader(is_legacy_layout: bool, token_count: usize) -> Self {
        if is_legacy_layout || token_count == 0 {
            return Self::None;
        }

        let group_size = u32::try_from(runtime_posting_group_tokens())
            .unwrap_or(u32::MAX)
            .max(1);
        Self::SyntheticFixed { group_size }
    }

    pub(super) fn is_grouped(&self) -> bool {
        !matches!(self, Self::None)
    }

    pub(super) fn range_for_token(&self, token_id: u32, token_count: usize) -> Option<(u32, u32)> {
        match self {
            Self::None => None,
            Self::SyntheticFixed { group_size } => {
                let token_count = u32::try_from(token_count).unwrap_or(u32::MAX);
                let start = (token_id / *group_size) * *group_size;
                let end = start.saturating_add(*group_size).min(token_count);
                Some((start, end))
            }
        }
    }

    fn aligned_chunk_end(&self, token_count: usize, tok_start: usize, desired_end: usize) -> usize {
        match self {
            Self::None => desired_end,
            Self::SyntheticFixed { group_size } => synthetic_group_aligned_chunk_end(
                usize::try_from(*group_size).unwrap_or(usize::MAX).max(1),
                token_count,
                tok_start,
                desired_end,
            ),
        }
    }

    pub(super) fn ranges_for_chunk(
        &self,
        tok_start: usize,
        tok_end: usize,
        token_count: usize,
    ) -> Vec<(u32, u32)> {
        match self {
            Self::None => Vec::new(),
            Self::SyntheticFixed { group_size } => synthetic_group_ranges_for_chunk(
                usize::try_from(*group_size).unwrap_or(usize::MAX).max(1),
                tok_start,
                tok_end,
                token_count,
            ),
        }
    }
}

/// Token rows per chunk: byte target / average bytes-per-token, clamped to `[MIN, MAX]`.
pub(super) fn prewarm_chunk_tokens(token_count: usize, file_size_bytes: u64) -> usize {
    if token_count == 0 {
        return PREWARM_MIN_CHUNK_TOKENS;
    }
    let bytes_per_token = (file_size_bytes / token_count as u64).max(1); // >= 1: no div-by-zero
    let by_bytes = (PREWARM_CHUNK_TARGET_BYTES / bytes_per_token) as usize;
    by_bytes.clamp(PREWARM_MIN_CHUNK_TOKENS, PREWARM_MAX_CHUNK_TOKENS)
}

pub(super) fn synthetic_group_aligned_chunk_end(
    group_size: usize,
    token_count: usize,
    tok_start: usize,
    desired_end: usize,
) -> usize {
    if desired_end >= token_count {
        return token_count;
    }

    let boundary = desired_end - (desired_end % group_size);
    if boundary > tok_start {
        boundary
    } else {
        tok_start.saturating_add(group_size).min(token_count)
    }
}

pub(super) fn synthetic_group_ranges_for_chunk(
    group_size: usize,
    tok_start: usize,
    tok_end: usize,
    token_count: usize,
) -> Vec<(u32, u32)> {
    let mut ranges = Vec::new();
    let mut start = tok_start - (tok_start % group_size);
    if start < tok_start {
        start = start.saturating_add(group_size).min(token_count);
    }
    while start < tok_end {
        let end = start.saturating_add(group_size).min(token_count);
        ranges.push((
            u32::try_from(start).unwrap_or(u32::MAX),
            u32::try_from(end).unwrap_or(u32::MAX),
        ));
        start = end;
    }
    ranges
}

pub(super) fn prewarm_chunk_ranges(
    grouping: &PostingGrouping,
    token_count: usize,
    chunk_tokens: usize,
) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut tok_start = 0usize;
    while tok_start < token_count {
        let mut tok_end = (tok_start + chunk_tokens).min(token_count);
        // `tok_start` is always a group boundary; snap `tok_end` back to one too.
        if grouping.is_grouped() {
            tok_end = grouping.aligned_chunk_end(token_count, tok_start, tok_end);
        }
        ranges.push((tok_start, tok_end));
        tok_start = tok_end;
    }
    ranges
}
