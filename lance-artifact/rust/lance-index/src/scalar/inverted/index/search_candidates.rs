// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;

#[derive(Debug)]
pub(in super::super) struct PartitionCandidates<C> {
    pub(super) tokens_by_position: Vec<String>,
    pub(super) grouped_expansions: Vec<GroupedExpansionTerms>,
    pub(super) candidates: Vec<DocCandidate<C>>,
}

pub(super) struct ModernSearchRequest<'a> {
    pub(super) tokens: Arc<Tokens>,
    pub(super) params: Arc<FtsSearchParams>,
    pub(super) operator: Operator,
    pub(super) mask: Arc<RowAddrMask>,
    pub(super) metrics: Arc<dyn MetricsCollector>,
    pub(super) scorer: &'a MemBM25Scorer,
    pub(super) impact_scorer: Arc<MemBM25Scorer>,
    pub(super) limit: usize,
}

/// Typed identity for one modern candidate after partition-local scoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PartitionDocId {
    pub(super) partition_ordinal: u32,
    pub(super) doc_id: DocId,
}

impl PartitionDocId {
    pub(super) fn try_new(partition_ordinal: usize, doc_id: DocId) -> Result<Self> {
        Ok(Self {
            partition_ordinal: u32::try_from(partition_ordinal).map_err(|_| {
                Error::index(format!(
                    "FTS partition ordinal {partition_ordinal} exceeds candidate identity capacity"
                ))
            })?,
            doc_id,
        })
    }

    pub(super) fn partition_ordinal(self) -> usize {
        self.partition_ordinal as usize
    }
}

#[derive(Debug, Clone)]
pub(super) struct ScoredPartitionDoc {
    pub(super) document: PartitionDocId,
    pub(super) score: OrderedFloat,
}

impl ScoredPartitionDoc {
    fn new(document: PartitionDocId, score: f32) -> Self {
        Self {
            document,
            score: OrderedFloat(score),
        }
    }
}

impl PartialEq for ScoredPartitionDoc {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for ScoredPartitionDoc {}

impl PartialOrd for ScoredPartitionDoc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredPartitionDoc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score.cmp(&other.score)
    }
}

pub(super) const MAX_CONCURRENT_ADDRESS_READ_BYTES: usize = 64 * 1024 * 1024;

pub(super) fn address_read_concurrency(io_parallelism: usize, largest_read_bytes: usize) -> usize {
    let io_parallelism = io_parallelism.max(1);
    if largest_read_bytes == 0 {
        return io_parallelism;
    }
    io_parallelism.min(
        MAX_CONCURRENT_ADDRESS_READ_BYTES
            .checked_div(largest_read_bytes)
            .unwrap_or(0)
            .max(1),
    )
}

pub(super) fn push_scored_key(
    candidates: &mut BinaryHeap<Reverse<ScoredDoc>>,
    limit: usize,
    key: u64,
    score: f32,
) {
    if candidates.len() < limit {
        candidates.push(Reverse(ScoredDoc::new(key, score)));
    } else if candidates
        .peek()
        .is_some_and(|candidate| candidate.0.score.0 < score)
    {
        candidates.pop();
        candidates.push(Reverse(ScoredDoc::new(key, score)));
    }
}

pub(super) fn push_scored_partition_doc(
    candidates: &mut BinaryHeap<Reverse<ScoredPartitionDoc>>,
    limit: usize,
    document: PartitionDocId,
    score: f32,
) {
    if candidates.len() < limit {
        candidates.push(Reverse(ScoredPartitionDoc::new(document, score)));
    } else if candidates
        .peek()
        .is_some_and(|candidate| candidate.0.score.0 < score)
    {
        candidates.pop();
        candidates.push(Reverse(ScoredPartitionDoc::new(document, score)));
    }
}

pub(super) fn rescore_partition_candidates<C>(
    partition: PartitionCandidates<C>,
    scorer: &MemBM25Scorer,
    idf_cache: &mut HashMap<String, f32>,
) -> Vec<(C, f32)> {
    let PartitionCandidates {
        tokens_by_position,
        grouped_expansions,
        candidates,
    } = partition;
    let idf_by_position = tokens_by_position
        .iter()
        .map(|token| {
            *idf_cache
                .entry(token.clone())
                .or_insert_with(|| scorer.query_weight(token))
        })
        .collect::<Vec<_>>();
    let grouped_positions = grouped_expansions
        .iter()
        .map(|group| group.position)
        .collect::<HashSet<_>>();

    candidates
        .into_iter()
        .map(
            |DocCandidate {
                 document,
                 posting_doc_id,
                 freqs,
                 doc_length,
             }| {
                let mut score = 0.0;
                for (term_index, freq) in freqs {
                    if grouped_positions.contains(&term_index) {
                        continue;
                    }
                    debug_assert!((term_index as usize) < idf_by_position.len());
                    score +=
                        idf_by_position[term_index as usize] * scorer.doc_weight(freq, doc_length);
                }
                for group in &grouped_expansions {
                    for term in group.terms.iter() {
                        let Some(freq) = term.frequency(posting_doc_id) else {
                            continue;
                        };
                        score += term.query_weight() * scorer.doc_weight(freq, doc_length);
                    }
                }
                (document, score)
            },
        )
        .collect()
}

#[derive(Debug)]
pub(in super::super) struct LoadedPostings {
    pub(in super::super) postings: Vec<PostingIterator>,
    pub(super) grouped_expansions: Vec<GroupedExpansionTerms>,
    pub(super) impact_safe: bool,
    pub(super) exact_scoring_required: bool,
}

pub(super) enum LoadedDocLengths {
    Legacy(Arc<DocSet>),
    Modern(Arc<DocLengths>),
}

impl LoadedDocLengths {
    pub(super) fn scoring_num_tokens(&self, doc_id: u32) -> u32 {
        match self {
            Self::Legacy(docs) => docs.scoring_num_tokens(doc_id),
            Self::Modern(lengths) => lengths.scoring(DocId::new(doc_id)),
        }
    }

    pub(super) fn num_tokens_by_row_id(&self, row_id: u64) -> u32 {
        match self {
            Self::Legacy(docs) => docs.num_tokens_by_row_id(row_id),
            Self::Modern(_) => unreachable!("modern posting lists use dense DocIds"),
        }
    }
}

impl LoadedPostings {
    pub(super) fn empty() -> Self {
        Self {
            postings: Vec::new(),
            grouped_expansions: Vec::new(),
            impact_safe: false,
            exact_scoring_required: false,
        }
    }
}

#[derive(Debug)]
pub(super) struct GroupedExpansionTerms {
    pub(super) position: u32,
    pub(super) terms: Arc<[GroupedTermScorer]>,
}
