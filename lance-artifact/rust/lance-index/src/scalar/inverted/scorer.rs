// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::InvertedPartition;
use std::collections::HashMap;
use std::sync::Arc;

// the Scorer trait is used to calculate the score of a token in a document
// in general, the score is calculated as:
// sum over all query_weight(query_token) * doc_weight(freq, doc_tokens)
pub trait Scorer: Send + Sync {
    fn query_weight(&self, token: &str) -> f32;
    fn doc_weight(&self, freq: u32, doc_tokens: u32) -> f32;

    /// Finite upper bound for every non-negative value returned by
    /// [`Self::doc_weight`]. Returning `None` disables score-independent
    /// pruning where the posting format has no stored impact bounds.
    fn doc_weight_upper_bound(&self) -> Option<f32> {
        None
    }

    /// Stable identity for the corpus-level inputs used by [`Self::doc_weight`].
    ///
    /// Implementations should return `Some` only when equal keys guarantee the
    /// same document weight for every `(freq, doc_tokens)` pair. Scorers without
    /// such an identity keep impact bounds in the query-local cache only.
    fn doc_weight_cache_key(&self) -> Option<u64> {
        None
    }

    /// The doc-length-dependent BM25 denominator addend, when `doc_weight`
    /// factors as `(K1 + 1) * freq / (freq + addend)`; `None` for scorers
    /// without that shape. Scoring hot loops use this to bake a per-norm-code
    /// addend cache (Lucene's norm cache), which is bit-identical to calling
    /// `doc_weight` because both paths evaluate the same expressions.
    fn doc_norm(&self, doc_tokens: u32) -> Option<f32> {
        let _ = doc_tokens;
        None
    }
}

impl<T: Scorer + ?Sized> Scorer for Arc<T> {
    fn query_weight(&self, token: &str) -> f32 {
        self.as_ref().query_weight(token)
    }

    fn doc_weight(&self, freq: u32, doc_tokens: u32) -> f32 {
        self.as_ref().doc_weight(freq, doc_tokens)
    }

    fn doc_weight_upper_bound(&self) -> Option<f32> {
        self.as_ref().doc_weight_upper_bound()
    }

    fn doc_weight_cache_key(&self) -> Option<u64> {
        self.as_ref().doc_weight_cache_key()
    }

    fn doc_norm(&self, doc_tokens: u32) -> Option<f32> {
        self.as_ref().doc_norm(doc_tokens)
    }
}

/// The frequency-dependent half of the BM25 doc weight; `doc_norm` is the
/// doc-length addend (from [`Scorer::doc_norm`] or a per-norm-code cache).
#[inline]
pub(super) fn bm25_doc_weight_with_norm(freq: u32, doc_norm: f32) -> f32 {
    let freq = freq as f32;
    (K1 + 1.0) * freq / (freq + doc_norm)
}

// BM25 parameters
pub const K1: f32 = 1.2;
pub const B: f32 = 0.75;

#[inline]
fn bm25_doc_norm(doc_tokens: u32, avg_doc_length: f32) -> f32 {
    let doc_tokens = doc_tokens as f32;
    K1 * (1.0 - B + B * doc_tokens / avg_doc_length)
}

#[derive(Debug, Clone)]
pub struct MemBM25Scorer {
    pub total_tokens: u64,
    pub num_docs: usize,
    pub token_docs: HashMap<String, usize>,
}

impl MemBM25Scorer {
    pub fn new(total_tokens: u64, num_docs: usize, token_docs: HashMap<String, usize>) -> Self {
        Self {
            total_tokens,
            num_docs,
            token_docs,
        }
    }

    /// Incremental update bm25 scorer with one new document.
    ///
    /// # Arguments
    /// * `tokens` - The tokens of the new document that are also in the query
    /// * `num_tokens` - The total number of tokens in the document
    pub fn update(&mut self, doc_token_count: &HashMap<String, usize>, num_tokens: u64) {
        self.total_tokens += num_tokens;
        self.num_docs += 1;
        for (token, count) in doc_token_count {
            if let Some(old_count) = self.token_docs.get_mut(token) {
                *old_count += *count;
            } else {
                // This shouldn't happen because `tokens` should only contain tokens that are in the query
                // and we should have already initialized this with query tokens.  Still, log a warning just in case.
                log::warn!("Token {} not found in token_docs", token);
            }
        }
    }

    pub fn num_docs(&self) -> usize {
        self.num_docs
    }

    pub fn avg_doc_length(&self) -> f32 {
        self.total_tokens as f32 / self.num_docs as f32
    }

    pub fn num_docs_containing_token(&self, token: &str) -> usize {
        match self.token_docs.get(token) {
            Some(nq) => *nq,
            None => 0,
        }
    }
}

impl Scorer for MemBM25Scorer {
    fn query_weight(&self, token: &str) -> f32 {
        let token_docs = self.num_docs_containing_token(token);
        if token_docs == 0 {
            return 0.0;
        }
        idf(token_docs, self.num_docs)
    }

    fn doc_weight(&self, freq: u32, doc_tokens: u32) -> f32 {
        let doc_norm = bm25_doc_norm(doc_tokens, self.avg_doc_length());
        bm25_doc_weight_with_norm(freq, doc_norm)
    }

    fn doc_norm(&self, doc_tokens: u32) -> Option<f32> {
        Some(bm25_doc_norm(doc_tokens, self.avg_doc_length()))
    }

    fn doc_weight_upper_bound(&self) -> Option<f32> {
        Some(K1 + 1.0)
    }

    fn doc_weight_cache_key(&self) -> Option<u64> {
        Some(u64::from(self.avg_doc_length().to_bits()))
    }
}

pub struct IndexBM25Scorer<'a> {
    partitions: Vec<&'a InvertedPartition>,
    num_docs: usize,
    avg_doc_length: f32,
}

impl<'a> IndexBM25Scorer<'a> {
    /// Sync constructor.  Query setup populates immutable partition stats
    /// before entering the CPU-only WAND executor.
    pub fn new(partitions: impl Iterator<Item = &'a InvertedPartition>) -> Self {
        let partitions = partitions.collect::<Vec<_>>();
        let stats = partitions
            .iter()
            .map(|partition| {
                partition.docs.cached_stats().expect(
                    "IndexBM25Scorer::new requires partition stats to be loaded before WAND",
                )
            })
            .collect::<Vec<_>>();
        let num_docs = stats.iter().map(|stats| stats.num_docs).sum();
        let total_tokens: u64 = stats.iter().map(|stats| stats.total_tokens).sum();
        let avgdl = total_tokens as f32 / num_docs as f32;
        Self {
            partitions,
            num_docs,
            avg_doc_length: avgdl,
        }
    }

    pub fn num_docs_containing_token(&self, token: &str) -> usize {
        self.partitions
            .iter()
            .map(|part| {
                if let Some(token_id) = part.tokens.get(token) {
                    part.inverted_list.posting_len(token_id)
                } else {
                    0
                }
            })
            .sum()
    }
}

impl Scorer for IndexBM25Scorer<'_> {
    fn query_weight(&self, token: &str) -> f32 {
        let token_docs = self.num_docs_containing_token(token);
        if token_docs == 0 {
            return 0.0;
        }
        idf(token_docs, self.num_docs)
    }

    fn doc_weight(&self, freq: u32, doc_tokens: u32) -> f32 {
        let doc_norm = bm25_doc_norm(doc_tokens, self.avg_doc_length);
        bm25_doc_weight_with_norm(freq, doc_norm)
    }

    fn doc_norm(&self, doc_tokens: u32) -> Option<f32> {
        Some(bm25_doc_norm(doc_tokens, self.avg_doc_length))
    }

    fn doc_weight_upper_bound(&self) -> Option<f32> {
        Some(K1 + 1.0)
    }

    fn doc_weight_cache_key(&self) -> Option<u64> {
        Some(u64::from(self.avg_doc_length.to_bits()))
    }
}

#[inline]
pub fn idf(token_docs: usize, num_docs: usize) -> f32 {
    let num_docs = num_docs as f32;
    ((num_docs - token_docs as f32 + 0.5) / (token_docs as f32 + 0.5) + 1.0).ln()
}
