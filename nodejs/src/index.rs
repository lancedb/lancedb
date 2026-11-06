// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Mutex;

use lancedb::index::Index as LanceDbIndex;
use lancedb::index::scalar::{BTreeIndexBuilder, FmIndexBuilder, FtsIndexBuilder};
use lancedb::index::vector::{
    IvfFlatIndexBuilder, IvfHnswPqIndexBuilder, IvfHnswSqIndexBuilder, IvfPqIndexBuilder,
    IvfRqIndexBuilder,
};
use lancedb::tokenize as lancedb_tokenize;
use napi_derive::napi;

use crate::error::NapiErrorExt;
use crate::table::FtsToken;
use crate::util::parse_distance_type;

#[napi]
pub struct Index {
    inner: Mutex<Option<LanceDbIndex>>,
}

impl Index {
    pub fn consume(&self) -> napi::Result<LanceDbIndex> {
        self.inner
            .lock()
            .unwrap()
            .take()
            .ok_or(napi::Error::from_reason(
                "attempt to use an index more than once",
            ))
    }
}

/// Code-analyzer options shared by `Index.fts` and `tokenize`.
struct CodeAnalyzerOptions {
    analyzer: Option<String>,
    split_identifiers: Option<bool>,
    split_on_numerics: Option<bool>,
    preserve_original: Option<bool>,
    index_operators: Option<bool>,
}

impl CodeAnalyzerOptions {
    /// Check the options against the requested base tokenizer and return the
    /// base tokenizer to use, if any. `analyzer: "code"` selects the code
    /// tokenizer when `baseTokenizer` is unset or `"simple"`.
    fn resolve_base_tokenizer(
        &self,
        base_tokenizer: Option<String>,
    ) -> napi::Result<Option<String>> {
        let uses_code = match self.analyzer.as_deref() {
            None | Some("text") => base_tokenizer.as_deref() == Some("code"),
            Some("code") => true,
            Some(other) => {
                return Err(napi::Error::from_reason(format!(
                    "analyzer must be 'text' or 'code', got '{other}'"
                )));
            }
        };
        match (self.analyzer.as_deref(), base_tokenizer.as_deref()) {
            (Some("text"), Some("code")) => {
                return Err(napi::Error::from_reason(
                    "baseTokenizer 'code' requires analyzer 'code'",
                ));
            }
            (Some("code"), Some(tokenizer)) if tokenizer != "code" && tokenizer != "simple" => {
                return Err(napi::Error::from_reason(format!(
                    "analyzer 'code' requires baseTokenizer 'code', got '{tokenizer}'"
                )));
            }
            _ => {}
        }
        let flags = [
            self.split_identifiers,
            self.split_on_numerics,
            self.preserve_original,
            self.index_operators,
        ];
        if !uses_code && flags.into_iter().flatten().any(|flag| flag) {
            return Err(napi::Error::from_reason(
                "splitIdentifiers, splitOnNumerics, preserveOriginal and indexOperators \
                 require analyzer 'code'",
            ));
        }
        Ok(if uses_code {
            Some("code".to_string())
        } else {
            base_tokenizer
        })
    }

    /// Apply the explicit flags on top of the tokenizer defaults.
    fn apply(&self, mut opts: FtsIndexBuilder) -> FtsIndexBuilder {
        if let Some(value) = self.split_identifiers {
            opts = opts.split_identifiers(value);
        }
        if let Some(value) = self.split_on_numerics {
            opts = opts.split_on_numerics(value);
        }
        if let Some(value) = self.preserve_original {
            opts = opts.preserve_original(value);
        }
        if let Some(value) = self.index_operators {
            opts = opts.index_operators(value);
        }
        opts
    }
}

#[napi(catch_unwind)]
#[allow(dead_code, clippy::too_many_arguments)]
pub fn tokenize(
    query: String,
    base_tokenizer: Option<String>,
    language: Option<String>,
    max_token_length: Option<u32>,
    lower_case: Option<bool>,
    stem: Option<bool>,
    remove_stop_words: Option<bool>,
    custom_stop_words: Option<Vec<String>>,
    ascii_folding: Option<bool>,
    ngram_min_length: Option<u32>,
    ngram_max_length: Option<u32>,
    prefix_only: Option<bool>,
    analyzer: Option<String>,
    split_identifiers: Option<bool>,
    split_on_numerics: Option<bool>,
    preserve_original: Option<bool>,
    index_operators: Option<bool>,
) -> napi::Result<Vec<FtsToken>> {
    let mut opts = FtsIndexBuilder::default();
    let code_options = CodeAnalyzerOptions {
        analyzer,
        split_identifiers,
        split_on_numerics,
        preserve_original,
        index_operators,
    };
    if let Some(base_tokenizer) = code_options.resolve_base_tokenizer(base_tokenizer)? {
        opts = opts.base_tokenizer(base_tokenizer);
    }
    if let Some(language) = language {
        opts = opts.language(&language).map_err(|_| {
            napi::Error::from_reason(format!(
                "LanceDB does not support the requested language: '{}'",
                language
            ))
        })?;
    }
    if let Some(max_token_length) = max_token_length {
        opts = opts.max_token_length(Some(max_token_length as usize));
    }
    if let Some(lower_case) = lower_case {
        opts = opts.lower_case(lower_case);
    }
    if let Some(stem) = stem {
        opts = opts.stem(stem);
    }
    if let Some(remove_stop_words) = remove_stop_words {
        opts = opts.remove_stop_words(remove_stop_words);
    }
    opts = opts.custom_stop_words(custom_stop_words);
    if let Some(ascii_folding) = ascii_folding {
        opts = opts.ascii_folding(ascii_folding);
    }
    if let Some(ngram_min_length) = ngram_min_length {
        opts = opts.ngram_min_length(ngram_min_length);
    }
    if let Some(ngram_max_length) = ngram_max_length {
        opts = opts.ngram_max_length(ngram_max_length);
    }
    if let Some(prefix_only) = prefix_only {
        opts = opts.ngram_prefix_only(prefix_only);
    }
    opts = code_options.apply(opts);

    Ok(lancedb_tokenize(&query, &opts)
        .default_error()?
        .into_iter()
        .map(FtsToken::from)
        .collect())
}

#[napi]
impl Index {
    #[napi(factory)]
    pub fn ivf_pq(
        distance_type: Option<String>,
        num_partitions: Option<u32>,
        num_sub_vectors: Option<u32>,
        num_bits: Option<u32>,
        max_iterations: Option<u32>,
        sample_rate: Option<u32>,
        target_partition_size: Option<u32>,
    ) -> napi::Result<Self> {
        let mut ivf_pq_builder = IvfPqIndexBuilder::default();
        if let Some(distance_type) = distance_type {
            let distance_type = parse_distance_type(distance_type)?;
            ivf_pq_builder = ivf_pq_builder.distance_type(distance_type);
        }
        if let Some(num_partitions) = num_partitions {
            ivf_pq_builder = ivf_pq_builder.num_partitions(num_partitions);
        }
        if let Some(target_partition_size) = target_partition_size {
            ivf_pq_builder = ivf_pq_builder.target_partition_size(target_partition_size);
        }
        if let Some(num_sub_vectors) = num_sub_vectors {
            ivf_pq_builder = ivf_pq_builder.num_sub_vectors(num_sub_vectors);
        }
        if let Some(num_bits) = num_bits {
            ivf_pq_builder = ivf_pq_builder.num_bits(num_bits);
        }
        if let Some(max_iterations) = max_iterations {
            ivf_pq_builder = ivf_pq_builder.max_iterations(max_iterations);
        }
        if let Some(sample_rate) = sample_rate {
            ivf_pq_builder = ivf_pq_builder.sample_rate(sample_rate);
        }
        Ok(Self {
            inner: Mutex::new(Some(LanceDbIndex::IvfPq(ivf_pq_builder))),
        })
    }

    #[napi(factory)]
    pub fn ivf_rq(
        distance_type: Option<String>,
        num_partitions: Option<u32>,
        num_bits: Option<u32>,
        max_iterations: Option<u32>,
        sample_rate: Option<u32>,
        target_partition_size: Option<u32>,
    ) -> napi::Result<Self> {
        let mut ivf_rq_builder = IvfRqIndexBuilder::default();
        if let Some(distance_type) = distance_type {
            let distance_type = parse_distance_type(distance_type)?;
            ivf_rq_builder = ivf_rq_builder.distance_type(distance_type);
        }
        if let Some(num_partitions) = num_partitions {
            ivf_rq_builder = ivf_rq_builder.num_partitions(num_partitions);
        }
        if let Some(target_partition_size) = target_partition_size {
            ivf_rq_builder = ivf_rq_builder.target_partition_size(target_partition_size);
        }
        if let Some(num_bits) = num_bits {
            ivf_rq_builder = ivf_rq_builder.num_bits(num_bits);
        }
        if let Some(max_iterations) = max_iterations {
            ivf_rq_builder = ivf_rq_builder.max_iterations(max_iterations);
        }
        if let Some(sample_rate) = sample_rate {
            ivf_rq_builder = ivf_rq_builder.sample_rate(sample_rate);
        }
        Ok(Self {
            inner: Mutex::new(Some(LanceDbIndex::IvfRq(ivf_rq_builder))),
        })
    }

    #[napi(factory)]
    pub fn ivf_flat(
        distance_type: Option<String>,
        num_partitions: Option<u32>,
        max_iterations: Option<u32>,
        sample_rate: Option<u32>,
        target_partition_size: Option<u32>,
    ) -> napi::Result<Self> {
        let mut ivf_flat_builder = IvfFlatIndexBuilder::default();
        if let Some(distance_type) = distance_type {
            let distance_type = parse_distance_type(distance_type)?;
            ivf_flat_builder = ivf_flat_builder.distance_type(distance_type);
        }
        if let Some(num_partitions) = num_partitions {
            ivf_flat_builder = ivf_flat_builder.num_partitions(num_partitions);
        }
        if let Some(target_partition_size) = target_partition_size {
            ivf_flat_builder = ivf_flat_builder.target_partition_size(target_partition_size);
        }
        if let Some(max_iterations) = max_iterations {
            ivf_flat_builder = ivf_flat_builder.max_iterations(max_iterations);
        }
        if let Some(sample_rate) = sample_rate {
            ivf_flat_builder = ivf_flat_builder.sample_rate(sample_rate);
        }
        Ok(Self {
            inner: Mutex::new(Some(LanceDbIndex::IvfFlat(ivf_flat_builder))),
        })
    }

    #[napi(factory)]
    pub fn btree() -> Self {
        Self {
            inner: Mutex::new(Some(LanceDbIndex::BTree(BTreeIndexBuilder::default()))),
        }
    }

    #[napi(factory)]
    pub fn bitmap() -> Self {
        Self {
            inner: Mutex::new(Some(LanceDbIndex::Bitmap(Default::default()))),
        }
    }

    #[napi(factory)]
    pub fn label_list() -> Self {
        Self {
            inner: Mutex::new(Some(LanceDbIndex::LabelList(Default::default()))),
        }
    }

    #[napi(factory)]
    pub fn fm() -> Self {
        Self {
            inner: Mutex::new(Some(LanceDbIndex::Fm(FmIndexBuilder::default()))),
        }
    }

    #[napi(factory)]
    #[allow(clippy::too_many_arguments)]
    pub fn fts(
        with_position: Option<bool>,
        base_tokenizer: Option<String>,
        language: Option<String>,
        max_token_length: Option<u32>,
        lower_case: Option<bool>,
        stem: Option<bool>,
        remove_stop_words: Option<bool>,
        custom_stop_words: Option<Vec<String>>,
        ascii_folding: Option<bool>,
        ngram_min_length: Option<u32>,
        ngram_max_length: Option<u32>,
        prefix_only: Option<bool>,
        block_size: Option<u32>,
        analyzer: Option<String>,
        split_identifiers: Option<bool>,
        split_on_numerics: Option<bool>,
        preserve_original: Option<bool>,
        index_operators: Option<bool>,
    ) -> napi::Result<Self> {
        let mut opts = FtsIndexBuilder::default();
        if let Some(with_position) = with_position {
            opts = opts.with_position(with_position);
        }
        let code_options = CodeAnalyzerOptions {
            analyzer,
            split_identifiers,
            split_on_numerics,
            preserve_original,
            index_operators,
        };
        if let Some(base_tokenizer) = code_options.resolve_base_tokenizer(base_tokenizer)? {
            opts = opts.base_tokenizer(base_tokenizer);
        }
        opts = code_options.apply(opts);
        if let Some(language) = language {
            opts = opts.language(&language).map_err(|_| {
                napi::Error::from_reason(format!(
                    "LanceDB does not support the requested language: '{}'",
                    language
                ))
            })?;
        }
        if let Some(max_token_length) = max_token_length {
            opts = opts.max_token_length(Some(max_token_length as usize));
        }
        if let Some(lower_case) = lower_case {
            opts = opts.lower_case(lower_case);
        }
        if let Some(stem) = stem {
            opts = opts.stem(stem);
        }
        if let Some(remove_stop_words) = remove_stop_words {
            opts = opts.remove_stop_words(remove_stop_words);
        }
        opts = opts.custom_stop_words(custom_stop_words);
        if let Some(ascii_folding) = ascii_folding {
            opts = opts.ascii_folding(ascii_folding);
        }
        if let Some(ngram_min_length) = ngram_min_length {
            opts = opts.ngram_min_length(ngram_min_length);
        }
        if let Some(ngram_max_length) = ngram_max_length {
            opts = opts.ngram_max_length(ngram_max_length);
        }
        if let Some(prefix_only) = prefix_only {
            opts = opts.ngram_prefix_only(prefix_only);
        }
        if let Some(block_size) = block_size {
            opts = opts
                .block_size(block_size as usize)
                .map_err(|err| napi::Error::from_reason(err.to_string()))?;
        }

        Ok(Self {
            inner: Mutex::new(Some(LanceDbIndex::FTS(opts))),
        })
    }

    #[napi(factory)]
    #[allow(clippy::too_many_arguments)]
    pub fn hnsw_pq(
        distance_type: Option<String>,
        num_partitions: Option<u32>,
        num_sub_vectors: Option<u32>,
        max_iterations: Option<u32>,
        sample_rate: Option<u32>,
        m: Option<u32>,
        ef_construction: Option<u32>,
        target_partition_size: Option<u32>,
    ) -> napi::Result<Self> {
        let mut hnsw_pq_builder = IvfHnswPqIndexBuilder::default();
        if let Some(distance_type) = distance_type {
            let distance_type = parse_distance_type(distance_type)?;
            hnsw_pq_builder = hnsw_pq_builder.distance_type(distance_type);
        }
        if let Some(num_partitions) = num_partitions {
            hnsw_pq_builder = hnsw_pq_builder.num_partitions(num_partitions);
        }
        if let Some(target_partition_size) = target_partition_size {
            hnsw_pq_builder = hnsw_pq_builder.target_partition_size(target_partition_size);
        }
        if let Some(num_sub_vectors) = num_sub_vectors {
            hnsw_pq_builder = hnsw_pq_builder.num_sub_vectors(num_sub_vectors);
        }
        if let Some(max_iterations) = max_iterations {
            hnsw_pq_builder = hnsw_pq_builder.max_iterations(max_iterations);
        }
        if let Some(sample_rate) = sample_rate {
            hnsw_pq_builder = hnsw_pq_builder.sample_rate(sample_rate);
        }
        if let Some(m) = m {
            hnsw_pq_builder = hnsw_pq_builder.num_edges(m);
        }
        if let Some(ef_construction) = ef_construction {
            hnsw_pq_builder = hnsw_pq_builder.ef_construction(ef_construction);
        }
        Ok(Self {
            inner: Mutex::new(Some(LanceDbIndex::IvfHnswPq(hnsw_pq_builder))),
        })
    }

    #[napi(factory)]
    pub fn hnsw_sq(
        distance_type: Option<String>,
        num_partitions: Option<u32>,
        max_iterations: Option<u32>,
        sample_rate: Option<u32>,
        m: Option<u32>,
        ef_construction: Option<u32>,
        target_partition_size: Option<u32>,
    ) -> napi::Result<Self> {
        let mut hnsw_sq_builder = IvfHnswSqIndexBuilder::default();
        if let Some(distance_type) = distance_type {
            let distance_type = parse_distance_type(distance_type)?;
            hnsw_sq_builder = hnsw_sq_builder.distance_type(distance_type);
        }
        if let Some(num_partitions) = num_partitions {
            hnsw_sq_builder = hnsw_sq_builder.num_partitions(num_partitions);
        }
        if let Some(target_partition_size) = target_partition_size {
            hnsw_sq_builder = hnsw_sq_builder.target_partition_size(target_partition_size);
        }
        if let Some(max_iterations) = max_iterations {
            hnsw_sq_builder = hnsw_sq_builder.max_iterations(max_iterations);
        }
        if let Some(sample_rate) = sample_rate {
            hnsw_sq_builder = hnsw_sq_builder.sample_rate(sample_rate);
        }
        if let Some(m) = m {
            hnsw_sq_builder = hnsw_sq_builder.num_edges(m);
        }
        if let Some(ef_construction) = ef_construction {
            hnsw_sq_builder = hnsw_sq_builder.ef_construction(ef_construction);
        }
        Ok(Self {
            inner: Mutex::new(Some(LanceDbIndex::IvfHnswSq(hnsw_sq_builder))),
        })
    }
}
