// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::{Error, Result};
use serde::{Deserialize, Deserializer, Serialize};
use std::{env, path::PathBuf};

#[cfg(feature = "tokenizer-jieba")]
mod jieba;

pub mod document_tokenizer;
#[cfg(feature = "tokenizer-lindera")]
mod lindera;

#[cfg(feature = "tokenizer-jieba")]
use jieba::JiebaTokenizerBuilder;

#[cfg(feature = "tokenizer-lindera")]
use lindera::LinderaTokenizerBuilder;

use crate::pbold;
use crate::pbold::inverted_index_details::DocumentGranularity as PbDocumentGranularity;
use crate::scalar::inverted::tokenizer::document_tokenizer::{
    JsonTokenizer, LanceTokenizer, TextTokenizer,
};
use crate::scalar::inverted::{
    InvertedListFormatVersion, default_fts_format_version_for_block_size,
    resolve_fts_format_version, validate_format_version_block_size,
};
pub use lance_tokenizer::Language;
use lance_tokenizer::{
    AsciiFoldingFilter, CodeLexTokenizer, IcuTokenizer, LowerCaser, NgramTokenizer, RawTokenizer,
    RemoveLongFilter, SimpleTokenizer, Stemmer, StopWordFilter, TextAnalyzer, TextAnalyzerBuilder,
    WhitespaceTokenizer, WordDelimiterFilter,
};

/// Posting block size for indexes whose metadata predates configurable block sizes.
///
/// This must remain 128 so that legacy on-disk data is decoded correctly.
pub const LEGACY_BLOCK_SIZE: usize = 128;
/// Default posting block size for newly created indexes when none is configured.
///
/// This intentionally matches [`LEGACY_BLOCK_SIZE`] today but may evolve independently.
pub const DEFAULT_BLOCK_SIZE: usize = 128;
pub const VALID_BLOCK_SIZES: [usize; 2] = [128, 256];
const LANCE_FTS_FORMAT_VERSION_ENV_KEY: &str = "LANCE_FTS_FORMAT_VERSION";

/// The unit treated as one logical full-text-search document.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum DocumentGranularity {
    /// All text selected from one dataset row belongs to one document.
    #[default]
    Row,
    /// Each element of the deepest list on the field path is one document.
    ListElement,
}

impl DocumentGranularity {
    /// Whether results identify a list element within the dataset row.
    pub fn is_list_element(self) -> bool {
        self == Self::ListElement
    }
}

impl TryFrom<&str> for DocumentGranularity {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "row" => Ok(Self::Row),
            "list_element" => Ok(Self::ListElement),
            _ => Err(Error::invalid_input(format!(
                "unknown FTS document granularity '{value}'; expected 'row' or 'list_element'"
            ))),
        }
    }
}

impl TryFrom<i32> for DocumentGranularity {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self> {
        match PbDocumentGranularity::try_from(value) {
            Ok(PbDocumentGranularity::Row) => Ok(Self::Row),
            Ok(PbDocumentGranularity::ListElement) => Ok(Self::ListElement),
            Err(_) => Err(Error::invalid_input(format!(
                "unknown FTS document granularity value {value}"
            ))),
        }
    }
}

impl From<DocumentGranularity> for PbDocumentGranularity {
    fn from(value: DocumentGranularity) -> Self {
        match value {
            DocumentGranularity::Row => Self::Row,
            DocumentGranularity::ListElement => Self::ListElement,
        }
    }
}

/// Tokenizer configs
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct InvertedIndexParams {
    /// The logical document boundary used while creating an index.
    ///
    /// This is persisted in `InvertedIndexDetails`, not in the tokenizer
    /// params stored inside the physical index. Creation JSON adds it back
    /// explicitly in [`Self::to_training_json`].
    #[serde(default, skip_serializing)]
    pub(crate) document_granularity: DocumentGranularity,
    /// Document-level tokenizer.
    ///
    /// This decides how Lance extracts searchable text from the stored value:
    /// - `text`: plain string documents
    /// - `json`: JSON string documents
    /// - `None`: infer from the Arrow field type during index build
    ///
    /// The extracted text is then passed to `base_tokenizer`.
    pub(crate) lance_tokenizer: Option<String>,

    /// Lexical tokenizer used after document-level text extraction.
    ///
    /// Client-facing analyzer profiles are resolved into this field and the
    /// concrete filter options before params are persisted.
    /// - `simple`: splits tokens on whitespace and punctuation
    /// - `whitespace`: splits tokens on whitespace
    /// - `raw`: no tokenization
    /// - `code`: code-aware lexical tokenization
    /// - `icu`: ICU dictionary-based word segmentation
    /// - `icu/split`: ICU segmentation with simple-style delimiter splitting
    /// - `lindera/*`: Lindera tokenizer
    /// - `jieba/*`: Jieba tokenizer
    ///
    /// `simple` is recommended for most cases and the default value
    pub(crate) base_tokenizer: String,

    /// language for stemming and stop words
    /// this is only used when `stem` or `remove_stop_words` is true
    pub(crate) language: Language,

    /// If true, store the position of the term in the document
    /// This can significantly increase the size of the index
    /// If false, only store the frequency of the term in the document
    /// Default is false
    pub(crate) with_position: bool,

    /// maximum token length
    /// - `None`: no limit
    /// - `Some(n)`: remove tokens longer than `n`
    pub(crate) max_token_length: Option<usize>,

    /// whether lower case tokens
    pub(crate) lower_case: bool,

    /// whether apply stemming
    pub(crate) stem: bool,

    /// whether remove stop words
    pub(crate) remove_stop_words: bool,

    /// use customized stop words.
    /// - `None`: use built-in stop words based on language
    /// - `Some(words)`: use customized stop words
    pub(crate) custom_stop_words: Option<Vec<String>>,

    /// ascii folding
    pub(crate) ascii_folding: bool,

    /// min ngram length
    pub(crate) min_ngram_length: u32,

    /// max ngram length
    pub(crate) max_ngram_length: u32,

    /// whether prefix only
    pub(crate) prefix_only: bool,

    /// Number of documents in each compressed posting block.
    ///
    /// Missing serialized values come from indexes written before this
    /// parameter existed and must read as 128 for backwards compatibility. New
    /// indexes currently default to 128.
    pub(crate) block_size: usize,

    /// Split code identifiers into subwords.
    pub(crate) split_identifiers: bool,

    /// Split identifier subwords at letter/number boundaries.
    pub(crate) split_on_numerics: bool,

    /// Index a complete identifier in addition to subwords.
    pub(crate) preserve_original: bool,

    /// Index code operators such as `::`, `->`, and `!=`.
    pub(crate) index_operators: bool,

    /// Total memory limit in MiB for the build stage.
    ///
    /// This is split evenly across FTS workers at build time. By default Lance
    /// uses roughly `num_cpus / 2` workers, unless `LANCE_FTS_NUM_SHARDS` is set.
    /// If unset, each worker defaults to a 2 GiB build-time memory limit.
    ///
    /// This is a build-time only parameter and is not persisted with the index.
    #[serde(
        rename = "memory_limit",
        skip_serializing,
        default,
        alias = "worker_memory_limit_mb"
    )]
    pub(crate) memory_limit_mb: Option<u64>,

    /// Number of workers to use for FTS build.
    ///
    /// This is a build-time only parameter and is not persisted with the index.
    /// By default Lance uses roughly `num_cpus / 2` workers.
    /// The effective worker count is clamped to `[1, num_cpus - 2]`.
    #[serde(rename = "num_workers", skip_serializing, default)]
    pub(crate) num_workers: Option<usize>,

    /// On-disk FTS format version to write when creating a new index.
    ///
    /// This is a build-time only parameter and is not persisted with the index.
    /// If unset, new index creation falls back to
    /// `LANCE_FTS_FORMAT_VERSION`. Without either override, text analysis with
    /// 128-document blocks writes v2, while code analysis or 256-document blocks
    /// write v3.
    #[serde(
        rename = "format_version",
        skip_serializing,
        default,
        deserialize_with = "deserialize_format_version"
    )]
    pub(crate) format_version: Option<InvertedListFormatVersion>,
}

// Unknown fields must remain ignored because these params are persisted across Lance versions.
#[derive(Debug, Deserialize)]
struct RawInvertedIndexParams {
    // Input-only preset expanded before constructing normalized params.
    analyzer: Option<String>,
    document_granularity: Option<DocumentGranularity>,
    lance_tokenizer: Option<String>,
    base_tokenizer: Option<String>,
    language: Option<Language>,
    with_position: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_explicit_option")]
    max_token_length: Option<Option<usize>>,
    lower_case: Option<bool>,
    stem: Option<bool>,
    remove_stop_words: Option<bool>,
    custom_stop_words: Option<Vec<String>>,
    ascii_folding: Option<bool>,
    min_ngram_length: Option<u32>,
    max_ngram_length: Option<u32>,
    prefix_only: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_optional_block_size")]
    block_size: Option<usize>,
    split_identifiers: Option<bool>,
    split_on_numerics: Option<bool>,
    preserve_original: Option<bool>,
    index_operators: Option<bool>,
    #[serde(rename = "memory_limit", alias = "worker_memory_limit_mb")]
    memory_limit_mb: Option<u64>,
    #[serde(rename = "num_workers")]
    num_workers: Option<usize>,
    #[serde(
        rename = "format_version",
        default,
        deserialize_with = "deserialize_format_version"
    )]
    format_version: Option<InvertedListFormatVersion>,
}

impl<'de> Deserialize<'de> for InvertedIndexParams {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        RawInvertedIndexParams::deserialize(deserializer)?
            .resolve()
            .map_err(serde::de::Error::custom)
    }
}

impl RawInvertedIndexParams {
    fn resolve(self) -> Result<InvertedIndexParams> {
        let analyzer = match (
            self.analyzer.as_deref().map(normalize_analyzer),
            self.base_tokenizer.as_deref(),
        ) {
            (Some(Ok(analyzer)), _) => analyzer,
            (Some(Err(err)), _) => return Err(err),
            (None, Some("code")) => "code",
            (None, _) => "text",
        };

        if analyzer == "text" && self.base_tokenizer.as_deref() == Some("code") {
            return Err(Error::invalid_input(
                "base_tokenizer='code' requires analyzer='code'".to_string(),
            ));
        }
        if analyzer == "code"
            && let Some(base_tokenizer) = self.base_tokenizer.as_deref()
            && base_tokenizer != "code"
        {
            return Err(Error::invalid_input(format!(
                "analyzer='code' requires base_tokenizer='code', got '{}'",
                base_tokenizer
            )));
        }
        if analyzer == "text"
            && matches!(
                (
                    self.split_identifiers,
                    self.split_on_numerics,
                    self.preserve_original,
                    self.index_operators,
                ),
                (Some(true), _, _, _)
                    | (_, Some(true), _, _)
                    | (_, _, Some(true), _)
                    | (_, _, _, Some(true))
            )
        {
            return Err(Error::invalid_input(
                "code analyzer flags require analyzer='code'".to_string(),
            ));
        }

        let mut params = match analyzer {
            "code" => InvertedIndexParams::code(),
            "text" => InvertedIndexParams::default(),
            _ => unreachable!("analyzer is normalized above"),
        };

        if let Some(document_granularity) = self.document_granularity {
            params.document_granularity = document_granularity;
        }
        if let Some(lance_tokenizer) = self.lance_tokenizer {
            params.lance_tokenizer = Some(lance_tokenizer);
        }
        if let Some(base_tokenizer) = self.base_tokenizer {
            params.base_tokenizer = base_tokenizer;
        }
        if let Some(language) = self.language {
            params.language = language;
        }
        if let Some(with_position) = self.with_position {
            params.with_position = with_position;
        }
        if let Some(max_token_length) = self.max_token_length {
            params.max_token_length = max_token_length;
        }
        if let Some(lower_case) = self.lower_case {
            params.lower_case = lower_case;
        }
        if let Some(stem) = self.stem {
            params.stem = stem;
        }
        if let Some(remove_stop_words) = self.remove_stop_words {
            params.remove_stop_words = remove_stop_words;
        }
        if let Some(custom_stop_words) = self.custom_stop_words {
            params.custom_stop_words = Some(custom_stop_words);
        }
        if let Some(ascii_folding) = self.ascii_folding {
            params.ascii_folding = ascii_folding;
        }
        if let Some(min_ngram_length) = self.min_ngram_length {
            params.min_ngram_length = min_ngram_length;
        }
        if let Some(max_ngram_length) = self.max_ngram_length {
            params.max_ngram_length = max_ngram_length;
        }
        if let Some(prefix_only) = self.prefix_only {
            params.prefix_only = prefix_only;
        }
        if let Some(block_size) = self.block_size {
            params.block_size = validate_block_size(block_size)?;
        }
        if let Some(split_identifiers) = self.split_identifiers {
            params.split_identifiers = split_identifiers;
        }
        if let Some(split_on_numerics) = self.split_on_numerics {
            params.split_on_numerics = split_on_numerics;
        }
        if let Some(preserve_original) = self.preserve_original {
            params.preserve_original = preserve_original;
        }
        if let Some(index_operators) = self.index_operators {
            params.index_operators = index_operators;
        }
        params.memory_limit_mb = self.memory_limit_mb;
        params.num_workers = self.num_workers;
        params.format_version = self.format_version;
        params.validate()?;
        Ok(params)
    }
}

impl TryFrom<&InvertedIndexParams> for pbold::InvertedIndexDetails {
    type Error = Error;

    fn try_from(params: &InvertedIndexParams) -> Result<Self> {
        Ok(Self {
            base_tokenizer: Some(params.base_tokenizer.clone()),
            language: serde_json::to_string(&params.language)?,
            with_position: params.with_position,
            max_token_length: params.max_token_length.map(|l| l as u32),
            lower_case: params.lower_case,
            stem: params.stem,
            remove_stop_words: params.remove_stop_words,
            ascii_folding: params.ascii_folding,
            min_ngram_length: params.min_ngram_length,
            max_ngram_length: params.max_ngram_length,
            prefix_only: params.prefix_only,
            block_size: Some(params.block_size as u32),
            code_config: (params.base_tokenizer == "code").then_some(
                pbold::inverted_index_details::CodeTokenizerConfig {
                    split_identifiers: params.split_identifiers,
                    split_on_numerics: Some(params.split_on_numerics),
                    preserve_original: Some(params.preserve_original),
                    index_operators: params.index_operators,
                },
            ),
            document_granularity: PbDocumentGranularity::from(params.document_granularity) as i32,
            posting_format_version: Some(params.resolved_format_version().index_version()),
        })
    }
}

impl TryFrom<&pbold::InvertedIndexDetails> for InvertedIndexParams {
    type Error = Error;

    fn try_from(details: &pbold::InvertedIndexDetails) -> Result<Self> {
        if details.code_config.is_some() && details.base_tokenizer.as_deref() != Some("code") {
            return Err(Error::invalid_input(
                "code_config requires base_tokenizer='code'".to_string(),
            ));
        }
        if details.base_tokenizer.is_none() && details.language.is_empty() {
            let mut params = Self {
                block_size: match details.block_size {
                    Some(block_size) => validate_block_size(block_size as usize)?,
                    None => LEGACY_BLOCK_SIZE,
                },
                ..Self::default()
            };
            params.document_granularity = details.document_granularity.try_into()?;
            params.format_version = details
                .posting_format_version
                .map(|version| resolve_fts_format_version(Some(&version.to_string())))
                .transpose()?;
            return Ok(params);
        }

        let mut params = match details.base_tokenizer.as_deref() {
            Some("code") => Self::code(),
            _ => Self::default(),
        };
        params.base_tokenizer = details
            .base_tokenizer
            .as_ref()
            .cloned()
            .unwrap_or_else(|| params.base_tokenizer.clone());
        params.language = if details.language.is_empty() {
            params.language
        } else {
            serde_json::from_str(details.language.as_str())?
        };
        params.with_position = details.with_position;
        params.max_token_length = details.max_token_length.map(|l| l as usize);
        params.lower_case = details.lower_case;
        params.stem = details.stem;
        params.remove_stop_words = details.remove_stop_words;
        params.ascii_folding = details.ascii_folding;
        params.min_ngram_length = details.min_ngram_length;
        params.max_ngram_length = details.max_ngram_length;
        params.prefix_only = details.prefix_only;
        params.block_size = match details.block_size {
            Some(block_size) => validate_block_size(block_size as usize)?,
            None => LEGACY_BLOCK_SIZE,
        };
        if let Some(code_config) = &details.code_config {
            params.split_identifiers = code_config.split_identifiers;
            if let Some(split_on_numerics) = code_config.split_on_numerics {
                params.split_on_numerics = split_on_numerics;
            }
            if let Some(preserve_original) = code_config.preserve_original {
                params.preserve_original = preserve_original;
            }
            params.index_operators = code_config.index_operators;
        }
        params.document_granularity = details.document_granularity.try_into()?;
        params.format_version = details
            .posting_format_version
            .map(|version| resolve_fts_format_version(Some(&version.to_string())))
            .transpose()?;
        params.validate()?;
        Ok(params)
    }
}

fn normalize_analyzer(analyzer: &str) -> Result<&'static str> {
    match analyzer {
        "text" => Ok("text"),
        "code" => Ok("code"),
        other => Err(Error::invalid_input(format!(
            "unknown analyzer '{}', expected 'text' or 'code'",
            other
        ))),
    }
}

fn default_min_ngram_length() -> u32 {
    3
}

fn default_max_ngram_length() -> u32 {
    3
}

fn invalid_block_size_message(block_size: usize) -> String {
    format!("FTS inverted index block_size must be one of 128 or 256, got {block_size}")
}

pub fn validate_block_size(block_size: usize) -> Result<usize> {
    if VALID_BLOCK_SIZES.contains(&block_size) {
        Ok(block_size)
    } else {
        Err(Error::invalid_input(invalid_block_size_message(block_size)))
    }
}

fn deserialize_optional_block_size<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<usize>::deserialize(deserializer)?
        .map(validate_block_size)
        .transpose()
        .map_err(serde::de::Error::custom)
}

fn deserialize_format_version<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<InvertedListFormatVersion>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(value) => resolve_fts_format_version(Some(&value))
            .map(Some)
            .map_err(serde::de::Error::custom),
        serde_json::Value::Number(value) => {
            let Some(format_version) = value.as_u64() else {
                return Err(serde::de::Error::custom(format!(
                    "FTS format_version must be 1, 2, or 3, got {value}"
                )));
            };
            resolve_fts_format_version(Some(&format_version.to_string()))
                .map(Some)
                .map_err(serde::de::Error::custom)
        }
        other => Err(serde::de::Error::custom(format!(
            "FTS format_version must be 1, 2, or 3, got {other}"
        ))),
    }
}

fn resolve_creation_format_version(
    explicit: Option<InvertedListFormatVersion>,
    default: InvertedListFormatVersion,
) -> Result<InvertedListFormatVersion> {
    if let Some(format_version) = explicit {
        return Ok(format_version);
    }

    match env::var(LANCE_FTS_FORMAT_VERSION_ENV_KEY) {
        Ok(value) => resolve_fts_format_version(Some(&value)).map_err(|err| {
            Error::invalid_input(format!(
                "invalid {LANCE_FTS_FORMAT_VERSION_ENV_KEY} value {value:?}: {err}"
            ))
        }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(value)) => Err(Error::invalid_input(format!(
            "invalid {LANCE_FTS_FORMAT_VERSION_ENV_KEY} value {value:?}: expected UTF-8 value 1, 2, or 3"
        ))),
    }
}

fn deserialize_explicit_option<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

impl Default for InvertedIndexParams {
    fn default() -> Self {
        Self::new("simple".to_owned(), Language::English)
    }
}

impl InvertedIndexParams {
    /// Create a new `InvertedIndexParams` with the given base tokenizer and language.
    ///
    /// The `base_tokenizer` can be one of the following:
    /// - `simple`: splits tokens on whitespace and punctuation, default
    /// - `whitespace`: splits tokens on whitespace
    /// - `raw`: no tokenization
    /// - `code`: code identifier tokenization
    /// - `ngram`: N-Gram tokenizer
    /// - `icu`: ICU dictionary-based word segmentation
    /// - `icu/split`: ICU segmentation with simple-style delimiter splitting
    /// - `lindera/*`: Lindera tokenizer
    /// - `jieba/*`: Jieba tokenizer
    ///
    /// The `language` is used for stemming and removing stop words,
    /// this is not used for `lindera/*` and `jieba/*` tokenizers.
    /// Default to `English`.
    pub fn new(base_tokenizer: String, language: Language) -> Self {
        let mut params = Self {
            document_granularity: DocumentGranularity::Row,
            lance_tokenizer: None,
            base_tokenizer,
            language,
            with_position: false,
            max_token_length: Some(40),
            lower_case: true,
            stem: true,
            remove_stop_words: true,
            custom_stop_words: None,
            ascii_folding: true,
            min_ngram_length: default_min_ngram_length(),
            max_ngram_length: default_max_ngram_length(),
            prefix_only: false,
            block_size: DEFAULT_BLOCK_SIZE,
            split_identifiers: false,
            split_on_numerics: false,
            preserve_original: false,
            index_operators: false,
            memory_limit_mb: None,
            num_workers: None,
            format_version: None,
        };
        if params.base_tokenizer == "code" {
            params.apply_code_defaults();
        }
        params
    }

    fn apply_text_defaults(&mut self) {
        self.base_tokenizer = "simple".to_string();
        self.split_identifiers = false;
        self.split_on_numerics = false;
        self.preserve_original = false;
        self.lower_case = true;
        self.ascii_folding = true;
        self.stem = true;
        self.remove_stop_words = true;
        self.index_operators = false;
    }

    fn apply_code_defaults(&mut self) {
        self.base_tokenizer = "code".to_string();
        self.split_identifiers = false;
        self.split_on_numerics = true;
        self.preserve_original = true;
        self.lower_case = true;
        self.ascii_folding = true;
        self.stem = false;
        self.remove_stop_words = false;
        self.index_operators = false;
    }

    /// Create parameters for the code analyzer profile.
    ///
    /// # Examples
    ///
    /// ```
    /// use lance_index::scalar::InvertedIndexParams;
    ///
    /// let tokenizer = InvertedIndexParams::code().build();
    /// assert!(tokenizer.is_ok());
    /// ```
    pub fn code() -> Self {
        Self::new("code".to_string(), Language::English)
    }

    /// Apply an analyzer profile to the concrete tokenizer parameters.
    ///
    /// The profile name is an input-time preset and is not persisted. Explicit
    /// options applied after this method override the profile defaults.
    ///
    /// # Examples
    ///
    /// ```
    /// use lance_index::scalar::InvertedIndexParams;
    ///
    /// let params = InvertedIndexParams::default()
    ///     .analyzer("code")?
    ///     .split_identifiers(true);
    /// assert!(params.build().is_ok());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn analyzer(mut self, analyzer: &str) -> Result<Self> {
        match normalize_analyzer(analyzer)? {
            "text" => self.apply_text_defaults(),
            "code" => self.apply_code_defaults(),
            _ => unreachable!("analyzer is normalized above"),
        }
        Ok(self)
    }

    pub fn lance_tokenizer(mut self, lance_tokenizer: String) -> Self {
        self.lance_tokenizer = Some(lance_tokenizer);
        self
    }

    /// Set the logical FTS document boundary.
    pub fn document_granularity(mut self, document_granularity: DocumentGranularity) -> Self {
        self.document_granularity = document_granularity;
        self
    }

    /// Return the logical FTS document boundary.
    pub fn get_document_granularity(&self) -> DocumentGranularity {
        self.document_granularity
    }

    /// Set the lexical tokenizer implementation.
    ///
    /// Setting this to `"code"` selects the code analyzer defaults.
    ///
    /// # Examples
    ///
    /// ```
    /// use lance_index::scalar::InvertedIndexParams;
    ///
    /// let params = InvertedIndexParams::default()
    ///     .base_tokenizer("code".to_string())
    ///     .split_identifiers(true);
    /// assert!(params.build().is_ok());
    /// ```
    pub fn base_tokenizer(mut self, base_tokenizer: String) -> Self {
        self.base_tokenizer = base_tokenizer;
        if self.base_tokenizer == "code" {
            self.apply_code_defaults();
        }
        self
    }

    pub fn language(mut self, language: &str) -> Result<Self> {
        // need to convert to valid JSON string
        let language = serde_json::from_str(format!("\"{}\"", language).as_str())?;
        self.language = language;
        Ok(self)
    }

    /// Set whether to store the position of the term in the document.
    /// This can significantly increase the size of the index.
    /// If false, only store the frequency of the term in the document.
    /// This doesn't work with `ngram` tokenizer.
    /// Default to `false`.
    pub fn with_position(mut self, with_position: bool) -> Self {
        self.with_position = with_position;
        self
    }

    /// Get whether positions are stored in this index.
    pub fn has_positions(&self) -> bool {
        self.with_position
    }

    pub fn max_token_length(mut self, max_token_length: Option<usize>) -> Self {
        self.max_token_length = max_token_length;
        self
    }

    pub fn lower_case(mut self, lower_case: bool) -> Self {
        self.lower_case = lower_case;
        self
    }

    pub fn stem(mut self, stem: bool) -> Self {
        self.stem = stem;
        self
    }

    pub fn remove_stop_words(mut self, remove_stop_words: bool) -> Self {
        self.remove_stop_words = remove_stop_words;
        self
    }

    pub fn custom_stop_words(mut self, custom_stop_words: Option<Vec<String>>) -> Self {
        self.custom_stop_words = custom_stop_words;
        self
    }

    pub fn ascii_folding(mut self, ascii_folding: bool) -> Self {
        self.ascii_folding = ascii_folding;
        self
    }

    /// Set whether code identifiers are split into subwords.
    ///
    /// # Examples
    ///
    /// ```
    /// use lance_index::scalar::InvertedIndexParams;
    ///
    /// let params = InvertedIndexParams::code()
    ///     .split_identifiers(true)
    ///     .split_on_numerics(false)
    ///     .preserve_original(false)
    ///     .index_operators(true);
    /// assert!(params.build().is_ok());
    /// ```
    pub fn split_identifiers(mut self, split_identifiers: bool) -> Self {
        self.split_identifiers = split_identifiers;
        self
    }

    /// Set whether identifier subwords are split at letter/number boundaries.
    pub fn split_on_numerics(mut self, split_on_numerics: bool) -> Self {
        self.split_on_numerics = split_on_numerics;
        self
    }

    /// Set whether the complete identifier is indexed alongside subwords.
    pub fn preserve_original(mut self, preserve_original: bool) -> Self {
        self.preserve_original = preserve_original;
        self
    }

    /// Set whether operator tokens such as `::`, `->`, and `!=` are indexed.
    pub fn index_operators(mut self, index_operators: bool) -> Self {
        self.index_operators = index_operators;
        self
    }

    /// Set the minimum N-Gram length, only works when `base_tokenizer` is `ngram`.
    /// Must be greater than 0 and not greater than `max_ngram_length`.
    /// Default to 3.
    pub fn ngram_min_length(mut self, min_length: u32) -> Self {
        self.min_ngram_length = min_length;
        self
    }

    /// Set the maximum N-Gram length, only works when `base_tokenizer` is `ngram`.
    /// Must be greater than 0 and not less than `min_ngram_length`.
    /// Default to 3.
    pub fn ngram_max_length(mut self, max_length: u32) -> Self {
        self.max_ngram_length = max_length;
        self
    }

    /// Set whether only prefix N-Gram is generated, only works when `base_tokenizer` is `ngram`.
    /// Default to `false`.
    pub fn ngram_prefix_only(mut self, prefix_only: bool) -> Self {
        self.prefix_only = prefix_only;
        self
    }

    /// Set the compressed posting block size.
    ///
    /// Supported values are 128 and 256. Larger values reduce block-max metadata
    /// and WAND skip granularity; smaller values preserve the legacy layout.
    ///
    /// `block_size = 256` is experimental and may introduce breaking changes.
    /// Use `128` when stable compatibility with the legacy posting layout is required.
    pub fn block_size(mut self, block_size: usize) -> Result<Self> {
        self.block_size = validate_block_size(block_size)?;
        Ok(self)
    }

    /// Get the compressed posting block size.
    ///
    /// `256` is experimental and may introduce breaking changes.
    pub fn posting_block_size(&self) -> usize {
        self.block_size
    }

    pub fn memory_limit_mb(mut self, memory_limit_mb: u64) -> Self {
        self.memory_limit_mb = Some(memory_limit_mb);
        self
    }

    /// Set the number of workers to use for this build.
    ///
    /// By default Lance uses roughly `num_cpus / 2` workers.
    /// The effective worker count is clamped to `[1, num_cpus - 2]`.
    pub fn num_workers(mut self, num_workers: usize) -> Self {
        self.num_workers = Some(num_workers);
        self
    }

    /// Set the on-disk FTS format version to use when creating a new index.
    ///
    /// If unset, new index creation falls back to
    /// `LANCE_FTS_FORMAT_VERSION`. Without either override, text analysis with
    /// 128-document blocks writes v2, while code analysis or 256-document blocks
    /// write v3. Existing indexes keep their own format during update and
    /// optimize operations.
    pub fn format_version(mut self, format_version: InvertedListFormatVersion) -> Self {
        self.format_version = Some(format_version);
        self
    }

    /// Resolve the requested FTS format version, falling back to the default for
    /// the configured analyzer and block size.
    pub fn resolved_format_version(&self) -> InvertedListFormatVersion {
        self.format_version.unwrap_or_else(|| {
            if self.base_tokenizer == "code" {
                InvertedListFormatVersion::V3
            } else {
                default_fts_format_version_for_block_size(self.block_size)
                    .expect("InvertedIndexParams block_size must be validated before use")
            }
        })
    }

    /// Validate that the requested FTS format version can safely encode the
    /// configured posting block size.
    pub fn validate_format_version(&self) -> Result<()> {
        let format_version = self.resolved_format_version();
        validate_format_version_block_size(format_version, self.block_size)?;
        if self.base_tokenizer == "code" && format_version != InvertedListFormatVersion::V3 {
            return Err(Error::invalid_input(format!(
                "base_tokenizer='code' requires FTS format_version=3, got {}",
                format_version.index_version()
            )));
        }
        Ok(())
    }

    /// Serialize user-visible params, including logical index identity fields.
    pub(crate) fn to_details_json(&self) -> serde_json::Result<serde_json::Value> {
        let mut value = serde_json::to_value(self)?;
        let object = value
            .as_object_mut()
            .expect("inverted index params should serialize to a JSON object");
        object.insert(
            "document_granularity".to_string(),
            serde_json::to_value(self.document_granularity)?,
        );
        Ok(value)
    }

    /// Serialize params for the build/training path, including build-only fields.
    pub fn to_training_json(&self) -> serde_json::Result<serde_json::Value> {
        let mut value = self.to_details_json()?;
        let object = value
            .as_object_mut()
            .expect("inverted index params should serialize to a JSON object");
        if let Some(memory_limit_mb) = self.memory_limit_mb {
            object.insert(
                "memory_limit".to_string(),
                serde_json::Value::from(memory_limit_mb),
            );
        }
        if let Some(num_workers) = self.num_workers {
            object.insert(
                "num_workers".to_string(),
                serde_json::Value::from(num_workers),
            );
        }
        if let Some(format_version) = self.format_version {
            object.insert(
                "format_version".to_string(),
                serde_json::Value::from(format_version.index_version()),
            );
        }
        Ok(value)
    }

    /// Deserialize params for new index training, using the environment
    /// override and current creation defaults for omitted fields.
    pub(crate) fn from_training_json(params: &str) -> Result<Self> {
        let supplied = serde_json::from_str::<serde_json::Value>(params)?;
        let mut value = serde_json::to_value(Self::default())?;

        let supplied = supplied.as_object().ok_or_else(|| {
            Error::invalid_input("FTS inverted index params must be a JSON object".to_string())
        })?;
        let object = value
            .as_object_mut()
            .expect("inverted index params should serialize to a JSON object");
        object.extend(supplied.clone());

        let mut params: Self = serde_json::from_value(value)?;
        let default_format_version = params.resolved_format_version();
        params.format_version = Some(resolve_creation_format_version(
            params.format_version,
            default_format_version,
        )?);
        params.validate_format_version()?;
        Ok(params)
    }

    pub fn build(&self) -> Result<Box<dyn LanceTokenizer>> {
        self.validate()?;
        let mut builder = self.build_base_tokenizer()?;
        if let Some(max_token_length) = self.max_token_length {
            builder = builder.filter_dynamic(RemoveLongFilter::limit(max_token_length));
        }
        if self.lower_case {
            builder = builder.filter_dynamic(LowerCaser);
        }
        if self.stem {
            builder = builder.filter_dynamic(Stemmer::new(self.language));
        }
        if self.remove_stop_words {
            builder = builder.filter_dynamic(self.stop_word_filter()?);
        }
        if self.ascii_folding {
            builder = builder.filter_dynamic(AsciiFoldingFilter);
        }
        let tokenizer = builder.build();

        match self.lance_tokenizer {
            Some(ref t) if t == "text" => Ok(Box::new(TextTokenizer::new(tokenizer))),
            Some(ref t) if t == "json" => Ok(Box::new(JsonTokenizer::new(tokenizer))),
            None => Ok(Box::new(TextTokenizer::new(tokenizer))),
            _ => Err(Error::invalid_input(format!(
                "unknown lance tokenizer {}",
                self.lance_tokenizer.as_ref().unwrap()
            ))),
        }
    }

    fn stop_word_filter(&self) -> Result<StopWordFilter> {
        match &self.custom_stop_words {
            Some(words) => Ok(StopWordFilter::remove(words.iter().cloned())),
            None if self.base_tokenizer == "icu" || self.base_tokenizer == "icu/split" => {
                Ok(StopWordFilter::all())
            }
            None => StopWordFilter::new(self.language).ok_or_else(|| {
                Error::invalid_input(format!(
                    "removing stop words for language {:?} is not supported yet",
                    self.language
                ))
            }),
        }
    }

    fn validate(&self) -> Result<()> {
        validate_block_size(self.block_size)?;
        if self.base_tokenizer != "code"
            && (self.split_identifiers
                || self.split_on_numerics
                || self.preserve_original
                || self.index_operators)
        {
            return Err(Error::invalid_input(
                "code analyzer flags require base_tokenizer='code'".to_string(),
            ));
        }
        Ok(())
    }

    fn build_base_tokenizer(&self) -> Result<TextAnalyzerBuilder> {
        match self.base_tokenizer.as_str() {
            "simple" => Ok(TextAnalyzer::builder(SimpleTokenizer::default()).dynamic()),
            "whitespace" => Ok(TextAnalyzer::builder(WhitespaceTokenizer::default()).dynamic()),
            "raw" => Ok(TextAnalyzer::builder(RawTokenizer::default()).dynamic()),
            "code" => {
                let mut builder =
                    TextAnalyzer::builder(CodeLexTokenizer::new(self.index_operators)).dynamic();
                if self.split_identifiers {
                    builder = builder.filter_dynamic(WordDelimiterFilter::new(
                        self.preserve_original,
                        self.split_on_numerics,
                    ));
                }
                Ok(builder)
            }
            "icu" => Ok(TextAnalyzer::builder(IcuTokenizer::default()).dynamic()),
            "icu/split" => {
                Ok(TextAnalyzer::builder(IcuTokenizer::default().with_simple_split()).dynamic())
            }
            "ngram" => {
                let tokenizer = NgramTokenizer::new(
                    self.min_ngram_length as usize,
                    self.max_ngram_length as usize,
                    self.prefix_only,
                )
                .map_err(|e| Error::invalid_input(e.to_string()))?;
                Ok(TextAnalyzer::builder(tokenizer).dynamic())
            }
            #[cfg(feature = "tokenizer-lindera")]
            s if s.starts_with("lindera/") => {
                let Some(home) = language_model_home() else {
                    return Err(Error::invalid_input(format!(
                        "unknown base tokenizer {}",
                        self.base_tokenizer
                    )));
                };
                lindera::LinderaBuilder::load(&home.join(s))?.build()
            }
            #[cfg(feature = "tokenizer-jieba")]
            s if s.starts_with("jieba/") || s == "jieba" => {
                let s = if s == "jieba" { "jieba/default" } else { s };
                let Some(home) = language_model_home() else {
                    return Err(Error::invalid_input(format!(
                        "unknown base tokenizer {}",
                        self.base_tokenizer
                    )));
                };
                jieba::JiebaBuilder::load(&home.join(s))?.build()
            }
            _ => Err(Error::invalid_input(format!(
                "unknown base tokenizer {}",
                self.base_tokenizer
            ))),
        }
    }
}

pub const LANCE_LANGUAGE_MODEL_HOME_ENV_KEY: &str = "LANCE_LANGUAGE_MODEL_HOME";

pub const LANCE_LANGUAGE_MODEL_DEFAULT_DIRECTORY: &str = "lance/language_models";

pub fn language_model_home() -> Option<PathBuf> {
    match env::var(LANCE_LANGUAGE_MODEL_HOME_ENV_KEY) {
        Ok(p) => Some(PathBuf::from(p)),
        Err(_) => dirs::data_local_dir().map(|p| p.join(LANCE_LANGUAGE_MODEL_DEFAULT_DIRECTORY)),
    }
}

#[cfg(test)]
mod tests {
    use crate::pbold;
    use crate::pbold::inverted_index_details::DocumentGranularity as PbDocumentGranularity;

    use super::{DocumentGranularity, InvertedIndexParams, InvertedListFormatVersion};
    use lance_core::Error;
    use lance_tokenizer::{Language, TokenStream};
    use rstest::rstest;
    use serde_json::json;

    #[test]
    fn test_physical_params_omit_creation_only_fields() {
        let params = InvertedIndexParams::default()
            .document_granularity(DocumentGranularity::ListElement)
            .memory_limit_mb(4096)
            .num_workers(7)
            .format_version(InvertedListFormatVersion::V1);
        let json = serde_json::to_value(&params).unwrap();
        assert!(json.get("document_granularity").is_none());
        assert!(json.get("memory_limit").is_none());
        assert!(json.get("num_workers").is_none());
        assert!(json.get("format_version").is_none());
    }

    #[test]
    fn test_memory_limit_serde_accepts_legacy_worker_field_name() {
        let mut json = serde_json::to_value(InvertedIndexParams::default()).unwrap();
        let obj = json.as_object_mut().unwrap();
        obj.remove("memory_limit");
        obj.insert(
            "worker_memory_limit_mb".to_string(),
            serde_json::Value::from(2048),
        );
        let params: InvertedIndexParams = serde_json::from_value(json).unwrap();
        assert_eq!(params.memory_limit_mb, Some(2048));
    }

    #[test]
    fn test_build_only_fields_deserialize_from_public_names() {
        let mut json = serde_json::to_value(InvertedIndexParams::default()).unwrap();
        let obj = json.as_object_mut().unwrap();
        obj.insert("memory_limit".to_string(), serde_json::Value::from(4096));
        obj.insert("num_workers".to_string(), serde_json::Value::from(3));
        obj.insert("format_version".to_string(), serde_json::Value::from("v1"));

        let params: InvertedIndexParams = serde_json::from_value(json).unwrap();
        assert_eq!(params.memory_limit_mb, Some(4096));
        assert_eq!(params.num_workers, Some(3));
        assert_eq!(params.format_version, Some(InvertedListFormatVersion::V1));
    }

    #[test]
    fn test_training_json_serializes_creation_fields() {
        let params = InvertedIndexParams::default()
            .document_granularity(DocumentGranularity::ListElement)
            .memory_limit_mb(4096)
            .num_workers(3)
            .format_version(InvertedListFormatVersion::V1);
        let json = params.to_training_json().unwrap();
        assert_eq!(
            json.get("document_granularity"),
            Some(&serde_json::Value::from("list_element"))
        );
        assert_eq!(
            json.get("memory_limit"),
            Some(&serde_json::Value::from(4096))
        );
        assert_eq!(json.get("num_workers"), Some(&serde_json::Value::from(3)));
        assert_eq!(
            json.get("format_version"),
            Some(&serde_json::Value::from(1))
        );
    }

    #[test]
    fn test_training_json_preserves_disabled_max_token_length() {
        let params = InvertedIndexParams::default().max_token_length(None);
        let json = params.to_training_json().unwrap();
        assert_eq!(json.get("max_token_length"), Some(&serde_json::Value::Null));

        let params: InvertedIndexParams = serde_json::from_value(json).unwrap();
        assert_eq!(params.max_token_length, None);
    }

    #[test]
    fn test_default_format_version_resolves_to_v2() {
        assert_eq!(
            InvertedIndexParams::default().resolved_format_version(),
            InvertedListFormatVersion::V2
        );
    }

    #[test]
    fn test_code_analyzer_resolves_defaults() {
        let params: InvertedIndexParams = serde_json::from_value(json!({
            "analyzer": "code"
        }))
        .unwrap();
        assert_eq!(params.base_tokenizer, "code");
        assert!(!params.split_identifiers);
        assert!(params.split_on_numerics);
        assert!(params.preserve_original);
        assert!(params.lower_case);
        assert!(params.ascii_folding);
        assert!(!params.stem);
        assert!(!params.remove_stop_words);
        assert!(!params.index_operators);
    }

    #[test]
    fn test_analyzer_profile_resolves_to_persisted_params() {
        let from_profile: InvertedIndexParams = serde_json::from_value(json!({
            "analyzer": "code",
            "split_identifiers": true,
            "preserve_original": false
        }))
        .unwrap();
        let from_base_tokenizer: InvertedIndexParams = serde_json::from_value(json!({
            "base_tokenizer": "code",
            "split_identifiers": true,
            "preserve_original": false
        }))
        .unwrap();
        let from_constructor = InvertedIndexParams::new("code".to_string(), Language::English)
            .split_identifiers(true)
            .preserve_original(false);

        assert_eq!(from_profile, from_base_tokenizer);
        assert_eq!(from_profile, from_constructor);

        let persisted = serde_json::to_value(&from_profile).unwrap();
        assert!(persisted.get("analyzer").is_none());
        assert_eq!(
            serde_json::from_value::<InvertedIndexParams>(persisted).unwrap(),
            from_profile
        );
    }

    #[test]
    fn test_block_size_256_defaults_to_v3() {
        assert_eq!(
            InvertedIndexParams::default()
                .block_size(256)
                .unwrap()
                .resolved_format_version(),
            InvertedListFormatVersion::V3
        );
    }

    #[test]
    fn test_code_analyzer_defaults_to_v3_for_supported_block_sizes() {
        assert_eq!(
            InvertedIndexParams::code().resolved_format_version(),
            InvertedListFormatVersion::V3
        );
        assert_eq!(
            InvertedIndexParams::code()
                .block_size(256)
                .unwrap()
                .resolved_format_version(),
            InvertedListFormatVersion::V3
        );
    }

    #[test]
    fn test_training_json_uses_v3_for_code_analyzer_and_supported_block_sizes() {
        for block_size in [128, 256] {
            let params = InvertedIndexParams::from_training_json(
                &serde_json::json!({
                    "base_tokenizer": "code",
                    "block_size": block_size,
                })
                .to_string(),
            )
            .unwrap();
            assert_eq!(params.format_version, Some(InvertedListFormatVersion::V3));
        }
    }

    #[test]
    fn test_text_analyzer_replaces_code_defaults() {
        let params = InvertedIndexParams::code().analyzer("text").unwrap();
        assert_eq!(params, InvertedIndexParams::default());
    }

    #[test]
    fn test_code_analyzer_rejects_conflicting_base_tokenizer() {
        let err = serde_json::from_value::<InvertedIndexParams>(json!({
            "analyzer": "code",
            "base_tokenizer": "simple"
        }))
        .unwrap_err();
        assert!(err.to_string().contains("requires base_tokenizer='code'"));
    }

    #[test]
    fn test_build_code_tokenizer_does_not_split_identifiers_by_default() {
        let mut tokenizer = InvertedIndexParams::code().build().unwrap();
        let mut stream = tokenizer.token_stream_for_doc(
            "getUserName XMLHttpRequest parseHTML2JSON utf8_reader SCREAMING_SNAKE_CASE",
        );
        let mut tokens = Vec::new();
        stream.process(&mut |token| {
            tokens.push((token.text.clone(), token.position, token.position_length))
        });
        assert_eq!(
            tokens,
            vec![
                ("getusername".to_string(), 0, 1),
                ("xmlhttprequest".to_string(), 1, 1),
                ("parsehtml2json".to_string(), 2, 1),
                ("utf8_reader".to_string(), 3, 1),
                ("screaming_snake_case".to_string(), 4, 1),
            ]
        );
    }

    #[test]
    fn test_build_code_tokenizer_with_identifier_splitting() {
        let mut tokenizer = InvertedIndexParams::code()
            .split_identifiers(true)
            .build()
            .unwrap();
        let mut stream = tokenizer.token_stream_for_doc(
            "getUserName XMLHttpRequest parseHTML2JSON utf8_reader SCREAMING_SNAKE_CASE",
        );
        let mut tokens = Vec::new();
        stream.process(&mut |token| {
            tokens.push((token.text.clone(), token.position, token.position_length))
        });
        assert_eq!(
            tokens,
            vec![
                ("getusername".to_string(), 0, 3),
                ("get".to_string(), 0, 1),
                ("user".to_string(), 1, 1),
                ("name".to_string(), 2, 1),
                ("xmlhttprequest".to_string(), 3, 3),
                ("xml".to_string(), 3, 1),
                ("http".to_string(), 4, 1),
                ("request".to_string(), 5, 1),
                ("parsehtml2json".to_string(), 6, 4),
                ("parse".to_string(), 6, 1),
                ("html".to_string(), 7, 1),
                ("2".to_string(), 8, 1),
                ("json".to_string(), 9, 1),
                ("utf8_reader".to_string(), 10, 3),
                ("utf".to_string(), 10, 1),
                ("8".to_string(), 11, 1),
                ("reader".to_string(), 12, 1),
                ("screaming_snake_case".to_string(), 13, 3),
                ("screaming".to_string(), 13, 1),
                ("snake".to_string(), 14, 1),
                ("case".to_string(), 15, 1),
            ]
        );
    }

    #[test]
    fn test_inverted_details_round_trip_code_params() {
        let params = InvertedIndexParams::code()
            .with_position(true)
            .index_operators(true)
            .preserve_original(false)
            .format_version(InvertedListFormatVersion::V3);
        let details = crate::pbold::InvertedIndexDetails::try_from(&params).unwrap();
        let code_config = details.code_config.as_ref().unwrap();
        assert_eq!(code_config.split_on_numerics, Some(true));
        assert_eq!(code_config.preserve_original, Some(false));
        let round_tripped = InvertedIndexParams::try_from(&details).unwrap();
        assert_eq!(round_tripped, params);
    }

    #[test]
    fn test_inverted_details_uses_code_defaults_for_absent_flags() {
        let mut details =
            crate::pbold::InvertedIndexDetails::try_from(&InvertedIndexParams::code()).unwrap();
        let code_config = details.code_config.as_mut().unwrap();
        code_config.split_on_numerics = None;
        code_config.preserve_original = None;

        let params = InvertedIndexParams::try_from(&details).unwrap();
        assert!(params.split_on_numerics);
        assert!(params.preserve_original);
    }

    #[test]
    fn test_inverted_details_rejects_code_config_for_text_tokenizer() {
        let mut details =
            crate::pbold::InvertedIndexDetails::try_from(&InvertedIndexParams::code()).unwrap();
        details.base_tokenizer = Some("simple".to_string());

        let err = InvertedIndexParams::try_from(&details).unwrap_err();
        assert!(matches!(&err, Error::InvalidInput { .. }));
        assert!(
            err.to_string()
                .contains("code_config requires base_tokenizer='code'")
        );
    }

    #[test]
    fn test_inverted_details_does_not_persist_document_tokenizer() {
        let params = InvertedIndexParams::default().lance_tokenizer("json".to_string());
        let details = crate::pbold::InvertedIndexDetails::try_from(&params).unwrap();
        let round_tripped = InvertedIndexParams::try_from(&details).unwrap();

        assert_eq!(round_tripped.lance_tokenizer, None);
    }

    #[test]
    fn test_format_version_must_match_block_size() {
        InvertedIndexParams::default()
            .format_version(InvertedListFormatVersion::V2)
            .validate_format_version()
            .unwrap();
        InvertedIndexParams::default()
            .block_size(256)
            .unwrap()
            .validate_format_version()
            .unwrap();
        InvertedIndexParams::default()
            .format_version(InvertedListFormatVersion::V3)
            .validate_format_version()
            .unwrap();
        InvertedIndexParams::default()
            .block_size(256)
            .unwrap()
            .format_version(InvertedListFormatVersion::V3)
            .validate_format_version()
            .unwrap();

        let err = InvertedIndexParams::default()
            .block_size(256)
            .unwrap()
            .format_version(InvertedListFormatVersion::V2)
            .validate_format_version()
            .unwrap_err();
        assert!(err.to_string().contains("block_size=256"));

        let err = InvertedIndexParams::code()
            .format_version(InvertedListFormatVersion::V2)
            .validate_format_version()
            .unwrap_err();
        assert!(err.to_string().contains("requires FTS format_version=3"));
    }

    #[test]
    fn test_training_json_rejects_incompatible_format_version_and_block_size() {
        let err =
            InvertedIndexParams::from_training_json(r#"{"block_size": 256, "format_version": 2}"#)
                .unwrap_err();
        assert!(err.to_string().contains("block_size=256"));
    }

    #[test]
    fn test_training_json_invalid_numeric_format_version_includes_value() {
        let err = InvertedIndexParams::from_training_json(r#"{"format_version": -1}"#).unwrap_err();
        assert!(matches!(&err, lance_core::Error::Arrow { .. }));
        assert!(err.to_string().contains("got -1"));
    }

    #[test]
    fn test_training_json_ignores_unknown_fields() {
        let params = InvertedIndexParams::from_training_json(
            r#"{
                "lower_case": false,
                "skip_merge": true,
                "future_parameter": {"enabled": true}
            }"#,
        )
        .unwrap();

        assert!(!params.lower_case);
        let normalized = params.to_training_json().unwrap();
        assert!(normalized.get("skip_merge").is_none());
        assert!(normalized.get("future_parameter").is_none());
    }

    #[test]
    fn test_block_size_default_serializes() {
        let params = InvertedIndexParams::default();
        assert_eq!(params.block_size, 128);
        let json = serde_json::to_value(&params).unwrap();
        assert_eq!(json.get("block_size"), Some(&serde_json::Value::from(128)));
    }

    #[test]
    fn test_block_size_missing_metadata_falls_back_to_128() {
        let mut json = serde_json::to_value(InvertedIndexParams::default()).unwrap();
        json.as_object_mut().unwrap().remove("block_size");

        let params: InvertedIndexParams = serde_json::from_value(json).unwrap();
        assert_eq!(params.block_size, 128);
    }

    #[test]
    fn test_block_size_details_conversion() {
        let params = InvertedIndexParams::default().block_size(256).unwrap();
        let details = pbold::InvertedIndexDetails::try_from(&params).unwrap();
        assert_eq!(details.block_size, Some(256));

        let old_details = pbold::InvertedIndexDetails {
            base_tokenizer: Some("simple".to_string()),
            language: serde_json::to_string(&Language::English).unwrap(),
            with_position: false,
            max_token_length: Some(40),
            lower_case: true,
            stem: true,
            remove_stop_words: true,
            ascii_folding: true,
            min_ngram_length: 3,
            max_ngram_length: 3,
            prefix_only: false,
            block_size: None,
            code_config: None,
            document_granularity: PbDocumentGranularity::Row as i32,
            posting_format_version: None,
        };
        let params = InvertedIndexParams::try_from(&old_details).unwrap();
        assert_eq!(params.block_size, 128);
        assert_eq!(params.get_document_granularity(), DocumentGranularity::Row);
    }

    #[test]
    fn test_document_granularity_details_conversion() {
        let params =
            InvertedIndexParams::default().document_granularity(DocumentGranularity::ListElement);
        let details = pbold::InvertedIndexDetails::try_from(&params).unwrap();
        assert_eq!(details.posting_format_version, Some(2));
        assert_eq!(
            details.document_granularity,
            PbDocumentGranularity::ListElement as i32
        );
        let roundtrip = InvertedIndexParams::try_from(&details).unwrap();
        assert_eq!(
            roundtrip.get_document_granularity(),
            DocumentGranularity::ListElement
        );
    }

    #[rstest]
    #[case::block_size_128(128)]
    #[case::block_size_256(256)]
    fn test_block_size_accepts_supported_values(#[case] block_size: usize) {
        let params = InvertedIndexParams::default()
            .block_size(block_size)
            .unwrap();
        assert_eq!(params.block_size, block_size);

        let roundtrip: InvertedIndexParams =
            serde_json::from_value(serde_json::to_value(&params).unwrap()).unwrap();
        assert_eq!(roundtrip.block_size, block_size);
    }

    #[test]
    fn test_block_size_rejects_invalid_values() {
        let err = InvertedIndexParams::default().block_size(129).unwrap_err();
        assert!(err.to_string().contains("block_size"));

        let err = InvertedIndexParams::default().block_size(512).unwrap_err();
        assert!(err.to_string().contains("128 or 256"));

        let mut json = serde_json::to_value(InvertedIndexParams::default()).unwrap();
        json.as_object_mut()
            .unwrap()
            .insert("block_size".to_string(), serde_json::Value::from(1024));
        let err = serde_json::from_value::<InvertedIndexParams>(json).unwrap_err();
        assert!(err.to_string().contains("128 or 256"));
    }

    #[test]
    fn test_build_icu_tokenizer() {
        let mut tokenizer = InvertedIndexParams::default()
            .base_tokenizer("icu".to_string())
            .stem(false)
            .remove_stop_words(false)
            .build()
            .unwrap();
        let mut stream = tokenizer.token_stream_for_doc("Hello, こんにちは世界!");
        let mut tokens = Vec::new();
        stream.process(&mut |token| tokens.push(token.text.clone()));
        assert_eq!(tokens, vec!["hello", "こんにちは", "世界"]);
    }

    #[test]
    fn test_build_icu_tokenizer_with_split_on_non_alphanumeric() {
        let mut tokenizer = InvertedIndexParams::default()
            .base_tokenizer("icu/split".to_string())
            .stem(false)
            .remove_stop_words(false)
            .build()
            .unwrap();
        let mut stream = tokenizer.token_stream_for_doc("hello_world こんにちは世界 alpha.beta");
        let mut tokens = Vec::new();
        stream.process(&mut |token| tokens.push(token.text.clone()));
        assert_eq!(
            tokens,
            vec!["hello", "world", "こんにちは", "世界", "alpha", "beta"]
        );
    }

    #[test]
    fn test_remove_stop_words_respects_language_for_non_icu_tokenizer() {
        let mut tokenizer = InvertedIndexParams::default()
            .stem(false)
            .base_tokenizer("simple".to_string())
            .build()
            .unwrap();
        let mut stream = tokenizer.token_stream_for_search("the 的 lance data");
        let mut tokens = Vec::new();
        while let Some(token) = stream.next() {
            tokens.push(token.text.clone());
        }
        assert_eq!(
            tokens,
            vec!["的".to_string(), "lance".to_string(), "data".to_string()]
        );
    }

    #[test]
    fn test_custom_stop_words_replace_language_builtins() {
        let mut tokenizer = InvertedIndexParams::default()
            .stem(false)
            .custom_stop_words(Some(vec!["lance".to_string()]))
            .build()
            .unwrap();
        let mut stream = tokenizer.token_stream_for_search("the lance data");
        let mut tokens = Vec::new();
        while let Some(token) = stream.next() {
            tokens.push(token.text.clone());
        }
        assert_eq!(tokens, vec!["the".to_string(), "data".to_string()]);
    }

    #[rstest]
    #[case::icu("icu")]
    #[case::icu_split("icu/split")]
    fn test_icu_stop_words_use_all_builtin_lists(#[case] base_tokenizer: &str) {
        let mut tokenizer = InvertedIndexParams::default()
            .stem(false)
            .base_tokenizer(base_tokenizer.to_string())
            .build()
            .unwrap();
        let mut stream = tokenizer.token_stream_for_search("the 的 lance data");
        let mut tokens = Vec::new();
        while let Some(token) = stream.next() {
            tokens.push(token.text.clone());
        }
        assert_eq!(tokens, vec!["lance".to_string(), "data".to_string()]);
    }

    // Common English pronouns/function words such as `you`/`my`/`your`/`we`
    // must be removed by the ICU `all()` stop-word path. These are among the
    // highest-frequency tokens, so leaking them builds pathologically large
    // single-term posting lists (and previously overflowed the u32 posting-list
    // size counter, panicking the whole index build). The leak is independent
    // of stemming, so we assert it for both stem=false and stem=true.
    #[rstest]
    #[case::icu_no_stem("icu", false)]
    #[case::icu_stem("icu", true)]
    #[case::icu_split_no_stem("icu/split", false)]
    #[case::icu_split_stem("icu/split", true)]
    // `simple` is the recommended tokenizer for monolingual English corpora and
    // uses StopWordFilter::new(English) rather than the ICU all() path, so it
    // must be covered too.
    #[case::simple_no_stem("simple", false)]
    #[case::simple_stem("simple", true)]
    fn test_icu_common_english_stop_words_do_not_leak(
        #[case] base_tokenizer: &str,
        #[case] stem: bool,
    ) {
        let mut tokenizer = InvertedIndexParams::default()
            .base_tokenizer(base_tokenizer.to_string())
            .stem(stem)
            .remove_stop_words(true)
            .build()
            .unwrap();
        let mut stream = tokenizer.token_stream_for_search("you my your we lance data");
        let tokens: Vec<String> = std::iter::from_fn(|| stream.next().map(|t| t.text.clone()))
            .filter(|t| matches!(t.as_str(), "you" | "my" | "your" | "we"))
            .collect();
        assert!(
            tokens.is_empty(),
            "common English stop words leaked through the icu pipeline (stem={stem}): {tokens:?}"
        );
    }

    // Common Chinese function words/particles (了 是 在 的 和 有 我) are the
    // highest-frequency Chinese tokens; like the English pronouns they must be
    // removed by the ICU `all()` stop-word path so they don't build huge
    // posting lists. Real content words (英语 = "English", 数据 = "data") must
    // survive. ICU dictionary segmentation splits the input into words, so this
    // exercises the CJK stop-word path end to end.
    #[rstest]
    #[case::icu("icu")]
    #[case::icu_split("icu/split")]
    fn test_icu_common_chinese_stop_words_do_not_leak(#[case] base_tokenizer: &str) {
        let mut tokenizer = InvertedIndexParams::default()
            .base_tokenizer(base_tokenizer.to_string())
            .stem(true)
            .remove_stop_words(true)
            .build()
            .unwrap();
        let mut stream = tokenizer.token_stream_for_search("我 在 有 了 是 的 和 英语 数据");
        let tokens: Vec<String> =
            std::iter::from_fn(|| stream.next().map(|t| t.text.clone())).collect();
        let stop = ["我", "在", "有", "了", "是", "的", "和"];
        let leaked: Vec<&String> = tokens
            .iter()
            .filter(|t| stop.contains(&t.as_str()))
            .collect();
        assert!(
            leaked.is_empty(),
            "common Chinese stop words leaked through the icu pipeline: {leaked:?} (all tokens: {tokens:?})"
        );
        // The real content words must still be indexed.
        assert!(
            tokens.iter().any(|t| t == "英语"),
            "content word 英语 was dropped: {tokens:?}"
        );
    }
}
