// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::Result;

use lance_table::format::IndexMetadata;

use crate::scalar::inverted::DocumentGranularity;

/// A set of criteria used to filter potential indices to use for a query
#[derive(Debug, Default)]
pub struct IndexCriteria<'a> {
    /// Only consider indices for this column (this also means the index
    /// maps to a single column)
    pub for_column: Option<&'a str>,
    /// Only consider indices with this name
    pub has_name: Option<&'a str>,
    /// If true, only consider indices that support FTS
    pub must_support_fts: bool,
    /// Logical FTS document boundary. FTS lookups default to row documents.
    pub fts_document_granularity: Option<DocumentGranularity>,
    /// If true, only consider indices that support exact equality
    pub must_support_exact_equality: bool,
}

impl<'a> IndexCriteria<'a> {
    /// Only consider indices for this column (this also means the index
    /// maps to a single column)
    pub fn for_column(mut self, column: &'a str) -> Self {
        self.for_column = Some(column);
        self
    }

    /// Only consider indices with this name
    pub fn with_name(mut self, name: &'a str) -> Self {
        self.has_name = Some(name);
        self
    }

    /// Only consider indices that support FTS
    pub fn supports_fts(mut self) -> Self {
        self.must_support_fts = true;
        self
    }

    /// Select an FTS index with the requested logical document boundary.
    pub fn with_fts_document_granularity(
        mut self,
        document_granularity: DocumentGranularity,
    ) -> Self {
        self.fts_document_granularity = Some(document_granularity);
        self
    }

    /// Only consider indices that support exact equality
    ///
    /// This will disqualify, for example, the ngram and inverted indices
    /// or an index like a bloom filter
    pub fn supports_exact_equality(mut self) -> Self {
        self.must_support_exact_equality = true;
        self
    }
}

#[deprecated(since = "0.39.0", note = "Use IndexCriteria instead")]
pub type ScalarIndexCriteria<'a> = IndexCriteria<'a>;

/// Options for prewarming an inverted index.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FtsPrewarmOptions {
    /// If true, prewarm positions along with posting lists.
    pub with_position: bool,
}

impl FtsPrewarmOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_position(mut self, with_position: bool) -> Self {
        self.with_position = with_position;
        self
    }
}

/// Options for prewarming an index.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrewarmOptions {
    Fts(FtsPrewarmOptions),
}

/// Additional information about an index
///
/// Note that a single index might consist of multiple segments.  Each segment has its own
/// UUID and collection of files and covers some subset of the data fragments.
///
/// All segments in an index should have the same index type and index details.
pub trait IndexDescription: Send + Sync {
    /// Returns the index name
    ///
    /// This is the user-defined name of the index.  It is shared by all segments of the index
    /// and is what is used to refer to the index in the API.  It is guaranteed to be unique
    /// within the dataset.
    fn name(&self) -> &str;

    /// Returns the index metadata
    ///
    /// This is the raw metadata information stored in the manifest.  There is one
    /// IndexMetadata for each segment of the index.
    fn metadata(&self) -> &[IndexMetadata];

    /// Returns the physical index segments that make up this logical index.
    ///
    /// This is an alias for [`Self::metadata`] with a less ambiguous name.
    fn segments(&self) -> &[IndexMetadata] {
        self.metadata()
    }

    /// Returns the index type URL
    ///
    /// This is extracted from the type url of the index details
    fn type_url(&self) -> &str;

    /// Returns the index type
    ///
    /// This is a short string identifier that is friendlier than the type URL but not
    /// guaranteed to be unique.
    ///
    /// This is calculated by the plugin and will be "Unknown" if no plugin could be found
    /// for the type URL.
    fn index_type(&self) -> &str;

    /// Returns the number of rows indexed by the index, across all segments.
    ///
    /// This is an approximate count and may include rows that have been
    /// deleted.
    fn rows_indexed(&self) -> u64;

    /// Returns the ids of the fields that the index is built on.
    fn field_ids(&self) -> &[u32];

    /// Returns a JSON string representation of the index details
    ///
    /// The format of these details will vary depending on the index type and
    /// since indexes can be provided by plugins we cannot fully define it here.
    ///
    /// However, plugins should do their best to maintain backwards compatibility
    /// and consider this method part of the public API.
    ///
    /// See individual index plugins for more description of the expected format.
    ///
    /// The conversion from Any to JSON is controlled by the index
    /// plugin.  As a result, this method may fail if there is no plugin
    /// available for the index.
    fn details(&self) -> Result<String>;

    /// Returns the total size in bytes of all files across all segments.
    ///
    /// Returns `None` if file size information is not available for any segment
    /// (for backward compatibility with indices created before file tracking was added).
    fn total_size_bytes(&self) -> Option<u64>;
}
