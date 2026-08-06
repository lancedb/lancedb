// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! MemTableScanner builder for creating query execution plans.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::{Expr, SessionContext};
use datafusion_physical_expr::PhysicalExprRef;
use futures::TryStreamExt;
use lance_core::{Error, ROW_ID, Result};
use lance_datafusion::expr::safe_coerce_scalar;
use lance_datafusion::planner::Planner;
use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::inverted::query::{FtsQuery as IndexFtsQuery, Operator};
use lance_index::scalar::inverted::{DOC_INDEX_FIELD, DocumentGranularity};
use lance_linalg::distance::DistanceType;

use super::exec::{
    BTreeIndexExec, FtsIndexExec, MemTableBruteForceVectorExec, MemTableDedupScanExec,
    MemTableScanExec, SCORE_COLUMN, VectorIndexExec,
};
use crate::dataset::mem_wal::scanner::{exec::validate_pk_types, parse_filter_expr};
use crate::dataset::mem_wal::write::{BatchStore, IndexStore};

/// Vector search query parameters.
#[derive(Debug, Clone)]
pub struct VectorQuery {
    /// Column name containing vectors.
    pub column: String,
    /// Query vector.
    pub query_vector: Arc<dyn Array>,
    /// Number of results to return.
    pub k: usize,
    /// The minimum number of probes to search. More partitions may be searched
    /// if needed to satisfy k results or recall requirements. Defaults to 1.
    pub nprobes: usize,
    /// The maximum number of probes to search. If None, all partitions may be
    /// searched if needed to satisfy k results.
    pub maximum_nprobes: Option<usize>,
    /// Distance metric type. If None, uses the index's metric.
    pub distance_type: Option<DistanceType>,
    /// Number of candidates to reserve for HNSW search.
    pub ef: Option<usize>,
    /// Refine factor for re-ranking results using original vectors.
    pub refine_factor: Option<u32>,
    /// The lower bound (inclusive) of the distance to be searched.
    pub distance_lower_bound: Option<f32>,
    /// The upper bound (exclusive) of the distance to be searched.
    pub distance_upper_bound: Option<f32>,
}

/// Full-text search query type.
#[derive(Debug, Clone)]
pub enum FtsQueryType {
    /// Simple term match.
    Match {
        /// The search query string.
        query: String,
        /// The operator used to combine tokenized query terms.
        operator: Operator,
        /// Boost factor applied to the score.
        boost: f32,
    },
    /// Phrase query with slop.
    Phrase {
        /// The phrase to search for.
        query: String,
        /// Maximum allowed distance between consecutive tokens.
        slop: u32,
    },
    /// Boolean query with MUST/SHOULD/MUST_NOT.
    Boolean {
        /// Terms that must match and contribute to the score.
        must: Vec<String>,
        /// Terms that should match (adds to score).
        should: Vec<String>,
        /// Terms that must not match.
        must_not: Vec<String>,
    },
    /// Fuzzy match query with typo tolerance.
    Fuzzy {
        /// The search query string.
        query: String,
        /// Maximum edit distance (Levenshtein distance).
        /// None means auto-fuzziness based on token length.
        fuzziness: Option<u32>,
        /// Number of initial characters that must match exactly.
        prefix_length: u32,
        /// Maximum number of terms to expand to.
        max_expansions: usize,
        /// Boost factor applied to the score.
        boost: f32,
    },
}

/// Full-text search query parameters.
#[derive(Debug, Clone)]
pub struct FtsQuery {
    /// Column name to search.
    pub column: String,
    /// Query type.
    pub query_type: FtsQueryType,
    /// Logical document unit. Defaults to one document per dataset row.
    pub document_granularity: DocumentGranularity,
    /// WAND factor for early termination (0.0 to 1.0).
    /// 1.0 = full recall (default), <1.0 = faster but may miss low-scoring results.
    pub wand_factor: f32,
    /// Query-level result limit.
    pub limit: Option<usize>,
    /// Whether to also search the mutable tail (rows written since the last
    /// freeze). `true` (default) = read-your-writes; `false` = search only the
    /// immutable frozen partitions (the Lucene model), trading read-recency for
    /// query latency. See [`crate::dataset::mem_wal::index::SearchOptions`].
    pub include_tail: bool,
}

/// Default maximum number of fuzzy expansions.
pub const DEFAULT_MAX_EXPANSIONS: usize = 50;

/// Default WAND factor for full recall (no early termination).
pub const DEFAULT_WAND_FACTOR: f32 = 1.0;

impl FtsQuery {
    /// Create a simple term match query.
    pub fn match_query(column: impl Into<String>, query: impl Into<String>) -> Self {
        Self::match_query_with_operator(column, query, Operator::Or)
    }

    pub fn match_query_with_operator(
        column: impl Into<String>,
        query: impl Into<String>,
        operator: Operator,
    ) -> Self {
        Self {
            column: column.into(),
            query_type: FtsQueryType::Match {
                query: query.into(),
                operator,
                boost: 1.0,
            },
            document_granularity: DocumentGranularity::Row,
            wand_factor: DEFAULT_WAND_FACTOR,
            limit: None,
            include_tail: true,
        }
    }

    /// Create a phrase query.
    pub fn phrase(column: impl Into<String>, query: impl Into<String>, slop: u32) -> Self {
        Self {
            column: column.into(),
            query_type: FtsQueryType::Phrase {
                query: query.into(),
                slop,
            },
            document_granularity: DocumentGranularity::Row,
            wand_factor: DEFAULT_WAND_FACTOR,
            limit: None,
            include_tail: true,
        }
    }

    /// Create a Boolean query.
    pub fn boolean(
        column: impl Into<String>,
        must: Vec<String>,
        should: Vec<String>,
        must_not: Vec<String>,
    ) -> Self {
        Self {
            column: column.into(),
            query_type: FtsQueryType::Boolean {
                must,
                should,
                must_not,
            },
            document_granularity: DocumentGranularity::Row,
            wand_factor: DEFAULT_WAND_FACTOR,
            limit: None,
            include_tail: true,
        }
    }

    /// Create a fuzzy match query with auto-fuzziness.
    ///
    /// Auto-fuzziness is calculated based on token length:
    /// - 0-2 chars: 0 (exact match)
    /// - 3-5 chars: 1 edit allowed
    /// - 6+ chars: 2 edits allowed
    pub fn fuzzy(column: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            query_type: FtsQueryType::Fuzzy {
                query: query.into(),
                fuzziness: None,
                prefix_length: 0,
                max_expansions: DEFAULT_MAX_EXPANSIONS,
                boost: 1.0,
            },
            document_granularity: DocumentGranularity::Row,
            wand_factor: DEFAULT_WAND_FACTOR,
            limit: None,
            include_tail: true,
        }
    }

    /// Create a fuzzy match query with specified edit distance.
    pub fn fuzzy_with_distance(
        column: impl Into<String>,
        query: impl Into<String>,
        fuzziness: u32,
    ) -> Self {
        Self {
            column: column.into(),
            query_type: FtsQueryType::Fuzzy {
                query: query.into(),
                fuzziness: Some(fuzziness),
                prefix_length: 0,
                max_expansions: DEFAULT_MAX_EXPANSIONS,
                boost: 1.0,
            },
            document_granularity: DocumentGranularity::Row,
            wand_factor: DEFAULT_WAND_FACTOR,
            limit: None,
            include_tail: true,
        }
    }

    /// Create a fuzzy match query with full options.
    pub fn fuzzy_with_options(
        column: impl Into<String>,
        query: impl Into<String>,
        fuzziness: Option<u32>,
        prefix_length: u32,
        max_expansions: usize,
    ) -> Self {
        Self {
            column: column.into(),
            query_type: FtsQueryType::Fuzzy {
                query: query.into(),
                fuzziness,
                prefix_length,
                max_expansions,
                boost: 1.0,
            },
            document_granularity: DocumentGranularity::Row,
            wand_factor: DEFAULT_WAND_FACTOR,
            limit: None,
            include_tail: true,
        }
    }

    /// Set the WAND factor for early termination.
    ///
    /// - 1.0 = full recall (default)
    /// - 0.5 = prune documents scoring below 50% of the k-th best score
    /// - 0.0 = only return the absolute best match
    pub fn with_wand_factor(mut self, wand_factor: f32) -> Self {
        self.wand_factor = wand_factor.clamp(0.0, 1.0);
        self
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    /// Set whether to search the mutable tail (read-your-writes) or only the
    /// immutable frozen partitions (the Lucene model). Default `true`.
    pub fn with_include_tail(mut self, include_tail: bool) -> Self {
        self.include_tail = include_tail;
        self
    }

    pub fn with_document_granularity(mut self, document_granularity: DocumentGranularity) -> Self {
        self.document_granularity = document_granularity;
        self
    }

    fn with_boost(mut self, boost: f32) -> Self {
        match &mut self.query_type {
            FtsQueryType::Match { boost: b, .. } | FtsQueryType::Fuzzy { boost: b, .. } => {
                *b = boost;
            }
            FtsQueryType::Phrase { .. } | FtsQueryType::Boolean { .. } => {}
        }
        self
    }
}

/// Convert an index-level [`FullTextSearchQuery`] into the MemTable's local
/// [`FtsQuery`], so the MemTable scanner shares the dataset `Scanner`'s FTS
/// entry type. Supports match (exact `fuzziness == Some(0)` and fuzzy) and
/// phrase leaf queries; the column must be bound on the query. Compound queries
/// (boolean / boost / multi-match) cannot be modeled by the MemTable path and
/// return a `not_supported` error rather than failing deep in planning.
fn resolve_memtable_document_granularity(
    column: &str,
    requested: Option<DocumentGranularity>,
    indexes: Option<&IndexStore>,
) -> Result<DocumentGranularity> {
    let available = indexes
        .map(|indexes| indexes.fts_document_granularities_by_column(column))
        .unwrap_or_default();
    match requested {
        Some(requested) if available.is_empty() || available.contains(&requested) => Ok(requested),
        Some(requested) => Err(Error::invalid_input(format!(
            "FTS query for field '{column}' requested {requested:?} document granularity, but \
             the active MemTable FTS index uses a different granularity: {available:?}"
        ))),
        None if available.is_empty() => Ok(DocumentGranularity::Row),
        None if available.len() == 1 => Ok(available[0]),
        None => Err(Error::invalid_input(format!(
            "FTS query for field '{column}' is ambiguous because Row and ListElement active \
             MemTable indexes coexist; specify document_granularity"
        ))),
    }
}

fn local_fts_query(query: FullTextSearchQuery, indexes: Option<&IndexStore>) -> Result<FtsQuery> {
    let wand_factor = query.wand_factor.unwrap_or(DEFAULT_WAND_FACTOR);
    let limit = query
        .limit
        .map(|limit| {
            if limit < 0 {
                Err(Error::invalid_input(
                    "full-text search limit must be non-negative".to_string(),
                ))
            } else {
                Ok(limit as usize)
            }
        })
        .transpose()?;
    let require_column = |column: Option<String>| {
        column.ok_or_else(|| {
            Error::invalid_input(
                "full-text search requires a column; set it with \
                 `FullTextSearchQuery::with_column`"
                    .to_string(),
            )
        })
    };
    let local = match query.query {
        IndexFtsQuery::Match(m) => {
            let column = require_column(m.column)?;
            let document_granularity =
                resolve_memtable_document_granularity(&column, m.document_granularity, indexes)?;
            let local = match m.fuzziness {
                // Some(0) is an exact match in the index model.
                Some(0) => FtsQuery::match_query_with_operator(column, m.terms, m.operator)
                    .with_boost(m.boost),
                _ if m.operator != Operator::Or => {
                    return Err(Error::not_supported(
                        "MemTable fuzzy full-text search only supports OR match operators"
                            .to_string(),
                    ));
                }
                fuzziness => FtsQuery::fuzzy_with_options(
                    column,
                    m.terms,
                    fuzziness,
                    m.prefix_length,
                    m.max_expansions,
                )
                .with_boost(m.boost),
            };
            local.with_document_granularity(document_granularity)
        }
        IndexFtsQuery::Phrase(p) => {
            let column = require_column(p.column)?;
            let document_granularity =
                resolve_memtable_document_granularity(&column, p.document_granularity, indexes)?;
            FtsQuery::phrase(column, p.terms, p.slop)
                .with_document_granularity(document_granularity)
        }
        other => {
            return Err(Error::not_supported(format!(
                "MemTable full-text search supports match and phrase queries, got: {other}"
            )));
        }
    };
    Ok(local.with_wand_factor(wand_factor).with_limit(limit))
}

/// Scalar predicate for BTree index queries.
#[derive(Debug, Clone)]
pub enum ScalarPredicate {
    /// Exact match: column = value.
    Eq { column: String, value: ScalarValue },
    /// Range query: column in [lower, upper).
    Range {
        column: String,
        lower: Option<ScalarValue>,
        upper: Option<ScalarValue>,
    },
    /// IN query: column in (values...).
    In {
        column: String,
        values: Vec<ScalarValue>,
    },
}

impl ScalarPredicate {
    /// Get the column name for this predicate.
    pub fn column(&self) -> &str {
        match self {
            Self::Eq { column, .. } => column,
            Self::Range { column, .. } => column,
            Self::In { column, .. } => column,
        }
    }
}

/// Scanner builder for querying MemTable data.
///
/// Provides a builder pattern similar to Lance's Scanner interface
/// for constructing DataFusion execution plans over in-memory data.
///
/// # Index Visibility Model
///
/// The scanner captures `visible_count` from the `IndexStore` at
/// construction time. This frozen visibility ensures queries only see data
/// that has been indexed, providing consistent results.
///
/// # Example
///
/// The builder methods take `&mut self` (mirroring
/// [`crate::dataset::scanner::Scanner`]), so configure the scanner with
/// statements rather than a fluent chain:
///
/// ```ignore
/// let mut scanner = MemTableScanner::new(batch_store, indexes, schema);
/// scanner.project(&["id", "name"])?;
/// scanner.filter("id > 10")?;
/// scanner.limit(Some(100), None)?;
///
/// let stream = scanner.try_into_stream().await?;
/// ```
pub struct MemTableScanner {
    batch_store: Arc<BatchStore>,
    indexes: Arc<IndexStore>,
    schema: SchemaRef,
    /// Frozen visibility captured at scanner construction time.
    /// This is the `visible_count` from the IndexStore.
    visible_count: usize,
    projection: Option<Vec<String>>,
    filter: Option<Expr>,
    limit: Option<usize>,
    offset: Option<usize>,
    nearest: Option<VectorQuery>,
    full_text_query: Option<FtsQuery>,
    use_index: bool,
    batch_size: Option<usize>,
    /// Whether to include _rowid column in output.
    /// In MemTable, _rowid is the row_position (global row offset).
    with_row_id: bool,
    /// Whether to include _rowaddr column in output.
    /// Same value as _rowid but named for compatibility with LSM scanner.
    with_row_address: bool,
    /// Primary-key columns, supplied by the LSM planner. When set, a filtered
    /// vector/FTS search evaluates the predicate against the newest version of
    /// each PK only, so an in-memtable update whose current version fails the
    /// predicate is excluded rather than leaking a stale older match.
    pk_columns: Option<Vec<String>>,
}

impl MemTableScanner {
    /// Create a new scanner.
    ///
    /// Captures `visible_count` from the `IndexStore` at construction
    /// time to ensure consistent query visibility.
    ///
    /// # Arguments
    ///
    /// * `batch_store` - Lock-free batch store containing the data
    /// * `indexes` - Index registry (carries the visibility watermark)
    /// * `schema` - Schema of the data
    pub fn new(batch_store: Arc<BatchStore>, indexes: Arc<IndexStore>, schema: SchemaRef) -> Self {
        // Snapshot the visibility cursor at construction time. The cursor is
        // advanced by `flush_from_batch_store` after the WAL append succeeds,
        // so this snapshot reflects WAL-durable data.
        let visible_count = indexes.visible_count();

        Self {
            batch_store,
            indexes,
            schema,
            visible_count,
            projection: None,
            filter: None,
            limit: None,
            offset: None,
            nearest: None,
            full_text_query: None,
            use_index: true,
            batch_size: None,
            with_row_id: false,
            with_row_address: false,
            pk_columns: None,
        }
    }

    /// Provide the primary-key columns. When set, a filtered vector/FTS search
    /// evaluates the predicate against the newest version of each PK only,
    /// preventing a stale older match from leaking past an in-memtable update
    /// whose current version fails the predicate.
    pub fn with_pk_columns(&mut self, pk_columns: Vec<String>) -> &mut Self {
        self.pk_columns = if pk_columns.is_empty() {
            None
        } else {
            Some(pk_columns)
        };
        self
    }

    /// Project only the specified columns. Mirrors
    /// [`crate::dataset::scanner::Scanner::project`].
    ///
    /// Special columns:
    /// - `_rowid`: Returns the row position (global row offset in MemTable)
    pub fn project<T: AsRef<str>>(&mut self, columns: &[T]) -> Result<&mut Self> {
        // Check if _rowid is requested in projection
        let mut filtered_columns = Vec::new();
        for col in columns {
            let col = col.as_ref();
            if col == ROW_ID {
                self.with_row_id = true;
            } else {
                filtered_columns.push(col.to_string());
            }
        }
        // Only set projection if there are non-special columns
        if !filtered_columns.is_empty() || self.with_row_id {
            self.projection = Some(filtered_columns);
        }
        Ok(self)
    }

    /// Include the _rowid column in output.
    ///
    /// In MemTable, _rowid is the row_position (global row offset).
    pub fn with_row_id(&mut self) -> &mut Self {
        self.with_row_id = true;
        self
    }

    /// The `visible_count` snapshot this scanner latched at
    /// construction. A downstream recency filter must key on this same snapshot
    /// (not a fresh read of the IndexStore watermark, which a concurrent append
    /// could have advanced) so it stays consistent with the rows the search saw.
    pub fn visible_count(&self) -> usize {
        self.visible_count
    }

    /// Include the _rowaddr column in output.
    ///
    /// Same value as _rowid but named for compatibility with LSM scanner.
    /// Used when scanning MemTable as part of a unified LSM scan.
    pub fn with_row_address(&mut self) -> &mut Self {
        self.with_row_address = true;
        self
    }

    /// Set a filter expression using SQL-like syntax.
    pub fn filter(&mut self, filter_expr: &str) -> Result<&mut Self> {
        let expr = parse_filter_expr(self.schema.as_ref(), filter_expr)?;
        self.filter = Some(expr);
        Ok(self)
    }

    /// Set a filter expression directly.
    pub fn filter_expr(&mut self, expr: Expr) -> &mut Self {
        self.filter = Some(expr);
        self
    }

    /// Limit the number of results, with an optional offset. Mirrors
    /// [`crate::dataset::scanner::Scanner::limit`]: both bounds are `Option<i64>`
    /// and must be non-negative.
    pub fn limit(&mut self, limit: Option<i64>, offset: Option<i64>) -> Result<&mut Self> {
        if let Some(value) = limit
            && value < 0
        {
            return Err(Error::invalid_input(
                "limit must be non-negative".to_string(),
            ));
        }
        if let Some(value) = offset
            && value < 0
        {
            return Err(Error::invalid_input(
                "offset must be non-negative".to_string(),
            ));
        }
        self.limit = limit.map(|value| value as usize);
        self.offset = offset.map(|value| value as usize);
        Ok(self)
    }

    /// Set up a vector similarity search. Mirrors
    /// [`crate::dataset::scanner::Scanner::nearest`] — the query vector is passed
    /// by reference.
    ///
    /// # Arguments
    ///
    /// * `column` - The name of the vector column to search.
    /// * `query` - The query vector.
    /// * `k` - Number of nearest neighbors to return.
    pub fn nearest(&mut self, column: &str, query: &dyn Array, k: usize) -> Result<&mut Self> {
        if k == 0 {
            return Err(Error::invalid_input("k must be positive".to_string()));
        }
        if query.is_empty() {
            return Err(Error::invalid_input(
                "query vector must have non-zero length".to_string(),
            ));
        }
        self.nearest = Some(VectorQuery {
            column: column.to_string(),
            query_vector: query.slice(0, query.len()),
            k,
            nprobes: 1,
            maximum_nprobes: None,
            distance_type: None,
            ef: None,
            refine_factor: None,
            distance_lower_bound: None,
            distance_upper_bound: None,
        });
        Ok(self)
    }

    /// Set the number of probes for IVF search.
    ///
    /// This is a convenience method that sets both minimum and maximum nprobes
    /// to the same value, guaranteeing exactly `n` partitions will be searched.
    pub fn nprobes(&mut self, n: usize) -> &mut Self {
        if let Some(ref mut q) = self.nearest {
            q.nprobes = n;
            q.maximum_nprobes = Some(n);
        } else {
            log::warn!("nprobes is not set because nearest has not been called yet");
        }
        self
    }

    /// Set the minimum number of probes for IVF search.
    ///
    /// This is the minimum number of partitions to search. More partitions may be
    /// searched if needed to satisfy k results or recall requirements. Defaults to 1.
    pub fn minimum_nprobes(&mut self, n: usize) -> &mut Self {
        if let Some(ref mut q) = self.nearest {
            q.nprobes = n;
        } else {
            log::warn!("minimum_nprobes is not set because nearest has not been called yet");
        }
        self
    }

    /// Set the maximum number of probes for IVF search.
    ///
    /// If not set, all partitions may be searched if needed to satisfy k results.
    pub fn maximum_nprobes(&mut self, n: usize) -> &mut Self {
        if let Some(ref mut q) = self.nearest {
            q.maximum_nprobes = Some(n);
        } else {
            log::warn!("maximum_nprobes is not set because nearest has not been called yet");
        }
        self
    }

    /// Set the distance metric type for vector search.
    ///
    /// If not set, uses the index's default metric type.
    pub fn distance_metric(&mut self, metric: DistanceType) -> &mut Self {
        if let Some(ref mut q) = self.nearest {
            q.distance_type = Some(metric);
        } else {
            log::warn!("distance_metric is not set because nearest has not been called yet");
        }
        self
    }

    /// Set the ef parameter for HNSW search.
    ///
    /// The number of candidates to reserve while searching. This controls the
    /// accuracy/speed tradeoff for HNSW-based indices.
    pub fn ef(&mut self, ef: usize) -> &mut Self {
        if let Some(ref mut q) = self.nearest {
            q.ef = Some(ef);
        } else {
            log::warn!("ef is not set because nearest has not been called yet");
        }
        self
    }

    /// Set the refine factor for re-ranking results.
    ///
    /// When set, the search will first retrieve `k * refine_factor` candidates
    /// using the approximate index, then re-rank them using the original vectors.
    pub fn refine(&mut self, factor: u32) -> &mut Self {
        if let Some(ref mut q) = self.nearest {
            q.refine_factor = Some(factor);
        } else {
            log::warn!("refine is not set because nearest has not been called yet");
        }
        self
    }

    /// Set the distance range for filtering results.
    ///
    /// * `lower` - The lower bound (inclusive) of the distance.
    /// * `upper` - The upper bound (exclusive) of the distance.
    pub fn distance_range(&mut self, lower: Option<f32>, upper: Option<f32>) -> &mut Self {
        if let Some(ref mut q) = self.nearest {
            q.distance_lower_bound = lower;
            q.distance_upper_bound = upper;
        } else {
            log::warn!("distance_range is not set because nearest has not been called yet");
        }
        self
    }

    /// Set up a full-text search. Mirrors
    /// [`crate::dataset::scanner::Scanner::full_text_search`], taking a
    /// [`FullTextSearchQuery`] whose column is set via
    /// `FullTextSearchQuery::with_column`. Match (exact/fuzzy) and phrase leaf
    /// queries are supported; compound queries (boolean/boost/multi-match) are
    /// not yet supported by the MemTable path and return an error.
    pub fn full_text_search(&mut self, query: FullTextSearchQuery) -> Result<&mut Self> {
        self.full_text_query = Some(local_fts_query(query, Some(self.indexes.as_ref()))?);
        Ok(self)
    }

    /// Set up a full-text phrase search.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to search.
    /// * `phrase` - The phrase to search for.
    /// * `slop` - Maximum allowed distance between consecutive tokens.
    ///   0 means exact phrase match (tokens must be adjacent).
    pub fn full_text_phrase(&mut self, column: &str, phrase: &str, slop: u32) -> &mut Self {
        self.full_text_query = Some(FtsQuery::phrase(column, phrase, slop));
        self
    }

    /// Set up a full-text Boolean search.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to search.
    /// * `must` - Terms that must match (intersection).
    /// * `should` - Terms that should match (adds to score).
    /// * `must_not` - Terms that must not match (exclusion).
    pub fn full_text_boolean(
        &mut self,
        column: &str,
        must: Vec<String>,
        should: Vec<String>,
        must_not: Vec<String>,
    ) -> &mut Self {
        self.full_text_query = Some(FtsQuery::boolean(column, must, should, must_not));
        self
    }

    /// Set up a full-text fuzzy search with auto-fuzziness.
    ///
    /// Auto-fuzziness is calculated based on token length:
    /// - 0-2 chars: 0 (exact match)
    /// - 3-5 chars: 1 edit allowed
    /// - 6+ chars: 2 edits allowed
    ///
    /// # Arguments
    ///
    /// * `column` - The column to search.
    /// * `query` - The search query (may contain typos).
    pub fn full_text_fuzzy(&mut self, column: &str, query: &str) -> &mut Self {
        self.full_text_query = Some(FtsQuery::fuzzy(column, query));
        self
    }

    /// Set up a full-text fuzzy search with specified edit distance.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to search.
    /// * `query` - The search query (may contain typos).
    /// * `fuzziness` - Maximum edit distance (Levenshtein distance).
    pub fn full_text_fuzzy_with_distance(
        &mut self,
        column: &str,
        query: &str,
        fuzziness: u32,
    ) -> &mut Self {
        self.full_text_query = Some(FtsQuery::fuzzy_with_distance(column, query, fuzziness));
        self
    }

    /// Set up a full-text fuzzy search with full options.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to search.
    /// * `query` - The search query (may contain typos).
    /// * `fuzziness` - Maximum edit distance. None means auto-fuzziness.
    /// * `max_expansions` - Maximum number of terms to expand to.
    pub fn full_text_fuzzy_with_options(
        &mut self,
        column: &str,
        query: &str,
        fuzziness: Option<u32>,
        max_expansions: usize,
    ) -> &mut Self {
        self.full_text_query = Some(FtsQuery::fuzzy_with_options(
            column,
            query,
            fuzziness,
            0,
            max_expansions,
        ));
        self
    }

    /// Set the WAND factor for FTS queries to control performance/recall tradeoff.
    ///
    /// This only applies when a full-text query is set.
    ///
    /// - 1.0 = full recall (default)
    /// - 0.5 = prune documents scoring below 50% of the k-th best score
    /// - 0.0 = only return the absolute best match
    ///
    /// # Arguments
    ///
    /// * `wand_factor` - Value between 0.0 and 1.0
    pub fn fts_wand_factor(&mut self, wand_factor: f32) -> &mut Self {
        if let Some(ref mut q) = self.full_text_query {
            q.wand_factor = wand_factor.clamp(0.0, 1.0);
        } else {
            log::warn!(
                "fts_wand_factor is not set because full_text_query has not been called yet"
            );
        }
        self
    }

    /// Choose whether FTS searches the mutable tail (read-your-writes, default)
    /// or only the immutable frozen partitions (the Lucene model — lower latency,
    /// does not reflect rows written since the last freeze). Only applies when a
    /// full-text query is set.
    pub fn fts_include_tail(&mut self, include_tail: bool) -> &mut Self {
        if let Some(ref mut q) = self.full_text_query {
            q.include_tail = include_tail;
        } else {
            log::warn!(
                "fts_include_tail is not set because full_text_query has not been called yet"
            );
        }
        self
    }

    /// Enable or disable index usage.
    pub fn use_index(&mut self, use_index: bool) -> &mut Self {
        self.use_index = use_index;
        self
    }

    /// Set the batch size for output.
    pub fn batch_size(&mut self, size: usize) -> &mut Self {
        self.batch_size = Some(size);
        self
    }

    /// Execute the scan and return a stream of record batches.
    pub async fn try_into_stream(&self) -> Result<SendableRecordBatchStream> {
        let plan = self.create_plan().await?;
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        plan.execute(0, task_ctx)
            .map_err(|e| Error::io(format!("Failed to execute plan: {}", e)))
    }

    /// Execute the scan and collect all results into a single RecordBatch.
    pub async fn try_into_batch(&self) -> Result<RecordBatch> {
        let plan = self.create_plan().await?;
        let output_schema = plan.schema();
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let stream = plan
            .execute(0, task_ctx)
            .map_err(|e| Error::io(format!("Failed to execute plan: {}", e)))?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| Error::io(format!("Failed to collect batches: {}", e)))?;

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(output_schema));
        }

        arrow_select::concat::concat_batches(&output_schema, &batches)
            .map_err(|e| Error::io(format!("Failed to concatenate batches: {}", e)))
    }

    /// Count the number of rows that match the query.
    pub async fn count_rows(&self) -> Result<u64> {
        let stream = self.try_into_stream().await?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| Error::io(format!("Failed to count rows: {}", e)))?;

        Ok(batches.iter().map(|b| b.num_rows() as u64).sum())
    }

    /// Get the output schema after projection.
    ///
    /// If `with_row_id` is true, adds `_rowid` column at the end.
    /// If `with_row_address` is true, adds `_rowaddr` column at the end.
    pub fn output_schema(&self) -> SchemaRef {
        use super::exec::ROW_ADDRESS_COLUMN;

        let mut fields: Vec<Field> = if let Some(ref projection) = self.projection {
            projection
                .iter()
                .filter_map(|name| self.schema.field_with_name(name).ok().cloned())
                .collect()
        } else {
            self.schema
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect()
        };

        // Add _rowid column if requested
        if self.with_row_id {
            fields.push(Field::new(ROW_ID, DataType::UInt64, true));
        }

        // Add _rowaddr column if requested
        if self.with_row_address {
            fields.push(Field::new(ROW_ADDRESS_COLUMN, DataType::UInt64, true));
        }

        Arc::new(arrow_schema::Schema::new(fields))
    }

    /// Get the base output schema after projection, WITHOUT special columns like _rowid.
    /// This is used by index execs that add their own special columns.
    fn base_output_schema(&self) -> SchemaRef {
        let fields: Vec<Field> = if let Some(ref projection) = self.projection {
            projection
                .iter()
                .filter_map(|name| self.schema.field_with_name(name).ok().cloned())
                .collect()
        } else {
            self.schema
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect()
        };
        Arc::new(arrow_schema::Schema::new(fields))
    }

    /// Create the execution plan based on the query configuration.
    pub async fn create_plan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        if self.nearest.is_some() && self.full_text_query.is_some() {
            return Err(Error::invalid_input(
                "MemTableScanner cannot combine vector and full-text search".to_string(),
            ));
        }

        // Determine which type of plan to create
        if let Some(ref vector_query) = self.nearest {
            return self.plan_vector_search(vector_query).await;
        }

        if let Some(ref fts_query) = self.full_text_query {
            return self.plan_fts_search(fts_query).await;
        }

        // Check if we can use a BTree index for the filter
        if self.use_index
            && let Some(predicate) = self.extract_btree_predicate()
            && self.has_btree_index(predicate.column())
        {
            return self.plan_btree_query(&predicate).await;
        }

        // Fall back to full scan
        self.plan_full_scan().await
    }

    /// Plan a full table scan.
    async fn plan_full_scan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let projection_indices = self.compute_projection_indices()?;

        // Build filter predicate if present
        // Note: optimize_expr() must be called before create_physical_expr() to handle
        // type coercion (e.g., Int64 literal -> Int32 to match column type)
        let (filter_predicate, filter_expr) = if let Some(ref filter) = self.filter {
            let planner = Planner::new(self.schema.clone());
            let optimized = planner.optimize_expr(filter.clone())?;
            let predicate = planner.create_physical_expr(&optimized)?;
            (Some(predicate), Some(optimized))
        } else {
            (None, None)
        };

        let scan = MemTableScanExec::with_filter(
            self.batch_store.clone(),
            self.visible_count,
            projection_indices,
            self.output_schema(),
            self.schema.clone(),
            self.with_row_id,
            self.with_row_address,
            filter_predicate,
            filter_expr,
        );

        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(scan);

        // Apply limit / offset if present.
        if self.limit.is_some() || self.offset.unwrap_or(0) > 0 {
            plan = Arc::new(GlobalLimitExec::new(
                plan,
                self.offset.unwrap_or(0),
                self.limit,
            ));
        }

        Ok(plan)
    }

    /// Plan a newest-per-PK active-arm scan via `MemTableDedupScanExec` —
    /// dedup runs before the predicate so a PK whose newest version fails the
    /// filter cannot leak an older version that passes. Unlike
    /// `plan_full_scan`, this never takes the BTree skip (dedup needs
    /// every version) and never pushes a limit (the LSM caps results above
    /// the cross-source merge).
    pub async fn create_dedup_plan(&self, pk_columns: &[String]) -> Result<Arc<dyn ExecutionPlan>> {
        validate_pk_types(&self.schema, pk_columns)?;

        let pk_indices = pk_columns
            .iter()
            .map(|name| {
                self.schema
                    .column_with_name(name)
                    .map(|(idx, _)| idx)
                    .ok_or_else(|| {
                        Error::invalid_input(format!(
                            "Primary key column '{}' not found in schema",
                            name
                        ))
                    })
            })
            .collect::<Result<Vec<usize>>>()?;

        let projection_indices = self.compute_projection_indices()?;

        // optimize_expr() must run before create_physical_expr() for type coercion.
        let (filter_predicate, filter_expr) = if let Some(ref filter) = self.filter {
            let planner = Planner::new(self.schema.clone());
            let optimized = planner.optimize_expr(filter.clone())?;
            let predicate = planner.create_physical_expr(&optimized)?;
            (Some(predicate), Some(optimized))
        } else {
            (None, None)
        };

        Ok(Arc::new(MemTableDedupScanExec::new(
            self.batch_store.clone(),
            self.visible_count,
            projection_indices,
            self.output_schema(),
            pk_indices,
            self.with_row_id,
            self.with_row_address,
            filter_predicate,
            filter_expr,
        )))
    }

    /// Plan a BTree index query.
    ///
    /// Uses the effective visibility (min of max_visible and max_indexed) to ensure
    /// queries only see indexed data. Falls back to full scan if no index exists.
    async fn plan_btree_query(
        &self,
        predicate: &ScalarPredicate,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !self.has_btree_index(predicate.column()) {
            return self.plan_full_scan().await;
        }

        let max_visible = self.visible_count;
        let projection_indices = self.compute_projection_indices()?;

        let index_exec = BTreeIndexExec::new(
            self.batch_store.clone(),
            self.indexes.clone(),
            predicate.clone(),
            max_visible,
            projection_indices,
            self.output_schema(),
            self.with_row_id,
            self.with_row_address,
        )?;
        self.apply_post_index_ops(Arc::new(index_exec)).await
    }

    /// Plan a vector similarity search.
    ///
    /// Always emits a plan whose output schema includes `_distance`: dispatches
    /// to [`VectorIndexExec`] when an HNSW exists for the column, otherwise to
    /// [`MemTableBruteForceVectorExec`]. The brute-force arm exists because the
    /// active memtable is the LSM's unindexed-rows path — when the HNSW config
    /// hasn't reached this writer yet (cold-start, or rows written between an
    /// index commit and the next memtable rotation), KNN must still produce
    /// correct, distance-bearing results so the LSM-level merge stays sound.
    /// Compile the optional logical `filter` into a physical predicate against
    /// the memtable schema. Shared by the vector and FTS search arms; mirrors the
    /// compilation in [`Self::plan_full_scan`] (`optimize_expr` before
    /// `create_physical_expr` for literal type coercion).
    fn filter_predicate(&self) -> Result<Option<PhysicalExprRef>> {
        let Some(ref filter) = self.filter else {
            return Ok(None);
        };
        let planner = Planner::new(self.schema.clone());
        let optimized = planner.optimize_expr(filter.clone())?;
        Ok(Some(planner.create_physical_expr(&optimized)?))
    }

    async fn plan_vector_search(&self, query: &VectorQuery) -> Result<Arc<dyn ExecutionPlan>> {
        let max_visible = self.visible_count;
        let projection_indices = self.compute_projection_indices()?;
        let base_schema = self.base_output_schema();
        let filter_predicate = self.filter_predicate()?;
        if let Some(pk_columns) = &self.pk_columns {
            validate_pk_types(&self.schema, pk_columns)?;
        }

        // With a prefilter we use brute force rather than HNSW because graph
        // traversal cannot honor an arbitrary predicate. With PK rewrites, we
        // also need exact newest-before-top-k semantics: a stale near vector
        // must not consume an HNSW top-k slot and hide the next live row. Pure
        // append-only PK data can still use HNSW safely. This relies on
        // `IndexStore` marking PK overrides before advancing the visible batch
        // watermark, so any snapshot that sees a rewrite also sees the flag.
        let hnsw_safe_with_pk = self
            .pk_columns
            .as_ref()
            .map(|_| self.indexes.has_pk_index() && !self.indexes.pk_has_overrides())
            .unwrap_or(true);
        let exec: Arc<dyn ExecutionPlan> = if filter_predicate.is_none()
            && hnsw_safe_with_pk
            && self.has_vector_index(&query.column)
        {
            Arc::new(VectorIndexExec::new(
                self.batch_store.clone(),
                self.indexes.clone(),
                query.clone(),
                max_visible,
                projection_indices,
                base_schema,
                self.with_row_id,
            )?)
        } else {
            Arc::new(
                MemTableBruteForceVectorExec::new(
                    self.batch_store.clone(),
                    query.clone(),
                    max_visible,
                    projection_indices,
                    base_schema,
                    self.with_row_id,
                )?
                .with_filter(filter_predicate)
                .with_pk_columns(self.pk_columns.clone()),
            )
        };
        self.apply_post_index_ops(exec).await
    }

    /// Plan a full-text search.
    ///
    /// Uses the effective visibility (min of max_visible and max_indexed) to ensure
    /// queries only see indexed data.
    async fn plan_fts_search(&self, query: &FtsQuery) -> Result<Arc<dyn ExecutionPlan>> {
        if !self.has_fts_index(&query.column, query.document_granularity) {
            return self.empty_fts_plan(query.document_granularity);
        }

        let max_visible = self.visible_count;
        let projection_indices = self.compute_projection_indices()?;
        let filter_predicate = self.filter_predicate()?;
        if let Some(pk_columns) = &self.pk_columns {
            validate_pk_types(&self.schema, pk_columns)?;
        }

        let index_exec = FtsIndexExec::new(
            self.batch_store.clone(),
            self.indexes.clone(),
            query.clone(),
            max_visible,
            projection_indices,
            self.base_output_schema(),
            self.with_row_id,
        )?
        .with_filter(filter_predicate)
        .with_pk_columns(self.pk_columns.clone());
        self.apply_post_index_ops(Arc::new(index_exec)).await
    }

    fn empty_fts_plan(
        &self,
        document_granularity: DocumentGranularity,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_plan::empty::EmptyExec;

        let mut fields: Vec<Field> = self
            .base_output_schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        if document_granularity.is_list_element() {
            fields.push(DOC_INDEX_FIELD.clone());
        }
        fields.push(Field::new(SCORE_COLUMN, DataType::Float32, true));
        if self.with_row_id {
            fields.push(Field::new(ROW_ID, DataType::UInt64, true));
        }
        let schema = Arc::new(arrow_schema::Schema::new(fields));
        Ok(Arc::new(EmptyExec::new(schema)))
    }

    /// Apply limit and other post-processing operations.
    async fn apply_post_index_ops(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut result = plan;

        if self.limit.is_some() || self.offset.unwrap_or(0) > 0 {
            result = Arc::new(GlobalLimitExec::new(
                result,
                self.offset.unwrap_or(0),
                self.limit,
            ));
        }

        Ok(result)
    }

    /// Compute column indices for projection.
    fn compute_projection_indices(&self) -> Result<Option<Vec<usize>>> {
        if let Some(ref columns) = self.projection {
            let indices: Result<Vec<usize>> = columns
                .iter()
                .map(|name| {
                    self.schema
                        .column_with_name(name)
                        .map(|(idx, _)| idx)
                        .ok_or_else(|| {
                            Error::invalid_input(format!("Column '{}' not found in schema", name))
                        })
                })
                .collect();
            Ok(Some(indices?))
        } else {
            Ok(None)
        }
    }

    /// Extract a BTree-compatible predicate from the filter.
    ///
    /// This method also coerces literal values to match the column's data type
    /// (e.g., Int64 literal -> Int32 when the column is Int32).
    fn extract_btree_predicate(&self) -> Option<ScalarPredicate> {
        let filter = self.filter.as_ref()?;

        // Simple pattern matching for common predicates
        match filter {
            Expr::BinaryExpr(binary) => {
                if let (Expr::Column(col), Expr::Literal(lit, _)) =
                    (binary.left.as_ref(), binary.right.as_ref())
                {
                    // Coerce literal to match column type
                    let coerced_lit = self.coerce_literal_to_column(&col.name, lit)?;

                    match binary.op {
                        datafusion::logical_expr::Operator::Eq => {
                            return Some(ScalarPredicate::Eq {
                                column: col.name.clone(),
                                value: coerced_lit,
                            });
                        }
                        datafusion::logical_expr::Operator::Lt => {
                            return Some(ScalarPredicate::Range {
                                column: col.name.clone(),
                                lower: None,
                                upper: Some(coerced_lit),
                            });
                        }
                        datafusion::logical_expr::Operator::GtEq => {
                            return Some(ScalarPredicate::Range {
                                column: col.name.clone(),
                                lower: Some(coerced_lit),
                                upper: None,
                            });
                        }
                        _ => {}
                    }
                }
            }
            Expr::InList(in_list) if !in_list.negated => {
                if let Expr::Column(col) = in_list.expr.as_ref() {
                    let values: Vec<ScalarValue> = in_list
                        .list
                        .iter()
                        .filter_map(|e| {
                            if let Expr::Literal(lit, _) = e {
                                // Coerce each literal to match column type
                                self.coerce_literal_to_column(&col.name, lit)
                            } else {
                                None
                            }
                        })
                        .collect();

                    if values.len() == in_list.list.len() {
                        return Some(ScalarPredicate::In {
                            column: col.name.clone(),
                            values,
                        });
                    }
                }
            }
            _ => {}
        }

        None
    }

    /// Coerce a literal value to match the column's data type.
    fn coerce_literal_to_column(&self, column: &str, lit: &ScalarValue) -> Option<ScalarValue> {
        let field = self.schema.field_with_name(column).ok()?;
        let target_type = field.data_type();

        // If types already match, return as-is
        if &lit.data_type() == target_type {
            return Some(lit.clone());
        }

        // Use safe_coerce_scalar to convert the value
        safe_coerce_scalar(lit, target_type)
    }

    /// Check if a BTree index exists for a column.
    fn has_btree_index(&self, column: &str) -> bool {
        self.indexes.get_btree_by_column(column).is_some()
    }

    /// Check if a vector index exists for a column.
    fn has_vector_index(&self, column: &str) -> bool {
        self.indexes.get_hnsw_by_column(column).is_some()
    }

    /// Check if an FTS index exists for a column.
    fn has_fts_index(&self, column: &str, document_granularity: DocumentGranularity) -> bool {
        self.indexes
            .get_fts_by_column_and_granularity(column, document_granularity)
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn create_test_batch(schema: &Schema, start_id: i32, count: usize) -> RecordBatch {
        let ids: Vec<i32> = (start_id..start_id + count as i32).collect();
        let names: Vec<String> = ids.iter().map(|id| format!("name_{}", id)).collect();

        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    /// Create an IndexStore and insert batches with batch position tracking.
    fn create_index_store_with_batches(
        batch_store: &Arc<BatchStore>,
        schema: &Schema,
        batches: &[(i32, usize)], // (start_id, count)
    ) -> Arc<IndexStore> {
        let mut index_store = IndexStore::new();
        // Add a btree index on "id" column
        index_store.add_btree("id_idx".to_string(), 0, "id".to_string());

        let mut row_offset = 0u64;
        for (batch_pos, (start_id, count)) in batches.iter().enumerate() {
            let batch = create_test_batch(schema, *start_id, *count);
            batch_store.append(batch.clone()).unwrap();

            // Insert into indexes with batch position tracking
            index_store
                .insert_with_batch_position(&batch, row_offset, Some(batch_pos))
                .unwrap();

            row_offset += *count as u64;
        }

        Arc::new(index_store)
    }

    #[tokio::test]
    async fn test_scanner_basic_scan() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        // Insert test data with index tracking
        let indexes = create_index_store_with_batches(&batch_store, &schema, &[(0, 10)]);

        let scanner = MemTableScanner::new(batch_store, indexes, schema.clone());

        let result = scanner.try_into_batch().await.unwrap();
        assert_eq!(result.num_rows(), 10);
    }

    #[tokio::test]
    async fn test_scanner_visibility_filtering() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        // Create index store and insert 2 batches (positions 0, 1)
        let mut index_store = IndexStore::new();
        index_store.add_btree("id_idx".to_string(), 0, "id".to_string());

        let batch1 = create_test_batch(&schema, 0, 10);
        batch_store.append(batch1.clone()).unwrap();
        index_store
            .insert_with_batch_position(&batch1, 0, Some(0))
            .unwrap();

        let batch2 = create_test_batch(&schema, 10, 10);
        batch_store.append(batch2.clone()).unwrap();
        index_store
            .insert_with_batch_position(&batch2, 10, Some(1))
            .unwrap();

        // Add a third batch to batch_store but DON'T index it
        let batch3 = create_test_batch(&schema, 20, 10);
        batch_store.append(batch3).unwrap();

        // Scanner should only see indexed data (batches 0 and 1)
        let indexes = Arc::new(index_store);
        let scanner = MemTableScanner::new(batch_store, indexes, schema.clone());
        let result = scanner.try_into_batch().await.unwrap();
        // visible_count is 1, so we see batches 0 and 1 (20 rows)
        assert_eq!(result.num_rows(), 20);
    }

    #[tokio::test]
    async fn test_scanner_projection() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let indexes = create_index_store_with_batches(&batch_store, &schema, &[(0, 10)]);

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema.clone());
        scanner.project(&["id"]).unwrap();

        let result = scanner.try_into_batch().await.unwrap();
        assert_eq!(result.num_columns(), 1);
        assert_eq!(result.schema().field(0).name(), "id");
    }

    #[tokio::test]
    async fn test_scanner_limit() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let indexes = create_index_store_with_batches(&batch_store, &schema, &[(0, 100)]);

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema.clone());
        scanner.limit(Some(10), None).unwrap();

        let result = scanner.try_into_batch().await.unwrap();
        assert_eq!(result.num_rows(), 10);
    }

    #[tokio::test]
    async fn test_scanner_offset_without_limit() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let indexes = create_index_store_with_batches(&batch_store, &schema, &[(0, 10)]);

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema.clone());
        scanner.limit(Some(3), None).unwrap();
        scanner.limit(None, Some(2)).unwrap();

        let result = scanner.try_into_batch().await.unwrap();
        let ids = result
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(ids, vec![2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[tokio::test]
    async fn btree_filter_fallback_preserves_non_representable_predicates() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));
        let indexes = create_index_store_with_batches(&batch_store, &schema, &[(0, 10)]);

        async fn ids_for(
            batch_store: Arc<BatchStore>,
            indexes: Arc<IndexStore>,
            schema: SchemaRef,
            filter: &str,
        ) -> Vec<i32> {
            let mut scanner = MemTableScanner::new(batch_store, indexes, schema);
            scanner.filter(filter).unwrap();
            scanner
                .try_into_batch()
                .await
                .unwrap()
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        }

        assert_eq!(
            ids_for(
                batch_store.clone(),
                indexes.clone(),
                schema.clone(),
                "id NOT IN (1, 2)"
            )
            .await,
            vec![0, 3, 4, 5, 6, 7, 8, 9]
        );
        assert_eq!(
            ids_for(
                batch_store.clone(),
                indexes.clone(),
                schema.clone(),
                "id <= 5"
            )
            .await,
            vec![0, 1, 2, 3, 4, 5]
        );
        assert_eq!(
            ids_for(batch_store, indexes, schema, "id > 5").await,
            vec![6, 7, 8, 9]
        );
    }

    /// `full_text_search` now takes a structured `FullTextSearchQuery` (matching
    /// the dataset `Scanner`); `local_fts_query` maps the supported leaf shapes
    /// and rejects compound queries and missing columns.
    #[test]
    fn local_fts_query_maps_leaf_shapes_and_rejects_the_rest() {
        use lance_index::scalar::inverted::query::{
            BooleanQuery, MatchQuery, Occur, Operator, PhraseQuery,
        };

        // Exact match (default fuzziness Some(0)) -> local Match, preserving the
        // old `full_text_search(col, terms)` behavior.
        let q = FullTextSearchQuery::new("hello".to_string())
            .with_column("text".to_string())
            .unwrap();
        let local = local_fts_query(q, None).unwrap();
        assert_eq!(local.column, "text");
        assert!(
            matches!(local.query_type, FtsQueryType::Match { query, operator, .. }
                if query == "hello" && operator == Operator::Or)
        );

        let mut indexes = IndexStore::new();
        indexes
            .add_fts_with_params(
                "tags_list_element_idx".to_string(),
                1,
                "tags".to_string(),
                lance_index::scalar::inverted::InvertedIndexParams::default()
                    .document_granularity(DocumentGranularity::ListElement),
            )
            .unwrap();
        let inferred = FullTextSearchQuery::new("hello".to_string())
            .with_column("tags".to_string())
            .unwrap();
        let inferred = local_fts_query(inferred, Some(&indexes)).unwrap();
        assert_eq!(
            inferred.document_granularity,
            DocumentGranularity::ListElement
        );
        let conflicting = FullTextSearchQuery::new_query(IndexFtsQuery::Match(
            MatchQuery::new("hello".to_string())
                .with_column(Some("tags".to_string()))
                .with_document_granularity(DocumentGranularity::Row),
        ));
        assert!(
            local_fts_query(conflicting, Some(&indexes))
                .unwrap_err()
                .to_string()
                .contains("different granularity")
        );
        indexes
            .add_fts_with_params(
                "tags_idx".to_string(),
                1,
                "tags".to_string(),
                lance_index::scalar::inverted::InvertedIndexParams::default(),
            )
            .unwrap();
        let ambiguous = FullTextSearchQuery::new("hello".to_string())
            .with_column("tags".to_string())
            .unwrap();
        assert!(
            local_fts_query(ambiguous, Some(&indexes))
                .unwrap_err()
                .to_string()
                .contains("ambiguous")
        );

        let exact_and = FullTextSearchQuery::new_query(IndexFtsQuery::Match(
            MatchQuery::new("hello world".to_string())
                .with_operator(Operator::And)
                .with_boost(3.0)
                .with_column(Some("text".to_string())),
        ));
        let local = local_fts_query(exact_and, None).unwrap();
        assert!(
            matches!(local.query_type, FtsQueryType::Match { query, operator, boost }
                if query == "hello world" && operator == Operator::And && boost == 3.0)
        );

        // Fuzzy match -> local Fuzzy carrying edit distance, prefix length, and boost.
        let fuzzy = FullTextSearchQuery::new_query(IndexFtsQuery::Match(
            MatchQuery::new("lance".to_string())
                .with_fuzziness(Some(2))
                .with_prefix_length(2)
                .with_boost(2.5)
                .with_column(Some("text".to_string())),
        ));
        let local = local_fts_query(fuzzy, None).unwrap();
        assert!(
            matches!(local.query_type, FtsQueryType::Fuzzy { fuzziness, prefix_length, boost, .. }
                if fuzziness == Some(2) && prefix_length == 2 && boost == 2.5)
        );

        let fuzzy_and = FullTextSearchQuery::new_query(IndexFtsQuery::Match(
            MatchQuery::new("lance memwal".to_string())
                .with_operator(Operator::And)
                .with_fuzziness(Some(1))
                .with_column(Some("text".to_string())),
        ));
        assert!(
            local_fts_query(fuzzy_and, None).is_err(),
            "fuzzy AND cannot be represented by the local memtable query"
        );

        // Phrase -> local Phrase.
        let phrase = FullTextSearchQuery::new_query(IndexFtsQuery::Phrase(
            PhraseQuery::new("quick fox".to_string()).with_column(Some("text".to_string())),
        ));
        let local = local_fts_query(phrase, None).unwrap();
        assert!(matches!(local.query_type, FtsQueryType::Phrase { .. }));

        // Compound (boolean) -> not supported.
        let boolean =
            FullTextSearchQuery::new_query(IndexFtsQuery::Boolean(BooleanQuery::new(vec![(
                Occur::Must,
                IndexFtsQuery::Match(
                    MatchQuery::new("x".to_string()).with_column(Some("text".to_string())),
                ),
            )])));
        assert!(
            local_fts_query(boolean, None).is_err(),
            "boolean must be rejected"
        );

        // Missing column -> error.
        let no_col = FullTextSearchQuery::new("hi".to_string());
        assert!(
            local_fts_query(no_col, None).is_err(),
            "missing column must error"
        );
    }

    #[tokio::test]
    async fn full_text_search_honors_query_limit() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, true),
        ]));
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    "lance",
                    "lance filler",
                    "lance filler filler",
                ])),
            ],
        )
        .unwrap();
        let mut indexes = IndexStore::new();
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());
        batch_store.append(batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();

        let mut scanner = MemTableScanner::new(batch_store, Arc::new(indexes), schema);
        scanner
            .full_text_search(
                FullTextSearchQuery::new("lance".to_string())
                    .with_column("text".to_string())
                    .unwrap()
                    .limit(Some(1)),
            )
            .unwrap();

        let result = scanner.try_into_batch().await.unwrap();
        assert_eq!(
            result.num_rows(),
            1,
            "query-level FTS limit must cap direct MemTableScanner results"
        );
    }

    #[tokio::test]
    async fn full_text_search_without_index_returns_empty_score_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, true),
        ]));
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["needle", "needle"])),
            ],
        )
        .unwrap();
        batch_store.append(batch).unwrap();

        let mut scanner = MemTableScanner::new(batch_store, Arc::new(IndexStore::new()), schema);
        scanner
            .full_text_search(
                FullTextSearchQuery::new("needle".to_string())
                    .with_column("text".to_string())
                    .unwrap(),
            )
            .unwrap();

        let result = scanner.try_into_batch().await.unwrap();
        assert_eq!(result.num_rows(), 0);
        assert!(
            result.schema().field_with_name("_score").is_ok(),
            "missing FTS indexes should produce an empty FTS-shaped result"
        );
    }

    #[tokio::test]
    async fn full_text_search_prefilter_null_predicate_excludes_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
        ]));
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["needle", "needle", "needle"])),
                Arc::new(BooleanArray::from(vec![None, Some(true), Some(false)])),
            ],
        )
        .unwrap();
        let mut indexes = IndexStore::new();
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());
        batch_store.append(batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();

        let mut scanner = MemTableScanner::new(batch_store, Arc::new(indexes), schema);
        scanner.filter("active = true").unwrap();
        scanner
            .full_text_search(
                FullTextSearchQuery::new("needle".to_string())
                    .with_column("text".to_string())
                    .unwrap(),
            )
            .unwrap();

        let result = scanner.try_into_batch().await.unwrap();
        let ids = result
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(
            ids,
            vec![2],
            "NULL predicate results must be excluded from FTS prefilter candidates"
        );
    }

    #[tokio::test]
    async fn full_text_search_prefilter_disables_wand_pruning() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
        ]));
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alpha beta gamma delta", "alpha"])),
                Arc::new(BooleanArray::from(vec![Some(false), Some(true)])),
            ],
        )
        .unwrap();
        let mut indexes = IndexStore::new();
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());
        batch_store.append(batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();

        let mut scanner = MemTableScanner::new(batch_store, Arc::new(indexes), schema);
        scanner.filter("active = true").unwrap();
        scanner
            .full_text_search(
                FullTextSearchQuery::new("alpha beta gamma delta".to_string())
                    .with_column("text".to_string())
                    .unwrap()
                    .wand_factor(Some(0.99)),
            )
            .unwrap();

        let result = scanner.try_into_batch().await.unwrap();
        let ids = result
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(
            ids,
            vec![2],
            "filtered FTS must not let WAND prune rows before the prefilter is applied"
        );
    }

    #[tokio::test]
    async fn full_text_search_append_only_pk_keeps_wand_pruning() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, true),
        ]));
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alpha beta gamma delta", "alpha"])),
            ],
        )
        .unwrap();
        let mut indexes = IndexStore::new();
        indexes.enable_pk_index(&[("id".to_string(), 0)]);
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());
        batch_store.append(batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();

        let mut scanner = MemTableScanner::new(batch_store, Arc::new(indexes), schema);
        scanner.with_pk_columns(vec!["id".to_string()]);
        scanner
            .full_text_search(
                FullTextSearchQuery::new("alpha beta gamma delta".to_string())
                    .with_column("text".to_string())
                    .unwrap()
                    .wand_factor(Some(0.99)),
            )
            .unwrap();

        let result = scanner.try_into_batch().await.unwrap();
        let ids = result
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(
            ids,
            vec![1],
            "append-only PK data should keep index WAND pruning enabled"
        );
    }

    #[tokio::test]
    async fn full_text_search_with_pk_rewrite_disables_index_limit_pushdown() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, true),
        ]));
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    "alpha beta gamma delta epsilon",
                    "other",
                    "alpha beta gamma delta",
                    "alpha",
                ])),
            ],
        )
        .unwrap();
        let mut indexes = IndexStore::new();
        indexes.enable_pk_index(&[("id".to_string(), 0)]);
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());
        batch_store.append(batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();

        let mut scanner = MemTableScanner::new(batch_store, Arc::new(indexes), schema);
        scanner.with_pk_columns(vec!["id".to_string()]);
        scanner
            .full_text_search(
                FullTextSearchQuery::new("alpha beta gamma delta epsilon".to_string())
                    .with_column("text".to_string())
                    .unwrap()
                    .limit(Some(2)),
            )
            .unwrap();

        let result = scanner.try_into_batch().await.unwrap();
        let ids = result
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(
            ids,
            vec![2, 3],
            "FTS-only PK rewrites must disable index limit pushdown so live lower-scoring PKs can backfill"
        );
    }

    #[tokio::test]
    async fn full_text_search_with_pk_columns_drops_stale_filtered_hits() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
        ]));
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(StringArray::from(vec!["needle", "needle"])),
                Arc::new(BooleanArray::from(vec![true, false])),
            ],
        )
        .unwrap();
        let mut indexes = IndexStore::new();
        indexes.enable_pk_index(&[("id".to_string(), 0)]);
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());
        batch_store.append(batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();

        let mut scanner = MemTableScanner::new(batch_store, Arc::new(indexes), schema);
        scanner.with_pk_columns(vec!["id".to_string()]);
        scanner.filter("active = true").unwrap();
        scanner
            .full_text_search(
                FullTextSearchQuery::new("needle".to_string())
                    .with_column("text".to_string())
                    .unwrap(),
            )
            .unwrap();

        let result = scanner.try_into_batch().await.unwrap();
        assert_eq!(
            result.num_rows(),
            0,
            "the older matching version must not leak when the newest PK fails the filter"
        );
    }

    #[tokio::test]
    async fn full_text_search_with_pk_columns_falls_back_without_pk_index() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, true),
        ]));
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 2])),
                Arc::new(StringArray::from(vec![
                    "needle stale",
                    "other",
                    "other",
                    "needle fresh",
                ])),
            ],
        )
        .unwrap();
        let mut indexes = IndexStore::new();
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());
        batch_store.append(batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();

        let mut scanner = MemTableScanner::new(batch_store, Arc::new(indexes), schema);
        scanner.with_pk_columns(vec!["id".to_string()]);
        scanner
            .full_text_search(
                FullTextSearchQuery::new("needle".to_string())
                    .with_column("text".to_string())
                    .unwrap(),
            )
            .unwrap();

        let result = scanner
            .try_into_batch()
            .await
            .expect("FTS PK recency should fall back without a PK index");
        let ids = result
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(
            ids,
            vec![2],
            "without a PK index the batch-scan fallback must drop stale id=1 \
             but keep id=2 whose newest version still matches"
        );
    }

    #[tokio::test]
    async fn test_scanner_count_rows() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let indexes = create_index_store_with_batches(&batch_store, &schema, &[(0, 50)]);

        let scanner = MemTableScanner::new(batch_store, indexes, schema.clone());
        let count = scanner.count_rows().await.unwrap();
        assert_eq!(count, 50);
    }

    #[tokio::test]
    async fn test_scanner_with_row_id() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let indexes = create_index_store_with_batches(&batch_store, &schema, &[(0, 10)]);

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema.clone());
        scanner.with_row_id();

        // Verify output schema includes _rowid
        let output_schema = scanner.output_schema();
        assert_eq!(output_schema.fields().len(), 3);
        assert_eq!(output_schema.field(0).name(), "id");
        assert_eq!(output_schema.field(1).name(), "name");
        assert_eq!(output_schema.field(2).name(), "_rowid");
        assert_eq!(output_schema.field(2).data_type(), &DataType::UInt64);

        // Verify data includes correct row IDs
        let result = scanner.try_into_batch().await.unwrap();
        assert_eq!(result.num_columns(), 3);
        assert_eq!(result.schema().field(2).name(), "_rowid");

        let row_ids = result
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        assert_eq!(row_ids.len(), 10);
        // Row IDs should be 0-9 for a single batch
        for i in 0..10 {
            assert_eq!(row_ids.value(i), i as u64);
        }
    }

    #[tokio::test]
    async fn test_scanner_project_with_row_id() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let indexes = create_index_store_with_batches(&batch_store, &schema, &[(0, 10)]);

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema.clone());
        // Project only "id" and "_rowid"
        scanner.project(&["id", "_rowid"]).unwrap();

        // Verify output schema
        let output_schema = scanner.output_schema();
        assert_eq!(output_schema.fields().len(), 2);
        assert_eq!(output_schema.field(0).name(), "id");
        assert_eq!(output_schema.field(1).name(), "_rowid");

        // Verify data
        let result = scanner.try_into_batch().await.unwrap();
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.schema().field(0).name(), "id");
        assert_eq!(result.schema().field(1).name(), "_rowid");
    }

    #[tokio::test]
    async fn test_scanner_row_id_across_batches() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        // Insert two batches with 5 rows each
        let indexes = create_index_store_with_batches(&batch_store, &schema, &[(0, 5), (5, 5)]);

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema.clone());
        scanner.with_row_id();

        let result = scanner.try_into_batch().await.unwrap();
        assert_eq!(result.num_rows(), 10);

        let row_ids = result
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();

        // Row IDs should be 0-9 across both batches
        for i in 0..10 {
            assert_eq!(row_ids.value(i), i as u64);
        }
    }

    #[test]
    fn test_output_schema_with_row_id() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));
        let indexes = Arc::new(IndexStore::new());

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema);

        // Without with_row_id, schema should not include _rowid
        let output_schema = scanner.output_schema();
        assert_eq!(output_schema.fields().len(), 2);
        assert!(output_schema.field_with_name("_rowid").is_err());

        // With with_row_id, schema should include _rowid
        scanner.with_row_id();
        let output_schema = scanner.output_schema();
        assert_eq!(output_schema.fields().len(), 3);
        assert!(output_schema.field_with_name("_rowid").is_ok());
    }

    #[test]
    fn test_project_extracts_row_id() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));
        let indexes = Arc::new(IndexStore::new());

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema);

        // Project with _rowid should set with_row_id flag
        scanner.project(&["id", "_rowid"]).unwrap();

        // with_row_id should be true now
        assert!(scanner.with_row_id);

        // _rowid should not be in projection list (it's handled separately)
        assert_eq!(scanner.projection, Some(vec!["id".to_string()]));

        // Output schema should include _rowid at the end
        let output_schema = scanner.output_schema();
        assert_eq!(output_schema.fields().len(), 2);
        assert_eq!(output_schema.field(0).name(), "id");
        assert_eq!(output_schema.field(1).name(), "_rowid");
    }

    #[tokio::test]
    async fn test_scan_plan_with_row_id() {
        use crate::utils::test::assert_plan_node_equals;

        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let indexes = create_index_store_with_batches(&batch_store, &schema, &[(0, 10)]);

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema.clone());
        scanner.with_row_id();

        let plan = scanner.create_plan().await.unwrap();

        // Verify plan structure using assert_plan_node_equals
        assert_plan_node_equals(
            plan,
            "MemTableScanExec: projection=[id, name, _rowid], with_row_id=true",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_scan_plan_projection_with_row_id() {
        use crate::utils::test::assert_plan_node_equals;

        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let indexes = create_index_store_with_batches(&batch_store, &schema, &[(0, 10)]);

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema.clone());
        scanner.project(&["id", "_rowid"]).unwrap();

        let plan = scanner.create_plan().await.unwrap();

        // Verify plan structure with projection
        assert_plan_node_equals(
            plan,
            "MemTableScanExec: projection=[id, _rowid], with_row_id=true",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_scan_plan_without_row_id() {
        use crate::utils::test::assert_plan_node_equals;

        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let indexes = create_index_store_with_batches(&batch_store, &schema, &[(0, 10)]);

        let scanner = MemTableScanner::new(batch_store, indexes, schema.clone());

        let plan = scanner.create_plan().await.unwrap();

        // Verify plan structure without _rowid
        assert_plan_node_equals(
            plan,
            "MemTableScanExec: projection=[id, name], with_row_id=false",
        )
        .await
        .unwrap();
    }

    #[test]
    fn test_output_schema_with_row_address() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));
        let indexes = Arc::new(IndexStore::new());

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema);

        // Without with_row_address, schema should not include _rowaddr
        let output_schema = scanner.output_schema();
        assert_eq!(output_schema.fields().len(), 2);
        assert!(output_schema.field_with_name("_rowaddr").is_err());

        // With with_row_address, schema should include _rowaddr
        scanner.with_row_address();
        let output_schema = scanner.output_schema();
        assert_eq!(output_schema.fields().len(), 3);
        assert!(output_schema.field_with_name("_rowaddr").is_ok());
    }

    #[tokio::test]
    async fn test_scanner_with_row_address() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let indexes = create_index_store_with_batches(&batch_store, &schema, &[(0, 10)]);

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema.clone());
        scanner.with_row_address();

        // Verify output schema includes _rowaddr
        let output_schema = scanner.output_schema();
        assert_eq!(output_schema.fields().len(), 3);
        assert_eq!(output_schema.field(0).name(), "id");
        assert_eq!(output_schema.field(1).name(), "name");
        assert_eq!(output_schema.field(2).name(), "_rowaddr");
        assert_eq!(output_schema.field(2).data_type(), &DataType::UInt64);

        // Verify data includes correct row addresses
        let result = scanner.try_into_batch().await.unwrap();
        assert_eq!(result.num_columns(), 3);
        assert_eq!(result.schema().field(2).name(), "_rowaddr");

        let row_addrs = result
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        assert_eq!(row_addrs.len(), 10);
        // Row addresses should be 0-9 for a single batch
        for i in 0..10 {
            assert_eq!(row_addrs.value(i), i as u64);
        }
    }

    #[tokio::test]
    async fn test_scan_plan_with_row_address() {
        use crate::utils::test::assert_plan_node_equals;

        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let indexes = create_index_store_with_batches(&batch_store, &schema, &[(0, 10)]);

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema.clone());
        scanner.with_row_address();

        let plan = scanner.create_plan().await.unwrap();

        // Verify plan structure with _rowaddr
        assert_plan_node_equals(
            plan,
            "MemTableScanExec: projection=[id, name, _rowaddr], with_row_id=false, with_row_address=true",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_scanner_with_both_row_id_and_row_address() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let indexes = create_index_store_with_batches(&batch_store, &schema, &[(0, 5)]);

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema.clone());
        scanner.with_row_id();
        scanner.with_row_address();

        // Verify output schema includes both _rowid and _rowaddr
        let output_schema = scanner.output_schema();
        assert_eq!(output_schema.fields().len(), 4);
        assert_eq!(output_schema.field(2).name(), "_rowid");
        assert_eq!(output_schema.field(3).name(), "_rowaddr");

        // Verify data
        let result = scanner.try_into_batch().await.unwrap();
        assert_eq!(result.num_columns(), 4);

        let row_ids = result
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        let row_addrs = result
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();

        // Both should have the same values
        for i in 0..5 {
            assert_eq!(row_ids.value(i), i as u64);
            assert_eq!(row_addrs.value(i), i as u64);
        }
    }

    /// Regression: vector search against a column with no HNSW must still
    /// emit a plan whose output schema contains `_distance`. The earlier
    /// behaviour fell back to `plan_full_scan` (no `_distance`), which broke
    /// the LSM caller's `sort_by_distance` chain. Now the planner dispatches
    /// to `MemTableBruteForceVectorExec` instead — see
    /// [`super::super::exec::MemTableBruteForceVectorExec`].
    #[tokio::test]
    async fn test_plan_vector_search_without_hnsw_produces_distance_schema() {
        use std::sync::Arc;

        const DISTANCE_COLUMN: &str = "_distance";

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            ),
        ]));

        let batch_store = Arc::new(BatchStore::with_capacity(4));
        let indexes = Arc::new(IndexStore::new()); // intentionally no HNSW

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema.clone());
        let query: Arc<dyn arrow_array::Array> =
            Arc::new(arrow_array::Float32Array::from(vec![0.0_f32, 0.0_f32]));
        scanner.nearest("vector", query.as_ref(), 5).unwrap();

        let plan = scanner
            .create_plan()
            .await
            .expect("planner must produce a plan when no HNSW exists");
        let out_schema = plan.schema();
        assert!(
            out_schema.field_with_name(DISTANCE_COLUMN).is_ok(),
            "plan output schema missing `{DISTANCE_COLUMN}` — got {:?}",
            out_schema
        );
    }

    #[tokio::test]
    async fn test_nearest_rejects_invalid_query_shape() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            ),
        ]));
        let batch_store = Arc::new(BatchStore::with_capacity(4));
        let indexes = Arc::new(IndexStore::new());

        let mut scanner =
            MemTableScanner::new(batch_store.clone(), indexes.clone(), schema.clone());
        let query: Arc<dyn arrow_array::Array> =
            Arc::new(arrow_array::Float32Array::from(vec![0.0_f32, 0.0_f32]));
        let Err(err) = scanner.nearest("vector", query.as_ref(), 0) else {
            panic!("zero-k vector search should fail");
        };
        assert!(
            err.to_string().contains("k must be positive"),
            "unexpected zero-k error: {err}"
        );

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema);
        let empty_query: Arc<dyn arrow_array::Array> =
            Arc::new(arrow_array::Float32Array::from(Vec::<f32>::new()));
        let Err(err) = scanner.nearest("vector", empty_query.as_ref(), 5) else {
            panic!("empty vector search should fail");
        };
        assert!(
            err.to_string().contains("non-zero length"),
            "unexpected empty-query error: {err}"
        );
    }

    #[tokio::test]
    async fn test_create_plan_rejects_vector_and_fts_combination() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, true),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            ),
        ]));
        let batch_store = Arc::new(BatchStore::with_capacity(4));
        let indexes = Arc::new(IndexStore::new());

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema);
        let query: Arc<dyn arrow_array::Array> =
            Arc::new(arrow_array::Float32Array::from(vec![0.0_f32, 0.0_f32]));
        scanner.nearest("vector", query.as_ref(), 5).unwrap();
        scanner
            .full_text_search(
                FullTextSearchQuery::new("needle".to_string())
                    .with_column("text".to_string())
                    .unwrap(),
            )
            .unwrap();

        let err = scanner
            .create_plan()
            .await
            .expect_err("vector and FTS search must not be silently combined");
        assert!(
            err.to_string().contains("vector and full-text search"),
            "unexpected combined-search error: {err}"
        );
    }

    #[tokio::test]
    async fn test_plan_vector_search_validates_pk_types() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Float64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            ),
        ]));
        let batch_store = Arc::new(BatchStore::with_capacity(4));
        let indexes = Arc::new(IndexStore::new());

        let mut scanner = MemTableScanner::new(batch_store, indexes, schema);
        scanner.with_pk_columns(vec!["id".to_string()]);
        let query: Arc<dyn arrow_array::Array> =
            Arc::new(arrow_array::Float32Array::from(vec![0.0_f32, 0.0_f32]));
        scanner.nearest("vector", query.as_ref(), 5).unwrap();

        let err = scanner
            .create_plan()
            .await
            .expect_err("unsupported vector PK type must be rejected");
        assert!(
            err.to_string().contains("unsupported type Float64"),
            "unexpected error: {err}"
        );
    }
}
