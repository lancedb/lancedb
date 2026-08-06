// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, hash_map::Entry},
    ops::Bound,
    sync::Arc,
};

use arrow_schema::{DataType, Field};
use async_recursion::async_recursion;
use async_trait::async_trait;
use datafusion_common::ScalarValue;
use datafusion_expr::{
    Between, BinaryExpr, Expr, Operator, ReturnFieldArgs, ScalarUDF,
    expr::{InList, Like, ScalarFunction},
};
use tokio::try_join;

use super::{
    AnyQuery, BloomFilterQuery, LabelListQuery, MetricsCollector, SargableQuery, ScalarIndex,
    SearchResult, TextQuery, TokenQuery, label_list::validate_label_list_data_type,
};
#[cfg(feature = "geo")]
use super::{GeoQuery, RelationQuery};
use lance_core::{Error, Result};
use lance_datafusion::{expr::safe_coerce_scalar, planner::Planner};
use lance_select::{IndexExprResult, NullableIndexExprResult, NullableRowAddrMask};
use roaring::RoaringBitmap;
use tracing::instrument;

const MAX_DEPTH: usize = 500;

/// An indexed expression consists of a scalar index query with a post-scan filter
///
/// When a user wants to filter the data returned by a scan we may be able to use
/// one or more scalar indices to reduce the amount of data we load from the disk.
///
/// For example, if a user provides the filter "x = 7", and we have a scalar index
/// on x, then we can possibly identify the exact row that the user desires with our
/// index.  A full-table scan can then turn into a take operation fetching the rows
/// desired.  This would create an IndexedExpression with a scalar_query but no
/// refine.
///
/// If the user asked for "type = 'dog' && z = 3" and we had a scalar index on the
/// "type" column then we could convert this to an indexed scan for "type='dog'"
/// followed by an in-memory filter for z=3.  This would create an IndexedExpression
/// with both a scalar_query AND a refine.
///
/// Finally, if the user asked for "z = 3" and we do not have a scalar index on the
/// "z" column then we must fallback to an IndexedExpression with no scalar_query and
/// only a refine.
///
/// Two IndexedExpressions can be AND'd together.  Each part is AND'd together.
/// Two IndexedExpressions cannot be OR'd together unless both are scalar_query only
///   or both are refine only
/// An IndexedExpression cannot be negated if it has both a refine and a scalar_query
///
/// When an operation cannot be performed we fallback to the original expression-only
/// representation
#[derive(Debug, PartialEq)]
pub struct IndexedExpression {
    /// The portion of the query that can be satisfied by scalar indices
    pub scalar_query: Option<ScalarIndexExpr>,
    /// The portion of the query that cannot be satisfied by scalar indices
    pub refine_expr: Option<Expr>,
}

pub trait ScalarQueryParser: std::fmt::Debug + Send + Sync {
    /// Visit a between expression
    ///
    /// Returns an IndexedExpression if the index can accelerate between expressions
    fn visit_between(
        &self,
        column: &str,
        low: &Bound<ScalarValue>,
        high: &Bound<ScalarValue>,
    ) -> Option<IndexedExpression>;
    /// Visit an in list expression
    ///
    /// Returns an IndexedExpression if the index can accelerate in list expressions
    fn visit_in_list(&self, column: &str, in_list: &[ScalarValue]) -> Option<IndexedExpression>;
    /// Visit an is bool expression
    ///
    /// Returns an IndexedExpression if the index can accelerate is bool expressions
    fn visit_is_bool(&self, column: &str, value: bool) -> Option<IndexedExpression>;
    /// Visit an is null expression
    ///
    /// Returns an IndexedExpression if the index can accelerate is null expressions
    fn visit_is_null(&self, column: &str) -> Option<IndexedExpression>;
    /// Visit a comparison expression
    ///
    /// Returns an IndexedExpression if the index can accelerate comparison expressions
    fn visit_comparison(
        &self,
        column: &str,
        value: &ScalarValue,
        op: &Operator,
    ) -> Option<IndexedExpression>;
    /// Visit a scalar function expression
    ///
    /// Returns an IndexedExpression if the index can accelerate the given scalar function.
    /// For example, an ngram index can accelerate the contains function.
    fn visit_scalar_function(
        &self,
        column: &str,
        data_type: &DataType,
        func: &ScalarUDF,
        args: &[Expr],
    ) -> Option<IndexedExpression>;

    /// Visit a LIKE expression
    ///
    /// Returns an IndexedExpression if the index can accelerate LIKE expressions.
    /// For prefix patterns (e.g., "foo%"):
    /// - ZoneMaps prune zones based on min/max statistics
    /// - BTrees use range query conversion `[prefix, next_prefix)`
    ///
    /// For patterns with wildcards in the middle (e.g., "foo%bar%"), the leading prefix
    /// can still be used for pruning, with the full pattern as a refine expression.
    ///
    /// # Arguments
    /// * `column` - The column name
    /// * `like` - The full LIKE expression (for constructing refine_expr if needed)
    /// * `pattern` - The LIKE pattern as ScalarValue (e.g., "foo%")
    fn visit_like(
        &self,
        _column: &str,
        _like: &Like,
        _pattern: &ScalarValue,
    ) -> Option<IndexedExpression> {
        None
    }

    /// Visits a potential reference to a column
    ///
    /// This function is a little different from the other visitors.  It is used to test if a potential
    /// column reference is a reference the index handles.
    ///
    /// Most indexes are designed to run on references to the indexed column.  For example, if a query
    /// is "x = 7" and we have a scalar index on "x" then we apply the index to the "x" column reference.
    ///
    /// However, some indexes are designed to run on projections of the indexed column.  For example,
    /// if a query is "json_extract(json, '$.name') = 'books'" and we have a JSON index on the "json" column
    /// then we apply the index to the projection of the "json" column.
    ///
    /// This function is used to test if a potential column reference is a reference the index handles.
    /// The default implementation matches column references but this can be overridden by indexes that
    /// handle projections.
    ///
    /// The function is also passed in the data type of the column and should return the data type of the
    /// reference.  Normally this is the same as the input for a direct column reference and possibly something
    /// different for a projection.  E.g. a JSON column (LargeBinary) might be projected to a string or float
    ///
    /// Note: higher logic in the expression parser already limits references to either Expr::Column or Expr::ScalarFunction
    /// where the first argument is an Expr::Column.  If your projection doesn't fit that mold then the
    /// expression parser will need to be modified.
    fn is_valid_reference(&self, func: &Expr, data_type: &DataType) -> Option<DataType> {
        match func {
            Expr::Column(_) => Some(data_type.clone()),
            _ => None,
        }
    }
}

/// A generic parser that wraps multiple scalar query parsers
///
/// It will search each parser in order and return the first non-None result
#[derive(Debug)]
pub struct MultiQueryParser {
    parsers: Vec<Box<dyn ScalarQueryParser>>,
}

impl MultiQueryParser {
    /// Create a new MultiQueryParser with a single parser
    pub fn single(parser: Box<dyn ScalarQueryParser>) -> Self {
        Self {
            parsers: vec![parser],
        }
    }

    /// Add a new parser to the MultiQueryParser
    pub fn add(&mut self, other: Box<dyn ScalarQueryParser>) {
        self.parsers.push(other);
    }

    /// Pick the first underlying parser whose `is_valid_reference` accepts `expr`.
    pub fn select(
        &self,
        expr: &Expr,
        data_type: &DataType,
    ) -> Option<(&dyn ScalarQueryParser, DataType)> {
        self.parsers.iter().find_map(|p| {
            p.is_valid_reference(expr, data_type)
                .map(|dt| (p.as_ref(), dt))
        })
    }
}

impl ScalarQueryParser for MultiQueryParser {
    fn visit_between(
        &self,
        column: &str,
        low: &Bound<ScalarValue>,
        high: &Bound<ScalarValue>,
    ) -> Option<IndexedExpression> {
        self.parsers
            .iter()
            .find_map(|parser| parser.visit_between(column, low, high))
    }
    fn visit_in_list(&self, column: &str, in_list: &[ScalarValue]) -> Option<IndexedExpression> {
        self.parsers
            .iter()
            .find_map(|parser| parser.visit_in_list(column, in_list))
    }
    fn visit_is_bool(&self, column: &str, value: bool) -> Option<IndexedExpression> {
        self.parsers
            .iter()
            .find_map(|parser| parser.visit_is_bool(column, value))
    }
    fn visit_is_null(&self, column: &str) -> Option<IndexedExpression> {
        self.parsers
            .iter()
            .find_map(|parser| parser.visit_is_null(column))
    }
    fn visit_comparison(
        &self,
        column: &str,
        value: &ScalarValue,
        op: &Operator,
    ) -> Option<IndexedExpression> {
        self.parsers
            .iter()
            .find_map(|parser| parser.visit_comparison(column, value, op))
    }
    fn visit_scalar_function(
        &self,
        column: &str,
        data_type: &DataType,
        func: &ScalarUDF,
        args: &[Expr],
    ) -> Option<IndexedExpression> {
        self.parsers
            .iter()
            .find_map(|parser| parser.visit_scalar_function(column, data_type, func, args))
    }
    fn visit_like(
        &self,
        column: &str,
        like: &Like,
        pattern: &ScalarValue,
    ) -> Option<IndexedExpression> {
        self.parsers
            .iter()
            .find_map(|parser| parser.visit_like(column, like, pattern))
    }
    /// TODO(low-priority): This is maybe not quite right.  We should filter down the list of parsers based
    /// on those that consider the reference valid.  Instead what we are doing is checking all parsers if any one
    /// parser considers the reference valid.
    ///
    /// This will be a problem if the user creates two indexes (e.g. btree and json) on the same column and those two
    /// indexes have different reference schemes.
    fn is_valid_reference(&self, func: &Expr, data_type: &DataType) -> Option<DataType> {
        self.parsers
            .iter()
            .find_map(|parser| parser.is_valid_reference(func, data_type))
    }
}

/// A parser for indices that handle SARGable queries
#[derive(Debug)]
pub struct SargableQueryParser {
    index_name: String,
    index_type: String,
    needs_recheck: bool,
    /// Whether IS NULL queries need post-filter rechecking. May be false even
    /// when `needs_recheck` is true for indexes that track exact null positions
    /// (e.g. zone maps with a null bitmap).
    is_null_needs_recheck: bool,
    supports_like_prefix: bool,
}

impl SargableQueryParser {
    pub fn new(index_name: String, index_type: String, needs_recheck: bool) -> Self {
        Self {
            index_name,
            index_type,
            needs_recheck,
            is_null_needs_recheck: needs_recheck,
            supports_like_prefix: true,
        }
    }

    /// Bitmap (and similar) indexes cannot answer prefix queries; disabling
    /// `LikePrefix` emission makes `LIKE`/`starts_with` predicates fall back to
    /// ordinary filtering instead of failing at search time.
    pub fn without_like_prefix(mut self) -> Self {
        self.supports_like_prefix = false;
        self
    }

    /// Mark IS NULL as exact so that IS NOT NULL can be served by the index
    /// without a post-filter. Use this when the index tracks null row addresses
    /// precisely (e.g. a zone map with a null bitmap).
    pub fn with_exact_null_tracking(mut self) -> Self {
        self.is_null_needs_recheck = false;
        self
    }
}

impl ScalarQueryParser for SargableQueryParser {
    fn is_valid_reference(&self, func: &Expr, data_type: &DataType) -> Option<DataType> {
        match func {
            Expr::Column(_) => Some(data_type.clone()),
            // Also accept get_field expressions for nested field access
            Expr::ScalarFunction(udf) if udf.name() == "get_field" => Some(data_type.clone()),
            _ => None,
        }
    }

    fn visit_between(
        &self,
        column: &str,
        low: &Bound<ScalarValue>,
        high: &Bound<ScalarValue>,
    ) -> Option<IndexedExpression> {
        if let Bound::Included(val) | Bound::Excluded(val) = low
            && val.is_null()
        {
            return None;
        }
        if let Bound::Included(val) | Bound::Excluded(val) = high
            && val.is_null()
        {
            return None;
        }
        let query = SargableQuery::Range(low.clone(), high.clone());
        Some(IndexedExpression::index_query_with_recheck(
            column.to_string(),
            self.index_name.clone(),
            self.index_type.clone(),
            Arc::new(query),
            self.needs_recheck,
        ))
    }

    fn visit_in_list(&self, column: &str, in_list: &[ScalarValue]) -> Option<IndexedExpression> {
        if in_list.iter().any(|val| val.is_null()) {
            return None;
        }
        let query = SargableQuery::IsIn(in_list.to_vec());
        Some(IndexedExpression::index_query_with_recheck(
            column.to_string(),
            self.index_name.clone(),
            self.index_type.clone(),
            Arc::new(query),
            self.needs_recheck,
        ))
    }

    fn visit_is_bool(&self, column: &str, value: bool) -> Option<IndexedExpression> {
        Some(IndexedExpression::index_query_with_recheck(
            column.to_string(),
            self.index_name.clone(),
            self.index_type.clone(),
            Arc::new(SargableQuery::Equals(ScalarValue::Boolean(Some(value)))),
            self.needs_recheck,
        ))
    }

    fn visit_is_null(&self, column: &str) -> Option<IndexedExpression> {
        Some(IndexedExpression::index_query_with_recheck(
            column.to_string(),
            self.index_name.clone(),
            self.index_type.clone(),
            Arc::new(SargableQuery::IsNull()),
            self.is_null_needs_recheck,
        ))
    }

    fn visit_comparison(
        &self,
        column: &str,
        value: &ScalarValue,
        op: &Operator,
    ) -> Option<IndexedExpression> {
        if value.is_null() {
            return None;
        }
        let query = match op {
            Operator::Lt => SargableQuery::Range(Bound::Unbounded, Bound::Excluded(value.clone())),
            Operator::LtEq => {
                SargableQuery::Range(Bound::Unbounded, Bound::Included(value.clone()))
            }
            Operator::Gt => SargableQuery::Range(Bound::Excluded(value.clone()), Bound::Unbounded),
            Operator::GtEq => {
                SargableQuery::Range(Bound::Included(value.clone()), Bound::Unbounded)
            }
            Operator::Eq => SargableQuery::Equals(value.clone()),
            // This will be negated by the caller
            Operator::NotEq => SargableQuery::Equals(value.clone()),
            _ => unreachable!(),
        };
        Some(IndexedExpression::index_query_with_recheck(
            column.to_string(),
            self.index_name.clone(),
            self.index_type.clone(),
            Arc::new(query),
            self.needs_recheck,
        ))
    }

    fn visit_scalar_function(
        &self,
        column: &str,
        _data_type: &DataType,
        func: &ScalarUDF,
        args: &[Expr],
    ) -> Option<IndexedExpression> {
        // Handle starts_with(col, 'prefix') -> convert to LikePrefix query
        if func.name() == "starts_with" && args.len() == 2 {
            // Indexes that cannot answer prefix queries (e.g. bitmap) fall back to
            // ordinary filtering rather than emitting a query they would reject.
            if !self.supports_like_prefix {
                return None;
            }
            // Extract the prefix from the second argument
            let prefix = match &args[1] {
                Expr::Literal(ScalarValue::Utf8(Some(s)), _) => ScalarValue::Utf8(Some(s.clone())),
                Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => {
                    ScalarValue::LargeUtf8(Some(s.clone()))
                }
                // Lance stores `Utf8View` columns as `Utf8` (normalized at write time), so a
                // `Utf8View` literal is normalized to `Utf8` to match the indexed data: the
                // BTree compares the query bound against `Utf8` page statistics at the Arrow
                // level, which rejects a `Utf8View` bound.
                Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => {
                    ScalarValue::Utf8(Some(s.clone()))
                }
                _ => return None,
            };

            let query = SargableQuery::LikePrefix(prefix);
            return Some(IndexedExpression::index_query_with_recheck(
                column.to_string(),
                self.index_name.clone(),
                self.index_type.clone(),
                Arc::new(query),
                self.needs_recheck,
            ));
        }

        None
    }

    fn visit_like(
        &self,
        column: &str,
        like: &Like,
        pattern: &ScalarValue,
    ) -> Option<IndexedExpression> {
        // Case-insensitive LIKE (ILIKE) cannot be efficiently pruned with zone maps
        if like.case_insensitive {
            return None;
        }

        // Indexes that cannot answer prefix queries (e.g. bitmap) fall back to
        // ordinary filtering rather than emitting a query they would reject.
        if !self.supports_like_prefix {
            return None;
        }

        // Extract the pattern string
        let pattern_str = match pattern {
            ScalarValue::Utf8(Some(s))
            | ScalarValue::LargeUtf8(Some(s))
            | ScalarValue::Utf8View(Some(s)) => s.as_str(),
            _ => return None,
        };

        // Try to extract a prefix from the LIKE pattern
        let (prefix, needs_refine) = extract_like_leading_prefix(pattern_str, like.escape_char)?;

        // Create the prefix ScalarValue with the same type as the pattern. `Utf8View` is
        // normalized to `Utf8` because Lance stores `Utf8View` columns as `Utf8`, and the
        // downstream BTree compares the query bound against `Utf8` page statistics at the
        // Arrow level (a `Utf8View` bound would fail that comparison).
        let prefix_value = match pattern {
            ScalarValue::Utf8(_) | ScalarValue::Utf8View(_) => ScalarValue::Utf8(Some(prefix)),
            ScalarValue::LargeUtf8(_) => ScalarValue::LargeUtf8(Some(prefix)),
            _ => return None,
        };

        let query = SargableQuery::LikePrefix(prefix_value);
        let scalar_query = Some(ScalarIndexExpr::Query(ScalarIndexSearch {
            column: column.to_string(),
            index_name: self.index_name.clone(),
            index_type: self.index_type.clone(),
            query: Arc::new(query),
            needs_recheck: self.needs_recheck,
            fragment_bitmap: None,
        }));

        // If the pattern has wildcards beyond simple prefix, add refine expression
        let refine_expr = if needs_refine {
            Some(Expr::Like(like.clone()))
        } else {
            None
        };

        Some(IndexedExpression {
            scalar_query,
            refine_expr,
        })
    }
}

/// Extract the leading literal prefix from a LIKE pattern.
///
/// Returns `Some((prefix, needs_refine))` where:
/// - `prefix` is the leading literal portion before any wildcards
/// - `needs_refine` is true if the pattern has wildcards beyond a simple trailing `%`
///
/// Returns `None` if the pattern starts with a wildcard (no leading literal).
///
/// Examples:
/// - "foo%" -> Some(("foo", false)) - pure prefix, no recheck needed
/// - "foo%bar%" -> Some(("foo", true)) - can use prefix for pruning, needs recheck
/// - "foo_bar%" -> Some(("foo", true)) - _ is a wildcard, needs recheck
/// - "foo\%bar%" with escape '\' -> Some(("foo%bar", false)) - escaped %, pure prefix
/// - "%foo" -> None - starts with wildcard, cannot prune
/// - "foo" -> None - no wildcard at all, use equality instead
fn extract_like_leading_prefix(pattern: &str, escape_char: Option<char>) -> Option<(String, bool)> {
    let chars: Vec<char> = pattern.chars().collect();
    let len = chars.len();

    if len == 0 {
        return None;
    }

    // DataFusion's starts_with simplification escapes special characters with backslash
    // but doesn't set escape_char. Use backslash as default escape character.
    // Pattern: starts_with(col, 'test_ns$') -> col LIKE 'test\_ns$%' (escape_char: None)
    // See: https://github.com/apache/datafusion/issues/XXXX
    let effective_escape_char = escape_char.or(Some('\\'));

    // Helper to check if a character at position i is escaped
    let is_escaped = |i: usize| -> bool {
        if let Some(esc) = effective_escape_char {
            if i > 0 && chars[i - 1] == esc {
                // Check if the escape char itself is escaped
                if i >= 2 && chars[i - 2] == esc {
                    false // Escape was escaped, so this char is NOT escaped
                } else {
                    true // This char is escaped
                }
            } else {
                false
            }
        } else {
            // No escape character defined - nothing can be escaped
            false
        }
    };

    // Pattern must contain at least one unescaped wildcard
    let has_wildcard = chars.iter().enumerate().any(|(i, &c)| {
        if c != '%' && c != '_' {
            return false;
        }
        !is_escaped(i)
    });

    if !has_wildcard {
        return None; // No wildcards, should use equality
    }

    // Check if pattern starts with an unescaped wildcard
    if chars[0] == '%' || chars[0] == '_' {
        return None; // Starts with wildcard, cannot prune
    }

    // Extract the leading literal prefix (everything before first unescaped wildcard)
    let mut prefix = String::new();
    let mut i = 0;
    let mut found_wildcard = false;

    while i < len {
        let c = chars[i];

        // Check for escape character (using effective escape char which may be inferred)
        if let Some(esc) = effective_escape_char
            && c == esc
            && i + 1 < len
        {
            let next = chars[i + 1];
            if next == '%' || next == '_' || next == esc {
                // Escaped character - add the literal character
                prefix.push(next);
                i += 2;
                continue;
            }
        }

        // Check for unescaped wildcard
        if c == '%' || c == '_' {
            found_wildcard = true;
            break;
        }

        prefix.push(c);
        i += 1;
    }

    if prefix.is_empty() {
        return None;
    }

    // Check if pattern is just a simple prefix (ends with single % and nothing after)
    let needs_refine = if found_wildcard && i < len {
        // Check if we're at a % wildcard
        if chars[i] == '%' && i + 1 == len {
            // Pattern is "prefix%" - pure prefix match, no refine needed
            false
        } else {
            // Pattern has more after first wildcard, or has _ wildcard
            true
        }
    } else {
        // No wildcard found (shouldn't happen due to earlier check)
        false
    };

    Some((prefix, needs_refine))
}

/// A parser for bloom filter indices that only support equals, is_null, and is_in operations
#[derive(Debug)]
pub struct BloomFilterQueryParser {
    index_name: String,
    index_type: String,
    needs_recheck: bool,
}

impl BloomFilterQueryParser {
    pub fn new(index_name: String, index_type: String, needs_recheck: bool) -> Self {
        Self {
            index_name,
            index_type,
            needs_recheck,
        }
    }
}

impl ScalarQueryParser for BloomFilterQueryParser {
    fn visit_between(
        &self,
        _: &str,
        _: &Bound<ScalarValue>,
        _: &Bound<ScalarValue>,
    ) -> Option<IndexedExpression> {
        // Bloom filters don't support range queries
        None
    }

    fn visit_in_list(&self, column: &str, in_list: &[ScalarValue]) -> Option<IndexedExpression> {
        let query = BloomFilterQuery::IsIn(in_list.to_vec());
        Some(IndexedExpression::index_query_with_recheck(
            column.to_string(),
            self.index_name.clone(),
            self.index_type.clone(),
            Arc::new(query),
            self.needs_recheck,
        ))
    }

    fn visit_is_bool(&self, column: &str, value: bool) -> Option<IndexedExpression> {
        Some(IndexedExpression::index_query_with_recheck(
            column.to_string(),
            self.index_name.clone(),
            self.index_type.clone(),
            Arc::new(BloomFilterQuery::Equals(ScalarValue::Boolean(Some(value)))),
            self.needs_recheck,
        ))
    }

    fn visit_is_null(&self, column: &str) -> Option<IndexedExpression> {
        Some(IndexedExpression::index_query_with_recheck(
            column.to_string(),
            self.index_name.clone(),
            self.index_type.clone(),
            Arc::new(BloomFilterQuery::IsNull()),
            self.needs_recheck,
        ))
    }

    fn visit_comparison(
        &self,
        column: &str,
        value: &ScalarValue,
        op: &Operator,
    ) -> Option<IndexedExpression> {
        let query = match op {
            // Bloom filters only support equality comparisons
            Operator::Eq => BloomFilterQuery::Equals(value.clone()),
            // This will be negated by the caller
            Operator::NotEq => BloomFilterQuery::Equals(value.clone()),
            // Bloom filters don't support range operations
            _ => return None,
        };
        Some(IndexedExpression::index_query_with_recheck(
            column.to_string(),
            self.index_name.clone(),
            self.index_type.clone(),
            Arc::new(query),
            self.needs_recheck,
        ))
    }

    fn visit_scalar_function(
        &self,
        _: &str,
        _: &DataType,
        _: &ScalarUDF,
        _: &[Expr],
    ) -> Option<IndexedExpression> {
        // Bloom filters don't support scalar functions
        None
    }
}

/// A parser for indices that handle label list queries
#[derive(Debug)]
pub struct LabelListQueryParser {
    index_name: String,
    index_type: String,
}

impl LabelListQueryParser {
    pub fn new(index_name: String, index_type: String) -> Self {
        Self {
            index_name,
            index_type,
        }
    }
}

impl ScalarQueryParser for LabelListQueryParser {
    fn visit_between(
        &self,
        _: &str,
        _: &Bound<ScalarValue>,
        _: &Bound<ScalarValue>,
    ) -> Option<IndexedExpression> {
        None
    }

    fn visit_in_list(&self, _: &str, _: &[ScalarValue]) -> Option<IndexedExpression> {
        None
    }

    fn visit_is_bool(&self, _: &str, _: bool) -> Option<IndexedExpression> {
        None
    }

    fn visit_is_null(&self, _: &str) -> Option<IndexedExpression> {
        None
    }

    fn visit_comparison(
        &self,
        _: &str,
        _: &ScalarValue,
        _: &Operator,
    ) -> Option<IndexedExpression> {
        None
    }

    fn visit_scalar_function(
        &self,
        column: &str,
        data_type: &DataType,
        func: &ScalarUDF,
        args: &[Expr],
    ) -> Option<IndexedExpression> {
        if args.len() != 2 {
            return None;
        }
        // LABEL_LIST stores unnested items as bitmap keys. Nested items cannot be
        // ordered by the bitmap lookup, so legacy indexes must fall back to a scan.
        if validate_label_list_data_type(data_type).is_err() {
            return None;
        }
        // DataFusion normalizes array_contains to array_has
        if func.name() == "array_has" {
            let inner_type = match data_type {
                DataType::List(field) | DataType::LargeList(field) => field.data_type(),
                _ => return None,
            };
            let scalar = maybe_scalar(&args[1], inner_type)?;
            // array_has(..., NULL) returns no matches in datafusion, but the index would
            // match rows containing NULL. Fallback to match datafusion behavior.
            if scalar.is_null() {
                return None;
            }
            let query = LabelListQuery::HasAnyLabel(vec![scalar]);
            return Some(IndexedExpression::index_query(
                column.to_string(),
                self.index_name.clone(),
                self.index_type.clone(),
                Arc::new(query),
            ));
        }

        let label_list = maybe_scalar(&args[1], data_type)?;
        let list_values = match label_list {
            ScalarValue::List(list_arr) => list_arr.values().clone(),
            ScalarValue::LargeList(list_arr) => list_arr.values().clone(),
            _ => return None,
        };
        if list_values.is_empty() {
            return None;
        }
        let mut scalars = Vec::with_capacity(list_values.len());
        for idx in 0..list_values.len() {
            scalars.push(ScalarValue::try_from_array(list_values.as_ref(), idx).ok()?);
        }
        if func.name() == "array_has_all" {
            let query = LabelListQuery::HasAllLabels(scalars);
            Some(IndexedExpression::index_query(
                column.to_string(),
                self.index_name.clone(),
                self.index_type.clone(),
                Arc::new(query),
            ))
        } else if func.name() == "array_has_any" {
            let query = LabelListQuery::HasAnyLabel(scalars);
            Some(IndexedExpression::index_query(
                column.to_string(),
                self.index_name.clone(),
                self.index_type.clone(),
                Arc::new(query),
            ))
        } else {
            None
        }
    }
}

/// A parser for indices that handle string `contains` queries, and -- when
/// `supports_regex` is set -- `regexp_like` / `regexp_match` queries.
#[derive(Debug, Clone)]
pub struct TextQueryParser {
    index_name: String,
    index_type: String,
    needs_recheck: bool,
    supports_regex: bool,
    /// The shortest `contains` pattern (in characters, not bytes) the index can
    /// say anything useful about.  Shorter patterns bypass the index entirely so
    /// they are answered by a full scan.  Use 0 for indices with no such limit.
    min_contains_chars: usize,
}

impl TextQueryParser {
    pub fn new(
        index_name: String,
        index_type: String,
        needs_recheck: bool,
        supports_regex: bool,
        min_contains_chars: usize,
    ) -> Self {
        Self {
            index_name,
            index_type,
            needs_recheck,
            supports_regex,
            min_contains_chars,
        }
    }
}

impl ScalarQueryParser for TextQueryParser {
    fn visit_between(
        &self,
        _: &str,
        _: &Bound<ScalarValue>,
        _: &Bound<ScalarValue>,
    ) -> Option<IndexedExpression> {
        None
    }

    fn visit_in_list(&self, _: &str, _: &[ScalarValue]) -> Option<IndexedExpression> {
        None
    }

    fn visit_is_bool(&self, _: &str, _: bool) -> Option<IndexedExpression> {
        None
    }

    fn visit_is_null(&self, _: &str) -> Option<IndexedExpression> {
        None
    }

    fn visit_comparison(
        &self,
        _: &str,
        _: &ScalarValue,
        _: &Operator,
    ) -> Option<IndexedExpression> {
        None
    }

    fn visit_scalar_function(
        &self,
        column: &str,
        data_type: &DataType,
        func: &ScalarUDF,
        args: &[Expr],
    ) -> Option<IndexedExpression> {
        // The first argument is the indexed column; the second is the substring
        // / pattern. `contains` takes exactly two arguments; the regex functions
        // optionally take a third flags argument.
        if args.len() < 2 {
            return None;
        }
        // A non-string pattern cannot be handled.
        let (ScalarValue::Utf8(Some(pattern)) | ScalarValue::LargeUtf8(Some(pattern))) =
            maybe_scalar(&args[1], data_type)?
        else {
            return None;
        };

        let query = match func.name() {
            "contains" if args.len() == 2 => {
                // A pattern shorter than the index's token width produces no
                // tokens, so the index can only answer "recheck everything".
                // Leave it to a full scan instead.  The count is in characters
                // because tokenizers split on characters, not bytes.
                if pattern.chars().count() < self.min_contains_chars {
                    return None;
                }
                TextQuery::StringContains(pattern)
            }
            "regexp_like" | "regexp_match" if self.supports_regex => {
                let pattern = match args.get(2) {
                    Some(flags_expr) => apply_regex_flags(&pattern, flags_expr)?,
                    None => pattern,
                };
                // If the pattern yields no usable trigram (e.g. `a.b`), leave it
                // to a full scan instead of routing it to the index, which could
                // only answer with an unsupported "recheck everything" result.
                if !crate::scalar::ngram::regex_can_use_index(&pattern) {
                    return None;
                }
                TextQuery::Regex(pattern)
            }
            _ => return None,
        };

        Some(IndexedExpression::index_query_with_recheck(
            column.to_string(),
            self.index_name.clone(),
            self.index_type.clone(),
            Arc::new(query),
            self.needs_recheck,
        ))
    }

    fn visit_like(
        &self,
        column: &str,
        like: &Like,
        pattern: &ScalarValue,
    ) -> Option<IndexedExpression> {
        // Infix LIKE is accelerated only by the ngram index (via its regex
        // machinery). A plain-literal `regexp_like(col, 'foo')` is rewritten to
        // `col LIKE '%foo%'` before it reaches the index, so this is the path
        // that accelerates those. ILIKE is skipped because its case folding does
        // not match the index's normalization.
        if !self.supports_regex || like.case_insensitive {
            return None;
        }
        let pattern_str = match pattern {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.as_str(),
            _ => return None,
        };
        // Translate the LIKE pattern into a loose regex used only for candidate
        // generation; the original LIKE stays as the recheck filter, so the
        // regex only needs to be a sound superset.
        let regex = like_to_regex(pattern_str, like.escape_char)?;
        if !crate::scalar::ngram::regex_can_use_index(&regex) {
            return None;
        }
        Some(IndexedExpression {
            scalar_query: Some(ScalarIndexExpr::Query(ScalarIndexSearch {
                column: column.to_string(),
                index_name: self.index_name.clone(),
                index_type: self.index_type.clone(),
                query: Arc::new(TextQuery::Regex(regex)),
                needs_recheck: self.needs_recheck,
                fragment_bitmap: None,
            })),
            refine_expr: Some(Expr::Like(like.clone())),
        })
    }
}

/// Translate a LIKE pattern into a regular expression used purely for ngram
/// candidate generation: `%` becomes `.*`, `_` becomes `.`, and literal
/// characters are regex-escaped. Returns `None` when no literal run is long
/// enough to yield a trigram (the index could not help, so a full scan is left
/// to handle it).
fn like_to_regex(pattern: &str, escape: Option<char>) -> Option<String> {
    let mut regex = String::new();
    let mut run = 0usize;
    let mut longest_run = 0usize;
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        let literal = if Some(c) == escape {
            // The next character is escaped, i.e. a literal.
            chars.next()
        } else {
            match c {
                '%' => {
                    regex.push_str(".*");
                    run = 0;
                    None
                }
                '_' => {
                    regex.push('.');
                    run = 0;
                    None
                }
                other => Some(other),
            }
        };
        if let Some(lit) = literal {
            if regex_syntax::is_meta_character(lit) {
                regex.push('\\');
            }
            regex.push(lit);
            // Only runs of alphanumeric characters can produce a trigram.
            if lit.is_alphanumeric() {
                run += 1;
                longest_run = longest_run.max(run);
            } else {
                run = 0;
            }
        }
    }
    (longest_run >= 3).then_some(regex)
}

/// Fold the supported `regexp_like` / `regexp_match` flags into an inline prefix
/// on the pattern (e.g. flags `"i"` -> `"(?i)pattern"`). Returns `None` for a
/// non-literal flags argument or an unrecognized flag, so the caller leaves the
/// predicate to a full recheck rather than risk changing its semantics.
fn apply_regex_flags(pattern: &str, flags_expr: &Expr) -> Option<String> {
    let (Expr::Literal(ScalarValue::Utf8(Some(flags)), _)
    | Expr::Literal(ScalarValue::LargeUtf8(Some(flags)), _)) = flags_expr
    else {
        return None;
    };
    let mut inline = String::new();
    for flag in flags.chars() {
        // Only flags expressible as an inline `(?...)` group in the regex crate
        // (which the recheck uses) are safe to fold.
        if ['i', 's', 'm', 'x'].contains(&flag) {
            inline.push(flag);
        } else {
            return None;
        }
    }
    if inline.is_empty() {
        Some(pattern.to_string())
    } else {
        Some(format!("(?{inline}){pattern}"))
    }
}

/// A parser for indices that handle queries with the contains_tokens function
#[derive(Debug, Clone)]
pub struct FtsQueryParser {
    index_name: String,
    index_type: String,
}

impl FtsQueryParser {
    pub fn new(name: String, index_type: String) -> Self {
        Self {
            index_name: name,
            index_type,
        }
    }
}

impl ScalarQueryParser for FtsQueryParser {
    fn visit_between(
        &self,
        _: &str,
        _: &Bound<ScalarValue>,
        _: &Bound<ScalarValue>,
    ) -> Option<IndexedExpression> {
        None
    }

    fn visit_in_list(&self, _: &str, _: &[ScalarValue]) -> Option<IndexedExpression> {
        None
    }

    fn visit_is_bool(&self, _: &str, _: bool) -> Option<IndexedExpression> {
        None
    }

    fn visit_is_null(&self, _: &str) -> Option<IndexedExpression> {
        None
    }

    fn visit_comparison(
        &self,
        _: &str,
        _: &ScalarValue,
        _: &Operator,
    ) -> Option<IndexedExpression> {
        None
    }

    fn visit_scalar_function(
        &self,
        column: &str,
        data_type: &DataType,
        func: &ScalarUDF,
        args: &[Expr],
    ) -> Option<IndexedExpression> {
        if args.len() != 2 {
            return None;
        }
        let scalar = maybe_scalar(&args[1], data_type)?;
        if let ScalarValue::Utf8(Some(scalar_str)) = scalar
            && func.name() == "contains_tokens"
        {
            let query = TokenQuery::TokensContains(scalar_str);
            return Some(IndexedExpression::index_query(
                column.to_string(),
                self.index_name.clone(),
                self.index_type.clone(),
                Arc::new(query),
            ));
        }
        None
    }
}

/// A parser for geo indices that handles spatial queries
#[cfg(feature = "geo")]
#[derive(Debug, Clone)]
pub struct GeoQueryParser {
    index_name: String,
    index_type: String,
}

#[cfg(feature = "geo")]
impl GeoQueryParser {
    pub fn new(index_name: String, index_type: String) -> Self {
        Self {
            index_name,
            index_type,
        }
    }
}

#[cfg(feature = "geo")]
impl ScalarQueryParser for GeoQueryParser {
    fn visit_between(
        &self,
        _: &str,
        _: &Bound<ScalarValue>,
        _: &Bound<ScalarValue>,
    ) -> Option<IndexedExpression> {
        None
    }

    fn visit_in_list(&self, _: &str, _: &[ScalarValue]) -> Option<IndexedExpression> {
        None
    }

    fn visit_is_bool(&self, _: &str, _: bool) -> Option<IndexedExpression> {
        None
    }

    fn visit_is_null(&self, column: &str) -> Option<IndexedExpression> {
        Some(IndexedExpression::index_query_with_recheck(
            column.to_string(),
            self.index_name.clone(),
            self.index_type.clone(),
            Arc::new(GeoQuery::IsNull),
            true,
        ))
    }

    fn visit_comparison(
        &self,
        _: &str,
        _: &ScalarValue,
        _: &Operator,
    ) -> Option<IndexedExpression> {
        None
    }

    fn visit_scalar_function(
        &self,
        column: &str,
        _data_type: &DataType,
        func: &ScalarUDF,
        args: &[Expr],
    ) -> Option<IndexedExpression> {
        if (func.name() == "st_intersects"
            || func.name() == "st_contains"
            || func.name() == "st_within"
            || func.name() == "st_touches"
            || func.name() == "st_crosses"
            || func.name() == "st_overlaps"
            || func.name() == "st_covers"
            || func.name() == "st_coveredby")
            && args.len() == 2
        {
            let left_arg = &args[0];
            let right_arg = &args[1];
            return match (left_arg, right_arg) {
                (Expr::Literal(left_value, metadata), Expr::Column(_)) => {
                    let mut field = Field::new("_geo", left_value.data_type(), false);
                    if let Some(metadata) = metadata {
                        field = field.with_metadata(metadata.to_hashmap());
                    }
                    let query = GeoQuery::IntersectQuery(RelationQuery {
                        value: left_value.clone(),
                        field,
                    });
                    Some(IndexedExpression::index_query_with_recheck(
                        column.to_string(),
                        self.index_name.clone(),
                        self.index_type.clone(),
                        Arc::new(query),
                        true,
                    ))
                }
                (Expr::Column(_), Expr::Literal(right_value, metadata)) => {
                    let mut field = Field::new("_geo", right_value.data_type(), false);
                    if let Some(metadata) = metadata {
                        field = field.with_metadata(metadata.to_hashmap());
                    }
                    let query = GeoQuery::IntersectQuery(RelationQuery {
                        value: right_value.clone(),
                        field,
                    });
                    Some(IndexedExpression::index_query_with_recheck(
                        column.to_string(),
                        self.index_name.clone(),
                        self.index_type.clone(),
                        Arc::new(query),
                        true,
                    ))
                }
                _ => None,
            };
        }
        None
    }
}

impl IndexedExpression {
    /// Create an expression that only does refine
    fn refine_only(refine_expr: Expr) -> Self {
        Self {
            scalar_query: None,
            refine_expr: Some(refine_expr),
        }
    }

    /// Create an expression that is only an index query
    fn index_query(
        column: String,
        index_name: String,
        index_type: String,
        query: Arc<dyn AnyQuery>,
    ) -> Self {
        Self {
            scalar_query: Some(ScalarIndexExpr::Query(ScalarIndexSearch {
                column,
                index_name,
                index_type,
                query,
                needs_recheck: false,  // Default to false, will be set by parser
                fragment_bitmap: None, // Filled in by `apply_scalar_indices`
            })),
            refine_expr: None,
        }
    }

    /// Create an expression that is only an index query with explicit needs_recheck
    fn index_query_with_recheck(
        column: String,
        index_name: String,
        index_type: String,
        query: Arc<dyn AnyQuery>,
        needs_recheck: bool,
    ) -> Self {
        Self {
            scalar_query: Some(ScalarIndexExpr::Query(ScalarIndexSearch {
                column,
                index_name,
                index_type,
                query,
                needs_recheck,
                fragment_bitmap: None, // Filled in by `apply_scalar_indices`
            })),
            refine_expr: None,
        }
    }

    /// Try and negate the expression
    ///
    /// If the expression contains both an index query and a refine expression then it
    /// cannot be negated today and None will be returned (we give up trying to use indices)
    fn maybe_not(self) -> Option<Self> {
        match (self.scalar_query, self.refine_expr) {
            (Some(_), Some(_)) => None,
            (Some(scalar_query), None) => {
                if scalar_query.needs_recheck() {
                    return None;
                }
                Some(Self {
                    scalar_query: Some(ScalarIndexExpr::Not(Box::new(scalar_query))),
                    refine_expr: None,
                })
            }
            (None, Some(refine_expr)) => Some(Self {
                scalar_query: None,
                refine_expr: Some(Expr::Not(Box::new(refine_expr))),
            }),
            (None, None) => panic!("Empty node should not occur"),
        }
    }

    /// Perform a logical AND of two indexed expressions
    ///
    /// This is straightforward because we can just AND the individual parts
    /// because (A && B) && (C && D) == (A && C) && (B && D)
    fn and(self, other: Self) -> Self {
        let scalar_query = match (self.scalar_query, other.scalar_query) {
            (Some(scalar_query), Some(other_scalar_query)) => Some(ScalarIndexExpr::And(
                Box::new(scalar_query),
                Box::new(other_scalar_query),
            )),
            (Some(scalar_query), None) => Some(scalar_query),
            (None, Some(scalar_query)) => Some(scalar_query),
            (None, None) => None,
        };
        let refine_expr = match (self.refine_expr, other.refine_expr) {
            (Some(refine_expr), Some(other_refine_expr)) => {
                Some(refine_expr.and(other_refine_expr))
            }
            (Some(refine_expr), None) => Some(refine_expr),
            (None, Some(refine_expr)) => Some(refine_expr),
            (None, None) => None,
        };
        Self {
            scalar_query,
            refine_expr,
        }
    }

    /// Try and perform a logical OR of two indexed expressions
    ///
    /// This is a bit tricky because something like:
    ///   (color == 'blue' AND size < 20) OR (color == 'green' AND size < 50)
    /// is not equivalent to:
    ///   (color == 'blue' OR color == 'green') AND (size < 20 OR size < 50)
    fn maybe_or(self, other: Self) -> Option<Self> {
        // If either expression is missing a scalar_query then we need to load all rows from
        // the database and so we short-circuit and return None
        let scalar_query = self.scalar_query?;
        let other_scalar_query = other.scalar_query?;
        let scalar_query = Some(ScalarIndexExpr::Or(
            Box::new(scalar_query),
            Box::new(other_scalar_query),
        ));

        let refine_expr = match (self.refine_expr, other.refine_expr) {
            // TODO
            //
            // To handle these cases we need a way of going back from a scalar expression query to a logical DF expression (perhaps
            // we can store the expression that led to the creation of the query)
            //
            // For example, imagine we have something like "(color == 'blue' AND size < 20) OR (color == 'green' AND size < 50)"
            //
            // We can do an indexed load of all rows matching "color == 'blue' OR color == 'green'" but then we need to
            // refine that load with the full original expression which, at the moment, we no longer have.
            (Some(_), Some(_)) => {
                return None;
            }
            (Some(_), None) => {
                return None;
            }
            (None, Some(_)) => {
                return None;
            }
            (None, None) => None,
        };
        Some(Self {
            scalar_query,
            refine_expr,
        })
    }

    fn refine(self, expr: Expr) -> Self {
        match self.refine_expr {
            Some(refine_expr) => Self {
                scalar_query: self.scalar_query,
                refine_expr: Some(refine_expr.and(expr)),
            },
            None => Self {
                scalar_query: self.scalar_query,
                refine_expr: Some(expr),
            },
        }
    }
}

/// A trait implemented by anything that can load indices by name
///
/// This is used during the evaluation of an index expression
#[async_trait]
pub trait ScalarIndexLoader: Send + Sync {
    /// Load the index with the given name
    async fn load_index(
        &self,
        column: &str,
        index_name: &str,
        metrics: &dyn MetricsCollector,
    ) -> Result<Arc<dyn ScalarIndex>>;

    /// Translate an address-domain index result into the row-id domain
    ///
    /// Address-domain indices (see [`ScalarIndex::results_are_row_addresses`])
    /// report matches as physical row addresses. The default returns `result`
    /// unchanged, which is correct when addresses and row ids coincide (no
    /// stable row ids). A dataset with stable row ids overrides this to remap
    /// addresses to stable row ids via its per-fragment row-id sequences.
    async fn row_addr_result_to_row_ids(
        &self,
        result: NullableIndexExprResult,
    ) -> Result<NullableIndexExprResult> {
        Ok(result)
    }
}

/// This represents a search into a scalar index
#[derive(Debug, Clone)]
pub struct ScalarIndexSearch {
    /// The column to search (redundant, used for debugging messages)
    pub column: String,
    /// The name of the index to search
    pub index_name: String,
    /// The type of the index being searched (e.g. "BTree", "Bitmap"), used for display purposes
    pub index_type: String,
    /// The query to search for
    pub query: Arc<dyn AnyQuery>,
    /// If true, the query results are inexact and will need a recheck
    pub needs_recheck: bool,
    /// The fragments the underlying index has entries for.
    ///
    /// `None` means coverage is unknown (e.g. constructed outside of scanner
    /// planning, or from a legacy code path). Optimizer rules that need to
    /// decide whether the index covers the dataset must treat `None` as
    /// "refuse to use" — the bitmap is the only way to safely answer that
    /// question synchronously without an async metadata load.
    pub fragment_bitmap: Option<RoaringBitmap>,
}

impl PartialEq for ScalarIndexSearch {
    fn eq(&self, other: &Self) -> bool {
        // `fragment_bitmap` is metadata derived from the dataset state, not
        // part of the query identity, so it intentionally does not participate
        // in equality.
        self.column == other.column
            && self.index_name == other.index_name
            && self.query.as_ref().eq(other.query.as_ref())
    }
}

/// This represents a lookup into one or more scalar indices
///
/// This is a tree of operations because we may need to logically combine or
/// modify the results of scalar lookups
#[derive(Debug, Clone)]
pub enum ScalarIndexExpr {
    Not(Box<Self>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Query(ScalarIndexSearch),
}

impl PartialEq for ScalarIndexExpr {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Not(l0), Self::Not(r0)) => l0 == r0,
            (Self::And(l0, l1), Self::And(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::Or(l0, l1), Self::Or(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::Query(l_search), Self::Query(r_search)) => l_search == r_search,
            _ => false,
        }
    }
}

/// Conservative grouping key for rewrites targeting the same parsed scalar index.
/// `index_type` is display metadata, but including it prevents rewrites across
/// index implementations that may not share query semantics.
type ScalarIndexQueryKey = (String, String, String);

/// Returns the tighter (more restrictive) lower bound.
///
/// Returns `None` if the bound values cannot be compared. In that case callers
/// keep the original predicates rather than inventing ordering semantics.
fn tighter_lower_bound(
    a: &Bound<ScalarValue>,
    b: &Bound<ScalarValue>,
) -> Option<Bound<ScalarValue>> {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Some(Bound::Unbounded),
        (Bound::Unbounded, other) | (other, Bound::Unbounded) => Some(other.clone()),
        (
            Bound::Included(a_value) | Bound::Excluded(a_value),
            Bound::Included(b_value) | Bound::Excluded(b_value),
        ) => match a_value.partial_cmp(b_value)? {
            Ordering::Less => Some(b.clone()),
            Ordering::Equal => Some(stricter_bound_for_equal_value(a_value, a, b)),
            Ordering::Greater => Some(a.clone()),
        },
    }
}

/// Returns the tighter (more restrictive) upper bound.
///
/// Returns `None` if the bound values cannot be compared. In that case callers
/// keep the original predicates rather than inventing ordering semantics.
fn tighter_upper_bound(
    a: &Bound<ScalarValue>,
    b: &Bound<ScalarValue>,
) -> Option<Bound<ScalarValue>> {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Some(Bound::Unbounded),
        (Bound::Unbounded, other) | (other, Bound::Unbounded) => Some(other.clone()),
        (
            Bound::Included(a_value) | Bound::Excluded(a_value),
            Bound::Included(b_value) | Bound::Excluded(b_value),
        ) => match a_value.partial_cmp(b_value)? {
            Ordering::Less => Some(a.clone()),
            Ordering::Equal => Some(stricter_bound_for_equal_value(a_value, a, b)),
            Ordering::Greater => Some(b.clone()),
        },
    }
}

fn is_excluded_bound(bound: &Bound<ScalarValue>) -> bool {
    matches!(bound, Bound::Excluded(_))
}

/// For an equal scalar value, an excluded bound is stricter than an included
/// bound. This handles cases like `x >= 5 AND x > 5`.
fn stricter_bound_for_equal_value(
    value: &ScalarValue,
    lhs: &Bound<ScalarValue>,
    rhs: &Bound<ScalarValue>,
) -> Bound<ScalarValue> {
    if is_excluded_bound(lhs) || is_excluded_bound(rhs) {
        Bound::Excluded(value.clone())
    } else {
        Bound::Included(value.clone())
    }
}

fn range_has_non_null_bound(lower: &Bound<ScalarValue>, upper: &Bound<ScalarValue>) -> bool {
    let mut has_bound = false;
    for bound in [lower, upper] {
        match bound {
            Bound::Included(value) | Bound::Excluded(value) => {
                if value.is_null() {
                    return false;
                }
                has_bound = true;
            }
            Bound::Unbounded => {}
        }
    }
    has_bound
}

/// Null bounds are skipped by range optimization. Comparisons with NULL should
/// already be rejected by normal parsing, but this keeps manually constructed
/// ScalarIndexExpr values from being rewritten into misleading ranges.
fn range_has_null_bound(lower: &Bound<ScalarValue>, upper: &Bound<ScalarValue>) -> bool {
    [lower, upper].iter().any(|bound| match bound {
        Bound::Included(value) | Bound::Excluded(value) => value.is_null(),
        Bound::Unbounded => false,
    })
}

impl ScalarIndexSearch {
    /// Only SargableQuery participates in these expression-level rewrites.
    /// Other AnyQuery implementations may have different null/range semantics.
    fn sargable_query(&self) -> Option<&SargableQuery> {
        self.query.as_any().downcast_ref::<SargableQuery>()
    }

    /// Require a concrete SargableQuery before producing a key so callers do not
    /// accidentally group unrelated AnyQuery implementations by metadata alone.
    fn sargable_query_key(&self) -> Option<ScalarIndexQueryKey> {
        self.sargable_query()?;
        Some((
            self.column.clone(),
            self.index_name.clone(),
            self.index_type.clone(),
        ))
    }

    /// Only exact SargableQuery values may drive NULL-elimination rewrites.
    /// Range merging also accepts inexact queries and preserves their recheck.
    fn exact_sargable_query(&self) -> Option<&SargableQuery> {
        if self.needs_recheck {
            return None;
        }
        self.sargable_query()
    }

    /// Return a scalar-index key only when the query is an exact SargableQuery.
    fn exact_sargable_query_key(&self) -> Option<ScalarIndexQueryKey> {
        self.exact_sargable_query()?;
        self.sargable_query_key()
    }

    /// A query is null-intolerant if the original predicate cannot match NULL.
    /// Only exact queries can make `IS NOT NULL` redundant in the index expression.
    fn is_null_intolerant_sargable_query(&self) -> bool {
        match self.exact_sargable_query() {
            Some(SargableQuery::Range(lower, upper)) => range_has_non_null_bound(lower, upper),
            Some(SargableQuery::Equals(value)) => !value.is_null(),
            Some(SargableQuery::IsIn(values)) => {
                !values.is_empty() && values.iter().all(|value| !value.is_null())
            }
            _ => false,
        }
    }
}

impl ScalarIndexExpr {
    /// Apply scalar-index optimizer rules within each contiguous AND region.
    ///
    /// Range queries for the same parsed scalar index are intersected and retain
    /// `needs_recheck` if any input range requires it. An exact `IS NOT NULL`
    /// query is removed when another exact same-index query is null-intolerant.
    /// OR branches are optimized independently. Range queries are still merged
    /// under NOT, but `IS NOT NULL` elimination is disabled there to preserve
    /// SQL NULL semantics.
    ///
    /// Compatible [`SargableQuery::Range`] values carried by
    /// [`ScalarIndexSearch`] are merged into one query.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::{ops::Bound, sync::Arc};
    /// # use lance_index::scalar::{SargableQuery, expression::{ScalarIndexExpr, ScalarIndexSearch}};
    /// # let range = |lower, upper| ScalarIndexExpr::Query(ScalarIndexSearch {
    /// #     column: "x".into(),
    /// #     index_name: "x_idx".into(),
    /// #     index_type: "BTree".into(),
    /// #     query: Arc::new(SargableQuery::Range(lower, upper)),
    /// #     needs_recheck: false,
    /// #     fragment_bitmap: None,
    /// # });
    /// let optimized = ScalarIndexExpr::And(
    ///     Box::new(range(Bound::Included(10_i64.into()), Bound::Unbounded)),
    ///     Box::new(range(Bound::Unbounded, Bound::Included(20_i64.into()))),
    /// )
    /// .optimize();
    ///
    /// assert!(matches!(optimized, ScalarIndexExpr::Query(_)));
    /// ```
    pub fn optimize(self) -> Self {
        self.optimize_with_context(true)
    }

    fn optimize_with_context(self, is_not_null_elidable: bool) -> Self {
        match self {
            Self::And(_, _) => self.optimize_and_tree(is_not_null_elidable),
            Self::Or(lhs, rhs) => Self::Or(
                Box::new(lhs.optimize_with_context(is_not_null_elidable)),
                Box::new(rhs.optimize_with_context(is_not_null_elidable)),
            ),
            Self::Not(inner) => Self::Not(Box::new(inner.optimize_with_context(false))),
            other => other,
        }
    }

    /// Flatten one contiguous AND region, apply local optimizer rules, and
    /// rebuild a balanced AND tree.
    fn optimize_and_tree(self, is_not_null_elidable: bool) -> Self {
        let mut leaves = Vec::new();
        self.collect_and_leaves(&mut leaves, is_not_null_elidable);
        let leaves = Self::optimize_and_leaves(leaves, is_not_null_elidable);

        Self::rebuild_and_tree(leaves).expect("AND tree optimization should keep at least one leaf")
    }

    /// Apply optimizer rules within one AND region only. OR branches have already
    /// been kept as opaque leaves, so range and null-intolerance rewrites cannot
    /// cross boolean boundaries.
    fn optimize_and_leaves(leaves: Vec<Self>, is_not_null_elidable: bool) -> Vec<Self> {
        let null_intolerant_keys = if !is_not_null_elidable {
            HashSet::new()
        } else {
            let is_not_null_keys = leaves
                .iter()
                .filter_map(Self::is_not_null_query_key)
                .collect::<HashSet<_>>();
            if is_not_null_keys.is_empty() {
                HashSet::new()
            } else {
                leaves
                    .iter()
                    .filter_map(Self::null_intolerant_query_key)
                    .filter(|key| is_not_null_keys.contains(key))
                    .collect()
            }
        };

        let mut optimized = Vec::with_capacity(leaves.len());
        let mut range_positions = HashMap::new();

        for leaf in leaves {
            if let Some(key) = leaf.is_not_null_query_key()
                && null_intolerant_keys.contains(&key)
            {
                continue;
            }

            if let Some(key) = leaf.range_query_key() {
                match range_positions.entry(key) {
                    Entry::Vacant(entry) => {
                        entry.insert(optimized.len());
                        optimized.push(leaf);
                    }
                    Entry::Occupied(entry) => {
                        if !optimized[*entry.get()].try_merge_range(&leaf) {
                            optimized.push(leaf);
                        }
                    }
                }
            } else {
                optimized.push(leaf);
            }
        }

        optimized
    }

    /// Recursively collect all leaf nodes from an AND tree. OR branches are
    /// optimized independently with the current null-elision context. NOT
    /// branches remain leaves while their children are optimized with null
    /// elimination disabled.
    fn collect_and_leaves(self, leaves: &mut Vec<Self>, is_not_null_elidable: bool) {
        match self {
            Self::And(lhs, rhs) => {
                lhs.collect_and_leaves(leaves, is_not_null_elidable);
                rhs.collect_and_leaves(leaves, is_not_null_elidable);
            }
            Self::Or(lhs, rhs) => {
                leaves.push(Self::Or(
                    Box::new(lhs.optimize_with_context(is_not_null_elidable)),
                    Box::new(rhs.optimize_with_context(is_not_null_elidable)),
                ));
            }
            Self::Not(inner) => {
                leaves.push(Self::Not(Box::new(inner.optimize_with_context(false))))
            }
            other => leaves.push(other),
        }
    }

    /// Rebuild as a balanced tree so large planner-generated conjunctions do not
    /// become deep left-leaning trees after optimization.
    fn rebuild_and_tree(leaves: Vec<Self>) -> Option<Self> {
        let mut leaves = leaves;
        if leaves.is_empty() {
            return None;
        }

        while leaves.len() > 1 {
            let mut next = Vec::with_capacity(leaves.len().div_ceil(2));
            let mut iter = leaves.into_iter();
            while let Some(lhs) = iter.next() {
                if let Some(rhs) = iter.next() {
                    next.push(Self::And(Box::new(lhs), Box::new(rhs)));
                } else {
                    next.push(lhs);
                }
            }
            leaves = next;
        }

        leaves.pop()
    }

    /// Detect the scalar-index representation of `col IS NOT NULL`, which is
    /// stored as `NOT(col IS NULL)`.
    fn is_not_null_query_key(&self) -> Option<ScalarIndexQueryKey> {
        match self {
            Self::Not(inner) => match inner.as_ref() {
                Self::Query(search)
                    if matches!(search.sargable_query(), Some(SargableQuery::IsNull())) =>
                {
                    search.exact_sargable_query_key()
                }
                _ => None,
            },
            _ => None,
        }
    }

    /// Return the key of a same-column predicate that makes `IS NOT NULL`
    /// redundant in the same AND region.
    fn null_intolerant_query_key(&self) -> Option<ScalarIndexQueryKey> {
        match self {
            Self::Query(search) if search.is_null_intolerant_sargable_query() => {
                search.exact_sargable_query_key()
            }
            Self::Not(inner) => match inner.as_ref() {
                Self::Query(search)
                    if matches!(
                        search.exact_sargable_query(),
                        Some(SargableQuery::Equals(value)) if !value.is_null()
                    ) =>
                {
                    search.exact_sargable_query_key()
                }
                _ => None,
            },
            _ => None,
        }
    }

    /// Return the grouping key for an optimizable range query. Ranges with NULL
    /// bounds are left unchanged.
    fn range_query_key(&self) -> Option<ScalarIndexQueryKey> {
        let Self::Query(search) = self else {
            return None;
        };
        let SargableQuery::Range(lower, upper) = search.sargable_query()? else {
            return None;
        };
        if range_has_null_bound(lower, upper) {
            return None;
        }
        search.sargable_query_key()
    }

    /// Merge another compatible range into this expression.
    ///
    /// The caller must first group both expressions by [`ScalarIndexQueryKey`].
    /// Different fragment coverage or incomparable bounds leave both predicates
    /// unchanged. Empty intersections are retained as ranges.
    fn try_merge_range(&mut self, other: &Self) -> bool {
        let (Self::Query(search), Self::Query(other_search)) = (self, other) else {
            return false;
        };
        if search.fragment_bitmap != other_search.fragment_bitmap {
            return false;
        }

        let (
            Some(SargableQuery::Range(lower, upper)),
            Some(SargableQuery::Range(other_lower, other_upper)),
        ) = (search.sargable_query(), other_search.sargable_query())
        else {
            return false;
        };
        let Some(lower) = tighter_lower_bound(lower, other_lower) else {
            return false;
        };
        let Some(upper) = tighter_upper_bound(upper, other_upper) else {
            return false;
        };

        search.query = Arc::new(SargableQuery::Range(lower, upper));
        search.needs_recheck |= other_search.needs_recheck;
        true
    }
}

impl std::fmt::Display for ScalarIndexExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Not(inner) => write!(f, "NOT({})", inner),
            Self::And(lhs, rhs) => write!(f, "AND({},{})", lhs, rhs),
            Self::Or(lhs, rhs) => write!(f, "OR({},{})", lhs, rhs),
            Self::Query(search) => write!(
                f,
                "[{}]@{}({})",
                search.query.format(&search.column),
                search.index_name,
                search.index_type
            ),
        }
    }
}

fn search_result_to_nullable(result: SearchResult) -> NullableIndexExprResult {
    match result {
        SearchResult::Exact(mask) => {
            NullableIndexExprResult::exact(NullableRowAddrMask::AllowList(mask))
        }
        SearchResult::AtMost(mask) => {
            NullableIndexExprResult::at_most(NullableRowAddrMask::AllowList(mask))
        }
        SearchResult::AtLeast(mask) => {
            NullableIndexExprResult::at_least(NullableRowAddrMask::AllowList(mask))
        }
    }
}

impl ScalarIndexExpr {
    /// Evaluates the scalar index expression
    ///
    /// This will result in loading one or more scalar indices and searching them
    ///
    /// TODO: We could potentially try and be smarter about reusing loaded indices for
    /// any situations where the session cache has been disabled.
    #[async_recursion]
    pub async fn evaluate_nullable(
        &self,
        index_loader: &dyn ScalarIndexLoader,
        metrics: &dyn MetricsCollector,
    ) -> Result<NullableIndexExprResult> {
        match self {
            Self::Not(inner) => {
                let result = inner.evaluate_nullable(index_loader, metrics).await?;
                Ok(!result)
            }
            Self::And(lhs, rhs) => {
                let lhs_result = lhs.evaluate_nullable(index_loader, metrics);
                let rhs_result = rhs.evaluate_nullable(index_loader, metrics);
                let (lhs_result, rhs_result) = try_join!(lhs_result, rhs_result)?;
                Ok(lhs_result & rhs_result)
            }
            Self::Or(lhs, rhs) => {
                let lhs_result = lhs.evaluate_nullable(index_loader, metrics);
                let rhs_result = rhs.evaluate_nullable(index_loader, metrics);
                let (lhs_result, rhs_result) = try_join!(lhs_result, rhs_result)?;
                Ok(lhs_result | rhs_result)
            }
            Self::Query(search) => {
                let index = index_loader
                    .load_index(&search.column, &search.index_name, metrics)
                    .await?;
                let search_result = index.search(search.query.as_ref(), metrics).await?;
                let result = search_result_to_nullable(search_result);
                if index.results_are_row_addresses() {
                    // Translate address-domain results to the row-id domain
                    // before combining or scanning; otherwise stable-row-id
                    // datasets silently drop matches (issue #7434).
                    index_loader.row_addr_result_to_row_ids(result).await
                } else {
                    Ok(result)
                }
            }
        }
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn evaluate(
        &self,
        index_loader: &dyn ScalarIndexLoader,
        metrics: &dyn MetricsCollector,
    ) -> Result<IndexExprResult> {
        Ok(self
            .evaluate_nullable(index_loader, metrics)
            .await?
            .drop_nulls())
    }

    pub fn to_expr(&self) -> Expr {
        match self {
            Self::Not(inner) => Expr::Not(inner.to_expr().into()),
            Self::And(lhs, rhs) => {
                let lhs = lhs.to_expr();
                let rhs = rhs.to_expr();
                lhs.and(rhs)
            }
            Self::Or(lhs, rhs) => {
                let lhs = lhs.to_expr();
                let rhs = rhs.to_expr();
                lhs.or(rhs)
            }
            Self::Query(search) => search.query.to_expr(search.column.clone()),
        }
    }

    pub fn needs_recheck(&self) -> bool {
        match self {
            Self::Not(inner) => inner.needs_recheck(),
            Self::And(lhs, rhs) | Self::Or(lhs, rhs) => lhs.needs_recheck() || rhs.needs_recheck(),
            Self::Query(search) => search.needs_recheck,
        }
    }
}

// Extract a column from the expression, if it is a column, or None
fn maybe_column(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(col) => Some(&col.name),
        _ => None,
    }
}

// Extract the full nested column path from chained or variadic get_field expressions.
// For example, both get_field(get_field(metadata, "status"), "code") and
// get_field(metadata, "status", "code") produce "metadata.status.code".
fn extract_nested_column_path(expr: &Expr) -> Option<String> {
    let mut current_expr = expr;
    let mut parts = Vec::new();

    // Walk up the get_field chain. Add variadic arguments in reverse because all
    // parts are reversed after reaching the base column.
    loop {
        match current_expr {
            Expr::ScalarFunction(udf) if udf.name() == "get_field" => {
                if udf.args.len() < 2 {
                    return None;
                }

                for field_expr in udf.args[1..].iter().rev() {
                    if let Expr::Literal(ScalarValue::Utf8(Some(field_name)), _) = field_expr {
                        parts.push(field_name.clone());
                    } else {
                        return None;
                    }
                }

                current_expr = &udf.args[0];
            }
            Expr::Column(col) => {
                // We've reached the base column
                parts.push(col.name.clone());
                break;
            }
            _ => {
                return None;
            }
        }
    }

    // Reverse to get the correct order (parent.child.grandchild)
    parts.reverse();

    // Format the path correctly
    let field_refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
    Some(lance_core::datatypes::format_field_path(&field_refs))
}

// Extract a column from the expression, if it is a column, and we have an index for that column, or None
//
// There's two ways to get a column.  First, the obvious way, is a
// simple column reference (e.g. x = 7).  Second, a more complex way,
// is some kind of projection into a column (e.g. json_extract(json, '$.name')).
// Third way is nested field access (e.g. get_field(metadata, "status.code"))
fn maybe_indexed_column<'b>(
    expr: &Expr,
    index_info: &'b dyn IndexInformationProvider,
) -> Option<(String, DataType, &'b dyn ScalarQueryParser)> {
    // First try to extract the full nested column path for get_field expressions
    if let Some(nested_path) = extract_nested_column_path(expr)
        && let Some((data_type, multi)) = index_info.get_index(&nested_path)
        && let Some((parser, data_type)) = multi.select(expr, data_type)
    {
        return Some((nested_path, data_type, parser));
    }

    match expr {
        Expr::Column(col) => {
            let col = col.name.as_str();
            let (data_type, multi) = index_info.get_index(col)?;
            let (parser, data_type) = multi.select(expr, data_type)?;
            Some((col.to_string(), data_type, parser))
        }
        Expr::ScalarFunction(udf) => {
            if udf.args.is_empty() {
                return None;
            }
            // For non-get_field functions, fall back to old behavior
            let col = maybe_column(&udf.args[0])?;
            let (data_type, multi) = index_info.get_index(col)?;
            let (parser, data_type) = multi.select(expr, data_type)?;
            Some((col.to_string(), data_type, parser))
        }
        _ => None,
    }
}

// Extract a literal scalar value from an expression, if it is a literal, or None
fn maybe_scalar(expr: &Expr, expected_type: &DataType) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(value, _) => safe_coerce_scalar(value, expected_type),
        // Some literals can't be expressed in datafusion's SQL and can only be expressed with
        // a cast.  For example, there is no way to express a fixed-size-binary literal (which is
        // commonly used for UUID).  As a result the expression could look like...
        //
        // col = arrow_cast(value, 'fixed_size_binary(16)')
        //
        // In this case we need to extract the value, apply the cast, and then test the casted value
        Expr::Cast(cast) => match cast.expr.as_ref() {
            Expr::Literal(value, _) => {
                let casted = value.cast_to(cast.field.data_type()).ok()?;
                safe_coerce_scalar(&casted, expected_type)
            }
            _ => None,
        },
        Expr::ScalarFunction(scalar_function) => {
            if scalar_function.name() == "arrow_cast" {
                if scalar_function.args.len() != 2 {
                    return None;
                }
                match (&scalar_function.args[0], &scalar_function.args[1]) {
                    (Expr::Literal(value, _), Expr::Literal(cast_type, _)) => {
                        let target_type = scalar_function
                            .func
                            .return_field_from_args(ReturnFieldArgs {
                                arg_fields: &[
                                    Arc::new(Field::new("expression", value.data_type(), false)),
                                    Arc::new(Field::new("datatype", cast_type.data_type(), false)),
                                ],
                                scalar_arguments: &[Some(value), Some(cast_type)],
                            })
                            .ok()?;
                        let casted = value.cast_to(target_type.data_type()).ok()?;
                        safe_coerce_scalar(&casted, expected_type)
                    }
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

// Extract a list of scalar values from an expression, if it is a list of scalar values, or None
fn maybe_scalar_list(exprs: &Vec<Expr>, expected_type: &DataType) -> Option<Vec<ScalarValue>> {
    let mut scalar_values = Vec::with_capacity(exprs.len());
    for expr in exprs {
        match maybe_scalar(expr, expected_type) {
            Some(scalar_val) => {
                scalar_values.push(scalar_val);
            }
            None => {
                return None;
            }
        }
    }
    Some(scalar_values)
}

fn visit_between(
    between: &Between,
    index_info: &dyn IndexInformationProvider,
) -> Option<IndexedExpression> {
    let (column, col_type, query_parser) = maybe_indexed_column(&between.expr, index_info)?;
    let low = maybe_scalar(&between.low, &col_type)?;
    let high = maybe_scalar(&between.high, &col_type)?;

    let indexed_expr =
        query_parser.visit_between(&column, &Bound::Included(low), &Bound::Included(high))?;

    if between.negated {
        indexed_expr.maybe_not()
    } else {
        Some(indexed_expr)
    }
}

fn visit_in_list(
    in_list: &InList,
    index_info: &dyn IndexInformationProvider,
) -> Option<IndexedExpression> {
    let (column, col_type, query_parser) = maybe_indexed_column(&in_list.expr, index_info)?;
    let values = maybe_scalar_list(&in_list.list, &col_type)?;

    let indexed_expr = query_parser.visit_in_list(&column, &values)?;

    if in_list.negated {
        indexed_expr.maybe_not()
    } else {
        Some(indexed_expr)
    }
}

fn visit_is_bool(
    expr: &Expr,
    index_info: &dyn IndexInformationProvider,
    value: bool,
) -> Option<IndexedExpression> {
    let (column, col_type, query_parser) = maybe_indexed_column(expr, index_info)?;
    if col_type != DataType::Boolean {
        None
    } else {
        query_parser.visit_is_bool(&column, value)
    }
}

// A column can be a valid indexed expression if the column is boolean (e.g. 'WHERE on_sale')
fn visit_column(
    col: &Expr,
    index_info: &dyn IndexInformationProvider,
) -> Option<IndexedExpression> {
    let (column, col_type, query_parser) = maybe_indexed_column(col, index_info)?;
    if col_type != DataType::Boolean {
        None
    } else {
        query_parser.visit_is_bool(&column, true)
    }
}

fn visit_is_null(
    expr: &Expr,
    index_info: &dyn IndexInformationProvider,
    negated: bool,
) -> Option<IndexedExpression> {
    let (column, _, query_parser) = maybe_indexed_column(expr, index_info)?;
    let indexed_expr = query_parser.visit_is_null(&column)?;
    if negated {
        indexed_expr.maybe_not()
    } else {
        Some(indexed_expr)
    }
}

fn visit_not(
    expr: &Expr,
    index_info: &dyn IndexInformationProvider,
    depth: usize,
) -> Result<Option<IndexedExpression>> {
    let node = visit_node(expr, index_info, depth + 1)?;
    Ok(node.and_then(|node| node.maybe_not()))
}

fn visit_comparison(
    expr: &BinaryExpr,
    index_info: &dyn IndexInformationProvider,
) -> Option<IndexedExpression> {
    let left_col = maybe_indexed_column(&expr.left, index_info);
    if let Some((column, col_type, query_parser)) = left_col {
        let scalar = maybe_scalar(&expr.right, &col_type)?;
        query_parser.visit_comparison(&column, &scalar, &expr.op)
    } else {
        // Datafusion's query simplifier will canonicalize expressions and so we shouldn't reach this case.  If, for some reason, we
        // do reach this case we can handle it in the future by inverting expr.op and swapping the left and right sides
        None
    }
}

fn maybe_range(
    expr: &BinaryExpr,
    index_info: &dyn IndexInformationProvider,
) -> Option<IndexedExpression> {
    let left_expr = match expr.left.as_ref() {
        Expr::BinaryExpr(binary_expr) => Some(binary_expr),
        _ => None,
    }?;
    let right_expr = match expr.right.as_ref() {
        Expr::BinaryExpr(binary_expr) => Some(binary_expr),
        _ => None,
    }?;

    let (left_col, dt, parser) = maybe_indexed_column(&left_expr.left, index_info)?;
    let right_col = maybe_column(&right_expr.left)?;

    if left_col != right_col {
        return None;
    }

    let left_value = maybe_scalar(&left_expr.right, &dt)?;
    let right_value = maybe_scalar(&right_expr.right, &dt)?;

    let (low, high) = match (left_expr.op, right_expr.op) {
        // x >= a && x <= b
        (Operator::GtEq, Operator::LtEq) => {
            (Bound::Included(left_value), Bound::Included(right_value))
        }
        // x >= a && x < b
        (Operator::GtEq, Operator::Lt) => {
            (Bound::Included(left_value), Bound::Excluded(right_value))
        }
        // x > a && x <= b
        (Operator::Gt, Operator::LtEq) => {
            (Bound::Excluded(left_value), Bound::Included(right_value))
        }
        // x > a && x < b
        (Operator::Gt, Operator::Lt) => (Bound::Excluded(left_value), Bound::Excluded(right_value)),
        // x <= a && x >= b
        (Operator::LtEq, Operator::GtEq) => {
            (Bound::Included(right_value), Bound::Included(left_value))
        }
        // x <= a && x > b
        (Operator::LtEq, Operator::Gt) => {
            (Bound::Excluded(right_value), Bound::Included(left_value))
        }
        // x < a && x >= b
        (Operator::Lt, Operator::GtEq) => {
            (Bound::Included(right_value), Bound::Excluded(left_value))
        }
        // x < a && x > b
        (Operator::Lt, Operator::Gt) => (Bound::Excluded(right_value), Bound::Excluded(left_value)),
        _ => return None,
    };

    parser.visit_between(&left_col, &low, &high)
}

fn visit_and(
    expr: &BinaryExpr,
    index_info: &dyn IndexInformationProvider,
    depth: usize,
) -> Result<Option<IndexedExpression>> {
    // Many scalar indices can efficiently handle a BETWEEN query as a single search and this
    // can be much more efficient than two separate range queries.  As an optimization we check
    // to see if this is a between query and, if so, we handle it as a single query
    //
    // Note: We can't rely on users writing the SQL BETWEEN operator because:
    //   * Some users won't realize it's an option or a good idea
    //   * Datafusion's simplifier will rewrite the BETWEEN operator into two separate range queries
    if let Some(range_expr) = maybe_range(expr, index_info) {
        return Ok(Some(range_expr));
    }

    let left = visit_node(&expr.left, index_info, depth + 1)?;
    let right = visit_node(&expr.right, index_info, depth + 1)?;
    Ok(match (left, right) {
        (Some(left), Some(right)) => Some(left.and(right)),
        (Some(left), None) => Some(left.refine((*expr.right).clone())),
        (None, Some(right)) => Some(right.refine((*expr.left).clone())),
        (None, None) => None,
    })
}

fn visit_or(
    expr: &BinaryExpr,
    index_info: &dyn IndexInformationProvider,
    depth: usize,
) -> Result<Option<IndexedExpression>> {
    let left = visit_node(&expr.left, index_info, depth + 1)?;
    let right = visit_node(&expr.right, index_info, depth + 1)?;
    Ok(match (left, right) {
        (Some(left), Some(right)) => left.maybe_or(right),
        // If one side can use an index and the other side cannot then
        // we must abandon the entire thing.  For example, consider the
        // query "color == 'blue' or size > 10" where color is indexed but
        // size is not.  It's entirely possible that size > 10 matches every
        // row in our database.  There is nothing we can do except a full scan
        (Some(_), None) => None,
        (None, Some(_)) => None,
        (None, None) => None,
    })
}

fn visit_binary_expr(
    expr: &BinaryExpr,
    index_info: &dyn IndexInformationProvider,
    depth: usize,
) -> Result<Option<IndexedExpression>> {
    match &expr.op {
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq | Operator::Eq => {
            Ok(visit_comparison(expr, index_info))
        }
        // visit_comparison will maybe create an Eq query which we negate
        Operator::NotEq => Ok(visit_comparison(expr, index_info).and_then(|node| node.maybe_not())),
        Operator::And => visit_and(expr, index_info, depth),
        Operator::Or => visit_or(expr, index_info, depth),
        _ => Ok(None),
    }
}

fn visit_scalar_fn(
    scalar_fn: &ScalarFunction,
    index_info: &dyn IndexInformationProvider,
) -> Option<IndexedExpression> {
    if scalar_fn.args.is_empty() {
        return None;
    }
    let (col, data_type, query_parser) = maybe_indexed_column(&scalar_fn.args[0], index_info)?;
    query_parser.visit_scalar_function(&col, &data_type, &scalar_fn.func, &scalar_fn.args)
}

fn visit_like_expr(
    like: &Like,
    index_info: &dyn IndexInformationProvider,
) -> Option<IndexedExpression> {
    let (column, _, query_parser) = maybe_indexed_column(&like.expr, index_info)?;

    // Extract the pattern as a ScalarValue
    let pattern = match like.pattern.as_ref() {
        Expr::Literal(scalar, _) => scalar.clone(),
        _ => return None,
    };

    query_parser.visit_like(&column, like, &pattern)
}

fn visit_node(
    expr: &Expr,
    index_info: &dyn IndexInformationProvider,
    depth: usize,
) -> Result<Option<IndexedExpression>> {
    if depth >= MAX_DEPTH {
        return Err(Error::invalid_input(format!(
            "the filter expression is too long, lance limit the max number of conditions to {}",
            MAX_DEPTH
        )));
    }
    match expr {
        Expr::Between(between) => Ok(visit_between(between, index_info)),
        Expr::Alias(alias) => visit_node(alias.expr.as_ref(), index_info, depth),
        Expr::Column(_) => Ok(visit_column(expr, index_info)),
        Expr::InList(in_list) => Ok(visit_in_list(in_list, index_info)),
        Expr::IsFalse(expr) => Ok(visit_is_bool(expr.as_ref(), index_info, false)),
        Expr::IsTrue(expr) => Ok(visit_is_bool(expr.as_ref(), index_info, true)),
        Expr::IsNull(expr) => Ok(visit_is_null(expr.as_ref(), index_info, false)),
        Expr::IsNotNull(expr) => {
            // `regexp_match(col, pat)` returns a list and is coerced to
            // `IsNotNull(regexp_match(...))` before it reaches here. Unwrap that
            // so the regex acceleration applies; everything else is a genuine
            // IS NOT NULL check.
            if let Expr::ScalarFunction(scalar_fn) = expr.as_ref()
                && scalar_fn.func.name() == "regexp_match"
            {
                return Ok(visit_scalar_fn(scalar_fn, index_info));
            }
            Ok(visit_is_null(expr.as_ref(), index_info, true))
        }
        Expr::Not(expr) => visit_not(expr.as_ref(), index_info, depth),
        Expr::BinaryExpr(binary_expr) => visit_binary_expr(binary_expr, index_info, depth),
        Expr::ScalarFunction(scalar_fn) => Ok(visit_scalar_fn(scalar_fn, index_info)),
        Expr::Like(like) => {
            if like.negated {
                // NOT LIKE cannot be efficiently pruned with zone maps
                Ok(None)
            } else {
                Ok(visit_like_expr(like, index_info))
            }
        }
        _ => Ok(None),
    }
}

/// A trait to be used in `apply_scalar_indices` to inform the function which columns are indexeds
pub trait IndexInformationProvider {
    /// Check if an index exists for `col` and, if so, return the data type of col
    /// as well as a query parser that can parse queries for that column
    fn get_index(&self, col: &str) -> Option<(&DataType, &MultiQueryParser)>;

    /// The set of fragments covered by `(column, index_name)`.
    ///
    /// Returns `None` when the provider doesn't know — callers must treat
    /// that as "coverage unknown" rather than "covers everything". The
    /// default implementation always returns `None`, so providers that
    /// haven't been updated cannot accidentally claim full coverage.
    fn fragment_bitmap(&self, _column: &str, _index_name: &str) -> Option<RoaringBitmap> {
        None
    }
}

/// Attempt to split a filter expression into a search of scalar indexes and an
///   optional post-search refinement query
pub fn apply_scalar_indices(
    expr: Expr,
    index_info: &dyn IndexInformationProvider,
) -> Result<IndexedExpression> {
    let mut result =
        visit_node(&expr, index_info, 0)?.unwrap_or(IndexedExpression::refine_only(expr));
    if let Some(query) = result.scalar_query.as_mut() {
        populate_fragment_bitmaps(query, index_info);
    }
    Ok(result)
}

/// Walk a [`ScalarIndexExpr`] and fill in `fragment_bitmap` on each leaf from
/// the `index_info` provider. Leaves the bitmap as `None` if the provider
/// can't answer.
fn populate_fragment_bitmaps(
    expr: &mut ScalarIndexExpr,
    index_info: &dyn IndexInformationProvider,
) {
    match expr {
        ScalarIndexExpr::Not(inner) => populate_fragment_bitmaps(inner, index_info),
        ScalarIndexExpr::And(lhs, rhs) | ScalarIndexExpr::Or(lhs, rhs) => {
            populate_fragment_bitmaps(lhs, index_info);
            populate_fragment_bitmaps(rhs, index_info);
        }
        ScalarIndexExpr::Query(search) => {
            search.fragment_bitmap = index_info.fragment_bitmap(&search.column, &search.index_name);
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct FilterPlan {
    pub index_query: Option<ScalarIndexExpr>,
    /// True if the index query is guaranteed to return exact results
    pub skip_recheck: bool,
    pub refine_expr: Option<Expr>,
    pub full_expr: Option<Expr>,
}

impl FilterPlan {
    pub fn empty() -> Self {
        Self {
            index_query: None,
            skip_recheck: true,
            refine_expr: None,
            full_expr: None,
        }
    }

    pub fn new_refine_only(expr: Expr) -> Self {
        Self {
            index_query: None,
            skip_recheck: true,
            refine_expr: Some(expr.clone()),
            full_expr: Some(expr),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.refine_expr.is_none() && self.index_query.is_none()
    }

    pub fn all_columns(&self) -> Vec<String> {
        self.full_expr
            .as_ref()
            .map(Planner::column_names_in_expr)
            .unwrap_or_default()
    }

    pub fn refine_columns(&self) -> Vec<String> {
        self.refine_expr
            .as_ref()
            .map(Planner::column_names_in_expr)
            .unwrap_or_default()
    }

    /// Return true if this has a refine step, regardless of the status of prefilter
    pub fn has_refine(&self) -> bool {
        self.refine_expr.is_some()
    }

    /// Return true if this has a scalar index query
    pub fn has_index_query(&self) -> bool {
        self.index_query.is_some()
    }

    pub fn has_any_filter(&self) -> bool {
        self.refine_expr.is_some() || self.index_query.is_some()
    }

    pub fn make_refine_only(&mut self) {
        self.index_query = None;
        self.refine_expr = self.full_expr.clone();
    }

    /// Return true if there is no refine or recheck of any kind and there is an index query
    pub fn is_exact_index_search(&self) -> bool {
        self.index_query.is_some() && self.refine_expr.is_none() && self.skip_recheck
    }
}

pub trait PlannerIndexExt {
    /// Determine how to apply a provided filter
    ///
    /// We parse the filter into a logical expression.  We then
    /// split the logical expression into a portion that can be
    /// satisfied by an index search (of one or more indices) and
    /// a refine portion that must be applied after the index search
    fn create_filter_plan(
        &self,
        filter: Expr,
        index_info: &dyn IndexInformationProvider,
        use_scalar_index: bool,
    ) -> Result<FilterPlan>;
}

impl PlannerIndexExt for Planner {
    fn create_filter_plan(
        &self,
        filter: Expr,
        index_info: &dyn IndexInformationProvider,
        use_scalar_index: bool,
    ) -> Result<FilterPlan> {
        let logical_expr = self.optimize_expr(filter)?;
        if use_scalar_index {
            let indexed_expr = apply_scalar_indices(logical_expr.clone(), index_info)?;
            let mut skip_recheck = false;
            if let Some(scalar_query) = indexed_expr.scalar_query.as_ref() {
                skip_recheck = !scalar_query.needs_recheck();
            }
            Ok(FilterPlan {
                index_query: indexed_expr.scalar_query,
                refine_expr: indexed_expr.refine_expr,
                full_expr: Some(logical_expr),
                skip_recheck,
            })
        } else {
            Ok(FilterPlan {
                index_query: None,
                skip_recheck: true,
                refine_expr: Some(logical_expr.clone()),
                full_expr: Some(logical_expr),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_array::Array;
    use arrow_schema::{Field, Fields, Schema};
    use chrono::Utc;
    use datafusion_common::{Column, DFSchema};
    use datafusion_expr::simplify::SimplifyContext;
    use lance_datafusion::exec::{LanceExecutionOptions, get_session_context};
    use lance_select::result::IndexExprResultWireFormat;
    use roaring::RoaringBitmap;
    use rstest::rstest;

    use crate::scalar::json::{JsonQuery, JsonQueryParser};

    use super::*;

    struct ColInfo {
        data_type: DataType,
        parser: Box<MultiQueryParser>,
    }

    impl ColInfo {
        fn new(data_type: DataType, parser: Box<dyn ScalarQueryParser>) -> Self {
            Self {
                data_type,
                parser: Box::new(MultiQueryParser::single(parser)),
            }
        }

        fn with_multi(data_type: DataType, parser: Box<MultiQueryParser>) -> Self {
            Self { data_type, parser }
        }
    }

    struct MockIndexInfoProvider {
        indexed_columns: HashMap<String, ColInfo>,
    }

    impl MockIndexInfoProvider {
        fn new(indexed_columns: Vec<(&str, ColInfo)>) -> Self {
            Self {
                indexed_columns: HashMap::from_iter(
                    indexed_columns
                        .into_iter()
                        .map(|(s, ty)| (s.to_string(), ty)),
                ),
            }
        }
    }

    impl IndexInformationProvider for MockIndexInfoProvider {
        fn get_index(&self, col: &str) -> Option<(&DataType, &MultiQueryParser)> {
            self.indexed_columns
                .get(col)
                .map(|col_info| (&col_info.data_type, col_info.parser.as_ref()))
        }
    }

    fn check(
        index_info: &dyn IndexInformationProvider,
        expr: &str,
        expected: Option<IndexedExpression>,
        optimize: bool,
    ) {
        let schema = Schema::new(vec![
            Field::new("color", DataType::Utf8, false),
            Field::new("size", DataType::Float32, false),
            Field::new("aisle", DataType::UInt32, false),
            Field::new("on_sale", DataType::Boolean, false),
            Field::new("price", DataType::Float32, false),
            Field::new("json", DataType::LargeBinary, false),
        ]);
        check_with_schema(index_info, expr, expected, optimize, schema);
    }

    fn check_with_schema(
        index_info: &dyn IndexInformationProvider,
        expr: &str,
        expected: Option<IndexedExpression>,
        optimize: bool,
        schema: Schema,
    ) {
        let df_schema: DFSchema = schema.try_into().unwrap();

        let ctx = get_session_context(&LanceExecutionOptions::default());
        let state = ctx.state();
        let mut expr = state.create_logical_expr(expr, &df_schema).unwrap();
        if optimize {
            let simplify_context = SimplifyContext::builder()
                .with_schema(Arc::new(df_schema))
                .with_query_execution_start_time(Some(Utc::now()))
                .build();
            let simplifier =
                datafusion::optimizer::simplify_expressions::ExprSimplifier::new(simplify_context);
            expr = simplifier.simplify(expr).unwrap();
        }

        let actual = apply_scalar_indices(expr.clone(), index_info).unwrap();
        if let Some(expected) = expected {
            assert_eq!(actual, expected);
        } else {
            assert!(actual.scalar_query.is_none());
            assert_eq!(actual.refine_expr.unwrap(), expr);
        }
    }

    fn check_no_index(index_info: &dyn IndexInformationProvider, expr: &str) {
        check(index_info, expr, None, false)
    }

    fn check_simple(
        index_info: &dyn IndexInformationProvider,
        expr: &str,
        col: &str,
        query: impl AnyQuery,
    ) {
        check(
            index_info,
            expr,
            Some(IndexedExpression::index_query(
                col.to_string(),
                format!("{}_idx", col),
                "BTree".to_string(),
                Arc::new(query),
            )),
            false,
        )
    }

    fn check_range(
        index_info: &dyn IndexInformationProvider,
        expr: &str,
        col: &str,
        query: SargableQuery,
    ) {
        check(
            index_info,
            expr,
            Some(IndexedExpression::index_query(
                col.to_string(),
                format!("{}_idx", col),
                "BTree".to_string(),
                Arc::new(query),
            )),
            true,
        )
    }

    fn check_simple_negated(
        index_info: &dyn IndexInformationProvider,
        expr: &str,
        col: &str,
        query: SargableQuery,
    ) {
        check(
            index_info,
            expr,
            Some(
                IndexedExpression::index_query(
                    col.to_string(),
                    format!("{}_idx", col),
                    "BTree".to_string(),
                    Arc::new(query),
                )
                .maybe_not()
                .unwrap(),
            ),
            false,
        )
    }

    #[rstest]
    #[case::list(DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))))]
    #[case::large_list(DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))))]
    fn test_label_list_query_parser(#[case] label_type: DataType) {
        let index_info = MockIndexInfoProvider::new(vec![(
            "labels",
            ColInfo::new(
                label_type.clone(),
                Box::new(LabelListQueryParser::new(
                    "labels_idx".to_string(),
                    "LabelList".to_string(),
                )),
            ),
        )]);
        let schema = Schema::new(vec![Field::new("labels", label_type, true)]);

        check_with_schema(
            &index_info,
            "array_has_any(labels, ['distributed'])",
            Some(IndexedExpression::index_query(
                "labels".to_string(),
                "labels_idx".to_string(),
                "LabelList".to_string(),
                Arc::new(LabelListQuery::HasAnyLabel(vec![ScalarValue::Utf8(Some(
                    "distributed".to_string(),
                ))])),
            )),
            true,
            schema,
        );
    }

    #[rstest]
    #[case::list(DataType::List(Arc::new(Field::new(
        "item",
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    ))))]
    #[case::large_list(DataType::LargeList(Arc::new(Field::new(
        "item",
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    ))))]
    fn test_label_list_nested_item(#[case] label_type: DataType) {
        let index_info = MockIndexInfoProvider::new(vec![(
            "labels",
            ColInfo::new(
                label_type.clone(),
                Box::new(LabelListQueryParser::new(
                    "labels_idx".to_string(),
                    "LabelList".to_string(),
                )),
            ),
        )]);
        let schema = Schema::new(vec![Field::new("labels", label_type, true)]);

        // Nested item types must not produce a scalar index query.
        check_with_schema(
            &index_info,
            "array_has_any(labels, [[1]])",
            None,
            true,
            schema,
        );
    }

    #[test]
    fn test_nested_column_index() {
        let column = "user_profile.basic.age";
        let index_info = MockIndexInfoProvider::new(vec![(
            column,
            ColInfo::new(
                DataType::Int32,
                Box::new(SargableQueryParser::new(
                    format!("{column}_idx"),
                    "BTree".to_string(),
                    false,
                )),
            ),
        )]);
        let schema = Schema::new(vec![Field::new(
            "user_profile",
            DataType::Struct(Fields::from(vec![Field::new(
                "basic",
                DataType::Struct(Fields::from(vec![Field::new("age", DataType::Int32, true)])),
                true,
            )])),
            true,
        )]);

        let planner = Planner::new(Arc::new(schema));
        let filter = planner
            .parse_filter("user_profile.basic.age > 900")
            .unwrap();
        let plan = planner
            .create_filter_plan(filter, &index_info, true)
            .unwrap();
        let expected = IndexedExpression::index_query(
            column.to_string(),
            format!("{column}_idx"),
            "BTree".to_string(),
            Arc::new(SargableQuery::Range(
                Bound::Excluded(ScalarValue::Int32(Some(900))),
                Bound::Unbounded,
            )),
        );

        assert_eq!(plan.index_query, expected.scalar_query);
        assert!(plan.refine_expr.is_none());
    }

    #[test]
    fn test_expressions() {
        let index_info = MockIndexInfoProvider::new(vec![
            (
                "color",
                ColInfo::new(
                    DataType::Utf8,
                    Box::new(SargableQueryParser::new(
                        "color_idx".to_string(),
                        "BTree".to_string(),
                        false,
                    )),
                ),
            ),
            (
                "aisle",
                ColInfo::new(
                    DataType::UInt32,
                    Box::new(SargableQueryParser::new(
                        "aisle_idx".to_string(),
                        "BTree".to_string(),
                        false,
                    )),
                ),
            ),
            (
                "on_sale",
                ColInfo::new(
                    DataType::Boolean,
                    Box::new(SargableQueryParser::new(
                        "on_sale_idx".to_string(),
                        "BTree".to_string(),
                        false,
                    )),
                ),
            ),
            (
                "price",
                ColInfo::new(
                    DataType::Float32,
                    Box::new(SargableQueryParser::new(
                        "price_idx".to_string(),
                        "BTree".to_string(),
                        false,
                    )),
                ),
            ),
            (
                "json",
                ColInfo::new(
                    DataType::LargeBinary,
                    Box::new(JsonQueryParser::new(
                        "$.name".to_string(),
                        Box::new(SargableQueryParser::new(
                            "json_idx".to_string(),
                            "BTree".to_string(),
                            false,
                        )),
                    )),
                ),
            ),
        ]);

        check_simple(
            &index_info,
            "json_extract(json, '$.name') = 'foo'",
            "json",
            JsonQuery::new(
                Arc::new(SargableQuery::Equals(ScalarValue::Utf8(Some(
                    "foo".to_string(),
                )))),
                "$.name".to_string(),
            ),
        );

        check_no_index(&index_info, "size BETWEEN 5 AND 10");
        // Cast case.  We will cast 5 (an int64) to Int16 and then coerce to UInt32
        check_simple(
            &index_info,
            "aisle = arrow_cast(5, 'Int16')",
            "aisle",
            SargableQuery::Equals(ScalarValue::UInt32(Some(5))),
        );
        // 5 different ways of writing BETWEEN (all should be recognized)
        check_range(
            &index_info,
            "aisle BETWEEN 5 AND 10",
            "aisle",
            SargableQuery::Range(
                Bound::Included(ScalarValue::UInt32(Some(5))),
                Bound::Included(ScalarValue::UInt32(Some(10))),
            ),
        );
        check_range(
            &index_info,
            "aisle >= 5 AND aisle <= 10",
            "aisle",
            SargableQuery::Range(
                Bound::Included(ScalarValue::UInt32(Some(5))),
                Bound::Included(ScalarValue::UInt32(Some(10))),
            ),
        );

        check_range(
            &index_info,
            "aisle <= 10 AND aisle >= 5",
            "aisle",
            SargableQuery::Range(
                Bound::Included(ScalarValue::UInt32(Some(5))),
                Bound::Included(ScalarValue::UInt32(Some(10))),
            ),
        );

        check_range(
            &index_info,
            "5 <= aisle AND 10 >= aisle",
            "aisle",
            SargableQuery::Range(
                Bound::Included(ScalarValue::UInt32(Some(5))),
                Bound::Included(ScalarValue::UInt32(Some(10))),
            ),
        );

        check_range(
            &index_info,
            "10 >= aisle AND 5 <= aisle",
            "aisle",
            SargableQuery::Range(
                Bound::Included(ScalarValue::UInt32(Some(5))),
                Bound::Included(ScalarValue::UInt32(Some(10))),
            ),
        );
        check_range(
            &index_info,
            "aisle <= 10 AND aisle > 5",
            "aisle",
            SargableQuery::Range(
                Bound::Excluded(ScalarValue::UInt32(Some(5))),
                Bound::Included(ScalarValue::UInt32(Some(10))),
            ),
        );
        check_range(
            &index_info,
            "aisle < 10 AND aisle >= 5",
            "aisle",
            SargableQuery::Range(
                Bound::Included(ScalarValue::UInt32(Some(5))),
                Bound::Excluded(ScalarValue::UInt32(Some(10))),
            ),
        );
        check_simple(
            &index_info,
            "on_sale IS TRUE",
            "on_sale",
            SargableQuery::Equals(ScalarValue::Boolean(Some(true))),
        );
        check_simple(
            &index_info,
            "on_sale",
            "on_sale",
            SargableQuery::Equals(ScalarValue::Boolean(Some(true))),
        );
        check_simple_negated(
            &index_info,
            "NOT on_sale",
            "on_sale",
            SargableQuery::Equals(ScalarValue::Boolean(Some(true))),
        );
        check_simple(
            &index_info,
            "on_sale IS FALSE",
            "on_sale",
            SargableQuery::Equals(ScalarValue::Boolean(Some(false))),
        );
        check_simple_negated(
            &index_info,
            "aisle NOT BETWEEN 5 AND 10",
            "aisle",
            SargableQuery::Range(
                Bound::Included(ScalarValue::UInt32(Some(5))),
                Bound::Included(ScalarValue::UInt32(Some(10))),
            ),
        );
        // Small in-list (in-list with 3 or fewer items optimizes into or-chain)
        check_simple(
            &index_info,
            "aisle IN (5, 6, 7)",
            "aisle",
            SargableQuery::IsIn(vec![
                ScalarValue::UInt32(Some(5)),
                ScalarValue::UInt32(Some(6)),
                ScalarValue::UInt32(Some(7)),
            ]),
        );
        check_simple_negated(
            &index_info,
            "NOT aisle IN (5, 6, 7)",
            "aisle",
            SargableQuery::IsIn(vec![
                ScalarValue::UInt32(Some(5)),
                ScalarValue::UInt32(Some(6)),
                ScalarValue::UInt32(Some(7)),
            ]),
        );
        check_simple_negated(
            &index_info,
            "aisle NOT IN (5, 6, 7)",
            "aisle",
            SargableQuery::IsIn(vec![
                ScalarValue::UInt32(Some(5)),
                ScalarValue::UInt32(Some(6)),
                ScalarValue::UInt32(Some(7)),
            ]),
        );
        check_simple(
            &index_info,
            "aisle IN (5, 6, 7, 8, 9)",
            "aisle",
            SargableQuery::IsIn(vec![
                ScalarValue::UInt32(Some(5)),
                ScalarValue::UInt32(Some(6)),
                ScalarValue::UInt32(Some(7)),
                ScalarValue::UInt32(Some(8)),
                ScalarValue::UInt32(Some(9)),
            ]),
        );
        check_simple_negated(
            &index_info,
            "NOT aisle IN (5, 6, 7, 8, 9)",
            "aisle",
            SargableQuery::IsIn(vec![
                ScalarValue::UInt32(Some(5)),
                ScalarValue::UInt32(Some(6)),
                ScalarValue::UInt32(Some(7)),
                ScalarValue::UInt32(Some(8)),
                ScalarValue::UInt32(Some(9)),
            ]),
        );
        check_simple_negated(
            &index_info,
            "aisle NOT IN (5, 6, 7, 8, 9)",
            "aisle",
            SargableQuery::IsIn(vec![
                ScalarValue::UInt32(Some(5)),
                ScalarValue::UInt32(Some(6)),
                ScalarValue::UInt32(Some(7)),
                ScalarValue::UInt32(Some(8)),
                ScalarValue::UInt32(Some(9)),
            ]),
        );
        check_simple(
            &index_info,
            "on_sale is false",
            "on_sale",
            SargableQuery::Equals(ScalarValue::Boolean(Some(false))),
        );
        check_simple(
            &index_info,
            "on_sale is true",
            "on_sale",
            SargableQuery::Equals(ScalarValue::Boolean(Some(true))),
        );
        check_simple(
            &index_info,
            "aisle < 10",
            "aisle",
            SargableQuery::Range(
                Bound::Unbounded,
                Bound::Excluded(ScalarValue::UInt32(Some(10))),
            ),
        );
        check_simple(
            &index_info,
            "aisle <= 10",
            "aisle",
            SargableQuery::Range(
                Bound::Unbounded,
                Bound::Included(ScalarValue::UInt32(Some(10))),
            ),
        );
        check_simple(
            &index_info,
            "aisle > 10",
            "aisle",
            SargableQuery::Range(
                Bound::Excluded(ScalarValue::UInt32(Some(10))),
                Bound::Unbounded,
            ),
        );
        // In the future we can handle this case if we need to.  For
        // now let's make sure we don't accidentally do the wrong thing
        // (we were getting this backwards in the past)
        check_no_index(&index_info, "10 > aisle");
        check_simple(
            &index_info,
            "aisle >= 10",
            "aisle",
            SargableQuery::Range(
                Bound::Included(ScalarValue::UInt32(Some(10))),
                Bound::Unbounded,
            ),
        );
        check_simple(
            &index_info,
            "aisle = 10",
            "aisle",
            SargableQuery::Equals(ScalarValue::UInt32(Some(10))),
        );
        check_simple_negated(
            &index_info,
            "aisle <> 10",
            "aisle",
            SargableQuery::Equals(ScalarValue::UInt32(Some(10))),
        );
        // // Common compound case, AND'd clauses
        let left = Box::new(ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "aisle".to_string(),
            index_name: "aisle_idx".to_string(),
            index_type: "BTree".to_string(),
            query: Arc::new(SargableQuery::Equals(ScalarValue::UInt32(Some(10)))),
            needs_recheck: false,
            fragment_bitmap: None,
        }));
        let right = Box::new(ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "color".to_string(),
            index_name: "color_idx".to_string(),
            index_type: "BTree".to_string(),
            query: Arc::new(SargableQuery::Equals(ScalarValue::Utf8(Some(
                "blue".to_string(),
            )))),
            needs_recheck: false,
            fragment_bitmap: None,
        }));
        check(
            &index_info,
            "aisle = 10 AND color = 'blue'",
            Some(IndexedExpression {
                scalar_query: Some(ScalarIndexExpr::And(left.clone(), right.clone())),
                refine_expr: None,
            }),
            false,
        );
        // Compound AND's and not all of them are indexed columns
        let refine = Expr::Column(Column::new_unqualified("size")).gt(datafusion_expr::lit(30_i64));
        check(
            &index_info,
            "aisle = 10 AND color = 'blue' AND size > 30",
            Some(IndexedExpression {
                scalar_query: Some(ScalarIndexExpr::And(left.clone(), right.clone())),
                refine_expr: Some(refine.clone()),
            }),
            false,
        );
        // Compounded OR's where ALL columns are indexed
        check(
            &index_info,
            "aisle = 10 OR color = 'blue'",
            Some(IndexedExpression {
                scalar_query: Some(ScalarIndexExpr::Or(left.clone(), right.clone())),
                refine_expr: None,
            }),
            false,
        );
        // Compounded OR's with one or more unindexed columns
        check_no_index(&index_info, "aisle = 10 OR color = 'blue' OR size > 30");
        // AND'd group of OR
        check(
            &index_info,
            "(aisle = 10 OR color = 'blue') AND size > 30",
            Some(IndexedExpression {
                scalar_query: Some(ScalarIndexExpr::Or(left, right)),
                refine_expr: Some(refine),
            }),
            false,
        );
        // Examples of things that are not yet supported but should be supportable someday

        // OR'd group of refined index searches (see IndexedExpression::or for details)
        check_no_index(
            &index_info,
            "(aisle = 10 AND size > 30) OR (color = 'blue' AND size > 20)",
        );

        // Non-normalized arithmetic (can use expression simplification)
        check_no_index(&index_info, "aisle + 3 < 10");

        // Currently we assume that the return of an index search tells us which rows are
        // TRUE and all other rows are FALSE.  This will need to change but for now it is
        // safer to not support the following cases because the return value of non-matched
        // rows is NULL and not FALSE.
        check_no_index(&index_info, "aisle IN (5, 6, NULL)");
        // OR-list with NULL (in future DF version this will be optimized repr of
        // small in-list with NULL so let's get ready for it)
        check_no_index(&index_info, "aisle = 5 OR aisle = 6 OR NULL");
        check_no_index(&index_info, "aisle IN (5, 6, 7, 8, NULL)");
        check_no_index(&index_info, "aisle = NULL");
        check_no_index(&index_info, "aisle BETWEEN 5 AND NULL");
        check_no_index(&index_info, "aisle BETWEEN NULL AND 10");
    }

    #[tokio::test]
    async fn test_not_flips_certainty() {
        use lance_select::{NullableRowAddrSet, RowAddrTreeMap};

        // Test that NOT flips certainty for inexact index results.
        // Under the {lower, upper} form, `!{l, u} = {!u, !l}`, which
        // preserves the AtMost ↔ AtLeast swap and leaves Exact as Exact.

        // AtMost: superset of matches (e.g., bloom filter says "might be in [1,2]")
        let at_most = NullableIndexExprResult::at_most(NullableRowAddrMask::AllowList(
            NullableRowAddrSet::new(RowAddrTreeMap::from_iter(&[1, 2]), RowAddrTreeMap::new()),
        ));
        // NOT(AtMost) should be AtLeast (definitely NOT in [1,2], might be elsewhere)
        assert!((!at_most).is_at_least());

        // AtLeast: subset of matches (e.g., definitely in [1,2], might be more)
        let at_least = NullableIndexExprResult::at_least(NullableRowAddrMask::AllowList(
            NullableRowAddrSet::new(RowAddrTreeMap::from_iter(&[1, 2]), RowAddrTreeMap::new()),
        ));
        // NOT(AtLeast) should be AtMost (might NOT be in [1,2], definitely elsewhere)
        assert!((!at_least).is_at_most());

        // Exact should stay Exact
        let exact = NullableIndexExprResult::exact(NullableRowAddrMask::AllowList(
            NullableRowAddrSet::new(RowAddrTreeMap::from_iter(&[1, 2]), RowAddrTreeMap::new()),
        ));
        assert!((!exact).is_exact());
    }

    #[tokio::test]
    async fn test_and_or_preserve_certainty() {
        use lance_select::{NullableRowAddrSet, RowAddrTreeMap};

        // Test that AND/OR correctly propagate certainty under the
        // {lower, upper} algebra. Each binary op is elementwise on the
        // endpoints, so degenerate shapes (Exact / AtMost / AtLeast)
        // combine into a result that lands in one of those same shapes
        // in every case exercised below.
        let make_at_most = || {
            NullableIndexExprResult::at_most(NullableRowAddrMask::AllowList(
                NullableRowAddrSet::new(
                    RowAddrTreeMap::from_iter(&[1, 2, 3]),
                    RowAddrTreeMap::new(),
                ),
            ))
        };

        let make_at_least = || {
            NullableIndexExprResult::at_least(NullableRowAddrMask::AllowList(
                NullableRowAddrSet::new(
                    RowAddrTreeMap::from_iter(&[2, 3, 4]),
                    RowAddrTreeMap::new(),
                ),
            ))
        };

        let make_exact = || {
            NullableIndexExprResult::exact(NullableRowAddrMask::AllowList(NullableRowAddrSet::new(
                RowAddrTreeMap::from_iter(&[1, 2]),
                RowAddrTreeMap::new(),
            )))
        };

        // AtMost & AtMost → AtMost
        assert!((make_at_most() & make_at_most()).is_at_most());

        // AtLeast & AtLeast → AtLeast
        assert!((make_at_least() & make_at_least()).is_at_least());

        // AtMost & AtLeast → AtMost (the lower side stays empty)
        assert!((make_at_most() & make_at_least()).is_at_most());

        // AtMost | AtMost → AtMost
        assert!((make_at_most() | make_at_most()).is_at_most());

        // AtLeast | AtLeast → AtLeast
        assert!((make_at_least() | make_at_least()).is_at_least());

        // AtMost | AtLeast → AtLeast (upper stays universe)
        assert!((make_at_most() | make_at_least()).is_at_least());

        // Exact & AtMost → AtMost
        assert!((make_exact() & make_at_most()).is_at_most());

        // Exact | AtLeast → AtLeast
        assert!((make_exact() | make_at_least()).is_at_least());
    }

    /// The whole point of the `{lower, upper}` representation is that it
    /// can express a Refined result — a non-empty `lower` strictly inside
    /// a non-universe `upper` — which the old enum couldn't. This test
    /// constructs one through the algebra and verifies the endpoints.
    #[tokio::test]
    async fn test_refined_result_constructed_through_algebra() {
        use lance_select::{NullableRowAddrSet, RowAddrTreeMap};

        let allow_set = |rows: &[u64]| {
            NullableRowAddrMask::AllowList(NullableRowAddrSet::new(
                RowAddrTreeMap::from_iter(rows),
                RowAddrTreeMap::new(),
            ))
        };

        // AtLeast({1,2}) & Exact({1,2,3}) is Refined, because:
        //   lower = {1,2} ∩ {1,2,3} = {1,2}        (non-empty)
        //   upper = universe ∩ {1,2,3} = {1,2,3}   (not universe)
        //   lower ≠ upper                          (not Exact)
        let at_least_12 = NullableIndexExprResult::at_least(allow_set(&[1, 2]));
        let exact_123 = NullableIndexExprResult::exact(allow_set(&[1, 2, 3]));
        let refined = at_least_12 & exact_123;

        // None of the shape predicates should fire — that's what makes
        // this a Refined result.
        assert!(
            !refined.is_exact(),
            "Refined must not be classified as Exact"
        );
        assert!(
            !refined.is_at_most(),
            "Refined must not be classified as AtMost"
        );
        assert!(
            !refined.is_at_least(),
            "Refined must not be classified as AtLeast"
        );

        // Check the actual endpoints.
        assert_eq!(refined.lower, allow_set(&[1, 2]));
        assert_eq!(refined.upper, allow_set(&[1, 2, 3]));

        // NOT swaps the endpoints, preserving the Refined shape.
        let negated = !refined;
        assert!(!negated.is_exact());
        assert!(!negated.is_at_most());
        assert!(!negated.is_at_least());
        // !{l, u} = {!u, !l}. AllowList → BlockList.
        assert!(matches!(negated.lower, NullableRowAddrMask::BlockList(_)));
        assert!(matches!(negated.upper, NullableRowAddrMask::BlockList(_)));
    }

    #[test]
    fn test_like_to_regex() {
        // `%` -> `.*`, `_` -> `.`, with a literal run of at least three chars.
        assert_eq!(like_to_regex("%foo%", None).as_deref(), Some(".*foo.*"));
        assert_eq!(like_to_regex("foo%bar", None).as_deref(), Some("foo.*bar"));
        assert_eq!(like_to_regex("foo_bar", None).as_deref(), Some("foo.bar"));
        assert_eq!(like_to_regex("foobar", None).as_deref(), Some("foobar"));

        // Regex metacharacters in the literal portion are escaped.
        assert_eq!(
            like_to_regex("%a.bcd%", None).as_deref(),
            Some(".*a\\.bcd.*")
        );

        // No literal run of three alphanumeric characters -> no index help.
        assert_eq!(like_to_regex("%ab%", None), None);
        assert_eq!(like_to_regex("%a%b%c%", None), None);
        assert_eq!(like_to_regex("%", None), None);

        // The escape character makes the following character a literal.
        assert_eq!(
            like_to_regex(r"%foo\%bar%", Some('\\')).as_deref(),
            Some(".*foo%bar.*")
        );
    }

    #[test]
    fn test_apply_regex_flags() {
        fn flags(s: &str) -> Expr {
            Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None)
        }

        // Empty flags leave the pattern untouched (no inline group emitted).
        assert_eq!(apply_regex_flags("foo", &flags("")).as_deref(), Some("foo"));
        // Supported flags are folded into an inline `(?...)` prefix.
        assert_eq!(
            apply_regex_flags("foo", &flags("i")).as_deref(),
            Some("(?i)foo")
        );
        assert_eq!(
            apply_regex_flags("foo", &flags("is")).as_deref(),
            Some("(?is)foo")
        );
        // An unrecognized flag bails out so the caller leaves the predicate to a
        // full recheck rather than risk changing its semantics.
        assert_eq!(apply_regex_flags("foo", &flags("g")), None);
        // A non-string (hence non-literal-flags) argument cannot be folded.
        assert_eq!(
            apply_regex_flags("foo", &Expr::Literal(ScalarValue::Int32(Some(1)), None)),
            None
        );
    }

    #[test]
    fn test_extract_like_leading_prefix() {
        // Simple prefix patterns (no recheck needed)
        assert_eq!(
            extract_like_leading_prefix("foo%", None),
            Some(("foo".to_string(), false))
        );
        assert_eq!(
            extract_like_leading_prefix("abc%", None),
            Some(("abc".to_string(), false))
        );

        // Patterns with wildcards in the middle (need recheck)
        assert_eq!(
            extract_like_leading_prefix("foo%bar%", None),
            Some(("foo".to_string(), true))
        );
        assert_eq!(
            extract_like_leading_prefix("foo_bar%", None),
            Some(("foo".to_string(), true))
        );
        assert_eq!(
            extract_like_leading_prefix("foo%bar", None),
            Some(("foo".to_string(), true))
        );
        assert_eq!(
            extract_like_leading_prefix("foo_", None),
            Some(("foo".to_string(), true))
        );

        // Not prefix patterns (starts with wildcard)
        assert_eq!(extract_like_leading_prefix("%foo", None), None);
        assert_eq!(extract_like_leading_prefix("_foo%", None), None);
        assert_eq!(extract_like_leading_prefix("%", None), None);

        // No wildcard at all (should use equality)
        assert_eq!(extract_like_leading_prefix("foo", None), None);

        // With escape character
        assert_eq!(
            extract_like_leading_prefix(r"foo\%bar%", Some('\\')),
            Some(("foo%bar".to_string(), false))
        );
        assert_eq!(
            extract_like_leading_prefix(r"foo\_bar%", Some('\\')),
            Some(("foo_bar".to_string(), false))
        );
        assert_eq!(
            extract_like_leading_prefix(r"foo\\bar%", Some('\\')),
            Some(("foo\\bar".to_string(), false))
        );

        // Escaped trailing % is not a wildcard (no wildcards)
        assert_eq!(extract_like_leading_prefix(r"foo\%", Some('\\')), None);

        // With backslash as default escape (for DataFusion starts_with compatibility):
        // "foo\%" means escaped %, no wildcard -> None (should use equality)
        assert_eq!(extract_like_leading_prefix(r"foo\%", None), None);
        // "foo\bar%" - \b is not a valid escape sequence, so \ and b are literals, % is wildcard
        assert_eq!(
            extract_like_leading_prefix(r"foo\bar%", None),
            Some(("foo\\bar".to_string(), false))
        );

        // Empty pattern
        assert_eq!(extract_like_leading_prefix("", None), None);

        // Mixed escaped and unescaped
        assert_eq!(
            extract_like_leading_prefix(r"foo\%bar%baz%", Some('\\')),
            Some(("foo%bar".to_string(), true))
        );
    }

    #[test]
    fn test_like_expression_parsing() {
        // Test that LIKE expressions are parsed correctly with refine_expr for complex patterns

        let index_info = MockIndexInfoProvider::new(vec![(
            "color",
            ColInfo::new(
                DataType::Utf8,
                Box::new(SargableQueryParser::new(
                    "color_idx".to_string(),
                    "BTree".to_string(),
                    false,
                )),
            ),
        )]);

        // Simple prefix pattern: LIKE 'foo%' -> LikePrefix("foo"), no refine_expr
        let schema = Schema::new(vec![Field::new("color", DataType::Utf8, false)]);
        let df_schema: DFSchema = schema.try_into().unwrap();
        let ctx = get_session_context(&LanceExecutionOptions::default());
        let state = ctx.state();

        let expr = state
            .create_logical_expr("color LIKE 'foo%'", &df_schema)
            .unwrap();
        let result = apply_scalar_indices(expr, &index_info).unwrap();

        assert!(result.scalar_query.is_some(), "Should have scalar_query");
        assert!(
            result.refine_expr.is_none(),
            "Simple prefix should not need refine_expr"
        );

        // Extract the query and verify it's LikePrefix
        if let Some(ScalarIndexExpr::Query(search)) = &result.scalar_query {
            let query = search.query.as_any().downcast_ref::<SargableQuery>();
            assert!(query.is_some(), "Query should be SargableQuery");
            match query.unwrap() {
                SargableQuery::LikePrefix(prefix) => {
                    assert_eq!(prefix, &ScalarValue::Utf8(Some("foo".to_string())));
                }
                _ => panic!("Expected LikePrefix query"),
            }
        } else {
            panic!("Expected Query variant");
        }

        // Complex pattern: LIKE 'foo%bar%' -> LikePrefix("foo"), with refine_expr
        let expr = state
            .create_logical_expr("color LIKE 'foo%bar%'", &df_schema)
            .unwrap();
        let result = apply_scalar_indices(expr, &index_info).unwrap();

        assert!(result.scalar_query.is_some(), "Should have scalar_query");
        assert!(
            result.refine_expr.is_some(),
            "Complex pattern should have refine_expr"
        );

        // Verify the query is still LikePrefix("foo")
        if let Some(ScalarIndexExpr::Query(search)) = &result.scalar_query {
            let query = search.query.as_any().downcast_ref::<SargableQuery>();
            assert!(query.is_some(), "Query should be SargableQuery");
            match query.unwrap() {
                SargableQuery::LikePrefix(prefix) => {
                    assert_eq!(prefix, &ScalarValue::Utf8(Some("foo".to_string())));
                }
                _ => panic!("Expected LikePrefix query"),
            }
        }

        // Verify the refine_expr is the original LIKE expression
        let refine = result.refine_expr.unwrap();
        match refine {
            Expr::Like(like) => {
                assert!(!like.negated);
                assert!(!like.case_insensitive);
                if let Expr::Literal(ScalarValue::Utf8(Some(pattern)), _) = like.pattern.as_ref() {
                    assert_eq!(pattern, "foo%bar%");
                } else {
                    panic!("Expected Utf8 literal pattern");
                }
            }
            _ => panic!("Expected Like expression in refine_expr"),
        }

        // Pattern starting with wildcard: LIKE '%foo' -> no index, only refine
        let expr = state
            .create_logical_expr("color LIKE '%foo'", &df_schema)
            .unwrap();
        let result = apply_scalar_indices(expr, &index_info).unwrap();

        assert!(
            result.scalar_query.is_none(),
            "Pattern starting with wildcard should not use index"
        );
        assert!(result.refine_expr.is_some(), "Should fall back to refine");
    }

    #[test]
    fn test_starts_with_with_underscore_after_optimization() {
        // Test that starts_with with underscore in prefix works correctly after DataFusion optimization
        // DataFusion simplifies starts_with(col, 'test_ns$') to col LIKE 'test_ns$%'
        // The underscore in the prefix should NOT be treated as a wildcard!
        let index_info = MockIndexInfoProvider::new(vec![(
            "object_id",
            ColInfo::new(
                DataType::Utf8,
                Box::new(SargableQueryParser::new(
                    "object_id_idx".to_string(),
                    "BTree".to_string(),
                    false,
                )),
            ),
        )]);

        let schema = Schema::new(vec![Field::new("object_id", DataType::Utf8, false)]);
        let df_schema: DFSchema = schema.try_into().unwrap();
        let ctx = get_session_context(&LanceExecutionOptions::default());
        let state = ctx.state();

        // Create the expression with starts_with containing underscore
        let expr = state
            .create_logical_expr("starts_with(object_id, 'test_ns$')", &df_schema)
            .unwrap();

        // Apply DataFusion simplification (this may convert starts_with to LIKE)
        let simplify_context = SimplifyContext::builder()
            .with_schema(Arc::new(df_schema))
            .with_query_execution_start_time(Some(Utc::now()))
            .build();
        let simplifier =
            datafusion::optimizer::simplify_expressions::ExprSimplifier::new(simplify_context);
        let simplified_expr = simplifier.simplify(expr).unwrap();

        // Apply scalar indices
        let result = apply_scalar_indices(simplified_expr, &index_info).unwrap();

        // The prefix should be "test_ns$", NOT "test"
        // This test documents the current (potentially broken) behavior
        if let Some(ScalarIndexExpr::Query(search)) = &result.scalar_query {
            let query = search
                .query
                .as_any()
                .downcast_ref::<SargableQuery>()
                .unwrap();
            match query {
                SargableQuery::LikePrefix(prefix) => {
                    let prefix_str = match prefix {
                        ScalarValue::Utf8(Some(s)) => s.clone(),
                        _ => panic!("Expected Utf8 prefix"),
                    };
                    // Verify the prefix is correctly extracted with underscore as literal
                    assert_eq!(
                        prefix_str, "test_ns$",
                        "Prefix should be 'test_ns$', not 'test' (underscore should not be a wildcard)"
                    );
                }
                _ => panic!("Expected LikePrefix query"),
            }
        } else {
            // If no scalar query, it means the pattern was not recognized
            panic!("Expected scalar_query to be present");
        }
    }

    #[test]
    fn test_starts_with_to_like_conversion() {
        // Test that starts_with(col, 'prefix') is converted to LikePrefix query
        let index_info = MockIndexInfoProvider::new(vec![(
            "color",
            ColInfo::new(
                DataType::Utf8,
                Box::new(SargableQueryParser::new(
                    "color_idx".to_string(),
                    "BTree".to_string(),
                    false,
                )),
            ),
        )]);

        let schema = Schema::new(vec![Field::new("color", DataType::Utf8, false)]);
        let df_schema: DFSchema = schema.try_into().unwrap();
        let ctx = get_session_context(&LanceExecutionOptions::default());
        let state = ctx.state();

        // starts_with(color, 'foo') should be converted to LikePrefix("foo")
        let expr = state
            .create_logical_expr("starts_with(color, 'foo')", &df_schema)
            .unwrap();
        let result = apply_scalar_indices(expr, &index_info).unwrap();

        assert!(
            result.scalar_query.is_some(),
            "starts_with should use index"
        );
        assert!(
            result.refine_expr.is_none(),
            "Pure prefix starts_with should not need refine_expr"
        );

        // Extract the query and verify it's LikePrefix
        if let Some(ScalarIndexExpr::Query(search)) = &result.scalar_query {
            let query = search.query.as_any().downcast_ref::<SargableQuery>();
            assert!(query.is_some(), "Query should be SargableQuery");
            match query.unwrap() {
                SargableQuery::LikePrefix(prefix) => {
                    assert_eq!(prefix, &ScalarValue::Utf8(Some("foo".to_string())));
                }
                _ => panic!("Expected LikePrefix query"),
            }
        } else {
            panic!("Expected Query variant");
        }

        // Both starts_with and LIKE 'prefix%' should produce the same LikePrefix query
        let like_expr = state
            .create_logical_expr("color LIKE 'foo%'", &df_schema)
            .unwrap();
        let like_result = apply_scalar_indices(like_expr, &index_info).unwrap();

        // Compare the queries - both should be LikePrefix("foo")
        if let (
            Some(ScalarIndexExpr::Query(starts_with_search)),
            Some(ScalarIndexExpr::Query(like_search)),
        ) = (&result.scalar_query, &like_result.scalar_query)
        {
            let sw_query = starts_with_search
                .query
                .as_any()
                .downcast_ref::<SargableQuery>()
                .unwrap();
            let like_query = like_search
                .query
                .as_any()
                .downcast_ref::<SargableQuery>()
                .unwrap();
            assert_eq!(
                sw_query, like_query,
                "starts_with and LIKE 'prefix%' should produce identical queries"
            );
        }
    }

    #[test]
    fn test_sargable_query_parser_utf8view() {
        // Follow-up to PR #7310 / #7139: the BTree `SargableQueryParser` must accept
        // `Utf8View` prefixes for `starts_with` and infix-free LIKE, not only `Utf8` /
        // `LargeUtf8`. DataFusion can coerce the predicate literal to `ScalarValue::Utf8View`;
        // dropping that variant silently skips the index. The `Utf8View` prefix is normalized
        // to `Utf8` (Lance stores `Utf8View` columns as `Utf8`), so the emitted query is a
        // `LikePrefix(Utf8(..))`. `visit_scalar_function` / `visit_like` are exercised directly
        // so the test does not depend on the planner's coercion choices, and the `Utf8` case
        // is a parity control: the pre-existing path must keep behaving identically.
        let parser = SargableQueryParser::new("color_idx".to_string(), "BTree".to_string(), false);

        let assert_like_prefix =
            |indexed: &IndexedExpression, expected: &ScalarValue, needs_refine: bool| {
                assert_eq!(
                    indexed.refine_expr.is_some(),
                    needs_refine,
                    "unexpected refine_expr presence"
                );
                let Some(ScalarIndexExpr::Query(search)) = &indexed.scalar_query else {
                    panic!("expected a scalar index query");
                };
                match search
                    .query
                    .as_any()
                    .downcast_ref::<SargableQuery>()
                    .expect("query should be a SargableQuery")
                {
                    SargableQuery::LikePrefix(prefix) => assert_eq!(prefix, expected),
                    _ => panic!("expected a LikePrefix query"),
                }
            };

        // starts_with(col, <Utf8View prefix>) -> LikePrefix(Utf8). Reuse a real
        // `starts_with` UDF parsed from SQL, then swap in a `Utf8View` literal argument.
        let schema = Schema::new(vec![Field::new("color", DataType::Utf8View, false)]);
        let df_schema: DFSchema = schema.try_into().unwrap();
        let ctx = get_session_context(&LanceExecutionOptions::default());
        let state = ctx.state();
        let Expr::ScalarFunction(starts_with) = state
            .create_logical_expr("starts_with(color, 'foo')", &df_schema)
            .unwrap()
        else {
            panic!("expected starts_with to parse as a scalar function");
        };
        let args = vec![
            starts_with.args[0].clone(),
            Expr::Literal(ScalarValue::Utf8View(Some("foo".to_string())), None),
        ];
        let indexed = parser
            .visit_scalar_function(
                "color",
                &DataType::Utf8View,
                starts_with.func.as_ref(),
                &args,
            )
            .expect("starts_with should use the BTree index");
        assert_like_prefix(&indexed, &ScalarValue::Utf8(Some("foo".to_string())), false);

        // col LIKE <Utf8View pattern>. `visit_like` is called directly so the test does not
        // depend on DataFusion's LIKE type coercion choosing `Utf8View` for the pattern.
        let like = |pattern: ScalarValue| {
            Like::new(
                false,
                Box::new(Expr::Column(Column::new_unqualified("color"))),
                Box::new(Expr::Literal(pattern, None)),
                None,
                false,
            )
        };

        // Pure prefix: routed to the index with no recheck needed.
        let pattern = ScalarValue::Utf8View(Some("foo%".to_string()));
        let indexed = parser
            .visit_like("color", &like(pattern.clone()), &pattern)
            .expect("LIKE prefix should use the BTree index");
        assert_like_prefix(&indexed, &ScalarValue::Utf8(Some("foo".to_string())), false);

        // Wildcards beyond the leading prefix keep the original LIKE as a recheck.
        let pattern = ScalarValue::Utf8View(Some("foo%bar%".to_string()));
        let indexed = parser
            .visit_like("color", &like(pattern.clone()), &pattern)
            .expect("LIKE prefix should use the BTree index");
        assert_like_prefix(&indexed, &ScalarValue::Utf8(Some("foo".to_string())), true);

        // Parity control: the pre-existing `Utf8` path is unchanged.
        let pattern = ScalarValue::Utf8(Some("foo%".to_string()));
        let indexed = parser
            .visit_like("color", &like(pattern.clone()), &pattern)
            .expect("LIKE prefix should use the BTree index");
        assert_like_prefix(&indexed, &ScalarValue::Utf8(Some("foo".to_string())), false);
    }

    #[test]
    fn test_sargable_query_parser_without_like_prefix() {
        // Bitmap indexes configure the parser with `without_like_prefix`: a bitmap index
        // cannot answer `LikePrefix` queries (its `search` rejects them), so `starts_with` /
        // `LIKE 'prefix%'` must not be turned into an index query. Returning `None` lets the
        // predicate fall back to ordinary filtering instead of failing at search time.
        let bitmap_parser =
            SargableQueryParser::new("color_idx".to_string(), "BITMAP".to_string(), false)
                .without_like_prefix();
        let btree_parser =
            SargableQueryParser::new("color_idx".to_string(), "BTree".to_string(), false);

        let schema = Schema::new(vec![Field::new("color", DataType::Utf8, false)]);
        let df_schema: DFSchema = schema.try_into().unwrap();
        let ctx = get_session_context(&LanceExecutionOptions::default());
        let state = ctx.state();
        let Expr::ScalarFunction(starts_with) = state
            .create_logical_expr("starts_with(color, 'foo')", &df_schema)
            .unwrap()
        else {
            panic!("expected starts_with to parse as a scalar function");
        };

        let pattern = ScalarValue::Utf8(Some("foo%".to_string()));
        let like = Like::new(
            false,
            Box::new(Expr::Column(Column::new_unqualified("color"))),
            Box::new(Expr::Literal(pattern.clone(), None)),
            None,
            false,
        );

        // Bitmap parser: both prefix paths fall back (return `None`).
        assert!(
            bitmap_parser
                .visit_scalar_function(
                    "color",
                    &DataType::Utf8,
                    starts_with.func.as_ref(),
                    &starts_with.args,
                )
                .is_none(),
            "bitmap parser must not emit a LikePrefix for starts_with"
        );
        assert!(
            bitmap_parser.visit_like("color", &like, &pattern).is_none(),
            "bitmap parser must not emit a LikePrefix for LIKE"
        );

        // A prefix-capable parser (e.g. BTree) still emits the index query.
        assert!(
            btree_parser
                .visit_scalar_function(
                    "color",
                    &DataType::Utf8,
                    starts_with.func.as_ref(),
                    &starts_with.args,
                )
                .is_some(),
            "BTree parser should still emit a LikePrefix for starts_with"
        );
        assert!(
            btree_parser.visit_like("color", &like, &pattern).is_some(),
            "BTree parser should still emit a LikePrefix for LIKE"
        );
    }

    #[test]
    fn test_serialize_index_expr_result_round_trip() {
        use lance_select::{RowAddrMask, RowAddrTreeMap};

        for format in [
            IndexExprResultWireFormat::TwoMask,
            IndexExprResultWireFormat::ThreeVariant,
        ] {
            let mut addrs = RowAddrTreeMap::new();
            addrs.insert_range(0..5);
            addrs.insert_range(100..103);

            let mut fragments_covered = RoaringBitmap::new();
            fragments_covered.insert(0);
            fragments_covered.insert(7);

            let cases = [
                (
                    "exact",
                    IndexExprResult::exact(RowAddrMask::from_allowed(addrs.clone())),
                ),
                (
                    "at_most",
                    IndexExprResult::at_most(RowAddrMask::from_allowed(addrs.clone())),
                ),
                (
                    "at_least",
                    IndexExprResult::at_least(RowAddrMask::from_allowed(addrs)),
                ),
            ];

            for (label, original) in cases {
                let batch = original.serialize(&fragments_covered, format).unwrap();
                assert_eq!(
                    batch.schema(),
                    *format.schema(),
                    "format {format:?}, case {label}"
                );
                assert_eq!(batch.num_rows(), 2, "format {format:?}, case {label}");

                let (round_tripped, round_tripped_frags) =
                    IndexExprResult::deserialize(&batch).unwrap();
                assert_eq!(
                    round_tripped.lower, original.lower,
                    "format {format:?}, case {label}: lower"
                );
                assert_eq!(
                    round_tripped.upper, original.upper,
                    "format {format:?}, case {label}: upper"
                );
                assert_eq!(
                    round_tripped_frags, fragments_covered,
                    "format {format:?}, case {label}: frags"
                );
                assert_eq!(
                    round_tripped.is_exact(),
                    original.is_exact(),
                    "format {format:?}, case {label}"
                );
                assert_eq!(
                    round_tripped.is_at_most(),
                    original.is_at_most(),
                    "format {format:?}, case {label}"
                );
                assert_eq!(
                    round_tripped.is_at_least(),
                    original.is_at_least(),
                    "format {format:?}, case {label}"
                );
            }
        }
    }

    /// Exact results encode `upper` as a fully-null column on the wire — the
    /// payload only needs to ship once. `RowAddrMask::into_arrow` never
    /// produces a fully-null array (it always sets exactly one of the two
    /// rows), so the sentinel can't collide with a real mask. This pins
    /// both halves: exact ⇒ upper fully null, non-exact ⇒ upper carries the
    /// real mask.
    #[test]
    fn test_serialize_omits_upper_when_exact() {
        use lance_select::{RowAddrMask, RowAddrTreeMap};

        let mask = RowAddrMask::from_allowed(RowAddrTreeMap::from_iter(0u64..5));
        let fragments_covered = RoaringBitmap::from_iter([0u32]);

        use arrow::array::AsArray;

        // Exact: upper column must be fully null on the wire.
        let exact_batch = IndexExprResult::exact(mask.clone())
            .serialize(&fragments_covered, IndexExprResultWireFormat::TwoMask)
            .unwrap();
        let exact_upper = exact_batch.column(1).as_binary::<i32>();
        assert!(exact_upper.is_null(0) && exact_upper.is_null(1));

        // Non-exact (at_most): upper column must carry the upper mask, so at
        // least one row is non-null (`AllowList(mask)` puts the payload at
        // row 1).
        let at_most_batch = IndexExprResult::at_most(mask.clone())
            .serialize(&fragments_covered, IndexExprResultWireFormat::TwoMask)
            .unwrap();
        let at_most_upper = at_most_batch.column(1).as_binary::<i32>();
        assert!(!(at_most_upper.is_null(0) && at_most_upper.is_null(1)));

        // Non-exact (at_least): upper = all_rows, which `into_arrow`
        // encodes as `BlockList(empty)` — row 0 holds the empty-tree bytes,
        // row 1 is null. Round-trip must preserve `is_at_least`.
        let at_least_batch = IndexExprResult::at_least(mask)
            .serialize(&fragments_covered, IndexExprResultWireFormat::TwoMask)
            .unwrap();
        let at_least_upper = at_least_batch.column(1).as_binary::<i32>();
        assert!(!at_least_upper.is_null(0));
        let (round_tripped, _) = IndexExprResult::deserialize(&at_least_batch).unwrap();
        assert!(round_tripped.is_at_least());
        assert!(!round_tripped.is_exact());
    }

    /// A refined `IndexExprResult` (`lower` strictly inside a non-universe
    /// `upper`) has no legacy three-shape encoding. The serializer
    /// must not error in that case — it must degrade to `AtMost(upper)` so
    /// older read planners still see a valid superset and recheck.
    #[test]
    fn test_three_variant_serialize_refined_degrades_to_at_most() {
        use lance_select::{RowAddrMask, RowAddrTreeMap};

        let lower_addrs = RowAddrTreeMap::from_iter(0u64..3);
        let upper_addrs = RowAddrTreeMap::from_iter(0u64..10);
        let refined = IndexExprResult::new(
            RowAddrMask::from_allowed(lower_addrs),
            RowAddrMask::from_allowed(upper_addrs.clone()),
        );
        assert!(!refined.is_exact() && !refined.is_at_most() && !refined.is_at_least());

        let fragments_covered = RoaringBitmap::from_iter([0u32, 1]);

        let batch = refined
            .serialize(&fragments_covered, IndexExprResultWireFormat::ThreeVariant)
            .unwrap();
        assert_eq!(
            batch.schema(),
            *IndexExprResultWireFormat::ThreeVariant.schema()
        );

        // Discriminant 1 == AtMost; the round-tripped result carries the
        // original `upper` as the AtMost mask (empty lower, upper = upper).
        let (round_tripped, round_tripped_frags) = IndexExprResult::deserialize(&batch).unwrap();
        assert!(round_tripped.is_at_most());
        assert_eq!(round_tripped.upper, RowAddrMask::from_allowed(upper_addrs));
        assert_eq!(round_tripped_frags, fragments_covered);
    }

    /// Regression test: when two JSON indices target different paths on the same
    /// column, a query against one path must be routed to its own index instead
    /// of being intercepted by whichever parser was registered first.
    #[test]
    fn test_multi_json_indices_route_by_path() {
        // Build a MultiQueryParser containing two JSON sub-parsers: one for
        // path "$.a" and one for path "$.b".
        let mut multi = MultiQueryParser::single(Box::new(JsonQueryParser::new(
            "$.a".to_string(),
            Box::new(SargableQueryParser::new(
                "json_a_idx".to_string(),
                "Json".to_string(),
                false,
            )),
        )));
        multi.add(Box::new(JsonQueryParser::new(
            "$.b".to_string(),
            Box::new(SargableQueryParser::new(
                "json_b_idx".to_string(),
                "Json".to_string(),
                false,
            )),
        )));

        let index_info = MockIndexInfoProvider::new(vec![(
            "json",
            ColInfo::with_multi(DataType::LargeBinary, Box::new(multi)),
        )]);

        // Query against path "$.b" must hit the "$.b" index.
        let expected_b = IndexedExpression::index_query(
            "json".to_string(),
            "json_b_idx".to_string(),
            "Json".to_string(),
            Arc::new(JsonQuery::new(
                Arc::new(SargableQuery::Equals(ScalarValue::Utf8(Some(
                    "foo".to_string(),
                )))),
                "$.b".to_string(),
            )),
        );
        check(
            &index_info,
            "json_extract(json, '$.b') = 'foo'",
            Some(expected_b),
            false,
        );

        // Query against path "$.a" must hit the "$.a" index.
        let expected_a = IndexedExpression::index_query(
            "json".to_string(),
            "json_a_idx".to_string(),
            "Json".to_string(),
            Arc::new(JsonQuery::new(
                Arc::new(SargableQuery::Equals(ScalarValue::Utf8(Some(
                    "foo".to_string(),
                )))),
                "$.a".to_string(),
            )),
        );
        check(
            &index_info,
            "json_extract(json, '$.a') = 'foo'",
            Some(expected_a),
            false,
        );

        // Query against an unindexed path must not bind to either index.
        check_no_index(&index_info, "json_extract(json, '$.c') = 'foo'");
    }

    #[test]
    fn test_optimize_nested_and_tree() {
        use super::{ScalarIndexExpr, ScalarIndexSearch};
        use crate::scalar::SargableQuery;
        use datafusion_common::ScalarValue;
        use std::ops::Bound;
        use std::sync::Arc;

        // Simulate: AND(AND(fqdn@idx_fqdn, log_time >= X @idx_time), AND(log_time <= Y @idx_time, channel@idx_ch))
        let fqdn_query = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "fqdn".to_string(),
            index_name: "fqdn_idx".to_string(),
            index_type: "".to_string(),
            query: Arc::new(SargableQuery::Equals(ScalarValue::Utf8(Some(
                "eng.bhd.0068".to_string(),
            )))),
            needs_recheck: false,
            fragment_bitmap: None,
        });
        let time_gte = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "log_time".to_string(),
            index_name: "log_time_idx".to_string(),
            index_type: "".to_string(),
            query: Arc::new(SargableQuery::Range(
                Bound::Included(ScalarValue::Int64(Some(100))),
                Bound::Unbounded,
            )),
            needs_recheck: false,
            fragment_bitmap: None,
        });
        let time_lte = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "log_time".to_string(),
            index_name: "log_time_idx".to_string(),
            index_type: "".to_string(),
            query: Arc::new(SargableQuery::Range(
                Bound::Unbounded,
                Bound::Included(ScalarValue::Int64(Some(200))),
            )),
            needs_recheck: false,
            fragment_bitmap: None,
        });
        let channel_query = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "channel".to_string(),
            index_name: "channel_idx".to_string(),
            index_type: "".to_string(),
            query: Arc::new(SargableQuery::Equals(ScalarValue::Utf8(Some(
                "/ados/node/monitor".to_string(),
            )))),
            needs_recheck: false,
            fragment_bitmap: None,
        });

        // Build nested AND: AND(AND(fqdn, time_gte), AND(time_lte, channel))
        let nested = ScalarIndexExpr::And(
            Box::new(ScalarIndexExpr::And(
                Box::new(fqdn_query),
                Box::new(time_gte),
            )),
            Box::new(ScalarIndexExpr::And(
                Box::new(time_lte),
                Box::new(channel_query),
            )),
        );

        // Optimize should merge time_gte + time_lte into a single Range
        let optimized = nested.optimize();

        // The optimized tree should contain a merged Range(Included(100), Included(200))
        // and the other queries as separate leaves
        // Let's verify by collecting leaves
        let mut leaves = Vec::new();
        optimized.collect_and_leaves(&mut leaves, true);

        // Should have 3 leaves: fqdn, merged_time, channel
        assert_eq!(
            leaves.len(),
            3,
            "Expected 3 leaves after merge, got: {:?}",
            leaves
        );

        // Find the merged time query
        let time_query = leaves
            .iter()
            .find(|l| {
                if let ScalarIndexExpr::Query(s) = l {
                    s.index_name == "log_time_idx"
                } else {
                    false
                }
            })
            .expect("Should have a log_time query");

        if let ScalarIndexExpr::Query(s) = time_query {
            let range = s.query.as_any().downcast_ref::<SargableQuery>().unwrap();
            assert_eq!(
                *range,
                SargableQuery::Range(
                    Bound::Included(ScalarValue::Int64(Some(100))),
                    Bound::Included(ScalarValue::Int64(Some(200))),
                ),
                "Range should be merged into closed interval"
            );
        }
    }

    #[test]
    fn test_optimize_no_merge_different_indexes() {
        use super::{ScalarIndexExpr, ScalarIndexSearch};
        use crate::scalar::SargableQuery;
        use datafusion_common::ScalarValue;
        use std::ops::Bound;
        use std::sync::Arc;

        // Two range queries on DIFFERENT indexes should NOT be merged
        let range_a = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "a".to_string(),
            index_name: "idx_a".to_string(),
            index_type: "".to_string(),
            query: Arc::new(SargableQuery::Range(
                Bound::Included(ScalarValue::Int64(Some(10))),
                Bound::Unbounded,
            )),
            needs_recheck: false,
            fragment_bitmap: None,
        });
        let range_b = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "b".to_string(),
            index_name: "idx_b".to_string(),
            index_type: "".to_string(),
            query: Arc::new(SargableQuery::Range(
                Bound::Unbounded,
                Bound::Included(ScalarValue::Int64(Some(100))),
            )),
            needs_recheck: false,
            fragment_bitmap: None,
        });

        let expr = ScalarIndexExpr::And(Box::new(range_a), Box::new(range_b));
        let optimized = expr.optimize();

        // Should remain as two separate leaves (not merged)
        let mut leaves = Vec::new();
        optimized.collect_and_leaves(&mut leaves, true);
        assert_eq!(leaves.len(), 2);
    }

    #[test]
    fn test_optimize_no_merge_non_range() {
        use super::{ScalarIndexExpr, ScalarIndexSearch};
        use crate::scalar::SargableQuery;
        use datafusion_common::ScalarValue;
        use std::ops::Bound;
        use std::sync::Arc;

        // Equals + Range on same index should NOT merge
        let eq_query = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "x".to_string(),
            index_name: "idx_x".to_string(),
            index_type: "".to_string(),
            query: Arc::new(SargableQuery::Equals(ScalarValue::Int64(Some(42)))),
            needs_recheck: false,
            fragment_bitmap: None,
        });
        let range_query = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "x".to_string(),
            index_name: "idx_x".to_string(),
            index_type: "".to_string(),
            query: Arc::new(SargableQuery::Range(
                Bound::Unbounded,
                Bound::Included(ScalarValue::Int64(Some(100))),
            )),
            needs_recheck: false,
            fragment_bitmap: None,
        });

        let expr = ScalarIndexExpr::And(Box::new(eq_query), Box::new(range_query));
        let optimized = expr.optimize();

        let mut leaves = Vec::new();
        optimized.collect_and_leaves(&mut leaves, true);
        assert_eq!(leaves.len(), 2, "Equals + Range should not merge");
    }

    #[test]
    fn test_optimize_exclusive_bounds() {
        use super::{ScalarIndexExpr, ScalarIndexSearch};
        use crate::scalar::SargableQuery;
        use datafusion_common::ScalarValue;
        use std::ops::Bound;
        use std::sync::Arc;

        // x > 10 AND x < 20 → Range(Excluded(10), Excluded(20))
        let gt = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "x".to_string(),
            index_name: "idx_x".to_string(),
            index_type: "".to_string(),
            query: Arc::new(SargableQuery::Range(
                Bound::Excluded(ScalarValue::Int64(Some(10))),
                Bound::Unbounded,
            )),
            needs_recheck: false,
            fragment_bitmap: None,
        });
        let lt = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "x".to_string(),
            index_name: "idx_x".to_string(),
            index_type: "".to_string(),
            query: Arc::new(SargableQuery::Range(
                Bound::Unbounded,
                Bound::Excluded(ScalarValue::Int64(Some(20))),
            )),
            needs_recheck: false,
            fragment_bitmap: None,
        });

        let expr = ScalarIndexExpr::And(Box::new(gt), Box::new(lt));
        let optimized = expr.optimize();

        let mut leaves = Vec::new();
        optimized.collect_and_leaves(&mut leaves, true);
        assert_eq!(leaves.len(), 1);

        if let ScalarIndexExpr::Query(s) = &leaves[0] {
            let range = s.query.as_any().downcast_ref::<SargableQuery>().unwrap();
            assert_eq!(
                *range,
                SargableQuery::Range(
                    Bound::Excluded(ScalarValue::Int64(Some(10))),
                    Bound::Excluded(ScalarValue::Int64(Some(20))),
                )
            );
        } else {
            panic!("Expected a Query leaf");
        }
    }

    #[test]
    fn test_optimize_preserves_or_and_not() {
        use super::{ScalarIndexExpr, ScalarIndexSearch};
        use crate::scalar::SargableQuery;
        use datafusion_common::ScalarValue;
        use std::ops::Bound;
        use std::sync::Arc;

        // AND(OR(a, b), Range_gte, Range_lte)
        // OR node should be preserved, ranges should merge
        let or_node = ScalarIndexExpr::Or(
            Box::new(ScalarIndexExpr::Query(ScalarIndexSearch {
                column: "c".to_string(),
                index_name: "idx_c".to_string(),
                index_type: "".to_string(),
                query: Arc::new(SargableQuery::Equals(ScalarValue::Utf8(Some(
                    "a".to_string(),
                )))),
                needs_recheck: false,
                fragment_bitmap: None,
            })),
            Box::new(ScalarIndexExpr::Query(ScalarIndexSearch {
                column: "c".to_string(),
                index_name: "idx_c".to_string(),
                index_type: "".to_string(),
                query: Arc::new(SargableQuery::Equals(ScalarValue::Utf8(Some(
                    "b".to_string(),
                )))),
                needs_recheck: false,
                fragment_bitmap: None,
            })),
        );
        let range_gte = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "t".to_string(),
            index_name: "idx_t".to_string(),
            index_type: "".to_string(),
            query: Arc::new(SargableQuery::Range(
                Bound::Included(ScalarValue::Int64(Some(5))),
                Bound::Unbounded,
            )),
            needs_recheck: false,
            fragment_bitmap: None,
        });
        let range_lte = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "t".to_string(),
            index_name: "idx_t".to_string(),
            index_type: "".to_string(),
            query: Arc::new(SargableQuery::Range(
                Bound::Unbounded,
                Bound::Included(ScalarValue::Int64(Some(50))),
            )),
            needs_recheck: false,
            fragment_bitmap: None,
        });

        let expr = ScalarIndexExpr::And(
            Box::new(ScalarIndexExpr::And(Box::new(or_node), Box::new(range_gte))),
            Box::new(range_lte),
        );
        let optimized = expr.optimize();

        let mut leaves = Vec::new();
        optimized.collect_and_leaves(&mut leaves, true);
        // Should have 2 leaves: OR node (preserved) + merged range
        assert_eq!(leaves.len(), 2);

        // One leaf should be the merged range
        let merged = leaves
            .iter()
            .find(|l| matches!(l, ScalarIndexExpr::Query(s) if s.index_name == "idx_t"))
            .expect("Should have merged range");

        if let ScalarIndexExpr::Query(s) = merged {
            let range = s.query.as_any().downcast_ref::<SargableQuery>().unwrap();
            assert_eq!(
                *range,
                SargableQuery::Range(
                    Bound::Included(ScalarValue::Int64(Some(5))),
                    Bound::Included(ScalarValue::Int64(Some(50))),
                )
            );
        }

        // Other leaf should be the OR node
        assert!(
            leaves
                .iter()
                .any(|l| matches!(l, ScalarIndexExpr::Or(_, _)))
        );
    }

    #[test]
    fn test_optimize_respects_fragment_coverage_when_merging_ranges() {
        let fragments = RoaringBitmap::from_iter([1, 3, 5]);
        let lower = test_scalar_range_with_metadata(
            Bound::Included(ScalarValue::Int64(Some(1))),
            Bound::Unbounded,
            true,
            Some(fragments.clone()),
        );
        let upper = test_scalar_range_with_metadata(
            Bound::Unbounded,
            Bound::Included(ScalarValue::Int64(Some(99))),
            false,
            Some(fragments.clone()),
        );

        let leaves = collect_test_and_leaves(test_and_terms(vec![lower.clone(), upper]).optimize());
        assert_eq!(leaves.len(), 1);
        assert!(matches!(
            &leaves[0],
            ScalarIndexExpr::Query(search)
                if search.needs_recheck
                    && search.fragment_bitmap.as_ref() == Some(&fragments)
                    && matches!(
                        search.sargable_query(),
                        Some(SargableQuery::Range(
                            Bound::Included(ScalarValue::Int64(Some(1))),
                            Bound::Included(ScalarValue::Int64(Some(99))),
                        ))
                    )
        ));

        let upper_without_coverage = test_scalar_range_with_metadata(
            Bound::Unbounded,
            Bound::Included(ScalarValue::Int64(Some(99))),
            false,
            None,
        );
        for (lhs, rhs) in [
            (lower.clone(), upper_without_coverage.clone()),
            (upper_without_coverage, lower),
        ] {
            let optimized = ScalarIndexExpr::And(Box::new(lhs), Box::new(rhs)).optimize();
            let leaves = collect_test_and_leaves(optimized);
            assert_eq!(leaves.len(), 2);
            assert!(leaves.iter().any(|leaf| {
                matches!(leaf, ScalarIndexExpr::Query(search)
                    if search.needs_recheck
                        && search.fragment_bitmap.as_ref() == Some(&fragments))
            }));
            assert!(leaves.iter().any(|leaf| {
                matches!(leaf, ScalarIndexExpr::Query(search)
                    if !search.needs_recheck && search.fragment_bitmap.is_none())
            }));
        }
    }

    #[test]
    fn test_optimize_merges_recheck_ranges_into_empty_range() {
        let lower = test_scalar_query_with_recheck(
            "x",
            "idx_x",
            SargableQuery::Range(
                Bound::Included(ScalarValue::Int64(Some(200))),
                Bound::Unbounded,
            ),
        );
        let upper = test_scalar_query_with_recheck(
            "x",
            "idx_x",
            SargableQuery::Range(
                Bound::Unbounded,
                Bound::Included(ScalarValue::Int64(Some(100))),
            ),
        );

        let leaves = collect_test_and_leaves(test_and_terms(vec![lower, upper]).optimize());

        assert_eq!(leaves.len(), 1);
        assert!(matches!(
            &leaves[0],
            ScalarIndexExpr::Query(search)
                if search.needs_recheck
                    && matches!(
                        search.sargable_query(),
                        Some(SargableQuery::Range(
                            Bound::Included(ScalarValue::Int64(Some(200))),
                            Bound::Included(ScalarValue::Int64(Some(100))),
                        ))
                    )
        ));
    }

    fn test_scalar_query(column: &str, index_name: &str, query: SargableQuery) -> ScalarIndexExpr {
        ScalarIndexExpr::Query(ScalarIndexSearch {
            column: column.to_string(),
            index_name: index_name.to_string(),
            index_type: "BTree".to_string(),
            query: Arc::new(query),
            needs_recheck: false,
            fragment_bitmap: None,
        })
    }

    fn test_scalar_query_with_recheck(
        column: &str,
        index_name: &str,
        query: SargableQuery,
    ) -> ScalarIndexExpr {
        ScalarIndexExpr::Query(ScalarIndexSearch {
            column: column.to_string(),
            index_name: index_name.to_string(),
            index_type: "ZoneMap".to_string(),
            query: Arc::new(query),
            needs_recheck: true,
            fragment_bitmap: None,
        })
    }

    fn test_scalar_range(
        column: &str,
        index_name: &str,
        lower: Bound<ScalarValue>,
        upper: Bound<ScalarValue>,
    ) -> ScalarIndexExpr {
        test_scalar_query(column, index_name, SargableQuery::Range(lower, upper))
    }

    fn test_scalar_range_with_metadata(
        lower: Bound<ScalarValue>,
        upper: Bound<ScalarValue>,
        needs_recheck: bool,
        fragment_bitmap: Option<RoaringBitmap>,
    ) -> ScalarIndexExpr {
        ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "x".to_string(),
            index_name: "idx_x".to_string(),
            index_type: "ZoneMap".to_string(),
            query: Arc::new(SargableQuery::Range(lower, upper)),
            needs_recheck,
            fragment_bitmap,
        })
    }

    fn test_and_terms(mut terms: Vec<ScalarIndexExpr>) -> ScalarIndexExpr {
        assert!(!terms.is_empty());
        while terms.len() > 1 {
            let mut next = Vec::with_capacity(terms.len().div_ceil(2));
            let mut iter = terms.into_iter();
            while let Some(lhs) = iter.next() {
                if let Some(rhs) = iter.next() {
                    next.push(ScalarIndexExpr::And(Box::new(lhs), Box::new(rhs)));
                } else {
                    next.push(lhs);
                }
            }
            terms = next;
        }
        terms.pop().unwrap()
    }

    fn collect_test_and_leaves(expr: ScalarIndexExpr) -> Vec<ScalarIndexExpr> {
        let mut leaves = Vec::new();
        expr.collect_and_leaves(&mut leaves, true);
        leaves
    }

    fn test_and_depth(expr: &ScalarIndexExpr) -> usize {
        match expr {
            ScalarIndexExpr::And(lhs, rhs) => 1 + test_and_depth(lhs).max(test_and_depth(rhs)),
            ScalarIndexExpr::Or(lhs, rhs) => test_and_depth(lhs).max(test_and_depth(rhs)),
            ScalarIndexExpr::Not(inner) => test_and_depth(inner),
            ScalarIndexExpr::Query(_) => 0,
        }
    }

    fn balanced_depth_bound(mut term_count: usize) -> usize {
        let mut depth = 0;
        while term_count > 1 {
            term_count = term_count.div_ceil(2);
            depth += 1;
        }
        depth
    }

    fn int64_index_info(index_type: &str, needs_recheck: bool) -> MockIndexInfoProvider {
        let parser = |column: &str| {
            ColInfo::new(
                DataType::Int64,
                Box::new(SargableQueryParser::new(
                    format!("{}_idx", column),
                    index_type.to_string(),
                    needs_recheck,
                )),
            )
        };
        MockIndexInfoProvider::new(vec![("x", parser("x")), ("y", parser("y"))])
    }

    fn parse_int64_filter(
        expr: &str,
        index_info: &dyn IndexInformationProvider,
    ) -> IndexedExpression {
        let schema = Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Int64, true),
        ]);
        let df_schema: DFSchema = schema.try_into().unwrap();
        let ctx = get_session_context(&LanceExecutionOptions::default());
        let state = ctx.state();
        let expr = state.create_logical_expr(expr, &df_schema).unwrap();
        apply_scalar_indices(expr, index_info).unwrap()
    }

    fn optimize_parsed_scalar_filter(
        expr: &str,
        index_info: &dyn IndexInformationProvider,
    ) -> Vec<ScalarIndexExpr> {
        let indexed = parse_int64_filter(expr, index_info);
        collect_test_and_leaves(indexed.scalar_query.unwrap().optimize())
    }

    #[test]
    fn test_optimize_does_not_remove_is_not_null_for_recheck_range() {
        let is_not_null = ScalarIndexExpr::Not(Box::new(test_scalar_query(
            "x",
            "idx_x",
            SargableQuery::IsNull(),
        )));
        let mut range = test_scalar_range(
            "x",
            "idx_x",
            Bound::Included(ScalarValue::Int64(Some(10))),
            Bound::Unbounded,
        );
        let ScalarIndexExpr::Query(search) = &mut range else {
            panic!("expected a range query");
        };
        search.needs_recheck = true;

        let leaves = collect_test_and_leaves(test_and_terms(vec![is_not_null, range]).optimize());

        assert_eq!(leaves.len(), 2);
        assert!(leaves.iter().any(|leaf| {
            matches!(
                leaf,
                ScalarIndexExpr::Not(inner)
                    if matches!(inner.as_ref(), ScalarIndexExpr::Query(search)
                        if matches!(search.sargable_query(), Some(SargableQuery::IsNull()))
                            && !search.needs_recheck)
            )
        }));
    }

    #[test]
    fn test_optimize_does_not_remove_is_not_null_across_or() {
        let is_not_null = ScalarIndexExpr::Not(Box::new(test_scalar_query(
            "x",
            "idx_x",
            SargableQuery::IsNull(),
        )));
        let range = test_scalar_range(
            "x",
            "idx_x",
            Bound::Included(ScalarValue::Int64(Some(10))),
            Bound::Unbounded,
        );
        let other = test_scalar_query(
            "y",
            "idx_y",
            SargableQuery::Equals(ScalarValue::Int64(Some(1))),
        );
        let disjunction = ScalarIndexExpr::Or(Box::new(range), Box::new(other));

        let leaves = collect_test_and_leaves(
            ScalarIndexExpr::And(Box::new(is_not_null), Box::new(disjunction)).optimize(),
        );

        assert_eq!(leaves.len(), 2);
        assert!(
            leaves
                .iter()
                .any(|leaf| matches!(leaf, ScalarIndexExpr::Not(_)))
        );
        assert!(
            leaves
                .iter()
                .any(|leaf| matches!(leaf, ScalarIndexExpr::Or(_, _)))
        );
    }

    #[test]
    fn test_optimize_does_not_remove_is_not_null_for_different_index() {
        let is_not_null = ScalarIndexExpr::Not(Box::new(test_scalar_query(
            "x",
            "idx_x",
            SargableQuery::IsNull(),
        )));
        let range = test_scalar_range(
            "x",
            "idx_x_other",
            Bound::Included(ScalarValue::Int64(Some(10))),
            Bound::Unbounded,
        );

        let leaves = collect_test_and_leaves(
            ScalarIndexExpr::And(Box::new(is_not_null), Box::new(range)).optimize(),
        );

        assert_eq!(leaves.len(), 2);
        assert!(
            leaves
                .iter()
                .any(|leaf| matches!(leaf, ScalarIndexExpr::Not(_)))
        );
    }

    #[test]
    fn test_optimize_does_not_merge_different_index_types() {
        let range = |index_type: &str, lower, upper| {
            ScalarIndexExpr::Query(ScalarIndexSearch {
                column: "x".to_string(),
                index_name: "idx_x".to_string(),
                index_type: index_type.to_string(),
                query: Arc::new(SargableQuery::Range(lower, upper)),
                needs_recheck: false,
                fragment_bitmap: None,
            })
        };
        let btree = range(
            "BTree",
            Bound::Included(ScalarValue::Int64(Some(10))),
            Bound::Unbounded,
        );
        let zone_map = range(
            "ZoneMap",
            Bound::Unbounded,
            Bound::Included(ScalarValue::Int64(Some(20))),
        );

        let leaves = collect_test_and_leaves(test_and_terms(vec![btree, zone_map]).optimize());

        assert_eq!(leaves.len(), 2);
    }

    #[test]
    fn test_optimize_parser_exact_removes_is_not_null_and_merges_ranges() {
        let index_info = int64_index_info("BTree", false);

        let leaves = optimize_parsed_scalar_filter(
            "x IS NOT NULL AND y = 1 AND x >= 10 AND x <= 20",
            &index_info,
        );

        assert_eq!(leaves.len(), 2);
        assert!(leaves.iter().any(|leaf| {
            matches!(
                leaf,
                ScalarIndexExpr::Query(search)
                    if search.column == "x"
                        && matches!(
                            search.sargable_query(),
                            Some(SargableQuery::Range(
                                Bound::Included(ScalarValue::Int64(Some(10))),
                                Bound::Included(ScalarValue::Int64(Some(20))),
                            ))
                        )
            )
        }));
        assert!(leaves.iter().any(|leaf| {
            matches!(
                leaf,
                ScalarIndexExpr::Query(search)
                    if search.column == "y"
                        && matches!(
                            search.sargable_query(),
                            Some(SargableQuery::Equals(ScalarValue::Int64(Some(1))))
                        )
            )
        }));
        assert!(
            !leaves
                .iter()
                .any(|leaf| matches!(leaf, ScalarIndexExpr::Not(_)))
        );
    }

    #[test]
    fn test_optimize_parser_preserves_standalone_null_checks() {
        let index_info = int64_index_info("BTree", false);

        let is_not_null = optimize_parsed_scalar_filter("x IS NOT NULL", &index_info);
        assert_eq!(is_not_null.len(), 1);
        assert!(matches!(&is_not_null[0], ScalarIndexExpr::Not(_)));

        let is_null = optimize_parsed_scalar_filter("x IS NULL", &index_info);
        assert_eq!(is_null.len(), 1);
        assert!(matches!(
            &is_null[0],
            ScalarIndexExpr::Query(search)
                if matches!(search.sargable_query(), Some(SargableQuery::IsNull()))
        ));
    }

    #[test]
    fn test_optimize_parser_does_not_remove_is_not_null_for_different_column() {
        let index_info = int64_index_info("BTree", false);

        let leaves = optimize_parsed_scalar_filter("x IS NOT NULL AND y >= 10", &index_info);

        assert_eq!(leaves.len(), 2);
        assert!(leaves.iter().any(|leaf| {
            matches!(
                leaf,
                ScalarIndexExpr::Not(inner)
                    if matches!(inner.as_ref(), ScalarIndexExpr::Query(search)
                        if search.column == "x"
                            && matches!(search.sargable_query(), Some(SargableQuery::IsNull())))
            )
        }));
    }

    #[test]
    fn test_optimize_parser_handles_in_list_null_semantics() {
        let index_info = int64_index_info("BTree", false);

        let leaves = optimize_parsed_scalar_filter("x IS NOT NULL AND x IN (1, 2)", &index_info);
        assert_eq!(leaves.len(), 1);
        assert!(matches!(
            &leaves[0],
            ScalarIndexExpr::Query(search)
                if matches!(search.sargable_query(), Some(SargableQuery::IsIn(values)) if values.len() == 2)
        ));

        let indexed = parse_int64_filter("x IS NOT NULL AND x IN (1, NULL)", &index_info);
        assert!(indexed.refine_expr.is_some());
        let leaves = collect_test_and_leaves(indexed.scalar_query.unwrap().optimize());
        assert_eq!(leaves.len(), 1);
        assert!(matches!(&leaves[0], ScalarIndexExpr::Not(_)));
    }

    #[test]
    fn test_optimize_parser_removes_is_not_null_from_not_equal() {
        let index_info = int64_index_info("BTree", false);

        let leaves = optimize_parsed_scalar_filter("x IS NOT NULL AND x != 5", &index_info);

        assert_eq!(leaves.len(), 1);
        assert!(matches!(
            &leaves[0],
            ScalarIndexExpr::Not(inner)
                if matches!(inner.as_ref(), ScalarIndexExpr::Query(search)
                    if matches!(search.sargable_query(), Some(SargableQuery::Equals(value))
                        if *value == ScalarValue::Int64(Some(5))))
        ));
    }

    #[test]
    fn test_optimize_parser_merges_recheck_ranges() {
        let index_info = int64_index_info("ZoneMap", true);

        let leaves = optimize_parsed_scalar_filter("x >= 10 AND y = 1 AND x <= 20", &index_info);

        assert_eq!(leaves.len(), 2);
        assert!(leaves.iter().any(|leaf| {
            matches!(
                leaf,
                ScalarIndexExpr::Query(search)
                    if search.column == "x"
                        && search.needs_recheck
                        && matches!(
                            search.sargable_query(),
                            Some(SargableQuery::Range(
                                Bound::Included(ScalarValue::Int64(Some(10))),
                                Bound::Included(ScalarValue::Int64(Some(20))),
                            ))
                        )
            )
        }));
    }

    #[test]
    fn test_optimize_parser_keeps_recheck_is_not_null_as_refine() {
        let index_info = int64_index_info("ZoneMap", true);

        let indexed = parse_int64_filter("x IS NOT NULL AND x >= 10", &index_info);

        assert!(indexed.scalar_query.is_some());
        assert!(matches!(
            indexed.refine_expr.as_ref(),
            Some(Expr::IsNotNull(expr))
                if matches!(expr.as_ref(), Expr::Column(column) if column.name == "x")
        ));
        let leaves = collect_test_and_leaves(indexed.scalar_query.unwrap().optimize());
        assert_eq!(leaves.len(), 1);
        assert!(matches!(
            &leaves[0],
            ScalarIndexExpr::Query(search)
                if search.needs_recheck
                    && matches!(search.sargable_query(), Some(SargableQuery::Range(_, _)))
        ));
    }

    #[test]
    fn test_optimize_preserves_balanced_depth_for_unmerged_terms() {
        let term_count = 2048;
        let terms = (0..term_count)
            .map(|value| {
                test_scalar_query(
                    "x",
                    "idx_x",
                    SargableQuery::Equals(ScalarValue::Int64(Some(value))),
                )
            })
            .collect::<Vec<_>>();

        let optimized = test_and_terms(terms).optimize();

        assert_eq!(
            collect_test_and_leaves(optimized.clone()).len(),
            term_count as usize
        );
        assert!(
            test_and_depth(&optimized) <= balanced_depth_bound(term_count as usize),
            "optimized AND depth should stay balanced, got {} for {} terms",
            test_and_depth(&optimized),
            term_count
        );
    }

    #[test]
    fn test_optimize_merges_ranges_inside_not() {
        let lower = test_scalar_range(
            "x",
            "idx_x",
            Bound::Included(ScalarValue::Int64(Some(10))),
            Bound::Unbounded,
        );
        let upper = test_scalar_range(
            "x",
            "idx_x",
            Bound::Unbounded,
            Bound::Included(ScalarValue::Int64(Some(20))),
        );
        let sibling = test_scalar_query(
            "y",
            "idx_y",
            SargableQuery::Equals(ScalarValue::Int64(Some(1))),
        );

        let nested_not = ScalarIndexExpr::Not(Box::new(test_and_terms(vec![lower, upper])));
        let optimized = ScalarIndexExpr::And(Box::new(nested_not), Box::new(sibling)).optimize();

        let ScalarIndexExpr::And(nested_not, _) = optimized else {
            panic!("expected outer AND expression");
        };
        let ScalarIndexExpr::Not(inner) = *nested_not else {
            panic!("expected NOT expression");
        };
        let leaves = collect_test_and_leaves(*inner);

        assert_eq!(leaves.len(), 1);
        assert!(matches!(
            &leaves[0],
            ScalarIndexExpr::Query(search)
                if matches!(
                    search.sargable_query(),
                    Some(SargableQuery::Range(
                        Bound::Included(ScalarValue::Int64(Some(10))),
                        Bound::Included(ScalarValue::Int64(Some(20))),
                    ))
                )
        ));
    }

    #[test]
    fn test_optimize_does_not_remove_is_not_null_inside_not() {
        let is_not_null = ScalarIndexExpr::Not(Box::new(test_scalar_query(
            "x",
            "idx_x",
            SargableQuery::IsNull(),
        )));
        let range = test_scalar_range(
            "x",
            "idx_x",
            Bound::Included(ScalarValue::Int64(Some(10))),
            Bound::Unbounded,
        );
        let guarded_range = test_and_terms(vec![is_not_null, range]);
        let alternative = test_scalar_query(
            "y",
            "idx_y",
            SargableQuery::Equals(ScalarValue::Int64(Some(1))),
        );
        let or = ScalarIndexExpr::Or(Box::new(guarded_range), Box::new(alternative));
        let sibling = test_scalar_query(
            "z",
            "idx_z",
            SargableQuery::Equals(ScalarValue::Int64(Some(2))),
        );
        let outer_sibling = test_scalar_query(
            "w",
            "idx_w",
            SargableQuery::Equals(ScalarValue::Int64(Some(3))),
        );

        let nested_not = ScalarIndexExpr::Not(Box::new(test_and_terms(vec![or, sibling])));
        let optimized =
            ScalarIndexExpr::And(Box::new(nested_not), Box::new(outer_sibling)).optimize();

        let ScalarIndexExpr::And(nested_not, _) = optimized else {
            panic!("expected outer AND expression");
        };
        let ScalarIndexExpr::Not(inner) = *nested_not else {
            panic!("expected NOT expression");
        };
        let ScalarIndexExpr::And(or, _) = *inner else {
            panic!("expected AND expression");
        };
        let ScalarIndexExpr::Or(lhs, _) = *or else {
            panic!("expected OR expression");
        };
        let leaves = collect_test_and_leaves(*lhs);

        assert_eq!(leaves.len(), 2);
        assert!(leaves.iter().any(|leaf| {
            matches!(
                leaf,
                ScalarIndexExpr::Not(inner)
                    if matches!(inner.as_ref(), ScalarIndexExpr::Query(search)
                        if matches!(search.sargable_query(), Some(SargableQuery::IsNull())))
            )
        }));
    }

    #[test]
    fn test_optimize_single_query_passthrough() {
        use super::{ScalarIndexExpr, ScalarIndexSearch};
        use crate::scalar::SargableQuery;
        use datafusion_common::ScalarValue;
        use std::ops::Bound;
        use std::sync::Arc;

        // A single query (not in AND) should pass through unchanged
        let single = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "x".to_string(),
            index_name: "idx_x".to_string(),
            index_type: "".to_string(),
            query: Arc::new(SargableQuery::Range(
                Bound::Included(ScalarValue::Int64(Some(1))),
                Bound::Unbounded,
            )),
            needs_recheck: false,
            fragment_bitmap: None,
        });

        let optimized = single.clone().optimize();
        assert_eq!(optimized, single);
    }

    #[test]
    fn test_optimize_complex_nested_cases() {
        use super::{ScalarIndexExpr, ScalarIndexSearch};
        use crate::scalar::SargableQuery;
        use datafusion_common::ScalarValue;
        use std::ops::Bound;
        use std::sync::Arc;

        let make_range =
            |col: &str, idx: &str, low: Bound<ScalarValue>, high: Bound<ScalarValue>| {
                ScalarIndexExpr::Query(ScalarIndexSearch {
                    column: col.to_string(),
                    index_name: idx.to_string(),
                    index_type: "".to_string(),
                    query: Arc::new(SargableQuery::Range(low, high)),
                    needs_recheck: false,
                    fragment_bitmap: None,
                })
            };
        let make_eq = |col: &str, idx: &str, val: ScalarValue| {
            ScalarIndexExpr::Query(ScalarIndexSearch {
                column: col.to_string(),
                index_name: idx.to_string(),
                index_type: "".to_string(),
                query: Arc::new(SargableQuery::Equals(val)),
                needs_recheck: false,
                fragment_bitmap: None,
            })
        };

        // Case 1: Multiple ranges on same index should all merge
        // x >= 10 AND x <= 200 AND x >= 50 AND x <= 100 → Range(50, 100)
        {
            let expr = ScalarIndexExpr::And(
                Box::new(ScalarIndexExpr::And(
                    Box::new(make_range(
                        "x",
                        "idx_x",
                        Bound::Included(ScalarValue::Int64(Some(10))),
                        Bound::Unbounded,
                    )),
                    Box::new(make_range(
                        "x",
                        "idx_x",
                        Bound::Unbounded,
                        Bound::Included(ScalarValue::Int64(Some(200))),
                    )),
                )),
                Box::new(ScalarIndexExpr::And(
                    Box::new(make_range(
                        "x",
                        "idx_x",
                        Bound::Included(ScalarValue::Int64(Some(50))),
                        Bound::Unbounded,
                    )),
                    Box::new(make_range(
                        "x",
                        "idx_x",
                        Bound::Unbounded,
                        Bound::Included(ScalarValue::Int64(Some(100))),
                    )),
                )),
            );

            let optimized = expr.optimize();
            let mut leaves = Vec::new();
            optimized.collect_and_leaves(&mut leaves, true);
            assert_eq!(leaves.len(), 1, "All 4 ranges should merge into 1");

            if let ScalarIndexExpr::Query(s) = &leaves[0] {
                let range = s.query.as_any().downcast_ref::<SargableQuery>().unwrap();
                assert_eq!(
                    *range,
                    SargableQuery::Range(
                        Bound::Included(ScalarValue::Int64(Some(50))),
                        Bound::Included(ScalarValue::Int64(Some(100))),
                    )
                );
            } else {
                panic!("Expected merged Query");
            }
        }

        // Case 2: NOT(range) should NOT be merged with sibling range
        // AND(NOT(x >= 10), x <= 20) → stays as 2 leaves
        {
            let not_node = ScalarIndexExpr::Not(Box::new(make_range(
                "x",
                "idx_x",
                Bound::Included(ScalarValue::Int64(Some(10))),
                Bound::Unbounded,
            )));
            let range_lte = make_range(
                "x",
                "idx_x",
                Bound::Unbounded,
                Bound::Included(ScalarValue::Int64(Some(20))),
            );

            let expr = ScalarIndexExpr::And(Box::new(not_node), Box::new(range_lte));
            let optimized = expr.optimize();

            let mut leaves = Vec::new();
            optimized.collect_and_leaves(&mut leaves, true);
            assert_eq!(
                leaves.len(),
                2,
                "NOT(range) should not merge with sibling range"
            );
            assert!(leaves.iter().any(|l| matches!(l, ScalarIndexExpr::Not(_))));
        }

        // Case 3: Ranges inside OR branches get optimized independently
        // OR(AND(x >= 10, x <= 20), AND(x >= 30, x <= 40))
        {
            let branch1 = ScalarIndexExpr::And(
                Box::new(make_range(
                    "x",
                    "idx_x",
                    Bound::Included(ScalarValue::Int64(Some(10))),
                    Bound::Unbounded,
                )),
                Box::new(make_range(
                    "x",
                    "idx_x",
                    Bound::Unbounded,
                    Bound::Included(ScalarValue::Int64(Some(20))),
                )),
            );
            let branch2 = ScalarIndexExpr::And(
                Box::new(make_range(
                    "x",
                    "idx_x",
                    Bound::Included(ScalarValue::Int64(Some(30))),
                    Bound::Unbounded,
                )),
                Box::new(make_range(
                    "x",
                    "idx_x",
                    Bound::Unbounded,
                    Bound::Included(ScalarValue::Int64(Some(40))),
                )),
            );

            let expr = ScalarIndexExpr::Or(Box::new(branch1), Box::new(branch2));
            let optimized = expr.optimize();

            // Top level should still be OR
            if let ScalarIndexExpr::Or(lhs, rhs) = &optimized {
                // Each branch should be a single merged range
                let mut left_leaves = Vec::new();
                lhs.clone().collect_and_leaves(&mut left_leaves, true);
                assert_eq!(
                    left_leaves.len(),
                    1,
                    "Left OR branch should have merged range"
                );
                if let ScalarIndexExpr::Query(s) = &left_leaves[0] {
                    let range = s.query.as_any().downcast_ref::<SargableQuery>().unwrap();
                    assert_eq!(
                        *range,
                        SargableQuery::Range(
                            Bound::Included(ScalarValue::Int64(Some(10))),
                            Bound::Included(ScalarValue::Int64(Some(20))),
                        )
                    );
                }

                let mut right_leaves = Vec::new();
                rhs.clone().collect_and_leaves(&mut right_leaves, true);
                assert_eq!(
                    right_leaves.len(),
                    1,
                    "Right OR branch should have merged range"
                );
                if let ScalarIndexExpr::Query(s) = &right_leaves[0] {
                    let range = s.query.as_any().downcast_ref::<SargableQuery>().unwrap();
                    assert_eq!(
                        *range,
                        SargableQuery::Range(
                            Bound::Included(ScalarValue::Int64(Some(30))),
                            Bound::Included(ScalarValue::Int64(Some(40))),
                        )
                    );
                }
            } else {
                panic!("Expected OR at top level");
            }
        }

        // Case 4: Multiple indexes, each with its own range pair
        // AND(a >= 1, a <= 10, b >= 100, b <= 200)
        // → merged into: AND(a in [1,10], b in [100,200])
        {
            let expr = ScalarIndexExpr::And(
                Box::new(ScalarIndexExpr::And(
                    Box::new(make_range(
                        "a",
                        "idx_a",
                        Bound::Included(ScalarValue::Int64(Some(1))),
                        Bound::Unbounded,
                    )),
                    Box::new(make_range(
                        "a",
                        "idx_a",
                        Bound::Unbounded,
                        Bound::Included(ScalarValue::Int64(Some(10))),
                    )),
                )),
                Box::new(ScalarIndexExpr::And(
                    Box::new(make_range(
                        "b",
                        "idx_b",
                        Bound::Included(ScalarValue::Int64(Some(100))),
                        Bound::Unbounded,
                    )),
                    Box::new(make_range(
                        "b",
                        "idx_b",
                        Bound::Unbounded,
                        Bound::Included(ScalarValue::Int64(Some(200))),
                    )),
                )),
            );

            let optimized = expr.optimize();
            let mut leaves = Vec::new();
            optimized.collect_and_leaves(&mut leaves, true);
            assert_eq!(
                leaves.len(),
                2,
                "Two indexes should each merge independently"
            );

            let a_leaf = leaves
                .iter()
                .find(|l| matches!(l, ScalarIndexExpr::Query(s) if s.index_name == "idx_a"))
                .expect("Should have idx_a");
            if let ScalarIndexExpr::Query(s) = a_leaf {
                let range = s.query.as_any().downcast_ref::<SargableQuery>().unwrap();
                assert_eq!(
                    *range,
                    SargableQuery::Range(
                        Bound::Included(ScalarValue::Int64(Some(1))),
                        Bound::Included(ScalarValue::Int64(Some(10))),
                    )
                );
            }

            let b_leaf = leaves
                .iter()
                .find(|l| matches!(l, ScalarIndexExpr::Query(s) if s.index_name == "idx_b"))
                .expect("Should have idx_b");
            if let ScalarIndexExpr::Query(s) = b_leaf {
                let range = s.query.as_any().downcast_ref::<SargableQuery>().unwrap();
                assert_eq!(
                    *range,
                    SargableQuery::Range(
                        Bound::Included(ScalarValue::Int64(Some(100))),
                        Bound::Included(ScalarValue::Int64(Some(200))),
                    )
                );
            }
        }

        // Case 5: Mix of Equals and Range on different indexes
        // AND(fqdn = 'x', time >= 100, time <= 200, channel = 'y')
        // → AND(fqdn = 'x', time in [100,200], channel = 'y')
        {
            let expr = ScalarIndexExpr::And(
                Box::new(ScalarIndexExpr::And(
                    Box::new(make_eq(
                        "fqdn",
                        "fqdn_idx",
                        ScalarValue::Utf8(Some("x".to_string())),
                    )),
                    Box::new(make_range(
                        "time",
                        "time_idx",
                        Bound::Included(ScalarValue::Int64(Some(100))),
                        Bound::Unbounded,
                    )),
                )),
                Box::new(ScalarIndexExpr::And(
                    Box::new(make_range(
                        "time",
                        "time_idx",
                        Bound::Unbounded,
                        Bound::Included(ScalarValue::Int64(Some(200))),
                    )),
                    Box::new(make_eq(
                        "channel",
                        "ch_idx",
                        ScalarValue::Utf8(Some("y".to_string())),
                    )),
                )),
            );

            let optimized = expr.optimize();
            let mut leaves = Vec::new();
            optimized.collect_and_leaves(&mut leaves, true);
            assert_eq!(leaves.len(), 3, "fqdn + merged_time + channel = 3 leaves");

            let time_leaf = leaves
                .iter()
                .find(|l| matches!(l, ScalarIndexExpr::Query(s) if s.index_name == "time_idx"))
                .expect("Should have time query");
            if let ScalarIndexExpr::Query(s) = time_leaf {
                let range = s.query.as_any().downcast_ref::<SargableQuery>().unwrap();
                assert_eq!(
                    *range,
                    SargableQuery::Range(
                        Bound::Included(ScalarValue::Int64(Some(100))),
                        Bound::Included(ScalarValue::Int64(Some(200))),
                    )
                );
            }
        }

        // Case 6: Overlapping closed ranges → takes intersection
        // Range(10, 50) AND Range(30, 80) → Range(30, 50)
        {
            let expr = ScalarIndexExpr::And(
                Box::new(make_range(
                    "x",
                    "idx_x",
                    Bound::Included(ScalarValue::Int64(Some(10))),
                    Bound::Included(ScalarValue::Int64(Some(50))),
                )),
                Box::new(make_range(
                    "x",
                    "idx_x",
                    Bound::Included(ScalarValue::Int64(Some(30))),
                    Bound::Included(ScalarValue::Int64(Some(80))),
                )),
            );

            let optimized = expr.optimize();
            let mut leaves = Vec::new();
            optimized.collect_and_leaves(&mut leaves, true);
            assert_eq!(leaves.len(), 1);
            if let ScalarIndexExpr::Query(s) = &leaves[0] {
                let range = s.query.as_any().downcast_ref::<SargableQuery>().unwrap();
                assert_eq!(
                    *range,
                    SargableQuery::Range(
                        Bound::Included(ScalarValue::Int64(Some(30))),
                        Bound::Included(ScalarValue::Int64(Some(50))),
                    )
                );
            }
        }

        // Case 7: Mixed Included/Excluded on same value
        // x >= 5 AND x > 5 → Excluded(5) (stricter)
        {
            let expr = ScalarIndexExpr::And(
                Box::new(make_range(
                    "x",
                    "idx_x",
                    Bound::Included(ScalarValue::Int64(Some(5))),
                    Bound::Unbounded,
                )),
                Box::new(make_range(
                    "x",
                    "idx_x",
                    Bound::Excluded(ScalarValue::Int64(Some(5))),
                    Bound::Unbounded,
                )),
            );

            let optimized = expr.optimize();
            let mut leaves = Vec::new();
            optimized.collect_and_leaves(&mut leaves, true);
            assert_eq!(leaves.len(), 1);
            if let ScalarIndexExpr::Query(s) = &leaves[0] {
                let range = s.query.as_any().downcast_ref::<SargableQuery>().unwrap();
                assert_eq!(
                    *range,
                    SargableQuery::Range(
                        Bound::Excluded(ScalarValue::Int64(Some(5))),
                        Bound::Unbounded,
                    )
                );
            }
        }

        // Case 8: Empty range (lower > upper) — still valid, just produces empty results
        // x >= 200 AND x <= 100 → Range(Included(200), Included(100))
        // BTree search will find 0 pages matching this, which is correct.
        {
            let expr = ScalarIndexExpr::And(
                Box::new(make_range(
                    "x",
                    "idx_x",
                    Bound::Included(ScalarValue::Int64(Some(200))),
                    Bound::Unbounded,
                )),
                Box::new(make_range(
                    "x",
                    "idx_x",
                    Bound::Unbounded,
                    Bound::Included(ScalarValue::Int64(Some(100))),
                )),
            );

            let optimized = expr.optimize();
            let mut leaves = Vec::new();
            optimized.collect_and_leaves(&mut leaves, true);
            assert_eq!(leaves.len(), 1);
            if let ScalarIndexExpr::Query(s) = &leaves[0] {
                let range = s.query.as_any().downcast_ref::<SargableQuery>().unwrap();
                // Merged: lower=Included(200), upper=Included(100) — empty range, but valid
                assert_eq!(
                    *range,
                    SargableQuery::Range(
                        Bound::Included(ScalarValue::Int64(Some(200))),
                        Bound::Included(ScalarValue::Int64(Some(100))),
                    )
                );
            }
        }
    }
}
