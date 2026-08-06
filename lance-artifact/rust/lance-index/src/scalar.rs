// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Scalar indices for metadata search & filtering

use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow_array::ListArray;
use arrow_schema::Field;
use datafusion::functions::regex::regexplike::RegexpLikeFunc;
use datafusion::functions::string::contains::ContainsFunc;
use datafusion::functions_nested::array_has;
use datafusion_common::{Column, scalar::ScalarValue};
use std::collections::HashSet;
use std::{any::Any, ops::Bound, sync::Arc};

use datafusion_expr::{
    Expr,
    expr::{Like, ScalarFunction},
};
use inverted::query::{FtsQuery, FtsQueryNode, FtsSearchParams, MatchQuery, fill_fts_query_column};
use lance_core::Result;

use lance_datafusion::udf::CONTAINS_TOKENS_UDF;

use crate::IndexParams;
pub use crate::metrics::MetricsCollector;
pub use lance_index_core::scalar::{
    AnyQuery, BuiltinIndexType, CreatedIndex, IndexFile, IndexReader, IndexStore, IndexWriter,
    LANCE_SCALAR_INDEX, OldIndexDataFilter, RowIdRemapper, ScalarIndex, ScalarIndexParams,
    SearchResult, TrainingCriteria, TrainingOrdering, UpdateCriteria,
};

pub mod bitmap;
pub mod bloomfilter;
pub mod btree;
pub mod expression;
pub mod fmindex;
pub mod inverted;
pub mod json;
pub mod label_list;
pub mod lance_format;
pub mod ngram;
pub mod registry;
#[cfg(feature = "geo")]
pub mod rtree;
pub mod seed;
pub mod zoned;
pub mod zonemap;

pub use inverted::tokenizer::InvertedIndexParams;

/// Convert a `Vec<`[`lance_index_core::scalar::IndexFile`]`>` to a
/// `Vec<`[`lance_table::format::IndexFile`]`>`.
///
/// These two structs have identical fields; this helper bridges the crate
/// boundary without relying on orphan-rule–violating `From` impls.
pub fn index_files_to_table(
    files: Vec<lance_index_core::scalar::IndexFile>,
) -> Vec<lance_table::format::IndexFile> {
    files
        .into_iter()
        .map(|f| lance_table::format::IndexFile {
            path: f.path,
            size_bytes: f.size_bytes,
        })
        .collect()
}

/// Convert a `Vec<`[`lance_table::format::IndexFile`]`>` to a
/// `Vec<`[`lance_index_core::scalar::IndexFile`]`>`.
///
/// These two structs have identical fields; this helper bridges the crate
/// boundary without relying on orphan-rule–violating `From` impls.
pub fn table_files_to_index(
    files: Vec<lance_table::format::IndexFile>,
) -> Vec<lance_index_core::scalar::IndexFile> {
    files
        .into_iter()
        .map(|f| lance_index_core::scalar::IndexFile {
            path: f.path,
            size_bytes: f.size_bytes,
        })
        .collect()
}

impl IndexParams for InvertedIndexParams {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn index_name(&self) -> &str {
        "INVERTED"
    }
}

/// A full text search query
#[derive(Debug, Clone, PartialEq)]
pub struct FullTextSearchQuery {
    pub query: FtsQuery,

    /// The maximum number of results to return
    pub limit: Option<i64>,

    /// The wand factor to use for ranking
    /// if None, use the default value of 1.0
    /// Increasing this value will reduce the recall and improve the performance
    /// 1.0 is the value that would give the best performance without recall loss
    pub wand_factor: Option<f32>,
}

impl FullTextSearchQuery {
    /// Create a new terms query
    pub fn new(query: String) -> Self {
        let query = MatchQuery::new(query).into();
        Self {
            query,
            limit: None,
            wand_factor: None,
        }
    }

    /// Create a new fuzzy query
    pub fn new_fuzzy(term: String, max_distance: Option<u32>) -> Self {
        let query = MatchQuery::new(term).with_fuzziness(max_distance).into();
        Self {
            query,
            limit: None,
            wand_factor: None,
        }
    }

    /// Create a new compound query
    pub fn new_query(query: FtsQuery) -> Self {
        Self {
            query,
            limit: None,
            wand_factor: None,
        }
    }

    /// Set the column to search over
    /// This is available for only MatchQuery and PhraseQuery
    pub fn with_column(mut self, column: String) -> Result<Self> {
        self.query = fill_fts_query_column(&self.query, &[column], true)?;
        Ok(self)
    }

    /// Set the column to search over
    /// This is available for only MatchQuery
    pub fn with_columns(mut self, columns: &[String]) -> Result<Self> {
        self.query = fill_fts_query_column(&self.query, columns, true)?;
        Ok(self)
    }

    /// limit the number of results to return
    /// if None, return all results
    pub fn limit(mut self, limit: Option<i64>) -> Self {
        self.limit = limit;
        self
    }

    pub fn wand_factor(mut self, wand_factor: Option<f32>) -> Self {
        self.wand_factor = wand_factor;
        self
    }

    pub fn columns(&self) -> HashSet<String> {
        self.query.columns()
    }

    pub fn params(&self) -> FtsSearchParams {
        FtsSearchParams::new()
            .with_limit(self.limit.map(|limit| limit as usize))
            .with_wand_factor(self.wand_factor.unwrap_or(1.0))
    }
}

/// A query that a basic scalar index (e.g. btree / bitmap) can satisfy
///
/// This is a subset of expression operators that is often referred to as the
/// "sargable" operators
///
/// Note that negation is not included.  Negation should be applied later.  For
/// example, to invert an equality query (e.g. all rows where the value is not 7)
/// you can grab all rows where the value = 7 and then do an inverted take (or use
/// a block list instead of an allow list for prefiltering)
#[derive(Debug, Clone, PartialEq)]
pub enum SargableQuery {
    /// Retrieve all row ids where the value is in the given [min, max) range
    Range(Bound<ScalarValue>, Bound<ScalarValue>),
    /// Retrieve all row ids where the value is in the given set of values
    IsIn(Vec<ScalarValue>),
    /// Retrieve all row ids where the value is exactly the given value
    Equals(ScalarValue),
    /// Retrieve all row ids where the value matches the given full text search query
    FullTextSearch(FullTextSearchQuery),
    /// Retrieve all row ids where the value is null
    IsNull(),
    /// Retrieve all row ids where the value matches LIKE 'prefix%' pattern
    /// This is used for both explicit LIKE expressions and starts_with() function calls
    LikePrefix(ScalarValue),
}

/// Escape the LIKE metacharacters (`\`, `%`, `_`) in a literal string so it can be
/// embedded in a LIKE pattern and matched literally (paired with `ESCAPE '\'`).
fn escape_like_pattern(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        if matches!(c, '\\' | '%' | '_') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

impl AnyQuery for SargableQuery {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn format(&self, col: &str) -> String {
        match self {
            Self::Range(lower, upper) => match (lower, upper) {
                (Bound::Unbounded, Bound::Unbounded) => "true".to_string(),
                (Bound::Unbounded, Bound::Included(rhs)) => format!("{} <= {}", col, rhs),
                (Bound::Unbounded, Bound::Excluded(rhs)) => format!("{} < {}", col, rhs),
                (Bound::Included(lhs), Bound::Unbounded) => format!("{} >= {}", col, lhs),
                (Bound::Included(lhs), Bound::Included(rhs)) => {
                    format!("{} >= {} && {} <= {}", col, lhs, col, rhs)
                }
                (Bound::Included(lhs), Bound::Excluded(rhs)) => {
                    format!("{} >= {} && {} < {}", col, lhs, col, rhs)
                }
                (Bound::Excluded(lhs), Bound::Unbounded) => format!("{} > {}", col, lhs),
                (Bound::Excluded(lhs), Bound::Included(rhs)) => {
                    format!("{} > {} && {} <= {}", col, lhs, col, rhs)
                }
                (Bound::Excluded(lhs), Bound::Excluded(rhs)) => {
                    format!("{} > {} && {} < {}", col, lhs, col, rhs)
                }
            },
            Self::IsIn(values) => {
                format!(
                    "{} IN [{}]",
                    col,
                    values
                        .iter()
                        .map(|val| val.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            Self::FullTextSearch(query) => {
                format!("fts({})", query.query)
            }
            Self::IsNull() => {
                format!("{} IS NULL", col)
            }
            Self::Equals(val) => {
                format!("{} = {}", col, val)
            }
            Self::LikePrefix(prefix) => {
                format!("{} LIKE '{}%'", col, prefix)
            }
        }
    }

    fn to_expr(&self, col: String) -> Expr {
        let col_expr = Expr::Column(Column::new_unqualified(col));
        match self {
            Self::Range(lower, upper) => match (lower, upper) {
                (Bound::Unbounded, Bound::Unbounded) => {
                    Expr::Literal(ScalarValue::Boolean(Some(true)), None)
                }
                (Bound::Unbounded, Bound::Included(rhs)) => {
                    col_expr.lt_eq(Expr::Literal(rhs.clone(), None))
                }
                (Bound::Unbounded, Bound::Excluded(rhs)) => {
                    col_expr.lt(Expr::Literal(rhs.clone(), None))
                }
                (Bound::Included(lhs), Bound::Unbounded) => {
                    col_expr.gt_eq(Expr::Literal(lhs.clone(), None))
                }
                (Bound::Included(lhs), Bound::Included(rhs)) => col_expr.between(
                    Expr::Literal(lhs.clone(), None),
                    Expr::Literal(rhs.clone(), None),
                ),
                (Bound::Included(lhs), Bound::Excluded(rhs)) => col_expr
                    .clone()
                    .gt_eq(Expr::Literal(lhs.clone(), None))
                    .and(col_expr.lt(Expr::Literal(rhs.clone(), None))),
                (Bound::Excluded(lhs), Bound::Unbounded) => {
                    col_expr.gt(Expr::Literal(lhs.clone(), None))
                }
                (Bound::Excluded(lhs), Bound::Included(rhs)) => col_expr
                    .clone()
                    .gt(Expr::Literal(lhs.clone(), None))
                    .and(col_expr.lt_eq(Expr::Literal(rhs.clone(), None))),
                (Bound::Excluded(lhs), Bound::Excluded(rhs)) => col_expr
                    .clone()
                    .gt(Expr::Literal(lhs.clone(), None))
                    .and(col_expr.lt(Expr::Literal(rhs.clone(), None))),
            },
            Self::IsIn(values) => col_expr.in_list(
                values
                    .iter()
                    .map(|val| Expr::Literal(val.clone(), None))
                    .collect::<Vec<_>>(),
                false,
            ),
            Self::FullTextSearch(query) => col_expr.like(Expr::Literal(
                ScalarValue::Utf8(Some(query.query.to_string())),
                None,
            )),
            Self::IsNull() => col_expr.is_null(),
            Self::Equals(value) => col_expr.eq(Expr::Literal(value.clone(), None)),
            Self::LikePrefix(prefix) => match prefix {
                ScalarValue::Utf8(Some(s))
                | ScalarValue::LargeUtf8(Some(s))
                | ScalarValue::Utf8View(Some(s)) => {
                    // The prefix is a literal string. If it contains LIKE metacharacters
                    // (`_`, `%`, `\`) they must be escaped before appending the `%` wildcard;
                    // otherwise an inexact recheck (e.g. zone maps) would treat them as
                    // wildcards and over-match rows that do not start with the literal prefix.
                    // When the prefix has no metacharacters we keep the plain
                    // `col LIKE 'prefix%'` form (no `ESCAPE`), identical to the prior behavior,
                    // so DataFusion's optimized prefix matcher still applies.
                    // A `Utf8View` prefix is handled here too (rather than falling through to
                    // the catch-all arm, which would rebuild the recheck without the trailing
                    // `%` and silently turn prefix matching into equality matching) and is
                    // normalized to a `Utf8` pattern below, mirroring how the parser normalizes
                    // `Utf8View` to `Utf8` (see `expression.rs`).
                    let escaped = escape_like_pattern(s);
                    let needs_escape = escaped.as_str() != s.as_str();
                    let pattern = format!("{}%", escaped);
                    let pattern_value = match prefix {
                        ScalarValue::LargeUtf8(_) => ScalarValue::LargeUtf8(Some(pattern)),
                        _ => ScalarValue::Utf8(Some(pattern)),
                    };
                    if needs_escape {
                        Expr::Like(Like {
                            negated: false,
                            expr: Box::new(col_expr),
                            pattern: Box::new(Expr::Literal(pattern_value, None)),
                            escape_char: Some('\\'),
                            case_insensitive: false,
                        })
                    } else {
                        col_expr.like(Expr::Literal(pattern_value, None))
                    }
                }
                other => col_expr.like(Expr::Literal(other.clone(), None)),
            },
        }
    }

    fn dyn_eq(&self, other: &dyn AnyQuery) -> bool {
        match other.as_any().downcast_ref::<Self>() {
            Some(o) => self == o,
            None => false,
        }
    }
}

/// A query that a LabelListIndex can satisfy
#[derive(Debug, Clone, PartialEq)]
pub enum LabelListQuery {
    /// Retrieve all row ids where every label is in the list of values for the row
    HasAllLabels(Vec<ScalarValue>),
    /// Retrieve all row ids where at least one of the given labels is in the list of values for the row
    HasAnyLabel(Vec<ScalarValue>),
}

impl AnyQuery for LabelListQuery {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn format(&self, col: &str) -> String {
        format!("{}", self.to_expr(col.to_string()))
    }

    fn to_expr(&self, col: String) -> Expr {
        match self {
            Self::HasAllLabels(labels) => {
                let labels_arr = ScalarValue::iter_to_array(labels.iter().cloned()).unwrap();
                let offsets_buffer =
                    OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, labels_arr.len() as i32]));
                let labels_list = ListArray::try_new(
                    Arc::new(Field::new("item", labels_arr.data_type().clone(), true)),
                    offsets_buffer,
                    labels_arr,
                    None,
                )
                .unwrap();
                let labels_arr = Arc::new(labels_list);
                Expr::ScalarFunction(ScalarFunction {
                    func: Arc::new(array_has::ArrayHasAll::new().into()),
                    args: vec![
                        Expr::Column(Column::new_unqualified(col)),
                        Expr::Literal(ScalarValue::List(labels_arr), None),
                    ],
                })
            }
            Self::HasAnyLabel(labels) => {
                let labels_arr = ScalarValue::iter_to_array(labels.iter().cloned()).unwrap();
                let offsets_buffer =
                    OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, labels_arr.len() as i32]));
                let labels_list = ListArray::try_new(
                    Arc::new(Field::new("item", labels_arr.data_type().clone(), true)),
                    offsets_buffer,
                    labels_arr,
                    None,
                )
                .unwrap();
                let labels_arr = Arc::new(labels_list);
                Expr::ScalarFunction(ScalarFunction {
                    func: Arc::new(array_has::ArrayHasAny::new().into()),
                    args: vec![
                        Expr::Column(Column::new_unqualified(col)),
                        Expr::Literal(ScalarValue::List(labels_arr), None),
                    ],
                })
            }
        }
    }

    fn dyn_eq(&self, other: &dyn AnyQuery) -> bool {
        match other.as_any().downcast_ref::<Self>() {
            Some(o) => self == o,
            None => false,
        }
    }
}

/// A query that a NGramIndex can satisfy
#[derive(Debug, Clone, PartialEq)]
pub enum TextQuery {
    /// Retrieve all row ids where the text contains the given string
    StringContains(String),
    /// Retrieve all row ids whose text matches the given regular expression.
    ///
    /// The pattern is a full regular expression (as accepted by `regexp_like`).
    /// The index returns a candidate superset that the scan rechecks, so any
    /// pattern is sound; patterns with no usable trigram structure simply fall
    /// back to rechecking every row.
    Regex(String),
    // TODO: In the future we should be able to do case-insensitive contains
    // as well as partial matches (e.g. LIKE 'foo%').
}

impl AnyQuery for TextQuery {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn format(&self, col: &str) -> String {
        format!("{}", self.to_expr(col.to_string()))
    }

    fn to_expr(&self, col: String) -> Expr {
        match self {
            Self::StringContains(substr) => Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(ContainsFunc::new().into()),
                args: vec![
                    Expr::Column(Column::new_unqualified(col)),
                    Expr::Literal(ScalarValue::Utf8(Some(substr.clone())), None),
                ],
            }),
            // `regexp_like` returns Boolean directly, so the reconstructed
            // expression can be used as-is for the recheck filter (no IsNotNull
            // wrapper, unlike `regexp_match`). It is the semantic equivalent of
            // the original predicate for the "does it match" question.
            Self::Regex(pattern) => Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(RegexpLikeFunc::new().into()),
                args: vec![
                    Expr::Column(Column::new_unqualified(col)),
                    Expr::Literal(ScalarValue::Utf8(Some(pattern.clone())), None),
                ],
            }),
        }
    }

    fn dyn_eq(&self, other: &dyn AnyQuery) -> bool {
        match other.as_any().downcast_ref::<Self>() {
            Some(o) => self == o,
            None => false,
        }
    }
}

/// A query that a InvertedIndex can satisfy
#[derive(Debug, Clone, PartialEq)]
pub enum TokenQuery {
    /// Retrieve all row ids where the text contains all tokens parsed from given string. The tokens
    /// are separated by punctuations and white spaces.
    TokensContains(String),
}

/// A query that a BloomFilter index can satisfy
///
/// This is a subset of SargableQuery that only includes operations that bloom filters
/// can efficiently handle: equals, is_null, and is_in queries.
#[derive(Debug, Clone, PartialEq)]
pub enum BloomFilterQuery {
    /// Retrieve all row ids where the value is exactly the given value
    Equals(ScalarValue),
    /// Retrieve all row ids where the value is null
    IsNull(),
    /// Retrieve all row ids where the value is in the given set of values
    IsIn(Vec<ScalarValue>),
}

impl AnyQuery for BloomFilterQuery {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn format(&self, col: &str) -> String {
        match self {
            Self::Equals(val) => {
                format!("{} = {}", col, val)
            }
            Self::IsNull() => {
                format!("{} IS NULL", col)
            }
            Self::IsIn(values) => {
                format!(
                    "{} IN [{}]",
                    col,
                    values
                        .iter()
                        .map(|val| val.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
        }
    }

    fn to_expr(&self, col: String) -> Expr {
        let col_expr = Expr::Column(Column::new_unqualified(col));
        match self {
            Self::Equals(value) => col_expr.eq(Expr::Literal(value.clone(), None)),
            Self::IsNull() => col_expr.is_null(),
            Self::IsIn(values) => col_expr.in_list(
                values
                    .iter()
                    .map(|val| Expr::Literal(val.clone(), None))
                    .collect::<Vec<_>>(),
                false,
            ),
        }
    }

    fn dyn_eq(&self, other: &dyn AnyQuery) -> bool {
        match other.as_any().downcast_ref::<Self>() {
            Some(o) => self == o,
            None => false,
        }
    }
}

impl AnyQuery for TokenQuery {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn format(&self, col: &str) -> String {
        format!("{}", self.to_expr(col.to_string()))
    }

    fn to_expr(&self, col: String) -> Expr {
        match self {
            Self::TokensContains(substr) => Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(CONTAINS_TOKENS_UDF.clone()),
                args: vec![
                    Expr::Column(Column::new_unqualified(col)),
                    Expr::Literal(ScalarValue::Utf8(Some(substr.clone())), None),
                ],
            }),
        }
    }

    fn dyn_eq(&self, other: &dyn AnyQuery) -> bool {
        match other.as_any().downcast_ref::<Self>() {
            Some(o) => self == o,
            None => false,
        }
    }
}

#[cfg(feature = "geo")]
#[derive(Debug, Clone, PartialEq)]
pub struct RelationQuery {
    pub value: ScalarValue,
    pub field: Field,
}

/// A query that a Geo index can satisfy
#[cfg(feature = "geo")]
#[derive(Debug, Clone, PartialEq)]
pub enum GeoQuery {
    IntersectQuery(RelationQuery),
    IsNull,
}

#[cfg(feature = "geo")]
impl AnyQuery for GeoQuery {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn format(&self, col: &str) -> String {
        match self {
            Self::IntersectQuery(query) => {
                format!("Intersect({} {})", col, query.value)
            }
            Self::IsNull => {
                format!("{} IS NULL", col)
            }
        }
    }

    fn to_expr(&self, _col: String) -> Expr {
        todo!()
    }

    fn dyn_eq(&self, other: &dyn AnyQuery) -> bool {
        match other.as_any().downcast_ref::<Self>() {
            Some(o) => self == o,
            None => false,
        }
    }
}

/// Compute the lexicographically next prefix by incrementing the last character's code point.
/// Returns None if no valid upper bound exists.
///
/// This is used for LIKE prefix queries to convert `LIKE 'foo%'` to range `[foo, fop)`.
///
/// # UTF-8 and Unicode Handling
///
/// This function operates on Unicode code points (characters), not bytes. Since UTF-8
/// byte ordering is identical to Unicode code point ordering, incrementing a character's
/// code point produces the correct lexicographic successor for byte-wise string comparison.
///
/// If incrementing the last character would overflow or land in the surrogate range
/// (U+D800-U+DFFF), we try incrementing the previous character, and so on.
///
/// Examples:
/// - `"foo"` → `Some("fop")`
/// - `"café"` → `Some("cafê")`  (é U+00E9 → ê U+00EA)
/// - `"abc中"` → `Some("abc丮")` (中 U+4E2D → 丮 U+4E2E)
/// - `"cafÿ"` → `Some("cafĀ")` (ÿ U+00FF → Ā U+0100)
pub fn compute_next_prefix(prefix: &str) -> Option<String> {
    if prefix.is_empty() {
        return None;
    }

    let chars: Vec<char> = prefix.chars().collect();

    // Try incrementing characters from right to left
    for i in (0..chars.len()).rev() {
        if let Some(next_char) = next_unicode_char(chars[i]) {
            let mut result: String = chars[..i].iter().collect();
            result.push(next_char);
            return Some(result);
        }
        // This character cannot be incremented (e.g., U+10FFFF), try previous
    }

    // All characters were at maximum value
    None
}

/// Get the next valid Unicode scalar value after the given character.
/// Skips the surrogate range (U+D800-U+DFFF) which is not valid in UTF-8.
fn next_unicode_char(c: char) -> Option<char> {
    let cp = c as u32;
    let next_cp = cp.checked_add(1)?;

    // Skip surrogate range (U+D800-U+DFFF)
    let next_cp = if (0xD800..=0xDFFF).contains(&next_cp) {
        0xE000
    } else {
        next_cp
    };

    char::from_u32(next_cp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_like_prefix_to_expr_escapes_metacharacters() {
        // The stored prefix is a literal string, so LIKE metacharacters in it must be
        // escaped when the recheck predicate is rebuilt; otherwise `_`/`%` would act as
        // wildcards and over-match. The reconstructed expression uses `ESCAPE '\'`.
        let query = SargableQuery::LikePrefix(ScalarValue::Utf8(Some("a_b%x".to_string())));
        let Expr::Like(like) = query.to_expr("name".to_string()) else {
            panic!("expected a LIKE expression");
        };
        assert_eq!(like.escape_char, Some('\\'));
        assert!(!like.negated);
        assert!(!like.case_insensitive);
        let Expr::Literal(ScalarValue::Utf8(Some(pattern)), _) = like.pattern.as_ref() else {
            panic!("expected a Utf8 literal pattern");
        };
        assert_eq!(pattern.as_str(), "a\\_b\\%x%");

        // A prefix without metacharacters only gains the trailing wildcard and keeps the
        // plain `LIKE 'app%'` form (no `ESCAPE`) so the optimized prefix matcher still applies.
        let query = SargableQuery::LikePrefix(ScalarValue::Utf8(Some("app".to_string())));
        let Expr::Like(like) = query.to_expr("name".to_string()) else {
            panic!("expected a LIKE expression");
        };
        assert_eq!(like.escape_char, None);
        let Expr::Literal(ScalarValue::Utf8(Some(pattern)), _) = like.pattern.as_ref() else {
            panic!("expected a Utf8 literal pattern");
        };
        assert_eq!(pattern.as_str(), "app%");

        // A `Utf8View` prefix must get the same treatment instead of falling through to the
        // catch-all arm: it is normalized to a `Utf8` pattern that keeps the trailing `%`
        // (and `ESCAPE '\'` when the prefix has metacharacters), so prefix pruning preserves
        // starts-with semantics rather than degrading to equality.
        let query = SargableQuery::LikePrefix(ScalarValue::Utf8View(Some("a_b%x".to_string())));
        let Expr::Like(like) = query.to_expr("name".to_string()) else {
            panic!("expected a LIKE expression");
        };
        assert_eq!(like.escape_char, Some('\\'));
        let Expr::Literal(ScalarValue::Utf8(Some(pattern)), _) = like.pattern.as_ref() else {
            panic!("expected a Utf8 literal pattern");
        };
        assert_eq!(pattern.as_str(), "a\\_b\\%x%");

        let query = SargableQuery::LikePrefix(ScalarValue::Utf8View(Some("app".to_string())));
        let Expr::Like(like) = query.to_expr("name".to_string()) else {
            panic!("expected a LIKE expression");
        };
        assert_eq!(like.escape_char, None);
        let Expr::Literal(ScalarValue::Utf8(Some(pattern)), _) = like.pattern.as_ref() else {
            panic!("expected a Utf8 literal pattern");
        };
        assert_eq!(pattern.as_str(), "app%");
    }
}
