// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Deriving a trigram pre-filter from a regular expression.
//!
//! This is the query-side counterpart of the ngram index that lets us
//! accelerate `regexp_like` / `regexp_match` predicates the same way the index
//! already accelerates `contains`. The idea (the same one Postgres `pg_trgm`
//! and Russ Cox's Google Code Search use) is to derive, from the regex, a
//! boolean condition over trigram presence that is *necessary* for any string
//! to match, evaluate it against the inverted index, and let the scan recheck
//! the true regex on the surviving rows.
//!
//! The derived condition is a [`TrigramQuery`] -- an AND/OR tree of trigram
//! tokens. `AND` maps onto posting-list intersection and `OR` onto union, which
//! is exactly the set algebra the ngram index is built for.
//!
//! # Soundness
//!
//! The single invariant that matters is that the condition must never require a
//! trigram that a matching string could lack -- otherwise we would drop real
//! matches (a false negative, far worse than a false positive, which the recheck
//! removes). Everything here is therefore a conservative *over*-approximation:
//! when in doubt we emit [`TrigramQuery::All`] ("no constraint, recheck
//! everything"). Concretely:
//!
//! * Every trigram requirement is produced by [`trigrams_of_string`], which runs
//!   the *same* tokenizer the index was built with, so a string shorter than a
//!   trigram (or with no alphanumeric run) contributes no requirement.
//! * Character classes and case-insensitive folds are treated as a single
//!   unknown character (`All`), because the index's normalization does not agree
//!   with Unicode case folding (e.g. `(?i)c` also matches `ℂ`, which the index
//!   does not fold to `c`). Literal runs -- the common case -- are fully used.
//! * When the exact / prefix / suffix string sets grow past a bound we first fold
//!   their trigrams into the running condition and only then drop the strings, so
//!   collapsing precision never removes a necessary trigram.

use std::collections::{BTreeSet, HashMap, HashSet};

use regex_syntax::hir::{Class, Hir, HirKind};
use roaring::RoaringTreemap;

use super::{NGRAM_N, NGRAM_TOKENIZER, ngram_to_token, tokenize_visitor};

/// Maximum number of strings kept in an `exact` / `prefix` / `suffix` set before
/// it is folded into the trigram condition and dropped.
const MAX_SET_SIZE: usize = 16;
/// Maximum length (in characters) of a string kept in a set. Longer strings are
/// trimmed to a sound shorter affix.
const MAX_STRING_LEN: usize = 32;

/// A boolean condition over trigram presence that is *necessary* for a regex to
/// match. `All` means "no constraint" and `None` means "unsatisfiable"; by
/// construction these only ever appear at the root of the tree.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum TrigramQuery {
    /// No constraint: every row is a candidate (the scan must recheck all rows).
    All,
    /// Unsatisfiable: no row can match.
    None,
    /// The given trigram token must be present.
    Trigram(u32),
    /// Every child condition must hold (posting-list intersection).
    And(Vec<Self>),
    /// At least one child condition must hold (posting-list union).
    Or(Vec<Self>),
}

impl TrigramQuery {
    /// Build an `AND` of conditions, applying identity (`All`), absorbing
    /// (`None`), flattening, sorting and de-duplication so the result is
    /// canonical and free of nested `All`/`None`.
    fn and(items: Vec<Self>) -> Self {
        let mut flat = Vec::with_capacity(items.len());
        for item in items {
            match item {
                Self::All => {}                               // identity
                Self::None => return Self::None,              // absorbing
                Self::And(children) => flat.extend(children), // flatten
                other => flat.push(other),
            }
        }
        flat.sort();
        flat.dedup();
        match flat.len() {
            0 => Self::All,
            1 => flat.pop().unwrap(),
            _ => Self::And(flat),
        }
    }

    /// Build an `OR` of conditions, applying absorbing (`All`), identity
    /// (`None`), flattening, sorting and de-duplication.
    fn or(items: Vec<Self>) -> Self {
        let mut flat = Vec::with_capacity(items.len());
        for item in items {
            match item {
                Self::All => return Self::All,               // absorbing
                Self::None => {}                             // identity
                Self::Or(children) => flat.extend(children), // flatten
                other => flat.push(other),
            }
        }
        flat.sort();
        flat.dedup();
        match flat.len() {
            0 => Self::None,
            1 => flat.pop().unwrap(),
            _ => Self::Or(flat),
        }
    }
}

/// Information about the set of strings a sub-expression can match, used to
/// build a necessary trigram condition bottom-up. For every string `s` the
/// sub-expression matches: `s` is in `exact` (when it is `Some`), `s` starts
/// with some member of `prefix` and ends with some member of `suffix`, and `s`
/// satisfies `match_q`.
struct RegexInfo {
    /// Whether the sub-expression can match the empty string.
    emptyable: bool,
    /// The complete set of strings the sub-expression matches, or `None` if that
    /// set is unbounded / unknown.
    exact: Option<BTreeSet<String>>,
    /// Strings that every match must start with (empty = unknown).
    prefix: BTreeSet<String>,
    /// Strings that every match must end with (empty = unknown).
    suffix: BTreeSet<String>,
    /// A necessary trigram condition for the sub-expression.
    match_q: TrigramQuery,
}

impl RegexInfo {
    /// The empty string (also used for zero-width anchors): matches only `""`.
    fn empty_string() -> Self {
        let empty = BTreeSet::from([String::new()]);
        Self {
            emptyable: true,
            exact: Some(empty.clone()),
            prefix: empty.clone(),
            suffix: empty,
            match_q: TrigramQuery::All,
        }
    }

    /// A fixed literal string.
    fn literal(s: &str) -> Self {
        let set = BTreeSet::from([s.to_string()]);
        Self {
            emptyable: s.is_empty(),
            exact: Some(set.clone()),
            prefix: set.clone(),
            suffix: set,
            match_q: trigrams_of_string(s),
        }
    }

    /// A single unknown character (a character class we cannot pin down).
    fn any_char() -> Self {
        Self {
            emptyable: false,
            exact: None,
            prefix: BTreeSet::new(),
            suffix: BTreeSet::new(),
            match_q: TrigramQuery::All,
        }
    }

    /// Enforce the size/length bounds, folding any information about to be
    /// discarded into `match_q` first so that precision loss never drops a
    /// necessary trigram. Idempotent.
    fn bound(&mut self) {
        let oversized_exact = self.exact.as_ref().is_some_and(|exact| {
            exact.len() > MAX_SET_SIZE || exact.iter().any(|s| s.chars().count() > MAX_STRING_LEN)
        });
        if oversized_exact {
            let exact = self.exact.take().expect("checked above");
            self.fold_into_match(&exact);
        }

        self.prefix = self
            .prefix
            .iter()
            .map(|s| leading(s, MAX_STRING_LEN))
            .collect();
        if self.prefix.len() > MAX_SET_SIZE {
            let prefix = std::mem::take(&mut self.prefix);
            self.fold_into_match(&prefix);
        }

        self.suffix = self
            .suffix
            .iter()
            .map(|s| trailing(s, MAX_STRING_LEN))
            .collect();
        if self.suffix.len() > MAX_SET_SIZE {
            let suffix = std::mem::take(&mut self.suffix);
            self.fold_into_match(&suffix);
        }
    }

    /// AND the trigrams of `set` (a complete set of possible affixes/strings)
    /// into `match_q`. Sound because the set is exhaustive for its role.
    fn fold_into_match(&mut self, set: &BTreeSet<String>) {
        let folded = trigrams_of_set(set.iter());
        let current = std::mem::replace(&mut self.match_q, TrigramQuery::All);
        self.match_q = TrigramQuery::and(vec![current, folded]);
    }
}

/// AND together the trigrams of `s`. Reuses the index's own tokenizer so the
/// tokens are normalized (lowercase, ASCII-folded, alphanumeric-bounded)
/// exactly as they were stored. Returns `All` if `s` yields no trigram (too
/// short, or no run of three alphanumeric characters).
fn trigrams_of_string(s: &str) -> TrigramQuery {
    let mut tokens = Vec::new();
    tokenize_visitor(&NGRAM_TOKENIZER, s, |ngram| {
        tokens.push(TrigramQuery::Trigram(ngram_to_token(ngram, NGRAM_N)));
    });
    TrigramQuery::and(tokens)
}

/// OR together the trigram conditions of each string in `set`. An empty set
/// means "unknown" and yields `All` (no constraint); if any member yields `All`
/// the whole OR is `All`.
fn trigrams_of_set<'a>(set: impl IntoIterator<Item = &'a String>) -> TrigramQuery {
    let queries: Vec<_> = set.into_iter().map(|s| trigrams_of_string(s)).collect();
    if queries.is_empty() {
        return TrigramQuery::All;
    }
    TrigramQuery::or(queries)
}

/// Concatenate every string in `a` with every string in `b`.
fn cross_concat(a: &BTreeSet<String>, b: &BTreeSet<String>) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for x in a {
        for y in b {
            out.insert(format!("{x}{y}"));
        }
    }
    out
}

/// The first `n` characters of `s` (a sound shorter prefix).
fn leading(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

/// The last `n` characters of `s` (a sound shorter suffix).
fn trailing(s: &str, n: usize) -> String {
    let count = s.chars().count();
    s.chars().skip(count.saturating_sub(n)).collect()
}

/// If `class` matches exactly one scalar value, return that character.
fn singleton_char(class: &Class) -> Option<char> {
    match class {
        Class::Unicode(u) => {
            let ranges = u.ranges();
            match ranges {
                [r] if r.start() == r.end() => Some(r.start()),
                _ => None,
            }
        }
        Class::Bytes(b) => {
            let ranges = b.ranges();
            match ranges {
                [r] if r.start() == r.end() && r.start() < 0x80 => Some(r.start() as char),
                _ => None,
            }
        }
    }
}

/// Compute the [`RegexInfo`] for `hir` bottom-up.
fn analyze(hir: &Hir) -> RegexInfo {
    let mut info = match hir.kind() {
        // Zero-width: the empty match. Anchors (^, $, \b) carry no trigram.
        HirKind::Empty | HirKind::Look(_) => RegexInfo::empty_string(),
        HirKind::Literal(lit) => match std::str::from_utf8(&lit.0) {
            Ok(s) => RegexInfo::literal(s),
            // A literal that is not valid UTF-8 cannot be reasoned about here.
            Err(_) => RegexInfo::any_char(),
        },
        HirKind::Class(class) => match singleton_char(class) {
            Some(ch) => RegexInfo::literal(ch.encode_utf8(&mut [0u8; 4])),
            None => RegexInfo::any_char(),
        },
        HirKind::Repetition(rep) => {
            let inner = analyze(&rep.sub);
            let at_least_one = rep.min >= 1;
            RegexInfo {
                emptyable: !at_least_one || inner.emptyable,
                // We do not unroll bounded repetitions, so the matched set is
                // unbounded as far as we are concerned.
                exact: None,
                prefix: if at_least_one {
                    inner.prefix.clone()
                } else {
                    BTreeSet::new()
                },
                suffix: if at_least_one {
                    inner.suffix.clone()
                } else {
                    BTreeSet::new()
                },
                // Only a required occurrence (min >= 1) contributes; the single
                // inner match is necessary, never multiplied.
                match_q: if at_least_one {
                    inner.match_q
                } else {
                    TrigramQuery::All
                },
            }
        }
        HirKind::Capture(cap) => analyze(&cap.sub),
        HirKind::Concat(subs) => analyze_concat(subs),
        HirKind::Alternation(subs) => analyze_alternation(subs),
    };
    info.bound();
    info
}

fn analyze_concat(subs: &[Hir]) -> RegexInfo {
    let mut acc = RegexInfo::empty_string();
    for sub in subs {
        acc = concat_info(acc, analyze(sub));
    }
    acc
}

/// Combine two adjacent sub-expressions. This is the subtle part: it recovers
/// trigrams that straddle the junction via the cross product of `acc.suffix` and
/// `next.prefix`.
fn concat_info(acc: RegexInfo, next: RegexInfo) -> RegexInfo {
    let emptyable = acc.emptyable && next.emptyable;

    // Trigrams spanning the junction (computed from the pre-merge affixes).
    let boundary = if acc.suffix.is_empty() || next.prefix.is_empty() {
        TrigramQuery::All
    } else {
        trigrams_of_set(cross_concat(&acc.suffix, &next.prefix).iter())
    };

    // exact = acc.exact x next.exact, only while both are finite and small.
    let exact = match (&acc.exact, &next.exact) {
        (Some(a), Some(b)) if a.len().saturating_mul(b.len()) <= MAX_SET_SIZE => {
            Some(cross_concat(a, b))
        }
        _ => None,
    };

    // A match starts with acc's full string (when known) then next's prefix,
    // otherwise with acc's own prefix.
    let prefix = match &acc.exact {
        Some(a) if !next.prefix.is_empty() => cross_concat(a, &next.prefix),
        Some(a) => a.clone(),
        None => acc.prefix.clone(),
    };

    // Mirror image for the suffix (driven by the right side).
    let suffix = match &next.exact {
        Some(b) if !acc.suffix.is_empty() => cross_concat(&acc.suffix, b),
        Some(b) => b.clone(),
        None => next.suffix.clone(),
    };

    let match_q = TrigramQuery::and(vec![acc.match_q, next.match_q, boundary]);

    let mut info = RegexInfo {
        emptyable,
        exact,
        prefix,
        suffix,
        match_q,
    };
    info.bound();
    info
}

fn analyze_alternation(subs: &[Hir]) -> RegexInfo {
    let infos: Vec<RegexInfo> = subs.iter().map(analyze).collect();

    let emptyable = infos.iter().any(|i| i.emptyable);

    let exact = if infos.iter().all(|i| i.exact.is_some()) {
        Some(
            infos
                .iter()
                .flat_map(|i| i.exact.as_ref().unwrap().iter().cloned())
                .collect(),
        )
    } else {
        None
    };

    // A common prefix exists only if every branch contributes one.
    let prefix = if infos.iter().all(|i| !i.prefix.is_empty()) {
        infos
            .iter()
            .flat_map(|i| i.prefix.iter().cloned())
            .collect()
    } else {
        BTreeSet::new()
    };
    let suffix = if infos.iter().all(|i| !i.suffix.is_empty()) {
        infos
            .iter()
            .flat_map(|i| i.suffix.iter().cloned())
            .collect()
    } else {
        BTreeSet::new()
    };

    let match_q = TrigramQuery::or(infos.into_iter().map(|i| i.match_q).collect());

    RegexInfo {
        emptyable,
        exact,
        prefix,
        suffix,
        match_q,
    }
}

/// Derive a necessary trigram condition from a regular expression pattern.
///
/// Returns [`TrigramQuery::All`] when no useful condition can be derived (an
/// unparsable pattern, or one with no trigram-able literal structure such as
/// `a.b` or `.*`); callers must treat that as "recheck everything".
pub fn regex_to_trigram_query(pattern: &str) -> TrigramQuery {
    // An unparsable pattern cannot be accelerated; rechecking is still safe.
    let Ok(hir) = regex_syntax::parse(pattern) else {
        return TrigramQuery::All;
    };
    let info = analyze(&hir);

    let mut conditions = vec![info.match_q];
    if let Some(exact) = &info.exact {
        if exact.is_empty() {
            // The expression matches nothing.
            return TrigramQuery::None;
        }
        conditions.push(trigrams_of_set(exact.iter()));
    }
    conditions.push(trigrams_of_set(info.prefix.iter()));
    conditions.push(trigrams_of_set(info.suffix.iter()));
    TrigramQuery::and(conditions)
}

/// Whether a regular expression yields any trigram condition the index can use
/// to prune candidates. When it does not (e.g. `a.b`, `.*`, or a case-insensitive
/// pattern), callers should leave the predicate to a full scan rather than route
/// it to the index, which would otherwise have to ask the scan to recheck every
/// row -- a path the index result type (`AtLeast`) does not support.
pub fn regex_can_use_index(pattern: &str) -> bool {
    regex_to_trigram_query(pattern) != TrigramQuery::All
}

/// Collect the distinct trigram tokens referenced anywhere in the tree.
pub fn collect_tokens(query: &TrigramQuery, out: &mut HashSet<u32>) {
    match query {
        TrigramQuery::Trigram(token) => {
            out.insert(*token);
        }
        TrigramQuery::And(items) | TrigramQuery::Or(items) => {
            for item in items {
                collect_tokens(item, out);
            }
        }
        TrigramQuery::All | TrigramQuery::None => {}
    }
}

/// Evaluate the tree against a map of `trigram token -> posting list`. A token
/// missing from the map contributes an empty set (sound: a required trigram that
/// is absent everywhere yields no rows; an absent OR branch contributes
/// nothing). `All` / `None` are handled by the caller before evaluation.
pub fn eval_trigram_query(
    query: &TrigramQuery,
    bitmaps: &HashMap<u32, RoaringTreemap>,
) -> RoaringTreemap {
    match query {
        TrigramQuery::Trigram(token) => bitmaps.get(token).cloned().unwrap_or_default(),
        TrigramQuery::And(items) => {
            let mut iter = items.iter();
            let mut acc = match iter.next() {
                Some(first) => eval_trigram_query(first, bitmaps),
                None => return RoaringTreemap::new(),
            };
            for item in iter {
                if acc.is_empty() {
                    break;
                }
                acc &= &eval_trigram_query(item, bitmaps);
            }
            acc
        }
        TrigramQuery::Or(items) => {
            let mut acc = RoaringTreemap::new();
            for item in items {
                acc |= &eval_trigram_query(item, bitmaps);
            }
            acc
        }
        TrigramQuery::All | TrigramQuery::None => RoaringTreemap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A single trigram condition, hashed the same way the index hashes it.
    fn tri(trigram: &str) -> TrigramQuery {
        TrigramQuery::Trigram(ngram_to_token(trigram, NGRAM_N))
    }

    fn q(pattern: &str) -> TrigramQuery {
        regex_to_trigram_query(pattern)
    }

    #[test]
    fn test_single_literal_trigram() {
        assert_eq!(q("foo"), tri("foo"));
    }

    #[test]
    fn test_multi_trigram_literal() {
        assert_eq!(
            q("foobar"),
            TrigramQuery::and(vec![tri("foo"), tri("oob"), tri("oba"), tri("bar")])
        );
    }

    #[test]
    fn test_wildcard_splits_into_and() {
        // `.*` breaks the literal run; both sides are required.
        assert_eq!(
            q("foo.*bar"),
            TrigramQuery::and(vec![tri("foo"), tri("bar")])
        );
    }

    #[test]
    fn test_alternation_is_or() {
        assert_eq!(
            q("(cat|dog)"),
            TrigramQuery::or(vec![tri("cat"), tri("dog")])
        );
    }

    #[test]
    fn test_anchors_are_transparent() {
        assert_eq!(
            q("^rhino"),
            TrigramQuery::and(vec![tri("rhi"), tri("hin"), tri("ino")])
        );
        assert_eq!(q("nose$"), TrigramQuery::and(vec![tri("nos"), tri("ose")]));
    }

    #[test]
    fn test_boundary_trigram_recovered_across_groups() {
        // A capturing group is not merged into the adjacent literals, so this
        // exercises the suffix x prefix cross product that recovers the `foo`
        // trigram straddling the `(o)` group boundary in "foobar".
        assert_eq!(
            q("fo(o)bar"), // spellchecker:disable-line
            TrigramQuery::and(vec![tri("foo"), tri("oob"), tri("oba"), tri("bar")])
        );
    }

    #[test]
    fn test_no_trigram_yields_all() {
        // No run of three literal characters anywhere.
        assert_eq!(q("a.b"), TrigramQuery::All);
        assert_eq!(q(".*"), TrigramQuery::All);
        // Every alternation branch is shorter than a trigram, so we must not
        // require either two-character branch as a (non-existent) trigram.
        assert_eq!(q("fo|ba"), TrigramQuery::All); // spellchecker:disable-line
    }

    #[test]
    fn test_case_insensitive_not_accelerated() {
        // Unicode case folding (e.g. `(?i)c` also matches U+2102) does not agree
        // with the index's normalization, so case-insensitive patterns are left
        // unaccelerated (correct via recheck) rather than risk a false negative.
        assert_eq!(q("(?i)Cat"), TrigramQuery::All);
    }

    #[test]
    fn test_unparsable_pattern_yields_all() {
        assert_eq!(q("("), TrigramQuery::All);
    }

    #[test]
    fn test_large_alternation_stays_bounded() {
        // More than MAX_SET_SIZE branches: must still produce a sound OR without
        // panicking or exploding.
        let pattern = (0..40)
            .map(|i| format!("aa{i:02}zz"))
            .collect::<Vec<_>>()
            .join("|");
        let result = q(&pattern);
        // Each branch shares the trigram `aa0`/`aa1`/... and `zz`-ish endings;
        // the important property is that it is a sound non-empty condition.
        assert_ne!(result, TrigramQuery::None);
    }

    #[test]
    fn test_plus_requires_inner() {
        // `(abc)+` must contain at least one `abc`.
        assert_eq!(q("(abc)+"), tri("abc"));
    }

    #[test]
    fn test_optional_group_is_not_required() {
        // `(foo)?bar` -> foo optional, bar required.
        assert_eq!(q("(foo)?bar"), tri("bar"));
    }

    #[test]
    fn test_eval_and_or_with_missing_tokens() {
        let foo = ngram_to_token("foo", NGRAM_N);
        let bar = ngram_to_token("bar", NGRAM_N);
        let mut bitmaps = HashMap::new();
        bitmaps.insert(foo, RoaringTreemap::from_iter([1u64, 2, 3]));
        bitmaps.insert(bar, RoaringTreemap::from_iter([2u64, 3, 4]));
        // `baz` is absent from the index.

        // AND intersects.
        let and = TrigramQuery::and(vec![tri("foo"), tri("bar")]);
        assert_eq!(
            eval_trigram_query(&and, &bitmaps),
            RoaringTreemap::from_iter([2u64, 3])
        );

        // OR unions.
        let or = TrigramQuery::or(vec![tri("foo"), tri("bar")]);
        assert_eq!(
            eval_trigram_query(&or, &bitmaps),
            RoaringTreemap::from_iter([1u64, 2, 3, 4])
        );

        // A missing token is empty: it zeroes an AND but is harmless in an OR.
        let and_missing = TrigramQuery::and(vec![tri("foo"), tri("baz")]);
        assert!(eval_trigram_query(&and_missing, &bitmaps).is_empty());
        let or_missing = TrigramQuery::or(vec![tri("foo"), tri("baz")]);
        assert_eq!(
            eval_trigram_query(&or_missing, &bitmaps),
            RoaringTreemap::from_iter([1u64, 2, 3])
        );
    }

    #[test]
    fn test_collect_tokens() {
        let query = TrigramQuery::and(vec![
            tri("foo"),
            TrigramQuery::or(vec![tri("bar"), tri("baz")]),
        ]);
        let mut tokens = HashSet::new();
        collect_tokens(&query, &mut tokens);
        assert_eq!(
            tokens,
            HashSet::from([
                ngram_to_token("foo", NGRAM_N),
                ngram_to_token("bar", NGRAM_N),
                ngram_to_token("baz", NGRAM_N),
            ])
        );
    }
}
