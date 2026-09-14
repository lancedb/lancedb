// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Routing a statement to the grammar that owns it.

use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::sqlparser::{
    dialect::GenericDialect,
    parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer, TokenizerError},
};

use super::statement::StatementRegistry;

/// Route a statement through the registry's grammars.
///
/// Returns `Ok(None)` when no grammar claims the statement, which is the
/// caller's cue to hand it to DataFusion's own planner.
///
/// The first grammar whose `matches` accepts the tokens is the only one given
/// the statement: a grammar that matches and then returns `Ok(None)` declines
/// the form rather than falling through to the next grammar. Registration
/// order therefore decides reachability, which is why [`StatementRegistry`]
/// fixes it explicitly.
pub fn route_custom_sql(registry: &StatementRegistry, sql: &str) -> Result<Option<LogicalPlan>> {
    let dialect = GenericDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize().map_err(|e: TokenizerError| {
        DataFusionError::SQL(Box::new(ParserError::TokenizerError(e.to_string())), None)
    })?;

    // Handlers match on keywords, so layout must not change the decision.
    let word_tokens: Vec<&Token> = tokens
        .iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect();

    for handler in registry.parsers() {
        if handler.matches(&word_tokens) {
            // `Parser` takes ownership of the tokens, so it is built only once
            // a handler has claimed the statement.
            let mut parser = Parser::new(&dialect).with_tokens(tokens.clone());
            if let Some(plan) = handler.parse(&mut parser)? {
                return Ok(Some(plan));
            }
            break;
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use datafusion::common::DFSchema;
    use datafusion::error::Result as DfResult;
    use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
    use datafusion::sql::sqlparser::keywords::Keyword;

    use super::*;
    use crate::sql::statement::CustomSqlHandler;

    fn empty_plan() -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        })
    }

    /// Matches on a leading keyword, and reports whether it was asked to parse.
    struct Handler {
        keyword: Keyword,
        outcome: Outcome,
        parsed: Arc<AtomicUsize>,
    }

    enum Outcome {
        Plans,
        Declines,
    }

    impl Handler {
        fn new(keyword: Keyword, outcome: Outcome) -> (Arc<Self>, Arc<AtomicUsize>) {
            let parsed = Arc::new(AtomicUsize::new(0));
            let handler = Arc::new(Self {
                keyword,
                outcome,
                parsed: parsed.clone(),
            });
            (handler, parsed)
        }
    }

    impl CustomSqlHandler for Handler {
        fn matches(&self, tokens: &[&Token]) -> bool {
            matches!(tokens.first(), Some(Token::Word(w)) if w.keyword == self.keyword)
        }

        fn parse(&self, _parser: &mut Parser) -> DfResult<Option<LogicalPlan>> {
            self.parsed.fetch_add(1, Ordering::SeqCst);
            Ok(match self.outcome {
                Outcome::Plans => Some(empty_plan()),
                Outcome::Declines => None,
            })
        }
    }

    #[test]
    fn an_unclaimed_statement_is_left_for_datafusion() {
        let registry = StatementRegistry::new();
        assert!(route_custom_sql(&registry, "SELECT 1").unwrap().is_none());
    }

    #[test]
    fn whitespace_does_not_change_which_handler_matches() {
        let (handler, parsed) = Handler::new(Keyword::EXPLAIN, Outcome::Plans);
        let mut registry = StatementRegistry::new();
        registry.register_parser(handler);

        for sql in ["EXPLAIN t", "  EXPLAIN\n\t t  "] {
            assert!(route_custom_sql(&registry, sql).unwrap().is_some());
        }
        assert_eq!(parsed.load(Ordering::SeqCst), 2);
    }

    /// Front-insertion is what lets an extension get ahead of a catch-all that
    /// would otherwise swallow the same keyword.
    #[test]
    fn the_last_registered_handler_is_consulted_first() {
        let (first, first_parsed) = Handler::new(Keyword::EXPLAIN, Outcome::Plans);
        let (second, second_parsed) = Handler::new(Keyword::EXPLAIN, Outcome::Plans);

        let mut registry = StatementRegistry::new();
        registry.register_parser(first).register_parser(second);

        assert!(route_custom_sql(&registry, "EXPLAIN t").unwrap().is_some());
        assert_eq!(second_parsed.load(Ordering::SeqCst), 1);
        assert_eq!(first_parsed.load(Ordering::SeqCst), 0);
    }

    /// A handler that matches and declines vetoes the statement rather than
    /// letting a later handler see it. Shadowing is silent, which is why
    /// registration order is part of the contract.
    #[test]
    fn a_handler_that_declines_shadows_the_handlers_behind_it() {
        let (shadowed, shadowed_parsed) = Handler::new(Keyword::EXPLAIN, Outcome::Plans);
        let (decliner, decliner_parsed) = Handler::new(Keyword::EXPLAIN, Outcome::Declines);

        let mut registry = StatementRegistry::new();
        registry.register_parser(shadowed).register_parser(decliner);

        assert!(route_custom_sql(&registry, "EXPLAIN t").unwrap().is_none());
        assert_eq!(decliner_parsed.load(Ordering::SeqCst), 1);
        assert_eq!(shadowed_parsed.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn from_parts_keeps_the_order_it_was_given() {
        let (first, first_parsed) = Handler::new(Keyword::EXPLAIN, Outcome::Plans);
        let (second, second_parsed) = Handler::new(Keyword::EXPLAIN, Outcome::Plans);

        let registry = StatementRegistry::from_parts(vec![first, second], vec![]);

        assert!(route_custom_sql(&registry, "EXPLAIN t").unwrap().is_some());
        assert_eq!(first_parsed.load(Ordering::SeqCst), 1);
        assert_eq!(second_parsed.load(Ordering::SeqCst), 0);
    }
}
