// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! SQL compatibility helpers for query filters.

use std::{any::TypeId, ops::ControlFlow};

use datafusion_sql::sqlparser::{
    ast::{
        BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName,
        visit_expressions_mut,
    },
    dialect::{Dialect, GenericDialect},
    parser::Parser,
    tokenizer::{Token, Tokenizer},
};

#[derive(Debug, Default)]
struct LanceDialect(GenericDialect);

impl Dialect for LanceDialect {
    fn dialect(&self) -> TypeId {
        self.0.dialect()
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        self.0.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.0.is_identifier_part(ch)
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`'
    }
}

/// Rewrite `ST_DWithin(a, b, distance)` as `ST_Distance(a, b) <= distance`.
///
/// GeoDataFusion does not currently expose `ST_DWithin`, but its definition is
/// exactly this distance comparison. Only unqualified, ordinary calls with
/// exactly three positional arguments are rewritten. Invalid SQL and other
/// function shapes are left untouched so the query planner can report the
/// usual error.
pub(crate) fn rewrite_st_dwithin(filter: &str) -> String {
    if !filter.to_ascii_lowercase().contains("st_dwithin") {
        return filter.to_string();
    }

    let Some(mut expr) = parse_filter(filter) else {
        return filter.to_string();
    };
    let mut rewritten = false;
    let _ = visit_expressions_mut(&mut expr, |expr| {
        let Expr::Function(function) = expr else {
            return ControlFlow::<()>::Continue(());
        };
        if function.name.0.len() != 1
            || !function.name.to_string().eq_ignore_ascii_case("st_dwithin")
            || function.uses_odbc_syntax
            || !matches!(&function.parameters, FunctionArguments::None)
            || function.filter.is_some()
            || function.null_treatment.is_some()
            || function.over.is_some()
            || !function.within_group.is_empty()
        {
            return ControlFlow::Continue(());
        }
        let FunctionArguments::List(arguments) = &function.args else {
            return ControlFlow::Continue(());
        };
        if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
            return ControlFlow::Continue(());
        }
        let [
            FunctionArg::Unnamed(FunctionArgExpr::Expr(first)),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(second)),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(distance)),
        ] = arguments.args.as_slice()
        else {
            return ControlFlow::Continue(());
        };

        let mut st_distance = function.clone();
        st_distance.name = ObjectName::from(vec![Ident::new("ST_Distance")]);
        if let FunctionArguments::List(arguments) = &mut st_distance.args {
            arguments.args = vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(first.clone())),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(second.clone())),
            ];
        }
        *expr = Expr::BinaryOp {
            left: Box::new(Expr::Function(st_distance)),
            op: BinaryOperator::LtEq,
            right: Box::new(distance.clone()),
        };
        rewritten = true;
        ControlFlow::Continue(())
    });

    if rewritten {
        expr.to_string()
    } else {
        filter.to_string()
    }
}

fn parse_filter(filter: &str) -> Option<Expr> {
    let dialect = LanceDialect::default();
    let mut token_iter = Tokenizer::new(&dialect, filter)
        .tokenize()
        .ok()?
        .into_iter();
    let mut previous = token_iter.next()?;
    let mut tokens = Vec::new();

    // Match Lance's support for `==` as an equality operator.
    for next in token_iter {
        if let (Token::Eq, Token::Eq) = (&previous, &next) {
            continue;
        }
        tokens.push(std::mem::replace(&mut previous, next));
    }
    tokens.push(previous);

    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    let expr = parser.parse_expr().ok()?;
    // A filter is one expression. Reject trailing clauses or statements rather
    // than rewriting a prefix and silently changing what the caller asked for.
    if parser.peek_token().token != Token::EOF {
        return None;
    }
    Some(expr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_nested_and_case_insensitive_dwithin() {
        assert_eq!(
            rewrite_st_dwithin("id == 1 AND st_dwithin(ST_Point(x, y), ST_Point(0, 0), radius)"),
            "id = 1 AND ST_Distance(ST_Point(x, y), ST_Point(0, 0)) <= radius"
        );
    }

    #[test]
    fn leaves_unsupported_function_shapes_unchanged() {
        for filter in [
            "id == 1",
            "'ST_DWithin(point, origin, 1.0)'",
            "geo.ST_DWithin(point, origin, 1.0)",
            "ST_DWithin(point, origin)",
            "ST_DWithin(point, origin, 1.0, extra)",
            "ST_DWithin(DISTINCT point, origin, 1.0)",
            "ST_DWithin(point => point, origin => origin, distance => 1.0)",
            "ST_DWithin(point, origin, 1.0) OVER ()",
        ] {
            assert_eq!(rewrite_st_dwithin(filter), filter);
        }
    }

    #[test]
    fn leaves_malformed_or_multi_expression_filters_unchanged() {
        for filter in [
            "ST_DWithin(point, origin, 1.0",
            "ST_DWithin(point, origin, 1.0) LIMIT 0",
            "ST_DWithin(point, origin, 1.0) ORDER BY id",
            "ST_DWithin(point, origin, 1.0); SELECT 1",
        ] {
            assert_eq!(rewrite_st_dwithin(filter), filter);
        }
    }
}
