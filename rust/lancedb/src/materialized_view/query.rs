// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! The SQL a materialized view is defined by. A definition is stored as one
//! canonical query, parsed here into the relational shape refresh maintains:
//!
//! ```sql
//! SELECT <column | expr AS name | *>, ...
//! FROM [ns.]table [, function(args) AS alias | , UNNEST(column) AS alias]
//! [WHERE predicate] [LIMIT n]
//! ```
//!
//! A Function in `FROM` position yields one row per element it returns;
//! `UNNEST` does the same for a list column the table already holds.
//!
//! Anything else is a query this engine cannot maintain yet and is refused
//! at parse time, which is also what an older engine does with a newer
//! query: fail closed, never materialize it at the wrong cardinality.

use datafusion_sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, GroupByExpr, JoinOperator, LimitClause, ObjectName,
    ObjectNamePart, Query, SelectItem, SetExpr, Statement, TableFactor, TableFunctionArgs,
    TableWithJoins, Value,
};
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::keywords::{
    ALL_KEYWORDS, ALL_KEYWORDS_INDEX, RESERVED_FOR_COLUMN_ALIAS, RESERVED_FOR_IDENTIFIER,
    RESERVED_FOR_TABLE_ALIAS,
};
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::sqlparser::tokenizer::{Token, Tokenizer};
use lance_datafusion::planner::Planner;

use super::{LateralSource, MaterializedViewDefinition, ViewLateral, ViewProjection};
use crate::{Error, Result};

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidInput {
        message: message.into(),
    }
}

const SHAPE: &str = "a materialized view is defined by `SELECT columns FROM table \
    [, function(args) AS alias | , UNNEST(column) AS alias] [WHERE predicate] \
    [GROUP BY expr, ...] [LIMIT n]`";

/// Whether `name` must be delimited to read back as this identifier: bare,
/// the parser would take it as a keyword, or a different spelling.
fn needs_quote(name: &str) -> bool {
    let plain = !name.is_empty()
        && name
            .chars()
            .enumerate()
            .all(|(i, c)| c == '_' || c.is_ascii_lowercase() || (i > 0 && c.is_ascii_digit()));
    if !plain {
        return true;
    }
    let reserved = ALL_KEYWORDS
        .binary_search(&name.to_ascii_uppercase().as_str())
        .is_ok_and(|i| {
            let keyword = &ALL_KEYWORDS_INDEX[i];
            RESERVED_FOR_TABLE_ALIAS.contains(keyword)
                || RESERVED_FOR_COLUMN_ALIAS.contains(keyword)
                || RESERVED_FOR_IDENTIFIER.contains(keyword)
        });
    if reserved {
        return true;
    }
    // Then the parsers' own judgement: lance's in an expression, where a
    // column name is read, and sqlparser's in a `FROM`, where a table's is.
    let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        name,
        arrow_schema::DataType::Int32,
        true,
    )]));
    let planner = Planner::new(schema);
    let column = planner
        .parse_expr(&format!("{name} IS NOT NULL"))
        .is_ok_and(|expr| Planner::column_names_in_expr(&expr) == [name]);
    if !column {
        return true;
    }
    let sql = format!("SELECT 1 FROM {name}");
    !matches!(
        Parser::parse_sql(&GenericDialect {}, &sql).as_deref(),
        Ok([Statement::Query(query)]) if matches!(
            query.body.as_ref(),
            SetExpr::Select(select) if matches!(
                select.from.as_slice(),
                [TableWithJoins { relation: TableFactor::Table { name: table, .. }, .. }]
                    if table.to_string() == name
            )
        )
    )
}

/// `name` as a lance SQL identifier: bare when the parser reads it back
/// unchanged, backtick-delimited otherwise.
pub fn ident_sql(name: &str) -> String {
    if needs_quote(name) {
        format!("`{}`", name.replace('`', "``"))
    } else {
        name.to_string()
    }
}

/// Rewrite every delimited identifier in `sql` to the form [`ident_sql`]
/// produces, so the same name is spelled one way wherever it appears.
/// Lance's parser delimits with backticks only; a `"name"` is rewritten
/// rather than read as a string.
pub fn canonical_tokens(sql: &str) -> Result<String> {
    let tokens = Tokenizer::new(&GenericDialect {}, sql)
        .with_unescape(false)
        .tokenize()
        .map_err(|err| invalid(format!("invalid SQL: {err}")))?;
    Ok(tokens
        .into_iter()
        .map(|token| match token {
            Token::Word(word) if word.quote_style == Some('"') => {
                ident_sql(&word.value.replace("\"\"", "\""))
            }
            Token::Word(word) if word.quote_style == Some('`') => {
                ident_sql(&word.value.replace("``", "`"))
            }
            other => other.to_string(),
        })
        .collect())
}

/// One expression, in the spelling the stored query uses.
pub fn canonical_expr(sql: &str) -> Result<String> {
    let text = canonical_tokens(sql)?;
    let expr = Parser::new(&GenericDialect {})
        .try_with_sql(&text)
        .and_then(|mut parser| {
            let expr = parser.parse_expr()?;
            parser.expect_token(&Token::EOF)?;
            Ok(expr)
        })
        .map_err(|err| invalid(format!("invalid SQL expression '{sql}': {err}")))?;
    Ok(expr.to_string())
}

/// The column a bare `SELECT` item names: the last part of a plain or
/// compound identifier, `None` for any other expression.
fn column_ref_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()),
        _ => None,
    }
}

/// Parse `sql` into a definition. The query is re-rendered and compared
/// with what was parsed, so any clause this shape does not carry is
/// refused rather than dropped.
pub fn parse(sql: &str) -> Result<MaterializedViewDefinition> {
    let text = canonical_tokens(sql)?;
    let mut statements = Parser::parse_sql(&GenericDialect {}, &text)
        .map_err(|err| invalid(format!("invalid SQL: {err}")))?;
    let query = match (statements.pop(), statements.is_empty()) {
        (Some(Statement::Query(query)), true) => normalize_from(*query),
        _ => {
            return Err(invalid(format!(
                "expected a single SELECT statement; {SHAPE}"
            )));
        }
    };
    let definition = extract(&query)?;
    let rendered = render(&definition);
    if canonical_tokens(&query.to_string())? != rendered {
        return Err(invalid(format!(
            "unsupported clause in the view query; {SHAPE}"
        )));
    }
    Ok(definition)
}

/// One spelling per relation: `FROM t CROSS JOIN UNNEST(..)` is
/// `FROM t, UNNEST(..)`, and the alias always takes `AS`.
fn normalize_from(mut query: Query) -> Query {
    if let SetExpr::Select(select) = query.body.as_mut() {
        if select.from.len() == 1
            && select.from[0].joins.len() == 1
            && matches!(
                select.from[0].joins[0].join_operator,
                JoinOperator::CrossJoin(_)
            )
            && is_lateral_item(&select.from[0].joins[0].relation)
        {
            let join = select.from[0].joins.pop().expect("checked above");
            select.from.push(TableWithJoins {
                relation: join.relation,
                joins: Vec::new(),
            });
        }
        // `meta.title AS title` names what `meta.title` already names.
        for item in &mut select.projection {
            if let SelectItem::ExprWithAlias { expr, alias } = item
                && column_ref_name(expr).as_deref() == Some(alias.value.as_str())
            {
                *item = SelectItem::UnnamedExpr(expr.clone());
            }
        }
        if let Some(item) = select.from.get_mut(1) {
            // `LATERAL f(x) AS c` and `f(x) AS c` are one relation: a function
            // in FROM position is lateral by nature.
            if let TableFactor::Function {
                name, args, alias, ..
            } = &item.relation
            {
                item.relation = TableFactor::Table {
                    name: name.clone(),
                    alias: alias.clone(),
                    args: Some(TableFunctionArgs {
                        args: args.clone(),
                        settings: None,
                    }),
                    with_hints: Vec::new(),
                    version: None,
                    with_ordinality: false,
                    partitions: Vec::new(),
                    json_path: None,
                    sample: None,
                    index_hints: Vec::new(),
                };
            }
            // `UNNEST(c) e` and `f(x) e` take `AS`.
            match &mut item.relation {
                TableFactor::UNNEST {
                    alias: Some(alias), ..
                }
                | TableFactor::Table {
                    alias: Some(alias), ..
                } => alias.explicit = true,
                _ => {}
            }
        }
    }
    query
}

fn is_lateral_item(factor: &TableFactor) -> bool {
    matches!(
        factor,
        TableFactor::UNNEST { .. }
            | TableFactor::Function { .. }
            | TableFactor::Table { args: Some(_), .. }
    )
}

fn single_name(name: &ObjectName, what: &str) -> Result<String> {
    match name.0.as_slice() {
        [ObjectNamePart::Identifier(ident)] => Ok(ident.value.clone()),
        _ => Err(invalid(format!(
            "{what} must be a single name, not '{name}'"
        ))),
    }
}

fn extract(query: &Query) -> Result<MaterializedViewDefinition> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(invalid(format!("expected a SELECT; {SHAPE}")));
    };

    let mut from = select.from.iter();
    let (source_namespace, source_table) = match from.next().map(|f| &f.relation) {
        Some(TableFactor::Table { args: Some(_), .. }) => {
            return Err(invalid(
                "a view reads a table; a Function in FROM position follows it: \
                 `FROM table, function(args) AS alias`",
            ));
        }
        Some(TableFactor::Table {
            alias: Some(alias), ..
        }) => {
            return Err(invalid(format!(
                "table aliases are not supported (`AS {}`); refer to columns unqualified",
                alias.name
            )));
        }
        Some(TableFactor::Table { name, .. }) => {
            let mut parts = Vec::with_capacity(name.0.len());
            for part in &name.0 {
                match part {
                    ObjectNamePart::Identifier(ident) => parts.push(ident.value.clone()),
                    other => return Err(invalid(format!("unsupported table name part '{other}'"))),
                }
            }
            let table = parts.pop().ok_or_else(|| invalid("empty table name"))?;
            (parts, table)
        }
        _ => return Err(invalid(format!("the view must read one table; {SHAPE}"))),
    };
    let lateral = match from.next().map(|f| &f.relation) {
        None => None,
        Some(TableFactor::UNNEST {
            alias, array_exprs, ..
        }) => {
            let column = match array_exprs.as_slice() {
                [Expr::Identifier(ident)] => ident.value.clone(),
                _ => {
                    return Err(invalid(
                        "UNNEST takes one top-level list column of the table",
                    ));
                }
            };
            let alias = alias
                .as_ref()
                .ok_or_else(|| invalid("UNNEST needs an alias: `UNNEST(column) AS alias`"))?;
            Some(ViewLateral {
                source: LateralSource::Unnest { column },
                alias: alias.name.value.clone(),
            })
        }
        Some(TableFactor::Table {
            name,
            args: Some(TableFunctionArgs { args, .. }),
            alias,
            ..
        }) => {
            let function = single_name(name, "a Function in FROM position")?;
            let mut rendered = Vec::with_capacity(args.len());
            for arg in args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        rendered.push(expr.to_string())
                    }
                    other => {
                        return Err(invalid(format!(
                            "'{function}' takes positional expression arguments, not '{other}'"
                        )));
                    }
                }
            }
            let alias = alias.as_ref().ok_or_else(|| {
                invalid(format!(
                    "'{function}' in FROM position needs an alias: `{function}(...) AS alias`"
                ))
            })?;
            Some(ViewLateral {
                source: LateralSource::Function {
                    name: function,
                    args: rendered,
                },
                alias: alias.name.value.clone(),
            })
        }
        Some(_) => return Err(invalid(format!("the view must read one table; {SHAPE}"))),
    };
    if from.next().is_some() {
        return Err(invalid(format!("the view must read one table; {SHAPE}")));
    }

    let mut projections = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) if select.projection.len() == 1 => {
                projections.push(ViewProjection::star());
            }
            SelectItem::Wildcard(_) => {
                return Err(invalid("`*` must be the only column selected"));
            }
            SelectItem::UnnamedExpr(expr) => {
                let output = column_ref_name(expr).ok_or_else(|| {
                    invalid(format!(
                        "view column `{expr}` needs a name: `{expr} AS name`"
                    ))
                })?;
                projections.push(ViewProjection {
                    output,
                    expression: expr.to_string(),
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => projections.push(ViewProjection {
                output: alias.value.clone(),
                expression: expr.to_string(),
            }),
            other => return Err(invalid(format!("unsupported select item '{other}'"))),
        }
    }

    let limit =
        match &query.limit_clause {
            None => None,
            Some(LimitClause::LimitOffset {
                limit: Some(Expr::Value(value)),
                offset: None,
                limit_by,
            }) if limit_by.is_empty() => match &value.value {
                Value::Number(n, _) => Some(n.parse::<u64>().map_err(|_| {
                    invalid(format!("view limit {n} is not a non-negative integer"))
                })?),
                _ => return Err(invalid("view limit must be an integer literal")),
            },
            Some(_) => return Err(invalid("view limit must be a plain `LIMIT n`")),
        };

    if select.having.is_some() {
        return Err(invalid("HAVING is not supported in a view query"));
    }
    let group_by = match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers)
            if modifiers.is_empty()
                && !exprs.iter().any(|e| {
                    matches!(e, Expr::Rollup(_) | Expr::Cube(_) | Expr::GroupingSets(_))
                }) =>
        {
            exprs.iter().map(|e| e.to_string()).collect()
        }
        _ => {
            return Err(invalid(
                "a view groups by a plain `GROUP BY expr, ...` list",
            ));
        }
    };

    let definition = MaterializedViewDefinition {
        source_table,
        source_namespace,
        lateral,
        projections,
        filter: select.selection.as_ref().map(|e| e.to_string()),
        group_by,
        limit,
    };
    check_grouping(&definition)?;
    Ok(definition)
}

/// The clauses a grouped view cannot combine with: each would have to be
/// maintained per group rather than per source row.
pub fn check_grouping(definition: &MaterializedViewDefinition) -> Result<()> {
    if !definition.is_grouped() {
        return Ok(());
    }
    if definition.lateral.is_some() {
        return Err(invalid(
            "GROUP BY cannot be combined with a FROM-position item; group in one view \
             and expand in a view over it",
        ));
    }
    if definition.selects_star() {
        return Err(invalid(
            "a grouped view selects its keys and aggregates, not `*`",
        ));
    }
    if definition.limit.is_some() {
        return Err(invalid("LIMIT is not supported together with GROUP BY"));
    }
    Ok(())
}

/// The canonical query for `definition`; [`parse`] reads it back equal.
pub fn render(definition: &MaterializedViewDefinition) -> String {
    let mut sql = String::from("SELECT ");
    if definition.selects_star() {
        sql.push('*');
    } else {
        let items: Vec<String> = definition
            .projections
            .iter()
            .map(|p| {
                let bare = Parser::new(&GenericDialect {})
                    .try_with_sql(&p.expression)
                    .and_then(|mut parser| parser.parse_expr())
                    .ok()
                    .and_then(|expr| column_ref_name(&expr))
                    .is_some_and(|name| name == p.output);
                if bare {
                    p.expression.clone()
                } else {
                    format!("{} AS {}", p.expression, ident_sql(&p.output))
                }
            })
            .collect();
        sql.push_str(&items.join(", "));
    }
    sql.push_str(" FROM ");
    let table: Vec<String> = definition
        .source_namespace
        .iter()
        .chain(std::iter::once(&definition.source_table))
        .map(|part| ident_sql(part))
        .collect();
    sql.push_str(&table.join("."));
    if let Some(lateral) = &definition.lateral {
        match &lateral.source {
            LateralSource::Unnest { column } => {
                sql.push_str(&format!(", UNNEST({})", ident_sql(column)))
            }
            LateralSource::Function { name, args } => {
                sql.push_str(&format!(", {}({})", ident_sql(name), args.join(", ")))
            }
        }
        sql.push_str(&format!(" AS {}", ident_sql(&lateral.alias)));
    }
    if let Some(filter) = &definition.filter {
        sql.push_str(&format!(" WHERE {filter}"));
    }
    if definition.is_grouped() {
        sql.push_str(&format!(" GROUP BY {}", definition.group_by.join(", ")));
    }
    if let Some(limit) = definition.limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    sql
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_query_round_trips_through_its_canonical_form() {
        for (sql, canonical) in [
            (
                r#"select "Name", x*2 as twice from ns.docs where x > 1 limit 5"#,
                "SELECT `Name`, x * 2 AS twice FROM ns.docs WHERE x > 1 LIMIT 5",
            ),
            (
                "SELECT id, c.chunk, c.ordinal + 1 AS nth FROM docs, UNNEST(chunks) AS c WHERE c.ordinal < 5",
                "SELECT id, c.chunk, c.ordinal + 1 AS nth FROM docs, UNNEST(chunks) AS c WHERE c.ordinal < 5",
            ),
            (
                "SELECT id FROM docs CROSS JOIN UNNEST(chunks) c",
                "SELECT id FROM docs, UNNEST(chunks) AS c",
            ),
            (
                "select d.id, c.text from docs, chunk(body, 512) c where c.ordinal < 3",
                "SELECT d.id, c.text FROM docs, chunk(body, 512) AS c WHERE c.ordinal < 3",
            ),
            (
                "SELECT id, c.text FROM docs CROSS JOIN LATERAL chunk(body) AS c",
                "SELECT id, c.text FROM docs, chunk(body) AS c",
            ),
            (
                "SELECT id, c.text FROM docs, LATERAL chunk(upper(body)) AS c",
                "SELECT id, c.text FROM docs, chunk(upper(body)) AS c",
            ),
            ("SELECT * FROM `select`.t", "SELECT * FROM `select`.t"),
            (
                "select k, array_agg(id) as ids, count(*) n from t where x > 1 group by k",
                "SELECT k, array_agg(id) AS ids, count(*) AS n FROM t WHERE x > 1 GROUP BY k",
            ),
            (
                "SELECT meta.title AS title, id AS id FROM t",
                "SELECT meta.title, id FROM t",
            ),
        ] {
            let definition = parse(sql).unwrap();
            assert_eq!(render(&definition), canonical, "{sql}");
            assert_eq!(parse(canonical).unwrap(), definition, "{sql}");
        }
    }

    #[test]
    fn parsed_parts_are_the_relational_shape() {
        let definition =
            parse("SELECT id, c.chunk FROM ns.docs, UNNEST(chunks) AS c WHERE id > 1").unwrap();
        assert_eq!(definition.source_namespace, ["ns"]);
        assert_eq!(definition.source_table, "docs");
        assert_eq!(
            definition.lateral,
            Some(ViewLateral {
                source: LateralSource::Unnest {
                    column: "chunks".into()
                },
                alias: "c".into()
            })
        );
        let function = parse("SELECT id, c.text FROM docs, chunk(body, 512) AS c").unwrap();
        assert_eq!(
            function.lateral,
            Some(ViewLateral {
                source: LateralSource::Function {
                    name: "chunk".into(),
                    args: vec!["body".into(), "512".into()]
                },
                alias: "c".into()
            })
        );
        assert_eq!(
            definition.projections,
            [
                ViewProjection {
                    output: "id".into(),
                    expression: "id".into()
                },
                ViewProjection {
                    output: "chunk".into(),
                    expression: "c.chunk".into()
                },
            ]
        );
        assert_eq!(definition.filter.as_deref(), Some("id > 1"));
        assert!(parse("SELECT * FROM t").unwrap().selects_star());
    }

    /// A clause the engine cannot maintain is refused, never dropped.
    #[test]
    fn unsupported_clauses_are_refused() {
        for sql in [
            "SELECT id FROM t ORDER BY id",
            "SELECT DISTINCT id FROM t",
            "SELECT id FROM t LIMIT 5 OFFSET 2",
            "SELECT id FROM t JOIN u ON t.id = u.id",
            "SELECT id FROM t, u",
            "SELECT id FROM t, UNNEST(c)",
            "SELECT id FROM t, UNNEST(a.b) AS c",
            "SELECT id FROM t, chunk(body)",
            "SELECT id FROM t, ns.chunk(body) AS c",
            "SELECT id FROM t, chunk(size => 5) AS c",
            "SELECT id FROM t AS d, chunk(d.body) AS c",
            "SELECT id FROM chunk(body) AS c",
            "SELECT id FROM t, chunk(body) AS c, UNNEST(x) AS u",
            "SELECT count(*) FROM t",
            "SELECT x * 2 FROM t",
            "SELECT id FROM t; SELECT id FROM t",
            "SELECT *, id FROM t",
            "WITH q AS (SELECT 1) SELECT id FROM t",
            "SELECT id FROM t HAVING id > 1",
            "SELECT k, count(*) AS n FROM t GROUP BY k HAVING count(*) > 1",
            "SELECT k FROM t GROUP BY ALL",
            "SELECT k FROM t GROUP BY ROLLUP (k)",
            "SELECT * FROM t GROUP BY k",
            "SELECT k FROM t GROUP BY k LIMIT 5",
            "SELECT k, c.x FROM t, UNNEST(xs) AS c GROUP BY k",
        ] {
            assert!(parse(sql).is_err(), "{sql}");
        }
    }

    /// The canonical spelling is what lance's planner reads back, for the
    /// expression forms a view is likely to carry.
    #[test]
    fn canonical_expressions_plan_in_lance() {
        use arrow_schema::{DataType, Field, Schema};

        let planner = Planner::new(std::sync::Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("Name", DataType::Utf8, true),
            Field::new(
                "when",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "meta",
                DataType::Struct(vec![Field::new("title", DataType::Utf8, true)].into()),
                true,
            ),
        ])));
        for (raw, canonical) in [
            ("x*2+1", "x * 2 + 1"),
            (r#"CAST(x as  bigint)"#, "CAST(x AS BIGINT)"),
            (r#"upper("Name") like 'A%'"#, "upper(`Name`) LIKE 'A%'"),
            (
                "x is not null and x between 1 and 3",
                "x IS NOT NULL AND x BETWEEN 1 AND 3",
            ),
            ("meta.title", "meta.title"),
            (r#"`Name` = 'it''s'"#, "`Name` = 'it''s'"),
            ("x in (1, 2)", "x IN (1, 2)"),
            (
                "`when` > timestamp '2024-01-01'",
                "when > TIMESTAMP '2024-01-01'",
            ),
            ("-x", "-x"),
        ] {
            let text = canonical_expr(raw).unwrap();
            assert_eq!(text, canonical, "{raw}");
            let expr = planner
                .parse_expr(&text)
                .unwrap_or_else(|e| panic!("{text}: {e}"));
            planner
                .optimize_expr(expr)
                .unwrap_or_else(|e| panic!("{text}: {e}"));
        }
    }

    #[test]
    fn identifiers_are_delimited_only_when_the_parser_needs_it() {
        assert_eq!(ident_sql("name"), "name");
        assert_eq!(ident_sql("Name"), "`Name`");
        assert_eq!(ident_sql("select"), "`select`");
        assert_eq!(ident_sql("1st"), "`1st`");
        assert_eq!(ident_sql("a`b"), "`a``b`");
        assert_eq!(canonical_expr(r#""Party" = 'D'"#).unwrap(), "`Party` = 'D'");
        assert_eq!(canonical_expr("`name`").unwrap(), "name");
    }
}
