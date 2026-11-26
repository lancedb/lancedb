// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{collections::HashMap, ops::ControlFlow, sync::Arc};

use arrow_array::{cast::AsArray, types::UInt64Type};
use datafusion::{
    dataframe::DataFrame,
    execution::SessionStateBuilder,
    prelude::SessionContext,
    sql::sqlparser::{
        ast::{Expr, Ident, visit_expressions},
        dialect::GenericDialect,
        parser::Parser,
        tokenizer::Token,
    },
};
use datafusion_common::TableReference;
use futures::TryStreamExt;
use lance::{
    Dataset,
    datafusion::LanceTableProvider,
    dataset::scanner::{RowAddrMask, RowAddrTreeMap},
};
use lance_core::ROW_ID;

use crate::{
    Error, Result,
    database::OpenTableRequest,
    query::{QueryDatabase, QueryFilter, QueryRequest},
    table::{NativeTable, datafusion::BaseTableAdapter},
};

/// Leave ordinary SQL filters to Lance, including literals containing SQL keywords.
pub(super) fn predicate(filter: Option<&QueryFilter>) -> Option<Expr> {
    let Some(QueryFilter::Sql(sql)) = filter else {
        return None;
    };
    if !sql
        .as_bytes()
        .windows(6)
        .any(|word| word.eq_ignore_ascii_case(b"select"))
    {
        return None;
    }

    let mut parser = Parser::new(&GenericDialect {}).try_with_sql(sql).ok()?;
    let expr = parser.parse_expr().ok()?;
    if parser.peek_token().token != Token::EOF {
        return None;
    }
    visit_expressions(&expr, |expr| match expr {
        Expr::InSubquery { .. } => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    })
    .is_break()
    .then_some(expr)
}

pub(super) async fn row_mask(
    table: &NativeTable,
    dataset: Arc<Dataset>,
    query: &QueryRequest,
    predicate: Expr,
) -> Result<RowAddrMask> {
    if !query.prefilter {
        return Err(Error::NotSupported {
            message: "SQL subquery filters require prefiltering.".into(),
        });
    }
    let Some(QueryDatabase(database)) = &query.database else {
        return Err(Error::NotSupported {
            message: "SQL subquery filters require a table opened through a database connection."
                .into(),
        });
    };

    // Only expose tables resolved through the originating connection. Use the
    // same snapshot for the outer scan and the ranked scan so their row IDs agree.
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new_with_default_features()
            .with_table_functions(HashMap::new())
            .build(),
    );
    let state = ctx.state();
    let name = Ident::with_quote('`', &table.name);
    let sql = format!("SELECT {name}.{ROW_ID} FROM {name} WHERE {predicate}");
    let statement = state.sql_to_statement(&sql, &Default::default())?;
    ctx.register_table(
        TableReference::bare(table.name.clone()),
        Arc::new(LanceTableProvider::new_with_ordering(
            dataset, true, false, false,
        )),
    )?;
    for reference in state.resolve_table_references(&statement)? {
        let TableReference::Bare { table: name } = reference else {
            return Err(Error::NotSupported {
                message: "Use unqualified table names in SQL subquery filters.".into(),
            });
        };
        if name.as_ref() == table.name {
            continue;
        }
        let source = database
            .open_table(OpenTableRequest {
                name: name.to_string(),
                namespace_path: table.namespace.clone(),
                index_cache_size: None,
                lance_read_params: None,
                location: None,
                namespace_client: None,
                managed_versioning: None,
            })
            .await?;
        let provider = BaseTableAdapter::try_new(source).await?;
        ctx.register_table(TableReference::bare(name), Arc::new(provider))?;
    }

    let state = ctx.state();
    let plan = state.statement_to_plan(statement).await?;
    let mut batches = DataFrame::new(state, plan).execute_stream().await?;
    let mut rows = RowAddrTreeMap::default();
    while let Some(batch) = batches.try_next().await? {
        rows.extend(
            batch
                .column(0)
                .as_primitive::<UInt64Type>()
                .iter()
                .flatten(),
        );
    }
    Ok(RowAddrMask::from_allowed(rows))
}
