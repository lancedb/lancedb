// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! User-Defined Table Functions (UDTFs) for DataFusion integration
//!
//! This module provides SQL table functions for LanceDB search capabilities:
//! - `fts(table_name, query_json)` — full-text search
//! - `vector_search(table_name, query_vector_json, top_k)` — vector similarity search
//! - `hybrid_search(table_name, query_vector_json, fts_query_json, top_k)` — combined search

pub mod fts;
pub mod hybrid_search;
pub mod vector_search;

use std::sync::Arc;

use datafusion_catalog::TableProvider;
use datafusion_common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion_expr::Expr;
use lance_index::scalar::FullTextSearchQuery;

use super::VectorSearchParams;

/// Describes the type of search to apply when resolving a table.
#[derive(Debug, Clone)]
pub enum SearchQuery {
    /// Full-text search only
    Fts(FullTextSearchQuery),
    /// Vector similarity search only
    Vector(VectorSearchParams),
    /// Hybrid search combining FTS and vector search
    Hybrid {
        fts: FullTextSearchQuery,
        vector: VectorSearchParams,
    },
}

/// Trait for resolving table names to TableProvider instances, optionally with a search query.
pub trait TableResolver: std::fmt::Debug + Send + Sync {
    /// Resolve a table name to a TableProvider, optionally applying a search query.
    fn resolve_table(
        &self,
        name: &str,
        search: Option<SearchQuery>,
    ) -> DataFusionResult<Arc<dyn TableProvider>>;
}

/// Extract a string literal from a DataFusion expression.
pub(crate) fn extract_string_literal(expr: &Expr, param_name: &str) -> DataFusionResult<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Ok(s.clone()),
        Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Ok(s.clone()),
        _ => Err(DataFusionError::Plan(format!(
            "Parameter '{}' must be a string literal, got: {:?}",
            param_name, expr
        ))),
    }
}

/// Extract an integer literal from a DataFusion expression.
pub(crate) fn extract_int_literal(expr: &Expr, param_name: &str) -> DataFusionResult<usize> {
    match expr {
        Expr::Literal(ScalarValue::Int8(Some(v)), _) => Ok(*v as usize),
        Expr::Literal(ScalarValue::Int16(Some(v)), _) => Ok(*v as usize),
        Expr::Literal(ScalarValue::Int32(Some(v)), _) => Ok(*v as usize),
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => Ok(*v as usize),
        Expr::Literal(ScalarValue::UInt8(Some(v)), _) => Ok(*v as usize),
        Expr::Literal(ScalarValue::UInt16(Some(v)), _) => Ok(*v as usize),
        Expr::Literal(ScalarValue::UInt32(Some(v)), _) => Ok(*v as usize),
        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => Ok(*v as usize),
        _ => Err(DataFusionError::Plan(format!(
            "Parameter '{}' must be an integer literal, got: {:?}",
            param_name, expr
        ))),
    }
}
