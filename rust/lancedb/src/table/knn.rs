// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::FixedSizeListArray;
use datafusion::prelude::SessionContext;

use crate::DistanceType;
use crate::error::Result;
use crate::table::NativeTable;
use crate::table::datafusion::BaseTableAdapter;
use crate::table::datafusion::knn_ext::{DataFrameKnnExt, register_knn_planner};

/// Build a brute-force KNN DataFrame over all rows in `table`.
///
/// Returns a [`datafusion::dataframe::DataFrame`] with the table's columns
/// plus `query_index: Int32` and `_distance: Float32`, containing at most `k`
/// rows per query vector sorted by `(query_index ASC, _distance ASC)`.
///
/// The caller can chain additional DataFrame operations before collecting.
pub async fn batch_knn(
    table: &NativeTable,
    column: impl Into<String>,
    query_vectors: Arc<FixedSizeListArray>,
    k: usize,
    distance_type: DistanceType,
) -> Result<datafusion::dataframe::DataFrame> {
    let state = register_knn_planner(SessionContext::new().state());
    let ctx = SessionContext::new_with_state(state);

    // NativeTable implements BaseTable — coerce via explicit type on binding.
    let base: Arc<dyn crate::table::BaseTable> = Arc::new(table.clone());
    let adapter = BaseTableAdapter::try_new(base).await?;
    let provider: Arc<dyn datafusion_catalog::TableProvider> = Arc::new(adapter);
    ctx.register_table("_knn_source", provider)
        .map_err(|e| crate::error::Error::Runtime {
            message: e.to_string(),
        })?;

    let df = ctx
        .table("_knn_source")
        .await
        .map_err(|e| crate::error::Error::Runtime {
            message: e.to_string(),
        })?;

    df.knn(column, query_vectors, k, distance_type)
        .map_err(|e| crate::error::Error::Runtime {
            message: e.to_string(),
        })
}
