// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::FixedSizeListArray;
use arrow_schema::Schema;
use datafusion_physical_plan::ExecutionPlan;

use crate::error::{Error, Result};
use crate::table::NativeTable;
use crate::table::datafusion::knn::{MergeKnnExec, PartialKnnExec};
use crate::utils::default_vector_column;

/// Convenience alias — this module operates on the native (local) table type.
type LanceTable = NativeTable;

/// Build an [`ExecutionPlan`] that finds the `k` nearest neighbors for every
/// vector in `query_vectors`.
///
/// Output schema: table columns + `query_index: Int32` + `_distance: Float32`.
/// Results are grouped by `query_index` (0-based position in `query_vectors`)
/// with at most `k` rows per group, sorted ascending by `_distance`.
async fn batch_knn(
    table: &LanceTable,
    query_vectors: FixedSizeListArray,
    k: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let ds_ref = table.dataset.get().await?;
    let arrow_schema = Schema::from(ds_ref.schema());

    // Auto-detect the vector column by matching the query dimension.
    let dim = query_vectors.value_length();
    let column = default_vector_column(&arrow_schema, Some(dim))?;

    let scan_plan = ds_ref
        .scan()
        .create_plan()
        .await
        .map_err(|e| Error::Runtime {
            message: e.to_string(),
        })?;

    let partial = PartialKnnExec::try_new(scan_plan, column, query_vectors, k).map_err(|e| {
        Error::Runtime {
            message: e.to_string(),
        }
    })?;

    let merged =
        MergeKnnExec::try_new(Arc::new(partial) as Arc<dyn ExecutionPlan>, k).map_err(|e| {
            Error::Runtime {
                message: e.to_string(),
            }
        })?;

    Ok(Arc::new(merged) as Arc<dyn ExecutionPlan>)
}
