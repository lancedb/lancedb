// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Options, previews, and results for data-cache prewarm.

/// Data prewarm settings for [`super::Table::prewarm_data_async`] and
/// [`super::Table::explain_prewarm_data`].
///
/// The table handle supplies the database, namespace, and branch. Omitted
/// columns warm all fields. The filter selects conservative metadata ranges;
/// it does not execute a row-filter scan or change the source data.
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct PrewarmDataOptions {
    /// Lance field paths, including nested paths such as `attrs.score`.
    pub columns: Vec<String>,
    /// An optional SQL predicate for metadata pruning.
    pub filter: Option<String>,
    /// One row of typed filter values. Column i binds `$<i + 1>` and a
    /// nonnumeric column name also binds `$name`. Requires server support.
    pub parameters: Option<arrow_array::RecordBatch>,
    /// Positive per-executor warming concurrency. Omit for the server default.
    pub concurrency: Option<u32>,
}

impl std::fmt::Debug for PrewarmDataOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrewarmDataOptions")
            .field("columns", &self.columns)
            .field("filter", &self.filter)
            .field("concurrency", &self.concurrency)
            .field(
                "parameter_count",
                &self.parameters.as_ref().map(|batch| batch.num_columns()),
            )
            .finish()
    }
}

impl PrewarmDataOptions {
    /// Use server defaults and warm every column.
    pub fn new() -> Self {
        Self::default()
    }

    /// Select fields to warm. An empty collection selects all columns.
    pub fn columns(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.columns = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Restrict warming using a single SQL predicate for metadata pruning.
    pub fn filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Bind scalar filter values without interpolating them into SQL text.
    ///
    /// ```
    /// # use lancedb::table::PrewarmDataOptions;
    /// # use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    /// # use std::sync::Arc;
    /// let values = RecordBatch::try_from_iter(vec![(
    ///     "minimum", Arc::new(Int64Array::from(vec![100])) as ArrayRef,
    /// )]).unwrap();
    /// let options = PrewarmDataOptions::new()
    ///     .columns(["payload"])
    ///     .filter("id >= $minimum")
    ///     .parameters(values);
    /// ```
    pub fn parameters(mut self, parameters: arrow_array::RecordBatch) -> Self {
        self.parameters = Some(parameters);
        self
    }

    /// Set positive per-executor warming concurrency.
    pub fn concurrency(mut self, concurrency: u32) -> Self {
        self.concurrency = Some(concurrency);
        self
    }
}

/// Metadata-only preview of a data-cache prewarm on one table snapshot.
///
/// No warming job is submitted. A later submission chooses its own snapshot and
/// may produce a different plan if the table or index metadata has changed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct PrewarmDataPlan {
    /// Source snapshot used to produce this preview.
    pub table_version: u64,
    /// Human-readable plan, intended for display rather than parsing.
    pub plan: String,
    /// Conservative metadata pruning, not exact row-filter matches.
    pub pruning: PrewarmPruningSummary,
}

/// Conservative pruning facts for a PREWARM preview.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct PrewarmPruningSummary {
    /// Proof mode, such as `unfiltered`, `constant`, `zonemap_ranges`, or
    /// `full_warm_fallback`. Future servers may return additional modes.
    pub mode: String,
    /// Fragments in the source snapshot.
    pub input_fragments: u64,
    /// Fragments selected for warming.
    pub selected_fragments: u64,
    /// Fragments excluded by metadata proof.
    pub pruned_fragments: u64,
    /// Candidate physical intervals, when known; not an exact matching-row count.
    pub selected_row_ranges: Option<u64>,
    /// Why metadata could not prune, when reported.
    pub fallback_reason: Option<String>,
}

/// Reported outcome of a remote data-prewarm job. Counts describe executor work;
/// absent measurements remain unknown rather than being inferred as zero.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct PrewarmDataResult {
    /// Source snapshot captured by the submitted prewarm.
    pub table_version: u64,
    /// Reported executors targeted; unknown on older workers.
    pub executors_targeted: Option<u64>,
    /// Executors that reached a successful or partial terminal state.
    pub executors_completed: Option<u64>,
    /// Subset of completed executors that reported partial warming.
    pub executors_partial: Option<u64>,
    /// Reported fragments selected; unknown on older workers.
    pub fragments_selected: Option<u64>,
    /// Reported ranges completed; unknown on older workers.
    pub ranges_completed: Option<u64>,
    /// Reported bytes read; unknown on older workers.
    pub bytes_read: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn result_preserves_zero_partial_and_unknown_measurements() {
        let result: PrewarmDataResult = serde_json::from_value(serde_json::json!({
            "table_version":42,"executors_targeted":2,"executors_completed":2,
            "executors_partial":1,"ranges_completed":0,"extra":"ignored",
        }))
        .unwrap();
        assert_eq!(result.executors_partial, Some(1));
        assert_eq!(result.ranges_completed, Some(0));
        assert_eq!(result.bytes_read, None);
        assert!(serde_json::from_value::<PrewarmDataResult>(serde_json::json!({})).is_err());
        assert!(
            serde_json::from_value::<PrewarmDataResult>(
                serde_json::json!({"table_version":42,"bytes_read":-1})
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn local_table_rejects_durable_data_prewarm() {
        let db = crate::connect(&format!("memory://prewarm-{}", uuid::Uuid::new_v4()))
            .execute()
            .await
            .unwrap();
        let table = db
            .create_empty_table(
                "items",
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            )
            .execute()
            .await
            .unwrap();
        let error = table
            .prewarm_data_async(PrewarmDataOptions::new().filter("id > 0"))
            .await
            .unwrap_err();
        let preview_error = table
            .explain_prewarm_data(Default::default())
            .await
            .unwrap_err();
        assert!(matches!(preview_error, crate::Error::NotSupported { .. }));
        assert!(matches!(error, crate::Error::NotSupported { .. }));
        assert!(
            error
                .to_string()
                .contains("only supported on remote tables")
        );
    }
}
