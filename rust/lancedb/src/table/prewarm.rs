// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Options for submitting a durable data-cache prewarm job.

/// Data prewarm settings for [`super::Table::prewarm_data_async`].
///
/// The table handle supplies the database, namespace, and branch. Omitted
/// columns warm all fields. The filter selects conservative metadata ranges;
/// it does not execute a row-filter scan or change the source data.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct PrewarmDataOptions {
    /// Lance field paths, including nested paths such as `attrs.score`.
    pub columns: Vec<String>,
    /// An optional SQL predicate for metadata pruning.
    pub filter: Option<String>,
    /// Positive per-executor warming concurrency. Omit for the server default.
    pub concurrency: Option<u32>,
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

    /// Set positive per-executor warming concurrency.
    pub fn concurrency(mut self, concurrency: u32) -> Self {
        self.concurrency = Some(concurrency);
        self
    }
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
        assert!(matches!(error, crate::Error::NotSupported { .. }));
        assert!(
            error
                .to_string()
                .contains("only supported on remote tables")
        );
    }
}
