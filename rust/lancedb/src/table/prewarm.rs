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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

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
