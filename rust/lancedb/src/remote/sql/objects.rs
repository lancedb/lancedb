// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::SqlClient;
use crate::{
    database::ExecuteQueryRequest,
    error::{Error, Result},
    job::Job,
};
use arrow_array::{Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;

pub(super) fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
pub(super) fn strings(batches: &[RecordBatch], name: &str) -> Result<Vec<String>> {
    batches
        .iter()
        .map(|batch| {
            let values = batch
                .column_by_name(name)
                .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| Error::Runtime {
                    message: format!("SQL result is missing UTF-8 column '{name}'"),
                })?;
            if values.null_count() != 0 {
                return Err(Error::Runtime {
                    message: format!("SQL result contains null '{name}'"),
                });
            }
            Ok(values
                .iter()
                .map(|value| value.unwrap().to_owned())
                .collect::<Vec<_>>())
        })
        .collect::<Result<Vec<_>>>()
        .map(|rows| rows.into_iter().flatten().collect())
}
pub(super) fn definition<T: DeserializeOwned>(batches: Vec<RecordBatch>) -> Result<T> {
    let values = strings(&batches, "definition")?;
    let [value]: [String; 1] = values.try_into().map_err(|_| Error::Runtime {
        message: "SQL definition command must return exactly one row".into(),
    })?;
    serde_json::from_str(&value).map_err(|error| Error::Runtime {
        message: format!("invalid SQL definition: {error}"),
    })
}
impl SqlClient {
    pub(super) async fn object_job<T, F>(
        &self,
        request: ExecuteQueryRequest,
        decode: F,
    ) -> Result<Job<T>>
    where
        T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
        F: FnOnce(Vec<RecordBatch>) -> Result<T> + Send + 'static,
    {
        let query = Arc::new(self.execute(request).await?);
        let id = query.id();
        let execution = query.clone();
        let task = tokio::spawn(async move {
            let reader = execution.reader().await?;
            decode(reader.try_collect().await?)
        });
        Ok(Job::from_query_task(id, task, move || {
            let query = query.clone();
            async move { query.cancel().await }
        }))
    }
}
