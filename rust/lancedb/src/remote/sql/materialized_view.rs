// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::TryStreamExt;

use super::SqlClient;
use crate::arrow::SendableRecordBatchStream;
use crate::database::ExecuteQueryRequest;
use crate::error::{Error, Result};
use crate::listing::{ListingOptions, ListingPage};
use crate::materialized_view::{MaterializedViewDefinition, MaterializedViewInfo};
use crate::sql::Query;
use crate::utils::{validate_namespace, validate_table_name};

pub(in crate::remote) fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn strings(batch: &RecordBatch, name: &str) -> Result<Vec<String>> {
    let values = batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| Error::Runtime {
            message: format!("materialized-view SQL result is missing UTF-8 column '{name}'"),
        })?;
    if values.null_count() != 0 {
        return Err(Error::Runtime {
            message: format!("materialized-view SQL result contains null '{name}'"),
        });
    }
    Ok(values
        .iter()
        .map(|value| value.unwrap().to_owned())
        .collect())
}

pub(in crate::remote) enum MaterializedViewJobSubmission {
    AlreadyExists,
    Registry { job_id: String },
    Query(Arc<Query>),
}

impl SqlClient {
    pub(in crate::remote) async fn submit_materialized_view_job(
        &self,
        statement: String,
        namespace: Vec<String>,
    ) -> Result<MaterializedViewJobSubmission> {
        let submitted = self
            .execute_with_materialized_view_job_handle(
                ExecuteQueryRequest {
                    query: statement,
                    default_namespace_path: namespace,
                    parameters: None,
                },
                true,
            )
            .await?;
        let query = Arc::new(submitted.query);
        if !submitted.materialized_view_job_handle_supported {
            return Ok(MaterializedViewJobSubmission::Query(query));
        }
        let mut reader: SendableRecordBatchStream = query.reader().await?;
        let mut rows = 0;
        let mut registry_job_id = None;
        let mut already_exists = false;
        while let Some(batch) = reader.try_next().await? {
            rows += batch.num_rows();
            if rows > 1 {
                return Err(Error::Runtime {
                    message: "materialized-view submission must return exactly one status row"
                        .into(),
                });
            }
            if let Some(column) = batch.column_by_name("status") {
                let values = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| Error::Runtime {
                        message: "materialized-view submission status is not UTF-8".into(),
                    })?;
                already_exists =
                    values.len() == 1 && !values.is_null(0) && values.value(0) == "already_exists";
            }
            if let Some(column) = batch.column_by_name("job_id") {
                let values = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| Error::Runtime {
                        message: "materialized-view submission job_id is not UTF-8".into(),
                    })?;
                if values.null_count() != 0 {
                    return Err(Error::Runtime {
                        message: "materialized-view submission contains a null job_id".into(),
                    });
                }
                if values.is_empty() {
                    continue;
                }
                let value = values.value(0);
                if !value.is_empty() {
                    registry_job_id = Some(value.to_string());
                }
            }
        }
        if rows != 1 {
            return Err(Error::Runtime {
                message: "materialized-view submission must return exactly one status row".into(),
            });
        }
        if already_exists && registry_job_id.is_none() {
            return Ok(MaterializedViewJobSubmission::AlreadyExists);
        }
        let job_id = registry_job_id.ok_or_else(|| Error::Runtime {
            message: "server acknowledged an early materialized-view handle but returned no job_id"
                .into(),
        })?;
        Ok(MaterializedViewJobSubmission::Registry { job_id })
    }

    pub(in crate::remote) async fn describe_materialized_view(
        &self,
        name: &str,
        namespace: &[String],
    ) -> Result<MaterializedViewInfo> {
        validate_table_name(name)?;
        validate_namespace(namespace)?;
        let query = self
            .execute(ExecuteQueryRequest {
                query: format!("DESCRIBE MATERIALIZED VIEW {}", quote_identifier(name)),
                default_namespace_path: namespace.to_vec(),
                parameters: None,
            })
            .await?;
        let mut reader = query.reader().await?;
        let mut definitions = Vec::new();
        while let Some(batch) = reader.try_next().await? {
            definitions.extend(strings(&batch, "query")?);
        }
        let [definition_sql] =
            <[String; 1]>::try_from(definitions).map_err(|_| Error::Runtime {
                message: "DESCRIBE MATERIALIZED VIEW must return exactly one definition".into(),
            })?;
        let parsed_definition = MaterializedViewDefinition::from_sql(&definition_sql).ok();
        Ok(MaterializedViewInfo {
            definition_sql,
            parsed_definition,
        })
    }

    pub(in crate::remote) async fn list_materialized_views(
        &self,
        namespace: &[String],
        options: ListingOptions,
    ) -> Result<ListingPage<String>> {
        validate_namespace(namespace)?;
        let limit = options.page_limit.unwrap_or(100);
        if limit == 0 || limit > i32::MAX as u32 {
            return Err(Error::InvalidInput {
                message: "page limit must be between 1 and i32::MAX".into(),
            });
        }
        let invalid_token = || Error::InvalidInput {
            message: "invalid materialized-view page token for this database or namespace".into(),
        };
        let after = if let Some(token) = options.page_token.filter(|token| !token.is_empty()) {
            let decoded = URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid_token())?;
            let (database, prefix, path, last): (String, Option<String>, Vec<String>, String) =
                serde_json::from_slice(&decoded).map_err(|_| invalid_token())?;
            if database != self.inner.database
                || prefix != self.inner.database_prefix
                || path != namespace
            {
                return Err(invalid_token());
            }
            format!(" WHERE name > '{}'", last.replace('\'', "''"))
        } else {
            String::new()
        };
        let query = self
            .execute(ExecuteQueryRequest {
                query: format!(
                    "SHOW MATERIALIZED VIEWS{after} LIMIT {}",
                    u64::from(limit) + 1
                ),
                default_namespace_path: namespace.to_vec(),
                parameters: None,
            })
            .await?;
        let mut reader = query.reader().await?;
        let mut items = Vec::new();
        while let Some(batch) = reader.try_next().await? {
            items.extend(strings(&batch, "name")?);
        }
        let page_token = if items.len() > limit as usize {
            items.truncate(limit as usize);
            let cursor = serde_json::to_vec(&(
                &self.inner.database,
                &self.inner.database_prefix,
                namespace,
                items.last().unwrap(),
            ))
            .map_err(|error| Error::Runtime {
                message: error.to_string(),
            })?;
            Some(URL_SAFE_NO_PAD.encode(cursor))
        } else {
            None
        };
        Ok(ListingPage { items, page_token })
    }
}
