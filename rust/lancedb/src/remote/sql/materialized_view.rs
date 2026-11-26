// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use futures::TryStreamExt;

use super::SqlClient;
use super::receipt::ReceiptRow;
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
                    options: Default::default(),
                },
                true,
            )
            .await?;
        if !submitted.materialized_view_job_handle_supported {
            return Ok(MaterializedViewJobSubmission::Query(Arc::new(
                submitted.query,
            )));
        }
        let row = ReceiptRow::read(&submitted.query)
            .await?
            .ok_or_else(|| Error::Runtime {
                message: "materialized-view submission must return exactly one status row".into(),
            })?;
        let already_exists = if let Some(column) = row.column("status") {
            let status = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Runtime {
                    message: "materialized-view submission status is not UTF-8".into(),
                })?;
            !status.is_null(0) && status.value(0) == "already_exists"
        } else {
            false
        };
        match row.job_id()? {
            Some(id) => Ok(MaterializedViewJobSubmission::Registry {
                job_id: id.to_owned(),
            }),
            None if already_exists => Ok(MaterializedViewJobSubmission::AlreadyExists),
            None => Err(Error::Runtime {
                message:
                    "server acknowledged an early materialized-view handle but returned no job_id"
                        .into(),
            }),
        }
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
                options: Default::default(),
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
        self.list_names("SHOW MATERIALIZED VIEWS", namespace, options)
            .await
    }
}
