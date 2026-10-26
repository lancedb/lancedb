// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::{
    SqlClient,
    materialized_view::quote_identifier,
    objects::{definition, literal, strings},
};
use crate::{
    database::ExecuteQueryRequest,
    error::{Error, Result},
    function::{FunctionRegistrationRequest, FunctionVersion},
    job::Job,
    listing::{ListingOptions, ListingPage},
    utils::{validate_function_name, validate_namespace},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

impl SqlClient {
    pub(in crate::remote) async fn register_function(
        &self,
        request: FunctionRegistrationRequest,
        namespace: &[String],
    ) -> Result<Job<FunctionVersion>> {
        validate_function_name(&request.name)?;
        validate_namespace(namespace)?;
        let name = quote_identifier(&request.name);
        let mut body = serde_json::to_value(request).map_err(|error| Error::Runtime {
            message: error.to_string(),
        })?;
        body.as_object_mut().unwrap().remove("name");
        self.object_job(
            ExecuteQueryRequest {
                query: format!("CREATE FUNCTION {name} FROM {}", literal(&body.to_string())),
                default_namespace_path: namespace.to_vec(),
                parameters: None,
            },
            definition,
        )
        .await
    }
    pub(in crate::remote) async fn describe_function(
        &self,
        name: &str,
        version: &str,
        namespace: &[String],
    ) -> Result<FunctionVersion> {
        validate_function_name(name)?;
        validate_namespace(namespace)?;
        self.object_job(
            ExecuteQueryRequest {
                query: format!(
                    "DESCRIBE FUNCTION {} WITH (version = {})",
                    quote_identifier(name),
                    literal(version)
                ),
                default_namespace_path: namespace.to_vec(),
                parameters: None,
            },
            definition,
        )
        .await?
        .wait()
        .await
    }
    pub(in crate::remote) async fn list_functions(
        &self,
        namespace: &[String],
        options: ListingOptions,
    ) -> Result<ListingPage<FunctionVersion>> {
        validate_namespace(namespace)?;
        let limit = options.page_limit.unwrap_or(100);
        if limit == 0 || limit > i32::MAX as u32 {
            return Err(Error::InvalidInput {
                message: "page limit must be between 1 and i32::MAX".into(),
            });
        }
        let invalid = || Error::InvalidInput {
            message: "invalid function page token for this database or namespace".into(),
        };
        let after = if let Some(token) = options.page_token.filter(|token| !token.is_empty()) {
            let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid())?;
            let (database, prefix, path, name, version): (
                String,
                Option<String>,
                Vec<String>,
                String,
                String,
            ) = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            if database != self.inner.database
                || prefix != self.inner.database_prefix
                || path != namespace
            {
                return Err(invalid());
            }
            Some((name, version))
        } else {
            None
        };
        let mut sql_options = vec![
            format!("limit = {}", u64::from(limit) + 1),
            "include_definition = true".to_owned(),
        ];
        if let Some((name, version)) = &after {
            sql_options.push(format!("after_name = {}", literal(name)));
            sql_options.push(format!("after_version = {}", literal(version)));
        }
        let mut items: Vec<FunctionVersion> = self
            .object_job(
                ExecuteQueryRequest {
                    query: format!("SHOW FUNCTIONS WITH ({})", sql_options.join(", ")),
                    default_namespace_path: namespace.to_vec(),
                    parameters: None,
                },
                |batches| {
                    strings(&batches, "definition")?
                        .into_iter()
                        .map(|value| {
                            serde_json::from_str(&value).map_err(|error| Error::Runtime {
                                message: error.to_string(),
                            })
                        })
                        .collect::<Result<Vec<FunctionVersion>>>()
                },
            )
            .await?
            .wait()
            .await?;
        items.sort_by(|a, b| (a.name(), a.version()).cmp(&(b.name(), b.version())));
        if let Some((name, version)) = after {
            items.retain(|item| (item.name(), item.version()) > (name.as_str(), version.as_str()));
        }
        let page_token = if items.len() > limit as usize {
            items.truncate(limit as usize);
            let last = items.last().unwrap();
            let bytes = serde_json::to_vec(&(
                &self.inner.database,
                &self.inner.database_prefix,
                namespace,
                last.name(),
                last.version(),
            ))
            .map_err(|error| Error::Runtime {
                message: error.to_string(),
            })?;
            Some(URL_SAFE_NO_PAD.encode(bytes))
        } else {
            None
        };
        Ok(ListingPage { items, page_token })
    }
    pub(in crate::remote) async fn drop_function(
        &self,
        name: &str,
        version: &str,
        namespace: &[String],
    ) -> Result<(bool, Job)> {
        validate_function_name(name)?;
        validate_namespace(namespace)?;
        let job = self
            .object_job(
                ExecuteQueryRequest {
                    query: format!(
                        "DROP FUNCTION IF EXISTS {} WITH (version = {})",
                        quote_identifier(name),
                        literal(version)
                    ),
                    default_namespace_path: namespace.to_vec(),
                    parameters: None,
                },
                |batches| {
                    let rows = strings(&batches, "status")?;
                    match rows.as_slice() {
                        [status] if status == "dropped" => Ok(true),
                        [status] if status == "not_found" => Ok(false),
                        _ => Err(Error::Runtime {
                            message: "invalid DROP FUNCTION result".into(),
                        }),
                    }
                },
            )
            .await?;
        let id = job.id().unwrap().parse().map_err(|_| Error::Runtime {
            message: "invalid SQL query job id".into(),
        })?;
        let dropped = job.wait().await?;
        Ok((
            dropped,
            Job::from_query_task(id, tokio::spawn(async { Ok(()) }), || async { Ok(()) }),
        ))
    }
}
