// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::{
    SqlClient,
    materialized_view::quote_identifier,
    objects::{definition, literal, string_lists, strings},
};
use crate::{
    database::ExecuteQueryRequest,
    error::{Error, Result},
    function::{FunctionRegistrationRequest, FunctionVersion},
    job::Job,
    listing::{ListingOptions, ListingPage},
    utils::{validate_function_name, validate_namespace},
};

/// The SQL result of one Function drop, before the public boolean wrapper
/// discards the advisory details.
#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub(in crate::remote) struct DropFunctionOutcome {
    pub(in crate::remote) dropped: bool,
    pub(in crate::remote) warnings: Vec<String>,
}

impl SqlClient {
    pub(in crate::remote) async fn register_function(
        &self,
        request: FunctionRegistrationRequest,
        namespace: &[String],
    ) -> Result<Job<FunctionVersion>> {
        request.validate()?;
        validate_function_name(&request.name)?;
        validate_namespace(namespace)?;
        let name = quote_identifier(&request.name);
        let if_not_exists = if request.exist_ok {
            " IF NOT EXISTS"
        } else {
            ""
        };
        let mut body = serde_json::to_value(request).map_err(|error| Error::Runtime {
            message: error.to_string(),
        })?;
        body.as_object_mut().unwrap().remove("name");
        body.as_object_mut().unwrap().remove("exist_ok");
        self.object_job(
            ExecuteQueryRequest {
                query: format!(
                    "CREATE FUNCTION{if_not_exists} {name} FROM {}",
                    literal(&body.to_string())
                ),
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
        version: Option<&str>,
        namespace: &[String],
    ) -> Result<FunctionVersion> {
        validate_function_name(name)?;
        validate_namespace(namespace)?;
        let version_clause = match version {
            Some(version) => format!(" WITH (version = {})", literal(version)),
            None => String::new(),
        };
        self.object_job(
            ExecuteQueryRequest {
                query: format!(
                    "DESCRIBE FUNCTION {}{version_clause}",
                    quote_identifier(name)
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
        let (batches, page_token) = self
            .listing_page("SHOW FUNCTIONS HISTORY", namespace, options)
            .await?;
        let items = strings(&batches, "definition")?
            .into_iter()
            .map(|value| {
                serde_json::from_str(&value).map_err(|error| Error::Runtime {
                    message: error.to_string(),
                })
            })
            .collect::<Result<Vec<FunctionVersion>>>()?;
        Ok(ListingPage { items, page_token })
    }
    pub(in crate::remote) async fn drop_function_outcome(
        &self,
        name: &str,
        version: &str,
        namespace: &[String],
    ) -> Result<(DropFunctionOutcome, Job)> {
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
                    let dropped = match rows.as_slice() {
                        [status] if status == "dropped" => true,
                        [status] if status == "not_found" => false,
                        _ => {
                            return Err(Error::Runtime {
                                message: "invalid DROP FUNCTION result".into(),
                            });
                        }
                    };
                    Ok(DropFunctionOutcome {
                        dropped,
                        warnings: string_lists(&batches, "warnings")?,
                    })
                },
            )
            .await?;
        let id = job.id().unwrap().parse().map_err(|_| Error::Runtime {
            message: "invalid SQL query job id".into(),
        })?;
        let outcome = job.wait().await?;
        let job = Job::from_query_task(id, tokio::spawn(async { Ok(()) }), || async { Ok(()) });
        Ok((outcome, job))
    }

    pub(in crate::remote) async fn drop_function(
        &self,
        name: &str,
        version: &str,
        namespace: &[String],
    ) -> Result<(bool, Job)> {
        let (outcome, job) = self.drop_function_outcome(name, version, namespace).await?;
        Ok((outcome.dropped, job))
    }
}
