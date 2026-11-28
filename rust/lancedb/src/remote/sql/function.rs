// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::{
    SqlClient,
    materialized_view::quote_identifier,
    objects::{definition, description, literal, string_lists, strings},
};
use crate::{
    database::ExecuteQueryRequest,
    error::{Error, Result},
    function::{FunctionRegistrationRequest, FunctionVersion},
    job::Job,
    listing::{ListingOptions, ListingPage},
    utils::{validate_function_name, validate_namespace},
};

// These parser diagnostics identify a rejected clause emitted by this SDK.
// They do not identify the server build.
fn with_catalog_pairing(mut error: Error, clause: &str) -> Error {
    if let Error::Sql {
        message,
        grpc_code,
        metadata,
        ..
    } = &mut error
        && *grpc_code == tonic::Code::InvalidArgument as i32
        && metadata.error_code.is_none()
        && metadata.resource.is_none()
    {
        let diagnostic = metadata.server.as_ref().map_or(message.as_str(), |build| {
            message
                .strip_suffix(&format!(" [server: {build}]"))
                .unwrap_or(message)
        });
        if diagnostic.strip_prefix("unexpected trailing input after the statement: ")
            == Some(clause)
        {
            message.push_str(
                ". Check the Python SDK version paired with your LanceDB Enterprise build. \
                 For a build paired with b8, install lancedb==0.42.0b8. \
                 To use lancedb==0.42.0b38, connect to a LanceDB Enterprise build that pins b38.",
            );
        }
    }
    error
}

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
                options: Default::default(),
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
        let with_pairing = |error| with_catalog_pairing(error, "FORMAT");
        self.object_job(
            ExecuteQueryRequest {
                query: format!(
                    "DESCRIBE FUNCTION {}{version_clause} FORMAT JSON",
                    quote_identifier(name)
                ),
                default_namespace_path: namespace.to_vec(),
                parameters: None,
                options: Default::default(),
            },
            description,
        )
        .await
        .map_err(with_pairing)?
        .wait()
        .await
        .map_err(with_pairing)
    }
    pub(in crate::remote) async fn list_functions(
        &self,
        namespace: &[String],
        options: ListingOptions,
    ) -> Result<ListingPage<FunctionVersion>> {
        let (batches, page_token) = self
            .listing_page("SHOW FUNCTIONS HISTORY", namespace, options)
            .await
            .map_err(|error| with_catalog_pairing(error, "LIMIT"))?;
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
                    options: Default::default(),
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
