// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::{SqlClient, materialized_view::quote_identifier, objects::strings};
use crate::{
    database::ExecuteQueryRequest,
    error::{Error, Result},
    listing::{ListingOptions, ListingPage},
    utils::{validate_namespace, validate_view_name},
    view::ViewDescription,
};
use lance_namespace::models::JsonArrowSchema;
use std::sync::Arc;
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ViewDefinition {
    name: String,
    namespace: Vec<String>,
    query: String,
    default_database: String,
    default_namespace: Vec<String>,
    schema: JsonArrowSchema,
}

impl ViewDefinition {
    fn into_description(self) -> Result<ViewDescription> {
        let schema =
            lance_namespace::schema::convert_json_arrow_schema(&self.schema).map_err(|error| {
                Error::Runtime {
                    message: format!("View '{}' has an undecodable schema: {error}", self.name),
                }
            })?;
        Ok(ViewDescription {
            name: self.name,
            namespace_path: self.namespace,
            query: self.query,
            default_database: self.default_database,
            default_namespace_path: self.default_namespace,
            schema: Arc::new(schema),
        })
    }
}

fn view_description(
    batches: Vec<arrow_array::RecordBatch>,
    namespace: Vec<String>,
) -> Result<ViewDefinition> {
    let one = |column: &str| -> Result<String> {
        let mut values = strings(&batches, column)?;
        if values.len() != 1 {
            return Err(Error::Runtime {
                message: "View metadata must contain exactly one row".into(),
            });
        }
        Ok(values.remove(0))
    };
    let name = one("name")?;
    let json: JsonArrowSchema =
        serde_json::from_str(&one("schema")?).map_err(|error| Error::Runtime {
            message: format!("View '{name}' has invalid schema JSON: {error}"),
        })?;
    let schema = lance_namespace::schema::convert_json_arrow_schema(&json).map_err(|error| {
        Error::Runtime {
            message: format!("View '{name}' has an undecodable schema: {error}"),
        }
    })?;
    let default_database = schema
        .metadata()
        .get("lancedb::view::default_db")
        .ok_or_else(|| Error::Runtime {
            message: format!("View '{name}' has no defining database"),
        })?
        .clone();
    let default_namespace = schema
        .metadata()
        .get("lancedb::view::default_namespace")
        .ok_or_else(|| Error::Runtime {
            message: format!("View '{name}' has no defining namespace"),
        })?;
    let default_namespace_path = if default_namespace == "public" {
        Vec::new()
    } else {
        default_namespace.split('$').map(str::to_owned).collect()
    };
    Ok(ViewDefinition {
        name,
        namespace,
        query: one("query")?,
        default_database,
        default_namespace: default_namespace_path,
        schema: json,
    })
}

impl SqlClient {
    pub(in crate::remote) async fn create_view(
        &self,
        name: &str,
        query: &str,
        namespace: &[String],
        exist_ok: bool,
    ) -> Result<ViewDescription> {
        validate_view_name(name)?;
        validate_namespace(namespace)?;
        let namespace_path = namespace.to_vec();
        let if_not_exists = if exist_ok { " IF NOT EXISTS" } else { "" };
        self.object_job(
            ExecuteQueryRequest {
                query: format!(
                    "CREATE VIEW{if_not_exists} {} AS {query}",
                    quote_identifier(name)
                ),
                default_namespace_path: namespace.to_vec(),
                parameters: None,
            },
            move |batches| view_description(batches, namespace_path),
        )
        .await?
        .wait()
        .await?
        .into_description()
    }
    pub(in crate::remote) async fn describe_view(
        &self,
        name: &str,
        namespace: &[String],
    ) -> Result<ViewDescription> {
        validate_view_name(name)?;
        validate_namespace(namespace)?;
        let namespace_path = namespace.to_vec();
        self.object_job(
            ExecuteQueryRequest {
                query: format!("DESCRIBE VIEW {}", quote_identifier(name)),
                default_namespace_path: namespace.to_vec(),
                parameters: None,
            },
            move |batches| view_description(batches, namespace_path),
        )
        .await?
        .wait()
        .await?
        .into_description()
    }
    pub(in crate::remote) async fn list_views(
        &self,
        namespace: &[String],
        options: ListingOptions,
    ) -> Result<ListingPage<String>> {
        self.list_names("SHOW VIEWS", namespace, options).await
    }
}
