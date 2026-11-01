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
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::TryStreamExt;
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
    ) -> Result<ViewDescription> {
        validate_view_name(name)?;
        validate_namespace(namespace)?;
        let namespace_path = namespace.to_vec();
        self.object_job(
            ExecuteQueryRequest {
                query: format!("CREATE VIEW {} AS {query}", quote_identifier(name)),
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
        validate_namespace(namespace)?;
        let limit = options.page_limit.unwrap_or(100);
        if limit == 0 || limit > i32::MAX as u32 {
            return Err(Error::InvalidInput {
                message: "page limit must be between 1 and i32::MAX".into(),
            });
        }
        let invalid_token = || Error::InvalidInput {
            message: "invalid view page token for this database or namespace".into(),
        };
        let after = if let Some(token) = options.page_token.filter(|token| !token.is_empty()) {
            let decoded = URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid_token())?;
            let (kind, database, prefix, path, last): (
                String,
                String,
                Option<String>,
                Vec<String>,
                String,
            ) = serde_json::from_slice(&decoded).map_err(|_| invalid_token())?;
            if kind != "view"
                || database != self.inner.database
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
                query: format!("SHOW VIEWS{after} LIMIT {}", u64::from(limit) + 1),
                default_namespace_path: namespace.to_vec(),
                parameters: None,
            })
            .await?;
        let mut reader = query.reader().await?;
        let mut items = Vec::new();
        while let Some(batch) = reader.try_next().await? {
            items.extend(strings(&[batch], "name")?);
        }
        let page_token = if items.len() > limit as usize {
            items.truncate(limit as usize);
            let cursor = serde_json::to_vec(&(
                "view",
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
