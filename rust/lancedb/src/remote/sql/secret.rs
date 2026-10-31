// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::{
    SqlClient,
    materialized_view::quote_identifier,
    objects::{definition, strings},
};
use crate::{
    database::ExecuteQueryRequest,
    error::{Error, Result},
    listing::{ListingOptions, ListingPage},
    secrets::SecretInfo,
    utils::{validate_namespace, validate_secret_name},
};
use arrow_array::{RecordBatch, StringArray};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::TryStreamExt;
use std::sync::Arc;
impl SqlClient {
    pub(in crate::remote) async fn write_secret(
        &self,
        name: &str,
        value: &str,
        namespace: &[String],
        create: bool,
    ) -> Result<()> {
        validate_secret_name(name)?;
        validate_namespace(namespace)?;
        let parameters = RecordBatch::try_from_iter(vec![(
            "",
            Arc::new(StringArray::from(vec![value])) as arrow_array::ArrayRef,
        )])
        .map_err(|error| Error::Runtime {
            message: error.to_string(),
        })?;
        self.object_job(
            ExecuteQueryRequest {
                query: format!(
                    "{} SECRET {} AS $1",
                    if create { "CREATE" } else { "ALTER" },
                    quote_identifier(name)
                ),
                default_namespace_path: namespace.to_vec(),
                parameters: Some(parameters),
            },
            |_| Ok(()),
        )
        .await?
        .wait()
        .await
    }
    pub(in crate::remote) async fn describe_secret(
        &self,
        name: &str,
        namespace: &[String],
    ) -> Result<SecretInfo> {
        validate_secret_name(name)?;
        validate_namespace(namespace)?;
        self.object_job(
            ExecuteQueryRequest {
                query: format!("DESCRIBE SECRET {}", quote_identifier(name)),
                default_namespace_path: namespace.to_vec(),
                parameters: None,
            },
            definition,
        )
        .await?
        .wait()
        .await
    }
    pub(in crate::remote) async fn drop_secret(
        &self,
        name: &str,
        namespace: &[String],
    ) -> Result<()> {
        validate_secret_name(name)?;
        validate_namespace(namespace)?;
        self.object_job(
            ExecuteQueryRequest {
                query: format!("DROP SECRET IF EXISTS {}", quote_identifier(name)),
                default_namespace_path: namespace.to_vec(),
                parameters: None,
            },
            |_| Ok(()),
        )
        .await?
        .wait()
        .await
    }
    pub(in crate::remote) async fn list_secrets(
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
            message: "invalid secret page token for this database or namespace".into(),
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
            if kind != "secret"
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
                query: format!("SHOW SECRETS{after} LIMIT {}", u64::from(limit) + 1),
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
                "secret",
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
