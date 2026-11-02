// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::{SqlClient, materialized_view::quote_identifier, objects::strings};
use crate::{
    database::ExecuteQueryRequest,
    error::{Error, Result},
    listing::{ListingOptions, ListingPage},
    secrets::SecretInfo,
    utils::{validate_namespace, validate_secret_name},
};
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::TryStreamExt;
use std::sync::Arc;
fn secret_info(batches: Vec<RecordBatch>) -> Result<SecretInfo> {
    if batches.iter().map(RecordBatch::num_rows).sum::<usize>() != 1 {
        return Err(Error::Runtime {
            message: "DESCRIBE SECRET must return exactly one row".into(),
        });
    }
    let batch = batches.iter().find(|batch| batch.num_rows() == 1).unwrap();
    let timestamp = |name: &str| -> Result<i64> {
        let values = batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| Error::Runtime {
                message: format!("DESCRIBE SECRET is missing INT64 column '{name}'"),
            })?;
        if values.is_null(0) {
            return Err(Error::Runtime {
                message: format!("DESCRIBE SECRET contains null '{name}'"),
            });
        }
        Ok(values.value(0))
    };
    Ok(SecretInfo {
        name: strings(&batches, "name")?.remove(0),
        created_at_millis: timestamp("created_at_millis")?,
        updated_at_millis: timestamp("updated_at_millis")?,
    })
}

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
                    "{} SECRET {} {}SECRET_STRING = $1",
                    if create { "CREATE" } else { "ALTER" },
                    quote_identifier(name),
                    if create {
                        "TYPE = GENERIC_STRING "
                    } else {
                        "SET "
                    }
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
            secret_info,
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::ArrayRef;

    #[test]
    fn decodes_native_secret_metadata() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "name",
                Arc::new(StringArray::from(vec!["api-key"])) as ArrayRef,
            ),
            (
                "created_at_millis",
                Arc::new(Int64Array::from(vec![10])) as ArrayRef,
            ),
            (
                "updated_at_millis",
                Arc::new(Int64Array::from(vec![20])) as ArrayRef,
            ),
        ])
        .unwrap();
        let info = secret_info(vec![batch.clone()]).unwrap();
        assert_eq!(info.name, "api-key");
        assert_eq!(info.created_at_millis, 10);
        assert_eq!(info.updated_at_millis, 20);
        assert!(secret_info(vec![]).is_err());
        assert!(secret_info(vec![batch.clone(), batch]).is_err());
        let missing = RecordBatch::try_from_iter(vec![(
            "name",
            Arc::new(StringArray::from(vec!["api-key"])) as ArrayRef,
        )])
        .unwrap();
        assert!(secret_info(vec![missing]).is_err());
    }
}
