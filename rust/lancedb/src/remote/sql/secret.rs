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
use futures::TryStreamExt;
use std::sync::Arc;

pub(in crate::remote) enum SecretWriteOperation {
    Create { exist_ok: bool },
    Alter,
}

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
        operation: SecretWriteOperation,
    ) -> Result<()> {
        validate_secret_name(name)?;
        validate_namespace(namespace)?;
        let (prefix, assignment) = match operation {
            SecretWriteOperation::Create { exist_ok } => (
                if exist_ok {
                    "CREATE SECRET IF NOT EXISTS"
                } else {
                    "CREATE SECRET"
                },
                "TYPE = GENERIC_STRING ",
            ),
            SecretWriteOperation::Alter => ("ALTER SECRET", "SET "),
        };
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
                    "{} {} {}SECRET_STRING = $1",
                    prefix,
                    quote_identifier(name),
                    assignment
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
        self.list_names("SHOW SECRETS", namespace, options).await
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
