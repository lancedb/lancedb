// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::{SqlClient, materialized_view::quote_identifier, objects::description};
use crate::{
    database::ExecuteQueryRequest,
    error::Result,
    listing::{ListingOptions, ListingPage},
    secrets::SecretInfo,
    utils::{validate_namespace, validate_secret_name},
};
use arrow_array::{RecordBatch, StringArray};
use std::sync::Arc;

pub(in crate::remote) enum SecretWriteOperation {
    Create { exist_ok: bool },
    Alter,
}

fn secret_info(batches: Vec<RecordBatch>) -> Result<SecretInfo> {
    description(batches)
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
                options: Default::default(),
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
                query: format!("DESCRIBE SECRET {} FORMAT JSON", quote_identifier(name)),
                default_namespace_path: namespace.to_vec(),
                parameters: None,
                options: Default::default(),
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
                options: Default::default(),
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

    #[test]
    fn decodes_secret_json_metadata() {
        let document = serde_json::json!({"name":"api-key", "created_at_millis":10, "updated_at_millis":20, "history":[{"generation":1,"at_millis":10}]});
        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "description",
            arrow_schema::DataType::Utf8,
            false,
        )])
        .with_metadata(std::collections::HashMap::from([(
            "lancedb.describe.format".into(),
            "json".into(),
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(vec![document.to_string()]))],
        )
        .unwrap();
        let info = secret_info(vec![batch.clone()]).unwrap();
        assert_eq!(info.name, "api-key");
        assert_eq!(info.created_at_millis, 10);
        assert_eq!(info.updated_at_millis, 20);
        assert!(secret_info(vec![]).is_err());
        assert!(secret_info(vec![batch.clone(), batch.clone()]).is_err());
        for invalid in [
            serde_json::json!({"name":"api-key"}),
            serde_json::json!({"name":"api-key","created_at_millis":"10","updated_at_millis":20}),
        ] {
            let batch = RecordBatch::try_new(
                batch.schema(),
                vec![Arc::new(StringArray::from(vec![invalid.to_string()]))],
            )
            .unwrap();
            assert!(secret_info(vec![batch]).is_err());
        }
    }
}
