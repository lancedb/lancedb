// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_array::{
    Array, BooleanArray, Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
};
use serde_json::Value;

pub(super) use super::storage_integration::object_name;
use super::storage_integration::{literal, secret_name};
use crate::compute_integration::{ComputeIntegrationInfo, CreateComputeIntegrationRequest};
use crate::{Error, Result};

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidInput {
        message: message.into(),
    }
}

pub(super) fn create_sql(
    name: &str,
    namespace: &[String],
    request: &CreateComputeIntegrationRequest,
) -> Result<String> {
    let mut sql = format!(
        "CREATE COMPUTE INTEGRATION {} COMPUTE_PROVIDER = 'KUBERNETES' JOB_SUBMISSION_ENDPOINT = {} KUBERNETES_NAMESPACE = {} KUBERNETES_BEARER_TOKEN_SECRET = {}",
        object_name(name, namespace)?,
        literal(&request.job_submission_endpoint),
        literal(&request.kubernetes_namespace),
        secret_name(&request.kubernetes_bearer_token_secret)?,
    );
    if let Some(secret) = &request.kubernetes_ca_cert_secret {
        sql.push_str(&format!(
            " KUBERNETES_CA_CERT_SECRET = {}",
            secret_name(secret)?
        ));
    }
    for (key, value) in [
        (
            "KUBERNETES_SERVICE_ACCOUNT",
            &request.kubernetes_service_account,
        ),
        (
            "KUBERNETES_IMAGE_PULL_SECRET",
            &request.kubernetes_image_pull_secret,
        ),
    ] {
        if let Some(value) = value {
            sql.push_str(&format!(" {key} = {}", literal(value)));
        }
    }
    Ok(sql)
}

fn string_value(column: &dyn Array, row: usize) -> Option<&str> {
    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        Some(array.value(row))
    } else if let Some(array) = column.as_any().downcast_ref::<LargeStringArray>() {
        Some(array.value(row))
    } else if let Some(array) = column.as_any().downcast_ref::<StringViewArray>() {
        Some(array.value(row))
    } else {
        None
    }
}

pub(super) fn decode_info(batches: &[RecordBatch]) -> Result<ComputeIntegrationInfo> {
    let mut rows = batches.iter().filter(|batch| batch.num_rows() > 0);
    let batch = rows
        .next()
        .ok_or_else(|| invalid("SQL returned no compute integration"))?;
    if batch.num_rows() != 1 || rows.next().is_some() {
        return Err(invalid("SQL must return exactly one compute integration"));
    }
    let mut values = serde_json::Map::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        let mut value = if column.is_null(0) {
            Value::Null
        } else if let Some(value) = string_value(column.as_ref(), 0) {
            Value::String(value.into())
        } else if let Some(array) = column.as_any().downcast_ref::<BooleanArray>() {
            Value::Bool(array.value(0))
        } else if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
            Value::from(array.value(0))
        } else {
            return Err(invalid(format!("Unexpected SQL type for {}", field.name())));
        };
        if matches!(
            field.name().as_str(),
            "namespace_path" | "kubernetes_bearer_token_secret" | "kubernetes_ca_cert_secret"
        ) && let Some(json) = value.as_str()
        {
            value = serde_json::from_str(json)
                .map_err(|error| invalid(format!("Invalid {} metadata: {error}", field.name())))?;
        }
        values.insert(field.name().clone(), value);
    }
    serde_json::from_value(Value::Object(values))
        .map_err(|error| invalid(format!("Invalid compute integration SQL result: {error}")))
}

pub(super) fn decode_names(batches: &[RecordBatch]) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name("name")
            .ok_or_else(|| invalid("SQL compute integration listing is missing names"))?;
        for row in 0..batch.num_rows() {
            if column.is_null(row) {
                return Err(invalid("SQL compute integration name cannot be null"));
            }
            names.push(
                string_value(column.as_ref(), row)
                    .ok_or_else(|| {
                        invalid("SQL compute integration listing requires string names")
                    })?
                    .to_string(),
            );
        }
    }
    names.sort();
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_integration::SecretReference;
    use std::sync::Arc;

    #[test]
    fn renders_secret_identifiers_and_quotes_endpoint_values() {
        let request = CreateComputeIntegrationRequest::kubernetes(
            "https://api.example.com/it's",
            "jobs",
            SecretReference::new("token")
                .with_database("shared")
                .with_namespace(vec!["credentials".into()]),
        )
        .with_kubernetes_image_pull_secret("registry");
        let sql = create_sql("batch", &["compute".into()], &request).unwrap();
        assert!(sql.contains("JOB_SUBMISSION_ENDPOINT = 'https://api.example.com/it''s'"));
        assert!(
            sql.contains("KUBERNETES_BEARER_TOKEN_SECRET = \"shared\".\"credentials\".\"token\"")
        );
        assert!(sql.contains("KUBERNETES_IMAGE_PULL_SECRET = 'registry'"));
        assert!(create_sql("a/b", &[], &request).is_err());
    }

    #[test]
    fn renders_nested_bearer_and_ca_secret_names() {
        let request = CreateComputeIntegrationRequest::kubernetes(
            "https://api.example.com",
            "jobs",
            SecretReference::new("token").with_namespace(vec!["team".into(), "prod".into()]),
        )
        .with_kubernetes_ca_cert_secret(
            SecretReference::new("ca")
                .with_database("shared")
                .with_namespace(vec!["security".into(), "certs".into()]),
        );
        let sql = create_sql("batch", &[], &request).unwrap();
        assert!(sql.contains("KUBERNETES_BEARER_TOKEN_SECRET = \"team$prod\".\"token\""));
        assert!(sql.contains("KUBERNETES_CA_CERT_SECRET = \"shared\".\"security$certs\".\"ca\""));
    }

    #[test]
    fn decodes_flat_sql_result_with_structured_secret_references() {
        let info: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/compute_integration.json"
        ))
        .unwrap();
        let columns = info
            .as_object()
            .unwrap()
            .iter()
            .map(|(name, value)| {
                let column: arrow_array::ArrayRef = match value {
                    Value::Bool(v) => Arc::new(BooleanArray::from(vec![*v])),
                    Value::Number(v) => Arc::new(Int64Array::from(vec![v.as_i64().unwrap()])),
                    Value::Null => Arc::new(StringArray::from(vec![None::<String>])),
                    Value::String(v) => Arc::new(StringArray::from(vec![v.clone()])),
                    other => Arc::new(StringArray::from(vec![other.to_string()])),
                };
                (name.as_str(), column)
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        let decoded = decode_info(&[batch]).unwrap();
        assert_eq!(
            decoded.configuration.kubernetes_bearer_token_secret.name,
            "token"
        );
        assert_eq!(decoded.namespace_path, vec!["compute"]);
        assert!(decode_info(&[]).is_err());
    }
}
