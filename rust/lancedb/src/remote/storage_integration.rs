// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_array::{
    Array, BooleanArray, Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
};
use serde_json::Value;

use crate::storage_integration::{
    CreateStorageIntegrationRequest, SecretReference, StorageIntegrationInfo, StorageProvider,
};
use crate::utils::{validate_database_name, validate_namespace, validate_secret_name};
use crate::{Error, Result};

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidInput {
        message: message.into(),
    }
}

pub(super) fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(super) fn object_name(name: &str, namespace: &[String]) -> Result<String> {
    validate_secret_name(name)?;
    validate_namespace(namespace)?;
    Ok(identifier(name))
}

fn secret_name(reference: &SecretReference) -> Result<String> {
    validate_secret_name(&reference.name)?;
    validate_namespace(&reference.namespace_path)?;
    let mut parts = Vec::new();
    if let Some(database) = &reference.database {
        validate_database_name(database)?;
        parts.push(identifier(database));
    }
    if reference.namespace_path.is_empty() {
        parts.push(identifier("public"));
    } else {
        parts.extend(reference.namespace_path.iter().map(|name| identifier(name)));
    }
    parts.push(identifier(&reference.name));
    Ok(parts.join("."))
}

pub(super) fn create_sql(
    name: &str,
    namespace: &[String],
    request: &CreateStorageIntegrationRequest,
) -> Result<String> {
    if request.storage_provider == StorageProvider::Azure {
        if request.aws_role_arn.is_some()
            || request.aws_access_key_id_secret.is_some()
            || request.aws_secret_access_key_secret.is_some()
            || request.aws_region.is_some()
        {
            return Err(invalid("Azure integrations cannot contain AWS options"));
        }
        let account = request
            .azure_storage_account
            .as_ref()
            .filter(|account| !account.is_empty())
            .ok_or_else(|| invalid("Azure integrations require azure_storage_account"))?;
        let mut sql = format!(
            "CREATE STORAGE INTEGRATION {} STORAGE_PROVIDER = 'AZURE' STORAGE_PREFIX = {} STORAGE_AZURE_STORAGE_ACCOUNT = {}",
            object_name(name, namespace)?,
            literal(&request.storage_prefix),
            literal(account)
        );
        for (key, reference) in [
            (
                "STORAGE_AZURE_TENANT_ID_SECRET",
                &request.azure_tenant_id_secret,
            ),
            (
                "STORAGE_AZURE_CLIENT_ID_SECRET",
                &request.azure_client_id_secret,
            ),
            (
                "STORAGE_AZURE_CLIENT_SECRET_SECRET",
                &request.azure_client_secret_secret,
            ),
        ] {
            let reference = reference.as_ref().ok_or_else(|| {
                invalid("Azure integrations require all three credential Secret references")
            })?;
            sql.push_str(&format!(" {key} = {}", secret_name(reference)?));
        }
        return Ok(sql);
    }
    if request.azure_storage_account.is_some()
        || request.azure_tenant_id_secret.is_some()
        || request.azure_client_id_secret.is_some()
        || request.azure_client_secret_secret.is_some()
    {
        return Err(invalid("S3 integrations cannot contain Azure options"));
    }
    let mut sql = format!(
        "CREATE STORAGE INTEGRATION {} STORAGE_PROVIDER = 'S3' STORAGE_PREFIX = {}",
        object_name(name, namespace)?,
        literal(&request.storage_prefix)
    );
    match (
        &request.aws_role_arn,
        &request.aws_access_key_id_secret,
        &request.aws_secret_access_key_secret,
    ) {
        (Some(role), None, None) => {
            sql.push_str(&format!(" STORAGE_AWS_ROLE_ARN = {}", literal(role)))
        }
        (None, Some(key), Some(secret)) => sql.push_str(&format!(
            " STORAGE_AWS_ACCESS_KEY_ID_SECRET = {} STORAGE_AWS_SECRET_ACCESS_KEY_SECRET = {}",
            secret_name(key)?,
            secret_name(secret)?
        )),
        _ => {
            return Err(invalid(
                "Specify either an AWS role or both credential Secret references",
            ));
        }
    }
    if let Some(region) = &request.aws_region {
        sql.push_str(&format!(" STORAGE_AWS_REGION = {}", literal(region)));
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

pub(super) fn decode_info(batches: &[RecordBatch]) -> Result<StorageIntegrationInfo> {
    let mut rows = batches.iter().filter(|batch| batch.num_rows() > 0);
    let batch = rows
        .next()
        .ok_or_else(|| invalid("SQL returned no storage integration"))?;
    if batch.num_rows() != 1 || rows.next().is_some() {
        return Err(invalid("SQL must return exactly one storage integration"));
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
            "namespace_path"
                | "aws_access_key_id_secret"
                | "aws_secret_access_key_secret"
                | "azure_tenant_id_secret"
                | "azure_client_id_secret"
                | "azure_client_secret_secret"
                | "trust_policy"
                | "permissions_policy"
        ) && let Some(json) = value.as_str()
        {
            value = serde_json::from_str(json)
                .map_err(|error| invalid(format!("Invalid {} metadata: {error}", field.name())))?;
        }
        values.insert(field.name().clone(), value);
    }
    serde_json::from_value(Value::Object(values))
        .map_err(|error| invalid(format!("Invalid storage integration SQL result: {error}")))
}

pub(super) fn decode_names(batches: &[RecordBatch]) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name("name")
            .ok_or_else(|| invalid("SQL storage integration listing is missing names"))?;
        for row in 0..batch.num_rows() {
            if column.is_null(row) {
                return Err(invalid("SQL storage integration name cannot be null"));
            }
            names.push(
                string_value(column.as_ref(), row)
                    .ok_or_else(|| {
                        invalid("SQL storage integration listing requires string names")
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
    use std::sync::Arc;

    use super::*;

    #[test]
    fn sql_quotes_values_and_keeps_secret_root_namespace_explicit() {
        let role = CreateStorageIntegrationRequest::s3("s3://bucket/it's/", "role'value");
        let sql = create_sql("raw", &[], &role).unwrap();
        assert!(sql.contains("STORAGE_PREFIX = 's3://bucket/it''s/'"));
        assert!(sql.contains("STORAGE_AWS_ROLE_ARN = 'role''value'"));
        let reference = SecretReference::new("key").with_database("platform/raw");
        let static_config = CreateStorageIntegrationRequest::s3_with_secrets(
            "s3://bucket/raw/",
            reference.clone(),
            reference,
        );
        let sql = create_sql("raw", &["nested".into()], &static_config).unwrap();
        assert!(
            sql.contains("STORAGE_AWS_ACCESS_KEY_ID_SECRET = \"platform/raw\".\"public\".\"key\"")
        );
        assert!(create_sql("a/b", &[], &role).is_err());
    }

    #[test]
    fn azure_sql_uses_only_explicit_account_and_three_secret_objects() {
        let reference = SecretReference::new("credential")
            .with_database("shared")
            .with_namespace(vec!["credentials".into()]);
        let mut config = CreateStorageIntegrationRequest::azure(
            "az://container/raw/",
            "customeraccount",
            reference.clone(),
            reference.clone(),
            reference,
        );
        let sql = create_sql("blobs", &[], &config).unwrap();
        assert!(sql.contains("STORAGE_PROVIDER = 'AZURE'"));
        assert!(sql.contains("STORAGE_AZURE_STORAGE_ACCOUNT = 'customeraccount'"));
        assert!(sql.contains(
            "STORAGE_AZURE_CLIENT_SECRET_SECRET = \"shared\".\"credentials\".\"credential\""
        ));
        assert!(!sql.contains("STORAGE_AWS"));
        config.azure_tenant_id_secret = None;
        assert!(create_sql("blobs", &[], &config).is_err());
        config.aws_region = Some("us-east-1".into());
        assert!(create_sql("blobs", &[], &config).is_err());
    }

    #[test]
    fn sql_metadata_rejects_missing_rows_and_invalid_json() {
        assert!(decode_info(&[]).is_err());
        let batch = RecordBatch::try_from_iter([(
            "namespace_path",
            Arc::new(StringArray::from(vec!["invalid json"])) as arrow_array::ArrayRef,
        )])
        .unwrap();
        assert!(
            decode_info(&[batch])
                .unwrap_err()
                .to_string()
                .contains("namespace_path")
        );
    }
}
