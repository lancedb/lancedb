// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray};
use serde_json::Value;

use crate::storage_integration::{
    CreateStorageIntegrationRequest, SecretReference, StorageIntegrationInfo,
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
        } else if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
            Value::String(array.value(0).into())
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
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| invalid("SQL storage integration listing is missing names"))?;
        for value in column.iter() {
            names.push(
                value
                    .ok_or_else(|| invalid("SQL storage integration name cannot be null"))?
                    .to_string(),
            );
        }
    }
    names.sort();
    Ok(names)
}
