// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Translate [`lancedb::Error`] into the [`napi::Error`] values Node receives.
//!
//! Async Node-API rejections can carry a message, a Node-API status code, and
//! `error.cause`. They cannot carry an application error subclass. Each LanceDB
//! error is therefore encoded as a single-line `LanceError {json}` header
//! followed by the Rust [`Display`] text. `lancedb/errors.ts` reads that header
//! once, on the way out of the native module, and rethrows a real subclass.
//! Nested Rust sources become `error.cause` instead of indented `"Caused by"`
//! text.

use std::error::Error as StdError;

use lancedb::error::Error as LanceError;
use serde_json::{Map, Value, json};

pub type Result<T> = napi::Result<T>;

const HEADER_PREFIX: &str = "LanceError ";
const MAX_CAUSE_DEPTH: usize = 8;

pub trait NapiErrorExt<T> {
    /// Convert a LanceDB error into a Node error, preserving its kind and cause chain.
    fn default_error(self) -> Result<T>;
}

impl<T> NapiErrorExt<T> for std::result::Result<T, LanceError> {
    fn default_error(self) -> Result<T> {
        self.map_err(|err| convert_error(&err))
    }
}

pub fn convert_error(err: &(dyn StdError + 'static)) -> napi::Error {
    convert_error_at(err, 0)
}

fn convert_error_at(err: &(dyn StdError + 'static), depth: usize) -> napi::Error {
    let mut napi_err = napi::Error::from_reason(format_message(err));
    if depth < MAX_CAUSE_DEPTH
        && let Some(source) = err.source()
    {
        napi_err.set_cause(convert_error_at(source, depth + 1));
    }
    napi_err
}

fn format_message(err: &(dyn StdError + 'static)) -> String {
    match err.downcast_ref::<LanceError>() {
        Some(err) => format!("{HEADER_PREFIX}{}\n{err}", error_payload(err)),
        None => err.to_string(),
    }
}

fn error_payload(err: &LanceError) -> String {
    let mut meta = Map::new();
    let code = error_code(err);
    meta.insert("code".to_owned(), Value::String(code.to_owned()));
    insert_fields(err, &mut meta);
    Value::Object(meta).to_string()
}

fn error_code(err: &LanceError) -> &'static str {
    match err {
        LanceError::InvalidTableName { .. } => "InvalidTableName",
        LanceError::InvalidInput { .. } => "InvalidInput",
        LanceError::TableNotFound { .. } => "TableNotFound",
        LanceError::TableCorrupted { .. } => "TableCorrupted",
        LanceError::DatabaseNotFound { .. } => "DatabaseNotFound",
        LanceError::DatabaseAlreadyExists { .. } => "DatabaseAlreadyExists",
        LanceError::IndexNotFound { .. } => "IndexNotFound",
        LanceError::EmbeddingFunctionNotFound { .. } => "EmbeddingFunctionNotFound",
        LanceError::ColumnNotFound { .. } => "ColumnNotFound",
        LanceError::FieldNotFound { .. } => "FieldNotFound",
        LanceError::ColumnAlreadyExists { .. } => "ColumnAlreadyExists",
        LanceError::NotAComputedColumn { .. } => "NotAComputedColumn",
        LanceError::NotAMaterializedView { .. } => "NotAMaterializedView",
        LanceError::InvalidExpression { .. } => "InvalidExpression",
        LanceError::TableAlreadyExists { .. } => "TableAlreadyExists",
        LanceError::CreateDir { .. } => "CreateDir",
        LanceError::Schema { .. } => "Schema",
        #[cfg(feature = "remote")]
        LanceError::Sql { .. } => "Sql",
        LanceError::Runtime { .. } => "Runtime",
        LanceError::Timeout { .. } => "Timeout",
        LanceError::JobFailed { .. } => "JobFailed",
        LanceError::JobCancelled { .. } => "JobCancelled",
        LanceError::JobNotFound { .. } => "JobNotFound",
        LanceError::ObjectStore { .. } => "ObjectStore",
        LanceError::Lance { .. } => "Lance",
        #[cfg(feature = "remote")]
        LanceError::Http { .. } => "Http",
        #[cfg(feature = "remote")]
        LanceError::Retry { .. } => "Retry",
        LanceError::Arrow { .. } => "Arrow",
        LanceError::NotSupported { .. } => "NotSupported",
        LanceError::External { .. } => "External",
        LanceError::Other { .. } => "Other",
    }
}

fn insert_fields(err: &LanceError, meta: &mut Map<String, Value>) {
    match err {
        LanceError::InvalidTableName { name, .. } | LanceError::TableAlreadyExists { name } => {
            meta.insert("tableName".to_owned(), json!(name));
        }
        LanceError::TableNotFound { name, .. } | LanceError::TableCorrupted { name, .. } => {
            meta.insert("tableName".to_owned(), json!(name));
            #[cfg(feature = "remote")]
            insert_remote_metadata(err, meta);
        }
        LanceError::DatabaseNotFound { name } | LanceError::DatabaseAlreadyExists { name } => {
            meta.insert("databaseName".to_owned(), json!(name));
        }
        LanceError::IndexNotFound { name } => {
            meta.insert("indexName".to_owned(), json!(name));
        }
        LanceError::EmbeddingFunctionNotFound { name, .. } => {
            meta.insert("functionName".to_owned(), json!(name));
        }
        LanceError::ColumnNotFound { name }
        | LanceError::ColumnAlreadyExists { name }
        | LanceError::NotAComputedColumn { name }
        | LanceError::FieldNotFound { name, .. } => {
            meta.insert("columnName".to_owned(), json!(name));
        }
        LanceError::NotAMaterializedView { name } => {
            meta.insert("tableName".to_owned(), json!(name));
        }
        LanceError::InvalidExpression { column, .. } => {
            meta.insert("columnName".to_owned(), json!(column));
        }
        LanceError::CreateDir { path, .. } => {
            meta.insert("path".to_owned(), json!(path));
        }
        LanceError::JobFailed { job_id, failure } => {
            insert_optional(meta, "jobId", job_id.as_deref());
            insert_optional(meta, "phase", failure.phase.as_deref());
            if let Some(retryable) = failure.retryable {
                meta.insert("retryable".to_owned(), json!(retryable));
            }
            insert_optional(meta, "sqlJobId", failure.sql_job_id.as_deref());
            #[cfg(feature = "remote")]
            insert_remote_metadata(err, meta);
        }
        LanceError::JobCancelled { job_id } => {
            insert_optional(meta, "jobId", job_id.as_deref());
        }
        LanceError::JobNotFound { job_id } => {
            meta.insert("jobId".to_owned(), json!(job_id));
        }
        #[cfg(feature = "remote")]
        LanceError::Http {
            request_id,
            status_code,
            ..
        } => {
            meta.insert("requestId".to_owned(), json!(request_id));
            if let Some(status_code) = status_code {
                meta.insert("statusCode".to_owned(), json!(status_code.as_u16()));
            }
            insert_remote_metadata(err, meta);
        }
        #[cfg(feature = "remote")]
        LanceError::Retry {
            request_id,
            status_code,
            request_failures,
            max_request_failures,
            connect_failures,
            max_connect_failures,
            read_failures,
            max_read_failures,
            ..
        } => {
            meta.insert("requestId".to_owned(), json!(request_id));
            if let Some(status_code) = status_code {
                meta.insert("statusCode".to_owned(), json!(status_code.as_u16()));
            }
            meta.insert("requestFailures".to_owned(), json!(request_failures));
            meta.insert("maxRequestFailures".to_owned(), json!(max_request_failures));
            meta.insert("connectFailures".to_owned(), json!(connect_failures));
            meta.insert("maxConnectFailures".to_owned(), json!(max_connect_failures));
            meta.insert("readFailures".to_owned(), json!(read_failures));
            meta.insert("maxReadFailures".to_owned(), json!(max_read_failures));
            insert_remote_metadata(err, meta);
        }
        #[cfg(feature = "remote")]
        LanceError::Sql {
            request_id,
            grpc_code,
            ..
        } => {
            meta.insert("requestId".to_owned(), json!(request_id));
            meta.insert("grpcCode".to_owned(), json!(grpc_code));
            insert_remote_metadata(err, meta);
        }
        _ => {}
    }
}

fn insert_optional(meta: &mut Map<String, Value>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        meta.insert(key.to_owned(), json!(value));
    }
}

#[cfg(feature = "remote")]
fn insert_remote_metadata(err: &LanceError, meta: &mut Map<String, Value>) {
    if let Some(details) = err.http_error_details() {
        meta.insert("errorCode".to_owned(), json!(details.error_code));
        meta.insert("resourceKind".to_owned(), json!(details.resource_kind));
        meta.insert("resourceName".to_owned(), json!(details.resource_name));
        meta.insert("namespacePath".to_owned(), json!(details.namespace_path));
    }
    let Some(metadata) = err.remote_error_metadata() else {
        return;
    };
    if let Some(error_code) = metadata.error_code.as_deref() {
        meta.entry("errorCode".to_owned())
            .or_insert(json!(error_code));
    }
    if let Some(job_id) = metadata.job_id.as_deref() {
        meta.entry("jobId".to_owned()).or_insert(json!(job_id));
    }
    if let Some(job_request_id) = metadata.job_request_id.as_deref() {
        meta.insert("jobRequestId".to_owned(), json!(job_request_id));
    }
}
