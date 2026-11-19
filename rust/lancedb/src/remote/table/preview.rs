// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Shared versioned SQL preview decoding and compatibility diagnostics.

use arrow_array::{Array, RecordBatch, StringArray};
use serde::de::DeserializeOwned;

use crate::{Error, Result};

pub(super) fn decode<T: DeserializeOwned>(batches: &[RecordBatch], operation: &str) -> Result<T> {
    let invalid = || Error::Runtime {
        message: format!(
            "EXPLAIN {operation} did not return a valid structured preview; a server supporting EXPLAIN (FORMAT JSON) {operation} is required"
        ),
    };
    if batches.iter().map(RecordBatch::num_rows).sum::<usize>() != 1 {
        return Err(invalid());
    }
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() == 1)
        .ok_or_else(invalid)?;
    let payload = batch
        .column_by_name("plan")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or_else(invalid)?;
    if payload.is_null(0) {
        return Err(invalid());
    }
    #[derive(serde::Deserialize)]
    struct Preview<T> {
        format_version: u32,
        #[serde(flatten)]
        plan: T,
    }
    let preview: Preview<T> = serde_json::from_str(payload.value(0)).map_err(|_| invalid())?;
    if preview.format_version != 1 {
        return Err(invalid());
    }
    Ok(preview.plan)
}

pub(super) fn map_error(error: Error, operation: &str, classify: fn(Error) -> Error) -> Error {
    fn unsupported_format(error: &Error) -> bool {
        match error {
            Error::Sql {
                message, grpc_code, ..
            } => {
                let diagnostic = message.to_ascii_lowercase();
                // Older servers reject JSON through DataFusion, sometimes as Internal.
                (*grpc_code == tonic::Code::Internal as i32
                    || *grpc_code == tonic::Code::InvalidArgument as i32)
                    && diagnostic.contains("invalid explain format.")
                    && diagnostic.contains("got 'json'")
            }
            Error::JobFailed { failure, .. } => {
                failure.source.as_deref().is_some_and(unsupported_format)
            }
            _ => false,
        }
    }
    let error = if unsupported_format(&error) {
        Error::NotSupported {
            message: error.to_string(),
        }
    } else {
        classify(error)
    };
    match error {
        Error::NotSupported { message } => Error::NotSupported {
            message: format!(
                "{operation} previews require a server supporting EXPLAIN (FORMAT JSON) {operation}; upgrade the server. Preview will not fall back to text or REST. Server diagnostic: {message}"
            ),
        },
        other => other,
    }
}
