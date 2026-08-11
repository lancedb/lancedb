// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Remote first-class Function catalog lookup wire helper.
//!
//! POST `/v1/functions/lookup` resolves a database-scoped name or exact
//! [`FunctionId`] to an immutable [`Function`] value. Name is lookup
//! indirection only and never becomes part of the returned handle.

use serde::Deserialize;
use serde_json::Value;

use crate::error::{Error, FunctionErrorCode, Result};
use crate::function::{Function, FunctionId};

use super::client::{HttpSend, RestfulLanceDbClient};
use super::retry::RetryCounter;

const LOOKUP_PATH: &str = "/v1/functions/lookup";

/// Fixed client diagnostic for [`Error::Function`]. Never carry server text,
/// selector values, or response payload bytes.
const LOOKUP_FUNCTION_ERROR_MESSAGE: &str = "function lookup failed";

/// Fixed client diagnostic for protocol / HTTP failures. Never include response
/// payload bytes or selector values.
const LOOKUP_HTTP_ERROR_MESSAGE: &str = "function lookup request failed";

/// Fixed client diagnostic for malformed success payloads.
const LOOKUP_INVALID_SUCCESS_MESSAGE: &str = "function lookup response missing or invalid function";

/// One exact lookup selector. Exactly one variant is serialized on the wire.
pub enum FunctionLookupSelector {
    Name(String),
    FunctionId(String),
}

impl FunctionLookupSelector {
    pub fn by_name(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(Error::InvalidInput {
                message: "function lookup name must be non-empty".into(),
            });
        }
        Ok(Self::Name(name))
    }

    pub fn by_function_id(function_id: &FunctionId) -> Self {
        Self::FunctionId(function_id.as_str().to_string())
    }

    fn to_wire(&self) -> Value {
        match self {
            Self::Name(name) => serde_json::json!({ "name": name }),
            Self::FunctionId(function_id) => {
                serde_json::json!({ "function_id": function_id })
            }
        }
    }
}

#[derive(Deserialize)]
struct LookupSuccessResponse {
    function: Function,
}

/// Resolve a Function via POST `/v1/functions/lookup`.
///
/// Transport classification matches [`RestfulLanceDbClient::send_with_retry`]:
/// connect → connect_retries; timeout/body/decode (including response-byte
/// reads) → read_retries; configured retryable statuses without an explicit
/// `error_code` → request retries; all other transport/client errors return
/// immediately. An explicit nonempty `error_code` is terminal and wins over
/// HTTP status. Request/response payload bytes never enter error chains.
pub async fn lookup_function<S: HttpSend>(
    client: &RestfulLanceDbClient<S>,
    selector: FunctionLookupSelector,
) -> Result<Function> {
    let req_builder = client.post(LOOKUP_PATH).json(&selector.to_wire());

    let tmp_req = req_builder.try_clone().ok_or_else(|| Error::Runtime {
        message: "Attempted to retry a request that cannot be cloned".to_string(),
    })?;
    let (_, built) = tmp_req.build_split();
    let mut built = built.map_err(|e| Error::Runtime {
        message: format!("Failed to build request: {}", e),
    })?;
    let request_id = client.extract_request_id(&mut built);
    let mut retry_counter = RetryCounter::new(&client.retry_config, request_id.clone());

    loop {
        let attempt = req_builder.try_clone().ok_or_else(|| Error::Runtime {
            message: "Attempted to retry a request that cannot be cloned".to_string(),
        })?;

        let rsp = match client
            .send_attempt_with_request_id(attempt, &retry_counter.request_id)
            .await
        {
            Ok(rsp) => rsp,
            Err(err) => {
                classify_lookup_send_error(&mut retry_counter, err)?;
                tokio::time::sleep(retry_counter.next_sleep_time()).await;
                continue;
            }
        };

        let status = rsp.status();
        let bytes = match rsp.bytes().await {
            Ok(bytes) => bytes,
            Err(err) => {
                // Response body/decode failures share the read budget with
                // send-time timeout/body/decode errors.
                retry_counter.increment_read_failures(err)?;
                tokio::time::sleep(retry_counter.next_sleep_time()).await;
                continue;
            }
        };

        if status.is_success() {
            return decode_lookup_success(&bytes, retry_counter.request_id);
        }

        // Explicit nonempty error_code wins over HTTP status and precludes retry.
        if let Some(code) = explicit_error_code(&bytes) {
            return Err(Error::Function {
                code,
                message: LOOKUP_FUNCTION_ERROR_MESSAGE.to_string(),
            });
        }

        if client.retry_config.statuses.contains(&status) {
            let source = Error::Http {
                source: LOOKUP_HTTP_ERROR_MESSAGE.into(),
                request_id: retry_counter.request_id.clone(),
                status_code: Some(status),
            };
            retry_counter.increment_request_failures(source)?;
            tokio::time::sleep(retry_counter.next_sleep_time()).await;
            continue;
        }

        return Err(Error::Http {
            source: LOOKUP_HTTP_ERROR_MESSAGE.into(),
            request_id: retry_counter.request_id,
            status_code: Some(status),
        });
    }
}

/// Classify a send-attempt error using the same buckets as `send_with_retry`.
///
/// Returns `Ok(())` when the caller should sleep and retry. Returns `Err` for
/// nonretryable failures or when a retry budget is exhausted (no extra attempt).
fn classify_lookup_send_error(retry_counter: &mut RetryCounter<'_>, err: Error) -> Result<()> {
    match err {
        Error::Http {
            source,
            request_id,
            status_code,
        } => match source.downcast::<reqwest::Error>() {
            Ok(reqwest_err) if reqwest_err.is_connect() => {
                retry_counter.increment_connect_failures(*reqwest_err)
            }
            Ok(reqwest_err)
                if reqwest_err.is_timeout() || reqwest_err.is_body() || reqwest_err.is_decode() =>
            {
                retry_counter.increment_read_failures(*reqwest_err)
            }
            Ok(reqwest_err) => Err(Error::Http {
                source: Box::new(*reqwest_err),
                request_id,
                status_code,
            }),
            Err(source) => Err(Error::Http {
                source,
                request_id,
                status_code,
            }),
        },
        // Header-provider / client failures are not transport retries.
        other => Err(other),
    }
}

fn decode_lookup_success(bytes: &[u8], request_id: String) -> Result<Function> {
    match serde_json::from_slice::<LookupSuccessResponse>(bytes) {
        Ok(body) => Ok(body.function),
        Err(_) => Err(Error::Http {
            source: LOOKUP_INVALID_SUCCESS_MESSAGE.into(),
            request_id,
            status_code: None,
        }),
    }
}

/// Decode a stable category only from an explicit nonempty string `error_code`.
/// Missing, empty, wrong-type, or non-JSON bodies yield [`None`].
fn explicit_error_code(bytes: &[u8]) -> Option<FunctionErrorCode> {
    let value: Value = serde_json::from_slice(bytes).ok()?;
    let code = value.get("error_code")?;
    match code {
        Value::String(raw) if !raw.is_empty() => {
            serde_json::from_value(Value::String(raw.clone())).ok()
        }
        _ => None,
    }
}
