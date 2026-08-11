// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Remote first-class Function catalog wire helpers.
//!
//! POST `/v1/functions/lookup` resolves a database-scoped name or exact
//! [`FunctionId`] to an immutable [`Function`] value. Name is lookup
//! indirection only and never becomes part of the returned handle.
//!
//! POST `/v1/functions/remove` performs a direct synchronous catalog CAS that
//! unbinds a name when the caller's observed [`Function`] id still matches.
//! This is not a Job, not physical Function deletion, and not revocation.

use reqwest::{RequestBuilder, StatusCode};
use serde::Deserialize;
use serde_json::Value;

use crate::error::{Error, FunctionErrorCode, Result};
use crate::function::{Function, FunctionId};

use super::client::{HttpSend, RestfulLanceDbClient};
use super::retry::RetryCounter;

const LOOKUP_PATH: &str = "/v1/functions/lookup";
const REMOVE_PATH: &str = "/v1/functions/remove";

/// Fixed client diagnostic for [`Error::Function`]. Never carry server text,
/// selector values, or response payload bytes.
const LOOKUP_FUNCTION_ERROR_MESSAGE: &str = "function lookup failed";

/// Fixed client diagnostic for protocol / HTTP failures. Never include response
/// payload bytes or selector values.
const LOOKUP_HTTP_ERROR_MESSAGE: &str = "function lookup request failed";

/// Fixed client diagnostic for malformed success payloads.
const LOOKUP_INVALID_SUCCESS_MESSAGE: &str = "function lookup response missing or invalid function";

/// Fixed client diagnostic for remove [`Error::Function`]. Never carry server
/// text, catalog name, Function id, or response payload bytes.
const REMOVE_FUNCTION_ERROR_MESSAGE: &str = "function name removal failed";

/// Fixed client diagnostic for remove protocol / HTTP failures.
const REMOVE_HTTP_ERROR_MESSAGE: &str = "function name removal request failed";

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

/// Decision before reading response bytes.
enum BeforeBody<T> {
    /// Finish without reading or interpreting any body (HTTP 204 remove CAS).
    Done(Result<T>),
    /// Read bytes and continue classification.
    ReadBody,
}

/// How a catalog POST treats a successfully read response body.
enum CatalogBodyAction<T> {
    /// Terminal success or failure for this attempt.
    Done(Result<T>),
    /// Configured retryable status without an explicit `error_code`: consume
    /// the request budget and retry with the same request id and body.
    RetryRequest,
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
    catalog_post_with_retry(
        client,
        req_builder,
        LOOKUP_HTTP_ERROR_MESSAGE,
        |_status, _request_id| BeforeBody::ReadBody,
        |status, bytes, request_id| {
            if status.is_success() {
                return CatalogBodyAction::Done(decode_lookup_success(bytes, request_id));
            }
            if let Some(code) = explicit_error_code(bytes) {
                return CatalogBodyAction::Done(Err(Error::Function {
                    code,
                    message: LOOKUP_FUNCTION_ERROR_MESSAGE.to_string(),
                }));
            }
            if client.retry_config.statuses.contains(&status) {
                return CatalogBodyAction::RetryRequest;
            }
            CatalogBodyAction::Done(Err(Error::Http {
                source: LOOKUP_HTTP_ERROR_MESSAGE.into(),
                request_id,
                status_code: Some(status),
            }))
        },
    )
    .await
}

/// Conditionally remove a database-scoped Function name via POST
/// `/v1/functions/remove`.
///
/// Direct synchronous catalog CAS: the wire body is exactly
/// `{"name","expected_current_function_id"}` using only `current.id`. Only
/// HTTP 204 means the CAS completed; other 2xx are payload-free protocol
/// [`Error::Http`]. Empty names are [`Error::InvalidInput`] before transport.
///
/// Retry budgets match lookup: stable internal request id and exact cloned
/// body across attempts; response-byte failures consume read budget; configured
/// retryable status without explicit `error_code` consumes request budget;
/// header/client errors are immediate. Sophon deduplicates the internal request
/// id; it is not a user-facing idempotency key.
pub async fn remove_function_name<S: HttpSend>(
    client: &RestfulLanceDbClient<S>,
    name: &str,
    current: &Function,
) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidInput {
            message: "function name removal name must be non-empty".into(),
        });
    }

    // Authority is the observed immutable Function id only; never send
    // signature, raw Function objects, Job fields, or user idempotency keys.
    let body = serde_json::json!({
        "name": name,
        "expected_current_function_id": current.id().as_str(),
    });
    let req_builder = client.post(REMOVE_PATH).json(&body);

    catalog_post_with_retry(
        client,
        req_builder,
        REMOVE_HTTP_ERROR_MESSAGE,
        |status, request_id| {
            // Exact HTTP 204 completes the CAS; do not read or interpret any body.
            if status == StatusCode::NO_CONTENT {
                BeforeBody::Done(Ok(()))
            } else if status.is_success() {
                // Other 2xx are payload-free protocol failures from status alone.
                BeforeBody::Done(Err(Error::Http {
                    source: REMOVE_HTTP_ERROR_MESSAGE.into(),
                    request_id: request_id.to_string(),
                    status_code: Some(status),
                }))
            } else {
                BeforeBody::ReadBody
            }
        },
        |status, bytes, request_id| {
            // Explicit nonempty error_code wins over HTTP status and precludes retry.
            if let Some(code) = explicit_error_code(bytes) {
                return CatalogBodyAction::Done(Err(Error::Function {
                    code,
                    message: REMOVE_FUNCTION_ERROR_MESSAGE.to_string(),
                }));
            }

            if client.retry_config.statuses.contains(&status) {
                return CatalogBodyAction::RetryRequest;
            }

            CatalogBodyAction::Done(Err(Error::Http {
                source: REMOVE_HTTP_ERROR_MESSAGE.into(),
                request_id,
                status_code: Some(status),
            }))
        },
    )
    .await
}

/// Shared Function-catalog POST retry loop used by lookup and remove.
///
/// Sensitive attempt sending logs no body/header selectors. One SDK-generated
/// request id and the exact cloned JSON body are reused across attempts.
async fn catalog_post_with_retry<S, Before, After, T>(
    client: &RestfulLanceDbClient<S>,
    req_builder: RequestBuilder,
    http_error_message: &'static str,
    mut before_body: Before,
    mut after_body: After,
) -> Result<T>
where
    S: HttpSend,
    Before: FnMut(StatusCode, &str) -> BeforeBody<T>,
    After: FnMut(StatusCode, &[u8], String) -> CatalogBodyAction<T>,
{
    let mut retry_counter = prepare_catalog_retry(client, &req_builder)?;

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
                classify_catalog_send_error(&mut retry_counter, err)?;
                tokio::time::sleep(retry_counter.next_sleep_time()).await;
                continue;
            }
        };

        let status = rsp.status();
        match before_body(status, &retry_counter.request_id) {
            BeforeBody::Done(result) => return result,
            BeforeBody::ReadBody => {}
        }

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

        match after_body(status, &bytes, retry_counter.request_id.clone()) {
            CatalogBodyAction::Done(result) => return result,
            CatalogBodyAction::RetryRequest => {
                let source = Error::Http {
                    source: http_error_message.into(),
                    request_id: retry_counter.request_id.clone(),
                    status_code: Some(status),
                };
                retry_counter.increment_request_failures(source)?;
                tokio::time::sleep(retry_counter.next_sleep_time()).await;
            }
        }
    }
}

fn prepare_catalog_retry<'a, S: HttpSend>(
    client: &'a RestfulLanceDbClient<S>,
    req_builder: &RequestBuilder,
) -> Result<RetryCounter<'a>> {
    let tmp_req = req_builder.try_clone().ok_or_else(|| Error::Runtime {
        message: "Attempted to retry a request that cannot be cloned".to_string(),
    })?;
    let (_, built) = tmp_req.build_split();
    let mut built = built.map_err(|e| Error::Runtime {
        message: format!("Failed to build request: {}", e),
    })?;
    let request_id = client.extract_request_id(&mut built);
    Ok(RetryCounter::new(&client.retry_config, request_id))
}

/// Classify a send-attempt error using the same buckets as `send_with_retry`.
///
/// Returns `Ok(())` when the caller should sleep and retry. Returns `Err` for
/// nonretryable failures or when a retry budget is exhausted (no extra attempt).
fn classify_catalog_send_error(retry_counter: &mut RetryCounter<'_>, err: Error) -> Result<()> {
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
