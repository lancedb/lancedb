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
//!
//! POST `/v1/functions/revoke` performs a direct synchronous administrator
//! catalog set-bit for an exact [`Function`] id. This is not a Job, not name
//! removal, not physical deletion, and not Function mutation.

use reqwest::{RequestBuilder, StatusCode};
use serde::Deserialize;
use serde_json::Value;

use crate::error::{Error, Result};
use crate::function::{Function, FunctionId};

use super::client::{HttpSend, RestfulLanceDbClient};
use super::transport::{
    BeforeBody, BodyAction, explicit_error_code, post_with_body_classification,
};

const LOOKUP_PATH: &str = "/v1/functions/lookup";
const REMOVE_PATH: &str = "/v1/functions/remove";
const REVOKE_PATH: &str = "/v1/functions/revoke";

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

/// Fixed client diagnostic for revoke [`Error::Function`]. Never carry server
/// text, Function id, or response payload bytes.
const REVOKE_FUNCTION_ERROR_MESSAGE: &str = "function revocation failed";

/// Fixed client diagnostic for revoke protocol / HTTP failures.
const REVOKE_HTTP_ERROR_MESSAGE: &str = "function revocation request failed";

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
    post_with_body_classification(
        client,
        req_builder,
        LOOKUP_HTTP_ERROR_MESSAGE,
        |_status, _request_id| BeforeBody::ReadBody,
        |status, bytes, request_id| {
            if status.is_success() {
                return BodyAction::Done(decode_lookup_success(bytes, request_id));
            }
            if let Some(code) = explicit_error_code(bytes) {
                return BodyAction::Done(Err(Error::Function {
                    code,
                    message: LOOKUP_FUNCTION_ERROR_MESSAGE.to_string(),
                }));
            }
            if client.retry_config.statuses.contains(&status) {
                return BodyAction::RetryRequest;
            }
            BodyAction::Done(Err(Error::Http {
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

    catalog_mutation_with_retry(
        client,
        req_builder,
        REMOVE_HTTP_ERROR_MESSAGE,
        REMOVE_FUNCTION_ERROR_MESSAGE,
    )
    .await
}

/// Revoke an exact Function via POST `/v1/functions/revoke`.
///
/// Direct synchronous administrator catalog set-bit: the wire body is exactly
/// `{"function_id"}` from `function.id`. Only HTTP 204 means the set-bit
/// completed; other 2xx are payload-free protocol [`Error::Http`] and are not
/// retried or body-read. There is no empty-input validation because
/// [`Function`] is already a validated exact handle.
///
/// Retry and explicit-code classification match remove. Sophon owns durable
/// idempotent set-bit semantics; repeated logical calls that each receive 204
/// succeed with no client already-revoked branch.
pub async fn revoke_function<S: HttpSend>(
    client: &RestfulLanceDbClient<S>,
    function: &Function,
) -> Result<()> {
    let body = serde_json::json!({
        "function_id": function.id().as_str(),
    });
    let req_builder = client.post(REVOKE_PATH).json(&body);

    catalog_mutation_with_retry(
        client,
        req_builder,
        REVOKE_HTTP_ERROR_MESSAGE,
        REVOKE_FUNCTION_ERROR_MESSAGE,
    )
    .await
}

/// Shared remove/revoke catalog-mutation response classification.
///
/// Exact HTTP 204 succeeds without reading the body. Other 2xx are immediate
/// payload-free [`Error::Http`]. Non-success bodies use explicit nonempty
/// `error_code` as terminal [`Error::Function`], else configured retryable
/// status retry, else payload-free [`Error::Http`]. Each caller supplies its
/// own fixed sanitized messages.
async fn catalog_mutation_with_retry<S: HttpSend>(
    client: &RestfulLanceDbClient<S>,
    req_builder: RequestBuilder,
    http_error_message: &'static str,
    function_error_message: &'static str,
) -> Result<()> {
    post_with_body_classification(
        client,
        req_builder,
        http_error_message,
        |status, request_id| {
            // Exact HTTP 204 completes the mutation; do not read or interpret any body.
            if status == StatusCode::NO_CONTENT {
                BeforeBody::Done(Ok(()))
            } else if status.is_success() {
                // Other 2xx are payload-free protocol failures from status alone.
                BeforeBody::Done(Err(Error::Http {
                    source: http_error_message.into(),
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
                return BodyAction::Done(Err(Error::Function {
                    code,
                    message: function_error_message.to_string(),
                }));
            }

            if client.retry_config.statuses.contains(&status) {
                return BodyAction::RetryRequest;
            }

            BodyAction::Done(Err(Error::Http {
                source: http_error_message.into(),
                request_id,
                status_code: Some(status),
            }))
        },
    )
    .await
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
