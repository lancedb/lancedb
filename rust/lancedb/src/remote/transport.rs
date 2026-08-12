// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Shared Remote HTTP transport primitives for explicit `error_code` classification.
//!
//! These helpers implement the same retry-bucket policy as
//! [`RestfulLanceDbClient::send_with_retry`]: connect → connect budget; timeout /
//! body / decode (including non-success body reads) → read budget; configured
//! retryable status without an explicit nonempty `error_code` → request budget;
//! other client/header errors are immediate. An explicit nonempty top-level
//! `error_code` is terminal [`Error::Function`] and wins over HTTP status.
//! Request/response payload bytes never enter error chains on the classified
//! path.

use reqwest::{RequestBuilder, Response, StatusCode};
use serde_json::Value;

use crate::error::{Error, FunctionErrorCode, Result};

use super::client::{HttpSend, RestfulLanceDbClient};
use super::retry::RetryCounter;

/// Prepare a [`RetryCounter`] with one SDK-generated request id taken from the
/// request builder. The same id is reused across every attempt.
fn prepare_transport_retry<'a, S: HttpSend>(
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
fn classify_transport_send_error(retry_counter: &mut RetryCounter<'_>, err: Error) -> Result<()> {
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

/// Decode a stable category only from an explicit nonempty string `error_code`.
/// Missing, empty, wrong-type, nested-only, or non-JSON bodies yield [`None`].
pub(super) fn explicit_error_code(bytes: &[u8]) -> Option<FunctionErrorCode> {
    let value: Value = serde_json::from_slice(bytes).ok()?;
    let code = value.get("error_code")?;
    match code {
        Value::String(raw) if !raw.is_empty() => {
            serde_json::from_value(Value::String(raw.clone())).ok()
        }
        _ => None,
    }
}

/// Send a clonable request with explicit-`error_code` classification.
///
/// Uses the sensitive attempt path (no request body/header logging). Successful
/// 2xx responses are returned **unconsumed** so callers can run their own
/// success decoders. Non-success bodies are inspected for an explicit nonempty
/// `error_code` before status-based retry. Payload bytes never enter
/// [`Error::Function`], [`Error::Http`], or exhausted [`Error::Retry`] chains.
pub(super) async fn send_with_explicit_error_code<S: HttpSend>(
    client: &RestfulLanceDbClient<S>,
    req_builder: RequestBuilder,
    http_error_message: &'static str,
    function_error_message: &'static str,
) -> Result<(String, Response)> {
    let mut retry_counter = prepare_transport_retry(client, &req_builder)?;

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
                classify_transport_send_error(&mut retry_counter, err)?;
                tokio::time::sleep(retry_counter.next_sleep_time()).await;
                continue;
            }
        };

        let status = rsp.status();
        if status.is_success() {
            // Leave the body unconsumed for Arrow / plan success decoders.
            return Ok((retry_counter.request_id.clone(), rsp));
        }

        // Inspect the body before deciding whether the status is retryable.
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

        if let Some(code) = explicit_error_code(&bytes) {
            return Err(Error::Function {
                code,
                message: function_error_message.to_string(),
            });
        }

        if client.retry_config.statuses.contains(&status) {
            let source = Error::Http {
                source: http_error_message.into(),
                request_id: retry_counter.request_id.clone(),
                status_code: Some(status),
            };
            retry_counter.increment_request_failures(source)?;
            tokio::time::sleep(retry_counter.next_sleep_time()).await;
            continue;
        }

        return Err(Error::Http {
            source: http_error_message.into(),
            request_id: retry_counter.request_id.clone(),
            status_code: Some(status),
        });
    }
}

/// Decision before reading response bytes (catalog mutations / lookup).
pub(super) enum BeforeBody<T> {
    /// Finish without reading or interpreting any body (HTTP 204 mutations).
    Done(Result<T>),
    /// Read bytes and continue classification.
    ReadBody,
}

/// How a classified body is treated after a successful read.
pub(super) enum BodyAction<T> {
    /// Terminal success or failure for this attempt.
    Done(Result<T>),
    /// Configured retryable status without an explicit `error_code`: consume
    /// the request budget and retry with the same request id and body.
    RetryRequest,
}

/// Shared POST retry loop used by Function-catalog helpers.
///
/// Sensitive attempt sending logs no body/header selectors. One SDK-generated
/// request id and the exact cloned JSON body are reused across attempts.
/// Callers supply before/after body classifiers; this loop owns budgets only.
pub(super) async fn post_with_body_classification<S, Before, After, T>(
    client: &RestfulLanceDbClient<S>,
    req_builder: RequestBuilder,
    http_error_message: &'static str,
    mut before_body: Before,
    mut after_body: After,
) -> Result<T>
where
    S: HttpSend,
    Before: FnMut(StatusCode, &str) -> BeforeBody<T>,
    After: FnMut(StatusCode, &[u8], String) -> BodyAction<T>,
{
    let mut retry_counter = prepare_transport_retry(client, &req_builder)?;

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
                classify_transport_send_error(&mut retry_counter, err)?;
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
            BodyAction::Done(result) => return result,
            BodyAction::RetryRequest => {
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
