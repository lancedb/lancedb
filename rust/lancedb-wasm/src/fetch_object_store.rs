// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::io;
use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use futures::channel::oneshot;
use futures::stream::{self, BoxStream, StreamExt};
use http::header::{
    CONTENT_LENGTH, CONTENT_RANGE, ETAG, IF_MATCH, IF_MODIFIED_SINCE, IF_NONE_MATCH,
    IF_UNMODIFIED_SINCE, LAST_MODIFIED, RANGE,
};
use js_sys::{Function, Promise, Reflect, Uint8Array};
use object_store::path::Path;
use object_store::{
    Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::{JsFuture, spawn_local};
use web_sys::{Headers, Request, RequestInit, Response};

#[derive(Debug, Clone)]
pub struct FetchHttpStore {
    base_url: url::Url,
    headers: HashMap<String, String>,
}

impl FetchHttpStore {
    pub fn new(base_url: url::Url, headers: HashMap<String, String>) -> Self {
        Self { base_url, headers }
    }

    async fn fetch(&self, location: &Path, options: &GetOptions) -> object_store::Result<Response> {
        let url = self.object_url(location)?;
        let headers = self.request_headers(options)?;
        let method = if options.head { "HEAD" } else { "GET" };

        let init = RequestInit::new();
        init.set_method(method);
        init.set_headers(&headers);

        let request = Request::new_with_str_and_init(url.as_str(), &init)
            .map_err(|source| js_error("creating fetch request", source))?;
        let global = js_sys::global();
        let fetch = Reflect::get(&global, &JsValue::from_str("fetch"))
            .map_err(|source| js_error("reading global fetch", source))?
            .dyn_into::<Function>()
            .map_err(|source| js_error("casting global fetch", source))?;
        let response = fetch
            .call1(&global, request.as_ref())
            .map(Promise::from)
            .map_err(|source| js_error("calling global fetch", source))?;
        JsFuture::from(response)
            .await
            .map_err(|source| js_error("awaiting fetch response", source))?
            .dyn_into::<Response>()
            .map_err(|source| js_error("casting fetch response", source))
    }

    async fn fetch_parts(
        &self,
        location: Path,
        options: GetOptions,
    ) -> object_store::Result<FetchParts> {
        let response = self.fetch(&location, &options).await?;
        let status = response.status();
        let headers = response.headers();

        match status {
            404 => {
                return Err(object_store::Error::NotFound {
                    path: location.to_string(),
                    source: boxed("fetch returned 404"),
                });
            }
            304 => {
                return Err(object_store::Error::NotModified {
                    path: location.to_string(),
                    source: boxed("fetch returned 304"),
                });
            }
            412 => {
                return Err(object_store::Error::Precondition {
                    path: location.to_string(),
                    source: boxed("fetch returned 412"),
                });
            }
            200 | 206 => {}
            _ => {
                return Err(object_store::Error::Generic {
                    store: "lancedb-wasm-fetch",
                    source: boxed(format!("unexpected HTTP status {status} for {}", location)),
                });
            }
        }

        let (meta, range) = parse_meta(&location, &headers, &options, status)?;
        let bytes = if options.head {
            Bytes::new()
        } else {
            let buffer = JsFuture::from(
                response
                    .array_buffer()
                    .map_err(|source| js_error("reading response arrayBuffer", source))?,
            )
            .await
            .map_err(|source| js_error("awaiting response arrayBuffer", source))?;
            Bytes::from(Uint8Array::new(&buffer).to_vec())
        };

        if !options.head && bytes.len() as u64 != range.end.saturating_sub(range.start) {
            return Err(object_store::Error::Generic {
                store: "lancedb-wasm-fetch",
                source: boxed(format!(
                    "response length {} did not match expected range {}..{} for {}",
                    bytes.len(),
                    range.start,
                    range.end,
                    location
                )),
            });
        }

        Ok(FetchParts { meta, range, bytes })
    }

    fn object_url(&self, location: &Path) -> object_store::Result<url::Url> {
        if location.as_ref().is_empty() {
            return Ok(self.base_url.clone());
        }

        self.base_url
            .join(location.as_ref())
            .map_err(|source| object_store::Error::Generic {
                store: "lancedb-wasm-fetch",
                source: Box::new(source),
            })
    }

    fn request_headers(&self, options: &GetOptions) -> object_store::Result<Headers> {
        let headers =
            Headers::new().map_err(|source| js_error("creating request headers", source))?;
        for (key, value) in &self.headers {
            headers
                .set(key, value)
                .map_err(|source| js_error(format!("setting header '{key}'"), source))?;
        }
        if let Some(range) = options.range.as_ref() {
            headers
                .set(RANGE.as_str(), &range.to_string())
                .map_err(|source| js_error("setting range header", source))?;
        }
        if let Some(value) = options.if_match.as_deref() {
            headers
                .set(IF_MATCH.as_str(), value)
                .map_err(|source| js_error("setting If-Match header", source))?;
        }
        if let Some(value) = options.if_none_match.as_deref() {
            headers
                .set(IF_NONE_MATCH.as_str(), value)
                .map_err(|source| js_error("setting If-None-Match header", source))?;
        }
        if let Some(value) = options.if_modified_since {
            headers
                .set(IF_MODIFIED_SINCE.as_str(), &value.to_rfc2822())
                .map_err(|source| js_error("setting If-Modified-Since header", source))?;
        }
        if let Some(value) = options.if_unmodified_since {
            headers
                .set(IF_UNMODIFIED_SINCE.as_str(), &value.to_rfc2822())
                .map_err(|source| js_error("setting If-Unmodified-Since header", source))?;
        }
        Ok(headers)
    }
}

impl Display for FetchHttpStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FetchHttpStore({})", self.base_url)
    }
}

#[async_trait]
impl ObjectStore for FetchHttpStore {
    async fn put_opts(
        &self,
        location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(unsupported(format!(
            "put is not supported for {}",
            location
        )))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(unsupported(format!(
            "multipart upload is not supported for {}",
            location
        )))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let store = self.clone();
        let location = location.clone();
        let (tx, rx) = oneshot::channel();
        spawn_local(async move {
            let result = store.fetch_parts(location, options).await;
            let _ = tx.send(result);
        });
        let FetchParts { meta, range, bytes } =
            rx.await.map_err(|_| object_store::Error::Generic {
                store: "lancedb-wasm-fetch",
                source: boxed("fetch task was canceled"),
            })??;

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
            meta,
            range,
            attributes: Attributes::default(),
        })
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        Err(unsupported(format!(
            "delete is not supported for {}",
            location
        )))
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let message = match prefix {
            Some(prefix) => format!("list is not supported for prefix {}", prefix),
            None => "list is not supported".to_string(),
        };
        stream::once(async move { Err(unsupported(message)) }).boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let message = match prefix {
            Some(prefix) => format!("list_with_delimiter is not supported for prefix {}", prefix),
            None => "list_with_delimiter is not supported".to_string(),
        };
        Err(unsupported(message))
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        Err(unsupported(format!(
            "copy is not supported from {} to {}",
            from, to
        )))
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        Err(unsupported(format!(
            "copy_if_not_exists is not supported from {} to {}",
            from, to
        )))
    }
}

struct FetchParts {
    meta: ObjectMeta,
    range: Range<u64>,
    bytes: Bytes,
}

fn parse_meta(
    location: &Path,
    headers: &Headers,
    options: &GetOptions,
    status: u16,
) -> object_store::Result<(ObjectMeta, Range<u64>)> {
    let e_tag = header(headers, ETAG.as_str())?;
    let last_modified = match header(headers, LAST_MODIFIED.as_str())? {
        Some(value) => DateTime::parse_from_rfc2822(&value)
            .map(|value| value.with_timezone(&Utc))
            .map_err(|source| object_store::Error::Generic {
                store: "lancedb-wasm-fetch",
                source: boxed(format!("invalid Last-Modified header '{value}': {source}")),
            })?,
        None => Utc.timestamp_nanos(0),
    };

    let (size, range) = if status == 206 {
        let value = required_header(headers, CONTENT_RANGE.as_str())?;
        let (range, size) = parse_content_range(&value)?;
        if let Some(expected) = options.range.as_ref() {
            let expected =
                expected
                    .as_range(size)
                    .map_err(|source| object_store::Error::Generic {
                        store: "lancedb-wasm-fetch",
                        source: boxed(format!("invalid expected range for {}: {source}", location)),
                    })?;
            if expected != range {
                return Err(object_store::Error::Generic {
                    store: "lancedb-wasm-fetch",
                    source: boxed(format!(
                        "server returned range {}..{} but expected {}..{} for {}",
                        range.start, range.end, expected.start, expected.end, location
                    )),
                });
            }
        }
        (size, range)
    } else if let Some(range) = options.range.as_ref() {
        return Err(object_store::Error::Generic {
            store: "lancedb-wasm-fetch",
            source: boxed(format!(
                "server did not honor range request '{}' for {}",
                range, location
            )),
        });
    } else {
        let size = required_header(headers, CONTENT_LENGTH.as_str())?
            .parse::<u64>()
            .map_err(|source| object_store::Error::Generic {
                store: "lancedb-wasm-fetch",
                source: boxed(format!(
                    "invalid Content-Length header for {}: {}",
                    location, source
                )),
            })?;
        let range = if options.head { 0..0 } else { 0..size };
        (size, range)
    };

    Ok((
        ObjectMeta {
            location: location.clone(),
            last_modified,
            size,
            e_tag,
            version: None,
        },
        range,
    ))
}

fn parse_content_range(value: &str) -> object_store::Result<(Range<u64>, u64)> {
    let value = value.trim();
    let rest = value
        .strip_prefix("bytes ")
        .ok_or_else(|| object_store::Error::Generic {
            store: "lancedb-wasm-fetch",
            source: boxed(format!("invalid Content-Range header '{value}'")),
        })?;
    let (range, size) = rest
        .split_once('/')
        .ok_or_else(|| object_store::Error::Generic {
            store: "lancedb-wasm-fetch",
            source: boxed(format!("invalid Content-Range header '{value}'")),
        })?;
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| object_store::Error::Generic {
            store: "lancedb-wasm-fetch",
            source: boxed(format!("invalid Content-Range header '{value}'")),
        })?;
    let start = start
        .parse::<u64>()
        .map_err(|source| object_store::Error::Generic {
            store: "lancedb-wasm-fetch",
            source: boxed(format!(
                "invalid Content-Range start in '{value}': {source}"
            )),
        })?;
    let end_inclusive = end
        .parse::<u64>()
        .map_err(|source| object_store::Error::Generic {
            store: "lancedb-wasm-fetch",
            source: boxed(format!("invalid Content-Range end in '{value}': {source}")),
        })?;
    let size = size
        .parse::<u64>()
        .map_err(|source| object_store::Error::Generic {
            store: "lancedb-wasm-fetch",
            source: boxed(format!("invalid Content-Range size in '{value}': {source}")),
        })?;
    let end = end_inclusive
        .checked_add(1)
        .ok_or_else(|| object_store::Error::Generic {
            store: "lancedb-wasm-fetch",
            source: boxed(format!("invalid Content-Range end in '{value}'")),
        })?;
    Ok((start..end, size))
}

fn header(headers: &Headers, name: &str) -> object_store::Result<Option<String>> {
    headers
        .get(name)
        .map_err(|source| js_error(format!("reading '{name}' response header"), source))
}

fn required_header(headers: &Headers, name: &str) -> object_store::Result<String> {
    header(headers, name)?.ok_or_else(|| object_store::Error::Generic {
        store: "lancedb-wasm-fetch",
        source: boxed(format!("response header '{name}' was missing")),
    })
}

fn unsupported(message: impl Into<String>) -> object_store::Error {
    object_store::Error::NotSupported {
        source: boxed(message.into()),
    }
}

fn js_error(context: impl Into<String>, source: JsValue) -> object_store::Error {
    object_store::Error::Generic {
        store: "lancedb-wasm-fetch",
        source: boxed(format!("{}: {:?}", context.into(), source)),
    }
}

fn boxed(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
    Box::new(io::Error::other(message.into()))
}

#[cfg(test)]
mod tests {
    use super::parse_content_range;

    #[test]
    fn parses_content_range() {
        let (range, size) = parse_content_range("bytes 10-24/100").unwrap();
        assert_eq!(range, 10..25);
        assert_eq!(size, 100);
    }

    #[test]
    fn rejects_invalid_content_range() {
        assert!(parse_content_range("nope").is_err());
    }
}
