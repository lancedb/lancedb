// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[derive(Deserialize)]
pub(super) struct TableDescription {
    pub(super) version: u64,
    pub(super) schema: JsonSchema,
    pub(super) location: Option<String>,
}

/// How a response body frames its Arrow IPC payload. `/query` answers with file framing
/// and `fetch_blobs` with stream framing, so the reader is picked per response.
pub(super) enum ArrowIpcFraming {
    File,
    Stream,
}

/// A response with no `Content-Type` uses file framing, preserving this helper's
/// behavior before `fetch_blobs` introduced stream responses.
pub(super) fn resolve_arrow_ipc_framing(
    content_type: Option<&str>,
    request_id: &str,
) -> Result<ArrowIpcFraming> {
    let Some(media_type) = content_type.map(base_media_type) else {
        return Ok(ArrowIpcFraming::File);
    };
    if media_type.eq_ignore_ascii_case(ARROW_STREAM_CONTENT_TYPE) {
        return Ok(ArrowIpcFraming::Stream);
    }
    if media_type.eq_ignore_ascii_case(ARROW_FILE_CONTENT_TYPE) {
        return Ok(ArrowIpcFraming::File);
    }
    Err(Error::Http {
        source: format!(
            "Expected an Arrow IPC response with Content-Type '{ARROW_STREAM_CONTENT_TYPE}' \
            or '{ARROW_FILE_CONTENT_TYPE}', got '{media_type}'"
        )
        .into(),
        request_id: request_id.into(),
        status_code: None,
    })
}

/// An Arrow IPC stream carrying `schema` and no batches.
///
/// The body of an all-null `add_columns`: the server reads the fields to
/// add straight off the schema message, so nothing but the field list is
/// worth sending.
pub(super) fn write_ipc_schema(schema: &arrow_schema::Schema) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut body, schema)?;
        writer.finish()?;
    }
    Ok(body)
}

/// Strip media-type parameters before matching against the Arrow content types.
pub(super) fn base_media_type(content_type: &str) -> &str {
    match content_type.split_once(';') {
        Some((media_type, _parameters)) => media_type.trim(),
        None => content_type.trim(),
    }
}

/// Extract an Error from Arc<Error>, reconstructing if the Arc is shared.
/// This is needed because `Shared` futures cache results internally, so
/// `Arc::try_unwrap` typically fails.
pub(super) fn unwrap_shared_error(arc: Arc<Error>) -> Error {
    match Arc::try_unwrap(arc) {
        Ok(err) => err,
        Err(arc) => match &*arc {
            Error::TableNotFound { name, source } => Error::TableNotFound {
                name: name.clone(),
                source: source.to_string().into(),
            },
            _ => Error::Runtime {
                message: arc.to_string(),
            },
        },
    }
}

pub(super) async fn fetch_schema<S: HttpSend>(
    client: &RestfulLanceDbClient<S>,
    identifier: &str,
    table_name: &str,
    read_snapshot: ReadSnapshot,
    branch: Option<String>,
    freshness: Arc<Mutex<FreshnessState>>,
) -> Result<SchemaRef> {
    let mut body = serde_json::json!({ "version": read_snapshot.version });
    if let Some(branch) = &branch {
        body["branch"] = serde_json::Value::String(branch.clone());
    }
    let freshness_headers = read_snapshot.freshness;
    let request = freshness_headers
        .apply(client.post(&format!("/v1/table/{}/describe/", identifier)))
        .json(&body);

    let (request_id, response) = client.send_with_retry(request, None, true).await?;

    if response.status() == StatusCode::NOT_FOUND {
        let body = response.text().await.ok().unwrap_or_default();
        return Err(Error::TableNotFound {
            name: table_name.to_string(),
            source: Box::new(Error::Http {
                source: body.into(),
                request_id,
                status_code: Some(StatusCode::NOT_FOUND),
            }),
        });
    }

    let response = client.check_response(&request_id, response).await?;
    freshness_headers.observe_headers(&freshness, response.headers());
    let body = response.text().await.map_err(|e| {
        let status_code = e.status();
        Error::Http {
            source: Box::new(e),
            request_id: request_id.clone(),
            status_code,
        }
    })?;

    let description: TableDescription = serde_json::from_str(&body).map_err(|e| Error::Http {
        source: format!("Failed to parse table description: {}", e).into(),
        request_id,
        status_code: None,
    })?;
    freshness_headers.observe_version(&freshness, description.version);
    if !freshness_headers.is_current(&freshness) {
        return Err(Error::Runtime {
            message: SCHEMA_SELECTOR_CHANGED.to_string(),
        });
    }

    let arrow_schema: arrow_schema::Schema = description.schema.try_into()?;
    Ok(Arc::new(arrow_schema))
}
