// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

/// Deserialize an index's `created_at` field.
///
/// The server returns this as an RFC 3339 string (e.g. `"2026-06-18T21:37:36.637Z"`),
/// but older deployments sent a unix timestamp in milliseconds. Accept both so the
/// client works against any server version.
pub(super) fn deserialize_created_at<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<DateTime<Utc>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as _;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum CreatedAt {
        Rfc3339(String),
        Millis(i64),
    }

    match Option::<CreatedAt>::deserialize(deserializer)? {
        None => Ok(None),
        Some(CreatedAt::Rfc3339(s)) => DateTime::parse_from_rfc3339(&s)
            .map(|dt| Some(dt.with_timezone(&Utc)))
            .map_err(D::Error::custom),
        Some(CreatedAt::Millis(ms)) => Ok(DateTime::from_timestamp_millis(ms)),
    }
}

impl<S: HttpSend + 'static> RemoteTable<S> {
    pub(super) async fn index_stats_read_snapshot(
        &self,
        index_name: &str,
        read_snapshot: ReadSnapshot,
    ) -> Result<Option<IndexStatistics>> {
        let encoded_name = urlencoding::encode(index_name);
        let mut body = serde_json::json!({ "version": read_snapshot.version });
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!(
                "/v1/table/{}/index/{encoded_name}/stats/",
                self.identifier
            ))
            .json(&body);

        let (request_id, response) = self
            .send_with_freshness(request, true, read_snapshot.freshness)
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let stats = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse index statistics: {}", e).into(),
            request_id,
            status_code: None,
        })?;
        Ok(Some(stats))
    }

    /// Parse the response from `/index/list/` into `IndexConfig` entries.
    ///
    /// When the server returns `index_type` inline, all enriched fields are
    /// used directly and no further requests are made. When `index_type` is
    /// absent (legacy servers), a `/index/{name}/stats/` call is made for each
    /// index to retrieve the type.
    pub(super) async fn parse_index_list_response(
        &self,
        body: &str,
        request_id: &str,
        schema: &SchemaRef,
        read_snapshot: ReadSnapshot,
    ) -> Result<Vec<IndexConfig>> {
        use crate::index::IndexType;

        #[derive(Deserialize)]
        struct ListIndicesResponse {
            indexes: Vec<IndexListEntry>,
        }

        #[derive(Deserialize)]
        struct IndexListEntry {
            index_name: String,
            columns: Vec<String>,
            // Present on enriched responses; absent on legacy servers.
            // Used as the sentinel to decide whether to skip the stats call.
            index_type: Option<IndexType>,
            index_uuid: Option<String>,
            #[serde(default, deserialize_with = "deserialize_created_at")]
            created_at: Option<DateTime<Utc>>,
            num_indexed_rows: Option<u64>,
            num_unindexed_rows: Option<u64>,
            size_bytes: Option<u64>,
            num_segments: Option<u32>,
            index_version: Option<i32>,
            index_details: Option<String>,
            type_url: Option<String>,
        }

        let response: ListIndicesResponse =
            serde_json::from_str(body).map_err(|err| Error::Http {
                source: format!(
                    "Failed to parse list_indices response: {}, body: {}",
                    err, body
                )
                .into(),
                request_id: request_id.to_string(),
                status_code: None,
            })?;

        let mut futures = Vec::with_capacity(response.indexes.len());
        for entry in response.indexes {
            let columns = entry
                .columns
                .iter()
                .map(|column| {
                    resolve_arrow_field_path(schema, column)
                        .map(|(canonical_column, _)| canonical_column)
                })
                .collect::<Result<Vec<_>>>()?;

            let future = async move {
                if let Some(index_type) = entry.index_type {
                    // Enriched response: all fields available, no stats call needed.
                    Ok(Some(IndexConfig {
                        name: entry.index_name,
                        index_type,
                        columns,
                        index_uuid: entry.index_uuid,
                        type_url: entry.type_url,
                        created_at: entry.created_at,
                        num_indexed_rows: entry.num_indexed_rows,
                        num_unindexed_rows: entry.num_unindexed_rows,
                        size_bytes: entry.size_bytes,
                        num_segments: entry.num_segments,
                        index_version: entry.index_version,
                        index_details: entry.index_details,
                    }))
                } else {
                    // Legacy response: fetch index type via stats endpoint.
                    match self
                        .index_stats_read_snapshot(&entry.index_name, read_snapshot)
                        .await
                    {
                        Ok(Some(stats)) => Ok(Some(IndexConfig {
                            name: entry.index_name,
                            index_type: stats.index_type,
                            columns,
                            index_uuid: None,
                            type_url: None,
                            created_at: None,
                            num_indexed_rows: None,
                            num_unindexed_rows: None,
                            size_bytes: None,
                            num_segments: None,
                            index_version: None,
                            index_details: None,
                        })),
                        Ok(None) => Ok(None), // Index deleted since we listed it.
                        Err(e) => Err(e),
                    }
                }
            };
            futures.push(future);
        }

        let results = futures::future::try_join_all(futures).await?;
        let mut indices: Vec<IndexConfig> = results.into_iter().flatten().collect();
        let lance_schema = lance_core::datatypes::Schema::try_from(schema.as_ref())?;
        for index in &mut indices {
            if index.index_type == IndexType::FTS {
                // The wire format uses physical paths for schema resolution. Match
                // native tables by exposing list-transparent paths to callers.
                for column in &mut index.columns {
                    let field_id = lance_schema.field_id(column)?;
                    *column = public_fts_field_path_by_id(&lance_schema, field_id)?;
                }
            }
        }
        Ok(indices)
    }
}
