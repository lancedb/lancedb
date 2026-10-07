// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

impl<S: HttpSend> RemoteTable<S> {
    pub(super) async fn get_lsm_stats_impl(
        &self,
        include_generation_rows: bool,
    ) -> Result<Option<LsmStats>> {
        // Read-semantics POST, like `get_lsm_write_spec`.
        let request = self
            .client
            .post(&format!("/v1/table/{}/get_lsm_stats/", self.identifier))
            .json(&serde_json::json!({
                "include_generation_rows": include_generation_rows,
            }));
        let (request_id, response) = self.send_lsm_route(request).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let parsed: GetLsmStatsResponse = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse get_lsm_stats response: {e}").into(),
            request_id,
            status_code: None,
        })?;
        // `null` — and only — when the table has no LSM write path.
        Ok(parsed.lsm_stats)
    }

    pub(super) async fn set_lsm_write_spec_impl(&self, spec: LsmWriteSpec) -> Result<()> {
        self.check_mutable().await?;

        // Map the spec onto the server's request DTO. `sharding` is internally
        // tagged on `mode` to mirror sophon's `Sharding` enum. A null
        // `maintained_indexes` asks the server to resolve every maintainable
        // index at HEAD; a list is verbatim, an empty one meaning none.
        let sharding = match &spec {
            LsmWriteSpec::Bucket {
                column,
                num_buckets,
                ..
            } => serde_json::json!({
                "mode": "bucket",
                "column": column,
                "num_buckets": num_buckets,
            }),
            LsmWriteSpec::Identity { column, .. } => serde_json::json!({
                "mode": "identity",
                "column": column,
            }),
            LsmWriteSpec::Unsharded { .. } => serde_json::json!({ "mode": "unsharded" }),
        };
        let body = serde_json::json!({
            "sharding": sharding,
            "maintained_indexes": spec.maintained_indexes(),
            "writer_config_defaults": spec.writer_config_defaults(),
        });

        let request = self
            .client
            .post(&format!(
                "/v1/table/{}/set_lsm_write_spec/",
                self.identifier
            ))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        self.check_table_response(&request_id, response).await?;
        Ok(())
    }

    pub(super) async fn unset_lsm_write_spec_impl(&self) -> Result<()> {
        self.check_mutable().await?;
        let request = self.client.post(&format!(
            "/v1/table/{}/unset_lsm_write_spec/",
            self.identifier
        ));
        let (request_id, response) = self.send(request, true).await?;
        self.check_table_response(&request_id, response).await?;
        self.wal_pk_fusion.forget(); // WAL-PK-FUSION: delete.
        Ok(())
    }

    pub(super) async fn get_lsm_write_spec_impl(&self) -> Result<Option<LsmWriteSpec>> {
        // Read counterpart to set/unset, resolved server-side against HEAD. The
        // server reads the spec from the `__lance_mem_wal` system index (shard
        // column mapped from its Lance field id against the current schema) and
        // re-encodes it into the same sophon-owned shape the set endpoint
        // accepts — no lance/lancedb types cross the wire. `lsm_write_spec` is
        // null when the LSM write path is not enabled for the table.
        let request = self.client.post(&format!(
            "/v1/table/{}/get_lsm_write_spec/",
            self.identifier
        ));
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        // Mirror of sophon's `Sharding` (internally tagged on `mode`) and
        // `LsmWriteSpecBody` / `GetLsmWriteSpecResponse`.
        #[derive(Deserialize)]
        #[serde(tag = "mode", rename_all = "snake_case")]
        enum Sharding {
            Unsharded,
            Bucket { column: String, num_buckets: u32 },
            Identity { column: String },
        }
        #[derive(Deserialize)]
        struct LsmWriteSpecBody {
            sharding: Sharding,
            /// `null` selects every index the table has; `[]` selects none.
            #[serde(default)]
            maintained_indexes: Option<Vec<String>>,
            #[serde(default)]
            writer_config_defaults: std::collections::HashMap<String, String>,
        }
        #[derive(Deserialize)]
        struct GetLsmWriteSpecResponse {
            lsm_write_spec: Option<LsmWriteSpecBody>,
        }

        let parsed: GetLsmWriteSpecResponse =
            serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("Failed to parse get_lsm_write_spec response: {}", e).into(),
                request_id,
                status_code: None,
            })?;

        let Some(body) = parsed.lsm_write_spec else {
            // The LSM write path is not enabled for this table.
            return Ok(None);
        };

        let spec = match body.sharding {
            Sharding::Bucket {
                column,
                num_buckets,
            } => LsmWriteSpec::bucket(column, num_buckets),
            Sharding::Identity { column } => LsmWriteSpec::identity(column),
            Sharding::Unsharded => LsmWriteSpec::unsharded(),
        }
        .with_maintained_indexes(body.maintained_indexes)
        .with_writer_config_defaults(body.writer_config_defaults);

        Ok(Some(spec))
    }
}
