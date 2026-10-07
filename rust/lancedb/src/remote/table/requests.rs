// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

impl<S: HttpSend> RemoteTable<S> {
    pub(super) async fn submit_create_index(
        &self,
        mut index: IndexBuilder,
    ) -> Result<Option<String>> {
        self.check_mutable().await?;
        let request = self
            .client
            .post(&format!("/v1/table/{}/create_index/", self.identifier));

        let column = match index.columns.len() {
            0 => {
                return Err(Error::InvalidInput {
                    message: "No columns specified".into(),
                });
            }
            1 => index.columns.pop().unwrap(),
            _ => {
                return Err(Error::NotSupported {
                    message: "Indices over multiple columns not yet supported".into(),
                });
            }
        };
        if matches!(
            &index.index,
            Index::FTS(params) if params.get_document_granularity().is_list_element()
        ) && !self.server_version.support_fts_document_granularity()
        {
            return Err(Error::NotSupported {
                message: "FTS document granularity requires remote server version 0.6.0 or later"
                    .into(),
            });
        }
        let schema = self.schema().await?;
        let (canonical_column, field) = match &index.index {
            Index::FTS(_) => resolve_arrow_fts_field_path(&schema, &column)?,
            _ => resolve_arrow_field_path(&schema, &column)?,
        };
        let mut body = serde_json::json!({
            "column": canonical_column
        });

        if !index.replace {
            body["replace"] = false.into();
        }

        // Add name parameter if provided (for backwards compatibility, only include if Some)
        if let Some(ref name) = index.name {
            body["name"] = serde_json::Value::String(name.clone());
        }

        // Warn if train=false is specified since it's not meaningful
        if !index.train {
            log::warn!(
                "train=false has no effect remote tables. The index will be created empty and automatically populated in the background."
            );
        }

        fn to_json(params: &impl serde::Serialize) -> crate::Result<serde_json::Value> {
            serde_json::to_value(params).map_err(|e| Error::InvalidInput {
                message: format!("failed to serialize index params {:?}", e),
            })
        }

        // Map each Index variant to its wire type name and serializable params.
        // Auto is special-cased since it needs schema inspection.
        let (index_type_str, params) = match &index.index {
            Index::IvfFlat(p) => ("IVF_FLAT", Some(to_json(p)?)),
            Index::IvfPq(p) => ("IVF_PQ", Some(to_json(p)?)),
            Index::IvfSq(p) => ("IVF_SQ", Some(to_json(p)?)),
            Index::IvfHnswSq(p) => ("IVF_HNSW_SQ", Some(to_json(p)?)),
            Index::IvfHnswFlat(p) => ("IVF_HNSW_FLAT", Some(to_json(p)?)),
            Index::IvfRq(p) => ("IVF_RQ", Some(to_json(p)?)),
            Index::BTree(p) => ("BTREE", Some(to_json(p)?)),
            Index::Bitmap(p) => ("BITMAP", Some(to_json(p)?)),
            Index::LabelList(p) => ("LABEL_LIST", Some(to_json(p)?)),
            Index::Fm(p) => ("FM", Some(to_json(p)?)),
            Index::ZoneMap(p) => ("ZONEMAP", Some(to_json(p)?)),
            Index::NGram(p) => ("NGRAM", Some(to_json(p)?)),
            Index::BloomFilter(p) => ("BLOOM_FILTER", Some(to_json(p)?)),
            Index::RTree(p) => ("RTREE", Some(to_json(p)?)),
            Index::FTS(p) => {
                let mut params = to_json(p)?;
                if p.get_document_granularity().is_list_element() {
                    params["document_granularity"] = "list_element".into();
                }
                ("FTS", Some(params))
            }
            Index::Auto => {
                if supported_vector_data_type(field.data_type()) {
                    body[METRIC_TYPE_KEY] =
                        serde_json::Value::String(DistanceType::L2.to_string().to_lowercase());
                    ("IVF_PQ", None)
                } else if supported_btree_data_type(field.data_type()) {
                    ("BTREE", None)
                } else {
                    return Err(Error::NotSupported {
                        message: format!(
                            "there are no indices supported for the field `{}` with the data type {}",
                            field.name(),
                            field.data_type()
                        ),
                    });
                }
            }
            _ => {
                return Err(Error::NotSupported {
                    message: "Index type not supported".into(),
                });
            }
        };

        body[INDEX_TYPE_KEY] = index_type_str.into();
        if let Some(params) = params {
            for (key, value) in params.as_object().expect("params should be a JSON object") {
                body[key] = value.clone();
            }
        }
        self.apply_branch_body(&mut body);

        let request = request.json(&body);

        let (request_id, response) = self.send(request, true).await?;

        let response = self.check_table_response(&request_id, response).await?;
        let job_id = response
            .text()
            .await
            .ok()
            .and_then(|body| extract_job_id(&body));

        if let Some(wait_timeout) = index.wait_timeout {
            let index_name = index.name.unwrap_or_else(|| format!("{}_idx", column));
            self.wait_for_index(&[&index_name], wait_timeout).await?;
        }

        Ok(job_id)
    }

    pub(in crate::remote) fn new_with_sql_client(
        client: RestfulLanceDbClient<S>,
        name: String,
        namespace: Vec<String>,
        identifier: String,
        server_version: ServerVersion,
        sql_client: Option<SqlClient>,
    ) -> Self {
        Self {
            client,
            name,
            namespace,
            identifier,
            server_version,
            sql_client,
            version: Arc::new(RwLock::new(None)),
            location: RwLock::new(None),
            schema_cache: BackgroundCache::new(SCHEMA_CACHE_TTL, SCHEMA_CACHE_REFRESH_WINDOW),
            wal_pk_fusion: PkFusionMemory::default(),
            freshness: Arc::new(Mutex::new(FreshnessState::default())),
            branch: None,
        }
    }

    /// Seed the schema cache from a `describe` body the caller already fetched.
    ///
    /// Best effort. `open_table` succeeds today without reading this body, so a
    /// body we cannot parse leaves the cache empty and the next schema read
    /// fetches it again through the path that reports a real error.
    pub(crate) fn seed_schema(&self, describe_body: &str) {
        let Ok(description) = serde_json::from_str::<TableDescription>(describe_body) else {
            return;
        };
        self.track_read_version(description.version);
        if let Ok(schema) = arrow_schema::Schema::try_from(description.schema) {
            self.schema_cache.seed(Arc::new(schema));
        }
    }

    /// Return a new handle scoped to `branch`, sharing the client but with fresh
    /// caches and version/freshness state (the branch tracks its own latest).
    /// Mirrors `NativeTable`'s handle-per-branch model.
    pub(super) fn with_branch(&self, branch: Option<String>) -> Self {
        Self {
            client: self.client.clone(),
            name: self.name.clone(),
            namespace: self.namespace.clone(),
            identifier: self.identifier.clone(),
            server_version: self.server_version.clone(),
            sql_client: self.sql_client.clone(),
            version: Arc::new(RwLock::new(None)),
            location: RwLock::new(None),
            schema_cache: BackgroundCache::new(SCHEMA_CACHE_TTL, SCHEMA_CACHE_REFRESH_WINDOW),
            wal_pk_fusion: PkFusionMemory::default(),
            freshness: Arc::new(Mutex::new(FreshnessState::default())),
            branch,
        }
    }

    /// Stamp the branch onto a request as a `?branch=` query param (used for
    /// Arrow-body / query-only ops). `None` (main) leaves the request unchanged,
    /// keeping it byte-identical to the non-branch path.
    pub(super) fn apply_branch_query(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.branch {
            Some(branch) => request.query(&[("branch", branch.as_str())]),
            None => request,
        }
    }

    /// Stamp the branch into a JSON request body under `"branch"` (used for JSON
    /// ops). `None` (main) leaves the body unchanged.
    pub(super) fn apply_branch_body(&self, body: &mut serde_json::Value) {
        if let Some(branch) = &self.branch {
            body["branch"] = serde_json::Value::String(branch.clone());
        }
    }

    pub(super) async fn describe(&self) -> Result<TableDescription> {
        self.describe_read_snapshot(self.snapshot_read_state().await)
            .await
    }

    pub(super) async fn describe_read_snapshot(
        &self,
        read_snapshot: ReadSnapshot,
    ) -> Result<TableDescription> {
        let request = self
            .client
            .post(&format!("/v1/table/{}/describe/", self.identifier));
        self.describe_with_request(
            request,
            read_snapshot.version,
            Some(read_snapshot.freshness),
        )
        .await
    }

    pub(super) async fn schema_read_snapshot(
        &self,
        read_snapshot: ReadSnapshot,
    ) -> Result<SchemaRef> {
        if read_snapshot.freshness.is_current(&self.freshness)
            && let Some(schema) = self.schema_cache.try_get()
            && read_snapshot.freshness.is_current(&self.freshness)
        {
            return Ok(schema);
        }

        let description = self.describe_read_snapshot(read_snapshot).await?;
        Ok(Arc::new(description.schema.try_into()?))
    }

    pub(super) async fn resolve_tag_version_with_request(
        &self,
        tag: &str,
        request: RequestBuilder,
        fenced: bool,
    ) -> Result<u64> {
        let request = request.json(&serde_json::json!({ "tag": tag }));

        let (request_id, response) = if fenced {
            self.send(request, true).await?
        } else {
            self.send_unfenced(request, true).await?
        };
        let response = self.check_table_response(&request_id, response).await?;

        match response.text().await {
            Ok(body) => {
                let value: serde_json::Value =
                    serde_json::from_str(&body).map_err(|e| Error::Http {
                        source: format!("Failed to parse tag version: {}", e).into(),
                        request_id: request_id.clone(),
                        status_code: None,
                    })?;

                value
                    .get("version")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| Error::Http {
                        source: format!("Invalid tag version response: {}", body).into(),
                        request_id,
                        status_code: None,
                    })
            }
            Err(err) => {
                let status_code = err.status();
                Err(Error::Http {
                    source: Box::new(err),
                    request_id,
                    status_code,
                })
            }
        }
    }

    /// Resolve a tag to its `(branch, version)` coordinate via the `tags/version`
    /// endpoint, since the `/branches/create` contract accepts no `from_tag`.
    pub(super) async fn resolve_tag_ref(&self, tag: &str) -> Result<(Option<String>, u64)> {
        let request = self
            .client
            .post(&format!("/v1/table/{}/tags/version/", self.identifier))
            .json(&serde_json::json!({ "tag": tag }));
        let (request_id, response) = self.send_unfenced(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let value: serde_json::Value = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse tag version: {}", e).into(),
            request_id: request_id.clone(),
            status_code: None,
        })?;
        let version = value
            .get("version")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| Error::Http {
                source: format!("Invalid tag version response: {}", body).into(),
                request_id,
                status_code: None,
            })?;
        let branch = value
            .get("branch")
            .and_then(|v| v.as_str())
            .map(String::from);
        Ok((normalize_branch(branch), version))
    }

    pub(super) async fn describe_with_request(
        &self,
        request: RequestBuilder,
        version: Option<u64>,
        freshness_request: Option<FreshnessHeaders>,
    ) -> Result<TableDescription> {
        let mut body = serde_json::json!({ "version": version });
        self.apply_branch_body(&mut body);
        let request = request.json(&body);

        let (request_id, response) = if let Some(freshness_request) = freshness_request {
            self.send_with_freshness(request, true, freshness_request)
                .await?
        } else {
            self.send_unfenced(request, true).await?
        };

        let response = self.check_table_response(&request_id, response).await?;

        match response.text().await {
            Ok(body) => {
                let description: TableDescription =
                    serde_json::from_str(&body).map_err(|e| Error::Http {
                        source: format!("Failed to parse table description: {}", e).into(),
                        request_id,
                        status_code: None,
                    })?;
                if let Some(freshness_request) = freshness_request {
                    freshness_request.observe_version(&self.freshness, description.version);
                }
                Ok(description)
            }
            Err(err) => {
                let status_code = err.status();
                Err(Error::Http {
                    source: Box::new(err),
                    request_id,
                    status_code,
                })
            }
        }
    }

    pub(super) async fn send(
        &self,
        req: RequestBuilder,
        with_retry: bool,
    ) -> Result<(String, Response)> {
        let freshness_request = self.snapshot_freshness_headers();
        self.send_with_freshness(req, with_retry, freshness_request)
            .await
    }

    pub(super) async fn send_with_freshness(
        &self,
        req: RequestBuilder,
        with_retry: bool,
        freshness_request: FreshnessHeaders,
    ) -> Result<(String, Response)> {
        let req = freshness_request.apply(req);
        let res = if with_retry {
            self.client.send_with_retry(req, None, true).await?
        } else {
            self.client.send(req).await?
        };
        if res.1.status().is_success() {
            freshness_request.observe_headers(&self.freshness, res.1.headers());
        }
        Ok(res)
    }

    pub(super) async fn send_unfenced(
        &self,
        req: RequestBuilder,
        with_retry: bool,
    ) -> Result<(String, Response)> {
        if with_retry {
            self.client.send_with_retry(req, None, true).await
        } else {
            self.client.send(req).await
        }
    }

    pub(in crate::remote) async fn handle_table_not_found(
        table_name: &str,
        response: reqwest::Response,
        request_id: &str,
    ) -> Result<reqwest::Response> {
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            let body = response.text().await.ok().unwrap_or_default();
            let request_error = Error::Http {
                source: body.into(),
                request_id: request_id.into(),
                status_code: Some(status),
            };
            return Err(Error::TableNotFound {
                name: table_name.to_string(),
                source: Box::new(request_error),
            });
        }
        Ok(response)
    }

    /// Check if a status code should trigger schema cache invalidation
    pub(super) fn should_invalidate_cache_for_status(status: StatusCode) -> bool {
        // Only invalidate for errors that could be schema-related
        // Don't invalidate for auth errors (401, 403) or temporary failures (503, 502)
        matches!(
            status,
            StatusCode::BAD_REQUEST // 400 - could be schema mismatch
            | StatusCode::NOT_FOUND // 404 - table might have been recreated
            | StatusCode::UNPROCESSABLE_ENTITY // 422 - schema validation error
            | StatusCode::INTERNAL_SERVER_ERROR // 500 - could be schema issue on server
        )
    }

    pub(super) async fn check_table_response(
        &self,
        request_id: &str,
        response: reqwest::Response,
    ) -> Result<reqwest::Response> {
        let status = response.status();
        let not_found_result = Self::handle_table_not_found(&self.name, response, request_id).await;

        // Check if we should invalidate cache for 404 errors
        if not_found_result.is_err() && Self::should_invalidate_cache_for_status(status) {
            self.invalidate_schema_cache();
        }

        let response = not_found_result?;
        let result = self.client.check_response(request_id, response).await;

        // Invalidate schema cache on errors that could be schema-related
        if result.is_err() && Self::should_invalidate_cache_for_status(status) {
            self.invalidate_schema_cache();
        }

        result
    }

    pub(super) async fn read_arrow_response(
        &self,
        request_id: &str,
        response: reqwest::Response,
    ) -> Result<SendableRecordBatchStream> {
        let response = self.check_table_response(request_id, response).await?;

        // The header has to be read before the body, which consumes the response.
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let framing = resolve_arrow_ipc_framing(content_type.as_deref(), request_id)?;

        // Buffer the whole body. File framing keeps its footer at the end, so /query
        // cannot decode incrementally. Stream framing could, via
        // arrow_ipc::reader::StreamDecoder, but fetch_blobs concatenates every batch
        // before returning, so no caller would see data sooner.
        let body = response.bytes().await.err_to_http(request_id.into())?;
        type IpcBatchIterator =
            Box<dyn Iterator<Item = std::result::Result<RecordBatch, ArrowError>> + Send>;
        let (schema, batches): (SchemaRef, IpcBatchIterator) = match framing {
            ArrowIpcFraming::Stream => {
                let reader = StreamReader::try_new(Cursor::new(body), None)?;
                (reader.schema(), Box::new(reader))
            }
            ArrowIpcFraming::File => {
                let reader = FileReader::try_new(Cursor::new(body), None)?;
                (reader.schema(), Box::new(reader))
            }
        };
        let stream = futures::stream::iter(batches).map_err(DataFusionError::from);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    pub(super) fn apply_query_params(
        &self,
        body: &mut serde_json::Value,
        params: &QueryRequest,
    ) -> Result<()> {
        params.check_filter()?;
        body["prefilter"] = params.prefilter.into();
        // Only forward use_lsm when explicitly set; a server that predates it
        // ignores the field and routes as it would by default.
        if let Some(use_lsm) = params.use_lsm {
            body["use_lsm"] = serde_json::Value::Bool(use_lsm);
        }
        if let Some(offset) = params.offset {
            body["offset"] = serde_json::Value::Number(serde_json::Number::from(offset));
        }

        // Server requires k.
        // use isize::MAX as usize to avoid overflow: https://github.com/lancedb/lancedb/issues/2211
        let limit = params.limit.unwrap_or(isize::MAX as usize);
        body["k"] = serde_json::Value::Number(serde_json::Number::from(limit));

        if let Some(filter) = &params.filter {
            let filter_sql = match filter {
                QueryFilter::Sql(sql) => sql.clone(),
                QueryFilter::Datafusion(expr) => expr_to_sql_string(expr)?,
                QueryFilter::Substrait(_) => {
                    return Err(Error::NotSupported {
                        message: "Substrait filters are not supported for remote queries"
                            .to_string(),
                    });
                }
            };
            body["filter"] = serde_json::Value::String(filter_sql);
        }

        match &params.select {
            Select::All => {}
            Select::Columns(columns) => {
                body["columns"] = serde_json::Value::Array(
                    columns
                        .iter()
                        .map(|s| serde_json::Value::String(s.clone()))
                        .collect(),
                );
            }
            Select::Dynamic(pairs) => {
                let alias_map =
                    serde_json::Map::from_iter(pairs.iter().map(|(name, expr)| {
                        (name.clone(), serde_json::Value::String(expr.clone()))
                    }));
                body["columns"] = alias_map.into();
            }
            Select::Expr(pairs) => {
                let alias_map: Result<serde_json::Map<String, serde_json::Value>> = pairs
                    .iter()
                    .map(|(name, expr)| {
                        expr_to_sql_string(expr)
                            .map(|sql| (name.clone(), serde_json::Value::String(sql)))
                    })
                    .collect();
                body["columns"] = alias_map?.into();
            }
        }

        if params.fast_search {
            body["fast_search"] = serde_json::Value::Bool(true);
        }

        if params.with_row_id {
            body["with_row_id"] = serde_json::Value::Bool(true);
        }

        if let Some(full_text_search) = &params.full_text_search {
            if full_text_search.wand_factor.is_some() {
                return Err(Error::NotSupported {
                    message: "Wand factor is not yet supported in LanceDB Cloud".into(),
                });
            }

            let requires_document_granularity_support =
                fts_query_requires_document_granularity_support(&full_text_search.query);
            if requires_document_granularity_support
                && !self.server_version.support_fts_document_granularity()
            {
                return Err(Error::NotSupported {
                    message:
                        "FTS document granularity requires remote server version 0.6.0 or later"
                            .into(),
                });
            }

            if self.server_version.support_structural_fts() {
                body["full_text_query"] = serde_json::json!({
                    "query": full_text_search.query.clone(),
                });
            } else {
                body["full_text_query"] = serde_json::json!({
                    "columns": full_text_search.columns().into_iter().collect::<Vec<_>>(),
                    "query": full_text_search.query.query(),
                })
            }
        }

        if let Some(order_by) = &params.order_by {
            body["order_by"] = serde_json::Value::Array(
                order_by
                    .iter()
                    .map(|o| {
                        serde_json::json!({
                            "column_name": o.column_name,
                            "ascending": o.ascending,
                            "nulls_first": o.nulls_first,
                        })
                    })
                    .collect(),
            );
        }

        Ok(())
    }

    pub(super) fn apply_vector_query_params(
        &self,
        mut body: serde_json::Value,
        query: &VectorQueryRequest,
    ) -> Result<Vec<serde_json::Value>> {
        self.apply_query_params(&mut body, &query.base)?;

        // Apply general parameters, before we dispatch based on number of query vectors.
        if let Some(distance_type) = query.distance_type {
            body["distance_type"] = serde_json::json!(distance_type);
        }
        if let Some(approx_mode) = query.approx_mode {
            body["approx_mode"] = serde_json::json!(approx_mode);
        }
        // In 0.23.1 we migrated from `nprobes` to `minimum_nprobes` and `maximum_nprobes`.
        // Old client / new server: since minimum_nprobes is missing, fallback to nprobes
        // New client / old server: old server will only see nprobes, make sure to set both
        //                          nprobes and minimum_nprobes
        // New client / new server: since minimum_nprobes is present, server can ignore nprobes
        body["nprobes"] = query.minimum_nprobes.into();
        body["minimum_nprobes"] = query.minimum_nprobes.into();
        if let Some(maximum_nprobes) = query.maximum_nprobes {
            body["maximum_nprobes"] = maximum_nprobes.into();
        } else {
            body["maximum_nprobes"] = serde_json::Value::Number(Number::from_u128(0).unwrap())
        }
        body["lower_bound"] = query.lower_bound.into();
        body["upper_bound"] = query.upper_bound.into();
        body["ef"] = query.ef.into();
        body["refine_factor"] = query.refine_factor.into();
        if let Some(vector_column) = query.column.as_ref() {
            body["vector_column"] = serde_json::Value::String(vector_column.clone());
        }
        if !query.use_index {
            body["bypass_vector_index"] = serde_json::Value::Bool(true);
        }

        fn vector_to_json(vector: &arrow_array::ArrayRef) -> Result<serde_json::Value> {
            match vector.data_type() {
                DataType::Float32 => {
                    let array = vector
                        .as_any()
                        .downcast_ref::<arrow_array::Float32Array>()
                        .unwrap();
                    Ok(serde_json::Value::Array(
                        array
                            .values()
                            .iter()
                            .map(|v| {
                                serde_json::Number::from_f64(*v as f64)
                                    .map(serde_json::Value::Number)
                                    .ok_or_else(|| Error::InvalidInput {
                                        message: "query vector must contain only finite values"
                                            .into(),
                                    })
                            })
                            .collect::<Result<Vec<_>>>()?,
                    ))
                }
                _ => Err(Error::InvalidInput {
                    message: "VectorQuery vector must be of type Float32".into(),
                }),
            }
        }

        let bodies = match query.query_vector.len() {
            0 => {
                // Server takes empty vector, not null or undefined.
                body["vector"] = serde_json::Value::Array(Vec::new());
                vec![body]
            }
            1 => {
                body["vector"] = vector_to_json(&query.query_vector[0])?;
                vec![body]
            }
            _ => {
                if self.server_version.support_multivector() {
                    let vectors = query
                        .query_vector
                        .iter()
                        .map(vector_to_json)
                        .collect::<Result<Vec<_>>>()?;
                    body["vector"] = serde_json::Value::Array(vectors);
                    vec![body]
                } else {
                    // Server does not support multiple vectors in a single query.
                    // We need to send multiple requests.
                    let mut bodies = Vec::with_capacity(query.query_vector.len());
                    for vector in &query.query_vector {
                        let mut body = body.clone();
                        body["vector"] = vector_to_json(vector)?;
                        bodies.push(body);
                    }
                    bodies
                }
            }
        };

        Ok(bodies)
    }

    pub(super) async fn create_multipart_write(&self) -> Result<String> {
        let request = self.apply_branch_query(self.client.post(&format!(
            "/v1/table/{}/multipart_write/create",
            self.identifier
        )));
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let parsed: serde_json::Value = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse multipart create response: {}", e).into(),
            request_id,
            status_code: None,
        })?;
        parsed["upload_id"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| Error::Http {
                source: "Missing upload_id in multipart create response".into(),
                request_id: String::new(),
                status_code: None,
            })
    }

    pub(super) async fn complete_multipart_write(&self, upload_id: &str) -> Result<AddResult> {
        let request = self.apply_branch_query(
            self.client
                .post(&format!(
                    "/v1/table/{}/multipart_write/complete",
                    self.identifier
                ))
                .query(&[("upload_id", upload_id)]),
        );
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let parsed: serde_json::Value = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse multipart complete response: {}", e).into(),
            request_id,
            status_code: None,
        })?;
        let version = parsed["version"].as_u64().ok_or_else(|| Error::Http {
            source: "Missing version in multipart complete response".into(),
            request_id: String::new(),
            status_code: None,
        })?;
        Ok(AddResult { version })
    }

    pub(super) async fn abort_multipart_write(&self, upload_id: &str) -> Result<()> {
        let request = self.apply_branch_query(
            self.client
                .post(&format!(
                    "/v1/table/{}/multipart_write/abort",
                    self.identifier
                ))
                .query(&[("upload_id", upload_id)]),
        );
        let (request_id, response) = self.send(request, true).await?;
        self.check_table_response(&request_id, response).await?;
        Ok(())
    }

    pub(super) async fn check_mutable(&self) -> Result<()> {
        let read_guard = self.version.read().await;
        match *read_guard {
            None => Ok(()),
            Some(version) => Err(Error::NotSupported {
                message: format!(
                    "Cannot mutate table reference fixed at version {}. Call checkout_latest() to get a mutable table reference.",
                    version
                ),
            }),
        }
    }

    pub(super) async fn snapshot_read_state(&self) -> ReadSnapshot {
        let version = self.version.read().await;
        let (freshness_state, freshness) =
            freshness_state_snapshot(&self.freshness, self.client.read_consistency_interval);
        ReadSnapshot {
            version: *version,
            freshness_state,
            freshness,
        }
    }

    /// Snapshot the freshness headers to attach to a single table request.
    /// Computed at call time so that retries reuse the same snapshot.
    pub(super) fn snapshot_freshness_headers(&self) -> FreshnessHeaders {
        freshness_headers_snapshot(&self.freshness, self.client.read_consistency_interval)
    }

    pub(super) fn reset_freshness(&self, checkout_baseline: Option<SystemTime>, pinned: bool) {
        let mut state = self.freshness.lock().unwrap();
        let generation = state.generation.wrapping_add(1);
        *state = FreshnessState {
            generation,
            pinned,
            checkout_baseline,
            ..FreshnessState::default()
        };
    }

    /// Send an LSM operator request with the transport retry layer **off**.
    ///
    /// Retry policy on these routes belongs to the checkpoint loop, which
    /// reads the status and can tell contention from a lost claim. Leaving the
    /// transport layer on would re-ask on its own schedule first, and surface
    /// an `Error::Retry` whose status the loop would then have to unwrap.
    pub(super) async fn send_lsm_route(
        &self,
        request: RequestBuilder,
    ) -> Result<(String, reqwest::Response)> {
        let (request_id, response) = self.send(request, false).await?;
        let response = self.check_table_response(&request_id, response).await?;
        Ok((request_id, response))
    }

    /// Record a version returned by a write so subsequent reads can request at
    /// least that version via `x-lancedb-min-version`. A returned `0` from a
    /// backward-compatible old server is ignored.
    pub(super) fn track_write_version(&self, freshness_request: FreshnessHeaders, version: u64) {
        if version == 0 {
            return;
        }
        freshness_request.update_if_current(&self.freshness, |state| {
            state.min_version = Some(state.min_version.map_or(version, |v| v.max(version)));
        });
    }

    /// Record a committed dataset version observed in a table response so
    /// subsequent requests ask for at least this version via
    /// `x-lancedb-min-read-version`,
    /// giving monotonic reads across load-balanced query nodes. A returned `0`
    /// (or absent header from an old server) is ignored.
    pub(super) fn track_read_version(&self, version: u64) {
        track_read_version(&self.freshness, version);
    }

    pub(super) async fn execute_query(
        &self,
        query: &AnyQuery,
        options: &QueryExecutionOptions,
    ) -> Result<Vec<Pin<Box<dyn RecordBatchStream + Send>>>> {
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/query/", self.identifier));

        if let Some(timeout) = options.timeout {
            // Also send to server, so it can abort the query if it takes too long.
            // (If it doesn't fit into u64, it's not worth sending anyways.)
            if let Ok(timeout_ms) = u64::try_from(timeout.as_millis()) {
                request = request.header(REQUEST_TIMEOUT_HEADER, timeout_ms);
            }
        }

        let read_snapshot = self.snapshot_read_state().await;
        let query_bodies = self.prepare_query_bodies(query, read_snapshot.version)?;
        let requests: Vec<reqwest::RequestBuilder> = query_bodies
            .into_iter()
            .map(|body| request.try_clone().unwrap().json(&body))
            .collect();

        let futures = requests.into_iter().map(|req| async move {
            let (request_id, response) = self
                .send_with_freshness(req, true, read_snapshot.freshness)
                .await?;
            self.read_arrow_response(&request_id, response).await
        });
        let streams = futures::future::try_join_all(futures);

        if let Some(timeout) = options.timeout {
            let timeout_future = tokio::time::sleep(timeout);
            tokio::pin!(timeout_future);
            tokio::pin!(streams);
            tokio::select! {
                _ = &mut timeout_future => {
                    Err(Error::Other {
                        message: format!("Query timeout after {} ms", timeout.as_millis()),
                        source: None,
                    })
                }
                result = &mut streams => {
                    Ok(result?)
                }
            }
        } else {
            Ok(streams.await?)
        }
    }

    pub(super) fn prepare_query_bodies(
        &self,
        query: &AnyQuery,
        version: Option<u64>,
    ) -> Result<Vec<serde_json::Value>> {
        let query = query.canonicalized()?;
        let mut base_body = serde_json::json!({ "version": version });
        self.apply_branch_body(&mut base_body);

        match &query {
            AnyQuery::Query(query) => {
                let mut body = base_body.clone();
                self.apply_query_params(&mut body, query)?;
                // Empty vector can be passed if no vector search is performed.
                body["vector"] = serde_json::Value::Array(Vec::new());
                Ok(vec![body])
            }
            AnyQuery::VectorQuery(query) => self.apply_vector_query_params(base_body, query),
        }
    }

    pub(super) fn invalidate_schema_cache(&self) {
        self.schema_cache.invalidate();
    }

    pub(super) fn handle_error_invalidation(&self, error: &Error) {
        let status_code = match error {
            Error::Http { status_code, .. } => *status_code,
            Error::Retry { status_code, .. } => *status_code,
            _ => None,
        };
        if let Some(status_code) = status_code
            && Self::should_invalidate_cache_for_status(status_code)
        {
            self.invalidate_schema_cache();
        }
    }
}
