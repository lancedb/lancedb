// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

impl<S: HttpSend> RemoteTable<S> {
    pub(super) async fn add_columns_impl(
        &self,
        transforms: NewColumnTransform,
        _read_columns: Option<Vec<String>>,
    ) -> Result<AddColumnsResult> {
        self.check_mutable().await?;
        crate::table::computed_columns::ensure_not_function_bound(
            self.schema().await?.as_ref(),
            "schema evolution",
            crate::table::schema_evolution::new_column_names(&transforms),
        )?;
        let path = format!("/v1/table/{}/add_columns/", self.identifier);
        let request = match transforms {
            NewColumnTransform::SqlExpressions(expressions) => {
                let body = expressions
                    .into_iter()
                    .map(|(name, expression)| {
                        serde_json::json!({
                            "name": name,
                            "expression": expression,
                        })
                    })
                    .collect::<Vec<_>>();
                let mut body = serde_json::json!({ "new_columns": body });
                self.apply_branch_body(&mut body);
                self.client.post(&path).json(&body)
            }
            // Every field becomes a column that reads null for existing
            // rows. The schema goes over the wire as Arrow IPC rather than
            // a JSON description of the types: that is what the server
            // takes, and it is the only encoding that round-trips a field
            // whole, including decimal precision and a timestamp's unit and
            // timezone. With no JSON envelope the branch rides the query
            // string, as it does for the other binary-bodied endpoints.
            NewColumnTransform::AllNulls(schema) => {
                let body = write_ipc_schema(schema.as_ref())?;
                self.apply_branch_query(
                    self.client
                        .post(&path)
                        .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
                        .body(body),
                )
            }
            _ => {
                return Err(Error::NotSupported {
                    message: "Only SQL expressions and all-null column schemas are supported \
                              for adding columns"
                        .into(),
                });
            }
        };

        let freshness_request = self.snapshot_freshness_headers();
        let (request_id, response) = self
            .send_with_freshness(request, true, freshness_request)
            .await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        if body.trim().is_empty() {
            // Backward compatible with old servers
            return Ok(AddColumnsResult { version: 0 });
        }

        let result: AddColumnsResult = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse add_columns response: {}", e).into(),
            request_id,
            status_code: None,
        })?;

        self.invalidate_schema_cache();
        self.track_write_version(freshness_request, result.version);

        Ok(result)
    }

    pub(super) async fn add_computed_columns_impl(
        &self,
        columns: &[(String, String)],
    ) -> Result<AddColumnsResult> {
        self.check_mutable().await?;
        crate::table::computed_columns::ensure_not_function_bound(
            self.schema().await?.as_ref(),
            "schema evolution",
            columns.iter().map(|(name, _)| name),
        )?;
        // The server plans the declaration against its table schema, including
        // Blob v2 semantics inherited by a direct field projection.
        let entries = columns
            .iter()
            .map(
                |(name, expression)| lance_namespace::models::AddColumnsEntry {
                    name: name.clone(),
                    computed: Some(Some(expression.clone())),
                    ..Default::default()
                },
            )
            .collect::<Vec<_>>();
        let mut body = serde_json::json!({ "new_columns": entries });
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!("/v1/table/{}/add_columns/", self.identifier))
            .json(&body);
        let freshness_request = self.snapshot_freshness_headers();
        let (request_id, response) = self
            .send_with_freshness(request, true, freshness_request)
            .await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        if body.trim().is_empty() {
            // Backward compatible with old servers
            return Ok(AddColumnsResult { version: 0 });
        }

        let result: AddColumnsResult = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse add_columns response: {}", e).into(),
            request_id,
            status_code: None,
        })?;

        self.invalidate_schema_cache();
        self.track_write_version(freshness_request, result.version);

        Ok(result)
    }

    pub(super) async fn add_function_columns_impl(
        &self,
        application: &crate::function::FunctionApplication,
        output_name: Option<&str>,
    ) -> Result<AddColumnsResult> {
        self.check_mutable().await?;
        let schema = self.schema().await?;
        let plan = crate::table::computed_columns::plan_function_application(
            schema.as_ref(),
            application,
            output_name,
        )?;
        let new_columns = plan
            .outputs
            .iter()
            .map(|output| {
                serde_json::json!({
                    "name": output.output_name,
                    "all_null": true,
                })
            })
            .collect::<Vec<_>>();
        let mut body = serde_json::json!({
            "new_columns": new_columns,
            "function": {
                "application": plan.application,
                "binding_metadata_version": plan.binding_metadata_version,
                "input_bindings": plan.input_bindings,
                "input_schema": plan.input_schema,
                "output_schema": plan.output_schema,
                "outputs": plan.outputs,
            },
        });
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!("/v1/table/{}/add_columns/", self.identifier))
            .json(&body);
        let freshness_request = self.snapshot_freshness_headers();
        let (request_id, response) = self
            .send_with_freshness(request, true, freshness_request)
            .await?;
        // A Function declaration can return 404 for the Function rather than the table.
        let response = self
            .client
            .check_response(&request_id, response)
            .await
            .inspect_err(|error| self.handle_error_invalidation(error))?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        if body.trim().is_empty() {
            return Ok(AddColumnsResult { version: 0 });
        }

        let result: AddColumnsResult = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse add Function columns response: {e}").into(),
            request_id,
            status_code: None,
        })?;

        self.invalidate_schema_cache();
        self.track_write_version(freshness_request, result.version);
        Ok(result)
    }

    pub(super) async fn refresh_column_async_impl(
        &self,
        column: &str,
    ) -> Result<Job<crate::function::RefreshColumnResult>> {
        self.check_mutable().await?;
        let mut body = serde_json::json!({ "column": column });
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!("/v1/table/{}/backfill_column", self.identifier))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        // Preserve dependency errors: a deleted bound Function also returns 404.
        let response = self
            .client
            .check_response(&request_id, response)
            .await
            .inspect_err(|error| self.handle_error_invalidation(error))?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        #[derive(serde::Deserialize)]
        struct BackfillResponse {
            job_id: String,
        }
        let response: BackfillResponse = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse backfill_column response: {}", e).into(),
            request_id,
            status_code: None,
        })?;

        Ok(Job::new_typed(Box::new(FreshnessJob {
            inner: RemoteJob::new(self.client.clone(), response.job_id),
            freshness: self.freshness.clone(),
            version: self.version.clone(),
            tracked_result: TrackedJobResult::RefreshColumn,
            freshness_request: self.snapshot_freshness_headers(),
        })))
    }

    pub(super) async fn function_errors_impl(
        &self,
        request: &crate::function::FunctionErrorsRequest,
    ) -> Result<crate::function::FunctionErrors> {
        let mut body = serde_json::json!({});
        if let Some(job_id) = &request.job_id {
            body["job_id"] = serde_json::json!(job_id);
        }
        if let Some(column) = &request.column {
            body["column"] = serde_json::json!(column);
        }
        if let Some(limit) = request.limit {
            body["limit"] = serde_json::json!(limit);
        }
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!("/v1/table/{}/errors", self.identifier))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse errors response: {}", e).into(),
            request_id,
            status_code: None,
        })
    }

    pub(super) async fn alter_columns_impl(
        &self,
        alterations: &[ColumnAlteration],
    ) -> Result<AlterColumnsResult> {
        self.check_mutable().await?;
        let body = alterations
            .iter()
            .map(|alteration| {
                let mut value = serde_json::json!({
                    "path": alteration.path,
                });
                if let Some(rename) = &alteration.rename {
                    value["rename"] = serde_json::Value::String(rename.clone());
                }
                if let Some(data_type) = &alteration.data_type {
                    let json_data_type = JsonDataType::try_from(data_type).unwrap();
                    let json_data_type = serde_json::to_value(&json_data_type).unwrap();
                    value["data_type"] = json_data_type;
                }
                if let Some(nullable) = &alteration.nullable {
                    value["nullable"] = serde_json::Value::Bool(*nullable);
                }
                value
            })
            .collect::<Vec<_>>();
        let mut body = serde_json::json!({ "alterations": body });
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!("/v1/table/{}/alter_columns/", self.identifier))
            .json(&body);
        let freshness_request = self.snapshot_freshness_headers();
        let (request_id, response) = self
            .send_with_freshness(request, true, freshness_request)
            .await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        if body.trim().is_empty() {
            // Backward compatible with old servers
            return Ok(AlterColumnsResult { version: 0 });
        }

        let result: AlterColumnsResult = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse alter_columns response: {}", e).into(),
            request_id,
            status_code: None,
        })?;

        self.invalidate_schema_cache();
        self.track_write_version(freshness_request, result.version);

        Ok(result)
    }

    pub(super) async fn update_field_metadata_impl(
        &self,
        updates: &[FieldMetadataUpdate],
    ) -> Result<UpdateFieldMetadataResult> {
        self.check_mutable().await?;
        let mut body = serde_json::json!({ "updates": updates });
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!(
                "/v1/table/{}/update_field_metadata/",
                self.identifier
            ))
            .json(&body);
        let freshness_request = self.snapshot_freshness_headers();
        let (request_id, response) = self
            .send_with_freshness(request, true, freshness_request)
            .await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        let result: UpdateFieldMetadataResult =
            serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("Failed to parse update_field_metadata response: {}", e).into(),
                request_id,
                status_code: None,
            })?;

        self.invalidate_schema_cache();
        self.track_write_version(freshness_request, result.version);
        Ok(result)
    }

    pub(super) async fn drop_columns_impl(&self, columns: &[&str]) -> Result<DropColumnsResult> {
        self.check_mutable().await?;
        let mut body = serde_json::json!({ "columns": columns });
        self.apply_branch_body(&mut body);
        let request = self
            .client
            .post(&format!("/v1/table/{}/drop_columns/", self.identifier))
            .json(&body);
        let freshness_request = self.snapshot_freshness_headers();
        let (request_id, response) = self
            .send_with_freshness(request, true, freshness_request)
            .await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        if body.trim().is_empty() {
            // Backward compatible with old servers
            return Ok(DropColumnsResult { version: 0 });
        }

        let result: DropColumnsResult = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse drop_columns response: {}", e).into(),
            request_id,
            status_code: None,
        })?;

        self.invalidate_schema_cache();
        self.track_write_version(freshness_request, result.version);

        Ok(result)
    }
}
