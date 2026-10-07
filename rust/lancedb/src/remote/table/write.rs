// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

impl<S: HttpSend + 'static> RemoteTable<S> {
    pub(super) fn is_retryable_write_error(&self, err: &Error) -> bool {
        match err {
            Error::Http {
                source,
                status_code,
                ..
            } => {
                // Don't retry read errors (is_body/is_decode): the
                // server may have committed the write already, and
                // without an idempotency key we'd duplicate data.
                source
                    .downcast_ref::<reqwest::Error>()
                    .is_some_and(|e| e.is_connect())
                    || status_code.is_some_and(|s| self.client.retry_config.statuses.contains(&s))
            }
            // send_with_retry exhausted its internal retries on a retryable
            // status. The outer loop can still retry the whole operation with
            // a fresh session.
            Error::Retry { status_code, .. } => {
                status_code.is_some_and(|s| self.client.retry_config.statuses.contains(&s))
            }
            _ => false,
        }
    }

    pub(super) async fn add_single_partition(
        &self,
        output: PreprocessingOutput,
    ) -> Result<AddResult> {
        use crate::remote::retry::RetryCounter;

        let _guard = output.tracker.as_ref().map(|t| t.track_task());
        let freshness_request = self.snapshot_freshness_headers();

        let mut insert: Arc<dyn ExecutionPlan> = Arc::new(
            RemoteWriteExec::new(
                self.name.clone(),
                self.identifier.clone(),
                self.client.clone(),
                output.plan,
                WriteOp::Insert {
                    overwrite: output.overwrite,
                },
                output.tracker.clone(),
                self.branch.clone(),
            )
            .with_freshness(
                self.freshness.clone(),
                self.client.read_consistency_interval,
            ),
        );

        let mut retry_counter =
            RetryCounter::new(&self.client.retry_config, uuid::Uuid::new_v4().to_string());

        loop {
            let stream = execute_plan(insert.clone(), Default::default())?;
            let result: Result<Vec<_>> = stream.try_collect().await.map_err(Error::from);

            match result {
                Ok(_) => {
                    let add_result = (insert.as_ref() as &dyn std::any::Any)
                        .downcast_ref::<RemoteWriteExec<S>>()
                        .and_then(|i| i.add_result())
                        .unwrap_or(AddResult { version: 0 });

                    if output.overwrite {
                        self.invalidate_schema_cache();
                    }
                    self.track_write_version(freshness_request, add_result.version);

                    return Ok(add_result);
                }
                Err(err) if output.rescannable && self.is_retryable_write_error(&err) => {
                    retry_counter.increment_from_error(err)?;
                    tokio::time::sleep(retry_counter.next_sleep_time()).await;
                    insert = insert.reset_state()?;
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub(super) async fn add_multipart(
        &self,
        output: PreprocessingOutput,
        num_partitions: usize,
    ) -> Result<AddResult> {
        use crate::remote::retry::RetryCounter;

        let mut retry_counter =
            RetryCounter::new(&self.client.retry_config, uuid::Uuid::new_v4().to_string());

        loop {
            let freshness_request = self.snapshot_freshness_headers();
            let upload_id = self.create_multipart_write().await?;

            let result = self
                .execute_multipart_inserts(&upload_id, &output, num_partitions)
                .await;

            match result {
                Ok(()) => match self.complete_multipart_write(&upload_id).await {
                    Ok(result) => {
                        if output.overwrite {
                            self.invalidate_schema_cache();
                        }
                        self.track_write_version(freshness_request, result.version);
                        return Ok(result);
                    }
                    Err(e) => {
                        if let Err(abort_err) = self.abort_multipart_write(&upload_id).await {
                            log::warn!(
                                "Failed to abort multipart write {}: {}",
                                upload_id,
                                abort_err
                            );
                        }
                        if output.rescannable && self.is_retryable_write_error(&e) {
                            retry_counter.increment_from_error(e)?;
                            tokio::time::sleep(retry_counter.next_sleep_time()).await;
                            continue;
                        }
                        return Err(e);
                    }
                },
                Err(e) => {
                    if let Err(abort_err) = self.abort_multipart_write(&upload_id).await {
                        log::warn!(
                            "Failed to abort multipart write {}: {}",
                            upload_id,
                            abort_err
                        );
                    }
                    if output.rescannable && self.is_retryable_write_error(&e) {
                        retry_counter.increment_from_error(e)?;
                        tokio::time::sleep(retry_counter.next_sleep_time()).await;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    pub(super) async fn execute_multipart_inserts(
        &self,
        upload_id: &str,
        output: &PreprocessingOutput,
        num_partitions: usize,
    ) -> Result<()> {
        debug_assert!(
            output.rescannable,
            "multipart inserts require rescannable input for retry support"
        );

        let plan = Arc::new(
            datafusion_physical_plan::repartition::RepartitionExec::try_new(
                output.plan.clone(),
                datafusion_physical_plan::Partitioning::RoundRobinBatch(num_partitions),
            )?,
        ) as Arc<dyn ExecutionPlan>;

        let insert = Arc::new(
            RemoteWriteExec::new_multipart(
                self.name.clone(),
                self.identifier.clone(),
                self.client.clone(),
                plan,
                output.overwrite,
                upload_id.to_string(),
                output.tracker.clone(),
                self.branch.clone(),
                self.client.max_bytes_per_request(),
                self.client.max_request_duration(),
            )
            .with_freshness(
                self.freshness.clone(),
                self.client.read_consistency_interval,
            ),
        );

        let task_ctx = Arc::new(datafusion_execution::TaskContext::default());
        let tracker = output.tracker.clone();
        let mut join_set = tokio::task::JoinSet::new();
        for partition in 0..num_partitions {
            let exec = insert.clone();
            let ctx = task_ctx.clone();
            let tracker = tracker.clone();
            join_set.spawn(async move {
                let _guard = tracker.as_ref().map(|t| t.track_task());
                let mut stream = exec
                    .execute(partition, ctx)
                    .map_err(|e| -> Error { e.into() })?;
                while let Some(batch) = stream.next().await {
                    batch.map_err(|e| -> Error { e.into() })?;
                }
                Ok::<_, Error>(())
            });
        }

        // JoinSet aborts all remaining tasks when dropped, so if we return
        // early on error the orphaned tasks are automatically cancelled.
        while let Some(result) = join_set.join_next().await {
            result.map_err(|e| Error::Runtime {
                message: format!("Insert task panicked: {}", e),
            })??;
        }

        Ok(())
    }
}
