// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! DataFusion ExecutionPlan for streaming writes (add / merge_insert) to
//! remote LanceDB tables.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_ipc::CompressionType;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::StreamExt;
use http::header::CONTENT_TYPE;
use lance::io::exec::utils::InstrumentedRecordBatchStreamAdapter;

use crate::Error;
use crate::remote::ARROW_STREAM_CONTENT_TYPE;
use crate::remote::client::{HttpSend, RestfulLanceDbClient, Sender};
use crate::remote::table::{
    FreshnessHeaders, FreshnessState, MergeInsertRequest, REQUEST_TIMEOUT_HEADER, RemoteTable,
    freshness_headers_snapshot,
};
use crate::table::datafusion::insert::COUNT_SCHEMA;
use crate::table::write_progress::WriteProgressTracker;
use crate::table::{AddResult, MergeResult};

/// The write operation a [`RemoteWriteExec`] performs. Both variants share the
/// same Arrow-IPC streaming body and error side-channel; only the target
/// endpoint, query parameters, and parsed result type differ.
#[derive(Debug, Clone)]
pub enum WriteOp {
    /// `add`: stream to `/v1/table/{id}/insert/`, optionally overwriting.
    Insert { overwrite: bool },
    /// `merge_insert`: stream to `/v1/table/{id}/merge_insert/` with the merge
    /// parameters carried as query params. Multipart is not supported for this
    /// operation (the server has no multipart merge_insert endpoint), so an
    /// `upload_id` combined with this op is a programming error.
    MergeInsert {
        query: MergeInsertRequest,
        timeout: Option<Duration>,
    },
}

/// The parsed server response for a completed write, discriminated by the
/// operation that produced it.
#[derive(Debug, Clone)]
pub enum WriteResult {
    Add(AddResult),
    Merge(MergeResult),
}

#[derive(Debug, Clone, Default)]
struct WriteFreshness {
    state: Option<Arc<Mutex<FreshnessState>>>,
    read_consistency_interval: Option<Duration>,
}

impl WriteFreshness {
    fn prepare(
        &self,
        request: reqwest::RequestBuilder,
    ) -> (reqwest::RequestBuilder, Option<FreshnessHeaders>) {
        match &self.state {
            Some(state) => {
                let freshness_request =
                    freshness_headers_snapshot(state, self.read_consistency_interval);
                (freshness_request.apply(request), Some(freshness_request))
            }
            None => (request, None),
        }
    }

    fn observe(
        &self,
        freshness_request: Option<FreshnessHeaders>,
        headers: &reqwest::header::HeaderMap,
    ) {
        if let (Some(state), Some(freshness_request)) = (&self.state, freshness_request) {
            freshness_request.observe_headers(state, headers);
        }
    }
}

/// ExecutionPlan for streaming a write (add or merge_insert) to a remote
/// LanceDB table.
///
/// Streams data as Arrow IPC to the endpoint selected by [`WriteOp`]. Both
/// operations reuse the same error side-channel so an input stream error (e.g.
/// NaN rejection) surfaces with its original message rather than the masked
/// HTTP error Hyper produces when a request body stream fails under HTTP2.
///
/// When `upload_id` is set, inserts are staged as part of a multipart write
/// session and the plan supports multiple partitions for parallel uploads.
/// Without `upload_id`, the plan requires a single partition and commits
/// immediately. Multipart applies to `add` only.
#[derive(Debug)]
pub struct RemoteWriteExec<S: HttpSend = Sender> {
    table_name: String,
    identifier: String,
    client: RestfulLanceDbClient<S>,
    freshness: WriteFreshness,
    input: Arc<dyn ExecutionPlan>,
    op: WriteOp,
    properties: Arc<PlanProperties>,
    result: Arc<Mutex<Option<WriteResult>>>,
    metrics: ExecutionPlanMetricsSet,
    upload_id: Option<String>,
    tracker: Option<Arc<WriteProgressTracker>>,
    /// Branch to write to via `?branch=`. `None` targets the main branch.
    branch: Option<String>,
    /// For multipart writes, split each partition into parts of at most this
    /// many bytes, each uploaded as a separate request. `None` sends the whole
    /// partition as a single request.
    max_bytes_per_request: Option<u64>,
    /// For multipart writes, also cut a part once it has been uploading for this
    /// long, even if it has not reached `max_bytes_per_request`. Bounds request
    /// duration on slow/throttled uploads so no request exceeds the read
    /// timeout. `None` disables the time-based cut.
    max_request_duration: Option<Duration>,
}

impl<S: HttpSend + 'static> RemoteWriteExec<S> {
    /// Create a new single-partition RemoteWriteExec.
    pub fn new(
        table_name: String,
        identifier: String,
        client: RestfulLanceDbClient<S>,
        input: Arc<dyn ExecutionPlan>,
        op: WriteOp,
        tracker: Option<Arc<WriteProgressTracker>>,
        branch: Option<String>,
    ) -> Self {
        Self::new_inner(
            table_name, identifier, client, input, op, None, tracker, branch, None, None,
        )
    }

    /// Create a multi-partition RemoteWriteExec for use with multipart writes.
    ///
    /// Each partition's insert is staged under the given `upload_id` without
    /// committing. The caller is responsible for calling the complete (or abort)
    /// endpoint after all partitions finish. Multipart is insert-only, so the
    /// op is fixed to [`WriteOp::Insert`].
    #[allow(clippy::too_many_arguments)]
    pub fn new_multipart(
        table_name: String,
        identifier: String,
        client: RestfulLanceDbClient<S>,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
        upload_id: String,
        tracker: Option<Arc<WriteProgressTracker>>,
        branch: Option<String>,
        max_bytes_per_request: Option<u64>,
        max_request_duration: Option<Duration>,
    ) -> Self {
        Self::new_inner(
            table_name,
            identifier,
            client,
            input,
            WriteOp::Insert { overwrite },
            Some(upload_id),
            tracker,
            branch,
            max_bytes_per_request,
            max_request_duration,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_inner(
        table_name: String,
        identifier: String,
        client: RestfulLanceDbClient<S>,
        input: Arc<dyn ExecutionPlan>,
        op: WriteOp,
        upload_id: Option<String>,
        tracker: Option<Arc<WriteProgressTracker>>,
        branch: Option<String>,
        max_bytes_per_request: Option<u64>,
        max_request_duration: Option<Duration>,
    ) -> Self {
        let num_partitions = if upload_id.is_some() {
            input.output_partitioning().partition_count()
        } else {
            1
        };
        let schema = COUNT_SCHEMA.clone();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            datafusion_physical_plan::Partitioning::UnknownPartitioning(num_partitions),
            datafusion_physical_plan::execution_plan::EmissionType::Final,
            datafusion_physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            table_name,
            identifier,
            client,
            freshness: WriteFreshness::default(),
            input,
            op,
            properties: Arc::new(properties),
            result: Arc::new(Mutex::new(None)),
            metrics: ExecutionPlanMetricsSet::new(),
            upload_id,
            tracker,
            branch,
            max_bytes_per_request,
            max_request_duration,
        }
    }

    pub(super) fn with_freshness(
        mut self,
        state: Arc<Mutex<FreshnessState>>,
        read_consistency_interval: Option<Duration>,
    ) -> Self {
        self.freshness = WriteFreshness {
            state: Some(state),
            read_consistency_interval,
        };
        self
    }

    /// Get the add result after execution, if this exec ran an insert.
    pub fn add_result(&self) -> Option<AddResult> {
        match self
            .result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            Some(WriteResult::Add(r)) => Some(r),
            _ => None,
        }
    }

    /// Get the merge result after execution, if this exec ran a merge_insert.
    pub fn merge_result(&self) -> Option<MergeResult> {
        match self
            .result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            Some(WriteResult::Merge(r)) => Some(r),
            _ => None,
        }
    }

    /// Stream the input into an HTTP body as an Arrow IPC stream, capturing any
    /// stream errors into the provided channel. Errors from the input plan
    /// (e.g. NaN rejection) would otherwise be swallowed inside the HTTP body
    /// upload; by stashing them in the channel we can surface them with their
    /// original message after the request completes.
    fn stream_as_http_body(
        data: SendableRecordBatchStream,
        error_tx: tokio::sync::oneshot::Sender<DataFusionError>,
        tracker: Option<Arc<WriteProgressTracker>>,
    ) -> DataFusionResult<reqwest::Body> {
        let data: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            data.schema(),
            data.flat_map(|batch| match batch {
                Ok(batch) => futures::stream::iter(
                    crate::remote::util::split_batch(batch, crate::remote::util::IPC_BATCH_BYTES)
                        .map(|batch| batch.map_err(Into::into)),
                )
                .left_stream(),
                Err(err) => futures::stream::iter([Err(err)]).right_stream(),
            }),
        ));
        let options = arrow_ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))?;
        let writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
            Vec::new(),
            &data.schema(),
            options,
        )?;

        let stream = futures::stream::try_unfold(
            (data, writer, Some(error_tx), false),
            move |(mut data, mut writer, error_tx, finished)| {
                let tracker = tracker.clone();
                async move {
                    if finished {
                        return Ok(None);
                    }
                    match data.next().await {
                        Some(Ok(batch)) => {
                            writer
                                .write(&batch)
                                .map_err(|e| std::io::Error::other(e.to_string()))?;
                            let buffer = std::mem::take(writer.get_mut());
                            if let Some(ref t) = tracker {
                                t.record_bytes(buffer.len());
                            }
                            Ok(Some((buffer, (data, writer, error_tx, false))))
                        }
                        Some(Err(e)) => {
                            // Send the original error through the channel before
                            // returning a generic error to reqwest.
                            if let Some(tx) = error_tx {
                                let _ = tx.send(e);
                            }
                            Err(std::io::Error::other(
                                "input stream error (see error channel)",
                            ))
                        }
                        None => {
                            writer
                                .finish()
                                .map_err(|e| std::io::Error::other(e.to_string()))?;
                            let buffer = std::mem::take(writer.get_mut());
                            if buffer.is_empty() {
                                Ok(None)
                            } else {
                                if let Some(ref t) = tracker {
                                    t.record_bytes(buffer.len());
                                }
                                Ok(Some((buffer, (data, writer, None, true))))
                            }
                        }
                    }
                }
            },
        );

        Ok(reqwest::Body::wrap_stream(stream))
    }
}

/// Shared context for the requests of a single partition's multipart upload.
/// These values are identical for every part; only the part id and streamed
/// body differ between requests. Bundling them keeps the per-part helpers from
/// each threading the same handful of arguments.
struct PartRequestCtx<'a, S: HttpSend> {
    client: &'a RestfulLanceDbClient<S>,
    freshness: &'a WriteFreshness,
    identifier: &'a str,
    table_name: &'a str,
    upload_id: &'a str,
    branch: Option<&'a str>,
    overwrite: bool,
}

impl<S: HttpSend + 'static> PartRequestCtx<'_, S> {
    /// Upload a partition as one or more multipart parts, cutting a new part
    /// whenever the current one reaches `max_bytes` (Arrow IPC, compressed) or
    /// has been uploading for `max_duration`, whichever comes first.
    ///
    /// Each part has a fixed IPC body and transaction ID. A retry replaces the
    /// same staged transaction instead of adding another part. Only the final
    /// complete request makes staged rows visible. Parts are prepared on demand;
    /// the target does not bound memory retained by input or ancestor buffers.
    ///
    /// The duration budget limits payload preparation time; HTTP timeouts are
    /// handled by retrying the prepared payload.
    async fn send_multipart_chunked(
        &self,
        max_bytes: u64,
        max_duration: Option<Duration>,
        input: SendableRecordBatchStream,
        tracker: Option<Arc<WriteProgressTracker>>,
    ) -> DataFusionResult<()> {
        let schema = input.schema();
        let max_batch_bytes = (max_bytes as usize).clamp(1, crate::remote::util::IPC_BATCH_BYTES);
        let stream = input.flat_map(move |batch| match batch {
            Ok(batch) => futures::stream::iter(
                crate::remote::util::split_batch(batch, max_batch_bytes)
                    .map(|batch| batch.map_err(Into::into)),
            )
            .left_stream(),
            Err(err) => futures::stream::iter([Err(err)]).right_stream(),
        });
        let mut input: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), stream));

        let mut pending = std::collections::VecDeque::new();
        loop {
            let first = match pending.pop_front() {
                Some(batch) => batch,
                None => match input.next().await {
                    Some(batch) => batch?,
                    None => break,
                },
            };
            if self
                .send_one_part(
                    max_bytes,
                    max_duration,
                    first,
                    &mut input,
                    &mut pending,
                    &tracker,
                )
                .await?
            {
                break;
            }
        }

        Ok(())
    }

    /// Build the `/insert` request for a single multipart part.
    fn build_part_request(
        &self,
        part_id: &str,
        body: reqwest::Body,
    ) -> (reqwest::RequestBuilder, Option<FreshnessHeaders>) {
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/insert/", self.identifier))
            .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
            .query(&[("upload_id", self.upload_id)])
            .query(&[("upload_part_id", part_id)]);
        // Every part of an overwrite carries `mode=overwrite`. The server records
        // it against the shared `upload_id` and applies the overwrite once, when
        // the multipart write is completed, rather than per part.
        if self.overwrite {
            request = request.query(&[("mode", "overwrite")]);
        }
        if let Some(b) = self.branch {
            request = request.query(&[("branch", b)]);
        }
        self.freshness.prepare(request.body(body))
    }

    /// Send a single part's request and drain the response, mapping HTTP and
    /// table-not-found errors into `DataFusionError`.
    async fn send_part_request(
        &self,
        request: reqwest::RequestBuilder,
        freshness_request: Option<FreshnessHeaders>,
    ) -> crate::Result<()> {
        let (request_id, response) = self.client.send(request).await?;
        let response =
            RemoteTable::<Sender>::handle_table_not_found(self.table_name, response, &request_id)
                .await?;
        let response = self.client.check_response(&request_id, response).await?;
        self.freshness
            .observe(freshness_request, response.headers());
        response.bytes().await.map_err(|e| Error::Http {
            source: Box::new(e),
            request_id: request_id.clone(),
            status_code: None,
        })?;
        Ok(())
    }

    /// Encode each part before sending so a lost staging response can be retried
    /// with exactly the same transaction ID and bytes.
    async fn send_one_part(
        &self,
        max_bytes: u64,
        max_duration: Option<Duration>,
        first: RecordBatch,
        input: &mut SendableRecordBatchStream,
        queued: &mut std::collections::VecDeque<RecordBatch>,
        tracker: &Option<Arc<WriteProgressTracker>>,
    ) -> DataFusionResult<bool> {
        let schema = input.schema();
        let options = arrow_ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))?;
        let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
            Vec::new(),
            &schema,
            options.clone(),
        )?;
        let started = Instant::now();
        let mut pending = Some(first);
        let mut has_batch = false;
        let mut part_rows = 0;
        let mut input_ended = false;
        loop {
            let batch = match pending.take().or_else(|| queued.pop_front()) {
                Some(batch) => batch,
                None => match input.next().await {
                    Some(batch) => batch?,
                    None => {
                        input_ended = true;
                        break;
                    }
                },
            };
            let before = writer.get_ref().len();
            writer.write(&batch)?;
            // Reserve the EOS marker. A rejected batch starts a fresh stream,
            // so its dictionary messages are encoded again with fresh state.
            if (writer.get_ref().len() as u64).saturating_add(8) > max_bytes {
                if has_batch {
                    writer.get_mut().truncate(before);
                    queued.push_front(batch);
                    break;
                }
                if batch.num_rows() > 1 {
                    let middle = batch.num_rows() / 2;
                    queued.push_front(crate::remote::util::compact_batch_dictionaries(
                        batch.slice(middle, batch.num_rows() - middle),
                    )?);
                    pending = Some(crate::remote::util::compact_batch_dictionaries(
                        batch.slice(0, middle),
                    )?);
                    writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
                        Vec::new(),
                        &schema,
                        options.clone(),
                    )?;
                    continue;
                }
                // A single row and its schema cannot be split further.
            }
            has_batch = true;
            part_rows += batch.num_rows();
            if writer.get_ref().len() as u64 >= max_bytes
                || max_duration.is_some_and(|limit| started.elapsed() >= limit)
            {
                break;
            }
        }
        writer.finish()?;
        let body = bytes::Bytes::from(writer.into_inner()?);
        let part_id = uuid::Uuid::new_v4().to_string();
        let mut retries =
            crate::remote::retry::RetryCounter::new(&self.client.retry_config, part_id.clone());
        loop {
            let (request, freshness) = self.build_part_request(&part_id, body.clone().into());
            match self.send_part_request(request, freshness).await {
                Ok(()) => {
                    if let Some(tracker) = tracker {
                        tracker.record_acknowledged_part(part_rows, body.len());
                    }
                    return Ok(input_ended);
                }
                Err(err) if retryable_part_error(&err, &self.client.retry_config.statuses) => {
                    let cause = err.to_string();
                    retries
                        .increment_from_error(err)
                        .map_err(|err| DataFusionError::External(Box::new(err)))?;
                    log::warn!(
                        "Retrying multipart upload {} part {} after {}; reusing the same payload",
                        self.upload_id,
                        part_id,
                        cause
                    );
                    tokio::time::sleep(retries.next_sleep_time()).await;
                }
                Err(err) => return Err(DataFusionError::External(Box::new(err))),
            }
        }
    }
}

fn retryable_part_error(err: &Error, statuses: &[reqwest::StatusCode]) -> bool {
    match err {
        Error::Http {
            status_code: Some(status),
            ..
        } => statuses.contains(status),
        Error::Http { source, .. } => source.downcast_ref::<reqwest::Error>().is_some_and(|err| {
            err.is_connect()
                || err.is_timeout()
                || err.is_body()
                || err.is_decode()
                || err.is_request()
        }),
        _ => false,
    }
}

impl<S: HttpSend + 'static> DisplayAs for RemoteWriteExec<S> {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "RemoteWriteExec: table={}, op=", self.table_name)?;
                match &self.op {
                    WriteOp::Insert { overwrite } => write!(f, "insert, overwrite={}", overwrite),
                    WriteOp::MergeInsert { .. } => write!(f, "merge_insert"),
                }
            }
            DisplayFormatType::TreeRender => {
                write!(f, "RemoteWriteExec")
            }
        }
    }
}

impl<S: HttpSend + 'static> ExecutionPlan for RemoteWriteExec<S> {
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
        ) -> datafusion_common::Result<
            datafusion_common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion_common::Result<datafusion_common::tree_node::TreeNodeRecursion> {
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    }
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn required_input_distribution(&self) -> Vec<datafusion_physical_plan::Distribution> {
        if self.upload_id.is_some() {
            vec![datafusion_physical_plan::Distribution::UnspecifiedDistribution]
        } else {
            vec![datafusion_physical_plan::Distribution::SinglePartition]
        }
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "RemoteWriteExec requires exactly one child".to_string(),
            ));
        }
        // Building a fresh exec (with a new, empty `result`) is what makes the
        // outer rescannable retry loop work: `reset_state()` clears the captured
        // result so a re-execution starts clean.
        let mut exec = Self::new_inner(
            self.table_name.clone(),
            self.identifier.clone(),
            self.client.clone(),
            children[0].clone(),
            self.op.clone(),
            self.upload_id.clone(),
            self.tracker.clone(),
            self.branch.clone(),
            self.max_bytes_per_request,
            self.max_request_duration,
        );
        exec.freshness = self.freshness.clone();
        Ok(Arc::new(exec))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if self.upload_id.is_none() && partition != 0 {
            return Err(DataFusionError::Internal(
                "RemoteWriteExec only supports single partition execution without upload_id"
                    .to_string(),
            ));
        }

        // Multipart is insert-only: the server has no multipart merge_insert
        // endpoint, so a merge_insert with an upload_id is a programming error.
        if self.upload_id.is_some() && matches!(self.op, WriteOp::MergeInsert { .. }) {
            return Err(DataFusionError::Internal(
                "merge_insert does not support multipart (upload_id) writes".to_string(),
            ));
        }

        let input_stream = self.input.execute(partition, context)?;
        let input_schema = input_stream.schema();
        let input_stream: SendableRecordBatchStream =
            Box::pin(InstrumentedRecordBatchStreamAdapter::new(
                input_schema,
                input_stream,
                partition,
                &self.metrics,
            ));
        let client = self.client.clone();
        let freshness = self.freshness.clone();
        let identifier = self.identifier.clone();
        let op = self.op.clone();
        let result_slot = self.result.clone();
        let table_name = self.table_name.clone();
        let upload_id = self.upload_id.clone();
        let tracker = self.tracker.clone();
        let branch = self.branch.clone();
        let max_bytes_per_request = self.max_bytes_per_request;
        let max_request_duration = self.max_request_duration;

        let stream = futures::stream::once(async move {
            // Multipart payloads remain replayable even when byte-based cuts
            // are disabled. Single-request writes never replay ambiguous errors.
            if let Some(upload_id) = upload_id.as_deref() {
                let max_bytes = max_bytes_per_request.unwrap_or(u64::MAX);
                let overwrite = matches!(op, WriteOp::Insert { overwrite: true });
                let ctx = PartRequestCtx {
                    client: &client,
                    freshness: &freshness,
                    identifier: &identifier,
                    table_name: &table_name,
                    upload_id,
                    branch: branch.as_deref(),
                    overwrite,
                };
                ctx.send_multipart_chunked(max_bytes, max_request_duration, input_stream, tracker)
                    .await?;
                // Count 0 here as for the non-multipart path below: the parts are
                // only staged, so the real row count is resolved when the caller
                // completes the multipart write.
                let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![0u64]));
                return Ok::<RecordBatch, DataFusionError>(RecordBatch::try_new(
                    COUNT_SCHEMA.clone(),
                    vec![count_array],
                )?);
            }

            // Build the request for the selected operation. Both endpoints take
            // an Arrow-IPC streaming body and reuse the same error side-channel.
            let mut request = match &op {
                WriteOp::Insert { overwrite } => {
                    let mut request = client
                        .post(&format!("/v1/table/{}/insert/", identifier))
                        .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE);
                    if *overwrite {
                        request = request.query(&[("mode", "overwrite")]);
                    }
                    if let Some(ref uid) = upload_id {
                        request = request.query(&[("upload_id", uid.as_str())]);
                    }
                    request
                }
                WriteOp::MergeInsert { query, timeout } => {
                    let mut request = client
                        .post(&format!("/v1/table/{}/merge_insert/", identifier))
                        .query(&query.on_query_params())
                        .query(query)
                        .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE);
                    if let Some(timeout) = timeout {
                        // (If it doesn't fit into u64, it's not worth sending anyways.)
                        if let Ok(timeout_ms) = u64::try_from(timeout.as_millis()) {
                            request = request.header(REQUEST_TIMEOUT_HEADER, timeout_ms);
                        }
                    }
                    request
                }
            };

            if let Some(ref b) = branch {
                request = request.query(&[("branch", b.as_str())]);
            }

            let (error_tx, mut error_rx) = tokio::sync::oneshot::channel();
            let body = Self::stream_as_http_body(input_stream, error_tx, tracker)?;
            let (request, freshness_request) = freshness.prepare(request.body(body));

            let result: DataFusionResult<(String, _)> = async {
                let (request_id, response) = client
                    .send(request)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let response = RemoteTable::<Sender>::handle_table_not_found(
                    &table_name,
                    response,
                    &request_id,
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let response = client
                    .check_response(&request_id, response)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                freshness.observe(freshness_request, response.headers());

                Ok((request_id, response))
            }
            .await;

            // If the request failed due to an input stream error, surface the
            // original error (e.g. NaN rejection) instead of the HTTP error.
            // This is the crux of the #2339 fix: Hyper silently swallows body
            // stream errors under HTTP2, so we recover the original here.
            if let Ok(stream_err) = error_rx.try_recv() {
                return Err(stream_err);
            }

            let (request_id, response) = result?;

            // For multipart writes, the staging response is not the final
            // version. Only parse the result for non-multipart writes.
            if upload_id.is_none() {
                let body_text = response.text().await.map_err(|e| {
                    DataFusionError::External(Box::new(Error::Http {
                        source: Box::new(e),
                        request_id: request_id.clone(),
                        status_code: None,
                    }))
                })?;

                let parsed_result = match &op {
                    WriteOp::Insert { .. } => {
                        let add = if body_text.trim().is_empty() {
                            // Backward compatible with old servers
                            AddResult { version: 0 }
                        } else {
                            serde_json::from_str(&body_text).map_err(|e| {
                                DataFusionError::External(Box::new(Error::Http {
                                    source: format!("Failed to parse add response: {}", e).into(),
                                    request_id: request_id.clone(),
                                    status_code: None,
                                }))
                            })?
                        };
                        WriteResult::Add(add)
                    }
                    WriteOp::MergeInsert { .. } => {
                        let merge = if body_text.trim().is_empty() {
                            // Backward compatible with old servers
                            MergeResult::default()
                        } else {
                            serde_json::from_str(&body_text).map_err(|e| {
                                DataFusionError::External(Box::new(Error::Http {
                                    source: format!("Failed to parse merge_insert response: {}", e)
                                        .into(),
                                    request_id: request_id.clone(),
                                    status_code: None,
                                }))
                            })?
                        };
                        WriteResult::Merge(merge)
                    }
                };

                let mut res_lock = result_slot.lock().map_err(|_| {
                    DataFusionError::Execution(
                        "Failed to acquire lock for write result".to_string(),
                    )
                })?;
                *res_lock = Some(parsed_result);
            } else {
                // We don't use the body in this case, but we should still consume it.
                let _ = response.bytes().await.map_err(|e| {
                    DataFusionError::External(Box::new(Error::Http {
                        source: Box::new(e),
                        request_id: request_id.clone(),
                        status_code: None,
                    }))
                })?;
            }

            // Return a single batch with count 0 (actual count is tracked in result)
            let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![0u64]));
            let batch = RecordBatch::try_new(COUNT_SCHEMA.clone(), vec![count_array])?;
            Ok::<_, DataFusionError>(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            COUNT_SCHEMA.clone(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::record_batch;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use datafusion::prelude::SessionContext;
    use datafusion_catalog::MemTable;
    use datafusion_common::{DataFusionError, Result as DataFusionResult};
    use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    use datafusion_physical_expr::EquivalenceProperties;
    use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::RemoteWriteExec;
    use super::WriteOp;
    use crate::Table;
    use crate::remote::ARROW_STREAM_CONTENT_TYPE;
    use crate::remote::table::MergeInsertRequest;
    use crate::table::datafusion::BaseTableAdapter;

    fn schema_json() -> &'static str {
        r#"{"fields": [{"name": "id", "type": {"type": "int32"}, "nullable": true}]}"#
    }

    #[tokio::test]
    async fn test_remote_insert_exec_execute_empty() {
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_count_clone = request_count.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            let path = request.url().path();

            if path == "/v1/table/my_table/describe/" {
                // Return schema for BaseTableAdapter::try_new
                return http::Response::builder()
                    .status(200)
                    .body(format!(r#"{{"version": 1, "schema": {}}}"#, schema_json()))
                    .unwrap();
            }

            if path == "/v1/table/my_table/insert/" {
                assert_eq!(request.method(), "POST");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    ARROW_STREAM_CONTENT_TYPE
                );
                request_count_clone.fetch_add(1, Ordering::SeqCst);

                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 2}"#.to_string())
                    .unwrap();
            }

            panic!("Unexpected request path: {}", path);
        });

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));

        // Create empty MemTable (no batches)
        let source_table = MemTable::try_new(schema, vec![vec![]]).unwrap();

        let ctx = SessionContext::new();

        // Register the remote table as insert target
        let provider = BaseTableAdapter::try_new(table.base_table().clone())
            .await
            .unwrap();
        ctx.register_table("my_table", Arc::new(provider)).unwrap();

        // Register empty source
        ctx.register_table("empty_source", Arc::new(source_table))
            .unwrap();

        // Execute the INSERT
        ctx.sql("INSERT INTO my_table SELECT * FROM empty_source")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Verify: should have made exactly one HTTP request even with empty input
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_remote_insert_exec_multi_partition() {
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_count_clone = request_count.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            let path = request.url().path();

            if path == "/v1/table/my_table/describe/" {
                // Return schema for BaseTableAdapter::try_new
                return http::Response::builder()
                    .status(200)
                    .body(format!(r#"{{"version": 1, "schema": {}}}"#, schema_json()))
                    .unwrap();
            }

            if path == "/v1/table/my_table/insert/" {
                assert_eq!(request.method(), "POST");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    ARROW_STREAM_CONTENT_TYPE
                );
                request_count_clone.fetch_add(1, Ordering::SeqCst);

                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 2}"#.to_string())
                    .unwrap();
            }

            panic!("Unexpected request path: {}", path);
        });

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));

        // Create MemTable with multiple partitions and multiple batches
        let source_table = MemTable::try_new(
            schema,
            vec![
                // Partition 0
                vec![
                    record_batch!(("id", Int32, [1, 2])).unwrap(),
                    record_batch!(("id", Int32, [3, 4])).unwrap(),
                ],
                // Partition 1
                vec![record_batch!(("id", Int32, [5, 6, 7])).unwrap()],
                // Partition 2
                vec![record_batch!(("id", Int32, [8])).unwrap()],
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();

        // Register the remote table as insert target
        let provider = BaseTableAdapter::try_new(table.base_table().clone())
            .await
            .unwrap();
        ctx.register_table("my_table", Arc::new(provider)).unwrap();

        // Register multi-partition source
        ctx.register_table("multi_partition_source", Arc::new(source_table))
            .unwrap();

        // Get the physical plan and verify it includes a repartition to 1
        let df = ctx
            .sql("INSERT INTO my_table SELECT * FROM multi_partition_source")
            .await
            .unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let plan_str = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();

        // The plan should include a CoalescePartitionsExec to merge partitions
        assert!(
            plan_str.contains("CoalescePartitionsExec"),
            "Expected CoalescePartitionsExec in plan:\n{}",
            plan_str
        );

        // Execute the INSERT
        df.collect().await.unwrap();

        // Verify: should have made exactly one HTTP request despite multiple input partitions
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
    }

    /// Build a single-partition input plan from the given batches.
    async fn input_plan_from_batches(
        schema: Arc<ArrowSchema>,
        batches: Vec<arrow_array::RecordBatch>,
    ) -> Arc<dyn ExecutionPlan> {
        use datafusion_catalog::TableProvider;
        let mem = MemTable::try_new(schema, vec![batches]).unwrap();
        let ctx = SessionContext::new();
        mem.scan(&ctx.state(), None, &[], None).await.unwrap()
    }

    /// Build a single-partition input plan from the batches spread across the
    /// given partitions.
    async fn input_plan_from_partitions(
        schema: Arc<ArrowSchema>,
        partitions: Vec<Vec<arrow_array::RecordBatch>>,
    ) -> Arc<dyn ExecutionPlan> {
        use datafusion_catalog::TableProvider;
        let mem = MemTable::try_new(schema, partitions).unwrap();
        let ctx = SessionContext::new();
        mem.scan(&ctx.state(), None, &[], None).await.unwrap()
    }

    fn counting_insert_client(
        counter: Arc<AtomicUsize>,
    ) -> crate::remote::client::RestfulLanceDbClient<crate::remote::client::test_utils::MockSender>
    {
        crate::remote::client::test_utils::client_with_handler(move |request| {
            let path = request.url().path();
            assert_eq!(path, "/v1/table/my_table/insert/");
            let query = request.url().query().unwrap_or("");
            assert!(query.contains("upload_id=upload-1"), "query: {query}");
            assert!(query.contains("upload_part_id="), "query: {query}");
            counter.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                .status(200)
                .body(String::new())
                .unwrap()
        })
    }

    /// Insert handler that records the `upload_part_id` of every part request so
    /// a test can assert the ids are distinct.
    fn recording_insert_client(
        part_ids: Arc<Mutex<Vec<String>>>,
    ) -> crate::remote::client::RestfulLanceDbClient<crate::remote::client::test_utils::MockSender>
    {
        crate::remote::client::test_utils::client_with_handler(move |request| {
            assert_eq!(request.url().path(), "/v1/table/my_table/insert/");
            let part_id = request
                .url()
                .query_pairs()
                .find(|(k, _)| k == "upload_part_id")
                .map(|(_, v)| v.into_owned())
                .expect("upload_part_id query param");
            part_ids.lock().unwrap().push(part_id);
            http::Response::builder()
                .status(200)
                .body(String::new())
                .unwrap()
        })
    }

    /// Single-partition input plan that yields one good batch and then an error,
    /// for exercising the mid-part input-error abort path in `send_one_part`.
    #[derive(Debug)]
    struct ErroringExec {
        schema: Arc<ArrowSchema>,
        properties: Arc<PlanProperties>,
    }

    impl ErroringExec {
        fn new() -> Self {
            let schema = record_batch!(("id", Int32, [1, 2])).unwrap().schema();
            let properties = PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                datafusion_physical_plan::Partitioning::UnknownPartitioning(1),
                datafusion_physical_plan::execution_plan::EmissionType::Incremental,
                datafusion_physical_plan::execution_plan::Boundedness::Bounded,
            );
            Self {
                schema,
                properties: Arc::new(properties),
            }
        }
    }

    impl DisplayAs for ErroringExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "ErroringExec")
        }
    }

    impl ExecutionPlan for ErroringExec {
        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(
                &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
            ) -> datafusion_common::Result<
                datafusion_common::tree_node::TreeNodeRecursion,
            >,
        ) -> datafusion_common::Result<datafusion_common::tree_node::TreeNodeRecursion> {
            Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
        }
        fn name(&self) -> &str {
            "ErroringExec"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> DataFusionResult<SendableRecordBatchStream> {
            let batch = record_batch!(("id", Int32, [1, 2])).unwrap();
            let stream = futures::stream::iter(vec![
                Ok(batch),
                Err(DataFusionError::Execution("boom".to_string())),
            ]);
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                stream,
            )))
        }
    }

    #[tokio::test]
    async fn test_multipart_chunked_splits_into_parts() {
        use futures::StreamExt;

        let insert_count = Arc::new(AtomicUsize::new(0));
        let client = counting_insert_client(insert_count.clone());

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let batches = vec![
            record_batch!(("id", Int32, [1, 2])).unwrap(),
            record_batch!(("id", Int32, [3, 4])).unwrap(),
            record_batch!(("id", Int32, [5, 6])).unwrap(),
        ];
        let input = input_plan_from_batches(schema, batches).await;

        // A 1-byte budget forces every row into its own part.
        let exec = RemoteWriteExec::new_multipart(
            "my_table".to_string(),
            "my_table".to_string(),
            client,
            input,
            false,
            "upload-1".to_string(),
            None,
            None,
            Some(1),
            None,
        );

        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        while stream.next().await.transpose().unwrap().is_some() {}

        assert_eq!(insert_count.load(Ordering::SeqCst), 6);
    }

    #[tokio::test]
    async fn test_multipart_respects_disabled_status_retries() {
        use futures::StreamExt;
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let mut client = crate::remote::client::test_utils::client_with_handler(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                .status(503)
                .body("unavailable")
                .unwrap()
        });
        client.retry_config.statuses.clear();
        let batch = record_batch!(("id", Int32, [1, 2])).unwrap();
        let input = input_plan_from_batches(batch.schema(), vec![batch]).await;
        let exec = RemoteWriteExec::new_multipart(
            "test".into(),
            "test".into(),
            client,
            input,
            false,
            "upload-1".into(),
            None,
            None,
            Some(64 * 1024 * 1024),
            None,
        );
        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        assert!(stream.next().await.unwrap().is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_multipart_splits_one_batch_within_encoded_byte_budget() {
        use futures::StreamExt;
        use rand::{RngCore, SeedableRng, rngs::SmallRng};
        let mut bytes = vec![0; 128 * 1024];
        SmallRng::seed_from_u64(7).fill_bytes(&mut bytes);
        let values = arrow_array::LargeBinaryArray::from_iter_values(bytes.chunks_exact(1024));
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "payload",
            DataType::LargeBinary,
            false,
        )]));
        let input = input_plan_from_batches(
            schema.clone(),
            vec![arrow_array::RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap()],
        )
        .await;
        let decoded = Arc::new(Mutex::new(Vec::new()));
        let observed = decoded.clone();
        let requests = Arc::new(AtomicUsize::new(0));
        let count = requests.clone();
        let client = crate::remote::client::test_utils::client_with_handler(move |request| {
            let body = request.body().unwrap().as_bytes().unwrap();
            assert!(
                body.len() <= 16 * 1024,
                "encoded part exceeded its budget: {}",
                body.len()
            );
            let reader =
                arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(body), None).unwrap();
            let mut decoded = observed.lock().unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::LargeBinaryArray>()
                    .unwrap();
                for value in values.iter() {
                    decoded.extend_from_slice(value.unwrap());
                }
            }
            count.fetch_add(1, Ordering::SeqCst);
            http::Response::builder().status(200).body("{}").unwrap()
        });
        let exec = RemoteWriteExec::new_multipart(
            "test".into(),
            "test".into(),
            client,
            input,
            false,
            "upload-1".into(),
            None,
            None,
            Some(16 * 1024),
            None,
        );
        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        while stream.next().await.transpose().unwrap().is_some() {}
        assert!(requests.load(Ordering::SeqCst) > 1);
        assert_eq!(*decoded.lock().unwrap(), bytes);
    }

    #[tokio::test]
    async fn test_multipart_single_part_when_under_budget() {
        use futures::StreamExt;

        let insert_count = Arc::new(AtomicUsize::new(0));
        let client = counting_insert_client(insert_count.clone());

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let batches = vec![
            record_batch!(("id", Int32, [1, 2])).unwrap(),
            record_batch!(("id", Int32, [3, 4])).unwrap(),
            record_batch!(("id", Int32, [5, 6])).unwrap(),
        ];
        let input = input_plan_from_batches(schema, batches).await;

        // A large byte budget and no time limit keep the whole partition in a
        // single part.
        let exec = RemoteWriteExec::new_multipart(
            "my_table".to_string(),
            "my_table".to_string(),
            client,
            input,
            false,
            "upload-1".to_string(),
            None,
            None,
            Some(64 * 1024 * 1024),
            None,
        );

        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        while stream.next().await.transpose().unwrap().is_some() {}

        assert_eq!(insert_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_multipart_chunked_splits_by_duration() {
        use futures::StreamExt;

        let insert_count = Arc::new(AtomicUsize::new(0));
        let client = counting_insert_client(insert_count.clone());

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let batches = vec![
            record_batch!(("id", Int32, [1, 2])).unwrap(),
            record_batch!(("id", Int32, [3, 4])).unwrap(),
            record_batch!(("id", Int32, [5, 6])).unwrap(),
        ];
        let input = input_plan_from_batches(schema, batches).await;

        // A large byte budget but a tiny duration budget: writing and sending
        // one batch already takes longer than the limit, so each batch is cut
        // into its own part on the time check rather than the byte check.
        let exec = RemoteWriteExec::new_multipart(
            "my_table".to_string(),
            "my_table".to_string(),
            client,
            input,
            false,
            "upload-1".to_string(),
            None,
            None,
            Some(64 * 1024 * 1024),
            Some(std::time::Duration::from_nanos(1)),
        );

        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        while stream.next().await.transpose().unwrap().is_some() {}

        assert_eq!(insert_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_multipart_empty_partition_stages_nothing() {
        use futures::StreamExt;

        let insert_count = Arc::new(AtomicUsize::new(0));
        let client = counting_insert_client(insert_count.clone());

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        // An empty partition should stage no parts; on the multipart path the
        // write relies on another partition having data to commit.
        let input = input_plan_from_batches(schema, vec![]).await;

        let exec = RemoteWriteExec::new_multipart(
            "my_table".to_string(),
            "my_table".to_string(),
            client,
            input,
            false,
            "upload-1".to_string(),
            None,
            None,
            Some(64 * 1024 * 1024),
            None,
        );

        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        while stream.next().await.transpose().unwrap().is_some() {}

        assert_eq!(insert_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_multipart_chunked_uses_distinct_part_ids() {
        use futures::StreamExt;
        use std::collections::HashSet;

        let part_ids = Arc::new(Mutex::new(Vec::new()));
        let client = recording_insert_client(part_ids.clone());

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let batches = vec![
            record_batch!(("id", Int32, [1, 2])).unwrap(),
            record_batch!(("id", Int32, [3, 4])).unwrap(),
            record_batch!(("id", Int32, [5, 6])).unwrap(),
        ];
        let input = input_plan_from_batches(schema, batches).await;

        // A 1-byte budget forces every row into its own part.
        let exec = RemoteWriteExec::new_multipart(
            "my_table".to_string(),
            "my_table".to_string(),
            client,
            input,
            false,
            "upload-1".to_string(),
            None,
            None,
            Some(1),
            None,
        );

        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        while stream.next().await.transpose().unwrap().is_some() {}

        let ids = part_ids.lock().unwrap().clone();
        assert_eq!(ids.len(), 6, "expected one part id per part: {ids:?}");
        assert!(
            ids.iter().all(|id| !id.is_empty()),
            "part ids must be non-empty: {ids:?}"
        );
        let unique: HashSet<&String> = ids.iter().collect();
        assert_eq!(unique.len(), 6, "part ids must be distinct: {ids:?}");
    }

    #[tokio::test]
    async fn test_multipart_chunks_each_partition_independently() {
        use futures::StreamExt;

        let insert_count = Arc::new(AtomicUsize::new(0));
        let client = counting_insert_client(insert_count.clone());

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let partitions = vec![
            // Partition 0: two batches, split into two parts by the 1-byte budget.
            vec![
                record_batch!(("id", Int32, [1, 2])).unwrap(),
                record_batch!(("id", Int32, [3, 4])).unwrap(),
            ],
            // Partition 1: one batch, one part.
            vec![record_batch!(("id", Int32, [5, 6])).unwrap()],
        ];
        let input = input_plan_from_partitions(schema, partitions).await;

        let exec = RemoteWriteExec::new_multipart(
            "my_table".to_string(),
            "my_table".to_string(),
            client,
            input,
            false,
            "upload-1".to_string(),
            None,
            None,
            Some(1),
            None,
        );

        for partition in 0..2 {
            let mut stream = exec
                .execute(partition, Arc::new(TaskContext::default()))
                .unwrap();
            while stream.next().await.transpose().unwrap().is_some() {}
        }

        // 4 single-row parts from partition 0 + 2 from partition 1.
        assert_eq!(insert_count.load(Ordering::SeqCst), 6);
    }

    #[tokio::test]
    async fn test_multipart_input_error_surfaces_original() {
        use futures::StreamExt;

        let insert_count = Arc::new(AtomicUsize::new(0));
        let client = counting_insert_client(insert_count.clone());

        // A large byte budget keeps the good batch and the following error in
        // the same part, exercising the mid-part abort path.
        let input: Arc<dyn ExecutionPlan> = Arc::new(ErroringExec::new());
        let exec = RemoteWriteExec::new_multipart(
            "my_table".to_string(),
            "my_table".to_string(),
            client,
            input,
            false,
            "upload-1".to_string(),
            None,
            None,
            Some(64 * 1024 * 1024),
            None,
        );

        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let mut err = None;
        while let Some(item) = stream.next().await {
            if let Err(e) = item {
                err = Some(e);
                break;
            }
        }

        let err = err.expect("expected the input stream error to surface");
        // The original DataFusion error must win over the HTTP error it induces.
        assert!(
            err.to_string().contains("boom"),
            "expected original input error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_merge_insert_input_error_surfaces_original() {
        // Regression test for #2339 on the single-request merge_insert path.
        // When the input stream errors mid-body, Hyper masks it under HTTP2 as a
        // generic "stream error sent by user" message. The error side-channel
        // must recover and surface the original DataFusion error instead.
        use futures::StreamExt;

        let client = crate::remote::client::test_utils::client_with_handler(|request| {
            assert_eq!(request.url().path(), "/v1/table/my_table/merge_insert/");
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"version": 2, "num_updated_rows": 0, "num_inserted_rows": 0, "num_deleted_rows": 0}"#
                        .to_string(),
                )
                .unwrap()
        });

        let query = MergeInsertRequest {
            on: vec!["id".to_string()],
            when_matched_update_all: false,
            when_matched_update_all_filt: None,
            when_not_matched_insert_all: false,
            when_not_matched_by_source_delete: false,
            when_not_matched_by_source_delete_filt: None,
            use_index: true,
            use_rte: None,
        };

        let input: Arc<dyn ExecutionPlan> = Arc::new(ErroringExec::new());
        let exec = RemoteWriteExec::new(
            "my_table".to_string(),
            "my_table".to_string(),
            client,
            input,
            WriteOp::MergeInsert {
                query,
                timeout: None,
            },
            None,
            None,
        );

        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let mut err = None;
        while let Some(item) = stream.next().await {
            if let Err(e) = item {
                err = Some(e);
                break;
            }
        }

        let err = err.expect("expected the input stream error to surface");
        assert!(
            err.to_string().contains("boom"),
            "expected original input error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_multipart_records_acknowledged_rows_and_bytes() {
        use crate::table::write_progress::{ProgressCallback, WriteProgress, WriteProgressTracker};
        use futures::StreamExt;

        let insert_count = Arc::new(AtomicUsize::new(0));
        let client = counting_insert_client(insert_count.clone());

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let batches = vec![
            record_batch!(("id", Int32, [1, 2])).unwrap(),
            record_batch!(("id", Int32, [3, 4])).unwrap(),
            record_batch!(("id", Int32, [5, 6])).unwrap(),
        ];
        let input = input_plan_from_batches(schema, batches).await;

        let observed = Arc::new(Mutex::new(Vec::<(usize, usize)>::new()));
        let observed_cb = observed.clone();
        let callback: ProgressCallback = Arc::new(Mutex::new(move |p: &WriteProgress| {
            observed_cb
                .lock()
                .unwrap()
                .push((p.output_rows(), p.output_bytes()));
        }));
        let tracker = Arc::new(WriteProgressTracker::new(callback, None));

        tracker.use_acknowledged_parts();
        // Count rows from all encoded batches once when the part is acknowledged.
        let exec = RemoteWriteExec::new_multipart(
            "my_table".to_string(),
            "my_table".to_string(),
            client,
            input,
            false,
            "upload-1".to_string(),
            Some(tracker),
            None,
            Some(64 * 1024 * 1024),
            None,
        );

        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        while stream.next().await.transpose().unwrap().is_some() {}

        assert_eq!(
            insert_count.load(Ordering::SeqCst),
            1,
            "batches should all land in a single part"
        );
        let observed = observed.lock().unwrap();
        assert_eq!(observed.last().unwrap().0, 6);
        assert!(
            observed.len() == 1,
            "expected one progress update per acknowledged part: {observed:?}"
        );
        assert!(
            observed.windows(2).all(|w| w[1] >= w[0]),
            "progress bytes should be monotonic: {observed:?}"
        );
        assert!(
            observed.last().unwrap().1 > 0,
            "final progress should report bytes: {observed:?}"
        );
    }
    #[tokio::test]
    async fn test_multipart_nested_dictionary_values_and_retries() {
        use arrow_array::{
            Array, ArrayRef, BinaryArray, DictionaryArray, ListArray, RecordBatch, StructArray,
            UInt8Array, types::UInt8Type,
        };
        use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        use arrow_cast::cast;
        use arrow_ipc::reader::StreamReader;
        use futures::StreamExt;
        use rand::{RngCore, SeedableRng, rngs::SmallRng};
        use std::collections::HashMap;

        let mut bytes = vec![0; 128 * 1024];
        SmallRng::seed_from_u64(4977).fill_bytes(&mut bytes);
        let mut batches = Vec::new();
        for shift in [0, 1] {
            let values = BinaryArray::from_iter(
                std::iter::once(None).chain(bytes.chunks_exact(1024).skip(shift).map(Some)),
            );
            let keys = UInt8Array::from(vec![
                Some(120),
                Some(1),
                None,
                Some(1),
                Some(2),
                Some(0),
                Some(3),
                Some(4),
            ]);
            let dict = Arc::new(DictionaryArray::<UInt8Type>::new(keys, Arc::new(values)));
            let field = Arc::new(
                Field::new("category", dict.data_type().clone(), true)
                    .with_metadata(HashMap::from([("field-meta".into(), "retained".into())])),
            );
            let entries = StructArray::new(vec![field].into(), vec![dict], None);
            let list = ListArray::new(
                Arc::new(Field::new_list_field(entries.data_type().clone(), false)),
                OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 3, 5, 6, 6, 8])),
                Arc::new(entries),
                Some(NullBuffer::from(vec![true, true, true, true, false, true])),
            );
            let schema = Arc::new(ArrowSchema::new_with_metadata(
                vec![Field::new("categories", list.data_type().clone(), true)],
                HashMap::from([("schema-meta".into(), "retained".into())]),
            ));
            // The discarded first row references a large, otherwise unused value.
            batches.push(
                RecordBatch::try_new(schema, vec![Arc::new(list)])
                    .unwrap()
                    .slice(1, 5),
            );
        }
        let schema = batches[0].schema();
        let plain_type = DataType::List(Arc::new(Field::new_list_field(
            DataType::Struct(
                vec![
                    Field::new("category", DataType::Binary, true)
                        .with_metadata(HashMap::from([("field-meta".into(), "retained".into())])),
                ]
                .into(),
            ),
            false,
        )));
        let expected: Vec<ArrayRef> = batches
            .iter()
            .map(|b| cast(b.column(0), &plain_type).unwrap())
            .collect();
        let expected =
            arrow_select::concat::concat(&expected.iter().map(|a| a.as_ref()).collect::<Vec<_>>())
                .unwrap();
        let input = input_plan_from_batches(schema.clone(), batches).await;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let mut client = crate::remote::client::test_utils::client_with_handler(move |request| {
            let query: HashMap<_, _> = request.url().query_pairs().into_owned().collect();
            assert_eq!(query["upload_id"], "upload-1");
            let body = request.body().unwrap().as_bytes().unwrap().to_vec();
            let mut requests = observed.lock().unwrap();
            requests.push((query["upload_part_id"].clone(), body));
            http::Response::builder()
                .status(if requests.len() == 1 { 503 } else { 200 })
                .body("{}")
                .unwrap()
        });
        client.retry_config.backoff_factor = 0.0;
        client.retry_config.backoff_jitter = 0.0;
        use crate::table::write_progress::{ProgressCallback, WriteProgress, WriteProgressTracker};
        let acknowledged = Arc::new(AtomicUsize::new(0));
        let acknowledged_c = acknowledged.clone();
        let callback: ProgressCallback = Arc::new(Mutex::new(move |p: &WriteProgress| {
            acknowledged_c.store(p.output_bytes(), Ordering::SeqCst);
        }));
        let tracker = Arc::new(WriteProgressTracker::new(callback, None));
        let exec = RemoteWriteExec::new_multipart(
            "test".into(),
            "test".into(),
            client,
            input,
            false,
            "upload-1".into(),
            Some(tracker),
            None,
            Some(16 * 1024),
            None,
        );
        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        while stream.next().await.transpose().unwrap().is_some() {}
        let requests = requests.lock().unwrap();
        assert!(requests.len() >= 2);
        assert_eq!(
            requests[0], requests[1],
            "retry must reuse part id and encoded bytes"
        );
        let mut decoded = Vec::new();
        for (_, body) in &requests[1..] {
            assert!(body.len() <= 16 * 1024, "part size: {}", body.len());
            let reader = StreamReader::try_new(std::io::Cursor::new(body), None).unwrap();
            assert_eq!(reader.schema(), schema);
            for batch in reader {
                decoded.push(cast(batch.unwrap().column(0), &plain_type).unwrap());
            }
        }
        let actual =
            arrow_select::concat::concat(&decoded.iter().map(|a| a.as_ref()).collect::<Vec<_>>())
                .unwrap();
        assert_eq!(actual.as_ref(), expected.as_ref());
        let total: usize = requests[1..].iter().map(|(_, b)| b.len()).sum();
        assert_eq!(acknowledged.load(Ordering::SeqCst), total);
        assert!(
            total < 16 * 1024,
            "unused dictionary values leaked: {total}"
        );
    }

    #[rstest::rstest]
    #[case(false, "linear", 1024)]
    #[case(true, "linear", 1024)]
    #[case(false, "linear", 1000)]
    #[case(true, "linear", 1000)]
    #[case(false, "nested", 1024)]
    #[case(true, "nested", 1024)]
    #[case(false, "sharing", 1024)]
    #[case(true, "sharing", 1024)]
    #[case(false, "empty", 1024)]
    #[case(true, "empty", 1024)]
    #[tokio::test]
    async fn test_multipart_list_view_dictionary_ranges(
        #[case] large: bool,
        #[case] layout: &str,
        #[case] value_bytes: usize,
    ) {
        if large {
            check_list_view_dictionary_ranges::<i64>(layout, value_bytes).await;
        } else {
            check_list_view_dictionary_ranges::<i32>(layout, value_bytes).await;
        }
    }

    async fn check_list_view_dictionary_ranges<O: arrow_array::OffsetSizeTrait>(
        layout: &str,
        value_bytes: usize,
    ) {
        use arrow_array::{
            Array, ArrayRef, DictionaryArray, GenericListViewArray, Int32Array, ListArray,
            RecordBatch, StringArray, StructArray, cast::AsArray, types::Int32Type,
        };
        use arrow_buffer::{NullBuffer, OffsetBuffer};
        use arrow_ipc::reader::StreamReader;
        use futures::StreamExt;
        use rand::{RngCore, SeedableRng, rngs::SmallRng};
        use std::collections::HashMap;

        let mut bytes = vec![0; 128 * value_bytes];
        SmallRng::seed_from_u64(4977).fill_bytes(&mut bytes);
        let strings: Vec<String> = bytes
            .chunks_exact(value_bytes)
            .map(|c| c.iter().map(|b| char::from(b'!' + b % 90)).collect())
            .collect();
        let mut expected = Vec::new();
        let mut batches = Vec::new();
        let nested = layout != "linear";
        for reverse in [false, true]
            .into_iter()
            .take(if layout == "nested" { 2 } else { 1 })
        {
            let values: Vec<_> = if reverse {
                strings.iter().rev().cloned().collect()
            } else {
                strings.clone()
            };
            let keys: Vec<i32> = (0..128)
                .map(|i| if layout == "sharing" { i % 2 } else { i })
                .collect();
            // A sliced child has its own nonzero physical offset.
            let dict = DictionaryArray::<Int32Type>::new(
                Int32Array::from_iter_values(std::iter::once(127).chain(keys.iter().copied())),
                Arc::new(StringArray::from(values.clone())),
            )
            .slice(1, 128);
            let mut offsets = Vec::new();
            let mut sizes = Vec::new();
            let mut valid = Vec::new();
            let start = usize::from(layout == "nested");
            for row in 0..128 + start {
                let (offset, size, is_valid) = match layout {
                    "empty" if row % 2 == 0 => (80, 48, false),
                    "empty" => (128, 0, true),
                    "nested" if row % 17 == 0 => (80, 48, false),
                    "nested" if row % 13 == 0 => (128, 0, true),
                    "nested" => (4 + (row * 7) % 64, 3, true),
                    "sharing" => (4 + (row % 2) * 4, 64, true),
                    _ => (row, 1, true),
                };
                offsets.push(O::from_usize(offset).unwrap());
                sizes.push(O::from_usize(size).unwrap());
                valid.push(is_valid);
                if row >= start {
                    expected.push(is_valid.then(|| {
                        (offset..offset + size)
                            .map(|i| values[keys[i] as usize].clone())
                            .collect::<Vec<_>>()
                    }));
                }
            }
            let item = Arc::new(
                Field::new("category", dict.data_type().clone(), false)
                    .with_metadata(HashMap::from([("item-meta".into(), "retained".into())])),
            );
            let views = GenericListViewArray::<O>::new(
                item,
                offsets.into(),
                sizes.into(),
                Arc::new(dict),
                Some(NullBuffer::from(valid)),
            );
            let view_count = views.len();
            let column: ArrayRef = if nested {
                let field = Arc::new(Field::new("views", views.data_type().clone(), true));
                let entries = StructArray::new(vec![field].into(), vec![Arc::new(views)], None);
                Arc::new(ListArray::new(
                    Arc::new(Field::new_list_field(entries.data_type().clone(), false)),
                    OffsetBuffer::from_lengths(std::iter::repeat_n(1, view_count)),
                    Arc::new(entries),
                    None,
                ))
            } else {
                Arc::new(views)
            };
            let schema = Arc::new(ArrowSchema::new_with_metadata(
                vec![Field::new("categories", column.data_type().clone(), true)],
                HashMap::from([("schema-meta".into(), "retained".into())]),
            ));
            batches.push(
                RecordBatch::try_new(schema, vec![column])
                    .unwrap()
                    .slice(start, 128),
            );
        }
        let schema = batches[0].schema();
        let logical_bytes: usize = expected.iter().flatten().flatten().map(String::len).sum();
        let input = input_plan_from_batches(schema.clone(), batches).await;
        let lengths = Arc::new(Mutex::new(Vec::new()));
        let observed = lengths.clone();
        let decoded = Arc::new(Mutex::new(Vec::new()));
        let decoded_c = decoded.clone();
        let sharing = layout == "sharing";
        let empty = layout == "empty";
        let client = crate::remote::client::test_utils::client_with_handler(move |request| {
            let body = request.body().unwrap().as_bytes().unwrap();
            observed.lock().unwrap().push(body.len());
            let reader = StreamReader::try_new(std::io::Cursor::new(body), None).unwrap();
            assert_eq!(reader.schema(), schema);
            for batch in reader {
                let batch = batch.unwrap();
                for row in 0..batch.num_rows() {
                    let column = if nested {
                        batch
                            .column(0)
                            .as_list::<i32>()
                            .value(row)
                            .as_struct()
                            .column(0)
                            .slice(0, 1)
                    } else {
                        batch.column(0).slice(row, 1)
                    };
                    let views = column
                        .as_any()
                        .downcast_ref::<GenericListViewArray<O>>()
                        .unwrap();
                    if empty {
                        assert_eq!(views.values().len(), 0, "null/empty views retain no child");
                    }
                    if sharing {
                        assert_eq!(
                            views.values().len(),
                            68,
                            "overlapping ranges must share their child"
                        );
                    }
                    decoded_c.lock().unwrap().push(if views.is_null(0) {
                        None
                    } else {
                        let value = arrow_cast::cast(&views.value(0), &DataType::Utf8).unwrap();
                        Some(
                            value
                                .as_string::<i32>()
                                .iter()
                                .map(|v| v.unwrap().to_owned())
                                .collect::<Vec<_>>(),
                        )
                    });
                }
            }
            http::Response::builder().status(200).body("{}").unwrap()
        });
        let exec = RemoteWriteExec::new_multipart(
            "test".into(),
            "test".into(),
            client,
            input,
            false,
            "upload-1".into(),
            None,
            None,
            Some(16 * 1024),
            None,
        );
        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        while stream.next().await.transpose().unwrap().is_some() {}
        assert_eq!(*decoded.lock().unwrap(), expected);
        let lengths = lengths.lock().unwrap();
        let total: usize = lengths.iter().sum();
        eprintln!(
            "ListView IPC: large={}, layout={layout}, value_bytes={value_bytes}, parts={}, first={}, total={total}",
            O::IS_LARGE,
            lengths.len(),
            lengths[0]
        );
        // Every selected row in these cases fits the target, including IPC framing.
        assert!(
            lengths.iter().all(|n| *n <= 16 * 1024),
            "oversized parts: {lengths:?}"
        );
        assert!(
            total <= (2 * logical_bytes).max(16 * 1024),
            "dictionary upload amplified: {total}"
        );
        if sharing {
            assert_eq!(lengths.len(), 1);
        }
    }

    #[rstest::rstest]
    #[case(1024, false)]
    // Dictionary data fits the target; IPC framing forces the sender to split again.
    #[case(1000, false)]
    #[case(1024, true)]
    #[case(20 * 1024, false)]
    #[tokio::test]
    async fn test_multipart_dictionary_payload_is_not_repeated_per_row(
        #[case] value_bytes: usize,
        #[case] use_views: bool,
    ) {
        use arrow_array::{
            Array, ArrayRef, DictionaryArray, Int32Array, RecordBatch, StringArray,
            StringViewArray, types::Int32Type,
        };
        use arrow_ipc::reader::StreamReader;
        use futures::StreamExt;
        use rand::{RngCore, SeedableRng, rngs::SmallRng};

        let mut bytes = vec![0; 128 * value_bytes];
        SmallRng::seed_from_u64(4977).fill_bytes(&mut bytes);
        let strings: Vec<String> = bytes
            .chunks_exact(value_bytes)
            .map(|chunk| chunk.iter().map(|b| char::from(b'!' + b % 90)).collect())
            .collect();
        let values: ArrayRef = if use_views {
            Arc::new(StringViewArray::from_iter_values(&strings))
        } else {
            Arc::new(StringArray::from(strings.clone()))
        };
        let array =
            DictionaryArray::<Int32Type>::try_new(Int32Array::from_iter_values(0..128), values)
                .unwrap();
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "category",
            array.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
        let input = input_plan_from_batches(schema.clone(), vec![batch]).await;
        let lengths = Arc::new(Mutex::new(Vec::new()));
        let observed = lengths.clone();
        let decoded = Arc::new(Mutex::new(Vec::new()));
        let decoded_c = decoded.clone();
        let client = crate::remote::client::test_utils::client_with_handler(move |request| {
            let body = request.body().unwrap().as_bytes().unwrap();
            observed.lock().unwrap().push(body.len());
            let reader = StreamReader::try_new(std::io::Cursor::new(body), None).unwrap();
            assert_eq!(reader.schema(), schema);
            let mut rows = 0;
            for batch in reader {
                let batch = batch.unwrap();
                rows += batch.num_rows();
                let values = arrow_cast::cast(batch.column(0), &DataType::Utf8).unwrap();
                let values = values.as_any().downcast_ref::<StringArray>().unwrap();
                decoded_c
                    .lock()
                    .unwrap()
                    .extend(values.iter().map(|v| v.unwrap().to_owned()));
            }
            assert!(
                body.len() <= 16 * 1024 || rows == 1,
                "oversized divisible part: {} bytes, {rows} rows",
                body.len()
            );
            http::Response::builder().status(200).body("{}").unwrap()
        });
        let exec = RemoteWriteExec::new_multipart(
            "test".into(),
            "test".into(),
            client,
            input,
            false,
            "upload-1".into(),
            None,
            None,
            Some(16 * 1024),
            None,
        );
        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        while stream.next().await.transpose().unwrap().is_some() {}
        assert_eq!(*decoded.lock().unwrap(), strings);
        let lengths = lengths.lock().unwrap();
        let total: usize = lengths.iter().sum();
        eprintln!(
            "dictionary IPC: value_bytes={value_bytes}, views={use_views}, parts={}, first={}, total={total}",
            lengths.len(),
            lengths[0]
        );
        assert!(
            total <= 2 * bytes.len(),
            "dictionary upload amplified: parts={}, first={}, total={total}",
            lengths.len(),
            lengths[0]
        );
    }
}
