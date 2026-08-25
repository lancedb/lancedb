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
use futures::{SinkExt, StreamExt};
use http::header::CONTENT_TYPE;
use lance::io::exec::utils::InstrumentedRecordBatchStreamAdapter;

use crate::Error;
use crate::remote::ARROW_STREAM_CONTENT_TYPE;
use crate::remote::client::{HttpSend, RestfulLanceDbClient, Sender};
use crate::remote::table::{MergeInsertRequest, REQUEST_TIMEOUT_HEADER, RemoteTable};
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
    /// Each part is a separate `/insert?upload_id=...&upload_part_id=...` request
    /// whose body is still streamed through a bounded channel, so peak memory
    /// stays at a couple of batches regardless of `max_bytes`. The server stages
    /// every part under the shared `upload_id` and merges them atomically when
    /// the caller completes the multipart write. An empty partition stages
    /// nothing: the multipart write always has at least one non-empty partition
    /// to commit.
    ///
    /// The byte budget targets a good on-disk fragment size; the duration budget
    /// bounds request time so a slow or throttled upload does not keep a request
    /// open past the client read timeout (which also covers the request body).
    async fn send_multipart_chunked(
        &self,
        max_bytes: u64,
        max_duration: Option<Duration>,
        mut input: SendableRecordBatchStream,
        tracker: Option<Arc<WriteProgressTracker>>,
    ) -> DataFusionResult<()> {
        let schema = input.schema();

        // A part always starts from a batch we already hold: the first batch of
        // the partition, or the look-ahead batch from the previous part. This
        // keeps empty partitions from staging a part and stops a size cut that
        // lands exactly on the end of input from emitting a trailing empty part.
        let mut first = match input.next().await {
            Some(batch) => batch?,
            None => return Ok(()),
        };

        loop {
            let input_ended = self
                .send_one_part(
                    &schema,
                    max_bytes,
                    max_duration,
                    first,
                    &mut input,
                    &tracker,
                )
                .await?;

            if input_ended {
                break;
            }

            first = match input.next().await {
                Some(batch) => batch?,
                None => break,
            };
        }

        Ok(())
    }

    /// Build the `/insert` request for a single multipart part.
    fn build_part_request(&self, part_id: &str, body: reqwest::Body) -> reqwest::RequestBuilder {
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
        request.body(body)
    }

    /// Send a single part's request and drain the response, mapping HTTP and
    /// table-not-found errors into `DataFusionError`.
    async fn send_part_request(&self, request: reqwest::RequestBuilder) -> DataFusionResult<()> {
        let (request_id, response) = self
            .client
            .send(request)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let response =
            RemoteTable::<Sender>::handle_table_not_found(self.table_name, response, &request_id)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let response = self
            .client
            .check_response(&request_id, response)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        response.bytes().await.map_err(|e| {
            DataFusionError::External(Box::new(Error::Http {
                source: Box::new(e),
                request_id: request_id.clone(),
                status_code: None,
            }))
        })?;
        Ok(())
    }

    /// Stream one part, starting from `first` and pulling from `input` until the
    /// part reaches `max_bytes`, has been uploading for `max_duration`, or the
    /// input ends. The body is streamed through a bounded channel concurrently
    /// with the request, so peak memory stays at a couple of batches. Wire bytes
    /// are recorded on `tracker` as each chunk is produced, so progress advances
    /// smoothly rather than jumping once per completed part. Returns whether the
    /// input was exhausted while filling this part.
    async fn send_one_part(
        &self,
        schema: &arrow_schema::SchemaRef,
        max_bytes: u64,
        max_duration: Option<Duration>,
        first: RecordBatch,
        input: &mut SendableRecordBatchStream,
        tracker: &Option<Arc<WriteProgressTracker>>,
    ) -> DataFusionResult<bool> {
        let (mut chunk_tx, chunk_rx) =
            futures::channel::mpsc::channel::<Result<Vec<u8>, std::io::Error>>(2);
        let body = reqwest::Body::wrap_stream(chunk_rx);

        let part_id = uuid::Uuid::new_v4().to_string();
        let request = self.build_part_request(&part_id, body);

        // Measured from just before the request is sent, matching the window the
        // client read timeout applies to the upload.
        let started = Instant::now();
        let tracker = tracker.clone();
        // Unlike `stream_as_http_body`, this producer also cuts the part at the
        // byte/time budget and reports back whether the input ended, so it drives
        // its own bounded mpsc channel joined with the request instead of reusing
        // that helper.
        let producer = async move {
            let options = arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::LZ4_FRAME))
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let mut writer =
                arrow_ipc::writer::StreamWriter::try_new_with_options(Vec::new(), schema, options)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut part_bytes: u64 = 0;
            let mut input_ended = false;
            let mut pending = Some(first);
            loop {
                let batch = match pending.take() {
                    Some(batch) => batch,
                    None => match input.next().await {
                        Some(Ok(batch)) => batch,
                        Some(Err(e)) => {
                            // Abort the body so the server does not treat the
                            // truncated stream as a successful write; the
                            // original error is surfaced to the caller.
                            let _ = chunk_tx
                                .send(Err(std::io::Error::other("input stream error")))
                                .await;
                            return Err(e);
                        }
                        None => {
                            input_ended = true;
                            break;
                        }
                    },
                };
                writer
                    .write(&batch)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let chunk = std::mem::take(writer.get_mut());
                let chunk_len = chunk.len();
                part_bytes += chunk_len as u64;
                if chunk_tx.send(Ok(chunk)).await.is_err() {
                    // The request finished or failed; stop producing.
                    break;
                }
                if let Some(ref t) = tracker {
                    t.record_bytes(chunk_len);
                }
                if part_bytes >= max_bytes
                    || max_duration.is_some_and(|limit| started.elapsed() >= limit)
                {
                    break;
                }
            }

            writer
                .finish()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let tail = std::mem::take(writer.get_mut());
            if !tail.is_empty() {
                let tail_len = tail.len();
                if chunk_tx.send(Ok(tail)).await.is_ok()
                    && let Some(ref t) = tracker
                {
                    t.record_bytes(tail_len);
                }
            }
            Ok::<bool, DataFusionError>(input_ended)
        };

        let send = self.send_part_request(request);

        // `join!` rather than `tokio::spawn`: the producer borrows `input` (and
        // `schema`), so it cannot satisfy the `'static` bound a spawned task
        // needs. Running both futures on this task lets them make progress
        // concurrently without that constraint.
        let (producer_result, send_result) = futures::join!(producer, send);
        // Prefer the producer error (e.g. NaN rejection) over any HTTP error it
        // induced.
        let input_ended = producer_result?;
        send_result?;

        Ok(input_ended)
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
        Ok(Arc::new(Self::new_inner(
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
        )))
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
            // Multipart writes with a byte budget split the partition into
            // several bounded, still-streamed requests so no single request
            // stays open long enough to hit the client read timeout. This path
            // is insert-only (guarded above).
            if let (Some(upload_id), Some(max_bytes)) =
                (upload_id.as_deref(), max_bytes_per_request)
            {
                let overwrite = matches!(op, WriteOp::Insert { overwrite: true });
                let ctx = PartRequestCtx {
                    client: &client,
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
            let request = request.body(body);

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

        // A 1-byte budget forces every batch into its own part.
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

        assert_eq!(insert_count.load(Ordering::SeqCst), 3);
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

        // A 1-byte budget forces every batch into its own part.
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
        assert_eq!(ids.len(), 3, "expected one part id per part: {ids:?}");
        assert!(
            ids.iter().all(|id| !id.is_empty()),
            "part ids must be non-empty: {ids:?}"
        );
        let unique: HashSet<&String> = ids.iter().collect();
        assert_eq!(unique.len(), 3, "part ids must be distinct: {ids:?}");
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

        // 2 parts from partition 0 + 1 part from partition 1.
        assert_eq!(insert_count.load(Ordering::SeqCst), 3);
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
            on: "id".to_string(),
            when_matched_update_all: false,
            when_matched_update_all_filt: None,
            when_not_matched_insert_all: false,
            when_not_matched_by_source_delete: false,
            when_not_matched_by_source_delete_filt: None,
            use_index: true,
            use_lsm: None,
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
    async fn test_multipart_records_progress_within_a_part() {
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

        let observed = Arc::new(Mutex::new(Vec::<usize>::new()));
        let observed_cb = observed.clone();
        let callback: ProgressCallback = Arc::new(Mutex::new(move |p: &WriteProgress| {
            observed_cb.lock().unwrap().push(p.output_bytes());
        }));
        let tracker = Arc::new(WriteProgressTracker::new(callback, None));

        // A large byte budget keeps all three batches in one part; smooth
        // progress therefore requires bytes to be reported per chunk rather than
        // once when the part completes.
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
        assert!(
            observed.len() > 1,
            "expected multiple incremental progress updates within the part: {observed:?}"
        );
        assert!(
            observed.windows(2).all(|w| w[1] >= w[0]),
            "progress bytes should be monotonic: {observed:?}"
        );
        assert!(
            *observed.last().unwrap() > 0,
            "final progress should report bytes: {observed:?}"
        );
    }
}
