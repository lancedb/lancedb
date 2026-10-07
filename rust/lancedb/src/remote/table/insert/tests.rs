// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

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
) -> crate::remote::client::RestfulLanceDbClient<crate::remote::client::test_utils::MockSender> {
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
) -> crate::remote::client::RestfulLanceDbClient<crate::remote::client::test_utils::MockSender> {
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
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
        on: vec!["id".to_string()],
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
