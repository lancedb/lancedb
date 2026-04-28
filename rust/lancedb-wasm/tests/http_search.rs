// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, header};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::{Router, serve};
use lancedb::connect;
use lancedb::index::Index;
use lancedb::index::scalar::FtsIndexBuilder;
use lancedb::ipc::ipc_file_to_batches;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tower_http::services::ServeDir;

use lancedb_wasm::{
    OpenTableOptions, RemoteSearchTable, SearchDistanceType, SearchRequest, SelectRequest,
    TextRequest,
};

#[derive(Debug, Clone)]
struct LoggedRequest {
    path: String,
    range: Option<String>,
}

struct TestFixture {
    root: TempDir,
    local_table: lancedb::Table,
    base_url: String,
    requests: Arc<Mutex<Vec<LoggedRequest>>>,
    server_task: tokio::task::JoinHandle<()>,
}

impl Drop for TestFixture {
    fn drop(&mut self) {
        self.server_task.abort();
    }
}

impl TestFixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let db = connect(root.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let local_table = db
            .create_table("search_table", make_batch())
            .execute()
            .await
            .unwrap();

        local_table
            .create_index(&["doc"], Index::FTS(FtsIndexBuilder::default()))
            .execute()
            .await
            .unwrap();

        let requests = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .fallback_service(ServeDir::new(root.path()))
            .layer(middleware::from_fn_with_state(
                requests.clone(),
                log_requests,
            ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            serve(listener, app).await.unwrap();
        });

        Self {
            root,
            local_table,
            base_url: format!("http://{addr}"),
            requests,
            server_task,
        }
    }

    fn table_url(&self, trailing_slash: bool) -> String {
        let url = format!("{}/search_table.lance", self.base_url);
        if trailing_slash {
            format!("{url}/")
        } else {
            url
        }
    }

    async fn open_remote(
        &self,
        trailing_slash: bool,
        mut options: OpenTableOptions,
    ) -> RemoteSearchTable {
        if options.manifest_url.is_none() {
            options.manifest_url = None;
        }
        RemoteSearchTable::open(&self.table_url(trailing_slash), options)
            .await
            .unwrap()
    }
}

async fn log_requests(
    State(requests): State<Arc<Mutex<Vec<LoggedRequest>>>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let logged = LoggedRequest {
        path: request.uri().path().to_string(),
        range: request
            .headers()
            .get(header::RANGE)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned),
    };
    requests.lock().await.push(logged);
    next.run(request).await
}

fn make_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            true,
        ),
        Field::new("doc", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    [
                        Some(vec![Some(0.0), Some(0.0)]),
                        Some(vec![Some(1.0), Some(1.0)]),
                        Some(vec![Some(0.1), Some(0.1)]),
                    ],
                    2,
                ),
            ),
            Arc::new(StringArray::from(vec![
                "apple pie",
                "banana split",
                "apple tart",
            ])),
        ],
    )
    .unwrap()
}

fn decode_batches(bytes: Vec<u8>) -> Vec<RecordBatch> {
    let mut reader = ipc_file_to_batches(bytes).unwrap();
    let mut batches = Vec::new();
    while let Some(batch) = reader.next() {
        batches.push(batch.unwrap());
    }
    batches
}

fn ids_from_batches(batches: &[RecordBatch]) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("id")
                .unwrap()
                .as_primitive::<arrow_array::types::Int32Type>()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn opens_reads_schema_and_searches_over_http() {
    let fixture = TestFixture::new().await;
    let remote = fixture
        .open_remote(false, OpenTableOptions::default())
        .await;

    let schema = lancedb::ipc::ipc_file_to_schema(remote.schema().await.unwrap()).unwrap();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "vector");
    assert_eq!(schema.field(2).name(), "doc");

    let vector_batches = decode_batches(
        remote
            .search(SearchRequest {
                vector: Some(vec![0.0, 0.0]),
                text: None,
                distance_type: None,
                filter: None,
                select: Some(SelectRequest::Columns(vec!["id".into(), "doc".into()])),
                limit: Some(2),
                offset: None,
                vector_column: Some("vector".into()),
                prefilter: None,
                with_row_id: None,
                fast_search: None,
            })
            .await
            .unwrap(),
    );
    let vector_ids = ids_from_batches(&vector_batches);
    assert_eq!(vector_ids.len(), 2);
    assert!(vector_ids.contains(&1));
    assert!(vector_ids.contains(&3));

    let text_batches = decode_batches(
        remote
            .search(SearchRequest {
                vector: None,
                text: Some(TextRequest::Query("apple".into())),
                distance_type: None,
                filter: None,
                select: Some(SelectRequest::Columns(vec!["id".into(), "doc".into()])),
                limit: Some(2),
                offset: None,
                vector_column: None,
                prefilter: None,
                with_row_id: Some(true),
                fast_search: None,
            })
            .await
            .unwrap(),
    );
    assert_eq!(ids_from_batches(&text_batches).len(), 2);
    assert!(text_batches[0].column_by_name("_rowid").is_some());

    let hybrid_batches = decode_batches(
        remote
            .search(SearchRequest {
                vector: Some(vec![0.0, 0.0]),
                text: Some(TextRequest::Query("apple".into())),
                distance_type: None,
                filter: None,
                select: Some(SelectRequest::Columns(vec!["id".into(), "doc".into()])),
                limit: Some(2),
                offset: None,
                vector_column: Some("vector".into()),
                prefilter: Some(true),
                with_row_id: None,
                fast_search: None,
            })
            .await
            .unwrap(),
    );
    let hybrid_ids = ids_from_batches(&hybrid_batches);
    assert_eq!(hybrid_ids.len(), 2);
    assert!(hybrid_ids.iter().all(|id| [1, 3].contains(id)));

    let requests = fixture.requests.lock().await.clone();
    assert!(
        requests
            .iter()
            .any(|request| request.path.ends_with("/search_table.lance/_web.json"))
    );
    assert!(
        !requests
            .iter()
            .any(|request| request.path.ends_with("/search_table.lance/_snapshot.json"))
    );
    assert!(requests.iter().any(|request| {
        request
            .path
            .ends_with("/search_table.lance/_latest.manifest")
            && request.range.as_deref() == Some("bytes=0-0")
    }));
}

#[tokio::test]
async fn published_http_smoke_path_covers_open_search_refresh() {
    let fixture = TestFixture::new().await;
    let mut remote = fixture.open_remote(true, OpenTableOptions::default()).await;

    let schema = lancedb::ipc::ipc_file_to_schema(remote.schema().await.unwrap()).unwrap();
    assert_eq!(schema.fields().len(), 3);

    let text_batches = decode_batches(
        remote
            .search(SearchRequest {
                vector: None,
                text: Some(TextRequest::Query("apple".into())),
                distance_type: None,
                filter: None,
                select: Some(SelectRequest::Columns(vec!["id".into(), "doc".into()])),
                limit: Some(2),
                offset: None,
                vector_column: None,
                prefilter: None,
                with_row_id: None,
                fast_search: None,
            })
            .await
            .unwrap(),
    );
    let text_ids = ids_from_batches(&text_batches);
    assert_eq!(text_ids.len(), 2);
    assert!(text_ids.iter().all(|id| [1, 3].contains(id)));

    let vector_batches = decode_batches(
        remote
            .search(SearchRequest {
                vector: Some(vec![0.0, 0.0]),
                text: None,
                distance_type: None,
                filter: None,
                select: Some(SelectRequest::Columns(vec!["id".into()])),
                limit: Some(2),
                offset: None,
                vector_column: Some("vector".into()),
                prefilter: None,
                with_row_id: None,
                fast_search: None,
            })
            .await
            .unwrap(),
    );
    let vector_ids = ids_from_batches(&vector_batches);
    assert_eq!(vector_ids.len(), 2);
    assert!(vector_ids.iter().all(|id| [1, 3].contains(id)));

    let new_row = RecordBatch::try_new(
        fixture.local_table.schema().await.unwrap(),
        vec![
            Arc::new(Int32Array::from(vec![4])),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    [Some(vec![Some(2.0), Some(2.0)])],
                    2,
                ),
            ),
            Arc::new(StringArray::from(vec!["dragon fruit"])),
        ],
    )
    .unwrap();
    fixture.local_table.add(new_row).execute().await.unwrap();

    assert!(remote.refresh().await.unwrap());

    let refreshed_batches = decode_batches(
        remote
            .search(SearchRequest {
                vector: None,
                text: None,
                distance_type: None,
                filter: Some("id = 4".into()),
                select: Some(SelectRequest::Columns(vec!["id".into(), "doc".into()])),
                limit: Some(1),
                offset: None,
                vector_column: None,
                prefilter: None,
                with_row_id: None,
                fast_search: None,
            })
            .await
            .unwrap(),
    );
    assert_eq!(ids_from_batches(&refreshed_batches), vec![4]);

    let requests = fixture.requests.lock().await.clone();
    assert!(
        requests
            .iter()
            .any(|request| request.path.ends_with("/search_table.lance/_web.json"))
    );
    assert!(
        !requests
            .iter()
            .any(|request| request.path.ends_with("/search_table.lance/_snapshot.json"))
    );
    assert!(requests.iter().any(|request| {
        request
            .path
            .ends_with("/search_table.lance/_latest.version")
    }));
}

#[tokio::test]
async fn refreshes_to_latest_snapshot() {
    let fixture = TestFixture::new().await;
    let mut remote = fixture.open_remote(true, OpenTableOptions::default()).await;

    let new_row = RecordBatch::try_new(
        fixture.local_table.schema().await.unwrap(),
        vec![
            Arc::new(Int32Array::from(vec![4])),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    [Some(vec![Some(2.0), Some(2.0)])],
                    2,
                ),
            ),
            Arc::new(StringArray::from(vec!["dragon fruit"])),
        ],
    )
    .unwrap();
    fixture.local_table.add(new_row).execute().await.unwrap();

    assert!(remote.refresh().await.unwrap());

    let batches = decode_batches(
        remote
            .search(SearchRequest {
                vector: None,
                text: None,
                distance_type: None,
                filter: Some("id = 4".into()),
                select: Some(SelectRequest::Columns(vec!["id".into()])),
                limit: Some(1),
                offset: None,
                vector_column: None,
                prefilter: None,
                with_row_id: None,
                fast_search: None,
            })
            .await
            .unwrap(),
    );

    assert_eq!(ids_from_batches(&batches), vec![4]);
}

#[tokio::test]
async fn refresh_uses_latest_version_sidecar_when_snapshot_is_unchanged() {
    let fixture = TestFixture::new().await;
    let mut remote = fixture.open_remote(true, OpenTableOptions::default()).await;

    fixture.requests.lock().await.clear();
    assert!(!remote.refresh().await.unwrap());

    let requests = fixture.requests.lock().await.clone();
    assert!(requests.iter().any(|request| {
        request
            .path
            .ends_with("/search_table.lance/_latest.version")
    }));
    assert!(!requests.iter().any(|request| {
        request
            .path
            .ends_with("/search_table.lance/_latest.manifest")
            && request.range.as_deref() == Some("bytes=0-0")
    }));
}

#[tokio::test]
async fn supports_manifest_url_override() {
    let fixture = TestFixture::new().await;
    let source_manifest = fixture
        .root
        .path()
        .join("search_table.lance/_latest.manifest");
    let alias_manifest = fixture.root.path().join("search_table-latest.manifest");
    fs::copy(&source_manifest, &alias_manifest).unwrap();
    fs::remove_file(&source_manifest).unwrap();

    let remote = fixture
        .open_remote(
            true,
            OpenTableOptions {
                manifest_url: Some(format!("{}/search_table-latest.manifest", fixture.base_url)),
                ..Default::default()
            },
        )
        .await;

    let schema = lancedb::ipc::ipc_file_to_schema(remote.schema().await.unwrap()).unwrap();
    assert_eq!(schema.field(0).name(), "id");
}

#[tokio::test]
async fn honors_explicit_vector_distance_type() {
    let root = tempfile::tempdir().unwrap();
    let db = connect(root.path().to_str().unwrap())
        .execute()
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    [
                        Some(vec![Some(10.0), Some(0.0)]),
                        Some(vec![Some(1.0), Some(1.0)]),
                        Some(vec![Some(0.0), Some(1.0)]),
                    ],
                    2,
                ),
            ),
        ],
    )
    .unwrap();
    db.create_table("metric_table", batch)
        .execute()
        .await
        .unwrap();

    let requests = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .fallback_service(ServeDir::new(root.path()))
        .layer(middleware::from_fn_with_state(requests, log_requests));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        serve(listener, app).await.unwrap();
    });

    let remote = RemoteSearchTable::open(
        &format!("http://{addr}/metric_table.lance"),
        OpenTableOptions::default(),
    )
    .await
    .unwrap();

    let cosine = decode_batches(
        remote
            .search(SearchRequest {
                vector: Some(vec![1.0, 0.0]),
                text: None,
                distance_type: Some(SearchDistanceType::Cosine),
                filter: None,
                select: Some(SelectRequest::Columns(vec!["id".into()])),
                limit: Some(1),
                offset: None,
                vector_column: Some("vector".into()),
                prefilter: None,
                with_row_id: None,
                fast_search: None,
            })
            .await
            .unwrap(),
    );
    assert_eq!(ids_from_batches(&cosine), vec![1]);

    let l2 = decode_batches(
        remote
            .search(SearchRequest {
                vector: Some(vec![1.0, 0.0]),
                text: None,
                distance_type: Some(SearchDistanceType::L2),
                filter: None,
                select: Some(SelectRequest::Columns(vec!["id".into()])),
                limit: Some(1),
                offset: None,
                vector_column: Some("vector".into()),
                prefilter: None,
                with_row_id: None,
                fast_search: None,
            })
            .await
            .unwrap(),
    );
    assert_eq!(ids_from_batches(&l2), vec![2]);

    server_task.abort();
}
