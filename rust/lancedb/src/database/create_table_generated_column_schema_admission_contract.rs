// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! RED runtime contract tests for create-table schema admission (B4g).
//!
//! Caller-authored Arrow field metadata under
//! [`crate::function::GENERATED_COLUMN_METADATA_KEY`] must not enter table
//! schema state through general-purpose `Database::create_table`. Generated
//! definitions are Job-owned. This module proves the missing admission guard
//! on Native listing, Native namespace, and Remote create paths.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use tempfile::TempDir;

use crate::arrow::SendableRecordBatchStream;
use crate::data::scannable::Scannable;
use crate::database::listing::ListingDatabase;
use crate::database::{CreateTableMode, CreateTableRequest, Database, TableNamesRequest};
use crate::error::Error;
use crate::function::{
    Function, FunctionArgument, FunctionCall, FunctionId, FunctionOutput, FunctionParameter,
    FunctionSignature, GENERATED_COLUMN_METADATA_KEY, GeneratedColumnDefinition,
};

const ID: &str = "id";
const ORDINARY: &str = "ordinary";
const GEN_OUT: &str = "gen_out";
const ORDINARY_META_KEY: &str = "unit";
const ORDINARY_META_VALUE: &str = "label";
const FN_ID: &str = "fn.exact.b4g.create_table.literal";
const MALFORMED_MARKER: &str = "SENSITIVE_B4G_CREATE_TABLE_METADATA_MARKER_9d2e_a7c1";

/// Counts [`Scannable::scan_as_stream`] calls. [`Scannable::schema`] is free.
struct ObservableScannable {
    batch: RecordBatch,
    scan_calls: Arc<AtomicUsize>,
}

impl ObservableScannable {
    fn new(batch: RecordBatch, scan_calls: Arc<AtomicUsize>) -> Self {
        Self { batch, scan_calls }
    }
}

impl Scannable for ObservableScannable {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    fn scan_as_stream(&mut self) -> SendableRecordBatchStream {
        self.scan_calls.fetch_add(1, Ordering::SeqCst);
        self.batch.scan_as_stream()
    }

    fn num_rows(&self) -> Option<usize> {
        Some(self.batch.num_rows())
    }

    fn rescannable(&self) -> bool {
        true
    }
}

fn literal_definition(output_field_id: i32) -> GeneratedColumnDefinition {
    let function = Function::new(
        FunctionId::try_new(FN_ID).unwrap(),
        FunctionSignature::try_new(
            vec![FunctionParameter::new("label", DataType::Utf8)],
            FunctionOutput::new(DataType::Int32, true),
        )
        .unwrap(),
    );
    let call = FunctionCall::try_new(
        &function,
        vec![(
            "label".to_string(),
            FunctionArgument::try_literal(
                Arc::new(StringArray::from(vec![Some("literal-only")])) as arrow_array::ArrayRef
            )
            .unwrap(),
        )],
    )
    .unwrap();
    GeneratedColumnDefinition::try_new(output_field_id, call, 1, 1).unwrap()
}

fn valid_reserved_payload() -> String {
    literal_definition(1).to_metadata_json().unwrap()
}

fn malformed_reserved_payload() -> String {
    format!(
        r#"{{"format_version":1,"output_field_id":1,"function_call":"{MALFORMED_MARKER}","dependency_epoch":1,"materialized_epoch":1}}"#
    )
}

fn batch_with_field_metadata(metadata: HashMap<String, String>) -> RecordBatch {
    let gen_field = Field::new(GEN_OUT, DataType::Int32, true).with_metadata(metadata);
    let schema = Arc::new(Schema::new(vec![
        Field::new(ID, DataType::Int32, false),
        Field::new(ORDINARY, DataType::Utf8, true),
        gen_field,
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("seed")])),
            Arc::new(Int32Array::from(vec![10])),
        ],
    )
    .unwrap()
}

fn reserved_batch(payload: &str) -> RecordBatch {
    batch_with_field_metadata(
        [(
            GENERATED_COLUMN_METADATA_KEY.to_string(),
            payload.to_string(),
        )]
        .into(),
    )
}

fn ordinary_metadata_batch() -> RecordBatch {
    batch_with_field_metadata(
        [(
            ORDINARY_META_KEY.to_string(),
            ORDINARY_META_VALUE.to_string(),
        )]
        .into(),
    )
}

fn plain_seed_batch() -> RecordBatch {
    batch_with_field_metadata(HashMap::new())
}

fn assert_not_supported_redacted(err: &Error, label: &str, forbidden_substrings: &[&str]) {
    match err {
        Error::NotSupported { message } => {
            let rendered = format!("{err}\n{err:?}\n{message}");
            assert!(
                !rendered.contains(GENERATED_COLUMN_METADATA_KEY),
                "{label}: leaked metadata wire key: {rendered}"
            );
            assert!(
                !rendered.contains(FN_ID),
                "{label}: leaked Function ID: {rendered}"
            );
            assert!(
                !rendered.contains(MALFORMED_MARKER),
                "{label}: leaked malformed marker: {rendered}"
            );
            for needle in forbidden_substrings {
                assert!(
                    !rendered.contains(needle),
                    "{label}: leaked forbidden substring `{needle}`: {rendered}"
                );
            }
            assert!(
                message.to_lowercase().contains("generated")
                    || message.to_lowercase().contains("job"),
                "{label}: message must describe Job-owned generated-column boundary: {message}"
            );
        }
        other => panic!("{label}: expected Error::NotSupported, got {other:?}"),
    }
}

async fn listing_db() -> (TempDir, ListingDatabase) {
    let tmp = tempfile::tempdir().unwrap();
    let uri = tmp.path().to_str().unwrap();
    let request = crate::connection::ConnectRequest {
        uri: uri.to_string(),
        #[cfg(feature = "remote")]
        client_config: Default::default(),
        options: Default::default(),
        namespace_client_properties: Default::default(),
        manifest_enabled: false,
        read_consistency_interval: None,
        session: None,
    };
    let db = ListingDatabase::connect_with_options(&request)
        .await
        .unwrap();
    (tmp, db)
}

fn listing_table_dir(tmp: &TempDir, name: &str) -> std::path::PathBuf {
    tmp.path().join(format!("{name}.lance"))
}

async fn listing_create(
    db: &ListingDatabase,
    name: &str,
    data: Box<dyn Scannable>,
    mode: CreateTableMode,
) -> crate::Result<Arc<dyn crate::table::BaseTable>> {
    db.create_table(CreateTableRequest {
        name: name.to_string(),
        namespace_path: vec![],
        data,
        mode,
        write_options: Default::default(),
        location: None,
        namespace_client: None,
    })
    .await
}

async fn assert_listing_absent(db: &ListingDatabase, tmp: &TempDir, name: &str) {
    #[allow(deprecated)]
    let names = db.table_names(TableNamesRequest::default()).await.unwrap();
    assert!(
        !names.contains(&name.to_string()),
        "rejected create must leave no listed table `{name}`; got {names:?}"
    );
    assert!(
        !listing_table_dir(tmp, name).exists(),
        "rejected create must leave no storage directory for `{name}`"
    );
}

#[tokio::test]
async fn listing_create_rejects_reserved_generated_column_metadata_before_scan() {
    let (tmp, db) = listing_db().await;
    let payload = valid_reserved_payload();
    let scan_calls = Arc::new(AtomicUsize::new(0));
    let data = Box::new(ObservableScannable::new(
        reserved_batch(&payload),
        scan_calls.clone(),
    )) as Box<dyn Scannable>;

    let err = listing_create(&db, "b4g_listing_create", data, CreateTableMode::Create)
        .await
        .expect_err("listing Create must reject reserved generated-column metadata");
    assert_not_supported_redacted(
        &err,
        "listing Create reserved admission",
        &[payload.as_str()],
    );
    assert_eq!(
        scan_calls.load(Ordering::SeqCst),
        0,
        "rejection must occur before Scannable::scan_as_stream"
    );
    assert_listing_absent(&db, &tmp, "b4g_listing_create").await;
}

#[tokio::test]
async fn listing_overwrite_rejects_reserved_generated_column_metadata_and_preserves_table() {
    let (tmp, db) = listing_db().await;
    let seed = listing_create(
        &db,
        "b4g_listing_overwrite",
        Box::new(plain_seed_batch()) as Box<dyn Scannable>,
        CreateTableMode::Create,
    )
    .await
    .unwrap();
    let version_before = seed.version().await.unwrap();
    let schema_before = seed.schema().await.unwrap();
    assert!(
        !schema_before
            .field_with_name(GEN_OUT)
            .unwrap()
            .metadata()
            .contains_key(GENERATED_COLUMN_METADATA_KEY)
    );

    let payload = malformed_reserved_payload();
    assert!(payload.contains(MALFORMED_MARKER));
    let scan_calls = Arc::new(AtomicUsize::new(0));
    let data = Box::new(ObservableScannable::new(
        reserved_batch(&payload),
        scan_calls.clone(),
    )) as Box<dyn Scannable>;

    let err = listing_create(
        &db,
        "b4g_listing_overwrite",
        data,
        CreateTableMode::Overwrite,
    )
    .await
    .expect_err("listing Overwrite must reject reserved generated-column metadata");
    assert_not_supported_redacted(
        &err,
        "listing Overwrite reserved admission",
        &[payload.as_str()],
    );
    assert_eq!(scan_calls.load(Ordering::SeqCst), 0);

    let reopened = db
        .open_table(crate::database::OpenTableRequest {
            name: "b4g_listing_overwrite".to_string(),
            namespace_path: vec![],
            index_cache_size: None,
            lance_read_params: None,
            location: None,
            namespace_client: None,
            managed_versioning: None,
        })
        .await
        .unwrap();
    assert_eq!(reopened.version().await.unwrap(), version_before);
    let schema_after = reopened.schema().await.unwrap();
    assert_eq!(schema_after.as_ref(), schema_before.as_ref());
    assert!(
        !schema_after
            .field_with_name(GEN_OUT)
            .unwrap()
            .metadata()
            .contains_key(GENERATED_COLUMN_METADATA_KEY)
    );
    assert!(listing_table_dir(&tmp, "b4g_listing_overwrite").exists());
}

#[tokio::test]
async fn listing_exist_ok_absent_rejects_reserved_generated_column_metadata_before_scan() {
    let (tmp, db) = listing_db().await;
    let payload = valid_reserved_payload();
    let scan_calls = Arc::new(AtomicUsize::new(0));
    let data = Box::new(ObservableScannable::new(
        reserved_batch(&payload),
        scan_calls.clone(),
    )) as Box<dyn Scannable>;

    let err = listing_create(
        &db,
        "b4g_listing_exist_ok",
        data,
        CreateTableMode::exist_ok(|req| req),
    )
    .await
    .expect_err("listing ExistOk (absent) must reject reserved generated-column metadata");
    assert_not_supported_redacted(
        &err,
        "listing ExistOk absent reserved admission",
        &[payload.as_str()],
    );
    assert_eq!(scan_calls.load(Ordering::SeqCst), 0);
    assert_listing_absent(&db, &tmp, "b4g_listing_exist_ok").await;
}

#[tokio::test]
async fn listing_ordinary_field_metadata_is_accepted_and_preserved() {
    let (_tmp, db) = listing_db().await;
    let scan_calls = Arc::new(AtomicUsize::new(0));
    let data = Box::new(ObservableScannable::new(
        ordinary_metadata_batch(),
        scan_calls.clone(),
    )) as Box<dyn Scannable>;

    let table = listing_create(&db, "b4g_listing_ordinary", data, CreateTableMode::Create)
        .await
        .expect("ordinary field metadata must remain accepted");
    assert!(
        scan_calls.load(Ordering::SeqCst) > 0,
        "successful create may consume the Scannable"
    );

    let schema = table.schema().await.unwrap();
    let md = schema.field_with_name(GEN_OUT).unwrap().metadata();
    assert_eq!(
        md.get(ORDINARY_META_KEY).map(String::as_str),
        Some(ORDINARY_META_VALUE)
    );
    assert!(!md.contains_key(GENERATED_COLUMN_METADATA_KEY));
}

#[cfg(not(windows))] // directory namespace tests are unix-only in this crate
mod namespace_admission {
    use super::*;
    use crate::connect_namespace;
    use lance_namespace::models::{CreateNamespaceRequest, DescribeTableRequest};

    async fn namespace_conn() -> (TempDir, crate::Connection) {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_str().unwrap().to_string();
        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root);
        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .unwrap();
        conn.create_namespace(CreateNamespaceRequest {
            id: Some(vec!["b4g_ns".into()]),
            ..Default::default()
        })
        .await
        .unwrap();
        (tmp, conn)
    }

    async fn assert_namespace_undeclared(conn: &crate::Connection, name: &str) {
        let names = conn
            .table_names()
            .namespace(vec!["b4g_ns".into()])
            .execute()
            .await
            .unwrap();
        assert!(
            !names.contains(&name.to_string()),
            "rejected namespace create must leave no declared/listed table `{name}`; got {names:?}"
        );
        let ns = conn.namespace_client().await.unwrap();
        let describe = ns
            .describe_table(DescribeTableRequest {
                id: Some(vec!["b4g_ns".into(), name.into()]),
                ..Default::default()
            })
            .await;
        assert!(
            describe.is_err(),
            "rejected namespace create must leave no describable table `{name}`"
        );
    }

    async fn namespace_create(
        conn: &crate::Connection,
        name: &str,
        data: Box<dyn Scannable>,
        mode: CreateTableMode,
    ) -> crate::Result<Arc<dyn crate::table::BaseTable>> {
        conn.database()
            .create_table(CreateTableRequest {
                name: name.to_string(),
                namespace_path: vec!["b4g_ns".into()],
                data,
                mode,
                write_options: Default::default(),
                location: None,
                namespace_client: None,
            })
            .await
    }

    #[tokio::test]
    async fn namespace_create_rejects_reserved_before_declare_describe_or_storage() {
        let (_tmp, conn) = namespace_conn().await;
        let payload = valid_reserved_payload();
        let scan_calls = Arc::new(AtomicUsize::new(0));
        let data = Box::new(ObservableScannable::new(
            reserved_batch(&payload),
            scan_calls.clone(),
        )) as Box<dyn Scannable>;

        let err = namespace_create(&conn, "b4g_ns_create", data, CreateTableMode::Create)
            .await
            .expect_err("namespace Create must reject reserved generated-column metadata");
        assert_not_supported_redacted(
            &err,
            "namespace Create reserved admission",
            &[payload.as_str()],
        );
        assert_eq!(scan_calls.load(Ordering::SeqCst), 0);
        assert_namespace_undeclared(&conn, "b4g_ns_create").await;
    }

    #[tokio::test]
    async fn namespace_overwrite_rejects_reserved_before_declare_describe_or_storage() {
        let (_tmp, conn) = namespace_conn().await;
        let seed = namespace_create(
            &conn,
            "b4g_ns_overwrite",
            Box::new(plain_seed_batch()) as Box<dyn Scannable>,
            CreateTableMode::Create,
        )
        .await
        .unwrap();
        let version_before = seed.version().await.unwrap();
        let schema_before = seed.schema().await.unwrap();

        let payload = malformed_reserved_payload();
        let scan_calls = Arc::new(AtomicUsize::new(0));
        let data = Box::new(ObservableScannable::new(
            reserved_batch(&payload),
            scan_calls.clone(),
        )) as Box<dyn Scannable>;

        let err = namespace_create(&conn, "b4g_ns_overwrite", data, CreateTableMode::Overwrite)
            .await
            .expect_err("namespace Overwrite must reject reserved generated-column metadata");
        assert_not_supported_redacted(
            &err,
            "namespace Overwrite reserved admission",
            &[payload.as_str()],
        );
        assert_eq!(scan_calls.load(Ordering::SeqCst), 0);

        let reopened = conn
            .database()
            .open_table(crate::database::OpenTableRequest {
                name: "b4g_ns_overwrite".to_string(),
                namespace_path: vec!["b4g_ns".into()],
                index_cache_size: None,
                lance_read_params: None,
                location: None,
                namespace_client: None,
                managed_versioning: None,
            })
            .await
            .unwrap();
        assert_eq!(reopened.version().await.unwrap(), version_before);
        assert_eq!(
            reopened.schema().await.unwrap().as_ref(),
            schema_before.as_ref()
        );
        assert!(
            !reopened
                .schema()
                .await
                .unwrap()
                .field_with_name(GEN_OUT)
                .unwrap()
                .metadata()
                .contains_key(GENERATED_COLUMN_METADATA_KEY)
        );
    }

    #[tokio::test]
    async fn namespace_exist_ok_absent_rejects_reserved_before_declare_describe_or_storage() {
        let (_tmp, conn) = namespace_conn().await;
        let payload = valid_reserved_payload();
        let scan_calls = Arc::new(AtomicUsize::new(0));
        let data = Box::new(ObservableScannable::new(
            reserved_batch(&payload),
            scan_calls.clone(),
        )) as Box<dyn Scannable>;

        let err = namespace_create(
            &conn,
            "b4g_ns_exist_ok",
            data,
            CreateTableMode::exist_ok(|req| req),
        )
        .await
        .expect_err("namespace ExistOk (absent) must reject reserved generated-column metadata");
        assert_not_supported_redacted(
            &err,
            "namespace ExistOk absent reserved admission",
            &[payload.as_str()],
        );
        assert_eq!(scan_calls.load(Ordering::SeqCst), 0);
        assert_namespace_undeclared(&conn, "b4g_ns_exist_ok").await;
    }

    #[tokio::test]
    async fn namespace_exist_ok_existing_rejects_reserved_even_when_mode_would_ignore_data() {
        let (_tmp, conn) = namespace_conn().await;
        let seed = namespace_create(
            &conn,
            "b4g_ns_exist_ok_existing",
            Box::new(plain_seed_batch()) as Box<dyn Scannable>,
            CreateTableMode::Create,
        )
        .await
        .unwrap();
        let version_before = seed.version().await.unwrap();
        let schema_before = seed.schema().await.unwrap();

        let payload = valid_reserved_payload();
        let scan_calls = Arc::new(AtomicUsize::new(0));
        let data = Box::new(ObservableScannable::new(
            reserved_batch(&payload),
            scan_calls.clone(),
        )) as Box<dyn Scannable>;

        let err = namespace_create(
            &conn,
            "b4g_ns_exist_ok_existing",
            data,
            CreateTableMode::exist_ok(|req| req),
        )
        .await
        .expect_err(
            "namespace ExistOk must not accept reserved metadata merely because data is ignored",
        );
        assert_not_supported_redacted(
            &err,
            "namespace ExistOk existing reserved admission",
            &[payload.as_str()],
        );
        assert_eq!(scan_calls.load(Ordering::SeqCst), 0);

        let reopened = conn
            .database()
            .open_table(crate::database::OpenTableRequest {
                name: "b4g_ns_exist_ok_existing".to_string(),
                namespace_path: vec!["b4g_ns".into()],
                index_cache_size: None,
                lance_read_params: None,
                location: None,
                namespace_client: None,
                managed_versioning: None,
            })
            .await
            .unwrap();
        assert_eq!(reopened.version().await.unwrap(), version_before);
        assert_eq!(
            reopened.schema().await.unwrap().as_ref(),
            schema_before.as_ref()
        );
    }
}

#[cfg(feature = "remote")]
mod remote_admission {
    use super::*;
    use std::io::Cursor;

    use arrow_ipc::reader::StreamReader;
    use async_trait::async_trait;

    use crate::Connection;
    use crate::remote::{ClientConfig, HeaderProvider};

    #[derive(Debug)]
    struct CountingHeaderProvider {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl HeaderProvider for CountingHeaderProvider {
        async fn get_headers(&self) -> crate::Result<HashMap<String, String>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(HashMap::from([(
                "X-B4g-Test".to_string(),
                "must-not-be-requested".to_string(),
            )]))
        }
    }

    fn counting_handler(
        calls: Arc<AtomicUsize>,
    ) -> impl Fn(reqwest::Request) -> http::Response<String> + Clone + Send + Sync + 'static {
        move |_request| {
            calls.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                .status(200)
                .body(String::new())
                .unwrap()
        }
    }

    async fn remote_create(
        conn: &Connection,
        name: &str,
        data: Box<dyn Scannable>,
        mode: CreateTableMode,
    ) -> crate::Result<Arc<dyn crate::table::BaseTable>> {
        // Direct Database trait path used by Connection::create_table.
        conn.database()
            .create_table(CreateTableRequest {
                name: name.to_string(),
                namespace_path: vec![],
                data,
                mode,
                write_options: Default::default(),
                location: None,
                namespace_client: None,
            })
            .await
    }

    async fn assert_remote_rejects(
        mode: CreateTableMode,
        table_name: &str,
        payload: &str,
        label: &str,
    ) {
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let header_calls = Arc::new(AtomicUsize::new(0));
        let scan_calls = Arc::new(AtomicUsize::new(0));
        let config = ClientConfig {
            header_provider: Some(Arc::new(CountingHeaderProvider {
                calls: header_calls.clone(),
            }) as Arc<dyn HeaderProvider>),
            ..Default::default()
        };
        let conn = Connection::new_with_handler_and_config(
            counting_handler(handler_calls.clone()),
            config,
        );
        let data = Box::new(ObservableScannable::new(
            reserved_batch(payload),
            scan_calls.clone(),
        )) as Box<dyn Scannable>;

        let err = remote_create(&conn, table_name, data, mode)
            .await
            .expect_err("remote create must reject reserved generated-column metadata");
        assert_not_supported_redacted(&err, label, &[payload]);
        assert_eq!(
            scan_calls.load(Ordering::SeqCst),
            0,
            "{label}: rejection must occur before scan_as_stream"
        );
        assert_eq!(
            header_calls.load(Ordering::SeqCst),
            0,
            "{label}: rejection must occur before header-provider invocation"
        );
        assert_eq!(
            handler_calls.load(Ordering::SeqCst),
            0,
            "{label}: rejection must occur before HTTP handler"
        );
    }

    #[tokio::test]
    async fn remote_create_rejects_reserved_before_scan_headers_and_http() {
        assert_remote_rejects(
            CreateTableMode::Create,
            "b4g_remote_create",
            &valid_reserved_payload(),
            "remote Create reserved admission",
        )
        .await;
    }

    #[tokio::test]
    async fn remote_overwrite_rejects_reserved_before_scan_headers_and_http() {
        assert_remote_rejects(
            CreateTableMode::Overwrite,
            "b4g_remote_overwrite",
            &malformed_reserved_payload(),
            "remote Overwrite reserved admission",
        )
        .await;
    }

    #[tokio::test]
    async fn remote_exist_ok_rejects_reserved_before_scan_headers_and_http() {
        assert_remote_rejects(
            CreateTableMode::exist_ok(|req| req),
            "b4g_remote_exist_ok",
            &valid_reserved_payload(),
            "remote ExistOk reserved admission",
        )
        .await;
    }

    #[tokio::test]
    async fn remote_ordinary_field_metadata_is_transmitted_unchanged() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(
                request.url().path(),
                "/v1/table/b4g_remote_ordinary/create/"
            );
            let body = request
                .body()
                .and_then(|b| b.as_bytes())
                .expect("ordinary create must send an Arrow IPC body");
            let reader = StreamReader::try_new(Cursor::new(body), None).unwrap();
            let schema = reader.schema();
            let md = schema.field_with_name(GEN_OUT).unwrap().metadata();
            assert_eq!(
                md.get(ORDINARY_META_KEY).map(String::as_str),
                Some(ORDINARY_META_VALUE),
                "ordinary field metadata must be transmitted unchanged"
            );
            assert!(!md.contains_key(GENERATED_COLUMN_METADATA_KEY));
            // Consume stream to completion for a well-formed IPC body.
            for batch in reader {
                batch.unwrap();
            }
            http::Response::builder()
                .status(200)
                .body(String::new())
                .unwrap()
        });

        conn.create_table("b4g_remote_ordinary", ordinary_metadata_batch())
            .mode(CreateTableMode::Create)
            .execute()
            .await
            .expect("ordinary field metadata must remain accepted on remote create");
    }
}
