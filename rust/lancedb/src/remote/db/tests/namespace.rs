// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[tokio::test]
async fn test_header_provider_in_request() {
    // Test HeaderProvider implementation that adds custom headers
    #[derive(Debug, Clone)]
    struct TestHeaderProvider {
        headers: HashMap<String, String>,
    }

    #[async_trait::async_trait]
    impl HeaderProvider for TestHeaderProvider {
        async fn get_headers(&self) -> crate::Result<HashMap<String, String>> {
            Ok(self.headers.clone())
        }
    }

    // Create a test header provider with custom headers
    let mut headers = HashMap::new();
    headers.insert("X-Custom-Auth".to_string(), "test-token".to_string());
    headers.insert("X-Request-Id".to_string(), "test-123".to_string());
    let provider = Arc::new(TestHeaderProvider { headers }) as Arc<dyn HeaderProvider>;

    // Create client config with the header provider
    let client_config = ClientConfig {
        header_provider: Some(provider),
        ..Default::default()
    };

    // Create connection with handler that verifies the headers are present
    let conn = Connection::new_with_handler_and_config(
        move |request| {
            // Verify that our custom headers are present
            assert_eq!(
                request.headers().get("X-Custom-Auth").unwrap(),
                "test-token"
            );
            assert_eq!(request.headers().get("X-Request-Id").unwrap(), "test-123");

            // Also check standard headers are still there
            assert_eq!(request.method(), &reqwest::Method::GET);
            assert_eq!(request.url().path(), "/v1/table/");

            http::Response::builder()
                .status(200)
                .body(r#"{"tables": ["table1", "table2"]}"#)
                .unwrap()
        },
        client_config,
    );

    // Make a request that should include the custom headers
    let names = conn.table_names().execute().await.unwrap();
    assert_eq!(names, vec!["table1", "table2"]);
}

#[tokio::test]
async fn test_header_provider_error_handling() {
    // Test HeaderProvider that returns an error
    #[derive(Debug)]
    struct ErrorHeaderProvider;

    #[async_trait::async_trait]
    impl HeaderProvider for ErrorHeaderProvider {
        async fn get_headers(&self) -> crate::Result<HashMap<String, String>> {
            Err(crate::Error::Runtime {
                message: "Failed to fetch auth token".to_string(),
            })
        }
    }

    let provider = Arc::new(ErrorHeaderProvider) as Arc<dyn HeaderProvider>;
    let client_config = ClientConfig {
        header_provider: Some(provider),
        ..Default::default()
    };

    // Create connection - handler won't be called because header provider fails
    let conn = Connection::new_with_handler_and_config(
        move |_request| -> http::Response<&'static str> {
            panic!("Handler should not be called when header provider fails");
        },
        client_config,
    );

    // Request should fail due to header provider error
    let result = conn.table_names().execute().await;
    assert!(result.is_err());

    match result.unwrap_err() {
        crate::Error::Runtime { message } => {
            assert_eq!(message, "Failed to fetch auth token");
        }
        _ => panic!("Expected Runtime error from header provider"),
    }
}

#[tokio::test]
async fn test_clone_table() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/table/cloned_table/clone");
        assert_eq!(
            request.headers().get("Content-Type").unwrap(),
            JSON_CONTENT_TYPE
        );

        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(body["source_location"], "s3://bucket/source_table");
        assert_eq!(body["is_shallow"], true);

        http::Response::builder().status(200).body("").unwrap()
    });

    let table = conn
        .clone_table("cloned_table", "s3://bucket/source_table")
        .execute()
        .await
        .unwrap();
    assert_eq!(table.name(), "cloned_table");
}

#[tokio::test]
async fn test_clone_table_with_version() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/table/cloned_table/clone");

        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(body["source_location"], "s3://bucket/source_table");
        assert_eq!(body["source_version"], 42);
        assert_eq!(body["is_shallow"], true);

        http::Response::builder().status(200).body("").unwrap()
    });

    let table = conn
        .clone_table("cloned_table", "s3://bucket/source_table")
        .source_version(42)
        .execute()
        .await
        .unwrap();
    assert_eq!(table.name(), "cloned_table");
}

#[tokio::test]
async fn test_clone_table_with_tag() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/table/cloned_table/clone");

        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(body["source_location"], "s3://bucket/source_table");
        assert_eq!(body["source_tag"], "v1.0");
        assert_eq!(body["is_shallow"], true);

        http::Response::builder().status(200).body("").unwrap()
    });

    let table = conn
        .clone_table("cloned_table", "s3://bucket/source_table")
        .source_tag("v1.0")
        .execute()
        .await
        .unwrap();
    assert_eq!(table.name(), "cloned_table");
}

#[tokio::test]
async fn test_clone_table_deep_clone() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/table/cloned_table/clone");

        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(body["source_location"], "s3://bucket/source_table");
        assert_eq!(body["is_shallow"], false);

        http::Response::builder().status(200).body("").unwrap()
    });

    let table = conn
        .clone_table("cloned_table", "s3://bucket/source_table")
        .is_shallow(false)
        .execute()
        .await
        .unwrap();
    assert_eq!(table.name(), "cloned_table");
}

#[tokio::test]
async fn test_clone_table_with_namespace() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/table/ns1$ns2$cloned_table/clone");

        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(body["source_location"], "s3://bucket/source_table");
        assert_eq!(body["is_shallow"], true);

        http::Response::builder().status(200).body("").unwrap()
    });

    let table = conn
        .clone_table("cloned_table", "s3://bucket/source_table")
        .target_namespace(vec!["ns1".to_string(), "ns2".to_string()])
        .execute()
        .await
        .unwrap();
    assert_eq!(table.name(), "cloned_table");
}

#[tokio::test]
async fn test_clone_table_error() {
    let conn = Connection::new_with_handler(|_| {
        http::Response::builder()
            .status(500)
            .body("Internal server error")
            .unwrap()
    });

    let result = conn
        .clone_table("cloned_table", "s3://bucket/source_table")
        .execute()
        .await;

    assert!(result.is_err());
    if let Err(crate::Error::Http { source, .. }) = result {
        assert!(source.to_string().contains("Failed to clone table"));
    } else {
        panic!("Expected HTTP error");
    }
}

#[tokio::test]
async fn test_namespace_client() {
    let conn = Connection::new_with_handler(|_| {
        http::Response::builder()
            .status(200)
            .body(r#"{"tables": []}"#)
            .unwrap()
    });

    // Get the namespace client from the connection's internal database
    let namespace_client = conn.namespace_client().await;
    assert!(namespace_client.is_ok());
}

#[tokio::test]
async fn test_namespace_client_with_tls_config() {
    use crate::remote::client::TlsConfig;

    let tls_config = TlsConfig {
        cert_file: Some("/path/to/cert.pem".to_string()),
        key_file: Some("/path/to/key.pem".to_string()),
        ssl_ca_cert: Some("/path/to/ca.pem".to_string()),
        assert_hostname: true,
    };

    let client_config = ClientConfig {
        tls_config: Some(tls_config),
        ..Default::default()
    };

    let conn = Connection::new_with_handler_and_config(
        |_| {
            http::Response::builder()
                .status(200)
                .body(r#"{"tables": []}"#)
                .unwrap()
        },
        client_config,
    );

    // Get the namespace client - it should be created with the TLS config
    let namespace_client = conn.namespace_client().await;
    assert!(namespace_client.is_ok());
}

#[tokio::test]
async fn test_namespace_client_with_headers() {
    let mut extra_headers = HashMap::new();
    extra_headers.insert("X-Custom-Header".to_string(), "custom-value".to_string());

    let client_config = ClientConfig {
        extra_headers,
        ..Default::default()
    };

    let conn = Connection::new_with_handler_and_config(
        |_| {
            http::Response::builder()
                .status(200)
                .body(r#"{"tables": []}"#)
                .unwrap()
        },
        client_config,
    );

    // Get the namespace client - it should be created with the extra headers
    let namespace_client = conn.namespace_client().await;
    assert!(namespace_client.is_ok());
}

#[test]
fn test_namespace_header_provider_context_maps_headers() {
    #[derive(Debug)]
    struct TestHeaderProvider;

    #[async_trait::async_trait]
    impl HeaderProvider for TestHeaderProvider {
        async fn get_headers(&self) -> crate::Result<HashMap<String, String>> {
            Ok(HashMap::from([(
                "authorization".to_string(),
                "Bearer token".to_string(),
            )]))
        }
    }

    let context_provider = NamespaceHeaderProviderContext {
        header_provider: Arc::new(TestHeaderProvider) as Arc<dyn HeaderProvider>,
    };

    let context = context_provider.provide_context(&OperationInfo::new("list_tables", "namespace"));

    assert_eq!(
        context.get("headers.authorization"),
        Some(&"Bearer token".to_string())
    );
}

#[tokio::test]
async fn test_namespace_client_supports_dynamic_headers() {
    #[derive(Debug)]
    struct TestHeaderProvider;

    #[async_trait::async_trait]
    impl HeaderProvider for TestHeaderProvider {
        async fn get_headers(&self) -> crate::Result<HashMap<String, String>> {
            Ok(HashMap::from([(
                "authorization".to_string(),
                "Bearer token".to_string(),
            )]))
        }
    }

    let client_config = ClientConfig {
        header_provider: Some(Arc::new(TestHeaderProvider) as Arc<dyn HeaderProvider>),
        ..Default::default()
    };

    let conn = Connection::new_with_handler_and_config(
        |_| {
            http::Response::builder()
                .status(200)
                .body(r#"{"tables": []}"#)
                .unwrap()
        },
        client_config,
    );

    let namespace_client = conn.namespace_client().await;
    assert!(namespace_client.is_ok());

    match conn.namespace_client_config().await {
        Err(Error::NotSupported { message })
            if message.contains("dynamic headers are configured") => {}
        Err(err) => panic!("expected NotSupported, got {err:?}"),
        Ok(_) => panic!("expected namespace_client_config to reject dynamic headers"),
    }
}

/// Integration tests using RestAdapter to run RemoteDatabase against a real namespace server
mod rest_adapter_integration {
    use super::*;
    use lance_namespace::models::ListTablesRequest;
    use lance_namespace_impls::{DirectoryNamespaceBuilder, RestAdapter, RestAdapterConfig};
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Test fixture that manages a REST server backed by DirectoryNamespace
    struct RestServerFixture {
        _temp_dir: TempDir,
        server_handle: lance_namespace_impls::RestAdapterHandle,
        server_url: String,
    }

    impl RestServerFixture {
        async fn new() -> Self {
            let temp_dir = TempDir::new().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap().to_string();

            // Create DirectoryNamespace backend
            let backend = DirectoryNamespaceBuilder::new(&temp_path)
                .build()
                .await
                .unwrap();
            let backend = Arc::new(backend);

            // Start REST server with port 0 (OS assigns available port)
            let config = RestAdapterConfig {
                port: 0,
                ..Default::default()
            };

            let server = RestAdapter::new(backend, config);
            let server_handle = server.start().await.unwrap();

            // Get the actual port assigned by OS
            let actual_port = server_handle.port();
            let server_url = format!("http://127.0.0.1:{}", actual_port);

            Self {
                _temp_dir: temp_dir,
                server_handle,
                server_url,
            }
        }
    }

    impl Drop for RestServerFixture {
        fn drop(&mut self) {
            self.server_handle.shutdown();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_remote_database_with_rest_adapter() {
        use lance_namespace::models::CreateNamespaceRequest;

        let fixture = RestServerFixture::new().await;

        // Connect to the REST server using lancedb Connection
        // Use db://dummy as URI and set actual server URL via host_override
        let conn = ConnectBuilder::new("db://dummy")
            .api_key("test-api-key")
            .region("us-east-1")
            .host_override(&fixture.server_url)
            .execute()
            .await
            .unwrap();

        // Create a child namespace first
        let namespace = vec!["test_ns".to_string()];
        conn.create_namespace(CreateNamespaceRequest {
            id: Some(namespace.clone()),
            ..Default::default()
        })
        .await
        .expect("Failed to create namespace");

        // Create a table in the child namespace
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let table = conn
            .create_table("test_table", data)
            .namespace(namespace.clone())
            .execute()
            .await;
        assert!(table.is_ok(), "Failed to create table: {:?}", table.err());

        // List tables in the child namespace
        let list_response = conn
            .list_tables(ListTablesRequest {
                id: Some(namespace.clone()),
                ..Default::default()
            })
            .await
            .expect("Failed to list tables");
        assert_eq!(list_response.tables, vec!["test_table"]);

        // Get namespace client and verify it can also list tables
        let namespace_client = conn.namespace_client().await.unwrap();
        let list_response = namespace_client
            .list_tables(ListTablesRequest {
                id: Some(namespace.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(list_response.tables, vec!["test_table"]);

        // Open the table from the child namespace
        let opened_table = conn
            .open_table("test_table")
            .namespace(namespace.clone())
            .execute()
            .await;
        assert!(
            opened_table.is_ok(),
            "Failed to open table: {:?}",
            opened_table.err()
        );
        assert_eq!(opened_table.unwrap().name(), "test_table");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_remote_database_with_multiple_tables() {
        use lance_namespace::models::CreateNamespaceRequest;

        let fixture = RestServerFixture::new().await;

        // Connect to the REST server
        // Use db://dummy as URI and set actual server URL via host_override
        let conn = ConnectBuilder::new("db://dummy")
            .api_key("test-api-key")
            .region("us-east-1")
            .host_override(&fixture.server_url)
            .execute()
            .await
            .unwrap();

        // Create a child namespace first
        let namespace = vec!["multi_table_ns".to_string()];
        conn.create_namespace(CreateNamespaceRequest {
            id: Some(namespace.clone()),
            ..Default::default()
        })
        .await
        .expect("Failed to create namespace");

        // Create multiple tables in the child namespace
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        for i in 1..=3 {
            let data =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![i]))])
                    .unwrap();
            conn.create_table(format!("table{}", i), data)
                .namespace(namespace.clone())
                .execute()
                .await
                .unwrap_or_else(|e| panic!("Failed to create table{}: {:?}", i, e));
        }

        // List tables in the child namespace
        let list_response = conn
            .list_tables(ListTablesRequest {
                id: Some(namespace.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(list_response.tables.len(), 3);
        assert!(list_response.tables.contains(&"table1".to_string()));
        assert!(list_response.tables.contains(&"table2".to_string()));
        assert!(list_response.tables.contains(&"table3".to_string()));
    }
}
