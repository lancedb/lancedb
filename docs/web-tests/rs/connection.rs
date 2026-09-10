// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::path::PathBuf;

use lancedb::connect;

// --8<-- [start:connect]
async fn connect_example(uri: &str) {
    let db = connect(uri).execute().await.unwrap();
    let _ = db;
}
// --8<-- [end:connect]

#[tokio::main]
async fn main() {
    let temp_dir = tempfile::tempdir().unwrap();
    let uri = temp_dir.path().join("ex_lancedb");
    connect_example(uri.to_str().unwrap()).await;

    // Keep the enterprise snippet in this file, but don't run it in CI.
    let _ = connect_enterprise_quickstart_config();
    let _ = connect_object_storage_config();
}

fn connect_enterprise_quickstart_config() -> (String, String, String, String) {
    // --8<-- [start:connect_enterprise_quickstart]
    let uri = "db://your-database-uri";
    let api_key = "your-api-key";
    let region = "us-east-1";
    let host_override = "https://your-enterprise-endpoint.com";
    // --8<-- [end:connect_enterprise_quickstart]
    (
        uri.to_string(),
        api_key.to_string(),
        region.to_string(),
        host_override.to_string(),
    )
}

fn connect_object_storage_config() -> &'static str {
    // --8<-- [start:connect_object_storage]
    let uri = "s3://your-bucket/path";
    // You can also use "gs://your-bucket/path" or "az://your-container/path".
    // --8<-- [end:connect_object_storage]

    uri
}

async fn namespace_table_ops_example(uri: &str) -> lancedb::Result<()> {
    // --8<-- [start:namespace_table_ops]
    let conn = connect(uri).execute().await?;
    let search_namespace = vec!["prod".to_string(), "search".to_string()];
    let recommendations_namespace = vec!["prod".to_string(), "recommendations".to_string()];

    let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
    ]));

    conn.create_empty_table("user", schema.clone())
        .namespace(search_namespace.clone())
        .execute()
        .await?;

    conn.create_empty_table("user", schema)
        .namespace(recommendations_namespace.clone())
        .execute()
        .await?;

    let search_table_names = conn
        .table_names()
        .namespace(search_namespace)
        .execute()
        .await?;
    let recommendation_table_names = conn
        .table_names()
        .namespace(recommendations_namespace)
        .execute()
        .await?;

    println!("{search_table_names:?}"); // ["user"]
    println!("{recommendation_table_names:?}"); // ["user"]
    // --8<-- [end:namespace_table_ops]
    Ok(())
}

async fn namespace_admin_ops_example() -> lancedb::Result<()> {
    // --8<-- [start:namespace_admin_ops]
    let mut properties = std::collections::HashMap::new();
    properties.insert("root".to_string(), "./local_lancedb".to_string());
    let db = lancedb::connect_namespace("dir", properties).execute().await?;
    let namespace = vec!["prod".to_string(), "search".to_string()];

    db.create_namespace(lance_namespace::models::CreateNamespaceRequest {
        id: Some(vec!["prod".to_string()]),
        ..Default::default()
    })
    .await?;
    db.create_namespace(lance_namespace::models::CreateNamespaceRequest {
        id: Some(namespace.clone()),
        ..Default::default()
    })
    .await?;

    let child_namespaces = db
        .list_namespaces(lance_namespace::models::ListNamespacesRequest {
            id: Some(vec!["prod".to_string()]),
            ..Default::default()
        })
        .await?;
    println!(
        "Child namespaces under {:?}: {:?}",
        namespace, child_namespaces
    );
    // Child namespaces under ["prod", "search"]: ["search"]

    db.drop_namespace(lance_namespace::models::DropNamespaceRequest {
        id: Some(namespace.clone()),
        ..Default::default()
    })
    .await?;
    db.drop_namespace(lance_namespace::models::DropNamespaceRequest {
        id: Some(vec!["prod".to_string()]),
        ..Default::default()
    })
    .await?;
    // --8<-- [end:namespace_admin_ops]
    Ok(())
}

#[allow(dead_code)]
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}
