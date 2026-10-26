// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::path::PathBuf;

use lancedb::{Connection, connect};

async fn connect_local() -> lancedb::Result<Connection> {
    // --8<-- [start:connect]
    let db = connect("ex_lancedb").execute().await?;
    // --8<-- [end:connect]
    Ok(db)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    std::env::set_current_dir(temp_dir.path())?;
    let db = connect_local().await?;
    db.create_empty_table(
        "probe",
        std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )])),
    )
    .execute()
    .await?;
    let reopened = connect(temp_dir.path().join("ex_lancedb").to_str().unwrap())
        .execute()
        .await?;
    assert_eq!(reopened.table_names().execute().await?, vec!["probe"]);

    // Connects to a deployment only when asked, and only lists its tables.
    if std::env::var("LANCEDB_DOCS_TEST_DEPLOYMENT").as_deref() == Ok("1") {
        let db = connect_enterprise_quickstart().await?;
        println!("{:?}", db.table_names().execute().await?);
    }
    Ok(())
}

async fn connect_enterprise_quickstart() -> Result<Connection, Box<dyn std::error::Error>> {
    // --8<-- [start:connect_enterprise_quickstart]
    let uri = std::env::var("LANCEDB_URI")?; // db://your-database
    let api_key = std::env::var("LANCEDB_API_KEY")?;
    let region = std::env::var("LANCEDB_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let host_override = std::env::var("LANCEDB_HOST_OVERRIDE")?; // https://your-endpoint

    let db = connect(&uri)
        .api_key(&api_key)
        .region(&region)
        .host_override(&host_override)
        .execute()
        .await?;
    // --8<-- [end:connect_enterprise_quickstart]
    Ok(db)
}

// Compiled as documentation, never called by the example runner.
#[allow(dead_code)]
async fn connect_object_storage() -> lancedb::Result<Connection> {
    // --8<-- [start:connect_object_storage]
    let uri = "s3://your-bucket/path";
    // You can also use "gs://your-bucket/path" or "az://your-container/path".
    let db = connect(uri).execute().await?;
    // --8<-- [end:connect_object_storage]
    Ok(db)
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

// Compiled as documentation, never called by the example runner.
#[allow(dead_code)]
async fn connect_enterprise_placeholders() -> Result<Connection, Box<dyn std::error::Error>> {
    // --8<-- [start:connect_enterprise_placeholders]
    let db = connect("db://your-database")
        .api_key("your-api-key")
        .region("us-east-1")
        .host_override("https://your-endpoint")
        .execute()
        .await?;
    // --8<-- [end:connect_enterprise_placeholders]
    Ok(db)
}
