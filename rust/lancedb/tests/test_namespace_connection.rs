// Test for namespace connections without DynamicConfigProvider

use lancedb::connect_namespace;
use std::collections::HashMap;
use tempfile::tempdir;

#[tokio::test]
async fn test_namespace_connection_simple() {
    // Test that namespace connections work with simple connect_namespace(impl_type, properties)
    let tmp_dir = tempdir().unwrap();
    let root_path = tmp_dir.path().to_str().unwrap().to_string();

    let mut properties = HashMap::new();
    properties.insert("root".to_string(), root_path);

    // This should succeed with directory-based namespace
    let result = connect_namespace("dir", properties).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_namespace_connection_with_properties() {
    // Test namespace connections work with properties passed directly
    let tmp_dir = tempdir().unwrap();
    let root_path = tmp_dir.path().to_str().unwrap().to_string();

    let mut properties = HashMap::new();
    properties.insert("root".to_string(), root_path);
    // Storage options can be passed as part of properties if needed by the namespace implementation
    properties.insert("timeout".to_string(), "30s".to_string());

    // This should succeed with directory-based namespace
    let result = connect_namespace("dir", properties).await;

    assert!(result.is_ok());
}
