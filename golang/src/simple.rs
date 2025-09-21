// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Simplified implementation for the Go SDK
//! This provides basic working functionality to get started

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::sync::{Arc, OnceLock};

use tokio::runtime::Runtime;
use lancedb::connect;
use lancedb::query::{QueryBase, ExecutableQuery};
use tokio_stream::StreamExt;
use arrow_ipc;

/// Result type for C interface
#[repr(C)]
pub struct SimpleResult {
    pub success: bool,
    pub error_message: *mut c_char,
}

impl SimpleResult {
    pub fn ok() -> Self {
        Self {
            success: true,
            error_message: ptr::null_mut(),
        }
    }

    pub fn error(msg: String) -> Self {
        let c_msg = CString::new(msg).unwrap_or_else(|_| CString::new("Invalid error message").unwrap());
        Self {
            success: false,
            error_message: c_msg.into_raw(),
        }
    }
}

/// Global runtime for async operations  
static SIMPLE_RUNTIME: OnceLock<Arc<Runtime>> = OnceLock::new();

fn get_simple_runtime() -> Arc<Runtime> {
    SIMPLE_RUNTIME.get_or_init(|| {
        let rt = Runtime::new().expect("Failed to create tokio runtime");
        Arc::new(rt)
    }).clone()
}

/// Convert C string to Rust string
fn from_c_str(s: *const c_char) -> Result<String, Box<dyn std::error::Error>> {
    if s.is_null() {
        return Err("Null pointer".into());
    }
    let c_str = unsafe { CStr::from_ptr(s) };
    Ok(c_str.to_str()?.to_string())
}

/// Initialize the simple LanceDB library
#[no_mangle]
pub extern "C" fn simple_lancedb_init() -> c_int {
    env_logger::try_init().ok();
    log::info!("Simple LanceDB Go bindings initialized");
    0
}

/// Connect to a LanceDB database (simple version)
#[no_mangle]
pub extern "C" fn simple_lancedb_connect(
    uri: *const c_char,
    handle: *mut *mut c_void,
) -> *mut SimpleResult {
    let result = std::panic::catch_unwind(|| -> SimpleResult {
        if uri.is_null() || handle.is_null() {
            return SimpleResult::error("Invalid null arguments".to_string());
        }

        let uri_str = match from_c_str(uri) {
            Ok(s) => s,
            Err(e) => return SimpleResult::error(format!("Invalid URI: {}", e)),
        };

        let rt = get_simple_runtime();
        
        match rt.block_on(async {
            connect(&uri_str).execute().await
        }) {
            Ok(conn) => {
                let boxed_conn = Box::new(conn);
                unsafe {
                    *handle = Box::into_raw(boxed_conn) as *mut c_void;
                }
                SimpleResult::ok()
            }
            Err(e) => SimpleResult::error(e.to_string()),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_connect".to_string()))),
    }
}

/// Connect to a database with storage options
#[no_mangle]
pub extern "C" fn simple_lancedb_connect_with_options(
    uri: *const c_char,
    options_json: *const c_char,
    handle: *mut *mut c_void,
) -> *mut SimpleResult {
    let result = std::panic::catch_unwind(|| -> SimpleResult {
        if uri.is_null() || options_json.is_null() || handle.is_null() {
            return SimpleResult::error("Invalid null arguments".to_string());
        }

        let uri_str = match from_c_str(uri) {
            Ok(s) => s,
            Err(e) => return SimpleResult::error(format!("Invalid URI: {}", e)),
        };

        let options_str = match from_c_str(options_json) {
            Ok(s) => s,
            Err(e) => return SimpleResult::error(format!("Invalid options JSON: {}", e)),
        };

        // Parse storage options from JSON
        let storage_options: serde_json::Value = match serde_json::from_str(&options_str) {
            Ok(opts) => opts,
            Err(e) => return SimpleResult::error(format!("Failed to parse storage options JSON: {}", e)),
        };

        let rt = get_simple_runtime();
        
        match rt.block_on(async {
            // For now, we'll handle S3 credentials via environment variables or AWS config
            // This is a simplified approach until LanceDB's API structure is clearer
            
            // Apply AWS credentials if provided
            if let Some(s3_config) = storage_options.get("s3_config") {
                apply_s3_environment_variables(s3_config);
            }
            
            // Create connection with URI (storage options applied via environment)
            connect(&uri_str).execute().await
        }) {
            Ok(conn) => {
                let boxed_conn = Box::new(conn);
                unsafe {
                    *handle = Box::into_raw(boxed_conn) as *mut c_void;
                }
                SimpleResult::ok()
            }
            Err(e) => SimpleResult::error(e.to_string()),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_connect_with_options".to_string()))),
    }
}

/// Apply AWS S3 configuration via environment variables
/// This is a simplified approach that works with most AWS SDK integrations
fn apply_s3_environment_variables(s3_config: &serde_json::Value) {
    use std::env;
    
    // Set AWS credentials via environment variables if provided
    if let Some(access_key) = s3_config.get("access_key_id").and_then(|v| v.as_str()) {
        env::set_var("AWS_ACCESS_KEY_ID", access_key);
    }
    
    if let Some(secret_key) = s3_config.get("secret_access_key").and_then(|v| v.as_str()) {
        env::set_var("AWS_SECRET_ACCESS_KEY", secret_key);
    }
    
    if let Some(session_token) = s3_config.get("session_token").and_then(|v| v.as_str()) {
        env::set_var("AWS_SESSION_TOKEN", session_token);
    }
    
    if let Some(region) = s3_config.get("region").and_then(|v| v.as_str()) {
        env::set_var("AWS_REGION", region);
        env::set_var("AWS_DEFAULT_REGION", region);
    }
    
    if let Some(profile) = s3_config.get("profile").and_then(|v| v.as_str()) {
        env::set_var("AWS_PROFILE", profile);
    }
    
    // Note: Other S3 options like custom endpoints, path style, etc. would need
    // to be supported by LanceDB's connection builder API directly.
    // For now, this provides basic AWS credential management.
}


/// Close a connection
#[no_mangle]
pub extern "C" fn simple_lancedb_close(handle: *mut c_void) -> *mut SimpleResult {
    if handle.is_null() {
        return Box::into_raw(Box::new(SimpleResult::error("Invalid null handle".to_string())));
    }

    let result = std::panic::catch_unwind(|| -> SimpleResult {
        unsafe {
            let _conn = Box::from_raw(handle as *mut lancedb::Connection);
            // Connection will be dropped here
        }
        SimpleResult::ok()
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_close".to_string()))),
    }
}

/// Get table names
#[no_mangle]
pub extern "C" fn simple_lancedb_table_names(
    handle: *mut c_void,
    names: *mut *mut *mut c_char,
    count: *mut c_int,
) -> *mut SimpleResult {
    let result = std::panic::catch_unwind(|| -> SimpleResult {
        if handle.is_null() || names.is_null() || count.is_null() {
            return SimpleResult::error("Invalid null arguments".to_string());
        }

        let conn = unsafe { &*(handle as *const lancedb::Connection) };
        let rt = get_simple_runtime();

        match rt.block_on(async { conn.table_names().execute().await }) {
            Ok(table_names) => {
                let len = table_names.len();
                unsafe {
                    *count = len as c_int;
                    
                    if len == 0 {
                        *names = ptr::null_mut();
                        return SimpleResult::ok();
                    }

                    // Allocate array of string pointers
                    let array = libc::malloc(len * std::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
                    if array.is_null() {
                        return SimpleResult::error("Failed to allocate memory".to_string());
                    }

                    // Convert each string and store pointer
                    for (i, name) in table_names.iter().enumerate() {
                        let c_name = CString::new(name.clone()).unwrap().into_raw();
                        *array.add(i) = c_name;
                    }
                    
                    *names = array;
                }
                SimpleResult::ok()
            }
            Err(e) => SimpleResult::error(e.to_string()),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_table_names".to_string()))),
    }
}

/// Free table names array
#[no_mangle]
pub extern "C" fn simple_lancedb_free_table_names(names: *mut *mut c_char, count: c_int) {
    if names.is_null() {
        return;
    }

    unsafe {
        for i in 0..count {
            let name_ptr = *names.add(i as usize);
            if !name_ptr.is_null() {
                let _ = CString::from_raw(name_ptr);
            }
        }
        libc::free(names as *mut c_void);
    }
}

/// Free a SimpleResult
#[no_mangle]
pub extern "C" fn simple_lancedb_result_free(result: *mut SimpleResult) {
    if result.is_null() {
        return;
    }
    unsafe {
        let result = Box::from_raw(result);
        if !result.error_message.is_null() {
            let _ = CString::from_raw(result.error_message);
        }
    }
}

/// Create a table with a simple JSON schema
#[no_mangle]
pub extern "C" fn simple_lancedb_create_table(
    handle: *mut c_void,
    table_name: *const c_char,
    schema_json: *const c_char,
) -> *mut SimpleResult {
    let result = std::panic::catch_unwind(|| -> SimpleResult {
        if handle.is_null() || table_name.is_null() || schema_json.is_null() {
            return SimpleResult::error("Invalid null arguments".to_string());
        }

        let name = match from_c_str(table_name) {
            Ok(s) => s,
            Err(e) => return SimpleResult::error(format!("Invalid table name: {}", e)),
        };

        let schema_str = match from_c_str(schema_json) {
            Ok(s) => s,
            Err(e) => return SimpleResult::error(format!("Invalid schema JSON: {}", e)),
        };

        let conn = unsafe { &*(handle as *const lancedb::Connection) };
        let rt = get_simple_runtime();

        // Parse the JSON schema and create an Arrow schema
        match serde_json::from_str::<serde_json::Value>(&schema_str) {
            Ok(schema_json_value) => {
                match create_arrow_schema_from_json(&schema_json_value) {
                    Ok(arrow_schema) => {
                        match rt.block_on(async {
                            use arrow_array::RecordBatchIterator;
                            let empty_batches = RecordBatchIterator::new(
                                vec![] as Vec<Result<arrow_array::RecordBatch, arrow_schema::ArrowError>>,
                                Arc::new(arrow_schema)
                            );
                            conn.create_table(&name, empty_batches)
                                .execute()
                                .await
                        }) {
                            Ok(_) => SimpleResult::ok(),
                            Err(e) => SimpleResult::error(format!("Failed to create table: {}", e)),
                        }
                    }
                    Err(e) => SimpleResult::error(format!("Failed to create Arrow schema: {}", e)),
                }
            }
            Err(e) => SimpleResult::error(format!("Failed to parse schema JSON: {}", e)),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_create_table".to_string()))),
    }
}

/// Create a table with Arrow IPC schema (more efficient than JSON)
#[no_mangle]
pub extern "C" fn simple_lancedb_create_table_with_ipc(
    handle: *mut c_void,
    table_name: *const c_char,
    schema_ipc: *const u8,
    schema_len: usize,
) -> *mut SimpleResult {
    let result = std::panic::catch_unwind(|| -> SimpleResult {
        if handle.is_null() || table_name.is_null() || schema_ipc.is_null() {
            return SimpleResult::error("Invalid null arguments".to_string());
        }

        let name = match from_c_str(table_name) {
            Ok(s) => s,
            Err(e) => return SimpleResult::error(format!("Invalid table name: {}", e)),
        };

        // Convert raw pointer to slice
        let schema_bytes = unsafe { 
            std::slice::from_raw_parts(schema_ipc, schema_len) 
        };

        let conn = unsafe { &*(handle as *const lancedb::Connection) };
        let rt = get_simple_runtime();

        // Deserialize Arrow schema directly from IPC bytes using FileReader
        let arrow_schema = match arrow_ipc::reader::FileReader::try_new(
            std::io::Cursor::new(schema_bytes), None
        ) {
            Ok(reader) => reader.schema(),
            Err(e) => return SimpleResult::error(format!("Invalid IPC schema: {}", e)),
        };

        match rt.block_on(async {
            use arrow_array::RecordBatchIterator;
            let empty_batches = RecordBatchIterator::new(
                vec![] as Vec<Result<arrow_array::RecordBatch, arrow_schema::ArrowError>>,
                arrow_schema  // arrow_schema is already Arc<Schema>
            );
            conn.create_table(&name, empty_batches)
                .execute()
                .await
        }) {
            Ok(_) => SimpleResult::ok(),
            Err(e) => SimpleResult::error(format!("Failed to create table: {}", e)),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_create_table_with_ipc".to_string()))),
    }
}

/// Helper function to create Arrow schema from JSON
fn create_arrow_schema_from_json(schema_json: &serde_json::Value) -> Result<arrow_schema::Schema, Box<dyn std::error::Error>> {
    use arrow_schema::{DataType, Field, Schema};
    
    let fields_array = schema_json.get("fields")
        .and_then(|f| f.as_array())
        .ok_or("Schema JSON must have 'fields' array")?;

    let mut fields = Vec::new();
    
    for field_json in fields_array {
        let name = field_json.get("name")
            .and_then(|n| n.as_str())
            .ok_or("Field must have 'name' string")?
            .to_string();
            
        let type_str = field_json.get("type")
            .and_then(|t| t.as_str())
            .ok_or("Field must have 'type' string")?;
            
        let nullable = field_json.get("nullable")
            .and_then(|n| n.as_bool())
            .unwrap_or(true);

        let data_type = match type_str {
            "int8" => DataType::Int8,
            "int16" => DataType::Int16,
            "int32" => DataType::Int32,
            "int64" => DataType::Int64,
            "float16" => DataType::Float16,
            "float32" => DataType::Float32,
            "float64" => DataType::Float64,
            "string" => DataType::Utf8,
            "binary" => DataType::Binary,
            "boolean" => DataType::Boolean,
            _ => {
                // Check for vector type
                if type_str.starts_with("fixed_size_list[int8;") {
                    let dimension_str = type_str.trim_start_matches("fixed_size_list[int8;")
                        .trim_end_matches(']');
                    let dimension: i32 = dimension_str.parse()
                        .map_err(|_| format!("Invalid vector dimension: {}", dimension_str))?;
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Int8, false)),
                        dimension,
                    )
                }
                else if type_str.starts_with("fixed_size_list[int16;") {
                    let dimension_str = type_str.trim_start_matches("fixed_size_list[int16;")
                        .trim_end_matches(']');
                    let dimension: i32 = dimension_str.parse()
                        .map_err(|_| format!("Invalid vector dimension: {}", dimension_str))?;
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Int16, false)),
                        dimension,
                    )
                }
                else if type_str.starts_with("fixed_size_list[int32;") {
                    let dimension_str = type_str.trim_start_matches("fixed_size_list[int32;")
                        .trim_end_matches(']');
                    let dimension: i32 = dimension_str.parse()
                        .map_err(|_| format!("Invalid vector dimension: {}", dimension_str))?;
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Int32, false)),
                        dimension,
                    )
                }
                else if type_str.starts_with("fixed_size_list[int64;") {
                    let dimension_str = type_str.trim_start_matches("fixed_size_list[int64;")
                        .trim_end_matches(']');
                    let dimension: i32 = dimension_str.parse()
                        .map_err(|_| format!("Invalid vector dimension: {}", dimension_str))?;
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Int64, false)),
                        dimension,
                    )
                }
                else if type_str.starts_with("fixed_size_list[float16;") {
                    let dimension_str = type_str.trim_start_matches("fixed_size_list[float16;")
                        .trim_end_matches(']');
                    let dimension: i32 = dimension_str.parse()
                        .map_err(|_| format!("Invalid vector dimension: {}", dimension_str))?;
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float16, false)),
                        dimension,
                    )
                } else if type_str.starts_with("fixed_size_list[float32;") {
                    let dimension_str = type_str.trim_start_matches("fixed_size_list[float32;")
                        .trim_end_matches(']');
                    let dimension: i32 = dimension_str.parse()
                        .map_err(|_| format!("Invalid vector dimension: {}", dimension_str))?;
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, false)),
                        dimension,
                    )
                }
                else if type_str.starts_with("fixed_size_list[float64;") {
                    let dimension_str = type_str.trim_start_matches("fixed_size_list[float64;")
                        .trim_end_matches(']');
                    let dimension: i32 = dimension_str.parse()
                        .map_err(|_| format!("Invalid vector dimension: {}", dimension_str))?;
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float64, false)),
                        dimension,
                    )
                }
                else {
                    return Err(format!("Unsupported data type: {}", type_str).into());
                }
            }
        };

        fields.push(Field::new(name, data_type, nullable));
    }

    Ok(Schema::new(fields))
}

/// Drop a table from the database (simple version)
#[no_mangle]
pub extern "C" fn simple_lancedb_drop_table(
    handle: *mut c_void,
    table_name: *const c_char,
) -> *mut SimpleResult {
    let result = std::panic::catch_unwind(|| -> SimpleResult {
        if handle.is_null() || table_name.is_null() {
            return SimpleResult::error("Invalid null arguments".to_string());
        }

        let name = match from_c_str(table_name) {
            Ok(s) => s,
            Err(e) => return SimpleResult::error(format!("Invalid table name: {}", e)),
        };

        let conn = unsafe { &*(handle as *const lancedb::Connection) };
        let rt = get_simple_runtime();

        match rt.block_on(async { conn.drop_table(&name, &[]).await }) {
            Ok(_) => SimpleResult::ok(),
            Err(e) => SimpleResult::error(format!("Failed to drop table: {}", e)),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_drop_table".to_string()))),
    }
}

/// Open a table from the database (simple version)
#[no_mangle]
pub extern "C" fn simple_lancedb_open_table(
    handle: *mut c_void,
    table_name: *const c_char,
    table_handle: *mut *mut c_void,
) -> *mut SimpleResult {
    let result = std::panic::catch_unwind(|| -> SimpleResult {
        if handle.is_null() || table_name.is_null() || table_handle.is_null() {
            return SimpleResult::error("Invalid null arguments".to_string());
        }

        let name = match from_c_str(table_name) {
            Ok(s) => s,
            Err(e) => return SimpleResult::error(format!("Invalid table name: {}", e)),
        };

        let conn = unsafe { &*(handle as *const lancedb::Connection) };
        let rt = get_simple_runtime();

        match rt.block_on(async { conn.open_table(&name).execute().await }) {
            Ok(table) => {
                let boxed_table = Box::new(table);
                unsafe {
                    *table_handle = Box::into_raw(boxed_table) as *mut c_void;
                }
                SimpleResult::ok()
            }
            Err(e) => SimpleResult::error(format!("Failed to open table: {}", e)),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_open_table".to_string()))),
    }
}

/// Count rows in a table (simple version)
#[no_mangle]
pub extern "C" fn simple_lancedb_table_count_rows(
    table_handle: *mut c_void,
    count: *mut i64,
) -> *mut SimpleResult {
    let result = std::panic::catch_unwind(|| -> SimpleResult {
        if table_handle.is_null() || count.is_null() {
            return SimpleResult::error("Invalid null arguments".to_string());
        }

        let table = unsafe { &*(table_handle as *const lancedb::Table) };
        let rt = get_simple_runtime();

        match rt.block_on(async { table.count_rows(None).await }) {
            Ok(row_count) => {
                unsafe {
                    *count = row_count as i64;
                }
                SimpleResult::ok()
            }
            Err(e) => SimpleResult::error(format!("Failed to count rows: {}", e)),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_table_count_rows".to_string()))),
    }
}

/// Get table version (simple version)
#[no_mangle]
pub extern "C" fn simple_lancedb_table_version(
    table_handle: *mut c_void,
    version: *mut i64,
) -> *mut SimpleResult {
    let result = std::panic::catch_unwind(|| -> SimpleResult {
        if table_handle.is_null() || version.is_null() {
            return SimpleResult::error("Invalid null arguments".to_string());
        }

        let table = unsafe { &*(table_handle as *const lancedb::Table) };
        let rt = get_simple_runtime();

        match rt.block_on(async { table.version().await }) {
            Ok(table_version) => {
                unsafe {
                    *version = table_version as i64;
                }
                SimpleResult::ok()
            }
            Err(e) => SimpleResult::error(format!("Failed to get table version: {}", e)),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_table_version".to_string()))),
    }
}

/// Get table schema as JSON (simple version)
#[no_mangle]
pub extern "C" fn simple_lancedb_table_schema(
    table_handle: *mut c_void,
    schema_json: *mut *mut c_char,
) -> *mut SimpleResult {
    let result = std::panic::catch_unwind(|| -> SimpleResult {
        if table_handle.is_null() || schema_json.is_null() {
            return SimpleResult::error("Invalid null arguments".to_string());
        }

        let table = unsafe { &*(table_handle as *const lancedb::Table) };
        let rt = get_simple_runtime();

        match rt.block_on(async { table.schema().await }) {
            Ok(arrow_schema) => {
                // Convert Arrow schema to JSON
                let fields: Vec<serde_json::Value> = arrow_schema.fields()
                    .iter()
                    .map(|field| {
                        let type_str = match field.data_type() {
                            arrow_schema::DataType::Int32 => "int32",
                            arrow_schema::DataType::Int64 => "int64",
                            arrow_schema::DataType::Float32 => "float32",
                            arrow_schema::DataType::Float64 => "float64",
                            arrow_schema::DataType::Utf8 => "string",
                            arrow_schema::DataType::Binary => "binary",
                            arrow_schema::DataType::Boolean => "boolean",
                            arrow_schema::DataType::FixedSizeList(inner, size) => {
                                if matches!(inner.data_type(), arrow_schema::DataType::Float32) {
                                    return serde_json::json!({
                                        "name": field.name(),
                                        "type": format!("fixed_size_list[float32;{}]", size),
                                        "nullable": field.is_nullable()
                                    });
                                } else {
                                    "unknown"
                                }
                            },
                            _ => "unknown"
                        };
                        
                        serde_json::json!({
                            "name": field.name(),
                            "type": type_str,
                            "nullable": field.is_nullable()
                        })
                    })
                    .collect();

                let schema_json_obj = serde_json::json!({
                    "fields": fields
                });

                match serde_json::to_string(&schema_json_obj) {
                    Ok(json_str) => {
                        let c_str = CString::new(json_str).unwrap();
                        unsafe {
                            *schema_json = c_str.into_raw();
                        }
                        SimpleResult::ok()
                    }
                    Err(e) => SimpleResult::error(format!("Failed to serialize schema: {}", e)),
                }
            }
            Err(e) => SimpleResult::error(format!("Failed to get table schema: {}", e)),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_table_schema".to_string()))),
    }
}

/// Close a table handle (simple version)
#[no_mangle]
pub extern "C" fn simple_lancedb_table_close(table_handle: *mut c_void) -> *mut SimpleResult {
    if table_handle.is_null() {
        return Box::into_raw(Box::new(SimpleResult::error("Invalid null handle".to_string())));
    }

    let result = std::panic::catch_unwind(|| -> SimpleResult {
        unsafe {
            let _table = Box::from_raw(table_handle as *mut lancedb::Table);
            // Table will be dropped here, cleaning up resources
        }
        SimpleResult::ok()
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_table_close".to_string()))),
    }
}

/// Delete rows from a table using SQL predicate (simple version)
#[no_mangle]
pub extern "C" fn simple_lancedb_table_delete(
    table_handle: *mut c_void,
    predicate: *const c_char,
    deleted_count: *mut i64,
) -> *mut SimpleResult {
    let result = std::panic::catch_unwind(|| -> SimpleResult {
        if table_handle.is_null() || predicate.is_null() || deleted_count.is_null() {
            return SimpleResult::error("Invalid null arguments".to_string());
        }

        let predicate_str = match from_c_str(predicate) {
            Ok(s) => s,
            Err(e) => return SimpleResult::error(format!("Invalid predicate: {}", e)),
        };

        let table = unsafe { &*(table_handle as *const lancedb::Table) };
        let rt = get_simple_runtime();

        match rt.block_on(async { table.delete(&predicate_str).await }) {
            Ok(_delete_result) => {
                // Note: LanceDB's DeleteResult doesn't expose the number of deleted rows
                // We set this to -1 to indicate successful deletion but unknown count
                unsafe {
                    *deleted_count = -1;
                }
                SimpleResult::ok()
            }
            Err(e) => SimpleResult::error(format!("Failed to delete rows: {}", e)),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_table_delete".to_string()))),
    }
}

/// Update rows in a table using SQL predicate and column updates (simple version)
#[no_mangle]
pub extern "C" fn simple_lancedb_table_update(
    table_handle: *mut c_void,
    predicate: *const c_char,
    updates_json: *const c_char,
) -> *mut SimpleResult {
    let result = std::panic::catch_unwind(|| -> SimpleResult {
        if table_handle.is_null() || predicate.is_null() || updates_json.is_null() {
            return SimpleResult::error("Invalid null arguments".to_string());
        }

        let predicate_str = match from_c_str(predicate) {
            Ok(s) => s,
            Err(e) => return SimpleResult::error(format!("Invalid predicate: {}", e)),
        };

        let updates_str = match from_c_str(updates_json) {
            Ok(s) => s,
            Err(e) => return SimpleResult::error(format!("Invalid updates JSON: {}", e)),
        };

        // Parse updates JSON into a map
        let updates: std::collections::HashMap<String, serde_json::Value> = match serde_json::from_str(&updates_str) {
            Ok(u) => u,
            Err(e) => return SimpleResult::error(format!("Failed to parse updates JSON: {}", e)),
        };

        let table = unsafe { &*(table_handle as *const lancedb::Table) };
        let rt = get_simple_runtime();

        // Validate all update values first
        for (column, value) in updates.iter() {
            match value {
                serde_json::Value::String(_) | serde_json::Value::Number(_) | 
                serde_json::Value::Bool(_) | serde_json::Value::Null => {},
                _ => return SimpleResult::error(format!("Unsupported update value type for column {}", column)),
            }
        }

        match rt.block_on(async { 
            let mut update_builder = table.update().only_if(&predicate_str);
            
            // Add each column update separately
            for (column, value) in updates.iter() {
                let value_str = match value {
                    serde_json::Value::String(s) => format!("'{}'", s), // String values need quotes
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Null => "NULL".to_string(),
                    _ => unreachable!(), // Already validated above
                };
                update_builder = update_builder.column(column, &value_str);
            }
            
            update_builder.execute().await 
        }) {
            Ok(_update_result) => {
                SimpleResult::ok()
            }
            Err(e) => SimpleResult::error(format!("Failed to update rows: {}", e)),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_table_update".to_string()))),
    }
}

/// Add JSON data to a table (simple version)
/// Converts JSON array of objects to Arrow RecordBatch and adds to table
#[no_mangle]
pub extern "C" fn simple_lancedb_table_add_json(
    table_handle: *mut c_void,
    json_data: *const c_char,
    added_count: *mut i64,
) -> *mut SimpleResult {
    let result = std::panic::catch_unwind(|| -> SimpleResult {
        if table_handle.is_null() || json_data.is_null() || added_count.is_null() {
            return SimpleResult::error("Invalid null arguments".to_string());
        }

        let json_str = match from_c_str(json_data) {
            Ok(s) => s,
            Err(e) => return SimpleResult::error(format!("Invalid JSON data: {}", e)),
        };

        let table = unsafe { &*(table_handle as *const lancedb::Table) };
        let rt = get_simple_runtime();

        // Parse JSON array
        let json_values: Vec<serde_json::Value> = match serde_json::from_str(&json_str) {
            Ok(serde_json::Value::Array(arr)) => arr,
            Ok(single_value) => vec![single_value], // Convert single object to array
            Err(e) => return SimpleResult::error(format!("Failed to parse JSON: {}", e)),
        };

        if json_values.is_empty() {
            unsafe { *added_count = 0; }
            return SimpleResult::ok();
        }

        // Get table schema
        let table_schema = match rt.block_on(async { table.schema().await }) {
            Ok(schema) => schema,
            Err(e) => return SimpleResult::error(format!("Failed to get table schema: {}", e)),
        };

        // Convert JSON to RecordBatch
        match json_to_record_batch(&json_values, &table_schema) {
            Ok(record_batch) => {
                // Add the record batch to the table
                match rt.block_on(async {
                    use arrow_array::RecordBatchIterator;
                    let batches = vec![Ok(record_batch.clone())];
                    let batch_iter = RecordBatchIterator::new(batches, record_batch.schema());
                    table.add(batch_iter).execute().await
                }) {
                    Ok(_) => {
                        unsafe { *added_count = record_batch.num_rows() as i64; }
                        SimpleResult::ok()
                    }
                    Err(e) => SimpleResult::error(format!("Failed to add data to table: {}", e)),
                }
            }
            Err(e) => SimpleResult::error(format!("Failed to convert JSON to RecordBatch: {}", e)),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_table_add_json".to_string()))),
    }
}

/// Convert JSON values to Arrow RecordBatch
fn json_to_record_batch(
    json_values: &[serde_json::Value],
    schema: &arrow_schema::Schema,
) -> Result<arrow_array::RecordBatch, String> {
    use arrow_array::{ArrayRef, Int32Array, Int64Array, Float32Array, Float64Array, 
                     BooleanArray, StringArray, FixedSizeListArray};
    use arrow_schema::DataType;
    use std::sync::Arc;

    let mut columns: Vec<ArrayRef> = Vec::new();

    for field in schema.fields() {
        let field_name = field.name();
        let data_type = field.data_type();

        match data_type {
            DataType::Int32 => {
                let values: Result<Vec<Option<i32>>, String> = json_values.iter().map(|obj| {
                    match obj.get(field_name) {
                        Some(serde_json::Value::Number(n)) => {
                            if let Some(i) = n.as_i64() {
                                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                                    Ok(Some(i as i32))
                                } else {
                                    Err(format!("Number {} out of range for i32 in field {}", i, field_name))
                                }
                            } else {
                                Err(format!("Invalid number format in field {}", field_name))
                            }
                        }
                        Some(serde_json::Value::Null) if field.is_nullable() => Ok(None),
                        None if field.is_nullable() => Ok(None),
                        Some(_) => Err(format!("Expected number for field {} but got different type", field_name)),
                        None => Err(format!("Missing required field {}", field_name)),
                    }
                }).collect();
                
                let array = Int32Array::from(values?);
                columns.push(Arc::new(array) as ArrayRef);
            }
            DataType::Int64 => {
                let values: Result<Vec<Option<i64>>, String> = json_values.iter().map(|obj| {
                    match obj.get(field_name) {
                        Some(serde_json::Value::Number(n)) => {
                            if let Some(i) = n.as_i64() {
                                Ok(Some(i))
                            } else {
                                Err(format!("Invalid number format in field {}", field_name))
                            }
                        }
                        Some(serde_json::Value::Null) if field.is_nullable() => Ok(None),
                        None if field.is_nullable() => Ok(None),
                        Some(_) => Err(format!("Expected number for field {} but got different type", field_name)),
                        None => Err(format!("Missing required field {}", field_name)),
                    }
                }).collect();
                
                let array = Int64Array::from(values?);
                columns.push(Arc::new(array) as ArrayRef);
            }
            DataType::Float32 => {
                let values: Result<Vec<Option<f32>>, String> = json_values.iter().map(|obj| {
                    match obj.get(field_name) {
                        Some(serde_json::Value::Number(n)) => {
                            if let Some(f) = n.as_f64() {
                                Ok(Some(f as f32))
                            } else {
                                Err(format!("Invalid number format in field {}", field_name))
                            }
                        }
                        Some(serde_json::Value::Null) if field.is_nullable() => Ok(None),
                        None if field.is_nullable() => Ok(None),
                        Some(_) => Err(format!("Expected number for field {} but got different type", field_name)),
                        None => Err(format!("Missing required field {}", field_name)),
                    }
                }).collect();
                
                let array = Float32Array::from(values?);
                columns.push(Arc::new(array) as ArrayRef);
            }
            DataType::Float64 => {
                let values: Result<Vec<Option<f64>>, String> = json_values.iter().map(|obj| {
                    match obj.get(field_name) {
                        Some(serde_json::Value::Number(n)) => {
                            if let Some(f) = n.as_f64() {
                                Ok(Some(f))
                            } else {
                                Err(format!("Invalid number format in field {}", field_name))
                            }
                        }
                        Some(serde_json::Value::Null) if field.is_nullable() => Ok(None),
                        None if field.is_nullable() => Ok(None),
                        Some(_) => Err(format!("Expected number for field {} but got different type", field_name)),
                        None => Err(format!("Missing required field {}", field_name)),
                    }
                }).collect();
                
                let array = Float64Array::from(values?);
                columns.push(Arc::new(array) as ArrayRef);
            }
            DataType::Boolean => {
                let values: Result<Vec<Option<bool>>, String> = json_values.iter().map(|obj| {
                    match obj.get(field_name) {
                        Some(serde_json::Value::Bool(b)) => Ok(Some(*b)),
                        Some(serde_json::Value::Null) if field.is_nullable() => Ok(None),
                        None if field.is_nullable() => Ok(None),
                        Some(_) => Err(format!("Expected boolean for field {} but got different type", field_name)),
                        None => Err(format!("Missing required field {}", field_name)),
                    }
                }).collect();
                
                let array = BooleanArray::from(values?);
                columns.push(Arc::new(array) as ArrayRef);
            }
            DataType::Utf8 => {
                let values: Result<Vec<Option<String>>, String> = json_values.iter().map(|obj| {
                    match obj.get(field_name) {
                        Some(serde_json::Value::String(s)) => Ok(Some(s.clone())),
                        Some(serde_json::Value::Null) if field.is_nullable() => Ok(None),
                        None if field.is_nullable() => Ok(None),
                        Some(_) => Err(format!("Expected string for field {} but got different type", field_name)),
                        None => Err(format!("Missing required field {}", field_name)),
                    }
                }).collect();
                
                let array = StringArray::from(values?);
                columns.push(Arc::new(array) as ArrayRef);
            }
            DataType::FixedSizeList(inner_field, list_size) if matches!(inner_field.data_type(), DataType::Float32) => {
                // Handle vector fields (FixedSizeList of Float32)
                let values: Result<Vec<Option<Vec<f32>>>, String> = json_values.iter().map(|obj| {
                    match obj.get(field_name) {
                        Some(serde_json::Value::Array(arr)) => {
                            if arr.len() != *list_size as usize {
                                return Err(format!("Vector field {} expects {} elements but got {}", 
                                                 field_name, list_size, arr.len()));
                            }
                            let vec_values: Result<Vec<f32>, String> = arr.iter().map(|v| {
                                match v.as_f64() {
                                    Some(f) => Ok(f as f32),
                                    None => Err(format!("Invalid vector element in field {}", field_name)),
                                }
                            }).collect();
                            Ok(Some(vec_values?))
                        }
                        Some(serde_json::Value::Null) if field.is_nullable() => Ok(None),
                        None if field.is_nullable() => Ok(None),
                        Some(_) => Err(format!("Expected array for vector field {} but got different type", field_name)),
                        None => Err(format!("Missing required field {}", field_name)),
                    }
                }).collect();
                
                let flat_values: Vec<Option<f32>> = values?.into_iter().flat_map(|opt_vec| {
                    match opt_vec {
                        Some(vec) => vec.into_iter().map(Some).collect::<Vec<_>>(),
                        None => (0..*list_size).map(|_| None).collect::<Vec<_>>(),
                    }
                }).collect();
                
                let float_array = Float32Array::from(flat_values);
                let list_array = FixedSizeListArray::new(
                    inner_field.clone(),
                    *list_size,
                    Arc::new(float_array),
                    None, // No null buffer for now - simplified
                );
                columns.push(Arc::new(list_array) as ArrayRef);
            }
            _ => return Err(format!("Unsupported data type: {:?}", data_type)),
        }
    }

    arrow_array::RecordBatch::try_new(Arc::new(schema.clone()), columns)
        .map_err(|e| format!("Failed to create RecordBatch: {}", e))
}

/// Create an index on the specified columns
#[no_mangle]
pub extern "C" fn simple_lancedb_table_create_index(
    table_handle: *mut c_void,
    columns_json: *const c_char,
    index_type: *const c_char,
    index_name: *const c_char,
) -> *mut SimpleResult {
    let result = std::panic::catch_unwind(|| -> SimpleResult {
        if table_handle.is_null() || columns_json.is_null() || index_type.is_null() {
            return SimpleResult::error("Invalid null arguments".to_string());
        }

        let columns_str = match from_c_str(columns_json) {
            Ok(s) => s,
            Err(e) => return SimpleResult::error(format!("Invalid columns JSON: {}", e)),
        };

        let index_type_str = match from_c_str(index_type) {
            Ok(s) => s,
            Err(e) => return SimpleResult::error(format!("Invalid index type: {}", e)),
        };

        let index_name_str = if index_name.is_null() {
            None
        } else {
            match from_c_str(index_name) {
                Ok(s) => Some(s),
                Err(e) => return SimpleResult::error(format!("Invalid index name: {}", e)),
            }
        };

        // Parse columns JSON
        let columns: Vec<String> = match serde_json::from_str(&columns_str) {
            Ok(cols) => cols,
            Err(e) => return SimpleResult::error(format!("Failed to parse columns JSON: {}", e)),
        };

        let table = unsafe { &*(table_handle as *const lancedb::Table) };
        let rt = get_simple_runtime();

        // Map index type string to LanceDB index type
        let index_result = match index_type_str.as_str() {
            "vector" | "ivf_pq" => {
                // Create vector index (IVF_PQ)
                rt.block_on(async {
                    let mut index_builder = table.create_index(&columns, lancedb::index::Index::IvfPq(
                        lancedb::index::vector::IvfPqIndexBuilder::default()
                    ));
                    
                    if let Some(name) = index_name_str {
                        index_builder = index_builder.name(name);
                    }
                    
                    index_builder.execute().await
                })
            }
            "ivf_flat" => {
                rt.block_on(async {
                    let mut index_builder = table.create_index(&columns, lancedb::index::Index::IvfFlat(
                        lancedb::index::vector::IvfFlatIndexBuilder::default()
                    ));
                    
                    if let Some(name) = index_name_str {
                        index_builder = index_builder.name(name);
                    }
                    
                    index_builder.execute().await
                })
            }
            "hnsw_pq" => {
                rt.block_on(async {
                    let mut index_builder = table.create_index(&columns, lancedb::index::Index::IvfHnswPq(
                        lancedb::index::vector::IvfHnswPqIndexBuilder::default()
                    ));
                    
                    if let Some(name) = index_name_str {
                        index_builder = index_builder.name(name);
                    }
                    
                    index_builder.execute().await
                })
            }
            "hnsw_sq" => {
                rt.block_on(async {
                    let mut index_builder = table.create_index(&columns, lancedb::index::Index::IvfHnswSq(
                        lancedb::index::vector::IvfHnswSqIndexBuilder::default()
                    ));
                    
                    if let Some(name) = index_name_str {
                        index_builder = index_builder.name(name);
                    }
                    
                    index_builder.execute().await
                })
            }
            "btree" => {
                rt.block_on(async {
                    let mut index_builder = table.create_index(&columns, lancedb::index::Index::BTree(
                        lancedb::index::scalar::BTreeIndexBuilder {}
                    ));
                    
                    if let Some(name) = index_name_str {
                        index_builder = index_builder.name(name);
                    }
                    
                    index_builder.execute().await
                })
            }
            "bitmap" => {
                rt.block_on(async {
                    let mut index_builder = table.create_index(&columns, lancedb::index::Index::Bitmap(
                        lancedb::index::scalar::BitmapIndexBuilder {}
                    ));
                    
                    if let Some(name) = index_name_str {
                        index_builder = index_builder.name(name);
                    }
                    
                    index_builder.execute().await
                })
            }
            "label_list" => {
                rt.block_on(async {
                    let mut index_builder = table.create_index(&columns, lancedb::index::Index::LabelList(
                        lancedb::index::scalar::LabelListIndexBuilder {}
                    ));
                    
                    if let Some(name) = index_name_str {
                        index_builder = index_builder.name(name);
                    }
                    
                    index_builder.execute().await
                })
            }
            "fts" => {
                rt.block_on(async {
                    let mut index_builder = table.create_index(&columns, lancedb::index::Index::FTS(
                        lancedb::index::scalar::FtsIndexBuilder::default()
                    ));
                    
                    if let Some(name) = index_name_str {
                        index_builder = index_builder.name(name);
                    }
                    
                    index_builder.execute().await
                })
            }
            _ => return SimpleResult::error(format!("Unsupported index type: {}", index_type_str)),
        };

        match index_result {
            Ok(_) => SimpleResult::ok(),
            Err(e) => SimpleResult::error(format!("Failed to create index: {}", e)),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_table_create_index".to_string()))),
    }
}

/// Get all indexes for a table (returns JSON string)
#[no_mangle]
pub extern "C" fn simple_lancedb_table_get_indexes(
    table_handle: *mut c_void,
    indexes_json: *mut *mut c_char,
) -> *mut SimpleResult {
    let result = std::panic::catch_unwind(|| -> SimpleResult {
        if table_handle.is_null() || indexes_json.is_null() {
            return SimpleResult::error("Invalid null arguments".to_string());
        }

        let table = unsafe { &*(table_handle as *const lancedb::Table) };
        let rt = get_simple_runtime();

        match rt.block_on(async { table.list_indices().await }) {
            Ok(indexes) => {
                // Convert the indexes to a JSON-serializable format
                let mut index_info_list = Vec::new();
                
                for index in indexes {
                    let index_info = serde_json::json!({
                        "name": index.name,
                        "columns": index.columns,
                        "index_type": format!("{:?}", index.index_type),
                    });
                    index_info_list.push(index_info);
                }

                match serde_json::to_string(&index_info_list) {
                    Ok(json_str) => {
                        match CString::new(json_str) {
                            Ok(c_string) => {
                                unsafe {
                                    *indexes_json = c_string.into_raw();
                                }
                                SimpleResult::ok()
                            }
                            Err(_) => SimpleResult::error("Failed to convert JSON to C string".to_string()),
                        }
                    }
                    Err(e) => SimpleResult::error(format!("Failed to serialize indexes to JSON: {}", e)),
                }
            }
            Err(e) => SimpleResult::error(format!("Failed to list indexes: {}", e)),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_table_get_indexes".to_string()))),
    }
}

/// Execute a select query with various predicates (vector search, filters, etc.)
#[no_mangle]
pub extern "C" fn simple_lancedb_table_select_query(
    table_handle: *mut c_void,
    query_config_json: *const c_char,
    result_json: *mut *mut c_char,
) -> *mut SimpleResult {
    let result = std::panic::catch_unwind(|| -> SimpleResult {
        if table_handle.is_null() || query_config_json.is_null() || result_json.is_null() {
            return SimpleResult::error("Invalid null arguments".to_string());
        }

        let config_str = match from_c_str(query_config_json) {
            Ok(s) => s,
            Err(e) => return SimpleResult::error(format!("Invalid query config JSON: {}", e)),
        };

        let table = unsafe { &*(table_handle as *const lancedb::Table) };
        let rt = get_simple_runtime();

        // Parse query configuration
        let query_config: serde_json::Value = match serde_json::from_str(&config_str) {
            Ok(config) => config,
            Err(e) => return SimpleResult::error(format!("Failed to parse query config: {}", e)),
        };

        // Execute query based on configuration
        match rt.block_on(async {
            // Check if this is a vector search query first, as it needs special handling
            if let Some(vector_search) = query_config.get("vector_search") {
                if let (Some(column), Some(vector_values), Some(k)) = (
                    vector_search.get("column").and_then(|v| v.as_str()),
                    vector_search.get("vector").and_then(|v| v.as_array()),
                    vector_search.get("k").and_then(|v| v.as_u64()),
                ) {
                    // Convert JSON array to Vec<f32>
                    let vector: Result<Vec<f32>, String> = vector_values
                        .iter()
                        .map(|v| {
                            v.as_f64()
                                .map(|f| f as f32)
                                .ok_or_else(|| "Invalid vector element".to_string())
                        })
                        .collect();

                    match vector {
                        Ok(vec) => {
                            // Use the limit from query config, or k if not specified
                            let effective_limit = query_config.get("limit")
                                .and_then(|v| v.as_u64())
                                .map(|l| l as usize)
                                .unwrap_or(k as usize);
                            
                            let mut vector_query = table.query().nearest_to(vec)?.column(column).limit(effective_limit);
                            
                            // Apply WHERE filter for vector queries
                            if let Some(filter) = query_config.get("where").and_then(|v| v.as_str()) {
                                vector_query = vector_query.only_if(filter);
                            }
                            
                            // Apply column selection for vector queries
                            if let Some(columns) = query_config.get("columns").and_then(|v| v.as_array()) {
                                let column_names: Vec<String> = columns
                                    .iter()
                                    .filter_map(|v| v.as_str())
                                    .map(|s| s.to_string())
                                    .collect();
                                if !column_names.is_empty() {
                                    vector_query = vector_query.select(lancedb::query::Select::Columns(column_names));
                                }
                            }
                            
                            return vector_query.execute().await;
                        }
                        Err(e) => return Err(lancedb::Error::InvalidInput {
                            message: format!("Failed to parse vector: {}", e),
                        }),
                    }
                }
            }

            // Apply full-text search
            if let Some(fts_search) = query_config.get("fts_search") {
                if let (Some(_column), Some(_query_text)) = (
                    fts_search.get("column").and_then(|v| v.as_str()),
                    fts_search.get("query").and_then(|v| v.as_str()),
                ) {
                    // Note: FTS search is not currently available in this API version
                    // This is a placeholder for future implementation
                    return Err(lancedb::Error::InvalidInput {
                        message: "Full-text search is not currently supported".to_string(),
                    });
                }
            }

            // For non-vector queries, use regular query
            let mut query = table.query();

            // Apply column selection
            if let Some(columns) = query_config.get("columns").and_then(|v| v.as_array()) {
                let column_names: Vec<String> = columns
                    .iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| s.to_string())
                    .collect();
                if !column_names.is_empty() {
                    query = query.select(lancedb::query::Select::Columns(column_names));
                }
            }

            // Apply limit
            if let Some(limit) = query_config.get("limit").and_then(|v| v.as_u64()) {
                query = query.limit(limit as usize);
            }

            // Apply offset
            if let Some(offset) = query_config.get("offset").and_then(|v| v.as_u64()) {
                query = query.offset(offset as usize);
            }

            // Apply WHERE filter
            if let Some(filter) = query_config.get("where").and_then(|v| v.as_str()) {
                query = query.only_if(filter);
            }

            // Execute the query
            query.execute().await
        }) {
            Ok(record_batch_reader) => {
                // Convert RecordBatch results to JSON
                let mut results = Vec::new();
                
                // Note: This is a simplified approach. In a real implementation,
                // you might want to stream results or handle large datasets differently.
                match rt.block_on(async {
                    let mut stream = record_batch_reader;
                    while let Some(batch_result) = stream.next().await {
                        match batch_result {
                            Ok(batch) => {
                                // Convert RecordBatch to JSON
                                for row_idx in 0..batch.num_rows() {
                                    let mut row = serde_json::Map::new();
                                    let schema = batch.schema();
                                    
                                    for (col_idx, field) in schema.fields().iter().enumerate() {
                                        let column = batch.column(col_idx);
                                        let field_name = field.name();
                                        
                                        // Convert Arrow array value to JSON value
                                        let json_value = match convert_arrow_value_to_json(column, row_idx) {
                                            Ok(v) => v,
                                            Err(_) => serde_json::Value::Null,
                                        };
                                        
                                        row.insert(field_name.clone(), json_value);
                                    }
                                    results.push(serde_json::Value::Object(row));
                                }
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    Ok(())
                }) {
                    Ok(()) => {
                        // Serialize results to JSON
                        match serde_json::to_string(&results) {
                            Ok(json_str) => {
                                match CString::new(json_str) {
                                    Ok(c_string) => {
                                        unsafe {
                                            *result_json = c_string.into_raw();
                                        }
                                        SimpleResult::ok()
                                    }
                                    Err(_) => SimpleResult::error("Failed to convert results to C string".to_string()),
                                }
                            }
                            Err(e) => SimpleResult::error(format!("Failed to serialize results to JSON: {}", e)),
                        }
                    }
                    Err(e) => SimpleResult::error(format!("Failed to process query results: {}", e)),
                }
            }
            Err(e) => SimpleResult::error(format!("Failed to execute query: {}", e)),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(SimpleResult::error("Panic in simple_lancedb_table_select_query".to_string()))),
    }
}

/// Helper function to convert Arrow array value to JSON
fn convert_arrow_value_to_json(array: &dyn arrow_array::Array, row_idx: usize) -> Result<serde_json::Value, String> {
    use arrow_schema::DataType;
    
    if array.is_null(row_idx) {
        return Ok(serde_json::Value::Null);
    }
    
    match array.data_type() {
        DataType::Int32 => {
            let typed_array = array.as_any().downcast_ref::<arrow_array::Int32Array>()
                .ok_or("Failed to downcast to Int32Array")?;
            Ok(serde_json::Value::Number(serde_json::Number::from(typed_array.value(row_idx))))
        }
        DataType::Int64 => {
            let typed_array = array.as_any().downcast_ref::<arrow_array::Int64Array>()
                .ok_or("Failed to downcast to Int64Array")?;
            Ok(serde_json::Value::Number(serde_json::Number::from(typed_array.value(row_idx))))
        }
        DataType::Float32 => {
            let typed_array = array.as_any().downcast_ref::<arrow_array::Float32Array>()
                .ok_or("Failed to downcast to Float32Array")?;
            Ok(serde_json::json!(typed_array.value(row_idx)))
        }
        DataType::Float64 => {
            let typed_array = array.as_any().downcast_ref::<arrow_array::Float64Array>()
                .ok_or("Failed to downcast to Float64Array")?;
            Ok(serde_json::json!(typed_array.value(row_idx)))
        }
        DataType::Boolean => {
            let typed_array = array.as_any().downcast_ref::<arrow_array::BooleanArray>()
                .ok_or("Failed to downcast to BooleanArray")?;
            Ok(serde_json::Value::Bool(typed_array.value(row_idx)))
        }
        DataType::Utf8 => {
            let typed_array = array.as_any().downcast_ref::<arrow_array::StringArray>()
                .ok_or("Failed to downcast to StringArray")?;
            Ok(serde_json::Value::String(typed_array.value(row_idx).to_string()))
        }
        DataType::FixedSizeList(_, list_size) => {
            let typed_array = array.as_any().downcast_ref::<arrow_array::FixedSizeListArray>()
                .ok_or("Failed to downcast to FixedSizeListArray")?;
            let values_array = typed_array.values();
            
            let start_idx = row_idx * (*list_size as usize);
            let end_idx = start_idx + (*list_size as usize);
            
            let mut list_values = Vec::new();
            for i in start_idx..end_idx {
                match convert_arrow_value_to_json(values_array.as_ref(), i) {
                    Ok(val) => list_values.push(val),
                    Err(_) => list_values.push(serde_json::Value::Null),
                }
            }
            Ok(serde_json::Value::Array(list_values))
        }
        _ => Ok(serde_json::Value::String(format!("Unsupported type: {:?}", array.data_type()))),
    }
}

/// Free a C string allocated by the library
#[no_mangle]
pub extern "C" fn simple_lancedb_free_string(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    unsafe {
        let _ = CString::from_raw(s);
    }
}
