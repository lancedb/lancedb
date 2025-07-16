use jni::objects::{JObject, JString, JList, JMap, JValue};
use jni::sys::{jlong, jobject};
use jni::JNIEnv;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use crate::{ok_or_throw, RT, BlockingTable, Error, NATIVE_TABLE};
use std::result::Result as StdResult;
use lancedb::table::MergeResult;

type Result<T> = StdResult<T, Error>;

/// Java MergeBuilder的Rust包装器
#[derive(Clone)]
pub struct BlockingMergeBuilder {
    pub(crate) table: Arc<BlockingTable>,
    pub(crate) on_columns: Vec<String>,
    pub(crate) when_matched_update_all: bool,
    pub(crate) when_matched_update_all_filt: Option<String>,
    pub(crate) when_not_matched_insert_all: bool,
    pub(crate) when_not_matched_by_source_delete: bool,
    pub(crate) when_not_matched_by_source_delete_filt: Option<String>,
    pub(crate) timeout: Option<Duration>,
}

impl BlockingMergeBuilder {
    pub fn new(table: Arc<BlockingTable>, on_columns: Vec<String>) -> Self {
        Self {
            table,
            on_columns,
            when_matched_update_all: false,
            when_matched_update_all_filt: None,
            when_not_matched_insert_all: false,
            when_not_matched_by_source_delete: false,
            when_not_matched_by_source_delete_filt: None,
            timeout: None,
        }
    }

    pub fn when_matched_update_all(&mut self, condition: Option<String>) -> &mut Self {
        self.when_matched_update_all = true;
        self.when_matched_update_all_filt = condition;
        self
    }

    pub fn when_not_matched_insert_all(&mut self) -> &mut Self {
        self.when_not_matched_insert_all = true;
        self
    }

    pub fn when_not_matched_by_source_delete(&mut self, filter: Option<String>) -> &mut Self {
        self.when_not_matched_by_source_delete = true;
        self.when_not_matched_by_source_delete_filt = filter;
        self
    }

    pub fn timeout(&mut self, timeout: Duration) -> &mut Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn execute(&self, new_data: Vec<HashMap<String, serde_json::Value>>) -> Result<MergeResult> {
        // 使用 Arrow 原生构建器直接创建 RecordBatch
        let record_batch = create_record_batch_from_data(new_data)?;
        let reader = arrow::record_batch::RecordBatchReader::from_batch(record_batch)
            .map_err(|e| Error::Arrow(format!("Failed to create reader: {}", e)))?;

        let mut builder = RT.block_on(
            self.table.inner.merge_insert(&self.on_columns.iter().map(|s| s.as_str()).collect::<Vec<_>>())
        )?;

        if self.when_matched_update_all {
            builder.when_matched_update_all(self.when_matched_update_all_filt.clone());
        }

        if self.when_not_matched_insert_all {
            builder.when_not_matched_insert_all();
        }

        if self.when_not_matched_by_source_delete {
            builder.when_not_matched_by_source_delete(self.when_not_matched_by_source_delete_filt.clone());
        }

        if let Some(timeout) = self.timeout {
            builder.timeout(timeout);
        }

        let result = RT.block_on(builder.execute(Box::new(reader)))?;
        Ok(result)
    }
}

/// 使用 Arrow 原生构建器创建 RecordBatch
fn create_record_batch_from_data(
    data: Vec<HashMap<String, serde_json::Value>>
) -> Result<arrow::record_batch::RecordBatch> {
    if data.is_empty() {
        return Err(Error::InvalidInput("Cannot execute merge with empty data".to_string()));
    }

    // 获取所有列名
    let mut column_names = std::collections::HashSet::new();
    for row in &data {
        for key in row.keys() {
            column_names.insert(key.clone());
        }
    }
    let column_names: Vec<String> = column_names.into_iter().collect();

    // 为每列推断数据类型并创建构建器
    let mut builders: HashMap<String, Box<dyn arrow::array::ArrayBuilder>> = HashMap::new();
    for column_name in &column_names {
        let data_type = infer_column_type(&data, column_name);
        builders.insert(column_name.clone(), create_builder_for_type(&data_type));
    }

    // 填充数据
    for row in &data {
        for column_name in &column_names {
            let value = row.get(column_name).unwrap_or(&serde_json::Value::Null);
            append_value_to_builder(&mut builders[column_name], value)?;
        }
    }

    // 构建数组
    let mut arrays: Vec<arrow::array::ArrayRef> = Vec::new();
    let mut fields: Vec<arrow::datatypes::Field> = Vec::new();
    
    for column_name in &column_names {
        let builder = &mut builders[column_name];
        let array = builder.finish();
        arrays.push(Arc::new(array));
        
        let data_type = infer_column_type(&data, column_name);
        fields.push(arrow::datatypes::Field::new(column_name, data_type, true));
    }

    let schema = arrow::datatypes::Schema::new(fields);
    Ok(arrow::record_batch::RecordBatch::try_new(Arc::new(schema), arrays)
        .map_err(|e| Error::Arrow(format!("Failed to create record batch: {}", e)))?)
}

/// 推断列的数据类型
fn infer_column_type(data: &[HashMap<String, serde_json::Value>], column_name: &str) -> arrow::datatypes::DataType {
    let mut has_string = false;
    let mut has_number = false;
    let mut has_boolean = false;
    let mut has_null = false;

    for row in data {
        if let Some(value) = row.get(column_name) {
            match value {
                serde_json::Value::String(_) => has_string = true,
                serde_json::Value::Number(_) => has_number = true,
                serde_json::Value::Bool(_) => has_boolean = true,
                serde_json::Value::Null => has_null = true,
                _ => has_string = true, // 默认为字符串
            }
        }
    }

    if has_boolean {
        arrow::datatypes::DataType::Boolean
    } else if has_number {
        arrow::datatypes::DataType::Float64
    } else {
        arrow::datatypes::DataType::Utf8
    }
}

/// 为数据类型创建相应的构建器
fn create_builder_for_type(data_type: &arrow::datatypes::DataType) -> Box<dyn arrow::array::ArrayBuilder> {
    match data_type {
        arrow::datatypes::DataType::Boolean => Box::new(arrow::array::BooleanBuilder::new()),
        arrow::datatypes::DataType::Float64 => Box::new(arrow::array::Float64Builder::new()),
        arrow::datatypes::DataType::Utf8 => Box::new(arrow::array::StringBuilder::new()),
        _ => Box::new(arrow::array::StringBuilder::new()), // 默认为字符串
    }
}

/// 将值追加到构建器中
fn append_value_to_builder(
    builder: &mut Box<dyn arrow::array::ArrayBuilder>,
    value: &serde_json::Value
) -> Result<()> {
    match value {
        serde_json::Value::Null => {
            builder.append_null();
        }
        serde_json::Value::Bool(b) => {
            if let Some(bool_builder) = builder.as_any().downcast_mut::<arrow::array::BooleanBuilder>() {
                bool_builder.append_value(*b);
            } else {
                return Err(Error::InvalidInput("Type mismatch for boolean value".to_string()));
            }
        }
        serde_json::Value::Number(n) => {
            if let Some(float_builder) = builder.as_any().downcast_mut::<arrow::array::Float64Builder>() {
                if let Some(f) = n.as_f64() {
                    float_builder.append_value(f);
                } else {
                    float_builder.append_null();
                }
            } else {
                return Err(Error::InvalidInput("Type mismatch for number value".to_string()));
            }
        }
        serde_json::Value::String(s) => {
            if let Some(string_builder) = builder.as_any().downcast_mut::<arrow::array::StringBuilder>() {
                string_builder.append_value(s);
            } else {
                return Err(Error::InvalidInput("Type mismatch for string value".to_string()));
            }
        }
        _ => {
            // 对于其他类型，转换为字符串
            if let Some(string_builder) = builder.as_any().downcast_mut::<arrow::array::StringBuilder>() {
                string_builder.append_value(&value.to_string());
            } else {
                return Err(Error::InvalidInput("Type mismatch for complex value".to_string()));
            }
        }
    }
    Ok(())
}

/// 转换Java List<Map<String, Object>>到Rust数据结构
fn convert_java_list_to_rust(
    env: &mut JNIEnv,
    list_obj: JObject,
) -> Result<Vec<HashMap<String, serde_json::Value>>> {
    let list = JList::from_env(env, &list_obj)?;
    let mut result = Vec::new();
    
    let size = list.size(env)?;
    for i in 0..size {
        let map_obj = list.get(env, i)?;
        let map = JMap::from_env(env, &map_obj)?;
        let mut record = HashMap::new();
        
        let key_set = map.key_set(env)?;
        let key_list = JList::from_env(env, &key_set)?;
        let key_count = key_list.size(env)?;
        
        for j in 0..key_count {
            let key_obj = key_list.get(env, j)?;
            let key_str = env.get_string(&JString::from(key_obj))?.into();
            
            let value_obj = map.get(env, &key_obj)?;
            let value = convert_java_object_to_json(env, value_obj)?;
            record.insert(key_str, value);
        }
        
        result.push(record);
    }
    
    Ok(result)
}

/// 转换Java对象到JSON值
fn convert_java_object_to_json(
    env: &mut JNIEnv,
    obj: JObject,
) -> Result<serde_json::Value> {
    if obj.is_null() {
        return Ok(serde_json::Value::Null);
    }

    // 检查是否为字符串
    if let Ok(string_obj) = env.call_method(&obj, "toString", "()Ljava/lang/String;", &[]) {
        if let Ok(jstring) = string_obj.l() {
            let string = env.get_string(&JString::from(jstring))?.into();
            return Ok(serde_json::Value::String(string));
        }
    }

    // 检查是否为数字
    if let Ok(int_obj) = env.call_method(&obj, "intValue", "()I", &[]) {
        let int_value = int_obj.i()?;
        return Ok(serde_json::Value::Number(serde_json::Number::from(int_value)));
    }

    if let Ok(long_obj) = env.call_method(&obj, "longValue", "()J", &[]) {
        let long_value = long_obj.j()?;
        return Ok(serde_json::Value::Number(serde_json::Number::from(long_value)));
    }

    if let Ok(double_obj) = env.call_method(&obj, "doubleValue", "()D", &[]) {
        let double_value = double_obj.d()?;
        return Ok(serde_json::Value::Number(serde_json::Number::from_f64(double_value).unwrap_or(serde_json::Number::from(0))));
    }

    // 检查是否为布尔值
    if let Ok(bool_obj) = env.call_method(&obj, "booleanValue", "()Z", &[]) {
        let bool_value = bool_obj.z()?;
        return Ok(serde_json::Value::Bool(bool_value));
    }

    // 默认为字符串
    let string = env.get_string(&JString::from(obj))?.into();
    Ok(serde_json::Value::String(string))
}

// JNI函数实现

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_MergeBuilder_createNativeMergeBuilder<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject,
    table_handle: jlong,
    on_columns_obj: jobject,
) -> jlong {
    let table: &BlockingTable = unsafe {
        &*(table_handle as *const BlockingTable)
    };

    let on_columns = ok_or_throw!(env, {
        let array = env.get_object_array(&JObject::from_raw(on_columns_obj))?;
        let len = env.get_array_length(&array)?;
        let mut columns = Vec::with_capacity(len as usize);
        for i in 0..len {
            let string_obj = env.get_object_array_element(&array, i)?;
            let string = env.get_string(&JString::from(string_obj))?.into();
            columns.push(string);
        }
        Ok(columns)
    });

    let merge_builder = BlockingMergeBuilder::new(Arc::new(table.clone()), on_columns);
    let handle = Box::into_raw(Box::new(merge_builder)) as jlong;
    handle
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_MergeBuilder_whenMatchedUpdateAllNative<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject,
    handle: jlong,
    condition_obj: jobject,
) {
    let merge_builder: &mut BlockingMergeBuilder = unsafe {
        &mut *(handle as *mut BlockingMergeBuilder)
    };

    let condition = if condition_obj.is_null() {
        None
    } else {
        let condition_string = ok_or_throw!(env, env.get_string(&JString::from(JObject::from_raw(condition_obj))));
        Some(condition_string.into())
    };

    merge_builder.when_matched_update_all(condition);
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_MergeBuilder_whenNotMatchedInsertAllNative<'local>(
    _env: JNIEnv<'local>,
    _obj: JObject,
    handle: jlong,
) {
    let merge_builder: &mut BlockingMergeBuilder = unsafe {
        &mut *(handle as *mut BlockingMergeBuilder)
    };

    merge_builder.when_not_matched_insert_all();
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_MergeBuilder_whenNotMatchedBySourceDeleteNative<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject,
    handle: jlong,
    filter_obj: jobject,
) {
    let merge_builder: &mut BlockingMergeBuilder = unsafe {
        &mut *(handle as *mut BlockingMergeBuilder)
    };

    let filter = if filter_obj.is_null() {
        None
    } else {
        let filter_string = ok_or_throw!(env, env.get_string(&JString::from(JObject::from_raw(filter_obj))));
        Some(filter_string.into())
    };

    merge_builder.when_not_matched_by_source_delete(filter);
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_MergeBuilder_timeoutNative<'local>(
    _env: JNIEnv<'local>,
    _obj: JObject,
    handle: jlong,
    timeout_millis: jlong,
) {
    let merge_builder: &mut BlockingMergeBuilder = unsafe {
        &mut *(handle as *mut BlockingMergeBuilder)
    };

    let timeout = Duration::from_millis(timeout_millis as u64);
    merge_builder.timeout(timeout);
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_MergeBuilder_executeNative<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject,
    handle: jlong,
    new_data_obj: JObject,
) -> jobject {
    let merge_builder: &BlockingMergeBuilder = unsafe {
        &*(handle as *const BlockingMergeBuilder)
    };

    let new_data = ok_or_throw!(env, convert_java_list_to_rust(&mut env, new_data_obj));
    let result = ok_or_throw!(env, merge_builder.execute(new_data));

    // 创建Java MergeResult对象
    let merge_result_class = ok_or_throw!(env, env.find_class("com/lancedb/lancedb/MergeResult"));
    let merge_result = ok_or_throw!(env, env.new_object(
        merge_result_class,
        "(JJJJ)V",
        &[
            JValue::Long(result.version as jlong),
            JValue::Long(result.num_inserted_rows as jlong),
            JValue::Long(result.num_updated_rows as jlong),
            JValue::Long(result.num_deleted_rows as jlong),
        ]
    ));

    merge_result.into_raw()
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_MergeBuilder_releaseNativeMergeBuilder<'local>(
    _env: JNIEnv<'local>,
    _obj: JObject,
    handle: jlong,
) {
    if handle != 0 {
        unsafe {
            let _ = Box::from_raw(handle as *mut BlockingMergeBuilder);
        }
    }
} 