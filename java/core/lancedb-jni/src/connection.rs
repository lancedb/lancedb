use jni::objects::{JObject, JString, JValue, JList, JMap};  
use jni::sys::{jobject, jlong};  
use jni::JNIEnv;  
use std::collections::HashMap;  
use crate::{ok_or_throw, RT, BlockingConnection, NATIVE_CONNECTION};  

#[derive(Clone)]
pub struct BlockingConnection {
    pub(crate) inner: Connection,
}

impl BlockingConnection {  
    pub fn create_table_with_data(  
        &self,  
        name: &str,  
        data: Vec<HashMap<String, serde_json::Value>>,  
        mode: &str,  
    ) -> Result<BlockingTable> {  
        let records: Vec<_> = data.into_iter()  
            .map(|map| serde_json::Value::Object(map.into_iter().collect()))  
            .collect();  
          
        let json_data = serde_json::Value::Array(records);  
        let reader = lance::io::RecordBatchReader::from_json(&json_data.to_string())?;  
          
        let create_mode = match mode {  
            "create" => lance::dataset::CreateMode::Create,  
            "overwrite" => lance::dataset::CreateMode::Overwrite,  
            "exist_ok" => lance::dataset::CreateMode::ExistOk,  
            _ => return Err(lance::Error::InvalidInput("Invalid create mode".into())),  
        };  
  
        let table = RT.block_on(  
            self.inner.create_table(name, reader).mode(create_mode).execute()  
        )?;  
          
        Ok(BlockingTable { inner: table })  
    }  
  
    pub fn open_table(&self, name: &str) -> Result<BlockingTable> {  
        let table = RT.block_on(self.inner.open_table(name))?;  
        Ok(BlockingTable { inner: table })  
    }  
  
    pub fn drop_table(&self, name: &str) -> Result<()> {  
        RT.block_on(self.inner.drop_table(name))  
    }  
}  
  
#[no_mangle]  
pub extern "system" fn Java_com_lancedb_lancedb_Connection_connectNative<'local>(  
    mut env: JNIEnv<'local>,  
    _obj: JObject,  
    uri_obj: JString,  
    options_obj: JObject,  
) -> JObject<'local> {  
    let uri: String = ok_or_throw!(env, env.get_string(&uri_obj)).into();  
      
    // Parse connection options  
    let mut builder = lancedb::connect(&uri);  
      
    if !options_obj.is_null() {  
        // Extract API key  
        // Extract API key
        if let Some(api_key) = extract_optional_string(&mut env, &options_obj, "getApiKey")? {
           builder = builder.api_key(&api_key);
        }
          
        // Extract region  
        if let Ok(region_obj) = env.call_method(&options_obj, "getRegion", "()Ljava/lang/String;", &[]) {  
            if let Ok(region_str) = env.get_string(&JString::from(region_obj.l().unwrap())) {  
                builder = builder.region(&String::from(region_str));  
            }  
        }  
    }  
  
    let blocking_connection = ok_or_throw!(env, RT.block_on(builder.execute()).map(|conn| BlockingConnection { inner: conn }));  
    blocking_connection.into_java(&mut env)  
}  
  
#[no_mangle]  
pub extern "system" fn Java_com_lancedb_lancedb_Connection_createTableNative<'local>(  
    mut env: JNIEnv<'local>,  
    j_connection: JObject,  
    name_obj: JString,  
    data_obj: JObject, // List<Map<String, Object>>  
    mode_obj: JString,  
) -> JObject<'local> {  
    let connection: &BlockingConnection = unsafe {  
        env.get_rust_field(j_connection, NATIVE_CONNECTION)  
            .expect("Failed to get native Connection handle")  
    };  
  
    let name: String = ok_or_throw!(env, env.get_string(&name_obj)).into();  
    let mode: String = ok_or_throw!(env, env.get_string(&mode_obj)).into();  
      
    // Convert Java List<Map<String, Object>> to Rust data structure  
    let data = ok_or_throw!(env, convert_java_list_to_rust(&mut env, data_obj));  
      
    let table = ok_or_throw!(env, connection.create_table_with_data(&name, data, &mode));  
    table.into_java(&mut env)  
}  
  
#[no_mangle]  
pub extern "system" fn Java_com_lancedb_lancedb_Connection_openTableNative<'local>(  
    mut env: JNIEnv<'local>,  
    j_connection: JObject,  
    name_obj: JString,  
) -> JObject<'local> {  
    let connection: &BlockingConnection = unsafe {  
        env.get_rust_field(j_connection, NATIVE_CONNECTION)  
            .expect("Failed to get native Connection handle")  
    };  
  
    let name: String = ok_or_throw!(env, env.get_string(&name_obj)).into();  
    let table = ok_or_throw!(env, connection.open_table(&name));  
    table.into_java(&mut env)  
}  
  
fn convert_java_list_to_rust(  
    env: &mut JNIEnv,  
    list_obj: JObject,  
) -> Result<Vec<HashMap<String, serde_json::Value>>, Box<dyn std::error::Error>> {  
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

fn extract_optional_string(env: &mut JNIEnv, obj: &JObject, method: &str) -> Result<Option<String>> {
    let optional = env.call_method(obj, method, "()Ljava/util/Optional;", &[])?;
    let optional_obj = optional.l()?;

    let is_present = env.call_method(&optional_obj, "isPresent", "()Z", &[])?.z()?;
    if !is_present {
        return Ok(None);
    }

    let value = env.call_method(&optional_obj, "get", "()Ljava/lang/Object;", &[])?.l()?;
    let string = env.get_string(&JString::from(value))?.into();
    Ok(Some(string))
}
  
fn convert_java_object_to_json(  
    env: &mut JNIEnv,  
    obj: JObject,  
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {  
    if obj.is_null() {  
        return Ok(serde_json::Value::Null);  
    }  
      
    // Check if it's a String
    if env.is_instance_of(&obj, "java/lang/String")? {
        let jstr = JString::from(obj);
        let val = env.get_string(&jstr)?;
        return Ok(serde_json::Value::String(val.to_str()?.to_string()));
    }

    // Check if it's an Integer
    if env.is_instance_of(&obj, "java/lang/Integer")? {
        let int_obj = env.call_method(obj, "intValue", "()I", &[])?;
        let int_value = int_obj.i()?;
        return Ok(serde_json::Value::Number(serde_json::Number::from(int_value)));
    }

    // Check if it's a Long
    if env.is_instance_of(&obj, "java/lang/Long")? {
        let long_obj = env.call_method(obj, "longValue", "()J", &[])?;
        let long_value = long_obj.j()?;
        return Ok(serde_json::Value::Number(serde_json::Number::from(long_value)));
    }

    // Check if it's a Float
    if env.is_instance_of(&obj, "java/lang/Float")? {
        let float_obj = env.call_method(obj, "floatValue", "()F", &[])?;
        let float_value = float_obj.f()?;
        return Ok(serde_json::Value::Number(serde_json::Number::from_f64(float_value as f64).unwrap()));
    }

    // Check if it's a Double
    if env.is_instance_of(&obj, "java/lang/Double")? {
        let double_obj = env.call_method(obj, "doubleValue", "()D", &[])?;
        let double_value = double_obj.d()?;
        return Ok(serde_json::Value::Number(serde_json::Number::from_f64(double_value).unwrap()));
    }

    // Check if it's a Boolean
    if env.is_instance_of(&obj, "java/lang/Boolean")? {
        let bool_obj = env.call_method(obj, "booleanValue", "()Z", &[])?;
        let bool_value = bool_obj.z()?;
        return Ok(serde_json::Value::Bool(bool_value));
    }

    // Check if it's a List (for vectors)
    if env.is_instance_of(&obj, "java/util/List")? {
        let list = JList::from_env(env, &obj)?;
        let mut vec_values = Vec::new();
        let size = list.size(env)?;

        for i in 0..size {
            let item = list.get(env, i)?;
            let item_value = convert_java_object_to_json(env, item)?;
            vec_values.push(item_value);
        }

        return Ok(serde_json::Value::Array(vec_values));
    }

    // Default case - try to convert to string
    let string_obj = env.call_method(obj, "toString", "()Ljava/lang/String;", &[])?;
    let jstr = JString::from(string_obj.l()?);
    let val = env.get_string(&jstr)?;
    Ok(serde_json::Value::String(val.to_str()?.to_string()))
}