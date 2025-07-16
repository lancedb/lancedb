use jni::objects::{JObject, JString, JValue, JList, JMap};
use jni::sys::{jobject, jlong};
use jni::JNIEnv;
use std::collections::HashMap;
use crate::{ok_or_throw, RT, NATIVE_TABLE, Error};
use crate::connection::BlockingTable;

#[derive(Clone)]
pub struct BlockingTable {
    pub(crate) inner: lancedb::Table,
}

impl BlockingTable {
    pub fn add_data(&self, data: Vec<HashMap<String, serde_json::Value>>) -> Result<(), Error> {
        let records: Vec<_> = data.into_iter()
            .map(|map| serde_json::Value::Object(map.into_iter().collect()))
            .collect();

        let json_data = serde_json::Value::Array(records);
        let reader = lance::io::RecordBatchReader::from_json(&json_data.to_string())?;

        RT.block_on(self.inner.add(reader).execute())?;
        Ok(())
    }

    pub fn delete_rows(&self, condition: &str) -> Result<u64, Error> {
        let result = RT.block_on(self.inner.delete(condition).execute())?;
        Ok(result)
    }

    pub fn count_rows(&self) -> Result<u64, Error> {
        RT.block_on(self.inner.count_rows(None))
    }

    pub fn get_schema(&self) -> Result<arrow::datatypes::Schema, Error> {
        Ok(self.inner.schema().clone())
    }
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_Table_addNative<'local>(
    mut env: JNIEnv<'local>,
    j_table: JObject,
    data_obj: JObject, // List<Map<String, Object>>
) {
    let table: &BlockingTable = unsafe {
        &*env.get_rust_field(j_table, NATIVE_TABLE)
            .expect("Failed to get native Table handle")
    };

    let data = ok_or_throw_without_return!(env, convert_java_list_to_rust(&mut env, data_obj));
    ok_or_throw_without_return!(env, table.add_data(data));
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_Table_deleteNative<'local>(
    mut env: JNIEnv<'local>,
    j_table: JObject,
    condition_obj: JString,
) -> jlong {
    let table: &BlockingTable = unsafe {
        &*env.get_rust_field(j_table, NATIVE_TABLE)
            .expect("Failed to get native Table handle")
    };

    let condition: String = ok_or_throw_with_return!(env, env.get_string(&condition_obj), 0).into();
    let result = ok_or_throw_with_return!(env, table.delete_rows(&condition), 0);
    result as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_Table_countRowsNative<'local>(
    mut env: JNIEnv<'local>,
    j_table: JObject,
) -> jlong {
    let table: &BlockingTable = unsafe {
        &*env.get_rust_field(j_table, NATIVE_TABLE)
            .expect("Failed to get native Table handle")
    };

    let result = ok_or_throw_with_return!(env, table.count_rows(), 0);
    result as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_Table_getSchemaNative<'local>(
    mut env: JNIEnv<'local>,
    j_table: JObject,
) -> JObject<'local> {
    let table: &BlockingTable = unsafe {
        &*env.get_rust_field(j_table, NATIVE_TABLE)
            .expect("Failed to get native Table handle")
    };

    let schema = ok_or_throw!(env, table.get_schema());
    // Convert Arrow schema to Java Arrow schema
    convert_arrow_schema_to_java(&mut env, &schema)
}

fn convert_arrow_schema_to_java<'local>(
    env: &mut JNIEnv<'local>,
    schema: &arrow::datatypes::Schema,
) -> JObject<'local> {
    // This is a simplified implementation
    // In practice, you'd need to properly convert Arrow schema to Java Arrow schema
    JObject::null()
}