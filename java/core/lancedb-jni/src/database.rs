use jni::objects::{JObject, JString, JValue};
use jni::JNIEnv;
use lancedb::connection::connect;

use crate::{Error, RT};

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_Database_tableNames<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject,
    dataset_uri_object: JString,
) -> JObject<'local> {
    let dataset_uri: String = ok_or_throw!(env, env.get_string(&dataset_uri_object)).into();
    let conn = ok_or_throw!(env, RT.block_on(connect(dataset_uri.as_str()).execute()));
    let table_names = ok_or_throw!(env, RT.block_on(conn.table_names().execute()));

    let j_names = env.new_object("java/util/ArrayList", "()V", &[]).unwrap();
    for item in table_names {
        let item_jobj = JObject::from(env.new_string(item).expect("msg"));
        let item_gen = JValue::Object(&item_jobj);
        env.call_method(&j_names, "add", "(Ljava/lang/Object;)Z", &[item_gen])
            .expect("msg");
    }
    j_names
}
