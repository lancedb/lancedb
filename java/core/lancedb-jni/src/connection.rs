use jni::objects::{JObject, JString, JValue};
use jni::JNIEnv;
use lancedb::connection::connect;

use crate::{Error, RT};

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_Connection_nativeTableNames<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject,
    dataset_uri_object: JString,
) -> JObject<'local> {
    let dataset_uri: String = ok_or_throw!(env, env.get_string(&dataset_uri_object)).into();
    let conn = ok_or_throw!(env, RT.block_on(connect(dataset_uri.as_str()).execute()));
    let table_names = ok_or_throw!(env, RT.block_on(conn.table_names().execute()));

    let j_names = ok_or_throw!(env, env.new_object("java/util/ArrayList", "()V", &[]));
    for item in table_names {
        let jstr_item = ok_or_throw!(env, env.new_string(item));
        let item_jobj = JObject::from(jstr_item);
        let item_gen = JValue::Object(&item_jobj);
        ok_or_throw!(
            env,
            env.call_method(&j_names, "add", "(Ljava/lang/Object;)Z", &[item_gen])
        );
    }
    j_names
}
