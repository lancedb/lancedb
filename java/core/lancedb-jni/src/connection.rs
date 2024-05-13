use crate::ffi::JNIEnvExt;
use crate::{Error, RT};
use jni::objects::{JObject, JString, JValue};
use jni::JNIEnv;
use lancedb::connection::connect;

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_Connection_nativeTableNames<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject,
    dataset_uri_object: JString,
    start_after_obj: JObject, // Optional<String>
    limit_obj: JObject,       // Optional<Integer>
) -> JObject<'local> {
    let dataset_uri: String = ok_or_throw!(env, env.get_string(&dataset_uri_object)).into();
    let start_after = ok_or_throw!(env, env.get_string_opt(&start_after_obj));
    let limit = ok_or_throw!(env, env.get_int_opt(&limit_obj));

    let conn = ok_or_throw!(env, RT.block_on(connect(dataset_uri.as_str()).execute()));
    let mut op = conn.table_names();
    if let Some(start_after) = start_after {
        op = op.start_after(start_after);
    }
    if let Some(limit) = limit {
        op = op.limit(limit as u32);
    }
    let table_names = ok_or_throw!(env, RT.block_on(op.execute()));

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
