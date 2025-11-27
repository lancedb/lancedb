// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use jni::objects::JObject;
use jni::JNIEnv;
use lancedb::Table;

pub const NATIVE_TABLE: &str = "nativeTableHandle";
use crate::traits::IntoJava;

#[derive(Clone)]
pub struct BlockingTable {
    pub(crate) inner: Table,
}

impl IntoJava for BlockingTable {
    fn into_java<'a>(self, env: &mut JNIEnv<'a>) -> JObject<'a> {
        attach_native_table(env, self)
    }
}

fn attach_native_table<'local>(env: &mut JNIEnv<'local>, table: BlockingTable) -> JObject<'local> {
    let j_table = create_java_table_object(env);

    match unsafe { env.set_rust_field(&j_table, NATIVE_TABLE, table) } {
        Ok(_) => j_table,
        Err(err) => {
            env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to set native handle for Table: {}", err),
            )
            .expect("Error throwing exception");
            JObject::null()
        }
    }
}

fn create_java_table_object<'a>(env: &mut JNIEnv<'a>) -> JObject<'a> {
    env.new_object("com/lancedb/lancedb/Table", "()V", &[])
        .expect("Failed to create Java Lance Table instance")
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_Table_releaseNativeTable(
    mut env: JNIEnv,
    j_table: JObject,
) {
    let _: BlockingTable = unsafe {
        env.take_rust_field(j_table, NATIVE_TABLE)
            .expect("Failed to take native Table handle")
    };
}
