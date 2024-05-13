use crate::ffi::JNIEnvExt;
use crate::traits::IntoJava;
use crate::{Error, RT};
use jni::objects::{JObject, JString, JValue};
use jni::JNIEnv;
pub const NATIVE_CONNECTION: &str = "nativeConnectionHandle";
use crate::Result;
use lancedb::connection::{connect, Connection};

#[derive(Clone)]
pub struct BlockingConnection {
    pub(crate) inner: Connection,
}

impl BlockingConnection {
    pub fn create(dataset_uri: &str) -> Result<Self> {
        let inner = RT.block_on(connect(dataset_uri).execute())?;
        Ok(Self { inner })
    }

    pub fn table_names(
        &self,
        start_after: Option<String>,
        limit: Option<i32>,
    ) -> Result<Vec<String>> {
        let mut op = self.inner.table_names();
        if let Some(start_after) = start_after {
            op = op.start_after(start_after);
        }
        if let Some(limit) = limit {
            op = op.limit(limit as u32);
        }
        Ok(RT.block_on(op.execute())?)
    }
}

impl IntoJava for BlockingConnection {
    fn into_java<'a>(self, env: &mut JNIEnv<'a>) -> JObject<'a> {
        attach_native_connection(env, self)
    }
}

fn attach_native_connection<'local>(
    env: &mut JNIEnv<'local>,
    connection: BlockingConnection,
) -> JObject<'local> {
    let j_connection = create_java_connection_object(env);
    // This block sets a native Rust object (Connection) as a field in the Java object (j_Connection).
    // Caution: This creates a potential for memory leaks. The Rust object (Connection) is not
    // automatically garbage-collected by Java, and its memory will not be freed unless
    // explicitly handled.
    //
    // To prevent memory leaks, ensure the following:
    // 1. The Java object (`j_Connection`) should implement the `java.io.Closeable` interface.
    // 2. Users of this Java object should be instructed to always use it within a try-with-resources
    //    statement (or manually call the `close()` method) to ensure that `self.close()` is invoked.
    match unsafe { env.set_rust_field(&j_connection, NATIVE_CONNECTION, connection) } {
        Ok(_) => j_connection,
        Err(err) => {
            env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to set native handle for Connection: {}", err),
            )
            .expect("Error throwing exception");
            JObject::null()
        }
    }
}

fn create_java_connection_object<'a>(env: &mut JNIEnv<'a>) -> JObject<'a> {
    env.new_object("com/lancedb/lancedb/Connection", "()V", &[])
        .expect("Failed to create Java Lance Connection instance")
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_Connection_releaseNativeConnection(
    mut env: JNIEnv,
    j_connection: JObject,
) {
    let _: BlockingConnection = unsafe {
        env.take_rust_field(j_connection, NATIVE_CONNECTION)
            .expect("Failed to take native Connection handle")
    };
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_Connection_connect<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject,
    dataset_uri_object: JString,
) -> JObject<'local> {
    let dataset_uri: String = ok_or_throw!(env, env.get_string(&dataset_uri_object)).into();
    let blocking_connection = ok_or_throw!(env, BlockingConnection::create(&dataset_uri));
    blocking_connection.into_java(&mut env)
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lancedb_Connection_tableNames<'local>(
    mut env: JNIEnv<'local>,
    j_connection: JObject,
    start_after_obj: JObject, // Optional<String>
    limit_obj: JObject,       // Optional<Integer>
) -> JObject<'local> {
    ok_or_throw!(
        env,
        inner_table_names(&mut env, j_connection, start_after_obj, limit_obj)
    )
}

fn inner_table_names<'local>(
    env: &mut JNIEnv<'local>,
    j_connection: JObject,
    start_after_obj: JObject, // Optional<String>
    limit_obj: JObject,       // Optional<Integer>
) -> Result<JObject<'local>> {
    let start_after = env.get_string_opt(&start_after_obj)?;
    let limit = env.get_int_opt(&limit_obj)?;
    let conn =
        unsafe { env.get_rust_field::<_, _, BlockingConnection>(j_connection, NATIVE_CONNECTION) }?;
    let table_names = conn.table_names(start_after, limit)?;
    drop(conn);
    let j_names = env.new_object("java/util/ArrayList", "()V", &[])?;
    for item in table_names {
        let jstr_item = env.new_string(item)?;
        let item_jobj = JObject::from(jstr_item);
        let item_gen = JValue::Object(&item_jobj);
        env.call_method(&j_names, "add", "(Ljava/lang/Object;)Z", &[item_gen])?;
    }
    Ok(j_names)
}
