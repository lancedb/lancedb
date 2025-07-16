use jni::objects::{JObject, JValue};  
use jni::JNIEnv;  
use jni::sys::jlong;
use crate::{BlockingConnection, BlockingTable, NATIVE_CONNECTION, NATIVE_TABLE};  
  
pub trait IntoJava<'local> {  
    fn into_java(self, env: &mut JNIEnv<'local>) -> JObject<'local>;  
}  
  
impl<'local> IntoJava<'local> for BlockingConnection {  
    fn into_java(self, env: &mut JNIEnv<'local>) -> JObject<'local> {  
        let connection_class = env.find_class("com/lancedb/lancedb/Connection")  
            .expect("Failed to find Connection class");  
          
        let connection_obj = env.alloc_object(&connection_class)  
            .expect("Failed to allocate Connection object");  
          
        // Store the native handle  
        let boxed_connection = Box::new(self);  
        let handle = Box::into_raw(boxed_connection) as jlong;  
          
        env.set_field(&connection_obj, NATIVE_CONNECTION, "J", JValue::Long(handle))  
            .expect("Failed to set native connection handle");  
          
        connection_obj  
    }  
}  
  
impl<'local> IntoJava<'local> for BlockingTable {  
    fn into_java(self, env: &mut JNIEnv<'local>) -> JObject<'local> {  
        let table_class = env.find_class("com/lancedb/lancedb/Table")  
            .expect("Failed to find Table class");  
          
        let table_obj = env.alloc_object(&table_class)  
            .expect("Failed to allocate Table object");  
          
        // Store the native handle  
        let boxed_table = Box::new(self.clone());  
        let handle = Box::into_raw(boxed_table) as jlong;  
          
        env.set_field(&table_obj, NATIVE_TABLE, "J", JValue::Long(handle))  
            .expect("Failed to set native table handle");  
          
        // Set table name  
        let table_name = self.inner.name();  
        let name_str = env.new_string(table_name)  
            .expect("Failed to create table name string");  
          
        env.set_field(&table_obj, "name", "Ljava/lang/String;", JValue::Object(&name_str))  
            .expect("Failed to set table name");  
          
        table_obj  
    }  
}