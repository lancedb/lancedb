// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::slice;

use jni::objects::{JByteBuffer, JObjectArray, JString};
use jni::sys::jobjectArray;
use jni::{objects::JObject, JNIEnv};

use crate::error::{Error, Result};

/// TODO(lu) import from lance-jni without duplicate
/// Extend JNIEnv with helper functions.
pub trait JNIEnvExt {
    /// Get integers from Java List<Integer> object.
    fn get_integers(&mut self, obj: &JObject) -> Result<Vec<i32>>;

    /// Get strings from Java List<String> object.
    fn get_strings(&mut self, obj: &JObject) -> Result<Vec<String>>;

    /// Get strings from Java String[] object.
    /// Note that get Option<Vec<String>> from Java Optional<String[]> just doesn't work.
    #[allow(unused)]
    fn get_strings_array(&mut self, obj: jobjectArray) -> Result<Vec<String>>;

    /// Get Option<String> from Java Optional<String>.
    fn get_string_opt(&mut self, obj: &JObject) -> Result<Option<String>>;

    /// Get Option<Vec<String>> from Java Optional<List<String>>.
    #[allow(unused)]
    fn get_strings_opt(&mut self, obj: &JObject) -> Result<Option<Vec<String>>>;

    /// Get Option<i32> from Java Optional<Integer>.
    fn get_int_opt(&mut self, obj: &JObject) -> Result<Option<i32>>;

    /// Get Option<Vec<i32>> from Java Optional<List<Integer>>.
    fn get_ints_opt(&mut self, obj: &JObject) -> Result<Option<Vec<i32>>>;

    /// Get Option<i64> from Java Optional<Long>.
    #[allow(unused)]
    fn get_long_opt(&mut self, obj: &JObject) -> Result<Option<i64>>;

    /// Get Option<u64> from Java Optional<Long>.
    #[allow(unused)]
    fn get_u64_opt(&mut self, obj: &JObject) -> Result<Option<u64>>;

    /// Get Option<&[u8]> from Java Optional<ByteBuffer>.
    #[allow(unused)]
    fn get_bytes_opt(&mut self, obj: &JObject) -> Result<Option<&[u8]>>;

    fn get_optional<T, F>(&mut self, obj: &JObject, f: F) -> Result<Option<T>>
    where
        F: FnOnce(&mut JNIEnv, &JObject) -> Result<T>;
}

impl JNIEnvExt for JNIEnv<'_> {
    fn get_integers(&mut self, obj: &JObject) -> Result<Vec<i32>> {
        let list = self.get_list(obj)?;
        let mut iter = list.iter(self)?;
        let mut results = Vec::with_capacity(list.size(self)? as usize);
        while let Some(elem) = iter.next(self)? {
            let int_obj = self.call_method(elem, "intValue", "()I", &[])?;
            let int_value = int_obj.i()?;
            results.push(int_value);
        }
        Ok(results)
    }

    fn get_strings(&mut self, obj: &JObject) -> Result<Vec<String>> {
        let list = self.get_list(obj)?;
        let mut iter = list.iter(self)?;
        let mut results = Vec::with_capacity(list.size(self)? as usize);
        while let Some(elem) = iter.next(self)? {
            let jstr = JString::from(elem);
            let val = self.get_string(&jstr)?;
            results.push(val.to_str()?.to_string())
        }
        Ok(results)
    }

    fn get_strings_array(&mut self, obj: jobjectArray) -> Result<Vec<String>> {
        let jobject_array = unsafe { JObjectArray::from_raw(obj) };
        let array_len = self.get_array_length(&jobject_array)?;
        let mut res: Vec<String> = Vec::new();
        for i in 0..array_len {
            let item: JString = self.get_object_array_element(&jobject_array, i)?.into();
            res.push(self.get_string(&item)?.into());
        }
        Ok(res)
    }

    fn get_string_opt(&mut self, obj: &JObject) -> Result<Option<String>> {
        self.get_optional(obj, |env, inner_obj| {
            let java_obj_gen = env.call_method(inner_obj, "get", "()Ljava/lang/Object;", &[])?;
            let java_string_obj = java_obj_gen.l()?;
            let jstr = JString::from(java_string_obj);
            let val = env.get_string(&jstr)?;
            Ok(val.to_str()?.to_string())
        })
    }

    fn get_strings_opt(&mut self, obj: &JObject) -> Result<Option<Vec<String>>> {
        self.get_optional(obj, |env, inner_obj| {
            let java_obj_gen = env.call_method(inner_obj, "get", "()Ljava/lang/Object;", &[])?;
            let java_list_obj = java_obj_gen.l()?;
            env.get_strings(&java_list_obj)
        })
    }

    fn get_int_opt(&mut self, obj: &JObject) -> Result<Option<i32>> {
        self.get_optional(obj, |env, inner_obj| {
            let java_obj_gen = env.call_method(inner_obj, "get", "()Ljava/lang/Object;", &[])?;
            let java_int_obj = java_obj_gen.l()?;
            let int_obj = env.call_method(java_int_obj, "intValue", "()I", &[])?;
            let int_value = int_obj.i()?;
            Ok(int_value)
        })
    }

    fn get_ints_opt(&mut self, obj: &JObject) -> Result<Option<Vec<i32>>> {
        self.get_optional(obj, |env, inner_obj| {
            let java_obj_gen = env.call_method(inner_obj, "get", "()Ljava/lang/Object;", &[])?;
            let java_list_obj = java_obj_gen.l()?;
            env.get_integers(&java_list_obj)
        })
    }

    fn get_long_opt(&mut self, obj: &JObject) -> Result<Option<i64>> {
        self.get_optional(obj, |env, inner_obj| {
            let java_obj_gen = env.call_method(inner_obj, "get", "()Ljava/lang/Object;", &[])?;
            let java_long_obj = java_obj_gen.l()?;
            let long_obj = env.call_method(java_long_obj, "longValue", "()J", &[])?;
            let long_value = long_obj.j()?;
            Ok(long_value)
        })
    }

    fn get_u64_opt(&mut self, obj: &JObject) -> Result<Option<u64>> {
        self.get_optional(obj, |env, inner_obj| {
            let java_obj_gen = env.call_method(inner_obj, "get", "()Ljava/lang/Object;", &[])?;
            let java_long_obj = java_obj_gen.l()?;
            let long_obj = env.call_method(java_long_obj, "longValue", "()J", &[])?;
            let long_value = long_obj.j()?;
            Ok(long_value as u64)
        })
    }

    fn get_bytes_opt(&mut self, obj: &JObject) -> Result<Option<&[u8]>> {
        self.get_optional(obj, |env, inner_obj| {
            let java_obj_gen = env.call_method(inner_obj, "get", "()Ljava/lang/Object;", &[])?;
            let java_byte_buffer_obj = java_obj_gen.l()?;
            let j_byte_buffer = JByteBuffer::from(java_byte_buffer_obj);
            let raw_data = env.get_direct_buffer_address(&j_byte_buffer)?;
            let capacity = env.get_direct_buffer_capacity(&j_byte_buffer)?;
            let data = unsafe { slice::from_raw_parts(raw_data, capacity) };
            Ok(data)
        })
    }

    fn get_optional<T, F>(&mut self, obj: &JObject, f: F) -> Result<Option<T>>
    where
        F: FnOnce(&mut JNIEnv, &JObject) -> Result<T>,
    {
        if obj.is_null() {
            return Ok(None);
        }
        let is_present = self.call_method(obj, "isPresent", "()Z", &[])?;
        if !is_present.z()? {
            // TODO(lu): put get java object into here cuz can only get java Object
            Ok(None)
        } else {
            f(self, obj).map(Some)
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lance_test_JniTestHelper_parseInts(
    mut env: JNIEnv,
    _obj: JObject,
    list_obj: JObject, // List<Integer>
) {
    ok_or_throw_without_return!(env, env.get_integers(&list_obj));
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lance_test_JniTestHelper_parseIntsOpt(
    mut env: JNIEnv,
    _obj: JObject,
    list_obj: JObject, // Optional<List<Integer>>
) {
    ok_or_throw_without_return!(env, env.get_ints_opt(&list_obj));
}
