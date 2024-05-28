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

use jni::objects::{JMap, JObject, JString, JValue};
use jni::JNIEnv;

use crate::Result;

pub trait FromJObject<T> {
    fn extract(&self) -> Result<T>;
}

/// Convert a Rust type into a Java Object.
pub trait IntoJava {
    fn into_java<'a>(self, env: &mut JNIEnv<'a>) -> JObject<'a>;
}

impl FromJObject<i32> for JObject<'_> {
    fn extract(&self) -> Result<i32> {
        Ok(JValue::from(self).i()?)
    }
}

impl FromJObject<i64> for JObject<'_> {
    fn extract(&self) -> Result<i64> {
        Ok(JValue::from(self).j()?)
    }
}

impl FromJObject<f32> for JObject<'_> {
    fn extract(&self) -> Result<f32> {
        Ok(JValue::from(self).f()?)
    }
}

impl FromJObject<f64> for JObject<'_> {
    fn extract(&self) -> Result<f64> {
        Ok(JValue::from(self).d()?)
    }
}

pub trait FromJString {
    fn extract(&self, env: &mut JNIEnv) -> Result<String>;
}

impl FromJString for JString<'_> {
    fn extract(&self, env: &mut JNIEnv) -> Result<String> {
        Ok(env.get_string(self)?.into())
    }
}

pub trait JMapExt {
    #[allow(dead_code)]
    fn get_string(&self, env: &mut JNIEnv, key: &str) -> Result<Option<String>>;

    #[allow(dead_code)]
    fn get_i32(&self, env: &mut JNIEnv, key: &str) -> Result<Option<i32>>;

    #[allow(dead_code)]
    fn get_i64(&self, env: &mut JNIEnv, key: &str) -> Result<Option<i64>>;

    #[allow(dead_code)]
    fn get_f32(&self, env: &mut JNIEnv, key: &str) -> Result<Option<f32>>;

    #[allow(dead_code)]
    fn get_f64(&self, env: &mut JNIEnv, key: &str) -> Result<Option<f64>>;
}

fn get_map_value<T>(env: &mut JNIEnv, map: &JMap, key: &str) -> Result<Option<T>>
where
    for<'a> JObject<'a>: FromJObject<T>,
{
    let key_obj: JObject = env.new_string(key)?.into();
    if let Some(value) = map.get(env, &key_obj)? {
        if value.is_null() {
            Ok(None)
        } else {
            Ok(Some(value.extract()?))
        }
    } else {
        Ok(None)
    }
}

impl JMapExt for JMap<'_, '_, '_> {
    fn get_string(&self, env: &mut JNIEnv, key: &str) -> Result<Option<String>> {
        let key_obj: JObject = env.new_string(key)?.into();
        if let Some(value) = self.get(env, &key_obj)? {
            let value_str: JString = value.into();
            Ok(Some(value_str.extract(env)?))
        } else {
            Ok(None)
        }
    }

    fn get_i32(&self, env: &mut JNIEnv, key: &str) -> Result<Option<i32>> {
        get_map_value(env, self, key)
    }

    fn get_i64(&self, env: &mut JNIEnv, key: &str) -> Result<Option<i64>> {
        get_map_value(env, self, key)
    }

    fn get_f32(&self, env: &mut JNIEnv, key: &str) -> Result<Option<f32>> {
        get_map_value(env, self, key)
    }

    fn get_f64(&self, env: &mut JNIEnv, key: &str) -> Result<Option<f64>> {
        get_map_value(env, self, key)
    }
}
