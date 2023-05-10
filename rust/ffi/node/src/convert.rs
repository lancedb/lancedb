// Copyright 2023 Lance Developers.
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

use neon::prelude::*;

pub(crate) fn vec_str_to_array<'a, C: Context<'a>>(
    vec: &Vec<String>,
    cx: &mut C,
) -> JsResult<'a, JsArray> {
    let a = JsArray::new(cx, vec.len() as u32);
    for (i, s) in vec.iter().enumerate() {
        let v = cx.string(s);
        a.set(cx, i as u32, v)?;
    }
    Ok(a)
}

pub(crate) fn js_array_to_vec(array: &JsArray, cx: &mut FunctionContext) -> Vec<f32> {
    let mut query_vec: Vec<f32> = Vec::new();
    for i in 0..array.len(cx) {
        let entry: Handle<JsNumber> = array.get(cx, i).unwrap();
        query_vec.push(entry.value(cx) as f32);
    }
    query_vec
}
