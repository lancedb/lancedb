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
use neon::types::buffer::TypedArray;

use crate::error::ResultExt;

pub fn vec_str_to_array<'a, C: Context<'a>>(
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

pub fn js_array_to_vec(array: &JsArray, cx: &mut FunctionContext) -> Vec<f32> {
    let mut query_vec: Vec<f32> = Vec::new();
    for i in 0..array.len(cx) {
        let entry: Handle<JsNumber> = array.get(cx, i).unwrap();
        query_vec.push(entry.value(cx) as f32);
    }
    query_vec
}

// Creates a new JsBuffer from a rust buffer with a special logic for electron
pub fn new_js_buffer<'a>(
    buffer: Vec<u8>,
    cx: &mut TaskContext<'a>,
    is_electron: bool,
) -> NeonResult<Handle<'a, JsBuffer>> {
    if is_electron {
        // Electron does not support `external`: https://github.com/neon-bindings/neon/pull/937
        let mut js_buffer = JsBuffer::new(cx, buffer.len()).or_throw(cx)?;
        let buffer_data = js_buffer.as_mut_slice(cx);
        buffer_data.copy_from_slice(buffer.as_slice());
        Ok(js_buffer)
    } else {
        Ok(JsBuffer::external(cx, buffer))
    }
}
