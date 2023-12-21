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

use neon::{
    context::{Context, FunctionContext},
    result::JsResult,
    types::{JsBoolean, JsBox, JsPromise, JsString},
};

use crate::{error::ResultExt, runtime, table::JsTable};

pub(crate) fn table_create_scalar_index(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let js_table = cx.this().downcast_or_throw::<JsBox<JsTable>, _>(&mut cx)?;
    let column = cx.argument::<JsString>(0)?.value(&mut cx);
    let replace = cx.argument::<JsBoolean>(1)?.value(&mut cx);

    let rt = runtime(&mut cx)?;

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();
    let mut table = js_table.table.clone();

    rt.spawn(async move {
        let idx_result = table.create_scalar_index(&column, replace).await;

        deferred.settle_with(&channel, move |mut cx| {
            idx_result.or_throw(&mut cx)?;
            Ok(cx.undefined())
        });
    });
    Ok(promise)
}
