// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use lancedb::index::{scalar::BTreeIndexBuilder, Index};
use neon::{
    context::{Context, FunctionContext},
    result::JsResult,
    types::{JsBoolean, JsBox, JsPromise, JsString},
};

use crate::{error::ResultExt, runtime, table::JsTable};

pub fn table_create_scalar_index(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let js_table = cx.this().downcast_or_throw::<JsBox<JsTable>, _>(&mut cx)?;
    let column = cx.argument::<JsString>(0)?.value(&mut cx);
    let replace = cx.argument::<JsBoolean>(1)?.value(&mut cx);

    let rt = runtime(&mut cx)?;

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();
    let table = js_table.table.clone();

    rt.spawn(async move {
        let idx_result = table
            .create_index(&[column], Index::BTree(BTreeIndexBuilder::default()))
            .replace(replace)
            .execute()
            .await;

        deferred.settle_with(&channel, move |mut cx| {
            idx_result.or_throw(&mut cx)?;
            Ok(cx.undefined())
        });
    });
    Ok(promise)
}
