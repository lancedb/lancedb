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

use lance_linalg::distance::MetricType;
use lancedb::index::vector::IvfPqIndexBuilder;
use lancedb::index::Index;
use neon::context::FunctionContext;
use neon::prelude::*;
use std::convert::TryFrom;

use crate::error::ResultExt;
use crate::neon_ext::js_object_ext::JsObjectExt;
use crate::runtime;
use crate::table::JsTable;

pub fn table_create_vector_index(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let js_table = cx.this().downcast_or_throw::<JsBox<JsTable>, _>(&mut cx)?;
    let index_params = cx.argument::<JsObject>(0)?;

    let rt = runtime(&mut cx)?;

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();
    let table = js_table.table.clone();

    let column_name = index_params
        .get_opt::<JsString, _, _>(&mut cx, "column")?
        .map(|s| s.value(&mut cx))
        .unwrap_or("vector".to_string()); // Backward compatibility

    let replace = index_params
        .get_opt::<JsBoolean, _, _>(&mut cx, "replace")?
        .map(|r| r.value(&mut cx));

    let tbl = table.clone();
    let ivf_pq_builder = get_index_params_builder(&mut cx, index_params).or_throw(&mut cx)?;

    let mut index_builder = tbl.create_index(&[column_name], Index::IvfPq(ivf_pq_builder));
    if let Some(replace) = replace {
        index_builder = index_builder.replace(replace);
    }

    rt.spawn(async move {
        let idx_result = index_builder.execute().await;
        deferred.settle_with(&channel, move |mut cx| {
            idx_result.or_throw(&mut cx)?;
            Ok(cx.boxed(JsTable::from(table)))
        });
    });
    Ok(promise)
}

fn get_index_params_builder(
    cx: &mut FunctionContext,
    obj: Handle<JsObject>,
) -> crate::error::Result<IvfPqIndexBuilder> {
    if obj.get_opt::<JsString, _, _>(cx, "index_name")?.is_some() {
        return Err(crate::error::Error::LanceDB {
            message: "Setting the index_name is no longer supported".to_string(),
        });
    }
    let mut builder = IvfPqIndexBuilder::default();
    if let Some(metric_type) = obj.get_opt::<JsString, _, _>(cx, "metric_type")? {
        let metric_type = MetricType::try_from(metric_type.value(cx).as_str())?;
        builder = builder.distance_type(metric_type);
    }
    if let Some(np) = obj.get_opt_u32(cx, "num_partitions")? {
        builder = builder.num_partitions(np);
    }
    if let Some(ns) = obj.get_opt_u32(cx, "num_sub_vectors")? {
        builder = builder.num_sub_vectors(ns);
    }
    if let Some(max_iters) = obj.get_opt_u32(cx, "max_iters")? {
        builder = builder.max_iterations(max_iters);
    }
    Ok(builder)
}
