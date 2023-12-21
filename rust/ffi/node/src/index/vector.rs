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

use lance_index::vector::{ivf::IvfBuildParams, pq::PQBuildParams};
use lance_linalg::distance::MetricType;
use neon::context::FunctionContext;
use neon::prelude::*;
use std::convert::TryFrom;

use vectordb::index::vector::{IvfPQIndexBuilder, VectorIndexBuilder};

use crate::error::Error::InvalidIndexType;
use crate::error::ResultExt;
use crate::neon_ext::js_object_ext::JsObjectExt;
use crate::runtime;
use crate::table::JsTable;

pub(crate) fn table_create_vector_index(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let js_table = cx.this().downcast_or_throw::<JsBox<JsTable>, _>(&mut cx)?;
    let index_params = cx.argument::<JsObject>(0)?;
    let index_params_builder = get_index_params_builder(&mut cx, index_params).or_throw(&mut cx)?;

    let rt = runtime(&mut cx)?;

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();
    let mut table = js_table.table.clone();

    rt.spawn(async move {
        let idx_result = table.create_index(&index_params_builder).await;

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
) -> crate::error::Result<impl VectorIndexBuilder> {
    let idx_type = obj.get::<JsString, _, _>(cx, "type")?.value(cx);

    match idx_type.as_str() {
        "ivf_pq" => {
            let mut index_builder: IvfPQIndexBuilder = IvfPQIndexBuilder::new();
            let mut pq_params = PQBuildParams::default();

            obj.get_opt::<JsString, _, _>(cx, "column")?
                .map(|s| index_builder.column(s.value(cx)));

            obj.get_opt::<JsString, _, _>(cx, "index_name")?
                .map(|s| index_builder.index_name(s.value(cx)));

            if let Some(metric_type) = obj.get_opt::<JsString, _, _>(cx, "metric_type")? {
                let metric_type = MetricType::try_from(metric_type.value(cx).as_str()).unwrap();
                index_builder.metric_type(metric_type);
            }

            let num_partitions = obj.get_opt_usize(cx, "num_partitions")?;
            let max_iters = obj.get_opt_usize(cx, "max_iters")?;

            num_partitions.map(|np| {
                let max_iters = max_iters.unwrap_or(50);
                let ivf_params = IvfBuildParams {
                    num_partitions: np,
                    max_iters,
                    ..Default::default()
                };
                index_builder.ivf_params(ivf_params)
            });

            if let Some(use_opq) = obj.get_opt::<JsBoolean, _, _>(cx, "use_opq")? {
                pq_params.use_opq = use_opq.value(cx);
            }

            if let Some(num_sub_vectors) = obj.get_opt_usize(cx, "num_sub_vectors")? {
                pq_params.num_sub_vectors = num_sub_vectors;
            }

            if let Some(num_bits) = obj.get_opt_usize(cx, "num_bits")? {
                pq_params.num_bits = num_bits;
            }

            if let Some(max_iters) = obj.get_opt_usize(cx, "max_iters")? {
                pq_params.max_iters = max_iters;
            }

            if let Some(max_opq_iters) = obj.get_opt_usize(cx, "max_opq_iters")? {
                pq_params.max_opq_iters = max_opq_iters;
            }

            if let Some(replace) = obj.get_opt::<JsBoolean, _, _>(cx, "replace")? {
                index_builder.replace(replace.value(cx));
            }

            Ok(index_builder)
        }
        index_type => Err(InvalidIndexType {
            index_type: index_type.into(),
        }),
    }
}
