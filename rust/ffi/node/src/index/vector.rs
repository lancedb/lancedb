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

use std::convert::TryFrom;

use lance::index::vector::ivf::IvfBuildParams;
use lance::index::vector::pq::PQBuildParams;
use lance::index::vector::MetricType;
use neon::context::FunctionContext;
use neon::prelude::*;

use vectordb::index::vector::{IvfPQIndexBuilder, VectorIndexBuilder};

use crate::error::Error::InvalidIndexType;
use crate::error::ResultExt;
use crate::neon_ext::js_object_ext::JsObjectExt;
use crate::{runtime, JsTable};

pub(crate) fn table_create_vector_index(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let js_table = cx.this().downcast_or_throw::<JsBox<JsTable>, _>(&mut cx)?;
    let index_params = cx.argument::<JsObject>(0)?;
    let index_params_builder = get_index_params_builder(&mut cx, index_params).or_throw(&mut cx)?;

    let rt = runtime(&mut cx)?;
    let channel = cx.channel();

    let (deferred, promise) = cx.promise();
    let table = js_table.table.clone();

    rt.block_on(async move {
        let add_result = table
            .lock()
            .unwrap()
            .create_index(&index_params_builder)
            .await;

        deferred.settle_with(&channel, move |mut cx| {
            add_result
                .map(|_| cx.undefined())
                .or_else(|err| cx.throw_error(err.to_string()))
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

            obj.get_opt::<JsString, _, _>(cx, "metric_type")?
                .map(|s| MetricType::try_from(s.value(cx).as_str()))
                .map(|mt| {
                    let metric_type = mt.unwrap();
                    index_builder.metric_type(metric_type);
                    pq_params.metric_type = metric_type;
                });

            let num_partitions = obj.get_opt_usize(cx, "num_partitions")?;
            let max_iters = obj.get_opt_usize(cx, "max_iters")?;

            num_partitions.map(|np| {
                let max_iters = max_iters.unwrap_or(50);
                let ivf_params = IvfBuildParams {
                    num_partitions: np,
                    max_iters,
                    centroids: None,
                };
                index_builder.ivf_params(ivf_params)
            });

            obj.get_opt::<JsBoolean, _, _>(cx, "use_opq")?
                .map(|s| pq_params.use_opq = s.value(cx));

            obj.get_opt_usize(cx, "num_sub_vectors")?
                .map(|s| pq_params.num_sub_vectors = s);

            obj.get_opt_usize(cx, "num_bits")?
                .map(|s| pq_params.num_bits = s);

            obj.get_opt_usize(cx, "max_iters")?
                .map(|s| pq_params.max_iters = s);

            obj.get_opt_usize(cx, "max_opq_iters")?
                .map(|s| pq_params.max_opq_iters = s);

            obj.get_opt::<JsBoolean, _, _>(cx, "replace")?
                .map(|s| index_builder.replace(s.value(cx)));

            Ok(index_builder)
        }
        index_type => Err(InvalidIndexType {
            index_type: index_type.into(),
        }),
    }
}
