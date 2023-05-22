use std::convert::TryFrom;

use lance::index::vector::ivf::IvfBuildParams;
use lance::index::vector::pq::PQBuildParams;
use lance::index::vector::MetricType;
use neon::context::FunctionContext;
use neon::prelude::*;

use vectordb::index::vector::{IvfIndexBuilder, VectorIndexBuilder};

use crate::{runtime, JsTable};

pub(crate) fn table_create_vector_index(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let js_table = cx.this().downcast_or_throw::<JsBox<JsTable>, _>(&mut cx)?;
    let index_params = cx.argument::<JsObject>(0)?;
    let index_params_builder = get_index_params_builder(&mut cx, index_params).unwrap();

    let rt = runtime(&mut cx)?;
    let channel = cx.channel();

    let (deferred, promise) = cx.promise();
    let table = js_table.table.clone();

    rt.block_on(async move {
        let add_result = table
            .lock()
            .unwrap()
            .create_idx(&index_params_builder)
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
) -> Result<impl VectorIndexBuilder, String> {
    let idx_type = obj
        .get::<JsString, _, _>(cx, "type")
        .map_err(|t| t.to_string())?
        .value(cx);

    match idx_type.as_str() {
        "ivf" => {
            let mut index_builder: IvfIndexBuilder = IvfIndexBuilder::new();
            let mut pq_params = PQBuildParams::default();

            obj.get_opt::<JsString, _, _>(cx, "column")
                .map_err(|t| t.to_string())?
                .map(|s| index_builder.column(s.value(cx)));

            obj.get_opt::<JsString, _, _>(cx, "index_name")
                .map_err(|t| t.to_string())?
                .map(|s| index_builder.index_name(s.value(cx)));

            obj.get_opt::<JsString, _, _>(cx, "metric_type")
                .map_err(|t| t.to_string())?
                .map(|s| MetricType::try_from(s.value(cx).as_str()))
                .map(|mt| {
                    let metric_type = mt.unwrap();
                    index_builder.metric_type(metric_type);
                    pq_params.metric_type = metric_type;
                });

            let num_partitions = obj
                .get_opt::<JsNumber, _, _>(cx, "num_partitions")
                .map_err(|t| t.to_string())?
                .map(|s| s.value(cx) as usize);

            let max_iters = obj
                .get_opt::<JsNumber, _, _>(cx, "max_iters")
                .map_err(|t| t.to_string())?
                .map(|s| s.value(cx) as usize);

            num_partitions.map(|np| {
                let max_iters = max_iters.unwrap_or(50);
                let ivf_params = IvfBuildParams {
                    num_partitions: np,
                    max_iters,
                };
                index_builder.ivf_params(ivf_params)
            });

            obj.get_opt::<JsBoolean, _, _>(cx, "use_opq")
                .map_err(|t| t.to_string())?
                .map(|s| pq_params.use_opq = s.value(cx));

            obj.get_opt::<JsNumber, _, _>(cx, "num_sub_vectors")
                .map_err(|t| t.to_string())?
                .map(|s| pq_params.num_sub_vectors = s.value(cx) as usize);

            obj.get_opt::<JsNumber, _, _>(cx, "num_bits")
                .map_err(|t| t.to_string())?
                .map(|s| pq_params.num_bits = s.value(cx) as usize);

            obj.get_opt::<JsNumber, _, _>(cx, "max_iters")
                .map_err(|t| t.to_string())?
                .map(|s| pq_params.max_iters = s.value(cx) as usize);

            obj.get_opt::<JsNumber, _, _>(cx, "max_opq_iters")
                .map_err(|t| t.to_string())?
                .map(|s| pq_params.max_opq_iters = s.value(cx) as usize);

            Ok(index_builder)
        }
        t => Err(format!("{} is not a valid index type", t).to_string()),
    }
}
