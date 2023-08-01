use std::convert::TryFrom;
use std::ops::Deref;

use arrow_array::Float32Array;
use futures::{TryFutureExt, TryStreamExt};
use lance::index::vector::MetricType;
use neon::prelude::*;
use neon::context::FunctionContext;
use neon::handle::Handle;

use crate::{convert, runtime};
use crate::arrow::record_batch_to_buffer;
use crate::error::ResultExt;
use crate::neon_ext::js_object_ext::JsObjectExt;
use crate::table::JsTable;

pub(crate) struct JsQuery {

}

impl JsQuery {
    pub(crate) fn js_search(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let js_table = cx.this().downcast_or_throw::<JsBox<JsTable>, _>(&mut cx)?;
        let query_obj = cx.argument::<JsObject>(0)?;

        let limit = query_obj
            .get::<JsNumber, _, _>(&mut cx, "_limit")?
            .value(&mut cx);
        let select = query_obj
            .get_opt::<JsArray, _, _>(&mut cx, "_select")?
            .map(|arr| {
                let js_array = arr.deref();
                let mut projection_vec: Vec<String> = Vec::new();
                for i in 0..js_array.len(&mut cx) {
                    let entry: Handle<JsString> = js_array.get(&mut cx, i).unwrap();
                    projection_vec.push(entry.value(&mut cx));
                }
                projection_vec
            });
        let filter = query_obj
            .get_opt::<JsString, _, _>(&mut cx, "_filter")?
            .map(|s| s.value(&mut cx));
        let refine_factor = query_obj
            .get_opt_u32(&mut cx, "_refineFactor")
            .or_throw(&mut cx)?;
        let nprobes = query_obj.get_usize(&mut cx, "_nprobes").or_throw(&mut cx)?;
        let metric_type = query_obj
            .get_opt::<JsString, _, _>(&mut cx, "_metricType")?
            .map(|s| s.value(&mut cx))
            .map(|s| MetricType::try_from(s.as_str()).unwrap());

        let rt = runtime(&mut cx)?;

        let (deferred, promise) = cx.promise();
        let query_vector = query_obj.get::<JsArray, _, _>(&mut cx, "_queryVector")?;
        let query = convert::js_array_to_vec(query_vector.deref(), &mut cx);

        js_table
            .send(deferred, move |table, channel, deferred| {
                rt.block_on(async move {
                    let builder = table
                        .search(Float32Array::from(query))
                        .limit(limit as usize)
                        .refine_factor(refine_factor)
                        .nprobes(nprobes)
                        .filter(filter)
                        .metric_type(metric_type)
                        .select(select);
                    let record_batch_stream = builder.execute();
                    let results = record_batch_stream
                        .and_then(|stream| {
                            stream
                                .try_collect::<Vec<_>>()
                                .map_err(vectordb::error::Error::from)
                        })
                        .await;

                    deferred.settle_with(&channel, move |mut cx| {
                        let results = results.or_throw(&mut cx)?;
                        let buffer = record_batch_to_buffer(results).or_throw(&mut cx)?;
                        Ok(JsBuffer::external(&mut cx, buffer))
                    });
                });
            })
            .or_throw(&mut cx)?;
        Ok(promise)
    }
}