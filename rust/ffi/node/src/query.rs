use std::convert::TryFrom;
use std::ops::Deref;

use arrow_array::Float32Array;
use futures::{TryFutureExt, TryStreamExt};
use lance_linalg::distance::MetricType;
use neon::context::FunctionContext;
use neon::handle::Handle;
use neon::prelude::*;
use neon::types::buffer::TypedArray;

use crate::arrow::record_batch_to_buffer;
use crate::error::ResultExt;
use crate::neon_ext::js_object_ext::JsObjectExt;
use crate::table::JsTable;
use crate::{convert, runtime};

pub(crate) struct JsQuery {}

impl JsQuery {
    pub(crate) fn js_search(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let js_table = cx.this().downcast_or_throw::<JsBox<JsTable>, _>(&mut cx)?;
        let query_obj = cx.argument::<JsObject>(0)?;

        let limit = query_obj
            .get_opt::<JsNumber, _, _>(&mut cx, "_limit")?
            .map(|value| {
                let limit = value.value(&mut cx);
                if limit <= 0.0 {
                    panic!("Limit must be a positive integer");
                }
                limit as u64
            });
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

        let prefilter = query_obj
            .get::<JsBoolean, _, _>(&mut cx, "_prefilter")?
            .value(&mut cx);

        let is_electron = cx
            .argument::<JsBoolean>(1)
            .or_throw(&mut cx)?
            .value(&mut cx);

        let rt = runtime(&mut cx)?;

        let (deferred, promise) = cx.promise();
        let channel = cx.channel();
        let query_vector = query_obj.get_opt::<JsArray, _, _>(&mut cx, "_queryVector")?;
        let table = js_table.table.clone();
        let query = query_vector.map(|q| convert::js_array_to_vec(q.deref(), &mut cx));

        rt.spawn(async move {
            let mut builder = table
                .search(query.map(Float32Array::from))
                .refine_factor(refine_factor)
                .nprobes(nprobes)
                .filter(filter)
                .metric_type(metric_type)
                .select(select)
                .prefilter(prefilter);
            if let Some(limit) = limit {
                builder = builder.limit(limit as usize);
            };

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
                Self::new_js_buffer(buffer, &mut cx, is_electron)
            });
        });
        Ok(promise)
    }

    // Creates a new JsBuffer from a rust buffer with a special logic for electron
    fn new_js_buffer<'a>(
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
}
