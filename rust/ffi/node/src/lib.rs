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

use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

use arrow_array::{Float32Array, RecordBatchReader};
use arrow_ipc::writer::FileWriter;
use futures::{TryFutureExt, TryStreamExt};
use lance::arrow::RecordBatchBuffer;
use lance::dataset::{WriteMode, WriteParams};
use lance::index::vector::MetricType;
use neon::prelude::*;
use neon::types::buffer::TypedArray;
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;

use vectordb::database::Database;
use vectordb::error::Error;
use vectordb::table::Table;

use crate::arrow::arrow_buffer_to_record_batch;

mod arrow;
mod convert;
mod index;

struct JsDatabase {
    database: Arc<Database>,
}

impl Finalize for JsDatabase {}

struct JsTable {
    table: Arc<Mutex<Table>>,
}

impl Finalize for JsTable {}

fn runtime<'a, C: Context<'a>>(cx: &mut C) -> NeonResult<&'static Runtime> {
    static RUNTIME: OnceCell<Runtime> = OnceCell::new();

    RUNTIME.get_or_try_init(|| Runtime::new().or_else(|err| cx.throw_error(err.to_string())))
}

fn database_new(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let path = cx.argument::<JsString>(0)?.value(&mut cx);

    let rt = runtime(&mut cx)?;
    let channel = cx.channel();
    let (deferred, promise) = cx.promise();

    rt.spawn(async move {
        let database = Database::connect(&path).await;

        deferred.settle_with(&channel, move |mut cx| {
            let db = JsDatabase {
                database: Arc::new(database.or_else(|err| cx.throw_error(err.to_string()))?),
            };
            Ok(cx.boxed(db))
        });
    });
    Ok(promise)
}

fn database_table_names(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let db = cx
        .this()
        .downcast_or_throw::<JsBox<JsDatabase>, _>(&mut cx)?;

    let rt = runtime(&mut cx)?;
    let (deferred, promise) = cx.promise();
    let channel = cx.channel();
    let database = db.database.clone();

    rt.spawn(async move {
        let tables_rst = database.table_names().await;

        deferred.settle_with(&channel, move |mut cx| {
            let tables = tables_rst.or_else(|err| cx.throw_error(err.to_string()))?;
            let table_names = convert::vec_str_to_array(&tables, &mut cx);
            table_names
        });
    });
    Ok(promise)
}

fn database_open_table(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let db = cx
        .this()
        .downcast_or_throw::<JsBox<JsDatabase>, _>(&mut cx)?;
    let table_name = cx.argument::<JsString>(0)?.value(&mut cx);

    let rt = runtime(&mut cx)?;
    let channel = cx.channel();
    let database = db.database.clone();

    let (deferred, promise) = cx.promise();
    rt.spawn(async move {
        let table_rst = database.open_table(&table_name).await;

        deferred.settle_with(&channel, move |mut cx| {
            let table = Arc::new(Mutex::new(
                table_rst.or_else(|err| cx.throw_error(err.to_string()))?,
            ));
            Ok(cx.boxed(JsTable { table }))
        });
    });
    Ok(promise)
}

fn database_drop_table(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let db = cx
        .this()
        .downcast_or_throw::<JsBox<JsDatabase>, _>(&mut cx)?;
    let table_name = cx.argument::<JsString>(0)?.value(&mut cx);

    let rt = runtime(&mut cx)?;
    let channel = cx.channel();
    let database = db.database.clone();

    let (deferred, promise) = cx.promise();
    rt.spawn(async move {
        let result = database.drop_table(&table_name).await;
        deferred.settle_with(&channel, move |mut cx| {
            result.or_else(|err| cx.throw_error(err.to_string()))?;
            Ok(cx.null())
        });
    });
    Ok(promise)
}

fn table_search(mut cx: FunctionContext) -> JsResult<JsPromise> {
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
        .get_opt::<JsNumber, _, _>(&mut cx, "_refineFactor")?
        .map(|s| s.value(&mut cx))
        .map(|i| i as u32);
    let nprobes = query_obj
        .get::<JsNumber, _, _>(&mut cx, "_nprobes")?
        .value(&mut cx) as usize;
    let metric_type = query_obj
        .get_opt::<JsString, _, _>(&mut cx, "_metricType")?
        .map(|s| s.value(&mut cx))
        .map(|s| MetricType::try_from(s.as_str()).unwrap());

    let rt = runtime(&mut cx)?;
    let channel = cx.channel();

    let (deferred, promise) = cx.promise();
    let table = js_table.table.clone();
    let query_vector = query_obj.get::<JsArray, _, _>(&mut cx, "_queryVector")?;
    let query = convert::js_array_to_vec(query_vector.deref(), &mut cx);

    rt.spawn(async move {
        let builder = table
            .lock()
            .unwrap()
            .search(Float32Array::from(query))
            .limit(limit as usize)
            .refine_factor(refine_factor)
            .nprobes(nprobes)
            .filter(filter)
            .metric_type(metric_type)
            .select(select);
        let record_batch_stream = builder.execute();
        let results = record_batch_stream
            .and_then(|stream| stream.try_collect::<Vec<_>>().map_err(Error::from))
            .await;

        deferred.settle_with(&channel, move |mut cx| {
            let results = results.or_else(|err| cx.throw_error(err.to_string()))?;
            let vector: Vec<u8> = Vec::new();

            if results.is_empty() {
                return cx.buffer(0);
            }

            let schema = results.get(0).unwrap().schema();
            let mut fr = FileWriter::try_new(vector, schema.deref())
                .or_else(|err| cx.throw_error(err.to_string()))?;

            for batch in results.iter() {
                fr.write(batch)
                    .or_else(|err| cx.throw_error(err.to_string()))?;
            }
            fr.finish().or_else(|err| cx.throw_error(err.to_string()))?;
            let buf = fr
                .into_inner()
                .or_else(|err| cx.throw_error(err.to_string()))?;
            Ok(JsBuffer::external(&mut cx, buf))
        });
    });
    Ok(promise)
}

fn table_create(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let db = cx
        .this()
        .downcast_or_throw::<JsBox<JsDatabase>, _>(&mut cx)?;
    let table_name = cx.argument::<JsString>(0)?.value(&mut cx);
    let buffer = cx.argument::<JsBuffer>(1)?;
    let batches = arrow_buffer_to_record_batch(buffer.as_slice(&mut cx));

    // Write mode
    let mode = match cx.argument::<JsString>(2)?.value(&mut cx).as_str() {
        "overwrite" => WriteMode::Overwrite,
        "append" => WriteMode::Append,
        "create" => WriteMode::Create,
        _ => return cx.throw_error("Table::create only supports 'overwrite' and 'create' modes")
    };
    let mut params = WriteParams::default();
    params.mode = mode;

    let rt = runtime(&mut cx)?;
    let channel = cx.channel();

    let (deferred, promise) = cx.promise();
    let database = db.database.clone();

    rt.block_on(async move {
        let batch_reader: Box<dyn RecordBatchReader> = Box::new(RecordBatchBuffer::new(batches));
        let table_rst = database.create_table(&table_name, batch_reader, Some(params)).await;

        deferred.settle_with(&channel, move |mut cx| {
            let table = Arc::new(Mutex::new(
                table_rst.or_else(|err| cx.throw_error(err.to_string()))?,
            ));
            Ok(cx.boxed(JsTable { table }))
        });
    });
    Ok(promise)
}

fn table_add(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let write_mode_map: HashMap<&str, WriteMode> = HashMap::from([
        ("create", WriteMode::Create),
        ("append", WriteMode::Append),
        ("overwrite", WriteMode::Overwrite),
    ]);

    let js_table = cx.this().downcast_or_throw::<JsBox<JsTable>, _>(&mut cx)?;
    let buffer = cx.argument::<JsBuffer>(0)?;
    let write_mode = cx.argument::<JsString>(1)?.value(&mut cx);
    let batches = arrow_buffer_to_record_batch(buffer.as_slice(&mut cx));

    let rt = runtime(&mut cx)?;
    let channel = cx.channel();

    let (deferred, promise) = cx.promise();
    let table = js_table.table.clone();
    let write_mode = write_mode_map.get(write_mode.as_str()).cloned();

    rt.block_on(async move {
        let batch_reader: Box<dyn RecordBatchReader> = Box::new(RecordBatchBuffer::new(batches));
        let add_result = table.lock().unwrap().add(batch_reader, write_mode).await;

        deferred.settle_with(&channel, move |mut cx| {
            let added = add_result.or_else(|err| cx.throw_error(err.to_string()))?;
            Ok(cx.number(added as f64))
        });
    });
    Ok(promise)
}

fn table_count_rows(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let js_table = cx.this().downcast_or_throw::<JsBox<JsTable>, _>(&mut cx)?;
    let rt = runtime(&mut cx)?;
    let channel = cx.channel();

    let (deferred, promise) = cx.promise();
    let table = js_table.table.clone();

    rt.block_on(async move {
        let num_rows_result = table.lock().unwrap().count_rows().await;

        deferred.settle_with(&channel, move |mut cx| {
            let num_rows = num_rows_result.or_else(|err| cx.throw_error(err.to_string()))?;
            Ok(cx.number(num_rows as f64))
        });
    });
    Ok(promise)
}

fn table_delete(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let js_table = cx.this().downcast_or_throw::<JsBox<JsTable>, _>(&mut cx)?;
    let rt = runtime(&mut cx)?;
    let channel = cx.channel();

    let (deferred, promise) = cx.promise();
    let table = js_table.table.clone();

    let predicate = cx.argument::<JsString>(0)?.value(&mut cx);

    let delete_result = rt.block_on(async move { table.lock().unwrap().delete(&predicate).await });

    deferred.settle_with(&channel, move |mut cx| {
        delete_result.or_else(|err| cx.throw_error(err.to_string()))?;
        Ok(cx.undefined())
    });

    Ok(promise)
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("databaseNew", database_new)?;
    cx.export_function("databaseTableNames", database_table_names)?;
    cx.export_function("databaseOpenTable", database_open_table)?;
    cx.export_function("databaseDropTable", database_drop_table)?;
    cx.export_function("tableSearch", table_search)?;
    cx.export_function("tableCreate", table_create)?;
    cx.export_function("tableAdd", table_add)?;
    cx.export_function("tableCountRows", table_count_rows)?;
    cx.export_function("tableDelete", table_delete)?;
    cx.export_function(
        "tableCreateVectorIndex",
        index::vector::table_create_vector_index,
    )?;
    Ok(())
}
