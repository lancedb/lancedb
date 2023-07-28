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

use arrow_array::{Float32Array, RecordBatchIterator};
use async_trait::async_trait;
use futures::{TryFutureExt, TryStreamExt};
use lance::dataset::{WriteMode, WriteParams};
use lance::index::vector::MetricType;
use lance::io::object_store::ObjectStoreParams;
use neon::prelude::*;
use neon::types::buffer::TypedArray;
use object_store::aws::{AwsCredential, AwsCredentialProvider};
use object_store::CredentialProvider;
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;

use vectordb::database::Database;
use vectordb::table::{ReadParams, Table};

use crate::arrow::{arrow_buffer_to_record_batch, record_batch_to_buffer};
use crate::error::ResultExt;
use crate::neon_ext::js_object_ext::JsObjectExt;

mod arrow;
mod convert;
mod error;
mod index;
mod neon_ext;

struct JsDatabase {
    database: Arc<Database>,
}

impl Finalize for JsDatabase {}

struct JsTable {
    table: Arc<Mutex<Table>>,
}

impl Finalize for JsTable {}

// TODO: object_store didn't export this type so I copied it.
// Make a request to object_store to export this type
#[derive(Debug)]
pub struct StaticCredentialProvider<T> {
    credential: Arc<T>,
}

impl<T> StaticCredentialProvider<T> {
    pub fn new(credential: T) -> Self {
        Self {
            credential: Arc::new(credential),
        }
    }
}

#[async_trait]
impl<T> CredentialProvider for StaticCredentialProvider<T>
where
    T: std::fmt::Debug + Send + Sync,
{
    type Credential = T;

    async fn get_credential(&self) -> object_store::Result<Arc<T>> {
        Ok(Arc::clone(&self.credential))
    }
}

fn runtime<'a, C: Context<'a>>(cx: &mut C) -> NeonResult<&'static Runtime> {
    static RUNTIME: OnceCell<Runtime> = OnceCell::new();
    static LOG: OnceCell<()> = OnceCell::new();

    LOG.get_or_init(|| env_logger::init());

    RUNTIME.get_or_try_init(|| Runtime::new().or_throw(cx))
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
                database: Arc::new(database.or_throw(&mut cx)?),
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
            let tables = tables_rst.or_throw(&mut cx)?;
            let table_names = convert::vec_str_to_array(&tables, &mut cx);
            table_names
        });
    });
    Ok(promise)
}

fn get_aws_creds<T>(
    cx: &mut FunctionContext,
    arg_starting_location: i32,
) -> Result<Option<AwsCredentialProvider>, NeonResult<T>> {
    let secret_key_id = cx
        .argument_opt(arg_starting_location)
        .map(|arg| arg.downcast_or_throw::<JsString, FunctionContext>(cx).ok())
        .flatten()
        .map(|v| v.value(cx));

    let secret_key = cx
        .argument_opt(arg_starting_location + 1)
        .map(|arg| arg.downcast_or_throw::<JsString, FunctionContext>(cx).ok())
        .flatten()
        .map(|v| v.value(cx));

    let temp_token = cx
        .argument_opt(arg_starting_location + 2)
        .map(|arg| arg.downcast_or_throw::<JsString, FunctionContext>(cx).ok())
        .flatten()
        .map(|v| v.value(cx));

    match (secret_key_id, secret_key, temp_token) {
        (Some(key_id), Some(key), optional_token) => Ok(Some(Arc::new(
            StaticCredentialProvider::new(AwsCredential {
                key_id: key_id,
                secret_key: key,
                token: optional_token,
            }),
        ))),
        (None, None, None) => Ok(None),
        _ => Err(cx.throw_error("Invalid credentials configuration")),
    }
}

fn database_open_table(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let db = cx
        .this()
        .downcast_or_throw::<JsBox<JsDatabase>, _>(&mut cx)?;
    let table_name = cx.argument::<JsString>(0)?.value(&mut cx);

    let aws_creds = match get_aws_creds(&mut cx, 1) {
        Ok(creds) => creds,
        Err(err) => return err,
    };

    let params = ReadParams {
        store_options: Some(ObjectStoreParams {
            aws_credentials: aws_creds,
            ..ObjectStoreParams::default()
        }),
        ..ReadParams::default()
    };

    let rt = runtime(&mut cx)?;
    let channel = cx.channel();
    let database = db.database.clone();

    let (deferred, promise) = cx.promise();
    rt.spawn(async move {
        let table_rst = database.open_table_with_params(&table_name, &params).await;

        deferred.settle_with(&channel, move |mut cx| {
            let table = Arc::new(Mutex::new(table_rst.or_throw(&mut cx)?));
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
            result.or_throw(&mut cx)?;
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
        .get_opt_u32(&mut cx, "_refineFactor")
        .or_throw(&mut cx)?;
    let nprobes = query_obj.get_usize(&mut cx, "_nprobes").or_throw(&mut cx)?;
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
    Ok(promise)
}

fn table_create(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let db = cx
        .this()
        .downcast_or_throw::<JsBox<JsDatabase>, _>(&mut cx)?;
    let table_name = cx.argument::<JsString>(0)?.value(&mut cx);
    let buffer = cx.argument::<JsBuffer>(1)?;
    let batches = arrow_buffer_to_record_batch(buffer.as_slice(&mut cx)).or_throw(&mut cx)?;
    let schema = batches[0].schema();

    // Write mode
    let mode = match cx.argument::<JsString>(2)?.value(&mut cx).as_str() {
        "overwrite" => WriteMode::Overwrite,
        "append" => WriteMode::Append,
        "create" => WriteMode::Create,
        _ => return cx.throw_error("Table::create only supports 'overwrite' and 'create' modes"),
    };

    let rt = runtime(&mut cx)?;
    let channel = cx.channel();

    let (deferred, promise) = cx.promise();
    let database = db.database.clone();

    let aws_creds = match get_aws_creds(&mut cx, 3) {
        Ok(creds) => creds,
        Err(err) => return err,
    };

    let params = WriteParams {
        store_params: Some(ObjectStoreParams {
            aws_credentials: aws_creds,
            ..ObjectStoreParams::default()
        }),
        mode: mode,
        ..WriteParams::default()
    };

    rt.block_on(async move {
        let batch_reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        let table_rst = database
            .create_table(&table_name, batch_reader, Some(params))
            .await;

        deferred.settle_with(&channel, move |mut cx| {
            let table = Arc::new(Mutex::new(table_rst.or_throw(&mut cx)?));
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

    let batches = arrow_buffer_to_record_batch(buffer.as_slice(&mut cx)).or_throw(&mut cx)?;
    let schema = batches[0].schema();

    let rt = runtime(&mut cx)?;
    let channel = cx.channel();

    let (deferred, promise) = cx.promise();
    let table = js_table.table.clone();
    let write_mode = write_mode_map.get(write_mode.as_str()).cloned();

    let aws_creds = match get_aws_creds(&mut cx, 2) {
        Ok(creds) => creds,
        Err(err) => return err,
    };

    let params = WriteParams {
        store_params: Some(ObjectStoreParams {
            aws_credentials: aws_creds,
            ..ObjectStoreParams::default()
        }),
        mode: write_mode.unwrap_or(WriteMode::Append),
        ..WriteParams::default()
    };

    rt.block_on(async move {
        let batch_reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        let add_result = table.lock().unwrap().add(batch_reader, Some(params)).await;

        deferred.settle_with(&channel, move |mut cx| {
            let _added = add_result.or_throw(&mut cx)?;
            Ok(cx.boolean(true))
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
            let num_rows = num_rows_result.or_throw(&mut cx)?;
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
        delete_result.or_throw(&mut cx)?;
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
