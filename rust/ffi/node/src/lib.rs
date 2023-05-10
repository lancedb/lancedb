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

mod convert;

use std::ops::Deref;
use std::sync::Arc;

use arrow_array::Float32Array;
use arrow_ipc::writer::FileWriter;
use futures::TryStreamExt;
use neon::prelude::*;
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;

use vectordb::database::Database;
use vectordb::table::Table;

struct JsDatabase {
    database: Arc<Database>,
}

struct JsTable {
    table: Arc<Table>,
}

impl Finalize for JsDatabase {}

impl Finalize for JsTable {}

fn runtime<'a, C: Context<'a>>(cx: &mut C) -> NeonResult<&'static Runtime> {
    static RUNTIME: OnceCell<Runtime> = OnceCell::new();

    RUNTIME.get_or_try_init(|| Runtime::new().or_else(|err| cx.throw_error(err.to_string())))
}

fn database_new(mut cx: FunctionContext) -> JsResult<JsBox<JsDatabase>> {
    let path = cx.argument::<JsString>(0)?.value(&mut cx);
    let db = JsDatabase {
        database: Arc::new(Database::connect(path).or_else(|err| cx.throw_error(err.to_string()))?),
    };
    Ok(cx.boxed(db))
}

fn database_table_names(mut cx: FunctionContext) -> JsResult<JsArray> {
    let db = cx
        .this()
        .downcast_or_throw::<JsBox<JsDatabase>, _>(&mut cx)?;
    let tables = db
        .database
        .table_names()
        .or_else(|err| cx.throw_error(err.to_string()))?;
    convert::vec_str_to_array(&tables, &mut cx)
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
        let table_rst = database.open_table(table_name).await;

        deferred.settle_with(&channel, move |mut cx| {
            let table = Arc::new(table_rst.or_else(|err| cx.throw_error(err.to_string()))?);
            Ok(cx.boxed(JsTable { table }))
        });
    });
    Ok(promise)
}

fn table_search(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let js_table = cx.this().downcast_or_throw::<JsBox<JsTable>, _>(&mut cx)?;
    let query_vector = cx.argument::<JsArray>(0)?; //. .as_value(&mut cx);
    let limit = cx.argument::<JsNumber>(1)?.value(&mut cx);

    let rt = runtime(&mut cx)?;
    let channel = cx.channel();

    let (deferred, promise) = cx.promise();
    let table = js_table.table.clone();
    let query = convert::js_array_to_vec(query_vector.deref(), &mut cx);

    rt.spawn(async move {
        let builder = table
            .search(Float32Array::from(query))
            .limit(limit as usize);
        let results = builder
            .execute()
            .await
            .unwrap() // FIXME unwrap
            .try_collect::<Vec<_>>()
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

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("databaseNew", database_new)?;
    cx.export_function("databaseTableNames", database_table_names)?;
    cx.export_function("databaseOpenTable", database_open_table)?;
    cx.export_function("tableSearch", table_search)?;
    Ok(())
}
