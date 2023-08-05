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

use std::sync::mpsc;
use std::thread;
use arrow_array::RecordBatchIterator;
use lance::dataset::{WriteMode, WriteParams};
use lance::io::object_store::ObjectStoreParams;

use neon::{prelude::*, types::Deferred};
use neon::types::buffer::TypedArray;
use crate::arrow::arrow_buffer_to_record_batch;

use crate::error::{Error, Result, ResultExt};
use crate::{get_aws_creds, JsDatabase, runtime};

type TableCallback = Box<dyn FnOnce(&mut vectordb::Table, &Channel, Deferred) + Send>;

// Wraps a LanceDB table into a channel, allowing concurrent access
pub(crate) struct JsTable {
    tx: mpsc::Sender<JsTableMessage>,
}

impl Finalize for JsTable {}

// Messages sent on the table channel
pub(crate) enum JsTableMessage {
    // Promise to resolve and callback to be executed
    Callback(Deferred, TableCallback),
    // Forces to shutdown the thread
    Close,
}

impl JsTable {
    pub(crate) fn new<'a, C>(cx: &mut C, mut table: vectordb::Table) -> Result<Self>
    where
        C: Context<'a>,
    {
        // Creates a mpsc Channel to receive messages  / commands from Javascript
        let (tx, rx) = mpsc::channel::<JsTableMessage>();
        let channel = cx.channel();

        // Spawn a new thread to receive messages without blocking the main JS thread
        thread::spawn(move || {
            // Runs until the channel is closed
            while let Ok(message) = rx.recv() {
                match message {
                    JsTableMessage::Callback(deferred, f) => {
                        f(&mut table, &channel, deferred);
                    },
                    JsTableMessage::Close => break
                }
            }
        });

        Ok(Self { tx })
    }

    // It is not necessary to call `close` since the database will be closed when the wrapping
    // `JsBox` is garbage collected. However, calling `close` allows the process to exit
    // immediately instead of waiting on garbage collection. This is useful in tests.
    pub(crate) fn close(&self) -> Result<()> {
        self.tx.send(JsTableMessage::Close)
            .map_err(Error::from)
    }

    pub(crate) fn send(
        &self,
        deferred: Deferred,
        callback: impl FnOnce(&mut vectordb::Table, &Channel, Deferred) + Send + 'static,
    ) -> Result<()> {
        self.tx
            .send(JsTableMessage::Callback(deferred, Box::new(callback)))
            .map_err(Error::from)
    }
}

impl JsTable {

    pub(crate) fn js_create(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let db = cx
            .this()
            .downcast_or_throw::<JsBox<JsDatabase>, _>(&mut cx)?;
        let table_name = cx.argument::<JsString>(0)?.value(&mut cx);
        let buffer = cx.argument::<JsBuffer>(1)?;
        let (batches, schema) = arrow_buffer_to_record_batch(buffer.as_slice(&mut cx)).or_throw(&mut cx)?;

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

        rt.spawn(async move {
            let batch_reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
            let table_rst = database
                .create_table(&table_name, batch_reader, Some(params))
                .await;

            deferred.settle_with(&channel, move |mut cx| {
                let table = table_rst.or_throw(&mut cx)?;
                let js_table = JsTable::new(&mut cx, table).or_throw(&mut cx)?;
                Ok(cx.boxed(js_table))
            });
        });
        Ok(promise)
    }

    pub(crate) fn js_add(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let js_table = cx.this().downcast_or_throw::<JsBox<JsTable>, _>(&mut cx)?;
        let buffer = cx.argument::<JsBuffer>(0)?;
        let write_mode = cx.argument::<JsString>(1)?.value(&mut cx);
        let (batches, schema) = arrow_buffer_to_record_batch(buffer.as_slice(&mut cx)).or_throw(&mut cx)?;
        let rt = runtime(&mut cx)?;

        let (deferred, promise) = cx.promise();
        let write_mode = match write_mode.as_str() {
            "create" => WriteMode::Create,
            "append" => WriteMode::Append,
            "overwrite" => WriteMode::Overwrite,
            s =>  return cx.throw_error(format!("invalid write mode {}", s)),
        };
        let aws_creds = match get_aws_creds(&mut cx, 2) {
            Ok(creds) => creds,
            Err(err) => return err,
        };

        let params = WriteParams {
            store_params: Some(ObjectStoreParams {
                aws_credentials: aws_creds,
                ..ObjectStoreParams::default()
            }),
            mode: write_mode,
            ..WriteParams::default()
        };

        js_table
            .send(deferred, move |table, channel, deferred| {
                rt.block_on(async move {
                    let batch_reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
                    let add_result = table.add(batch_reader, Some(params)).await;

                    deferred.settle_with(&channel, move |mut cx| {
                        let _added = add_result.or_throw(&mut cx)?;
                        Ok(cx.boolean(true))
                    });
                });
            })
            .or_throw(&mut cx)?;
        Ok(promise)
    }

    pub(crate) fn js_count_rows(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let js_table = cx.this().downcast_or_throw::<JsBox<JsTable>, _>(&mut cx)?;
        let rt = runtime(&mut cx)?;

        let (deferred, promise) = cx.promise();

        js_table
            .send(deferred, move |table, channel, deferred| {
                rt.block_on(async move {
                    let num_rows_result = table.count_rows().await;

                    deferred.settle_with(&channel, move |mut cx| {
                        let num_rows = num_rows_result.or_throw(&mut cx)?;
                        Ok(cx.number(num_rows as f64))
                    });
                });
            })
            .or_throw(&mut cx)?;
        Ok(promise)
    }

    pub(crate) fn js_delete(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let js_table = cx.this().downcast_or_throw::<JsBox<JsTable>, _>(&mut cx)?;
        let rt = runtime(&mut cx)?;
        let (deferred, promise) = cx.promise();
        let predicate = cx.argument::<JsString>(0)?.value(&mut cx);

        js_table
            .send(deferred, move |table, channel, deferred| {
                let delete_result = rt.block_on(async move { table.delete(&predicate).await });

                deferred.settle_with(&channel, move |mut cx| {
                    delete_result.or_throw(&mut cx)?;
                    Ok(cx.undefined())
                });
            })
            .or_throw(&mut cx)?;
        Ok(promise)
    }

    pub(crate) fn js_close(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        cx.this()
            .downcast_or_throw::<JsBox<JsTable>, _>(&mut cx)?
            .close()
            .or_throw(&mut cx)?;
        Ok(cx.undefined())
    }
}
