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

use std::ops::Deref;

use arrow_array::{RecordBatch, RecordBatchIterator};
use lance::dataset::optimize::CompactionOptions;
use lance::dataset::{ColumnAlteration, NewColumnTransform, WriteMode, WriteParams};
use lancedb::table::{OptimizeAction, WriteOptions};

use crate::arrow::{arrow_buffer_to_record_batch, record_batch_to_buffer};
use lancedb::table::Table as LanceDbTable;
use neon::prelude::*;
use neon::types::buffer::TypedArray;

use crate::error::ResultExt;
use crate::{convert, runtime, JsDatabase};

pub struct JsTable {
    pub table: LanceDbTable,
}

impl Finalize for JsTable {}

impl From<LanceDbTable> for JsTable {
    fn from(table: LanceDbTable) -> Self {
        Self { table }
    }
}

impl JsTable {
    pub(crate) fn js_create(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let db = cx
            .this()
            .downcast_or_throw::<JsBox<JsDatabase>, _>(&mut cx)?;
        let table_name = cx.argument::<JsString>(0)?.value(&mut cx);
        let buffer = cx.argument::<JsBuffer>(1)?;
        let (batches, schema) =
            arrow_buffer_to_record_batch(buffer.as_slice(&cx)).or_throw(&mut cx)?;

        // Write mode
        let mode = match cx.argument::<JsString>(2)?.value(&mut cx).as_str() {
            "overwrite" => WriteMode::Overwrite,
            "append" => WriteMode::Append,
            "create" => WriteMode::Create,
            _ => {
                return cx.throw_error("Table::create only supports 'overwrite' and 'create' modes")
            }
        };
        let params = WriteParams {
            mode,
            ..WriteParams::default()
        };

        let rt = runtime(&mut cx)?;
        let channel = cx.channel();

        let (deferred, promise) = cx.promise();
        let database = db.database.clone();

        rt.spawn(async move {
            let batch_reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
            let table_rst = database
                .create_table(&table_name, batch_reader)
                .write_options(WriteOptions {
                    lance_write_params: Some(params),
                })
                .execute()
                .await;

            deferred.settle_with(&channel, move |mut cx| {
                let table = table_rst.or_throw(&mut cx)?;
                Ok(cx.boxed(Self::from(table)))
            });
        });
        Ok(promise)
    }

    pub(crate) fn js_add(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let js_table = cx.this().downcast_or_throw::<JsBox<Self>, _>(&mut cx)?;
        let buffer = cx.argument::<JsBuffer>(0)?;
        let write_mode = cx.argument::<JsString>(1)?.value(&mut cx);
        let (batches, schema) =
            arrow_buffer_to_record_batch(buffer.as_slice(&cx)).or_throw(&mut cx)?;
        let rt = runtime(&mut cx)?;
        let channel = cx.channel();
        let table = js_table.table.clone();

        let (deferred, promise) = cx.promise();
        let write_mode = match write_mode.as_str() {
            "create" => WriteMode::Create,
            "append" => WriteMode::Append,
            "overwrite" => WriteMode::Overwrite,
            s => return cx.throw_error(format!("invalid write mode {}", s)),
        };

        let params = WriteParams {
            mode: write_mode,
            ..WriteParams::default()
        };

        rt.spawn(async move {
            let batch_reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
            let add_result = table
                .add(batch_reader)
                .write_options(WriteOptions {
                    lance_write_params: Some(params),
                })
                .execute()
                .await;

            deferred.settle_with(&channel, move |mut cx| {
                add_result.or_throw(&mut cx)?;
                Ok(cx.boxed(Self::from(table)))
            });
        });
        Ok(promise)
    }

    pub(crate) fn js_count_rows(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let js_table = cx.this().downcast_or_throw::<JsBox<Self>, _>(&mut cx)?;
        let filter = cx
            .argument_opt(0)
            .and_then(|filt| {
                if filt.is_a::<JsUndefined, _>(&mut cx) || filt.is_a::<JsNull, _>(&mut cx) {
                    None
                } else {
                    Some(
                        filt.downcast_or_throw::<JsString, _>(&mut cx)
                            .map(|js_filt| js_filt.deref().value(&mut cx)),
                    )
                }
            })
            .transpose()?;
        let rt = runtime(&mut cx)?;
        let (deferred, promise) = cx.promise();
        let channel = cx.channel();
        let table = js_table.table.clone();

        rt.spawn(async move {
            let num_rows_result = table.count_rows(filter).await;

            deferred.settle_with(&channel, move |mut cx| {
                let num_rows = num_rows_result.or_throw(&mut cx)?;
                Ok(cx.number(num_rows as f64))
            });
        });
        Ok(promise)
    }

    pub(crate) fn js_delete(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let js_table = cx.this().downcast_or_throw::<JsBox<Self>, _>(&mut cx)?;
        let rt = runtime(&mut cx)?;
        let (deferred, promise) = cx.promise();
        let predicate = cx.argument::<JsString>(0)?.value(&mut cx);
        let channel = cx.channel();
        let table = js_table.table.clone();

        rt.spawn(async move {
            let delete_result = table.delete(&predicate).await;

            deferred.settle_with(&channel, move |mut cx| {
                delete_result.or_throw(&mut cx)?;
                Ok(cx.boxed(Self::from(table)))
            })
        });
        Ok(promise)
    }

    pub(crate) fn js_merge_insert(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let js_table = cx.this().downcast_or_throw::<JsBox<Self>, _>(&mut cx)?;
        let rt = runtime(&mut cx)?;
        let (deferred, promise) = cx.promise();
        let channel = cx.channel();
        let table = js_table.table.clone();

        let key = cx.argument::<JsString>(0)?.value(&mut cx);
        let mut builder = table.merge_insert(&[&key]);
        if cx.argument::<JsBoolean>(1)?.value(&mut cx) {
            let filter = cx.argument_opt(2).unwrap();
            if filter.is_a::<JsNull, _>(&mut cx) {
                builder.when_matched_update_all(None);
            } else {
                let filter = filter
                    .downcast_or_throw::<JsString, _>(&mut cx)?
                    .deref()
                    .value(&mut cx);
                builder.when_matched_update_all(Some(filter));
            }
        }
        if cx.argument::<JsBoolean>(3)?.value(&mut cx) {
            builder.when_not_matched_insert_all();
        }
        if cx.argument::<JsBoolean>(4)?.value(&mut cx) {
            let filter = cx.argument_opt(5).unwrap();
            if filter.is_a::<JsNull, _>(&mut cx) {
                builder.when_not_matched_by_source_delete(None);
            } else {
                let filter = filter
                    .downcast_or_throw::<JsString, _>(&mut cx)?
                    .deref()
                    .value(&mut cx);
                builder.when_not_matched_by_source_delete(Some(filter));
            }
        }

        let buffer = cx.argument::<JsBuffer>(6)?;
        let (batches, schema) =
            arrow_buffer_to_record_batch(buffer.as_slice(&cx)).or_throw(&mut cx)?;

        rt.spawn(async move {
            let new_data = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
            let merge_insert_result = builder.execute(Box::new(new_data)).await;

            deferred.settle_with(&channel, move |mut cx| {
                merge_insert_result.or_throw(&mut cx)?;
                Ok(cx.boxed(Self::from(table)))
            })
        });
        Ok(promise)
    }

    pub(crate) fn js_update(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let js_table = cx.this().downcast_or_throw::<JsBox<Self>, _>(&mut cx)?;
        let table = js_table.table.clone();

        let rt = runtime(&mut cx)?;
        let (deferred, promise) = cx.promise();
        let channel = cx.channel();

        // create a vector of updates from the passed map
        let updates_arg = cx.argument::<JsObject>(1)?;
        let properties = updates_arg.get_own_property_names(&mut cx)?;
        let mut updates: Vec<(String, String)> =
            Vec::with_capacity(properties.len(&mut cx) as usize);

        let len_properties = properties.len(&mut cx);
        for i in 0..len_properties {
            let property = properties
                .get_value(&mut cx, i)?
                .downcast_or_throw::<JsString, _>(&mut cx)?;

            let value = updates_arg
                .get_value(&mut cx, property)?
                .downcast_or_throw::<JsString, _>(&mut cx)?;

            let property = property.value(&mut cx);
            let value = value.value(&mut cx);
            updates.push((property, value));
        }

        // get the filter/predicate if the user passed one
        let predicate = cx.argument_opt(0);
        let predicate = predicate.unwrap().downcast::<JsString, _>(&mut cx);
        let predicate = match predicate {
            Ok(_) => {
                let val = predicate.map(|s| s.value(&mut cx)).unwrap();
                Some(val)
            }
            Err(_) => {
                // if the predicate is not string, check it's null otherwise an invalid
                // type was passed
                cx.argument::<JsNull>(0)?;
                None
            }
        };

        rt.spawn(async move {
            let updates_arg = updates
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect::<Vec<_>>();

            let predicate = predicate.as_deref();

            let mut update_op = table.update();
            if let Some(predicate) = predicate {
                update_op = update_op.only_if(predicate);
            }
            for (column, value) in updates_arg {
                update_op = update_op.column(column, value);
            }
            let update_result = update_op.execute().await;
            deferred.settle_with(&channel, move |mut cx| {
                update_result.or_throw(&mut cx)?;
                Ok(cx.boxed(Self::from(table)))
            })
        });

        Ok(promise)
    }

    pub(crate) fn js_cleanup(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let js_table = cx.this().downcast_or_throw::<JsBox<Self>, _>(&mut cx)?;
        let rt = runtime(&mut cx)?;
        let (deferred, promise) = cx.promise();
        let table = js_table.table.clone();
        let channel = cx.channel();

        let older_than: i64 = cx
            .argument_opt(0)
            .and_then(|val| val.downcast::<JsNumber, _>(&mut cx).ok())
            .map(|val| val.value(&mut cx) as i64)
            .unwrap_or_else(|| 2 * 7 * 24 * 60); // 2 weeks
        let older_than = chrono::Duration::try_minutes(older_than).unwrap();
        let delete_unverified: Option<bool> = Some(
            cx.argument_opt(1)
                .and_then(|val| val.downcast::<JsBoolean, _>(&mut cx).ok())
                .map(|val| val.value(&mut cx))
                .unwrap_or_default(),
        );
        let error_if_tagged_old_versions: Option<bool> = Some(
            cx.argument_opt(2)
                .and_then(|val| val.downcast::<JsBoolean, _>(&mut cx).ok())
                .map(|val| val.value(&mut cx))
                .unwrap_or_default(),
        );

        rt.spawn(async move {
            let stats = table
                .optimize(OptimizeAction::Prune {
                    older_than: Some(older_than),
                    delete_unverified,
                    error_if_tagged_old_versions,
                })
                .await;

            deferred.settle_with(&channel, move |mut cx| {
                let stats = stats.or_throw(&mut cx)?;

                let prune_stats = stats.prune.as_ref().expect("Prune stats missing");
                let output_metrics = JsObject::new(&mut cx);
                let bytes_removed = cx.number(prune_stats.bytes_removed as f64);
                output_metrics.set(&mut cx, "bytesRemoved", bytes_removed)?;

                let old_versions = cx.number(prune_stats.old_versions as f64);
                output_metrics.set(&mut cx, "oldVersions", old_versions)?;

                let output_table = cx.boxed(Self::from(table));

                let output = JsObject::new(&mut cx);
                output.set(&mut cx, "metrics", output_metrics)?;
                output.set(&mut cx, "newTable", output_table)?;

                Ok(output)
            })
        });
        Ok(promise)
    }

    pub(crate) fn js_compact(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let js_table = cx.this().downcast_or_throw::<JsBox<Self>, _>(&mut cx)?;
        let rt = runtime(&mut cx)?;
        let (deferred, promise) = cx.promise();
        let table = js_table.table.clone();
        let channel = cx.channel();

        let js_options = cx.argument::<JsObject>(0)?;
        let mut options = CompactionOptions::default();

        if let Some(target_rows) =
            js_options.get_opt::<JsNumber, _, _>(&mut cx, "targetRowsPerFragment")?
        {
            options.target_rows_per_fragment = target_rows.value(&mut cx) as usize;
        }
        if let Some(max_per_group) =
            js_options.get_opt::<JsNumber, _, _>(&mut cx, "maxRowsPerGroup")?
        {
            options.max_rows_per_group = max_per_group.value(&mut cx) as usize;
        }
        if let Some(materialize_deletions) =
            js_options.get_opt::<JsBoolean, _, _>(&mut cx, "materializeDeletions")?
        {
            options.materialize_deletions = materialize_deletions.value(&mut cx);
        }
        if let Some(materialize_deletions_threshold) =
            js_options.get_opt::<JsNumber, _, _>(&mut cx, "materializeDeletionsThreshold")?
        {
            options.materialize_deletions_threshold =
                materialize_deletions_threshold.value(&mut cx) as f32;
        }
        if let Some(num_threads) = js_options.get_opt::<JsNumber, _, _>(&mut cx, "numThreads")? {
            options.num_threads = num_threads.value(&mut cx) as usize;
        }

        rt.spawn(async move {
            let stats = table
                .optimize(OptimizeAction::Compact {
                    options,
                    remap_options: None,
                })
                .await;

            deferred.settle_with(&channel, move |mut cx| {
                let stats = stats.or_throw(&mut cx)?;
                let stats = stats.compaction.as_ref().expect("Compact stats missing");

                let output_metrics = JsObject::new(&mut cx);
                let fragments_removed = cx.number(stats.fragments_removed as f64);
                output_metrics.set(&mut cx, "fragmentsRemoved", fragments_removed)?;

                let fragments_added = cx.number(stats.fragments_added as f64);
                output_metrics.set(&mut cx, "fragmentsAdded", fragments_added)?;

                let files_removed = cx.number(stats.files_removed as f64);
                output_metrics.set(&mut cx, "filesRemoved", files_removed)?;

                let files_added = cx.number(stats.files_added as f64);
                output_metrics.set(&mut cx, "filesAdded", files_added)?;

                let output_table = cx.boxed(Self::from(table));

                let output = JsObject::new(&mut cx);
                output.set(&mut cx, "metrics", output_metrics)?;
                output.set(&mut cx, "newTable", output_table)?;

                Ok(output)
            })
        });
        Ok(promise)
    }

    pub(crate) fn js_list_indices(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let js_table = cx.this().downcast_or_throw::<JsBox<Self>, _>(&mut cx)?;
        let rt = runtime(&mut cx)?;
        let (deferred, promise) = cx.promise();
        // let predicate = cx.argument::<JsString>(0)?.value(&mut cx);
        let channel = cx.channel();
        let table = js_table.table.clone();

        rt.spawn(async move {
            let indices = table.as_native().unwrap().load_indices().await;

            deferred.settle_with(&channel, move |mut cx| {
                let indices = indices.or_throw(&mut cx)?;

                let output = JsArray::new(&mut cx, indices.len() as u32);
                for (i, index) in indices.iter().enumerate() {
                    let js_index = JsObject::new(&mut cx);
                    let index_name = cx.string(index.index_name.clone());
                    js_index.set(&mut cx, "name", index_name)?;

                    let index_uuid = cx.string(index.index_uuid.clone());
                    js_index.set(&mut cx, "uuid", index_uuid)?;

                    let js_index_columns = JsArray::new(&mut cx, index.columns.len() as u32);
                    for (j, column) in index.columns.iter().enumerate() {
                        let js_column = cx.string(column.clone());
                        js_index_columns.set(&mut cx, j as u32, js_column)?;
                    }
                    js_index.set(&mut cx, "columns", js_index_columns)?;

                    output.set(&mut cx, i as u32, js_index)?;
                }

                Ok(output)
            })
        });
        Ok(promise)
    }

    #[allow(deprecated)]
    pub(crate) fn js_index_stats(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let js_table = cx.this().downcast_or_throw::<JsBox<Self>, _>(&mut cx)?;
        let rt = runtime(&mut cx)?;
        let (deferred, promise) = cx.promise();
        let index_uuid = cx.argument::<JsString>(0)?.value(&mut cx);
        let channel = cx.channel();
        let table = js_table.table.clone();

        rt.spawn(async move {
            let load_stats = futures::try_join!(
                table.as_native().unwrap().count_indexed_rows(&index_uuid),
                table.as_native().unwrap().count_unindexed_rows(&index_uuid)
            );

            deferred.settle_with(&channel, move |mut cx| {
                let (indexed_rows, unindexed_rows) = load_stats.or_throw(&mut cx)?;

                let output = JsObject::new(&mut cx);

                match indexed_rows {
                    Some(x) => {
                        let i = cx.number(x as f64);
                        output.set(&mut cx, "numIndexedRows", i)?;
                    }
                    None => {
                        let null = cx.null();
                        output.set(&mut cx, "numIndexedRows", null)?;
                    }
                };

                match unindexed_rows {
                    Some(x) => {
                        let i = cx.number(x as f64);
                        output.set(&mut cx, "numUnindexedRows", i)?;
                    }
                    None => {
                        let null = cx.null();
                        output.set(&mut cx, "numUnindexedRows", null)?;
                    }
                };

                Ok(output)
            })
        });

        Ok(promise)
    }

    pub(crate) fn js_schema(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let js_table = cx.this().downcast_or_throw::<JsBox<Self>, _>(&mut cx)?;
        let rt = runtime(&mut cx)?;
        let (deferred, promise) = cx.promise();
        let channel = cx.channel();
        let table = js_table.table.clone();

        let is_electron = cx
            .argument::<JsBoolean>(0)
            .or_throw(&mut cx)?
            .value(&mut cx);

        rt.spawn(async move {
            let schema = table.schema().await;
            deferred.settle_with(&channel, move |mut cx| {
                let schema = schema.or_throw(&mut cx)?;
                let batches = vec![RecordBatch::new_empty(schema)];
                let buffer = record_batch_to_buffer(batches).or_throw(&mut cx)?;
                convert::new_js_buffer(buffer, &mut cx, is_electron)
            })
        });
        Ok(promise)
    }

    pub(crate) fn js_add_columns(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let expressions = cx
            .argument::<JsArray>(0)?
            .to_vec(&mut cx)?
            .into_iter()
            .map(|val| {
                let obj = val.downcast_or_throw::<JsObject, _>(&mut cx)?;
                let name = obj.get::<JsString, _, _>(&mut cx, "name")?.value(&mut cx);
                let sql = obj
                    .get::<JsString, _, _>(&mut cx, "valueSql")?
                    .value(&mut cx);
                Ok((name, sql))
            })
            .collect::<NeonResult<Vec<(String, String)>>>()?;

        let transforms = NewColumnTransform::SqlExpressions(expressions);

        let js_table = cx.this().downcast_or_throw::<JsBox<Self>, _>(&mut cx)?;
        let rt = runtime(&mut cx)?;

        let (deferred, promise) = cx.promise();
        let channel = cx.channel();
        let table = js_table.table.clone();

        rt.spawn(async move {
            let result = table.add_columns(transforms, None).await;
            deferred.settle_with(&channel, move |mut cx| {
                result.or_throw(&mut cx)?;
                Ok(cx.undefined())
            })
        });

        Ok(promise)
    }

    pub(crate) fn js_alter_columns(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let alterations = cx
            .argument::<JsArray>(0)?
            .to_vec(&mut cx)?
            .into_iter()
            .map(|val| {
                let obj = val.downcast_or_throw::<JsObject, _>(&mut cx)?;
                let path = obj.get::<JsString, _, _>(&mut cx, "path")?.value(&mut cx);
                let rename = obj
                    .get_opt::<JsString, _, _>(&mut cx, "rename")?
                    .map(|val| val.value(&mut cx));
                let nullable = obj
                    .get_opt::<JsBoolean, _, _>(&mut cx, "nullable")?
                    .map(|val| val.value(&mut cx));
                // TODO: support data type here. Will need to do some serialization/deserialization

                if rename.is_none() && nullable.is_none() {
                    return cx.throw_error("At least one of 'name' or 'nullable' must be provided");
                }

                Ok(ColumnAlteration {
                    path,
                    rename,
                    nullable,
                    // TODO: wire up this field
                    data_type: None,
                })
            })
            .collect::<NeonResult<Vec<ColumnAlteration>>>()?;

        let js_table = cx.this().downcast_or_throw::<JsBox<Self>, _>(&mut cx)?;
        let rt = runtime(&mut cx)?;

        let (deferred, promise) = cx.promise();
        let channel = cx.channel();
        let table = js_table.table.clone();

        rt.spawn(async move {
            let result = table.alter_columns(&alterations).await;
            deferred.settle_with(&channel, move |mut cx| {
                result.or_throw(&mut cx)?;
                Ok(cx.undefined())
            })
        });

        Ok(promise)
    }

    pub(crate) fn js_drop_columns(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let columns = cx
            .argument::<JsArray>(0)?
            .to_vec(&mut cx)?
            .into_iter()
            .map(|val| {
                Ok(val
                    .downcast_or_throw::<JsString, _>(&mut cx)?
                    .value(&mut cx))
            })
            .collect::<NeonResult<Vec<String>>>()?;

        let js_table = cx.this().downcast_or_throw::<JsBox<Self>, _>(&mut cx)?;
        let rt = runtime(&mut cx)?;

        let (deferred, promise) = cx.promise();
        let channel = cx.channel();
        let table = js_table.table.clone();

        rt.spawn(async move {
            let col_refs = columns.iter().map(|s| s.as_str()).collect::<Vec<_>>();
            let result = table.drop_columns(&col_refs).await;
            deferred.settle_with(&channel, move |mut cx| {
                result.or_throw(&mut cx)?;
                Ok(cx.undefined())
            })
        });

        Ok(promise)
    }
}
