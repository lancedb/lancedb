// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;
use crate::index::vector::{
    IvfFlatIndexBuilder, IvfHnswFlatIndexBuilder, IvfHnswSqIndexBuilder, IvfRqIndexBuilder,
    IvfSqIndexBuilder,
};
use crate::remote::JSON_CONTENT_TYPE;
use crate::remote::client::{ClientConfig, RetryConfig};
use crate::remote::db::DEFAULT_SERVER_VERSION;
use crate::table::{AddDataMode, FieldMetadataUpdate, FtsToken};
use crate::utils::background_cache::clock;
use crate::{
    DistanceType, Error, Table,
    index::{Index, IndexStatistics, IndexType, vector::IvfPqIndexBuilder},
    query::{
        AnalyzePlanDistributedMetrics, ColumnOrdering, ExecutableQuery, QueryBase,
        QueryExecutionOptions,
    },
};
use arrow::{array::AsArray, compute::concat_batches, datatypes::Int32Type};
use arrow_array::Array;
use arrow_array::builder::LargeBinaryBuilder;
use arrow_array::{
    BinaryArray, Int32Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray,
    StructArray, record_batch,
};
use arrow_schema::{DataType, Field, Schema};
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryFutureExt, TryStreamExt, future::BoxFuture};
use lance::dataset::ROW_ID;
use lance_index::scalar::inverted::{DocumentGranularity, query::MatchQuery};
use lance_index::scalar::{FullTextSearchQuery, InvertedIndexParams};
use reqwest::Body;
use rstest::rstest;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::{collections::HashMap, pin::Pin};

fn refresh_done(job_id: &str) -> String {
    json!({
        "job_id": job_id,
        "job_state": "DONE",
        "result": {
            "rows_assigned": 12,
            "rows_failed": 0,
            "rows_remaining": 0,
            "source_version": 7,
            "published_version": 8,
        }
    })
    .to_string()
}

async fn collect_body(body: Body) -> Vec<u8> {
    use http_body::Body;
    let mut body = body;
    let mut data = Vec::new();
    let mut body_pin = Pin::new(&mut body);
    futures::stream::poll_fn(|cx| body_pin.as_mut().poll_frame(cx))
        .for_each(|frame| {
            data.extend_from_slice(frame.unwrap().data_ref().unwrap());
            futures::future::ready(())
        })
        .await;
    data
}

fn write_ipc_stream(data: &RecordBatch) -> Vec<u8> {
    let mut body = Vec::new();
    {
        let options = arrow_ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(arrow_ipc::CompressionType::LZ4_FRAME))
            .expect("Failed to create IPC write options");
        let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
            &mut body,
            &data.schema(),
            options,
        )
        .expect("Failed to create writer");
        writer.write(data).expect("Failed to write data");
        writer.finish().expect("Failed to finish");
    }
    body
}

fn write_ipc_stream_uncompressed(data: &RecordBatch) -> Vec<u8> {
    let mut body = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut body, &data.schema())
            .expect("Failed to create writer");
        writer.write(data).expect("Failed to write data");
        writer.finish().expect("Failed to finish");
    }
    body
}

fn write_ipc_file(data: &RecordBatch) -> Vec<u8> {
    let mut body = Vec::new();
    {
        let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut body, &data.schema())
            .expect("Failed to create writer");
        writer.write(data).expect("Failed to write data");
        writer.finish().expect("Failed to finish");
    }
    body
}

/// Build a JSON describe response for the given schema.
fn describe_response(schema: &Schema) -> String {
    let json_schema = JsonSchema::try_from(schema).unwrap();
    serde_json::to_string(&json!({
        "version": 1,
        "schema": json_schema,
    }))
    .unwrap()
}

fn nested_index_schema() -> Schema {
    let vector_type =
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 8);
    Schema::new(vec![
        Field::new("rowId", DataType::Int32, false),
        Field::new("row-id", DataType::Int32, false),
        Field::new("userId", DataType::Int32, false),
        Field::new(
            "metadata",
            DataType::Struct(vec![Field::new("user_id", DataType::Int32, false)].into()),
            false,
        ),
        Field::new(
            "MetaData",
            DataType::Struct(vec![Field::new("userId", DataType::Int32, false)].into()),
            false,
        ),
        Field::new(
            "image",
            DataType::Struct(vec![Field::new("embedding", vector_type, false)].into()),
            false,
        ),
        Field::new(
            "payload",
            DataType::Struct(vec![Field::new("text", DataType::Utf8, false)].into()),
            false,
        ),
        Field::new(
            "docs",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Field::new("content", DataType::Utf8, true)].into()),
                true,
            ))),
            true,
        ),
        Field::new(
            "meta-data",
            DataType::Struct(vec![Field::new("user-id", DataType::Int32, false)].into()),
            false,
        ),
        Field::new(
            "literal",
            DataType::Struct(vec![Field::new("a.b", DataType::Int32, false)].into()),
            false,
        ),
    ])
}

/// Build a `get_lsm_stats` body for one bucket holding `generations`.
fn stats_body(generations: &[u64], compacting: bool) -> String {
    serde_json::json!({
        "lsm_stats": {
            "buckets": [{
                "shard_id": "b0",
                "status": "Active",
                "writer_epoch": 1,
                "manifest_version": 1,
                "current_generation": generations.iter().max().copied().unwrap_or(0) + 1,
                "replay_after_wal_entry_position": 0,
                "wal_entry_position_last_seen": 0,
                "generations": generations.iter()
                    .map(|g| serde_json::json!({ "generation": g, "bytes": 1 }))
                    .collect::<Vec<_>>(),
                "compacting": compacting,
                "memtables": [],
            }],
        }
    })
    .to_string()
}

fn schema_json() -> &'static str {
    r#"{"fields": [{"name": "id", "type": {"type": "int32"}, "nullable": true}]}"#
}

fn simple_describe_response() -> http::Response<String> {
    http::Response::builder()
        .status(200)
        .body(format!(r#"{{"version": 1, "schema": {}}}"#, schema_json()))
        .unwrap()
}

// ----- Branch support -----

/// Parse a request's in-memory JSON body. Only valid for JSON-body ops
/// (not Arrow-stream inserts, whose body is a stream).
fn request_body_json(request: &reqwest::Request) -> serde_json::Value {
    let bytes = request
        .body()
        .expect("request has a body")
        .as_bytes()
        .expect("body is in-memory");
    serde_json::from_slice(bytes).expect("body is valid JSON")
}

/// One leg's worth of results, keyed so the two legs share a row.
/// `row_ids` mirrors a server answering a query that asked for `_rowid`.
fn leg(score_column: &str, ids: Vec<&str>, row_ids: bool) -> RecordBatch {
    use arrow_array::{Float32Array, UInt64Array};
    let mut fields = vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, true),
        Field::new(score_column, DataType::Float32, false),
    ];
    let scores = Float32Array::from(vec![1.0_f32; ids.len()]);
    let texts = StringArray::from(ids.clone());
    let mut columns: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(StringArray::from(ids.clone())),
        Arc::new(texts),
        Arc::new(scores),
    ];
    if row_ids {
        fields.push(Field::new(ROW_ID, DataType::UInt64, false));
        columns.push(Arc::new(UInt64Array::from_iter_values(
            ids.iter().map(|id| id.as_bytes()[0] as u64),
        )));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

mod blobs;
mod branches;
mod columns;
mod freshness;
mod hybrid;
mod index;
mod lsm;
mod multipart;
mod query;
mod schema_cache;
mod versions;
mod write;
