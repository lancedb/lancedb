// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Int64Array, LargeBinaryArray, ListArray, RecordBatch, StringArray,
    StructArray, UInt64Array, cast::AsArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::concat::concat_batches;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_catalog::TableProvider;
use lance_core::{ROW_ADDR, ROW_ID};
use lance_index::scalar::FullTextSearchQuery;
use lancedb::{
    Table, blob, connect,
    database::listing::OPT_NEW_TABLE_STORAGE_VERSION,
    table::datafusion::{BaseTableAdapter, BlobSchema},
};
use tempfile::{TempDir, tempdir};

async fn empty_table(fields: Vec<Field>) -> (TempDir, Table) {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap())
        .execute()
        .await
        .unwrap();
    let table = db
        .create_empty_table("docs", Arc::new(Schema::new(fields)))
        .execute()
        .await
        .unwrap();
    (tmp, table)
}

async fn register(ctx: &SessionContext, table: &Table, mode: BlobSchema) {
    let adapter = BaseTableAdapter::try_new(table.base_table().clone())
        .await
        .unwrap()
        .with_blob_schema(mode);
    ctx.deregister_table("docs").unwrap();
    ctx.register_table("docs", Arc::new(adapter)).unwrap();
}

async fn sql(ctx: &SessionContext, query: &str) -> RecordBatch {
    let batches = ctx.sql(query).await.unwrap().collect().await.unwrap();
    concat_batches(&batches[0].schema(), &batches).unwrap()
}

async fn planning_error(ctx: &SessionContext, query: &str) -> String {
    let error = match ctx.sql(query).await {
        Ok(df) => df.create_physical_plan().await.unwrap_err(),
        Err(error) => error,
    };
    error.to_string()
}

async fn row_ids(ctx: &SessionContext, table: &Table) -> Vec<u64> {
    let reader = BaseTableAdapter::try_new(table.base_table().clone())
        .await
        .unwrap()
        .with_system_columns(&[ROW_ID])
        .unwrap();
    ctx.register_table("read_docs", Arc::new(reader)).unwrap();
    sql(ctx, "SELECT _rowid FROM read_docs ORDER BY id")
        .await
        .column(0)
        .as_primitive::<arrow_array::types::UInt64Type>()
        .values()
        .to_vec()
}

#[tokio::test]
async fn write_mode_inserts_blobs_nulls_omitted_and_reordered_columns() {
    let (_tmp, table) = empty_table(vec![
        Field::new("id", DataType::Int64, false),
        blob("payload", true),
    ])
    .await;
    let ctx = SessionContext::new();
    register(&ctx, &table, BlobSchema::Write).await;
    for query in [
        "INSERT INTO docs SELECT 1, named_struct('data', X'6869', 'uri', NULL)",
        "INSERT INTO docs VALUES (2, NULL)",
        "INSERT INTO docs (id) VALUES (3)",
        "INSERT INTO docs SELECT 4, named_struct('data', X'', 'uri', NULL)",
        "INSERT INTO docs (payload, id) SELECT named_struct('data', X'627965', 'uri', NULL), 5",
    ] {
        sql(&ctx, query).await;
    }
    let ids = row_ids(&ctx, &table).await;
    assert_eq!(
        table.fetch_blobs("payload", &ids).await.unwrap(),
        LargeBinaryArray::from(vec![
            Some(b"hi".as_slice()),
            None,
            None,
            Some(b"".as_slice()),
            Some(b"bye".as_slice())
        ])
    );
    let batch = sql(&ctx, "SELECT id, payload.size FROM read_docs ORDER BY id").await;
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<arrow_array::types::UInt64Type>(),
        &UInt64Array::from(vec![Some(2), None, None, Some(0), Some(3)])
    );
    assert_eq!(
        sql(&ctx, "SELECT count(payload) FROM read_docs")
            .await
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        3
    );
}

#[tokio::test]
async fn write_mode_inserts_two_blobs_in_explicit_column_order() {
    let (_tmp, table) = empty_table(vec![
        Field::new("id", DataType::Int64, false),
        blob("payload", true),
        blob("thumbnail", true),
    ])
    .await;
    let ctx = SessionContext::new();
    register(&ctx, &table, BlobSchema::Write).await;
    for query in [
        "INSERT INTO docs (thumbnail, id, payload) SELECT named_struct('data', X'', 'uri', NULL), 1, named_struct('data', X'6869', 'uri', NULL)",
        "INSERT INTO docs (id) VALUES (2)",
        "INSERT INTO docs VALUES (3, NULL, NULL)",
    ] {
        sql(&ctx, query).await;
    }
    let ids = row_ids(&ctx, &table).await;
    assert_eq!(
        table.fetch_blobs("payload", &ids).await.unwrap(),
        LargeBinaryArray::from(vec![Some(b"hi".as_slice()), None, None])
    );
    assert_eq!(
        table.fetch_blobs("thumbnail", &ids).await.unwrap(),
        LargeBinaryArray::from(vec![Some(b"".as_slice()), None, None])
    );
}

#[tokio::test]
async fn write_mode_rejects_blob_reads_before_execution_and_supports_scalar_self_insert() {
    let (_tmp, table) = empty_table(vec![
        Field::new("id", DataType::Int64, false),
        blob("payload", true),
    ])
    .await;
    let ctx = SessionContext::new();
    register(&ctx, &table, BlobSchema::Write).await;
    sql(
        &ctx,
        "INSERT INTO docs SELECT 1, named_struct('data', X'6869', 'uri', NULL)",
    )
    .await;
    sql(&ctx, "INSERT INTO docs VALUES (2, NULL)").await;
    let system_adapter = BaseTableAdapter::try_new(table.base_table().clone())
        .await
        .unwrap()
        .with_system_columns(&[ROW_ID, lance_core::ROW_ADDR])
        .unwrap()
        .with_blob_schema(BlobSchema::Read);
    ctx.register_table("system_docs", Arc::new(system_adapter))
        .unwrap();
    assert_eq!(
        sql(
            &ctx,
            "SELECT id, _rowid, _rowaddr FROM system_docs WHERE id > 0"
        )
        .await
        .num_rows(),
        2
    );
    for query in [
        "SELECT * FROM docs",
        "SELECT payload FROM docs",
        "SELECT payload.size FROM docs",
        "SELECT payload.data FROM docs",
        "SELECT count(payload) FROM docs",
        "SELECT id FROM docs WHERE payload IS NULL",
        "SELECT id FROM docs WHERE payload.size = 0",
        "INSERT INTO docs SELECT id, payload FROM docs",
    ] {
        let error = planning_error(&ctx, query).await;
        assert!(
            error.contains("Blob v2 scans require BlobSchema::Read"),
            "{query}: {error}"
        );
        assert_eq!(table.count_rows(None).await.unwrap(), 2);
    }
    assert_eq!(
        sql(&ctx, "SELECT count(*) FROM docs WHERE id > 0")
            .await
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        2
    );
    assert_eq!(
        sql(&ctx, "SELECT id FROM docs WHERE id = 1")
            .await
            .num_rows(),
        1
    );
    sql(&ctx, "INSERT INTO docs (id) SELECT id + 100 FROM docs").await;
    let ids = row_ids(&ctx, &table).await;
    assert_eq!(ids.len(), 4);
    assert_eq!(
        table.fetch_blobs("payload", &ids).await.unwrap(),
        LargeBinaryArray::from(vec![Some(b"hi".as_slice()), None, None, None])
    );
}

fn blob_inputs(values: Vec<Option<&[u8]>>) -> ArrayRef {
    let data = LargeBinaryArray::from(values);
    let DataType::Struct(fields) = blob("clip", true).data_type().clone() else {
        unreachable!()
    };
    let len = data.len();
    let nulls = data.nulls().cloned();
    Arc::new(StructArray::new(
        fields,
        vec![
            Arc::new(data),
            Arc::new(StringArray::new_null(len)),
            Arc::new(UInt64Array::new_null(len)),
            Arc::new(UInt64Array::new_null(len)),
        ],
        nulls,
    ))
}

#[tokio::test]
async fn nested_blob_projections_require_a_read_adapter() {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap())
        .execute()
        .await
        .unwrap();
    let fields = vec![
        Arc::new(Field::new("note", DataType::Utf8, true)),
        Arc::new(blob("clip", true)),
    ];
    let extra = StructArray::new(
        fields.clone().into(),
        vec![
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
            blob_inputs(vec![Some(b"hi"), None, Some(b"")]),
        ],
        Some(NullBuffer::from(vec![true, true, false])),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("extra", DataType::Struct(fields.into()), true),
        ])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3])), Arc::new(extra)],
    )
    .unwrap();
    let table = db.create_table("docs", batch).execute().await.unwrap();
    let ctx = SessionContext::new();
    register(&ctx, &table, BlobSchema::Write).await;
    // DataFusion requests the same whole parent struct for extra.note and
    // extra.clip. Neither can satisfy the writer's advertised scan schema.
    for query in [
        "SELECT extra.note FROM docs ORDER BY id",
        "SELECT count(extra.note) FROM docs",
        "SELECT extra.note || 'x' FROM docs",
        "SELECT row_number() OVER (ORDER BY extra.note) FROM docs",
        "SELECT extra FROM docs",
        "SELECT extra.clip FROM docs",
        "SELECT extra.clip.size FROM docs",
        "SELECT count(extra.clip) FROM docs",
    ] {
        let error = planning_error(&ctx, query).await;
        assert!(
            error.contains("Blob v2 scans require BlobSchema::Read"),
            "{query}: {error}"
        );
    }
    let explain = sql(&ctx, "EXPLAIN SELECT id FROM docs WHERE extra.note = 'a'").await;
    let explain = explain
        .column(1)
        .as_string::<i32>()
        .iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    assert!(explain.contains("full_filters=["), "{explain}");
    assert!(!explain.contains("FilterExec"), "{explain}");
    assert_eq!(
        sql(&ctx, "SELECT id FROM docs WHERE extra.note = 'a'")
            .await
            .num_rows(),
        1
    );
    register(&ctx, &table, BlobSchema::Read).await;
    let batch = sql(&ctx, "SELECT extra.note FROM docs ORDER BY id").await;
    assert_eq!(
        batch.column(0).as_string::<i32>(),
        &StringArray::from(vec![Some("a"), Some("b"), None])
    );
    assert_eq!(
        sql(&ctx, "SELECT count(extra.note) FROM docs")
            .await
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        2
    );
    assert_eq!(
        sql(&ctx, "SELECT extra.note FROM docs WHERE extra.note = 'a'")
            .await
            .num_rows(),
        1
    );
    let reader = Arc::new(
        BaseTableAdapter::try_new(table.base_table().clone())
            .await
            .unwrap(),
    );
    let plan = reader.scan(&ctx.state(), None, &[], None).await.unwrap();
    assert_eq!(plan.schema(), reader.schema());
    let batches = datafusion_physical_plan::collect(plan, ctx.task_ctx())
        .await
        .unwrap();
    assert!(
        batches
            .iter()
            .all(|batch| batch.schema() == reader.schema())
    );
    let schema = reader.schema();
    ctx.register_table("read_docs", reader).unwrap();
    assert_eq!(sql(&ctx, "SELECT * FROM read_docs").await.schema(), schema);
    assert_eq!(
        sql(&ctx, "SELECT extra.clip.size FROM read_docs ORDER BY id")
            .await
            .column(0)
            .as_primitive::<arrow_array::types::UInt64Type>(),
        &UInt64Array::from(vec![Some(2), None, None])
    );
}

#[tokio::test]
async fn list_descriptors_match_declared_schema_and_cannot_be_scanned_in_write_mode() {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap())
        .execute()
        .await
        .unwrap();
    let list = ListArray::new(
        Arc::new(blob("item", true)),
        OffsetBuffer::from_lengths([1, 2]),
        blob_inputs(vec![Some(b"hi"), None, Some(b"")]),
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "payloads",
            list.data_type().clone(),
            true,
        )])),
        vec![Arc::new(list)],
    )
    .unwrap();
    let table = db.create_table("docs", batch).execute().await.unwrap();
    let ctx = SessionContext::new();
    let reader = Arc::new(
        BaseTableAdapter::try_new(table.base_table().clone())
            .await
            .unwrap(),
    );
    let plan = reader.scan(&ctx.state(), None, &[], None).await.unwrap();
    assert_eq!(plan.schema(), reader.schema());
    let batches = datafusion_physical_plan::collect(plan, ctx.task_ctx())
        .await
        .unwrap();
    assert!(
        batches
            .iter()
            .all(|batch| batch.schema() == reader.schema())
    );
    ctx.register_table("docs", reader).unwrap();
    assert_eq!(
        sql(&ctx, "SELECT count(payloads) FROM docs")
            .await
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        2
    );
    register(&ctx, &table, BlobSchema::Write).await;
    assert!(
        planning_error(&ctx, "SELECT payloads FROM docs")
            .await
            .contains("Blob v2 scans require BlobSchema::Read")
    );
    assert_eq!(
        sql(&ctx, "SELECT count(*) FROM docs")
            .await
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        2
    );
}

#[tokio::test]
async fn tables_without_blobs_support_inserts_and_full_scans_in_both_modes() {
    let (_tmp, table) = empty_table(vec![Field::new("id", DataType::Int64, false)]).await;
    let ctx = SessionContext::new();
    for (mode, id) in [(BlobSchema::Read, 1), (BlobSchema::Write, 2)] {
        register(&ctx, &table, mode).await;
        sql(&ctx, &format!("INSERT INTO docs VALUES ({id})")).await;
        let provider = ctx.table_provider("docs").await.unwrap();
        let plan = provider.scan(&ctx.state(), None, &[], None).await.unwrap();
        assert_eq!(plan.schema(), provider.schema());
        assert_eq!(
            sql(&ctx, "SELECT * FROM docs").await.num_rows(),
            id as usize
        );
    }
}

#[tokio::test]
async fn read_mode_rejects_null_and_omitted_blob_inserts_without_writing() {
    let (_tmp, table) = empty_table(vec![
        Field::new("id", DataType::Int64, false),
        blob("payload", true),
    ])
    .await;
    let ctx = SessionContext::new();
    register(&ctx, &table, BlobSchema::Read).await;
    for query in [
        "INSERT INTO docs VALUES (2, NULL)",
        "INSERT INTO docs (id) VALUES (3)",
    ] {
        let error = planning_error(&ctx, query).await;
        assert!(
            error.contains("SQL INSERT into Blob v2 tables requires BlobSchema::Write"),
            "{query}: {error}"
        );
        assert_eq!(table.count_rows(None).await.unwrap(), 0);
    }
}

#[tokio::test]
async fn write_mode_inserts_exclude_read_only_builder_columns() {
    for mode_first in [false, true] {
        for system_first in [false, true] {
            let (_tmp, table) = empty_table(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("text", DataType::Utf8, true),
                blob("payload", true),
            ])
            .await;
            let mut adapter = BaseTableAdapter::try_new(table.base_table().clone())
                .await
                .unwrap();
            if mode_first {
                adapter = adapter.with_blob_schema(BlobSchema::Write);
            }
            let fts = FullTextSearchQuery::new("hello".into());
            adapter = if system_first {
                adapter
                    .with_system_columns(&[ROW_ID, ROW_ADDR])
                    .unwrap()
                    .with_fts_query(fts)
            } else {
                adapter
                    .with_fts_query(fts)
                    .with_system_columns(&[ROW_ID, ROW_ADDR])
                    .unwrap()
            };
            let writer = adapter.with_blob_schema(BlobSchema::Write);
            let ctx = SessionContext::new();
            ctx.register_table("docs", Arc::new(writer)).unwrap();
            sql(
                &ctx,
                "INSERT INTO docs SELECT 1, 'hello', named_struct('data', X'6869', 'uri', NULL)",
            )
            .await;
            sql(&ctx, "INSERT INTO docs (text, id) VALUES ('hello', 2)").await;
            let ids = row_ids(&ctx, &table).await;
            assert_eq!(
                table.fetch_blobs("payload", &ids).await.unwrap(),
                LargeBinaryArray::from(vec![Some(b"hi".as_slice()), None])
            );
        }
    }
}

#[tokio::test]
async fn legacy_blob_write_mode_rejects_descriptor_scans_and_keeps_scalar_scans() {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap())
        .storage_options([(OPT_NEW_TABLE_STORAGE_VERSION, "2.1")])
        .execute()
        .await
        .unwrap();
    let legacy = Field::new("payload", DataType::LargeBinary, true).with_metadata(
        std::collections::HashMap::from([("lance-encoding:blob".into(), "true".into())]),
    );
    let table = db
        .create_empty_table(
            "docs",
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                legacy,
            ])),
        )
        .execute()
        .await
        .unwrap();
    let ctx = SessionContext::new();
    register(&ctx, &table, BlobSchema::Write).await;
    sql(&ctx, "INSERT INTO docs VALUES (1, X'6869')").await;
    let writer = ctx.table_provider("docs").await.unwrap();
    assert_eq!(writer.schema().field(1).data_type(), &DataType::LargeBinary);
    let error = writer
        .scan(&ctx.state(), None, &[], None)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("Scan schema mismatch"),
        "{error}"
    );
    assert!(!error.to_string().contains("Blob v2"), "{error}");
    let plan = writer
        .scan(&ctx.state(), Some(&vec![0]), &[], None)
        .await
        .unwrap();
    assert_eq!(
        plan.schema(),
        Arc::new(writer.schema().project(&[0]).unwrap())
    );
    assert_eq!(
        sql(&ctx, "SELECT id + 1 FROM docs WHERE id = 1")
            .await
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        2
    );
    assert_eq!(
        sql(&ctx, "SELECT count(*) FROM docs")
            .await
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        1
    );
    register(&ctx, &table, BlobSchema::Read).await;
    let reader = ctx.table_provider("docs").await.unwrap();
    let plan = reader.scan(&ctx.state(), None, &[], None).await.unwrap();
    assert_eq!(plan.schema(), reader.schema());
    let DataType::Struct(fields) = reader.schema().field(1).data_type().clone() else {
        panic!("expected a legacy descriptor");
    };
    assert_eq!(fields.len(), 2);
    assert!(fields.find("position").is_some());
    assert!(fields.find("size").is_some());
    assert_eq!(sql(&ctx, "SELECT * FROM docs").await.num_rows(), 1);
}

#[tokio::test]
async fn copying_non_null_descriptors_between_tables_commits_no_rows() {
    let (_tmp, source) = empty_table(vec![
        Field::new("id", DataType::Int64, false),
        blob("payload", true),
    ])
    .await;
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_batch_size(1)
            .with_target_partitions(1),
    );
    register(&ctx, &source, BlobSchema::Write).await;
    sql(
        &ctx,
        "INSERT INTO docs SELECT 1, named_struct('data', X'6869', 'uri', NULL)",
    )
    .await;
    sql(&ctx, "INSERT INTO docs VALUES (2, NULL)").await;
    register(&ctx, &source, BlobSchema::Read).await;
    let (_dest_tmp, destination) = empty_table(
        source
            .schema()
            .await
            .unwrap()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect(),
    )
    .await;
    let writer = BaseTableAdapter::try_new(destination.base_table().clone())
        .await
        .unwrap()
        .with_blob_schema(BlobSchema::Write);
    ctx.register_table("destination", Arc::new(writer)).unwrap();
    let version = destination.version().await.unwrap();
    // The null row is valid and reaches the writer before the non-null
    // descriptor. Even if it is written to a fragment, it must not be committed.
    let plan = ctx
        .sql("INSERT INTO destination SELECT id, payload FROM docs ORDER BY id DESC")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let error = datafusion_physical_plan::collect(plan, ctx.task_ctx())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("blob"), "{error}");
    assert_eq!(destination.count_rows(None).await.unwrap(), 0);
    assert_eq!(destination.version().await.unwrap(), version);
    sql(
        &ctx,
        "INSERT INTO destination SELECT id, payload FROM docs WHERE payload IS NULL",
    )
    .await;
    let ids = row_ids(&ctx, &destination).await;
    assert_eq!(ids.len(), 1);
    assert_eq!(
        destination.fetch_blobs("payload", &ids).await.unwrap(),
        LargeBinaryArray::from(vec![None::<&[u8]>])
    );
}
