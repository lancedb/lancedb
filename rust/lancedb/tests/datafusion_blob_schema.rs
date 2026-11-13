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
async fn copying_descriptors_materializes_payloads_and_preserves_snapshot() {
    use std::collections::HashMap;
    use std::num::NonZeroUsize;

    use lancedb::blob::{BlobOptions, blob_with_options};
    use lancedb::table::OptimizeAction;

    for managed in [false, true] {
        for mode in [BlobSchema::Read, BlobSchema::Write] {
            let source_tmp = tempdir().unwrap();
            let db = connect(source_tmp.path().to_str().unwrap())
                .execute()
                .await
                .unwrap();
            let payload = blob_with_options(
                "payload",
                true,
                BlobOptions {
                    inline_size_threshold: Some(8),
                    dedicated_size_threshold: NonZeroUsize::new(32),
                },
            );
            let values: Vec<Option<&[u8]>> = vec![
                Some(b"hi"),
                Some(&[2; 16]),
                Some(&[3; 64]),
                None,
                Some(b""),
                Some(b"skip"),
            ];
            let schema = Arc::new(Schema::new_with_metadata(
                vec![Field::new("id", DataType::Int64, false), payload],
                HashMap::from([("lance.blob.managed".into(), managed.to_string())]),
            ));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
                    blob_inputs(values.clone()),
                ],
            )
            .unwrap();
            let source = db.create_table("docs", batch).execute().await.unwrap();
            let ctx = SessionContext::new_with_config(
                SessionConfig::new()
                    .with_batch_size(2)
                    .with_target_partitions(2),
            );
            register(&ctx, &source, BlobSchema::Read).await;
            // Descriptor queries and SELECT * retain their original read schema.
            let sizes = sql(&ctx, "SELECT payload.size FROM docs ORDER BY id").await;
            assert_eq!(
                sizes
                    .column(0)
                    .as_primitive::<arrow_array::types::UInt64Type>()
                    .value(2),
                64
            );
            assert_eq!(sql(&ctx, "SELECT * FROM docs").await.num_columns(), 2);
            let kinds = sql(&ctx, "SELECT payload.kind FROM docs ORDER BY id").await;
            let kinds = kinds
                .column(0)
                .as_primitive::<arrow_array::types::UInt8Type>();
            if !managed {
                assert_eq!(
                    kinds.value(0),
                    lance_core::datatypes::BlobKind::Inline as u8
                );
                assert_eq!(
                    kinds.value(1),
                    lance_core::datatypes::BlobKind::Packed as u8
                );
                assert_eq!(
                    kinds.value(2),
                    lance_core::datatypes::BlobKind::Dedicated as u8
                );
            } else {
                assert_eq!(
                    kinds.value(2),
                    lance_core::datatypes::BlobKind::Managed as u8
                );
            }
            let (_destination_tmp, destination) = empty_table(vec![
                blob("copied", true),
                Field::new("id", DataType::Int64, false),
            ])
            .await;
            let writer = BaseTableAdapter::try_new(destination.base_table().clone())
                .await
                .unwrap()
                .with_blob_schema(mode);
            ctx.register_table("destination", Arc::new(writer)).unwrap();
            let plan = ctx.sql("INSERT INTO destination (id, copied) SELECT id + 10, payload AS copied FROM docs WHERE id > 0 AND (payload.size IS NULL OR payload.size < 100) ORDER BY id DESC LIMIT 5 OFFSET 1")
                .await.unwrap().create_physical_plan().await.unwrap();
            // A live-table change after planning must not change the source
            // version used to decode the descriptors in this insert.
            source.delete("id = 1").await.unwrap();
            datafusion_physical_plan::collect(plan, ctx.task_ctx())
                .await
                .unwrap();
            assert_eq!(destination.count_rows(None).await.unwrap(), 5);
            source.optimize(OptimizeAction::All).await.unwrap();
            std::fs::remove_dir_all(source_tmp.path()).unwrap();
            let reader = BaseTableAdapter::try_new(destination.base_table().clone())
                .await
                .unwrap()
                .with_system_columns(&[ROW_ID])
                .unwrap();
            ctx.register_table("copied_rows", Arc::new(reader)).unwrap();
            let ids = sql(&ctx, "SELECT _rowid FROM copied_rows ORDER BY id").await;
            let ids = ids
                .column(0)
                .as_primitive::<arrow_array::types::UInt64Type>()
                .values();
            assert_eq!(
                destination.fetch_blobs("copied", ids).await.unwrap(),
                LargeBinaryArray::from(values[..5].to_vec())
            );
        }
    }
}

#[tokio::test]
async fn copying_nested_descriptors_preserves_parent_nulls_and_field_identity() {
    use arrow_array::Int32Array;
    use lancedb::blob::{BlobOptions, blob_with_options};

    for difference in ["metadata", "order", "scalar"] {
        for mode in [BlobSchema::Read, BlobSchema::Write] {
            let source_tmp = tempdir().unwrap();
            let db = connect(source_tmp.path().to_str().unwrap())
                .execute()
                .await
                .unwrap();
            let children = vec![
                Arc::new(Field::new("note", DataType::Utf8, true)),
                Arc::new(Field::new("rank", DataType::Int32, true)),
                Arc::new(blob_with_options(
                    "clip",
                    true,
                    BlobOptions {
                        inline_size_threshold: Some(8),
                        ..Default::default()
                    },
                )),
            ];
            let extra_field = Field::new("extra", DataType::Struct(children.clone().into()), true);
            let extra = StructArray::new(
                children.into(),
                vec![
                    Arc::new(StringArray::from(vec![
                        Some("a"),
                        Some("hidden"),
                        Some("c"),
                    ])),
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    blob_inputs(vec![Some(b"hi"), Some(b"hidden"), Some(b"")]),
                ],
                Some(NullBuffer::from(vec![true, false, true])),
            );
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    extra_field,
                ])),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3])), Arc::new(extra)],
            )
            .unwrap();
            let source = db.create_table("docs", batch).execute().await.unwrap();
            let ctx = SessionContext::new();
            register(&ctx, &source, BlobSchema::Read).await;
            let mut target_children = vec![
                Field::new("note", DataType::Utf8, true),
                Field::new(
                    "rank",
                    if difference == "scalar" {
                        DataType::Int64
                    } else {
                        DataType::Int32
                    },
                    true,
                ),
                blob("clip", true),
            ];
            if difference == "order" {
                target_children.swap(0, 2);
            }
            let (_destination_tmp, destination) = empty_table(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("extra", DataType::Struct(target_children.into()), true),
                blob("flattened", true),
            ])
            .await;
            ctx.register_table(
                "destination",
                Arc::new(
                    BaseTableAdapter::try_new(destination.base_table().clone())
                        .await
                        .unwrap()
                        .with_blob_schema(mode),
                ),
            )
            .unwrap();
            let version = destination.version().await.unwrap();
            for expression in [
                "CASE WHEN id = 1 THEN extra ELSE NULL END",
                "named_struct('note', extra.note, 'rank', extra.rank, 'clip', extra.clip)",
            ] {
                let query = format!(
                    "INSERT INTO destination SELECT id, {expression} AS extra, extra.clip AS flattened FROM docs"
                );
                let error = planning_error(&ctx, &query).await;
                assert!(
                    error.contains("one source table snapshot"),
                    "{difference}, {mode:?}: {error}"
                );
                assert_eq!(destination.version().await.unwrap(), version);
                assert_eq!(destination.count_rows(None).await.unwrap(), 0);
            }
            sql(&ctx, "INSERT INTO destination SELECT id, extra, extra.clip AS flattened FROM docs ORDER BY id DESC").await;
            std::fs::remove_dir_all(source_tmp.path()).unwrap();
            let ids = row_ids(&ctx, &destination).await;
            let expected =
                LargeBinaryArray::from(vec![Some(b"hi".as_slice()), None, Some(b"".as_slice())]);
            assert_eq!(
                destination.fetch_blobs("extra.clip", &ids).await.unwrap(),
                expected,
                "{difference}, {mode:?}"
            );
            assert_eq!(
                destination.fetch_blobs("flattened", &ids).await.unwrap(),
                expected
            );
            let nested = sql(&ctx, "SELECT extra FROM read_docs ORDER BY id").await;
            let extra = nested.column(0).as_struct();
            assert!(extra.is_null(1));
            assert_eq!(
                extra
                    .column_by_name("note")
                    .unwrap()
                    .as_string::<i32>()
                    .value(2),
                "c"
            );
            let rank = extra.column_by_name("rank").unwrap();
            if difference == "scalar" {
                assert_eq!(
                    rank.as_primitive::<arrow_array::types::Int64Type>()
                        .value(2),
                    3
                );
            } else {
                assert_eq!(
                    rank.as_primitive::<arrow_array::types::Int32Type>()
                        .value(2),
                    3
                );
            }
        }
    }
}

#[tokio::test]
async fn ordinary_descriptor_shaped_structs_remain_plain_data() {
    let fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "raw",
            DataType::Struct(
                vec![
                    Field::new("kind", DataType::UInt8, true),
                    Field::new("position", DataType::UInt64, true),
                    Field::new("size", DataType::UInt64, true),
                    Field::new("blob_id", DataType::UInt32, true),
                    Field::new("blob_uri", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        ),
    ];
    let (source_tmp, source) = empty_table(fields.clone()).await;
    let (_destination_tmp, destination) = empty_table(fields).await;
    let ctx = SessionContext::new();
    register(&ctx, &source, BlobSchema::Read).await;
    sql(&ctx, "INSERT INTO docs VALUES (1, named_struct('kind', 1, 'position', 2, 'size', 3, 'blob_id', 4, 'blob_uri', 'plain data'))").await;
    let expected = sql(&ctx, "SELECT * FROM docs").await;
    ctx.register_table(
        "destination",
        Arc::new(
            BaseTableAdapter::try_new(destination.base_table().clone())
                .await
                .unwrap(),
        ),
    )
    .unwrap();
    sql(&ctx, "INSERT INTO destination SELECT * FROM docs").await;
    std::fs::remove_dir_all(source_tmp.path()).unwrap();
    let actual = sql(&ctx, "SELECT * FROM destination").await;
    assert_eq!(actual.columns(), expected.columns());
}

#[tokio::test]
async fn ambiguous_descriptor_sources_fail_during_planning_without_writing() {
    let (_tmp, source) = empty_table(vec![
        Field::new("id", DataType::Int64, false),
        blob("payload", true),
    ])
    .await;
    let ctx = SessionContext::new();
    register(&ctx, &source, BlobSchema::Write).await;
    sql(
        &ctx,
        "INSERT INTO docs SELECT 1, named_struct('data', X'6869', 'uri', NULL)",
    )
    .await;
    register(&ctx, &source, BlobSchema::Read).await;
    let (_destination_tmp, destination) = empty_table(vec![
        Field::new("id", DataType::Int64, false),
        blob("payload", true),
    ])
    .await;
    let writer = BaseTableAdapter::try_new(destination.base_table().clone())
        .await
        .unwrap();
    ctx.register_table("destination", Arc::new(writer)).unwrap();
    let version = destination.version().await.unwrap();
    for select in [
        "SELECT a.id, a.payload FROM docs a JOIN docs b ON a.id = b.id",
        "SELECT id, payload FROM docs UNION ALL SELECT id, payload FROM docs",
        "SELECT id, payload FROM docs GROUP BY id, payload",
        "SELECT id, CASE WHEN id = 1 THEN payload ELSE NULL END AS payload FROM docs",
    ] {
        let query = format!("INSERT INTO destination {select}");
        let error = planning_error(&ctx, &query).await;
        assert!(
            error.contains("one source table snapshot"),
            "{query}: {error}"
        );
    }
    assert_eq!(destination.count_rows(None).await.unwrap(), 0);
    assert_eq!(destination.version().await.unwrap(), version);
}

#[tokio::test]
async fn external_descriptor_copy_obeys_destination_bases_and_preserves_ranges() {
    use lance::dataset::WriteParams;
    use lance_core::datatypes::BlobKind;
    use lance_table::format::BasePath;
    use lancedb::table::WriteOptions;

    let external_tmp = tempdir().unwrap();
    let payload = b"0123456789abcdef";
    let path = external_tmp.path().join("payload.bin");
    std::fs::write(&path, payload).unwrap();
    let uri = url::Url::from_file_path(&path).unwrap().to_string();
    let base_uri = url::Url::from_directory_path(external_tmp.path())
        .unwrap()
        .to_string();
    let base = |id| BasePath {
        id,
        name: Some("external".into()),
        path: base_uri.clone(),
        is_dataset_root: false,
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        blob("payload", true),
    ]));
    let DataType::Struct(children) = schema.field(1).data_type().clone() else {
        unreachable!()
    };
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StructArray::new(
                children,
                vec![
                    Arc::new(LargeBinaryArray::from(vec![
                        None,
                        Some(b"inline".as_slice()),
                    ])),
                    Arc::new(StringArray::from(vec![Some(uri.as_str()), None])),
                    Arc::new(UInt64Array::from(vec![Some(4), None])),
                    Arc::new(UInt64Array::from(vec![Some(6), None])),
                ],
                None,
            )),
        ],
    )
    .unwrap();
    for source_registered in [false, true] {
        let source_tmp = tempdir().unwrap();
        let db = connect(source_tmp.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let source = db
            .create_empty_table("source", schema.clone())
            .write_options(WriteOptions {
                lance_write_params: Some(WriteParams {
                    initial_bases: source_registered.then(|| vec![base(7)]),
                    ..Default::default()
                }),
            })
            .execute()
            .await
            .unwrap();
        source
            .add(input.clone())
            .allow_external_blob_outside_bases(!source_registered)
            .execute()
            .await
            .unwrap();
        for mode in [BlobSchema::Read, BlobSchema::Write] {
            let ctx = SessionContext::new();
            register(&ctx, &source, BlobSchema::Read).await;
            let descriptor = sql(&ctx, "SELECT payload.blob_id FROM docs WHERE id = 1").await;
            assert_eq!(
                descriptor
                    .column(0)
                    .as_primitive::<arrow_array::types::UInt32Type>()
                    .value(0),
                if source_registered { 7 } else { 0 }
            );
            for authorized in [false, true] {
                let destination_tmp = tempdir().unwrap();
                let destination_db = connect(destination_tmp.path().to_str().unwrap())
                    .execute()
                    .await
                    .unwrap();
                let destination = destination_db
                    .create_empty_table("destination", schema.clone())
                    .write_options(WriteOptions {
                        lance_write_params: Some(WriteParams {
                            initial_bases: authorized.then(|| vec![base(11)]),
                            ..Default::default()
                        }),
                    })
                    .execute()
                    .await
                    .unwrap();
                ctx.deregister_table("destination").unwrap();
                ctx.register_table(
                    "destination",
                    Arc::new(
                        BaseTableAdapter::try_new(destination.base_table().clone())
                            .await
                            .unwrap()
                            .with_blob_schema(mode),
                    ),
                )
                .unwrap();
                let version = destination.version().await.unwrap();
                let result = ctx
                    .sql("INSERT INTO destination SELECT * FROM docs ORDER BY id")
                    .await
                    .unwrap()
                    .collect()
                    .await;
                if !authorized {
                    let error = result.unwrap_err().to_string();
                    assert!(
                        error.contains("outside registered external bases"),
                        "{error}"
                    );
                    let reopened = destination_db
                        .open_table("destination")
                        .execute()
                        .await
                        .unwrap();
                    assert_eq!(reopened.version().await.unwrap(), version);
                    assert_eq!(reopened.count_rows(None).await.unwrap(), 0);
                    // A logical URI input has the same destination authorization boundary.
                    ctx.register_table(
                        "logical_target",
                        Arc::new(
                            BaseTableAdapter::try_new(destination.base_table().clone())
                                .await
                                .unwrap()
                                .with_blob_schema(BlobSchema::Write),
                        ),
                    )
                    .unwrap();
                    let error = ctx.sql(&format!("INSERT INTO logical_target SELECT 3, named_struct('data', NULL, 'uri', '{uri}')")).await.unwrap().collect().await.unwrap_err().to_string();
                    assert!(
                        error.contains("allow_external_blob_outside_bases"),
                        "{error}"
                    );
                    destination.checkout_latest().await.unwrap();
                    assert_eq!(destination.version().await.unwrap(), version);
                    assert_eq!(destination.count_rows(None).await.unwrap(), 0);
                } else {
                    result.unwrap();
                    let ids = row_ids(&ctx, &destination).await;
                    let descriptor = sql(&ctx, "SELECT payload.kind, payload.blob_id, payload.blob_uri, payload.position, payload.size FROM read_docs WHERE id = 1").await;
                    assert_eq!(
                        descriptor
                            .column(0)
                            .as_primitive::<arrow_array::types::UInt8Type>()
                            .value(0),
                        BlobKind::External as u8
                    );
                    assert_eq!(
                        descriptor
                            .column(1)
                            .as_primitive::<arrow_array::types::UInt32Type>()
                            .value(0),
                        11
                    );
                    assert_eq!(
                        descriptor.column(2).as_string::<i32>().value(0),
                        "payload.bin"
                    );
                    assert_eq!(
                        descriptor
                            .column(3)
                            .as_primitive::<arrow_array::types::UInt64Type>()
                            .value(0),
                        4
                    );
                    assert_eq!(
                        descriptor
                            .column(4)
                            .as_primitive::<arrow_array::types::UInt64Type>()
                            .value(0),
                        6
                    );
                    assert_eq!(
                        destination.fetch_blobs("payload", &ids).await.unwrap(),
                        LargeBinaryArray::from(vec![
                            Some(&payload[4..10]),
                            Some(b"inline".as_slice())
                        ])
                    );
                    let files = destination.fetch_blob_files("payload", &ids).await.unwrap();
                    let file = files[0].as_ref().unwrap();
                    assert_eq!(file.uri(), Some("payload.bin"));
                    let dataset = destination.dataset().unwrap().get().await.unwrap();
                    let registered = &dataset.manifest().base_paths[&11];
                    let resolved =
                        url::Url::parse(&format!("{}/", registered.path.trim_end_matches('/')))
                            .unwrap()
                            .join(file.uri().unwrap())
                            .unwrap();
                    assert_eq!(resolved.as_str(), uri);
                    assert_eq!(file.position(), Some(4));
                    assert_eq!(file.size(), 6);
                }
            }
        }
    }
}

#[tokio::test]
async fn source_payload_failure_after_written_partition_does_not_commit() {
    use datafusion_physical_plan::{
        ExecutionPlanProperties,
        execution_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions},
        union::UnionExec,
    };
    use futures::TryStreamExt;
    use lancedb::blob::{BlobOptions, blob_with_options};
    use std::num::NonZeroUsize;

    let source_tmp = tempdir().unwrap();
    let db = connect(source_tmp.path().to_str().unwrap())
        .execute()
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        blob_with_options(
            "payload",
            true,
            BlobOptions {
                inline_size_threshold: Some(0),
                dedicated_size_threshold: NonZeroUsize::new(1),
            },
        ),
    ]));
    let source = db
        .create_table(
            "docs",
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])),
                    blob_inputs(vec![Some(&[1; 64]), Some(&[2; 64])]),
                ],
            )
            .unwrap(),
        )
        .execute()
        .await
        .unwrap();
    let (_destination_tmp, destination) = empty_table(vec![
        Field::new("id", DataType::Int64, false),
        blob("payload", true),
    ])
    .await;
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(1),
    );
    register(&ctx, &source, BlobSchema::Read).await;
    ctx.register_table(
        "destination",
        Arc::new(
            BaseTableAdapter::try_new(destination.base_table().clone())
                .await
                .unwrap(),
        ),
    )
    .unwrap();
    let first = ctx
        .sql("INSERT INTO destination SELECT * FROM docs WHERE id = 1")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let second = ctx
        .sql("INSERT INTO destination SELECT * FROM docs WHERE id = 2")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    assert_eq!(first.name(), "InsertExec");
    assert_eq!(second.name(), "InsertExec");
    // Combine already validated SQL inputs only to control partition scheduling:
    // finish one real uncommitted write before faulting the next payload read.
    let input = UnionExec::try_new(vec![
        first.children()[0].clone(),
        second.children()[0].clone(),
    ])
    .unwrap();
    assert_eq!(input.output_partitioning().partition_count(), 2);
    let insert = first
        .replace_children(
            vec![input],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .unwrap();
    let version = destination.version().await.unwrap();
    let batches: Vec<RecordBatch> = insert
        .execute(0, ctx.task_ctx())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_primitive::<arrow_array::types::UInt64Type>()
            .value(0),
        1
    );
    assert_eq!(destination.count_rows(None).await.unwrap(), 0);
    assert_eq!(destination.version().await.unwrap(), version);
    let dataset = destination.dataset().unwrap().get().await.unwrap();
    let data_dir = _destination_tmp.path().join("docs.lance").join("data");
    assert!(
        std::fs::read_dir(data_dir).unwrap().next().is_some(),
        "first partition must have written a data file"
    );
    let ids = row_ids(&ctx, &source).await;
    let files = source.fetch_blob_files("payload", &ids).await.unwrap();
    let file = files[1].as_ref().unwrap();
    assert_eq!(
        file.kind(),
        Some(lance_core::datatypes::BlobKind::Dedicated)
    );
    let missing_path = std::path::Path::new("/").join(file.data_path().unwrap().as_ref());
    drop(files);
    std::fs::remove_file(&missing_path).unwrap();
    let error = insert
        .execute(1, ctx.task_ctx())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(missing_path.file_name().unwrap().to_str().unwrap()),
        "{error}"
    );
    // Check both the live handle and an independently reopened manifest.
    destination.checkout_latest().await.unwrap();
    assert_eq!(destination.version().await.unwrap(), version);
    assert_eq!(destination.count_rows(None).await.unwrap(), 0);
    let reopened = lance::Dataset::open(dataset.uri()).await.unwrap();
    assert_eq!(reopened.version().version, version);
    assert_eq!(reopened.count_rows(None).await.unwrap(), 0);
}
