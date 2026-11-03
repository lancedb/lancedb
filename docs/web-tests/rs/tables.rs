// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;
use std::time::Duration as StdDuration;

use arrow_array::types::Float32Type;
use arrow_array::{
    Array, FixedSizeListArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    RecordBatchIterator, RecordBatchReader, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lancedb::connect;
use lancedb::query::ExecutableQuery;
use lancedb::database::CreateTableMode;
use lancedb::table::{
    ColumnAlteration, Duration, FieldMetadataUpdate, NewColumnTransform, OptimizeAction,
};

// --8<-- [start:update_make_users_reader]
fn make_users_reader(
    ids: Vec<i64>,
    names: Vec<&str>,
    login_counts: Option<Vec<i64>>,
) -> Box<dyn RecordBatchReader + Send> {
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ];
    let mut columns: Vec<Arc<dyn Array>> =
        vec![Arc::new(Int64Array::from(ids)), Arc::new(StringArray::from(names))];

    if let Some(login_counts) = login_counts {
        fields.push(Field::new("login_count", DataType::Int64, true));
        columns.push(Arc::new(Int64Array::from(login_counts)));
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
    Box::new(reader)
}
// --8<-- [end:update_make_users_reader]

// --8<-- [start:versioning_make_quotes_reader]
fn make_quotes_reader(rows: Vec<(i64, &str, &str)>) -> Box<dyn RecordBatchReader + Send> {
    let ids: Vec<i64> = rows.iter().map(|(id, _, _)| *id).collect();
    let authors: Vec<&str> = rows.iter().map(|(_, author, _)| *author).collect();
    let quotes: Vec<&str> = rows.iter().map(|(_, _, quote)| *quote).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("author", DataType::Utf8, false),
        Field::new("quote", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(authors)),
            Arc::new(StringArray::from(quotes)),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
    Box::new(reader)
}
// --8<-- [end:versioning_make_quotes_reader]

// Helper: a table with a `vector` and a `text` column, used to demonstrate
// building indexes on a branch.
fn make_products_reader(n: i32) -> Box<dyn RecordBatchReader + Send> {
    const DIM: usize = 4;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            true,
        ),
        Field::new("text", DataType::Utf8, false),
    ]));

    let ids = Int32Array::from_iter_values(0..n);
    let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        (0..n).map(|i| {
            Some(
                (0..DIM)
                    .map(|d| Some((((i as usize * 31 + d * 7) % 97) as f32) / 97.0))
                    .collect::<Vec<Option<f32>>>(),
            )
        }),
        DIM as i32,
    );
    let texts = StringArray::from_iter_values((0..n).map(|i| format!("product number {i}")));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(vectors), Arc::new(texts)],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
    Box::new(reader)
}

#[allow(dead_code)]
async fn update_connect_enterprise_example() {
    // --8<-- [start:update_connect_enterprise]
    let uri = "db://your-project-slug";
    let api_key = "your-api-key";
    let region = "us-east-1";
    // --8<-- [end:update_connect_enterprise]
    let _ = (uri, api_key, region);
}

#[allow(dead_code)]
async fn update_connect_local_example() {
    // --8<-- [start:update_connect_local]
    let db = connect("./data").execute().await.unwrap();
    // --8<-- [end:update_connect_local]
    let _ = db;
}

#[tokio::main]
async fn main() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_uri = temp_dir.path().to_str().unwrap().to_string();
    let db = connect(&db_uri).execute().await.unwrap();

    // --8<-- [start:create_table_from_dicts]
    struct Location {
        vector: [f32; 2],
        lat: f32,
        long: f32,
    }

    let data = vec![
        Location {
            vector: [1.1, 1.2],
            lat: 45.5,
            long: -122.7,
        },
        Location {
            vector: [0.2, 1.8],
            lat: 40.1,
            long: -74.1,
        },
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            false,
        ),
        Field::new("lat", DataType::Float32, false),
        Field::new("long", DataType::Float32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    data.iter()
                        .map(|row| Some(row.vector.iter().copied().map(Some).collect::<Vec<_>>())),
                    2,
                ),
            ),
            Arc::new(Float32Array::from_iter_values(
                data.iter().map(|row| row.lat),
            )),
            Arc::new(Float32Array::from_iter_values(
                data.iter().map(|row| row.long),
            )),
        ],
    )
    .unwrap();
    let reader: Box<dyn RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema.clone()));
    let table = db
        .create_table("test_table", reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:create_table_from_dicts]
    assert_eq!(table.count_rows(None).await.unwrap(), 2);

    // Seed an existing table so the conflict-handling examples have something to act on.
    let conflict_seed = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    data.iter()
                        .map(|row| Some(row.vector.iter().copied().map(Some).collect::<Vec<_>>())),
                    2,
                ),
            ),
            Arc::new(Float32Array::from_iter_values(
                data.iter().map(|row| row.lat),
            )),
            Arc::new(Float32Array::from_iter_values(
                data.iter().map(|row| row.long),
            )),
        ],
    )
    .unwrap();
    let conflict_seed_reader: Box<dyn RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(vec![Ok(conflict_seed)].into_iter(), schema.clone()));
    db.create_table("conflict_table", conflict_seed_reader)
        .execute()
        .await
        .unwrap();

    // Build readers for the rows we want to ingest. Outside the snippet markers
    // because the docs only need to highlight the `.mode(...)` differences.
    let make_reader = || {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        data.iter().map(|row| {
                            Some(row.vector.iter().copied().map(Some).collect::<Vec<_>>())
                        }),
                        2,
                    ),
                ),
                Arc::new(Float32Array::from_iter_values(
                    data.iter().map(|row| row.lat),
                )),
                Arc::new(Float32Array::from_iter_values(
                    data.iter().map(|row| row.long),
                )),
            ],
        )
        .unwrap();
        let reader: Box<dyn RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema.clone()));
        reader
    };
    let exist_ok_reader = make_reader();
    let overwrite_reader = make_reader();

    // --8<-- [start:create_table_conflict_handling]
    // Idempotent open: reuse the existing table if it exists.
    // The provided data is ignored; the schema is validated against the
    // existing table and a mismatch raises an error.
    let _conflict_table = db
        .create_table("conflict_table", exist_ok_reader)
        .mode(CreateTableMode::exist_ok(|req| req))
        .execute()
        .await
        .unwrap();

    // Overwrite: drop the existing table and create a new one with the
    // provided data. This permanently discards the old table's data.
    let conflict_table = db
        .create_table("conflict_table", overwrite_reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:create_table_conflict_handling]
    assert_eq!(conflict_table.count_rows(None).await.unwrap(), 2);

    // --8<-- [start:create_table_custom_schema]
    let custom_schema = Arc::new(Schema::new(vec![
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            false,
        ),
        Field::new("lat", DataType::Float32, false),
        Field::new("long", DataType::Float32, false),
    ]));

    let custom_batch = RecordBatch::try_new(
        custom_schema.clone(),
        vec![
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    vec![
                        Some(vec![Some(1.1), Some(1.2), Some(1.3), Some(1.4)]),
                        Some(vec![Some(0.2), Some(1.8), Some(0.4), Some(3.6)]),
                    ],
                    4,
                ),
            ),
            Arc::new(Float32Array::from(vec![45.5, 40.1])),
            Arc::new(Float32Array::from(vec![-122.7, -74.1])),
        ],
    )
    .unwrap();
    let custom_reader: Box<dyn RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(vec![Ok(custom_batch)].into_iter(), custom_schema.clone()));
    let custom_table = db
        .create_table("my_table_custom_schema", custom_reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:create_table_custom_schema]
    assert_eq!(custom_table.count_rows(None).await.unwrap(), 2);

    // --8<-- [start:create_table_from_arrow]
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 16),
            false,
        ),
        Field::new("text", DataType::Utf8, false),
    ]));

    let arrow_batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    vec![Some(vec![Some(0.1); 16]), Some(vec![Some(0.2); 16])],
                    16,
                ),
            ),
            Arc::new(StringArray::from(vec!["foo", "bar"])),
        ],
    )
    .unwrap();
    let arrow_reader: Box<dyn RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(vec![Ok(arrow_batch)].into_iter(), arrow_schema.clone()));
    let arrow_table = db
        .create_table("arrow_table_example", arrow_reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:create_table_from_arrow]
    assert_eq!(arrow_table.count_rows(None).await.unwrap(), 2);

    // --8<-- [start:create_table_from_iterator]
    let batch_schema = Arc::new(Schema::new(vec![
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            false,
        ),
        Field::new("item", DataType::Utf8, false),
        Field::new("price", DataType::Float32, false),
    ]));

    let batches = (0..5)
        .map(|i| {
            RecordBatch::try_new(
                batch_schema.clone(),
                vec![
                    Arc::new(
                        FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                            vec![
                                Some(vec![Some(3.1 + i as f32), Some(4.1), Some(5.1), Some(6.1)]),
                                Some(vec![
                                    Some(5.9),
                                    Some(26.5 + i as f32),
                                    Some(4.7),
                                    Some(32.8),
                                ]),
                            ],
                            4,
                        ),
                    ),
                    Arc::new(StringArray::from(vec![
                        format!("item{}", i * 2 + 1),
                        format!("item{}", i * 2 + 2),
                    ])),
                    Arc::new(Float32Array::from(vec![
                        ((i * 2 + 1) * 10) as f32,
                        ((i * 2 + 2) * 10) as f32,
                    ])),
                ],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();

    let batch_reader: Box<dyn RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(batches.into_iter().map(Ok), batch_schema.clone()));
    let batch_table = db
        .create_table("batched_table", batch_reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:create_table_from_iterator]
    assert_eq!(batch_table.count_rows(None).await.unwrap(), 10);

    // --8<-- [start:open_existing_table]
    let open_schema = Arc::new(Schema::new(vec![
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            false,
        ),
        Field::new("lat", DataType::Float32, false),
        Field::new("long", DataType::Float32, false),
    ]));
    let open_batch = RecordBatch::try_new(
        open_schema.clone(),
        vec![
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    vec![Some(vec![Some(1.1), Some(1.2)])],
                    2,
                ),
            ),
            Arc::new(Float32Array::from(vec![45.5])),
            Arc::new(Float32Array::from(vec![-122.7])),
        ],
    )
    .unwrap();
    let open_reader: Box<dyn RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(vec![Ok(open_batch)].into_iter(), open_schema.clone()));
    db.create_table("test_table", open_reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();

    println!("{:?}", db.table_names().execute().await.unwrap());

    let opened_table = db.open_table("test_table").execute().await.unwrap();
    // --8<-- [end:open_existing_table]
    assert_eq!(opened_table.count_rows(None).await.unwrap(), 1);

    // --8<-- [start:create_empty_table]
    let empty_schema = Arc::new(Schema::new(vec![
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            false,
        ),
        Field::new("item", DataType::Utf8, false),
        Field::new("price", DataType::Float32, false),
    ]));
    let empty_table = db
        .create_empty_table("test_empty_table", empty_schema)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:create_empty_table]
    assert_eq!(empty_table.count_rows(None).await.unwrap(), 0);

    // --8<-- [start:drop_table]
    let drop_schema = Arc::new(Schema::new(vec![
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            false,
        ),
        Field::new("lat", DataType::Float32, false),
    ]));
    let drop_batch = RecordBatch::try_new(
        drop_schema.clone(),
        vec![
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    vec![Some(vec![Some(1.1), Some(1.2)])],
                    2,
                ),
            ),
            Arc::new(Float32Array::from(vec![45.5])),
        ],
    )
    .unwrap();
    let drop_reader: Box<dyn RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(vec![Ok(drop_batch)].into_iter(), drop_schema.clone()));
    db.create_table("my_table", drop_reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();

    db.drop_table("my_table", &[]).await.unwrap();
    // --8<-- [end:drop_table]
    assert!(
        !db.table_names()
            .execute()
            .await
            .unwrap()
            .contains(&"my_table".to_string())
    );

    // --8<-- [start:schema_add_setup]
    let schema_add_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 128),
            false,
        ),
    ]));
    let schema_add_batch = RecordBatch::try_new(
        schema_add_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Laptop", "Smartphone", "Headphones"])),
            Arc::new(Float64Array::from(vec![1200.0, 800.0, 150.0])),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    vec![
                        Some(vec![Some(0.1_f32); 128]),
                        Some(vec![Some(0.2_f32); 128]),
                        Some(vec![Some(0.3_f32); 128]),
                    ],
                    128,
                ),
            ),
        ],
    )
    .unwrap();
    let schema_add_reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
        vec![Ok(schema_add_batch)].into_iter(),
        schema_add_schema.clone(),
    ));
    let schema_add_table = db
        .create_table("schema_evolution_add_example", schema_add_reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:schema_add_setup]
    assert_eq!(schema_add_table.count_rows(None).await.unwrap(), 3);

    // --8<-- [start:add_columns_calculated]
    // Add a discounted price column (10% discount)
    schema_add_table
        .add_columns(
            NewColumnTransform::SqlExpressions(vec![(
                "discounted_price".to_string(),
                "cast((price * 0.9) as float)".to_string(),
            )]),
            None,
        )
        .await
        .unwrap();
    // --8<-- [end:add_columns_calculated]

    // --8<-- [start:add_columns_default_values]
    // Add a stock status column with default value
    schema_add_table
        .add_columns(
            NewColumnTransform::SqlExpressions(vec![(
                "in_stock".to_string(),
                "cast(true as boolean)".to_string(),
            )]),
            None,
        )
        .await
        .unwrap();
    // --8<-- [end:add_columns_default_values]

    // --8<-- [start:add_columns_nullable]
    // Add a nullable timestamp column
    schema_add_table
        .add_columns(
            NewColumnTransform::SqlExpressions(vec![(
                "last_ordered".to_string(),
                "cast(NULL as timestamp)".to_string(),
            )]),
            None,
        )
        .await
        .unwrap();
    // --8<-- [end:add_columns_nullable]

    // --8<-- [start:add_feature_columns_sql]
    schema_add_table
        .add_columns(
            NewColumnTransform::SqlExpressions(vec![
                (
                    "price_per_id".to_string(),
                    "cast(price / id as float)".to_string(),
                ),
                ("price_log".to_string(), "ln(price)".to_string()),
                (
                    "price_score".to_string(),
                    "cast(price / (price + 100.0) as float)".to_string(),
                ),
            ]),
            None,
        )
        .await
        .unwrap();
    // --8<-- [end:add_feature_columns_sql]

    // --8<-- [start:schema_alter_setup]
    let schema_alter_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("price", DataType::Int32, false),
        Field::new("discount_price", DataType::Float64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 128),
            false,
        ),
    ]));
    let schema_alter_batch = RecordBatch::try_new(
        schema_alter_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Laptop", "Smartphone"])),
            Arc::new(Int32Array::from(vec![1200, 800])),
            Arc::new(Float64Array::from(vec![1080.0, 720.0])),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    vec![Some(vec![Some(0.1_f32); 128]), Some(vec![Some(0.2_f32); 128])],
                    128,
                ),
            ),
        ],
    )
    .unwrap();
    let schema_alter_reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
        vec![Ok(schema_alter_batch)].into_iter(),
        schema_alter_schema.clone(),
    ));
    let schema_alter_table = db
        .create_table("schema_evolution_alter_example", schema_alter_reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:schema_alter_setup]
    assert_eq!(schema_alter_table.count_rows(None).await.unwrap(), 2);

    // --8<-- [start:alter_columns_rename]
    // Rename discount_price to sale_price
    schema_alter_table
        .alter_columns(&[ColumnAlteration::new("discount_price".to_string())
            .rename("sale_price".to_string())])
        .await
        .unwrap();
    // --8<-- [end:alter_columns_rename]

    // --8<-- [start:alter_columns_data_type]
    // Change price from int32 to int64 for larger numbers
    schema_alter_table
        .alter_columns(&[ColumnAlteration::new("price".to_string()).cast_to(DataType::Int64)])
        .await
        .unwrap();
    // --8<-- [end:alter_columns_data_type]

    // --8<-- [start:alter_columns_nullable]
    // Make the name column nullable
    schema_alter_table
        .alter_columns(&[ColumnAlteration::new("name".to_string()).set_nullable(true)])
        .await
        .unwrap();
    // --8<-- [end:alter_columns_nullable]

    // --8<-- [start:alter_columns_multiple]
    // Rename, change type, and make nullable in one operation
    schema_alter_table
        .alter_columns(&[ColumnAlteration::new("sale_price".to_string())
            .rename("final_price".to_string())
            .cast_to(DataType::Float64)
            .set_nullable(true)])
        .await
        .unwrap();
    // --8<-- [end:alter_columns_multiple]

    // --8<-- [start:alter_columns_with_expression]
    // For custom transforms, create a new column from a SQL expression.
    let expression_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price_text", DataType::Utf8, false),
    ]));
    let expression_batch = RecordBatch::try_new(
        expression_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["$100"])),
        ],
    )
    .unwrap();
    let expression_reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
        vec![Ok(expression_batch)].into_iter(),
        expression_schema.clone(),
    ));
    let expression_table = db
        .create_table("schema_evolution_expression_example", expression_reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();

    expression_table
        .add_columns(
            NewColumnTransform::SqlExpressions(vec![(
                "price_numeric".to_string(),
                "cast(replace(price_text, '$', '') as int)".to_string(),
            )]),
            None,
        )
        .await
        .unwrap();
    expression_table.drop_columns(&["price_text"]).await.unwrap();
    expression_table
        .alter_columns(&[ColumnAlteration::new("price_numeric".to_string())
            .rename("price".to_string())])
        .await
        .unwrap();
    // --8<-- [end:alter_columns_with_expression]
    assert_eq!(expression_table.count_rows(None).await.unwrap(), 1);

    // --8<-- [start:schema_drop_setup]
    let schema_drop_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("temp_col1", DataType::Utf8, false),
        Field::new("temp_col2", DataType::Int32, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 128),
            false,
        ),
    ]));
    let schema_drop_batch = RecordBatch::try_new(
        schema_drop_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Laptop", "Smartphone", "Headphones"])),
            Arc::new(Float64Array::from(vec![1200.0, 800.0, 150.0])),
            Arc::new(StringArray::from(vec!["X", "Y", "Z"])),
            Arc::new(Int32Array::from(vec![100, 200, 300])),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    vec![
                        Some(vec![Some(0.1_f32); 128]),
                        Some(vec![Some(0.2_f32); 128]),
                        Some(vec![Some(0.3_f32); 128]),
                    ],
                    128,
                ),
            ),
        ],
    )
    .unwrap();
    let schema_drop_reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
        vec![Ok(schema_drop_batch)].into_iter(),
        schema_drop_schema.clone(),
    ));
    let schema_drop_table = db
        .create_table("schema_evolution_drop_example", schema_drop_reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:schema_drop_setup]
    assert_eq!(schema_drop_table.count_rows(None).await.unwrap(), 3);

    // --8<-- [start:drop_columns_single]
    // Remove the first temporary column
    schema_drop_table.drop_columns(&["temp_col1"]).await.unwrap();
    // --8<-- [end:drop_columns_single]

    // --8<-- [start:drop_columns_multiple]
    // Remove the second temporary column
    schema_drop_table.drop_columns(&["temp_col2"]).await.unwrap();
    // --8<-- [end:drop_columns_multiple]

    // --8<-- [start:alter_vector_column]
    let old_dim = 384;
    let new_dim = 1024;
    let vector_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                old_dim,
            ),
            true,
        ),
    ]));
    let vector_batch = RecordBatch::try_new(
        vector_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    vec![Some(vec![Some(0.1_f32); old_dim as usize])],
                    old_dim,
                ),
            ),
        ],
    )
    .unwrap();
    let vector_reader: Box<dyn RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(vec![Ok(vector_batch)].into_iter(), vector_schema.clone()));
    let vector_table = db
        .create_table("vector_alter_example", vector_reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();

    // Changing FixedSizeList dimensions (384 -> 1024) is not supported via alter_columns.
    // Use add_columns + drop_columns + alter_columns(rename) to replace the column.
    vector_table
        .add_columns(
            NewColumnTransform::SqlExpressions(vec![(
                "embedding_v2".to_string(),
                format!("arrow_cast(NULL, 'FixedSizeList({}, Float32)')", new_dim),
            )]),
            None,
        )
        .await
        .unwrap();
    vector_table.drop_columns(&["embedding"]).await.unwrap();
    vector_table
        .alter_columns(&[ColumnAlteration::new("embedding_v2".to_string())
            .rename("embedding".to_string())])
        .await
        .unwrap();
    // --8<-- [end:alter_vector_column]
    assert_eq!(vector_table.count_rows(None).await.unwrap(), 1);

    let field_metadata_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
    ]));
    let field_metadata_batch = RecordBatch::try_new(
        field_metadata_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![0, 1])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap();
    let field_metadata_reader: Box<dyn RecordBatchReader + Send> = Box::new(
        RecordBatchIterator::new(vec![Ok(field_metadata_batch)].into_iter(), field_metadata_schema),
    );
    let field_metadata_table = db
        .create_table("schema_field_metadata_example", field_metadata_reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();

    // --8<-- [start:schema_field_metadata_merge]
    // Set two metadata keys on the `category` field.
    let res = field_metadata_table
        .update_field_metadata(&[FieldMetadataUpdate::new("category")
            .set("unit", "label")
            .set("pii", "false")])
        .await
        .unwrap();
    println!("version: {}", res.version);

    // Merge: add a new key, delete one with `.remove`, keep the rest.
    field_metadata_table
        .update_field_metadata(&[FieldMetadataUpdate::new("category")
            .set("source", "import")
            .remove("pii")])
        .await
        .unwrap();
    // --8<-- [end:schema_field_metadata_merge]

    // --8<-- [start:schema_field_metadata_replace]
    field_metadata_table
        .update_field_metadata(&[FieldMetadataUpdate::new("category")
            .set("owner", "search-team")
            .replace()])
        .await
        .unwrap();
    // --8<-- [end:schema_field_metadata_replace]

    // --8<-- [start:update_example_table_setup]
    let table = db
        .create_table(
            "users_example",
            make_users_reader(vec![1, 2], vec!["Alice", "Bob"], Some(vec![10, 20])),
        )
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:update_example_table_setup]
    let _ = table;

    // --8<-- [start:update_operation]
    let table = db
        .create_table(
            "users_example",
            make_users_reader(vec![1, 2], vec!["Alice", "Bob"], Some(vec![10, 20])),
        )
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    table
        .update()
        .only_if("id = 2")
        .column("name", "'Bobby'")
        .execute()
        .await
        .unwrap();
    // --8<-- [end:update_operation]

    // --8<-- [start:update_using_sql]
    let table = db
        .create_table(
            "users_example",
            make_users_reader(vec![1, 2], vec!["Alice", "Bob"], Some(vec![10, 20])),
        )
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    table
        .update()
        .only_if("id = 2")
        .column("login_count", "login_count + 1")
        .execute()
        .await
        .unwrap();
    // --8<-- [end:update_using_sql]

    // --8<-- [start:merge_matched_update_only]
    let table = db
        .create_table(
            "users_example",
            make_users_reader(vec![1, 2], vec!["Alice", "Bob"], Some(vec![10, 20])),
        )
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();

    let mut merge_insert = table.merge_insert(&["id"]);
    merge_insert.when_matched_update_all(None);
    merge_insert
        .execute(make_users_reader(
            vec![2, 3],
            vec!["Bobby", "Charlie"],
            Some(vec![21, 5]),
        ))
        .await
        .unwrap();
    // --8<-- [end:merge_matched_update_only]

    // --8<-- [start:insert_if_not_exists]
    let table = db
        .create_table(
            "users_example",
            make_users_reader(vec![1, 2], vec!["Alice", "Bob"], Some(vec![10, 20])),
        )
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();

    let mut merge_insert = table.merge_insert(&["id"]);
    merge_insert.when_not_matched_insert_all();
    merge_insert
        .execute(make_users_reader(
            vec![2, 3],
            vec!["Bobby", "Charlie"],
            Some(vec![21, 5]),
        ))
        .await
        .unwrap();
    // --8<-- [end:insert_if_not_exists]

    // --8<-- [start:merge_update_insert]
    let table = db
        .create_table(
            "users_example",
            make_users_reader(vec![1, 2], vec!["Alice", "Bob"], Some(vec![10, 20])),
        )
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();

    let mut merge_insert = table.merge_insert(&["id"]);
    merge_insert
        .when_matched_update_all(None)
        .when_not_matched_insert_all();
    merge_insert
        .execute(make_users_reader(
            vec![2, 3],
            vec!["Bobby", "Charlie"],
            Some(vec![21, 5]),
        ))
        .await
        .unwrap();
    // --8<-- [end:merge_update_insert]

    // --8<-- [start:merge_delete_missing_by_source]
    let table = db
        .create_table(
            "users_example",
            make_users_reader(
                vec![1, 2, 3],
                vec!["Alice", "Bob", "Charlie"],
                Some(vec![10, 20, 5]),
            ),
        )
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();

    let mut merge_insert = table.merge_insert(&["id"]);
    merge_insert
        .when_matched_update_all(None)
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete(None);
    merge_insert
        .execute(make_users_reader(
            vec![2, 3],
            vec!["Bobby", "Charlie"],
            Some(vec![21, 5]),
        ))
        .await
        .unwrap();
    // --8<-- [end:merge_delete_missing_by_source]

    // --8<-- [start:merge_partial_columns]
    let table = db
        .create_table(
            "users_example",
            make_users_reader(vec![1, 2], vec!["Alice", "Bob"], Some(vec![10, 20])),
        )
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();

    let mut merge_insert = table.merge_insert(&["id"]);
    merge_insert
        .when_matched_update_all(None)
        .when_not_matched_insert_all();
    merge_insert
        .execute(make_users_reader(vec![2, 3], vec!["Bobby", "Charlie"], None))
        .await
        .unwrap();
    // --8<-- [end:merge_partial_columns]

    let table = db
        .create_table(
            "users_example",
            make_users_reader(
                vec![1, 2, 3],
                vec!["Alice", "Bob", "Charlie"],
                Some(vec![10, 20, 5]),
            ),
        )
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();

    // --8<-- [start:delete_operation]
    // delete data
    let predicate = "id = 3";
    table.delete(predicate).await.unwrap();
    // --8<-- [end:delete_operation]

    let table = db
        .create_table(
            "users_cleanup_example",
            make_users_reader(
                vec![1, 2, 3],
                vec!["Alice", "Bob", "Charlie"],
                Some(vec![10, 20, 5]),
            ),
        )
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();

    // --8<-- [start:update_optimize_cleanup]
    table
        .optimize(OptimizeAction::Prune {
            older_than: Some(Duration::days(1)),
            delete_unverified: None,
            error_if_tagged_old_versions: None,
        })
        .await
        .unwrap();
    // --8<-- [end:update_optimize_cleanup]

    // --8<-- [start:consistency_strong]
    let strong_writer_db = connect(&db_uri).execute().await.unwrap();
    let strong_reader_db = connect(&db_uri)
        .read_consistency_interval(StdDuration::from_secs(0))
        .execute()
        .await
        .unwrap();
    let strong_writer_table = strong_writer_db
        .create_table(
            "consistency_strong_table",
            make_users_reader(vec![1], vec!["Alice"], None),
        )
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    let strong_reader_table = strong_reader_db
        .open_table("consistency_strong_table")
        .execute()
        .await
        .unwrap();
    strong_writer_table
        .add(make_users_reader(vec![2], vec!["Bob"], None))
        .execute()
        .await
        .unwrap();
    let strong_rows_after_write = strong_reader_table.count_rows(None).await.unwrap();
    println!(
        "Rows visible with strong consistency: {}",
        strong_rows_after_write
    );
    // --8<-- [end:consistency_strong]
    assert_eq!(strong_rows_after_write, 2);

    // --8<-- [start:consistency_eventual]
    let eventual_writer_db = connect(&db_uri).execute().await.unwrap();
    let eventual_reader_db = connect(&db_uri)
        .read_consistency_interval(StdDuration::from_secs(3600))
        .execute()
        .await
        .unwrap();
    let eventual_writer_table = eventual_writer_db
        .create_table(
            "consistency_eventual_table",
            make_users_reader(vec![1], vec!["Alice"], None),
        )
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    let eventual_reader_table = eventual_reader_db
        .open_table("consistency_eventual_table")
        .execute()
        .await
        .unwrap();
    eventual_writer_table
        .add(make_users_reader(vec![2], vec!["Bob"], None))
        .execute()
        .await
        .unwrap();
    let eventual_rows_after_write = eventual_reader_table.count_rows(None).await.unwrap();
    println!(
        "Rows visible before eventual refresh interval: {}",
        eventual_rows_after_write
    );
    // --8<-- [end:consistency_eventual]
    assert_eq!(eventual_rows_after_write, 1);

    // --8<-- [start:consistency_checkout_latest]
    let checkout_writer_db = connect(&db_uri).execute().await.unwrap();
    let checkout_reader_db = connect(&db_uri).execute().await.unwrap();
    let checkout_writer_table = checkout_writer_db
        .create_table(
            "consistency_checkout_latest_table",
            make_users_reader(vec![1], vec!["Alice"], None),
        )
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    let checkout_reader_table = checkout_reader_db
        .open_table("consistency_checkout_latest_table")
        .execute()
        .await
        .unwrap();
    checkout_writer_table
        .add(make_users_reader(vec![2], vec!["Bob"], None))
        .execute()
        .await
        .unwrap();
    let rows_before_refresh = checkout_reader_table.count_rows(None).await.unwrap();
    println!("Rows before checkout_latest: {}", rows_before_refresh);
    checkout_reader_table.checkout_latest().await.unwrap();
    let rows_after_refresh = checkout_reader_table.count_rows(None).await.unwrap();
    println!("Rows after checkout_latest: {}", rows_after_refresh);
    // --8<-- [end:consistency_checkout_latest]
    assert_eq!(rows_before_refresh, 1);
    assert_eq!(rows_after_refresh, 2);

    // --8<-- [start:versioning_basic_setup]
    let table_name = "quotes_versioning_example";
    let data = vec![
        (1, "Richard", "Wubba Lubba Dub Dub!"),
        (2, "Morty", "Rick, what's going on?"),
        (3, "Richard", "I turned myself into a pickle, Morty!"),
    ];

    let table = db
        .create_table(table_name, make_quotes_reader(data))
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:versioning_basic_setup]
    assert_eq!(table.count_rows(None).await.unwrap(), 3);

    // --8<-- [start:versioning_check_initial_version]
    let versions = table.list_versions().await.unwrap();
    let current_version = table.version().await.unwrap();
    println!("Number of versions after creation: {}", versions.len());
    println!("Current version: {}", current_version);
    // --8<-- [end:versioning_check_initial_version]
    assert_eq!(versions.len(), 1);
    assert_eq!(current_version, versions.last().unwrap().version);

    // --8<-- [start:versioning_update_data]
    table
        .update()
        .only_if("author = 'Richard'")
        .column("author", "'Richard Daniel Sanchez'")
        .execute()
        .await
        .unwrap();
    let rows_after_update = table
        .count_rows(Some("author = 'Richard Daniel Sanchez'".to_string()))
        .await
        .unwrap();
    println!(
        "Rows updated to Richard Daniel Sanchez: {}",
        rows_after_update
    );
    // --8<-- [end:versioning_update_data]
    assert_eq!(rows_after_update, 2);

    // --8<-- [start:versioning_add_data]
    let more_data = vec![
        (4, "Richard Daniel Sanchez", "That's the way the news goes!"),
        (5, "Morty", "Aww geez, Rick!"),
    ];
    table
        .add(make_quotes_reader(more_data))
        .execute()
        .await
        .unwrap();
    // --8<-- [end:versioning_add_data]
    assert_eq!(table.count_rows(None).await.unwrap(), 5);

    // --8<-- [start:versioning_check_versions_after_mod]
    let versions_after_mod = table.list_versions().await.unwrap();
    let version_count_after_mod = versions_after_mod.len();
    let version_after_mod = table.version().await.unwrap();
    println!(
        "Number of versions after modifications: {}",
        version_count_after_mod
    );
    println!("Current version: {}", version_after_mod);
    // --8<-- [end:versioning_check_versions_after_mod]
    assert!(version_count_after_mod >= 2);
    assert_eq!(version_after_mod, versions_after_mod.last().unwrap().version);

    // --8<-- [start:versioning_list_all_versions]
    let all_versions = table.list_versions().await.unwrap();
    for v in &all_versions {
        println!("Version {}, created at {}", v.version, v.timestamp);
    }
    // --8<-- [end:versioning_list_all_versions]
    assert!(!all_versions.is_empty());

    // --8<-- [start:versioning_rollback]
    table.checkout(version_after_mod).await.unwrap();
    table.restore().await.unwrap();
    let versions_after_rollback = table.list_versions().await.unwrap();
    let version_count_after_rollback = versions_after_rollback.len();
    println!(
        "Total number of versions after rollback: {}",
        version_count_after_rollback
    );
    // --8<-- [end:versioning_rollback]
    assert_eq!(version_count_after_rollback, version_count_after_mod + 1);
    assert_eq!(table.count_rows(None).await.unwrap(), 5);

    // --8<-- [start:versioning_checkout_latest]
    table.checkout_latest().await.unwrap();
    // --8<-- [end:versioning_checkout_latest]
    let latest_version = table.version().await.unwrap();
    let versions_after_checkout = table.list_versions().await.unwrap();
    assert_eq!(latest_version, versions_after_checkout.last().unwrap().version);

    // --8<-- [start:versioning_delete_data]
    table.delete("author = 'Morty'").await.unwrap();
    let rows_after_deletion = table.count_rows(None).await.unwrap();
    println!("Number of rows after deletion: {}", rows_after_deletion);
    // --8<-- [end:versioning_delete_data]
    assert_eq!(rows_after_deletion, 3);

    // Setup: build a table with three versions to operate on with tags.
    let tags_table = db
        .create_table(
            "quotes_tags_example",
            make_quotes_reader(vec![(1, "Richard", "Wubba Lubba Dub Dub!")]),
        )
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap(); // v1
    tags_table
        .add(make_quotes_reader(vec![(2, "Morty", "Aww geez, Rick!")]))
        .execute()
        .await
        .unwrap(); // v2
    tags_table
        .add(make_quotes_reader(vec![(3, "Summer", "Whatever, Grandpa")]))
        .execute()
        .await
        .unwrap(); // v3

    // --8<-- [start:versioning_tags]
    let mut tags = tags_table.tags().await.unwrap();

    // Create a tag pointing at a specific version
    tags.create("baseline", 1).await.unwrap();
    let current_version = tags_table.version().await.unwrap();
    tags.create("with-edits", current_version).await.unwrap();

    // List all tags on this table
    let all_tags = tags.list().await.unwrap();
    println!("Tags: {:?}", all_tags);

    // Look up the version a tag points at
    let baseline_version = tags.get_version("baseline").await.unwrap();
    println!("baseline -> v{}", baseline_version);

    // Move an existing tag to a different version
    tags.update("baseline", 2).await.unwrap();

    // Check out a version by tag name (separate method in Rust)
    tags_table.checkout_tag("baseline").await.unwrap();
    println!("Current version: {}", tags_table.version().await.unwrap());

    // Delete a tag (does not delete the underlying version)
    tags.delete("with-edits").await.unwrap();

    // Return to the latest version
    tags_table.checkout_latest().await.unwrap();
    // --8<-- [end:versioning_tags]
    assert_eq!(tags_table.version().await.unwrap(), 3);
    let remaining = tags.list().await.unwrap();
    assert!(remaining.contains_key("baseline"));
    assert!(!remaining.contains_key("with-edits"));

    // Setup: a fresh quotes table to demonstrate branches on.
    let branches_table = db
        .create_table(
            "quotes_branches_example",
            make_quotes_reader(vec![
                (1, "Lancelot", "My lance never fails."),
                (2, "Arthur", "Long live Camelot!"),
                (3, "Merlin", "Magic always has a price."),
            ]),
        )
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();

    use lancedb::table::Ref;

    // --8<-- [start:branch_create]
    // Fork an isolated, writable branch from main's latest version.
    // `create_branch` returns a table handle scoped to the new branch.
    let branch = branches_table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    // --8<-- [end:branch_create]

    // --8<-- [start:branch_write]
    // Writes land on the branch handle only; main is left untouched.
    branch
        .add(make_quotes_reader(vec![(4, "Lancelot", "For the realm!")]))
        .execute()
        .await
        .unwrap();
    println!("Branch rows: {}", branch.count_rows(None).await.unwrap()); // 4
    println!("Main rows: {}", branches_table.count_rows(None).await.unwrap()); // 3

    // List every branch, each mapped to its metadata (including its fork point).
    println!("Branches: {:?}", branches_table.list_branches().await.unwrap());
    // --8<-- [end:branch_write]

    // --8<-- [start:branch_reopen]
    // Reopen an existing branch by name from the table handle...
    let checked_out = branches_table.checkout_branch("exp", None).await.unwrap();
    // ...or open it directly via the connection's builder.
    let opened = db
        .open_table("quotes_branches_example")
        .branch("exp")
        .execute()
        .await
        .unwrap();
    println!(
        "Reopened rows: {}, {}",
        checked_out.count_rows(None).await.unwrap(),
        opened.count_rows(None).await.unwrap()
    ); // both 4
    // --8<-- [end:branch_reopen]

    // --8<-- [start:branch_delete]
    // Delete the branch and its branch-local history. Data on main is safe.
    branches_table.delete_branch("exp").await.unwrap();
    // --8<-- [end:branch_delete]

    assert_eq!(branches_table.count_rows(None).await.unwrap(), 3);
    assert!(!branches_table.list_branches().await.unwrap().contains_key("exp"));

    // Setup: a branch with row results that we want to apply to main.
    let candidate = branches_table
        .create_branch("candidate", Ref::Version(None, None))
        .await
        .unwrap();
    candidate
        .update()
        .only_if("id = 1")
        .column("quote", "'Revised on the branch'")
        .execute()
        .await
        .unwrap();
    candidate
        .add(make_quotes_reader(vec![(
            4,
            "Galahad",
            "The grail awaits.",
        )]))
        .execute()
        .await
        .unwrap();

    // --8<-- [start:branch_upsert_to_main]
    // This is a row-level upsert, not a merge of branch histories.
    // `merge_insert` updates matching rows and inserts new rows using a stable
    // unique key. Filter the branch read if you only want to apply some results.
    let schema = candidate.schema().await.unwrap();
    let batches = candidate
        .query()
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let rows_to_apply = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

    let mut merge = branches_table.merge_insert(&["id"]);
    merge
        .when_matched_update_all(None) // update rows that already exist on main
        .when_not_matched_insert_all(); // insert rows that are new on the branch
    merge.execute(Box::new(rows_to_apply)).await.unwrap();
    // --8<-- [end:branch_upsert_to_main]

    assert_eq!(branches_table.count_rows(None).await.unwrap(), 4);
    branches_table.delete_branch("candidate").await.unwrap();

    // Setup: a larger table with a vector and a text column to index.
    let products = db
        .create_table("products_branch_index", make_products_reader(512))
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();

    // --8<-- [start:branch_index]
    use lancedb::index::scalar::FtsIndexBuilder;
    use lancedb::index::Index;

    // Build and validate indexes on a branch before using the configuration on
    // main.
    let dev = products
        .create_branch("index-dev", Ref::Version(None, None))
        .await
        .unwrap();

    // A vector (ANN) index and a full-text search index, both branch-scoped.
    dev.create_index(&["vector"], Index::Auto)
        .execute()
        .await
        .unwrap();
    dev.create_index(&["text"], Index::FTS(FtsIndexBuilder::default()))
        .execute()
        .await
        .unwrap();

    // Both indexes live only on the branch; main still has none.
    println!("Branch indexes: {}", dev.list_indices().await.unwrap().len()); // 2
    println!("Main indexes: {}", products.list_indices().await.unwrap().len()); // 0
    // --8<-- [end:branch_index]

    assert_eq!(dev.list_indices().await.unwrap().len(), 2);
    assert_eq!(products.list_indices().await.unwrap().len(), 0);
    products.delete_branch("index-dev").await.unwrap();
}
