// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, DictionaryArray, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, UInt8Array,
};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt as _;
use lance_datagen::{BatchCount, BatchGeneratorBuilder, Dimension, RowCount, array};
use lancedb::{DistanceType, Result, connect, table::knn::batch_knn};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a table of 10 rows where row i has vec [4i, 4i+1, 4i+2, 4i+3].
async fn make_table_known_vecs() -> lancedb::Table {
    // We need deterministic vectors for nearest-neighbor assertions, so we
    // still construct the batch manually here.
    let dim = 4_i32;
    let num_rows: usize = 10;
    let id_array = Int32Array::from_iter_values(0..num_rows as i32);
    let values: Float32Array = (0..num_rows * dim as usize)
        .map(|i| i as f32)
        .collect::<Vec<_>>()
        .into();
    let list_array = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        dim,
        Arc::new(values),
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            false,
        ),
    ]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(list_array)]).unwrap();
    let db = connect("memory:///").execute().await.unwrap();
    db.create_table("t", vec![batch]).execute().await.unwrap()
}

fn float_queries(vecs: Vec<Vec<f32>>, dim: usize) -> Arc<FixedSizeListArray> {
    let flat: Float32Array = vecs.into_iter().flatten().collect::<Vec<_>>().into();
    Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            dim as i32,
            Arc::new(flat),
            None,
        )
        .unwrap(),
    )
}

/// Returns `(query_index, id)` pairs for the nearest row per query_index.
fn nearest_ids(batches: &[RecordBatch]) -> Vec<(i32, i32)> {
    let mut rows: Vec<(i32, f32, i32)> = batches
        .iter()
        .flat_map(|b| {
            let qi = b
                .column_by_name("_query_index")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let dist = b
                .column_by_name("_distance")
                .unwrap()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            let id = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..b.num_rows()).map(move |i| (qi.value(i), dist.value(i), id.value(i)))
        })
        .collect();
    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.partial_cmp(&b.1).unwrap()));
    let mut result: Vec<(i32, i32)> = Vec::new();
    let mut last_qi: Option<i32> = None;
    for (qi, _, id) in rows {
        if last_qi != Some(qi) {
            result.push((qi, id));
            last_qi = Some(qi);
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_single_query_l2() -> Result<()> {
    let tbl = make_table_known_vecs().await;
    let native = tbl.as_native().unwrap();

    // Exact match for row 3: vec [12, 13, 14, 15]
    let q = float_queries(vec![vec![12.0, 13.0, 14.0, 15.0]], 4);
    let batches = batch_knn(native, "vec", q, 1, DistanceType::L2)
        .await?
        .collect()
        .await
        .unwrap();

    assert_eq!(nearest_ids(&batches), vec![(0, 3)]);
    Ok(())
}

#[tokio::test]
async fn test_multiple_queries() -> Result<()> {
    let tbl = make_table_known_vecs().await;
    let native = tbl.as_native().unwrap();

    // Query 0 → row 0, query 1 → row 7
    let q = float_queries(
        vec![vec![0.0, 1.0, 2.0, 3.0], vec![28.0, 29.0, 30.0, 31.0]],
        4,
    );
    let batches = batch_knn(native, "vec", q, 1, DistanceType::L2)
        .await?
        .collect()
        .await
        .unwrap();

    assert_eq!(nearest_ids(&batches), vec![(0, 0), (1, 7)]);
    Ok(())
}

#[tokio::test]
async fn test_top_k_returns_k_rows() -> Result<()> {
    let tbl = make_table_known_vecs().await;
    let native = tbl.as_native().unwrap();

    let q = float_queries(vec![vec![0.0, 1.0, 2.0, 3.0]], 4);
    let batches = batch_knn(native, "vec", q, 3, DistanceType::L2)
        .await?
        .collect()
        .await
        .unwrap();

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "k=3 should return exactly 3 rows");
    assert_eq!(nearest_ids(&batches), vec![(0, 0)]);
    Ok(())
}

#[tokio::test]
async fn test_cosine_distance() -> Result<()> {
    let tbl = make_table_known_vecs().await;
    let native = tbl.as_native().unwrap();

    // Row 2 has vec [8, 9, 10, 11]; a scalar multiple is cosine-identical.
    let q = float_queries(vec![vec![8.0, 9.0, 10.0, 11.0]], 4);
    let batches = batch_knn(native, "vec", q, 1, DistanceType::Cosine)
        .await?
        .collect()
        .await
        .unwrap();

    assert_eq!(nearest_ids(&batches), vec![(0, 2)]);
    Ok(())
}

#[tokio::test]
async fn test_hamming_binary_vectors() -> Result<()> {
    let dim = 4_i32;
    let rows_flat: Vec<u8> = vec![
        0b0000_0000,
        0b0000_0000,
        0b0000_0000,
        0b0000_0000, // id=0: all zeros
        0b1111_1111,
        0b1111_1111,
        0b1111_1111,
        0b1111_1111, // id=1: all ones
        0b0000_1111,
        0b0000_1111,
        0b0000_1111,
        0b0000_1111, // id=2: half
        0b0000_0001,
        0b0000_0000,
        0b0000_0000,
        0b0000_0000, // id=3: one bit set
    ];
    let list_array = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::UInt8, true)),
        dim,
        Arc::new(UInt8Array::from(rows_flat)),
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::UInt8, true)), dim),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_iter_values(0..4_i32)),
            Arc::new(list_array),
        ],
    )
    .unwrap();
    let db = connect("memory:///").execute().await.unwrap();
    let tbl = db.create_table("t", vec![batch]).execute().await.unwrap();
    let native = tbl.as_native().unwrap();

    let q_list = Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::UInt8, true)),
            dim,
            Arc::new(UInt8Array::from(vec![0u8; dim as usize])),
            None,
        )
        .unwrap(),
    );
    let batches = batch_knn(native, "vec", q_list, 1, DistanceType::Hamming)
        .await?
        .collect()
        .await
        .unwrap();

    assert_eq!(nearest_ids(&batches), vec![(0, 0)]);
    Ok(())
}

#[tokio::test]
async fn test_output_has_query_vector_column() -> Result<()> {
    let tbl = make_table_known_vecs().await;
    let native = tbl.as_native().unwrap();

    let q = float_queries(vec![vec![0.0, 1.0, 2.0, 3.0]], 4);
    let batches = batch_knn(native, "vec", q, 1, DistanceType::L2)
        .await?
        .collect()
        .await
        .unwrap();

    // Verify the _query_vector column exists and is a Dictionary type.
    let batch = &batches[0];
    let qv_col = batch
        .column_by_name("_query_vector")
        .expect("_query_vector column");
    assert!(
        matches!(qv_col.data_type(), DataType::Dictionary(_, _)),
        "expected Dictionary type, got {:?}",
        qv_col.data_type()
    );
    // The dictionary should have exactly one distinct entry (our single query vector).
    let dict = qv_col
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .unwrap();
    assert_eq!(dict.values().len(), 1);
    Ok(())
}

#[tokio::test]
async fn test_schema_and_row_count_with_random_data() -> Result<()> {
    // Use lance_datagen for table creation — we only check schema/counts here,
    // not exact nearest neighbors.
    let (stream, _schema) = BatchGeneratorBuilder::new()
        .col("id", array::step::<arrow_array::types::Int32Type>())
        .col("vec", array::rand_vec::<Float32Type>(Dimension::from(8u32)))
        .into_reader_stream(RowCount::from(50), BatchCount::from(1));
    let batches: Vec<RecordBatch> = stream.try_collect().await.expect("datagen failed");
    let db = connect("memory:///").execute().await.unwrap();
    let tbl = db.create_table("t", batches).execute().await.unwrap();

    let native = tbl.as_native().unwrap();
    let q = float_queries(vec![vec![0.0; 8], vec![1.0; 8]], 8);
    let batches = batch_knn(native, "vec", q, 3, DistanceType::L2)
        .await?
        .collect()
        .await
        .unwrap();

    // 2 queries × k=3 = 6 rows
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 6);

    // All expected columns present
    let schema = batches[0].schema();
    assert!(schema.field_with_name("id").is_ok());
    assert!(schema.field_with_name("_query_index").is_ok());
    assert!(schema.field_with_name("_distance").is_ok());
    assert!(schema.field_with_name("_query_vector").is_ok());
    // Vector column is projected by default
    assert!(schema.field_with_name("vec").is_ok());
    Ok(())
}
