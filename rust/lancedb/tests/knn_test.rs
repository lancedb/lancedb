// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::{Array, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, UInt8Array};
use arrow_schema::{DataType, Field, Schema};
use lancedb::{DistanceType, Result, connect, table::knn::batch_knn};

fn make_float_batch(num_rows: usize, dim: usize) -> RecordBatch {
    let id_array = Int32Array::from_iter_values(0..num_rows as i32);
    // Row i has vec [dim*i, dim*i+1, ..., dim*i+dim-1]
    let values: Float32Array = (0..num_rows * dim)
        .map(|i| i as f32)
        .collect::<Vec<_>>()
        .into();
    let list_array = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        dim as i32,
        Arc::new(values),
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            ),
            false,
        ),
    ]));
    RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(list_array)]).unwrap()
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

/// For each query_index in `batches`, return the id of the nearest row.
fn nearest_ids(batches: &[RecordBatch]) -> Vec<(i32, i32)> {
    let mut rows: Vec<(i32, f32, i32)> = batches
        .iter()
        .flat_map(|b| {
            let qi = b
                .column_by_name("query_index")
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

#[tokio::test]
async fn test_single_query_l2() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let db = connect(dir.path().to_str().unwrap()).execute().await?;
    let batch = make_float_batch(10, 4);
    let tbl = db.create_table("t", vec![batch]).execute().await?;
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
    let dir = tempfile::tempdir().unwrap();
    let db = connect(dir.path().to_str().unwrap()).execute().await?;
    let batch = make_float_batch(10, 4);
    let tbl = db.create_table("t", vec![batch]).execute().await?;
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
    let dir = tempfile::tempdir().unwrap();
    let db = connect(dir.path().to_str().unwrap()).execute().await?;
    let batch = make_float_batch(10, 4);
    let tbl = db.create_table("t", vec![batch]).execute().await?;
    let native = tbl.as_native().unwrap();

    let q = float_queries(vec![vec![0.0, 1.0, 2.0, 3.0]], 4);
    let batches = batch_knn(native, "vec", q, 3, DistanceType::L2)
        .await?
        .collect()
        .await
        .unwrap();

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "k=3 should return exactly 3 rows");
    // Nearest is still row 0
    assert_eq!(nearest_ids(&batches), vec![(0, 0)]);
    Ok(())
}

#[tokio::test]
async fn test_cosine_distance() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let db = connect(dir.path().to_str().unwrap()).execute().await?;
    let batch = make_float_batch(5, 4);
    let tbl = db.create_table("t", vec![batch]).execute().await?;
    let native = tbl.as_native().unwrap();

    // Row 2 has vec [8, 9, 10, 11]; a scalar multiple is cosine-identical
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
    let dir = tempfile::tempdir().unwrap();
    let db = connect(dir.path().to_str().unwrap()).execute().await?;

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
    let tbl = db.create_table("t", vec![batch]).execute().await?;
    let native = tbl.as_native().unwrap();

    // All-zero query — nearest by Hamming is id=0
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
