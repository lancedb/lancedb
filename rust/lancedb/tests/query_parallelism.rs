// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::{
    Array, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, types::Float32Type,
};
use datafusion_physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use lance::io::exec::{ANNIvfBatchExec, ANNIvfSubIndexExec};
use lancedb::index::{Index, vector::IvfFlatIndexBuilder};
use lancedb::query::{ExecutableQuery, QueryBase, QueryExecutionOptions};
use lancedb::table::{AnyQuery, query::create_plan};

fn ann_parallelism(plan: &dyn ExecutionPlan) -> Option<i32> {
    if let Some(ann) = plan.downcast_ref::<ANNIvfSubIndexExec>() {
        return Some(ann.query().query_parallelism);
    }
    if let Some(ann) = plan.downcast_ref::<ANNIvfBatchExec>() {
        return Some(ann.query().query_parallelism);
    }
    plan.children()
        .into_iter()
        .find_map(|child| ann_parallelism(child.as_ref()))
}

#[tokio::test]
async fn query_parallelism_reaches_lance_and_preserves_results() -> lancedb::Result<()> {
    let conn = lancedb::connect("memory://parallelism").execute().await?;
    let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        (0..256).map(|i| Some(vec![Some(i as f32), Some((i % 7) as f32)])),
        2,
    );
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int32Array::from_iter_values(0..256)) as Arc<dyn Array>,
        ),
        ("vector", Arc::new(vectors) as Arc<dyn Array>),
    ])?;
    let table = conn.create_table("vectors", vec![batch]).execute().await?;
    table
        .create_index(
            &["vector"],
            Index::IvfFlat(IvfFlatIndexBuilder::default().num_partitions(4)),
        )
        .execute()
        .await?;
    let query = table
        .query()
        .nearest_to(&[3.25f32, 1.5])?
        .nprobes(4)?
        .limit(8);
    assert_eq!(query.current_request().query_parallelism, None);
    let mut baseline = None;
    for parallelism in [None, Some(0), Some(1), Some(2), Some(-1)] {
        let query = query.clone().query_parallelism(parallelism)?;
        let plan = query.create_plan(QueryExecutionOptions::default()).await?;
        assert_eq!(
            ann_parallelism(plan.as_ref()),
            Some(parallelism.unwrap_or(0))
        );
        let batches: Vec<RecordBatch> = query.execute().await?.try_collect().await?;
        let rows: Vec<(i32, f32)> = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let distances = batch
                    .column_by_name("_distance")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap();
                ids.values()
                    .iter()
                    .copied()
                    .zip(distances.values().iter().copied())
            })
            .collect();
        assert_eq!(rows.len(), 8);
        if let Some(expected) = &baseline {
            assert_eq!(&rows, expected);
        } else {
            baseline = Some(rows);
        }
    }
    assert_eq!(
        query
            .clone()
            .query_parallelism(4)?
            .query_parallelism(None)?
            .current_request()
            .query_parallelism,
        None
    );
    assert!(query.clone().query_parallelism(-2).is_err());
    // Public request fields must be validated too, even if the builder is bypassed.
    let mut request = query.into_request();
    request.query_parallelism = Some(-2);
    let error = create_plan(
        table.as_native().unwrap(),
        &AnyQuery::VectorQuery(request),
        QueryExecutionOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("query_parallelism must be >= -1")
    );
    Ok(())
}
