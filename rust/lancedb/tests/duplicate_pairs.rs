// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_array::{
    Array, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, RecordBatchIterator,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use datafusion_catalog::TableProvider;
use datafusion_common::Result;
use futures::TryStreamExt;
use lance::{
    Dataset,
    dataset::WriteParams,
    index::{DatasetIndexExt, vector::VectorIndexParams},
};
use lance_index::{IndexType, vector::ivf::IvfBuildParams};
use lance_linalg::distance::MetricType;
use lancedb::table::datafusion::{
    BaseTableAdapter,
    udtf::duplicate_pairs::{
        DuplicatePairsConfig, DuplicatePairsExec, DuplicatePairsResolver,
        DuplicatePairsTableFunction, duplicate_pair_task_batch, plan_duplicate_pairs,
    },
};
use std::sync::Arc;

#[derive(Debug)]
struct Resolver(Arc<BaseTableAdapter>);
impl DuplicatePairsResolver for Resolver {
    fn resolve(
        &self,
        table: &str,
        config: DuplicatePairsConfig,
        tasks_only: bool,
    ) -> Result<Arc<dyn TableProvider>> {
        assert_eq!(table, "source");
        Ok(Arc::new(self.0.duplicate_pairs(config, tasks_only)))
    }
}

fn pairs(batches: &[RecordBatch]) -> Vec<(u64, u64, u32)> {
    let mut result = Vec::new();
    for batch in batches {
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let d = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            result.push((a.value(i), b.value(i), d.value(i).to_bits()));
        }
    }
    result.sort_unstable();
    result
}

#[tokio::test]
async fn sql_pairs_match_native_snapshot_and_partition_tasks() -> anyhow::Result<()> {
    for partitions in [1, 8] {
        let dir = tempfile::tempdir()?;
        let uri = dir
            .path()
            .join("source.lance")
            .to_str()
            .unwrap()
            .to_string();
        let n = 1025;
        let vectors = (0..n * 2)
            .flat_map(|i| {
                let i = i % n;
                let x = if i == n - 1 {
                    0.0
                } else if i < 3 {
                    i as f32
                } else {
                    i as f32 * 10.0
                };
                [x, 0.0]
            })
            .collect::<Vec<_>>();
        let vectors = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            2,
            Arc::new(Float32Array::from(vectors)),
            None,
        )?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("vector", vectors.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..(n * 2) as i64)),
                Arc::new(vectors),
            ],
        )?;
        let mut ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            &uri,
            Some(WriteParams {
                max_rows_per_file: n,
                ..Default::default()
            }),
        )
        .await?;
        let fragments = ds
            .fragments()
            .iter()
            .map(|f| f.id as u32)
            .collect::<Vec<_>>();
        assert_eq!(fragments.len(), 2);
        let mut segments = Vec::new();
        for (i, fragment) in fragments.into_iter().enumerate() {
            let centroids = FixedSizeListArray::try_new(
                Arc::new(Field::new("item", DataType::Float32, true)),
                2,
                Arc::new(Float32Array::from(
                    (0..partitions)
                        .flat_map(|p| [p as f32 * 1400.0 + i as f32 * 0.125, 0.0])
                        .collect::<Vec<_>>(),
                )),
                None,
            )?;
            let ivf = IvfBuildParams {
                centroids: Some(Arc::new(centroids)),
                ..IvfBuildParams::new(partitions)
            };
            let params = VectorIndexParams::with_ivf_flat_params(MetricType::L2, ivf);
            segments.push(
                ds.create_index_builder(&["vector"], IndexType::Vector, &params)
                    .name("vector_idx".into())
                    .fragments(vec![fragment])
                    .execute_uncommitted()
                    .await?,
            );
        }
        ds.commit_existing_index_segments("vector_idx", "vector", segments)
            .await?;
        let version = ds.version().version;
        let ds = Arc::new(ds);
        let conn = lancedb::connect(dir.path().to_str().unwrap())
            .execute()
            .await?;
        let table = conn.open_table("source").execute().await?;
        let resolver = Arc::new(Resolver(Arc::new(
            BaseTableAdapter::try_new(table.base_table().clone()).await?,
        )));
        // More partitions than this pool could hold concurrently: local
        // execution must consume scoped readers sequentially.
        let runtime = Arc::new(
            datafusion_execution::runtime_env::RuntimeEnvBuilder::new()
                .with_memory_limit(128 * 1024 * 1024, 1.0)
                .build()?,
        );
        let ctx =
            SessionContext::new_with_config_rt(datafusion::prelude::SessionConfig::new(), runtime);
        ctx.register_udtf(
            "vector_duplicate_pairs",
            Arc::new(DuplicatePairsTableFunction::new(resolver.clone())),
        );
        ctx.register_udtf(
            "vector_duplicate_pair_tasks",
            Arc::new(DuplicatePairsTableFunction::task_manifest(resolver)),
        );
        for threshold in [-1.0, 0.0, 1.0] {
            let config = DuplicatePairsConfig {
                dataset_version: version,
                column: "vector".into(),
                distance_threshold: threshold,
            };
            let tasks = plan_duplicate_pairs(ds.clone(), &config).await?;
            assert_eq!(tasks.len(), partitions * 2);
            let manifest = duplicate_pair_task_batch(&ds, &config, &tasks)?;
            let sql_manifest = ctx.sql(&format!("SELECT * FROM vector_duplicate_pair_tasks('source', {version}, 'vector', {threshold})")).await?.collect().await?;
            assert_eq!(sql_manifest, vec![manifest]);
            let expected =
                lance::index::vector::dedup::find_duplicate_pairs(ds.clone(), "vector", threshold)
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?;
            let sql = format!(
                "SELECT * FROM vector_duplicate_pairs('source', {version}, 'vector', {threshold})"
            );
            let actual = ctx.sql(&sql).await?.collect().await?;
            assert_eq!(pairs(&actual), pairs(&expected));
            assert_eq!(
                pairs(&actual).len(),
                match threshold {
                    -1.0 => 0,
                    0.0 => 2,
                    _ => 8,
                }
            );
            let retry = ctx.sql(&sql).await?.collect().await?;
            assert_eq!(pairs(&retry), pairs(&actual));
            assert!(
                DuplicatePairsExec::try_new(
                    ds.clone(),
                    config.clone(),
                    vec![tasks[0].clone(), tasks[0].clone()]
                )
                .is_err()
            );
        }
        // Latest no longer contains B, but pinned queries must retain the same pairs.
        table.delete("id = 1").await?;
        let actual = ctx
            .sql(&format!(
                "SELECT * FROM vector_duplicate_pairs('source', {version}, 'vector', 1.0)"
            ))
            .await?
            .collect()
            .await?;
        assert_eq!(pairs(&actual).len(), 8);
        assert!(
            ctx.sql(&format!(
                "SELECT * FROM vector_duplicate_pairs('source', {version}, 'missing', 1.0)"
            ))
            .await?
            .collect()
            .await
            .is_err()
        );
    }
    Ok(())
}
