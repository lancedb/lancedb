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
            // The same table function can be maintained through the MV unit
            // lifecycle. Staging attempts do not make any rows visible.
            use lancedb::materialized_view::{
                MaterializedViewDefinition, commit_partitioned_refresh, plan_partitioned_refresh,
                prepare_definition, write_refresh_partition,
            };
            let view_name = format!("pairs_{}", threshold.to_bits());
            let view = prepare_definition(&table, MaterializedViewDefinition::from_sql(&sql)?)
                .await?
                .create(&view_name)
                .await?;
            assert_eq!(view.table().schema().await?.fields().len(), 3);
            let plan = plan_partitioned_refresh(view.table(), Some(version))
                .await?
                .unwrap();
            assert_eq!(plan.units() as usize, tasks.len());
            let plan = serde_json::from_slice(&serde_json::to_vec(&plan)?)?;
            let first = write_refresh_partition(view.table(), 0, &plan).await?;
            assert_eq!(view.table().count_rows(None).await?, 0);
            assert!(
                commit_partitioned_refresh(
                    view.table(),
                    &plan,
                    vec![first.clone(), first.clone()],
                    view.incarnation()
                )
                .await
                .is_err()
            );
            // Lost receipt: its files remain unreferenced; the retry writes
            // another attempt, of which only one receipt is ever committed.
            drop(write_refresh_partition(view.table(), 0, &plan).await?);
            let mut units = vec![first];
            for unit in 1..plan.units() {
                units.push(write_refresh_partition(view.table(), unit, &plan).await?);
            }
            assert_eq!(view.table().count_rows(None).await?, 0);
            let outcome =
                commit_partitioned_refresh(view.table(), &plan, units.clone(), view.incarnation())
                    .await?;
            assert_eq!(outcome.rows_written as usize, pairs(&expected).len());
            assert!(
                commit_partitioned_refresh(view.table(), &plan, units, view.incarnation())
                    .await
                    .is_err()
            );
            use lancedb::query::ExecutableQuery;
            let written = view
                .table()
                .query()
                .execute()
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            assert_eq!(pairs(&written), pairs(&expected));
            // Repeating a refresh replaces the view rather than appending.
            view.refresh().execute().await?;
            assert_eq!(
                view.table().count_rows(None).await? as usize,
                pairs(&expected).len()
            );
            // The complete declarative pipeline keeps direct representatives
            // and materializes original rows, including isolated source rows.
            use lancedb::materialized_view::write_refresh_partition_with_inputs;
            use lancedb::query::QueryBase;
            let definition = MaterializedViewDefinition::from_sql(&format!(
                "SELECT * FROM vector_dedup('source', {version}, 'vector', {threshold})"
            ))?;
            let clean = prepare_definition(&table, definition)
                .await?
                .create(&format!("clean_{}", threshold.to_bits()))
                .await?;
            let clean_plan = plan_partitioned_refresh(clean.table(), Some(version))
                .await?
                .unwrap();
            assert_eq!(clean_plan.dependency_units() as usize, tasks.len());
            assert_eq!(clean_plan.units() as usize, tasks.len() + 2);
            let clean_plan: lancedb::materialized_view::PartitionedRefreshPlan =
                serde_json::from_slice(&serde_json::to_vec(&clean_plan)?)?;
            assert!(
                write_refresh_partition(clean.table(), tasks.len() as u32, &clean_plan)
                    .await
                    .is_err()
            );
            let mut receipts = Vec::new();
            for unit in 0..clean_plan.units() {
                let dependencies = &receipts[..receipts.len().min(tasks.len())];
                if unit == 0 || unit == tasks.len() as u32 {
                    // Both selection and materialization can lose a receipt.
                    drop(
                        write_refresh_partition_with_inputs(
                            clean.table(),
                            unit,
                            &clean_plan,
                            dependencies,
                        )
                        .await?,
                    );
                }
                let receipt = write_refresh_partition_with_inputs(
                    clean.table(),
                    unit,
                    &clean_plan,
                    dependencies,
                )
                .await?;
                receipts.push(serde_json::from_slice(&serde_json::to_vec(&receipt)?)?);
            }
            assert_eq!(clean.table().count_rows(None).await?, 0);
            assert!(
                commit_partitioned_refresh(
                    clean.table(),
                    &clean_plan,
                    receipts[..receipts.len() - 1].to_vec(),
                    clean.incarnation()
                )
                .await
                .is_err()
            );
            commit_partitioned_refresh(clean.table(), &clean_plan, receipts, clean.incarnation())
                .await?;
            let mut edges = pairs(&expected)
                .into_iter()
                .map(|(a, b, _)| (a.min(b), a.max(b)))
                .collect::<Vec<_>>();
            edges.sort_unstable();
            let mut removed = std::collections::BTreeSet::new();
            for (a, b) in edges {
                if !removed.contains(&a) {
                    removed.insert(b);
                }
            }
            let source_ids = ds
                .scan()
                .project(&["id"])?
                .with_row_id()
                .try_into_stream()
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            let mut expected_ids = Vec::new();
            for batch in source_ids {
                let ids = batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let rowids = batch
                    .column_by_name("_rowid")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    if !removed.contains(&rowids.value(i)) {
                        expected_ids.push(ids.value(i));
                    }
                }
            }
            let kept = clean
                .table()
                .query()
                .select(lancedb::query::Select::columns(&["id"]))
                .execute()
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            let mut actual_ids = kept
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied()
                })
                .collect::<Vec<_>>();
            actual_ids.sort_unstable();
            expected_ids.sort_unstable();
            assert_eq!(actual_ids, expected_ids);
            if threshold == 1.0 {
                assert!(actual_ids.contains(&0) && actual_ids.contains(&2));
                assert!(!actual_ids.contains(&1), "A-B-C retains A and C");
            }
            clean.refresh().execute().await?;
            assert_eq!(clean.table().count_rows(None).await?, expected_ids.len());
            assert_eq!(table.count_rows(None).await?, n * 2);
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

#[tokio::test]
async fn dedup_materializes_blob_payloads_null_vectors_and_snapshot_deletions() -> anyhow::Result<()>
{
    use arrow_array::{LargeBinaryArray, StringArray, StructArray, types::Float32Type};
    use lancedb::materialized_view::{MaterializedViewDefinition, prepare_definition};
    for stable in [false, true] {
        let dir = tempfile::tempdir()?;
        let conn = lancedb::connect(dir.path().to_str().unwrap())
            .execute()
            .await?;
        let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            [
                Some(vec![Some(0.0), Some(0.0)]),
                Some(vec![Some(1.0), Some(0.0)]),
                Some(vec![Some(2.0), Some(0.0)]),
                None,
                Some(vec![Some(30.0), Some(0.0)]),
                Some(vec![Some(40.0), Some(0.0)]),
            ],
            2,
        );
        let blob = lancedb::blob::blob("image", true);
        let DataType::Struct(fields) = blob.data_type().clone() else {
            unreachable!()
        };
        let images = StructArray::new(
            fields,
            vec![
                Arc::new(LargeBinaryArray::from_iter_values(
                    (0..6).map(|id| vec![id; 4096]),
                )),
                Arc::new(StringArray::from(vec![None::<&str>; 6])),
            ],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("vector", vectors.data_type().clone(), true),
                blob,
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(0..6)),
                Arc::new(vectors),
                Arc::new(images),
            ],
        )?;
        let table = conn
            .create_table("source", batch)
            .storage_option("new_table_enable_stable_row_ids", stable.to_string())
            .execute()
            .await?;
        table.delete("id = 5").await?;
        table
            .optimize(lancedb::table::OptimizeAction::Compact {
                options: lancedb::table::CompactionOptions::default(),
                remap_options: None,
            })
            .await?;
        let uri = table.uri().await?;
        let mut ds = Dataset::open(&uri).await?;
        assert!(
            ds.fragments().iter().all(|f| f.id > 0),
            "compaction must change physical row addresses"
        );
        let centroids = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            [Some(vec![Some(0.0), Some(0.0)])],
            2,
        );
        let ivf = IvfBuildParams {
            centroids: Some(Arc::new(centroids)),
            ..IvfBuildParams::new(1)
        };
        ds.create_index(
            &["vector"],
            IndexType::Vector,
            Some("vector_idx".into()),
            &VectorIndexParams::with_ivf_flat_params(MetricType::L2, ivf),
            true,
        )
        .await?;
        let version = ds.version().version;
        let definition = MaterializedViewDefinition::from_sql(&format!(
            "SELECT * FROM vector_dedup('source', {version}, 'vector', 1)"
        ))?;
        let clean = prepare_definition(&table, definition)
            .await?
            .create("clean")
            .await?;
        // A later source delete does not change the declared result snapshot.
        table.delete("id = 0").await?;
        clean.refresh().execute().await?;
        assert_eq!(clean.table().count_rows(None).await?, 4);
        let output = Dataset::open(&clean.table().uri().await?).await?;
        let batches = output
            .scan()
            .blob_handling(lance_core::datatypes::BlobHandling::AllBinary)
            .try_into_stream()
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let mut ids = Vec::new();
        for batch in batches {
            let row_ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let images = batch
                .column_by_name("image")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                let id = row_ids.value(i);
                assert_eq!(images.value(i), vec![id as u8; 4096]);
                ids.push(id);
            }
        }
        ids.sort_unstable();
        assert_eq!(ids, [0, 2, 3, 4]);
        assert_eq!(
            table.count_rows(None).await?,
            4,
            "refresh must not mutate the source"
        );
    }
    Ok(())
}
