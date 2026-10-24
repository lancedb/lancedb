// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{Array, RecordBatch};
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::disk_manager::{DiskManager, RefCountedTempFile};
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use futures::TryStreamExt;
use redb::{Database, Durability, ReadableDatabase, ReadableTable, Table, TableDefinition};
use roaring::RoaringTreemap;

const PARENTS: TableDefinition<u64, u64> = TableDefinition::new("__dedup_parents");
const CACHE_BYTES: usize = 64 * 1024 * 1024;
const TRANSACTION_EDGES: usize = 1024;

fn external(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

struct UnionFind {
    database: Database,
    file: RefCountedTempFile,
    disk_manager: Arc<DiskManager>,
}

impl UnionFind {
    fn new(file: RefCountedTempFile, disk_manager: Arc<DiskManager>) -> Result<Self> {
        let database = Database::builder()
            .set_cache_size(CACHE_BYTES)
            .create(file.path())
            .map_err(external)?;
        let mut transaction = database.begin_write().map_err(external)?;
        transaction
            .set_durability(Durability::None)
            .map_err(external)?;
        transaction.open_table(PARENTS).map_err(external)?;
        transaction.commit().map_err(external)?;
        let mut state = Self {
            database,
            file,
            disk_manager,
        };
        state.record_disk_usage()?;
        Ok(state)
    }

    fn record_disk_usage(&mut self) -> Result<()> {
        let bytes = std::fs::metadata(self.file.path())?.len();
        let growth = bytes.saturating_sub(self.file.current_disk_usage());
        let available = self
            .disk_manager
            .max_temp_directory_size()
            .saturating_sub(self.disk_manager.used_disk_space());
        if growth > available {
            return Err(DataFusionError::ResourcesExhausted(
                "union_find parent state exceeds the temporary disk budget".into(),
            ));
        }
        self.file.update_disk_usage()
    }

    fn root(parents: &mut Table<'_, u64, u64>, mut node: u64) -> Result<u64> {
        loop {
            let parent = parents
                .get(node)
                .map_err(external)?
                .map(|value| value.value());
            let Some(parent) = parent else {
                return Ok(node);
            };
            let grandparent = parents
                .get(parent)
                .map_err(external)?
                .map(|value| value.value())
                .unwrap_or(parent);
            if grandparent != parent {
                parents.insert(node, grandparent).map_err(external)?;
            }
            node = grandparent;
        }
    }

    fn consume(&mut self, batch: &RecordBatch) -> Result<()> {
        let a = batch
            .column_by_name("row_id_a")
            .and_then(|array| array.as_primitive_opt::<UInt64Type>())
            .ok_or_else(|| {
                DataFusionError::Execution("union_find requires UInt64 row_id_a".into())
            })?;
        let b = batch
            .column_by_name("row_id_b")
            .and_then(|array| array.as_primitive_opt::<UInt64Type>())
            .ok_or_else(|| {
                DataFusionError::Execution("union_find requires UInt64 row_id_b".into())
            })?;
        for start in (0..batch.num_rows()).step_by(TRANSACTION_EDGES) {
            let mut transaction = self.database.begin_write().map_err(external)?;
            transaction
                .set_durability(Durability::None)
                .map_err(external)?;
            {
                let mut parents = transaction.open_table(PARENTS).map_err(external)?;
                for row in start..(start + TRANSACTION_EDGES).min(batch.num_rows()) {
                    if a.is_null(row) || b.is_null(row) {
                        return Err(DataFusionError::Execution(
                            "union_find requires non-null row IDs".into(),
                        ));
                    }
                    let left = Self::root(&mut parents, a.value(row))?;
                    let right = Self::root(&mut parents, b.value(row))?;
                    if left != right {
                        parents
                            .insert(left.max(right), left.min(right))
                            .map_err(external)?;
                    }
                }
            }
            transaction.commit().map_err(external)?;
            self.record_disk_usage()?;
        }
        Ok(())
    }

    fn removed(self) -> Result<RoaringTreemap> {
        let transaction = self.database.begin_read().map_err(external)?;
        let parents = transaction.open_table(PARENTS).map_err(external)?;
        let mut removed = RoaringTreemap::new();
        for entry in parents.iter().map_err(external)? {
            let (vertex, _) = entry.map_err(external)?;
            removed.insert(vertex.value());
        }
        Ok(removed)
    }
}

pub(super) async fn select_removed(
    mut pairs: SendableRecordBatchStream,
    context: Arc<TaskContext>,
) -> Result<RoaringTreemap> {
    let first = loop {
        match pairs.try_next().await? {
            Some(batch) if batch.num_rows() > 0 => break batch,
            Some(_) => continue,
            None => return Ok(RoaringTreemap::new()),
        }
    };
    let reservation = MemoryConsumer::new("DedupUnionFind").register(context.memory_pool());
    reservation.try_grow(CACHE_BYTES)?;
    let file = context
        .runtime_env()
        .disk_manager
        .create_tmp_file("dedup union-find parents")?;
    let disk_manager = context.runtime_env().disk_manager.clone();
    let mut state = tokio::task::spawn_blocking(move || UnionFind::new(file, disk_manager))
        .await
        .map_err(external)??;
    let mut next = Some(first);
    while let Some(batch) = next {
        state = tokio::task::spawn_blocking(move || {
            state.consume(&batch)?;
            Ok::<_, DataFusionError>(state)
        })
        .await
        .map_err(external)??;
        next = pairs.try_next().await?;
    }
    tokio::task::spawn_blocking(move || state.removed())
        .await
        .map_err(external)?
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::UInt64Array;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::SessionContext;
    use futures::stream;

    #[tokio::test]
    async fn chains_and_bridges_keep_minimum_row_id_in_any_edge_order() {
        for edges in [
            vec![(9, 8), (2, 1), (8, 2), (20, 21), (21, 20), (9, 9)],
            vec![(9, 9), (21, 20), (20, 21), (8, 2), (2, 1), (9, 8)],
        ] {
            let batch = RecordBatch::try_from_iter([
                (
                    "row_id_a",
                    Arc::new(UInt64Array::from_iter_values(
                        edges.iter().map(|edge| edge.0),
                    )) as arrow_array::ArrayRef,
                ),
                (
                    "row_id_b",
                    Arc::new(UInt64Array::from_iter_values(
                        edges.iter().map(|edge| edge.1),
                    )) as arrow_array::ArrayRef,
                ),
            ])
            .unwrap();
            let input = Box::pin(RecordBatchStreamAdapter::new(
                batch.schema(),
                stream::iter(vec![Ok(batch)]),
            ));
            let context = SessionContext::new().task_ctx();
            let manager = context.runtime_env().disk_manager.clone();
            let removed = select_removed(input, context).await.unwrap();
            assert_eq!(removed.iter().collect::<Vec<_>>(), vec![2, 8, 9, 21]);
            assert_eq!(manager.used_disk_space(), 0);
        }
    }

    #[test]
    fn disk_budget_failure_releases_temporary_state() {
        let manager = Arc::new(
            datafusion_execution::disk_manager::DiskManagerBuilder::default()
                .with_max_temp_directory_size(1)
                .build()
                .unwrap(),
        );
        let file = manager.create_tmp_file("union-find budget test").unwrap();
        let error = UnionFind::new(file, manager.clone()).err().unwrap();
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
        assert_eq!(manager.used_disk_space(), 0);
    }

    #[tokio::test]
    async fn sparse_extreme_ids_and_stream_errors() {
        let batch = RecordBatch::try_from_iter([
            (
                "row_id_a",
                Arc::new(UInt64Array::from(vec![u64::MAX, 17, 0])) as arrow_array::ArrayRef,
            ),
            (
                "row_id_b",
                Arc::new(UInt64Array::from(vec![17, 0, 0])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let input = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            stream::iter(vec![Ok(batch.clone())]),
        ));
        let context = SessionContext::new().task_ctx();
        let manager = context.runtime_env().disk_manager.clone();
        assert_eq!(
            select_removed(input, context.clone())
                .await
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![17, u64::MAX]
        );
        let input = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            stream::iter(vec![
                Ok(batch),
                Err(DataFusionError::Execution("pair stream failed".into())),
            ]),
        ));
        assert!(
            select_removed(input, context)
                .await
                .unwrap_err()
                .to_string()
                .contains("pair stream failed")
        );
        assert_eq!(manager.used_disk_space(), 0);
    }
}
