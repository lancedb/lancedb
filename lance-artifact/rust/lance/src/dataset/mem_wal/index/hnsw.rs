// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! In-memory HNSW index for vector similarity search.
//!
//! This is the MemWAL adapter around the local HNSW graph. Vectors are
//! retained by reference from the writer's Arrow batches, inserts are
//! published under a multi-reader / single-writer contract, and flush
//! snapshots are emitted in Lance's on-disk HNSW + FLAT storage format.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::types::Float32Type;
use arrow_array::{Array, FixedSizeListArray, RecordBatch};
use lance_core::{Error, Result};
use lance_index::vector::hnsw::{HNSW, builder::HnswBuildParams};
use lance_index::vector::v3::subindex::IvfSubIndex;
use lance_linalg::distance::DistanceType;

use super::super::hnsw::{ArrowFixedSizeListVectorStore, BuildParams, HnswGraph, SearchParams};
use super::super::memtable::batch_store::StoredBatch;

pub use super::RowPosition;

const MEM_HNSW_DIM_PLACEHOLDER: usize = 0;

/// Write-optimized default HNSW build parameters for MemTable indexes.
///
/// MemTable HNSW graphs are rebuilt on every flush, so build speed matters more
/// than for a static base-table index. A dbpedia-1M (1536-d) sweep showed
/// `num_edges = 16, ef_construction = 100` is the best fast-write/good-recall
/// point: ~27% faster to flush than the generic `HnswBuildParams::default()`
/// (`num_edges = 20, ef_construction = 150`) with an equal-or-better recall
/// ceiling (~0.95 at ef=256, the SQ8-quantization limit). Lower `num_edges`
/// (e.g. 12) flushes faster still but drops the recall ceiling below 0.95.
pub fn mem_wal_hnsw_default() -> HnswBuildParams {
    HnswBuildParams::default()
        .num_edges(16)
        .ef_construction(100)
}

/// Configuration for an in-memory HNSW index.
#[derive(Debug, Clone)]
pub struct HnswIndexConfig {
    pub name: String,
    pub field_id: i32,
    /// Vector column name for batch lookups.
    pub column: String,
    pub distance_type: DistanceType,
    pub build_params: HnswBuildParams,
}

impl HnswIndexConfig {
    pub fn new(name: String, field_id: i32, column: String, distance_type: DistanceType) -> Self {
        Self {
            name,
            field_id,
            column,
            distance_type,
            build_params: mem_wal_hnsw_default(),
        }
    }

    pub fn with_build_params(mut self, params: HnswBuildParams) -> Self {
        self.build_params = params;
        self
    }
}

/// In-memory HNSW index queryable while building.
pub struct HnswMemIndex {
    field_id: i32,
    column: String,
    distance_type: DistanceType,
    /// Vector dimension (lazy-initialized on first insert).
    dim: AtomicUsize,
    /// Capacity (max vectors) for both the HNSW graph and vector store.
    capacity: usize,
    /// Maximum number of Arrow batches retained by reference.
    max_batches: usize,
    build_params: HnswBuildParams,
    state: OnceLock<HnswState>,
}

struct HnswState {
    storage: Arc<ArrowFixedSizeListVectorStore>,
    graph: HnswGraph,
}

impl std::fmt::Debug for HnswMemIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnswMemIndex")
            .field("field_id", &self.field_id)
            .field("column", &self.column)
            .field("distance_type", &self.distance_type)
            .field("dim", &self.dim.load(Ordering::Acquire))
            .field("capacity", &self.capacity)
            .field("len", &self.len())
            .finish()
    }
}

impl HnswMemIndex {
    pub fn with_capacity(
        field_id: i32,
        column: String,
        distance_type: DistanceType,
        build_params: HnswBuildParams,
        capacity: usize,
        max_batches: usize,
    ) -> Self {
        Self {
            field_id,
            column,
            distance_type,
            dim: AtomicUsize::new(MEM_HNSW_DIM_PLACEHOLDER),
            capacity,
            max_batches,
            build_params,
            state: OnceLock::new(),
        }
    }

    pub fn field_id(&self) -> i32 {
        self.field_id
    }

    pub fn column_name(&self) -> &str {
        &self.column
    }

    pub fn distance_type(&self) -> DistanceType {
        self.distance_type
    }

    pub fn build_params(&self) -> &HnswBuildParams {
        &self.build_params
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Vector dimension. Returns 0 before the first insert.
    pub fn dim(&self) -> usize {
        self.dim.load(Ordering::Acquire)
    }

    pub fn len(&self) -> usize {
        self.state.get().map(|s| s.graph.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn ensure_state(&self, dim: usize) -> Result<&HnswState> {
        if let Some(state) = self.state.get() {
            if state.storage.dim() != dim {
                return Err(Error::invalid_input(format!(
                    "HNSW index column '{}' dimension changed: expected {}, got {}",
                    self.column,
                    state.storage.dim(),
                    dim
                )));
            }
            return Ok(state);
        }

        let state = HnswState {
            storage: Arc::new(ArrowFixedSizeListVectorStore::try_new(
                self.capacity,
                self.max_batches,
                dim,
                self.distance_type,
            )?),
            graph: HnswGraph::try_new(self.capacity, to_lance_hnsw_params(&self.build_params)?)?,
        };
        self.dim.store(dim, Ordering::Release);

        if self.state.set(state).is_err() {
            // Another writer initialized first. The shard writer has a
            // single-writer contract, but handle the race defensively.
            let Some(state) = self.state.get() else {
                return Err(Error::internal(
                    "HNSW state initialization raced but no state was installed",
                ));
            };
            if state.storage.dim() != dim {
                return Err(Error::invalid_input(format!(
                    "HNSW index column '{}' dimension changed: expected {}, got {}",
                    self.column,
                    state.storage.dim(),
                    dim
                )));
            }
            return Ok(state);
        }

        self.state
            .get()
            .ok_or_else(|| Error::internal("HNSW state was not installed after initialization"))
    }

    /// Insert vectors from a single batch.
    ///
    /// The vector column is appended by reference; the Arrow values buffer is
    /// not copied. The graph then indexes the dense id range assigned to this
    /// batch.
    pub fn insert(&self, batch: &RecordBatch, row_offset: u64) -> Result<()> {
        let (col_idx, _) = batch
            .schema()
            .column_with_name(&self.column)
            .ok_or_else(|| {
                Error::invalid_input(format!(
                    "HNSW index column '{}' is not in the inserted batch schema",
                    self.column
                ))
            })?;
        let column = batch.column(col_idx);
        let fsl_ref = column.as_fixed_size_list_opt().ok_or_else(|| {
            Error::invalid_input(format!(
                "Column '{}' is not a FixedSizeList, got {:?}",
                self.column,
                column.data_type()
            ))
        })?;
        if fsl_ref.is_empty() {
            return Ok(());
        }
        if fsl_ref.values().as_primitive_opt::<Float32Type>().is_none() {
            return Err(Error::invalid_input(format!(
                "Column '{}' must be FixedSizeList<Float32>, got values type {:?}",
                self.column,
                fsl_ref.values().data_type()
            )));
        }
        // Null-vector rows (e.g. tombstones) are skipped inside `append_batch`,
        // which yields an empty range for an all-null batch — handled below.
        let dim = fsl_ref.value_length() as usize;
        let state = self.ensure_state(dim)?;
        let vectors = Arc::new(fsl_ref.clone());
        let id_range = state.storage.append_batch(vectors, row_offset)?;
        if id_range.is_empty() {
            return Ok(());
        }
        let snapshot = state.storage.snapshot();
        state.graph.insert_batch(id_range, &snapshot)
    }

    /// Insert vectors from multiple batches.
    pub fn insert_batches(&self, batches: &[StoredBatch]) -> Result<()> {
        let mut combined_range: Option<std::ops::Range<u32>> = None;
        let mut state: Option<&HnswState> = None;

        for stored in batches {
            let (col_idx, _) = stored
                .data
                .schema()
                .column_with_name(&self.column)
                .ok_or_else(|| {
                    Error::invalid_input(format!(
                        "HNSW index column '{}' is not in the inserted batch schema",
                        self.column
                    ))
                })?;
            let column = stored.data.column(col_idx);
            let fsl_ref = column.as_fixed_size_list_opt().ok_or_else(|| {
                Error::invalid_input(format!(
                    "Column '{}' is not a FixedSizeList, got {:?}",
                    self.column,
                    column.data_type()
                ))
            })?;
            if fsl_ref.is_empty() {
                continue;
            }
            if fsl_ref.values().as_primitive_opt::<Float32Type>().is_none() {
                return Err(Error::invalid_input(format!(
                    "Column '{}' must be FixedSizeList<Float32>, got values type {:?}",
                    self.column,
                    fsl_ref.values().data_type()
                )));
            }
            // Null-vector rows (e.g. tombstones) are skipped inside
            // `append_batch`; an all-null batch yields an empty range, handled
            // by the `id_range.is_empty()` continue below.
            let dim = fsl_ref.value_length() as usize;
            let current_state = match state {
                Some(state) => {
                    if state.storage.dim() != dim {
                        return Err(Error::invalid_input(format!(
                            "HNSW index column '{}' dimension changed: expected {}, got {}",
                            self.column,
                            state.storage.dim(),
                            dim
                        )));
                    }
                    state
                }
                None => self.ensure_state(dim)?,
            };
            state = Some(current_state);

            let vectors = Arc::new(fsl_ref.clone());
            let id_range = current_state
                .storage
                .append_batch(vectors, stored.row_offset)?;
            if id_range.is_empty() {
                continue;
            }

            match &mut combined_range {
                Some(range) if range.end == id_range.start => {
                    range.end = id_range.end;
                }
                Some(range) => {
                    return Err(Error::internal(format!(
                        "non-contiguous HNSW vector id range while inserting batches: existing={:?}, next={:?}",
                        range, id_range
                    )));
                }
                None => {
                    combined_range = Some(id_range);
                }
            }
        }

        if let (Some(state), Some(id_range)) = (state, combined_range) {
            let snapshot = state.storage.snapshot();
            state.graph.insert_batch(id_range, &snapshot)?;
        }
        Ok(())
    }

    /// Search for nearest neighbors of `query` with MVCC visibility.
    ///
    /// Distances are exact because the in-memory graph is backed by FLAT
    /// vectors. Rows with positions greater than `max_row_position` are
    /// filtered after graph search.
    pub fn search(
        &self,
        query: &FixedSizeListArray,
        k: usize,
        ef: Option<usize>,
        max_row_position: RowPosition,
    ) -> Result<Vec<(f32, RowPosition)>> {
        if k == 0 {
            return Ok(Vec::new());
        }
        if query.len() != 1 {
            return Err(Error::invalid_input(format!(
                "Query must have exactly 1 vector, got {}",
                query.len()
            )));
        }
        if query.null_count() > 0 {
            return Err(Error::invalid_input("HNSW query vector must not be null"));
        }
        let Some(state) = self.state.get() else {
            return Ok(Vec::new());
        };
        if query.value_length() as usize != state.storage.dim() {
            return Err(Error::invalid_input(format!(
                "HNSW query dimension mismatch: expected {}, got {}",
                state.storage.dim(),
                query.value_length()
            )));
        }
        let query_values = query.value(0);
        let Some(query_values) = query_values.as_primitive_opt::<Float32Type>() else {
            return Err(Error::invalid_input(format!(
                "HNSW query must contain Float32 values, got {:?}",
                query_values.data_type()
            )));
        };

        let ef_actual = ef.unwrap_or(k.max(64)).max(k);
        let snapshot = state.storage.snapshot();
        let mut out: Vec<_> = state
            .graph
            .search(
                query_values.values(),
                SearchParams::new(ef_actual, ef_actual),
                &snapshot,
            )?
            .into_iter()
            .filter_map(|result| {
                if result.row_id <= max_row_position && result.distance.is_finite() {
                    Some((result.distance, result.row_id))
                } else {
                    None
                }
            })
            .collect();
        out.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        out.truncate(k);
        Ok(out)
    }

    /// Snapshot the in-memory HNSW into the Lance on-disk representation:
    /// returns the graph + the FLAT vector storage record batch.
    pub fn to_lance_hnsw(&self, total_rows: Option<u64>) -> Result<Option<(HNSW, RecordBatch)>> {
        let Some(state) = self.state.get() else {
            return Ok(None);
        };
        if state.graph.is_empty() {
            return Ok(None);
        }
        let storage_batch = state.storage.to_record_batch(total_rows)?;
        let hnsw_batch = state.graph.to_lance_hnsw_batch()?;
        let hnsw = HNSW::load(hnsw_batch)?;
        Ok(Some((hnsw, storage_batch)))
    }
}

fn to_lance_hnsw_params(params: &HnswBuildParams) -> Result<BuildParams> {
    let params = BuildParams {
        max_level: params.max_level,
        m: params.m,
        ef_construction: params.ef_construction,
        prefetch_distance: params.prefetch_distance,
        ..BuildParams::default()
    };
    // Validate by constructing a tiny graph with these params. This keeps
    // invalid builder options as boundary errors instead of delayed panics.
    HnswGraph::try_new(1, params.clone())?;
    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Float32Array, Int32Array};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use lance_arrow::FixedSizeListArrayExt;

    fn make_batch(start_id: i32, n: usize, dim: usize) -> RecordBatch {
        let ids: Vec<i32> = (start_id..start_id + n as i32).collect();
        let mut flat: Vec<f32> = Vec::with_capacity(n * dim);
        for &id in &ids {
            for d in 0..dim {
                flat.push((id as f32 * 0.01) + (d as f32 * 0.001));
            }
        }
        let inner = Float32Array::from(flat);
        let fsl = FixedSizeListArray::try_new_from_values(inner, dim as i32).unwrap();
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    dim as i32,
                ),
                false,
            ),
        ]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids)), Arc::new(fsl)]).unwrap()
    }

    #[test]
    fn test_index_insert_and_search() {
        let dim = 8;
        let n = 200;
        let index = HnswMemIndex::with_capacity(
            1,
            "vector".to_string(),
            DistanceType::L2,
            HnswBuildParams::default().num_edges(16).ef_construction(64),
            n,
            64,
        );

        let batch = make_batch(0, n, dim);
        index.insert(&batch, 0).unwrap();
        assert_eq!(index.len(), n);

        let fsl = batch.column_by_name("vector").unwrap().as_fixed_size_list();
        let query_inner =
            Float32Array::from(fsl.value(5).as_primitive::<Float32Type>().values().to_vec());
        let query = FixedSizeListArray::try_new_from_values(query_inner, dim as i32).unwrap();

        let results = index.search(&query, 5, Some(32), u64::MAX).unwrap();
        assert!(!results.is_empty());
        let (best_dist, best_pos) = results[0];
        assert!(
            best_dist < 1e-4,
            "expected near-zero distance, got {}",
            best_dist
        );
        assert_eq!(best_pos, 5);
    }

    #[test]
    fn test_index_insert_batches_combines_hnsw_insert_range() {
        let dim = 8;
        let n = 200;
        let index = HnswMemIndex::with_capacity(
            1,
            "vector".to_string(),
            DistanceType::L2,
            HnswBuildParams::default().num_edges(16).ef_construction(64),
            n,
            64,
        );

        let first = make_batch(0, 75, dim);
        let second = make_batch(75, 125, dim);
        let stored = vec![
            StoredBatch::new(first, 0, 0),
            StoredBatch::new(second.clone(), 75, 1),
        ];
        index.insert_batches(&stored).unwrap();
        assert_eq!(index.len(), n);

        let fsl = second
            .column_by_name("vector")
            .unwrap()
            .as_fixed_size_list();
        let query_inner =
            Float32Array::from(fsl.value(7).as_primitive::<Float32Type>().values().to_vec());
        let query = FixedSizeListArray::try_new_from_values(query_inner, dim as i32).unwrap();

        let results = index.search(&query, 5, Some(32), u64::MAX).unwrap();
        assert!(!results.is_empty());
        assert!(
            results.iter().any(|&(dist, pos)| pos == 82 && dist < 1e-4),
            "expected exact row position 82 in top-5 candidates, got {:?}",
            results
        );
    }

    #[test]
    fn test_index_visibility_filter() {
        let dim = 8;
        let n = 50;
        let index = HnswMemIndex::with_capacity(
            1,
            "vector".to_string(),
            DistanceType::L2,
            HnswBuildParams::default().num_edges(16).ef_construction(64),
            n,
            64,
        );
        let batch = make_batch(0, n, dim);
        index.insert(&batch, 0).unwrap();

        let fsl = batch.column_by_name("vector").unwrap().as_fixed_size_list();
        let query_inner = Float32Array::from(
            fsl.value(40)
                .as_primitive::<Float32Type>()
                .values()
                .to_vec(),
        );
        let query = FixedSizeListArray::try_new_from_values(query_inner, dim as i32).unwrap();

        let results = index.search(&query, 5, Some(32), 10).unwrap();
        for (_, pos) in &results {
            assert!(*pos <= 10);
        }
    }

    #[test]
    fn test_index_empty_search() {
        let index = HnswMemIndex::with_capacity(
            1,
            "vector".to_string(),
            DistanceType::L2,
            HnswBuildParams::default(),
            16,
            16,
        );
        let inner = Float32Array::from(vec![0.0; 4]);
        let query = FixedSizeListArray::try_new_from_values(inner, 4).unwrap();
        let results = index.search(&query, 5, None, u64::MAX).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_to_lance_hnsw_reverses_row_ids() {
        let dim = 8;
        let n = 32;
        let index = HnswMemIndex::with_capacity(
            1,
            "vector".to_string(),
            DistanceType::L2,
            HnswBuildParams::default().num_edges(8).ef_construction(32),
            n,
            4,
        );
        let batch = make_batch(0, n, dim);
        index.insert(&batch, 10).unwrap();

        let Some((hnsw, storage_batch)) = index.to_lance_hnsw(Some(100)).unwrap() else {
            panic!("expected HNSW snapshot");
        };
        assert_eq!(hnsw.len(), n);
        let row_ids = storage_batch
            .column_by_name(lance_core::ROW_ID)
            .unwrap()
            .as_primitive::<arrow_array::types::UInt64Type>();
        assert_eq!(row_ids.value(0), 89);
        assert_eq!(row_ids.value(n - 1), 58);
    }

    #[test]
    fn test_index_concurrent_insert_and_search() {
        use std::sync::Arc as StdArc;
        use std::sync::atomic::{AtomicBool, Ordering as StdOrdering};
        use std::thread;

        let dim = 16;
        let n = 500;
        let index = StdArc::new(HnswMemIndex::with_capacity(
            1,
            "vector".to_string(),
            DistanceType::L2,
            HnswBuildParams::default().num_edges(8).ef_construction(32),
            n,
            256,
        ));

        let initial = make_batch(-1, 1, dim);
        index.insert(&initial, 0).unwrap();

        let stop = StdArc::new(AtomicBool::new(false));
        let mut reader_handles = Vec::new();
        for _ in 0..4 {
            let index = index.clone();
            let stop = stop.clone();
            reader_handles.push(thread::spawn(move || {
                let inner = Float32Array::from(vec![0.5_f32; dim]);
                let query = FixedSizeListArray::try_new_from_values(inner, dim as i32).unwrap();
                let mut iters = 0u64;
                while !stop.load(StdOrdering::Relaxed) {
                    let _ = index.search(&query, 5, Some(32), u64::MAX).unwrap();
                    iters += 1;
                }
                iters
            }));
        }

        let writer_index = index.clone();
        let writer_handle = thread::spawn(move || {
            for i in 1..(n / 5) {
                let batch = make_batch(i as i32 * 5, 5, dim);
                let row_offset = (i as u64) * 5 + 1;
                writer_index.insert(&batch, row_offset).unwrap();
            }
        });

        writer_handle.join().unwrap();
        stop.store(true, StdOrdering::Release);
        let mut total_reader_iters = 0u64;
        for h in reader_handles {
            total_reader_iters += h.join().unwrap();
        }

        assert!(index.len() > 1);
        assert!(total_reader_iters > 0);
    }

    /// Build a 3-row batch whose middle vector row is null at the list level.
    fn batch_with_null_middle(dim: usize) -> RecordBatch {
        let mut values: Vec<f32> = Vec::new();
        values.extend(std::iter::repeat_n(1.0f32, dim)); // row 0
        values.extend(std::iter::repeat_n(0.0f32, dim)); // row 1 (null placeholder)
        values.extend(std::iter::repeat_n(3.0f32, dim)); // row 2
        let inner = Arc::new(Float32Array::from(values)) as arrow_array::ArrayRef;
        let nulls = arrow_buffer::NullBuffer::new(arrow_buffer::BooleanBuffer::from(vec![
            true, false, true,
        ]));
        let fsl = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            dim as i32,
            inner,
            Some(nulls),
        )
        .unwrap();
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    dim as i32,
                ),
                true,
            ),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![0, 1, 2])), Arc::new(fsl)],
        )
        .unwrap()
    }

    #[test]
    fn test_index_skips_null_vector_row() {
        // A null vector row gets no graph node; the surviving rows keep their
        // original positions (the tombstone-enabling fix).
        let dim = 4;
        let index = HnswMemIndex::with_capacity(
            1,
            "vector".to_string(),
            DistanceType::L2,
            HnswBuildParams::default().num_edges(16).ef_construction(64),
            16,
            16,
        );
        index.insert(&batch_with_null_middle(dim), 0).unwrap();
        assert_eq!(index.len(), 2, "the null row is skipped");

        let query = FixedSizeListArray::try_new_from_values(
            Float32Array::from(vec![3.0f32; dim]),
            dim as i32,
        )
        .unwrap();
        let results = index.search(&query, 2, Some(16), u64::MAX).unwrap();
        assert!(!results.is_empty());
        let (best_dist, best_pos) = results[0];
        assert!(best_dist < 1e-4, "got {}", best_dist);
        assert_eq!(
            best_pos, 2,
            "row 2 resolves to its original offset, not 1, after the skip"
        );
        assert!(
            results.iter().all(|(_, pos)| *pos != 1),
            "the skipped null row must never be returned"
        );
    }

    #[test]
    fn test_index_all_null_batch_adds_no_nodes() {
        // An all-null batch (e.g. an all-tombstone memtable) inserts cleanly and
        // adds no nodes.
        let dim = 4;
        let index = HnswMemIndex::with_capacity(
            1,
            "vector".to_string(),
            DistanceType::L2,
            HnswBuildParams::default(),
            8,
            8,
        );
        let inner = Arc::new(Float32Array::from(vec![0.0f32; dim * 2])) as arrow_array::ArrayRef;
        let nulls =
            arrow_buffer::NullBuffer::new(arrow_buffer::BooleanBuffer::from(vec![false, false]));
        let fsl = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            dim as i32,
            inner,
            Some(nulls),
        )
        .unwrap();
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    dim as i32,
                ),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![0, 1])), Arc::new(fsl)],
        )
        .unwrap();
        index.insert(&batch, 0).unwrap();
        assert_eq!(index.len(), 0, "no nodes for an all-null batch");
    }
}
