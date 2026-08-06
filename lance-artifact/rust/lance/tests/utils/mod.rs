// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use futures::FutureExt;
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance::{
    Dataset,
    dataset::{InsertBuilder, WriteParams},
};
use lance_index::scalar::{InvertedIndexParams, ScalarIndexParams};
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::PQBuildParams;
use lance_index::vector::sq::builder::SQBuildParams;
use lance_index::{IndexParams, IndexType};
use lance_linalg::distance::{DistanceType, MetricType};

#[derive(Clone, Copy, Debug)]
pub enum Fragmentation {
    /// All data in a single file.
    SingleFragment,
    /// Data is spread across multiple fragments, one file per fragment.
    MultiFragment,
}

#[derive(Clone, Copy, Debug)]
pub enum DeletionState {
    /// No deletions are applied.
    NoDeletions,
    /// Delete odd rows.
    DeleteOdd,
    /// Delete even rows.
    DeleteEven,
}

pub struct DatasetTestCases {
    original: RecordBatch,
    index_options: Vec<(String, Vec<Option<IndexType>>)>,
    inverted_index_params: HashMap<String, InvertedIndexParams>,
}

impl DatasetTestCases {
    pub fn from_data(original: RecordBatch) -> Self {
        Self {
            original,
            index_options: Vec::new(),
            inverted_index_params: HashMap::new(),
        }
    }

    pub fn with_index_types(
        mut self,
        column: impl Into<String>,
        index_types: impl IntoIterator<Item = Option<IndexType>>,
    ) -> Self {
        self.index_options
            .push((column.into(), index_types.into_iter().collect()));
        self
    }

    pub fn with_index_types_and_inverted_index_params(
        mut self,
        column: impl Into<String>,
        index_types: impl IntoIterator<Item = Option<IndexType>>,
        inverted_params: InvertedIndexParams,
    ) -> Self {
        let column = column.into();
        self.index_options
            .push((column.clone(), index_types.into_iter().collect()));
        self.inverted_index_params.insert(column, inverted_params);
        self
    }

    fn generate_index_combinations(&self) -> Vec<Vec<(&str, IndexType)>> {
        if self.index_options.is_empty() {
            return vec![vec![]];
        }

        fn generate_recursive<'a>(
            options: &'a [(String, Vec<Option<IndexType>>)],
            current_idx: usize,
            current_combination: Vec<(&'a str, IndexType)>,
            results: &mut Vec<Vec<(&'a str, IndexType)>>,
        ) {
            if current_idx == options.len() {
                // Only add non-empty combinations (filter out all-None case)
                if !current_combination.is_empty() {
                    results.push(current_combination);
                }
                return;
            }

            let (column, index_types) = &options[current_idx];

            // Try each index type for this column (including None)
            for index_type_opt in index_types {
                let mut next_combination = current_combination.clone();
                if let Some(index_type) = index_type_opt {
                    next_combination.push((column.as_str(), *index_type));
                }
                generate_recursive(options, current_idx + 1, next_combination, results);
            }
        }

        let mut results = Vec::new();
        generate_recursive(&self.index_options, 0, Vec::new(), &mut results);
        results
    }

    pub async fn run<F, Fut>(self, test_fn: F) -> Fut::Output
    where
        F: Fn(Dataset, RecordBatch) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        for fragmentation in [Fragmentation::SingleFragment, Fragmentation::MultiFragment] {
            for deletion in [
                DeletionState::NoDeletions,
                DeletionState::DeleteOdd,
                DeletionState::DeleteEven,
            ] {
                let index_combinations = self.generate_index_combinations();
                for indices in index_combinations {
                    let ds = build_dataset(
                        self.original.clone(),
                        fragmentation,
                        deletion,
                        &indices,
                        &self.inverted_index_params,
                    )
                    .await;
                    let context = format!(
                        "fragmentation: {:?}, deletion: {:?}, index: {:?}, inverted_index_params: {:?}",
                        fragmentation, deletion, indices, self.inverted_index_params
                    );
                    // Catch unwind so we can add test context to the panic.
                    AssertUnwindSafe(test_fn(ds, self.original.clone()))
                        .catch_unwind()
                        .await
                        .unwrap_or_else(|_| panic!("Test failed for {}", context));
                }
            }
        }
    }
}

/// Create an in-memory dataset with the given state and data.
///
/// The data in dataset will exactly match the `original` batch. (Extra rows are
/// created for the deleted rows created by `DeletionState`.)
async fn build_dataset(
    original: RecordBatch,
    fragmentation: Fragmentation,
    deletion: DeletionState,
    indices: &[(&str, IndexType)],
    inverted_index_params: &HashMap<String, InvertedIndexParams>,
) -> Dataset {
    let data_to_write = fill_deleted_rows(&original, deletion);

    let max_rows_per_file = if let Fragmentation::MultiFragment = fragmentation {
        3
    } else {
        1_000_000
    };

    let mut ds = InsertBuilder::new("memory://")
        .with_params(&WriteParams {
            max_rows_per_file,
            ..Default::default()
        })
        .execute(vec![data_to_write])
        .await
        .expect("Failed to create test dataset");

    ds.delete("id = -1")
        .await
        .expect("Failed to delete filler rows (id = -1)");

    assert_eq!(ds.count_rows(None).await.unwrap(), original.num_rows());

    for (column, index_type) in indices.iter() {
        // TODO: when possible, make indices cover a portion of rows and not be
        // aligned between indices.

        // Index parameters are chosen to make search results deterministic for small
        // test datasets, not for production use.
        let index_params: Box<dyn IndexParams> = match index_type {
            IndexType::BTree
            | IndexType::Bitmap
            | IndexType::LabelList
            | IndexType::NGram
            | IndexType::ZoneMap
            | IndexType::BloomFilter => Box::new(ScalarIndexParams::for_builtin(
                (*index_type).try_into().unwrap(),
            )),
            IndexType::Inverted => inverted_index_params
                .get(*column)
                .map(|params| Box::new(params.clone()) as Box<dyn IndexParams>)
                .unwrap_or_else(|| {
                    Box::new(ScalarIndexParams::for_builtin(
                        (*index_type).try_into().unwrap(),
                    ))
                }),
            IndexType::IvfFlat => {
                // Use a small number of partitions for testing
                Box::new(VectorIndexParams::ivf_flat(2, MetricType::L2))
            }
            IndexType::IvfPq => {
                // Simple PQ params for testing
                Box::new(VectorIndexParams::ivf_pq(2, 8, 2, MetricType::L2, 10))
            }
            IndexType::IvfSq => Box::new(VectorIndexParams::with_ivf_sq_params(
                DistanceType::L2,
                IvfBuildParams::new(2),
                SQBuildParams::default(),
            )),
            IndexType::IvfHnswFlat => Box::new(VectorIndexParams::with_ivf_flat_params(
                DistanceType::L2,
                IvfBuildParams::new(2),
            )),
            IndexType::IvfHnswPq => Box::new(VectorIndexParams::with_ivf_hnsw_pq_params(
                DistanceType::L2,
                IvfBuildParams::new(2),
                HnswBuildParams::default().ef_construction(200),
                PQBuildParams::new(2, 8),
            )),
            IndexType::IvfHnswSq => Box::new(VectorIndexParams::with_ivf_hnsw_sq_params(
                DistanceType::L2,
                IvfBuildParams::new(2),
                HnswBuildParams::default().ef_construction(200),
                SQBuildParams::default(),
            )),
            _ => {
                // For other index types, use default scalar params
                Box::new(ScalarIndexParams::default())
            }
        };

        ds.create_index_builder(&[column], *index_type, index_params.as_ref())
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to create index on column '{}' with type {:?}: {}",
                    column, index_type, e
                )
            });
    }

    ds
}

/// Insert filler rows into a record batch such that applying deletions to the
/// output will yield the input. For example, given the `deletions: DeletionState::DeleteOdd`
/// and the table:
///
/// ```
/// id | value
///  1 |   "a"
///  2 |   "b"
/// ```
///
/// Produce:
///
/// ```
/// id | value
/// -1 |   "a" (filler row)
///  1 |   "a"
/// -1 |   "a"
///  2 |   "b"
/// ```
///
/// The filler row will have the same values as the original row, but with a special
/// identifier (e.g., -1) to indicate that it is a filler row.
fn fill_deleted_rows(batch: &RecordBatch, deletions: DeletionState) -> RecordBatch {
    // Early return for no deletions
    if let DeletionState::NoDeletions = deletions {
        return batch.clone();
    }

    // Create a filler batch by taking the first row and replacing id with -1
    let schema = batch.schema();
    let mut filler_columns: Vec<ArrayRef> = Vec::new();

    for (i, field) in schema.fields().iter().enumerate() {
        if field.name() == "id" {
            // Create an array with a single -1 value
            filler_columns.push(Arc::new(Int32Array::from(vec![-1])));
        } else {
            // Take the first value from the original column
            let original_column = batch.column(i);
            let sliced = original_column.slice(0, 1);
            filler_columns.push(sliced);
        }
    }

    let filler_batch = RecordBatch::try_new(schema.clone(), filler_columns).unwrap();

    // Create an array of filler batches, one for each row that will be deleted
    let num_rows = batch.num_rows();
    let filler_batches = vec![filler_batch; num_rows];

    // Concatenate all filler batches into one
    let all_fillers = arrow_select::concat::concat_batches(&schema, &filler_batches).unwrap();

    // Create indices for interleaving based on the deletion pattern
    // Format: (batch_index, row_index) where batch_index 0 = original, 1 = fillers
    let mut indices: Vec<(usize, usize)> = Vec::new();

    match deletions {
        DeletionState::DeleteOdd => {
            // Pattern: filler, original[0], filler, original[1], ...
            for i in 0..num_rows {
                indices.push((1, i)); // filler batch, row i
                indices.push((0, i)); // original batch, row i
            }
        }
        DeletionState::DeleteEven => {
            // Pattern: original[0], filler, original[1], filler, ...
            for i in 0..num_rows {
                indices.push((0, i)); // original batch, row i
                indices.push((1, i)); // filler batch, row i
            }
        }
        DeletionState::NoDeletions => unreachable!(),
    }

    // Use interleave to reorder according to our indices
    arrow::compute::interleave_record_batch(&[batch, &all_fillers], &indices).unwrap()
}
