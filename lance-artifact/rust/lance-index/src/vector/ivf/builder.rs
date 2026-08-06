// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Build IVF model

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, FixedSizeListArray, UInt32Array, UInt64Array};
use futures::TryStreamExt;
use object_store::path::Path;

use lance_core::error::{Error, Result};
use lance_io::stream::RecordBatchStream;

/// Parameters to build IVF partitions
#[derive(Debug, Clone)]
pub struct IvfBuildParams {
    /// Deprecated: use `target_partition_size` instead.
    /// Number of partitions to build.
    pub num_partitions: Option<usize>,

    /// Target partition size.
    /// If set, the number of partitions will be computed based on the target partition size.
    /// Otherwise, the `target_partition_size` will be set by index type.
    pub target_partition_size: Option<usize>,

    // ---- kmeans parameters
    /// Max number of iterations to train kmeans.
    pub max_iters: usize,

    /// Use provided IVF centroids.
    pub centroids: Option<Arc<FixedSizeListArray>>,

    /// Retrain centroids.
    /// If true, the centroids will be retrained based on provided `centroids`.
    pub retrain: bool,

    pub sample_rate: usize,

    /// Optional per-step sample rate for streaming IVF kmeans training.
    ///
    /// When set, IVF training loads at most `num_partitions * streaming_sample_rate`
    /// vectors at a time. For `num_partitions > 256`, each chunk is compressed into
    /// a weighted coreset and final centroids are trained with weighted hierarchical
    /// kmeans over the coreset. The coreset budget is also bounded by this rate by
    /// default so large partition counts can control peak memory by lowering
    /// `streaming_sample_rate`. The total number of sampled vectors remains bounded
    /// by `num_partitions * sample_rate`.
    pub streaming_sample_rate: Option<usize>,

    /// Optional coreset rate for streaming IVF kmeans training.
    ///
    /// When set, the final weighted coreset budget is
    /// `num_partitions * streaming_coreset_rate`, independent of
    /// `streaming_sample_rate`. The streaming chunk size is still controlled by
    /// `streaming_sample_rate`.
    pub streaming_coreset_rate: Option<usize>,

    /// Number of extra streaming Lloyd refinement passes to run after streaming
    /// coreset training.
    ///
    /// Each pass reuses the same sampled vectors and only loads
    /// `num_partitions * streaming_sample_rate` raw vectors at a time.  This is
    /// experimental and defaults to 0 to preserve existing behavior.
    pub streaming_refine_passes: usize,

    /// Precomputed partitions file (row_id -> partition_id)
    /// mutually exclusive with `precomputed_shuffle_buffers`
    pub precomputed_partitions_file: Option<String>,

    /// Precomputed shuffle buffers (row_id -> partition_id, pq_code)
    /// mutually exclusive with `precomputed_partitions_file`
    /// requires `centroids` to be set
    ///
    /// The input is expected to be (/dir/to/buffers, [buffer1.lance, buffer2.lance, ...])
    pub precomputed_shuffle_buffers: Option<(Path, Vec<String>)>,

    pub shuffle_partition_batches: usize,

    pub shuffle_partition_concurrency: usize,

    /// Storage options used to load precomputed partitions.
    pub storage_options: Option<HashMap<String, String>>,
}

impl Default for IvfBuildParams {
    fn default() -> Self {
        Self {
            num_partitions: None,
            target_partition_size: None,
            max_iters: 50,
            centroids: None,
            retrain: false,
            sample_rate: 256, // See faiss
            streaming_sample_rate: None,
            streaming_coreset_rate: None,
            streaming_refine_passes: 0,
            precomputed_partitions_file: None,
            precomputed_shuffle_buffers: None,
            shuffle_partition_batches: 1024 * 10,
            shuffle_partition_concurrency: 2,
            storage_options: None,
        }
    }
}

impl IvfBuildParams {
    /// Create a new instance of `IvfBuildParams`.
    pub fn new(num_partitions: usize) -> Self {
        Self {
            num_partitions: Some(num_partitions),
            ..Default::default()
        }
    }

    pub fn with_target_partition_size(target_partition_size: usize) -> Self {
        Self {
            target_partition_size: Some(target_partition_size),
            ..Default::default()
        }
    }

    /// Create a new instance of [`IvfBuildParams`] with centroids.
    pub fn try_with_centroids(
        num_partitions: usize,
        centroids: Arc<FixedSizeListArray>,
    ) -> Result<Self> {
        if num_partitions != centroids.len() {
            return Err(Error::index(format!(
                "IvfBuildParams::try_with_centroids: num_partitions {} != centroids.len() {}",
                num_partitions,
                centroids.len()
            )));
        }
        Ok(Self {
            num_partitions: Some(num_partitions),
            centroids: Some(centroids),
            ..Default::default()
        })
    }
}

pub fn recommended_num_partitions(num_rows: usize, target_partition_size: usize) -> usize {
    // The maximum number of partitions is 4096 to avoid slow KMeans clustering,
    // bump it once we have better clustering algorithms.
    const MAX_PARTITIONS: usize = 4096;
    (num_rows / target_partition_size).clamp(1, MAX_PARTITIONS)
}

/// Load precomputed partitions from disk.
///
/// Currently, because `Dataset` is not cleanly refactored from `lance` to `lance-core`,
/// we have to use `RecordBatchStream` as parameter.
pub async fn load_precomputed_partitions(
    stream: impl RecordBatchStream + Unpin + 'static,
    size_hint: usize,
) -> Result<HashMap<u64, u32>> {
    let partition_lookup = stream
        .try_fold(HashMap::with_capacity(size_hint), |mut lookup, batch| {
            let row_ids: &UInt64Array = batch
                .column_by_name("row_id")
                .expect("malformed partition file: missing row_id column")
                .as_primitive();
            let partitions: &UInt32Array = batch
                .column_by_name("partition")
                .expect("malformed partition file: missing partition column")
                .as_primitive();
            row_ids
                .values()
                .iter()
                .zip(partitions.values().iter())
                .for_each(|(row_id, partition)| {
                    lookup.insert(*row_id, *partition);
                });
            async move { Ok(lookup) }
        })
        .await?;

    Ok(partition_lookup)
}
