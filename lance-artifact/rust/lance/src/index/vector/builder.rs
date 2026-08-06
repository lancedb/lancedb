// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::utils::row_addr_remap::RowAddrRemap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, pin::Pin};

use arrow::array::{AsArray as _, PrimitiveBuilder, UInt32Builder, UInt64Builder};
use arrow::compute::sort_to_indices;
use arrow::datatypes::{self};
use arrow::datatypes::{Float16Type, Float64Type, UInt8Type, UInt64Type};
use arrow_array::types::Float32Type;
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, FixedSizeListArray, PrimitiveArray,
    RecordBatch, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Fields};
use futures::{FutureExt, stream};
use futures::{
    Stream,
    prelude::stream::{StreamExt, TryStreamExt},
};
use lance_arrow::{FixedSizeListArrayExt, RecordBatchExt};
use lance_core::ROW_ID;
use lance_core::datatypes::Schema;
use lance_core::utils::tempfile::TempStdDir;
use lance_core::utils::tokio::{get_num_compute_intensive_cpus, spawn_cpu};
use lance_core::{Error, ROW_ID_FIELD, Result};
use lance_file::version::ConcreteFileVersion;
use lance_file::version::LanceFileVersion;
use lance_file::versions;
use lance_file::writer::FileWriterOptions;
use lance_index::frag_reuse::FragReuseIndex;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::optimize::OptimizeOptions;
use lance_index::progress::{IndexBuildProgress, NoopIndexBuildProgress};
use lance_index::vector::bq::storage::{RABIT_CODE_COLUMN, unpack_codes};
use lance_index::vector::kmeans::KMeansParams;
use lance_index::vector::pq::storage::transpose;
use lance_index::vector::quantizer::{
    QuantizationMetadata, QuantizationType, QuantizerBuildParams,
};
use lance_index::vector::quantizer::{QuantizerMetadata, QuantizerStorage};
use lance_index::vector::shared::{SupportedIvfIndexType, write_unified_ivf_and_index_metadata};
use lance_index::vector::storage::STORAGE_METADATA_KEY;
use lance_index::vector::transform::Flatten;
use lance_index::vector::v3::shuffler::{EmptyReader, IvfShufflerReader, create_ivf_shuffler};
use lance_index::vector::v3::subindex::SubIndexType;
use lance_index::vector::{LOSS_METADATA_KEY, PART_ID_COLUMN, PQ_CODE_COLUMN, VectorIndex};
use lance_index::vector::{PART_ID_FIELD, ivf::storage::IvfModel};
use lance_index::{
    INDEX_AUXILIARY_FILE_NAME, INDEX_FILE_NAME, pb,
    vector::{
        DISTANCE_TYPE_KEY,
        ivf::{IvfBuildParams, storage::IVF_METADATA_KEY},
        quantizer::Quantization,
        storage::{StorageBuilder, VectorStore},
        transform::Transformer,
        v3::{
            shuffler::{ShuffleReader, Shuffler},
            subindex::IvfSubIndex,
        },
    },
};
use lance_index::{
    INDEX_METADATA_SCHEMA_KEY, IndexMetadata, IndexType, MAX_PARTITION_SIZE_FACTOR,
    MIN_PARTITION_SIZE_PERCENT,
};
use lance_io::local::to_local_path;
use lance_io::stream::RecordBatchStream;
use lance_io::{object_store::ObjectStore, stream::RecordBatchStreamAdapter};
use lance_linalg::distance::{DistanceType, Dot, L2, Normalize};
use lance_linalg::kernels::normalize_fsl;
use lance_table::format::IndexFile;
use log::info;
use object_store::path::Path;
use prost::Message;
use tracing::{Level, instrument, span};

use crate::Dataset;
use crate::dataset::ProjectionRequest;
use crate::dataset::index::dataset_format_version;
use crate::index::vector::ivf::v2::PartitionEntry;
use crate::index::vector::utils::infer_vector_dim;

use super::v2::IVFIndex;
use super::{
    ivf::load_precomputed_partitions_if_available,
    utils::{self, get_vector_type},
};

// the number of partitions to evaluate for reassigning
const REASSIGN_RANGE: usize = 64;
// sample size for kmeans training when splitting a partition (sample_rate * k = 256 * 2)
const SPLIT_SAMPLE_SIZE: usize = 512;

/// Build a new centroid array that incorporates the results of partition splits.
///
/// For each `(part_idx, centroid1, centroid2)` in `splits`:
/// - `original[part_idx]` is replaced by `centroid1`
/// - `centroid2` is appended after all existing centroids
///
/// Unchanged centroids keep their original indices.  The k-th split's second
/// centroid lands at index `original.len() + k`.
fn apply_centroid_splits(
    original: &FixedSizeListArray,
    splits: &[(usize, ArrayRef, ArrayRef)],
) -> Result<FixedSizeListArray> {
    let mut new_centroids: Vec<ArrayRef> = original.iter().map(|v| v.unwrap()).collect();
    for (part_idx, centroid1, centroid2) in splits {
        new_centroids[*part_idx] = centroid1.clone();
        new_centroids.push(centroid2.clone());
    }
    let refs: Vec<&dyn Array> = new_centroids.iter().map(|a| a.as_ref()).collect();
    let concatenated = arrow::compute::concat(&refs)?;
    Ok(FixedSizeListArray::try_new_from_values(
        concatenated,
        original.value_length(),
    )?)
}

// Builder for IVF index
// The builder will train the IVF model and quantizer, shuffle the dataset, and build the sub index
// for each partition.
// To build the index for the whole dataset, call `build` method.
// To build the index for given IVF, quantizer, data stream,
// call `with_ivf`, `with_quantizer`, `shuffle_data_input`, and `build` in order.
pub struct IvfIndexBuilder<S: IvfSubIndex, Q: Quantization> {
    store: ObjectStore,
    column: String,
    index_dir: Path,
    distance_type: DistanceType,
    // build params, only needed for building new IVF, quantizer
    dataset: Option<Dataset>,
    shuffler: Option<Arc<dyn Shuffler>>,
    ivf_params: Option<IvfBuildParams>,
    quantizer_params: Option<Q::BuildParams>,
    sub_index_params: Option<S::BuildParams>,
    _temp_dir: TempStdDir, // store this for keeping the temp dir alive and clean up after build
    temp_dir: Path,

    // fields will be set during build
    ivf: Option<IvfModel>,
    quantizer: Option<Q>,
    shuffle_reader: Option<Arc<dyn ShuffleReader>>,
    // unindexed input stream attached by callers; consumed during `build`'s
    // shuffle stage so progress is reported. Wrapped in Mutex so the builder
    // remains `Sync` (the boxed dyn Stream is not Sync on its own).
    shuffle_data_input: Mutex<Option<UnindexedStream>>,

    // fields for merging indices / remapping
    existing_indices: Vec<Arc<dyn VectorIndex>>,

    frag_reuse_index: Option<Arc<FragReuseIndex>>,

    // fragments for distributed indexing
    fragment_filter: Option<Vec<u32>>,

    // optimize options for only incremental build
    optimize_options: Option<OptimizeOptions>,
    // number of indices merged
    merged_num: usize,
    // whether to transpose codes when building storage
    transpose_codes: bool,

    // lance file version for writing index files
    format_version: LanceFileVersion,

    progress: Arc<dyn IndexBuildProgress>,
}

type BuildStream<S, Q> =
    Pin<Box<dyn Stream<Item = Result<Option<(<Q as Quantization>::Storage, S, f64)>>> + Send>>;

type UnindexedStream = Box<dyn Stream<Item = Result<RecordBatch>> + Send + Unpin + 'static>;

pub struct VectorIndexBuildSummary {
    pub indices_merged: usize,
    pub files: Vec<IndexFile>,
}

impl<S: IvfSubIndex + 'static, Q: Quantization + 'static> IvfIndexBuilder<S, Q> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dataset: Dataset,
        column: String,
        index_dir: Path,
        distance_type: DistanceType,
        shuffler: Box<dyn Shuffler>,
        ivf_params: Option<IvfBuildParams>,
        quantizer_params: Option<Q::BuildParams>,
        sub_index_params: S::BuildParams,
        frag_reuse_index: Option<Arc<FragReuseIndex>>,
    ) -> Result<Self> {
        let temp_dir = TempStdDir::default();
        let temp_dir_path = Path::from_filesystem_path(&temp_dir)?;
        let format_version = dataset_format_version(&dataset);
        Ok(Self {
            store: dataset.object_store.as_ref().clone(),
            column,
            index_dir,
            distance_type,
            dataset: Some(dataset),
            shuffler: Some(shuffler.into()),
            ivf_params,
            quantizer_params,
            sub_index_params: Some(sub_index_params),
            _temp_dir: temp_dir,
            temp_dir: temp_dir_path,
            // fields will be set during build
            ivf: None,
            quantizer: None,
            shuffle_reader: None,
            shuffle_data_input: Mutex::new(None),
            existing_indices: Vec::new(),
            frag_reuse_index,
            fragment_filter: None,
            optimize_options: None,
            merged_num: 0,
            transpose_codes: true,
            format_version,
            progress: Arc::new(NoopIndexBuildProgress),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_incremental(
        dataset: Dataset,
        column: String,
        index_dir: Path,
        distance_type: DistanceType,
        shuffler: Box<dyn Shuffler>,
        sub_index_params: S::BuildParams,
        frag_reuse_index: Option<Arc<FragReuseIndex>>,
        optimize_options: OptimizeOptions,
    ) -> Result<Self> {
        let mut builder = Self::new(
            dataset,
            column,
            index_dir,
            distance_type,
            shuffler,
            None,
            None,
            sub_index_params,
            frag_reuse_index,
        )?;
        builder.optimize_options = Some(optimize_options);
        Ok(builder)
    }

    pub fn new_remapper(
        dataset: Dataset,
        column: String,
        index_dir: Path,
        index: Arc<dyn VectorIndex>,
    ) -> Result<Self> {
        let ivf_index = index
            .as_any()
            .downcast_ref::<IVFIndex<S, Q>>()
            .ok_or(Error::invalid_input("existing index is not IVF index"))?;

        let temp_dir = TempStdDir::default();
        let temp_dir_path = Path::from_filesystem_path(&temp_dir)?;
        let format_version = dataset_format_version(&dataset);
        Ok(Self {
            store: dataset.object_store.as_ref().clone(),
            column,
            index_dir,
            distance_type: ivf_index.metric_type(),
            dataset: Some(dataset),
            shuffler: None,
            ivf_params: None,
            quantizer_params: None,
            sub_index_params: None,
            _temp_dir: temp_dir,
            temp_dir: temp_dir_path,
            ivf: Some(ivf_index.ivf_model().clone()),
            quantizer: Some(ivf_index.quantizer().try_into()?),
            shuffle_reader: None,
            shuffle_data_input: Mutex::new(None),
            existing_indices: vec![index],
            frag_reuse_index: None,
            fragment_filter: None,
            optimize_options: None,
            merged_num: 0,
            transpose_codes: true,
            format_version,
            progress: Arc::new(NoopIndexBuildProgress),
        })
    }

    // build the index and return the files created by the writer.
    pub async fn build(&mut self) -> Result<VectorIndexBuildSummary> {
        let progress = self.progress.clone();

        // step 1. train IVF & quantizer
        let max_iters = self.ivf_params.as_ref().map(|p| p.max_iters as u64);
        progress
            .stage_start("train_ivf", max_iters, "iterations")
            .await?;
        self.with_ivf(self.load_or_build_ivf().boxed().await?);
        progress.stage_complete("train_ivf").await?;

        progress.stage_start("train_quantizer", None, "").await?;
        self.with_quantizer(self.load_or_build_quantizer().await?);
        progress.stage_complete("train_quantizer").await?;

        // step 2. shuffle the dataset
        if self.shuffle_reader.is_none() {
            let num_rows = self.num_rows_to_shuffle().await?;
            progress.stage_start("shuffle", num_rows, "rows").await?;
            let input = self.shuffle_data_input.lock().unwrap().take();
            if let Some(input) = input {
                self.shuffle_data(Some(input)).boxed().await?;
            } else {
                self.shuffle_dataset().boxed().await?;
            }
            progress.stage_complete("shuffle").await?;
        }

        // step 3. build and merge partitions
        let num_partitions = self.ivf.as_ref().map(|ivf| ivf.num_partitions() as u64);
        progress
            .stage_start("merge_partitions", num_partitions, "partitions")
            .await?;
        let build_idx_stream = self.build_partitions().boxed().await?;
        let files = self.merge_partitions(build_idx_stream).await?;
        progress.stage_complete("merge_partitions").await?;

        Ok(VectorIndexBuildSummary {
            indices_merged: self.merged_num,
            files,
        })
    }

    pub async fn remap(&mut self, mapping: &RowAddrRemap) -> Result<Vec<IndexFile>> {
        if self.existing_indices.is_empty() {
            return Err(Error::invalid_input(
                "No existing indices available for remapping",
            ));
        }
        let Some(ivf) = self.ivf.as_ref() else {
            return Err(Error::invalid_input("IVF model not set before remapping"));
        };

        log::info!("remap {} partitions", ivf.num_partitions());
        let existing_index = self.existing_indices[0].clone();
        let mapping = Arc::new(mapping.clone());
        let build_iter =
            (0..ivf.num_partitions()).map(move |part_id| {
                let existing_index = existing_index.clone();
                let mapping = mapping.clone();
                async move {
                    let ivf_index = existing_index
                        .as_any()
                        .downcast_ref::<IVFIndex<S, Q>>()
                        .ok_or(Error::invalid_input("existing index is not IVF index"))?;
                    let part = ivf_index
                        .load_partition(part_id, false, &NoOpMetricsCollector)
                        .await?;
                    let part = part.as_any().downcast_ref::<PartitionEntry<S, Q>>().ok_or(
                        Error::internal("failed to downcast partition entry".to_string()),
                    )?;

                    let storage = part.storage.remap(&mapping)?;
                    let index = part.index.remap(&mapping, &storage)?;
                    Result::Ok(Some((storage, index, 0.0)))
                }
            });

        let files = self
            .merge_partitions(
                stream::iter(build_iter)
                    .buffered(get_num_compute_intensive_cpus())
                    .boxed(),
            )
            .await?;
        Ok(files)
    }

    pub fn with_ivf(&mut self, ivf: IvfModel) -> &mut Self {
        self.ivf = Some(ivf);
        self
    }

    pub fn with_quantizer(&mut self, quantizer: Q) -> &mut Self {
        self.quantizer = Some(quantizer);
        self
    }

    pub fn with_existing_indices(&mut self, indices: Vec<Arc<dyn VectorIndex>>) -> &mut Self {
        self.existing_indices = indices;
        self
    }

    /// Set fragment filter for distributed indexing
    pub fn with_fragment_filter(&mut self, fragment_ids: Vec<u32>) -> &mut Self {
        self.fragment_filter = Some(Dataset::normalize_fragment_ids(&fragment_ids));
        self
    }

    pub fn with_optional_fragment_filter(&mut self, fragment_ids: Option<&[u32]>) -> &mut Self {
        if let Some(fragment_ids) = fragment_ids {
            self.fragment_filter = Some(Dataset::normalize_fragment_ids(fragment_ids));
        }
        self
    }

    /// Control whether codes are transposed when building storage.
    /// This mainly affects intermediate PQ/RQ storage when building distributed indices.
    pub fn with_transpose(&mut self, transpose: bool) -> &mut Self {
        self.transpose_codes = transpose;
        self
    }

    /// Set progress callback for index building
    pub fn with_progress(&mut self, progress: Arc<dyn IndexBuildProgress>) -> &mut Self {
        self.progress = progress;
        self
    }

    #[instrument(name = "load_or_build_ivf", level = "debug", skip_all)]
    async fn load_or_build_ivf(&self) -> Result<IvfModel> {
        match &self.ivf {
            Some(ivf) => Ok(ivf.clone()),
            None => {
                let Some(dataset) = self.dataset.as_ref() else {
                    return Err(Error::invalid_input(
                        "dataset not set before loading or building IVF",
                    ));
                };
                let dim = utils::get_vector_dim(dataset.schema(), &self.column)?;
                let ivf_params = self
                    .ivf_params
                    .as_ref()
                    .ok_or(Error::invalid_input("IVF build params not set"))?;
                super::build_ivf_model(
                    dataset,
                    &self.column,
                    dim,
                    self.distance_type,
                    ivf_params,
                    self.fragment_filter.as_deref(),
                    self.progress.clone(),
                )
                .await
            }
        }
    }

    #[instrument(name = "load_or_build_quantizer", level = "debug", skip_all)]
    async fn load_or_build_quantizer(&self) -> Result<Q> {
        if self.quantizer.is_some() {
            return Ok(self.quantizer.clone().unwrap());
        }

        let Some(dataset) = self.dataset.as_ref() else {
            return Err(Error::invalid_input(
                "dataset not set before loading or building quantizer",
            ));
        };
        let sample_size_hint = match &self.quantizer_params {
            Some(params) => params.sample_size(),
            None => 256 * 256, // here it must be retrain, let's just set sample size to the default value
        };

        let start = std::time::Instant::now();
        info!(
            "loading training data for quantizer. sample size: {}",
            sample_size_hint
        );
        let training_data = utils::maybe_sample_training_data(
            dataset,
            &self.column,
            sample_size_hint,
            self.fragment_filter.as_deref(),
        )
        .await?;
        info!(
            "Finished loading training data in {:02} seconds",
            start.elapsed().as_secs_f32()
        );

        // If metric type is cosine, normalize the training data, and after this point,
        // treat the metric type as L2.
        let training_data = if self.distance_type == DistanceType::Cosine {
            lance_linalg::kernels::normalize_fsl_owned(training_data)?
        } else {
            training_data
        };

        // we filtered out nulls when sampling, but we still need to filter out NaNs and INFs here
        let training_data = utils::filter_finite_training_data(training_data)?;

        let training_data = match (self.ivf.as_ref(), Q::use_residual(self.distance_type)) {
            (Some(ivf), true) => {
                let ivf_transformer = lance_index::vector::ivf::new_ivf_transformer(
                    ivf.centroids.clone().unwrap(),
                    DistanceType::L2,
                    vec![],
                );
                span!(Level::INFO, "compute residual for PQ training")
                    .in_scope(|| ivf_transformer.compute_residual(&training_data))?
            }
            _ => training_data,
        };

        info!("Start to train quantizer");
        let start = std::time::Instant::now();
        let quantizer = match &self.quantizer {
            Some(q) => q.clone(),
            None => {
                let quantizer_params = self
                    .quantizer_params
                    .as_ref()
                    .ok_or(Error::invalid_input("quantizer build params not set"))?;
                Q::build(&training_data, DistanceType::L2, quantizer_params)?
            }
        };
        info!(
            "Trained quantizer in {:02} seconds",
            start.elapsed().as_secs_f32()
        );
        Ok(quantizer)
    }

    fn rename_row_id(
        stream: impl RecordBatchStream + Unpin + 'static,
        row_id_idx: usize,
    ) -> impl RecordBatchStream + Unpin + 'static {
        let new_schema = Arc::new(arrow_schema::Schema::new(
            stream
                .schema()
                .fields
                .iter()
                .enumerate()
                .map(|(field_idx, field)| {
                    if field_idx == row_id_idx {
                        arrow_schema::Field::new(
                            ROW_ID,
                            field.data_type().clone(),
                            field.is_nullable(),
                        )
                    } else {
                        field.as_ref().clone()
                    }
                })
                .collect::<Fields>(),
        ));
        RecordBatchStreamAdapter::new(
            new_schema.clone(),
            stream.map_ok(move |batch| {
                RecordBatch::try_new(new_schema.clone(), batch.columns().to_vec()).unwrap()
            }),
        )
    }

    async fn num_rows_to_shuffle(&self) -> Result<Option<u64>> {
        let Some(dataset) = self.dataset.as_ref() else {
            return Ok(None);
        };
        match &self.fragment_filter {
            Some(fragment_ids) => Ok(Some(
                dataset
                    .count_rows_in_existing_fragments(fragment_ids)
                    .await? as u64,
            )),
            None => Ok(Some(dataset.count_rows(None).await? as u64)),
        }
    }

    async fn shuffle_dataset(&mut self) -> Result<()> {
        let Some(dataset) = self.dataset.as_ref() else {
            return Err(Error::invalid_input("dataset not set before shuffling"));
        };

        let stream = match self
            .ivf_params
            .as_ref()
            .and_then(|p| p.precomputed_shuffle_buffers.as_ref())
        {
            Some((uri, _)) => {
                let uri = to_local_path(uri);
                // the uri points to data directory,
                // so need to trim the "data" suffix for reading the dataset
                let uri = uri.trim_end_matches("data");
                log::info!("shuffle with precomputed shuffle buffers from {}", uri);
                let ds = Dataset::open(uri).await?;
                ds.scan().try_into_stream().await?
            }
            _ => {
                log::info!("shuffle column {} over dataset", self.column);
                let mut builder = dataset.scan();
                builder
                    .batch_readahead(get_num_compute_intensive_cpus())
                    .project(&[self.column.as_str()])?
                    .with_row_id();

                // Apply fragment filter for distributed indexing
                if let Some(fragment_ids) = &self.fragment_filter {
                    log::info!(
                        "applying fragment filter for distributed indexing: {:?}",
                        fragment_ids
                    );
                    builder.with_fragments(
                        dataset.get_existing_fragment_metadata_from_ids(fragment_ids),
                    );
                }

                let (vector_type, _) = get_vector_type(dataset.schema(), &self.column)?;
                let is_multivector = matches!(vector_type, datatypes::DataType::List(_));
                if is_multivector {
                    builder.batch_size(64);
                }
                builder.try_into_stream().await?
            }
        };

        if let Some((row_id_idx, _)) = stream.schema().column_with_name("row_id") {
            // When using precomputed shuffle buffers we can't use the column name _rowid
            // since it is reserved.  So we tolerate `row_id` as well here (and rename it
            // to _rowid to match the non-precomputed path)
            self.shuffle_data(Some(Self::rename_row_id(stream, row_id_idx)))
                .await?;
        } else {
            self.shuffle_data(Some(stream)).await?;
        }
        Ok(())
    }

    /// Attach an unindexed input stream. The shuffle is deferred until
    /// `build()` so progress reporting wraps the actual shuffle work.
    /// Data must have schema | ROW_ID | vector_column |.
    ///
    /// Passing `None` records "no unindexed data" by installing an empty
    /// shuffle reader directly, so `build()` won't fall back to re-scanning
    /// the dataset.
    pub fn shuffle_data_input(
        &mut self,
        data: Option<impl RecordBatchStream + Unpin + 'static>,
    ) -> &mut Self {
        match data {
            Some(d) => {
                *self.shuffle_data_input.lock().unwrap() = Some(Box::new(d) as UnindexedStream);
            }
            None => {
                self.shuffle_reader = Some(Arc::new(EmptyReader));
            }
        }
        self
    }

    // shuffle the unindexed data and existing indices
    // data must be with schema | ROW_ID | vector_column |
    // the shuffled data will be with schema | ROW_ID | PART_ID | code_column |
    pub async fn shuffle_data(
        &mut self,
        data: Option<impl Stream<Item = Result<RecordBatch>> + Unpin + Send + 'static>,
    ) -> Result<&mut Self> {
        let Some(ivf) = self.ivf.as_ref() else {
            return Err(Error::invalid_input("IVF not set before shuffle data"));
        };

        let Some(data) = data else {
            // If we don't specify the shuffle reader, it's going to re-read the
            // dataset and duplicate the data.
            self.shuffle_reader = Some(Arc::new(EmptyReader));

            return Ok(self);
        };

        let Some(quantizer) = self.quantizer.clone() else {
            return Err(Error::invalid_input(
                "quantizer not set before shuffle data",
            ));
        };
        let Some(shuffler) = self.shuffler.as_ref() else {
            return Err(Error::invalid_input("shuffler not set before shuffle data"));
        };

        let code_column = quantizer.column();

        let transformer = Arc::new(
            lance_index::vector::ivf::new_ivf_transformer_with_quantizer(
                ivf.centroids.clone().unwrap(),
                self.distance_type,
                &self.column,
                quantizer.into(),
                None,
            )?,
        );

        let precomputed_partitions = if let Some(params) = self.ivf_params.as_ref() {
            load_precomputed_partitions_if_available(params)
                .await?
                .unwrap_or_default()
        } else {
            HashMap::new()
        };

        let partition_map = Arc::new(precomputed_partitions);
        let mut transformed_stream = Box::pin(
            data.map(move |batch| {
                let partition_map = partition_map.clone();
                let ivf_transformer = transformer.clone();
                tokio::spawn(async move {
                    let mut batch = batch?;
                    if !partition_map.is_empty() {
                        let row_ids = &batch[ROW_ID];
                        let part_ids = UInt32Array::from_iter(
                            row_ids
                                .as_primitive::<UInt64Type>()
                                .values()
                                .iter()
                                .map(|row_id| partition_map.get(row_id).copied()),
                        );
                        let part_ids = UInt32Array::from(part_ids);
                        batch = batch
                            .try_with_column(PART_ID_FIELD.clone(), Arc::new(part_ids.clone()))
                            .expect("failed to add part id column");

                        if part_ids.null_count() > 0 {
                            log::info!(
                                "Filter out rows without valid partition IDs: null_count={}",
                                part_ids.null_count()
                            );
                            let indices = UInt32Array::from_iter(
                                part_ids
                                    .iter()
                                    .enumerate()
                                    .filter_map(|(idx, v)| v.map(|_| idx as u32)),
                            );
                            assert_eq!(indices.len(), batch.num_rows() - part_ids.null_count());
                            batch = batch.take(&indices)?;
                        }
                    }

                    match batch.schema().column_with_name(code_column) {
                        Some(_) => {
                            // this batch is already transformed (in case of GPU training)
                            Ok(batch)
                        }
                        None => ivf_transformer.transform(&batch),
                    }
                })
            })
            .buffered(get_num_compute_intensive_cpus())
            .map(|x| x.unwrap())
            .peekable(),
        );

        let batch = transformed_stream.as_mut().peek_mut().await;
        let schema = match batch {
            Some(Ok(b)) => b.schema(),
            Some(Err(e)) => return Err(std::mem::replace(e, Error::Stop)),
            None => {
                log::info!("no data to shuffle");
                self.shuffle_reader = Some(Arc::new(IvfShufflerReader::new(
                    Arc::new(self.store.clone()),
                    self.temp_dir.clone(),
                    vec![0; ivf.num_partitions()],
                    0.0,
                )));
                return Ok(self);
            }
        };

        self.shuffle_reader = Some(
            shuffler
                .shuffle(Box::new(RecordBatchStreamAdapter::new(
                    schema,
                    transformed_stream,
                )))
                .await?
                .into(),
        );

        Ok(self)
    }

    #[instrument(name = "build_partitions", level = "debug", skip_all)]
    async fn build_partitions(&mut self) -> Result<BuildStream<S, Q>> {
        let Some(ivf) = self.ivf.as_ref() else {
            return Err(Error::invalid_input(
                "IVF not set before building partitions",
            ));
        };
        let Some(quantizer) = self.quantizer.clone() else {
            return Err(Error::invalid_input(
                "quantizer not set before building partition",
            ));
        };
        let Some(sub_index_params) = self.sub_index_params.clone() else {
            return Err(Error::invalid_input(
                "sub index params not set before building partition",
            ));
        };
        let Some(reader) = self.shuffle_reader.as_ref() else {
            return Err(Error::invalid_input(
                "shuffle reader not set before building partitions",
            ));
        };

        // if no partitions to split, we just create a new delta index,
        // otherwise, we need to merge all existing indices and split large partitions.
        let reader = reader.clone();
        let num_indices_to_merge = self
            .optimize_options
            .as_ref()
            .and_then(|opt| opt.num_indices_to_merge);
        let no_partition_adjustment = || {
            let is_retrain = self
                .optimize_options
                .as_ref()
                .map(|opt| opt.retrain)
                .unwrap_or(false);
            let num_to_merge = match is_retrain {
                true => self.existing_indices.len(), // retrain, merge all indices
                false => num_indices_to_merge.unwrap_or(0),
            };

            let indices_to_merge = self.existing_indices
                [self.existing_indices.len().saturating_sub(num_to_merge)..]
                .to_vec();

            (
                vec![None; ivf.num_partitions()],
                Arc::new(indices_to_merge),
                None,
            )
        };

        let (assign_batches, merge_indices, partition_adjustment) =
            if num_indices_to_merge.is_some() || self.optimize_options.is_none() {
                no_partition_adjustment()
            } else {
                let (split_partitions, join_partition) =
                    Self::check_partition_adjustment(ivf, reader.as_ref(), &self.existing_indices)?;
                if !split_partitions.is_empty() {
                    log::info!(
                        "split partitions {:?}, will merge all {} delta indices",
                        split_partitions,
                        self.existing_indices.len()
                    );
                    let split_result = self
                        .split_partitions_streaming(&split_partitions, ivf)
                        .await?;
                    let Some(ivf) = self.ivf.as_mut() else {
                        return Err(Error::invalid_input(
                            "IVF not set before building partitions",
                        ));
                    };
                    ivf.centroids = Some(split_result.new_centroids);
                    (
                        vec![None; ivf.num_partitions()],
                        Arc::new(self.existing_indices.clone()),
                        Some(PartitionAdjustment::Split {
                            affected_partitions: split_result.affected_partitions,
                            split_shuffle_reader: split_result.shuffle_reader,
                        }),
                    )
                } else {
                    match join_partition {
                        Some(partition) => {
                            log::info!("join partition {}", partition);
                            let results = self.join_partition(partition, ivf).await?;
                            let Some(ivf) = self.ivf.as_mut() else {
                                return Err(Error::invalid_input(
                                    "IVF model not set before joining partition",
                                ));
                            };
                            ivf.centroids = Some(results.new_centroids);
                            (
                                results.assign_batches,
                                Arc::new(self.existing_indices.clone()),
                                Some(PartitionAdjustment::Join(partition)),
                            )
                        }
                        None => no_partition_adjustment(),
                    }
                }
            };
        self.merged_num = merge_indices.len();
        log::info!(
            "merge {}/{} delta indices",
            self.merged_num,
            self.existing_indices.len()
        );

        let distance_type = self.distance_type;
        let column = self.column.clone();
        let frag_reuse_index = self.frag_reuse_index.clone();
        let partition_adjustment = Arc::new(partition_adjustment);
        let build_iter =
            assign_batches
                .into_iter()
                .enumerate()
                .map(move |(partition, assign_batch)| {
                    let reader = reader.clone();
                    let indices = merge_indices.clone();
                    let distance_type = distance_type;
                    let quantizer = quantizer.clone();
                    let sub_index_params = sub_index_params.clone();
                    let column = column.clone();
                    let frag_reuse_index = frag_reuse_index.clone();
                    let partition_adjustment = partition_adjustment.clone();
                    async move {
                        let (is_affected, split_reader) = match partition_adjustment.as_ref() {
                            Some(PartitionAdjustment::Split {
                                affected_partitions,
                                split_shuffle_reader,
                            }) => (
                                affected_partitions.contains(&partition),
                                Some(split_shuffle_reader.clone()),
                            ),
                            _ => (false, None),
                        };
                        let partition = match partition_adjustment.as_ref() {
                            Some(PartitionAdjustment::Join(joined_partition))
                                if partition >= *joined_partition =>
                            {
                                partition + 1
                            }
                            _ => partition,
                        };

                        // For affected partitions, the split shuffle reader has
                        // all data (existing + new), re-assigned with updated
                        // centroids. For other partitions, read from existing
                        // indices + original shuffle reader as normal.
                        let (mut batches, mut loss) = if is_affected {
                            Self::take_partition_batches(
                                partition,
                                &[],
                                Some(split_reader.as_ref().unwrap().as_ref()),
                            )
                            .await?
                        } else {
                            Self::take_partition_batches(
                                partition,
                                indices.as_ref(),
                                Some(reader.as_ref()),
                            )
                            .await?
                        };

                        // For unaffected partitions during a split, vectors from
                        // affected partitions may have been reassigned here.
                        if !is_affected && let Some(sr) = split_reader.as_ref() {
                            let (extra, extra_loss) =
                                Self::take_partition_batches(partition, &[], Some(sr.as_ref()))
                                    .await?;
                            batches.extend(extra);
                            loss += extra_loss;
                        }

                        spawn_cpu(move || {
                            // Apply assign_batch for join operations (splits no
                            // longer use assign_batches)
                            if let Some((assign_batch, deleted_row_ids)) = assign_batch {
                                if !deleted_row_ids.is_empty() {
                                    let deleted_row_ids = HashSet::<u64>::from_iter(
                                        deleted_row_ids.values().iter().copied(),
                                    );
                                    for batch in batches.iter_mut() {
                                        let row_ids = batch[ROW_ID].as_primitive::<UInt64Type>();
                                        let mask =
                                            BooleanArray::from_iter(row_ids.iter().map(|row_id| {
                                                row_id.map(|row_id| {
                                                    !deleted_row_ids.contains(&row_id)
                                                })
                                            }));
                                        *batch = arrow::compute::filter_record_batch(batch, &mask)?;
                                    }
                                }

                                if assign_batch.num_rows() > 0 {
                                    // Drop PART_ID column from assign_batch to match schema of existing batches
                                    let assign_batch = assign_batch.drop_column(PART_ID_COLUMN)?;
                                    batches.push(assign_batch);
                                }
                            }

                            let num_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
                            if num_rows == 0 {
                                return Ok(None);
                            }

                            let (storage, sub_index) = Self::build_index(
                                distance_type,
                                quantizer,
                                sub_index_params,
                                batches,
                                column,
                                frag_reuse_index,
                            )?;
                            Ok(Some((storage, sub_index, loss)))
                        })
                        .await
                    }
                });
        Ok(stream::iter(build_iter)
            .buffered(get_num_compute_intensive_cpus())
            .boxed())
    }

    #[instrument(name = "build_index", level = "debug", skip_all)]
    #[allow(clippy::too_many_arguments)]
    fn build_index(
        distance_type: DistanceType,
        quantizer: Q,
        sub_index_params: S::BuildParams,
        batches: Vec<RecordBatch>,
        column: String,
        frag_reuse_index: Option<Arc<FragReuseIndex>>,
    ) -> Result<(Q::Storage, S)> {
        let storage = StorageBuilder::new(column, distance_type, quantizer, frag_reuse_index)?
            .build(batches)?;
        let sub_index = S::index_vectors(&storage, sub_index_params)?;

        Ok((storage, sub_index))
    }

    #[instrument(name = "take_partition_batches", level = "debug", skip_all)]
    async fn take_partition_batches(
        part_id: usize,
        existing_indices: &[Arc<dyn VectorIndex>],
        reader: Option<&dyn ShuffleReader>,
    ) -> Result<(Vec<RecordBatch>, f64)> {
        let mut batches = Vec::new();
        for existing_index in existing_indices.iter() {
            let existing_index = existing_index
                .as_any()
                .downcast_ref::<IVFIndex<S, Q>>()
                .ok_or(Error::invalid_input("existing index is not IVF index"))?;

            // Skip if this partition doesn't exist in the existing index
            // This can happen after a split creates a new partition
            if part_id >= existing_index.ivf_model().num_partitions() {
                continue;
            }

            let part_storage = existing_index.load_partition_storage(part_id, None).await?;
            let mut part_batches = part_storage.to_batches()?.collect::<Vec<_>>();
            // for PQ, the PQ codes are transposed, so we need to transpose them back
            match Q::quantization_type() {
                QuantizationType::Product => {
                    for batch in part_batches.iter_mut() {
                        if batch.num_rows() == 0 {
                            continue;
                        }

                        let codes = batch[PQ_CODE_COLUMN]
                            .as_fixed_size_list()
                            .values()
                            .as_primitive::<datatypes::UInt8Type>();
                        let codes_num_bytes = codes.len() / batch.num_rows();
                        let original_codes = transpose(codes, codes_num_bytes, batch.num_rows());
                        let original_codes = FixedSizeListArray::try_new_from_values(
                            original_codes,
                            codes_num_bytes as i32,
                        )?;
                        *batch = batch
                            .replace_column_by_name(PQ_CODE_COLUMN, Arc::new(original_codes))?
                            .drop_column(PART_ID_COLUMN)?;
                    }
                }
                QuantizationType::Rabit => {
                    for batch in part_batches.iter_mut() {
                        if batch.num_rows() == 0 {
                            continue;
                        }

                        let codes = batch[RABIT_CODE_COLUMN].as_fixed_size_list();
                        let original_codes = unpack_codes(codes);
                        *batch = batch
                            .replace_column_by_name(RABIT_CODE_COLUMN, Arc::new(original_codes))?
                            .drop_column(PART_ID_COLUMN)?;
                    }
                }
                _ => {}
            }

            batches.extend(part_batches);
        }

        let mut loss = 0.0;
        // Skip if this partition doesn't exist in the reader
        // This can happen after a split creates a new partition
        if let Some(reader) = reader
            && reader.partition_size(part_id)? > 0
        {
            let mut partition_data =
                reader
                    .read_partition(part_id)
                    .await?
                    .ok_or(Error::invalid_input(format!(
                        "partition {} is empty",
                        part_id
                    )))?;
            while let Some(batch) = partition_data.try_next().await? {
                loss += batch
                    .metadata()
                    .get(LOSS_METADATA_KEY)
                    .map(|s| s.parse::<f64>().unwrap_or(0.0))
                    .unwrap_or(0.0);
                batches.push(batch.drop_column(PART_ID_COLUMN)?);
            }
        }

        Ok((batches, loss))
    }

    #[instrument(name = "merge_partitions", level = "debug", skip_all)]
    async fn merge_partitions(
        &mut self,
        mut build_stream: BuildStream<S, Q>,
    ) -> Result<Vec<IndexFile>> {
        let Some(ivf) = self.ivf.as_ref() else {
            return Err(Error::invalid_input("IVF not set before merge partitions"));
        };
        let Some(quantizer) = self.quantizer.clone() else {
            return Err(Error::invalid_input(
                "quantizer not set before merge partitions",
            ));
        };

        let quantization_type = Q::quantization_type();
        let is_pq = quantization_type == QuantizationType::Product;
        let is_rq = quantization_type == QuantizationType::Rabit;
        let is_flat = quantization_type == QuantizationType::Flat;

        // prepare the final writers
        let storage_path = self.index_dir.clone().join(INDEX_AUXILIARY_FILE_NAME);
        let index_path = self.index_dir.clone().join(INDEX_FILE_NAME);

        let file_version = ConcreteFileVersion::from(self.format_version);
        let writer_options = FileWriterOptions::default();
        let mut storage_writer = if is_flat {
            None
        } else {
            let mut fields = vec![ROW_ID_FIELD.clone(), quantizer.field()];
            fields.extend(quantizer.extra_fields());
            let storage_schema: Schema = (&arrow_schema::Schema::new(fields)).try_into()?;
            Some(versions::create_writer(
                file_version,
                self.store.create(&storage_path).await?,
                storage_schema,
                writer_options.clone(),
            )?)
        };
        let mut index_writer = versions::create_writer(
            file_version,
            self.store.create(&index_path).await?,
            S::schema().as_ref().try_into()?,
            writer_options.clone(),
        )?;

        // maintain the IVF partitions
        let mut storage_ivf = IvfModel::empty();
        let mut index_ivf = IvfModel::new(ivf.centroids.clone().unwrap(), ivf.loss);
        let mut partition_index_metadata = Vec::with_capacity(ivf.num_partitions());

        let mut part_id = 0;
        let mut total_loss = 0.0;
        let progress = self.progress.clone();
        log::info!("merging {} partitions", ivf.num_partitions());
        while let Some(part) = build_stream.try_next().await? {
            part_id += 1;
            progress.stage_progress("merge_partitions", part_id).await?;
            let Some((storage, index, loss)) = part else {
                log::warn!("partition {} is empty, skipping", part_id);

                storage_ivf.add_partition(0);
                index_ivf.add_partition(0);
                partition_index_metadata.push(String::new());

                continue;
            };
            total_loss += loss;

            if storage.len() == 0 {
                storage_ivf.add_partition(0);
            } else {
                for mut batch in storage.to_batches()? {
                    if is_pq
                        && !self.transpose_codes
                        && batch.num_rows() > 0
                        && batch.column_by_name(PQ_CODE_COLUMN).is_some()
                    {
                        let codes_fsl = batch
                            .column_by_name(PQ_CODE_COLUMN)
                            .unwrap()
                            .as_fixed_size_list();
                        let num_rows = batch.num_rows();
                        let bytes_per_code = codes_fsl.value_length() as usize;
                        let codes = codes_fsl.values().as_primitive::<datatypes::UInt8Type>();
                        let original_codes = transpose(codes, bytes_per_code, num_rows);
                        let original_fsl = Arc::new(FixedSizeListArray::try_new_from_values(
                            original_codes,
                            bytes_per_code as i32,
                        )?);
                        batch = batch.replace_column_by_name(PQ_CODE_COLUMN, original_fsl)?;
                    }

                    if is_rq
                        && !self.transpose_codes
                        && batch.num_rows() > 0
                        && batch.column_by_name(RABIT_CODE_COLUMN).is_some()
                    {
                        let codes_fsl = batch
                            .column_by_name(RABIT_CODE_COLUMN)
                            .unwrap()
                            .as_fixed_size_list();
                        let unpacked = Arc::new(unpack_codes(codes_fsl));
                        batch = batch.replace_column_by_name(RABIT_CODE_COLUMN, unpacked)?;
                    }

                    if storage_writer.is_none() {
                        let storage_schema: Schema = batch.schema_ref().as_ref().try_into()?;
                        storage_writer = Some(versions::create_writer(
                            file_version,
                            self.store.create(&storage_path).await?,
                            storage_schema,
                            writer_options.clone(),
                        )?);
                    }
                    storage_writer
                        .as_mut()
                        .expect("storage writer must be initialized before write")
                        .write_batch(&batch)
                        .await?;
                    storage_ivf.add_partition(batch.num_rows() as u32);
                }
            }

            let index_batch = index.to_batch()?;
            if index_batch.num_rows() == 0 {
                index_ivf.add_partition(0);
                partition_index_metadata.push(String::new());
            } else {
                index_writer.write_batch(&index_batch).await?;
                index_ivf.add_partition(index_batch.num_rows() as u32);
                partition_index_metadata.push(
                    index_batch
                        .schema()
                        .metadata
                        .get(S::metadata_key())
                        .cloned()
                        .unwrap_or_default(),
                );
            }
        }

        match self.shuffle_reader.as_ref() {
            Some(reader) => {
                // it's building index, the loss is already calculated in the shuffle reader
                if let Some(loss) = reader.total_loss() {
                    total_loss += loss;
                }
                index_ivf.loss = Some(total_loss);
            }
            None => {
                // it's remapping, we don't need to change the loss
            }
        }

        if storage_writer.is_none() {
            let Some(centroids) = ivf.centroids.as_ref() else {
                return Err(Error::invalid_input(
                    "flat storage writer could not infer schema from empty partitions without IVF centroids",
                ));
            };
            let flat_schema = arrow_schema::Schema::new(vec![
                ROW_ID_FIELD.as_ref().clone(),
                arrow_schema::Field::new(
                    lance_index::vector::flat::storage::FLAT_COLUMN,
                    DataType::FixedSizeList(
                        Arc::new(arrow_schema::Field::new(
                            "item",
                            centroids.value_type(),
                            true,
                        )),
                        centroids.value_length(),
                    ),
                    true,
                ),
            ]);
            let storage_schema: Schema = (&flat_schema).try_into()?;
            storage_writer = Some(versions::create_writer(
                file_version,
                self.store.create(&storage_path).await?,
                storage_schema,
                writer_options.clone(),
            )?);
        }

        let storage_writer = storage_writer
            .as_mut()
            .expect("storage writer must be initialized before final metadata write");
        let storage_ivf_pb = pb::Ivf::try_from(&storage_ivf)?;
        storage_writer.add_schema_metadata(DISTANCE_TYPE_KEY, self.distance_type.to_string());
        let ivf_buffer_pos = storage_writer
            .add_global_buffer(storage_ivf_pb.encode_to_vec().into())
            .await?;
        storage_writer.add_schema_metadata(IVF_METADATA_KEY, ivf_buffer_pos.to_string());
        let transposed = match quantization_type {
            QuantizationType::Product | QuantizationType::Rabit => self.transpose_codes,
            _ => false,
        };
        // For now, each partition's metadata is just the quantizer,
        // it's all the same for now, so we just take the first one
        let mut metadata = quantizer.metadata(Some(QuantizationMetadata {
            codebook_position: Some(0),
            codebook: None,
            transposed,
        }));
        if let Some(extra_metadata) = metadata.extra_metadata()? {
            let idx = storage_writer.add_global_buffer(extra_metadata).await?;
            metadata.set_buffer_index(idx);
        }
        let metadata = serde_json::to_string(&metadata)?;
        let storage_partition_metadata = vec![metadata];
        storage_writer.add_schema_metadata(
            STORAGE_METADATA_KEY,
            serde_json::to_string(&storage_partition_metadata)?,
        );

        let index_type_str = index_type_string(S::name().try_into()?, Q::quantization_type());
        if let Some(idx_type) = SupportedIvfIndexType::from_index_type_str(&index_type_str) {
            write_unified_ivf_and_index_metadata(
                &mut index_writer,
                &index_ivf,
                self.distance_type,
                idx_type,
            )
            .await?;
        } else {
            // Fallback for index types not covered by SupportedIndexType (e.g. IVF_RQ).
            let index_ivf_pb = pb::Ivf::try_from(&index_ivf)?;
            let index_metadata = IndexMetadata {
                index_type: index_type_str,
                distance_type: self.distance_type.to_string(),
            };
            index_writer.add_schema_metadata(
                INDEX_METADATA_SCHEMA_KEY,
                serde_json::to_string(&index_metadata)?,
            );
            let ivf_buffer_pos = index_writer
                .add_global_buffer(index_ivf_pb.encode_to_vec().into())
                .await?;
            index_writer.add_schema_metadata(IVF_METADATA_KEY, ivf_buffer_pos.to_string());
        }
        index_writer.add_schema_metadata(
            S::metadata_key(),
            serde_json::to_string(&partition_index_metadata)?,
        );

        let storage_summary = storage_writer.finish().await?;
        let index_summary = index_writer.finish().await?;

        log::info!("merging {} partitions done", ivf.num_partitions());

        Ok(vec![
            IndexFile {
                path: INDEX_AUXILIARY_FILE_NAME.to_string(),
                size_bytes: storage_summary.size_bytes,
            },
            IndexFile {
                path: INDEX_FILE_NAME.to_string(),
                size_bytes: index_summary.size_bytes,
            },
        ])
    }

    // take raw vectors from the dataset
    //
    // returns batches of schema | row_id | vector |
    async fn take_vectors(
        dataset: &Dataset,
        column: &str,
        store: &ObjectStore,
        row_ids: &[u64],
    ) -> Result<Vec<RecordBatch>> {
        let projection = Arc::new(dataset.schema().project(&[column])?);
        // arrow uses i32 for index, so we chunk the row ids to avoid large batch causing overflow
        let mut batches = Vec::new();
        let row_ids = dataset.filter_deleted_ids(row_ids).await?;
        for chunk in row_ids.chunks(store.block_size()) {
            let batch = dataset
                .take_rows(chunk, ProjectionRequest::Schema(projection.clone()))
                .await?;
            if batch.num_rows() != chunk.len() {
                return Err(Error::invalid_input(format!(
                    "batch.num_rows() != chunk.len() ({} != {})",
                    batch.num_rows(),
                    chunk.len()
                )));
            }
            let batch = batch.try_with_column(
                ROW_ID_FIELD.clone(),
                Arc::new(UInt64Array::from(chunk.to_vec())),
            )?;
            batches.push(batch);
        }
        Ok(batches)
    }

    // helper to load row ids and vectors for a partition
    async fn load_partition_raw_vectors(
        &self,
        part_idx: usize,
    ) -> Result<Option<(UInt64Array, FixedSizeListArray)>> {
        let Some(dataset) = self.dataset.as_ref() else {
            return Err(Error::invalid_input(
                "dataset not set before split partition",
            ));
        };

        let mut row_ids = self.partition_row_ids(part_idx).await?;
        if !row_ids.is_sorted() {
            row_ids.sort();
        }
        // dedup is needed if it's multivector
        row_ids.dedup();

        let batches = Self::take_vectors(dataset, &self.column, &self.store, &row_ids).await?;
        if batches.is_empty() {
            return Ok(None);
        }
        let batch = arrow::compute::concat_batches(&batches[0].schema(), batches.iter())?;
        // for multivector, we need to flatten the vectors
        let batch = Flatten::new(&self.column).transform(&batch)?;
        // need to retrieve the row ids from the batch because some rows may have been deleted
        let row_ids = batch[ROW_ID].as_primitive::<UInt64Type>().clone();
        let vectors = batch
            .column_by_qualified_name(&self.column)
            .ok_or(Error::invalid_input(format!(
                "vector column {} not found in batch {}",
                self.column,
                batch.schema()
            )))?
            .as_fixed_size_list()
            .clone();
        Ok(Some((row_ids, vectors)))
    }

    // check whether need to split or join partition
    fn check_partition_adjustment(
        ivf: &IvfModel,
        reader: &dyn ShuffleReader,
        existing_indices: &[Arc<dyn VectorIndex>],
    ) -> Result<(Vec<usize>, Option<usize>)> {
        let index_type = IndexType::try_from(
            index_type_string(S::name().try_into()?, Q::quantization_type()).as_str(),
        )?;

        let mut split_partitions = Vec::new();
        let mut join_partition = None;
        let mut min_partition_size = usize::MAX;
        for partition in 0..ivf.num_partitions() {
            let mut num_rows = reader.partition_size(partition)?;
            for index in existing_indices.iter() {
                num_rows += index.partition_size(partition);
            }
            if num_rows > MAX_PARTITION_SIZE_FACTOR * index_type.target_partition_size() {
                split_partitions.push(partition);
            }
            if ivf.num_partitions() > 1
                && num_rows < min_partition_size
                && num_rows < MIN_PARTITION_SIZE_PERCENT * index_type.target_partition_size() / 100
            {
                min_partition_size = num_rows;
                join_partition = Some(partition);
            }
        }

        Ok((split_partitions, join_partition))
    }

    /// Split oversized partitions using a streaming approach.
    ///
    /// 1. Train new centroids by sampling vectors (low memory).
    /// 2. Compute the set of affected partitions (split targets + their neighbors).
    /// 3. Stream raw vectors for affected partitions through the IVF+quantizer
    ///    transform pipeline, writing to temp files via a shuffler.
    ///
    /// The returned `SplitResult` contains a `ShuffleReader` with properly
    /// quantized data for all affected partitions.
    async fn split_partitions_streaming(
        &self,
        split_partitions: &[usize],
        ivf: &IvfModel,
    ) -> Result<SplitResult> {
        let Some(dataset) = self.dataset.as_ref() else {
            return Err(Error::invalid_input(
                "dataset not set before split partition",
            ));
        };

        let (_, element_type) = get_vector_type(dataset.schema(), &self.column)?;
        let (new_centroids, actual_splits) = match element_type {
            DataType::Float16 => {
                self.compute_split_centroids::<Float16Type>(split_partitions, ivf)
                    .await?
            }
            DataType::Float32 => {
                self.compute_split_centroids::<Float32Type>(split_partitions, ivf)
                    .await?
            }
            DataType::Float64 => {
                self.compute_split_centroids::<Float64Type>(split_partitions, ivf)
                    .await?
            }
            DataType::UInt8 => {
                self.compute_split_centroids::<UInt8Type>(split_partitions, ivf)
                    .await?
            }
            dt => {
                return Err(Error::invalid_input(format!(
                    "vectors must be float16, float32, float64 or uint8, but got {:?}",
                    dt
                )));
            }
        };

        if actual_splits.is_empty() {
            let centroids = ivf.centroids_array().unwrap();
            // Create a dummy empty shuffle reader
            let empty_reader = Arc::new(IvfShufflerReader::new(
                Arc::new(self.store.clone()),
                self.temp_dir.clone().join("split_shuffle"),
                vec![0; ivf.num_partitions()],
                0.0,
            ));
            return Ok(SplitResult {
                new_centroids: centroids.clone(),
                affected_partitions: HashSet::new(),
                shuffle_reader: empty_reader,
            });
        }

        // Compute affected partitions: split targets + their neighbors
        let mut affected_partitions = HashSet::new();
        for &part_idx in &actual_splits {
            affected_partitions.insert(part_idx);
            let c0 = ivf.centroid(part_idx).ok_or(Error::invalid_input(format!(
                "centroid not found for partition {part_idx}",
            )))?;
            let (neighbor_ids, _) =
                select_reassign_candidates_impl(self.distance_type, ivf, part_idx, &c0)?;
            for neighbor_id in neighbor_ids.values() {
                affected_partitions.insert(*neighbor_id as usize);
            }
        }
        log::info!(
            "split {} partitions, {} total affected partitions (including neighbors)",
            actual_splits.len(),
            affected_partitions.len(),
        );

        // Stream raw vectors for affected partitions through the IVF+quantizer
        // transform, writing to temp files via a second shuffler.
        let split_shuffle_reader = self
            .reshuffle_partitions(&affected_partitions, &new_centroids)
            .await?;

        Ok(SplitResult {
            new_centroids,
            affected_partitions,
            shuffle_reader: split_shuffle_reader.into(),
        })
    }

    /// Train new centroids for partitions that need splitting.
    /// Returns the full updated centroids array and the list of partition IDs
    /// that were actually split (empty partitions are skipped).
    async fn compute_split_centroids<T: ArrowPrimitiveType>(
        &self,
        split_partitions: &[usize],
        ivf: &IvfModel,
    ) -> Result<(FixedSizeListArray, Vec<usize>)>
    where
        T::Native: Dot + L2 + Normalize,
        PrimitiveArray<T>: From<Vec<T::Native>>,
    {
        let centroids = ivf.centroids_array().unwrap();

        // Train split centroids in parallel (low memory — only samples).
        let trained_centroids = stream::iter(split_partitions.iter().copied())
            .map(|part_idx| async move { self.train_split_centroids::<T>(part_idx).await })
            .buffered(get_num_compute_intensive_cpus())
            .try_collect::<Vec<_>>()
            .await?;

        let mut splits = Vec::new();
        for (&part_idx, centroids_opt) in split_partitions.iter().zip(trained_centroids) {
            let Some((centroid1, centroid2)) = centroids_opt else {
                continue;
            };
            splits.push((part_idx, centroid1, centroid2));
        }

        if splits.is_empty() {
            return Ok((centroids.clone(), vec![]));
        }

        let actual_splits: Vec<usize> = splits.iter().map(|(idx, _, _)| *idx).collect();
        let new_centroids = apply_centroid_splits(centroids, &splits)?;

        Ok((new_centroids, actual_splits))
    }

    /// Stream raw vectors for the given partitions through the IVF+quantizer
    /// transform, writing to temp files. Returns a ShuffleReader for the results.
    async fn reshuffle_partitions(
        &self,
        affected_partitions: &HashSet<usize>,
        new_centroids: &FixedSizeListArray,
    ) -> Result<Box<dyn ShuffleReader>> {
        let Some(dataset) = self.dataset.as_ref() else {
            return Err(Error::invalid_input("dataset not set before reshuffle"));
        };
        let Some(quantizer) = self.quantizer.clone() else {
            return Err(Error::invalid_input("quantizer not set before reshuffle"));
        };

        // Collect all row IDs for affected partitions, dedup, sort.
        let mut all_row_ids = Vec::new();
        for &part_idx in affected_partitions {
            let mut row_ids = self.partition_row_ids(part_idx).await?;
            all_row_ids.append(&mut row_ids);
        }
        all_row_ids.sort();
        all_row_ids.dedup();

        // Stream raw vectors in chunks
        let projection = Arc::new(dataset.schema().project(&[self.column.as_str()])?);
        let row_ids = dataset.filter_deleted_ids(&all_row_ids).await?;
        let block_size = self.store.block_size();
        let column = self.column.clone();

        let dataset_clone = dataset.clone();
        let projection_clone = projection.clone();
        let raw_stream = stream::iter(
            row_ids
                .chunks(block_size)
                .map(|c| c.to_vec())
                .collect::<Vec<_>>(),
        )
        .then(move |chunk| {
            let dataset = dataset_clone.clone();
            let projection = projection_clone.clone();
            let column = column.clone();
            async move {
                let batch = dataset
                    .take_rows(&chunk, ProjectionRequest::Schema(projection))
                    .await?;
                let batch = batch
                    .try_with_column(ROW_ID_FIELD.clone(), Arc::new(UInt64Array::from(chunk)))?;
                // For multivector, flatten
                Flatten::new(&column).transform(&batch)
            }
        })
        .boxed();

        let transformer = Arc::new(
            lance_index::vector::ivf::new_ivf_transformer_with_quantizer(
                new_centroids.clone(),
                self.distance_type,
                &self.column,
                quantizer.into(),
                None,
            )?,
        );

        let mut transformed_stream = Box::pin(
            raw_stream
                .map(move |batch| {
                    let ivf_transformer = transformer.clone();
                    tokio::spawn(async move { ivf_transformer.transform(&batch?) })
                })
                .buffered(get_num_compute_intensive_cpus())
                .map(|x| x.unwrap())
                .peekable(),
        );

        // Peek transformed stream to get schema (includes PART_ID + PQ codes)
        let schema = match transformed_stream.as_mut().peek_mut().await {
            Some(Ok(b)) => b.schema(),
            Some(Err(e)) => return Err(std::mem::replace(e, Error::Stop)),
            None => {
                log::info!("no vectors to reshuffle");
                let empty_reader: Box<dyn ShuffleReader> = Box::new(IvfShufflerReader::new(
                    Arc::new(self.store.clone()),
                    self.temp_dir.clone().join("split_shuffle"),
                    vec![0; new_centroids.len()],
                    0.0,
                ));
                return Ok(empty_reader);
            }
        };

        let transformed_stream =
            Box::new(RecordBatchStreamAdapter::new(schema, transformed_stream));

        let split_shuffle_dir = self.temp_dir.clone().join("split_shuffle");
        let shuffler = create_ivf_shuffler(
            split_shuffle_dir,
            new_centroids.len(),
            self.format_version,
            None,
        );
        shuffler.shuffle(transformed_stream).await
    }

    /// Sample raw vectors from a partition for kmeans training.
    ///
    /// Samples row IDs first, then only loads `sample_size` vectors from disk.
    async fn sample_partition_raw_vectors(
        &self,
        part_idx: usize,
        sample_size: usize,
    ) -> Result<Option<FixedSizeListArray>> {
        let Some(dataset) = self.dataset.as_ref() else {
            return Err(Error::invalid_input(
                "dataset not set before sample partition",
            ));
        };

        let mut row_ids = self.partition_row_ids(part_idx).await?;
        if !row_ids.is_sorted() {
            row_ids.sort();
        }
        row_ids.dedup();

        if row_ids.is_empty() {
            return Ok(None);
        }

        // Sample row IDs with stride before loading any vectors
        if row_ids.len() > sample_size {
            let stride = row_ids.len() / sample_size;
            row_ids = (0..sample_size).map(|i| row_ids[i * stride]).collect();
        }

        let batches = Self::take_vectors(dataset, &self.column, &self.store, &row_ids).await?;
        if batches.is_empty() {
            return Ok(None);
        }
        let batch = arrow::compute::concat_batches(&batches[0].schema(), batches.iter())?;
        let batch = Flatten::new(&self.column).transform(&batch)?;
        let vectors = batch
            .column_by_qualified_name(&self.column)
            .ok_or(Error::invalid_input(format!(
                "vector column {} not found in batch",
                self.column,
            )))?
            .as_fixed_size_list()
            .clone();
        Ok(Some(vectors))
    }

    /// Train kmeans centroids for splitting a partition. This only needs a
    /// small sample of vectors and is cheap enough to run in parallel.
    async fn train_split_centroids<T: ArrowPrimitiveType>(
        &self,
        part_idx: usize,
    ) -> Result<Option<(ArrayRef, ArrayRef)>>
    where
        T::Native: Dot + L2 + Normalize,
        PrimitiveArray<T>: From<Vec<T::Native>>,
    {
        let Some(vectors) = self
            .sample_partition_raw_vectors(part_idx, SPLIT_SAMPLE_SIZE)
            .await?
        else {
            return Ok(None);
        };

        let dimension = infer_vector_dim(vectors.data_type())?;
        let (normalized_dist_type, normalized_vectors) = match self.distance_type {
            DistanceType::Cosine => {
                let vectors = normalize_fsl(&vectors)?;
                (DistanceType::L2, vectors)
            }
            _ => (self.distance_type, vectors),
        };
        let params = KMeansParams::new(None, 50, 1, normalized_dist_type);
        let kmeans = lance_index::vector::kmeans::train_kmeans::<T>(
            normalized_vectors.values().as_primitive::<T>(),
            params,
            dimension,
            2,
            256,
        )?;

        let centroid1 = kmeans.centroids.slice(0, dimension);
        let centroid2 = kmeans.centroids.slice(dimension, dimension);
        Ok(Some((centroid1, centroid2)))
    }

    // join the given partition:
    // 1. delete the original parttion
    // 2. reasign all vectors of the original partitions
    async fn join_partition(&self, part_idx: usize, ivf: &IvfModel) -> Result<AssignResult> {
        let centroids = ivf.centroids_array().unwrap();
        let mut new_centroids: Vec<ArrayRef> = Vec::with_capacity(ivf.num_partitions() - 1);
        new_centroids.extend(centroids.iter().enumerate().filter_map(|(i, vec)| {
            if i == part_idx {
                None
            } else {
                Some(vec.unwrap())
            }
        }));
        let new_centroids = new_centroids
            .iter()
            .map(|vec| vec.as_ref())
            .collect::<Vec<_>>();
        let new_centroids = arrow::compute::concat(&new_centroids)?;
        let new_centroids =
            FixedSizeListArray::try_new_from_values(new_centroids, centroids.value_length())?;

        // take the raw vectors from dataset
        let Some((row_ids, vectors)) = self.load_partition_raw_vectors(part_idx).await? else {
            return Ok(AssignResult {
                assign_batches: vec![None; ivf.num_partitions() - 1],
                new_centroids,
            });
        };

        match vectors.value_type() {
            DataType::Float16 => {
                self.join_partition_impl::<Float16Type>(
                    part_idx,
                    ivf,
                    &row_ids,
                    &vectors,
                    new_centroids,
                )
                .await
            }
            DataType::Float32 => {
                self.join_partition_impl::<Float32Type>(
                    part_idx,
                    ivf,
                    &row_ids,
                    &vectors,
                    new_centroids,
                )
                .await
            }
            DataType::Float64 => {
                self.join_partition_impl::<Float64Type>(
                    part_idx,
                    ivf,
                    &row_ids,
                    &vectors,
                    new_centroids,
                )
                .await
            }
            DataType::UInt8 => {
                self.join_partition_impl::<UInt8Type>(
                    part_idx,
                    ivf,
                    &row_ids,
                    &vectors,
                    new_centroids,
                )
                .await
            }
            dt => Err(Error::invalid_input(format!(
                "vectors must be float16, float32, float64 or uint8, but got {:?}",
                dt
            ))),
        }
    }

    async fn join_partition_impl<T: ArrowPrimitiveType>(
        &self,
        part_idx: usize,
        ivf: &IvfModel,
        row_ids: &UInt64Array,
        vectors: &FixedSizeListArray,
        new_centroids: FixedSizeListArray,
    ) -> Result<AssignResult>
    where
        T::Native: Dot + L2 + Normalize,
        PrimitiveArray<T>: From<Vec<T::Native>>,
    {
        assert_eq!(row_ids.len(), vectors.len());

        // the original centroid
        let c0 = ivf
            .centroid(part_idx)
            .ok_or(Error::invalid_input("original centroid not found"))?;

        // get top REASSIGN_RANGE centroids from c0
        let (reassign_part_ids, reassign_part_centroids) =
            self.select_reassign_candidates(ivf, part_idx, &c0)?;

        let new_part_id = |idx: usize| -> usize {
            if idx < part_idx {
                idx
            } else {
                // part_idx has been deleted, so any part id after it should be decremented by 1
                idx - 1
            }
        };
        let mut assign_ops = vec![Vec::new(); ivf.num_partitions() - 1];
        // reassign the vectors in the original partition
        for (i, &row_id) in row_ids.values().iter().enumerate() {
            let ReassignPartition::ReassignCandidate(idx) = self.reassign_vectors(
                vectors.value(i).as_primitive::<T>(),
                None,
                &reassign_part_ids,
                &reassign_part_centroids,
            )?
            else {
                log::warn!("this is a bug, the vector is not reassigned");
                continue;
            };

            assign_ops[new_part_id(idx as usize)].push(AssignOp::Add((row_id, vectors.value(i))));
        }
        let assign_batches = self.build_assign_batch::<T>(&new_centroids, &assign_ops)?;

        Ok(AssignResult {
            assign_batches,
            new_centroids,
        })
    }

    // Build the assign batch form assign ops for each partition
    // returns the assign batch and the deleted row ids
    fn build_assign_batch<T: ArrowPrimitiveType>(
        &self,
        centroids: &FixedSizeListArray,
        assign_ops: &[Vec<AssignOp>],
    ) -> Result<Vec<Option<(RecordBatch, UInt64Array)>>> {
        let Some(dataset) = self.dataset.as_ref() else {
            return Err(Error::invalid_input(
                "dataset not set before building assign batch",
            ));
        };
        let Some(quantizer) = self.quantizer.clone() else {
            return Err(Error::invalid_input(
                "quantizer not set before building assign batch",
            ));
        };

        let Some(vector_field) =
            dataset
                .schema()
                .field(&self.column)
                .map(|f| match f.data_type() {
                    DataType::List(inner) | DataType::LargeList(inner) => {
                        Field::new(self.column.as_str(), inner.data_type().clone(), true)
                    }
                    _ => f.into(),
                })
        else {
            return Err(Error::invalid_input(
                "vector field not found in dataset schema",
            ));
        };

        let transformer = Arc::new(
            lance_index::vector::ivf::new_ivf_transformer_with_quantizer(
                centroids.clone(),
                self.distance_type,
                vector_field.name().as_str(),
                quantizer.into(),
                None,
            )?,
        );

        let num_rows: usize = assign_ops.iter().map(|ops| ops.len()).sum();

        // build the input batch with schema | row_id | vector | part_id |
        let mut row_ids_builder = UInt64Builder::with_capacity(num_rows);
        let mut vector_builder =
            PrimitiveBuilder::<T>::with_capacity(num_rows * centroids.value_length() as usize);
        let mut part_ids_builder = UInt32Builder::with_capacity(num_rows);

        let mut counts = Vec::with_capacity(assign_ops.len());
        for (part_idx, ops) in assign_ops.iter().enumerate() {
            for AssignOp::Add((row_id, vector)) in ops {
                row_ids_builder.append_value(*row_id);
                vector_builder.append_array(vector.as_primitive::<T>());
                part_ids_builder.append_value(part_idx as u32);
            }
            counts.push(ops.len());
        }

        let row_ids = row_ids_builder.finish();
        let vector = FixedSizeListArray::try_new_from_values(
            vector_builder.finish(),
            centroids.value_length(),
        )?;
        let part_ids = part_ids_builder.finish();
        let schema = arrow_schema::Schema::new(vec![
            ROW_ID_FIELD.clone(),
            vector_field,
            PART_ID_FIELD.clone(),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(row_ids), Arc::new(vector), Arc::new(part_ids)],
        )?;
        let batch = transformer.transform(&batch)?;

        let empty_deleted = UInt64Array::from(Vec::<u64>::new());
        let mut results = Vec::with_capacity(assign_ops.len());
        let mut offset = 0;
        for count in counts {
            if count == 0 {
                results.push(None);
            } else {
                results.push(Some((batch.slice(offset, count), empty_deleted.clone())));
                offset += count;
            }
        }
        Ok(results)
    }

    async fn partition_row_ids(&self, part_idx: usize) -> Result<Vec<u64>> {
        // existing part: read from the existing indices
        let mut row_ids = Vec::new();
        for index in self.existing_indices.iter() {
            if part_idx >= index.ivf_model().num_partitions() {
                // there was a bug that may cause delta indices have different number of partitions,
                // it's safe to skip loading the extra partition, and split/join the existing partitions,
                // split/join would merge all delta indices into one so it would fix the issue
                // see https://github.com/lance-format/lance/issues/5312
                log::warn!(
                    "partition index is {} but the number of partitions is {}, skip loading it",
                    part_idx,
                    index.ivf_model().num_partitions()
                );
                continue;
            }
            let mut reader = index
                .partition_reader(part_idx, false, &NoOpMetricsCollector)
                .await?;
            while let Some(batch) = reader.try_next().await? {
                row_ids.extend(batch[ROW_ID].as_primitive::<UInt64Type>().values());
            }
        }

        // incremental part: read from the shuffler reader
        if let Some(reader) = self.shuffle_reader.as_ref() {
            // TODO: don't read vectors here, just read row ids
            if let Some(mut reader) = reader.read_partition(part_idx).await? {
                while let Some(batch) = reader.try_next().await? {
                    row_ids.extend(batch[ROW_ID].as_primitive::<UInt64Type>().values());
                }
            }
        }
        Ok(row_ids)
    }

    // returns the closest REASSIGN_RANGE partitions (indices and centroids) from c0
    fn select_reassign_candidates(
        &self,
        ivf: &IvfModel,
        part_idx: usize,
        c0: &ArrayRef,
    ) -> Result<(UInt32Array, FixedSizeListArray)> {
        select_reassign_candidates_impl(self.distance_type, ivf, part_idx, c0)
    }
    // assign a vector to the closest partition among:
    // 1. the 2 new centroids
    // 2. the closest REASSIGN_RANGE partitions from the original centroid
    fn reassign_vectors<T: ArrowPrimitiveType>(
        &self,
        vector: &PrimitiveArray<T>,
        // the dists to the 2 new centroids
        split_centroids_dists: Option<(f32, f32)>,
        reassign_candidate_ids: &UInt32Array,
        reassign_candidate_centroids: &FixedSizeListArray,
    ) -> Result<ReassignPartition> {
        Self::reassign_vectors_impl(
            self.distance_type,
            vector,
            split_centroids_dists,
            reassign_candidate_ids,
            reassign_candidate_centroids,
        )
    }

    fn reassign_vectors_impl<T: ArrowPrimitiveType>(
        distance_type: DistanceType,
        vector: &PrimitiveArray<T>,
        split_centroids_dists: Option<(f32, f32)>,
        reassign_candidate_ids: &UInt32Array,
        reassign_candidate_centroids: &FixedSizeListArray,
    ) -> Result<ReassignPartition> {
        let dists = distance_type.arrow_batch_func()(vector, reassign_candidate_centroids)?;
        let min_dist_idx = dists
            .values()
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| a.total_cmp(b))
            .map(|(i, _)| i);
        let min_dist = min_dist_idx
            .map(|idx| dists.value(idx))
            .unwrap_or(f32::INFINITY);
        match split_centroids_dists {
            Some((d1, d2)) => {
                if min_dist <= d1 && min_dist <= d2 {
                    Ok(ReassignPartition::ReassignCandidate(
                        reassign_candidate_ids.value(min_dist_idx.unwrap()),
                    ))
                } else if d1 <= d2 {
                    Ok(ReassignPartition::NewCentroid1)
                } else {
                    Ok(ReassignPartition::NewCentroid2)
                }
            }
            None => Ok(ReassignPartition::ReassignCandidate(
                reassign_candidate_ids.value(min_dist_idx.unwrap()),
            )),
        }
    }
}

fn select_reassign_candidates_impl(
    distance_type: DistanceType,
    ivf: &IvfModel,
    part_idx: usize,
    c0: &ArrayRef,
) -> Result<(UInt32Array, FixedSizeListArray)> {
    let reassign_range = std::cmp::min(REASSIGN_RANGE + 1, ivf.num_partitions());
    let centroids = ivf.centroids_array().unwrap();
    let centroid_dists = distance_type.arrow_batch_func()(&c0, centroids)?;
    let reassign_range_candidates =
        sort_to_indices(centroid_dists.as_ref(), None, Some(reassign_range))?;
    let selection_len = reassign_range.saturating_sub(1);
    let filtered_ids = reassign_range_candidates
        .values()
        .iter()
        .copied()
        .filter(|&idx| idx as usize != part_idx)
        .take(selection_len)
        .collect::<Vec<_>>();
    let reassign_candidate_ids = UInt32Array::from(filtered_ids);
    let reassign_candidate_centroids =
        arrow::compute::take(centroids, &reassign_candidate_ids, None)?;
    Ok((
        reassign_candidate_ids,
        reassign_candidate_centroids.as_fixed_size_list().clone(),
    ))
}

struct AssignResult {
    assign_batches: Vec<Option<(RecordBatch, UInt64Array)>>,
    new_centroids: FixedSizeListArray,
}

struct SplitResult {
    new_centroids: FixedSizeListArray,
    affected_partitions: HashSet<usize>,
    shuffle_reader: Arc<dyn ShuffleReader>,
}

#[derive(Debug, Clone)]
enum AssignOp {
    Add((u64, ArrayRef)),
}

#[derive(Debug, Copy, Clone)]
enum ReassignPartition {
    NewCentroid1,
    NewCentroid2,
    ReassignCandidate(u32),
}

enum PartitionAdjustment {
    /// Split partitions. Carries the set of all affected partitions (split
    /// targets + neighbors) and a shuffle reader with re-quantized data.
    Split {
        affected_partitions: HashSet<usize>,
        split_shuffle_reader: Arc<dyn ShuffleReader>,
    },
    /// Join partition at given id
    Join(usize),
}

impl std::fmt::Debug for PartitionAdjustment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Split {
                affected_partitions,
                ..
            } => f
                .debug_struct("Split")
                .field("affected_partitions", affected_partitions)
                .finish(),
            Self::Join(id) => f.debug_tuple("Join").field(id).finish(),
        }
    }
}

pub(crate) fn index_type_string(sub_index: SubIndexType, quantizer: QuantizationType) -> String {
    // FlatBin is a QuantizationType variant used internally for reconstruction,
    // but the persisted index type string uses "FLAT" (differentiated by DataType).
    let quantizer = match quantizer {
        QuantizationType::FlatBin => QuantizationType::Flat,
        other => other,
    };
    match (sub_index, quantizer) {
        // ignore FLAT sub index,
        // IVF_FLAT_FLAT => IVF_FLAT
        // IVF_FLAT_PQ => IVF_PQ
        (SubIndexType::Flat, quantization_type) => format!("IVF_{}", quantization_type),
        (sub_index_type, quantization_type) => {
            if sub_index_type.to_string() == quantization_type.to_string() {
                // ignore redundant quantization type
                // e.g. IVF_PQ_PQ should be IVF_PQ
                format!("IVF_{}", sub_index_type)
            } else {
                format!("IVF_{}_{}", sub_index_type, quantization_type)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Float32Array, NullArray};
    use lance_index::vector::flat::index::{FlatIndex, FlatQuantizer};

    struct SingleBatchReader {
        batch: RecordBatch,
        partition_id: usize,
    }

    #[async_trait::async_trait]
    impl ShuffleReader for SingleBatchReader {
        async fn read_partition(
            &self,
            partition_id: usize,
        ) -> Result<Option<Box<dyn RecordBatchStream + Unpin + 'static>>> {
            if partition_id != self.partition_id || self.batch.num_rows() == 0 {
                return Ok(None);
            }

            let schema = self.batch.schema();
            let stream = stream::iter(vec![Ok(self.batch.clone())]);
            Ok(Some(Box::new(RecordBatchStreamAdapter::new(
                schema, stream,
            ))))
        }

        fn partition_size(&self, partition_id: usize) -> Result<usize> {
            Ok(if partition_id == self.partition_id {
                self.batch.num_rows()
            } else {
                0
            })
        }

        fn total_loss(&self) -> Option<f64> {
            None
        }
    }

    // Helper to read centroid i from a FixedSizeListArray as a Vec<f32>
    fn centroid_values(arr: &FixedSizeListArray, i: usize) -> Vec<f32> {
        arr.value(i).as_primitive::<Float32Type>().values().to_vec()
    }

    #[test]
    fn apply_centroid_splits_correct_count_and_ordering() {
        // 4 original centroids at [0,0], [1,1], [2,2], [3,3].
        // Split partitions 1 and 3; verify that:
        //   - result has 6 centroids (4 original + 2 splits)
        //   - unchanged partition indices 0 and 2 keep their original values
        //   - split partitions 1 and 3 have centroid1 at their original index
        //   - centroid2 for each split is appended at the end (indices 4, 5)
        let original = FixedSizeListArray::try_new_from_values(
            Float32Array::from(vec![0.0_f32, 0.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0]),
            2,
        )
        .unwrap();

        let c1_for_1: ArrayRef = Arc::new(Float32Array::from(vec![1.1_f32, 1.1]));
        let c2_for_1: ArrayRef = Arc::new(Float32Array::from(vec![0.9_f32, 0.9]));
        let c1_for_3: ArrayRef = Arc::new(Float32Array::from(vec![3.1_f32, 3.1]));
        let c2_for_3: ArrayRef = Arc::new(Float32Array::from(vec![2.9_f32, 2.9]));

        let splits = vec![(1_usize, c1_for_1, c2_for_1), (3_usize, c1_for_3, c2_for_3)];
        let result = apply_centroid_splits(&original, &splits).unwrap();

        assert_eq!(result.len(), 6);
        // Unchanged partitions
        assert_eq!(centroid_values(&result, 0), [0.0, 0.0]);
        assert_eq!(centroid_values(&result, 2), [2.0, 2.0]);
        // Replaced centroids (centroid1 for each split partition)
        assert_eq!(centroid_values(&result, 1), [1.1, 1.1]);
        assert_eq!(centroid_values(&result, 3), [3.1, 3.1]);
        // Appended centroid2s, in split order
        assert_eq!(centroid_values(&result, 4), [0.9, 0.9]);
        assert_eq!(centroid_values(&result, 5), [2.9, 2.9]);
    }

    #[test]
    fn select_reassign_candidates_skips_deleted_partition() {
        let dim = 4;
        let centroid_values = Float32Array::from(vec![0.0_f32; dim * 2]);
        let centroids =
            FixedSizeListArray::try_new_from_values(centroid_values, dim as i32).unwrap();
        let mut ivf = IvfModel::new(centroids, None);
        ivf.lengths = vec![10, 20];
        ivf.offsets = vec![0, 10];

        let c0 = ivf.centroid(1).unwrap();
        let (reassign_ids, reassign_centroids) =
            select_reassign_candidates_impl(DistanceType::L2, &ivf, 1, &c0).unwrap();

        assert_eq!(reassign_ids.len(), 1);
        assert_eq!(reassign_ids.value(0), 0);
        assert_eq!(reassign_centroids.len(), 1);

        let expected_centroid = ivf.centroid(0).unwrap();
        assert_eq!(
            reassign_centroids
                .value(0)
                .as_primitive::<Float32Type>()
                .values(),
            expected_centroid.as_primitive::<Float32Type>().values()
        );
    }

    #[tokio::test]
    async fn optimize_split_after_append_pushes_partition_over_threshold() {
        use crate::dataset::{InsertBuilder, WriteMode, WriteParams};
        use crate::index::vector::VectorIndexParams;
        use crate::index::{DatasetIndexExt, DatasetIndexInternalExt};
        use arrow_array::RecordBatchIterator;
        use arrow_schema::Schema as ArrowSchema;
        use lance_index::optimize::OptimizeOptions;
        use lance_linalg::distance::MetricType;

        let item_field = Arc::new(Field::new("item", DataType::Float32, true));
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "vec",
            DataType::FixedSizeList(item_field, 4),
            false,
        )]));

        // Tiny per-row perturbation gives kmeans something to fit while keeping
        // within-cluster variance negligible relative to the 1000-unit cluster
        // separation, so kmeans (k=2) reliably places one centroid per cluster.
        let make_batch = |num_rows: usize, center: f32| -> RecordBatch {
            let mut values = Vec::with_capacity(num_rows * 4);
            for i in 0..num_rows {
                let p = center + (i as f32) * 0.0001;
                values.extend_from_slice(&[p, p, p, p]);
            }
            let fsl =
                FixedSizeListArray::try_new_from_values(Float32Array::from(values), 4).unwrap();
            RecordBatch::try_new(schema.clone(), vec![Arc::new(fsl)]).unwrap()
        };

        let tmp = tempfile::tempdir().unwrap();
        let uri = tmp.path().to_str().unwrap();

        // 15k near origin (just under split threshold of 4 * 4096 = 16384) plus
        // 1.5k far away in cluster B.
        let initial = vec![Ok(make_batch(15_000, 0.0)), Ok(make_batch(1_500, 1000.0))];
        let reader = RecordBatchIterator::new(initial, schema.clone());
        let mut dataset = crate::Dataset::write(reader, uri, None).await.unwrap();

        let params = VectorIndexParams::ivf_flat(2, MetricType::L2);
        dataset
            .create_index(
                &["vec"],
                IndexType::Vector,
                Some("idx".into()),
                &params,
                false,
            )
            .await
            .unwrap();

        let indices = dataset.load_indices_by_name("idx").await.unwrap();
        let initial_index = dataset
            .open_vector_index("vec", &indices[0].uuid, &NoOpMetricsCollector)
            .await
            .unwrap();
        let initial_ivf = initial_index.ivf_model();
        assert_eq!(initial_ivf.num_partitions(), 2);
        let max_initial = (0..2).map(|p| initial_ivf.partition_size(p)).max().unwrap();
        assert!(
            max_initial <= 16_384,
            "initial max partition size {max_initial} should be at or under split threshold",
        );

        // Append 3k more cluster-A rows so partition 0 grows past the threshold.
        let append = make_batch(3_000, 0.0);
        let mut dataset = InsertBuilder::new(Arc::new(dataset))
            .with_params(&WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            })
            .execute(vec![append])
            .await
            .unwrap();

        dataset
            .optimize_indices(&OptimizeOptions::default())
            .await
            .unwrap();

        let indices = dataset.load_indices_by_name("idx").await.unwrap();
        assert_eq!(indices.len(), 1, "expected merge-all on split");
        let optimized = dataset
            .open_vector_index("vec", &indices[0].uuid, &NoOpMetricsCollector)
            .await
            .unwrap();
        let ivf = optimized.ivf_model();

        assert_eq!(
            ivf.num_partitions(),
            3,
            "expected one split: 2 original + 1 new = 3 partitions",
        );
        let total_rows: usize = (0..ivf.num_partitions())
            .map(|p| optimized.partition_size(p))
            .sum();
        assert_eq!(total_rows, 19_500, "all vectors preserved across split");
    }

    #[tokio::test]
    async fn take_partition_batches_preserves_partition_order_for_large_fixed_size_list() {
        let value_length = 1_073_741_824i32;
        let num_rows = 5usize;
        let row_ids = UInt64Array::from(vec![4_u64, 3, 2, 1, 0]);
        let part_ids = UInt32Array::from(vec![0_u32; num_rows]);
        let values = Arc::new(NullArray::new(num_rows * value_length as usize));
        let item_field = Arc::new(Field::new("item", DataType::Null, true));
        let codes = FixedSizeListArray::try_new(item_field, value_length, values, None).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![
                ROW_ID_FIELD.clone(),
                PART_ID_FIELD.clone(),
                Field::new(PQ_CODE_COLUMN, codes.data_type().clone(), true),
            ])),
            vec![Arc::new(row_ids), Arc::new(part_ids), Arc::new(codes)],
        )
        .unwrap();
        let reader = SingleBatchReader {
            batch,
            partition_id: 0,
        };

        let (batches, loss) = IvfIndexBuilder::<FlatIndex, FlatQuantizer>::take_partition_batches(
            0,
            &[],
            Some(&reader),
        )
        .await
        .unwrap();

        assert_eq!(loss, 0.0);
        assert_eq!(batches.len(), 1);
        assert!(batches[0].column_by_name(PART_ID_COLUMN).is_none());
        let row_ids = batches[0][ROW_ID].as_primitive::<UInt64Type>();
        assert_eq!(row_ids.values(), &[4, 3, 2, 1, 0]);
    }
}
