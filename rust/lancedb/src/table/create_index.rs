// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Index creation helpers for [`NativeTable`].
//!
//! This module contains methods for building index parameters, validating
//! index types, and resolving index fields. These are used by the
//! [`BaseTable::create_index`](super::BaseTable::create_index) implementation
//! on `NativeTable`.

use arrow_schema::{DataType, Field};
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance::index::vector::utils::infer_vector_dim;
use lance_index::IndexType;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_index::vector::bq::RQBuildParams;
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::PQBuildParams;
use lance_index::vector::sq::builder::SQBuildParams;

use crate::error::{Error, Result};
use crate::index::Index;
use crate::index::vector::{VectorIndex, suggested_num_sub_vectors};
use crate::utils::{
    supported_bitmap_data_type, supported_btree_data_type, supported_fm_data_type,
    supported_fts_data_type, supported_label_list_data_type, supported_vector_data_type,
};

use super::NativeTable;

impl NativeTable {
    pub async fn load_indices(&self) -> Result<Vec<VectorIndex>> {
        let dataset = self.dataset.get().await?;
        let mf = dataset.manifest();
        let indices = dataset.load_indices().await?;
        Ok(indices
            .iter()
            .map(|i| VectorIndex::new_from_format(mf, i))
            .collect())
    }

    // Helper to validate index type compatibility with field data type
    pub(super) fn validate_index_type(
        field: &Field,
        index_name: &str,
        supported_fn: impl Fn(&DataType) -> bool,
    ) -> Result<()> {
        if !supported_fn(field.data_type()) {
            return Err(Error::Schema {
                message: format!(
                    "A {} index cannot be created on the field `{}` which has data type {}",
                    index_name,
                    field.name(),
                    field.data_type()
                ),
            });
        }
        Ok(())
    }

    // Helper to build IVF params honoring table options.
    pub(super) fn build_ivf_params(
        num_partitions: Option<u32>,
        target_partition_size: Option<u32>,
        sample_rate: u32,
        max_iterations: u32,
    ) -> IvfBuildParams {
        let mut ivf_params = match (num_partitions, target_partition_size) {
            (Some(num_partitions), _) => IvfBuildParams::new(num_partitions as usize),
            (None, Some(target_partition_size)) => {
                IvfBuildParams::with_target_partition_size(target_partition_size as usize)
            }
            (None, None) => IvfBuildParams::default(),
        };
        ivf_params.sample_rate = sample_rate as usize;
        ivf_params.max_iters = max_iterations as usize;
        ivf_params
    }

    // Helper to get num_sub_vectors with default calculation
    pub(super) fn get_num_sub_vectors(
        provided: Option<u32>,
        dim: u32,
        num_bits: Option<u32>,
    ) -> u32 {
        if let Some(provided) = provided {
            return provided;
        }
        let suggested = suggested_num_sub_vectors(dim);
        if num_bits.is_some_and(|num_bits| num_bits == 4) && !suggested.is_multiple_of(2) {
            // num_sub_vectors must be even when 4 bits are used
            suggested + 1
        } else {
            suggested
        }
    }

    // Helper to extract vector dimension from field
    pub(super) fn get_vector_dimension(field: &Field) -> Result<u32> {
        match field.data_type() {
            arrow_schema::DataType::FixedSizeList(_, n) => Ok(*n as u32),
            _ => Ok(infer_vector_dim(field.data_type())? as u32),
        }
    }

    pub(super) fn resolve_index_field(
        schema: &lance_core::datatypes::Schema,
        column: &str,
    ) -> Result<(String, Field)> {
        lance_core::datatypes::parse_field_path(column).map_err(|e| Error::InvalidInput {
            message: format!("Invalid field path `{}`: {}", column, e),
        })?;

        let field_path = schema
            .resolve_case_insensitive(column)
            .ok_or_else(|| Error::Schema {
                message: format!(
                    "Field path `{}` not found in schema. Available field paths: {}",
                    column,
                    schema.field_paths().join(", ")
                ),
            })?;
        let field = field_path.last().expect("field path should be non-empty");
        let path_segments = field_path
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();
        let canonical_path = lance_core::datatypes::format_field_path(&path_segments);

        Ok((canonical_path, Field::from(*field)))
    }

    // Convert LanceDB Index to Lance IndexParams
    pub(super) async fn make_index_params(
        &self,
        field: &Field,
        index_opts: Index,
    ) -> Result<Box<dyn lance::index::IndexParams>> {
        match index_opts {
            Index::Auto => {
                if supported_vector_data_type(field.data_type()) {
                    // Use IvfPq as the default for auto vector indices
                    let dim = Self::get_vector_dimension(field)?;
                    let ivf_params = lance_index::vector::ivf::IvfBuildParams::default();
                    let num_sub_vectors = Self::get_num_sub_vectors(None, dim, None);
                    let pq_params =
                        lance_index::vector::pq::PQBuildParams::new(num_sub_vectors as usize, 8);
                    let lance_idx_params =
                        lance::index::vector::VectorIndexParams::with_ivf_pq_params(
                            lance_linalg::distance::MetricType::L2,
                            ivf_params,
                            pq_params,
                        );
                    Ok(Box::new(lance_idx_params))
                } else if supported_btree_data_type(field.data_type()) {
                    Ok(Box::new(ScalarIndexParams::for_builtin(
                        BuiltinIndexType::BTree,
                    )))
                } else {
                    Err(Error::InvalidInput {
                        message: format!(
                            "there are no indices supported for the field `{}` with the data type {}",
                            field.name(),
                            field.data_type()
                        ),
                    })?
                }
            }
            Index::BTree(_) => {
                Self::validate_index_type(field, "BTree", supported_btree_data_type)?;
                Ok(Box::new(ScalarIndexParams::for_builtin(
                    BuiltinIndexType::BTree,
                )))
            }
            Index::Bitmap(_) => {
                Self::validate_index_type(field, "Bitmap", supported_bitmap_data_type)?;
                Ok(Box::new(ScalarIndexParams::for_builtin(
                    BuiltinIndexType::Bitmap,
                )))
            }
            Index::LabelList(_) => {
                Self::validate_index_type(field, "LabelList", supported_label_list_data_type)?;
                Ok(Box::new(ScalarIndexParams::for_builtin(
                    BuiltinIndexType::LabelList,
                )))
            }
            Index::Fm(_) => {
                Self::validate_index_type(field, "FM", supported_fm_data_type)?;
                Ok(Box::new(ScalarIndexParams::for_builtin(
                    BuiltinIndexType::Fm,
                )))
            }
            Index::FTS(fts_opts) => {
                Self::validate_index_type(field, "FTS", supported_fts_data_type)?;
                Ok(Box::new(fts_opts))
            }
            Index::IvfFlat(index) => {
                Self::validate_index_type(field, "IVF Flat", supported_vector_data_type)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let lance_idx_params =
                    VectorIndexParams::with_ivf_flat_params(index.distance_type.into(), ivf_params);
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfSq(index) => {
                Self::validate_index_type(field, "IVF SQ", supported_vector_data_type)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let sq_params = SQBuildParams {
                    sample_rate: index.sample_rate as usize,
                    ..Default::default()
                };
                let lance_idx_params = VectorIndexParams::with_ivf_sq_params(
                    index.distance_type.into(),
                    ivf_params,
                    sq_params,
                );
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfPq(index) => {
                Self::validate_index_type(field, "IVF PQ", supported_vector_data_type)?;
                let dim = Self::get_vector_dimension(field)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let num_sub_vectors =
                    Self::get_num_sub_vectors(index.num_sub_vectors, dim, index.num_bits);
                let num_bits = index.num_bits.unwrap_or(8) as usize;
                let mut pq_params = PQBuildParams::new(num_sub_vectors as usize, num_bits);
                pq_params.max_iters = index.max_iterations as usize;
                let lance_idx_params = VectorIndexParams::with_ivf_pq_params(
                    index.distance_type.into(),
                    ivf_params,
                    pq_params,
                );
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfRq(index) => {
                Self::validate_index_type(field, "IVF RQ", supported_vector_data_type)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let rq_params = RQBuildParams::new(index.num_bits.unwrap_or(1) as u8);
                let lance_idx_params = VectorIndexParams::with_ivf_rq_params(
                    index.distance_type.into(),
                    ivf_params,
                    rq_params,
                );
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfHnswPq(index) => {
                Self::validate_index_type(field, "IVF HNSW PQ", supported_vector_data_type)?;
                let dim = Self::get_vector_dimension(field)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let num_sub_vectors =
                    Self::get_num_sub_vectors(index.num_sub_vectors, dim, index.num_bits);
                let hnsw_params = HnswBuildParams::default()
                    .num_edges(index.m as usize)
                    .ef_construction(index.ef_construction as usize);
                let pq_params = PQBuildParams::new(
                    num_sub_vectors as usize,
                    index.num_bits.unwrap_or(8) as usize,
                );
                let lance_idx_params = VectorIndexParams::with_ivf_hnsw_pq_params(
                    index.distance_type.into(),
                    ivf_params,
                    hnsw_params,
                    pq_params,
                );
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfHnswSq(index) => {
                Self::validate_index_type(field, "IVF HNSW SQ", supported_vector_data_type)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let hnsw_params = HnswBuildParams::default()
                    .num_edges(index.m as usize)
                    .ef_construction(index.ef_construction as usize);
                let sq_params = SQBuildParams {
                    sample_rate: index.sample_rate as usize,
                    ..Default::default()
                };
                let lance_idx_params = VectorIndexParams::with_ivf_hnsw_sq_params(
                    index.distance_type.into(),
                    ivf_params,
                    hnsw_params,
                    sq_params,
                );
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfHnswFlat(index) => {
                Self::validate_index_type(field, "IVF HNSW FLAT", supported_vector_data_type)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let hnsw_params = HnswBuildParams::default()
                    .num_edges(index.m as usize)
                    .ef_construction(index.ef_construction as usize);
                let lance_idx_params = VectorIndexParams::ivf_hnsw(
                    index.distance_type.into(),
                    ivf_params,
                    hnsw_params,
                );
                Ok(Box::new(lance_idx_params))
            }
        }
    }

    // Helper method to get the correct IndexType based on the Index variant and field data type
    pub(super) fn get_index_type_for_field(&self, field: &Field, index: &Index) -> IndexType {
        match index {
            Index::Auto => {
                if supported_vector_data_type(field.data_type()) {
                    IndexType::Vector
                } else if supported_btree_data_type(field.data_type()) {
                    IndexType::BTree
                } else {
                    // This should not happen since make_index_params would have failed
                    IndexType::BTree
                }
            }
            Index::BTree(_) => IndexType::BTree,
            Index::Bitmap(_) => IndexType::Bitmap,
            Index::LabelList(_) => IndexType::LabelList,
            Index::Fm(_) => IndexType::Fm,
            Index::FTS(_) => IndexType::Inverted,
            Index::IvfFlat(_)
            | Index::IvfSq(_)
            | Index::IvfPq(_)
            | Index::IvfRq(_)
            | Index::IvfHnswPq(_)
            | Index::IvfHnswSq(_)
            | Index::IvfHnswFlat(_) => IndexType::Vector,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_array::builder::{LargeListBuilder, ListBuilder, StringBuilder};
    use arrow_array::record_batch;
    use arrow_array::{
        Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Int32Array,
        LargeBinaryArray, LargeStringArray, RecordBatch, StringArray, StructArray,
    };
    use arrow_data::ArrayDataBuilder;
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use tempfile::tempdir;

    use crate::connect;
    use crate::connection::ConnectBuilder;
    use crate::index::Index;
    use crate::index::scalar::{BTreeIndexBuilder, BitmapIndexBuilder, FmIndexBuilder};
    use crate::index::vector::{
        IvfHnswFlatIndexBuilder, IvfHnswPqIndexBuilder, IvfHnswSqIndexBuilder,
    };
    use crate::query::{ExecutableQuery, QueryBase};
    use crate::table::optimize::{CompactionOptions, OptimizeAction};
    use lance_index::scalar::FullTextSearchQuery;

    fn create_fixed_size_list<T: Array>(
        values: T,
        list_size: i32,
    ) -> crate::error::Result<FixedSizeListArray> {
        let list_type = DataType::FixedSizeList(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            list_size,
        );
        let data = ArrayDataBuilder::new(list_type)
            .len(values.len() / list_size as usize)
            .add_child_data(values.into_data())
            .build()
            .unwrap();

        Ok(FixedSizeListArray::from(data))
    }

    #[tokio::test]
    async fn test_create_index() {
        use std::iter::repeat_with;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        let dimension = 16;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embeddings",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimension,
            ),
            false,
        )]));

        let float_arr = Float32Array::from(
            repeat_with(rand::random::<f32>)
                .take(512 * dimension as usize)
                .collect::<Vec<f32>>(),
        );

        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap();

        let table = conn.create_table("test", batch).execute().await.unwrap();

        assert_eq!(table.index_stats("my_index").await.unwrap(), None);

        table
            .create_index(&["embeddings"], Index::Auto)
            .execute()
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::IvfPq);
        assert_eq!(index.columns, vec!["embeddings".to_string()]);
        assert!(index.index_uuid.is_some());
        assert!(index.type_url.is_some());
        assert_eq!(index.num_segments, Some(1));
        assert_eq!(index.num_indexed_rows, Some(512));
        assert_eq!(index.num_unindexed_rows, Some(0));
        assert!(index.created_at.is_some());
        assert!(index.size_bytes.is_some());
        assert!(index.index_version.is_some());
        assert!(index.index_details.is_some());
        assert_eq!(table.count_rows(None).await.unwrap(), 512);
        assert_eq!(table.name(), "test");

        let indices = table.as_native().unwrap().load_indices().await.unwrap();
        let index_name = &indices[0].index_name;
        let stats = table.index_stats(index_name).await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, 512);
        assert_eq!(stats.num_unindexed_rows, 0);
        assert_eq!(stats.index_type, crate::index::IndexType::IvfPq);
        assert_eq!(stats.distance_type, Some(crate::DistanceType::L2));

        table.drop_index(index_name).await.unwrap();
        assert_eq!(table.list_indices().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_ivf_pq_uses_default_partition_size_for_num_partitions() {
        use crate::index::vector::IvfPqIndexBuilder;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        const PARTITION_SIZE: usize = 8192;
        let num_rows = PARTITION_SIZE * 2;
        let dimension = 8usize;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embeddings",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimension as i32,
            ),
            false,
        )]));

        let float_arr =
            Float32Array::from_iter_values((0..(num_rows * dimension)).map(|v| v as f32));
        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension as i32).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![vectors]).unwrap();

        let table = conn.create_table("test", batch).execute().await.unwrap();
        let native_table = table.as_native().unwrap();
        let builder = IvfPqIndexBuilder::default();
        table
            .create_index(&["embeddings"], Index::IvfPq(builder))
            .execute()
            .await
            .unwrap();
        table
            .wait_for_index(&["embeddings_idx"], std::time::Duration::from_secs(30))
            .await
            .unwrap();

        use lance::index::DatasetIndexInternalExt;
        use lance::index::vector::ivf::v2::IvfPq as LanceIvfPq;
        use lance_index::metrics::NoOpMetricsCollector;
        use lance_index::vector::VectorIndex as LanceVectorIndex;

        let indices = native_table.load_indices().await.unwrap();
        let index_uuid = uuid::Uuid::parse_str(&indices[0].index_uuid).unwrap();

        let dataset_guard = native_table.dataset.get().await.unwrap();
        let dataset = (*dataset_guard).clone();
        drop(dataset_guard);

        let lance_index = dataset
            .open_vector_index("embeddings", &index_uuid, &NoOpMetricsCollector)
            .await
            .unwrap();
        let ivf_index = lance_index
            .as_any()
            .downcast_ref::<LanceIvfPq>()
            .expect("expected IvfPq index");
        let partition_count = ivf_index.ivf_model().num_partitions();

        let expected_partitions = num_rows / PARTITION_SIZE;
        assert_eq!(partition_count, expected_partitions);
    }

    #[tokio::test]
    async fn test_create_index_ivf_hnsw_sq() {
        use std::iter::repeat_with;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        let dimension = 16;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embeddings",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimension,
            ),
            false,
        )]));

        let float_arr = Float32Array::from(
            repeat_with(rand::random::<f32>)
                .take(512 * dimension as usize)
                .collect::<Vec<f32>>(),
        );

        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap();

        let table = conn.create_table("test", batch).execute().await.unwrap();

        let stats = table.index_stats("my_index").await.unwrap();
        assert!(stats.is_none());

        let index = IvfHnswSqIndexBuilder::default();
        table
            .create_index(&["embeddings"], Index::IvfHnswSq(index))
            .execute()
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::IvfHnswSq);
        assert_eq!(index.columns, vec!["embeddings".to_string()]);
        assert_eq!(table.count_rows(None).await.unwrap(), 512);
        assert_eq!(table.name(), "test");

        let indices = table.as_native().unwrap().load_indices().await.unwrap();
        let index_name = &indices[0].index_name;
        let stats = table.index_stats(index_name).await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, 512);
        assert_eq!(stats.num_unindexed_rows, 0);
    }

    #[tokio::test]
    async fn test_create_index_ivf_hnsw_pq() {
        use std::iter::repeat_with;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        let dimension = 16;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embeddings",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimension,
            ),
            false,
        )]));

        let float_arr = Float32Array::from(
            repeat_with(rand::random::<f32>)
                .take(512 * dimension as usize)
                .collect::<Vec<f32>>(),
        );

        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap();

        let table = conn.create_table("test", batch).execute().await.unwrap();
        let stats = table.index_stats("my_index").await.unwrap();
        assert!(stats.is_none());

        let index = IvfHnswPqIndexBuilder::default();
        table
            .create_index(&["embeddings"], Index::IvfHnswPq(index))
            .execute()
            .await
            .unwrap();
        table
            .wait_for_index(&["embeddings_idx"], Duration::from_millis(10))
            .await
            .unwrap();
        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::IvfHnswPq);
        assert_eq!(index.columns, vec!["embeddings".to_string()]);
        assert_eq!(table.count_rows(None).await.unwrap(), 512);
        assert_eq!(table.name(), "test");

        let indices: Vec<crate::index::vector::VectorIndex> =
            table.as_native().unwrap().load_indices().await.unwrap();
        let index_name = &indices[0].index_name;
        let stats = table.index_stats(index_name).await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, 512);
        assert_eq!(stats.num_unindexed_rows, 0);
    }

    #[tokio::test]
    async fn test_create_index_ivf_hnsw_flat() {
        use std::iter::repeat_with;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        let dimension = 16;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embeddings",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimension,
            ),
            false,
        )]));

        let float_arr = Float32Array::from(
            repeat_with(rand::random::<f32>)
                .take(512 * dimension as usize)
                .collect::<Vec<f32>>(),
        );

        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap();

        let table = conn.create_table("test", batch).execute().await.unwrap();

        let index = IvfHnswFlatIndexBuilder::default();
        table
            .create_index(&["embeddings"], Index::IvfHnswFlat(index))
            .execute()
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::IvfHnswFlat);
        assert_eq!(index.columns, vec!["embeddings".to_string()]);
        assert_eq!(table.count_rows(None).await.unwrap(), 512);
    }

    #[tokio::test]
    async fn test_create_scalar_index() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("i", Int32, [1])).unwrap();
        let table = conn
            .create_table("my_table", batch.clone())
            .execute()
            .await
            .unwrap();

        // Can create an index on a scalar column (will default to btree)
        table
            .create_index(&["i"], Index::Auto)
            .execute()
            .await
            .unwrap();
        table
            .wait_for_index(&["i_idx"], Duration::from_millis(10))
            .await
            .unwrap();
        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::BTree);
        assert_eq!(index.columns, vec!["i".to_string()]);

        // Can also specify btree
        table
            .create_index(&["i"], Index::BTree(BTreeIndexBuilder::default()))
            .execute()
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::BTree);
        assert_eq!(index.columns, vec!["i".to_string()]);

        // The richer metadata surfaced from describe_indices should be populated.
        assert!(index.index_uuid.is_some());
        assert!(index.type_url.is_some());
        assert_eq!(index.num_segments, Some(1));
        assert_eq!(index.num_indexed_rows, Some(1));
        assert_eq!(index.num_unindexed_rows, Some(0));
        assert!(index.created_at.is_some());
        assert!(index.size_bytes.is_some());
        assert!(index.index_version.is_some());
        assert!(index.index_details.is_some());

        let indices = table.as_native().unwrap().load_indices().await.unwrap();
        let index_name = &indices[0].index_name;
        let stats = table.index_stats(index_name).await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, 1);
        assert_eq!(stats.num_unindexed_rows, 0);
    }

    #[tokio::test]
    async fn test_create_fm_index() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["hello world"]))],
        )
        .unwrap();
        let conn = ConnectBuilder::new(uri).execute().await.unwrap();
        let table = conn
            .create_table("my_table", batch.clone())
            .execute()
            .await
            .unwrap();

        table
            .create_index(&["text"], Index::Fm(FmIndexBuilder::default()))
            .execute()
            .await
            .unwrap();
        table
            .wait_for_index(&["text_idx"], Duration::from_millis(10))
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Fm);
        assert_eq!(index.columns, vec!["text".to_string()]);

        let count = table
            .query()
            .only_if("contains(text, 'world')")
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_create_index_nested_field_paths() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = ConnectBuilder::new(uri).execute().await.unwrap();

        let num_rows = 512;
        let dimension = 8;

        let row_id = Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef;
        let row_dash_id = Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef;
        let top_user_id = Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef;

        let metadata = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("user_id", DataType::Int32, false)),
            Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef,
        )]));

        let mixed_case_metadata = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("userId", DataType::Int32, false)),
            Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef,
        )]));

        let vector_values = arrow_array::Float32Array::from_iter_values(
            (0..num_rows * dimension).map(|v| v as f32),
        );
        let embeddings =
            Arc::new(create_fixed_size_list(vector_values, dimension).unwrap()) as ArrayRef;
        let image = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new(
                "embedding",
                embeddings.data_type().clone(),
                false,
            )),
            embeddings,
        )]));

        let payload = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("text", DataType::Utf8, false)),
            Arc::new(StringArray::from_iter_values(
                (0..num_rows).map(|i| format!("document {}", i)),
            )) as ArrayRef,
        )]));

        let meta_data = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("user-id", DataType::Int32, false)),
            Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef,
        )]));

        let literal = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("a.b", DataType::Int32, false)),
            Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef,
        )]));

        let schema = Arc::new(Schema::new(vec![
            Field::new("rowId", DataType::Int32, false),
            Field::new("row-id", DataType::Int32, false),
            Field::new("userId", DataType::Int32, false),
            Field::new("metadata", metadata.data_type().clone(), false),
            Field::new("MetaData", mixed_case_metadata.data_type().clone(), false),
            Field::new("image", image.data_type().clone(), false),
            Field::new("payload", payload.data_type().clone(), false),
            Field::new("meta-data", meta_data.data_type().clone(), false),
            Field::new("literal", literal.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                row_id,
                row_dash_id,
                top_user_id,
                metadata,
                mixed_case_metadata,
                image,
                payload,
                meta_data,
                literal,
            ],
        )
        .unwrap();

        let table = conn
            .create_table("nested_index_paths", batch)
            .execute()
            .await
            .unwrap();

        table
            .create_index(
                &["metadata.user_id"],
                Index::BTree(BTreeIndexBuilder::default()),
            )
            .name("metadata_user_id_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["rowId"], Index::BTree(BTreeIndexBuilder::default()))
            .name("row_id_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["`row-id`"], Index::BTree(BTreeIndexBuilder::default()))
            .name("row_dash_id_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["userId"], Index::BTree(BTreeIndexBuilder::default()))
            .name("top_user_id_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(
                &["MetaData.userId"],
                Index::BTree(BTreeIndexBuilder::default()),
            )
            .name("mixed_case_metadata_user_id_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["image.embedding"], Index::Auto)
            .name("image_embedding_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["payload.text"], Index::FTS(Default::default()))
            .name("payload_text_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(
                &["`meta-data`.`user-id`"],
                Index::BTree(BTreeIndexBuilder::default()),
            )
            .name("escaped_names_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(
                &["literal.`a.b`"],
                Index::BTree(BTreeIndexBuilder::default()),
            )
            .name("literal_dot_idx".to_string())
            .execute()
            .await
            .unwrap();

        let mut index_configs = table.list_indices().await.unwrap();
        index_configs.sort_by(|left, right| left.name.cmp(&right.name));

        let indexed_columns = index_configs
            .iter()
            .map(|index| {
                (
                    index.name.as_str(),
                    index.columns.as_slice(),
                    index.index_type.clone(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            indexed_columns,
            vec![
                (
                    "escaped_names_idx",
                    &["`meta-data`.`user-id`".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "image_embedding_idx",
                    &["image.embedding".to_string()][..],
                    crate::index::IndexType::IvfPq,
                ),
                (
                    "literal_dot_idx",
                    &["literal.`a.b`".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "metadata_user_id_idx",
                    &["metadata.user_id".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "mixed_case_metadata_user_id_idx",
                    &["MetaData.userId".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "payload_text_idx",
                    &["payload.text".to_string()][..],
                    crate::index::IndexType::FTS,
                ),
                (
                    "row_dash_id_idx",
                    &["`row-id`".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "row_id_idx",
                    &["rowId".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "top_user_id_idx",
                    &["userId".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
            ]
        );

        let vector_results = table
            .query()
            .nearest_to(&[0.0; 8])
            .unwrap()
            .column("image.embedding")
            .limit(1)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            vector_results
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );

        let default_vector_results = table
            .query()
            .nearest_to(&[0.0; 8])
            .unwrap()
            .limit(1)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            default_vector_results
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );

        let fts_results = table
            .query()
            .full_text_search(FullTextSearchQuery::new("document".to_string()))
            .limit(5)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(!fts_results.is_empty());

        let filtered_results = table
            .query()
            .only_if("metadata.user_id = 42")
            .limit(1)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            filtered_results
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );
    }

    #[tokio::test]
    async fn test_create_bitmap_index() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri).execute().await.unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, true),
            Field::new("large_category", DataType::LargeUtf8, true),
            Field::new("is_active", DataType::Boolean, true),
            Field::new("data", DataType::Binary, true),
            Field::new("large_data", DataType::LargeBinary, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..100)),
                Arc::new(StringArray::from_iter_values(
                    (0..100).map(|i| format!("category_{}", i % 5)),
                )),
                Arc::new(LargeStringArray::from_iter_values(
                    (0..100).map(|i| format!("large_category_{}", i % 5)),
                )),
                Arc::new(BooleanArray::from_iter((0..100).map(|i| Some(i % 2 == 0)))),
                Arc::new(BinaryArray::from_iter_values(
                    (0_u32..100).map(|i| i.to_le_bytes()),
                )),
                Arc::new(LargeBinaryArray::from_iter_values(
                    (0_u32..100).map(|i| i.to_le_bytes()),
                )),
            ],
        )
        .unwrap();

        let table = conn
            .create_table("test_bitmap", batch.clone())
            .execute()
            .await
            .unwrap();

        // Create bitmap index on the "category" column
        table
            .create_index(&["category"], Index::Bitmap(Default::default()))
            .execute()
            .await
            .unwrap();

        // Create bitmap index on the "is_active" column
        table
            .create_index(&["is_active"], Index::Bitmap(Default::default()))
            .execute()
            .await
            .unwrap();

        // Create bitmap index on the "data" column
        table
            .create_index(&["data"], Index::Bitmap(Default::default()))
            .execute()
            .await
            .unwrap();

        // Create bitmap index on the "large_data" column
        table
            .create_index(&["large_data"], Index::Bitmap(Default::default()))
            .execute()
            .await
            .unwrap();

        // Create bitmap index on the "large_category" column
        table
            .create_index(&["large_category"], Index::Bitmap(Default::default()))
            .execute()
            .await
            .unwrap();

        // Verify the index was created
        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 5);

        // list_indices returns indices in alphabetical order by name
        let mut configs_iter = index_configs.into_iter();
        let index = configs_iter.next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(index.columns, vec!["category".to_string()]);

        let index = configs_iter.next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(index.columns, vec!["data".to_string()]);

        let index = configs_iter.next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(index.columns, vec!["is_active".to_string()]);

        let index = configs_iter.next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(index.columns, vec!["large_category".to_string()]);

        let index = configs_iter.next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(index.columns, vec!["large_data".to_string()]);
    }

    #[tokio::test]
    async fn test_create_label_list_index() {
        let conn = connect("memory://").execute().await.unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "tags",
                DataType::List(Field::new("item", DataType::Utf8, true).into()),
                true,
            ),
        ]));

        const TAGS: [&str; 3] = ["cat", "dog", "fish"];

        let values_builder = StringBuilder::new();
        let mut builder = ListBuilder::new(values_builder);
        for i in 0..120 {
            builder.values().append_value(TAGS[i % 3]);
            if i % 3 == 0 {
                builder.append(true)
            }
        }
        let tags = Arc::new(builder.finish());

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..40)), tags],
        )
        .unwrap();

        let table = conn
            .create_table("test_bitmap", batch.clone())
            .execute()
            .await
            .unwrap();

        // Can not create btree or bitmap index on list column
        assert!(
            table
                .create_index(&["tags"], Index::BTree(Default::default()))
                .execute()
                .await
                .is_err()
        );
        assert!(
            table
                .create_index(&["tags"], Index::Bitmap(Default::default()))
                .execute()
                .await
                .is_err()
        );

        // Create bitmap index on the "category" column
        table
            .create_index(&["tags"], Index::LabelList(Default::default()))
            .execute()
            .await
            .unwrap();

        // Verify the index was created
        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::LabelList);
        assert_eq!(index.columns, vec!["tags".to_string()]);
    }

    #[tokio::test]
    async fn test_create_label_list_index_on_large_list() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri).execute().await.unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            DataType::LargeList(Field::new("item", DataType::Utf8, true).into()),
            true,
        )]));

        const TAGS: [&str; 3] = ["cat", "dog", "fish"];

        let values_builder = StringBuilder::new();
        let mut builder = LargeListBuilder::new(values_builder);
        for i in 0..120 {
            builder.values().append_value(TAGS[i % 3]);
            if i % 3 == 0 {
                builder.append(true)
            }
        }
        let tags = Arc::new(builder.finish());

        let batch = RecordBatch::try_new(schema, vec![tags]).unwrap();

        let table = conn
            .create_table("test_large_list_label_list", batch)
            .execute()
            .await
            .unwrap();

        table
            .create_index(&["tags"], Index::LabelList(Default::default()))
            .execute()
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::LabelList);
        assert_eq!(index.columns, vec!["tags".to_string()]);
    }

    #[tokio::test]
    async fn test_create_inverted_index() {
        let conn = connect("memory://").execute().await.unwrap();

        let id = Int32Array::from_iter_values(0..120_i32);
        let text = StringArray::from_iter_values((0..120).map(|i| {
            const WORDS: [&str; 3] = ["cat", "dog", "fish"];
            WORDS[i % 3].to_string()
        }));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("text", DataType::Utf8, true),
            ])),
            vec![Arc::new(id) as ArrayRef, Arc::new(text) as ArrayRef],
        )
        .unwrap();

        let table = conn
            .create_table("test_bitmap", batch.clone())
            .execute()
            .await
            .unwrap();

        table
            .create_index(&["text"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();
        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::FTS);
        assert_eq!(index.columns, vec!["text".to_string()]);
        assert_eq!(index.name, "text_idx");

        let num_rows = 120;
        let stats = table.index_stats("text_idx").await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, num_rows);
        assert_eq!(stats.num_unindexed_rows, 0);
        assert_eq!(stats.index_type, crate::index::IndexType::FTS);
        assert_eq!(stats.distance_type, None);

        // Make sure we can call prewarm without error
        table.prewarm_index("text_idx").await.unwrap();
    }

    #[tokio::test]
    pub async fn test_list_indices_skip_frag_reuse() {
        let conn = connect("memory://").execute().await.unwrap();

        let id = Int32Array::from_iter_values(0..100);
        let foo = Int32Array::from_iter_values(0..100);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("foo", DataType::Int32, true),
            ])),
            vec![Arc::new(id) as ArrayRef, Arc::new(foo) as ArrayRef],
        )
        .unwrap();

        let table = conn
            .create_table("test_list_indices_skip_frag_reuse", batch.clone())
            .execute()
            .await
            .unwrap();

        table.add(batch.clone()).execute().await.unwrap();

        table
            .create_index(&["id"], Index::Bitmap(BitmapIndexBuilder {}))
            .execute()
            .await
            .unwrap();

        table
            .optimize(OptimizeAction::Compact {
                options: CompactionOptions {
                    target_rows_per_fragment: 2_000,
                    defer_index_remap: true,
                    ..Default::default()
                },
                remap_options: None,
            })
            .await
            .unwrap();

        let result = table.list_indices().await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].index_type, crate::index::IndexType::Bitmap);
    }
}
