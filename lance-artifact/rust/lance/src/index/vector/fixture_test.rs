// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! tests with fixtures -- nothing in this file is should be public interface

#[cfg(test)]
mod test {
    use lance_core::utils::row_addr_remap::RowAddrRemap;
    use std::{
        any::Any,
        cell::OnceCell,
        sync::{Arc, Mutex},
    };

    use approx::assert_relative_eq;
    use arrow::array::AsArray;
    use arrow_array::{Array, FixedSizeListArray, Float32Array, RecordBatch, UInt32Array};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use datafusion::execution::SendableRecordBatchStream;
    use lance_arrow::FixedSizeListArrayExt;
    use lance_core::deepsize::{Context, DeepSizeOf};
    use lance_core::{cache::LanceCache, utils::tempfile::TempStdFile};
    use lance_index::vector::v3::subindex::SubIndexType;
    use lance_index::{Index, IndexType, vector::Query};
    use lance_index::{metrics::MetricsCollector, vector::ivf::storage::IvfModel};
    use lance_index::{
        metrics::NoOpMetricsCollector,
        vector::quantizer::{QuantizationType, Quantizer},
    };
    use lance_io::{local::LocalObjectReader, traits::Reader};
    use lance_linalg::{distance::MetricType, kernels::normalize_arrow};
    use roaring::RoaringBitmap;
    use uuid::Uuid;

    use super::super::VectorIndex;
    use crate::{
        Result,
        index::{
            prefilter::{DatasetPreFilter, PreFilter},
            vector::ivf::IVFIndex,
        },
    };

    #[derive(Clone, Debug)]
    struct ResidualCheckMockIndex {
        use_residual: bool,
        metric_type: MetricType,

        assert_query_value: Vec<f32>,

        ret_val: RecordBatch,
    }

    impl DeepSizeOf for ResidualCheckMockIndex {
        fn deep_size_of_children(&self, cx: &mut Context) -> usize {
            self.assert_query_value.deep_size_of_children(cx)
                + self.ret_val.deep_size_of_children(cx)
        }
    }

    #[async_trait]
    impl Index for ResidualCheckMockIndex {
        /// Cast to [Any].
        fn as_any(&self) -> &dyn Any {
            self
        }

        /// Cast to [Index]
        fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
            self
        }

        async fn prewarm(&self) -> Result<()> {
            Ok(())
        }

        /// Retrieve index statistics as a JSON Value
        fn statistics(&self) -> Result<serde_json::Value> {
            Ok(serde_json::Value::Null)
        }

        /// Get the type of the index
        fn index_type(&self) -> IndexType {
            IndexType::Vector
        }

        async fn calculate_included_frags(&self) -> Result<RoaringBitmap> {
            Ok(RoaringBitmap::new())
        }
    }

    #[async_trait]
    impl VectorIndex for ResidualCheckMockIndex {
        async fn search(
            &self,
            query: &Query,
            _pre_filter: Arc<dyn PreFilter>,
            _metrics: &dyn MetricsCollector,
        ) -> Result<RecordBatch> {
            let key: &Float32Array = query.key.as_primitive();
            assert_eq!(key.len(), self.assert_query_value.len());
            for (i, &v) in key.iter().zip(self.assert_query_value.iter()) {
                assert_relative_eq!(v, i.unwrap());
            }
            Ok(self.ret_val.clone())
        }

        fn find_partitions(&self, _: &Query) -> Result<(UInt32Array, Float32Array)> {
            unimplemented!("only for IVF")
        }

        async fn search_in_partition(
            &self,
            _: usize,
            _: &Query,
            _: Arc<dyn PreFilter>,
            _: &dyn MetricsCollector,
        ) -> Result<RecordBatch> {
            unimplemented!("only for IVF")
        }

        fn total_partitions(&self) -> usize {
            1
        }

        fn is_loadable(&self) -> bool {
            true
        }

        fn use_residual(&self) -> bool {
            self.use_residual
        }

        async fn load(
            &self,
            _reader: Arc<dyn Reader>,
            _offset: usize,
            _length: usize,
        ) -> Result<Box<dyn VectorIndex>> {
            Ok(Box::new(self.clone()))
        }

        fn num_rows(&self) -> u64 {
            self.ret_val.num_rows() as u64
        }

        fn row_ids(&self) -> Box<dyn Iterator<Item = &u64>> {
            todo!("this method is for only IVF_HNSW_* index");
        }

        async fn remap(&mut self, _mapping: &RowAddrRemap) -> Result<()> {
            Ok(())
        }

        async fn to_batch_stream(&self, _with_vector: bool) -> Result<SendableRecordBatchStream> {
            unimplemented!("only for SubIndex")
        }

        fn ivf_model(&self) -> &IvfModel {
            unimplemented!("only for IVF")
        }

        fn quantizer(&self) -> Quantizer {
            unimplemented!("only for IVF")
        }

        fn partition_size(&self, _: usize) -> usize {
            unimplemented!("only for IVF")
        }

        /// the index type of this vector index.
        fn sub_index_type(&self) -> (SubIndexType, QuantizationType) {
            unimplemented!("only for IVF")
        }

        /// The metric type of this vector index.
        fn metric_type(&self) -> MetricType {
            self.metric_type
        }
    }

    #[tokio::test]
    async fn test_ivf_residual_handling() {
        let centroids = Float32Array::from_iter(vec![1.0, 1.0, -1.0, -1.0, -1.0, 1.0, 1.0, -1.0]);
        let centroids = FixedSizeListArray::try_new_from_values(centroids, 2).unwrap();
        let mut ivf = IvfModel::new(centroids, None);
        // Add 4 partitions
        for _ in 0..4 {
            ivf.add_partition(0);
        }

        let make_idx = move |assert_query: Vec<f32>, metric: MetricType| async move {
            let f = TempStdFile::default();

            let reader = LocalObjectReader::open_local_path(f, 64, None)
                .await
                .unwrap();

            let mock_sub_index = Arc::new(ResidualCheckMockIndex {
                use_residual: true,
                // always L2
                metric_type: MetricType::L2,
                assert_query_value: assert_query,
                ret_val: RecordBatch::new_empty(Arc::new(Schema::new(vec![
                    Field::new("id", DataType::UInt64, false),
                    Field::new("_distance", DataType::Float32, true),
                ]))),
            });
            IVFIndex::try_new(
                Uuid::new_v4(),
                ivf,
                reader.into(),
                mock_sub_index,
                metric,
                LanceCache::no_cache(),
            )
            .unwrap()
        };

        struct TestCase {
            query: Vec<f32>,
            metric: MetricType,
            expected_query_at_subindex: Vec<f32>,
        }

        for TestCase {
            query,
            metric,
            expected_query_at_subindex,
        } in [
            // L2 should residualize with the correct centroid
            TestCase {
                query: vec![1.0; 2],
                metric: MetricType::L2,
                expected_query_at_subindex: vec![0.0; 2],
            },
            // Cosine should normalize and residualize
            TestCase {
                query: vec![1.0; 2],
                metric: MetricType::Cosine,
                expected_query_at_subindex: vec![1.0 / 2.0_f32.sqrt() - 1.0; 2],
            },
            TestCase {
                query: vec![2.0; 2],
                metric: MetricType::Cosine,
                expected_query_at_subindex: vec![2.0 / 8.0_f32.sqrt() - 1.0; 2],
            },
        ] {
            let mut key = Arc::new(Float32Array::from(query)) as Arc<dyn Array>;
            if metric == MetricType::Cosine {
                key = normalize_arrow(&key).unwrap().0;
            };
            let q = Query {
                column: "test".to_string(),
                key,
                k: 1,
                lower_bound: None,
                upper_bound: None,
                minimum_nprobes: 1,
                maximum_nprobes: None,
                ef: None,
                refine_factor: None,
                metric_type: Some(metric),
                use_index: true,
                query_parallelism: lance_index::vector::DEFAULT_QUERY_PARALLELISM,
                dist_q_c: 0.0,
                approx_mode: Default::default(),
            };
            let idx = make_idx.clone()(expected_query_at_subindex, metric).await;
            let (partition_ids, _) = idx.find_partitions(&q).unwrap();
            assert_eq!(partition_ids.len(), 4);
            let nearest_partition_id = partition_ids.value(0);
            idx.search_in_partition(
                nearest_partition_id as usize,
                &q,
                Arc::new(DatasetPreFilter {
                    deleted_ids: None,
                    filtered_ids: None,
                    deleted_fragments: None,
                    overlay_block: None,
                    final_mask: Mutex::new(OnceCell::new()),
                }),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();
        }
    }
}
