use std::collections::BTreeMap;

use arrow::array::downcast_array;
use arrow_array::{Float32Array, RecordBatch};
use async_trait::async_trait;
use lance::dataset::ROW_ID;

use crate::error::{Error, Result};
use crate::rerankers::Reranker;

pub struct RRFReranker {
    k: i32,
}

#[async_trait]
impl Reranker for RRFReranker {
    /// TODO api comments
    async fn rerank_vector(
        &self,
        _query: &str,
        _vector_results: RecordBatch
    ) -> Result<RecordBatch> {
        Err(Error::NotSupported { message: "RRFReranker is not supported for vector search (only Hybrid search is supporrted)".to_string() })
    }

    /// TODO api comments
    async fn rerank_fts(
        &self,
        _query: &str,
        _fts_results: RecordBatch,
    ) -> Result<RecordBatch> {
        Err(Error::NotSupported { message: "RRFReranker is not supported for vector search (only Hybrid search is supported)".to_string() })
    }

    /// TODO api comments
    async fn rerank_hybrid(
        &self,
        query: &str,
        vector_results: RecordBatch,
        fts_results: RecordBatch
    ) -> Result<RecordBatch> {
        let vector_ids = vector_results.column_by_name(ROW_ID).ok_or(Error::InvalidInput {
            message:  format!(
                "expected column {} not found in vector_results. found columns {:?}",
                ROW_ID,
                vector_results.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>()
            )
        })?;
        let fts_ids = fts_results.column_by_name(ROW_ID).ok_or(Error::InvalidInput {
            message:  format!(
                "expected column {} not found in fts_results. found columns {:?}",
                ROW_ID,
                fts_results.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>()
            )
        })?;

        let vector_ids: Float32Array = downcast_array(&vector_ids);
        let fts_ids: Float32Array = downcast_array(&fts_ids);

        let mut rrf_score_map = BTreeMap::new();
        vector_ids.values().iter().enumerate().for_each(|(i, result_id)| {
            rrf_score_map.insert(key, value)
        });


        todo!()
    }
}