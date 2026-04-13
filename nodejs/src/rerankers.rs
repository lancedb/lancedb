// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_array::RecordBatch;
use async_trait::async_trait;
use napi::{bindgen_prelude::*, threadsafe_function::ThreadsafeFunction};
use napi_derive::napi;

use lancedb::ipc::batches_to_ipc_file;
use lancedb::rerankers::Reranker as LanceDBReranker;
use lancedb::{error::Error, ipc::ipc_file_to_batches};

use crate::error::NapiErrorExt;

type RerankHybridFn = ThreadsafeFunction<
    RerankHybridCallbackArgs,
    Promise<Buffer>,
    RerankHybridCallbackArgs,
    Status,
    false,
>;

/// Reranker implementation that "wraps" a NodeJS Reranker implementation.
/// This contains references to the callbacks that can be used to invoke the
/// reranking methods on the NodeJS implementation and handles serializing the
/// record batches to Arrow IPC buffers.
pub struct Reranker {
    rerank_hybrid: RerankHybridFn,
}

impl Reranker {
    pub fn new(
        rerank_hybrid: Function<RerankHybridCallbackArgs, Promise<Buffer>>,
    ) -> napi::Result<Self> {
        let rerank_hybrid = rerank_hybrid.build_threadsafe_function().build()?;
        Ok(Self { rerank_hybrid })
    }
}

#[async_trait]
impl lancedb::rerankers::Reranker for Reranker {
    async fn rerank_hybrid(
        &self,
        query: &str,
        vector_results: RecordBatch,
        fts_results: RecordBatch,
    ) -> lancedb::error::Result<RecordBatch> {
        let callback_args = RerankHybridCallbackArgs {
            query: query.to_string(),
            vec_results: Buffer::from(batches_to_ipc_file(&[vector_results])?.as_ref()),
            fts_results: Buffer::from(batches_to_ipc_file(&[fts_results])?.as_ref()),
        };
        let promised_buffer: Promise<Buffer> = self
            .rerank_hybrid
            .call_async(callback_args)
            .await
            .map_err(|e| Error::Runtime {
            message: format!("napi error status={}, reason={}", e.status, e.reason),
        })?;
        let buffer = promised_buffer.await.map_err(|e| Error::Runtime {
            message: format!("napi error status={}, reason={}", e.status, e.reason),
        })?;
        let mut reader = ipc_file_to_batches(buffer.to_vec())?;
        let result = reader.next().ok_or(Error::Runtime {
            message: "reranker result deserialization failed".to_string(),
        })??;

        return Ok(result);
    }
}

impl std::fmt::Debug for Reranker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NodeJSRerankerWrapper")
    }
}

#[napi(object)]
pub struct RerankHybridCallbackArgs {
    pub query: String,
    pub vec_results: Buffer,
    pub fts_results: Buffer,
}

fn buffer_to_record_batch(buffer: Buffer) -> Result<RecordBatch> {
    let mut reader = ipc_file_to_batches(buffer.to_vec()).default_error()?;
    reader
        .next()
        .ok_or(Error::InvalidInput {
            message: "expected buffer containing record batch".to_string(),
        })
        .default_error()?
        .map_err(Error::from)
        .default_error()
}

/// Wrapper around rust RRFReranker
#[napi]
pub struct RRFReranker {
    inner: lancedb::rerankers::rrf::RRFReranker,
}

#[napi]
impl RRFReranker {
    #[napi]
    pub async fn try_new(k: &[f32]) -> Result<Self> {
        let k = k
            .first()
            .copied()
            .ok_or(Error::InvalidInput {
                message: "must supply RRF Reranker constructor arg 'k'".to_string(),
            })
            .default_error()?;

        Ok(Self {
            inner: lancedb::rerankers::rrf::RRFReranker::new(k),
        })
    }

    #[napi]
    pub async fn rerank_hybrid(
        &self,
        query: String,
        vec_results: Buffer,
        fts_results: Buffer,
    ) -> Result<Buffer> {
        let vec_results = buffer_to_record_batch(vec_results)?;
        let fts_results = buffer_to_record_batch(fts_results)?;

        let result = self
            .inner
            .rerank_hybrid(&query, vec_results, fts_results)
            .await
            .unwrap();

        let result_buff = batches_to_ipc_file(&[result]).default_error()?;

        Ok(Buffer::from(result_buff.as_ref()))
    }
}
