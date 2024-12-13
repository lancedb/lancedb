// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow_array::RecordBatch;
use async_trait::async_trait;
use napi::{
    bindgen_prelude::*,
    threadsafe_function::{ErrorStrategy, ThreadsafeFunction},
};
use napi_derive::napi;

use lancedb::ipc::batches_to_ipc_file;
use lancedb::rerankers::Reranker as LanceDBReranker;
use lancedb::{error::Error, ipc::ipc_file_to_batches};

use crate::error::NapiErrorExt;

/// Reranker implementation that "wraps" a NodeJS Reranker implementation.
/// This contains references to the callbacks that can be used to invoke the
/// reranking methods on the NodeJS implementation and handles serializing the
/// record batches to Arrow IPC buffers.
#[napi]
pub struct Reranker {
    /// callback to the Javascript which will call the rerankHybrid method of
    /// some Reranker implementation
    rerank_hybrid: ThreadsafeFunction<RerankHybridCallbackArgs, ErrorStrategy::CalleeHandled>,
}

#[napi]
impl Reranker {
    #[napi]
    pub fn new(callbacks: RerankerCallbacks) -> Self {
        let rerank_hybrid = callbacks
            .rerank_hybrid
            .create_threadsafe_function(0, move |ctx| Ok(vec![ctx.value]))
            .unwrap();

        Self { rerank_hybrid }
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
            vec_results: batches_to_ipc_file(&[vector_results])?,
            fts_results: batches_to_ipc_file(&[fts_results])?,
        };
        let promised_buffer: Promise<Buffer> = self
            .rerank_hybrid
            .call_async(Ok(callback_args))
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
pub struct RerankerCallbacks {
    pub rerank_hybrid: JsFunction,
}

#[napi(object)]
pub struct RerankHybridCallbackArgs {
    pub query: String,
    pub vec_results: Vec<u8>,
    pub fts_results: Vec<u8>,
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
