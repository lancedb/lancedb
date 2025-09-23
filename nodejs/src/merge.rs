// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::time::Duration;

use lancedb::{arrow::IntoArrow, ipc::ipc_file_to_batches, table::merge::MergeInsertBuilder};
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::{error::convert_error, table::MergeResult};

#[napi]
#[derive(Clone)]
/// A builder used to create and run a merge insert operation
pub struct NativeMergeInsertBuilder {
    pub(crate) inner: MergeInsertBuilder,
}

#[napi]
impl NativeMergeInsertBuilder {
    #[napi]
    pub fn when_matched_update_all(&self, condition: Option<String>) -> Self {
        let mut this = self.clone();
        this.inner.when_matched_update_all(condition);
        this
    }

    #[napi]
    pub fn when_not_matched_insert_all(&self) -> Self {
        let mut this = self.clone();
        this.inner.when_not_matched_insert_all();
        this
    }
    #[napi]
    pub fn when_not_matched_by_source_delete(&self, filter: Option<String>) -> Self {
        let mut this = self.clone();
        this.inner.when_not_matched_by_source_delete(filter);
        this
    }

    #[napi]
    pub fn set_timeout(&mut self, timeout: u32) {
        self.inner.timeout(Duration::from_millis(timeout as u64));
    }

    #[napi(catch_unwind)]
    pub async fn execute(&self, buf: Buffer) -> napi::Result<MergeResult> {
        let data = ipc_file_to_batches(buf.to_vec())
            .and_then(IntoArrow::into_arrow)
            .map_err(|e| {
                napi::Error::from_reason(format!("Failed to read IPC file: {}", convert_error(&e)))
            })?;

        let this = self.clone();

        let res = this.inner.execute(data).await.map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to execute merge insert: {}",
                convert_error(&e)
            ))
        })?;
        Ok(res.into())
    }

    #[napi(catch_unwind)]
    pub async fn explain_plan(&self, verbose: bool) -> napi::Result<String> {
        let this = self.clone();
        this.inner.explain_plan(None, verbose).await.map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to explain merge insert plan: {}",
                convert_error(&e)
            ))
        })
    }

    #[napi(catch_unwind)]
    pub async fn analyze_plan(&self, buf: Buffer) -> napi::Result<String> {
        let data = ipc_file_to_batches(buf.to_vec())
            .and_then(IntoArrow::into_arrow)
            .map_err(|e| {
                napi::Error::from_reason(format!("Failed to read IPC file: {}", convert_error(&e)))
            })?;

        let this = self.clone();

        this.inner.analyze_plan(data).await.map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to analyze merge insert plan: {}",
                convert_error(&e)
            ))
        })
    }
}

impl From<MergeInsertBuilder> for NativeMergeInsertBuilder {
    fn from(inner: MergeInsertBuilder) -> Self {
        Self { inner }
    }
}
