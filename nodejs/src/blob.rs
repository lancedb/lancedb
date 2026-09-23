// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::ops::Range;
use std::sync::Arc;

use arrow_array::{Array, LargeBinaryArray};
use lancedb::blob::BlobFile as LanceBlobFile;
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::error::convert_error;

#[napi]
pub struct BlobFile {
    inner: Arc<LanceBlobFile>,
}

impl BlobFile {
    pub(crate) fn new(inner: LanceBlobFile) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[napi]
impl BlobFile {
    #[napi]
    pub fn size(&self) -> BigInt {
        BigInt::from(self.inner.size())
    }

    #[napi]
    pub async fn read(&self) -> napi::Result<Buffer> {
        let bytes = self.inner.read().await.map_err(|err| convert_error(&err))?;
        Ok(Buffer::from(bytes.as_ref()))
    }

    #[napi]
    pub async fn read_range(&self, start: BigInt, end: BigInt) -> napi::Result<Buffer> {
        let range = bigint_range(start, end)?;
        let bytes = self
            .inner
            .read_range(range)
            .await
            .map_err(|err| convert_error(&err))?;
        Ok(Buffer::from(bytes.as_ref()))
    }
}

fn bigint_range(start: BigInt, end: BigInt) -> napi::Result<Range<u64>> {
    let start = parse_u64(start, "start")?;
    let end = parse_u64(end, "end")?;
    if start > end {
        return Err(napi::Error::from_reason(format!(
            "invalid blob range: start ({start}) > end ({end})"
        )));
    }
    Ok(start..end)
}

fn parse_u64(value: BigInt, name: &str) -> napi::Result<u64> {
    let (negative, value, lossless) = value.get_u64();
    if negative {
        return Err(napi::Error::from_reason(format!(
            "{name} cannot be negative"
        )));
    }
    if !lossless {
        return Err(napi::Error::from_reason(format!(
            "{name} is too large to fit in u64"
        )));
    }
    Ok(value)
}

pub fn parse_row_ids(row_ids: Vec<BigInt>) -> napi::Result<Vec<u64>> {
    row_ids
        .into_iter()
        .map(|id| parse_u64(id, "row id"))
        .collect()
}

pub fn copy_blob_buffers(array: LargeBinaryArray) -> Vec<Option<Buffer>> {
    (0..array.len())
        .map(|i| {
            if array.is_null(i) {
                None
            } else {
                Some(Buffer::from(array.value(i).to_vec()))
            }
        })
        .collect()
}
