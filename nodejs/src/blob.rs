// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::{Array, LargeBinaryArray};
use futures::future::abortable;
use lancedb::blob::BlobFile as LanceBlobFile;
use napi::Env;
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::error::convert_error;

#[napi(object)]
pub struct BlobRange {
    pub start: BigInt,
    pub end: BigInt,
}

#[napi]
pub struct BlobFile {
    inner: Arc<LanceBlobFile>,
    closed: AtomicBool,
}

impl BlobFile {
    pub(crate) fn new(inner: LanceBlobFile) -> Self {
        Self {
            inner: Arc::new(inner),
            closed: AtomicBool::new(false),
        }
    }

    fn ensure_open(&self) -> napi::Result<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(napi::Error::from_reason("blob file is already closed"))
        } else {
            Ok(())
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
    pub fn read<'env>(
        &self,
        env: &'env Env,
        max_bytes: Option<BigInt>,
        signal: Option<AbortSignal>,
    ) -> napi::Result<PromiseRaw<'env, Buffer>> {
        self.ensure_open()?;
        let max_bytes = max_bytes
            .map(|max_bytes| {
                usize::try_from(parse_u64(max_bytes, "maxBytes")?)
                    .map_err(|_| napi::Error::from_reason("maxBytes is too large"))
            })
            .transpose()?;
        let inner = self.inner.clone();
        spawn_abortable(env, signal, async move {
            let bytes = match max_bytes {
                None => inner.read().await,
                Some(max_bytes) => inner.read_up_to(max_bytes).await,
            }
            .map_err(|err| convert_error(&err))?;
            Ok(Buffer::from(bytes.as_ref()))
        })
    }

    #[napi]
    pub fn read_range<'env>(
        &self,
        env: &'env Env,
        start: BigInt,
        end: BigInt,
        signal: Option<AbortSignal>,
    ) -> napi::Result<PromiseRaw<'env, Buffer>> {
        self.ensure_open()?;
        let range = bigint_range(start, end)?;
        let inner = self.inner.clone();
        spawn_abortable(env, signal, async move {
            let bytes = inner
                .read_range(range)
                .await
                .map_err(|err| convert_error(&err))?;
            Ok(Buffer::from(bytes.as_ref()))
        })
    }

    #[napi]
    pub fn read_ranges<'env>(
        &self,
        env: &'env Env,
        ranges: Vec<BlobRange>,
        signal: Option<AbortSignal>,
    ) -> napi::Result<PromiseRaw<'env, Vec<Buffer>>> {
        self.ensure_open()?;
        let ranges = ranges
            .into_iter()
            .map(|range| bigint_range(range.start, range.end))
            .collect::<napi::Result<Vec<_>>>()?;
        let inner = self.inner.clone();
        spawn_abortable(env, signal, async move {
            let buffers = inner
                .read_ranges(&ranges)
                .await
                .map_err(|err| convert_error(&err))?;
            Ok(buffers
                .iter()
                .map(|bytes| Buffer::from(bytes.as_ref()))
                .collect())
        })
    }

    #[napi]
    pub async fn seek(&self, position: BigInt) -> napi::Result<()> {
        self.ensure_open()?;
        let position = parse_u64(position, "position")?;
        self.inner
            .seek(position)
            .await
            .map_err(|err| convert_error(&err))
    }

    #[napi]
    pub async fn tell(&self) -> napi::Result<BigInt> {
        self.ensure_open()?;
        let position = self.inner.tell().await.map_err(|err| convert_error(&err))?;
        Ok(BigInt::from(position))
    }

    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.inner.close().await.map_err(|err| convert_error(&err))
    }

    #[napi]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// Runs `fut` on the napi runtime. Aborting `signal` drops the future, which
/// cancels in-flight reads.
pub(crate) fn spawn_abortable<'env, T>(
    env: &'env Env,
    signal: Option<AbortSignal>,
    fut: impl Future<Output = napi::Result<T>> + Send + 'static,
) -> napi::Result<PromiseRaw<'env, T>>
where
    T: ToNapiValue + Send + 'static,
{
    let (fut, handle) = abortable(fut);
    if let Some(signal) = signal {
        signal.on_abort(move || handle.abort());
    }
    env.spawn_future(async move {
        fut.await
            .unwrap_or_else(|_| Err(napi::Error::from_reason("blob read aborted")))
    })
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
