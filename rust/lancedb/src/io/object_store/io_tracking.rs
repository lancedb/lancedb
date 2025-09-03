// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{
    fmt::{Display, Formatter},
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use futures::stream::BoxStream;
use lance::io::WrappingObjectStore;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OSResult, UploadPart,
};

#[derive(Debug, Default)]
pub struct IoStats {
    pub read_iops: u64,
    pub read_bytes: u64,
    pub write_iops: u64,
    pub write_bytes: u64,
}

impl Display for IoStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

#[derive(Debug, Clone)]
pub struct IoTrackingStore {
    target: Arc<dyn ObjectStore>,
    stats: Arc<Mutex<IoStats>>,
}

impl Display for IoTrackingStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

#[derive(Debug, Default, Clone)]
pub struct IoStatsHolder(Arc<Mutex<IoStats>>);

impl IoStatsHolder {
    pub fn incremental_stats(&self) -> IoStats {
        std::mem::take(&mut self.0.lock().expect("failed to lock IoStats"))
    }
}

impl WrappingObjectStore for IoStatsHolder {
    fn wrap(
        &self,
        target: Arc<dyn ObjectStore>,
        _storage_options: Option<&std::collections::HashMap<String, String>>,
    ) -> Arc<dyn ObjectStore> {
        Arc::new(IoTrackingStore {
            target,
            stats: self.0.clone(),
        })
    }
}

impl IoTrackingStore {
    pub fn new_wrapper() -> (Arc<dyn WrappingObjectStore>, Arc<Mutex<IoStats>>) {
        let stats = Arc::new(Mutex::new(IoStats::default()));
        (Arc::new(IoStatsHolder(stats.clone())), stats)
    }

    fn record_read(&self, num_bytes: u64) {
        let mut stats = self.stats.lock().unwrap();
        stats.read_iops += 1;
        stats.read_bytes += num_bytes;
    }

    fn record_write(&self, num_bytes: u64) {
        let mut stats = self.stats.lock().unwrap();
        stats.write_iops += 1;
        stats.write_bytes += num_bytes;
    }
}

#[async_trait::async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for IoTrackingStore {
    async fn put(&self, location: &Path, bytes: PutPayload) -> OSResult<PutResult> {
        self.record_write(bytes.content_length() as u64);
        self.target.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> OSResult<PutResult> {
        self.record_write(bytes.content_length() as u64);
        self.target.put_opts(location, bytes, opts).await
    }

    async fn put_multipart(&self, location: &Path) -> OSResult<Box<dyn MultipartUpload>> {
        let target = self.target.put_multipart(location).await?;
        Ok(Box::new(IoTrackingMultipartUpload {
            target,
            stats: self.stats.clone(),
        }))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OSResult<Box<dyn MultipartUpload>> {
        let target = self.target.put_multipart_opts(location, opts).await?;
        Ok(Box::new(IoTrackingMultipartUpload {
            target,
            stats: self.stats.clone(),
        }))
    }

    async fn get(&self, location: &Path) -> OSResult<GetResult> {
        let result = self.target.get(location).await;
        if let Ok(result) = &result {
            let num_bytes = result.range.end - result.range.start;
            self.record_read(num_bytes);
        }
        result
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
        let result = self.target.get_opts(location, options).await;
        if let Ok(result) = &result {
            let num_bytes = result.range.end - result.range.start;
            self.record_read(num_bytes);
        }
        result
    }

    async fn get_range(&self, location: &Path, range: std::ops::Range<u64>) -> OSResult<Bytes> {
        let result = self.target.get_range(location, range).await;
        if let Ok(result) = &result {
            self.record_read(result.len() as u64);
        }
        result
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> OSResult<Vec<Bytes>> {
        let result = self.target.get_ranges(location, ranges).await;
        if let Ok(result) = &result {
            self.record_read(result.iter().map(|b| b.len() as u64).sum());
        }
        result
    }

    async fn head(&self, location: &Path) -> OSResult<ObjectMeta> {
        self.record_read(0);
        self.target.head(location).await
    }

    async fn delete(&self, location: &Path) -> OSResult<()> {
        self.record_write(0);
        self.target.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, OSResult<Path>>,
    ) -> BoxStream<'a, OSResult<Path>> {
        self.target.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.record_read(0);
        self.target.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.record_read(0);
        self.target.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
        self.record_read(0);
        self.target.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OSResult<()> {
        self.record_write(0);
        self.target.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> OSResult<()> {
        self.record_write(0);
        self.target.rename(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> OSResult<()> {
        self.record_write(0);
        self.target.rename_if_not_exists(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OSResult<()> {
        self.record_write(0);
        self.target.copy_if_not_exists(from, to).await
    }
}

#[derive(Debug)]
struct IoTrackingMultipartUpload {
    target: Box<dyn MultipartUpload>,
    stats: Arc<Mutex<IoStats>>,
}

#[async_trait::async_trait]
impl MultipartUpload for IoTrackingMultipartUpload {
    async fn abort(&mut self) -> OSResult<()> {
        self.target.abort().await
    }

    async fn complete(&mut self) -> OSResult<PutResult> {
        self.target.complete().await
    }

    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        {
            let mut stats = self.stats.lock().unwrap();
            stats.write_iops += 1;
            stats.write_bytes += payload.content_length() as u64;
        }
        self.target.put_part(payload)
    }
}
