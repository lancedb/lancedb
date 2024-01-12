// Copyright 2023 Lance Developers.
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

//! A mirroring object store that mirror writes to a secondary object store

use std::{
    fmt::Formatter,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{stream::BoxStream, FutureExt, StreamExt};
use lance::io::object_store::WrappingObjectStore;
use object_store::{
    path::Path, Error, GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    PutOptions, PutResult, Result,
};

use async_trait::async_trait;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    task::JoinHandle,
};

#[derive(Debug)]
struct MirroringObjectStore {
    primary: Arc<dyn ObjectStore>,
    secondary: Arc<dyn ObjectStore>,
}

impl std::fmt::Display for MirroringObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "MirrowingObjectStore")?;
        writeln!(f, "primary:")?;
        self.primary.fmt(f)?;
        writeln!(f, "secondary:")?;
        self.secondary.fmt(f)?;
        Ok(())
    }
}

trait PrimaryOnly {
    fn primary_only(&self) -> bool;
}

impl PrimaryOnly for Path {
    fn primary_only(&self) -> bool {
        self.filename().unwrap_or("") == "_latest.manifest"
    }
}

/// An object store that mirrors write to secondsry object store first
/// and than commit to primary object store.
///
/// This is meant to mirrow writes to a less-durable but lower-latency
/// store. We have primary store that is durable but slow, and a secondary
/// store that is fast but not asdurable
///
/// Note: this object store does not mirror writes to *.manifest files
#[async_trait]
impl ObjectStore for MirroringObjectStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<PutResult> {
        if location.primary_only() {
            self.primary.put(location, bytes).await
        } else {
            self.secondary.put(location, bytes.clone()).await?;
            self.primary.put(location, bytes).await
        }
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: Bytes,
        options: PutOptions,
    ) -> Result<PutResult> {
        if location.primary_only() {
            self.primary.put_opts(location, bytes, options).await
        } else {
            self.secondary
                .put_opts(location, bytes.clone(), options.clone())
                .await?;
            self.primary.put_opts(location, bytes, options).await
        }
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        if location.primary_only() {
            return self.primary.put_multipart(location).await;
        }

        let (id, stream) = self.secondary.put_multipart(location).await?;

        let mirroring_upload = MirroringUpload::new(
            Pin::new(stream),
            self.primary.clone(),
            self.secondary.clone(),
            location.clone(),
        );

        Ok((id, Box::new(mirroring_upload)))
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        if location.primary_only() {
            return self.primary.abort_multipart(location, multipart_id).await;
        }

        self.secondary.abort_multipart(location, multipart_id).await
    }

    // Reads are routed to primary only
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.primary.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.primary.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        if !location.primary_only() {
            match self.secondary.delete(location).await {
                Err(Error::NotFound { .. }) | Ok(_) => {}
                Err(e) => return Err(e),
            }
        }
        self.primary.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.primary.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.primary.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        if to.primary_only() {
            self.primary.copy(from, to).await
        } else {
            self.secondary.copy(from, to).await?;
            self.primary.copy(from, to).await?;
            Ok(())
        }
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        if !to.primary_only() {
            self.secondary.copy(from, to).await?;
        }
        self.primary.copy_if_not_exists(from, to).await
    }
}

struct MirroringUpload {
    secondary_stream: Pin<Box<dyn AsyncWrite + Unpin + Send>>,

    primary_store: Arc<dyn ObjectStore>,
    secondary_store: Arc<dyn ObjectStore>,
    location: Path,

    state: MirroringUploadShutdown,
}

// The state goes from
// None
// -> (secondary)ShutingDown
// -> (secondary)ShutdownDone
// -> Uploading(to primary)
// -> Done
#[derive(Debug)]
enum MirroringUploadShutdown {
    None,
    ShutingDown,
    ShutdownDone,
    Uploading(Pin<Box<JoinHandle<()>>>),
    Completed,
}

impl MirroringUpload {
    pub fn new(
        secondary_stream: Pin<Box<dyn AsyncWrite + Unpin + Send>>,
        primary_store: Arc<dyn ObjectStore>,
        secondary_store: Arc<dyn ObjectStore>,
        location: Path,
    ) -> Self {
        Self {
            secondary_stream,
            primary_store,
            secondary_store,
            location,
            state: MirroringUploadShutdown::None,
        }
    }
}

impl AsyncWrite for MirroringUpload {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if !matches!(self.state, MirroringUploadShutdown::None) {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "already shutdown",
            )));
        }
        // Write to secondary first
        let mut_self = self.get_mut();
        mut_self.secondary_stream.as_mut().poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if !matches!(self.state, MirroringUploadShutdown::None) {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "already shutdown",
            )));
        }

        let mut_self = self.get_mut();
        mut_self.secondary_stream.as_mut().poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let mut_self = self.get_mut();

        loop {
            // try to shutdown secondary first
            match &mut mut_self.state {
                MirroringUploadShutdown::None | MirroringUploadShutdown::ShutingDown => {
                    match mut_self.secondary_stream.as_mut().poll_shutdown(cx) {
                        Poll::Ready(Ok(())) => {
                            mut_self.state = MirroringUploadShutdown::ShutdownDone;
                            // don't return, no waker is setup
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => {
                            mut_self.state = MirroringUploadShutdown::ShutingDown;
                            return Poll::Pending;
                        }
                    }
                }
                MirroringUploadShutdown::ShutdownDone => {
                    let primary_store = mut_self.primary_store.clone();
                    let secondary_store = mut_self.secondary_store.clone();
                    let location = mut_self.location.clone();

                    let upload_future =
                        Box::pin(tokio::runtime::Handle::current().spawn(async move {
                            let mut source =
                                secondary_store.get(&location).await.unwrap().into_stream();
                            let upload_stream = primary_store.put_multipart(&location).await;
                            let (_, mut stream) = upload_stream.unwrap();

                            while let Some(buf) = source.next().await {
                                let buf = buf.unwrap();
                                stream.write_all(&buf).await.unwrap();
                            }

                            stream.shutdown().await.unwrap();
                        }));
                    mut_self.state = MirroringUploadShutdown::Uploading(upload_future);
                    // don't return, no waker is setup
                }
                MirroringUploadShutdown::Uploading(ref mut join_handle) => {
                    match join_handle.poll_unpin(cx) {
                        Poll::Ready(Ok(())) => {
                            mut_self.state = MirroringUploadShutdown::Completed;
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Ready(Err(e)) => {
                            mut_self.state = MirroringUploadShutdown::Completed;
                            return Poll::Ready(Err(e.into()));
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }
                MirroringUploadShutdown::Completed => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "shutdown already completed",
                    )))
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct MirroringObjectStoreWrapper {
    secondary: Arc<dyn ObjectStore>,
}

impl MirroringObjectStoreWrapper {
    pub fn new(secondary: Arc<dyn ObjectStore>) -> Self {
        Self { secondary }
    }
}

impl WrappingObjectStore for MirroringObjectStoreWrapper {
    fn wrap(&self, primary: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(MirroringObjectStore {
            primary,
            secondary: self.secondary.clone(),
        })
    }
}

// windows pathing can't be simply concatenated
#[cfg(all(test, not(windows)))]
mod test {
    use super::*;
    use crate::Database;
    use arrow_array::PrimitiveArray;
    use futures::TryStreamExt;
    use lance::{dataset::WriteParams, io::object_store::ObjectStoreParams};
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32, RandomVector};
    use object_store::local::LocalFileSystem;
    use tempfile;

    #[tokio::test]
    async fn test_e2e() {
        let dir1 = tempfile::tempdir().unwrap().into_path();
        let dir2 = tempfile::tempdir().unwrap().into_path();

        let secondary_store = LocalFileSystem::new_with_prefix(dir2.to_str().unwrap()).unwrap();
        let object_store_wrapper = Arc::new(MirroringObjectStoreWrapper {
            secondary: Arc::new(secondary_store),
        });

        let db = Database::connect(dir1.to_str().unwrap()).await.unwrap();

        let mut param = WriteParams::default();
        let mut store_params = ObjectStoreParams::default();
        store_params.object_store_wrapper = Some(object_store_wrapper);
        param.store_params = Some(store_params);

        let mut datagen = BatchGenerator::new();
        datagen = datagen.col(Box::new(IncrementingInt32::default()));
        datagen = datagen.col(Box::new(RandomVector::default().named("vector".into())));

        let res = db
            .create_table("test", datagen.batch(100), Some(param.clone()))
            .await;

        // leave this here for easy debugging
        let t = res.unwrap();

        assert_eq!(t.count_rows().await.unwrap(), 100);

        let q = t
            .search(Some(PrimitiveArray::from_iter_values(vec![
                0.1, 0.1, 0.1, 0.1,
            ])))
            .limit(10)
            .execute()
            .await
            .unwrap();

        let bateches = q.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(bateches.len(), 1);
        assert_eq!(bateches[0].num_rows(), 10);

        use walkdir::WalkDir;

        let primary_location = dir1.join("test.lance").canonicalize().unwrap();
        let secondary_location = dir2.join(primary_location.strip_prefix("/").unwrap());

        let mut primary_iter = WalkDir::new(&primary_location).into_iter();
        let mut secondary_iter = WalkDir::new(&secondary_location).into_iter();

        let mut primary_elem = primary_iter.next();
        let mut secondary_elem = secondary_iter.next();

        loop {
            if primary_elem.is_none() && secondary_elem.is_none() {
                break;
            }
            // primary has more data then secondary, should not run out before secondary
            let primary_f = primary_elem.unwrap().unwrap();
            // hit manifest, skip, _versions contains all the manifest and should not exist on secondary
            let primary_raw_path = primary_f.file_name().to_str().unwrap();
            if primary_raw_path.contains("_latest.manifest") {
                primary_elem = primary_iter.next();
                continue;
            }
            let secondary_f = secondary_elem.unwrap().unwrap();
            assert_eq!(
                primary_f.path().strip_prefix(&primary_location),
                secondary_f.path().strip_prefix(&secondary_location)
            );

            primary_elem = primary_iter.next();
            secondary_elem = secondary_iter.next();
        }
    }
}
