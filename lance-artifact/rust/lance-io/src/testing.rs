// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
use std::fmt::{self, Display, Formatter};

use async_trait::async_trait;
use futures::stream::BoxStream;
use mockall::mock;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore as OSObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as OSResult, path::Path,
};
use std::future::Future;

mock! {
    pub ObjectStore {}

    #[async_trait]
    impl OSObjectStore for ObjectStore {
        async fn put_opts(&self, location: &Path, bytes: PutPayload, opts: PutOptions) -> OSResult<PutResult>;
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OSResult<Box<dyn MultipartUpload>>;
        fn get_opts<'life0, 'life1, 'async_trait>(
            &'life0 self,
            location: &'life1 Path,
            options: GetOptions
        ) -> std::pin::Pin<Box<dyn Future<Output=OSResult<GetResult> > +Send+'async_trait> > where
        Self: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait;
        fn delete_stream(&self, locations: BoxStream<'static, OSResult<Path>>) -> BoxStream<'static, OSResult<Path>>;
        fn list<'a>(&'a self, prefix: Option<&'a Path>) -> BoxStream<'_, OSResult<ObjectMeta>>;
        async fn list_with_delimiter<'a, 'b>(&'a self, prefix: Option<&'b Path>) -> OSResult<ListResult>;
        async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> OSResult<()>;
    }
}

impl std::fmt::Debug for MockObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "MockObjectStore")
    }
}

impl Display for MockObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "MockObjectStore")
    }
}
