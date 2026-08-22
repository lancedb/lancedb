// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Windows local filesystem compatibility for atomic manifest commits.

use std::ffi::OsStr;
use std::fmt::{Display, Formatter};
use std::os::windows::ffi::OsStrExt;
use std::path::{Path as StdPath, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use lance::io::WrappingObjectStore;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    RenameTargetMode, Result, UploadPart, path::Path,
};
use windows_sys::Win32::Foundation::{ERROR_ALREADY_EXISTS, ERROR_FILE_EXISTS};
use windows_sys::Win32::Storage::FileSystem::MoveFileExW;

const STORE_NAME: &str = "WindowsLocalFileSystem";

/// Uses the Windows move primitive for create-only renames on local stores.
///
/// `object_store` implements create-only local renames with a hard link followed
/// by a delete. Some Windows filesystems do not support hard links, but
/// `MoveFileExW` without `MOVEFILE_REPLACE_EXISTING` provides the same atomic
/// create-only rename semantics without requiring them.
#[derive(Debug, Default)]
pub struct WindowsLocalFileSystemWrapper;

impl WrappingObjectStore for WindowsLocalFileSystemWrapper {
    fn wrap(&self, _store_prefix: &str, target: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(WindowsLocalFileSystem { target })
    }
}

#[derive(Debug)]
struct WindowsLocalFileSystem {
    target: Arc<dyn ObjectStore>,
}

impl Display for WindowsLocalFileSystem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{STORE_NAME}({})", self.target)
    }
}

#[async_trait::async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for WindowsLocalFileSystem {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.target.put_opts(location, bytes, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.target.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.target.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> Result<Vec<Bytes>> {
        self.target.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.target.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.target.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.target.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.target.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.target.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        if options.target_mode != RenameTargetMode::Create {
            return self.target.rename_opts(from, to, options).await;
        }

        let from = PathBuf::from(from.as_ref());
        let to = PathBuf::from(to.as_ref());
        tokio::task::spawn_blocking(move || move_file_if_not_exists(&from, &to))
            .await
            .map_err(|source| Error::Generic {
                store: STORE_NAME,
                source: Box::new(source),
            })?
    }
}

fn move_file_if_not_exists(from: &StdPath, to: &StdPath) -> Result<()> {
    let from_wide = null_terminated_wide(from.as_os_str());
    let to_wide = null_terminated_wide(to.as_os_str());

    // SAFETY: both pointers reference null-terminated UTF-16 buffers that remain
    // alive for the duration of this call. A zero flag value deliberately omits
    // MOVEFILE_REPLACE_EXISTING, giving this operation create-only semantics.
    if unsafe { MoveFileExW(from_wide.as_ptr(), to_wide.as_ptr(), 0) } != 0 {
        return Ok(());
    }

    let source = std::io::Error::last_os_error();
    let path = to.to_string_lossy().into_owned();
    match source.raw_os_error().map(|code| code as u32) {
        Some(ERROR_ALREADY_EXISTS | ERROR_FILE_EXISTS) => Err(Error::AlreadyExists {
            path,
            source: Box::new(source),
        }),
        _ if source.kind() == std::io::ErrorKind::NotFound => Err(Error::NotFound {
            path,
            source: Box::new(source),
        }),
        _ => Err(Error::Generic {
            store: STORE_NAME,
            source: Box::new(source),
        }),
    }
}

fn null_terminated_wide(value: &OsStr) -> Vec<u16> {
    value.encode_wide().chain(Some(0)).collect()
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn create_only_rename_does_not_use_hard_links() {
        let tempdir = tempfile::tempdir().unwrap();
        let source_path = tempdir.path().join("staged.manifest");
        let destination_path = tempdir.path().join("1.manifest");
        std::fs::write(&source_path, b"manifest").unwrap();

        let source = Path::from_absolute_path(&source_path).unwrap();
        let destination = Path::from_absolute_path(&destination_path).unwrap();
        let store = WindowsLocalFileSystem {
            // The source does not exist in this inner store. Delegating the
            // rename would fail, proving the wrapper uses the native move path.
            target: Arc::new(InMemory::new()),
        };

        store
            .rename_opts(
                &source,
                &destination,
                RenameOptions::new().with_target_mode(RenameTargetMode::Create),
            )
            .await
            .unwrap();

        assert!(!source_path.exists());
        assert_eq!(std::fs::read(destination_path).unwrap(), b"manifest");
    }

    #[tokio::test]
    async fn create_only_rename_preserves_existing_destination() {
        let tempdir = tempfile::tempdir().unwrap();
        let source_path = tempdir.path().join("staged.manifest");
        let destination_path = tempdir.path().join("1.manifest");
        std::fs::write(&source_path, b"new manifest").unwrap();
        std::fs::write(&destination_path, b"existing manifest").unwrap();

        let source = Path::from_absolute_path(&source_path).unwrap();
        let destination = Path::from_absolute_path(&destination_path).unwrap();
        let store = WindowsLocalFileSystem {
            target: Arc::new(InMemory::new()),
        };

        let error = store
            .rename_opts(
                &source,
                &destination,
                RenameOptions::new().with_target_mode(RenameTargetMode::Create),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, Error::AlreadyExists { .. }));
        assert_eq!(std::fs::read(source_path).unwrap(), b"new manifest");
        assert_eq!(
            std::fs::read(destination_path).unwrap(),
            b"existing manifest"
        );
    }
}
