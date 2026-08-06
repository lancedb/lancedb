// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Utility functions for creating temporary files and directories.
//!
//! Most of these types wrap around the `tempfile` crate.  We add two
//! additional features:
//!
//! * There are wrappers around temporary directories and files that expose
//!   the dir/file as an object store path, std path, or string, which can save
//!   some boilerplate.
//! * We work around a current bug in the `url` crate which fails to parse
//!   Windows paths like `C:\` correctly.  We do so by replacing all `\` with
//!   `/` in the path.  This is not safe in general (e.g. paths may use `\` as
//!   an escape) but it should be safe for temporary paths.

use object_store::path::Path as ObjPath;
use std::{
    ops::Deref,
    path::{Path as StdPath, PathBuf},
};
use tempfile::NamedTempFile;

use crate::Result;

/// A temporary directory
///
/// This create a temporary directory using [`tempfile::tempdir`].  It will
/// generally be cleaned up when the object is dropped.
///
/// This type is primarily useful when you need multiple representations (string,
/// path, object store path) of the same temporary directory.  If you only need
/// a single representation you can use the [`TempStdDir`], [`TempStrDir`], or
/// [`TempObjDir`] types.
#[derive(Debug)]
pub struct TempDir {
    tempdir: tempfile::TempDir,
}

impl TempDir {
    fn new() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        Self { tempdir }
    }

    /// Create a temporary directory, exposing any potential errors.
    ///
    /// For most test cases you should use the [`Default`] implementation instead.
    /// However, when we use this type in production code, we may want to return
    /// errors gracefully instead of panicking.
    pub fn try_new() -> Result<Self> {
        let tempdir = tempfile::tempdir()?;
        Ok(Self { tempdir })
    }

    /// Get the path as a string
    ///
    /// This path will be safe to use as a URI on Windows
    pub fn path_str(&self) -> String {
        if cfg!(windows) {
            self.tempdir.path().to_str().unwrap().replace("\\", "/")
        } else {
            self.tempdir.path().to_str().unwrap().to_owned()
        }
    }

    /// Get the path as a standard library path
    ///
    /// If you convert this to a string, it will NOT be safe to use as a URI on Windows.
    /// Use [`TempDir::path_str`] instead.
    ///
    /// It is safe to use this as a standard path on Windows.
    pub fn std_path(&self) -> &StdPath {
        self.tempdir.path()
    }

    /// Get the path as an object store path
    ///
    /// This path will be safe to use as a URI on Windows
    pub fn obj_path(&self) -> ObjPath {
        ObjPath::parse(self.path_str()).unwrap()
    }
}

impl Default for TempDir {
    fn default() -> Self {
        Self::new()
    }
}

/// A temporary directory that is exposed as an object store path
///
/// This is a wrapper around [`TempDir`] that exposes the path as an object store path.
/// It is useful when you need to create a temporary directory that is only
/// used as an object store path.
pub struct TempObjDir {
    _tempdir: TempDir,
    path: ObjPath,
}

impl Deref for TempObjDir {
    type Target = ObjPath;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl AsRef<ObjPath> for TempObjDir {
    fn as_ref(&self) -> &ObjPath {
        &self.path
    }
}

impl Default for TempObjDir {
    fn default() -> Self {
        let tempdir = TempDir::default();
        let path = tempdir.obj_path();
        Self {
            _tempdir: tempdir,
            path,
        }
    }
}

/// A temporary directory that is exposed as a string
///
/// This is a wrapper around [`TempDir`] that exposes the path as a string.
/// It is useful when you need to create a temporary directory that is only
/// used as a string.
pub struct TempStrDir {
    _tempdir: TempDir,
    string: String,
}

impl std::fmt::Display for TempStrDir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.string.fmt(f)
    }
}

impl TempStrDir {
    /// Create a cloned copy of the string that can be used if `Into<String>` is needed
    pub fn as_into_string(&self) -> impl Into<String> {
        self.string.clone()
    }
}

impl Default for TempStrDir {
    fn default() -> Self {
        let tempdir = TempDir::default();
        let string = tempdir.path_str();
        Self {
            _tempdir: tempdir,
            string,
        }
    }
}

impl Deref for TempStrDir {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.string
    }
}

impl AsRef<str> for TempStrDir {
    fn as_ref(&self) -> &str {
        self.string.as_ref()
    }
}

/// A temporary directory that is exposed as a standard library path
///
/// This is a wrapper around [`TempDir`] that exposes the path as a standard library path.
/// It is useful when you need to create a temporary directory that is only
/// used as a standard library path.
#[derive(Default)]
pub struct TempStdDir {
    tempdir: TempDir,
}

impl AsRef<StdPath> for TempStdDir {
    fn as_ref(&self) -> &StdPath {
        self.tempdir.std_path()
    }
}

impl Deref for TempStdDir {
    type Target = StdPath;

    fn deref(&self) -> &Self::Target {
        self.tempdir.std_path()
    }
}

/// A temporary file
///
/// This is a wrapper around [`tempfile::NamedTempFile`].  The file will normally be cleaned
/// up when the object is dropped.
///
/// Note: this function may create an empty file when the object is created.  If you are checking
/// that the path does not exist, you should use [`TempStdPath`] instead.
pub struct TempFile {
    temppath: NamedTempFile,
}

impl TempFile {
    fn new() -> Self {
        let temppath = tempfile::NamedTempFile::new().unwrap();
        Self { temppath }
    }

    /// Get the path as a string safe to use as a URI on Windows.
    pub fn path_str(&self) -> String {
        if cfg!(windows) {
            self.temppath.path().to_str().unwrap().replace("\\", "/")
        } else {
            self.temppath.path().to_str().unwrap().to_owned()
        }
    }

    /// Get the path as a standard library path
    ///
    /// If you convert this to a string, it will NOT be safe to use as a URI on Windows.
    /// Use [`TempFile::path_str`] instead.
    ///
    /// It is safe to use this as a standard path on Windows.
    pub fn std_path(&self) -> &StdPath {
        self.temppath.path()
    }

    /// Get the path as an object store path
    ///
    /// This path will be safe to use as a URI on Windows
    pub fn obj_path(&self) -> ObjPath {
        ObjPath::parse(self.path_str()).unwrap()
    }
}

impl Default for TempFile {
    fn default() -> Self {
        Self::new()
    }
}

/// A temporary file that is exposed as a standard library path
///
/// This is a wrapper around [`TempFile`] that exposes the path as a standard library path.
/// It is useful when you need to create a temporary file that is only used as a standard library path.
#[derive(Default)]
pub struct TempStdFile {
    tempfile: TempFile,
}

impl AsRef<StdPath> for TempStdFile {
    fn as_ref(&self) -> &StdPath {
        self.tempfile.std_path()
    }
}

impl Deref for TempStdFile {
    type Target = StdPath;

    fn deref(&self) -> &Self::Target {
        self.tempfile.std_path()
    }
}

/// A unique path to a temporary file, exposed as an object store path
///
/// Unlike [`TempFile`], this does not create an empty file. We create a
/// temporary directory and then construct a path inside it, following the
/// same pattern as [`TempStdPath`]. This avoids holding an open file handle,
/// which on Windows would prevent atomic renames to the same path.
pub struct TempObjFile {
    _tempdir: TempDir,
    path: ObjPath,
}

impl AsRef<ObjPath> for TempObjFile {
    fn as_ref(&self) -> &ObjPath {
        &self.path
    }
}

impl std::ops::Deref for TempObjFile {
    type Target = ObjPath;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl Default for TempObjFile {
    fn default() -> Self {
        let tempdir = TempDir::default();
        let path = ObjPath::parse(format!("{}/some_file", tempdir.path_str())).unwrap();
        Self {
            _tempdir: tempdir,
            path,
        }
    }
}

/// Get a unique path to a temporary file
///
/// Unlike [`TempFile`], this function will not create an empty file.  We create
/// a temporary directory and then create a path inside of it.  Since the temporary
/// directory is created first, we can be confident that the path is unique.
///
/// This path will be safe to use as a URI on Windows
pub struct TempStdPath {
    _tempdir: TempDir,
    path: PathBuf,
}

impl Default for TempStdPath {
    fn default() -> Self {
        let tempdir = TempDir::default();
        let path = format!("{}/some_file", tempdir.path_str());
        let path = PathBuf::from(path);
        Self {
            _tempdir: tempdir,
            path,
        }
    }
}

impl Deref for TempStdPath {
    type Target = PathBuf;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl AsRef<StdPath> for TempStdPath {
    fn as_ref(&self) -> &StdPath {
        self.path.as_path()
    }
}
