use std::path::PathBuf;

use datafusion_execution::disk_manager::DiskManagerMode;
use rand::{RngCore, SeedableRng};
use tempfile::TempDir;

use crate::{Error, Result};

/// Directory to use for temporary files
#[derive(Debug, Clone, Default)]
pub enum TemporaryDirectory {
    /// Use the operating system's default temporary directory (e.g. /tmp)
    #[default]
    OsDefault,
    /// Use the specified directory (must be an absolute path)
    Specific(PathBuf),
    /// If spilling is required, then error out
    None,
}

impl TemporaryDirectory {
    pub fn create_temp_dir(&self) -> Result<TempDir> {
        match self {
            TemporaryDirectory::OsDefault => tempfile::tempdir(),
            TemporaryDirectory::Specific(path) => tempfile::Builder::default().tempdir_in(path),
            TemporaryDirectory::None => {
                return Err(Error::Runtime {
                    message: format!(
                        "No temporary directory was supplied and this operation requires spilling to disk"
                    ),
                });
            }
        }
        .map_err(|err| Error::Other {
            message: format!("Failed to create temporary directory"),
            source: Some(err.into()),
        })
    }

    pub fn to_disk_manager_mode(&self) -> DiskManagerMode {
        match self {
            TemporaryDirectory::OsDefault => DiskManagerMode::OsTmpDirectory,
            TemporaryDirectory::Specific(path) => DiskManagerMode::Directories(vec![path.clone()]),
            TemporaryDirectory::None => DiskManagerMode::Disabled,
        }
    }
}

pub fn non_crypto_rng(seed: &Option<u64>) -> Box<dyn RngCore + Send> {
    Box::new(
        seed.as_ref()
            .map(|seed| rand_xoshiro::Xoshiro256Plus::seed_from_u64(*seed))
            .unwrap_or_else(|| rand_xoshiro::Xoshiro256Plus::from_rng(&mut rand::rng())),
    )
}
