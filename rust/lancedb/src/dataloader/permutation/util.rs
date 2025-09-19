// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{path::PathBuf, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::{Fields, Schema};
use datafusion_execution::disk_manager::DiskManagerMode;
use futures::TryStreamExt;
use rand::{rngs::SmallRng, RngCore, SeedableRng};
use tempfile::TempDir;

use crate::{
    arrow::{SendableRecordBatchStream, SimpleRecordBatchStream},
    Error, Result,
};

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
            Self::OsDefault => tempfile::tempdir(),
            Self::Specific(path) => tempfile::Builder::default().tempdir_in(path),
            Self::None => {
                return Err(Error::Runtime {
                    message: "No temporary directory was supplied and this operation requires spilling to disk".to_string(),
                });
            }
        }
        .map_err(|err| Error::Other {
            message: "Failed to create temporary directory".to_string(),
            source: Some(err.into()),
        })
    }

    pub fn to_disk_manager_mode(&self) -> DiskManagerMode {
        match self {
            Self::OsDefault => DiskManagerMode::OsTmpDirectory,
            Self::Specific(path) => DiskManagerMode::Directories(vec![path.clone()]),
            Self::None => DiskManagerMode::Disabled,
        }
    }
}

pub fn non_crypto_rng(seed: &Option<u64>) -> Box<dyn RngCore + Send> {
    Box::new(
        seed.as_ref()
            .map(|seed| SmallRng::seed_from_u64(*seed))
            .unwrap_or_else(SmallRng::from_os_rng),
    )
}

pub fn rename_column(
    stream: SendableRecordBatchStream,
    old_name: &str,
    new_name: &str,
) -> Result<SendableRecordBatchStream> {
    let schema = stream.schema();
    let field_index = schema.index_of(old_name)?;

    let new_fields = schema
        .fields
        .iter()
        .cloned()
        .enumerate()
        .map(|(idx, f)| {
            if idx == field_index {
                Arc::new(f.as_ref().clone().with_name(new_name))
            } else {
                f
            }
        })
        .collect::<Fields>();
    let new_schema = Arc::new(Schema::new(new_fields).with_metadata(schema.metadata().clone()));
    let new_schema_clone = new_schema.clone();

    let renamed_stream = stream.and_then(move |batch| {
        let renamed_batch =
            RecordBatch::try_new(new_schema.clone(), batch.columns().to_vec()).map_err(Error::from);
        std::future::ready(renamed_batch)
    });

    Ok(Box::pin(SimpleRecordBatchStream::new(
        renamed_stream,
        new_schema_clone,
    )))
}
