// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::cli::LanceFileMetaArgs;
use lance_core::Result;
use lance_file::reader::{CachedFileMetadata, FileReader};
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::utils::CachedFileSize;
use std::fmt;
use std::fmt::Formatter;

pub struct LanceToolFileMetadata {
    file_metadata: CachedFileMetadata,
}

impl fmt::Display for LanceToolFileMetadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "version: {}", self.file_metadata.version())?;
        writeln!(f, "num_rows: {}", self.file_metadata.num_rows)?;
        writeln!(f, "num_data_bytes: {}", self.file_metadata.num_data_bytes)?;
        writeln!(
            f,
            "num_column_metadata_bytes: {}",
            self.file_metadata.num_column_metadata_bytes
        )?;
        writeln!(
            f,
            "num_footer_bytes: {}",
            self.file_metadata.num_footer_bytes
        )?;
        writeln!(f, "schema:")?;
        write!(f, "{}", self.file_metadata.file_schema)?;
        Ok(())
    }
}

impl LanceToolFileMetadata {
    async fn open(source: &String) -> Result<Self> {
        let (object_store, path) = crate::util::get_object_store_and_path(source).await?;
        let scan_scheduler =
            ScanScheduler::new(object_store, SchedulerConfig::new(2 * 1024 * 1024 * 1024));
        let file_scheduler = scan_scheduler
            .open_file(&path, &CachedFileSize::unknown())
            .await?;
        let file_metadata = FileReader::read_all_metadata(&file_scheduler).await?;
        let lance_tool_file_metadata = Self { file_metadata };
        Ok(lance_tool_file_metadata)
    }
}

pub(crate) async fn show_file_meta(
    mut writer: impl std::io::Write,
    args: &LanceFileMetaArgs,
) -> Result<()> {
    let metadata = LanceToolFileMetadata::open(&args.source).await?;
    write!(writer, "{}", metadata)?;
    Ok(())
}
