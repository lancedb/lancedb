// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! MemWAL LSM write-path spec management.
//!
//! [`set_lsm_write_spec`] installs a [`super::super::LsmWriteSpec`] on a
//! table, which selects Lance's MemWAL LSM-style write path for future
//! `merge_insert` calls. [`unset_lsm_write_spec`] removes it. The actual
//! `merge_insert` dispatch and writer are a follow-up.

use lance::dataset::mem_wal::DatasetMemWalExt;
use lance::index::DatasetIndexExt;

use crate::error::{Error, Result};
use crate::table::{LsmWriteSpec, NativeTable};

// =============================================================================
// set_lsm_write_spec
// =============================================================================

/// Install an [`LsmWriteSpec`] on the table.
///
/// The bucket / unsharded sharding spec is constructed and validated by Lance's
/// [`InitializeMemWalBuilder`](lance::dataset::mem_wal::InitializeMemWalBuilder).
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn set_lsm_write_spec(table: &NativeTable, spec: LsmWriteSpec) -> Result<()> {
    table.dataset.ensure_mutable()?;

    {
        let dataset = table.dataset.get().await?;
        if dataset.mem_wal_index_details().await?.is_some() {
            return Err(Error::InvalidInput {
                message: "set_lsm_write_spec: an LSM write spec is already set on this table; mutation is not supported".into(),
            });
        }
    }

    let mut dataset = (*table.dataset.get().await?).clone();
    let mut builder = dataset.initialize_mem_wal();
    let (maintained_indexes, writer_config_defaults) = match spec {
        LsmWriteSpec::Bucket {
            column,
            num_buckets,
            maintained_indexes,
            writer_config_defaults,
        } => {
            builder = builder.bucket_sharding(column, num_buckets);
            (maintained_indexes, writer_config_defaults)
        }
        LsmWriteSpec::Identity {
            column,
            maintained_indexes,
            writer_config_defaults,
        } => {
            builder = builder.identity_sharding(column);
            (maintained_indexes, writer_config_defaults)
        }
        LsmWriteSpec::Unsharded {
            maintained_indexes,
            writer_config_defaults,
        } => {
            builder = builder.unsharded();
            (maintained_indexes, writer_config_defaults)
        }
    };
    builder = builder.maintained_indexes(maintained_indexes);
    for (key, value) in writer_config_defaults {
        builder = builder.add_writer_config_default(key, value);
    }
    builder.execute().await?;
    table.dataset.update(dataset);
    Ok(())
}

// =============================================================================
// unset_lsm_write_spec
// =============================================================================

/// Remove the [`LsmWriteSpec`] from the table by dropping the MemWAL index.
///
/// Errors if no spec is currently set.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn unset_lsm_write_spec(table: &NativeTable) -> Result<()> {
    table.dataset.ensure_mutable()?;

    {
        let dataset = table.dataset.get().await?;
        if dataset.mem_wal_index_details().await?.is_none() {
            return Err(Error::InvalidInput {
                message: "unset_lsm_write_spec: no LSM write spec is set on this table".into(),
            });
        }
    }

    let mut dataset = (*table.dataset.get().await?).clone();
    dataset
        .drop_index(lance_index::mem_wal::MEM_WAL_INDEX_NAME)
        .await?;
    table.dataset.update(dataset);
    Ok(())
}
