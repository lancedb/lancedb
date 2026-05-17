// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! MemWAL LSM write-path spec management.
//!
//! [`set_lsm_write_spec`] installs a [`super::super::LsmWriteSpec`] on a
//! table, which selects Lance's MemWAL LSM-style write path for future
//! `merge_insert` calls. [`unset_lsm_write_spec`] removes it. The actual
//! `merge_insert` dispatch and writer are a follow-up.

use std::collections::HashMap;

use lance::dataset::mem_wal::{DatasetMemWalExt, MemWalConfig, MemWalShardConfig};
use lance::index::DatasetIndexExt;
use lance_index::mem_wal::{ShardField, ShardSpec};

use crate::error::{Error, Result};
use crate::table::{LsmWriteSpec, NativeTable};

/// Spec id used for the single bucket-style shard spec we install.
const BUCKET_SHARD_SPEC_ID: u32 = 1;

/// Field id (within the shard spec) for the bucket-index value.
const BUCKET_SHARD_FIELD_ID: &str = "bucket";

/// Transform name used by [`LsmWriteSpec::Bucket`]. Matches Iceberg's
/// `bucket(col, N)` partition transform name.
const BUCKET_TRANSFORM: &str = "bucket";

/// Transform name used by [`LsmWriteSpec::Unsharded`]. Mirrors the
/// proposed Lance MemWAL spec name for "no partitioning"; every row maps
/// to a single shard.
const UNSHARDED_TRANSFORM: &str = "unsharded";

/// Parameter key holding the bucket count `N` on the bucket transform.
const BUCKET_NUM_BUCKETS_PARAM: &str = "num_buckets";

/// Inclusive upper bound for `num_buckets`. Bounds the number of distinct
/// MemWAL shards that can be addressed by a single bucket spec, which in
/// turn caps how many shard manifests Lance has to manage on this table.
const MAX_NUM_BUCKETS: u32 = 1024;

// =============================================================================
// set_lsm_write_spec
// =============================================================================

/// Install an [`LsmWriteSpec`] on the table.
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

    let (shard_spec, num_shards, maintained_indexes) = match spec {
        LsmWriteSpec::Bucket {
            column,
            num_buckets,
            maintained_indexes,
        } => {
            let (s, n) = build_bucket_shard_spec(table, &column, num_buckets).await?;
            (s, n, maintained_indexes)
        }
        LsmWriteSpec::Unsharded { maintained_indexes } => {
            (build_unsharded_shard_spec(), 1, maintained_indexes)
        }
    };

    let mut dataset = (*table.dataset.get().await?).clone();
    dataset
        .initialize_mem_wal_with_shards(
            MemWalConfig {
                shard_spec: Some(shard_spec),
                maintained_indexes,
            },
            MemWalShardConfig { num_shards },
        )
        .await?;
    table.dataset.update(dataset);
    Ok(())
}

async fn build_bucket_shard_spec(
    table: &NativeTable,
    column: &str,
    num_buckets: u32,
) -> Result<(ShardSpec, u32)> {
    if num_buckets == 0 || num_buckets > MAX_NUM_BUCKETS {
        return Err(Error::InvalidInput {
            message: format!(
                "set_lsm_write_spec: num_buckets must be in [1, {}], got {}",
                MAX_NUM_BUCKETS, num_buckets
            ),
        });
    }

    let pk_field_id = {
        let dataset = table.dataset.get().await?;
        let schema = dataset.schema();

        let pk_fields = schema.unenforced_primary_key();
        let pk = match pk_fields.as_slice() {
            [single] => *single,
            [] => {
                return Err(Error::InvalidInput {
                    message: "set_lsm_write_spec: table has no unenforced primary key. Call set_unenforced_primary_key first.".into(),
                });
            }
            _ => {
                return Err(Error::InvalidInput {
                    message: "set_lsm_write_spec: bucket(...) currently requires a single-column unenforced primary key; use LsmWriteSpec::Unsharded for multi-column or no primary key".into(),
                });
            }
        };

        if pk.name != column {
            return Err(Error::InvalidInput {
                message: format!(
                    "set_lsm_write_spec: bucket column '{}' does not match the table's unenforced primary key column '{}'",
                    column, pk.name
                ),
            });
        }

        pk.id
    };

    let parameters = HashMap::from([(BUCKET_NUM_BUCKETS_PARAM.into(), num_buckets.to_string())]);
    let spec = ShardSpec {
        spec_id: BUCKET_SHARD_SPEC_ID,
        fields: vec![ShardField {
            field_id: BUCKET_SHARD_FIELD_ID.into(),
            source_ids: vec![pk_field_id],
            transform: Some(BUCKET_TRANSFORM.into()),
            expression: None,
            result_type: "int32".into(),
            parameters,
        }],
    };
    Ok((spec, num_buckets))
}

fn build_unsharded_shard_spec() -> ShardSpec {
    ShardSpec {
        spec_id: BUCKET_SHARD_SPEC_ID,
        fields: vec![ShardField {
            field_id: BUCKET_SHARD_FIELD_ID.into(),
            source_ids: vec![],
            transform: Some(UNSHARDED_TRANSFORM.into()),
            expression: None,
            result_type: "int32".into(),
            parameters: HashMap::new(),
        }],
    }
}

// =============================================================================
// unset_lsm_write_spec
// =============================================================================

/// Remove the [`LsmWriteSpec`] from the table by dropping the MemWAL index.
///
/// No-op if no spec is currently set.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn unset_lsm_write_spec(table: &NativeTable) -> Result<()> {
    table.dataset.ensure_mutable()?;

    {
        let dataset = table.dataset.get().await?;
        if dataset.mem_wal_index_details().await?.is_none() {
            return Ok(());
        }
    }

    let mut dataset = (*table.dataset.get().await?).clone();
    dataset
        .drop_index(lance_index::mem_wal::MEM_WAL_INDEX_NAME)
        .await?;
    table.dataset.update(dataset);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_shard_spec_parameters_only_num_buckets() {
        let spec = build_unsharded_shard_spec();
        assert_eq!(spec.fields.len(), 1);
        assert!(spec.fields[0].parameters.is_empty());
        assert_eq!(
            spec.fields[0].transform.as_deref(),
            Some(UNSHARDED_TRANSFORM)
        );
    }
}
