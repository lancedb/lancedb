// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Table-level unenforced primary key support.
//!
//! [`set_unenforced_primary_key`] records an ordered list of columns as the
//! table's primary key by writing Lance schema field metadata. "Unenforced"
//! means LanceDB does not check uniqueness on write; the key is metadata for
//! features such as `merge_insert` to consume.

use std::collections::{HashMap, HashSet};

use arrow_schema::DataType;
use lance_core::datatypes::{LANCE_UNENFORCED_PRIMARY_KEY, LANCE_UNENFORCED_PRIMARY_KEY_POSITION};

use crate::error::{Error, Result};
use crate::table::NativeTable;

/// Set the unenforced primary key on `table` to the given ordered list of
/// columns, replacing any previously-set key.
///
/// Each column must exist on the table and have a supported dtype. Columns
/// that were part of the previous key but are not in `columns` have their
/// primary-key metadata cleared. Order is preserved by writing 1-indexed
/// position metadata.
pub(super) async fn set_unenforced_primary_key(
    table: &NativeTable,
    columns: &[&str],
) -> Result<()> {
    table.dataset.ensure_mutable()?;

    if columns.is_empty() {
        return Err(Error::InvalidInput {
            message: "set_unenforced_primary_key: at least one column is required".into(),
        });
    }
    let mut seen = HashSet::new();
    for c in columns {
        if !seen.insert(*c) {
            return Err(Error::InvalidInput {
                message: format!(
                    "set_unenforced_primary_key: duplicate column '{}' in input",
                    c
                ),
            });
        }
    }

    let updates = {
        let dataset = table.dataset.get().await?;
        let schema = dataset.schema();

        let mut updates: Vec<(u32, HashMap<String, String>)> = Vec::new();
        let mut new_pk_field_ids = HashSet::new();

        // Lance's `Schema::unenforced_primary_key` puts fields with
        // position 0 at the *end* of the list (treating 0 as a legacy
        // "no specific position" fallback). To preserve the caller's
        // order, position metadata is 1-indexed.
        for (idx, name) in columns.iter().enumerate() {
            let field = schema.field(name).ok_or_else(|| Error::InvalidInput {
                message: format!(
                    "set_unenforced_primary_key: column '{}' not found on table",
                    name
                ),
            })?;
            if !is_supported_pk_dtype(&field.data_type()) {
                return Err(Error::InvalidInput {
                    message: format!(
                        "set_unenforced_primary_key: column '{}' has dtype {:?} which is not supported as a primary key. Supported: Int32, Int64, Utf8, LargeUtf8, Binary, LargeBinary, FixedSizeBinary",
                        name,
                        field.data_type()
                    ),
                });
            }
            new_pk_field_ids.insert(field.id);

            let mut metadata = field.metadata.clone();
            metadata.remove(LANCE_UNENFORCED_PRIMARY_KEY);
            metadata.insert(
                LANCE_UNENFORCED_PRIMARY_KEY_POSITION.to_string(),
                (idx + 1).to_string(),
            );
            updates.push((field_id_to_u32(field.id, &field.name)?, metadata));
        }

        // Clear PK metadata from columns that are no longer part of the key.
        for f in schema.unenforced_primary_key() {
            if new_pk_field_ids.contains(&f.id) {
                continue;
            }
            let mut metadata = f.metadata.clone();
            metadata.remove(LANCE_UNENFORCED_PRIMARY_KEY);
            metadata.remove(LANCE_UNENFORCED_PRIMARY_KEY_POSITION);
            updates.push((field_id_to_u32(f.id, &f.name)?, metadata));
        }

        updates
    };

    let mut dataset = (*table.dataset.get().await?).clone();
    dataset.replace_field_metadata(updates).await?;
    table.dataset.update(dataset);
    Ok(())
}

fn field_id_to_u32(id: i32, name: &str) -> Result<u32> {
    u32::try_from(id).map_err(|_| Error::Runtime {
        message: format!(
            "internal: field '{}' has unexpected negative field id {}",
            name, id
        ),
    })
}

fn is_supported_pk_dtype(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Int32
            | DataType::Int64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
    )
}
