// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Table-level unenforced primary key support.
//!
//! [`set_unenforced_primary_key`] records a column as the table's primary key
//! by writing Lance schema field metadata. "Unenforced" means LanceDB does not
//! check uniqueness on write; the key is metadata for features such as
//! `merge_insert` to consume.
//!
//! Only a single-column primary key is supported, and the key cannot be
//! changed once set.

use std::collections::HashMap;

use arrow_schema::DataType;
use lance_core::datatypes::{
    Field as LanceField, LANCE_UNENFORCED_PRIMARY_KEY, LANCE_UNENFORCED_PRIMARY_KEY_POSITION,
    Schema as LanceSchema,
};

use crate::error::{Error, Result};
use crate::table::NativeTable;

/// Validate a `set_unenforced_primary_key` request against `schema`, returning
/// the field the key would be installed on.
///
/// Shared by [`NativeTable`] and the remote table so both reject the same
/// requests with the same messages. Fails if `columns` is not exactly one
/// column (compound primary keys are not supported), if the column does not
/// exist or has an unsupported dtype, or if the table already has an
/// unenforced primary key (changing the primary key is not supported).
pub fn validate<'a>(schema: &'a LanceSchema, columns: &[&str]) -> Result<&'a LanceField> {
    if columns.is_empty() {
        return Err(Error::InvalidInput {
            message: "set_unenforced_primary_key: a column is required".into(),
        });
    }
    if columns.len() > 1 {
        return Err(Error::InvalidInput {
            message: format!(
                "set_unenforced_primary_key: compound primary keys are not supported, got {} columns",
                columns.len()
            ),
        });
    }
    let column = columns[0];

    // The primary key is immutable once set. The Lance commit layer is the
    // source of truth for this (it also covers the concurrent-writer race);
    // this check just fails fast with a clear message.
    if !schema.unenforced_primary_key().is_empty() {
        return Err(Error::InvalidInput {
            message: "set_unenforced_primary_key: an unenforced primary key is already set on this table; changing it is not supported".into(),
        });
    }

    let field = schema.field(column).ok_or_else(|| Error::InvalidInput {
        message: format!(
            "set_unenforced_primary_key: column '{}' not found on table",
            column
        ),
    })?;
    if !is_supported_pk_dtype(&field.data_type()) {
        return Err(Error::InvalidInput {
            message: format!(
                "set_unenforced_primary_key: column '{}' has dtype {:?} which is not supported as a primary key. Supported: Int32, Int64, Utf8, LargeUtf8, Binary, LargeBinary, FixedSizeBinary",
                column,
                field.data_type()
            ),
        });
    }
    Ok(field)
}

/// The field metadata edit that installs the primary key on a field: keys to
/// set (`Some`) or delete (`None`).
///
/// Position metadata is 1-indexed; `Schema::unenforced_primary_key` treats
/// position 0 as a legacy "no specific position" fallback, so the legacy
/// boolean key is cleared and only the position governs.
pub fn install_edit() -> HashMap<String, Option<String>> {
    HashMap::from([
        (LANCE_UNENFORCED_PRIMARY_KEY.to_string(), None),
        (
            LANCE_UNENFORCED_PRIMARY_KEY_POSITION.to_string(),
            Some("1".to_string()),
        ),
    ])
}

/// Set the unenforced primary key on `table` to the single column in `columns`.
pub(super) async fn set_unenforced_primary_key(
    table: &NativeTable,
    columns: &[&str],
) -> Result<()> {
    table.dataset.ensure_mutable()?;

    let updates = {
        let dataset = table.dataset.get().await?;
        let field = validate(dataset.schema(), columns)?;

        let mut metadata = field.metadata.clone();
        for (key, value) in install_edit() {
            match value {
                Some(value) => {
                    metadata.insert(key, value);
                }
                None => {
                    metadata.remove(&key);
                }
            }
        }
        vec![(field_id_to_u32(field.id, &field.name)?, metadata)]
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
