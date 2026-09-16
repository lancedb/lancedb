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

use arrow_schema::DataType;
use lance_core::datatypes::{LANCE_UNENFORCED_PRIMARY_KEY, LANCE_UNENFORCED_PRIMARY_KEY_POSITION};
use lance_core::utils::parse::str_is_truthy;

use crate::error::{Error, Result};
use crate::table::NativeTable;

/// Set the unenforced primary key on `table` to the single column in `columns`.
///
/// Fails if `columns` is not exactly one column (compound primary keys are not
/// supported), if the column does not exist or has an unsupported dtype, or if
/// the table already has an unenforced primary key (changing the primary key
/// is not supported).
pub(super) async fn set_unenforced_primary_key(
    table: &NativeTable,
    columns: &[&str],
) -> Result<()> {
    table.dataset.ensure_mutable()?;

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

    let updates = {
        let dataset = table.dataset.get().await?;
        let schema = dataset.schema();

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

        // Position metadata is 1-indexed; `Schema::unenforced_primary_key`
        // treats position 0 as a legacy "no specific position" fallback.
        let mut metadata = field.metadata.clone();
        metadata.remove(LANCE_UNENFORCED_PRIMARY_KEY);
        metadata.insert(
            LANCE_UNENFORCED_PRIMARY_KEY_POSITION.to_string(),
            "1".to_string(),
        );
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

/// The unenforced primary key columns declared in `schema`, in key order.
///
/// Reads the field metadata directly rather than going through
/// [`lance_core::datatypes::Schema::unenforced_primary_key`], because a remote
/// table only ever has the Arrow schema `describe` returned — there is no Lance
/// schema, and no field ids, on that side of the wire.
///
/// Ordering mirrors Lance's: fields with an explicit 1-based position first, in
/// position order, then fields carrying only the legacy boolean marker, in
/// declaration order. Only top-level fields are considered, which is every
/// field a supported key dtype can live on.
pub fn pk_columns(schema: &arrow_schema::Schema) -> Vec<String> {
    let mut keyed: Vec<(bool, u32, usize, &str)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            let metadata = field.metadata();
            let position = metadata
                .get(LANCE_UNENFORCED_PRIMARY_KEY_POSITION)
                .and_then(|value| value.parse::<u32>().ok())
                .or_else(|| {
                    metadata
                        .get(LANCE_UNENFORCED_PRIMARY_KEY)
                        .filter(|value| str_is_truthy(value))
                        .map(|_| 0)
                })?;
            Some((position == 0, position, idx, field.name().as_str()))
        })
        .collect();
    keyed.sort_unstable();
    keyed
        .into_iter()
        .map(|(_, _, _, name)| name.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_schema::{Field, Schema};

    use super::*;

    fn field(name: &str, metadata: &[(&str, &str)]) -> Field {
        Field::new(name, DataType::Utf8, false).with_metadata(
            metadata
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
        )
    }

    #[test]
    fn pk_columns_reads_the_position_marker() {
        let schema = Schema::new(vec![
            field("other", &[]),
            field("id", &[(LANCE_UNENFORCED_PRIMARY_KEY_POSITION, "1")]),
        ]);
        assert_eq!(pk_columns(&schema), vec!["id".to_string()]);
    }

    /// Key order is the position, not the order the columns are declared in.
    #[test]
    fn pk_columns_orders_a_composite_key_by_position() {
        let schema = Schema::new(vec![
            field("id", &[(LANCE_UNENFORCED_PRIMARY_KEY_POSITION, "2")]),
            field("tenant", &[(LANCE_UNENFORCED_PRIMARY_KEY_POSITION, "1")]),
        ]);
        assert_eq!(
            pk_columns(&schema),
            vec!["tenant".to_string(), "id".to_string()]
        );
    }

    /// Datasets written before the position marker carry only the boolean.
    #[test]
    fn pk_columns_accepts_the_legacy_boolean_marker() {
        let schema = Schema::new(vec![field("id", &[(LANCE_UNENFORCED_PRIMARY_KEY, "true")])]);
        assert_eq!(pk_columns(&schema), vec!["id".to_string()]);
    }

    /// Mirrors Lance: an explicit position sorts ahead of a legacy field.
    #[test]
    fn pk_columns_sorts_positioned_fields_ahead_of_legacy_ones() {
        let schema = Schema::new(vec![
            field("legacy", &[(LANCE_UNENFORCED_PRIMARY_KEY, "yes")]),
            field(
                "positioned",
                &[(LANCE_UNENFORCED_PRIMARY_KEY_POSITION, "1")],
            ),
        ]);
        assert_eq!(
            pk_columns(&schema),
            vec!["positioned".to_string(), "legacy".to_string()]
        );
    }

    #[test]
    fn pk_columns_is_empty_without_a_key() {
        let schema = Schema::new(vec![
            field("a", &[]),
            field("b", &[(LANCE_UNENFORCED_PRIMARY_KEY, "false")]),
        ]);
        assert!(pk_columns(&schema).is_empty());
    }
}
