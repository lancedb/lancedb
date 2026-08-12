// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Dependency-contract test for Lance A4 schema metadata attachment (B4p).
//!
//! Pins the exact generic Lance API shape LanceDB B4 will consume:
//! [`SchemaMetadataUpdates`], [`UpdateMap`], [`UpdateMapEntry`],
//! [`Transaction::with_schema_metadata_updates`], and the public
//! `with_schema_metadata_updates` methods on insert/update/delete builders.
//!
//! Neutral metadata keys only. No Function / UDF / Job semantics.

use std::collections::HashMap;

use lance::Result;
use lance::dataset::transaction::{
    Operation, SchemaMetadataUpdates, Transaction, UpdateMap, UpdateMapEntry,
};
use lance::dataset::{DeleteBuilder, InsertBuilder, UpdateBuilder};
use lance_table::format::Fragment;

const FIELD_ID: i32 = 7;
const META_KEY: &str = "b4p.dependency.meta";
const META_VALUE: &str = "neutral-value";

fn field_metadata_patch() -> SchemaMetadataUpdates {
    SchemaMetadataUpdates {
        schema_metadata_updates: None,
        field_metadata_updates: HashMap::from([(
            FIELD_ID,
            UpdateMap {
                update_entries: vec![UpdateMapEntry {
                    key: META_KEY.to_string(),
                    value: Some(META_VALUE.to_string()),
                }],
                replace: false,
            },
        )]),
    }
}

/// Compile-time proof that InsertBuilder exposes the A4 attachment method.
#[allow(dead_code)]
fn typecheck_insert_builder_attachment<'a>(
    builder: InsertBuilder<'a>,
    updates: SchemaMetadataUpdates,
) -> Result<InsertBuilder<'a>> {
    builder.with_schema_metadata_updates(updates)
}

/// Compile-time proof that UpdateBuilder exposes the A4 attachment method.
#[allow(dead_code)]
fn typecheck_update_builder_attachment(
    builder: UpdateBuilder,
    updates: SchemaMetadataUpdates,
) -> Result<UpdateBuilder> {
    builder.with_schema_metadata_updates(updates)
}

/// Compile-time proof that DeleteBuilder exposes the A4 attachment method.
#[allow(dead_code)]
fn typecheck_delete_builder_attachment(
    builder: DeleteBuilder,
    updates: SchemaMetadataUpdates,
) -> Result<DeleteBuilder> {
    builder.with_schema_metadata_updates(updates)
}

#[test]
fn append_transaction_retains_schema_metadata_updates_patch() {
    let updates = field_metadata_patch();
    assert!(
        !updates.is_empty(),
        "fixture must be a substantive non-empty field metadata patch"
    );

    let transaction = Transaction::new(
        0,
        Operation::Append {
            fragments: vec![Fragment::new(1)],
        },
        None,
    )
    .with_schema_metadata_updates(updates.clone())
    .expect("non-empty field metadata patch must attach to Append");

    assert_eq!(transaction.schema_metadata_updates.as_ref(), Some(&updates));

    let field_map = transaction
        .schema_metadata_updates
        .as_ref()
        .expect("attached patch must be present")
        .field_metadata_updates
        .get(&FIELD_ID)
        .expect("stable field id 7 must be present");
    assert!(!field_map.replace);
    assert_eq!(field_map.update_entries.len(), 1);
    assert_eq!(field_map.update_entries[0].key, META_KEY);
    assert_eq!(
        field_map.update_entries[0].value.as_deref(),
        Some(META_VALUE)
    );
}
