// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Schema admission for caller-authored generated-column definition ingress.
//!
//! General-purpose table-schema inputs (for example `Database::create_table`)
//! must not invent or mutate Job-owned `lancedb::generated_column` top-level
//! field metadata. Only generated-column create/change/refresh Job publication
//! may create or change that reserved key.
//!
//! This helper checks raw key presence on top-level fields only. It does not
//! recurse into nested children, inspect schema-level metadata, decode the
//! payload, look up a Function, or validate epochs.

use arrow_schema::Schema;

use super::GENERATED_COLUMN_METADATA_KEY;
use crate::{Error, Result};

/// Reject a caller-authored Arrow schema that carries reserved generated-column
/// definition metadata on any top-level field.
///
/// Safe to call at the start of create-table (and later overwrite / add-columns)
/// paths before source consumption, namespace mutation, or HTTP.
pub fn reject_caller_authored_generated_column_schema(schema: &Schema) -> Result<()> {
    for field in schema.fields() {
        if field.metadata().contains_key(GENERATED_COLUMN_METADATA_KEY) {
            return Err(Error::NotSupported {
                message: "generated column definitions are owned by create/change/refresh Jobs \
                          and cannot be supplied through general-purpose table schema input"
                    .into(),
            });
        }
    }
    Ok(())
}
