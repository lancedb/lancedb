// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Atomic generated-column binding snapshot projection (FF-029).
//!
//! This is an implementation projection for table call binding. It is not a
//! catalog resource, Job, persistent model, wire payload, or table-version
//! replacement.

use std::collections::HashSet;

use arrow_schema::FieldRef;

use super::invalid_input;
use crate::Result;

/// One top-level field identity from a single table snapshot.
///
/// Pairs a non-negative Lance stable field ID with the exact Arrow field from
/// that same snapshot. IDs are carried only here; they are never injected into
/// Arrow field metadata.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneratedColumnBindingEntry {
    field_id: i32,
    field: FieldRef,
}

impl GeneratedColumnBindingEntry {
    /// Stable Lance field ID for this top-level entry.
    pub fn field_id(&self) -> i32 {
        self.field_id
    }

    /// Exact Arrow field from the same snapshot.
    pub fn field(&self) -> &FieldRef {
        &self.field
    }
}

/// Atomic table snapshot projection for generated-column call binding.
///
/// Contains one table version and immutable top-level field entries in schema
/// order. Construction validates field/ID count equality, non-negative unique
/// IDs, and unique top-level names.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneratedColumnBindingSnapshot {
    version: u64,
    entries: Vec<GeneratedColumnBindingEntry>,
}

impl GeneratedColumnBindingSnapshot {
    /// Build a binding snapshot from one version and ordered field/ID pairs.
    ///
    /// `fields` and `field_ids` must have the same length. Every ID must be
    /// non-negative and unique. Top-level field names must be unique. Order is
    /// preserved exactly as provided.
    pub fn try_new(
        version: u64,
        fields: impl IntoIterator<Item = FieldRef>,
        field_ids: impl IntoIterator<Item = i32>,
    ) -> Result<Self> {
        let fields: Vec<FieldRef> = fields.into_iter().collect();
        let field_ids: Vec<i32> = field_ids.into_iter().collect();
        if fields.len() != field_ids.len() {
            return Err(invalid_input(
                "generated-column binding snapshot field count must equal field_ids count",
            ));
        }

        let mut seen_ids = HashSet::with_capacity(field_ids.len());
        let mut seen_names = HashSet::with_capacity(fields.len());
        let mut entries = Vec::with_capacity(fields.len());

        for (field, field_id) in fields.into_iter().zip(field_ids) {
            if field_id < 0 {
                return Err(invalid_input(
                    "generated-column binding snapshot field IDs must be non-negative",
                ));
            }
            if !seen_ids.insert(field_id) {
                return Err(invalid_input(
                    "generated-column binding snapshot field IDs must be unique",
                ));
            }
            if !seen_names.insert(field.name().clone()) {
                return Err(invalid_input(
                    "generated-column binding snapshot top-level field names must be unique",
                ));
            }
            entries.push(GeneratedColumnBindingEntry { field_id, field });
        }

        Ok(Self { version, entries })
    }

    /// Table version for this snapshot.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Top-level entries in schema order.
    pub fn entries(&self) -> &[GeneratedColumnBindingEntry] {
        &self.entries
    }

    /// Exact case-sensitive top-level field name lookup.
    ///
    /// A name containing `.` is a literal top-level field name, not a nested
    /// path. Lookup does not fold case or interpret dotted selectors.
    pub fn field(&self, name: &str) -> Option<&GeneratedColumnBindingEntry> {
        self.entries
            .iter()
            .find(|entry| entry.field().name() == name)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field};

    use super::*;
    use crate::Error;

    fn fields() -> Vec<FieldRef> {
        vec![
            Arc::new(Field::new("text", DataType::Utf8, true)),
            Arc::new(Field::new("Score", DataType::Int32, false)),
            Arc::new(Field::new("a.b", DataType::Utf8, true)),
        ]
    }

    #[test]
    fn try_new_preserves_version_order_and_entry_data() {
        let snapshot =
            GeneratedColumnBindingSnapshot::try_new(11, fields(), vec![2, 4, 8]).unwrap();
        assert_eq!(snapshot.version(), 11);
        assert_eq!(snapshot.entries().len(), 3);
        assert_eq!(snapshot.entries()[0].field_id(), 2);
        assert_eq!(snapshot.entries()[0].field().name(), "text");
        assert_eq!(snapshot.entries()[0].field().data_type(), &DataType::Utf8);
        assert_eq!(snapshot.entries()[1].field_id(), 4);
        assert_eq!(snapshot.entries()[1].field().name(), "Score");
        assert_eq!(snapshot.entries()[2].field_id(), 8);
        assert_eq!(snapshot.entries()[2].field().name(), "a.b");
    }

    #[test]
    fn lookup_is_exact_case_sensitive_and_treats_dot_literally() {
        let snapshot = GeneratedColumnBindingSnapshot::try_new(1, fields(), vec![2, 4, 8]).unwrap();
        assert_eq!(snapshot.field("Score").unwrap().field_id(), 4);
        assert!(snapshot.field("score").is_none());
        assert!(snapshot.field("TEXT").is_none());
        assert!(snapshot.field("a").is_none());
        assert!(snapshot.field("b").is_none());
        assert_eq!(snapshot.field("a.b").unwrap().field_id(), 8);
    }

    #[test]
    fn try_new_rejects_count_mismatch_negative_duplicate_ids_and_names() {
        let base = fields();
        assert!(matches!(
            GeneratedColumnBindingSnapshot::try_new(1, base.clone(), vec![1, 2]),
            Err(Error::InvalidInput { .. })
        ));
        assert!(matches!(
            GeneratedColumnBindingSnapshot::try_new(1, base.clone(), vec![1, 2, -3]),
            Err(Error::InvalidInput { .. })
        ));
        assert!(matches!(
            GeneratedColumnBindingSnapshot::try_new(1, base.clone(), vec![1, 2, 1]),
            Err(Error::InvalidInput { .. })
        ));
        let duplicate_names = vec![
            Arc::new(Field::new("text", DataType::Utf8, true)),
            Arc::new(Field::new("text", DataType::Int32, false)),
        ];
        assert!(matches!(
            GeneratedColumnBindingSnapshot::try_new(1, duplicate_names, vec![1, 2]),
            Err(Error::InvalidInput { .. })
        ));
    }
}
