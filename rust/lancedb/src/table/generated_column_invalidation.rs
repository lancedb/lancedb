// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Crate-private Native wiring for generated-column invalidation (B4b / B4c / B4d / B4e).
//!
//! Converts the B4a pure planner into one Lance field-metadata patch for Native
//! append, update, and delete commits. Planning is strict-decode/validate;
//! overwrite of a table with any generated-column definition, direct writes of
//! generated outputs via Update, and Native merge-insert (standard and LSM)
//! fail closed as [`Error::NotSupported`].

use std::collections::{BTreeSet, HashMap};

use lance::Dataset;
use lance::dataset::transaction::{SchemaMetadataUpdates, UpdateMap, UpdateMapEntry};

use crate::Result;
use crate::error::Error;
use crate::function::GENERATED_COLUMN_METADATA_KEY;
use crate::function::plan_generated_column_invalidation::{
    GeneratedColumnMutationImpact, PlannedGeneratedColumnMetadataUpdate,
    plan_generated_column_invalidation,
};

use super::generated_column_binding_snapshot_from_dataset;

/// Plan Native append invalidation against one exact dataset snapshot.
///
/// Strict-decodes and validates every present generated-column metadata value
/// through the B4a planner. When `is_overwrite` is true and any generated column
/// is present, returns [`Error::NotSupported`] before mutation. Otherwise returns
/// `Some(patch)` when at least one generated column would be invalidated, or
/// `None` when the table has no generated columns.
pub(super) fn plan_native_append_generated_column_invalidation(
    dataset: &Dataset,
    is_overwrite: bool,
) -> Result<Option<SchemaMetadataUpdates>> {
    let snapshot = generated_column_binding_snapshot_from_dataset(dataset)?;
    let plan = plan_generated_column_invalidation(
        &snapshot,
        &GeneratedColumnMutationImpact::RowSetChanged,
    )?;
    if plan.is_empty() {
        return Ok(None);
    }
    if is_overwrite {
        return Err(Error::NotSupported {
            message: "Overwrite is not supported on tables with generated columns".to_string(),
        });
    }
    Ok(Some(planned_invalidation_to_schema_metadata_updates(plan)))
}

/// Plan Native update invalidation against one exact dataset snapshot.
///
/// Strict-decodes and validates every present generated-column definition before
/// impact calculation, even when `updated_field_ids` does not affect any
/// generated output. After the global planner succeeds, a target whose snapshot
/// entry contains generated metadata is rejected as a direct generated-output
/// write ([`Error::NotSupported`]) before any Update file write. Returns
/// `Some(patch)` when the impact closure is non-empty, otherwise `None`.
pub(super) fn plan_native_update_generated_column_invalidation(
    dataset: &Dataset,
    updated_field_ids: BTreeSet<i32>,
) -> Result<Option<SchemaMetadataUpdates>> {
    let snapshot = generated_column_binding_snapshot_from_dataset(dataset)?;
    let plan = plan_generated_column_invalidation(
        &snapshot,
        &GeneratedColumnMutationImpact::UpdatedFields(updated_field_ids.clone()),
    )?;

    for field_id in &updated_field_ids {
        let Some(entry) = snapshot
            .entries()
            .iter()
            .find(|entry| entry.field_id() == *field_id)
        else {
            return Err(Error::InvalidInput {
                message: format!("updated field id {field_id} was not found in the table schema"),
            });
        };
        if entry
            .field()
            .metadata()
            .contains_key(GENERATED_COLUMN_METADATA_KEY)
        {
            return Err(Error::NotSupported {
                message: "Updating generated columns is not supported".to_string(),
            });
        }
    }

    if plan.is_empty() {
        return Ok(None);
    }
    Ok(Some(planned_invalidation_to_schema_metadata_updates(plan)))
}

/// Plan Native delete invalidation against one exact dataset snapshot.
///
/// Strict-decodes and validates every present generated-column metadata value
/// through the B4a `RowSetChanged` planner before any Delete scanner/file IO.
/// Returns `Some(patch)` when at least one generated column would be invalidated,
/// or `None` when the table has no generated columns. Actual zero-row Delete
/// suppression is owned by Lance A4d, not this planner.
pub(super) fn plan_native_delete_generated_column_invalidation(
    dataset: &Dataset,
) -> Result<Option<SchemaMetadataUpdates>> {
    let snapshot = generated_column_binding_snapshot_from_dataset(dataset)?;
    let plan = plan_generated_column_invalidation(
        &snapshot,
        &GeneratedColumnMutationImpact::RowSetChanged,
    )?;
    if plan.is_empty() {
        return Ok(None);
    }
    Ok(Some(planned_invalidation_to_schema_metadata_updates(plan)))
}

/// Fail closed before Native `merge_insert` when any generated column is present.
///
/// Strict-decodes and validates every present generated-column metadata value
/// through the B4a `RowSetChanged` planner against one exact dataset snapshot.
/// Malformed metadata returns the existing [`Error::InvalidInput`] validation
/// category. When at least one valid generated column is present, returns
/// [`Error::NotSupported`] before LSM dispatch or source iteration. Ordinary
/// tables (no generated metadata) return `Ok(())`.
pub(super) fn reject_native_merge_insert_if_generated_columns_present(
    dataset: &Dataset,
) -> Result<()> {
    let snapshot = generated_column_binding_snapshot_from_dataset(dataset)?;
    let plan = plan_generated_column_invalidation(
        &snapshot,
        &GeneratedColumnMutationImpact::RowSetChanged,
    )?;
    if plan.is_empty() {
        return Ok(());
    }
    Err(Error::NotSupported {
        message: "Merge insert is not supported on tables with generated columns".to_string(),
    })
}

/// Convert planner replacements into one non-empty Lance field-metadata patch.
///
/// Each entry is keyed by stable output field ID, uses `replace: false`, and
/// replaces only [`GENERATED_COLUMN_METADATA_KEY`].
fn planned_invalidation_to_schema_metadata_updates(
    plan: Vec<PlannedGeneratedColumnMetadataUpdate>,
) -> SchemaMetadataUpdates {
    SchemaMetadataUpdates {
        schema_metadata_updates: None,
        field_metadata_updates: plan
            .into_iter()
            .map(|update| {
                (
                    update.output_field_id(),
                    UpdateMap {
                        update_entries: vec![UpdateMapEntry {
                            key: GENERATED_COLUMN_METADATA_KEY.to_string(),
                            value: Some(update.metadata_json().to_string()),
                        }],
                        replace: false,
                    },
                )
            })
            .collect::<HashMap<_, _>>(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Construct a planned update through the public accessors by planning a
    /// minimal in-memory snapshot, then assert the Lance patch shape.
    #[test]
    fn planned_replacements_become_non_replace_field_patch() {
        use crate::function::{
            Function, FunctionArgument, FunctionCall, FunctionId, FunctionOutput,
            FunctionParameter, FunctionSignature, GeneratedColumnBindingSnapshot,
            GeneratedColumnDefinition,
        };
        use arrow_array::{ArrayRef, StringArray};
        use arrow_schema::{DataType, Field};
        use std::sync::Arc;

        let field_id = 11;
        let function = Function::new(
            FunctionId::try_new("fn.exact.b4b.helper.patch").unwrap(),
            FunctionSignature::try_new(
                vec![FunctionParameter::new("label", DataType::Utf8)],
                FunctionOutput::new(DataType::Int32, true),
            )
            .unwrap(),
        );
        let call = FunctionCall::try_new(
            &function,
            vec![(
                "label".to_string(),
                FunctionArgument::try_literal(
                    Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef
                )
                .unwrap(),
            )],
        )
        .unwrap();
        let definition = GeneratedColumnDefinition::try_new(field_id, call, 3, 3).unwrap();
        let json = definition.to_metadata_json().unwrap();
        let snap = GeneratedColumnBindingSnapshot::try_new(
            1,
            vec![Arc::new(
                Field::new("gen_out", DataType::Int32, true)
                    .with_metadata([(GENERATED_COLUMN_METADATA_KEY.to_string(), json)].into()),
            )],
            vec![field_id],
        )
        .unwrap();
        let plan = plan_generated_column_invalidation(
            &snap,
            &GeneratedColumnMutationImpact::RowSetChanged,
        )
        .unwrap();
        let patch = planned_invalidation_to_schema_metadata_updates(plan);
        assert!(!patch.is_empty());
        assert!(patch.schema_metadata_updates.is_none());
        let map = patch
            .field_metadata_updates
            .get(&field_id)
            .expect("stable field id must be present");
        assert!(!map.replace);
        assert_eq!(map.update_entries.len(), 1);
        assert_eq!(map.update_entries[0].key, GENERATED_COLUMN_METADATA_KEY);
        let decoded = GeneratedColumnDefinition::from_metadata_json(
            map.update_entries[0].value.as_deref().unwrap(),
            field_id,
        )
        .unwrap();
        assert_eq!(decoded.dependency_epoch(), 4);
        assert_eq!(decoded.materialized_epoch(), 3);
    }
}
