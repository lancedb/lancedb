// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Pure crate-private generated-column invalidation planner (B4a).
//!
//! Plans column-wide dependency-epoch advances from a binding snapshot and a
//! mutation impact. This module does not mutate tables, write metadata, or
//! execute append/update/delete/merge paths. Native append consumes the plan
//! through the B4b runtime wiring.

use std::collections::BTreeSet;

use super::{GeneratedColumnBindingSnapshot, GeneratedColumnDefinition};
use crate::Result;

/// Mutation impact considered by the crate-private invalidation planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GeneratedColumnMutationImpact {
    /// Append or delete: whole-column coverage / row membership changed.
    RowSetChanged,
    /// Update of the listed stable field IDs (direct and transitive dependents).
    ///
    /// Reserved for future update/delete/merge invalidation consumers; Native
    /// append (B4b) only constructs [`Self::RowSetChanged`].
    #[cfg_attr(not(test), allow(dead_code))]
    UpdatedFields(BTreeSet<i32>),
}

/// One planned field-metadata replacement produced by the pure planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedGeneratedColumnMetadataUpdate {
    output_field_id: i32,
    metadata_json: String,
}

impl PlannedGeneratedColumnMetadataUpdate {
    /// Stable output field ID whose metadata should be replaced.
    pub fn output_field_id(&self) -> i32 {
        self.output_field_id
    }

    /// Canonical [`GeneratedColumnDefinition::to_metadata_json`] bytes.
    pub fn metadata_json(&self) -> &str {
        &self.metadata_json
    }
}

/// Plan generated-column metadata replacements for `impact`.
///
/// Planning is pure: `snapshot` is never mutated. Every present
/// `lancedb::generated_column` value is decoded and every decoded call's field
/// arguments are validated against `snapshot` before impact is calculated.
/// Decode, missing-field, type-mismatch, serialization, or overflow errors
/// return no plan.
///
/// Impacted definitions advance `dependency_epoch` exactly once (checked
/// arithmetic) while preserving `materialized_epoch`, output identity, and the
/// embedded [`super::FunctionCall`]. Replacements are returned in snapshot
/// schema order.
pub fn plan_generated_column_invalidation(
    snapshot: &GeneratedColumnBindingSnapshot,
    impact: &GeneratedColumnMutationImpact,
) -> Result<Vec<PlannedGeneratedColumnMetadataUpdate>> {
    let definitions = decode_and_validate_generated_columns(snapshot)?;
    let impacted = compute_impacted_output_ids(&definitions, impact);

    let mut plan = Vec::new();
    for (output_field_id, definition) in &definitions {
        if !impacted.contains(output_field_id) {
            continue;
        }
        let mut next = definition.clone();
        next.invalidate()?;
        let metadata_json = next.to_metadata_json()?;
        plan.push(PlannedGeneratedColumnMetadataUpdate {
            output_field_id: *output_field_id,
            metadata_json,
        });
    }
    Ok(plan)
}

/// Decode every present generated-column definition in schema order and
/// validate field arguments against the same snapshot.
fn decode_and_validate_generated_columns(
    snapshot: &GeneratedColumnBindingSnapshot,
) -> Result<Vec<(i32, GeneratedColumnDefinition)>> {
    let mut definitions = Vec::new();
    for entry in snapshot.entries() {
        let Some(definition) = entry.generated_column_definition()? else {
            continue;
        };
        snapshot.validate_field_arguments(definition.function_call())?;
        definitions.push((entry.field_id(), definition));
    }
    Ok(definitions)
}

/// Compute the set of impacted generated-column output field IDs.
///
/// `RowSetChanged` impacts every generated column. `UpdatedFields` computes a
/// deterministic fixed point over generated output IDs: a definition is
/// impacted when any field argument references a dirty ID, and each generated
/// definition is added at most once so cycles terminate.
fn compute_impacted_output_ids(
    definitions: &[(i32, GeneratedColumnDefinition)],
    impact: &GeneratedColumnMutationImpact,
) -> BTreeSet<i32> {
    match impact {
        GeneratedColumnMutationImpact::RowSetChanged => {
            definitions.iter().map(|(id, _)| *id).collect()
        }
        GeneratedColumnMutationImpact::UpdatedFields(updated) => {
            let mut dirty = updated.clone();
            let mut impacted = BTreeSet::new();
            let mut progressed = true;
            while progressed {
                progressed = false;
                for (output_field_id, definition) in definitions {
                    if impacted.contains(output_field_id) {
                        continue;
                    }
                    let depends_on_dirty =
                        definition
                            .function_call()
                            .arguments()
                            .iter()
                            .any(|(_, argument)| {
                                argument
                                    .field_id()
                                    .is_some_and(|field_id| dirty.contains(&field_id))
                            });
                    if depends_on_dirty {
                        impacted.insert(*output_field_id);
                        dirty.insert(*output_field_id);
                        progressed = true;
                    }
                }
            }
            impacted
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int32Array};
    use arrow_schema::{DataType, Field, FieldRef};

    use super::*;
    use crate::function::{
        Function, FunctionArgument, FunctionCall, FunctionId, FunctionOutput, FunctionParameter,
        FunctionSignature, GENERATED_COLUMN_METADATA_KEY,
    };

    fn int_field_function(id: &str) -> Function {
        Function::new(
            FunctionId::try_new(id).unwrap(),
            FunctionSignature::try_new(
                vec![FunctionParameter::new("upstream", DataType::Int32)],
                FunctionOutput::new(DataType::Int32, true),
            )
            .unwrap(),
        )
    }

    fn int_field_bound_call(function: &Function, input_field_id: i32) -> FunctionCall {
        FunctionCall::try_new(
            function,
            vec![(
                "upstream".to_string(),
                FunctionArgument::try_field(input_field_id, DataType::Int32).unwrap(),
            )],
        )
        .unwrap()
    }

    fn definition(
        output_field_id: i32,
        call: FunctionCall,
        dependency_epoch: u64,
        materialized_epoch: u64,
    ) -> GeneratedColumnDefinition {
        GeneratedColumnDefinition::try_new(
            output_field_id,
            call,
            dependency_epoch,
            materialized_epoch,
        )
        .unwrap()
    }

    fn generated_field(name: &str, def: &GeneratedColumnDefinition) -> FieldRef {
        let json = def.to_metadata_json().unwrap();
        Arc::new(
            Field::new(name, DataType::Int32, true)
                .with_metadata([(GENERATED_COLUMN_METADATA_KEY.to_string(), json)].into()),
        )
    }

    #[test]
    fn cyclic_dependency_fixed_point_impacts_each_definition_at_most_once() {
        // A <-> B cycle. Seeding either side must terminate and advance each
        // impacted definition exactly once. This proves planner termination; it
        // is not a public cyclic-dependency creation guarantee.
        let a_id = 60;
        let b_id = 70;
        let fn_a = int_field_function("fn.exact.b4a.cycle.a");
        let fn_b = int_field_function("fn.exact.b4a.cycle.b");
        let a = definition(a_id, int_field_bound_call(&fn_a, b_id), 1, 1);
        let b = definition(b_id, int_field_bound_call(&fn_b, a_id), 2, 2);
        let snap = GeneratedColumnBindingSnapshot::try_new(
            11,
            vec![generated_field("gen_a", &a), generated_field("gen_b", &b)],
            vec![a_id, b_id],
        )
        .unwrap();
        let before = snap.clone();

        let plan = plan_generated_column_invalidation(
            &snap,
            &GeneratedColumnMutationImpact::UpdatedFields(BTreeSet::from([a_id])),
        )
        .expect("cyclic fixed point must terminate");
        assert_eq!(snap, before);
        assert_eq!(plan.len(), 2);
        assert_eq!(plan[0].output_field_id(), a_id);
        assert_eq!(plan[1].output_field_id(), b_id);

        let decoded_a =
            GeneratedColumnDefinition::from_metadata_json(plan[0].metadata_json(), a_id).unwrap();
        let decoded_b =
            GeneratedColumnDefinition::from_metadata_json(plan[1].metadata_json(), b_id).unwrap();
        assert_eq!(decoded_a.dependency_epoch(), 2);
        assert_eq!(decoded_a.materialized_epoch(), 1);
        assert_eq!(decoded_b.dependency_epoch(), 3);
        assert_eq!(decoded_b.materialized_epoch(), 2);
        assert_eq!(decoded_a.function_call(), a.function_call());
        assert_eq!(decoded_b.function_call(), b.function_call());
    }

    #[test]
    fn row_set_change_with_cycle_still_invalidates_each_column_once() {
        let a_id = 61;
        let b_id = 71;
        let fn_a = int_field_function("fn.exact.b4a.cycle.row.a");
        let fn_b = int_field_function("fn.exact.b4a.cycle.row.b");
        let a = definition(a_id, int_field_bound_call(&fn_a, b_id), 5, 5);
        let b = definition(b_id, int_field_bound_call(&fn_b, a_id), 8, 8);
        let snap = GeneratedColumnBindingSnapshot::try_new(
            12,
            vec![generated_field("gen_b", &b), generated_field("gen_a", &a)],
            vec![b_id, a_id],
        )
        .unwrap();

        let plan = plan_generated_column_invalidation(
            &snap,
            &GeneratedColumnMutationImpact::RowSetChanged,
        )
        .expect("row-set change over a cycle must plan once per column");
        assert_eq!(plan.len(), 2);
        assert_eq!(plan[0].output_field_id(), b_id);
        assert_eq!(plan[1].output_field_id(), a_id);
        let decoded_b =
            GeneratedColumnDefinition::from_metadata_json(plan[0].metadata_json(), b_id).unwrap();
        let decoded_a =
            GeneratedColumnDefinition::from_metadata_json(plan[1].metadata_json(), a_id).unwrap();
        assert_eq!(decoded_b.dependency_epoch(), 9);
        assert_eq!(decoded_a.dependency_epoch(), 6);
    }

    #[test]
    fn literal_only_is_ignored_by_updated_fields_even_with_empty_seed() {
        let literal_fn = Function::new(
            FunctionId::try_new("fn.exact.b4a.cycle.literal").unwrap(),
            FunctionSignature::try_new(
                vec![FunctionParameter::new("constant", DataType::Int32)],
                FunctionOutput::new(DataType::Int32, true),
            )
            .unwrap(),
        );
        let literal_id = 80;
        let literal = definition(
            literal_id,
            FunctionCall::try_new(
                &literal_fn,
                vec![(
                    "constant".to_string(),
                    FunctionArgument::try_literal(
                        Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef
                    )
                    .unwrap(),
                )],
            )
            .unwrap(),
            3,
            3,
        );
        let snap = GeneratedColumnBindingSnapshot::try_new(
            13,
            vec![generated_field("gen_literal", &literal)],
            vec![literal_id],
        )
        .unwrap();

        let plan = plan_generated_column_invalidation(
            &snap,
            &GeneratedColumnMutationImpact::UpdatedFields(BTreeSet::new()),
        )
        .expect("empty UpdatedFields must succeed");
        assert!(plan.is_empty());
    }
}
