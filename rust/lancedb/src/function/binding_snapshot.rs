// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Atomic generated-column binding snapshot projection (FF-029).
//!
//! This is an implementation projection for table call binding. It is not a
//! catalog resource, Job, persistent model, wire payload, or table-version
//! replacement.

use std::collections::HashSet;

use arrow_schema::FieldRef;

use super::{
    FunctionCall, GENERATED_COLUMN_METADATA_KEY, GeneratedColumnDefinition, invalid_input,
};
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

    /// Strict generated-column definition from this entry's Arrow metadata.
    ///
    /// Reads only [`GENERATED_COLUMN_METADATA_KEY`] on the exact snapshot field
    /// and decodes through
    /// [`GeneratedColumnDefinition::from_metadata_json`] with
    /// [`Self::field_id`] as the expected output identity. The same-snapshot
    /// stable field ID is mandatory so decode rejects metadata whose embedded
    /// `output_field_id` does not match this entry; name/ordinal/hash fallbacks
    /// are not used.
    ///
    /// Returns [`Ok`]`(`[`None`]`)` when the key is absent. Present but invalid
    /// metadata fails closed as [`crate::Error::InvalidInput`] with a short
    /// field-ID diagnostic that does not echo the raw metadata payload.
    pub(crate) fn generated_column_definition(&self) -> Result<Option<GeneratedColumnDefinition>> {
        let Some(raw) = self.field.metadata().get(GENERATED_COLUMN_METADATA_KEY) else {
            return Ok(None);
        };
        match GeneratedColumnDefinition::from_metadata_json(raw, self.field_id) {
            Ok(definition) => Ok(Some(definition)),
            Err(_) => Err(invalid_input(format!(
                "invalid generated-column metadata for field id {}",
                self.field_id
            ))),
        }
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

    /// Validate table-dependent field arguments of an already canonical call.
    ///
    /// For every field argument, finds the snapshot entry by stable Lance field
    /// ID and requires exact Arrow [`arrow_schema::DataType`] equality. Literal
    /// arguments are table-independent and ignored. Missing field ID or type
    /// mismatch returns [`crate::Error::InvalidInput`] without modifying `call`
    /// or this snapshot.
    ///
    /// This check is orthogonal to [`FunctionCall::validate_against`]: it does
    /// not perform catalog lookup, Function identity/signature validation, or
    /// table mutation.
    pub fn validate_field_arguments(&self, call: &FunctionCall) -> Result<()> {
        for (_parameter, argument) in call.arguments() {
            let Some(field_id) = argument.field_id() else {
                continue;
            };
            let Some(entry) = self.entry_by_field_id(field_id) else {
                return Err(invalid_input(format!(
                    "generated-column binding snapshot missing field id {field_id}"
                )));
            };
            let expected = argument.data_type();
            let current = entry.field().data_type();
            if current != expected {
                return Err(invalid_input(format!(
                    "generated-column binding snapshot field id {field_id} type mismatch: \
                     expected {expected}, found {current}"
                )));
            }
        }
        Ok(())
    }

    fn entry_by_field_id(&self, field_id: i32) -> Option<&GeneratedColumnBindingEntry> {
        self.entries
            .iter()
            .find(|entry| entry.field_id() == field_id)
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

    fn sample_function() -> crate::function::Function {
        use crate::function::{
            Function, FunctionId, FunctionOutput, FunctionParameter, FunctionSignature,
        };
        let id = FunctionId::try_new("fn.exact.snapshot.lib").unwrap();
        let signature = FunctionSignature::try_new(
            vec![
                FunctionParameter::new("payload_arg", DataType::Utf8),
                FunctionParameter::new("metric_arg", DataType::Int32),
            ],
            FunctionOutput::new(DataType::Int32, true),
        )
        .unwrap();
        Function::new(id, signature)
    }

    #[test]
    fn validate_field_arguments_value_cases() {
        use crate::function::{FunctionArgument, FunctionCall};
        use arrow_array::{ArrayRef, Int32Array};

        let snapshot = GeneratedColumnBindingSnapshot::try_new(2, fields(), vec![2, 4, 8]).unwrap();
        let function = sample_function();

        let valid = FunctionCall::try_new(
            &function,
            vec![
                (
                    "payload_arg".to_string(),
                    FunctionArgument::try_field(2, DataType::Utf8).unwrap(),
                ),
                (
                    "metric_arg".to_string(),
                    FunctionArgument::try_field(4, DataType::Int32).unwrap(),
                ),
            ],
        )
        .unwrap();
        snapshot.validate_field_arguments(&valid).unwrap();

        let missing = FunctionCall::try_new(
            &function,
            vec![
                (
                    "payload_arg".to_string(),
                    FunctionArgument::try_field(99, DataType::Utf8).unwrap(),
                ),
                (
                    "metric_arg".to_string(),
                    FunctionArgument::try_field(4, DataType::Int32).unwrap(),
                ),
            ],
        )
        .unwrap();
        assert!(matches!(
            snapshot.validate_field_arguments(&missing),
            Err(Error::InvalidInput { .. })
        ));

        // Same stable ID, different Arrow type: exact-type equality must reject.
        let type_mismatch = FunctionCall::try_new(
            &function,
            vec![
                (
                    "payload_arg".to_string(),
                    // ID 4 is Int32 in the snapshot.
                    FunctionArgument::try_field(4, DataType::Utf8).unwrap(),
                ),
                (
                    "metric_arg".to_string(),
                    FunctionArgument::try_field(4, DataType::Int32).unwrap(),
                ),
            ],
        )
        .unwrap();
        let err = snapshot
            .validate_field_arguments(&type_mismatch)
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));
        let message = err.to_string();
        assert!(message.contains('4'));
        assert!(message.contains("Utf8") && message.contains("Int32"));
        assert!(!message.contains("Score") && !message.contains("text"));

        let mixed = FunctionCall::try_new(
            &function,
            vec![
                (
                    "payload_arg".to_string(),
                    FunctionArgument::try_field(2, DataType::Utf8).unwrap(),
                ),
                (
                    "metric_arg".to_string(),
                    FunctionArgument::try_literal(
                        Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef
                    )
                    .unwrap(),
                ),
            ],
        )
        .unwrap();
        snapshot.validate_field_arguments(&mixed).unwrap();

        let literal_only_fn = {
            use crate::function::{
                Function, FunctionId, FunctionOutput, FunctionParameter, FunctionSignature,
            };
            Function::new(
                FunctionId::try_new("fn.exact.snapshot.literal").unwrap(),
                FunctionSignature::try_new(
                    vec![FunctionParameter::new("constant_arg", DataType::Int32)],
                    FunctionOutput::new(DataType::Int32, true),
                )
                .unwrap(),
            )
        };
        let literal_only =
            FunctionCall::try_new(
                &literal_only_fn,
                vec![(
                    "constant_arg".to_string(),
                    FunctionArgument::try_literal(
                        Arc::new(Int32Array::from(vec![Some(9)])) as ArrayRef
                    )
                    .unwrap(),
                )],
            )
            .unwrap();
        // Empty snapshot still accepts literal-only calls.
        let empty =
            GeneratedColumnBindingSnapshot::try_new(1, Vec::<FieldRef>::new(), vec![]).unwrap();
        empty.validate_field_arguments(&literal_only).unwrap();
    }

    fn status_sample_function() -> crate::function::Function {
        use crate::function::{
            Function, FunctionId, FunctionOutput, FunctionParameter, FunctionSignature,
        };
        Function::new(
            FunctionId::try_new("fn.exact.status.binding").unwrap(),
            FunctionSignature::try_new(
                vec![FunctionParameter::new("label", DataType::Utf8)],
                FunctionOutput::new(DataType::Int32, true),
            )
            .unwrap(),
        )
    }

    fn status_sample_call() -> crate::function::FunctionCall {
        use crate::function::{FunctionArgument, FunctionCall};
        use arrow_array::{ArrayRef, StringArray};
        let function = status_sample_function();
        FunctionCall::try_new(
            &function,
            vec![(
                "label".to_string(),
                FunctionArgument::try_literal(
                    Arc::new(StringArray::from(vec![Some("ok")])) as ArrayRef
                )
                .unwrap(),
            )],
        )
        .unwrap()
    }

    fn definition_json(
        output_field_id: i32,
        dependency_epoch: u64,
        materialized_epoch: u64,
    ) -> String {
        use crate::function::GeneratedColumnDefinition;
        GeneratedColumnDefinition::try_new(
            output_field_id,
            status_sample_call(),
            dependency_epoch,
            materialized_epoch,
        )
        .unwrap()
        .to_metadata_json()
        .unwrap()
    }

    fn entry_with_metadata(
        name: &str,
        field_id: i32,
        metadata_json: Option<&str>,
    ) -> GeneratedColumnBindingEntry {
        use crate::function::GENERATED_COLUMN_METADATA_KEY;
        let field = if let Some(json) = metadata_json {
            Field::new(name, DataType::Int32, true).with_metadata(
                [(GENERATED_COLUMN_METADATA_KEY.to_string(), json.to_string())].into(),
            )
        } else {
            Field::new(name, DataType::Int32, true)
        };
        let snapshot =
            GeneratedColumnBindingSnapshot::try_new(1, vec![Arc::new(field)], vec![field_id])
                .unwrap();
        snapshot.entries()[0].clone()
    }

    #[test]
    fn generated_column_definition_absent_returns_none() {
        let entry = entry_with_metadata("ordinary", 3, None);
        let got = entry.generated_column_definition().unwrap();
        assert!(got.is_none());
    }

    #[test]
    fn generated_column_definition_decodes_complete_and_incomplete() {
        use crate::function::{GeneratedColumnDefinition, GeneratedColumnStatus};

        let complete_json = definition_json(5, 3, 3);
        let complete_entry = entry_with_metadata("gen_complete", 5, Some(&complete_json));
        let complete = complete_entry
            .generated_column_definition()
            .unwrap()
            .expect("complete metadata present");
        assert_eq!(complete.output_field_id(), 5);
        assert_eq!(complete.dependency_epoch(), 3);
        assert_eq!(complete.materialized_epoch(), 3);
        assert_eq!(complete.status(), GeneratedColumnStatus::Complete);
        assert_eq!(
            complete,
            GeneratedColumnDefinition::from_metadata_json(&complete_json, 5).unwrap()
        );

        let incomplete_json = definition_json(7, 4, 2);
        let incomplete_entry = entry_with_metadata("gen_incomplete", 7, Some(&incomplete_json));
        let incomplete = incomplete_entry
            .generated_column_definition()
            .unwrap()
            .expect("incomplete metadata present");
        assert_eq!(incomplete.output_field_id(), 7);
        assert_eq!(incomplete.dependency_epoch(), 4);
        assert_eq!(incomplete.materialized_epoch(), 2);
        assert_eq!(incomplete.status(), GeneratedColumnStatus::Incomplete);
        assert_eq!(
            incomplete,
            GeneratedColumnDefinition::from_metadata_json(&incomplete_json, 7).unwrap()
        );
    }

    #[test]
    fn generated_column_definition_fail_closed_for_invalid_metadata() {
        use crate::function::GENERATED_COLUMN_METADATA_KEY;

        let field_id = 9i32;
        let valid = definition_json(field_id, 2, 2);
        let mut mismatched: serde_json::Value = serde_json::from_str(&valid).unwrap();
        mismatched["output_field_id"] = serde_json::json!(field_id + 1);

        let mut unsupported: serde_json::Value = serde_json::from_str(&valid).unwrap();
        unsupported["format_version"] = serde_json::json!(2);

        let mut reversed: serde_json::Value = serde_json::from_str(&valid).unwrap();
        reversed["dependency_epoch"] = serde_json::json!(1);
        reversed["materialized_epoch"] = serde_json::json!(2);

        let malformed_json = "{not-json";
        let mut malformed_call: serde_json::Value = serde_json::from_str(&valid).unwrap();
        malformed_call["function_call"] = serde_json::json!("not-an-object");

        for raw in [
            mismatched.to_string(),
            unsupported.to_string(),
            reversed.to_string(),
            malformed_json.to_string(),
            malformed_call.to_string(),
        ] {
            let entry = entry_with_metadata("gen_bad", field_id, Some(&raw));
            assert!(
                entry
                    .field()
                    .metadata()
                    .contains_key(GENERATED_COLUMN_METADATA_KEY),
                "fixture must carry generated-column metadata"
            );
            let err = entry.generated_column_definition().unwrap_err();
            assert!(
                matches!(err, Error::InvalidInput { .. }),
                "expected InvalidInput, got {err:?}"
            );
        }
    }

    #[test]
    fn generated_column_definition_errors_omit_raw_metadata_marker() {
        const MARKER: &str = "SENSITIVE_STATUS_METADATA_MARKER_b3d1_9f2e";
        let raw = format!(
            r#"{{"format_version":1,"output_field_id":3,"function_call":{MARKER},"dependency_epoch":1,"materialized_epoch":1}}"#
        );
        assert!(raw.contains(MARKER));
        let entry = entry_with_metadata("gen_redact", 3, Some(&raw));
        let err = entry.generated_column_definition().unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));
        let text = format!("{err}\n{err:?}");
        assert!(
            !text.contains(MARKER),
            "status definition diagnostics must not echo raw metadata marker: {text}"
        );
        assert!(
            !text.contains(&raw),
            "status definition diagnostics must not echo raw metadata payload: {text}"
        );
    }
}
