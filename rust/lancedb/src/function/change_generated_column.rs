// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Immutable ChangeGeneratedColumnJobSpec change-generated-column Job
//! operation input (FF-011).
//!
//! This type is Job operation input only. It does not look up catalogs or
//! tables, execute Jobs, stage artifacts, call Lance, derive candidate
//! definitions, or mutate epochs.

use std::fmt;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{Function, FunctionCall, GeneratedColumnDefinition, invalid_input};
use crate::Result;

const FORMAT_VERSION_V1: u32 = 1;

/// Immutable Job operation input for changing a generated column (format
/// version 1).
///
/// Semantic fields are exactly the expected [`GeneratedColumnDefinition`] CAS
/// precondition and the new [`FunctionCall`]. Wire keys are exactly
/// `format_version`, `expected_generated_column_definition`, and
/// `new_function_call`.
///
/// Construction via [`Self::try_new`] validates only the new call against the
/// new catalog [`Function`]. The expected definition is an opaque exact CAS
/// precondition and is not validated against an old Function handle.
/// Structural deserialize does not validate the new call either; execution
/// consumers must call [`Self::validate_against`].
///
/// Both complete and incomplete expected definitions are accepted. Same-call
/// change and new Functions whose output type or nullability differs from the
/// old Function are valid. Status and output-type equality are not constructor
/// or wire restrictions.
#[derive(Clone, PartialEq, Eq)]
pub struct ChangeGeneratedColumnJobSpec {
    expected_generated_column_definition: GeneratedColumnDefinition,
    new_function_call: FunctionCall,
}

impl ChangeGeneratedColumnJobSpec {
    /// Create a change-generated-column Job operation input.
    ///
    /// Requires [`FunctionCall::validate_against`] to succeed for
    /// `new_function_call` and `new_function` before returning (exact Function
    /// ID, parameter name/order, argument count, and Arrow type equality).
    ///
    /// The `expected_definition` is stored as an opaque exact CAS
    /// precondition. Its nested call is not validated against any Function.
    pub fn try_new(
        expected_definition: GeneratedColumnDefinition,
        new_function: &Function,
        new_function_call: FunctionCall,
    ) -> Result<Self> {
        new_function_call.validate_against(new_function)?;
        Ok(Self {
            expected_generated_column_definition: expected_definition,
            new_function_call,
        })
    }

    /// Wire format version (always 1 for this type).
    pub fn format_version(&self) -> u32 {
        FORMAT_VERSION_V1
    }

    /// Expected generated-column definition used as an exact CAS precondition.
    pub fn expected_generated_column_definition(&self) -> &GeneratedColumnDefinition {
        &self.expected_generated_column_definition
    }

    /// New function call to apply.
    pub fn new_function_call(&self) -> &FunctionCall {
        &self.new_function_call
    }

    /// Validate the new call against a catalog [`Function`].
    ///
    /// Structural decode does not perform this check. Execution consumers must
    /// call this before using the new call. The expected definition remains an
    /// opaque CAS precondition and is not validated here.
    pub fn validate_against(&self, new_function: &Function) -> Result<()> {
        self.new_function_call.validate_against(new_function)
    }

    fn to_wire(&self) -> ChangeGeneratedColumnJobSpecWire {
        ChangeGeneratedColumnJobSpecWire {
            format_version: FORMAT_VERSION_V1,
            expected_generated_column_definition: self.expected_generated_column_definition.clone(),
            new_function_call: self.new_function_call.clone(),
        }
    }

    fn from_wire(wire: ChangeGeneratedColumnJobSpecWire) -> Result<Self> {
        if wire.format_version != FORMAT_VERSION_V1 {
            return Err(invalid_input(format!(
                "unsupported ChangeGeneratedColumnJobSpec format_version {}",
                wire.format_version
            )));
        }
        Ok(Self {
            expected_generated_column_definition: wire.expected_generated_column_definition,
            new_function_call: wire.new_function_call,
        })
    }
}

impl fmt::Debug for ChangeGeneratedColumnJobSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let expected = &self.expected_generated_column_definition;
        let old_call = expected.function_call();
        let new_call = &self.new_function_call;
        let old_field_ids: Vec<_> = old_call
            .arguments()
            .iter()
            .filter_map(|(_, argument)| argument.field_id())
            .collect();
        let new_field_ids: Vec<_> = new_call
            .arguments()
            .iter()
            .filter_map(|(_, argument)| argument.field_id())
            .collect();
        f.debug_struct("ChangeGeneratedColumnJobSpec")
            .field("output_field_id", &expected.output_field_id())
            .field("old_function_id", &old_call.function_id().as_str())
            .field("new_function_id", &new_call.function_id().as_str())
            .field("dependency_epoch", &expected.dependency_epoch())
            .field("materialized_epoch", &expected.materialized_epoch())
            .field("old_argument_count", &old_call.arguments().len())
            .field("new_argument_count", &new_call.arguments().len())
            .field("old_field_ids", &old_field_ids)
            .field("new_field_ids", &new_field_ids)
            .finish()
    }
}

// Do not derive Debug: nested GeneratedColumnDefinition / FunctionCall may
// carry typed literal payloads on the trusted change-generated-column wire.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangeGeneratedColumnJobSpecWire {
    format_version: u32,
    expected_generated_column_definition: GeneratedColumnDefinition,
    new_function_call: FunctionCall,
}

impl Serialize for ChangeGeneratedColumnJobSpec {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_wire().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ChangeGeneratedColumnJobSpec {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ChangeGeneratedColumnJobSpecWire::deserialize(deserializer)?;
        Self::from_wire(wire).map_err(D::Error::custom)
    }
}
