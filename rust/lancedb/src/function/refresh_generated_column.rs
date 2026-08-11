// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Immutable RefreshGeneratedColumnJobSpec refresh-generated-column Job
//! operation input (FF-010).
//!
//! This type is Job operation input only. It does not look up catalogs or
//! tables, execute Jobs, stage artifacts, call Lance, or mutate epochs.

use std::fmt;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{Function, GeneratedColumnDefinition, invalid_input};
use crate::Result;

const FORMAT_VERSION_V1: u32 = 1;

/// Immutable Job operation input for refreshing a generated column (format
/// version 1).
///
/// Semantic field is exactly the nested [`GeneratedColumnDefinition`]. Wire
/// keys are exactly `format_version` and `generated_column_definition`.
///
/// Construction via [`Self::try_new`] validates the nested call against a
/// catalog [`Function`]. Structural deserialize does not; execution consumers
/// must call [`Self::validate_against`], and later compare the full nested
/// definition to current field metadata in the pinned snapshot.
///
/// Both complete and incomplete definitions are accepted. Status is not a
/// constructor or wire restriction.
#[derive(Clone, PartialEq, Eq)]
pub struct RefreshGeneratedColumnJobSpec {
    generated_column_definition: GeneratedColumnDefinition,
}

impl RefreshGeneratedColumnJobSpec {
    /// Create a refresh-generated-column Job operation input.
    ///
    /// Requires [`crate::function::FunctionCall::validate_against`] to succeed
    /// for the nested call and `function` before returning (exact Function ID,
    /// parameter name/order, argument count, and Arrow type equality).
    pub fn try_new(
        function: &Function,
        generated_column_definition: GeneratedColumnDefinition,
    ) -> Result<Self> {
        generated_column_definition
            .function_call()
            .validate_against(function)?;
        Ok(Self {
            generated_column_definition,
        })
    }

    /// Wire format version (always 1 for this type).
    pub fn format_version(&self) -> u32 {
        FORMAT_VERSION_V1
    }

    /// Nested generated-column definition to refresh.
    pub fn generated_column_definition(&self) -> &GeneratedColumnDefinition {
        &self.generated_column_definition
    }

    /// Validate the nested call against a catalog [`Function`].
    ///
    /// Structural decode does not perform this check. Execution consumers must
    /// call this before using the call.
    pub fn validate_against(&self, function: &Function) -> Result<()> {
        self.generated_column_definition
            .function_call()
            .validate_against(function)
    }

    fn to_wire(&self) -> RefreshGeneratedColumnJobSpecWire {
        RefreshGeneratedColumnJobSpecWire {
            format_version: FORMAT_VERSION_V1,
            generated_column_definition: self.generated_column_definition.clone(),
        }
    }

    fn from_wire(wire: RefreshGeneratedColumnJobSpecWire) -> Result<Self> {
        if wire.format_version != FORMAT_VERSION_V1 {
            return Err(invalid_input(format!(
                "unsupported RefreshGeneratedColumnJobSpec format_version {}",
                wire.format_version
            )));
        }
        Ok(Self {
            generated_column_definition: wire.generated_column_definition,
        })
    }
}

impl fmt::Debug for RefreshGeneratedColumnJobSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let definition = &self.generated_column_definition;
        let call = definition.function_call();
        let field_ids: Vec<_> = call
            .arguments()
            .iter()
            .filter_map(|(_, argument)| argument.field_id())
            .collect();
        f.debug_struct("RefreshGeneratedColumnJobSpec")
            .field("output_field_id", &definition.output_field_id())
            .field("function_id", &call.function_id().as_str())
            .field("dependency_epoch", &definition.dependency_epoch())
            .field("materialized_epoch", &definition.materialized_epoch())
            .field("argument_count", &call.arguments().len())
            .field("field_ids", &field_ids)
            .finish()
    }
}

// Do not derive Debug: nested GeneratedColumnDefinition / FunctionCall may
// carry typed literal payloads on the trusted refresh wire.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RefreshGeneratedColumnJobSpecWire {
    format_version: u32,
    generated_column_definition: GeneratedColumnDefinition,
}

impl Serialize for RefreshGeneratedColumnJobSpec {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_wire().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RefreshGeneratedColumnJobSpec {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RefreshGeneratedColumnJobSpecWire::deserialize(deserializer)?;
        Self::from_wire(wire).map_err(D::Error::custom)
    }
}
