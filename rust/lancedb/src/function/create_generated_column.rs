// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Immutable CreateGeneratedColumnJobSpec create-generated-column Job operation
//! input (FF-009).
//!
//! This type is Job operation input only. It does not allocate output fields,
//! construct [`super::GeneratedColumnDefinition`], mutate tables, or execute
//! Jobs.

use std::fmt;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{Function, FunctionCall, invalid_input};
use crate::Result;

const FORMAT_VERSION_V1: u32 = 1;

/// Immutable Job operation input for creating a generated column (format
/// version 1).
///
/// Semantic fields are exactly `column_name` and [`FunctionCall`]. Wire keys
/// are exactly `format_version`, `column_name`, and `function_call`.
///
/// Construction via [`Self::try_new`] validates the call against a catalog
/// [`Function`]. Structural deserialize does not; execution consumers must
/// call [`Self::validate_against`].
#[derive(Clone, PartialEq, Eq)]
pub struct CreateGeneratedColumnJobSpec {
    column_name: String,
    function_call: FunctionCall,
}

impl CreateGeneratedColumnJobSpec {
    /// Create a create-generated-column Job operation input.
    ///
    /// Rejects an empty `column_name`. Requires
    /// [`FunctionCall::validate_against`] to succeed for `function` before
    /// returning (exact Function ID, parameter name/order, argument count, and
    /// Arrow type equality).
    pub fn try_new(
        column_name: impl Into<String>,
        function: &Function,
        call: FunctionCall,
    ) -> Result<Self> {
        let column_name = column_name.into();
        if column_name.is_empty() {
            return Err(invalid_input(
                "CreateGeneratedColumnJobSpec column_name must be non-empty",
            ));
        }
        call.validate_against(function)?;
        Ok(Self {
            column_name,
            function_call: call,
        })
    }

    /// Wire format version (always 1 for this type).
    pub fn format_version(&self) -> u32 {
        FORMAT_VERSION_V1
    }

    /// Target generated column name.
    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    /// Embedded function call.
    pub fn function_call(&self) -> &FunctionCall {
        &self.function_call
    }

    /// Validate the embedded call against a catalog [`Function`].
    ///
    /// Structural decode does not perform this check. Execution consumers must
    /// call this before using the call.
    pub fn validate_against(&self, function: &Function) -> Result<()> {
        self.function_call.validate_against(function)
    }

    fn to_wire(&self) -> CreateGeneratedColumnJobSpecWire {
        CreateGeneratedColumnJobSpecWire {
            format_version: FORMAT_VERSION_V1,
            column_name: self.column_name.clone(),
            function_call: self.function_call.clone(),
        }
    }

    fn from_wire(wire: CreateGeneratedColumnJobSpecWire) -> Result<Self> {
        if wire.format_version != FORMAT_VERSION_V1 {
            return Err(invalid_input(format!(
                "unsupported CreateGeneratedColumnJobSpec format_version {}",
                wire.format_version
            )));
        }
        if wire.column_name.is_empty() {
            return Err(invalid_input(
                "CreateGeneratedColumnJobSpec column_name must be non-empty",
            ));
        }
        Ok(Self {
            column_name: wire.column_name,
            function_call: wire.function_call,
        })
    }
}

impl fmt::Debug for CreateGeneratedColumnJobSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let field_ids: Vec<_> = self
            .function_call
            .arguments()
            .iter()
            .filter_map(|(_, argument)| argument.field_id())
            .collect();
        f.debug_struct("CreateGeneratedColumnJobSpec")
            .field("column_name", &self.column_name)
            .field("function_id", &self.function_call.function_id().as_str())
            .field("argument_count", &self.function_call.arguments().len())
            .field("field_ids", &field_ids)
            .finish()
    }
}

// Do not derive Debug: nested FunctionCall may carry typed literal payloads on
// the trusted create-generated-column wire.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateGeneratedColumnJobSpecWire {
    format_version: u32,
    column_name: String,
    function_call: FunctionCall,
}

impl Serialize for CreateGeneratedColumnJobSpec {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_wire().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CreateGeneratedColumnJobSpec {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = CreateGeneratedColumnJobSpecWire::deserialize(deserializer)?;
        Self::from_wire(wire).map_err(D::Error::custom)
    }
}
