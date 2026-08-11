// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Immutable RegisterFunctionJobSpec registration Job operation input (B1d / FF-008).
//!
//! This type is Job operation input only. It does not execute registration,
//! upsert into a catalog, mint identity, or manage Job lifecycle.

use std::fmt;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{FunctionDefinition, FunctionId, invalid_input};
use crate::Result;

const FORMAT_VERSION_V1: u32 = 1;

/// Immutable Job operation input for registering a first-class Function
/// (format version 1).
///
/// `expected_current_function_id` is a precondition only:
/// - [`None`] means create-if-absent (no current Function is expected).
/// - [`Some`] with an exact opaque [`FunctionId`] means conditional replace of
///   that current Function.
///
/// This type does not perform catalog execution or upsert.
#[derive(Clone, PartialEq, Eq)]
pub struct RegisterFunctionJobSpec {
    name: String,
    definition: FunctionDefinition,
    expected_current_function_id: Option<FunctionId>,
}

impl RegisterFunctionJobSpec {
    /// Create a registration Job operation input.
    ///
    /// Rejects an empty `name`. Nested definition validation is enforced by
    /// [`FunctionDefinition`]. When `expected_current_function_id` is
    /// [`Some`], emptiness is enforced by [`FunctionId::try_new`].
    ///
    /// - `expected_current_function_id = None`: create-if-absent.
    /// - `expected_current_function_id = Some(id)`: conditional replace of the
    ///   Function with that exact opaque id.
    pub fn try_new(
        name: impl Into<String>,
        definition: FunctionDefinition,
        expected_current_function_id: Option<FunctionId>,
    ) -> Result<Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(invalid_input(
                "RegisterFunctionJobSpec name must be non-empty",
            ));
        }
        Ok(Self {
            name,
            definition,
            expected_current_function_id,
        })
    }

    /// Wire format version (always 1 for this type).
    pub fn format_version(&self) -> u32 {
        FORMAT_VERSION_V1
    }

    /// Catalog Function name to register.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Nested registration definition (exact FF-007 [`FunctionDefinition`]).
    pub fn definition(&self) -> &FunctionDefinition {
        &self.definition
    }

    /// Precondition on the current Function id.
    ///
    /// [`None`] is create-if-absent. [`Some`] is conditional replace of that
    /// exact opaque id.
    pub fn expected_current_function_id(&self) -> Option<&FunctionId> {
        self.expected_current_function_id.as_ref()
    }

    fn to_wire(&self) -> RegisterFunctionJobSpecWire {
        RegisterFunctionJobSpecWire {
            format_version: FORMAT_VERSION_V1,
            name: self.name.clone(),
            definition: self.definition.clone(),
            expected_current_function_id: self
                .expected_current_function_id
                .as_ref()
                .map(|id| id.as_str().to_string()),
        }
    }

    fn from_wire(wire: RegisterFunctionJobSpecWire) -> Result<Self> {
        if wire.format_version != FORMAT_VERSION_V1 {
            return Err(invalid_input(format!(
                "unsupported RegisterFunctionJobSpec format_version {}",
                wire.format_version
            )));
        }
        let expected_current_function_id = match wire.expected_current_function_id {
            None => None,
            Some(id) => Some(FunctionId::try_new(id)?),
        };
        Self::try_new(wire.name, wire.definition, expected_current_function_id)
    }
}

impl fmt::Debug for RegisterFunctionJobSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegisterFunctionJobSpec")
            .field("name", &self.name)
            .field("definition", &self.definition)
            .field(
                "expected_current_function_id",
                &self.expected_current_function_id,
            )
            .finish()
    }
}

// Do not derive Debug: nested FunctionDefinition carries Python source and
// secret references on the trusted registration wire.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterFunctionJobSpecWire {
    format_version: u32,
    name: String,
    definition: FunctionDefinition,
    expected_current_function_id: Option<String>,
}

impl Serialize for RegisterFunctionJobSpec {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_wire().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RegisterFunctionJobSpec {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RegisterFunctionJobSpecWire::deserialize(deserializer)?;
        Self::from_wire(wire).map_err(D::Error::custom)
    }
}
