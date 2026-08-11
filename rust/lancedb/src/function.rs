// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Immutable first-class Function and generated-column value model (B1a/B1c).
//!
//! This module defines transport and metadata value types only. It does not
//! provide catalogs, job execution, query planning, or generated-column runtime.

mod change_generated_column;
mod create_generated_column;
mod definition;
mod refresh_generated_column;
mod registration;

pub use change_generated_column::ChangeGeneratedColumnJobSpec;
pub use create_generated_column::CreateGeneratedColumnJobSpec;
pub use definition::{FunctionCapability, FunctionDefinition, PythonFunctionDefinition};
pub use refresh_generated_column::RefreshGeneratedColumnJobSpec;
pub use registration::RegisterFunctionJobSpec;

use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::reader::FileReader;
use arrow_schema::{DataType, Field, Schema};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::ipc::{batches_to_ipc_file, schema_to_ipc_file};
use crate::{Error, Result};

/// Field metadata key used to store a [`GeneratedColumnDefinition`] JSON document.
pub const GENERATED_COLUMN_METADATA_KEY: &str = "lancedb::generated_column";

const FORMAT_VERSION_V1: u32 = 1;
const TYPE_IPC_FIELD_NAME: &str = "";
const LITERAL_IPC_FIELD_NAME: &str = "value";

fn invalid_input(message: impl Into<String>) -> Error {
    Error::InvalidInput {
        message: message.into(),
    }
}

fn encode_type_ipc(data_type: &DataType) -> Result<Vec<u8>> {
    let schema = Schema::new(vec![Field::new(
        TYPE_IPC_FIELD_NAME,
        data_type.clone(),
        true,
    )]);
    schema_to_ipc_file(&schema)
}

fn decode_type_ipc(bytes: &[u8]) -> Result<DataType> {
    let reader = FileReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| invalid_input(format!("invalid Arrow IPC for data type: {e}")))?;
    let schema = reader.schema();
    if schema.fields().len() != 1 {
        return Err(invalid_input(
            "type data_type_ipc must be a schema-only Arrow IPC file with exactly one field",
        ));
    }
    if reader.num_batches() != 0 {
        return Err(invalid_input(
            "type data_type_ipc must be schema-only Arrow IPC (no record batches)",
        ));
    }
    let data_type = schema.field(0).data_type().clone();
    let canonical = encode_type_ipc(&data_type)?;
    if canonical.as_slice() != bytes {
        return Err(invalid_input(
            "type data_type_ipc must be canonical schema-only Arrow IPC with no trailing bytes",
        ));
    }
    Ok(data_type)
}

fn encode_type_ipc_b64(data_type: &DataType) -> Result<String> {
    Ok(BASE64.encode(encode_type_ipc(data_type)?))
}

fn decode_type_ipc_b64(encoded: &str) -> Result<DataType> {
    let bytes = BASE64
        .decode(encoded.as_bytes())
        .map_err(|e| invalid_input(format!("invalid base64 data_type_ipc: {e}")))?;
    decode_type_ipc(&bytes)
}

fn encode_literal_ipc(array: &ArrayRef) -> Result<Vec<u8>> {
    if array.len() != 1 {
        return Err(invalid_input(
            "literal argument must contain exactly one row",
        ));
    }
    let schema = Arc::new(Schema::new(vec![Field::new(
        LITERAL_IPC_FIELD_NAME,
        array.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![array.clone()])?;
    batches_to_ipc_file(&[batch])
}

fn decode_literal_ipc(bytes: &[u8]) -> Result<ArrayRef> {
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| invalid_input(format!("invalid Arrow IPC for literal: {e}")))?;
    let schema = reader.schema();
    if schema.fields().len() != 1 {
        return Err(invalid_input("literal ipc must contain exactly one column"));
    }
    if reader.num_batches() != 1 {
        return Err(invalid_input(
            "literal ipc must contain exactly one record batch",
        ));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid_input("literal ipc must contain exactly one record batch"))?
        .map_err(|e| invalid_input(format!("invalid Arrow IPC for literal batch: {e}")))?;
    if batch.num_columns() != 1 {
        return Err(invalid_input("literal ipc must contain exactly one column"));
    }
    if batch.num_rows() != 1 {
        return Err(invalid_input("literal ipc must contain exactly one row"));
    }
    let array = batch.column(0).clone();
    let canonical = encode_literal_ipc(&array)?;
    if canonical.as_slice() != bytes {
        return Err(invalid_input(
            "literal ipc must be canonical one-batch one-field one-row Arrow IPC with no trailing bytes",
        ));
    }
    Ok(array)
}

fn encode_literal_ipc_b64(array: &ArrayRef) -> Result<String> {
    Ok(BASE64.encode(encode_literal_ipc(array)?))
}

fn decode_literal_ipc_b64(encoded: &str) -> Result<ArrayRef> {
    let bytes = BASE64
        .decode(encoded.as_bytes())
        .map_err(|e| invalid_input(format!("invalid base64 literal ipc: {e}")))?;
    decode_literal_ipc(&bytes)
}

/// Opaque, non-empty identifier for a [`Function`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionId {
    value: String,
}

impl FunctionId {
    /// Create a new opaque function id.
    ///
    /// Returns an error when `value` is empty.
    pub fn try_new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(invalid_input("FunctionId must be non-empty"));
        }
        Ok(Self { value })
    }

    /// Borrow the opaque id string.
    pub fn as_str(&self) -> &str {
        &self.value
    }
}

/// A named typed parameter in a [`FunctionSignature`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionParameter {
    name: String,
    data_type: DataType,
}

impl FunctionParameter {
    /// Create a parameter with the given name and Arrow data type.
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }

    /// Parameter name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Parameter Arrow data type.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

/// Output type description for a [`FunctionSignature`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionOutput {
    data_type: DataType,
    nullable: bool,
}

impl FunctionOutput {
    /// Create an output type with Arrow data type and nullability.
    pub fn new(data_type: DataType, nullable: bool) -> Self {
        Self {
            data_type,
            nullable,
        }
    }

    /// Output Arrow data type.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Whether the output may be null.
    pub fn nullable(&self) -> bool {
        self.nullable
    }
}

/// Ordered parameter list plus a single output type for a [`Function`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionSignature {
    parameters: Vec<FunctionParameter>,
    output: FunctionOutput,
}

impl FunctionSignature {
    /// Create a signature with unique non-empty parameter names.
    pub fn try_new(parameters: Vec<FunctionParameter>, output: FunctionOutput) -> Result<Self> {
        let mut seen = HashSet::with_capacity(parameters.len());
        for parameter in &parameters {
            if parameter.name.is_empty() {
                return Err(invalid_input(
                    "FunctionSignature parameter names must be non-empty",
                ));
            }
            if !seen.insert(parameter.name.as_str()) {
                return Err(invalid_input(format!(
                    "duplicate FunctionSignature parameter name `{}`",
                    parameter.name
                )));
            }
        }
        Ok(Self { parameters, output })
    }

    /// Ordered parameters.
    pub fn parameters(&self) -> &[FunctionParameter] {
        &self.parameters
    }

    /// Output type.
    pub fn output(&self) -> &FunctionOutput {
        &self.output
    }

    fn to_wire(&self) -> Result<SignatureWire> {
        let parameters = self
            .parameters
            .iter()
            .map(|parameter| {
                Ok(ParameterWire {
                    name: parameter.name.clone(),
                    data_type_ipc: encode_type_ipc_b64(&parameter.data_type)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(SignatureWire {
            parameters,
            output: OutputWire {
                data_type_ipc: encode_type_ipc_b64(&self.output.data_type)?,
                nullable: self.output.nullable,
            },
        })
    }

    fn from_wire(wire: SignatureWire) -> Result<Self> {
        let parameters = wire
            .parameters
            .into_iter()
            .map(|parameter| {
                Ok(FunctionParameter::new(
                    parameter.name,
                    decode_type_ipc_b64(&parameter.data_type_ipc)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let output = FunctionOutput::new(
            decode_type_ipc_b64(&wire.output.data_type_ipc)?,
            wire.output.nullable,
        );
        Self::try_new(parameters, output)
    }
}

/// Immutable first-class function value (format version 1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Function {
    id: FunctionId,
    signature: FunctionSignature,
}

impl Function {
    /// Create a function from an opaque id and signature.
    pub fn new(id: FunctionId, signature: FunctionSignature) -> Self {
        Self { id, signature }
    }

    /// Opaque function id.
    pub fn id(&self) -> &FunctionId {
        &self.id
    }

    /// Function signature.
    pub fn signature(&self) -> &FunctionSignature {
        &self.signature
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FunctionWire {
    format_version: u32,
    id: String,
    signature: SignatureWire,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SignatureWire {
    parameters: Vec<ParameterWire>,
    output: OutputWire,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ParameterWire {
    name: String,
    data_type_ipc: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputWire {
    data_type_ipc: String,
    nullable: bool,
}

impl Function {
    fn to_wire(&self) -> Result<FunctionWire> {
        Ok(FunctionWire {
            format_version: FORMAT_VERSION_V1,
            id: self.id.value.clone(),
            signature: self.signature.to_wire()?,
        })
    }

    fn from_wire(wire: FunctionWire) -> Result<Self> {
        if wire.format_version != FORMAT_VERSION_V1 {
            return Err(invalid_input(format!(
                "unsupported Function format_version {}",
                wire.format_version
            )));
        }
        let id = FunctionId::try_new(wire.id)?;
        let signature = FunctionSignature::from_wire(wire.signature)?;
        Ok(Self { id, signature })
    }
}

impl Serialize for Function {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_wire()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Function {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FunctionWire::deserialize(deserializer)?;
        Self::from_wire(wire).map_err(D::Error::custom)
    }
}

/// A typed argument bound to a function parameter.
#[derive(Debug, Clone)]
pub struct FunctionArgument {
    kind: FunctionArgumentKind,
}

#[derive(Debug, Clone)]
enum FunctionArgumentKind {
    Field { field_id: i32, data_type: DataType },
    Literal { array: ArrayRef },
}

impl PartialEq for FunctionArgument {
    fn eq(&self, other: &Self) -> bool {
        match (&self.kind, &other.kind) {
            (
                FunctionArgumentKind::Field {
                    field_id: a_id,
                    data_type: a_ty,
                },
                FunctionArgumentKind::Field {
                    field_id: b_id,
                    data_type: b_ty,
                },
            ) => a_id == b_id && a_ty == b_ty,
            (
                FunctionArgumentKind::Literal { array: a },
                FunctionArgumentKind::Literal { array: b },
            ) => a.as_ref() == b.as_ref(),
            _ => false,
        }
    }
}

impl Eq for FunctionArgument {}

impl FunctionArgument {
    /// Create a field argument referencing a non-negative stable field id.
    pub fn try_field(field_id: i32, data_type: DataType) -> Result<Self> {
        if field_id < 0 {
            return Err(invalid_input(
                "field argument field_id must be non-negative",
            ));
        }
        Ok(Self {
            kind: FunctionArgumentKind::Field {
                field_id,
                data_type,
            },
        })
    }

    /// Create a literal argument from a one-row Arrow array (including typed NULL).
    pub fn try_literal(array: ArrayRef) -> Result<Self> {
        if array.len() != 1 {
            return Err(invalid_input(
                "literal argument must contain exactly one row",
            ));
        }
        // Validate that the value has a canonical IPC encoding.
        let _ = encode_literal_ipc(&array)?;
        Ok(Self {
            kind: FunctionArgumentKind::Literal { array },
        })
    }

    /// Stable field id when this argument is a field reference.
    pub fn field_id(&self) -> Option<i32> {
        match &self.kind {
            FunctionArgumentKind::Field { field_id, .. } => Some(*field_id),
            FunctionArgumentKind::Literal { .. } => None,
        }
    }

    /// Arrow data type expected or carried by this argument.
    pub fn data_type(&self) -> &DataType {
        match &self.kind {
            FunctionArgumentKind::Field { data_type, .. } => data_type,
            FunctionArgumentKind::Literal { array } => array.data_type(),
        }
    }

    /// One-row literal array when this argument is a literal.
    pub fn literal_array(&self) -> Option<&ArrayRef> {
        match &self.kind {
            FunctionArgumentKind::Literal { array } => Some(array),
            FunctionArgumentKind::Field { .. } => None,
        }
    }

    /// Whether this argument is a typed NULL literal.
    pub fn is_typed_null(&self) -> bool {
        match &self.kind {
            FunctionArgumentKind::Literal { array } => array.is_null(0),
            FunctionArgumentKind::Field { .. } => false,
        }
    }

    fn to_value_wire(&self) -> Result<ArgumentValueWire> {
        match &self.kind {
            FunctionArgumentKind::Field {
                field_id,
                data_type,
            } => Ok(ArgumentValueWire::Field {
                field_id: *field_id,
                data_type_ipc: encode_type_ipc_b64(data_type)?,
            }),
            FunctionArgumentKind::Literal { array } => Ok(ArgumentValueWire::Literal {
                ipc: encode_literal_ipc_b64(array)?,
            }),
        }
    }

    fn from_value_wire(wire: ArgumentValueWire) -> Result<Self> {
        match wire {
            ArgumentValueWire::Field {
                field_id,
                data_type_ipc,
            } => Self::try_field(field_id, decode_type_ipc_b64(&data_type_ipc)?),
            ArgumentValueWire::Literal { ipc } => Self::try_literal(decode_literal_ipc_b64(&ipc)?),
        }
    }
}

/// A function id plus named argument bindings.
///
/// [`FunctionCall::try_new`] validates against a [`Function`] and normalizes
/// bindings to signature parameter order. Structural decode via
/// [`Deserialize`] or [`GeneratedColumnDefinition::from_metadata_json`] does
/// not perform that catalog validation: decoded calls must pass
/// [`FunctionCall::validate_against`] before execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionCall {
    function_id: FunctionId,
    arguments: Vec<(String, FunctionArgument)>,
}

impl FunctionCall {
    /// Bind arguments to `function`, normalizing to signature parameter order.
    pub fn try_new(function: &Function, bindings: Vec<(String, FunctionArgument)>) -> Result<Self> {
        let parameters = function.signature().parameters();
        if bindings.len() != parameters.len() {
            return Err(invalid_input(format!(
                "FunctionCall requires exactly {} bindings, got {}",
                parameters.len(),
                bindings.len()
            )));
        }

        let mut seen = HashSet::with_capacity(bindings.len());
        let mut by_name = std::collections::HashMap::with_capacity(bindings.len());
        for (name, argument) in bindings {
            if !seen.insert(name.clone()) {
                return Err(invalid_input(format!(
                    "duplicate FunctionCall binding for parameter `{name}`"
                )));
            }
            by_name.insert(name, argument);
        }

        let mut arguments = Vec::with_capacity(parameters.len());
        for parameter in parameters {
            let Some(argument) = by_name.remove(parameter.name()) else {
                return Err(invalid_input(format!(
                    "missing FunctionCall binding for parameter `{}`",
                    parameter.name()
                )));
            };
            if argument.data_type() != parameter.data_type() {
                return Err(invalid_input(format!(
                    "FunctionCall argument type mismatch for parameter `{}`",
                    parameter.name()
                )));
            }
            arguments.push((parameter.name().to_string(), argument));
        }

        if let Some((unknown, _)) = by_name.into_iter().next() {
            return Err(invalid_input(format!(
                "unknown FunctionCall parameter `{unknown}`"
            )));
        }

        Ok(Self {
            function_id: function.id().clone(),
            arguments,
        })
    }

    /// Validate this call against a catalog [`Function`] identity and signature.
    ///
    /// Requires exact [`FunctionId`] equality, exact argument count, each
    /// parameter name in exact signature order, and exact Arrow type equality
    /// for every binding. This does not check that field arguments exist or
    /// match types in a table schema.
    ///
    /// Calls produced by [`Self::try_new`] always pass for the same
    /// `function`. Structurally decoded calls (for example from
    /// [`GeneratedColumnDefinition::from_metadata_json`]) must pass this check
    /// before execution.
    pub fn validate_against(&self, function: &Function) -> Result<()> {
        if self.function_id != *function.id() {
            return Err(invalid_input(format!(
                "FunctionCall function_id mismatch: call has `{}`, function has `{}`",
                self.function_id.as_str(),
                function.id().as_str()
            )));
        }

        let parameters = function.signature().parameters();
        if self.arguments.len() != parameters.len() {
            return Err(invalid_input(format!(
                "FunctionCall requires exactly {} bindings, got {}",
                parameters.len(),
                self.arguments.len()
            )));
        }

        for (index, parameter) in parameters.iter().enumerate() {
            let (bound_name, argument) = &self.arguments[index];
            if bound_name != parameter.name() {
                return Err(invalid_input(format!(
                    "FunctionCall parameter name mismatch at position {index}: call has `{bound_name}`, signature has `{}`",
                    parameter.name()
                )));
            }
            if argument.data_type() != parameter.data_type() {
                return Err(invalid_input(format!(
                    "FunctionCall argument type mismatch for parameter `{}`",
                    parameter.name()
                )));
            }
        }

        Ok(())
    }

    /// Opaque function id stored on the call.
    pub fn function_id(&self) -> &FunctionId {
        &self.function_id
    }

    /// Bindings as `(parameter_name, argument)`.
    ///
    /// After [`Self::try_new`], bindings are in signature parameter order.
    /// Structurally decoded calls may not be ordered or complete until
    /// [`Self::validate_against`] succeeds.
    pub fn arguments(&self) -> &[(String, FunctionArgument)] {
        &self.arguments
    }

    fn to_wire(&self) -> Result<FunctionCallWire> {
        let arguments = self
            .arguments
            .iter()
            .map(|(parameter, argument)| {
                Ok(ArgumentBindingWire {
                    parameter: parameter.clone(),
                    value: argument.to_value_wire()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(FunctionCallWire {
            function_id: self.function_id.value.clone(),
            arguments,
        })
    }

    fn from_wire(wire: FunctionCallWire) -> Result<Self> {
        let function_id = FunctionId::try_new(wire.function_id)?;
        let mut seen = HashSet::with_capacity(wire.arguments.len());
        let mut arguments = Vec::with_capacity(wire.arguments.len());
        for binding in wire.arguments {
            if binding.parameter.is_empty() {
                return Err(invalid_input(
                    "FunctionCall parameter names must be non-empty",
                ));
            }
            if !seen.insert(binding.parameter.clone()) {
                return Err(invalid_input(format!(
                    "duplicate FunctionCall binding for parameter `{}`",
                    binding.parameter
                )));
            }
            arguments.push((
                binding.parameter,
                FunctionArgument::from_value_wire(binding.value)?,
            ));
        }
        Ok(Self {
            function_id,
            arguments,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FunctionCallWire {
    function_id: String,
    arguments: Vec<ArgumentBindingWire>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArgumentBindingWire {
    parameter: String,
    value: ArgumentValueWire,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum ArgumentValueWire {
    #[serde(rename = "field")]
    Field {
        field_id: i32,
        data_type_ipc: String,
    },
    #[serde(rename = "literal")]
    Literal { ipc: String },
}

impl Serialize for FunctionCall {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_wire()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for FunctionCall {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FunctionCallWire::deserialize(deserializer)?;
        Self::from_wire(wire).map_err(D::Error::custom)
    }
}

/// Projection completeness of a generated column relative to its dependency epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum GeneratedColumnStatus {
    /// `materialized_epoch == dependency_epoch`.
    Complete,
    /// `materialized_epoch < dependency_epoch`.
    Incomplete,
}

/// Strict version-1 generated column metadata value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedColumnDefinition {
    output_field_id: i32,
    function_call: FunctionCall,
    dependency_epoch: u64,
    materialized_epoch: u64,
}

impl GeneratedColumnDefinition {
    /// Create a generated-column definition.
    ///
    /// Rejects a negative `output_field_id` and `materialized_epoch > dependency_epoch`.
    pub fn try_new(
        output_field_id: i32,
        function_call: FunctionCall,
        dependency_epoch: u64,
        materialized_epoch: u64,
    ) -> Result<Self> {
        if output_field_id < 0 {
            return Err(invalid_input(
                "generated column output_field_id must be non-negative",
            ));
        }
        if materialized_epoch > dependency_epoch {
            return Err(invalid_input(
                "materialized_epoch must not be greater than dependency_epoch",
            ));
        }
        Ok(Self {
            output_field_id,
            function_call,
            dependency_epoch,
            materialized_epoch,
        })
    }

    /// Wire format version (always 1 for this type).
    pub fn format_version(&self) -> u32 {
        FORMAT_VERSION_V1
    }

    /// Output field id this definition applies to.
    pub fn output_field_id(&self) -> i32 {
        self.output_field_id
    }

    /// Embedded function call.
    pub fn function_call(&self) -> &FunctionCall {
        &self.function_call
    }

    /// Dependency epoch.
    pub fn dependency_epoch(&self) -> u64 {
        self.dependency_epoch
    }

    /// Materialized epoch.
    pub fn materialized_epoch(&self) -> u64 {
        self.materialized_epoch
    }

    /// Completeness status derived from the epochs.
    pub fn status(&self) -> GeneratedColumnStatus {
        if self.materialized_epoch == self.dependency_epoch {
            GeneratedColumnStatus::Complete
        } else {
            GeneratedColumnStatus::Incomplete
        }
    }

    /// Serialize to the strict metadata JSON string.
    ///
    /// Byte-identical to [`Serialize`] for the same value: both paths share
    /// the same private wire-encoding step.
    pub fn to_metadata_json(&self) -> Result<String> {
        let wire = self.to_wire()?;
        serde_json::to_string(&wire).map_err(|e| invalid_input(format!("serialize metadata: {e}")))
    }

    /// Deserialize metadata JSON and require `expected_output_field_id`.
    ///
    /// Validation order (fail-closed precedence for multi-invalid payloads):
    /// parse JSON and the strict private wire shape, unsupported
    /// `format_version`, negative `output_field_id`, mismatch with
    /// `expected_output_field_id`, `materialized_epoch > dependency_epoch`,
    /// then structural [`FunctionCall`] decode.
    ///
    /// Generic [`Deserialize`] shares the same path without the external
    /// `expected_output_field_id` check (version, non-negative output id,
    /// epoch ordering, then [`FunctionCall`]).
    ///
    /// Decoding is structural: the embedded [`FunctionCall`] is not validated
    /// against a catalog [`Function`]. Callers that will execute the call must
    /// look up the function and run [`FunctionCall::validate_against`] first.
    /// Structural decode still allows query/invalidation paths to inspect
    /// stored field ids without a catalog lookup.
    pub fn from_metadata_json(json: &str, expected_output_field_id: i32) -> Result<Self> {
        let value: Value = serde_json::from_str(json)
            .map_err(|e| invalid_input(format!("invalid generated column metadata JSON: {e}")))?;
        let wire: GeneratedColumnWire = serde_json::from_value(value)
            .map_err(|e| invalid_input(format!("invalid generated column metadata: {e}")))?;
        Self::from_wire_validated(wire, Some(expected_output_field_id))
    }

    /// Increment `dependency_epoch` by one using checked arithmetic.
    ///
    /// On overflow, returns an error and leaves the definition unchanged.
    pub fn invalidate(&mut self) -> Result<()> {
        let next = self
            .dependency_epoch
            .checked_add(1)
            .ok_or_else(|| invalid_input("dependency_epoch overflow"))?;
        self.dependency_epoch = next;
        Ok(())
    }

    /// Set `materialized_epoch` to the current `dependency_epoch`.
    pub fn mark_materialized(&mut self) {
        self.materialized_epoch = self.dependency_epoch;
    }

    fn to_wire(&self) -> Result<GeneratedColumnWire> {
        Ok(GeneratedColumnWire {
            format_version: FORMAT_VERSION_V1,
            output_field_id: self.output_field_id,
            function_call: self.function_call.to_wire()?,
            dependency_epoch: self.dependency_epoch,
            materialized_epoch: self.materialized_epoch,
        })
    }

    fn from_wire(wire: GeneratedColumnWire) -> Result<Self> {
        Self::from_wire_validated(wire, None)
    }

    /// Shared wire validation for [`Deserialize`] and [`Self::from_metadata_json`].
    ///
    /// Order: format version, non-negative `output_field_id`, optional expected
    /// field-id match, epoch ordering, then structural [`FunctionCall`] decode.
    /// The optional expected check is applied immediately after the non-negative
    /// check so metadata decode does not postpone external context validation.
    fn from_wire_validated(
        wire: GeneratedColumnWire,
        expected_output_field_id: Option<i32>,
    ) -> Result<Self> {
        if wire.format_version != FORMAT_VERSION_V1 {
            return Err(invalid_input(format!(
                "unsupported generated column format_version {}",
                wire.format_version
            )));
        }
        if wire.output_field_id < 0 {
            return Err(invalid_input(
                "generated column output_field_id must be non-negative",
            ));
        }
        if let Some(expected) = expected_output_field_id
            && wire.output_field_id != expected
        {
            return Err(invalid_input(format!(
                "generated column output_field_id mismatch: metadata has {}, expected {}",
                wire.output_field_id, expected
            )));
        }
        if wire.materialized_epoch > wire.dependency_epoch {
            return Err(invalid_input(
                "materialized_epoch must not be greater than dependency_epoch",
            ));
        }
        let function_call = FunctionCall::from_wire(wire.function_call)?;
        Ok(Self {
            output_field_id: wire.output_field_id,
            function_call,
            dependency_epoch: wire.dependency_epoch,
            materialized_epoch: wire.materialized_epoch,
        })
    }
}

impl Serialize for GeneratedColumnDefinition {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_wire()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GeneratedColumnDefinition {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = GeneratedColumnWire::deserialize(deserializer)?;
        Self::from_wire(wire).map_err(D::Error::custom)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GeneratedColumnWire {
    format_version: u32,
    output_field_id: i32,
    function_call: FunctionCallWire,
    dependency_epoch: u64,
    materialized_epoch: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;

    #[test]
    fn type_ipc_rejects_non_canonical_field_name() {
        let schema = Schema::new(vec![Field::new("value", DataType::Int32, true)]);
        let bytes = schema_to_ipc_file(&schema).expect("schema ipc");
        let err = decode_type_ipc(&bytes).expect_err("non-canonical field name");
        let message = err.to_string().to_lowercase();
        assert!(
            message.contains("canonical") || message.contains("schema-only"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn type_ipc_round_trip_is_byte_identical() {
        let data_type = DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("UTC".into()));
        let bytes = encode_type_ipc(&data_type).expect("encode");
        let decoded = decode_type_ipc(&bytes).expect("decode");
        assert_eq!(decoded, data_type);
        assert_eq!(encode_type_ipc(&decoded).expect("re-encode"), bytes);
    }

    #[test]
    fn literal_ipc_rejects_non_canonical_field_name() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef],
        )
        .expect("batch");
        let bytes = batches_to_ipc_file(&[batch]).expect("literal ipc");
        let err = decode_literal_ipc(&bytes).expect_err("non-canonical field name");
        let message = err.to_string().to_lowercase();
        assert!(
            message.contains("canonical") || message.contains("literal"),
            "unexpected error: {message}"
        );
    }
}
