// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Computed columns.
//!
//! A computed column is defined by a rule rather than by values supplied at
//! write time. Declaring one commits the column carrying that rule in field
//! metadata but no data, so the cost does not scale with the table; a later
//! refresh fills the rows.
//!
//! The rule is tagged by kind ([`ComputedColumnKind`]) because kinds differ in
//! where the column's type and inputs come from. A SQL expression is
//! self-describing -- both are derived from the expression, so a caller writes
//! neither -- while a kind resolved through a registry cannot be typed without
//! consulting it. Registered Functions use an exact remote version plus a
//! schema-level Function binding; unknown newer kinds remain readable and fail
//! closed before mutation.
//!
//! [`computed_columns`] and [`computed_column_from_field`] read declarations
//! back off a schema.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef};
use datafusion_common::tree_node::TreeNode;
use datafusion_physical_plan::PhysicalExpr;
use lance::dataset::NewColumnTransform;
use lance_datafusion::planner::Planner;
use lance_namespace::models::{JsonArrowDataType, JsonArrowField, JsonArrowSchema};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::function::{FunctionApplication, FunctionBinding};
use crate::{Error, Result};

/// Field metadata key marking a column as computed. The value is `"true"`.
pub const COMPUTED_COLUMN_META_KEY: &str = "computed_column";

/// Field metadata key naming the kind of rule that defines the column.
pub const KIND_META_KEY: &str = "computed_column.kind";

/// Field metadata key holding the SQL expression that defines the column.
pub const EXPRESSION_META_KEY: &str = "computed_column.expression";

/// Field metadata key holding the column's inputs, as a JSON array of names.
pub const INPUTS_META_KEY: &str = "computed_column.inputs";

/// Field metadata key holding the Function binding identity.
pub const FUNCTION_BINDING_ID_META_KEY: &str = "computed_column.function.binding_id";

/// Field metadata key holding this sibling's ordered Function output ordinal.
pub const FUNCTION_OUTPUT_ORDINAL_META_KEY: &str = "computed_column.function.output_ordinal";

/// Schema metadata key holding all immutable Function bindings.
pub const FUNCTION_BINDINGS_META_KEY: &str = "lancedb::function_bindings";

/// Version of the schema-level Function binding envelope.
pub const FUNCTION_BINDINGS_VERSION: u32 = 1;

/// Value of [`KIND_META_KEY`] for a column defined by a SQL expression.
pub const SQL_KIND: &str = "sql";

/// Value of [`KIND_META_KEY`] for a registered Function binding.
pub const FUNCTION_KIND: &str = "function";

/// Synthetic result identity used when the entire Function result maps to one
/// table column (scalar or struct-as-one-column).
pub const WHOLE_RESULT_FIELD: &str = "$value";

/// The rule that defines a computed column's values.
///
/// Non-exhaustive: a kind added later is an additive change, and a caller that
/// only handles the kinds it knows keeps compiling.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ComputedColumnKind {
    /// A SQL expression evaluated by DataFusion. It is the whole definition:
    /// the column's type and its inputs are both derived from it.
    Sql {
        /// The expression.
        expression: String,
    },
    /// One physical output in an immutable registered-Function
    /// binding. The full binding lives in schema metadata.
    Function {
        /// Shared immutable binding identity.
        binding_id: String,
        /// Position of this field in the binding's ordered sibling outputs.
        output_ordinal: u32,
    },
    /// A kind this version does not understand, written by a newer one.
    ///
    /// Reported rather than hidden so a caller can tell a column it cannot
    /// refresh apart from one that was never computed. Nothing produces this.
    Unrecognized {
        /// The kind as it was found in the metadata.
        kind: String,
    },
}

/// A computed column's declaration, as read back from field metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputedColumn {
    /// Name of the computed column.
    pub name: String,
    /// The rule that defines it.
    pub kind: ComputedColumnKind,
    /// Columns the rule reads, recorded at declaration time.
    ///
    /// Outside the kind because every kind has inputs and the consumers that
    /// use them -- refresh planning, dependency ordering -- do not care which
    /// kind produced them. Where they come from does differ, and that is
    /// settled at declaration: derived from a SQL expression, supplied by the
    /// caller for a kind that cannot be parsed.
    pub inputs: Vec<String>,
}

/// Build the field metadata recording a SQL binding.
fn computed_column_metadata(expression: &str, inputs: &[String]) -> HashMap<String, String> {
    HashMap::from([
        (COMPUTED_COLUMN_META_KEY.to_string(), "true".to_string()),
        (KIND_META_KEY.to_string(), SQL_KIND.to_string()),
        (EXPRESSION_META_KEY.to_string(), expression.to_string()),
        (
            INPUTS_META_KEY.to_string(),
            serde_json::to_string(inputs).unwrap_or_else(|_| "[]".to_string()),
        ),
    ])
}

/// Build field metadata for one physical sibling of a Function binding.
pub fn function_computed_column_metadata(
    binding_id: &str,
    output_ordinal: u32,
    inputs: &[String],
) -> HashMap<String, String> {
    HashMap::from([
        (COMPUTED_COLUMN_META_KEY.to_string(), "true".to_string()),
        (KIND_META_KEY.to_string(), FUNCTION_KIND.to_string()),
        (
            FUNCTION_BINDING_ID_META_KEY.to_string(),
            binding_id.to_string(),
        ),
        (
            FUNCTION_OUTPUT_ORDINAL_META_KEY.to_string(),
            output_ordinal.to_string(),
        ),
        (
            INPUTS_META_KEY.to_string(),
            serde_json::to_string(inputs).unwrap_or_else(|_| "[]".to_string()),
        ),
    ])
}

#[derive(Debug, Serialize, Deserialize)]
struct FunctionBindingEnvelope {
    version: u32,
    bindings: Vec<Value>,
}

/// Encode immutable Function bindings for schema-level persistence.
pub fn function_bindings_metadata(bindings: &[FunctionBinding]) -> Result<String> {
    let bindings = bindings
        .iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::InvalidInput {
            message: format!("invalid Function binding metadata: {e}"),
        })?;
    serde_json::to_string(&FunctionBindingEnvelope {
        version: FUNCTION_BINDINGS_VERSION,
        bindings,
    })
    .map_err(|e| Error::InvalidInput {
        message: format!("invalid Function binding metadata: {e}"),
    })
}

/// Decode known Function bindings without rewriting their raw schema
/// metadata. Unknown envelope versions fail closed.
pub fn function_bindings(schema: &ArrowSchema) -> Result<Vec<FunctionBinding>> {
    let Some(envelope) = function_binding_envelope(schema)? else {
        return Ok(Vec::new());
    };
    envelope
        .bindings
        .into_iter()
        .map(|binding| {
            serde_json::from_value(binding).map_err(|e| Error::InvalidInput {
                message: format!("invalid Function binding metadata: {e}"),
            })
        })
        .collect()
}

fn function_binding_envelope(schema: &ArrowSchema) -> Result<Option<FunctionBindingEnvelope>> {
    let Some(raw) = schema.metadata().get(FUNCTION_BINDINGS_META_KEY) else {
        return Ok(None);
    };
    let envelope: FunctionBindingEnvelope =
        serde_json::from_str(raw).map_err(|e| Error::InvalidInput {
            message: format!("invalid Function binding metadata: {e}"),
        })?;
    if envelope.version != FUNCTION_BINDINGS_VERSION {
        return Err(Error::NotSupported {
            message: format!(
                "Function binding metadata version {} is not supported by this client",
                envelope.version
            ),
        });
    }
    Ok(Some(envelope))
}

/// Validate metadata before a schema mutation. Read-only access remains
/// possible for older datasets, while incomplete or newer contracts cannot be
/// silently rewritten by this client.
pub(crate) fn ensure_supported_function_metadata(schema: &ArrowSchema) -> Result<()> {
    let raw_bindings = function_binding_envelope(schema)?
        .map(|envelope| envelope.bindings)
        .unwrap_or_default();
    for value in &raw_bindings {
        ensure_known_binding_shape(value)?;
    }
    let bindings = raw_bindings
        .into_iter()
        .map(|binding| {
            serde_json::from_value(binding).map_err(|e| Error::InvalidInput {
                message: format!("invalid Function binding metadata: {e}"),
            })
        })
        .collect::<Result<Vec<FunctionBinding>>>()?;
    let mut binding_ids = BTreeSet::new();
    for binding in &bindings {
        if !binding_ids.insert(binding.binding_id().to_string()) {
            return Err(Error::InvalidInput {
                message: format!("duplicate Function binding '{}'", binding.binding_id()),
            });
        }
        if binding.outputs().is_empty() {
            return Err(Error::InvalidInput {
                message: format!("Function binding '{}' has no outputs", binding.binding_id()),
            });
        }
        if binding.function().name.is_empty() || binding.function().version.is_empty() {
            return Err(Error::InvalidInput {
                message: format!(
                    "Function binding '{}' has no exact version",
                    binding.binding_id()
                ),
            });
        }
        if binding.input_schema().is_none() || binding.output_schema().is_none() {
            return Err(Error::NotSupported {
                message: format!(
                    "Function binding '{}' does not contain exact Arrow schemas",
                    binding.binding_id()
                ),
            });
        }
        for (ordinal, output) in binding.outputs().iter().enumerate() {
            if output.output_ordinal != ordinal as u32 {
                return Err(Error::InvalidInput {
                    message: format!(
                        "Function binding '{}' has non-canonical output ordinals",
                        binding.binding_id()
                    ),
                });
            }
        }
        ensure_binding_matches_schema(schema, binding)?;
    }

    let bindings_by_id = bindings
        .iter()
        .map(|binding| (binding.binding_id(), binding))
        .collect::<HashMap<_, _>>();
    for field in schema.fields() {
        if field
            .metadata()
            .get(COMPUTED_COLUMN_META_KEY)
            .map(String::as_str)
            != Some("true")
        {
            continue;
        }
        match computed_column_from_field(field) {
            Some(ComputedColumn {
                kind:
                    ComputedColumnKind::Function {
                        binding_id,
                        output_ordinal,
                    },
                ..
            }) => {
                let binding =
                    bindings_by_id
                        .get(binding_id.as_str())
                        .ok_or_else(|| Error::InvalidInput {
                            message: format!(
                                "Function output '{}' references missing binding '{}'",
                                field.name(),
                                binding_id
                            ),
                        })?;
                let output = binding
                    .outputs()
                    .get(output_ordinal as usize)
                    .ok_or_else(|| Error::InvalidInput {
                        message: format!(
                            "Function output '{}' has invalid ordinal {}",
                            field.name(),
                            output_ordinal
                        ),
                    })?;
                if output.output_name != field.name().as_str() {
                    return Err(Error::InvalidInput {
                        message: format!(
                            "Function output '{}' does not match binding destination '{}'",
                            field.name(),
                            output.output_name
                        ),
                    });
                }
            }
            Some(ComputedColumn {
                kind: ComputedColumnKind::Sql { .. },
                ..
            }) => {}
            Some(ComputedColumn {
                kind: ComputedColumnKind::Unrecognized { kind },
                ..
            }) => {
                return Err(Error::NotSupported {
                    message: format!(
                        "computed column '{}' uses unsupported kind '{}'",
                        field.name(),
                        kind
                    ),
                });
            }
            None => {
                return Err(Error::InvalidInput {
                    message: format!(
                        "computed column '{}' has incomplete declaration metadata",
                        field.name()
                    ),
                });
            }
        }
    }
    Ok(())
}

pub(crate) fn ensure_no_function_bindings_for_mutation(
    schema: &ArrowSchema,
    operation: &str,
) -> Result<()> {
    ensure_supported_function_metadata(schema)?;
    if !function_bindings(schema)?.is_empty() {
        return Err(Error::NotSupported {
            message: format!(
                "{operation} is not supported on a table with registered Function bindings"
            ),
        });
    }
    Ok(())
}

/// Read a field's computed-column declaration, if it carries one.
///
/// A field flagged computed but carrying no kind, or a SQL one missing its
/// expression, is not a computed column here: without the rule there is
/// nothing to refresh from, so it is reported as absent rather than as a
/// half-formed declaration. An unrecognized kind is different -- the rule is
/// there and intact, this version just cannot act on it -- and comes back as
/// [`ComputedColumnKind::Unrecognized`].
pub fn computed_column_from_field(field: &ArrowField) -> Option<ComputedColumn> {
    let metadata = field.metadata();
    if metadata.get(COMPUTED_COLUMN_META_KEY).map(String::as_str) != Some("true") {
        return None;
    }
    let kind = match metadata.get(KIND_META_KEY)?.as_str() {
        SQL_KIND => ComputedColumnKind::Sql {
            expression: metadata.get(EXPRESSION_META_KEY)?.clone(),
        },
        FUNCTION_KIND => match (
            metadata.get(FUNCTION_BINDING_ID_META_KEY),
            metadata
                .get(FUNCTION_OUTPUT_ORDINAL_META_KEY)
                .and_then(|value| value.parse::<u32>().ok()),
        ) {
            (Some(binding_id), Some(output_ordinal)) if !binding_id.is_empty() => {
                ComputedColumnKind::Function {
                    binding_id: binding_id.clone(),
                    output_ordinal,
                }
            }
            _ => ComputedColumnKind::Unrecognized {
                kind: FUNCTION_KIND.to_string(),
            },
        },
        other => ComputedColumnKind::Unrecognized {
            kind: other.to_string(),
        },
    };
    let inputs = metadata
        .get(INPUTS_META_KEY)
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .unwrap_or_default();
    Some(ComputedColumn {
        name: field.name().clone(),
        kind,
        inputs,
    })
}

/// Read every computed-column declaration carried by `schema`, in field order.
///
/// Introspection is a pure read of the schema the caller already holds, the
/// way a SQL catalog reports a generation expression as another column of
/// `information_schema.columns`.
pub fn computed_columns(schema: &ArrowSchema) -> Vec<ComputedColumn> {
    schema
        .fields()
        .iter()
        .filter_map(|field| computed_column_from_field(field))
        .collect()
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FunctionOutputTarget {
    pub result_field: String,
    pub output_name: String,
    pub output_ordinal: u32,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FunctionInputTarget {
    pub parameter: String,
    pub field_path: String,
    pub arrow_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FunctionDeclarationPlan {
    pub application: FunctionApplication,
    pub binding_metadata_version: u32,
    pub input_bindings: Vec<FunctionInputTarget>,
    pub input_schema: JsonArrowSchema,
    pub output_schema: JsonArrowSchema,
    pub outputs: Vec<FunctionOutputTarget>,
}

fn invalid_function(message: impl Into<String>) -> Error {
    Error::InvalidInput {
        message: message.into(),
    }
}

fn reject_unknown_object_fields(value: &Value, allowed: &[&str], context: &str) -> Result<()> {
    let object = value.as_object().ok_or_else(|| {
        invalid_function(format!(
            "invalid Function binding metadata: {context} must be an object"
        ))
    })?;
    let unknown = object
        .keys()
        .filter(|key| !allowed.contains(&key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if unknown.is_empty() {
        Ok(())
    } else {
        Err(Error::NotSupported {
            message: format!(
                "Function binding metadata contains newer {context} fields: {unknown:?}"
            ),
        })
    }
}

fn ensure_known_binding_shape(value: &Value) -> Result<()> {
    reject_unknown_object_fields(
        value,
        &[
            "binding_id",
            "function",
            "inputs",
            "outputs",
            "input_schema",
            "output_schema",
        ],
        "binding",
    )?;
    let object = value.as_object().unwrap();
    reject_unknown_object_fields(
        object
            .get("function")
            .ok_or_else(|| invalid_function("Function binding is missing its exact version"))?,
        &["name", "version"],
        "version reference",
    )?;
    for input in object
        .get("inputs")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid_function("Function binding inputs must be an array"))?
    {
        reject_unknown_object_fields(
            input,
            &[
                "parameter",
                "field_id",
                "field_path",
                "arrow_type",
                "nullable",
            ],
            "input binding",
        )?;
    }
    for output in object
        .get("outputs")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid_function("Function binding outputs must be an array"))?
    {
        reject_unknown_object_fields(
            output,
            &[
                "result_field",
                "output_name",
                "output_field_id",
                "output_ordinal",
                "arrow_type",
                "nullable",
            ],
            "output mapping",
        )?;
    }
    Ok(())
}

fn resolve_field_path<'a>(schema: &'a ArrowSchema, path: &str) -> Result<&'a ArrowField> {
    let parts = lance_core::datatypes::parse_field_path(path).map_err(|e| {
        invalid_function(format!("invalid Function input field path '{path}': {e}"))
    })?;
    let Some((root, children)) = parts.split_first() else {
        return Err(invalid_function(
            "Function input field path cannot be empty",
        ));
    };
    let mut field = schema
        .field_with_name(root)
        .map_err(|_| invalid_function(format!("unknown Function input column '{path}'")))?;
    for child in children {
        let DataType::Struct(fields) = field.data_type() else {
            return Err(invalid_function(format!(
                "Function input field path '{path}' traverses a non-struct field"
            )));
        };
        field = fields
            .iter()
            .find(|field| field.name() == child)
            .map(AsRef::as_ref)
            .ok_or_else(|| invalid_function(format!("unknown Function input column '{path}'")))?;
    }
    Ok(field)
}

fn canonical_input_arrow_type(field: &JsonArrowField) -> Result<String> {
    if field.r#type.fields.is_none() && field.r#type.length.is_none() {
        Ok(field.r#type.r#type.clone())
    } else {
        serde_json::to_string(field.r#type.as_ref()).map_err(|e| {
            invalid_function(format!("could not encode exact Function input type: {e}"))
        })
    }
}

/// `fixed_size_list<item, size>` -> (`item`, `size`); the comma must sit outside
/// any nested `<...>`.
fn split_fixed_size_list(raw: &str) -> Option<(&str, i32)> {
    let inner = raw.strip_prefix("fixed_size_list<")?.strip_suffix('>')?;
    let mut depth = 0_u32;
    let mut separator = None;
    for (index, byte) in inner.bytes().enumerate() {
        match byte {
            b'<' => depth += 1,
            b'>' => depth = depth.checked_sub(1)?,
            b',' if depth == 0 => separator = Some(index),
            _ => {}
        }
    }
    let (item, size) = inner.split_at(separator?);
    let size: i32 = size[1..].trim().parse().ok()?;
    (size > 0).then_some((item.trim(), size))
}

fn parse_output_arrow_type(raw: &str) -> Result<JsonArrowDataType> {
    fn parse(raw: &str) -> Result<JsonArrowDataType> {
        let raw = raw.trim();
        if raw.starts_with('{') {
            return serde_json::from_str(raw).map_err(|e| {
                invalid_function(format!("invalid Function Arrow type '{raw}': {e}"))
            });
        }
        if let Some(inner) = raw
            .strip_prefix("list<")
            .and_then(|value| value.strip_suffix('>'))
        {
            let mut data_type = JsonArrowDataType::new("list".to_string());
            data_type.fields = Some(vec![JsonArrowField::new(
                "item".to_string(),
                false,
                parse(inner)?,
            )]);
            return Ok(data_type);
        }
        if let Some(inner) = raw
            .strip_prefix("large_list<")
            .and_then(|value| value.strip_suffix('>'))
        {
            let mut data_type = JsonArrowDataType::new("large_list".to_string());
            data_type.fields = Some(vec![JsonArrowField::new(
                "item".to_string(),
                false,
                parse(inner)?,
            )]);
            return Ok(data_type);
        }
        if let Some((inner, size)) = split_fixed_size_list(raw) {
            let mut data_type = JsonArrowDataType::new("fixed_size_list".to_string());
            data_type.fields = Some(vec![JsonArrowField::new(
                "item".to_string(),
                false,
                parse(inner)?,
            )]);
            data_type.length = Some(i64::from(size));
            return Ok(data_type);
        }
        let normalized = match raw {
            "boolean" => "bool",
            "string" => "utf8",
            "large_string" => "large_utf8",
            "halffloat" => "float16",
            "float" => "float32",
            "double" => "float64",
            other => other,
        };
        Ok(JsonArrowDataType::new(normalized.to_string()))
    }

    let data_type = parse(raw)?;
    lance_namespace::schema::convert_json_arrow_type(&data_type)
        .map_err(|e| invalid_function(format!("unsupported Function Arrow type '{raw}': {e}")))?;
    Ok(data_type)
}

fn ensure_binding_matches_schema(schema: &ArrowSchema, binding: &FunctionBinding) -> Result<()> {
    let mut input_fields = Vec::with_capacity(binding.inputs().len());
    for input in binding.inputs() {
        let field = resolve_field_path(schema, &input.field_path)?;
        if field
            .metadata()
            .get(COMPUTED_COLUMN_META_KEY)
            .map(String::as_str)
            == Some("true")
        {
            return Err(invalid_function(format!(
                "Function input '{}' is computed",
                input.field_path
            )));
        }
        // A non-null source is within a nullable parameter's domain. The
        // reverse can pass nulls to a Function that does not accept them.
        if field.is_nullable() && !input.nullable {
            return Err(invalid_function(format!(
                "Function input column '{}' is nullable, but parameter '{}' in binding '{}' is non-nullable",
                input.field_path,
                input.parameter,
                binding.binding_id()
            )));
        }
        let parameter_field = ArrowField::new(
            input.parameter.clone(),
            field.data_type().clone(),
            input.nullable,
        )
        .with_metadata(field.metadata().clone());
        let json = lance_namespace::schema::arrow_schema_to_json(&ArrowSchema::new(vec![
            parameter_field.clone(),
        ]))
        .map_err(|e| invalid_function(format!("invalid Function input schema: {e}")))?;
        let json_field = json.fields.into_iter().next().unwrap();
        if canonical_input_arrow_type(&json_field)? != input.arrow_type {
            return Err(invalid_function(format!(
                "Function input '{}' type no longer matches binding '{}'",
                input.field_path,
                binding.binding_id()
            )));
        }
        input_fields.push(parameter_field);
    }
    let input_schema =
        lance_namespace::schema::arrow_schema_to_json(&ArrowSchema::new(input_fields))
            .map_err(|e| invalid_function(format!("invalid Function input schema: {e}")))?;
    let input_schema = serde_json::to_value(input_schema).map_err(|e| {
        invalid_function(format!("could not encode exact Function input schema: {e}"))
    })?;
    if binding.input_schema() != Some(&input_schema) {
        return Err(invalid_function(format!(
            "Function binding '{}' input schema does not match its inputs",
            binding.binding_id()
        )));
    }

    let mut output_fields = Vec::with_capacity(binding.outputs().len());
    for output in binding.outputs() {
        let field = schema.field_with_name(&output.output_name).map_err(|_| {
            invalid_function(format!(
                "Function binding '{}' output '{}' is missing",
                binding.binding_id(),
                output.output_name
            ))
        })?;
        if field.name() != &output.output_name || !field.is_nullable() || output.nullable {
            return Err(invalid_function(format!(
                "Function output '{}' no longer matches binding '{}'",
                output.output_name,
                binding.binding_id()
            )));
        }
        let expected_type = parse_output_arrow_type(&output.arrow_type)?;
        let expected_type = lance_namespace::schema::convert_json_arrow_type(&expected_type)
            .map_err(|e| invalid_function(format!("invalid Function output type: {e}")))?;
        if field.data_type() != &expected_type {
            return Err(invalid_function(format!(
                "Function output '{}' type no longer matches binding '{}'",
                output.output_name,
                binding.binding_id()
            )));
        }
        output_fields.push(ArrowField::new(
            field.name().clone(),
            field.data_type().clone(),
            true,
        ));
    }
    let output_schema =
        lance_namespace::schema::arrow_schema_to_json(&ArrowSchema::new(output_fields))
            .map_err(|e| invalid_function(format!("invalid Function output schema: {e}")))?;
    let output_schema = serde_json::to_value(output_schema).map_err(|e| {
        invalid_function(format!(
            "could not encode exact Function output schema: {e}"
        ))
    })?;
    if binding.output_schema() != Some(&output_schema) {
        return Err(invalid_function(format!(
            "Function binding '{}' output schema does not match physical siblings",
            binding.binding_id()
        )));
    }
    Ok(())
}

/// Resolve a Function application against a table schema before any request is
/// serialized. Input paths and the complete sibling output schema are fixed in
/// one plan.
pub(crate) fn plan_function_application(
    schema: &ArrowSchema,
    application: &FunctionApplication,
    output_name: Option<&str>,
) -> Result<FunctionDeclarationPlan> {
    ensure_no_function_bindings_for_mutation(schema, "Function binding declaration")?;
    if application.has_unknown_fields() {
        return Err(Error::NotSupported {
            message: "Function application contains fields from a newer contract".into(),
        });
    }
    if application.function().name.is_empty() || application.function().version.is_empty() {
        return Err(invalid_function(
            "Function application requires an exact version",
        ));
    }

    let mut parameters = BTreeSet::new();
    let mut input_bindings = Vec::with_capacity(application.inputs().len());
    let mut input_fields = Vec::with_capacity(application.inputs().len());
    for input in application.inputs() {
        if !parameters.insert(input.parameter.as_str()) {
            return Err(invalid_function(format!(
                "duplicate Function parameter '{}'",
                input.parameter
            )));
        }
        if input.kind != "column" {
            return Err(Error::NotSupported {
                message: format!(
                    "Function input kind '{}' is not supported for column declaration",
                    input.kind
                ),
            });
        }
        let source = input.value.as_object().ok_or_else(|| {
            invalid_function(format!(
                "Function parameter '{}' has an invalid column source",
                input.parameter
            ))
        })?;
        if source.len() != 1 {
            return Err(Error::NotSupported {
                message: format!(
                    "Function parameter '{}' uses a newer column source contract",
                    input.parameter
                ),
            });
        }
        let path = source.get("path").and_then(Value::as_str).ok_or_else(|| {
            invalid_function(format!(
                "Function parameter '{}' requires a column path",
                input.parameter
            ))
        })?;
        let field = resolve_field_path(schema, path)?;
        if field
            .metadata()
            .get(COMPUTED_COLUMN_META_KEY)
            .map(String::as_str)
            == Some("true")
        {
            return Err(invalid_function(format!(
                "Function input '{path}' is computed; computed-on-computed bindings are not supported"
            )));
        }
        let parameter_field = ArrowField::new(
            input.parameter.clone(),
            field.data_type().clone(),
            field.is_nullable(),
        )
        .with_metadata(field.metadata().clone());
        let input_schema = lance_namespace::schema::arrow_schema_to_json(&ArrowSchema::new(vec![
            parameter_field.clone(),
        ]))
        .map_err(|e| invalid_function(format!("invalid Function input schema: {e}")))?;
        let json_field = input_schema.fields.into_iter().next().unwrap();
        input_bindings.push(FunctionInputTarget {
            parameter: input.parameter.clone(),
            field_path: path.to_string(),
            arrow_type: canonical_input_arrow_type(&json_field)?,
            nullable: field.is_nullable(),
        });
        input_fields.push(parameter_field);
    }
    let input_schema =
        lance_namespace::schema::arrow_schema_to_json(&ArrowSchema::new(input_fields))
            .map_err(|e| invalid_function(format!("invalid Function input schema: {e}")))?;

    let output = application.output();
    let mut outputs = Vec::new();
    let mut output_fields = Vec::new();
    match output.kind.as_str() {
        "scalar" => {
            if !application.columns().is_empty() {
                return Err(invalid_function(
                    "scalar Function applications cannot rename result fields",
                ));
            }
            let name = output_name.ok_or_else(|| {
                invalid_function(
                    "a scalar Function application must be mapped to one output column",
                )
            })?;
            if output.nullable != Some(false) {
                return Err(invalid_function(
                    "Function logical outputs must be non-nullable during NULL assignment",
                ));
            }
            let data_type =
                parse_output_arrow_type(output.arrow_type.as_deref().ok_or_else(|| {
                    invalid_function("scalar Function output is missing its Arrow type")
                })?)?;
            outputs.push(FunctionOutputTarget {
                result_field: WHOLE_RESULT_FIELD.to_string(),
                output_name: name.to_string(),
                output_ordinal: 0,
            });
            output_fields.push(JsonArrowField::new(name.to_string(), true, data_type));
        }
        "named_struct" => {
            if output.fields.is_empty() {
                return Err(invalid_function(
                    "named-struct Function output requires at least one field",
                ));
            }
            let result_names = output
                .fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<BTreeSet<_>>();
            if result_names.len() != output.fields.len() {
                return Err(invalid_function(
                    "named-struct Function result field names must be unique",
                ));
            }
            if output.fields.iter().any(|field| field.nullable) {
                return Err(invalid_function(
                    "Function logical outputs must be non-nullable during NULL assignment",
                ));
            }
            let unknown = application
                .columns()
                .keys()
                .filter(|name| !result_names.contains(name.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            if !unknown.is_empty() {
                return Err(invalid_function(format!(
                    "unknown Function result fields: {unknown:?}"
                )));
            }

            if let Some(name) = output_name {
                if !application.columns().is_empty() {
                    return Err(invalid_function(
                        "a named-struct mapped to one column cannot also rename expanded fields",
                    ));
                }
                let fields = output
                    .fields
                    .iter()
                    .map(|field| {
                        Ok(JsonArrowField::new(
                            field.name.clone(),
                            false,
                            parse_output_arrow_type(&field.arrow_type)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let mut data_type = JsonArrowDataType::new("struct".to_string());
                data_type.fields = Some(fields);
                outputs.push(FunctionOutputTarget {
                    result_field: WHOLE_RESULT_FIELD.to_string(),
                    output_name: name.to_string(),
                    output_ordinal: 0,
                });
                output_fields.push(JsonArrowField::new(name.to_string(), true, data_type));
            } else {
                let mut destinations = BTreeSet::new();
                for (ordinal, field) in output.fields.iter().enumerate() {
                    let name = application
                        .columns()
                        .get(&field.name)
                        .unwrap_or(&field.name);
                    if !destinations.insert(name.as_str()) {
                        return Err(invalid_function(
                            "Function output destinations must be unique",
                        ));
                    }
                    outputs.push(FunctionOutputTarget {
                        result_field: field.name.clone(),
                        output_name: name.clone(),
                        output_ordinal: ordinal as u32,
                    });
                    output_fields.push(JsonArrowField::new(
                        name.clone(),
                        true,
                        parse_output_arrow_type(&field.arrow_type)?,
                    ));
                }
            }
        }
        kind => {
            return Err(Error::NotSupported {
                message: format!(
                    "Function output kind '{kind}' is not supported for column declaration"
                ),
            });
        }
    }

    for output in &outputs {
        if output.output_name.is_empty() {
            return Err(invalid_function(
                "Function output column name cannot be empty",
            ));
        }
        if schema.field_with_name(&output.output_name).is_ok() {
            return Err(Error::ColumnAlreadyExists {
                name: output.output_name.clone(),
            });
        }
    }

    Ok(FunctionDeclarationPlan {
        application: application.clone(),
        binding_metadata_version: FUNCTION_BINDINGS_VERSION,
        input_bindings,
        input_schema,
        output_schema: JsonArrowSchema::new(output_fields),
        outputs,
    })
}

/// Reject a schema change to a column some declaration reads.
///
/// A binding is SQL text naming its inputs, so renaming, retyping or dropping
/// one leaves an expression that no longer resolves. Refusing the change keeps
/// a declaration that survived [`plan`] evaluable for as long as it exists.
///
/// Paths are compared at their root: a declaration reading `metadata` is
/// invalidated by a change to `metadata.age` just as surely.
pub(crate) fn ensure_not_an_input(schema: &SchemaRef, paths: &[&str]) -> Result<()> {
    for declaration in computed_columns(schema) {
        // The expression, not stored inputs, is the source of truth; an
        // expression that no longer parses proves nothing, so refuse.
        let inputs = match &declaration.kind {
            ComputedColumnKind::Sql { expression } => Planner::new(schema.clone())
                .parse_expr(expression)
                .map(|parsed| Planner::column_names_in_expr(&parsed))
                .map_err(|e| Error::InvalidInput {
                    message: format!(
                        "computed column '{}' has an unevaluable expression ({e}); drop it \
                         before changing the schema",
                        declaration.name
                    ),
                })?,
            _ => declaration.inputs.clone(),
        };
        for path in paths {
            // Exact target only: the binding travels with the whole column,
            // not with a nested field the expression still shapes.
            if declaration.name == *path {
                continue;
            }
            if declaration.name == root(path) {
                return Err(Error::InvalidInput {
                    message: format!(
                        "'{}' is part of computed column '{}'; drop the column and declare \
                         it again",
                        path, declaration.name
                    ),
                });
            }
            if inputs.iter().any(|input| root(input) == root(path)) {
                return Err(Error::InvalidInput {
                    message: format!(
                        "column '{}' is read by computed column '{}'; drop that column first",
                        path, declaration.name
                    ),
                });
            }
        }
    }
    Ok(())
}

/// Reject a write that supplies values for a computed column directly:
/// only refresh materializes one, and refresh never revisits a filled row.
pub(crate) fn ensure_not_written<'a>(
    schema: &ArrowSchema,
    written: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    let declared: Vec<String> = computed_columns(schema)
        .into_iter()
        .map(|declaration| declaration.name)
        .collect();
    for name in written {
        if declared.iter().any(|declared| declared == root(name)) {
            return Err(Error::InvalidInput {
                message: format!(
                    "column '{}' is computed; its values come from refresh and cannot be \
                     written directly",
                    root(name)
                ),
            });
        }
    }
    Ok(())
}

/// Reject a batch holding values for a computed column. Null slots are the
/// declared state, so planner-padded placeholders pass.
pub(crate) fn ensure_batch_writes_no_computed_values(
    declared: &[String],
    batch: &arrow_array::RecordBatch,
) -> Result<()> {
    for name in declared {
        if let Some(column) = batch.column_by_name(name)
            && column.null_count() != column.len()
        {
            return Err(Error::InvalidInput {
                message: format!(
                    "column '{name}' is computed; its values come from refresh and cannot \
                     be written directly"
                ),
            });
        }
    }
    Ok(())
}

/// Reject fields carrying declaration metadata that did not come through
/// [`plan`]. One authority for creation, overwrite and raw transforms.
pub(crate) fn ensure_no_foreign_declarations<'a>(
    fields: impl IntoIterator<Item = &'a Arc<ArrowField>>,
) -> Result<()> {
    for field in fields {
        if field.metadata().keys().any(|k| is_declaration_key(k)) {
            return Err(Error::InvalidInput {
                message: format!(
                    "field '{}' carries computed-column metadata; declare computed columns \
                     with add_columns().computed()",
                    field.name()
                ),
            });
        }
    }
    Ok(())
}

/// True for field-metadata keys that belong to a computed-column declaration.
///
/// A declaration is immutable through metadata edits: it is validated as a
/// whole at declare time, and rewriting any piece of it -- the flag, the
/// kind, the expression, the inputs -- would bypass that validation or move
/// a binding out from under a refresh. Drop the column and declare it again.
pub(crate) fn is_declaration_key(key: &str) -> bool {
    key == COMPUTED_COLUMN_META_KEY || key.starts_with("computed_column.")
}

/// Reject retyping a computed column itself.
///
/// A cast keeps the stored expression while changing the type it must yield
/// -- and lance's cast rewrites the field without its metadata, so the
/// declaration silently stops being one. Dropping and redeclaring is the
/// coherent way to change a computed column's type.
pub(crate) fn ensure_not_retyped(schema: &ArrowSchema, paths: &[&str]) -> Result<()> {
    for declaration in computed_columns(schema) {
        for path in paths {
            if declaration.name == root(path) {
                return Err(Error::InvalidInput {
                    message: format!(
                        "column '{}' is computed; drop it and declare it again to change \
                         its type",
                        declaration.name
                    ),
                });
            }
        }
    }
    Ok(())
}

/// The top-level column a possibly nested input path reads.
pub(crate) fn root(path: &str) -> &str {
    path.split('.').next().unwrap_or(path)
}

/// A declaration's expression bound to a schema, ready to evaluate.
pub(crate) struct BoundExpression {
    /// The columns the expression names, as written; nested inputs keep
    /// their dotted path.
    pub inputs: Vec<String>,
    /// The top-level columns evaluation reads, in [`Self::read_schema`]
    /// order. A nested input appears through its root.
    pub roots: Vec<String>,
    /// The projected schema evaluation runs against.
    pub read_schema: SchemaRef,
    /// The compiled expression.
    pub physical: Arc<dyn PhysicalExpr>,
    /// The type the expression yields.
    pub data_type: DataType,
}

/// Parse, resolve and compile `expression` against `schema`.
///
/// Inputs come from the expression as written, before optimization: the
/// simplifier can fold a referenced column out entirely (`true OR x > 0`),
/// and the guard protecting the stored SQL has to see every column the text
/// names, not just the ones the simplified form still reads.
pub(crate) fn bind(schema: SchemaRef, column: &str, expression: &str) -> Result<BoundExpression> {
    let invalid = |message: String| Error::InvalidExpression {
        column: column.to_string(),
        message,
    };

    let planner = Planner::new(schema.clone());
    let parsed = planner
        .parse_expr(expression)
        .map_err(|e| invalid(e.to_string()))?;

    // A declaration is evaluated more than once -- staging and writing are
    // separate passes, and a refresh years later replays the same text -- so
    // a function that can answer differently each time has no coherent value
    // to declare.
    let mut volatile = None;
    parsed
        .apply(|expr| {
            use datafusion_common::tree_node::TreeNodeRecursion;
            if let datafusion_expr::Expr::ScalarFunction(function) = expr
                && function.func.signature().volatility != datafusion_expr::Volatility::Immutable
            {
                volatile = Some(function.func.name().to_string());
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .map_err(|e| invalid(e.to_string()))?;
    if let Some(function) = volatile {
        return Err(invalid(format!(
            "'{function}' is not deterministic; a computed column's expression must \
             yield the same value every time it is evaluated"
        )));
    }

    let mut inputs = Planner::column_names_in_expr(&parsed);
    inputs.sort();
    inputs.dedup();

    // A nested input is recorded by its path but read through its root
    // column; Schema::index_of resolves top-level names only. Resolved here
    // rather than left to the planner so an unknown column names itself in
    // the error instead of surfacing as a plan failure.
    let mut indices = Vec::with_capacity(inputs.len());
    for input in &inputs {
        let index = schema
            .index_of(root(input))
            .map_err(|_| invalid(format!("unknown column '{input}'")))?;
        if !indices.contains(&index) {
            indices.push(index);
        }
    }
    indices.sort_unstable();

    // Physical expressions address columns by position, so the planner that
    // compiles the expression has to be built on the projected schema
    // evaluation will actually read.
    let read_schema = Arc::new(
        schema
            .project(&indices)
            .map_err(|e| invalid(e.to_string()))?,
    );
    let roots = read_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();

    let optimized = planner
        .optimize_expr(parsed)
        .map_err(|e| invalid(e.to_string()))?;
    let physical = Planner::new(read_schema.clone())
        .create_physical_expr(&optimized)
        .map_err(|e| invalid(e.to_string()))?;
    let data_type = physical
        .data_type(read_schema.as_ref())
        .map_err(|e| invalid(e.to_string()))?;

    Ok(BoundExpression {
        inputs,
        roots,
        read_schema,
        physical,
        data_type,
    })
}

/// Resolve `(name, expression)` pairs against `schema` into fields carrying
/// their bindings.
///
/// Everything that can be known statically is checked here rather than at
/// refresh time: that the expression parses, that every column it reads
/// exists, and that the target name is free. A declaration that survives this
/// is one a refresh can always act on.
pub(crate) fn plan(schema: SchemaRef, columns: &[(String, String)]) -> Result<Vec<ArrowField>> {
    if columns.is_empty() {
        return Err(Error::InvalidInput {
            message: "at least one computed column is required".into(),
        });
    }

    let mut fields = Vec::with_capacity(columns.len());
    let mut declared: Vec<&str> = Vec::with_capacity(columns.len());

    for (name, expression) in columns {
        if schema.field_with_name(name).is_ok() || declared.contains(&name.as_str()) {
            return Err(Error::ColumnAlreadyExists { name: name.clone() });
        }

        let bound = bind(schema.clone(), name, expression)?;

        // Declared columns start entirely null, so nullability is a property
        // of the declaration rather than of what the expression yields.
        fields.push(
            ArrowField::new(name, bound.data_type, true)
                .with_metadata(computed_column_metadata(expression, &bound.inputs)),
        );
        declared.push(name);
    }

    Ok(fields)
}

/// Build the transform that declares `columns` against `schema`.
///
/// An all-null column is how a binding with no values yet is carried into a
/// commit; that it is spelled `AllNulls` is a detail of the commit, not of the
/// column, which is why this is internal and
/// [`AddColumnsBuilder::computed`](super::AddColumnsBuilder::computed) is the
/// public way in.
pub(crate) fn declare(
    schema: SchemaRef,
    columns: &[(String, String)],
) -> Result<NewColumnTransform> {
    let fields = plan(schema, columns)?;
    Ok(NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(
        fields,
    ))))
}

/// Commit a declaration of a kind this version does not produce, the way a
/// newer lancedb would leave one behind. Bypasses admission, which exists to
/// stop exactly this through the public API.
#[cfg(test)]
pub(super) async fn add_foreign_kind(table: &crate::Table, name: &str, kind: &str) {
    let field = ArrowField::new(name, DataType::Int32, true).with_metadata(HashMap::from([
        (COMPUTED_COLUMN_META_KEY.to_string(), "true".to_string()),
        (KIND_META_KEY.to_string(), kind.to_string()),
        (INPUTS_META_KEY.to_string(), r#"["x"]"#.to_string()),
    ]));
    super::schema_evolution::commit_add_columns(
        table.as_native().unwrap(),
        NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(vec![field]))),
        None,
    )
    .await
    .unwrap();
}

#[cfg(test)]
mod tests {
    #[test]
    fn output_arrow_type_grammar_matches_the_shared_golden() {
        let golden: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/first_class_functions/v1/arrow_types.json"
        ))
        .unwrap();
        let valid = golden["valid"].as_array().unwrap().iter();
        for case in valid.chain(golden["server_only"].as_array().unwrap()) {
            let raw = case["arrow_type"].as_str().unwrap();
            let parsed = super::parse_output_arrow_type(raw)
                .unwrap_or_else(|error| panic!("{raw}: {error}"));
            assert_eq!(
                serde_json::to_value(&parsed).unwrap(),
                case["json"],
                "{raw}"
            );
        }
        for raw in golden["invalid"].as_array().unwrap() {
            let raw = raw.as_str().unwrap();
            assert!(
                super::parse_output_arrow_type(raw).is_err(),
                "{raw:?} should be rejected"
            );
        }
    }

    use arrow_array::record_batch;
    use arrow_schema::DataType;
    use futures::TryStreamExt;
    use lance::dataset::ColumnAlteration;

    use super::*;
    use crate::connect;
    use crate::query::{ExecutableQuery, QueryBase, Select};
    use crate::{Error, Table};

    async fn table_with_ints(name: &str) -> Table {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("x", Int32, [1, 2, 3])).unwrap();
        conn.create_table(name, batch).execute().await.unwrap()
    }

    /// Declare `columns` the way a caller would: plan the expressions, then
    /// add them through the ordinary column API.
    async fn add_computed(table: &Table, columns: &[(String, String)]) -> Result<u64> {
        let mut builder = table.add_columns();
        for (name, expression) in columns {
            builder = builder.computed(name, expression);
        }
        Ok(builder.execute().await?.version)
    }

    async fn declared(table: &Table) -> Vec<ComputedColumn> {
        computed_columns(table.schema().await.unwrap().as_ref())
    }

    #[tokio::test]
    async fn test_declare_infers_type_and_inputs() {
        let table = table_with_ints("declare_infers").await;
        let initial = table.version().await.unwrap();

        let version = add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();
        assert!(version > initial);

        let schema = table.schema().await.unwrap();
        let field = schema.field_with_name("doubled").unwrap();
        assert_eq!(field.data_type(), &DataType::Int32);
        assert!(field.is_nullable());

        assert_eq!(
            declared(&table).await,
            vec![ComputedColumn {
                name: "doubled".into(),
                kind: ComputedColumnKind::Sql {
                    expression: "x * 2".into()
                },
                inputs: vec!["x".into()],
            }]
        );
    }

    /// The binding reaches the schema only if `AllNulls` carries per-field
    /// metadata through the commit. The whole representation rests on it.
    #[tokio::test]
    async fn test_all_nulls_preserves_field_metadata() {
        let table = table_with_ints("metadata_survives").await;
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        let schema = table.schema().await.unwrap();
        let metadata = schema.field_with_name("doubled").unwrap().metadata();
        assert_eq!(
            metadata.get(COMPUTED_COLUMN_META_KEY).map(String::as_str),
            Some("true")
        );
        assert_eq!(metadata.get(KIND_META_KEY).map(String::as_str), Some("sql"));
        assert_eq!(
            metadata.get(EXPRESSION_META_KEY).map(String::as_str),
            Some("x * 2")
        );
        assert_eq!(
            metadata.get(INPUTS_META_KEY).map(String::as_str),
            Some(r#"["x"]"#)
        );
    }

    #[tokio::test]
    async fn test_declared_column_is_all_null() {
        let table = table_with_ints("declare_is_null").await;
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        let batches = table
            .query()
            .select(Select::columns(&["doubled"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
        for batch in &batches {
            assert_eq!(batch["doubled"].null_count(), batch.num_rows());
        }
    }

    #[tokio::test]
    async fn test_unknown_column_fails_at_declare_time() {
        let table = table_with_ints("unknown_input").await;
        let err = add_computed(&table, &[("bad".into(), "missing + 1".into())])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidExpression { column, .. } if column == "bad"));

        let schema = table.schema().await.unwrap();
        assert!(schema.field_with_name("bad").is_err());
    }

    #[tokio::test]
    async fn test_unparsable_expression_fails_at_declare_time() {
        let table = table_with_ints("bad_syntax").await;
        let err = add_computed(&table, &[("bad".into(), "x *".into())])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidExpression { column, .. } if column == "bad"));
        assert!(
            table
                .schema()
                .await
                .unwrap()
                .field_with_name("bad")
                .is_err()
        );
    }

    /// A user-defined function is an expression like any other; only its
    /// resolution is missing. When a registry-aware planner exists this
    /// becomes a supported declaration rather than a new API.
    #[tokio::test]
    async fn test_unregistered_function_is_rejected_for_now() {
        let table = table_with_ints("udf_not_yet").await;
        let err = add_computed(&table, &[("vec".into(), "embed(x)".into())])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidExpression { column, .. } if column == "vec"));
        assert!(
            table
                .schema()
                .await
                .unwrap()
                .field_with_name("vec")
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_existing_column_name_is_rejected() {
        let table = table_with_ints("name_taken").await;
        let err = add_computed(&table, &[("x".into(), "x * 2".into())])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ColumnAlreadyExists { name } if name == "x"));
        assert!(declared(&table).await.is_empty());
    }

    #[tokio::test]
    async fn test_constant_expression_needs_no_inputs() {
        let table = table_with_ints("constant").await;
        add_computed(&table, &[("answer".into(), "42".into())])
            .await
            .unwrap();

        let declared = declared(&table).await;
        assert_eq!(declared.len(), 1);
        assert!(declared[0].inputs.is_empty());
    }

    #[tokio::test]
    async fn test_multiple_columns_in_one_commit() {
        let table = table_with_ints("multi").await;
        let initial = table.version().await.unwrap();

        add_computed(
            &table,
            &[
                ("plus".into(), "x + 1".into()),
                ("squared".into(), "x * x".into()),
            ],
        )
        .await
        .unwrap();

        assert_eq!(table.version().await.unwrap(), initial + 1);
        let declared = declared(&table).await;
        assert_eq!(declared.len(), 2);
        assert_eq!(declared[0].name, "plus");
        assert_eq!(declared[1].name, "squared");
    }

    #[tokio::test]
    async fn test_duplicate_declaration_in_one_call_is_rejected() {
        let table = table_with_ints("dupe").await;
        let err = add_computed(
            &table,
            &[
                ("dup".into(), "x + 1".into()),
                ("dup".into(), "x + 2".into()),
            ],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::ColumnAlreadyExists { name } if name == "dup"));
        assert!(declared(&table).await.is_empty());
    }

    /// A column added by an ordinary transform is materialized, not bound, so
    /// it carries no declaration to report.
    #[tokio::test]
    async fn test_ordinary_columns_are_not_reported_as_computed() {
        let table = table_with_ints("plain").await;
        assert!(declared(&table).await.is_empty());

        table
            .add_columns()
            .transform(NewColumnTransform::SqlExpressions(vec![(
                "eager".into(),
                "x * 2".into(),
            )]))
            .execute()
            .await
            .unwrap();
        assert!(declared(&table).await.is_empty());
    }

    /// Built-in functions type the column the same way an operator does.
    #[tokio::test]
    async fn test_builtin_function_inference() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("name", Utf8, ["ada", "grace"]), ("n", Int32, [-1, 2])).unwrap();
        let table = conn
            .create_table("builtins", batch)
            .execute()
            .await
            .unwrap();

        add_computed(
            &table,
            &[
                ("shout".into(), "upper(name)".into()),
                ("width".into(), "length(name)".into()),
                ("magnitude".into(), "abs(n)".into()),
            ],
        )
        .await
        .unwrap();

        let schema = table.schema().await.unwrap();
        assert_eq!(
            schema.field_with_name("shout").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("magnitude").unwrap().data_type(),
            &DataType::Int32
        );
        // length() returns a width-dependent integer type; assert it is one
        // rather than pinning which.
        assert!(
            schema
                .field_with_name("width")
                .unwrap()
                .data_type()
                .is_integer()
        );

        let declared = declared(&table).await;
        assert_eq!(declared.len(), 3);
        assert_eq!(declared[0].inputs, vec!["name".to_string()]);
        assert_eq!(declared[2].inputs, vec!["n".to_string()]);
    }

    /// The reason the kind is tagged: a declaration written by a newer version
    /// has to read back as a computed column this one cannot evaluate, not as
    /// an ordinary column. Reported as absent it would be refreshable by
    /// nothing and redeclarable over, silently.
    #[tokio::test]
    async fn test_unrecognized_kind_is_reported_rather_than_hidden() {
        let table = table_with_ints("foreign_kind").await;
        super::add_foreign_kind(&table, "embedding", "udf").await;

        assert_eq!(
            declared(&table).await,
            vec![ComputedColumn {
                name: "embedding".into(),
                kind: ComputedColumnKind::Unrecognized { kind: "udf".into() },
                inputs: vec!["x".into()],
            }]
        );

        let err = add_computed(&table, &[("embedding".into(), "x * 2".into())])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotSupported { .. }));
    }

    /// A kind is what makes a declaration readable at all, so the flag alone
    /// is half-formed in the same way a missing expression is.
    #[test]
    fn test_flag_without_a_kind_is_not_a_declaration() {
        let field =
            ArrowField::new("half", DataType::Int32, true).with_metadata(HashMap::from([(
                COMPUTED_COLUMN_META_KEY.to_string(),
                "true".to_string(),
            )]));
        assert_eq!(computed_column_from_field(&field), None);
    }

    /// A SQL declaration is its expression; without one there is nothing to
    /// refresh from.
    #[test]
    fn test_sql_kind_without_an_expression_is_not_a_declaration() {
        let field = ArrowField::new("half", DataType::Int32, true).with_metadata(HashMap::from([
            (COMPUTED_COLUMN_META_KEY.to_string(), "true".to_string()),
            (KIND_META_KEY.to_string(), SQL_KIND.to_string()),
        ]));
        assert_eq!(computed_column_from_field(&field), None);
    }

    #[tokio::test]
    async fn test_inputs_are_deduplicated_and_sorted() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("b", Int32, [1, 2]), ("a", Int32, [3, 4])).unwrap();
        let table = conn.create_table("dedupe", batch).execute().await.unwrap();

        add_computed(&table, &[("total".into(), "b + a + b".into())])
            .await
            .unwrap();

        assert_eq!(
            declared(&table).await[0].inputs,
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[tokio::test]
    async fn test_dropping_an_input_is_refused() {
        let table = table_with_ints("drop_input").await;
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        let err = table.drop_columns(&["x"]).await.unwrap_err();
        assert!(
            matches!(&err, Error::InvalidInput { message } if message.contains("doubled")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn test_renaming_an_input_is_refused() {
        let table = table_with_ints("rename_input").await;
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        let err = table
            .alter_columns(&[ColumnAlteration::new("x".into()).rename("y".into())])
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::InvalidInput { message } if message.contains("doubled")),
            "{err:?}"
        );
    }

    /// Nothing resolves against nullability, so it is not a rebinding.
    #[tokio::test]
    async fn test_altering_an_input_nullability_is_allowed() {
        let table = table_with_ints("nullable_input").await;
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        table
            .alter_columns(&[ColumnAlteration::new("x".into()).set_nullable(true)])
            .await
            .unwrap();
    }

    /// The gate's reproducer: a volatile function evaluates differently in
    /// the counting and writing passes, so the declared value is incoherent.
    /// Refused at declare time.
    #[tokio::test]
    async fn test_a_volatile_expression_is_refused() {
        let table = table_with_ints("volatile_expr").await;
        let err = add_computed(&table, &[("maybe".into(), "random() < 0.5".into())])
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::InvalidExpression { message, .. }
                if message.contains("random") && message.contains("deterministic")),
            "{err:?}"
        );
    }

    /// The gate's reproducer: the simplifier folds `true OR x > 0` to a
    /// constant, but the stored SQL still names `x`, so the recorded inputs
    /// must too -- otherwise dropping `x` is allowed and refresh breaks.
    #[tokio::test]
    async fn test_inputs_survive_expression_optimization() {
        let table = table_with_ints("optimized_inputs").await;
        add_computed(&table, &[("flag".into(), "true OR x > 0".into())])
            .await
            .unwrap();

        assert_eq!(declared(&table).await[0].inputs, vec!["x".to_string()]);
        let err = table.drop_columns(&["x"]).await.unwrap_err();
        assert!(
            matches!(&err, Error::InvalidInput { message } if message.contains("flag")),
            "{err:?}"
        );
    }

    /// The gate's reproducer: casting a computed column rewrites the field
    /// without its metadata, silently destroying the declaration.
    #[tokio::test]
    async fn test_retyping_the_computed_column_is_refused() {
        use arrow_schema::DataType as ArrowDataType;

        let table = table_with_ints("retype_computed").await;
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        let err = table
            .alter_columns(&[ColumnAlteration::new("doubled".into()).cast_to(ArrowDataType::Int64)])
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::InvalidInput { message } if message.contains("computed")),
            "{err:?}"
        );

        // The declaration survives the refused change.
        table.refresh_column("doubled").await.unwrap();
    }

    /// A declaration cannot be edited, fabricated or erased through field
    /// metadata: it is validated as a whole at declare time.
    #[tokio::test]
    async fn test_declaration_metadata_is_immutable() {
        use crate::table::FieldMetadataUpdate;

        let table = table_with_ints("metadata_tamper").await;
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        // Moving the binding.
        let err = table
            .update_field_metadata(&[
                FieldMetadataUpdate::new("doubled").set(EXPRESSION_META_KEY, "x * 3")
            ])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "{err:?}");

        // Fabricating a declaration on a plain column.
        let err = table
            .update_field_metadata(&[FieldMetadataUpdate::new("x")
                .set(COMPUTED_COLUMN_META_KEY, "true")
                .set(KIND_META_KEY, SQL_KIND)
                .set(EXPRESSION_META_KEY, "x")])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "{err:?}");

        // Erasing the declaration wholesale.
        let err = table
            .update_field_metadata(&[FieldMetadataUpdate::new("doubled")
                .set("note", "hi")
                .replace()])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "{err:?}");

        // Ordinary metadata on a computed column still merges.
        table
            .update_field_metadata(&[FieldMetadataUpdate::new("doubled").set("note", "hi")])
            .await
            .unwrap();
        table.refresh_column("doubled").await.unwrap();
    }

    /// The gate's reproducer: only refresh materializes a declared column;
    /// a direct write would store an arbitrary durable value.
    #[tokio::test]
    async fn test_a_computed_column_cannot_be_written_directly() {
        let table = table_with_ints("direct_write").await;
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        let batch = record_batch!(("x", Int32, [4]), ("doubled", Int32, [999])).unwrap();
        let err = table.add(batch.clone()).execute().await.unwrap_err();
        assert!(
            matches!(&err, Error::InvalidInput { message } if message.contains("refresh")),
            "{err:?}"
        );

        let err = table
            .update()
            .column("doubled", "999")
            .execute()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));

        let mut merge = table.merge_insert(&["x"]);
        merge
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        let err = merge
            .execute(Box::new(arrow_array::RecordBatchIterator::new(
                vec![Ok(batch.clone())],
                batch.schema(),
            )))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));

        // The append that omits the column still works.
        let plain = record_batch!(("x", Int32, [4])).unwrap();
        table.add(plain).execute().await.unwrap();
    }

    /// The gate's reproducer: the reciprocal of the declare-under-spec check.
    #[tokio::test]
    async fn test_installing_an_lsm_spec_over_computed_columns_is_refused() {
        use crate::table::LsmWriteSpec;

        let tmp_dir = tempfile::tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "x",
            DataType::Int32,
            false,
        )]));
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2])) as _],
        )
        .unwrap();
        let table = conn
            .create_table("lsm_after", batch)
            .execute()
            .await
            .unwrap();
        table.set_unenforced_primary_key(["x"]).await.unwrap();
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        let err = table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::NotSupported { message } if message.contains("computed")),
            "{err:?}"
        );
        assert!(table.get_lsm_write_spec().await.unwrap().is_none());
    }

    /// The gate's reproducer: declaration metadata is admitted only through
    /// the validated declare path, never smuggled through a raw transform.
    #[tokio::test]
    async fn test_forged_declaration_metadata_is_rejected() {
        let table = table_with_ints("forged_metadata").await;
        let field =
            ArrowField::new("doubled", DataType::Int32, true).with_metadata(HashMap::from([
                (COMPUTED_COLUMN_META_KEY.to_string(), "true".to_string()),
                (KIND_META_KEY.to_string(), SQL_KIND.to_string()),
                (EXPRESSION_META_KEY.to_string(), "x * 2".to_string()),
                (INPUTS_META_KEY.to_string(), "[]".to_string()),
            ]));
        let err = table
            .add_columns()
            .transform(NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(
                vec![field],
            ))))
            .execute()
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::InvalidInput { message } if message.contains("computed()")),
            "{err:?}"
        );
        assert!(declared(&table).await.is_empty());
    }

    /// The gate's reproducer: SQL INSERT is a write path too.
    #[tokio::test]
    async fn test_sql_insert_cannot_write_a_computed_column() {
        use datafusion::prelude::SessionContext;

        let table = table_with_ints("sql_insert").await;
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let provider =
            crate::table::datafusion::BaseTableAdapter::try_new(table.base_table().clone())
                .await
                .unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let result = async {
            ctx.sql("INSERT INTO t (x, doubled) VALUES (4, 999)")
                .await?
                .collect()
                .await
        }
        .await;
        let err = result.unwrap_err().to_string();
        assert!(err.contains("refresh"), "{err}");
    }

    /// The gate's reproducer: an overwrite must not smuggle in a filled
    /// declaration.
    #[tokio::test]
    async fn test_overwrite_cannot_inject_a_declaration() {
        use crate::table::AddDataMode;

        let table = table_with_ints("overwrite_inject").await;
        let field =
            ArrowField::new("doubled", DataType::Int32, true).with_metadata(HashMap::from([
                (COMPUTED_COLUMN_META_KEY.to_string(), "true".to_string()),
                (KIND_META_KEY.to_string(), SQL_KIND.to_string()),
                (EXPRESSION_META_KEY.to_string(), "x * 2".to_string()),
            ]));
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("x", DataType::Int32, true),
            field,
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::Int32Array::from(vec![1])) as _,
                Arc::new(arrow_array::Int32Array::from(vec![999])) as _,
            ],
        )
        .unwrap();

        let err = table
            .add(batch)
            .mode(AddDataMode::Overwrite)
            .execute()
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::InvalidInput { message } if message.contains("declare")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn test_create_table_cannot_inject_a_declaration() {
        let conn = connect("memory://").execute().await.unwrap();
        let field =
            ArrowField::new("doubled", DataType::Int32, true).with_metadata(HashMap::from([
                (COMPUTED_COLUMN_META_KEY.to_string(), "true".to_string()),
                (KIND_META_KEY.to_string(), SQL_KIND.to_string()),
                (EXPRESSION_META_KEY.to_string(), "x * 2".to_string()),
            ]));
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("x", DataType::Int32, true),
            field,
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::Int32Array::from(vec![1])) as _,
                Arc::new(arrow_array::Int32Array::from(vec![999])) as _,
            ],
        )
        .unwrap();
        let err = conn
            .create_table("forged_create", batch)
            .execute()
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::InvalidInput { message } if message.contains("computed()")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn test_sql_insert_omitting_computed_is_allowed() {
        use datafusion::prelude::SessionContext;

        let table = table_with_ints("sql_insert_omitted").await;
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let provider =
            crate::table::datafusion::BaseTableAdapter::try_new(table.base_table().clone())
                .await
                .unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();
        ctx.sql("INSERT INTO t (x) VALUES (4)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        table.checkout_latest().await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 4);
    }

    #[tokio::test]
    async fn test_a_nested_computed_field_cannot_be_renamed() {
        let table = table_with_ints("computed_struct_rename").await;
        add_computed(&table, &[("payload".into(), "named_struct('a', x)".into())])
            .await
            .unwrap();

        let err = table
            .alter_columns(&[ColumnAlteration::new("payload.a".into()).rename("b".into())])
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::InvalidInput { message } if message.contains("payload")),
            "{err:?}"
        );
    }

    /// Stale handles must not commit the computed/LSM state in either order.
    #[tokio::test]
    async fn test_stale_handles_cannot_mix_computed_and_lsm() {
        use crate::table::LsmWriteSpec;

        let tmp_dir = tempfile::tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "x",
            DataType::Int32,
            false,
        )]));
        let batch = arrow_array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![1])) as _],
        )
        .unwrap();
        let conn = connect(uri).execute().await.unwrap();
        let table = conn.create_table("mix", batch).execute().await.unwrap();
        table.set_unenforced_primary_key(["x"]).await.unwrap();
        let stale = conn.open_table("mix").execute().await.unwrap();

        // Declare on one handle; the stale handle must not install a spec.
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();
        let err = stale
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotSupported { .. }), "install won");

        // Reverse order on fresh tables.
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int32Array::from(vec![1])) as _],
        )
        .unwrap();
        let table = conn.create_table("mix2", batch).execute().await.unwrap();
        table.set_unenforced_primary_key(["x"]).await.unwrap();
        let stale = conn.open_table("mix2").execute().await.unwrap();
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();
        let err = add_computed(&stale, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotSupported { .. }), "declare won");
    }

    /// The gate's reproducer: after catch-up activation, an LSM write, and
    /// unset, retained SSTable rows survive without a live spec. The catch-up
    /// flag is the durable marker; declaration refuses on it.
    #[tokio::test]
    async fn test_unset_with_retained_lsm_rows_cannot_admit_a_declaration() {
        use crate::table::LsmWriteSpec;
        use arrow_array::{Int64Array, RecordBatchIterator};

        let tmp_dir = tempfile::tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("value", DataType::Int64, false),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as _,
                Arc::new(Int64Array::from(vec![10, 20])) as _,
            ],
        )
        .unwrap();
        let table = conn
            .create_table("t", batch.clone())
            .execute()
            .await
            .unwrap();
        table.set_unenforced_primary_key(["id"]).await.unwrap();
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();

        let mut merge = table.merge_insert(&["id"]);
        merge
            .when_matched_update_all(None)
            .when_not_matched_insert_all()
            .use_lsm(true);
        merge
            .execute(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))
            .await
            .unwrap();
        table.unset_lsm_write_spec().await.unwrap();

        let err = add_computed(&table, &[("doubled".into(), "value * 2".into())])
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::NotSupported { message } if message.contains("LSM")),
            "{err:?}"
        );
    }

    /// A declaration does not read itself, so it travels with its binding.
    #[tokio::test]
    async fn test_dropping_the_computed_column_is_allowed() {
        let table = table_with_ints("drop_computed").await;
        add_computed(&table, &[("doubled".into(), "x * 2".into())])
            .await
            .unwrap();

        table.drop_columns(&["doubled"]).await.unwrap();
        assert!(declared(&table).await.is_empty());
    }

    fn function_input_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            ArrowField::new("title", DataType::Utf8, true),
            ArrowField::new("body", DataType::Utf8, true),
        ])
    }

    fn named_struct_application(columns: &str) -> FunctionApplication {
        FunctionApplication::from_json(&format!(
            r#"{{
                "function":{{"name":"text_features","version":"fv_exact"}},
                "inputs":[
                    {{"parameter":"title","kind":"column","value":{{"path":"title"}}}},
                    {{"parameter":"body","kind":"column","value":{{"path":"body"}}}}
                ],
                "output":{{"kind":"named_struct","fields":[
                    {{"name":"normalized_text","arrow_type":"utf8","nullable":false}},
                    {{"name":"token_count","arrow_type":"int64","nullable":false}}
                ]}},
                "columns":{columns}
            }}"#
        ))
        .unwrap()
    }

    fn function_binding_schema(title_nullable: bool, body_nullable: bool) -> ArrowSchema {
        ArrowSchema::new(vec![
            ArrowField::new("title", DataType::Utf8, title_nullable),
            ArrowField::new("body", DataType::Utf8, body_nullable),
            ArrowField::new("search_text", DataType::Utf8, true),
            ArrowField::new("search_token_count", DataType::Int64, true),
        ])
    }

    #[test]
    fn test_non_nullable_function_inputs_can_bind_to_nullable_parameters() {
        let binding = FunctionBinding::from_json(include_str!(
            "../../tests/fixtures/first_class_functions/v1/remote_function_binding.json"
        ))
        .unwrap();

        ensure_binding_matches_schema(&function_binding_schema(false, false), &binding).unwrap();
    }

    #[test]
    fn test_nullable_function_input_cannot_bind_to_non_nullable_parameter() {
        let mut raw_binding: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/first_class_functions/v1/remote_function_binding.json"
        ))
        .unwrap();
        raw_binding["inputs"][0]["nullable"] = Value::Bool(false);
        raw_binding["input_schema"]["fields"][0]["nullable"] = Value::Bool(false);
        let binding: FunctionBinding = serde_json::from_value(raw_binding).unwrap();

        let err = ensure_binding_matches_schema(&function_binding_schema(true, false), &binding)
            .unwrap_err();
        assert!(
            matches!(&err, Error::InvalidInput { message }
                if message.contains("input column 'title' is nullable")
                    && message.contains("parameter 'title'")
                    && message.contains("binding 'fb_01K3TEXT'")
                    && message.contains("non-nullable")),
            "{err:?}"
        );
    }

    #[test]
    fn test_function_binding_metadata_survives_schema_round_trip() {
        let binding = FunctionBinding::from_json(include_str!(
            "../../tests/fixtures/first_class_functions/v1/remote_function_binding.json"
        ))
        .unwrap();
        let raw = function_bindings_metadata(std::slice::from_ref(&binding)).unwrap();
        let mut fields = vec![
            ArrowField::new("title", DataType::Utf8, true),
            ArrowField::new("body", DataType::Utf8, true),
        ];
        fields.extend(
            binding
                .outputs()
                .iter()
                .map(|output| {
                    let data_type = match output.arrow_type.as_str() {
                        "utf8" => DataType::Utf8,
                        "int64" => DataType::Int64,
                        other => panic!("unexpected fixture output type {other}"),
                    };
                    let metadata = function_computed_column_metadata(
                        binding.binding_id(),
                        output.output_ordinal,
                        &["title".into(), "body".into()],
                    );
                    ArrowField::new(&output.output_name, data_type, true).with_metadata(metadata)
                })
                .collect::<Vec<_>>(),
        );
        let schema = ArrowSchema::new_with_metadata(
            fields,
            HashMap::from([(FUNCTION_BINDINGS_META_KEY.to_string(), raw)]),
        );

        let reopened =
            ArrowSchema::new_with_metadata(schema.fields().to_vec(), schema.metadata().clone());
        let bindings = function_bindings(&reopened).unwrap();
        assert_eq!(bindings, vec![binding.clone()]);
        assert!(bindings[0].input_schema().is_some());
        assert!(bindings[0].output_schema().is_some());
        assert!(matches!(
            computed_column_from_field(reopened.field(3)).unwrap().kind,
            ComputedColumnKind::Function {
                ref binding_id,
                output_ordinal: 1,
            } if binding_id == "fb_01K3TEXT"
        ));
        let err = plan_function_application(&reopened, &named_struct_application("{}"), None)
            .unwrap_err();
        assert!(matches!(err, Error::NotSupported { .. }));
    }

    #[test]
    fn test_newer_binding_fields_remain_readable_but_fail_closed_on_mutation() {
        let raw_binding: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/first_class_functions/v1/remote_function_binding.json"
        ))
        .unwrap();
        let binding: FunctionBinding = serde_json::from_value(raw_binding.clone()).unwrap();
        assert_eq!(binding.binding_id(), "fb_01K3TEXT");

        let schema = ArrowSchema::new_with_metadata(
            Vec::<ArrowField>::new(),
            HashMap::from([(
                FUNCTION_BINDINGS_META_KEY.to_string(),
                serde_json::json!({
                    "version": FUNCTION_BINDINGS_VERSION,
                    "bindings": [raw_binding],
                })
                .to_string(),
            )]),
        );
        let err = ensure_supported_function_metadata(&schema).unwrap_err();
        assert!(matches!(err, Error::NotSupported { .. }));
    }

    #[test]
    fn test_named_struct_can_be_kept_as_one_nullable_physical_column() {
        let application = named_struct_application("{}");
        let plan =
            plan_function_application(&function_input_schema(), &application, Some("features"))
                .unwrap();

        assert_eq!(plan.outputs.len(), 1);
        assert_eq!(plan.outputs[0].result_field, WHOLE_RESULT_FIELD);
        assert_eq!(plan.output_schema.fields.len(), 1);
        assert!(plan.output_schema.fields[0].nullable);
        assert_eq!(plan.output_schema.fields[0].r#type.r#type, "struct");
        assert_eq!(
            plan.output_schema.fields[0]
                .r#type
                .fields
                .as_ref()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn test_function_mapping_and_sibling_collisions_fail_before_request() {
        let unknown = named_struct_application(r#"{"missing":"renamed"}"#);
        let err = plan_function_application(&function_input_schema(), &unknown, None).unwrap_err();
        assert!(matches!(&err, Error::InvalidInput { message } if message.contains("unknown")));

        let duplicate =
            named_struct_application(r#"{"normalized_text":"same","token_count":"same"}"#);
        let err =
            plan_function_application(&function_input_schema(), &duplicate, None).unwrap_err();
        assert!(
            matches!(&err, Error::InvalidInput { message } if message.contains("destinations"))
        );

        let mut fields = function_input_schema().fields().to_vec();
        fields.push(Arc::new(ArrowField::new(
            "token_count",
            DataType::Int64,
            true,
        )));
        let collision_schema = ArrowSchema::new(fields);
        let err =
            plan_function_application(&collision_schema, &named_struct_application("{}"), None)
                .unwrap_err();
        assert!(matches!(err, Error::ColumnAlreadyExists { name } if name == "token_count"));
    }

    #[test]
    fn test_unknown_and_mixed_version_function_contracts_fail_closed() {
        let application = FunctionApplication::from_json(
            r#"{
                "function":{"name":"f","version":"fv"},
                "inputs":[{"parameter":"title","kind":"future_source","value":{"path":"title"}}],
                "output":{"kind":"scalar","arrow_type":"int64","nullable":false}
            }"#,
        )
        .unwrap();
        let err = plan_function_application(&function_input_schema(), &application, Some("out"))
            .unwrap_err();
        assert!(matches!(err, Error::NotSupported { .. }));

        let future_application = FunctionApplication::from_json(
            r#"{
                "function":{"name":"f","version":"fv"},
                "inputs":[],
                "output":{"kind":"scalar","arrow_type":"int64","nullable":false},
                "future_declaration":{"mode":"managed"}
            }"#,
        )
        .unwrap();
        let err =
            plan_function_application(&function_input_schema(), &future_application, Some("out"))
                .unwrap_err();
        assert!(matches!(err, Error::NotSupported { .. }));

        let nested_future_application = FunctionApplication::from_json(
            r#"{
                "function":{"name":"f","version":"fv"},
                "inputs":[],
                "output":{"kind":"scalar","arrow_type":"int64","nullable":false,"assignment":"cell_flag"}
            }"#,
        )
        .unwrap();
        let err = plan_function_application(
            &function_input_schema(),
            &nested_future_application,
            Some("out"),
        )
        .unwrap_err();
        assert!(matches!(err, Error::NotSupported { .. }));

        let mixed_schema = ArrowSchema::new_with_metadata(
            function_input_schema().fields().to_vec(),
            HashMap::from([(
                FUNCTION_BINDINGS_META_KEY.to_string(),
                r#"{"version":2,"bindings":[]}"#.to_string(),
            )]),
        );
        let err = plan_function_application(&mixed_schema, &named_struct_application("{}"), None)
            .unwrap_err();
        assert!(matches!(err, Error::NotSupported { .. }));
    }

    #[test]
    fn test_function_inputs_use_paths_and_cannot_be_computed() {
        let mut schema = function_input_schema();
        let plan =
            plan_function_application(&schema, &named_struct_application("{}"), None).unwrap();
        assert_eq!(plan.input_bindings[0].field_path, "title");
        assert_eq!(plan.input_bindings[1].field_path, "body");

        let title = schema
            .field(0)
            .as_ref()
            .clone()
            .with_metadata(HashMap::from([
                (COMPUTED_COLUMN_META_KEY.to_string(), "true".to_string()),
                (KIND_META_KEY.to_string(), SQL_KIND.to_string()),
                (EXPRESSION_META_KEY.to_string(), "title".to_string()),
            ]));
        schema = ArrowSchema::new(vec![title, schema.field(1).as_ref().clone()]);
        let err =
            plan_function_application(&schema, &named_struct_application("{}"), None).unwrap_err();
        assert!(
            matches!(&err, Error::InvalidInput { message } if message.contains("computed-on-computed"))
        );
    }
}
