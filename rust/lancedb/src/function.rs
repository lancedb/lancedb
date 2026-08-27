// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Canonical Function values exchanged with the Enterprise service, plus the
//! backend-neutral terminal result of a computed-column refresh.
//!
//! This module contains client/wire values only. Catalog persistence,
//! environment bake, secret resolution, and execution are owned by Sophon.

use std::collections::{BTreeMap, BTreeSet};

use serde::de::{self, DeserializeOwned};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::{Error, Result};

// Keep these byte limits aligned with Sophon's Function submission validation.
pub(crate) const MAX_FUNCTION_SECRET_VALUE_BYTES: usize = 64 * 1024;
const MAX_FUNCTION_SECRET_VALUES_BYTES: usize = 512 * 1024;

fn is_portable_environment_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b'A'..=b'Z' | b'a'..=b'z' | b'_'))
        && bytes.all(|byte| matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_'))
}

fn invalid_json(error: impl std::fmt::Display) -> Error {
    Error::InvalidInput {
        message: format!("invalid remote Function JSON: {error}"),
    }
}

fn write_canonical_json(value: &Value, output: &mut String) -> serde_json::Result<()> {
    match value {
        Value::Object(map) => {
            output.push('{');
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_unstable_by_key(|(key, _)| *key);
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                output.push_str(&serde_json::to_string(key)?);
                output.push(':');
                write_canonical_json(value, output)?;
            }
            output.push('}');
        }
        Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                write_canonical_json(value, output)?;
            }
            output.push(']');
        }
        other => output.push_str(&serde_json::to_string(other)?),
    }
    Ok(())
}

fn canonical_json<T: Serialize>(value: &T) -> Result<String> {
    let value = serde_json::to_value(value).map_err(invalid_json)?;
    let mut output = String::new();
    write_canonical_json(&value, &mut output).map_err(invalid_json)?;
    Ok(output)
}

fn from_json<T: DeserializeOwned>(json: &str) -> Result<T> {
    serde_json::from_str(json).map_err(invalid_json)
}

fn validate_literal(value: &Value) -> Result<()> {
    match value {
        Value::Number(number) if number.is_f64() => Err(Error::InvalidInput {
            message: "floating-point Function literals are not part of the Slice 1 canonical wire contract"
                .to_string(),
        }),
        Value::Array(values) => values.iter().try_for_each(validate_literal),
        Value::Object(values) => values.values().try_for_each(validate_literal),
        _ => Ok(()),
    }
}

fn has_unknown_keys(value: &Value, allowed: &[&str]) -> bool {
    value
        .as_object()
        .is_some_and(|object| object.keys().any(|key| !allowed.contains(&key.as_str())))
}

fn application_has_unknown_nested_fields(value: &Value) -> bool {
    let Some(application) = value.as_object() else {
        return false;
    };
    if application
        .get("function")
        .is_some_and(|value| has_unknown_keys(value, &["name", "version"]))
    {
        return true;
    }
    if application
        .get("inputs")
        .and_then(Value::as_array)
        .is_some_and(|inputs| {
            inputs
                .iter()
                .any(|input| has_unknown_keys(input, &["parameter", "kind", "value"]))
        })
    {
        return true;
    }
    application.get("output").is_some_and(|output| {
        has_unknown_keys(output, &["kind", "arrow_type", "nullable", "fields"])
            || output
                .get("fields")
                .and_then(Value::as_array)
                .is_some_and(|fields| {
                    fields
                        .iter()
                        .any(|field| has_unknown_keys(field, &["name", "arrow_type", "nullable"]))
                })
    })
}

macro_rules! impl_json {
    ($type:ty) => {
        impl $type {
            /// Decode a remote value. Unknown fields and discriminator values
            /// are accepted so newer servers remain readable.
            pub fn from_json(json: &str) -> Result<Self> {
                from_json(json)
            }

            /// Encode the known client contract with bytewise-sorted JSON keys.
            pub fn to_canonical_json(&self) -> Result<String> {
                canonical_json(self)
            }
        }
    };
}

/// Packaged Python artifact identity. Source bytes are never part of this value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionArtifact {
    pub kind: String,
    pub digest: String,
    pub entrypoint: String,
}

/// One ordered Arrow input parameter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionParameter {
    pub name: String,
    pub arrow_type: String,
    pub nullable: bool,
}

/// One field of an ordered named-struct result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionResultField {
    pub name: String,
    pub arrow_type: String,
    pub nullable: bool,
}

/// Scalar or named-struct Function output.
///
/// `kind` remains a string so unknown future result shapes can be decoded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionOutput {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arrow_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<FunctionResultField>,
}

/// Ordered language-neutral Function signature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionSignature {
    pub inputs: Vec<FunctionParameter>,
    pub output: FunctionOutput,
}

/// One Python environment source.
///
/// The selected source is interpreted by Sophon. `kind` is open for forward
/// compatible decoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PythonEnvironmentSpec {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub packages: Vec<String>,
    /// Conda channels in priority order; conda environments only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub channels: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub modules: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
}

/// Reproducible Python runtime definition understood by Sophon.
///
/// `env` contains non-secret values. Secret values are submission-only in the
/// client model and do not become part of this public runtime identity;
/// [`FunctionVersion::required_secrets`] contains names only. Sophon persists
/// submitted values separately in the private execution artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PythonRuntimeSpec {
    /// The V1 Sophon-managed Python runtime.
    Python {
        python_version: String,
        environment: PythonEnvironmentSpec,
        env: BTreeMap<String, String>,
    },
    /// A runtime kind introduced by a newer server.
    ///
    /// Unknown payload fields are intentionally not retained because the
    /// client does not proxy catalog values.
    Unrecognized { kind: String },
}

impl PythonRuntimeSpec {
    /// The wire discriminator reported by Sophon.
    pub fn kind(&self) -> &str {
        match self {
            Self::Python { .. } => "python",
            Self::Unrecognized { kind } => kind,
        }
    }

    /// The Python version for the V1 runtime, or `None` for an unknown kind.
    pub fn python_version(&self) -> Option<&str> {
        match self {
            Self::Python { python_version, .. } => Some(python_version),
            Self::Unrecognized { .. } => None,
        }
    }

    /// The Python environment for the V1 runtime, or `None` for an unknown kind.
    pub fn environment(&self) -> Option<&PythonEnvironmentSpec> {
        match self {
            Self::Python { environment, .. } => Some(environment),
            Self::Unrecognized { .. } => None,
        }
    }

    /// Non-secret environment variables, or `None` for an unknown kind.
    pub fn env(&self) -> Option<&BTreeMap<String, String>> {
        match self {
            Self::Python { env, .. } => Some(env),
            Self::Unrecognized { .. } => None,
        }
    }
}

#[derive(Deserialize)]
struct PythonRuntimeWire {
    kind: String,
    #[serde(default)]
    python_version: Option<String>,
    #[serde(default)]
    environment: Option<PythonEnvironmentSpec>,
    #[serde(default)]
    env: BTreeMap<String, String>,
}

impl<'de> Deserialize<'de> for PythonRuntimeSpec {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let wire = PythonRuntimeWire::deserialize(deserializer)?;
        if wire.kind == "python" {
            Ok(Self::Python {
                python_version: wire
                    .python_version
                    .ok_or_else(|| de::Error::missing_field("python_version"))?,
                environment: wire
                    .environment
                    .ok_or_else(|| de::Error::missing_field("environment"))?,
                env: wire.env,
            })
        } else {
            Ok(Self::Unrecognized { kind: wire.kind })
        }
    }
}

impl Serialize for PythonRuntimeSpec {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct PythonRuntimeRef<'a> {
            kind: &'static str,
            python_version: &'a str,
            environment: &'a PythonEnvironmentSpec,
            #[serde(skip_serializing_if = "BTreeMap::is_empty")]
            env: &'a BTreeMap<String, String>,
        }

        #[derive(Serialize)]
        struct UnrecognizedRuntimeRef<'a> {
            kind: &'a str,
        }

        match self {
            Self::Python {
                python_version,
                environment,
                env,
            } => PythonRuntimeRef {
                kind: "python",
                python_version,
                environment,
                env,
            }
            .serialize(serializer),
            Self::Unrecognized { kind } => UnrecognizedRuntimeRef { kind }.serialize(serializer),
        }
    }
}

/// Immutable Function version returned by the Enterprise catalog.
///
/// Scheduling resources, priority, concurrency, and retry policy belong to
/// the submitting Job and are not part of this identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionVersion {
    name: String,
    version: String,
    artifact: FunctionArtifact,
    signature: FunctionSignature,
    runtime: PythonRuntimeSpec,
    runtime_digest: String,
    environment_digest: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    required_secrets: Vec<String>,
    created_at: String,
}

impl FunctionVersion {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn artifact(&self) -> &FunctionArtifact {
        &self.artifact
    }

    pub fn signature(&self) -> &FunctionSignature {
        &self.signature
    }

    pub fn runtime(&self) -> &PythonRuntimeSpec {
        &self.runtime
    }

    pub fn runtime_digest(&self) -> &str {
        &self.runtime_digest
    }

    pub fn environment_digest(&self) -> &str {
        &self.environment_digest
    }

    /// Required secret names. Resolved values exist only in Sophon's private
    /// execution artifact and worker launch path.
    pub fn required_secrets(&self) -> &[String] {
        &self.required_secrets
    }

    pub fn created_at(&self) -> &str {
        &self.created_at
    }
}

impl_json!(FunctionVersion);

/// Encoded artifact bytes uploaded with a Function registration request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionArtifactContent {
    /// Encoding of `data`. V1 Python authoring uses `base64`.
    pub encoding: String,
    pub data: String,
}

/// Internal execution adapter selected for a Python callable artifact.
///
/// The adapter converts the public scalar callable to the Arrow batch ABI
/// used by the remote executor. It is part of the request envelope, not a
/// public batch-UDF authoring mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PythonAdapterSpec {
    pub kind: String,
    pub version: u32,
}

/// Python artifact uploaded while registering a Function.
///
/// Unlike [`FunctionArtifact`], which is the durable artifact identity
/// returned by the catalog, this request value contains the encoded source
/// bytes that Sophon must durably bake before publishing a FunctionVersion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionArtifactRequest {
    pub kind: String,
    pub digest: String,
    pub entrypoint: String,
    pub content: FunctionArtifactContent,
    pub adapter: PythonAdapterSpec,
}

/// Stable request envelope for remote immutable Function registration.
///
/// Secret values are submission-only in the client model. Sophon persists them
/// in the database-scoped private execution artifact; returned
/// [`FunctionVersion`] and Job metadata contain only
/// [`Self::required_secrets`] names. Debug formatting always redacts values.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionRegistrationRequest {
    pub name: String,
    pub artifact: FunctionArtifactRequest,
    pub signature: FunctionSignature,
    pub runtime: PythonRuntimeSpec,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_secrets: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secret_values: BTreeMap<String, String>,
}

impl FunctionRegistrationRequest {
    pub(crate) fn validate_secret_values(&self) -> Result<()> {
        let mut required = BTreeSet::new();
        for name in &self.required_secrets {
            if !is_portable_environment_name(name) {
                return Err(Error::InvalidInput {
                    message: format!(
                        "Function secret name {name:?} must be a portable environment variable name"
                    ),
                });
            }
            if !required.insert(name) {
                return Err(Error::InvalidInput {
                    message: format!("Function required_secrets contains duplicate name {name:?}"),
                });
            }
        }

        if let PythonRuntimeSpec::Python { env, .. } = &self.runtime
            && let Some(name) = required.iter().find(|name| env.contains_key(**name))
        {
            return Err(Error::InvalidInput {
                message: format!(
                    "Function runtime env and secret names must be disjoint: {name:?}"
                ),
            });
        }

        let provided = self.secret_values.keys().collect::<BTreeSet<_>>();
        if required != provided {
            return Err(Error::InvalidInput {
                message: "Function secret_values keys must exactly match required_secrets"
                    .to_string(),
            });
        }

        let mut total_bytes = 0usize;
        for (name, value) in &self.secret_values {
            if value.is_empty() {
                return Err(Error::InvalidInput {
                    message: format!("Function secret {name:?} value must be non-empty"),
                });
            }
            if value.contains('\0') {
                return Err(Error::InvalidInput {
                    message: format!("Function secret {name:?} value must not contain NUL"),
                });
            }
            if value.len() > MAX_FUNCTION_SECRET_VALUE_BYTES {
                return Err(Error::InvalidInput {
                    message: format!(
                        "Function secret {name:?} value exceeds the \
                         {MAX_FUNCTION_SECRET_VALUE_BYTES}-byte limit"
                    ),
                });
            }
            total_bytes =
                total_bytes
                    .checked_add(value.len())
                    .ok_or_else(|| Error::InvalidInput {
                        message: "Function secret values exceed the request byte limit".to_string(),
                    })?;
        }
        if total_bytes > MAX_FUNCTION_SECRET_VALUES_BYTES {
            return Err(Error::InvalidInput {
                message: format!(
                    "Function secret values exceed the \
                     {MAX_FUNCTION_SECRET_VALUES_BYTES}-byte request limit"
                ),
            });
        }
        Ok(())
    }
}

impl std::fmt::Debug for FunctionRegistrationRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let secret_values = self
            .secret_values
            .keys()
            .map(|name| (name, "[REDACTED]"))
            .collect::<BTreeMap<_, _>>();
        formatter
            .debug_struct("FunctionRegistrationRequest")
            .field("name", &self.name)
            .field("artifact", &self.artifact)
            .field("signature", &self.signature)
            .field("runtime", &self.runtime)
            .field("required_secrets", &self.required_secrets)
            .field("secret_values", &secret_values)
            .finish()
    }
}

impl_json!(FunctionRegistrationRequest);

/// Exact FunctionVersion reference embedded in applications and bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionVersionRef {
    pub name: String,
    pub version: String,
}

/// Parameter binding in a FunctionApplication.
///
/// `kind` remains open until Python authoring is added in Slice 2. Slice 1
/// freezes JSON integers, strings, booleans, nulls, arrays, and objects as
/// canonical literal values. Floating-point literals are rejected until a
/// language-neutral numeric representation is defined.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApplicationInput {
    pub parameter: String,
    pub kind: String,
    pub value: Value,
}

/// Pre-declaration application of an exact FunctionVersion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionApplication {
    function: FunctionVersionRef,
    inputs: Vec<ApplicationInput>,
    output: FunctionOutput,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    columns: BTreeMap<String, String>,
    #[serde(default, flatten, skip_serializing)]
    unknown_fields: BTreeMap<String, Value>,
    #[serde(default, skip)]
    unknown_nested_fields: bool,
}

impl FunctionApplication {
    pub fn function(&self) -> &FunctionVersionRef {
        &self.function
    }

    pub fn inputs(&self) -> &[ApplicationInput] {
        &self.inputs
    }

    pub fn output(&self) -> &FunctionOutput {
        &self.output
    }

    pub fn columns(&self) -> &BTreeMap<String, String> {
        &self.columns
    }
    /// Whether a newer writer attached application fields this client cannot
    /// validate. Such applications remain readable but must not be declared.
    pub fn has_unknown_fields(&self) -> bool {
        !self.unknown_fields.is_empty() || self.unknown_nested_fields
    }

    /// Decode a remote application after validating the Slice 1 literal domain.
    pub fn from_json(json: &str) -> Result<Self> {
        let value: Value = from_json(json)?;
        let has_unknown_nested_fields = application_has_unknown_nested_fields(&value);
        let mut application: Self = serde_json::from_value(value).map_err(invalid_json)?;
        application.unknown_nested_fields = has_unknown_nested_fields;
        application
            .inputs
            .iter()
            .try_for_each(|input| validate_literal(&input.value))?;
        Ok(application)
    }

    /// Encode the application with bytewise-sorted JSON keys.
    pub fn to_canonical_json(&self) -> Result<String> {
        self.inputs
            .iter()
            .try_for_each(|input| validate_literal(&input.value))?;
        canonical_json(self)
    }
}

/// Stable table input bound to a registered parameter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputBinding {
    pub parameter: String,
    pub field_id: i32,
    pub field_path: String,
    pub arrow_type: String,
    pub nullable: bool,
}

/// Ordered result-field to table-field mapping for a Function binding.
///
/// Assignment state is not part of the Slice 1 client contract. During the
/// NULL transition there is no public Lance cell-flag identifier to persist.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputMapping {
    pub result_field: String,
    pub output_name: String,
    pub output_field_id: i32,
    pub output_ordinal: u32,
    pub arrow_type: String,
    pub nullable: bool,
}

/// Immutable Function binding persisted by the Enterprise table service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionBinding {
    binding_id: String,
    function: FunctionVersionRef,
    inputs: Vec<InputBinding>,
    outputs: Vec<OutputMapping>,
    /// Exact Arrow schema presented to the Function, encoded with the Lance
    /// Namespace Arrow JSON representation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    input_schema: Option<Value>,
    /// Exact physical Arrow schema of the binding's table outputs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    output_schema: Option<Value>,
}

impl FunctionBinding {
    pub fn binding_id(&self) -> &str {
        &self.binding_id
    }

    pub fn function(&self) -> &FunctionVersionRef {
        &self.function
    }

    pub fn inputs(&self) -> &[InputBinding] {
        &self.inputs
    }

    pub fn outputs(&self) -> &[OutputMapping] {
        &self.outputs
    }

    pub fn input_schema(&self) -> Option<&Value> {
        self.input_schema.as_ref()
    }

    pub fn output_schema(&self) -> Option<&Value> {
        self.output_schema.as_ref()
    }
}

impl_json!(FunctionBinding);

/// Stable terminal result of an expression-backed or Function-backed column
/// refresh [`crate::Job`].
///
/// Local refresh jobs produce this value in process. LanceDB Cloud and
/// Enterprise decode the same value from the durable job's terminal payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshColumnResult {
    /// Rows assigned a value by this refresh.
    pub rows_assigned: u64,
    /// Rows whose computation failed.
    pub rows_failed: u64,
    /// Rows that still need a value when the job completes.
    pub rows_remaining: u64,
    /// Exact table version the refresh read.
    pub source_version: u64,
    /// Table version made visible by the refresh, when one was published.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_version: Option<u64>,
}

impl RefreshColumnResult {
    /// Deprecated compatibility alias for `rows_assigned`.
    pub fn rows_filled(&self) -> u64 {
        self.rows_assigned
    }

    /// Deprecated compatibility alias for `published_version`.
    pub fn version(&self) -> Option<u64> {
        self.published_version
    }
}

impl_json!(RefreshColumnResult);

#[cfg(test)]
mod secret_value_tests {
    use super::{
        FunctionRegistrationRequest, MAX_FUNCTION_SECRET_VALUE_BYTES,
        MAX_FUNCTION_SECRET_VALUES_BYTES, PythonRuntimeSpec,
    };
    use crate::Error;

    fn request() -> FunctionRegistrationRequest {
        FunctionRegistrationRequest::from_json(include_str!(
            "../tests/fixtures/first_class_functions/v1/remote_function_registration_request.json"
        ))
        .unwrap()
    }

    #[test]
    fn validates_secret_name_and_value_invariants() {
        let missing = request();
        assert!(matches!(
            missing.validate_secret_values(),
            Err(Error::InvalidInput { message }) if message.contains("exactly match")
        ));

        let mut empty = request();
        empty
            .secret_values
            .insert("API_TOKEN".to_string(), String::new());
        assert!(matches!(
            empty.validate_secret_values(),
            Err(Error::InvalidInput { message }) if message.contains("non-empty")
        ));

        let mut nul = request();
        nul.secret_values
            .insert("API_TOKEN".to_string(), "before\0after".to_string());
        assert!(matches!(
            nul.validate_secret_values(),
            Err(Error::InvalidInput { message }) if message.contains("NUL")
        ));

        let mut unexpected = request();
        unexpected
            .secret_values
            .insert("OTHER".to_string(), "value".to_string());
        assert!(matches!(
            unexpected.validate_secret_values(),
            Err(Error::InvalidInput { message }) if message.contains("exactly match")
        ));
    }

    #[test]
    fn rejects_invalid_duplicate_and_overlapping_secret_declarations() {
        let mut invalid_name = request();
        invalid_name.required_secrets = vec!["BAD=NAME".to_string()];
        invalid_name
            .secret_values
            .insert("BAD=NAME".to_string(), "secret".to_string());

        let mut duplicate = request();
        duplicate.required_secrets = vec!["API_TOKEN".to_string(), "API_TOKEN".to_string()];
        duplicate
            .secret_values
            .insert("API_TOKEN".to_string(), "secret".to_string());

        let mut overlap = request();
        overlap
            .secret_values
            .insert("API_TOKEN".to_string(), "secret".to_string());
        if let PythonRuntimeSpec::Python { env, .. } = &mut overlap.runtime {
            env.insert("API_TOKEN".to_string(), "public".to_string());
        }

        assert!(matches!(
            invalid_name.validate_secret_values(),
            Err(Error::InvalidInput { message }) if message.contains("portable environment variable")
        ));
        assert!(matches!(
            duplicate.validate_secret_values(),
            Err(Error::InvalidInput { message }) if message.contains("duplicate")
        ));
        assert!(matches!(
            overlap.validate_secret_values(),
            Err(Error::InvalidInput { message }) if message.contains("must be disjoint")
        ));
    }

    #[test]
    fn enforces_portable_secret_name_boundaries() {
        for name in ["A", "_", "A0_"] {
            let mut request = request();
            request.required_secrets = vec![name.to_string()];
            request
                .secret_values
                .insert(name.to_string(), "secret".to_string());
            request.validate_secret_values().unwrap();
        }

        for name in ["", "0TOKEN", "BAD-NAME", "TÖKEN"] {
            let mut request = request();
            request.required_secrets = vec![name.to_string()];
            request
                .secret_values
                .insert(name.to_string(), "secret".to_string());
            assert!(matches!(
                request.validate_secret_values(),
                Err(Error::InvalidInput { message })
                    if message.contains("portable environment variable")
            ));
        }
    }

    #[test]
    fn accepts_exact_secret_value_utf8_byte_limit() {
        for value in [
            "x".repeat(MAX_FUNCTION_SECRET_VALUE_BYTES),
            "é".repeat(MAX_FUNCTION_SECRET_VALUE_BYTES / "é".len()),
        ] {
            assert_eq!(value.len(), MAX_FUNCTION_SECRET_VALUE_BYTES);
            let mut request = request();
            request.secret_values.insert("API_TOKEN".to_string(), value);
            request.validate_secret_values().unwrap();
        }
    }

    #[test]
    fn rejects_secret_value_over_utf8_byte_limit() {
        for value in [
            "x".repeat(MAX_FUNCTION_SECRET_VALUE_BYTES + 1),
            "é".repeat(MAX_FUNCTION_SECRET_VALUE_BYTES / "é".len() + 1),
        ] {
            assert!(value.len() > MAX_FUNCTION_SECRET_VALUE_BYTES);
            let mut request = request();
            request.secret_values.insert("API_TOKEN".to_string(), value);
            assert!(matches!(
                request.validate_secret_values(),
                Err(Error::InvalidInput { message }) if message.contains("65536-byte limit")
            ));
        }
    }

    #[test]
    fn rejects_aggregate_secret_value_bytes_over_server_limit() {
        let mut request = request();
        request.required_secrets = (0..9).map(|index| format!("SECRET_{index}")).collect();
        request.secret_values = request
            .required_secrets
            .iter()
            .map(|name| (name.clone(), "x".repeat(MAX_FUNCTION_SECRET_VALUE_BYTES)))
            .collect();

        assert!(matches!(
            request.validate_secret_values(),
            Err(Error::InvalidInput { message })
                if message.contains(&format!("{MAX_FUNCTION_SECRET_VALUES_BYTES}-byte request limit"))
        ));
    }

    #[test]
    fn accepts_exact_aggregate_secret_value_byte_limit() {
        let mut request = request();
        request.required_secrets = (0..8).map(|index| format!("SECRET_{index}")).collect();
        request.secret_values = request
            .required_secrets
            .iter()
            .map(|name| (name.clone(), "x".repeat(MAX_FUNCTION_SECRET_VALUE_BYTES)))
            .collect();

        assert_eq!(
            request
                .secret_values
                .values()
                .map(String::len)
                .sum::<usize>(),
            MAX_FUNCTION_SECRET_VALUES_BYTES
        );
        request.validate_secret_values().unwrap();
    }
}

#[cfg(test)]
mod conda_environment_tests {
    use super::PythonEnvironmentSpec;

    #[test]
    fn conda_channels_round_trip_and_pip_stays_bare() {
        let conda: PythonEnvironmentSpec = serde_json::from_str(
            r#"{"kind":"conda","packages":["numpy"],"channels":["conda-forge"]}"#,
        )
        .unwrap();
        assert_eq!(conda.channels, ["conda-forge"]);
        assert!(
            serde_json::to_string(&conda)
                .unwrap()
                .contains(r#""channels":["conda-forge"]"#)
        );

        let pip: PythonEnvironmentSpec =
            serde_json::from_str(r#"{"kind":"pip","packages":["numpy"]}"#).unwrap();
        assert!(!serde_json::to_string(&pip).unwrap().contains("channels"));
    }
}
