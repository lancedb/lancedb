// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Canonical Function values exchanged with the Enterprise service, plus the
//! backend-neutral terminal result of a computed-column refresh.
//!
//! This module contains client/wire values only. Catalog persistence,
//! environment bake, secret resolution, and execution are owned by Sophon.

use std::collections::BTreeMap;

use serde::de::{self, DeserializeOwned};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::{Error, Result};

/// Semantic Function type for a Blob v2 value.
pub const FUNCTION_BLOB_V2_TYPE: &str = "blob_v2";

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
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PythonRuntimeSpec {
    /// The V1 Sophon-managed Python runtime.
    Python {
        python_version: String,
        environment: PythonEnvironmentSpec,
        env: BTreeMap<String, String>,
    },
    /// The GPU-enabled Sophon-managed Python runtime.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::BTreeMap;
    /// use lancedb::function::{PythonEnvironmentSpec, PythonRuntimeSpec};
    ///
    /// let runtime = PythonRuntimeSpec::PythonV2 {
    ///     python_version: "3.12".to_string(),
    ///     environment: PythonEnvironmentSpec {
    ///         kind: "pip".to_string(),
    ///         packages: vec!["cupy-cuda12x".to_string()],
    ///         channels: Vec::new(),
    ///         path: None,
    ///         modules: Vec::new(),
    ///         image: None,
    ///     },
    ///     env: BTreeMap::new(),
    /// };
    /// assert!(runtime.requires_gpu());
    /// ```
    PythonV2 {
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
            Self::PythonV2 { .. } => "python_v2",
            Self::Unrecognized { kind } => kind,
        }
    }

    /// The Python version for a known Python runtime, or `None` for an unknown kind.
    pub fn python_version(&self) -> Option<&str> {
        match self {
            Self::Python { python_version, .. } | Self::PythonV2 { python_version, .. } => {
                Some(python_version)
            }
            Self::Unrecognized { .. } => None,
        }
    }

    /// The Python environment for a known Python runtime, or `None` for an unknown kind.
    pub fn environment(&self) -> Option<&PythonEnvironmentSpec> {
        match self {
            Self::Python { environment, .. } | Self::PythonV2 { environment, .. } => {
                Some(environment)
            }
            Self::Unrecognized { .. } => None,
        }
    }

    /// Environment variables, or `None` for an unknown kind.
    pub fn env(&self) -> Option<&BTreeMap<String, String>> {
        match self {
            Self::Python { env, .. } | Self::PythonV2 { env, .. } => Some(env),
            Self::Unrecognized { .. } => None,
        }
    }

    /// Whether the runtime requires a GPU selected by the execution platform.
    pub fn requires_gpu(&self) -> bool {
        matches!(self, Self::PythonV2 { .. })
    }
}

#[derive(Deserialize)]
struct PythonRuntimeV1Wire {
    python_version: String,
    environment: PythonEnvironmentSpec,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    gpu: Option<Value>,
}

#[derive(Deserialize)]
struct PythonRuntimeV2Wire {
    python_version: String,
    environment: PythonEnvironmentSpec,
    #[serde(default)]
    env: BTreeMap<String, String>,
    gpu: bool,
}

impl<'de> Deserialize<'de> for PythonRuntimeSpec {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        let kind = value
            .get("kind")
            .ok_or_else(|| de::Error::missing_field("kind"))?
            .as_str()
            .ok_or_else(|| de::Error::custom("runtime.kind must be a string"))?
            .to_string();
        match kind.as_str() {
            "python" => {
                let wire: PythonRuntimeV1Wire =
                    serde_json::from_value(value).map_err(de::Error::custom)?;
                if wire.gpu.is_some() {
                    return Err(de::Error::custom(
                        "python runtime with gpu requires kind='python_v2'",
                    ));
                }
                Ok(Self::Python {
                    python_version: wire.python_version,
                    environment: wire.environment,
                    env: wire.env,
                })
            }
            "python_v2" => {
                let wire: PythonRuntimeV2Wire =
                    serde_json::from_value(value).map_err(de::Error::custom)?;
                if !wire.gpu {
                    return Err(de::Error::custom("runtime.gpu must be true"));
                }
                Ok(Self::PythonV2 {
                    python_version: wire.python_version,
                    environment: wire.environment,
                    env: wire.env,
                })
            }
            _ => Ok(Self::Unrecognized { kind }),
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
            #[serde(skip_serializing_if = "Option::is_none")]
            gpu: Option<bool>,
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
                gpu: None,
            }
            .serialize(serializer),
            Self::PythonV2 {
                python_version,
                environment,
                env,
            } => PythonRuntimeRef {
                kind: "python_v2",
                python_version,
                environment,
                env,
                gpu: Some(true),
            }
            .serialize(serializer),
            Self::Unrecognized { kind } => UnrecognizedRuntimeRef { kind }.serialize(serializer),
        }
    }
}

/// Immutable Function version returned by the Enterprise catalog.
///
/// The GPU execution requirement is part of this identity. CPU and memory sizing,
/// priority, concurrency, and retry policy belong to the execution platform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionVersion {
    name: String,
    version: String,
    artifact: FunctionArtifact,
    signature: FunctionSignature,
    runtime: PythonRuntimeSpec,
    runtime_digest: String,
    environment_digest: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    secret_bindings: BTreeMap<String, String>,
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

    /// Declared environment variable name to the Secret each one resolves.
    ///
    /// Bindings are part of this version's identity; the credentials behind
    /// them are not, and resolve at execution. Rotating a bound Secret
    /// therefore changes what the same version runs with, and no value has a
    /// field in this model.
    pub fn secret_bindings(&self) -> &BTreeMap<String, String> {
        &self.secret_bindings
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
/// Credential values deliberately have no field here. The only secret-shaped
/// thing a client sends is `secret_bindings`: the name of a Secret the
/// database already holds, which Sophon resolves inside the remote runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionRegistrationRequest {
    pub name: String,
    pub artifact: FunctionArtifactRequest,
    pub signature: FunctionSignature,
    pub runtime: PythonRuntimeSpec,
    /// Declared environment variable name to the Secret it binds. Every bound
    /// Secret must already exist; registration fails otherwise.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secret_bindings: BTreeMap<String, String>,
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
/// `nullable` describes the logical Function result. Physical computed-column
/// fields remain nullable while unassigned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputMapping {
    pub result_field: String,
    pub output_name: String,
    pub output_field_id: i32,
    pub output_ordinal: u32,
    pub arrow_type: String,
    pub nullable: bool,
}

/// Internal physical column preserving the parent validity of a flattened
/// named-struct result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentMapping {
    pub output_name: String,
    pub output_field_id: i32,
}

/// Immutable Function binding persisted by the Enterprise table service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionBinding {
    binding_id: String,
    function: FunctionVersionRef,
    inputs: Vec<InputBinding>,
    outputs: Vec<OutputMapping>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    assignment: Option<AssignmentMapping>,
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

    pub fn assignment(&self) -> Option<&AssignmentMapping> {
        self.assignment.as_ref()
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
mod conda_environment_tests {
    use super::{PythonEnvironmentSpec, PythonRuntimeSpec};

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

    #[test]
    fn gpu_python_runtime_marker_round_trips_and_validates() {
        let runtime: PythonRuntimeSpec = serde_json::from_str(
            r#"{"kind":"python_v2","python_version":"3.12","environment":{"kind":"pip"},"gpu":true}"#,
        )
        .unwrap();
        assert_eq!(runtime.kind(), "python_v2");
        assert!(runtime.requires_gpu());
        assert_eq!(
            super::canonical_json(&runtime).unwrap(),
            r#"{"environment":{"kind":"pip"},"gpu":true,"kind":"python_v2","python_version":"3.12"}"#
        );

        for invalid in [
            r#"{"kind":"python","python_version":"3.12","environment":{"kind":"pip"},"gpu":true}"#,
            r#"{"kind":"python_v2","python_version":"3.12","environment":{"kind":"pip"}}"#,
            r#"{"kind":"python_v2","python_version":"3.12","environment":{"kind":"pip"},"gpu":1}"#,
            r#"{"kind":"python_v2","python_version":"3.12","environment":{"kind":"pip"},"gpu":false}"#,
            r#"{"kind":"python_v2","python_version":"3.12","environment":{"kind":"pip"},"gpu":"true"}"#,
            r#"{"kind":"python_v2","python_version":"3.12","environment":{"kind":"pip"},"gpu":"H100"}"#,
        ] {
            assert!(serde_json::from_str::<PythonRuntimeSpec>(invalid).is_err());
        }
    }

    #[test]
    fn unknown_runtime_discards_payload_before_known_field_validation() {
        for encoded in [
            r#"{"kind":"python_v3","gpu":{"model":"H100"}}"#,
            r#"{"kind":"python_v3","resources":[]}"#,
            r#"{"kind":"python_v3","python_version":3.15,"environment":{"kind":[]}}"#,
        ] {
            let runtime: PythonRuntimeSpec = serde_json::from_str(encoded).unwrap();
            assert_eq!(runtime.kind(), "python_v3");
            assert_eq!(
                super::canonical_json(&runtime).unwrap(),
                r#"{"kind":"python_v3"}"#
            );
        }
    }
}
