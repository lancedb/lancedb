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

use crate::secrets::SecretBinding;
use crate::{Error, Result};

mod retry;
pub use retry::{FunctionErrorPolicy, FunctionRetryPolicy};

/// Semantic Function type for a Blob v2 value.
pub const FUNCTION_BLOB_V2_TYPE: &str = "blob_v2";

/// Configured placement-independent hardware and worker limits of a Function
/// GPU class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct GpuClass {
    /// Stable class name accepted by Function registration and refresh options.
    pub name: String,
    /// Kubernetes extended resource requested by this class.
    pub resource_name: String,
    /// Human-readable GPU model, when configured by the operator.
    pub gpu_model: Option<String>,
    /// Usable memory of one GPU in the class, in bytes.
    pub gpu_memory_bytes: Option<u64>,
    /// Maximum GPUs one worker in this class may request.
    pub max_gpu_count_per_worker: Option<u32>,
    /// CPU limit for a worker in this class, in millicores.
    pub cpu_millis: Option<u64>,
    /// Memory limit for a worker in this class, in bytes.
    pub memory_bytes: Option<u64>,
    /// Ephemeral-storage limit for a worker in this class, in bytes.
    pub ephemeral_storage_bytes: Option<u64>,
}

pub(crate) const GPU_CLASS_NAME_ERROR: &str = "gpu_class must be a lowercase name of at most 63 letters, digits, or hyphens, and cannot start or end with a hyphen";

pub(crate) fn validate_gpu_class_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 63
        || name.starts_with('-')
        || name.ends_with('-')
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(Error::InvalidInput {
            message: GPU_CLASS_NAME_ERROR.to_string(),
        });
    }
    Ok(())
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

/// Every field a [`FunctionVersionRef`] document may carry. An application or
/// binding with any other key comes from a newer contract.
pub(crate) const FUNCTION_VERSION_REF_FIELDS: &[&str] = &[
    "name",
    "namespace_path",
    "object_id",
    "location",
    "version",
    "manifest_digest",
];

fn application_has_unknown_nested_fields(value: &Value) -> bool {
    let Some(application) = value.as_object() else {
        return false;
    };
    if application
        .get("function")
        .is_some_and(|value| has_unknown_keys(value, FUNCTION_VERSION_REF_FIELDS))
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlobAccess {
    Bytes,
    Reader,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionParameter {
    pub name: String,
    pub arrow_type: String,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob_access: Option<BlobAccess>,
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
    /// Ordered fields of the single initialization row a Function instance is
    /// created with. Each binding supplies its own values; they are not part
    /// of the Function version. Empty when the Function takes none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub initialization: Vec<FunctionParameter>,
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
    /// Debian packages (`name` or `name=version`) added to the Function's
    /// image before its Python environment is installed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub system_packages: Vec<String>,
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
    ///         system_packages: Vec::new(),
    ///     },
    ///     env: BTreeMap::new(),
    ///     gpu_class: Some("a10g".to_string()),
    ///     gpus_per_worker: std::num::NonZeroU32::new(2),
    /// };
    /// assert!(runtime.requires_gpu());
    /// ```
    PythonV2 {
        python_version: String,
        environment: PythonEnvironmentSpec,
        env: BTreeMap<String, String>,
        /// Operator-defined GPU class; absent uses the deployment default.
        gpu_class: Option<String>,
        /// GPUs allocated together to each worker.
        gpus_per_worker: Option<std::num::NonZeroU32>,
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

    /// Operator-defined GPU class, or `None` to use the deployment default.
    pub fn gpu_class(&self) -> Option<&str> {
        match self {
            Self::PythonV2 { gpu_class, .. } => gpu_class.as_deref(),
            _ => None,
        }
    }

    /// GPUs allocated to one worker; zero for CPU runtimes.
    pub fn gpus_per_worker(&self) -> u32 {
        match self {
            Self::PythonV2 {
                gpus_per_worker, ..
            } => gpus_per_worker.map_or(1, std::num::NonZeroU32::get),
            _ => 0,
        }
    }
}

#[derive(Deserialize)]
struct PythonRuntimeV1Wire {
    #[serde(default)]
    gpus_per_worker: Option<Value>,
    python_version: String,
    environment: PythonEnvironmentSpec,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    gpu: Option<Value>,
    #[serde(default)]
    gpu_class: Option<String>,
}

#[derive(Deserialize)]
struct PythonRuntimeV2Wire {
    #[serde(default)]
    gpus_per_worker: Option<std::num::NonZeroU32>,
    python_version: String,
    environment: PythonEnvironmentSpec,
    #[serde(default)]
    env: BTreeMap<String, String>,
    gpu: bool,
    #[serde(default)]
    gpu_class: Option<String>,
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
                if wire.gpu_class.is_some() {
                    return Err(de::Error::custom(
                        "python runtime with gpu_class requires kind='python_v2'",
                    ));
                }
                if wire.gpus_per_worker.is_some() {
                    return Err(de::Error::custom(
                        "gpus_per_worker requires kind='python_v2'",
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
                if let Some(gpu_class) = wire.gpu_class.as_deref() {
                    validate_gpu_class_name(gpu_class).map_err(de::Error::custom)?;
                }
                Ok(Self::PythonV2 {
                    python_version: wire.python_version,
                    environment: wire.environment,
                    env: wire.env,
                    gpu_class: wire.gpu_class,
                    gpus_per_worker: wire.gpus_per_worker,
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
            #[serde(skip_serializing_if = "Option::is_none")]
            gpu_class: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            gpus_per_worker: Option<std::num::NonZeroU32>,
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
                gpu_class: None,
                gpus_per_worker: None,
            }
            .serialize(serializer),
            Self::PythonV2 {
                python_version,
                environment,
                env,
                gpu_class,
                gpus_per_worker,
            } => PythonRuntimeRef {
                kind: "python_v2",
                python_version,
                environment,
                env,
                gpu: Some(true),
                gpu_class: gpu_class.as_deref(),
                gpus_per_worker: *gpus_per_worker,
            }
            .serialize(serializer),
            Self::Unrecognized { kind } => UnrecognizedRuntimeRef { kind }.serialize(serializer),
        }
    }
}

/// An immutable Hugging Face model snapshot required by a Function.
///
/// The service prepares this snapshot before user code runs. `revision` is a
/// full 40-character lowercase commit SHA, not a branch or tag.
///
/// ```
/// use lancedb::function::HuggingFaceModel;
///
/// let model = HuggingFaceModel {
///     repo: "sentence-transformers/all-MiniLM-L6-v2".into(),
///     revision: "1110a243fdf4706b3f48f1d95db1a4f5529b4d41".into(),
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HuggingFaceModel {
    /// Hugging Face repository in `owner/name` form.
    pub repo: String,
    /// Exact repository commit containing the model snapshot.
    pub revision: String,
}

/// A complete OCI Function image. Its digest is independent of catalog names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionImage {
    pub manifest_digest: String,
    pub descriptor: Value,
    pub source: bool,
    /// Immutable snapshots declared by this Function version.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<HuggingFaceModel>,
}

fn deserialize_object_version<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    let value = String::deserialize(deserializer)?;
    match value.parse::<u64>() {
        Ok(number) if number > 0 && number.to_string() == value => Ok(value),
        _ => Err(de::Error::custom(
            "Function version must be a canonical positive uint64",
        )),
    }
}

/// A positive, finite per-call deadline reported by the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(transparent)]
struct RowTimeoutSeconds(f64);

impl Eq for RowTimeoutSeconds {}

impl RowTimeoutSeconds {
    fn get(self) -> f64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for RowTimeoutSeconds {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let seconds = f64::deserialize(deserializer)?;
        if !seconds.is_finite() || seconds <= 0.0 {
            return Err(de::Error::custom(
                "row_timeout_seconds must be positive and finite",
            ));
        }
        Ok(Self(seconds))
    }
}

/// One immutable Function object revision and its executable artifact.
///
/// The Function is named by its parts, `name` and `namespace_path`, the way a
/// [`SecretReference`](crate::secrets::SecretReference) names a Secret.
///
/// Unknown top-level fields from newer servers are accepted for forward
/// decoding, but they are not part of this value and are omitted from
/// canonical JSON. Known fields retain their complete values, while unknown
/// fields inside the open [`FunctionImage::descriptor`] JSON object remain
/// available.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionVersion {
    name: String,
    /// The namespace holding the Function. Empty is the root, and is absent
    /// from the wire, as a [`SecretReference`](crate::secrets::SecretReference)
    /// omits it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    namespace_path: Vec<String>,
    object_id: String,
    location: String,
    #[serde(deserialize_with = "deserialize_object_version")]
    version: String,
    image: FunctionImage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    runtime_digest: Option<String>,
    signature: FunctionSignature,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    secret_bindings: Vec<SecretBinding>,
    /// When this version was created, in milliseconds since the epoch.
    created_at_millis: i64,
    metadata: BTreeMap<String, String>,
    disabled: bool,
    /// Operator-defined GPU class declared at registration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    gpu_class: Option<String>,
    /// Default maximum rows in one callable batch.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_batch_size"
    )]
    batch_size: Option<u32>,
    /// Default maximum concurrent user-code calls per logical execution.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_max_concurrency"
    )]
    max_concurrency: Option<u32>,
    /// Per-call deadline in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    row_timeout_seconds: Option<RowTimeoutSeconds>,
    /// Result-stability value declared at registration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result_stability: Option<String>,
}

impl FunctionVersion {
    pub fn object_id(&self) -> &str {
        &self.object_id
    }
    pub fn location(&self) -> &str {
        &self.location
    }
    pub fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }
    pub fn disabled(&self) -> bool {
        self.disabled
    }
    pub fn reference(&self) -> FunctionVersionRef {
        FunctionVersionRef {
            name: self.name.clone(),
            namespace_path: self.namespace_path.clone(),
            object_id: self.object_id.clone(),
            location: self.location.clone(),
            version: self.version.clone(),
            manifest_digest: self.image.manifest_digest.clone(),
        }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn namespace_path(&self) -> &[String] {
        &self.namespace_path
    }
    pub fn version(&self) -> &str {
        &self.version
    }
    pub fn image(&self) -> &FunctionImage {
        &self.image
    }

    /// Digest of the registered source artifact, when provided by the server.
    ///
    /// Compare this with [`FunctionArtifactRequest::digest`] to identify matching
    /// source, independently of the built image's manifest digest. It excludes
    /// dependencies, environment variables, Python version, and GPU settings;
    /// matching source alone does not establish that a version can be reused.
    ///
    /// ```
    /// use lancedb::function::{FunctionArtifactRequest, FunctionVersion};
    ///
    /// fn has_same_source(version: &FunctionVersion, artifact: &FunctionArtifactRequest) -> bool {
    ///     version.source_digest() == Some(artifact.digest.as_str())
    /// }
    /// ```
    pub fn source_digest(&self) -> Option<&str> {
        self.source_digest.as_deref()
    }

    /// Digest of canonical runtime inputs, when provided by the server.
    ///
    /// Includes Python version, environment sources, variables, and GPU settings.
    /// A missing digest cannot establish that a local runtime matches. Reusing a
    /// version also requires matching its source and other registration inputs.
    pub fn runtime_digest(&self) -> Option<&str> {
        self.runtime_digest.as_deref()
    }

    /// Operator-defined GPU class declared at registration.
    pub fn gpu_class(&self) -> Option<&str> {
        self.gpu_class.as_deref()
    }

    /// Default maximum rows in one callable batch, when declared.
    pub fn batch_size(&self) -> Option<u32> {
        self.batch_size
    }

    /// Default maximum concurrent user-code calls, when declared.
    pub fn max_concurrency(&self) -> Option<u32> {
        self.max_concurrency
    }

    /// Per-call deadline in seconds, when declared.
    pub fn row_timeout_seconds(&self) -> Option<f64> {
        self.row_timeout_seconds.map(RowTimeoutSeconds::get)
    }

    /// Result-stability value declared at registration, when present.
    pub fn result_stability(&self) -> Option<&str> {
        self.result_stability.as_deref()
    }

    pub fn signature(&self) -> &FunctionSignature {
        &self.signature
    }

    /// Declared environment variable name to the Secret each one resolves.
    ///
    /// Bindings are part of this version's identity; the credentials behind
    /// them are not, and resolve at execution. Rotating a bound Secret
    /// therefore changes what the same version runs with, and no value has a
    /// field in this model.
    pub fn secret_bindings(&self) -> &[SecretBinding] {
        &self.secret_bindings
    }

    /// When this version was created, in milliseconds since the epoch.
    pub fn created_at_millis(&self) -> i64 {
        self.created_at_millis
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
/// This is part of the executor request envelope, not the user-facing Function
/// authoring mode. For version 1, the executor selects vectorized execution
/// when every callable parameter is annotated as `pyarrow.Array`; it passes one
/// Arrow array per declared input and requires an Arrow array matching the
/// declared output. Other callables are invoked row by row. Both modes use the
/// same internal adapter kind.
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

fn deserialize_batch_size<'de, D>(deserializer: D) -> std::result::Result<Option<u32>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<u32>::deserialize(deserializer)
        .map_err(|error| de::Error::custom(format!("batch_size: {error}")))?;
    validate_batch_size_value(value).map_err(de::Error::custom)?;
    Ok(value)
}

fn deserialize_row_timeout<'de, D>(deserializer: D) -> std::result::Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<f64>::deserialize(deserializer)
        .map_err(|error| de::Error::custom(format!("row_timeout_seconds: {error}")))?;
    validate_row_timeout_value(value).map_err(de::Error::custom)?;
    Ok(value)
}

fn deserialize_max_concurrency<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<u32>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<u32>::deserialize(deserializer)
        .map_err(|error| de::Error::custom(format!("max_concurrency: {error}")))?;
    validate_max_concurrency_value(value).map_err(de::Error::custom)?;
    Ok(value)
}

fn validate_batch_size_value(value: Option<u32>) -> std::result::Result<(), &'static str> {
    if value == Some(0) {
        return Err("batch_size must be between 1 and u32::MAX");
    }
    Ok(())
}

fn validate_max_concurrency_value(value: Option<u32>) -> std::result::Result<(), &'static str> {
    if value == Some(0) {
        return Err("max_concurrency must be between 1 and u32::MAX");
    }
    Ok(())
}

fn validate_row_timeout_value(value: Option<f64>) -> std::result::Result<(), &'static str> {
    if value.is_some_and(|seconds| !seconds.is_finite() || seconds <= 0.0) {
        return Err("row_timeout_seconds must be positive and finite");
    }
    Ok(())
}

/// Stable request envelope for remote immutable Function registration.
///
/// Credential values deliberately have no field here. The only secret-shaped
/// thing a client sends is `secret_bindings`: the name of a Secret the
/// database already holds, which Sophon resolves inside the remote runtime.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionRegistrationRequest {
    /// Default retries of failed user-code calls. Enterprise only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<FunctionRetryPolicy>,
    pub name: String,
    pub artifact: FunctionArtifactRequest,
    pub signature: FunctionSignature,
    pub runtime: PythonRuntimeSpec,
    /// Maximum rows in one execution batch; the executor may use fewer.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_batch_size"
    )]
    pub batch_size: Option<u32>,
    /// Default maximum concurrent user-code calls for each logical execution
    /// of this Function version. Omission leaves the current behavior
    /// unchanged.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_max_concurrency"
    )]
    pub max_concurrency: Option<u32>,
    /// Deadline for each Python call, in seconds: one row for scalar Functions
    /// and one batch for vectorized Functions. It does not govern Function
    /// initialization.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_row_timeout"
    )]
    pub row_timeout_seconds: Option<f64>,
    /// Declared environment variable name to the Secret it binds. A binding is
    /// a reference: whether the Secret exists is answered when a column is
    /// declared against this version, not here.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secret_bindings: Vec<SecretBinding>,
    /// Model snapshots prepared by the service before user code runs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<HuggingFaceModel>,
}

impl FunctionRegistrationRequest {
    /// Decode a registration request supplied by an SDK language binding.
    pub fn from_json(json: &str) -> Result<Self> {
        from_json(json)
    }

    /// Encode the known client contract with bytewise-sorted JSON keys.
    pub fn to_canonical_json(&self) -> Result<String> {
        self.validate()?;
        canonical_json(self)
    }

    /// Reject explicit execution controls that cannot be submitted safely.
    pub fn validate(&self) -> Result<()> {
        let invalid_input = |message: &'static str| Error::InvalidInput {
            message: message.to_string(),
        };
        validate_batch_size_value(self.batch_size).map_err(invalid_input)?;
        validate_max_concurrency_value(self.max_concurrency).map_err(invalid_input)?;
        validate_row_timeout_value(self.row_timeout_seconds).map_err(invalid_input)?;
        if let Some(policy) = &self.retry_policy {
            policy.validate()?;
        }
        Ok(())
    }
}

/// Exact FunctionVersion reference embedded in applications and bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionVersionRef {
    pub name: String,
    /// The namespace holding the Function; empty is the root and is absent
    /// from the wire.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub namespace_path: Vec<String>,
    pub object_id: String,
    pub location: String,
    #[serde(deserialize_with = "deserialize_object_version")]
    pub version: String,
    pub manifest_digest: String,
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
    /// Initialization values for this binding, by initialization field name.
    /// The service validates them against the Function's initialization
    /// schema and persists the resulting Arrow row in the binding; this JSON
    /// is transport only and never hashed into an identity, so floating-point
    /// values are accepted here.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    initialization: BTreeMap<String, Value>,
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

    /// Initialization values supplied by this application, by field name.
    pub fn initialization(&self) -> &BTreeMap<String, Value> {
        &self.initialization
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
    /// Standard base64 of the one-row Arrow IPC stream every instance of this
    /// binding is created with, already validated against the Function's
    /// initialization schema. Absent when the Function takes none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    initialization: Option<String>,
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

    /// Base64 one-row Arrow IPC initialization stream, if the Function takes one.
    pub fn initialization(&self) -> Option<&str> {
        self.initialization.as_deref()
    }
}

impl_json!(FunctionBinding);

/// Stable terminal result of an expression-backed or Function-backed column
/// refresh [`crate::Job`].
///
/// LanceDB Embedded refresh jobs produce this value in process. LanceDB
/// Enterprise decodes the same value from the durable job's terminal payload.
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

/// Which per-row errors [`crate::Table::function_errors`] lists. Every
/// filter is optional; the listing is table-addressed, so with none set it
/// covers every refresh of every column.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FunctionErrorsRequest {
    /// Only errors recorded by this job.
    pub job_id: Option<String>,
    /// Only errors on this column.
    pub column: Option<String>,
    /// At most this many records; the server default is 10000 and its cap
    /// 100000. [`FunctionErrors::truncated`] says whether the cap was hit.
    pub limit: Option<usize>,
}

impl FunctionErrorsRequest {
    /// A request with no filter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Only errors recorded by `job_id`.
    pub fn job_id(mut self, job_id: impl Into<String>) -> Self {
        self.job_id = Some(job_id.into());
        self
    }

    /// Only errors on `column`.
    pub fn column(mut self, column: impl Into<String>) -> Self {
        self.column = Some(column.into());
        self
    }

    /// At most `limit` records.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// One row a Function refresh skipped, as the server recorded it. The
/// message carries the input that failed, which is why reading errors needs
/// read access to the table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionErrorRecord {
    /// Number of user-code calls for this failed invocation, when recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempts: Option<u32>,
    /// The refresh job that recorded the error.
    pub job_id: String,
    /// The fragment holding the row.
    pub fragment_id: u64,
    /// The row's offset within the fragment; `None` when the fragment's
    /// detail was capped and only the fragment summary remains.
    #[serde(default)]
    pub row_offset: Option<u32>,
    /// Stable row identity at the table version the refresh read.
    ///
    /// Present only when that version used stable row IDs. It remains valid
    /// across later compaction and rewrite, unlike `row_address`. `None` on
    /// legacy tables and on records written before the server reported it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_id: Option<u64>,
    /// Portable physical row address, `(fragment_id << 32) | row_offset`.
    ///
    /// `None` when the record covers a fragment rather than one row, or when
    /// an older server does not report row addresses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_address: Option<u64>,
    /// The column being computed.
    pub column: String,
    /// The Function that failed.
    pub function: String,
    /// The Function's version.
    pub function_version: String,
    /// The table version the refresh read.
    pub table_version: u64,
    /// The error's class, as the executor reported it.
    pub error_type: String,
    /// The error's text.
    pub error_message: String,
    /// User-code traceback, possibly truncated by the server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceback: Option<String>,
    /// When the error was recorded, in milliseconds since the epoch.
    pub created_at_millis: i64,
}

/// A fragment whose per-row detail was capped: `rows_skipped` rows failed,
/// of which only `rows_recorded` have a [`FunctionErrorRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionErrorFragment {
    /// The refresh job that recorded the errors.
    pub job_id: String,
    /// The fragment.
    pub fragment_id: u64,
    /// Rows the refresh skipped in this fragment.
    pub rows_skipped: u64,
    /// Rows with a record of their own.
    pub rows_recorded: u64,
}

/// A table's per-row Function errors; see [`crate::Table::function_errors`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionErrors {
    /// The recorded rows, newest job first.
    pub records: Vec<FunctionErrorRecord>,
    /// Fragments whose detail was capped.
    #[serde(default)]
    pub fragments: Vec<FunctionErrorFragment>,
    /// Whether the listing stopped at its limit.
    #[serde(default)]
    pub truncated: bool,
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
    use serde_json::Value;

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
    fn system_packages_round_trip_and_stay_absent_when_empty() {
        let apt: PythonEnvironmentSpec = serde_json::from_str(
            r#"{"kind":"pip","packages":["torchcodec"],"system_packages":["libavutil58"]}"#,
        )
        .unwrap();
        assert_eq!(apt.system_packages, ["libavutil58"]);
        assert!(
            serde_json::to_string(&apt)
                .unwrap()
                .contains(r#""system_packages":["libavutil58"]"#)
        );
        let pip: PythonEnvironmentSpec =
            serde_json::from_str(r#"{"kind":"pip","packages":["numpy"]}"#).unwrap();
        assert!(
            !serde_json::to_string(&pip)
                .unwrap()
                .contains("system_packages")
        );
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
        let runtime: PythonRuntimeSpec = serde_json::from_str(
            r#"{"kind":"python_v2","python_version":"3.12","environment":{"kind":"pip"},"gpu":true,"gpu_class":"a10g"}"#,
        )
        .unwrap();
        assert_eq!(runtime.gpu_class(), Some("a10g"));
        assert_eq!(
            super::canonical_json(&runtime).unwrap(),
            r#"{"environment":{"kind":"pip"},"gpu":true,"gpu_class":"a10g","kind":"python_v2","python_version":"3.12"}"#
        );
        let runtime: PythonRuntimeSpec = serde_json::from_str(
            r#"{"kind":"python_v2","python_version":"3.12","environment":{"kind":"pip"},"gpu":true,"gpu_class":"a10g","gpus_per_worker":2}"#,
        ).unwrap();
        assert_eq!(runtime.gpus_per_worker(), 2);
        let encoded = super::canonical_json(&runtime).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&encoded).unwrap()["gpus_per_worker"],
            2
        );
        for count in [
            serde_json::json!(0),
            serde_json::json!(-1),
            serde_json::json!(true),
            serde_json::json!("2"),
            serde_json::json!(1.5),
            serde_json::json!(4294967296_u64),
        ] {
            let mut value: Value = serde_json::from_str(&encoded).unwrap();
            value["gpus_per_worker"] = count;
            assert!(serde_json::from_value::<PythonRuntimeSpec>(value).is_err());
        }

        for invalid in [
            r#"{"kind":"python","python_version":"3.12","environment":{"kind":"pip"},"gpu":true}"#,
            r#"{"kind":"python","python_version":"3.12","environment":{"kind":"pip"},"gpu_class":"a10g"}"#,
            r#"{"kind":"python_v2","python_version":"3.12","environment":{"kind":"pip"}}"#,
            r#"{"kind":"python_v2","python_version":"3.12","environment":{"kind":"pip"},"gpu":1}"#,
            r#"{"kind":"python_v2","python_version":"3.12","environment":{"kind":"pip"},"gpu":false}"#,
            r#"{"kind":"python_v2","python_version":"3.12","environment":{"kind":"pip"},"gpu":"true"}"#,
            r#"{"kind":"python_v2","python_version":"3.12","environment":{"kind":"pip"},"gpu":"H100"}"#,
            r#"{"kind":"python_v2","python_version":"3.12","environment":{"kind":"pip"},"gpu":true,"gpu_class":"H100"}"#,
            r#"{"kind":"python_v2","python_version":"3.12","environment":{"kind":"pip"},"gpu":true,"gpu_class":"-bad"}"#,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn function_version_preserves_runtime_digest() {
        const VERSION: &str = include_str!(
            "../tests/fixtures/first_class_functions/v1/remote_function_version.canonical.json"
        );
        let mut document: Value = serde_json::from_str(VERSION).unwrap();
        let legacy = FunctionVersion::from_json(VERSION).unwrap();
        assert_eq!(legacy.runtime_digest(), None);
        assert_eq!(legacy.to_canonical_json().unwrap(), VERSION.trim_end());
        for digest in [Value::Null, serde_json::json!("sha256:runtime-inputs")] {
            document["runtime_digest"] = digest.clone();
            let version = FunctionVersion::from_json(&document.to_string()).unwrap();
            assert_eq!(version.runtime_digest(), digest.as_str());
            let encoded = version.to_canonical_json().unwrap();
            let encoded_value: Value = serde_json::from_str(&encoded).unwrap();
            if digest.is_null() {
                assert!(encoded_value.get("runtime_digest").is_none());
            } else {
                assert_eq!(encoded_value["runtime_digest"], digest);
            }
            assert_eq!(FunctionVersion::from_json(&encoded).unwrap(), version);
        }
    }

    #[test]
    fn function_version_preserves_source_digest() {
        const VERSION: &str = include_str!(
            "../tests/fixtures/first_class_functions/v1/remote_function_version.canonical.json"
        );
        let mut document: Value = serde_json::from_str(VERSION).unwrap();
        let legacy = FunctionVersion::from_json(VERSION).unwrap();
        assert_eq!(legacy.source_digest(), None);
        assert_eq!(legacy.to_canonical_json().unwrap(), VERSION.trim_end());

        for digest in [Value::Null, serde_json::json!("sha256:source-artifact")] {
            document["source_digest"] = digest.clone();
            let version = FunctionVersion::from_json(&document.to_string()).unwrap();
            assert_eq!(version.source_digest(), digest.as_str());
            assert_eq!(version.image(), legacy.image());
            let encoded = version.to_canonical_json().unwrap();
            let encoded_value: Value = serde_json::from_str(&encoded).unwrap();
            if digest.is_null() {
                assert!(encoded_value.get("source_digest").is_none());
            } else {
                assert_eq!(encoded_value["source_digest"], digest);
            }
            assert_eq!(FunctionVersion::from_json(&encoded).unwrap(), version);
        }

        document["gpu_class"] = serde_json::json!("a10g");
        let version = FunctionVersion::from_json(&document.to_string()).unwrap();
        assert_eq!(version.gpu_class(), Some("a10g"));
        let encoded: Value = serde_json::from_str(&version.to_canonical_json().unwrap()).unwrap();
        assert_eq!(encoded["gpu_class"], "a10g");

        document["max_concurrency"] = serde_json::json!(4);
        let version = FunctionVersion::from_json(&document.to_string()).unwrap();
        assert_eq!(version.max_concurrency(), Some(4));
        let encoded: Value = serde_json::from_str(&version.to_canonical_json().unwrap()).unwrap();
        assert_eq!(encoded["max_concurrency"], 4);

        document["max_concurrency"] = serde_json::json!(0);
        assert!(FunctionVersion::from_json(&document.to_string()).is_err());
    }

    #[test]
    fn blob_reader_access_survives_signature_round_trip() {
        let parameter: FunctionParameter = serde_json::from_value(serde_json::json!({
            "name": "video", "arrow_type": "blob_v2", "nullable": false,
            "blob_access": "reader"
        }))
        .unwrap();
        assert_eq!(parameter.blob_access, Some(BlobAccess::Reader));
        assert_eq!(
            serde_json::to_value(parameter).unwrap()["blob_access"],
            "reader"
        );
    }

    /// Canonical form is what the FunctionVersion hash is taken over, so key
    /// order must come from the keys and not from however serde happened to
    /// emit them. Nesting is included because the sort is recursive.
    #[test]
    fn canonical_json_sorts_keys_at_every_depth() {
        let value = serde_json::json!({
            "runtime": {"kind": "python", "env": {"B": "2", "A": "1"}},
            "artifact": {"digest": "sha256:x"},
            "name": "embed",
        });
        let mut out = String::new();
        write_canonical_json(&value, &mut out).expect("canonical JSON");

        assert_eq!(
            out,
            r#"{"artifact":{"digest":"sha256:x"},"name":"embed","runtime":{"env":{"A":"1","B":"2"},"kind":"python"}}"#
        );
    }

    /// A Function is named by its parts, and a root one states no namespace.
    #[test]
    fn function_version_carries_its_namespace_as_a_separate_field() {
        const VERSION: &str = include_str!(
            "../tests/fixtures/first_class_functions/v1/remote_function_version.canonical.json"
        );
        let root = FunctionVersion::from_json(VERSION).unwrap();
        assert!(root.namespace_path().is_empty());
        assert_eq!(root.to_canonical_json().unwrap(), VERSION.trim_end());

        let mut document: Value = serde_json::from_str(VERSION).unwrap();
        document["namespace_path"] = serde_json::json!(["analytics", "features"]);
        let nested = FunctionVersion::from_json(&document.to_string()).unwrap();
        assert_eq!(nested.name(), "embed");
        assert_eq!(nested.namespace_path(), ["analytics", "features"]);
        let reference = nested.reference();
        assert_eq!(reference.name, "embed");
        assert_eq!(reference.namespace_path, ["analytics", "features"]);
        let reference: Value = serde_json::to_value(&reference).unwrap();
        assert_eq!(
            reference["namespace_path"],
            serde_json::json!(["analytics", "features"])
        );
        let root_reference = serde_json::to_value(root.reference()).unwrap();
        assert!(root_reference.get("namespace_path").is_none());

        // An application carrying the namespace is not from a newer contract.
        let application = serde_json::json!({
            "function": reference,
            "inputs": [],
            "output": serde_json::to_value(nested.signature().output.clone()).unwrap(),
        });
        let application = FunctionApplication::from_json(&application.to_string()).unwrap();
        assert!(!application.has_unknown_fields());
        assert_eq!(
            application.function().namespace_path,
            ["analytics", "features"]
        );
    }

    /// Arrays are ordered by the caller, so canonicalization must leave them
    /// alone -- sorting them would change what a signature means.
    #[test]
    fn canonical_json_preserves_array_order() {
        let value = serde_json::json!({"inputs": ["b", "a", "c"]});
        let mut out = String::new();
        write_canonical_json(&value, &mut out).expect("canonical JSON");

        assert_eq!(out, r#"{"inputs":["b","a","c"]}"#);
    }

    /// A float has no single canonical spelling, so two clients could hash the
    /// same literal differently. Rejected at any depth rather than rounded.
    #[test]
    fn validate_literal_rejects_floats_at_any_depth() {
        for value in [
            serde_json::json!(1.5),
            serde_json::json!([1, [2, 3.5]]),
            serde_json::json!({"a": {"b": 0.25}}),
        ] {
            let error = validate_literal(&value).expect_err("floats are not canonical");
            assert!(
                error.to_string().contains("floating-point"),
                "unexpected error: {error}"
            );
        }

        for value in [
            serde_json::json!(1),
            serde_json::json!("1.5"),
            serde_json::json!([1, {"a": true}]),
            serde_json::json!(null),
        ] {
            validate_literal(&value).expect("non-float literals are canonical");
        }
    }

    /// A Function without initialization keeps its pre-initialization wire
    /// form, so existing signatures and applications encode byte-identically.
    #[test]
    fn empty_initialization_is_omitted_from_the_wire() {
        let signature: FunctionSignature = serde_json::from_str(
            r#"{"inputs":[{"name":"x","arrow_type":"int64","nullable":true}],"output":{"kind":"scalar","arrow_type":"int64","nullable":false}}"#,
        )
        .unwrap();
        assert!(signature.initialization.is_empty());
        assert!(
            !canonical_json(&signature)
                .unwrap()
                .contains("initialization")
        );

        let signature: FunctionSignature = serde_json::from_str(
            r#"{"inputs":[],"output":{"kind":"scalar","arrow_type":"int64","nullable":false},"initialization":[{"name":"factor","arrow_type":"float64","nullable":false}]}"#,
        )
        .unwrap();
        assert_eq!(signature.initialization[0].name, "factor");
        assert!(
            canonical_json(&signature)
                .unwrap()
                .contains(r#""initialization":[{"#)
        );
    }

    /// Initialization values are transport JSON, validated and re-encoded as
    /// Arrow by the service, so floats are accepted there while column-input
    /// literals keep the Slice 1 domain.
    #[test]
    fn application_initialization_accepts_floats() {
        let application = FunctionApplication::from_json(
            r#"{"function":{"name":"scale","object_id":"o","location":"l","version":"1","manifest_digest":"sha256:x"},"inputs":[{"parameter":"x","kind":"column","value":{"path":"x"}}],"output":{"kind":"scalar","arrow_type":"float64","nullable":false},"initialization":{"factor":2.5,"label":"fast"}}"#,
        )
        .unwrap();
        assert!(!application.has_unknown_fields());
        assert_eq!(
            application.initialization()["factor"],
            serde_json::json!(2.5)
        );
        let encoded = application.to_canonical_json().unwrap();
        assert!(encoded.contains(r#""initialization":{"factor":2.5,"label":"fast"}"#));
    }

    #[test]
    fn binding_initialization_round_trips() {
        let binding = FunctionBinding::from_json(
            r#"{"binding_id":"fb_1","function":{"name":"scale","object_id":"o","location":"l","version":"1","manifest_digest":"sha256:x"},"inputs":[],"outputs":[],"initialization":"QUJD"}"#,
        )
        .unwrap();
        assert_eq!(binding.initialization(), Some("QUJD"));
        assert!(
            binding
                .to_canonical_json()
                .unwrap()
                .ends_with(r#""initialization":"QUJD","inputs":[],"outputs":[]}"#)
        );
    }

    /// Unknown keys are how a newer server's payload reaches an older client,
    /// so the check has to be exact about which level it is looking at.
    #[test]
    fn has_unknown_keys_only_inspects_the_level_it_is_given() {
        let value = serde_json::json!({"name": "embed", "version": "fv_1"});
        assert!(!has_unknown_keys(&value, &["name", "version"]));
        assert!(has_unknown_keys(&value, &["name"]));

        // A nested unknown is not this level's business.
        let nested = serde_json::json!({"name": {"unexpected": 1}});
        assert!(!has_unknown_keys(&nested, &["name"]));

        // A non-object has no keys to be unknown.
        assert!(!has_unknown_keys(&serde_json::json!("embed"), &["name"]));
    }
}
