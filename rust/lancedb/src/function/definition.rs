// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Immutable FunctionDefinition registration input (B1c / FF-007).
//!
//! These types are authoring/transport values only. They do not mint identity,
//! store digests/artifacts, or execute Python.

use std::collections::HashSet;
use std::fmt;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{FunctionSignature, SignatureWire, invalid_input};
use crate::Result;

const FORMAT_VERSION_V1: u32 = 1;

/// Immutable Python implementation description for a [`FunctionDefinition`].
///
/// The source body is carried on the trusted registration wire but is omitted
/// from [`Debug`] output.
#[derive(Clone, PartialEq, Eq)]
pub struct PythonFunctionDefinition {
    module: String,
    callable: String,
    source: String,
    python: String,
    packages: Vec<String>,
}

impl PythonFunctionDefinition {
    /// Create a Python implementation description.
    ///
    /// Rejects empty `module`, `callable`, `source`, `python`, or any empty
    /// package requirement, and rejects duplicate package requirement strings.
    pub fn try_new(
        module: impl Into<String>,
        callable: impl Into<String>,
        source: impl Into<String>,
        python: impl Into<String>,
        packages: Vec<String>,
    ) -> Result<Self> {
        let module = module.into();
        let callable = callable.into();
        let source = source.into();
        let python = python.into();
        validate_python_fields(&module, &callable, &source, &python, &packages)?;
        Ok(Self {
            module,
            callable,
            source,
            python,
            packages,
        })
    }

    /// Python module name.
    pub fn module(&self) -> &str {
        &self.module
    }

    /// Callable name within the module.
    pub fn callable(&self) -> &str {
        &self.callable
    }

    /// Source body submitted at the trusted registration boundary.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Requested Python runtime version string.
    pub fn python(&self) -> &str {
        &self.python
    }

    /// Ordered package requirements.
    pub fn packages(&self) -> &[String] {
        &self.packages
    }
}

impl fmt::Debug for PythonFunctionDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PythonFunctionDefinition")
            .field("module", &self.module)
            .field("callable", &self.callable)
            .field("source", &"<redacted>")
            .field("python", &self.python)
            .field("packages", &self.packages)
            .finish()
    }
}

fn validate_python_fields(
    module: &str,
    callable: &str,
    source: &str,
    python: &str,
    packages: &[String],
) -> Result<()> {
    if module.is_empty() {
        return Err(invalid_input(
            "PythonFunctionDefinition module must be non-empty",
        ));
    }
    if callable.is_empty() {
        return Err(invalid_input(
            "PythonFunctionDefinition callable must be non-empty",
        ));
    }
    if source.is_empty() {
        return Err(invalid_input(
            "PythonFunctionDefinition source must be non-empty",
        ));
    }
    if python.is_empty() {
        return Err(invalid_input(
            "PythonFunctionDefinition python must be non-empty",
        ));
    }
    let mut seen = HashSet::with_capacity(packages.len());
    for package in packages {
        if package.is_empty() {
            return Err(invalid_input(
                "PythonFunctionDefinition package must be non-empty",
            ));
        }
        if !seen.insert(package.as_str()) {
            return Err(invalid_input(
                "PythonFunctionDefinition packages must not contain duplicates",
            ));
        }
    }
    Ok(())
}

/// Explicit capability grant attached to a [`FunctionDefinition`].
///
/// Secret references are carried on the trusted registration wire but are
/// omitted from [`Debug`] output. Plaintext secret values are never part of
/// this type.
#[derive(Clone, PartialEq, Eq)]
pub struct FunctionCapability {
    kind: FunctionCapabilityKind,
}

#[derive(Clone, PartialEq, Eq)]
enum FunctionCapabilityKind {
    Network {
        origin: String,
    },
    Secret {
        reference: String,
        environment_variable: String,
    },
}

impl FunctionCapability {
    /// Create a network capability for a non-empty origin.
    pub fn try_network(origin: impl Into<String>) -> Result<Self> {
        let origin = origin.into();
        if origin.is_empty() {
            return Err(invalid_input(
                "FunctionCapability network origin must be non-empty",
            ));
        }
        Ok(Self {
            kind: FunctionCapabilityKind::Network { origin },
        })
    }

    /// Create a secret capability for a non-empty reference and environment variable.
    ///
    /// Errors name the fields and never echo the reference value.
    pub fn try_secret(
        reference: impl Into<String>,
        environment_variable: impl Into<String>,
    ) -> Result<Self> {
        let reference = reference.into();
        let environment_variable = environment_variable.into();
        if reference.is_empty() {
            return Err(invalid_input(
                "FunctionCapability secret reference must be non-empty",
            ));
        }
        if environment_variable.is_empty() {
            return Err(invalid_input(
                "FunctionCapability secret environment_variable must be non-empty",
            ));
        }
        Ok(Self {
            kind: FunctionCapabilityKind::Secret {
                reference,
                environment_variable,
            },
        })
    }

    /// Network origin when this capability is a network grant.
    pub fn origin(&self) -> Option<&str> {
        match &self.kind {
            FunctionCapabilityKind::Network { origin } => Some(origin.as_str()),
            FunctionCapabilityKind::Secret { .. } => None,
        }
    }

    /// Secret reference when this capability is a secret grant.
    pub fn reference(&self) -> Option<&str> {
        match &self.kind {
            FunctionCapabilityKind::Secret { reference, .. } => Some(reference.as_str()),
            FunctionCapabilityKind::Network { .. } => None,
        }
    }

    /// Environment variable name when this capability is a secret grant.
    pub fn environment_variable(&self) -> Option<&str> {
        match &self.kind {
            FunctionCapabilityKind::Secret {
                environment_variable,
                ..
            } => Some(environment_variable.as_str()),
            FunctionCapabilityKind::Network { .. } => None,
        }
    }

    fn to_wire(&self) -> CapabilityWire {
        match &self.kind {
            FunctionCapabilityKind::Network { origin } => CapabilityWire::Network {
                origin: origin.clone(),
            },
            FunctionCapabilityKind::Secret {
                reference,
                environment_variable,
            } => CapabilityWire::Secret {
                reference: reference.clone(),
                environment_variable: environment_variable.clone(),
            },
        }
    }

    fn from_wire(wire: CapabilityWire) -> Result<Self> {
        match wire {
            CapabilityWire::Network { origin } => Self::try_network(origin),
            CapabilityWire::Secret {
                reference,
                environment_variable,
            } => Self::try_secret(reference, environment_variable),
        }
    }
}

impl fmt::Debug for FunctionCapability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            FunctionCapabilityKind::Network { origin } => f
                .debug_struct("FunctionCapability")
                .field("kind", &"network")
                .field("origin", origin)
                .finish(),
            FunctionCapabilityKind::Secret {
                environment_variable,
                ..
            } => f
                .debug_struct("FunctionCapability")
                .field("kind", &"secret")
                .field("reference", &"<redacted>")
                .field("environment_variable", environment_variable)
                .finish(),
        }
    }
}

/// Immutable registration input for a first-class Function (format version 1).
///
/// This value has no catalog identity. Source bodies and secret references are
/// present on the trusted serde wire but omitted from [`Debug`].
#[derive(Clone, PartialEq, Eq)]
pub struct FunctionDefinition {
    signature: FunctionSignature,
    python_definition: PythonFunctionDefinition,
    capabilities: Vec<FunctionCapability>,
}

impl FunctionDefinition {
    /// Create a definition from a signature, Python implementation, and capabilities.
    ///
    /// Emptiness and package uniqueness are enforced by the child constructors.
    pub fn try_new(
        signature: FunctionSignature,
        python_definition: PythonFunctionDefinition,
        capabilities: Vec<FunctionCapability>,
    ) -> Result<Self> {
        Ok(Self {
            signature,
            python_definition,
            capabilities,
        })
    }

    /// Function signature.
    pub fn signature(&self) -> &FunctionSignature {
        &self.signature
    }

    /// Python implementation description.
    pub fn python_definition(&self) -> &PythonFunctionDefinition {
        &self.python_definition
    }

    /// Ordered capability grants.
    pub fn capabilities(&self) -> &[FunctionCapability] {
        &self.capabilities
    }

    fn to_wire(&self) -> Result<FunctionDefinitionWire> {
        Ok(FunctionDefinitionWire {
            format_version: FORMAT_VERSION_V1,
            signature: self.signature.to_wire()?,
            implementation: ImplementationWire::Python {
                module: self.python_definition.module.clone(),
                callable: self.python_definition.callable.clone(),
                source: self.python_definition.source.clone(),
                python: self.python_definition.python.clone(),
                packages: self.python_definition.packages.clone(),
            },
            capabilities: self
                .capabilities
                .iter()
                .map(FunctionCapability::to_wire)
                .collect(),
        })
    }

    fn from_wire(wire: FunctionDefinitionWire) -> Result<Self> {
        if wire.format_version != FORMAT_VERSION_V1 {
            return Err(invalid_input(format!(
                "unsupported FunctionDefinition format_version {}",
                wire.format_version
            )));
        }
        let signature = FunctionSignature::from_wire(wire.signature)?;
        let python_definition = match wire.implementation {
            ImplementationWire::Python {
                module,
                callable,
                source,
                python,
                packages,
            } => PythonFunctionDefinition::try_new(module, callable, source, python, packages)?,
        };
        let capabilities = wire
            .capabilities
            .into_iter()
            .map(FunctionCapability::from_wire)
            .collect::<Result<Vec<_>>>()?;
        Self::try_new(signature, python_definition, capabilities)
    }
}

impl fmt::Debug for FunctionDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FunctionDefinition")
            .field("signature", &self.signature)
            .field("python_definition", &self.python_definition)
            .field("capabilities", &self.capabilities)
            .finish()
    }
}

// Do not derive Debug: wire payloads carry Python source and secret references.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FunctionDefinitionWire {
    format_version: u32,
    signature: SignatureWire,
    implementation: ImplementationWire,
    capabilities: Vec<CapabilityWire>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum ImplementationWire {
    #[serde(rename = "python")]
    Python {
        module: String,
        callable: String,
        source: String,
        python: String,
        packages: Vec<String>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum CapabilityWire {
    #[serde(rename = "network")]
    Network { origin: String },
    #[serde(rename = "secret")]
    Secret {
        reference: String,
        environment_variable: String,
    },
}

impl Serialize for FunctionDefinition {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_wire()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for FunctionDefinition {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FunctionDefinitionWire::deserialize(deserializer)?;
        Self::from_wire(wire).map_err(D::Error::custom)
    }
}
