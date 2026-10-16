// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Named Secrets: database-scoped credentials a Function binds by name.
//!
//! Nothing here holds a credential. The verbs live on
//! [`crate::connection::Connection`], and none of them returns a value -- by
//! construction rather than by policy, so there is no code path that could.
//!
//! What a Secret is, how one is named, and how a Function binds one all live
//! here; [`crate::function`] holds the bindings a FunctionVersion records, the
//! way Sophon's Secret catalog and Function catalog divide the same two.

use serde::de;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

/// What a database records about a Secret. Never its value.
///
/// Returned by [`crate::connection::Connection::describe_secret`]. There is no
/// field for the credential and no method that could produce one.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct SecretInfo {
    /// The Secret's database-scoped name.
    pub name: String,
    /// When the Secret was created, in milliseconds since the Unix epoch.
    pub created_at_millis: i64,
    /// When the Secret's value was last rotated, in milliseconds since the Unix
    /// epoch.
    ///
    /// This is the only observable that a rotation landed: no API returns a
    /// credential, so a caller confirms `alter_secret` took effect by watching
    /// this move.
    pub updated_at_millis: i64,
}

/// Where a Secret lives, carried as its parts rather than as one string.
///
/// Nothing here is parsed, so nothing can parse two ways. A joined id would
/// instead need a delimiter excluded from every name and segment, agreed on by
/// both sides.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SecretReference {
    pub name: String,
    /// The namespace holding the Secret. Empty is the root, and is absent from
    /// the wire rather than sent empty: a binding states a namespace only when
    /// it has one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub namespace_path: Vec<String>,
}

impl SecretReference {
    /// A Secret in the root namespace.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            namespace_path: Vec::new(),
        }
    }

    /// A Secret in `namespace_path`.
    pub fn in_namespace(name: impl Into<String>, namespace_path: Vec<String>) -> Self {
        Self {
            name: name.into(),
            namespace_path,
        }
    }
}

/// How a Secret reaches the Function that binds it.
///
/// One list rather than a field per delivery mode, so a mode added later is a
/// variant and the per-Function rules -- how many Secrets a Function may bind,
/// which ones it needs -- stay answerable from one place.
///
/// A mode this client does not know decodes rather than failing the whole
/// FunctionVersion, as [`PythonRuntimeSpec`] does for an unknown runtime. That
/// takes both halves: [`SecretBinding::Unrecognized`] gives the wire somewhere
/// to land, and `#[non_exhaustive]` denies callers an exhaustive match, so a
/// later mode arrives as a case they already had to handle. Its payload is
/// dropped -- the client does not proxy catalog values.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum SecretBinding {
    /// Delivered as an environment variable, which the UDF's library already
    /// reads. The variable is the delivery target; the Secret is what fills it.
    Env {
        variable: String,
        /// Named `secret_ref` rather than `secret` because a Job payload is
        /// scanned server-side for credential-shaped keys, and a key called
        /// `secret` trips that guard whatever it actually holds.
        secret_ref: SecretReference,
    },
    /// A binding kind introduced by a newer server.
    Unrecognized { kind: String },
}

impl SecretBinding {
    /// The wire discriminator reported by Sophon.
    pub fn kind(&self) -> &str {
        match self {
            Self::Env { .. } => "env",
            Self::Unrecognized { kind } => kind,
        }
    }

    /// The environment variable this binding fills, or `None` for a kind that
    /// does not deliver through one.
    pub fn variable(&self) -> Option<&str> {
        match self {
            Self::Env { variable, .. } => Some(variable),
            Self::Unrecognized { .. } => None,
        }
    }

    /// The Secret bound, or `None` for a kind this client cannot read.
    pub fn secret(&self) -> Option<&SecretReference> {
        match self {
            Self::Env { secret_ref, .. } => Some(secret_ref),
            Self::Unrecognized { .. } => None,
        }
    }
}

#[derive(Deserialize)]
struct EnvSecretBindingWire {
    variable: String,
    secret_ref: SecretReference,
}

impl<'de> Deserialize<'de> for SecretBinding {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        let kind = value
            .get("kind")
            .ok_or_else(|| de::Error::missing_field("kind"))?
            .as_str()
            .ok_or_else(|| de::Error::custom("secret binding kind must be a string"))?
            .to_string();
        match kind.as_str() {
            "env" => {
                let wire: EnvSecretBindingWire =
                    serde_json::from_value(value).map_err(de::Error::custom)?;
                Ok(Self::Env {
                    variable: wire.variable,
                    secret_ref: wire.secret_ref,
                })
            }
            _ => Ok(Self::Unrecognized { kind }),
        }
    }
}

impl Serialize for SecretBinding {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct EnvBindingRef<'a> {
            kind: &'static str,
            variable: &'a str,
            secret_ref: &'a SecretReference,
        }

        #[derive(Serialize)]
        struct UnrecognizedBindingRef<'a> {
            kind: &'a str,
        }

        match self {
            Self::Env {
                variable,
                secret_ref,
            } => EnvBindingRef {
                kind: "env",
                variable,
                secret_ref,
            }
            .serialize(serializer),
            Self::Unrecognized { kind } => UnrecognizedBindingRef { kind }.serialize(serializer),
        }
    }
}
