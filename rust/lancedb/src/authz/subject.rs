// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Identity selectors. Provider names resolve on the server; IDs remain literal.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::{Error, Result};

/// The kind of identifier supplied to an authorization operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SubjectKind {
    PrincipalId,
    PrincipalName,
    GroupId,
    GroupName,
    Role,
    PrincipalApiKey,
    /// An unrecognized canonical subject. Its full wire string is stored as the value.
    Unknown,
}

impl SubjectKind {
    /// Stable name used by language bindings.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PrincipalId => "principal_id",
            Self::PrincipalName => "principal_name",
            Self::GroupId => "group_id",
            Self::GroupName => "group_name",
            Self::Role => "role",
            Self::PrincipalApiKey => "principal_api_key",
            Self::Unknown => "unknown",
        }
    }

    fn prefix(self) -> &'static str {
        match self {
            Self::PrincipalId => "p:",
            Self::PrincipalName => "P:",
            Self::GroupId => "g:",
            Self::GroupName => "G:",
            Self::Role => "r:",
            Self::PrincipalApiKey => "a:",
            Self::Unknown => "",
        }
    }
}

const KINDS: [SubjectKind; 6] = [
    SubjectKind::PrincipalId,
    SubjectKind::PrincipalName,
    SubjectKind::GroupId,
    SubjectKind::GroupName,
    SubjectKind::Role,
    SubjectKind::PrincipalApiKey,
];

impl FromStr for SubjectKind {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        if value == "unknown" {
            return Ok(Self::Unknown);
        }
        KINDS
            .into_iter()
            .find(|kind| kind.as_str() == value)
            .ok_or_else(|| invalid("Invalid subject kind"))
    }
}

/// An immutable validated principal, group, or role selector.
///
/// Unrecognized canonical prefixes are preserved as [`SubjectKind::Unknown`],
/// with the complete wire string in [`Self::value`], so future subject kinds can
/// be listed and sent back to the server.
///
/// ```
/// use lancedb::authz::{Subject, SubjectKind};
/// let subject = Subject::from_canonical("x:future-subject")?;
/// assert_eq!(subject.kind(), SubjectKind::Unknown);
/// assert_eq!(subject.to_wire(), "x:future-subject");
/// # Ok::<(), lancedb::Error>(())
/// ```
///
/// Debug and Display redact API keys. [`Self::to_wire`] and serialization
/// intentionally expose the key for transport and must not be logged.
///
/// ```
/// use lancedb::authz::Subject;
/// let subject = Subject::group_name("Engineering")?;
/// assert_eq!(subject.to_wire(), "G:Engineering");
/// assert_eq!(subject, "G:Engineering".parse::<Subject>()?);
/// # Ok::<(), lancedb::Error>(())
/// ```
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Subject {
    kind: SubjectKind,
    value: String,
}

fn invalid(message: &str) -> Error {
    // Never include the selector: it may contain an API key.
    Error::InvalidInput {
        message: message.into(),
    }
}

impl Subject {
    /// Construct a selector without resolving it against an identity provider.
    pub fn new(kind: SubjectKind, value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(invalid("Subject value must be a nonempty string"));
        }
        if kind == SubjectKind::Unknown
            && (!value
                .split_once(':')
                .is_some_and(|(prefix, value)| !prefix.is_empty() && !value.is_empty())
                || KINDS.iter().any(|kind| value.starts_with(kind.prefix())))
        {
            return Err(invalid("Invalid unknown subject selector"));
        }
        Ok(Self { kind, value })
    }

    /// Select a principal by canonical ID, without stripping any prefix in the ID.
    pub fn principal_id(id: impl Into<String>) -> Result<Self> {
        Self::new(SubjectKind::PrincipalId, id)
    }
    /// Select a principal by an unambiguous provider name.
    pub fn principal_name(name: impl Into<String>) -> Result<Self> {
        Self::new(SubjectKind::PrincipalName, name)
    }
    /// Select a group by canonical ID.
    pub fn group_id(id: impl Into<String>) -> Result<Self> {
        Self::new(SubjectKind::GroupId, id)
    }
    /// Select a group by an unambiguous provider name.
    pub fn group_name(name: impl Into<String>) -> Result<Self> {
        Self::new(SubjectKind::GroupName, name)
    }
    /// Select a LanceDB-managed role.
    pub fn role(name: impl Into<String>) -> Result<Self> {
        Self::new(SubjectKind::Role, name)
    }
    /// Select the principal holding this secret API key, not the request's caller.
    pub fn principal_api_key(key: impl Into<String>) -> Result<Self> {
        Self::new(SubjectKind::PrincipalApiKey, key)
    }

    /// The selector kind.
    pub fn kind(&self) -> SubjectKind {
        self.kind
    }
    /// The selected ID, name, role, or API key. API-key values are secrets.
    /// For unknown kinds, this is the full canonical wire string.
    pub fn value(&self) -> &str {
        &self.value
    }
    /// The value safe for display; API keys are replaced with `<redacted>`.
    pub fn display_value(&self) -> &str {
        if self.kind == SubjectKind::PrincipalApiKey {
            "<redacted>"
        } else {
            &self.value
        }
    }
    /// Encode the literal wire selector. The result can contain a secret API key.
    pub fn to_wire(&self) -> String {
        format!("{}{}", self.kind.prefix(), self.value)
    }

    /// Parse a returned canonical identity, rejecting names and secret selectors.
    pub fn from_canonical(value: &str) -> Result<Self> {
        let subject: Self = value.parse()?;
        if !matches!(
            subject.kind,
            SubjectKind::PrincipalId
                | SubjectKind::GroupId
                | SubjectKind::Role
                | SubjectKind::Unknown
        ) {
            return Err(invalid("Server returned a non-canonical subject"));
        }
        Ok(subject)
    }

    /// Deserialize a canonical response subject, rejecting secrets and name selectors.
    pub fn deserialize_canonical<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Self::from_canonical(&String::deserialize(deserializer)?).map_err(D::Error::custom)
    }

    /// Require a principal selector for a principal lookup.
    pub fn validate_principal(&self) -> Result<()> {
        if matches!(
            self.kind,
            SubjectKind::PrincipalId | SubjectKind::PrincipalName | SubjectKind::PrincipalApiKey
        ) {
            Ok(())
        } else {
            Err(invalid("Subject kind is not valid for a principal lookup"))
        }
    }
    /// Require a group selector for a group lookup.
    pub fn validate_group(&self) -> Result<()> {
        if matches!(self.kind, SubjectKind::GroupId | SubjectKind::GroupName) {
            Ok(())
        } else {
            Err(invalid("Subject kind is not valid for a group lookup"))
        }
    }
    /// Require a principal/group selector. Roles cannot contain other roles.
    pub fn validate_role_member(&self) -> Result<()> {
        if self.kind == SubjectKind::Role {
            Err(invalid("Roles cannot be members of other roles"))
        } else {
            Ok(())
        }
    }
}

impl FromStr for Subject {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self> {
        for kind in KINDS {
            if let Some(value) = value.strip_prefix(kind.prefix()) {
                return Self::new(kind, value);
            }
        }
        Self::new(SubjectKind::Unknown, value)
    }
}

impl fmt::Debug for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subject")
            .field("kind", &self.kind)
            .field("value", &self.display_value())
            .finish()
    }
}
impl fmt::Display for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.kind.prefix(), self.display_value())
    }
}
impl Serialize for Subject {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_wire())
    }
}
impl<'de> Deserialize<'de> for Subject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selectors_roundtrip_without_normalizing_values() {
        for kind in KINDS {
            for value in [
                "alice@example.com",
                "p:literal-id",
                "Engineering / R&D",
                "名前",
            ] {
                let subject = Subject::new(kind, value).unwrap();
                assert_eq!(subject.to_wire().parse::<Subject>().unwrap(), subject);
                let json = serde_json::to_string(&subject).unwrap();
                assert_eq!(serde_json::from_str::<Subject>(&json).unwrap(), subject);
                assert_eq!(subject.value(), value);
                assert!(Subject::new(kind, "").is_err());
            }
        }
    }

    #[test]
    fn contexts_and_canonical_ids() {
        for kind in KINDS {
            let subject = Subject::new(kind, "value").unwrap();
            assert_eq!(
                subject.validate_principal().is_ok(),
                matches!(
                    kind,
                    SubjectKind::PrincipalId
                        | SubjectKind::PrincipalName
                        | SubjectKind::PrincipalApiKey
                )
            );
            assert_eq!(
                subject.validate_group().is_ok(),
                matches!(kind, SubjectKind::GroupId | SubjectKind::GroupName)
            );
            assert_eq!(
                subject.validate_role_member().is_ok(),
                kind != SubjectKind::Role
            );
            assert_eq!(
                Subject::from_canonical(&subject.to_wire()).is_ok(),
                matches!(
                    kind,
                    SubjectKind::PrincipalId | SubjectKind::GroupId | SubjectKind::Role
                )
            );
        }
    }

    #[test]
    fn unknown_subjects_preserve_canonical_wire_strings() {
        for wire in ["x:future-subject", "future:literal:identifier"] {
            let subject = Subject::from_canonical(wire).unwrap();
            assert_eq!(subject.kind(), SubjectKind::Unknown);
            assert_eq!(subject.value(), wire);
            assert_eq!(subject.to_wire(), wire);
            assert_eq!(serde_json::to_value(&subject).unwrap(), wire);
            assert_eq!(
                serde_json::from_value::<Subject>(serde_json::json!(wire)).unwrap(),
                subject
            );
            assert!(subject.validate_principal().is_err());
            assert!(subject.validate_group().is_err());
        }
        for wire in [
            "",
            "no-prefix",
            ":value",
            "x:",
            "p:value",
            "P:value",
            "g:value",
            "G:value",
            "r:value",
            "a:secret",
        ] {
            assert!(Subject::new(SubjectKind::Unknown, wire).is_err());
        }
    }

    #[test]
    fn acl_page_preserves_known_and_unknown_subjects() {
        let payload = serde_json::json!({"entries": [
            {"subject_id": "p:known", "object": "system", "privilege": "USAGE"},
            {"subject_id": "x:future-subject", "object": "system", "privilege": "USAGE"}
        ]});
        let page: crate::authz::ListAccessControlEntriesResponse =
            serde_json::from_value(payload.clone()).unwrap();
        assert_eq!(page.entries.len(), 2);
        assert_eq!(page.entries[0].subject_id.kind(), SubjectKind::PrincipalId);
        assert_eq!(page.entries[1].subject_id.kind(), SubjectKind::Unknown);
        assert_eq!(serde_json::to_value(page).unwrap(), payload);
    }

    #[test]
    fn secrets_are_only_exposed_by_explicit_serialization() {
        let secret = "never-log-this-key";
        let subject = Subject::principal_api_key(secret).unwrap();
        assert!(!format!("{subject:?} {subject}").contains(secret));
        assert!(serde_json::to_string(&subject).unwrap().contains(secret));
        let error = Subject::from_canonical(&subject.to_wire()).unwrap_err();
        assert!(!error.to_string().contains(secret));
        assert!(!format!("{error:?}").contains(secret));
        let error = secret.parse::<Subject>().unwrap_err();
        assert!(!error.to_string().contains(secret));
    }
}
