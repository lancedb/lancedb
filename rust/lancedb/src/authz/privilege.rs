// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

/// A LanceDB privilege, serialized by name (for example, `SELECT`).
/// Future privilege names use `PRIVILEGE` followed by decimal digits. They are
/// preserved verbatim; their meaning and validity are determined by the server.
///
/// ```
/// use lancedb::authz::Privilege;
///
/// let privilege: Privilege = "PRIVILEGE256".parse().unwrap();
/// assert_eq!(privilege.to_string(), "PRIVILEGE256");
///
/// // Delegate creation on a namespace without transferring its ownership.
/// for privilege in [Privilege::CreateSecret, Privilege::CreateFunction] {
///     assert!(privilege.valid_for_namespace());
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, strum_macros::Display)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum Privilege {
    /// A special privilege that conveys ownership.
    Ownership,

    /// On a database: allows accessing the namespaces inside.
    /// On a namespace: allows accessing the tables inside.
    ///
    /// When looking for Usage on a Database, if there are no ACLs
    /// found for that database, we assume it doesn't exist yet and
    /// check for CreateDatabase on System.
    ///
    /// When looking for Usage on a public (aka default) Namespace,
    /// if there are no ACLs found for that Namespace, we assume it
    /// doesn't exist yet and check for CreateNamespace on the
    /// containing database instead.
    Usage,

    /// On a table: allows reading rows.
    Select,

    /// On a table: allows appending rows.
    Insert,

    /// On a table: allows modifying existing rows.
    Update,

    /// On a table: allows deleting rows.
    Delete,

    /// On the system object: can create a database.
    CreateDatabase,

    /// On a database or namespace: can create immediate child namespaces.
    CreateNamespace,

    /// On a namespace: can create a table in that namespace.
    CreateTable,

    /// On a namespace: can create a materialized view in that namespace.
    CreateMaterializedView,

    /// On a namespace: can create a view in that namespace.
    CreateView,

    /// On a namespace: can create a secret without taking ownership of the namespace.
    CreateSecret,

    /// On a namespace: can create a function without taking ownership of the namespace.
    CreateFunction,

    /// On the system object:
    /// - can read server configuration.
    Operate,

    /// A future privilege name, preserved verbatim.
    #[strum(to_string = "{0}")]
    Unknown(UnknownPrivilege),
}

/// An unknown privilege.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownPrivilege(String);

impl fmt::Display for UnknownPrivilege {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Privilege {
    pub const KNOWN: [Self; 14] = [
        Self::Ownership,
        Self::Usage,
        Self::Select,
        Self::Insert,
        Self::Update,
        Self::Delete,
        Self::CreateDatabase,
        Self::CreateNamespace,
        Self::CreateTable,
        Self::CreateMaterializedView,
        Self::CreateView,
        Self::CreateSecret,
        Self::CreateFunction,
        Self::Operate,
    ];

    pub fn valid_for_system(&self) -> bool {
        matches!(
            self,
            Self::Operate | Self::CreateDatabase | Self::Usage | Self::Unknown(_)
        )
    }

    pub fn valid_for_database(&self) -> bool {
        matches!(
            self,
            Self::Ownership | Self::Usage | Self::CreateNamespace | Self::Unknown(_)
        )
    }

    pub fn valid_for_namespace(&self) -> bool {
        matches!(
            self,
            Self::Ownership
                | Self::Usage
                | Self::CreateNamespace
                | Self::CreateTable
                | Self::CreateMaterializedView
                | Self::CreateView
                | Self::CreateSecret
                | Self::CreateFunction
                | Self::Unknown(_)
        )
    }

    pub fn valid_for_table(&self) -> bool {
        matches!(
            self,
            Self::Ownership
                | Self::Select
                | Self::Insert
                | Self::Update
                | Self::Delete
                | Self::Unknown(_)
        )
    }

    pub fn valid_for_view(&self) -> bool {
        matches!(self, Self::Ownership | Self::Select | Self::Unknown(_))
    }

    /// Whether this privilege can mutate database contents or schema.
    /// Unknown privileges are conservatively treated as writes.
    pub fn is_write(&self) -> bool {
        !matches!(self, Self::Usage | Self::Select | Self::Operate)
    }
}

impl FromStr for Privilege {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        if let Some(privilege) = Self::KNOWN.into_iter().find(|p| p.to_string() == name) {
            return Ok(privilege);
        }
        if let Some(digits) = name.strip_prefix("PRIVILEGE")
            && !digits.is_empty()
            && digits.bytes().all(|b| b.is_ascii_digit())
        {
            return Ok(Self::Unknown(UnknownPrivilege(name.to_owned())));
        }
        Err(format!("invalid privilege {name:?}"))
    }
}

impl Serialize for Privilege {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Privilege {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::Privilege;

    #[test]
    fn display_matches_serde() {
        for privilege in Privilege::KNOWN {
            assert_eq!(
                serde_json::to_string(&privilege).unwrap(),
                format!("\"{privilege}\""),
            );
        }
    }

    #[test]
    fn secret_and_function_creation_privileges() {
        for (name, privilege) in [
            ("CREATE_SECRET", Privilege::CreateSecret),
            ("CREATE_FUNCTION", Privilege::CreateFunction),
        ] {
            assert_eq!(name.parse::<Privilege>().unwrap(), privilege);
            assert_eq!(privilege.to_string(), name);
            assert_eq!(serde_json::to_value(&privilege).unwrap(), name);
            assert_eq!(
                serde_json::from_value::<Privilege>(serde_json::json!(name)).unwrap(),
                privilege
            );
            assert!(privilege.valid_for_namespace());
            assert!(!privilege.valid_for_system());
            assert!(!privilege.valid_for_database());
            assert!(!privilege.valid_for_table());
            assert!(!privilege.valid_for_view());
            assert!(privilege.is_write());
        }
    }

    #[test]
    fn future_privilege_names_round_trip() {
        for name in [
            "PRIVILEGE0",
            "PRIVILEGE2",
            "PRIVILEGE20",
            "PRIVILEGE64",
            "PRIVILEGE256",
            "PRIVILEGE18446744073709551616",
            "PRIVILEGE002",
        ] {
            let privilege: Privilege = name.parse().unwrap();
            assert!(matches!(privilege, Privilege::Unknown(_)));
            assert_eq!(privilege.to_string(), name);
            assert_eq!(serde_json::to_value(&privilege).unwrap(), name);
            assert_eq!(
                serde_json::from_value::<Privilege>(serde_json::json!(name)).unwrap(),
                privilege
            );
        }
        assert_ne!(
            "PRIVILEGE2".parse::<Privilege>().unwrap(),
            Privilege::Select
        );
    }

    #[test]
    fn rejects_invalid_privileges() {
        for name in [
            "",
            "select",
            " SELECT",
            "SELECT ",
            "PRIVILEGE",
            "PRIVILEGE-1",
            "PRIVILEGE+1",
            "PRIVILEGE 1",
            "PRIVILEGE1x",
            "privilege12",
            "UNKNOWN",
        ] {
            assert!(name.parse::<Privilege>().is_err(), "{name}");
            assert!(
                serde_json::from_value::<Privilege>(serde_json::json!(name)).is_err(),
                "{name}"
            );
        }
    }

    #[test]
    fn unknown_privileges_are_valid_for_all_objects_and_treated_as_writes() {
        for name in [
            "PRIVILEGE0",
            "PRIVILEGE2",
            "PRIVILEGE20",
            "PRIVILEGE64",
            "PRIVILEGE256",
        ] {
            let privilege: Privilege = name.parse().unwrap();
            assert!(privilege.valid_for_system());
            assert!(privilege.valid_for_database());
            assert!(privilege.valid_for_namespace());
            assert!(privilege.valid_for_table());
            assert!(privilege.valid_for_view());
            assert!(privilege.is_write());
        }
        assert!(!Privilege::Ownership.valid_for_system());
        assert!(!Privilege::Select.valid_for_database());
    }

    #[test]
    fn test_privilege_is_write() {
        for privilege in Privilege::KNOWN {
            assert_eq!(
                privilege.is_write(),
                matches!(
                    privilege,
                    Privilege::Ownership
                        | Privilege::Insert
                        | Privilege::Update
                        | Privilege::Delete
                        | Privilege::CreateDatabase
                        | Privilege::CreateNamespace
                        | Privilege::CreateTable
                        | Privilege::CreateMaterializedView
                        | Privilege::CreateView
                        | Privilege::CreateSecret
                        | Privilege::CreateFunction
                ),
                "unexpected write classification for {privilege}"
            );
        }
    }
}
