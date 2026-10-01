// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use crate::utils::{
    validate_database_name, validate_namespace_component, validate_secret_name, validate_table_name,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _, ser::Error as _};

/// The default namespace for tables, views, secrets, and functions.
pub const DEFAULT_NAMESPACE: &str = "public";

pub const SYSTEM_TYPE_STRING: &str = "system";
pub const DATABASE_TYPE_STRING: &str = "database";
pub const NAMESPACE_TYPE_STRING: &str = "namespace";
pub const TABLE_TYPE_STRING: &str = "table";
pub const VIEW_TYPE_STRING: &str = "view";
pub const SECRET_TYPE_STRING: &str = "secret";
pub const FUNCTION_TYPE_STRING: &str = "function";
pub const UNKNOWN_TYPE_STRING: &str = "unknown";

/// A non-empty path through the namespace hierarchy.
///
/// The `$` separator is only the canonical wire encoding. Authorization code
/// should use the components and prefix helpers instead of interpreting the
/// encoded form.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct NamespacePath(Vec<String>);

impl NamespacePath {
    /// Construct a path, validating each component without splitting separators.
    pub fn new(components: Vec<String>) -> Result<Self, String> {
        if components.is_empty() {
            return Err("namespace paths must contain at least one component".to_string());
        }
        for component in &components {
            validate_namespace_component(component).map_err(|error| error.to_string())?;
        }
        Ok(Self(components))
    }

    /// Construct a single-component path.
    ///
    /// # Panics
    /// Panics if the component is invalid. Use [`Self::new`] for untrusted input.
    pub fn from_component(component: impl Into<String>) -> Self {
        Self::new(vec![component.into()]).expect("namespace components must be valid")
    }

    pub fn components(&self) -> &[String] {
        &self.0
    }

    pub fn parent(&self) -> Option<Self> {
        (self.0.len() > 1).then(|| Self(self.0[..self.0.len() - 1].to_vec()))
    }

    pub fn prefixes(&self) -> impl Iterator<Item = Self> + '_ {
        (1..=self.0.len()).map(|length| Self(self.0[..length].to_vec()))
    }

    pub fn is_descendant_of(&self, ancestor: &Self) -> bool {
        self.0.len() > ancestor.0.len() && self.0.starts_with(&ancestor.0)
    }

    pub fn parse(path: &str) -> Result<Self, String> {
        Self::new(path.split('$').map(str::to_string).collect())
    }
}

impl std::fmt::Display for NamespacePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.join("$"))
    }
}

/// An authorization resource, serialized using its canonical wire name.
///
/// Unknown resource types are preserved so clients can read rules created by
/// newer servers. Known resource types validate names during parsing and serialization.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Object {
    System,
    Database(DatabaseObject),
    Namespace(NamespaceObject),
    Table(TableObject),
    View(ViewObject),
    Secret(SecretObject),
    Function(FunctionObject),
    Unknown(String),
}

impl Object {
    /// Validate resource names, including objects recovered from storage keys.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::System => Ok(()),
            Self::Database(object) => object.validate(),
            Self::Namespace(object) => object.validate(),
            Self::Table(object) => object.validate(),
            Self::View(object) => object.validate(),
            Self::Secret(object) => object.validate(),
            Self::Function(object) => object.validate(),
            Self::Unknown(value) if value.is_empty() => Err("object must not be empty".into()),
            Self::Unknown(_) => Ok(()),
        }
    }

    /// The database an object belongs to, or None.
    pub fn database(&self) -> Option<&str> {
        match self {
            Self::System => None,
            Self::Database(object) => Some(&object.database),
            Self::Namespace(object) => Some(&object.database),
            Self::Table(object) => Some(&object.database),
            Self::View(object) => Some(&object.database),
            Self::Secret(object) => Some(&object.database),
            Self::Function(object) => Some(&object.database),
            Self::Unknown(_) => None,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Self::System => SYSTEM_TYPE_STRING,
            Self::Database(_) => DATABASE_TYPE_STRING,
            Self::Namespace(_) => NAMESPACE_TYPE_STRING,
            Self::Table(_) => TABLE_TYPE_STRING,
            Self::View(_) => VIEW_TYPE_STRING,
            Self::Secret(_) => SECRET_TYPE_STRING,
            Self::Function(_) => FUNCTION_TYPE_STRING,
            Self::Unknown(_) => UNKNOWN_TYPE_STRING,
        }
    }
}

impl std::str::FromStr for Object {
    type Err = crate::Error;

    fn from_str(value: &str) -> crate::Result<Self> {
        Self::deserialize(serde::de::value::StrDeserializer::<serde::de::value::Error>::new(value))
            .map_err(|error| crate::Error::InvalidInput {
                message: error.to_string(),
            })
    }
}

impl std::fmt::Display for Object {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::System => f.write_str(SYSTEM_TYPE_STRING),
            Self::Database(object) => object.fmt(f),
            Self::Namespace(object) => object.fmt(f),
            Self::Table(object) => object.fmt(f),
            Self::View(object) => object.fmt(f),
            Self::Secret(object) => object.fmt(f),
            Self::Function(object) => object.fmt(f),
            Self::Unknown(s) => f.write_str(s),
        }
    }
}

impl Serialize for Object {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate().map_err(S::Error::custom)?;
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Object {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let kind = s.split(':').next().unwrap_or_default();
        if s == SYSTEM_TYPE_STRING {
            Ok(Self::System)
        } else if s.is_empty() || kind == SYSTEM_TYPE_STRING {
            Err(D::Error::custom(format!("invalid Object {s:?}")))
        } else if kind == DATABASE_TYPE_STRING {
            let database = DatabaseObject::deserialize(serde::de::value::StringDeserializer::<
                D::Error,
            >::new(s))?;
            Ok(Self::Database(database))
        } else if kind == NAMESPACE_TYPE_STRING {
            let namespace = NamespaceObject::deserialize(serde::de::value::StringDeserializer::<
                D::Error,
            >::new(s))?;
            Ok(Self::Namespace(namespace))
        } else if kind == TABLE_TYPE_STRING {
            let table =
                TableObject::deserialize(serde::de::value::StringDeserializer::<D::Error>::new(s))?;
            Ok(Self::Table(table))
        } else if kind == VIEW_TYPE_STRING {
            let view =
                ViewObject::deserialize(serde::de::value::StringDeserializer::<D::Error>::new(s))?;
            Ok(Self::View(view))
        } else if kind == SECRET_TYPE_STRING {
            let secret = SecretObject::deserialize(
                serde::de::value::StringDeserializer::<D::Error>::new(s),
            )?;
            Ok(Self::Secret(secret))
        } else if kind == FUNCTION_TYPE_STRING {
            let function = FunctionObject::deserialize(serde::de::value::StringDeserializer::<
                D::Error,
            >::new(s))?;
            Ok(Self::Function(function))
        } else {
            Ok(Self::Unknown(s))
        }
    }
}

/// A database resource, encoded as `database:NAME`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DatabaseObject {
    pub database: String,
}

impl DatabaseObject {
    pub fn new(database: String) -> Self {
        Self { database }
    }

    fn validate(&self) -> Result<(), String> {
        validate_database_name(&self.database).map_err(|error| error.to_string())?;
        Ok(())
    }
}

impl std::fmt::Display for DatabaseObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "database:{}", self.database)
    }
}

impl Serialize for DatabaseObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate().map_err(S::Error::custom)?;
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DatabaseObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let database = s
            .strip_prefix("database:")
            .ok_or_else(|| D::Error::custom(format!("invalid DatabaseObject {s:?}")))?;
        let object = Self {
            database: database.to_string(),
        };
        object.validate().map_err(D::Error::custom)?;
        Ok(object)
    }
}

/// A namespace resource, encoded as `namespace:DATABASE:PATH`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct NamespaceObject {
    pub database: String,
    pub namespace: NamespacePath,
}

impl NamespaceObject {
    fn validate(&self) -> Result<(), String> {
        validate_database_name(&self.database).map_err(|error| error.to_string())?;
        Ok(())
    }
}

impl std::fmt::Display for NamespaceObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "namespace:{}:{}", self.database, self.namespace)
    }
}

impl Serialize for NamespaceObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate().map_err(S::Error::custom)?;
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NamespaceObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let invalid = || D::Error::custom(format!("invalid NamespaceObject {s:?}"));
        let rest = s.strip_prefix("namespace:").ok_or_else(invalid)?;
        let (database, namespace) = rest.split_once(':').ok_or_else(invalid)?;
        let object = Self {
            database: database.to_string(),
            namespace: NamespacePath::parse(namespace).map_err(D::Error::custom)?,
        };
        object.validate().map_err(D::Error::custom)?;
        Ok(object)
    }
}

/// A table resource, encoded as `table:DATABASE:NAMESPACE$TABLE`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TableObject {
    pub database: String,
    pub namespace: NamespacePath,
    pub table: String,
}

impl TableObject {
    fn validate(&self) -> Result<(), String> {
        validate_database_name(&self.database).map_err(|error| error.to_string())?;
        validate_table_name(&self.table).map_err(|error| error.to_string())?;
        Ok(())
    }
}

impl std::fmt::Display for TableObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "table:{}:{}${}",
            self.database, self.namespace, self.table
        )
    }
}

impl Serialize for TableObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate().map_err(S::Error::custom)?;
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TableObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let invalid = || D::Error::custom(format!("invalid TableObject {s:?}"));
        let rest = s.strip_prefix("table:").ok_or_else(invalid)?;
        let (database, path) = rest.split_once(':').ok_or_else(invalid)?;
        let (namespace, table) = path.rsplit_once('$').ok_or_else(invalid)?;
        let object = Self {
            database: database.to_string(),
            namespace: NamespacePath::parse(namespace).map_err(D::Error::custom)?,
            table: table.to_string(),
        };
        object.validate().map_err(D::Error::custom)?;
        Ok(object)
    }
}

/// A view resource, encoded as `view:DATABASE:NAMESPACE$VIEW`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ViewObject {
    pub database: String,
    pub namespace: NamespacePath,
    pub view: String,
}

impl ViewObject {
    fn validate(&self) -> Result<(), String> {
        validate_database_name(&self.database).map_err(|error| error.to_string())?;
        validate_table_name(&self.view).map_err(|error| error.to_string())?;
        Ok(())
    }
}

impl std::fmt::Display for ViewObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "view:{}:{}${}", self.database, self.namespace, self.view)
    }
}

impl Serialize for ViewObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate().map_err(S::Error::custom)?;
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ViewObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let invalid = || D::Error::custom(format!("invalid ViewObject {s:?}"));
        let rest = s.strip_prefix("view:").ok_or_else(invalid)?;
        let (database, path) = rest.split_once(':').ok_or_else(invalid)?;
        let (namespace, view) = path.rsplit_once('$').ok_or_else(invalid)?;
        let object = Self {
            database: database.to_string(),
            namespace: NamespacePath::parse(namespace).map_err(D::Error::custom)?,
            view: view.to_string(),
        };
        object.validate().map_err(D::Error::custom)?;
        Ok(object)
    }
}
/// A secret resource, encoded as `secret:DATABASE:NAMESPACE$SECRET`.
///
/// ```
/// use lancedb::authz::{SecretObject, NamespacePath, Object};
///
/// let object = Object::Secret(SecretObject {
///     database: "analytics".into(),
///     namespace: NamespacePath::from_component("public"),
///     secret: "example".into(),
/// });
/// assert_eq!(object.to_string(), "secret:analytics:public$example");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SecretObject {
    pub database: String,
    pub namespace: NamespacePath,
    pub secret: String,
}

impl SecretObject {
    fn validate(&self) -> Result<(), String> {
        validate_database_name(&self.database).map_err(|error| error.to_string())?;
        validate_secret_name(&self.secret).map_err(|error| error.to_string())?;
        Ok(())
    }
}

impl std::fmt::Display for SecretObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "secret:{}:{}${}",
            self.database, self.namespace, self.secret
        )
    }
}

impl Serialize for SecretObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate().map_err(S::Error::custom)?;
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SecretObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let invalid = || D::Error::custom(format!("invalid SecretObject {s:?}"));
        let rest = s.strip_prefix("secret:").ok_or_else(invalid)?;
        let (database, path) = rest.split_once(':').ok_or_else(invalid)?;
        let (namespace, secret) = path.rsplit_once('$').ok_or_else(invalid)?;
        let object = Self {
            database: database.to_string(),
            namespace: NamespacePath::parse(namespace).map_err(D::Error::custom)?,
            secret: secret.to_string(),
        };
        object.validate().map_err(D::Error::custom)?;
        Ok(object)
    }
}

/// A function resource, encoded as `function:DATABASE:NAMESPACE$FUNCTION`.
///
/// ```
/// use lancedb::authz::{FunctionObject, NamespacePath, Object};
///
/// let object = Object::Function(FunctionObject {
///     database: "analytics".into(),
///     namespace: NamespacePath::from_component("public"),
///     function: "example".into(),
/// });
/// assert_eq!(object.to_string(), "function:analytics:public$example");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FunctionObject {
    pub database: String,
    pub namespace: NamespacePath,
    pub function: String,
}

impl FunctionObject {
    fn validate(&self) -> Result<(), String> {
        validate_database_name(&self.database).map_err(|error| error.to_string())?;
        validate_table_name(&self.function).map_err(|error| error.to_string())?;
        Ok(())
    }
}

impl std::fmt::Display for FunctionObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "function:{}:{}${}",
            self.database, self.namespace, self.function
        )
    }
}

impl Serialize for FunctionObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate().map_err(S::Error::custom)?;
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for FunctionObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let invalid = || D::Error::custom(format!("invalid FunctionObject {s:?}"));
        let rest = s.strip_prefix("function:").ok_or_else(invalid)?;
        let (database, path) = rest.split_once(':').ok_or_else(invalid)?;
        let (namespace, function) = path.rsplit_once('$').ok_or_else(invalid)?;
        let object = Self {
            database: database.to_string(),
            namespace: NamespacePath::parse(namespace).map_err(D::Error::custom)?,
            function: function.to_string(),
        };
        object.validate().map_err(D::Error::custom)?;
        Ok(object)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_object_round_trip() {
        let encoded = serde_json::to_string(&Object::System).unwrap();
        assert_eq!(encoded, r#""system""#);
        assert_eq!(
            serde_json::from_str::<Object>(&encoded).unwrap(),
            Object::System
        );
    }

    #[test]
    fn object_database_round_trip() {
        let object = Object::Database(DatabaseObject {
            database: "db".to_string(),
        });
        let encoded = serde_json::to_string(&object).unwrap();
        assert_eq!(encoded, r#""database:db""#);
        assert_eq!(serde_json::from_str::<Object>(&encoded).unwrap(), object);
    }

    #[test]
    fn object_namespace_round_trip() {
        let object = Object::Namespace(NamespaceObject {
            database: "db".to_string(),
            namespace: NamespacePath::from_component("ns"),
        });
        let encoded = serde_json::to_string(&object).unwrap();
        assert_eq!(encoded, r#""namespace:db:ns""#);
        assert_eq!(serde_json::from_str::<Object>(&encoded).unwrap(), object);
    }

    #[test]
    fn object_table_round_trip() {
        let object = Object::Table(TableObject {
            database: "db".to_string(),
            namespace: NamespacePath::from_component("ns"),
            table: "foo".to_string(),
        });
        let encoded = serde_json::to_string(&object).unwrap();
        assert_eq!(encoded, r#""table:db:ns$foo""#);
        assert_eq!(serde_json::from_str::<Object>(&encoded).unwrap(), object);
    }

    #[test]
    fn object_view_round_trip() {
        let object = Object::View(ViewObject {
            database: "db".to_string(),
            namespace: NamespacePath::from_component("ns"),
            view: "foo".to_string(),
        });
        let encoded = serde_json::to_string(&object).unwrap();
        assert_eq!(encoded, r#""view:db:ns$foo""#);
        assert_eq!(serde_json::from_str::<Object>(&encoded).unwrap(), object);
    }

    #[test]
    fn secret_objects_round_trip_and_validate() {
        let resource = SecretObject {
            database: "tenant/db".into(),
            namespace: NamespacePath::new(vec!["a".into(), "b".into()]).unwrap(),
            secret: "example".into(),
        };
        let wire = "secret:tenant/db:a$b$example";
        assert_eq!(serde_json::to_value(&resource).unwrap(), wire);
        assert_eq!(
            serde_json::from_value::<SecretObject>(serde_json::json!(wire)).unwrap(),
            resource
        );
        let object = Object::Secret(resource.clone());
        assert_eq!(object.database(), Some("tenant/db"));
        assert_eq!(object.type_name(), "secret");
        assert_eq!(wire.parse::<Object>().unwrap(), object);
        assert_eq!(serde_json::to_value(object).unwrap(), wire);
        for name in ["", ".", "..", "bad$name", "bad/name", "bad:name"] {
            let invalid = SecretObject {
                secret: name.into(),
                ..resource.clone()
            };
            assert!(serde_json::to_value(&invalid).is_err());
            assert!(serde_json::to_value(Object::Secret(invalid)).is_err());
        }
    }

    #[test]
    fn function_objects_round_trip_and_validate() {
        let resource = FunctionObject {
            database: "tenant/db".into(),
            namespace: NamespacePath::new(vec!["a".into(), "b".into()]).unwrap(),
            function: "example".into(),
        };
        let wire = "function:tenant/db:a$b$example";
        assert_eq!(serde_json::to_value(&resource).unwrap(), wire);
        assert_eq!(
            serde_json::from_value::<FunctionObject>(serde_json::json!(wire)).unwrap(),
            resource
        );
        let object = Object::Function(resource.clone());
        assert_eq!(object.database(), Some("tenant/db"));
        assert_eq!(object.type_name(), "function");
        assert_eq!(wire.parse::<Object>().unwrap(), object);
        assert_eq!(serde_json::to_value(object).unwrap(), wire);
        for name in ["", ".", "..", "bad$name", "bad/name", "bad:name"] {
            let invalid = FunctionObject {
                function: name.into(),
                ..resource.clone()
            };
            assert!(serde_json::to_value(&invalid).is_err());
            assert!(serde_json::to_value(Object::Function(invalid)).is_err());
        }
    }

    #[test]
    fn unknown_objects_round_trip() {
        for wire in [
            "future:db:resource",
            "table_extension:db:resource",
            "garbage",
        ] {
            let object: Object = wire.parse().unwrap();
            assert_eq!(object, Object::Unknown(wire.into()));
            assert_eq!(object.database(), None);
            assert_eq!(object.type_name(), UNKNOWN_TYPE_STRING);
            assert_eq!(object.to_string(), wire);
            assert_eq!(serde_json::to_value(&object).unwrap(), wire);
            assert_eq!(
                serde_json::from_value::<Object>(serde_json::json!(wire)).unwrap(),
                object
            );
        }
    }

    #[test]
    fn database_object_round_trip() {
        let object = DatabaseObject {
            database: "db".to_string(),
        };
        let encoded = serde_json::to_string(&object).unwrap();
        assert_eq!(encoded, r#""database:db""#);
        assert_eq!(
            serde_json::from_str::<DatabaseObject>(&encoded).unwrap(),
            object
        );
    }

    #[test]
    fn namespace_object_round_trip() {
        let object = NamespaceObject {
            database: "db".to_string(),
            namespace: NamespacePath::from_component("ns"),
        };
        let encoded = serde_json::to_string(&object).unwrap();
        assert_eq!(encoded, r#""namespace:db:ns""#);
        assert_eq!(
            serde_json::from_str::<NamespaceObject>(&encoded).unwrap(),
            object
        );
    }

    #[test]
    fn namespace_object_rejects_missing_namespace() {
        assert!(serde_json::from_str::<NamespaceObject>(r#""namespace:db""#).is_err());
    }

    #[test]
    fn table_object_round_trip() {
        let object = TableObject {
            database: "db".to_string(),
            namespace: NamespacePath::from_component("ns"),
            table: "foo".to_string(),
        };
        let encoded = serde_json::to_string(&object).unwrap();
        assert_eq!(encoded, r#""table:db:ns$foo""#);
        assert_eq!(
            serde_json::from_str::<TableObject>(&encoded).unwrap(),
            object
        );
    }

    #[test]
    fn table_object_rejects_missing_prefix() {
        assert!(serde_json::from_str::<TableObject>(r#""foo""#).is_err());
    }

    #[test]
    fn table_object_rejects_missing_table() {
        assert!(serde_json::from_str::<TableObject>(r#""table:db:ns""#).is_err());
    }

    #[test]
    fn objects_order_by_variant_then_path_components() {
        let database = Object::Database(DatabaseObject::new("z".to_string()));
        let namespace = Object::Namespace(NamespaceObject {
            database: "a".to_string(),
            namespace: NamespacePath::from_component("a"),
        });
        assert!(Object::System < database);
        assert!(database < namespace);

        let first_table = Object::Table(TableObject {
            database: "db".to_string(),
            namespace: NamespacePath::from_component("a"),
            table: "z".to_string(),
        });
        let second_table = Object::Table(TableObject {
            database: "db".to_string(),
            namespace: NamespacePath::from_component("b"),
            table: "a".to_string(),
        });
        assert!(first_table < second_table);
    }

    #[test]
    fn namespace_path_returns_cumulative_prefixes() {
        let path = NamespacePath::new(vec!["ns1".into(), "ns2".into(), "ns3".into()]).unwrap();
        assert_eq!(
            path.prefixes()
                .map(|prefix| prefix.to_string())
                .collect::<Vec<_>>(),
            vec!["ns1", "ns1$ns2", "ns1$ns2$ns3"]
        );
        assert_eq!(path.parent().unwrap().to_string(), "ns1$ns2");
    }

    #[test]
    fn nested_objects_round_trip_without_escaping() {
        for wire in [
            "database:tenant/db",
            "namespace:tenant/db:a$b",
            "table:tenant/db:a$b$c",
            "view:tenant/db:a$b$c",
            "secret:tenant/db:a$b$c",
            "function:tenant/db:a$b$c",
        ] {
            let object: Object = serde_json::from_value(serde_json::json!(wire)).unwrap();
            assert_eq!(wire.parse::<Object>().unwrap(), object);
            assert_eq!(object.database(), Some("tenant/db"));
            assert_eq!(object.to_string(), wire);
            assert_eq!(serde_json::to_value(object).unwrap(), wire);
        }
    }

    #[test]
    fn objects_reject_invalid_names_and_legacy_forms() {
        for database in [
            "",
            ".",
            "..",
            "db:bad",
            "db%2Fchild",
            "db$child",
            "/db",
            "db/",
            "db//child",
            "db/../child",
            "db name",
        ] {
            for wire in [
                format!("database:{database}"),
                format!("namespace:{database}:ns"),
                format!("table:{database}:ns$t"),
                format!("view:{database}:ns$v"),
                format!("secret:{database}:ns$s"),
                format!("function:{database}:ns$f"),
            ] {
                assert!(
                    serde_json::from_value::<Object>(serde_json::json!(wire)).is_err(),
                    "{wire}"
                );
            }
        }
        for name in ["", "bad:name", "bad/name", "bad%24name", "bad name"] {
            for wire in [
                format!("namespace:db:{name}"),
                format!("table:db:{name}$t"),
                format!("view:db:{name}$v"),
                format!("table:db:ns${name}"),
                format!("view:db:ns${name}"),
                format!("secret:db:{name}$s"),
                format!("function:db:{name}$f"),
                format!("secret:db:ns${name}"),
                format!("function:db:ns${name}"),
            ] {
                assert!(
                    serde_json::from_value::<Object>(serde_json::json!(wire)).is_err(),
                    "{wire}"
                );
            }
        }
        for wire in [
            "",
            "system:db",
            "database",
            "namespace",
            "table",
            "view",
            "namespace:db/ns",
            "table:db/ns/t",
            "view:db/ns/v",
            "table:db:ns",
            "view:db:ns",
            "namespace:db:a$$b",
            "table:db:ns$.",
            "view:db:ns$..",
            "table:db:a$$t",
            "view:db:$v",
            "secret",
            "function",
            "secret:db/ns/s",
            "function:db/ns/f",
            "secret:db:ns",
            "function:db:ns",
            "secret:db:a$$s",
            "function:db:$f",
            "secret:db:ns$.",
            "function:db:ns$..",
        ] {
            assert!(
                serde_json::from_value::<Object>(serde_json::json!(wire)).is_err(),
                "{wire}"
            );
            assert!(matches!(
                wire.parse::<Object>(),
                Err(crate::Error::InvalidInput { .. })
            ));
        }
    }

    #[test]
    fn namespace_components_use_namespace_validation() {
        for component in ["a$b", "c/d", "e%f", "a:b", "", ".", ".."] {
            assert!(NamespacePath::new(vec![component.into()]).is_err());
        }
        assert!(NamespacePath::new(vec!["a.b".into(), "c..d".into()]).is_ok());
    }

    #[test]
    fn serialization_rejects_invalid_directly_constructed_objects() {
        let database = DatabaseObject::new("db:bad".into());
        let namespace = NamespaceObject {
            database: "db%2Fbad".into(),
            namespace: NamespacePath::from_component("ns"),
        };
        let table = TableObject {
            database: "db".into(),
            namespace: NamespacePath::from_component("ns"),
            table: "bad$name".into(),
        };
        let view = ViewObject {
            database: "db".into(),
            namespace: NamespacePath::from_component("ns"),
            view: "..".into(),
        };
        assert!(serde_json::to_string(&database).is_err());
        assert!(serde_json::to_string(&namespace).is_err());
        assert!(serde_json::to_string(&table).is_err());
        assert!(serde_json::to_string(&view).is_err());
        for object in [
            Object::Database(database),
            Object::Namespace(namespace),
            Object::Table(table),
            Object::View(view),
        ] {
            assert!(serde_json::to_string(&object).is_err());
        }
    }
}
