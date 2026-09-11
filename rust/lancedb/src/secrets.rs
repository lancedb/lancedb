// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Named Secrets: database-scoped credentials a Function binds by name.
//!
//! Nothing here holds a credential. The verbs live on
//! [`crate::connection::Connection`], and none of them returns a value -- by
//! construction rather than by policy, so there is no code path that could.
//! What a Function records is a binding, in [`crate::function`].

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
