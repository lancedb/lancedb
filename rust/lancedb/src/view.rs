// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Views: a named query a database stores and plans on every read.
//!
//! A view holds no rows. What it stores is the statement that defines it and
//! the schema that statement resolved to, so a reader sees the sources as they
//! are now rather than as they were when the view was created. That is the
//! whole difference from [`crate::materialized_view`], which holds rows and
//! moves them forward by refresh.
//!
//! The verbs live on [`crate::connection::Connection`]. A view is read through
//! SQL, by name, so there is no `open_view` returning a
//! [`crate::table::Table`]: there would be no rows behind it.

use arrow_schema::SchemaRef;

/// What a database records about one view.
///
/// Returned by [`crate::connection::Connection::describe_view`], and by
/// `create_view` so a caller has the resolved schema without a second call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewDescription {
    /// The view's name within its namespace.
    pub name: String,
    /// The namespace holding the view; empty is the root namespace.
    pub namespace_path: Vec<String>,
    /// The defining query, as the database stores it.
    pub query: String,
    /// The database that unqualified table names in `query` resolve against.
    /// A view created from a session connected elsewhere can name a database
    /// other than its own, so this is part of what the query means rather
    /// than a restatement of where the view lives.
    pub default_database: String,
    /// The namespace path those unqualified names resolve against; empty is
    /// the root namespace.
    ///
    /// Recorded with the view because it outlives the session that declared
    /// it: a reader that resolved the query against its own default namespace
    /// could read a different table than the view was defined over.
    pub default_namespace_path: Vec<String>,
    /// The schema the defining query resolved to when the view was created.
    pub schema: SchemaRef,
}
