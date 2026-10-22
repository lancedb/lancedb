// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Deployment-wide authorization, accessed through a catalog.
//!
//! Subjects use `p:ID`, `P:NAME`, `g:ID`, `G:NAME`, `r:ROLE`, or `a:API_KEY`.
//! Objects use literal names such as `table:tenant/db:public$events`.
//! Names are resolved by the server. Request types intentionally do not implement
//! Debug because their subject selectors may contain secret API keys.
//!
//! ```no_run
//! # #[cfg(feature = "remote")]
//! # async fn example() -> lancedb::Result<()> {
//! let catalog = lancedb::connect_catalog("https://example.com")
//!     .api_key("secret")
//!     .execute().await?;
//! let authz = catalog.authz()?;
//! use lancedb::authz::{AccessControlEntryRequest, Object, TableObject, NamespacePath, Subject, Privilege};
//! let grant = AccessControlEntryRequest::new(
//!     Object::Table(TableObject {
//!         database: "analytics".into(),
//!         namespace: NamespacePath::from_component("public"),
//!         table: "foo".into(),
//!     }),
//!     Subject::role("reader")?,
//!     Privilege::Select,
//! );
//! authz.add_acl(grant).await?;
//! let page = authz.list_acls(Default::default()).await?;
//! for entry in page.entries {
//!     println!("{} {} {}", entry.object, entry.subject_id, entry.privilege);
//! }
//! # Ok(())
//! # }
//! ```

pub mod api;
pub mod object;
pub mod privilege;
pub mod subject;

pub use api::*;
pub use object::{
    DatabaseObject, FunctionObject, NamespaceObject, NamespacePath, Object, SecretObject,
    TableObject, ViewObject,
};
pub use privilege::*;
pub use subject::{Subject, SubjectKind};
