// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Authorization requests, responses, and the deployment-wide API.

use super::{Object, Privilege, Subject};
use crate::Result;
use serde::{Deserialize, Serialize};

/// Filters and pagination for a page of ACL grants. Omitted filters match all.
#[derive(Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ListAccessControlEntriesRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<Object>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<Subject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Maximum stored subject/object pairs, not expanded privilege entries.
    /// The server caps this at 100; None uses its default. Must be positive.
    pub max_results: Option<u32>,
}

/// One privilege granted to a canonical subject on an object.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct AccessControlEntry {
    #[serde(deserialize_with = "Subject::deserialize_canonical")]
    pub subject_id: Subject,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_name: Option<String>,
    pub object: Object,
    pub privilege: Privilege,
}

/// One page of grants. A stored subject/object pair may expand to several entries.
#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ListAccessControlEntriesResponse {
    pub entries: Vec<AccessControlEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,
}

/// One grant to add or delete. The subject may be selected by ID, name, or API key.
#[derive(Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AccessControlEntryRequest {
    pub subject: Subject,
    pub object: Object,
    /// A privilege such as [`Privilege::Select`] or [`Privilege::Usage`].
    /// [`Privilege::Unknown`] preserves future privilege names.
    pub privilege: Privilege,
}

/// Filters and pagination for direct principal/group role bindings.
#[derive(Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ListRoleBindingsRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<Subject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Maximum bindings on this page; the server caps this at 100.
    pub max_results: Option<u32>,
}

/// A direct principal or group membership in a LanceDB-managed role.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RoleBinding {
    #[serde(deserialize_with = "Subject::deserialize_canonical")]
    pub subject_id: Subject,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_name: Option<String>,
    pub role: String,
}

/// One page of direct bindings, without expanding group membership.
#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ListRoleBindingsResponse {
    pub bindings: Vec<RoleBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,
}

/// A role to grant to or revoke from a principal or group. Roles cannot nest.
#[derive(Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RoleBindingRequest {
    pub subject: Subject,
    pub role: String,
}

/// Select a principal by canonical ID, provider name, or API key.
#[derive(Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct GetPrincipalRequest {
    pub principal: Subject,
}

/// Select a group by canonical ID or provider name.
#[derive(Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct GetGroupRequest {
    pub group: Subject,
}

/// A canonical identity-provider ID and optional display name.
#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct IdentityInfo {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// A principal, its provider-managed groups, and roles held directly or via groups.
#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct PrincipalInfo {
    pub principal_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_name: Option<String>,
    pub groups: Vec<IdentityInfo>,
    pub roles: Vec<String>,
}

/// A group, its provider-managed members, and its directly granted roles.
#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct GroupInfo {
    pub group_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_name: Option<String>,
    pub principals: Vec<IdentityInfo>,
    pub roles: Vec<String>,
}

/// Authorization operations on one deployment, independent of database selection.
///
/// ACL writes preserve server conflict/not-found errors and must not be replayed
/// after ambiguous transport failures. Role-binding writes are idempotent.
#[async_trait::async_trait]
pub trait Authorization: Send + Sync + std::fmt::Debug + 'static {
    /// List a page of ACLs. Requires USAGE on the object, or system if unfiltered.
    async fn list_acls(
        &self,
        request: ListAccessControlEntriesRequest,
    ) -> Result<ListAccessControlEntriesResponse>;
    /// Grant a privilege; requires OWNERSHIP. Duplicate grants return HTTP 409.
    /// Granting OWNERSHIP transfers it from the previous owner.
    async fn add_acl(&self, request: AccessControlEntryRequest) -> Result<()>;
    /// Revoke a privilege; requires OWNERSHIP. Missing grants return HTTP 404.
    /// OWNERSHIP cannot be deleted.
    async fn delete_acl(&self, request: AccessControlEntryRequest) -> Result<()>;
    /// List a page of direct bindings; requires USAGE on system.
    async fn list_role_bindings(
        &self,
        request: ListRoleBindingsRequest,
    ) -> Result<ListRoleBindingsResponse>;
    /// Idempotently grant a role; requires OPERATE on system.
    async fn add_role_binding(&self, request: RoleBindingRequest) -> Result<()>;
    /// Idempotently revoke a role; requires OPERATE on system.
    async fn delete_role_binding(&self, request: RoleBindingRequest) -> Result<()>;
    /// Inspect a principal and effective roles; requires USAGE on system.
    async fn get_principal(&self, request: GetPrincipalRequest) -> Result<PrincipalInfo>;
    /// Inspect a group and direct roles; requires USAGE on system.
    async fn get_group(&self, request: GetGroupRequest) -> Result<GroupInfo>;
}

impl AccessControlEntryRequest {
    /// Build one grant from validated resource and identity selectors.
    pub fn new(object: Object, subject: Subject, privilege: Privilege) -> Self {
        Self {
            object,
            subject,
            privilege,
        }
    }
}

impl ListRoleBindingsRequest {
    /// Reject role subjects, since roles cannot contain other roles.
    pub fn validate(&self) -> Result<()> {
        if let Some(subject) = &self.subject {
            subject.validate_role_member()?;
        }
        Ok(())
    }
}

impl RoleBindingRequest {
    /// Build a role grant/revocation for a principal or group.
    pub fn new(subject: Subject, role: impl Into<String>) -> Result<Self> {
        subject.validate_role_member()?;
        Ok(Self {
            subject,
            role: role.into(),
        })
    }

    /// Validate the member selector, including deserialized requests.
    pub fn validate(&self) -> Result<()> {
        self.subject.validate_role_member()
    }
}

impl GetPrincipalRequest {
    /// Select a principal by ID, name, or API key.
    pub fn new(principal: Subject) -> Result<Self> {
        principal.validate_principal()?;
        Ok(Self { principal })
    }

    /// Validate the lookup selector, including deserialized requests.
    pub fn validate(&self) -> Result<()> {
        self.principal.validate_principal()
    }
}

impl GetGroupRequest {
    /// Select a group by ID or name.
    pub fn new(group: Subject) -> Result<Self> {
        group.validate_group()?;
        Ok(Self { group })
    }

    /// Validate the lookup selector, including deserialized requests.
    pub fn validate(&self) -> Result<()> {
        self.group.validate_group()
    }
}
