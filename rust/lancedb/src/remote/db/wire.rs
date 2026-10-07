// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

// Request structure for the remote clone table API
#[derive(serde::Serialize)]
pub(super) struct RemoteCloneTableRequest {
    pub(super) source_location: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) source_version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) source_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) is_shallow: Option<bool>,
}

#[derive(serde::Deserialize)]
pub(super) struct RemoteListJobRow {
    pub(super) job_id: String,
    #[serde(default)]
    pub(super) table: String,
    #[serde(default)]
    pub(super) job_type: String,
    #[serde(default)]
    pub(super) state: String,
    #[serde(default)]
    pub(super) created_at_millis: i64,
}

#[derive(serde::Deserialize)]
pub(super) struct RemoteListJobsResponse {
    #[serde(default)]
    pub(super) jobs: Vec<RemoteListJobRow>,
    #[serde(default)]
    pub(super) page_token: Option<String>,
}

#[derive(serde::Deserialize)]
pub(super) struct RemoteListedFunctionVersion {
    pub(super) definition: FunctionVersion,
}

#[derive(serde::Deserialize)]
pub(super) struct RemoteListFunctionsResponse {
    #[serde(default)]
    pub(super) functions: Vec<RemoteListedFunctionVersion>,
    #[serde(default)]
    pub(super) page_token: Option<String>,
}

#[derive(serde::Deserialize)]
pub(super) struct RemoteDropFunctionResponse {
    pub(super) dropped: bool,
}

/// The create body: every field of [`FunctionRegistrationRequest`] except the
/// name, which is the path identifier.
///
/// A struct rather than a literal listing the fields, so that the compiler
/// decides what reaches the service. A field the registration request grows is
/// a build error here until it is handled; a literal would simply not send it.
#[derive(serde::Serialize)]
pub(super) struct RemoteCreateFunctionRequest<'a> {
    pub(super) artifact: &'a FunctionArtifactRequest,
    pub(super) signature: &'a FunctionSignature,
    pub(super) runtime: &'a PythonRuntimeSpec,
    /// Absent when the Function binds nothing, so such a client sends what a
    /// client without bindings sends.
    ///
    /// A service that does not know the field ignores it: registration
    /// succeeds, the returned version carries no bindings, and the Function
    /// fails at execution with the variable unset. [`ServerVersion`] is how
    /// this codebase refuses a feature the service is too old for; it is held
    /// per table, so gating a database-level call is follow-up work.
    ///
    /// [`ServerVersion`]: super::db::ServerVersion
    #[serde(skip_serializing_if = "<[SecretBinding]>::is_empty")]
    pub(super) secret_bindings: &'a [SecretBinding],
}

/// Create a Secret under a name the database does not yet hold.
///
/// Declared separately from the alter request although the two are identical
/// today: they are different operations to the service -- one refuses an
/// existing name, the other requires it -- and either may grow a field the
/// other has no meaning for.
///
/// The name and its namespace are the path identifier, so neither appears here.
#[derive(serde::Serialize)]
pub(super) struct RemoteCreateSecretRequest<'a> {
    pub(super) value: &'a str,
}

/// Replace the credential behind a Secret the database already holds.
#[derive(serde::Serialize)]
pub(super) struct RemoteAlterSecretRequest<'a> {
    pub(super) value: &'a str,
}

#[derive(serde::Deserialize)]
pub(super) struct RemoteListSecretsResponse {
    #[serde(default)]
    pub(super) secrets: Vec<RemoteListedSecret>,
    #[serde(default)]
    pub(super) page_token: Option<String>,
}

/// An object rather than a bare name so a later listing can carry a Secret's
/// type or last-updated time without breaking this one.
#[derive(serde::Deserialize)]
pub(super) struct RemoteListedSecret {
    pub(super) name: String,
}

/// Define a view from a query. The name and its namespace are the path
/// identifier, so neither appears here.
#[derive(serde::Serialize)]
pub(super) struct RemoteCreateViewRequest<'a> {
    pub(super) query: &'a str,
}

/// What the service reports about one view. The schema arrives as the
/// namespace spec's JSON encoding, which is what `describe_table` uses too.
#[derive(serde::Deserialize)]
pub(super) struct RemoteViewDescription {
    pub(super) name: String,
    #[serde(default)]
    pub(super) namespace: Vec<String>,
    pub(super) query: String,
    pub(super) default_database: String,
    /// A path, like `namespace`: the root is the absent field rather than a
    /// spelling of its own.
    #[serde(default)]
    pub(super) default_namespace: Vec<String>,
    pub(super) schema: JsonArrowSchema,
}

impl RemoteViewDescription {
    pub(super) fn into_description(self, request_id: String) -> Result<ViewDescription> {
        let schema =
            lance_namespace::schema::convert_json_arrow_schema(&self.schema).map_err(|source| {
                Error::Http {
                    source: format!("View '{}' has an undecodable schema: {source}", self.name)
                        .into(),
                    request_id,
                    status_code: None,
                }
            })?;
        Ok(ViewDescription {
            name: self.name,
            namespace_path: self.namespace,
            query: self.query,
            default_database: self.default_database,
            default_namespace_path: self.default_namespace,
            schema: Arc::new(schema),
        })
    }
}

#[derive(serde::Deserialize)]
pub(super) struct RemoteListViewsResponse {
    #[serde(default)]
    pub(super) views: Vec<String>,
    #[serde(default)]
    pub(super) page_token: Option<String>,
}
