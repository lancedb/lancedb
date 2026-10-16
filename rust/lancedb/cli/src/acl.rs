// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Manage deployment-wide access-control entries through the public catalog API.

use std::fmt;
use std::io::{self, Write};
use std::sync::Arc;

use anyhow::{bail, Result};
use clap::{Args, Subcommand, ValueEnum};
use lancedb::authz::{
    AccessControlEntryRequest, Authorization, ListAccessControlEntriesRequest,
    ListAccessControlEntriesResponse, Object, Privilege, Subject,
};

use crate::connection::is_native_database_url;
use crate::server_config::{parse_endpoint, Authentication, ConnectionArgs, DEFAULT_HTTP_PORT};

/// List, add, or delete ACL grants on a remote deployment.
#[derive(Args, Debug)]
pub struct AclArgs {
    #[command(subcommand)]
    pub command: AclCommand,
}

#[derive(Subcommand, Debug)]
pub enum AclCommand {
    /// List one page of access-control entries.
    List(ListArgs),
    /// Grant a privilege to a principal, group, or role.
    Add(ChangeArgs),
    /// Revoke a privilege from a principal, group, or role.
    Delete(ChangeArgs),
}

/// Select a subject by its canonical ID, provider name, role, or API key.
#[derive(Args, Default)]
#[group(id = "acl_subject", multiple = false)]
pub struct SubjectArgs {
    /// Canonical principal ID.
    #[arg(long)]
    pub principal_id: Option<String>,
    /// Unambiguous principal name.
    #[arg(long)]
    pub principal_name: Option<String>,
    /// Canonical group ID.
    #[arg(long)]
    pub group_id: Option<String>,
    /// Unambiguous group name.
    #[arg(long)]
    pub group_name: Option<String>,
    /// LanceDB-managed role name.
    #[arg(long)]
    pub role: Option<String>,
    /// API key belonging to the principal being selected, not the connection credential.
    #[arg(long)]
    pub principal_api_key: Option<String>,
}

impl fmt::Debug for SubjectArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubjectArgs")
            .field("principal_id", &self.principal_id)
            .field("principal_name", &self.principal_name)
            .field("group_id", &self.group_id)
            .field("group_name", &self.group_name)
            .field("role", &self.role)
            .field(
                "principal_api_key",
                &self.principal_api_key.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl SubjectArgs {
    fn selector(&self) -> Result<Option<Subject>> {
        let mut subjects = Vec::new();
        if let Some(value) = &self.principal_id {
            subjects.push(Subject::principal_id(value.clone())?);
        }
        if let Some(value) = &self.principal_name {
            subjects.push(Subject::principal_name(value.clone())?);
        }
        if let Some(value) = &self.group_id {
            subjects.push(Subject::group_id(value.clone())?);
        }
        if let Some(value) = &self.group_name {
            subjects.push(Subject::group_name(value.clone())?);
        }
        if let Some(value) = &self.role {
            subjects.push(Subject::role(value.clone())?);
        }
        if let Some(value) = &self.principal_api_key {
            subjects.push(Subject::principal_api_key(value.clone())?);
        }
        if subjects.len() > 1 {
            bail!("specify only one ACL subject selector");
        }
        Ok(subjects.pop())
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ListFormat {
    Tsv,
    Json,
}

#[derive(Args, Debug)]
pub struct ListArgs {
    #[command(flatten)]
    pub connection: ConnectionArgs,
    /// List only grants on this object, for example table:analytics:public$events.
    #[arg(long)]
    pub object: Option<Object>,
    #[command(flatten)]
    pub subject: SubjectArgs,
    /// Maximum stored subject/object pairs on this page (server cap: 100).
    #[arg(long)]
    pub limit: Option<u32>,
    /// Continue from the token returned by a previous list command.
    #[arg(long)]
    pub page_token: Option<String>,
    /// Output format.
    #[arg(long, value_enum, default_value = "tsv")]
    pub format: ListFormat,
}

impl ListArgs {
    fn request(&self) -> Result<ListAccessControlEntriesRequest> {
        if self.limit == Some(0) {
            bail!("--limit must be positive");
        }
        let mut request = ListAccessControlEntriesRequest::default();
        request.object = self.object.clone();
        request.subject = self.subject.selector()?;
        request.continuation_token = self.page_token.clone();
        request.max_results = self.limit;
        Ok(request)
    }
}

#[derive(Args, Debug)]
pub struct ChangeArgs {
    #[command(flatten)]
    pub connection: ConnectionArgs,
    #[command(flatten)]
    pub subject: SubjectArgs,
    /// Object to grant or revoke on, for example table:analytics:public$events.
    #[arg(long)]
    pub object: Object,
    /// Privilege name, for example SELECT or OWNERSHIP.
    #[arg(long)]
    pub privilege: Privilege,
}

impl ChangeArgs {
    fn request(&self) -> Result<AccessControlEntryRequest> {
        let subject = self
            .subject
            .selector()?
            .ok_or_else(|| anyhow::anyhow!("specify one ACL subject selector"))?;
        Ok(AccessControlEntryRequest::new(
            self.object.clone(),
            subject,
            self.privilege.clone(),
        ))
    }
}

async fn connect(connection: &ConnectionArgs) -> Result<Arc<dyn Authorization>> {
    let connection = connection.resolve(None)?;
    if is_native_database_url(&connection.http_server_url) {
        bail!("ACL administration requires a remote HTTP endpoint");
    }
    let endpoint = parse_endpoint(&connection.http_server_url, DEFAULT_HTTP_PORT)?.url();
    let mut builder = lancedb::connect_catalog(endpoint);
    builder = match &connection.authentication {
        Authentication::ApiKey(key) => builder.api_key(key),
        _ => builder.api_key(""),
    };
    let mut config = crate::lancedb_client_config(&connection.tls, &connection.authentication)
        .unwrap_or_default();
    if let Some(account) = &connection.azure_storage_account_name {
        config
            .extra_headers
            .insert("x-azure-storage-account-name".into(), account.clone());
    }
    Ok(builder.client_config(config).execute().await?.authz()?)
}

/// Execute an ACL command using the selected connection profile.
pub async fn run_command(args: AclArgs) -> Result<()> {
    match args.command {
        AclCommand::List(args) => {
            let request = args.request()?;
            let authz = connect(&args.connection).await?;
            let page = authz.list_acls(request).await?;
            write_page(&page, args.format, &mut io::stdout(), &mut io::stderr())
        }
        AclCommand::Add(args) => {
            let request = args.request()?;
            connect(&args.connection).await?.add_acl(request).await?;
            println!("Access-control entry added");
            Ok(())
        }
        AclCommand::Delete(args) => {
            let request = args.request()?;
            connect(&args.connection).await?.delete_acl(request).await?;
            println!("Access-control entry deleted");
            Ok(())
        }
    }
}

fn write_tsv_field(output: &mut impl Write, value: &str) -> Result<()> {
    if value.contains(['\t', '\r', '\n', '"']) {
        write!(output, "\"{}\"", value.replace('"', "\"\""))?;
    } else {
        write!(output, "{value}")?;
    }
    Ok(())
}

fn write_page(
    page: &ListAccessControlEntriesResponse,
    format: ListFormat,
    output: &mut impl Write,
    diagnostics: &mut impl Write,
) -> Result<()> {
    match format {
        ListFormat::Json => {
            serde_json::to_writer_pretty(&mut *output, page)?;
            writeln!(output)?;
        }
        ListFormat::Tsv => {
            for entry in &page.entries {
                let subject = entry
                    .subject_name
                    .as_deref()
                    .filter(|name| !name.is_empty())
                    .map(str::to_owned)
                    .unwrap_or_else(|| entry.subject_id.to_string());
                for (index, value) in [
                    subject,
                    entry.object.to_string(),
                    entry.privilege.to_string(),
                ]
                .iter()
                .enumerate()
                {
                    if index > 0 {
                        write!(output, "\t")?;
                    }
                    write_tsv_field(output, value)?;
                }
                writeln!(output)?;
            }
            if let Some(token) = &page.continuation_token {
                writeln!(diagnostics, "Next page token: {token}")?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn parse(args: &[&str]) -> crate::cli::Args {
        crate::cli::Args::try_parse_from(args).unwrap()
    }

    #[test]
    fn acl_commands_parse_and_validate_selectors() {
        let args = parse(&[
            "lancedb",
            "acl",
            "add",
            "--principal-name",
            "Alice",
            "--object",
            "table:analytics:public$events",
            "--privilege",
            "SELECT",
        ]);
        let Some(crate::cli::Commands::Acl(AclArgs {
            command: AclCommand::Add(add),
        })) = args.command
        else {
            panic!("expected ACL add command");
        };
        let request = add.request().unwrap();
        assert_eq!(request.subject.to_wire(), "P:Alice");
        assert_eq!(request.object.to_string(), "table:analytics:public$events");
        assert_eq!(request.privilege, Privilege::Select);

        for selector in [
            ["--principal-id", "alice"],
            ["--group-id", "eng"],
            ["--group-name", "Engineering"],
            ["--role", "reader"],
            ["--principal-api-key", "secret"],
        ] {
            let args = parse(&[
                "lancedb",
                "acl",
                "delete",
                selector[0],
                selector[1],
                "--object",
                "table:analytics:public$events",
                "--privilege",
                "SELECT",
            ]);
            let Some(crate::cli::Commands::Acl(AclArgs {
                command: AclCommand::Delete(delete),
            })) = args.command
            else {
                panic!("expected ACL delete command");
            };
            assert!(delete.request().is_ok());
        }

        assert!(crate::cli::Args::try_parse_from([
            "lancedb",
            "acl",
            "list",
            "--principal-id",
            "alice",
            "--role",
            "reader"
        ])
        .is_err());
        let args = crate::cli::Args::try_parse_from([
            "lancedb",
            "acl",
            "add",
            "--object",
            "system",
            "--privilege",
            "OPERATE",
        ])
        .unwrap();
        let Some(crate::cli::Commands::Acl(AclArgs {
            command: AclCommand::Add(add),
        })) = args.command
        else {
            panic!("expected ACL add command");
        };
        assert!(add.request().err().unwrap().to_string().contains("subject"));
        assert!(crate::cli::Args::try_parse_from([
            "lancedb",
            "acl",
            "add",
            "--principal-id",
            "alice",
            "--object",
            "table:bad",
            "--privilege",
            "SELECT"
        ])
        .is_err());
        let subject = SubjectArgs {
            principal_api_key: Some("secret-target-key".into()),
            ..Default::default()
        };
        assert!(!format!("{subject:?}").contains("secret-target-key"));
    }

    #[test]
    fn list_request_and_output_preserve_page_token_and_canonical_id() {
        let args = parse(&[
            "lancedb",
            "acl",
            "list",
            "--object",
            "table:analytics:public$events",
            "--group-name",
            "Engineering",
            "--limit",
            "10",
            "--page-token",
            "prior",
        ]);
        let Some(crate::cli::Commands::Acl(AclArgs {
            command: AclCommand::List(list),
        })) = args.command
        else {
            panic!("expected ACL list command");
        };
        let request = list.request().unwrap();
        assert_eq!(request.subject.unwrap().to_wire(), "G:Engineering");
        assert_eq!(request.max_results, Some(10));
        assert_eq!(request.continuation_token.as_deref(), Some("prior"));

        let page: ListAccessControlEntriesResponse = serde_json::from_value(serde_json::json!({
            "entries": [{
                "subject_id": "p:alice",
                "subject_name": "Alice\tSmith",
                "object": "table:analytics:public$events",
                "privilege": "SELECT"
            }],
            "continuation_token": "next"
        }))
        .unwrap();
        let (mut output, mut diagnostics) = (Vec::new(), Vec::new());
        write_page(&page, ListFormat::Tsv, &mut output, &mut diagnostics).unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            "\"Alice\tSmith\"\ttable:analytics:public$events\tSELECT\n"
        );
        assert_eq!(
            String::from_utf8(diagnostics).unwrap(),
            "Next page token: next\n"
        );

        let (mut output, mut diagnostics) = (Vec::new(), Vec::new());
        write_page(&page, ListFormat::Json, &mut output, &mut diagnostics).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(json["entries"][0]["subject_id"], "p:alice");
        assert_eq!(json["continuation_token"], "next");
        assert!(diagnostics.is_empty());
    }
}
