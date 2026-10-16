// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::borrow::Cow;
use std::io::{self, Write};

use anyhow::{anyhow, Result};
use clap::{Args, Subcommand};
use rustyline::highlight::Highlighter;
use rustyline::{Editor, Helper};

use crate::server_config::{Config, Profile};

#[derive(Args, Debug)]
pub struct ProfileArgs {
    #[command(subcommand)]
    pub command: ProfileCommand,
}

/// The settable fields of a profile, shared by `create` and `edit`.
///
/// `create` writes them all; `edit` applies only the ones that were passed.
#[derive(Args, Debug, Default)]
pub struct ProfileFields {
    /// SQL server URL (Flight SQL endpoint), as host:port or grpc(s)://host:port
    #[arg(long)]
    pub sql_server_url: Option<String>,

    /// HTTP server URL (REST endpoint), as host:port or http(s)://host:port
    #[arg(long)]
    pub http_server_url: Option<String>,

    /// API key for authentication
    #[arg(long, conflicts_with = "oidc_access_token_file")]
    pub api_key: Option<String>,

    /// Path to a file containing an OIDC access token
    #[arg(long, value_name = "PATH", conflicts_with = "api_key")]
    pub oidc_access_token_file: Option<String>,

    /// Default database name
    #[arg(long, short)]
    pub database: Option<String>,

    /// Azure storage account name for Azure-backed databases
    #[arg(long)]
    pub azure_storage_account_name: Option<String>,

    /// Path to a PEM CA certificate trusted in addition to the platform roots
    #[arg(long, value_name = "PATH")]
    pub ca_cert: Option<String>,

    /// Path to a PEM client certificate presented for mTLS
    #[arg(long, value_name = "PATH")]
    pub client_cert: Option<String>,

    /// Path to the PKCS#8 PEM private key for the client certificate
    #[arg(long, value_name = "PATH")]
    pub client_key: Option<String>,
}

impl ProfileFields {
    /// Whether any field was supplied, which is what makes `create`
    /// non-interactive. Destructured so a new field cannot be forgotten here.
    fn any_set(&self) -> bool {
        let Self {
            sql_server_url,
            http_server_url,
            api_key,
            oidc_access_token_file,
            database,
            azure_storage_account_name,
            ca_cert,
            client_cert,
            client_key,
        } = self;
        [
            sql_server_url,
            http_server_url,
            api_key,
            oidc_access_token_file,
            database,
            azure_storage_account_name,
            ca_cert,
            client_cert,
            client_key,
        ]
        .iter()
        .any(|field| field.is_some())
    }
}

impl From<&ProfileFields> for Profile {
    fn from(fields: &ProfileFields) -> Self {
        Self {
            sql_server_url: fields.sql_server_url.clone(),
            http_server_url: fields.http_server_url.clone(),
            api_key: fields.api_key.clone(),
            oidc_access_token_file: fields.oidc_access_token_file.clone(),
            oidc: None,
            database: fields.database.clone(),
            azure_storage_account_name: fields.azure_storage_account_name.clone(),
            ca_cert: fields.ca_cert.clone(),
            client_cert: fields.client_cert.clone(),
            client_key: fields.client_key.clone(),
        }
    }
}

#[derive(Subcommand, Debug)]
pub enum ProfileCommand {
    /// List all configured profiles
    List,

    /// Show details of a specific profile
    Show {
        /// Name of the profile to show (uses default if not specified)
        name: Option<String>,
    },

    /// Create a new profile (interactive by default)
    Create {
        /// Name for the new profile
        name: String,

        #[command(flatten)]
        fields: ProfileFields,
    },

    /// Set the default profile
    #[command(name = "set")]
    SetDefault {
        /// Name of the profile to set as default
        name: String,
    },

    /// Delete a profile
    Delete {
        /// Name of the profile to delete
        name: String,
    },

    /// Edit an existing profile
    Edit {
        /// Name of the profile to edit
        name: String,

        #[command(flatten)]
        fields: ProfileFields,
    },
}

pub async fn run_profile_command(args: ProfileArgs) -> Result<()> {
    match args.command {
        ProfileCommand::List => list_profiles(),
        ProfileCommand::Show { name } => show_profile(name),
        ProfileCommand::Create { name, fields } => {
            // If any flags are provided, use non-interactive mode
            if fields.any_set() {
                create_profile(&name, Profile::from(&fields))
            } else {
                create_profile_interactive(&name)
            }
        }
        ProfileCommand::SetDefault { name } => set_default_profile(&name),
        ProfileCommand::Delete { name } => delete_profile(&name),
        ProfileCommand::Edit { name, fields } => edit_profile(&name, Profile::from(&fields)),
    }
}

fn list_profiles() -> Result<()> {
    let config = Config::load_default()?;
    let default_profile = config.default_profile.as_deref().unwrap_or("local");

    println!("Configured profiles:");

    // Always show "local" as it's built-in
    let is_default = default_profile == "local";
    let default_marker = if is_default { "*" } else { " " };
    let in_config = if config.profiles.contains_key("local") {
        ""
    } else {
        " [built-in]"
    };
    println!("  {} local{}", default_marker, in_config);

    // Show configured profiles
    let mut profile_names: Vec<_> = config.profiles.keys().collect();
    profile_names.sort();

    for name in profile_names {
        if name == "local" {
            continue; // Already shown above
        }
        let is_default = default_profile == name;
        let default_marker = if is_default { "*" } else { " " };
        println!("  {} {}", default_marker, name);
    }

    if config.profiles.is_empty()
        || (config.profiles.len() == 1 && config.profiles.contains_key("local"))
    {
        println!();
        println!("No custom profiles configured. Use 'lancedb profile create <name>' to add one.");
    }

    Ok(())
}

fn show_profile(name: Option<String>) -> Result<()> {
    let config = Config::load_default()?;
    let profile_name = name
        .as_deref()
        .or(config.default_profile.as_deref())
        .unwrap_or("local");

    let profile = config.get_profile(Some(profile_name));
    let is_builtin = !config.profiles.contains_key(profile_name) && profile_name == "local";

    let builtin_marker = if is_builtin { " [built-in]" } else { "" };
    println!("Profile: {}{}", profile_name, builtin_marker);
    println!(
        "  sql_server_url:  {}",
        profile.sql_server_url.as_deref().unwrap_or("<not set>")
    );
    println!(
        "  http_server_url: {}",
        profile.http_server_url.as_deref().unwrap_or("<not set>")
    );
    println!(
        "  api_key:         {}",
        profile
            .api_key
            .as_ref()
            .map(|_| "********")
            .unwrap_or("<not set>")
    );
    println!(
        "  oidc_access_token_file: {}",
        profile
            .oidc_access_token_file
            .as_deref()
            .unwrap_or("<not set>")
    );
    if let Some(oidc) = &profile.oidc {
        println!("  oidc: {}", toml::to_string(oidc)?);
    }
    println!(
        "  database:        {}",
        profile.database.as_deref().unwrap_or("<not set>")
    );
    println!(
        "  azure_storage_account_name: {}",
        profile
            .azure_storage_account_name
            .as_deref()
            .unwrap_or("<not set>")
    );
    println!(
        "  ca_cert:         {}",
        profile.ca_cert.as_deref().unwrap_or("<not set>")
    );
    println!(
        "  client_cert:     {}",
        profile.client_cert.as_deref().unwrap_or("<not set>")
    );
    println!(
        "  client_key:      {}",
        profile.client_key.as_deref().unwrap_or("<not set>")
    );

    Ok(())
}

fn create_profile(name: &str, profile: Profile) -> Result<()> {
    let mut config = Config::load_default()?;

    if config.profiles.contains_key(name) {
        return Err(anyhow!(
            "Profile '{}' already exists. Use 'lancedb profile edit {}' to modify it.",
            name,
            name
        ));
    }

    config.set_profile(name, profile);
    config.save_default()?;

    println!("Created profile '{}'", name);
    Ok(())
}

fn create_profile_interactive(name: &str) -> Result<()> {
    let mut config = Config::load_default()?;

    if config.profiles.contains_key(name) {
        return Err(anyhow!(
            "Profile '{}' already exists. Use 'lancedb profile edit {}' to modify it.",
            name,
            name
        ));
    }

    println!("Creating profile '{}'", name);
    println!("Press Enter to skip optional fields.");
    println!();

    let sql_server_url =
        prompt_optional("SQL server URL (host:port, or grpcs://host:port for TLS)")?;
    let http_server_url =
        prompt_optional("HTTP server URL (host:port, or https://host:port for TLS)")?;
    let api_key = prompt_secret("API key")?;
    let oidc_access_token_file = if api_key.is_none() {
        prompt_optional("OIDC access token file")?
    } else {
        None
    };
    let database = prompt_optional("Default database")?;
    let azure_storage_account_name = prompt_optional("Azure storage account name")?;
    let ca_cert = prompt_optional("CA certificate path (PEM, for a private CA)")?;
    let client_cert = prompt_optional("Client certificate path (PEM, for mTLS)")?;
    let client_key = prompt_optional("Client key path (PKCS#8 PEM, for mTLS)")?;

    let profile = Profile {
        sql_server_url,
        http_server_url,
        api_key,
        oidc_access_token_file,
        oidc: None,
        database,
        azure_storage_account_name,
        ca_cert,
        client_cert,
        client_key,
    };
    config.set_profile(name, profile);
    config.save_default()?;

    println!();
    println!("Created profile '{}'", name);
    Ok(())
}

fn set_default_profile(name: &str) -> Result<()> {
    let mut config = Config::load_default()?;

    // Verify profile exists (or is built-in "local")
    if name != "local" && !config.profiles.contains_key(name) {
        return Err(anyhow!(
            "Profile '{}' does not exist. Use 'lancedb profile list' to see available profiles.",
            name
        ));
    }

    config.set_default_profile(name);
    config.save_default()?;

    println!("Set '{}' as the default profile", name);
    Ok(())
}

fn delete_profile(name: &str) -> Result<()> {
    if name == "local" {
        return Err(anyhow!("Cannot delete the built-in 'local' profile"));
    }

    let mut config = Config::load_default()?;

    if !config.profiles.contains_key(name) {
        return Err(anyhow!("Profile '{}' does not exist", name));
    }

    config.remove_profile(name);

    // If we deleted the default profile, reset to "local"
    if config.default_profile.as_deref() == Some(name) {
        config.set_default_profile("local");
        println!("Default profile reset to 'local'");
    }

    config.save_default()?;

    println!("Deleted profile '{}'", name);
    Ok(())
}

fn edit_profile(name: &str, updates: Profile) -> Result<()> {
    let mut config = Config::load_default()?;

    // Get existing profile or create from built-in
    let mut profile = if config.profiles.contains_key(name) {
        config.profiles.get(name).cloned().unwrap()
    } else if name == "local" {
        Profile::local_default()
    } else {
        return Err(anyhow!(
            "Profile '{}' does not exist. Use 'lancedb profile create {}' to create it.",
            name,
            name
        ));
    };

    // Update only the fields that were provided
    let Profile {
        sql_server_url,
        http_server_url,
        api_key,
        oidc_access_token_file,
        oidc,
        database,
        azure_storage_account_name,
        ca_cert,
        client_cert,
        client_key,
    } = updates;
    profile.sql_server_url = sql_server_url.or(profile.sql_server_url);
    profile.http_server_url = http_server_url.or(profile.http_server_url);
    if let Some(api_key) = api_key {
        profile.oidc = None;
        profile.api_key = Some(api_key);
        profile.oidc_access_token_file = None;
    } else if let Some(oidc_access_token_file) = oidc_access_token_file {
        profile.oidc = None;
        profile.api_key = None;
        profile.oidc_access_token_file = Some(oidc_access_token_file);
    }
    if let Some(oidc) = oidc {
        profile.api_key = None;
        profile.oidc_access_token_file = None;
        profile.oidc = Some(oidc);
    }
    profile.database = database.or(profile.database);
    profile.azure_storage_account_name =
        azure_storage_account_name.or(profile.azure_storage_account_name);
    profile.ca_cert = ca_cert.or(profile.ca_cert);
    profile.client_cert = client_cert.or(profile.client_cert);
    profile.client_key = client_key.or(profile.client_key);

    config.set_profile(name, profile);
    config.save_default()?;

    println!("Updated profile '{}'", name);
    Ok(())
}

fn prompt_optional(field: &str) -> Result<Option<String>> {
    print!("{}: ", field);
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    let value = input.trim();
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value.to_string()))
    }
}

fn prompt_secret(field: &str) -> Result<Option<String>> {
    struct MaskHelper;
    impl Highlighter for MaskHelper {
        fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
            Cow::Owned("*".repeat(line.len()))
        }
        fn highlight_char(&self, _line: &str, _pos: usize, _forced: bool) -> bool {
            true
        }
    }
    impl rustyline::completion::Completer for MaskHelper {
        type Candidate = String;
    }
    impl rustyline::hint::Hinter for MaskHelper {
        type Hint = String;
    }
    impl rustyline::validate::Validator for MaskHelper {}
    impl Helper for MaskHelper {}

    let mut editor = Editor::new()?;
    editor.set_helper(Some(MaskHelper));
    let input = editor.readline(&format!("{}: ", field))?;
    let value = input.trim();
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value.to_string()))
    }
}
