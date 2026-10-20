// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::process::Command;

#[test]
fn no_arguments_print_help_without_creating_configuration() {
    let home = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_lancedb"))
        .env("HOME", home.path())
        .env("LANCEDB_API_KEY", "secret-that-must-not-appear")
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Usage:"));
    for command in ["auth", "acl", "profile", "sql", "job", "load"] {
        assert!(stdout.contains(command));
    }
    assert!(!stdout.contains("secret-that-must-not-appear"));
    assert!(!home.path().join(".lancedb").exists());
}

#[test]
fn excluded_commands_and_legacy_sql_flags_are_rejected() {
    for args in [
        vec!["server"],
        vec!["management"],
        vec!["job", "usage"],
        vec!["--command", "SELECT 1"],
        vec!["--file", "queries.sql"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_lancedb"))
            .args(args)
            .output()
            .unwrap();
        assert!(!output.status.success());
    }
}

#[test]
fn profile_updates_preserve_enterprise_data_directory() {
    use lancedb_cli::server_config::{Config, Profile};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(
        &path,
        "data_dir = '/enterprise/data'\ndefault_profile = 'local'\n",
    )
    .unwrap();
    let mut config = Config::load_from_path(&path).unwrap();
    config.set_profile(
        "cloud",
        Profile {
            database: Some("analytics".into()),
            ..Default::default()
        },
    );
    config.save_to_path(&path).unwrap();
    let saved = Config::load_from_path(&path).unwrap();
    assert_eq!(saved.data_dir.as_deref(), Some("/enterprise/data"));
    assert_eq!(
        saved.get_profile(Some("cloud")).database.as_deref(),
        Some("analytics")
    );
}
