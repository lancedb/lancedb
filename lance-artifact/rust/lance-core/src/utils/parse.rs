// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

/// Parse a string into a boolean value.
pub fn str_is_truthy(val: &str) -> bool {
    val.eq_ignore_ascii_case("1")
        | val.eq_ignore_ascii_case("true")
        | val.eq_ignore_ascii_case("on")
        | val.eq_ignore_ascii_case("yes")
        | val.eq_ignore_ascii_case("y")
}

/// Parse an environment variable as a truthy-only boolean.
///
/// Returns `default_value` if the env var is not set.
/// Returns `true` only for truthy values (1/true/on/yes/y, case-insensitive).
/// Returns `false` for all other set values.
pub fn parse_env_as_bool(env_var_name: &str, default_value: bool) -> bool {
    std::env::var(env_var_name)
        .ok()
        .map(|value| str_is_truthy(value.trim()))
        .unwrap_or(default_value)
}
