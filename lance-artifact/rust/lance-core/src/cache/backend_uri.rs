// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! URI-based configuration for cache backends.
//!
//! [`build_from_uri`] parses a compact string form such as
//! `moka://?capacity=1073741824` into a [`BackendConfig`] and hands it to
//! the registry. This gives Python/Java bindings and configuration files a
//! single-string representation of a backend without having to expose a
//! typed builder for every backend.
//!
//! Grammar (intentionally a subset of RFC 3986 — Lance only needs a
//! predictable, unambiguous form):
//!
//! ```text
//! uri     ::= scheme ":" hier ( "?" query )?
//! scheme  ::= ALPHA ( ALPHA | DIGIT | "+" | "-" | "." )*
//! hier    ::= "//" authority path?   -- e.g. moka://?..., other:///path?...
//!           | path                    -- e.g. moka:capacity=...  (rare)
//! authority ::= *( any char except "/" | "?" )
//! path    ::= *( any char except "?" )
//! query   ::= pair ( "&" pair )*
//! pair    ::= key "=" value          -- both percent-decoded
//! ```
//!
//! Mapping to [`BackendConfig`]:
//!
//! * `scheme` becomes `kind`.
//! * The joined `authority + path` (with any leading `//` stripped) is stored
//!   under the option key `path` when non-empty. Empty-authority absolute
//!   paths such as `backend:///tmp/cache` keep their leading `/`; host-style
//!   paths such as `backend://localhost:6379/0` are stored as
//!   `localhost:6379/0`. If the query already contains a `path` key, the
//!   URI-supplied path wins and the parser errors on the conflict.
//! * Each `key=value` pair from the query becomes an entry in `options`.
//!   Duplicate keys are rejected.

use std::sync::Arc;

use super::backend::CacheBackend;
use super::registry::{BackendConfig, build_from_config, normalize_backend_kind};
use crate::{Error, Result};

/// Parse `uri` into a [`BackendConfig`] and build the backend registered
/// under its `scheme`.
///
/// Returns an error if:
/// * the URI cannot be parsed,
/// * no backend is registered for that scheme, or
/// * the constructor itself fails.
pub fn build_from_uri(uri: &str) -> Result<Arc<dyn CacheBackend>> {
    let config = parse_backend_uri(uri)?;
    build_from_config(&config)
}

/// Parse `uri` into a [`BackendConfig`] without touching the registry.
///
/// See the module docs for the accepted grammar.
pub fn parse_backend_uri(uri: &str) -> Result<BackendConfig> {
    let (scheme, rest) = split_scheme(uri)?;
    let (path, query) = split_path_query(rest);

    let mut config = BackendConfig::new(&scheme)?;

    let normalized_path = normalize_path(path);
    if !normalized_path.is_empty() {
        config.options.insert("path".to_string(), normalized_path);
    }

    if let Some(query) = query {
        for raw_pair in query.split('&') {
            if raw_pair.is_empty() {
                continue;
            }
            let (raw_key, raw_value) = raw_pair.split_once('=').ok_or_else(|| {
                Error::invalid_input(format!(
                    "cache backend uri {:?}: query pair {:?} is missing '='",
                    uri, raw_pair
                ))
            })?;
            let key = percent_decode(raw_key).map_err(|err| {
                Error::invalid_input(format!(
                    "cache backend uri {:?}: cannot decode query key {:?}: {}",
                    uri, raw_key, err
                ))
            })?;
            let value = percent_decode(raw_value).map_err(|err| {
                Error::invalid_input(format!(
                    "cache backend uri {:?}: cannot decode query value {:?}: {}",
                    uri, raw_value, err
                ))
            })?;
            if config.options.contains_key(&key) {
                return Err(Error::invalid_input(format!(
                    "cache backend uri {:?}: option {:?} is set more than once",
                    uri, key
                )));
            }
            config.options.insert(key, value);
        }
    }

    Ok(config)
}

fn split_scheme(uri: &str) -> Result<(String, &str)> {
    let colon = uri.find(':').ok_or_else(|| {
        Error::invalid_input(format!("cache backend uri {:?} is missing ':'", uri))
    })?;
    let scheme = &uri[..colon];
    let scheme = normalize_backend_kind(scheme)
        .map_err(|err| Error::invalid_input(format!("cache backend uri {:?}: {}", uri, err)))?;
    Ok((scheme, &uri[colon + 1..]))
}

fn split_path_query(rest: &str) -> (&str, Option<&str>) {
    match rest.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (rest, None),
    }
}

/// Strip leading `//authority/` boilerplate and return the useful path
/// component. Empty authorities (e.g. `moka://`) yield an empty path, while
/// empty-authority absolute paths (e.g. `disk:///tmp/cache`) retain their
/// leading slash.
fn normalize_path(raw: &str) -> String {
    let Some(without_marker) = raw.strip_prefix("//") else {
        return raw.to_string();
    };
    if without_marker.is_empty() {
        return String::new();
    }
    without_marker.to_string()
}

fn percent_decode(input: &str) -> std::result::Result<String, String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                if i + 2 >= bytes.len() {
                    return Err(format!("truncated percent-escape at offset {}", i));
                }
                let hi = decode_hex_digit(bytes[i + 1])?;
                let lo = decode_hex_digit(bytes[i + 2])?;
                out.push((hi << 4) | lo);
                i += 3;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|err| err.to_string())
}

fn decode_hex_digit(b: u8) -> std::result::Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(10 + b - b'a'),
        b'A'..=b'F' => Ok(10 + b - b'A'),
        _ => Err(format!(
            "invalid hex digit {:?} in percent-escape",
            b as char
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_authority_only() {
        let cfg = parse_backend_uri("moka://?capacity=1073741824").unwrap();
        assert_eq!(cfg.kind, "moka");
        assert_eq!(
            cfg.options.get("capacity").map(String::as_str),
            Some("1073741824")
        );
        assert!(!cfg.options.contains_key("path"));
    }

    #[test]
    fn test_parse_path_and_query() {
        let cfg = parse_backend_uri("example:///var/lance/cache?capacity=10G").unwrap();
        assert_eq!(cfg.kind, "example");
        assert_eq!(
            cfg.options.get("path").map(String::as_str),
            Some("/var/lance/cache")
        );
        assert_eq!(cfg.options.get("capacity").map(String::as_str), Some("10G"));
    }

    #[test]
    fn test_parse_host_style() {
        // Redis-style URI with host:port + path segment. All of it lives
        // under the "path" option; the backend is responsible for
        // interpreting it.
        let cfg = parse_backend_uri("redis://localhost:6379/0?prefix=lance").unwrap();
        assert_eq!(cfg.kind, "redis");
        assert_eq!(
            cfg.options.get("path").map(String::as_str),
            Some("localhost:6379/0"),
        );
        assert_eq!(cfg.options.get("prefix").map(String::as_str), Some("lance"));
    }

    #[test]
    fn test_scheme_is_lowercased() {
        // Different upper/lower cases must resolve to the same registry
        // key, otherwise `Moka://` and `moka://` would look up different
        // backends.
        let cfg = parse_backend_uri("MOKA://?capacity=1").unwrap();
        assert_eq!(cfg.kind, "moka");
    }

    #[test]
    fn test_percent_decoding() {
        let cfg = parse_backend_uri("kv://?prefix=a%2Fb&name=hello%20world&token=a+b%2Bc").unwrap();
        assert_eq!(cfg.options.get("prefix").map(String::as_str), Some("a/b"));
        assert_eq!(
            cfg.options.get("name").map(String::as_str),
            Some("hello world")
        );
        assert_eq!(cfg.options.get("token").map(String::as_str), Some("a+b+c"));
    }

    #[test]
    fn test_empty_query_pair_is_skipped() {
        // A trailing "&" should not cause a spurious "" pair to appear.
        let cfg = parse_backend_uri("moka://?capacity=1&").unwrap();
        assert_eq!(cfg.options.len(), 1);
    }

    #[test]
    fn test_missing_scheme_errors() {
        let err = parse_backend_uri("no-scheme-here").unwrap_err();
        assert!(err.to_string().contains("missing ':'"));
    }

    #[test]
    fn test_invalid_scheme_errors() {
        // Digit-leading schemes are invalid per RFC 3986 and would clash
        // with URI-like values elsewhere in the config.
        let err = parse_backend_uri("1moka://").unwrap_err();
        assert!(err.to_string().contains("must start with an ASCII letter"));
    }

    #[test]
    fn test_duplicate_option_errors() {
        let err = parse_backend_uri("moka://?capacity=1&capacity=2").unwrap_err();
        assert!(err.to_string().contains("more than once"));
    }

    #[test]
    fn test_query_pair_without_equals_errors() {
        let err = parse_backend_uri("moka://?capacity").unwrap_err();
        assert!(err.to_string().contains("missing '='"));
    }
}
