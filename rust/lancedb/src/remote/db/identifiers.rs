// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

pub(super) fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// The path segment addressing one object: its namespace path and its name.
///
/// One builder for tables, Secrets, Functions and materialized views: the
/// identifier grammar belongs to the namespace spec, not to an object type. An
/// empty path addresses an object with no namespace.
///
/// Components are checked for addressability, not a character set. The name's
/// grammar is the caller's, so a table reports [`Error::InvalidTableName`], a
/// Function admits names a table may not, and a catalog database carries the
/// `/` that [`RemoteCatalog`] allows.
///
/// [`RemoteCatalog`]: super::catalog::RemoteCatalog
pub(super) fn build_object_identifier(what: &str, name: &str, namespace: &[String]) -> Result<String> {
    for segment in namespace {
        reject_unaddressable_component("namespace segment", segment)?;
    }
    reject_unaddressable_component(what, name)?;
    Ok(join_identifier(
        namespace.iter().map(String::as_str).chain([name]),
    ))
}

/// What a component may not be if the join is to survive being split back
/// apart: empty, a segment URL parsing resolves away, or the delimiter itself.
///
/// Each erases a boundary no encoding of the joined form recovers. `["prod",
/// ""]` joins to `prod$`, which reads back as `["prod"]`, so a drop reaches the
/// parent of the namespace the caller named.
///
/// Not a character set: per-component percent-encoding makes the wider set
/// safe, since a `/` in a name reaches the service as `%2F`, still one
/// segment.
pub(super) fn reject_unaddressable_component(what: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Error::InvalidInput {
            message: format!(
                "{what} must not be empty: the identifier would carry two delimiters in a row, \
                 and splitting it back apart would name a different object"
            ),
        });
    }
    reject_relative_segment(what, value)?;
    if value.contains(ID_DELIMITER) {
        return Err(Error::InvalidInput {
            message: format!(
                "{what} '{value}' contains the identifier delimiter '{ID_DELIMITER}', so the \
                 namespace path and the name it joins could not be told apart"
            ),
        });
    }
    Ok(())
}

/// The path segment addressing one table. A wrapper for the error type:
/// callers match on [`Error::InvalidTableName`].
pub(super) fn build_table_identifier(name: &str, namespace: &[String]) -> Result<String> {
    validate_table_name(name)?;
    build_object_identifier("table name", name, namespace)
}

/// Join components into the `{id}` a route addresses: each percent-encoded,
/// then joined by the delimiter.
///
/// Per component rather than over the joined string, so the delimiter stays a
/// delimiter and nothing inside a component can end the path segment.
///
/// A second line, not the first: a component from the object charset is all
/// unreserved and encodes to itself, so the route reads as the caller wrote it.
/// It does not cover `.` and `..`, which are unreserved too and resolve away
/// after decoding -- [`build_object_identifier`] refuses those.
pub(super) fn join_identifier<'a>(components: impl Iterator<Item = &'a str>) -> String {
    components
        .map(|component| urlencoding::encode(component).into_owned())
        .collect::<Vec<_>>()
        .join(ID_DELIMITER)
}

/// The path segment addressing one namespace.
pub(super) fn build_namespace_identifier(namespace: &[String]) -> Result<String> {
    for segment in namespace {
        reject_unaddressable_component("namespace segment", segment)?;
    }
    if namespace.is_empty() {
        // According to the namespace spec, use delimiter to represent root namespace
        return Ok(ID_DELIMITER.to_string());
    }
    Ok(join_identifier(namespace.iter().map(String::as_str)))
}

/// Build a secure cache key using length prefixes.
/// This format is completely unambiguous regardless of delimiter or content.
/// Format: [u32_len][namespace1][u32_len][namespace2]...[u32_len][table_name]
/// Returns a hex-encoded string for use as a cache key.
pub(super) fn build_cache_key(name: &str, namespace: &[String]) -> String {
    let mut key = Vec::new();

    // Add each namespace component with length prefix
    for ns in namespace {
        let bytes = ns.as_bytes();
        key.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        key.extend_from_slice(bytes);
    }

    // Add table name with length prefix
    let name_bytes = name.as_bytes();
    key.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
    key.extend_from_slice(name_bytes);

    // Convert to hex string for use as a cache key
    key.iter().map(|b| format!("{:02x}", b)).collect()
}
