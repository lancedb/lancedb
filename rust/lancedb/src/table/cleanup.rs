// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Options and results for durable remote version cleanup.

/// Per-invocation retention. Omission inherits table/branch then deployment policy.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct CleanupOptions {
    /// Minimum old-version age in seconds. None inherits server policy.
    pub min_age_seconds: Option<u64>,
}

impl CleanupOptions {
    /// Inherit selected table/branch and deployment retention.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the minimum old-version age in seconds; zero is explicit, not omitted.
    pub fn min_age_seconds(mut self, seconds: u64) -> Self {
        self.min_age_seconds = Some(seconds);
        self
    }
}

/// Successful remote version cleanup. Auxiliary sweeps are best effort and
/// excluded from the counts. Retry/recovery can leave counts unknown (`None`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "CleanupResultWire")]
#[non_exhaustive]
pub struct CleanupResult {
    /// Objects that could not be deleted, if measured. Other removals remain committed.
    pub failed_deletes: Option<u64>,
    /// Public result schema version; currently 1.
    pub format_version: u32,
    /// Retention resolved and recorded when the server accepted this job.
    pub min_age_seconds: u64,
    /// Whether the entire invocation's version-deletion counts are known.
    pub statistics_complete: bool,
    /// Old versions removed, or None after uncertain retry/recovery.
    pub old_versions: Option<u64>,
    /// Bytes removed by version deletion, excluding auxiliary sweeps.
    pub bytes_removed: Option<u64>,
}

#[derive(serde::Deserialize)]
struct CleanupResultWire {
    failed_deletes: Option<u64>,
    format_version: u32,
    min_age_seconds: u64,
    statistics_complete: bool,
    old_versions: Option<u64>,
    bytes_removed: Option<u64>,
}

impl TryFrom<CleanupResultWire> for CleanupResult {
    type Error = &'static str;
    fn try_from(value: CleanupResultWire) -> std::result::Result<Self, Self::Error> {
        if value.format_version != 1
            || if value.statistics_complete {
                value.old_versions.is_none() || value.bytes_removed.is_none()
            } else {
                value.old_versions.is_some()
                    || value.bytes_removed.is_some()
                    || value.failed_deletes.is_some()
            }
        {
            return Err("unsupported or inconsistent cleanup result");
        }
        Ok(Self {
            failed_deletes: value.failed_deletes,
            format_version: value.format_version,
            min_age_seconds: value.min_age_seconds,
            statistics_complete: value.statistics_complete,
            old_versions: value.old_versions,
            bytes_removed: value.bytes_removed,
        })
    }
}

/// Read-only version-deletion estimate. Execution re-evaluates current state;
/// no job or deletion snapshot is reserved and auxiliary sweeps are excluded.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct CleanupPlan {
    #[serde(deserialize_with = "preview_version")]
    pub format_version: u32,
    pub source_version: u64,
    pub min_age_seconds: u64,
    pub cutoff_millis: i64,
    pub versions_to_remove: u64,
    pub bytes_to_remove: u64,
}

fn preview_version<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<u32, D::Error> {
    let version = <u32 as serde::Deserialize>::deserialize(deserializer)?;
    if version != 1 {
        return Err(serde::de::Error::custom(
            "unsupported cleanup preview format",
        ));
    }
    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_result_distinguishes_known_zero_unknown_and_invalid() {
        let mut value = serde_json::json!({"format_version":1,"min_age_seconds":0,
            "statistics_complete":true,"old_versions":0,"bytes_removed":0,"future":true});
        assert_eq!(
            serde_json::from_value::<CleanupResult>(value.clone())
                .unwrap()
                .old_versions,
            Some(0)
        );
        value["statistics_complete"] = false.into();
        assert!(serde_json::from_value::<CleanupResult>(value.clone()).is_err());
        value["old_versions"] = serde_json::Value::Null;
        value["bytes_removed"] = serde_json::Value::Null;
        assert_eq!(
            serde_json::from_value::<CleanupResult>(value.clone())
                .unwrap()
                .old_versions,
            None
        );
        value["format_version"] = 2.into();
        assert!(serde_json::from_value::<CleanupResult>(value).is_err());
    }
}
