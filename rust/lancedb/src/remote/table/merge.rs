// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[derive(Serialize, Clone, Debug)]
pub struct MergeInsertRequest {
    // Sent as one repeated `on` query parameter per column, which is how the
    // namespace spec encodes an array-valued `on`. serde_urlencoded (which
    // reqwest's `query()` uses) cannot serialize a sequence nested in a struct,
    // so this field is emitted separately by [`Self::on_query_params`].
    #[serde(skip_serializing)]
    pub(super) on: Vec<String>,
    pub(super) when_matched_update_all: bool,
    pub(super) when_matched_update_all_filt: Option<String>,
    pub(super) when_not_matched_insert_all: bool,
    pub(super) when_not_matched_by_source_delete: bool,
    pub(super) when_not_matched_by_source_delete_filt: Option<String>,
    // For backwards compatibility, only serialize use_index when it's false
    // (the default is true)
    #[serde(skip_serializing_if = "is_true")]
    pub(super) use_index: bool,
    // Only serialize use_lsm when explicitly set (Some); a server that predates
    // it ignores the field and routes as it would by default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) use_lsm: Option<bool>,
}

impl MergeInsertRequest {
    /// The `on` columns as repeated query parameters: `?on=a&on=b`.
    ///
    /// A single column serializes to `?on=a`, exactly what clients sent before
    /// `on` became a list, so a server that predates composite keys sees no
    /// change from a single-column caller.
    pub(crate) fn on_query_params(&self) -> Vec<(&str, &str)> {
        self.on.iter().map(|col| ("on", col.as_str())).collect()
    }
}

pub(super) fn is_true(b: &bool) -> bool {
    *b
}

impl TryFrom<MergeInsertBuilder> for MergeInsertRequest {
    type Error = Error;

    fn try_from(value: MergeInsertBuilder) -> Result<Self> {
        if value.on.is_empty() {
            return Err(Error::InvalidInput {
                message: "MergeInsertBuilder missing required 'on' field".into(),
            });
        }
        // The server rejects a repeated column with a 400; catching it here
        // names the offending column and costs no round trip.
        let mut seen = HashSet::with_capacity(value.on.len());
        if let Some(dup) = value.on.iter().find(|col| !seen.insert(*col)) {
            return Err(Error::InvalidInput {
                message: format!("MergeInsertBuilder 'on' column '{dup}' is repeated"),
            });
        }

        let when_matched_update_all_filt = match value.when_matched_update_all_filt {
            Some(MergeFilter::Sql(sql)) => Some(sql),
            Some(MergeFilter::Expr(_)) => {
                return Err(Error::NotSupported {
                    message: "DataFusion expressions are not supported on remote tables".into(),
                });
            }
            None => None,
        };

        let when_not_matched_by_source_delete_filt =
            match value.when_not_matched_by_source_delete_filt {
                Some(MergeFilter::Sql(sql)) => Some(sql),
                Some(MergeFilter::Expr(_)) => {
                    return Err(Error::NotSupported {
                        message: "DataFusion expressions are not supported on remote tables".into(),
                    });
                }
                None => None,
            };

        Ok(Self {
            on: value.on,
            when_matched_update_all: value.when_matched_update_all,
            when_matched_update_all_filt,
            when_not_matched_insert_all: value.when_not_matched_insert_all,
            when_not_matched_by_source_delete: value.when_not_matched_by_source_delete,
            when_not_matched_by_source_delete_filt,
            // Only serialize use_index when it's false for backwards compatibility
            use_index: value.use_index,
            use_lsm: value.use_lsm,
        })
    }
}
