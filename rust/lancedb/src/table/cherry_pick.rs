// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Types for remote branch diff / cherry-pick onto main.

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ColumnSummary {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ColumnChange {
    pub name: String,
    pub main: ColumnSummary,
    pub branch: ColumnSummary,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct IndexSummary {
    pub index_name: String,
    pub columns: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_type: Option<String>,
    pub status: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RowCountSummary {
    pub unchanged: u64,
    pub new_on_base: u64,
    pub new_on_branch: u64,
    pub stale_recompute: u64,
    pub inputs_changed: u64,
    pub delta_available: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CherryPickErrorCode {
    BaseMoved,
    RowCountMismatch,
    RowsChanged,
    ColumnRemoved,
    ColumnChanged,
    NothingToApply,
    NoColumnChanges,
    InputColumnDependency,
    ParentNotMain,
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CherryPickError {
    pub code: CherryPickErrorCode,
    pub message: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BranchDiff {
    pub from_branch: String,
    pub parent_version: u64,
    pub main_version: u64,
    pub branch_version: u64,
    pub base_moved: bool,
    pub row_count_main: u64,
    pub row_count_branch: u64,
    pub row_summary: RowCountSummary,
    pub added_columns: Vec<ColumnSummary>,
    pub removed_columns: Vec<ColumnSummary>,
    pub changed_columns: Vec<ColumnChange>,
    pub added_indexes: Vec<IndexSummary>,
    pub removed_indexes: Vec<IndexSummary>,
    pub errors: Vec<CherryPickError>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CherryPickPreview {
    #[serde(default)]
    pub promoted_columns: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CherryPickStatus {
    Ready,
    Failed,
    NotImplemented,
    CherryPicked,
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CherryPickResult {
    pub status: CherryPickStatus,
    pub diff: BranchDiff,
    pub preview: CherryPickPreview,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub main_version_after: Option<u64>,
}
