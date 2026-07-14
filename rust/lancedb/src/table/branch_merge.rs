// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Types for remote branch diff / merge against main.

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
pub enum MergeBlockerCode {
    BaseMoved,
    RowCountMismatch,
    RowsChanged,
    ColumnRemoved,
    ColumnChanged,
    NoMergeableChanges,
    NoColumnChanges,
    InputColumnDependency,
    ParentNotMain,
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MergeBlocker {
    pub code: MergeBlockerCode,
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
    pub mergeable: bool,
    pub merge_blockers: Vec<MergeBlocker>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MergePreview {
    #[serde(default)]
    pub promoted_columns: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum MergeBranchStatus {
    Ready,
    Rejected,
    NotImplemented,
    Merged,
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MergeBranchResult {
    pub status: MergeBranchStatus,
    pub diff: BranchDiff,
    pub preview: MergePreview,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub main_version_after: Option<u64>,
}
