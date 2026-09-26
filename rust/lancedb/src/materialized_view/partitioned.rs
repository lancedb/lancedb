// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Shared plan/write/commit surface for materialized views with independent units.

use serde::{Deserialize, Serialize};

use super::duplicate_pairs::{self, DuplicatePairsRefreshPlan, WrittenPairsPartition};
use super::{GroupedRefreshPlan, RefreshMaterializedViewResult, StoredDefinition, WrittenUnit};
use crate::table::Table;
use crate::{Error, Result};

/// Serializable work description consumed by a host's existing job workers.
/// The grouped variant preserves the previous saved-plan representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PartitionedRefreshPlan {
    DuplicatePairs(DuplicatePairsRefreshPlan),
    Grouped(GroupedRefreshPlan),
}

impl PartitionedRefreshPlan {
    /// The shared source snapshot and destination generation fence.
    pub fn snapshot(&self) -> &GroupedRefreshPlan {
        match self {
            Self::DuplicatePairs(plan) => &plan.snapshot,
            Self::Grouped(plan) => plan,
        }
    }
    /// Number of independent units in this immutable plan.
    pub fn units(&self) -> u32 {
        self.snapshot().units
    }
}

/// One completed unit's uncommitted fragments. Failed attempts produce no receipt.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WrittenPartition {
    DuplicatePairs(WrittenPairsPartition),
    Grouped(WrittenUnit),
}

impl WrittenPartition {
    /// Number of pair or grouped rows staged by this unit.
    pub fn rows(&self) -> u64 {
        match self {
            Self::DuplicatePairs(unit) => unit.rows,
            Self::Grouped(unit) => unit.rows(),
        }
    }
    /// Uncommitted output files, retained only after task completion.
    pub fn fragments(&self) -> &[lance_table::format::Fragment] {
        match self {
            Self::DuplicatePairs(unit) => &unit.fragments,
            Self::Grouped(unit) => unit.fragments(),
        }
    }
}

/// Plan native pair or indexed grouped work; return `None` for ordinary views.
/// `pinned`, when supplied, must match a native view's declared source version.
pub async fn plan_partitioned_refresh(
    view: &Table,
    pinned: Option<u64>,
) -> Result<Option<PartitionedRefreshPlan>> {
    if let Some(StoredDefinition::Query(definition)) =
        super::read_definition(view.schema().await?.metadata())?
        && definition.duplicate_pairs.is_some()
    {
        return duplicate_pairs::plan_refresh(view, pinned)
            .await
            .map(|p| Some(PartitionedRefreshPlan::DuplicatePairs(p)));
    }
    super::plan_grouped_refresh(view, pinned)
        .await
        .map(|plan| plan.map(PartitionedRefreshPlan::Grouped))
}

/// Stream one unit into uncommitted fragments. Persist the returned receipt only
/// on success. A failed or interrupted call must be retried from the unit's start.
pub async fn write_refresh_partition(
    view: &Table,
    unit: u32,
    plan: &PartitionedRefreshPlan,
) -> Result<WrittenPartition> {
    match plan {
        PartitionedRefreshPlan::DuplicatePairs(plan) => {
            duplicate_pairs::write_unit(view, unit, plan)
                .await
                .map(WrittenPartition::DuplicatePairs)
        }
        PartitionedRefreshPlan::Grouped(plan) => super::write_grouped_unit(view, unit, plan)
            .await
            .map(WrittenPartition::Grouped),
    }
}

/// Publish a complete set of unit receipts through the common MV commit sink.
/// `expected` fences the destination incarnation. No partial set is publishable.
pub async fn commit_partitioned_refresh(
    view: &Table,
    plan: &PartitionedRefreshPlan,
    units: Vec<WrittenPartition>,
    expected: Option<&str>,
) -> Result<RefreshMaterializedViewResult> {
    let foreign = || Error::InvalidInput {
        message: "refresh receipt does not match the plan kind".into(),
    };
    match plan {
        PartitionedRefreshPlan::DuplicatePairs(plan) => {
            let units = units
                .into_iter()
                .map(|unit| match unit {
                    WrittenPartition::DuplicatePairs(unit) => Ok(unit),
                    _ => Err(foreign()),
                })
                .collect::<Result<Vec<_>>>()?;
            duplicate_pairs::commit(view, plan, units, expected).await
        }
        PartitionedRefreshPlan::Grouped(plan) => {
            let units = units
                .into_iter()
                .map(|unit| match unit {
                    WrittenPartition::Grouped(unit) => Ok(unit),
                    _ => Err(foreign()),
                })
                .collect::<Result<Vec<_>>>()?;
            super::commit_grouped_refresh(view, plan, units, expected).await
        }
    }
}
