// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Execution nodes
//!
//! WARNING: Internal API with no stability guarantees.

#[cfg(feature = "substrait")]
pub mod ann_proto;
pub mod count_from_mask;
pub mod count_pushdown;
mod filter;
pub mod filtered_read;
#[cfg(feature = "substrait")]
pub mod filtered_read_proto;
pub mod fts;
pub(crate) mod knn;
mod optimizer;
mod projection;
mod pushdown_scan;
mod rowids;
pub mod scalar_index;
mod scan;
#[cfg(feature = "substrait")]
pub mod table_identifier;
mod take;
#[cfg(test)]
pub mod testing;
pub mod utils;

pub use filter::LanceFilterExec;
pub use knn::{ANNIvfPartitionExec, ANNIvfSubIndexExec, KNNVectorDistanceExec};
pub use lance_datafusion::planner::Planner;
pub use lance_index::scalar::expression::FilterPlan;
pub use optimizer::get_physical_optimizer;
pub use projection::project;
pub use pushdown_scan::{LancePushdownScanExec, ScanConfig};
pub use rowids::{AddRowAddrExec, AddRowOffsetExec};
pub use scan::{LanceScanConfig, LanceScanExec};
pub use take::TakeExec;
pub use utils::PreFilterSource;

/// Declares which vesrion of the relational algebra we are producing
///
/// In order to enable plan pushdown (executing parts of the physical plan on different nodes)
/// we must treat the physical plan serialization and the inputs and outputs of phyical plan
/// nodes as part of the public API surface.
///
/// We should attempt to handle as many versions as possible on read paths.  This variable
/// controls which versions we support producing.
///
/// This includes changes to the proto serialization of physical plan nodes themselves or
/// changes to the format of the inputs or outputs of the nodes.
///
/// This does not include changes to the behavior of the nodes unless that behavior is a change
/// in semantic meaning.
pub const LANCE_RELATIONAL_ALGEBRA_VERSION: u32 = 1;
