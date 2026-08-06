// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#[cfg(feature = "substrait")]
mod dataset_aggregate;
mod dataset_common;
mod dataset_concurrency_store;
#[cfg(feature = "geo")]
mod dataset_geo;
mod dataset_index;
mod dataset_io;
mod dataset_merge_update;
mod dataset_migrations;
mod dataset_overlay_index_masking;
mod dataset_scanner;
mod dataset_schema_evolution;
mod dataset_transactions;
mod dataset_versioning;
mod fragment_validate_tombstones;
