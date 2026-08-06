// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

// NOTE: we only create one integration test binary, to keep compilation overhead down.

mod count_pushdown;
mod mem_wal;
#[cfg(feature = "slow_tests")]
mod query;
#[cfg(feature = "slow_tests")]
mod utils;
