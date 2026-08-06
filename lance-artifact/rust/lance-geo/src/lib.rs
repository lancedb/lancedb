// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use datafusion::prelude::SessionContext;

#[cfg(feature = "geo")]
pub mod bbox;

pub fn register_functions(ctx: &SessionContext) {
    #[cfg(feature = "geo")]
    geodatafusion::register(ctx);
    #[cfg(not(feature = "geo"))]
    let _ = ctx;
}
