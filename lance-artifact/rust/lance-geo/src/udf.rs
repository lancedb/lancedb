// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use datafusion::prelude::SessionContext;

/// Register UDF functions to datafusion context.
pub fn register_functions(ctx: &SessionContext) {
    ctx.register_udf(geodatafusion::udf::geo::measurement::Area::new().into());
    ctx.register_udf(geodatafusion::udf::geo::measurement::Distance::new().into());
    ctx.register_udf(geodatafusion::udf::geo::measurement::Length::new().into());
    ctx.register_udf(geodatafusion::udf::geo::relationships::Contains::new().into());
    ctx.register_udf(geodatafusion::udf::geo::relationships::CoveredBy::new().into());
    ctx.register_udf(geodatafusion::udf::geo::relationships::Covers::new().into());
    ctx.register_udf(geodatafusion::udf::geo::relationships::Disjoint::new().into());
    ctx.register_udf(geodatafusion::udf::geo::relationships::Intersects::new().into());
    ctx.register_udf(geodatafusion::udf::geo::relationships::Overlaps::new().into());
    ctx.register_udf(geodatafusion::udf::geo::relationships::Touches::new().into());
    ctx.register_udf(geodatafusion::udf::geo::relationships::Within::new().into());
    ctx.register_udf(geodatafusion::udf::geo::validation::IsValid::new().into());
}
