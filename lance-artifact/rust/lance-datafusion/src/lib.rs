// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

pub mod aggregate;
pub mod chunker;
pub mod dataframe;
pub mod datagen;
pub mod exec;
pub mod expr;
pub mod logical_expr;
pub mod planner;
pub mod projection;
pub mod pb {
    #![allow(clippy::all)]
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(unused)]
    #![allow(improper_ctypes)]
    #![allow(clippy::upper_case_acronyms)]
    #![allow(clippy::use_self)]
    include!(concat!(env!("OUT_DIR"), "/lance.datafusion.rs"));
}
pub mod spill;
pub mod sql;
#[cfg(feature = "substrait")]
pub mod substrait;
pub mod udf;
pub mod utils;
