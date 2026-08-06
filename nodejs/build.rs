// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

extern crate napi_build;

use std::env;

const ENFORCE_BASELINE: &str = "LANCEDB_NODE_ENFORCE_X86_64_V2";
const X86_64_V2_FEATURES: [&str; 9] = [
    "cmpxchg16b",
    "fxsr",
    "popcnt",
    "sse",
    "sse2",
    "sse3",
    "sse4.1",
    "sse4.2",
    "ssse3",
];

fn main() {
    napi_build::setup();
    println!("cargo:rerun-if-env-changed={ENFORCE_BASELINE}");

    let is_linux_x64 = env::var("CARGO_CFG_TARGET_ARCH").as_deref() == Ok("x86_64")
        && env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux");
    let is_release = env::var("PROFILE").as_deref() == Ok("release");
    let is_node_build = env::var(ENFORCE_BASELINE).as_deref() == Ok("1");

    if !is_linux_x64 || (!is_release && !is_node_build) {
        return;
    }

    let target_features = env::var("CARGO_CFG_TARGET_FEATURE").unwrap_or_default();
    let features_above_v2 = target_features
        .split(',')
        .filter(|feature| !feature.is_empty() && !X86_64_V2_FEATURES.contains(feature))
        .collect::<Vec<_>>();

    assert!(
        features_above_v2.is_empty(),
        "Linux x64 Node addons must use the x86-64-v2 baseline; features above v2: {}",
        features_above_v2.join(", ")
    );
}
