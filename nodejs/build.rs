// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

extern crate napi_build;

use std::env;

const ENFORCE_BASELINE: &str = "LANCEDB_NODE_ENFORCE_X86_64_V2";
const UNSUPPORTED_FEATURES: [&str; 4] = ["avx", "avx2", "fma", "f16c"];

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
    let enabled_features = target_features.split(',').collect::<Vec<_>>();
    let leaked_features = UNSUPPORTED_FEATURES
        .iter()
        .filter(|feature| enabled_features.contains(feature))
        .copied()
        .collect::<Vec<_>>();

    assert!(
        leaked_features.is_empty(),
        "Linux x64 Node addons must use the x86-64-v2 baseline; unsupported target features: {}",
        leaked_features.join(", ")
    );
}
