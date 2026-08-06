// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

extern crate napi_build;

use std::env;

#[path = "build_support/x86_64_v2.rs"]
mod x86_64_v2;

const ENFORCE_BASELINE: &str = "LANCEDB_NODE_ENFORCE_X86_64_V2";

fn main() {
    napi_build::setup();
    println!("cargo:rerun-if-env-changed={ENFORCE_BASELINE}");
    println!("cargo:rerun-if-env-changed=CARGO_ENCODED_RUSTFLAGS");

    let is_linux_x64 = env::var("CARGO_CFG_TARGET_ARCH").as_deref() == Ok("x86_64")
        && env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux");
    let is_release = env::var("PROFILE").as_deref() == Ok("release");
    let is_node_build = env::var(ENFORCE_BASELINE).as_deref() == Ok("1");

    if !is_linux_x64 || (!is_release && !is_node_build) {
        return;
    }

    let encoded_rustflags = env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
    x86_64_v2::validate_encoded_rustflags(&encoded_rustflags).unwrap_or_else(|error| {
        panic!("Linux x64 Node addons must use the x86-64-v2 baseline; {error}")
    });
}
