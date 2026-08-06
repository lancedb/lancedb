// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::env;
use std::io::Result;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=protos");
    // Cache-entry protos are library-internal serialization, not part of the
    // on-disk format spec, so they live here rather than in the shared `protos/`.
    println!("cargo:rerun-if-changed=protos-cache");

    #[cfg(feature = "protoc")]
    // Use vendored protobuf compiler if requested.
    unsafe {
        std::env::set_var("PROTOC", protobuf_src::protoc());
    }

    let mut prost_build = prost_build::Config::new();
    prost_build.protoc_arg("--experimental_allow_proto3_optional");
    prost_build.enable_type_names();
    prost_build.compile_protos(
        &[
            "./protos/index.proto",
            "./protos/index_old.proto",
            "./protos-cache/cache.proto",
        ],
        &["./protos", "./protos-cache"],
    )?;

    let rust_toolchain = env::var("RUSTUP_TOOLCHAIN")
        .or_else(|e| match e {
            env::VarError::NotPresent => Ok("stable".into()),
            e => Err(e),
        })
        .unwrap();
    if rust_toolchain.starts_with("nightly") {
        // enable the 'nightly' feature flag
        println!("cargo:rustc-cfg=feature=\"nightly\"");
    }

    Ok(())
}
