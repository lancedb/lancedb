// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::io::Result;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=protos");

    #[cfg(feature = "protoc")]
    // Use vendored protobuf compiler if requested.
    unsafe {
        std::env::set_var("PROTOC", protobuf_src::protoc());
    }

    let mut prost_build = prost_build::Config::new();
    prost_build.extern_path(".lance.table", "::lance_table::format::pb");
    prost_build.extern_path(".lance.index.pb", "::lance_index::pb");
    prost_build.extern_path(".lance.datafusion", "::lance_datafusion::pb");
    prost_build.protoc_arg("--experimental_allow_proto3_optional");
    prost_build.enable_type_names();
    prost_build.compile_protos(&["./protos/ann.proto"], &["./protos"])?;

    Ok(())
}
