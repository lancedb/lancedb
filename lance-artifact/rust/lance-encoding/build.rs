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
    prost_build.protoc_arg("--experimental_allow_proto3_optional");
    prost_build.enable_type_names();
    prost_build.bytes(["."]); // Enable Bytes type for all messages to avoid Vec clones.
    prost_build.compile_protos(&["./protos/encodings_v2_0.proto"], &["./protos"])?;
    prost_build.compile_protos(&["./protos/encodings_v2_1.proto"], &["./protos"])?;

    Ok(())
}
