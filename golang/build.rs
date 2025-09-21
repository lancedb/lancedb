// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::env;

fn main() {
    let _package_name = env::var("CARGO_PKG_NAME").unwrap();
    let out_dir = env::var("OUT_DIR").unwrap();

    // Generate C header using cbindgen
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    
    // Override C++ standard library on macOS
    if cfg!(target_os = "macos") {
        println!("cargo:rustc-link-arg=-stdlib=libc++");
        println!("cargo:rustc-link-lib=c++");
        
        // Set environment variables for C/C++ compilation
        println!("cargo:rustc-env=MACOSX_DEPLOYMENT_TARGET=11.0");
        
        // Override any attempts to link with libstdc++
        println!("cargo:rustc-link-arg=-Wl,-undefined,dynamic_lookup");
    }

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_language(cbindgen::Language::C)
        .with_include_guard("LANCEDB_H")
        .with_include_version(true)
        .with_documentation(true)
        .with_pragma_once(true)
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(format!("{}/lancedb.h", out_dir));

    println!("cargo:rerun-if-changed=src/");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=cbindgen.toml");
}