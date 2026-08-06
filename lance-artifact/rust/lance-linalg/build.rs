// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::env;

fn main() -> Result<(), String> {
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

    // Let clippy know about our custom cfg attribute
    println!(
        "cargo::rustc-check-cfg=cfg(kernel_support, values(\"avx512_f16\", \"avx512_bf16\", \"avx512_dist_table\"))"
    );

    println!("cargo:rerun-if-changed=src/simd/f16.c");
    println!("cargo:rerun-if-changed=src/simd/bf16.c");
    println!("cargo:rerun-if-changed=src/simd/dist_table.c");

    // Important: we don't use `cfg!(target_arch)` here because that is the target_arch
    // for the build script, not the target_arch for the library. Similar story for
    // target_os.
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();

    if target_os == "windows" {
        println!(
            "cargo:warning=fp16 kernels are not supported on Windows. Skipping compilation of kernels."
        );
        return Ok(());
    }

    if target_arch == "aarch64" && target_os == "macos" {
        // Build a version with NEON
        build_f16_with_flags("neon", &["-mtune=apple-m1"]).unwrap();
        build_bf16_with_flags("neon", &["-mtune=apple-m1"]).unwrap();
    } else if target_arch == "aarch64" && target_os == "ios" {
        // Build version with NEON
        // A13 bionic is the earliest supported iOS SOC
        build_f16_with_flags("neon", &["-mtune=apple-a13"]).unwrap();
        build_bf16_with_flags("neon", &["-mtune=apple-a13"]).unwrap();
    } else if target_arch == "aarch64" && (target_os == "linux" || target_os == "android") {
        // Build a version with NEON
        build_f16_with_flags("neon", &["-march=armv8.2-a+fp16"]).unwrap();
        build_bf16_with_flags("neon", &["-march=armv8.2-a+fp16"]).unwrap();
    } else if target_arch == "x86_64" {
        // Build a version with AVX512
        if let Err(err) = build_f16_with_flags("avx512", &["-march=sapphirerapids", "-mavx512fp16"])
        {
            // It's likely the compiler doesn't support the sapphirerapids architecture
            // Clang 12 and GCC 11 are the first versions with sapphire rapids support
            println!(
                "cargo:warning=Skipping build of AVX-512 fp16 kernels. Error: {}",
                err
            );
        } else if cfg!(feature = "fp16kernels") {
            // We create a special cfg so that we can detect we have in fact
            // generated the AVX512 version of the f16 kernels.
            println!("cargo:rustc-cfg=kernel_support=\"avx512_f16\"");
        };
        // Build AVX-512 bf16 kernels (sapphirerapids has native vdpbf16ps)
        if let Err(err) =
            build_bf16_with_flags("avx512", &["-march=sapphirerapids", "-mavx512fp16"])
        {
            println!(
                "cargo:warning=Skipping build of AVX-512 bf16 kernels. Error: {}",
                err
            );
        } else if cfg!(feature = "fp16kernels") {
            println!("cargo:rustc-cfg=kernel_support=\"avx512_bf16\"");
        };
        if let Err(err) = build_dist_table_with_flags("avx512", &["-march=sapphirerapids"]) {
            println!(
                "cargo:warning=Skipping build of AVX-512 dist_table. Error: {}",
                err
            );
        } else {
            println!("cargo:rustc-cfg=kernel_support=\"avx512_dist_table\"");
        };
        // Build a version with AVX
        // While GCC doesn't have support for _Float16 until GCC 12, clang
        // has support for __fp16 going back to at least clang 6.
        // We use haswell since it's the oldest CPUs on AWS.
        if let Err(err) = build_f16_with_flags("avx2", &["-march=haswell"]) {
            return Err(format!(
                "Unable to build AVX2 f16 kernels.  Please use Clang >= 6 or GCC >= 12 or remove the fp16kernels feature.  Received error: {}",
                err
            ));
        };
        // Build AVX2 bf16 kernels (bf16-to-f32 is just a shift, auto-vectorizes well)
        if let Err(err) = build_bf16_with_flags("avx2", &["-march=haswell"]) {
            return Err(format!(
                "Unable to build AVX2 bf16 kernels.  Received error: {}",
                err
            ));
        };
        // There is no SSE instruction set for f16 -> f32 float conversion
    } else if target_arch == "loongarch64" {
        // Build a version with LSX and LASX
        build_f16_with_flags("lsx", &["-mlsx"]).unwrap();
        build_f16_with_flags("lasx", &["-mlasx"]).unwrap();
        build_bf16_with_flags("lsx", &["-mlsx"]).unwrap();
        build_bf16_with_flags("lasx", &["-mlasx"]).unwrap();
    } else {
        // Only error if fp16kernels was explicitly requested on unsupported platform.
        // This allows builds on iOS, Android, etc. when the feature is disabled.
        //
        // Note: We use CARGO_FEATURE_* env var instead of cfg!() because cfg!()
        // checks the build script's features, not the library's features.
        if env::var("CARGO_FEATURE_FP16KERNELS").is_ok() {
            return Err("Unable to build f16 kernels on given target_arch.  Please use x86_64 or aarch64 or remove the fp16kernels feature".to_string());
        }
    }
    Ok(())
}

fn build_f16_with_flags(suffix: &str, flags: &[&str]) -> Result<(), cc::Error> {
    if cfg!(not(feature = "fp16kernels")) {
        println!(
            "cargo:warning=fp16kernels feature is not enabled, skipping build of fp16 kernels"
        );
        return Ok(());
    }

    let mut builder = cc::Build::new();
    builder
        // We use clang #pragma to yields better vectorization
        // See https://github.com/lance-format/lance/pull/2885
        // .compiler("clang")
        .std("c17")
        .file("src/simd/f16.c")
        .flag("-ffast-math")
        .flag("-funroll-loops")
        .flag("-O3")
        .flag("-Wall")
        // .flag("-Werror")
        .flag("-Wextra")
        // Pedantic will complain about _Float16 in some versions of GCC
        // .flag("-Wpedantic")
        // We pass in the suffix to make sure the symbol names are unique
        .flag(format!("-DSUFFIX=_{}", suffix).as_str());

    for flag in flags {
        builder.flag(flag);
    }

    builder.try_compile(&format!("f16_{}", suffix))
}

fn build_bf16_with_flags(suffix: &str, flags: &[&str]) -> Result<(), cc::Error> {
    if cfg!(not(feature = "fp16kernels")) {
        println!(
            "cargo:warning=fp16kernels feature is not enabled, skipping build of bf16 kernels"
        );
        return Ok(());
    }

    let mut builder = cc::Build::new();
    builder
        .std("c17")
        .file("src/simd/bf16.c")
        .flag("-ffast-math")
        .flag("-funroll-loops")
        .flag("-O3")
        .flag("-Wall")
        .flag("-Wextra")
        .flag(format!("-DSUFFIX=_{}", suffix).as_str());

    for flag in flags {
        builder.flag(flag);
    }

    builder.try_compile(&format!("bf16_{}", suffix))
}

fn build_dist_table_with_flags(suffix: &str, flags: &[&str]) -> Result<(), cc::Error> {
    let mut builder = cc::Build::new();
    builder
        .std("c17")
        .file("src/simd/dist_table.c")
        .flag("-funroll-loops")
        .flag("-O3")
        .flag("-Wall")
        .flag("-Wextra")
        .flag("-mavx512bw")
        .flag(format!("-DSUFFIX=_{}", suffix).as_str());
    for flag in flags {
        builder.flag(flag);
    }
    builder.try_compile(&format!("dist_table_{}", suffix))
}
