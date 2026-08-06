// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::{BTreeMap, BTreeSet};

const SAFE_TARGET_CPUS: [&str; 2] = ["x86-64", "x86-64-v2"];
const BASELINE_FEATURES: [&str; 9] = [
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

pub(crate) fn validate_encoded_rustflags(encoded: &str) -> Result<(), String> {
    let mut target_cpu = None;
    let mut feature_states = BTreeMap::new();
    let mut required_disables = BTreeSet::new();
    let mut unsupported_features = BTreeSet::new();

    for option in codegen_options(encoded)? {
        let Some((name, value)) = option.split_once('=') else {
            continue;
        };

        let name = name.replace('_', "-");
        match name.as_str() {
            "target-cpu" => target_cpu = Some(value),
            "target-feature" => {
                for toggle in value.split(',').filter(|toggle| !toggle.is_empty()) {
                    let (enabled, feature) = match toggle.as_bytes()[0] {
                        b'+' => (true, &toggle[1..]),
                        b'-' => (false, &toggle[1..]),
                        _ => return Err(format!("invalid target feature flag: {toggle}")),
                    };

                    feature_states.insert(feature, enabled);
                    if enabled && !BASELINE_FEATURES.contains(&feature) {
                        match feature {
                            // These are inherited from the workspace configuration and
                            // explicitly canceled by the Node configuration. Account for
                            // their implied AVX prerequisite as well as the named feature.
                            "avx" => {
                                required_disables.insert("avx");
                            }
                            "avx2" => {
                                required_disables.extend(["avx", "avx2"]);
                            }
                            "f16c" => {
                                required_disables.extend(["avx", "f16c"]);
                            }
                            "fma" => {
                                required_disables.extend(["avx", "fma"]);
                            }
                            _ => {
                                unsupported_features.insert(feature);
                            }
                        }
                    }
                }
            }
            // LLVM arguments can independently alter the target feature set and
            // cannot be proven safe by inspecting rustc's target options.
            "llvm-args" => return Err("LLVM arguments can override the CPU baseline".to_owned()),
            _ => {}
        }
    }

    if let Some(cpu) = target_cpu.filter(|cpu| !SAFE_TARGET_CPUS.contains(cpu)) {
        return Err(format!(
            "effective target CPU is {}, expected x86-64-v2 or lower",
            cpu
        ));
    }

    if !unsupported_features.is_empty() {
        return Err(format!(
            "features above v2: {}",
            unsupported_features
                .into_iter()
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let not_disabled = required_disables
        .into_iter()
        .filter(|feature| feature_states.get(feature) != Some(&false))
        .collect::<Vec<_>>();
    if !not_disabled.is_empty() {
        return Err(format!(
            "inherited features not fully disabled: {}",
            not_disabled.join(", ")
        ));
    }

    Ok(())
}

fn codegen_options(encoded: &str) -> Result<Vec<&str>, String> {
    let arguments = encoded.split('\u{1f}').collect::<Vec<_>>();
    let mut options = Vec::new();
    let mut index = 0;

    while index < arguments.len() {
        let argument = arguments[index];
        if argument == "-C" || argument == "--codegen" {
            index += 1;
            let option = arguments
                .get(index)
                .copied()
                .ok_or_else(|| format!("missing value after {argument}"))?;
            options.push(option.trim_start_matches('='));
        } else if let Some(option) = argument.strip_prefix("-C") {
            if !option.is_empty() {
                options.push(option.trim_start_matches('='));
            }
        } else if let Some(option) = argument.strip_prefix("--codegen=") {
            options.push(option);
        }
        index += 1;
    }

    Ok(options)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded(arguments: &[&str]) -> String {
        arguments.join("\u{1f}")
    }

    #[test]
    fn accepts_merged_workspace_and_node_flags() {
        let flags = encoded(&[
            "-C",
            "target-cpu=haswell",
            "-C",
            "target-feature=+avx2,+fma,+f16c",
            "-C",
            "target-cpu=x86-64-v2",
            "-C",
            "target-feature=-avx,-avx2,-fma,-f16c",
        ]);

        assert_eq!(validate_encoded_rustflags(&flags), Ok(()));
    }

    #[test]
    fn accepts_non_merging_v2_boundary() {
        let flags = encoded(&["-Ctarget-cpu=x86-64-v2"]);

        assert_eq!(validate_encoded_rustflags(&flags), Ok(()));
    }

    #[test]
    fn accepts_default_cpu_with_non_codegen_flags() {
        let flags = encoded(&["-D", "warnings"]);

        assert_eq!(validate_encoded_rustflags(&flags), Ok(()));
    }

    #[test]
    fn accepts_explicit_x86_64_v1_cpu() {
        let flags = encoded(&["-Ctarget-cpu=x86-64"]);

        assert_eq!(validate_encoded_rustflags(&flags), Ok(()));
    }

    #[test]
    fn accepts_musl_dynamic_crt_configuration() {
        let flags = encoded(&[
            "-C",
            "target-cpu=haswell",
            "-C",
            "target-feature=-crt-static,+avx2,+fma,+f16c",
            "-C",
            "target-cpu=x86-64-v2",
            "-C",
            "target-feature=-crt-static,-avx,-avx2,-fma,-f16c",
        ]);

        assert_eq!(validate_encoded_rustflags(&flags), Ok(()));
    }

    #[test]
    fn rejects_feature_omitted_from_target_cfg() {
        let flags = encoded(&["-Ctarget-cpu=x86-64-v2", "-Ctarget-feature=+apxf"]);

        assert_eq!(
            validate_encoded_rustflags(&flags),
            Err("features above v2: apxf".to_owned())
        );
    }

    #[test]
    fn rejects_underscore_spelling_above_baseline_feature() {
        let flags = encoded(&["-Ctarget_cpu=x86-64-v2", "-Ctarget_feature=+apxf"]);

        assert_eq!(
            validate_encoded_rustflags(&flags),
            Err("features above v2: apxf".to_owned())
        );
    }

    #[test]
    fn rejects_unexpected_above_baseline_feature() {
        let flags = encoded(&[
            "--codegen=target-cpu=x86-64-v2",
            "--codegen",
            "target-feature=+bmi2",
        ]);

        assert_eq!(
            validate_encoded_rustflags(&flags),
            Err("features above v2: bmi2".to_owned())
        );
    }

    #[test]
    fn rejects_cpu_above_v2() {
        let flags = encoded(&["-Ctarget-cpu=haswell"]);

        assert_eq!(
            validate_encoded_rustflags(&flags),
            Err("effective target CPU is haswell, expected x86-64-v2 or lower".to_owned())
        );
    }

    #[test]
    fn rejects_incompletely_disabled_feature_implications() {
        let flags = encoded(&["-Ctarget-cpu=x86-64-v2", "-Ctarget-feature=+avx2,-avx2"]);

        assert_eq!(
            validate_encoded_rustflags(&flags),
            Err("inherited features not fully disabled: avx".to_owned())
        );
    }

    #[test]
    fn rejects_llvm_feature_overrides() {
        let flags = encoded(&["-Ctarget-cpu=x86-64-v2", "-Cllvm-args=-mattr=+apxf"]);

        assert_eq!(
            validate_encoded_rustflags(&flags),
            Err("LLVM arguments can override the CPU baseline".to_owned())
        );
    }

    #[test]
    fn rejects_underscore_spelling_llvm_feature_overrides() {
        let flags = encoded(&["-Ctarget_cpu=x86-64-v2", "-Cllvm_args=-mattr=+apxf"]);

        assert_eq!(
            validate_encoded_rustflags(&flags),
            Err("LLVM arguments can override the CPU baseline".to_owned())
        );
    }
}
