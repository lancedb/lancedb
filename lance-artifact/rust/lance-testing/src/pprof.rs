// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{fs::File, os::raw::c_int, path::Path};

use ::pprof::{ProfilerGuard, flamegraph::Options as FlamegraphOptions};
use criterion::profiler::Profiler;

#[allow(clippy::large_enum_variant)]
pub enum Output<'a> {
    Flamegraph(Option<FlamegraphOptions<'a>>),
}

pub struct PProfProfiler<'a, 'b> {
    frequency: c_int,
    output: Output<'b>,
    active_profiler: Option<ProfilerGuard<'a>>,
}

impl<'a, 'b> PProfProfiler<'a, 'b> {
    pub fn new(frequency: c_int, output: Output<'b>) -> Self {
        Self {
            frequency,
            output,
            active_profiler: None,
        }
    }
}

impl<'a, 'b> Profiler for PProfProfiler<'a, 'b> {
    fn start_profiling(&mut self, _benchmark_id: &str, _benchmark_dir: &Path) {
        self.active_profiler = Some(ProfilerGuard::new(self.frequency).unwrap());
    }

    fn stop_profiling(&mut self, _benchmark_id: &str, benchmark_dir: &Path) {
        std::fs::create_dir_all(benchmark_dir).unwrap();

        let Output::Flamegraph(options) = &mut self.output;

        let output_path = benchmark_dir.join("flamegraph.svg");
        let output_file = File::create(&output_path).unwrap_or_else(|_| {
            panic!("File system error while creating {}", output_path.display())
        });

        if let Some(profiler) = self.active_profiler.take() {
            let default_options = &mut FlamegraphOptions::default();
            let options = options.as_mut().unwrap_or(default_options);

            profiler
                .report()
                .build()
                .unwrap()
                .flamegraph_with_options(output_file, options)
                .expect("Error while writing flamegraph");
        }
    }
}
