// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::LazyLock;
use std::time::Duration;

use divan::Bencher;
use opendal::Executor;
use opendal::raw::ConcurrentTasks;

pub static TOKIO: LazyLock<tokio::runtime::Runtime> =
    LazyLock::new(|| tokio::runtime::Runtime::new().expect("build tokio runtime"));

pub static TOTAL_TASKS: usize = 128;

#[divan::bench]
fn baseline(b: Bencher) {
    b.bench(|| {
        TOKIO.block_on(async move {
            for _ in 0..TOTAL_TASKS {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    });
}

#[divan::bench(args = [1, 2, 8, 32])]
fn concurrent(b: Bencher, concurrent: usize) {
    b.with_inputs(|| {
        ConcurrentTasks::new(Executor::new(), concurrent, 8, |()| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(1)).await;
                ((), Ok(()))
            })
        })
    })
    .bench_refs(|tasks| {
        TOKIO.block_on(async move {
            for _ in 0..TOTAL_TASKS {
                let _ = tasks.execute(()).await;
            }

            loop {
                if tasks.next().await.is_none() {
                    break;
                }
            }
        })
    });
}
