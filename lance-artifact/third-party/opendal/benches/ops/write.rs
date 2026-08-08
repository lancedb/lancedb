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

use divan::Bencher;
use divan::counter::BytesCount;
use opendal::tests::TEST_RUNTIME;
use opendal::tests::init_test_service;
use rand::rng;
use size::Size;

use super::utils::*;

#[divan::bench(
    args = [
        Size::from_kib(4),
        Size::from_kib(64),
        Size::from_mib(1),
        Size::from_mib(16),
    ],
    ignore = std::env::var("OPENDAL_TEST").is_err()
)]
fn whole(b: Bencher, size: Size) {
    let op = init_test_service().unwrap().unwrap();
    let path = uuid::Uuid::new_v4().to_string();
    let _temp_data = TempData::existing(op.clone(), &path);

    b.counter(BytesCount::from(size.bytes() as u64))
        .with_inputs(|| {
            let mut rng = rng();
            gen_bytes(&mut rng, size.bytes() as usize)
        })
        .bench_values(|content| {
            let op = op.clone();
            let path = path.clone();
            TEST_RUNTIME.block_on(async move {
                let _ = op.write(&path, content).await.unwrap();
            })
        })
}

mod concurrent {
    use super::*;

    /// Size of the data to be written.
    const SIZE: usize = 64 * 1024 * 1024;

    #[divan::bench(
        ignore = std::env::var("OPENDAL_TEST").is_err()
    )]
    fn baseline(b: Bencher) {
        let op = init_test_service().unwrap().unwrap();
        let path = uuid::Uuid::new_v4().to_string();
        let _temp_data = TempData::existing(op.clone(), &path);
        let mut rng = rng();
        let content = gen_bytes(&mut rng, SIZE);

        b.counter(BytesCount::from(SIZE)).bench(|| {
            let op = op.clone();
            let path = path.clone();
            let content = content.clone();
            TEST_RUNTIME.block_on(async move {
                let _ = op.write(&path, content).await.unwrap();
            })
        })
    }

    #[divan::bench(
        args = [1, 2, 4, 8],
        ignore = std::env::var("OPENDAL_TEST").is_err()
    )]
    fn concurrent(b: Bencher, concurrent: usize) {
        let op = init_test_service().unwrap().unwrap();
        let path = uuid::Uuid::new_v4().to_string();
        let _temp_data = TempData::existing(op.clone(), &path);
        let mut rng = rng();
        let content = gen_bytes(&mut rng, SIZE);

        b.counter(BytesCount::from(SIZE)).bench(|| {
            let op = op.clone();
            let path = path.clone();
            let content = content.clone();
            TEST_RUNTIME.block_on(async move {
                let _ = op
                    .write_with(&path, content)
                    .chunk(8 * 1024 * 1024)
                    .concurrent(concurrent)
                    .await
                    .unwrap();
            })
        })
    }
}
