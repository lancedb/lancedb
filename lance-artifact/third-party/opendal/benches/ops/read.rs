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
    let mut rng = rng();
    let content = gen_bytes(&mut rng, size.bytes() as usize);
    let path = uuid::Uuid::new_v4().to_string();
    let _temp_data = TempData::generate(op.clone(), &path, content.clone());

    b.counter(BytesCount::from(size.bytes() as u64)).bench(|| {
        let op = op.clone();
        let path = path.clone();
        TEST_RUNTIME.block_on(async move {
            let _ = op.read(&path).await.unwrap();
        })
    })
}

mod concurrent {
    use super::*;

    #[divan::bench(
        ignore = std::env::var("OPENDAL_TEST").is_err()
    )]
    fn baseline(b: Bencher) {
        let op = init_test_service().unwrap().unwrap();
        let mut rng = rng();
        let content = gen_bytes(&mut rng, 1024 * 1024 * 1024);
        let path = uuid::Uuid::new_v4().to_string();
        let _temp_data = TempData::generate(op.clone(), &path, content.clone());

        b.counter(BytesCount::from(1024usize * 1024 * 1024))
            .bench(|| {
                let op = op.clone();
                let path = path.clone();
                TEST_RUNTIME.block_on(async move {
                    let _ = op.read(&path).await.unwrap();
                })
            })
    }

    #[divan::bench(
        args = [1, 2, 4, 8, 16],
        ignore = std::env::var("OPENDAL_TEST").is_err()
    )]
    fn concurrent(b: Bencher, concurrent: usize) {
        let op = init_test_service().unwrap().unwrap();
        let mut rng = rng();
        let content = gen_bytes(&mut rng, 1024 * 1024 * 1024);
        let path = uuid::Uuid::new_v4().to_string();
        let _temp_data = TempData::generate(op.clone(), &path, content.clone());

        b.counter(BytesCount::from(1024usize * 1024 * 1024))
            .bench(|| {
                let op = op.clone();
                let path = path.clone();
                TEST_RUNTIME.block_on(async move {
                    let _ = op
                        .read_with(&path)
                        .chunk(16 * 1024 * 1024)
                        .concurrent(concurrent)
                        .await
                        .unwrap();
                })
            })
    }
}
