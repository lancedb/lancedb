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

use bytes::Buf;
use bytes::Bytes;
use divan::Bencher;
use divan::black_box;
use opendal::Buffer;

mod chunk {
    use super::*;

    #[divan::bench]
    fn bytes(b: Bencher) {
        b.with_inputs(|| Bytes::from(vec![1; 10]))
            .bench_refs(|buffer| {
                black_box(buffer.chunk());
            });
    }

    #[divan::bench]
    fn contiguous(b: Bencher) {
        b.with_inputs(|| Buffer::from(vec![1; 10]))
            .bench_refs(|buffer| {
                black_box(buffer.chunk());
            });
    }

    #[divan::bench(args = [10, 1_000, 1_000_000])]
    fn non_contiguous(b: Bencher, parts: usize) {
        b.with_inputs(|| Buffer::from([1; 1].repeat(parts)))
            .bench_refs(|buffer| {
                black_box(buffer.chunk());
            });
    }
}

mod advance {
    use super::*;

    #[divan::bench]
    fn bytes(b: Bencher) {
        b.with_inputs(|| Bytes::from(vec![1; 10]))
            .bench_refs(|buffer| buffer.advance(4));
    }

    #[divan::bench]
    fn contiguous(b: Bencher) {
        b.with_inputs(|| Buffer::from(vec![1; 10]))
            .bench_refs(|buffer| buffer.advance(4));
    }

    #[divan::bench(args = [10, 1_000, 1_000_000])]
    fn non_contiguous(b: Bencher, parts: usize) {
        b.with_inputs(|| Buffer::from([1; 1].repeat(parts)))
            .bench_refs(|buffer| buffer.advance(4));
    }
}

mod truncate {
    use super::*;

    #[divan::bench]
    fn bytes(b: Bencher) {
        b.with_inputs(|| Bytes::from(vec![1; 10]))
            .bench_refs(|buffer| buffer.truncate(5));
    }

    #[divan::bench]
    fn contiguous(b: Bencher) {
        b.with_inputs(|| Buffer::from(vec![1; 10]))
            .bench_refs(|buffer| buffer.truncate(5));
    }

    #[divan::bench(args = [10, 1_000, 1_000_000])]
    fn non_contiguous(b: Bencher, parts: usize) {
        b.with_inputs(|| Buffer::from([1; 1].repeat(parts)))
            .bench_refs(|buffer| buffer.truncate(4));
    }
}

mod iterator {
    use super::*;

    #[divan::bench]
    fn contiguous(b: Bencher) {
        b.with_inputs(|| Buffer::from(vec![1; 1_000_000]))
            .bench_refs(|buffer| for _ in buffer {});
    }

    #[divan::bench(args = [10, 1_000, 1_000_000])]
    fn non_contiguous(b: Bencher, parts: usize) {
        b.with_inputs(|| Buffer::from(vec![1; 1_000_000 / parts].repeat(parts)))
            .bench_refs(|buffer| for _ in buffer {});
    }
}
