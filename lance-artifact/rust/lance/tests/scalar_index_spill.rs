// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Regression test: training a BTREE scalar index on a large string column
//! must succeed when the sort spills under a small bounded memory pool.
//!
//! Sorting far more data than the pool holds produces many spilled runs; the
//! external-sort merge phase then needs pool memory on top of the runs.
//! Before the sort spill reservation was sized to the pool (#7675), the merge
//! reservation could exceed the whole `FairSpillPool`, failing index creation
//! with `ResourcesExhausted` unless spilling was bypassed entirely via
//! `LANCE_BYPASS_SPILLING`.
//!
//! This file must stay a single-test integration binary: the training path
//! only reads the pool size from the process-global `LANCE_MEM_POOL_SIZE`, so
//! any sibling test could race the `set_var` or inherit the tiny pool.

use lance::Dataset;
use lance::dataset::WriteParams;
use lance::index::DatasetIndexExt;
use lance_datafusion::exec::LanceExecutionOptions;
use lance_datagen::{BatchCount, ByteCount, RowCount, array, gen_batch};
use lance_index::IndexType;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};

const MEM_POOL_SIZE: u64 = 4 * 1024 * 1024;

#[tokio::test]
async fn test_btree_training_sort_spill_merge_fits_pool() {
    // 4 MiB pool vs ~36 MiB of sort input (512K rows of 64-byte strings plus
    // row ids) forces many spilled sort runs.
    unsafe {
        std::env::set_var("LANCE_MEM_POOL_SIZE", MEM_POOL_SIZE.to_string());
        // The historical workaround for this very bug; if it leaks in from the
        // environment the bounded pool is skipped and the test checks nothing.
        std::env::remove_var("LANCE_BYPASS_SPILLING");
    }
    // The training scan builds its execution options from the env vars above;
    // fail loudly if that plumbing ever changes, otherwise the sort would run
    // against the default (much larger) pool and pass vacuously.
    let options = LanceExecutionOptions {
        use_spilling: true,
        ..Default::default()
    };
    assert_eq!(options.mem_pool_size(), MEM_POOL_SIZE);
    assert!(options.use_spilling());

    let data = gen_batch()
        .col("value", array::rand_utf8(ByteCount::from(64), false))
        .into_reader_rows(RowCount::from(8192), BatchCount::from(64));

    let write_params = WriteParams {
        max_rows_per_file: 256 * 1024,
        ..Default::default()
    };
    let mut dataset = Dataset::write(data, "memory://", Some(write_params))
        .await
        .unwrap();
    assert!(dataset.get_fragments().len() > 1);

    let params = ScalarIndexParams::for_builtin(BuiltinIndexType::BTree);
    dataset
        .create_index(&["value"], IndexType::BTree, None, &params, false)
        .await
        .unwrap();

    let indices = dataset.load_indices().await.unwrap();
    assert_eq!(indices.len(), 1);
}
