// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    future::Future,
    sync::Arc,
};

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lancedb::{
    Table, connect,
    index::Index,
    query::{ExecutableQuery, QueryBase},
};

struct ThreadCountingAllocator;

thread_local! {
    static COUNT_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
    static ALLOCATED_BYTES: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record_allocation(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record_allocation(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            record_allocation(new_size);
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

const ROW_COUNT: usize = 262_144;
const VALUE_COUNT: usize = 1_000;

fn record_allocation(bytes: usize) {
    COUNT_ALLOCATIONS.with(|enabled| {
        if enabled.get() {
            ALLOCATED_BYTES.with(|allocated| allocated.set(allocated.get() + bytes));
        }
    });
}

async fn measure_allocated_bytes<F: Future>(future: F) -> (F::Output, usize) {
    ALLOCATED_BYTES.with(|allocated| allocated.set(0));
    COUNT_ALLOCATIONS.with(|enabled| enabled.set(true));
    let output = future.await;
    COUNT_ALLOCATIONS.with(|enabled| enabled.set(false));
    let allocated = ALLOCATED_BYTES.with(Cell::get);
    (output, allocated)
}

fn in_predicate(ids: impl Iterator<Item = usize>) -> String {
    let values = ids
        .map(|id| format!("'id_{id:06}'"))
        .collect::<Vec<_>>()
        .join(",");
    format!("id IN ({values})")
}

async fn create_indexed_table(name: &str) -> Table {
    let conn = connect("memory://").execute().await.unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let ids = StringArray::from_iter_values((0..ROW_COUNT).map(|id| format!("id_{id:06}")));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(ids)]).unwrap();
    let table = conn.create_table(name, batch).execute().await.unwrap();
    table
        .create_index(&["id"], Index::BTree(Default::default()))
        .execute()
        .await
        .unwrap();
    table
}

async fn warm_index(table: &Table, predicate: &str) {
    table
        .query()
        .only_if(predicate)
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn large_in_delete_compiles_predicate_once() {
    let clustered = in_predicate(0..VALUE_COUNT);
    let spread = in_predicate((0..VALUE_COUNT).map(|id| id * (ROW_COUNT / VALUE_COUNT)));
    let clustered_table = create_indexed_table("clustered_ids").await;
    let spread_table = create_indexed_table("spread_ids").await;

    let plan = spread_table
        .query()
        .only_if(&spread)
        .explain_plan(false)
        .await
        .unwrap();
    assert!(plan.contains("ScalarIndexQuery"), "unexpected plan: {plan}");

    // Remove page-loading noise from the allocation comparison. Predicate
    // compilation is deliberately not cached, so each delete still compiles it.
    warm_index(&clustered_table, &spread).await;
    warm_index(&spread_table, &spread).await;

    let (clustered_result, clustered_bytes) =
        measure_allocated_bytes(clustered_table.delete(&clustered)).await;
    let (spread_result, spread_bytes) = measure_allocated_bytes(spread_table.delete(&spread)).await;

    assert_eq!(
        clustered_result.unwrap().num_deleted_rows,
        VALUE_COUNT as u64
    );
    assert_eq!(spread_result.unwrap().num_deleted_rows, VALUE_COUNT as u64);

    // Both predicates contain the same number and size of values. Spreading them
    // across BTree pages may add modest page-processing overhead, but it must not
    // rematerialize all values per page. This ratio fails by a wide margin if
    // Lance's compile-once path is moved back inside the per-page loop.
    assert!(
        spread_bytes * 2 < clustered_bytes * 3,
        "spread delete allocated {spread_bytes} bytes versus {clustered_bytes} for one page"
    );
}
