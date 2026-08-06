// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::alloc::{GlobalAlloc, Layout, System};
use std::borrow::Cow;
use std::cell::Cell;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};

use lance_core::cache::{CacheKey, CacheKeySchema, CacheNamespace, InternalCacheKey, KeyBuilder};

struct TrackingAllocator;

thread_local! {
    static TRACK_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
}

static ALLOCATION_COUNT: AtomicUsize = AtomicUsize::new(0);

fn record_allocation() {
    if TRACK_ALLOCATIONS.try_with(Cell::get).unwrap_or(false) {
        ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation();
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

fn measured_allocations(operation: impl FnOnce()) -> usize {
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    ALLOCATION_COUNT.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
    operation();
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    ALLOCATION_COUNT.load(Ordering::Relaxed)
}

fn prepare_key<K: CacheKey>(namespace: CacheNamespace, key: &K) -> InternalCacheKey {
    let mut builder = KeyBuilder::new(namespace, K::stable_type_id(), K::schema());
    key.write_key(&mut builder);
    builder.finish()
}

struct PageKey {
    path: &'static str,
    column_index: u32,
    page_index: u64,
}

impl CacheKey for PageKey {
    type ValueType = u64;

    fn key(&self) -> Cow<'_, str> {
        Cow::Borrowed("unused")
    }

    fn type_name() -> &'static str {
        "allocation-test-page"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("allocation-test-page", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_str(self.path);
        builder.write_u32(self.column_index);
        builder.write_u64(self.page_index);
    }
}

struct OptionalUuidKey {
    generation: u64,
    uuid: Option<[u8; 16]>,
}

impl CacheKey for OptionalUuidKey {
    type ValueType = u64;

    fn key(&self) -> Cow<'_, str> {
        Cow::Borrowed("unused")
    }

    fn type_name() -> &'static str {
        "allocation-test-optional-uuid"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("allocation-test-optional-uuid", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u64(self.generation);
        match self.uuid {
            Some(uuid) => {
                builder.write_some();
                builder.write_fixed_bytes(&uuid);
            }
            None => builder.write_none(),
        }
    }
}

#[test]
fn production_shaped_typed_keys_allocate_nothing_after_warmup() {
    let namespace = CacheNamespace::root()
        .child("tenant-with-a-long-stable-identifier")
        .child("index-with-a-long-stable-identifier");
    let page = PageKey {
        path: "indices/01999f62-c3c2-7d6f-820d-22e7db948f31/pages/000000000042.lance",
        column_index: 17,
        page_index: 42,
    };
    let uuid = OptionalUuidKey {
        generation: 9,
        uuid: Some(*b"0123456789abcdef"),
    };
    let no_uuid = OptionalUuidKey {
        generation: 10,
        uuid: None,
    };

    black_box(prepare_key(namespace, &page));
    black_box(prepare_key(namespace, &uuid));
    black_box(prepare_key(namespace, &no_uuid));

    assert_eq!(
        measured_allocations(|| {
            black_box(prepare_key(namespace, &page));
        }),
        0
    );
    assert_eq!(
        measured_allocations(|| {
            black_box(prepare_key(namespace, &uuid));
        }),
        0
    );
    assert_eq!(
        measured_allocations(|| {
            black_box(prepare_key(namespace, &no_uuid));
        }),
        0
    );
}
