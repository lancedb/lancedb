// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Single-writer, lock-free-read skiplist with no epoch reclamation.
//!
//! Purpose-built for the MemTable scalar index, whose access pattern is:
//! append-only (no per-entry delete), a single writer (serialized by the
//! `ShardWriter` actor), and many concurrent readers. Under that pattern there
//! is nothing to reclaim until the whole index is dropped, so — unlike a
//! general-purpose concurrent skiplist (`crossbeam_skiplist`) — we pay **no
//! epoch pin** on reads. Profiling showed crossbeam's per-operation epoch
//! pinning (`try_pin_loop`) dominating the point-lookup hot path and, worse,
//! contending across threads (the N-thread read-scaling bottleneck). This
//! mirrors RocksDB's arena `InlineSkipList`: nodes live for the index's whole
//! life, readers only do `Acquire` loads, the writer publishes with `Release`.
//!
//! # Node layout (cache locality)
//! The seek is **cache-miss bound** — each tower hop loads a node's key. So a
//! node is a **single bump-arena allocation** with the key and its forward-
//! pointer tower laid out contiguously (`[key][AtomicPtr; height]`), exactly
//! like RocksDB. This is one cache miss per hop, not two (a separate boxed
//! tower measured ~2x slower single-thread). Nodes are bump-allocated from
//! large chunks, so they are also contiguous in insertion order.
//!
//! # Safety model
//! - **Single writer.** Only [`SkipListWriter`] mutates (the sole `&mut`).
//!   Callers must serialize writes (the MemTable does so via the actor; the
//!   BTree index additionally guards the writer behind a `Mutex`).
//! - **No free before drop.** Nodes live in the arena and are freed only when
//!   the core (the whole index generation) drops, when no readers remain. So a
//!   reader can never observe a freed node. Node addresses are stable (bump
//!   chunks never move or realloc).
//! - **Publish/consume.** The writer initializes a node fully, then links it in
//!   with `Release` stores; readers follow links with `Acquire` loads, so a
//!   reader that sees a pointer also sees the fully-initialized node.
//! - **In-bounds towers.** A node is linked at level `L` only if its height is
//!   `> L`, so any node reached during a level-`L` traversal has a tower slot at
//!   `L` — reads never run off the end of a variable-length tower.

use std::alloc::{self, Layout};
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

/// Maximum tower height. 16 levels with p=1/4 supports ~4^16 ≈ 4 billion
/// entries before degrading, far beyond any single MemTable generation.
const MAX_HEIGHT: usize = 16;
/// Inverse promotion probability (p = 1/4): a node grows one level with prob
/// 1/4. Matches RocksDB's default `kBranching`.
const BRANCHING: u64 = 4;
/// Bump-arena chunk size (1 MiB). Nodes are packed contiguously within a chunk.
const CHUNK_SIZE: usize = 1 << 20;

/// Node header. The variable-length forward-pointer tower (`height` slots of
/// `AtomicPtr<Node<K>>`) is laid out immediately after this header in the same
/// allocation; see [`tower`]. `#[repr(C)]` fixes the field order so the tower
/// offset is stable.
#[repr(C)]
struct Node<K> {
    key: K,
}

/// Byte offset of the tower (first `AtomicPtr`) within a node allocation.
#[inline]
fn tower_offset<K>() -> usize {
    let align = align_of::<AtomicPtr<Node<K>>>();
    (size_of::<Node<K>>() + align - 1) & !(align - 1)
}

/// Allocation layout for a node with `height` tower levels.
#[inline]
fn node_layout<K>(height: usize) -> Layout {
    let size = tower_offset::<K>() + height * size_of::<AtomicPtr<Node<K>>>();
    let align = align_of::<Node<K>>().max(align_of::<AtomicPtr<Node<K>>>());
    Layout::from_size_align(size, align).expect("valid node layout")
}

/// Pointer to a node's tower (level-0 forward pointer). `node` must be non-null
/// and point to an initialized node; level access must stay `< node.height`.
#[inline]
unsafe fn tower<K>(node: *const Node<K>) -> *const AtomicPtr<Node<K>> {
    (node as *const u8).add(tower_offset::<K>()).cast()
}

/// A simple single-threaded bump allocator. Hands out node-sized blocks from
/// large chunks; never frees individual blocks. Chunks are freed on drop.
struct Arena {
    chunks: Vec<(NonNull<u8>, Layout)>,
    cursor: *mut u8,
    end: *mut u8,
}

impl Arena {
    fn new() -> Self {
        Self {
            chunks: Vec::new(),
            cursor: ptr::null_mut(),
            end: ptr::null_mut(),
        }
    }

    /// Bump-allocate `layout`. Caller must have exclusive access (single writer).
    unsafe fn alloc(&mut self, layout: Layout) -> *mut u8 {
        let align = layout.align();
        let mut aligned = (self.cursor as usize).wrapping_add(align - 1) & !(align - 1);
        if self.cursor.is_null() || aligned + layout.size() > self.end as usize {
            self.grow(layout);
            aligned = (self.cursor as usize + align - 1) & !(align - 1);
        }
        self.cursor = (aligned + layout.size()) as *mut u8;
        aligned as *mut u8
    }

    /// Allocate a fresh chunk large enough for `layout` and make it current.
    #[cold]
    unsafe fn grow(&mut self, layout: Layout) {
        let align = layout.align().max(64);
        let size = CHUNK_SIZE.max(layout.size().next_power_of_two());
        let chunk_layout = Layout::from_size_align(size, align).expect("valid chunk layout");
        let ptr = alloc::alloc(chunk_layout);
        if ptr.is_null() {
            alloc::handle_alloc_error(chunk_layout);
        }
        self.chunks
            .push((NonNull::new_unchecked(ptr), chunk_layout));
        self.cursor = ptr;
        self.end = ptr.add(size);
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        for (ptr, layout) in &self.chunks {
            // SAFETY: each chunk was allocated by `grow` with this exact layout
            // and is freed exactly once.
            unsafe { alloc::dealloc(ptr.as_ptr(), *layout) };
        }
    }
}

/// Shared, append-only core. Owns the arena (and thus every node) for the
/// index's lifetime. `head` is a bare tower (no key) acting as the before-first
/// sentinel; a null node pointer in a traversal means "at head". `arena` is
/// touched only by the single writer; readers only follow `next` pointers.
struct SkipListCore<K> {
    /// Forward pointers out of the head sentinel, one per level (`MAX_HEIGHT`).
    head: Box<[AtomicPtr<Node<K>>]>,
    /// Backing storage for all nodes. Writer-only access.
    arena: UnsafeCell<Arena>,
    /// Highest tower level currently in use (1..=MAX_HEIGHT).
    height: AtomicUsize,
    /// Number of entries.
    len: AtomicUsize,
}

// SAFETY: `arena` (the only non-Sync field) is mutated exclusively by the single
// writer; readers never access it. Reader/writer interaction on `head`/towers
// goes through atomics with Acquire/Release. `K: Send + Sync` covers the keys
// shared with readers.
unsafe impl<K: Send + Sync> Send for SkipListCore<K> {}
unsafe impl<K: Send + Sync> Sync for SkipListCore<K> {}

impl<K> SkipListCore<K> {
    fn new() -> Self {
        let head = (0..MAX_HEIGHT)
            .map(|_| AtomicPtr::new(ptr::null_mut()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            head,
            arena: UnsafeCell::new(Arena::new()),
            height: AtomicUsize::new(1),
            len: AtomicUsize::new(0),
        }
    }

    /// The forward-pointer slot at `level` for `node` (null `node` = head).
    /// `level` must be `< node.height` for a non-null node (upheld by the
    /// "linked at L ⇒ height > L" invariant during traversal).
    #[inline]
    fn next_slot(&self, node: *const Node<K>, level: usize) -> &AtomicPtr<Node<K>> {
        if node.is_null() {
            &self.head[level]
        } else {
            // SAFETY: `node` is a live node owned by the arena; `level` is in
            // bounds by the traversal invariant.
            unsafe { &*tower(node).add(level) }
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }
}

impl<K> Drop for SkipListCore<K> {
    fn drop(&mut self) {
        // Drop each key in place before the arena frees the backing chunks.
        // Runs before fields drop, so `arena` memory is still valid here. No
        // readers remain (the core drops only when all handles are gone), so
        // relaxed loads suffice.
        let mut node = self.head[0].load(Ordering::Relaxed);
        while !node.is_null() {
            // SAFETY: `node` is a live node in the arena; tower[0] is its
            // level-0 successor (or null at the end).
            let next = unsafe { (*tower(node)).load(Ordering::Relaxed) };
            unsafe { ptr::drop_in_place(ptr::addr_of_mut!((*node).key)) };
            node = next;
        }
    }
}

/// Create a paired writer and reader over a fresh, empty skiplist core.
pub fn new_skiplist<K: Ord + Send + Sync>() -> (SkipListWriter<K>, SkipListReader<K>) {
    let core = Arc::new(SkipListCore::new());
    let writer = SkipListWriter {
        core: core.clone(),
        // Nonzero xorshift seed; the writer is single-threaded so a private,
        // deterministic RNG is fine (and keeps tests reproducible).
        rng: 0x9E3779B97F4A7C15,
    };
    let reader = SkipListReader { core };
    (writer, reader)
}

/// The sole mutator of a skiplist. Not `Sync`: only one writer may exist.
pub struct SkipListWriter<K> {
    core: Arc<SkipListCore<K>>,
    rng: u64,
}

impl<K: Ord> SkipListWriter<K> {
    /// Geometric height with p = 1/BRANCHING, capped at `MAX_HEIGHT`.
    fn random_height(&mut self) -> usize {
        // xorshift64
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        let mut height = 1;
        while height < MAX_HEIGHT && x.is_multiple_of(BRANCHING) {
            height += 1;
            x /= BRANCHING;
        }
        height
    }

    #[cfg(test)]
    pub fn insert(&mut self, key: K) {
        self.insert_and_check_neighbors(key, |_, _| false);
    }

    /// Insert `key` and let the caller inspect its immediate level-0 neighbors.
    ///
    /// Keys must be unique (the MemTable key carries a row position, making
    /// every entry distinct). Backends that index `(value, row_position)` use
    /// the neighbor check to detect whether the same value already existed,
    /// reusing the insertion traversal instead of issuing a second lookup.
    pub fn insert_and_check_neighbors(
        &mut self,
        key: K,
        check: impl FnOnce(Option<&K>, Option<&K>) -> bool,
    ) -> bool {
        let cur_height = self.core.height.load(Ordering::Relaxed);

        // Find the predecessor at every level. For levels at/above the current
        // height the predecessor is the head (null); the descent never enters
        // those levels, leaving `pred` null there.
        let mut preds: [*const Node<K>; MAX_HEIGHT] = [ptr::null(); MAX_HEIGHT];
        let mut pred: *const Node<K> = ptr::null();
        for level in (0..MAX_HEIGHT).rev() {
            if level < cur_height {
                loop {
                    let next = self.core.next_slot(pred, level).load(Ordering::Acquire);
                    if !next.is_null() && unsafe { (*next).key < key } {
                        pred = next;
                    } else {
                        break;
                    }
                }
            }
            preds[level] = pred;
        }

        let succ = self.core.next_slot(preds[0], 0).load(Ordering::Acquire);
        // SAFETY: `preds[0]` and `succ` are either null or point to nodes owned
        // by this skiplist's arena. The single writer never removes nodes, so
        // any non-null neighbor remains alive for the duration of this call.
        let had_neighbor_match =
            check(unsafe { preds[0].as_ref().map(|node| &node.key) }, unsafe {
                succ.as_ref().map(|node| &node.key)
            });

        let height = self.random_height();

        // Allocate the node in one block and initialize it fully (key + tower)
        // before publishing. Successors are stable: the single writer is the
        // only mutator, so no link changes between read and publish.
        let layout = node_layout::<K>(height);
        // SAFETY: single-writer exclusive access to the arena.
        let node = unsafe { (*self.core.arena.get()).alloc(layout) } as *mut Node<K>;
        // SAFETY: `node` points to a fresh, uninitialized, correctly-sized and
        // -aligned block; we write the key then `height` tower slots.
        unsafe {
            ptr::write(ptr::addr_of_mut!((*node).key), key);
            let tower = tower::<K>(node) as *mut AtomicPtr<Node<K>>;
            for (level, pred) in preds.iter().enumerate().take(height) {
                let succ = self.core.next_slot(*pred, level).load(Ordering::Acquire);
                ptr::write(tower.add(level), AtomicPtr::new(succ));
            }
        }

        // Advertise the taller height before linking the top levels: a reader
        // that sees the new height but not yet a top link just finds a null
        // there and descends — still correct.
        if height > cur_height {
            self.core.height.store(height, Ordering::Release);
        }

        // Publish: splice the node in at each level with Release so a reader
        // that loads the pointer also sees the initialized node.
        for (level, pred) in preds.iter().enumerate().take(height) {
            self.core
                .next_slot(*pred, level)
                .store(node, Ordering::Release);
        }

        self.core.len.fetch_add(1, Ordering::Release);
        had_neighbor_match
    }
}

/// A lock-free, pin-free reader. Cheaply cloned and shared across threads.
#[derive(Clone)]
pub struct SkipListReader<K> {
    core: Arc<SkipListCore<K>>,
}

impl<K: Ord> SkipListReader<K> {
    /// Greatest node with `key <= target`, mapped through `f` while it is alive.
    /// Equivalent to crossbeam's `upper_bound(Included(target))`. `None` if no
    /// such node. The closure avoids cloning the key on the hot path.
    pub fn upper_bound_with<R>(&self, target: &K, f: impl FnOnce(&K) -> R) -> Option<R> {
        let node = self.find_le(target);
        if node.is_null() {
            None
        } else {
            // SAFETY: non-null node owned by the core, alive for this call.
            Some(f(unsafe { &(*node).key }))
        }
    }

    /// Greatest node with `key <= target`, or null. Descends the tower.
    fn find_le(&self, target: &K) -> *const Node<K> {
        let height = self.core.height.load(Ordering::Acquire);
        let mut pred: *const Node<K> = ptr::null();
        for level in (0..height).rev() {
            loop {
                let next = self.core.next_slot(pred, level).load(Ordering::Acquire);
                if !next.is_null() && unsafe { (*next).key <= *target } {
                    pred = next;
                } else {
                    break;
                }
            }
        }
        pred
    }

    /// First node with `key >= start`, or null.
    fn lower_bound(&self, start: &K) -> *const Node<K> {
        let height = self.core.height.load(Ordering::Acquire);
        let mut pred: *const Node<K> = ptr::null();
        for level in (0..height).rev() {
            loop {
                let next = self.core.next_slot(pred, level).load(Ordering::Acquire);
                if !next.is_null() && unsafe { (*next).key < *start } {
                    pred = next;
                } else {
                    break;
                }
            }
        }
        self.core.next_slot(pred, 0).load(Ordering::Acquire)
    }

    /// Iterate all keys in ascending order.
    pub fn iter(&self) -> Iter<'_, K> {
        Iter {
            node: self.core.head[0].load(Ordering::Acquire),
            _marker: PhantomData,
        }
    }

    /// Iterate keys in ascending order starting at the first `key >= start`.
    pub fn range_from(&self, start: &K) -> Iter<'_, K> {
        Iter {
            node: self.lower_bound(start),
            _marker: PhantomData,
        }
    }

    /// The smallest key, mapped through `f`, or `None` if empty.
    pub fn front_with<R>(&self, f: impl FnOnce(&K) -> R) -> Option<R> {
        let node = self.core.head[0].load(Ordering::Acquire);
        if node.is_null() {
            None
        } else {
            // SAFETY: non-null node owned by the core, alive for this call.
            Some(f(unsafe { &(*node).key }))
        }
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.core.len()
    }
}

/// Forward iterator over keys in ascending order. Yields `&K` borrowed from the
/// reader: nodes are never freed while the core (and thus the reader) is alive.
pub struct Iter<'a, K> {
    node: *const Node<K>,
    _marker: PhantomData<&'a SkipListReader<K>>,
}

impl<'a, K> Iterator for Iter<'a, K> {
    type Item = &'a K;

    fn next(&mut self) -> Option<&'a K> {
        if self.node.is_null() {
            return None;
        }
        // SAFETY: non-null node owned by the core; the borrow lifetime `'a` is
        // bounded by the reader, which keeps the core (and node) alive. tower[0]
        // is the level-0 successor.
        let node = unsafe { &*self.node };
        self.node = unsafe { (*tower(self.node)).load(Ordering::Acquire) };
        Some(&node.key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::thread;

    fn collect(reader: &SkipListReader<i64>) -> Vec<i64> {
        reader.iter().copied().collect()
    }

    #[test]
    fn test_insert_keeps_sorted_order() {
        let (mut w, r) = new_skiplist::<i64>();
        for k in [5, 1, 9, 3, 7, 2, 8, 4, 6, 0] {
            w.insert(k);
        }
        assert_eq!(collect(&r), vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(r.len(), 10);
    }

    #[test]
    fn test_empty() {
        let (_w, r) = new_skiplist::<i64>();
        assert_eq!(r.len(), 0);
        assert_eq!(collect(&r), Vec::<i64>::new());
        assert_eq!(r.upper_bound_with(&5, |k| *k), None);
        assert_eq!(r.front_with(|k| *k), None);
        assert_eq!(r.range_from(&0).count(), 0);
    }

    #[test]
    fn test_upper_bound_le() {
        let (mut w, r) = new_skiplist::<i64>();
        for k in [10, 20, 30, 40] {
            w.insert(k);
        }
        // Exact hits.
        assert_eq!(r.upper_bound_with(&20, |k| *k), Some(20));
        assert_eq!(r.upper_bound_with(&40, |k| *k), Some(40));
        // Between keys → greatest <= target.
        assert_eq!(r.upper_bound_with(&25, |k| *k), Some(20));
        assert_eq!(r.upper_bound_with(&39, |k| *k), Some(30));
        // Above all.
        assert_eq!(r.upper_bound_with(&999, |k| *k), Some(40));
        // Below all → None.
        assert_eq!(r.upper_bound_with(&5, |k| *k), None);
    }

    #[test]
    fn test_front_and_range_from() {
        let (mut w, r) = new_skiplist::<i64>();
        for k in [3, 1, 4, 1_000, 2] {
            w.insert(k);
        }
        assert_eq!(r.front_with(|k| *k), Some(1));
        assert_eq!(
            r.range_from(&3).copied().collect::<Vec<_>>(),
            vec![3, 4, 1_000]
        );
        // start below first → all; start above last → empty.
        assert_eq!(
            r.range_from(&0).copied().collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 1_000]
        );
        assert_eq!(r.range_from(&2_000).count(), 0);
        // start between → from first >= start.
        assert_eq!(r.range_from(&5).copied().collect::<Vec<_>>(), vec![1_000]);
    }

    #[test]
    fn test_composite_key_dup_values() {
        // Mirrors IndexKey = (value, position): same value, distinct positions.
        let (mut w, r) = new_skiplist::<(i64, u64)>();
        for key in [(7, 0), (3, 1), (7, 2), (3, 0), (7, 1)] {
            w.insert(key);
        }
        let all: Vec<_> = r.iter().copied().collect();
        assert_eq!(all, vec![(3, 0), (3, 1), (7, 0), (7, 1), (7, 2)]);
        // Newest visible position for value 7 with watermark 1 = (7,1).
        assert_eq!(r.upper_bound_with(&(7, 1), |k| *k), Some((7, 1)));
        // Watermark below all of value 3 → falls back to a smaller value.
        assert_eq!(r.upper_bound_with(&(3, 5), |k| *k), Some((3, 1)));
    }

    #[test]
    fn test_string_keys_drop() {
        // Exercises key Drop (heap-owning K) through the arena Drop path.
        let (mut w, r) = new_skiplist::<String>();
        for s in ["delta", "alpha", "charlie", "bravo"] {
            w.insert(s.to_string());
        }
        assert_eq!(
            r.iter().cloned().collect::<Vec<_>>(),
            vec!["alpha", "bravo", "charlie", "delta"]
        );
        assert_eq!(
            r.upper_bound_with(&"c".to_string(), |k| k.clone()),
            Some("bravo".to_string())
        );
        drop(w);
        drop(r); // arena drops here; keys must be dropped (no leak / double free)
    }

    #[test]
    fn test_many_inserts_force_chunk_growth() {
        // Enough entries to span multiple arena chunks; verifies ordering and
        // pointer stability across chunk growth.
        let (mut w, r) = new_skiplist::<i64>();
        const N: i64 = 200_000;
        for k in (0..N).rev() {
            w.insert(k);
        }
        assert_eq!(r.len(), N as usize);
        assert!(r.iter().copied().eq(0..N));
        assert_eq!(r.upper_bound_with(&(N - 1), |k| *k), Some(N - 1));
    }

    #[test]
    fn test_concurrent_single_writer_many_readers() {
        // 1 writer inserting 0..N while readers continuously seek. Asserts:
        // every value a reader observes is one the writer has inserted, the
        // observed prefix is contiguous and monotonically non-decreasing (no
        // torn/lost nodes), and the final state is complete and sorted.
        const N: i64 = 50_000;
        let (mut w, r) = new_skiplist::<i64>();
        let done = Arc::new(AtomicBool::new(false));

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let r = r.clone();
                let done = done.clone();
                thread::spawn(move || {
                    let mut max_seen = -1;
                    while !done.load(Ordering::Acquire) {
                        // Largest key present <= N is a contiguous prefix max.
                        if let Some(top) = r.upper_bound_with(&N, |k| *k) {
                            assert!((0..N).contains(&top));
                            assert!(top >= max_seen, "visibility went backwards");
                            max_seen = top;
                            // The observed max must itself be present.
                            assert!(r.upper_bound_with(&top, |k| *k) == Some(top));
                        }
                    }
                    max_seen
                })
            })
            .collect();

        for k in 0..N {
            w.insert(k);
        }
        done.store(true, Ordering::Release);
        for h in readers {
            h.join().unwrap();
        }

        assert_eq!(r.len(), N as usize);
        assert_eq!(collect(&r), (0..N).collect::<Vec<_>>());
    }
}
