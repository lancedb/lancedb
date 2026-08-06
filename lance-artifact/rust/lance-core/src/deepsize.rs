// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

pub use lance_derive::DeepSizeOf;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::mem::{size_of, size_of_val};
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex, RwLock};

use arrow_array::{Array, RecordBatch};
use arrow_buffer::ArrowNativeType;
use arrow_data::ArrayData;

pub struct Context {
    seen: HashSet<usize>,
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

impl Context {
    pub fn new() -> Self {
        Self {
            seen: HashSet::new(),
        }
    }

    /// Returns true if this pointer was NOT previously seen (i.e., it's new).
    pub fn mark_seen(&mut self, ptr: usize) -> bool {
        self.seen.insert(ptr)
    }
}

pub trait DeepSizeOf {
    fn deep_size_of(&self) -> usize {
        size_of_val(self) + self.deep_size_of_children(&mut Context::new())
    }

    fn deep_size_of_children(&self, context: &mut Context) -> usize;
}

// Primitives — no heap children
macro_rules! impl_deep_size_primitive {
    ($($t:ty),*) => {
        $(
            impl DeepSizeOf for $t {
                fn deep_size_of_children(&self, _context: &mut Context) -> usize {
                    0
                }
            }
        )*
    };
}

impl_deep_size_primitive!(
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    f32,
    f64,
    bool,
    ()
);

impl DeepSizeOf for str {
    fn deep_size_of_children(&self, _context: &mut Context) -> usize {
        0
    }
}

impl DeepSizeOf for String {
    fn deep_size_of_children(&self, _context: &mut Context) -> usize {
        self.capacity()
    }
}

impl DeepSizeOf for bytes::Bytes {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        if context.mark_seen(self.as_ptr() as usize) {
            self.len()
        } else {
            0
        }
    }
}

impl DeepSizeOf for AtomicU64 {
    fn deep_size_of_children(&self, _context: &mut Context) -> usize {
        0
    }
}

impl DeepSizeOf for AtomicUsize {
    fn deep_size_of_children(&self, _context: &mut Context) -> usize {
        0
    }
}

impl<T: DeepSizeOf, const N: usize> DeepSizeOf for [T; N] {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        self.iter()
            .map(|item| item.deep_size_of_children(context))
            .sum()
    }
}

impl<T: DeepSizeOf> DeepSizeOf for [T] {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        // The slice's own element bytes are accounted for by the owner (e.g. the
        // `size_of_val` in the `Arc`/`Box` impls); here we only sum the heap
        // children of each element.
        self.iter()
            .map(|item| item.deep_size_of_children(context))
            .sum()
    }
}

impl<T: DeepSizeOf> DeepSizeOf for RwLock<T> {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        self.read()
            .map(|val| val.deep_size_of_children(context))
            .unwrap_or(0)
    }
}

impl<T: DeepSizeOf> DeepSizeOf for Mutex<T> {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        self.lock()
            .map(|val| val.deep_size_of_children(context))
            .unwrap_or(0)
    }
}

// Tuples
macro_rules! impl_deep_size_tuple {
    ($($name:ident),+) => {
        impl<$($name: DeepSizeOf),+> DeepSizeOf for ($($name,)+) {
            #[allow(non_snake_case)]
            fn deep_size_of_children(&self, context: &mut Context) -> usize {
                let ($($name,)+) = self;
                0 $(+ $name.deep_size_of_children(context))+
            }
        }
    };
}

impl_deep_size_tuple!(A, B);
impl_deep_size_tuple!(A, B, C);
impl_deep_size_tuple!(A, B, C, D);
impl_deep_size_tuple!(A, B, C, D, E);
impl_deep_size_tuple!(A, B, C, D, E, F);

impl<T: DeepSizeOf> DeepSizeOf for Vec<T> {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        self.capacity() * size_of::<T>()
            + self
                .iter()
                .map(|item| item.deep_size_of_children(context))
                .sum::<usize>()
    }
}

impl<T: DeepSizeOf + ?Sized> DeepSizeOf for Box<T> {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        size_of_val(&**self) + (**self).deep_size_of_children(context)
    }
}

impl<T: DeepSizeOf + ?Sized> DeepSizeOf for Arc<T> {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        if context.mark_seen(Self::as_ptr(self) as *const () as usize) {
            size_of_val(&**self) + (**self).deep_size_of_children(context)
        } else {
            0
        }
    }
}

impl<T: DeepSizeOf> DeepSizeOf for Option<T> {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        match self {
            Some(val) => val.deep_size_of_children(context),
            None => 0,
        }
    }
}

impl<K: DeepSizeOf, V: DeepSizeOf> DeepSizeOf for HashMap<K, V> {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        // Each bucket holds a key-value pair plus hash metadata (~1 byte control per bucket).
        // Robin hood / Swiss table capacity is always a power of 2.
        let capacity_bytes = self.capacity() * (size_of::<K>() + size_of::<V>() + 1);
        let children: usize = self
            .iter()
            .map(|(k, v)| k.deep_size_of_children(context) + v.deep_size_of_children(context))
            .sum();
        capacity_bytes + children
    }
}

impl<K: DeepSizeOf> DeepSizeOf for HashSet<K> {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        let capacity_bytes = self.capacity() * (size_of::<K>() + 1);
        let children: usize = self.iter().map(|k| k.deep_size_of_children(context)).sum();
        capacity_bytes + children
    }
}

impl<K: DeepSizeOf, V: DeepSizeOf> DeepSizeOf for BTreeMap<K, V> {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        // BTreeMap nodes have ~11 entries each. Rough estimate: per-entry overhead ~3 pointers.
        let per_entry = size_of::<K>() + size_of::<V>() + 3 * size_of::<usize>();
        let overhead = self.len() * per_entry;
        let children: usize = self
            .iter()
            .map(|(k, v)| k.deep_size_of_children(context) + v.deep_size_of_children(context))
            .sum();
        overhead + children
    }
}

impl<K: DeepSizeOf> DeepSizeOf for BTreeSet<K> {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        let per_entry = size_of::<K>() + 3 * size_of::<usize>();
        let overhead = self.len() * per_entry;
        let children: usize = self.iter().map(|k| k.deep_size_of_children(context)).sum();
        overhead + children
    }
}

// Arrow types

fn record_array_data(context: &mut Context, data: &ArrayData) -> usize {
    let mut total = 0;
    for buffer in data.buffers() {
        if context.mark_seen(buffer.as_ptr() as usize) {
            total += buffer.capacity();
        }
    }
    if let Some(nulls) = data.nulls() {
        let null_buf = nulls.inner().inner();
        if context.mark_seen(null_buf.as_ptr() as usize) {
            total += null_buf.capacity();
        }
    }
    for child in data.child_data() {
        total += record_array_data(context, child);
    }
    total
}

impl DeepSizeOf for dyn Array {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        // `to_data()` only clones Arc refs (no data copy) and allocates a small
        // ArrayData metadata struct. This lets us walk buffer pointers for dedup.
        // Cost is O(number_of_buffers), not O(data_size).
        let data = self.to_data();
        record_array_data(context, &data)
    }
}

impl DeepSizeOf for RecordBatch {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        self.columns()
            .iter()
            .map(|col| col.deep_size_of_children(context))
            .sum()
    }
}

impl<T> DeepSizeOf for arrow_buffer::ScalarBuffer<T>
where
    T: ArrowNativeType,
{
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        // Track the underlying buffer pointer to avoid double-counting shared allocations.
        // Use capacity() rather than len() * size_of::<T>() because sliced buffers retain
        // their full original allocation.
        let buf = self.inner();
        if context.mark_seen(buf.as_ptr() as usize) {
            buf.capacity()
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray, StructArray};
    use arrow_schema::{DataType, Field, Fields, Schema};

    #[test]
    fn test_basic_record_batch() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let size = batch.deep_size_of();
        // Should at least include the buffer for 3 i32s
        assert!(size >= 3 * size_of::<i32>());
    }

    #[test]
    fn test_same_batch_dedup() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        let mut ctx = Context::new();
        let size_a = batch.deep_size_of_children(&mut ctx);
        let size_b = batch.deep_size_of_children(&mut ctx);

        // First measurement should report buffer sizes
        assert!(size_a > 0);
        // Second measurement of the same batch should add nothing (buffers already seen)
        assert_eq!(size_b, 0);
    }

    #[test]
    fn test_arc_dedup() {
        let batch = Arc::new(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap(),
        );
        let clone = Arc::clone(&batch);

        let mut ctx = Context::new();
        let size_a = batch.deep_size_of_children(&mut ctx);
        let size_b = clone.deep_size_of_children(&mut ctx);

        assert!(size_a > 0);
        assert_eq!(size_b, 0);
    }

    #[test]
    fn test_multi_column_shared_array() {
        // Two columns pointing to the same Arc<dyn Array>
        let array: Arc<dyn Array> = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        // Single-column batch for reference
        let one_col = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![array.clone()],
        )
        .unwrap();

        // Two-column batch with the same Arc shared
        let two_col = RecordBatch::try_new(schema, vec![array.clone(), array]).unwrap();

        let mut ctx1 = Context::new();
        let size_one = one_col.deep_size_of_children(&mut ctx1);

        let mut ctx2 = Context::new();
        let size_two = two_col.deep_size_of_children(&mut ctx2);

        // Both should report the same size since the second column's Arc is
        // already seen and contributes nothing
        assert_eq!(size_one, size_two);
    }

    #[test]
    fn test_nested_struct_array() {
        let int_array = Int32Array::from(vec![1, 2, 3]);
        let str_array = StringArray::from(vec!["a", "b", "c"]);
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::Int32, false)),
                Arc::new(int_array) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("y", DataType::Utf8, false)),
                Arc::new(str_array) as Arc<dyn Array>,
            ),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "s",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int32, false),
                    Field::new("y", DataType::Utf8, false),
                ])),
                false,
            )])),
            vec![Arc::new(struct_array)],
        )
        .unwrap();

        let size = batch.deep_size_of();
        // Should include buffers for both child arrays
        assert!(size > 3 * size_of::<i32>());
    }

    #[test]
    fn test_std_types() {
        assert_eq!(42u32.deep_size_of(), size_of::<u32>());

        let s = String::from("hello");
        assert!(s.deep_size_of() >= size_of::<String>() + 5);

        let v = vec![1u32, 2, 3];
        assert!(v.deep_size_of() >= size_of::<Vec<u32>>() + 3 * size_of::<u32>());

        let a = Arc::new(42u32);
        let b = Arc::clone(&a);
        let mut ctx = Context::new();
        let size_a = a.deep_size_of_children(&mut ctx);
        let size_b = b.deep_size_of_children(&mut ctx);
        assert_eq!(size_a, size_of::<u32>());
        assert_eq!(size_b, 0);
    }

    #[test]
    fn test_derive_macro() {
        use lance_derive::DeepSizeOf;

        #[derive(DeepSizeOf)]
        struct Outer {
            count: u64,
            label: String,
            inner: Inner,
        }

        #[derive(DeepSizeOf)]
        struct Inner {
            values: Vec<u32>,
        }

        let val = Outer {
            count: 7,
            label: String::from("hello"),
            inner: Inner {
                values: vec![1, 2, 3],
            },
        };

        let size = val.deep_size_of();
        // Must be at least the stack size + heap allocations for label + values
        assert!(size >= std::mem::size_of::<Outer>() + 5 + 3 * std::mem::size_of::<u32>());
    }
}
