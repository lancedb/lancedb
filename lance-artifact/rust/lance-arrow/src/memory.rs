// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashSet;

use arrow_array::{Array, RecordBatch};
use arrow_data::ArrayData;

/// Counts memory used by buffers of Arrow arrays and RecordBatches.
///
/// This is meant to capture how much memory is being used by the Arrow data
/// structures as they are. It does not represent the memory used if the data
/// were to be serialized and then deserialized. In particular:
///
/// * This does not double count memory used by buffers shared by multiple
///   arrays or batches. Round-tripped data may use more memory because of this.
/// * This counts the **total** size of the buffers, even if the array is a slice.
///   Round-tripped data may use less memory because of this.
#[derive(Default)]
pub struct MemoryAccumulator {
    seen: HashSet<usize>,
    total: usize,
}

impl MemoryAccumulator {
    pub fn record_array(&mut self, array: &dyn Array) {
        let data = array.to_data();
        self.record_array_data(&data);
    }

    fn record_array_data(&mut self, data: &ArrayData) {
        for buffer in data.buffers() {
            let ptr = buffer.as_ptr();
            if self.seen.insert(ptr as usize) {
                self.total += buffer.capacity();
            }
        }

        if let Some(nulls) = data.nulls() {
            let null_buf = nulls.inner().inner();
            let ptr = null_buf.as_ptr();
            if self.seen.insert(ptr as usize) {
                self.total += null_buf.capacity();
            }
        }

        for child in data.child_data() {
            self.record_array_data(child);
        }
    }

    pub fn record_batch(&mut self, batch: &RecordBatch) {
        for array in batch.columns() {
            self.record_array(array);
        }
    }

    pub fn total(&self) -> usize {
        self.total
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn test_memory_accumulator() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let slice = batch.slice(1, 2);

        let mut acc = MemoryAccumulator::default();

        // Should record whole buffer, not just slice
        acc.record_batch(&slice);
        assert_eq!(acc.total(), 3 * std::mem::size_of::<i32>());

        // Should not double count
        acc.record_batch(&slice);
        assert_eq!(acc.total(), 3 * std::mem::size_of::<i32>());
    }
}
