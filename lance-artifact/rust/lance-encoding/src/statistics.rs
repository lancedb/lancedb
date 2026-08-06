// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{
    fmt::{self},
    hash::{Hash, RandomState},
    sync::Arc,
};

use arrow_array::{Array, ArrowPrimitiveType, UInt64Array, cast::AsArray, types::UInt64Type};
use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};
use num_traits::PrimInt;

use crate::data::{
    AllNullDataBlock, DataBlock, DictionaryDataBlock, FixedSizeListBlock, FixedWidthDataBlock,
    NullableDataBlock, OpaqueBlock, StructDataBlock, VariableWidthBlock,
};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stat {
    BitWidth,
    DataSize,
    Cardinality,
    FixedSize,
    NullCount,
    MaxLength,
    RunCount,
    BytePositionEntropy,
}

impl fmt::Debug for Stat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BitWidth => write!(f, "BitWidth"),
            Self::DataSize => write!(f, "DataSize"),
            Self::Cardinality => write!(f, "Cardinality"),
            Self::FixedSize => write!(f, "FixedSize"),
            Self::NullCount => write!(f, "NullCount"),
            Self::MaxLength => write!(f, "MaxLength"),
            Self::RunCount => write!(f, "RunCount"),
            Self::BytePositionEntropy => write!(f, "BytePositionEntropy"),
        }
    }
}

impl fmt::Display for Stat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub trait ComputeStat {
    fn compute_stat(&mut self);
}

impl ComputeStat for DataBlock {
    fn compute_stat(&mut self) {
        match self {
            Self::Empty() => {}
            Self::Constant(_) => {}
            Self::AllNull(_) => {}
            Self::Nullable(data_block) => data_block.data.compute_stat(),
            Self::FixedWidth(data_block) => data_block.compute_stat(),
            Self::FixedSizeList(data_block) => data_block.compute_stat(),
            Self::VariableWidth(data_block) => data_block.compute_stat(),
            Self::Opaque(data_block) => data_block.compute_stat(),
            Self::Struct(data_block) => data_block.compute_stat(),
            Self::Dictionary(_) => {}
        }
    }
}

impl ComputeStat for VariableWidthBlock {
    fn compute_stat(&mut self) {
        if !self.block_info.0.read().unwrap().is_empty() {
            panic!("compute_stat should only be called once during DataBlock construction");
        }
        let data_size = self.data_size();
        let data_size_array = Arc::new(UInt64Array::from(vec![data_size]));

        let max_length_array = self.max_length();

        let mut info = self.block_info.0.write().unwrap();
        info.insert(Stat::DataSize, data_size_array);
        info.insert(Stat::MaxLength, max_length_array);
    }
}

impl ComputeStat for FixedWidthDataBlock {
    fn compute_stat(&mut self) {
        // compute this datablock's data_size
        let data_size = self.data_size();
        let data_size_array = Arc::new(UInt64Array::from(vec![data_size]));

        // compute this datablock's max_bit_width
        let max_bit_widths = self.max_bit_widths();

        // the MaxLength of FixedWidthDataBlock is it's self.bits_per_value / 8
        let max_len = self.bits_per_value / 8;
        let max_len_array = Arc::new(UInt64Array::from(vec![max_len]));

        // compute run count
        let run_count_array = self.run_count();

        // compute byte position entropy
        let byte_position_entropy = self.byte_position_entropy();

        let mut info = self.block_info.0.write().unwrap();
        info.insert(Stat::DataSize, data_size_array);
        info.insert(Stat::BitWidth, max_bit_widths);
        info.insert(Stat::MaxLength, max_len_array);
        info.insert(Stat::RunCount, run_count_array);
        info.insert(Stat::BytePositionEntropy, byte_position_entropy);
    }
}

impl ComputeStat for FixedSizeListBlock {
    fn compute_stat(&mut self) {
        // We leave the child stats unchanged.  This may seem odd (e.g. should bit width be the
        // bit width of the child * dimension?) but it's because we use these stats to determine
        // compression and we are currently just compressing the child data.
        //
        // There is a potential opportunity here to do better.  For example, if we have a FSL of
        // 4 32-bit integers then we should probably treat them as a single 128-bit integer or maybe
        // even 4 columns of 32-bit integers.  This might yield better compression.
        self.child.compute_stat();
    }
}

impl ComputeStat for OpaqueBlock {
    fn compute_stat(&mut self) {
        // compute this datablock's data_size
        let data_size = self.data_size();
        let data_size_array = Arc::new(UInt64Array::from(vec![data_size]));
        let mut info = self.block_info.0.write().unwrap();
        info.insert(Stat::DataSize, data_size_array);
    }
}

pub trait GetStat: fmt::Debug {
    fn get_stat(&self, stat: Stat) -> Option<Arc<dyn Array>>;

    fn expect_stat(&self, stat: Stat) -> Arc<dyn Array> {
        self.get_stat(stat)
            .unwrap_or_else(|| panic!("{:?} DataBlock does not have `{}` statistics.", self, stat))
    }

    fn expect_single_stat<T: ArrowPrimitiveType>(&self, stat: Stat) -> T::Native {
        let stat_value = self.expect_stat(stat);
        let stat_value = stat_value.as_primitive::<T>();
        if stat_value.len() != 1 {
            panic!(
                "{:?} DataBlock does not have exactly one value for `{} statistics.",
                self, stat
            );
        }
        stat_value.value(0)
    }
}

impl GetStat for DataBlock {
    fn get_stat(&self, stat: Stat) -> Option<Arc<dyn Array>> {
        match self {
            Self::Empty() => None,
            Self::Constant(_) => None,
            Self::AllNull(data_block) => data_block.get_stat(stat),
            Self::Nullable(data_block) => data_block.get_stat(stat),
            Self::FixedWidth(data_block) => data_block.get_stat(stat),
            Self::FixedSizeList(data_block) => data_block.get_stat(stat),
            Self::VariableWidth(data_block) => data_block.get_stat(stat),
            Self::Opaque(data_block) => data_block.get_stat(stat),
            Self::Struct(data_block) => data_block.get_stat(stat),
            Self::Dictionary(data_block) => data_block.get_stat(stat),
        }
    }
}

// NullableDataBlock will be deprecated in Lance 2.1.
impl GetStat for NullableDataBlock {
    // This function simply returns the statistics of the inner `DataBlock` of `NullableDataBlock`,
    // this is not accurate but `NullableDataBlock` is going to be deprecated in Lance 2.1 anyway.
    fn get_stat(&self, stat: Stat) -> Option<Arc<dyn Array>> {
        self.data.get_stat(stat)
    }
}

impl GetStat for VariableWidthBlock {
    fn get_stat(&self, stat: Stat) -> Option<Arc<dyn Array>> {
        {
            let block_info = self.block_info.0.read().unwrap();
            if block_info.is_empty() {
                panic!("get_stat should be called after statistics are computed.");
            }
            if let Some(stat_value) = block_info.get(&stat) {
                return Some(stat_value.clone());
            }
        }

        if stat != Stat::Cardinality {
            return None;
        }

        let computed = self.compute_cardinality();
        let mut block_info = self.block_info.0.write().unwrap();
        if block_info.is_empty() {
            panic!("get_stat should be called after statistics are computed.");
        }
        Some(
            block_info
                .entry(stat)
                .or_insert_with(|| computed.clone())
                .clone(),
        )
    }
}

impl GetStat for FixedSizeListBlock {
    fn get_stat(&self, stat: Stat) -> Option<Arc<dyn Array>> {
        let child_stat = self.child.get_stat(stat);
        match stat {
            Stat::MaxLength => child_stat.map(|max_length| {
                // this is conservative when working with variable length data as we shouldn't assume
                // that we have a list of all max-length elements but it's cheap and easy to calculate
                let max_length = max_length.as_primitive::<UInt64Type>().value(0);
                Arc::new(UInt64Array::from(vec![max_length * self.dimension])) as Arc<dyn Array>
            }),
            _ => child_stat,
        }
    }
}

impl VariableWidthBlock {
    // Caveat: the computation here assumes VariableWidthBlock.offsets maps directly to VariableWidthBlock.data
    // without any adjustment(for example, no null_adjustment for offsets)
    fn compute_cardinality(&self) -> Arc<dyn Array> {
        const PRECISION: u8 = 4;
        // The default hasher (currently sip hash 1-3) does not seem to give good results
        // with HLL.
        //
        // In particular, when using randomly generated 12-byte strings, the HLL count was
        // suggested a cardinality of 500 (out of 1000 unique items and hashes) at least 10%
        // of the time.
        //
        // Using xxhash3 consistently gives better results.
        let mut hll: HyperLogLogPlus<&[u8], xxhash_rust::xxh3::Xxh3Builder> =
            HyperLogLogPlus::new(PRECISION, xxhash_rust::xxh3::Xxh3Builder::default()).unwrap();

        match self.bits_per_offset {
            32 => {
                let offsets_ref = self.offsets.borrow_to_typed_slice::<u32>();
                let offsets: &[u32] = offsets_ref.as_ref();

                offsets
                    .iter()
                    .zip(offsets.iter().skip(1))
                    .for_each(|(&start, &end)| {
                        hll.insert(&self.data[start as usize..end as usize]);
                    });
                let cardinality = hll.count() as u64;
                Arc::new(UInt64Array::from(vec![cardinality]))
            }
            64 => {
                let offsets_ref = self.offsets.borrow_to_typed_slice::<u64>();
                let offsets: &[u64] = offsets_ref.as_ref();

                offsets
                    .iter()
                    .zip(offsets.iter().skip(1))
                    .for_each(|(&start, &end)| {
                        hll.insert(&self.data[start as usize..end as usize]);
                    });

                let cardinality = hll.count() as u64;
                Arc::new(UInt64Array::from(vec![cardinality]))
            }
            _ => {
                unreachable!("the bits_per_offset of VariableWidthBlock can only be 32 or 64")
            }
        }
    }

    fn max_length(&mut self) -> Arc<dyn Array> {
        match self.bits_per_offset {
            32 => {
                let offsets = self.offsets.borrow_to_typed_slice::<u32>();
                let offsets = offsets.as_ref();
                let max_len = offsets
                    .windows(2)
                    .map(|pair| pair[1] - pair[0])
                    .max()
                    .unwrap_or(0);
                Arc::new(UInt64Array::from(vec![max_len as u64]))
            }
            64 => {
                let offsets = self.offsets.borrow_to_typed_slice::<u64>();
                let offsets = offsets.as_ref();
                let max_len = offsets
                    .windows(2)
                    .map(|pair| pair[1] - pair[0])
                    .max()
                    .unwrap_or(0);
                Arc::new(UInt64Array::from(vec![max_len]))
            }
            _ => {
                unreachable!("the type of offsets in VariableWidth can only be u32 or u64");
            }
        }
    }
}

impl GetStat for AllNullDataBlock {
    fn get_stat(&self, stat: Stat) -> Option<Arc<dyn Array>> {
        match stat {
            Stat::NullCount => {
                let null_count = self.num_values;
                Some(Arc::new(UInt64Array::from(vec![null_count])))
            }
            Stat::DataSize => Some(Arc::new(UInt64Array::from(vec![0]))),
            _ => None,
        }
    }
}

impl GetStat for FixedWidthDataBlock {
    fn get_stat(&self, stat: Stat) -> Option<Arc<dyn Array>> {
        {
            let block_info = self.block_info.0.read().unwrap();

            if block_info.is_empty() {
                panic!("get_stat should be called after statistics are computed.");
            }

            if let Some(stat_value) = block_info.get(&stat) {
                return Some(stat_value.clone());
            }
        }

        if stat == Stat::Cardinality && (self.bits_per_value == 64 || self.bits_per_value == 128) {
            let computed = self.cardinality();
            let mut block_info = self.block_info.0.write().unwrap();
            Some(
                block_info
                    .entry(stat)
                    .or_insert_with(|| computed.clone())
                    .clone(),
            )
        } else {
            None
        }
    }
}

impl FixedWidthDataBlock {
    fn max_bit_widths(&mut self) -> Arc<dyn Array> {
        if self.num_values == 0 {
            return Arc::new(UInt64Array::from(vec![0u64]));
        }

        const CHUNK_SIZE: usize = 1024;

        fn calculate_max_bit_width<T: PrimInt>(slice: &[T], bits_per_value: u64) -> Vec<u64> {
            slice
                .chunks(CHUNK_SIZE)
                .map(|chunk| {
                    let max_value = chunk.iter().fold(T::zero(), |acc, &x| acc | x);
                    bits_per_value - max_value.leading_zeros() as u64
                })
                .collect()
        }

        match self.bits_per_value {
            8 => {
                let u8_slice = self.data.borrow_to_typed_slice::<u8>();
                let u8_slice = u8_slice.as_ref();
                Arc::new(UInt64Array::from(calculate_max_bit_width(
                    u8_slice,
                    self.bits_per_value,
                )))
            }
            16 => {
                let u16_slice = self.data.borrow_to_typed_slice::<u16>();
                let u16_slice = u16_slice.as_ref();
                Arc::new(UInt64Array::from(calculate_max_bit_width(
                    u16_slice,
                    self.bits_per_value,
                )))
            }
            32 => {
                let u32_slice = self.data.borrow_to_typed_slice::<u32>();
                let u32_slice = u32_slice.as_ref();
                Arc::new(UInt64Array::from(calculate_max_bit_width(
                    u32_slice,
                    self.bits_per_value,
                )))
            }
            64 => {
                let u64_slice = self.data.borrow_to_typed_slice::<u64>();
                let u64_slice = u64_slice.as_ref();
                Arc::new(UInt64Array::from(calculate_max_bit_width(
                    u64_slice,
                    self.bits_per_value,
                )))
            }
            _ => Arc::new(UInt64Array::from(vec![self.bits_per_value])),
        }
    }

    fn cardinality(&self) -> Arc<dyn Array> {
        match self.bits_per_value {
            64 => {
                let u64_slice_ref = self.data.borrow_to_typed_slice::<u64>();
                let u64_slice = u64_slice_ref.as_ref();

                const PRECISION: u8 = 4;
                let mut hll: HyperLogLogPlus<u64, xxhash_rust::xxh3::Xxh3Builder> =
                    HyperLogLogPlus::new(PRECISION, xxhash_rust::xxh3::Xxh3Builder::default())
                        .unwrap();
                for val in u64_slice {
                    hll.insert(val);
                }
                let cardinality = hll.count() as u64;
                Arc::new(UInt64Array::from(vec![cardinality]))
            }
            128 => {
                let u128_slice_ref = self.data.borrow_to_typed_slice::<u128>();
                let u128_slice = u128_slice_ref.as_ref();

                const PRECISION: u8 = 4;
                let mut hll: HyperLogLogPlus<u128, RandomState> =
                    HyperLogLogPlus::new(PRECISION, RandomState::new()).unwrap();
                for val in u128_slice {
                    hll.insert(val);
                }
                let cardinality = hll.count() as u64;
                Arc::new(UInt64Array::from(vec![cardinality]))
            }
            _ => unreachable!(),
        }
    }

    /// Counts the number of runs (consecutive sequences of equal values) in the data.
    ///
    /// A "run" is defined as a sequence of one or more consecutive equal values.
    /// For example:
    /// - `[1, 1, 2, 2, 2, 3]` has 3 runs: [1,1], [2,2,2], and [3]
    /// - `[1, 2, 3, 4]` has 4 runs (each value is its own run)
    /// - `[5, 5, 5, 5]` has 1 run
    ///
    /// This count is used to determine if RLE compression would be effective.
    /// Fewer runs relative to the total number of values indicates better RLE compression potential.
    fn run_count(&mut self) -> Arc<dyn Array> {
        if self.num_values == 0 {
            return Arc::new(UInt64Array::from(vec![0u64]));
        }

        // Inner function to count runs in typed data
        fn count_runs<T: PartialEq + Copy>(slice: &[T]) -> u64 {
            if slice.is_empty() {
                return 0;
            }

            // Start with 1 run (the first value)
            let mut runs = 1u64;
            let mut prev = slice[0];

            // Count value transitions (each transition indicates a new run)
            for &val in &slice[1..] {
                if val != prev {
                    runs += 1;
                    prev = val;
                }
            }

            runs
        }

        let run_count = match self.bits_per_value {
            8 => {
                let u8_slice = self.data.borrow_to_typed_slice::<u8>();
                count_runs(u8_slice.as_ref())
            }
            16 => {
                let u16_slice = self.data.borrow_to_typed_slice::<u16>();
                count_runs(u16_slice.as_ref())
            }
            32 => {
                let u32_slice = self.data.borrow_to_typed_slice::<u32>();
                count_runs(u32_slice.as_ref())
            }
            64 => {
                let u64_slice = self.data.borrow_to_typed_slice::<u64>();
                count_runs(u64_slice.as_ref())
            }
            128 => {
                let u128_slice = self.data.borrow_to_typed_slice::<u128>();
                count_runs(u128_slice.as_ref())
            }
            _ => self.num_values, // For other bit widths, assume no runs
        };

        Arc::new(UInt64Array::from(vec![run_count]))
    }

    /// Calculates entropy for each byte position.
    /// Returns an array with entropy values for each byte position (scaled by 1000 for integer storage).
    /// Lower entropy in specific byte positions indicates better suitability for BSS.
    fn byte_position_entropy(&mut self) -> Arc<dyn Array> {
        const SAMPLE_SIZE: usize = 64; // Sample more values for better entropy estimation

        // Get sample size (min of data length and SAMPLE_SIZE)
        let sample_count = (self.num_values as usize).min(SAMPLE_SIZE);

        if sample_count == 0 {
            // Return empty array for empty data
            return Arc::new(UInt64Array::from(vec![] as Vec<u64>));
        }

        let bytes_per_value = (self.bits_per_value / 8) as usize;
        let mut entropies = Vec::with_capacity(bytes_per_value);

        // Calculate entropy for each byte position
        for pos in 0..bytes_per_value {
            let mut byte_counts = [0u32; 256];

            // Count occurrences of each byte value at this position
            for i in 0..sample_count {
                let byte_offset = i * bytes_per_value + pos;
                if byte_offset < self.data.len() {
                    byte_counts[self.data[byte_offset] as usize] += 1;
                }
            }

            // Calculate Shannon entropy for this position
            let mut entropy = 0.0f64;
            let total = sample_count as f64;

            for &count in &byte_counts {
                if count > 0 {
                    let p = count as f64 / total;
                    entropy -= p * p.log2();
                }
            }

            // Scale by 1000 and store as integer for efficient storage
            entropies.push((entropy * 1000.0) as u64);
        }

        Arc::new(UInt64Array::from(entropies))
    }
}

impl GetStat for OpaqueBlock {
    fn get_stat(&self, stat: Stat) -> Option<Arc<dyn Array>> {
        let block_info = self.block_info.0.read().unwrap();

        if block_info.is_empty() {
            panic!("get_stat should be called after statistics are computed.");
        }
        block_info.get(&stat).cloned()
    }
}

impl GetStat for DictionaryDataBlock {
    fn get_stat(&self, _stat: Stat) -> Option<Arc<dyn Array>> {
        None
    }
}

impl GetStat for StructDataBlock {
    fn get_stat(&self, stat: Stat) -> Option<Arc<dyn Array>> {
        let block_info = self.block_info.0.read().unwrap();
        if block_info.is_empty() {
            panic!("get_stat should be called after statistics are computed.")
        }
        block_info.get(&stat).cloned()
    }
}

impl ComputeStat for StructDataBlock {
    fn compute_stat(&mut self) {
        let data_size = self.data_size();
        let data_size_array = Arc::new(UInt64Array::from(vec![data_size]));

        let max_len = self
            .children
            .iter()
            .map(|child| child.expect_single_stat::<UInt64Type>(Stat::MaxLength))
            .sum::<u64>();
        let max_len_array = Arc::new(UInt64Array::from(vec![max_len]));

        let mut info = self.block_info.0.write().unwrap();
        info.insert(Stat::DataSize, data_size_array);
        info.insert(Stat::MaxLength, max_len_array);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray,
        UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow_schema::{DataType, Field};
    use lance_arrow::DataTypeExt;
    use lance_datagen::{ArrayGeneratorExt, DEFAULT_SEED, RowCount, array};
    use rand::SeedableRng;

    use crate::statistics::{GetStat, Stat};

    use super::DataBlock;

    use arrow_array::{
        Array,
        cast::AsArray,
        types::{Int32Type, UInt64Type},
    };
    use arrow_select::concat::concat;
    #[test]
    fn test_data_size_stat() {
        let mut rng = rand_xoshiro::Xoshiro256PlusPlus::seed_from_u64(DEFAULT_SEED.0);
        let mut genn = array::rand::<Int32Type>().with_nulls(&[false, false, false]);
        let arr1 = genn.generate(RowCount::from(3), &mut rng).unwrap();
        let arr2 = genn.generate(RowCount::from(3), &mut rng).unwrap();
        let arr3 = genn.generate(RowCount::from(3), &mut rng).unwrap();
        let block = DataBlock::from_arrays(&[arr1.clone(), arr2.clone(), arr3.clone()], 9);

        let concatenated_array = concat(&[
            &*Arc::new(arr1.clone()) as &dyn Array,
            &*Arc::new(arr2.clone()) as &dyn Array,
            &*Arc::new(arr3.clone()) as &dyn Array,
        ])
        .unwrap();

        let data_size = block.expect_single_stat::<UInt64Type>(Stat::DataSize);

        let total_buffer_size: usize = concatenated_array
            .to_data()
            .buffers()
            .iter()
            .map(|buffer| buffer.len())
            .sum();
        assert!(data_size == total_buffer_size as u64);

        // test DataType::Binary
        let mut genn = lance_datagen::array::rand_type(&DataType::Binary);
        let arr = genn.generate(RowCount::from(3), &mut rng).unwrap();
        let block = DataBlock::from_array(arr.clone());
        let data_size = block.expect_single_stat::<UInt64Type>(Stat::DataSize);

        let total_buffer_size: usize = arr
            .to_data()
            .buffers()
            .iter()
            .map(|buffer| buffer.len())
            .sum();
        assert!(data_size == total_buffer_size as u64);

        // test DataType::Struct
        let fields = vec![
            Arc::new(Field::new("int_field", DataType::Int32, false)),
            Arc::new(Field::new("float_field", DataType::Float32, false)),
        ]
        .into();

        let mut genn = lance_datagen::array::rand_type(&DataType::Struct(fields));
        let arr = genn.generate(RowCount::from(3), &mut rng).unwrap();
        let block = DataBlock::from_array(arr.clone());
        let (_, arr_parts, _) = arr.as_struct().clone().into_parts();
        let total_buffer_size: usize = arr_parts
            .iter()
            .map(|arr| {
                arr.to_data()
                    .buffers()
                    .iter()
                    .map(|buffer| buffer.len())
                    .sum::<usize>()
            })
            .sum();
        let data_size = block.expect_single_stat::<UInt64Type>(Stat::DataSize);
        assert!(data_size == total_buffer_size as u64);

        // test DataType::Dictionary
        let mut genn = array::rand_type(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        ));
        let arr = genn.generate(RowCount::from(3), &mut rng).unwrap();
        let block = DataBlock::from_array(arr.clone());
        assert!(block.get_stat(Stat::DataSize).is_none());

        let mut genn = array::rand::<Int32Type>().with_nulls(&[false, true, false]);
        let arr = genn.generate(RowCount::from(3), &mut rng).unwrap();
        let block = DataBlock::from_array(arr.clone());
        let data_size = block.expect_single_stat::<UInt64Type>(Stat::DataSize);
        let total_buffer_size: usize = arr
            .to_data()
            .buffers()
            .iter()
            .map(|buffer| buffer.len())
            .sum();

        assert!(data_size == total_buffer_size as u64);
    }

    #[test]
    fn test_bit_width_stat_for_integers() {
        let int8_array = Int8Array::from(vec![1, 2, 3]);
        let array_ref: ArrayRef = Arc::new(int8_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![2])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);

        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref(),);

        let int8_array = Int8Array::from(vec![0x1, 0x2, 0x3, 0x7F]);
        let array_ref: ArrayRef = Arc::new(int8_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![7])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref(),);

        let int8_array = Int8Array::from(vec![0x1, 0x2, 0x3, 0xF, 0x1F]);
        let array_ref: ArrayRef = Arc::new(int8_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![5])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref(),);

        let int8_array = Int8Array::from(vec![-1, 2, 3]);
        let array_ref: ArrayRef = Arc::new(int8_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![8])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let int16_array = Int16Array::from(vec![1, 2, 3]);
        let array_ref: ArrayRef = Arc::new(int16_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![2])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let int16_array = Int16Array::from(vec![0x1, 0x2, 0x3, 0x7F]);
        let array_ref: ArrayRef = Arc::new(int16_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![7])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let int16_array = Int16Array::from(vec![0x1, 0x2, 0x3, 0xFF]);
        let array_ref: ArrayRef = Arc::new(int16_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![8])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let int16_array = Int16Array::from(vec![0x1, 0x2, 0x3, 0x1FF]);
        let array_ref: ArrayRef = Arc::new(int16_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![9])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let int16_array = Int16Array::from(vec![0x1, 0x2, 0x3, 0xF, 0x1F]);
        let array_ref: ArrayRef = Arc::new(int16_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![5])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let int16_array = Int16Array::from(vec![-1, 2, 3]);
        let array_ref: ArrayRef = Arc::new(int16_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![16])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let int32_array = Int32Array::from(vec![1, 2, 3]);
        let array_ref: ArrayRef = Arc::new(int32_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![2])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let int32_array = Int32Array::from(vec![0x1, 0x2, 0x3, 0xFF]);
        let array_ref: ArrayRef = Arc::new(int32_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![8])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let int32_array = Int32Array::from(vec![0x1, 0x2, 0x3, 0xFF, 0x1FF]);
        let array_ref: ArrayRef = Arc::new(int32_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![9])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let int32_array = Int32Array::from(vec![-1, 2, 3]);
        let array_ref: ArrayRef = Arc::new(int32_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![32])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let int32_array = Int32Array::from(vec![-1, 2, 3, -88]);
        let array_ref: ArrayRef = Arc::new(int32_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![32])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let int64_array = Int64Array::from(vec![1, 2, 3]);
        let array_ref: ArrayRef = Arc::new(int64_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![2])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let int64_array = Int64Array::from(vec![0x1, 0x2, 0x3, 0xFF]);
        let array_ref: ArrayRef = Arc::new(int64_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![8])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let int64_array = Int64Array::from(vec![0x1, 0x2, 0x3, 0xFF, 0x1FF]);
        let array_ref: ArrayRef = Arc::new(int64_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![9])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let int64_array = Int64Array::from(vec![-1, 2, 3]);
        let array_ref: ArrayRef = Arc::new(int64_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![64])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let int64_array = Int64Array::from(vec![-1, 2, 3, -88]);
        let array_ref: ArrayRef = Arc::new(int64_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![64])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint8_array = UInt8Array::from(vec![1, 2, 3]);
        let array_ref: ArrayRef = Arc::new(uint8_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![2])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint8_array = UInt8Array::from(vec![0x1, 0x2, 0x3, 0x7F]);
        let array_ref: ArrayRef = Arc::new(uint8_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![7])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint8_array = UInt8Array::from(vec![0x1, 0x2, 0x3, 0xF, 0x1F]);
        let array_ref: ArrayRef = Arc::new(uint8_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![5])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint8_array = UInt8Array::from(vec![1, 2, 3, 0xF]);
        let array_ref: ArrayRef = Arc::new(uint8_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![4])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint16_array = UInt16Array::from(vec![1, 2, 3]);
        let array_ref: ArrayRef = Arc::new(uint16_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![2])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint16_array = UInt16Array::from(vec![0x1, 0x2, 0x3, 0x7F]);
        let array_ref: ArrayRef = Arc::new(uint16_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![7])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint16_array = UInt16Array::from(vec![0x1, 0x2, 0x3, 0xFF]);
        let array_ref: ArrayRef = Arc::new(uint16_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![8])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint16_array = UInt16Array::from(vec![0x1, 0x2, 0x3, 0x1FF]);
        let array_ref: ArrayRef = Arc::new(uint16_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![9])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint16_array = UInt16Array::from(vec![0x1, 0x2, 0x3, 0xF, 0x1F]);
        let array_ref: ArrayRef = Arc::new(uint16_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![5])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint16_array = UInt16Array::from(vec![1, 2, 3, 0xFFFF]);
        let array_ref: ArrayRef = Arc::new(uint16_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![16])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint32_array = UInt32Array::from(vec![1, 2, 3]);
        let array_ref: ArrayRef = Arc::new(uint32_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![2])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint32_array = UInt32Array::from(vec![0x1, 0x2, 0x3, 0xFF]);
        let array_ref: ArrayRef = Arc::new(uint32_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![8])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref(),);

        let uint32_array = UInt32Array::from(vec![0x1, 0x2, 0x3, 0xFF, 0x1FF]);
        let array_ref: ArrayRef = Arc::new(uint32_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![9])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint32_array = UInt32Array::from(vec![1, 2, 3, 0xF]);
        let array_ref: ArrayRef = Arc::new(uint32_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![4])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint32_array = UInt32Array::from(vec![1, 2, 3, 0x77]);
        let array_ref: ArrayRef = Arc::new(uint32_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![7])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint64_array = UInt64Array::from(vec![1, 2, 3]);
        let array_ref: ArrayRef = Arc::new(uint64_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![2])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint64_array = UInt64Array::from(vec![0x1, 0x2, 0x3, 0xFF]);
        let array_ref: ArrayRef = Arc::new(uint64_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![8])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint64_array = UInt64Array::from(vec![0x1, 0x2, 0x3, 0xFF, 0x1FF]);
        let array_ref: ArrayRef = Arc::new(uint64_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![9])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint64_array = UInt64Array::from(vec![0, 2, 3, 0xFFFF]);
        let array_ref: ArrayRef = Arc::new(uint64_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![16])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());

        let uint64_array = UInt64Array::from(vec![1, 2, 3, 0xFFFF_FFFF_FFFF_FFFF]);
        let array_ref: ArrayRef = Arc::new(uint64_array);
        let block = DataBlock::from_array(array_ref);

        let expected_bit_width = Arc::new(UInt64Array::from(vec![64])) as ArrayRef;
        let actual_bit_width = block.expect_stat(Stat::BitWidth);
        assert_eq!(actual_bit_width.as_ref(), expected_bit_width.as_ref());
    }

    #[test]
    fn test_bit_width_stat_more_than_1024() {
        for data_type in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            let array1 = Int64Array::from(vec![3; 1024]);
            let array2 = Int64Array::from(vec![8; 1024]);
            let array3 = Int64Array::from(vec![-1; 10]);
            let array1 = arrow_cast::cast(&array1, &data_type).unwrap();
            let array2 = arrow_cast::cast(&array2, &data_type).unwrap();
            let array3 = arrow_cast::cast(&array3, &data_type).unwrap();

            let arrays: Vec<&dyn arrow_array::Array> =
                vec![array1.as_ref(), array2.as_ref(), array3.as_ref()];
            let concatenated = concat(&arrays).unwrap();
            let block = DataBlock::from_array(concatenated.clone());

            let expected_bit_width = Arc::new(UInt64Array::from(vec![
                2,
                4,
                (data_type.byte_width() * 8) as u64,
            ])) as ArrayRef;
            let actual_bit_widths = block.expect_stat(Stat::BitWidth);
            assert_eq!(actual_bit_widths.as_ref(), expected_bit_width.as_ref(),);
        }
    }

    #[test]
    fn test_bit_width_when_none() {
        let mut rng = rand_xoshiro::Xoshiro256PlusPlus::seed_from_u64(DEFAULT_SEED.0);
        let mut genn = lance_datagen::array::rand_type(&DataType::Binary);
        let arr = genn.generate(RowCount::from(3), &mut rng).unwrap();
        let block = DataBlock::from_array(arr.clone());
        assert!(block.get_stat(Stat::BitWidth).is_none(),);
    }

    #[test]
    fn test_cardinality_variable_width_datablock() {
        let string_array = StringArray::from(vec![Some("hello"), Some("world")]);
        let block = DataBlock::from_array(string_array);
        let expected_cardinality = 2;
        let actual_cardinality = block.expect_single_stat::<UInt64Type>(Stat::Cardinality);
        assert_eq!(actual_cardinality, expected_cardinality,);

        let string_array = StringArray::from(vec![
            Some("to be named by variables"),
            Some("to be passed as arguments to procedures"),
            Some("to be returned as values of procedures"),
        ]);
        let block = DataBlock::from_array(string_array);
        let expected_cardinality = 3;
        let actual_cardinality = block.expect_single_stat::<UInt64Type>(Stat::Cardinality);

        assert_eq!(actual_cardinality, expected_cardinality,);

        let string_array = StringArray::from(vec![
            Some("Samuel Eilenberg"),
            Some("Saunders Mac Lane"),
            Some("Samuel Eilenberg"),
        ]);
        let block = DataBlock::from_array(string_array);
        let expected_cardinality = 2;
        let actual_cardinality = block.expect_single_stat::<UInt64Type>(Stat::Cardinality);
        assert_eq!(actual_cardinality, expected_cardinality,);

        let string_array = LargeStringArray::from(vec![Some("hello"), Some("world")]);
        let block = DataBlock::from_array(string_array);
        let expected_cardinality = 2;
        let actual_cardinality = block.expect_single_stat::<UInt64Type>(Stat::Cardinality);
        assert_eq!(actual_cardinality, expected_cardinality,);

        let string_array = LargeStringArray::from(vec![
            Some("to be named by variables"),
            Some("to be passed as arguments to procedures"),
            Some("to be returned as values of procedures"),
        ]);
        let block = DataBlock::from_array(string_array);
        let expected_cardinality = 3;
        let actual_cardinality = block.expect_single_stat::<UInt64Type>(Stat::Cardinality);
        assert_eq!(actual_cardinality, expected_cardinality,);

        let string_array = LargeStringArray::from(vec![
            Some("Samuel Eilenberg"),
            Some("Saunders Mac Lane"),
            Some("Samuel Eilenberg"),
        ]);
        let block = DataBlock::from_array(string_array);
        let expected_cardinality = 2;
        let actual_cardinality = block.expect_single_stat::<UInt64Type>(Stat::Cardinality);
        assert_eq!(actual_cardinality, expected_cardinality,);
    }

    #[test]
    fn test_max_length_variable_width_datablock() {
        let string_array = StringArray::from(vec![Some("hello"), Some("world")]);
        let block = DataBlock::from_array(string_array.clone());
        let expected_max_length = string_array.value_length(0) as u64;
        let actual_max_length = block.expect_single_stat::<UInt64Type>(Stat::MaxLength);
        assert_eq!(actual_max_length, expected_max_length);

        let string_array = StringArray::from(vec![
            Some("to be named by variables"),
            Some("to be passed as arguments to procedures"), // string that has max length
            Some("to be returned as values of procedures"),
        ]);
        let block = DataBlock::from_array(string_array.clone());
        let expected_max_length = string_array.value_length(1) as u64;
        let actual_max_length = block.expect_single_stat::<UInt64Type>(Stat::MaxLength);
        assert_eq!(actual_max_length, expected_max_length);

        let string_array = StringArray::from(vec![
            Some("Samuel Eilenberg"),
            Some("Saunders Mac Lane"), // string that has max length
            Some("Samuel Eilenberg"),
        ]);
        let block = DataBlock::from_array(string_array.clone());
        let expected_max_length = string_array.value_length(1) as u64;
        let actual_max_length = block.expect_single_stat::<UInt64Type>(Stat::MaxLength);
        assert_eq!(actual_max_length, expected_max_length);

        let string_array = LargeStringArray::from(vec![Some("hello"), Some("world")]);
        let block = DataBlock::from_array(string_array.clone());
        let expected_max_length = string_array.value_length(1) as u64;
        let actual_max_length = block.expect_single_stat::<UInt64Type>(Stat::MaxLength);
        assert_eq!(actual_max_length, expected_max_length);

        let string_array = LargeStringArray::from(vec![
            Some("to be named by variables"),
            Some("to be passed as arguments to procedures"), // string that has max length
            Some("to be returned as values of procedures"),
        ]);
        let block = DataBlock::from_array(string_array.clone());
        let expected_max_length = string_array.value(1).len() as u64;
        let actual_max_length = block.expect_single_stat::<UInt64Type>(Stat::MaxLength);

        assert_eq!(actual_max_length, expected_max_length);
    }

    #[test]
    fn test_run_count_stat() {
        // Test with highly repetitive data
        let int32_array = Int32Array::from(vec![1, 1, 1, 2, 2, 2, 3, 3, 3]);
        let block = DataBlock::from_array(int32_array);
        let expected_run_count = 3;
        let actual_run_count = block.expect_single_stat::<UInt64Type>(Stat::RunCount);
        assert_eq!(actual_run_count, expected_run_count);

        // Test with no repetition
        let int32_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let block = DataBlock::from_array(int32_array);
        let expected_run_count = 5;
        let actual_run_count = block.expect_single_stat::<UInt64Type>(Stat::RunCount);
        assert_eq!(actual_run_count, expected_run_count);

        // Test with mixed pattern
        let int32_array = Int32Array::from(vec![1, 1, 2, 3, 3, 3, 4, 5, 5]);
        let block = DataBlock::from_array(int32_array);
        let expected_run_count = 5;
        let actual_run_count = block.expect_single_stat::<UInt64Type>(Stat::RunCount);
        assert_eq!(actual_run_count, expected_run_count);

        // Test with single value
        let int32_array = Int32Array::from(vec![42, 42, 42, 42, 42]);
        let block = DataBlock::from_array(int32_array);
        let expected_run_count = 1;
        let actual_run_count = block.expect_single_stat::<UInt64Type>(Stat::RunCount);
        assert_eq!(actual_run_count, expected_run_count);

        // Test with different data types
        let uint8_array = UInt8Array::from(vec![1, 1, 2, 2, 3, 3]);
        let block = DataBlock::from_array(uint8_array);
        let expected_run_count = 3;
        let actual_run_count = block.expect_single_stat::<UInt64Type>(Stat::RunCount);
        assert_eq!(actual_run_count, expected_run_count);

        let int64_array = Int64Array::from(vec![100, 100, 200, 300, 300]);
        let block = DataBlock::from_array(int64_array);
        let expected_run_count = 3;
        let actual_run_count = block.expect_single_stat::<UInt64Type>(Stat::RunCount);
        assert_eq!(actual_run_count, expected_run_count);
    }

    #[test]
    fn test_fixed_width_cardinality_is_lazy() {
        let int64_array = Int64Array::from(vec![1, 2, 3, 1, 2, 3, 1]);
        let block = DataBlock::from_array(int64_array);

        let DataBlock::FixedWidth(fixed) = &block else {
            panic!("Expected FixedWidth datablock");
        };

        let info = fixed.block_info.0.read().unwrap();
        assert!(info.contains_key(&Stat::DataSize));
        assert!(info.contains_key(&Stat::BitWidth));
        assert!(!info.contains_key(&Stat::Cardinality));
    }

    #[test]
    fn test_fixed_width_cardinality_computed_on_demand() {
        let int64_array = Int64Array::from(vec![1, 2, 3, 1, 2, 3, 1]);
        let block = DataBlock::from_array(int64_array);

        let cardinality = block.expect_single_stat::<UInt64Type>(Stat::Cardinality);
        assert_eq!(cardinality, 3);

        let DataBlock::FixedWidth(fixed) = &block else {
            panic!("Expected FixedWidth datablock");
        };

        let info = fixed.block_info.0.read().unwrap();
        assert!(info.contains_key(&Stat::Cardinality));
    }

    #[test]
    fn test_variable_width_cardinality_is_lazy() {
        let string_array = StringArray::from(vec!["a", "b", "a"]);
        let block = DataBlock::from_array(string_array);

        let DataBlock::VariableWidth(var) = &block else {
            panic!("Expected VariableWidth datablock");
        };

        {
            let info = var.block_info.0.read().unwrap();
            assert!(info.contains_key(&Stat::DataSize));
            assert!(info.contains_key(&Stat::MaxLength));
            assert!(!info.contains_key(&Stat::Cardinality));
        }

        let cardinality = block.expect_single_stat::<UInt64Type>(Stat::Cardinality);
        assert_eq!(cardinality, 2);

        let info = var.block_info.0.read().unwrap();
        assert!(info.contains_key(&Stat::Cardinality));
    }
}
