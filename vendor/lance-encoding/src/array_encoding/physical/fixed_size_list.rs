// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_schema::DataType;
use futures::{FutureExt, future::BoxFuture};
use lance_core::Result;
use log::trace;

use crate::{
    EncodingsIo,
    data::{DataBlock, FixedSizeListBlock},
    decoder::{PageScheduler, PrimitivePageDecoder},
    encoder::{ArrayEncoder, EncodedArray},
    format::ProtobufUtils,
};

/// A scheduler for fixed size lists of primitive values
///
/// This scheduler is, itself, primitive
#[derive(Debug)]
pub struct FixedListScheduler {
    items_scheduler: Box<dyn PageScheduler>,
    dimension: u32,
}

impl FixedListScheduler {
    pub fn new(items_scheduler: Box<dyn PageScheduler>, dimension: u32) -> Self {
        Self {
            items_scheduler,
            dimension,
        }
    }
}

impl PageScheduler for FixedListScheduler {
    fn schedule_ranges(
        &self,
        ranges: &[std::ops::Range<u64>],
        scheduler: &Arc<dyn EncodingsIo>,
        top_level_row: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PrimitivePageDecoder>>> {
        let expanded_ranges = ranges
            .iter()
            .map(|range| (range.start * self.dimension as u64)..(range.end * self.dimension as u64))
            .collect::<Vec<_>>();
        trace!(
            "Expanding {} fsl ranges across {}..{} to item ranges across {}..{}",
            ranges.len(),
            ranges[0].start,
            ranges[ranges.len() - 1].end,
            expanded_ranges[0].start,
            expanded_ranges[expanded_ranges.len() - 1].end
        );
        let inner_page_decoder =
            self.items_scheduler
                .schedule_ranges(&expanded_ranges, scheduler, top_level_row);
        let dimension = self.dimension;
        async move {
            let items_decoder = inner_page_decoder.await?;
            Ok(Box::new(FixedListDecoder {
                items_decoder,
                dimension: dimension as u64,
            }) as Box<dyn PrimitivePageDecoder>)
        }
        .boxed()
    }
}

pub struct FixedListDecoder {
    items_decoder: Box<dyn PrimitivePageDecoder>,
    dimension: u64,
}

impl PrimitivePageDecoder for FixedListDecoder {
    fn decode(&self, rows_to_skip: u64, num_rows: u64) -> Result<DataBlock> {
        let rows_to_skip = rows_to_skip * self.dimension;
        let num_child_rows = num_rows * self.dimension;
        let child_data = self.items_decoder.decode(rows_to_skip, num_child_rows)?;
        Ok(DataBlock::FixedSizeList(FixedSizeListBlock {
            child: Box::new(child_data),
            dimension: self.dimension,
        }))
    }
}

#[derive(Debug)]
pub struct FslEncoder {
    items_encoder: Box<dyn ArrayEncoder>,
    dimension: u32,
}

impl FslEncoder {
    pub fn new(items_encoder: Box<dyn ArrayEncoder>, dimension: u32) -> Self {
        Self {
            items_encoder,
            dimension,
        }
    }
}

impl ArrayEncoder for FslEncoder {
    fn encode(
        &self,
        data: DataBlock,
        data_type: &DataType,
        buffer_index: &mut u32,
    ) -> Result<EncodedArray> {
        let inner_type = match data_type {
            DataType::FixedSizeList(inner_field, _) => inner_field.data_type().clone(),
            _ => panic!("Expected fixed size list data type and got {}", data_type),
        };
        let data = data.as_fixed_size_list().unwrap();
        let child = *data.child;

        let encoded_data = self
            .items_encoder
            .encode(child, &inner_type, buffer_index)?;

        let data = DataBlock::FixedSizeList(FixedSizeListBlock {
            child: Box::new(encoded_data.data),
            dimension: self.dimension as u64,
        });

        let encoding =
            ProtobufUtils::fsl_encoding(self.dimension as u64, encoded_data.encoding, false);
        Ok(EncodedArray { data, encoding })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow_array::{FixedSizeListArray, Int32Array, types::Int32Type};
    use arrow_buffer::{BooleanBuffer, NullBuffer};
    use arrow_schema::{DataType, Field};
    use lance_datagen::{ArrayGeneratorExt, RowCount, array, gen_array};

    use crate::testing::{
        TestCases, TestEncoding, check_basic_random_case, check_round_trip_encoding_of_data,
    };
    use rstest::rstest;

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_value_fsl_primitive(
        #[values(DataType::Int8, DataType::Float32, DataType::Float64)] data_type: DataType,
        #[values(
            TestEncoding::Array,
            TestEncoding::StructuralU16,
            TestEncoding::StructuralU32,
            TestEncoding::StructuralSparse
        )]
        encoding: TestEncoding,
        #[values(4096, 1024 * 1024)] page_size: u64,
        #[values(false, true)] use_slicing: bool,
    ) {
        let inner_field = Field::new("item", data_type, true);
        let data_type = DataType::FixedSizeList(Arc::new(inner_field), 16);
        let field = Field::new("", data_type, false);
        check_basic_random_case(field, encoding, page_size, use_slicing).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_simple_fsl() {
        // [0, NULL], NULL, [4, 5]
        let items = Arc::new(Int32Array::from(vec![
            Some(0),
            None,
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ]));
        let items_field = Arc::new(Field::new("item", DataType::Int32, true));
        let list_nulls = NullBuffer::new(BooleanBuffer::from(vec![true, false, true]));
        let list = Arc::new(FixedSizeListArray::new(
            items_field,
            2,
            items,
            Some(list_nulls),
        ));

        let test_cases = TestCases::default()
            .with_range(0..3)
            .with_range(0..2)
            .with_range(1..3)
            .with_indices(vec![0, 1, 2])
            .with_indices(vec![1])
            .with_indices(vec![2])
            .with_structural_encodings();

        check_round_trip_encoding_of_data(vec![list], &test_cases, HashMap::default()).await;
    }

    #[test_log::test(tokio::test)]
    #[ignore]
    async fn test_simple_wide_fsl() {
        let items = gen_array(array::rand::<Int32Type>().with_random_nulls(0.1))
            .into_array_rows(RowCount::from(4096))
            .unwrap();
        let items_field = Arc::new(Field::new("item", DataType::Int32, true));
        let list_nulls = NullBuffer::new(BooleanBuffer::from(vec![true, false, true, false]));
        let list = Arc::new(FixedSizeListArray::new(
            items_field,
            1024,
            items,
            Some(list_nulls),
        ));

        let test_cases = TestCases::default()
            .with_range(0..3)
            .with_range(0..2)
            .with_range(1..3)
            .with_indices(vec![0, 1, 2])
            .with_indices(vec![1])
            .with_indices(vec![2])
            .with_structural_encodings();

        check_round_trip_encoding_of_data(vec![list], &test_cases, HashMap::default()).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_nested_fsl() {
        // [[0, 1], NULL], NULL, [[8, 9], [NULL, 11]]
        let items = Arc::new(Int32Array::from(vec![
            Some(0),
            Some(1),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(8),
            Some(9),
            None,
            Some(11),
        ]));
        let items_field = Arc::new(Field::new("item", DataType::Int32, true));
        let inner_list_nulls = NullBuffer::new(BooleanBuffer::from(vec![
            true, false, false, false, true, true,
        ]));
        let inner_list = Arc::new(FixedSizeListArray::new(
            items_field.clone(),
            2,
            items,
            Some(inner_list_nulls),
        ));
        let inner_list_field = Arc::new(Field::new(
            "item",
            DataType::FixedSizeList(items_field, 2),
            true,
        ));
        let outer_list_nulls = NullBuffer::new(BooleanBuffer::from(vec![true, false, true]));
        let outer_list = Arc::new(FixedSizeListArray::new(
            inner_list_field,
            2,
            inner_list,
            Some(outer_list_nulls),
        ));

        let test_cases = TestCases::default()
            .with_range(0..3)
            .with_range(0..2)
            .with_range(1..3)
            .with_indices(vec![0, 1, 2])
            .with_indices(vec![2])
            .with_structural_encodings();

        check_round_trip_encoding_of_data(vec![outer_list], &test_cases, HashMap::default()).await;
    }
}
