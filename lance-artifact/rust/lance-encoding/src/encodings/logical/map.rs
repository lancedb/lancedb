// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{ops::Range, sync::Arc};

use arrow_array::{Array, ArrayRef, ListArray, MapArray};
use arrow_schema::DataType;
use futures::future::BoxFuture;
use lance_arrow::deepcopy::deep_copy_nulls;
use lance_arrow::list::ListArrayExt;
use lance_core::{Error, Result};

use crate::{
    decoder::{
        DecodedArray, FilterExpression, ScheduledScanLine, SchedulerContext,
        StructuralDecodeArrayTask, StructuralFieldDecoder, StructuralFieldScheduler,
        StructuralSchedulingJob,
    },
    encoder::{EncodeTask, FieldEncoder, OutOfLineBuffers},
    repdef::RepDefBuilder,
};

/// A structural encoder for map fields
///
/// Map in Arrow is represented as List<Struct<key, value>>
/// The map's offsets are added to the rep/def builder
/// and the map's entries (struct array) are passed to the child encoder
pub struct MapStructuralEncoder {
    keep_original_array: bool,
    child: Box<dyn FieldEncoder>,
}

impl MapStructuralEncoder {
    pub fn new(keep_original_array: bool, child: Box<dyn FieldEncoder>) -> Self {
        Self {
            keep_original_array,
            child,
        }
    }
}

impl FieldEncoder for MapStructuralEncoder {
    fn maybe_encode(
        &mut self,
        array: ArrayRef,
        external_buffers: &mut OutOfLineBuffers,
        mut repdef: RepDefBuilder,
        row_number: u64,
        num_rows: u64,
    ) -> Result<Vec<EncodeTask>> {
        let map_array = array
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("MapEncoder used for non-map data");

        // Add offsets to RepDefBuilder to handle nullability and list structure
        let has_garbage_values = if self.keep_original_array {
            repdef.add_offsets(map_array.offsets().clone(), array.nulls().cloned())
        } else {
            repdef.add_offsets(map_array.offsets().clone(), deep_copy_nulls(array.nulls()))
        };

        // MapArray is physically a ListArray, so convert and use ListArrayExt
        let list_array: ListArray = map_array.clone().into();
        let entries = if has_garbage_values {
            list_array.filter_garbage_nulls().trimmed_values()
        } else {
            list_array.trimmed_values()
        };

        self.child
            .maybe_encode(entries, external_buffers, repdef, row_number, num_rows)
    }

    fn flush(&mut self, external_buffers: &mut OutOfLineBuffers) -> Result<Vec<EncodeTask>> {
        self.child.flush(external_buffers)
    }

    fn num_columns(&self) -> u32 {
        self.child.num_columns()
    }

    fn finish(
        &mut self,
        external_buffers: &mut OutOfLineBuffers,
    ) -> BoxFuture<'_, Result<Vec<crate::encoder::EncodedColumn>>> {
        self.child.finish(external_buffers)
    }
}

#[derive(Debug)]
pub struct StructuralMapScheduler {
    child: Box<dyn StructuralFieldScheduler>,
}

impl StructuralMapScheduler {
    pub fn new(child: Box<dyn StructuralFieldScheduler>) -> Self {
        Self { child }
    }
}

impl StructuralFieldScheduler for StructuralMapScheduler {
    fn schedule_ranges<'a>(
        &'a self,
        ranges: &[Range<u64>],
        filter: &FilterExpression,
    ) -> Result<Box<dyn StructuralSchedulingJob + 'a>> {
        let child = self.child.schedule_ranges(ranges, filter)?;

        Ok(Box::new(StructuralMapSchedulingJob::new(child)))
    }

    fn initialize<'a>(
        &'a mut self,
        filter: &'a FilterExpression,
        context: &'a SchedulerContext,
    ) -> BoxFuture<'a, Result<()>> {
        self.child.initialize(filter, context)
    }
}

/// Scheduling job for map data
///
/// Scheduling is handled by the child encoder (struct) and nothing special
/// happens here, similar to list.
#[derive(Debug)]
struct StructuralMapSchedulingJob<'a> {
    child: Box<dyn StructuralSchedulingJob + 'a>,
}

impl<'a> StructuralMapSchedulingJob<'a> {
    fn new(child: Box<dyn StructuralSchedulingJob + 'a>) -> Self {
        Self { child }
    }
}

impl StructuralSchedulingJob for StructuralMapSchedulingJob<'_> {
    fn schedule_next(&mut self, context: &mut SchedulerContext) -> Result<Vec<ScheduledScanLine>> {
        self.child.schedule_next(context)
    }
}

#[derive(Debug)]
pub struct StructuralMapDecoder {
    child: Box<dyn StructuralFieldDecoder>,
    data_type: DataType,
}

impl StructuralMapDecoder {
    pub fn new(child: Box<dyn StructuralFieldDecoder>, data_type: DataType) -> Self {
        Self { child, data_type }
    }
}

impl StructuralFieldDecoder for StructuralMapDecoder {
    fn accept_page(&mut self, child: crate::decoder::LoadedPageShard) -> Result<()> {
        self.child.accept_page(child)
    }

    fn drain(&mut self, num_rows: u64) -> Result<Box<dyn StructuralDecodeArrayTask>> {
        let child_task = self.child.drain(num_rows)?;
        Ok(Box::new(StructuralMapDecodeTask::new(
            child_task,
            self.data_type.clone(),
        )))
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

#[derive(Debug)]
struct StructuralMapDecodeTask {
    child_task: Box<dyn StructuralDecodeArrayTask>,
    data_type: DataType,
}

impl StructuralMapDecodeTask {
    fn new(child_task: Box<dyn StructuralDecodeArrayTask>, data_type: DataType) -> Self {
        Self {
            child_task,
            data_type,
        }
    }
}

impl StructuralDecodeArrayTask for StructuralMapDecodeTask {
    fn decode(self: Box<Self>) -> Result<DecodedArray> {
        let DecodedArray {
            array,
            mut repdef,
            data_size,
        } = self.child_task.decode()?;

        // Decode the offsets from RepDef
        let (offsets, validity) = repdef.unravel_offsets::<i32>()?;

        // Extract the entries field and keys_sorted from the map data type
        let (entries_field, keys_sorted) = match &self.data_type {
            DataType::Map(field, keys_sorted) => {
                if *keys_sorted {
                    return Err(Error::not_supported_source(
                        "Map type decoder does not support keys_sorted=true now"
                            .to_string()
                            .into(),
                    ));
                }
                (field.clone(), *keys_sorted)
            }
            _ => {
                return Err(Error::schema(
                    "Map decoder did not have a map field".to_string(),
                ));
            }
        };

        // Convert the decoded array to StructArray
        let entries = array
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .ok_or_else(|| Error::schema("Map entries should be a StructArray".to_string()))?
            .clone();

        // Build the MapArray from offsets, entries, validity, and keys_sorted
        let map_array = MapArray::try_new(entries_field, offsets, entries, validity, keys_sorted)
            .map_err(|error| Error::invalid_input_source(error.to_string().into()))?;

        Ok(DecodedArray {
            array: Arc::new(map_array),
            repdef,
            data_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow_array::{
        Array, Int32Array, MapArray, StringArray, StructArray,
        builder::{Int32Builder, MapBuilder, StringBuilder},
    };
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field, Fields};

    use crate::decoder::{DecodedArray, StructuralDecodeArrayTask};
    use crate::encoder::{ColumnIndexSequence, EncodingOptions};
    use crate::encodings::logical::primitive::sparse::{
        SparseCountSet, SparsePositionSet, SparseStructuralLayerPlan, SparseStructuralPlan,
        SparseValidityMeaning, SparseValiditySet,
    };
    use crate::repdef::{CompositeRepDefUnraveler, RepDefUnraveler};
    use crate::testing::{
        TestCases, TestEncoding, check_round_trip_encoding_of_data, test_encoding_strategy,
    };
    use arrow_schema::Field as ArrowField;
    use lance_core::datatypes::Field as LanceField;

    use super::StructuralMapDecodeTask;

    fn make_map_type(key_type: DataType, value_type: DataType) -> DataType {
        // Note: Arrow MapBuilder uses "keys" and "values" as field names (plural)
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", key_type, false),
                Field::new("values", value_type, true),
            ])),
            false,
        );
        DataType::Map(Arc::new(entries), false)
    }

    #[derive(Debug)]
    struct StaticMapEntriesTask {
        entries: StructArray,
        repdef: CompositeRepDefUnraveler,
    }

    impl StructuralDecodeArrayTask for StaticMapEntriesTask {
        fn decode(self: Box<Self>) -> lance_core::Result<DecodedArray> {
            let Self { entries, repdef } = *self;
            Ok(DecodedArray {
                array: Arc::new(entries),
                repdef,
                data_size: 0,
            })
        }
    }

    #[test]
    fn malformed_sparse_map_entries_return_invalid_input() {
        let entry_fields = Fields::from(vec![
            Field::new("keys", DataType::Int32, false),
            Field::new("values", DataType::Int32, true),
        ]);
        let entries = StructArray::try_new(
            entry_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![2])),
            ],
            Some(NullBuffer::from(vec![false])),
        )
        .unwrap();
        let validity = SparseValiditySet {
            meaning: SparseValidityMeaning::NullPositions,
            positions: SparsePositionSet::Empty,
        };
        let plan = SparseStructuralPlan {
            layers: vec![SparseStructuralLayerPlan::List {
                num_slots: 1,
                num_child_slots: 1,
                non_empty_positions: SparsePositionSet::All { len: 1 },
                counts: SparseCountSet::Constant { value: 1, len: 1 },
                validity,
            }],
            num_items: 1,
            num_visible_items: 1,
        };
        let child_task = StaticMapEntriesTask {
            entries,
            repdef: CompositeRepDefUnraveler::new(vec![RepDefUnraveler::new_sparse(plan)]),
        };
        let map_type = DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(entry_fields), false)),
            false,
        );

        let Err(err) =
            Box::new(StructuralMapDecodeTask::new(Box::new(child_task), map_type)).decode()
        else {
            panic!("expected malformed map entries to be rejected");
        };
        assert!(matches!(err, lance_core::Error::InvalidInput { .. }));
    }

    #[test_log::test(tokio::test)]
    async fn test_simple_map() {
        // Create a simple Map<String, Int32>
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::new();
        let mut map_builder = MapBuilder::new(None, string_builder, int_builder);

        // Map 1: {"key1": 10, "key2": 20}
        map_builder.keys().append_value("key1");
        map_builder.values().append_value(10);
        map_builder.keys().append_value("key2");
        map_builder.values().append_value(20);
        map_builder.append(true).unwrap();

        // Map 2: {"key3": 30}
        map_builder.keys().append_value("key3");
        map_builder.values().append_value(30);
        map_builder.append(true).unwrap();

        let map_array = map_builder.finish();

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_u32_structural_encodings();

        check_round_trip_encoding_of_data(vec![Arc::new(map_array)], &test_cases, HashMap::new())
            .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_empty_maps() {
        // Test maps with empty entries
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::new();
        let mut map_builder = MapBuilder::new(None, string_builder, int_builder);

        // Map 1: {"a": 1}
        map_builder.keys().append_value("a");
        map_builder.values().append_value(1);
        map_builder.append(true).unwrap();

        // Map 2: {} (empty)
        map_builder.append(true).unwrap();

        // Map 3: null
        map_builder.append(false).unwrap();

        // Map 4: {} (empty)
        map_builder.append(true).unwrap();

        let map_array = map_builder.finish();

        let test_cases = TestCases::default()
            .with_range(0..4)
            .with_indices(vec![1])
            .with_indices(vec![2])
            .with_u32_structural_encodings();

        check_round_trip_encoding_of_data(vec![Arc::new(map_array)], &test_cases, HashMap::new())
            .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_map_with_null_values() {
        // Test Map<String, Int32> with null values
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::new();
        let mut map_builder = MapBuilder::new(None, string_builder, int_builder);

        // Map 1: {"key1": 10, "key2": null}
        map_builder.keys().append_value("key1");
        map_builder.values().append_value(10);
        map_builder.keys().append_value("key2");
        map_builder.values().append_null();
        map_builder.append(true).unwrap();

        // Map 2: {"key3": null}
        map_builder.keys().append_value("key3");
        map_builder.values().append_null();
        map_builder.append(true).unwrap();

        let map_array = map_builder.finish();

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_indices(vec![0])
            .with_indices(vec![1])
            .with_u32_structural_encodings();

        check_round_trip_encoding_of_data(vec![Arc::new(map_array)], &test_cases, HashMap::new())
            .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_map_in_struct() {
        // Test Struct containing Map
        // Struct<id: Int32, properties: Map<String, String>>

        let string_key_builder = StringBuilder::new();
        let string_val_builder = StringBuilder::new();
        let mut map_builder = MapBuilder::new(None, string_key_builder, string_val_builder);

        // First struct: id=1, properties={"name": "Alice", "city": "NYC"}
        map_builder.keys().append_value("name");
        map_builder.values().append_value("Alice");
        map_builder.keys().append_value("city");
        map_builder.values().append_value("NYC");
        map_builder.append(true).unwrap();

        // Second struct: id=2, properties={"name": "Bob"}
        map_builder.keys().append_value("name");
        map_builder.values().append_value("Bob");
        map_builder.append(true).unwrap();

        // Third struct: id=3, properties=null
        map_builder.append(false).unwrap();

        let map_array = Arc::new(map_builder.finish());
        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3]));

        let struct_array = StructArray::new(
            Fields::from(vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "properties",
                    make_map_type(DataType::Utf8, DataType::Utf8),
                    true,
                ),
            ]),
            vec![id_array, map_array],
            None,
        );

        let test_cases = TestCases::default()
            .with_range(0..3)
            .with_indices(vec![0, 2])
            .with_u32_structural_encodings();

        check_round_trip_encoding_of_data(
            vec![Arc::new(struct_array)],
            &test_cases,
            HashMap::new(),
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_map_in_nullable_struct() {
        // Test Struct<Map> where null struct rows have garbage map entries.
        // The encoder must filter these garbage entries before encoding.
        let entries_fields = Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Int32, true),
        ]);
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(entries_fields.clone()),
            false,
        ));
        let map_entries = StructArray::new(
            entries_fields,
            vec![
                Arc::new(StringArray::from(vec!["a", "garbage", "b"])),
                Arc::new(Int32Array::from(vec![1, 999, 2])),
            ],
            None,
        );
        // map0: {"a": 1}, map1 (garbage): {"garbage": 999}, map2: {"b": 2}
        let map_array: Arc<dyn Array> = Arc::new(MapArray::new(
            entries_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2, 3])),
            map_entries,
            None, // No nulls at map level - nulls come from struct
            false,
        ));

        let struct_array = StructArray::new(
            Fields::from(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("props", map_array.data_type().clone(), true),
            ]),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
                map_array,
            ],
            Some(NullBuffer::from(vec![true, false, true])), // Middle row is null
        );

        let test_cases = TestCases::default()
            .with_range(0..3)
            .with_u32_structural_encodings();

        check_round_trip_encoding_of_data(
            vec![Arc::new(struct_array)],
            &test_cases,
            HashMap::new(),
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_list_of_maps() {
        // Test List<Map<String, Int32>>
        use arrow_array::builder::ListBuilder;

        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::new();
        let map_builder = MapBuilder::new(None, string_builder, int_builder);
        let mut list_builder = ListBuilder::new(map_builder);

        // List 1: [{"a": 1}, {"b": 2}]
        list_builder.values().keys().append_value("a");
        list_builder.values().values().append_value(1);
        list_builder.values().append(true).unwrap();

        list_builder.values().keys().append_value("b");
        list_builder.values().values().append_value(2);
        list_builder.values().append(true).unwrap();

        list_builder.append(true);

        // List 2: [{"c": 3}]
        list_builder.values().keys().append_value("c");
        list_builder.values().values().append_value(3);
        list_builder.values().append(true).unwrap();

        list_builder.append(true);

        // List 3: [] (empty list)
        list_builder.append(true);

        let list_array = list_builder.finish();

        let test_cases = TestCases::default()
            .with_range(0..3)
            .with_indices(vec![0, 2])
            .with_u32_structural_encodings();

        check_round_trip_encoding_of_data(vec![Arc::new(list_array)], &test_cases, HashMap::new())
            .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_nested_map() {
        // Test Map<String, Map<String, Int32>>
        // This is more complex as we need to build nested maps manually

        // Build inner maps first
        let inner_string_builder = StringBuilder::new();
        let inner_int_builder = Int32Builder::new();
        let mut inner_map_builder1 = MapBuilder::new(None, inner_string_builder, inner_int_builder);

        // Inner map 1: {"x": 10}
        inner_map_builder1.keys().append_value("x");
        inner_map_builder1.values().append_value(10);
        inner_map_builder1.append(true).unwrap();

        // Inner map 2: {"y": 20, "z": 30}
        inner_map_builder1.keys().append_value("y");
        inner_map_builder1.values().append_value(20);
        inner_map_builder1.keys().append_value("z");
        inner_map_builder1.values().append_value(30);
        inner_map_builder1.append(true).unwrap();

        let inner_maps = Arc::new(inner_map_builder1.finish());

        // Build outer map keys
        let outer_keys = Arc::new(StringArray::from(vec!["key1", "key2"]));

        // Build outer map structure
        let entries_struct = StructArray::new(
            Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new(
                    "value",
                    make_map_type(DataType::Utf8, DataType::Int32),
                    true,
                ),
            ]),
            vec![outer_keys, inner_maps],
            None,
        );

        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2]));
        let entries_field = Field::new("entries", entries_struct.data_type().clone(), false);

        let outer_map = MapArray::new(
            Arc::new(entries_field),
            offsets,
            entries_struct,
            None,
            false,
        );

        let test_cases = TestCases::default()
            .with_range(0..1)
            .with_u32_structural_encodings();

        check_round_trip_encoding_of_data(vec![Arc::new(outer_map)], &test_cases, HashMap::new())
            .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_map_different_key_types() {
        // Test Map<Int32, String> (integer keys)
        let int_builder = Int32Builder::new();
        let string_builder = StringBuilder::new();
        let mut map_builder = MapBuilder::new(None, int_builder, string_builder);

        // Map 1: {1: "one", 2: "two"}
        map_builder.keys().append_value(1);
        map_builder.values().append_value("one");
        map_builder.keys().append_value(2);
        map_builder.values().append_value("two");
        map_builder.append(true).unwrap();

        // Map 2: {3: "three"}
        map_builder.keys().append_value(3);
        map_builder.values().append_value("three");
        map_builder.append(true).unwrap();

        let map_array = map_builder.finish();

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_indices(vec![0, 1])
            .with_u32_structural_encodings();

        check_round_trip_encoding_of_data(vec![Arc::new(map_array)], &test_cases, HashMap::new())
            .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_map_with_extreme_sizes() {
        // Test maps with large number of entries
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::new();
        let mut map_builder = MapBuilder::new(None, string_builder, int_builder);

        // Create a map with many entries
        for i in 0..100 {
            map_builder.keys().append_value(format!("key{}", i));
            map_builder.values().append_value(i);
        }
        map_builder.append(true).unwrap();

        // Create a second map with no entries
        map_builder.append(true).unwrap();

        let map_array = map_builder.finish();

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_u32_structural_encodings();

        check_round_trip_encoding_of_data(vec![Arc::new(map_array)], &test_cases, HashMap::new())
            .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_map_all_null() {
        // Test map where all entries are null
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::new();
        let mut map_builder = MapBuilder::new(None, string_builder, int_builder);

        // All null maps
        map_builder.append(false).unwrap(); // null
        map_builder.append(false).unwrap(); // null

        let map_array = map_builder.finish();

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_u32_structural_encodings();

        check_round_trip_encoding_of_data(vec![Arc::new(map_array)], &test_cases, HashMap::new())
            .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_map_encoder_keep_original_array_scenarios() {
        // Test scenarios that highlight the difference between keep_original_array=true/false
        // This test focuses on round-trip behavior which should be equivalent in both cases
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::new();
        let mut map_builder = MapBuilder::new(None, string_builder, int_builder);

        // Create a map with mixed null and non-null values to test both scenarios
        // Map 1: {"key1": 10, "key2": null}
        map_builder.keys().append_value("key1");
        map_builder.values().append_value(10);
        map_builder.keys().append_value("key2");
        map_builder.values().append_null();
        map_builder.append(true).unwrap();

        // Map 2: null
        map_builder.append(false).unwrap();

        // Map 3: {"key3": 30}
        map_builder.keys().append_value("key3");
        map_builder.values().append_value(30);
        map_builder.append(true).unwrap();

        let map_array = map_builder.finish();

        let test_cases = TestCases::default()
            .with_range(0..3)
            .with_indices(vec![0, 1, 2])
            .with_u32_structural_encodings();

        // This test ensures that regardless of the internal keep_original_array setting,
        // the end-to-end behavior produces equivalent results
        check_round_trip_encoding_of_data(vec![Arc::new(map_array)], &test_cases, HashMap::new())
            .await;
    }

    #[test]
    fn test_map_not_supported_write_in_v2_1() {
        // Create a map field using Arrow Field first, then convert to Lance Field
        let map_arrow_field = ArrowField::new(
            "map_field",
            make_map_type(DataType::Utf8, DataType::Int32),
            true,
        );
        let map_field = LanceField::try_from(&map_arrow_field).unwrap();

        // Test encoder: Try to create encoder with V2_1 version - should fail
        let encoder_strategy = test_encoding_strategy(TestEncoding::StructuralU16);
        let mut column_index = ColumnIndexSequence::default();
        let options = EncodingOptions::default();

        let encoder_result = crate::testing::create_test_field_encoder(
            encoder_strategy.as_ref(),
            &map_field,
            &mut column_index,
            &options,
        );

        assert!(
            encoder_result.is_err(),
            "Map type should not be supported in V2_1 for encoder"
        );
        let Err(encoder_err) = encoder_result else {
            panic!("Expected error but got Ok")
        };

        let encoder_err_msg = format!("{}", encoder_err);
        assert!(
            encoder_err_msg.contains("not enabled by the selected file format"),
            "unexpected encoder error: {encoder_err_msg}"
        );
        assert!(
            encoder_err_msg.contains("Map data type"),
            "Encoder error message should mention Map data type, got: {}",
            encoder_err_msg
        );
    }
}
