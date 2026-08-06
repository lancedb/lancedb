// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Comprehensive fuzz testing for Lance 2.1 encoding coverage
//!
//! This module implements property-based testing using proptest to ensure
//! correct behavior across 16 different encoding permutations as specified
//! in issue #3347.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Int32Builder, ListBuilder};
use arrow_array::*;
use arrow_schema::{DataType, Field};
use proptest::prelude::*;

use crate::testing::{TestCases, check_round_trip_encoding_of_data};
use lance_core::Result;
use lance_datagen::{ArrayGenerator, ByteCount, Dimension, RowCount, Seed, array, gen_batch};

/// Test configuration representing one of the 16 permutations
#[derive(Debug, Clone)]
struct EncodingTestConfig {
    encoding_type: EncodingType,
    data_structure: DataStructure,
    data_width: DataWidth,
    nullable: bool,
}

#[derive(Debug, Clone, PartialEq)]
enum EncodingType {
    Miniblock,
    FullZip,
}

#[derive(Debug, Clone, PartialEq)]
enum DataStructure {
    Primitive,
    List,
    FixedSizeList(i32),
}

#[derive(Debug, Clone)]
enum DataWidth {
    Fixed(FixedWidthType),
    Variable(VariableWidthType),
}

#[derive(Debug, Clone)]
enum FixedWidthType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    FixedBinary(i32),
}

#[derive(Debug, Clone)]
enum VariableWidthType {
    String,
    Binary,
    LargeString,
    LargeBinary,
}

impl EncodingTestConfig {
    /// Create field based on configuration
    fn to_field(&self, name: &str) -> Field {
        let data_type = self.to_data_type();
        Field::new(name, data_type, self.nullable)
    }

    /// Get the Arrow data type for this configuration
    fn to_data_type(&self) -> DataType {
        let base_type = match &self.data_width {
            DataWidth::Fixed(fixed) => match fixed {
                FixedWidthType::Int8 => DataType::Int8,
                FixedWidthType::Int16 => DataType::Int16,
                FixedWidthType::Int32 => DataType::Int32,
                FixedWidthType::Int64 => DataType::Int64,
                FixedWidthType::UInt8 => DataType::UInt8,
                FixedWidthType::UInt16 => DataType::UInt16,
                FixedWidthType::UInt32 => DataType::UInt32,
                FixedWidthType::UInt64 => DataType::UInt64,
                FixedWidthType::Float16 => DataType::Float16,
                FixedWidthType::Float32 => DataType::Float32,
                FixedWidthType::Float64 => DataType::Float64,
                FixedWidthType::FixedBinary(size) => DataType::FixedSizeBinary(*size),
            },
            DataWidth::Variable(var) => match var {
                VariableWidthType::String => DataType::Utf8,
                VariableWidthType::Binary => DataType::Binary,
                VariableWidthType::LargeString => DataType::LargeUtf8,
                VariableWidthType::LargeBinary => DataType::LargeBinary,
            },
        };

        match &self.data_structure {
            DataStructure::Primitive => base_type,
            DataStructure::List => DataType::List(Arc::new(Field::new("item", base_type, true))),
            DataStructure::FixedSizeList(size) => {
                DataType::FixedSizeList(Arc::new(Field::new("item", base_type, true)), *size)
            }
        }
    }
}

/// Generate all valid test configurations (excluding FSL+List combinations)
fn encoding_config_strategy() -> impl Strategy<Value = EncodingTestConfig> {
    (
        prop_oneof![Just(EncodingType::Miniblock), Just(EncodingType::FullZip)],
        prop_oneof![
            Just(DataStructure::Primitive),
            Just(DataStructure::List),
            (1..=100i32).prop_map(DataStructure::FixedSizeList)
        ],
        prop_oneof![
            fixed_width_strategy().prop_map(DataWidth::Fixed),
            variable_width_strategy().prop_map(DataWidth::Variable)
        ],
        any::<bool>(), // nullable
    )
        .prop_filter(
            "Skip unsupported combinations",
            |(_, structure, width, _)| {
                // FSL with Variable width types is not supported
                // This combination causes compute_stat to be called twice
                !matches!(
                    (structure, width),
                    (DataStructure::FixedSizeList(_), DataWidth::Variable(_))
                )
            },
        )
        .prop_map(
            |(encoding, structure, width, nullable)| EncodingTestConfig {
                encoding_type: encoding,
                data_structure: structure,
                data_width: width,
                nullable,
            },
        )
}

fn fixed_width_strategy() -> impl Strategy<Value = FixedWidthType> {
    prop_oneof![
        Just(FixedWidthType::Int8),
        Just(FixedWidthType::Int16),
        Just(FixedWidthType::Int32),
        Just(FixedWidthType::Int64),
        Just(FixedWidthType::UInt8),
        Just(FixedWidthType::UInt16),
        Just(FixedWidthType::UInt32),
        Just(FixedWidthType::UInt64),
        Just(FixedWidthType::Float16),
        Just(FixedWidthType::Float32),
        Just(FixedWidthType::Float64),
        (1..=128i32).prop_map(FixedWidthType::FixedBinary)
    ]
}

fn variable_width_strategy() -> impl Strategy<Value = VariableWidthType> {
    prop_oneof![
        Just(VariableWidthType::String),
        Just(VariableWidthType::Binary),
        Just(VariableWidthType::LargeString),
        Just(VariableWidthType::LargeBinary)
    ]
}

/// Generate test data based on configuration
fn generate_test_data_for_config(
    config: &EncodingTestConfig,
    num_rows: usize,
    seed: u64,
) -> Result<Arc<dyn Array>> {
    let mut batch_gen = gen_batch().with_seed(Seed::from(seed));

    // Generate base values
    let generator: Box<dyn ArrayGenerator> = match &config.data_width {
        DataWidth::Fixed(fixed) => match fixed {
            FixedWidthType::Int8 => array::rand_type(&DataType::Int8),
            FixedWidthType::Int16 => array::rand_type(&DataType::Int16),
            FixedWidthType::Int32 => array::rand_type(&DataType::Int32),
            FixedWidthType::Int64 => array::rand_type(&DataType::Int64),
            FixedWidthType::UInt8 => array::rand_type(&DataType::UInt8),
            FixedWidthType::UInt16 => array::rand_type(&DataType::UInt16),
            FixedWidthType::UInt32 => array::rand_type(&DataType::UInt32),
            FixedWidthType::UInt64 => array::rand_type(&DataType::UInt64),
            FixedWidthType::Float16 => array::rand_type(&DataType::Float16),
            FixedWidthType::Float32 => array::rand_type(&DataType::Float32),
            FixedWidthType::Float64 => array::rand_type(&DataType::Float64),
            FixedWidthType::FixedBinary(size) => {
                array::rand_type(&DataType::FixedSizeBinary(*size))
            }
        },
        DataWidth::Variable(var) => {
            // Control string/binary size based on encoding type
            let max_len = if config.encoding_type == EncodingType::Miniblock {
                10 // Small strings for miniblock
            } else {
                1000 // Large strings for full-zip
            };

            match var {
                VariableWidthType::String => {
                    array::rand_utf8(ByteCount::from(max_len as u64), false)
                }
                VariableWidthType::Binary => {
                    array::rand_varbin(ByteCount::from(1), ByteCount::from(max_len as u64))
                }
                VariableWidthType::LargeString => {
                    array::rand_utf8(ByteCount::from(max_len as u64), true)
                }
                VariableWidthType::LargeBinary => {
                    // For large binary, just use regular binary with larger size
                    array::rand_varbin(ByteCount::from(1), ByteCount::from(max_len as u64))
                }
            }
        }
    };

    // Wrap in list structure if needed
    let final_generator: Box<dyn ArrayGenerator> = match &config.data_structure {
        DataStructure::Primitive => generator,
        DataStructure::List => {
            // Use rand_list_any for wrapping any generator
            array::rand_list_any(generator, false)
        }
        DataStructure::FixedSizeList(size) => {
            // Use cycle_vec for fixed size lists
            array::cycle_vec(generator, Dimension::from(*size as u32))
        }
    };

    // Add nulls if configured
    if config.nullable {
        batch_gen.with_random_nulls(0.1); // 10% null rate
    }

    let batch = batch_gen
        .anon_col(final_generator)
        .into_batch_rows(RowCount::from(num_rows as u64))?;

    Ok(batch.column(0).clone())
}

// Main property test for encoding round-trip
proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn test_encoding_round_trip(
        config in encoding_config_strategy(),
        num_rows in 100..=5000usize,
        seed in any::<u64>()
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            // Generate test data
            let test_data = generate_test_data_for_config(&config, num_rows, seed)
                .expect("Failed to generate test data");

            // Set up test cases
            let _field = config.to_field("test");

            let mut metadata = HashMap::new();
            // Force specific encoding through metadata hints if needed
            if config.encoding_type == EncodingType::Miniblock {
                metadata.insert("encoding_hint".to_string(), "miniblock".to_string());
            }

            let test_cases = TestCases::default()
                .with_structural_encodings()
                .with_batch_size(100)
                .with_range(0..num_rows.min(500) as u64)
                .with_indices(vec![0, num_rows as u64 / 2, (num_rows - 1) as u64]);

            // Execute round-trip test
            check_round_trip_encoding_of_data(
                vec![test_data],
                &test_cases,
                metadata
            ).await;
        });
    }
}

#[tokio::test]
async fn test_edge_cases_single_value() {
    // Test single value arrays
    let single_int32 = Arc::new(Int32Array::from(vec![42])) as Arc<dyn Array>;
    let single_string = Arc::new(StringArray::from(vec!["test"])) as Arc<dyn Array>;

    let test_cases = TestCases::default().with_structural_encodings();

    check_round_trip_encoding_of_data(vec![single_int32], &test_cases, HashMap::new()).await;

    check_round_trip_encoding_of_data(vec![single_string], &test_cases, HashMap::new()).await;
}

#[tokio::test]
async fn test_edge_cases_all_nulls() {
    // Test arrays with all null values
    let all_nulls_int32 =
        Arc::new(Int32Array::from(vec![None, None, None] as Vec<Option<i32>>)) as Arc<dyn Array>;
    let all_nulls_string = Arc::new(StringArray::from(
        vec![None, None, None] as Vec<Option<&str>>
    )) as Arc<dyn Array>;

    let test_cases = TestCases::default().with_structural_encodings();

    check_round_trip_encoding_of_data(vec![all_nulls_int32], &test_cases, HashMap::new()).await;

    check_round_trip_encoding_of_data(vec![all_nulls_string], &test_cases, HashMap::new()).await;
}

// Test list with repetition and definition levels
proptest! {
    #[test]
    fn test_list_repdef_handling(
        list_sizes in prop::collection::vec(1..=20usize, 10..=100),
        _seed in any::<u64>()
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            // Create a list array with varying sizes
            let mut list_builder = ListBuilder::new(Int32Builder::new());

            for size in &list_sizes {
                for i in 0..*size {
                    list_builder.values().append_value(i as i32);
                }
                list_builder.append(true);
            }

            let list_array = Arc::new(list_builder.finish()) as Arc<dyn Array>;

            let test_cases = TestCases::default()
                .with_structural_encodings()
                .with_range(0..list_sizes.len().min(50) as u64);

            check_round_trip_encoding_of_data(
                vec![list_array],
                &test_cases,
                HashMap::new()
            ).await;
        });
    }
}

// Test fixed size list encoding
proptest! {
    #[test]
    fn test_fixed_size_list_encoding(
        list_size in 1..=100i32,
        num_rows in 10..=1000usize,
        seed in any::<u64>()
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let config = EncodingTestConfig {
                encoding_type: EncodingType::Miniblock,
                data_structure: DataStructure::FixedSizeList(list_size),
                data_width: DataWidth::Fixed(FixedWidthType::Int32),
                nullable: false,
            };

            let test_data = generate_test_data_for_config(&config, num_rows, seed)
                .expect("Failed to generate test data");

            let test_cases = TestCases::default()
                .with_structural_encodings();

            check_round_trip_encoding_of_data(
                vec![test_data],
                &test_cases,
                HashMap::new()
            ).await;
        });
    }
}

#[tokio::test]
async fn test_list_dict_empty_batch() {
    use arrow_array::builder::BinaryBuilder;
    use arrow_array::builder::ListBuilder;

    // Create a list with some values followed by empty/null lists
    let mut list_builder = ListBuilder::new(BinaryBuilder::new());

    // First 50 lists have values with LOW CARDINALITY to trigger dictionary encoding
    // Only 5 unique values repeated many times (150 total values, 5 unique)
    let values = [b"aaaaa", b"bbbbb", b"ccccc", b"ddddd", b"eeeee"];
    for i in 0..50 {
        // Each list has 3 values, cycling through the 5 unique values
        list_builder.append_value([
            Some(values[i % 5]),
            Some(values[(i + 1) % 5]),
            Some(values[(i + 2) % 5]),
        ]);
    }

    // Next 50 lists are empty or null (no values)
    for i in 0..50 {
        if i % 2 == 0 {
            list_builder.append_value(Vec::<Option<&[u8]>>::new()); // empty list
        } else {
            list_builder.append_null(); // null list
        }
    }

    let list_array = Arc::new(list_builder.finish());

    let test_cases = TestCases::default()
        .with_structural_encodings()
        // Read only the empty/null lists (rows 50-99)
        // This batch will have 0 underlying values
        .with_range(50..100);

    check_round_trip_encoding_of_data(vec![list_array], &test_cases, HashMap::new()).await;
}

// Test all valid combinations systematically (14 combinations)
// Excludes FSL with Variable width types which are not supported
#[tokio::test]
async fn test_all_valid_combinations() {
    let combinations = vec![
        // Basic primitive types (4 combinations)
        (
            EncodingType::Miniblock,
            DataStructure::Primitive,
            DataWidth::Fixed(FixedWidthType::Int32),
            false,
        ),
        (
            EncodingType::Miniblock,
            DataStructure::Primitive,
            DataWidth::Variable(VariableWidthType::String),
            false,
        ),
        (
            EncodingType::FullZip,
            DataStructure::Primitive,
            DataWidth::Fixed(FixedWidthType::Float64),
            false,
        ),
        (
            EncodingType::FullZip,
            DataStructure::Primitive,
            DataWidth::Variable(VariableWidthType::Binary),
            false,
        ),
        // List types (4 combinations)
        (
            EncodingType::Miniblock,
            DataStructure::List,
            DataWidth::Fixed(FixedWidthType::Int32),
            false,
        ),
        (
            EncodingType::Miniblock,
            DataStructure::List,
            DataWidth::Variable(VariableWidthType::String),
            false,
        ),
        (
            EncodingType::FullZip,
            DataStructure::List,
            DataWidth::Fixed(FixedWidthType::Float64),
            false,
        ),
        (
            EncodingType::FullZip,
            DataStructure::List,
            DataWidth::Variable(VariableWidthType::Binary),
            false,
        ),
        // Fixed size list types (2 combinations - only with Fixed width types)
        // Note: FSL with Variable width types is not supported
        (
            EncodingType::Miniblock,
            DataStructure::FixedSizeList(10),
            DataWidth::Fixed(FixedWidthType::Int32),
            false,
        ),
        (
            EncodingType::FullZip,
            DataStructure::FixedSizeList(10),
            DataWidth::Fixed(FixedWidthType::Float64),
            false,
        ),
        // Nullable variants (4 combinations)
        (
            EncodingType::Miniblock,
            DataStructure::Primitive,
            DataWidth::Fixed(FixedWidthType::Int32),
            true,
        ),
        (
            EncodingType::Miniblock,
            DataStructure::Primitive,
            DataWidth::Variable(VariableWidthType::String),
            true,
        ),
        (
            EncodingType::FullZip,
            DataStructure::Primitive,
            DataWidth::Fixed(FixedWidthType::Float64),
            true,
        ),
        (
            EncodingType::FullZip,
            DataStructure::Primitive,
            DataWidth::Variable(VariableWidthType::Binary),
            true,
        ),
    ];

    for (encoding, structure, width, nullable) in combinations {
        let config = EncodingTestConfig {
            encoding_type: encoding,
            data_structure: structure,
            data_width: width,
            nullable,
        };

        let test_data =
            generate_test_data_for_config(&config, 100, 42).expect("Failed to generate test data");

        let test_cases = TestCases::default().with_structural_encodings();

        check_round_trip_encoding_of_data(vec![test_data], &test_cases, HashMap::new()).await;
    }
}
