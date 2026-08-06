// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
use std::{collections::HashMap, hint::black_box, sync::Arc};

use arrow_array::{RecordBatch, UInt32Array};
#[cfg(feature = "bitpacking")]
use arrow_buffer::ArrowNativeType;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use arrow_select::take::take;
#[cfg(feature = "bitpacking")]
use bytemuck::Pod;
use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
#[cfg(feature = "bitpacking")]
use lance_bitpacking::BitPacking;
use lance_core::cache::LanceCache;
use lance_datagen::ArrayGeneratorExt;
#[cfg(feature = "bitpacking")]
use lance_encoding::buffer::LanceBuffer;
#[cfg(feature = "bitpacking")]
use lance_encoding::compression::BlockDecompressor;
#[cfg(feature = "bitpacking")]
use lance_encoding::data::{BlockInfo, DataBlock, FixedWidthDataBlock};
#[cfg(feature = "bitpacking")]
use lance_encoding::encodings::physical::bitpacking::{ELEMS_PER_CHUNK, InlineBitpacking};
use lance_encoding::{
    decoder::{
        DecodeBatchScheduler, DecoderConfig, DecoderPlugins, EncodedBatchLayout, FilterExpression,
        create_decode_stream,
    },
    encoder::{EncodingOptions, encode_batch},
};
use tokio::sync::mpsc::unbounded_channel;

use rand::Rng;

pub mod common;
use common::{BenchEncoding, encoding_strategy};

const PRIMITIVE_TYPES: &[DataType] = &[
    DataType::Date32,
    DataType::Date64,
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Float16,
    DataType::Float32,
    DataType::Float64,
    DataType::Decimal128(10, 10),
    DataType::Decimal256(10, 10),
    DataType::Timestamp(TimeUnit::Nanosecond, None),
    DataType::Time32(TimeUnit::Second),
    DataType::Time64(TimeUnit::Nanosecond),
    DataType::Duration(TimeUnit::Second),
    // The Interval type is supported by the reader but the writer works with Lance schema
    // at the moment and Lance schema can't parse interval
    // DataType::Interval(IntervalUnit::DayTime),
];

// Some types are supported by the encoder/decoder but Lance
// schema doesn't yet parse them in the context of a fixed size list.
const PRIMITIVE_TYPES_FOR_FSL: &[DataType] = &[DataType::Int8, DataType::Float32];

fn encoded_batch_layout(encoding: BenchEncoding) -> EncodedBatchLayout {
    match encoding {
        BenchEncoding::Array => EncodedBatchLayout::Array,
        BenchEncoding::StructuralU16 | BenchEncoding::StructuralU32 => {
            EncodedBatchLayout::Structural
        }
    }
}

fn bench_decode(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("decode_primitive");
    const NUM_BYTES: u64 = 1024 * 1024 * 128;
    group.throughput(criterion::Throughput::Bytes(NUM_BYTES));
    for data_type in PRIMITIVE_TYPES {
        let func_name = format!("{:?}", data_type).to_lowercase();
        let num_rows = NUM_BYTES / data_type.primitive_width().unwrap() as u64;
        group.bench_function(func_name, |b| {
            let data = lance_datagen::gen_batch()
                .anon_col(lance_datagen::array::rand_type(data_type))
                .into_batch_rows(lance_datagen::RowCount::from(num_rows))
                .unwrap();
            let lance_schema =
                Arc::new(lance_core::datatypes::Schema::try_from(data.schema().as_ref()).unwrap());
            let encoding_strategy = encoding_strategy(BenchEncoding::StructuralU16);
            let encoded = rt
                .block_on(encode_batch(
                    &data,
                    lance_schema,
                    encoding_strategy.as_ref(),
                    &EncodingOptions::default(),
                ))
                .unwrap();

            b.iter(|| {
                let batch = rt
                    .block_on(lance_encoding::decoder::decode_batch(
                        &encoded,
                        &FilterExpression::no_filter(),
                        Arc::<DecoderPlugins>::default(),
                        false,
                        EncodedBatchLayout::Structural,
                        Some(Arc::new(LanceCache::no_cache())),
                    ))
                    .unwrap();
                assert_eq!(data.num_rows(), batch.num_rows());
            })
        });
    }
}

fn bench_decode_fsl(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("decode_fsl");
    const NUM_BYTES: u64 = 1024 * 1024 * 128;
    for encoding in [
        BenchEncoding::Array,
        BenchEncoding::StructuralU16,
        BenchEncoding::StructuralU32,
    ] {
        for data_type in PRIMITIVE_TYPES_FOR_FSL {
            for dimension in [4, 16, 32, 64, 128] {
                let nullable_choices: &[bool] = if encoding == BenchEncoding::Array {
                    &[false]
                } else {
                    &[false, true]
                };
                for nullable in nullable_choices {
                    let func_name = format!(
                        "{:?}_{}_v{}_null{}",
                        data_type, dimension, encoding, nullable
                    )
                    .to_lowercase();
                    group.throughput(criterion::Throughput::Bytes(NUM_BYTES));
                    group.bench_function(func_name, |b| {
                        let num_rows =
                            NUM_BYTES / (dimension * data_type.primitive_width().unwrap() as u64);
                        let mut arraygen =
                            lance_datagen::array::rand_type(&DataType::FixedSizeList(
                                Arc::new(Field::new("item", data_type.clone(), true)),
                                dimension as i32,
                            ));
                        if *nullable {
                            arraygen = arraygen.with_random_nulls(0.5);
                        }
                        let data = lance_datagen::gen_batch()
                            .anon_col(arraygen)
                            .into_batch_rows(lance_datagen::RowCount::from(num_rows))
                            .unwrap();
                        let lance_schema = Arc::new(
                            lance_core::datatypes::Schema::try_from(data.schema().as_ref())
                                .unwrap(),
                        );
                        let encoding_strategy = encoding_strategy(encoding);
                        let encoded = rt
                            .block_on(encode_batch(
                                &data,
                                lance_schema,
                                encoding_strategy.as_ref(),
                                &EncodingOptions::default(),
                            ))
                            .unwrap();
                        b.iter(|| {
                            let batch = rt
                                .block_on(lance_encoding::decoder::decode_batch(
                                    &encoded,
                                    &FilterExpression::no_filter(),
                                    Arc::<DecoderPlugins>::default(),
                                    false,
                                    encoded_batch_layout(encoding),
                                    Some(Arc::new(LanceCache::no_cache())),
                                ))
                                .unwrap();
                            assert_eq!(data.num_rows(), batch.num_rows());
                        })
                    });
                }
            }
        }
    }
}

fn bench_decode_str_with_dict_encoding(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("decode_primitive");
    const NUM_ROWS: u64 = 100000;

    let data_type = DataType::Utf8;
    // generate string column with 20 rows
    let string_data = lance_datagen::gen_batch()
        .anon_col(lance_datagen::array::rand_type(&DataType::Utf8))
        .into_batch_rows(lance_datagen::RowCount::from(20))
        .unwrap();

    group.throughput(criterion::Throughput::Bytes(
        NUM_ROWS * std::mem::size_of::<u32>() as u64 + string_data.get_array_memory_size() as u64,
    ));

    let func_name = format!("{:?}", data_type).to_lowercase();
    group.bench_function(func_name, |b| {
        let string_array = string_data.column(0);

        // generate random int column with 100000 rows
        let mut rng = rand::rng();
        let integer_arr: Vec<u32> = (0..100_000).map(|_| rng.random_range(0..20)).collect();
        let integer_array = UInt32Array::from(integer_arr);

        let mapped_strings = take(string_array, &integer_array, None).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "string",
            DataType::Utf8,
            false,
        )]));

        let data = RecordBatch::try_new(schema, vec![Arc::new(mapped_strings)]).unwrap();

        let lance_schema =
            Arc::new(lance_core::datatypes::Schema::try_from(data.schema().as_ref()).unwrap());
        let encoding_strategy = encoding_strategy(BenchEncoding::StructuralU16);
        let encoded = rt
            .block_on(encode_batch(
                &data,
                lance_schema,
                encoding_strategy.as_ref(),
                &EncodingOptions::default(),
            ))
            .unwrap();
        b.iter(|| {
            let batch = rt
                .block_on(lance_encoding::decoder::decode_batch(
                    &encoded,
                    &FilterExpression::no_filter(),
                    Arc::<DecoderPlugins>::default(),
                    false,
                    EncodedBatchLayout::Structural,
                    Some(Arc::new(LanceCache::no_cache())),
                ))
                .unwrap();
            assert_eq!(data.num_rows(), batch.num_rows());
        })
    });
}

fn bench_decode_packed_struct(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("decode_primitive");

    const NUM_ROWS: u64 = 10000;
    let size_bytes =
        ((6 * std::mem::size_of::<i32>() as u64) + std::mem::size_of::<f32>() as u64) * NUM_ROWS;
    group.throughput(criterion::Throughput::Bytes(size_bytes));

    let func_name = "struct";
    group.bench_function(func_name, |b| {
        let fields = vec![
            Arc::new(Field::new("int_field", DataType::Int32, false)),
            Arc::new(Field::new("float_field", DataType::Float32, false)),
            Arc::new(Field::new(
                "fsl_field",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 5),
                false,
            )),
        ]
        .into();

        // generate struct column with 1M rows
        let data = lance_datagen::gen_batch()
            .anon_col(lance_datagen::array::rand_type(&DataType::Struct(fields)))
            .into_batch_rows(lance_datagen::RowCount::from(NUM_ROWS))
            .unwrap();

        let schema = data.schema();
        let new_fields: Vec<Arc<Field>> = schema
            .fields()
            .iter()
            .map(|field| {
                if matches!(field.data_type(), &DataType::Struct(_)) {
                    let mut metadata = HashMap::new();
                    metadata.insert("packed".to_string(), "true".to_string());
                    let field =
                        Field::new(field.name(), field.data_type().clone(), field.is_nullable());
                    Arc::new(field.with_metadata(metadata))
                } else {
                    field.clone()
                }
            })
            .collect();

        let new_schema = Schema::new(new_fields);
        let data =
            RecordBatch::try_new(Arc::new(new_schema.clone()), data.columns().to_vec()).unwrap();

        let lance_schema = Arc::new(lance_core::datatypes::Schema::try_from(&new_schema).unwrap());
        let encoding_strategy = encoding_strategy(BenchEncoding::StructuralU32);
        let encoded = rt
            .block_on(encode_batch(
                &data,
                lance_schema,
                encoding_strategy.as_ref(),
                &EncodingOptions::default(),
            ))
            .unwrap();

        b.iter(|| {
            let batch = rt
                .block_on(lance_encoding::decoder::decode_batch(
                    &encoded,
                    &FilterExpression::no_filter(),
                    Arc::<DecoderPlugins>::default(),
                    false,
                    EncodedBatchLayout::Structural,
                    Some(Arc::new(LanceCache::no_cache())),
                ))
                .unwrap();
            assert_eq!(data.num_rows(), batch.num_rows());
        })
    });
}

#[cfg(target_os = "linux")]
fn bench_decode_str_with_fixed_size_binary_encoding(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("decode_primitive");

    const NUM_ROWS: u64 = 10000;
    // Randomly generated strings are always 12 characters (at the moment)
    // Plus we need 4 bytes for the offset
    const NUM_BYTES: u64 = NUM_ROWS * 16;
    group.throughput(criterion::Throughput::Bytes(NUM_BYTES));

    let func_name = "fixed-utf8".to_string();
    group.bench_function(func_name, |b| {
        // generate string column with 10k rows
        // Currently the generator generates fixed size strings by default
        // This function will need to be updated once that changes.
        let string_data = lance_datagen::gen_batch()
            .anon_col(lance_datagen::array::rand_type(&DataType::Utf8))
            .into_batch_rows(lance_datagen::RowCount::from(10000))
            .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "string",
            DataType::Utf8,
            false,
        )]));

        let data = RecordBatch::try_new(schema, string_data.columns().to_vec()).unwrap();

        let lance_schema =
            Arc::new(lance_core::datatypes::Schema::try_from(data.schema().as_ref()).unwrap());
        let encoding_strategy = encoding_strategy(BenchEncoding::StructuralU16);
        let encoded = rt
            .block_on(encode_batch(
                &data,
                lance_schema,
                encoding_strategy.as_ref(),
                &EncodingOptions::default(),
            ))
            .unwrap();
        b.iter(|| {
            let batch = rt
                .block_on(lance_encoding::decoder::decode_batch(
                    &encoded,
                    &FilterExpression::no_filter(),
                    Arc::<DecoderPlugins>::default(),
                    false,
                    EncodedBatchLayout::Structural,
                    Some(Arc::new(LanceCache::no_cache())),
                ))
                .unwrap();
            assert_eq!(data.num_rows(), batch.num_rows());
        })
    });
}

fn bench_decode_compressed(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("decode_compressed");

    const NUM_ROWS: usize = 5_000_000;
    const NUM_COLUMNS: usize = 10;

    // Generate compressible string data - high cardinality but compressible
    // (unique values to avoid dictionary encoding, repeated prefix for compression)
    let array: Arc<dyn arrow_array::Array> = Arc::new(arrow_array::StringArray::from_iter_values(
        (0..NUM_ROWS).map(|i| format!("prefix_that_compresses_well_{}", i)),
    ));

    for compression in ["zstd", "lz4"] {
        let mut metadata = HashMap::new();
        metadata.insert(
            "lance-encoding:compression".to_string(),
            compression.to_string(),
        );
        // Disable dictionary encoding to ensure we hit the compression path
        metadata.insert(
            "lance-encoding:dict-divisor".to_string(),
            "100000".to_string(),
        );
        // Force miniblock encoding (the path that benefits from compressor caching)
        metadata.insert(
            "lance-encoding:structural-encoding".to_string(),
            "miniblock".to_string(),
        );
        let fields: Vec<Field> = (0..NUM_COLUMNS)
            .map(|i| {
                Field::new(format!("s{}", i), DataType::Utf8, false).with_metadata(metadata.clone())
            })
            .collect();
        let columns: Vec<Arc<dyn arrow_array::Array>> =
            (0..NUM_COLUMNS).map(|_| array.clone()).collect();
        let schema = Arc::new(Schema::new(fields));
        let data = RecordBatch::try_new(schema.clone(), columns).unwrap();

        let lance_schema =
            Arc::new(lance_core::datatypes::Schema::try_from(schema.as_ref()).unwrap());
        // V2_2+ required for general compression
        let encoding_strategy = encoding_strategy(BenchEncoding::StructuralU32);

        // Encode once during setup
        let encoded = rt
            .block_on(encode_batch(
                &data,
                lance_schema,
                encoding_strategy.as_ref(),
                &EncodingOptions::default(),
            ))
            .unwrap();

        group.throughput(criterion::Throughput::Elements(
            (NUM_ROWS * NUM_COLUMNS) as u64,
        ));
        group.bench_function(
            format!("{}_strings_{}cols", compression, NUM_COLUMNS),
            |b| {
                b.iter(|| {
                    let batch = rt
                        .block_on(lance_encoding::decoder::decode_batch(
                            &encoded,
                            &FilterExpression::no_filter(),
                            Arc::<DecoderPlugins>::default(),
                            false,
                            EncodedBatchLayout::Structural,
                            Some(Arc::new(LanceCache::no_cache())),
                        ))
                        .unwrap();
                    assert_eq!(data.num_rows(), batch.num_rows());
                })
            },
        );
    }
}

/// Benchmark parallel decoding with multiple concurrent batch decode tasks.
/// This creates contention on the shared decompressor mutex when multiple
/// batches from the same page are decoded in parallel.
fn bench_decode_compressed_parallel(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("decode_compressed_parallel");

    const NUM_ROWS: u64 = 1_000_000;
    const NUM_COLUMNS: usize = 10;
    // Small batch size to create many batches that will contend on the same decompressor
    const BATCH_SIZE: u32 = 100_000;

    let array: Arc<dyn arrow_array::Array> = Arc::new(arrow_array::StringArray::from_iter_values(
        (0..NUM_ROWS as usize).map(|i| format!("prefix_that_compresses_well_{}", i)),
    ));

    for compression in ["zstd", "lz4"] {
        let mut metadata = HashMap::new();
        metadata.insert(
            "lance-encoding:compression".to_string(),
            compression.to_string(),
        );
        metadata.insert(
            "lance-encoding:dict-divisor".to_string(),
            "100000".to_string(),
        );
        metadata.insert(
            "lance-encoding:structural-encoding".to_string(),
            "miniblock".to_string(),
        );
        let fields: Vec<Field> = (0..NUM_COLUMNS)
            .map(|i| {
                Field::new(format!("s{}", i), DataType::Utf8, false).with_metadata(metadata.clone())
            })
            .collect();
        let columns: Vec<Arc<dyn arrow_array::Array>> =
            (0..NUM_COLUMNS).map(|_| array.clone()).collect();
        let schema = Arc::new(Schema::new(fields));
        let data = RecordBatch::try_new(schema.clone(), columns).unwrap();

        let lance_schema =
            Arc::new(lance_core::datatypes::Schema::try_from(schema.as_ref()).unwrap());
        let encoding_strategy = encoding_strategy(BenchEncoding::StructuralU32);

        let encoded = rt
            .block_on(encode_batch(
                &data,
                lance_schema,
                encoding_strategy.as_ref(),
                &EncodingOptions::default(),
            ))
            .unwrap();

        let encoded = Arc::new(encoded);

        // Test with different parallelism levels to see impact of mutex contention
        // parallelism=1 is sequential (no contention), higher values cause contention
        for parallelism in [1, 8] {
            group.throughput(criterion::Throughput::Elements(
                NUM_ROWS * NUM_COLUMNS as u64,
            ));
            group.bench_function(
                format!(
                    "{}_{}cols_parallel_{}",
                    compression, NUM_COLUMNS, parallelism
                ),
                |b| {
                    b.iter(|| {
                        rt.block_on(async {
                            let io_scheduler = Arc::new(lance_encoding::BufferScheduler::new(
                                encoded.data.clone(),
                            ))
                                as Arc<dyn lance_encoding::EncodingsIo>;
                            let cache = Arc::new(LanceCache::no_cache());
                            let filter = FilterExpression::no_filter();

                            let mut decode_scheduler = DecodeBatchScheduler::try_new(
                                encoded.schema.as_ref(),
                                &encoded.top_level_columns,
                                &encoded.page_table,
                                &vec![],
                                encoded.num_rows,
                                Arc::<DecoderPlugins>::default(),
                                io_scheduler.clone(),
                                cache,
                                &filter,
                                &DecoderConfig::default(),
                            )
                            .await
                            .unwrap();

                            let (tx, rx) = unbounded_channel();
                            decode_scheduler.schedule_range(
                                0..encoded.num_rows,
                                &filter,
                                tx,
                                io_scheduler,
                            );

                            let decode_stream = create_decode_stream(
                                &encoded.schema,
                                encoded.num_rows,
                                BATCH_SIZE,
                                true, // is_structural for V2_2
                                false,
                                false,
                                rx,
                                None,
                            )
                            .unwrap();

                            // Buffer multiple batch decodes in parallel - this causes contention
                            let batches: Vec<_> = decode_stream
                                .map(|task| task.task)
                                .buffered(parallelism)
                                .collect()
                                .await;

                            let total_rows: usize =
                                batches.iter().map(|b| b.as_ref().unwrap().num_rows()).sum();
                            assert_eq!(total_rows, NUM_ROWS as usize);
                        })
                    })
                },
            );
        }
    }
}

#[cfg(feature = "bitpacking")]
fn make_inline_bitpacking_chunk<T>(bit_width: usize) -> LanceBuffer
where
    T: ArrowNativeType + BitPacking + Pod,
{
    let value_range = 1_usize << bit_width;
    let values: Vec<T> = (0..ELEMS_PER_CHUNK as usize)
        .map(|i| T::from_usize((i * 31 + 7) % value_range).unwrap())
        .collect();
    let packed_words = ELEMS_PER_CHUNK as usize * bit_width / (std::mem::size_of::<T>() * 8);

    let mut chunk = Vec::with_capacity(1 + packed_words);
    chunk.push(T::from_usize(bit_width).unwrap());
    let payload_start = chunk.len();
    chunk.resize(payload_start + packed_words, T::from_usize(0).unwrap());
    unsafe {
        BitPacking::unchecked_pack(bit_width, &values, &mut chunk[payload_start..]);
    }

    LanceBuffer::reinterpret_vec(chunk)
}

#[cfg(feature = "bitpacking")]
fn read_little_endian_header<T>(bytes: &[u8]) -> usize {
    bytes[..std::mem::size_of::<T>()]
        .iter()
        .enumerate()
        .fold(0_u64, |value, (idx, byte)| {
            value | ((*byte as u64) << (idx * 8))
        }) as usize
}

#[cfg(feature = "bitpacking")]
fn legacy_copy_unchunk<T>(data: LanceBuffer, num_values: u64) -> DataBlock
where
    T: ArrowNativeType + BitPacking + Pod,
{
    assert!(data.len() >= std::mem::size_of::<T>());
    assert!(num_values <= ELEMS_PER_CHUNK);

    let chunk_in_u8 = data.to_vec();
    let bit_width_value = read_little_endian_header::<T>(&chunk_in_u8);
    let chunk = bytemuck::cast_slice(&chunk_in_u8[std::mem::size_of::<T>()..]);
    assert!(std::mem::size_of_val(chunk) == bit_width_value * ELEMS_PER_CHUNK as usize / 8);

    let mut decompressed = vec![T::from_usize(0).unwrap(); ELEMS_PER_CHUNK as usize];
    unsafe {
        BitPacking::unchecked_unpack(bit_width_value, chunk, &mut decompressed);
    }

    decompressed.truncate(num_values as usize);
    DataBlock::FixedWidth(FixedWidthDataBlock {
        data: LanceBuffer::reinterpret_vec(decompressed),
        bits_per_value: (std::mem::size_of::<T>() * 8) as u64,
        num_values,
        block_info: BlockInfo::new(),
    })
}

#[cfg(feature = "bitpacking")]
fn typed_view_unchunk(buffer: LanceBuffer, uncompressed_bits: u64, num_values: u64) -> DataBlock {
    InlineBitpacking::new(uncompressed_bits)
        .decompress(buffer, num_values)
        .unwrap()
}

#[cfg(feature = "bitpacking")]
fn assert_same_fixed_width_payloads(legacy: &DataBlock, typed_view: &DataBlock) {
    let legacy = legacy.as_fixed_width_ref().unwrap();
    let typed_view = typed_view.as_fixed_width_ref().unwrap();

    assert_eq!(legacy.num_values, typed_view.num_values);
    assert_eq!(legacy.bits_per_value, typed_view.bits_per_value);
    assert_eq!(legacy.data.as_ref(), typed_view.data.as_ref());
}

#[cfg(feature = "bitpacking")]
fn bench_inline_bitpacking_case<T>(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    name: &str,
    bit_width: usize,
) where
    T: ArrowNativeType + BitPacking + Pod,
{
    let buffer = make_inline_bitpacking_chunk::<T>(bit_width);
    let compressed_bytes = buffer.len() as u64;
    let uncompressed_bits = (std::mem::size_of::<T>() * 8) as u64;
    group.throughput(criterion::Throughput::Bytes(compressed_bytes));

    let legacy = legacy_copy_unchunk::<T>(buffer.clone(), ELEMS_PER_CHUNK);
    let typed_view = typed_view_unchunk(buffer.clone(), uncompressed_bits, ELEMS_PER_CHUNK);
    assert_same_fixed_width_payloads(&legacy, &typed_view);

    group.bench_function(format!("{name}/legacy_copy/compressed_bytes"), |b| {
        b.iter(|| {
            let decoded =
                legacy_copy_unchunk::<T>(black_box(buffer.clone()), black_box(ELEMS_PER_CHUNK));
            let fixed = decoded.as_fixed_width().unwrap();
            black_box(fixed.data.as_ref());
        })
    });

    group.bench_function(format!("{name}/typed_view/compressed_bytes"), |b| {
        b.iter(|| {
            let decoded = typed_view_unchunk(
                black_box(buffer.clone()),
                black_box(uncompressed_bits),
                black_box(ELEMS_PER_CHUNK),
            );
            let fixed = decoded.as_fixed_width().unwrap();
            black_box(fixed.data.as_ref());
        })
    });
}

#[cfg(feature = "bitpacking")]
fn bench_decode_inline_bitpacking_unchunk(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_inline_bitpacking_unchunk");
    bench_inline_bitpacking_case::<u32>(&mut group, "u32_bw12_1024", 12);
    bench_inline_bitpacking_case::<u64>(&mut group, "u64_bw23_1024", 23);
    group.finish();
}

#[cfg(not(feature = "bitpacking"))]
fn bench_decode_inline_bitpacking_unchunk(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_inline_bitpacking_unchunk");
    group.bench_function("bitpacking_feature_disabled", |b| b.iter(|| black_box(())));
    group.finish();
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
        .with_profiler(lance_testing::pprof::PProfProfiler::new(100, lance_testing::pprof::Output::Flamegraph(None)));
    targets = bench_decode, bench_decode_fsl, bench_decode_str_with_dict_encoding, bench_decode_packed_struct,
                bench_decode_str_with_fixed_size_binary_encoding, bench_decode_compressed,
                bench_decode_compressed_parallel, bench_decode_inline_bitpacking_unchunk);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_decode, bench_decode_fsl, bench_decode_str_with_dict_encoding, bench_decode_packed_struct,
                bench_decode_compressed, bench_decode_compressed_parallel, bench_decode_inline_bitpacking_unchunk);
criterion_main!(benches);
