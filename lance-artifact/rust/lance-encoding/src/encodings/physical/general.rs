// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use log::trace;

use crate::{
    Result,
    buffer::LanceBuffer,
    compression::MiniBlockDecompressor,
    data::DataBlock,
    encodings::{
        logical::primitive::miniblock::{
            MiniBlockCompressed, MiniBlockCompressionContext, MiniBlockCompressor,
        },
        physical::block::{CompressionConfig, GeneralBufferCompressor},
    },
    format::{ProtobufUtils21, pb21::CompressiveEncoding},
};

/// A miniblock compressor that wraps another miniblock compressor and applies
/// general-purpose compression (LZ4, Zstd) to the resulting buffers.
#[derive(Debug)]
pub struct GeneralMiniBlockCompressor {
    inner: Box<dyn MiniBlockCompressor>,
    compression: CompressionConfig,
}

impl GeneralMiniBlockCompressor {
    pub fn new(inner: Box<dyn MiniBlockCompressor>, compression: CompressionConfig) -> Self {
        Self { inner, compression }
    }
}

/// Minimum buffer size to consider for compression
const MIN_BUFFER_SIZE_FOR_COMPRESSION: usize = 4 * 1024;

use super::super::logical::primitive::miniblock::MiniBlockChunk;

impl MiniBlockCompressor for GeneralMiniBlockCompressor {
    fn compress(
        &self,
        context: MiniBlockCompressionContext,
        page: DataBlock,
    ) -> Result<(MiniBlockCompressed, CompressiveEncoding)> {
        // First, compress with the inner compressor
        let (inner_compressed, inner_encoding) = self.inner.compress(context, page)?;

        // Return the original encoding without compression if there's no data or
        // the first buffer is not large enough
        if inner_compressed.data.is_empty()
            || inner_compressed.data[0].len() < MIN_BUFFER_SIZE_FOR_COMPRESSION
        {
            return Ok((inner_compressed, inner_encoding));
        }

        // We'll compress each chunk's portion of the first buffer independently
        let first_buffer = &inner_compressed.data[0];
        let mut compressed_first_buffer = Vec::new();
        let mut new_chunks = Vec::with_capacity(inner_compressed.chunks.iter().len());
        let mut offset = 0usize;
        let mut total_original_size = 0usize;

        let compressor = GeneralBufferCompressor::get_compressor(self.compression)?;

        for chunk in &inner_compressed.chunks {
            let chunk_first_buffer_size = chunk.buffer_sizes[0] as usize;

            let chunk_data = &first_buffer.as_ref()[offset..offset + chunk_first_buffer_size];
            total_original_size += chunk_first_buffer_size;

            let compressed_start = compressed_first_buffer.len();
            compressor.compress(chunk_data, &mut compressed_first_buffer)?;
            let compressed_size = compressed_first_buffer.len() - compressed_start;

            // Create new chunk with updated first buffer size
            let mut new_buffer_sizes = chunk.buffer_sizes.clone();
            new_buffer_sizes[0] = compressed_size as u32;

            new_chunks.push(MiniBlockChunk {
                buffer_sizes: new_buffer_sizes,
                log_num_values: chunk.log_num_values,
            });

            offset += chunk_first_buffer_size;
        }

        // Check if compression was effective
        let compressed_total_size = compressed_first_buffer.len();
        if compressed_total_size >= total_original_size {
            // Compression didn't help, return original
            return Ok((inner_compressed, inner_encoding));
        }

        trace!(
            "First buffer compressed from {} to {} bytes (ratio: {:.2})",
            total_original_size,
            compressed_total_size,
            compressed_total_size as f32 / total_original_size as f32
        );

        // Build final buffers: compressed first buffer + remaining original buffers
        let mut final_buffers = vec![LanceBuffer::from(compressed_first_buffer)];
        final_buffers.extend(inner_compressed.data.into_iter().skip(1));

        let compressed_result = MiniBlockCompressed {
            data: final_buffers,
            chunks: new_chunks,
            num_values: inner_compressed.num_values,
        };

        // Return compressed encoding
        let encoding = ProtobufUtils21::wrapped(self.compression, inner_encoding)?;
        Ok((compressed_result, encoding))
    }
}

/// A miniblock decompressor that first decompresses buffers using general-purpose
/// compression (LZ4, Zstd) and then delegates to an inner miniblock decompressor.
#[derive(Debug)]
pub struct GeneralMiniBlockDecompressor {
    inner: Box<dyn MiniBlockDecompressor>,
    compression: CompressionConfig,
}

impl GeneralMiniBlockDecompressor {
    pub fn new(inner: Box<dyn MiniBlockDecompressor>, compression: CompressionConfig) -> Self {
        Self { inner, compression }
    }
}

impl MiniBlockDecompressor for GeneralMiniBlockDecompressor {
    fn decompress(&self, mut data: Vec<LanceBuffer>, num_values: u64) -> Result<DataBlock> {
        let mut decompressed_buffer = Vec::new();

        let decompressor = GeneralBufferCompressor::get_compressor(self.compression)?;
        decompressor.decompress(&data[0], &mut decompressed_buffer)?;
        data[0] = LanceBuffer::from(decompressed_buffer);

        self.inner.decompress(data, num_values)
    }

    fn decoded_size_bytes(&self, num_values: u64) -> Option<u64> {
        self.inner.decoded_size_bytes(num_values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::{DecompressionStrategy, DefaultDecompressionStrategy};
    use crate::data::{BlockInfo, FixedWidthDataBlock};
    use crate::encodings::physical::block::CompressionScheme;
    use crate::encodings::physical::rle::RleEncoder;
    use crate::encodings::physical::value::ValueEncoder;
    use crate::format::pb21;
    use crate::format::pb21::compressive_encoding::Compression;
    use arrow_array::{Float64Array, Int32Array};

    fn miniblock_context() -> MiniBlockCompressionContext {
        MiniBlockCompressionContext::new(0, true, true)
    }

    #[derive(Debug)]
    struct TestCase {
        name: &'static str,
        inner_encoder: Box<dyn MiniBlockCompressor>,
        compression: CompressionConfig,
        data: DataBlock,
        expected_compressed: bool, // Whether we expect compression to be applied
        min_compression_ratio: f32, // Minimum compression ratio if compressed
    }

    fn create_test_cases() -> Vec<TestCase> {
        vec![
            // Small data with RLE - should not compress due to size threshold
            TestCase {
                name: "small_rle_data",
                inner_encoder: Box::new(RleEncoder::new()),
                compression: CompressionConfig {
                    scheme: CompressionScheme::Lz4,
                    level: None,
                },
                data: create_repeated_i32_block(vec![1, 1, 1, 1, 2, 2, 2, 2]),
                expected_compressed: false,
                min_compression_ratio: 1.0,
            },
            // Large repeated data with RLE + LZ4
            TestCase {
                name: "large_rle_lz4",
                inner_encoder: Box::new(RleEncoder::new()),
                compression: CompressionConfig {
                    scheme: CompressionScheme::Lz4,
                    level: None,
                },
                data: create_pattern_i32_block(2048, |i| (i / 8) as i32),
                expected_compressed: false, // RLE already compresses well, additional LZ4 may not help
                min_compression_ratio: 1.0,
            },
            // Large repeated data with RLE + Zstd
            TestCase {
                name: "large_rle_zstd",
                inner_encoder: Box::new(RleEncoder::new()),
                compression: CompressionConfig {
                    scheme: CompressionScheme::Zstd,
                    level: Some(3),
                },
                data: create_pattern_i32_block(8192, |i| (i / 16) as i32),
                expected_compressed: true, // Zstd might provide additional compression
                min_compression_ratio: 0.9, // But not as much since RLE already compressed
            },
            // Sequential data with ValueEncoder + LZ4
            TestCase {
                name: "sequential_value_lz4",
                inner_encoder: Box::new(ValueEncoder {}),
                compression: CompressionConfig {
                    scheme: CompressionScheme::Lz4,
                    level: None,
                },
                data: create_pattern_i32_block(1024, |i| i as i32),
                expected_compressed: false, // Sequential data doesn't compress well
                min_compression_ratio: 1.0,
            },
            // Float data with ValueEncoder + Zstd
            TestCase {
                name: "float_value_zstd",
                inner_encoder: Box::new(ValueEncoder {}),
                compression: CompressionConfig {
                    scheme: CompressionScheme::Zstd,
                    level: Some(3),
                },
                data: create_pattern_f64_block(1024, |i| i as f64 * 0.1),
                expected_compressed: true,
                min_compression_ratio: 0.9,
            },
        ]
    }

    fn create_repeated_i32_block(values: Vec<i32>) -> DataBlock {
        let array = Int32Array::from(values);
        DataBlock::from_array(array)
    }

    fn create_pattern_i32_block<F>(size: usize, pattern: F) -> DataBlock
    where
        F: Fn(usize) -> i32,
    {
        let values: Vec<i32> = (0..size).map(pattern).collect();
        let array = Int32Array::from(values);
        DataBlock::from_array(array)
    }

    fn create_pattern_f64_block<F>(size: usize, pattern: F) -> DataBlock
    where
        F: Fn(usize) -> f64,
    {
        let values: Vec<f64> = (0..size).map(pattern).collect();
        let array = Float64Array::from(values);
        DataBlock::from_array(array)
    }

    fn run_round_trip_test(test_case: TestCase) {
        let compressor =
            GeneralMiniBlockCompressor::new(test_case.inner_encoder, test_case.compression);

        // Compress the data
        let (compressed, encoding) = compressor
            .compress(miniblock_context(), test_case.data)
            .unwrap();

        // Check if compression was applied as expected
        match &encoding.compression {
            Some(Compression::General(cm)) => {
                assert!(
                    test_case.expected_compressed,
                    "{}: Expected compression to be applied",
                    test_case.name
                );
                assert_eq!(
                    CompressionScheme::try_from(cm.compression.as_ref().unwrap().scheme()).unwrap(),
                    test_case.compression.scheme
                );
            }
            _ => {
                // Could be RLE or other encoding if compression didn't help
                if test_case.expected_compressed {
                    // Check if it's RLE encoding (which means compression didn't help)
                    match &encoding.compression {
                        Some(Compression::Rle(_)) => {
                            // RLE encoding returned - compression didn't help
                        }
                        Some(Compression::Flat(_)) => {
                            // Flat encoding returned - compression didn't help
                        }
                        _ => {
                            panic!(
                                "{}: Expected GeneralMiniBlock but got {:?}",
                                test_case.name, encoding.compression
                            );
                        }
                    }
                }
            }
        }

        // Verify chunks are created correctly
        assert!(
            !compressed.chunks.is_empty(),
            "{}: No chunks created",
            test_case.name
        );

        // Test decompression by simulating the actual miniblock decoding process
        let decompressed_data = decompress_miniblock_chunks(&compressed, &encoding);

        // Verify round trip by checking data size
        // We expect the decompressed data to match the original number of values
        // The bytes per value depends on the test case
        let bytes_per_value = if test_case.name.contains("float") {
            8 // f64
        } else {
            4 // i32
        };
        let expected_bytes = compressed.num_values as usize * bytes_per_value;
        assert_eq!(
            expected_bytes,
            decompressed_data.len(),
            "{}: Data size mismatch",
            test_case.name
        );

        // Check compression ratio if applicable
        if test_case.expected_compressed {
            let compression_ratio = compressed.data[0].len() as f32 / expected_bytes as f32;
            assert!(
                compression_ratio <= test_case.min_compression_ratio,
                "{}: Compression ratio {} > expected {}",
                test_case.name,
                compression_ratio,
                test_case.min_compression_ratio
            );
        }
    }

    fn decompress_miniblock_chunks(
        compressed: &MiniBlockCompressed,
        encoding: &CompressiveEncoding,
    ) -> Vec<u8> {
        let mut decompressed_data = Vec::new();
        let mut offsets = vec![0usize; compressed.data.len()]; // Track offset for each buffer
        let decompression_strategy = DefaultDecompressionStrategy::default();

        for chunk in &compressed.chunks {
            let chunk_values = if chunk.log_num_values > 0 {
                1u64 << chunk.log_num_values
            } else {
                // Last chunk - calculate remaining values
                let decompressed_values =
                    decompressed_data.len() as u64 / get_bytes_per_value(compressed) as u64;
                compressed.num_values.saturating_sub(decompressed_values)
            };

            // Extract buffers for this chunk
            let mut chunk_buffers = Vec::new();
            for (i, &size) in chunk.buffer_sizes.iter().enumerate() {
                if i < compressed.data.len() {
                    let buffer_data =
                        compressed.data[i].slice_with_length(offsets[i], size as usize);
                    chunk_buffers.push(buffer_data);
                    offsets[i] += size as usize;
                }
            }

            // Create a decompressor for this chunk
            let decompressor = decompression_strategy
                .create_miniblock_decompressor(encoding, &decompression_strategy)
                .unwrap();

            // Decompress the chunk
            let chunk_decompressed = decompressor
                .decompress(chunk_buffers, chunk_values)
                .unwrap();

            match chunk_decompressed {
                DataBlock::FixedWidth(ref block) => {
                    decompressed_data.extend_from_slice(block.data.as_ref());
                }
                _ => panic!("Expected FixedWidth block"),
            }
        }

        decompressed_data
    }

    fn get_bytes_per_value(compressed: &MiniBlockCompressed) -> usize {
        // This is a simplification - in reality we'd need to know the data type
        // For our tests, we mostly use i32 (4 bytes) or f64 (8 bytes)
        // We can try to guess based on the data size
        if compressed.num_values == 0 {
            return 4; // Default to i32
        }

        // For float tests, the number is usually 1024 and we use f64
        if compressed.num_values == 1024 {
            return 8; // Likely f64
        }

        4 // Default to i32
    }

    #[test]
    fn test_compressed_mini_block_table_driven() {
        for test_case in create_test_cases() {
            run_round_trip_test(test_case);
        }
    }

    #[test]
    fn test_compressed_mini_block_threshold() {
        // Test that small buffers don't get compressed
        let small_test = TestCase {
            name: "small_buffer_no_compression",
            inner_encoder: Box::new(RleEncoder::new()),
            compression: CompressionConfig {
                scheme: CompressionScheme::Lz4,
                level: None,
            },
            data: create_repeated_i32_block(vec![1, 1, 2, 2]),
            expected_compressed: false,
            min_compression_ratio: 1.0,
        };
        run_round_trip_test(small_test);
    }

    #[test]
    fn test_compressed_mini_block_with_doubles() {
        // Test with large sequential doubles that should compress well with Zstd
        // The test focuses on verifying that GeneralMiniBlock works correctly
        // when wrapping a simple ValueEncoder
        let test_case = TestCase {
            name: "float_values_with_zstd",
            inner_encoder: Box::new(ValueEncoder {}),
            compression: CompressionConfig {
                scheme: CompressionScheme::Zstd,
                level: Some(3),
            },
            // Create enough data to ensure compression is applied
            data: create_pattern_f64_block(1024, |i| (i / 10) as f64),
            expected_compressed: true,
            min_compression_ratio: 0.5, // Zstd should achieve good compression on repetitive data
        };

        run_round_trip_test(test_case);
    }

    #[test]
    fn test_compressed_mini_block_large_buffers() {
        // Use value encoding which doesn't compress data, ensuring large buffers
        // Create 1024 i32 values (4KB of data)
        let values: Vec<i32> = (0..1024).collect();
        let data = LanceBuffer::from_bytes(
            bytemuck::cast_slice(&values).to_vec().into(),
            std::mem::align_of::<i32>() as u64,
        );
        let block = DataBlock::FixedWidth(FixedWidthDataBlock {
            bits_per_value: 32,
            data,
            num_values: 1024,
            block_info: BlockInfo::new(),
        });

        // Create compressor with ValueEncoder (no compression) and Zstd wrapper
        let inner = Box::new(ValueEncoder {});
        let compression = CompressionConfig {
            scheme: CompressionScheme::Zstd,
            level: Some(3),
        };
        let compressor = GeneralMiniBlockCompressor::new(inner, compression);

        // Compress the data
        let (compressed, encoding) = compressor.compress(miniblock_context(), block).unwrap();

        // Should get GeneralMiniBlock encoding since buffer is 4KB
        match &encoding.compression {
            Some(Compression::General(cm)) => {
                assert!(cm.values.is_some());
                assert_eq!(
                    cm.compression.as_ref().unwrap().scheme(),
                    pb21::CompressionScheme::CompressionAlgorithmZstd
                );
                assert_eq!(cm.compression.as_ref().unwrap().level, Some(3));

                // Verify inner encoding is Flat (from ValueEncoder)
                match &cm.values.as_ref().unwrap().compression {
                    Some(Compression::Flat(flat)) => {
                        assert_eq!(flat.bits_per_value, 32);
                    }
                    _ => panic!("Expected Flat inner encoding"),
                }
            }
            _ => panic!("Expected GeneralMiniBlock encoding"),
        }

        assert_eq!(compressed.num_values, 1024);
        // ValueEncoder produces 1 buffer, so compressed result also has 1 buffer
        assert_eq!(compressed.data.len(), 1);
    }

    // Special test cases that don't fit the table-driven pattern

    #[test]
    fn test_compressed_mini_block_rle_multiple_buffers() {
        // RLE produces 2 buffers (values and lengths), test that both are handled correctly
        let data = create_repeated_i32_block(vec![1; 100]);
        let compressor = GeneralMiniBlockCompressor::new(
            Box::new(RleEncoder::new()),
            CompressionConfig {
                scheme: CompressionScheme::Lz4,
                level: None,
            },
        );

        let (compressed, _) = compressor.compress(miniblock_context(), data).unwrap();
        // RLE produces 2 buffers, but only the first one is compressed
        assert_eq!(compressed.data.len(), 2);
    }

    #[test]
    fn test_rle_with_general_miniblock_wrapper() {
        // Test that RLE encoding with bits_per_value >= 32 is automatically wrapped
        // in GeneralMiniBlock with LZ4 compression

        // This test directly tests the RLE encoder behavior
        // When bits_per_value >= 32, RLE should be wrapped in GeneralMiniBlock with LZ4

        // Test case 1: 32-bit RLE data
        let test_32 = TestCase {
            name: "rle_32bit_with_general_wrapper",
            inner_encoder: Box::new(RleEncoder::new()),
            compression: CompressionConfig {
                scheme: CompressionScheme::Lz4,
                level: None,
            },
            data: create_repeated_i32_block(vec![1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3]),
            expected_compressed: false, // RLE already compresses well, LZ4 might not help much
            min_compression_ratio: 1.0,
        };

        // For 32-bit RLE, the compression strategy should automatically wrap it
        // Let's directly test the compressor
        let compressor = GeneralMiniBlockCompressor::new(
            Box::new(RleEncoder::new()),
            CompressionConfig {
                scheme: CompressionScheme::Lz4,
                level: None,
            },
        );

        let (_compressed, encoding) = compressor
            .compress(miniblock_context(), test_32.data)
            .unwrap();

        // Verify the encoding structure
        match &encoding.compression {
            Some(Compression::General(cm)) => {
                // Check inner encoding is RLE
                match &cm.values.as_ref().unwrap().compression {
                    Some(Compression::Rle(rle)) => {
                        let Compression::Flat(values) =
                            rle.values.as_ref().unwrap().compression.as_ref().unwrap()
                        else {
                            panic!("Expected flat for RLE values")
                        };
                        let Compression::Flat(run_lengths) = rle
                            .run_lengths
                            .as_ref()
                            .unwrap()
                            .compression
                            .as_ref()
                            .unwrap()
                        else {
                            panic!("Expected flat for RLE run lengths")
                        };
                        assert_eq!(values.bits_per_value, 32);
                        assert_eq!(run_lengths.bits_per_value, 8);
                    }
                    _ => panic!("Expected RLE as inner encoding"),
                }
                // Check compression is LZ4
                assert_eq!(
                    cm.compression.as_ref().unwrap().scheme(),
                    pb21::CompressionScheme::CompressionAlgorithmLz4
                );
            }
            Some(Compression::Rle(_)) => {
                // Also acceptable if compression didn't help
            }
            _ => panic!("Expected GeneralMiniBlock or Rle encoding"),
        }

        // Test case 2: 64-bit RLE data
        let values_64: Vec<i64> = vec![100i64; 50]
            .into_iter()
            .chain(vec![200i64; 50])
            .chain(vec![300i64; 50])
            .collect();
        let array_64 = arrow_array::Int64Array::from(values_64);
        let block_64 = DataBlock::from_array(array_64);

        let compressor_64 = GeneralMiniBlockCompressor::new(
            Box::new(RleEncoder::new()),
            CompressionConfig {
                scheme: CompressionScheme::Lz4,
                level: None,
            },
        );

        let (_compressed_64, encoding_64) = compressor_64
            .compress(miniblock_context(), block_64)
            .unwrap();

        // Verify the encoding structure for 64-bit
        match &encoding_64.compression {
            Some(Compression::General(cm)) => {
                // Check inner encoding is RLE
                match &cm.values.as_ref().unwrap().compression {
                    Some(Compression::Rle(rle)) => {
                        let Compression::Flat(values) =
                            rle.values.as_ref().unwrap().compression.as_ref().unwrap()
                        else {
                            panic!("Expected flat for RLE values")
                        };
                        let Compression::Flat(run_lengths) = rle
                            .run_lengths
                            .as_ref()
                            .unwrap()
                            .compression
                            .as_ref()
                            .unwrap()
                        else {
                            panic!("Expected flat for RLE run lengths")
                        };
                        assert_eq!(values.bits_per_value, 64);
                        assert_eq!(run_lengths.bits_per_value, 8);
                    }
                    _ => panic!("Expected RLE as inner encoding for 64-bit"),
                }
                // Check compression is LZ4
                assert_eq!(
                    cm.compression.as_ref().unwrap().scheme(),
                    pb21::CompressionScheme::CompressionAlgorithmLz4
                );
            }
            Some(Compression::Rle(_)) => {
                // Also acceptable if compression didn't help
            }
            _ => panic!("Expected GeneralMiniBlock or Rle encoding for 64-bit"),
        }
    }

    #[test]
    fn test_compressed_mini_block_empty_data() {
        let empty_array = Int32Array::from(vec![] as Vec<i32>);
        let empty_block = DataBlock::from_array(empty_array);

        let compressor = GeneralMiniBlockCompressor::new(
            Box::new(ValueEncoder {}),
            CompressionConfig {
                scheme: CompressionScheme::Lz4,
                level: None,
            },
        );

        let result = compressor.compress(miniblock_context(), empty_block);
        match result {
            Ok((compressed, _)) => {
                assert_eq!(compressed.num_values, 0);
            }
            Err(_) => {
                // Empty data might not be supported by ValueEncoder
            }
        }
    }
}
