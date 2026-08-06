// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::fs::File;
use std::io::{BufRead, BufReader};

use arrow_array::StringArray;
use fsst::fsst::{FSST_SYMBOL_TABLE_SIZE, compress, decompress};
use rand::Rng;

const TEST_NUM: usize = 20;
const BUFFER_SIZE: usize = 8 * 1024 * 1024;

fn read_random_8_m_chunk(file_path: &str) -> Result<StringArray, std::io::Error> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    let lines: Vec<String> = reader.lines().collect::<std::result::Result<_, _>>()?;
    let num_lines = lines.len();

    let mut rng = rand::rng();
    let mut curr_line = rng.random_range(0..num_lines);

    let chunk_size = BUFFER_SIZE;
    let mut size = 0;
    let mut result_lines = vec![];
    while size + lines[curr_line].len() < chunk_size {
        result_lines.push(lines[curr_line].clone());
        size += lines[curr_line].len();
        curr_line += 1;
        curr_line %= num_lines;
    }

    Ok(StringArray::from(result_lines))
}

fn benchmark(file_path: &str) {
    // Step 1: load data in memory
    let mut inputs: Vec<StringArray> = vec![];
    let mut symbol_tables: Vec<[u8; FSST_SYMBOL_TABLE_SIZE]> = vec![];
    for _ in 0..TEST_NUM {
        let this_input = read_random_8_m_chunk(file_path).unwrap();
        inputs.push(this_input);
        symbol_tables.push([0u8; FSST_SYMBOL_TABLE_SIZE]);
    }

    // Step 2: allocate memory for compression and decompression outputs
    let mut compression_out_bufs = vec![];
    let mut compression_out_offsets_bufs = vec![];
    for _ in 0..TEST_NUM {
        let this_com_out_buf = vec![0u8; BUFFER_SIZE];
        let this_com_out_offsets_buf = vec![0i32; BUFFER_SIZE];
        compression_out_bufs.push(this_com_out_buf);
        compression_out_offsets_bufs.push(this_com_out_offsets_buf);
    }
    let mut decompression_out_bufs = vec![];
    let mut decompression_out_offsets_bufs = vec![];
    for _ in 0..TEST_NUM {
        // `decompress` requires the output buffer to be at least 8x the compressed input (a 1-byte
        // code can expand to an 8-byte symbol). The compressed buffer is at most `BUFFER_SIZE`, so
        // `BUFFER_SIZE * 8` is a safe upper bound.
        let this_decom_out_buf = vec![0u8; BUFFER_SIZE * 8];
        let this_decom_out_offsets_buf = vec![0i32; BUFFER_SIZE * 3];
        decompression_out_bufs.push(this_decom_out_buf);
        decompression_out_offsets_bufs.push(this_decom_out_offsets_buf);
    }

    let original_total_size: usize = inputs.iter().map(|input| input.values().len()).sum();

    // Step 3: compress data
    let start = std::time::Instant::now();
    for i in 0..TEST_NUM {
        compress(
            symbol_tables[i].as_mut(),
            inputs[i].values(),
            inputs[i].value_offsets(),
            &mut compression_out_bufs[i],
            &mut compression_out_offsets_bufs[i],
        )
        .unwrap();
    }
    let compression_finish_time = std::time::Instant::now();

    for i in 0..TEST_NUM {
        decompress(
            &symbol_tables[i],
            &compression_out_bufs[i],
            &compression_out_offsets_bufs[i],
            &mut decompression_out_bufs[i],
            &mut decompression_out_offsets_bufs[i],
        )
        .unwrap();
    }
    let decompression_finish_time = std::time::Instant::now();
    let compression_total_size: usize = compression_out_bufs.iter().map(|buf| buf.len()).sum();
    let compression_ratio = original_total_size as f64 / compression_total_size as f64;
    let compress_time = compression_finish_time - start;
    let decompress_time = decompression_finish_time - compression_finish_time;

    let compress_seconds =
        compress_time.as_secs() as f64 + compress_time.subsec_nanos() as f64 * 1e-9;

    let decompress_seconds =
        decompress_time.as_secs() as f64 + decompress_time.subsec_nanos() as f64 * 1e-9;

    let com_speed = (original_total_size as f64 / compress_seconds) / 1024f64 / 1024f64;

    let d_speed = (original_total_size as f64 / decompress_seconds) / 1024f64 / 1024f64;
    for i in 0..TEST_NUM {
        assert_eq!(
            inputs[i].value_offsets().len(),
            decompression_out_offsets_bufs[i].len()
        );
    }

    // Print tsv headers
    #[allow(clippy::print_stdout)]
    {
        println!("for file: {}", file_path);
        println!("Compression ratio\tCompression speed\tDecompression speed");
        println!(
            "{:.3}\t\t\t\t{:.2}MB/s\t\t\t{:.2}MB/s",
            compression_ratio, com_speed, d_speed
        );
    }
    for i in 0..TEST_NUM {
        assert_eq!(inputs[i].value_data(), decompression_out_bufs[i]);
        assert_eq!(inputs[i].value_offsets(), decompression_out_offsets_bufs[i]);
    }
}

// to run this test, download MS Marco dataset from https://msmarco.z22.web.core.windows.net/msmarcoranking/fulldocs.tsv.gz
// and use a script like this to get each column
/*
import csv
import sys
def write_second_column(input_path, output_path):
    csv.field_size_limit(sys.maxsize)
    with open(input_path, 'r') as input_file, open(output_path, 'w') as output_file:
        tsv_reader = csv.reader(input_file, delimiter='\t')
        tsv_writer = csv.writer(output_file, delimiter='\t')
        for row in tsv_reader:
            tsv_writer.writerow([row[2]])
#write_second_column('/Users/x/fulldocs.tsv', '/Users/x/first_column_fulldocs.tsv')
#write_second_column('/Users/x/fulldocs.tsv', '/Users/x/second_column_fulldocs.tsv')
write_second_column('/Users/x/fulldocs.tsv', '/Users/x/third_column_fulldocs.tsv')
*/
fn main() {
    let file_paths = [
        "/home/x/first_column_fulldocs.tsv",
        "/home/x/second_column_fulldocs.tsv",
        "/home/x/third_column_fulldocs_chunk_0.tsv",
    ];
    for file_path in file_paths {
        benchmark(file_path);
    }
}
