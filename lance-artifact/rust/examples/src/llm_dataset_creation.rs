// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! This example demonstrates how to:
//!
//! 1. Download and process a text dataset in parts from huggingface
//! 2. Tokenize the text data with a custom RecordBatchReader
//! 3. Save it as a Lance dataset using Lance API
//!
//! Run with `cargo run -v --package lance-examples --example llm_dataset_creation`
//!
//!
// linked to `docs/examples/Rust/llm_dataset_creation.rst`

use arrow::array::{Array, Int64Builder, ListBuilder, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::record_batch::RecordBatchReader;
use futures::StreamExt;
use hf_hub::{Repo, RepoType, api::sync::Api};
use lance::dataset::WriteParams;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rand::SeedableRng;
use rand::seq::SliceRandom;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokenizers::Tokenizer;

// Implement a custom stream batch reader
struct WikiTextBatchReader {
    schema: Arc<Schema>,
    parquet_readers: Vec<Option<ParquetRecordBatchReaderBuilder<File>>>,
    current_reader_idx: usize,
    current_reader: Option<Box<dyn RecordBatchReader + Send>>,
    tokenizer: Tokenizer,
    num_samples: u64,
    cur_samples_cnt: u64,
}

impl WikiTextBatchReader {
    fn new(
        parquet_readers: Vec<ParquetRecordBatchReaderBuilder<File>>,
        tokenizer: Tokenizer,
        num_samples: Option<u64>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "input_ids",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            false,
        )]));

        Ok(Self {
            schema,
            parquet_readers: parquet_readers.into_iter().map(Some).collect(),
            current_reader_idx: 0,
            current_reader: None,
            tokenizer,
            num_samples: num_samples.unwrap_or(100_000),
            cur_samples_cnt: 0,
        })
    }

    fn process_batch(
        &mut self,
        input_batch: &RecordBatch,
    ) -> Result<RecordBatch, arrow::error::ArrowError> {
        let num_rows = input_batch.num_rows();
        let mut token_builder = ListBuilder::new(Int64Builder::with_capacity(num_rows * 1024)); // Pre-allocate space
        let mut should_break = false;

        let column = input_batch.column_by_name("text").unwrap();
        let string_array = column
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for i in 0..num_rows {
            if self.cur_samples_cnt >= self.num_samples {
                should_break = true;
                break;
            }
            if !Array::is_null(string_array, i) {
                let text = string_array.value(i);
                // Split paragraph into lines
                for line in text.split('\n') {
                    if let Ok(encoding) = self.tokenizer.encode(line, true) {
                        let tb_values = token_builder.values();
                        for &id in encoding.get_ids() {
                            tb_values.append_value(id as i64);
                        }
                        token_builder.append(true);
                        self.cur_samples_cnt += 1;
                        if self.cur_samples_cnt.is_multiple_of(5000) {
                            println!("Processed {} rows", self.cur_samples_cnt);
                        }
                        if self.cur_samples_cnt >= self.num_samples {
                            should_break = true;
                            break;
                        }
                    }
                }
            }
        }

        // Create array and shuffle it
        let input_ids_array = token_builder.finish();

        // Create shuffled array by randomly sampling indices
        let mut rng = rand::rngs::StdRng::seed_from_u64(1337);
        let len = input_ids_array.len();
        let mut indices: Vec<u32> = (0..len as u32).collect();
        indices.shuffle(&mut rng);

        // Take values in shuffled order
        let indices_array = UInt32Array::from(indices);
        let shuffled = arrow::compute::take(&input_ids_array, &indices_array, None)?;

        let batch = RecordBatch::try_new(self.schema.clone(), vec![Arc::new(shuffled)]);
        if should_break {
            println!("Stop at {} rows", self.cur_samples_cnt);
            self.parquet_readers.clear();
            self.current_reader = None;
        }

        batch
    }
}

impl RecordBatchReader for WikiTextBatchReader {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

impl Iterator for WikiTextBatchReader {
    type Item = Result<RecordBatch, arrow::error::ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If we have a current reader, try to get next batch
            if let Some(reader) = &mut self.current_reader
                && let Some(batch_result) = reader.next()
            {
                return Some(batch_result.and_then(|batch| self.process_batch(&batch)));
            }

            // If no current reader or current reader is exhausted, try to get next reader
            if self.current_reader_idx < self.parquet_readers.len()
                && let Some(builder) = self.parquet_readers[self.current_reader_idx].take()
            {
                match builder.build() {
                    Ok(reader) => {
                        self.current_reader = Some(Box::new(reader));
                        self.current_reader_idx += 1;
                        continue;
                    }
                    Err(e) => {
                        return Some(Err(arrow::error::ArrowError::ExternalError(Box::new(e))));
                    }
                }
            }

            // No more readers available
            return None;
        }
    }
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        // Load tokenizer
        let tokenizer = load_tokenizer("gpt2")?;

        // Set up Hugging Face API
        // Download from https://huggingface.co/datasets/Salesforce/wikitext/tree/main/wikitext-103-raw-v1
        let api = Api::new()?;
        let repo = api.repo(Repo::with_revision(
            "Salesforce/wikitext".into(),
            RepoType::Dataset,
            "main".into(),
        ));

        // Define the parquet files we want to download
        let train_files = vec![
            "wikitext-103-raw-v1/train-00000-of-00002.parquet",
            "wikitext-103-raw-v1/train-00001-of-00002.parquet",
        ];

        let mut parquet_readers = Vec::new();
        for file in &train_files {
            println!("Downloading file: {}", file);
            let file_path = repo.get(file)?;
            let data = std::fs::read(file_path)?;

            // Create a temporary file in the system temp directory and write the downloaded data to it
            let mut temp_file = NamedTempFile::new()?;
            temp_file.write_all(&data)?;

            // Create the parquet reader builder with a larger batch size
            let builder = ParquetRecordBatchReaderBuilder::try_new(temp_file.into_file())?
                .with_batch_size(8192); // Increase batch size for better performance
            parquet_readers.push(builder);
        }

        if parquet_readers.is_empty() {
            println!("No parquet files found to process.");
            return Ok(());
        }

        // Create batch reader
        let num_samples: u64 = 500_000;
        let batch_reader = WikiTextBatchReader::new(parquet_readers, tokenizer, Some(num_samples))?;

        // Save as Lance dataset
        println!("Writing to Lance dataset...");
        let lance_dataset_path = "rust_wikitext_lance_dataset.lance";

        let write_params = WriteParams::default();
        lance::Dataset::write(batch_reader, lance_dataset_path, Some(write_params)).await?;

        // Verify the dataset
        let ds = lance::Dataset::open(lance_dataset_path).await?;
        let scanner = ds.scan();
        let mut stream = scanner.try_into_stream().await?;

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            total_rows += batch.num_rows();
        }

        println!(
            "Lance dataset created successfully with {} rows",
            total_rows
        );
        println!("Dataset location: {}", lance_dataset_path);

        Ok(())
    })
}

fn load_tokenizer(model_name: &str) -> Result<Tokenizer, Box<dyn Error + Send + Sync>> {
    let api = Api::new()?;
    let repo = api.repo(Repo::with_revision(
        model_name.into(),
        RepoType::Model,
        "main".into(),
    ));

    let tokenizer_path = repo.get("tokenizer.json")?;
    let tokenizer = Tokenizer::from_file(tokenizer_path)?;

    Ok(tokenizer)
}
