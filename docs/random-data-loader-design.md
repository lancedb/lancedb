# LanceDB Random Data Loader Design Document

## Overview

This design document outlines the implementation of a data loader class for LanceDB in Rust that can load data from a LanceDB table in random order. The loader will be framework-agnostic and leverage a new `take` method in the `BaseTable` trait for efficient row-offset-based loading.

The loader operates in two phases. First, a permutation of the base table is created. This permutation is persisted as a LanceDB table (can be persisted in memory or on disk). Next, a data loader is created from a permutation table. The data loader can be used to load data from the base table, applying the permutation.

## Requirements Analysis

Based on the codebase analysis, here are the key requirements and constraints:

### Functional Requirements

1. **Random Order Loading**: Load data from LanceDB table in random order
2. **Persistable**: The permutation should be persisted as a LanceDB table so that it can be reused for subsequent runs
3. **External Shuffle**: The permutation should be shuffled externally to avoid loading all offsets into memory
4. **Table Offsets**: The permutation should be stored as a dataset of offsets into a base table
5. **Dataset metadata**: Details such as the base table name and loading config should be stored in the permutation table's metadata
6. **Splits**: The data loader should support splits which is a user supplied array of percentages or row counts that define how much data is in each split. The total percentage / row count can be a smaller than the entire base table and any remaining data will be ignored.
7. **Batch Support**: When reading a permutation, the data loader should support configurable batch sizes
8. **Configurable Splitting**: Support different splitting strategies. One strategy could be random splits where a row is assigned to a split based on a random number generator. Another strategy could assign a row based on a hash of one or more columns.
9. **Shards**: The user can specify a shard size. When a shard size is specified then data will be loaded in small shards of contiguous rows. For example, a dataset with 1 million rows and a shard size of 100 will result in 10000 shards and the permutation will contain 10000 "shard offsets" instead of row offsets. The data loader will then load data in shards. This reduces the amount of random access.
10. **Column Selection**: Allow users to specify which columns to load in the data loader. The column selection is only applied to the load phase. The permutation can be reused when the user changes column selection.
11. **Framework Agnostic**: Work independently of PyTorch or other ML frameworks

### Technical Constraints

1. **LanceDB Integration**: Must work with existing `Table` struct and `BaseTable` trait
2. **Arrow Format**: Data returned as Arrow RecordBatches
3. **Async/Await**: Follow LanceDB's async patterns
4. **Error Handling**: Use LanceDB's `Result<T>` error handling

## Current LanceDB Architecture + New `take` Method

### Key Components

- **`Table` struct**: Main table interface wrapping `Arc<dyn BaseTable>`
- **`BaseTable` trait**: Core functionality including `query()` method
- **`Query` struct**: Query builder with `limit()`, `only_if()`, `execute()` methods
- **`DatasetRecordBatchStream`**: Async stream of RecordBatch data
- **NEW: `BaseTable::take` method**: Direct row access by offset indices

### New BaseTable Method

```rust
#[async_trait]
pub trait BaseTable: std::fmt::Display + std::fmt::Debug + Send + Sync {
    // ... existing methods ...

    /// Load specific rows by their offset positions in the dataset
    ///
    /// # Arguments
    /// * `indices` - Vector of row offsets to load
    /// * `columns` - Optional column selection (None for all columns)
    ///
    /// # Returns
    /// RecordBatch containing the requested rows
    async fn take(
        &self,
        indices: &[usize],
        columns: Option<&[String]>
    ) -> Result<RecordBatch>;
}
```

## Detailed Implementation Design

### Phase 1: Permutation Table Creation

#### Core Data Structures

```rust
/// Configuration for creating a permutation table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermutationConfig {
    /// Base table to permute
    pub base_table_uri: String,
    /// Random seed for reproducible shuffles
    pub seed: Option<u64>,
    /// Split configuration
    pub splits: SplitConfig,
    /// Splitting strategy
    pub split_strategy: SplitStrategy,
    /// Shard size for contiguous access (None for row-level access)
    pub shard_size: Option<usize>,
    /// Maximum rows to include (None for all rows)
    pub max_rows: Option<usize>,
}

/// Split configuration - either percentages or absolute counts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SplitConfig {
    /// Percentage splits (must sum to <= 1.0)
    Percentages(Vec<f32>),
    /// Absolute row counts per split
    Counts(Vec<usize>),
}

/// Strategy for assigning rows to splits
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SplitStrategy {
    /// Random assignment using RNG
    Random,
    /// Hash-based assignment using specified columns
    Hash { columns: Vec<String> },
    /// Sequential assignment (for debugging/testing)
    Sequential,
}
```

#### Permutation Table Schema

```rust
/// Schema for the permutation table
fn permutation_table_schema(has_shards: bool) -> SchemaRef {
    let mut fields = vec![
        Field::new("split_id", DataType::UInt32, false),
        Field::new("position_in_split", DataType::UInt64, false),
    ];
    
    if has_shards {
        fields.push(Field::new("shard_start_offset", DataType::UInt64, false));
        fields.push(Field::new("shard_size", DataType::UInt32, false));
    } else {
        fields.push(Field::new("row_offset", DataType::UInt64, false));
    }
    
    Arc::new(Schema::new(fields))
}
```

#### Permutation Builder

```rust
pub struct PermutationBuilder {
    config: PermutationConfig,
    base_table: Arc<dyn BaseTable>,
    connection: Arc<dyn Connection>,
}

impl PermutationBuilder {
    pub fn new(
        base_table: Arc<dyn BaseTable>,
        connection: Arc<dyn Connection>,
        config: PermutationConfig,
    ) -> Self {
        Self {
            config,
            base_table,
            connection,
        }
    }
    
    /// Create the permutation table using external sorting
    pub async fn build(&self, permutation_table_name: &str) -> Result<PermutationTable> {
        // 1. Get total row count
        let total_rows = self.base_table.count_rows(None).await?;
        
        // 2. Determine effective row count based on config
        let effective_rows = self.config.max_rows
            .map(|max| max.min(total_rows))
            .unwrap_or(total_rows);
        
        // 3. Calculate split sizes
        let split_sizes = self.calculate_split_sizes(effective_rows)?;
        
        // 4. Generate permutation using external shuffle
        let permutation_batches = if self.config.shard_size.is_some() {
            self.generate_shard_permutation(effective_rows, &split_sizes).await?
        } else {
            self.generate_row_permutation(effective_rows, &split_sizes).await?
        };
        
        // 5. Create permutation table
        let schema = permutation_table_schema(self.config.shard_size.is_some());
        let permutation_table = self.connection
            .create_table(permutation_table_name, Box::new(permutation_batches))
            .with_schema(schema)
            .execute()
            .await?;
        
        // 6. Store metadata
        self.store_metadata(&permutation_table).await?;
        
        Ok(PermutationTable {
            table: permutation_table,
            config: self.config.clone(),
            base_table: self.base_table.clone(),
        })
    }
    
    /// Calculate split sizes based on configuration
    fn calculate_split_sizes(&self, total_rows: usize) -> Result<Vec<usize>> {
        match &self.config.splits {
            SplitConfig::Percentages(percentages) => {
                let sum: f32 = percentages.iter().sum();
                if sum > 1.0 {
                    return Err(Error::InvalidInput {
                        message: "Split percentages cannot sum to more than 1.0".to_string(),
                    });
                }
                
                Ok(percentages.iter()
                    .map(|&p| (total_rows as f32 * p) as usize)
                    .collect())
            },
            SplitConfig::Counts(counts) => {
                let sum: usize = counts.iter().sum();
                if sum > total_rows {
                    return Err(Error::InvalidInput {
                        message: format!(
                            "Split counts {} exceed total rows {}", 
                            sum, total_rows
                        ),
                    });
                }
                Ok(counts.clone())
            }
        }
    }
    
    /// Generate row-level permutation with external shuffling
    async fn generate_row_permutation(
        &self,
        total_rows: usize,
        split_sizes: &[usize],
    ) -> Result<impl Iterator<Item = Result<RecordBatch>>> {
        
        // Create iterator over row indices with split assignments
        let mut rng = StdRng::seed_from_u64(self.config.seed.unwrap_or(0));
        let indices_with_splits = self.assign_rows_to_splits(
            0..total_rows,
            split_sizes,
            &mut rng
        )?;
        
        // External sort by (split_id, random_key) to shuffle within splits
        let shuffled_indices = self.external_shuffle(indices_with_splits, &mut rng).await?;
        
        // Convert to RecordBatch iterator
        self.indices_to_record_batches(shuffled_indices, false).await
    }
    
    /// Assign rows to splits based on strategy
    fn assign_rows_to_splits<I: Iterator<Item = usize>>(
        &self,
        indices: I,
        split_sizes: &[usize],
        rng: &mut StdRng,
    ) -> Result<Vec<(usize, u32, u64)>> { // (row_offset, split_id, sort_key)
        let mut result = Vec::new();
        let mut split_counters = vec![0usize; split_sizes.len()];
        
        for row_offset in indices {
            let split_id = match &self.config.split_strategy {
                SplitStrategy::Random => {
                    // Find a split with remaining capacity
                    let mut candidates = Vec::new();
                    for (i, (&size, &count)) in split_sizes.iter().zip(&split_counters).enumerate() {
                        if count < size {
                            candidates.push(i);
                        }
                    }
                    
                    if candidates.is_empty() {
                        break; // All splits are full
                    }
                    
                    candidates[rng.gen_range(0..candidates.len())]
                },
                SplitStrategy::Hash { columns } => {
                    // For hash-based splitting, we'd need to read the actual data
                    // This is more complex and would require a separate pass
                    todo!("Hash-based splitting requires reading column data")
                },
                SplitStrategy::Sequential => {
                    // Sequential assignment
                    let mut split_id = 0;
                    for (i, (&size, &count)) in split_sizes.iter().zip(&split_counters).enumerate() {
                        if count < size {
                            split_id = i;
                            break;
                        }
                    }
                    split_id
                }
            };
            
            split_counters[split_id] += 1;
            let sort_key = rng.gen::<u64>(); // Random sort key for shuffling
            result.push((row_offset, split_id as u32, sort_key));
        }
        
        Ok(result)
    }
    
    /// External shuffle using disk-based merge sort
    async fn external_shuffle(
        &self,
        mut data: Vec<(usize, u32, u64)>,
        _rng: &mut StdRng,
    ) -> Result<Vec<(usize, u32, u64)>> {
        // Sort by (split_id, sort_key) to group by split and shuffle within splits
        data.sort_by_key(|&(_, split_id, sort_key)| (split_id, sort_key));
        
        // Add position within split
        let mut split_positions = HashMap::new();
        for (_, split_id, _) in &mut data {
            let position = split_positions.entry(*split_id).or_insert(0u64);
            *position += 1;
        }
        
        Ok(data)
    }
    
    /// Store configuration metadata in the permutation table
    async fn store_metadata(&self, table: &Table) -> Result<()> {
        let metadata_json = serde_json::to_string(&self.config)
            .map_err(|e| Error::InvalidInput {
                message: format!("Failed to serialize config: {}", e),
            })?;
        
        // Store in table metadata (implementation depends on LanceDB metadata API)
        // This might be stored as table tags or in a separate metadata system
        table.add_tag("permutation_config", &metadata_json).await?;
        table.add_tag("base_table_uri", &self.config.base_table_uri).await?;
        
        Ok(())
    }
}
```

#### Shard-Based Permutation Generation

```rust
impl PermutationBuilder {
    /// Generate shard-level permutation for better locality
    async fn generate_shard_permutation(
        &self,
        total_rows: usize,
        split_sizes: &[usize],
    ) -> Result<impl Iterator<Item = Result<RecordBatch>>> {
        let shard_size = self.config.shard_size.unwrap();
        let num_shards = (total_rows + shard_size - 1) / shard_size; // Ceiling division
        
        let mut rng = StdRng::seed_from_u64(self.config.seed.unwrap_or(0));
        
        // Create shard descriptors with their row ranges
        let shard_descriptors: Vec<ShardDescriptor> = (0..num_shards)
            .map(|shard_id| {
                let start_offset = shard_id * shard_size;
                let end_offset = (start_offset + shard_size).min(total_rows);
                let actual_shard_size = end_offset - start_offset;
                
                ShardDescriptor {
                    start_offset,
                    size: actual_shard_size,
                }
            })
            .collect();
        
        // Assign shards to splits (treating each shard as a unit)
        let shards_with_splits = self.assign_shards_to_splits(
            shard_descriptors,
            split_sizes,
            &mut rng
        )?;
        
        // Shuffle shards within each split
        let shuffled_shards = self.external_shuffle_shards(shards_with_splits, &mut rng).await?;
        
        // Convert to RecordBatch iterator
        self.shards_to_record_batches(shuffled_shards).await
    }
    
    fn assign_shards_to_splits(
        &self,
        shards: Vec<ShardDescriptor>,
        split_sizes: &[usize],
        rng: &mut StdRng,
    ) -> Result<Vec<ShardWithSplit>> {
        let mut result = Vec::new();
        let mut split_row_counts = vec![0usize; split_sizes.len()];
        
        for shard in shards {
            // Find split with capacity for this entire shard
            let split_id = match &self.config.split_strategy {
                SplitStrategy::Random => {
                    let mut candidates = Vec::new();
                    for (i, (&max_size, &current_count)) in split_sizes.iter().zip(&split_row_counts).enumerate() {
                        if current_count + shard.size <= max_size {
                            candidates.push(i);
                        }
                    }
                    
                    if candidates.is_empty() {
                        continue; // Skip this shard if no splits have capacity
                    }
                    
                    candidates[rng.gen_range(0..candidates.len())]
                },
                SplitStrategy::Sequential => {
                    let mut split_id = None;
                    for (i, (&max_size, &current_count)) in split_sizes.iter().zip(&split_row_counts).enumerate() {
                        if current_count + shard.size <= max_size {
                            split_id = Some(i);
                            break;
                        }
                    }
                    split_id.unwrap_or_else(|| {
                        // If no perfect fit, use the split with most remaining capacity
                        split_sizes.iter().zip(&split_row_counts)
                            .enumerate()
                            .max_by_key(|(_, (&max, &current))| max.saturating_sub(current))
                            .map(|(i, _)| i)
                            .unwrap_or(0)
                    })
                },
                SplitStrategy::Hash { .. } => {
                    todo!("Hash-based shard splitting")
                }
            };
            
            split_row_counts[split_id] += shard.size;
            result.push(ShardWithSplit {
                shard: shard,
                split_id: split_id as u32,
                sort_key: rng.gen::<u64>(),
            });
        }
        
        Ok(result)
    }
}

#[derive(Debug, Clone)]
struct ShardDescriptor {
    start_offset: usize,
    size: usize,
}

#[derive(Debug, Clone)]
struct ShardWithSplit {
    shard: ShardDescriptor,
    split_id: u32,
    sort_key: u64,
}
```

### Phase 2: Data Loader Implementation

#### Core Data Loader Structure

```rust
/// Configuration for data loading from permutation table
#[derive(Debug, Clone)]
pub struct DataLoaderConfig {
    /// Batch size for loading data
    pub batch_size: usize,
    /// Columns to select from base table
    pub columns: Option<Vec<String>>,
    /// Which split to load from (None for all splits)
    pub split_id: Option<u32>,
    /// Maximum number of batches to load (None for unlimited)
    pub max_batches: Option<usize>,
}

/// Main data loader that reads from permutation table and loads base table data
pub struct RandomDataLoader {
    permutation_table: Arc<dyn BaseTable>,
    base_table: Arc<dyn BaseTable>,
    config: DataLoaderConfig,
    permutation_config: PermutationConfig,
    current_position: usize,
    is_shard_based: bool,
}

impl RandomDataLoader {
    /// Create data loader from existing permutation table
    pub async fn from_permutation_table(
        permutation_table: Arc<dyn BaseTable>,
        base_table: Arc<dyn BaseTable>,
        config: DataLoaderConfig,
    ) -> Result<Self> {
        // Load metadata from permutation table
        let metadata = permutation_table.get_tag("permutation_config").await?
            .ok_or_else(|| Error::InvalidInput {
                message: "Permutation table missing configuration metadata".to_string(),
            })?;
        
        let permutation_config: PermutationConfig = serde_json::from_str(&metadata)
            .map_err(|e| Error::InvalidInput {
                message: format!("Invalid permutation config: {}", e),
            })?;
        
        let is_shard_based = permutation_config.shard_size.is_some();
        
        Ok(Self {
            permutation_table,
            base_table,
            config,
            permutation_config,
            current_position: 0,
            is_shard_based,
        })
    }
    
    /// Load next batch of data
    pub async fn load_batch(&mut self) -> Result<Option<RecordBatch>> {
        // Check if we've reached the end or max batches
        if let Some(max_batches) = self.config.max_batches {
            if self.current_position >= max_batches * self.config.batch_size {
                return Ok(None);
            }
        }
        
        // Query permutation table for next batch of indices/shards
        let permutation_batch = self.get_permutation_batch().await?;
        if permutation_batch.num_rows() == 0 {
            return Ok(None);
        }
        
        // Load actual data from base table using indices
        let data_batch = if self.is_shard_based {
            self.load_shard_batch(permutation_batch).await?
        } else {
            self.load_row_batch(permutation_batch).await?
        };
        
        self.current_position += data_batch.num_rows();
        Ok(Some(data_batch))
    }
    
    /// Get next batch from permutation table
    async fn get_permutation_batch(&self) -> Result<RecordBatch> {
        let mut query = self.permutation_table.query();
        
        // Filter by split if specified
        if let Some(split_id) = self.config.split_id {
            query = query.only_if(&format!("split_id = {}", split_id));
        }
        
        // Apply limit and offset for this batch
        let batch = query
            .limit(self.config.batch_size)
            .offset(self.current_position)
            .execute()
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        
        // Concatenate all batches (should typically be just one)
        if batch.is_empty() {
            Ok(RecordBatch::new_empty(self.permutation_table.schema().await?))
        } else if batch.len() == 1 {
            Ok(batch.into_iter().next().unwrap())
        } else {
            let schema = batch[0].schema();
            concat_batches(&schema, &batch)
                .map_err(|e| Error::InvalidInput {
                    message: format!("Failed to concatenate batches: {}", e),
                })
        }
    }
    
    /// Load data for row-based permutation
    async fn load_row_batch(&self, permutation_batch: RecordBatch) -> Result<RecordBatch> {
        let row_offsets = permutation_batch
            .column_by_name("row_offset")
            .ok_or_else(|| Error::InvalidInput {
                message: "Permutation table missing row_offset column".to_string(),
            })?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| Error::InvalidInput {
                message: "row_offset column has wrong type".to_string(),
            })?;
        
        let indices: Vec<usize> = row_offsets.values()
            .iter()
            .map(|&offset| offset as usize)
            .collect();
        
        self.base_table.take(&indices, self.config.columns.as_deref()).await
    }
    
    /// Load data for shard-based permutation
    async fn load_shard_batch(&self, permutation_batch: RecordBatch) -> Result<RecordBatch> {
        let shard_starts = permutation_batch
            .column_by_name("shard_start_offset")
            .ok_or_else(|| Error::InvalidInput {
                message: "Permutation table missing shard_start_offset column".to_string(),
            })?
            .as_any()
            .downcast_ref::<UInt64Array>()?;
            
        let shard_sizes = permutation_batch
            .column_by_name("shard_size")
            .ok_or_else(|| Error::InvalidInput {
                message: "Permutation table missing shard_size column".to_string(),
            })?
            .as_any()
            .downcast_ref::<UInt32Array>()?;
        
        // Collect all indices from all shards in this batch
        let mut all_indices = Vec::new();
        for (start, size) in shard_starts.values().iter().zip(shard_sizes.values().iter()) {
            let start = *start as usize;
            let size = *size as usize;
            all_indices.extend(start..start + size);
        }
        
        self.base_table.take(&all_indices, self.config.columns.as_deref()).await
    }
    
    /// Create a stream interface for the data loader
    pub fn into_stream(self) -> RandomDataLoaderStream {
        RandomDataLoaderStream { loader: self }
    }
    
    /// Reset the loader to start from beginning
    pub fn reset(&mut self) {
        self.current_position = 0;
    }
    
    /// Get information about available splits
    pub async fn get_split_info(&self) -> Result<Vec<SplitInfo>> {
        let query_result = self.permutation_table
            .query()
            .select(&[
                ("split_id", "split_id"),
                ("count", "count(*)"),
                ("rows", "sum(CASE WHEN shard_size IS NULL THEN 1 ELSE shard_size END)")
            ])
            .group_by(&["split_id"])
            .execute()
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        
        // Process results to create SplitInfo
        // Implementation depends on query result format
        todo!("Convert query results to SplitInfo")
    }
}

#[derive(Debug, Clone)]
pub struct SplitInfo {
    pub split_id: u32,
    pub num_entries: usize,  // Number of rows or shards
    pub num_rows: usize,     // Total number of actual data rows
}
```

#### Stream Interface

```rust
pub struct RandomDataLoaderStream {
    loader: RandomDataLoader,
}

impl Stream for RandomDataLoaderStream {
    type Item = Result<RecordBatch>;
    
    fn poll_next(
        mut self: Pin<&mut Self>, 
        cx: &mut Context<'_>
    ) -> Poll<Option<Self::Item>> {
        let future = self.loader.load_batch();
        match future.poll_unpin(cx) {
            Poll::Ready(Ok(Some(batch))) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(Ok(None)) => Poll::Ready(None),
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Pending => Poll::Pending,
        }
    }
}
```

## Complete API Design and Usage Examples

### High-Level API

```rust
/// Main entry point for random data loading
pub struct RandomDataLoaderFactory;

impl RandomDataLoaderFactory {
    /// Create a new permutation table and data loader in one step
    pub async fn create_with_new_permutation(
        base_table: Arc<dyn BaseTable>,
        connection: Arc<dyn Connection>,
        permutation_config: PermutationConfig,
        permutation_table_name: &str,
        loader_config: DataLoaderConfig,
    ) -> Result<RandomDataLoader> {
        // Create permutation table
        let builder = PermutationBuilder::new(base_table.clone(), connection, permutation_config);
        let permutation_table = builder.build(permutation_table_name).await?;
        
        // Create data loader
        RandomDataLoader::from_permutation_table(
            permutation_table.table.inner.clone(),
            base_table,
            loader_config,
        ).await
    }
    
    /// Create data loader from existing permutation table
    pub async fn create_from_existing_permutation(
        permutation_table_name: &str,
        base_table: Arc<dyn BaseTable>,
        connection: Arc<dyn Connection>,
        loader_config: DataLoaderConfig,
    ) -> Result<RandomDataLoader> {
        let permutation_table = connection.open_table(permutation_table_name).execute().await?;
        
        RandomDataLoader::from_permutation_table(
            permutation_table.inner.clone(),
            base_table,
            loader_config,
        ).await
    }
}
```

### Usage Examples

```rust
use lancedb::random_loader::*;
use futures::TryStreamExt;

// Example 1: Basic usage with train/validation splits
async fn example_train_val_split() -> Result<()> {
    let db = connect("./my_dataset").execute().await?;
    let table = db.open_table("my_data").execute().await?;
    
    // Create permutation with 80/20 train/val split
    let permutation_config = PermutationConfig {
        base_table_uri: table.dataset_uri().to_string(),
        seed: Some(42),
        splits: SplitConfig::Percentages(vec![0.8, 0.2]), // 80% train, 20% val
        split_strategy: SplitStrategy::Random,
        shard_size: Some(1000), // Use shards for better locality
        max_rows: None,
    };
    
    // Create training loader
    let train_config = DataLoaderConfig {
        batch_size: 32,
        columns: Some(vec!["features".to_string(), "label".to_string()]),
        split_id: Some(0), // First split (training)
        max_batches: None,
    };
    
    let mut train_loader = RandomDataLoaderFactory::create_with_new_permutation(
        table.inner.clone(),
        db.clone(),
        permutation_config.clone(),
        "my_data_permutation",
        train_config,
    ).await?;
    
    // Training loop
    println!("Training:");
    while let Some(batch) = train_loader.load_batch().await? {
        println!("Training batch with {} rows", batch.num_rows());
        // Train model with batch
    }
    
    // Create validation loader using same permutation
    let val_config = DataLoaderConfig {
        batch_size: 64,
        columns: Some(vec!["features".to_string(), "label".to_string()]),
        split_id: Some(1), // Second split (validation)
        max_batches: None,
    };
    
    let mut val_loader = RandomDataLoaderFactory::create_from_existing_permutation(
        "my_data_permutation",
        table.inner.clone(),
        db.clone(),
        val_config,
    ).await?;
    
    // Validation loop
    println!("Validation:");
    while let Some(batch) = val_loader.load_batch().await? {
        println!("Validation batch with {} rows", batch.num_rows());
        // Validate model with batch
    }
    
    Ok(())
}

// Example 2: Stream interface for async processing
async fn example_streaming() -> Result<()> {
    let db = connect("./my_dataset").execute().await?;
    let table = db.open_table("my_data").execute().await?;
    
    let config = PermutationConfig {
        base_table_uri: table.dataset_uri().to_string(),
        seed: Some(123),
        splits: SplitConfig::Percentages(vec![1.0]), // Use all data
        split_strategy: SplitStrategy::Random,
        shard_size: None, // Row-level access
        max_rows: Some(10000), // Limit to first 10k rows
    };
    
    let loader_config = DataLoaderConfig {
        batch_size: 64,
        columns: None, // Load all columns
        split_id: Some(0),
        max_batches: Some(100), // Limit to 100 batches
    };
    
    let loader = RandomDataLoaderFactory::create_with_new_permutation(
        table.inner.clone(),
        db.clone(),
        config,
        "streaming_permutation",
        loader_config,
    ).await?;
    
    // Use as stream
    let mut stream = loader.into_stream();
    while let Some(batch) = stream.try_next().await? {
        println!("Streamed batch with {} rows", batch.num_rows());
        // Process batch
    }
    
    Ok(())
}

// Example 3: Multiple epochs with reset
async fn example_multi_epoch() -> Result<()> {
    let db = connect("./my_dataset").execute().await?;
    let table = db.open_table("my_data").execute().await?;
    
    let config = PermutationConfig {
        base_table_uri: table.dataset_uri().to_string(),
        seed: Some(456),
        splits: SplitConfig::Percentages(vec![1.0]),
        split_strategy: SplitStrategy::Random,
        shard_size: Some(512),
        max_rows: None,
    };
    
    let loader_config = DataLoaderConfig {
        batch_size: 32,
        columns: Some(vec!["input".to_string(), "target".to_string()]),
        split_id: Some(0),
        max_batches: None,
    };
    
    let mut loader = RandomDataLoaderFactory::create_with_new_permutation(
        table.inner.clone(),
        db.clone(),
        config,
        "multi_epoch_permutation",
        loader_config,
    ).await?;
    
    // Multiple epochs
    for epoch in 0..5 {
        println!("Epoch {}", epoch);
        
        while let Some(batch) = loader.load_batch().await? {
            println!("  Batch with {} rows", batch.num_rows());
            // Train with batch
        }
        
        // Reset for next epoch
        loader.reset();
    }
    
    Ok(())
}

// Example 4: Hash-based splitting for stratified sampling
async fn example_hash_splitting() -> Result<()> {
    let db = connect("./my_dataset").execute().await?;
    let table = db.open_table("user_data").execute().await?;
    
    // Split by user_id hash to ensure users don't span train/test
    let config = PermutationConfig {
        base_table_uri: table.dataset_uri().to_string(),
        seed: Some(789),
        splits: SplitConfig::Percentages(vec![0.7, 0.15, 0.15]), // train/val/test
        split_strategy: SplitStrategy::Hash { 
            columns: vec!["user_id".to_string()] 
        },
        shard_size: Some(256),
        max_rows: None,
    };
    
    let train_config = DataLoaderConfig {
        batch_size: 64,
        columns: Some(vec!["features".to_string(), "label".to_string()]),
        split_id: Some(0), // Training split
        max_batches: None,
    };
    
    let mut train_loader = RandomDataLoaderFactory::create_with_new_permutation(
        table.inner.clone(),
        db.clone(),
        config,
        "user_stratified_permutation",
        train_config,
    ).await?;
    
    // Get split information
    let split_info = train_loader.get_split_info().await?;
    for info in split_info {
        println!("Split {}: {} entries, {} rows", 
                 info.split_id, info.num_entries, info.num_rows);
    }
    
    Ok(())
}
```

### Error Handling and Edge Cases

```rust
impl RandomDataLoader {
    /// Validate configuration and handle edge cases
    fn validate_config(&self) -> Result<()> {
        if self.config.batch_size == 0 {
            return Err(Error::InvalidInput {
                message: "Batch size must be greater than 0".to_string(),
            });
        }
        
        if let Some(max_batches) = self.config.max_batches {
            if max_batches == 0 {
                return Err(Error::InvalidInput {
                    message: "max_batches must be greater than 0 if specified".to_string(),
                });
            }
        }
        
        // Validate split_id exists
        if let Some(split_id) = self.config.split_id {
            // This could be validated by querying the permutation table
            // for available split IDs
        }
        
        Ok(())
    }
    
    /// Handle empty permutation table gracefully
    async fn handle_empty_permutation(&self) -> Result<bool> {
        let count = self.permutation_table.count_rows(None).await?;
        Ok(count == 0)
    }
}
```

## Performance Considerations

### Advantages with `take` Method
- **Direct Access**: No SQL parsing or query planning overhead
- **Single Call per Batch**: Reduced round-trips vs multiple row ID queries
- **Optimized Column Selection**: Built into the method vs post-processing
- **Better Caching**: Dataset can optimize for offset-based access patterns

### Performance Characteristics
- **Memory Usage**: O(batch_size) for each batch
- **Time Complexity**: O(batch_size) per batch for random access
- **Network Overhead**: Minimal with single method calls

### Optimizations Enabled by `take`
- Dataset-level optimizations for offset access
- Potential for parallel loading of multiple offset ranges
- More efficient column pruning at the storage layer

## Implementation Requirements for `BaseTable::take`

The `take` method should be implemented in all `BaseTable` implementations:

1. **Native Tables**: Use Lance dataset's row offset capabilities
2. **Remote Tables**: Translate to appropriate API calls for remote access
3. **Error Handling**: Validate indices and handle out-of-bounds gracefully
4. **Column Selection**: Efficiently prune columns at the storage layer

## Testing Strategy

1. **Unit Tests**: Test sampling algorithms with deterministic seeds
2. **Integration Tests**: Test with various table configurations and data types
3. **Performance Benchmarks**: Compare against sequential loading
4. **Correctness Tests**: Verify randomness properties and distribution
5. **Edge Cases**: Empty tables, single-row tables, large datasets

## Future Enhancements

1. **Weighted Sampling**: Support probability-based sampling
2. **Stratified Sampling**: Sample proportionally from different groups
3. **Checkpointing**: Save/restore loader state for resumable training
4. **Multi-table Support**: Load from multiple tables simultaneously
5. **Parallel Loading**: Concurrent batch loading for improved throughput

## Benefits of This Approach

1. **Simplified Implementation**: Random data loader becomes much simpler
2. **Better Performance**: Direct offset access vs complex query filtering
3. **Reusable Method**: `take` method useful for other use cases beyond random loading
4. **Consistent API**: Follows Arrow conventions (similar to `take` in Arrow compute)
5. **Future-Proof**: Enables more advanced sampling strategies