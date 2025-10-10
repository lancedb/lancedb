// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_execution::{disk_manager::DiskManagerBuilder, runtime_env::RuntimeEnvBuilder};
use datafusion_expr::col;
use futures::TryStreamExt;
use lance_core::ROW_ID;
use lance_datafusion::exec::SessionContextExt;

use crate::{
    arrow::{SendableRecordBatchStream, SendableRecordBatchStreamExt, SimpleRecordBatchStream},
    connect,
    database::{CreateTableData, CreateTableRequest, Database},
    dataloader::permutation::{
        shuffle::{Shuffler, ShufflerConfig},
        split::{SplitStrategy, Splitter, SPLIT_ID_COLUMN},
        util::{rename_column, TemporaryDirectory},
    },
    query::{ExecutableQuery, QueryBase},
    Error, Result, Table,
};

pub const SRC_ROW_ID_COL: &str = "row_id";

/// Where to store the permutation table
#[derive(Debug, Clone, Default)]
enum PermutationDestination {
    /// The permutation table is a temporary table in memory
    #[default]
    Temporary,
    /// The permutation table is a permanent table in a database
    Permanent(Arc<dyn Database>, String),
}

/// Configuration for creating a permutation table
#[derive(Debug, Default)]
pub struct PermutationConfig {
    /// Splitting configuration
    split_strategy: SplitStrategy,
    /// Shuffle strategy
    shuffle_strategy: ShuffleStrategy,
    /// Optional filter to apply to the base table
    filter: Option<String>,
    /// Directory to use for temporary files
    temp_dir: TemporaryDirectory,
    /// Destination
    destination: PermutationDestination,
}

/// Strategy for shuffling the data.
#[derive(Debug, Clone)]
pub enum ShuffleStrategy {
    /// The data is randomly shuffled
    ///
    /// A seed can be provided to make the shuffle deterministic.
    ///
    /// If a clump size is provided, then data will be shuffled in small blocks of contiguous rows.
    /// This decreases the overall randomization but can improve I/O performance when reading from
    /// cloud storage.
    ///
    /// For example, a clump size of 16 will means we will shuffle blocks of 16 contiguous rows.  This
    /// will mean 16x fewer IOPS but these 16 rows will always be close together and this can influence
    /// the performance of the model.  Note: shuffling within clumps can still be done at read time but
    /// this will only provide a local shuffle and not a global shuffle.
    Random {
        seed: Option<u64>,
        clump_size: Option<u64>,
    },
    /// The data is not shuffled
    ///
    /// This is useful for debugging and testing.
    None,
}

impl Default for ShuffleStrategy {
    fn default() -> Self {
        Self::None
    }
}

/// Builder for creating a permutation table.
///
/// A permutation table is a table that stores split assignments and a shuffled order of rows.  This
/// can be used to create a
pub struct PermutationBuilder {
    config: PermutationConfig,
    base_table: Table,
}

impl PermutationBuilder {
    pub fn new(base_table: Table) -> Self {
        Self {
            config: PermutationConfig::default(),
            base_table,
        }
    }

    /// Configures the strategy for assigning rows to splits.
    ///
    /// For example, it is common to create a test/train split of the data.  Splits can also be used
    /// to limit the number of rows.  For example, to only use 10% of the data in a permutation you can
    /// create a single split with 10% of the data.
    ///
    /// Splits are _not_ required for parallel processing.  A single split can be loaded in parallel across
    /// multiple processes and multiple nodes.
    ///
    /// The default is a single split that contains all rows.
    pub fn with_split_strategy(mut self, split_strategy: SplitStrategy) -> Self {
        self.config.split_strategy = split_strategy;
        self
    }

    /// Configures the strategy for shuffling the data.
    ///
    /// The default is to shuffle the data randomly at row-level granularity (no shard size) and
    /// with a random seed.
    pub fn with_shuffle_strategy(mut self, shuffle_strategy: ShuffleStrategy) -> Self {
        self.config.shuffle_strategy = shuffle_strategy;
        self
    }

    /// Configures a filter to apply to the base table.
    ///
    /// Only rows matching the filter will be included in the permutation.
    pub fn with_filter(mut self, filter: String) -> Self {
        self.config.filter = Some(filter);
        self
    }

    /// Configures the directory to use for temporary files.
    ///
    /// The default is to use the operating system's default temporary directory.
    pub fn with_temp_dir(mut self, temp_dir: TemporaryDirectory) -> Self {
        self.config.temp_dir = temp_dir;
        self
    }

    /// Stores the permutation as a table in a database
    ///
    /// By default, the permutation is stored in memory.  If this method is called then
    /// the permutation will be stored as a table in the given database.
    pub fn persist(mut self, database: Arc<dyn Database>, table_name: String) -> Self {
        self.config.destination = PermutationDestination::Permanent(database, table_name);
        self
    }

    async fn sort_by_split_id(
        &self,
        data: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::default(),
            RuntimeEnvBuilder::new()
                .with_memory_limit(100 * 1024 * 1024, 1.0)
                .with_disk_manager_builder(
                    DiskManagerBuilder::default()
                        .with_mode(self.config.temp_dir.to_disk_manager_mode()),
                )
                .build_arc()
                .unwrap(),
        );
        let df = ctx
            .read_one_shot(data.into_df_stream())
            .map_err(|e| Error::Other {
                message: format!("Failed to setup sort by split id: {}", e),
                source: Some(e.into()),
            })?;
        let df_stream = df
            .sort_by(vec![col(SPLIT_ID_COLUMN)])
            .map_err(|e| Error::Other {
                message: format!("Failed to plan sort by split id: {}", e),
                source: Some(e.into()),
            })?
            .execute_stream()
            .await
            .map_err(|e| Error::Other {
                message: format!("Failed to sort by split id: {}", e),
                source: Some(e.into()),
            })?;

        let schema = df_stream.schema();
        let stream = df_stream.map_err(|e| Error::Other {
            message: format!("Failed to execute sort by split id: {}", e),
            source: Some(e.into()),
        });
        Ok(Box::pin(SimpleRecordBatchStream { schema, stream }))
    }

    /// Builds the permutation table and stores it in the given database.
    pub async fn build(self) -> Result<Table> {
        // First pass, apply filter and load row ids
        let mut rows = self.base_table.query().with_row_id();

        if let Some(filter) = &self.config.filter {
            rows = rows.only_if(filter);
        }

        let splitter = Splitter::new(
            self.config.temp_dir.clone(),
            self.config.split_strategy.clone(),
        );

        let mut needs_sort = !splitter.orders_by_split_id();

        // Might need to load additional columns to calculate splits (e.g. hash columns or calculated
        // split id)
        rows = splitter.project(rows);

        let num_rows = self
            .base_table
            .count_rows(self.config.filter.clone())
            .await? as u64;

        // Apply splits
        let rows = rows.execute().await?;
        let split_data = splitter.apply(rows, num_rows).await?;

        // Shuffle data if requested
        let shuffled = match self.config.shuffle_strategy {
            ShuffleStrategy::None => split_data,
            ShuffleStrategy::Random { seed, clump_size } => {
                let shuffler = Shuffler::new(ShufflerConfig {
                    seed,
                    clump_size,
                    temp_dir: self.config.temp_dir.clone(),
                    max_rows_per_file: 10 * 1024 * 1024,
                });
                shuffler.shuffle(split_data, num_rows).await?
            }
        };

        // We want the final permutation to be sorted by the split id.  If we shuffled or if
        // the split was not assigned sequentially then we need to sort the data.
        needs_sort |= !matches!(self.config.shuffle_strategy, ShuffleStrategy::None);

        let sorted = if needs_sort {
            self.sort_by_split_id(shuffled).await?
        } else {
            shuffled
        };

        // Rename _rowid to row_id
        let renamed = rename_column(sorted, ROW_ID, SRC_ROW_ID_COL)?;

        let (name, database) = match &self.config.destination {
            PermutationDestination::Permanent(database, table_name) => {
                (table_name.as_str(), database.clone())
            }
            PermutationDestination::Temporary => {
                let conn = connect("memory:///").execute().await?;
                ("permutation", conn.database().clone())
            }
        };

        let create_table_request =
            CreateTableRequest::new(name.to_string(), CreateTableData::StreamingData(renamed));

        let table = database.create_table(create_table_request).await?;
        Ok(Table::new(table, database))
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Int32Type;
    use lance_datagen::{BatchCount, RowCount};

    use crate::{arrow::LanceDbDatagenExt, connect, dataloader::permutation::split::SplitSizes};

    use super::*;

    #[tokio::test]
    async fn test_permutation_builder() {
        let temp_dir = tempfile::tempdir().unwrap();

        let db = connect(temp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();

        let initial_data = lance_datagen::gen_batch()
            .col("some_value", lance_datagen::array::step::<Int32Type>())
            .into_ldb_stream(RowCount::from(100), BatchCount::from(10));
        let data_table = db
            .create_table_streaming("mytbl", initial_data)
            .execute()
            .await
            .unwrap();

        let permutation_table = PermutationBuilder::new(data_table.clone())
            .with_filter("some_value > 57".to_string())
            .with_split_strategy(SplitStrategy::Random {
                seed: Some(42),
                sizes: SplitSizes::Percentages(vec![0.05, 0.30]),
            })
            .build()
            .await
            .unwrap();

        println!("permutation_table: {:?}", permutation_table);

        // Potentially brittle seed-dependent values below
        assert_eq!(permutation_table.count_rows(None).await.unwrap(), 330);
        assert_eq!(
            permutation_table
                .count_rows(Some("split_id = 0".to_string()))
                .await
                .unwrap(),
            47
        );
        assert_eq!(
            permutation_table
                .count_rows(Some("split_id = 1".to_string()))
                .await
                .unwrap(),
            283
        );
    }
}
