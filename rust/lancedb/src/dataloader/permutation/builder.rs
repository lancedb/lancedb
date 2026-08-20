// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{collections::HashMap, sync::Arc};

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_execution::{disk_manager::DiskManagerBuilder, runtime_env::RuntimeEnvBuilder};
use datafusion_expr::col;
use futures::TryStreamExt;
use lance_core::ROW_ID;
use lance_datafusion::exec::SessionContextExt;

use crate::{
    Error, Result, Table,
    arrow::{SendableRecordBatchStream, SendableRecordBatchStreamExt, SimpleRecordBatchStream},
    connect,
    database::{CreateTableRequest, Database},
    dataloader::permutation::{
        shuffle::{Shuffler, ShufflerConfig},
        split::{SPLIT_ID_COLUMN, SplitStrategy, Splitter},
        util::{TemporaryDirectory, rename_column},
    },
    query::{ExecutableQuery, QueryBase, Select},
    table::Filter,
};

pub const SRC_ROW_ID_COL: &str = "row_id";

pub const SPLIT_NAMES_CONFIG_KEY: &str = "split_names";

pub const DEFAULT_MEMORY_LIMIT: usize = 100 * 1024 * 1024;

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
    /// Optional names for the splits
    split_names: Option<Vec<String>>,
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
#[derive(Debug, Clone, Default)]
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
    #[default]
    None,
}

/// Builder for creating a permutation table.
///
/// A permutation table is a table that stores split assignments and a shuffled order of rows.  This
/// can be used to create a permutation reader that reads rows in the order defined by the permutation.
///
/// The permutation table is not a materialized copy of the underlying data and can be very lightweight.
/// It is not a view of the underlying data and is not a copy of the data.  It is a separate table that
/// stores just row id and split id.
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
    ///
    /// An optional list of names can be provided for the splits.  This is for convenience and the names
    /// will be stored in the permutation table's config metadata.
    pub fn with_split_strategy(
        mut self,
        split_strategy: SplitStrategy,
        split_names: Option<Vec<String>>,
    ) -> Self {
        self.config.split_strategy = split_strategy;
        self.config.split_names = split_names;
        self
    }

    /// Configures the strategy for shuffling the data.
    ///
    /// The default is to shuffle the data randomly at row-level granularity (no clump size) and
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
        let memory_limit = std::env::var("LANCEDB_PERM_BUILDER_MEMORY_LIMIT")
            .unwrap_or_else(|_| DEFAULT_MEMORY_LIMIT.to_string())
            .parse::<usize>()
            .unwrap_or_else(|_| {
                log::error!(
                    "Failed to parse LANCEDB_PERM_BUILDER_MEMORY_LIMIT, using default: {}",
                    DEFAULT_MEMORY_LIMIT
                );
                DEFAULT_MEMORY_LIMIT
            });
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::default(),
            RuntimeEnvBuilder::new()
                .with_memory_limit(memory_limit, 1.0)
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

    fn add_split_names(
        data: SendableRecordBatchStream,
        split_names: &[String],
    ) -> Result<SendableRecordBatchStream> {
        let schema = data
            .schema()
            .as_ref()
            .clone()
            .with_metadata(HashMap::from([(
                SPLIT_NAMES_CONFIG_KEY.to_string(),
                serde_json::to_string(split_names).map_err(|e| Error::Other {
                    message: format!("Failed to serialize split names: {}", e),
                    source: Some(e.into()),
                })?,
            )]));
        let schema = Arc::new(schema);
        let schema_clone = schema.clone();
        let stream = data.map_ok(move |batch| batch.with_schema(schema.clone()).unwrap());
        Ok(Box::pin(SimpleRecordBatchStream {
            schema: schema_clone,
            stream,
        }))
    }

    /// Check that the row-id scan came back with exactly the columns we asked for.
    ///
    /// The scan requests the row id with `with_row_id` and an *empty* projection,
    /// which is a different wire value from "no projection given". A server that read
    /// the empty list as "all columns" would answer with the whole table, and nothing
    /// downstream would notice: the split id is appended to whatever arrives and
    /// `_rowid` is still found by name, so the permutation would silently materialize
    /// every base column — and `Hash` would silently hash all of them rather than the
    /// configured ones. Fail here instead.
    fn validate_scan_schema(schema: &arrow_schema::Schema, splitter: &Splitter) -> Result<()> {
        /// Sorted but not deduplicated, so the comparison and the error message agree.
        fn normalize(mut names: Vec<String>) -> Vec<String> {
            names.sort_unstable();
            names
        }

        // Sorted rather than positional, because `apply_hash` finds the row id by name
        // precisely since where a backing store puts it is not ours to dictate.
        // Multiplicity is still compared: `apply_hash` hashes every non-`_rowid` array,
        // so an extra copy of a hash column would silently change split assignment.
        let mut expected = splitter.projected_columns();
        expected.push(ROW_ID.to_string());
        let expected = normalize(expected);
        let actual = normalize(schema.fields().iter().map(|f| f.name().clone()).collect());

        if actual != expected {
            return Err(Error::InvalidInput {
                message: format!(
                    "Permutation row id scan returned columns {:?}, expected {:?}.  \
                     The table's backing store did not honor the requested projection.",
                    actual, expected
                ),
            });
        }
        Ok(())
    }

    /// Builds the permutation table and stores it in the given database.
    pub async fn build(self) -> Result<Table> {
        // First pass, apply filter and load row ids.  The row id is requested with the
        // `with_row_id` flag rather than by projecting `_rowid`: that is the shape the
        // LanceDB server accepts for remote tables, and it appends the row id after any
        // columns the splitter projects, which `Splitter` relies on.
        let mut rows = self
            .base_table
            .query()
            .select(Select::Columns(Vec::new()))
            .with_row_id()
            // A permutation addresses rows by stable `_rowid`, which the MemWAL LSM
            // scanner does not expose, so the permutation always reads the base table.
            .use_lsm(false);

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

        // Base-only, to agree with the base-only scan above.  On a remote MemWAL
        // table a plain `count_rows` is rejected outright rather than merely
        // disagreeing.
        let num_rows = self
            .base_table
            .base_table()
            .count_base_rows(self.config.filter.clone().map(Filter::Sql))
            .await? as u64;

        // Apply splits
        let rows = rows.execute().await?;
        Self::validate_scan_schema(rows.schema().as_ref(), &splitter)?;
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

        let streaming_data = if let Some(split_names) = &self.config.split_names {
            Self::add_split_names(renamed, split_names)?
        } else {
            renamed
        };

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
            CreateTableRequest::new(name.to_string(), Box::new(streaming_data));

        let table = database.create_table(create_table_request).await?;
        let table = Table::new(table, database);

        // The splits were sized from `count_rows`, but nothing so far has checked that
        // the scan actually produced that many rows.  A short scan — a server-side
        // result cap, a truncated stream, a concurrent delete between the count and the
        // scan — otherwise just under-fills the trailing splits: `apply_sequential`
        // assigns ids as data flows and `Shuffler` only objects to seeing *more* rows
        // than expected, so training would quietly run on less data than was asked for.
        if let Some(expected) = splitter.expected_row_count(num_rows) {
            let actual = table.count_rows(None).await? as u64;
            if actual != expected {
                return Err(Error::InvalidInput {
                    message: format!(
                        "Permutation has {} rows, expected {}.  The row id scan returned \
                         fewer rows than the table reported; the table may have changed \
                         while the permutation was being built.",
                        actual, expected
                    ),
                });
            }
        }

        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::AsArray;
    use arrow::datatypes::{Int32Type, UInt64Type};
    use futures::TryStreamExt;
    use lance_datagen::{BatchCount, RowCount};
    use std::collections::HashSet;

    use crate::{
        arrow::LanceDbDatagenExt,
        connect,
        dataloader::permutation::split::SplitSizes,
        query::{ExecutableQuery, QueryBase, Select},
    };

    use super::*;

    /// Collect `(row_id, split_id)` pairs out of a permutation table.
    async fn permutation_pairs(permutation_table: &Table) -> Vec<(u64, u64)> {
        let batches = permutation_table
            .query()
            .select(Select::Columns(vec![
                SRC_ROW_ID_COL.to_string(),
                SPLIT_ID_COLUMN.to_string(),
            ]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let mut pairs = Vec::new();
        for batch in batches {
            // `Calculated` splits carry whatever type the SQL produced, so normalize.
            let to_u64 = |idx: usize| {
                arrow::compute::cast(batch.column(idx), &arrow_schema::DataType::UInt64).unwrap()
            };
            let row_ids = to_u64(0);
            let split_ids = to_u64(1);
            let row_ids = row_ids.as_primitive::<UInt64Type>();
            let split_ids = split_ids.as_primitive::<UInt64Type>();
            for i in 0..batch.num_rows() {
                pairs.push((row_ids.value(i), split_ids.value(i)));
            }
        }
        pairs
    }

    /// A 100-row table whose `col_a` steps 0..100, for the split-strategy tests below.
    async fn stepped_table(name: &str) -> (tempfile::TempDir, Table) {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = connect(temp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let data = lance_datagen::gen_batch()
            .col("col_a", lance_datagen::array::step::<Int32Type>())
            .into_ldb_stream(RowCount::from(10), BatchCount::from(10));
        let table = db.create_table(name, data).execute().await.unwrap();
        (temp_dir, table)
    }

    #[tokio::test]
    async fn test_permutation_table_only_stores_row_id_and_split_id() {
        let temp_dir = tempfile::tempdir().unwrap();

        let db = connect(temp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();

        let initial_data = lance_datagen::gen_batch()
            .col("col_a", lance_datagen::array::step::<Int32Type>())
            .col("col_b", lance_datagen::array::step::<Int32Type>())
            .into_ldb_stream(RowCount::from(100), BatchCount::from(10));
        let data_table = db
            .create_table("base_tbl", initial_data)
            .execute()
            .await
            .unwrap();

        let permutation_table = PermutationBuilder::new(data_table.clone())
            .with_split_strategy(
                SplitStrategy::Sequential {
                    sizes: SplitSizes::Percentages(vec![0.5, 0.5]),
                },
                None,
            )
            .with_filter("col_a > 57".to_string())
            .build()
            .await
            .unwrap();

        let schema = permutation_table.schema().await.unwrap();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            field_names,
            vec!["row_id", "split_id"],
            "Permutation table should only contain row_id and split_id columns, but found: {:?}",
            field_names,
        );
    }

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
            .create_table("mytbl", initial_data)
            .execute()
            .await
            .unwrap();

        let permutation_table = PermutationBuilder::new(data_table.clone())
            .with_filter("some_value > 57".to_string())
            .with_split_strategy(
                SplitStrategy::Random {
                    seed: Some(42),
                    sizes: SplitSizes::Percentages(vec![0.05, 0.30]),
                    clump_size: None,
                },
                None,
            )
            .build()
            .await
            .unwrap();

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

    /// The row id reaches the splitter via `with_row_id` rather than a projected
    /// `_rowid` column.  `Hash` is one of the two strategies that projects columns of
    /// its own, and it hashes "every column except the last", so this pins that the row
    /// id still arrives last and is not itself fed into the hash.
    #[tokio::test]
    async fn test_permutation_builder_hash_split() {
        let (_temp_dir, data_table) = stepped_table("hash_tbl").await;

        let permutation_table = PermutationBuilder::new(data_table.clone())
            .with_split_strategy(
                SplitStrategy::Hash {
                    columns: vec!["col_a".to_string()],
                    split_weights: vec![1, 1],
                    discard_weight: 0,
                },
                None,
            )
            .build()
            .await
            .unwrap();

        let pairs = permutation_pairs(&permutation_table).await;
        assert_eq!(pairs.len(), 100);

        // Every base row appears exactly once. If the row id had been hashed along with
        // `col_a`, or read from the wrong column, this is what would break.
        let row_ids: HashSet<u64> = pairs.iter().map(|(row_id, _)| *row_id).collect();
        assert_eq!(row_ids, (0..100).collect::<HashSet<u64>>());
        assert!(pairs.iter().all(|(_, split_id)| *split_id < 2));
        // Both splits should get rows; a constant split id would mean the hash saw a
        // constant input.
        assert!(pairs.iter().any(|(_, split_id)| *split_id == 0));
        assert!(pairs.iter().any(|(_, split_id)| *split_id == 1));
    }

    /// The other strategy with a projection of its own: `Calculated` computes the split
    /// id in SQL, and the row id rides alongside it via `with_row_id`.
    #[tokio::test]
    async fn test_permutation_builder_calculated_split() {
        let (_temp_dir, data_table) = stepped_table("calculated_tbl").await;

        let permutation_table = PermutationBuilder::new(data_table.clone())
            .with_split_strategy(
                SplitStrategy::Calculated {
                    calculation: "col_a % 2".to_string(),
                },
                None,
            )
            .build()
            .await
            .unwrap();

        let mut pairs = permutation_pairs(&permutation_table).await;
        pairs.sort_unstable();
        assert_eq!(pairs.len(), 100);

        // `col_a` steps 0..100 and row ids follow storage order, so the split id of row
        // `n` must be `n % 2` — an exact check that the row id was paired with the split
        // id computed from *its own* row.
        let expected: Vec<(u64, u64)> = (0..100).map(|row_id| (row_id, row_id % 2)).collect();
        assert_eq!(pairs, expected);
    }
}
