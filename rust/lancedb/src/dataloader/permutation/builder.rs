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
///
/// # Determinism
///
/// Clients that build a permutation independently — the ranks of a distributed training job, say —
/// have to arrive at the same one.  If they do not, their split boundaries disagree and they
/// silently train on overlapping or missing data.
///
/// Splitting and shuffling both work on the position of a row in the scan, and no backend
/// guarantees that two scans return rows in the same order: a server is free to fan a scan out
/// across nodes and return the rows in whatever order they arrive.  The builder therefore sorts by
/// row id before anything positional runs, so the permutation depends only on the rows themselves.
///
/// Two things remain the caller's responsibility.  Seed the shuffle and any random split
/// explicitly, or each client draws its own seed.  And use the same
/// `LANCEDB_PERM_BUILDER_MEMORY_LIMIT` everywhere, because regrouping a shuffled permutation by
/// split id is not a stable sort and where it spills decides how rows within a split are ordered.
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

    /// Sorts the stream by the given columns, spilling to disk when it does not fit in memory.
    async fn sort_by(
        &self,
        data: SendableRecordBatchStream,
        columns: &[&str],
    ) -> Result<SendableRecordBatchStream> {
        let sort_key = columns.join(", ");
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
                message: format!("Failed to setup sort by {}: {}", sort_key, e),
                source: Some(e.into()),
            })?;
        let df_stream = df
            .sort_by(columns.iter().map(|column| col(*column)).collect())
            .map_err(|e| Error::Other {
                message: format!("Failed to plan sort by {}: {}", sort_key, e),
                source: Some(e.into()),
            })?
            .execute_stream()
            .await
            .map_err(|e| Error::Other {
                message: format!("Failed to sort by {}: {}", sort_key, e),
                source: Some(e.into()),
            })?;

        let schema = df_stream.schema();
        let stream = df_stream.map_err(move |e| Error::Other {
            message: format!("Failed to execute sort by {}: {}", sort_key, e),
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

    /// Builds the permutation table and stores it in the given database.
    pub async fn build(self) -> Result<Table> {
        // First pass, apply filter and load row ids
        let mut rows = self.base_table.query().select(Select::columns(&[ROW_ID]));

        if let Some(filter) = &self.config.filter {
            rows = rows.only_if(filter);
        }

        let splitter = Splitter::new(
            self.config.temp_dir.clone(),
            self.config.split_strategy.clone(),
        );

        // Might need to load additional columns to calculate splits (e.g. hash columns or calculated
        // split id)
        rows = splitter.project(rows);

        let num_rows = self
            .base_table
            .count_rows(self.config.filter.clone())
            .await? as u64;

        let scanned = rows.execute().await?;

        // Apply splits.  Scans carry no ordering guarantee, so the row order has to be
        // canonicalized before anything positional runs against it.
        let split_data = if splitter.assigns_split_by_position() {
            let canonical = self.sort_by(scanned, &[ROW_ID]).await?;
            splitter.apply(canonical, num_rows).await?
        } else {
            // The split id does not depend on position here, so canonicalize afterwards, where
            // the stream has narrowed to the row id and the split id.  Leading with the split id
            // also groups the output, which the positional strategies get from `apply` itself.
            let split_data = splitter.apply(scanned, num_rows).await?;
            self.sort_by(split_data, &[SPLIT_ID_COLUMN, ROW_ID]).await?
        };

        // Shuffle data if requested.  A shuffle is global, so it breaks the grouping by split id
        // that the split established and the permutation has to be regrouped afterwards.
        let permutation = match self.config.shuffle_strategy {
            ShuffleStrategy::None => split_data,
            ShuffleStrategy::Random { seed, clump_size } => {
                let shuffler = Shuffler::new(ShufflerConfig {
                    seed,
                    clump_size,
                    temp_dir: self.config.temp_dir.clone(),
                    max_rows_per_file: 10 * 1024 * 1024,
                });
                let shuffled = shuffler.shuffle(split_data, num_rows).await?;
                self.sort_by(shuffled, &[SPLIT_ID_COLUMN]).await?
            }
        };

        // Rename _rowid to row_id
        let renamed = rename_column(permutation, ROW_ID, SRC_ROW_ID_COL)?;

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

    /// A scan carries no ordering guarantee, and a server is free to fan one out across nodes and
    /// return the rows in whatever order they arrive.  Every client still has to build the same
    /// permutation, so the builder canonicalizes by row id.  These tests drive the builder against
    /// a mock server that serves the same rows in two different orders.
    #[cfg(feature = "remote")]
    mod scan_order {
        use arrow::array::AsArray;
        use arrow::datatypes::UInt64Type;
        use arrow_array::{Int32Array, RecordBatch, UInt64Array};
        use arrow_schema::{DataType, Field, Schema};
        use futures::TryStreamExt;
        use http::header::CONTENT_TYPE;

        use super::*;

        const ARROW_FILE_CONTENT_TYPE: &str = "application/vnd.apache.arrow.file";
        const HASH_COLUMN: &str = "hash_col";
        const ROW_COUNT: u64 = 40;

        fn ascending_scan() -> Vec<u64> {
            (0..ROW_COUNT).collect()
        }

        /// The same rows a fan-out scan might return: whole blocks arriving out of order.
        fn out_of_order_scan() -> Vec<u64> {
            let mut row_ids = Vec::with_capacity(ROW_COUNT as usize);
            for start in [20, 0, 30, 10] {
                row_ids.extend(start..start + 10);
            }
            row_ids
        }

        fn row_id_batch(row_ids: Vec<u64>) -> RecordBatch {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    ROW_ID,
                    DataType::UInt64,
                    false,
                )])),
                vec![Arc::new(UInt64Array::from(row_ids))],
            )
            .unwrap()
        }

        /// The hash strategy projects its columns with the row id last.
        fn hash_batch(row_ids: Vec<u64>) -> RecordBatch {
            let hashes = Int32Array::from(row_ids.iter().map(|id| *id as i32).collect::<Vec<_>>());
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new(HASH_COLUMN, DataType::Int32, false),
                    Field::new(ROW_ID, DataType::UInt64, false),
                ])),
                vec![Arc::new(hashes), Arc::new(UInt64Array::from(row_ids))],
            )
            .unwrap()
        }

        fn write_ipc_file(batch: &RecordBatch) -> Vec<u8> {
            let mut body = Vec::new();
            {
                let mut writer =
                    arrow_ipc::writer::FileWriter::try_new(&mut body, &batch.schema()).unwrap();
                writer.write(batch).unwrap();
                writer.finish().unwrap();
            }
            body
        }

        /// A remote table whose scan returns exactly `batch`, in exactly that order.
        fn table_scanning(batch: RecordBatch) -> Table {
            let num_rows = batch.num_rows();
            let body = write_ipc_file(&batch);
            Table::new_with_handler("scan_order", move |request| match request.url().path() {
                "/v1/table/scan_order/query/" => http::Response::builder()
                    .status(200)
                    .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                    .body(body.clone())
                    .unwrap(),
                "/v1/table/scan_order/count_rows/" => http::Response::builder()
                    .status(200)
                    .body(num_rows.to_string().into_bytes())
                    .unwrap(),
                path => panic!("Unexpected request path: {}", path),
            })
        }

        /// The permutation's (row id, split id) pairs, in permutation order.
        async fn permutation_of(
            batch: RecordBatch,
            split_strategy: SplitStrategy,
            shuffle_strategy: ShuffleStrategy,
        ) -> Vec<(u64, u64)> {
            let permutation = PermutationBuilder::new(table_scanning(batch))
                .with_split_strategy(split_strategy, None)
                .with_shuffle_strategy(shuffle_strategy)
                .build()
                .await
                .unwrap();

            let batches = permutation
                .query()
                .select(Select::columns(&[SRC_ROW_ID_COL, SPLIT_ID_COLUMN]))
                .execute()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();

            let mut rows = Vec::new();
            for batch in batches {
                let row_ids = batch
                    .column_by_name(SRC_ROW_ID_COL)
                    .unwrap()
                    .as_primitive::<UInt64Type>();
                let split_ids = batch
                    .column_by_name(SPLIT_ID_COLUMN)
                    .unwrap()
                    .as_primitive::<UInt64Type>();
                for idx in 0..batch.num_rows() {
                    rows.push((row_ids.value(idx), split_ids.value(idx)));
                }
            }
            rows
        }

        fn sequential_halves() -> SplitStrategy {
            SplitStrategy::Sequential {
                sizes: SplitSizes::Counts(vec![10, 10]),
            }
        }

        #[tokio::test]
        async fn test_positional_split_ignores_scan_order() {
            // Two splits of 10 out of 40 rows, so the scan order decides both which rows land in
            // which split and which 20 rows are dropped entirely.
            let expected = (0..20u64)
                .map(|row_id| (row_id, row_id / 10))
                .collect::<Vec<_>>();

            let in_order = permutation_of(
                row_id_batch(ascending_scan()),
                sequential_halves(),
                ShuffleStrategy::None,
            )
            .await;
            let out_of_order = permutation_of(
                row_id_batch(out_of_order_scan()),
                sequential_halves(),
                ShuffleStrategy::None,
            )
            .await;

            assert_eq!(in_order, expected);
            assert_eq!(out_of_order, expected);
        }

        #[tokio::test]
        async fn test_value_based_split_ignores_scan_order() {
            let hash_halves = || SplitStrategy::Hash {
                columns: vec![HASH_COLUMN.to_string()],
                split_weights: vec![1, 1],
                discard_weight: 0,
            };

            let in_order = permutation_of(
                hash_batch(ascending_scan()),
                hash_halves(),
                ShuffleStrategy::None,
            )
            .await;
            let out_of_order = permutation_of(
                hash_batch(out_of_order_scan()),
                hash_halves(),
                ShuffleStrategy::None,
            )
            .await;

            assert_eq!(in_order, out_of_order);
            // Hashing a row's own values is order-independent, so what the scan order leaked into
            // here was the permutation's order, which is what the reader slices by offset.  Rows
            // come out grouped by split id and ascending by row id within a split.
            assert!(
                in_order
                    .windows(2)
                    .all(|pair| (pair[0].1, pair[0].0) < (pair[1].1, pair[1].0)),
                "not grouped by split id and ordered by row id: {:?}",
                in_order
            );
            assert_eq!(in_order.len(), ROW_COUNT as usize);
            assert!(in_order.iter().any(|(_, split_id)| *split_id == 0));
            assert!(in_order.iter().any(|(_, split_id)| *split_id == 1));
        }

        #[tokio::test]
        async fn test_shuffled_permutation_ignores_scan_order() {
            let shuffle = || ShuffleStrategy::Random {
                seed: Some(7),
                clump_size: None,
            };

            let in_order = permutation_of(
                row_id_batch(ascending_scan()),
                sequential_halves(),
                shuffle(),
            )
            .await;
            let out_of_order = permutation_of(
                row_id_batch(out_of_order_scan()),
                sequential_halves(),
                shuffle(),
            )
            .await;

            assert_eq!(in_order, out_of_order);

            let mut members = in_order
                .iter()
                .map(|(row_id, _)| *row_id)
                .collect::<Vec<_>>();
            members.sort_unstable();
            assert_eq!(members, (0..20u64).collect::<Vec<_>>());

            // Regrouped by split id, but still shuffled within each split.
            assert!(
                in_order.windows(2).all(|pair| pair[0].1 <= pair[1].1),
                "not grouped by split id: {:?}",
                in_order
            );
            assert!(
                in_order
                    .windows(2)
                    .any(|pair| pair[0].1 == pair[1].1 && pair[0].0 > pair[1].0),
                "shuffle left the rows in row id order: {:?}",
                in_order
            );
        }
    }
}
