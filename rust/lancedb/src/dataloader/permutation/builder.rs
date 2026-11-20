// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{collections::HashMap, sync::Arc};

use arrow_array::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_execution::{disk_manager::DiskManagerBuilder, runtime_env::RuntimeEnvBuilder};
use datafusion_expr::col;
use futures::TryStreamExt;
use lance_core::{ROW_ADDR, ROW_ID};
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

/// Each row's address in the base table, `fragment_id << 32 | offset`, at
/// the version the permutation was built against. Stored only when the
/// builder is asked to; see [`PermutationBuilder::with_row_addresses`].
pub const SRC_ROW_ADDR_COL: &str = "row_addr";

pub const SPLIT_NAMES_CONFIG_KEY: &str = "split_names";

/// Base table version the permutation was built against.
pub const BASE_VERSION_CONFIG_KEY: &str = "base_version";

/// Base table branch the permutation was built against.  Absent means main.
pub const BASE_BRANCH_CONFIG_KEY: &str = "base_branch";

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
    /// Whether to store each row's address next to its row id
    row_addresses: bool,
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
/// stores just row id and split id, and each row's address when asked to.
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

    /// Also stores each row's address, in a [`SRC_ROW_ADDR_COL`] column.
    ///
    /// A row id says which row to read; a row address says where the row is
    /// stored. The two are the same number unless the table has stable row
    /// ids, where a row keeps its id when compaction moves it. A reader that
    /// must name storage locations, such as one that asks LanceDB Enterprise
    /// to warm the rows it will read next, needs the address.
    ///
    /// Addresses are only valid at the base table version the permutation
    /// records. They are carried through the split strategies that keep the
    /// scan's columns: no split, sequential and random. Any other strategy
    /// fails the build.
    pub fn with_row_addresses(mut self) -> Self {
        self.config.row_addresses = true;
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

    async fn sort_by_column(
        &self,
        data: SendableRecordBatchStream,
        column: &str,
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
                message: format!("Failed to setup sort by {}: {}", column, e),
                source: Some(e.into()),
            })?;
        let df_stream = df
            .sort_by(vec![col(column)])
            .map_err(|e| Error::Other {
                message: format!("Failed to plan sort by {}: {}", column, e),
                source: Some(e.into()),
            })?
            .execute_stream()
            .await
            .map_err(|e| Error::Other {
                message: format!("Failed to sort by {}: {}", column, e),
                source: Some(e.into()),
            })?;

        let column = column.to_string();
        let schema = df_stream.schema();
        let stream = df_stream.map_err(move |e| Error::Other {
            message: format!("Failed to execute sort by {}: {}", column, e),
            source: Some(e.into()),
        });
        Ok(Box::pin(SimpleRecordBatchStream { schema, stream }))
    }

    /// Replaces the stream's schema metadata with the permutation config.
    ///
    /// The scan of the base table can carry the base table's schema metadata (e.g. Hugging
    /// Face's `huggingface` key).  That metadata describes columns the permutation table does
    /// not have, and its keys could collide with the config keys, so it is dropped rather
    /// than merged.
    fn add_config_metadata(
        data: SendableRecordBatchStream,
        metadata: HashMap<String, String>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = data.schema().as_ref().clone().with_metadata(metadata);
        let schema = Arc::new(schema);
        let schema_clone = schema.clone();
        // `RecordBatch::with_schema` only accepts a superset of the batch's schema, which a
        // schema with replaced metadata is not, so rebuild the batch around the new schema.
        let stream = data.and_then(move |batch| {
            std::future::ready(
                RecordBatch::try_new(schema.clone(), batch.columns().to_vec()).map_err(Error::from),
            )
        });
        Ok(Box::pin(SimpleRecordBatchStream {
            schema: schema_clone,
            stream,
        }))
    }

    /// Builds the permutation table and stores it in the given database.
    pub async fn build(mut self) -> Result<Table> {
        // Remote tables resolve latest independently for each request, and a
        // native handle can refresh or answer a query from a namespace server.
        // Use a separate pinned handle so count, projection, and scan all refer
        // to one snapshot without changing the caller's table checkout state.
        if let Some(snapshot) = self
            .base_table
            .base_table()
            .snapshot_at_current_version()
            .await?
        {
            self.base_table = Table::from(snapshot);
        }

        // Unflushed rows have no row id, so a permutation cannot address them.
        match self.base_table.base_table().get_rte_spec().await {
            Ok(Some(_)) => {
                return Err(Error::NotSupported {
                    message: "the data loader does not support tables with an RTE write \
                              spec: rows that have not been flushed to the base table \
                              have no row id, so a permutation cannot reference them"
                        .to_string(),
                });
            }
            Ok(None) => {}
            // No RTE write path means no spec.
            Err(Error::NotSupported { .. }) => {}
            Err(err) => return Err(err),
        }

        // The handle above is already pinned to one version.  Record which one, so a
        // reader -- in a DataLoader worker, against a table that has since moved --
        // resolves these row addresses against the same snapshot.
        let base_version = self.base_table.version().await?;
        let base_branch = self.base_table.current_branch();

        // First pass, apply filter and load row ids.  `Shuffler` permutes positions, so
        // every rank must scan the rows in the same order to build the same permutation.
        let scan_columns: &[&str] = if self.config.row_addresses {
            // The other strategies rebuild each batch from the row id and the
            // split id they compute, which would drop the address.
            if !matches!(
                self.config.split_strategy,
                SplitStrategy::NoSplit
                    | SplitStrategy::Sequential { .. }
                    | SplitStrategy::Random { .. }
            ) {
                return Err(Error::NotSupported {
                    message: "row addresses can only be stored with no split, a sequential \
                              split or a random split"
                        .to_string(),
                });
            }
            &[ROW_ID, ROW_ADDR]
        } else {
            &[ROW_ID]
        };
        let mut rows = self
            .base_table
            .query()
            .select(Select::columns(scan_columns));

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
        // Splits are assigned by position, so the scan has to arrive in a fixed order.
        let rows = if self.base_table.base_table().scan_order_is_deterministic() {
            rows
        } else {
            self.sort_by_column(rows, ROW_ID).await?
        };
        // Bootstrap's output row count (num_samples) differs from its input row count
        // (num_rows); every other strategy's output matches its input. The shuffler
        // must be told the former, not assume it always equals the latter.
        let output_rows = splitter.output_row_count(num_rows);
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
                shuffler.shuffle(split_data, output_rows).await?
            }
        };

        // We want the final permutation to be sorted by the split id.  If we shuffled or if
        // the split was not assigned sequentially then we need to sort the data.
        needs_sort |= !matches!(self.config.shuffle_strategy, ShuffleStrategy::None);

        let sorted = if needs_sort {
            self.sort_by_column(shuffled, SPLIT_ID_COLUMN).await?
        } else {
            shuffled
        };

        // Rename _rowid to row_id
        let renamed = rename_column(sorted, ROW_ID, SRC_ROW_ID_COL)?;
        let renamed = if self.config.row_addresses {
            rename_column(renamed, ROW_ADDR, SRC_ROW_ADDR_COL)?
        } else {
            renamed
        };

        let mut metadata = HashMap::from([(
            BASE_VERSION_CONFIG_KEY.to_string(),
            base_version.to_string(),
        )]);
        // Version numbers are per-branch, so the branch is part of the coordinate.
        if let Some(branch) = &base_branch {
            metadata.insert(BASE_BRANCH_CONFIG_KEY.to_string(), branch.clone());
        }
        if let Some(split_names) = &self.config.split_names {
            metadata.insert(
                SPLIT_NAMES_CONFIG_KEY.to_string(),
                serde_json::to_string(split_names).map_err(|e| Error::Other {
                    message: format!("Failed to serialize split names: {}", e),
                    source: Some(e.into()),
                })?,
            );
        }
        let streaming_data = Self::add_config_metadata(renamed, metadata)?;

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

    /// Stands in for a namespace server whose table has advanced past the local
    /// dataset. A pushed-down query reads this newer row instead of the local
    /// snapshot.
    #[derive(Debug, Default)]
    struct ServerLatestNamespace {
        query_table_calls: std::sync::atomic::AtomicUsize,
        latest_row_id: u64,
    }

    #[async_trait::async_trait]
    impl lance_namespace::LanceNamespace for ServerLatestNamespace {
        fn namespace_id(&self) -> String {
            "server-latest".to_string()
        }

        async fn query_table(
            &self,
            _request: lance_namespace::models::QueryTableRequest,
        ) -> lance::Result<bytes::Bytes> {
            self.query_table_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            use arrow_array::UInt64Array;
            use arrow_schema::{DataType, Field, Schema};

            let schema = Arc::new(Schema::new(vec![Field::new(
                ROW_ID,
                DataType::UInt64,
                false,
            )]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(UInt64Array::from(vec![self.latest_row_id]))],
            )
            .unwrap();
            let mut body = Vec::new();
            {
                let mut writer =
                    arrow_ipc::writer::FileWriter::try_new(&mut body, &schema).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }
            Ok(bytes::Bytes::from(body))
        }
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

    /// On a table without stable row ids a row's address is its row id, so
    /// equal values in every row show the two columns stay paired through
    /// the split, the shuffle and the sort.
    #[tokio::test]
    async fn test_row_addresses_are_stored_beside_row_ids() {
        use arrow::datatypes::UInt64Type;
        use arrow_array::cast::AsArray;
        use futures::TryStreamExt;

        let temp_dir = tempfile::tempdir().unwrap();
        let db = connect(temp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let initial_data = lance_datagen::gen_batch()
            .col("col_a", lance_datagen::array::step::<Int32Type>())
            .into_ldb_stream(RowCount::from(100), BatchCount::from(10));
        let data_table = db
            .create_table("base_tbl", initial_data)
            .execute()
            .await
            .unwrap();

        let permutation_table = PermutationBuilder::new(data_table.clone())
            .with_split_strategy(
                SplitStrategy::Random {
                    seed: Some(7),
                    sizes: SplitSizes::Fixed(4),
                    clump_size: Some(5),
                },
                None,
            )
            .with_row_addresses()
            .build()
            .await
            .unwrap();

        let schema = permutation_table.schema().await.unwrap();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["row_id", "row_addr", "split_id"]);

        let batches: Vec<RecordBatch> = permutation_table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut rows = 0;
        for batch in &batches {
            let ids = batch["row_id"].as_primitive::<UInt64Type>();
            let addrs = batch["row_addr"].as_primitive::<UInt64Type>();
            assert_eq!(ids, addrs);
            rows += batch.num_rows();
        }
        assert_eq!(rows, 1000);

        // A strategy that rebuilds its batches cannot carry the address.
        let error = PermutationBuilder::new(data_table)
            .with_split_strategy(
                SplitStrategy::Calculated {
                    calculation: "col_a % 2".to_string(),
                },
                None,
            )
            .with_row_addresses()
            .build()
            .await
            .err()
            .unwrap();
        assert!(matches!(error, Error::NotSupported { .. }), "{error:?}");
    }

    #[tokio::test]
    async fn test_native_scan_order_is_deterministic() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = connect(temp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let data = lance_datagen::gen_batch()
            .col("col_a", lance_datagen::array::step::<Int32Type>())
            .into_ldb_stream(RowCount::from(10), BatchCount::from(1));
        let table = db.create_table("t", data).execute().await.unwrap();

        // Native tables skip the canonicalizing sort; remote does not.
        assert!(table.base_table().scan_order_is_deterministic());

        // A main/latest handle may answer from a namespace server, and its
        // QueryTable request carries no order. The local native scan guarantee
        // does not extend to that path.
        let mut pushed_down = table.as_native().unwrap().clone();
        pushed_down.namespace_client = Some(Arc::new(ServerLatestNamespace::default()));
        pushed_down
            .pushdown_operations
            .insert(crate::connection::NamespaceClientPushdownOperation::QueryTable);
        let pushed_down: Arc<dyn crate::table::BaseTable> = Arc::new(pushed_down);
        let pushed_down = Table::from(pushed_down);
        assert!(!pushed_down.base_table().scan_order_is_deterministic());
    }

    /// The builder must pin one native snapshot before it counts or scans. A
    /// namespace-backed latest handle can otherwise count local rows, scan
    /// server-latest rows, and record the local version as if they agreed.
    #[tokio::test]
    async fn test_native_permutation_builder_pins_namespace_pushdown() {
        use std::sync::atomic::Ordering;

        use arrow_array::{cast::AsArray, types::UInt64Type};

        let temp_dir = tempfile::tempdir().unwrap();
        let db = connect(temp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let data = lance_datagen::gen_batch()
            .col("col_a", lance_datagen::array::step::<Int32Type>())
            .into_ldb_stream(RowCount::from(10), BatchCount::from(1));
        let table = db.create_table("base_tbl", data).execute().await.unwrap();

        let namespace = Arc::new(ServerLatestNamespace {
            latest_row_id: 10,
            ..Default::default()
        });
        let mut native = table.as_native().unwrap().clone();
        native.namespace_client = Some(namespace.clone());
        native
            .pushdown_operations
            .insert(crate::connection::NamespaceClientPushdownOperation::QueryTable);
        let native: Arc<dyn crate::table::BaseTable> = Arc::new(native);
        let table = Table::from(native);

        let permutation = PermutationBuilder::new(table.clone())
            .build()
            .await
            .unwrap();

        assert_eq!(
            namespace.query_table_calls.load(Ordering::SeqCst),
            0,
            "the builder must read its pinned local snapshot, not server-latest rows"
        );
        assert_eq!(permutation.count_rows(None).await.unwrap(), 10);

        let batches: Vec<RecordBatch> = permutation
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let ids: Vec<u64> = batches
            .iter()
            .flat_map(|batch| {
                batch[SRC_ROW_ID_COL]
                    .as_primitive::<UInt64Type>()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, (0..10).collect::<Vec<_>>());

        let recorded = permutation
            .schema()
            .await
            .unwrap()
            .metadata
            .get(BASE_VERSION_CONFIG_KEY)
            .expect("permutation should record its base version")
            .parse::<u64>()
            .unwrap();
        assert_eq!(recorded, table.version().await.unwrap());
    }

    /// `snapshot_at_current_version` is the builder's one-snapshot hook. With
    /// read refresh enabled, the caller can advance while the pin stays put.
    #[tokio::test]
    async fn test_native_snapshot_pins_count_scan_and_version() {
        use std::time::Duration;

        use crate::connection::ConnectBuilder;

        let temp_dir = tempfile::tempdir().unwrap();
        let db = ConnectBuilder::new(temp_dir.path().to_str().unwrap())
            .read_consistency_interval(Duration::ZERO)
            .execute()
            .await
            .unwrap();
        let data = lance_datagen::gen_batch()
            .col("col_a", lance_datagen::array::step::<Int32Type>())
            .into_ldb_stream(RowCount::from(10), BatchCount::from(1));
        let table = db.create_table("base_tbl", data).execute().await.unwrap();

        let snapshot = table
            .base_table()
            .snapshot_at_current_version()
            .await
            .unwrap()
            .expect("native tables should provide an independent pinned handle");
        let version = snapshot.version().await.unwrap();
        assert_eq!(snapshot.count_rows(None).await.unwrap(), 10);

        let more_data = lance_datagen::gen_batch()
            .col("col_a", lance_datagen::array::step::<Int32Type>())
            .into_ldb_stream(RowCount::from(5), BatchCount::from(1));
        table.add(more_data).execute().await.unwrap();
        assert!(table.version().await.unwrap() > version);

        assert_eq!(snapshot.version().await.unwrap(), version);
        assert_eq!(snapshot.count_rows(None).await.unwrap(), 10);
        let batches: Vec<RecordBatch> = Table::from(snapshot)
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 10);
        assert_eq!(table.count_rows(None).await.unwrap(), 15);
    }

    #[cfg(feature = "remote")]
    #[tokio::test]
    async fn test_remote_permutation_builder_pins_snapshot() {
        use std::sync::{
            Mutex,
            atomic::{AtomicU64, Ordering},
        };

        use arrow_array::{RecordBatch, UInt64Array};
        use arrow_schema::{DataType, Field, Schema};

        let row_ids = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                ROW_ID,
                DataType::UInt64,
                false,
            )])),
            vec![Arc::new(UInt64Array::from(vec![100]))],
        )
        .unwrap();
        let mut query_body = Vec::new();
        {
            let mut writer =
                arrow_ipc::writer::FileWriter::try_new(&mut query_body, &row_ids.schema()).unwrap();
            writer.write(&row_ids).unwrap();
            writer.finish().unwrap();
        }

        let latest = Arc::new(AtomicU64::new(7));
        let expected_snapshot = Arc::new(AtomicU64::new(7));
        let planning_versions = Arc::new(Mutex::new(Vec::new()));
        let latest_ref = latest.clone();
        let expected_snapshot_ref = expected_snapshot.clone();
        let planning_versions_ref = planning_versions.clone();
        let table = Table::new_with_handler("remote_base", move |request| {
            let path = request.url().path();
            let body = request
                .body()
                .and_then(|body| body.as_bytes())
                .map(|body| serde_json::from_slice::<serde_json::Value>(body).unwrap());

            match path {
                "/v1/table/remote_base/describe/" => {
                    let requested = body.as_ref().and_then(|body| body["version"].as_u64());
                    let version = requested.unwrap_or_else(|| latest_ref.load(Ordering::SeqCst));
                    http::Response::builder()
                        .status(200)
                        .body(
                            format!(r#"{{"version":{version},"schema":{{"fields":[]}}}}"#)
                                .into_bytes(),
                        )
                        .unwrap()
                }
                "/v1/table/remote_base/get_rte_spec/" => http::Response::builder()
                    .status(200)
                    .body(br#"{"rte_spec":null}"#.to_vec())
                    .unwrap(),
                "/v1/table/remote_base/count_rows/" => {
                    let body = body.unwrap();
                    let version = body["version"].as_u64().unwrap();
                    assert_eq!(version, expected_snapshot_ref.load(Ordering::SeqCst));
                    assert_eq!(body["predicate"], "value > 0");
                    planning_versions_ref.lock().unwrap().push(version);

                    // Simulate a concurrent append after count_rows. An unpinned
                    // scan would now resolve version 8 and include different rows.
                    latest_ref.store(8, Ordering::SeqCst);
                    http::Response::builder()
                        .status(200)
                        .body(b"1".to_vec())
                        .unwrap()
                }
                "/v1/table/remote_base/query/" => {
                    let body = body.unwrap();
                    let version = body["version"].as_u64().unwrap();
                    assert_eq!(version, expected_snapshot_ref.load(Ordering::SeqCst));
                    assert_eq!(body["filter"], "value > 0");
                    assert_eq!(body["columns"], serde_json::json!([ROW_ID]));
                    planning_versions_ref.lock().unwrap().push(version);
                    http::Response::builder()
                        .status(200)
                        .header("content-type", "application/vnd.apache.arrow.file")
                        .body(query_body.clone())
                        .unwrap()
                }
                _ => panic!("unexpected request: {path}"),
            }
        });

        let permutation = PermutationBuilder::new(table.clone())
            .with_filter("value > 0".to_string())
            .build()
            .await
            .unwrap();
        assert_eq!(permutation.count_rows(None).await.unwrap(), 1);

        // Building uses a separate handle and must not pin the caller's table.
        assert_eq!(table.version().await.unwrap(), 8);

        // An explicit checkout is copied as-is and remains checked out afterward.
        expected_snapshot.store(6, Ordering::SeqCst);
        table.checkout(6).await.unwrap();
        let permutation = PermutationBuilder::new(table.clone())
            .with_filter("value > 0".to_string())
            .build()
            .await
            .unwrap();
        assert_eq!(permutation.count_rows(None).await.unwrap(), 1);
        assert_eq!(table.version().await.unwrap(), 6);
        assert_eq!(*planning_versions.lock().unwrap(), vec![7, 7, 6, 6]);
    }

    #[tokio::test]
    async fn test_permutation_records_base_version() {
        let temp_dir = tempfile::tempdir().unwrap();

        let db = connect(temp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();

        let initial_data = lance_datagen::gen_batch()
            .col("col_a", lance_datagen::array::step::<Int32Type>())
            .into_ldb_stream(RowCount::from(100), BatchCount::from(2));
        let data_table = db
            .create_table("base_tbl", initial_data)
            .execute()
            .await
            .unwrap();

        let build_version = data_table.version().await.unwrap();
        let permutation_table = PermutationBuilder::new(data_table.clone())
            .build()
            .await
            .unwrap();

        let recorded = permutation_table
            .schema()
            .await
            .unwrap()
            .metadata
            .get(BASE_VERSION_CONFIG_KEY)
            .expect("permutation should record the base version")
            .parse::<u64>()
            .unwrap();
        assert_eq!(recorded, build_version);

        // Advancing the base table must not move the recorded version.
        let more_data = lance_datagen::gen_batch()
            .col("col_a", lance_datagen::array::step::<Int32Type>())
            .into_ldb_stream(RowCount::from(50), BatchCount::from(1));
        data_table.add(more_data).execute().await.unwrap();
        assert!(data_table.version().await.unwrap() > recorded);
        assert_eq!(
            permutation_table
                .schema()
                .await
                .unwrap()
                .metadata
                .get(BASE_VERSION_CONFIG_KEY)
                .unwrap()
                .parse::<u64>()
                .unwrap(),
            recorded,
        );
    }

    /// Version numbers are per-branch, so a permutation built on a branch must record
    /// it -- a worker reopens by name and lands on main at the same number.
    #[tokio::test]
    async fn test_permutation_records_base_branch() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = connect(temp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();

        let initial_data = lance_datagen::gen_batch()
            .col("col_a", lance_datagen::array::step::<Int32Type>())
            .into_ldb_stream(RowCount::from(10), BatchCount::from(1));
        let data_table = db
            .create_table("base_tbl", initial_data)
            .execute()
            .await
            .unwrap();

        let branch = data_table
            .create_branch("exp", lance::dataset::refs::Ref::from(("main", 1)))
            .await
            .unwrap();
        let permutation_table = PermutationBuilder::new(branch.clone())
            .build()
            .await
            .unwrap();

        let metadata = permutation_table.schema().await.unwrap().metadata.clone();
        assert_eq!(
            metadata.get(BASE_BRANCH_CONFIG_KEY).map(String::as_str),
            Some("exp")
        );

        // Main records nothing, so an absent key keeps meaning main.
        let main_permutation = PermutationBuilder::new(data_table.clone())
            .build()
            .await
            .unwrap();
        assert!(
            !main_permutation
                .schema()
                .await
                .unwrap()
                .metadata
                .contains_key(BASE_BRANCH_CONFIG_KEY)
        );
    }

    #[tokio::test]
    async fn test_build_does_not_pin_the_callers_table() {
        let temp_dir = tempfile::tempdir().unwrap();

        let db = connect(temp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();

        let initial_data = lance_datagen::gen_batch()
            .col("col_a", lance_datagen::array::step::<Int32Type>())
            .into_ldb_stream(RowCount::from(100), BatchCount::from(1));
        let data_table = db
            .create_table("base_tbl", initial_data)
            .execute()
            .await
            .unwrap();

        PermutationBuilder::new(data_table.clone())
            .build()
            .await
            .unwrap();

        // The builder pins its own handle; the caller's must still track latest.
        let more_data = lance_datagen::gen_batch()
            .col("col_a", lance_datagen::array::step::<Int32Type>())
            .into_ldb_stream(RowCount::from(50), BatchCount::from(1));
        data_table.add(more_data).execute().await.unwrap();
        assert_eq!(data_table.count_rows(None).await.unwrap(), 150);
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

    /// Rows that have not been flushed to the base table have no row id, so a
    /// permutation cannot reference them.  Reading the base table alone would drop
    /// them from training without saying so, so the table is refused instead.
    #[tokio::test]
    async fn test_permutation_rejects_rte_spec() {
        use crate::table::RTEWriteSpec;
        use arrow_array::{Int32Array, RecordBatchIterator};
        use arrow_schema::{DataType, Field, Schema};

        // RTE needs a real dataset directory and a non-nullable primary key.
        let temp_dir = tempfile::tempdir().unwrap();
        let db = connect(temp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("idx", DataType::Int32, false)]));
        let batch = arrow_array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![0, 1, 2, 3]))],
        )
        .unwrap();
        let reader: Box<dyn arrow_array::RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));
        let table = db.create_table("tbl", reader).execute().await.unwrap();

        // Without a spec the build succeeds.
        PermutationBuilder::new(table.clone())
            .build()
            .await
            .unwrap();

        table.set_unenforced_primary_key(["idx"]).await.unwrap();
        table.enable_rte(RTEWriteSpec::unsharded()).await.unwrap();

        let err = PermutationBuilder::new(table).build().await.unwrap_err();
        assert!(
            err.to_string().contains("RTE write spec"),
            "expected the pre-check to refuse the table, got: {err}"
        );
    }

    /// Base table schema metadata (e.g. Hugging Face's `huggingface` key) describes the base
    /// table's columns, so the permutation table drops it and keeps only its own config.
    #[tokio::test]
    async fn test_permutation_over_table_with_schema_metadata() {
        use arrow_array::{Int64Array, RecordBatchIterator};
        use arrow_schema::{DataType, Field, Schema};

        let temp_dir = tempfile::tempdir().unwrap();
        let db = connect(temp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let schema = Arc::new(
            Schema::new(vec![Field::new("x", DataType::Int64, false)]).with_metadata(
                HashMap::from([
                    ("huggingface".to_string(), "{}".to_string()),
                    // Collides with a config key; the config value must win.
                    (SPLIT_NAMES_CONFIG_KEY.to_string(), "not json".to_string()),
                ]),
            ),
        );
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..100))],
        )
        .unwrap();
        let reader: Box<dyn arrow_array::RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));
        let table = db.create_table("tbl", reader).execute().await.unwrap();
        assert!(
            table
                .schema()
                .await
                .unwrap()
                .metadata
                .contains_key("huggingface")
        );

        let permutation_table = PermutationBuilder::new(table)
            .with_split_strategy(
                SplitStrategy::Random {
                    seed: Some(1),
                    sizes: SplitSizes::Fixed(2),
                    clump_size: None,
                },
                Some(vec!["train".to_string(), "test".to_string()]),
            )
            .with_shuffle_strategy(ShuffleStrategy::Random {
                seed: Some(1),
                clump_size: None,
            })
            .build()
            .await
            .unwrap();

        assert_eq!(permutation_table.count_rows(None).await.unwrap(), 100);
        let metadata = permutation_table.schema().await.unwrap().metadata.clone();
        assert!(!metadata.contains_key("huggingface"));
        assert_eq!(
            metadata.get(SPLIT_NAMES_CONFIG_KEY).map(String::as_str),
            Some(r#"["train","test"]"#)
        );
        assert!(metadata.contains_key(BASE_VERSION_CONFIG_KEY));
    }
}
