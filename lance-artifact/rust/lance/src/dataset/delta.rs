// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::transaction::Transaction;
use crate::Dataset;
use crate::Result;
use crate::dataset::scanner::DatasetRecordBatchStream;
use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt, TryStreamExt};
use lance_core::Error;
use lance_core::ROW_CREATED_AT_VERSION;
use lance_core::ROW_ID;
use lance_core::ROW_LAST_UPDATED_AT_VERSION;
use lance_core::WILDCARD;
use lance_core::utils::tokio::get_num_compute_intensive_cpus;

/// Builder for creating a [`DatasetDelta`] to explore changes between dataset versions.
///
/// # Example
///
/// ```
/// # use lance::{Dataset, Result};
/// # use lance::dataset::delta::DatasetDeltaBuilder;
/// # async fn example(dataset: &Dataset) -> Result<()> {
/// // Compare against a specific version
/// let delta = DatasetDeltaBuilder::new(dataset.clone())
///     .compared_against_version(5)
///     .build()?;
///
/// // Or specify explicit version range
/// let delta = DatasetDeltaBuilder::new(dataset.clone())
///     .with_begin_version(3)
///     .with_end_version(7)
///     .build()?;
///
/// // Or specify explicit time range
/// let delta = DatasetDeltaBuilder::new(dataset.clone())
///     .with_begin_date(chrono::Utc::now())
///     .with_end_date(chrono::Utc::now())
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct DatasetDeltaBuilder {
    dataset: Dataset,
    compared_against_version: Option<u64>,
    begin_version: Option<u64>,
    end_version: Option<u64>,
    begin_timestamp: Option<DateTime<Utc>>,
    end_timestamp: Option<DateTime<Utc>>,
}

impl DatasetDeltaBuilder {
    /// Create a new builder for the given dataset.
    pub fn new(dataset: Dataset) -> Self {
        Self {
            dataset,
            compared_against_version: None,
            begin_version: None,
            end_version: None,
            begin_timestamp: None,
            end_timestamp: None,
        }
    }

    /// Compare the current dataset version against the specified version.
    ///
    /// The delta will automatically order the versions so that `begin_version` < `end_version`.
    /// Cannot be used together with explicit `with_begin_version` and `with_end_version`.
    pub fn compared_against_version(mut self, version: u64) -> Self {
        self.compared_against_version = Some(version);
        self
    }

    /// Set the beginning version for the delta (exclusive).
    ///
    /// Must be used together with `with_end_version`.
    /// Cannot be used together with `compared_against_version`.
    pub fn with_begin_version(mut self, version: u64) -> Self {
        self.begin_version = Some(version);
        self
    }

    /// Set the ending version for the delta (inclusive).
    ///
    /// Must be used together with `with_begin_version`.
    /// Cannot be used together with `compared_against_version`.
    pub fn with_end_version(mut self, version: u64) -> Self {
        self.end_version = Some(version);
        self
    }

    /// Set the beginning timestamp for the delta (exclusive).
    ///
    /// Must be used together with `with_end_date`.
    /// Cannot be used together with `compared_against_version` or explicit version range.
    pub fn with_begin_date(mut self, timestamp: DateTime<Utc>) -> Self {
        self.begin_timestamp = Some(timestamp);
        self
    }

    /// Set the ending timestamp for the delta (inclusive).
    ///
    /// Must be used together with `with_begin_date`.
    /// Cannot be used together with `compared_against_version` or explicit version range.
    pub fn with_end_date(mut self, timestamp: DateTime<Utc>) -> Self {
        self.end_timestamp = Some(timestamp);
        self
    }

    /// Build the [`DatasetDelta`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Both `compared_against_version` and explicit version range are specified
    /// - Neither `compared_against_version` nor explicit version range are specified
    /// - Only one of `with_begin_version` or `with_end_version` is specified
    pub fn build(self) -> Result<DatasetDelta> {
        // Validate incompatible combinations
        if self.compared_against_version.is_some()
            && (self.begin_version.is_some()
                || self.end_version.is_some()
                || self.begin_timestamp.is_some()
                || self.end_timestamp.is_some())
        {
            return Err(Error::invalid_input(
                "Cannot combine compared_against_version with explicit begin/end versions or dates",
            ));
        }

        // Resolve parameters and construct DatasetDelta. For date ranges, defer mapping to versions.
        let (begin_version, end_version, begin_ts, end_ts) = match (
            self.compared_against_version,
            self.begin_version,
            self.end_version,
            self.begin_timestamp,
            self.end_timestamp,
        ) {
            (Some(compared), None, None, None, None) => {
                let current_version = self.dataset.version().version;
                if current_version > compared {
                    (compared, current_version, None, None)
                } else {
                    (current_version, compared, None, None)
                }
            }
            (None, Some(begin), Some(end), None, None) => (begin, end, None, None),
            (None, None, None, Some(begin_ts), Some(end_ts)) => {
                (0, 0, Some(begin_ts), Some(end_ts))
            }
            (None, Some(_), None, None, None) | (None, None, Some(_), None, None) => {
                return Err(Error::invalid_input(
                    "Must specify both with_begin_version and with_end_version",
                ));
            }
            (None, None, None, Some(begin_ts), None) => (0, 0, Some(begin_ts), None),
            (None, None, None, None, Some(_)) => {
                return Err(Error::invalid_input(
                    "Must specify with_begin_date when with_end_date is provided",
                ));
            }
            (None, None, None, None, None) => {
                return Err(Error::invalid_input(
                    "Must specify either compared_against_version or both with_begin_version and with_end_version",
                ));
            }
            _ => {
                return Err(Error::invalid_input(
                    "Invalid combination of parameters for DatasetDeltaBuilder",
                ));
            }
        };

        Ok(DatasetDelta {
            begin_version,
            end_version,
            base_dataset: self.dataset,
            begin_timestamp: begin_ts,
            end_timestamp: end_ts,
        })
    }
}

/// APIs for exploring changes between two versions of a dataset.
pub struct DatasetDelta {
    /// The base version number for comparison.
    pub(crate) begin_version: u64,
    /// The end version number for comparison
    pub(crate) end_version: u64,
    /// The Lance dataset to compute delta
    pub(crate) base_dataset: Dataset,
    pub(crate) begin_timestamp: Option<DateTime<Utc>>,
    pub(crate) end_timestamp: Option<DateTime<Utc>>,
}

impl DatasetDelta {
    /// Resolve the effective version range for this delta.
    ///
    /// If a date window is set (`begin_timestamp` and `end_timestamp` provided), this lazily
    /// maps timestamps to version ids by scanning dataset versions:
    /// - Begin is exclusive: pick the greatest version with `timestamp < begin_timestamp`.
    /// - End is inclusive:  pick the greatest version with `timestamp <= end_timestamp`.
    ///
    /// If no date window is set, returns the explicit `begin_version`/`end_version` stored on
    /// the struct.
    async fn resolve_range(&self) -> Result<(u64, u64)> {
        if let (Some(begin_ts), Some(end_ts)) = (self.begin_timestamp, self.end_timestamp) {
            // Load all dataset versions and fold them to a version interval matching the date window
            let versions = self.base_dataset.versions().await?;
            let mut begin_version: u64 = 0;
            let mut end_version: u64 = 0;
            for v in &versions {
                // Exclusive begin: track the largest version strictly before begin_ts
                if v.timestamp < begin_ts && v.version > begin_version {
                    begin_version = v.version;
                }
                // Inclusive end: track the largest version at or before end_ts
                if v.timestamp <= end_ts && v.version > end_version {
                    end_version = v.version;
                }
            }
            Ok((begin_version, end_version))
        } else if let (Some(begin_ts), None) = (self.begin_timestamp, self.end_timestamp) {
            // Open-ended range: use latest version as end
            let versions = self.base_dataset.versions().await?;
            let mut begin_version: u64 = 0;
            for v in &versions {
                if v.timestamp < begin_ts && v.version > begin_version {
                    begin_version = v.version;
                }
            }
            let end_version = self.base_dataset.latest_version_id().await?;
            Ok((begin_version, end_version))
        } else {
            // No date window: use the pre-resolved version interval
            Ok((self.begin_version, self.end_version))
        }
    }

    /// Listing the transactions between two versions.
    pub async fn list_transactions(&self) -> Result<Vec<Transaction>> {
        let (begin_version, end_version) = self.resolve_range().await?;
        stream::iter((begin_version + 1)..=end_version)
            .map(|version| {
                let base_dataset = self.base_dataset.clone();
                async move {
                    let current_ds = match base_dataset.checkout_version(version).await {
                        Ok(ds) => ds,
                        Err(err) => {
                            if matches!(err, Error::DatasetNotFound { .. }) {
                                return Err(Error::VersionNotFound {
                                    message: format!(
                                        "Can not find version {}, please check if it has been cleanup.",
                                        version
                                    ),
                                });
                            } else {
                                return Err(err);
                            }
                        }
                    };
                    current_ds.read_transaction().await
                }
            })
            .buffered(get_num_compute_intensive_cpus())
            .try_filter_map(|result| async move { Ok(result) })
            .try_collect()
            .await
    }

    /// Get inserted rows between the two versions.
    ///
    /// This returns rows where `_row_created_at_version` is greater than `begin_version`
    /// and less than or equal to `end_version`.
    ///
    /// The result always includes:
    /// - `_row_created_at_version`: Version when the row was created
    /// - `_row_last_updated_at_version`: Version when the row was last updated
    /// - `_rowid`: Row ID
    /// - All other columns from the dataset
    ///
    /// # Returns
    ///
    /// A stream of record batches containing the inserted rows.
    ///
    /// # Example
    ///
    /// ```
    /// # use lance::{Dataset, Result};
    /// # use futures::TryStreamExt;
    /// # async fn example(dataset: &Dataset, previous_version: u64) -> Result<()> {
    /// let delta = dataset.delta()
    ///     .compared_against_version(previous_version)
    ///     .build()?;
    /// let mut inserted = delta.get_inserted_rows().await?;
    /// while let Some(batch) = inserted.try_next().await? {
    ///     // Process batch...
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_inserted_rows(&self) -> Result<DatasetRecordBatchStream> {
        let mut scanner = self.base_dataset.scan();

        // Enable version columns
        scanner.project(&[
            WILDCARD,
            ROW_ID,
            ROW_CREATED_AT_VERSION,
            ROW_LAST_UPDATED_AT_VERSION,
        ])?;

        // Filter for rows created in the version range
        let filter = self.build_inserted_rows_filter().await?;
        scanner.filter(&filter)?;

        scanner.try_into_stream().await
    }

    async fn build_inserted_rows_filter(&self) -> Result<String> {
        let (begin_version, end_version) = self.resolve_range().await?;
        Ok(format!(
            "_row_created_at_version > {} AND _row_created_at_version <= {}",
            begin_version, end_version
        ))
    }

    /// Get updated rows between the two versions.
    ///
    /// This returns rows where `_row_last_updated_at_version` is greater than `begin_version`
    /// and less than or equal to `end_version`, but `_row_created_at_version` is less than
    /// or equal to `begin_version` (to exclude newly inserted rows).
    ///
    /// The result always includes:
    /// - `_row_created_at_version`: Version when the row was created
    /// - `_row_last_updated_at_version`: Version when the row was last updated
    /// - `_rowid`: Row ID
    /// - All other columns from the dataset
    ///
    /// # Returns
    ///
    /// A stream of record batches containing the updated rows.
    ///
    /// # Example
    ///
    /// ```
    /// # use lance::{Dataset, Result};
    /// # use futures::TryStreamExt;
    /// # async fn example(dataset: &Dataset, previous_version: u64) -> Result<()> {
    /// let delta = dataset.delta()
    ///     .compared_against_version(previous_version)
    ///     .build()?;
    /// let mut updated = delta.get_updated_rows().await?;
    /// while let Some(batch) = updated.try_next().await? {
    ///     // Process batch...
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_updated_rows(&self) -> Result<DatasetRecordBatchStream> {
        let mut scanner = self.base_dataset.scan();

        // Enable version columns
        scanner.project(&[
            WILDCARD,
            ROW_ID,
            ROW_CREATED_AT_VERSION,
            ROW_LAST_UPDATED_AT_VERSION,
        ])?;

        // Filter for rows that were updated (not inserted) in the version range
        let filter = self.build_updated_rows_batch_filter().await?;
        scanner.filter(&filter)?;

        scanner.try_into_stream().await
    }

    async fn build_updated_rows_batch_filter(&self) -> Result<String> {
        let (begin_version, end_version) = self.resolve_range().await?;
        Ok(format!(
            "_row_created_at_version <= {} AND _row_last_updated_at_version > {} AND _row_last_updated_at_version <= {}",
            begin_version, begin_version, end_version
        ))
    }

    /// Get upserted rows between the two versions.
    ///
    /// This returns rows meet following conditions:
    /// Condition 1:
    ///     `_row_last_updated_at_version` is greater than `begin_version`
    ///     and less than or equal to `end_version`, but `_row_created_at_version` is less than
    ///     or equal to `begin_version` (to exclude newly inserted rows).
    /// Condition 2:
    ///     This returns rows where `_row_created_at_version` is greater than `begin_version`
    ///     and less than or equal to `end_version`.
    ///
    /// The result always includes:
    /// - `_row_created_at_version`: Version when the row was created
    /// - `_row_last_updated_at_version`: Version when the row was last updated
    /// - `_rowid`: Row ID
    /// - All other columns from the dataset
    ///
    /// # Returns
    ///
    /// A stream of record batches containing the updated and inserted rows.
    ///
    /// # Example
    ///
    /// ```
    /// # use lance::{Dataset, Result};
    /// # use futures::TryStreamExt;
    /// # async fn example(dataset: &Dataset, previous_version: u64) -> Result<()> {
    /// let delta = dataset.delta()
    ///     .compared_against_version(previous_version)
    ///     .build()?;
    /// let mut updated = delta.get_upserted_rows().await?;
    /// while let Some(batch) = updated.try_next().await? {
    ///     // Process batch...
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_upserted_rows(&self) -> Result<DatasetRecordBatchStream> {
        let mut scanner = self.base_dataset.scan();

        // Enable version columns
        scanner.project(&[
            WILDCARD,
            ROW_ID,
            ROW_CREATED_AT_VERSION,
            ROW_LAST_UPDATED_AT_VERSION,
        ])?;

        // Filter for rows that were updated or inserted in the version range
        let filter = self.build_upserted_rows_filter().await?;
        scanner.filter(&filter)?;

        scanner.try_into_stream().await
    }

    async fn build_upserted_rows_filter(&self) -> Result<String> {
        let inserted_row_filter = self.build_inserted_rows_filter().await?;
        let updated_rows_filter = self.build_updated_rows_batch_filter().await?;
        Ok(format!(
            "({}) OR ({})",
            inserted_row_filter, updated_rows_filter
        ))
    }
}

#[cfg(test)]
mod tests {

    use crate::dataset::transaction::Operation;
    use crate::dataset::{Dataset, WriteParams};
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use arrow_array::types::UInt64Type;
    use chrono::Duration;
    use futures::TryStreamExt;
    use lance_core::{ROW_CREATED_AT_VERSION, ROW_ID, ROW_LAST_UPDATED_AT_VERSION};
    use lance_datagen::{BatchCount, RowCount, array};
    use mock_instant::thread_local::MockClock;
    use std::sync::Arc;

    async fn create_test_dataset(
        rows: usize,
        batches: usize,
        value: &str,
        stable_row_ids: bool,
    ) -> Dataset {
        let data = lance_datagen::gen_batch()
            .col("key", array::step::<Int32Type>())
            .col("value", array::fill_utf8(value.to_string()))
            .into_reader_rows(
                RowCount::from(rows as u64),
                BatchCount::from(batches as u32),
            );

        let write_params = WriteParams {
            enable_stable_row_ids: stable_row_ids,
            ..Default::default()
        };
        Dataset::write(data, "memory://", Some(write_params))
            .await
            .unwrap()
    }

    async fn write_dataset_temp(
        dir: &lance_core::utils::tempfile::TempStrDir,
        start_key: i32,
        rows: usize,
        batches: usize,
        value: &str,
        stable_row_ids: bool,
        append: bool,
    ) -> Dataset {
        let data = lance_datagen::gen_batch()
            .col("key", array::step_custom::<Int32Type>(start_key, 1))
            .col("value", array::fill_utf8(value.to_string()))
            .into_reader_rows(
                RowCount::from(rows as u64),
                BatchCount::from(batches as u32),
            );

        let write_params = WriteParams {
            enable_stable_row_ids: stable_row_ids,
            mode: if append {
                crate::dataset::WriteMode::Append
            } else {
                crate::dataset::WriteMode::Create
            },
            ..Default::default()
        };
        Dataset::write(data, dir, Some(write_params)).await.unwrap()
    }

    async fn update_where<T: Into<Arc<Dataset>>>(ds: T, predicate: &str, value: &str) -> Dataset {
        let updated = crate::dataset::UpdateBuilder::new(ds.into())
            .update_where(predicate)
            .unwrap()
            .set("value", &format!("'{}'", value))
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();
        Arc::try_unwrap(updated.new_dataset).unwrap_or_else(|arc| arc.as_ref().clone())
    }

    async fn scan_project_filter(
        ds: &Dataset,
        cols: &[&str],
        filter: Option<&str>,
    ) -> arrow_array::RecordBatch {
        let mut scanner = ds.scan();
        scanner.project(cols).unwrap();
        if let Some(f) = filter {
            scanner.filter(f).unwrap();
        }
        scanner.try_into_batch().await.unwrap()
    }

    // Optional: collect a stream of RecordBatch into a single batch
    async fn collect_stream(
        stream: crate::dataset::scanner::DatasetRecordBatchStream,
    ) -> arrow_array::RecordBatch {
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap()
    }

    #[tokio::test]
    async fn test_list_no_transaction() {
        let ds = create_test_dataset(1_000, 10, "value", false).await;
        let delta = ds.delta().compared_against_version(1).build().unwrap();
        let result = delta.list_transactions().await;
        assert_eq!(result.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_list_single_transaction() {
        let mut ds = create_test_dataset(1_000, 10, "value", false).await;
        ds.delete("key = 5").await.unwrap();

        let delta_struct = ds
            .delta()
            .with_begin_version(1)
            .with_end_version(ds.version().version)
            .build()
            .unwrap();
        let txs = delta_struct.list_transactions().await.unwrap();
        assert_eq!(txs.len(), 1);
        assert!(matches!(txs[0].operation, Operation::Delete { .. }));
    }

    #[tokio::test]
    async fn test_list_multiple_transactions() {
        let mut ds = create_test_dataset(1_000, 10, "value", false).await;
        ds.delete("key = 5").await.unwrap();
        ds.delete("key = 6").await.unwrap();

        let delta_struct = ds
            .delta()
            .with_begin_version(1)
            .with_end_version(ds.version().version)
            .build()
            .unwrap();
        let txs = delta_struct.list_transactions().await.unwrap();
        assert_eq!(txs.len(), 2);
    }

    #[tokio::test]
    async fn test_list_contains_deleted_transaction() {
        MockClock::set_system_time(std::time::Duration::from_secs(1));

        let mut ds = create_test_dataset(1_000, 10, "value", false).await;

        MockClock::set_system_time(std::time::Duration::from_secs(2));

        ds.delete("key = 5").await.unwrap();
        ds.delete("key = 6").await.unwrap();
        ds.delete("key = 7").await.unwrap();

        MockClock::set_system_time(std::time::Duration::from_secs(3));

        let end_version = ds.version().version;
        let base_dataset = ds.clone();

        MockClock::set_system_time(std::time::Duration::from_secs(4));

        ds.cleanup_old_versions(Duration::seconds(1), Some(true), None)
            .await
            .expect("Cleanup old versions failed");

        MockClock::set_system_time(std::time::Duration::from_secs(5));

        let delta_struct = base_dataset
            .delta()
            .with_begin_version(1)
            .with_end_version(end_version)
            .build()
            .unwrap();

        let result = delta_struct.list_transactions().await;
        match result {
            Err(lance_core::Error::VersionNotFound { message }) => {
                assert!(message.contains("Can not find version"));
            }
            _ => panic!("Expected VersionNotFound error."),
        }
    }

    #[tokio::test]
    async fn test_row_created_at_version_basic() {
        // Create dataset with stable row IDs enabled
        let ds = create_test_dataset(100, 1, "value", true).await;

        assert_eq!(ds.version().version, 1);

        // Scan with _row_created_at_version
        let result = scan_project_filter(&ds, &["key", ROW_CREATED_AT_VERSION], None).await;

        // All rows should have _row_created_at_version = 1
        let created_at = result[ROW_CREATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();

        assert_eq!(result.num_rows(), 100);
        for version in created_at.iter() {
            assert_eq!(*version, 1);
        }
    }

    #[tokio::test]
    async fn test_row_last_updated_at_version_basic() {
        // Create dataset with stable row IDs enabled
        let ds = create_test_dataset(100, 1, "value", true).await;

        assert_eq!(ds.version().version, 1);

        // Update some rows (version 2)
        let ds = update_where(ds, "key < 30", "updated_v2").await;
        assert_eq!(ds.version().version, 2);

        // Update different rows (version 3)
        let ds = update_where(ds, "key >= 30 AND key < 50", "updated_v3").await;
        assert_eq!(ds.version().version, 3);

        // Update some rows again (version 4) - these rows were updated in v2
        let ds = update_where(ds, "key >= 10 AND key < 20", "updated_v4").await;
        assert_eq!(ds.version().version, 4);

        // Scan with _row_last_updated_at_version
        let result = scan_project_filter(&ds, &["key", ROW_LAST_UPDATED_AT_VERSION], None).await;

        let updated_at = result[ROW_LAST_UPDATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        assert_eq!(result.num_rows(), 100);

        for i in 0..result.num_rows() {
            let key = keys[i];
            if (10..20).contains(&key) {
                // Updated in v2, then again in v4 - should show v4
                assert_eq!(updated_at[i], 4);
            } else if key < 30 {
                // Updated only in v2 (but not in the 10-20 range)
                assert_eq!(updated_at[i], 2);
            } else if (30..50).contains(&key) {
                // Updated only in v3
                assert_eq!(updated_at[i], 3);
            } else {
                // Never updated - still at v1
                assert_eq!(updated_at[i], 1);
            }
        }
    }

    #[tokio::test]
    async fn test_row_version_metadata_after_update() {
        // Create dataset with stable row IDs enabled
        let ds = create_test_dataset(100, 1, "value", true).await;

        assert_eq!(ds.version().version, 1);

        // Update some rows (version 2)
        let ds = update_where(ds, "key < 10", "updated_v2").await;
        assert_eq!(ds.version().version, 2);

        // Update different rows (version 3)
        let ds = update_where(ds, "key >= 20 AND key < 30", "updated_v3").await;
        assert_eq!(ds.version().version, 3);

        // Update some of the same rows again (version 4)
        let ds = update_where(ds, "key >= 5 AND key < 15", "updated_v4").await;
        assert_eq!(ds.version().version, 4);

        // Scan with both version metadata columns
        let result = scan_project_filter(
            &ds,
            &["key", ROW_CREATED_AT_VERSION, ROW_LAST_UPDATED_AT_VERSION],
            None,
        )
        .await;

        let created_at = result[ROW_CREATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let updated_at = result[ROW_LAST_UPDATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        assert_eq!(result.num_rows(), 100);

        for i in 0..result.num_rows() {
            let key = keys[i];
            // All rows were created at version 1
            assert_eq!(created_at[i], 1);

            if (5..15).contains(&key) {
                // Updated in v4 (some also updated in v2)
                assert_eq!(updated_at[i], 4);
            } else if key < 10 {
                // Updated in v2 only (keys 0-4)
                assert_eq!(updated_at[i], 2);
            } else if (20..30).contains(&key) {
                // Updated in v3 only
                assert_eq!(updated_at[i], 3);
            } else {
                // Never updated - still at v1
                assert_eq!(updated_at[i], 1);
            }
        }
    }

    #[tokio::test]
    async fn test_row_version_metadata_after_append() {
        // Create initial dataset
        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let ds = write_dataset_temp(&temp_dir, 0, 50, 1, "value", true, false).await;

        assert_eq!(ds.version().version, 1);

        // Append more data
        let ds = write_dataset_temp(&temp_dir, 50, 50, 1, "appended", true, true).await;

        assert_eq!(ds.version().version, 2);

        // Scan with both version metadata columns
        let result = scan_project_filter(
            &ds,
            &["key", ROW_CREATED_AT_VERSION, ROW_LAST_UPDATED_AT_VERSION],
            None,
        )
        .await;

        let created_at = result[ROW_CREATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let updated_at = result[ROW_LAST_UPDATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        assert_eq!(result.num_rows(), 100);

        for i in 0..result.num_rows() {
            let key = keys[i];
            if key < 50 {
                // Original rows created at version 1
                assert_eq!(created_at[i], 1);
                assert_eq!(updated_at[i], 1);
            } else {
                // Appended rows created at version 2
                assert_eq!(created_at[i], 2);
                assert_eq!(updated_at[i], 2);
            }
        }
    }

    #[tokio::test]
    async fn test_row_version_metadata_after_delete() {
        // Create dataset with stable row IDs enabled
        let mut ds = create_test_dataset(100, 1, "value", true).await;

        assert_eq!(ds.version().version, 1);

        // Delete some rows
        ds.delete("key < 10").await.unwrap();
        assert_eq!(ds.version().version, 2);

        // Scan with both version metadata columns
        let result = scan_project_filter(
            &ds,
            &["key", ROW_CREATED_AT_VERSION, ROW_LAST_UPDATED_AT_VERSION],
            None,
        )
        .await;

        let created_at = result[ROW_CREATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let updated_at = result[ROW_LAST_UPDATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        // Should have 90 rows remaining (100 - 10 deleted)
        assert_eq!(result.num_rows(), 90);

        for i in 0..result.num_rows() {
            let key = keys[i];
            // All remaining rows should be key >= 10
            assert!(key >= 10);
            // All rows were created at version 1
            assert_eq!(created_at[i], 1);
            // All rows still have last_updated at version 1 (delete doesn't update rows)
            assert_eq!(updated_at[i], 1);
        }
    }

    #[tokio::test]
    async fn test_row_version_metadata_combined() {
        // Create dataset with stable row IDs enabled
        let data = lance_datagen::gen_batch()
            .col("key", array::step::<Int32Type>())
            .col("value", array::fill_utf8("value".to_string()))
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));

        let write_params = WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        };
        let ds = Dataset::write(data, "memory://", Some(write_params))
            .await
            .unwrap();

        // Version 1: Initial write
        assert_eq!(ds.version().version, 1);

        // Version 2: Update some rows
        let updated = crate::dataset::UpdateBuilder::new(Arc::new(ds))
            .update_where("key >= 40 AND key < 50")
            .unwrap()
            .set("value", "'updated1'")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();
        let ds = updated.new_dataset;

        // Version 3: Update different rows
        let updated = crate::dataset::UpdateBuilder::new(ds)
            .update_where("key >= 50 AND key < 60")
            .unwrap()
            .set("value", "'updated2'")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();
        let mut ds = Arc::try_unwrap(updated.new_dataset).expect("no other Arc references");

        // Version 4: Delete some rows
        ds.delete("key < 10").await.unwrap();

        assert_eq!(ds.version().version, 4);

        // Scan with all metadata columns
        let result = ds
            .scan()
            .with_row_id()
            .project(&["key", ROW_CREATED_AT_VERSION, ROW_LAST_UPDATED_AT_VERSION])
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        let row_ids = result[ROW_ID].as_primitive::<UInt64Type>().values();
        let created_at = result[ROW_CREATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let updated_at = result[ROW_LAST_UPDATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        // Should have 90 rows (100 - 10 deleted)
        assert_eq!(result.num_rows(), 90);

        for i in 0..result.num_rows() {
            let key = keys[i];
            let _row_id = row_ids[i];

            // All rows were created at version 1
            assert_eq!(created_at[i], 1);

            // Check last_updated_at_version based on key range
            if (40..50).contains(&key) {
                // Updated at version 2
                assert_eq!(updated_at[i], 2);
            } else if (50..60).contains(&key) {
                // Updated at version 3
                assert_eq!(updated_at[i], 3);
            } else {
                // Not updated, still at version 1
                assert_eq!(updated_at[i], 1);
            }
        }
    }

    #[tokio::test]
    async fn test_filter_by_row_created_at_version() {
        // Create initial dataset
        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let ds = write_dataset_temp(&temp_dir, 0, 50, 1, "value", true, false).await;

        assert_eq!(ds.version().version, 1);

        // Append more data (version 2)
        let ds = write_dataset_temp(&temp_dir, 50, 50, 1, "appended", true, true).await;

        assert_eq!(ds.version().version, 2);

        // Test 1: Filter for rows created at version 1
        let result = scan_project_filter(
            &ds,
            &["key", ROW_CREATED_AT_VERSION],
            Some("_row_created_at_version = 1"),
        )
        .await;

        assert_eq!(result.num_rows(), 50);
        let created_at = result[ROW_CREATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        for i in 0..result.num_rows() {
            assert_eq!(created_at[i], 1);
            assert!(keys[i] < 50);
        }

        // Test 2: Filter for rows created at version 2
        let result = scan_project_filter(
            &ds,
            &["key", ROW_CREATED_AT_VERSION],
            Some("_row_created_at_version = 2"),
        )
        .await;

        assert_eq!(result.num_rows(), 50);
        let created_at = result[ROW_CREATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        for i in 0..result.num_rows() {
            assert_eq!(created_at[i], 2);
            assert!(keys[i] >= 50);
        }

        // Test 3: Filter for rows created at version >= 2
        let result = scan_project_filter(
            &ds,
            &["key", ROW_CREATED_AT_VERSION],
            Some("_row_created_at_version >= 2"),
        )
        .await;

        assert_eq!(result.num_rows(), 50);
        for i in 0..result.num_rows() {
            let created_at_val = result[ROW_CREATED_AT_VERSION]
                .as_primitive::<UInt64Type>()
                .value(i);
            assert!(created_at_val >= 2);
        }
    }

    #[tokio::test]
    async fn test_filter_by_row_last_updated_at_version() {
        // Create dataset with stable row IDs enabled
        let data = lance_datagen::gen_batch()
            .col("key", array::step::<Int32Type>())
            .col("value", array::fill_utf8("value".to_string()))
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));

        let write_params = WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        };
        let ds = Dataset::write(data, "memory://", Some(write_params))
            .await
            .unwrap();

        assert_eq!(ds.version().version, 1);

        // Update some rows (version 2)
        let updated = crate::dataset::UpdateBuilder::new(Arc::new(ds))
            .update_where("key < 30")
            .unwrap()
            .set("value", "'updated_v2'")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();
        let ds = updated.new_dataset;
        assert_eq!(ds.version().version, 2);

        // Update different rows (version 3)
        let updated = crate::dataset::UpdateBuilder::new(ds)
            .update_where("key >= 30 AND key < 50")
            .unwrap()
            .set("value", "'updated_v3'")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();
        let ds = updated.new_dataset;
        assert_eq!(ds.version().version, 3);

        // Test 1: Filter for rows last updated at version 1
        let result = ds
            .scan()
            .project(&["key", ROW_LAST_UPDATED_AT_VERSION])
            .unwrap()
            .filter("_row_last_updated_at_version = 1")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        // Should have 50 rows (keys 50-99 that were never updated)
        assert_eq!(result.num_rows(), 50);
        let updated_at = result[ROW_LAST_UPDATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        for i in 0..result.num_rows() {
            assert_eq!(updated_at[i], 1);
            assert!(keys[i] >= 50);
        }

        // Test 2: Filter for rows last updated at version 2
        let result = ds
            .scan()
            .project(&["key", ROW_LAST_UPDATED_AT_VERSION])
            .unwrap()
            .filter("_row_last_updated_at_version = 2")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        // Should have 30 rows (keys 0-29)
        assert_eq!(result.num_rows(), 30);
        let updated_at = result[ROW_LAST_UPDATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        for i in 0..result.num_rows() {
            assert_eq!(updated_at[i], 2);
            assert!(keys[i] < 30);
        }

        // Test 3: Filter for rows last updated at version 3
        let result = ds
            .scan()
            .project(&["key", ROW_LAST_UPDATED_AT_VERSION])
            .unwrap()
            .filter("_row_last_updated_at_version = 3")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        // Should have 20 rows (keys 30-49)
        assert_eq!(result.num_rows(), 20);
        let updated_at = result[ROW_LAST_UPDATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        for i in 0..result.num_rows() {
            assert_eq!(updated_at[i], 3);
            assert!(keys[i] >= 30 && keys[i] < 50);
        }

        // Test 4: Filter for rows last updated at version > 1
        let result = ds
            .scan()
            .project(&["key", ROW_LAST_UPDATED_AT_VERSION])
            .unwrap()
            .filter("_row_last_updated_at_version > 1")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        // Should have 50 rows (30 from v2 + 20 from v3)
        assert_eq!(result.num_rows(), 50);
        for i in 0..result.num_rows() {
            let updated_at_val = result[ROW_LAST_UPDATED_AT_VERSION]
                .as_primitive::<UInt64Type>()
                .value(i);
            assert!(updated_at_val > 1);
        }
    }

    #[tokio::test]
    async fn test_filter_by_combined_version_columns() {
        // Create initial dataset
        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let ds = write_dataset_temp(&temp_dir, 0, 50, 1, "value", true, false).await;

        assert_eq!(ds.version().version, 1);

        // Append more data (version 2)
        let ds = write_dataset_temp(&temp_dir, 50, 50, 1, "appended", true, true).await;

        assert_eq!(ds.version().version, 2);

        // Update some of the original rows (version 3)
        let ds = update_where(ds, "key >= 20 AND key < 30", "updated_v3").await;
        assert_eq!(ds.version().version, 3);

        // Test 1: Filter for rows created at v1 AND last updated at v1
        // (Original rows that were never updated)
        let result = scan_project_filter(
            &ds,
            &["key", ROW_CREATED_AT_VERSION, ROW_LAST_UPDATED_AT_VERSION],
            Some("_row_created_at_version = 1 AND _row_last_updated_at_version = 1"),
        )
        .await;

        // Should have 40 rows (keys 0-19 and 30-49)
        assert_eq!(result.num_rows(), 40);
        let created_at = result[ROW_CREATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let updated_at = result[ROW_LAST_UPDATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        for i in 0..result.num_rows() {
            assert_eq!(created_at[i], 1);
            assert_eq!(updated_at[i], 1);
            assert!(keys[i] < 50);
            assert!(keys[i] < 20 || keys[i] >= 30);
        }

        // Test 2: Filter for rows created at v1 AND last updated at v3
        // (Original rows that were updated in v3)
        let result = scan_project_filter(
            &ds,
            &["key", ROW_CREATED_AT_VERSION, ROW_LAST_UPDATED_AT_VERSION],
            Some("_row_created_at_version = 1 AND _row_last_updated_at_version = 3"),
        )
        .await;

        // Should have 10 rows (keys 20-29)
        assert_eq!(result.num_rows(), 10);
        let created_at = result[ROW_CREATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let updated_at = result[ROW_LAST_UPDATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        for i in 0..result.num_rows() {
            assert_eq!(created_at[i], 1);
            assert_eq!(updated_at[i], 3);
            assert!(keys[i] >= 20 && keys[i] < 30);
        }

        // Test 3: Filter for rows where created_at = last_updated_at
        // (Rows that were never updated after creation)
        let result = scan_project_filter(
            &ds,
            &["key", ROW_CREATED_AT_VERSION, ROW_LAST_UPDATED_AT_VERSION],
            Some("_row_created_at_version = _row_last_updated_at_version"),
        )
        .await;

        // Should have 90 rows (40 from v1 that weren't updated + 50 from v2)
        assert_eq!(result.num_rows(), 90);
        let created_at = result[ROW_CREATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let updated_at = result[ROW_LAST_UPDATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();

        for i in 0..result.num_rows() {
            assert_eq!(created_at[i], updated_at[i]);
        }

        // Test 4: Filter for rows where created_at != last_updated_at
        // (Rows that were updated after creation)
        let result = scan_project_filter(
            &ds,
            &["key", ROW_CREATED_AT_VERSION, ROW_LAST_UPDATED_AT_VERSION],
            Some("_row_created_at_version != _row_last_updated_at_version"),
        )
        .await;

        // Should have 10 rows (keys 20-29 that were updated)
        assert_eq!(result.num_rows(), 10);
        let created_at = result[ROW_CREATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let updated_at = result[ROW_LAST_UPDATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        for i in 0..result.num_rows() {
            assert_ne!(created_at[i], updated_at[i]);
            assert_eq!(created_at[i], 1);
            assert_eq!(updated_at[i], 3);
            assert!(keys[i] >= 20 && keys[i] < 30);
        }
    }

    #[tokio::test]
    async fn test_filter_version_columns_with_other_columns() {
        // Create dataset
        let ds = create_test_dataset(100, 1, "value", true).await;

        // Update some rows (version 2)
        let ds = update_where(ds, "key >= 30 AND key < 60", "updated").await;

        // Test: Combine version filter with regular column filter
        // Find rows where key < 50 AND last_updated_at_version = 2
        let result = scan_project_filter(
            &ds,
            &["key", "value", ROW_LAST_UPDATED_AT_VERSION],
            Some("key < 50 AND _row_last_updated_at_version = 2"),
        )
        .await;

        // Should have 20 rows (keys 30-49 that were updated in v2)
        assert_eq!(result.num_rows(), 20);
        let updated_at = result[ROW_LAST_UPDATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        for i in 0..result.num_rows() {
            assert_eq!(updated_at[i], 2);
            assert!(keys[i] >= 30 && keys[i] < 50);
        }
    }

    #[tokio::test]
    async fn test_get_inserted_rows() {
        // Create initial dataset (version 1)
        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let ds = write_dataset_temp(&temp_dir, 0, 50, 1, "value", true, false).await;

        assert_eq!(ds.version().version, 1);

        // Append more data (version 2)
        let ds = write_dataset_temp(&temp_dir, 50, 30, 1, "appended_v2", true, true).await;

        assert_eq!(ds.version().version, 2);

        // Append more data (version 3)
        let ds = write_dataset_temp(&temp_dir, 80, 20, 1, "appended_v3", true, true).await;

        assert_eq!(ds.version().version, 3);

        // Test 1: Get all inserted rows between version 0 and 3
        let delta = ds
            .delta()
            .with_begin_version(0)
            .with_end_version(3)
            .build()
            .unwrap();

        let stream = delta.get_inserted_rows().await.unwrap();
        let result = collect_stream(stream).await;

        // Should have all 100 rows
        assert_eq!(result.num_rows(), 100);
        assert!(result.column_by_name(ROW_ID).is_some());
        assert!(result.column_by_name(ROW_CREATED_AT_VERSION).is_some());
        assert!(result.column_by_name(ROW_LAST_UPDATED_AT_VERSION).is_some());

        // Test 2: Get inserted rows between version 1 and 2
        let delta = ds
            .delta()
            .with_begin_version(1)
            .with_end_version(2)
            .build()
            .unwrap();

        let stream = delta.get_inserted_rows().await.unwrap();
        let result = collect_stream(stream).await;

        // Should have 30 rows (inserted in version 2)
        assert_eq!(result.num_rows(), 30);
        let created_at = result[ROW_CREATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        for i in 0..result.num_rows() {
            assert_eq!(created_at[i], 2);
            assert!(keys[i] >= 50 && keys[i] < 80);
        }

        // Test 3: Get inserted rows between version 2 and 3
        let delta = ds
            .delta()
            .with_begin_version(2)
            .with_end_version(3)
            .build()
            .unwrap();

        let stream = delta.get_inserted_rows().await.unwrap();
        let result = collect_stream(stream).await;

        // Should have 20 rows (inserted in version 3)
        assert_eq!(result.num_rows(), 20);
        let created_at = result[ROW_CREATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        for i in 0..result.num_rows() {
            assert_eq!(created_at[i], 3);
            assert!(keys[i] >= 80 && keys[i] < 100);
        }
    }

    #[tokio::test]
    async fn test_get_updated_rows() {
        // Create initial dataset (version 1)
        let ds = create_test_dataset(100, 1, "value", true).await;

        assert_eq!(ds.version().version, 1);

        // Update some rows (version 2)
        let ds = update_where(ds, "key < 30", "updated_v2").await;
        assert_eq!(ds.version().version, 2);

        // Update different rows (version 3)
        let ds = update_where(ds, "key >= 50 AND key < 70", "updated_v3").await;
        assert_eq!(ds.version().version, 3);

        // Update some rows again (version 4)
        let ds = update_where(ds, "key >= 10 AND key < 20", "updated_v4").await;
        assert_eq!(ds.version().version, 4);

        // Test 1: Get updated rows between version 1 and 2
        let delta = ds
            .delta()
            .with_begin_version(1)
            .with_end_version(2)
            .build()
            .unwrap();

        let stream = delta.get_updated_rows().await.unwrap();
        let result = collect_stream(stream).await;

        // Should have 20 rows (keys 0-9 and 20-29)
        // Note: keys 10-19 were updated in v2 but then updated again in v4,
        // so they have _row_last_updated_at_version = 4, not 2
        assert_eq!(result.num_rows(), 20);
        assert!(result.column_by_name(ROW_ID).is_some());
        assert!(result.column_by_name(ROW_CREATED_AT_VERSION).is_some());
        assert!(result.column_by_name(ROW_LAST_UPDATED_AT_VERSION).is_some());

        let created_at = result[ROW_CREATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let updated_at = result[ROW_LAST_UPDATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        for i in 0..result.num_rows() {
            assert_eq!(created_at[i], 1); // Created at version 1
            assert_eq!(updated_at[i], 2); // Updated at version 2
            // Keys should be in range [0, 30) but excluding [10, 20)
            assert!(keys[i] < 30);
            assert!(keys[i] < 10 || keys[i] >= 20);
        }

        // Test 2: Get updated rows between version 2 and 3
        let delta = ds
            .delta()
            .with_begin_version(2)
            .with_end_version(3)
            .build()
            .unwrap();

        let stream = delta.get_updated_rows().await.unwrap();
        let result = collect_stream(stream).await;

        // Should have 20 rows (keys 50-69)
        assert_eq!(result.num_rows(), 20);
        let updated_at = result[ROW_LAST_UPDATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        for i in 0..result.num_rows() {
            assert_eq!(updated_at[i], 3);
            assert!(keys[i] >= 50 && keys[i] < 70);
        }

        // Test 3: Get updated rows between version 1 and 4 (includes all updates)
        let delta = ds
            .delta()
            .with_begin_version(1)
            .with_end_version(4)
            .build()
            .unwrap();

        let stream = delta.get_updated_rows().await.unwrap();
        let result = collect_stream(stream).await;

        // Should have 50 rows total (30 from v2, 20 from v3, 10 from v4)
        // But some rows were updated twice, so we get unique rows
        assert_eq!(result.num_rows(), 50);
        let created_at = result[ROW_CREATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();

        for i in 0..result.num_rows() {
            assert_eq!(created_at[i], 1); // All created at version 1
        }
    }

    #[tokio::test]
    async fn test_get_upsert_rows() {
        // Create initial dataset (version 1)
        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let ds = write_dataset_temp(&temp_dir, 0, 50, 1, "value", true, false).await;

        assert_eq!(ds.version().version, 1);

        // Append inserted rows (version 2)
        let ds = write_dataset_temp(&temp_dir, 50, 20, 1, "appended_v2", true, true).await;
        assert_eq!(ds.version().version, 2);

        // Update some existing rows (version 3)
        let ds = update_where(ds, "key < 10", "updated_v3").await;
        assert_eq!(ds.version().version, 3);

        // Get upserted rows between version 1 and 3
        let delta = ds
            .delta()
            .with_begin_version(1)
            .with_end_version(3)
            .build()
            .unwrap();

        let stream = delta.get_upserted_rows().await.unwrap();
        let result = collect_stream(stream).await;

        // Should include 20 inserted rows (keys 50-69) and 10 updated rows (keys 0-9)
        assert_eq!(result.num_rows(), 30);
        assert!(result.column_by_name(ROW_ID).is_some());
        assert!(result.column_by_name(ROW_CREATED_AT_VERSION).is_some());
        assert!(result.column_by_name(ROW_LAST_UPDATED_AT_VERSION).is_some());

        let created_at = result[ROW_CREATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let updated_at = result[ROW_LAST_UPDATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values();
        let keys = result["key"].as_primitive::<Int32Type>().values();

        for i in 0..result.num_rows() {
            let key = keys[i];
            if key < 10 {
                // Updated rows from version 3
                assert_eq!(created_at[i], 1);
                assert_eq!(updated_at[i], 3);
            } else {
                // Inserted rows from version 2
                assert!((50..70).contains(&key));
                assert_eq!(created_at[i], 2);
                assert_eq!(updated_at[i], 2);
            }
        }
    }

    #[tokio::test]
    async fn test_build_with_date_window_basic() {
        MockClock::set_system_time(std::time::Duration::from_secs(10));
        let ds = create_test_dataset(50, 1, "v1", true).await;
        assert_eq!(ds.version().version, 1);

        MockClock::set_system_time(std::time::Duration::from_secs(20));
        let ds = update_where(ds, "key < 10", "v2").await;
        assert_eq!(ds.version().version, 2);

        MockClock::set_system_time(std::time::Duration::from_secs(30));
        let ds = update_where(ds, "key >= 10 AND key < 20", "v3").await;
        assert_eq!(ds.version().version, 3);

        let begin_ts = chrono::DateTime::<chrono::Utc>::from_timestamp(15, 0).unwrap();
        let end_ts = chrono::DateTime::<chrono::Utc>::from_timestamp(25, 0).unwrap();

        let delta = ds
            .delta()
            .with_begin_date(begin_ts)
            .with_end_date(end_ts)
            .build()
            .unwrap();

        let txs = delta.list_transactions().await.unwrap();
        assert_eq!(txs.len(), 1);
    }

    #[tokio::test]
    async fn test_build_with_date_window_edges() {
        MockClock::set_system_time(std::time::Duration::from_secs(100));
        let ds = create_test_dataset(10, 1, "v1", true).await;
        assert_eq!(ds.version().version, 1);

        MockClock::set_system_time(std::time::Duration::from_secs(200));
        let ds = update_where(ds, "key < 5", "v2").await;
        assert_eq!(ds.version().version, 2);

        let begin_ts = chrono::DateTime::<chrono::Utc>::from_timestamp(50, 0).unwrap();
        let end_ts = chrono::DateTime::<chrono::Utc>::from_timestamp(250, 0).unwrap();

        let delta = ds
            .delta()
            .with_begin_date(begin_ts)
            .with_end_date(end_ts)
            .build()
            .unwrap();

        let txs = delta.list_transactions().await.unwrap();
        assert_eq!(txs.len(), 2);
    }

    #[tokio::test]
    async fn test_build_with_date_open_end_uses_latest() {
        MockClock::set_system_time(std::time::Duration::from_secs(10));
        let ds = create_test_dataset(20, 1, "v1", true).await;
        assert_eq!(ds.version().version, 1);

        MockClock::set_system_time(std::time::Duration::from_secs(20));
        let ds = update_where(ds, "key < 5", "v2").await;
        assert_eq!(ds.version().version, 2);

        MockClock::set_system_time(std::time::Duration::from_secs(30));
        let ds = update_where(ds, "key >= 5 AND key < 10", "v3").await;
        assert_eq!(ds.version().version, 3);

        let begin_ts = chrono::DateTime::<chrono::Utc>::from_timestamp(15, 0).unwrap();

        let delta = ds.delta().with_begin_date(begin_ts).build().unwrap();

        let txs = delta.list_transactions().await.unwrap();
        // Should include transactions at v2 and v3
        assert_eq!(txs.len(), 2);
    }
}
