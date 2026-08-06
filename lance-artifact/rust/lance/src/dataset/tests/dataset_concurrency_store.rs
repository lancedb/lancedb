// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;
use std::vec;

use crate::dataset::WriteDestination;
use crate::{Dataset, Error, Result};

use crate::dataset::write::{WriteMode, WriteParams};
use crate::index::DatasetIndexExt;
use arrow_array::RecordBatch;
use arrow_array::{Int32Array, RecordBatchIterator};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance_core::utils::tempfile::TempStrDir;
use lance_index::{IndexType, scalar::ScalarIndexParams};

#[tokio::test]
async fn concurrent_create() {
    async fn write(uri: &str) -> Result<()> {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "a",
            DataType::Int32,
            false,
        )]));
        let empty_reader = RecordBatchIterator::new(vec![], schema.clone());
        Dataset::write(empty_reader, uri, None).await?;
        Ok(())
    }

    for _ in 0..5 {
        let test_uri = TempStrDir::default();

        let (res1, res2) = tokio::join!(write(&test_uri), write(&test_uri));

        assert!(res1.is_ok() || res2.is_ok());
        if res1.is_err() {
            assert!(
                matches!(res1, Err(Error::DatasetAlreadyExists { .. })),
                "{:?}",
                res1
            );
        } else if res2.is_err() {
            assert!(
                matches!(res2, Err(Error::DatasetAlreadyExists { .. })),
                "{:?}",
                res2
            );
        } else {
            assert!(res1.is_ok() && res2.is_ok());
        }
    }
}

#[tokio::test]
async fn test_limit_pushdown_in_physical_plan() -> Result<()> {
    use tempfile::tempdir;
    let temp_dir = tempdir()?;

    let dataset_path = temp_dir.path().join("limit_pushdown_dataset");
    let values: Vec<i32> = (0..1000).collect();
    let array = Int32Array::from(values);
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])?;

    let write_params = WriteParams {
        mode: WriteMode::Create,
        max_rows_per_file: 100,
        ..Default::default()
    };

    let batch_reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    Dataset::write(
        batch_reader,
        dataset_path.to_str().unwrap(),
        Some(write_params),
    )
    .await?;

    let mut dataset = Dataset::open(dataset_path.to_str().unwrap()).await?;

    dataset
        .create_index(
            &["value"],
            IndexType::Scalar,
            None,
            &ScalarIndexParams::default(),
            false,
        )
        .await?;

    // Test 1: No filter with limit
    {
        let mut scanner = dataset.scan();
        scanner.limit(Some(100), None)?;
        let plan = scanner.explain_plan(true).await?;

        assert!(plan.contains("range_before=Some(0..100)"));
        assert!(plan.contains("range_after=None"));

        let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(100, total_rows);
    }

    // Test 2: Indexed filter with limit
    {
        let mut scanner = dataset.scan();
        scanner.filter("value >= 500")?.limit(Some(50), None)?;
        let plan = scanner.explain_plan(true).await?;

        assert!(plan.contains("range_after=Some(0..50)"));
        assert!(plan.contains("range_before=None"));

        let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(50, total_rows);
    }

    // Test 3: Offset + Limit
    {
        let mut scanner = dataset.scan();
        scanner.filter("value < 500")?.limit(Some(30), Some(20))?;
        let plan = scanner.explain_plan(true).await?;

        assert!(plan.contains("GlobalLimitExec: skip=20, fetch=30"));
        assert!(plan.contains("range_after=Some(0..50)"));

        let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(30, total_rows);

        // Verify exact values (should be 20..50)
        let all_values: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("value")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(all_values, (20..50).collect::<Vec<i32>>());
    }

    // Test 4: Large limit exceeding data
    {
        let mut scanner = dataset.scan();
        scanner.limit(Some(5000), None)?;
        let plan = scanner.explain_plan(true).await?;

        assert!(plan.contains("range_before=Some(0..1000)"));

        let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(1000, total_rows);
    }

    // Test 5: Cross-fragment filter with limit
    {
        let mut scanner = dataset.scan();
        scanner
            .filter("value >= 95 AND value <= 205")?
            .limit(Some(50), None)?;
        let plan = scanner.explain_plan(true).await?;

        assert!(plan.contains("range_after=Some(0..50)"));

        let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(50, total_rows);
    }

    Ok(())
}

#[tokio::test]
async fn test_add_bases() {
    use lance_table::format::BasePath;
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32};
    use std::sync::Arc;

    // Create a test dataset
    let test_uri = "memory://add_bases_test";
    let mut data_gen =
        BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));

    let dataset = Dataset::write(
        data_gen.batch(5),
        test_uri,
        Some(WriteParams {
            mode: WriteMode::Create,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let dataset = Arc::new(dataset);

    // Test adding new base paths
    let new_bases = vec![
        BasePath::new(
            0,
            "memory://bucket1".to_string(),
            Some("bucket1".to_string()),
            false,
        ),
        BasePath::new(
            0,
            "memory://bucket2".to_string(),
            Some("bucket2".to_string()),
            true,
        ),
    ];

    let updated_dataset = dataset.add_bases(new_bases, None).await.unwrap();

    // Verify the base paths were added
    assert_eq!(updated_dataset.manifest.base_paths.len(), 2);

    let bucket1 = updated_dataset
        .manifest
        .base_paths
        .values()
        .find(|bp| bp.name == Some("bucket1".to_string()))
        .expect("bucket1 not found");
    let bucket2 = updated_dataset
        .manifest
        .base_paths
        .values()
        .find(|bp| bp.name == Some("bucket2".to_string()))
        .expect("bucket2 not found");

    assert_eq!(bucket1.path, "memory://bucket1");
    assert!(!bucket1.is_dataset_root);
    assert_eq!(bucket2.path, "memory://bucket2");
    assert!(bucket2.is_dataset_root);

    let updated_dataset = Arc::new(updated_dataset);

    // Test conflict detection - try to add a base with the same name
    let conflicting_bases = vec![BasePath::new(
        0,
        "memory://bucket3".to_string(),
        Some("bucket1".to_string()),
        false,
    )];

    let result = updated_dataset.add_bases(conflicting_bases, None).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Conflict detected")
    );

    // Test conflict detection - try to add a base with the same path
    let conflicting_bases = vec![BasePath::new(
        0,
        "memory://bucket1".to_string(),
        Some("bucket3".to_string()),
        false,
    )];

    let result = updated_dataset.add_bases(conflicting_bases, None).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Conflict detected")
    );
}

#[tokio::test]
async fn test_concurrent_add_bases_conflict() {
    use lance_table::format::BasePath;
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32};
    use std::sync::Arc;

    // Create a test dataset
    let test_uri = "memory://concurrent_add_bases_test";
    let mut data_gen =
        BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));

    let dataset = Dataset::write(
        data_gen.batch(5),
        test_uri,
        Some(WriteParams {
            mode: WriteMode::Create,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Clone the dataset to simulate concurrent access
    let dataset = Arc::new(dataset);
    let dataset_clone = Arc::new(dataset.clone());

    // First transaction adds base1
    let new_bases1 = vec![BasePath::new(
        0,
        "memory://bucket1".to_string(),
        Some("base1".to_string()),
        false,
    )];

    let updated_dataset = dataset.add_bases(new_bases1, None).await.unwrap();

    // Second transaction tries to add a different base (base2)
    // This should succeed as there's no conflict
    let new_bases2 = vec![BasePath::new(
        0,
        "memory://bucket2".to_string(),
        Some("base2".to_string()),
        false,
    )];

    let result = dataset_clone.add_bases(new_bases2, None).await;
    assert!(result.is_ok());

    // Verify both bases are present after conflict resolution
    let mut final_dataset = updated_dataset;
    final_dataset.checkout_latest().await.unwrap();
    assert_eq!(final_dataset.manifest.base_paths.len(), 2);

    let base1 = final_dataset
        .manifest
        .base_paths
        .values()
        .find(|bp| bp.name == Some("base1".to_string()));
    let base2 = final_dataset
        .manifest
        .base_paths
        .values()
        .find(|bp| bp.name == Some("base2".to_string()));

    assert!(base1.is_some());
    assert!(base2.is_some());
}

#[tokio::test]
async fn test_concurrent_add_bases_name_conflict() {
    use lance_table::format::BasePath;
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32};
    use std::sync::Arc;

    // Create a test dataset
    let test_uri = "memory://concurrent_name_conflict_test";
    let mut data_gen =
        BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));

    let dataset = Dataset::write(
        data_gen.batch(5),
        test_uri,
        Some(WriteParams {
            mode: WriteMode::Create,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Clone the dataset to simulate concurrent access
    let dataset_clone = dataset.clone();
    let dataset = Arc::new(dataset);
    let dataset_clone = Arc::new(dataset_clone);

    // First transaction adds base with name "shared_base"
    let new_bases1 = vec![BasePath::new(
        0,
        "memory://bucket1".to_string(),
        Some("shared_base".to_string()),
        false,
    )];

    let _updated_dataset = dataset.add_bases(new_bases1, None).await.unwrap();

    // Second transaction tries to add a different base with same name
    // This should fail due to name conflict
    let new_bases2 = vec![BasePath::new(
        0,
        "memory://bucket2".to_string(),
        Some("shared_base".to_string()),
        false,
    )];

    let result = dataset_clone.add_bases(new_bases2, None).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("incompatible with concurrent transaction")
    );
}

#[tokio::test]
async fn test_concurrent_add_bases_path_conflict() {
    use lance_table::format::BasePath;
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32};
    use std::sync::Arc;

    // Create a test dataset
    let test_uri = "memory://concurrent_path_conflict_test";
    let mut data_gen =
        BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));

    let dataset = Dataset::write(
        data_gen.batch(5),
        test_uri,
        Some(WriteParams {
            mode: WriteMode::Create,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Clone the dataset to simulate concurrent access
    let dataset_clone = dataset.clone();
    let dataset = Arc::new(dataset);
    let dataset_clone = Arc::new(dataset_clone);

    // First transaction adds base with path "memory://shared_path"
    let new_bases1 = vec![BasePath::new(
        0,
        "memory://shared_path".to_string(),
        Some("base1".to_string()),
        false,
    )];

    let _updated_dataset = dataset.add_bases(new_bases1, None).await.unwrap();

    // Second transaction tries to add a different base with same path
    // This should fail due to path conflict
    let new_bases2 = vec![BasePath::new(
        0,
        "memory://shared_path".to_string(),
        Some("base2".to_string()),
        false,
    )];

    let result = dataset_clone.add_bases(new_bases2, None).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("incompatible with concurrent transaction")
    );
}

#[tokio::test]
async fn test_concurrent_add_bases_with_data_write() {
    use lance_table::format::BasePath;
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32};
    use std::sync::Arc;

    // Create a test dataset
    let test_uri = "memory://concurrent_write_test";
    let mut data_gen =
        BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));

    let dataset = Dataset::write(
        data_gen.batch(5),
        test_uri,
        Some(WriteParams {
            mode: WriteMode::Create,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Clone the dataset to simulate concurrent access
    let dataset_clone = dataset.clone();
    let dataset = Arc::new(dataset);

    // First transaction adds a new base
    let new_bases = vec![BasePath::new(
        0,
        "memory://bucket1".to_string(),
        Some("base1".to_string()),
        false,
    )];

    let updated_dataset = dataset.add_bases(new_bases, None).await.unwrap();

    // Concurrent transaction appends data
    // This should succeed as add_bases doesn't conflict with data writes
    let result = Dataset::write(
        data_gen.batch(5),
        WriteDestination::Dataset(Arc::new(dataset_clone)),
        Some(WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        }),
    )
    .await;

    assert!(result.is_ok());

    // Verify both operations are reflected
    let mut final_dataset = updated_dataset;
    final_dataset.checkout_latest().await.unwrap();

    // Should have the new base
    assert_eq!(final_dataset.manifest.base_paths.len(), 1);
    assert!(
        final_dataset
            .manifest
            .base_paths
            .values()
            .any(|bp| bp.name == Some("base1".to_string()))
    );

    // Should have both data writes (10 rows total)
    assert_eq!(final_dataset.count_rows(None).await.unwrap(), 10);
}
