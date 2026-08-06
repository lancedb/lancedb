// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::{RecordBatchIterator, record_batch};
use lance_file::version::LanceFileVersion;
use rstest::rstest;

use crate::Dataset;
use crate::dataset::WriteParams;
use crate::dataset::fragment::FileFragment;

/// Rewriting a column with `update_columns` tombstones the field in the file
/// that held it, leaving `-2` behind while a new file answers for it.
/// Validation has to accept that: a tombstone marks a superseded field, not a
/// corrupt one. Legacy files are checked by a separate sort rule, so both
/// storage versions are covered.
#[rstest]
#[case::legacy(LanceFileVersion::Legacy)]
#[case::stable(LanceFileVersion::Stable)]
#[tokio::test]
async fn test_validate_accepts_tombstoned_fields(#[case] version: LanceFileVersion) {
    let batch = record_batch!(("i", Int32, [1, 2]), ("v", Int64, [10, 20])).unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
    let dataset = Arc::new(
        Dataset::write(
            reader,
            "memory://",
            Some(WriteParams {
                data_storage_version: Some(version),
                ..Default::default()
            }),
        )
        .await
        .unwrap(),
    );

    let update = record_batch!(("i1", Int32, [1, 2]), ("v", Int64, [99, 99])).unwrap();
    let right = RecordBatchIterator::new(vec![Ok(update.clone())], update.schema());

    let mut fragment = dataset.get_fragments().into_iter().next().unwrap();
    let updated = fragment
        .update_columns_with_offsets(right, "i", "i1")
        .await
        .unwrap();

    let layout: Vec<Vec<i32>> = updated
        .fragment
        .files
        .iter()
        .map(|file| file.fields.as_ref().to_vec())
        .collect();
    assert_eq!(layout, vec![vec![0, -2], vec![1]]);

    FileFragment::new(dataset, updated.fragment)
        .validate()
        .await
        .unwrap();
}
