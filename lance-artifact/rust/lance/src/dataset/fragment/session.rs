// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::dataset::fragment::{
    FileFragment, FragReadConfig, FragmentReader, resolve_actual_row_ids,
};
use arrow_array::RecordBatch;
use lance_core::Result;
use lance_core::datatypes::Schema;
use std::borrow::Cow;
use std::sync::Arc;

/// A [`FragmentSession`] manages a short-lived session of [`FileFragment`], allowing us to maintain
/// internal states instead of creating new ones each time.
///
/// This API works well for users making repeated requests over the same projected schema.
#[derive(Debug)]
pub struct FragmentSession {
    reader: FragmentReader,
    sorted_deleted_ids: Option<Vec<u32>>,
}

impl FragmentSession {
    pub async fn open(
        fragment: Arc<FileFragment>,
        projection: &Schema,
        with_row_address: bool,
    ) -> Result<Self> {
        let reader = fragment
            .open(
                projection,
                FragReadConfig::default().with_row_address(with_row_address),
            )
            .await?;

        let sorted_deleted_ids = fragment.get_deletion_vector().await?.map(|dv| {
            let mut ids = dv.as_ref().clone().into_iter().collect::<Vec<_>>();
            ids.sort();
            ids
        });

        Ok(Self {
            reader,
            sorted_deleted_ids,
        })
    }

    pub async fn take(&self, indices: &[u32]) -> Result<RecordBatch> {
        // Remap row ids if needed
        let row_ids = if let Some(sorted_deleted_ids) = &self.sorted_deleted_ids {
            Cow::Owned(resolve_actual_row_ids(indices, sorted_deleted_ids))
        } else {
            Cow::Borrowed(indices)
        };

        // Then call take rows
        let batch = self.take_rows(&row_ids).await?;

        // Convert Lance JSON columns (LargeBinary/JSONB) back to Arrow JSON (Utf8)
        // for user-facing output, mirroring FileFragment::take.
        if batch
            .schema()
            .fields()
            .iter()
            .any(|f| lance_arrow::json::is_json_field(f) || lance_arrow::json::has_json_fields(f))
        {
            Ok(lance_arrow::json::convert_lance_json_to_arrow(&batch)?)
        } else {
            Ok(batch)
        }
    }

    pub(crate) async fn take_rows(&self, row_offsets: &[u32]) -> Result<RecordBatch> {
        if row_offsets.len() > 1 && FileFragment::row_ids_contiguous(row_offsets) {
            let range =
                (row_offsets[0] as usize)..(row_offsets[row_offsets.len() - 1] as usize + 1);
            self.reader.legacy_read_range_as_batch(range).await
        } else {
            self.reader.take_as_batch(row_offsets, None).await
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Dataset;
    use crate::dataset::WriteParams;
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use lance_core::ROW_ADDR;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_file::version::LanceFileVersion;
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_fragment_session_take_indices(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = create_dataset(test_uri, data_storage_version).await;
        let fragment = dataset
            .get_fragments()
            .into_iter()
            .find(|f| f.id() == 3)
            .unwrap();

        // Repeated indices are repeated in result.
        let take_session = fragment
            .open_session(dataset.schema(), false)
            .await
            .unwrap();
        let batch = take_session.take(&[1, 2, 4, 5, 5, 8]).await.unwrap();
        pretty_assertions::assert_eq!(
            batch.column_by_name("i").unwrap().as_ref(),
            &Int32Array::from(vec![121, 122, 124, 125, 125, 128])
        );

        dataset.delete("i in (122, 123, 125)").await.unwrap();
        dataset.validate().await.unwrap();

        // Deleted rows are skipped
        let fragment = dataset
            .get_fragments()
            .into_iter()
            .find(|f| f.id() == 3)
            .unwrap();
        let take_session = fragment
            .open_session(dataset.schema(), false)
            .await
            .unwrap();
        assert!(fragment.metadata().deletion_file.is_some());
        let batch = take_session.take(&[1, 2, 4, 5, 8]).await.unwrap();
        pretty_assertions::assert_eq!(
            batch.column_by_name("i").unwrap().as_ref(),
            &Int32Array::from(vec![121, 124, 127, 128, 131])
        );

        // Empty indices gives empty result
        let batch = take_session.take(&[]).await.unwrap();
        pretty_assertions::assert_eq!(
            batch.column_by_name("i").unwrap().as_ref(),
            &Int32Array::from(Vec::<i32>::new())
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_fragment_session_take_rows(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = create_dataset(test_uri, data_storage_version).await;
        let fragment = dataset
            .get_fragments()
            .into_iter()
            .find(|f| f.id() == 3)
            .unwrap();

        // Repeated indices are repeated in result.
        let take_session = fragment
            .open_session(dataset.schema(), false)
            .await
            .unwrap();
        let batch = take_session.take_rows(&[1, 2, 4, 5, 5, 8]).await.unwrap();
        pretty_assertions::assert_eq!(
            batch.column_by_name("i").unwrap().as_ref(),
            &Int32Array::from(vec![121, 122, 124, 125, 125, 128])
        );

        dataset.delete("i in (122, 124)").await.unwrap();
        dataset.validate().await.unwrap();

        // Cannot get rows 2 and 4 anymore
        let fragment = dataset
            .get_fragments()
            .into_iter()
            .find(|f| f.id() == 3)
            .unwrap();
        assert!(fragment.metadata().deletion_file.is_some());
        let take_session = fragment
            .open_session(dataset.schema(), false)
            .await
            .unwrap();
        let batch = take_session.take_rows(&[1, 2, 4, 5, 8]).await.unwrap();
        pretty_assertions::assert_eq!(
            batch.column_by_name("i").unwrap().as_ref(),
            &Int32Array::from(vec![121, 125, 128])
        );

        // Empty indices gives empty result
        let take_session = fragment
            .open_session(dataset.schema(), false)
            .await
            .unwrap();
        let batch = take_session.take_rows(&[]).await.unwrap();
        pretty_assertions::assert_eq!(
            batch.column_by_name("i").unwrap().as_ref(),
            &Int32Array::from(Vec::<i32>::new())
        );

        // Can get row ids
        let take_session = fragment.open_session(dataset.schema(), true).await.unwrap();
        let batch = take_session.take_rows(&[1, 2, 4, 5, 8]).await.unwrap();
        pretty_assertions::assert_eq!(
            batch.column_by_name("i").unwrap().as_ref(),
            &Int32Array::from(vec![121, 125, 128])
        );
        pretty_assertions::assert_eq!(
            batch.column_by_name(ROW_ADDR).unwrap().as_ref(),
            &UInt64Array::from(vec![(3 << 32) + 1, (3 << 32) + 5, (3 << 32) + 8])
        );
    }

    #[tokio::test]
    async fn test_fragment_session_take_json() {
        use arrow_array::LargeBinaryArray;
        use lance_arrow::ARROW_EXT_NAME_KEY;
        use lance_arrow::json::{ARROW_JSON_EXT_NAME, JsonArray, json_field};

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        // Build a schema with a Lance JSON column (LargeBinary + lance.json metadata).
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", DataType::Int32, true),
            json_field("j", true),
        ]));

        let json_strings = (0..10)
            .map(|v| Some(format!("{{\"v\":{}}}", v)))
            .collect::<Vec<_>>();
        let jsonb = JsonArray::try_from_iter(json_strings).unwrap().into_inner();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..10)),
                Arc::new(jsonb),
            ],
        )
        .unwrap();

        let write_params = WriteParams {
            data_storage_version: Some(LanceFileVersion::V2_2),
            ..Default::default()
        };
        let batches = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let dataset = Dataset::write(batches, test_uri, Some(write_params))
            .await
            .unwrap();

        let fragment = dataset.get_fragments().into_iter().next().unwrap();
        let take_session = fragment
            .open_session(dataset.schema(), false)
            .await
            .unwrap();
        let result = take_session.take(&[0, 3, 7]).await.unwrap();

        // The JSON column must be returned as Arrow JSON (Utf8 + arrow.json), not raw JSONB.
        let field = result.schema().field_with_name("j").unwrap().clone();
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert_eq!(
            field.metadata().get(ARROW_EXT_NAME_KEY).map(|s| s.as_str()),
            Some(ARROW_JSON_EXT_NAME)
        );
        assert!(
            result
                .column_by_name("j")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .is_none(),
            "JSON column should not be raw LargeBinary JSONB"
        );
        let json_col = result
            .column_by_name("j")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("JSON column should be Utf8 strings");
        assert_eq!(json_col.value(0), "{\"v\":0}");
        assert_eq!(json_col.value(1), "{\"v\":3}");
        assert_eq!(json_col.value(2), "{\"v\":7}");
    }

    async fn create_dataset(test_uri: &str, data_storage_version: LanceFileVersion) -> Dataset {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", DataType::Int32, true),
            ArrowField::new("s", DataType::Utf8, true),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int32Array::from_iter_values(i * 20..(i + 1) * 20)),
                        Arc::new(StringArray::from_iter_values(
                            (i * 20..(i + 1) * 20).map(|v| format!("s-{}", v)),
                        )),
                    ],
                )
                .unwrap()
            })
            .collect();

        let write_params = WriteParams {
            max_rows_per_file: 40,
            max_rows_per_group: 10,
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        };
        let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
        Dataset::write(batches, test_uri, Some(write_params))
            .await
            .unwrap();

        Dataset::open(test_uri).await.unwrap()
    }
}
