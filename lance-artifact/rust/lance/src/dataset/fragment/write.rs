// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_schema::Schema as ArrowSchema;
use datafusion::execution::SendableRecordBatchStream;
use futures::{StreamExt, TryStreamExt};
use lance_core::Error;
use lance_core::datatypes::Schema;
use lance_datafusion::chunker::{break_stream, chunk_stream};
use lance_datafusion::utils::StreamingWriteSource;
use lance_file::version::{ConcreteFileVersion, LanceFileVersion};
use lance_file::versions::v1::writer::FileWriter as V1FileWriter;
use lance_file::writer::FileWriter;
use lance_io::object_store::ObjectStore;
use lance_io::traits::Writer;
use lance_io::utils::CachedFileSize;
use lance_table::format::{DataFile, Fragment};
use lance_table::io::manifest::ManifestDescribing;
use std::borrow::Cow;
use std::sync::Arc;
use uuid::Uuid;

use crate::Result;
use crate::dataset::builder::DatasetBuilder;
use crate::dataset::utils::SchemaAdapter;
use crate::dataset::write::validate_and_resolve_target_bases_with_primary;
use crate::dataset::{DATA_DIR, Dataset, ReadParams, WriteMode, WriteParams};

/// Generates a filename optimized for S3 throughput using a UUID-based approach.
///
/// This approach follows Apache Iceberg's ObjectStoreLocationProvider pattern:
/// - Takes a UUID (16 bytes total)
/// - Uses first 3 bytes (24 bits) as binary string prefix for S3 distribution
/// - Uses remaining 13 bytes as hex string for uniqueness
///
/// Format: `<24-bit-binary><remaining-hex>`
/// Example: "101100101101010011010110a1b2c3d4e5f6g7h8i9j0"
///
/// We use binary instead of hex for the prefix because it helps S3 scale up on the
/// prefix faster with fewer throttling and retries. Binary provides maximum entropy per character
/// (1 bit) compared to hex (4 bits), allowing S3's internal partitioning to more
/// quickly recognize the access pattern and scale appropriately.
///
/// The binary prefix ensures files are distributed evenly across S3 prefixes,
/// minimizing throttling and maximizing throughput, while maintaining uniqueness.
pub(crate) fn generate_random_filename() -> String {
    let uuid = Uuid::new_v4();
    let bytes = uuid.as_bytes();

    let mut out = String::with_capacity(50);

    // Convert first 3 bytes to binary string (24 bits)
    for &b in &bytes[..3] {
        for i in (0..8).rev() {
            out.push(if (b >> i) & 1 == 1 { '1' } else { '0' });
        }
    }

    // Convert remaining 13 bytes to hex string (26 chars)
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &b in &bytes[3..] {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }

    out
}

/// Builder for writing a new fragment.
///
/// This builder can be re-used to write multiple fragments.
pub struct FragmentCreateBuilder<'a> {
    dataset_uri: &'a str,
    schema: Option<&'a Schema>,
    write_params: Option<&'a WriteParams>,
}

impl<'a> FragmentCreateBuilder<'a> {
    pub fn new(dataset_uri: &'a str) -> Self {
        Self {
            dataset_uri,
            schema: None,
            write_params: None,
        }
    }

    /// Set the schema of the fragment. If it is not known, it will be inferred.
    ///
    /// If the schema isn't provided, but the `write_mode` is `WriteMode::Append`,
    /// the schema will be inferred from the existing dataset.
    ///
    /// If that fails, the schema will be inferred from the first batch.
    pub fn schema(mut self, schema: &'a Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the write parameters.
    pub fn write_params(mut self, params: &'a WriteParams) -> Self {
        self.write_params = Some(params);
        self
    }

    /// Write a fragment.
    pub async fn write(
        &self,
        source: impl StreamingWriteSource,
        id: Option<u64>,
    ) -> Result<Fragment> {
        let (stream, schema) = self.get_stream_and_schema(Box::new(source)).await?;
        // Convert Arrow JSON columns (`arrow.json`, stored as Utf8) into Lance JSON
        // (`lance.json`, stored as JSONB-encoded LargeBinary) before writing. The
        // multi-fragment and dataset write paths perform this through `do_write_fragments`;
        // the single-fragment create path must do the same or the raw UTF-8 string bytes
        // would be written into a column whose schema declares JSONB, corrupting reads.
        let stream = SchemaAdapter::new(stream.schema()).to_physical_stream(stream);
        let version = self
            .write_params
            .map(|params| params.storage_version_or_default())
            .unwrap_or_else(|| ConcreteFileVersion::from(LanceFileVersion::Stable));
        crate::dataset::versions::write_fragment(
            version,
            self,
            stream,
            schema,
            id.unwrap_or_default(),
        )
        .await
    }

    /// Write multi fragment which separated by max_rows_per_file.
    pub async fn write_fragments(
        &self,
        source: impl StreamingWriteSource,
    ) -> Result<Vec<Fragment>> {
        let (stream, schema) = self.get_stream_and_schema(Box::new(source)).await?;
        self.write_fragments_v2_impl(stream, schema).await
    }

    pub(crate) async fn write_current_impl<F>(
        &self,
        create_writer: F,
        stream: SendableRecordBatchStream,
        schema: Schema,
        id: u64,
    ) -> Result<Fragment>
    where
        F: FnOnce(Box<dyn Writer>, Schema, String) -> Result<(FileWriter, DataFile)>,
    {
        let params = self.write_params.map(Cow::Borrowed).unwrap_or_default();
        let progress = params.progress.as_ref();

        Self::validate_schema(&schema, stream.schema().as_ref())?;

        let (object_store, base_path) = ObjectStore::from_uri_and_params(
            params.store_registry(),
            self.dataset_uri,
            &params.store_params.clone().unwrap_or_default(),
        )
        .await?;
        let data_file_key = generate_random_filename();
        let filename = format!("{}.lance", data_file_key);
        let mut fragment = Fragment::new(id);
        let full_path = base_path.clone().join(DATA_DIR).join(filename.clone());
        let obj_writer = object_store.create(&full_path).await?;
        let (mut writer, data_file) = create_writer(obj_writer, schema, filename)?;
        fragment.files.push(data_file);

        progress.begin(&fragment).await?;

        let break_limit = (128 * 1024).min(params.max_rows_per_file);

        let mut broken_stream = break_stream(stream, break_limit)
            .map_ok(|batch| vec![batch])
            .boxed();
        while let Some(batched_chunk) = broken_stream.next().await {
            let batch_chunk = batched_chunk?;
            writer.write_batches(batch_chunk.iter()).await?;
        }

        let write_summary = writer.finish().await?;
        fragment.physical_rows = Some(write_summary.num_rows as usize);

        if matches!(fragment.physical_rows, Some(0)) {
            return Err(Error::invalid_input("Input data was empty."));
        }

        let field_ids: Arc<[i32]> = writer
            .field_id_to_column_indices()
            .iter()
            .map(|(field_id, _)| *field_id as i32)
            .collect::<Vec<_>>()
            .into();
        let column_indices: Arc<[i32]> = writer
            .field_id_to_column_indices()
            .iter()
            .map(|(_, column_index)| *column_index as i32)
            .collect::<Vec<_>>()
            .into();

        fragment.files[0].fields = field_ids;
        fragment.files[0].column_indices = column_indices;
        fragment.files[0].file_size_bytes = CachedFileSize::new(write_summary.size_bytes);

        progress.complete(&fragment).await?;

        Ok(fragment)
    }
    async fn write_fragments_v2_impl(
        &self,
        stream: SendableRecordBatchStream,
        schema: Schema,
    ) -> Result<Vec<Fragment>> {
        let mut params = self.write_params.cloned().unwrap_or_default();

        Self::validate_schema(&schema, stream.schema().as_ref())?;

        let version = params.storage_version_or_default();
        let needs_existing_dataset = params.target_base_names_or_paths.is_some()
            || params.target_bases.is_some()
            || params.target_all_bases.is_some()
            || params.initial_bases.is_some();
        let existing_dataset = if needs_existing_dataset {
            self.existing_dataset(&params).await?
        } else {
            None
        };
        let existing_base_paths = existing_dataset
            .as_ref()
            .map(|dataset| &dataset.manifest.base_paths);
        let (object_store, base_path) = ObjectStore::from_uri_and_params(
            params.store_registry(),
            self.dataset_uri,
            &params.store_params.clone().unwrap_or_default(),
        )
        .await?;
        let target_bases_info = if needs_existing_dataset {
            validate_and_resolve_target_bases_with_primary(
                &mut params,
                existing_base_paths,
                &object_store,
                &base_path,
                self.dataset_uri,
            )
            .await?
        } else {
            None
        };
        crate::dataset::versions::write_fragments_direct(
            version,
            existing_dataset.as_ref(),
            object_store,
            &base_path,
            &schema,
            stream,
            params,
            target_bases_info,
            Vec::new(),
        )
        .await
    }

    pub(crate) async fn write_v1_impl(
        &self,
        stream: SendableRecordBatchStream,
        schema: Schema,
        id: u64,
    ) -> Result<Fragment> {
        let params = self.write_params.map(Cow::Borrowed).unwrap_or_default();
        let progress = params.progress.as_ref();

        Self::validate_schema(&schema, stream.schema().as_ref())?;

        let (object_store, base_path) = ObjectStore::from_uri_and_params(
            params.store_registry(),
            self.dataset_uri,
            &params.store_params.clone().unwrap_or_default(),
        )
        .await?;
        let filename = format!("{}.lance", generate_random_filename());
        let mut fragment = Fragment::with_file_legacy(id, &filename, &schema, None);
        let full_path = base_path.clone().join(DATA_DIR).join(filename.clone());
        let mut writer = V1FileWriter::<ManifestDescribing>::try_new(
            &object_store,
            &full_path,
            schema,
            &Default::default(),
        )
        .await?;

        progress.begin(&fragment).await?;

        let mut buffered_reader = chunk_stream(stream, params.max_rows_per_group);
        while let Some(batched_chunk) = buffered_reader.next().await {
            let batch = batched_chunk?;
            writer.write(&batch).await?;
        }

        if writer.is_empty() {
            return Err(Error::invalid_input("Input data was empty."));
        }

        fragment.physical_rows = Some(writer.finish().await?.num_rows as usize);

        progress.complete(&fragment).await?;

        Ok(fragment)
    }

    async fn get_stream_and_schema(
        &self,
        source: impl StreamingWriteSource,
    ) -> Result<(SendableRecordBatchStream, Schema)> {
        if let Some(schema) = self.schema {
            return Ok((source.into_stream(), schema.clone()));
        } else if matches!(self.write_params.map(|p| p.mode), Some(WriteMode::Append))
            && let Some(schema) = self.existing_dataset_schema().await?
        {
            return Ok((source.into_stream(), schema));
        }
        source.into_stream_and_schema().await
    }

    async fn existing_dataset_schema(&self) -> Result<Option<Schema>> {
        let mut builder = DatasetBuilder::from_uri(self.dataset_uri);
        let accessor = self
            .write_params
            .and_then(|p| p.store_params.as_ref())
            .and_then(|p| p.storage_options_accessor.clone());
        if let Some(accessor) = accessor {
            builder = builder.with_storage_options_accessor(accessor);
        }
        match builder.load().await {
            Ok(dataset) => {
                // Use the schema from the dataset, because it has the correct
                // field ids.
                Ok(Some(dataset.schema().clone()))
            }
            Err(Error::DatasetNotFound { .. }) => {
                // If the dataset does not exist, we can use the schema from
                // the reader.
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    async fn existing_dataset(&self, params: &WriteParams) -> Result<Option<Dataset>> {
        let mut builder = DatasetBuilder::from_uri(self.dataset_uri).with_read_params(ReadParams {
            store_options: params.store_params.clone(),
            commit_handler: params.commit_handler.clone(),
            session: params.session.clone(),
            ..Default::default()
        });
        if let Some(base_store_params) = &params.base_store_params {
            for (base_path, store_params) in base_store_params {
                builder = builder.with_base_store_params(base_path, store_params.clone());
            }
        }
        match builder.load().await {
            Ok(dataset) => Ok(Some(dataset)),
            Err(Error::DatasetNotFound { .. } | Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn validate_schema(expected: &Schema, actual: &ArrowSchema) -> Result<()> {
        if actual.fields().is_empty() {
            return Err(Error::invalid_input("Cannot write with an empty schema."));
        }
        let actual_lance = Schema::try_from(actual)?;
        actual_lance.check_compatible(expected, &Default::default())?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        Int64Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray,
    };
    use arrow_schema::{DataType, Field as ArrowField};
    use lance_arrow::SchemaExt;
    use lance_core::utils::tempfile::{TempDir, TempStrDir};
    use lance_table::format::BasePath;
    use rstest::rstest;

    use super::*;
    use crate::dataset::InsertBuilder;

    fn test_data() -> Box<dyn RecordBatchReader + Send> {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int64, false),
            ArrowField::new("b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        );
        Box::new(RecordBatchIterator::new(vec![batch], schema))
    }

    #[tokio::test]
    async fn test_fragment_write_validation() {
        // Writing with empty schema produces an error
        let empty_schema = Arc::new(ArrowSchema::empty());
        let empty_reader = Box::new(RecordBatchIterator::new(vec![], empty_schema));
        let tmp_dir = TempDir::default();
        let result = FragmentCreateBuilder::new(&tmp_dir.path_str())
            .write(empty_reader, None)
            .await;
        assert!(result.is_err());
        assert!(
            matches!(result.as_ref().unwrap_err(), Error::InvalidInput { source, .. }
            if source.to_string().contains("Cannot write with an empty schema.")),
            "{:?}",
            result
        );

        // Writing empty reader produces an error
        let arrow_schema = test_data().schema();
        let empty_reader = Box::new(RecordBatchIterator::new(vec![], arrow_schema.clone()));
        let result = FragmentCreateBuilder::new(tmp_dir.std_path().to_str().unwrap())
            .write(empty_reader, None)
            .await;
        assert!(result.is_err());
        assert!(
            matches!(result.as_ref().unwrap_err(), Error::InvalidInput { source, .. }
            if source.to_string().contains("Input data was empty.")),
            "{:?}",
            result
        );

        // Writing with incorrect schema produces an error.
        let wrong_schema = arrow_schema
            .as_ref()
            .try_with_column(ArrowField::new("c", DataType::Utf8, false))
            .unwrap();
        let wrong_schema = Schema::try_from(&wrong_schema).unwrap();
        let result = FragmentCreateBuilder::new(tmp_dir.std_path().to_str().unwrap())
            .schema(&wrong_schema)
            .write(test_data(), None)
            .await;
        assert!(result.is_err());
        assert!(
            matches!(result.as_ref().unwrap_err(), Error::SchemaMismatch { difference, .. }
            if difference.contains("fields did not match")),
            "{:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_fragment_write_default_schema() {
        // Infers schema and uses 0 as default field id
        let data = test_data();
        let tmp_dir = TempStrDir::default();
        let fragment = FragmentCreateBuilder::new(&tmp_dir)
            .write(data, None)
            .await
            .unwrap();

        // If unspecified, the fragment id should be 0.
        assert_eq!(fragment.id, 0);
        assert_eq!(fragment.deletion_file, None);
        assert_eq!(fragment.files.len(), 1);
        assert_eq!(fragment.files[0].fields.as_ref(), &[0, 1]);
    }

    #[tokio::test]
    async fn test_fragment_write_with_schema() {
        // Uses provided schema. Field ids are correct in fragment metadata.
        let data = test_data();

        let arrow_schema = data.schema();
        let mut custom_schema = Schema::try_from(arrow_schema.as_ref()).unwrap();
        custom_schema.mut_field_by_id(0).unwrap().id = 3;
        custom_schema.mut_field_by_id(1).unwrap().id = 1;

        let tmp_dir = TempStrDir::default();
        let fragment = FragmentCreateBuilder::new(&tmp_dir)
            .schema(&custom_schema)
            .write(data, Some(42))
            .await
            .unwrap();

        assert_eq!(fragment.id, 42);
        assert_eq!(fragment.deletion_file, None);
        assert_eq!(fragment.files.len(), 1);
        assert_eq!(fragment.files[0].fields.as_ref(), &[3, 1]);
        assert_eq!(fragment.files[0].column_indices.as_ref(), &[0, 1]);
    }

    #[tokio::test]
    async fn test_write_fragments_validation() {
        // Writing with empty schema produces an error
        let empty_schema = Arc::new(ArrowSchema::empty());
        let empty_reader = Box::new(RecordBatchIterator::new(vec![], empty_schema));
        let tmp_dir = TempDir::default();
        let result = FragmentCreateBuilder::new(&tmp_dir.path_str())
            .write_fragments(empty_reader)
            .await;
        assert!(result.is_err());
        assert!(
            matches!(result.as_ref().unwrap_err(), Error::InvalidInput { source, .. }
            if source.to_string().contains("Cannot write with an empty schema.")),
            "{:?}",
            result
        );

        // Writing empty reader produces an error
        let arrow_schema = test_data().schema();
        let empty_reader = Box::new(RecordBatchIterator::new(vec![], arrow_schema.clone()));
        let result = FragmentCreateBuilder::new(tmp_dir.std_path().to_str().unwrap())
            .write_fragments(empty_reader)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);

        // Writing with incorrect schema produces an error.
        let wrong_schema = arrow_schema
            .as_ref()
            .try_with_column(ArrowField::new("c", DataType::Utf8, false))
            .unwrap();
        let wrong_schema = Schema::try_from(&wrong_schema).unwrap();
        let result = FragmentCreateBuilder::new(tmp_dir.std_path().to_str().unwrap())
            .schema(&wrong_schema)
            .write_fragments(test_data())
            .await;
        assert!(result.is_err());
        assert!(
            matches!(result.as_ref().unwrap_err(), Error::SchemaMismatch { difference, .. }
            if difference.contains("fields did not match")),
            "{:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_write_fragments_default_schema() {
        // Infers schema and uses 0 as default field id
        let data = test_data();
        let tmp_dir = TempStrDir::default();
        let fragments = FragmentCreateBuilder::new(&tmp_dir)
            .write_fragments(data)
            .await
            .unwrap();

        // If unspecified, the fragment id should be 0.
        assert_eq!(fragments.len(), 1);
        assert_eq!(fragments[0].deletion_file, None);
        assert_eq!(fragments[0].files.len(), 1);
        assert_eq!(fragments[0].files[0].fields.as_ref(), &[0, 1]);
    }

    #[tokio::test]
    async fn test_write_fragments_with_options() {
        // Uses provided schema. Field ids are correct in fragment metadata.
        let data = test_data();
        let tmp_dir = TempStrDir::default();
        let writer_params = WriteParams {
            max_rows_per_file: 1,
            ..Default::default()
        };
        let fragments = FragmentCreateBuilder::new(&tmp_dir)
            .write_params(&writer_params)
            .write_fragments(data)
            .await
            .unwrap();

        assert_eq!(fragments.len(), 3);
        assert_eq!(fragments[0].deletion_file, None);
        assert_eq!(fragments[0].files.len(), 1);
        assert_eq!(fragments[0].files[0].column_indices.as_ref(), &[0, 1]);
        assert_eq!(fragments[1].deletion_file, None);
        assert_eq!(fragments[1].files.len(), 1);
        assert_eq!(fragments[1].files[0].column_indices.as_ref(), &[0, 1]);
        assert_eq!(fragments[2].deletion_file, None);
        assert_eq!(fragments[2].files.len(), 1);
        assert_eq!(fragments[2].files[0].column_indices.as_ref(), &[0, 1]);
    }

    #[tokio::test]
    async fn test_write_fragments_with_target_base() {
        let primary = TempStrDir::default();
        let base1 = TempStrDir::default();
        let base2 = TempStrDir::default();
        let create_params = WriteParams::default()
            .with_initial_bases(vec![
                BasePath::new(0, base1.to_string(), Some("base1".to_string()), false),
                BasePath::new(0, base2.to_string(), Some("base2".to_string()), false),
            ])
            .with_target_base_names_or_paths(vec!["base1".to_string()]);

        let dataset = InsertBuilder::new(primary.as_str())
            .with_params(&create_params)
            .execute_stream(test_data())
            .await
            .unwrap();

        let append_params = WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        }
        .with_target_base_names_or_paths(vec!["base2".to_string()]);
        let fragments = FragmentCreateBuilder::new(dataset.uri.as_str())
            .write_params(&append_params)
            .write_fragments(test_data())
            .await
            .unwrap();

        assert_eq!(fragments.len(), 1);
        assert_eq!(fragments[0].files[0].base_id, Some(2));
    }

    #[tokio::test]
    async fn test_write_fragments_with_target_all_bases() {
        let primary = TempStrDir::default();
        let base1 = TempStrDir::default();
        let base2 = TempStrDir::default();
        let create_params = WriteParams::default().with_initial_bases(vec![
            BasePath::new(0, base1.to_string(), Some("base1".to_string()), false),
            BasePath::new(0, base2.to_string(), Some("base2".to_string()), false),
        ]);

        let dataset = InsertBuilder::new(primary.as_str())
            .with_params(&create_params)
            .execute_stream(test_data())
            .await
            .unwrap();

        // Without primary, the first slot is the lowest registered base id.
        let append_params = WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        }
        .with_target_all_bases(false);
        let fragments = FragmentCreateBuilder::new(dataset.uri.as_str())
            .write_params(&append_params)
            .write_fragments(test_data())
            .await
            .unwrap();
        assert_eq!(fragments.len(), 1);
        assert_eq!(fragments[0].files[0].base_id, Some(1));

        // With primary included, the first slot is primary storage.
        let append_params = WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        }
        .with_target_all_bases(true);
        let fragments = FragmentCreateBuilder::new(dataset.uri.as_str())
            .write_params(&append_params)
            .write_fragments(test_data())
            .await
            .unwrap();
        assert_eq!(fragments.len(), 1);
        assert_eq!(fragments[0].files[0].base_id, None);
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_with_format_version(
        #[values(
            LanceFileVersion::V2_0,
            LanceFileVersion::V2_1,
            LanceFileVersion::V2_2,
            LanceFileVersion::Legacy,
            LanceFileVersion::Stable
        )]
        file_version: LanceFileVersion,
    ) {
        let data = test_data();
        let tmp_dir = TempStrDir::default();
        let writer_params = WriteParams {
            data_storage_version: Some(file_version),
            ..Default::default()
        };
        let fragment = FragmentCreateBuilder::new(&tmp_dir)
            .write_params(&writer_params)
            .write(data, None)
            .await
            .unwrap();

        assert!(!fragment.files.is_empty());
        fragment.files.iter().for_each(|f| {
            let (major_version, minor_version) =
                ConcreteFileVersion::from(file_version).to_data_file_numbers();
            assert_eq!(f.file_major_version, major_version);
            assert_eq!(f.file_minor_version, minor_version);
        })
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_fragments_with_format_version(
        #[values(
            LanceFileVersion::V2_0,
            LanceFileVersion::V2_1,
            LanceFileVersion::V2_2,
            LanceFileVersion::Legacy,
            LanceFileVersion::Stable
        )]
        file_version: LanceFileVersion,
    ) {
        let data = test_data();
        let tmp_dir = TempStrDir::default();
        let writer_params = WriteParams {
            data_storage_version: Some(file_version),
            ..Default::default()
        };
        let fragment = FragmentCreateBuilder::new(&tmp_dir)
            .write_params(&writer_params)
            .write_fragments(data)
            .await
            .unwrap();

        assert!(!fragment.is_empty());
        fragment[0].files.iter().for_each(|f| {
            let (major_version, minor_version) =
                ConcreteFileVersion::from(file_version).to_data_file_numbers();
            assert_eq!(f.file_major_version, major_version);
            assert_eq!(f.file_minor_version, minor_version);
        })
    }

    #[test]
    fn test_binary_filename_generation() {
        use std::collections::HashSet;

        // Test format and uniqueness
        let mut filenames = HashSet::new();
        for _ in 0..100 {
            let filename = generate_random_filename();

            // Should be 50 characters: 24 binary + 26 hex
            assert_eq!(filename.len(), 50, "Filename should be 50 characters");

            // First 24 should be binary
            let binary_part = &filename[0..24];
            assert!(
                binary_part.chars().all(|c| c == '0' || c == '1'),
                "First 24 chars should be binary: {}",
                binary_part
            );

            // Last 26 should be hex
            let hex_part = &filename[24..];
            assert_eq!(hex_part.len(), 26, "Hex part should be 26 characters");
            assert!(
                hex_part.chars().all(|c| c.is_ascii_hexdigit()),
                "Last 26 chars should be hex: {}",
                hex_part
            );

            // Should be unique
            assert!(filenames.insert(filename.clone()));
        }
    }
}
