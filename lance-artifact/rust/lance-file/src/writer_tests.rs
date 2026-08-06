// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::reader::{FileReader, FileReaderOptions, ReaderProjection, describe_encoding};
    use crate::testing::FsFixture;
    use crate::version::ConcreteFileVersion;
    use crate::versions;
    use crate::writer::{ENV_LANCE_FILE_WRITER_MAX_PAGE_BYTES, FileWriter, FileWriterOptions};
    use arrow_array::builder::{Float32Builder, Int32Builder};
    use arrow_array::types::Float64Type;
    use arrow_array::{
        Array, ArrayRef, Int32Array, RecordBatch, RecordBatchReader, StringArray, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Field as ArrowField, Schema, Schema as ArrowSchema};
    use lance_core::cache::LanceCache;
    use lance_core::datatypes::Schema as LanceSchema;
    use lance_core::utils::tempfile::TempObjFile;
    use lance_datagen::{BatchCount, RowCount, array, gen_batch};
    use lance_encoding::compression_config::{CompressionFieldParams, CompressionParams};
    use lance_encoding::decoder::DecoderPlugins;
    use lance_io::object_store::ObjectStore;
    use lance_io::traits::Writer;
    use lance_io::utils::CachedFileSize;
    use rstest::rstest;

    fn create_writer(
        object_writer: Box<dyn Writer>,
        schema: LanceSchema,
        version: ConcreteFileVersion,
        options: FileWriterOptions,
    ) -> lance_core::Result<FileWriter> {
        versions::create_writer(version, object_writer, schema, options)
    }

    fn create_v2_1_writer_with_compression(
        object_writer: Box<dyn Writer>,
        schema: LanceSchema,
        options: FileWriterOptions,
        compression: CompressionParams,
    ) -> lance_core::Result<FileWriter> {
        versions::v2_1::create_writer_with_compression(object_writer, schema, options, compression)
            .map(Into::into)
    }

    fn reader_projection_from_column_names(
        version: ConcreteFileVersion,
        schema: &LanceSchema,
        column_names: &[&str],
    ) -> lance_core::Result<ReaderProjection> {
        versions::reader_projection_from_column_names(version, schema, column_names)
    }

    #[tokio::test]
    async fn test_basic_write() {
        let tmp_path = TempObjFile::default();
        let obj_store = Arc::new(ObjectStore::local());

        let reader = gen_batch()
            .col("score", array::rand::<Float64Type>())
            .into_reader_rows(RowCount::from(1000), BatchCount::from(10));

        let writer = obj_store.create(&tmp_path).await.unwrap();

        let lance_schema =
            lance_core::datatypes::Schema::try_from(reader.schema().as_ref()).unwrap();

        let mut file_writer = create_writer(
            writer,
            lance_schema,
            ConcreteFileVersion::V2_1,
            FileWriterOptions::default(),
        )
        .unwrap();

        for batch in reader {
            file_writer.write_batch(&batch.unwrap()).await.unwrap();
        }
        file_writer.add_schema_metadata("foo", "bar");
        file_writer.finish().await.unwrap();
        // Tests asserting the contents of the written file are in reader.rs
    }

    #[tokio::test]
    async fn test_write_empty() {
        let tmp_path = TempObjFile::default();
        let obj_store = Arc::new(ObjectStore::local());

        let reader = gen_batch()
            .col("score", array::rand::<Float64Type>())
            .into_reader_rows(RowCount::from(0), BatchCount::from(0));

        let writer = obj_store.create(&tmp_path).await.unwrap();

        let lance_schema =
            lance_core::datatypes::Schema::try_from(reader.schema().as_ref()).unwrap();

        let mut file_writer = create_writer(
            writer,
            lance_schema,
            ConcreteFileVersion::V2_1,
            FileWriterOptions::default(),
        )
        .unwrap();

        for batch in reader {
            file_writer.write_batch(&batch.unwrap()).await.unwrap();
        }
        file_writer.add_schema_metadata("foo", "bar");
        file_writer.finish().await.unwrap();
    }

    // Read a single column back at an explicit range/index set, returning its
    // `Int32` values. Reading one column (or an equal-length group) at a time is
    // how unequal-length files are consumed: a full scan across columns of
    // differing lengths cannot form a single rectangular batch.
    async fn read_int32_column(
        reader: &FileReader,
        schema: &LanceSchema,
        version: ConcreteFileVersion,
        name: &str,
        params: lance_io::ReadBatchParams,
    ) -> Vec<Option<i32>> {
        use futures::TryStreamExt;
        use lance_encoding::decoder::FilterExpression;

        let projection = reader_projection_from_column_names(version, schema, &[name]).unwrap();
        let batches: Vec<RecordBatch> = reader
            .read_stream_projected(params, 1024, 16, projection, FilterExpression::no_filter())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// A single file may hold columns of differing item counts, written by
    /// advancing each column's row counter independently (no shared global
    /// counter).
    #[rstest]
    #[tokio::test]
    async fn test_write_columns_unequal_lengths(
        #[values(ConcreteFileVersion::V2_0, ConcreteFileVersion::V2_1)]
        version: ConcreteFileVersion,
    ) {
        use lance_io::ReadBatchParams;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, true),
            ArrowField::new("b", DataType::Int32, true),
            ArrowField::new("c", DataType::Int32, true),
        ]));
        let lance_schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();

        let fs = FsFixture::default();
        let mut writer = create_writer(
            fs.object_store.create(&fs.tmp_path).await.unwrap(),
            lance_schema.clone(),
            version,
            FileWriterOptions::default(),
        )
        .unwrap();

        // Field "a" gets 5 values across two calls (appending), field "b" gets a
        // single value, and field "c" is never written (a zero-length column).
        let a1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![10]));
        writer.write_column(0, a1).await.unwrap();
        writer.write_column(1, b).await.unwrap();
        let a2: ArrayRef = Arc::new(Int32Array::from(vec![4, 5]));
        writer.write_column(0, a2).await.unwrap();
        // An empty array is a no-op whether or not the field already has rows:
        // field "a" keeps its 5 rows, field "c" stays a zero-length column.
        let empty: ArrayRef = Arc::new(Int32Array::from(Vec::<i32>::new()));
        writer.write_column(0, empty.clone()).await.unwrap();
        writer.write_column(2, empty).await.unwrap();

        let summary = writer.finish().await.unwrap();
        // The file's logical length is the longest column.
        assert_eq!(summary.num_rows, 5);

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        // Per-column row counts are recorded in / derivable from file metadata.
        assert_eq!(reader.num_rows(), 5);
        assert_eq!(reader.column_num_rows(0).unwrap(), 5);
        assert_eq!(reader.column_num_rows(1).unwrap(), 1);
        assert_eq!(reader.column_num_rows(2).unwrap(), 0);
        assert!(reader.column_num_rows(3).is_err());

        // Each column reads back independently at its own length.
        assert_eq!(
            read_int32_column(
                &reader,
                &lance_schema,
                version,
                "a",
                ReadBatchParams::Range(0..5)
            )
            .await,
            vec![Some(1), Some(2), Some(3), Some(4), Some(5)],
        );
        assert_eq!(
            read_int32_column(
                &reader,
                &lance_schema,
                version,
                "b",
                ReadBatchParams::Range(0..1)
            )
            .await,
            vec![Some(10)],
        );

        // Random access by position within the longer column returns the right
        // value even though other columns are shorter. (The take path requires
        // strictly increasing indices.)
        assert_eq!(
            read_int32_column(
                &reader,
                &lance_schema,
                version,
                "a",
                ReadBatchParams::Indices(arrow_array::UInt32Array::from(vec![0, 2, 4])),
            )
            .await,
            vec![Some(1), Some(3), Some(5)],
        );
    }

    /// Reading an unequal-length file:
    /// - a projection whose columns are equal length full-scans normally;
    /// - a full scan across columns of differing length is rejected up front,
    ///   before any batch is produced (even though a prefix would be rectangular);
    /// - a bounded read is valid as long as every projected column covers it;
    /// - a single-column `RangeFull` resolves to that column's own length, not
    ///   the file's (maximum) length.
    #[rstest]
    #[tokio::test]
    async fn test_read_unequal_length_projection(
        #[values(ConcreteFileVersion::V2_0, ConcreteFileVersion::V2_1)]
        version: ConcreteFileVersion,
    ) {
        use futures::TryStreamExt;
        use lance_encoding::decoder::FilterExpression;
        use lance_io::ReadBatchParams;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, true),
            ArrowField::new("b", DataType::Int32, true),
            ArrowField::new("c", DataType::Int32, true),
        ]));
        let lance_schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();
        let fs = FsFixture::default();
        let mut writer = create_writer(
            fs.object_store.create(&fs.tmp_path).await.unwrap(),
            lance_schema.clone(),
            version,
            FileWriterOptions::default(),
        )
        .unwrap();
        // "a" and "b" are equal length (5); "c" is shorter (1).
        writer
            .write_column(0, Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])))
            .await
            .unwrap();
        writer
            .write_column(1, Arc::new(Int32Array::from(vec![6, 7, 8, 9, 10])))
            .await
            .unwrap();
        writer
            .write_column(2, Arc::new(Int32Array::from(vec![100])))
            .await
            .unwrap();
        writer.finish().await.unwrap();

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let read = |names: &'static [&'static str], params: ReadBatchParams| {
            let projection =
                reader_projection_from_column_names(version, &lance_schema, names).unwrap();
            async {
                match reader
                    .read_stream_projected(
                        params,
                        1024,
                        16,
                        projection,
                        FilterExpression::no_filter(),
                    )
                    .await
                {
                    Ok(stream) => stream.try_collect::<Vec<RecordBatch>>().await,
                    Err(e) => Err(e),
                }
            }
        };
        let col_values = |batches: &[RecordBatch], idx: usize| -> Vec<Option<i32>> {
            batches
                .iter()
                .flat_map(|b| {
                    b.column(idx)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>()
                })
                .collect()
        };

        // Equal-length projection [a, b] full-scans into rectangular batches.
        let batches = read(&["a", "b"], ReadBatchParams::RangeFull).await.unwrap();
        assert_eq!(
            col_values(&batches, 0),
            vec![Some(1), Some(2), Some(3), Some(4), Some(5)]
        );
        assert_eq!(
            col_values(&batches, 1),
            vec![Some(6), Some(7), Some(8), Some(9), Some(10)]
        );

        // A mismatched-length projection [a, c] (5 vs 1) is rejected before any
        // batch is yielded, regardless of the read params — its columns cannot
        // be combined into rectangular batches. The error names each column's
        // length so the caller can see which column is the odd one out.
        let err = read(&["a", "c"], ReadBatchParams::RangeFull)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("a=5") && err.contains("c=1"),
            "error should name each column's length, got: {err}"
        );
        assert!(
            read(&["a", "c"], ReadBatchParams::Range(0..1))
                .await
                .is_err(),
            "even a common-prefix read of unequal-length columns must error"
        );

        // A single-column RangeFull resolves to that column's own length.
        let batches = read(&["c"], ReadBatchParams::RangeFull).await.unwrap();
        assert_eq!(col_values(&batches, 0), vec![Some(100)]);
        let batches = read(&["a"], ReadBatchParams::RangeFull).await.unwrap();
        assert_eq!(
            col_values(&batches, 0),
            vec![Some(1), Some(2), Some(3), Some(4), Some(5)]
        );

        // RangeFrom/RangeTo likewise resolve against the projected column's own
        // length rather than the file's longest column.
        let batches = read(&["a"], ReadBatchParams::RangeFrom(2..)).await.unwrap();
        assert_eq!(col_values(&batches, 0), vec![Some(3), Some(4), Some(5)]);
        // RangeFrom on the short column "c" resolves to length 1, not 5.
        let batches = read(&["c"], ReadBatchParams::RangeFrom(0..)).await.unwrap();
        assert_eq!(col_values(&batches, 0), vec![Some(100)]);
        let batches = read(&["a"], ReadBatchParams::RangeTo(..3)).await.unwrap();
        assert_eq!(col_values(&batches, 0), vec![Some(1), Some(2), Some(3)]);
        // A bound past the projected column's length errors.
        assert!(
            read(&["a"], ReadBatchParams::RangeTo(..6)).await.is_err(),
            "RangeTo past the column length must error"
        );
        assert!(
            read(&["c"], ReadBatchParams::RangeFrom(2..)).await.is_err(),
            "RangeFrom past the column length must error"
        );
    }

    /// A struct and a list column each map to multiple physical columns, and a
    /// list's item column is longer than its top-level row count. The
    /// projection-length check must partition `column_indices` by top-level
    /// field and use each field's root column, so an ordinary (rectangular) file
    /// with nested columns still reads under the new validation path.
    #[rstest]
    #[tokio::test]
    async fn test_read_nested_columns_under_validation(
        #[values(ConcreteFileVersion::V2_0, ConcreteFileVersion::V2_1)]
        version: ConcreteFileVersion,
    ) {
        use arrow_array::types::Int32Type;
        use arrow_array::{ListArray, StructArray};
        use futures::TryStreamExt;
        use lance_encoding::decoder::FilterExpression;
        use lance_io::ReadBatchParams;

        let struct_type = DataType::Struct(
            vec![
                ArrowField::new("x", DataType::Int32, true),
                ArrowField::new("y", DataType::Int32, true),
            ]
            .into(),
        );
        let list_type = DataType::List(Arc::new(ArrowField::new("item", DataType::Int32, true)));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, true),
            ArrowField::new("s", struct_type, true),
            ArrowField::new("lst", list_type, true),
        ]));
        let lance_schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let s: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(ArrowField::new("x", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef,
            ),
            (
                Arc::new(ArrowField::new("y", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![11, 21, 31])) as ArrayRef,
            ),
        ]));
        // 3 lists, 6 items: the item column is longer than the top-level rows.
        let lst: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
            Some(vec![Some(4), Some(5), Some(6)]),
        ]));
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![a, s, lst]).unwrap();

        let fs = FsFixture::default();
        let mut writer = create_writer(
            fs.object_store.create(&fs.tmp_path).await.unwrap(),
            lance_schema.clone(),
            version,
            FileWriterOptions::default(),
        )
        .unwrap();
        writer.write_batch(&batch).await.unwrap();
        writer.finish().await.unwrap();

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        // If `validate_field_length` mispartitioned the physical columns, the
        // length check would read the wrong root column (e.g. the list's item
        // column, length 6) and spuriously reject this rectangular file.
        for names in [&["a", "s", "lst"][..], &["a", "lst"][..], &["a", "s"][..]] {
            let projection =
                reader_projection_from_column_names(version, &lance_schema, names).unwrap();
            let batches: Vec<RecordBatch> = reader
                .read_stream_projected(
                    ReadBatchParams::RangeFull,
                    1024,
                    16,
                    projection,
                    FilterExpression::no_filter(),
                )
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                total_rows, 3,
                "projection {names:?} should read 3 top-level rows"
            );
        }
    }

    /// `write_column` rejects invalid inputs at the API boundary with
    /// descriptive errors: a writer without an explicit schema, an
    /// out-of-bounds field index, and a null written into a non-nullable field.
    #[tokio::test]
    async fn test_write_column_validation_errors() {
        // A lazy-schema writer cannot infer the schema from a single column.
        let fs = FsFixture::default();
        let mut lazy_writer = versions::v2_1::create_lazy_writer(
            fs.object_store.create(&fs.tmp_path).await.unwrap(),
            FileWriterOptions::default(),
        );
        let err = lazy_writer
            .write_column(0, Arc::new(Int32Array::from(vec![1, 2, 3])))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("explicit schema"),
            "expected explicit-schema error, got: {err}"
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new("b", DataType::Int32, true),
        ]));
        let lance_schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();

        // An out-of-bounds field index is rejected, naming the index and count.
        let fs = FsFixture::default();
        let mut writer = create_writer(
            fs.object_store.create(&fs.tmp_path).await.unwrap(),
            lance_schema.clone(),
            ConcreteFileVersion::V2_1,
            FileWriterOptions::default(),
        )
        .unwrap();
        let err = writer
            .write_column(5, Arc::new(Int32Array::from(vec![1])))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains('5') && err.contains('2'),
            "expected out-of-bounds error naming index 5 and 2 fields, got: {err}"
        );

        // A null in a non-nullable field ("a") is rejected.
        let err = writer
            .write_column(0, Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("non-null"),
            "expected nullability error, got: {err}"
        );
    }

    /// The blocking read path applies the same projection-length validation as
    /// the async path: a short single column resolves to its own length, and a
    /// mismatched-length projection errors up front.
    #[rstest]
    #[tokio::test]
    async fn test_blocking_read_unequal_length(
        #[values(ConcreteFileVersion::V2_0, ConcreteFileVersion::V2_1)]
        version: ConcreteFileVersion,
    ) {
        use lance_encoding::decoder::FilterExpression;
        use lance_io::ReadBatchParams;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, true),
            ArrowField::new("c", DataType::Int32, true),
        ]));
        let lance_schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();
        let fs = FsFixture::default();
        let mut writer = create_writer(
            fs.object_store.create(&fs.tmp_path).await.unwrap(),
            lance_schema.clone(),
            version,
            FileWriterOptions::default(),
        )
        .unwrap();
        writer
            .write_column(0, Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])))
            .await
            .unwrap();
        writer
            .write_column(1, Arc::new(Int32Array::from(vec![100])))
            .await
            .unwrap();
        writer.finish().await.unwrap();

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let reader = Arc::new(
            FileReader::try_open(
                file_scheduler,
                None,
                Arc::<DecoderPlugins>::default(),
                &LanceCache::no_cache(),
                FileReaderOptions::default(),
            )
            .await
            .unwrap(),
        );

        // Single short column: RangeFull resolves to its own length (1).
        let proj_c = reader_projection_from_column_names(version, &lance_schema, &["c"]).unwrap();
        let reader_c = reader.clone();
        let batches = tokio::task::spawn_blocking(move || {
            reader_c
                .read_stream_projected_blocking(
                    ReadBatchParams::RangeFull,
                    1024,
                    Some(proj_c),
                    FilterExpression::no_filter(),
                )
                .unwrap()
                .collect::<std::result::Result<Vec<RecordBatch>, _>>()
                .unwrap()
        })
        .await
        .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);

        // A mismatched projection [a, c] errors on the blocking path too.
        let proj_ac =
            reader_projection_from_column_names(version, &lance_schema, &["a", "c"]).unwrap();
        let reader_ac = reader.clone();
        let is_err = tokio::task::spawn_blocking(move || {
            reader_ac
                .read_stream_projected_blocking(
                    ReadBatchParams::RangeFull,
                    1024,
                    Some(proj_ac),
                    FilterExpression::no_filter(),
                )
                .is_err()
        })
        .await
        .unwrap();
        assert!(
            is_err,
            "blocking full scan across unequal-length columns must error"
        );
    }

    /// Files written the ordinary (rectangular) way keep equal column lengths,
    /// so the unequal-length support is backwards compatible.
    #[tokio::test]
    async fn test_write_batch_keeps_equal_lengths() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, true),
            ArrowField::new("b", DataType::Int32, true),
        ]));
        let lance_schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();

        let fs = FsFixture::default();
        let mut writer = create_writer(
            fs.object_store.create(&fs.tmp_path).await.unwrap(),
            lance_schema,
            ConcreteFileVersion::V2_1,
            FileWriterOptions::default(),
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
            ],
        )
        .unwrap();
        writer.write_batch(&batch).await.unwrap();
        let summary = writer.finish().await.unwrap();
        assert_eq!(summary.num_rows, 3);

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(reader.column_num_rows(0).unwrap(), 3);
        assert_eq!(reader.column_num_rows(1).unwrap(), 3);
    }

    #[tokio::test]
    async fn test_max_page_bytes_enforced() {
        let arrow_field = Field::new("data", DataType::UInt64, false);
        let arrow_schema = Schema::new(vec![arrow_field]);
        let lance_schema = LanceSchema::try_from(&arrow_schema).unwrap();

        // 8MiB
        let data: Vec<u64> = (0..1_000_000).collect();
        let array = UInt64Array::from(data);
        let batch =
            RecordBatch::try_new(arrow_schema.clone().into(), vec![Arc::new(array)]).unwrap();

        let options = FileWriterOptions {
            max_page_bytes: Some(1024 * 1024), // 1MB
            ..Default::default()
        };

        let path = TempObjFile::default();
        let object_store = ObjectStore::local();
        let mut writer = create_writer(
            object_store.create(&path).await.unwrap(),
            lance_schema,
            ConcreteFileVersion::V2_0,
            options,
        )
        .unwrap();

        writer.write_batch(&batch).await.unwrap();
        writer.finish().await.unwrap();

        let fs = FsFixture::default();
        let file_scheduler = fs
            .scheduler
            .open_file(&path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let column_meta = file_reader.metadata();

        let mut total_page_num: u32 = 0;
        for (col_idx, col_metadata) in column_meta.column_metadatas.iter().enumerate() {
            assert!(
                !col_metadata.pages.is_empty(),
                "Column {} has no pages",
                col_idx
            );

            for (page_idx, page) in col_metadata.pages.iter().enumerate() {
                total_page_num += 1;
                let total_size: u64 = page.buffer_sizes.iter().sum();
                assert!(
                    total_size <= 1024 * 1024,
                    "Column {} Page {} size {} exceeds 1MB limit",
                    col_idx,
                    page_idx,
                    total_size
                );
            }
        }

        assert_eq!(total_page_num, 8)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_max_page_bytes_env_var() {
        let arrow_field = Field::new("data", DataType::UInt64, false);
        let arrow_schema = Schema::new(vec![arrow_field]);
        let lance_schema = LanceSchema::try_from(&arrow_schema).unwrap();
        // 4MiB
        let data: Vec<u64> = (0..500_000).collect();
        let array = UInt64Array::from(data);
        let batch =
            RecordBatch::try_new(arrow_schema.clone().into(), vec![Arc::new(array)]).unwrap();

        // 2MiB
        unsafe {
            std::env::set_var(ENV_LANCE_FILE_WRITER_MAX_PAGE_BYTES, "2097152");
        }

        let options = FileWriterOptions {
            max_page_bytes: None, // enforce env
            ..Default::default()
        };

        let path = TempObjFile::default();
        let object_store = ObjectStore::local();
        let mut writer = create_writer(
            object_store.create(&path).await.unwrap(),
            lance_schema.clone(),
            ConcreteFileVersion::V2_1,
            options,
        )
        .unwrap();

        writer.write_batch(&batch).await.unwrap();
        writer.finish().await.unwrap();

        let fs = FsFixture::default();
        let file_scheduler = fs
            .scheduler
            .open_file(&path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        for col_metadata in file_reader.metadata().column_metadatas.iter() {
            for page in col_metadata.pages.iter() {
                let total_size: u64 = page.buffer_sizes.iter().sum();
                assert!(
                    total_size <= 2 * 1024 * 1024,
                    "Page size {} exceeds 2MB limit",
                    total_size
                );
            }
        }

        unsafe {
            std::env::set_var(ENV_LANCE_FILE_WRITER_MAX_PAGE_BYTES, "");
        }
    }

    #[tokio::test]
    async fn test_compression_overrides_end_to_end() {
        // Create test schema with different column types
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("customer_id", DataType::Int32, false),
            ArrowField::new("product_id", DataType::Int32, false),
            ArrowField::new("quantity", DataType::Int32, false),
            ArrowField::new("price", DataType::Float32, false),
            ArrowField::new("description", DataType::Utf8, false),
        ]));

        let lance_schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();

        // Create test data with patterns suitable for different compression
        let mut customer_ids = Int32Builder::new();
        let mut product_ids = Int32Builder::new();
        let mut quantities = Int32Builder::new();
        let mut prices = Float32Builder::new();
        let mut descriptions = Vec::new();

        // Generate data with specific patterns:
        // - customer_id: highly repetitive (good for RLE)
        // - product_id: moderately repetitive (good for RLE)
        // - quantity: random values (not good for RLE)
        // - price: some repetition
        // - description: long strings (good for Zstd)
        for i in 0..10000 {
            // Customer ID repeats every 100 rows (100 unique customers)
            // This creates runs of 100 identical values
            customer_ids.append_value(i / 100);

            // Product ID has only 5 unique values with long runs
            product_ids.append_value(i / 2000);

            // Quantity is mostly 1 with occasional other values
            quantities.append_value(if i % 10 == 0 { 5 } else { 1 });

            // Price has only 3 unique values
            prices.append_value(match i % 3 {
                0 => 9.99,
                1 => 19.99,
                _ => 29.99,
            });

            // Descriptions are repetitive but we'll keep them simple
            descriptions.push(format!("Product {}", i / 2000));
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(customer_ids.finish()),
                Arc::new(product_ids.finish()),
                Arc::new(quantities.finish()),
                Arc::new(prices.finish()),
                Arc::new(StringArray::from(descriptions)),
            ],
        )
        .unwrap();

        // Configure compression parameters
        let mut params = CompressionParams::new();

        // RLE for ID columns (ends with _id)
        params.columns.insert(
            "*_id".to_string(),
            CompressionFieldParams {
                rle_threshold: Some(0.5), // Lower threshold to trigger RLE more easily
                compression: None,        // Will use default compression if any
                compression_level: None,
                bss: Some(lance_encoding::compression_config::BssMode::Off), // Explicitly disable BSS to ensure RLE is used
                minichunk_size: None,
            },
        );

        // For now, we'll skip Zstd compression since it's not imported
        // In a real implementation, you could add other compression types here

        // Configure file writer options
        let options = FileWriterOptions {
            max_page_bytes: Some(64 * 1024), // 64KB pages
            ..Default::default()
        };

        // Write the file
        let path = TempObjFile::default();
        let object_store = ObjectStore::local();

        let mut writer = create_v2_1_writer_with_compression(
            object_store.create(&path).await.unwrap(),
            lance_schema.clone(),
            options,
            params,
        )
        .unwrap();

        writer.write_batch(&batch).await.unwrap();
        writer.add_schema_metadata("compression_test", "configured_compression");
        writer.finish().await.unwrap();

        // Now write the same data without compression overrides for comparison
        let path_no_compression = TempObjFile::default();
        let default_options = FileWriterOptions {
            max_page_bytes: Some(64 * 1024),
            ..Default::default()
        };

        let mut writer_no_compression = create_writer(
            object_store.create(&path_no_compression).await.unwrap(),
            lance_schema.clone(),
            ConcreteFileVersion::V2_1,
            default_options,
        )
        .unwrap();

        writer_no_compression.write_batch(&batch).await.unwrap();
        writer_no_compression.finish().await.unwrap();

        // Note: With our current data patterns and RLE compression, the compressed file
        // might actually be slightly larger due to compression metadata overhead.
        // This is expected and the test is mainly to verify the system works end-to-end.

        // Read back the compressed file and verify data integrity
        let fs = FsFixture::default();
        let file_scheduler = fs
            .scheduler
            .open_file(&path, &CachedFileSize::unknown())
            .await
            .unwrap();

        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        // Verify metadata
        let metadata = file_reader.metadata();
        assert_eq!(metadata.version(), ConcreteFileVersion::V2_1);

        let schema = file_reader.schema();
        assert_eq!(
            schema.metadata.get("compression_test"),
            Some(&"configured_compression".to_string())
        );

        // Verify the actual encodings used
        let column_metadatas = &metadata.column_metadatas;

        // Check customer_id column (index 0) - should use RLE due to our configuration
        assert!(!column_metadatas[0].pages.is_empty());
        let customer_id_encoding = describe_encoding(&column_metadatas[0].pages[0]);
        assert!(
            customer_id_encoding.contains("RLE") || customer_id_encoding.contains("Rle"),
            "customer_id column should use RLE encoding due to '*_id' pattern match, but got: {}",
            customer_id_encoding
        );

        // Check product_id column (index 1) - should use RLE due to our configuration
        assert!(!column_metadatas[1].pages.is_empty());
        let product_id_encoding = describe_encoding(&column_metadatas[1].pages[0]);
        assert!(
            product_id_encoding.contains("RLE") || product_id_encoding.contains("Rle"),
            "product_id column should use RLE encoding due to '*_id' pattern match, but got: {}",
            product_id_encoding
        );
    }

    #[tokio::test]
    async fn test_field_metadata_compression() {
        // Test that field metadata compression settings are respected
        let mut metadata = HashMap::new();
        metadata.insert(
            lance_encoding::constants::COMPRESSION_META_KEY.to_string(),
            "zstd".to_string(),
        );
        metadata.insert(
            lance_encoding::constants::COMPRESSION_LEVEL_META_KEY.to_string(),
            "6".to_string(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int32, false),
            ArrowField::new("text", DataType::Utf8, false).with_metadata(metadata.clone()),
            ArrowField::new("data", DataType::Int32, false).with_metadata(HashMap::from([(
                lance_encoding::constants::COMPRESSION_META_KEY.to_string(),
                "none".to_string(),
            )])),
        ]));

        let lance_schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();

        // Create test data
        let id_array = Int32Array::from_iter_values(0..1000);
        let text_array = StringArray::from_iter_values(
            (0..1000).map(|i| format!("test string {} repeated text", i)),
        );
        let data_array = Int32Array::from_iter_values((0..1000).map(|i| i * 2));

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(text_array),
                Arc::new(data_array),
            ],
        )
        .unwrap();

        let path = TempObjFile::default();
        let object_store = ObjectStore::local();

        // Create encoding strategy that will read from field metadata
        let params = CompressionParams::new();
        let options = FileWriterOptions::default();
        let mut writer = create_v2_1_writer_with_compression(
            object_store.create(&path).await.unwrap(),
            lance_schema.clone(),
            options,
            params,
        )
        .unwrap();

        writer.write_batch(&batch).await.unwrap();
        writer.finish().await.unwrap();

        // Read back metadata
        let fs = FsFixture::default();
        let file_scheduler = fs
            .scheduler
            .open_file(&path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let column_metadatas = &file_reader.metadata().column_metadatas;

        // The text column (index 1) should use zstd compression based on metadata
        let text_encoding = describe_encoding(&column_metadatas[1].pages[0]);
        // For string columns, we expect Binary encoding with zstd compression
        assert!(
            text_encoding.contains("Zstd"),
            "text column should use zstd compression from field metadata, but got: {}",
            text_encoding
        );

        // The data column (index 2) should use no compression based on metadata
        let data_encoding = describe_encoding(&column_metadatas[2].pages[0]);
        // For Int32 columns with "none" compression, we expect Flat encoding without compression
        assert!(
            data_encoding.contains("Flat") && data_encoding.contains("compression: None"),
            "data column should use no compression from field metadata, but got: {}",
            data_encoding
        );
    }

    #[tokio::test]
    async fn test_field_metadata_rle_threshold() {
        // Test that RLE threshold from field metadata is respected
        let mut metadata = HashMap::new();
        metadata.insert(
            lance_encoding::constants::RLE_THRESHOLD_META_KEY.to_string(),
            "0.9".to_string(),
        );
        // Also set compression to ensure RLE is used
        metadata.insert(
            lance_encoding::constants::COMPRESSION_META_KEY.to_string(),
            "lz4".to_string(),
        );
        // Explicitly disable BSS to ensure RLE is tested
        metadata.insert(
            lance_encoding::constants::BSS_META_KEY.to_string(),
            "off".to_string(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("status", DataType::Int32, false).with_metadata(metadata),
        ]));

        let lance_schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();

        // Create data with very high repetition (3 runs for 10000 values = 0.0003 ratio)
        let status_array = Int32Array::from_iter_values(
            std::iter::repeat_n(200, 8000)
                .chain(std::iter::repeat_n(404, 1500))
                .chain(std::iter::repeat_n(500, 500)),
        );

        let batch =
            RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(status_array)]).unwrap();

        let path = TempObjFile::default();
        let object_store = ObjectStore::local();

        // Create encoding strategy that will read from field metadata
        let params = CompressionParams::new();
        let options = FileWriterOptions::default();
        let mut writer = create_v2_1_writer_with_compression(
            object_store.create(&path).await.unwrap(),
            lance_schema.clone(),
            options,
            params,
        )
        .unwrap();

        writer.write_batch(&batch).await.unwrap();
        writer.finish().await.unwrap();

        // Read back and check encoding
        let fs = FsFixture::default();
        let file_scheduler = fs
            .scheduler
            .open_file(&path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let column_metadatas = &file_reader.metadata().column_metadatas;
        let status_encoding = describe_encoding(&column_metadatas[0].pages[0]);
        assert!(
            status_encoding.contains("RLE") || status_encoding.contains("Rle"),
            "status column should use RLE encoding due to metadata threshold, but got: {}",
            status_encoding
        );
    }

    #[tokio::test]
    async fn test_large_page_split_on_read() {
        use arrow_array::Array;
        use futures::TryStreamExt;
        use lance_encoding::decoder::FilterExpression;
        use lance_io::ReadBatchParams;

        // Test that large pages written with relaxed limits can be split during read

        let arrow_field = ArrowField::new("data", DataType::Binary, false);
        let arrow_schema = ArrowSchema::new(vec![arrow_field]);
        let lance_schema = LanceSchema::try_from(&arrow_schema).unwrap();

        // Create a large binary value (40MB) to trigger large page creation
        let large_value = vec![42u8; 40 * 1024 * 1024];
        let array = arrow_array::BinaryArray::from(vec![
            Some(large_value.as_slice()),
            Some(b"small value"),
        ]);
        let batch = RecordBatch::try_new(Arc::new(arrow_schema), vec![Arc::new(array)]).unwrap();

        // Write with relaxed page size limit (128MB)
        let options = FileWriterOptions {
            max_page_bytes: Some(128 * 1024 * 1024),
            ..Default::default()
        };

        let fs = FsFixture::default();
        let path = fs.tmp_path;

        let mut writer = create_writer(
            fs.object_store.create(&path).await.unwrap(),
            lance_schema.clone(),
            ConcreteFileVersion::V2_1,
            options,
        )
        .unwrap();

        writer.write_batch(&batch).await.unwrap();
        let write_summary = writer.finish().await.unwrap();
        assert_eq!(write_summary.num_rows, 2);
        assert_eq!(
            write_summary.size_bytes,
            fs.object_store.size(&path).await.unwrap()
        );

        // Read back with split configuration
        let file_scheduler = fs
            .scheduler
            .open_file(&path, &CachedFileSize::unknown())
            .await
            .unwrap();

        // Configure reader to split pages larger than 10MB into chunks
        let reader_options = FileReaderOptions {
            read_chunk_size: 10 * 1024 * 1024, // 10MB chunks
            ..Default::default()
        };

        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            reader_options,
        )
        .await
        .unwrap();

        // Read the data back
        let stream = file_reader
            .read_stream(
                ReadBatchParams::RangeFull,
                1024,
                10, // batch_readahead
                FilterExpression::no_filter(),
            )
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        assert_eq!(batches.len(), 1);

        // Verify the data is correctly read despite splitting
        let read_array = batches[0].column(0);
        let read_binary = read_array
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();

        assert_eq!(read_binary.len(), 2);
        assert_eq!(read_binary.value(0).len(), 40 * 1024 * 1024);
        assert_eq!(read_binary.value(1), b"small value");

        // Verify first value matches what we wrote
        assert!(read_binary.value(0).iter().all(|&b| b == 42u8));
    }

    fn spill_config() -> (TempObjFile, Arc<ObjectStore>) {
        let spill_path = TempObjFile::default();
        (spill_path, Arc::new(ObjectStore::local()))
    }

    fn make_batches(num_batches: i32, num_cols: usize, rows_per_batch: i32) -> Vec<RecordBatch> {
        let fields: Vec<_> = (0..num_cols)
            .map(|c| ArrowField::new(format!("c{c}"), DataType::Int32, false))
            .collect();
        let schema = Arc::new(ArrowSchema::new(fields));
        (0..num_batches)
            .map(|i| {
                let cols: Vec<Arc<dyn arrow_array::Array>> = (0..num_cols)
                    .map(|c| {
                        let start = (i * rows_per_batch + c as i32) * 100;
                        Arc::new(Int32Array::from_iter_values(start..start + rows_per_batch))
                            as Arc<dyn arrow_array::Array>
                    })
                    .collect();
                RecordBatch::try_new(schema.clone(), cols).unwrap()
            })
            .collect()
    }

    async fn write_and_read_batches(
        batches: &[RecordBatch],
        spill: Option<(Arc<ObjectStore>, object_store::path::Path)>,
    ) -> Vec<RecordBatch> {
        let fs = FsFixture::default();
        let lance_schema = LanceSchema::try_from(batches[0].schema().as_ref()).unwrap();
        let writer = fs.object_store.create(&fs.tmp_path).await.unwrap();
        let mut file_writer = create_writer(
            writer,
            lance_schema,
            ConcreteFileVersion::V2_1,
            FileWriterOptions::default(),
        )
        .unwrap();
        if let Some((store, path)) = spill {
            file_writer = file_writer.with_page_metadata_spill(store, path);
        }
        for batch in batches {
            file_writer.write_batch(batch).await.unwrap();
        }
        file_writer.add_schema_metadata("foo", "bar");
        file_writer.finish().await.unwrap();

        crate::testing::read_lance_file(
            &fs,
            Arc::<DecoderPlugins>::default(),
            lance_encoding::decoder::FilterExpression::no_filter(),
        )
        .await
    }

    #[rstest::rstest]
    #[case::multi_col(20, 2, 100)]
    #[case::many_batches(50, 2, 100)]
    #[tokio::test]
    async fn test_page_metadata_spill_roundtrip(
        #[case] num_batches: i32,
        #[case] num_cols: usize,
        #[case] rows_per_batch: i32,
    ) {
        let batches = make_batches(num_batches, num_cols, rows_per_batch);
        let baseline = write_and_read_batches(&batches, None).await;
        let (spill_path, spill_store) = spill_config();
        let spilled =
            write_and_read_batches(&batches, Some((spill_store, spill_path.as_ref().clone())))
                .await;
        assert_eq!(baseline, spilled);
    }

    #[tokio::test]
    async fn test_page_metadata_spill_many_columns() {
        // Many columns forces small per-column buffer limits, exercising mid-write flushing.
        let batches = make_batches(10, 500, 100);
        let baseline = write_and_read_batches(&batches, None).await;
        let (spill_path, spill_store) = spill_config();
        let spilled =
            write_and_read_batches(&batches, Some((spill_store, spill_path.as_ref().clone())))
                .await;
        assert_eq!(baseline, spilled);
    }
}
