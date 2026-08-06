// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

mod statistics;

use std::collections::HashMap;
use std::marker::PhantomData;

use arrow_array::builder::{ArrayBuilder, PrimitiveBuilder};
use arrow_array::cast::{as_large_list_array, as_list_array, as_struct_array};
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow_buffer::ArrowNativeType;
use arrow_data::ArrayData;
use arrow_schema::DataType;
use async_recursion::async_recursion;
use async_trait::async_trait;
use lance_arrow::*;
use lance_core::datatypes::{Encoding, Field, NullabilityComparison, Schema, SchemaCompareOptions};
use lance_core::{Error, Result};
use lance_io::object_store::ObjectStore;
use lance_io::traits::{WriteExt, Writer};
use object_store::path::Path;
use tokio::io::AsyncWriteExt;

use crate::format::{MAGIC, MAJOR_VERSION, MINOR_VERSION};
use crate::versions::v1::encoding::{
    binary::BinaryEncoder, dictionary::DictionaryEncoder, plain::PlainEncoder,
    write_schema_dictionaries,
};
use crate::versions::v1::format::metadata::{Metadata, StatisticsMetadata};
use crate::versions::v1::page_table::{PageInfo, PageTable};
use crate::writer::FileWriteSummary;

/// The file format currently includes a "manifest" where it stores the schema for
/// self-describing files.  Historically this has been a table format manifest that
/// is empty except for the schema field.
///
/// Since this crate is not aware of the table format we need this to be provided
/// externally.  You should always use lance_table::io::manifest::ManifestDescribing
/// for this today.
#[async_trait]
pub trait ManifestProvider {
    /// Store the schema in the file
    ///
    /// This should just require writing the schema (or a manifest wrapper) as a proto struct
    ///
    /// Note: the dictionaries have already been written by this point and the schema should
    /// be populated with the dictionary lengths/offsets
    async fn store_schema(object_writer: &mut dyn Writer, schema: &Schema)
    -> Result<Option<usize>>;
}

/// Implementation of ManifestProvider that does not store the schema
#[cfg(test)]
pub(crate) struct NotSelfDescribing {}

#[cfg(test)]
#[async_trait]
impl ManifestProvider for NotSelfDescribing {
    async fn store_schema(_: &mut dyn Writer, _: &Schema) -> Result<Option<usize>> {
        Ok(None)
    }
}

/// [FileWriter] writes Arrow [RecordBatch] to one Lance file.
///
/// ```ignored
/// use lance::io::FileWriter;
/// use futures::stream::Stream;
///
/// let mut file_writer = FileWriter::new(object_store, &path, &schema);
/// while let Ok(batch) = stream.next().await {
///     file_writer.write(&batch).unwrap();
/// }
/// // Need to close file writer to flush buffer and footer.
/// file_writer.shutdown();
/// ```
pub struct FileWriter<M: ManifestProvider + Send + Sync> {
    pub object_writer: Box<dyn Writer>,
    schema: Schema,
    batch_id: i32,
    page_table: PageTable,
    metadata: Metadata,
    stats_collector: Option<statistics::StatisticsCollector>,
    manifest_provider: PhantomData<M>,
}

#[derive(Debug, Clone, Default)]
pub struct FileWriterOptions {
    /// The field ids to collect statistics for.
    ///
    /// If None, will collect for all fields in the schema (that support stats).
    /// If an empty vector, will not collect any statistics.
    pub collect_stats_for_fields: Option<Vec<i32>>,
}

impl<M: ManifestProvider + Send + Sync> FileWriter<M> {
    pub async fn try_new(
        object_store: &ObjectStore,
        path: &Path,
        schema: Schema,
        options: &FileWriterOptions,
    ) -> Result<Self> {
        let object_writer = object_store.create(path).await?;
        Self::with_object_writer(object_writer, schema, options)
    }

    pub fn with_object_writer(
        object_writer: Box<dyn Writer>,
        schema: Schema,
        options: &FileWriterOptions,
    ) -> Result<Self> {
        let collect_stats_for_fields = if let Some(stats_fields) = &options.collect_stats_for_fields
        {
            stats_fields.clone()
        } else {
            schema.field_ids()
        };

        let stats_collector = if !collect_stats_for_fields.is_empty() {
            let stats_schema = schema.project_by_ids(&collect_stats_for_fields, true);
            statistics::StatisticsCollector::try_new(&stats_schema)
        } else {
            None
        };

        Ok(Self {
            object_writer,
            schema,
            batch_id: 0,
            page_table: PageTable::default(),
            metadata: Metadata::default(),
            stats_collector,
            manifest_provider: PhantomData,
        })
    }

    /// Return the schema of the file writer.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    fn verify_field_nullability(arr: &ArrayData, field: &Field) -> Result<()> {
        if !field.nullable && arr.null_count() > 0 {
            return Err(Error::invalid_input(format!(
                "The field `{}` contained null values even though the field is marked non-null in the schema",
                field.name
            )));
        }

        for (child_field, child_arr) in field.children.iter().zip(arr.child_data()) {
            Self::verify_field_nullability(child_arr, child_field)?;
        }

        Ok(())
    }

    fn verify_nullability_constraints(&self, batch: &RecordBatch) -> Result<()> {
        for (col, field) in batch.columns().iter().zip(self.schema.fields.iter()) {
            Self::verify_field_nullability(&col.to_data(), field)?;
        }
        Ok(())
    }

    /// Write a [RecordBatch] to the open file.
    /// All RecordBatch will be treated as one RecordBatch on disk
    ///
    /// Returns [Err] if the schema does not match with the batch.
    pub async fn write(&mut self, batches: &[RecordBatch]) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        for batch in batches {
            // Compare, ignore metadata and dictionary
            //   dictionary should have been checked earlier and could be an expensive check
            let schema = Schema::try_from(batch.schema().as_ref())?;
            schema.check_compatible(
                &self.schema,
                &SchemaCompareOptions {
                    compare_nullability: NullabilityComparison::Ignore,
                    ..Default::default()
                },
            )?;
            self.verify_nullability_constraints(batch)?;
        }

        // If we are collecting stats for this column, collect them.
        // Statistics need to traverse nested arrays, so it's a separate loop
        // from writing which is done on top-level arrays.
        if let Some(stats_collector) = &mut self.stats_collector {
            for (field, arrays) in fields_in_batches(batches, &self.schema) {
                if let Some(stats_builder) = stats_collector.get_builder(field.id) {
                    let stats_row = statistics::collect_statistics(&arrays);
                    stats_builder.append(stats_row);
                }
            }
        }

        // Copy a list of fields to avoid borrow checker error.
        let fields = self.schema.fields.clone();
        for field in fields.iter() {
            let arrs = batches
                .iter()
                .map(|batch| {
                    batch.column_by_name(&field.name).ok_or_else(|| {
                        Error::invalid_input(format!(
                            "FileWriter::write: Field '{}' not found",
                            field.name
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            Self::write_array(
                self.object_writer.as_mut(),
                field,
                &arrs,
                self.batch_id,
                &mut self.page_table,
            )
            .await?;
        }
        let batch_length = batches.iter().map(|b| b.num_rows() as i32).sum();
        self.metadata.push_batch_length(batch_length);

        // It's imperative we complete any in-flight requests, since we are
        // returning control to the caller. If the caller takes a long time to
        // write the next batch, the in-flight requests will not be polled and
        // may time out.
        self.object_writer.flush().await?;

        self.batch_id += 1;
        Ok(())
    }

    /// Add schema metadata, as (key, value) pair to the file.
    pub fn add_metadata(&mut self, key: &str, value: &str) {
        self.schema
            .metadata
            .insert(key.to_string(), value.to_string());
    }

    pub async fn finish_with_metadata(
        &mut self,
        metadata: &HashMap<String, String>,
    ) -> Result<FileWriteSummary> {
        self.schema
            .metadata
            .extend(metadata.iter().map(|(k, y)| (k.clone(), y.clone())));
        self.finish().await
    }

    pub async fn finish(&mut self) -> Result<FileWriteSummary> {
        self.write_footer().await?;
        // `shutdown` flushes the footer and reports the authoritative on-disk
        // byte count, so the size is sourced here instead of via a later
        // `tell()` that would rely on the cursor surviving shutdown.
        let write_result = Writer::shutdown(self.object_writer.as_mut()).await?;
        let num_rows = self
            .metadata
            .batch_offsets
            .last()
            .cloned()
            .unwrap_or_default();
        Ok(FileWriteSummary {
            num_rows: num_rows as u64,
            size_bytes: write_result.size as u64,
        })
    }

    /// Total records written in this file.
    pub fn len(&self) -> usize {
        self.metadata.len()
    }

    /// Total bytes written so far
    pub async fn tell(&mut self) -> Result<usize> {
        self.object_writer.tell().await
    }

    /// Return the id of the next batch to be written.
    pub fn next_batch_id(&self) -> i32 {
        self.batch_id
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[async_recursion]
    async fn write_array(
        object_writer: &mut dyn Writer,
        field: &Field,
        arrs: &[&ArrayRef],
        batch_id: i32,
        page_table: &mut PageTable,
    ) -> Result<()> {
        assert!(!arrs.is_empty());
        let data_type = arrs[0].data_type();
        let arrs_ref = arrs.iter().map(|a| a.as_ref()).collect::<Vec<_>>();

        match data_type {
            DataType::Null => {
                Self::write_null_array(
                    object_writer,
                    field,
                    arrs_ref.as_slice(),
                    batch_id,
                    page_table,
                )
                .await
            }
            dt if dt.is_fixed_stride() => {
                Self::write_fixed_stride_array(
                    object_writer,
                    field,
                    arrs_ref.as_slice(),
                    batch_id,
                    page_table,
                )
                .await
            }
            dt if dt.is_binary_like() => {
                Self::write_binary_array(
                    object_writer,
                    field,
                    arrs_ref.as_slice(),
                    batch_id,
                    page_table,
                )
                .await
            }
            DataType::Dictionary(key_type, _) => {
                Self::write_dictionary_arr(
                    object_writer,
                    field,
                    arrs_ref.as_slice(),
                    key_type,
                    batch_id,
                    page_table,
                )
                .await
            }
            dt if dt.is_struct() => {
                let struct_arrays = arrs.iter().map(|a| as_struct_array(a)).collect::<Vec<_>>();
                Self::write_struct_array(
                    object_writer,
                    field,
                    struct_arrays.as_slice(),
                    batch_id,
                    page_table,
                )
                .await
            }
            DataType::FixedSizeList(_, _) | DataType::FixedSizeBinary(_) => {
                Self::write_fixed_stride_array(
                    object_writer,
                    field,
                    arrs_ref.as_slice(),
                    batch_id,
                    page_table,
                )
                .await
            }
            DataType::List(_) => {
                Self::write_list_array(
                    object_writer,
                    field,
                    arrs_ref.as_slice(),
                    batch_id,
                    page_table,
                )
                .await
            }
            DataType::LargeList(_) => {
                Self::write_large_list_array(
                    object_writer,
                    field,
                    arrs_ref.as_slice(),
                    batch_id,
                    page_table,
                )
                .await
            }
            _ => Err(Error::schema(format!(
                "FileWriter::write: unsupported data type: {data_type}"
            ))),
        }
    }

    async fn write_null_array(
        object_writer: &mut dyn Writer,
        field: &Field,
        arrs: &[&dyn Array],
        batch_id: i32,
        page_table: &mut PageTable,
    ) -> Result<()> {
        let arrs_length: i32 = arrs.iter().map(|a| a.len() as i32).sum();
        let page_info = PageInfo::new(object_writer.tell().await?, arrs_length as usize);
        page_table.set(field.id, batch_id, page_info);
        Ok(())
    }

    /// Write fixed size array, including, primtiives, fixed size binary, and fixed size list.
    async fn write_fixed_stride_array(
        object_writer: &mut dyn Writer,
        field: &Field,
        arrs: &[&dyn Array],
        batch_id: i32,
        page_table: &mut PageTable,
    ) -> Result<()> {
        assert_eq!(field.encoding, Some(Encoding::Plain));
        assert!(!arrs.is_empty());
        let data_type = arrs[0].data_type();

        let mut encoder = PlainEncoder::new(object_writer, data_type);
        let pos = encoder.encode(arrs).await?;
        let arrs_length: i32 = arrs.iter().map(|a| a.len() as i32).sum();
        let page_info = PageInfo::new(pos, arrs_length as usize);
        page_table.set(field.id, batch_id, page_info);
        Ok(())
    }

    /// Write var-length binary arrays.
    async fn write_binary_array(
        object_writer: &mut dyn Writer,
        field: &Field,
        arrs: &[&dyn Array],
        batch_id: i32,
        page_table: &mut PageTable,
    ) -> Result<()> {
        assert_eq!(field.encoding, Some(Encoding::VarBinary));
        let mut encoder = BinaryEncoder::new(object_writer);
        let pos = encoder.encode(arrs).await?;
        let arrs_length: i32 = arrs.iter().map(|a| a.len() as i32).sum();
        let page_info = PageInfo::new(pos, arrs_length as usize);
        page_table.set(field.id, batch_id, page_info);
        Ok(())
    }

    async fn write_dictionary_arr(
        object_writer: &mut dyn Writer,
        field: &Field,
        arrs: &[&dyn Array],
        key_type: &DataType,
        batch_id: i32,
        page_table: &mut PageTable,
    ) -> Result<()> {
        assert_eq!(field.encoding, Some(Encoding::Dictionary));

        // Write the dictionary keys.
        let mut encoder = DictionaryEncoder::new(object_writer, key_type);
        let pos = encoder.encode(arrs).await?;
        let arrs_length: i32 = arrs.iter().map(|a| a.len() as i32).sum();
        let page_info = PageInfo::new(pos, arrs_length as usize);
        page_table.set(field.id, batch_id, page_info);
        Ok(())
    }

    #[async_recursion]
    async fn write_struct_array(
        object_writer: &mut dyn Writer,
        field: &Field,
        arrays: &[&StructArray],
        batch_id: i32,
        page_table: &mut PageTable,
    ) -> Result<()> {
        arrays
            .iter()
            .for_each(|a| assert_eq!(a.num_columns(), field.children.len()));

        for child in &field.children {
            let mut arrs: Vec<&ArrayRef> = Vec::new();
            for struct_array in arrays {
                let arr = struct_array
                    .column_by_name(&child.name)
                    .ok_or(Error::schema(format!(
                        "FileWriter: schema mismatch: column {} does not exist in array: {:?}",
                        child.name,
                        struct_array.data_type()
                    )))?;
                arrs.push(arr);
            }
            Self::write_array(object_writer, child, arrs.as_slice(), batch_id, page_table).await?;
        }
        Ok(())
    }

    async fn write_list_array(
        object_writer: &mut dyn Writer,
        field: &Field,
        arrs: &[&dyn Array],
        batch_id: i32,
        page_table: &mut PageTable,
    ) -> Result<()> {
        let capacity: usize = arrs.iter().map(|a| a.len()).sum();
        let mut list_arrs: Vec<ArrayRef> = Vec::new();
        let mut pos_builder: PrimitiveBuilder<Int32Type> =
            PrimitiveBuilder::with_capacity(capacity);

        let mut last_offset: usize = 0;
        pos_builder.append_value(last_offset as i32);
        for array in arrs.iter() {
            let list_arr = as_list_array(*array);
            let offsets = list_arr.value_offsets();

            assert!(!offsets.is_empty());
            let start_offset = offsets[0].as_usize();
            let end_offset = offsets[offsets.len() - 1].as_usize();

            let list_values = list_arr.values();
            let sliced_values = list_values.slice(start_offset, end_offset - start_offset);
            list_arrs.push(sliced_values);

            offsets
                .iter()
                .skip(1)
                .map(|b| b.as_usize() - start_offset + last_offset)
                .for_each(|o| pos_builder.append_value(o as i32));
            last_offset = pos_builder.values_slice()[pos_builder.len() - 1_usize] as usize;
        }

        let positions: &dyn Array = &pos_builder.finish();
        Self::write_fixed_stride_array(object_writer, field, &[positions], batch_id, page_table)
            .await?;
        let arrs = list_arrs.iter().collect::<Vec<_>>();
        Self::write_array(
            object_writer,
            &field.children[0],
            arrs.as_slice(),
            batch_id,
            page_table,
        )
        .await
    }

    async fn write_large_list_array(
        object_writer: &mut dyn Writer,
        field: &Field,
        arrs: &[&dyn Array],
        batch_id: i32,
        page_table: &mut PageTable,
    ) -> Result<()> {
        let capacity: usize = arrs.iter().map(|a| a.len()).sum();
        let mut list_arrs: Vec<ArrayRef> = Vec::new();
        let mut pos_builder: PrimitiveBuilder<Int64Type> =
            PrimitiveBuilder::with_capacity(capacity);

        let mut last_offset: usize = 0;
        pos_builder.append_value(last_offset as i64);
        for array in arrs.iter() {
            let list_arr = as_large_list_array(*array);
            let offsets = list_arr.value_offsets();

            assert!(!offsets.is_empty());
            let start_offset = offsets[0].as_usize();
            let end_offset = offsets[offsets.len() - 1].as_usize();

            let sliced_values = list_arr
                .values()
                .slice(start_offset, end_offset - start_offset);
            list_arrs.push(sliced_values);

            offsets
                .iter()
                .skip(1)
                .map(|b| b.as_usize() - start_offset + last_offset)
                .for_each(|o| pos_builder.append_value(o as i64));
            last_offset = pos_builder.values_slice()[pos_builder.len() - 1_usize] as usize;
        }

        let positions: &dyn Array = &pos_builder.finish();
        Self::write_fixed_stride_array(object_writer, field, &[positions], batch_id, page_table)
            .await?;
        let arrs = list_arrs.iter().collect::<Vec<_>>();
        Self::write_array(
            object_writer,
            &field.children[0],
            arrs.as_slice(),
            batch_id,
            page_table,
        )
        .await
    }

    async fn write_statistics(&mut self) -> Result<Option<StatisticsMetadata>> {
        let statistics = self
            .stats_collector
            .as_mut()
            .map(|collector| collector.finish());

        match statistics {
            Some(Ok(stats_batch)) if stats_batch.num_rows() > 0 => {
                debug_assert_eq!(self.next_batch_id() as usize, stats_batch.num_rows());
                let schema = Schema::try_from(stats_batch.schema().as_ref())?;
                let leaf_field_ids = schema.field_ids();

                let mut stats_page_table = PageTable::default();
                for (i, field) in schema.fields.iter().enumerate() {
                    Self::write_array(
                        self.object_writer.as_mut(),
                        field,
                        &[stats_batch.column(i)],
                        0, // Only one batch for statistics.
                        &mut stats_page_table,
                    )
                    .await?;
                }

                let page_table_position = stats_page_table
                    .write(self.object_writer.as_mut(), 0)
                    .await?;

                Ok(Some(StatisticsMetadata {
                    schema,
                    leaf_field_ids,
                    page_table_position,
                }))
            }
            Some(Err(e)) => Err(e),
            _ => Ok(None),
        }
    }

    async fn write_footer(&mut self) -> Result<()> {
        // Step 1. Write page table.
        let field_id_offset = *self.schema.field_ids().iter().min().unwrap();
        let pos = self
            .page_table
            .write(self.object_writer.as_mut(), field_id_offset)
            .await?;
        self.metadata.page_table_position = pos;

        // Step 2. Write statistics.
        self.metadata.stats_metadata = self.write_statistics().await?;

        // Step 3. Write manifest and dictionary values.
        write_schema_dictionaries(self.object_writer.as_mut(), &mut self.schema).await?;
        let pos = M::store_schema(self.object_writer.as_mut(), &self.schema).await?;

        // Step 4. Write metadata.
        self.metadata.manifest_position = pos;
        let pos = self.object_writer.write_struct(&self.metadata).await?;

        // Step 5. Write magics.
        self.object_writer
            .write_magics(pos, MAJOR_VERSION, MINOR_VERSION, MAGIC)
            .await
    }
}

/// Walk through the schema and return arrays with their Lance field.
///
/// This skips over nested arrays and fields within list arrays. It does walk
/// over the children of structs.
fn fields_in_batches<'a>(
    batches: &'a [RecordBatch],
    schema: &'a Schema,
) -> impl Iterator<Item = (&'a Field, Vec<&'a ArrayRef>)> {
    let num_columns = batches[0].num_columns();
    let array_iters = (0..num_columns).map(|col_i| {
        batches
            .iter()
            .map(|batch| batch.column(col_i))
            .collect::<Vec<_>>()
    });
    let mut to_visit: Vec<(&'a Field, Vec<&'a ArrayRef>)> =
        schema.fields.iter().zip(array_iters).collect();

    std::iter::from_fn(move || {
        loop {
            let (field, arrays): (_, Vec<&'a ArrayRef>) = to_visit.pop()?;
            match field.data_type() {
                DataType::Struct(_) => {
                    for (i, child_field) in field.children.iter().enumerate() {
                        let child_arrays = arrays
                            .iter()
                            .map(|arr| as_struct_array(*arr).column(i))
                            .collect::<Vec<&'a ArrayRef>>();
                        to_visit.push((child_field, child_arrays));
                    }
                    continue;
                }
                // We only walk structs right now.
                _ if field.data_type().is_nested() => continue,
                _ => return Some((field, arrays)),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow_array::{
        BooleanArray, Decimal128Array, Decimal256Array, DictionaryArray, DurationMicrosecondArray,
        DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray,
        FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Int32Array, Int64Array, ListArray,
        NullArray, StringArray, TimestampMicrosecondArray, TimestampSecondArray, UInt8Array,
        types::UInt32Type,
    };
    use arrow_buffer::i256;
    use arrow_schema::{
        Field as ArrowField, Fields as ArrowFields, Schema as ArrowSchema, TimeUnit,
    };
    use arrow_select::concat::concat_batches;

    use crate::versions::v1::reader::FileReader;

    #[tokio::test]
    async fn test_write_file() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("null", DataType::Null, true),
            ArrowField::new("bool", DataType::Boolean, true),
            ArrowField::new("i", DataType::Int64, true),
            ArrowField::new("f", DataType::Float32, false),
            ArrowField::new("b", DataType::Utf8, true),
            ArrowField::new("decimal128", DataType::Decimal128(7, 3), false),
            ArrowField::new("decimal256", DataType::Decimal256(7, 3), false),
            ArrowField::new("duration_sec", DataType::Duration(TimeUnit::Second), false),
            ArrowField::new(
                "duration_msec",
                DataType::Duration(TimeUnit::Millisecond),
                false,
            ),
            ArrowField::new(
                "duration_usec",
                DataType::Duration(TimeUnit::Microsecond),
                false,
            ),
            ArrowField::new(
                "duration_nsec",
                DataType::Duration(TimeUnit::Nanosecond),
                false,
            ),
            ArrowField::new(
                "d",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                true,
            ),
            ArrowField::new(
                "fixed_size_list",
                DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float32, true)),
                    16,
                ),
                true,
            ),
            ArrowField::new("fixed_size_binary", DataType::FixedSizeBinary(8), true),
            ArrowField::new(
                "l",
                DataType::List(Arc::new(ArrowField::new("item", DataType::Utf8, true))),
                true,
            ),
            ArrowField::new(
                "large_l",
                DataType::LargeList(Arc::new(ArrowField::new("item", DataType::Utf8, true))),
                true,
            ),
            ArrowField::new(
                "l_dict",
                DataType::List(Arc::new(ArrowField::new(
                    "item",
                    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                    true,
                ))),
                true,
            ),
            ArrowField::new(
                "large_l_dict",
                DataType::LargeList(Arc::new(ArrowField::new(
                    "item",
                    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                    true,
                ))),
                true,
            ),
            ArrowField::new(
                "s",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("si", DataType::Int64, true),
                    ArrowField::new("sb", DataType::Utf8, true),
                ])),
                true,
            ),
        ]);
        let mut schema = Schema::try_from(&arrow_schema).unwrap();

        let dict_vec = (0..100).map(|n| ["a", "b", "c"][n % 3]).collect::<Vec<_>>();
        let dict_arr: DictionaryArray<UInt32Type> = dict_vec.into_iter().collect();

        let fixed_size_list_arr = FixedSizeListArray::try_new_from_values(
            Float32Array::from_iter((0..1600).map(|n| n as f32).collect::<Vec<_>>()),
            16,
        )
        .unwrap();

        let binary_data: [u8; 800] = [123; 800];
        let fixed_size_binary_arr =
            FixedSizeBinaryArray::try_new_from_values(&UInt8Array::from_iter(binary_data), 8)
                .unwrap();

        let list_offsets: Int32Array = (0..202).step_by(2).collect();
        let list_values =
            StringArray::from((0..200).map(|n| format!("str-{}", n)).collect::<Vec<_>>());
        let list_arr: arrow_array::GenericListArray<i32> =
            try_new_generic_list_array(list_values, &list_offsets).unwrap();

        let large_list_offsets: Int64Array = (0..202).step_by(2).collect();
        let large_list_values =
            StringArray::from((0..200).map(|n| format!("str-{}", n)).collect::<Vec<_>>());
        let large_list_arr: arrow_array::GenericListArray<i64> =
            try_new_generic_list_array(large_list_values, &large_list_offsets).unwrap();

        let list_dict_offsets: Int32Array = (0..202).step_by(2).collect();
        let list_dict_vec = (0..200).map(|n| ["a", "b", "c"][n % 3]).collect::<Vec<_>>();
        let list_dict_arr: DictionaryArray<UInt32Type> = list_dict_vec.into_iter().collect();
        let list_dict_arr: arrow_array::GenericListArray<i32> =
            try_new_generic_list_array(list_dict_arr, &list_dict_offsets).unwrap();

        let large_list_dict_offsets: Int64Array = (0..202).step_by(2).collect();
        let large_list_dict_vec = (0..200).map(|n| ["a", "b", "c"][n % 3]).collect::<Vec<_>>();
        let large_list_dict_arr: DictionaryArray<UInt32Type> =
            large_list_dict_vec.into_iter().collect();
        let large_list_dict_arr: arrow_array::GenericListArray<i64> =
            try_new_generic_list_array(large_list_dict_arr, &large_list_dict_offsets).unwrap();

        let columns: Vec<ArrayRef> = vec![
            Arc::new(NullArray::new(100)),
            Arc::new(BooleanArray::from_iter(
                (0..100).map(|f| Some(f % 3 == 0)).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from_iter((0..100).collect::<Vec<_>>())),
            Arc::new(Float32Array::from_iter(
                (0..100).map(|n| n as f32).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..100).map(|n| n.to_string()).collect::<Vec<_>>(),
            )),
            Arc::new(
                Decimal128Array::from_iter_values(0..100)
                    .with_precision_and_scale(7, 3)
                    .unwrap(),
            ),
            Arc::new(
                Decimal256Array::from_iter_values((0..100).map(|v| i256::from_i128(v as i128)))
                    .with_precision_and_scale(7, 3)
                    .unwrap(),
            ),
            Arc::new(DurationSecondArray::from_iter_values(0..100)),
            Arc::new(DurationMillisecondArray::from_iter_values(0..100)),
            Arc::new(DurationMicrosecondArray::from_iter_values(0..100)),
            Arc::new(DurationNanosecondArray::from_iter_values(0..100)),
            Arc::new(dict_arr),
            Arc::new(fixed_size_list_arr),
            Arc::new(fixed_size_binary_arr),
            Arc::new(list_arr),
            Arc::new(large_list_arr),
            Arc::new(list_dict_arr),
            Arc::new(large_list_dict_arr),
            Arc::new(StructArray::from(vec![
                (
                    Arc::new(ArrowField::new("si", DataType::Int64, true)),
                    Arc::new(Int64Array::from_iter((100..200).collect::<Vec<_>>())) as ArrayRef,
                ),
                (
                    Arc::new(ArrowField::new("sb", DataType::Utf8, true)),
                    Arc::new(StringArray::from(
                        (0..100).map(|n| n.to_string()).collect::<Vec<_>>(),
                    )) as ArrayRef,
                ),
            ])),
        ];
        let batch = RecordBatch::try_new(Arc::new(arrow_schema), columns).unwrap();
        schema.set_dictionary(&batch).unwrap();

        let store = ObjectStore::memory();
        let path = Path::from("/foo");
        let mut file_writer = FileWriter::<NotSelfDescribing>::try_new(
            &store,
            &path,
            schema.clone(),
            &Default::default(),
        )
        .await
        .unwrap();
        file_writer
            .write(std::slice::from_ref(&batch))
            .await
            .unwrap();
        file_writer.finish().await.unwrap();

        let reader = FileReader::try_new(&store, &path, schema).await.unwrap();
        let actual = reader.read_batch(0, .., reader.schema()).await.unwrap();
        assert_eq!(actual, batch);
    }

    #[tokio::test]
    async fn test_dictionary_first_element_file() {
        let arrow_schema = ArrowSchema::new(vec![ArrowField::new(
            "d",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            true,
        )]);
        let mut schema = Schema::try_from(&arrow_schema).unwrap();

        let dict_vec = (0..100).map(|n| ["a", "b", "c"][n % 3]).collect::<Vec<_>>();
        let dict_arr: DictionaryArray<UInt32Type> = dict_vec.into_iter().collect();

        let columns: Vec<ArrayRef> = vec![Arc::new(dict_arr)];
        let batch = RecordBatch::try_new(Arc::new(arrow_schema), columns).unwrap();
        schema.set_dictionary(&batch).unwrap();

        let store = ObjectStore::memory();
        let path = Path::from("/foo");
        let mut file_writer = FileWriter::<NotSelfDescribing>::try_new(
            &store,
            &path,
            schema.clone(),
            &Default::default(),
        )
        .await
        .unwrap();
        file_writer
            .write(std::slice::from_ref(&batch))
            .await
            .unwrap();
        file_writer.finish().await.unwrap();

        let reader = FileReader::try_new(&store, &path, schema).await.unwrap();
        let actual = reader.read_batch(0, .., reader.schema()).await.unwrap();
        assert_eq!(actual, batch);
    }

    #[tokio::test]
    async fn test_write_temporal_types() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                "ts_notz",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            ArrowField::new(
                "ts_tz",
                DataType::Timestamp(TimeUnit::Microsecond, Some("America/Los_Angeles".into())),
                false,
            ),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampSecondArray::from(vec![11111111, 22222222])),
            Arc::new(
                TimestampMicrosecondArray::from(vec![3333333, 4444444])
                    .with_timezone("America/Los_Angeles"),
            ),
        ];
        let batch = RecordBatch::try_new(arrow_schema.clone(), columns).unwrap();

        let schema = Schema::try_from(arrow_schema.as_ref()).unwrap();
        let store = ObjectStore::memory();
        let path = Path::from("/foo");
        let mut file_writer = FileWriter::<NotSelfDescribing>::try_new(
            &store,
            &path,
            schema.clone(),
            &Default::default(),
        )
        .await
        .unwrap();
        file_writer
            .write(std::slice::from_ref(&batch))
            .await
            .unwrap();
        file_writer.finish().await.unwrap();

        let reader = FileReader::try_new(&store, &path, schema).await.unwrap();
        let actual = reader.read_batch(0, .., reader.schema()).await.unwrap();
        assert_eq!(actual, batch);
    }

    #[tokio::test]
    async fn test_collect_stats() {
        // Validate:
        // Only collects stats for requested columns
        // Can collect stats in nested structs
        // Won't collect stats for list columns (for now)

        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("i", DataType::Int64, true),
            ArrowField::new("i2", DataType::Int64, true),
            ArrowField::new(
                "l",
                DataType::List(Arc::new(ArrowField::new("item", DataType::Int32, true))),
                true,
            ),
            ArrowField::new(
                "s",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("si", DataType::Int64, true),
                    ArrowField::new("sb", DataType::Utf8, true),
                ])),
                true,
            ),
        ]);

        let schema = Schema::try_from(&arrow_schema).unwrap();

        let store = ObjectStore::memory();
        let path = Path::from("/foo");

        let options = FileWriterOptions {
            collect_stats_for_fields: Some(vec![0, 1, 5, 6]),
        };
        let mut file_writer =
            FileWriter::<NotSelfDescribing>::try_new(&store, &path, schema.clone(), &options)
                .await
                .unwrap();

        let batch1 = RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![4, 5, 6])),
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(1i32), Some(2), Some(3)]),
                    Some(vec![Some(4), Some(5)]),
                    Some(vec![]),
                ])),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(ArrowField::new("si", DataType::Int64, true)),
                        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("sb", DataType::Utf8, true)),
                        Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
                    ),
                ])),
            ],
        )
        .unwrap();
        file_writer.write(&[batch1]).await.unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![5, 6])),
                Arc::new(Int64Array::from(vec![10, 11])),
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(1i32), Some(2), Some(3)]),
                    Some(vec![]),
                ])),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(ArrowField::new("si", DataType::Int64, true)),
                        Arc::new(Int64Array::from(vec![4, 5])) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("sb", DataType::Utf8, true)),
                        Arc::new(StringArray::from(vec!["d", "e"])) as ArrayRef,
                    ),
                ])),
            ],
        )
        .unwrap();
        file_writer.write(&[batch2]).await.unwrap();

        file_writer.finish().await.unwrap();

        let reader = FileReader::try_new(&store, &path, schema).await.unwrap();

        let read_stats = reader.read_page_stats(&[0, 1, 5, 6]).await.unwrap();
        assert!(read_stats.is_some());
        let read_stats = read_stats.unwrap();

        let expected_stats_schema = stats_schema([
            (0, DataType::Int64),
            (1, DataType::Int64),
            (5, DataType::Int64),
            (6, DataType::Utf8),
        ]);

        assert_eq!(read_stats.schema().as_ref(), &expected_stats_schema);

        let expected_stats = stats_batch(&[
            Stats {
                field_id: 0,
                null_counts: vec![0, 0],
                min_values: Arc::new(Int64Array::from(vec![1, 5])),
                max_values: Arc::new(Int64Array::from(vec![3, 6])),
            },
            Stats {
                field_id: 1,
                null_counts: vec![0, 0],
                min_values: Arc::new(Int64Array::from(vec![4, 10])),
                max_values: Arc::new(Int64Array::from(vec![6, 11])),
            },
            Stats {
                field_id: 5,
                null_counts: vec![0, 0],
                min_values: Arc::new(Int64Array::from(vec![1, 4])),
                max_values: Arc::new(Int64Array::from(vec![3, 5])),
            },
            // FIXME: these max values shouldn't be incremented
            // https://github.com/lancedb/lance/issues/1517
            Stats {
                field_id: 6,
                null_counts: vec![0, 0],
                min_values: Arc::new(StringArray::from(vec!["a", "d"])),
                max_values: Arc::new(StringArray::from(vec!["c", "e"])),
            },
        ]);

        assert_eq!(read_stats, expected_stats);
    }

    fn stats_schema(data_fields: impl IntoIterator<Item = (i32, DataType)>) -> ArrowSchema {
        let fields = data_fields
            .into_iter()
            .map(|(field_id, data_type)| {
                Arc::new(ArrowField::new(
                    format!("{}", field_id),
                    DataType::Struct(
                        vec![
                            Arc::new(ArrowField::new("null_count", DataType::Int64, false)),
                            Arc::new(ArrowField::new("min_value", data_type.clone(), true)),
                            Arc::new(ArrowField::new("max_value", data_type, true)),
                        ]
                        .into(),
                    ),
                    false,
                ))
            })
            .collect::<Vec<_>>();
        ArrowSchema::new(fields)
    }

    struct Stats {
        field_id: i32,
        null_counts: Vec<i64>,
        min_values: ArrayRef,
        max_values: ArrayRef,
    }

    fn stats_batch(stats: &[Stats]) -> RecordBatch {
        let schema = stats_schema(
            stats
                .iter()
                .map(|s| (s.field_id, s.min_values.data_type().clone())),
        );

        let columns = stats
            .iter()
            .map(|s| {
                let data_type = s.min_values.data_type().clone();
                let fields = vec![
                    Arc::new(ArrowField::new("null_count", DataType::Int64, false)),
                    Arc::new(ArrowField::new("min_value", data_type.clone(), true)),
                    Arc::new(ArrowField::new("max_value", data_type, true)),
                ];
                let arrays = vec![
                    Arc::new(Int64Array::from(s.null_counts.clone())),
                    s.min_values.clone(),
                    s.max_values.clone(),
                ];
                Arc::new(StructArray::new(fields.into(), arrays, None)) as ArrayRef
            })
            .collect();

        RecordBatch::try_new(Arc::new(schema), columns).unwrap()
    }

    async fn read_file_as_one_batch(
        object_store: &ObjectStore,
        path: &Path,
        schema: Schema,
    ) -> RecordBatch {
        let reader = FileReader::try_new(object_store, path, schema)
            .await
            .unwrap();
        let mut batches = vec![];
        for i in 0..reader.num_batches() {
            batches.push(
                reader
                    .read_batch(i as i32, .., reader.schema())
                    .await
                    .unwrap(),
            );
        }
        let arrow_schema = Arc::new(reader.schema().into());
        concat_batches(&arrow_schema, &batches).unwrap()
    }

    /// Test encoding arrays that share the same underneath buffer.
    #[tokio::test]
    async fn test_encode_slice() {
        let store = ObjectStore::memory();
        let path = Path::from("/shared_slice");

        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "i",
            DataType::Int32,
            false,
        )]));
        let schema = Schema::try_from(arrow_schema.as_ref()).unwrap();
        let mut file_writer = FileWriter::<NotSelfDescribing>::try_new(
            &store,
            &path,
            schema.clone(),
            &Default::default(),
        )
        .await
        .unwrap();

        let array = Int32Array::from_iter_values(0..1000);

        for i in (0..1000).step_by(4) {
            let data = array.slice(i, 4);
            file_writer
                .write(&[RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(data)]).unwrap()])
                .await
                .unwrap();
        }
        file_writer.finish().await.unwrap();
        assert!(store.size(&path).await.unwrap() < 2 * 8 * 1000);

        let batch = read_file_as_one_batch(&store, &path, schema).await;
        assert_eq!(batch.column_by_name("i").unwrap().as_ref(), &array);
    }

    #[tokio::test]
    async fn test_write_schema_with_holes() {
        let store = ObjectStore::memory();
        let path = Path::from("test");

        let mut field0 = Field::try_from(&ArrowField::new("a", DataType::Int32, true)).unwrap();
        field0.set_id(-1, &mut 0);
        assert_eq!(field0.id, 0);
        let mut field2 = Field::try_from(&ArrowField::new("b", DataType::Int32, true)).unwrap();
        field2.set_id(-1, &mut 2);
        assert_eq!(field2.id, 2);
        // There is a hole at field id 1.
        let schema = Schema {
            fields: vec![field0, field2],
            metadata: Default::default(),
        };

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, true),
            ArrowField::new("b", DataType::Int32, true),
        ]));
        let data = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..10)),
                Arc::new(Int32Array::from_iter_values(10..20)),
            ],
        )
        .unwrap();

        let mut file_writer = FileWriter::<NotSelfDescribing>::try_new(
            &store,
            &path,
            schema.clone(),
            &Default::default(),
        )
        .await
        .unwrap();
        file_writer.write(&[data]).await.unwrap();
        file_writer.finish().await.unwrap();

        let page_table = file_writer.page_table;
        assert!(page_table.get(0, 0).is_some());
        assert!(page_table.get(2, 0).is_some());
    }
}
