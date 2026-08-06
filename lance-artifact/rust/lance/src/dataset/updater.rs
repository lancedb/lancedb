// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_array::{RecordBatch, UInt32Array};
use futures::StreamExt;
use lance_core::datatypes::{OnMissing, OnTypeMismatch};
use lance_core::utils::deletion::DeletionVector;
use lance_core::{Error, Result, datatypes::Schema};
use lance_file::version::ConcreteFileVersion;
use lance_table::format::{DataFile, Fragment};
use lance_table::utils::stream::ReadBatchFutStream;

use super::Dataset;
use super::fragment::FragmentReader;
use super::scanner::get_default_batch_size;
use super::versions;
use super::write::{GenericWriter, cleanup_data_fragments};
use crate::dataset::FileFragment;
use crate::dataset::utils::SchemaAdapter;

/// Update or insert a new column.
///
/// To use, call [`Updater::next`] to get the next [`RecordBatch`] as input,
/// then call [`Updater::update`] to update the batch. Repeat until
/// [`Updater::next`] returns `None`.
///
/// `write_schema` dictates the schema of the new file, while `final_schema` is
/// the schema of the full fragment after the update. These are optional and if
/// not specified, the updater will infer the write schema from the first batch
/// of results and will append them to the current schema to get the final schema.
pub struct Updater {
    fragment: FileFragment,

    /// The reader over the [`Fragment`]
    input_stream: ReadBatchFutStream,

    /// The last batch read from the file, with deleted rows removed
    last_input: Option<RecordBatch>,

    writer: Option<Box<dyn GenericWriter>>,

    /// The final schema of the fragment after the update.
    final_schema: Option<Schema>,

    /// The schema the new files will be written in. This only contains new columns.
    write_schema: Option<Schema>,

    /// The adapter to convert the logical data to physical data.
    schema_adapter: Option<SchemaAdapter>,

    finished: bool,

    deletion_restorer: DeletionRestorer,
}

impl Updater {
    /// Create a new updater with source reader, and destination writer.
    ///
    /// The `schemas` parameter is a tuple of the write schema (just the new fields)
    /// and the final schema (all the fields).
    ///
    /// If the schemas are not known, they can be None and will be inferred from
    /// the first batch of results.
    pub(super) async fn try_new(
        fragment: FileFragment,
        reader: FragmentReader,
        deletion_vector: DeletionVector,
        schemas: Option<(Schema, Schema)>,
        batch_size: Option<u32>,
    ) -> Result<Self> {
        let (write_schema, final_schema) = if let Some((write_schema, final_schema)) = schemas {
            (Some(write_schema), Some(final_schema))
        } else {
            (None, None)
        };

        let legacy_batch_size = reader.legacy_num_rows_in_batch(0);

        let batch_size = match (&legacy_batch_size, batch_size) {
            // If this is a v1 dataset we must use the row group size of the file
            (Some(legacy_batch_size), _) => *legacy_batch_size,
            // If this is a v2 dataset, let the user pick the batch size
            (None, Some(user_specified_batch_size)) => user_specified_batch_size,
            // Otherwise, default to 1024 if the user didn't specify anything
            (None, None) => get_default_batch_size().unwrap_or(1024) as u32,
        };

        let input_stream = reader.read_all(batch_size).await?;

        Ok(Self {
            fragment,
            input_stream,
            last_input: None,
            writer: None,
            write_schema,
            final_schema,
            // The schema adapter needs the data schema, not the logical schema, so it can't be
            // created until after the first batch is read.
            schema_adapter: None,
            finished: false,
            deletion_restorer: DeletionRestorer::new(deletion_vector, legacy_batch_size),
        })
    }

    pub fn fragment(&self) -> &FileFragment {
        &self.fragment
    }

    pub fn dataset(&self) -> &Dataset {
        self.fragment.dataset()
    }

    /// Returns the next [`RecordBatch`] as input for updater.
    pub async fn next(&mut self) -> Result<Option<&RecordBatch>> {
        if self.finished {
            return Ok(None);
        }
        let batch = self.input_stream.next().await;
        match batch {
            None => {
                if !self.deletion_restorer.is_exhausted() {
                    // This can happen only if there is a batch size (e.g. v1 file) and the
                    // last batch(es) are entirely deleted.
                    return Err(Error::not_supported_source("Missing too many rows in merge, run compaction to materialize deletions first".into()));
                }
                self.finished = true;
                Ok(None)
            }
            Some(batch) => {
                self.last_input = Some(batch.await?);
                Ok(self.last_input.as_ref())
            }
        }
    }

    /// Create a new Writer for new columns.
    ///
    /// After it is called, this Fragment contains the metadata of the new DataFile,
    /// containing the columns, even the data has not written yet.
    ///
    /// It is the caller's responsibility to close the [`FileWriter`].
    ///
    /// Internal use only.
    async fn new_writer(&mut self, schema: Schema) -> Result<Box<dyn GenericWriter>> {
        let data_storage_version = self
            .dataset()
            .manifest()
            .data_storage_format
            .lance_file_version()?;

        versions::open_update_writer(data_storage_version.into(), self.dataset(), &schema).await
    }

    /// Update one batch.
    pub async fn update(&mut self, batch: RecordBatch) -> Result<()> {
        let Some(last) = self.last_input.as_ref() else {
            return Err(Error::invalid_input(
                "Fragment Updater: no input data is available before update".to_string(),
            ));
        };

        if last.num_rows() != batch.num_rows() {
            return Err(Error::invalid_input(format!(
                "Fragment Updater: new batch has different size with the source batch: {} != {}",
                last.num_rows(),
                batch.num_rows()
            )));
        };

        // Add back in deleted rows
        let batch = self.deletion_restorer.restore(batch)?;

        if self.writer.is_none() {
            if self.write_schema.is_none() {
                // Need to infer the schema.
                let output_schema = batch.schema();
                let mut final_schema = self.fragment.schema().merge(output_schema.as_ref())?;
                final_schema.set_field_id(Some(self.fragment.dataset().manifest.max_field_id()));
                self.final_schema = Some(final_schema);
                self.final_schema.as_ref().unwrap().validate()?;
                self.write_schema = Some(self.final_schema.as_ref().unwrap().project_by_schema(
                    output_schema.as_ref(),
                    OnMissing::Error,
                    OnTypeMismatch::Error,
                )?);
            }

            self.writer = Some(
                self.new_writer(self.write_schema.as_ref().unwrap().clone())
                    .await?,
            );
        }

        let schema_adapter = if let Some(schema_adapter) = self.schema_adapter.as_ref() {
            schema_adapter
        } else {
            self.schema_adapter = Some(SchemaAdapter::new(batch.schema()));
            self.schema_adapter.as_ref().unwrap()
        };

        let batch = schema_adapter.to_physical_batch(batch)?;

        let writer = self.writer.as_mut().unwrap();

        writer.write(&[batch]).await?;

        Ok(())
    }

    /// Finish updating this fragment, and returns the updated [`Fragment`].
    pub async fn finish(&mut self) -> Result<Fragment> {
        if let Some(writer) = self.writer.as_mut() {
            let (_, data_file) = writer.finish().await?;
            self.fragment.metadata.files.push(data_file);
        }

        Ok(self.fragment.metadata().clone())
    }

    /// Clean up any data file and blob sidecars created by the current unfinished writer.
    pub(super) async fn cleanup_unfinished_writer(&mut self) {
        let Some(writer) = self.writer.take() else {
            return;
        };
        let (path, base_id) = writer.data_file_path();
        let path = path.to_string();
        drop(writer);

        if path.is_empty() {
            return;
        }

        let mut fragment = Fragment::new(self.fragment.id() as u64);
        // cleanup_data_fragments only needs path/base_id to remove the unfinished
        // data file and any blob sidecars. Build a minimal synthetic fragment so
        // we can reuse the shared cleanup path without fabricating full metadata.
        fragment.files.push(DataFile::new(
            path,
            vec![],
            vec![],
            ConcreteFileVersion::V1,
            None,
            base_id,
        ));
        cleanup_data_fragments(
            &self.dataset().object_store,
            &self.dataset().base,
            None,
            &[fragment],
        )
        .await;
    }

    /// Get the final schema of the fragment after the update.
    ///
    /// This may be None if the schema is not known. This can happen if it was
    /// not specified up front and the first batch of results has not yet been
    /// processed.
    pub fn schema(&self) -> Option<&Schema> {
        self.final_schema.as_ref()
    }
}

/// Restores deleted rows.
///
/// All data files in a fragment must have the same # of rows (including deleted rows)
/// When we run the update process the next/update methods don't actually calculate on
/// deleted rows.  This means the updated batches will have fewer rows than the original
/// data files.  This struct restores the deleted rows, inserting arbitrary values into the
/// batches where the deleted rows should be.
///
/// To do this we scan through the deletion vector in sorted order, merging deleted rows
/// in as appropriate.
struct DeletionRestorer {
    current_row_id: u32,

    /// Number of rows in each batch, only used in legacy files for validation
    legacy_batch_size: Option<u32>,

    deletion_vector_iter: Option<Box<dyn Iterator<Item = u32> + Send>>,

    last_deleted_row_id: Option<u32>,
}

impl DeletionRestorer {
    fn new(deletion_vector: DeletionVector, legacy_batch_size: Option<u32>) -> Self {
        Self {
            current_row_id: 0,
            legacy_batch_size,
            deletion_vector_iter: Some(deletion_vector.into_sorted_iter()),
            last_deleted_row_id: None,
        }
    }

    fn is_exhausted(&self) -> bool {
        self.deletion_vector_iter.is_none()
    }

    fn is_full(batch_size: Option<u32>, num_rows: u32) -> bool {
        if let Some(legacy_batch_size) = batch_size {
            // We should never encounter the case that `batch_size < num_rows` because
            // that would mean we have a v1 writer and it generated a batch with more rows
            // than expected
            debug_assert!(legacy_batch_size >= num_rows);
            legacy_batch_size == num_rows
        } else {
            false
        }
    }

    /// Given a batch of `num_rows`, walk through the deletion vector, and figure out where blanks
    /// should be inserted.
    ///
    /// For example, if self.current_row_id is 10 and the deletion vector is [11, 12, 19, 25] and
    /// num_rows is 7 then this function will at least return [1, 2] and the batch will at least
    /// span row ids 10..18.
    ///
    /// Then, in the example we need to choose whether the returned batch should include
    /// row 19 (and have 10 rows) or not (and have 9 rows).  This is only a concern in v1 files
    /// where we want to match the original row group size (which is the batch size).  If the
    /// batch size is 9 then we do not include 19 and return as above.
    ///
    /// If the batch size is 10 (or unset) then we do include 19 and the return will be [1, 2, 9]
    ///
    /// In v2 files, since the batch size will be unset, we will always include as many deleted
    /// rows at the end as we can.
    fn deleted_batch_offsets_in_range(&mut self, mut num_rows: u32) -> Vec<u32> {
        let mut deleted = Vec::new();
        let first_row_id = self.current_row_id;
        // The last row id (exclusive) in the batch
        let mut last_row_id = first_row_id + num_rows;
        // If there are zero deleted rows then the range covered will be first_row_id..last_row_id
        if self.deletion_vector_iter.is_none() {
            return deleted;
        }
        let deletion_vector_iter = self.deletion_vector_iter.as_mut().unwrap();

        // Now we need to walk through our deletion vector and figure out where to insert blanks
        let mut next_deleted_id = if self.last_deleted_row_id.is_some() {
            self.last_deleted_row_id
        } else {
            deletion_vector_iter.next()
        };
        loop {
            if let Some(next_deleted_id) = next_deleted_id {
                if next_deleted_id > last_row_id
                    || (next_deleted_id == last_row_id
                        && Self::is_full(self.legacy_batch_size, num_rows))
                {
                    // Either the next deleted id is out of range or it is the next row but
                    // we are full.  Either way, stash it and return
                    self.last_deleted_row_id = Some(next_deleted_id);
                    return deleted;
                }
                // Otherwise, the deleted row is in range, and we have space in our batch
                // and so we include it
                deleted.push(next_deleted_id - first_row_id);
                last_row_id += 1;
                num_rows += 1;
            } else {
                // Deleted row ids iterator is exhausted
                self.deletion_vector_iter = None;
                return deleted;
            }
            next_deleted_id = deletion_vector_iter.next();
        }
    }

    fn restore(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        // Because of deleted rows, the number of row ids in the batch might not
        // match the length.
        let deleted_batch_offsets = self.deleted_batch_offsets_in_range(batch.num_rows() as u32);
        let batch = add_blanks(batch, &deleted_batch_offsets)?;

        if let Some(batch_size) = self.legacy_batch_size {
            // validation just in case, when the input has a fixed batch size then the
            // output should have the same fixed batch size (except the last batch)
            let is_last = self.is_exhausted();
            if batch.num_rows() != batch_size as usize && !is_last {
                return Err(Error::internal(format!(
                    "Fragment Updater: batch size mismatch: {} != {}",
                    batch.num_rows(),
                    batch_size
                )));
            }
        }

        self.current_row_id += batch.num_rows() as u32;
        Ok(batch)
    }
}

/// Add blank rows where there are deleted rows
pub(crate) fn add_blanks(batch: RecordBatch, batch_offsets: &[u32]) -> Result<RecordBatch> {
    // Fast early return
    if batch_offsets.is_empty() {
        return Ok(batch);
    }

    if batch.num_rows() == 0 {
        // TODO: implement adding blanks for an empty batch.
        // This is difficult because we need to create a batch for arbitrary schemas.
        return Err(Error::not_supported_source(
            "Missing too many rows in merge, run compaction to materialize deletions first".into(),
        ));
    }

    let mut selection_vector = Vec::<u32>::with_capacity(batch.num_rows() + batch_offsets.len());
    let mut batch_pos = 0;
    let mut next_id = 0;
    for batch_offset in batch_offsets {
        let num_rows = *batch_offset - next_id;
        selection_vector.extend(batch_pos..batch_pos + num_rows);
        // For simplicity, we just use the first value for deleted rows
        // TODO: optimize this to use small value for each column.
        selection_vector.push(0);
        next_id = *batch_offset + 1;
        batch_pos += num_rows;
    }
    selection_vector.extend(batch_pos..batch.num_rows() as u32);
    let selection_vector = UInt32Array::from(selection_vector);

    let arrays = batch
        .columns()
        .iter()
        .map(|array| {
            arrow::compute::take(array.as_ref(), &selection_vector, None)
                .map_err(|e| Error::arrow(format!("Failed to add blanks: {}", e)))
        })
        .collect::<Result<Vec<_>>>()?;

    let batch = RecordBatch::try_new(batch.schema(), arrays)?;

    Ok(batch)
}

#[cfg(test)]
mod tests {
    use arrow::{array::AsArray, datatypes::Int32Type};
    use lance_datagen::RowCount;

    use super::add_blanks;

    #[test]
    fn test_restore_deletes() {
        for batch_size in &[None, Some(10)] {
            let mut restorer = super::DeletionRestorer::new(
                vec![11, 12, 19, 20, 25].into_iter().collect(),
                *batch_size,
            );

            let batch = lance_datagen::gen_batch()
                .col("x", lance_datagen::array::step::<Int32Type>())
                .into_batch_rows(RowCount::from(10))
                .unwrap();
            // First batch is rows ids 0..9 so nothing is restored
            let restored = restorer.restore(batch.clone()).unwrap();
            assert_eq!(restored, batch);

            let batch = lance_datagen::gen_batch()
                .col("x", lance_datagen::array::step::<Int32Type>())
                .into_batch_rows(RowCount::from(7))
                .unwrap();
            // Next batch is rows ids 10..16 so we need to restore 11, 12
            // 19, and maybe 20 (depends on batch size)
            let restored = restorer.restore(batch).unwrap();
            let values = restored.column(0).as_primitive::<Int32Type>();
            assert_eq!(values.value(0), 0);
            assert_eq!(values.value(1), 0);
            assert_eq!(values.value(2), 0);
            assert_eq!(values.value(3), 1);
            assert_eq!(values.value(4), 2);
            assert_eq!(values.value(5), 3);
            assert_eq!(values.value(6), 4);
            assert_eq!(values.value(7), 5);
            assert_eq!(values.value(8), 6);
            assert_eq!(values.value(9), 0);
            if *batch_size == Some(10) {
                assert_eq!(values.len(), 10);
            } else {
                assert_eq!(values.value(10), 0);
                assert_eq!(values.len(), 11);
            }
        }
    }

    #[test]
    fn test_add_blanks() {
        let batch = lance_datagen::gen_batch()
            .col("x", lance_datagen::array::step::<Int32Type>())
            .into_batch_rows(RowCount::from(10))
            .unwrap();

        let with_blanks = add_blanks(batch.clone(), &[5, 7]).unwrap();

        assert_eq!(with_blanks.num_rows(), 12);
        let values = with_blanks.column(0).as_primitive::<Int32Type>();
        for i in 0..5 {
            assert_eq!(values.value(i), i as i32);
        }
        assert_eq!(values.value(5), 0);
        assert_eq!(values.value(6), 5);
        assert_eq!(values.value(7), 0);
        for i in 8..12 {
            assert_eq!(values.value(i), (i - 2) as i32);
        }

        let with_blanks = add_blanks(batch, &[0, 11]).unwrap();
        let values = with_blanks.column(0).as_primitive::<Int32Type>();
        assert_eq!(values.value(0), 0);
        for i in 1..11 {
            assert_eq!(values.value(i), (i - 1) as i32);
        }
        assert_eq!(values.value(11), 0);
    }
}
