// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_array::builder::Int64Builder;
use arrow_array::{Array, Int64Array};
use arrow_schema::DataType;
use lance_core::deepsize::DeepSizeOf;
use std::collections::BTreeMap;
use tokio::io::AsyncWriteExt;

use lance_core::{Error, Result};
use lance_io::traits::{Reader, Writer};

use super::encoding::plain::PlainDecoder;

#[derive(Clone, Debug, PartialEq, DeepSizeOf)]
pub struct PageInfo {
    pub position: usize,
    pub length: usize,
}

impl PageInfo {
    pub fn new(position: usize, length: usize) -> Self {
        Self { position, length }
    }
}

/// Page lookup table.
///
#[derive(Debug, Default, Clone, PartialEq, DeepSizeOf)]
pub struct PageTable {
    /// map[field-id,  map[batch-id, PageInfo]]
    pages: BTreeMap<i32, BTreeMap<i32, PageInfo>>,
}

impl PageTable {
    /// Load [PageTable] from disk.
    ///
    /// Parameters:
    ///  * `position`: The start position in the file where the page table is stored.
    ///  * `min_field_id`: The smallest field_id that is present in the schema.
    ///  * `max_field_id`: The largest field_id that is present in the schema.
    ///  * `num_batches`: The number of batches in the file.
    ///
    /// The page table is stored as an array. The on-disk size is determined based
    /// on the `min_field_id`, `max_field_id`, and `num_batches` parameters. If
    /// these are incorrect, the page table will not be read correctly.
    ///
    /// The full sequence of field ids `min_field_id..=max_field_id` will be loaded.
    /// Non-existent pages will be represented as (0, 0) in the page table. Pages
    /// can be non-existent because they are not present in the file, or because
    /// they are struct fields which have no data pages.
    pub async fn load(
        reader: &dyn Reader,
        position: usize,
        min_field_id: i32,
        max_field_id: i32,
        num_batches: i32,
    ) -> Result<Self> {
        if max_field_id < min_field_id {
            return Err(Error::internal(format!(
                "max_field_id {} is less than min_field_id {}",
                max_field_id, min_field_id
            )));
        }

        let field_ids = min_field_id..=max_field_id;
        let num_columns = field_ids.clone().count();
        let length = num_columns * num_batches as usize * 2;
        let decoder = PlainDecoder::new(reader, &DataType::Int64, position, length)?;
        let raw_arr = decoder.decode().await?;
        let arr = raw_arr.as_any().downcast_ref::<Int64Array>().unwrap();

        let mut pages = BTreeMap::default();
        for (field_pos, field_id) in field_ids.enumerate() {
            pages.insert(field_id, BTreeMap::default());
            for batch in 0..num_batches {
                let idx = field_pos as i32 * num_batches + batch;
                let batch_position = &arr.value((idx * 2) as usize);
                let batch_length = &arr.value((idx * 2 + 1) as usize);
                pages.get_mut(&field_id).unwrap().insert(
                    batch,
                    PageInfo {
                        position: *batch_position as usize,
                        length: *batch_length as usize,
                    },
                );
            }
        }

        Ok(Self { pages })
    }

    /// Write [PageTable] to disk.
    ///
    /// `min_field_id` is the smallest field_id that is present in the schema.
    /// This might be a struct field, which has no data pages, but it still must
    /// be serialized to the page table per the format spec.
    ///
    /// Any (field_id, batch_id) combinations that are not present in the page table
    /// will be written as (0, 0) to indicate an empty page. This includes any
    /// holes in the field ids as well as struct fields which have no data pages.
    pub async fn write(&self, writer: &mut dyn Writer, min_field_id: i32) -> Result<usize> {
        if self.pages.is_empty() {
            return Err(Error::invalid_input_source("empty page table".into()));
        }

        let observed_min = *self.pages.keys().min().unwrap();
        if min_field_id > *self.pages.keys().min().unwrap() {
            return Err(Error::invalid_input(format!(
                "field_id_offset {} is greater than the minimum field_id {}",
                min_field_id, observed_min
            )));
        }
        let max_field_id = *self.pages.keys().max().unwrap();
        let field_ids = min_field_id..=max_field_id;

        let pos = writer.tell().await?;
        let num_batches = self
            .pages
            .values()
            .flat_map(|c_map| c_map.keys().max())
            .max()
            .unwrap()
            + 1;

        let mut builder =
            Int64Builder::with_capacity(field_ids.clone().count() * num_batches as usize);
        for field_id in field_ids {
            for batch in 0..num_batches {
                if let Some(page_info) = self.get(field_id, batch) {
                    builder.append_value(page_info.position as i64);
                    builder.append_value(page_info.length as i64);
                } else {
                    builder.append_slice(&[0, 0]);
                }
            }
        }
        let arr = builder.finish();
        writer
            .write_all(arr.into_data().buffers()[0].as_slice())
            .await?;

        Ok(pos)
    }

    /// Set page lookup info for a page identified by `(column, batch)` pair.
    pub fn set(&mut self, field_id: i32, batch: i32, page_info: PageInfo) {
        self.pages
            .entry(field_id)
            .or_default()
            .insert(batch, page_info);
    }

    pub fn get(&self, field_id: i32, batch: i32) -> Option<&PageInfo> {
        self.pages
            .get(&field_id)
            .and_then(|c_map| c_map.get(&batch))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use lance_core::utils::tempfile::TempStdFile;
    use pretty_assertions::assert_eq;

    use lance_io::local::LocalObjectReader;

    #[test]
    fn test_set_page_info() {
        let mut page_table = PageTable::default();
        let page_info = PageInfo::new(1, 2);
        page_table.set(10, 20, page_info.clone());

        let actual = page_table.get(10, 20).unwrap();
        assert_eq!(actual, &page_info);
    }

    #[tokio::test]
    async fn test_roundtrip_page_info() {
        let mut page_table = PageTable::default();
        let page_info = PageInfo::new(1, 2);

        // Add fields 10..14, 4 batches with some missing
        page_table.set(10, 2, page_info.clone());
        page_table.set(11, 1, page_info.clone());
        // A hole at 12
        page_table.set(13, 0, page_info.clone());
        page_table.set(13, 1, page_info.clone());
        page_table.set(13, 2, page_info.clone());
        page_table.set(13, 3, page_info.clone());

        let path = TempStdFile::default();

        // The first field_id with entries is 10, but if it's inside of a struct
        // the struct itself needs to be included in the page table. We use 9
        // here to represent the struct.
        let starting_field_id = 9;

        let mut writer = tokio::fs::File::create(&path).await.unwrap();
        let pos = page_table
            .write(&mut writer, starting_field_id)
            .await
            .unwrap();
        AsyncWriteExt::shutdown(&mut writer).await.unwrap();

        let reader = LocalObjectReader::open_local_path(&path, 1024, None)
            .await
            .unwrap();
        let actual = PageTable::load(
            reader.as_ref(),
            pos,
            starting_field_id, // First field id is 10, but we want to start at 9
            13,                // Last field id is 13
            4,                 // 4 batches
        )
        .await
        .unwrap();

        // Output should have filled in the empty pages.
        let mut expected = actual.clone();
        let default_page_info = PageInfo::new(0, 0);
        let expected_default_pages = [
            (9, 0),
            (9, 1),
            (9, 2),
            (9, 3),
            (10, 0),
            (10, 1),
            (10, 3),
            (11, 0),
            (11, 2),
            (11, 3),
            (12, 0),
            (12, 1),
            (12, 2),
            (12, 3),
        ];
        for (field_id, batch) in expected_default_pages.iter() {
            expected.set(*field_id, *batch, default_page_info.clone());
        }

        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn test_error_handling() {
        let mut page_table = PageTable::default();

        let path = TempStdFile::default();

        // Returns an error if the page table is empty
        let mut writer = tokio::fs::File::create(&path).await.unwrap();
        let res = page_table.write(&mut writer, 1).await;
        assert!(res.is_err());
        assert!(
            matches!(res.unwrap_err(), Error::InvalidInput { source, .. } if source.to_string().contains("empty page table"))
        );

        let page_info = PageInfo::new(1, 2);
        page_table.set(0, 0, page_info.clone());

        // Returns an error if passing a min_field_id higher than the lowest field_id
        let mut writer = tokio::fs::File::create(&path).await.unwrap();
        let res = page_table.write(&mut writer, 1).await;
        assert!(res.is_err());
        assert!(
            matches!(res.unwrap_err(), Error::InvalidInput { source, .. } 
                if source.to_string().contains("field_id_offset 1 is greater than the minimum field_id 0"))
        );

        let mut writer = tokio::fs::File::create(&path).await.unwrap();
        let res = page_table.write(&mut writer, 0).await.unwrap();

        let reader = LocalObjectReader::open_local_path(&path, 1024, None)
            .await
            .unwrap();

        // Returns an error if max_field_id is less than min_field_id
        let res = PageTable::load(reader.as_ref(), res, 1, 0, 1).await;
        assert!(res.is_err());
        assert!(matches!(res.unwrap_err(), Error::Internal { message, .. }
                if message.contains("max_field_id 0 is less than min_field_id 1")));
    }
}
