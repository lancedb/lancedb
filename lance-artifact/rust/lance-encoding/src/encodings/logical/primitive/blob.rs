// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Routines for decoding blob data
//!
//! The blob structural encoding is a structural encoding where the values (blobs) are stored
//! out-of-line in the file.  The page contains the descriptions, encoded using some other layout.

use std::{collections::VecDeque, ops::Range, sync::Arc};

use arrow_array::{Array, UInt64Array, cast::AsArray, make_array};
use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};

use lance_core::{
    Error, Result, cache::DeepSizeOf, datatypes::BLOB_DESC_TYPE, error::LanceOptionExt,
};

use crate::{
    EncodingsIo,
    buffer::LanceBuffer,
    data::{BlockInfo, DataBlock, VariableWidthBlock},
    decoder::{DecodePageTask, DecodedPage, StructuralPageDecoder},
    encodings::logical::primitive::{CachedPageData, PageLoadTask, StructuralPageScheduler},
    repdef::{DefinitionInterpretation, RepDefUnraveler},
};

/// How many bytes to target in each unloaded / loaded shard.  A larger value means
/// we buffer more data in memory / make bigger requests to the I/O scheduler while
/// a smaller value means more requests to the I/O scheduler.
///
/// This is probably a reasonable default for most cases.
pub const TARGET_SHARD_SIZE: u64 = 32 * 1024 * 1024;

#[derive(Debug)]
pub(super) struct BlobDescriptionPageScheduler {
    inner_scheduler: Box<dyn StructuralPageScheduler>,
    def_meaning: Arc<[DefinitionInterpretation]>,
}

impl BlobDescriptionPageScheduler {
    pub fn new(
        inner_scheduler: Box<dyn StructuralPageScheduler>,
        def_meaning: Arc<[DefinitionInterpretation]>,
    ) -> Self {
        Self {
            inner_scheduler,
            def_meaning,
        }
    }

    fn wrap_decoder_fut(
        decoder_fut: BoxFuture<'static, Result<Box<dyn StructuralPageDecoder>>>,
        def_meaning: Arc<[DefinitionInterpretation]>,
    ) -> BoxFuture<'static, Result<Box<dyn StructuralPageDecoder>>> {
        async move {
            let decoder = decoder_fut.await?;
            Ok(
                Box::new(BlobDescriptionPageDecoder::new(decoder, def_meaning))
                    as Box<dyn StructuralPageDecoder>,
            )
        }
        .boxed()
    }
}

impl StructuralPageScheduler for BlobDescriptionPageScheduler {
    fn initialize<'a>(
        &'a mut self,
        io: &Arc<dyn EncodingsIo>,
    ) -> BoxFuture<'a, Result<Arc<dyn CachedPageData>>> {
        self.inner_scheduler.initialize(io)
    }

    fn load(&mut self, data: &Arc<dyn CachedPageData>) {
        self.inner_scheduler.load(data);
    }

    fn schedule_ranges(
        &self,
        ranges: &[Range<u64>],
        io: &Arc<dyn EncodingsIo>,
    ) -> Result<Vec<PageLoadTask>> {
        let tasks = self.inner_scheduler.schedule_ranges(ranges, io)?;
        Ok(tasks
            .into_iter()
            .map(|task| PageLoadTask {
                decoder_fut: Self::wrap_decoder_fut(task.decoder_fut, self.def_meaning.clone()),
                num_rows: task.num_rows,
            })
            .collect())
    }
}

#[derive(Debug)]
struct BlobDescriptionPageDecoder {
    inner: Box<dyn StructuralPageDecoder>,
    def_meaning: Arc<[DefinitionInterpretation]>,
}

impl BlobDescriptionPageDecoder {
    fn new(
        inner: Box<dyn StructuralPageDecoder>,
        def_meaning: Arc<[DefinitionInterpretation]>,
    ) -> Self {
        Self { inner, def_meaning }
    }
}

impl StructuralPageDecoder for BlobDescriptionPageDecoder {
    fn drain(&mut self, num_rows: u64) -> Result<Box<dyn DecodePageTask>> {
        Ok(Box::new(BlobDescriptionDecodePageTask::new(
            self.inner.drain(num_rows)?,
            self.def_meaning.clone(),
        )))
    }

    fn num_rows(&self) -> u64 {
        self.inner.num_rows()
    }
}

#[derive(Debug)]
struct BlobDescriptionDecodePageTask {
    inner: Box<dyn DecodePageTask>,
    def_meaning: Arc<[DefinitionInterpretation]>,
}

impl BlobDescriptionDecodePageTask {
    fn new(inner: Box<dyn DecodePageTask>, def_meaning: Arc<[DefinitionInterpretation]>) -> Self {
        Self { inner, def_meaning }
    }
}

impl DecodePageTask for BlobDescriptionDecodePageTask {
    fn decode(self: Box<Self>) -> Result<DecodedPage> {
        let decoded = self.inner.decode()?;
        let num_values = decoded.data.num_values();

        // Need to extract out the repdef information
        let DataBlock::Struct(descriptions) = &decoded.data else {
            return Err(Error::internal(
                "Expected struct data block for descriptions",
            ));
        };
        let mut description_children = descriptions.children.iter();
        let DataBlock::FixedWidth(positions) = description_children.next().expect_ok()? else {
            return Err(Error::internal(
                "Expected fixed width data block for positions",
            ));
        };
        let DataBlock::FixedWidth(sizes) = description_children.next().expect_ok()? else {
            return Err(Error::internal("Expected fixed width data block for sizes"));
        };
        let positions = positions.data.borrow_to_typed_slice::<u64>();
        let sizes = sizes.data.borrow_to_typed_slice::<u64>();

        let mut rep = Vec::with_capacity(num_values as usize);
        let mut def = Vec::with_capacity(num_values as usize);

        for (position, size) in positions.iter().copied().zip(sizes.iter().copied()) {
            if size == 0 {
                if position == 0 {
                    rep.push(0);
                    def.push(0);
                } else {
                    let repval = (position & 0xFFFF) as u16;
                    let defval = ((position >> 16) & 0xFFFF) as u16;
                    rep.push(repval);
                    def.push(defval);
                }
            } else {
                rep.push(0);
                def.push(0);
            }
        }

        let rep = if rep.iter().any(|r| *r != 0) {
            Some(rep)
        } else {
            None
        };
        let def = if self.def_meaning.len() > 1
            || self.def_meaning[0] != DefinitionInterpretation::AllValidItem
        {
            Some(def)
        } else {
            None
        };

        let repdef =
            RepDefUnraveler::new(rep, def, self.def_meaning.clone(), positions.len() as u64);

        Ok(DecodedPage {
            data: decoded.data,
            repdef,
        })
    }
}

struct BlobCacheableState {
    positions: Arc<UInt64Array>,
    sizes: Arc<UInt64Array>,
    inner_state: Arc<dyn CachedPageData>,
}

impl DeepSizeOf for BlobCacheableState {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        (self.positions.as_ref() as &dyn arrow_array::Array).deep_size_of_children(context)
            + (self.sizes.as_ref() as &dyn arrow_array::Array).deep_size_of_children(context)
            + self.inner_state.deep_size_of_children(context)
    }
}

impl CachedPageData for BlobCacheableState {
    fn as_arc_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync + 'static> {
        self
    }
}

#[derive(Debug)]
pub(super) struct BlobPageScheduler {
    inner_scheduler: Box<dyn StructuralPageScheduler>,
    row_number: u64,
    num_rows: u64,
    def_meaning: Arc<[DefinitionInterpretation]>,
    positions: Option<Arc<UInt64Array>>,
    sizes: Option<Arc<UInt64Array>>,
}

impl BlobPageScheduler {
    pub fn new(
        inner_scheduler: Box<dyn StructuralPageScheduler>,
        row_number: u64,
        num_rows: u64,
        def_meaning: Arc<[DefinitionInterpretation]>,
    ) -> Self {
        Self {
            inner_scheduler,
            row_number,
            num_rows,
            def_meaning,
            positions: None,
            sizes: None,
        }
    }

    fn create_page_load_task(
        ranges_to_read: Vec<Range<u64>>,
        mut loaded_blobs: Vec<LoadedBlob>,
        first_row_number: u64,
        io: &dyn EncodingsIo,
        def_meaning: Arc<[DefinitionInterpretation]>,
    ) -> Result<PageLoadTask> {
        let num_rows = loaded_blobs.len() as u64;
        let read_fut = io.submit_request(ranges_to_read, first_row_number);
        let decoder_fut = async move {
            let bytes = read_fut.await?;
            let mut bytes_iter = bytes.into_iter();
            for blob in loaded_blobs.iter_mut() {
                // Empty values have def == 0 too but scheduled no read; their
                // bytes were set at scheduling time.
                if blob.def == 0 && blob.bytes.is_none() {
                    blob.set_bytes(bytes_iter.next().expect_ok()?);
                }
            }
            debug_assert!(bytes_iter.next().is_none());
            Ok(Box::new(BlobPageDecoder::new(loaded_blobs, def_meaning))
                as Box<dyn StructuralPageDecoder>)
        }
        .boxed();
        Ok(PageLoadTask {
            decoder_fut,
            num_rows,
        })
    }
}

impl StructuralPageScheduler for BlobPageScheduler {
    fn initialize<'a>(
        &'a mut self,
        io: &Arc<dyn EncodingsIo>,
    ) -> BoxFuture<'a, Result<Arc<dyn CachedPageData>>> {
        let io = io.clone();
        let num_rows = self.num_rows;
        async move {
            let cached = self.inner_scheduler.initialize(&io).await?;
            let mut desc_decoders = self.inner_scheduler.schedule_ranges(&[0..num_rows], &io)?;
            if desc_decoders.len() != 1 {
                // This can't happen yet today so being a little lazy but if it did happen we just
                // need to concatenate the descriptions.  I'm guessing by then we might be doing something
                // different than "load all descriptors in initialize" anyways.
                return Err(Error::not_supported_source(
                    "Expected exactly one descriptor decoder".into(),
                ));
            }
            let desc_decoder_task = desc_decoders.pop().unwrap();
            let mut desc_decoder = desc_decoder_task.decoder_fut.await?;

            let descs = desc_decoder.drain(desc_decoder_task.num_rows)?;
            let descs = descs.decode()?;
            let descs = make_array(descs.data.into_arrow(BLOB_DESC_TYPE.clone(), true)?);
            let descs = descs.as_struct();
            let positions = Arc::new(
                descs
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .clone(),
            );
            let sizes = Arc::new(
                descs
                    .column(1)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .clone(),
            );
            self.positions = Some(positions.clone());
            self.sizes = Some(sizes.clone());
            let state = Arc::new(BlobCacheableState {
                inner_state: cached,
                positions,
                sizes,
            });
            Ok(state as Arc<dyn CachedPageData>)
        }
        .boxed()
    }

    fn load(&mut self, data: &Arc<dyn CachedPageData>) {
        let blob_state = data
            .clone()
            .as_arc_any()
            .downcast::<BlobCacheableState>()
            .unwrap();
        self.positions = Some(blob_state.positions.clone());
        self.sizes = Some(blob_state.sizes.clone());
        self.inner_scheduler.load(&blob_state.inner_state);
    }

    fn schedule_ranges(
        &self,
        ranges: &[Range<u64>],
        io: &Arc<dyn EncodingsIo>,
    ) -> Result<Vec<PageLoadTask>> {
        let num_rows: u64 = ranges.iter().map(|r| r.end - r.start).sum();

        let positions = self.positions.as_ref().expect_ok()?;
        let sizes = self.sizes.as_ref().expect_ok()?;

        let mut page_load_tasks = Vec::new();
        let mut bytes_so_far = 0;
        let mut ranges_to_read = Vec::with_capacity(num_rows as usize);
        let mut loaded_blobs = Vec::with_capacity(num_rows as usize);
        let mut first_row_number = None;
        for range in ranges {
            for row in range.start..range.end {
                if first_row_number.is_none() {
                    first_row_number = Some(row + self.row_number);
                }
                let position = positions.value(row as usize);
                let size = sizes.value(row as usize);

                if size == 0 {
                    let rep = (position & 0xFFFF) as u16;
                    let def = ((position >> 16) & 0xFFFF) as u16;
                    let mut blob = LoadedBlob::new(rep, def);
                    if def == 0 {
                        // A size-0 descriptor with definition level 0 is a
                        // valid, empty value (nulls carry their non-zero
                        // packed rep/def levels in `position`). No read is
                        // scheduled for it, so it gets its zero-length bytes
                        // here rather than consuming another blob's read
                        // result in the load task.
                        blob.set_bytes(Bytes::new());
                    }
                    loaded_blobs.push(blob);
                } else {
                    loaded_blobs.push(LoadedBlob::new(0, 0));
                    ranges_to_read.push(position..(position + size));
                    bytes_so_far += size;
                }

                if bytes_so_far >= TARGET_SHARD_SIZE {
                    let page_load_task = Self::create_page_load_task(
                        std::mem::take(&mut ranges_to_read),
                        std::mem::take(&mut loaded_blobs),
                        first_row_number.unwrap(),
                        io.as_ref(),
                        self.def_meaning.clone(),
                    )?;
                    page_load_tasks.push(page_load_task);
                    bytes_so_far = 0;
                    first_row_number = None;
                }
            }
        }
        if !loaded_blobs.is_empty() {
            let page_load_task = Self::create_page_load_task(
                std::mem::take(&mut ranges_to_read),
                std::mem::take(&mut loaded_blobs),
                first_row_number.unwrap(),
                io.as_ref(),
                self.def_meaning.clone(),
            )?;
            page_load_tasks.push(page_load_task);
        }

        Ok(page_load_tasks)
    }
}

#[derive(Debug)]
struct LoadedBlob {
    bytes: Option<Bytes>,
    rep: u16,
    def: u16,
}

impl LoadedBlob {
    fn new(rep: u16, def: u16) -> Self {
        Self {
            bytes: None,
            rep,
            def,
        }
    }

    fn set_bytes(&mut self, bytes: Bytes) {
        self.bytes = Some(bytes);
    }
}

#[derive(Debug)]
struct BlobPageDecoder {
    blobs: VecDeque<LoadedBlob>,
    def_meaning: Arc<[DefinitionInterpretation]>,
    num_rows: u64,
}

impl BlobPageDecoder {
    fn new(blobs: Vec<LoadedBlob>, def_meaning: Arc<[DefinitionInterpretation]>) -> Self {
        Self {
            num_rows: blobs.len() as u64,
            blobs: blobs.into_iter().collect(),
            def_meaning,
        }
    }
}

impl StructuralPageDecoder for BlobPageDecoder {
    fn drain(&mut self, num_rows: u64) -> Result<Box<dyn DecodePageTask>> {
        let blobs = self.blobs.drain(0..num_rows as usize).collect::<Vec<_>>();
        Ok(Box::new(BlobDecodePageTask::new(
            blobs,
            self.def_meaning.clone(),
        )))
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }
}

#[derive(Debug)]
struct BlobDecodePageTask {
    blobs: Vec<LoadedBlob>,
    def_meaning: Arc<[DefinitionInterpretation]>,
}

impl BlobDecodePageTask {
    fn new(blobs: Vec<LoadedBlob>, def_meaning: Arc<[DefinitionInterpretation]>) -> Self {
        Self { blobs, def_meaning }
    }
}

impl DecodePageTask for BlobDecodePageTask {
    fn decode(self: Box<Self>) -> Result<DecodedPage> {
        let num_values = self.blobs.len() as u64;
        let num_bytes = self
            .blobs
            .iter()
            .filter_map(|b| b.bytes.as_ref())
            .map(|b| b.len())
            .sum::<usize>();
        let mut buffer = Vec::with_capacity(num_bytes);
        let mut offsets = Vec::with_capacity(num_values as usize + 1);
        let mut rep = Vec::with_capacity(num_values as usize);
        let mut def = Vec::with_capacity(num_values as usize);
        offsets.push(0_u64);
        for blob in self.blobs {
            rep.push(blob.rep);
            def.push(blob.def);
            if let Some(bytes) = blob.bytes {
                offsets.push(offsets.last().unwrap() + bytes.len() as u64);
                buffer.extend_from_slice(&bytes);
            } else {
                // Null / emptyvalue
                offsets.push(*offsets.last().unwrap());
            }
        }
        let offsets = LanceBuffer::reinterpret_vec(offsets);
        let data = LanceBuffer::from(buffer);
        let data_block = DataBlock::VariableWidth(VariableWidthBlock {
            data,
            offsets,
            bits_per_offset: 64,
            num_values,
            block_info: BlockInfo::new(),
        });

        let rep = if rep.iter().any(|r| *r != 0) {
            Some(rep)
        } else {
            None
        };
        let def = if self.def_meaning.len() > 1
            || self.def_meaning[0] != DefinitionInterpretation::AllValidItem
        {
            Some(def)
        } else {
            None
        };

        Ok(DecodedPage {
            data: data_block,
            repdef: RepDefUnraveler::new(rep, def, self.def_meaning, num_values),
        })
    }
}
