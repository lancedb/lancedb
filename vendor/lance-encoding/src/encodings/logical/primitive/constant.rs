// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{any::Any, collections::VecDeque, ops::Range, sync::Arc};

use arrow_array::{Array, ArrayRef, new_empty_array};
use arrow_buffer::ScalarBuffer;
use arrow_schema::DataType;
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;

use lance_core::{
    Error, Result,
    cache::{Context, DeepSizeOf},
};

use crate::{
    EncodingsIo,
    buffer::LanceBuffer,
    decoder::PageEncoding,
    encoder::EncodedPage,
    encodings::logical::primitive::{
        CachedPageData, PageInitialization, PageInitializationBuffers, PageLoadTask,
    },
    format::ProtobufUtils21,
    repdef::{DefinitionInterpretation, RepDefUnraveler, max_visible_level},
};

pub(crate) fn encode_constant_page(
    column_idx: u32,
    scalar: ArrayRef,
    repdef: crate::repdef::SerializedRepDefs,
    row_number: u64,
    num_rows: u64,
) -> Result<EncodedPage> {
    let inline_value = lance_arrow::scalar::try_inline_value(&scalar);
    let value_buffer = if inline_value.is_some() {
        None
    } else {
        Some(LanceBuffer::from(
            lance_arrow::scalar::encode_scalar_value_buffer(&scalar)?,
        ))
    };

    let description = ProtobufUtils21::constant_layout(&repdef.def_meaning, inline_value);

    let has_repdef = repdef.repetition_levels.is_some() || repdef.definition_levels.is_some();

    let data = if !has_repdef {
        value_buffer.into_iter().collect::<Vec<_>>()
    } else {
        let rep_bytes = repdef
            .repetition_levels
            .as_ref()
            .map(|rep| LanceBuffer::reinterpret_slice(rep.clone()))
            .unwrap_or_else(LanceBuffer::empty);
        let def_bytes = repdef
            .definition_levels
            .as_ref()
            .map(|def| LanceBuffer::reinterpret_slice(def.clone()))
            .unwrap_or_else(LanceBuffer::empty);

        match value_buffer {
            Some(value_buffer) => vec![value_buffer, rep_bytes, def_bytes],
            None => vec![rep_bytes, def_bytes],
        }
    };

    Ok(EncodedPage {
        column_idx,
        data,
        description: PageEncoding::Structural(description),
        num_rows,
        row_number,
    })
}

#[derive(Debug)]
struct CachedConstantState {
    scalar: ArrayRef,
    rep: Option<ScalarBuffer<u16>>,
    def: Option<ScalarBuffer<u16>>,
}

impl DeepSizeOf for CachedConstantState {
    fn deep_size_of_children(&self, _ctx: &mut Context) -> usize {
        self.scalar.get_buffer_memory_size()
            + self.rep.as_ref().map(|buf| buf.len() * 2).unwrap_or(0)
            + self.def.as_ref().map(|buf| buf.len() * 2).unwrap_or(0)
    }
}

impl CachedPageData for CachedConstantState {
    fn as_arc_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> {
        self
    }
}

#[derive(Debug, Clone)]
enum ScalarSource {
    Inline(Vec<u8>),
    ValueBuffer(usize),
}

/// The (scalar, rep, def) file ranges a constant page reads, in the order
/// `init_layout` declares and `init_from_buffers` consumes them.  Any field is
/// `None` when that buffer is absent. Computed once at construction so the two
/// halves share one source of truth and their buffer order cannot drift.
#[derive(Debug)]
struct ConstantBufferLayout {
    scalar: Option<Range<u64>>,
    rep: Option<Range<u64>>,
    def: Option<Range<u64>>,
}

#[derive(Debug)]
pub struct ConstantPageScheduler {
    scalar_source: ScalarSource,
    data_type: DataType,
    def_meaning: Arc<[DefinitionInterpretation]>,
    max_rep: u16,
    max_visible_def: u16,
    layout: ConstantBufferLayout,
    repdef: Option<Arc<CachedConstantState>>,
}

impl ConstantPageScheduler {
    pub fn try_new(
        buffer_offsets_and_sizes: Arc<[(u64, u64)]>,
        inline_value: Option<Bytes>,
        data_type: DataType,
        def_meaning: Arc<[DefinitionInterpretation]>,
    ) -> Result<Self> {
        let max_rep = def_meaning.iter().filter(|d| d.is_list()).count() as u16;
        let max_visible_def = max_visible_level(&def_meaning);

        let (scalar_source, rep_buf_idx, def_buf_idx) =
            match (inline_value, buffer_offsets_and_sizes.len()) {
                (Some(inline), 0) => (ScalarSource::Inline(inline.to_vec()), None, None),
                (Some(inline), 2) => (ScalarSource::Inline(inline.to_vec()), Some(0), Some(1)),
                (None, 1) => (ScalarSource::ValueBuffer(0), None, None),
                (None, 3) => (ScalarSource::ValueBuffer(0), Some(1), Some(2)),
                (Some(_inline), 1) => {
                    return Err(Error::invalid_input(format!(
                        "Invalid constant layout: inline_value present with {} buffers",
                        1
                    )));
                }
                (Some(_inline), 3) => {
                    return Err(Error::invalid_input(
                        "Invalid constant layout: inline_value present with 3 buffers",
                    ));
                }
                (None, 0) => {
                    return Err(Error::invalid_input(
                        "Invalid constant layout: missing scalar source",
                    ));
                }
                (None, 2) => {
                    return Err(Error::invalid_input(
                        "Invalid constant layout: ambiguous (2 buffers and no inline_value)",
                    ));
                }
                (Some(_), n) => {
                    return Err(Error::invalid_input(format!(
                        "Invalid constant layout: inline_value present with {} buffers",
                        n
                    )));
                }
                (None, n) => {
                    return Err(Error::invalid_input(format!(
                        "Invalid constant layout: unexpected buffer count {}",
                        n
                    )));
                }
            };

        let range_of = |idx: usize| {
            let (pos, len) = buffer_offsets_and_sizes[idx];
            pos..pos + len
        };
        let layout = ConstantBufferLayout {
            scalar: match &scalar_source {
                ScalarSource::ValueBuffer(idx) => Some(range_of(*idx)),
                ScalarSource::Inline(_) => None,
            },
            rep: rep_buf_idx
                .filter(|&idx| buffer_offsets_and_sizes[idx].1 > 0)
                .map(range_of),
            def: def_buf_idx
                .filter(|&idx| buffer_offsets_and_sizes[idx].1 > 0)
                .map(range_of),
        };

        Ok(Self {
            scalar_source,
            data_type,
            def_meaning,
            max_rep,
            max_visible_def,
            layout,
            repdef: None,
        })
    }
}

impl crate::encodings::logical::primitive::StructuralPageScheduler for ConstantPageScheduler {
    fn init_layout(&self) -> Result<PageInitialization> {
        // Order must match `init_from_buffers`' consumption: scalar, rep, def.
        Ok(PageInitialization::new([
            self.layout.scalar.clone(),
            self.layout.rep.clone(),
            self.layout.def.clone(),
        ]))
    }

    fn init_from_buffers<'a>(
        &'a mut self,
        buffers: PageInitializationBuffers,
        _io: &Arc<dyn EncodingsIo>,
    ) -> BoxFuture<'a, Result<Arc<dyn CachedPageData>>> {
        // Consume `buffers` in the same scalar, rep, def order `init_layout`
        // declared them, using the shared layout's presence flags.
        let scalar_source = self.scalar_source.clone();
        let data_type = self.data_type.clone();
        async move {
            let [scalar_buffer, rep_buffer, def_buffer] = buffers.try_into_array("constant")?;

            let scalar = match (scalar_source, scalar_buffer) {
                (ScalarSource::Inline(inline), None) => {
                    lance_arrow::scalar::decode_scalar_from_inline_value(&data_type, &inline)?
                }
                (ScalarSource::ValueBuffer(_), Some(bytes)) => {
                    let buf = LanceBuffer::from_bytes(bytes, 1);
                    lance_arrow::scalar::decode_scalar_from_value_buffer(&data_type, buf.as_ref())?
                }
                _ => {
                    return Err(Error::internal(
                        "Constant page scalar buffer does not match its initialization layout",
                    ));
                }
            };

            let rep = rep_buffer.map(|rep| {
                let rep = LanceBuffer::from_bytes(rep, 2);
                rep.borrow_to_typed_slice::<u16>()
            });

            let def = def_buffer.map(|def| {
                let def = LanceBuffer::from_bytes(def, 2);
                def.borrow_to_typed_slice::<u16>()
            });

            let cached = Arc::new(CachedConstantState { scalar, rep, def });
            Ok(cached as Arc<dyn CachedPageData>)
        }
        .boxed()
    }

    fn try_load(&mut self, data: &Arc<dyn CachedPageData>) -> Result<()> {
        let cached: Arc<CachedConstantState> =
            data.clone().as_arc_any().downcast().map_err(|_| {
                Error::invalid_input_source(
                    "Cached constant page data has an unexpected type".into(),
                )
            })?;
        if cached.scalar.data_type() != &self.data_type {
            return Err(Error::invalid_input_source(
                format!(
                    "Cached constant page scalar has data type {:?}, expected {:?}",
                    cached.scalar.data_type(),
                    self.data_type
                )
                .into(),
            ));
        }
        self.repdef = Some(cached);
        Ok(())
    }

    fn schedule_ranges(
        &self,
        ranges: &[Range<u64>],
        _io: &Arc<dyn EncodingsIo>,
    ) -> Result<Vec<PageLoadTask>> {
        let num_rows = ranges.iter().map(|r| r.end - r.start).sum::<u64>();
        let decoder = Box::new(ConstantPageDecoder {
            ranges: VecDeque::from_iter(ranges.iter().cloned()),
            scalar: self.repdef.as_ref().unwrap().scalar.clone(),
            rep: self.repdef.as_ref().unwrap().rep.clone(),
            def: self.repdef.as_ref().unwrap().def.clone(),
            def_meaning: self.def_meaning.clone(),
            max_rep: self.max_rep,
            max_visible_def: self.max_visible_def,
            cursor_row: 0,
            cursor_level: 0,
            num_rows,
        })
            as Box<dyn crate::encodings::logical::primitive::StructuralPageDecoder>;
        Ok(vec![PageLoadTask {
            decoder_fut: std::future::ready(Ok(decoder)).boxed(),
            num_rows,
        }])
    }
}

#[derive(Debug)]
struct ConstantPageDecoder {
    ranges: VecDeque<Range<u64>>,
    scalar: ArrayRef,
    rep: Option<ScalarBuffer<u16>>,
    def: Option<ScalarBuffer<u16>>,
    def_meaning: Arc<[DefinitionInterpretation]>,
    max_rep: u16,
    max_visible_def: u16,
    cursor_row: u64,
    cursor_level: usize,
    num_rows: u64,
}

impl ConstantPageDecoder {
    fn drain_ranges(&mut self, num_rows: u64) -> Vec<Range<u64>> {
        let mut rows_desired = num_rows;
        let mut ranges = Vec::with_capacity(self.ranges.len());
        while rows_desired > 0 {
            let front = self.ranges.front_mut().unwrap();
            let avail = front.end - front.start;
            if avail > rows_desired {
                ranges.push(front.start..front.start + rows_desired);
                front.start += rows_desired;
                rows_desired = 0;
            } else {
                ranges.push(self.ranges.pop_front().unwrap());
                rows_desired -= avail;
            }
        }
        ranges
    }

    fn take_row(&mut self) -> Result<(Range<usize>, u64)> {
        let start = self.cursor_level;
        let end = if let Some(rep) = &self.rep {
            if start >= rep.len() {
                return Err(Error::internal(
                    "Invalid constant layout: repetition buffer too short",
                ));
            }
            if rep[start] != self.max_rep {
                return Err(Error::internal(
                    "Invalid constant layout: row did not start at max_rep",
                ));
            }
            let mut end = start + 1;
            while end < rep.len() && rep[end] != self.max_rep {
                end += 1;
            }
            end
        } else {
            start + 1
        };

        let visible = if let Some(def) = &self.def {
            def[start..end]
                .iter()
                .filter(|d| **d <= self.max_visible_def)
                .count() as u64
        } else {
            (end - start) as u64
        };

        self.cursor_level = end;
        self.cursor_row += 1;
        Ok((start..end, visible))
    }

    fn skip_to_row(&mut self, target_row: u64) -> Result<()> {
        while self.cursor_row < target_row {
            self.take_row()?;
        }
        Ok(())
    }
}

impl crate::encodings::logical::primitive::StructuralPageDecoder for ConstantPageDecoder {
    fn drain(&mut self, num_rows: u64) -> Result<Box<dyn crate::decoder::DecodePageTask>> {
        let drained_ranges = self.drain_ranges(num_rows);

        let mut level_slices: Vec<Range<usize>> = Vec::new();
        let mut visible_items_total: u64 = 0;

        for range in drained_ranges {
            self.skip_to_row(range.start)?;
            for _ in range.start..range.end {
                let (level_range, visible) = self.take_row()?;
                visible_items_total += visible;
                if let Some(last) = level_slices.last_mut()
                    && last.end == level_range.start
                {
                    last.end = level_range.end;
                    continue;
                }
                level_slices.push(level_range);
            }
        }

        Ok(Box::new(DecodeConstantTask {
            scalar: self.scalar.clone(),
            rep: self.rep.clone(),
            def: self.def.clone(),
            level_slices,
            visible_items_total,
            def_meaning: self.def_meaning.clone(),
            max_visible_def: self.max_visible_def,
        }))
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }
}

#[derive(Debug)]
struct DecodeConstantTask {
    scalar: ArrayRef,
    rep: Option<ScalarBuffer<u16>>,
    def: Option<ScalarBuffer<u16>>,
    level_slices: Vec<Range<usize>>,
    visible_items_total: u64,
    def_meaning: Arc<[DefinitionInterpretation]>,
    max_visible_def: u16,
}

impl DecodeConstantTask {
    fn slice_levels(
        levels: &Option<ScalarBuffer<u16>>,
        slices: &[Range<usize>],
    ) -> Option<Vec<u16>> {
        levels.as_ref().map(|levels| {
            let total = slices.iter().map(|r| r.end - r.start).sum();
            let mut out = Vec::with_capacity(total);
            for r in slices {
                out.extend(levels[r.start..r.end].iter().copied());
            }
            out
        })
    }

    fn materialize_values(&self, num_values: u64) -> Result<ArrayRef> {
        if num_values == 0 {
            return Ok(new_empty_array(self.scalar.data_type()));
        }

        if let DataType::Struct(fields) = self.scalar.data_type()
            && fields.is_empty()
        {
            return Ok(Arc::new(arrow_array::StructArray::new_empty_fields(
                num_values as usize,
                None,
            )) as ArrayRef);
        }

        let indices = arrow_array::UInt64Array::from(vec![0u64; num_values as usize]);
        Ok(arrow_select::take::take(
            self.scalar.as_ref(),
            &indices,
            None,
        )?)
    }
}

impl crate::decoder::DecodePageTask for DecodeConstantTask {
    fn decode(self: Box<Self>) -> Result<crate::decoder::DecodedPage> {
        let rep = Self::slice_levels(&self.rep, &self.level_slices);
        let def = Self::slice_levels(&self.def, &self.level_slices);

        let visible_items_total = if let Some(def) = &def {
            def.iter().filter(|d| **d <= self.max_visible_def).count() as u64
        } else {
            self.visible_items_total
        };

        let values = self.materialize_values(visible_items_total)?;
        let data = crate::data::DataBlock::from_array(values);
        let unraveler =
            RepDefUnraveler::new(rep, def, self.def_meaning.clone(), visible_items_total);

        Ok(crate::decoder::DecodedPage {
            data,
            repdef: unraveler,
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::BooleanArray;

    use super::*;
    use crate::encodings::logical::primitive::StructuralPageScheduler;

    #[test]
    fn rejects_cached_scalar_with_different_data_type() {
        let mut scheduler = ConstantPageScheduler::try_new(
            Arc::from([]),
            Some(Bytes::new()),
            DataType::UInt8,
            Arc::from([DefinitionInterpretation::AllValidItem]),
        )
        .unwrap();
        let cached: Arc<dyn CachedPageData> = Arc::new(CachedConstantState {
            scalar: Arc::new(BooleanArray::from(vec![true])),
            rep: None,
            def: None,
        });

        let error = scheduler.try_load(&cached).unwrap_err();
        assert!(matches!(error, Error::InvalidInput { .. }));
        assert!(error.to_string().contains("Boolean"));
        assert!(error.to_string().contains("UInt8"));
    }
}
