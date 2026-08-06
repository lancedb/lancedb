// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Data layouts to represent encoded data in a sub-Arrow format
//!
//! These [`DataBlock`] structures represent physical layouts.  They fill a gap somewhere
//! between [`arrow_data::ArrayData`] (which, as a collection of buffers, is too
//! generic because it doesn't give us enough information about what those buffers represent)
//! and [`arrow_array::array::Array`] (which is too specific, because it cares about the
//! logical data type).
//!
//! In addition, the layouts represented here are slightly stricter than Arrow's layout rules.
//! For example, offset buffers MUST start with 0.  These additional restrictions impose a
//! slight penalty on encode (to normalize arrow data) but make the development of encoders
//! and decoders easier (since they can rely on a normalized representation)

use std::{
    ops::Range,
    sync::{Arc, RwLock},
};

use arrow_array::{
    Array, ArrayRef, OffsetSizeTrait, UInt64Array,
    cast::AsArray,
    new_empty_array, new_null_array,
    types::{ArrowDictionaryKeyType, UInt8Type, UInt16Type, UInt32Type, UInt64Type},
};
use arrow_buffer::{ArrowNativeType, BooleanBuffer, BooleanBufferBuilder, NullBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::DataType;
use lance_arrow::DataTypeExt;

use lance_core::{Error, Result};

use crate::{
    buffer::LanceBuffer,
    statistics::{ComputeStat, Stat},
};

/// A data block with no buffers where everything is null
///
/// Note: this data block should not be used for future work.  It will be deprecated
/// in the 2.1 version of the format where nullability will be handled by the structural
/// encoders.
#[derive(Debug, Clone)]
pub struct AllNullDataBlock {
    /// The number of values represented by this block
    pub num_values: u64,
}

impl AllNullDataBlock {
    fn into_arrow(self, data_type: DataType, _validate: bool) -> Result<ArrayData> {
        Ok(ArrayData::new_null(&data_type, self.num_values as usize))
    }

    fn into_buffers(self) -> Vec<LanceBuffer> {
        vec![]
    }
}

use std::collections::HashMap;

// `BlockInfo` stores the statistics of this `DataBlock`, such as `NullCount` for `NullableDataBlock`,
// `BitWidth` for `FixedWidthDataBlock`, `Cardinality` for all `DataBlock`
#[derive(Debug, Clone)]
pub struct BlockInfo(pub Arc<RwLock<HashMap<Stat, Arc<dyn Array>>>>);

impl Default for BlockInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockInfo {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }
}

impl PartialEq for BlockInfo {
    fn eq(&self, other: &Self) -> bool {
        let self_info = self.0.read().unwrap();
        let other_info = other.0.read().unwrap();
        *self_info == *other_info
    }
}

/// Wraps a data block and adds nullability information to it
///
/// Note: this data block should not be used for future work.  It will be deprecated
/// in the 2.1 version of the format where nullability will be handled by the structural
/// encoders.
#[derive(Debug, Clone)]
pub struct NullableDataBlock {
    /// The underlying data
    pub data: Box<DataBlock>,
    /// A bitmap of validity for each value
    pub nulls: LanceBuffer,

    pub block_info: BlockInfo,
}

impl NullableDataBlock {
    fn into_arrow(self, data_type: DataType, validate: bool) -> Result<ArrayData> {
        let nulls = self.nulls.into_buffer();
        let data = self.data.into_arrow(data_type, validate)?.into_builder();
        let data = data.null_bit_buffer(Some(nulls));
        if validate {
            Ok(data.build()?)
        } else {
            Ok(unsafe { data.build_unchecked() })
        }
    }

    fn into_buffers(self) -> Vec<LanceBuffer> {
        let mut buffers = vec![self.nulls];
        buffers.extend(self.data.into_buffers());
        buffers
    }

    pub fn data_size(&self) -> u64 {
        self.data.data_size() + self.nulls.len() as u64
    }
}

/// A block representing the same constant value repeated many times
#[derive(Debug, PartialEq, Clone)]
pub struct ConstantDataBlock {
    /// Data buffer containing the value
    pub data: LanceBuffer,
    /// The number of values
    pub num_values: u64,
}

impl ConstantDataBlock {
    fn into_buffers(self) -> Vec<LanceBuffer> {
        vec![self.data]
    }

    fn into_arrow(self, _data_type: DataType, _validate: bool) -> Result<ArrayData> {
        // We don't need this yet but if we come up with some way of serializing
        // scalars to/from bytes then we could implement it.
        todo!()
    }

    pub fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            data: self.data.clone(),
            num_values: self.num_values,
        })
    }

    pub fn data_size(&self) -> u64 {
        self.data.len() as u64
    }
}

/// A data block for a single buffer of data where each element has a fixed number of bits
#[derive(Debug, PartialEq, Clone)]
pub struct FixedWidthDataBlock {
    /// The data buffer
    pub data: LanceBuffer,
    /// The number of bits per value
    pub bits_per_value: u64,
    /// The number of values represented by this block
    pub num_values: u64,

    pub block_info: BlockInfo,
}

impl FixedWidthDataBlock {
    fn do_into_arrow(
        self,
        data_type: DataType,
        num_values: u64,
        validate: bool,
    ) -> Result<ArrayData> {
        // Booleans expanded for full-zip (bits_per_value==8, one byte each) need re-packing to
        // Arrow's bit-packed format.
        let data_buffer = if matches!(data_type, DataType::Boolean) && self.bits_per_value == 8 {
            let mut builder = BooleanBufferBuilder::new(num_values as usize);
            for &byte in self.data.as_ref().iter().take(num_values as usize) {
                builder.append(byte != 0);
            }
            builder.finish().into_inner()
        } else {
            self.data.into_buffer()
        };
        let builder = ArrayDataBuilder::new(data_type)
            .add_buffer(data_buffer)
            .len(num_values as usize)
            .null_count(0);
        if validate {
            Ok(builder.build()?)
        } else {
            Ok(unsafe { builder.build_unchecked() })
        }
    }

    pub fn into_arrow(self, data_type: DataType, validate: bool) -> Result<ArrayData> {
        let root_num_values = self.num_values;
        self.do_into_arrow(data_type, root_num_values, validate)
    }

    pub fn into_buffers(self) -> Vec<LanceBuffer> {
        vec![self.data]
    }

    pub fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            data: self.data.clone(),
            bits_per_value: self.bits_per_value,
            num_values: self.num_values,
            block_info: self.block_info.clone(),
        })
    }

    pub fn data_size(&self) -> u64 {
        self.data.len() as u64
    }
}

#[derive(Debug)]
struct VariableWidthDataBlockBuilder<T: OffsetSizeTrait> {
    offsets: Vec<T>,
    bytes: Vec<u8>,
}

impl<T: OffsetSizeTrait> VariableWidthDataBlockBuilder<T> {
    fn new(estimated_size_bytes: u64) -> Self {
        Self {
            offsets: vec![T::from_usize(0).unwrap()],
            bytes: Vec::with_capacity(estimated_size_bytes as usize),
        }
    }
}

impl<T: OffsetSizeTrait + bytemuck::Pod> DataBlockBuilderImpl for VariableWidthDataBlockBuilder<T> {
    fn validate_append(&self, data_block: &DataBlock, selection: &Range<u64>) -> Result<()> {
        let block = data_block.as_variable_width_ref().unwrap();
        block.validate_offsets_for_append::<T>(selection)
    }

    fn append_validated(&mut self, data_block: &DataBlock, selection: Range<u64>) -> Result<()> {
        let block = data_block.as_variable_width_ref().unwrap();
        debug_assert_eq!(block.bits_per_offset, T::get_byte_width() as u8 * 8);
        let offsets = block.offsets.borrow_to_typed_view::<T>();

        let start_offset = offsets[selection.start as usize];
        let end_offset = offsets[selection.end as usize];
        let selected_data_len = end_offset.as_usize() - start_offset.as_usize();
        let new_data_len = self
            .bytes
            .len()
            .checked_add(selected_data_len)
            .ok_or_else(|| {
                Error::not_supported_source(
                    "appending variable-width data would overflow usize".into(),
                )
            })?;
        if T::from_usize(new_data_len).is_none() {
            return Err(Error::not_supported_source(
                format!(
                    "appending variable-width data would require {} bytes, which exceeds the \
                     capacity of {}-bit offsets",
                    new_data_len,
                    T::get_byte_width() * 8
                )
                .into(),
            ));
        }
        let previous_len = self.bytes.len();

        self.bytes
            .extend_from_slice(&block.data[start_offset.as_usize()..end_offset.as_usize()]);

        self.offsets.extend(
            offsets[selection.start as usize + 1..=selection.end as usize]
                .iter()
                .map(|&offset| {
                    let rebased_offset =
                        previous_len + (offset.as_usize() - start_offset.as_usize());
                    T::from_usize(rebased_offset).unwrap()
                }),
        );
        Ok(())
    }

    fn finish(self: Box<Self>) -> DataBlock {
        let num_values = (self.offsets.len() - 1) as u64;
        DataBlock::VariableWidth(VariableWidthBlock {
            data: LanceBuffer::from(self.bytes),
            offsets: LanceBuffer::reinterpret_vec(self.offsets),
            bits_per_offset: T::get_byte_width() as u8 * 8,
            num_values,
            block_info: BlockInfo::new(),
        })
    }
}

#[derive(Debug)]
struct BitmapDataBlockBuilder {
    values: BooleanBufferBuilder,
}

impl BitmapDataBlockBuilder {
    fn new(estimated_size_bytes: u64) -> Self {
        Self {
            values: BooleanBufferBuilder::new(estimated_size_bytes as usize * 8),
        }
    }
}

impl DataBlockBuilderImpl for BitmapDataBlockBuilder {
    fn validate_append(&self, _data_block: &DataBlock, _selection: &Range<u64>) -> Result<()> {
        Ok(())
    }

    fn append_validated(&mut self, data_block: &DataBlock, selection: Range<u64>) -> Result<()> {
        let bitmap_blk = data_block.as_fixed_width_ref().unwrap();
        self.values.append_packed_range(
            selection.start as usize..selection.end as usize,
            &bitmap_blk.data,
        );
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> DataBlock {
        let bool_buf = self.values.finish();
        let num_values = bool_buf.len() as u64;
        let bits_buf = bool_buf.into_inner();
        DataBlock::FixedWidth(FixedWidthDataBlock {
            data: LanceBuffer::from(bits_buf),
            bits_per_value: 1,
            num_values,
            block_info: BlockInfo::new(),
        })
    }
}

#[derive(Debug)]
struct FixedWidthDataBlockBuilder {
    bits_per_value: u64,
    bytes_per_value: u64,
    values: Vec<u8>,
}

impl FixedWidthDataBlockBuilder {
    fn new(bits_per_value: u64, estimated_size_bytes: u64) -> Self {
        assert!(bits_per_value.is_multiple_of(8));
        Self {
            bits_per_value,
            bytes_per_value: bits_per_value / 8,
            values: Vec::with_capacity(estimated_size_bytes as usize),
        }
    }
}

impl DataBlockBuilderImpl for FixedWidthDataBlockBuilder {
    fn validate_append(&self, _data_block: &DataBlock, _selection: &Range<u64>) -> Result<()> {
        Ok(())
    }

    fn append_validated(&mut self, data_block: &DataBlock, selection: Range<u64>) -> Result<()> {
        let block = data_block.as_fixed_width_ref().unwrap();
        assert_eq!(self.bits_per_value, block.bits_per_value);
        let start = selection.start as usize * self.bytes_per_value as usize;
        let end = selection.end as usize * self.bytes_per_value as usize;
        self.values.extend_from_slice(&block.data[start..end]);
        Ok(())
    }

    fn finish(self: Box<Self>) -> DataBlock {
        let num_values = (self.values.len() / self.bytes_per_value as usize) as u64;
        DataBlock::FixedWidth(FixedWidthDataBlock {
            data: LanceBuffer::from(self.values),
            bits_per_value: self.bits_per_value,
            num_values,
            block_info: BlockInfo::new(),
        })
    }
}

#[derive(Debug)]
struct StructDataBlockBuilder {
    children: Vec<Box<dyn DataBlockBuilderImpl>>,
}

impl StructDataBlockBuilder {
    fn new(children: Vec<Box<dyn DataBlockBuilderImpl>>) -> Self {
        Self { children }
    }
}

impl DataBlockBuilderImpl for StructDataBlockBuilder {
    fn validate_append(&self, data_block: &DataBlock, selection: &Range<u64>) -> Result<()> {
        let data_block = data_block.as_struct_ref().unwrap();
        for i in 0..self.children.len() {
            self.children[i].validate_append(&data_block.children[i], selection)?;
        }
        Ok(())
    }

    fn append_validated(&mut self, data_block: &DataBlock, selection: Range<u64>) -> Result<()> {
        let data_block = data_block.as_struct_ref().unwrap();
        for i in 0..self.children.len() {
            self.children[i].append_validated(&data_block.children[i], selection.clone())?;
        }
        Ok(())
    }

    fn finish(self: Box<Self>) -> DataBlock {
        let mut children_data_block = Vec::new();
        for child in self.children {
            let child_data_block = child.finish();
            children_data_block.push(child_data_block);
        }
        DataBlock::Struct(StructDataBlock {
            children: children_data_block,
            block_info: BlockInfo::new(),
            validity: None,
        })
    }
}

#[derive(Debug, Default)]
struct AllNullDataBlockBuilder {
    num_values: u64,
}

impl DataBlockBuilderImpl for AllNullDataBlockBuilder {
    fn validate_append(&self, _data_block: &DataBlock, _selection: &Range<u64>) -> Result<()> {
        Ok(())
    }

    fn append_validated(&mut self, _data_block: &DataBlock, selection: Range<u64>) -> Result<()> {
        self.num_values += selection.end - selection.start;
        Ok(())
    }

    fn finish(self: Box<Self>) -> DataBlock {
        DataBlock::AllNull(AllNullDataBlock {
            num_values: self.num_values,
        })
    }
}

/// A data block to represent a fixed size list
#[derive(Debug, Clone)]
pub struct FixedSizeListBlock {
    /// The child data block
    pub child: Box<DataBlock>,
    /// The number of items in each list
    pub dimension: u64,
}

impl FixedSizeListBlock {
    pub fn num_values(&self) -> u64 {
        self.child.num_values() / self.dimension
    }

    /// Try to flatten a FixedSizeListBlock into a FixedWidthDataBlock
    ///
    /// Returns None if any children are nullable
    pub fn try_into_flat(self) -> Option<FixedWidthDataBlock> {
        match *self.child {
            // Cannot flatten a nullable child
            DataBlock::Nullable(_) => None,
            DataBlock::FixedSizeList(inner) => {
                let mut flat = inner.try_into_flat()?;
                flat.bits_per_value *= self.dimension;
                flat.num_values /= self.dimension;
                Some(flat)
            }
            DataBlock::FixedWidth(mut inner) => {
                inner.bits_per_value *= self.dimension;
                inner.num_values /= self.dimension;
                Some(inner)
            }
            _ => panic!(
                "Expected FixedSizeList or FixedWidth data block but found {:?}",
                self
            ),
        }
    }

    pub fn flatten_as_fixed(&mut self) -> FixedWidthDataBlock {
        match self.child.as_mut() {
            DataBlock::FixedSizeList(fsl) => fsl.flatten_as_fixed(),
            DataBlock::FixedWidth(fw) => fw.clone(),
            _ => panic!("Expected FixedSizeList or FixedWidth data block"),
        }
    }

    /// Convert a flattened values block into a FixedSizeListBlock
    pub fn from_flat(data: FixedWidthDataBlock, data_type: &DataType) -> DataBlock {
        match data_type {
            DataType::FixedSizeList(child_field, dimension) => {
                let mut data = data;
                data.bits_per_value /= *dimension as u64;
                data.num_values *= *dimension as u64;
                let child_data = Self::from_flat(data, child_field.data_type());
                DataBlock::FixedSizeList(Self {
                    child: Box::new(child_data),
                    dimension: *dimension as u64,
                })
            }
            // Base case, we've hit a non-list type
            _ => DataBlock::FixedWidth(data),
        }
    }

    fn into_arrow(self, data_type: DataType, validate: bool) -> Result<ArrayData> {
        let num_values = self.num_values();
        let builder = match &data_type {
            DataType::FixedSizeList(child_field, _) => {
                let child_data = self
                    .child
                    .into_arrow(child_field.data_type().clone(), validate)?;
                ArrayDataBuilder::new(data_type)
                    .add_child_data(child_data)
                    .len(num_values as usize)
                    .null_count(0)
            }
            _ => panic!("Expected FixedSizeList data type and got {:?}", data_type),
        };
        if validate {
            Ok(builder.build()?)
        } else {
            Ok(unsafe { builder.build_unchecked() })
        }
    }

    fn into_buffers(self) -> Vec<LanceBuffer> {
        self.child.into_buffers()
    }

    fn data_size(&self) -> u64 {
        self.child.data_size()
    }
}

#[derive(Debug)]
struct FixedSizeListBlockBuilder {
    inner: Box<dyn DataBlockBuilderImpl>,
    dimension: u64,
}

impl FixedSizeListBlockBuilder {
    fn new(inner: Box<dyn DataBlockBuilderImpl>, dimension: u64) -> Self {
        Self { inner, dimension }
    }
}

impl DataBlockBuilderImpl for FixedSizeListBlockBuilder {
    fn validate_append(&self, data_block: &DataBlock, selection: &Range<u64>) -> Result<()> {
        let selection = selection.start * self.dimension..selection.end * self.dimension;
        let fsl = data_block.as_fixed_size_list_ref().unwrap();
        self.inner.validate_append(fsl.child.as_ref(), &selection)
    }

    fn append_validated(&mut self, data_block: &DataBlock, selection: Range<u64>) -> Result<()> {
        let selection = selection.start * self.dimension..selection.end * self.dimension;
        let fsl = data_block.as_fixed_size_list_ref().unwrap();
        self.inner.append_validated(fsl.child.as_ref(), selection)
    }

    fn finish(self: Box<Self>) -> DataBlock {
        let inner_block = self.inner.finish();
        DataBlock::FixedSizeList(FixedSizeListBlock {
            child: Box::new(inner_block),
            dimension: self.dimension,
        })
    }
}

#[derive(Debug)]
struct NullableDataBlockBuilder {
    inner: Box<dyn DataBlockBuilderImpl>,
    validity: BooleanBufferBuilder,
}

impl NullableDataBlockBuilder {
    fn new(inner: Box<dyn DataBlockBuilderImpl>, estimated_size_bytes: usize) -> Self {
        Self {
            inner,
            validity: BooleanBufferBuilder::new(estimated_size_bytes * 8),
        }
    }
}

impl DataBlockBuilderImpl for NullableDataBlockBuilder {
    fn validate_append(&self, data_block: &DataBlock, selection: &Range<u64>) -> Result<()> {
        let nullable = data_block.as_nullable_ref().unwrap();
        self.inner
            .validate_append(nullable.data.as_ref(), selection)
    }

    fn append_validated(&mut self, data_block: &DataBlock, selection: Range<u64>) -> Result<()> {
        let nullable = data_block.as_nullable_ref().unwrap();
        self.inner
            .append_validated(nullable.data.as_ref(), selection.clone())?;
        let bool_buf = BooleanBuffer::new(
            nullable.nulls.clone().into_buffer(),
            selection.start as usize,
            (selection.end - selection.start) as usize,
        );
        self.validity.append_buffer(&bool_buf);
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> DataBlock {
        let inner_block = self.inner.finish();
        DataBlock::Nullable(NullableDataBlock {
            data: Box::new(inner_block),
            nulls: LanceBuffer::from(self.validity.finish().into_inner()),
            block_info: BlockInfo::new(),
        })
    }
}

/// A data block with no regular structure.  There is no available spot to attach
/// validity / repdef information and it cannot be converted to Arrow without being
/// decoded
#[derive(Debug, Clone)]
pub struct OpaqueBlock {
    pub buffers: Vec<LanceBuffer>,
    pub num_values: u64,
    pub block_info: BlockInfo,
}

impl OpaqueBlock {
    pub fn data_size(&self) -> u64 {
        self.buffers.iter().map(|b| b.len() as u64).sum()
    }
}

/// A data block for variable-width data (e.g. strings, packed rows, etc.)
#[derive(Debug, Clone)]
pub struct VariableWidthBlock {
    /// The data buffer
    pub data: LanceBuffer,
    /// The offsets buffer (contains num_values + 1 offsets)
    ///
    /// Offsets MUST start at 0
    pub offsets: LanceBuffer,
    /// The number of bits per offset
    pub bits_per_offset: u8,
    /// The number of values represented by this block
    pub num_values: u64,

    pub block_info: BlockInfo,
}

/// Proof that a [`VariableWidthBlock`] satisfies the Arrow layout contract for
/// its target data type (offsets buffer long enough, offsets monotonic and
/// within the data buffer, values valid UTF-8 where required).
///
/// Only [`VariableWidthBlock::validate_layout`] can construct it, which ties the
/// unchecked Arrow build below to an actual validation pass instead of a
/// caller-controlled flag.
struct ValidVariableWidthLayout;

impl VariableWidthBlock {
    fn append_error(&self, selection: &Range<u64>, detail: impl std::fmt::Display) -> Error {
        Error::corrupt_file_named(
            "variable width data block",
            format!(
                "cannot append offsets for selection {}..{}: {} (num_values: {}, \
                 bits_per_offset: {}, offsets buffer size: {} bytes, data buffer size: {} bytes)",
                selection.start,
                selection.end,
                detail,
                self.num_values,
                self.bits_per_offset,
                self.offsets.len(),
                self.data.len(),
            ),
        )
    }

    fn validate_offsets_for_append<T>(&self, selection: &Range<u64>) -> Result<()>
    where
        T: OffsetSizeTrait + bytemuck::Pod,
    {
        let expected_bits_per_offset = T::get_byte_width() as u8 * 8;
        if self.bits_per_offset != expected_bits_per_offset {
            return Err(self.append_error(
                selection,
                format!(
                    "expected {}-bit offsets but found {}-bit offsets",
                    expected_bits_per_offset, self.bits_per_offset
                ),
            ));
        }
        let offset_size = std::mem::size_of::<T>();
        if !self.offsets.len().is_multiple_of(offset_size) {
            return Err(self.append_error(
                selection,
                format!(
                    "offsets buffer length {} is not a multiple of the {}-byte offset width",
                    self.offsets.len(),
                    offset_size
                ),
            ));
        }
        if selection.start > selection.end || selection.end > self.num_values {
            return Err(
                self.append_error(selection, "selection is outside the block's value range")
            );
        }
        let selection_start = usize::try_from(selection.start)
            .map_err(|_| self.append_error(selection, "selection start does not fit in usize"))?;
        let selection_end = usize::try_from(selection.end)
            .map_err(|_| self.append_error(selection, "selection end does not fit in usize"))?;
        let offsets = self.offsets.borrow_to_typed_view::<T>();
        if selection_end >= offsets.len() {
            return Err(self.append_error(
                selection,
                format!(
                    "selection requires offset {} but the buffer holds {} offsets",
                    selection_end,
                    offsets.len()
                ),
            ));
        }
        let selected_offsets = &offsets[selection_start..=selection_end];
        if let Some(detail) =
            Self::offset_violation_detail(selected_offsets, self.data.len(), selection_start)
        {
            return Err(self.append_error(selection, detail));
        }
        Ok(())
    }

    // The offsets buffer comes straight from file bytes, so an unchecked build would
    // let a corrupt file smuggle out-of-bounds offsets into an Arrow array whose
    // consumers then read (or crash on) memory outside the data buffer.  This
    // boundary therefore always validates the layout, ignoring the optional
    // `validate` flag.  Lance validates the common layouts itself (a branchless
    // scan, measurably cheaper than Arrow's element-wise checked build) and only
    // falls back to Arrow's checked build for the cold cases.
    fn into_arrow(self, data_type: DataType, _validate: bool) -> Result<ArrayData> {
        let Some(expected_bits_per_offset) = Self::expected_bits_per_offset(&data_type) else {
            // Not an [offsets, bytes] layout we know how to prove; let Arrow
            // check it.
            return self.into_arrow_checked(data_type);
        };
        if self.bits_per_offset != expected_bits_per_offset {
            return Err(self.layout_error(
                &data_type,
                format!(
                    "expected {}-bit offsets but got {}-bit offsets",
                    expected_bits_per_offset, self.bits_per_offset
                ),
            ));
        }
        if self.num_values == 0 {
            // Cold path; Arrow handles the empty-offsets special cases.
            return self.into_arrow_checked(data_type);
        }
        let proof = self.validate_layout(&data_type)?;
        Ok(self.into_arrow_unchecked(data_type, proof))
    }

    /// The offset width Arrow mandates for `data_type`, or `None` if the type
    /// does not use the `[offsets, bytes]` layout this block represents.
    fn expected_bits_per_offset(data_type: &DataType) -> Option<u8> {
        match data_type {
            DataType::Binary | DataType::Utf8 => Some(32),
            DataType::LargeBinary | DataType::LargeUtf8 => Some(64),
            _ => None,
        }
    }

    fn layout_error(&self, data_type: &DataType, detail: impl std::fmt::Display) -> Error {
        Self::format_layout_error(
            data_type,
            detail,
            self.num_values,
            self.bits_per_offset,
            self.offsets.len(),
            self.data.len(),
        )
    }

    fn format_layout_error(
        data_type: &DataType,
        detail: impl std::fmt::Display,
        num_values: u64,
        bits_per_offset: u8,
        offsets_size: usize,
        data_size: usize,
    ) -> Error {
        Error::corrupt_file_named(
            "variable width data block",
            format!(
                "invalid variable-width layout for {}: {} (num_values: {}, bits_per_offset: {}, \
                 offsets buffer size: {} bytes, data buffer size: {} bytes)",
                data_type, detail, num_values, bits_per_offset, offsets_size, data_size,
            ),
        )
    }

    fn validate_layout(&self, data_type: &DataType) -> Result<ValidVariableWidthLayout> {
        let bytes_per_offset = (self.bits_per_offset / 8) as u64;
        let required_bytes = self
            .num_values
            .checked_add(1)
            .and_then(|num_offsets| num_offsets.checked_mul(bytes_per_offset))
            .ok_or_else(|| self.layout_error(data_type, "offsets buffer size overflows"))?;
        if (self.offsets.len() as u64) < required_bytes {
            return Err(self.layout_error(
                data_type,
                format!(
                    "offsets buffer must hold at least {} offsets ({} bytes)",
                    self.num_values + 1,
                    required_bytes
                ),
            ));
        }
        let validate_utf8 = matches!(data_type, DataType::Utf8 | DataType::LargeUtf8);
        match self.bits_per_offset {
            32 => self.validate_offsets_and_values::<i32>(data_type, validate_utf8),
            64 => self.validate_offsets_and_values::<i64>(data_type, validate_utf8),
            other => Err(self.layout_error(
                data_type,
                format!("unsupported offset width: {} bits", other),
            )),
        }
    }

    fn validate_offsets_and_values<T: ArrowNativeType + Ord>(
        &self,
        data_type: &DataType,
        validate_utf8: bool,
    ) -> Result<ValidVariableWidthLayout> {
        let num_offsets = self.num_values as usize + 1;
        // Slice before borrowing: the buffer may carry padding that is not a
        // multiple of the offset width.
        let offsets = self
            .offsets
            .slice_with_length(0, num_offsets * std::mem::size_of::<T>());
        let offsets = offsets.borrow_to_typed_slice::<T>();
        let offsets: &[T] = offsets.as_ref();
        let data = self.data.as_ref();

        if let Some(detail) = Self::offset_violation_detail(offsets, data.len(), 0) {
            return Err(self.layout_error(data_type, detail));
        }

        if validate_utf8 {
            let (first, last) = (offsets[0].as_usize(), offsets[num_offsets - 1].as_usize());
            let values = std::str::from_utf8(&data[first..last])
                .map_err(|utf8_err| self.layout_error(data_type, utf8_err))?;
            let mut on_char_boundaries = true;
            for &offset in offsets {
                on_char_boundaries &= values.is_char_boundary(offset.as_usize() - first);
            }
            if !on_char_boundaries {
                // Cold path: rescan to pinpoint the offending offset.
                let position = offsets
                    .iter()
                    .position(|offset| !values.is_char_boundary(offset.as_usize() - first))
                    .expect("the fast scan found a non-boundary offset");
                return Err(self.layout_error(
                    data_type,
                    format!("offset at position {position} splits a UTF-8 character"),
                ));
            }
        }

        Ok(ValidVariableWidthLayout)
    }

    fn offset_violation_detail<T: ArrowNativeType + Ord>(
        offsets: &[T],
        data_size: usize,
        position_base: usize,
    ) -> Option<String> {
        // A monotonic sequence with a non-negative first offset and an
        // in-bounds last offset is entirely within [0, data_size].  Keep this
        // valid path branchless so it vectorizes, and only rescan on failure.
        let mut is_monotonic = true;
        for window in offsets.windows(2) {
            is_monotonic &= window[0] <= window[1];
        }
        let first = offsets[0];
        let last = offsets[offsets.len() - 1];
        let bounds_ok =
            first >= T::usize_as(0) && last.to_usize().is_some_and(|last| last <= data_size);
        if is_monotonic && bounds_ok {
            return None;
        }

        for (relative_position, window) in offsets.windows(2).enumerate() {
            if window[0] > window[1] {
                let position = position_base + relative_position + 1;
                return Some(format!(
                    "non-monotonic offset at position {}: {:?} decreases from {:?}",
                    position, window[1], window[0]
                ));
            }
        }
        for (relative_position, offset) in offsets.iter().enumerate() {
            let position = position_base + relative_position;
            match offset.to_usize() {
                None => {
                    return Some(format!(
                        "offset at position {} is negative: {:?}",
                        position, offset
                    ));
                }
                Some(offset) if offset > data_size => {
                    return Some(format!(
                        "offset at position {} is out of bounds: {} > {}",
                        position, offset, data_size
                    ));
                }
                Some(_) => {}
            }
        }
        Some("offsets failed validation".to_string())
    }

    fn into_arrow_checked(self, data_type: DataType) -> Result<ArrayData> {
        let num_values = self.num_values;
        let bits_per_offset = self.bits_per_offset;
        let offsets_size = self.offsets.len();
        let data_size = self.data.len();
        let builder = self.into_arrow_builder(data_type.clone());
        builder.build().map_err(|arrow_err| {
            Self::format_layout_error(
                &data_type,
                arrow_err,
                num_values,
                bits_per_offset,
                offsets_size,
                data_size,
            )
        })
    }

    fn into_arrow_unchecked(
        self,
        data_type: DataType,
        _proof: ValidVariableWidthLayout,
    ) -> ArrayData {
        let builder = self.into_arrow_builder(data_type);
        // SAFETY: `_proof` witnesses that `validate_layout` proved this block
        // satisfies the Arrow layout contract for `data_type`.
        unsafe { builder.build_unchecked() }
    }

    fn into_arrow_builder(self, data_type: DataType) -> ArrayDataBuilder {
        let num_values = self.num_values;
        let data_buffer = self.data.into_buffer();
        let offsets_buffer = self.offsets.into_buffer();
        ArrayDataBuilder::new(data_type)
            .add_buffer(offsets_buffer)
            .add_buffer(data_buffer)
            .len(num_values as usize)
            .null_count(0)
    }

    fn into_buffers(self) -> Vec<LanceBuffer> {
        vec![self.offsets, self.data]
    }

    pub fn offsets_as_block(&mut self) -> DataBlock {
        let offsets = self.offsets.clone();
        DataBlock::FixedWidth(FixedWidthDataBlock {
            data: offsets,
            bits_per_value: self.bits_per_offset as u64,
            num_values: self.num_values + 1,
            block_info: BlockInfo::new(),
        })
    }

    pub fn data_size(&self) -> u64 {
        (self.data.len() + self.offsets.len()) as u64
    }
}

/// A data block representing a struct
#[derive(Debug, Clone)]
pub struct StructDataBlock {
    /// The child arrays
    pub children: Vec<DataBlock>,
    pub block_info: BlockInfo,
    /// The validity bitmap for the struct (None means all valid)
    pub validity: Option<NullBuffer>,
}

impl StructDataBlock {
    fn into_arrow(self, data_type: DataType, validate: bool) -> Result<ArrayData> {
        if let DataType::Struct(fields) = &data_type {
            let mut builder = ArrayDataBuilder::new(DataType::Struct(fields.clone()));
            let mut num_rows = 0;
            for (field, child) in fields.iter().zip(self.children) {
                let child_data = child.into_arrow(field.data_type().clone(), validate)?;
                num_rows = child_data.len();
                builder = builder.add_child_data(child_data);
            }

            // Apply validity if present
            let builder = if let Some(validity) = self.validity {
                let null_count = validity.null_count();
                builder
                    .null_bit_buffer(Some(validity.into_inner().into_inner()))
                    .null_count(null_count)
            } else {
                builder.null_count(0)
            };

            let builder = builder.len(num_rows);
            if validate {
                Ok(builder.build()?)
            } else {
                Ok(unsafe { builder.build_unchecked() })
            }
        } else {
            Err(Error::internal(format!(
                "Expected Struct, got {:?}",
                data_type
            )))
        }
    }

    fn remove_outer_validity(self) -> Self {
        Self {
            children: self
                .children
                .into_iter()
                .map(|c| c.remove_outer_validity())
                .collect(),
            block_info: self.block_info,
            validity: None, // Remove the validity
        }
    }

    fn into_buffers(self) -> Vec<LanceBuffer> {
        self.children
            .into_iter()
            .flat_map(|c| c.into_buffers())
            .collect()
    }

    pub fn has_variable_width_child(&self) -> bool {
        self.children
            .iter()
            .any(|child| !matches!(child, DataBlock::FixedWidth(_)))
    }

    pub fn data_size(&self) -> u64 {
        self.children
            .iter()
            .map(|data_block| data_block.data_size())
            .sum()
    }
}

/// A data block for dictionary encoded data
#[derive(Debug, Clone)]
pub struct DictionaryDataBlock {
    /// The indices buffer
    pub indices: FixedWidthDataBlock,
    /// The dictionary itself
    pub dictionary: Box<DataBlock>,
}

impl DictionaryDataBlock {
    fn decode_helper<K: ArrowDictionaryKeyType>(self) -> Result<DataBlock> {
        // Handle empty batch - this can happen when decoding a range that contains
        // only empty/null lists, or when reading sparse data
        if self.indices.num_values == 0 {
            return Ok(DataBlock::AllNull(AllNullDataBlock { num_values: 0 }));
        }

        // assume the indices are uniformly distributed.
        let estimated_size_bytes = self.dictionary.data_size()
            * (self.indices.num_values + self.dictionary.num_values() - 1)
            / self.dictionary.num_values();
        let mut data_builder = DataBlockBuilder::with_capacity_estimate(estimated_size_bytes);

        let indices = self.indices.data.borrow_to_typed_slice::<K::Native>();
        let indices = indices.as_ref();

        let selections = indices.iter().map(|idx| {
            let idx = idx.to_usize().unwrap() as u64;
            idx..idx + 1
        });
        data_builder.append_ranges(&self.dictionary, selections)?;

        Ok(data_builder.finish())
    }

    pub fn decode(self) -> Result<DataBlock> {
        match self.indices.bits_per_value {
            8 => self.decode_helper::<UInt8Type>(),
            16 => self.decode_helper::<UInt16Type>(),
            32 => self.decode_helper::<UInt32Type>(),
            64 => self.decode_helper::<UInt64Type>(),
            _ => Err(lance_core::Error::internal(format!(
                "Unsupported dictionary index bit width: {} bits",
                self.indices.bits_per_value
            ))),
        }
    }

    fn into_arrow_dict(
        self,
        key_type: Box<DataType>,
        value_type: Box<DataType>,
        validate: bool,
    ) -> Result<ArrayData> {
        let indices = self.indices.into_arrow((*key_type).clone(), validate)?;
        let dictionary = self
            .dictionary
            .into_arrow((*value_type).clone(), validate)?;

        let builder = indices
            .into_builder()
            .add_child_data(dictionary)
            .data_type(DataType::Dictionary(key_type, value_type));

        if validate {
            Ok(builder.build()?)
        } else {
            Ok(unsafe { builder.build_unchecked() })
        }
    }

    fn into_arrow(self, data_type: DataType, validate: bool) -> Result<ArrayData> {
        if let DataType::Dictionary(key_type, value_type) = data_type {
            self.into_arrow_dict(key_type, value_type, validate)
        } else {
            self.decode()?.into_arrow(data_type, validate)
        }
    }

    fn into_buffers(self) -> Vec<LanceBuffer> {
        let mut buffers = self.indices.into_buffers();
        buffers.extend(self.dictionary.into_buffers());
        buffers
    }

    pub fn into_parts(self) -> (DataBlock, DataBlock) {
        (DataBlock::FixedWidth(self.indices), *self.dictionary)
    }

    pub fn from_parts(indices: FixedWidthDataBlock, dictionary: DataBlock) -> Self {
        Self {
            indices,
            dictionary: Box::new(dictionary),
        }
    }
}

/// A DataBlock is a collection of buffers that represents an "array" of data in very generic terms
///
/// The output of each decoder is a DataBlock.  Decoders can be chained together to transform
/// one DataBlock into a different kind of DataBlock.
///
/// The DataBlock is somewhere in between Arrow's ArrayData and Array and represents a physical
/// layout of the data.
///
/// A DataBlock can be converted into an Arrow ArrayData (and then Array) for a given array type.
/// For example, a FixedWidthDataBlock can be converted into any primitive type or a fixed size
/// list of a primitive type.  This is a zero-copy operation.
///
/// In addition, a DataBlock can be created from an Arrow array or arrays.  This is not a zero-copy
/// operation as some normalization may be required.
#[derive(Debug, Clone)]
pub enum DataBlock {
    Empty(),
    Constant(ConstantDataBlock),
    AllNull(AllNullDataBlock),
    Nullable(NullableDataBlock),
    FixedWidth(FixedWidthDataBlock),
    FixedSizeList(FixedSizeListBlock),
    VariableWidth(VariableWidthBlock),
    Opaque(OpaqueBlock),
    Struct(StructDataBlock),
    Dictionary(DictionaryDataBlock),
}

impl DataBlock {
    /// Convert self into an Arrow ArrayData
    pub fn into_arrow(self, data_type: DataType, validate: bool) -> Result<ArrayData> {
        match self {
            Self::Empty() => Ok(new_empty_array(&data_type).to_data()),
            Self::Constant(inner) => inner.into_arrow(data_type, validate),
            Self::AllNull(inner) => inner.into_arrow(data_type, validate),
            Self::Nullable(inner) => inner.into_arrow(data_type, validate),
            Self::FixedWidth(inner) => inner.into_arrow(data_type, validate),
            Self::FixedSizeList(inner) => inner.into_arrow(data_type, validate),
            Self::VariableWidth(inner) => inner.into_arrow(data_type, validate),
            Self::Struct(inner) => inner.into_arrow(data_type, validate),
            Self::Dictionary(inner) => inner.into_arrow(data_type, validate),
            Self::Opaque(_) => Err(Error::internal(
                "Cannot convert OpaqueBlock to Arrow".to_string(),
            )),
        }
    }

    /// Convert the data block into a collection of buffers for serialization
    ///
    /// The order matters and will be used to reconstruct the data block at read time.
    pub fn into_buffers(self) -> Vec<LanceBuffer> {
        match self {
            Self::Empty() => Vec::default(),
            Self::Constant(inner) => inner.into_buffers(),
            Self::AllNull(inner) => inner.into_buffers(),
            Self::Nullable(inner) => inner.into_buffers(),
            Self::FixedWidth(inner) => inner.into_buffers(),
            Self::FixedSizeList(inner) => inner.into_buffers(),
            Self::VariableWidth(inner) => inner.into_buffers(),
            Self::Struct(inner) => inner.into_buffers(),
            Self::Dictionary(inner) => inner.into_buffers(),
            Self::Opaque(inner) => inner.buffers,
        }
    }

    /// Converts the data buffers into borrowed mode and clones the block
    ///
    /// This is a zero-copy operation but requires a mutable reference to self and, afterwards,
    /// all buffers will be in Borrowed mode.
    /// Try and clone the block
    ///
    /// This will fail if any buffers are in owned mode.  You can call borrow_and_clone() to
    /// ensure that all buffers are in borrowed mode before calling this method.
    pub fn try_clone(&self) -> Result<Self> {
        match self {
            Self::Empty() => Ok(Self::Empty()),
            Self::Constant(inner) => Ok(Self::Constant(inner.clone())),
            Self::AllNull(inner) => Ok(Self::AllNull(inner.clone())),
            Self::Nullable(inner) => Ok(Self::Nullable(inner.clone())),
            Self::FixedWidth(inner) => Ok(Self::FixedWidth(inner.clone())),
            Self::FixedSizeList(inner) => Ok(Self::FixedSizeList(inner.clone())),
            Self::VariableWidth(inner) => Ok(Self::VariableWidth(inner.clone())),
            Self::Struct(inner) => Ok(Self::Struct(inner.clone())),
            Self::Dictionary(inner) => Ok(Self::Dictionary(inner.clone())),
            Self::Opaque(inner) => Ok(Self::Opaque(inner.clone())),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Constant(_) => "Constant",
            Self::Empty() => "Empty",
            Self::AllNull(_) => "AllNull",
            Self::Nullable(_) => "Nullable",
            Self::FixedWidth(_) => "FixedWidth",
            Self::FixedSizeList(_) => "FixedSizeList",
            Self::VariableWidth(_) => "VariableWidth",
            Self::Struct(_) => "Struct",
            Self::Dictionary(_) => "Dictionary",
            Self::Opaque(_) => "Opaque",
        }
    }

    pub fn is_variable(&self) -> bool {
        match self {
            Self::Constant(_) => false,
            Self::Empty() => false,
            Self::AllNull(_) => false,
            Self::Nullable(nullable) => nullable.data.is_variable(),
            Self::FixedWidth(_) => false,
            Self::FixedSizeList(fsl) => fsl.child.is_variable(),
            Self::VariableWidth(_) => true,
            Self::Struct(strct) => strct.children.iter().any(|c| c.is_variable()),
            Self::Dictionary(_) => {
                todo!("is_variable for DictionaryDataBlock is not implemented yet")
            }
            Self::Opaque(_) => panic!("Does not make sense to ask if an Opaque block is variable"),
        }
    }

    pub fn is_nullable(&self) -> bool {
        match self {
            Self::AllNull(_) => true,
            Self::Nullable(_) => true,
            Self::FixedSizeList(fsl) => fsl.child.is_nullable(),
            Self::Struct(strct) => strct.children.iter().any(|c| c.is_nullable()),
            Self::Dictionary(_) => {
                todo!("is_nullable for DictionaryDataBlock is not implemented yet")
            }
            Self::Opaque(_) => panic!("Does not make sense to ask if an Opaque block is nullable"),
            _ => false,
        }
    }

    /// The number of values in the block
    ///
    /// This function does not recurse into child blocks.  If this is a FSL then it will
    /// be the number of lists and not the number of items.
    pub fn num_values(&self) -> u64 {
        match self {
            Self::Empty() => 0,
            Self::Constant(inner) => inner.num_values,
            Self::AllNull(inner) => inner.num_values,
            Self::Nullable(inner) => inner.data.num_values(),
            Self::FixedWidth(inner) => inner.num_values,
            Self::FixedSizeList(inner) => inner.num_values(),
            Self::VariableWidth(inner) => inner.num_values,
            Self::Struct(inner) => inner.children[0].num_values(),
            Self::Dictionary(inner) => inner.indices.num_values,
            Self::Opaque(inner) => inner.num_values,
        }
    }

    /// The number of items in a single row
    ///
    /// This is always 1 unless there are layers of FSL
    pub fn items_per_row(&self) -> u64 {
        match self {
            Self::Empty() => todo!(),     // Leave undefined until needed
            Self::Constant(_) => todo!(), // Leave undefined until needed
            Self::AllNull(_) => todo!(),  // Leave undefined until needed
            Self::Nullable(nullable) => nullable.data.items_per_row(),
            Self::FixedWidth(_) => 1,
            Self::FixedSizeList(fsl) => fsl.dimension * fsl.child.items_per_row(),
            Self::VariableWidth(_) => 1,
            Self::Struct(_) => todo!(), // Leave undefined until needed
            Self::Dictionary(_) => 1,
            Self::Opaque(_) => 1,
        }
    }

    /// The number of bytes in the data block (including any child blocks)
    pub fn data_size(&self) -> u64 {
        match self {
            Self::Empty() => 0,
            Self::Constant(inner) => inner.data_size(),
            Self::AllNull(_) => 0,
            Self::Nullable(inner) => inner.data_size(),
            Self::FixedWidth(inner) => inner.data_size(),
            Self::FixedSizeList(inner) => inner.data_size(),
            Self::VariableWidth(inner) => inner.data_size(),
            Self::Struct(inner) => inner.children.iter().map(|child| child.data_size()).sum(),
            Self::Dictionary(inner) => inner.indices.data_size() + inner.dictionary.data_size(),
            Self::Opaque(inner) => inner.data_size(),
        }
    }

    /// Removes any validity information from the block
    ///
    /// This does not filter the block (e.g. remove rows).  It only removes
    /// the validity bitmaps (if present).  Any garbage masked by null bits
    /// will now appear as proper values.
    ///
    /// If `recurse` is true, then this will also remove validity from any child blocks.
    pub fn remove_outer_validity(self) -> Self {
        match self {
            Self::AllNull(_) => panic!("Cannot remove validity on all-null data"),
            Self::Nullable(inner) => *inner.data,
            Self::Struct(inner) => Self::Struct(inner.remove_outer_validity()),
            other => other,
        }
    }

    fn make_builder(&self, estimated_size_bytes: u64) -> Box<dyn DataBlockBuilderImpl> {
        match self {
            Self::FixedWidth(inner) => {
                if inner.bits_per_value == 1 {
                    Box::new(BitmapDataBlockBuilder::new(estimated_size_bytes))
                } else {
                    Box::new(FixedWidthDataBlockBuilder::new(
                        inner.bits_per_value,
                        estimated_size_bytes,
                    ))
                }
            }
            Self::VariableWidth(inner) => {
                if inner.bits_per_offset == 32 {
                    Box::new(VariableWidthDataBlockBuilder::<i32>::new(
                        estimated_size_bytes,
                    ))
                } else if inner.bits_per_offset == 64 {
                    Box::new(VariableWidthDataBlockBuilder::<i64>::new(
                        estimated_size_bytes,
                    ))
                } else {
                    todo!()
                }
            }
            Self::FixedSizeList(inner) => {
                let inner_builder = inner.child.make_builder(estimated_size_bytes);
                Box::new(FixedSizeListBlockBuilder::new(
                    inner_builder,
                    inner.dimension,
                ))
            }
            Self::Nullable(nullable) => {
                // There's no easy way to know what percentage of the data is in the valiidty buffer
                // but 1/16th seems like a reasonable guess.
                let estimated_validity_size_bytes = estimated_size_bytes / 16;
                let inner_builder = nullable
                    .data
                    .make_builder(estimated_size_bytes - estimated_validity_size_bytes);
                Box::new(NullableDataBlockBuilder::new(
                    inner_builder,
                    estimated_validity_size_bytes as usize,
                ))
            }
            Self::Struct(struct_data_block) => {
                let num_children = struct_data_block.children.len();
                let per_child_estimate = if num_children == 0 {
                    0
                } else {
                    estimated_size_bytes / num_children as u64
                };
                let child_builders = struct_data_block
                    .children
                    .iter()
                    .map(|child| child.make_builder(per_child_estimate))
                    .collect();
                Box::new(StructDataBlockBuilder::new(child_builders))
            }
            Self::AllNull(_) => Box::new(AllNullDataBlockBuilder::default()),
            _ => todo!("make_builder for {:?}", self),
        }
    }
}

macro_rules! as_type {
    ($fn_name:ident, $inner:tt, $inner_type:ident) => {
        pub fn $fn_name(self) -> Option<$inner_type> {
            match self {
                Self::$inner(inner) => Some(inner),
                _ => None,
            }
        }
    };
}

macro_rules! as_type_ref {
    ($fn_name:ident, $inner:tt, $inner_type:ident) => {
        pub fn $fn_name(&self) -> Option<&$inner_type> {
            match self {
                Self::$inner(inner) => Some(inner),
                _ => None,
            }
        }
    };
}

macro_rules! as_type_ref_mut {
    ($fn_name:ident, $inner:tt, $inner_type:ident) => {
        pub fn $fn_name(&mut self) -> Option<&mut $inner_type> {
            match self {
                Self::$inner(inner) => Some(inner),
                _ => None,
            }
        }
    };
}

// Cast implementations
impl DataBlock {
    as_type!(as_all_null, AllNull, AllNullDataBlock);
    as_type!(as_nullable, Nullable, NullableDataBlock);
    as_type!(as_fixed_width, FixedWidth, FixedWidthDataBlock);
    as_type!(as_fixed_size_list, FixedSizeList, FixedSizeListBlock);
    as_type!(as_variable_width, VariableWidth, VariableWidthBlock);
    as_type!(as_struct, Struct, StructDataBlock);
    as_type!(as_dictionary, Dictionary, DictionaryDataBlock);
    as_type_ref!(as_all_null_ref, AllNull, AllNullDataBlock);
    as_type_ref!(as_nullable_ref, Nullable, NullableDataBlock);
    as_type_ref!(as_fixed_width_ref, FixedWidth, FixedWidthDataBlock);
    as_type_ref!(as_fixed_size_list_ref, FixedSizeList, FixedSizeListBlock);
    as_type_ref!(as_variable_width_ref, VariableWidth, VariableWidthBlock);
    as_type_ref!(as_struct_ref, Struct, StructDataBlock);
    as_type_ref!(as_dictionary_ref, Dictionary, DictionaryDataBlock);
    as_type_ref_mut!(as_all_null_ref_mut, AllNull, AllNullDataBlock);
    as_type_ref_mut!(as_nullable_ref_mut, Nullable, NullableDataBlock);
    as_type_ref_mut!(as_fixed_width_ref_mut, FixedWidth, FixedWidthDataBlock);
    as_type_ref_mut!(
        as_fixed_size_list_ref_mut,
        FixedSizeList,
        FixedSizeListBlock
    );
    as_type_ref_mut!(as_variable_width_ref_mut, VariableWidth, VariableWidthBlock);
    as_type_ref_mut!(as_struct_ref_mut, Struct, StructDataBlock);
    as_type_ref_mut!(as_dictionary_ref_mut, Dictionary, DictionaryDataBlock);
}

// Methods to convert from Arrow -> DataBlock

fn get_byte_range<T: ArrowNativeType>(offsets: &mut LanceBuffer) -> Range<usize> {
    let offsets = offsets.borrow_to_typed_slice::<T>();
    if offsets.as_ref().is_empty() {
        0..0
    } else {
        offsets.as_ref().first().unwrap().as_usize()..offsets.as_ref().last().unwrap().as_usize()
    }
}

// Given multiple offsets arrays [0, 5, 10], [0, 3, 7], etc. stitch
// them together to get [0, 5, 10, 13, 20, ...]
//
// Also returns the data range referenced by each offset array (may
// not be 0..len if there is slicing involved)
fn stitch_offsets<T: ArrowNativeType + std::ops::Add<Output = T> + std::ops::Sub<Output = T>>(
    offsets: Vec<LanceBuffer>,
) -> (LanceBuffer, Vec<Range<usize>>) {
    if offsets.is_empty() {
        return (LanceBuffer::empty(), Vec::default());
    }
    let len = offsets.iter().map(|b| b.len()).sum::<usize>();
    // Note: we are making a copy here, even if there is only one input, because we want to
    // normalize that input if it doesn't start with zero.  This could be micro-optimized out
    // if needed.
    let mut dest = Vec::with_capacity(len);
    let mut byte_ranges = Vec::with_capacity(offsets.len());

    // We insert one leading 0 before processing any of the inputs
    dest.push(T::from_usize(0).unwrap());

    for mut o in offsets.into_iter() {
        if !o.is_empty() {
            let last_offset = *dest.last().unwrap();
            let o = o.borrow_to_typed_slice::<T>();
            let start = *o.as_ref().first().unwrap();
            // First, we skip the first offset
            // Then, we subtract that first offset from each remaining offset
            //
            // This gives us a 0-based offset array (minus the leading 0)
            //
            // Then we add the last offset from the previous array to each offset
            // which shifts our offset array to the correct position
            //
            // For example, let's assume the last offset from the previous array
            // was 10 and we are given [13, 17, 22].  This means we have two values with
            // length 4 (17 - 13) and 5 (22 - 17).  The output from this step will be
            // [14, 19].  Combined with our last offset of 10, this gives us [10, 14, 19]
            // which is our same two values of length 4 and 5.
            dest.extend(o.as_ref()[1..].iter().map(|&x| x + last_offset - start));
        }
        byte_ranges.push(get_byte_range::<T>(&mut o));
    }
    (LanceBuffer::reinterpret_vec(dest), byte_ranges)
}

fn arrow_binary_to_data_block(
    arrays: &[ArrayRef],
    num_values: u64,
    bits_per_offset: u8,
) -> DataBlock {
    let data_vec = arrays.iter().map(|arr| arr.to_data()).collect::<Vec<_>>();
    let bytes_per_offset = bits_per_offset as usize / 8;
    let offsets = data_vec
        .iter()
        .map(|d| {
            LanceBuffer::from(
                d.buffers()[0].slice_with_length(d.offset(), (d.len() + 1) * bytes_per_offset),
            )
        })
        .collect::<Vec<_>>();
    let (offsets, data_ranges) = if bits_per_offset == 32 {
        stitch_offsets::<i32>(offsets)
    } else {
        stitch_offsets::<i64>(offsets)
    };
    let data = data_vec
        .iter()
        .zip(data_ranges)
        .map(|(d, byte_range)| {
            LanceBuffer::from(
                d.buffers()[1]
                    .slice_with_length(byte_range.start, byte_range.end - byte_range.start),
            )
        })
        .collect::<Vec<_>>();
    let data = LanceBuffer::concat_into_one(data);
    DataBlock::VariableWidth(VariableWidthBlock {
        data,
        offsets,
        bits_per_offset,
        num_values,
        block_info: BlockInfo::new(),
    })
}

fn encode_flat_data(arrays: &[ArrayRef], num_values: u64) -> LanceBuffer {
    let bytes_per_value = arrays[0].data_type().byte_width();
    let mut buffer = Vec::with_capacity(num_values as usize * bytes_per_value);
    for arr in arrays {
        let data = arr.to_data();
        buffer.extend_from_slice(data.buffers()[0].as_slice());
    }
    LanceBuffer::from(buffer)
}

fn do_encode_bitmap_data(bitmaps: &[BooleanBuffer], num_values: u64) -> LanceBuffer {
    let mut builder = BooleanBufferBuilder::new(num_values as usize);

    for buf in bitmaps {
        builder.append_buffer(buf);
    }

    let buffer = builder.finish().into_inner();
    LanceBuffer::from(buffer)
}

fn encode_bitmap_data(arrays: &[ArrayRef], num_values: u64) -> LanceBuffer {
    let bitmaps = arrays
        .iter()
        .map(|arr| arr.as_boolean().values().clone())
        .collect::<Vec<_>>();
    do_encode_bitmap_data(&bitmaps, num_values)
}

// Concatenate dictionary arrays.  This is a bit tricky because we might overflow the
// index type.  If we do, we need to upscale the indices to a larger type.
fn concat_dict_arrays(arrays: &[ArrayRef]) -> ArrayRef {
    let value_type = arrays[0].as_any_dictionary().values().data_type();
    let array_refs = arrays.iter().map(|arr| arr.as_ref()).collect::<Vec<_>>();
    match arrow_select::concat::concat(&array_refs) {
        Ok(array) => array,
        Err(arrow_schema::ArrowError::DictionaryKeyOverflowError) => {
            // Slow, but hopefully a corner case.  Optimize later
            let upscaled = array_refs
                .iter()
                .map(|arr| {
                    match arrow_cast::cast(
                        *arr,
                        &DataType::Dictionary(
                            Box::new(DataType::UInt32),
                            Box::new(value_type.clone()),
                        ),
                    ) {
                        Ok(arr) => arr,
                        Err(arrow_schema::ArrowError::DictionaryKeyOverflowError) => {
                            // Technically I think this means the input type was u64 already
                            unimplemented!("Dictionary arrays with more than 2^32 unique values")
                        }
                        err => err.unwrap(),
                    }
                })
                .collect::<Vec<_>>();
            let array_refs = upscaled.iter().map(|arr| arr.as_ref()).collect::<Vec<_>>();
            // Can still fail if concat pushes over u32 boundary
            match arrow_select::concat::concat(&array_refs) {
                Ok(array) => array,
                Err(arrow_schema::ArrowError::DictionaryKeyOverflowError) => {
                    unimplemented!("Dictionary arrays with more than 2^32 unique values")
                }
                err => err.unwrap(),
            }
        }
        // Shouldn't be any other possible errors in concat
        err => err.unwrap(),
    }
}

fn max_index_val(index_type: &DataType) -> u64 {
    match index_type {
        DataType::Int8 => i8::MAX as u64,
        DataType::Int16 => i16::MAX as u64,
        DataType::Int32 => i32::MAX as u64,
        DataType::Int64 => i64::MAX as u64,
        DataType::UInt8 => u8::MAX as u64,
        DataType::UInt16 => u16::MAX as u64,
        DataType::UInt32 => u32::MAX as u64,
        DataType::UInt64 => u64::MAX,
        _ => panic!("Invalid dictionary index type"),
    }
}

// If we get multiple dictionary arrays and they don't all have the same dictionary
// then we need to normalize the indices.  Otherwise we might have something like:
//
// First chunk ["hello", "foo"], [0, 0, 1, 1, 1]
// Second chunk ["bar", "world"], [0, 1, 0, 1, 1]
//
// If we simply encode as ["hello", "foo", "bar", "world"], [0, 0, 1, 1, 1, 0, 1, 0, 1, 1]
// then we will get the wrong answer because the dictionaries were not merged and the indices
// were not remapped.
//
// A simple way to do this today is to just concatenate all the arrays.  This is because
// arrow's dictionary concatenation function already has the logic to merge dictionaries.
//
// TODO: We could be more efficient here by checking if the dictionaries are the same
//       Also, if they aren't, we can possibly do something cheaper than concatenating
//
// In addition, we want to normalize the representation of nulls.  The cheapest thing to
// do (space-wise) is to put the nulls in the dictionary.
fn arrow_dictionary_to_data_block(arrays: &[ArrayRef], validity: Option<NullBuffer>) -> DataBlock {
    let array = concat_dict_arrays(arrays);
    let array_dict = array.as_any_dictionary();
    let mut indices = array_dict.keys();
    let num_values = indices.len() as u64;
    let mut values = array_dict.values().clone();
    // Placeholder, if we need to upcast, we will initialize this and set `indices` to refer to it
    let mut upcast = None;

    // TODO: Should we just always normalize indices to u32?  That would make logic simpler
    // and we're going to bitpack them soon anyways

    let indices_block = if let Some(validity) = validity {
        // If there is validity then we find the first invalid index in the dictionary values, inserting
        // a new value if we need to.  Then we change all indices to point to that value.  This way we
        // never need to store nullability of the indices.
        let mut first_invalid_index = None;
        if let Some(values_validity) = values.nulls() {
            first_invalid_index = (!values_validity.inner()).set_indices().next();
        }
        let first_invalid_index = first_invalid_index.unwrap_or_else(|| {
            let null_arr = new_null_array(values.data_type(), 1);
            values = arrow_select::concat::concat(&[values.as_ref(), null_arr.as_ref()]).unwrap();
            let null_index = values.len() - 1;
            let max_index_val = max_index_val(indices.data_type());
            if null_index as u64 > max_index_val {
                // Widen the index type
                if max_index_val >= u32::MAX as u64 {
                    unimplemented!("Dictionary arrays with 2^32 unique value (or more) and a null")
                }
                upcast = Some(arrow_cast::cast(indices, &DataType::UInt32).unwrap());
                indices = upcast.as_ref().unwrap();
            }
            null_index
        });
        // This can't fail since we already checked for fit
        let null_index_arr = arrow_cast::cast(
            &UInt64Array::from(vec![first_invalid_index as u64]),
            indices.data_type(),
        )
        .unwrap();

        let bytes_per_index = indices.data_type().byte_width();
        let bits_per_index = bytes_per_index as u64 * 8;

        let null_index_arr = null_index_arr.into_data();
        let null_index_bytes = &null_index_arr.buffers()[0];
        // Need to make a copy here since indices isn't mutable, could be avoided in theory
        let mut indices_bytes = indices.to_data().buffers()[0].to_vec();
        for invalid_idx in (!validity.inner()).set_indices() {
            indices_bytes[invalid_idx * bytes_per_index..(invalid_idx + 1) * bytes_per_index]
                .copy_from_slice(null_index_bytes.as_slice());
        }
        FixedWidthDataBlock {
            data: LanceBuffer::from(indices_bytes),
            bits_per_value: bits_per_index,
            num_values,
            block_info: BlockInfo::new(),
        }
    } else {
        FixedWidthDataBlock {
            data: LanceBuffer::from(indices.to_data().buffers()[0].clone()),
            bits_per_value: indices.data_type().byte_width() as u64 * 8,
            num_values,
            block_info: BlockInfo::new(),
        }
    };

    let items = DataBlock::from(values);
    DataBlock::Dictionary(DictionaryDataBlock {
        indices: indices_block,
        dictionary: Box::new(items),
    })
}

enum Nullability {
    None,
    All,
    Some(NullBuffer),
}

impl Nullability {
    fn to_option(&self) -> Option<NullBuffer> {
        match self {
            Self::Some(nulls) => Some(nulls.clone()),
            _ => None,
        }
    }
}

fn extract_nulls(arrays: &[ArrayRef], num_values: u64) -> Nullability {
    let mut has_nulls = false;
    let nulls_and_lens = arrays
        .iter()
        .map(|arr| {
            let nulls = arr.logical_nulls();
            has_nulls |= nulls.is_some();
            (nulls, arr.len())
        })
        .collect::<Vec<_>>();
    if !has_nulls {
        return Nullability::None;
    }
    let mut builder = BooleanBufferBuilder::new(num_values as usize);
    let mut num_nulls = 0;
    for (null, len) in nulls_and_lens {
        if let Some(null) = null {
            num_nulls += null.null_count();
            builder.append_buffer(&null.into_inner());
        } else {
            builder.append_n(len, true);
        }
    }
    if num_nulls == num_values as usize {
        Nullability::All
    } else {
        Nullability::Some(NullBuffer::new(builder.finish()))
    }
}

impl DataBlock {
    pub fn from_arrays(arrays: &[ArrayRef], num_values: u64) -> Self {
        if arrays.is_empty() || num_values == 0 {
            return Self::AllNull(AllNullDataBlock { num_values: 0 });
        }

        let data_type = arrays[0].data_type();
        let nulls = extract_nulls(arrays, num_values);

        if let Nullability::All = nulls {
            return Self::AllNull(AllNullDataBlock { num_values });
        }

        let mut encoded = match data_type {
            DataType::Binary | DataType::Utf8 => arrow_binary_to_data_block(arrays, num_values, 32),
            // View types have no Lance disk representation; cast to the classic offset layout.
            DataType::Utf8View => {
                let casted: Vec<ArrayRef> = arrays
                    .iter()
                    .map(|a| {
                        arrow_cast::cast(a.as_ref(), &DataType::Utf8)
                            .expect("Utf8View to Utf8 cast is always valid")
                    })
                    .collect();
                arrow_binary_to_data_block(&casted, num_values, 32)
            }
            DataType::BinaryView => {
                let casted: Vec<ArrayRef> = arrays
                    .iter()
                    .map(|a| {
                        arrow_cast::cast(a.as_ref(), &DataType::Binary)
                            .expect("BinaryView to Binary cast is always valid")
                    })
                    .collect();
                arrow_binary_to_data_block(&casted, num_values, 32)
            }
            DataType::LargeBinary | DataType::LargeUtf8 => {
                arrow_binary_to_data_block(arrays, num_values, 64)
            }
            DataType::Boolean => {
                let data = encode_bitmap_data(arrays, num_values);
                Self::FixedWidth(FixedWidthDataBlock {
                    data,
                    bits_per_value: 1,
                    num_values,
                    block_info: BlockInfo::new(),
                })
            }
            DataType::Date32
            | DataType::Date64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Duration(_)
            | DataType::FixedSizeBinary(_)
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Int8
            | DataType::Interval(_)
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::UInt8 => {
                let data = encode_flat_data(arrays, num_values);
                Self::FixedWidth(FixedWidthDataBlock {
                    data,
                    bits_per_value: data_type.byte_width() as u64 * 8,
                    num_values,
                    block_info: BlockInfo::new(),
                })
            }
            DataType::Null => Self::AllNull(AllNullDataBlock { num_values }),
            DataType::Dictionary(_, _) => arrow_dictionary_to_data_block(arrays, nulls.to_option()),
            DataType::Struct(fields) => {
                let structs = arrays.iter().map(|arr| arr.as_struct()).collect::<Vec<_>>();
                let mut children = Vec::with_capacity(fields.len());
                for child_idx in 0..fields.len() {
                    let child_vec = structs
                        .iter()
                        .map(|s| s.column(child_idx).clone())
                        .collect::<Vec<_>>();
                    children.push(Self::from_arrays(&child_vec, num_values));
                }

                // Extract validity for the struct array
                let validity = match &nulls {
                    Nullability::None => None,
                    Nullability::Some(null_buffer) => Some(null_buffer.clone()),
                    Nullability::All => unreachable!("Should have returned AllNull earlier"),
                };

                Self::Struct(StructDataBlock {
                    children,
                    block_info: BlockInfo::default(),
                    validity,
                })
            }
            DataType::FixedSizeList(_, dim) => {
                let children = arrays
                    .iter()
                    .map(|arr| arr.as_fixed_size_list().values().clone())
                    .collect::<Vec<_>>();
                let child_block = Self::from_arrays(&children, num_values * *dim as u64);
                Self::FixedSizeList(FixedSizeListBlock {
                    child: Box::new(child_block),
                    dimension: *dim as u64,
                })
            }
            DataType::LargeList(_)
            | DataType::List(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::Map(_, _)
            | DataType::RunEndEncoded(_, _)
            | DataType::Union(_, _) => {
                panic!(
                    "Field with data type {} cannot be converted to data block",
                    data_type
                )
            }
        };

        // compute statistics
        encoded.compute_stat();

        if !matches!(data_type, DataType::Dictionary(_, _)) {
            match nulls {
                Nullability::None => encoded,
                Nullability::Some(nulls) => Self::Nullable(NullableDataBlock {
                    data: Box::new(encoded),
                    nulls: LanceBuffer::from(nulls.into_inner().into_inner()),
                    block_info: BlockInfo::new(),
                }),
                _ => unreachable!(),
            }
        } else {
            // Dictionaries already insert the nulls into the dictionary items
            encoded
        }
    }

    pub fn from_array<T: Array + 'static>(array: T) -> Self {
        let num_values = array.len();
        Self::from_arrays(&[Arc::new(array)], num_values as u64)
    }
}

impl From<ArrayRef> for DataBlock {
    fn from(array: ArrayRef) -> Self {
        let num_values = array.len() as u64;
        Self::from_arrays(&[array], num_values)
    }
}

trait DataBlockBuilderImpl: std::fmt::Debug {
    fn validate_append(&self, data_block: &DataBlock, selection: &Range<u64>) -> Result<()>;

    fn append_validated(&mut self, data_block: &DataBlock, selection: Range<u64>) -> Result<()>;

    fn append(&mut self, data_block: &DataBlock, selection: Range<u64>) -> Result<()> {
        self.validate_append(data_block, &selection)?;
        self.append_validated(data_block, selection)
    }

    fn finish(self: Box<Self>) -> DataBlock;
}

#[derive(Debug)]
pub struct DataBlockBuilder {
    estimated_size_bytes: u64,
    builder: Option<Box<dyn DataBlockBuilderImpl>>,
}

impl DataBlockBuilder {
    pub fn with_capacity_estimate(estimated_size_bytes: u64) -> Self {
        Self {
            estimated_size_bytes,
            builder: None,
        }
    }

    fn get_builder(&mut self, block: &DataBlock) -> &mut dyn DataBlockBuilderImpl {
        if self.builder.is_none() {
            self.builder = Some(block.make_builder(self.estimated_size_bytes));
        }
        self.builder.as_mut().unwrap().as_mut()
    }

    pub fn append(&mut self, data_block: &DataBlock, selection: Range<u64>) -> Result<()> {
        self.get_builder(data_block).append(data_block, selection)
    }

    fn append_ranges(
        &mut self,
        data_block: &DataBlock,
        selections: impl IntoIterator<Item = Range<u64>>,
    ) -> Result<()> {
        let full_selection = 0..data_block.num_values();
        let builder = self.get_builder(data_block);
        builder.validate_append(data_block, &full_selection)?;
        for selection in selections {
            if selection.start > selection.end || selection.end > full_selection.end {
                return Err(Error::corrupt_file_named(
                    "data block",
                    format!(
                        "cannot append selection {}..{} from a block with {} values",
                        selection.start, selection.end, full_selection.end
                    ),
                ));
            }
            builder.append_validated(data_block, selection)?;
        }
        Ok(())
    }

    pub fn finish(self) -> DataBlock {
        let builder = self.builder.expect("DataBlockBuilder didn't see any data");
        builder.finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BinaryArray, BinaryViewArray, DictionaryArray, Int8Array, LargeBinaryArray,
        LargeStringArray, StringArray, StringViewArray, UInt8Array, UInt16Array, make_array,
        new_null_array,
        types::{Int8Type, Int32Type},
    };
    use arrow_buffer::{BooleanBuffer, NullBuffer};

    use arrow_schema::{DataType, Field, Fields};
    use lance_core::Error;
    use lance_datagen::{ArrayGeneratorExt, DEFAULT_SEED, RowCount, array};
    use rand::SeedableRng;
    use rstest::rstest;

    use crate::buffer::LanceBuffer;

    use super::{
        AllNullDataBlock, BlockInfo, DataBlock, DataBlockBuilder, DictionaryDataBlock,
        FixedWidthDataBlock, VariableWidthBlock,
    };

    use arrow_array::Array;

    #[test]
    fn test_sliced_to_data_block() {
        let ints = UInt16Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
        let ints = ints.slice(2, 4);
        let data = DataBlock::from_array(ints);

        let fixed_data = data.as_fixed_width().unwrap();
        assert_eq!(fixed_data.num_values, 4);
        assert_eq!(fixed_data.data.len(), 8);

        let nullable_ints =
            UInt16Array::from(vec![Some(0), None, Some(2), None, Some(4), None, Some(6)]);
        let nullable_ints = nullable_ints.slice(1, 3);
        let data = DataBlock::from_array(nullable_ints);

        let nullable = data.as_nullable().unwrap();
        assert_eq!(nullable.nulls, LanceBuffer::from(vec![0b00000010]));
    }

    #[test]
    fn test_string_to_data_block() {
        // Converting string arrays that contain nulls to DataBlock
        let strings1 = StringArray::from(vec![Some("hello"), None, Some("world")]);
        let strings2 = StringArray::from(vec![Some("a"), Some("b")]);
        let strings3 = StringArray::from(vec![Option::<&'static str>::None, None]);

        let arrays = &[strings1, strings2, strings3]
            .iter()
            .map(|arr| Arc::new(arr.clone()) as ArrayRef)
            .collect::<Vec<_>>();

        let block = DataBlock::from_arrays(arrays, 7);

        assert_eq!(block.num_values(), 7);
        let block = block.as_nullable().unwrap();

        assert_eq!(block.nulls, LanceBuffer::from(vec![0b00011101]));

        let data = block.data.as_variable_width().unwrap();
        assert_eq!(
            data.offsets,
            LanceBuffer::reinterpret_vec(vec![0, 5, 5, 10, 11, 12, 12, 12])
        );

        assert_eq!(data.data, LanceBuffer::copy_slice(b"helloworldab"));

        // Converting string arrays that do not contain nulls to DataBlock
        let strings1 = StringArray::from(vec![Some("a"), Some("bc")]);
        let strings2 = StringArray::from(vec![Some("def")]);

        let arrays = &[strings1, strings2]
            .iter()
            .map(|arr| Arc::new(arr.clone()) as ArrayRef)
            .collect::<Vec<_>>();

        let block = DataBlock::from_arrays(arrays, 3);

        assert_eq!(block.num_values(), 3);
        // Should be no nullable wrapper
        let data = block.as_variable_width().unwrap();
        assert_eq!(data.offsets, LanceBuffer::reinterpret_vec(vec![0, 1, 3, 6]));
        assert_eq!(data.data, LanceBuffer::copy_slice(b"abcdef"));
    }

    #[test]
    fn test_string_view_to_data_block() {
        let views1 = StringViewArray::from(vec![Some("hello"), None, Some("world")]);
        let views2 = StringViewArray::from(vec![Some("a"), Some("b")]);
        let views3 = StringViewArray::from(vec![Option::<&'static str>::None, None]);

        let arrays = &[views1, views2, views3]
            .iter()
            .map(|arr| Arc::new(arr.clone()) as ArrayRef)
            .collect::<Vec<_>>();

        let block = DataBlock::from_arrays(arrays, 7);

        assert_eq!(block.num_values(), 7);
        let block = block.as_nullable().unwrap();
        assert_eq!(block.nulls, LanceBuffer::from(vec![0b00011101]));
        let data = block.data.as_variable_width().unwrap();
        assert_eq!(
            data.offsets,
            LanceBuffer::reinterpret_vec(vec![0, 5, 5, 10, 11, 12, 12, 12])
        );
        assert_eq!(data.data, LanceBuffer::copy_slice(b"helloworldab"));

        let views1 = StringViewArray::from(vec![Some("a"), Some("bc")]);
        let views2 = StringViewArray::from(vec![Some("def")]);

        let arrays = &[views1, views2]
            .iter()
            .map(|arr| Arc::new(arr.clone()) as ArrayRef)
            .collect::<Vec<_>>();

        let block = DataBlock::from_arrays(arrays, 3);

        assert_eq!(block.num_values(), 3);
        let data = block.as_variable_width().unwrap();
        assert_eq!(data.offsets, LanceBuffer::reinterpret_vec(vec![0, 1, 3, 6]));
        assert_eq!(data.data, LanceBuffer::copy_slice(b"abcdef"));
    }

    #[test]
    fn test_binary_view_to_data_block() {
        let arr: ArrayRef = Arc::new(BinaryViewArray::from(vec![
            Some(b"foo".as_slice()),
            None,
            Some(b"bar".as_slice()),
        ]));
        let block = DataBlock::from_arrays(&[arr], 3);
        let block = block.as_nullable().unwrap();
        let data = block.data.as_variable_width().unwrap();
        assert_eq!(data.data, LanceBuffer::copy_slice(b"foobar"));
    }

    #[test]
    fn test_string_sliced() {
        let check = |arr: Vec<StringArray>, expected_off: Vec<i32>, expected_data: &[u8]| {
            let arrs = arr
                .into_iter()
                .map(|a| Arc::new(a) as ArrayRef)
                .collect::<Vec<_>>();
            let num_rows = arrs.iter().map(|a| a.len()).sum::<usize>() as u64;
            let data = DataBlock::from_arrays(&arrs, num_rows);

            assert_eq!(data.num_values(), num_rows);

            let data = data.as_variable_width().unwrap();
            assert_eq!(data.offsets, LanceBuffer::reinterpret_vec(expected_off));
            assert_eq!(data.data, LanceBuffer::copy_slice(expected_data));
        };

        let string = StringArray::from(vec![Some("hello"), Some("world")]);
        check(vec![string.slice(1, 1)], vec![0, 5], b"world");
        check(vec![string.slice(0, 1)], vec![0, 5], b"hello");
        check(
            vec![string.slice(0, 1), string.slice(1, 1)],
            vec![0, 5, 10],
            b"helloworld",
        );

        let string2 = StringArray::from(vec![Some("foo"), Some("bar")]);
        check(
            vec![string.slice(0, 1), string2.slice(0, 1)],
            vec![0, 5, 8],
            b"hellofoo",
        );
    }

    #[test]
    fn test_large() {
        let arr = LargeBinaryArray::from_vec(vec![b"hello", b"world"]);
        let data = DataBlock::from_array(arr);

        assert_eq!(data.num_values(), 2);
        let data = data.as_variable_width().unwrap();
        assert_eq!(data.bits_per_offset, 64);
        assert_eq!(data.num_values, 2);
        assert_eq!(data.data, LanceBuffer::copy_slice(b"helloworld"));
        assert_eq!(
            data.offsets,
            LanceBuffer::reinterpret_vec(vec![0_u64, 5, 10])
        );
    }

    #[test]
    fn test_dictionary_indices_normalized() {
        let arr1 = DictionaryArray::<Int8Type>::from_iter([Some("a"), Some("a"), Some("b")]);
        let arr2 = DictionaryArray::<Int8Type>::from_iter([Some("b"), Some("c")]);

        let data = DataBlock::from_arrays(&[Arc::new(arr1), Arc::new(arr2)], 5);

        assert_eq!(data.num_values(), 5);
        let data = data.as_dictionary().unwrap();
        let indices = data.indices;
        assert_eq!(indices.bits_per_value, 8);
        assert_eq!(indices.num_values, 5);
        assert_eq!(
            indices.data,
            // You might expect 0, 0, 1, 1, 2 but it seems that arrow's dictionary concat does
            // not actually collapse dictionaries.  This is an arrow problem however, and we don't
            // need to fix it here.
            LanceBuffer::reinterpret_vec::<i8>(vec![0, 0, 1, 2, 3])
        );

        let items = data.dictionary.as_variable_width().unwrap();
        assert_eq!(items.bits_per_offset, 32);
        assert_eq!(items.num_values, 4);
        assert_eq!(items.data, LanceBuffer::copy_slice(b"abbc"));
        assert_eq!(
            items.offsets,
            LanceBuffer::reinterpret_vec(vec![0, 1, 2, 3, 4],)
        );
    }

    #[test]
    fn test_dictionary_nulls() {
        // Test both ways of encoding nulls

        // By default, nulls get encoded into the indices
        let arr1 = DictionaryArray::<Int8Type>::from_iter([None, Some("a"), Some("b")]);
        let arr2 = DictionaryArray::<Int8Type>::from_iter([Some("c"), None]);

        let data = DataBlock::from_arrays(&[Arc::new(arr1), Arc::new(arr2)], 5);

        let check_common = |data: DataBlock| {
            assert_eq!(data.num_values(), 5);
            let dict = data.as_dictionary().unwrap();

            let nullable_items = dict.dictionary.as_nullable().unwrap();
            assert_eq!(nullable_items.nulls, LanceBuffer::from(vec![0b00000111]));
            assert_eq!(nullable_items.data.num_values(), 4);

            let items = nullable_items.data.as_variable_width().unwrap();
            assert_eq!(items.bits_per_offset, 32);
            assert_eq!(items.num_values, 4);
            assert_eq!(items.data, LanceBuffer::copy_slice(b"abc"));
            assert_eq!(
                items.offsets,
                LanceBuffer::reinterpret_vec(vec![0, 1, 2, 3, 3],)
            );

            let indices = dict.indices;
            assert_eq!(indices.bits_per_value, 8);
            assert_eq!(indices.num_values, 5);
            assert_eq!(
                indices.data,
                LanceBuffer::reinterpret_vec::<i8>(vec![3, 0, 1, 2, 3])
            );
        };
        check_common(data);

        // However, we can manually create a dictionary where nulls are in the dictionary
        let items = StringArray::from(vec![Some("a"), Some("b"), Some("c"), None]);
        let indices = Int8Array::from(vec![Some(3), Some(0), Some(1), Some(2), Some(3)]);
        let dict = DictionaryArray::new(indices, Arc::new(items));

        let data = DataBlock::from_array(dict);

        check_common(data);
    }

    #[test]
    fn test_dictionary_cannot_add_null() {
        // 256 unique strings
        let items = StringArray::from(
            (0..256)
                .map(|i| Some(String::from_utf8(vec![0; i]).unwrap()))
                .collect::<Vec<_>>(),
        );
        // 257 indices, covering the whole range, plus one null
        let indices = UInt8Array::from(
            (0..=256)
                .map(|i| if i == 256 { None } else { Some(i as u8) })
                .collect::<Vec<_>>(),
        );
        // We want to normalize this by pushing nulls into the dictionary, but we cannot because
        // the dictionary is too large for the index type
        let dict = DictionaryArray::new(indices, Arc::new(items));
        let data = DataBlock::from_array(dict);

        assert_eq!(data.num_values(), 257);

        let dict = data.as_dictionary().unwrap();

        assert_eq!(dict.indices.bits_per_value, 32);
        assert_eq!(
            dict.indices.data,
            LanceBuffer::reinterpret_vec((0_u32..257).collect::<Vec<_>>())
        );

        let nullable_items = dict.dictionary.as_nullable().unwrap();
        let null_buffer = NullBuffer::new(BooleanBuffer::new(
            nullable_items.nulls.into_buffer(),
            0,
            257,
        ));
        for i in 0..256 {
            assert!(!null_buffer.is_null(i));
        }
        assert!(null_buffer.is_null(256));

        assert_eq!(
            nullable_items.data.as_variable_width().unwrap().data.len(),
            32640
        );
    }

    #[test]
    fn test_all_null() {
        for data_type in [
            DataType::UInt32,
            DataType::FixedSizeBinary(2),
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::UInt32, true)])),
        ] {
            let block = DataBlock::AllNull(AllNullDataBlock { num_values: 10 });
            let arr = block.into_arrow(data_type.clone(), true).unwrap();
            let arr = make_array(arr);
            let expected = new_null_array(&data_type, 10);
            assert_eq!(&arr, &expected);
        }
    }

    #[test]
    fn test_dictionary_cannot_concatenate() {
        // 256 unique strings
        let items = StringArray::from(
            (0..256)
                .map(|i| Some(String::from_utf8(vec![0; i]).unwrap()))
                .collect::<Vec<_>>(),
        );
        // 256 different unique strings
        let other_items = StringArray::from(
            (0..256)
                .map(|i| Some(String::from_utf8(vec![1; i + 1]).unwrap()))
                .collect::<Vec<_>>(),
        );
        let indices = UInt8Array::from_iter_values(0..=255);
        let dict1 = DictionaryArray::new(indices.clone(), Arc::new(items));
        let dict2 = DictionaryArray::new(indices, Arc::new(other_items));
        let data = DataBlock::from_arrays(&[Arc::new(dict1), Arc::new(dict2)], 512);
        assert_eq!(data.num_values(), 512);

        let dict = data.as_dictionary().unwrap();

        assert_eq!(dict.indices.bits_per_value, 32);
        assert_eq!(
            dict.indices.data,
            LanceBuffer::reinterpret_vec::<u32>((0..512).collect::<Vec<_>>())
        );
        // What fun: 0 + 1 + .. + 255 + 1 + 2 + .. + 256 = 2^16
        assert_eq!(
            dict.dictionary.as_variable_width().unwrap().data.len(),
            65536
        );
    }

    #[test]
    fn test_data_size() {
        let mut rng = rand_xoshiro::Xoshiro256PlusPlus::seed_from_u64(DEFAULT_SEED.0);
        // test data_size() when input has no nulls
        let mut genn = array::rand::<Int32Type>().with_nulls(&[false, false, false]);

        let arr = genn.generate(RowCount::from(3), &mut rng).unwrap();
        let block = DataBlock::from_array(arr.clone());
        assert!(block.data_size() == arr.get_buffer_memory_size() as u64);

        let arr = genn.generate(RowCount::from(400), &mut rng).unwrap();
        let block = DataBlock::from_array(arr.clone());
        assert!(block.data_size() == arr.get_buffer_memory_size() as u64);

        // test data_size() when input has nulls
        let mut genn = array::rand::<Int32Type>().with_nulls(&[false, true, false]);
        let arr = genn.generate(RowCount::from(3), &mut rng).unwrap();
        let block = DataBlock::from_array(arr.clone());

        let array_data = arr.to_data();
        let total_buffer_size: usize = array_data.buffers().iter().map(|buffer| buffer.len()).sum();
        // the NullBuffer.len() returns the length in bits so we divide_round_up by 8
        let array_nulls_size_in_bytes = arr.nulls().unwrap().len().div_ceil(8);
        assert!(block.data_size() == (total_buffer_size + array_nulls_size_in_bytes) as u64);

        let arr = genn.generate(RowCount::from(400), &mut rng).unwrap();
        let block = DataBlock::from_array(arr.clone());

        let array_data = arr.to_data();
        let total_buffer_size: usize = array_data.buffers().iter().map(|buffer| buffer.len()).sum();
        let array_nulls_size_in_bytes = arr.nulls().unwrap().len().div_ceil(8);
        assert!(block.data_size() == (total_buffer_size + array_nulls_size_in_bytes) as u64);

        let mut genn = array::rand::<Int32Type>().with_nulls(&[true, true, false]);
        let arr = genn.generate(RowCount::from(3), &mut rng).unwrap();
        let block = DataBlock::from_array(arr.clone());

        let array_data = arr.to_data();
        let total_buffer_size: usize = array_data.buffers().iter().map(|buffer| buffer.len()).sum();
        let array_nulls_size_in_bytes = arr.nulls().unwrap().len().div_ceil(8);
        assert!(block.data_size() == (total_buffer_size + array_nulls_size_in_bytes) as u64);

        let arr = genn.generate(RowCount::from(400), &mut rng).unwrap();
        let block = DataBlock::from_array(arr.clone());

        let array_data = arr.to_data();
        let total_buffer_size: usize = array_data.buffers().iter().map(|buffer| buffer.len()).sum();
        let array_nulls_size_in_bytes = arr.nulls().unwrap().len().div_ceil(8);
        assert!(block.data_size() == (total_buffer_size + array_nulls_size_in_bytes) as u64);

        let mut genn = array::rand::<Int32Type>().with_nulls(&[false, true, false]);
        let arr1 = genn.generate(RowCount::from(3), &mut rng).unwrap();
        let arr2 = genn.generate(RowCount::from(3), &mut rng).unwrap();
        let arr3 = genn.generate(RowCount::from(3), &mut rng).unwrap();
        let block = DataBlock::from_arrays(&[arr1.clone(), arr2.clone(), arr3.clone()], 9);

        let concatenated_array = arrow_select::concat::concat(&[
            &*Arc::new(arr1.clone()) as &dyn Array,
            &*Arc::new(arr2.clone()) as &dyn Array,
            &*Arc::new(arr3.clone()) as &dyn Array,
        ])
        .unwrap();
        let total_buffer_size: usize = concatenated_array
            .to_data()
            .buffers()
            .iter()
            .map(|buffer| buffer.len())
            .sum();

        let total_nulls_size_in_bytes = concatenated_array.nulls().unwrap().len().div_ceil(8);
        assert!(block.data_size() == (total_buffer_size + total_nulls_size_in_bytes) as u64);
    }

    #[test]
    fn variable_width_rejects_out_of_bounds_offsets_without_optional_validation() {
        let block = VariableWidthBlock {
            data: LanceBuffer::copy_slice(b"alphabetagamma"),
            offsets: LanceBuffer::reinterpret_vec(vec![0_i32, 5, 9, 100_000]),
            bits_per_offset: 32,
            num_values: 3,
            block_info: BlockInfo::new(),
        };

        let error = block
            .into_arrow(DataType::Binary, false)
            .expect_err("out-of-bounds offsets must be rejected");
        assert!(
            matches!(error, Error::CorruptFile { .. }),
            "expected CorruptFile, got: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("100000") && message.contains("data buffer size: 14 bytes"),
            "error must report the offending offset and the data buffer size: {message}"
        );
    }

    #[rstest]
    #[case::i32_decreasing(
        LanceBuffer::reinterpret_vec(vec![0_i32, 5, 2]),
        32,
        2,
        "decreases"
    )]
    #[case::i64_decreasing(
        LanceBuffer::reinterpret_vec(vec![0_i64, 5, 2]),
        64,
        2,
        "decreases"
    )]
    #[case::i32_out_of_bounds(
        LanceBuffer::reinterpret_vec(vec![0_i32, 6]),
        32,
        1,
        "out of bounds"
    )]
    fn variable_width_builder_rejects_malformed_offsets(
        #[case] offsets: LanceBuffer,
        #[case] bits_per_offset: u8,
        #[case] num_values: u64,
        #[case] expected_message: &str,
    ) {
        let block = DataBlock::VariableWidth(VariableWidthBlock {
            data: LanceBuffer::copy_slice(b"abcde"),
            offsets,
            bits_per_offset,
            num_values,
            block_info: BlockInfo::new(),
        });
        let mut builder = DataBlockBuilder::with_capacity_estimate(5);

        let error = builder
            .append(&block, 0..num_values)
            .expect_err("malformed offsets must fail concatenation");
        assert!(
            matches!(error, Error::CorruptFile { .. }),
            "expected CorruptFile, got: {error}"
        );
        assert!(
            error.to_string().contains(expected_message),
            "unexpected message: {error}"
        );
    }

    #[rstest]
    #[case::binary_i32_tail_out_of_bounds(
        DataType::Binary,
        LanceBuffer::reinterpret_vec(vec![0_i32, 5, 9, 100_000]),
        32,
        3,
        b"alphabetagamma".as_slice()
    )]
    #[case::utf8_i32_tail_out_of_bounds(
        DataType::Utf8,
        LanceBuffer::reinterpret_vec(vec![0_i32, 5, 9, 100_000]),
        32,
        3,
        b"alphabetagamma".as_slice()
    )]
    #[case::large_binary_i64_tail_out_of_bounds(
        DataType::LargeBinary,
        LanceBuffer::reinterpret_vec(vec![0_i64, 5, 9, 100_000]),
        64,
        3,
        b"alphabetagamma".as_slice()
    )]
    #[case::large_utf8_i64_tail_out_of_bounds(
        DataType::LargeUtf8,
        LanceBuffer::reinterpret_vec(vec![0_i64, 5, 9, 100_000]),
        64,
        3,
        b"alphabetagamma".as_slice()
    )]
    #[case::binary_negative_offset(
        DataType::Binary,
        LanceBuffer::reinterpret_vec(vec![0_i32, -1, 9, 14]),
        32,
        3,
        b"alphabetagamma".as_slice()
    )]
    #[case::binary_non_monotonic_offsets(
        DataType::Binary,
        LanceBuffer::reinterpret_vec(vec![0_i32, 9, 5, 14]),
        32,
        3,
        b"alphabetagamma".as_slice()
    )]
    #[case::binary_interior_offset_out_of_bounds(
        DataType::Binary,
        LanceBuffer::reinterpret_vec(vec![0_i32, 100_000, 100_000, 14]),
        32,
        3,
        b"alphabetagamma".as_slice()
    )]
    #[case::binary_offsets_buffer_too_short(
        DataType::Binary,
        LanceBuffer::reinterpret_vec(vec![0_i32, 5, 9]),
        32,
        3,
        b"alphabetagamma".as_slice()
    )]
    #[case::utf8_invalid_byte_sequence(
        DataType::Utf8,
        LanceBuffer::reinterpret_vec(vec![0_i32, 1, 2, 3]),
        32,
        3,
        &[b'a', 0xFF, b'b']
    )]
    #[case::utf8_offset_splits_multibyte_char(
        DataType::Utf8,
        LanceBuffer::reinterpret_vec(vec![0_i32, 1, 2]),
        32,
        2,
        "é".as_bytes()
    )]
    #[case::large_utf8_invalid_byte_sequence(
        DataType::LargeUtf8,
        LanceBuffer::reinterpret_vec(vec![0_i64, 1, 2, 3]),
        64,
        3,
        &[b'a', 0xFF, b'b']
    )]
    fn variable_width_rejects_malformed_layout(
        #[case] data_type: DataType,
        #[case] offsets: LanceBuffer,
        #[case] bits_per_offset: u8,
        #[case] num_values: u64,
        #[case] data: &[u8],
    ) {
        let block = VariableWidthBlock {
            data: LanceBuffer::copy_slice(data),
            offsets,
            bits_per_offset,
            num_values,
            block_info: BlockInfo::new(),
        };

        // The malformed layout must be rejected regardless of the optional
        // `validate` flag: the flag selects extra validation, not the memory
        // safety proof required to construct an Arrow array.
        for validate in [false, true] {
            let error = DataBlock::VariableWidth(block.clone())
                .into_arrow(data_type.clone(), validate)
                .expect_err("malformed variable-width layout must be rejected");
            assert!(
                matches!(error, Error::CorruptFile { .. }),
                "expected CorruptFile with validate={validate}, got: {error}"
            );
        }
    }

    #[test]
    fn dictionary_rejects_malformed_variable_width_values_without_optional_validation() {
        let values = VariableWidthBlock {
            data: LanceBuffer::copy_slice(b"alphabetagamma"),
            offsets: LanceBuffer::reinterpret_vec(vec![0_i32, 5, 9, 100_000]),
            bits_per_offset: 32,
            num_values: 3,
            block_info: BlockInfo::new(),
        };
        let dictionary = DataBlock::Dictionary(DictionaryDataBlock {
            indices: FixedWidthDataBlock {
                data: LanceBuffer::reinterpret_vec(vec![0_i32, 1, 2]),
                bits_per_value: 32,
                num_values: 3,
                block_info: BlockInfo::new(),
            },
            dictionary: Box::new(DataBlock::VariableWidth(values)),
        });

        let data_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Binary));
        let error = dictionary
            .into_arrow(data_type, false)
            .expect_err("dictionary with out-of-bounds value offsets must be rejected");
        assert!(
            matches!(error, Error::CorruptFile { .. }),
            "expected CorruptFile, got: {error}"
        );
    }

    #[rstest]
    #[case::binary(Arc::new(BinaryArray::from_vec(vec![b"alpha", b"", b"gamma"])) as ArrayRef)]
    #[case::large_binary(
        Arc::new(LargeBinaryArray::from_vec(vec![b"alpha", b"", b"gamma"])) as ArrayRef
    )]
    #[case::utf8(Arc::new(StringArray::from(vec!["héllo", "", "world"])) as ArrayRef)]
    #[case::large_utf8(Arc::new(LargeStringArray::from(vec!["héllo", "", "world"])) as ArrayRef)]
    fn variable_width_valid_data_survives_mandatory_validation(#[case] array: ArrayRef) {
        let block = DataBlock::from_array(array.clone());
        for validate in [false, true] {
            let round_tripped = make_array(
                block
                    .clone()
                    .into_arrow(array.data_type().clone(), validate)
                    .unwrap(),
            );
            assert_eq!(&round_tripped, &array);
        }
    }
}
