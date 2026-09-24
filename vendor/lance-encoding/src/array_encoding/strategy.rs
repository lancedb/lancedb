// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{collections::HashMap, env, hash::RandomState, sync::Arc};

use arrow_array::{
    Array, ArrayRef, GenericListArray, OffsetSizeTrait, UInt8Array, cast::AsArray, make_array,
};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_data::ArrayData;
use arrow_schema::DataType;
use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};
use lance_arrow::BLOB_META_KEY;

use crate::{
    array_encoding::{
        logical::{
            blob::BlobFieldEncoder, list::ListFieldEncoder, primitive::PrimitiveFieldEncoder,
        },
        physical::{
            basic::BasicEncoder,
            binary::BinaryEncoder,
            dictionary::{AlreadyDictionaryEncoder, DictionaryEncoder},
            fixed_size_list::FslEncoder,
            fsst::FsstArrayEncoder,
            packed_struct::PackedStructEncoder,
        },
    },
    constants::{
        COMPRESSION_LEVEL_META_KEY, COMPRESSION_META_KEY, PACKED_STRUCT_LEGACY_META_KEY,
        PACKED_STRUCT_META_KEY,
    },
    encoder::{
        ArrayEncoder, ArrayEncodingStrategy, ColumnIndexSequence, EncodeTask, EncodedColumn,
        FieldEncoder, FieldEncodingContext, FieldEncodingStrategy, OutOfLineBuffers,
    },
    encodings::{
        logical::r#struct::StructFieldEncoder,
        physical::{
            block::{CompressionConfig, CompressionScheme},
            value::ValueEncoder,
        },
    },
};

use lance_core::datatypes::{BLOB_DESC_FIELD, Field};
use lance_core::utils::parse::str_is_truthy;
use lance_core::{Error, Result};

/// Field-to-column composition for the `pb::ArrayEncoding` grammar.
#[derive(Debug)]
pub struct ArrayFieldEncodingStrategy {
    array_encoding_strategy: Arc<dyn ArrayEncodingStrategy>,
}

struct ValidatingFieldEncoder {
    inner: Box<dyn FieldEncoder>,
    field: Field,
    prepared_array: Option<ArrayRef>,
}

impl ValidatingFieldEncoder {
    fn new(inner: Box<dyn FieldEncoder>, field: Field) -> Self {
        Self {
            inner,
            field,
            prepared_array: None,
        }
    }
}

impl FieldEncoder for ValidatingFieldEncoder {
    fn prepare_array(&mut self, array: ArrayRef) -> Result<ArrayRef> {
        ArrayFieldEncodingStrategy::validate_v2_0_array(array.as_ref(), &self.field, true)?;
        let array = ArrayFieldEncodingStrategy::clear_unreachable_v2_0_fsl_struct_validity(array)?;
        self.prepared_array = Some(array.clone());
        Ok(array)
    }

    fn maybe_encode(
        &mut self,
        array: ArrayRef,
        external_buffers: &mut OutOfLineBuffers,
        repdef: crate::repdef::RepDefBuilder,
        row_number: u64,
        num_rows: u64,
    ) -> Result<Vec<EncodeTask>> {
        let array = match self.prepared_array.take() {
            Some(prepared) if Arc::ptr_eq(&prepared, &array) => prepared,
            _ => {
                // Direct field encoders have historically accepted arrays whose top-level
                // nullability differs from the declared field. Keep that API behavior while
                // still making the v2.0 struct-validity invariant unavoidable.
                ArrayFieldEncodingStrategy::validate_v2_0_array(
                    array.as_ref(),
                    &self.field,
                    false,
                )?;
                ArrayFieldEncodingStrategy::clear_unreachable_v2_0_fsl_struct_validity(array)?
            }
        };
        self.inner
            .maybe_encode(array, external_buffers, repdef, row_number, num_rows)
    }

    fn flush(&mut self, external_buffers: &mut OutOfLineBuffers) -> Result<Vec<EncodeTask>> {
        self.inner.flush(external_buffers)
    }

    fn finish(
        &mut self,
        external_buffers: &mut OutOfLineBuffers,
    ) -> futures::future::BoxFuture<'_, Result<Vec<EncodedColumn>>> {
        self.inner.finish(external_buffers)
    }

    fn num_columns(&self) -> u32 {
        self.inner.num_columns()
    }
}

impl ArrayFieldEncodingStrategy {
    /// Create the field strategy for the `pb::ArrayEncoding` grammar.
    ///
    /// ```
    /// use lance_encoding::encoder::ArrayFieldEncodingStrategy;
    ///
    /// let strategy = ArrayFieldEncodingStrategy::new();
    /// ```
    pub fn new() -> Self {
        Self {
            array_encoding_strategy: Arc::new(ArrayStrategy),
        }
    }

    fn is_primitive_type(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Boolean
                | DataType::Date32
                | DataType::Date64
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Duration(_)
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Int8
                | DataType::Interval(_)
                | DataType::Null
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::UInt8
                | DataType::FixedSizeBinary(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::Utf8
                | DataType::LargeUtf8,
        )
    }

    fn validate_v2_0_array(
        array: &dyn Array,
        field: &Field,
        enforce_field_nullability: bool,
    ) -> Result<()> {
        Self::validate_v2_0_array_reachability(array, field, enforce_field_nullability, None)
    }

    fn field_requires_v2_0_validation(field: &Field, enforce_field_nullability: bool) -> bool {
        field.logical_type.is_struct()
            || (enforce_field_nullability && !field.nullable)
            || field
                .children
                .iter()
                .any(|child| Self::field_requires_v2_0_validation(child, enforce_field_nullability))
    }

    fn is_reachable(reachable: Option<&BooleanBuffer>, index: usize) -> bool {
        reachable.map(|mask| mask.value(index)).unwrap_or(true)
    }

    fn positional_child_reachability(
        array: &dyn Array,
        reachable: Option<&BooleanBuffer>,
    ) -> Option<BooleanBuffer> {
        if reachable.is_none() && array.null_count() == 0 {
            return None;
        }
        Some(BooleanBuffer::from_iter((0..array.len()).map(|index| {
            Self::is_reachable(reachable, index) && array.is_valid(index)
        })))
    }

    fn list_child_reachability<O: OffsetSizeTrait>(
        array: &GenericListArray<O>,
        reachable: Option<&BooleanBuffer>,
    ) -> Option<BooleanBuffer> {
        let values_len = array.values().len();
        let offsets = array.offsets();
        if reachable.is_none()
            && array.null_count() == 0
            && offsets.first().map(|offset| offset.as_usize()) == Some(0)
            && offsets.last().map(|offset| offset.as_usize()) == Some(values_len)
        {
            return None;
        }

        let mut child_reachable = vec![false; values_len];
        for index in 0..array.len() {
            if Self::is_reachable(reachable, index) && array.is_valid(index) {
                let start = offsets[index].as_usize();
                let end = offsets[index + 1].as_usize();
                for is_reachable in child_reachable.iter_mut().take(end).skip(start) {
                    *is_reachable = true;
                }
            }
        }
        Some(BooleanBuffer::from_iter(child_reachable))
    }

    fn map_child_reachability(
        array: &arrow_array::MapArray,
        reachable: Option<&BooleanBuffer>,
    ) -> Option<BooleanBuffer> {
        let values_len = array.entries().len();
        let offsets = array.offsets();
        if reachable.is_none()
            && array.null_count() == 0
            && offsets.first().copied() == Some(0)
            && offsets.last().copied() == Some(values_len as i32)
        {
            return None;
        }

        let mut child_reachable = vec![false; values_len];
        for index in 0..array.len() {
            if Self::is_reachable(reachable, index) && array.is_valid(index) {
                let start = offsets[index] as usize;
                let end = offsets[index + 1] as usize;
                for is_reachable in child_reachable.iter_mut().take(end).skip(start) {
                    *is_reachable = true;
                }
            }
        }
        Some(BooleanBuffer::from_iter(child_reachable))
    }

    fn fixed_size_list_child_reachability(
        array: &arrow_array::FixedSizeListArray,
        reachable: Option<&BooleanBuffer>,
    ) -> Option<BooleanBuffer> {
        let values_len = array.values().len();
        let dimension = array.value_length() as usize;
        let (first_value, final_value) = if array.is_empty() {
            (0, 0)
        } else {
            (
                array.value_offset(0) as usize,
                array.value_offset(array.len() - 1) as usize + dimension,
            )
        };
        if reachable.is_none()
            && array.null_count() == 0
            && first_value == 0
            && final_value == values_len
        {
            return None;
        }

        let mut child_reachable = vec![false; values_len];
        for index in 0..array.len() {
            if Self::is_reachable(reachable, index) && array.is_valid(index) {
                let start = array.value_offset(index) as usize;
                for is_reachable in child_reachable.iter_mut().skip(start).take(dimension) {
                    *is_reachable = true;
                }
            }
        }
        Some(BooleanBuffer::from_iter(child_reachable))
    }

    fn validate_v2_0_array_reachability(
        array: &dyn Array,
        field: &Field,
        enforce_field_nullability: bool,
        reachable: Option<&BooleanBuffer>,
    ) -> Result<()> {
        if !Self::field_requires_v2_0_validation(field, enforce_field_nullability) {
            return Ok(());
        }

        let reachable_null_count = if array.null_count() == 0 {
            0
        } else if let Some(reachable) = reachable {
            (0..array.len())
                .filter(|index| reachable.value(*index) && array.is_null(*index))
                .count()
        } else {
            array.null_count()
        };

        if enforce_field_nullability && !field.nullable && reachable_null_count > 0 {
            return Err(Error::invalid_input(format!(
                "The field `{}` contained null values even though the field is marked non-null in the schema",
                field.name
            )));
        }
        if field.logical_type.is_struct() && reachable_null_count > 0 {
            return Err(Error::invalid_input(format!(
                "The struct field `{}` contains {} null value(s), but Lance file version 2.0 does not encode struct validity; use file version 2.1 or later",
                field.name, reachable_null_count
            )));
        }

        match array.data_type() {
            DataType::Struct(_) => {
                let child_reachable = Self::positional_child_reachability(array, reachable);
                for (child_field, child_array) in
                    field.children.iter().zip(array.as_struct().columns())
                {
                    Self::validate_v2_0_array_reachability(
                        child_array.as_ref(),
                        child_field,
                        enforce_field_nullability,
                        child_reachable.as_ref(),
                    )?;
                }
            }
            DataType::List(_) => {
                let list_array = array.as_list::<i32>();
                if let Some(child_field) = field.children.first() {
                    let child_reachable = Self::list_child_reachability(list_array, reachable);
                    Self::validate_v2_0_array_reachability(
                        list_array.values().as_ref(),
                        child_field,
                        enforce_field_nullability,
                        child_reachable.as_ref(),
                    )?;
                }
            }
            DataType::LargeList(_) => {
                let list_array = array.as_list::<i64>();
                if let Some(child_field) = field.children.first() {
                    let child_reachable = Self::list_child_reachability(list_array, reachable);
                    Self::validate_v2_0_array_reachability(
                        list_array.values().as_ref(),
                        child_field,
                        enforce_field_nullability,
                        child_reachable.as_ref(),
                    )?;
                }
            }
            DataType::Map(_, _) => {
                let map_array = array.as_map();
                if let Some(child_field) = field.children.first() {
                    let child_reachable = Self::map_child_reachability(map_array, reachable);
                    Self::validate_v2_0_array_reachability(
                        map_array.entries(),
                        child_field,
                        enforce_field_nullability,
                        child_reachable.as_ref(),
                    )?;
                }
            }
            DataType::FixedSizeList(_, _) => {
                let list_array = array.as_fixed_size_list();
                if let Some(child_field) = field.children.first() {
                    let child_reachable =
                        Self::fixed_size_list_child_reachability(list_array, reachable);
                    Self::validate_v2_0_array_reachability(
                        list_array.values().as_ref(),
                        child_field,
                        enforce_field_nullability,
                        child_reachable.as_ref(),
                    )?;
                }
            }
            _ => {
                let array_data = array.to_data();
                for (child_field, child_data) in field.children.iter().zip(array_data.child_data())
                {
                    let child_array = make_array(child_data.clone());
                    let child_reachable = (child_array.len() == array.len())
                        .then(|| Self::positional_child_reachability(array, reachable))
                        .flatten();
                    Self::validate_v2_0_array_reachability(
                        child_array.as_ref(),
                        child_field,
                        enforce_field_nullability,
                        child_reachable.as_ref(),
                    )?;
                }
            }
        }
        Ok(())
    }

    fn clear_unreachable_v2_0_fsl_struct_validity(array: ArrayRef) -> Result<ArrayRef> {
        fn force_valid_under_null_struct(
            data: ArrayData,
            struct_nulls: &NullBuffer,
        ) -> Result<ArrayData> {
            let Some(child_nulls) = data.nulls() else {
                return Ok(data);
            };
            if !(0..data.len())
                .any(|index| struct_nulls.is_null(index) && child_nulls.is_null(index))
            {
                return Ok(data);
            }

            let nulls =
                NullBuffer::new(BooleanBuffer::from_iter((0..data.len()).map(|index| {
                    struct_nulls.is_null(index) || child_nulls.is_valid(index)
                })));
            let nulls = (nulls.null_count() > 0).then_some(nulls);
            Ok(data.into_builder().nulls(nulls).build()?)
        }

        fn strip_struct_validity(data: ArrayData) -> Result<(ArrayData, bool)> {
            let struct_fields = match data.data_type() {
                DataType::Struct(fields) => Some(fields.clone()),
                _ => None,
            };
            let struct_nulls = struct_fields
                .as_ref()
                .and_then(|_| data.nulls())
                .filter(|nulls| nulls.null_count() > 0)
                .cloned();
            let mut children_changed = false;
            let children = data
                .child_data()
                .iter()
                .cloned()
                .enumerate()
                .map(|(index, child)| {
                    let (mut child, changed) = strip_struct_validity(child)?;
                    children_changed |= changed;
                    if let (Some(fields), Some(struct_nulls)) = (&struct_fields, &struct_nulls)
                        && !fields[index].is_nullable()
                    {
                        let original_null_count = child.null_count();
                        child = force_valid_under_null_struct(child, struct_nulls)?;
                        children_changed |= child.null_count() != original_null_count;
                    }
                    Ok(child)
                })
                .collect::<Result<Vec<_>>>()?;
            if struct_nulls.is_none() && !children_changed {
                return Ok((data, false));
            }

            let mut builder = data.into_builder().child_data(children);
            if struct_nulls.is_some() {
                builder = builder.nulls(None);
            }
            Ok((builder.build()?, true))
        }

        fn normalize(data: ArrayData) -> Result<(ArrayData, bool)> {
            let is_fixed_size_list = matches!(data.data_type(), DataType::FixedSizeList(_, _));
            let mut children_changed = false;
            let children = data
                .child_data()
                .iter()
                .cloned()
                .map(|child| {
                    let (child, changed) = if is_fixed_size_list {
                        strip_struct_validity(child)?
                    } else {
                        normalize(child)?
                    };
                    children_changed |= changed;
                    Ok(child)
                })
                .collect::<Result<Vec<_>>>()?;
            if !children_changed {
                return Ok((data, false));
            }
            Ok((data.into_builder().child_data(children).build()?, true))
        }

        let (data, changed) = normalize(array.to_data())?;
        if changed {
            Ok(make_array(data))
        } else {
            Ok(array)
        }
    }

    fn create_field_encoder_raw(
        &self,
        field: &Field,
        column_index: &mut ColumnIndexSequence,
        context: &FieldEncodingContext<'_>,
    ) -> Result<Box<dyn FieldEncoder>> {
        let options = context.options;
        let data_type = field.data_type();
        if Self::is_primitive_type(&data_type) {
            let column_index = column_index.next_column_index(field.id as u32);
            if field.metadata.contains_key(BLOB_META_KEY) {
                let mut packed_meta = HashMap::new();
                packed_meta.insert(PACKED_STRUCT_META_KEY.to_string(), "true".to_string());
                let desc_field =
                    Field::try_from(BLOB_DESC_FIELD.clone().with_metadata(packed_meta)).unwrap();
                let desc_encoder = Box::new(PrimitiveFieldEncoder::try_new(
                    options,
                    self.array_encoding_strategy.clone(),
                    column_index,
                    desc_field,
                )?);
                Ok(Box::new(BlobFieldEncoder::new(desc_encoder)))
            } else {
                Ok(Box::new(PrimitiveFieldEncoder::try_new(
                    options,
                    self.array_encoding_strategy.clone(),
                    column_index,
                    field.clone(),
                )?))
            }
        } else {
            match data_type {
                DataType::List(_child) | DataType::LargeList(_child) => {
                    let list_idx = column_index.next_column_index(field.id as u32);
                    let inner_encoding =
                        self.create_field_encoder_raw(&field.children[0], column_index, context)?;
                    let offsets_encoder =
                        Arc::new(BasicEncoder::new(Box::new(ValueEncoder::default())));
                    Ok(Box::new(ListFieldEncoder::new(
                        inner_encoding,
                        offsets_encoder,
                        options.cache_bytes_per_column,
                        options.keep_original_array,
                        list_idx,
                    )))
                }
                DataType::Struct(_) => {
                    let field_metadata = &field.metadata;
                    if field_metadata
                        .get(PACKED_STRUCT_LEGACY_META_KEY)
                        .map(|v| str_is_truthy(v))
                        .unwrap_or(field_metadata.contains_key(PACKED_STRUCT_META_KEY))
                    {
                        Ok(Box::new(PrimitiveFieldEncoder::try_new(
                            options,
                            self.array_encoding_strategy.clone(),
                            column_index.next_column_index(field.id as u32),
                            field.clone(),
                        )?))
                    } else {
                        let header_idx = column_index.next_column_index(field.id as u32);
                        let children_encoders = field
                            .children
                            .iter()
                            .map(|field| {
                                self.create_field_encoder_raw(field, column_index, context)
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Ok(Box::new(StructFieldEncoder::new(
                            children_encoders,
                            header_idx,
                        )))
                    }
                }
                DataType::Dictionary(_, value_type) => {
                    if Self::is_primitive_type(&value_type) {
                        Ok(Box::new(PrimitiveFieldEncoder::try_new(
                            options,
                            self.array_encoding_strategy.clone(),
                            column_index.next_column_index(field.id as u32),
                            field.clone(),
                        )?))
                    } else {
                        Err(Error::not_supported_source(format!(
                            "cannot encode a dictionary column whose value type is a logical type ({})",
                            value_type
                        ).into()))
                    }
                }
                _ => Err(Error::not_supported_source(
                    format!(
                        "Lance v2.0 has no field encoding for '{}' with data type {}",
                        field.name,
                        field.data_type()
                    )
                    .into(),
                )),
            }
        }
    }
}

impl Default for ArrayFieldEncodingStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl FieldEncodingStrategy for ArrayFieldEncodingStrategy {
    fn validate_array(&self, array: &dyn Array, field: &Field) -> Result<()> {
        Self::validate_v2_0_array(array, field, true)
    }

    fn create_field_encoder(
        &self,
        field: &Field,
        column_index: &mut ColumnIndexSequence,
        context: &FieldEncodingContext<'_>,
    ) -> Result<Box<dyn FieldEncoder>> {
        let encoder = self.create_field_encoder_raw(field, column_index, context)?;
        Ok(Box::new(ValidatingFieldEncoder::new(
            encoder,
            field.clone(),
        )))
    }
}

/// Page-encoding selection for the `pb::ArrayEncoding` grammar.
#[derive(Debug)]
struct ArrayStrategy;

impl ArrayStrategy {
    fn get_field_compression(field_meta: &HashMap<String, String>) -> Option<CompressionConfig> {
        let compression = field_meta.get(COMPRESSION_META_KEY)?;
        let compression_scheme = compression.parse::<CompressionScheme>();
        match compression_scheme {
            Ok(compression_scheme) => Some(CompressionConfig::new(
                compression_scheme,
                field_meta
                    .get(COMPRESSION_LEVEL_META_KEY)
                    .and_then(|level| level.parse().ok()),
            )),
            Err(_) => None,
        }
    }

    fn default_binary_encoder(
        arrays: &[ArrayRef],
        field_meta: Option<&HashMap<String, String>>,
        data_size: u64,
    ) -> Result<Box<dyn ArrayEncoder>> {
        let bin_indices_encoder =
            Self::choose_array_encoder(arrays, &DataType::UInt64, data_size, false, None)?;

        if let Some(compression) = field_meta.and_then(Self::get_field_compression) {
            if compression.scheme() == CompressionScheme::Fsst {
                // User requested FSST
                let raw_encoder = Box::new(BinaryEncoder::try_new(bin_indices_encoder, None)?);
                Ok(Box::new(FsstArrayEncoder::new(raw_encoder)))
            } else {
                // Generic compression
                Ok(Box::new(BinaryEncoder::try_new(
                    bin_indices_encoder,
                    Some(compression),
                )?))
            }
        } else {
            Ok(Box::new(BinaryEncoder::try_new(bin_indices_encoder, None)?))
        }
    }

    fn choose_array_encoder(
        arrays: &[ArrayRef],
        data_type: &DataType,
        data_size: u64,
        use_dict_encoding: bool,
        field_meta: Option<&HashMap<String, String>>,
    ) -> Result<Box<dyn ArrayEncoder>> {
        match data_type {
            DataType::FixedSizeList(inner, dimension) => {
                Ok(Box::new(BasicEncoder::new(Box::new(FslEncoder::new(
                    Self::choose_array_encoder(
                        arrays,
                        inner.data_type(),
                        data_size,
                        use_dict_encoding,
                        None,
                    )?,
                    *dimension as u32,
                )))))
            }
            DataType::Dictionary(key_type, value_type) => {
                let key_encoder =
                    Self::choose_array_encoder(arrays, key_type, data_size, false, None)?;
                let value_encoder =
                    Self::choose_array_encoder(arrays, value_type, data_size, false, None)?;

                Ok(Box::new(AlreadyDictionaryEncoder::new(
                    key_encoder,
                    value_encoder,
                )))
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
                if use_dict_encoding {
                    let dict_indices_encoder = Self::choose_array_encoder(
                        // We need to pass arrays to this method to figure out what kind of compression to
                        // use but we haven't actually calculated the indices yet.  For now, we just assume
                        // worst case and use the full range.  In the future maybe we can pass in statistics
                        // instead of the actual data
                        &[Arc::new(UInt8Array::from_iter_values(0_u8..255_u8))],
                        &DataType::UInt8,
                        data_size,
                        false,
                        None,
                    )?;
                    let dict_items_encoder = Self::choose_array_encoder(
                        arrays,
                        &DataType::Utf8,
                        data_size,
                        false,
                        None,
                    )?;

                    Ok(Box::new(DictionaryEncoder::new(
                        dict_indices_encoder,
                        dict_items_encoder,
                    )))
                } else {
                    Self::default_binary_encoder(arrays, field_meta, data_size)
                }
            }
            DataType::Struct(fields) => {
                let num_fields = fields.len();
                let mut inner_encoders = Vec::new();

                for i in 0..num_fields {
                    let inner_datatype = fields[i].data_type();
                    let inner_encoder = Self::choose_array_encoder(
                        arrays,
                        inner_datatype,
                        data_size,
                        use_dict_encoding,
                        None,
                    )?;
                    inner_encoders.push(inner_encoder);
                }

                Ok(Box::new(PackedStructEncoder::new(inner_encoders)))
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => Ok(
                Box::new(BasicEncoder::new(Box::new(ValueEncoder::default()))),
            ),

            // TODO: for signed integers, I intend to make it a cascaded encoding, a sparse array for the negative values and very wide(bit-width) values,
            // then a bitpacked array for the narrow(bit-width) values, I need `BitpackedForNeg` to be merged first, I am
            // thinking about putting this sparse array in the metadata so bitpacking remain using one page buffer only.
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(Box::new(
                BasicEncoder::new(Box::new(ValueEncoder::default())),
            )),
            _ => Ok(Box::new(BasicEncoder::new(Box::new(
                ValueEncoder::default(),
            )))),
        }
    }
}

fn get_dict_encoding_threshold() -> u64 {
    env::var("LANCE_DICT_ENCODING_THRESHOLD")
        .ok()
        .and_then(|val| val.parse().ok())
        .unwrap_or(100)
}

// check whether we want to use dictionary encoding or not
// by applying a threshold on cardinality
// returns true if cardinality < threshold but false if the total number of rows is less than the threshold
// The choice to use 100 is just a heuristic for now
// hyperloglog is used for cardinality estimation
// error rate = 1.04 / sqrt(2^p), where p is the precision
// and error rate is 1.04 / sqrt(2^12) = 1.56%
fn check_dict_encoding(arrays: &[ArrayRef], threshold: u64) -> bool {
    let num_total_rows = arrays.iter().map(|arr| arr.len()).sum::<usize>();
    if num_total_rows < threshold as usize {
        return false;
    }
    const PRECISION: u8 = 12;

    let mut hll: HyperLogLogPlus<String, RandomState> =
        HyperLogLogPlus::new(PRECISION, RandomState::new()).unwrap();

    for arr in arrays {
        let string_array = arrow_array::cast::as_string_array(arr);
        for value in string_array.iter().flatten() {
            hll.insert(value);
            let estimated_cardinality = hll.count() as u64;
            if estimated_cardinality >= threshold {
                return false;
            }
        }
    }

    true
}

#[cfg(test)]
fn check_fixed_size_encoding(arrays: &[ArrayRef]) -> Option<u64> {
    if arrays.is_empty() {
        return None;
    }

    // make sure no array has an empty string
    if !arrays.iter().all(|arr| {
        if let Some(arr) = arr.as_string_opt::<i32>() {
            arr.iter().flatten().all(|s| !s.is_empty())
        } else if let Some(arr) = arr.as_binary_opt::<i32>() {
            arr.iter().flatten().all(|s| !s.is_empty())
        } else if let Some(arr) = arr.as_string_opt::<i64>() {
            arr.iter().flatten().all(|s| !s.is_empty())
        } else if let Some(arr) = arr.as_binary_opt::<i64>() {
            arr.iter().flatten().all(|s| !s.is_empty())
        } else {
            panic!("wrong dtype");
        }
    }) {
        return None;
    }

    let lengths = arrays
        .iter()
        .flat_map(|arr| {
            if let Some(arr) = arr.as_string_opt::<i32>() {
                let offsets = arr.offsets().inner();
                offsets
                    .windows(2)
                    .map(|w| (w[1] - w[0]) as u64)
                    .collect::<Vec<_>>()
            } else if let Some(arr) = arr.as_binary_opt::<i32>() {
                let offsets = arr.offsets().inner();
                offsets
                    .windows(2)
                    .map(|w| (w[1] - w[0]) as u64)
                    .collect::<Vec<_>>()
            } else if let Some(arr) = arr.as_string_opt::<i64>() {
                let offsets = arr.offsets().inner();
                offsets
                    .windows(2)
                    .map(|w| (w[1] - w[0]) as u64)
                    .collect::<Vec<_>>()
            } else if let Some(arr) = arr.as_binary_opt::<i64>() {
                let offsets = arr.offsets().inner();
                offsets
                    .windows(2)
                    .map(|w| (w[1] - w[0]) as u64)
                    .collect::<Vec<_>>()
            } else {
                panic!("wrong dtype");
            }
        })
        .collect::<Vec<_>>();

    // find first non-zero value in lengths
    let first_non_zero = lengths.iter().position(|&x| x != 0);
    if let Some(first_non_zero) = first_non_zero {
        // make sure all lengths are equal to first_non_zero length or zero
        if !lengths
            .iter()
            .all(|&x| x == 0 || x == lengths[first_non_zero])
        {
            return None;
        }

        // set the byte width
        Some(lengths[first_non_zero])
    } else {
        None
    }
}

impl ArrayEncodingStrategy for ArrayStrategy {
    fn create_array_encoder(
        &self,
        arrays: &[ArrayRef],
        field: &Field,
    ) -> Result<Box<dyn ArrayEncoder>> {
        let data_size = arrays
            .iter()
            .map(|arr| arr.get_buffer_memory_size() as u64)
            .sum::<u64>();
        let data_type = arrays[0].data_type();

        let use_dict_encoding = data_type == &DataType::Utf8
            && check_dict_encoding(arrays, get_dict_encoding_threshold());

        Self::choose_array_encoder(
            arrays,
            data_type,
            data_size,
            use_dict_encoding,
            Some(&field.metadata),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ArrayEncodingStrategy, ArrayFieldEncodingStrategy, ArrayStrategy, check_dict_encoding,
        check_fixed_size_encoding,
    };
    use crate::constants::{COMPRESSION_LEVEL_META_KEY, COMPRESSION_META_KEY};
    use crate::encoder::{BatchEncoder, EncodingOptions};
    use arrow_array::{ArrayRef, StringArray};
    use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema};
    use lance_core::{Error, datatypes::Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_unsupported_field_type_returns_error() {
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ])),
            false,
        );
        let arrow_schema = ArrowSchema::new(vec![Field::new(
            "attributes",
            DataType::Map(Arc::new(entries), false),
            true,
        )]);
        let schema = Schema::try_from(&arrow_schema).unwrap();

        let error = BatchEncoder::try_new(
            &schema,
            &ArrayFieldEncodingStrategy::new(),
            &EncodingOptions::default(),
        )
        .err()
        .unwrap();

        assert!(matches!(error, Error::NotSupported { .. }));
        assert!(error.to_string().contains("attributes"));
        assert!(error.to_string().contains("Map"));
    }

    fn is_dict_encoding_applicable(arr: Vec<Option<&str>>, threshold: u64) -> bool {
        let arr = StringArray::from(arr);
        let arr = Arc::new(arr) as ArrayRef;
        check_dict_encoding(&[arr], threshold)
    }

    #[test]
    fn test_dict_encoding_should_be_applied_if_cardinality_less_than_threshold() {
        assert!(is_dict_encoding_applicable(
            vec![Some("a"), Some("b"), Some("a"), Some("b")],
            3,
        ));
    }

    #[test]
    fn test_dict_encoding_should_not_be_applied_if_cardinality_larger_than_threshold() {
        assert!(!is_dict_encoding_applicable(
            vec![Some("a"), Some("b"), Some("c"), Some("d")],
            3,
        ));
    }

    #[test]
    fn test_dict_encoding_should_not_be_applied_if_cardinality_equal_to_threshold() {
        assert!(!is_dict_encoding_applicable(
            vec![Some("a"), Some("b"), Some("c"), Some("a")],
            3,
        ));
    }

    #[test]
    fn test_dict_encoding_should_not_be_applied_for_empty_arrays() {
        assert!(!is_dict_encoding_applicable(vec![], 3));
    }

    #[test]
    fn test_dict_encoding_should_not_be_applied_for_smaller_than_threshold_arrays() {
        assert!(!is_dict_encoding_applicable(vec![Some("a"), Some("a")], 3));
    }

    fn is_fixed_size_encoding_applicable(arrays: Vec<Vec<Option<&str>>>) -> bool {
        let mut final_arrays = Vec::new();
        for arr in arrays {
            let arr = StringArray::from(arr);
            let arr = Arc::new(arr) as ArrayRef;
            final_arrays.push(arr);
        }

        check_fixed_size_encoding(&final_arrays).is_some()
    }

    #[test]
    fn test_fixed_size_binary_encoding_applicable() {
        assert!(!is_fixed_size_encoding_applicable(vec![vec![]]));

        assert!(is_fixed_size_encoding_applicable(vec![vec![
            Some("a"),
            Some("b")
        ]]));

        assert!(!is_fixed_size_encoding_applicable(vec![vec![
            Some("abc"),
            Some("de")
        ]]));

        assert!(is_fixed_size_encoding_applicable(vec![vec![
            Some("pqr"),
            None
        ]]));

        assert!(!is_fixed_size_encoding_applicable(vec![vec![
            Some("pqr"),
            Some("")
        ]]));

        assert!(!is_fixed_size_encoding_applicable(vec![vec![
            Some(""),
            Some("")
        ]]));
    }

    #[test]
    fn test_fixed_size_binary_encoding_applicable_multiple_arrays() {
        assert!(is_fixed_size_encoding_applicable(vec![
            vec![Some("a"), Some("b")],
            vec![Some("c"), Some("d")]
        ]));

        assert!(!is_fixed_size_encoding_applicable(vec![
            vec![Some("ab"), Some("bc")],
            vec![Some("c"), Some("d")]
        ]));

        assert!(!is_fixed_size_encoding_applicable(vec![
            vec![Some("ab"), None],
            vec![None, Some("d")]
        ]));

        assert!(is_fixed_size_encoding_applicable(vec![
            vec![Some("a"), None],
            vec![None, Some("d")]
        ]));

        assert!(!is_fixed_size_encoding_applicable(vec![
            vec![Some(""), None],
            vec![None, Some("")]
        ]));

        assert!(!is_fixed_size_encoding_applicable(vec![
            vec![None, None],
            vec![None, None]
        ]));
    }

    fn verify_array_encoder(
        array: ArrayRef,
        field_meta: Option<HashMap<String, String>>,
        expected_encoder: &str,
    ) {
        let encoding_strategy = ArrayStrategy;
        let mut field = Field::new("test_field", array.data_type().clone(), true);
        if let Some(field_meta) = field_meta {
            field.set_metadata(field_meta);
        }
        let lance_field = lance_core::datatypes::Field::try_from(field).unwrap();
        let encoder_result = encoding_strategy.create_array_encoder(&[array], &lance_field);
        assert!(encoder_result.is_ok());
        let encoder = encoder_result.unwrap();
        assert_eq!(format!("{:?}", encoder).as_str(), expected_encoder);
    }

    #[test]
    fn test_choose_encoder_for_zstd_compressed_string_field() {
        verify_array_encoder(
            Arc::new(StringArray::from(vec!["a", "bb", "ccc"])),
            Some(HashMap::from([(
                COMPRESSION_META_KEY.to_string(),
                "zstd".to_string(),
            )])),
            "BinaryEncoder { indices_encoder: BasicEncoder { values_encoder: ValueEncoder }, compression_config: Some(CompressionConfig { scheme: Zstd, level: None }), buffer_compressor: Some(ZstdBufferCompressor { compression_level: 0 }) }",
        );
    }

    #[test]
    fn test_choose_encoder_for_zstd_compression_level() {
        verify_array_encoder(
            Arc::new(StringArray::from(vec!["a", "bb", "ccc"])),
            Some(HashMap::from([
                (COMPRESSION_META_KEY.to_string(), "zstd".to_string()),
                (COMPRESSION_LEVEL_META_KEY.to_string(), "22".to_string()),
            ])),
            "BinaryEncoder { indices_encoder: BasicEncoder { values_encoder: ValueEncoder }, compression_config: Some(CompressionConfig { scheme: Zstd, level: Some(22) }), buffer_compressor: Some(ZstdBufferCompressor { compression_level: 22 }) }",
        );
    }
}
