// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Version-free structural field encoder builders.

use std::sync::Arc;

use arrow_schema::DataType;
use lance_core::{Error, Result, datatypes::Field, error::LanceOptionExt};

pub use crate::encodings::logical::primitive::PrimitivePageEncoding;

use crate::encodings::logical::{
    blob::{BlobStructuralEncoder, BlobV2StructuralEncoder},
    fixed_size_list::FixedSizeListStructuralEncoder,
    list::ListStructuralEncoder,
    map::MapStructuralEncoder,
    primitive::PrimitiveStructuralEncoder,
    r#struct::StructStructuralEncoder,
};

use super::{ColumnIndexSequence, FieldEncoder, FieldEncodingContext};

/// Encode primitive leaves, primitive fixed-size lists, dictionaries, and
/// packed or empty structs using one concrete primitive page grammar.
#[derive(Debug, Clone)]
pub struct PrimitiveFieldEncoding {
    page_encodings: Arc<[PrimitivePageEncoding]>,
}

impl PrimitiveFieldEncoding {
    /// Create a primitive field mechanism from ordered executable page behaviors.
    pub fn new(page_encodings: impl IntoIterator<Item = PrimitivePageEncoding>) -> Self {
        Self {
            page_encodings: page_encodings.into_iter().collect(),
        }
    }

    fn is_primitive_type(data_type: &DataType) -> bool {
        match data_type {
            DataType::FixedSizeList(inner, _) => Self::is_primitive_type(inner.data_type()),
            _ => matches!(
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
                    | DataType::Binary
                    | DataType::LargeBinary
                    | DataType::Utf8
                    | DataType::LargeUtf8,
            ),
        }
    }

    fn create_at(
        &self,
        field: Field,
        column_index: u32,
        context: &FieldEncodingContext<'_>,
    ) -> Result<Box<dyn FieldEncoder>> {
        Ok(Box::new(PrimitiveStructuralEncoder::try_new(
            context.options,
            self.page_encodings.clone(),
            column_index,
            field,
            Arc::new(context.root_field_metadata.clone()),
        )?))
    }

    /// Create a primitive field encoder when this mechanism recognizes `field`.
    pub fn try_create(
        &self,
        field: &Field,
        column_index: &mut ColumnIndexSequence,
        context: &FieldEncodingContext<'_>,
    ) -> Result<Option<Box<dyn FieldEncoder>>> {
        if field.is_blob() {
            return Ok(None);
        }

        let data_type = field.data_type();
        let is_primitive = Self::is_primitive_type(&data_type);
        let is_packed_or_empty_struct = matches!(
            &data_type,
            DataType::Struct(fields) if field.is_packed_struct() || fields.is_empty()
        );
        let is_primitive_dictionary = matches!(
            &data_type,
            DataType::Dictionary(_, value_type) if Self::is_primitive_type(value_type)
        );

        if !is_primitive && !is_packed_or_empty_struct && !is_primitive_dictionary {
            if let DataType::Dictionary(_, value_type) = data_type {
                return Err(Error::not_supported_source(
                    format!(
                        "cannot encode a dictionary column whose value type is a logical type ({})",
                        value_type
                    )
                    .into(),
                ));
            }
            return Ok(None);
        }

        Ok(Some(self.create_at(
            field.clone(),
            column_index.next_column_index(field.id as u32),
            context,
        )?))
    }
}

/// Create the original binary blob descriptor when `field` matches.
pub fn try_create_binary_blob(
    primitive: &PrimitiveFieldEncoding,
    field: &Field,
    column_index: &mut ColumnIndexSequence,
    context: &FieldEncodingContext<'_>,
) -> Result<Option<Box<dyn FieldEncoder>>> {
    if !field.is_blob() || !matches!(field.data_type(), DataType::Binary | DataType::LargeBinary) {
        return Ok(None);
    }
    let descriptor_column_index = column_index.next_column_index(field.id as u32);
    Ok(Some(Box::new(BlobStructuralEncoder::new(
        field,
        |descriptor_field| primitive.create_at(descriptor_field, descriptor_column_index, context),
    )?)))
}

/// Create the structural blob descriptor when `field` matches.
pub fn try_create_structural_blob(
    primitive: &PrimitiveFieldEncoding,
    field: &Field,
    column_index: &mut ColumnIndexSequence,
    context: &FieldEncodingContext<'_>,
) -> Result<Option<Box<dyn FieldEncoder>>> {
    if !field.is_blob() || !matches!(field.data_type(), DataType::Struct(_)) {
        return Ok(None);
    }
    let descriptor_column_index = column_index.next_column_index(field.id as u32);
    Ok(Some(Box::new(BlobV2StructuralEncoder::new(
        field,
        |descriptor_field| primitive.create_at(descriptor_field, descriptor_column_index, context),
    )?)))
}

/// Create a variable-size list encoder when `field` matches.
pub fn try_create_list(
    field: &Field,
    column_index: &mut ColumnIndexSequence,
    context: &FieldEncodingContext<'_>,
) -> Result<Option<Box<dyn FieldEncoder>>> {
    if !matches!(
        field.data_type(),
        DataType::List(_) | DataType::LargeList(_)
    ) {
        return Ok(None);
    }
    let child = field.children.first().expect_ok()?;
    let child_encoder = context
        .strategy
        .create_field_encoder(child, column_index, context)?;
    Ok(Some(Box::new(ListStructuralEncoder::new(
        context.options.keep_original_array,
        child_encoder,
    ))))
}

/// Create a fixed-size-list encoder whose child is a struct when applicable.
pub fn try_create_structural_fixed_size_list(
    field: &Field,
    column_index: &mut ColumnIndexSequence,
    context: &FieldEncodingContext<'_>,
) -> Result<Option<Box<dyn FieldEncoder>>> {
    if !matches!(
        field.data_type(),
        DataType::FixedSizeList(inner, _) if matches!(inner.data_type(), DataType::Struct(_))
    ) {
        return Ok(None);
    }
    let child = field.children.first().expect_ok()?;
    let child_encoder = context
        .strategy
        .create_field_encoder(child, column_index, context)?;
    Ok(Some(Box::new(FixedSizeListStructuralEncoder::new(
        context.options.keep_original_array,
        child_encoder,
    ))))
}

/// Create an Arrow map encoder when `field` matches.
pub fn try_create_map(
    field: &Field,
    column_index: &mut ColumnIndexSequence,
    context: &FieldEncodingContext<'_>,
) -> Result<Option<Box<dyn FieldEncoder>>> {
    let DataType::Map(_, keys_sorted) = field.data_type() else {
        return Ok(None);
    };
    if keys_sorted {
        return Err(Error::not_supported_source(
            format!(
                "Map data type is not supported with keys_sorted=true now, current value is {}",
                keys_sorted
            )
            .into(),
        ));
    }
    let entries_child = field
        .children
        .first()
        .ok_or_else(|| Error::schema("Map should have an entries child".to_string()))?;
    let DataType::Struct(struct_fields) = entries_child.data_type() else {
        return Err(Error::schema(
            "Map entries field must be a Struct<key, value>".to_string(),
        ));
    };
    if struct_fields.len() < 2 {
        return Err(Error::schema(
            "Map entries struct must contain both key and value fields".to_string(),
        ));
    }
    let key_field = &struct_fields[0];
    if key_field.is_nullable() {
        return Err(Error::schema(format!(
            "Map key field '{}' must be non-nullable according to Arrow Map specification",
            key_field.name()
        )));
    }
    let child_encoder =
        context
            .strategy
            .create_field_encoder(entries_child, column_index, context)?;
    Ok(Some(Box::new(MapStructuralEncoder::new(
        context.options.keep_original_array,
        child_encoder,
    ))))
}

/// Create a non-packed, non-empty struct encoder when `field` matches.
pub fn try_create_struct(
    field: &Field,
    column_index: &mut ColumnIndexSequence,
    context: &FieldEncodingContext<'_>,
) -> Result<Option<Box<dyn FieldEncoder>>> {
    let DataType::Struct(fields) = field.data_type() else {
        return Ok(None);
    };
    if field.is_blob() || field.is_packed_struct() || fields.is_empty() {
        return Ok(None);
    }
    let children_encoders = field
        .children
        .iter()
        .map(|child| {
            context
                .strategy
                .create_field_encoder(child, column_index, context)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(Box::new(StructStructuralEncoder::new(
        context.options.keep_original_array,
        children_encoders,
    ))))
}
