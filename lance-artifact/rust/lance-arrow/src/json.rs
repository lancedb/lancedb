// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! JSON support for Apache Arrow.

use std::convert::TryFrom;
use std::sync::Arc;

use arrow_array::builder::{LargeBinaryBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, LargeBinaryArray, LargeListArray, LargeStringArray,
    ListArray, MapArray, RecordBatch, StringArray, StructArray,
};
use arrow_schema::{ArrowError, DataType, Field as ArrowField, Fields, Schema};

use crate::ARROW_EXT_NAME_KEY;

/// Arrow extension type name for JSON data (Lance internal)
pub const JSON_EXT_NAME: &str = "lance.json";

/// Arrow extension type name for JSON data (Arrow official)
pub const ARROW_JSON_EXT_NAME: &str = "arrow.json";

/// Check if a field is a JSON extension field (Lance internal JSONB storage)
pub fn is_json_field(field: &ArrowField) -> bool {
    field.data_type() == &DataType::LargeBinary
        && field
            .metadata()
            .get(ARROW_EXT_NAME_KEY)
            .map(|name| name == JSON_EXT_NAME)
            .unwrap_or_default()
}

/// Check if a field is an Arrow JSON extension field (PyArrow pa.json() type)
pub fn is_arrow_json_field(field: &ArrowField) -> bool {
    // Arrow JSON extension type uses Utf8 or LargeUtf8 as storage type
    (field.data_type() == &DataType::Utf8 || field.data_type() == &DataType::LargeUtf8)
        && field
            .metadata()
            .get(ARROW_EXT_NAME_KEY)
            .map(|name| name == ARROW_JSON_EXT_NAME)
            .unwrap_or_default()
}

/// Check if a field or any of its descendants is a JSON field
pub fn has_json_fields(field: &ArrowField) -> bool {
    if is_json_field(field) {
        return true;
    }

    match field.data_type() {
        DataType::Struct(fields) => fields.iter().any(|f| has_json_fields(f)),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            has_json_fields(f)
        }
        DataType::Map(f, _) => has_json_fields(f),
        _ => false,
    }
}

/// Check if a field or any of its descendants is an Arrow JSON field
pub fn has_arrow_json_fields(field: &ArrowField) -> bool {
    if is_arrow_json_field(field) {
        return true;
    }

    match field.data_type() {
        DataType::Struct(fields) => fields.iter().any(|f| has_arrow_json_fields(f)),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            has_arrow_json_fields(f)
        }
        DataType::Map(f, _) => has_arrow_json_fields(f),
        _ => false,
    }
}

/// Create a JSON field with the appropriate extension metadata
pub fn json_field(name: &str, nullable: bool) -> ArrowField {
    let mut field = ArrowField::new(name, DataType::LargeBinary, nullable);
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(ARROW_EXT_NAME_KEY.to_string(), JSON_EXT_NAME.to_string());
    field.set_metadata(metadata);
    field
}

/// A specialized array for JSON data stored as JSONB binary format
#[derive(Debug, Clone)]
pub struct JsonArray {
    inner: LargeBinaryArray,
}

impl JsonArray {
    /// Create a new JsonArray from an iterator of JSON strings
    pub fn try_from_iter<I, S>(iter: I) -> Result<Self, ArrowError>
    where
        I: IntoIterator<Item = Option<S>>,
        S: AsRef<str>,
    {
        let mut builder = LargeBinaryBuilder::new();

        for json_str in iter {
            match json_str {
                Some(s) => {
                    let encoded = encode_json(s.as_ref()).map_err(|e| {
                        ArrowError::InvalidArgumentError(format!("Failed to encode JSON: {}", e))
                    })?;
                    builder.append_value(&encoded);
                }
                None => builder.append_null(),
            }
        }

        Ok(Self {
            inner: builder.finish(),
        })
    }

    /// Get the underlying LargeBinaryArray
    pub fn into_inner(self) -> LargeBinaryArray {
        self.inner
    }

    /// Get a reference to the underlying LargeBinaryArray
    pub fn inner(&self) -> &LargeBinaryArray {
        &self.inner
    }

    /// Get the value at index i as decoded JSON string
    pub fn value(&self, i: usize) -> Result<String, ArrowError> {
        if self.inner.is_null(i) {
            return Err(ArrowError::InvalidArgumentError(
                "Value is null".to_string(),
            ));
        }

        let jsonb_bytes = self.inner.value(i);
        Ok(decode_json(jsonb_bytes))
    }

    /// Get the value at index i as raw JSONB bytes
    pub fn value_bytes(&self, i: usize) -> &[u8] {
        self.inner.value(i)
    }

    /// Get JSONPath value from the JSON at index i
    pub fn json_path(&self, i: usize, path: &str) -> Result<Option<String>, ArrowError> {
        if self.inner.is_null(i) {
            return Ok(None);
        }

        let jsonb_bytes = self.inner.value(i);
        get_json_path(jsonb_bytes, path).map_err(|e| {
            ArrowError::InvalidArgumentError(format!("Failed to extract JSONPath: {}", e))
        })
    }

    /// Convert to Arrow string array (JSON as UTF-8)
    pub fn to_arrow_json(&self) -> ArrayRef {
        let mut builder = arrow_array::builder::StringBuilder::new();

        for i in 0..self.inner.len() {
            if self.inner.is_null(i) {
                builder.append_null();
            } else {
                let jsonb_bytes = self.inner.value(i);
                let json_str = decode_json(jsonb_bytes);
                builder.append_value(&json_str);
            }
        }

        // Return as UTF-8 string array (Arrow represents JSON as strings)
        Arc::new(builder.finish())
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn is_null(&self, i: usize) -> bool {
        self.inner.is_null(i)
    }
}

// TryFrom implementations for string arrays
impl TryFrom<StringArray> for JsonArray {
    type Error = ArrowError;

    fn try_from(array: StringArray) -> Result<Self, Self::Error> {
        Self::try_from(&array)
    }
}

impl TryFrom<&StringArray> for JsonArray {
    type Error = ArrowError;

    fn try_from(array: &StringArray) -> Result<Self, Self::Error> {
        let mut builder = LargeBinaryBuilder::with_capacity(array.len(), array.value_data().len());

        for i in 0..array.len() {
            if array.is_null(i) {
                builder.append_null();
            } else {
                let json_str = array.value(i);
                let encoded = encode_json(json_str).map_err(|e| {
                    ArrowError::InvalidArgumentError(format!("Failed to encode JSON: {}", e))
                })?;
                builder.append_value(&encoded);
            }
        }

        Ok(Self {
            inner: builder.finish(),
        })
    }
}

impl TryFrom<LargeStringArray> for JsonArray {
    type Error = ArrowError;

    fn try_from(array: LargeStringArray) -> Result<Self, Self::Error> {
        Self::try_from(&array)
    }
}

impl TryFrom<&LargeStringArray> for JsonArray {
    type Error = ArrowError;

    fn try_from(array: &LargeStringArray) -> Result<Self, Self::Error> {
        let mut builder = LargeBinaryBuilder::with_capacity(array.len(), array.value_data().len());

        for i in 0..array.len() {
            if array.is_null(i) {
                builder.append_null();
            } else {
                let json_str = array.value(i);
                let encoded = encode_json(json_str).map_err(|e| {
                    ArrowError::InvalidArgumentError(format!("Failed to encode JSON: {}", e))
                })?;
                builder.append_value(&encoded);
            }
        }

        Ok(Self {
            inner: builder.finish(),
        })
    }
}

impl TryFrom<ArrayRef> for JsonArray {
    type Error = ArrowError;

    fn try_from(array_ref: ArrayRef) -> Result<Self, Self::Error> {
        match array_ref.data_type() {
            DataType::Utf8 => {
                // Downcast is guaranteed to succeed after matching on DataType::Utf8
                let string_array = array_ref
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("DataType::Utf8 array must be StringArray");
                Self::try_from(string_array)
            }
            DataType::LargeUtf8 => {
                // Downcast is guaranteed to succeed after matching on DataType::LargeUtf8
                let large_string_array = array_ref
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .expect("DataType::LargeUtf8 array must be LargeStringArray");
                Self::try_from(large_string_array)
            }
            dt => Err(ArrowError::InvalidArgumentError(format!(
                "Unsupported array type for JSON: {:?}. Expected Utf8 or LargeUtf8",
                dt
            ))),
        }
    }
}

/// Encode JSON string to JSONB format
pub fn encode_json(json_str: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let value = jsonb::parse_value(json_str.as_bytes())?;
    Ok(value.to_vec())
}

/// Decode JSONB bytes to JSON string
pub fn decode_json(jsonb_bytes: &[u8]) -> String {
    let raw_jsonb = jsonb::RawJsonb::new(jsonb_bytes);
    raw_jsonb.to_string()
}

/// Extract JSONPath value from JSONB
fn get_json_path(
    jsonb_bytes: &[u8],
    path: &str,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let json_path = jsonb::jsonpath::parse_json_path(path.as_bytes())?;
    let raw_jsonb = jsonb::RawJsonb::new(jsonb_bytes);
    let mut selector = jsonb::jsonpath::Selector::new(raw_jsonb);

    let values = selector.select_values(&json_path)?;
    if values.is_empty() {
        Ok(None)
    } else {
        Ok(Some(values[0].to_string()))
    }
}

/// Convert an Arrow JSON field to Lance JSON field (with JSONB storage)
pub fn arrow_json_to_lance_json(field: &ArrowField) -> ArrowField {
    if is_arrow_json_field(field) {
        return field_with_extension(field, DataType::LargeBinary, JSON_EXT_NAME);
    }

    let data_type = match field.data_type() {
        DataType::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|field| Arc::new(arrow_json_to_lance_json(field)))
                .collect::<Vec<_>>();
            DataType::Struct(Fields::from(fields))
        }
        DataType::List(item) => DataType::List(Arc::new(arrow_json_to_lance_json(item))),
        DataType::LargeList(item) => DataType::LargeList(Arc::new(arrow_json_to_lance_json(item))),
        DataType::FixedSizeList(item, size) => {
            DataType::FixedSizeList(Arc::new(arrow_json_to_lance_json(item)), *size)
        }
        DataType::Map(entries, keys_sorted) => {
            DataType::Map(Arc::new(arrow_json_to_lance_json(entries)), *keys_sorted)
        }
        _ => return field.clone(),
    };

    field_with_data_type(field, data_type)
}

/// Convert a Lance JSON field to Arrow JSON field.
pub fn lance_json_to_arrow_json(field: &ArrowField) -> ArrowField {
    if is_json_field(field) {
        return field_with_extension(field, DataType::Utf8, ARROW_JSON_EXT_NAME);
    }

    let data_type = match field.data_type() {
        DataType::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|field| Arc::new(lance_json_to_arrow_json(field)))
                .collect::<Vec<_>>();
            DataType::Struct(Fields::from(fields))
        }
        DataType::List(item) => DataType::List(Arc::new(lance_json_to_arrow_json(item))),
        DataType::LargeList(item) => DataType::LargeList(Arc::new(lance_json_to_arrow_json(item))),
        DataType::FixedSizeList(item, size) => {
            DataType::FixedSizeList(Arc::new(lance_json_to_arrow_json(item)), *size)
        }
        DataType::Map(entries, keys_sorted) => {
            DataType::Map(Arc::new(lance_json_to_arrow_json(entries)), *keys_sorted)
        }
        _ => return field.clone(),
    };

    field_with_data_type(field, data_type)
}

fn field_with_data_type(field: &ArrowField, data_type: DataType) -> ArrowField {
    ArrowField::new(field.name(), data_type, field.is_nullable())
        .with_metadata(field.metadata().clone())
}

fn field_with_extension(
    field: &ArrowField,
    data_type: DataType,
    extension_name: &str,
) -> ArrowField {
    let mut metadata = field.metadata().clone();
    metadata.insert(ARROW_EXT_NAME_KEY.to_string(), extension_name.to_string());
    ArrowField::new(field.name(), data_type, field.is_nullable()).with_metadata(metadata)
}

fn convert_json_array<F>(
    field: &ArrowField,
    array: &ArrayRef,
    convert_leaf: &F,
) -> Result<(ArrowField, ArrayRef, bool), ArrowError>
where
    F: Fn(&ArrowField, &ArrayRef) -> Result<Option<(ArrowField, ArrayRef)>, ArrowError>,
{
    if let Some((field, array)) = convert_leaf(field, array)? {
        return Ok((field, array, true));
    }

    match field.data_type() {
        DataType::Struct(fields) => {
            let struct_array = array.as_struct();
            let mut new_fields = Vec::with_capacity(fields.len());
            let mut new_columns = Vec::with_capacity(fields.len());
            let mut changed = false;

            for (field, column) in fields.iter().zip(struct_array.columns()) {
                let (new_field, new_column, field_changed) =
                    convert_json_array(field, column, convert_leaf)?;
                changed |= field_changed;
                new_fields.push(Arc::new(new_field));
                new_columns.push(new_column);
            }

            if changed {
                let fields = Fields::from(new_fields);
                let new_field = field_with_data_type(field, DataType::Struct(fields.clone()));
                let new_array =
                    StructArray::new(fields, new_columns, struct_array.nulls().cloned());
                Ok((new_field, Arc::new(new_array) as ArrayRef, true))
            } else {
                Ok((field.clone(), array.clone(), false))
            }
        }
        DataType::List(item) => {
            let list_array: &ListArray = array.as_list();
            let (new_item, new_values, changed) =
                convert_json_array(item, list_array.values(), convert_leaf)?;
            if changed {
                let new_field =
                    field_with_data_type(field, DataType::List(Arc::new(new_item.clone())));
                let new_array = ListArray::new(
                    Arc::new(new_item),
                    list_array.offsets().clone(),
                    new_values,
                    list_array.nulls().cloned(),
                );
                Ok((new_field, Arc::new(new_array) as ArrayRef, true))
            } else {
                Ok((field.clone(), array.clone(), false))
            }
        }
        DataType::LargeList(item) => {
            let list_array: &LargeListArray = array.as_list();
            let (new_item, new_values, changed) =
                convert_json_array(item, list_array.values(), convert_leaf)?;
            if changed {
                let new_field =
                    field_with_data_type(field, DataType::LargeList(Arc::new(new_item.clone())));
                let new_array = LargeListArray::new(
                    Arc::new(new_item),
                    list_array.offsets().clone(),
                    new_values,
                    list_array.nulls().cloned(),
                );
                Ok((new_field, Arc::new(new_array) as ArrayRef, true))
            } else {
                Ok((field.clone(), array.clone(), false))
            }
        }
        DataType::FixedSizeList(item, size) => {
            let list_array: &FixedSizeListArray = array.as_fixed_size_list();
            let (new_item, new_values, changed) =
                convert_json_array(item, list_array.values(), convert_leaf)?;
            if changed {
                let new_field = field_with_data_type(
                    field,
                    DataType::FixedSizeList(Arc::new(new_item.clone()), *size),
                );
                let new_array = FixedSizeListArray::try_new_with_length(
                    Arc::new(new_item),
                    *size,
                    new_values,
                    list_array.nulls().cloned(),
                    list_array.len(),
                )?;
                Ok((new_field, Arc::new(new_array) as ArrayRef, true))
            } else {
                Ok((field.clone(), array.clone(), false))
            }
        }
        DataType::Map(entries, keys_sorted) => {
            let map_array = array
                .as_any()
                .downcast_ref::<MapArray>()
                .expect("DataType::Map array must be MapArray");
            let entries_array = Arc::new(map_array.entries().clone()) as ArrayRef;
            let (new_entries, new_entries_array, changed) =
                convert_json_array(entries, &entries_array, convert_leaf)?;
            if changed {
                let entries_struct = new_entries_array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("Map entries must be StructArray")
                    .clone();
                let new_field = field_with_data_type(
                    field,
                    DataType::Map(Arc::new(new_entries.clone()), *keys_sorted),
                );
                let new_array = MapArray::new(
                    Arc::new(new_entries),
                    map_array.offsets().clone(),
                    entries_struct,
                    map_array.nulls().cloned(),
                    *keys_sorted,
                );
                Ok((new_field, Arc::new(new_array) as ArrayRef, true))
            } else {
                Ok((field.clone(), array.clone(), false))
            }
        }
        _ => Ok((field.clone(), array.clone(), false)),
    }
}

fn convert_arrow_json_array(
    field: &ArrowField,
    array: &ArrayRef,
) -> Result<(ArrowField, ArrayRef, bool), ArrowError> {
    convert_json_array(field, array, &|field, array| {
        if is_arrow_json_field(field) {
            let json_array = JsonArray::try_from(array.clone())?;
            Ok(Some((
                arrow_json_to_lance_json(field),
                Arc::new(json_array.into_inner()) as ArrayRef,
            )))
        } else {
            Ok(None)
        }
    })
}

fn convert_lance_json_array(
    field: &ArrowField,
    array: &ArrayRef,
) -> Result<(ArrowField, ArrayRef, bool), ArrowError> {
    convert_json_array(field, array, &|field, array| {
        if is_json_field(field) {
            let binary_array = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .expect("Lance JSON field must be LargeBinaryArray");
            let mut builder = StringBuilder::new();

            for i in 0..binary_array.len() {
                if binary_array.is_null(i) {
                    builder.append_null();
                } else {
                    let jsonb_bytes = binary_array.value(i);
                    let json_str = decode_json(jsonb_bytes);
                    builder.append_value(&json_str);
                }
            }

            Ok(Some((
                lance_json_to_arrow_json(field),
                Arc::new(builder.finish()) as ArrayRef,
            )))
        } else {
            Ok(None)
        }
    })
}

/// Convert a RecordBatch with Lance JSON columns (JSONB) back to Arrow JSON format (strings)
pub fn convert_lance_json_to_arrow(
    batch: &arrow_array::RecordBatch,
) -> Result<arrow_array::RecordBatch, ArrowError> {
    let schema = batch.schema();
    let mut needs_conversion = false;
    let mut new_fields = Vec::with_capacity(schema.fields().len());
    let mut new_columns = Vec::with_capacity(batch.num_columns());

    for (i, field) in schema.fields().iter().enumerate() {
        let column = batch.column(i);
        let (new_field, new_column, changed) = convert_lance_json_array(field, column)?;

        needs_conversion |= changed;
        new_fields.push(new_field);
        new_columns.push(new_column);
    }

    if needs_conversion {
        let new_schema = Arc::new(Schema::new_with_metadata(
            new_fields,
            schema.metadata().clone(),
        ));
        RecordBatch::try_new(new_schema, new_columns)
    } else {
        // No conversion needed, return original batch
        Ok(batch.clone())
    }
}

/// Convert a RecordBatch with Arrow JSON columns to Lance JSON format (JSONB)
pub fn convert_json_columns(
    batch: &arrow_array::RecordBatch,
) -> Result<arrow_array::RecordBatch, ArrowError> {
    let schema = batch.schema();
    let mut needs_conversion = false;
    let mut new_fields = Vec::with_capacity(schema.fields().len());
    let mut new_columns = Vec::with_capacity(batch.num_columns());

    for (i, field) in schema.fields().iter().enumerate() {
        let column = batch.column(i);
        let (new_field, new_column, changed) = convert_arrow_json_array(field, column)?;

        needs_conversion |= changed;
        new_fields.push(new_field);
        new_columns.push(new_column);
    }

    if needs_conversion {
        let new_schema = Arc::new(Schema::new_with_metadata(
            new_fields,
            schema.metadata().clone(),
        ));
        RecordBatch::try_new(new_schema, new_columns)
    } else {
        // No conversion needed, return original batch
        Ok(batch.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_field_creation() {
        let field = json_field("data", true);
        assert_eq!(field.name(), "data");
        assert_eq!(field.data_type(), &DataType::LargeBinary);
        assert!(field.is_nullable());
        assert!(is_json_field(&field));
    }

    #[test]
    fn test_json_array_from_strings() {
        let json_strings = vec![
            Some(r#"{"name": "Alice", "age": 30}"#),
            None,
            Some(r#"{"name": "Bob", "age": 25}"#),
        ];

        let array = JsonArray::try_from_iter(json_strings).unwrap();
        assert_eq!(array.len(), 3);
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
        assert!(!array.is_null(2));

        let decoded = array.value(0).unwrap();
        assert!(decoded.contains("Alice"));
    }

    #[test]
    fn test_json_array_from_string_array() {
        let string_array = StringArray::from(vec![
            Some(r#"{"name": "Alice"}"#),
            Some(r#"{"name": "Bob"}"#),
            None,
        ]);

        let json_array = JsonArray::try_from(string_array).unwrap();
        assert_eq!(json_array.len(), 3);
        assert!(!json_array.is_null(0));
        assert!(!json_array.is_null(1));
        assert!(json_array.is_null(2));
    }

    #[test]
    fn test_json_path_extraction() {
        let json_array = JsonArray::try_from_iter(vec![
            Some(r#"{"user": {"name": "Alice", "age": 30}}"#),
            Some(r#"{"user": {"name": "Bob"}}"#),
        ])
        .unwrap();

        let name = json_array.json_path(0, "$.user.name").unwrap();
        assert_eq!(name, Some("\"Alice\"".to_string()));

        let age = json_array.json_path(1, "$.user.age").unwrap();
        assert_eq!(age, None);
    }

    #[test]
    fn test_convert_json_columns() {
        // Create a batch with Arrow JSON column
        let json_strings = vec![Some(r#"{"name": "Alice"}"#), Some(r#"{"name": "Bob"}"#)];
        let json_arr = StringArray::from(json_strings);

        // Create field with arrow.json extension
        let mut field = ArrowField::new("data", DataType::Utf8, false);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            ARROW_JSON_EXT_NAME.to_string(),
        );
        field.set_metadata(metadata);

        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(json_arr) as ArrayRef]).unwrap();

        // Convert the batch
        let converted = convert_json_columns(&batch).unwrap();

        // Check the converted schema
        assert_eq!(converted.num_columns(), 1);
        let converted_schema = converted.schema();
        let converted_field = converted_schema.field(0);
        assert_eq!(converted_field.data_type(), &DataType::LargeBinary);
        assert_eq!(
            converted_field.metadata().get(ARROW_EXT_NAME_KEY),
            Some(&JSON_EXT_NAME.to_string())
        );

        // Check the data was converted
        let converted_column = converted.column(0);
        assert_eq!(converted_column.data_type(), &DataType::LargeBinary);
        assert_eq!(converted_column.len(), 2);

        // Verify the data is valid JSONB
        let binary_array = converted_column
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        for i in 0..binary_array.len() {
            let jsonb_bytes = binary_array.value(i);
            let decoded = decode_json(jsonb_bytes);
            assert!(decoded.contains("name"));
        }
    }

    #[test]
    fn test_convert_nested_json_columns() {
        use arrow_buffer::{OffsetBuffer, ScalarBuffer};

        let uri_field = Arc::new(ArrowField::new("uri", DataType::Utf8, false));
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            ARROW_JSON_EXT_NAME.to_string(),
        );
        let extra_field =
            Arc::new(ArrowField::new("extra", DataType::Utf8, true).with_metadata(metadata));
        let item_fields = Fields::from(vec![uri_field, extra_field]);

        let values = StructArray::new(
            item_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("a.jpg"), Some("b.jpg")])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some(r#"{"codec":"h264"}"#),
                    None::<&str>,
                ])) as ArrayRef,
            ],
            None,
        );
        let item = Arc::new(ArrowField::new("item", DataType::Struct(item_fields), true));
        let media = ListArray::new(
            item,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2])),
            Arc::new(values),
            None,
        );
        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            "media",
            media.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(media) as ArrayRef]).unwrap();

        assert!(has_arrow_json_fields(batch.schema().field(0)));

        let converted = convert_json_columns(&batch).unwrap();
        let converted_schema = converted.schema();
        let DataType::List(item) = converted_schema.field(0).data_type() else {
            panic!("expected list field");
        };
        let DataType::Struct(fields) = item.data_type() else {
            panic!("expected struct item");
        };
        assert!(is_json_field(&fields[1]));

        let list_array: &ListArray = converted.column(0).as_list();
        let values = list_array.values().as_struct();
        let extra = values
            .column(1)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert!(decode_json(extra.value(0)).contains("h264"));
        assert!(extra.is_null(1));

        let logical = convert_lance_json_to_arrow(&converted).unwrap();
        let logical_schema = logical.schema();
        let DataType::List(item) = logical_schema.field(0).data_type() else {
            panic!("expected list field");
        };
        let DataType::Struct(fields) = item.data_type() else {
            panic!("expected struct item");
        };
        assert!(is_arrow_json_field(&fields[1]));

        let list_array: &ListArray = logical.column(0).as_list();
        let values = list_array.values().as_struct();
        let extra = values
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(extra.value(0).contains("h264"));
        assert!(extra.is_null(1));
    }

    #[test]
    fn test_convert_fixed_size_list_zero_json_preserves_length() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            ARROW_JSON_EXT_NAME.to_string(),
        );
        let item = Arc::new(ArrowField::new("item", DataType::Utf8, true).with_metadata(metadata));
        let values = Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as ArrayRef;
        let lists = FixedSizeListArray::try_new_with_length(item, 0, values, None, 3).unwrap();
        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            "lists",
            lists.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(lists) as ArrayRef]).unwrap();

        let converted = convert_json_columns(&batch).unwrap();
        assert_eq!(converted.num_rows(), 3);
        assert_eq!(converted.column(0).len(), 3);

        let converted_schema = converted.schema();
        let DataType::FixedSizeList(item, size) = converted_schema.field(0).data_type() else {
            panic!("expected fixed size list field");
        };
        assert_eq!(*size, 0);
        assert!(is_json_field(item));

        let logical = convert_lance_json_to_arrow(&converted).unwrap();
        assert_eq!(logical.num_rows(), 3);
        assert_eq!(logical.column(0).len(), 3);

        let logical_schema = logical.schema();
        let DataType::FixedSizeList(item, size) = logical_schema.field(0).data_type() else {
            panic!("expected fixed size list field");
        };
        assert_eq!(*size, 0);
        assert!(is_arrow_json_field(item));
    }

    #[test]
    fn test_has_json_fields() {
        // Test direct JSON field
        let json_f = json_field("data", true);
        assert!(has_json_fields(&json_f));

        // Test non-JSON field
        let non_json = ArrowField::new("data", DataType::Utf8, true);
        assert!(!has_json_fields(&non_json));

        // Test struct containing JSON field
        let struct_field = ArrowField::new(
            "struct",
            DataType::Struct(vec![json_field("nested_json", true)].into()),
            true,
        );
        assert!(has_json_fields(&struct_field));

        // Test struct without JSON field
        let struct_no_json = ArrowField::new(
            "struct",
            DataType::Struct(vec![ArrowField::new("text", DataType::Utf8, true)].into()),
            true,
        );
        assert!(!has_json_fields(&struct_no_json));

        // Test List containing JSON field
        let list_field = ArrowField::new(
            "list",
            DataType::List(Arc::new(json_field("item", true))),
            true,
        );
        assert!(has_json_fields(&list_field));

        // Test LargeList containing JSON field
        let large_list_field = ArrowField::new(
            "large_list",
            DataType::LargeList(Arc::new(json_field("item", true))),
            true,
        );
        assert!(has_json_fields(&large_list_field));

        // Test FixedSizeList containing JSON field
        let fixed_list_field = ArrowField::new(
            "fixed_list",
            DataType::FixedSizeList(Arc::new(json_field("item", true)), 3),
            true,
        );
        assert!(has_json_fields(&fixed_list_field));

        // Test Map containing JSON field
        let map_field = ArrowField::new(
            "map",
            DataType::Map(
                Arc::new(ArrowField::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            ArrowField::new("key", DataType::Utf8, false),
                            json_field("value", true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        );
        assert!(has_json_fields(&map_field));
    }

    #[test]
    fn test_json_array_inner() {
        let json_array = JsonArray::try_from_iter(vec![Some(r#"{"a": 1}"#)]).unwrap();
        let inner = json_array.inner();
        assert_eq!(inner.len(), 1);
    }

    #[test]
    fn test_json_array_value_null_error() {
        let json_array = JsonArray::try_from_iter(vec![None::<&str>]).unwrap();
        let result = json_array.value(0);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("null"));
    }

    #[test]
    fn test_json_array_value_bytes() {
        let json_array = JsonArray::try_from_iter(vec![Some(r#"{"a": 1}"#)]).unwrap();
        let bytes = json_array.value_bytes(0);
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_json_path_with_null() {
        let json_array =
            JsonArray::try_from_iter(vec![Some(r#"{"user": {"name": "Alice"}}"#), None::<&str>])
                .unwrap();

        let result = json_array.json_path(1, "$.user.name").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_to_arrow_json() {
        let json_array = JsonArray::try_from_iter(vec![
            Some(r#"{"name": "Alice"}"#),
            None::<&str>,
            Some(r#"{"name": "Bob"}"#),
        ])
        .unwrap();

        let arrow_json = json_array.to_arrow_json();
        assert_eq!(arrow_json.len(), 3);
        assert!(!arrow_json.is_null(0));
        assert!(arrow_json.is_null(1));
        assert!(!arrow_json.is_null(2));

        let string_array = arrow_json.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(string_array.value(0).contains("Alice"));
        assert!(string_array.value(2).contains("Bob"));
    }

    #[test]
    fn test_json_array_trait_methods() {
        let json_array =
            JsonArray::try_from_iter(vec![Some(r#"{"a": 1}"#), Some(r#"{"b": 2}"#)]).unwrap();

        // Wrapper methods
        assert_eq!(json_array.len(), 2);
        assert!(!json_array.is_empty());
        assert!(!json_array.is_null(0));

        // Underlying Arrow array
        assert_eq!(json_array.inner().data_type(), &DataType::LargeBinary);
        assert_eq!(json_array.inner().len(), 2);
    }

    #[test]
    fn test_json_array_empty() {
        let json_array = JsonArray::try_from_iter(Vec::<Option<&str>>::new()).unwrap();
        assert!(json_array.is_empty());
        assert_eq!(json_array.len(), 0);
    }

    #[test]
    fn test_try_from_large_string_array() {
        let large_string_array = LargeStringArray::from(vec![
            Some(r#"{"name": "Alice"}"#),
            Some(r#"{"name": "Bob"}"#),
            None,
        ]);

        // Test TryFrom<&LargeStringArray>
        let json_array = JsonArray::try_from(&large_string_array).unwrap();
        assert_eq!(json_array.len(), 3);
        assert!(!json_array.is_null(0));
        assert!(!json_array.is_null(1));
        assert!(json_array.is_null(2));

        // Test TryFrom<LargeStringArray> (owned)
        let large_string_array2 = LargeStringArray::from(vec![Some(r#"{"x": 1}"#)]);
        let json_array2 = JsonArray::try_from(large_string_array2).unwrap();
        assert_eq!(json_array2.len(), 1);
    }

    #[test]
    fn test_try_from_array_ref() {
        // Test with Utf8
        let string_array: ArrayRef = Arc::new(StringArray::from(vec![
            Some(r#"{"a": 1}"#),
            Some(r#"{"b": 2}"#),
        ]));
        let json_array = JsonArray::try_from(string_array).unwrap();
        assert_eq!(json_array.len(), 2);

        // Test with LargeUtf8
        let large_string_array: ArrayRef = Arc::new(LargeStringArray::from(vec![
            Some(r#"{"c": 3}"#),
            Some(r#"{"d": 4}"#),
        ]));
        let json_array2 = JsonArray::try_from(large_string_array).unwrap();
        assert_eq!(json_array2.len(), 2);

        // Test with unsupported type
        let int_array: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]));
        let result = JsonArray::try_from(int_array);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unsupported"));
    }

    #[test]
    fn test_arrow_json_to_lance_json_non_json_field() {
        // Test that non-JSON fields are returned unchanged
        let field = ArrowField::new("text", DataType::Utf8, true);
        let converted = arrow_json_to_lance_json(&field);
        assert_eq!(converted.data_type(), &DataType::Utf8);
        assert_eq!(converted.name(), "text");
    }

    #[test]
    fn test_convert_lance_json_to_arrow() {
        // Create a batch with Lance JSON column (JSONB)
        let json_array = JsonArray::try_from_iter(vec![
            Some(r#"{"name": "Alice"}"#),
            None::<&str>,
            Some(r#"{"name": "Bob"}"#),
        ])
        .unwrap();

        let lance_json_field = json_field("data", true);
        let schema = Arc::new(Schema::new(vec![lance_json_field]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(json_array.into_inner()) as ArrayRef])
                .unwrap();

        // Convert back to Arrow JSON
        let converted = convert_lance_json_to_arrow(&batch).unwrap();

        // Check schema
        let converted_schema = converted.schema();
        let converted_field = converted_schema.field(0);
        assert_eq!(converted_field.data_type(), &DataType::Utf8);
        assert_eq!(
            converted_field.metadata().get(ARROW_EXT_NAME_KEY),
            Some(&ARROW_JSON_EXT_NAME.to_string())
        );

        // Check data
        let string_array = converted
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(!string_array.is_null(0));
        assert!(string_array.is_null(1));
        assert!(!string_array.is_null(2));
        assert!(string_array.value(0).contains("Alice"));
        assert!(string_array.value(2).contains("Bob"));
    }

    #[test]
    fn test_convert_lance_json_to_arrow_empty_batch() {
        // Create an empty batch with Lance JSON column
        let lance_json_field = json_field("data", true);
        let schema = Arc::new(Schema::new(vec![lance_json_field]));
        let empty_binary = LargeBinaryBuilder::new().finish();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(empty_binary) as ArrayRef]).unwrap();

        // Convert back to Arrow JSON
        let converted = convert_lance_json_to_arrow(&batch).unwrap();
        assert_eq!(converted.num_rows(), 0);
        assert_eq!(converted.schema().field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_convert_lance_json_to_arrow_no_json_columns() {
        // Create a batch without JSON columns
        let field = ArrowField::new("text", DataType::Utf8, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let string_array = StringArray::from(vec![Some("hello"), Some("world")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(string_array) as ArrayRef]).unwrap();

        // Convert - should return the same batch
        let converted = convert_lance_json_to_arrow(&batch).unwrap();
        assert_eq!(converted.num_columns(), 1);
        assert_eq!(converted.schema().field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_convert_json_columns_empty_batch() {
        // Create an empty batch with Arrow JSON column
        let mut field = ArrowField::new("data", DataType::Utf8, false);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            ARROW_JSON_EXT_NAME.to_string(),
        );
        field.set_metadata(metadata);

        let schema = Arc::new(Schema::new(vec![field]));
        let empty_strings = arrow_array::builder::StringBuilder::new().finish();
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(empty_strings) as ArrayRef]).unwrap();

        let converted = convert_json_columns(&batch).unwrap();
        assert_eq!(converted.num_rows(), 0);
        assert_eq!(
            converted.schema().field(0).data_type(),
            &DataType::LargeBinary
        );
    }

    #[test]
    fn test_convert_json_columns_large_string() {
        // Create a batch with Arrow JSON column using LargeUtf8
        let json_strings = LargeStringArray::from(vec![
            Some(r#"{"name": "Alice"}"#),
            Some(r#"{"name": "Bob"}"#),
        ]);

        let mut field = ArrowField::new("data", DataType::LargeUtf8, false);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            ARROW_JSON_EXT_NAME.to_string(),
        );
        field.set_metadata(metadata);

        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(json_strings) as ArrayRef]).unwrap();

        let converted = convert_json_columns(&batch).unwrap();
        assert_eq!(converted.num_columns(), 1);
        assert_eq!(
            converted.schema().field(0).data_type(),
            &DataType::LargeBinary
        );
        assert_eq!(converted.num_rows(), 2);
    }

    #[test]
    fn test_convert_json_columns_no_json_columns() {
        // Create a batch without JSON columns
        let field = ArrowField::new("text", DataType::Utf8, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let string_array = StringArray::from(vec![Some("hello"), Some("world")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(string_array) as ArrayRef]).unwrap();

        // Convert - should return the same batch
        let converted = convert_json_columns(&batch).unwrap();
        assert_eq!(converted.num_columns(), 1);
        assert_eq!(converted.schema().field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_convert_json_columns_mixed_columns() {
        // Create a batch with both JSON and non-JSON columns
        let json_strings = StringArray::from(vec![
            Some(r#"{"name": "Alice"}"#),
            Some(r#"{"name": "Bob"}"#),
        ]);
        let text_strings = StringArray::from(vec![Some("hello"), Some("world")]);

        let mut json_field = ArrowField::new("json_data", DataType::Utf8, false);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            ARROW_JSON_EXT_NAME.to_string(),
        );
        json_field.set_metadata(metadata);

        let text_field = ArrowField::new("text_data", DataType::Utf8, true);

        let schema = Arc::new(Schema::new(vec![json_field, text_field]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(json_strings) as ArrayRef,
                Arc::new(text_strings) as ArrayRef,
            ],
        )
        .unwrap();

        let converted = convert_json_columns(&batch).unwrap();
        assert_eq!(converted.num_columns(), 2);
        assert_eq!(
            converted.schema().field(0).data_type(),
            &DataType::LargeBinary
        );
        assert_eq!(converted.schema().field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_is_arrow_json_field_large_utf8() {
        // Test with LargeUtf8 storage type
        let mut field = ArrowField::new("data", DataType::LargeUtf8, true);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            ARROW_JSON_EXT_NAME.to_string(),
        );
        field.set_metadata(metadata);

        assert!(is_arrow_json_field(&field));
    }

    #[test]
    fn test_encode_json_invalid() {
        // Test encoding invalid JSON
        let result = encode_json("not valid json {");
        assert!(result.is_err());
    }

    #[test]
    fn test_json_array_from_invalid_json() {
        // Test creating JsonArray from invalid JSON strings
        let result = JsonArray::try_from_iter(vec![Some("invalid json {")]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to encode"));
    }

    #[test]
    fn test_try_from_string_array_invalid_json() {
        let string_array = StringArray::from(vec![Some("invalid json {")]);
        let result = JsonArray::try_from(string_array);
        assert!(result.is_err());
    }

    #[test]
    fn test_try_from_large_string_array_invalid_json() {
        let large_string_array = LargeStringArray::from(vec![Some("invalid json {")]);
        let result = JsonArray::try_from(large_string_array);
        assert!(result.is_err());
    }

    #[test]
    fn test_convert_lance_json_to_arrow_mixed_columns() {
        // Create a batch with both JSON and non-JSON columns
        let json_array = JsonArray::try_from_iter(vec![
            Some(r#"{"name": "Alice"}"#),
            Some(r#"{"name": "Bob"}"#),
        ])
        .unwrap();
        let text_strings = StringArray::from(vec![Some("hello"), Some("world")]);

        let json_f = json_field("json_data", true);
        let text_field = ArrowField::new("text_data", DataType::Utf8, true);

        let schema = Arc::new(Schema::new(vec![json_f, text_field]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(json_array.into_inner()) as ArrayRef,
                Arc::new(text_strings) as ArrayRef,
            ],
        )
        .unwrap();

        let converted = convert_lance_json_to_arrow(&batch).unwrap();
        assert_eq!(converted.num_columns(), 2);
        assert_eq!(converted.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(converted.schema().field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_json_path_invalid_path() {
        let json_array = JsonArray::try_from_iter(vec![Some(r#"{"a": 1}"#)]).unwrap();
        // Invalid JSONPath syntax should return error
        let result = json_array.json_path(0, "invalid path without $");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Failed to extract JSONPath")
        );
    }

    #[test]
    fn test_convert_json_columns_invalid_storage_type() {
        // Create a batch with Arrow JSON field but wrong storage type (Int32 instead of Utf8)
        let int_array = arrow_array::Int32Array::from(vec![1, 2, 3]);

        let mut field = ArrowField::new("data", DataType::Int32, false);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            ARROW_JSON_EXT_NAME.to_string(),
        );
        field.set_metadata(metadata);

        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(int_array) as ArrayRef]).unwrap();

        // This should succeed since Int32 doesn't match is_arrow_json_field check
        // (is_arrow_json_field requires Utf8 or LargeUtf8)
        let result = convert_json_columns(&batch);
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_json_field_wrong_extension() {
        // LargeBinary field without the correct extension metadata
        let field = ArrowField::new("data", DataType::LargeBinary, true);
        assert!(!is_json_field(&field));

        // LargeBinary field with wrong extension name
        let mut field2 = ArrowField::new("data", DataType::LargeBinary, true);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            "other.extension".to_string(),
        );
        field2.set_metadata(metadata);
        assert!(!is_json_field(&field2));
    }

    #[test]
    fn test_is_arrow_json_field_wrong_extension() {
        // Utf8 field without extension metadata
        let field = ArrowField::new("data", DataType::Utf8, true);
        assert!(!is_arrow_json_field(&field));

        // Utf8 field with wrong extension name
        let mut field2 = ArrowField::new("data", DataType::Utf8, true);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            "other.extension".to_string(),
        );
        field2.set_metadata(metadata);
        assert!(!is_arrow_json_field(&field2));

        // Wrong type entirely
        let field3 = ArrowField::new("data", DataType::Int32, true);
        assert!(!is_arrow_json_field(&field3));
    }

    #[test]
    fn test_convert_json_columns_invalid_json_utf8() {
        // Test error propagation when converting invalid JSON (Utf8)
        let invalid_json = StringArray::from(vec![Some("invalid json {")]);

        let mut field = ArrowField::new("data", DataType::Utf8, false);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            ARROW_JSON_EXT_NAME.to_string(),
        );
        field.set_metadata(metadata);

        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(invalid_json) as ArrayRef]).unwrap();

        let result = convert_json_columns(&batch);
        assert!(result.is_err());
    }

    #[test]
    fn test_convert_json_columns_invalid_json_large_utf8() {
        // Test error propagation when converting invalid JSON (LargeUtf8)
        let invalid_json = LargeStringArray::from(vec![Some("invalid json {")]);

        let mut field = ArrowField::new("data", DataType::LargeUtf8, false);
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            ARROW_JSON_EXT_NAME.to_string(),
        );
        field.set_metadata(metadata);

        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(invalid_json) as ArrayRef]).unwrap();

        let result = convert_json_columns(&batch);
        assert!(result.is_err());
    }

    #[test]
    fn test_json_path_on_corrupted_jsonb() {
        // Create corrupted JSONB bytes directly
        let corrupted_bytes: &[u8] = &[0xFF, 0xFE, 0x00, 0x01, 0x02];
        let corrupted_binary = LargeBinaryArray::from(vec![Some(corrupted_bytes)]);

        // Wrap in JsonArray
        let corrupted_json = JsonArray {
            inner: corrupted_binary,
        };

        // Try to use json_path on corrupted data - the selector might fail or return unexpected results
        // This exercises the code path but may not produce an error depending on jsonb library behavior
        let _result = corrupted_json.json_path(0, "$.a");
        // We don't assert on the result as the behavior depends on the jsonb library
    }

    #[test]
    fn test_decode_json_on_various_inputs() {
        // Test decode_json with various inputs
        let valid_jsonb = encode_json(r#"{"key": "value"}"#).unwrap();
        let decoded = decode_json(&valid_jsonb);
        assert!(decoded.contains("key"));

        // Empty bytes - jsonb library handles this gracefully
        let decoded_empty = decode_json(&[]);
        // Just verify it doesn't panic
        let _ = decoded_empty;

        // Random bytes - jsonb library handles this gracefully
        let decoded_random = decode_json(&[0xFF, 0xFE, 0x00]);
        // Just verify it doesn't panic
        let _ = decoded_random;
    }
}
