// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Schema conversion utilities for Lance Namespace.
//!
//! This module provides functions to convert between JsonArrow schema representations
//! and Arrow schema types.

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use lance_core::{Error, Result};
use lance_namespace_reqwest_client::models::{JsonArrowDataType, JsonArrowField, JsonArrowSchema};

/// Convert Arrow Schema to JsonArrowSchema
pub fn arrow_schema_to_json(arrow_schema: &ArrowSchema) -> Result<JsonArrowSchema> {
    let fields: Result<Vec<JsonArrowField>> = arrow_schema
        .fields()
        .iter()
        .map(|f| arrow_field_to_json(f.as_ref()))
        .collect();

    let metadata = if arrow_schema.metadata().is_empty() {
        None
    } else {
        Some(arrow_schema.metadata().clone())
    };

    Ok(JsonArrowSchema {
        fields: fields?,
        metadata,
    })
}

/// Convert Arrow Field to JsonArrowField
fn arrow_field_to_json(arrow_field: &Field) -> Result<JsonArrowField> {
    let data_type = arrow_type_to_json(arrow_field.data_type())?;

    Ok(JsonArrowField {
        name: arrow_field.name().clone(),
        nullable: arrow_field.is_nullable(),
        r#type: Box::new(data_type),
        metadata: if arrow_field.metadata().is_empty() {
            None
        } else {
            Some(arrow_field.metadata().clone())
        },
    })
}

/// Convert Arrow DataType to JsonArrowDataType
fn arrow_type_to_json(data_type: &DataType) -> Result<JsonArrowDataType> {
    match data_type {
        // Primitive types
        DataType::Null => Ok(JsonArrowDataType::new("null".to_string())),
        DataType::Boolean => Ok(JsonArrowDataType::new("bool".to_string())),
        DataType::Int8 => Ok(JsonArrowDataType::new("int8".to_string())),
        DataType::UInt8 => Ok(JsonArrowDataType::new("uint8".to_string())),
        DataType::Int16 => Ok(JsonArrowDataType::new("int16".to_string())),
        DataType::UInt16 => Ok(JsonArrowDataType::new("uint16".to_string())),
        DataType::Int32 => Ok(JsonArrowDataType::new("int32".to_string())),
        DataType::UInt32 => Ok(JsonArrowDataType::new("uint32".to_string())),
        DataType::Int64 => Ok(JsonArrowDataType::new("int64".to_string())),
        DataType::UInt64 => Ok(JsonArrowDataType::new("uint64".to_string())),
        DataType::Float16 => Ok(JsonArrowDataType::new("float16".to_string())),
        DataType::Float32 => Ok(JsonArrowDataType::new("float32".to_string())),
        DataType::Float64 => Ok(JsonArrowDataType::new("float64".to_string())),
        DataType::Decimal32(precision, scale) => {
            let mut dt = JsonArrowDataType::new("decimal32".to_string());
            dt.length = Some(*precision as i64 * 1000 + *scale as i64); // Encode precision and scale
            Ok(dt)
        }
        DataType::Decimal64(precision, scale) => {
            let mut dt = JsonArrowDataType::new("decimal64".to_string());
            dt.length = Some(*precision as i64 * 1000 + *scale as i64); // Encode precision and scale
            Ok(dt)
        }
        DataType::Decimal128(precision, scale) => {
            let mut dt = JsonArrowDataType::new("decimal128".to_string());
            dt.length = Some(*precision as i64 * 1000 + *scale as i64); // Encode precision and scale
            Ok(dt)
        }
        DataType::Decimal256(precision, scale) => {
            let mut dt = JsonArrowDataType::new("decimal256".to_string());
            dt.length = Some(*precision as i64 * 1000 + *scale as i64); // Encode precision and scale
            Ok(dt)
        }
        DataType::Date32 => Ok(JsonArrowDataType::new("date32".to_string())),
        DataType::Date64 => Ok(JsonArrowDataType::new("date64".to_string())),
        DataType::Time32(_) => Ok(JsonArrowDataType::new("time32".to_string())),
        DataType::Time64(_) => Ok(JsonArrowDataType::new("time64".to_string())),
        DataType::Timestamp(_, _tz) => {
            // TODO: We could encode timezone info if needed
            Ok(JsonArrowDataType::new("timestamp".to_string()))
        }
        DataType::Duration(_) => Ok(JsonArrowDataType::new("duration".to_string())),
        DataType::Interval(_) => Ok(JsonArrowDataType::new("interval".to_string())),

        // String and Binary types
        DataType::Utf8 => Ok(JsonArrowDataType::new("utf8".to_string())),
        DataType::LargeUtf8 => Ok(JsonArrowDataType::new("large_utf8".to_string())),
        DataType::Binary => Ok(JsonArrowDataType::new("binary".to_string())),
        DataType::LargeBinary => Ok(JsonArrowDataType::new("large_binary".to_string())),
        DataType::FixedSizeBinary(size) => {
            let mut dt = JsonArrowDataType::new("fixed_size_binary".to_string());
            dt.length = Some(*size as i64);
            Ok(dt)
        }

        // Nested types
        DataType::List(field) => {
            let inner_type = arrow_type_to_json(field.data_type())?;
            let inner_field = JsonArrowField {
                name: field.name().clone(),
                nullable: field.is_nullable(),
                r#type: Box::new(inner_type),
                metadata: if field.metadata().is_empty() {
                    None
                } else {
                    Some(field.metadata().clone())
                },
            };
            Ok(JsonArrowDataType {
                r#type: "list".to_string(),
                fields: Some(vec![inner_field]),
                length: None,
            })
        }
        DataType::LargeList(field) => {
            let inner_type = arrow_type_to_json(field.data_type())?;
            let inner_field = JsonArrowField {
                name: field.name().clone(),
                nullable: field.is_nullable(),
                r#type: Box::new(inner_type),
                metadata: if field.metadata().is_empty() {
                    None
                } else {
                    Some(field.metadata().clone())
                },
            };
            Ok(JsonArrowDataType {
                r#type: "large_list".to_string(),
                fields: Some(vec![inner_field]),
                length: None,
            })
        }
        DataType::FixedSizeList(field, size) => {
            let inner_type = arrow_type_to_json(field.data_type())?;
            let inner_field = JsonArrowField {
                name: field.name().clone(),
                nullable: field.is_nullable(),
                r#type: Box::new(inner_type),
                metadata: if field.metadata().is_empty() {
                    None
                } else {
                    Some(field.metadata().clone())
                },
            };
            Ok(JsonArrowDataType {
                r#type: "fixed_size_list".to_string(),
                fields: Some(vec![inner_field]),
                length: Some(*size as i64),
            })
        }
        DataType::Struct(fields) => {
            let json_fields: Result<Vec<JsonArrowField>> = fields
                .iter()
                .map(|f| arrow_field_to_json(f.as_ref()))
                .collect();
            Ok(JsonArrowDataType {
                r#type: "struct".to_string(),
                fields: Some(json_fields?),
                length: None,
            })
        }
        DataType::Union(_, _) => {
            // Union types are complex, for now we'll skip detailed conversion
            Ok(JsonArrowDataType::new("union".to_string()))
        }
        DataType::Dictionary(_, value_type) => {
            // For dictionary, return the value type
            arrow_type_to_json(value_type)
        }

        DataType::Map(entries_field, keys_sorted) => {
            if *keys_sorted {
                return Err(Error::namespace(format!(
                    "Map types with keys_sorted=true are not yet supported for JSON conversion: {:?}",
                    data_type
                )));
            }
            let inner_type = arrow_type_to_json(entries_field.data_type())?;
            let inner_field = JsonArrowField {
                name: entries_field.name().clone(),
                nullable: entries_field.is_nullable(),
                r#type: Box::new(inner_type),
                metadata: if entries_field.metadata().is_empty() {
                    None
                } else {
                    Some(entries_field.metadata().clone())
                },
            };
            Ok(JsonArrowDataType {
                r#type: "map".to_string(),
                fields: Some(vec![inner_field]),
                length: None,
            })
        }

        // Unsupported types
        DataType::RunEndEncoded(_, _) => Err(Error::namespace(format!(
            "RunEndEncoded type is not yet supported for JSON conversion: {:?}",
            data_type
        ))),
        DataType::ListView(_) | DataType::LargeListView(_) => Err(Error::namespace(format!(
            "ListView types are not yet supported for JSON conversion: {:?}",
            data_type
        ))),
        DataType::Utf8View | DataType::BinaryView => Err(Error::namespace(format!(
            "View types are not yet supported for JSON conversion: {:?}",
            data_type
        ))),
    }
}

/// Convert JsonArrowSchema to Arrow Schema
pub fn convert_json_arrow_schema(json_schema: &JsonArrowSchema) -> Result<ArrowSchema> {
    let fields: Result<Vec<Field>> = json_schema
        .fields
        .iter()
        .map(convert_json_arrow_field)
        .collect();

    let metadata = json_schema.metadata.as_ref().cloned().unwrap_or_default();

    Ok(ArrowSchema::new_with_metadata(fields?, metadata))
}

/// Convert JsonArrowField to Arrow Field
pub fn convert_json_arrow_field(json_field: &JsonArrowField) -> Result<Field> {
    let data_type = convert_json_arrow_type(&json_field.r#type)?;
    let nullable = json_field.nullable;

    let field = Field::new(&json_field.name, data_type, nullable);
    Ok(match json_field.metadata.as_ref() {
        Some(metadata) => field.with_metadata(metadata.clone()),
        None => field,
    })
}

/// Convert JsonArrowDataType to Arrow DataType
pub fn convert_json_arrow_type(json_type: &JsonArrowDataType) -> Result<DataType> {
    use std::sync::Arc;

    let type_name = json_type.r#type.to_lowercase();

    match type_name.as_str() {
        // Primitive types
        "null" => Ok(DataType::Null),
        "bool" | "boolean" => Ok(DataType::Boolean),
        "int8" => Ok(DataType::Int8),
        "uint8" => Ok(DataType::UInt8),
        "int16" => Ok(DataType::Int16),
        "uint16" => Ok(DataType::UInt16),
        "int32" => Ok(DataType::Int32),
        "uint32" => Ok(DataType::UInt32),
        "int64" => Ok(DataType::Int64),
        "uint64" => Ok(DataType::UInt64),
        "float16" => Ok(DataType::Float16),
        "float32" => Ok(DataType::Float32),
        "float64" => Ok(DataType::Float64),

        // Decimal types - encoding: precision * 1000 + scale
        // Decoding must handle negative scale: precision = ((encoded + 128) / 1000)
        "decimal32" => {
            let encoded = json_type.length.unwrap_or(0);
            let precision = ((encoded + 128) / 1000) as u8;
            let scale = (encoded - precision as i64 * 1000) as i8;
            Ok(DataType::Decimal32(precision, scale))
        }
        "decimal64" => {
            let encoded = json_type.length.unwrap_or(0);
            let precision = ((encoded + 128) / 1000) as u8;
            let scale = (encoded - precision as i64 * 1000) as i8;
            Ok(DataType::Decimal64(precision, scale))
        }
        "decimal128" => {
            let encoded = json_type.length.unwrap_or(0);
            let precision = ((encoded + 128) / 1000) as u8;
            let scale = (encoded - precision as i64 * 1000) as i8;
            Ok(DataType::Decimal128(precision, scale))
        }
        "decimal256" => {
            let encoded = json_type.length.unwrap_or(0);
            let precision = ((encoded + 128) / 1000) as u8;
            let scale = (encoded - precision as i64 * 1000) as i8;
            Ok(DataType::Decimal256(precision, scale))
        }

        // Date/Time types
        "date32" => Ok(DataType::Date32),
        "date64" => Ok(DataType::Date64),
        "timestamp" => Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        )),
        "duration" => Ok(DataType::Duration(arrow::datatypes::TimeUnit::Microsecond)),

        // String and Binary types
        "utf8" => Ok(DataType::Utf8),
        "large_utf8" => Ok(DataType::LargeUtf8),
        "binary" => Ok(DataType::Binary),
        "large_binary" => Ok(DataType::LargeBinary),
        "fixed_size_binary" => {
            let size = json_type.length.unwrap_or(0) as i32;
            Ok(DataType::FixedSizeBinary(size))
        }

        // Nested types
        "list" => {
            let inner = json_type
                .fields
                .as_ref()
                .and_then(|f| f.first())
                .ok_or_else(|| Error::namespace("list type missing inner field"))?;
            Ok(DataType::List(Arc::new(convert_json_arrow_field(inner)?)))
        }
        "large_list" => {
            let inner = json_type
                .fields
                .as_ref()
                .and_then(|f| f.first())
                .ok_or_else(|| Error::namespace("large_list type missing inner field"))?;
            Ok(DataType::LargeList(Arc::new(convert_json_arrow_field(
                inner,
            )?)))
        }
        "fixed_size_list" => {
            let inner = json_type
                .fields
                .as_ref()
                .and_then(|f| f.first())
                .ok_or_else(|| Error::namespace("fixed_size_list type missing inner field"))?;
            let size = json_type.length.unwrap_or(0) as i32;
            Ok(DataType::FixedSizeList(
                Arc::new(convert_json_arrow_field(inner)?),
                size,
            ))
        }
        "struct" => {
            let fields = json_type
                .fields
                .as_ref()
                .ok_or_else(|| Error::namespace("struct type missing fields"))?;
            let arrow_fields: Result<Vec<Field>> =
                fields.iter().map(convert_json_arrow_field).collect();
            Ok(DataType::Struct(arrow_fields?.into()))
        }
        "map" => {
            let entries = json_type
                .fields
                .as_ref()
                .and_then(|f| f.first())
                .ok_or_else(|| Error::namespace("map type missing entries field"))?;
            Ok(DataType::Map(
                Arc::new(convert_json_arrow_field(entries)?),
                false,
            ))
        }

        _ => Err(Error::namespace(format!(
            "Unsupported Arrow type: {}",
            type_name
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_extension_metadata_preserved_in_json_roundtrip() {
        const ARROW_EXT_NAME_KEY: &str = "ARROW:extension:name";
        const LANCE_JSON_EXT_NAME: &str = "lance.json";

        let meta_field =
            Field::new("meta", DataType::Binary, true).with_metadata(HashMap::from([(
                ARROW_EXT_NAME_KEY.to_string(),
                LANCE_JSON_EXT_NAME.to_string(),
            )]));
        let arrow_schema =
            ArrowSchema::new(vec![Field::new("id", DataType::Int32, false), meta_field]);

        let json_schema = arrow_schema_to_json(&arrow_schema).unwrap();
        let meta_json_field = json_schema
            .fields
            .iter()
            .find(|f| f.name == "meta")
            .unwrap();
        assert!(
            meta_json_field
                .metadata
                .as_ref()
                .unwrap()
                .contains_key(ARROW_EXT_NAME_KEY)
        );

        let roundtrip = convert_json_arrow_schema(&json_schema).unwrap();
        let meta_field = roundtrip.field_with_name("meta").unwrap();
        assert_eq!(
            meta_field.metadata().get(ARROW_EXT_NAME_KEY),
            Some(&LANCE_JSON_EXT_NAME.to_string())
        );
    }

    #[test]
    fn test_convert_basic_types() {
        // Test int32
        let int_type = JsonArrowDataType::new("int32".to_string());
        let result = convert_json_arrow_type(&int_type).unwrap();
        assert_eq!(result, DataType::Int32);

        // Test utf8
        let string_type = JsonArrowDataType::new("utf8".to_string());
        let result = convert_json_arrow_type(&string_type).unwrap();
        assert_eq!(result, DataType::Utf8);

        // Test float64
        let float_type = JsonArrowDataType::new("float64".to_string());
        let result = convert_json_arrow_type(&float_type).unwrap();
        assert_eq!(result, DataType::Float64);

        // Test binary
        let binary_type = JsonArrowDataType::new("binary".to_string());
        let result = convert_json_arrow_type(&binary_type).unwrap();
        assert_eq!(result, DataType::Binary);
    }

    #[test]
    fn test_convert_field() {
        let int_type = JsonArrowDataType::new("int32".to_string());
        let field = JsonArrowField {
            name: "test_field".to_string(),
            r#type: Box::new(int_type),
            nullable: false,
            metadata: None,
        };

        let result = convert_json_arrow_field(&field).unwrap();
        assert_eq!(result.name(), "test_field");
        assert_eq!(result.data_type(), &DataType::Int32);
        assert!(!result.is_nullable());
    }

    #[test]
    fn test_convert_schema() {
        let int_type = JsonArrowDataType::new("int32".to_string());
        let string_type = JsonArrowDataType::new("utf8".to_string());

        let id_field = JsonArrowField {
            name: "id".to_string(),
            r#type: Box::new(int_type),
            nullable: false,
            metadata: None,
        };

        let name_field = JsonArrowField {
            name: "name".to_string(),
            r#type: Box::new(string_type),
            nullable: true,
            metadata: None,
        };

        let mut metadata = HashMap::new();
        metadata.insert("key".to_string(), "value".to_string());

        let schema = JsonArrowSchema {
            fields: vec![id_field, name_field],
            metadata: Some(metadata.clone()),
        };

        let result = convert_json_arrow_schema(&schema).unwrap();
        assert_eq!(result.fields().len(), 2);
        assert_eq!(result.field(0).name(), "id");
        assert_eq!(result.field(1).name(), "name");
        assert_eq!(result.metadata(), &metadata);
    }

    #[test]
    fn test_unsupported_type() {
        let unsupported_type = JsonArrowDataType::new("unsupported".to_string());
        let result = convert_json_arrow_type(&unsupported_type);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unsupported Arrow type")
        );
    }

    #[test]
    fn test_list_type() {
        use arrow::datatypes::Field;

        let inner_field = Field::new("item", DataType::Int32, true);
        let list_type = DataType::List(Arc::new(inner_field));

        let result = arrow_type_to_json(&list_type).unwrap();
        assert_eq!(result.r#type, "list");
        assert!(result.fields.is_some());
        let fields = result.fields.unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "item");
        assert_eq!(fields[0].r#type.r#type, "int32");
    }

    #[test]
    fn test_struct_type() {
        use arrow::datatypes::Field;

        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ];
        let struct_type = DataType::Struct(fields.into());

        let result = arrow_type_to_json(&struct_type).unwrap();
        assert_eq!(result.r#type, "struct");
        assert!(result.fields.is_some());
        let json_fields = result.fields.unwrap();
        assert_eq!(json_fields.len(), 2);
        assert_eq!(json_fields[0].name, "id");
        assert_eq!(json_fields[0].r#type.r#type, "int64");
        assert_eq!(json_fields[1].name, "name");
        assert_eq!(json_fields[1].r#type.r#type, "utf8");
    }

    #[test]
    fn test_fixed_size_list_type() {
        use arrow::datatypes::Field;

        let inner_field = Field::new("item", DataType::Float32, false);
        let fixed_list_type = DataType::FixedSizeList(Arc::new(inner_field), 3);

        let result = arrow_type_to_json(&fixed_list_type).unwrap();
        assert_eq!(result.r#type, "fixed_size_list");
        assert_eq!(result.length, Some(3));
        assert!(result.fields.is_some());
        let fields = result.fields.unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].r#type.r#type, "float32");
    }

    #[test]
    fn test_nested_struct_with_list() {
        use arrow::datatypes::Field;

        let inner_list_field = Field::new("item", DataType::Utf8, true);
        let list_type = DataType::List(Arc::new(inner_list_field));

        let struct_fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("tags", list_type, true),
        ];
        let struct_type = DataType::Struct(struct_fields.into());

        let result = arrow_type_to_json(&struct_type).unwrap();
        assert_eq!(result.r#type, "struct");
        let json_fields = result.fields.unwrap();
        assert_eq!(json_fields.len(), 2);
        assert_eq!(json_fields[0].name, "id");
        assert_eq!(json_fields[1].name, "tags");
        assert_eq!(json_fields[1].r#type.r#type, "list");

        // Check nested list structure
        let list_fields = json_fields[1].r#type.fields.as_ref().unwrap();
        assert_eq!(list_fields.len(), 1);
        assert_eq!(list_fields[0].r#type.r#type, "utf8");
    }

    #[test]
    fn test_map_type_supported() {
        use arrow::datatypes::Field;

        let key_field = Field::new("keys", DataType::Utf8, false);
        let value_field = Field::new("values", DataType::Int32, true);
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(vec![key_field, value_field].into()),
                false,
            )),
            false,
        );

        let result = arrow_type_to_json(&map_type);
        assert!(result.is_ok());
        let json_type = result.unwrap();
        assert_eq!(json_type.r#type, "map");
        assert!(json_type.fields.is_some());

        let fields = json_type.fields.unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "entries");
        assert_eq!(fields[0].r#type.r#type, "struct");
    }

    #[test]
    fn test_additional_types() {
        // Test Date types
        let date32 = arrow_type_to_json(&DataType::Date32).unwrap();
        assert_eq!(date32.r#type, "date32");

        let date64 = arrow_type_to_json(&DataType::Date64).unwrap();
        assert_eq!(date64.r#type, "date64");

        // Test FixedSizeBinary
        let fixed_binary = arrow_type_to_json(&DataType::FixedSizeBinary(16)).unwrap();
        assert_eq!(fixed_binary.r#type, "fixed_size_binary");
        assert_eq!(fixed_binary.length, Some(16));

        // Test Float16
        let float16 = arrow_type_to_json(&DataType::Float16).unwrap();
        assert_eq!(float16.r#type, "float16");
    }

    /// Verify that convert_json_arrow_type (deserialization) is the inverse of
    /// arrow_type_to_json (serialization) for all supported types.
    #[test]
    fn test_json_arrow_type_roundtrip() {
        use arrow::datatypes::Field;

        let cases: Vec<DataType> = vec![
            // Scalars
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::UInt8,
            DataType::Int16,
            DataType::UInt16,
            DataType::Int32,
            DataType::UInt32,
            DataType::Int64,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::Date32,
            DataType::Date64,
            DataType::FixedSizeBinary(16),
            // Decimal types with positive and negative scales
            DataType::Decimal32(10, -2),
            DataType::Decimal32(9, 3),
            DataType::Decimal64(18, -5),
            DataType::Decimal64(10, 4),
            DataType::Decimal128(9, -2),
            DataType::Decimal128(38, 10),
            DataType::Decimal256(38, 10),
            DataType::Decimal256(76, -10),
            // Timestamp and Duration
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            DataType::Duration(arrow::datatypes::TimeUnit::Microsecond),
            // Nested
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 128),
            DataType::Struct(
                vec![
                    Field::new("a", DataType::Int64, false),
                    Field::new("b", DataType::Utf8, true),
                ]
                .into(),
            ),
            // Map
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("keys", DataType::Utf8, false),
                            Field::new("values", DataType::Int32, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        ];

        for dt in &cases {
            let json = arrow_type_to_json(dt)
                .unwrap_or_else(|e| panic!("arrow_type_to_json failed for {:?}: {}", dt, e));
            let back = convert_json_arrow_type(&json)
                .unwrap_or_else(|e| panic!("convert_json_arrow_type failed for {:?}: {}", dt, e));
            assert_eq!(&back, dt, "Roundtrip mismatch for {:?}: got {:?}", dt, back);
        }
    }

    #[test]
    fn test_decimal_negative_scale_roundtrip() {
        // Explicitly test the cases requested by reviewer
        let cases = vec![
            DataType::Decimal32(10, -2),
            DataType::Decimal128(9, -2),
            DataType::Decimal256(38, 10),
        ];
        for dt in &cases {
            let json = arrow_type_to_json(dt).unwrap();
            let back = convert_json_arrow_type(&json).unwrap();
            assert_eq!(&back, dt, "Decimal roundtrip failed for {:?}", dt);
        }
    }

    #[test]
    fn test_schema_with_metadata_roundtrip() {
        let mut metadata = HashMap::new();
        metadata.insert("key1".to_string(), "value1".to_string());
        metadata.insert("key2".to_string(), "value2".to_string());

        let arrow_schema = ArrowSchema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ],
            metadata.clone(),
        );

        let json_schema = arrow_schema_to_json(&arrow_schema).unwrap();
        assert_eq!(json_schema.metadata.as_ref().unwrap(), &metadata);

        let roundtrip = convert_json_arrow_schema(&json_schema).unwrap();
        assert_eq!(roundtrip.metadata(), &metadata);
    }

    #[test]
    fn test_dictionary_type_unwraps_to_value_type() {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let json = arrow_type_to_json(&dict_type).unwrap();
        assert_eq!(json.r#type, "utf8");
    }

    #[test]
    fn test_map_keys_sorted_unsupported() {
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Int32, true),
                    ]
                    .into(),
                ),
                false,
            )),
            true, // keys_sorted = true
        );
        let result = arrow_type_to_json(&map_type);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("keys_sorted=true"));
    }

    #[test]
    fn test_unsupported_types_error() {
        // RunEndEncoded
        let ree = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        );
        assert!(arrow_type_to_json(&ree).is_err());

        // ListView
        let lv = DataType::ListView(Arc::new(Field::new("item", DataType::Int32, true)));
        assert!(arrow_type_to_json(&lv).is_err());

        // LargeListView
        let llv = DataType::LargeListView(Arc::new(Field::new("item", DataType::Int32, true)));
        assert!(arrow_type_to_json(&llv).is_err());

        // Utf8View / BinaryView
        assert!(arrow_type_to_json(&DataType::Utf8View).is_err());
        assert!(arrow_type_to_json(&DataType::BinaryView).is_err());
    }

    #[test]
    fn test_large_list_roundtrip() {
        let inner_field = Field::new("item", DataType::Float64, true);
        let large_list = DataType::LargeList(Arc::new(inner_field));

        let json = arrow_type_to_json(&large_list).unwrap();
        assert_eq!(json.r#type, "large_list");

        let back = convert_json_arrow_type(&json).unwrap();
        assert_eq!(back, large_list);
    }

    #[test]
    fn test_field_with_metadata_roundtrip() {
        let mut field_meta = HashMap::new();
        field_meta.insert("custom_key".to_string(), "custom_val".to_string());

        let field = Field::new("col", DataType::Int64, false).with_metadata(field_meta.clone());
        let schema = ArrowSchema::new(vec![field]);

        let json_schema = arrow_schema_to_json(&schema).unwrap();
        let roundtrip = convert_json_arrow_schema(&json_schema).unwrap();
        assert_eq!(roundtrip.field(0).metadata(), &field_meta);
    }

    #[test]
    fn test_nested_list_with_field_metadata() {
        let mut meta = HashMap::new();
        meta.insert("encoding".to_string(), "delta".to_string());

        let inner = Field::new("item", DataType::Int32, true).with_metadata(meta.clone());
        let list_type = DataType::List(Arc::new(inner));

        let json = arrow_type_to_json(&list_type).unwrap();
        let fields = json.fields.as_ref().unwrap();
        assert_eq!(fields[0].metadata.as_ref().unwrap(), &meta);
    }
}
