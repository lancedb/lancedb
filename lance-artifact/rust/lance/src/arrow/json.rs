// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Serialization and deserialization of Arrow Schema to JSON.
//!
//! This serialization is a convenience utility.  It is not intended to be a standard
//! serialization format for Arrow.  No guarantees are made about the stability of the
//! serialization format.  Use at your own risk.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use crate::datatypes::LogicalType;
use lance_core::error::{Error, Result};

/// JSON representation of an Apache Arrow [DataType].
#[derive(Serialize, Deserialize, Debug)]
pub struct JsonDataType {
    #[serde(rename = "type")]
    type_: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    fields: Option<Vec<JsonField>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    length: Option<usize>,
}

impl JsonDataType {
    fn try_new(dt: &DataType) -> Result<Self> {
        dt.try_into()
    }
}

impl TryFrom<&DataType> for JsonDataType {
    type Error = Error;

    fn try_from(dt: &DataType) -> Result<Self> {
        let (type_name, fields) = match dt {
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Utf8
            | DataType::Binary
            | DataType::LargeUtf8
            | DataType::LargeBinary
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Dictionary(_, _) => {
                let logical_type: LogicalType = dt.try_into()?;
                (logical_type.to_string(), None)
            }
            DataType::List(f) => {
                let fields = vec![JsonField::try_from(f.as_ref())?];
                ("list".to_string(), Some(fields))
            }
            DataType::LargeList(f) => {
                let fields = vec![JsonField::try_from(f.as_ref())?];
                ("large_list".to_string(), Some(fields))
            }
            DataType::FixedSizeList(f, len) => {
                let fields = vec![JsonField::try_from(f.as_ref())?];
                return Ok(Self {
                    type_: "fixed_size_list".to_string(),
                    fields: Some(fields),
                    length: Some(*len as usize),
                });
            }
            DataType::FixedSizeBinary(len) => {
                return Ok(Self {
                    type_: "fixed_size_binary".to_string(),
                    fields: None,
                    length: Some(*len as usize),
                });
            }
            DataType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|f| JsonField::try_from(f.as_ref()))
                    .collect::<Result<Vec<_>>>()?;
                ("struct".to_string(), Some(fields))
            }
            _ => {
                return Err(Error::arrow(format!(
                    "Json conversion: Unsupported type: {dt}"
                )));
            }
        };

        Ok(Self {
            type_: type_name,
            fields,
            length: None,
        })
    }
}

impl TryFrom<&JsonDataType> for DataType {
    type Error = Error;

    fn try_from(value: &JsonDataType) -> Result<Self> {
        let type_name = value.type_.as_str();
        match type_name {
            "null" | "bool" | "int8" | "int16" | "int32" | "int64" | "uint8" | "uint16"
            | "uint32" | "uint64" | "halffloat" | "float" | "double" | "string" | "binary"
            | "large_string" | "large_binary" | "date32:day" | "date64:ms" => {
                let logical_type: LogicalType = type_name.into();
                (&logical_type).try_into()
            }
            dt if dt.starts_with("time32:")
                || dt.starts_with("time64:")
                || dt.starts_with("timestamp:")
                || dt.starts_with("duration:")
                || dt.starts_with("dict:")
                || dt.starts_with("decimal:") =>
            {
                let logical_type: LogicalType = dt.into();
                (&logical_type).try_into()
            }
            "list" | "large_list" | "fixed_size_list" | "struct" => {
                let fields = value
                    .fields
                    .as_ref()
                    .ok_or_else(|| {
                        Error::arrow("Json conversion: List type requires a field".to_string())
                    })?
                    .iter()
                    .map(Field::try_from)
                    .collect::<Result<Vec<_>>>()?;

                match type_name {
                    "list" => Ok(Self::List(Arc::new(fields[0].clone()))),
                    "large_list" => Ok(Self::LargeList(Arc::new(fields[0].clone()))),
                    "fixed_size_list" => {
                        let length = value.length.ok_or_else(|| {
                            Error::arrow(
                                "Json conversion: FixedSizeList type requires a length".to_string(),
                            )
                        })?;
                        Ok(Self::FixedSizeList(
                            Arc::new(fields[0].clone()),
                            length as i32,
                        ))
                    }
                    "struct" => Ok(Self::Struct(fields.into())),
                    _ => unreachable!(),
                }
            }
            "fixed_size_binary" => {
                let length = value.length.ok_or_else(|| {
                    Error::arrow(
                        "Json conversion: FixedSizeBinary type requires a length".to_string(),
                    )
                })?;
                Ok(Self::FixedSizeBinary(length as i32))
            }
            _ => Err(Error::arrow(format!(
                "Json conversion: Unsupported type: {value:?}"
            ))),
        }
    }
}

/// JSON representation of an Apache Arrow [Field].
#[derive(Serialize, Deserialize, Debug)]
pub struct JsonField {
    name: String,
    #[serde(rename = "type")]
    type_: JsonDataType,
    nullable: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<HashMap<String, String>>,
}

impl TryFrom<&Field> for JsonField {
    type Error = Error;

    fn try_from(field: &Field) -> Result<Self> {
        let data_type = JsonDataType::try_new(field.data_type())?;

        let metadata = if field.metadata().is_empty() {
            None
        } else {
            Some(field.metadata().clone())
        };

        Ok(Self {
            name: field.name().clone(),
            nullable: field.is_nullable(),
            type_: data_type,
            metadata,
        })
    }
}

impl TryFrom<&JsonField> for Field {
    type Error = Error;

    fn try_from(value: &JsonField) -> Result<Self> {
        let data_type = DataType::try_from(&value.type_)?;
        let mut field = Self::new(&value.name, data_type, value.nullable);
        if let Some(metadata) = value.metadata.clone() {
            field.set_metadata(metadata);
        }
        Ok(field)
    }
}

/// JSON representation of a Apache Arrow [Schema].
#[derive(Serialize, Deserialize, Debug)]
pub struct JsonSchema {
    fields: Vec<JsonField>,

    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<HashMap<String, String>>,
}

/// Convert Schema to JSON representation.
impl TryFrom<&Schema> for JsonSchema {
    type Error = Error;

    fn try_from(schema: &Schema) -> Result<Self> {
        let fields = schema
            .fields()
            .iter()
            .map(|f| JsonField::try_from(f.as_ref()))
            .collect::<Result<Vec<_>>>()?;

        let metadata = if schema.metadata.is_empty() {
            None
        } else {
            Some(schema.metadata.clone())
        };
        Ok(Self { fields, metadata })
    }
}

impl TryFrom<JsonSchema> for Schema {
    type Error = Error;

    fn try_from(json_schema: JsonSchema) -> Result<Self> {
        Self::try_from(&json_schema)
    }
}

impl TryFrom<&JsonSchema> for Schema {
    type Error = Error;

    fn try_from(json_schema: &JsonSchema) -> Result<Self> {
        let fields = json_schema
            .fields
            .iter()
            .map(Field::try_from)
            .collect::<Result<Vec<_>>>()?;

        let metadata = if let Some(metadata) = &json_schema.metadata {
            metadata.clone()
        } else {
            HashMap::new()
        };

        Ok(Self::new_with_metadata(fields, metadata))
    }
}

/// Conversion between Arrow [Schema] and JSON representation (string).
pub trait ArrowJsonExt: Sized {
    fn to_json(&self) -> Result<String>;

    fn from_json(json: &str) -> Result<Self>;
}

impl ArrowJsonExt for Schema {
    fn to_json(&self) -> Result<String> {
        let json_schema = JsonSchema::try_from(self)?;
        Ok(serde_json::to_string(&json_schema)?)
    }

    fn from_json(json: &str) -> Result<Self> {
        let json_schema: JsonSchema = serde_json::from_str(json)?;
        json_schema.try_into()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use arrow_schema::TimeUnit;
    use serde_json::{Value, json};

    fn assert_type_json_str(dt: DataType, val: Value) {
        assert_eq!(
            serde_json::from_str::<Value>(
                &serde_json::to_string(&JsonDataType::try_new(&dt).unwrap()).unwrap()
            )
            .unwrap(),
            val
        );
    }

    fn assert_primitive_types(dt: DataType, type_name: &str) {
        assert_type_json_str(dt, json!({ "type": type_name }));
    }

    #[test]
    fn test_data_type_to_json() {
        assert_primitive_types(DataType::Null, "null");
        assert_primitive_types(DataType::Boolean, "bool");
        assert_primitive_types(DataType::Int8, "int8");
        assert_primitive_types(DataType::Int16, "int16");
        assert_primitive_types(DataType::Int32, "int32");
        assert_primitive_types(DataType::Int64, "int64");
        assert_primitive_types(DataType::UInt8, "uint8");
        assert_primitive_types(DataType::UInt16, "uint16");
        assert_primitive_types(DataType::UInt32, "uint32");
        assert_primitive_types(DataType::UInt64, "uint64");
        assert_primitive_types(DataType::Float16, "halffloat");
        assert_primitive_types(DataType::Float32, "float");
        assert_primitive_types(DataType::Float64, "double");
        assert_primitive_types(DataType::Utf8, "string");
        assert_primitive_types(DataType::LargeUtf8, "large_string");
        assert_primitive_types(DataType::Binary, "binary");
        assert_primitive_types(DataType::LargeBinary, "large_binary");
        assert_primitive_types(DataType::Date32, "date32:day");
        assert_primitive_types(DataType::Date64, "date64:ms");
        assert_primitive_types(DataType::Time32(TimeUnit::Second), "time32:s");
        assert_primitive_types(DataType::Decimal128(38, 10), "decimal:128:38:10");
        assert_primitive_types(DataType::Decimal256(76, 20), "decimal:256:76:20");
        assert_primitive_types(DataType::Decimal128(18, 6), "decimal:128:18:6");
        assert_primitive_types(DataType::Decimal256(50, 15), "decimal:256:50:15");
    }

    #[test]
    fn test_complex_types_to_json() {
        assert_type_json_str(
            DataType::List(Arc::new(Field::new("item", DataType::Float32, false))),
            json!(
                {
                    "type": "list",
                    "fields": [
                        {
                            "name": "item",
                            "type": {
                                "type": "float"
                            },
                            "nullable": false
                        }
                    ]
                }
            ),
        );

        assert_type_json_str(
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 32),
            json!(
                {
                    "type": "fixed_size_list",
                    "fields": [
                        {
                            "name": "item",
                            "type": {
                                "type": "float"
                            },
                            "nullable": false
                        }
                    ],
                    "length": 32
                }
            ),
        );

        assert_type_json_str(
            DataType::FixedSizeBinary(32),
            json!({
                "type": "fixed_size_binary",
                "length": 32
            }),
        );

        assert_type_json_str(
            DataType::Struct(
                vec![
                    Field::new("a", DataType::Date32, false),
                    Field::new("b", DataType::Int32, true),
                ]
                .into(),
            ),
            json!({
                "type": "struct",
                "fields": [
                    {
                        "name": "a",
                        "type": {
                            "type": "date32:day"
                        },
                        "nullable": false
                    },
                    {
                        "name": "b",
                        "type": {
                            "type": "int32"
                        },
                        "nullable": true
                    }
                ]
            }),
        );
    }

    #[test]
    fn test_schema_to_json() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Date32, false),
            Field::new("b", DataType::Int32, true),
            Field::new(
                "s",
                DataType::Struct(vec![Field::new("str", DataType::Utf8, false)].into()),
                false,
            ),
        ]);

        let json_str = schema.to_json().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&json_str).unwrap(),
            json!({
                "fields": [
                    {
                        "name": "a",
                        "type": {
                            "type": "date32:day"
                        },
                        "nullable": false
                    },
                    {
                        "name": "b",
                        "type": {
                            "type": "int32"
                        },
                        "nullable": true
                    },
                    {
                        "name": "s",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "str",
                                    "type": {
                                        "type": "string"
                                    },
                                    "nullable": false
                                }
                            ]
                        },
                        "nullable": false
                    },
                ]
            })
        );

        let actual = Schema::from_json(&json_str).unwrap();
        assert_eq!(schema, actual);
    }

    #[test]
    fn test_metadata_roundtrip() {
        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("sk_1".to_string(), "sv_1".to_string());

        let mut field1_metadata = HashMap::new();
        field1_metadata.insert("fk_1".to_string(), "fv_1".to_string());

        let field1 = Field::new("a", DataType::UInt8, false).with_metadata(field1_metadata.clone());
        let field2 = Field::new("b", DataType::Int32, true);

        let schema = Schema::new_with_metadata(vec![field1, field2], schema_metadata.clone());

        let json_str = schema.to_json().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&json_str).unwrap(),
            json!({
                "fields": [
                    {
                        "name": "a",
                        "type": {
                            "type": "uint8"
                        },
                        "nullable": false,
                        "metadata": {
                            "fk_1": "fv_1"
                        }
                    },
                    {
                        "name": "b",
                        "type": {
                            "type": "int32"
                        },
                        "nullable": true
                    }
                ],
                "metadata": {
                    "sk_1": "sv_1"
                }
            })
        );

        let actual = Schema::from_json(&json_str).unwrap();
        assert_eq!(schema, actual);

        assert_eq!(actual.metadata, schema_metadata);

        assert_eq!(actual.field(0).metadata(), &field1_metadata);
        assert_eq!(actual.field(1).metadata(), &HashMap::new());
    }
}
