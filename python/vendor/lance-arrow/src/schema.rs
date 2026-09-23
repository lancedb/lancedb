// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Extension to arrow schema

use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema};

use crate::{ARROW_EXT_NAME_KEY, BLOB_META_KEY, BLOB_V2_EXT_NAME};

pub enum Indentation {
    OneLine,
    MultiLine(u8),
}

impl Indentation {
    fn value(&self) -> String {
        match self {
            Self::OneLine => "".to_string(),
            Self::MultiLine(spaces) => " ".repeat(*spaces as usize),
        }
    }

    fn deepen(&self) -> Self {
        match self {
            Self::OneLine => Self::OneLine,
            Self::MultiLine(spaces) => Self::MultiLine(spaces + 2),
        }
    }
}

/// Extends the functionality of [arrow_schema::Field].
pub trait FieldExt {
    /// Create a compact string representation of the field
    ///
    /// This is intended for display purposes and not for serialization
    fn to_compact_string(&self, indent: Indentation) -> String;

    /// Check if the field is marked as a packed struct
    fn is_packed_struct(&self) -> bool;

    /// Check if the field is marked as a blob
    fn is_blob(&self) -> bool;

    /// Check if the field is marked as a blob
    fn is_blob_v2(&self) -> bool;
}

impl FieldExt for Field {
    fn to_compact_string(&self, indent: Indentation) -> String {
        let mut result = format!("{}: ", self.name().clone());
        match self.data_type() {
            DataType::Struct(fields) => {
                result += "{";
                result += &indent.value();
                for (field_idx, field) in fields.iter().enumerate() {
                    result += field.to_compact_string(indent.deepen()).as_str();
                    if field_idx < fields.len() - 1 {
                        result += ",";
                    }
                    result += indent.value().as_str();
                }
                result += "}";
            }
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field) => {
                result += "[";
                result += field.to_compact_string(indent.deepen()).as_str();
                result += "]";
            }
            DataType::FixedSizeList(child, dimension) => {
                result += &format!(
                    "[{}; {}]",
                    child.to_compact_string(indent.deepen()),
                    dimension
                );
            }
            DataType::Dictionary(key_type, value_type) => {
                result += &value_type.to_string();
                result += "@";
                result += &key_type.to_string();
            }
            _ => {
                result += &self.data_type().to_string();
            }
        }
        if self.is_nullable() {
            result += "?";
        }
        result
    }

    // Check if field has metadata `packed` set to true, this check is case insensitive.
    fn is_packed_struct(&self) -> bool {
        let field_metadata = self.metadata();
        const PACKED_KEYS: [&str; 2] = ["packed", "lance-encoding:packed"];
        PACKED_KEYS.iter().any(|key| {
            field_metadata
                .get(*key)
                .map(|value| value.eq_ignore_ascii_case("true"))
                .unwrap_or(false)
        })
    }

    fn is_blob(&self) -> bool {
        let field_metadata = self.metadata();
        field_metadata.get(BLOB_META_KEY).is_some()
            || field_metadata
                .get(ARROW_EXT_NAME_KEY)
                .map(|value| value == BLOB_V2_EXT_NAME)
                .unwrap_or(false)
    }

    fn is_blob_v2(&self) -> bool {
        let field_metadata = self.metadata();
        field_metadata
            .get(ARROW_EXT_NAME_KEY)
            .map(|value| value == BLOB_V2_EXT_NAME)
            .unwrap_or(false)
    }
}

/// Extends the functionality of [arrow_schema::Schema].
pub trait SchemaExt {
    /// Create a new [`Schema`] with one extra field.
    fn try_with_column(&self, field: Field) -> std::result::Result<Schema, ArrowError>;

    fn try_with_column_at(
        &self,
        index: usize,
        field: Field,
    ) -> std::result::Result<Schema, ArrowError>;

    fn field_names(&self) -> Vec<&String>;

    fn without_column(&self, column_name: &str) -> Schema;

    /// Create a compact string representation of the schema
    ///
    /// This is intended for display purposes and not for serialization
    fn to_compact_string(&self, indent: Indentation) -> String;
}

impl SchemaExt for Schema {
    fn try_with_column(&self, field: Field) -> std::result::Result<Schema, ArrowError> {
        if self.column_with_name(field.name()).is_some() {
            return Err(ArrowError::SchemaError(format!(
                "Can not append column {} on schema: {:?}",
                field.name(),
                self
            )));
        };
        let mut fields: Vec<FieldRef> = self.fields().iter().cloned().collect();
        fields.push(FieldRef::new(field));
        Ok(Self::new_with_metadata(fields, self.metadata.clone()))
    }

    fn try_with_column_at(
        &self,
        index: usize,
        field: Field,
    ) -> std::result::Result<Schema, ArrowError> {
        if self.column_with_name(field.name()).is_some() {
            return Err(ArrowError::SchemaError(format!(
                "Failed to modify schema: Inserting column {} would create a duplicate column in schema: {:?}",
                field.name(),
                self
            )));
        };
        let mut fields: Vec<FieldRef> = self.fields().iter().cloned().collect();
        fields.insert(index, FieldRef::new(field));
        Ok(Self::new_with_metadata(fields, self.metadata.clone()))
    }

    /// Project the schema to remove the given column.
    ///
    /// This only works on top-level fields right now. If a field does not exist,
    /// the schema will be returned as is.
    fn without_column(&self, column_name: &str) -> Schema {
        let fields: Vec<FieldRef> = self
            .fields()
            .iter()
            .filter(|f| f.name() != column_name)
            .cloned()
            .collect();
        Self::new_with_metadata(fields, self.metadata.clone())
    }

    fn field_names(&self) -> Vec<&String> {
        self.fields().iter().map(|f| f.name()).collect()
    }

    fn to_compact_string(&self, indent: Indentation) -> String {
        let mut result = "{".to_string();
        result += &indent.value();
        for (field_idx, field) in self.fields.iter().enumerate() {
            result += field.to_compact_string(indent.deepen()).as_str();
            if field_idx < self.fields.len() - 1 {
                result += ",";
            }
            result += indent.value().as_str();
        }
        result += "}";
        result
    }
}
