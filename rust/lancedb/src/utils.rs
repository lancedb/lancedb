// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_schema::{DataType, Schema};
use lance::arrow::json::JsonDataType;
use lance::dataset::{ReadParams, WriteParams};
use lance::index::vector::utils::infer_vector_dim;
use lance::io::{ObjectStoreParams, WrappingObjectStore};
use lazy_static::lazy_static;

use crate::error::{Error, Result};

lazy_static! {
    static ref TABLE_NAME_REGEX: regex::Regex = regex::Regex::new(r"^[a-zA-Z0-9_\-\.]+$").unwrap();
}

pub trait PatchStoreParam {
    fn patch_with_store_wrapper(
        self,
        wrapper: Arc<dyn WrappingObjectStore>,
    ) -> Result<Option<ObjectStoreParams>>;
}

impl PatchStoreParam for Option<ObjectStoreParams> {
    fn patch_with_store_wrapper(
        self,
        wrapper: Arc<dyn WrappingObjectStore>,
    ) -> Result<Option<ObjectStoreParams>> {
        let mut params = self.unwrap_or_default();
        if params.object_store_wrapper.is_some() {
            return Err(Error::Other {
                message: "can not patch param because object store is already set".into(),
                source: None,
            });
        }
        params.object_store_wrapper = Some(wrapper);

        Ok(Some(params))
    }
}

pub trait PatchWriteParam {
    fn patch_with_store_wrapper(self, wrapper: Arc<dyn WrappingObjectStore>)
        -> Result<WriteParams>;
}

impl PatchWriteParam for WriteParams {
    fn patch_with_store_wrapper(
        mut self,
        wrapper: Arc<dyn WrappingObjectStore>,
    ) -> Result<WriteParams> {
        self.store_params = self.store_params.patch_with_store_wrapper(wrapper)?;
        Ok(self)
    }
}

// NOTE: we have some API inconsistency here.
// WriteParam is found in the form of Option<WriteParam> and ReadParam is found in the form of ReadParam

pub trait PatchReadParam {
    fn patch_with_store_wrapper(self, wrapper: Arc<dyn WrappingObjectStore>) -> Result<ReadParams>;
}

impl PatchReadParam for ReadParams {
    fn patch_with_store_wrapper(
        mut self,
        wrapper: Arc<dyn WrappingObjectStore>,
    ) -> Result<ReadParams> {
        self.store_options = self.store_options.patch_with_store_wrapper(wrapper)?;
        Ok(self)
    }
}

/// Validate table name.
pub fn validate_table_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidTableName {
            name: name.to_string(),
            reason: "Table names cannot be empty strings".to_string(),
        });
    }
    if !TABLE_NAME_REGEX.is_match(name) {
        return Err(Error::InvalidTableName {
            name: name.to_string(),
            reason:
                "Table names can only contain alphanumeric characters, underscores, hyphens, and periods"
                    .to_string(),
        });
    }
    Ok(())
}

/// Find one default column to create index or perform vector query.
pub(crate) fn default_vector_column(schema: &Schema, dim: Option<i32>) -> Result<String> {
    // Try to find a vector column.
    let candidates = schema
        .fields()
        .iter()
        .filter_map(|field| match infer_vector_dim(field.data_type()) {
            Ok(d) if dim.is_none() || dim == Some(d as i32) => Some(field.name()),
            _ => None,
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        Err(Error::InvalidInput {
            message: format!(
                "No vector column found to match with the query vector dimension: {}",
                dim.unwrap_or_default()
            ),
        })
    } else if candidates.len() != 1 {
        Err(Error::Schema {
            message: format!(
                "More than one vector columns found, \
                    please specify which column to create index or query: {:?}",
                candidates
            ),
        })
    } else {
        Ok(candidates[0].to_string())
    }
}

pub fn supported_btree_data_type(dtype: &DataType) -> bool {
    dtype.is_integer()
        || dtype.is_floating()
        || matches!(
            dtype,
            DataType::Boolean
                | DataType::Utf8
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Date32
                | DataType::Date64
                | DataType::Timestamp(_, _)
        )
}

pub fn supported_bitmap_data_type(dtype: &DataType) -> bool {
    dtype.is_integer() || matches!(dtype, DataType::Utf8)
}

pub fn supported_label_list_data_type(dtype: &DataType) -> bool {
    match dtype {
        DataType::List(field) => supported_bitmap_data_type(field.data_type()),
        DataType::FixedSizeList(field, _) => supported_bitmap_data_type(field.data_type()),
        _ => false,
    }
}

pub fn supported_fts_data_type(dtype: &DataType) -> bool {
    matches!(dtype, DataType::Utf8 | DataType::LargeUtf8)
}

pub fn supported_vector_data_type(dtype: &DataType) -> bool {
    match dtype {
        DataType::FixedSizeList(field, _) => {
            field.data_type().is_floating() || field.data_type() == &DataType::UInt8
        }
        DataType::List(field) => supported_vector_data_type(field.data_type()),
        _ => false,
    }
}

/// Note: this is temporary until we get a proper datatype conversion in Lance.
pub fn string_to_datatype(s: &str) -> Option<DataType> {
    let data_type: serde_json::Value = {
        if let Ok(data_type) = serde_json::from_str(s) {
            data_type
        } else {
            serde_json::json!({ "type": s })
        }
    };
    let json_type: JsonDataType = serde_json::from_value(data_type).ok()?;
    (&json_type).try_into().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_schema::{DataType, Field};

    #[test]
    fn test_guess_default_column() {
        let schema_no_vector = Schema::new(vec![
            Field::new("id", DataType::Int16, true),
            Field::new("tag", DataType::Utf8, false),
        ]);
        assert!(default_vector_column(&schema_no_vector, None)
            .unwrap_err()
            .to_string()
            .contains("No vector column"));

        let schema_with_vec_col = Schema::new(vec![
            Field::new("id", DataType::Int16, true),
            Field::new(
                "vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, false)), 10),
                false,
            ),
        ]);
        assert_eq!(
            default_vector_column(&schema_with_vec_col, None).unwrap(),
            "vec"
        );

        let multi_vec_col = Schema::new(vec![
            Field::new("id", DataType::Int16, true),
            Field::new(
                "vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, false)), 10),
                false,
            ),
            Field::new(
                "vec2",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, false)), 50),
                false,
            ),
        ]);
        assert!(default_vector_column(&multi_vec_col, None)
            .unwrap_err()
            .to_string()
            .contains("More than one"));
    }

    #[test]
    fn test_validate_table_name() {
        assert!(validate_table_name("my_table").is_ok());
        assert!(validate_table_name("my_table_1").is_ok());
        assert!(validate_table_name("123mytable").is_ok());
        assert!(validate_table_name("_12345table").is_ok());
        assert!(validate_table_name("table.12345").is_ok());
        assert!(validate_table_name("table.._dot_..12345").is_ok());

        assert!(validate_table_name("").is_err());
        assert!(validate_table_name("my_table!").is_err());
        assert!(validate_table_name("my/table").is_err());
        assert!(validate_table_name("my@table").is_err());
        assert!(validate_table_name("name with space").is_err());
    }

    #[test]
    fn test_string_to_datatype() {
        let string = "int32";
        let expected = DataType::Int32;
        assert_eq!(string_to_datatype(string), Some(expected));
    }
}
