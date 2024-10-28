// Copyright 2024 LanceDB Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use arrow_schema::{DataType, Schema};
use lance::dataset::{ReadParams, WriteParams};
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
    // Try to find one fixed size list array column.
    let candidates = schema
        .fields()
        .iter()
        .filter_map(|field| match field.data_type() {
            arrow_schema::DataType::FixedSizeList(f, d)
                if f.data_type().is_floating()
                    && dim.map(|expect| *d == expect).unwrap_or(true) =>
            {
                Some(field.name())
            }
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
        DataType::FixedSizeList(inner, _) => DataType::is_floating(inner.data_type()),
        _ => false,
    }
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
}
