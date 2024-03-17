use std::sync::Arc;

use arrow_schema::Schema;

use lance::dataset::{ReadParams, WriteParams};
use lance::io::{ObjectStoreParams, WrappingObjectStore};

use crate::error::{Error, Result};

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

/// Find one default column to create index.
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
        Err(Error::Schema {
            message: "No vector column found to create index".to_string(),
        })
    } else if candidates.len() != 1 {
        Err(Error::Schema {
            message: format!(
                "More than one vector columns found, \
                    please specify which column to create index: {:?}",
                candidates
            ),
        })
    } else {
        Ok(candidates[0].to_string())
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
}
