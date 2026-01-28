// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{borrow::Cow, sync::Arc};

use arrow_array::{Array, FixedSizeListArray, Float32Array};
use arrow_schema::{DataType, Field};

use crate::embeddings::EmbeddingFunction;
use crate::Result;

#[derive(Debug, Clone)]
pub struct MockEmbed {
    name: String,
    dim: usize,
}

impl MockEmbed {
    pub fn new(name: impl Into<String>, dim: usize) -> Self {
        Self {
            name: name.into(),
            dim,
        }
    }
}

impl EmbeddingFunction for MockEmbed {
    fn name(&self) -> &str {
        &self.name
    }

    fn source_type(&self) -> Result<Cow<'_, DataType>> {
        Ok(Cow::Borrowed(&DataType::Utf8))
    }

    fn dest_type(&self) -> Result<Cow<'_, DataType>> {
        Ok(Cow::Owned(DataType::new_fixed_size_list(
            DataType::Float32,
            self.dim as _,
            true,
        )))
    }

    fn compute_source_embeddings(&self, source: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
        // We can't use the FixedSizeListBuilder here because it always adds a null bitmap
        // and we want to explicitly work with non-nullable arrays.
        let len = source.len();
        let inner = Arc::new(Float32Array::from(vec![Some(1.0); len * self.dim]));
        let field = Field::new("item", inner.data_type().clone(), false);
        let arr = FixedSizeListArray::new(Arc::new(field), self.dim as _, inner, None);

        Ok(Arc::new(arr))
    }

    #[allow(unused_variables)]
    fn compute_query_embeddings(&self, input: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
        todo!()
    }
}
