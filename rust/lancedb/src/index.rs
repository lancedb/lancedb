// Copyright 2024 Lance Developers.
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

use crate::{table::TableInternal, Result};

use self::{scalar::BTreeIndexBuilder, vector::IvfPqIndexBuilder};

pub mod scalar;
pub mod vector;

pub enum Index {
    Auto,
    BTree(BTreeIndexBuilder),
    IvfPq(IvfPqIndexBuilder),
}

/// Builder for the create_index operation
///
/// The methods on this builder are used to specify options common to all indices.
pub struct IndexBuilder {
    parent: Arc<dyn TableInternal>,
    pub(crate) index: Index,
    pub(crate) columns: Vec<String>,
    pub(crate) replace: bool,
}

impl IndexBuilder {
    pub(crate) fn new(parent: Arc<dyn TableInternal>, columns: Vec<String>, index: Index) -> Self {
        Self {
            parent,
            index,
            columns,
            replace: true,
        }
    }

    /// Whether to replace the existing index, the default is `true`.
    ///
    /// If this is false, and another index already exists on the same columns
    /// and the same name, then an error will be returned.  This is true even if
    /// that index is out of date.
    pub fn replace(mut self, v: bool) -> Self {
        self.replace = v;
        self
    }

    pub async fn execute(self) -> Result<()> {
        self.parent.clone().create_index(self).await
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    IvfPq,
    BTree,
}

/// A description of an index currently configured on a column
pub struct IndexConfig {
    /// The type of the index
    pub index_type: IndexType,
    /// The columns in the index
    ///
    /// Currently this is always a Vec of size 1.  In the future there may
    /// be more columns to represent composite indices.
    pub columns: Vec<String>,
}
