// Copyright 2023 Lance Developers.
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

use serde::Deserialize;

use lance::table::format::{Index, Manifest};

pub struct VectorIndex {
    pub columns: Vec<String>,
    pub index_name: String,
    pub index_uuid: String,
}

impl VectorIndex {
    pub fn new_from_format(manifest: &Manifest, index: &Index) -> Self {
        let fields = index
            .fields
            .iter()
            .map(|i| manifest.schema.fields[*i as usize].name.clone())
            .collect();
        Self {
            columns: fields,
            index_name: index.name.clone(),
            index_uuid: index.uuid.to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct VectorIndexStatistics {
    pub num_indexed_rows: usize,
    pub num_unindexed_rows: usize,
}
