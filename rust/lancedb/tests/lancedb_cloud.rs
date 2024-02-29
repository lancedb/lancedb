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

use arrow_array::RecordBatchIterator;

#[tokio::test]
#[ignore]
async fn cloud_integration_test() {
    let project = std::env::var("LANCEDB_PROJECT")
        .expect("the LANCEDB_PROJECT env must be set to run the cloud integration test");
    let api_key = std::env::var("LANCEDB_API_KEY")
        .expect("the LANCEDB_API_KEY env must be set to run the cloud integration test");
    let region = std::env::var("LANCEDB_REGION")
        .expect("the LANCEDB_REGION env must be set to run the cloud integration test");
    let host_override = std::env::var("LANCEDB_HOST_OVERRIDE")
        .map(Some)
        .unwrap_or(None);
    if host_override.is_none() {
        println!("No LANCEDB_HOST_OVERRIDE has been set.  Running integration test against LanceDb Cloud production instance");
    }

    let mut builder = lancedb::connect(&format!("db://{}", project))
        .api_key(&api_key)
        .region(&region);
    if let Some(host_override) = &host_override {
        builder = builder.host_override(host_override);
    }
    let db = builder.execute().await.unwrap();

    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
    ]));
    let initial_data = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3])),
            Arc::new(arrow_array::StringArray::from(vec!["a", "b", "c"])),
        ],
    );
    let rbr = RecordBatchIterator::new(vec![initial_data], schema);

    let name = uuid::Uuid::new_v4().to_string();
    let tbl = db
        .create_table(name.clone(), Box::new(rbr))
        .execute()
        .await
        .unwrap();

    assert_eq!(tbl.name(), name);

    let table_names = db.table_names().await.unwrap();
    assert!(table_names.contains(&name));
}
