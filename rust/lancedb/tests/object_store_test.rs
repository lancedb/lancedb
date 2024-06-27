// Copyright 2023 LanceDB Developers.
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
#![cfg(feature = "s3-test")]
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};

use aws_config::{BehaviorVersion, ConfigLoader, Region, SdkConfig};
use aws_sdk_s3::{config::Credentials, types::ServerSideEncryption, Client as S3Client};
use lancedb::Result;

const CONFIG: &[(&str, &str)] = &[
    ("access_key_id", "ACCESS_KEY"),
    ("secret_access_key", "SECRET_KEY"),
    ("endpoint", "http://127.0.0.1:4566"),
    ("dynamodb_endpoint", "http://127.0.0.1:4566"),
    ("allow_http", "true"),
    ("region", "us-east-1"),
];

async fn aws_config() -> SdkConfig {
    let credentials = Credentials::new(CONFIG[0].1, CONFIG[1].1, None, None, "static");
    ConfigLoader::default()
        .credentials_provider(credentials)
        .endpoint_url(CONFIG[2].1)
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .load()
        .await
}

struct S3Bucket(String);

impl S3Bucket {
    async fn new(bucket: &str) -> Self {
        let config = aws_config().await;
        let client = S3Client::new(&config);

        // In case it wasn't deleted earlier
        Self::delete_bucket(client.clone(), bucket).await;

        client.create_bucket().bucket(bucket).send().await.unwrap();

        Self(bucket.to_string())
    }

    async fn delete_bucket(client: S3Client, bucket: &str) {
        // Before we delete the bucket, we need to delete all objects in it
        let res = client
            .list_objects_v2()
            .bucket(bucket)
            .send()
            .await
            .map_err(|err| err.into_service_error());
        match res {
            Err(e) if e.is_no_such_bucket() => return,
            Err(e) => panic!("Failed to list objects in bucket: {}", e),
            _ => {}
        }
        let objects = res.unwrap().contents.unwrap_or_default();
        for object in objects {
            client
                .delete_object()
                .bucket(bucket)
                .key(object.key.unwrap())
                .send()
                .await
                .unwrap();
        }
        client.delete_bucket().bucket(bucket).send().await.unwrap();
    }
}

impl Drop for S3Bucket {
    fn drop(&mut self) {
        let bucket_name = self.0.clone();
        tokio::task::spawn(async move {
            let config = aws_config().await;
            let client = S3Client::new(&config);
            Self::delete_bucket(client, &bucket_name).await;
        });
    }
}

fn test_data() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_minio_lifecycle() -> Result<()> {
    // test create, update, drop, list on localstack minio
    let bucket = S3Bucket::new("test-bucket").await;
    let uri = format!("s3://{}", bucket.0);

    let db = lancedb::connect(&uri)
        .storage_options(CONFIG.iter().cloned())
        .execute()
        .await?;

    let data = test_data();
    let data = RecordBatchIterator::new(vec![Ok(data.clone())], data.schema());

    let table = db.create_table("test_table", data).execute().await?;

    let row_count = table.count_rows(None).await?;
    assert_eq!(row_count, 3);

    let table_names = db.table_names().execute().await?;
    assert_eq!(table_names, vec!["test_table"]);

    // Re-open the table
    let table = db.open_table("test_table").execute().await?;
    let row_count = table.count_rows(None).await?;
    assert_eq!(row_count, 3);

    let data = test_data();
    let data = RecordBatchIterator::new(vec![Ok(data.clone())], data.schema());
    table.add(data).execute().await?;

    db.drop_table("test_table").await?;

    Ok(())
}

struct KMSKey(String);

impl KMSKey {
    async fn new() -> Self {
        let config = aws_config().await;
        let client = aws_sdk_kms::Client::new(&config);
        let key = client
            .create_key()
            .description("test key")
            .send()
            .await
            .unwrap()
            .key_metadata
            .unwrap()
            .key_id;
        Self(key)
    }
}

impl Drop for KMSKey {
    fn drop(&mut self) {
        let key_id = self.0.clone();
        tokio::task::spawn(async move {
            let config = aws_config().await;
            let client = aws_sdk_kms::Client::new(&config);
            client
                .schedule_key_deletion()
                .key_id(&key_id)
                .send()
                .await
                .unwrap();
        });
    }
}

async fn validate_objects_encrypted(bucket: &str, path: &str, kms_key_id: &str) {
    // Get S3 client
    let config = aws_config().await;
    let client = S3Client::new(&config);

    // list the objects are the path
    let objects = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(path)
        .send()
        .await
        .unwrap()
        .contents
        .unwrap();

    let mut errors = vec![];
    let mut correctly_encrypted = vec![];

    // For each object, call head
    for object in &objects {
        let head = client
            .head_object()
            .bucket(bucket)
            .key(object.key().unwrap())
            .send()
            .await
            .unwrap();

        // Verify the object is encrypted
        if head.server_side_encryption() != Some(&ServerSideEncryption::AwsKms) {
            errors.push(format!("Object {} is not encrypted", object.key().unwrap()));
            continue;
        }
        if !(head
            .ssekms_key_id()
            .map(|arn| arn.ends_with(kms_key_id))
            .unwrap_or(false))
        {
            errors.push(format!(
                "Object {} has wrong key id: {:?}, vs expected: {}",
                object.key().unwrap(),
                head.ssekms_key_id(),
                kms_key_id
            ));
            continue;
        }
        correctly_encrypted.push(object.key().unwrap().to_string());
    }

    if !errors.is_empty() {
        panic!(
            "{} of {} correctly encrypted: {:?}\n{} of {} not correct: {:?}",
            correctly_encrypted.len(),
            objects.len(),
            correctly_encrypted,
            errors.len(),
            objects.len(),
            errors
        );
    }
}

#[tokio::test]
async fn test_encryption() -> Result<()> {
    // test encryption on localstack minio
    let bucket = S3Bucket::new("test-encryption").await;
    let key = KMSKey::new().await;

    let uri = format!("s3://{}", bucket.0);
    let db = lancedb::connect(&uri)
        .storage_options(CONFIG.iter().cloned())
        .execute()
        .await?;

    // Create a table with encryption
    let data = test_data();
    let data = RecordBatchIterator::new(vec![Ok(data.clone())], data.schema());

    let mut builder = db.create_table("test_table", data);
    for (key, value) in CONFIG {
        builder = builder.storage_option(*key, *value);
    }
    let table = builder
        .storage_option("aws_server_side_encryption", "aws:kms")
        .storage_option("aws_sse_kms_key_id", &key.0)
        .execute()
        .await?;
    validate_objects_encrypted(&bucket.0, "test_table", &key.0).await;

    table.delete("a = 1").await?;
    validate_objects_encrypted(&bucket.0, "test_table", &key.0).await;

    // Test we can set encryption at the connection level.
    let db = lancedb::connect(&uri)
        .storage_options(CONFIG.iter().cloned())
        .storage_option("aws_server_side_encryption", "aws:kms")
        .storage_option("aws_sse_kms_key_id", &key.0)
        .execute()
        .await?;

    let table = db.open_table("test_table").execute().await?;

    let data = test_data();
    let data = RecordBatchIterator::new(vec![Ok(data.clone())], data.schema());
    table.add(data).execute().await?;
    validate_objects_encrypted(&bucket.0, "test_table", &key.0).await;

    Ok(())
}

struct DynamoDBCommitTable(String);

impl DynamoDBCommitTable {
    async fn new(name: &str) -> Self {
        let config = aws_config().await;
        let client = aws_sdk_dynamodb::Client::new(&config);

        // In case it wasn't deleted earlier
        Self::delete_table(client.clone(), name).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        use aws_sdk_dynamodb::types::*;

        client
            .create_table()
            .table_name(name)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("base_uri")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("version")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("base_uri")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("version")
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            )
            .provisioned_throughput(
                ProvisionedThroughput::builder()
                    .read_capacity_units(1)
                    .write_capacity_units(1)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();

        Self(name.to_string())
    }

    async fn delete_table(client: aws_sdk_dynamodb::Client, name: &str) {
        match client
            .delete_table()
            .table_name(name)
            .send()
            .await
            .map_err(|err| err.into_service_error())
        {
            Ok(_) => {}
            Err(e) if e.is_resource_not_found_exception() => {}
            Err(e) => panic!("Failed to delete table: {}", e),
        };
    }
}

impl Drop for DynamoDBCommitTable {
    fn drop(&mut self) {
        let table_name = self.0.clone();
        tokio::task::spawn(async move {
            let config = aws_config().await;
            let client = aws_sdk_dynamodb::Client::new(&config);
            Self::delete_table(client, &table_name).await;
        });
    }
}

#[tokio::test]
async fn test_concurrent_dynamodb_commit() {
    // test concurrent commit on dynamodb
    let bucket = S3Bucket::new("test-dynamodb").await;
    let table = DynamoDBCommitTable::new("test_table").await;

    let uri = format!("s3+ddb://{}?ddbTableName={}", bucket.0, table.0);
    let db = lancedb::connect(&uri)
        .storage_options(CONFIG.iter().cloned())
        .execute()
        .await
        .unwrap();

    let data = test_data();
    let data = RecordBatchIterator::new(vec![Ok(data.clone())], data.schema());

    let table = db.create_table("test_table", data).execute().await.unwrap();

    let data = test_data();

    let mut tasks = vec![];
    for _ in 0..5 {
        let table = db.open_table("test_table").execute().await.unwrap();
        let data = data.clone();
        tasks.push(tokio::spawn(async move {
            let data = RecordBatchIterator::new(vec![Ok(data.clone())], data.schema());
            table.add(data).execute().await.unwrap();
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    table.checkout_latest().await.unwrap();
    let row_count = table.count_rows(None).await.unwrap();
    assert_eq!(row_count, 18);
}
