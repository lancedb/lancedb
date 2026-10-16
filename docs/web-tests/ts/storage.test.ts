// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";
import * as lancedb from "@lancedb/lancedb";

// --8<-- [start:storage_connect_s3]
async function storageConnectS3() {
  const db = await lancedb.connect("s3://bucket/path");
  return db;
}
// --8<-- [end:storage_connect_s3]

// --8<-- [start:storage_connect_gcs]
async function storageConnectGcs() {
  const db = await lancedb.connect("gs://bucket/path");
  return db;
}
// --8<-- [end:storage_connect_gcs]

// --8<-- [start:storage_connect_azure]
async function storageConnectAzure() {
  const db = await lancedb.connect("az://bucket/path");
  return db;
}
// --8<-- [end:storage_connect_azure]

// --8<-- [start:storage_connect_timeout]
async function storageConnectTimeout() {
  const db = await lancedb.connect("s3://bucket/path", {
    storageOptions: { timeout: "60s" },
  });
  return db;
}
// --8<-- [end:storage_connect_timeout]

// --8<-- [start:storage_table_timeout]
async function storageTableTimeout() {
  const db = await lancedb.connect("s3://bucket/path");
  const table = await db.createTable(
    "table",
    [{ a: 1, b: 2 }],
    { storageOptions: { timeout: "60s" } },
  );
  return table;
}
// --8<-- [end:storage_table_timeout]

// --8<-- [start:storage_s3_ddb]
async function storageS3Ddb() {
  const db = await lancedb.connect(
    "s3+ddb://bucket/path?ddbTableName=my-dynamodb-table",
  );
  return db;
}
// --8<-- [end:storage_s3_ddb]

// --8<-- [start:storage_s3_ddb_local]
async function storageS3DdbLocal() {
  const db = await lancedb.connect(
    "s3+ddb://bucket/path?ddbTableName=my-dynamodb-table",
    {
      storageOptions: {
        endpoint: "http://localhost:4566",
        dynamodbEndpoint: "http://localhost:4566",
        allowHttp: "true",
      },
    },
  );
  return db;
}
// --8<-- [end:storage_s3_ddb_local]

// --8<-- [start:storage_s3_sse_kms]
async function storageS3SseKms() {
  const db = await lancedb.connect("s3://bucket/path", {
    storageOptions: {
      awsServerSideEncryption: "aws:kms",
      awsSseKmsKeyId: "<your-kms-key-id-or-arn>",
    },
  });
  return db;
}
// --8<-- [end:storage_s3_sse_kms]

// --8<-- [start:storage_azure_sas]
async function storageAzureSas() {
  const db = await lancedb.connect(
    "az://my-container/my-database",
    {
      storageOptions: {
        azureStorageAccountName: "some-account",
        azureStorageSasToken: "<sas-token>",
      },
    },
  );
  return db;
}
// --8<-- [end:storage_azure_sas]

// --8<-- [start:storage_s3_minio]
async function storageS3Minio() {
  const db = await lancedb.connect("s3://bucket/path", {
    storageOptions: {
      region: "us-east-1",
      endpoint: "http://minio:9000",
    },
  });
  return db;
}
// --8<-- [end:storage_s3_minio]

// --8<-- [start:storage_s3_express]
async function storageS3Express() {
  const db = await lancedb.connect(
    "s3://my-bucket--use1-az4--x-s3/path",
    {
      storageOptions: {
        region: "us-east-1",
        s3Express: "true",
      },
    },
  );
  return db;
}
// --8<-- [end:storage_s3_express]

// --8<-- [start:storage_gcs_service_account]
async function storageGcsServiceAccount() {
  const db = await lancedb.connect(
    "gs://my-bucket/my-database",
    {
      storageOptions: {
        serviceAccount: "path/to/service-account.json",
      },
    },
  );
  return db;
}
// --8<-- [end:storage_gcs_service_account]

// --8<-- [start:storage_azure_account]
async function storageAzureAccount() {
  const db = await lancedb.connect(
    "az://my-container/my-database",
    {
      storageOptions: {
        accountName: "some-account",
        accountKey: "some-key",
      },
    },
  );
  return db;
}
// --8<-- [end:storage_azure_account]

// --8<-- [start:storage_tigris_connect]
async function storageTigrisConnect() {
  const db = await lancedb.connect(
    "s3://your-bucket/path",
    {
      storageOptions: {
        endpoint: "https://t3.storage.dev",
        region: "auto",
      },
    },
  );
  return db;
}
// --8<-- [end:storage_tigris_connect]

test("storage ts snippets compile", async () => {
  expect(storageConnectS3).toBeDefined();
  expect(storageConnectGcs).toBeDefined();
  expect(storageConnectAzure).toBeDefined();
  expect(storageConnectTimeout).toBeDefined();
  expect(storageTableTimeout).toBeDefined();
  expect(storageS3Ddb).toBeDefined();
  expect(storageS3DdbLocal).toBeDefined();
  expect(storageS3Minio).toBeDefined();
  expect(storageS3Express).toBeDefined();
  expect(storageS3SseKms).toBeDefined();
  expect(storageGcsServiceAccount).toBeDefined();
  expect(storageAzureAccount).toBeDefined();
  expect(storageAzureSas).toBeDefined();
  expect(storageTigrisConnect).toBeDefined();
});

