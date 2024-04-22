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

/* eslint-disable @typescript-eslint/naming-convention */

import { connect } from "../dist";
import {
  CreateBucketCommand,
  DeleteBucketCommand,
  DeleteObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  S3Client,
} from "@aws-sdk/client-s3";
import {
  CreateKeyCommand,
  ScheduleKeyDeletionCommand,
  KMSClient,
} from "@aws-sdk/client-kms";

// Skip these tests unless the S3_TEST environment variable is set
const maybeDescribe = process.env.S3_TEST ? describe : describe.skip;

// These are all keys that are accepted by storage_options
const CONFIG = {
  allowHttp: "true",
  awsAccessKeyId: "ACCESSKEY",
  awsSecretAccessKey: "SECRETKEY",
  awsEndpoint: "http://127.0.0.1:4566",
  awsRegion: "us-east-1",
};

class S3Bucket {
  name: string;
  constructor(name: string) {
    this.name = name;
  }

  static s3Client() {
    return new S3Client({
      region: CONFIG.awsRegion,
      credentials: {
        accessKeyId: CONFIG.awsAccessKeyId,
        secretAccessKey: CONFIG.awsSecretAccessKey,
      },
      endpoint: CONFIG.awsEndpoint,
    });
  }

  public static async create(name: string): Promise<S3Bucket> {
    const client = this.s3Client();
    // Delete the bucket if it already exists
    try {
      await this.deleteBucket(client, name);
    } catch (e) {
      // It's fine if the bucket doesn't exist
    }
    await client.send(new CreateBucketCommand({ Bucket: name }));
    return new S3Bucket(name);
  }

  public async delete() {
    const client = S3Bucket.s3Client();
    await S3Bucket.deleteBucket(client, this.name);
  }

  static async deleteBucket(client: S3Client, name: string) {
    // Must delete all objects before we can delete the bucket
    const objects = await client.send(
      new ListObjectsV2Command({ Bucket: name }),
    );
    if (objects.Contents) {
      for (const object of objects.Contents) {
        await client.send(
          new DeleteObjectCommand({ Bucket: name, Key: object.Key }),
        );
      }
    }

    await client.send(new DeleteBucketCommand({ Bucket: name }));
  }

  public async assertAllEncrypted(path: string, keyId: string) {
    const client = S3Bucket.s3Client();
    const objects = await client.send(
      new ListObjectsV2Command({ Bucket: this.name, Prefix: path }),
    );
    if (objects.Contents) {
      for (const object of objects.Contents) {
        const metadata = await client.send(
          new HeadObjectCommand({ Bucket: this.name, Key: object.Key }),
        );
        expect(metadata.ServerSideEncryption).toBe("aws:kms");
        expect(metadata.SSEKMSKeyId).toContain(keyId);
      }
    }
  }
}

class KmsKey {
  keyId: string;
  constructor(keyId: string) {
    this.keyId = keyId;
  }

  static kmsClient() {
    return new KMSClient({
      region: CONFIG.awsRegion,
      credentials: {
        accessKeyId: CONFIG.awsAccessKeyId,
        secretAccessKey: CONFIG.awsSecretAccessKey,
      },
      endpoint: CONFIG.awsEndpoint,
    });
  }

  public static async create(): Promise<KmsKey> {
    const client = this.kmsClient();
    const key = await client.send(new CreateKeyCommand({}));
    const keyId = key?.KeyMetadata?.KeyId;
    if (!keyId) {
      throw new Error("Failed to create KMS key");
    }
    return new KmsKey(keyId);
  }

  public async delete() {
    const client = KmsKey.kmsClient();
    await client.send(new ScheduleKeyDeletionCommand({ KeyId: this.keyId }));
  }
}

maybeDescribe("storage_options", () => {
  let bucket: S3Bucket;
  let kmsKey: KmsKey;
  beforeAll(async () => {
    bucket = await S3Bucket.create("lancedb");
    kmsKey = await KmsKey.create();
  });
  afterAll(async () => {
    await kmsKey.delete();
    await bucket.delete();
  });

  it("can be used to configure auth and endpoints", async () => {
    const uri = `s3://${bucket.name}/test`;
    const db = await connect(uri, { storageOptions: CONFIG });

    let table = await db.createTable("test", [{ a: 1, b: 2 }]);

    let rowCount = await table.countRows();
    expect(rowCount).toBe(1);

    let tableNames = await db.tableNames();
    expect(tableNames).toEqual(["test"]);

    table = await db.openTable("test");
    rowCount = await table.countRows();
    expect(rowCount).toBe(1);

    await table.add([
      { a: 2, b: 3 },
      { a: 3, b: 4 },
    ]);
    rowCount = await table.countRows();
    expect(rowCount).toBe(3);

    await db.dropTable("test");

    tableNames = await db.tableNames();
    expect(tableNames).toEqual([]);
  });

  it("can configure encryption at connection and table level", async () => {
    const uri = `s3://${bucket.name}/test`;
    let db = await connect(uri, { storageOptions: CONFIG });

    let table = await db.createTable("table1", [{ a: 1, b: 2 }], {
      storageOptions: {
        awsServerSideEncryption: "aws:kms",
        awsSseKmsKeyId: kmsKey.keyId,
      },
    });

    let rowCount = await table.countRows();
    expect(rowCount).toBe(1);

    await table.add([{ a: 2, b: 3 }]);

    await bucket.assertAllEncrypted("test/table1.lance", kmsKey.keyId);

    // Now with encryption settings at connection level
    db = await connect(uri, {
      storageOptions: {
        ...CONFIG,
        awsServerSideEncryption: "aws:kms",
        awsSseKmsKeyId: kmsKey.keyId,
      },
    });
    table = await db.createTable("table2", [{ a: 1, b: 2 }]);
    rowCount = await table.countRows();
    expect(rowCount).toBe(1);

    await table.add([{ a: 2, b: 3 }]);

    await bucket.assertAllEncrypted("test/table2.lance", kmsKey.keyId);
  });
});
