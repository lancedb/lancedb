# Configuring cloud storage

<!-- TODO: When we add documentation for how to configure other storage types
           we can change the name to a more general "Configuring storage" -->

When using LanceDB OSS, you can choose where to store your data. The tradeoffs between different storage options are discussed in the [storage concepts guide](../concepts/storage.md). This guide shows how to configure LanceDB to use different storage options.

## Object Stores

LanceDB OSS supports object stores such as AWS S3 (and compatible stores), Azure Blob Store, and Google Cloud Storage. Which object store to use is determined by the URI scheme of the dataset path. `s3://` is used for AWS S3, `az://` is used for Azure Blob Storage, and `gs://` is used for Google Cloud Storage. These URIs are passed to the `connect` function:

=== "Python"

    AWS S3:

    ```python
    import lancedb
    db = lancedb.connect("s3://bucket/path")
    ```

    Google Cloud Storage:

    ```python
    import lancedb
    db = lancedb.connect("gs://bucket/path")
    ```

    Azure Blob Storage:

    ```python
    import lancedb
    db = lancedb.connect("az://bucket/path")
    ```

=== "TypeScript"

    === "@lancedb/lancedb"

        AWS S3:

        ```ts
        import * as lancedb from "@lancedb/lancedb";
        const db = await lancedb.connect("s3://bucket/path");
        ```

        Google Cloud Storage:

        ```ts
        import * as lancedb from "@lancedb/lancedb";
        const db = await lancedb.connect("gs://bucket/path");
        ```

        Azure Blob Storage:

        ```ts
        import * as lancedb from "@lancedb/lancedb";
        const db = await lancedb.connect("az://bucket/path");
        ```


    === "vectordb (deprecated)"

        AWS S3:

        ```ts
        const lancedb = require("lancedb");
        const db = await lancedb.connect("s3://bucket/path");
        ```

        Google Cloud Storage:

        ```ts
        const lancedb = require("lancedb");
        const db = await lancedb.connect("gs://bucket/path");
        ```

        Azure Blob Storage:

        ```ts
        const lancedb = require("lancedb");
        const db = await lancedb.connect("az://bucket/path");
        ```

In most cases, when running in the respective cloud and permissions are set up correctly, no additional configuration is required. When running outside of the respective cloud, authentication credentials must be provided. Credentials and other configuration options can be set in two ways: first, by setting environment variables. And second, by passing a `storage_options` object to the `connect` function. For example, to increase the request timeout to 60 seconds, you can set the `TIMEOUT` environment variable to `60s`:

```bash
export TIMEOUT=60s
```

!!! note "`storage_options` availability"

    The `storage_options` parameter is only available in Python *async* API and JavaScript API.
    It is not yet supported in the Python synchronous API.

If you only want this to apply to one particular connection, you can pass the `storage_options` argument when opening the connection:

=== "Python"

    ```python
    import lancedb
    db = await lancedb.connect_async(
        "s3://bucket/path",
        storage_options={"timeout": "60s"}
    )
    ```

=== "TypeScript"

    === "@lancedb/lancedb"

        ```ts
        import * as lancedb from "@lancedb/lancedb";

        const db = await lancedb.connect("s3://bucket/path", {
            storageOptions: {timeout: "60s"}
        });
        ```

    === "vectordb (deprecated)"

        ```ts
        const lancedb = require("lancedb");
        const db = await lancedb.connect("s3://bucket/path", {
            storageOptions: {timeout: "60s"}
        });
        ```

Getting even more specific, you can set the `timeout` for only a particular table:

=== "Python"

    <!-- skip-test -->
    ```python
    import lancedb
    db = await lancedb.connect_async("s3://bucket/path")
    table = await db.create_table(
        "table",
        [{"a": 1, "b": 2}],
        storage_options={"timeout": "60s"}
    )
    ```

=== "TypeScript"

    === "@lancedb/lancedb"

        <!-- skip-test -->
        ```ts
        import * as lancedb from "@lancedb/lancedb";
        const db = await lancedb.connect("s3://bucket/path");
        const table = db.createTable(
            "table",
            [{ a: 1, b: 2}],
            {storageOptions: {timeout: "60s"}}
        );
        ```

    === "vectordb (deprecated)"

        <!-- skip-test -->
        ```ts
        const lancedb = require("lancedb");
        const db = await lancedb.connect("s3://bucket/path");
        const table = db.createTable(
            "table",
            [{ a: 1, b: 2}],
            {storageOptions: {timeout: "60s"}}
        );
        ```

!!! info "Storage option casing"

    The storage option keys are case-insensitive. So `connect_timeout` and `CONNECT_TIMEOUT` are the same setting. Usually lowercase is used in the `storage_options` argument and uppercase is used for environment variables. In the `lancedb` Node package, the keys can also be provided in `camelCase` capitalization. For example, `connectTimeout` is equivalent to `connect_timeout`.

### General configuration

There are several options that can be set for all object stores, mostly related to network client configuration.

<!-- from here: https://docs.rs/object_store/latest/object_store/enum.ClientConfigKey.html -->

| Key                        | Description                                                                                      |
|----------------------------|--------------------------------------------------------------------------------------------------|
| `allow_http`               | Allow non-TLS, i.e. non-HTTPS connections. Default: `False`.                                      |
| `allow_invalid_certificates`| Skip certificate validation on HTTPS connections. Default: `False`.                               |
| `connect_timeout`          | Timeout for only the connect phase of a Client. Default: `5s`.                                    |
| `timeout`                  | Timeout for the entire request, from connection until the response body has finished. Default: `30s`. |
| `user_agent`               | User agent string to use in requests.                                                             |
| `proxy_url`                | URL of a proxy server to use for requests. Default: `None`.                                       |
| `proxy_ca_certificate`     | PEM-formatted CA certificate for proxy connections.                                                |
| `proxy_excludes`           | List of hosts that bypass the proxy. This is a comma-separated list of domains and IP masks. Any subdomain of the provided domain will be bypassed. For example, `example.com, 192.168.1.0/24` would bypass `https://api.example.com`, `https://www.example.com`, and any IP in the range `192.168.1.0/24`. |

### AWS S3

To configure credentials for AWS S3, you can use the `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` keys. Region can also be set, but it is not mandatory when using AWS.
These can be set as environment variables or passed in the `storage_options` parameter:

=== "Python"

    ```python
    import lancedb
    db = await lancedb.connect_async(
        "s3://bucket/path",
        storage_options={
            "aws_access_key_id": "my-access-key",
            "aws_secret_access_key": "my-secret-key",
            "aws_session_token": "my-session-token",
        }
    )
    ```

=== "TypeScript"

    === "@lancedb/lancedb"

        ```ts
        import * as lancedb from "@lancedb/lancedb";
        const db = await lancedb.connect(
            "s3://bucket/path",
            {
                storageOptions: {
                    awsAccessKeyId: "my-access-key",
                    awsSecretAccessKey: "my-secret-key",
                    awsSessionToken: "my-session-token",
                }
            }
        );
        ```

    === "vectordb (deprecated)"

        ```ts
        const lancedb = require("lancedb");
        const db = await lancedb.connect(
            "s3://bucket/path",
            {
                storageOptions: {
                    awsAccessKeyId: "my-access-key",
                    awsSecretAccessKey: "my-secret-key",
                    awsSessionToken: "my-session-token",
                }
            }
        );
        ```

Alternatively, if you are using AWS SSO, you can use the `AWS_PROFILE` and `AWS_DEFAULT_REGION` environment variables.

The following keys can be used as both environment variables or keys in the `storage_options` parameter:

| Key                                | Description                                                                                          |
|------------------------------------|------------------------------------------------------------------------------------------------------|
| `aws_region` / `region`             | The AWS region the bucket is in. This can be automatically detected when using AWS S3, but must be specified for S3-compatible stores. |
| `aws_access_key_id` / `access_key_id` | The AWS access key ID to use.                                                                       |
| `aws_secret_access_key` / `secret_access_key` | The AWS secret access key to use.                                                               |
| `aws_session_token` / `session_token` | The AWS session token to use.                                                                     |
| `aws_endpoint` / `endpoint`         | The endpoint to use for S3-compatible stores.                                                       |
| `aws_virtual_hosted_style_request` / `virtual_hosted_style_request` | Whether to use virtual hosted-style requests, where the bucket name is part of the endpoint. Meant to be used with `aws_endpoint`. Default: `False`. |
| `aws_s3_express` / `s3_express`     | Whether to use S3 Express One Zone endpoints. Default: `False`. See more details below.             |
| `aws_server_side_encryption`        | The server-side encryption algorithm to use. Must be one of `"AES256"`, `"aws:kms"`, or `"aws:kms:dsse"`. Default: `None`. |
| `aws_sse_kms_key_id`                | The KMS key ID to use for server-side encryption. If set, `aws_server_side_encryption` must be `"aws:kms"` or `"aws:kms:dsse"`. |
| `aws_sse_bucket_key_enabled`        | Whether to use bucket keys for server-side encryption.                                               |

!!! tip "Automatic cleanup for failed writes"

    LanceDB uses [multi-part uploads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html) when writing data to S3 in order to maximize write speed. LanceDB will abort these uploads when it shuts down gracefully, such as when cancelled by keyboard interrupt. However, in the rare case that LanceDB crashes, it is possible that some data will be left lingering in your account. To cleanup this data, we recommend (as AWS themselves do) that you setup a lifecycle rule to delete in-progress uploads after 7 days. See the AWS guide:

    **[Configuring a bucket lifecycle configuration to delete incomplete multipart uploads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html)**

#### AWS IAM Permissions

If a bucket is private, then an IAM policy must be specified to allow access to it. For many development scenarios, using broad permissions such as a PowerUser account is more than sufficient for working with LanceDB. However, in many production scenarios, you may wish to have as narrow as possible permissions.

For **read and write access**, LanceDB will need a policy such as:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
              "s3:PutObject",
              "s3:GetObject",
              "s3:DeleteObject",
            ],
            "Resource": "arn:aws:s3:::<bucket>/<prefix>/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::<bucket>",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "<prefix>/*"
                    ]
                }
            }
        }
    ]
}
```

For **read-only access**, LanceDB will need a policy such as:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
              "s3:GetObject",
            ],
            "Resource": "arn:aws:s3:::<bucket>/<prefix>/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::<bucket>",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "<prefix>/*"
                    ]
                }
            }
        }
    ]
}
```

#### DynamoDB Commit Store for concurrent writes

By default, S3 does not support concurrent writes. Having two or more processes
writing to the same table at the same time can lead to data corruption. This is
because S3, unlike other object stores, does not have any atomic put or copy
operation.

To enable concurrent writes, you can configure LanceDB to use a DynamoDB table
as a commit store. This table will be used to coordinate writes between
different processes. To enable this feature, you must modify your connection
URI to use the `s3+ddb` scheme and add a query parameter `ddbTableName` with the
name of the table to use.

=== "Python"

    ```python
    import lancedb
    db = await lancedb.connect_async(
        "s3+ddb://bucket/path?ddbTableName=my-dynamodb-table",
    )
    ```

=== "JavaScript"

    ```javascript
    const lancedb = require("lancedb");

    const db = await lancedb.connect(
        "s3+ddb://bucket/path?ddbTableName=my-dynamodb-table",
    );
    ```

The DynamoDB table must be created with the following schema:

- Hash key: `base_uri` (string)
- Range key: `version` (number)

You can create this programmatically with:

=== "Python"

    <!-- skip-test -->
    ```python
    import boto3

    dynamodb = boto3.client("dynamodb")
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "base_uri", "KeyType": "HASH"},
            {"AttributeName": "version", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "base_uri", "AttributeType": "S"},
            {"AttributeName": "version", "AttributeType": "N"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    ```

=== "JavaScript"

    <!-- skip-test -->
    ```javascript
    import {
      CreateTableCommand,
      DynamoDBClient,
    } from "@aws-sdk/client-dynamodb";

    const dynamodb = new DynamoDBClient({
      region: CONFIG.awsRegion,
      credentials: {
        accessKeyId: CONFIG.awsAccessKeyId,
        secretAccessKey: CONFIG.awsSecretAccessKey,
      },
      endpoint: CONFIG.awsEndpoint,
    });
    const command = new CreateTableCommand({
      TableName: table_name,
      AttributeDefinitions: [
        {
          AttributeName: "base_uri",
          AttributeType: "S",
        },
        {
          AttributeName: "version",
          AttributeType: "N",
        },
      ],
      KeySchema: [
        { AttributeName: "base_uri", KeyType: "HASH" },
        { AttributeName: "version", KeyType: "RANGE" },
      ],
      ProvisionedThroughput: {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 1,
      },
    });
    await client.send(command);
    ```


#### S3-compatible stores

LanceDB can also connect to S3-compatible stores, such as MinIO. To do so, you must specify both region and endpoint:

=== "Python"

    ```python
    import lancedb
    db = await lancedb.connect_async(
        "s3://bucket/path",
        storage_options={
            "region": "us-east-1",
            "endpoint": "http://minio:9000",
        }
    )
    ```

=== "TypeScript"

    === "@lancedb/lancedb"

        ```ts
        import * as lancedb from "@lancedb/lancedb";
        const db = await lancedb.connect(
            "s3://bucket/path",
            {
                storageOptions: {
                    region: "us-east-1",
                    endpoint: "http://minio:9000",
                }
            }
        );
        ```

    === "vectordb (deprecated)"

        ```ts
        const lancedb = require("lancedb");
        const db = await lancedb.connect(
            "s3://bucket/path",
            {
                storageOptions: {
                    region: "us-east-1",
                    endpoint: "http://minio:9000",
                }
            }
        );
        ```

This can also be done with the ``AWS_ENDPOINT`` and ``AWS_DEFAULT_REGION`` environment variables.

!!! tip "Local servers"

    For local development, the server often has a `http` endpoint rather than a
    secure `https` endpoint. In this case, you must also set the `ALLOW_HTTP`
    environment variable to `true` to allow non-TLS connections, or pass the
    storage option `allow_http` as `true`. If you do not do this, you will get
    an error like `URL scheme is not allowed`.

#### S3 Express

LanceDB supports [S3 Express One Zone](https://aws.amazon.com/s3/storage-classes/express-one-zone/) endpoints, but requires additional configuration. Also, S3 Express endpoints only support connecting from an EC2 instance within the same region.

To configure LanceDB to use an S3 Express endpoint, you must set the storage option `s3_express`. The bucket name in your table URI should **include the suffix**.

=== "Python"

    ```python
    import lancedb
    db = await lancedb.connect_async(
        "s3://my-bucket--use1-az4--x-s3/path",
        storage_options={
            "region": "us-east-1",
            "s3_express": "true",
        }
    )
    ```

=== "TypeScript"

    === "@lancedb/lancedb"

        ```ts
        import * as lancedb from "@lancedb/lancedb";
        const db = await lancedb.connect(
            "s3://my-bucket--use1-az4--x-s3/path",
            {
                storageOptions: {
                    region: "us-east-1",
                    s3Express: "true",
                }
            }
        );
        ```

    === "vectordb (deprecated)"

        ```ts
        const lancedb = require("lancedb");
        const db = await lancedb.connect(
            "s3://my-bucket--use1-az4--x-s3/path",
            {
                storageOptions: {
                    region: "us-east-1",
                    s3Express: "true",
                }
            }
        );
        ```

### Google Cloud Storage

GCS credentials are configured by setting the `GOOGLE_SERVICE_ACCOUNT` environment variable to the path of a JSON file containing the service account credentials. Alternatively, you can pass the path to the JSON file in the `storage_options`:

=== "Python"

    <!-- skip-test -->
    ```python
    import lancedb
    db = await lancedb.connect_async(
        "gs://my-bucket/my-database",
        storage_options={
            "service_account": "path/to/service-account.json",
        }
    )
    ```

=== "TypeScript"

    === "@lancedb/lancedb"

        ```ts
        import * as lancedb from "@lancedb/lancedb";
        const db = await lancedb.connect(
            "gs://my-bucket/my-database",
            {
                storageOptions: {
                    serviceAccount: "path/to/service-account.json",
                }
            }
        );
        ```

    === "vectordb (deprecated)"

        ```ts
        const lancedb = require("lancedb");
        const db = await lancedb.connect(
            "gs://my-bucket/my-database",
            {
                storageOptions: {
                    serviceAccount: "path/to/service-account.json",
                }
            }
        );
        ```

!!! info "HTTP/2 support"

    By default, GCS uses HTTP/1 for communication, as opposed to HTTP/2. This improves maximum throughput significantly. However, if you wish to use HTTP/2 for some reason, you can set the environment variable `HTTP1_ONLY` to `false`.

The following keys can be used as both environment variables or keys in the `storage_options` parameter:
<!-- source: https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html -->

| Key                                   | Description                                  |
|---------------------------------------|----------------------------------------------|
| ``google_service_account`` / `service_account` | Path to the service account JSON file.       |
| ``google_service_account_key``        | The serialized service account key.          |
| ``google_application_credentials``    | Path to the application credentials.         |

### Azure Blob Storage

Azure Blob Storage credentials can be configured by setting the `AZURE_STORAGE_ACCOUNT_NAME`and `AZURE_STORAGE_ACCOUNT_KEY` environment variables. Alternatively, you can pass the account name and key in the `storage_options` parameter:

=== "Python"

    <!-- skip-test -->
    ```python
    import lancedb
    db = await lancedb.connect_async(
        "az://my-container/my-database",
        storage_options={
            account_name: "some-account",
            account_key: "some-key",
        }
    )
    ```

=== "TypeScript"

    === "@lancedb/lancedb"

        ```ts
        import * as lancedb from "@lancedb/lancedb";
        const db = await lancedb.connect(
            "az://my-container/my-database",
            {
                storageOptions: {
                    accountName: "some-account",
                    accountKey: "some-key",
                }
            }
        );
        ```

    === "vectordb (deprecated)"

        ```ts
        const lancedb = require("lancedb");
        const db = await lancedb.connect(
            "az://my-container/my-database",
            {
                storageOptions: {
                    accountName: "some-account",
                    accountKey: "some-key",
                }
            }
        );
        ```

These keys can be used as both environment variables or keys in the `storage_options` parameter:

<!-- source: https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html -->

| Key                                   | Description                                                                                      |
|---------------------------------------|--------------------------------------------------------------------------------------------------|
| ``azure_storage_account_name``        | The name of the azure storage account.                                                           |
| ``azure_storage_account_key``         | The serialized service account key.                                                              |
| ``azure_client_id``                   | Service principal client id for authorizing requests.                                            |
| ``azure_client_secret``               | Service principal client secret for authorizing requests.                                        |
| ``azure_tenant_id``                   | Tenant id used in oauth flows.                                                                   |
| ``azure_storage_sas_key``             | Shared access signature. The signature is expected to be percent-encoded, much like they are provided in the azure storage explorer or azure portal. |
| ``azure_storage_token``               | Bearer token.                                                                                    |
| ``azure_storage_use_emulator``        | Use object store with azurite storage emulator.                                                  |
| ``azure_endpoint``                    | Override the endpoint used to communicate with blob storage.                                      |
| ``azure_use_fabric_endpoint``         | Use object store with url scheme account.dfs.fabric.microsoft.com.                               |
| ``azure_msi_endpoint``                | Endpoint to request a imds managed identity token.                                               |
| ``azure_object_id``                   | Object id for use with managed identity authentication.                                          |
| ``azure_msi_resource_id``             | Msi resource id for use with managed identity authentication.                                    |
| ``azure_federated_token_file``        | File containing token for Azure AD workload identity federation.                                 |
| ``azure_use_azure_cli``               | Use azure cli for acquiring access token.                                                        |
| ``azure_disable_tagging``             | Disables tagging objects. This can be desirable if not supported by the backing store.           |

<!-- TODO: demonstrate how to configure networked file systems for optimal performance -->
