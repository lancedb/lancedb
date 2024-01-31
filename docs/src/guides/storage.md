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

=== "JavaScript"

    AWS S3:

    ```javascript
    const lancedb = require("lancedb");
    const db = await lancedb.connect("s3://bucket/path");
    ```

    Google Cloud Storage:

    ```javascript
    const lancedb = require("lancedb");
    const db = await lancedb.connect("gs://bucket/path");
    ```

    Azure Blob Storage:

    ```javascript
    const lancedb = require("lancedb");
    const db = await lancedb.connect("az://bucket/path");
    ```

In most cases, when running in the respective cloud and permissions are set up correctly, no additional configuration is required. When running outside of the respective cloud, authentication credentials must be provided using environment variables. In general, these environment variables are the same as those used by the respective cloud SDKs. The sections below describe the environment variables that can be used to configure each object store.

LanceDB OSS uses the [object-store](https://docs.rs/object_store/latest/object_store/) Rust crate for object store access. There are general environment variables that can be used to configure the object store, such as the request timeout and proxy configuration. See the [object_store ClientConfigKey](https://docs.rs/object_store/latest/object_store/enum.ClientConfigKey.html) doc for available configuration options. The environment variables that can be set are the snake-cased versions of these variable names. For example, to set `ProxyUrl` use the environment variable `PROXY_URL`. (Don't let the Rust docs intimidate you! We link to them so you can see an up-to-date list of the available options.)


### AWS S3

To configure credentials for AWS S3, you can use the `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` environment variables.

Alternatively, if you are using AWS SSO, you can use the `AWS_PROFILE` and `AWS_DEFAULT_REGION` environment variables.

You can see a full list of environment variables [here](https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html#method.from_env).

!!! tip "Automatic cleanup for failed writes"

    LanceDB uses [multi-part uploads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html) when writing data to S3 in order to maximize write speed. LanceDB will abort these uploads when it shuts down gracefully, such as when cancelled by keyboard interrupt. However, in the rare case than LanceDB crashes, it is possible that some data will be left lingering in your account. To cleanup this data, we recommend (as AWS themselves do) to setup a lifecycle rule to delete in progress uploads after 7 days. See the AWS guide:

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
              "s3:DeleteObject"
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

#### S3-compatible stores

LanceDB can also connect to S3-compatible stores, such as MinIO. To do so, you must specify two environment variables: `AWS_ENDPOINT` and `AWS_DEFAULT_REGION`. `AWS_ENDPOINT` should be the URL of the S3-compatible store, and `AWS_DEFAULT_REGION` should be the region to use.

<!-- TODO: we should also document the use of S3 Express once we fully support it -->

### Google Cloud Storage

GCS credentials are configured by setting the `GOOGLE_SERVICE_ACCOUNT` environment variable to the path of a JSON file containing the service account credentials. There are several aliases for this environment variable, documented [here](https://docs.rs/object_store/latest/object_store/gcp/struct.GoogleCloudStorageBuilder.html#method.from_env).


!!! info "HTTP/2 support"

    By default, GCS uses HTTP/1 for communication, as opposed to HTTP/2. This improves maximum throughput significantly. However, if you wish to use HTTP/2 for some reason, you can set the environment variable `HTTP1_ONLY` to `false`.

### Azure Blob Storage

Azure Blob Storage credentials can be configured by setting the `AZURE_STORAGE_ACCOUNT_NAME` and ``AZURE_STORAGE_ACCOUNT_KEY`` environment variables. The full list of environment variables that can be set are documented [here](https://docs.rs/object_store/latest/object_store/azure/struct.MicrosoftAzureBuilder.html#method.from_env).


<!-- TODO: demonstrate how to configure networked file systems for optimal performance -->