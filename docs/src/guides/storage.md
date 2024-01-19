# Configuring storage

When using LanceDB OSS, you can choose where to store your data. The tradeoffs between different storage options are discussed in the [storage concepts guide](../concepts/storage.md). This guide shows how to configure LanceDB to use different storage options.

## Object Stores

LanceDB OSS supports object stores such as AWS S3 (and compatible stores), Azure Blob Store, and Google Cloud Storage. Which object store to use is determined by the URI scheme of the dataset path. For example, `s3://bucket/path` will use S3, `az://bucket/path` will use Azure, and `gs://bucket/path` will use GCS. In most cases, when running in the respective cloud and permissions are set up correctly, no additional configuration is required.

LanceDB OSS uses the [object-store](https://docs.rs/object_store/latest/object_store/) Rust crate for object store access. There are general environment variables that can be used to configure the object store, such as the request timeout and proxy configuration. See the [object_store ClientConfigKey](https://docs.rs/object_store/latest/object_store/enum.ClientConfigKey.html) doc for available configuration options. (The environment variables that can be set are the snake-cased versions of these variable names. For example, to set `ProxyUrl` use the environment variable `PROXY_URL`.)


### AWS S3

To configure credentials for AWS S3, you can use the `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` environment variables.

Alternatively, if you are using AWS SSO, you can use the `AWS_PROFILE` and `AWS_DEFAULT_REGION` environment variables.

You can see a full list of environment variables [here](https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html#method.from_env).

#### S3-compatible stores

Lance can also connect to S3-compatible stores, such as MinIO. To do so, you must specify two environment variables: `AWS_ENDPOINT` and `AWS_DEFAULT_REGION`. `AWS_ENDPOINT` should be the URL of the S3-compatible store, and `AWS_DEFAULT_REGION` should be the region to use.

<!-- TODO: we should also document the use of S3 Express once we fully support it -->

### Google Cloud Storage

GCS credentials are configured by setting the `GOOGLE_SERVICE_ACCOUNT` environment variable to the path of a JSON file containing the service account credentials. There are several aliases for this environment variable, documented [here](https://docs.rs/object_store/latest/object_store/gcp/struct.GoogleCloudStorageBuilder.html#method.from_env).


!!! info "HTTP/2 support"

    By default, GCS uses HTTP/1 for communication, as opposed to HTTP/2. This improves maximum throughput significantly. However, if you wish to use HTTP/2 for some reason, you can set the environment variable `HTTP1_ONLY` to `false`.

### Azure Blob Storage

Azure Blob Storage credentials can be configured by setting the `AZURE_STORAGE_ACCOUNT_NAME` and ``AZURE_STORAGE_ACCOUNT_KEY`` environment variables. The full list of environment variables that can be set are documented [here](https://docs.rs/object_store/latest/object_store/azure/struct.MicrosoftAzureBuilder.html#method.from_env).
