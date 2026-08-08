## Capabilities

Depending on its configuration and the backing system, this service can expose:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [x] copy
- [ ] rename
- [x] presign

Inspect the effective capability set with [`opendal_core::Operator::info`] and
[`opendal_core::OperatorInfo::capability`] after building an operator.

## Configuration

Use [`crate::S3Config`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

## Temporary security credentials

OpenDAL now provides support for S3 temporary security credentials in IAM.

The way to take advantage of this feature is to build your S3 backend with `Builder::session_token`.

But OpenDAL will not refresh the temporary security credentials, please keep in mind to refresh those credentials in time.

## Server Side Encryption

OpenDAL provides full support of S3 Server Side Encryption(SSE) features.

The easiest way to configure them is to use helper functions like

- SSE-KMS: `server_side_encryption_with_aws_managed_kms_key`
- SSE-KMS: `server_side_encryption_with_customer_managed_kms_key`
- SSE-S3: `server_side_encryption_with_s3_key`
- SSE-C: `server_side_encryption_with_customer_key`

If those functions don't fulfill need, low-level options are also provided:

- Use service managed kms key
  - `server_side_encryption="aws:kms"`
- Use customer provided kms key
  - `server_side_encryption="aws:kms"`
  - `server_side_encryption_aws_kms_key_id="your-kms-key"`
- Use S3 managed key
  - `server_side_encryption="AES256"`
- Use customer key
  - `server_side_encryption_customer_algorithm="AES256"`
  - `server_side_encryption_customer_key="base64-of-your-aes256-key"`
  - `server_side_encryption_customer_key_md5="base64-of-your-aes256-key-md5"`

After SSE have been configured, all requests send by this backed will attach those headers.

Reference: [Protecting data using server-side encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/serv-side-encryption.html)

## Example

## Via Builder

### Basic Setup

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_s3::S3;

#[tokio::main]
async fn main() -> Result<()> {
    // Create s3 backend builder.
    let mut builder = S3::default()
      // Set the root for s3, all operations will happen under this root.
      //
      // NOTE: the root must be absolute path.
      .root("/path/to/dir")
      // Set the bucket name. This is required.
      .bucket("test")
      // Set the region. This is required for some services, if you don't care about it, for example Minio service, just set it to "auto", it will be ignored.
      .region("us-east-1")
      // Set the endpoint.
      //
      // For examples:
      // - "https://s3.amazonaws.com"
      // - "http://127.0.0.1:9000"
      // - "https://oss-ap-northeast-1.aliyuncs.com"
      // - "https://cos.ap-seoul.myqcloud.com"
      //
      // Default to "https://s3.amazonaws.com"
      .endpoint("https://s3.amazonaws.com")
      // Set the access_key_id and secret_access_key.
      //
      // OpenDAL will try load credential from the env.
      // If credential not set and no valid credential in env, OpenDAL will
      // send request without signing like anonymous user.
      .access_key_id("access_key_id")
      .secret_access_key("secret_access_key");

    let op: Operator = Operator::new(builder)?;

    Ok(())
}

```

### S3 with SSE-C

```rust,no_run
use log::info;
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_s3::S3;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = S3::default()
      .root("/path/to/dir")
      .bucket("test")
      .region("us-east-1")
      .endpoint("https://s3.amazonaws.com")
      .access_key_id("access_key_id")
      .secret_access_key("secret_access_key")
      // Enable SSE-C
      .server_side_encryption_with_customer_key("AES256", "customer_key".as_bytes());

    let op = Operator::new(builder)?;
    info!("operator: {:?}", op);

    // Writing your testing code here.

    Ok(())
}
```

### S3 with SSE-KMS and aws managed kms key

```rust,no_run
use log::info;
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_s3::S3;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = S3::default()
      // Setup builders
      .root("/path/to/dir")
      .bucket("test")
      .region("us-east-1")
      .endpoint("https://s3.amazonaws.com")
      .access_key_id("access_key_id")
      .secret_access_key("secret_access_key")
      // Enable SSE-KMS with aws managed kms key
      .server_side_encryption_with_aws_managed_kms_key();

    let op = Operator::new(builder)?;
    info!("operator: {:?}", op);

    // Writing your testing code here.

    Ok(())
}
```

### S3 with SSE-KMS and customer managed kms key

```rust,no_run
use log::info;
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_s3::S3;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = S3::default()
      // Setup builders
      .root("/path/to/dir")
      .bucket("test")
      .region("us-east-1")
      .endpoint("https://s3.amazonaws.com")
      .access_key_id("access_key_id")
      .secret_access_key("secret_access_key")
      // Enable SSE-KMS with customer managed kms key
      .server_side_encryption_with_customer_managed_kms_key("aws_kms_key_id");

    let op = Operator::new(builder)?;
    info!("operator: {:?}", op);

    // Writing your testing code here.

    Ok(())
}
```

### S3 with SSE-S3

```rust,no_run
use log::info;
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_s3::S3;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = S3::default()
      // Setup builders
      .root("/path/to/dir")
      .bucket("test")
      .region("us-east-1")
      .endpoint("https://s3.amazonaws.com")
      .access_key_id("access_key_id")
      .secret_access_key("secret_access_key")
      // Enable SSE-S3
      .server_side_encryption_with_s3_key();

    let op = Operator::new(builder)?;
    info!("operator: {:?}", op);

    // Writing your testing code here.

    Ok(())
}
```

### S3 with default ACL

```rust,no_run
use log::info;
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_s3::S3;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = S3::default()
      // Setup builders
      .root("/path/to/dir")
      .bucket("test")
      .region("us-east-1")
      .endpoint("https://s3.amazonaws.com")
      .access_key_id("access_key_id")
      .secret_access_key("secret_access_key")
      // Enable public-read ACL
      .default_acl("public-read");

    let op = Operator::new(builder)?;
    info!("operator: {:?}", op);

    // New objects will be created with public-read ACL

    Ok(())
}
```
