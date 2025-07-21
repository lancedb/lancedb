# LanceDB Java SDK

## Configuration and Initialization

### LanceDB Cloud

For LanceDB Cloud, use the simplified builder API:

```java
import com.lancedb.lance.namespace.LanceRestNamespace;

// If your DB url is db://example-db, then your database here is example-db
LanceRestNamespace namespace = LanceDBRestNamespaceBuilder.builder()
    .apiKey("your_lancedb_cloud_api_key")
    .database("your_database_name")
    .build();
```

### LanceDB Enterprise

For Enterprise deployments, use your VPC endpoint:

```java
LanceRestNamespace namespace = LanceDBRestNamespaceBuilder.builder()
    .apiKey("your_lancedb_enterprise_api_key")
    .database("your-top-dir") // Your top level folder under your cloud bucket, e.g. s3://your-bucket/your-top-dir/
    .hostOverride("http://<vpc_endpoint_dns_name>:80")
    .build();
```

## Development

Build:

```shell
./mvnw install
```