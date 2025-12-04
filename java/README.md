# LanceDB Java SDK

## Configuration and Initialization

### LanceDB Cloud

For LanceDB Cloud, use the simplified builder API:

```java
import com.lancedb.LanceDbRestNamespaceBuilder;
import org.lance.namespace.RestNamespace;

// If your DB url is db://example-db, then your database here is example-db
RestNamespace namespace = LanceDbRestNamespaceBuilder.newBuilder()
    .apiKey("your_lancedb_cloud_api_key")
    .database("your_database_name")
    .build();
```

### LanceDB Enterprise

For Enterprise deployments, use your VPC endpoint:

```java
RestNamespace namespace = LanceDbRestNamespaceBuilder.newBuilder()
    .apiKey("your_lancedb_enterprise_api_key")
    .database("your_database_name")
    .endpoint("http://<vpc_endpoint_dns_name>:80")
    .build();
```

## Development

Build:

```shell
./mvnw install -pl lancedb-core -am
```

Run tests (requires LANCEDB_DB and LANCEDB_API_KEY environment variables):

```shell
./mvnw test -pl lancedb-core -P integration-tests
```
