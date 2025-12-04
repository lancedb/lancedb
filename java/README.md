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

For Enterprise deployments, use your custom endpoint:

```java
RestNamespace namespace = LanceDbRestNamespaceBuilder.newBuilder()
    .apiKey("your_lancedb_enterprise_api_key")
    .database("your_database_name")
    .endpoint("<your_enterprise_endpoint>")
    .build();
```

## Development

Build:

```shell
./mvnw install -pl lancedb-core -am
```

Run tests:

```shell
./mvnw test -pl lancedb-core
```
