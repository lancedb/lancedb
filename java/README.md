# LanceDB Java Enterprise Client

## Configuration and Initialization

### LanceDB Cloud

For LanceDB Cloud, use the simplified builder API:

```java
import com.lancedb.LanceDbNamespaceClientBuilder;
import org.lance.namespace.LanceNamespace;

// If your DB url is db://example-db, then your database here is example-db
LanceNamespace namespaceClient = LanceDbNamespaceClientBuilder.newBuilder()
    .apiKey("your_lancedb_cloud_api_key")
    .database("your_database_name")
    .build();
```

### LanceDB Enterprise

For Enterprise deployments, use your custom endpoint:

```java
LanceNamespace namespaceClient = LanceDbNamespaceClientBuilder.newBuilder()
    .apiKey("your_lancedb_enterprise_api_key")
    .database("your_database_name")
    .endpoint("<your_enterprise_endpoint>")
    .build();
```

## MemWAL LSM write path

Most table operations reach LanceDB through the `LanceNamespace` above, which is
generated from the Lance Namespace specification. The MemWAL LSM routes are not part
of that specification, so they are issued through a separate client:

```java
import com.lancedb.LanceDbRestClient;
import com.lancedb.LanceDbTableLsm;
import com.lancedb.LsmWriteSpec;

LanceDbRestClient client = LanceDbNamespaceClientBuilder.newBuilder()
    .apiKey("your_lancedb_cloud_api_key")
    .database("your_database_name")
    .buildRestClient();

LanceDbTableLsm lsm = new LanceDbTableLsm(client, "my_table");

// Route future merge_insert upserts through the MemWAL, hash-bucketed by `id`.
lsm.setLsmWriteSpec(LsmWriteSpec.bucket("id", 16));

// ... merge_insert traffic ...

// Converge the fresh tier into the base table.
lsm.checkpointLsm();

// Inspect live per-bucket state.
lsm.getLsmStats().ifPresent(stats -> stats.buckets().forEach(bucket ->
    System.out.println(bucket.shardId() + ": " + bucket.generations().size() + " L0 generations")));

client.close();
```

`maintainedIndexes` is tri-state, and the null default is the opposite of what a Java
reader usually expects:

| Value | Meaning |
| --- | --- |
| unset (null) | Maintain **every** index the MemWAL can, resolved on install |
| `Collections.emptyList()` | Maintain **none** |
| `Arrays.asList("id_idx")` | Maintain exactly those |

## Development

Build:

```shell
./mvnw install -pl lancedb-core -am
```

Run tests:

```shell
./mvnw test -pl lancedb-core
```
