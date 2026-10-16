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

## MemWAL write path

Most table operations reach LanceDB through the `LanceNamespace` above, which is
generated from the Lance Namespace specification. The MemWAL LSM routes are not part
of that specification, so they are issued through a separate client:

```java
import com.lancedb.LanceDbRestClient;
import com.lancedb.LanceDbTableMemWal;
import com.lancedb.MemWalWriteSpec;

LanceDbRestClient client = LanceDbNamespaceClientBuilder.newBuilder()
    .apiKey("your_lancedb_cloud_api_key")
    .database("your_database_name")
    .buildRestClient();

LanceDbTableMemWal memWal = new LanceDbTableMemWal(client, "my_table");

// Route future merge_insert upserts through the MemWAL, hash-bucketed by `id`.
memWal.enableMemWal(MemWalWriteSpec.bucket("id", 16));

// ... merge_insert traffic ...

// Converge the fresh tier into the base table.
memWal.checkpointMemWal();

// Inspect live per-table-shard state.
memWal.getMemWalStats().ifPresent(stats -> stats.tableShards().forEach(tableShard ->
    System.out.println(tableShard.shardId() + ": " + tableShard.sstables().size() + " SSTables")));

client.close();
```

`maintainedIndexes` is tri-state, and the null default is the opposite of what a Java
reader usually expects:

| Value | Meaning |
| --- | --- |
| unset (null) | Maintain **every** index the table has, including ones created later |
| `Collections.emptyList()` | Maintain **none** |
| `Arrays.asList("id_idx")` | Maintain exactly those |

## Development

The parent POM includes `https://maven.lance.org` to resolve Lance beta and RC
dependencies. Use an explicit `lance-core.version`; numbered beta and RC versions
are Maven releases, not snapshots. Maven Central remains available for stable
and historical versions, as well as other dependencies.

Build:

```shell
./mvnw install -pl lancedb-core -am
```

Run tests:

```shell
./mvnw test -pl lancedb-core
```
