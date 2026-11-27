# LanceDB Java Enterprise Client

## Installation

Stable releases of `com.lancedb:lancedb-core` are published to Maven Central.

Beta releases are published to `https://maven.fury.io/lancedb/` instead. They
depend on Lance beta releases, which are published to `https://maven.lance.org`.
To use a beta, add both repositories alongside Maven Central:

```xml
<repositories>
    <repository>
        <id>lancedb</id>
        <url>https://maven.fury.io/lancedb/</url>
    </repository>
    <repository>
        <id>lance</id>
        <url>https://maven.lance.org</url>
    </repository>
</repositories>
```

For Gradle:

```kotlin
repositories {
    mavenCentral()
    maven { url = uri("https://maven.fury.io/lancedb/") }
    maven { url = uri("https://maven.lance.org") }
}
```

Use an explicit version, including the full `-beta.N` suffix. Beta versions are
Maven releases, not `SNAPSHOT` versions.

## Configuration and Initialization

### LanceDB Cloud (legacy)

For LanceDB Cloud, the legacy hosted service, use the simplified builder API:

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

For LanceDB Enterprise deployments, use your custom endpoint:

```java
LanceNamespace namespaceClient = LanceDbNamespaceClientBuilder.newBuilder()
    .apiKey("your_lancedb_enterprise_api_key")
    .database("your_database_name")
    .endpoint("<your_enterprise_endpoint>")
    .build();
```

Calls made on the namespace client from `build()` retry namespace error code 21
(throttling, what Lance maps from HTTP 429 with a namespace error body) on every
such call. A pre-execution HTTP 429 whose body is plain text is retried on that
same budget. Error code 17 (service unavailable, including HTTP 503, connect
failures, and timeouts) is retried on reads only. A 503 without a namespace
error body surfaces as an internal error and is not retried. `Retry-After` is not
available through JNI. The budget is four attempts in total, with base waits of
250 ms, 500 ms, and 1000 ms, plus up to 250 ms of jitter. These retries do
not cover credential refresh or namespace commits that `Dataset.open` makes
through the native handle. The RTE client below keeps its own retry budget
and does not add another one.

## RTE write path

Most table operations reach LanceDB through the `LanceNamespace` above, which is
generated from the Lance Namespace specification. The RTE LSM routes are not part
of that specification, so they are issued through a separate client:

```java
import com.lancedb.LanceDbRestClient;
import com.lancedb.LanceDbTableRTE;
import com.lancedb.RTEWriteSpec;

LanceDbRestClient client = LanceDbNamespaceClientBuilder.newBuilder()
    .apiKey("your_lancedb_cloud_api_key")
    .database("your_database_name")
    .buildRestClient();

LanceDbTableRTE rte = new LanceDbTableRTE(client, "my_table");

// Route future merge_insert upserts through the RTE, hash-bucketed by `id`.
rte.enableRTE(RTEWriteSpec.bucket("id", 16));

// ... merge_insert traffic ...

// Converge the fresh tier into the base table.
rte.checkpointRTE();

// Inspect live per-table-shard state.
rte.getRTEStats().ifPresent(stats -> stats.tableShards().forEach(tableShard ->
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
