# Java SDK

The LanceDB Java SDK provides a convenient way to interact with LanceDB Cloud and Enterprise deployments using the Lance REST Namespace API.

!!! note
    The Java SDK currently only works for LanceDB remote database that connects to LanceDB Cloud and Enterprise.
    Local database support is a work in progress. Check [LANCEDB-2848](https://github.com/lancedb/lancedb/issues/2848) for the latest progress.

## Installation

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.lancedb</groupId>
    <artifactId>lancedb-core</artifactId>
    <version>0.37.0-beta.0</version>
</dependency>
```

## Quick Start

### Connecting to LanceDB Cloud

```java
import com.lancedb.LanceDbNamespaceClientBuilder;
import org.lance.namespace.LanceNamespace;

// If your DB url is db://example-db, then your database here is example-db
LanceNamespace namespaceClient = LanceDbNamespaceClientBuilder.newBuilder()
    .apiKey("your_lancedb_cloud_api_key")
    .database("your_database_name")
    .build();
```

### Connecting to LanceDB Enterprise

For LanceDB Enterprise deployments with a custom endpoint:

```java
LanceNamespace namespaceClient = LanceDbNamespaceClientBuilder.newBuilder()
    .apiKey("your_lancedb_enterprise_api_key")
    .database("your_database_name")
    .endpoint("<your_enterprise_endpoint>")
    .build();
```

### Configuration Options

| Method | Description | Required |
|--------|-------------|----------|
| `apiKey(String)` | LanceDB API key | Yes |
| `database(String)` | Database name | Yes |
| `endpoint(String)` | Custom endpoint URL for Enterprise deployments | No |
| `region(String)` | AWS region (default: "us-east-1") | No |
| `config(String, String)` | Additional configuration parameters | No |

## Metadata Operations

### Creating a Namespace Path

Namespace paths organize tables hierarchically. Create the desired namespace path before creating tables within it:

```java
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateNamespaceResponse;

// Create a child namespace path
CreateNamespaceRequest request = new CreateNamespaceRequest();
request.setId(Arrays.asList("my_namespace"));

CreateNamespaceResponse response = namespaceClient.createNamespace(request);
```

You can also create nested namespace paths:

```java
// Create a nested namespace path: parent/child
CreateNamespaceRequest request = new CreateNamespaceRequest();
request.setId(Arrays.asList("parent_namespace", "child_namespace"));

CreateNamespaceResponse response = namespaceClient.createNamespace(request);
```

### Describing a Namespace Path

```java
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;

DescribeNamespaceRequest request = new DescribeNamespaceRequest();
request.setId(Arrays.asList("my_namespace"));

DescribeNamespaceResponse response = namespaceClient.describeNamespace(request);
System.out.println("Namespace properties: " + response.getProperties());
```

### Listing Namespace Paths

```java
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;

// List all namespace paths at the root level
ListNamespacesRequest request = new ListNamespacesRequest();
request.setId(Arrays.asList());  // Empty for root

ListNamespacesResponse response = namespaceClient.listNamespaces(request);
for (String ns : response.getNamespaces()) {
    System.out.println("Namespace path: " + ns);
}

// List child namespace paths under a parent path
ListNamespacesRequest childRequest = new ListNamespacesRequest();
childRequest.setId(Arrays.asList("parent_namespace"));

ListNamespacesResponse childResponse = namespaceClient.listNamespaces(childRequest);
```

### Listing Tables

```java
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;

// List tables in a namespace path
ListTablesRequest request = new ListTablesRequest();
request.setId(Arrays.asList("my_namespace"));

ListTablesResponse response = namespaceClient.listTables(request);
for (String table : response.getTables()) {
    System.out.println("Table: " + table);
}
```

### Dropping a Namespace Path

```java
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropNamespaceResponse;

DropNamespaceRequest request = new DropNamespaceRequest();
request.setId(Arrays.asList("my_namespace"));

DropNamespaceResponse response = namespaceClient.dropNamespace(request);
```

### Describing a Table

```java
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;

DescribeTableRequest request = new DescribeTableRequest();
request.setId(Arrays.asList("my_namespace", "my_table"));

DescribeTableResponse response = namespaceClient.describeTable(request);
System.out.println("Table version: " + response.getVersion());
System.out.println("Schema fields: " + response.getSchema().getFields());
```

### Dropping a Table

```java
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.DropTableResponse;

DropTableRequest request = new DropTableRequest();
request.setId(Arrays.asList("my_namespace", "my_table"));

DropTableResponse response = namespaceClient.dropTable(request);
```

## Writing Data

### Creating a Table

Tables are created within a namespace path by providing data in Apache Arrow IPC format:

```java
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateTableRequest;
import org.lance.namespace.model.CreateTableResponse;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.Arrays;

// Create schema with id, name, and embedding fields
Schema schema = new Schema(Arrays.asList(
    new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
    new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
    new Field("embedding",
        FieldType.nullable(new ArrowType.FixedSizeList(128)),
        Arrays.asList(new Field("item",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            null)))
));

try (BufferAllocator allocator = new RootAllocator();
     VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

    // Populate data
    root.setRowCount(3);
    IntVector idVector = (IntVector) root.getVector("id");
    VarCharVector nameVector = (VarCharVector) root.getVector("name");
    FixedSizeListVector embeddingVector = (FixedSizeListVector) root.getVector("embedding");
    Float4Vector embeddingData = (Float4Vector) embeddingVector.getDataVector();

    for (int i = 0; i < 3; i++) {
        idVector.setSafe(i, i + 1);
        nameVector.setSafe(i, ("item_" + i).getBytes());
        embeddingVector.setNotNull(i);
        for (int j = 0; j < 128; j++) {
            embeddingData.setSafe(i * 128 + j, (float) i);
        }
    }
    idVector.setValueCount(3);
    nameVector.setValueCount(3);
    embeddingData.setValueCount(3 * 128);
    embeddingVector.setValueCount(3);

    // Serialize to Arrow IPC format
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
        writer.start();
        writer.writeBatch();
        writer.end();
    }
    byte[] tableData = out.toByteArray();

    // Create a table in a namespace path
    CreateTableRequest request = new CreateTableRequest();
    request.setId(Arrays.asList("my_namespace", "my_table"));
    CreateTableResponse response = namespaceClient.createTable(request, tableData);
}
```

### Creating an Empty Table

To create an empty table, send an Arrow IPC stream that contains the table schema and no record batches.
The schema in the IPC stream becomes the table schema, and rows can be inserted later.

```java
import org.lance.namespace.model.CreateTableRequest;
import org.lance.namespace.model.CreateTableResponse;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.Arrays;

Schema schema = new Schema(Arrays.asList(
    new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
    new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
    new Field("embedding",
        FieldType.nullable(new ArrowType.FixedSizeList(128)),
        Arrays.asList(new Field("item",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            null)))
));

byte[] emptyTableData;
try (BufferAllocator allocator = new RootAllocator();
     VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
    root.setRowCount(0);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
        writer.start();
        writer.end();
    }
    emptyTableData = out.toByteArray();
}

CreateTableRequest request = new CreateTableRequest();
request.setId(Arrays.asList("my_namespace", "empty_table"));

CreateTableResponse response = namespaceClient.createTable(request, emptyTableData);
```

### Insert

```java
import org.lance.namespace.model.InsertIntoTableRequest;
import org.lance.namespace.model.InsertIntoTableResponse;

// Prepare data in Arrow IPC format (similar to create table example)
byte[] insertData = prepareArrowData();

InsertIntoTableRequest request = new InsertIntoTableRequest();
request.setId(Arrays.asList("my_namespace", "my_table"));
request.setMode(InsertIntoTableRequest.ModeEnum.APPEND);

InsertIntoTableResponse response = namespaceClient.insertIntoTable(request, insertData);
System.out.println("New version: " + response.getVersion());
```

### Update

Update rows matching a predicate condition:

```java
import org.lance.namespace.model.UpdateTableRequest;
import org.lance.namespace.model.UpdateTableResponse;

UpdateTableRequest request = new UpdateTableRequest();
request.setId(Arrays.asList("my_namespace", "my_table"));

// Predicate to select rows to update
request.setPredicate("id = 1");

// Set new values using SQL expressions as [column_name, expression] pairs
request.setUpdates(Arrays.asList(
    Arrays.asList("name", "'updated_name'")
));

UpdateTableResponse response = namespaceClient.updateTable(request);
System.out.println("Updated rows: " + response.getUpdatedRows());
```

### Delete

Delete rows matching a predicate condition:

```java
import org.lance.namespace.model.DeleteFromTableRequest;
import org.lance.namespace.model.DeleteFromTableResponse;

DeleteFromTableRequest request = new DeleteFromTableRequest();
request.setId(Arrays.asList("my_namespace", "my_table"));

// Predicate to select rows to delete
request.setPredicate("id > 100");

DeleteFromTableResponse response = namespaceClient.deleteFromTable(request);
System.out.println("New version: " + response.getVersion());
```

### Merge Insert (Upsert)

Merge insert allows you to update existing rows and insert new rows in a single operation based on a key column:

```java
import org.lance.namespace.model.MergeInsertIntoTableRequest;
import org.lance.namespace.model.MergeInsertIntoTableResponse;

// Prepare data with rows to update (id=2,3) and new rows (id=4)
byte[] mergeData = prepareArrowData();  // Contains rows with id=2,3,4

MergeInsertIntoTableRequest request = new MergeInsertIntoTableRequest();
request.setId(Arrays.asList("my_namespace", "my_table"));

// Match on the "id" column
request.setOn("id");

// Update all columns when a matching row is found
request.setWhenMatchedUpdateAll(true);

// Insert new rows when no match is found
request.setWhenNotMatchedInsertAll(true);

MergeInsertIntoTableResponse response = namespaceClient.mergeInsertIntoTable(request, mergeData);

System.out.println("Updated rows: " + response.getNumUpdatedRows());
System.out.println("Inserted rows: " + response.getNumInsertedRows());
```

## Querying Data

### Counting Rows

```java
import org.lance.namespace.model.CountTableRowsRequest;

CountTableRowsRequest request = new CountTableRowsRequest();
request.setId(Arrays.asList("my_namespace", "my_table"));

Long rowCount = namespaceClient.countTableRows(request);
System.out.println("Row count: " + rowCount);
```

### Vector Search

```java
import org.lance.namespace.model.QueryTableRequest;
import org.lance.namespace.model.QueryTableRequestVector;

QueryTableRequest query = new QueryTableRequest();
query.setId(Arrays.asList("my_namespace", "my_table"));
query.setK(10);  // Return top 10 results

// Set the query vector
List<Float> queryVector = new ArrayList<>();
for (int i = 0; i < 128; i++) {
    queryVector.add(1.0f);
}
QueryTableRequestVector vector = new QueryTableRequestVector();
vector.setSingleVector(queryVector);
query.setVector(vector);

// Specify columns to return
query.setColumns(Arrays.asList("id", "name", "embedding"));

// Execute query - returns Arrow IPC format
byte[] result = namespaceClient.queryTable(query);
```

### Full Text Search

```java
import org.lance.namespace.model.QueryTableRequest;
import org.lance.namespace.model.QueryTableRequestFullTextQuery;
import org.lance.namespace.model.StringFtsQuery;

QueryTableRequest query = new QueryTableRequest();
query.setId(Arrays.asList("my_namespace", "my_table"));
query.setK(10);

// Set full text search query
StringFtsQuery stringQuery = new StringFtsQuery();
stringQuery.setQuery("search terms");
stringQuery.setColumns(Arrays.asList("text_column"));

QueryTableRequestFullTextQuery fts = new QueryTableRequestFullTextQuery();
fts.setStringQuery(stringQuery);
query.setFullTextQuery(fts);

// Specify columns to return
query.setColumns(Arrays.asList("id", "text_column"));

byte[] result = namespaceClient.queryTable(query);
```

### Query with Filter

```java
QueryTableRequest query = new QueryTableRequest();
query.setId(Arrays.asList("my_namespace", "my_table"));
query.setK(10);
query.setFilter("id > 50");
query.setColumns(Arrays.asList("id", "name"));

byte[] result = namespaceClient.queryTable(query);
```

### Query with Prefilter

```java
QueryTableRequest query = new QueryTableRequest();
query.setId(Arrays.asList("my_namespace", "my_table"));
query.setK(5);
query.setPrefilter(true);  // Apply filter before vector search
query.setFilter("category = 'electronics'");

// Set query vector
QueryTableRequestVector vector = new QueryTableRequestVector();
vector.setSingleVector(queryVector);
query.setVector(vector);

byte[] result = namespaceClient.queryTable(query);
```

## Indexing

The Java SDK exposes the REST namespace index operations through the same `LanceNamespace` client.
Index creation runs asynchronously, so use `listTableIndices` or `describeTableIndexStats` to check progress.

### Creating a Vector Index

```java
import org.lance.namespace.model.CreateTableIndexRequest;
import org.lance.namespace.model.CreateTableIndexResponse;

CreateTableIndexRequest request = new CreateTableIndexRequest();
request.setId(Arrays.asList("my_namespace", "my_table"));
request.setColumn("embedding");
request.setIndexType("IVF_PQ");
request.setDistanceType("cosine");
request.setName("embedding_idx");

CreateTableIndexResponse response = namespaceClient.createTableIndex(request);
System.out.println("Index transaction: " + response.getTransactionId());
```

### Creating a Scalar Index

```java
import org.lance.namespace.model.CreateTableIndexRequest;
import org.lance.namespace.model.CreateTableScalarIndexResponse;

CreateTableIndexRequest request = new CreateTableIndexRequest();
request.setId(Arrays.asList("my_namespace", "my_table"));
request.setColumn("category");
request.setIndexType("BTREE");
request.setName("category_idx");

CreateTableScalarIndexResponse response = namespaceClient.createTableScalarIndex(request);
System.out.println("Index transaction: " + response.getTransactionId());
```

### Creating a Full Text Search Index

```java
import org.lance.namespace.model.CreateTableIndexRequest;
import org.lance.namespace.model.CreateTableScalarIndexResponse;

CreateTableIndexRequest request = new CreateTableIndexRequest();
request.setId(Arrays.asList("my_namespace", "my_table"));
request.setColumn("text_column");
request.setIndexType("FTS");
request.setName("text_idx");
request.setBaseTokenizer("simple");
request.setLowerCase(true);
request.setWithPosition(true);

CreateTableScalarIndexResponse response = namespaceClient.createTableScalarIndex(request);
System.out.println("Index transaction: " + response.getTransactionId());
```

### Custom FTS Stop Words

`LanceDbFtsIndexRequest` accepts either an inline list or a newline-delimited
UTF-8 file on the Java client. The client resolves the source to a concrete
snapshot before the request is sent; a client-local path is never sent to
LanceDB Cloud or Enterprise.

Custom stop words replace the built-in list for `language` and are only applied
when `removeStopWords` is true. `null` keeps the built-in language list, while
an empty list explicitly replaces it with no stop words. Exact empty strings
are ignored and exact duplicates retain their first occurrence. Other content,
including case and surrounding whitespace, is preserved.

```java
import com.lancedb.LanceDbFtsIndexClient;
import com.lancedb.LanceDbFtsIndexRequest;
import com.lancedb.LanceDbNamespaceClientBuilder;
import org.lance.namespace.model.CreateTableIndexResponse;

import java.util.Arrays;

public final class CustomStopWordsExample {
    public static void main(String[] args) throws Exception {
        LanceDbNamespaceClientBuilder clientBuilder =
            LanceDbNamespaceClientBuilder.newBuilder()
                .apiKey("your_lancedb_api_key")
                .database("your_database_name");

        LanceDbFtsIndexRequest request = new LanceDbFtsIndexRequest();
        request.setId(Arrays.asList("my_namespace", "my_table"));
        request.setColumn("text_column");
        request.setName("text_idx");
        request.setRemoveStopWords(true);
        request.setCustomStopWords(Arrays.asList("copyright", "reserved"));

        try (LanceDbFtsIndexClient indexClient = clientBuilder.buildFtsIndexClient()) {
            CreateTableIndexResponse response = indexClient.createTableIndex(request);
            System.out.println("Index transaction: " + response.getTransactionId());
        }
    }
}
```

To read the snapshot from a file instead, configure the file source before
calling `createTableIndex`:

```java
import java.nio.file.Paths;

request.setCustomStopWordsFile(Paths.get("./stop-words.txt"));
```

The file is decoded as strict UTF-8 and read on the first
`createTableIndex` call. Lines use LF or CRLF terminators; a lone carriage
return is preserved as part of the stop word. The resolved list is then
retained on the request, so a retry uses the same snapshot even if the file
changes.

!!! warning "Use the dedicated FTS index client for custom stop words"
    Do not pass `LanceDbFtsIndexRequest` to
    `namespaceClient.createTableIndex` or
    `namespaceClient.createTableScalarIndex`. The current generated Lance
    Namespace model has no `custom_stop_words` field, and its JNI boundary would
    silently discard the extra JSON property. `LanceDbFtsIndexClient` uses the
    official HTTP client directly and preserves the resolved snapshot.

    The helper accepts `endpoint`, API key, database, `delimiter`, and
    `header.*`/`headers.*` builder configuration. It rejects transport settings
    it cannot safely reproduce, such as `tls.*`; callers needing a custom TLS
    stack can construct it with a preconfigured Namespace Apache `ApiClient`.
    It also rejects the `uri`, `header.x-api-key`, and
    `header.x-lancedb-database` configuration aliases instead of silently
    overriding them; use `endpoint()`, `apiKey()`, and `database()`,
    respectively.

!!! note "Java table and tokenization APIs"
    The current Java SDK is a remote Namespace client and has no embedded/local
    LanceDB `Table` API. It therefore cannot prove a complete table-column
    snapshot and intentionally does not accept remote table references as a
    stop-word source. Materialize those words into an inline list or a local
    UTF-8 file first.

    Java also has no public standalone tokenization API today, so there is no
    Java tokenization entry point to extend with custom stop words.

### Listing Indexes

```java
import org.lance.namespace.model.IndexContent;
import org.lance.namespace.model.ListTableIndicesRequest;
import org.lance.namespace.model.ListTableIndicesResponse;

ListTableIndicesRequest request = new ListTableIndicesRequest();
request.setId(Arrays.asList("my_namespace", "my_table"));

ListTableIndicesResponse response = namespaceClient.listTableIndices(request);
for (IndexContent index : response.getIndexes()) {
    System.out.println(index.getIndexName() + ": " + index.getStatus());
}
```

!!! note
    The current Java namespace API exposes index type, index name, distance type,
    and the generated FTS fields `withPosition`, `baseTokenizer`, `language`,
    `maxTokenLength`, `lowerCase`, `stem`, `removeStopWords`, and
    `asciiFolding`. `LanceDbFtsIndexRequest` adds the custom stop-word snapshot
    while the upstream generated request catches up. IVF training parameters
    such as `num_partitions` are not exposed by `CreateTableIndexRequest` yet.
    To make those configurable from Java, the namespace API must add those
    fields first.

## Reading Query Results

Query results are returned as bytes in Apache Arrow IPC file format. Put the byte-channel
adapter behind a small helper so query code can work with `ArrowFileReader` directly:

```java
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

final class ArrowIpc {
    static ArrowFileReader openFileReader(byte[] data, BufferAllocator allocator) throws IOException {
        return new ArrowFileReader(new ByteArraySeekableByteChannel(data), allocator);
    }

    private static final class ByteArraySeekableByteChannel implements SeekableByteChannel {
        private final byte[] data;
        private long position = 0;
        private boolean isOpen = true;

        private ByteArraySeekableByteChannel(byte[] data) {
            this.data = data;
        }

        @Override
        public int read(ByteBuffer dst) {
            int remaining = dst.remaining();
            int available = (int) (data.length - position);
            if (available <= 0) return -1;
            int toRead = Math.min(remaining, available);
            dst.put(data, (int) position, toRead);
            position += toRead;
            return toRead;
        }

        @Override public long position() { return position; }
        @Override public SeekableByteChannel position(long newPosition) { position = newPosition; return this; }
        @Override public long size() { return data.length; }
        @Override public boolean isOpen() { return isOpen; }
        @Override public void close() { isOpen = false; }
        @Override public int write(ByteBuffer src) { throw new UnsupportedOperationException(); }
        @Override public SeekableByteChannel truncate(long size) { throw new UnsupportedOperationException(); }
    }
}

// Read query results
byte[] queryResult = namespaceClient.queryTable(query);

try (BufferAllocator allocator = new RootAllocator();
     ArrowFileReader reader = ArrowIpc.openFileReader(queryResult, allocator)) {

    for (int i = 0; i < reader.getRecordBlocks().size(); i++) {
        reader.loadRecordBatch(reader.getRecordBlocks().get(i));
        VectorSchemaRoot root = reader.getVectorSchemaRoot();

        // Access data
        IntVector idVector = (IntVector) root.getVector("id");
        VarCharVector nameVector = (VarCharVector) root.getVector("name");

        for (int row = 0; row < root.getRowCount(); row++) {
            int id = idVector.get(row);
            String name = new String(nameVector.get(row));
            System.out.println("Row " + row + ": id=" + id + ", name=" + name);
        }
    }
}
```
