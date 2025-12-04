# Java SDK

The LanceDB Java SDK provides a convenient way to interact with LanceDB Cloud and Enterprise deployments using the Lance REST Namespace API.

## Installation

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.lancedb</groupId>
    <artifactId>lancedb-core</artifactId>
    <version>0.22.4-beta.3</version>
</dependency>
```

## Quick Start

### Connecting to LanceDB Cloud

```java
import com.lancedb.LanceDbRestNamespaceBuilder;
import org.lance.namespace.RestNamespace;

// If your DB url is db://example-db, then your database here is example-db
RestNamespace namespace = LanceDbRestNamespaceBuilder.newBuilder()
    .apiKey("your_lancedb_cloud_api_key")
    .database("your_database_name")
    .build();
```

### Connecting to LanceDB Enterprise

For LanceDB Enterprise deployments with a custom VPC endpoint:

```java
RestNamespace namespace = LanceDbRestNamespaceBuilder.newBuilder()
    .apiKey("your_lancedb_enterprise_api_key")
    .database("your_database_name")
    .endpoint("http://<vpc_endpoint_dns_name>:80")
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

### Creating a Namespace

Namespaces organize tables hierarchically. Create a namespace before creating tables within it:

```java
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateNamespaceResponse;

// Create a child namespace
CreateNamespaceRequest request = new CreateNamespaceRequest();
request.setId(Arrays.asList("my_namespace"));

CreateNamespaceResponse response = namespace.createNamespace(request);
```

You can also create nested namespaces:

```java
// Create a nested namespace: parent/child
CreateNamespaceRequest request = new CreateNamespaceRequest();
request.setId(Arrays.asList("parent_namespace", "child_namespace"));

CreateNamespaceResponse response = namespace.createNamespace(request);
```

### Describing a Namespace

```java
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;

DescribeNamespaceRequest request = new DescribeNamespaceRequest();
request.setId(Arrays.asList("my_namespace"));

DescribeNamespaceResponse response = namespace.describeNamespace(request);
System.out.println("Namespace properties: " + response.getProperties());
```

### Listing Namespaces

```java
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;

// List all namespaces at root level
ListNamespacesRequest request = new ListNamespacesRequest();
request.setId(Arrays.asList());  // Empty for root

ListNamespacesResponse response = namespace.listNamespaces(request);
for (String ns : response.getNamespaces()) {
    System.out.println("Namespace: " + ns);
}

// List child namespaces under a parent
ListNamespacesRequest childRequest = new ListNamespacesRequest();
childRequest.setId(Arrays.asList("parent_namespace"));

ListNamespacesResponse childResponse = namespace.listNamespaces(childRequest);
```

### Listing Tables

```java
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;

// List tables in a namespace
ListTablesRequest request = new ListTablesRequest();
request.setId(Arrays.asList("my_namespace"));

ListTablesResponse response = namespace.listTables(request);
for (String table : response.getTables()) {
    System.out.println("Table: " + table);
}
```

### Dropping a Namespace

```java
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropNamespaceResponse;

DropNamespaceRequest request = new DropNamespaceRequest();
request.setId(Arrays.asList("my_namespace"));

DropNamespaceResponse response = namespace.dropNamespace(request);
```

### Describing a Table

```java
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;

DescribeTableRequest request = new DescribeTableRequest();
request.setId(Arrays.asList("my_namespace", "my_table"));

DescribeTableResponse response = namespace.describeTable(request);
System.out.println("Table version: " + response.getVersion());
System.out.println("Schema fields: " + response.getSchema().getFields());
```

### Dropping a Table

```java
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.DropTableResponse;

DropTableRequest request = new DropTableRequest();
request.setId(Arrays.asList("my_namespace", "my_table"));

DropTableResponse response = namespace.dropTable(request);
```

## Writing Data

### Creating a Table

Tables are created within a namespace by providing data in Apache Arrow IPC format:

```java
import org.lance.namespace.RestNamespace;
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

    // Create table in a namespace
    CreateTableRequest request = new CreateTableRequest();
    request.setId(Arrays.asList("my_namespace", "my_table"));
    CreateTableResponse response = namespace.createTable(request, tableData);
}
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

InsertIntoTableResponse response = namespace.insertIntoTable(request, insertData);
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

UpdateTableResponse response = namespace.updateTable(request);
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

DeleteFromTableResponse response = namespace.deleteFromTable(request);
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

MergeInsertIntoTableResponse response = namespace.mergeInsertIntoTable(request, mergeData);

System.out.println("Updated rows: " + response.getNumUpdatedRows());
System.out.println("Inserted rows: " + response.getNumInsertedRows());
```

## Querying Data

### Counting Rows

```java
import org.lance.namespace.model.CountTableRowsRequest;

CountTableRowsRequest request = new CountTableRowsRequest();
request.setId(Arrays.asList("my_namespace", "my_table"));

Long rowCount = namespace.countTableRows(request);
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
byte[] result = namespace.queryTable(query);
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

byte[] result = namespace.queryTable(query);
```

### Query with Filter

```java
QueryTableRequest query = new QueryTableRequest();
query.setId(Arrays.asList("my_namespace", "my_table"));
query.setK(10);
query.setFilter("id > 50");
query.setColumns(Arrays.asList("id", "name"));

byte[] result = namespace.queryTable(query);
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

byte[] result = namespace.queryTable(query);
```

### Reading Query Results

Query results are returned in Apache Arrow IPC file format. Here's how to read them:

```java
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

// Helper class to read Arrow data from byte array
class ByteArraySeekableByteChannel implements SeekableByteChannel {
    private final byte[] data;
    private long position = 0;
    private boolean isOpen = true;

    public ByteArraySeekableByteChannel(byte[] data) {
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

// Read query results
byte[] queryResult = namespace.queryTable(query);

try (BufferAllocator allocator = new RootAllocator();
     ArrowFileReader reader = new ArrowFileReader(
         new ByteArraySeekableByteChannel(queryResult), allocator)) {

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
