/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lancedb;

import org.lance.namespace.RestNamespace;
import org.lance.namespace.model.CountTableRowsRequest;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.CreateTableRequest;
import org.lance.namespace.model.CreateTableResponse;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DescribeTableIndexStatsRequest;
import org.lance.namespace.model.DescribeTableIndexStatsResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropNamespaceResponse;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.DropTableResponse;
import org.lance.namespace.model.IndexContent;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTableIndicesRequest;
import org.lance.namespace.model.ListTableIndicesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.QueryTableRequest;
import org.lance.namespace.model.QueryTableRequestVector;

import com.google.common.collect.Lists;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

/** Common test utilities for Lance Namespace tests. */
public class Utils {
  private static final Logger log = LoggerFactory.getLogger(Utils.class);

  /** Generate a unique name for testing (used for both tables and namespaces). */
  public static String generateTableName(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replace("-", "_").substring(0, 8);
  }

  /** Generate a unique table name with default prefix. */
  public static String generateTableName() {
    return generateTableName("test_table");
  }

  /** Generate a unique namespace name for testing. */
  public static String generateNamespaceName(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replace("-", "_").substring(0, 8);
  }

  /** Generate a unique namespace name with default prefix. */
  public static String generateNamespaceName() {
    return generateNamespaceName("test_ns");
  }

  // ==================== Namespace Operations ====================

  /**
   * Create a namespace.
   *
   * @param namespace The RestNamespace instance
   * @param namespacePath The path components for the namespace (e.g., "parent", "child")
   * @return The CreateNamespaceResponse from the server
   */
  public static CreateNamespaceResponse createNamespace(
      RestNamespace namespace, String... namespacePath) {
    log.info("Creating namespace: {}", Arrays.toString(namespacePath));
    CreateNamespaceRequest request = new CreateNamespaceRequest();
    request.setId(Lists.newArrayList(namespacePath));
    CreateNamespaceResponse response = namespace.createNamespace(request);
    log.info("Namespace created successfully: {}", Arrays.toString(namespacePath));
    return response;
  }

  /**
   * Describe a namespace.
   *
   * @param namespace The RestNamespace instance
   * @param namespacePath The path components for the namespace
   * @return The DescribeNamespaceResponse from the server
   */
  public static DescribeNamespaceResponse describeNamespace(
      RestNamespace namespace, String... namespacePath) {
    DescribeNamespaceRequest request = new DescribeNamespaceRequest();
    request.setId(Lists.newArrayList(namespacePath));
    return namespace.describeNamespace(request);
  }

  /**
   * Drop a namespace.
   *
   * @param namespace The RestNamespace instance
   * @param namespacePath The path components for the namespace
   * @return The DropNamespaceResponse from the server
   */
  public static DropNamespaceResponse dropNamespace(
      RestNamespace namespace, String... namespacePath) {
    log.info("Dropping namespace: {}", Arrays.toString(namespacePath));
    DropNamespaceRequest request = new DropNamespaceRequest();
    request.setId(Lists.newArrayList(namespacePath));
    DropNamespaceResponse response = namespace.dropNamespace(request);
    log.info("Namespace dropped successfully: {}", Arrays.toString(namespacePath));
    return response;
  }

  /**
   * List child namespaces under a parent namespace.
   *
   * @param namespace The RestNamespace instance
   * @param parentPath The path components for the parent namespace (empty for root)
   * @return The ListNamespacesResponse from the server
   */
  public static ListNamespacesResponse listNamespaces(
      RestNamespace namespace, String... parentPath) {
    ListNamespacesRequest request = new ListNamespacesRequest();
    request.setParent(Lists.newArrayList(parentPath));
    return namespace.listNamespaces(request);
  }

  /**
   * List tables under a namespace.
   *
   * @param namespace The RestNamespace instance
   * @param namespacePath The path components for the namespace (empty for root)
   * @return The ListTablesResponse from the server
   */
  public static ListTablesResponse listTables(RestNamespace namespace, String... namespacePath) {
    ListTablesRequest request = new ListTablesRequest();
    request.setParent(Lists.newArrayList(namespacePath));
    return namespace.listTables(request);
  }

  // ==================== Cleanup Utilities ====================

  /**
   * Clean up a table and its parent namespace. Logs warnings on failure but does not throw.
   *
   * @param namespace The RestNamespace instance
   * @param tablePath The path to the table
   * @param namespacePath The namespace path components to drop after the table
   */
  public static void cleanupTableAndNamespace(
      RestNamespace namespace, List<String> tablePath, String... namespacePath) {
    try {
      dropTable(namespace, tablePath);
    } catch (Exception e) {
      log.warn("Failed to drop table {}: {}", tablePath, e.getMessage());
    }
    try {
      dropNamespace(namespace, namespacePath);
    } catch (Exception e) {
      log.warn("Failed to drop namespace {}: {}", Arrays.toString(namespacePath), e.getMessage());
    }
  }

  /**
   * Clean up multiple tables and a namespace. Logs warnings on failure but does not throw.
   *
   * @param namespace The RestNamespace instance
   * @param tablePaths List of table paths to drop
   * @param namespacePath The namespace path components to drop after the tables
   */
  public static void cleanupTablesAndNamespace(
      RestNamespace namespace, List<List<String>> tablePaths, String... namespacePath) {
    for (List<String> tablePath : tablePaths) {
      try {
        dropTable(namespace, tablePath);
      } catch (Exception e) {
        log.warn("Failed to drop table {}: {}", tablePath, e.getMessage());
      }
    }
    try {
      dropNamespace(namespace, namespacePath);
    } catch (Exception e) {
      log.warn("Failed to drop namespace {}: {}", Arrays.toString(namespacePath), e.getMessage());
    }
  }

  // ==================== Table Operations ====================

  /**
   * Create a table with the given path and data. This method logs the table path for better
   * visibility and centralized handling.
   *
   * @param namespace The RestNamespace instance
   * @param tablePath The path components for the table (e.g., "namespace", "table_name")
   * @param tableData The data to populate the table with (Arrow IPC format)
   * @return The CreateTableResponse from the server
   */
  public static CreateTableResponse createTable(
      RestNamespace namespace, List<String> tablePath, byte[] tableData) {
    log.info("Creating table: {}", tablePath);
    CreateTableRequest createRequest = new CreateTableRequest();
    createRequest.setId(tablePath);
    CreateTableResponse response = namespace.createTable(createRequest, tableData);
    log.info("Table created successfully: {}", tablePath);
    return response;
  }

  /**
   * Create a table with the given path and number of rows using default schema.
   *
   * @param namespace The RestNamespace instance
   * @param allocator The BufferAllocator to use for creating data
   * @param tablePath The path components for the table
   * @param numRows The number of rows to create
   * @return The CreateTableResponse from the server
   * @throws IOException if data creation fails
   */
  public static CreateTableResponse createTable(
      RestNamespace namespace, BufferAllocator allocator, List<String> tablePath, int numRows)
      throws IOException {
    return createTable(namespace, allocator, tablePath, 1, numRows);
  }

  /**
   * Create a table with the given path and rows starting from a specific ID.
   *
   * @param namespace The RestNamespace instance
   * @param allocator The BufferAllocator to use for creating data
   * @param tablePath The path components for the table
   * @param startId The starting ID for the rows
   * @param numRows The number of rows to create
   * @return The CreateTableResponse from the server
   * @throws IOException if data creation fails
   */
  public static CreateTableResponse createTable(
      RestNamespace namespace,
      BufferAllocator allocator,
      List<String> tablePath,
      int startId,
      int numRows)
      throws IOException {
    byte[] tableData = new TableDataBuilder(allocator).addRows(startId, numRows).build();
    return createTable(namespace, tablePath, tableData);
  }

  /**
   * Create a table with the given path using a custom TableDataBuilder.
   *
   * @param namespace The RestNamespace instance
   * @param tablePath The path components for the table
   * @param dataBuilder The TableDataBuilder configured with the desired data
   * @return The CreateTableResponse from the server
   * @throws IOException if data creation fails
   */
  public static CreateTableResponse createTable(
      RestNamespace namespace, List<String> tablePath, TableDataBuilder dataBuilder)
      throws IOException {
    byte[] tableData = dataBuilder.build();
    return createTable(namespace, tablePath, tableData);
  }

  /** Wait for an index to be fully built with no unindexed rows. */
  public static boolean waitForIndexComplete(
      RestNamespace namespace, List<String> tablePath, String indexName, int maxSeconds)
      throws InterruptedException {
    ListTableIndicesRequest listRequest = new ListTableIndicesRequest();
    listRequest.setId(tablePath);

    long startTime = System.currentTimeMillis();
    long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;

    while (elapsedSeconds < maxSeconds) {
      ListTableIndicesResponse listResponse = namespace.listTableIndices(listRequest);
      if (listResponse.getIndexes() != null) {
        Optional<IndexContent> indexOpt =
            listResponse.getIndexes().stream()
                .filter(idx -> idx.getIndexName().equals(indexName))
                .findFirst();

        if (indexOpt.isPresent()) {
          // Index exists, now check if it's fully built
          DescribeTableIndexStatsRequest statsRequest = new DescribeTableIndexStatsRequest();
          statsRequest.setId(tablePath);

          DescribeTableIndexStatsResponse stats =
              namespace.describeTableIndexStats(statsRequest, indexName);
          if (stats != null
              && stats.getNumUnindexedRows() != null
              && stats.getNumUnindexedRows() == 0) {
            log.info("Index {} is fully built with 0 unindexed rows", indexName);
            return true;
          } else if (stats != null && stats.getNumUnindexedRows() != null) {
            elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
            log.info(
                "  Waiting for index... {} rows remaining ({}s/{}s)",
                stats.getNumUnindexedRows(),
                elapsedSeconds,
                maxSeconds);
          }
        } else {
          elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
          log.info("Waiting for index {} to exist: {}s/{}s", indexName, elapsedSeconds, maxSeconds);
        }
      }
      Thread.sleep(1000);
    }
    return false;
  }

  /** Drop a table and print confirmation. */
  public static DropTableResponse dropTable(RestNamespace namespace, List<String> tablePath) {
    DropTableRequest dropRequest = new DropTableRequest();
    dropRequest.setId(tablePath);

    DropTableResponse response = namespace.dropTable(dropRequest);
    log.info("Table dropped successfully: {}", tablePath);

    return response;
  }

  /** Count rows in a table. */
  public static long countRows(RestNamespace namespace, List<String> tablePath) {
    CountTableRowsRequest countRequest = new CountTableRowsRequest();
    countRequest.setId(tablePath);
    return namespace.countTableRows(countRequest);
  }

  /** Create a simple query request for testing. */
  public static QueryTableRequest createSimpleQuery(List<String> tablePath, int k) {
    QueryTableRequest query = new QueryTableRequest();
    query.setId(tablePath);
    query.setK(k);
    // Add default columns to avoid "no columns selected" error
    query.setColumns(java.util.Arrays.asList("id", "name", "category", "embedding"));
    return query;
  }

  /** Create a vector query request with a specific target value. */
  public static QueryTableRequest createVectorQuery(List<String> tablePath, int k, int dimensions) {
    return createVectorQuery(tablePath, k, dimensions, 10.0f);
  }

  /** Create a vector query request with a specific target value for all dimensions. */
  public static QueryTableRequest createVectorQuery(
      List<String> tablePath, int k, int dimensions, float targetValue) {
    QueryTableRequest query = new QueryTableRequest();
    query.setId(tablePath);
    query.setK(k);

    // Generate a vector with all elements set to targetValue
    // This will find rows where the embedding values are closest to targetValue
    java.util.List<Float> vector = new java.util.ArrayList<>();
    for (int i = 0; i < dimensions; i++) {
      vector.add(targetValue);
    }
    QueryTableRequestVector queryVector = new QueryTableRequestVector();
    queryVector.setSingleVector(vector);
    query.setVector(queryVector);

    // Add default columns to avoid "no columns selected" error
    query.setColumns(java.util.Arrays.asList("id", "name", "category", "embedding"));

    return query;
  }

  /** Create a default schema with id, name, text, and embedding fields. */
  public static Schema createDefaultSchema() {
    return createDefaultSchema(128);
  }

  /**
   * Create a default schema with id, name, category, and embedding fields for general test cases.
   */
  public static Schema createDefaultSchema(int embeddingDimension) {
    Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field nameField = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field categoryField = new Field("category", FieldType.nullable(new ArrowType.Utf8()), null);
    Field embeddingField =
        new Field(
            "embedding",
            FieldType.nullable(new ArrowType.FixedSizeList(embeddingDimension)),
            Arrays.asList(
                new Field(
                    "item",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                    null)));

    return new Schema(Arrays.asList(idField, nameField, categoryField, embeddingField));
  }

  /** Create a schema with text field for FTS tests. */
  public static Schema createSchemaWithText(int embeddingDimension) {
    Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field nameField = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field textField = new Field("text", FieldType.nullable(new ArrowType.Utf8()), null);
    Field categoryField = new Field("category", FieldType.nullable(new ArrowType.Utf8()), null);
    Field embeddingField =
        new Field(
            "embedding",
            FieldType.nullable(new ArrowType.FixedSizeList(embeddingDimension)),
            Arrays.asList(
                new Field(
                    "item",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                    null)));

    return new Schema(Arrays.asList(idField, nameField, textField, categoryField, embeddingField));
  }

  /** Read Arrow file format data and process it. */
  public static void readArrowFile(
      byte[] data, BufferAllocator allocator, Consumer<VectorSchemaRoot> processor)
      throws IOException {
    ByteArraySeekableByteChannel channel = new ByteArraySeekableByteChannel(data);
    try (ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
      for (int i = 0; i < reader.getRecordBlocks().size(); i++) {
        reader.loadRecordBatch(reader.getRecordBlocks().get(i));
        processor.accept(reader.getVectorSchemaRoot());
      }
    }
  }

  /** Count total rows in Arrow file format data. */
  public static int countRows(byte[] data, BufferAllocator allocator) throws IOException {
    int[] totalRows = {0};
    readArrowFile(data, allocator, root -> totalRows[0] += root.getRowCount());
    return totalRows[0];
  }

  /** Extract values from a specific column. */
  public static <T> List<T> extractColumn(
      byte[] data, BufferAllocator allocator, String columnName, Class<T> type) throws IOException {
    List<T> values = new ArrayList<>();

    readArrowFile(
        data,
        allocator,
        root -> {
          if (type == Integer.class) {
            IntVector vector = (IntVector) root.getVector(columnName);
            for (int i = 0; i < root.getRowCount(); i++) {
              if (!vector.isNull(i)) {
                values.add(type.cast(vector.get(i)));
              }
            }
          } else if (type == String.class) {
            VarCharVector vector = (VarCharVector) root.getVector(columnName);
            for (int i = 0; i < root.getRowCount(); i++) {
              if (!vector.isNull(i)) {
                values.add(type.cast(new String(vector.get(i), StandardCharsets.UTF_8)));
              }
            }
          }
          // Add more type handlers as needed
        });

    return values;
  }

  /** Builder for creating Arrow data with common table schema. */
  public static class TableDataBuilder {
    private final BufferAllocator allocator;
    private final List<TableRow> rows = new ArrayList<>();
    private Schema customSchema;
    private Map<Integer, String> customTexts = new HashMap<>();

    // Default sample names for test data
    private static final String[] DEFAULT_NAMES = {
      "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack",
      "Kate", "Liam", "Maya", "Noah", "Olivia", "Peter", "Quinn", "Rose", "Sam", "Tara"
    };

    public TableDataBuilder(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    /** Add a row with default values. */
    public TableDataBuilder addRow(int id) {
      return addRow(id, DEFAULT_NAMES[id % DEFAULT_NAMES.length], generateVector(id, 128));
    }

    /** Add a row with custom values. */
    public TableDataBuilder addRow(int id, String name, float[] embedding) {
      rows.add(new TableRow(id, name, embedding));
      return this;
    }

    /** Add multiple rows with default values. */
    public TableDataBuilder addRows(int startId, int count) {
      for (int i = 0; i < count; i++) {
        addRow(startId + i);
      }
      return this;
    }

    /** Set a custom schema instead of the default one. */
    public TableDataBuilder withSchema(Schema schema) {
      this.customSchema = schema;
      return this;
    }

    /** Set custom text for a specific row ID. */
    public TableDataBuilder withText(int id, String text) {
      this.customTexts.put(id, text);
      return this;
    }

    /** Build the Arrow IPC data. */
    public byte[] build() throws IOException {
      Schema schema = customSchema != null ? customSchema : createDefaultSchema();

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        root.setRowCount(rows.size());

        // Populate vectors based on schema
        for (Field field : schema.getFields()) {
          populateVector(root, field, rows);
        }

        // Serialize to Arrow IPC format
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer =
            new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
          writer.start();
          writer.writeBatch();
          writer.end();
        }

        return out.toByteArray();
      }
    }

    private void populateVector(VectorSchemaRoot root, Field field, List<TableRow> rows) {
      String fieldName = field.getName();

      switch (fieldName) {
        case "id":
          IntVector idVector = (IntVector) root.getVector("id");
          for (int i = 0; i < rows.size(); i++) {
            idVector.setSafe(i, rows.get(i).id);
          }
          idVector.setValueCount(rows.size());
          break;

        case "name":
          VarCharVector nameVector = (VarCharVector) root.getVector("name");
          for (int i = 0; i < rows.size(); i++) {
            nameVector.setSafe(i, rows.get(i).name.getBytes(StandardCharsets.UTF_8));
          }
          nameVector.setValueCount(rows.size());
          break;

        case "text":
          VarCharVector textVector = (VarCharVector) root.getVector("text");
          for (int i = 0; i < rows.size(); i++) {
            int rowId = rows.get(i).id;
            String text;
            if (customTexts.containsKey(rowId)) {
              text = customTexts.get(rowId);
            } else {
              // Default text if not specified
              text = "Default text for row " + rowId;
            }
            textVector.setSafe(i, text.getBytes(StandardCharsets.UTF_8));
          }
          textVector.setValueCount(rows.size());
          break;

        case "category":
          VarCharVector categoryVector = (VarCharVector) root.getVector("category");
          String[] categories = {"category1", "category2", "category3"};
          for (int i = 0; i < rows.size(); i++) {
            // Use modulo to evenly distribute categories
            String category = categories[rows.get(i).id % 3];
            categoryVector.setSafe(i, category.getBytes(StandardCharsets.UTF_8));
          }
          categoryVector.setValueCount(rows.size());
          break;

        case "embedding":
          FixedSizeListVector vectorVector = (FixedSizeListVector) root.getVector("embedding");
          Float4Vector dataVector = (Float4Vector) vectorVector.getDataVector();
          vectorVector.allocateNew();

          for (int row = 0; row < rows.size(); row++) {
            vectorVector.setNotNull(row);
            float[] embedding = rows.get(row).embedding;
            for (int dim = 0; dim < embedding.length; dim++) {
              int index = row * embedding.length + dim;
              dataVector.setSafe(index, embedding[dim]);
            }
          }

          dataVector.setValueCount(rows.size() * rows.get(0).embedding.length);
          vectorVector.setValueCount(rows.size());
          break;
      }
    }

    private static float[] generateVector(int seed, int dimensions) {
      float[] vector = new float[dimensions];
      // Create deterministic vectors: each vector has all elements set to the row id value
      // This makes search results predictable: searching for vector of all 10s will find
      // row 10 as closest, then 11, then 9, etc.
      for (int i = 0; i < dimensions; i++) {
        vector[i] = (float) seed;
      }
      return vector;
    }
  }

  /** Simple row representation for building test data. */
  private static class TableRow {
    final int id;
    final String name;
    final float[] embedding;

    TableRow(int id, String name, float[] embedding) {
      this.id = id;
      this.name = name;
      this.embedding = embedding;
    }
  }

  /** SeekableByteChannel implementation for reading Arrow file format from byte array. */
  public static class ByteArraySeekableByteChannel implements SeekableByteChannel {
    private final byte[] data;
    private long position = 0;
    private boolean isOpen = true;

    public ByteArraySeekableByteChannel(byte[] data) {
      this.data = data;
    }

    @Override
    public long position() throws IOException {
      return position;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
      if (newPosition < 0 || newPosition > data.length) {
        throw new IOException("Invalid position: " + newPosition);
      }
      position = newPosition;
      return this;
    }

    @Override
    public long size() throws IOException {
      return data.length;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      if (!isOpen) {
        throw new IOException("Channel is closed");
      }
      int remaining = dst.remaining();
      int available = (int) (data.length - position);
      if (available <= 0) {
        return -1;
      }
      int toRead = Math.min(remaining, available);
      dst.put(data, (int) position, toRead);
      position += toRead;
      return toRead;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      throw new UnsupportedOperationException("Read-only channel");
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
      throw new UnsupportedOperationException("Read-only channel");
    }

    @Override
    public boolean isOpen() {
      return isOpen;
    }

    @Override
    public void close() throws IOException {
      isOpen = false;
    }
  }
}
