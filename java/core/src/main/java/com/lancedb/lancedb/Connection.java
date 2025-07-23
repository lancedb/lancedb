package com.lancedb.lancedb;

import io.questdb.jar.jni.JarJniLoader;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Represents LanceDB database connection with enhanced functionality. */
public class Connection implements Closeable {
  static {
    JarJniLoader.loadLib(Connection.class, "/nativelib", "lancedb_jni");
  }

  private long nativeConnectionHandle;
  private String uri;

  /** Connect to a LanceDB instance with default options. */
  public static Connection connect(String uri) {
    return connect(uri, new ConnectionOptions());
  }

  /** Connect to a LanceDB instance with custom options. */
  public static Connection connect(String uri, ConnectionOptions options) {
    return connectNative(uri, options);
  }

  /** Native method to create connection with options */
  private static native Connection connectNative(String uri, ConnectionOptions options);

  /**
   * Create a table with data from a list of maps.
   *
   * @param name The table name
   * @param data List of maps representing records
   * @return The created table
   */
  public Table createTable(String name, List<Map<String, Object>> data) {
    return createTable(name, data, CreateTableMode.CREATE);
  }

  /**
   * Create a table with specified mode.
   *
   * @param name The table name
   * @param data List of maps representing records
   * @param mode Creation mode (CREATE, OVERWRITE, EXIST_OK)
   * @return The created table
   */
  public Table createTable(String name, List<Map<String, Object>> data, CreateTableMode mode) {
    return createTableNative(name, data, mode.toString());
  }

  /**
   * Create an empty table with Arrow schema.
   *
   * @param name The table name
   * @param schema Arrow schema
   * @return The created table
   */
  public Table createEmptyTable(String name, org.apache.arrow.vector.types.pojo.Schema schema) {
    return createEmptyTableNative(name, schema);
  }

  /**
   * Open an existing table.
   *
   * @param name The table name
   * @return The opened table
   */
  public Table openTable(String name) {
    return openTableNative(name);
  }

  /**
   * Drop a table.
   *
   * @param name The table name to drop
   */
  public void dropTable(String name) {
    dropTableNative(name);
  }

  // Native method declarations
  private native Table createTableNative(String name, List<Map<String, Object>> data, String mode);
  private native Table createEmptyTableNative(String name, org.apache.arrow.vector.types.pojo.Schema schema);
  private native Table openTableNative(String name);
  private native void dropTableNative(String name);

  // Existing methods from the original Connection class
  public List<String> tableNames() {
    return tableNames(Optional.empty(), Optional.empty());
  }

  public List<String> tableNames(int limit) {
    return tableNames(Optional.empty(), Optional.of(limit));
  }

  public List<String> tableNames(String startAfter) {
    return tableNames(Optional.of(startAfter), Optional.empty());
  }

  public List<String> tableNames(String startAfter, int limit) {
    return tableNames(Optional.of(startAfter), Optional.of(limit));
  }

  public native List<String> tableNames(Optional<String> startAfter, Optional<Integer> limit);

  @Override
  public void close() {
    if (nativeConnectionHandle != 0) {
      releaseNativeConnection(nativeConnectionHandle);
      nativeConnectionHandle = 0;
    }
  }

  private native void releaseNativeConnection(long handle);

  private Connection() {}

  // Getters
  public String getUri() { return uri; }
}