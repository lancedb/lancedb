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

package com.lancedb.lancedb;

import io.questdb.jar.jni.JarJniLoader;
import java.io.Closeable;
import java.util.List;
import java.util.Optional;

/**
 * Represents LanceDB database.
 */
public class Connection implements Closeable {
  static {
    JarJniLoader.loadLib(Connection.class, "/nativelib", "lancedb_jni");
  }

  private long nativeConnectionHandle;

  /**
   * Connect to a LanceDB instance.
   */
  public static native Connection connect(String uri);

  /**
   * Get the names of all tables in the database. The names are sorted in
   * ascending order.
   *
   * @return the table names
   */
  public List<String> tableNames() {
    return tableNames(Optional.empty(), Optional.empty());
  }

  /**
   * Get the names of filtered tables in the database. The names are sorted in
   * ascending order.
   *
   * @param limit The number of results to return.
   * @return the table names
   */
  public List<String> tableNames(int limit) {
    return tableNames(Optional.empty(), Optional.of(limit));
  }

  /**
   * Get the names of filtered tables in the database. The names are sorted in
   * ascending order.
   *
   * @param startAfter If present, only return names that come lexicographically after the supplied
   *                   value. This can be combined with limit to implement pagination
   *                   by setting this to the last table name from the previous page.
   * @return the table names
   */
  public List<String> tableNames(String startAfter) {
    return tableNames(Optional.of(startAfter), Optional.empty());
  }

  /**
   * Get the names of filtered tables in the database. The names are sorted in
   * ascending order.
   *
   * @param startAfter If present, only return names that come lexicographically after the supplied
   *                   value. This can be combined with limit to implement pagination
   *                   by setting this to the last table name from the previous page.
   * @param limit The number of results to return.
   * @return the table names
   */
  public List<String> tableNames(String startAfter, int limit) {
    return tableNames(Optional.of(startAfter), Optional.of(limit));
  }

  /**
   * Get the names of filtered tables in the database. The names are sorted in
   * ascending order.
   *
   * @param startAfter If present, only return names that come lexicographically after the supplied
   *                   value. This can be combined with limit to implement pagination
   *                   by setting this to the last table name from the previous page.
   * @param limit The number of results to return.
   * @return the table names
   */
  public native List<String> tableNames(
      Optional<String> startAfter, Optional<Integer> limit);

  /**
   * Closes this connection and releases any system resources associated with it. If
   * the connection is
   * already closed, then invoking this method has no effect.
   */
  @Override
  public void close() {
    if (nativeConnectionHandle != 0) {
      releaseNativeConnection(nativeConnectionHandle);
      nativeConnectionHandle = 0;
    }
  }

  /**
   * Native method to release the Lance connection resources associated with the
   * given handle.
   *
   * @param handle The native handle to the connection resource.
   */
  private native void releaseNativeConnection(long handle);

  private Connection() {}
}