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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.net.URL;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ConnectionTest {
  @TempDir
  static Path tempDir; // Temporary directory for the tests

  @Test
  void emptyDB() {
    String databaseUri = tempDir.resolve("emptyDB").toString();
    try (Connection conn = Connection.connect(databaseUri)) {
      List<String> tableNames = conn.tableNames();
      assertTrue(tableNames.isEmpty());
    }
  }

  @Test
  void tableNmaes() {
    ClassLoader classLoader = getClass().getClassLoader();
    URL lanceDbURL = classLoader.getResource("example_db");
    try (Connection conn = Connection.connect(lanceDbURL.toString())) {
      List<String> tableNames = conn.tableNames();
      assertEquals("dataset_version", tableNames.get(0));
      assertEquals("new_empty_dataset", tableNames.get(1));
      assertEquals("test", tableNames.get(2));
      assertEquals("write_stream", tableNames.get(3));
    }
  }

  @Test
  void tableNmaesStartAfter() {
    ClassLoader classLoader = getClass().getClassLoader();
    URL lanceDbURL = classLoader.getResource("example_db");
    try (Connection conn = Connection.connect(lanceDbURL.toString())) {
      List<String> tableNames = conn.tableNames("new_empty_dataset");
      assertEquals("test", tableNames.get(0));
      assertEquals("write_stream", tableNames.get(1));
    }
  }

  @Test
  void tableNmaesLimit() {
    ClassLoader classLoader = getClass().getClassLoader();
    URL lanceDbURL = classLoader.getResource("example_db");
    try (Connection conn = Connection.connect(lanceDbURL.toString())) {
      List<String> tableNames = conn.tableNames(2);
      assertEquals("dataset_version", tableNames.get(0));
      assertEquals("new_empty_dataset", tableNames.get(1));
    }
  }
}
