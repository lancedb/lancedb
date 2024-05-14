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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ConnectionTest {
  private static final String[] TABLE_NAMES = {
      "dataset_version",
      "new_empty_dataset",
      "test",
      "write_stream"
  };

  @TempDir
  static Path tempDir; // Temporary directory for the tests
  private static URL lanceDbURL;

  @BeforeAll
  static void setUp() {
    ClassLoader classLoader = ConnectionTest.class.getClassLoader();
    lanceDbURL = classLoader.getResource("example_db");
  }

  @Test
  void emptyDB() {
    String databaseUri = tempDir.resolve("emptyDB").toString();
    try (Connection conn = Connection.connect(databaseUri)) {
      List<String> tableNames = conn.tableNames();
      assertTrue(tableNames.isEmpty());
    }
  }

  @Test
  void tableNames() {
    try (Connection conn = Connection.connect(lanceDbURL.toString())) {
      List<String> tableNames = conn.tableNames();
      assertEquals(4, tableNames.size());
      for (int i = 0; i < TABLE_NAMES.length; i++) {
        assertEquals(TABLE_NAMES[i], tableNames.get(i));
      }
    }
  }

  @Test
  void tableNamesStartAfter() {
    try (Connection conn = Connection.connect(lanceDbURL.toString())) {
      assertTableNamesStartAfter(conn, TABLE_NAMES[0], 3, TABLE_NAMES[1], TABLE_NAMES[2], TABLE_NAMES[3]);
      assertTableNamesStartAfter(conn, TABLE_NAMES[1], 2, TABLE_NAMES[2], TABLE_NAMES[3]);
      assertTableNamesStartAfter(conn, TABLE_NAMES[2], 1, TABLE_NAMES[3]);
      assertTableNamesStartAfter(conn, TABLE_NAMES[3], 0);
      assertTableNamesStartAfter(conn, "a_dataset", 4, TABLE_NAMES[0], TABLE_NAMES[1], TABLE_NAMES[2], TABLE_NAMES[3]);
      assertTableNamesStartAfter(conn, "o_dataset", 2, TABLE_NAMES[2], TABLE_NAMES[3]);
      assertTableNamesStartAfter(conn, "v_dataset", 1, TABLE_NAMES[3]);
      assertTableNamesStartAfter(conn, "z_dataset", 0);
    }
  }

  private void assertTableNamesStartAfter(Connection conn, String startAfter, int expectedSize, String... expectedNames) {
    List<String> tableNames = conn.tableNames(startAfter);
    assertEquals(expectedSize, tableNames.size());
    for (int i = 0; i < expectedNames.length; i++) {
      assertEquals(expectedNames[i], tableNames.get(i));
    }
  }

  @Test
  void tableNamesLimit() {
      try (Connection conn = Connection.connect(lanceDbURL.toString())) {
      for (int i = 0; i <= TABLE_NAMES.length; i++) {
        List<String> tableNames = conn.tableNames(i);
        assertEquals(i, tableNames.size());
        for (int j = 0; j < i; j++) {
          assertEquals(TABLE_NAMES[j], tableNames.get(j));
        }
      }
    }
  }

  @Test
  void tableNamesStartAfterLimit() {
    try (Connection conn = Connection.connect(lanceDbURL.toString())) {
      List<String> tableNames = conn.tableNames(TABLE_NAMES[0], 2);
      assertEquals(2, tableNames.size());
      assertEquals(TABLE_NAMES[1], tableNames.get(0));
      assertEquals(TABLE_NAMES[2], tableNames.get(1));
      tableNames = conn.tableNames(TABLE_NAMES[1], 1);
      assertEquals(1, tableNames.size());
      assertEquals(TABLE_NAMES[2], tableNames.get(0));
      tableNames = conn.tableNames(TABLE_NAMES[2], 2);
      assertEquals(1, tableNames.size());
      assertEquals(TABLE_NAMES[3], tableNames.get(0));
      tableNames = conn.tableNames(TABLE_NAMES[3], 2);
      assertEquals(0, tableNames.size());
      tableNames = conn.tableNames(TABLE_NAMES[0], 0);
      assertEquals(0, tableNames.size());

      // Limit larger than the number of remaining tables
      tableNames = conn.tableNames(TABLE_NAMES[0], 10);
      assertEquals(3, tableNames.size());
      assertEquals(TABLE_NAMES[1], tableNames.get(0));
      assertEquals(TABLE_NAMES[2], tableNames.get(1));
      assertEquals(TABLE_NAMES[3], tableNames.get(2));

      // Start after a value not in the list
      tableNames = conn.tableNames("non_existent_table", 2);
      assertEquals(2, tableNames.size());
      assertEquals(TABLE_NAMES[2], tableNames.get(0));
      assertEquals(TABLE_NAMES[3], tableNames.get(1));

      // Start after the last table with a limit
      tableNames = conn.tableNames(TABLE_NAMES[3], 1);
      assertEquals(0, tableNames.size());
    }
  }
}
