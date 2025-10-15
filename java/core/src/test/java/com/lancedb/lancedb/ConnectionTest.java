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

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectionTest {
  private static final String[] TABLE_NAMES = {
    "dataset_version", "new_empty_dataset", "test", "write_stream"
  };

  @TempDir static Path tempDir; // Temporary directory for the tests
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
      assertTableNamesStartAfter(
          conn, TABLE_NAMES[0], 3, TABLE_NAMES[1], TABLE_NAMES[2], TABLE_NAMES[3]);
      assertTableNamesStartAfter(conn, TABLE_NAMES[1], 2, TABLE_NAMES[2], TABLE_NAMES[3]);
      assertTableNamesStartAfter(conn, TABLE_NAMES[2], 1, TABLE_NAMES[3]);
      assertTableNamesStartAfter(conn, TABLE_NAMES[3], 0);
      assertTableNamesStartAfter(
          conn, "a_dataset", 4, TABLE_NAMES[0], TABLE_NAMES[1], TABLE_NAMES[2], TABLE_NAMES[3]);
      assertTableNamesStartAfter(conn, "o_dataset", 2, TABLE_NAMES[2], TABLE_NAMES[3]);
      assertTableNamesStartAfter(conn, "v_dataset", 1, TABLE_NAMES[3]);
      assertTableNamesStartAfter(conn, "z_dataset", 0);
    }
  }

  private void assertTableNamesStartAfter(
      Connection conn, String startAfter, int expectedSize, String... expectedNames) {
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

  @Test
  void createTableWithInitialData() throws IOException {
    try (Connection conn = Connection.connect(lanceDbURL.toString())) {
      String newTableName = "new_table_with_initial_data";
      List<String> tableNames = conn.tableNames();
      assertFalse(tableNames.contains(newTableName));
      List<Field> fields = new ArrayList<>();
      fields.add(new Field("id", FieldType.nullable(new ArrowType.Int(8, true)), null));
      fields.add(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null));
      Schema schema = new Schema(fields);
      VectorSchemaRoot initialData = VectorSchemaRoot.create(schema, new RootAllocator());
      TinyIntVector idVector = (TinyIntVector) initialData.getVector("id");
      idVector.allocateNew();
      idVector.setSafe(0, 1);
      idVector.setSafe(1, 2);
      idVector.setValueCount(2);

      VarCharVector nameVector = (VarCharVector) initialData.getVector("name");
      nameVector.allocateNew();
      nameVector.setSafe(0, "Alice".getBytes());
      nameVector.setSafe(1, "Bob".getBytes());
      nameVector.setValueCount(2);

      initialData.setRowCount(2);

      try (Table table = conn.createTable(newTableName, initialData)) {
        assertNotNull(table);
      }
      tableNames = conn.tableNames();
      assertTrue(tableNames.contains(newTableName));

      conn.dropTable(newTableName);
    }
  }

  @Test
  void createEmptyTable() throws IOException {
    try (Connection conn = Connection.connect(lanceDbURL.toString())) {
      String newTableName = "new_empty_table";
      List<String> tableNames = conn.tableNames();
      assertFalse(tableNames.contains(newTableName));
      List<Field> fields = new ArrayList<>();
      fields.add(new Field("id", FieldType.nullable(new ArrowType.Int(8, true)), null));
      fields.add(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null));
      Schema schema = new Schema(fields);
      try (Table table = conn.createEmptyTable(newTableName, schema)) {
        assertNotNull(table);
      }
      tableNames = conn.tableNames();
      assertTrue(tableNames.contains(newTableName));

      conn.dropTable(newTableName);
    }
  }

  @Test
  void dropTable() throws IOException {
    try (Connection conn = Connection.connect(lanceDbURL.toString())) {
      String newTableName = "new_empty_table_for_drop";
      List<Field> fields = new ArrayList<>();
      fields.add(new Field("id", FieldType.nullable(new ArrowType.Int(8, true)), null));
      fields.add(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null));
      Schema schema = new Schema(fields);
      try (Table table = conn.createEmptyTable(newTableName, schema)) {}
      assertTrue(conn.tableNames().contains(newTableName));
      conn.dropTable(newTableName);
      assertFalse(conn.tableNames().contains(newTableName));

      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> conn.dropTable(newTableName));
      assertEquals("Table '" + newTableName + "' was not found", exception.getMessage());
    }
  }
}
