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

package com.lancedb.lancedb.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SparkLanceConnectorTest {
  private static SparkSession spark;
  private static String dbPath;

  @BeforeAll
  static void setup() {
    spark = SparkSession.builder()
        .appName("spark-lance-connector-test")
        .master("local")
        .getOrCreate();
    dbPath = TestUtils.TestTable1Config.dbPath;
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private void validateData(Dataset<Row> data, List<List<Long>> expectedValues) {
    List<Row> rows = data.collectAsList();
    assertEquals(expectedValues.size(), rows.size());

    for (int i = 0; i < rows.size(); i++) {
      Row row = rows.get(i);
      List<Long> expectedRow = expectedValues.get(i);
      assertEquals(expectedRow.size(), row.size());

      for (int j = 0; j < expectedRow.size(); j++) {
        long expectedValue = expectedRow.get(j);
        long actualValue = row.getLong(j);
        assertEquals(expectedValue, actualValue, "Mismatch at row " + i + " column " + j);
      }
    }
  }

  @Test
  public void readAll() {
    Dataset<Row> data = spark.read().format("lance")
        .option("db", dbPath)
        .option("table", TestUtils.TestTable1Config.tableName)
        .load();
    validateData(data, TestUtils.TestTable1Config.expectedValues);
  }

  @Test
  public void filter() {
    Dataset<Row> data = spark.read().format("lance")
        .option("db", dbPath)
        .option("table", TestUtils.TestTable1Config.tableName)
        .load()
        .filter("x > 1");

    List<List<Long>> expectedValues = TestUtils.TestTable1Config.expectedValues.stream()
        .filter(row -> row.get(0) > 1)
        .collect(Collectors.toList());

    validateData(data, expectedValues);
  }

  @Test
  public void select() {
    Dataset<Row> data = spark.read().format("lance")
        .option("db", dbPath)
        .option("table", TestUtils.TestTable1Config.tableName)
        .load()
        .select("y", "b");

    List<List<Long>> expectedValues = TestUtils.TestTable1Config.expectedValues.stream()
        .map(row -> List.of(row.get(1), row.get(2)))
        .collect(Collectors.toList());

    validateData(data, expectedValues);
  }

  @Test
  public void filterSelect() {
    Dataset<Row> data = spark.read().format("lance")
        .option("db", dbPath)
        .option("table", TestUtils.TestTable1Config.tableName)
        .load()
        .select("y", "b")
        .filter("y > 3");

    List<List<Long>> expectedValues = TestUtils.TestTable1Config.expectedValues.stream()
        .map(row -> List.of(row.get(1), row.get(2))) // "y" is at index 1, "b" is at index 2
        .filter(row -> row.get(0) > 3)
        .collect(Collectors.toList());

    validateData(data, expectedValues);
  }
}
