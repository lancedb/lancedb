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

import java.net.URL;

public class SparkLanceConnectorTest {
  private static SparkSession spark;
  private static String dbPath;

  @BeforeAll
  static void setup() {
    spark = SparkSession.builder()
        .appName("spark-lance-connector-test")
        .master("local")
        .getOrCreate();
    // Get the path to the example_db in the test resources directory
    URL resource = SparkLanceConnectorTest.class.getResource("/example_db");
    if (resource != null) {
      dbPath = resource.toString();
    } else {
      throw new IllegalArgumentException("example_db not found in resources directory");
    }
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void readAll() {
    Dataset<Row> data = spark.read().format("lance")
        .option("db", dbPath)
        .option("table", "test_table1")
        .load();
    data.show();
  }
}
