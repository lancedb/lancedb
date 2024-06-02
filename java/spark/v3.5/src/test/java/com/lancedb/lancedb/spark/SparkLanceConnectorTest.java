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

  @Test
  public void readAll() {
    Dataset<Row> data = spark.read().format("lance")
        .option("db", dbPath)
        .option("table", TestUtils.TestTable1Config.tableName)
        .load();
    data.show();
  }
}
