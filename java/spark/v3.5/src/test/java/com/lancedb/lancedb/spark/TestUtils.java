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

import com.lancedb.lancedb.spark.internal.LanceConfig;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtils {
  /**
   * Converts dbPath and tableName to a LanceConfig instance using a Map.
   *
   * @param dbPath The database path.
   * @param tableName The table name.
   * @return A LanceConfig instance.
   */
  public static LanceConfig createLanceConfig(String dbPath, String tableName) {
    Map<String, String> properties = new HashMap<>();
    properties.put(LanceConfig.CONFIG_DB_PATH, dbPath);
    properties.put(LanceConfig.CONFIG_TABLE_NAME, tableName);
    return LanceConfig.from(properties);
  }

  public static class TestTable1Config {
    public static final String dbPath;
    public static final String tableName = "test_table1";
    public static final String tablePath;
    public static final List<List<Long>> expectedValues = Arrays.asList(
        Arrays.asList(0L, 0L, 0L, 0L),
        Arrays.asList(1L, 2L, 3L, -1L),
        Arrays.asList(2L, 4L, 6L, -2L),
        Arrays.asList(3L, 6L, 9L, -3L)
    );
    public static final LanceConfig lanceConfig;

    public static final StructType expectedSchema = new StructType(new StructField[]{
        DataTypes.createStructField("x", DataTypes.LongType, true),
        DataTypes.createStructField("y", DataTypes.LongType, true),
        DataTypes.createStructField("b", DataTypes.LongType, true),
        DataTypes.createStructField("c", DataTypes.LongType, true),
    });

    static {
      URL resource = TestUtils.class.getResource("/example_db");
      if (resource != null) {
        dbPath = resource.toString();
      } else {
        throw new IllegalArgumentException("example_db not found in resources directory");
      }
      tablePath = String.format("%s/%s.lance", dbPath, tableName);
      lanceConfig = createLanceConfig(dbPath, tableName);
    }
  }
}
