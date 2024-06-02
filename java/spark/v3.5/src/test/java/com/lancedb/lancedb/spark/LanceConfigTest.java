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
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LanceConfigTest {
  @Test
  public void testLanceConfigFromCaseInsensitiveStringMap() {
    String dbPath = "file://path/to/db";
    String tableName = "testTableName";
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(new HashMap<String, String>() {{
      put(LanceConfig.CONFIG_DB_PATH, dbPath);
      put(LanceConfig.CONFIG_TABLE_NAME, tableName);
    }});

    LanceConfig config = LanceConfig.from(options);

    assertEquals(dbPath, config.getDbPath());
    assertEquals(tableName, config.getTableName());
    assertEquals(dbPath + "/" + tableName + ".lance", config.getTablePath());
  }

  @Test
  public void testLanceConfigFromCaseInsensitiveStringMap2() {
    String dbPath = "s3://bucket/folder/";
    String tableName = "testTableName";
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(new HashMap<String, String>() {{
      put(LanceConfig.CONFIG_DB_PATH, dbPath);
      put(LanceConfig.CONFIG_TABLE_NAME, tableName);
    }});

    LanceConfig config = LanceConfig.from(options);

    assertEquals(dbPath, config.getDbPath());
    assertEquals(tableName, config.getTableName());
    assertEquals(dbPath + tableName  + ".lance", config.getTablePath());
  }

  @Test
  public void testLanceConfigFromMap() {
    String dbPath = "file://path/to/db";
    String tableName = "testTableName";
    Map<String, String> properties = new HashMap<>();
    properties.put(LanceConfig.CONFIG_DB_PATH, dbPath);
    properties.put(LanceConfig.CONFIG_TABLE_NAME, tableName);

    LanceConfig config = LanceConfig.from(properties);

    assertEquals(dbPath, config.getDbPath());
    assertEquals(tableName, config.getTableName());
    assertEquals(dbPath + "/" + tableName + ".lance", config.getTablePath());
  }

  @Test
  public void testLanceConfigFromMap2() {
    String dbPath = "s3://bucket/folder/";
    String tableName = "testTableName";
    Map<String, String> properties = new HashMap<>();
    properties.put(LanceConfig.CONFIG_DB_PATH, dbPath);
    properties.put(LanceConfig.CONFIG_TABLE_NAME, tableName);

    LanceConfig config = LanceConfig.from(properties);

    assertEquals(dbPath, config.getDbPath());
    assertEquals(tableName, config.getTableName());
    assertEquals(dbPath + tableName + ".lance", config.getTablePath());
  }
}
