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

package com.lancedb.lancedb.spark.internal;

import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class LanceDataSourceReadOptions {
  public static final String CONFIG_DB_PATH = "db";
  public static final String CONFIG_TABLE_NAME = "table";
  private static final String LANCE_FILE_SUFFIX = ".lance";

  private final String dbPath;
  private final String tableName;

  public LanceDataSourceReadOptions(String dbPath, String tableName) {
    this.dbPath = dbPath;
    this.tableName = tableName;
  }

  public static LanceDataSourceReadOptions from(CaseInsensitiveStringMap options) {
    if (options.containsKey(CONFIG_DB_PATH) && options.containsKey(CONFIG_TABLE_NAME)) {
      return new LanceDataSourceReadOptions(options.get(CONFIG_DB_PATH), options.get(CONFIG_TABLE_NAME));
    } else {
      throw new IllegalArgumentException("Missing required options");
    }
  }

  public static LanceDataSourceReadOptions from(Map<String, String> properties) {
    if (properties.containsKey(CONFIG_DB_PATH) && properties.containsKey(CONFIG_TABLE_NAME)) {
      return new LanceDataSourceReadOptions(properties.get(CONFIG_DB_PATH), properties.get(CONFIG_TABLE_NAME));
    } else {
      throw new IllegalArgumentException("Missing required options");
    }
  }

  public String getDbPath() {
    return dbPath;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTablePath() {
    StringBuilder sb = new StringBuilder().append(dbPath);
    if (!dbPath.endsWith("/")) {
      sb.append("/");
    }
    return sb.append(tableName).append(LANCE_FILE_SUFFIX).toString();
  }
}
