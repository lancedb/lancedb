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

import java.io.Serializable;
import java.util.Map;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Lance Configuration.
 */
public class LanceConfig implements Serializable {
  private static final long serialVersionUID = 827364827364823764L;
  public static final String CONFIG_DB_PATH = "db";
  public static final String CONFIG_TABLE_NAME = "table";
  private static final String LANCE_FILE_SUFFIX = ".lance";

  private final String dbPath;
  private final String tableName;
  private final String tablePath;

  private LanceConfig(String dbPath, String tableName, String tablePath) {
    this.dbPath = dbPath;
    this.tableName = tableName;
    this.tablePath = tablePath;
  }

  public static LanceConfig from(CaseInsensitiveStringMap options) {
    if (options.containsKey(CONFIG_DB_PATH) && options.containsKey(CONFIG_TABLE_NAME)) {
      return new Builder()
          .setDbPath(options.get(CONFIG_DB_PATH))
          .setTableName(options.get(CONFIG_TABLE_NAME))
          .build();
    } else {
      throw new IllegalArgumentException("Missing required options");
    }
  }

  public static LanceConfig from(Map<String, String> properties) {
    if (properties.containsKey(CONFIG_DB_PATH) && properties.containsKey(CONFIG_TABLE_NAME)) {
      return new Builder()
          .setDbPath(properties.get(CONFIG_DB_PATH))
          .setTableName(properties.get(CONFIG_TABLE_NAME))
          .build();
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
    return tablePath;
  }

  public static class Builder {
    private String dbPath;
    private String tableName;

    public Builder setDbPath(String dbPath) {
      this.dbPath = dbPath;
      return this;
    }

    public Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public LanceConfig build() {
      if (dbPath == null || tableName == null) {
        throw new IllegalArgumentException("dbPath and tableName are required");
      }
      String tablePath = calculateTablePath(dbPath, tableName);
      return new LanceConfig(dbPath, tableName, tablePath);
    }

    private String calculateTablePath(String dbPath, String tableName) {
      StringBuilder sb = new StringBuilder().append(dbPath);
      if (!dbPath.endsWith("/")) {
        sb.append("/");
      }
      return sb.append(tableName).append(LANCE_FILE_SUFFIX).toString();
    }
  }
}
