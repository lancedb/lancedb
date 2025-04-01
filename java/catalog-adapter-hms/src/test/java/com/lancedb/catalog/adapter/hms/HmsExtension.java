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
package com.lancedb.catalog.adapter.hms;

import com.google.common.collect.Maps;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Map;

public class HmsExtension implements BeforeAllCallback, AfterAllCallback {
  private HiveMetaStoreClient metastoreClient;
  private HmsMiniCluster hmsMiniCluster;
  private final Map<String, String> hiveConfOverride;
  private final String databaseName;

  private HmsExtension(String databaseName, Map<String, String> hiveConfOverride) {
    this.databaseName = databaseName;
    this.hiveConfOverride = hiveConfOverride;
  }

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    hmsMiniCluster = new HmsMiniCluster();
    HiveConf hiveConfWithOverrides = new HiveConf(HmsMiniCluster.class);
    if (hiveConfOverride != null) {
      for (Map.Entry<String, String> kv : hiveConfOverride.entrySet()) {
        hiveConfWithOverrides.set(kv.getKey(), kv.getValue());
      }
    }

    hmsMiniCluster.start(hiveConfWithOverrides);
    metastoreClient = new HiveMetaStoreClient(hiveConfWithOverrides);
    if (null != databaseName) {
      String dbPath = hmsMiniCluster.getDatabasePath(databaseName);
      Database db = new Database(databaseName, "description", dbPath, Maps.newHashMap());
      metastoreClient.createDatabase(db);
    }
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) throws Exception {
    if (null != metastoreClient) {
      metastoreClient.close();
    }

    if (null != hmsMiniCluster) {
      hmsMiniCluster.stop();
    }

    metastoreClient = null;
    hmsMiniCluster = null;
  }

  public HiveMetaStoreClient metastoreClient() {
    return metastoreClient;
  }

  public HiveConf hiveConf() {
    return hmsMiniCluster.hiveConf();
  }

  public HmsMiniCluster metastoreMiniCluster() {
    return hmsMiniCluster;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String databaseName;
    private Map<String, String> config;

    public Builder() {}

    public Builder withDatabase(String databaseToCreate) {
      this.databaseName = databaseToCreate;
      return this;
    }

    public Builder withConfig(Map<String, String> configToSet) {
      this.config = configToSet;
      return this;
    }

    public HmsExtension build() {
      return new HmsExtension(databaseName, config);
    }
  }
}
