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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class HmsCatalogService {

  private static final Logger LOGGER = LoggerFactory.getLogger(HmsCatalogService.class);

  private final ClientPool<IMetaStoreClient, TException> clients;
  private final Configuration conf;

  public HmsCatalogService() {
    this(new HiveConf());
  }

  public Configuration getConf() {
    return conf;
  }

  public HmsCatalogService(Configuration conf) {
    this.conf = conf;
    this.clients = new HiveClientPool(1, this.conf);
    String val = this.conf.get("hive.metastore.uris");
    System.out.println(val);
  }

  // ----------------------------------
  //        Database APIs
  // ----------------------------------

  /**
   * Get all databases.
   *
   * @return
   * @throws TException
   * @throws InterruptedException
   */
  public List<String> getDatabases() throws TException, InterruptedException {
    return clients.run(IMetaStoreClient::getAllDatabases).stream().collect(Collectors.toList());
  }

  public void dropDatabase(String dbName) throws TException, InterruptedException {
    try {
      clients.run(
          client -> {
            client.dropDatabase(
                dbName, false /* deleteData */, false /* ignoreUnknownDb */, false /* cascade */);
            return null;
          });
    } catch (NoSuchObjectException e) {
      LOGGER.warn("Dropping database {}, but it does not exist", dbName);
    }
  }

  // ----------------------------------
  //        Table APIs
  // ----------------------------------

  public List<String> getTables(String dbName) throws TException, InterruptedException {
    return clients.run(client -> client.getAllTables(dbName));
  }

  public void dropTable(String dbName, String tableName) throws TException, InterruptedException {
    try {
      clients.run(
          client -> {
            client.dropTable(
                dbName,
                tableName,
                false /* do not delete data */,
                false /* throw NoSuchObjectException if the table doesn't exist */);
            return null;
          });
    } catch (NoSuchObjectException e) {
      LOGGER.warn("Try to drop table " + tableName + ", but it does not exist.");
    }
  }
}
