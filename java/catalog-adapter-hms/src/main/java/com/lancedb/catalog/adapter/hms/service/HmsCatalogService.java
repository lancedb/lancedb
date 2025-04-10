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
package com.lancedb.catalog.adapter.hms.service;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.ReadOptions;
import com.lancedb.lance.WriteParams;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.lancedb.catalog.adapter.hms.service.client.ClientPool;
import com.lancedb.catalog.adapter.hms.service.client.HiveClientPool;
import com.lancedb.catalog.adapter.hms.service.client.Utils;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.lancedb.catalog.adapter.hms.service.client.Constants.LANCE_DATASET_SUFFIX;
import static com.lancedb.catalog.adapter.hms.service.client.Constants.LANCE_TYPE;

public class HmsCatalogService {

  private static final Logger LOGGER = LoggerFactory.getLogger(HmsCatalogService.class);

  public static final String TABLE_TYPE_PROP = "table_type";

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
  public List<String> getDatabases(Optional<String> startAfter, Optional<Integer> limit)
      throws TException, InterruptedException {
    List<String> allDatabases = clients.run(IMetaStoreClient::getAllDatabases);

    int startIndex = 0;
    if (startAfter.isPresent()) {
      String afterDb = startAfter.get();
      int afterIndex = allDatabases.indexOf(afterDb);
      if (afterIndex >= 0) {
        startIndex = afterIndex + 1;
      }
    }

    if (startIndex >= allDatabases.size()) {
      return Collections.emptyList();
    }

    int endIndex = allDatabases.size();
    if (limit.isPresent()) {
      endIndex = Math.min(startIndex + limit.get(), endIndex);
    }

    return allDatabases.subList(startIndex, endIndex);
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

  public void createDatabase(String dbName) throws TException, InterruptedException {}

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

  public Dataset createTable(String dbName, String tableName, Schema schema) {
    return createTable(dbName, tableName, schema, null, null);
  }

  public Dataset createTable(
      String dbName,
      String tableName,
      Schema schema,
      String location,
      Map<String, String> properties) {

    return buildTable(dbName, tableName, schema)
        .withLocation(location)
        .withProperties(properties)
        .create();
  }

  private Table loadHmsTable(String dbName, String tableName)
      throws TException, InterruptedException {
    try {
      return clients.run(client -> client.getTable(dbName, tableName));
    } catch (NoSuchObjectException nte) {
      LOGGER.trace("Table not found {}", dbName + "." + tableName, nte);
      return null;
    }
  }

  public HMSTableBuilder buildTable(String dbName, String tableName, Schema schema) {
    return new HMSTableBuilder(dbName, tableName, schema);
  }

  protected class HMSTableBuilder {

    private final String dbName;
    private final String tableName;
    private final Schema schema;
    private String location = null;
    private Map<String, String> storageOption;

    public HMSTableBuilder(String dbName, String tableName, Schema schema) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(dbName), "Database can not be null or empty.");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(tableName), "Table can not be null or empty.");

      this.dbName = dbName;
      this.tableName = tableName;
      this.schema = schema;
    }

    public HMSTableBuilder withLocation(String location) {
      this.location = location;
      return this;
    }

    public HMSTableBuilder withProperties(Map<String, String> properties) {
      this.storageOption = properties;
      return this;
    }

    public HMSTableBuilder withProperty(String key, String value) {
      this.storageOption.put(key, value);
      return this;
    }

    public Dataset create() {
      try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
        Table hmsTable = newHmsTable("admin", dbName, tableName, null);

        LOGGER.info("Creating table: {}.{} in hms.", dbName, tableName);
        clients.run(
            client -> {
              client.createTable(hmsTable);
              return null;
            });

        Table hmsTbl = loadHmsTable(dbName, tableName);
        if (hmsTbl != null) {
          String dsLocation = Utils.renameScheme(hmsTbl.getSd().getLocation());
          LOGGER.info("Creating dataset: {}.{} under {}", dbName, tableName, location);

          return Dataset.create(
              allocator,
              dsLocation,
              schema,
              new WriteParams.Builder().withStorageOptions(this.storageOption).build());
        } else {
          throw new RuntimeException(
              "Failed to create dataset (can not load): " + dbName + "." + tableName);
        }
      } catch (TException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public Dataset register() {
      try {
        Table hmsTable = newHmsTable("admin", dbName, tableName, this.location);

        LOGGER.info("Register table: {}.{} in hms.", dbName, tableName);
        clients.run(
            client -> {
              client.createTable(hmsTable);
              return null;
            });

        Table hmsTbl = loadHmsTable(dbName, tableName);
        if (hmsTbl != null) {
          String dsLocation = Utils.renameScheme(hmsTbl.getSd().getLocation());
          LOGGER.info("Open table: {} under {}", tableName, location);
          return Dataset.open(
              dsLocation, new ReadOptions.Builder().setStorageOptions(this.storageOption).build());
        } else {
          throw new RuntimeException(
              "Failed to create dataset (can not load): " + dbName + "." + tableName);
        }
      } catch (TException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    private Table newHmsTable(
        String hmsTableOwner, String dbName, String tableName, String location)
        throws TException, InterruptedException {
      Preconditions.checkNotNull(hmsTableOwner, "'hmsOwner' parameter can't be null");
      final long currentTimeMillis = System.currentTimeMillis();

      Map<String, String> parameters = Maps.newHashMap();
      parameters.put(
          serdeConstants.SERIALIZATION_CLASS,
          "org.apache.hadoop.hive.serde2.thrift.test.IntString");
      parameters.put(
          serdeConstants.SERIALIZATION_FORMAT, "org.apache.thrift.protocol.TBinaryProtocol");

      SerDeInfo serDeInfo =
          new SerDeInfo(
              null, "org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer", parameters);

      Database database = clients.run(client -> client.getDatabase(dbName));
      String tbLocation;
      if (location == null || location.isEmpty()) {
        String dbLocation = StringUtils.stripEnd(database.getLocationUri(), "/");
        tbLocation = String.format("%s/%s%s", dbLocation, tableName, LANCE_DATASET_SUFFIX);
      } else {
        tbLocation = location;
      }

      StorageDescriptor sd =
          new StorageDescriptor(
              Lists.newArrayList(),
              tbLocation,
              LANCE_TYPE,
              LANCE_TYPE,
              false,
              -1,
              serDeInfo,
              Lists.newArrayList(),
              Lists.newArrayList(),
              Maps.newHashMap());

      Map<String, String> tableParams = Maps.newHashMap();
      tableParams.put(TABLE_TYPE_PROP, LANCE_TYPE);

      Table newTable =
          new Table(
              tableName,
              dbName,
              hmsTableOwner,
              (int) currentTimeMillis / 1000,
              (int) currentTimeMillis / 1000,
              Integer.MAX_VALUE,
              sd,
              Collections.emptyList(),
              tableParams,
              "",
              "",
              TableType.EXTERNAL_TABLE.name());

      newTable
          .getParameters()
          .put("EXTERNAL", "TRUE"); // using the external table type also requires this
      newTable.getParameters().put("storage_format", LANCE_TYPE);

      return newTable;
    }
  }
}
