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
import org.apache.thrift.TException;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class HmsCatalogService {

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
  }

  public List<String> getDatabases() throws TException, InterruptedException {

    clients.run(IMetaStoreClient::getAllDatabases).stream().collect(Collectors.toList());
    return Collections.singletonList("db1");
  }
}
