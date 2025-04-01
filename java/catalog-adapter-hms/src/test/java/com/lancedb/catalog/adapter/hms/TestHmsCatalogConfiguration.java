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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class TestHmsCatalogConfiguration extends HmsCatalogTestBase {

  @Test
  public void testGetConf() {
    //    HmsCatalogService catalog = new HmsCatalogService();
    Configuration conf = catalogService.getConf();

    assertThat(conf).isNotNull();

    String metastoreUris = conf.get("hive.metastore.uris");
    // here because we inited a hms for the whole tests,
    // so its value will override the value in test/resource/hive-site.xml
    assertNotEquals("thrift://localhost:9083", metastoreUris);

    String configValInHiveSite = conf.get("customized.key");
    // here we verify the configuration can read the value in
    // test/resource/hive-site.xml
    // the behavior is: the sdk would search all the configuration files
    // and merge all the config items
    assertEquals("customized.value", configValInHiveSite);
  }
}
