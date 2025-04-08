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

import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHmsCatalogService extends HmsCatalogTestBase {

  //  @AfterAll
  //  public static void cleanDatasetsAndMetas() throws TException, InterruptedException {
  //    // drop dataset
  //    catalogService.dropTable(DB_NAME, TABLE_NAME);
  //
  //    // drop namespace
  //    catalogService.dropDatabase(DB_NAME);
  //  }

  @Test
  public void testGetDatabases() throws TException, InterruptedException {
    List<String> databases = catalogService.getDatabases(Optional.empty(), Optional.empty());

    assertThat(databases).isNotNull();

    assertTrue(databases.contains(DB_NAME));
  }

  @Test
  public void testGetTables() throws TException, InterruptedException {
    List<String> tables = catalogService.getTables(DB_NAME);

    assertThat(tables).isNotNull();

    assertTrue(tables.contains(TABLE_NAME));
  }
}
