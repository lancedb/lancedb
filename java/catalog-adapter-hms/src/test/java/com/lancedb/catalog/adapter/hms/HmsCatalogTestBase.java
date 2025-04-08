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
import com.lancedb.catalog.adapter.hms.service.HmsCatalogService;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;

public class HmsCatalogTestBase {

  @TempDir protected static Path tempFolder;

  protected static final String TABLE_NAME = "tbl";
  protected static final String DB_NAME = "hivedb";
  protected static final Schema SCHEMA =
      new Schema(
          Arrays.asList(
              Field.nullable("id", new ArrowType.Int(32, true)),
              Field.nullable("name", new ArrowType.Utf8())),
          null);
  protected static String TABLE_LOCATION;

  protected static HmsCatalogService catalogService;

  @RegisterExtension
  protected static final HmsExtension HIVE_METASTORE_EXTENSION =
      HmsExtension.builder().withDatabase(DB_NAME).build();

  @BeforeAll
  protected static void init() {
    catalogService = new HmsCatalogService();
    TABLE_LOCATION = tempFolder.toAbsolutePath().toString();
    catalogService.createTable(DB_NAME, TABLE_NAME, SCHEMA, TABLE_LOCATION, Maps.newHashMap());
  }
}
