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

import com.lancedb.lancedb.spark.internal.LanceDataSourceReadOptions;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;

public class LanceScanBuilder implements ScanBuilder {
  private final StructType schema;
  private final LanceDataSourceReadOptions options;
  
  public LanceScanBuilder(StructType schema, LanceDataSourceReadOptions options) {
    this.schema = schema;
    this.options = options;
  }
  
  @Override
  public Scan build() {
    return new LanceScan(schema, options);
  }
}
