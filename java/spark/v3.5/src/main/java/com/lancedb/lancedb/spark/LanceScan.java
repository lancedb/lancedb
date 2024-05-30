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
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

public class LanceScan implements Batch, Scan {
  private final StructType schema;
  private final LanceDataSourceReadOptions options;
  
  public LanceScan(StructType schema, LanceDataSourceReadOptions options) {
    this.schema = schema;
    this.options = options;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    // Return fragments???
    return new InputPartition[0];
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return null;
  }

  @Override
  public StructType readSchema() {
    return schema;
  }
}
