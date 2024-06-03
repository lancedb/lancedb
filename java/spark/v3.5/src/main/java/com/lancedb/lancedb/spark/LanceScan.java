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

import com.lancedb.lancedb.spark.internal.LanceConfig;
import org.apache.arrow.util.Preconditions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.Serializable;
import java.util.List;
import java.util.stream.IntStream;

public class LanceScan implements Batch, Scan, Serializable {
  private static final long serialVersionUID = 947284762748623947L;

  private final StructType schema;
  private final LanceConfig options;

  public LanceScan(StructType schema, LanceConfig options) {
    this.schema = schema;
    this.options = options;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    List<LanceSplit> splits = LanceSplit.generateLanceSplits(options);
    return IntStream.range(0, splits.size())
        .mapToObj(i -> new LanceInputPartition(schema, i, splits.get(i), options))
        .toArray(InputPartition[]::new);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new LanceReaderFactory();
  }

  @Override
  public StructType readSchema() {
    return schema;
  }

  private class LanceReaderFactory implements PartitionReaderFactory {
    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      Preconditions.checkArgument(partition instanceof LanceInputPartition,
          "Unknown InputPartition type. Expecting LanceInputPartition");
      return LanceRowPartitionReader.create((LanceInputPartition) partition);
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
      Preconditions.checkArgument(partition instanceof LanceInputPartition,
          "Unknown InputPartition type. Expecting LanceInputPartition");
      return new LanceColumnarPartitionReader((LanceInputPartition) partition);
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
      return true;
    }
  }
}
