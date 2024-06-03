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

import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.Iterator;

public class LanceRowPartitionReader implements PartitionReader<InternalRow> {
  private final LanceColumnarPartitionReader reader;
  private ColumnarBatch currentBatch;
  private Iterator<InternalRow> currentRows;
  private InternalRow currentRecord;

  public LanceRowPartitionReader(LanceColumnarPartitionReader reader) {
    this.reader = reader;
  }

  public static LanceRowPartitionReader create(LanceInputPartition inputPartition) {
    return new LanceRowPartitionReader(new LanceColumnarPartitionReader(inputPartition));
  }

  @Override
  public boolean next() throws IOException {
    // Read from current batch
    if (currentRows != null && currentRows.hasNext()) {
      currentRecord = currentRows.next();
      return true;
    }
    // Read from next batch
    if (reader.next()) {
      if (currentBatch != null) {
        currentBatch.close();
      }
      currentBatch = reader.get();
      currentRows = currentBatch.rowIterator();
      if (currentRows != null && currentRows.hasNext()) {
        currentRecord = currentRows.next();
        return true;
      }
    }
    return false;
  }

  @Override
  public InternalRow get() {
    return currentRecord;
  }

  @Override
  public void close() throws IOException {
    if (currentBatch != null) {
      currentBatch.close();
    }
    reader.close();
  }
}
