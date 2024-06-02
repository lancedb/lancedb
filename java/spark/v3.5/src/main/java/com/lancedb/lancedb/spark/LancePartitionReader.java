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

import com.lancedb.lancedb.spark.internal.LanceFragmentInternalRowScanner;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;
import java.util.Iterator;

public class LancePartitionReader implements PartitionReader<InternalRow> {
  private final LanceInputPartition inputPartition;
  private int fragmentIndex;
  private LanceFragmentInternalRowScanner fragmentReader;
  private Iterator<InternalRow> currentBatch;
  private InternalRow currentRecord;

  public LancePartitionReader(LanceInputPartition inputPartition) {
    this.inputPartition = inputPartition;
    this.fragmentIndex = 0;
  }

  @Override
  public boolean next() throws IOException {
    // Read from current batch
    if (currentBatch != null && currentBatch.hasNext()) {
      currentRecord = currentBatch.next();
      return true;
    }
    // Read from next batch
    if (fragmentReader != null && fragmentReader.loadNextBatch()) {
      currentBatch = fragmentReader.getCurrentBatchIterator();
      if (currentBatch != null && currentBatch.hasNext()) {
        currentRecord = currentBatch.next();
        return true;
      }
    }
    // Read from next fragments
    while (fragmentIndex < inputPartition.getLanceSplit().getFragments().size()) {
      if (fragmentReader != null) {
        fragmentReader.close();
      }
      fragmentReader = LanceFragmentInternalRowScanner.create(
          inputPartition.getLanceSplit().getFragments().get(fragmentIndex),
          inputPartition.getConfig().getTablePath());
      fragmentIndex++;
      if (fragmentReader.loadNextBatch()) {
        currentBatch = fragmentReader.getCurrentBatchIterator();
        if (currentBatch != null && currentBatch.hasNext()) {
          currentRecord = currentBatch.next();
          return true;
        }
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
    if (fragmentReader != null) {
      try {
        fragmentReader.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }
}
