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

package com.lancedb.lancedb.spark.internal;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.DatasetFragment;
import com.lancedb.lance.ipc.LanceScanner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.io.IOException;

public class LanceFragmentScanner implements AutoCloseable {
  private Dataset dataset;
  private DatasetFragment fragment;
  private LanceScanner scanner;

  private LanceFragmentScanner(Dataset dataset, DatasetFragment fragment, LanceScanner scanner) {
    this.dataset = dataset;
    this.fragment = fragment;
    this.scanner = scanner;
  }

  public static LanceFragmentScanner create(int fragmentId,
      String tablePath, BufferAllocator allocator) {
    Dataset dataset = null;
    DatasetFragment fragment = null;
    LanceScanner scanner = null;
    try {
      dataset = Dataset.open(tablePath, allocator);
      fragment = dataset.getFragments().get(fragmentId);
      scanner = fragment.newScan();
    } catch (Throwable t) {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (Throwable it) {
          t.addSuppressed(it);
        }
      }
      if (dataset != null) {
        try {
          dataset.close();
        } catch (Throwable it) {
          t.addSuppressed(it);
        }
      }
      throw t;
    }
    return new LanceFragmentScanner(dataset, fragment, scanner);
  }

  /**
   * @return the arrow reader. The caller is responsible for closing the reader
   */
  public ArrowReader getArrowReader() {
    return scanner.scanBatches();
  }

  @Override
  public void close() throws IOException {
    if (scanner != null) {
      try {
        scanner.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    if (dataset != null) {
      dataset.close();
    }
  }
}