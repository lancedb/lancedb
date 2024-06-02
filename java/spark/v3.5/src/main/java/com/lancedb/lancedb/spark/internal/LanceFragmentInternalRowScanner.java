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

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.Iterator;

public class LanceFragmentInternalRowScanner implements AutoCloseable {
  private final LanceFragmentScanner fragmentScanner;
  private final ArrowReader arrowReader;
  private ColumnarBatch currentColumnarBatch;
  private Iterator<InternalRow> currentBatchIterator;
  private boolean closed = false;

  public LanceFragmentInternalRowScanner(LanceFragmentScanner fragmentScanner,
      ArrowReader arrowReader) {
    this.fragmentScanner = fragmentScanner;
    this.arrowReader = arrowReader;
  }

  public static LanceFragmentInternalRowScanner create(int fragmentId, String tablePath) {
    LanceFragmentScanner fragmentScanner = LanceReader.getFragmentScanner(fragmentId, tablePath);
    return new LanceFragmentInternalRowScanner(fragmentScanner, fragmentScanner.getArrowReader());
  }

  public boolean loadNextBatch() throws IOException {
    if (arrowReader.loadNextBatch()) {
      if (currentBatchIterator != null) {
        currentColumnarBatch.close();
      }
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      currentColumnarBatch = new ColumnarBatch(root.getFieldVectors().stream()
          .map(ArrowColumnVector::new).toArray(ArrowColumnVector[]::new), root.getRowCount());
      currentBatchIterator = currentColumnarBatch.rowIterator();
      return currentBatchIterator.hasNext();
    }
    return false;
  }

  public Iterator<InternalRow> getCurrentBatchIterator() {
    return currentBatchIterator;
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    if (currentColumnarBatch != null) {
      currentColumnarBatch.close();
    }
    arrowReader.close();
    fragmentScanner.close();
    closed = true;
  }
}
