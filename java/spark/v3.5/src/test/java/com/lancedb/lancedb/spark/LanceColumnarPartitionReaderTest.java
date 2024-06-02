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

import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class LanceColumnarPartitionReaderTest {
  @Test
  public void test() throws Exception {
    LanceSplit split = new LanceSplit(Arrays.asList(0, 1));
    LanceInputPartition partition = new LanceInputPartition(
        TestUtils.TestTable1Config.schema, 0, split, TestUtils.TestTable1Config.lanceConfig);
    try (LanceColumnarPartitionReader reader = new LanceColumnarPartitionReader(partition)) {
      List<List<Long>> expectedValues = TestUtils.TestTable1Config.expectedValues;
      int rowIndex = 0;

      while (reader.next()) {
        ColumnarBatch batch = reader.get();
        assertNotNull(batch);

        for (int i = 0; i < batch.numRows(); i++) {
          for (int j = 0; j < batch.numCols(); j++) {
            long actualValue = batch.column(j).getLong(i);
            long expectedValue = expectedValues.get(rowIndex).get(j);
            assertEquals(expectedValue, actualValue, "Mismatch at row " + rowIndex + " column " + j);
          }
          rowIndex++;
        }
        batch.close();
      }

      assertEquals(expectedValues.size(), rowIndex);
    }
  }
}
