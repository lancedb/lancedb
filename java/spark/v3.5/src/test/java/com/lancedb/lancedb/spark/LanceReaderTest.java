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

import com.lancedb.lancedb.spark.internal.LanceFragmentScanner;
import com.lancedb.lancedb.spark.internal.LanceReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class LanceReaderTest {
  
  @Test
  public void testSchema() {
    StructType expectedSchema = TestUtils.TestTable1Config.expectedSchema;
    StructType schema = LanceReader.getSchema(TestUtils.TestTable1Config.lanceConfig);
    assertNotNull(schema);
    assertEquals(expectedSchema, schema);
  }
  
  @Test
  public void testFragmentIds() {
    List<Integer> fragments = LanceReader.getFragmentIds(TestUtils.TestTable1Config.lanceConfig);
    assertEquals(2, fragments.size());
    assertEquals(0, fragments.get(0));
    assertEquals(1, fragments.get(1));
  }
  
  @Test
  public void getFragmentScanner() throws IOException {
    List<List<Object>> expectedValues = Arrays.asList(
        Arrays.asList(0L, 0L, 0L, 0L),
        Arrays.asList(1L, 2L, 3L, -1L)
    );
    validateFragment(expectedValues, 0);
    List<List<Object>> expectedValues1 = Arrays.asList(
        Arrays.asList(2L, 4L, 6L, -2L),
        Arrays.asList(3L, 6L, 9L, -3L)
    );
    validateFragment(expectedValues1, 1);
  }
  
  public void validateFragment(List<List<Object>> expectedValues, int fragment) throws IOException {
    try (LanceFragmentScanner scanner = LanceReader.getFragmentScanner(
        fragment, TestUtils.TestTable1Config.tablePath)) {
      try (ArrowReader reader = scanner.getArrowReader()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        assertNotNull(root);
        
        while (reader.loadNextBatch()) {
          for (int i = 0; i < root.getRowCount(); i++) {
            for (int j = 0; j < root.getFieldVectors().size(); j++) {
              assertEquals(expectedValues.get(i).get(j), root.getFieldVectors().get(j).getObject(i));
            }
          }
        }
      }
    }
  }
  
  // TODO test_table4 [UNSUPPORTED_ARROWTYPE] Unsupported arrow type FixedSizeList(128).
}
