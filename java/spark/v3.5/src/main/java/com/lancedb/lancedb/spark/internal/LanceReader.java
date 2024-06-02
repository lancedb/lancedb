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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;

import java.util.List;
import java.util.stream.Collectors;

public class LanceReader {
  private static final BufferAllocator allocator = new RootAllocator(
      RootAllocator.configBuilder().from(RootAllocator.defaultConfig())
          .maxAllocation(4 * 1024 * 1024).build());

  public static StructType getSchema(LanceConfig options)
  {
    try (Dataset dataset = Dataset.open(options.getTablePath(), allocator)) {
      return ArrowUtils.fromArrowSchema(dataset.getSchema());
    }
  }

  public static List<Integer> getFragmentIds(LanceConfig config) {
    try (Dataset dataset = Dataset.open(config.getTablePath(), allocator)) {
      return dataset.getFragments().stream().map(DatasetFragment::getId).collect(Collectors.toList());
    }
  }

  public static LanceFragmentScanner getFragmentScanner(int fragmentId, String tablePath) {
    return LanceFragmentScanner.create(fragmentId, tablePath, allocator);
  }
}

