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
package com.lancedb;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Live per-bucket LSM state, as returned by {@link LanceDbTableLsm#getLsmStats()}.
 *
 * <p>Nothing here is derived: sums and differences (total L0 bytes, WAL lag) are the caller's to
 * compute. There is no "LSM is off" shape — that case is an empty {@link java.util.Optional},
 * because a stats object of zeros would read as measurements.
 */
public class LsmStats {
  private static final String CONTEXT = "lsm stats";

  private final List<BucketStats> buckets;

  LsmStats(List<BucketStats> buckets) {
    this.buckets = Collections.unmodifiableList(buckets);
  }

  /** One entry per bucket. */
  public List<BucketStats> buckets() {
    return buckets;
  }

  static LsmStats fromJson(JsonNode node) {
    JsonFields.requiredObject(node, CONTEXT);
    List<BucketStats> buckets = new ArrayList<BucketStats>();
    for (JsonNode bucket : JsonFields.requiredArray(node, "buckets", CONTEXT)) {
      buckets.add(BucketStats.fromJson(bucket));
    }
    return new LsmStats(buckets);
  }

  @Override
  public String toString() {
    return "LsmStats{buckets=" + buckets + "}";
  }
}
