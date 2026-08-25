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
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Live state of one bucket. A table is N buckets on one node; flattening to a single number hides
 * the one hot bucket that is usually why someone opened this endpoint.
 */
public class BucketStats {
  private static final String CONTEXT = "bucket stats";

  private final String shardId;
  private final String status;
  private final long writerEpoch;
  private final long manifestVersion;
  private final long currentGeneration;
  private final long replayAfterWalEntryPosition;
  private final long walEntryPositionLastSeen;
  private final List<GenerationStats> generations;
  private final boolean compacting;
  private final List<MemtableStats> memtables;

  BucketStats(
      String shardId,
      String status,
      long writerEpoch,
      long manifestVersion,
      long currentGeneration,
      long replayAfterWalEntryPosition,
      long walEntryPositionLastSeen,
      List<GenerationStats> generations,
      boolean compacting,
      List<MemtableStats> memtables) {
    this.shardId = shardId;
    this.status = status;
    this.writerEpoch = writerEpoch;
    this.manifestVersion = manifestVersion;
    this.currentGeneration = currentGeneration;
    this.replayAfterWalEntryPosition = replayAfterWalEntryPosition;
    this.walEntryPositionLastSeen = walEntryPositionLastSeen;
    this.generations = Collections.unmodifiableList(generations);
    this.compacting = compacting;
    this.memtables = memtables == null ? null : Collections.unmodifiableList(memtables);
  }

  /** The shard this bucket writes. */
  public String shardId() {
    return shardId;
  }

  /** {@code "Active"} or {@code "Sealed"} (drop-table 2PC in flight). */
  public String status() {
    return status;
  }

  /** Epoch of the writer that currently owns the shard. */
  public long writerEpoch() {
    return writerEpoch;
  }

  /** Version of the shard manifest these numbers were read from. */
  public long manifestVersion() {
    return manifestVersion;
  }

  /** The generation the active memtable will become. */
  public long currentGeneration() {
    return currentGeneration;
  }

  /** WAL position replay resumes from. */
  public long replayAfterWalEntryPosition() {
    return replayAfterWalEntryPosition;
  }

  /**
   * Highest WAL position the writer has seen. The difference against {@link
   * #replayAfterWalEntryPosition()} is the WAL lag.
   */
  public long walEntryPositionLastSeen() {
    return walEntryPositionLastSeen;
  }

  /** Flushed L0 generations not yet merged into the base table. */
  public List<GenerationStats> generations() {
    return generations;
  }

  /**
   * Whether a pass owns this bucket's compaction latch right now. Says <em>a</em> driver is
   * running, not <em>whose</em>, and the latch is held from dispatch — including while the pass
   * queues for a pod-wide compactor permit. Read it as "do not pile on", never as "mine is
   * progressing".
   */
  public boolean compacting() {
    return compacting;
  }

  /** Oldest first, active last. Empty for a {@code "Sealed"} bucket, whose state is torn down. */
  public Optional<List<MemtableStats>> memtables() {
    return Optional.ofNullable(memtables);
  }

  /** The newest flushed generation, or empty when L0 is empty. */
  OptionalLong newestGeneration() {
    OptionalLong newest = OptionalLong.empty();
    for (GenerationStats generation : generations) {
      if (!newest.isPresent() || generation.generation() > newest.getAsLong()) {
        newest = OptionalLong.of(generation.generation());
      }
    }
    return newest;
  }

  /**
   * How many generations at or below {@code target} are still in L0.
   *
   * <p>A count, not a boolean: one pass drains a bounded prefix rather than the whole target set,
   * so a boolean would read as "no progress" for every pass but the last. Compaction drains
   * oldest-first, so this decreases monotonically.
   */
  long outstandingGenerations(long target) {
    long count = 0;
    for (GenerationStats generation : generations) {
      if (generation.generation() <= target) {
        count++;
      }
    }
    return count;
  }

  static BucketStats fromJson(JsonNode node) {
    JsonFields.requiredObject(node, CONTEXT);
    List<GenerationStats> generations = new ArrayList<GenerationStats>();
    for (JsonNode generation : JsonFields.requiredArray(node, "generations", CONTEXT)) {
      generations.add(GenerationStats.fromJson(generation));
    }

    JsonNode memtablesNode = JsonFields.optionalArray(node, "memtables", CONTEXT);
    List<MemtableStats> memtables = null;
    if (memtablesNode != null) {
      memtables = new ArrayList<MemtableStats>();
      for (JsonNode memtable : memtablesNode) {
        memtables.add(MemtableStats.fromJson(memtable));
      }
    }

    return new BucketStats(
        JsonFields.requiredText(node, "shard_id", CONTEXT),
        JsonFields.requiredText(node, "status", CONTEXT),
        JsonFields.requiredLong(node, "writer_epoch", CONTEXT),
        JsonFields.requiredLong(node, "manifest_version", CONTEXT),
        JsonFields.requiredLong(node, "current_generation", CONTEXT),
        JsonFields.requiredLong(node, "replay_after_wal_entry_position", CONTEXT),
        JsonFields.requiredLong(node, "wal_entry_position_last_seen", CONTEXT),
        generations,
        JsonFields.requiredBoolean(node, "compacting", CONTEXT),
        memtables);
  }

  @Override
  public String toString() {
    return "BucketStats{shardId="
        + shardId
        + ", status="
        + status
        + ", currentGeneration="
        + currentGeneration
        + ", generations="
        + generations
        + ", compacting="
        + compacting
        + "}";
  }
}
