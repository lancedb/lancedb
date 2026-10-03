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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Specification selecting Lance's MemWAL LSM-style write path for {@code mergeInsert}.
 *
 * <p>Construct via {@link #bucket}, {@link #identity}, or {@link #unsharded}, then optionally chain
 * {@link #withMaintainedIndexes} and {@link #withWriterConfigDefaults}. Install it with {@link
 * LanceDbTableLsm#setLsmWriteSpec} and remove it with {@link LanceDbTableLsm#unsetLsmWriteSpec}.
 *
 * <p>This is deliberately not {@code org.lance.memwal.InitializeMemWalParams}. That type is Lance's
 * own, and its maintained-index default is the opposite of this one: it defaults to maintaining
 * <em>nothing</em>, while a fresh spec here maintains <em>every</em> index. It also cannot express
 * the null that asks the server to resolve the set.
 */
public class LsmWriteSpec {

  /** How writes are routed to MemWAL shards. */
  public enum Sharding {
    /** Hash-bucket writes by a scalar column. */
    BUCKET("bucket"),
    /** Shard by the raw value of a scalar column. */
    IDENTITY("identity"),
    /** Route every write to a single shard. */
    UNSHARDED("unsharded");

    private final String wireName;

    Sharding(String wireName) {
      this.wireName = wireName;
    }

    String wireName() {
      return wireName;
    }

    static Sharding fromWireName(String name) {
      for (Sharding s : values()) {
        if (s.wireName.equals(name)) {
          return s;
        }
      }
      throw new IllegalArgumentException("Unknown sharding mode: " + name);
    }
  }

  private final Sharding sharding;
  private final String column;
  private final Integer numBuckets;
  private final List<String> maintainedIndexes;
  private final Map<String, String> writerConfigDefaults;

  private LsmWriteSpec(
      Sharding sharding,
      String column,
      Integer numBuckets,
      List<String> maintainedIndexes,
      Map<String, String> writerConfigDefaults) {
    this.sharding = sharding;
    this.column = column;
    this.numBuckets = numBuckets;
    this.maintainedIndexes = maintainedIndexes;
    this.writerConfigDefaults = writerConfigDefaults;
  }

  /**
   * Hash-bucket sharding by a scalar column, maintaining every index on the table.
   *
   * <p>Iceberg-compatible Murmur3-x86-32 (seed 0) is used, so each row's {@code bucket(column,
   * numBuckets)} value is stable across processes.
   *
   * @param column A non-nested column with a supported scalar type.
   * @param numBuckets The number of buckets, in {@code [1, 1024]}.
   */
  public static LsmWriteSpec bucket(String column, int numBuckets) {
    if (column == null || column.trim().isEmpty()) {
      throw new IllegalArgumentException("Column cannot be null or empty");
    }
    return new LsmWriteSpec(
        Sharding.BUCKET, column, numBuckets, null, new HashMap<String, String>());
  }

  /**
   * Identity sharding — shard by the raw value of {@code column} — maintaining every index on the
   * table.
   *
   * <p>{@code column} must be a deterministic function of the unenforced primary key: every row
   * with a given primary key must always produce the same {@code column} value, or upserts of that
   * key can land in different shards and a stale version can win.
   */
  public static LsmWriteSpec identity(String column) {
    if (column == null || column.trim().isEmpty()) {
      throw new IllegalArgumentException("Column cannot be null or empty");
    }
    return new LsmWriteSpec(Sharding.IDENTITY, column, null, null, new HashMap<String, String>());
  }

  /** No sharding — every write goes to a single MemWAL shard — maintaining every index. */
  public static LsmWriteSpec unsharded() {
    return new LsmWriteSpec(Sharding.UNSHARDED, null, null, null, new HashMap<String, String>());
  }

  /**
   * Set the indexes the MemWAL keeps up to date as rows are appended.
   *
   * <p>Pass {@code null} — the default for a fresh spec — to maintain every index the MemWAL can,
   * resolved when the spec is installed. That is a snapshot: indexes created later are not
   * maintained until the spec is unset and set again. Pass an empty list to maintain none.
   *
   * <p>Note that {@code null} and the empty list mean opposite things here.
   */
  public LsmWriteSpec withMaintainedIndexes(List<String> maintainedIndexes) {
    return new LsmWriteSpec(
        sharding,
        column,
        numBuckets,
        maintainedIndexes == null ? null : new ArrayList<String>(maintainedIndexes),
        writerConfigDefaults);
  }

  /**
   * Set default {@code ShardWriter} configuration recorded in the MemWAL index.
   *
   * <p>A sparse override map — only the keys you set are recorded. Recognized keys include {@code
   * durable_write}, {@code max_wal_buffer_size}, {@code max_memtable_size}, {@code
   * max_memtable_rows}, {@code max_memtable_batches}, {@code manifest_scan_batch_size}, {@code
   * max_unflushed_memtable_bytes}, and {@code enable_memtable}. Duration knobs carry an {@code _ms}
   * suffix, such as {@code max_wal_flush_interval_ms}.
   */
  public LsmWriteSpec withWriterConfigDefaults(Map<String, String> writerConfigDefaults) {
    if (writerConfigDefaults == null) {
      throw new IllegalArgumentException("writerConfigDefaults cannot be null");
    }
    return new LsmWriteSpec(
        sharding,
        column,
        numBuckets,
        maintainedIndexes,
        new HashMap<String, String>(writerConfigDefaults));
  }

  /** How writes are routed to shards. */
  public Sharding sharding() {
    return sharding;
  }

  /** The sharding column for {@link Sharding#BUCKET} and {@link Sharding#IDENTITY}, else null. */
  public String column() {
    return column;
  }

  /** The bucket count for {@link Sharding#BUCKET}, else null. */
  public Integer numBuckets() {
    return numBuckets;
  }

  /**
   * The indexes the MemWAL maintains, or null to have the server resolve every maintainable index
   * on install. An empty list means none.
   */
  public List<String> maintainedIndexes() {
    return maintainedIndexes == null ? null : Collections.unmodifiableList(maintainedIndexes);
  }

  /** Default {@code ShardWriter} configuration recorded in the MemWAL index. */
  public Map<String, String> writerConfigDefaults() {
    return Collections.unmodifiableMap(writerConfigDefaults);
  }

  /** Render this spec as the {@code set_lsm_write_spec} request body. */
  Map<String, Object> toRequestBody() {
    Map<String, Object> shardingBody = new LinkedHashMap<String, Object>();
    shardingBody.put("mode", sharding.wireName());
    if (column != null) {
      shardingBody.put("column", column);
    }
    if (numBuckets != null) {
      shardingBody.put("num_buckets", numBuckets);
    }

    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("sharding", shardingBody);
    // Null is meaningful: it asks the server to resolve every maintainable index.
    body.put("maintained_indexes", maintainedIndexes);
    body.put("writer_config_defaults", writerConfigDefaults);
    return body;
  }

  /**
   * Rebuild a spec from a {@code get_lsm_write_spec} response body.
   *
   * <p>The server always reports a concrete maintained-index list, so a null selection never
   * round-trips.
   */
  static LsmWriteSpec fromJson(JsonNode node) {
    JsonNode shardingNode = node.get("sharding");
    if (shardingNode == null || shardingNode.get("mode") == null) {
      throw new IllegalStateException("get_lsm_write_spec response has no sharding mode");
    }
    Sharding sharding = Sharding.fromWireName(shardingNode.get("mode").asText());

    String column = shardingNode.hasNonNull("column") ? shardingNode.get("column").asText() : null;
    Integer numBuckets =
        shardingNode.hasNonNull("num_buckets") ? shardingNode.get("num_buckets").asInt() : null;

    List<String> maintainedIndexes = new ArrayList<String>();
    JsonNode indexesNode = node.get("maintained_indexes");
    if (indexesNode != null && indexesNode.isArray()) {
      for (JsonNode index : indexesNode) {
        maintainedIndexes.add(index.asText());
      }
    }

    Map<String, String> defaults = new HashMap<String, String>();
    JsonNode defaultsNode = node.get("writer_config_defaults");
    if (defaultsNode != null && defaultsNode.isObject()) {
      defaultsNode
          .fieldNames()
          .forEachRemaining(name -> defaults.put(name, defaultsNode.get(name).asText()));
    }

    return new LsmWriteSpec(sharding, column, numBuckets, maintainedIndexes, defaults);
  }

  @Override
  public String toString() {
    return "LsmWriteSpec{sharding="
        + sharding
        + ", column="
        + column
        + ", numBuckets="
        + numBuckets
        + ", maintainedIndexes="
        + maintainedIndexes
        + ", writerConfigDefaults="
        + writerConfigDefaults
        + "}";
  }
}
