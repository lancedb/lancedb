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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * The RTE write path for one LanceDB Enterprise table.
 *
 * <p>Installing an {@link RTEWriteSpec} routes {@code mergeInsert} upserts through Lance's RTE — an
 * LSM-style append — instead of the standard merge path. Rows land in an in-memory memtable, freeze
 * into SSTables, and are merged into the base table by compaction.
 *
 * <p>These routes are not part of the Lance Namespace specification, so they are issued directly
 * rather than through {@link org.lance.namespace.LanceNamespace}.
 *
 * <pre>{@code
 * LanceDbRestClient client = LanceDbNamespaceClientBuilder.newBuilder()
 *     .apiKey("your_lancedb_cloud_api_key")
 *     .database("your_database_name")
 *     .buildRestClient();
 *
 * LanceDbTableRTE rte = new LanceDbTableRTE(client, "my_table");
 * rte.enableRTE(RTEWriteSpec.bucket("id", 16));
 * // ... merge_insert traffic ...
 * rte.checkpointRTE();
 * }</pre>
 */
public class LanceDbTableRTE {

  /**
   * Interval between {@code get_rte_stats} polls during a checkpoint. One interval is roughly one
   * compaction pass, the granularity at which the answer can change.
   */
  private static final long POLL_INTERVAL_MS = 5_000L;

  /**
   * Cap on re-issues from {@code flushRTE} after a 421, so a crash-looping node cannot turn flush →
   * compact → 421 → flush into a spin.
   *
   * <p>Deliberately not shared with {@link #MAX_RETRIES}: a claim that keeps evaporating is a
   * broken node, while contention is routine and wants a real budget.
   */
  private static final int MAX_REISSUES = 3;

  /**
   * Retryable faults tolerated on a <em>single</em> request, reset on every success — scattered
   * contention across a long checkpoint must not accumulate toward a cap.
   */
  private static final int MAX_RETRIES = 8;

  private static final long RETRY_BACKOFF_BASE_MS = 100L;
  private static final long RETRY_BACKOFF_MAX_MS = 5_000L;

  private final LanceDbRestClient client;
  private final String tableIdentifier;

  /**
   * Bind the LSM routes for one table.
   *
   * @param client Transport for the LanceDB endpoint.
   * @param tableIdentifier The table's full identifier, {@code $}-delimited when it sits inside a
   *     namespace, such as {@code analytics$events}.
   */
  public LanceDbTableRTE(LanceDbRestClient client, String tableIdentifier) {
    if (client == null) {
      throw new IllegalArgumentException("Client cannot be null");
    }
    if (tableIdentifier == null || tableIdentifier.trim().isEmpty()) {
      throw new IllegalArgumentException("Table identifier cannot be null or empty");
    }
    this.client = client;
    this.tableIdentifier = tableIdentifier;
  }

  /**
   * Install an {@link RTEWriteSpec} on this table, selecting the RTE write path for future {@code
   * mergeInsert} calls.
   *
   * <p>All variants require the table to have an unenforced primary key; bucket sharding
   * additionally requires it to be the single column being bucketed.
   */
  public void enableRTE(RTEWriteSpec spec) {
    if (spec == null) {
      throw new IllegalArgumentException("Spec cannot be null");
    }
    client.post(route("enable_rte"), spec.toRequestBody());
  }

  /**
   * Remove the {@link RTEWriteSpec} from this table, reverting to the standard {@code mergeInsert}
   * write path.
   *
   * <p>Errors if no spec is currently set.
   */
  public void disableRTE() {
    client.post(route("disable_rte"), null);
  }

  /**
   * Read the {@link RTEWriteSpec} currently installed on this table.
   *
   * <p>Empty when the RTE write path is not enabled at all — no spec has been set, or one was
   * removed with {@link #disableRTE()}. That is a different answer from a present spec whose {@link
   * RTEWriteSpec#maintainedIndexes()} is null, which selects indexes automatically.
   *
   * <p>The spec read back is the one that was installed, selection included: null maintains every
   * supported index the table has now or gains later, an empty list maintains none, and a non-empty
   * list maintains exactly those. All three round-trip.
   */
  public Optional<RTEWriteSpec> getRTESpec() {
    JsonNode response = client.post(route("get_rte_spec"), null);
    if (response == null || !response.hasNonNull("rte_spec")) {
      return Optional.empty();
    }
    return Optional.of(RTEWriteSpec.fromJson(response.get("rte_spec")));
  }

  /**
   * Freeze every table shard's active memtable into a new SSTable.
   *
   * <p>Returns once the freeze is committed. Freezing an empty memtable is a no-op, so this is safe
   * to call repeatedly.
   */
  public void flushRTE() {
    client.post(route("flush_rte"), null);
  }

  /**
   * Trigger a background SSTable compaction pass per table shard.
   *
   * <p>Returns once the passes are <em>dispatched</em>, not once they finish — watch {@link
   * #getRTEStats}, or use {@link #checkpointRTE} to wait for convergence.
   */
  public void compactRTE() {
    client.post(route("compact_rte"), null);
  }

  /**
   * Read live per-table-shard RTE state.
   *
   * <p>Answers "how far behind is my fresh tier", "which table shard is hot", and "why is my
   * fresh-tier vector search brute-force". Mutates no table state.
   *
   * <p>Empty only when the RTE write path is not enabled — that is, when the server sends an absent
   * or null {@code rte_stats}. A stats object that is present is decoded strictly, and a malformed
   * one throws rather than decoding to something empty, because {@link #checkpointRTE} reads
   * convergence out of these numbers and cannot tell a defaulted array from a drained one.
   *
   * @param includeSstableRows Also count rows per SSTable. Off by default because each count opens
   *     an uncached Lance dataset.
   * @throws IllegalStateException if the response is absent or does not decode.
   */
  public Optional<RTEStats> getRTEStats(boolean includeSstableRows) {
    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("include_sstable_rows", includeSstableRows);
    JsonNode response = client.post(route("get_rte_stats"), body);
    if (response == null) {
      throw new IllegalStateException("get_rte_stats returned an empty response body");
    }
    JsonNode stats = response.get("rte_stats");
    if (stats == null || stats.isNull()) {
      return Optional.empty();
    }
    return Optional.of(RTEStats.fromJson(stats));
  }

  /** Equivalent to {@code getRTEStats(false)}. */
  public Optional<RTEStats> getRTEStats() {
    return getRTEStats(false);
  }

  /**
   * Converge this table's RTE write path into its base table.
   *
   * <p>Freezes once, fixes a target watermark from the resulting SSTables, then triggers compaction
   * and polls until those SSTables are gone. The target set is fixed at the start, so sstables
   * created <em>during</em> the checkpoint are ignored — that is what lets it terminate under write
   * load, and what makes it best-effort: it converges the fresh tier as of some instant.
   * Idempotent, abandonable at any point, safe on a cadence.
   *
   * <p>The loop runs here, not on the server: {@link #compactRTE} dispatches a pass and returns, so
   * nothing holds a socket and a client can vanish mid-operation with nothing to reconcile.
   * Completion is read from generation numbers in the shard manifest — durable state, unlike a
   * count in a compact response, which a concurrent write invalidates.
   *
   * <p>No liveness bound — the caller owns the deadline. The compactor pool is shared across
   * tables, so a checkpoint queued behind unrelated work looks exactly like one that is merging.
   */
  public void checkpointRTE() {
    for (int reissue = 0; reissue <= MAX_REISSUES; reissue++) {
      // The freeze turns everything written before this call into an SSTable, so the
      // watermark has to be read after it. Idempotent: freezing an empty memtable is a
      // no-op, so a re-issue does not churn empty sstables.
      if (issueVoid(this::flushRTE)) {
        backoff(reissue);
        continue;
      }

      Attempt<Optional<RTEStats>> stats = issue(() -> getRTEStats(false));
      if (stats.lostClaim) {
        backoff(reissue);
        continue;
      }
      if (!stats.value.isPresent()) {
        // Not WAL-backed; flushRTE would have errored first but for a race.
        return;
      }

      Map<String, Long> targets = newestSstableGenerations(stats.value.get());
      if (targets.isEmpty()) {
        return;
      }

      if (drainToTargets(targets)) {
        return;
      }
      backoff(reissue);
    }
    throw new IllegalStateException(
        "checkpointRTE: the owning node kept losing its claim; re-issued from flush the maximum "
            + "number of times");
  }

  /**
   * Trigger and poll until no table shard holds an SSTable generation at or below its target.
   *
   * @return true when the drain finished, false when the table needs re-claiming from flush.
   */
  private boolean drainToTargets(Map<String, Long> targets) {
    while (true) {
      Attempt<Optional<RTEStats>> stats = issue(() -> getRTEStats(false));
      if (stats.lostClaim) {
        return false;
      }
      if (!stats.value.isPresent()) {
        return true;
      }

      // `compacting` is the table shard's compaction latch, held from dispatch until the pass
      // ends — including while it waits on a pod-wide permit. So it answers one question
      // only: do not pile on. Table shards with nothing outstanding are skipped, not counted
      // as idle.
      long outstanding = 0;
      boolean allCompacting = true;
      for (TableShardStats tableShard : stats.value.get().tableShards()) {
        Long target = targets.get(tableShard.shardId());
        if (target == null) {
          continue;
        }
        long remaining = tableShard.outstandingSstables(target);
        if (remaining > 0) {
          outstanding += remaining;
          allCompacting &= tableShard.compacting();
        }
      }
      if (outstanding == 0) {
        return true;
      }

      if (!allCompacting) {
        try {
          compactRTE();
        } catch (LanceDbRestClient.HttpException e) {
          if (isLostClaim(e)) {
            return false;
          }
          if (!isRetryable(e)) {
            throw e;
          }
          // A 429 here means the server could latch no table shard at all, which the poll
          // above already handles. Not retried in place: the latch it would contend for
          // is the one doing the work, so fall through and re-read — POLL_INTERVAL_MS is
          // the backoff.
        }
      }
      sleep(POLL_INTERVAL_MS);
    }
  }

  /** The newest SSTable generation held by each table shard, skipping table shards holding none. */
  private static Map<String, Long> newestSstableGenerations(RTEStats stats) {
    Map<String, Long> targets = new HashMap<String, Long>();
    for (TableShardStats tableShard : stats.tableShards()) {
      OptionalLong newest = tableShard.newestSstableGeneration();
      if (newest.isPresent()) {
        targets.put(tableShard.shardId(), newest.getAsLong());
      }
    }
    return targets;
  }

  /**
   * 429 (latch held, pool saturated, or the pod replaying its WAL) and 503 (a draining node, or a
   * proxy between here and it).
   */
  private static boolean isRetryable(LanceDbRestClient.HttpException e) {
    return e.statusCode() == 429 || e.statusCode() == 503;
  }

  /**
   * 421: the owning node holds no claim. Only {@code flush} re-claims and replays, so this cannot
   * be retried in place — the caller has to start over.
   */
  private static boolean isLostClaim(LanceDbRestClient.HttpException e) {
    return e.statusCode() == 421;
  }

  /**
   * Issue one LSM request, retrying in place while the fault is retryable.
   *
   * <p>The two recoverable faults have separate budgets: contention clears on its own and retries
   * here against {@link #MAX_RETRIES}, while a 421 needs {@code flush} to re-claim, which only the
   * caller can drive.
   *
   * <p>An exhausted budget propagates the last error as itself rather than a synthesized one — "429
   * after nine tries" beats "checkpoint failed".
   */
  private static <T> Attempt<T> issue(Call<T> call) {
    int retries = 0;
    while (true) {
      try {
        return new Attempt<T>(call.run(), false);
      } catch (LanceDbRestClient.HttpException e) {
        if (isLostClaim(e)) {
          return new Attempt<T>(null, true);
        }
        if (!isRetryable(e) || retries >= MAX_RETRIES) {
          throw e;
        }
        backoff(retries);
        retries++;
      }
    }
  }

  /** {@link #issue} for a call with no return value. Returns true when the claim was lost. */
  private static boolean issueVoid(Runnable call) {
    return issue(
            () -> {
              call.run();
              return Boolean.TRUE;
            })
        .lostClaim;
  }

  /** Sleep before re-issuing a retryable request. Doubles up to {@link #RETRY_BACKOFF_MAX_MS}. */
  private static void backoff(int attempt) {
    long delay = RETRY_BACKOFF_BASE_MS << Math.min(attempt, 8);
    sleep(Math.min(delay, RETRY_BACKOFF_MAX_MS));
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting on the LSM checkpoint", e);
    }
  }

  private String route(String operation) {
    return "/v1/table/" + tableIdentifier + "/" + operation + "/";
  }

  /** What one LSM request produced: its value, or word that the owning node holds no claim. */
  private static final class Attempt<T> {
    private final T value;
    private final boolean lostClaim;

    private Attempt(T value, boolean lostClaim) {
      this.value = value;
      this.lostClaim = lostClaim;
    }
  }

  @FunctionalInterface
  private interface Call<T> {
    T run();
  }
}
