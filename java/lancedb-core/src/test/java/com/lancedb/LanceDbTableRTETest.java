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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the RTE LSM routes, run against a scripted local HTTP server.
 *
 * <p>The wire assertions mirror the Rust mocked-endpoint tests in {@code
 * rust/lancedb/src/remote/table.rs}, which are the contract these routes have to match.
 */
public class LanceDbTableRTETest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private HttpServer server;
  private LanceDbRestClient client;
  private LanceDbTableRTE rte;

  private final List<String> requestPaths = Collections.synchronizedList(new ArrayList<String>());
  private final List<String> requestBodies = Collections.synchronizedList(new ArrayList<String>());
  private final Map<String, Deque<Reply>> replies = new ConcurrentHashMap<String, Deque<Reply>>();

  @BeforeEach
  public void setUp() throws IOException {
    start();
  }

  /** Tear down and restart the scripted server, for a test that scripts several exchanges. */
  private void setUpFresh() {
    try {
      client.close();
      server.stop(0);
      requestPaths.clear();
      requestBodies.clear();
      replies.clear();
      start();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          String path = exchange.getRequestURI().getPath();
          requestPaths.add(path);
          requestBodies.add(readAll(exchange.getRequestBody()));

          Reply reply = nextReply(path);
          byte[] out = reply.body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(reply.status, out.length == 0 ? -1 : out.length);
          if (out.length > 0) {
            exchange.getResponseBody().write(out);
          }
          exchange.close();
        });
    server.start();

    client =
        LanceDbNamespaceClientBuilder.newBuilder()
            .apiKey("test-key")
            .database("test-db")
            .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
            .buildRestClient();
    rte = new LanceDbTableRTE(client, "my_table");
  }

  @AfterEach
  public void tearDown() throws IOException {
    client.close();
    server.stop(0);
  }

  // ===========================================================================
  // set / unset / get spec
  // ===========================================================================

  @Test
  public void testEnableRTEUnsharded() throws Exception {
    enqueue("enable_rte", 200, "");

    rte.enableRTE(RTEWriteSpec.unsharded());

    assertEquals("/v1/table/my_table/enable_rte/", requestPaths.get(0));
    JsonNode body = MAPPER.readTree(requestBodies.get(0));
    assertEquals("unsharded", body.get("sharding").get("mode").asText());
    assertFalse(body.get("sharding").has("column"));
    assertFalse(body.get("sharding").has("num_buckets"));
  }

  @Test
  public void testEnableRTEBucket() throws Exception {
    enqueue("enable_rte", 200, "");

    rte.enableRTE(RTEWriteSpec.bucket("id", 16).withMaintainedIndexes(Arrays.asList("id_idx")));

    JsonNode body = MAPPER.readTree(requestBodies.get(0));
    assertEquals("bucket", body.get("sharding").get("mode").asText());
    assertEquals("id", body.get("sharding").get("column").asText());
    assertEquals(16, body.get("sharding").get("num_buckets").asInt());
    assertEquals(1, body.get("maintained_indexes").size());
    assertEquals("id_idx", body.get("maintained_indexes").get(0).asText());
  }

  @Test
  public void testEnableRTEIdentity() throws Exception {
    enqueue("enable_rte", 200, "");

    rte.enableRTE(RTEWriteSpec.identity("tenant"));

    JsonNode body = MAPPER.readTree(requestBodies.get(0));
    assertEquals("identity", body.get("sharding").get("mode").asText());
    assertEquals("tenant", body.get("sharding").get("column").asText());
    assertFalse(body.get("sharding").has("num_buckets"));
  }

  /**
   * The tri-state that motivated a LanceDB-owned spec type: a null selection asks the server to
   * resolve every maintainable index, while an empty list asks for none. They must not collapse.
   */
  @Test
  public void testMaintainedIndexesNullAndEmptyAreDistinctOnTheWire() throws Exception {
    enqueue("enable_rte", 200, "");

    rte.enableRTE(RTEWriteSpec.unsharded());
    JsonNode fresh = MAPPER.readTree(requestBodies.get(0));
    assertTrue(fresh.has("maintained_indexes"), "the key must be present");
    assertTrue(fresh.get("maintained_indexes").isNull(), "a fresh spec sends null, not []");

    rte.enableRTE(RTEWriteSpec.unsharded().withMaintainedIndexes(Collections.<String>emptyList()));
    JsonNode none = MAPPER.readTree(requestBodies.get(1));
    assertTrue(none.get("maintained_indexes").isArray());
    assertEquals(0, none.get("maintained_indexes").size());
  }

  @Test
  public void testEnableRTEWriterConfigDefaults() throws Exception {
    enqueue("enable_rte", 200, "");

    Map<String, String> defaults = new HashMap<String, String>();
    defaults.put("max_memtable_rows", "50000");
    rte.enableRTE(RTEWriteSpec.unsharded().withWriterConfigDefaults(defaults));

    JsonNode body = MAPPER.readTree(requestBodies.get(0));
    assertEquals("50000", body.get("writer_config_defaults").get("max_memtable_rows").asText());
  }

  @Test
  public void testDisableRTE() {
    enqueue("disable_rte", 200, "");

    rte.disableRTE();

    assertEquals("/v1/table/my_table/disable_rte/", requestPaths.get(0));
    assertEquals("", requestBodies.get(0));
  }

  @Test
  public void testGetRTESpec() {
    enqueue(
        "get_rte_spec",
        200,
        "{\"rte_spec\":{\"sharding\":{\"mode\":\"bucket\",\"column\":\"id\","
            + "\"num_buckets\":16},\"maintained_indexes\":[\"id_idx\"],"
            + "\"writer_config_defaults\":{\"durable_write\":\"true\"}}}");

    Optional<RTEWriteSpec> spec = rte.getRTESpec();

    assertTrue(spec.isPresent());
    assertEquals(RTEWriteSpec.Sharding.BUCKET, spec.get().sharding());
    assertEquals("id", spec.get().column());
    assertEquals(Integer.valueOf(16), spec.get().numBuckets());
    assertEquals(Arrays.asList("id_idx"), spec.get().maintainedIndexes());
    assertEquals("true", spec.get().writerConfigDefaults().get("durable_write"));
  }

  /** A null selection, every index the table has, reads back as null rather than an empty list. */
  @Test
  public void testGetRTESpecRoundTripsEveryIndexSelection() {
    enqueue(
        "get_rte_spec",
        200,
        "{\"rte_spec\":{\"sharding\":{\"mode\":\"unsharded\"},"
            + "\"maintained_indexes\":null,\"writer_config_defaults\":{}}}");

    Optional<RTEWriteSpec> spec = rte.getRTESpec();

    assertTrue(spec.isPresent());
    assertNull(spec.get().maintainedIndexes());
  }

  @Test
  public void testGetRTESpecRoundTripsNoIndexSelection() {
    enqueue(
        "get_rte_spec",
        200,
        "{\"rte_spec\":{\"sharding\":{\"mode\":\"unsharded\"},"
            + "\"maintained_indexes\":[],\"writer_config_defaults\":{}}}");

    Optional<RTEWriteSpec> spec = rte.getRTESpec();

    assertTrue(spec.isPresent());
    assertEquals(Collections.emptyList(), spec.get().maintainedIndexes());
  }

  /**
   * Null is the automatic selection, so anything that is neither null nor an array has to be
   * rejected: reading a string or an object as null would turn a malformed response into a policy.
   */
  @Test
  public void testGetRTESpecRejectsANonArrayMaintainedIndexes() {
    enqueue(
        "get_rte_spec",
        200,
        "{\"rte_spec\":{\"sharding\":{\"mode\":\"unsharded\"},"
            + "\"maintained_indexes\":\"id_idx\",\"writer_config_defaults\":{}}}");

    assertThrows(IllegalStateException.class, () -> rte.getRTESpec());
  }

  @Test
  public void testGetRTESpecRejectsNonStringIndexNames() {
    enqueue(
        "get_rte_spec",
        200,
        "{\"rte_spec\":{\"sharding\":{\"mode\":\"unsharded\"},"
            + "\"maintained_indexes\":[7],\"writer_config_defaults\":{}}}");

    assertThrows(IllegalStateException.class, () -> rte.getRTESpec());
  }

  @Test
  public void testGetRTESpecAbsent() {
    enqueue("get_rte_spec", 200, "{\"rte_spec\":null}");

    assertFalse(rte.getRTESpec().isPresent());
  }

  // ===========================================================================
  // stats
  // ===========================================================================

  @Test
  public void testGetRTEStats() throws Exception {
    enqueue("get_rte_stats", 200, stats(tableShard("shard-0", false, 7L, 8L)));

    Optional<RTEStats> got = rte.getRTEStats(true);

    assertEquals("/v1/table/my_table/get_rte_stats/", requestPaths.get(0));
    assertTrue(MAPPER.readTree(requestBodies.get(0)).get("include_sstable_rows").asBoolean());
    assertTrue(got.isPresent());
    TableShardStats decoded = got.get().tableShards().get(0);
    assertEquals("shard-0", decoded.shardId());
    assertEquals("Active", decoded.status());
    assertEquals(1, decoded.writerEpoch());
    assertEquals(2, decoded.manifestVersion());
    assertEquals(9, decoded.currentGeneration());
    assertFalse(decoded.compacting());
    assertEquals(Arrays.asList(7L, 8L), generationNumbers(decoded));
    assertEquals(1024, decoded.sstables().get(0).bytes());
    assertFalse(decoded.sstables().get(0).rows().isPresent(), "rows absent unless requested");
    assertFalse(decoded.memtables().isPresent(), "absent memtables stay absent");
  }

  /** The optional fields decode when the server does send them. */
  @Test
  public void testGetRTEStatsDecodesOptionalFields() {
    enqueue(
        "get_rte_stats",
        200,
        "{\"rte_stats\":{\"table_shards\":[{\"shard_id\":\"shard-0\",\"status\":\"Active\","
            + "\"writer_epoch\":1,\"manifest_version\":2,\"current_generation\":9,"
            + "\"replay_after_wal_entry_position\":3,\"wal_entry_position_last_seen\":11,"
            + "\"sstables\":[{\"generation\":7,\"bytes\":1024,\"rows\":42}],"
            + "\"compacting\":true,\"memtables\":[{\"generation\":8,\"rows\":5,"
            + "\"bytes\":64,\"batches\":2,\"indexes\":[\"id_idx\"]}]}]}}");

    TableShardStats decoded = rte.getRTEStats(true).get().tableShards().get(0);

    assertEquals(3, decoded.replayAfterWalEntryPosition());
    assertEquals(11, decoded.walEntryPositionLastSeen());
    assertTrue(decoded.compacting());
    assertEquals(42, decoded.sstables().get(0).rows().getAsLong());
    assertTrue(decoded.memtables().isPresent());
    MemtableStats memtable = decoded.memtables().get().get(0);
    assertEquals(8, memtable.generation());
    assertEquals(5, memtable.rows());
    assertEquals(64, memtable.bytes());
    assertEquals(2, memtable.batches());
    assertEquals(Arrays.asList("id_idx"), memtable.indexes());
  }

  @Test
  public void testGetRTEStatsAbsentWhenRTEDisabled() {
    enqueue("get_rte_stats", 200, "{\"rte_stats\":null}");

    assertFalse(rte.getRTEStats().isPresent());
  }

  @Test
  public void testGetRTEStatsDefaultsToExcludingGenerationRows() throws Exception {
    enqueue("get_rte_stats", 200, stats());

    rte.getRTEStats();

    assertFalse(MAPPER.readTree(requestBodies.get(0)).get("include_sstable_rows").asBoolean());
  }

  // ===========================================================================
  // flush / compact
  // ===========================================================================

  @Test
  public void testFlushAndCompactRoutes() {
    enqueue("flush_rte", 200, "");
    enqueue("compact_rte", 200, "");

    rte.flushRTE();
    rte.compactRTE();

    assertEquals("/v1/table/my_table/flush_rte/", requestPaths.get(0));
    assertEquals("/v1/table/my_table/compact_rte/", requestPaths.get(1));
  }

  @Test
  public void testHttpErrorCarriesStatus() {
    enqueue("flush_rte", 404, "no such table");

    LanceDbRestClient.HttpException e =
        assertThrows(LanceDbRestClient.HttpException.class, () -> rte.flushRTE());
    assertEquals(404, e.statusCode());
  }

  // ===========================================================================
  // checkpoint
  // ===========================================================================

  @Test
  public void testCheckpointReturnsWhenRTEDisabled() {
    enqueue("flush_rte", 200, "");
    enqueue("get_rte_stats", 200, "{\"rte_stats\":null}");

    rte.checkpointRTE();

    assertEquals(0, countCalls("compact_rte"), "nothing to compact when the LSM path is off");
  }

  @Test
  public void testCheckpointReturnsWhenNoGenerationsOutstanding() {
    enqueue("flush_rte", 200, "");
    // A table shard with no SSTables yields no target, so the drain never starts.
    enqueue("get_rte_stats", 200, stats(tableShard("shard-0", false)));

    rte.checkpointRTE();

    assertEquals(0, countCalls("compact_rte"));
  }

  @Test
  public void testCheckpointConvergesOnceTargetGenerationsAreGone() {
    enqueue("flush_rte", 200, "");
    // Watermark read: shard-0 holds sstables 7 and 8, so target = 8.
    enqueue("get_rte_stats", 200, stats(tableShard("shard-0", false, 7L, 8L)));
    // First drain poll: both still outstanding, nothing compacting -> dispatch a pass.
    enqueue("get_rte_stats", 200, stats(tableShard("shard-0", false, 7L, 8L)));
    // Second drain poll: drained past the target -> done.
    enqueue("get_rte_stats", 200, stats(tableShard("shard-0", false, 9L)));
    enqueue("compact_rte", 200, "");

    rte.checkpointRTE();

    assertEquals(1, countCalls("compact_rte"), "one pass dispatched");
    assertEquals(3, countCalls("get_rte_stats"), "watermark read plus two drain polls");
  }

  @Test
  public void testCheckpointDoesNotPileOnWhileEveryTargetBucketIsCompacting() {
    enqueue("flush_rte", 200, "");
    enqueue("get_rte_stats", 200, stats(tableShard("shard-0", true, 4L)));
    // Still compacting on the first poll, so no pass is dispatched; then it drains.
    enqueue("get_rte_stats", 200, stats(tableShard("shard-0", true, 4L)));
    enqueue("get_rte_stats", 200, stats(tableShard("shard-0", false, 5L)));

    rte.checkpointRTE();

    assertEquals(0, countCalls("compact_rte"), "a latched table shard is left alone");
  }

  @Test
  public void testCheckpointRetriesFromFlushAfterLostClaim() {
    // 421 on the watermark read: the node lost its claim, so the whole thing restarts
    // from flush rather than retrying the read in place.
    enqueue("flush_rte", 200, "");
    enqueue("get_rte_stats", 421, "no claim");
    enqueue("get_rte_stats", 200, stats(tableShard("shard-0", false)));

    rte.checkpointRTE();

    assertEquals(2, countCalls("flush_rte"), "re-issued from flush");
  }

  @Test
  public void testCheckpointRetriesRetryableStatusInPlace() {
    enqueue("flush_rte", 429, "latch held");
    enqueue("flush_rte", 200, "");
    enqueue("get_rte_stats", 200, stats(tableShard("shard-0", false)));

    rte.checkpointRTE();

    assertEquals(2, countCalls("flush_rte"), "429 retried in place, not re-issued");
  }

  @Test
  public void testCheckpointPropagatesTerminalStatus() {
    enqueue("flush_rte", 400, "bad request");

    LanceDbRestClient.HttpException e =
        assertThrows(LanceDbRestClient.HttpException.class, () -> rte.checkpointRTE());
    assertEquals(400, e.statusCode());
    assertEquals(1, countCalls("flush_rte"), "a terminal status is not retried");
  }

  @Test
  public void testCheckpointGivesUpAfterRepeatedLostClaims() {
    enqueue("flush_rte", 421, "no claim");

    IllegalStateException e = assertThrows(IllegalStateException.class, () -> rte.checkpointRTE());
    assertTrue(e.getMessage().contains("kept losing its claim"), e.getMessage());
    assertEquals(4, countCalls("flush_rte"), "the initial attempt plus MAX_REISSUES");
  }

  // ===========================================================================
  // strict decoding
  // ===========================================================================

  /**
   * A stats payload that does not decode must fail closed. Every one of these bodies used to be
   * read as "no table shards", which is indistinguishable from a drained table, so {@code
   * checkpointRTE} reported convergence for a checkpoint that never ran.
   */
  @Test
  public void testCheckpointRejectsMalformedStats() {
    Map<String, String> malformed = new LinkedHashMap<String, String>();
    malformed.put("no response body at all", "");
    malformed.put("stats object with no table shards", "{\"rte_stats\":{}}");
    malformed.put(
        "table shard missing its required fields", "{\"rte_stats\":{\"table_shards\":[{}]}}");
    malformed.put(
        "table shard missing sstables",
        "{\"rte_stats\":{\"table_shards\":[{\"shard_id\":\"shard-0\",\"status\":\"Active\","
            + "\"writer_epoch\":1,\"manifest_version\":2,\"current_generation\":9,"
            + "\"replay_after_wal_entry_position\":0,\"wal_entry_position_last_seen\":0,"
            + "\"compacting\":false}]}}");
    malformed.put(
        "generation with a non-numeric generation number",
        "{\"rte_stats\":{\"table_shards\":[{\"shard_id\":\"shard-0\",\"status\":\"Active\","
            + "\"writer_epoch\":1,\"manifest_version\":2,\"current_generation\":9,"
            + "\"replay_after_wal_entry_position\":0,\"wal_entry_position_last_seen\":0,"
            + "\"sstables\":[{\"generation\":\"7\",\"bytes\":1024}],"
            + "\"compacting\":false}]}}");

    for (Map.Entry<String, String> each : malformed.entrySet()) {
      setUpFresh();
      enqueue("flush_rte", 200, "");
      enqueue("get_rte_stats", 200, each.getValue());

      assertThrows(
          IllegalStateException.class,
          () -> rte.checkpointRTE(),
          each.getKey() + " must not report convergence");
    }
  }

  /** The one shape that legitimately means "this table has no RTE write path". */
  @Test
  public void testCheckpointTreatsNullStatsAsNotWalBacked() {
    enqueue("flush_rte", 200, "");
    enqueue("get_rte_stats", 200, "{\"rte_stats\":null}");

    rte.checkpointRTE();

    assertEquals(1, countCalls("get_rte_stats"));
  }

  // ===========================================================================
  // retry budget
  // ===========================================================================

  /**
   * The transport must not retry on the checkpoint loop's behalf. Apache HttpClient's default
   * strategy retries exactly 429 and 503 — the two statuses {@code isRetryable} owns — which
   * doubled every budget here and also retried {@code compact_rte} in place, where the loop is
   * built to fall through to a fresh stats poll instead.
   */
  @Test
  public void testCheckpointRetryBudgetIsNotDoubledByTheTransport() {
    enqueue("flush_rte", 429, "latch held");

    LanceDbRestClient.HttpException e =
        assertThrows(LanceDbRestClient.HttpException.class, () -> rte.checkpointRTE());

    assertEquals(429, e.statusCode(), "the exhausted budget propagates the last error as itself");
    assertEquals(9, countCalls("flush_rte"), "the initial request plus MAX_RETRIES, and no more");
  }

  // ===========================================================================
  // harness
  // ===========================================================================

  private static List<Long> generationNumbers(TableShardStats tableShard) {
    List<Long> numbers = new ArrayList<Long>();
    for (SsTableStats generation : tableShard.sstables()) {
      numbers.add(generation.generation());
    }
    return numbers;
  }

  /** Build a {@code rte_stats} response body from table shard fragments. */
  private static String stats(String... tableShards) {
    return "{\"rte_stats\":{\"table_shards\":[" + String.join(",", tableShards) + "]}}";
  }

  private static String tableShard(String shardId, boolean compacting, Long... sstables) {
    StringBuilder gens = new StringBuilder();
    for (Long generation : sstables) {
      if (gens.length() > 0) {
        gens.append(",");
      }
      gens.append("{\"generation\":").append(generation).append(",\"bytes\":1024}");
    }
    return "{\"shard_id\":\""
        + shardId
        + "\",\"status\":\"Active\",\"writer_epoch\":1,\"manifest_version\":2,"
        + "\"current_generation\":9,\"replay_after_wal_entry_position\":0,"
        + "\"wal_entry_position_last_seen\":0,\"sstables\":["
        + gens
        + "],\"compacting\":"
        + compacting
        + "}";
  }

  /** Queue a reply for an operation. The last queued reply repeats once the queue drains. */
  private void enqueue(String operation, int status, String body) {
    replies.computeIfAbsent(operation, key -> new ArrayDeque<Reply>()).add(new Reply(status, body));
  }

  private Reply nextReply(String path) {
    String operation = operationOf(path);
    Deque<Reply> queued = replies.get(operation);
    if (queued == null || queued.isEmpty()) {
      return new Reply(200, "");
    }
    return queued.size() > 1 ? queued.poll() : queued.peek();
  }

  private long countCalls(String operation) {
    return requestPaths.stream().filter(path -> operationOf(path).equals(operation)).count();
  }

  /** {@code /v1/table/my_table/flush_rte/} -> {@code flush_rte}. */
  private static String operationOf(String path) {
    String[] segments = path.split("/");
    return segments.length == 0 ? "" : segments[segments.length - 1];
  }

  private static String readAll(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    int read;
    while ((read = in.read(buffer)) != -1) {
      out.write(buffer, 0, read);
    }
    return new String(out.toByteArray(), StandardCharsets.UTF_8);
  }

  private static final class Reply {
    private final int status;
    private final String body;

    private Reply(int status, String body) {
      this.status = status;
      this.body = body;
    }
  }
}
