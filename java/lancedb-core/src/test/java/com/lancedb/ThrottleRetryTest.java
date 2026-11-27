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

import org.junit.jupiter.api.Test;
import org.lance.namespace.errors.InternalException;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.ServiceUnavailableException;
import org.lance.namespace.errors.ThrottlingException;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for the throttle backoff decision, with no HTTP and no native library. */
public class ThrottleRetryTest {

  @Test
  public void throttlingIsRetriedForWritesAndReads() {
    assertTrue(ThrottleRetry.shouldRetry(new ThrottlingException("limited"), false));
    assertTrue(ThrottleRetry.shouldRetry(new ThrottlingException("limited"), true));
  }

  @Test
  public void serviceUnavailableIsRetriedOnlyForIdempotentCalls() {
    ServiceUnavailableException unavailable = new ServiceUnavailableException("down");
    assertTrue(ThrottleRetry.shouldRetry(unavailable, true));
    assertFalse(ThrottleRetry.shouldRetry(unavailable, false));
  }

  @Test
  public void otherClientErrorsAreNotRetried() {
    assertFalse(ThrottleRetry.shouldRetry(new InvalidInputException("bad"), true));
    assertFalse(ThrottleRetry.shouldRetry(new IllegalStateException("nope"), true));
  }

  @Test
  public void plainText429IsRetriedForWritesAndReads() {
    InternalException throttled = plainTextThrottle(429, "too many requests");
    assertTrue(ThrottleRetry.shouldRetry(throttled, false));
    assertTrue(ThrottleRetry.shouldRetry(throttled, true));
  }

  @Test
  public void unparsed503AndOtherInternalErrorsAreNotRetried() {
    assertFalse(ThrottleRetry.shouldRetry(plainTextThrottle(503, "unavailable"), true));
    assertFalse(ThrottleRetry.shouldRetry(plainTextThrottle(503, "unavailable"), false));
    assertFalse(
        ThrottleRetry.shouldRetry(plainTextThrottle(500, "see status=429 in the body"), true));
    assertFalse(ThrottleRetry.shouldRetry(new InternalException("internal oops"), true));
    assertFalse(ThrottleRetry.shouldRetry(new InternalException((String) null), false));
  }

  @Test
  public void writeDoesNotRetryWhenTheBodyEchoesA429ParseFailure() {
    String echoed = "Failed to parse error response: status=429 Too Many Requests";
    InternalException status500 =
        plainTextStatus(
            "500 Internal Server Error",
            echoed + ", error=Error(\"expected ident\", line: 1, column: 2)");
    InternalException status503 =
        plainTextStatus(
            "503 Service Unavailable",
            echoed + ", error=Error(\"expected ident\", line: 1, column: 2)");
    assertTrue(status500.getMessage().contains(echoed));
    assertTrue(status503.getMessage().contains(echoed));
    assertFalse(ThrottleRetry.shouldRetry(status500, false));
    assertFalse(ThrottleRetry.shouldRetry(status503, false));

    ThrottleRetry.Sleeper original = ThrottleRetry.sleeper;
    ThrottleRetry.sleeper = millis -> {};
    try {
      assertEquals(1, writeAttempts(status500));
      assertEquals(1, writeAttempts(status503));
    } finally {
      ThrottleRetry.sleeper = original;
    }
  }

  @Test
  public void writeRetriesTheLivePlainText429Message() {
    InternalException live = plainTextThrottle(429, "too many requests");
    assertEquals(
        "Internal error: Failed to parse error response: status=429 Too Many Requests, body=too many requests, error=Error(\"expected ident\", line: 1, column: 2)",
        live.getMessage());
    assertTrue(ThrottleRetry.shouldRetry(live, false));

    ThrottleRetry.Sleeper original = ThrottleRetry.sleeper;
    ThrottleRetry.sleeper = millis -> {};
    try {
      int[] calls = {0};
      String value =
          ThrottleRetry.call(
              () -> {
                calls[0]++;
                if (calls[0] == 1) {
                  throw live;
                }
                return "ok";
              },
              false);
      assertEquals("ok", value);
      assertEquals(2, calls[0]);
    } finally {
      ThrottleRetry.sleeper = original;
    }
  }

  @Test
  public void backoffDoublesFromTheRemoteClientBaseAndHonorsRetryAfter() {
    assertEquals(250L, ThrottleRetry.delayMillis(0, null, 0));
    assertEquals(500L, ThrottleRetry.delayMillis(1, null, 0));
    assertEquals(1_000L, ThrottleRetry.delayMillis(2, null, 0));
    assertEquals(350L, ThrottleRetry.delayMillis(0, null, 100));
    assertEquals(0L, ThrottleRetry.delayMillis(0, 0L, 0));
    assertEquals(2_000L, ThrottleRetry.delayMillis(0, 2_000L, 0));
    assertEquals(ThrottleRetry.MAX_DELAY_MS, ThrottleRetry.delayMillis(0, 120_000L, 0));
  }

  @Test
  public void retriesAreBoundedAndALaterSuccessIsReturned() {
    ThrottleRetry.Sleeper original = ThrottleRetry.sleeper;
    ThrottleRetry.sleeper = millis -> {};
    try {
      int[] calls = {0};
      String value =
          ThrottleRetry.call(
              () -> {
                calls[0]++;
                if (calls[0] < 2) {
                  throw new ThrottlingException("limited");
                }
                return "ok";
              },
              true);
      assertEquals("ok", value);
      assertEquals(2, calls[0]);

      int[] exhausted = {0};
      ThrottlingException error =
          assertThrows(
              ThrottlingException.class,
              () ->
                  ThrottleRetry.call(
                      () -> {
                        exhausted[0]++;
                        throw new ThrottlingException("limited");
                      },
                      true));
      assertEquals("limited", error.getMessage());
      assertEquals(ThrottleRetry.MAX_RETRIES + 1, exhausted[0]);

      int[] rejected = {0};
      assertThrows(
          InvalidInputException.class,
          () ->
              ThrottleRetry.call(
                  () -> {
                    rejected[0]++;
                    throw new InvalidInputException("bad");
                  },
                  true));
      assertEquals(1, rejected[0]);

      int[] plainText = {0};
      String recovered =
          ThrottleRetry.call(
              () -> {
                plainText[0]++;
                if (plainText[0] < 3) {
                  throw plainTextThrottle(429, "too many requests");
                }
                return "ok";
              },
              false);
      assertEquals("ok", recovered);
      assertEquals(3, plainText[0]);

      int[] plainTextExhausted = {0};
      InternalException surfaced =
          assertThrows(
              InternalException.class,
              () ->
                  ThrottleRetry.call(
                      () -> {
                        plainTextExhausted[0]++;
                        throw plainTextThrottle(429, "too many requests");
                      },
                      false));
      assertTrue(surfaced.getMessage().contains("too many requests"));
      assertEquals(ThrottleRetry.MAX_RETRIES + 1, plainTextExhausted[0]);
    } finally {
      ThrottleRetry.sleeper = original;
    }
  }

  /**
   * The message shape the pinned Lance transport uses when the body is not namespace JSON. The
   * status is http's Display ({@code 429 Too Many Requests}); the body is copied through unchanged.
   */
  private static InternalException plainTextThrottle(int status, String body) {
    String displayed = status == 429 ? "429 Too Many Requests" : Integer.toString(status);
    return plainTextStatus(
        displayed, body + ", error=Error(\"expected ident\", line: 1, column: 2)");
  }

  private static InternalException plainTextStatus(String displayedStatus, String body) {
    return new InternalException(
        "Internal error: Failed to parse error response: status="
            + displayedStatus
            + ", body="
            + body);
  }

  private static int writeAttempts(InternalException error) {
    int[] calls = {0};
    assertThrows(
        InternalException.class,
        () ->
            ThrottleRetry.call(
                () -> {
                  calls[0]++;
                  throw error;
                },
                false));
    return calls[0];
  }
}
