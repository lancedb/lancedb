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

import org.lance.namespace.errors.InternalException;
import org.lance.namespace.errors.ServiceUnavailableException;
import org.lance.namespace.errors.ThrottlingException;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Bounded retry for namespace throttling.
 *
 * <p>The budget is four attempts in total: the first try plus three retries. The base waits are 250
 * ms, 500 ms, and 1000 ms, plus up to 250 ms of jitter on each wait. {@link LanceDbTableRTE} keeps
 * a separate budget. This helper is not used on that transport, so the two loops cannot stack.
 *
 * <p>A {@link ThrottlingException} (namespace error 21, what Lance maps from HTTP 429 when the body
 * is a namespace error) is retried for every call this helper runs. The server refused the call. A
 * 429 whose body is not a namespace error, including the pre-execution plain-text response, is
 * reported as {@link InternalException} with {@code status=429} in the message. That refusal also
 * happens before the call runs, so it uses this same budget on reads and writes. {@link
 * ServiceUnavailableException} (error 17: HTTP 503, connect failures, and timeouts) is retried only
 * when the caller marks the operation as a read, because a 503 can arrive after the server applied
 * a write. A 503 whose body is not a namespace error, and any other internal error, stays {@link
 * InternalException} and is not retried. Every other failure, including other 4xx errors, is
 * returned immediately.
 *
 * <p>{@code Retry-After} is not available through JNI: the bridge throws {@link
 * ThrottlingException} with the message only, so namespace calls pass no delay and use the
 * exponential backoff above. {@link #delayMillis} still honors a delay-seconds value when a caller
 * can supply one, and caps it so one header cannot pin the thread.
 */
final class ThrottleRetry {
  static final int MAX_RETRIES = 3;

  /** Base wait before the first retry. The later base waits are 500 ms and 1000 ms. */
  static final long BACKOFF_BASE_MS = 250L;

  /** Maximum random jitter added to each base wait. */
  static final long JITTER_MS = 250L;

  /** Upper bound for a single sleep, including a {@code Retry-After} value. */
  static final long MAX_DELAY_MS = 60_000L;

  /**
   * Prefix of the message the pinned Lance transport uses when an HTTP error body is not a
   * namespace error. The status that follows is the only throttling signal JNI keeps. It is parsed
   * from the first occurrence, so a later copy of this phrase inside the body is not a status.
   */
  private static final String UNPARSED_STATUS_PREFIX = "Failed to parse error response: status=";

  interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  /** Replaced by tests so a bounded retry does not wait on the clock. */
  static volatile Sleeper sleeper =
      millis -> {
        if (millis > 0) {
          Thread.sleep(millis);
        }
      };

  private ThrottleRetry() {}

  static <T> T call(Callable<T> action, boolean idempotent) {
    int attempt = 0;
    while (true) {
      try {
        return action.call();
      } catch (Exception error) {
        if (!shouldRetry(error, idempotent) || attempt >= MAX_RETRIES) {
          throw propagate(error);
        }
        sleep(delayMillis(attempt, null, randomJitter()));
        attempt++;
      }
    }
  }

  static boolean shouldRetry(Throwable error, boolean idempotent) {
    if (error instanceof ThrottlingException || isPlainTextThrottle(error)) {
      return true;
    }
    return idempotent && error instanceof ServiceUnavailableException;
  }

  /**
   * A 429 that arrives before the request executes, with a body that is not namespace JSON. The
   * transport could not classify it, so the HTTP status in the {@link InternalException} message is
   * the signal. Only the digits immediately after the first {@code status=} count. The live display
   * is {@code 429 Too Many Requests}, and a 500 or 503 whose body quotes that phrase is not a 429.
   */
  private static boolean isPlainTextThrottle(Throwable error) {
    if (!(error instanceof InternalException)) {
      return false;
    }
    String message = error.getMessage();
    if (message == null) {
      return false;
    }
    int index = message.indexOf(UNPARSED_STATUS_PREFIX);
    if (index < 0) {
      return false;
    }
    int start = index + UNPARSED_STATUS_PREFIX.length();
    int end = start;
    while (end < message.length() && isAsciiDigit(message.charAt(end))) {
      end++;
    }
    if (end == start || end - start > 9) {
      return false;
    }
    return Integer.parseInt(message.substring(start, end)) == 429;
  }

  private static boolean isAsciiDigit(char c) {
    return c >= '0' && c <= '9';
  }

  /**
   * Sleep before retry number {@code previousRetries} (zero for the first retry).
   *
   * <p>When {@code retryAfterMillis} is non-null it replaces the computed delay. Zero means retry
   * immediately. Values are clamped into {@code [0, MAX_DELAY_MS]}.
   */
  static long delayMillis(int previousRetries, Long retryAfterMillis, long jitterMillis) {
    if (retryAfterMillis != null) {
      return clamp(retryAfterMillis);
    }
    long exponential = BACKOFF_BASE_MS << previousRetries;
    return clamp(exponential + Math.max(0L, jitterMillis));
  }

  private static long clamp(long millis) {
    if (millis < 0) {
      return 0;
    }
    return Math.min(millis, MAX_DELAY_MS);
  }

  private static long randomJitter() {
    return ThreadLocalRandom.current().nextLong(JITTER_MS + 1);
  }

  private static void sleep(long millis) {
    try {
      sleeper.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while backing off a throttled request", e);
    }
  }

  private static RuntimeException propagate(Throwable error) {
    if (error instanceof RuntimeException) {
      return (RuntimeException) error;
    }
    if (error instanceof Error) {
      throw (Error) error;
    }
    return new RuntimeException(error);
  }
}
