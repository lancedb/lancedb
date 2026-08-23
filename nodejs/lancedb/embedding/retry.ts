// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/**
 * Returns true for errors that are known to be permanent (retrying them
 * cannot succeed), such as an invalid API key. Matches the check the Python
 * SDK added in #2995: an error named `AuthenticationError` (e.g. OpenAI's
 * SDK), or one carrying a 401/403 HTTP status.
 */
export function isPermanentError(error: unknown): boolean {
  if (error instanceof Error && error.name === "AuthenticationError") {
    return true;
  }
  const status =
    (error as {
      status?: unknown;
      // biome-ignore lint/style/useNamingConvention: mirrors the snake_case some SDKs use
      status_code?: unknown;
      statusCode?: unknown;
    }) ?? {};
  const code = status.status ?? status.status_code ?? status.statusCode;
  return code === 401 || code === 403;
}

/**
 * Wrap an async function so it retries with exponential backoff on failure.
 *
 * There is no reliable, cross-provider way to distinguish a rate limit error
 * from any other failure, so (matching the Python SDK) this retries on any
 * thrown error by default, except for errors `isRetryable` identifies as
 * permanent (see {@link isPermanentError}) — those are thrown immediately.
 *
 * The original error is always what's thrown, whether retries are disabled,
 * the error is non-retryable, or retries are exhausted — it is never wrapped.
 */
export function retryWithExponentialBackoff<
  // biome-ignore lint/suspicious/noExplicitAny: wraps arbitrary embedding function methods
  F extends (...args: any[]) => Promise<any>,
>(
  func: F,
  options: {
    maxRetries?: number;
    initialDelayMs?: number;
    exponentialBase?: number;
    jitter?: boolean;
    isRetryable?: (error: unknown) => boolean;
  } = {},
): F {
  const {
    maxRetries = 7,
    initialDelayMs = 1000,
    exponentialBase = 2,
    jitter = true,
    isRetryable = (error: unknown) => !isPermanentError(error),
  } = options;

  return (async (...args: Parameters<F>) => {
    let delay = initialDelayMs;
    let numRetries = 0;

    while (true) {
      try {
        return await func(...args);
      } catch (e) {
        if (numRetries >= maxRetries || !isRetryable(e)) {
          throw e;
        }
        numRetries++;
        delay *= exponentialBase * (1 + (jitter ? Math.random() : 0));
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }) as F;
}
