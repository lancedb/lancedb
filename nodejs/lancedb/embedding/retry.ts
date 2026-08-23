// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/**
 * Wrap an async function so it retries with exponential backoff on failure.
 *
 * There is no reliable, cross-provider way to distinguish a rate limit error
 * from any other failure, so (matching the Python SDK) this retries on any
 * thrown error up to `maxRetries` times.
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
  } = {},
): F {
  const {
    maxRetries = 7,
    initialDelayMs = 1000,
    exponentialBase = 2,
    jitter = true,
  } = options;

  return (async (...args: Parameters<F>) => {
    let delay = initialDelayMs;
    let numRetries = 0;

    while (true) {
      try {
        return await func(...args);
      } catch (e) {
        numRetries++;
        if (numRetries > maxRetries) {
          throw new Error(
            `Maximum number of retries (${maxRetries}) exceeded.`,
            { cause: e },
          );
        }
        delay *= exponentialBase * (1 + (jitter ? Math.random() : 0));
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }) as F;
}
