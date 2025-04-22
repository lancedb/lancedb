// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/**
 * Retry a function with exponential backoff.
 * 
 * @param func The function to be retried
 * @param initialDelay Initial delay in seconds (default is 1)
 * @param exponentialBase The base for exponential backoff (default is 2)
 * @param jitter Whether to add jitter to the delay (default is true)
 * @param maxRetries Maximum number of retries (default is 7)
 * @returns The result of the function call
 */
export async function retryWithExponentialBackoff<T>(
  func: () => Promise<T>,
  initialDelay: number = 1,
  exponentialBase: number = 2,
  jitter: boolean = true,
  maxRetries: number = 7
): Promise<T> {
  let numRetries = 0;
  let delay = initialDelay;

  // Loop until a successful response or max_retries is hit or an exception is raised
  while (true) {
    try {
      return await func();
    } catch (error) {
      numRetries += 1;

      if (numRetries > maxRetries) {
        throw new Error(`Maximum number of retries (${maxRetries}) exceeded.`, { cause: error });
      }

      delay *= exponentialBase * (1 + (jitter ? Math.random() : 0));
      console.warn(
        `Error occurred: ${error.message}\nRetrying in ${delay.toFixed(2)} seconds (retry ${numRetries} of ${maxRetries})`
      );

      // Wait for the calculated delay
      await new Promise((resolve) => setTimeout(resolve, delay * 1000));
    }
  }
}

/**
 * Rate limiter class to control the frequency of API calls
 */
export class RateLimiter {
  private period: number;
  private maxCalls: number;
  private lastReset: number;
  private numCalls: number;

  /**
   * Create a rate limiter
   * 
   * @param maxCalls Maximum number of calls allowed in the period
   * @param period Period in seconds
   */
  constructor(maxCalls: number = 1, period: number = 1.0) {
    this.period = period;
    this.maxCalls = Math.max(1, Math.min(Number.MAX_SAFE_INTEGER, Math.floor(maxCalls)));
    this.lastReset = Date.now();
    this.numCalls = 0;
  }

  /**
   * Check if we need to sleep and if so, how long
   * @returns Time to sleep in milliseconds
   */
  private checkSleep(): number {
    const currentTime = Date.now();
    const elapsed = (currentTime - this.lastReset) / 1000;
    const periodRemaining = this.period - elapsed;

    // If the time window has elapsed then reset
    if (periodRemaining <= 0) {
      this.numCalls = 0;
      this.lastReset = currentTime;
    }

    this.numCalls += 1;

    if (this.numCalls > this.maxCalls) {
      return periodRemaining * 1000;
    }

    return 0;
  }

  /**
   * Apply rate limiting to a function
   * 
   * @param func Function to rate limit
   * @returns Rate limited function
   */
  wrap<T, Args extends any[]>(func: (...args: Args) => Promise<T>): (...args: Args) => Promise<T> {
    return async (...args: Args): Promise<T> => {
      const sleepTime = this.checkSleep();
      if (sleepTime > 0) {
        await new Promise((resolve) => setTimeout(resolve, sleepTime));
      }
      return await func(...args);
    };
  }
} 