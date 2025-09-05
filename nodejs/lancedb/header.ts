// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/**
 * Header providers for LanceDB remote connections.
 *
 * This module provides a flexible header management framework for LanceDB remote
 * connections, allowing users to implement custom header strategies for
 * authentication, request tracking, custom metadata, or any other header-based
 * requirements.
 *
 * @module header
 */

/**
 * Abstract base class for providing custom headers for each request.
 *
 * Users can implement this interface to provide dynamic headers for various purposes
 * such as authentication (OAuth tokens, API keys), request tracking (correlation IDs),
 * custom metadata, or any other header-based requirements. The provider is called
 * before each request to ensure fresh header values are always used.
 *
 * @example
 * Simple JWT token provider:
 * ```typescript
 * class JWTProvider extends HeaderProvider {
 *   constructor(private token: string) {
 *     super();
 *   }
 *
 *   getHeaders(): Record<string, string> {
 *     return { authorization: `Bearer ${this.token}` };
 *   }
 * }
 * ```
 *
 * @example
 * Provider with request tracking:
 * ```typescript
 * class RequestTrackingProvider extends HeaderProvider {
 *   constructor(private sessionId: string) {
 *     super();
 *   }
 *
 *   getHeaders(): Record<string, string> {
 *     return {
 *       "X-Session-Id": this.sessionId,
 *       "X-Request-Id": `req-${Date.now()}`
 *     };
 *   }
 * }
 * ```
 */
export abstract class HeaderProvider {
  /**
   * Get the latest headers to be added to requests.
   *
   * This method is called before each request to the remote LanceDB server.
   * Implementations should return headers that will be merged with existing headers.
   *
   * @returns Dictionary of header names to values to add to the request.
   * @throws If unable to fetch headers, the exception will be propagated and the request will fail.
   */
  abstract getHeaders(): Record<string, string>;
}

/**
 * Example implementation: A simple header provider that returns static headers.
 *
 * This is an example implementation showing how to create a HeaderProvider
 * for cases where headers don't change during the session.
 *
 * @example
 * ```typescript
 * const provider = new StaticHeaderProvider({
 *   authorization: "Bearer my-token",
 *   "X-Custom-Header": "custom-value"
 * });
 * const headers = provider.getHeaders();
 * // Returns: {authorization: 'Bearer my-token', 'X-Custom-Header': 'custom-value'}
 * ```
 */
export class StaticHeaderProvider extends HeaderProvider {
  private _headers: Record<string, string>;

  /**
   * Initialize with static headers.
   * @param headers - Headers to return for every request.
   */
  constructor(headers: Record<string, string>) {
    super();
    this._headers = { ...headers };
  }

  /**
   * Return the static headers.
   * @returns Copy of the static headers.
   */
  getHeaders(): Record<string, string> {
    return { ...this._headers };
  }
}

/**
 * Token response from OAuth provider.
 * @public
 */
export interface TokenResponse {
  accessToken: string;
  expiresIn?: number;
}

/**
 * Example implementation: OAuth token provider with automatic refresh.
 *
 * This is an example implementation showing how to manage OAuth tokens
 * with automatic refresh when they expire.
 *
 * @example
 * ```typescript
 * async function fetchToken(): Promise<TokenResponse> {
 *   const response = await fetch("https://oauth.example.com/token", {
 *     method: "POST",
 *     body: JSON.stringify({
 *       grant_type: "client_credentials",
 *       client_id: "your-client-id",
 *       client_secret: "your-client-secret"
 *     }),
 *     headers: { "Content-Type": "application/json" }
 *   });
 *   const data = await response.json();
 *   return {
 *     accessToken: data.access_token,
 *     expiresIn: data.expires_in
 *   };
 * }
 *
 * const provider = new OAuthHeaderProvider(fetchToken);
 * const headers = provider.getHeaders();
 * // Returns: {"authorization": "Bearer <your-token>"}
 * ```
 */
export class OAuthHeaderProvider extends HeaderProvider {
  private _tokenFetcher: () => Promise<TokenResponse> | TokenResponse;
  private _refreshBufferSeconds: number;
  private _currentToken: string | null = null;
  private _tokenExpiresAt: number | null = null;
  private _refreshPromise: Promise<void> | null = null;

  /**
   * Initialize the OAuth provider.
   * @param tokenFetcher - Function to fetch new tokens. Should return object with 'accessToken' and optionally 'expiresIn'.
   * @param refreshBufferSeconds - Seconds before expiry to refresh token. Default 300 (5 minutes).
   */
  constructor(
    tokenFetcher: () => Promise<TokenResponse> | TokenResponse,
    refreshBufferSeconds: number = 300,
  ) {
    super();
    this._tokenFetcher = tokenFetcher;
    this._refreshBufferSeconds = refreshBufferSeconds;
  }

  /**
   * Check if token needs refresh.
   */
  private _needsRefresh(): boolean {
    if (this._currentToken === null) {
      return true;
    }

    if (this._tokenExpiresAt === null) {
      // No expiration info, assume token is valid
      return false;
    }

    // Refresh if we're within the buffer time of expiration
    const now = Date.now() / 1000;
    return now >= this._tokenExpiresAt - this._refreshBufferSeconds;
  }

  /**
   * Refresh the token if it's expired or close to expiring.
   */
  private async _refreshTokenIfNeeded(): Promise<void> {
    if (!this._needsRefresh()) {
      return;
    }

    // If refresh is already in progress, wait for it
    if (this._refreshPromise) {
      await this._refreshPromise;
      return;
    }

    // Start refresh
    this._refreshPromise = (async () => {
      try {
        const tokenData = await this._tokenFetcher();

        this._currentToken = tokenData.accessToken;
        if (!this._currentToken) {
          throw new Error("Token fetcher did not return 'accessToken'");
        }

        // Set expiration if provided
        if (tokenData.expiresIn) {
          this._tokenExpiresAt = Date.now() / 1000 + tokenData.expiresIn;
        } else {
          // Token doesn't expire or expiration unknown
          this._tokenExpiresAt = null;
        }
      } finally {
        this._refreshPromise = null;
      }
    })();

    await this._refreshPromise;
  }

  /**
   * Get OAuth headers, refreshing token if needed.
   * Note: This is synchronous for now as the Rust implementation expects sync.
   * In a real implementation, this would need to handle async properly.
   * @returns Headers with Bearer token authorization.
   * @throws If unable to fetch or refresh token.
   */
  getHeaders(): Record<string, string> {
    // For simplicity in this example, we assume the token is already fetched
    // In a real implementation, this would need to handle the async nature properly
    if (!this._currentToken && !this._refreshPromise) {
      // Synchronously trigger refresh - this is a limitation of the current implementation
      throw new Error(
        "Token not initialized. Call refreshToken() first or use async initialization.",
      );
    }

    if (!this._currentToken) {
      throw new Error("Failed to obtain OAuth token");
    }

    return { authorization: `Bearer ${this._currentToken}` };
  }

  /**
   * Manually refresh the token.
   * Call this before using getHeaders() to ensure token is available.
   */
  async refreshToken(): Promise<void> {
    this._currentToken = null; // Force refresh
    await this._refreshTokenIfNeeded();
  }
}
