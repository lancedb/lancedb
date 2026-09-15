// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  OAuthConfig as NativeOAuthConfig,
  OAuthSession as NativeOAuthSession,
} from "./native";

/**
 * OAuth authentication flow types.
 */
export enum OAuthFlowType {
  /** Client Credentials grant (service-to-service / M2M). */
  ClientCredentials = "client_credentials",
  /** Interactive Authorization Code grant, using PKCE by default. */
  AuthorizationCode = "authorization_code",
  /** Device Authorization grant for CLI and headless environments. */
  DeviceCode = "device_code",
  /** Azure Managed Identity via IMDS. */
  AzureManagedIdentity = "azure_managed_identity",
}

/**
 * Options for the persistent OAuth token cache.
 *
 * The cache is opt-in: it is only used when set as `tokenCache` on
 * {@link OAuthConfig}. Only refresh tokens are persisted, in a private
 * directory with owner-only permissions, so short-lived processes can reuse
 * an authenticated session instead of re-prompting on every start.
 *
 * Multiple identities (issuer, client, scopes, flow, client authentication)
 * get separate cache entries. Within one identity the most recent login wins.
 */
export interface TokenCacheOptions {
  /**
   * Directory that holds cached credentials. Defaults to
   * `$XDG_CACHE_HOME/lancedb/oauth`, `$HOME/.cache/lancedb/oauth` on Unix,
   * or `%LOCALAPPDATA%\\lancedb\\oauth` on Windows. The directory is created
   * with owner-only permissions (`0700`) when missing.
   */
  cacheDir?: string;

  /**
   * How long to wait for the cross-process refresh lock before failing, in
   * seconds (default: 30).
   */
  lockTimeoutSecs?: number;
}

/**
 * OAuth configuration for LanceDB authentication.
 *
 * This is the public TypeScript OAuth configuration type. The generated
 * `NativeOAuthConfig` type has the same runtime shape but is an implementation
 * detail of the napi-rs binding.
 *
 * All token acquisition and refresh is handled in the Rust layer.
 * This config is passed through to Rust via napi-rs.
 *
 * @example Client Credentials (service-to-service):
 * ```typescript
 * const config: OAuthConfig = {
 *   issuerUrl: "https://login.microsoftonline.com/{tenant}/v2.0",
 *   clientId: "app-id",
 *   clientSecret: "secret",
 *   scopes: ["api://lancedb-api/.default"],
 * };
 * ```
 *
 * @example Azure Managed Identity:
 * ```typescript
 * const config: OAuthConfig = {
 *   issuerUrl: "https://login.microsoftonline.com/{tenant}/v2.0",
 *   clientId: "app-id",
 *   scopes: ["api://lancedb-api/.default"],
 *   flow: OAuthFlowType.AzureManagedIdentity,
 * };
 * ```
 *
 * @example Authorization Code with PKCE:
 * The authorization URL is written to stderr before LanceDB tries to open a
 * browser, so it can be copied in headless environments.
 * ```typescript
 * const config: OAuthConfig = {
 *   issuerUrl: "https://login.microsoftonline.com/{tenant}/v2.0",
 *   clientId: "app-id",
 *   scopes: ["openid", "api://lancedb-api/access"],
 *   flow: OAuthFlowType.AuthorizationCode,
 * };
 * ```
 *
 * Device Authorization writes the verification URL and user code to stderr
 * before polling begins.
 */
export interface OAuthConfig {
  /**
   * OIDC issuer URL or OAuth authority URL.
   * For Azure: `https://login.microsoftonline.com/{tenant_id}/v2.0`
   */
  issuerUrl: string;

  /** Application / Client ID. */
  clientId: string;

  /**
   * OAuth scopes to request.
   * For Azure managed identity, exactly one scope or resource is required.
   * For example: `["api://{app_id}/.default"]`
   */
  scopes: string[];

  /** Authentication flow (default: ClientCredentials). */
  flow?: OAuthFlowType;

  /** Client secret (required for ClientCredentials). */
  clientSecret?: string;

  /** Loopback redirect URI for AuthorizationCode. */
  redirectUri?: string;

  /** Port for the AuthorizationCode loopback callback server (default: 8400). */
  callbackPort?: number;

  /** Protect AuthorizationCode with S256 PKCE (default: true). */
  usePkce?: boolean;

  /** Client ID for user-assigned managed identity (AzureManagedIdentity). */
  managedIdentityClientId?: string;

  /**
   * Seconds before expiry to trigger proactive refresh (default: 300).
   * Keep this well below the token TTL; if it is greater than or equal to
   * the TTL, each request refreshes the token.
   */
  refreshBufferSecs?: number;

  /**
   * Opt in to the persistent token cache so short-lived processes reuse one
   * session. Only refresh tokens are persisted. Only supported by
   * AuthorizationCode and DeviceCode; Azure managed identity is rejected.
   * Default: unset (memory only).
   */
  tokenCache?: TokenCacheOptions;
}

/**
 * Safe, non-secret view of a cached OAuth session, returned by
 * {@link OAuthSession.status} and {@link OAuthSession.login}.
 */
export interface SessionStatus {
  /**
   * Whether a cached session exists that can obtain tokens without
   * interactive authentication. Because access tokens are not persisted,
   * this is `true` exactly when a refresh token is cached; the next
   * connection refreshes with it rather than opening a browser or device
   * prompt.
   */
  refreshable: boolean;

  /** Canonical issuer URL of the cached session. */
  issuerUrl: string;

  /** Client ID of the cached session. */
  clientId: string;

  /** Canonical (sorted, de-duplicated) scope set of the cached session. */
  scopes: string[];

  /** Flow that produced the cached session. */
  flow: string;

  /** When the cached session was obtained, as Unix seconds. */
  obtainedAt?: number;
}

/** Result of {@link OAuthSession.logout}. */
export interface SessionLogout {
  /**
   * Whether a cached credential was removed. `false` means no matching
   * session was cached; logout is idempotent.
   */
  removed: boolean;
}

/**
 * Explicit OAuth session lifecycle for the persistent token cache: eager
 * `login`, non-secret `status`, and local `logout`.
 *
 * A session is built from the same {@link OAuthConfig} used to connect
 * (including its `tokenCache` options). A connection created with the same
 * configuration shares the cache, so logging in here prepares tokens for
 * later processes without any database request.
 *
 * `login` always runs the configured interactive flow and replaces the cached
 * session (the most recent login wins). `logout` removes only the local
 * credential; it does not revoke anything with the provider and does not sign
 * out of a browser SSO session.
 *
 * @example
 * ```typescript
 * const config: OAuthConfig = {
 *   issuerUrl: "https://issuer.example.com",
 *   clientId: "my-app",
 *   scopes: ["openid", "offline_access"],
 *   flow: OAuthFlowType.DeviceCode,
 *   tokenCache: { cacheDir: "/tmp/my-app/oauth-cache" },
 * };
 * const session = new OAuthSession(config);
 * const status = await session.login();
 * ```
 */
export class OAuthSession {
  private readonly inner: NativeOAuthSession;

  /** Create a session manager for the given OAuth configuration. */
  constructor(config: OAuthConfig) {
    this.inner = new NativeOAuthSession(config as unknown as NativeOAuthConfig);
  }

  /**
   * Eagerly run the configured authentication flow and store the session.
   *
   * A successful login always replaces any prior cached session for this
   * identity; if the provider does not issue a refresh token (for example
   * without `offline_access`), the previous record is removed and the status
   * reports `refreshable == false`.
   */
  async login(): Promise<SessionStatus> {
    return this.inner.login();
  }

  /**
   * Report whether a matching cached session exists, with safe metadata.
   *
   * This never contacts the identity provider and never exposes token values.
   */
  async status(): Promise<SessionStatus> {
    return this.inner.status();
  }

  /**
   * Remove the matching local cached credential.
   *
   * This only deletes the local cache entry. It does not revoke the refresh
   * token with the provider and does not sign out of a browser SSO session.
   * Repeated calls succeed; `removed` reports whether a credential existed.
   */
  async logout(): Promise<SessionLogout> {
    return this.inner.logout();
  }
}
