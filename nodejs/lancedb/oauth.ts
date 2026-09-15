// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

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
}
