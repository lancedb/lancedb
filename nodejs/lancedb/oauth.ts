// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/**
 * OAuth authentication flow types.
 */
export enum OAuthFlowType {
  /** Client Credentials grant (service-to-service / M2M). */
  ClientCredentials = "client_credentials",
  /** Authorization Code with PKCE (interactive browser-based auth). */
  AuthorizationCodePKCE = "authorization_code_pkce",
  /** Device Code grant (CLI / headless environments). */
  DeviceCode = "device_code",
  /** Azure Managed Identity via IMDS. */
  AzureManagedIdentity = "azure_managed_identity",
  /** Workload Identity Federation (K8s, GitHub Actions). */
  WorkloadIdentity = "workload_identity",
}

/**
 * OAuth configuration for LanceDB authentication.
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
   * For Azure: `["api://{app_id}/.default"]`
   */
  scopes: string[];

  /** Authentication flow (default: ClientCredentials). */
  flow?: OAuthFlowType;

  /** Client secret (required for ClientCredentials). */
  clientSecret?: string;

  /** Redirect URI (AuthorizationCodePKCE flow). */
  redirectUri?: string;

  /** Port for local callback server (AuthorizationCodePKCE, default: 8400). */
  callbackPort?: number;

  /** Client ID for user-assigned managed identity (AzureManagedIdentity). */
  managedIdentityClientId?: string;

  /** Path to federated token file (WorkloadIdentity). */
  tokenFile?: string;

  /** Seconds before expiry to trigger proactive refresh (default: 300). */
  refreshBufferSecs?: number;
}
