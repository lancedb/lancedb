# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class OAuthFlowType(str, Enum):
    """OAuth authentication flow types."""

    CLIENT_CREDENTIALS = "client_credentials"
    """Client Credentials grant (service-to-service / M2M)."""

    AUTHORIZATION_CODE = "authorization_code"
    """Interactive Authorization Code grant, using PKCE by default."""

    DEVICE_CODE = "device_code"
    """Device Authorization grant for CLI and headless environments."""

    AZURE_MANAGED_IDENTITY = "azure_managed_identity"
    """Azure Managed Identity via IMDS."""


class ClientAuthMethod(str, Enum):
    """How the client authenticates to the OAuth token endpoint.

    The method applies to every OAuth request that carries client
    authentication: client-credentials, authorization-code exchange,
    refresh-token, and device-authorization requests. The Azure managed
    identity flow ignores this option.
    """

    NONE = "none"
    """No client authentication, for public clients using PKCE or the device
    flow. Cannot be combined with ``client_secret``."""

    CLIENT_SECRET_BASIC = "client_secret_basic"
    """HTTP Basic authentication. This is the RFC 6749 recommended method and
    the normal default for confidential clients, including default Okta
    applications. Requires ``client_secret``."""

    CLIENT_SECRET_POST = "client_secret_post"
    """Credentials in the request body, for providers configured to require
    it. Requires ``client_secret``."""


@dataclass
class TokenCacheOptions:
    """Options for the persistent OAuth token cache.

    The cache is opt-in: it is only used when set as ``token_cache`` on
    :class:`OAuthConfig`. Only refresh tokens are persisted, in a private
    directory with owner-only permissions, so short-lived processes can reuse
    an authenticated session instead of re-prompting on every start.

    Parameters
    ----------
    cache_dir : Optional[str]
        Directory that holds cached credentials. Defaults to
        ``$XDG_CACHE_HOME/lancedb/oauth``, ``$HOME/.cache/lancedb/oauth`` on
        Unix, or ``%LOCALAPPDATA%\\lancedb\\oauth`` on Windows. The directory
        is created with owner-only permissions (``0700``) when missing.
    lock_timeout_secs : Optional[int]
        How long to wait for the cross-process refresh lock before failing
        (default: 30 seconds).

    Examples
    --------
    >>> opts = TokenCacheOptions(cache_dir="/tmp/my-app/oauth-cache")

    Multiple identities (issuer, client, scopes, flow, client
    authentication) get separate cache entries. Within one identity the most
    recent login wins.
    """

    cache_dir: Optional[str] = None
    lock_timeout_secs: Optional[int] = None


@dataclass
class OAuthConfig:
    """OAuth configuration for LanceDB authentication.

    All token acquisition and refresh is handled in the Rust layer.
    This config is passed through to Rust via PyO3.

    Parameters
    ----------
    issuer_url : str
        OIDC issuer URL or OAuth authority URL.
        For Azure: ``https://login.microsoftonline.com/{tenant_id}/v2.0``
    client_id : str
        Application / Client ID.
    scopes : List[str]
        OAuth scopes to request.
        For Azure managed identity, exactly one scope or resource is required.
        For example: ``["api://{app_id}/.default"]``
    flow : OAuthFlowType
        Authentication flow to use. Default: CLIENT_CREDENTIALS.
    client_secret : Optional[str]
        Client secret (required for CLIENT_CREDENTIALS).
    client_auth_method : Optional[ClientAuthMethod]
        How the client authenticates to the token endpoint (default: auto).
        With a ``client_secret`` the default is
        ``ClientAuthMethod.CLIENT_SECRET_BASIC``, which matches the RFC 6749
        recommendation and the default configuration of Okta confidential
        applications; without a secret the client is public and no client
        authentication is sent.
    redirect_uri : Optional[str]
        Loopback redirect URI for AUTHORIZATION_CODE. The default is
        ``http://127.0.0.1:{callback_port}/callback``.
    callback_port : Optional[int]
        Port for the AUTHORIZATION_CODE loopback callback server (default: 8400).
    use_pkce : bool
        Protect AUTHORIZATION_CODE with S256 PKCE (default: True).
    managed_identity_client_id : Optional[str]
        Client ID for user-assigned managed identity (AZURE_MANAGED_IDENTITY).
    refresh_buffer_secs : Optional[int]
        Seconds before expiry to trigger proactive refresh (default: 300).
        Keep this well below the token TTL; if it is greater than or equal to
        the TTL, each request refreshes the token.
    token_cache : Optional[TokenCacheOptions]
        Opt in to the persistent token cache so short-lived processes reuse
        one session. Only supported by AUTHORIZATION_CODE and DEVICE_CODE;
        azure managed identity is rejected. Default: None (memory only).

    Examples
    --------
    Client Credentials (service-to-service):

    >>> config = OAuthConfig(
    ...     issuer_url="https://login.microsoftonline.com/{tenant}/v2.0",
    ...     client_id="app-id",
    ...     client_secret="secret",
    ...     scopes=["api://lancedb-api/.default"],
    ... )

    Azure Managed Identity:

    >>> config = OAuthConfig(
    ...     issuer_url="https://login.microsoftonline.com/{tenant}/v2.0",
    ...     client_id="app-id",
    ...     scopes=["api://lancedb-api/.default"],
    ...     flow=OAuthFlowType.AZURE_MANAGED_IDENTITY,
    ... )

    Authorization Code with PKCE:

    The authorization URL is written to standard error before LanceDB tries to
    open a browser, so it can be copied in headless environments.

    >>> config = OAuthConfig(
    ...     issuer_url="https://login.microsoftonline.com/{tenant}/v2.0",
    ...     client_id="app-id",
    ...     scopes=["openid", "api://lancedb-api/access"],
    ...     flow=OAuthFlowType.AUTHORIZATION_CODE,
    ... )

    Device Authorization, with a persistent cache so later processes reuse
    the session without a new device prompt. The verification URL and user
    code are written to standard error before polling begins:

    >>> config = OAuthConfig(
    ...     issuer_url="https://login.microsoftonline.com/{tenant}/v2.0",
    ...     client_id="app-id",
    ...     scopes=["openid", "offline_access", "api://lancedb-api/access"],
    ...     flow=OAuthFlowType.DEVICE_CODE,
    ...     token_cache=TokenCacheOptions(),
    ... )
    """

    issuer_url: str
    client_id: str
    scopes: List[str]
    flow: OAuthFlowType = OAuthFlowType.CLIENT_CREDENTIALS
    client_secret: Optional[str] = field(default=None, repr=False)
    client_auth_method: Optional[ClientAuthMethod] = None
    redirect_uri: Optional[str] = None
    callback_port: Optional[int] = None
    use_pkce: bool = True
    managed_identity_client_id: Optional[str] = None
    refresh_buffer_secs: Optional[int] = None
    token_cache: Optional[TokenCacheOptions] = None


class OAuthSession:
    """Explicit OAuth session lifecycle for the persistent token cache.

    Built from the same :class:`OAuthConfig` used for
    :func:`lancedb.connect_async` (including its ``token_cache`` options).
    A connection created with the same configuration shares the cache, so
    logging in here prepares tokens for later processes without any database
    request.

    ``login`` always runs the configured interactive flow and replaces the
    cached session (the most recent login wins). ``logout`` removes only the
    local credential; it does not revoke anything with the provider and does
    not sign out of a browser SSO session.

    Examples
    --------
    >>> config = OAuthConfig(
    ...     issuer_url="https://issuer.example.com",
    ...     client_id="my-app",
    ...     scopes=["openid", "offline_access"],
    ...     flow=OAuthFlowType.DEVICE_CODE,
    ...     token_cache=TokenCacheOptions(),
    ... )
    >>> session = OAuthSession(config)  # doctest: +SKIP
    >>> status = await session.login()  # doctest: +SKIP
    >>> status.refreshable  # doctest: +SKIP
    True
    """

    def __init__(self, config: OAuthConfig):
        from lancedb._lancedb import OAuthSession as PyOAuthSession

        self._inner: PyOAuthSession = PyOAuthSession(config)

    async def login(self):
        """Eagerly run the configured flow and store the session.

        Returns a :class:`lancedb._lancedb.SessionStatus` describing the
        cached session. A successful login always replaces any prior cached
        session for this identity; if the provider does not issue a refresh
        token (for example without ``offline_access``), the previous record
        is removed and ``refreshable`` is ``False``.
        """
        return await self._inner.login()

    async def status(self):
        """Report whether a cached session exists, with safe metadata.

        Never contacts the identity provider and never exposes token values.
        """
        return await self._inner.status()

    async def logout(self):
        """Remove the matching local cached credential.

        Returns a :class:`lancedb._lancedb.SessionLogout` whose ``removed``
        flag reports whether a credential existed. Logout is idempotent.
        """
        return await self._inner.logout()
