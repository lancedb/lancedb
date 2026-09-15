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

    Device Authorization:

    The verification URL and user code are written to standard error before
    polling begins.

    >>> config = OAuthConfig(
    ...     issuer_url="https://login.microsoftonline.com/{tenant}/v2.0",
    ...     client_id="app-id",
    ...     scopes=["openid", "api://lancedb-api/access"],
    ...     flow=OAuthFlowType.DEVICE_CODE,
    ... )
    """

    issuer_url: str
    client_id: str
    scopes: List[str]
    flow: OAuthFlowType = OAuthFlowType.CLIENT_CREDENTIALS
    client_secret: Optional[str] = field(default=None, repr=False)
    redirect_uri: Optional[str] = None
    callback_port: Optional[int] = None
    use_pkce: bool = True
    managed_identity_client_id: Optional[str] = None
    refresh_buffer_secs: Optional[int] = None
