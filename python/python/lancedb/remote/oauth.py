# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


class OAuthFlowType(str, Enum):
    """OAuth authentication flow types."""

    CLIENT_CREDENTIALS = "client_credentials"
    """Client Credentials grant (service-to-service / M2M)."""

    AUTHORIZATION_CODE_PKCE = "authorization_code_pkce"
    """Authorization Code with PKCE (interactive browser-based auth)."""

    DEVICE_CODE = "device_code"
    """Device Code grant (CLI / headless environments)."""

    AZURE_MANAGED_IDENTITY = "azure_managed_identity"
    """Azure Managed Identity via IMDS."""

    WORKLOAD_IDENTITY = "workload_identity"
    """Workload Identity Federation (K8s, GitHub Actions)."""


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
        For Azure: ``["api://{app_id}/.default"]``
    flow : OAuthFlowType
        Authentication flow to use. Default: CLIENT_CREDENTIALS.
    client_secret : Optional[str]
        Client secret (required for CLIENT_CREDENTIALS).
    redirect_uri : Optional[str]
        Redirect URI for AUTHORIZATION_CODE_PKCE flow.
    callback_port : Optional[int]
        Port for local HTTP callback server (AUTHORIZATION_CODE_PKCE, default: 8400).
    managed_identity_client_id : Optional[str]
        Client ID for user-assigned managed identity (AZURE_MANAGED_IDENTITY).
    token_file : Optional[str]
        Path to federated token file (WORKLOAD_IDENTITY).
    refresh_buffer_secs : Optional[int]
        Seconds before expiry to trigger proactive refresh (default: 300).

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
    """

    issuer_url: str
    client_id: str
    scopes: List[str]
    flow: OAuthFlowType = OAuthFlowType.CLIENT_CREDENTIALS
    client_secret: Optional[str] = None
    redirect_uri: Optional[str] = None
    callback_port: Optional[int] = None
    managed_identity_client_id: Optional[str] = None
    token_file: Optional[str] = None
    refresh_buffer_secs: Optional[int] = None
