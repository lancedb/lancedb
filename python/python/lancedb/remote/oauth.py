# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class OAuthFlowType(str, Enum):
    """OAuth authentication flow types."""

    CLIENT_CREDENTIALS = "client_credentials"
    """Client Credentials grant (service-to-service / M2M)."""

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
    """

    issuer_url: str
    client_id: str
    scopes: List[str]
    flow: OAuthFlowType = OAuthFlowType.CLIENT_CREDENTIALS
    client_secret: Optional[str] = field(default=None, repr=False)
    managed_identity_client_id: Optional[str] = None
    refresh_buffer_secs: Optional[int] = None
