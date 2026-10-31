# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Database-owned storage integrations and references to credential Secrets."""

from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional


@dataclass(frozen=True)
class SecretReference:
    """A credential Secret, optionally qualified by database and namespace."""

    name: str
    database: Optional[str] = None
    namespace_path: tuple[str, ...] = ()


@dataclass(frozen=True)
class StorageIntegrationInfo:
    """Registered cloud configuration and setup instructions.

    ``configuration_instructions`` is a single sentence of setup guidance for
    display; its wording may change. Use the structured identity and policy fields
    instead of parsing that text. Creating this object does not verify AWS access
    or configure a storage operation to use it. Static-key authentication stores
    only Secret references. No field contains AWS credentials.
    """

    name: str
    database: str
    namespace_path: list[str]
    storage_provider: Literal["S3", "AZURE", "GCP"]
    storage_prefix: str
    aws_region: Optional[str]
    aws_account_id: str
    aws_iam_principal_arn: str
    aws_external_id: str
    created_at_millis: int
    access_verified: bool
    trust_policy: Optional[Dict[str, Any]]
    permissions_policy: Optional[Dict[str, Any]]
    configuration_instructions: str
    aws_role_arn: Optional[str] = None
    aws_access_key_id_secret: Optional[SecretReference] = None
    aws_secret_access_key_secret: Optional[SecretReference] = None
    azure_storage_account: Optional[str] = None
    azure_tenant_id_secret: Optional[SecretReference] = None
    azure_client_id_secret: Optional[SecretReference] = None
    azure_client_secret_secret: Optional[SecretReference] = None
    gcp_credentials_json_secret: Optional[SecretReference] = None

    def __post_init__(self):
        for field in (
            "aws_access_key_id_secret",
            "aws_secret_access_key_secret",
            "azure_tenant_id_secret",
            "azure_client_id_secret",
            "azure_client_secret_secret",
            "gcp_credentials_json_secret",
        ):
            value = getattr(self, field)
            if isinstance(value, dict):
                object.__setattr__(self, field, SecretReference(**value))
