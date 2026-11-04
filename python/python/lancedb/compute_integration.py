# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Database-owned compute configuration using credential Secret references."""

from dataclasses import dataclass
from typing import Literal, Optional

from .storage_integration import SecretReference


@dataclass(frozen=True)
class ComputeIntegrationInfo:
    """Registered Kubernetes configuration; registration does not verify access.

    Credential fields contain Secret references, never credential values.
    ``kubernetes_image_pull_secret`` names a Kubernetes Secret already installed
    in the target namespace, rather than a database-owned Secret.
    """

    name: str
    database: str
    namespace_path: list[str]
    compute_provider: Literal["KUBERNETES"]
    job_submission_endpoint: str
    kubernetes_namespace: str
    kubernetes_bearer_token_secret: SecretReference
    created_at_millis: int
    access_verified: bool
    kubernetes_ca_cert_secret: Optional[SecretReference] = None
    kubernetes_service_account: Optional[str] = None
    kubernetes_image_pull_secret: Optional[str] = None

    def __post_init__(self):
        for field in ("kubernetes_bearer_token_secret", "kubernetes_ca_cert_secret"):
            value = getattr(self, field)
            if isinstance(value, dict):
                object.__setattr__(self, field, SecretReference(**value))
