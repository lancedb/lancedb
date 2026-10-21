# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Catalog-scoped cloud storage integrations and AWS setup information."""

from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional


@dataclass(frozen=True)
class StorageIntegrationInfo:
    """Registered S3 configuration and policies to give to your AWS administrator.

    ``configuration_instructions`` contains the account-specific trust policy and
    prefix-scoped permission policy. Creating this object does not verify AWS
    access or configure a storage operation to use it. No field contains AWS
    access credentials.
    """

    name: str
    storage_provider: Literal["S3"]
    storage_prefix: str
    aws_role_arn: str
    aws_region: Optional[str]
    aws_account_id: str
    aws_iam_principal_arn: str
    aws_external_id: str
    created_at_millis: int
    access_verified: bool
    trust_policy: Dict[str, Any]
    permissions_policy: Dict[str, Any]
    configuration_instructions: str
