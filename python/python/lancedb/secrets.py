# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Named Secrets, and the references that bind them to Functions.

A Secret is a database-scoped named credential. Nothing in this module holds a
value: :class:`SecretRef` names one, and the value is resolved by the remote
service when a Function bound to it runs. No API returns a stored credential,
by construction rather than by policy -- there is no code path that could.
"""

from __future__ import annotations

import re

_SECRET_NAME = re.compile(r"^[A-Za-z0-9_-]{1,255}$")


def validate_secret_name(name: str) -> str:
    """Check a Secret name locally and return it unchanged."""
    if not isinstance(name, str):
        raise TypeError(f"Secret name must be a string, not {type(name).__name__}")
    if not _SECRET_NAME.fullmatch(name):
        raise ValueError(f"invalid Secret name: {name!r}")
    return name


class SecretRef:
    """A reference to a named Secret in one database.

    Build one with
    [DBConnection.ref_secret][lancedb.db.DBConnection.ref_secret] and pass it
    in the ``secrets`` mapping of
    [DBConnection.create_function][lancedb.db.DBConnection.create_function].

    This is a local handle. Constructing it contacts no server, so it always
    succeeds and says nothing about whether the Secret exists; that is checked
    at registration, where a mistyped name surfaces as a clear "no such Secret"
    rather than as a client-side lookup that was already stale by the time the
    binding was used.

    The type exists so a credential cannot be passed by accident. A bare string
    in the same position -- ``secrets={"OPENAI_API_KEY": os.environ[...]}`` --
    is a plausible-looking mistake with the opposite meaning, and it reads
    identically in a diff.

    Examples
    --------
    >>> from lancedb.secrets import SecretRef
    >>> SecretRef("openai-prod").name
    'openai-prod'
    """

    __slots__ = ("_name",)

    def __init__(self, name: str):
        self._name = validate_secret_name(name)

    @property
    def name(self) -> str:
        """The Secret's database-scoped name."""
        return self._name

    def __repr__(self) -> str:
        return f"SecretRef({self._name!r})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, SecretRef) and other._name == self._name

    def __hash__(self) -> int:
        return hash((SecretRef, self._name))


__all__ = ["SecretRef", "validate_secret_name"]
