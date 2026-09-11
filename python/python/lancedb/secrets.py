# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Named Secrets, and the bindings that deliver them to Functions.

A Secret is a database-scoped named credential. Nothing in this module holds a
value: :class:`EnvVarSecret` names one and says which environment variable it
should arrive in, and the value is resolved by the remote service when a
Function bound to it runs. No API returns a stored credential, by construction
rather than by policy -- there is no code path that could.
"""

from __future__ import annotations

import re

# The same characters LanceDB already admits in a namespace or table name, and
# no positional rule on top of them: a segment may begin with `_`, `-` or `.`
# today, so anything narrower would put Secrets out of reach inside namespaces
# that already exist. Matches the service, which admits the same set.
_SECRET_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,255}$")
_ENV_VARIABLE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_secret_name(name: str) -> str:
    """Check a Secret name locally and return it unchanged."""
    if not isinstance(name, str):
        raise TypeError(f"Secret name must be a string, not {type(name).__name__}")
    if not _SECRET_NAME.fullmatch(name):
        raise ValueError(f"invalid Secret name: {name!r}")
    return name


def validate_env_variable(name: str) -> str:
    """Check an environment variable name locally and return it unchanged."""
    if not isinstance(name, str):
        raise TypeError(
            f"environment variable name must be a string, not {type(name).__name__}"
        )
    if not _ENV_VARIABLE.fullmatch(name):
        raise ValueError(f"invalid environment variable name: {name!r}")
    return name


class EnvVarSecret:
    """A Secret bound to the environment variable a Function's library reads.

    Pass these in the ``secrets`` sequence of
    [DBConnection.create_function][lancedb.db.DBConnection.create_function]. The
    Function's source is unchanged by binding: it reads ``OPENAI_API_KEY`` the
    way it always did, and the binding is what puts a value there.

    This is a local value. Constructing it contacts no server, so it always
    succeeds and says nothing about whether the Secret exists; that is checked
    at registration, where a mistyped Secret name surfaces as a clear "does not
    exist" naming both the Secret and the variable bound to it. A mistyped
    *variable* name cannot be caught anywhere -- nothing knows which variables a
    Function reads -- so it surfaces on the first rows instead.

    The type exists so a credential cannot be passed by accident. A bare string
    in the same position is a plausible-looking mistake with the opposite
    meaning, and it reads identically in a diff.

    Parameters
    ----------
    secret : str
        The Secret's database-scoped name.
    env_variable : str
        The environment variable the Function reads it from.

    Examples
    --------
    >>> from lancedb import EnvVarSecret
    >>> binding = EnvVarSecret(secret="openai-prod", env_variable="OPENAI_API_KEY")
    >>> binding.secret, binding.env_variable
    ('openai-prod', 'OPENAI_API_KEY')
    """

    __slots__ = ("_secret", "_env_variable")

    def __init__(self, secret: str, env_variable: str):
        self._secret = validate_secret_name(secret)
        self._env_variable = validate_env_variable(env_variable)

    @property
    def secret(self) -> str:
        """The Secret's database-scoped name."""
        return self._secret

    @property
    def env_variable(self) -> str:
        """The environment variable the value is delivered in."""
        return self._env_variable

    def __repr__(self) -> str:
        return (
            f"EnvVarSecret(secret={self._secret!r}, "
            f"env_variable={self._env_variable!r})"
        )

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, EnvVarSecret)
            and other._secret == self._secret
            and other._env_variable == self._env_variable
        )

    def __hash__(self) -> int:
        return hash((EnvVarSecret, self._secret, self._env_variable))


class SecretInfo:
    """What a database records about a Secret. Never its value.

    Returned by
    [DBConnection.describe_secret][lancedb.db.DBConnection.describe_secret].
    """

    __slots__ = ("_name", "_created_at_millis", "_updated_at_millis")

    def __init__(self, name: str, created_at_millis: int, updated_at_millis: int):
        self._name = name
        self._created_at_millis = created_at_millis
        self._updated_at_millis = updated_at_millis

    @property
    def name(self) -> str:
        """The Secret's database-scoped name."""
        return self._name

    @property
    def created_at_millis(self) -> int:
        """When the Secret was created, in milliseconds since the Unix epoch."""
        return self._created_at_millis

    @property
    def updated_at_millis(self) -> int:
        """When the Secret's value was last rotated, in epoch milliseconds.

        The only observable that a rotation landed: no API returns a credential,
        so a caller confirms ``alter_secret`` took effect by watching this move.
        """
        return self._updated_at_millis

    @classmethod
    def from_json(cls, value: dict) -> "SecretInfo":
        return cls(
            name=value["name"],
            created_at_millis=value["created_at_millis"],
            updated_at_millis=value["updated_at_millis"],
        )

    def __repr__(self) -> str:
        return (
            f"SecretInfo(name={self._name!r}, "
            f"created_at_millis={self._created_at_millis!r}, "
            f"updated_at_millis={self._updated_at_millis!r})"
        )

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, SecretInfo)
            and other._name == self._name
            and other._created_at_millis == self._created_at_millis
            and other._updated_at_millis == self._updated_at_millis
        )


__all__ = [
    "EnvVarSecret",
    "SecretInfo",
    "validate_env_variable",
    "validate_secret_name",
]
