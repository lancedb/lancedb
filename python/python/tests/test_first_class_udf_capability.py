# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""RED contract tests for local FunctionCapability authoring and @udf capabilities."""

from __future__ import annotations

import inspect

import pyarrow as pa
import pytest

import lancedb
from lancedb import Function, FunctionCapability, Job, udf
from lancedb._udf import _get_udf_config, _package_udf

_SECRET_REFERENCE = "secret://team/capability-redact-token-xyz"
_SECRET_ENV = "API_TOKEN"
_NETWORK_ORIGIN = "https://api.example.com"

_OVERDESIGN_ATTRS = (
    "id",
    "function_id",
    "FunctionVersion",
    "function_version",
    "artifact",
    "digest",
    "authorization",
    "authorized",
    "value",
    "plaintext",
    "plaintext_secret",
    "secret_value",
    "job",
    "job_id",
    "catalog",
    "retry_key",
    "user_version",
    "idempotency_key",
    "deterministic",
    "null_handling",
    "null_policy",
    "on_error",
    "error_policy",
    "geneva",
)


def _decorate(fn, **overrides):
    kwargs = {
        "inputs": {"x": pa.int32()},
        "output": pa.int64(),
        "python": "3.12",
    }
    kwargs.update(overrides)
    return udf(**kwargs)(fn)


def _network(origin: str = _NETWORK_ORIGIN) -> FunctionCapability:
    return FunctionCapability.network(origin)


def _secret(
    reference: str = _SECRET_REFERENCE,
    *,
    environment_variable: str = _SECRET_ENV,
) -> FunctionCapability:
    return FunctionCapability.secret(
        reference,
        environment_variable=environment_variable,
    )


@udf(
    inputs={"x": pa.int32()},
    output=pa.int64(),
    python="3.12",
    packages=["pkg-a==1"],
    output_nullable=False,
)
def packable_without_capabilities(x):
    return x + 1


@udf(
    inputs={"x": pa.int32()},
    output=pa.int64(),
    python="3.12",
    packages=["pkg-a==1"],
    output_nullable=False,
    capabilities=[
        FunctionCapability.network(_NETWORK_ORIGIN),
        FunctionCapability.secret(
            _SECRET_REFERENCE,
            environment_variable=_SECRET_ENV,
        ),
    ],
)
def packable_with_capabilities(x):
    return x + 1


def test_function_capability_export_factories_projection_equality_immutability():
    assert "FunctionCapability" in lancedb.__all__
    assert FunctionCapability is lancedb.FunctionCapability

    network = _network()
    secret = _secret()

    assert network.kind == "network"
    assert network.origin == _NETWORK_ORIGIN
    assert network.reference is None
    assert network.environment_variable is None

    assert secret.kind == "secret"
    assert secret.reference == _SECRET_REFERENCE
    assert secret.environment_variable == _SECRET_ENV
    assert secret.origin is None

    assert network == FunctionCapability.network(_NETWORK_ORIGIN)
    assert secret == FunctionCapability.secret(
        _SECRET_REFERENCE,
        environment_variable=_SECRET_ENV,
    )
    assert network != secret
    assert network != FunctionCapability.network("https://other.example.com")
    assert secret != FunctionCapability.secret(
        _SECRET_REFERENCE,
        environment_variable="OTHER_TOKEN",
    )

    public_attrs = ("kind", "origin", "reference", "environment_variable")
    internal_slots = ("_kind", "_origin", "_reference", "_environment_variable")
    immutable_attrs = public_attrs + internal_slots

    for attr in public_attrs:
        with pytest.raises(AttributeError):
            setattr(network, attr, None)
        with pytest.raises(AttributeError):
            setattr(secret, attr, None)

    for attr in immutable_attrs:
        # Fresh instances per attempt so a RED slot mutation cannot corrupt
        # shared fixtures used by later assertions in this test.
        fresh_network = _network("https://fresh-immutability.example.com")
        fresh_secret = _secret(
            "secret://team/fresh-immutability-token",
            environment_variable="FRESH_IMMUTABILITY_TOKEN",
        )
        with pytest.raises(AttributeError):
            setattr(fresh_network, attr, None)
        with pytest.raises(AttributeError):
            setattr(fresh_secret, attr, None)
        with pytest.raises(AttributeError):
            delattr(fresh_network, attr)
        with pytest.raises(AttributeError):
            delattr(fresh_secret, attr)

    retained_origin = "https://config-retain.example.com"
    retained_reference = "secret://team/config-retain-token"
    retained_env = "CONFIG_RETAIN_TOKEN"
    retained_network = FunctionCapability.network(retained_origin)
    retained_secret = FunctionCapability.secret(
        retained_reference,
        environment_variable=retained_env,
    )
    expected_capabilities = (
        FunctionCapability.network(retained_origin),
        FunctionCapability.secret(
            retained_reference,
            environment_variable=retained_env,
        ),
    )

    def retain_target(x):
        return x

    retained = udf(
        inputs={"x": pa.int32()},
        output=pa.int32(),
        python="3.12",
        capabilities=[retained_network, retained_secret],
    )(retain_target)
    retained_config = _get_udf_config(retained)
    assert retained_config.capabilities == expected_capabilities

    for attr in immutable_attrs:
        with pytest.raises(AttributeError):
            setattr(retained_network, attr, "mutated")
        with pytest.raises(AttributeError):
            setattr(retained_secret, attr, "mutated")
        with pytest.raises(AttributeError):
            delattr(retained_network, attr)
        with pytest.raises(AttributeError):
            delattr(retained_secret, attr)

    assert retained_config.capabilities == expected_capabilities
    assert retained_config.capabilities[0] is retained_network
    assert retained_config.capabilities[1] is retained_secret
    assert retained_config.capabilities[0].kind == "network"
    assert retained_config.capabilities[0].origin == retained_origin
    assert retained_config.capabilities[0].reference is None
    assert retained_config.capabilities[0].environment_variable is None
    assert retained_config.capabilities[1].kind == "secret"
    assert retained_config.capabilities[1].reference == retained_reference
    assert retained_config.capabilities[1].environment_variable == retained_env
    assert retained_config.capabilities[1].origin is None

    with pytest.raises(TypeError):
        FunctionCapability()
    with pytest.raises(TypeError):
        FunctionCapability(  # type: ignore[call-arg]
            kind="network",
            origin=_NETWORK_ORIGIN,
        )

    assert not isinstance(network, Function)
    assert not isinstance(secret, Function)
    assert not isinstance(network, Job)
    assert not isinstance(secret, Job)
    for attr in _OVERDESIGN_ATTRS:
        assert not hasattr(network, attr)
        assert not hasattr(secret, attr)


def test_function_capability_validation_and_secret_redaction():
    with pytest.raises(TypeError):
        FunctionCapability.network(None)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        FunctionCapability.network(123)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        FunctionCapability.network("")

    # Backend authorization owns URL/scheme policy; non-empty is enough here.
    loose = FunctionCapability.network("example.com")
    assert loose.kind == "network"
    assert loose.origin == "example.com"

    with pytest.raises(TypeError):
        FunctionCapability.secret(  # type: ignore[misc]
            _SECRET_REFERENCE,
            _SECRET_ENV,
        )
    with pytest.raises(TypeError):
        FunctionCapability.secret(None, environment_variable=_SECRET_ENV)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        FunctionCapability.secret(123, environment_variable=_SECRET_ENV)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        FunctionCapability.secret(_SECRET_REFERENCE, environment_variable=None)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        FunctionCapability.secret(_SECRET_REFERENCE, environment_variable=1)  # type: ignore[arg-type]

    with pytest.raises(ValueError) as empty_ref:
        FunctionCapability.secret("", environment_variable=_SECRET_ENV)
    assert _SECRET_REFERENCE not in str(empty_ref.value)
    assert _SECRET_REFERENCE not in repr(empty_ref.value)

    with pytest.raises(ValueError) as empty_env:
        FunctionCapability.secret(_SECRET_REFERENCE, environment_variable="")
    assert _SECRET_REFERENCE not in str(empty_env.value)
    assert _SECRET_REFERENCE not in repr(empty_env.value)

    with pytest.raises(TypeError):
        FunctionCapability.secret(  # type: ignore[call-arg]
            _SECRET_REFERENCE,
            environment_variable=_SECRET_ENV,
            value="super-secret",
        )
    with pytest.raises(TypeError):
        FunctionCapability.secret(  # type: ignore[call-arg]
            _SECRET_REFERENCE,
            environment_variable=_SECRET_ENV,
            plaintext_secret="super-secret",
        )
    with pytest.raises(TypeError):
        FunctionCapability.secret(  # type: ignore[call-arg]
            _SECRET_REFERENCE,
            environment_variable=_SECRET_ENV,
            environment={_SECRET_ENV: "super-secret"},
        )
    with pytest.raises(TypeError):
        FunctionCapability.secret(  # type: ignore[call-arg]
            _SECRET_REFERENCE,
            environment_variable=_SECRET_ENV,
            headers={"Authorization": "Bearer super-secret"},
        )
    with pytest.raises(TypeError):
        FunctionCapability.network(  # type: ignore[call-arg]
            _NETWORK_ORIGIN,
            headers={"X-Trace": "1"},
        )

    secret = _secret()
    assert not hasattr(secret, "value")
    assert not hasattr(secret, "plaintext")
    assert not hasattr(secret, "plaintext_secret")
    assert not hasattr(secret, "secret_value")

    secret_text = repr(secret)
    assert "secret" in secret_text.lower()
    assert _SECRET_ENV in secret_text
    assert _SECRET_REFERENCE not in secret_text
    assert "super-secret" not in secret_text

    network_text = repr(_network())
    assert "network" in network_text.lower()
    assert _NETWORK_ORIGIN in network_text


def test_udf_capabilities_ordered_immutable_config_default_and_validation():
    params = inspect.signature(udf).parameters
    assert "capabilities" in params
    assert params["capabilities"].kind is inspect.Parameter.KEYWORD_ONLY
    assert params["capabilities"].default == ()

    def identity_target(x):
        """capabilities identity marker."""
        return x + 1

    original = identity_target
    decorated = _decorate(identity_target)
    assert decorated is original
    assert decorated.__name__ == "identity_target"
    assert decorated.__doc__ == "capabilities identity marker."
    assert decorated(2) == 3
    assert _get_udf_config(decorated).capabilities == ()

    first = _network("https://b.example.com")
    second = _network("https://a.example.com")
    third = _network("https://b.example.com")
    secret = _secret()
    capabilities = [first, second, third, secret]

    def combine(x):
        return x

    with_caps = udf(
        inputs={"x": pa.int32()},
        output=pa.int32(),
        python="3.12",
        packages=["pkg-b==2", "pkg-a==1"],
        capabilities=capabilities,
    )(combine)
    config = _get_udf_config(with_caps)
    assert config.capabilities == (first, second, third, secret)
    assert isinstance(config.capabilities, tuple)
    assert config.packages == ("pkg-b==2", "pkg-a==1")
    assert config.inputs == (("x", pa.int32()),)

    capabilities.append(_network("https://mutated.example.com"))
    capabilities[0] = _network("https://replaced.example.com")
    assert config.capabilities == (first, second, third, secret)

    with pytest.raises(AttributeError):
        setattr(config, "capabilities", ())

    def target(x):
        return x

    with pytest.raises(TypeError):
        _decorate(target, capabilities="https://api.example.com")

    with pytest.raises(TypeError):
        _decorate(target, capabilities=b"https://api.example.com")

    class _BadCapability:
        def __repr__(self) -> str:
            return "unique-bad-capability-repr-xyz"

    with pytest.raises(TypeError) as bad_item:
        _decorate(target, capabilities=[_BadCapability()])
    assert "unique-bad-capability-repr-xyz" not in str(bad_item.value)
    assert "unique-bad-capability-repr-xyz" not in repr(bad_item.value)

    with pytest.raises(TypeError) as bad_mixed:
        _decorate(
            target,
            capabilities=[_network(), "unique-bad-capability-string-xyz"],
        )
    assert "unique-bad-capability-string-xyz" not in str(bad_mixed.value)
    assert "unique-bad-capability-string-xyz" not in repr(bad_mixed.value)


def test_udf_capabilities_rejects_function_capability_subclass_before_property_access():
    marker = "unique-hostile-capability-subclass-marker-xyz"

    class _HostileFunctionCapability(FunctionCapability):
        @property
        def kind(self) -> str:
            raise RuntimeError(marker)

        @property
        def origin(self) -> str | None:
            raise RuntimeError(marker)

        @property
        def reference(self) -> str | None:
            raise RuntimeError(marker)

        @property
        def environment_variable(self) -> str | None:
            raise RuntimeError(marker)

    hostile = object.__new__(_HostileFunctionCapability)
    assert isinstance(hostile, FunctionCapability)
    assert type(hostile) is not FunctionCapability

    def target(x):
        return x

    with pytest.raises(TypeError) as exc_info:
        _decorate(target, capabilities=[hostile])
    assert marker not in str(exc_info.value)
    assert marker not in repr(exc_info.value)
    assert _SECRET_REFERENCE not in str(exc_info.value)
    assert _SECRET_REFERENCE not in repr(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


def test_package_udf_preserves_capabilities_and_redacts_secret_reference():
    packaged = _package_udf(packable_with_capabilities)
    config = packaged.config

    assert packaged.config is _get_udf_config(packable_with_capabilities)
    assert config.capabilities == (
        FunctionCapability.network(_NETWORK_ORIGIN),
        FunctionCapability.secret(
            _SECRET_REFERENCE,
            environment_variable=_SECRET_ENV,
        ),
    )
    assert config.capabilities[0].kind == "network"
    assert config.capabilities[0].origin == _NETWORK_ORIGIN
    assert config.capabilities[1].kind == "secret"
    assert config.capabilities[1].reference == _SECRET_REFERENCE
    assert config.capabilities[1].environment_variable == _SECRET_ENV
    assert config.packages == ("pkg-a==1",)
    assert config.python == "3.12"
    assert config.output_nullable is False

    nested = (
        f"{packaged!r}\n{config!r}\n{config.capabilities!r}\n{config.capabilities[1]!r}"
    )
    assert _SECRET_REFERENCE not in nested
    assert _SECRET_ENV in repr(config.capabilities[1])


def test_capabilities_are_additive_to_existing_declaration_and_packaging():
    def score(x):
        return x

    decorated = _decorate(
        score,
        packages=["score==1.0"],
        output_nullable=True,
    )
    config = _get_udf_config(decorated)
    assert config.inputs == (("x", pa.int32()),)
    assert config.output == pa.int64()
    assert config.output_nullable is True
    assert config.python == "3.12"
    assert config.packages == ("score==1.0",)
    assert config.capabilities == ()
    assert decorated is score
    assert decorated(4) == 4

    packaged = _package_udf(packable_without_capabilities)
    assert packaged.config is _get_udf_config(packable_without_capabilities)
    assert packaged.callable_name == "packable_without_capabilities"
    assert packaged.config.capabilities == ()
    assert packaged.config.packages == ("pkg-a==1",)
    assert packaged.config.output_nullable is False
    assert packable_without_capabilities(1) == 2

    params = inspect.signature(udf).parameters
    for name in (
        "user_version",
        "idempotency_key",
        "deterministic",
        "null_handling",
        "null_policy",
        "on_error",
        "error_policy",
        "FunctionVersion",
        "artifact",
        "digest",
        "geneva",
    ):
        assert name not in params
