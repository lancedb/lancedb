# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from pathlib import Path
from typing import Any, Dict, Optional, Union
from urllib.parse import urlparse
import uuid

import pyarrow as pa

from .background_loop import LOOP
from .common import URI
from .remote import ClientConfig
from .remote.db import RemoteDBConnection


_COMMAND_STATEMENT_QUERY_TYPE_URL = (
    b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
)
_DEFAULT_FLIGHT_SQL_PORT = 10025
_DEFAULT_FLIGHT_SQL_TLS_PORT = 10026


def _encode_varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("Cannot encode a negative protobuf varint")

    encoded = bytearray()
    while value > 0x7F:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _encode_command_statement_query(query: str) -> bytes:
    query_bytes = query.encode("utf-8")
    command = b"\x0a" + _encode_varint(len(query_bytes)) + query_bytes
    type_url = _COMMAND_STATEMENT_QUERY_TYPE_URL
    return (
        b"\x0a"
        + _encode_varint(len(type_url))
        + type_url
        + b"\x12"
        + _encode_varint(len(command))
        + command
    )


def _format_host(hostname: str) -> str:
    if ":" in hostname and not hostname.startswith("["):
        return f"[{hostname}]"
    return hostname


def _normalize_flight_sql_uri(uri: str) -> str:
    parsed = urlparse(uri)
    scheme_map = {
        "grpc": "grpc",
        "grpc+tcp": "grpc",
        "http": "grpc",
        "grpc+tls": "grpc+tls",
        "grpcs": "grpc+tls",
        "https": "grpc+tls",
    }
    scheme = scheme_map.get(parsed.scheme.lower())
    if scheme is None:
        raise ValueError(
            "flight_sql_uri must use grpc, grpc+tcp, grpc+tls, grpcs, http, or https"
        )
    if parsed.hostname is None:
        raise ValueError("flight_sql_uri must include a hostname")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("flight_sql_uri must not include user information")
    if parsed.path not in ("", "/") or parsed.params or parsed.query or parsed.fragment:
        raise ValueError("flight_sql_uri must not include a path, query, or fragment")

    default_port = (
        _DEFAULT_FLIGHT_SQL_TLS_PORT
        if scheme == "grpc+tls"
        else _DEFAULT_FLIGHT_SQL_PORT
    )
    try:
        port = parsed.port
    except ValueError as err:
        raise ValueError(f"Invalid flight_sql_uri port: {err}") from err
    if port == 0:
        raise ValueError("flight_sql_uri port must be greater than zero")
    if port is None:
        port = default_port
    return f"{scheme}://{_format_host(parsed.hostname)}:{port}"


def _resolve_flight_sql_uri(
    connection: RemoteDBConnection, flight_sql_uri: Optional[str]
) -> str:
    if flight_sql_uri is not None:
        return _normalize_flight_sql_uri(flight_sql_uri)

    if connection.host_override is None:
        raise ValueError(
            "flight_sql_uri is required when the Flight SQL endpoint cannot be "
            "derived from host_override"
        )

    parsed = urlparse(connection.host_override)
    if parsed.scheme.lower() != "http" or parsed.hostname is None:
        raise ValueError(
            "flight_sql_uri is required for TLS or non-HTTP host overrides"
        )
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("host_override must not include user information")

    try:
        rest_port = parsed.port
    except ValueError as err:
        raise ValueError(f"Invalid host_override port: {err}") from err
    if rest_port == 65535:
        raise ValueError(
            "flight_sql_uri is required when host_override uses port 65535"
        )
    flight_port = rest_port + 1 if rest_port is not None else _DEFAULT_FLIGHT_SQL_PORT
    return f"grpc://{_format_host(parsed.hostname)}:{flight_port}"


def _flight_client_kwargs(
    connection: RemoteDBConnection, flight_sql_uri: str
) -> Dict[str, Any]:
    if not flight_sql_uri.startswith("grpc+tls://"):
        return {}

    tls_config = connection.client_config.tls_config
    if tls_config is None:
        return {}
    if not tls_config.assert_hostname:
        raise ValueError(
            "Flight SQL cannot disable hostname verification without also "
            "disabling certificate verification; configure a valid TLS "
            "hostname instead"
        )

    kwargs: Dict[str, Any] = {}
    if tls_config.ssl_ca_cert is not None:
        kwargs["tls_root_certs"] = Path(tls_config.ssl_ca_cert).read_bytes()
    if tls_config.cert_file is not None:
        kwargs["cert_chain"] = Path(tls_config.cert_file).read_bytes()
    if tls_config.key_file is not None:
        kwargs["private_key"] = Path(tls_config.key_file).read_bytes()
    return kwargs


def _flight_module() -> Any:
    try:
        from pyarrow import flight
    except ImportError as err:
        raise ImportError(
            "lancedb.sql requires a PyArrow build with Flight support"
        ) from err
    return flight


def _flight_call_options(
    connection: RemoteDBConnection, flight: Any, request_id: str
) -> Any:
    headers: Dict[bytes, tuple[bytes, bytes]] = {}
    if connection.client_config.extra_headers is not None:
        for key, value in connection.client_config.extra_headers.items():
            encoded_key = str(key).lower().encode("ascii")
            headers[encoded_key] = (encoded_key, str(value).encode("utf-8"))

    headers[b"authorization"] = (
        b"authorization",
        f"Bearer {connection.api_key}".encode("utf-8"),
    )
    headers[b"database"] = (b"database", connection.db_name.encode("utf-8"))
    headers[b"x-request-id"] = (
        b"x-request-id",
        request_id.encode("ascii"),
    )

    timeout = None
    timeout_config = connection.client_config.timeout_config
    if timeout_config is not None:
        duration = timeout_config.timeout
        if duration is None:
            duration = timeout_config.read_timeout
        if duration is not None:
            timeout = duration.total_seconds()

    return flight.FlightCallOptions(
        timeout=timeout,
        headers=list(headers.values()),
    )


def _location_uri(location: Any) -> str:
    uri = location.uri
    if isinstance(uri, bytes):
        return uri.decode("utf-8")
    return str(uri)


def _read_endpoint(
    endpoint: Any,
    primary_client: Any,
    connection: RemoteDBConnection,
    flight: Any,
    request_id: str,
) -> pa.Table:
    locations = list(endpoint.locations)
    location_uri = _location_uri(locations[0]) if locations else None
    reuse_primary = location_uri is None or location_uri.startswith(
        "arrow-flight-reuse-connection:"
    )
    client = primary_client
    if not reuse_primary:
        client = flight.FlightClient(
            location_uri,
            **_flight_client_kwargs(connection, location_uri),
        )

    try:
        options = _flight_call_options(connection, flight, request_id)
        return client.do_get(endpoint.ticket, options).read_all()
    finally:
        if client is not primary_client:
            client.close()


def _execute_flight_sql(
    query: str,
    connection: RemoteDBConnection,
    flight_sql_uri: Optional[str],
) -> pa.Table:
    flight = _flight_module()
    request_id = str(uuid.uuid4())
    resolved_uri = _resolve_flight_sql_uri(connection, flight_sql_uri)
    client = flight.FlightClient(
        resolved_uri,
        **_flight_client_kwargs(connection, resolved_uri),
    )
    descriptor = flight.FlightDescriptor.for_command(
        _encode_command_statement_query(query)
    )

    try:
        info = client.get_flight_info(
            descriptor, _flight_call_options(connection, flight, request_id)
        )
        if not info.endpoints:
            raise RuntimeError("Flight SQL returned no result endpoints")

        tables = [
            _read_endpoint(endpoint, client, connection, flight, request_id)
            for endpoint in info.endpoints
        ]
        if len(tables) == 1:
            return tables[0]
        return pa.concat_tables(tables)
    except Exception as err:
        raise RuntimeError(f"Flight SQL request {request_id} failed: {err}") from err
    finally:
        client.close()


def sql(
    query: str,
    database: URI,
    *,
    api_key: Optional[str] = None,
    region: str = "us-east-1",
    host_override: Optional[str] = None,
    flight_sql_uri: Optional[str] = None,
    client_config: Union[ClientConfig, Dict[str, Any], None] = None,
    storage_options: Optional[Dict[str, str]] = None,
) -> pa.Table:
    """Execute a SQL statement through a LanceDB Flight SQL server.

    The database is resolved with :func:`lancedb.connect` and becomes the
    statement's default SQL catalog. Fully qualified table references can query
    other databases available to the same server and credentials.

    Parameters
    ----------
    query: str
        The SQL statement to execute.
    database: str or Path
        A remote ``db://`` database URI resolved through :func:`lancedb.connect`.
    api_key: str, optional
        The API key used for the LanceDB connection and Flight SQL authentication.
        Can be set with the ``LANCEDB_API_KEY`` environment variable.
    region: str, default "us-east-1"
        The LanceDB Cloud region passed to :func:`lancedb.connect`.
    host_override: str, optional
        The LanceDB Enterprise HTTP endpoint passed to :func:`lancedb.connect`.
        For a plaintext endpoint, Flight SQL defaults to the following port.
        For example, ``http://localhost:10024`` resolves to
        ``grpc://localhost:10025``, while ``http://localhost`` resolves to
        ``grpc://localhost:10025``.
    flight_sql_uri: str, optional
        The Flight SQL endpoint. Use this for TLS and deployments where the HTTP
        and Flight SQL endpoints do not share a host or consecutive ports.
    client_config: ClientConfig or dict, optional
        Remote client configuration. Static headers, timeouts, and TLS files are
        also applied to Flight SQL where supported.
    storage_options: dict, optional
        Storage options forwarded to :func:`lancedb.connect`.

    Returns
    -------
    pyarrow.Table
        The combined result from all Flight endpoints.

    Examples
    --------
    >>> import lancedb
    >>> result = lancedb.sql(  # doctest: +SKIP
    ...     "SELECT * FROM analytics.public.events LIMIT 10",
    ...     database="db://analytics",
    ...     api_key="ldb_...",
    ...     flight_sql_uri="grpc+tls://sql.example.com:10026",
    ... )
    """
    import lancedb

    connection = lancedb.connect(
        database,
        api_key=api_key,
        region=region,
        host_override=host_override,
        client_config=client_config,
        storage_options=storage_options,
    )
    if not isinstance(connection, RemoteDBConnection):
        raise ValueError("lancedb.sql requires a remote db:// database")

    try:
        return _execute_flight_sql(query, connection, flight_sql_uri)
    finally:
        LOOP.run(connection.close())
