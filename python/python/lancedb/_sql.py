# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from pathlib import Path
import ssl
import time
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
_DEFAULT_FLIGHT_SQL_TIMEOUT_SECONDS = 300.0


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
        context = ssl.create_default_context()
        platform_roots = b"".join(
            ssl.DER_cert_to_PEM_cert(cert).encode("ascii")
            for cert in context.get_ca_certs(binary_form=True)
        )
        configured_roots = Path(tls_config.ssl_ca_cert).read_bytes()
        kwargs["tls_root_certs"] = platform_roots + configured_roots
    if (tls_config.cert_file is None) != (tls_config.key_file is None):
        raise ValueError("Flight SQL mTLS requires both cert_file and key_file")
    if tls_config.cert_file is not None and tls_config.key_file is not None:
        kwargs["cert_chain"] = Path(tls_config.cert_file).read_bytes()
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
    connection: RemoteDBConnection,
    flight: Any,
    request_id: str,
    *,
    streaming: bool = False,
    deadline: Optional[float] = None,
) -> Any:
    headers: Dict[bytes, tuple[bytes, bytes]] = {}

    def add_headers(values: Dict[Any, Any]) -> None:
        for key, value in values.items():
            try:
                encoded_key = str(key).lower().encode("ascii")
                encoded_value = str(value).encode("ascii")
            except UnicodeEncodeError as err:
                raise ValueError(f"Flight SQL metadata must be ASCII: {key!r}") from err
            headers[encoded_key] = (encoded_key, encoded_value)

    extra_headers = connection.client_config.extra_headers or {}
    header_provider = connection.client_config.header_provider
    provider_headers = header_provider.get_headers() if header_provider else {}
    credential_names = {
        str(key).lower() for key in [*extra_headers.keys(), *provider_headers.keys()]
    }
    if "authorization" in credential_names and "x-api-key" in credential_names:
        raise ValueError(
            "Flight SQL accepts either authorization or x-api-key, not both"
        )
    if not credential_names.intersection({"authorization", "x-api-key"}):
        add_headers({"authorization": f"Bearer {connection.api_key}"})
    add_headers(extra_headers)
    add_headers(provider_headers)
    add_headers(
        {
            "database": connection.db_name,
            "x-request-id": request_id,
        }
    )

    timeout = None
    if deadline is not None:
        timeout = max(0.0, deadline - time.monotonic())
    elif not streaming:
        timeout_config = connection.client_config.timeout_config
        if timeout_config is not None and timeout_config.read_timeout is not None:
            timeout = timeout_config.read_timeout.total_seconds()
    if timeout is None and not streaming:
        timeout = _DEFAULT_FLIGHT_SQL_TIMEOUT_SECONDS

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
    primary_uri: str,
    deadline: Optional[float],
) -> pa.Table:
    locations = list(endpoint.locations)
    location_uri = _location_uri(locations[0]) if locations else None
    client = primary_client
    if location_uri is not None and not location_uri.startswith(
        "arrow-flight-reuse-connection:"
    ):
        location_uri = _normalize_flight_sql_uri(location_uri)
        if primary_uri.startswith("grpc+tls://") and not location_uri.startswith(
            "grpc+tls://"
        ):
            raise ValueError("Flight SQL refused a TLS-to-plaintext endpoint redirect")
        client = flight.FlightClient(
            location_uri,
            **_flight_client_kwargs(connection, location_uri),
        )

    try:
        options = _flight_call_options(
            connection,
            flight,
            request_id,
            streaming=True,
            deadline=deadline,
        )
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
    timeout_config = connection.client_config.timeout_config
    overall_timeout = timeout_config.timeout if timeout_config is not None else None
    deadline = (
        time.monotonic() + overall_timeout.total_seconds()
        if overall_timeout is not None
        else None
    )
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
            descriptor,
            _flight_call_options(connection, flight, request_id, deadline=deadline),
        )
        if not info.endpoints:
            return pa.Table.from_batches([], schema=info.schema)

        tables = [
            _read_endpoint(
                endpoint,
                client,
                connection,
                flight,
                request_id,
                resolved_uri,
                deadline,
            )
            for endpoint in info.endpoints
        ]
        if len(tables) == 1:
            return tables[0]
        return pa.concat_tables(tables)
    except Exception as err:
        message = f"Flight SQL request {request_id} failed: {err}"
        try:
            err.args = (message, *err.args[1:])
        except Exception:
            raise RuntimeError(message) from err
        raise
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
        Database-prefix paths are not supported by Flight SQL.
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
        Remote client configuration. Static and dynamic headers, timeouts, and
        TLS files are also applied to Flight SQL where supported. Flight SQL
        uses a 300-second planning timeout when neither an overall nor read
        timeout is set. Only an overall timeout caps result streaming.
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

    database_uri = str(database)
    parsed_database = urlparse(database_uri)
    if not database_uri.startswith("db://") or parsed_database.netloc == "":
        raise ValueError("lancedb.sql requires a remote db:// database")
    try:
        database_port = parsed_database.port
    except ValueError as err:
        raise ValueError(f"Invalid database URI port: {err}") from err
    if (
        parsed_database.username is not None
        or parsed_database.password is not None
        or database_port is not None
    ):
        raise ValueError("lancedb.sql database URI must contain only a database name")
    if (
        parsed_database.path not in ("", "/")
        or parsed_database.params
        or parsed_database.query
        or parsed_database.fragment
    ):
        raise ValueError("lancedb.sql does not support database URI prefixes")

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
