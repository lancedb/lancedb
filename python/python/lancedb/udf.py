# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
"""UDF authoring for LanceDB derived compute (server-backed).

`@udf` / `@table_udf` turn a plain Python function into a registrable
server-side UDF: a cloudpickled (or source) body, a SQL signature inferred
from type hints, and the runtime options (pip deps, GPUs, batching, ...).
Register and use them through the existing connection/table API:

    import lancedb
    from lancedb import udf, table_udf

    db = lancedb.connect("db://my_db", api_key="...", host_override="...")

    @udf(pip=["torch>=2.0"], num_gpus=1)
    def embed(text: str) -> list[float]:
        return model.encode(text).tolist()

    db.create_function(embed)                 # CREATE FUNCTION (once)
    tbl = db.open_table("docs")
    tbl.add_columns(computed={"vec": embed("text")})  # bind embed(text) -> vec
    tbl.refresh_column("vec").wait()          # materialize (returns a JobHandle)
    view = db.create_materialized_view("chunks", tbl, ["id", chunk_fn])

`embed("text")` applies the registered function to the `text` column and yields
the expression `embed(text)`; the function itself stays decoupled from any
column, so the same `embed` works on any column or table.

These operations are server-backed (LanceDB Enterprise / Cloud); the
decorator itself works locally (define + call), only registration needs a
remote connection.
"""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import functools
import inspect
import re
import sys
import textwrap
import time
import typing

# -- type hints -> SQL type strings -------------------------------------

_SCALARS = {
    int: "BIGINT",
    # Pragmatic default for ML workloads: python float maps to FLOAT
    # (Float32). Use an explicit `returns=` for DOUBLE.
    float: "FLOAT",
    str: "VARCHAR",
    bool: "BOOLEAN",
    bytes: "BLOB",
}


class TypeInferenceError(TypeError):
    pass


def sql_type(hint) -> str:
    """SQL type string for a python type hint."""
    if hint in _SCALARS:
        return _SCALARS[hint]
    origin = typing.get_origin(hint)
    if origin in (list, typing.List):
        (item,) = typing.get_args(hint) or (None,)
        if item in _SCALARS:
            return f"{_SCALARS[item]}[]"
        raise TypeInferenceError(
            f"unsupported list item type {item!r}; use an explicit returns="
        )
    fields = _struct_fields(hint)
    if fields is not None:
        inner = ", ".join(f"{name} {sql_type(h)}" for name, h in fields)
        return f"STRUCT({inner})"
    raise TypeInferenceError(
        f"cannot infer a SQL type for {hint!r}; pass an explicit type string"
    )


def _struct_fields(hint):
    """(name, hint) pairs for a TypedDict or dataclass, else None."""
    if dataclasses.is_dataclass(hint):
        return [(f.name, f.type) for f in dataclasses.fields(hint)]
    # TypedDict detection: a dict subclass with __annotations__.
    if (
        isinstance(hint, type)
        and issubclass(hint, dict)
        and typing.get_type_hints(hint)
    ):
        return list(typing.get_type_hints(hint).items())
    return None


def return_type(fn, override: "str | None", table: bool) -> str:
    """SQL return type for a function: explicit override wins, else the
    return annotation. Table functions render as TABLE(...) and accept
    struct-shaped hints (TypedDict/dataclass, optionally list-wrapped)."""
    if override is not None:
        s = override.strip()
        if table and not s.upper().startswith("TABLE"):
            if s.upper().startswith("STRUCT"):
                return "TABLE" + s[len("STRUCT") :]
            raise TypeInferenceError(
                "a table function's returns= must be TABLE(...) or STRUCT(...)"
            )
        return s

    hints = typing.get_type_hints(fn)
    ret = hints.get("return")
    if ret is None:
        raise TypeInferenceError(
            f"function {fn.__name__!r} needs a return annotation or returns="
        )
    if table:
        # Accept list[Row] / Row where Row is a TypedDict or dataclass.
        if typing.get_origin(ret) in (list, typing.List):
            (ret,) = typing.get_args(ret)
        fields = _struct_fields(ret)
        if fields is None:
            raise TypeInferenceError(
                "a table function must return rows shaped as a TypedDict or "
                "dataclass (optionally list-wrapped); or pass returns=..."
            )
        inner = ", ".join(f"{name} {sql_type(h)}" for name, h in fields)
        return f"TABLE({inner})"
    return sql_type(ret)


def param_types(fn) -> "list[tuple[str, str]]":
    """(name, sql type) per parameter, from annotations. Each UDF
    parameter binds to a source column of the same name by default."""
    hints = typing.get_type_hints(fn)
    out = []
    for name, p in inspect.signature(fn).parameters.items():
        if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            raise TypeInferenceError("*args/**kwargs are not supported in UDFs")
        hint = hints.get(name)
        if hint is None:
            raise TypeInferenceError(
                f"parameter {name!r} of {fn.__name__!r} needs a type annotation"
            )
        out.append((name, sql_type(hint)))
    return out


# -- column expressions -------------------------------------------------


class ColumnExpr(str):
    """A computed-column expression produced by applying a registered
    function to column names, e.g. ``embed("data") -> "embed(data)"``.

    It IS the expression string everywhere a string is expected (views, SQL,
    logging), and additionally carries the function's declared return type so
    ``add_columns(computed=...)`` can declare the column without a hand-written
    type. ``field_types`` holds the per-field SQL types of a STRUCT return, for
    fanning one expression out to several columns.
    """

    data_type: "str | None"
    field_types: "list[str] | None"

    def __new__(cls, expr: str, data_type=None, field_types=None):
        obj = super().__new__(cls, expr)
        obj.data_type = data_type
        obj.field_types = field_types
        return obj


def _normalize_computed(computed: dict) -> dict:
    """Normalize the user-facing ``computed=`` mapping to the canonical
    ``{name: (sql_type, expression)}`` form.

    Accepts, per entry:
      - value is a `ColumnExpr` (from ``fn("col")``): the column's SQL type
        comes from the function's return type -- no hand-written type needed. A
        tuple key (``("chunk", "idx")``) fans a STRUCT return out to one
        (type, expression) entry per field, in declared order.
      - value is a legacy ``(sql_type, expression)`` tuple: passed through (the
        escape hatch, e.g. bare-name function strings).
    """
    out: dict = {}
    for key, val in computed.items():
        if isinstance(val, ColumnExpr):
            expr = str(val)
            if isinstance(key, (tuple, list)):
                if not val.field_types:
                    raise ValueError(
                        f"columns {tuple(key)} need a STRUCT-returning function; "
                        f"{expr} returns a single value"
                    )
                if len(val.field_types) != len(key):
                    raise ValueError(
                        f"{len(key)} columns but {len(val.field_types)} struct fields "
                        f"in {expr}"
                    )
                for name, t in zip(key, val.field_types):
                    out[name] = (t, expr)
            else:
                if val.data_type is None:
                    raise ValueError(f"cannot infer a type for {expr}; pass types=")
                out[key] = (val.data_type, expr)
        else:
            out[key] = val
    return out


# -- the @udf / @table_udf decorators -----------------------------------


class Udf:
    def __init__(
        self,
        fn,
        *,
        returns: "str | None" = None,
        table: bool = False,
        name: "str | None" = None,
        pip: "list[str] | None" = None,
        pip_index_url: "str | None" = None,
        pip_extra_index_urls: "list[str] | None" = None,
        find_links: "list[str] | None" = None,
        requirements: "str | list[str] | None" = None,
        conda: "list[str] | None" = None,
        conda_channels: "list[str] | None" = None,
        env: "dict[str, str] | list[str] | None" = None,
        num_cpus: "int | None" = None,
        num_gpus: "int | None" = None,
        batch_size: "int | None" = None,
        timeout: "float | None" = None,
        error_policy: "str | None" = None,
        max_skip_ratio: "float | None" = None,
        retries: "int | None" = None,
        docker_image: "str | None" = None,
        description: "str | None" = None,
        prefer_source: bool = False,
    ):
        functools.update_wrapper(self, fn)
        self.fn = fn
        self.name = name or fn.__name__
        self.table = table
        self.params = param_types(fn)
        self.returns = return_type(fn, returns, table)
        self.prefer_source = prefer_source
        self.options: "dict[str, str]" = {}
        if conda and (pip or requirements):
            raise ValueError("pass conda or pip/requirements, not both")
        if conda_channels and not conda:
            raise ValueError("conda_channels requires conda")
        if pip:
            self.options["pip"] = ",".join(pip)
        if pip_extra_index_urls:
            self.options["pip_extra_index_urls"] = ",".join(pip_extra_index_urls)
        if find_links:
            self.options["find_links"] = ",".join(find_links)
        if requirements:
            self.options["requirements"] = _format_requirements(requirements)
        if conda:
            self.options["conda"] = ",".join(conda)
        if conda_channels:
            self.options["conda_channels"] = ",".join(conda_channels)
        if env:
            self.options["env"] = _format_env(env)
        for key, val in [
            ("pip_index_url", pip_index_url),
            ("num_cpus", num_cpus),
            ("num_gpus", num_gpus),
            ("batch_size", batch_size),
            ("timeout", timeout),
            ("error_policy", error_policy),
            ("max_skip_ratio", max_skip_ratio),
            ("retries", retries),
            ("docker_image", docker_image),
        ]:
            if val is not None:
                self.options[key] = str(val)
        # Keep the source in the description (when available) so the
        # catalog stays inspectable even for pickled bodies.
        if description is not None:
            self.options["description"] = description
        else:
            try:
                self.options["description"] = textwrap.dedent(inspect.getsource(fn))
            except (OSError, TypeError):
                pass

    def __call__(self, *args, **kwargs):
        """Call with real values to run locally; call with column-name
        strings to build an expression for backfills and views, e.g.
        ``embed("data")`` -> the expression ``embed(data)`` (a `ColumnExpr`
        carrying the function's return type for `add_columns(computed=...)`)."""
        if args and all(isinstance(a, str) for a in args) and not kwargs:
            return self.expression(*args)
        return self.fn(*args, **kwargs)

    def expression(self, *columns: str) -> ColumnExpr:
        """The expression applying this function to `columns` (default: the
        function's own parameter names). Returns a `ColumnExpr` -- a string
        that also carries the declared return type (and struct field types)."""
        cols = columns or [p for p, _ in self.params]
        expr = f"{self.name}({', '.join(cols)})"
        field_types = None
        if self.returns.upper().startswith("STRUCT"):
            field_types = struct_field_types(self.returns)
        return ColumnExpr(expr, data_type=self.returns, field_types=field_types)

    def _body(self) -> "tuple[str, str]":
        """(body literal, body_format). Source when requested and
        retrievable; cloudpickle otherwise (handles closures)."""
        if self.prefer_source:
            try:
                src = textwrap.dedent(inspect.getsource(self.fn))
                # Strip the decorator line(s) so the stored body is a
                # plain function definition.
                lines = src.splitlines(keepends=True)
                while lines and lines[0].lstrip().startswith("@"):
                    lines.pop(0)
                return "".join(lines), "source"
            except (OSError, TypeError):
                pass
        import cloudpickle

        raw = cloudpickle.dumps(self.fn)
        return base64.b64encode(raw).decode("ascii"), "cloudpickle"

    def _body_and_options(self) -> "tuple[str, dict[str, str]]":
        """The body literal plus the finalized options (body_format /
        python_version / cloudpickle-pip bookkeeping for a non-source
        body)."""
        body, body_format = self._body()
        options = dict(self.options)
        if body_format != "source":
            options["body_format"] = body_format
            # Pickled code objects only load under the same interpreter
            # minor version; record ours so the worker can fail with a
            # clear message instead of a bytecode error.
            options["python_version"] = self.pickle_environment()
            # The worker deserializes the body with cloudpickle; make sure
            # the job's pip environment provides it. Conda bakes inject
            # cloudpickle server-side, so do not create an invalid pip+conda
            # declaration here.
            if "conda" not in options:
                pip = [d for d in options.get("pip", "").split(",") if d]
                if not any(d.startswith("cloudpickle") for d in pip):
                    pip.append("cloudpickle")
                options["pip"] = ",".join(pip)
        return body, options

    def create_request(self) -> dict:
        """Keyword arguments for `connection.create_function`."""
        body, options = self._body_and_options()
        return {
            "name": self.name,
            "language": "python",
            "return_type": self.returns,
            "body": body,
            "options": options,
        }

    def create_statement(self) -> str:
        """The equivalent `CREATE FUNCTION` SQL (for SQL-surface callers)."""
        params = ", ".join(f"{n} {t}" for n, t in self.params)
        body, options = self._body_and_options()
        with_clause = ""
        if options:
            rendered = ", ".join(
                f"{k} = '{_escape(v)}'" for k, v in sorted(options.items())
            )
            with_clause = f" WITH ({rendered})"
        return (
            f"CREATE FUNCTION {self.name}({params}) RETURNS {self.returns} "
            f"LANGUAGE python AS '{_escape_body(body)}'{with_clause}"
        )

    def pickle_environment(self) -> str:
        """Python version the body pickles under -- workers should match
        the minor version for cloudpickle compatibility."""
        return f"{sys.version_info.major}.{sys.version_info.minor}"


def _escape(s: str) -> str:
    return str(s).replace("'", "''")


def _format_requirements(requirements: "str | list[str]") -> str:
    if isinstance(requirements, str):
        return requirements
    return "\n".join(str(req) for req in requirements)


def _format_env(env: "dict[str, str] | list[str]") -> str:
    if isinstance(env, dict):
        return "; ".join(f"{key}={value}" for key, value in env.items())
    return "; ".join(str(entry) for entry in env)


def _escape_body(body: str) -> str:
    # The server unescapes \n / \t in single-quoted bodies; encode real
    # newlines accordingly and escape quotes.
    return (
        body.replace("\\", "\\\\")
        .replace("'", "''")
        .replace("\n", "\\n")
        .replace("\t", "\\t")
    )


def udf(fn=None, **kwargs):
    """Decorate a function as a scalar (or struct-returning) UDF.

    @udf
    def doubled(val: int) -> float: ...

    @udf(pip=["torch>=2"], num_gpus=1)
    def embed(body: str) -> list[float]: ...
    """
    if fn is not None:
        return Udf(fn, **kwargs)
    return lambda f: Udf(f, **kwargs)


def table_udf(fn=None, **kwargs):
    """Decorate a table function (UDTF): each input row may emit zero or
    more output rows. Only usable in materialized views.

        class Chunk(TypedDict):
            chunk: str
            chunk_idx: int

        @table_udf
        def chunker(body: str) -> list[Chunk]: ...
    """
    kwargs["table"] = True
    if fn is not None:
        return Udf(fn, **kwargs)
    return lambda f: Udf(f, **kwargs)


# -- view / job handles (thin references over a connection) -------------


def struct_field_types(returns: str) -> "list[str]":
    """Field type strings of a STRUCT(...) SQL type, in declared order."""
    inner = returns.strip()[len("STRUCT(") : -1]
    fields, depth, start = [], 0, 0
    for i, c in enumerate(inner):
        if c in "([":
            depth += 1
        elif c in ")]":
            depth -= 1
        elif c == "," and depth == 0:
            fields.append(inner[start:i].strip())
            start = i + 1
    fields.append(inner[start:].strip())
    # Each field is "name TYPE"; drop the name.
    return [f.split(None, 1)[1] for f in fields]


def build_view_query(source, select) -> str:
    """Assemble a view SELECT from a source (name or table) and select
    items: a column name, an expression string, a (alias, expression)
    tuple, or a @udf/@table_udf object."""
    src = source.name if hasattr(source, "name") else source
    items = []
    for item in select:
        if isinstance(item, Udf):
            items.append(item.expression())
        elif isinstance(item, tuple):
            alias, expr = item
            expr = expr.expression() if isinstance(expr, Udf) else expr
            items.append(f"{expr} AS {alias}")
        else:
            items.append(item)
    return f"SELECT {', '.join(items)} FROM {src}"


def _job_id_matches(handle_id: str, listed_id: str) -> bool:
    # The refresh/backfill endpoints return the submission id (a uuid), but
    # the agent names the manifest job "<table>-<type>-<first 8 of the
    # submission id>" -- which is what list_jobs and cancel report. Match the
    # canonical id directly, or by that submission prefix.
    if listed_id == handle_id:
        return True
    prefix = handle_id[:8]
    return len(prefix) >= 4 and prefix in listed_id


class MaterializedView:
    """A reference to a materialized view (name + connection). Operations are
    server-backed connection calls bound to the name.

    ``create_materialized_view`` returns one of these; ``job_id`` is the
    initial-population job (None when the view was created with no data), so
    ``db.create_materialized_view(...).wait()`` blocks until it is populated.
    """

    def __init__(self, conn, name: str, job_id: "str | None" = None):
        self.conn = conn
        self.name = name
        #: initial-population job id from create, or None (with_no_data).
        self.job_id = job_id

    def wait(self, timeout: float = 3600.0, poll: float = 2.0) -> str:
        """Block until the initial-population job (from create) finishes.
        A no-op when the view was created with no data."""
        if self.job_id is None:
            return "finished"
        return JobHandle(self.conn, self.job_id, table=self.name).wait(
            timeout=timeout, poll=poll
        )

    def refresh(self, full: bool = False) -> "JobHandle":
        """Refresh the materialized view; returns a `JobHandle` to wait on,
        poll, or cancel (``view.refresh().wait()``).

        ``full=True`` forces a full rebuild (recompute and replace every row)
        instead of the default incremental refresh. A full rebuild preserves
        the view's indexes -- they are reindexed by the distributed indexer.
        """
        job_id = self.conn._refresh_materialized_view(self.name, full=full)
        return JobHandle(self.conn, job_id, table=self.name)

    def explain_refresh(self, full: bool = False):
        """Plan a refresh without running it (EXPLAIN REFRESH)."""
        return self.conn.explain_refresh_materialized_view(self.name, full=full)

    def alter(self, auto_refresh: bool) -> None:
        self.conn.alter_materialized_view(self.name, auto_refresh=auto_refresh)

    def drop(self) -> None:
        self.conn.drop_materialized_view(self.name)

    # A materialized view is a first-class table: it can be indexed and
    # searched like any other. These open the materialized dataset by name and
    # delegate. Indexes declared this way are recorded against the view, so the
    # engine re-applies them after a full refresh rebuilds the dataset (a full
    # refresh overwrites the dataset, which would otherwise drop its indices).
    def _table(self):
        return self.conn.open_table(self.name)

    def create_index(self, *args, **kwargs):
        """Build an index on the materialized view (see Table.create_index)."""
        return self._table().create_index(*args, **kwargs)

    def create_scalar_index(self, *args, **kwargs):
        """Build a scalar index on the materialized view."""
        return self._table().create_scalar_index(*args, **kwargs)

    def create_fts_index(self, *args, **kwargs):
        """Build a full-text-search index on the materialized view."""
        return self._table().create_fts_index(*args, **kwargs)

    def search(self, *args, **kwargs):
        """Search the materialized view (vector / FTS / hybrid)."""
        return self._table().search(*args, **kwargs)

    def lineage(self, column=None, *, direction=None, depth=None):
        """Lineage of the materialized view (or one of its columns). Delegates
        to the backing table; the server already includes the view's sources
        and downstream dependents. Returns a `Lineage`."""
        return self._table().lineage(column, direction=direction, depth=depth)


_PROGRESS = re.compile(r"(\d+)/(\d+)")


class JobFailedError(RuntimeError):
    """Raised by ``JobHandle.wait()`` when the server reports the job ``failed``.

    Carries the server-side error so a doomed backfill (e.g. a multi-column
    ``REFRESH COLUMN`` of a scalar UDF) surfaces its real cause promptly,
    instead of the caller blocking until ``wait()``'s timeout.
    """

    def __init__(self, job_id: str, error: "str | None"):
        self.job_id = job_id
        self.error = error
        super().__init__(f"job {job_id} failed: {error or 'unknown error'}")


class JobHandle:
    """A reference to an inflight server-side job, with polling helpers."""

    #: How long an unseen job is treated as still materializing (submission
    #: -> agent cycle -> manifest write is async).
    GRACE_SECONDS = 20.0

    def __init__(self, conn, job_id: str, table: "str | None" = None):
        self.conn = conn
        self.id = job_id
        #: The job's table, when known (refresh_column / MV refresh). Lets the
        #: server resolve this job with an O(1) single-node read; without it the
        #: lookup scans the database's active jobs (still correct).
        self.table = table
        self._created = time.monotonic()
        self._seen = False

    def _job(self):
        # Poll by id (one job), not list_jobs (every active job): the server
        # matches the submission/manifest id and reads just this table's node.
        return self.conn.get_job(self.id, self.table)

    def status(self) -> str:
        """pending / running / cancelling / stale, or 'finished' once the
        job has left the inflight listing."""
        job = self._job()
        if job is not None:
            self._seen = True
            return job.state
        if not self._seen and time.monotonic() - self._created < self.GRACE_SECONDS:
            return "pending"
        return "finished"

    def progress(self) -> "tuple[int, int] | None":
        """(units_done, units_total) while running, else None."""
        job = self._job()
        if job is not None and job.units_total is not None:
            return job.units_done or 0, job.units_total
        return None

    def wait(self, timeout: float = 3600.0, poll: float = 2.0) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = self.status()
            if state in ("finished", "stale"):
                return state
            if state == "failed":
                # Terminal failure -- surface the server error now, don't block
                # until `timeout`. `finalize` wrote it to the job's status node.
                job = self._job()
                raise JobFailedError(self.id, job.error if job is not None else None)
            if state == "pending":
                time.sleep(min(poll, 0.5))
                continue
            job = self._job()
            if job is not None and job.committed:
                return "finished"
            time.sleep(poll)
        raise TimeoutError(f"job {self.id} still {self.status()} after {timeout}s")

    def cancel(self) -> None:
        # Cancel by the canonical manifest id (what cancel matches), found
        # via the submission prefix; fall back to the raw id.
        job = self._job()
        self.conn.cancel_job(job.job_id if job is not None else self.id)


class AsyncMaterializedView:
    """Async reference to a materialized view (name + async connection)."""

    def __init__(self, conn, name: str, job_id: "str | None" = None):
        self.conn = conn
        self.name = name
        #: initial-population job id from create, or None (with_no_data).
        self.job_id = job_id

    async def wait(self, timeout: float = 3600.0, poll: float = 2.0) -> str:
        """Block until the initial-population job (from create) finishes.
        A no-op when the view was created with no data."""
        if self.job_id is None:
            return "finished"
        return await AsyncJobHandle(self.conn, self.job_id, table=self.name).wait(
            timeout=timeout, poll=poll
        )

    async def refresh(self, full: bool = False) -> "AsyncJobHandle":
        """Refresh the materialized view; returns an `AsyncJobHandle` to wait
        on, poll, or cancel.

        ``full=True`` forces a full rebuild instead of an incremental refresh
        (indexes are preserved and reindexed by the distributed indexer).
        """
        job_id = await self.conn._refresh_materialized_view(self.name, full=full)
        return AsyncJobHandle(self.conn, job_id, table=self.name)

    async def explain_refresh(self, full: bool = False):
        return await self.conn.explain_refresh_materialized_view(self.name, full=full)

    async def alter(self, auto_refresh: bool) -> None:
        await self.conn.alter_materialized_view(self.name, auto_refresh=auto_refresh)

    async def drop(self) -> None:
        await self.conn.drop_materialized_view(self.name)

    async def lineage(self, column=None, *, direction=None, depth=None):
        """Lineage of the materialized view (or column). Returns a `Lineage`."""
        return await self.conn.lineage(
            self.name, column, direction=direction, depth=depth
        )


class AsyncJobHandle:
    """Async reference to an inflight server-side job, with polling helpers."""

    GRACE_SECONDS = 20.0

    def __init__(self, conn, job_id: str, table: "str | None" = None):
        self.conn = conn
        self.id = job_id
        #: See JobHandle.table -- enables an O(1) by-id lookup when known.
        self.table = table
        self._created = time.monotonic()
        self._seen = False

    async def _job(self):
        # Poll by id, not list_jobs (see JobHandle._job).
        return await self.conn.get_job(self.id, self.table)

    async def status(self) -> str:
        job = await self._job()
        if job is not None:
            self._seen = True
            return job.state
        if not self._seen and time.monotonic() - self._created < self.GRACE_SECONDS:
            return "pending"
        return "finished"

    async def progress(self) -> "tuple[int, int] | None":
        job = await self._job()
        if job is not None and job.units_total is not None:
            return job.units_done or 0, job.units_total
        return None

    async def wait(self, timeout: float = 3600.0, poll: float = 2.0) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = await self.status()
            if state in ("finished", "stale"):
                return state
            if state == "failed":
                # Terminal failure -- surface the server error now, don't block
                # until `timeout`. `finalize` wrote it to the job's status node.
                job = await self._job()
                raise JobFailedError(self.id, job.error if job is not None else None)
            if state == "pending":
                await asyncio.sleep(min(poll, 0.5))
                continue
            job = await self._job()
            if job is not None and job.committed:
                return "finished"
            await asyncio.sleep(poll)
        raise TimeoutError(
            f"job {self.id} still {await self.status()} after {timeout}s"
        )

    async def cancel(self) -> None:
        job = await self._job()
        await self.conn.cancel_job(job.job_id if job is not None else self.id)
