# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
"""Internal, versioned StreamingDataset checkpoint validation."""

from copy import deepcopy
import hashlib
from pathlib import Path
from urllib.parse import urlsplit, unquote

import pyarrow as pa


def integer(value, field, minimum=0):
    if type(value) is not int or (minimum is not None and value < minimum):
        raise ValueError(f"{field} must be an integer >= {minimum}")
    return value


def identity(value, field):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a nonempty, non-secret identity string")
    return value


def native_locator(table):
    """Bind ordinary local native handles, without connection credentials.

    Remote, namespace and memory handles do not expose a uniform durable locator.
    They retain the explicitly caller-asserted identity boundary.
    """
    from .table import LanceTable

    if not isinstance(table, LanceTable) or table._namespace_client is not None:
        return None
    location = table._location or table._conn.uri
    parsed = urlsplit(location)
    if parsed.scheme not in ("", "file") or parsed.query or parsed.fragment:
        return None
    if parsed.netloc not in ("", "localhost"):
        return None
    path = unquote(parsed.path) if parsed.scheme == "file" else location
    return {
        "path": str(Path(path).resolve()),
        "namespace": list(table._namespace_path),
        "table": table.name,
        "kind": "table_location" if table._location else "database_location",
    }


def schema_digest(schema):
    # IPC schema encoding is versioned by this identifier; metadata is sorted.
    # Arrow types retain nested field metadata, which is conservatively significant.
    fields = [f.with_metadata(dict(sorted((f.metadata or {}).items()))) for f in schema]
    canonical = pa.schema(
        fields, metadata=dict(sorted((schema.metadata or {}).items()))
    )
    return (
        "arrow-ipc-v1:" + hashlib.sha256(canonical.serialize().to_pybytes()).hexdigest()
    )


def legacy_provenance():
    return {"mode": "legacy", "guarantee": "unverified"}


def envelope(provenance):
    return {"checkpoint_schema_version": 1, "provenance": deepcopy(provenance)}


def provenance(state):
    version = state.get("checkpoint_schema_version")
    if "checkpoint_schema_version" not in state:
        if "provenance" in state:
            raise ValueError("provenance requires checkpoint_schema_version")
        return legacy_provenance()
    if type(version) is not int or version != 1:
        raise ValueError("unsupported checkpoint_schema_version; expected 1")
    p = state.get("provenance")
    if not isinstance(p, dict):
        raise ValueError("provenance must be a descriptor")
    if p == legacy_provenance():
        return deepcopy(p)
    if p.get("mode") not in ("snapshot", "logical_reference"):
        raise ValueError("invalid provenance mode")
    if p.get("guarantee") != "caller_asserted":
        raise ValueError("provenance guarantee must be caller_asserted")
    identity(p.get("dataset_identity"), "dataset_identity")
    snapshot = p.get("snapshot")
    if not isinstance(snapshot, dict):
        raise ValueError("provenance snapshot must contain branch and version")
    identity(snapshot.get("branch"), "snapshot branch")
    integer(snapshot.get("version"), "snapshot version", 1)
    if p["mode"] == "logical_reference":
        identity(p.get("logical_revision"), "logical_revision")
    config = p.get("config")
    if not isinstance(config, dict):
        raise ValueError("provenance config must be a descriptor")
    required = {
        "filter",
        "columns",
        "schema",
        "plan",
        "plan_identity",
        "seed",
        "epoch",
        "clump_size",
        "num_splits",
        "policy_id",
        "transform",
        "transform_batch_size",
        "error_policy",
        "pack_sequences",
        "eos_id",
        "pad_id",
        "blocks_per_epoch",
    }
    if set(config) != required:
        raise ValueError("provenance config fields do not match schema version 1")
    identity(config["schema"], "schema fingerprint")
    integer(config["num_splits"], "config num_splits", 1)
    if config["plan"] not in ("random", "sequential", "custom"):
        raise ValueError("invalid provenance plan")
    if config["plan"] == "custom":
        if config["plan_identity"] is not None:
            raise ValueError("custom plans use the existing permutation fingerprint")
    else:
        digest = config["plan_identity"]
        prefix = "logical-occurrences-v1:"
        if (
            not isinstance(digest, str)
            or not digest.startswith(prefix)
            or len(digest) != len(prefix) + 64
            or any(c not in "0123456789abcdef" for c in digest[len(prefix) :])
        ):
            raise ValueError("ordinary checkpoint requires a completed plan identity")
    return deepcopy(p)


def compatible(left, right):
    # Snapshot coordinates remain diagnostic in logical-reference mode.
    a, b = left, right
    if a.get("mode") == b.get("mode") == "logical_reference":
        ignored = {"snapshot", "native_locator"}
        a = {k: v for k, v in a.items() if k not in ignored}
        b = {k: v for k, v in b.items() if k not in ignored}
    if a != b:
        # Report schema-defined field names, never identity/filter/policy values.
        # Unexpected dictionary keys are not safe diagnostic text either.
        nested_fields = {
            "config": {
                "filter",
                "columns",
                "schema",
                "plan",
                "plan_identity",
                "seed",
                "epoch",
                "clump_size",
                "num_splits",
                "policy_id",
                "transform",
                "transform_batch_size",
                "error_policy",
                "pack_sequences",
                "eos_id",
                "pad_id",
                "blocks_per_epoch",
            },
            "snapshot": {"branch", "version"},
            "native_locator": {"path", "namespace", "table", "kind"},
        }
        fields = {
            "mode",
            "guarantee",
            "dataset_identity",
            "logical_revision",
            *nested_fields,
        }
        differences = []
        for key in sorted(fields):
            left_value, right_value = a.get(key), b.get(key)
            if left_value == right_value:
                continue
            allowed = nested_fields.get(key)
            if (
                allowed is not None
                and isinstance(left_value, dict)
                and isinstance(right_value, dict)
                and set(left_value) | set(right_value) <= allowed
            ):
                differences.extend(
                    f"{key}.{field}"
                    for field in sorted(allowed)
                    if left_value.get(field) != right_value.get(field)
                )
            else:
                differences.append(key)
        raise ValueError(
            "checkpoint provenance mismatch: "
            + ", ".join(differences or ["descriptor fields"])
        )


def validate(state):
    """Return a detached normalized payload without changing caller/live state."""
    if not isinstance(state, dict):
        raise ValueError("checkpoint must be a dictionary")
    s = deepcopy(state)
    p = provenance(s)
    n = integer(s.get("num_splits"), "num_splits", 1)
    integer(s.get("shuffle_seed"), "shuffle_seed", None)
    integer(s.get("epoch"), "epoch", None)

    def counts(key, fallback=None):
        values = s.get(key, fallback)
        if type(values) is int:
            values = [values] * n
        if not isinstance(values, list) or len(values) != n:
            raise ValueError(f"{key} must contain one entry per logical split")
        return [integer(v, key) for v in values]

    samples = counts("samples_consumed_per_split")
    s["samples_consumed_per_split"] = samples
    if "pack_buffers" in s:
        integer(s.get("pack_sequences"), "pack_sequences", 1)
        emitted = counts("blocks_emitted_per_split")
        limit = s.get("blocks_per_epoch")
        if limit is not None:
            integer(limit, "blocks_per_epoch", 1)
            if limit % n or any(v > limit // n for v in emitted):
                raise ValueError("blocks_emitted_per_split exceeds blocks_per_epoch")
        for key in ("eos_id", "pad_id"):
            if s.get(key) is not None:
                integer(s[key], key, None)
        buffers = s["pack_buffers"]
        if not isinstance(buffers, dict):
            raise ValueError("pack_buffers must be a dictionary")
        normalized = {}
        for raw_split, buf in buffers.items():
            if type(raw_split) is int:
                split = raw_split
            elif isinstance(raw_split, str) and raw_split.isdecimal():
                split = int(raw_split)
            else:
                raise ValueError("pack_buffers keys must be split indexes")
            if split < 0 or split >= n or split in normalized:
                raise ValueError("pack_buffers contains invalid or duplicate split")
            if not isinstance(buf, dict) or set(buf) != {"tokens", "starts"}:
                raise ValueError("pack_buffers entries require tokens and starts")
            tokens, starts = buf["tokens"], buf["starts"]
            if not isinstance(tokens, list) or not isinstance(starts, list):
                raise ValueError("pack buffer tokens and starts must be lists")
            for token in tokens:
                integer(token, "buffer token", None)
            for start in starts:
                integer(start, "buffer start")
            if starts != sorted(set(starts)) or any(v >= len(tokens) for v in starts):
                raise ValueError(
                    "pack buffer starts must be ordered offsets into tokens"
                )
            # Buffers can contain more than one block immediately after reading a row.
            normalized[split] = {"tokens": tokens, "starts": starts}
        s["blocks_emitted_per_split"] = emitted
        s["pack_buffers"] = normalized
    else:
        positions = counts("positions_consumed_per_split", samples)
        if any(a > b for a, b in zip(samples, positions)):
            raise ValueError("samples consumed cannot exceed permutation positions")
        s["positions_consumed_per_split"] = positions
    if "perm_fingerprint" in s:
        identity(s["perm_fingerprint"], "perm_fingerprint")
    if p["mode"] != "legacy":
        c = p["config"]
        for key in (
            "num_splits",
            "pack_sequences",
            "eos_id",
            "pad_id",
            "blocks_per_epoch",
        ):
            if c[key] != s.get(key):
                raise ValueError(f"provenance config disagrees with payload {key}")
        if c["plan"] != "custom" and c["epoch"] != s["epoch"]:
            raise ValueError("provenance config disagrees with payload epoch")
        if c["plan"] == "random" and c["seed"] != s["shuffle_seed"]:
            raise ValueError("provenance config disagrees with payload shuffle_seed")
        if (c["plan"] == "custom") != ("perm_fingerprint" in s):
            raise ValueError("custom plan requires a permutation fingerprint")
    s.update(envelope(p))
    return s
