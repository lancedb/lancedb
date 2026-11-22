# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Parent-side admission to an Enterprise ordered prewarm job."""

from dataclasses import dataclass
from typing import Optional

import torch.distributed as dist

from .remote.table import RemoteTable


@dataclass
class _Recipe:
    target: tuple
    columns: Optional[list[str]]
    predicate: Optional[str]
    settings: dict

    def submit(self, table, positions: list[int]) -> str:
        settings = dict(self.settings)
        if any(positions):
            settings["split_start_positions"] = positions
        return table._submit_streaming_prewarm(self.columns, self.predicate, settings)


def prepare(dataset, state, workers_per_rank, slices_per_worker):
    table = dataset._table
    if not isinstance(table, RemoteTable):
        raise ValueError("Ordered prewarm requires a LanceDB Enterprise table")
    if dataset._uses_split_strategy:
        raise ValueError("Ordered prewarm requires the built-in streaming sampler")
    if dataset._worker_info_override is not None:
        raise ValueError(
            "Ordered prewarm requires the actual DataLoader worker topology"
        )
    if dataset._num_splits % (dataset._world_size * workers_per_rank):
        raise ValueError(
            "num_splits must be divisible by world_size × workers_per_rank"
        )
    if slices_per_worker is not None and (
        type(slices_per_worker) is not int or slices_per_worker <= 0
    ):
        raise ValueError("slices_per_worker must be a positive integer")
    metadata = dataset._perm_table.schema.metadata or {}
    if b"base_version" not in metadata:
        raise ValueError("Ordered prewarm requires a snapshot-pinned permutation")
    branch = metadata.get(b"base_branch")
    branch = branch.decode() if branch is not None else None
    if branch != table.current_branch():
        raise ValueError("Permutation branch differs from the prewarm table branch")
    settings = dict(
        table_version=int(metadata[b"base_version"]),
        num_splits=dataset._num_splits,
        world_size=dataset._world_size,
        workers_per_rank=workers_per_rank,
        slices_per_worker=slices_per_worker,
        shuffle=dataset._shuffle,
        shuffle_seed=dataset._shuffle_seed,
        epoch=dataset._epoch,
        shuffle_clump_size=dataset._shuffle_clump_size if dataset._shuffle else None,
    )
    recipe = _Recipe(
        (table.db_name, tuple(table.namespace), table.name, branch),
        dataset._columns,
        dataset._filter,
        settings,
    )
    positions = state.get(
        "positions_consumed_per_split", state["samples_consumed_per_split"]
    )
    return recipe, [positions[i] for i in dataset._rank_splits]


def submit(dataset, prepared, failure: Optional[Exception]) -> str:
    """Agree on the snapshot and global resume vector before rank zero submits."""
    world = (
        dist.get_world_size() if dist.is_available() and dist.is_initialized() else 1
    )
    if world == 1:
        if failure is not None:
            raise failure
        if dataset._world_size != 1 or dataset._rank != 0:
            raise ValueError(
                "Multi-rank prewarm requires an initialized torch.distributed group"
            )
        recipe, positions = prepared
        return recipe.submit(dataset._table, positions)

    rank = dist.get_rank()
    error = str(failure) if failure is not None else None
    if (dataset._rank, dataset._world_size) != (rank, world):
        error = "Dataset rank and world_size must match the torch.distributed group"
    reports = [None] * world
    dist.all_gather_object(reports, (error, prepared))
    for peer, (error, _) in enumerate(reports):
        if error is not None:
            raise RuntimeError(
                f"Rank {peer} could not prepare ordered prewarm: {error}"
            )
    recipe, _ = prepared
    if any(other_recipe != recipe for _, (other_recipe, _) in reports):
        raise ValueError(
            "Ranks disagree on the prewarm table, snapshot, or loader settings"
        )

    receipt = [None]
    if rank == 0:
        positions = [p for _, (_, local_positions) in reports for p in local_positions]
        try:
            receipt[0] = (recipe.submit(dataset._table, positions), None)
        except Exception as exc:
            receipt[0] = (None, str(exc))
    dist.broadcast_object_list(receipt, src=0)
    job_id, error = receipt[0]
    if error is not None:
        raise RuntimeError(f"Enterprise ordered prewarm submission failed: {error}")
    return job_id
