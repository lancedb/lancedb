# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pyarrow as pa
import math
import pytest

from lancedb import DBConnection, Session, Table, connect, connect_namespace
from lancedb.background_loop import LOOP
from lancedb.permutation import (
    DEFAULT_BATCH_SIZE,
    Permutation,
    Permutations,
    Transforms,
    permutation_builder,
)
from utils import (
    MockPermutationServer,
    assert_server_safe_row_id_requests,
    mock_remote_table,
)


def _read_permutation_in_spawned_process(payload: bytes, output) -> None:
    import pickle

    permutation = pickle.loads(payload)
    output.put(
        [
            row["id"]
            for row in permutation.__getitems__(list(range(permutation.num_rows)))
        ]
    )


def test_split_random_ratios(mem_db):
    """Test random splitting with ratios."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"x": range(100), "y": range(100)})
    )
    permutation_tbl = permutation_builder(tbl).split_random(ratios=[0.3, 0.7]).execute()

    # Check that the table was created and has data
    assert permutation_tbl.count_rows() == 100

    # Check that split_id column exists and has correct values
    data = permutation_tbl.search(None).to_arrow().to_pydict()
    split_ids = data["split_id"]
    assert set(split_ids) == {0, 1}

    # Check approximate split sizes (allowing for rounding)
    split_0_count = split_ids.count(0)
    split_1_count = split_ids.count(1)
    assert 25 <= split_0_count <= 35  # ~30% ± tolerance
    assert 65 <= split_1_count <= 75  # ~70% ± tolerance


def test_execute_does_not_reenter_background_loop(tmp_path, monkeypatch):
    import threading

    db = connect(tmp_path)
    tbl = db.create_table("test_table", pa.table({"x": range(10)}))
    original_run = LOOP.run

    def fail_on_reentry(future):
        assert threading.current_thread() is not LOOP.thread
        return original_run(future)

    monkeypatch.setattr(LOOP, "run", fail_on_reentry)

    permutation_tbl = permutation_builder(tbl).execute()

    assert permutation_tbl.count_rows() == 10
    assert permutation_tbl._conn.read_consistency_interval is None


def test_pickled_permutation_reads_pinned_version(tmp_path):
    """An unpickled copy must still read the pinned version, which also covers the
    version surviving the ``to_arrow()`` round trip in ``__getstate__``."""
    import pickle

    db = connect(tmp_path)
    tbl = db.create_table("base", pa.table({"idx": range(20)}))
    permutation_tbl = permutation_builder(tbl).execute()
    perm = Permutation.from_tables(tbl, permutation_tbl)

    payload = pickle.dumps(perm)

    # Compact so the stored row addresses no longer describe these rows at latest.
    tbl.delete("true")
    tbl.optimize()
    assert tbl.count_rows() == 0

    # Unpickle after the mutation: __setstate__ reopens at latest, so this only
    # passes if the recorded version is applied on reopen.
    restored = pickle.loads(payload)
    assert len(restored) == 20
    rows = restored.__getitems__(list(range(20)))
    assert sorted(row["idx"] for row in rows) == list(range(20))


def test_persist_plan_create_overwrite_namespace_and_metadata(tmp_path):
    import pickle

    db = connect(tmp_path)
    tbl = db.create_table("base", pa.table({"id": range(20)}))
    db.create_namespace(["plans"])
    namespace_path = ["plans"]

    plan = (
        permutation_builder(tbl)
        .split_sequential(fixed=2, split_names=["train", "test"])
        .persist(db, "splits", namespace_path=namespace_path)
        .execute()
    )
    assert plan.count_rows() == 20
    assert plan.namespace == namespace_path
    assert pickle.loads(pickle.dumps(plan)).namespace == namespace_path

    permutation = Permutation.from_tables(tbl, plan, split="train")
    assert permutation.base_version == tbl.version
    assert permutation.base_branch is None
    assert permutation.num_rows == 10

    with pytest.raises((ValueError, RuntimeError), match="already exists"):
        (
            permutation_builder(tbl)
            .split_sequential(fixed=2)
            .persist(db, "splits", namespace_path=namespace_path)
            .execute()
        )

    with pytest.raises(ValueError, match="Invalid mode"):
        permutation_builder(tbl).persist(db, "other", mode="replace")

    class RemoteLike:
        uri = "db://example"

    with pytest.raises(NotImplementedError, match="Enterprise"):
        permutation_builder(tbl).persist(RemoteLike(), "remote_splits")

    tbl.add(pa.table({"id": range(20, 40)}))
    plan = (
        permutation_builder(tbl)
        .split_sequential(fixed=2)
        .persist(db, "splits", namespace_path=namespace_path, mode="overwrite")
        .execute()
    )
    assert plan.count_rows() == 40
    assert db.open_table("splits", namespace_path=namespace_path).count_rows() == 40
    # The earlier permutation still reads the plan and base snapshot it was built from.
    assert permutation.num_rows == 10
    assert [row["id"] for row in permutation.__getitems__([0, 1])] == [0, 1]


def test_persist_namespace_connection_direct_result_pickle(tmp_path):
    import pickle

    db = connect_namespace("dir", {"root": str(tmp_path)})
    db.create_namespace(["plans"])
    tbl = db.create_table("base", pa.table({"id": range(20)}))

    plan = (
        permutation_builder(tbl)
        .split_sequential(fixed=2, split_names=["train", "test"])
        .persist(db, "splits", namespace_path=["plans"])
        .execute()
    )
    assert plan.namespace == ["plans"]
    assert plan._conn is db

    restored_plan = pickle.loads(pickle.dumps(plan))
    assert restored_plan.namespace == ["plans"]
    assert restored_plan.count_rows() == 20

    permutation = Permutation.from_tables(tbl, plan, split="train")
    restored = pickle.loads(pickle.dumps(permutation))
    assert restored.num_rows == 10
    assert sorted(
        row["id"] for row in restored.__getitems__(list(range(restored.num_rows)))
    ) == list(range(10))


def test_persist_direct_result_keeps_session_pickle_error(tmp_path):
    import pickle

    db = connect(tmp_path, session=Session())
    tbl = db.create_table("base", pa.table({"id": range(5)}))
    plan = permutation_builder(tbl).persist(db, "splits").execute()

    with pytest.raises(TypeError, match="Session"):
        pickle.dumps(plan)

    permutation = Permutation.from_tables(tbl, plan)
    with pytest.raises(TypeError, match="Session"):
        pickle.dumps(permutation)


def test_persist_direct_result_preserves_manifest_connection_state(tmp_path):
    import pickle

    db = connect(tmp_path, manifest_enabled=True)
    tbl = db.create_table("base", pa.table({"id": range(5)}))
    plan = permutation_builder(tbl).persist(db, "splits").execute()

    restored = pickle.loads(pickle.dumps(plan))
    assert restored._conn._manifest_enabled is True
    assert restored.count_rows() == 5


def test_base_branch_property_reads_plan_metadata(tmp_path):
    db = connect(tmp_path)
    tbl = db.create_table("base", pa.table({"id": range(10)}))
    branch = tbl.branches.create("exp")
    branch.add(pa.table({"id": range(10, 20)}))

    plan = permutation_builder(branch).persist(db, "branch_splits").execute()
    permutation = Permutation.from_tables(branch, plan)

    assert permutation.base_branch == "exp"
    assert permutation.base_version == branch.version
    assert permutation.num_rows == 20


def test_persist_embedded_plan_for_remote_base(tmp_path):
    from lancedb import deserialize_conn

    server = MockPermutationServer(num_rows=8)
    db = connect(tmp_path)

    with mock_remote_table(server) as remote:
        permutation_builder(remote).shuffle(seed=7).persist(db, "plan").execute()
        plan = db.open_table("plan")
        expected_ids = plan.to_arrow().to_pydict()["row_id"]
        assert expected_ids != list(range(8))

        # Advance the mock's latest version. A fresh handle must see v2, while
        # the plan's recorded v1 still pins every read to the old snapshot.
        server.publish_version(2, list(range(100, 108)))
        fresh_remote = deserialize_conn(
            remote._serialized_connection_state()
        ).open_table(server.name)
        assert fresh_remote.version == 2

        permutation = Permutation.from_tables(fresh_remote, plan)
        assert permutation.base_version == 1
        assert permutation.base_branch is None
        assert permutation._plan_version == 1
        assert permutation.permutation_table.version == 1

        sequential_ids = [
            row["id"]
            for batch in permutation.iter(3, skip_last_batch=False)
            for row in batch
        ]
        assert sequential_ids == expected_ids
        assert [row["id"] for row in permutation.__getitems__([5, 1, 5, 3])] == [
            expected_ids[i] for i in [5, 1, 5, 3]
        ]

        # The plan is local, but its build and gathers address the remote base
        # at the recorded main/v1 snapshot. Latest describes carry no version.
        assert server.scans
        assert server.takes
        for body in [*server.scans, *server.takes]:
            assert body.get("version") == 1
            assert body.get("branch") is None
        describe_versions = [body.get("version") for body in server.describe_bodies]
        assert None in describe_versions
        assert 1 in describe_versions
        assert all(body.get("branch") is None for body in server.describe_bodies)


def test_persisted_plan_pickle_reopens_same_snapshot_after_overwrite(tmp_path):
    from datetime import timedelta
    import pickle

    db = connect(tmp_path, read_consistency_interval=timedelta(0))
    tbl = db.create_table("base", pa.table({"id": range(20)}))
    plan = permutation_builder(tbl).filter("id < 10").persist(db, "splits").execute()
    permutation = Permutation.from_tables(tbl, plan)
    old_base_version = permutation.base_version
    old_plan_version = permutation._plan_version
    state = permutation.__getstate__()
    assert "permutation_data" not in state
    assert (
        state["permutation_table_state"]["state"]["checkout_version"]
        == old_plan_version
    )

    # Replace the plan with disjoint rows of the same cardinality.
    permutation_builder(tbl).filter("id >= 10").persist(
        db, "splits", mode="overwrite"
    ).execute()
    assert plan.version == old_plan_version + 1
    assert permutation.permutation_table.version == old_plan_version

    live_ids = [
        row["id"] for row in permutation.__getitems__(list(range(permutation.num_rows)))
    ]
    assert live_ids == list(range(10))

    # A reader recreated after the overwrite must still use the pinned snapshot.
    permutation.reader = None
    permutation._pid = None
    recreated_ids = [
        row["id"] for row in permutation.__getitems__(list(range(permutation.num_rows)))
    ]
    assert recreated_ids == list(range(10))

    restored = pickle.loads(pickle.dumps(permutation))
    assert restored.base_version == old_base_version
    restored_ids = [
        row["id"] for row in restored.__getitems__(list(range(restored.num_rows)))
    ]
    assert restored_ids == list(range(10))

    # A newly constructed permutation sees the overwritten plan.
    fresh = Permutation.from_tables(tbl, db.open_table("splits"))
    assert fresh.base_version == old_base_version
    fresh_ids = [row["id"] for row in fresh.__getitems__(list(range(fresh.num_rows)))]
    assert fresh_ids == list(range(10, 20))


def test_persisted_plan_pickle_uses_captured_main_branch(tmp_path):
    import pickle

    db = connect(tmp_path)
    tbl = db.create_table("base", pa.table({"id": range(20)}))
    permutation_builder(tbl).persist(db, "splits").execute()

    initial_plan = db.open_table("splits")
    initial_plan.branches.create("exp", from_version=1)

    # Main plan version 2 still has all 20 rows.
    permutation_builder(tbl).persist(db, "splits", mode="overwrite").execute()
    main_plan = db.open_table("splits")
    assert main_plan.version == 2

    # Branch plan version 2 has different contents, so the numeric version alone
    # does not identify the snapshot.
    branch_plan = db.open_table("splits", branch="exp")
    branch_plan.add(
        pa.table(
            {
                "row_id": pa.array([0], type=pa.uint64()),
                "split_id": pa.array([0], type=pa.uint32()),
            }
        )
    )
    branch_plan = db.open_table("splits", branch="exp")
    assert branch_plan.version == 2
    assert branch_plan.count_rows() == 21
    branch_plan.tags.create("exp_v2", 2)

    permutation = Permutation.from_tables(tbl, main_plan)

    # Move the same passed-in plan handle to the other branch at the same
    # numeric version. Pickle must still reopen the captured main snapshot.
    main_plan.checkout("exp_v2")
    assert main_plan.current_branch() == "exp"
    assert main_plan.version == 2
    payload = pickle.dumps(permutation)

    restored = pickle.loads(payload)
    assert restored.permutation_table.current_branch() is None
    assert restored.permutation_table.version == 2
    assert restored.num_rows == 20


def test_persisted_plan_pickle_reopens_in_spawned_process(tmp_path):
    import multiprocessing as mp
    import pickle

    db = connect(tmp_path)
    tbl = db.create_table("base", pa.table({"id": range(12)}))
    plan = permutation_builder(tbl).persist(db, "splits").execute()
    payload = pickle.dumps(Permutation.from_tables(tbl, plan))

    tbl.add(pa.table({"id": range(12, 24)}))
    permutation_builder(tbl).persist(db, "splits", mode="overwrite").execute()

    context = mp.get_context("spawn")
    output = context.Queue()
    process = context.Process(
        target=_read_permutation_in_spawned_process, args=(payload, output)
    )
    process.start()
    try:
        ids = output.get(timeout=60)
    finally:
        process.join(timeout=60)

    assert process.exitcode == 0
    assert sorted(ids) == list(range(12))


def test_old_permutation_pickle_state_still_round_trips(tmp_path):
    db = connect(tmp_path)
    tbl = db.create_table("base", pa.table({"id": range(20)}))
    plan = permutation_builder(tbl).execute()
    state = {
        "base_table_name": tbl.name,
        "permutation_data": plan.to_arrow(),
        "split": 0,
        "selection": {"id": "id"},
        "batch_size": DEFAULT_BATCH_SIZE,
        "transform_fn": Transforms.arrow2python,
        "offset": None,
        "limit": None,
        "connection_factory": None,
        "base_table_uri": str(tmp_path),
        "base_table_storage_options": None,
        "base_table_namespace": [],
    }

    restored = Permutation.__new__(Permutation)
    restored.__setstate__(state)

    assert restored.base_version == tbl.version
    assert restored.base_branch is None
    assert sorted(
        row["id"] for row in restored.__getitems__(list(range(restored.num_rows)))
    ) == list(range(20))


def test_builder_copy_is_shallow_and_does_not_materialize(
    some_table: Table, some_perm_table: Table, monkeypatch
):
    """The with_* builders use copy.copy, which must stay a shallow copy.

    Without ``Permutation.__copy__`` the copy falls back to the pickle path,
    which materializes the permutation table with ``to_arrow()`` before
    rebuilding the base table and reader. That is expensive for in-memory
    tables and fails for remote permutation tables.
    """
    permutation = Permutation.from_tables(some_table, some_perm_table)

    def fail_materialize(*args, **kwargs):
        raise AssertionError("copy.copy must not materialize the permutation table")

    def fail_getstate(_self):
        raise AssertionError("copy.copy must not pickle the permutation")

    monkeypatch.setattr(permutation.permutation_table, "to_arrow", fail_materialize)
    monkeypatch.setattr(Permutation, "__getstate__", fail_getstate)

    formatted = permutation.with_format("arrow")
    assert formatted is not permutation
    assert formatted.transform_fn is Transforms.arrow2arrow
    assert permutation.transform_fn is Transforms.arrow2python
    assert formatted.base_table is permutation.base_table
    assert formatted.permutation_table is permutation.permutation_table
    assert formatted.reader is permutation.reader

    resized = permutation.with_batch_size(7)
    assert resized.batch_size == 7
    assert permutation.batch_size == DEFAULT_BATCH_SIZE
    assert resized.selection is permutation.selection
    resized.selection = {"id": "id"}
    assert permutation.selection is not resized.selection


def test_split_random_counts(mem_db):
    """Test random splitting with absolute counts."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"x": range(100), "y": range(100)})
    )
    permutation_tbl = permutation_builder(tbl).split_random(counts=[20, 30]).execute()

    # Check that we have exactly the requested counts
    assert permutation_tbl.count_rows() == 50

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    split_ids = data["split_id"]
    assert split_ids.count(0) == 20
    assert split_ids.count(1) == 30


def test_split_random_fixed(mem_db):
    """Test random splitting with fixed number of splits."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"x": range(100), "y": range(100)})
    )
    permutation_tbl = permutation_builder(tbl).split_random(fixed=4).execute()

    # Check that we have 4 splits with 25 rows each
    assert permutation_tbl.count_rows() == 100

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    split_ids = data["split_id"]
    assert set(split_ids) == {0, 1, 2, 3}

    for split_id in range(4):
        assert split_ids.count(split_id) == 25


def test_permutation_over_table_with_schema_metadata(tmp_path):
    """Schema-level metadata on the base table must not break the build.

    Hugging Face datasets carry keys like `huggingface` or `source-column-name-map`.
    """
    schema = pa.schema(
        [pa.field("x", pa.int64())],
        # `split_names` collides with a permutation config key; the config must win.
        metadata={b"huggingface": b"{}", b"split_names": b'["base"]'},
    )
    tbl = connect(tmp_path).create_table(
        "t", pa.table({"x": list(range(100))}, schema=schema)
    )
    permutation_tbl = (
        permutation_builder(tbl)
        .split_random(fixed=2, seed=1, split_names=["train", "test"])
        .execute()
    )

    permutations = Permutations(tbl, permutation_tbl)
    assert permutations.split_names == ["train", "test"]
    train = Permutation.from_tables(tbl, permutation_tbl, "train")
    test = Permutation.from_tables(tbl, permutation_tbl, "test")
    assert train.num_rows + test.num_rows == 100
    train_x = [row["x"] for batch in train.iter(16) for row in batch]
    test_x = [row["x"] for batch in test.iter(16) for row in batch]
    assert sorted(train_x + test_x) == list(range(100))


def test_split_random_with_seed(mem_db):
    """Test that seeded random splits are reproducible."""
    tbl = mem_db.create_table("test_table", pa.table({"x": range(50), "y": range(50)}))

    # Create two identical permutations with same seed
    perm1 = permutation_builder(tbl).split_random(ratios=[0.6, 0.4], seed=42).execute()

    perm2 = permutation_builder(tbl).split_random(ratios=[0.6, 0.4], seed=42).execute()

    # Results should be identical
    data1 = perm1.search(None).to_arrow().to_pydict()
    data2 = perm2.search(None).to_arrow().to_pydict()

    assert data1["row_id"] == data2["row_id"]
    assert data1["split_id"] == data2["split_id"]


def test_bootstrap_sample_fixed(mem_db):
    """Output row count is num_samples, not the input row count, and splits divide
    it evenly."""
    tbl = mem_db.create_table("test_table", pa.table({"x": range(20)}))
    permutation_tbl = (
        permutation_builder(tbl).bootstrap_sample(1000, fixed=4, seed=42).execute()
    )

    assert permutation_tbl.count_rows() == 1000

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    split_ids = data["split_id"]
    assert set(split_ids) == {0, 1, 2, 3}
    for split_id in range(4):
        assert split_ids.count(split_id) == 250

    # Every sampled row_id must still be one of the 20 real rows.
    assert set(data["row_id"]).issubset(set(range(20)))


def test_bootstrap_sample_draws_with_replacement(mem_db):
    """Sampling more rows than exist must produce repeats."""
    tbl = mem_db.create_table("test_table", pa.table({"x": range(5)}))
    permutation_tbl = (
        permutation_builder(tbl).bootstrap_sample(500, fixed=1, seed=1).execute()
    )

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    assert len(data["row_id"]) == 500
    # Only 5 distinct rows exist, so with 500 draws every one of them must repeat.
    for row_id in range(5):
        assert data["row_id"].count(row_id) > 1


def test_bootstrap_sample_with_seed_is_reproducible(mem_db):
    tbl = mem_db.create_table("test_table", pa.table({"x": range(50)}))

    perm1 = permutation_builder(tbl).bootstrap_sample(200, fixed=2, seed=42).execute()
    perm2 = permutation_builder(tbl).bootstrap_sample(200, fixed=2, seed=42).execute()

    data1 = perm1.search(None).to_arrow().to_pydict()
    data2 = perm2.search(None).to_arrow().to_pydict()
    assert data1["row_id"] == data2["row_id"]
    assert data1["split_id"] == data2["split_id"]


def test_bootstrap_sample_respects_filter(mem_db):
    tbl = mem_db.create_table("test_table", pa.table({"x": range(20)}))
    permutation_tbl = (
        permutation_builder(tbl)
        .filter("x >= 15")
        .bootstrap_sample(100, fixed=1, seed=7)
        .execute()
    )

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    assert len(data["row_id"]) == 100
    assert set(data["row_id"]).issubset(set(range(15, 20)))


def test_bootstrap_sample_requires_exactly_one_size_arg(mem_db):
    tbl = mem_db.create_table("test_table", pa.table({"x": range(10)}))
    with pytest.raises(ValueError):
        permutation_builder(tbl).bootstrap_sample(10).execute()
    with pytest.raises(ValueError):
        permutation_builder(tbl).bootstrap_sample(
            10, ratios=[0.5, 0.5], fixed=2
        ).execute()


def test_split_hash(mem_db):
    """Test hash-based splitting."""
    tbl = mem_db.create_table(
        "test_table",
        pa.table(
            {
                "id": range(100),
                "category": (["A", "B", "C"] * 34)[:100],  # Repeating pattern
                "value": range(100),
            }
        ),
    )

    permutation_tbl = (
        permutation_builder(tbl)
        .split_hash(["category"], [1, 1], discard_weight=0)
        .execute()
    )

    # Should have all 100 rows (no discard)
    assert permutation_tbl.count_rows() == 100

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    split_ids = data["split_id"]
    assert set(split_ids) == {0, 1}

    # Verify that each split has roughly 50 rows (allowing for hash variance)
    split_0_count = split_ids.count(0)
    split_1_count = split_ids.count(1)
    assert 30 <= split_0_count <= 70  # ~50 ± 20 tolerance for hash distribution
    assert 30 <= split_1_count <= 70  # ~50 ± 20 tolerance for hash distribution

    # Hash splits should be deterministic - same category should go to same split
    # Let's verify by creating another permutation and checking consistency
    perm2 = (
        permutation_builder(tbl)
        .split_hash(["category"], [1, 1], discard_weight=0)
        .execute()
    )

    data2 = perm2.search(None).to_arrow().to_pydict()
    assert data["split_id"] == data2["split_id"]  # Should be identical


def test_split_hash_default_discard_weight_matches_zero(mem_db):
    """Omitting discard_weight must assign every row like discard_weight=0."""
    tbl = mem_db.create_table(
        "test_table",
        pa.table(
            {
                "id": range(100),
                "category": (["A", "B", "C"] * 34)[:100],
                "value": range(100),
            }
        ),
    )

    default = (
        permutation_builder(tbl)
        .split_hash(["category"], [1, 1], split_names=["train", "test"])
        .execute()
    )
    explicit_zero = (
        permutation_builder(tbl)
        .split_hash(
            ["category"], [1, 1], discard_weight=0, split_names=["train", "test"]
        )
        .execute()
    )

    default_data = default.search(None).to_arrow().to_pydict()
    explicit_data = explicit_zero.search(None).to_arrow().to_pydict()
    assert default.count_rows() == 100
    assert default.count_rows() == explicit_zero.count_rows()
    assert dict(zip(default_data["row_id"], default_data["split_id"])) == dict(
        zip(explicit_data["row_id"], explicit_data["split_id"])
    )


def test_split_hash_with_discard(mem_db):
    """Test hash-based splitting with discard weight."""
    tbl = mem_db.create_table(
        "test_table",
        pa.table({"id": range(100), "category": ["A", "B"] * 50, "value": range(100)}),
    )

    permutation_tbl = (
        # Hash a high-cardinality column: "category" has only two distinct
        # values, so whether anything is discarded would hinge on where those
        # two hashes land rather than on the discard weight.
        permutation_builder(tbl)
        .split_hash(["id"], [1, 1], discard_weight=2)  # Should discard ~50%
        .execute()
    )

    # Should have fewer than 100 rows due to discard
    row_count = permutation_tbl.count_rows()
    assert row_count < 100
    assert row_count > 0  # But not empty


def test_split_sequential(mem_db):
    """Test sequential splitting."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"x": range(100), "y": range(100)})
    )

    permutation_tbl = (
        permutation_builder(tbl).split_sequential(counts=[30, 40]).execute()
    )

    assert permutation_tbl.count_rows() == 70

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    row_ids = data["row_id"]
    split_ids = data["split_id"]

    # Sequential should maintain order
    assert row_ids == sorted(row_ids)

    # First 30 should be split 0, next 40 should be split 1
    assert split_ids[:30] == [0] * 30
    assert split_ids[30:] == [1] * 40


def test_split_calculated(mem_db):
    """Test calculated splitting."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(100), "value": range(100)})
    )

    permutation_tbl = (
        permutation_builder(tbl)
        .split_calculated("id % 3")  # Split based on id modulo 3
        .execute()
    )

    assert permutation_tbl.count_rows() == 100

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    row_ids = data["row_id"]
    split_ids = data["split_id"]

    # Verify the calculation: each row's split_id should equal row_id % 3
    for i, (row_id, split_id) in enumerate(zip(row_ids, split_ids)):
        assert split_id == row_id % 3


def test_split_error_cases(mem_db):
    """Test error handling for invalid split parameters."""
    tbl = mem_db.create_table("test_table", pa.table({"x": range(10), "y": range(10)}))

    # Test split_random with no parameters
    with pytest.raises(
        ValueError,
        match="Exactly one of 'ratios', 'counts', or 'fixed' must be provided",
    ):
        permutation_builder(tbl).split_random().execute()

    # Test split_random with multiple parameters
    with pytest.raises(
        ValueError,
        match="Exactly one of 'ratios', 'counts', or 'fixed' must be provided",
    ):
        permutation_builder(tbl).split_random(
            ratios=[0.5, 0.5], counts=[5, 5]
        ).execute()

    # Test split_sequential with no parameters
    with pytest.raises(
        ValueError,
        match="Exactly one of 'ratios', 'counts', or 'fixed' must be provided",
    ):
        permutation_builder(tbl).split_sequential().execute()

    # Test split_sequential with multiple parameters
    with pytest.raises(
        ValueError,
        match="Exactly one of 'ratios', 'counts', or 'fixed' must be provided",
    ):
        permutation_builder(tbl).split_sequential(ratios=[0.5, 0.5], fixed=2).execute()


def test_shuffle_no_seed(mem_db):
    """Test shuffling without a seed."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(100), "value": range(100)})
    )

    # Create a permutation with shuffling (no seed)
    permutation_tbl = permutation_builder(tbl).shuffle().execute()

    assert permutation_tbl.count_rows() == 100

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    row_ids = data["row_id"]

    # Row IDs should not be in sequential order due to shuffling
    # This is probabilistic but with 100 rows, it's extremely unlikely they'd stay
    # in order
    assert row_ids != list(range(100))


def test_shuffle_with_seed(mem_db):
    """Test that shuffling with a seed is reproducible."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(50), "value": range(50)})
    )

    # Create two identical permutations with same shuffle seed
    perm1 = permutation_builder(tbl).shuffle(seed=42).execute()

    perm2 = permutation_builder(tbl).shuffle(seed=42).execute()

    # Results should be identical due to same seed
    data1 = perm1.search(None).to_arrow().to_pydict()
    data2 = perm2.search(None).to_arrow().to_pydict()

    assert data1["row_id"] == data2["row_id"]
    assert data1["split_id"] == data2["split_id"]


def test_shuffle_with_clump_size(mem_db):
    """Test shuffling with clump size."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(100), "value": range(100)})
    )

    # Create a permutation with shuffling using clumps
    permutation_tbl = (
        permutation_builder(tbl)
        .shuffle(clump_size=10)  # 10-row clumps
        .execute()
    )

    assert permutation_tbl.count_rows() == 100

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    row_ids = data["row_id"]

    for i in range(10):
        start = row_ids[i * 10]
        assert row_ids[i * 10 : (i + 1) * 10] == list(range(start, start + 10))


def test_shuffle_different_seeds(mem_db):
    """Test that different seeds produce different shuffle orders."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(50), "value": range(50)})
    )

    # Create two permutations with different shuffle seeds
    perm1 = permutation_builder(tbl).split_random(fixed=2).shuffle(seed=42).execute()

    perm2 = permutation_builder(tbl).split_random(fixed=2).shuffle(seed=123).execute()

    # Results should be different due to different seeds
    data1 = perm1.search(None).to_arrow().to_pydict()
    data2 = perm2.search(None).to_arrow().to_pydict()

    # Row order should be different
    assert data1["row_id"] != data2["row_id"]


def test_shuffle_combined_with_splits(mem_db):
    """Test shuffling combined with different split strategies."""
    tbl = mem_db.create_table(
        "test_table",
        pa.table(
            {
                "id": range(100),
                "category": (["A", "B", "C"] * 34)[:100],
                "value": range(100),
            }
        ),
    )

    # Test shuffle with random splits
    perm_random = (
        permutation_builder(tbl)
        .split_random(ratios=[0.6, 0.4], seed=42)
        .shuffle(seed=123, clump_size=None)
        .execute()
    )

    # Test shuffle with hash splits
    perm_hash = (
        permutation_builder(tbl)
        .split_hash(["category"], [1, 1], discard_weight=0)
        .shuffle(seed=456, clump_size=5)
        .execute()
    )

    # Test shuffle with sequential splits
    perm_sequential = (
        permutation_builder(tbl)
        .split_sequential(counts=[40, 35])
        .shuffle(seed=789, clump_size=None)
        .execute()
    )

    # Verify all permutations work and have expected properties
    assert perm_random.count_rows() == 100
    assert perm_hash.count_rows() == 100
    assert perm_sequential.count_rows() == 75

    # Verify shuffle affected the order
    data_random = perm_random.search(None).to_arrow().to_pydict()
    data_sequential = perm_sequential.search(None).to_arrow().to_pydict()

    assert data_random["row_id"] != list(range(100))
    assert data_sequential["row_id"] != list(range(75))


def test_no_shuffle_maintains_order(mem_db):
    """Test that not calling shuffle maintains the original order."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(50), "value": range(50)})
    )

    # Create permutation without shuffle (should maintain some order)
    permutation_tbl = (
        permutation_builder(tbl)
        .split_sequential(counts=[25, 25])  # Sequential maintains order
        .execute()
    )

    assert permutation_tbl.count_rows() == 50

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    row_ids = data["row_id"]

    # With sequential splits and no shuffle, should maintain order
    assert row_ids == list(range(50))


def test_filter_basic(mem_db):
    """Test basic filtering functionality."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(100), "value": range(100, 200)})
    )

    # Filter to only include rows where id < 50
    permutation_tbl = permutation_builder(tbl).filter("id < 50").execute()

    assert permutation_tbl.count_rows() == 50

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    row_ids = data["row_id"]

    # All row_ids should be less than 50
    assert all(row_id < 50 for row_id in row_ids)


def test_filter_with_splits(mem_db):
    """Test filtering combined with split strategies."""
    tbl = mem_db.create_table(
        "test_table",
        pa.table(
            {
                "id": range(100),
                "category": (["A", "B", "C"] * 34)[:100],
                "value": range(100),
            }
        ),
    )

    # Filter to only category A and B, then split
    permutation_tbl = (
        permutation_builder(tbl)
        .filter("category IN ('A', 'B')")
        .split_random(ratios=[0.5, 0.5])
        .execute()
    )

    # Should have fewer than 100 rows due to filtering
    row_count = permutation_tbl.count_rows()
    assert row_count == 67

    # Verify the permutation table only contains row_id and split_id
    assert set(permutation_tbl.schema.names) == {"row_id", "split_id"}

    row_ids = permutation_tbl.search(None).to_arrow().to_pydict()["row_id"]
    data = tbl.take_row_ids(row_ids).to_arrow().to_pydict()
    categories = data["category"]

    # All categories should be A or B
    assert all(cat in ("A", "B") for cat in categories)


def test_filter_with_shuffle(mem_db):
    """Test filtering combined with shuffling."""
    tbl = mem_db.create_table(
        "test_table",
        pa.table(
            {
                "id": range(100),
                "category": (["A", "B", "C", "D"] * 25)[:100],
                "value": range(100),
            }
        ),
    )

    # Filter and shuffle
    permutation_tbl = (
        permutation_builder(tbl)
        .filter("category IN ('A', 'C')")
        .shuffle(seed=42)
        .execute()
    )

    row_count = permutation_tbl.count_rows()
    assert row_count == 50  # Should have 50 rows (A and C categories)

    data = permutation_tbl.search(None).to_arrow().to_pydict()
    row_ids = data["row_id"]

    assert row_ids != sorted(row_ids)


def test_filter_empty_result(mem_db):
    """Test filtering that results in empty set."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(10), "value": range(10)})
    )

    # Filter that matches nothing
    permutation_tbl = (
        permutation_builder(tbl)
        .filter("value > 100")  # No values > 100 in our data
        .execute()
    )

    assert permutation_tbl.count_rows() == 0


@pytest.fixture
def mem_db() -> DBConnection:
    return connect("memory:///")


@pytest.fixture
def some_table(mem_db: DBConnection) -> Table:
    data = pa.table(
        {
            "id": range(1000),
            "value": range(1000),
        }
    )
    return mem_db.create_table("some_table", data)


def test_no_split_names(some_table: Table):
    perm_tbl = (
        permutation_builder(some_table).split_sequential(counts=[500, 500]).execute()
    )
    permutations = Permutations(some_table, perm_tbl)
    assert permutations.split_names == []
    assert permutations.split_dict == {}
    assert permutations[0].num_rows == 500
    assert permutations[1].num_rows == 500


def test_permutations_metadata_without_split_names_key(mem_db: DBConnection):
    """Regression: schema metadata present but missing split_names key must not crash.

    Previously, `.get(b"split_names", None).decode()` was called unconditionally,
    so any permutation table whose metadata dict had other keys but no split_names
    raised AttributeError: 'NoneType' has no attribute 'decode'.
    """
    base = mem_db.create_table("base_nosplit", pa.table({"x": range(10)}))

    # Build a permutation-like table that carries some metadata but NOT split_names.
    raw = pa.table(
        {
            "row_id": pa.array(range(10), type=pa.uint64()),
            "split_id": pa.array([0] * 10, type=pa.uint32()),
        }
    ).replace_schema_metadata({b"other_key": b"other_value"})
    perm_tbl = mem_db.create_table("perm_nosplit", raw)

    permutations = Permutations(base, perm_tbl)
    assert permutations.split_names == []
    assert permutations.split_dict == {}


def test_from_tables_string_split_missing_names_key(mem_db: DBConnection):
    """Regression: from_tables() with a string split must raise ValueError, not
    AttributeError.

    Previously, `.get(b"split_names", None).decode()` crashed with AttributeError
    when the metadata dict existed but had no split_names key.
    """
    base = mem_db.create_table("base_strsplit", pa.table({"x": range(10)}))

    raw = pa.table(
        {
            "row_id": pa.array(range(10), type=pa.uint64()),
            "split_id": pa.array([0] * 10, type=pa.uint32()),
        }
    ).replace_schema_metadata({b"other_key": b"other_value"})
    perm_tbl = mem_db.create_table("perm_strsplit", raw)

    with pytest.raises(ValueError, match="no split names are defined"):
        Permutation.from_tables(base, perm_tbl, split="train")


@pytest.fixture
def some_perm_table(some_table: Table) -> Table:
    return (
        permutation_builder(some_table)
        .split_random(ratios=[0.95, 0.05], seed=42, split_names=["train", "test"])
        .shuffle(seed=42)
        .execute()
    )


def test_nonexistent_split(some_table: Table, some_perm_table: Table):
    # Reference by name and name does not exist
    with pytest.raises(ValueError, match="split `nonexistent` is not defined"):
        Permutation.from_tables(some_table, some_perm_table, "nonexistent")

    # Reference by ordinal and there are no rows
    with pytest.raises(ValueError, match="No rows found"):
        Permutation.from_tables(some_table, some_perm_table, 5)


def test_permutations(some_table: Table, some_perm_table: Table):
    permutations = Permutations(some_table, some_perm_table)
    assert permutations.split_names == ["train", "test"]
    assert permutations.split_dict == {"train": 0, "test": 1}
    assert permutations["train"].num_rows == 950
    assert permutations[0].num_rows == 950
    assert permutations["test"].num_rows == 50
    assert permutations[1].num_rows == 50

    with pytest.raises(ValueError, match="No split named `nonexistent` found"):
        permutations["nonexistent"]
    with pytest.raises(ValueError, match="No rows found"):
        permutations[5]


@pytest.fixture
def some_permutation(some_table: Table, some_perm_table: Table) -> Permutation:
    return Permutation.from_tables(some_table, some_perm_table)


def test_num_rows(some_permutation: Permutation):
    assert some_permutation.num_rows == 950


def test_num_columns(some_permutation: Permutation):
    assert some_permutation.num_columns == 2


def test_column_names(some_permutation: Permutation):
    assert some_permutation.column_names == ["id", "value"]


def test_shape(some_permutation: Permutation):
    assert some_permutation.shape == (950, 2)


def test_schema(some_permutation: Permutation):
    assert some_permutation.schema == pa.schema(
        [("id", pa.int64()), ("value", pa.int64())]
    )


def test_limit_offset(some_permutation: Permutation):
    assert some_permutation.with_take(100).num_rows == 100
    assert some_permutation.with_skip(100).num_rows == 850
    assert some_permutation.with_take(100).with_skip(100).num_rows == 100

    with pytest.raises(Exception):
        some_permutation.with_take(1000000).num_rows
    with pytest.raises(Exception):
        some_permutation.with_skip(1000000).num_rows
    with pytest.raises(Exception):
        some_permutation.with_take(500).with_skip(500).num_rows
    with pytest.raises(Exception):
        some_permutation.with_skip(500).with_take(500).num_rows


def test_remove_columns(some_permutation: Permutation):
    assert some_permutation.remove_columns(["value"]).schema == pa.schema(
        [("id", pa.int64())]
    )
    # Should not modify the original permutation
    assert some_permutation.schema.names == ["id", "value"]
    # Cannot remove all columns
    with pytest.raises(ValueError, match="Cannot remove all columns"):
        some_permutation.remove_columns(["id", "value"])


def test_rename_column(some_permutation: Permutation):
    assert some_permutation.rename_column("value", "new_value").schema == pa.schema(
        [("id", pa.int64()), ("new_value", pa.int64())]
    )
    # Should not modify the original permutation
    assert some_permutation.schema.names == ["id", "value"]
    # Cannot rename to an existing column
    with pytest.raises(
        ValueError,
        match="a column with that name already exists",
    ):
        some_permutation.rename_column("value", "id")
    # Cannot rename a non-existent column
    with pytest.raises(
        ValueError,
        match="does not exist",
    ):
        some_permutation.rename_column("non_existent", "new_value")


def test_rename_column_keeps_position(some_permutation: Permutation):
    renamed = some_permutation.rename_column("id", "new_id")
    assert renamed.schema == pa.schema([("new_id", pa.int64()), ("value", pa.int64())])
    batch = renamed.with_format("arrow").__getitems__([0])
    assert batch.schema.names == ["new_id", "value"]


def test_rename_columns(some_permutation: Permutation):
    assert some_permutation.rename_columns({"value": "new_value"}).schema == pa.schema(
        [("id", pa.int64()), ("new_value", pa.int64())]
    )
    # Should not modify the original permutation
    assert some_permutation.schema.names == ["id", "value"]
    # Cannot rename to an existing column
    with pytest.raises(ValueError, match="a column with that name already exists"):
        some_permutation.rename_columns({"value": "id"})


def test_select_columns(some_permutation: Permutation):
    assert some_permutation.select_columns(["id"]).schema == pa.schema(
        [("id", pa.int64())]
    )
    # Should not modify the original permutation
    assert some_permutation.schema.names == ["id", "value"]
    # Cannot select a non-existent column
    with pytest.raises(ValueError, match="does not exist"):
        some_permutation.select_columns(["non_existent"])
    # Empty selection is not allowed
    with pytest.raises(ValueError, match="select at least one column"):
        some_permutation.select_columns([])


def test_iter_basic(some_permutation: Permutation):
    """Test basic iteration with custom batch size."""
    batch_size = 100
    batches = list(some_permutation.iter(batch_size, skip_last_batch=False))

    # Check that we got the expected number of batches
    expected_batches = (950 + batch_size - 1) // batch_size  # ceiling division
    assert len(batches) == expected_batches

    # Check that all batches are lists of dicts (default python format)
    assert all(isinstance(batch, list) for batch in batches)

    # Check that batches have the correct structure
    for batch in batches:
        assert "id" in batch[0]
        assert "value" in batch[0]

    # Check that all batches except the last have the correct size
    for batch in batches[:-1]:
        assert len(batch) == batch_size

    # Last batch might be smaller
    assert len(batches[-1]) <= batch_size


def test_iter_skip_last_batch(some_permutation: Permutation):
    """Test iteration with skip_last_batch=True."""
    batch_size = 300
    batches_with_skip = list(some_permutation.iter(batch_size, skip_last_batch=True))
    batches_without_skip = list(
        some_permutation.iter(batch_size, skip_last_batch=False)
    )

    # With skip_last_batch=True, we should have fewer batches if the last one is partial
    num_full_batches = 950 // batch_size
    assert len(batches_with_skip) == num_full_batches

    # Without skip_last_batch, we should have one more batch if there's a remainder
    if 950 % batch_size != 0:
        assert len(batches_without_skip) == num_full_batches + 1
        # Last batch should be smaller
        assert len(batches_without_skip[-1]) == 950 % batch_size

    # All batches with skip_last_batch should be full size
    for batch in batches_with_skip:
        assert len(batch) == batch_size


def test_iter_keeps_rows_from_short_reader_batches(some_table: Table):
    """A filtered split read can yield short batches before the end."""
    perm_tbl = (
        permutation_builder(some_table)
        .split_hash(["id"], [1, 1], discard_weight=0)
        .execute()
    )
    permutation = Permutation.from_tables(some_table, perm_tbl, 0)
    num_rows = permutation.num_rows
    expected_ids = [
        row["id"] for row in permutation.__getitems__(list(range(num_rows)))
    ]
    batch_size = 37
    assert num_rows % batch_size != 0

    batches = list(permutation.iter(batch_size, skip_last_batch=False))
    assert [len(batch) for batch in batches[:-1]] == [batch_size] * (len(batches) - 1)
    assert [row["id"] for batch in batches for row in batch] == expected_ids

    num_full = num_rows // batch_size
    full_batches = list(permutation.iter(batch_size, skip_last_batch=True))
    assert [len(batch) for batch in full_batches] == [batch_size] * num_full
    assert [row["id"] for batch in full_batches for row in batch] == expected_ids[
        : num_full * batch_size
    ]

    dunder_batches = list(permutation.with_batch_size(batch_size))
    assert [len(batch) for batch in dunder_batches] == [batch_size] * num_full


def test_iter_different_batch_sizes(some_permutation: Permutation):
    """Test iteration with different batch sizes."""

    # Test with small batch size
    small_batches = list(some_permutation.iter(100, skip_last_batch=False))
    assert len(small_batches) == 10  # ceiling(950 / 100)

    # Test with large batch size
    large_batches = list(some_permutation.iter(400, skip_last_batch=False))
    assert len(large_batches) == 3  # ceiling(950 / 400)

    # Test with batch size equal to total rows
    single_batch = list(some_permutation.iter(950, skip_last_batch=False))
    assert len(single_batch) == 1
    assert len(single_batch[0]) == 950

    # Test with batch size larger than total rows
    oversized_batch = list(some_permutation.iter(10000, skip_last_batch=False))
    assert len(oversized_batch) == 1
    assert len(oversized_batch[0]) == 950


def test_dunder_iter(some_permutation: Permutation):
    """Test the __iter__ method."""
    # __iter__ should use DEFAULT_BATCH_SIZE (100) and skip_last_batch=True
    batches = list(some_permutation)

    # With DEFAULT_BATCH_SIZE=100 and skip_last_batch=True, we should get 9 batches
    assert len(batches) == 9  # ceiling(950 / 100)

    # All batches should be full size
    for batch in batches:
        assert len(batch) == 100

    some_permutation = some_permutation.with_batch_size(400)
    batches = list(some_permutation)
    assert len(batches) == 2  # floor(950 / 400) since skip_last_batch=True
    for batch in batches:
        assert len(batch) == 400


def test_iter_with_different_formats(some_permutation: Permutation):
    """Test iteration with different output formats."""
    batch_size = 100

    # Test with arrow format
    arrow_perm = some_permutation.with_format("arrow")
    arrow_batches = list(arrow_perm.iter(batch_size, skip_last_batch=False))
    assert all(isinstance(batch, pa.RecordBatch) for batch in arrow_batches)

    # Test with python format (default)
    python_perm = some_permutation.with_format("python")
    python_batches = list(python_perm.iter(batch_size, skip_last_batch=False))
    assert all(isinstance(batch, list) for batch in python_batches)


def test_iter_with_pandas_format(some_permutation: Permutation):
    pd = pytest.importorskip("pandas")
    batch_size = 100
    pandas_perm = some_permutation.with_format("pandas")
    pandas_batches = list(pandas_perm.iter(batch_size, skip_last_batch=False))
    assert all(isinstance(batch, pd.DataFrame) for batch in pandas_batches)


def test_iter_with_column_selection(some_permutation: Permutation):
    """Test iteration after column selection."""
    # Select only the id column
    id_only = some_permutation.select_columns(["id"])
    batches = list(id_only.iter(100, skip_last_batch=False))

    # Check that batches only contain the id column
    for batch in batches:
        assert "id" in batch[0]
        assert "value" not in batch[0]


def test_iter_with_column_rename(some_permutation: Permutation):
    """Test iteration after renaming columns."""
    renamed = some_permutation.rename_column("value", "data")
    batches = list(renamed.iter(100, skip_last_batch=False))

    # Check that batches have the renamed column
    for batch in batches:
        assert "id" in batch[0]
        assert "data" in batch[0]
        assert "value" not in batch[0]


def test_iter_with_limit_offset(some_permutation: Permutation):
    """Test iteration with limit and offset."""
    # Test with offset
    offset_perm = some_permutation.with_skip(100)
    offset_batches = list(offset_perm.iter(100, skip_last_batch=False))
    # Should have 850 rows (950 - 100)
    expected_batches = math.ceil(850 / 100)
    assert len(offset_batches) == expected_batches

    # Test with limit
    limit_perm = some_permutation.with_take(500)
    limit_batches = list(limit_perm.iter(100, skip_last_batch=False))
    # Should have 5 batches (500 / 100)
    assert len(limit_batches) == 5

    no_skip = some_permutation.iter(101, skip_last_batch=False)
    row_100 = next(no_skip)[100]["id"]

    # Test with both limit and offset
    limited_perm = some_permutation.with_skip(100).with_take(300)
    limited_batches = list(limited_perm.iter(100, skip_last_batch=False))
    # Should have 3 batches (300 / 100)
    assert len(limited_batches) == 3
    assert limited_batches[0][0]["id"] == row_100


def test_iter_empty_permutation(mem_db):
    """Test iteration over an empty permutation."""
    # Create a table and filter it to be empty
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(10), "value": range(10)})
    )
    permutation_tbl = permutation_builder(tbl).filter("value > 100").execute()
    with pytest.raises(ValueError, match="No rows found"):
        Permutation.from_tables(tbl, permutation_tbl)


def test_iter_single_row(mem_db):
    """Test iteration over a permutation with a single row."""
    tbl = mem_db.create_table("test_table", pa.table({"id": [42], "value": [100]}))
    permutation_tbl = permutation_builder(tbl).execute()
    perm = Permutation.from_tables(tbl, permutation_tbl)

    # With skip_last_batch=False, should get one batch
    batches = list(perm.iter(10, skip_last_batch=False))
    assert len(batches) == 1
    assert len(batches[0]) == 1

    # With skip_last_batch=True, should skip the single row (since it's < batch_size)
    batches_skip = list(perm.iter(10, skip_last_batch=True))
    assert len(batches_skip) == 0


def test_identity_permutation(mem_db):
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(10), "value": range(10)})
    )
    permutation = Permutation.identity(tbl)

    assert permutation.num_rows == 10
    assert permutation.num_columns == 2

    batches = list(permutation.iter(10, skip_last_batch=False))
    assert len(batches) == 1
    assert len(batches[0]) == 10

    permutation = permutation.remove_columns(["value"])
    assert permutation.num_columns == 1
    assert permutation.schema == pa.schema([("id", pa.int64())])
    assert permutation.column_names == ["id"]
    assert permutation.shape == (10, 1)


def test_transform_fn(mem_db):
    import polars as pl

    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(10), "value": range(10)})
    )
    permutation = Permutation.identity(tbl)

    pl_result = list(permutation.with_format("polars").iter(10, skip_last_batch=False))[
        0
    ]
    assert pl_result.shape == (10, 2)
    assert pl_result.dtypes == [pl.Int64, pl.Int64]
    assert isinstance(pl_result, pl.DataFrame)

    py_result = list(permutation.with_format("python").iter(10, skip_last_batch=False))[
        0
    ]
    assert len(py_result) == 10
    assert "id" in py_result[0]
    assert "value" in py_result[0]
    assert isinstance(py_result, list)

    try:
        import torch

        # "torch" returns a list of per-row dicts. Default DataLoader collate
        # stacks the per-row dicts back into a dict of batched tensors.
        torch_perm = permutation.with_format("torch")
        torch_batch = list(torch_perm.iter(10, skip_last_batch=False))[0]
        assert isinstance(torch_batch, list)
        assert len(torch_batch) == 10
        assert isinstance(torch_batch[0], dict)
        assert set(torch_batch[0].keys()) == {"id", "value"}
        assert isinstance(torch_batch[0]["id"], torch.Tensor)
        assert torch_batch[0]["id"].dtype == torch.int64

        rows = torch_perm.__getitems__([0, 1, 2])
        assert isinstance(rows, list)
        assert len(rows) == 3
        assert isinstance(rows[0], dict)
        assert set(rows[0].keys()) == {"id", "value"}
        assert isinstance(rows[0]["id"], torch.Tensor)

        # "torch_row" returns a list of tensors, one per row.
        torch_rows = list(
            permutation.with_format("torch_row").iter(10, skip_last_batch=False)
        )[0]
        assert isinstance(torch_rows, list)
        assert len(torch_rows) == 10
        assert isinstance(torch_rows[0], torch.Tensor)
        assert torch_rows[0].shape == (2,)
        assert torch_rows[0].dtype == torch.int64

        # "torch_col" stacks columns into a single 2D tensor.
        torch_col = list(
            permutation.with_format("torch_col").iter(10, skip_last_batch=False)
        )[0]
        assert isinstance(torch_col, torch.Tensor)
        assert torch_col.shape == (2, 10)
        assert torch_col.dtype == torch.int64
    except ImportError:
        # Skip check if torch is not installed
        pass

    arrow_result = list(
        permutation.with_format("arrow").iter(10, skip_last_batch=False)
    )[0]
    assert arrow_result.shape == (10, 2)
    assert arrow_result.schema == pa.schema([("id", pa.int64()), ("value", pa.int64())])
    assert isinstance(arrow_result, pa.RecordBatch)


def test_transform_fn_pandas(mem_db):
    pd = pytest.importorskip("pandas")
    import numpy as np

    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(10), "value": range(10)})
    )
    permutation = Permutation.identity(tbl)
    numpy_result = list(
        permutation.with_format("numpy").iter(10, skip_last_batch=False)
    )[0]
    assert numpy_result.shape == (10, 2)
    assert numpy_result.dtype == np.int64
    assert isinstance(numpy_result, np.ndarray)

    result = list(permutation.with_format("pandas").iter(10, skip_last_batch=False))[0]
    assert result.shape == (10, 2)
    assert result.dtypes.tolist() == [np.int64, np.int64]
    assert isinstance(result, pd.DataFrame)


def test_numpy_format_mixed_columns_is_object_ndarray(mem_db):
    pytest.importorskip("pandas")
    import numpy as np

    tbl = mem_db.create_table(
        "test_table",
        pa.table(
            {
                "id": range(3),
                "vector": pa.array(
                    [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
                    type=pa.list_(pa.float32(), 2),
                ),
            }
        ),
    )
    permutation = Permutation.identity(tbl).with_format("numpy")
    result = next(permutation.iter(2, skip_last_batch=False))

    assert isinstance(result, np.ndarray)
    assert result.dtype == object
    assert result.shape == (2, 2)
    np.testing.assert_array_equal(result[0, 1], [1.0, 2.0])


def test_custom_transform(mem_db):
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(10), "value": range(10)})
    )
    permutation = Permutation.identity(tbl)

    def transform(batch: pa.RecordBatch) -> pa.RecordBatch:
        return batch.select(["id"])

    transformed = permutation.with_transform(transform)
    batches = list(transformed.iter(10, skip_last_batch=False))
    assert len(batches) == 1
    batch = batches[0]

    assert batch == pa.record_batch([range(10)], ["id"])


def test_getitems_basic(some_permutation: Permutation):
    """Test __getitems__ returns correct rows by offset."""
    result = some_permutation.__getitems__([0, 1, 2])
    assert isinstance(result, list)
    assert "id" in result[0]
    assert "value" in result[0]
    assert len(result) == 3


def test_getitems_single_index(some_permutation: Permutation):
    """Test __getitems__ with a single index."""
    result = some_permutation.__getitems__([0])
    assert len(result) == 1


def test_getitems_preserves_order(some_permutation: Permutation):
    """Test __getitems__ returns rows in the requested order."""
    # Get rows in forward order
    forward = some_permutation.__getitems__([0, 1, 2, 3, 4])
    # Get the same rows in reverse order
    reverse = some_permutation.__getitems__([4, 3, 2, 1, 0])

    assert [r["id"] for r in forward] == list(reversed([r["id"] for r in reverse]))
    assert [r["value"] for r in forward] == list(
        reversed([r["value"] for r in reverse])
    )


def test_getitems_non_contiguous(some_permutation: Permutation):
    """Test __getitems__ with non-contiguous indices."""
    result = some_permutation.__getitems__([0, 10, 50, 100, 500])
    assert len(result) == 5

    # Each id/value pair should match what we'd get individually
    for i, offset in enumerate([0, 10, 50, 100, 500]):
        single = some_permutation.__getitems__([offset])
        assert result[i]["id"] == single[0]["id"]
        assert result[i]["value"] == single[0]["value"]


def test_getitems_with_column_selection(some_permutation: Permutation):
    """Test __getitems__ respects column selection."""
    id_only = some_permutation.select_columns(["id"])
    result = id_only.__getitems__([0, 1, 2])
    assert "id" in result[0]
    assert "value" not in result[0]
    assert len(result) == 3


def test_getitems_with_column_rename(some_permutation: Permutation):
    """Test __getitems__ respects column renames."""
    renamed = some_permutation.rename_column("value", "data")
    result = renamed.__getitems__([0, 1])
    assert "data" in result[0]
    assert "value" not in result[0]
    assert len(result) == 2


def test_getitems_with_format(some_permutation: Permutation):
    """Test __getitems__ applies the transform function."""
    arrow_perm = some_permutation.with_format("arrow")
    result = arrow_perm.__getitems__([0, 1, 2])
    assert isinstance(result, pa.RecordBatch)
    assert result.num_rows == 3


def test_getitems_with_custom_transform(some_permutation: Permutation):
    """Test __getitems__ with a custom transform."""

    def transform(batch: pa.RecordBatch) -> list:
        return batch.column("id").to_pylist()

    custom = some_permutation.with_transform(transform)
    result = custom.__getitems__([0, 1, 2])
    assert isinstance(result, list)
    assert len(result) == 3


def test_getitems_identity_permutation(mem_db):
    """Test __getitems__ on an identity permutation."""
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(10), "value": range(10)})
    )
    perm = Permutation.identity(tbl)

    result = perm.__getitems__([0, 5, 9])
    assert [r["id"] for r in result] == [0, 5, 9]
    assert [r["value"] for r in result] == [0, 5, 9]


def test_getitems_with_limit_offset(some_permutation: Permutation):
    """Test __getitems__ on a permutation with skip/take applied."""
    limited = some_permutation.with_skip(100).with_take(200)

    # Should be able to access offsets within the limited range
    result = limited.__getitems__([0, 1, 199])
    assert len(result) == 3

    # The first item of the limited permutation should match offset 100 of original
    full_result = some_permutation.__getitems__([100])
    limited_result = limited.__getitems__([0])
    assert limited_result[0]["id"] == full_result[0]["id"]


def test_getitems_invalid_offset(some_permutation: Permutation):
    """Test __getitems__ with an out-of-range offset raises an error."""
    with pytest.raises(Exception):
        some_permutation.__getitems__([999999])


def test_take_offsets(some_permutation: Permutation):
    result = some_permutation.take_offsets([0, 1, 2])

    assert isinstance(result, list)
    assert "id" in result[0]
    assert "value" in result[0]
    assert len(result) == 3


def test_take_offsets_empty_identity_permutation(mem_db):
    tbl = mem_db.create_table(
        "test_table", pa.table({"id": range(10), "value": range(10)})
    )
    permutation = Permutation.identity(tbl)

    result = permutation.take_offsets([])

    assert result == []


def test_take_offsets_empty_permutation(some_permutation: Permutation):
    result = some_permutation.take_offsets([])

    assert result == []


def test_select_rowid(some_permutation: Permutation):
    """Test that _rowid can be selected alongside regular columns."""
    perm_with_rowid = some_permutation.select_columns(["_rowid", "id"])
    assert "_rowid" in perm_with_rowid.column_names
    batches = list(perm_with_rowid.iter(100, skip_last_batch=False))
    for batch in batches:
        assert "_rowid" in batch[0]


def test_select_rowid_only(some_permutation: Permutation):
    """Test that _rowid can be selected as the sole column."""
    perm_rowid_only = some_permutation.select_columns(["_rowid"])
    assert perm_rowid_only.column_names == ["_rowid"]
    batches = list(perm_rowid_only.iter(100, skip_last_batch=False))
    assert len(batches) > 0
    for batch in batches:
        assert list(batch[0].keys()) == ["_rowid"]


def test_select_rowid_not_in_default(some_permutation: Permutation):
    """Test that _rowid is NOT in the default column_names or schema."""
    assert "_rowid" not in some_permutation.column_names
    assert "_rowid" not in some_permutation.schema.names


def test_select_rowid_identity_permutation(mem_db):
    """Test that _rowid works with an identity permutation."""
    tbl = mem_db.create_table(
        "test_rowid_identity", pa.table({"id": range(10), "value": range(10)})
    )
    perm = Permutation.identity(tbl)
    perm_with_rowid = perm.select_columns(["_rowid", "id"])
    batches = list(perm_with_rowid.iter(10, skip_last_batch=False))
    assert len(batches) == 1
    assert "_rowid" in batches[0][0]


def test_rename_rowid(some_permutation: Permutation):
    """Test that _rowid can be selected and then renamed."""
    perm_with_rowid = some_permutation.select_columns(["_rowid", "id"])
    renamed = perm_with_rowid.rename_column("_rowid", "my_row_id")
    assert "my_row_id" in renamed.column_names
    assert "_rowid" not in renamed.column_names
    batches = list(renamed.iter(100, skip_last_batch=False))
    for batch in batches:
        assert "my_row_id" in batch[0]
        assert "_rowid" not in batch[0]


def test_remove_rowid_after_select(some_permutation: Permutation):
    """Test that _rowid can be selected and then removed."""
    perm_with_rowid = some_permutation.select_columns(["_rowid", "id"])
    assert "_rowid" in perm_with_rowid.column_names
    perm_without_rowid = perm_with_rowid.remove_columns(["_rowid"])
    assert "_rowid" not in perm_without_rowid.column_names
    assert perm_without_rowid.column_names == ["id"]


def test_permutation_is_stable_when_remote_scan_order_varies():
    """Splits are assigned by scan position, and every rank builds its own
    permutation, so two ranks seeing different scan orders must still agree."""
    server = MockPermutationServer(num_rows=16, vary_scan_order=True)

    def split_of_each_row(permutation_tbl):
        # Sequential splits are assigned by position, so a reversed scan would put
        # the last rows in split 0.  Compare the mapping rather than the table order,
        # which the split-id sort does not pin down.
        rows = permutation_tbl.search(None).to_arrow().to_pydict()
        return dict(zip(rows["row_id"], rows["split_id"]))

    with mock_remote_table(server) as table:
        first = split_of_each_row(
            permutation_builder(table).split_sequential(fixed=2).execute()
        )
        second = split_of_each_row(
            permutation_builder(table).split_sequential(fixed=2).execute()
        )

    assert server.scan_calls == 2, "both builds must have scanned"
    assert first == second
    assert first[0] == 0 and first[server.num_rows - 1] == 1, first


def test_permutation_over_remote_table():
    """The permutation API accepts a remote table, addressing rows by `_rowid` just
    as `take_row_ids` does.  Also pins the request shapes sent to the server.
    """
    server = MockPermutationServer()

    with mock_remote_table(server) as table:
        permutation_tbl = permutation_builder(table).split_sequential(fixed=2).execute()
        assert permutation_tbl.count_rows() == server.num_rows

        permutation = Permutation.from_tables(table, permutation_tbl, 0)
        assert permutation.num_rows == server.num_rows // 2

        # Compare against the permutation's own order; the split-id sort is not stable.
        rows = permutation_tbl.search(None).to_arrow().to_pydict()
        split0 = [
            row_id
            for row_id, split in zip(rows["row_id"], rows["split_id"])
            if not split
        ]
        # The mock table's `id` equals its `_rowid`.
        assert permutation.take_offsets([2, 0]) == [
            {"id": split0[2]},
            {"id": split0[0]},
        ]

    assert_server_safe_row_id_requests(server)


def test_from_tables_split_without_permutation_table(mem_db):
    tbl = mem_db.create_table("test_table", pa.table({"id": range(10)}))

    with pytest.raises(ValueError, match="permutation on split `train` because no"):
        Permutation.from_tables(tbl, split="train")


def _double_value_row(row):
    return {"id": row["id"], "value": row["value"] * 2}


def _double_value_batch(batch):
    return {"id": batch["id"], "value": [value * 2 for value in batch["value"]]}


def test_map_row_by_row(some_permutation: Permutation):
    """Test .map() applies fn to each row individually."""
    mapped = some_permutation.map(_double_value_row)
    batches = list(mapped.iter(10, skip_last_batch=False))
    first_batch = batches[0]
    assert len(first_batch) == 10
    assert isinstance(first_batch[0], dict)
    for row in first_batch:
        assert row["value"] == row["id"] * 2


def test_map_batched(some_permutation: Permutation):
    """Test batched only changes what fn receives, not what is yielded."""
    seen = []

    def double_value(batch):
        seen.append(batch)
        return _double_value_batch(batch)

    mapped = some_permutation.map(double_value, batched=True)
    first_batch = next(mapped.iter(10, skip_last_batch=False))
    assert isinstance(seen[0], dict)
    assert len(seen[0]["id"]) == 10
    assert first_batch == next(
        some_permutation.map(_double_value_row).iter(10, skip_last_batch=False)
    )


def test_map_chain(some_permutation: Permutation):
    """Test chained maps run in order and each one replaces the whole row."""

    def add_doubled(row):
        return {**row, "doubled": row["value"] * 2}

    def to_total(batch):
        return {
            "total": [a + b for a, b in zip(batch["value"], batch["doubled"])],
        }

    def add_label(row):
        return {**row, "label": str(row["total"])}

    first = some_permutation.map(add_doubled)
    mapped = first.map(to_total, batched=True).map(add_label)
    expected = [
        {"total": row["value"] * 3, "label": str(row["value"] * 3)}
        for row in some_permutation.take_offsets([0, 1, 2])
    ]
    assert mapped.take_offsets([0, 1, 2]) == expected
    assert next(mapped.iter(3, skip_last_batch=False)) == expected
    # Extending a chain does not change the permutation it was derived from
    assert set(first.take_offsets([0])[0]) == {"id", "value", "doubled"}


@pytest.mark.parametrize(
    "explicit",
    [
        lambda permutation: permutation.with_format("python"),
        lambda permutation: permutation.with_format("arrow"),
        lambda permutation: permutation.with_transform(Transforms.arrow2arrow),
    ],
)
def test_map_rejects_format_and_transform(some_permutation: Permutation, explicit):
    """Test .map() cannot be mixed with with_format or with_transform."""
    with pytest.raises(ValueError, match="map cannot be combined"):
        explicit(some_permutation).map(_double_value_row)
    with pytest.raises(ValueError, match="map cannot be combined"):
        explicit(some_permutation.map(_double_value_row))


def test_map_rejects_format_without_optional_dependency(
    some_permutation: Permutation, monkeypatch
):
    """Test the map check runs before a format imports an optional dependency."""
    import sys

    # Make polars fail to import, as it does on an install without extras
    monkeypatch.setitem(sys.modules, "polars", None)
    with pytest.raises(ValueError, match="map cannot be combined"):
        some_permutation.map(_double_value_row).with_format("polars")


def test_map_rejects_explicit_transform_from_old_pickle(
    some_permutation: Permutation,
):
    """Test pickles written before explicit_transform existed keep the guard."""

    def restore_without_flag(permutation):
        state = permutation.__getstate__()
        del state["explicit_transform"]
        restored = Permutation.__new__(Permutation)
        restored.__setstate__(state)
        return restored

    with pytest.raises(ValueError, match="map cannot be combined"):
        restore_without_flag(some_permutation.with_format("arrow")).map(
            _double_value_row
        )
    # The default transform is indistinguishable from an explicit "python" format
    mapped = restore_without_flag(some_permutation).map(_double_value_row)
    assert mapped.take_offsets([0]) == [
        _double_value_row(row) for row in some_permutation.take_offsets([0])
    ]


def test_map_rejects_transform_passed_to_constructor(some_table: Table):
    """Test a transform given to the constructor counts as explicit too."""
    permutation = Permutation(
        some_table,
        None,
        0,
        {"id": "id"},
        DEFAULT_BATCH_SIZE,
        Transforms.arrow2arrow,
    )
    with pytest.raises(ValueError, match="map cannot be combined"):
        permutation.map(_double_value_row)


def test_map_take_offsets(some_permutation: Permutation):
    """Test .map() keeps the positions and duplicates of the requested offsets."""
    calls = []

    def record(batch):
        calls.append(batch)
        return batch

    mapped = some_permutation.map(_double_value_row).map(record, batched=True)
    offsets = [5, 0, 5, 2]
    expected = [
        _double_value_row(row) for row in some_permutation.take_offsets(offsets)
    ]
    assert mapped.take_offsets(offsets) == expected
    assert mapped.__getitems__(offsets) == expected

    calls.clear()
    assert mapped.take_offsets([]) == []
    assert calls == []


def test_map_validates_output(some_permutation: Permutation):
    """Test .map() rejects callback output that would break the batch shape."""
    with pytest.raises(ValueError, match="returned 1 values for column `id`"):
        some_permutation.map(
            lambda batch: {"id": batch["id"][:1]}, batched=True
        ).take_offsets([0, 1])
    with pytest.raises(TypeError, match="must return a dict of columns, got list"):
        some_permutation.map(
            lambda batch: list(batch.values()), batched=True
        ).take_offsets([0, 1])
    # A str has the right length here but is still not a column of values
    for value in (2, "ab"):
        with pytest.raises(TypeError, match="list of values for column `n`, got"):
            some_permutation.map(lambda batch: {"n": value}, batched=True).take_offsets(
                [0, 1]
            )
    with pytest.raises(
        TypeError, match="must return a dict for each row, got NoneType"
    ):
        some_permutation.map(lambda row: None).take_offsets([0, 1])
    with pytest.raises(ValueError, match="did not return the same keys for every row"):
        some_permutation.map(
            lambda row: row if row["id"] % 2 else {"id": row["id"]}
        ).map(lambda batch: batch, batched=True).take_offsets(list(range(10)))


def test_map_projection_and_schema(some_permutation: Permutation):
    """Test the selection and schema describe the read that feeds the callbacks."""

    def to_str(row):
        return {name: str(value) for name, value in row.items()}

    mapped = some_permutation.map(to_str)
    assert mapped.schema == some_permutation.schema
    assert mapped.column_names == ["id", "value"]

    for projected in (
        some_permutation.select_columns(["id"]).map(to_str),
        mapped.select_columns(["id"]),
        mapped.remove_columns(["value"]),
    ):
        assert projected.schema == some_permutation.select_columns(["id"]).schema
        assert list(projected.take_offsets([0])[0]) == ["id"]

    renamed = mapped.rename_column("value", "data")
    assert renamed.column_names == ["id", "data"]
    assert list(renamed.take_offsets([0])[0]) == ["id", "data"]


def test_map_is_lazy(some_permutation: Permutation):
    """Test .map() does not call fn until iteration."""
    call_count = [0]

    def counting_fn(row):
        call_count[0] += 1
        return row

    mapped = some_permutation.map(counting_fn)
    assert call_count[0] == 0

    mapped.__getitems__([0])
    assert call_count[0] == 1


def test_map_non_callable(some_permutation: Permutation):
    """Test .map() raises TypeError for non-callable fn."""
    with pytest.raises(TypeError, match="fn must be callable"):
        some_permutation.map("not a function")


@pytest.mark.parametrize(
    "fn, batched", [(_double_value_row, False), (_double_value_batch, True)]
)
def test_map_pickle_round_trip(some_permutation: Permutation, fn, batched):
    """Test a mapped permutation reads the same data after a pickle round trip."""
    import pickle

    mapped = some_permutation.map(fn, batched=batched)
    restored = pickle.loads(pickle.dumps(mapped))

    offsets = [3, 1, 3]
    assert restored.take_offsets(offsets) == mapped.take_offsets(offsets)
    assert restored.take_offsets(offsets)[0]["value"] == (
        some_permutation.take_offsets(offsets)[0]["value"] * 2
    )
    # The restored permutation keeps enforcing the map / format constraint
    with pytest.raises(ValueError, match="map cannot be combined"):
        restored.with_format("arrow")
    explicit = pickle.loads(pickle.dumps(some_permutation.with_format("arrow")))
    with pytest.raises(ValueError, match="map cannot be combined"):
        explicit.map(fn, batched=batched)


def test_rebatch_regroups_short_and_empty_batches():
    """_rebatch fills batches from short, empty and oversized input batches."""
    from lancedb.permutation import _rebatch

    def make(start, n):
        return pa.record_batch({"id": pa.array(range(start, start + n), pa.int64())})

    sizes = [3, 0, 4, 10, 1, 2, 4, 0]
    batches, start = [], 0
    for n in sizes:
        batches.append(make(start, n))
        start += n

    out = list(_rebatch(iter(batches), 4))
    assert [b.num_rows for b in out] == [4, 4, 4, 4, 4, 4]
    assert [v for b in out for v in b.column("id").to_pylist()] == list(range(24))
    assert all(b.schema == batches[0].schema for b in out)

    out = list(_rebatch(iter(batches[:3]), 4))
    assert [b.num_rows for b in out] == [4, 3]

    assert list(_rebatch(iter([make(0, 0)]), 4)) == []
    assert list(_rebatch(iter([]), 4)) == []


def test_rebatch_rejects_non_positive_batch_size():
    """_rebatch raises before consuming any input when batch_size <= 0."""
    from lancedb.permutation import _rebatch

    consumed = []

    def tracking():
        consumed.append(True)
        yield pa.record_batch({"id": pa.array([0], pa.int64())})

    for batch_size in (0, -1):
        with pytest.raises(ValueError, match="batch_size must be greater than 0"):
            next(_rebatch(tracking(), batch_size))
    assert consumed == []


def test_iter_rejects_non_positive_batch_size(some_permutation: Permutation):
    for batch_size in (0, -1):
        with pytest.raises(ValueError, match="batch_size must be greater than 0"):
            next(some_permutation.iter(batch_size))


def test_single_batch_rejects_multi_chunk_combine():
    """_single_batch raises instead of dropping rows Arrow cannot fit in one batch."""
    from lancedb.permutation import _single_batch

    parts = [
        pa.record_batch({"id": pa.array([0], pa.int64())}),
        pa.record_batch({"id": pa.array([1], pa.int64())}),
    ]

    class Combined:
        def to_batches(self):
            return parts

    class Table:
        num_rows = 2

        def combine_chunks(self):
            return Combined()

    with pytest.raises(ValueError, match="smaller batch_size"):
        _single_batch(Table())
