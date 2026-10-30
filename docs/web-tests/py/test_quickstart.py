# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import os
import shutil
import uuid
from pathlib import Path

import lancedb
import pytest
import pytest_asyncio

# What the quickstart's download step leaves in an application's directory.
IMAGE = (
    Path(__file__).resolve().parents[2]
    / "web/static/assets/images/quickstart/sir-lancelot.jpg"
)
DEPLOYMENT = ("LANCEDB_URI", "LANCEDB_HOST_OVERRIDE", "LANCEDB_API_KEY")
# The connection settings alone never send these tests to a deployment.
OPT_IN = "LANCEDB_DOCS_TEST_DEPLOYMENT"


def deployment_settings():
    if os.environ.get(OPT_IN) != "1":
        pytest.skip(f"set {OPT_IN}=1 and {', '.join(DEPLOYMENT)} to use a deployment")
    missing = [name for name in DEPLOYMENT if not os.environ.get(name)]
    if missing:
        pytest.fail(f"{OPT_IN}=1 needs {', '.join(missing)}")
    return dict(
        uri=os.environ["LANCEDB_URI"],
        api_key=os.environ["LANCEDB_API_KEY"],
        region=os.environ.get("LANCEDB_REGION", "us-east-1"),
        host_override=os.environ["LANCEDB_HOST_OVERRIDE"],
    )


class OwnedTables:
    """A deployment connection on which a test creates only tables of its own.

    Each name gets a suffix unique to the test and is claimed only while no
    table has it; the test drops what it claimed, whether or not it passed.
    """

    def __init__(self, db):
        self.db = db
        self.suffix = uuid.uuid4().hex[:12]
        self.claimed = []

    def claim(self, name, existing, mode):
        owned = f"{name}_{self.suffix}"
        if owned in self.claimed:
            return owned, mode
        if owned in existing:
            raise RuntimeError(f"{owned} already exists and is not this test's")
        self.claimed.append(owned)
        return owned, "create"

    def table_names(self):
        names, token = [], None
        while True:
            page = self.db.list_tables(page_token=token)
            names += page.tables
            if not (token := page.page_token):
                return names

    def create_table(self, name, *args, mode="create", **kwargs):
        owned, mode = self.claim(name, self.table_names(), mode)
        return self.db.create_table(owned, *args, mode=mode, **kwargs)

    def drop_claimed(self):
        existing = self.table_names()
        for name in self.claimed:
            if name in existing:
                self.db.drop_table(name)


class AsyncOwnedTables(OwnedTables):
    async def table_names(self):
        names, token = [], None
        while True:
            page = await self.db.list_tables(page_token=token)
            names += page.tables
            if not (token := page.page_token):
                return names

    async def create_table(self, name, *args, mode="create", **kwargs):
        owned, mode = self.claim(name, await self.table_names(), mode)
        return await self.db.create_table(owned, *args, mode=mode, **kwargs)

    async def drop_claimed(self):
        existing = await self.table_names()
        for name in self.claimed:
            if name in existing:
                await self.db.drop_table(name)


@pytest.fixture
def deployment():
    owned = OwnedTables(lancedb.connect(**deployment_settings()))
    yield owned
    owned.drop_claimed()


@pytest.fixture(params=["local", "enterprise"])
def db(request, db_path_factory):
    if request.param == "local":
        yield lancedb.connect(db_path_factory("quickstart_db"))
    else:
        yield request.getfixturevalue("deployment")


@pytest_asyncio.fixture(params=["local", "enterprise"])
async def async_db(request, db_path_factory):
    if request.param == "local":
        yield await lancedb.connect_async(db_path_factory("quickstart_async_db"))
        return
    owned = AsyncOwnedTables(await lancedb.connect_async(**deployment_settings()))
    yield owned
    await owned.drop_claimed()


def create_frames(db):
    # --8<-- [start:quickstart_data]
    data = [
        {
            "frame_id": "1",
            "video_id": "clip-1",
            "caption": "a robot arm reaching toward a cup",
            "stats": {"motion": 3, "sharpness": 4, "exposure": 4, "framing": 3},
            "embedding": [0.7, 0.1, 0.9, 0.7],
        },
        {
            "frame_id": "2",
            "video_id": "clip-1",
            "caption": "a two-finger gripper closing on a cup",
            "stats": {"motion": 5, "sharpness": 4, "exposure": 5, "framing": 2},
            "embedding": [0.2, 0.9, 0.4, 0.9],
        },
        {
            "frame_id": "3",
            "video_id": "clip-2",
            "caption": "a robot arm holding a cup steady",
            "stats": {"motion": 2, "sharpness": 3, "exposure": 3, "framing": 4},
            "embedding": [0.9, 0.1, 0.5, 0.4],
        },
    ]
    # --8<-- [end:quickstart_data]

    # --8<-- [start:quickstart_create_table]
    table = db.create_table("frames", data=data, mode="overwrite")
    # --8<-- [end:quickstart_create_table]
    assert table.count_rows() == 3

    # --8<-- [start:quickstart_add_data]
    table.add(
        [
            {
                "frame_id": "4",
                "video_id": "clip-2",
                "caption": "a gripper lowering a cup onto the table",
                "stats": {"motion": 5, "sharpness": 4, "exposure": 4, "framing": 2},
                "embedding": [0.3, 0.9, 0.6, 0.8],
            }
        ]
    )
    # --8<-- [end:quickstart_add_data]
    assert table.count_rows() == 4
    return table


def test_quickstart(db):
    table = create_frames(db)

    # --8<-- [start:quickstart_vector_search_1]
    # Search for frames similar to "a gripper closing on an object"
    query_vector = [0.2, 0.8, 0.4, 0.9]

    # Ensure you run `pip install polars` beforehand
    result = (
        table.search(query_vector)
        .select(["frame_id", "video_id", "caption", "_distance"])
        .limit(2)
        .to_polars()
    )
    print(result)
    # --8<-- [end:quickstart_vector_search_1]
    assert result["frame_id"].to_list() == ["2", "4"]

    # --8<-- [start:quickstart_output_pandas]
    # Ensure you run `pip install pandas` beforehand
    result = table.search(query_vector).limit(2).to_pandas()
    print(result)
    # --8<-- [end:quickstart_output_pandas]
    assert result["frame_id"].to_list() == ["2", "4"]


@pytest.mark.asyncio
async def test_quickstart_async_api(async_db):
    data = [
        {
            "frame_id": "2",
            "video_id": "clip-1",
            "caption": "a two-finger gripper closing on a cup",
            "stats": {"motion": 5, "sharpness": 4, "exposure": 5, "framing": 2},
            "embedding": [0.2, 0.9, 0.4, 0.9],
        }
    ]

    # --8<-- [start:quickstart_create_table_async]
    async_table = await async_db.create_table(
        "frames",
        data=data,
        mode="overwrite",
    )
    # --8<-- [end:quickstart_create_table_async]

    # --8<-- [start:quickstart_vector_search_1_async]
    # Search for frames similar to "a gripper closing on an object"
    query_vector = [0.2, 0.8, 0.4, 0.9]

    # Ensure you run `pip install polars` beforehand
    async_result = (
        await (await async_table.search(query_vector))
        .select(["frame_id", "video_id", "caption", "_distance"])
        .limit(2)
        .to_polars()
    )
    print(async_result)
    # --8<-- [end:quickstart_vector_search_1_async]
    assert async_result["frame_id"].to_list() == ["2"]


def test_quickstart_curate_with_sql(deployment):
    table = create_frames(deployment)
    db = deployment.db

    # --8<-- [start:quickstart_curate_sample]
    sample = db.execute_query(
        "SELECT frame_id FROM frames TABLESAMPLE (2 ROWS)"
    ).read_all()
    print(sample.to_pylist())
    # --8<-- [end:quickstart_curate_sample]
    assert sample.num_rows == 2

    # --8<-- [start:quickstart_curate_dedupe_setup]
    # Frame 5 is a near-duplicate of frame 2: the gripper is still closing,
    # one video frame later, with an almost identical embedding.
    table.add(
        [
            {
                "frame_id": "5",
                "video_id": "clip-1",
                "caption": "the two-finger gripper still closing on the cup",
                "stats": {"motion": 5, "sharpness": 4, "exposure": 5, "framing": 2},
                "embedding": [0.2, 0.9, 0.4, 0.89],
            }
        ]
    )
    from lancedb.index import IvfFlat

    table.create_index(
        "embedding", config=IvfFlat(num_partitions=1, distance_type="l2")
    )
    table.wait_for_index(["embedding_idx"])
    # --8<-- [end:quickstart_curate_dedupe_setup]

    # --8<-- [start:quickstart_curate_dedupe]
    db.execute_query("""
        CREATE MATERIALIZED VIEW frames_deduped AS
        SELECT * FROM dedupe(
            'frames',
            'embedding',
            distance_threshold => 0.03
        )
    """)
    # --8<-- [end:quickstart_curate_dedupe]

    # --8<-- [start:quickstart_curate_dedupe_query]
    deduped = db.execute_query(
        "SELECT frame_id FROM frames_deduped ORDER BY frame_id"
    ).read_all()
    print(deduped.to_pylist())
    # --8<-- [end:quickstart_curate_dedupe_query]
    assert deduped.column("frame_id").to_pylist() in (
        ["1", "2", "3", "4"],
        ["1", "3", "4", "5"],
    )


def quality_scores(table):
    rows = table.search().select(["frame_id", "video_id", "quality_score"]).to_list()
    return {row["frame_id"]: row["quality_score"] for row in rows}


EXPECTED_QUALITY_SCORES = {"1": 3.5, "2": 4.0, "3": 3.0, "4": 3.75}


def flatten_stats(table):
    table.add_columns(
        {
            "motion": "cast(stats.motion as float)",
            "sharpness": "cast(stats.sharpness as float)",
            "exposure": "cast(stats.exposure as float)",
            "framing": "cast(stats.framing as float)",
        }
    )


def test_quickstart_derived_feature(db_path_factory):
    db = lancedb.connect(db_path_factory("derived_feature"))
    table = create_frames(db)
    flatten_stats(table)

    # --8<-- [start:quickstart_add_feature]
    from lancedb import col, udf

    @udf
    def quality_score(
        motion: float, sharpness: float, exposure: float, framing: float
    ) -> float:
        return (motion + sharpness + exposure + framing) / 4.0

    version = db.create_function_async(quality_score).wait()
    table.add_columns(
        {
            "quality_score": version(
                motion=col("motion"),
                sharpness=col("sharpness"),
                exposure=col("exposure"),
                framing=col("framing"),
            )
        }
    )
    table.refresh_column("quality_score")
    # --8<-- [end:quickstart_add_feature]

    # --8<-- [start:quickstart_query_feature]
    features = (
        table.search().select(["frame_id", "video_id", "quality_score"]).to_polars()
    )
    print(features)
    # --8<-- [end:quickstart_query_feature]
    assert quality_scores(table) == EXPECTED_QUALITY_SCORES


def test_quickstart_derived_feature_on_a_deployment(deployment):
    table = create_frames(deployment)
    flatten_stats(table)

    from lancedb import col, udf

    @udf
    def quality_score(
        motion: float, sharpness: float, exposure: float, framing: float
    ) -> float:
        return (motion + sharpness + exposure + framing) / 4.0

    version = deployment.db.create_function_async(quality_score).wait()
    table.add_columns(
        {
            "quality_score": version(
                motion=col("motion"),
                sharpness=col("sharpness"),
                exposure=col("exposure"),
                framing=col("framing"),
            )
        }
    )

    # --8<-- [start:quickstart_add_feature_enterprise]
    job = table.refresh_column_async("quality_score")
    job.wait()
    # --8<-- [end:quickstart_add_feature_enterprise]
    assert quality_scores(table) == EXPECTED_QUALITY_SCORES
    with pytest.raises(Exception):
        table.refresh_column("quality_score")


def test_quickstart_multimodal(db, tmp_path, monkeypatch):
    app = tmp_path / "app"
    (app / "data").mkdir(parents=True)
    shutil.copy(IMAGE, app / "data/sample-frame.jpg")
    monkeypatch.chdir(app)

    # --8<-- [start:quickstart_multimodal_bytes]
    from pathlib import Path

    image_bytes = Path("data/sample-frame.jpg").read_bytes()

    multimodal_table = db.create_table(
        "frame_images",
        data=[
            {
                "frame_id": "thumb-1",
                "caption": "Sample frame thumbnail",
                "image": image_bytes,
                "embedding": [0.9, 0.1, 0.5, 0.4],
            }
        ],
        mode="overwrite",
    )
    # --8<-- [end:quickstart_multimodal_bytes]
    assert multimodal_table.count_rows() == 1
    stored = multimodal_table.search().select(["image"]).to_list()[0]["image"]
    assert stored == IMAGE.read_bytes()


def test_quickstart_training(db):
    table = create_frames(db)

    # --8<-- [start:quickstart_training]
    import torch
    from lancedb.streaming import StreamingDataset

    table.add_columns(
        {
            "motion": "cast(stats.motion as float)",
            "sharpness": "cast(stats.sharpness as float)",
        }
    )
    dataset = StreamingDataset(table, columns=["motion", "sharpness"], shuffle_seed=42)
    dataloader = torch.utils.data.DataLoader(dataset, batch_size=1024, num_workers=0)
    # --8<-- [end:quickstart_training]
    batches = list(dataloader)
    assert len(batches) == 1
    assert set(batches[0]) == {"motion", "sharpness"}
    assert all(isinstance(value, torch.Tensor) for value in batches[0].values())
    assert sorted(batches[0]["motion"].tolist()) == [2.0, 3.0, 5.0, 5.0]
