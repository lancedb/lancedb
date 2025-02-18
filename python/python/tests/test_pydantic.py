# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json
import sys
from datetime import date, datetime
from typing import Annotated, List, Optional, Tuple

import pyarrow as pa
import pydantic
import pytest
from lancedb.pydantic import (
    PYDANTIC_VERSION,
    FixedSizeList,
    LanceModel,
    Vector,
    pydantic_to_schema,
)
from pydantic import Field


@pytest.mark.skipif(
    sys.version_info < (3, 9),
    reason="using native type alias requires python3.9 or higher",
)
def test_pydantic_to_arrow():
    class StructModel(pydantic.BaseModel):
        a: str
        b: Optional[float]

    class TestModel(pydantic.BaseModel):
        id: int
        s: str
        vec: list[float]
        li: list[int]
        lili: list[list[float]]
        litu: list[tuple[float, float]]
        opt: Optional[str] = None
        st: StructModel
        dt: date
        dtt: datetime
        dt_with_tz: datetime = Field(json_schema_extra={"tz": "Asia/Shanghai"})
        # d: dict

    # TODO: test we can actually convert the model into data.
    # m = TestModel(
    #     id=1,
    #     s="hello",
    #     vec=[1.0, 2.0, 3.0],
    #     li=[2, 3, 4],
    #     lili=[[2.5, 1.5], [3.5, 4.5], [5.5, 6.5]],
    #     litu=[(2.5, 1.5), (3.5, 4.5), (5.5, 6.5)],
    #     st=StructModel(a="a", b=1.0),
    #     dt=date.today(),
    #     dtt=datetime.now(),
    #     dt_with_tz=datetime.now(pytz.timezone("Asia/Shanghai")),
    # )

    schema = pydantic_to_schema(TestModel)

    expect_schema = pa.schema(
        [
            pa.field("id", pa.int64(), False),
            pa.field("s", pa.utf8(), False),
            pa.field("vec", pa.list_(pa.float64()), False),
            pa.field("li", pa.list_(pa.int64()), False),
            pa.field("lili", pa.list_(pa.list_(pa.float64())), False),
            pa.field("litu", pa.list_(pa.list_(pa.float64())), False),
            pa.field("opt", pa.utf8(), True),
            pa.field(
                "st",
                pa.struct(
                    [pa.field("a", pa.utf8(), False), pa.field("b", pa.float64(), True)]
                ),
                False,
            ),
            pa.field("dt", pa.date32(), False),
            pa.field("dtt", pa.timestamp("us"), False),
            pa.field("dt_with_tz", pa.timestamp("us", tz="Asia/Shanghai"), False),
        ]
    )
    assert schema == expect_schema


@pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="using | type syntax requires python3.10 or higher",
)
def test_optional_types_py310():
    class TestModel(pydantic.BaseModel):
        a: str | None
        b: None | str
        c: Optional[str]

    schema = pydantic_to_schema(TestModel)

    expect_schema = pa.schema(
        [
            pa.field("a", pa.utf8(), True),
            pa.field("b", pa.utf8(), True),
            pa.field("c", pa.utf8(), True),
        ]
    )
    assert schema == expect_schema


@pytest.mark.skipif(
    sys.version_info > (3, 8),
    reason="using native type alias requires python3.9 or higher",
)
def test_pydantic_to_arrow_py38():
    class StructModel(pydantic.BaseModel):
        a: str
        b: Optional[float]

    class TestModel(pydantic.BaseModel):
        id: int
        s: str
        vec: List[float]
        li: List[int]
        lili: List[List[float]]
        litu: List[Tuple[float, float]]
        opt: Optional[str] = None
        st: StructModel
        dt: date
        dtt: datetime
        dt_with_tz: datetime = Field(json_schema_extra={"tz": "Asia/Shanghai"})
        # d: dict

    # TODO: test we can actually convert the model to Arrow data.
    # m = TestModel(
    #     id=1,
    #     s="hello",
    #     vec=[1.0, 2.0, 3.0],
    #     li=[2, 3, 4],
    #     lili=[[2.5, 1.5], [3.5, 4.5], [5.5, 6.5]],
    #     litu=[(2.5, 1.5), (3.5, 4.5), (5.5, 6.5)],
    #     st=StructModel(a="a", b=1.0),
    #     dt=date.today(),
    #     dtt=datetime.now(),
    #     dt_with_tz=datetime.now(pytz.timezone("Asia/Shanghai")),
    # )

    schema = pydantic_to_schema(TestModel)

    expect_schema = pa.schema(
        [
            pa.field("id", pa.int64(), False),
            pa.field("s", pa.utf8(), False),
            pa.field("vec", pa.list_(pa.float64()), False),
            pa.field("li", pa.list_(pa.int64()), False),
            pa.field("lili", pa.list_(pa.list_(pa.float64())), False),
            pa.field("litu", pa.list_(pa.list_(pa.float64())), False),
            pa.field("opt", pa.utf8(), True),
            pa.field(
                "st",
                pa.struct(
                    [pa.field("a", pa.utf8(), False), pa.field("b", pa.float64(), True)]
                ),
                False,
            ),
            pa.field("dt", pa.date32(), False),
            pa.field("dtt", pa.timestamp("us"), False),
            pa.field("dt_with_tz", pa.timestamp("us", tz="Asia/Shanghai"), False),
        ]
    )
    assert schema == expect_schema


def test_nullable_vector_old_style():
    class NullableModel(pydantic.BaseModel):
        vec: Vector(16, nullable=False)

    schema = pydantic_to_schema(NullableModel)
    assert schema == pa.schema([pa.field("vec", pa.list_(pa.float32(), 16), False)])

    class DefaultModel(pydantic.BaseModel):
        vec: Vector(16)

    schema = pydantic_to_schema(DefaultModel)
    assert schema == pa.schema([pa.field("vec", pa.list_(pa.float32(), 16), True)])

    class NotNullableModel(pydantic.BaseModel):
        vec: Vector(16)

    schema = pydantic_to_schema(NotNullableModel)
    assert schema == pa.schema([pa.field("vec", pa.list_(pa.float32(), 16), True)])


def test_nullable_vector():
    class NullableModel(pydantic.BaseModel):
        vec: Annotated[FixedSizeList, Vector(dim=16, nullable=False)]

    schema = pydantic_to_schema(NullableModel)
    assert schema == pa.schema([pa.field("vec", pa.list_(pa.float32(), 16), False)])

    class DefaultModel(pydantic.BaseModel):
        vec: Annotated[FixedSizeList, Vector(dim=16)]

    schema = pydantic_to_schema(DefaultModel)
    assert schema == pa.schema([pa.field("vec", pa.list_(pa.float32(), 16), True)])

    class NotNullableModel(pydantic.BaseModel):
        vec: Annotated[FixedSizeList, Vector(dim=16)]

    schema = pydantic_to_schema(NotNullableModel)
    assert schema == pa.schema([pa.field("vec", pa.list_(pa.float32(), 16), True)])


def test_fixed_size_list_field():
    class TestModel(pydantic.BaseModel):
        vec: Annotated[FixedSizeList, Vector(dim=16)]
        li: List[int]

    data = TestModel(vec=list(range(16)), li=[1, 2, 3])
    if PYDANTIC_VERSION.major >= 2:
        assert json.loads(data.model_dump_json()) == {
            "vec": list(range(16)),
            "li": [1, 2, 3],
        }
    else:
        assert data.dict() == {
            "vec": list(range(16)),
            "li": [1, 2, 3],
        }

    schema = pydantic_to_schema(TestModel)
    assert schema == pa.schema(
        [
            pa.field("vec", pa.list_(pa.float32(), 16)),
            pa.field("li", pa.list_(pa.int64()), False),
        ]
    )

    if PYDANTIC_VERSION.major >= 2:
        json_schema = TestModel.model_json_schema()
    else:
        json_schema = TestModel.schema()

    assert json_schema == {
        "properties": {
            "vec": {
                "items": {"type": "number"},
                "maxItems": 16,
                "minItems": 16,
                "title": "Vec",
                "type": "array",
            },
            "li": {"items": {"type": "integer"}, "title": "Li", "type": "array"},
        },
        "required": ["vec", "li"],
        "title": "TestModel",
        "type": "object",
    }


def test_fixed_size_list_validation():
    class TestModel(pydantic.BaseModel):
        vec: Annotated[FixedSizeList, Vector(dim=8)]

    with pytest.raises(pydantic.ValidationError):
        TestModel(vec=range(9))

    with pytest.raises(pydantic.ValidationError):
        TestModel(vec=range(7))

    TestModel(vec=range(8))


def test_lance_model():
    class TestModel(LanceModel):
        vector: Annotated[FixedSizeList, Vector(16)] = Field(default=[0.0] * 16)
        li: List[int] = Field(default=[1, 2, 3])

    schema = pydantic_to_schema(TestModel)
    assert schema == TestModel.to_arrow_schema()
    assert TestModel.field_names() == ["vector", "li"]

    t = TestModel()
    assert t == TestModel(vec=[0.0] * 16, li=[1, 2, 3])
