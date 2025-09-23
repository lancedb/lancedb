# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json
import sys
from datetime import date, datetime
from typing import List, Optional, Tuple

import pyarrow as pa
import pydantic
import pytest
from lancedb.pydantic import (
    PYDANTIC_VERSION,
    LanceModel,
    Vector,
    pydantic_to_schema,
    MultiVector,
)
from pydantic import BaseModel
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


def test_nullable_vector():
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


def test_fixed_size_list_field():
    class TestModel(pydantic.BaseModel):
        vec: Vector(16)
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
        vec: Vector(8)

    with pytest.raises(pydantic.ValidationError):
        TestModel(vec=range(9))

    with pytest.raises(pydantic.ValidationError):
        TestModel(vec=range(7))

    TestModel(vec=range(8))


def test_lance_model():
    class TestModel(LanceModel):
        vector: Vector(16) = Field(default=[0.0] * 16)
        li: List[int] = Field(default=[1, 2, 3])

    schema = pydantic_to_schema(TestModel)
    assert schema == TestModel.to_arrow_schema()
    assert TestModel.field_names() == ["vector", "li"]

    t = TestModel()
    assert t == TestModel(vec=[0.0] * 16, li=[1, 2, 3])


def test_optional_nested_model():
    class WAMedia(BaseModel):
        url: str
        mimetype: str
        filename: Optional[str]
        error: Optional[str]
        data: bytes

    class WALocation(BaseModel):
        description: Optional[str]
        latitude: str
        longitude: str

    class ReplyToMessage(BaseModel):
        id: str
        participant: str
        body: str

    class Message(BaseModel):
        id: str
        timestamp: int
        from_: str
        fromMe: bool
        to: str
        body: str
        hasMedia: Optional[bool]
        media: WAMedia
        mediaUrl: Optional[str]
        ack: Optional[int]
        ackName: Optional[str]
        author: Optional[str]
        location: Optional[WALocation]
        vCards: Optional[List[str]]
        replyTo: Optional[ReplyToMessage]

    class AnyEvent(LanceModel):
        id: str
        session: str
        metadata: Optional[str] = None
        engine: str
        event: str

    class MessageEvent(AnyEvent):
        payload: Message

    schema = pydantic_to_schema(MessageEvent)

    payload = schema.field("payload")
    assert payload.type == pa.struct(
        [
            pa.field("id", pa.utf8(), False),
            pa.field("timestamp", pa.int64(), False),
            pa.field("from_", pa.utf8(), False),
            pa.field("fromMe", pa.bool_(), False),
            pa.field("to", pa.utf8(), False),
            pa.field("body", pa.utf8(), False),
            pa.field("hasMedia", pa.bool_(), True),
            pa.field(
                "media",
                pa.struct(
                    [
                        pa.field("url", pa.utf8(), False),
                        pa.field("mimetype", pa.utf8(), False),
                        pa.field("filename", pa.utf8(), True),
                        pa.field("error", pa.utf8(), True),
                        pa.field("data", pa.binary(), False),
                    ]
                ),
                False,
            ),
            pa.field("mediaUrl", pa.utf8(), True),
            pa.field("ack", pa.int64(), True),
            pa.field("ackName", pa.utf8(), True),
            pa.field("author", pa.utf8(), True),
            pa.field(
                "location",
                pa.struct(
                    [
                        pa.field("description", pa.utf8(), True),
                        pa.field("latitude", pa.utf8(), False),
                        pa.field("longitude", pa.utf8(), False),
                    ]
                ),
                True,  # Optional
            ),
            pa.field("vCards", pa.list_(pa.utf8()), True),
            pa.field(
                "replyTo",
                pa.struct(
                    [
                        pa.field("id", pa.utf8(), False),
                        pa.field("participant", pa.utf8(), False),
                        pa.field("body", pa.utf8(), False),
                    ]
                ),
                True,
            ),
        ]
    )


def test_multi_vector():
    class TestModel(pydantic.BaseModel):
        vec: MultiVector(8)

    schema = pydantic_to_schema(TestModel)
    assert schema == pa.schema(
        [pa.field("vec", pa.list_(pa.list_(pa.float32(), 8)), True)]
    )

    with pytest.raises(pydantic.ValidationError):
        TestModel(vec=[[1.0] * 7])

    with pytest.raises(pydantic.ValidationError):
        TestModel(vec=[[1.0] * 9])

    TestModel(vec=[[1.0] * 8])
    TestModel(vec=[[1.0] * 8, [2.0] * 8])

    TestModel(vec=[])


def test_multi_vector_nullable():
    class NullableModel(pydantic.BaseModel):
        vec: MultiVector(16, nullable=False)

    schema = pydantic_to_schema(NullableModel)
    assert schema == pa.schema(
        [pa.field("vec", pa.list_(pa.list_(pa.float32(), 16)), False)]
    )

    class DefaultModel(pydantic.BaseModel):
        vec: MultiVector(16)

    schema = pydantic_to_schema(DefaultModel)
    assert schema == pa.schema(
        [pa.field("vec", pa.list_(pa.list_(pa.float32(), 16)), True)]
    )


def test_multi_vector_in_lance_model():
    class TestModel(LanceModel):
        id: int
        vectors: MultiVector(16) = Field(default=[[0.0] * 16])

    schema = pydantic_to_schema(TestModel)
    assert schema == TestModel.to_arrow_schema()
    assert TestModel.field_names() == ["id", "vectors"]

    t = TestModel(id=1)
    assert t.vectors == [[0.0] * 16]


@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="StrEnum where only introduced in python 3.11",
)
def test_str_enum():
    from enum import StrEnum

    class Status(StrEnum):
        Pending = "pending"
        Running = "running"
        Finished = "finished"

    class TestModel(pydantic.BaseModel):
        status: Status

    schema = pydantic_to_schema(TestModel)

    expect_schema = pa.schema(
        [
            pa.field("status", pa.dictionary(pa.int16(), pa.string()), False),
        ]
    )
    assert schema == expect_schema
