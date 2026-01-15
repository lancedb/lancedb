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


def test_aliases_in_lance_model(mem_db):
    data = [
        {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
        {"vector": [5.9, 6.5], "item": "bar", "price": 20.0},
    ]
    tbl = mem_db.create_table("items", data=data)

    class TestModel(LanceModel):
        name: str = Field(alias="item")
        price: float
        distance: float = Field(alias="_distance")

    model = (
        tbl.search([5.9, 6.5])
        .distance_type("cosine")
        .limit(1)
        .to_pydantic(TestModel)[0]
    )
    assert hasattr(model, "name")
    assert hasattr(model, "distance")
    assert model.distance < 0.01


@pytest.mark.asyncio
async def test_aliases_in_lance_model_async(mem_db_async):
    data = [
        {"vector": [8.3, 2.5], "item": "foo", "price": 12.0},
        {"vector": [7.7, 3.9], "item": "bar", "price": 11.2},
    ]
    tbl = await mem_db_async.create_table("items", data=data)

    class TestModel(LanceModel):
        name: str = Field(alias="item")
        price: float
        distance: float = Field(alias="_distance")

    model = (
        await tbl.vector_search([7.7, 3.9])
        .distance_type("cosine")
        .limit(1)
        .to_pydantic(TestModel)
    )[0]
    assert hasattr(model, "name")
    assert hasattr(model, "distance")
    assert model.distance < 0.01


# Tests for list[LanceModel] support (Issue #2683)


def test_list_of_lance_model_basic():
    """Test basic list[LanceModel] schema conversion (Issue #2683)."""

    class SubFeature(LanceModel):
        amount: int
        name: str

    class RandomFeature(LanceModel):
        email: str
        items: List[SubFeature]

    schema = RandomFeature.to_arrow_schema()

    expected_schema = pa.schema(
        [
            pa.field("email", pa.utf8(), False),
            pa.field(
                "items",
                pa.list_(
                    pa.struct(
                        [
                            pa.field("amount", pa.int64(), False),
                            pa.field("name", pa.utf8(), False),
                        ]
                    )
                ),
                False,
            ),
        ]
    )
    assert schema == expected_schema


@pytest.mark.skipif(
    sys.version_info < (3, 9),
    reason="using native type alias requires python3.9 or higher",
)
def test_list_of_lance_model_native_syntax():
    """Test list[LanceModel] with Python 3.9+ native list syntax (Issue #2683)."""

    class SubModel(LanceModel):
        value: int
        label: str

    class ParentModel(LanceModel):
        name: str
        children: list[SubModel]

    schema = ParentModel.to_arrow_schema()

    expected_schema = pa.schema(
        [
            pa.field("name", pa.utf8(), False),
            pa.field(
                "children",
                pa.list_(
                    pa.struct(
                        [
                            pa.field("value", pa.int64(), False),
                            pa.field("label", pa.utf8(), False),
                        ]
                    )
                ),
                False,
            ),
        ]
    )
    assert schema == expected_schema


def test_list_of_lance_model_deeply_nested():
    """Test deeply nested list[LanceModel] structures (3+ levels)."""

    class Level3(LanceModel):
        data: str

    class Level2(LanceModel):
        items: List[Level3]

    class Level1(LanceModel):
        groups: List[Level2]

    schema = Level1.to_arrow_schema()

    expected_schema = pa.schema(
        [
            pa.field(
                "groups",
                pa.list_(
                    pa.struct(
                        [
                            pa.field(
                                "items",
                                pa.list_(
                                    pa.struct([pa.field("data", pa.utf8(), False)])
                                ),
                                False,
                            )
                        ]
                    )
                ),
                False,
            )
        ]
    )
    assert schema == expected_schema


def test_list_of_lance_model_optional():
    """Test Optional[list[LanceModel]] produces nullable list field."""

    class Item(LanceModel):
        name: str
        quantity: int

    class Order(LanceModel):
        order_id: str
        items: Optional[List[Item]] = None

    schema = Order.to_arrow_schema()

    expected_schema = pa.schema(
        [
            pa.field("order_id", pa.utf8(), False),
            pa.field(
                "items",
                pa.list_(
                    pa.struct(
                        [
                            pa.field("name", pa.utf8(), False),
                            pa.field("quantity", pa.int64(), False),
                        ]
                    )
                ),
                True,  # nullable because Optional
            ),
        ]
    )
    assert schema == expected_schema


def test_list_of_lance_model_with_vector():
    """Test list[LanceModel] combined with Vector fields."""

    class Chunk(LanceModel):
        text: str
        embedding: Vector(128)

    class Document(LanceModel):
        title: str
        chunks: List[Chunk]

    schema = Document.to_arrow_schema()

    expected_schema = pa.schema(
        [
            pa.field("title", pa.utf8(), False),
            pa.field(
                "chunks",
                pa.list_(
                    pa.struct(
                        [
                            pa.field("text", pa.utf8(), False),
                            pa.field("embedding", pa.list_(pa.float32(), 128), True),
                        ]
                    )
                ),
                False,
            ),
        ]
    )
    assert schema == expected_schema


def test_list_of_lance_model_mixed_with_primitives():
    """Test model with both list[primitive] and list[LanceModel] fields."""

    class Tag(LanceModel):
        name: str
        score: float

    class Article(LanceModel):
        title: str
        categories: List[str]  # list of primitives
        tags: List[Tag]  # list of LanceModel
        scores: List[float]  # list of primitives

    schema = Article.to_arrow_schema()

    expected_schema = pa.schema(
        [
            pa.field("title", pa.utf8(), False),
            pa.field("categories", pa.list_(pa.utf8()), False),
            pa.field(
                "tags",
                pa.list_(
                    pa.struct(
                        [
                            pa.field("name", pa.utf8(), False),
                            pa.field("score", pa.float64(), False),
                        ]
                    )
                ),
                False,
            ),
            pa.field("scores", pa.list_(pa.float64()), False),
        ]
    )
    assert schema == expected_schema


def test_list_of_base_model():
    """Test list[pydantic.BaseModel] (not just LanceModel) works too."""

    class SubItem(pydantic.BaseModel):
        key: str
        value: int

    class Container(LanceModel):
        name: str
        items: List[SubItem]

    schema = Container.to_arrow_schema()

    expected_schema = pa.schema(
        [
            pa.field("name", pa.utf8(), False),
            pa.field(
                "items",
                pa.list_(
                    pa.struct(
                        [
                            pa.field("key", pa.utf8(), False),
                            pa.field("value", pa.int64(), False),
                        ]
                    )
                ),
                False,
            ),
        ]
    )
    assert schema == expected_schema


def test_list_of_lance_model_with_optional_fields():
    """Test list[LanceModel] where the nested model has optional fields."""

    class ItemWithOptionals(LanceModel):
        required_field: str
        optional_field: Optional[int] = None
        another_optional: Optional[str] = None

    class Parent(LanceModel):
        items: List[ItemWithOptionals]

    schema = Parent.to_arrow_schema()

    expected_schema = pa.schema(
        [
            pa.field(
                "items",
                pa.list_(
                    pa.struct(
                        [
                            pa.field("required_field", pa.utf8(), False),
                            pa.field("optional_field", pa.int64(), True),
                            pa.field("another_optional", pa.utf8(), True),
                        ]
                    )
                ),
                False,
            )
        ]
    )
    assert schema == expected_schema


def test_list_of_lance_model_integration(mem_db):
    """Integration test: create table, insert, and query with list[LanceModel]."""

    class LineItem(LanceModel):
        product: str
        quantity: int
        price: float

    class Order(LanceModel):
        order_id: str
        customer: str
        items: List[LineItem]

    # Create table with schema
    tbl = mem_db.create_table("orders", schema=Order)

    # Add data
    order1 = Order(
        order_id="ord-001",
        customer="Alice",
        items=[
            LineItem(product="Widget", quantity=2, price=10.0),
            LineItem(product="Gadget", quantity=1, price=25.0),
        ],
    )
    order2 = Order(
        order_id="ord-002",
        customer="Bob",
        items=[LineItem(product="Gizmo", quantity=5, price=5.0)],
    )

    tbl.add([order1, order2])

    # Verify data was inserted
    assert tbl.count_rows() == 2

    # Query and verify schema
    result = tbl.to_arrow()
    assert result.schema == Order.to_arrow_schema()

    # Verify data integrity
    data = result.to_pydict()
    assert data["order_id"] == ["ord-001", "ord-002"]
    assert data["customer"] == ["Alice", "Bob"]
    assert len(data["items"][0]) == 2  # First order has 2 items
    assert len(data["items"][1]) == 1  # Second order has 1 item


@pytest.mark.asyncio
async def test_list_of_lance_model_integration_async(mem_db_async):
    """Async integration test for list[LanceModel]."""

    class Comment(LanceModel):
        author: str
        text: str

    class Post(LanceModel):
        title: str
        comments: List[Comment]

    # Create table with schema
    tbl = await mem_db_async.create_table("posts", schema=Post)

    # Add data
    post = Post(
        title="Hello World",
        comments=[
            Comment(author="user1", text="Great post!"),
            Comment(author="user2", text="Thanks for sharing"),
        ],
    )

    await tbl.add([post])

    # Verify data
    assert await tbl.count_rows() == 1
    result = await tbl.to_arrow()
    assert result.schema == Post.to_arrow_schema()
