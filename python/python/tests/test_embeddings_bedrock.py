# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import base64
import io
import json

import boto3
from botocore.response import StreamingBody
from botocore.stub import ANY, Stubber
from PIL import Image, UnidentifiedImageError
from pydantic import ValidationError
import pytest

import lancedb
from lancedb.embeddings import get_registry
from lancedb.pydantic import LanceModel, Vector


TITAN_IMAGE = "amazon.titan-embed-image-v1"
COHERE_IMAGE = "cohere.embed-v4:0"


@pytest.fixture
def bedrock_runtime(monkeypatch):
    runtime = boto3.Session(
        aws_access_key_id="stub-access-key",
        aws_secret_access_key="stub-secret-key",
        region_name="us-east-1",
    ).client("bedrock-runtime")
    monkeypatch.setattr(boto3.Session, "client", lambda *args, **kwargs: runtime)
    return runtime


def stub_embedding(stub, model_id, response):
    data = json.dumps(response).encode("utf-8")
    stub.add_response(
        "invoke_model",
        {
            "body": StreamingBody(io.BytesIO(data), len(data)),
            "contentType": "application/json",
        },
        {
            "modelId": model_id,
            "body": ANY,
            "contentType": "application/json",
            "accept": "application/json",
        },
    )


def encode_image(image, image_format):
    with io.BytesIO() as buffer:
        image.save(buffer, format=image_format)
        return buffer.getvalue()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "model_id, dimensions", [(TITAN_IMAGE, 1024), (COHERE_IMAGE, 1536)]
)
@pytest.mark.parametrize("source_type", [bytes, str], ids=["bytes", "paths"])
async def test_bedrock_image_search(
    tmp_path, bedrock_runtime, mocker, model_id, dimensions, source_type
):
    db_path = tmp_path / "database"
    db = lancedb.connect(db_path)
    kwargs = {"name": model_id} if model_id != TITAN_IMAGE else {}
    model = (
        get_registry()
        .get("bedrock-image")
        .create(region="eu-west-1", max_retries=0, **kwargs)
    )
    assert model.ndims() == dimensions

    class Photo(LanceModel):
        label: str
        image: source_type = model.SourceField()
        vector: Vector(dimensions) = model.VectorField()

    red_image = Image.new("RGB", (64, 64), "red")
    red_png = encode_image(red_image, "PNG")
    blue_image = Image.new("RGB", (64, 64), "blue")
    blue_jpeg = encode_image(blue_image, "JPEG")
    images = {"red": red_png, "blue": blue_jpeg}
    cohere_only_images = {}
    if model_id == COHERE_IMAGE:
        cohere_only_images = {
            "webp": encode_image(blue_image, "WEBP"),
            "gif": encode_image(blue_image, "GIF"),
        }
        images.update(cohere_only_images)
    sources = images.copy()
    if source_type is str:
        # The contents, not the file suffix, determine the request MIME type.
        for label, encoded_image in images.items():
            path = tmp_path / f"{label}.image"
            path.write_bytes(encoded_image)
            sources[label] = str(path)

    red = [1.0, 0.0] + [0.0] * (dimensions - 2)
    blue = [0.0, 1.0] + [0.0] * (dimensions - 2)
    other = [0.0, 0.0, 1.0] + [0.0] * (dimensions - 3)
    invoke = mocker.spy(bedrock_runtime, "invoke_model")
    session = mocker.spy(boto3, "Session")
    with Stubber(bedrock_runtime) as stub:
        for vector in (red, blue, *([other] * len(cohere_only_images)), blue, red, red):
            if model_id == TITAN_IMAGE:
                stub_embedding(stub, model_id, {"embedding": vector})
            else:
                stub_embedding(stub, model_id, {"embeddings": {"float": [vector]}})

        table = db.create_table("photos", schema=Photo)
        table.add(
            [{"label": label, "image": image} for label, image in sources.items()]
        )
        table = db.open_table("photos")
        restored = table.embedding_functions["vector"].function
        assert (restored.name, restored.region, restored.max_retries) == (
            model_id,
            "eu-west-1",
            0,
        )
        assert table.schema.field("vector").type.list_size == dimensions
        assert table.search("blue").limit(1).to_list()[0]["label"] == "blue"
        assert table.search(red_image).limit(1).to_list()[0]["label"] == "red"

        async_db = await lancedb.connect_async(db_path)
        async_table = await async_db.open_table("photos")
        query = await async_table.search(red_image, query_type="vector")
        assert (await query.limit(1).to_list())[0]["label"] == "red"
        stub.assert_no_pending_responses()

    assert session.call_args_list
    assert all(not call.args and not call.kwargs for call in session.call_args_list)
    red_base64 = base64.b64encode(red_png).decode("ascii")
    blue_base64 = base64.b64encode(blue_jpeg).decode("ascii")
    if model_id == TITAN_IMAGE:
        red_query = {"inputImage": red_base64}
        expected = [
            {"inputImage": red_base64},
            {"inputImage": blue_base64},
            {"inputText": "blue"},
            red_query,
            red_query,
        ]
    else:
        red_uri = f"data:image/png;base64,{red_base64}"
        blue_uri = f"data:image/jpeg;base64,{blue_base64}"
        red_query = {"images": [red_uri], "input_type": "search_query"}
        additional_documents = [
            {
                "images": [
                    f"data:image/{image_format};base64,"
                    f"{base64.b64encode(image).decode('ascii')}"
                ],
                "input_type": "search_document",
            }
            for image_format, image in cohere_only_images.items()
        ]
        expected = [
            {"images": [red_uri], "input_type": "search_document"},
            {"images": [blue_uri], "input_type": "search_document"},
            *additional_documents,
            {"texts": ["blue"], "input_type": "search_query"},
            red_query,
            red_query,
        ]
        expected = [{**payload, "embedding_types": ["float"]} for payload in expected]
    assert [
        json.loads(call.kwargs["body"]) for call in invoke.call_args_list
    ] == expected


def test_bedrock_image_retry_does_not_replay_successful_rows(
    tmp_path, bedrock_runtime, mocker
):
    sleep = mocker.patch("lancedb.embeddings.utils.time.sleep")
    model = get_registry().get("bedrock-image").create(max_retries=1)

    class Photo(LanceModel):
        image: bytes | None = model.SourceField()
        vector: Vector(model.ndims()) | None = model.VectorField()

    table = lancedb.connect(tmp_path).create_table("photos", schema=Photo)
    red_png = encode_image(Image.new("RGB", (64, 64), "red"), "PNG")
    blue_png = encode_image(Image.new("RGB", (64, 64), "blue"), "PNG")
    red = [1.0] + [0.0] * 1023
    blue = [0.0, 1.0] + [0.0] * 1022
    invoke = mocker.spy(bedrock_runtime, "invoke_model")
    with Stubber(bedrock_runtime) as stub:
        stub_embedding(stub, TITAN_IMAGE, {"embedding": red})
        stub.add_client_error(
            "invoke_model",
            service_error_code="ThrottlingException",
            http_status_code=429,
        )
        stub_embedding(stub, TITAN_IMAGE, {"embedding": blue})
        table.add(
            [{"image": red_png}, {"image": None}, {"image": blue_png}],
            on_bad_vectors="null",
        )
        stub.assert_no_pending_responses()

    sleep.assert_called_once()
    assert [
        base64.b64decode(json.loads(call.kwargs["body"])["inputImage"])
        for call in invoke.call_args_list
    ] == [red_png, blue_png, blue_png]
    assert table.to_arrow()["vector"].to_pylist() == [red, None, blue]


def test_bedrock_image_local_error_does_not_retry(tmp_path, mocker):
    sleep = mocker.patch("lancedb.embeddings.utils.time.sleep")
    session = mocker.patch.object(boto3, "Session")
    model = get_registry().get("bedrock-image").create(max_retries=1)

    class Photo(LanceModel):
        image: bytes = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    table = lancedb.connect(tmp_path).create_table("photos", schema=Photo)
    with pytest.raises(RuntimeError, match="UnidentifiedImageError"):
        table.add([{"image": b"invalid image bytes"}])
    assert table.count_rows() == 0

    with pytest.raises(UnidentifiedImageError):
        model.compute_source_embeddings_with_retry(b"invalid image bytes")
    gif = encode_image(Image.new("RGB", (64, 64)), "GIF")
    with pytest.raises(ValueError, match="accepts PNG and JPEG images"):
        model.compute_source_embeddings_with_retry(gif)
    with pytest.raises(TypeError, match="text or a Pillow image"):
        model.compute_query_embeddings_with_retry(b"invalid query")
    sleep.assert_not_called()
    session.assert_not_called()


@pytest.mark.parametrize(
    "field, value",
    [
        ("profile_name", "other-profile"),
        ("assumed_role", "arn:aws:iam::123456789012:role/OtherRole"),
        ("role_session_name", "other-session"),
    ],
)
def test_bedrock_image_rejects_credential_settings(tmp_path, mocker, field, value):
    registry = get_registry()
    image_provider = registry.get("bedrock-image")
    session = mocker.patch.object(boto3, "Session")
    with pytest.raises(ValidationError, match=field):
        image_provider.create(**{field: value})

    model = image_provider.create()

    class Photo(LanceModel):
        image: bytes = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    schema = Photo.to_arrow_schema()
    metadata = schema.metadata
    key = b"embedding_functions"
    entries = json.loads(metadata[key])
    entries[0]["model"][field] = value
    metadata[key] = json.dumps(entries).encode()
    schema = schema.with_metadata(metadata)
    lancedb.connect(tmp_path).create_table("photos", schema=schema)
    reopened = lancedb.connect(tmp_path).open_table("photos")
    with pytest.raises(ValidationError, match=field):
        reopened.search("sunset", query_type="vector").limit(1).to_list()
    session.assert_not_called()


def test_bedrock_text_restores_credential_settings(tmp_path, mocker):
    registry = get_registry()
    model = registry.get("bedrock-text").create(
        profile_name="text-profile",
        assumed_role="arn:aws:iam::123456789012:role/TextRole",
        role_session_name="text-session",
    )

    class TextRow(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    db = lancedb.connect(tmp_path)
    db.create_table("texts", schema=TextRow)
    restored = registry.parse_functions(db.open_table("texts").schema.metadata)[
        "vector"
    ].function
    session = mocker.patch.object(boto3, "Session")
    client = session.return_value.client
    sts = mocker.Mock()
    runtime = mocker.Mock()
    client.side_effect = [sts, runtime]
    sts.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "stub-key",
            "SecretAccessKey": "stub-secret",
            "SessionToken": "stub-token",
        }
    }

    assert restored.client is runtime
    session.assert_called_once_with(
        region_name=model.region, profile_name="text-profile"
    )
    sts.assume_role.assert_called_once_with(
        RoleArn="arn:aws:iam::123456789012:role/TextRole",
        RoleSessionName="text-session",
    )
    assert client.call_args.kwargs["aws_session_token"] == "stub-token"


def test_bedrock_text_cohere_v3(bedrock_runtime, mocker):
    model_id = "cohere.embed-multilingual-v3"
    model = get_registry().get("bedrock-text").create(name=model_id, max_retries=0)
    vector = [1.0] * 1024
    invoke = mocker.spy(bedrock_runtime, "invoke_model")
    with Stubber(bedrock_runtime) as stub:
        stub_embedding(stub, model_id, {"embeddings": [vector]})
        stub_embedding(stub, model_id, {"embeddings": [vector]})
        assert model.ndims() == 1024
        assert model.compute_source_embeddings(["document"]) == [vector]
        assert model.compute_query_embeddings("query") == [vector]
        stub.assert_no_pending_responses()

    assert [json.loads(call.kwargs["body"]) for call in invoke.call_args_list] == [
        {"texts": ["document"], "input_type": "search_document"},
        {"texts": ["query"], "input_type": "search_query"},
    ]


class _EmptyRejectingBedrockClient:
    """Mimics bedrock-runtime, which rejects an empty ``inputText``."""

    def __init__(self):
        self.calls = []

    def invoke_model(self, body, modelId, accept, contentType):
        payload = json.loads(body)
        self.calls.append(payload)
        text = payload["inputText"]
        if not isinstance(text, str):
            raise ValueError("inputText: expected a string")
        if not text:
            raise ValueError("inputText: Minimum length is 1")
        body = json.dumps({"embedding": [float(len(text))]}).encode()
        return {"body": io.BytesIO(body)}


@pytest.mark.parametrize("empty", [None, ""])
def test_empty_or_null_texts_are_skipped(empty):
    func = get_registry().get("bedrock-text").create()
    client = _EmptyRejectingBedrockClient()
    func.__dict__["client"] = client

    result = func.compute_source_embeddings(["alpha", empty, "be"])

    assert result == [[5.0], None, [2.0]]
    assert [c["inputText"] for c in client.calls] == ["alpha", "be"]


def test_add_surfaces_non_string_error_with_drop(tmp_path, monkeypatch):
    client = _EmptyRejectingBedrockClient()
    monkeypatch.setattr(boto3.Session, "client", lambda *args, **kwargs: client)
    model = get_registry().get("bedrock-text").create(max_retries=0)

    class Row(LanceModel):
        text: bool = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    table = lancedb.connect(tmp_path).create_table("texts", schema=Row)
    with pytest.raises(RuntimeError, match="expected a string"):
        table.add([{"text": False}], on_bad_vectors="drop")
    assert table.count_rows() == 0
