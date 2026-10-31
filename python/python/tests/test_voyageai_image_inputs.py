# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import base64
from io import BytesIO
from unittest.mock import MagicMock

import pytest

from lancedb.embeddings import get_registry
from lancedb.embeddings.voyageai import VoyageAIEmbeddingFunction


@pytest.mark.parametrize("model_name", ["voyage-multimodal-3", "voyage-multimodal-3.5"])
@pytest.mark.parametrize("input_kind", ["pil", "bytes", "path"])
@pytest.mark.parametrize(
    "mode,color,expected_rgb",
    [
        ("RGBA", (64, 128, 192, 128), (64, 128, 192)),
        ("LA", (96, 128), (96, 96, 96)),
        ("P", 0, (64, 128, 192)),
        ("RGB", (64, 128, 192), (64, 128, 192)),
    ],
)
def test_multimodal_source_embeddings_encode_image_modes(
    model_name, input_kind, mode, color, expected_rgb, tmp_path, monkeypatch
):
    Image = pytest.importorskip("PIL.Image")
    image = Image.new(mode, (8, 6), color)
    if mode == "P":
        image.putpalette([64, 128, 192] + [0, 0, 0] * 255)
        image.info["transparency"] = 0
    original_pixels = image.tobytes()
    image_input = image
    if input_kind != "pil":
        png = BytesIO()
        image.save(png, format="PNG")
        if input_kind == "bytes":
            image_input = png.getvalue()
        else:
            image_input = tmp_path / "image.png"
            image_input.write_bytes(png.getvalue())

    client = MagicMock()
    client.multimodal_embed.return_value.embeddings = [[0.1, 0.2]]
    monkeypatch.setattr(VoyageAIEmbeddingFunction, "client", client)
    func = get_registry().get("voyageai").create(name=model_name)

    assert func.compute_source_embeddings(image_input) == [[0.1, 0.2]]

    client.multimodal_embed.assert_called_once()
    request = client.multimodal_embed.call_args.kwargs
    assert request["model"] == model_name
    assert request["input_type"] == "document"
    assert len(request["inputs"]) == 1
    (content,) = request["inputs"][0]["content"]
    assert content["type"] == "image_base64"
    prefix, encoded = content["image_base64"].split(",", 1)
    assert prefix == "data:image/jpeg;base64"
    with Image.open(BytesIO(base64.b64decode(encoded, validate=True))) as decoded:
        assert decoded.format == "JPEG"
        assert decoded.mode == "RGB"
        assert decoded.size == (8, 6)
        assert decoded.getpixel((0, 0)) == pytest.approx(expected_rgb, abs=3)
    assert image.mode == mode
    assert image.tobytes() == original_pixels
    client.embed.assert_not_called()


@pytest.mark.parametrize("input_kind", ["bytes", "path"])
def test_multimodal_source_embeddings_reject_invalid_image(
    input_kind, tmp_path, monkeypatch
):
    PIL = pytest.importorskip("PIL")
    image_input = b"not an image"
    if input_kind == "path":
        image_input = tmp_path / "invalid.png"
        image_input.write_bytes(b"not an image")

    client = MagicMock()
    monkeypatch.setattr(VoyageAIEmbeddingFunction, "client", client)
    func = get_registry().get("voyageai").create(name="voyage-multimodal-3")

    with pytest.raises(PIL.UnidentifiedImageError):
        func.compute_source_embeddings(image_input)

    client.multimodal_embed.assert_not_called()
    client.embed.assert_not_called()
