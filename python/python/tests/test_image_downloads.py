# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import base64
import io
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from lancedb.embeddings import (
    ColPaliEmbeddings,
    JinaEmbeddings,
    OpenClipEmbeddings,
    SigLipEmbeddings,
)
from lancedb.embeddings.utils import retry_with_exponential_backoff, url_retrieve


@pytest.fixture
def image_bytes():
    Image = pytest.importorskip("PIL.Image")
    buffer = io.BytesIO()
    Image.new("RGB", (4, 4), color="red").save(buffer, format="PNG")
    return buffer.getvalue()


@pytest.fixture
def image_server(image_bytes):
    release = threading.Event()
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            if self.path == "/slow-headers":
                release.wait(5)
            if self.path.startswith("/redirect"):
                self.send_response(302)
                self.send_header(
                    "Location",
                    "ftp://127.0.0.1:1/image"
                    if self.path == "/redirect-ftp"
                    else "/image",
                )
                if self.path == "/redirect-body":
                    self.send_header("Content-Length", "1000000000")
                self.end_headers()
                if self.path == "/redirect-body":
                    release.wait(5)
                return
            if self.path == "/not-found":
                self.send_error(404)
                return
            self.send_response(200)
            if self.path == "/large":
                self.send_header("Content-Length", "1000000000")
            elif self.path == "/chunked":
                self.send_header("Transfer-Encoding", "chunked")
            elif self.path not in ("/no-length", "/invalid-length"):
                self.send_header("Content-Length", str(len(image_bytes)))
            elif self.path == "/invalid-length":
                self.send_header("Content-Length", "invalid")
            self.end_headers()
            if self.path == "/slow-body":
                # A partial body must not keep a bounded read waiting forever.
                self.wfile.write(image_bytes[:1])
                self.wfile.flush()
                release.wait(5)
                return
            try:
                if self.path == "/chunked":
                    self.wfile.write(
                        f"{len(image_bytes):x}\r\n".encode()
                        + image_bytes
                        + b"\r\n0\r\n\r\n"
                    )
                else:
                    self.wfile.write(image_bytes)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.parametrize(
    "path", ["/image", "/no-length", "/chunked", "/invalid-length"]
)
def test_download_accepts_exact_size(image_server, image_bytes, path):
    url, _ = image_server
    assert url_retrieve(url + path, max_bytes=len(image_bytes)) == image_bytes


@pytest.mark.parametrize(
    "path", ["/large", "/no-length", "/chunked", "/invalid-length"]
)
def test_download_rejects_oversized_response(image_server, path):
    url, _ = image_server
    with pytest.raises(ValueError, match="maximum size of 32 bytes"):
        url_retrieve(url + path, max_bytes=32)


@pytest.mark.parametrize("path", ["/slow-headers", "/slow-body"])
def test_download_times_out(image_server, path):
    url, _ = image_server
    with pytest.raises(ConnectionError, match="Could not download image"):
        url_retrieve(url + path, timeout=0.05)


def test_download_redirects(image_server, image_bytes):
    url, requests = image_server
    assert url_retrieve(url + "/redirect", max_bytes=len(image_bytes)) == image_bytes
    assert requests == ["/redirect", "/image"]
    with pytest.raises(ValueError, match="maximum size"):
        url_retrieve(url + "/redirect", max_bytes=32)
    with pytest.raises(ValueError, match="only support http and https"):
        url_retrieve(url + "/redirect-ftp")


def test_download_does_not_drain_redirect_bodies(image_server, image_bytes):
    url, requests = image_server
    assert url_retrieve(url + "/redirect-body", timeout=0.5) == image_bytes
    assert requests == ["/redirect-body", "/image"]


@pytest.mark.parametrize("scheme", ["file", "ftp", "data", "httpx"])
def test_download_rejects_other_schemes_before_io(monkeypatch, scheme):
    opener = Mock()
    monkeypatch.setattr("urllib.request.build_opener", opener)
    with pytest.raises(ValueError, match="only support http and https"):
        url_retrieve(f"{scheme}://example.com/image")
    opener.assert_not_called()


def test_download_bounds_reads_and_closes_response(monkeypatch):
    class Response(io.BytesIO):
        headers = {}
        bytes_read = 0

        def read(self, size=-1):
            assert 0 < size <= 64 * 1024
            data = super().read(size)
            self.bytes_read += len(data)
            return data

    response = Response(b"x" * 200000)
    opener = Mock()
    opener.open.return_value = response
    monkeypatch.setattr("urllib.request.build_opener", Mock(return_value=opener))
    with pytest.raises(ValueError, match="maximum size"):
        url_retrieve("https://example.com/image", timeout=2, max_bytes=100000)
    assert response.closed
    assert response.bytes_read == 100001
    opener.open.assert_called_once_with("https://example.com/image", timeout=2)


def test_download_defaults_are_bounded(monkeypatch):
    response = Mock()
    response.headers = {"Content-Length": str(50 * 1024 * 1024 + 1)}
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.read.side_effect = AssertionError("Oversized body should not be read")
    opener = Mock()
    opener.open.return_value = response
    # Patch urlopen too so the original implementation fails without network I/O.
    monkeypatch.setattr("urllib.request.urlopen", Mock(return_value=response))
    monkeypatch.setattr("urllib.request.build_opener", Mock(return_value=opener))
    with pytest.raises(ValueError, match="maximum size of 52428800 bytes"):
        url_retrieve("https://example.com/image")
    opener.open.assert_called_once_with("https://example.com/image", timeout=10.0)
    response.read.assert_not_called()
    response.__exit__.assert_called_once()


PROVIDERS = [OpenClipEmbeddings, SigLipEmbeddings, JinaEmbeddings, ColPaliEmbeddings]


@pytest.fixture(params=PROVIDERS, ids=lambda cls: cls.__name__)
def provider(request):
    # Exercise image preparation without loading models or contacting model hubs.
    return request.param.model_construct()


def prepare_image(provider, image):
    if isinstance(provider, JinaEmbeddings):
        result = provider._generate_image_input_dict(image)
        return pytest.importorskip("PIL.Image").open(
            io.BytesIO(base64.b64decode(result["image"]))
        )
    if isinstance(provider, ColPaliEmbeddings):
        return provider._prepare_images([image])[0]
    return provider._to_pil(image)


def test_providers_download_images(provider, image_server):
    url, _ = image_server
    with prepare_image(provider, url + "/image") as image:
        assert image.size == (4, 4)


def test_providers_apply_size_limit(provider, image_server):
    url, _ = image_server
    provider.image_download_max_bytes = 32
    with pytest.raises(ValueError, match="maximum size of 32 bytes"):
        prepare_image(provider, url + "/no-length")


def test_providers_apply_timeout(provider, image_server):
    url, _ = image_server
    provider.image_download_timeout = 0.05
    # ColPali adds context to connection failures during image preparation.
    with pytest.raises((ConnectionError, ValueError), match="Could not download image"):
        prepare_image(provider, url + "/slow-body")


def test_providers_can_disable_downloads_without_retrying(
    provider, image_server, monkeypatch
):
    url, requests = image_server
    provider.allow_image_downloads = False
    sleep = Mock()
    monkeypatch.setattr("lancedb.embeddings.utils.time.sleep", sleep)
    with pytest.raises(ValueError, match="URL downloads are disabled"):
        retry_with_exponential_backoff(
            lambda: prepare_image(provider, url + "/image")
        )()
    assert requests == []
    sleep.assert_not_called()


def test_providers_do_not_retry_size_failures(provider, image_server, monkeypatch):
    url, requests = image_server
    provider.image_download_max_bytes = 32
    sleep = Mock()
    monkeypatch.setattr("lancedb.embeddings.utils.time.sleep", sleep)
    with pytest.raises(ValueError, match="maximum size"):
        retry_with_exponential_backoff(
            lambda: prepare_image(provider, url + "/large")
        )()
    assert requests == ["/large"]
    sleep.assert_not_called()


def test_local_images_still_work_when_downloads_disabled(
    provider, image_bytes, tmp_path
):
    provider.allow_image_downloads = False
    Image = pytest.importorskip("PIL.Image")
    image_path = tmp_path / "image.png"
    image_path.write_bytes(image_bytes)
    for source in (str(image_path), image_bytes, Image.open(io.BytesIO(image_bytes))):
        with prepare_image(provider, source) as image:
            assert image.size == (4, 4)


@pytest.mark.parametrize(
    "options",
    [
        {"image_download_timeout": 0},
        {"image_download_timeout": -1},
        {"image_download_timeout": float("inf")},
        {"image_download_timeout": float("nan")},
        {"image_download_max_bytes": 0},
        {"image_download_max_bytes": -1},
    ],
)
@pytest.mark.parametrize("cls", PROVIDERS)
def test_invalid_download_configuration(cls, options):
    # Validation fails in the base __init__, before model dependencies are loaded.
    with pytest.raises(ValidationError):
        cls(**options)


def test_download_configuration_roundtrips_through_metadata():
    from lancedb.embeddings import EmbeddingFunctionConfig, EmbeddingFunctionRegistry

    registry = EmbeddingFunctionRegistry.get_instance()
    func = JinaEmbeddings.create(
        allow_image_downloads=False,
        image_download_timeout=2,
        image_download_max_bytes=1024,
    )
    metadata = registry.get_table_metadata(
        [
            EmbeddingFunctionConfig(
                source_column="image", vector_column="vector", function=func
            )
        ]
    )
    restored = registry.parse_functions(metadata)["vector"].function
    assert restored.allow_image_downloads is False
    assert restored.image_download_timeout == 2
    assert restored.image_download_max_bytes == 1024


def test_download_404_is_not_retried_and_closes_response(image_server, monkeypatch):
    url, requests = image_server
    sleep = Mock()
    monkeypatch.setattr("lancedb.embeddings.utils.time.sleep", sleep)
    from urllib.error import HTTPError

    with pytest.raises(HTTPError) as exc:
        retry_with_exponential_backoff(lambda: url_retrieve(url + "/not-found"))()
    assert exc.value.code == 404
    assert exc.value.closed
    assert requests == ["/not-found"]
    sleep.assert_not_called()
