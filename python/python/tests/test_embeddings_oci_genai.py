# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Unit tests for the OCI Generative AI embedding function.

The ``oci`` SDK is replaced with a small fake so these run without the package
or cloud credentials. Live coverage lives in ``test_embeddings_slow.py``.
"""

import os
import pickle
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import lancedb
import pytest
from lancedb.embeddings import OCIGenAIEmbeddings, get_registry
from lancedb.embeddings.oci_genai import EMBEDDING_BATCH_SIZE, OCIGenAIServiceError
from lancedb.pydantic import LanceModel, Vector
from pydantic import ValidationError

DIMS = 8
COMPARTMENT = "ocid1.compartment.oc1..test"


class FakeServiceError(Exception):
    """Mimics ``oci.exceptions.ServiceError``."""

    def __init__(self, status, code, message):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def _fake_embed_text(details):
    # Deterministic embeddings: first component encodes len(text) so ordering
    # and nearest-neighbour behaviour can be asserted.
    embeddings = [[float(len(text))] + [0.0] * (DIMS - 1) for text in details.inputs]
    return SimpleNamespace(data=SimpleNamespace(embeddings=embeddings))


@pytest.fixture
def fake_oci():
    """Stand-in for the ``oci`` package, injected via attempt_import_or_raise."""
    oci = MagicMock(name="oci")
    oci.exceptions.ServiceError = FakeServiceError
    models = oci.generative_ai_inference.models
    models.EmbedTextDetails.side_effect = lambda **kw: SimpleNamespace(**kw)
    models.OnDemandServingMode.side_effect = lambda **kw: SimpleNamespace(
        serving_type="ON_DEMAND", **kw
    )
    models.DedicatedServingMode.side_effect = lambda **kw: SimpleNamespace(
        serving_type="DEDICATED", **kw
    )
    oci.config.from_file.return_value = {"key_file": "~/.oci/key.pem"}
    client = oci.generative_ai_inference.GenerativeAiInferenceClient.return_value
    client.embed_text.side_effect = _fake_embed_text
    with patch(
        "lancedb.embeddings.oci_genai.attempt_import_or_raise", return_value=oci
    ):
        yield oci


def _create(**kwargs):
    kwargs.setdefault("compartment_id", COMPARTMENT)
    kwargs.setdefault("max_retries", 0)
    return get_registry().get("oci-genai").create(**kwargs)


def _client(fake_oci):
    return fake_oci.generative_ai_inference.GenerativeAiInferenceClient.return_value


def _client_kwargs(fake_oci):
    return fake_oci.generative_ai_inference.GenerativeAiInferenceClient.call_args.kwargs


def _embed_requests(fake_oci):
    return [call.args[0] for call in _client(fake_oci).embed_text.call_args_list]


def test_oci_genai_registered():
    assert get_registry().get("oci-genai") is OCIGenAIEmbeddings
    assert "cohere.embed-v4.0" in OCIGenAIEmbeddings.model_names()
    func = _create()
    assert func.name == "cohere.embed-v4.0"
    assert func.auth_type == "API_KEY"
    assert func.truncate == "END"
    assert func.sensitive_keys() == []


def test_oci_genai_service_endpoint(fake_oci):
    _create()._client
    assert (
        _client_kwargs(fake_oci)["service_endpoint"]
        == "https://inference.generativeai.us-chicago-1.oci.oraclecloud.com"
    )

    _create(region="eu-frankfurt-1")._client
    assert (
        _client_kwargs(fake_oci)["service_endpoint"]
        == "https://inference.generativeai.eu-frankfurt-1.oci.oraclecloud.com"
    )

    custom = "https://inference.generativeai.us-langley-1.oci.oraclegovcloud.com"
    _create(region="us-chicago-1", service_endpoint=custom)._client
    assert _client_kwargs(fake_oci)["service_endpoint"] == custom


def test_oci_genai_client_disables_sdk_retries(fake_oci):
    _create()._client
    kwargs = _client_kwargs(fake_oci)
    assert kwargs["retry_strategy"] is fake_oci.retry.NoneRetryStrategy.return_value


def test_oci_genai_api_key_auth(fake_oci, tmp_path):
    config_file = str(tmp_path / "config")
    func = _create(auth_profile="MYPROFILE", auth_file_location=config_file)
    func._client
    fake_oci.config.from_file.assert_called_once_with(
        file_location=config_file, profile_name="MYPROFILE"
    )
    kwargs = _client_kwargs(fake_oci)
    assert kwargs["config"] is fake_oci.config.from_file.return_value
    assert "signer" not in kwargs


def test_oci_genai_api_key_rejects_session_profile(fake_oci):
    fake_oci.config.from_file.return_value = {
        "key_file": "~/.oci/sessions/x/key.pem",
        "security_token_file": "~/.oci/sessions/x/token",
    }
    with pytest.raises(ValueError, match="SECURITY_TOKEN"):
        _create(auth_type="API_KEY")._client


def test_oci_genai_security_token_auth(fake_oci, tmp_path):
    token_file = tmp_path / "token"
    token_file.write_text("session-token\n")
    fake_oci.config.from_file.return_value = {
        "key_file": "~/.oci/sessions/x/key.pem",
        "security_token_file": str(token_file),
    }
    _create(auth_type="SECURITY_TOKEN", auth_profile="SESSION")._client

    fake_oci.signer.load_private_key_from_file.assert_called_once_with(
        os.path.expanduser("~/.oci/sessions/x/key.pem"), None
    )
    fake_oci.auth.signers.SecurityTokenSigner.assert_called_once_with(
        "session-token", fake_oci.signer.load_private_key_from_file.return_value
    )
    kwargs = _client_kwargs(fake_oci)
    assert kwargs["signer"] is fake_oci.auth.signers.SecurityTokenSigner.return_value
    assert kwargs["config"] is fake_oci.config.from_file.return_value


def test_oci_genai_security_token_requires_token_file(fake_oci):
    with pytest.raises(ValueError, match="security_token_file"):
        _create(auth_type="SECURITY_TOKEN")._client


@pytest.mark.parametrize(
    "auth_type,factory",
    [
        ("INSTANCE_PRINCIPAL", "InstancePrincipalsSecurityTokenSigner"),
        ("RESOURCE_PRINCIPAL", "get_resource_principals_signer"),
    ],
)
def test_oci_genai_principal_auth(fake_oci, auth_type, factory):
    _create(auth_type=auth_type)._client
    signer_factory = getattr(fake_oci.auth.signers, factory)
    signer_factory.assert_called_once_with()
    kwargs = _client_kwargs(fake_oci)
    assert kwargs["config"] == {}
    assert kwargs["signer"] is signer_factory.return_value
    fake_oci.config.from_file.assert_not_called()


def test_oci_genai_rejects_invalid_config():
    with pytest.raises(ValidationError, match="auth_type"):
        _create(auth_type="PASSWORD")
    with pytest.raises(ValidationError, match="truncate"):
        _create(truncate="MIDDLE")


def test_oci_genai_compartment_id_required(fake_oci, monkeypatch):
    monkeypatch.delenv("OCI_COMPARTMENT_ID", raising=False)
    func = get_registry().get("oci-genai").create(max_retries=0)
    with pytest.raises(ValueError, match="OCI_COMPARTMENT_ID"):
        func.compute_source_embeddings(["hello"])
    assert _embed_requests(fake_oci) == []

    monkeypatch.setenv("OCI_COMPARTMENT_ID", "ocid1.compartment.oc1..from-env")
    func.compute_source_embeddings(["hello"])
    assert _embed_requests(fake_oci)[0].compartment_id == (
        "ocid1.compartment.oc1..from-env"
    )


def test_oci_genai_request_shape(fake_oci):
    func = _create(name="cohere.embed-multilingual-v3.0", truncate="START")
    func.compute_source_embeddings(["hello"])
    (details,) = _embed_requests(fake_oci)
    assert details.inputs == ["hello"]
    assert details.compartment_id == COMPARTMENT
    assert details.truncate == "START"
    assert details.input_type == "SEARCH_DOCUMENT"
    assert details.serving_mode.serving_type == "ON_DEMAND"
    assert details.serving_mode.model_id == "cohere.embed-multilingual-v3.0"
    # only sent when configured so older SDKs / models keep working
    assert not hasattr(details, "output_dimensions")


def test_oci_genai_dedicated_endpoint(fake_oci):
    endpoint = "ocid1.generativeaiendpoint.oc1.us-chicago-1.test"
    _create(name=endpoint).compute_source_embeddings(["hello"])
    (details,) = _embed_requests(fake_oci)
    assert details.serving_mode.serving_type == "DEDICATED"
    assert details.serving_mode.endpoint_id == endpoint


def test_oci_genai_query_and_source_input_types(fake_oci):
    func = _create()
    func.compute_source_embeddings(["a document"])
    func.compute_query_embeddings("a query")
    source, query = _embed_requests(fake_oci)
    assert source.input_type == "SEARCH_DOCUMENT"
    assert query.input_type == "SEARCH_QUERY"

    custom = _create(source_input_type="CLUSTERING", query_input_type="CLASSIFICATION")
    custom.compute_source_embeddings(["a document"])
    custom.compute_query_embeddings("a query")
    source, query = _embed_requests(fake_oci)[2:]
    assert source.input_type == "CLUSTERING"
    assert query.input_type == "CLASSIFICATION"


def test_oci_genai_batches_at_service_limit(fake_oci):
    assert EMBEDDING_BATCH_SIZE == 96
    texts = [f"text {i:03d}" for i in range(2 * EMBEDDING_BATCH_SIZE + 8)]
    embeddings = _create().compute_source_embeddings(texts)

    requests = _embed_requests(fake_oci)
    assert [len(r.inputs) for r in requests] == [96, 96, 8]
    assert [t for r in requests for t in r.inputs] == texts
    assert all(r.input_type == "SEARCH_DOCUMENT" for r in requests)
    assert len(embeddings) == len(texts)
    assert [e[0] for e in embeddings] == [float(len(t)) for t in texts]


def test_oci_genai_empty_inputs_are_skipped(fake_oci):
    func = _create()
    assert func.compute_source_embeddings([]) == []
    assert _embed_requests(fake_oci) == []

    embeddings = func.compute_source_embeddings(["hello", "", None, "world"])
    (details,) = _embed_requests(fake_oci)
    assert details.inputs == ["hello", "world"]
    assert embeddings[1] is None
    assert embeddings[2] is None
    assert embeddings[0][0] == 5.0
    assert embeddings[3][0] == 5.0


def test_oci_genai_ndims_probe_is_cached(fake_oci):
    func = _create()
    assert func.ndims() == DIMS
    assert func.ndims() == DIMS
    assert len(_embed_requests(fake_oci)) == 1


def test_oci_genai_output_dimensions(fake_oci):
    func = _create(output_dimensions=512)
    assert func.ndims() == 512
    assert _embed_requests(fake_oci) == []  # no probe request needed

    func.compute_source_embeddings(["hello"])
    (details,) = _embed_requests(fake_oci)
    assert details.output_dimensions == 512


@patch("time.sleep")
def test_oci_genai_auth_errors_are_not_retried(mock_sleep, fake_oci):
    _client(fake_oci).embed_text.side_effect = FakeServiceError(
        401, "NotAuthenticated", "The required information was not provided."
    )
    func = _create(max_retries=3)
    with pytest.raises(OCIGenAIServiceError, match="HTTP 401") as exc_info:
        func.compute_source_embeddings_with_retry(["hello"])

    assert exc_info.value.status_code == 401
    assert "oci session authenticate" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, FakeServiceError)
    assert _client(fake_oci).embed_text.call_count == 1
    mock_sleep.assert_not_called()


@patch("time.sleep")
def test_oci_genai_server_errors_are_retried(mock_sleep, fake_oci):
    _client(fake_oci).embed_text.side_effect = FakeServiceError(
        500, "InternalError", "try again"
    )
    func = _create(max_retries=2)
    with pytest.raises(Exception, match="Maximum number of retries"):
        func.compute_source_embeddings_with_retry(["hello"])
    assert _client(fake_oci).embed_text.call_count == 3


def test_oci_genai_pickle_after_client_created(fake_oci):
    func = _create(auth_profile="MYPROFILE")
    func.compute_source_embeddings(["hello"])
    assert "_client" in func.__dict__

    restored = pickle.loads(pickle.dumps(func))
    assert "_client" not in restored.__dict__
    assert restored.compartment_id == COMPARTMENT
    assert restored.auth_profile == "MYPROFILE"


def test_oci_genai_table_round_trip(fake_oci):
    func = _create(region="us-ashburn-1", auth_profile="MYPROFILE")

    class Docs(LanceModel):
        text: str = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()

    db = lancedb.connect("memory://")
    tbl = db.create_table("docs", schema=Docs, mode="overwrite")
    tbl.add([{"text": "hello"}, {"text": "goodbye world"}])
    assert len(tbl.to_pandas()["vector"][0]) == DIMS

    # the fake embeds len(text), so an equally long query lands on that row
    hits = tbl.search("goodbye world").limit(1).to_pandas()
    assert hits["text"][0] == "goodbye world"
    assert _embed_requests(fake_oci)[-1].input_type == "SEARCH_QUERY"

    parsed = tbl.embedding_functions["vector"].function
    assert isinstance(parsed, OCIGenAIEmbeddings)
    assert parsed.safe_model_dump() == {
        "compartment_id": COMPARTMENT,
        "max_retries": 0,
        "region": "us-ashburn-1",
        "auth_profile": "MYPROFILE",
    }
