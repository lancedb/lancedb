# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Unit tests for WatsonxEmbeddings — no live API calls required."""

import pytest
from unittest.mock import MagicMock, patch

from lancedb.embeddings import get_registry
from lancedb.embeddings.watsonx import MODELS_DIMS, WatsonxEmbeddings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_func(monkeypatch, env=None, **create_kwargs):
    """
    Return a WatsonxEmbeddings instance with ibm_watsonx_ai mocked out.

    Parameters
    ----------
    env : dict, optional
        Environment variables to inject (merged on top of an empty env so that
        no real WATSONX_* vars from the host bleed into the test).
    create_kwargs :
        Forwarded to ``WatsonxEmbeddings.create()``.
    """
    base_env = {
        k: "" for k in ("WATSONX_API_KEY", "WATSONX_PROJECT_ID", "WATSONX_SPACE_ID")
    }
    base_env.update(env or {})
    # Only keep keys that have non-empty values so that absent vars are truly absent.
    clean_env = {k: v for k, v in base_env.items() if v}

    mock_embeddings_instance = MagicMock()
    mock_foundation = MagicMock()
    mock_foundation.Embeddings.return_value = mock_embeddings_instance
    mock_ibm = MagicMock()

    def _fake_import(name):
        if name == "ibm_watsonx_ai":
            return mock_ibm
        if name == "ibm_watsonx_ai.foundation_models":
            return mock_foundation
        raise ImportError(name)

    with patch.dict("os.environ", clean_env, clear=True):
        with patch(
            "lancedb.embeddings.watsonx.attempt_import_or_raise",
            side_effect=_fake_import,
        ):
            func = get_registry().get("watsonx").create(**create_kwargs)
            # Force the cached_property to evaluate inside the patch context.
            _ = func._watsonx_client
    return func, mock_foundation


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_watsonx_registered(self):
        assert get_registry().get("watsonx") is not None

    def test_model_names_returns_all_available(self):
        names = WatsonxEmbeddings.model_names()
        assert names == list(MODELS_DIMS.keys())
        for name in (
            "ibm/granite-embedding-278m-multilingual",
            "ibm/slate-125m-english-rtrvr-v2",
            "ibm/slate-125m-english-rtrvr",
            "ibm/slate-30m-english-rtrvr",
        ):
            assert name in names


# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------


class TestDimensions:
    @pytest.mark.parametrize(
        "model_name,expected_dims",
        [
            ("ibm/granite-embedding-278m-multilingual", 768),
            ("ibm/slate-125m-english-rtrvr-v2", 768),
            ("ibm/slate-30m-english-rtrvr-v2", 384),
            ("intfloat/multilingual-e5-large", 1024),
            ("sentence-transformers/all-minilm-l6-v2", 384),
        ],
    )
    def test_current_model_dimensions(self, monkeypatch, model_name, expected_dims):
        func, _ = _make_func(
            monkeypatch,
            env={"WATSONX_API_KEY": "key", "WATSONX_PROJECT_ID": "proj"},
            name=model_name,
        )
        assert func.ndims() == expected_dims

    def test_unknown_model_raises(self):
        func = WatsonxEmbeddings(name="not/a-real-model")
        with pytest.raises(ValueError, match="Unknown model"):
            func.ndims()

    # -- Backward-compat: legacy names must still resolve dims on table load --
    @pytest.mark.parametrize(
        "legacy_name,expected_dims",
        [
            ("ibm/slate-125m-english-rtrvr", 768),
            ("ibm/slate-30m-english-rtrvr", 384),
            ("sentence-transformers/all-minilm-l12-v2", 384),
        ],
    )
    def test_legacy_model_dimensions_still_resolve(self, legacy_name, expected_dims):
        """Tables written with old model names must not raise on reload."""
        assert MODELS_DIMS[legacy_name] == expected_dims


# ---------------------------------------------------------------------------
# Scope resolution (project_id / space_id)
# ---------------------------------------------------------------------------


class TestScopeResolution:
    def test_explicit_project_id(self, monkeypatch):
        func, mock_foundation = _make_func(
            monkeypatch,
            env={"WATSONX_API_KEY": "key"},
            project_id="explicit-proj",
        )
        _, call_kwargs = mock_foundation.Embeddings.call_args
        assert call_kwargs.get("project_id") == "explicit-proj"
        assert "space_id" not in call_kwargs

    def test_explicit_space_id(self, monkeypatch):
        func, mock_foundation = _make_func(
            monkeypatch,
            env={"WATSONX_API_KEY": "key"},
            space_id="explicit-space",
        )
        _, call_kwargs = mock_foundation.Embeddings.call_args
        assert call_kwargs.get("space_id") == "explicit-space"
        assert "project_id" not in call_kwargs

    def test_env_project_id_fallback(self, monkeypatch):
        func, mock_foundation = _make_func(
            monkeypatch,
            env={"WATSONX_API_KEY": "key", "WATSONX_PROJECT_ID": "env-proj"},
        )
        _, call_kwargs = mock_foundation.Embeddings.call_args
        assert call_kwargs.get("project_id") == "env-proj"

    def test_env_space_id_fallback(self, monkeypatch):
        func, mock_foundation = _make_func(
            monkeypatch,
            env={"WATSONX_API_KEY": "key", "WATSONX_SPACE_ID": "env-space"},
        )
        _, call_kwargs = mock_foundation.Embeddings.call_args
        assert call_kwargs.get("space_id") == "env-space"

    def test_explicit_project_id_wins_over_env_space_id(self, monkeypatch):
        """Explicit project_id must not be overridden by WATSONX_SPACE_ID in env."""
        func, mock_foundation = _make_func(
            monkeypatch,
            env={"WATSONX_API_KEY": "key", "WATSONX_SPACE_ID": "stray-env-space"},
            project_id="explicit-proj",
        )
        _, call_kwargs = mock_foundation.Embeddings.call_args
        assert call_kwargs.get("project_id") == "explicit-proj"
        assert "space_id" not in call_kwargs

    def test_explicit_space_id_wins_over_env_project_id(self, monkeypatch):
        """Explicit space_id must not be overridden by WATSONX_PROJECT_ID in env."""
        func, mock_foundation = _make_func(
            monkeypatch,
            env={"WATSONX_API_KEY": "key", "WATSONX_PROJECT_ID": "stray-env-proj"},
            space_id="explicit-space",
        )
        _, call_kwargs = mock_foundation.Embeddings.call_args
        assert call_kwargs.get("space_id") == "explicit-space"
        assert "project_id" not in call_kwargs

    def test_both_explicit_raises(self):
        func = WatsonxEmbeddings(
            name="ibm/granite-embedding-278m-multilingual",
            project_id="p",
            space_id="s",
        )
        # The error surfaces when _watsonx_client is first accessed.
        mock_ibm = MagicMock()
        mock_foundation = MagicMock()

        def _fake_import(name):
            if name == "ibm_watsonx_ai":
                return mock_ibm
            if name == "ibm_watsonx_ai.foundation_models":
                return mock_foundation
            raise ImportError(name)

        with patch.dict("os.environ", {"WATSONX_API_KEY": "key"}, clear=True):
            with patch(
                "lancedb.embeddings.watsonx.attempt_import_or_raise",
                side_effect=_fake_import,
            ):
                with pytest.raises(ValueError, match="not both"):
                    _ = func._watsonx_client

    def test_neither_raises(self):
        func = WatsonxEmbeddings(name="ibm/granite-embedding-278m-multilingual")
        mock_ibm = MagicMock()
        mock_foundation = MagicMock()

        def _fake_import(name):
            if name == "ibm_watsonx_ai":
                return mock_ibm
            if name == "ibm_watsonx_ai.foundation_models":
                return mock_foundation
            raise ImportError(name)

        with patch.dict("os.environ", {"WATSONX_API_KEY": "key"}, clear=True):
            with patch(
                "lancedb.embeddings.watsonx.attempt_import_or_raise",
                side_effect=_fake_import,
            ):
                with pytest.raises(
                    ValueError, match="WATSONX_PROJECT_ID or WATSONX_SPACE_ID"
                ):
                    _ = func._watsonx_client

    def test_missing_api_key_raises(self):
        func = WatsonxEmbeddings(name="ibm/granite-embedding-278m-multilingual")
        mock_ibm = MagicMock()
        mock_foundation = MagicMock()

        def _fake_import(name):
            if name == "ibm_watsonx_ai":
                return mock_ibm
            if name == "ibm_watsonx_ai.foundation_models":
                return mock_foundation
            raise ImportError(name)

        with patch.dict("os.environ", {"WATSONX_PROJECT_ID": "proj"}, clear=True):
            with patch(
                "lancedb.embeddings.watsonx.attempt_import_or_raise",
                side_effect=_fake_import,
            ):
                with pytest.raises(ValueError, match="WATSONX_API_KEY"):
                    _ = func._watsonx_client


# ---------------------------------------------------------------------------
# Metadata round-trip (backward compat)
# ---------------------------------------------------------------------------


class TestMetadataRoundTrip:
    def test_legacy_default_reloads_without_error(self):
        """
        Simulate an old table whose stored metadata has no 'name' key (so the
        class default is used on reload).  The legacy default
        ``ibm/slate-125m-english-rtrvr`` must still resolve its dims.
        """
        WatsonxEmbeddings(name="ibm/slate-125m-english-rtrvr")
        # Dimension lookup must not raise — the model is in MODELS_DIMS.
        assert MODELS_DIMS["ibm/slate-125m-english-rtrvr"] == 768

    def test_new_table_uses_granite_default(self):
        func = WatsonxEmbeddings()
        assert func.name == "ibm/granite-embedding-278m-multilingual"


# ---------------------------------------------------------------------------
# WatsonxReranker — scope resolution (project_id / space_id)
# ---------------------------------------------------------------------------


def _make_reranker(env=None, **init_kwargs):
    """
    Return a WatsonxReranker with ibm_watsonx_ai mocked out.

    Scope precedence is tested by inspecting what was passed to Rerank().
    """
    from lancedb.rerankers.watsonx import WatsonxReranker

    base_env = {
        k: "" for k in ("WATSONX_API_KEY", "WATSONX_PROJECT_ID", "WATSONX_SPACE_ID")
    }
    base_env.update(env or {})
    clean_env = {k: v for k, v in base_env.items() if v}

    mock_rerank_instance = MagicMock()
    mock_foundation = MagicMock()
    mock_foundation.Rerank.return_value = mock_rerank_instance
    mock_ibm = MagicMock()

    def _fake_import(name):
        if name == "ibm_watsonx_ai":
            return mock_ibm
        if name == "ibm_watsonx_ai.foundation_models":
            return mock_foundation
        raise ImportError(name)

    reranker = WatsonxReranker(**init_kwargs)
    with patch.dict("os.environ", clean_env, clear=True):
        with patch(
            "lancedb.rerankers.watsonx.attempt_import_or_raise",
            side_effect=_fake_import,
        ):
            _ = reranker._client
    return reranker, mock_foundation


class TestRerankerScopeResolution:
    def test_explicit_project_id(self):
        _, mock_foundation = _make_reranker(
            env={"WATSONX_API_KEY": "key"},
            project_id="explicit-proj",
        )
        _, call_kwargs = mock_foundation.Rerank.call_args
        assert call_kwargs.get("project_id") == "explicit-proj"
        assert "space_id" not in call_kwargs

    def test_explicit_space_id(self):
        _, mock_foundation = _make_reranker(
            env={"WATSONX_API_KEY": "key"},
            space_id="explicit-space",
        )
        _, call_kwargs = mock_foundation.Rerank.call_args
        assert call_kwargs.get("space_id") == "explicit-space"
        assert "project_id" not in call_kwargs

    def test_env_project_id_fallback(self):
        _, mock_foundation = _make_reranker(
            env={"WATSONX_API_KEY": "key", "WATSONX_PROJECT_ID": "env-proj"},
        )
        _, call_kwargs = mock_foundation.Rerank.call_args
        assert call_kwargs.get("project_id") == "env-proj"

    def test_env_space_id_fallback(self):
        _, mock_foundation = _make_reranker(
            env={"WATSONX_API_KEY": "key", "WATSONX_SPACE_ID": "env-space"},
        )
        _, call_kwargs = mock_foundation.Rerank.call_args
        assert call_kwargs.get("space_id") == "env-space"

    def test_explicit_project_id_wins_over_env_space_id(self):
        """Explicit project_id must not be overridden by WATSONX_SPACE_ID in env."""
        _, mock_foundation = _make_reranker(
            env={"WATSONX_API_KEY": "key", "WATSONX_SPACE_ID": "stray-env-space"},
            project_id="explicit-proj",
        )
        _, call_kwargs = mock_foundation.Rerank.call_args
        assert call_kwargs.get("project_id") == "explicit-proj"
        assert "space_id" not in call_kwargs

    def test_explicit_space_id_wins_over_env_project_id(self):
        """Explicit space_id must not be overridden by WATSONX_PROJECT_ID in env."""
        _, mock_foundation = _make_reranker(
            env={"WATSONX_API_KEY": "key", "WATSONX_PROJECT_ID": "stray-env-proj"},
            space_id="explicit-space",
        )
        _, call_kwargs = mock_foundation.Rerank.call_args
        assert call_kwargs.get("space_id") == "explicit-space"
        assert "project_id" not in call_kwargs

    def test_both_explicit_raises(self):
        from lancedb.rerankers.watsonx import WatsonxReranker

        reranker = WatsonxReranker(project_id="p", space_id="s")
        mock_ibm = MagicMock()
        mock_foundation = MagicMock()

        def _fake_import(name):
            if name == "ibm_watsonx_ai":
                return mock_ibm
            if name == "ibm_watsonx_ai.foundation_models":
                return mock_foundation
            raise ImportError(name)

        with patch.dict("os.environ", {"WATSONX_API_KEY": "key"}, clear=True):
            with patch(
                "lancedb.rerankers.watsonx.attempt_import_or_raise",
                side_effect=_fake_import,
            ):
                with pytest.raises(ValueError, match="not both"):
                    _ = reranker._client

    def test_neither_raises(self):
        from lancedb.rerankers.watsonx import WatsonxReranker

        reranker = WatsonxReranker()
        mock_ibm = MagicMock()
        mock_foundation = MagicMock()

        def _fake_import(name):
            if name == "ibm_watsonx_ai":
                return mock_ibm
            if name == "ibm_watsonx_ai.foundation_models":
                return mock_foundation
            raise ImportError(name)

        with patch.dict("os.environ", {"WATSONX_API_KEY": "key"}, clear=True):
            with patch(
                "lancedb.rerankers.watsonx.attempt_import_or_raise",
                side_effect=_fake_import,
            ):
                with pytest.raises(
                    ValueError, match="WATSONX_PROJECT_ID or WATSONX_SPACE_ID"
                ):
                    _ = reranker._client
