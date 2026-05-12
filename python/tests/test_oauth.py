# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import importlib.util
import sys
from pathlib import Path


def _load_oauth_module():
    oauth_path = (
        Path(__file__).parents[1] / "python" / "lancedb" / "remote" / "oauth.py"
    )
    spec = importlib.util.spec_from_file_location("lancedb_remote_oauth", oauth_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_oauth_config_repr_redacts_client_secret():
    oauth = _load_oauth_module()

    config = oauth.OAuthConfig(
        issuer_url="https://issuer.example.com",
        client_id="client-id",
        scopes=["scope"],
        client_secret="super-secret",
    )

    rendered = repr(config)
    assert "super-secret" not in rendered
    assert "client_secret" not in rendered
