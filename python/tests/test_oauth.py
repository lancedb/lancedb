# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import asyncio
import importlib.util
import json
import os
import subprocess
import sys
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest


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


def test_authorization_code_uses_pkce_by_default():
    oauth = _load_oauth_module()

    config = oauth.OAuthConfig(
        issuer_url="https://issuer.example.com",
        client_id="client-id",
        scopes=["openid"],
        flow=oauth.OAuthFlowType.AUTHORIZATION_CODE,
    )

    assert config.use_pkce is True
    assert config.redirect_uri is None
    assert config.callback_port is None


def test_device_code_flow_value():
    oauth = _load_oauth_module()

    assert oauth.OAuthFlowType.DEVICE_CODE.value == "device_code"


def test_token_cache_options_default_to_memory_only():
    oauth = _load_oauth_module()

    config = oauth.OAuthConfig(
        issuer_url="https://issuer.example.com",
        client_id="client-id",
        scopes=["openid"],
    )
    assert config.token_cache is None

    options = oauth.TokenCacheOptions()
    assert options.cache_dir is None
    assert options.lock_timeout_secs is None


def _remote_oauth():
    pytest.importorskip("lancedb")
    from lancedb.remote import oauth as remote_oauth

    return remote_oauth


def _device_config(remote_oauth, issuer_url, cache_dir):
    return remote_oauth.OAuthConfig(
        issuer_url=issuer_url,
        client_id="client-id",
        scopes=["openid"],
        flow=remote_oauth.OAuthFlowType.DEVICE_CODE,
        token_cache=remote_oauth.TokenCacheOptions(cache_dir=str(cache_dir)),
    )


def test_oauth_session_status_and_logout_without_cache_entry(tmp_path):
    remote_oauth = _remote_oauth()
    config = _device_config(remote_oauth, "https://issuer.example.com", tmp_path)

    session = remote_oauth.OAuthSession(config)
    status = asyncio.run(session.status())
    assert status.refreshable is False
    assert status.issuer_url == "https://issuer.example.com"
    assert status.client_id == "client-id"
    assert status.scopes == ["openid"]
    assert status.flow == "device_code"
    assert status.obtained_at is None

    logout = asyncio.run(session.logout())
    assert logout.removed is False


class _MockIdpState:
    def __init__(self, port):
        self.port = port
        self.lock = threading.Lock()
        self.device_authorizations = 0
        self.refresh_grants = 0
        self.invalid_grant_rejections = 0
        self.access_tokens_issued = 0
        self.current_refresh = None


class _MockIdpHandler(BaseHTTPRequestHandler):
    @property
    def state(self) -> _MockIdpState:
        return self.server.state

    def log_message(self, fmt, *args):
        pass

    def _respond(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/.well-known/openid-configuration"):
            base = f"http://127.0.0.1:{self.state.port}"
            self._respond(
                200,
                {
                    "token_endpoint": f"{base}/token",
                    "device_authorization_endpoint": f"{base}/device",
                },
            )
        else:
            self._respond(404, {})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode()
        params = urllib.parse.parse_qs(body)

        if self.path == "/device":
            with self.state.lock:
                self.state.device_authorizations += 1
            base = f"http://127.0.0.1:{self.state.port}"
            self._respond(
                200,
                {
                    "device_code": "device-code",
                    "user_code": "ABCD-EFGH",
                    "verification_uri": f"{base}/verify",
                    "expires_in": 60,
                    "interval": 1,
                },
            )
            return

        if self.path == "/token":
            grant_type = params.get("grant_type", [""])[0]
            with self.state.lock:
                if grant_type == "refresh_token":
                    self.state.refresh_grants += 1
                    offered = params.get("refresh_token", [""])[0]
                    if offered != self.state.current_refresh:
                        self.state.invalid_grant_rejections += 1
                        self._respond(400, {"error": "invalid_grant"})
                        return
                elif "device_code" not in grant_type:
                    self._respond(400, {"error": "unsupported_grant_type"})
                    return
                self.state.access_tokens_issued += 1
                number = self.state.access_tokens_issued
                refresh = f"refresh-{number}"
                self.state.current_refresh = refresh
            self._respond(
                200,
                {
                    "access_token": f"access-{number}",
                    "refresh_token": refresh,
                    "expires_in": 3600,
                },
            )
            return

        self._respond(404, {})


def _start_mock_idp() -> tuple[_MockIdpState, HTTPServer]:
    server = HTTPServer(("127.0.0.1", 0), _MockIdpHandler)
    state = _MockIdpState(server.server_address[1])
    server.state = state
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return state, server


def _run_subprocess(script: Path, issuer_url: str, cache_dir: Path):
    env = dict(os.environ)
    env["LANCEDB_OAUTH_BROWSER"] = "/usr/bin/true"
    result = subprocess.run(
        [sys.executable, str(script), issuer_url, str(cache_dir)],
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
    )
    assert result.returncode == 0, (
        f"subprocess failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
    return result


LOGIN_SCRIPT = """
import asyncio
import sys

from lancedb.remote import OAuthConfig, OAuthFlowType, OAuthSession, TokenCacheOptions

issuer_url, cache_dir = sys.argv[1], sys.argv[2]
config = OAuthConfig(
    issuer_url=issuer_url,
    client_id="client-id",
    scopes=["openid"],
    flow=OAuthFlowType.DEVICE_CODE,
    token_cache=TokenCacheOptions(cache_dir=cache_dir),
)
session = OAuthSession(config)
status = asyncio.run(session.login())
assert status.refreshable, "login must cache a refresh token"
print("LOGIN-OK")
"""

REUSE_SCRIPT = """
import asyncio
import sys

import lancedb
from lancedb.remote import OAuthConfig, OAuthFlowType, OAuthSession, TokenCacheOptions

issuer_url, cache_dir = sys.argv[1], sys.argv[2]
config = OAuthConfig(
    issuer_url=issuer_url,
    client_id="client-id",
    scopes=["openid"],
    flow=OAuthFlowType.DEVICE_CODE,
    token_cache=TokenCacheOptions(cache_dir=cache_dir),
)

session = OAuthSession(config)
status = asyncio.run(session.status())
assert status.refreshable, "second process must see the cached session"


async def main():
    # Point the database endpoint at a dead port. OAuth headers are fetched
    # before the request is sent, so a successful refresh proves the second
    # process reused the cached session; only the database call fails.
    db = await lancedb.connect_async(
        "db://e2e",
        host_override="http://127.0.0.1:1",
        client_config={"retry_config": {"retries": 0}},
        oauth_config=config,
    )
    try:
        await db.table_names()
    except Exception:
        print("DATABASE-UNREACHABLE-AS-EXPECTED")
    else:
        raise AssertionError("expected the database request to fail")


asyncio.run(main())
print("REUSE-OK")
"""


def test_cross_process_session_reuse_without_new_prompt(tmp_path):
    pytest.importorskip("lancedb")
    state, server = _start_mock_idp()
    try:
        issuer_url = f"http://127.0.0.1:{state.port}"
        login_script = tmp_path / "login.py"
        login_script.write_text(LOGIN_SCRIPT)
        reuse_script = tmp_path / "reuse.py"
        reuse_script.write_text(REUSE_SCRIPT)
        cache_dir = tmp_path / "oauth-cache"

        result = _run_subprocess(login_script, issuer_url, cache_dir)
        assert "LOGIN-OK" in result.stdout
        assert state.device_authorizations == 1

        result = _run_subprocess(reuse_script, issuer_url, cache_dir)
        assert "REUSE-OK" in result.stdout
        assert "DATABASE-UNREACHABLE-AS-EXPECTED" in result.stdout

        # The second process refreshed exactly once and never started a new
        # interactive device flow.
        assert state.refresh_grants == 1
        assert state.device_authorizations == 1
        assert state.invalid_grant_rejections == 0

        logout = asyncio.run(
            _remote_oauth()
            .OAuthSession(_device_config(_remote_oauth(), issuer_url, cache_dir))
            .logout()
        )
        assert logout.removed is True
    finally:
        server.shutdown()
        server.server_close()
