"""HARU fork guardrails for the local-subscription Herdr backend.

This intentionally tests catalog and connection admission without contacting a
Dispatch endpoint or a model provider.  A visible Console row must never turn
into an API-key fallback at task time.
"""
from __future__ import annotations

import asyncio
import pathlib
import sys

import pytest
from fastapi import HTTPException

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import app as gw  # noqa: E402


def _herdr_config(url: str = "https://dispatch.example.test/mcp") -> dict:
    return {"api_key": "dispatch-scoped-token", "base_url": url}


def test_local_subscription_is_an_explicit_non_billed_provider_and_herdr_only():
    entry = gw._PROVIDER_CATALOG["local_subscription"]
    assert entry["label"] == "HARU local subscription (experimental)"
    assert entry["secret_label"] == "Dispatch access token"
    assert entry["base_url"] is None
    assert gw._INTEGRATION_WIRING[("local_subscription", "herdr")] == "local_subscription"
    assert {b for p, b in gw._INTEGRATION_WIRING if p == "local_subscription"} == {"herdr"}
    assert set(gw._VENDOR_MODELS["local_subscription"]) == {"haru-auto", "haru-codex", "haru-claude"}
    assert not set(gw._VENDOR_MODELS["local_subscription"]) & set(gw._VENDOR_MODELS["openai"])


def test_herdr_base_is_opt_in_and_advertises_no_local_file_or_tool_surface():
    base = gw._BASE_CATALOG["herdr"]
    assert base["backend"] == "herdr"
    assert base["remote"] is True
    assert base["environments"] is False
    assert base["skills"] is False
    assert base["tools"] == []
    # The adapter is selectable in the Console. Availability remains false
    # until a local_subscription integration serves its selectors.
    assert base["status"] == "ready"
    assert "herdr" in gw._SETUP_NOT_AUTO
    assert gw._MODEL_CATALOG["herdr"] == {
        "default": "haru-auto", "models": ["haru-auto", "haru-codex", "haru-claude"],
    }


def test_setup_catalog_omits_herdr_until_an_operator_configures_dispatch(monkeypatch):
    async def none_served(_org, backend):
        return set()
    monkeypatch.setattr(gw, "_servable_models", none_served)
    assert "herdr" not in asyncio.run(gw._setup_bases("org"))


def test_herdr_cancel_response_is_explicitly_not_a_host_stop_confirmation(monkeypatch):
    async def owned(_sid, _request):
        return "org", {"backend": "herdr", "status": "running", "turn_status": "running"}

    async def stop(_org, _sid, _vertex, **_kwargs):
        return True, "resp-1"

    monkeypatch.setattr(gw, "_owned_session", owned)
    monkeypatch.setattr(gw, "_stop_session", stop)
    out = asyncio.run(gw.cancel_session("hsess-1", object()))
    assert out["cancelled"] is True and out["runner_killed"] is True
    assert out["host_stop_status"] == "unverified"
    assert "not verified" in out["host_stop_detail"]


def test_stop_session_marks_stored_herdr_response_as_stop_unverified(monkeypatch):
    rec = {"id": "resp-1", "status": "running", "metadata": {"session_id": "hsess-1"}}
    writes = []

    async def sandbox(*_args, **_kwargs):
        return {}
    async def vertex(*_args, **_kwargs):
        return None
    async def get_response(_rid):
        return rec
    async def blob_put(key, body, **_kwargs):
        writes.append((key, body)); return True

    monkeypatch.setattr(gw, "_sandbox_json", sandbox)
    monkeypatch.setattr(gw, "_vertex_upsert", vertex)
    monkeypatch.setattr(gw, "_resp_get", get_response)
    monkeypatch.setattr(gw, "_blob_put", blob_put)
    monkeypatch.setattr(gw, "_prefix_from_vertex", lambda *_args: "")
    monkeypatch.setattr(gw.control_store, "enabled", lambda: False)
    gw._session_trace.clear()
    asyncio.run(gw._stop_session("org", "hsess-1", {"backend": "herdr", "runner_turn_id": "rt-1"}, "resp-1"))
    assert rec["metadata"]["haru_stop"] == "unverified"
    assert any(key == "responses/resp-1.json" for key, _ in writes)


def test_herdr_never_inherits_an_auxiliary_image_or_vision_provider(monkeypatch):
    calls = []

    async def image(_sid, _backend):
        calls.append("image"); return {"api_key": "would-spend"}
    async def vision(_sid, _backend):
        calls.append("vision"); return {"api_key": "would-spend"}

    monkeypatch.setattr(gw, "_image_auth", image)
    monkeypatch.setattr(gw, "_vision_auth", vision)
    assert asyncio.run(gw._turn_auxiliary_media_auth("hsess", "herdr")) == (None, None)
    assert calls == []
    assert gw._backend_vision_hint("herdr", "haru-auto") is False


@pytest.mark.parametrize("url", [
    "https://dispatch.example.test/mcp",
    "http://127.0.0.1:8769/mcp",
    "http://localhost:8769/mcp",
])
def test_local_subscription_accepts_only_explicit_mcp_endpoints(url):
    assert gw._local_subscription_endpoint_error(url) == ""


@pytest.mark.parametrize("url", [
    "", "https://dispatch.example.test", "https://dispatch.example.test/mcp?x=1",
    "https://user:pass@dispatch.example.test/mcp", "http://dispatch.example.test/mcp",
    "ftp://dispatch.example.test/mcp", "https://dispatch.example.test/other",
])
def test_local_subscription_refuses_unsafe_or_ambiguous_endpoint(url):
    assert gw._local_subscription_endpoint_error(url)


def test_herdr_harness_requires_an_alias_and_rejects_settings_the_host_cannot_enforce():
    good = gw.HarnessBody(name="Fixture", base="herdr", default_model="haru-auto",
                          system_prompt="Read only", env={"HARU_WORKSPACE": "dispatch-fixture"})
    gw._validate_herdr_harness(good)
    with pytest.raises(HTTPException, match="registered project alias"):
        gw._validate_herdr_harness(gw.HarnessBody(name="Bad", base="herdr", env={"HARU_WORKSPACE": "/tmp/nope"}))
    for reference in ("vault:dispatch-token", "secret:dispatch-token", "$headers.Project"):
        with pytest.raises(HTTPException, match="registered project alias"):
            gw._validate_herdr_harness(gw.HarnessBody(name="Bad", base="herdr", env={"HARU_WORKSPACE": reference}))
    with pytest.raises(HTTPException, match="does not support MCP"):
        gw._validate_herdr_harness(gw.HarnessBody(name="Bad", base="herdr",
                                                   env={"HARU_WORKSPACE": "dispatch-fixture"},
                                                   mcp_servers=[{"name": "x", "url": "https://x"}]))


def test_legacy_herdr_reference_is_refused_before_environment_resolution():
    assert "literal registered project alias" in gw._herdr_runtime_alias_error({
        "env": '{"HARU_WORKSPACE":"vault:dispatch-token"}',
    })
    assert gw._herdr_runtime_alias_error({"env": '{"HARU_WORKSPACE":"dispatch-fixture"}'}) == ""


def test_owner_trust_hands_only_dispatch_token_to_herdr(monkeypatch):
    monkeypatch.setattr(gw, "SANDBOX_TRUST", "owner")
    auth = gw._auth_from_conn({"provider": "local_subscription", "backend": "herdr",
                               **_herdr_config()}, "session-1")
    assert auth == _herdr_config()
    monkeypatch.setattr(gw, "SANDBOX_TRUST", "")
    assert gw._auth_from_conn({"provider": "local_subscription", "backend": "herdr",
                               **_herdr_config()}, "session-1") is None


def test_public_integration_masks_the_dispatch_token():
    public = gw._integration_public({"name": "haru", "provider": "local_subscription",
                                     "config": _herdr_config(), "models": [], "rows_v": 2})
    assert public["config"]["api_key"] == gw._SECRET_SENTINEL
    assert public["config"]["base_url"] == "https://dispatch.example.test/mcp"


def test_integration_save_rejects_missing_or_bad_local_subscription_endpoint(monkeypatch):
    async def admin(_request):
        return {"role": "owner"}
    monkeypatch.setattr(gw, "_require_integrations_admin", admin)
    good = {"name": "haru", "provider": "local_subscription", "config": _herdr_config()}
    with pytest.raises(HTTPException, match="MCP endpoint"):
        asyncio.run(gw.admin_integrations_put(gw.IntegrationsBody(integrations=[{
            "name": "bad", "provider": "local_subscription", "config": {"api_key": "x", "base_url": "https://x"},
        }]), object()))
    with pytest.raises(HTTPException, match="Dispatch access token"):
        asyncio.run(gw.admin_integrations_put(gw.IntegrationsBody(integrations=[{
            "name": "empty", "provider": "local_subscription", "config": {"base_url": good["config"]["base_url"]},
        }]), object()))


def test_masked_dispatch_token_cannot_be_rebound_to_another_endpoint_or_provider(monkeypatch):
    writes = []

    async def admin(_request):
        return {"role": "owner"}
    async def integrations():
        return [{"name": "haru", "provider": "local_subscription", "config": {
            "api_key": "old-dispatch-token", "base_url": "https://old.example.test/mcp",
        }}]
    async def vault_put(*args):
        writes.append(args)
        return True

    monkeypatch.setattr(gw, "_require_integrations_admin", admin)
    monkeypatch.setattr(gw, "_integrations_doc", integrations)
    monkeypatch.setattr(gw, "_vault_put", vault_put)
    with pytest.raises(HTTPException, match="freshly supplied Dispatch access token"):
        asyncio.run(gw.admin_integrations_put(gw.IntegrationsBody(integrations=[{
            "name": "haru", "provider": "local_subscription", "config": {
                "api_key": gw._SECRET_SENTINEL, "base_url": "https://new.example.test/mcp",
            },
        }]), object()))
    with pytest.raises(HTTPException, match="freshly supplied Dispatch access token"):
        asyncio.run(gw.admin_integrations_put(gw.IntegrationsBody(integrations=[{
            "name": "haru", "provider": "openai", "config": {"api_key": gw._SECRET_SENTINEL},
        }]), object()))
    assert writes == []
