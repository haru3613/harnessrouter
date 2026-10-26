import hashlib
import asyncio
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import haru_route as route  # noqa: E402
import app as gateway  # noqa: E402


def test_route_headers_need_the_dedicated_key_and_bind_every_field(monkeypatch):
    token = "route-api-key"
    monkeypatch.setenv("HR_HARU_ROUTE_API_KEY_SHA256", hashlib.sha256(token.encode()).hexdigest())
    headers = {route.HEADER_TASK: "task-1", route.HEADER_FINGERPRINT: "a" * 64,
               route.HEADER_CAPABILITY: "capability_123456789"}
    claim = route.incoming(headers, token)
    assert claim and claim["task_id"] == "task-1"
    with pytest.raises(route.RouteError):
        route.incoming(headers, "ordinary-api-key")
    with pytest.raises(route.RouteError):
        route.incoming({route.HEADER_TASK: "task-1"}, token)


def test_signed_descriptor_keeps_capability_out_of_the_persistable_value():
    claim = {"task_id": "task-1", "fingerprint": "b" * 64, "capability": "capability_123456789"}
    descriptor = route.sign(claim, "resp-1", "herdr-1", "internal-key")
    assert "capability" not in descriptor
    assert route.verify(descriptor, claim["capability"], "internal-key") == {
        "task_id": "task-1", "fingerprint": "b" * 64, "response_id": "resp-1",
        "harness_id": "herdr-1", "capability_sha256": hashlib.sha256(claim["capability"].encode()).hexdigest()}
    with pytest.raises(route.RouteError):
        route.verify(descriptor, "changed_capability_123", "internal-key")


def test_route_scope_requires_a_live_workspace_scoped_api_key_not_ambient_identity():
    allowed = {"herdr-1"}
    assert route.scope_authorized({"org": "o", "workspace": "w"}, org="o", workspace="w", harness_id="herdr-1", allowed_harnesses=allowed)
    assert not route.scope_authorized(None, org="o", workspace="w", harness_id="herdr-1", allowed_harnesses=allowed)
    assert not route.scope_authorized({"org": "o", "workspace": "other"}, org="o", workspace="w", harness_id="herdr-1", allowed_harnesses=allowed)
    assert not route.scope_authorized({"org": "o", "workspace": "w"}, org="o", workspace="w", harness_id="different", allowed_harnesses=allowed)


def test_route_uses_only_the_named_endpoint_pinned_integration(monkeypatch):
    async def docs():
        return [
            {"name": "global-winner", "provider": "local_subscription",
             "config": {"base_url": "https://evil.test/mcp", "api_key": "other"},
             "models": [{"id": "haru-codex"}]},
            {"name": "haru-grok-router", "provider": "local_subscription",
             "config": {"base_url": "https://dispatch.qiongyu.com.tw/mcp", "api_key": "router"},
             "models": [{"id": "haru-codex"}]},
        ]
    monkeypatch.setenv("HR_HARU_ROUTE_INTEGRATION", "haru-grok-router")
    monkeypatch.setenv("HR_HARU_ROUTE_MCP_ENDPOINT", "https://dispatch.qiongyu.com.tw/mcp")
    monkeypatch.setattr(gateway, "_integrations_doc", docs)
    conn = asyncio.run(gateway._haru_route_integration_conn("herdr", "haru-codex"))
    assert conn and conn["name"] == "integration:haru-grok-router"
    assert conn["base_url"] == "https://dispatch.qiongyu.com.tw/mcp"
    # Retargeting the named integration, even with a fresh key, refuses it.
    async def retargeted():
        value = (await docs())[1]
        value["config"] = {"base_url": "https://evil.test/mcp", "api_key": "fresh"}
        return [value]
    monkeypatch.setattr(gateway, "_integrations_doc", retargeted)
    assert asyncio.run(gateway._haru_route_integration_conn("herdr", "haru-codex")) is None
