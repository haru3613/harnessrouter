from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PATH = ROOT / "docker-compose.integrated-dispatch.yml"
DOCKERFILE_PATH = ROOT / "docker" / "dispatch.Dockerfile"


def _compose() -> dict:
    return yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))


def test_private_dispatch_artifact_is_a_verified_named_build_context() -> None:
    compose = _compose()
    build = compose["services"]["source-gateway"]["build"]
    dockerfile = DOCKERFILE_PATH.read_text(encoding="utf-8")

    assert build["additional_contexts"] == {
        "dispatch_source": "${HR_DISPATCH_SOURCE_DIR:?Set the private dispatch source artifact directory}"
    }
    assert set(build["args"]) == {
        "HR_DISPATCH_SOURCE_REVISION",
        "HR_DISPATCH_ARTIFACT_SHA256",
        "HR_SOURCE_GATEWAY_REVISION",
    }
    assert "COPY --from=dispatch_source" in dockerfile
    assert 'tree_hash.update(b"\\0")' in dockerfile
    assert 'tree_hash.update(sha256(path.read_bytes()).digest())' in dockerfile
    assert "private dispatch artifact revision does not match" in dockerfile
    assert "private dispatch artifact tree hash does not match" in dockerfile
    assert 'org.opencontainers.image.revision="${HR_SOURCE_GATEWAY_REVISION}"' in dockerfile
    assert 'dispatch-source.revision="${HR_DISPATCH_SOURCE_REVISION}"' in dockerfile
    assert "USER 501:10001" in dockerfile


def test_source_credentials_and_state_do_not_cross_service_boundaries() -> None:
    compose = _compose()
    services = compose["services"]
    source = services["source-gateway"]
    tunnel = services["tunnel"]
    router = services["harnessrouter"]

    assert "secrets" not in router
    assert "volumes" not in router
    assert set(source["secrets"]) == set(compose["secrets"])
    assert {
        key: value
        for key, value in source["environment"].items()
        if key.endswith("_FILE")
    } == {
        "HARU_BRIDGE_HARU_TOKEN_FILE": "/run/secrets/haru_token",
        "HARU_BRIDGE_DOTS_TOKEN_FILE": "/run/secrets/dots_token",
        "HARU_BRIDGE_WORKER_TOKEN_FILE": "/run/secrets/worker_token",
        "HARU_BRIDGE_ROUTER_TOKEN_FILE": "/run/secrets/router_token",
        "HARU_GROK_WEBHOOK_URL_FILE": "/run/secrets/grok_url",
        "HARU_GROK_WEBHOOK_TOKEN_FILE": "/run/secrets/grok_token",
        "HARU_ROUTER_API_KEY_FILE": "/run/secrets/router_api_key",
    }
    assert source["volumes"][0] == "dispatch-state:/state"
    assert all("named-tunnel-token" not in str(mount) for mount in source["volumes"])
    assert "secrets" not in tunnel
    assert len(tunnel["volumes"]) == 1
    assert tunnel["volumes"][0]["target"] == "/run/secrets/named-tunnel-token"


def test_tunnel_and_router_are_fenced_onto_separate_networks() -> None:
    compose = _compose()
    services = compose["services"]

    assert set(compose["networks"]) == {"ingress", "route"}
    assert all("external" not in (network or {}) for network in compose["networks"].values())
    assert services["harnessrouter"]["networks"] == ["route"]
    assert set(services["source-gateway"]["networks"]) == {"ingress", "route"}
    assert services["source-gateway"]["networks"]["ingress"]["aliases"] == ["mcp"]
    assert services["tunnel"]["networks"] == ["ingress"]
    assert services["tunnel"]["command"][-1] == "/run/secrets/named-tunnel-token"
    assert "http://mcp:8769" not in str(services["tunnel"]["command"])
    assert services["harnessrouter"]["environment"]["HR_HARU_ROUTE_MCP_ENDPOINT"] == (
        "${HARU_ROUTE_MCP_ENDPOINT:?Set the fixed public HTTPS MCP endpoint}"
    )


def test_source_gateway_has_the_required_runtime_confinement() -> None:
    source = _compose()["services"]["source-gateway"]

    assert source["environment"]["HARU_DISPATCH_STATE_DIR"] == "/state/current"
    assert source["user"] == "501:10001"
    assert source["read_only"] is True
    assert source["cap_drop"] == ["ALL"]
    assert source["security_opt"] == ["no-new-privileges:true"]
    assert source["mem_limit"] == "256m"
    assert source["cpus"] == 0.5
    assert source["ports"] == ["127.0.0.1:8769:8769"]
    assert source["healthcheck"]["test"] == [
        "CMD",
        "python",
        "-m",
        "source_gateway.runtime",
        "health",
    ]
