"""Private adoption grants for staged HARU routes.

The public Responses payload never carries these grants. The gateway verifies a
dedicated API key, then signs a descriptor for its local runner. The capability
itself is only delivered in the runner process environment.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
from typing import Any, Mapping

_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{1,127}$")
_HEX = re.compile(r"^[0-9a-f]{64}$")
_CAP = re.compile(r"^[A-Za-z0-9_-]{16,256}$")
HEADER_TASK = "x-haru-route-task"
HEADER_FINGERPRINT = "x-haru-route-fingerprint"
HEADER_CAPABILITY = "x-haru-route-capability"


class RouteError(ValueError):
    pass


def _text(value: object, label: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise RouteError(f"invalid HARU route {label}")
    return value


def incoming(headers: Mapping[str, str], bearer: str) -> dict[str, str] | None:
    values = {"task_id": headers.get(HEADER_TASK, ""),
              "fingerprint": headers.get(HEADER_FINGERPRINT, ""),
              "capability": headers.get(HEADER_CAPABILITY, "")}
    if not any(values.values()):
        return None
    expected = os.environ.get("HR_HARU_ROUTE_API_KEY_SHA256", "")
    actual = hashlib.sha256(bearer.encode()).hexdigest() if bearer else ""
    if not _HEX.fullmatch(expected) or not hmac.compare_digest(expected, actual):
        raise RouteError("HARU route credential is not authorized")
    return {"task_id": _text(values["task_id"], "task id", _ID),
            "fingerprint": _text(values["fingerprint"], "fingerprint", _HEX),
            "capability": _text(values["capability"], "capability", _CAP)}


def scope_authorized(principal: Mapping[str, object] | None, *, org: str,
                     workspace: str, harness_id: str, allowed_harnesses: set[str]) -> bool:
    """Route headers require a live API-key principal, not ambient BFF identity."""
    return bool(principal and principal.get("org") == org and workspace and
                principal.get("workspace") == workspace and harness_id in allowed_harnesses)


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(dict(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def sign(route: Mapping[str, str], response_id: str, harness_id: str, key: str) -> dict[str, str]:
    task_id = _text(route.get("task_id"), "task id", _ID)
    fingerprint = _text(route.get("fingerprint"), "fingerprint", _HEX)
    capability = _text(route.get("capability"), "capability", _CAP)
    response_id = _text(response_id, "response id", _ID)
    harness_id = _text(harness_id, "harness id", _ID)
    if not key:
        raise RouteError("internal route signer is unavailable")
    public = {"task_id": task_id, "fingerprint": fingerprint, "response_id": response_id,
              "harness_id": harness_id, "capability_sha256": hashlib.sha256(capability.encode()).hexdigest()}
    proof = hmac.new(key.encode(), _canonical(public), hashlib.sha256).hexdigest()
    return {**public, "proof": proof}


def verify(descriptor: object, capability: object, key: str) -> dict[str, str]:
    if not isinstance(descriptor, dict):
        raise RouteError("HARU route descriptor is invalid")
    if set(descriptor) != {"task_id", "fingerprint", "response_id", "harness_id", "capability_sha256", "proof"}:
        raise RouteError("HARU route descriptor is invalid")
    task_id = _text(descriptor.get("task_id"), "task id", _ID)
    fingerprint = _text(descriptor.get("fingerprint"), "fingerprint", _HEX)
    response_id = _text(descriptor.get("response_id"), "response id", _ID)
    harness_id = _text(descriptor.get("harness_id"), "harness id", _ID)
    capability_sha256 = _text(descriptor.get("capability_sha256"), "capability digest", _HEX)
    proof = _text(descriptor.get("proof"), "proof", _HEX)
    capability = _text(capability, "capability", _CAP)
    if not key:
        raise RouteError("internal route signer is unavailable")
    if not hmac.compare_digest(capability_sha256, hashlib.sha256(capability.encode()).hexdigest()):
        raise RouteError("HARU route capability verification failed")
    expected = hmac.new(key.encode(), _canonical({"task_id": task_id, "fingerprint": fingerprint,
        "response_id": response_id, "harness_id": harness_id, "capability_sha256": capability_sha256}), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, proof):
        raise RouteError("HARU route descriptor verification failed")
    return {"task_id": task_id, "fingerprint": fingerprint, "response_id": response_id,
            "harness_id": harness_id, "capability_sha256": capability_sha256}
