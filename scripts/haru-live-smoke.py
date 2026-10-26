#!/usr/bin/env python3
"""Isolated end-to-end proof harness for the HARU Herdr backend.

This script deliberately has two phases.  ``prepare`` owns only a temporary
HARU broker, fixture and session worker.  The operator then connects the
printed endpoint to an *isolated* HarnessRouter instance.  ``verify`` makes a
real public Responses request and proves the resulting task reached the
isolated HARU worker and an owned Herdr workspace.  It never touches the
already deployed HARU Dispatch service or a real project checkout.

The controlled source acknowledgement in this fixture is evidence that the
HARU source-return contract works for the fixture adapter.  It is not evidence
that the Grok application woke an original conversation.
"""
from __future__ import annotations

import argparse
import importlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


STATE_NAME = "smoke-state.json"
MAX_BODY = 512 * 1024


class SmokeError(RuntimeError):
    pass


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


def _private(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    path.chmod(0o600)


def _read_private_json(path: Path) -> dict[str, Any]:
    if not path.is_file() or path.stat().st_mode & 0o077:
        raise SmokeError(f"private fixture state is unavailable: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise SmokeError("fixture state is malformed")
    return value


def _read_secret_file(path: str | Path, label: str) -> str:
    value_path = Path(path).expanduser().resolve(strict=True)
    info = value_path.stat()
    if not value_path.is_file() or info.st_mode & 0o077:
        raise SmokeError(f"{label} must be a private regular file")
    value = value_path.read_text(encoding="utf-8").strip()
    if not value:
        raise SmokeError(f"{label} is empty")
    return value


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _source_root(raw: str) -> Path:
    source = Path(raw).expanduser().resolve(strict=True)
    if not (source / "bin" / "haru-dispatch").is_file() or not (source / "haru_dispatch").is_dir():
        raise SmokeError("--haru-source must be a HARU Dispatch release/source checkout")
    return source


def _validate_herdr_socket(raw: str) -> Path:
    if os.environ.get("HERDR_ENV") != "1":
        raise SmokeError("HERDR_ENV=1 is required; run from an authorized Herdr pane")
    path = Path(raw).expanduser()
    if not path.is_absolute() or not path.exists():
        raise SmokeError("--herdr-socket must be an existing absolute socket path")
    return path


def _run_root(parent: str | None, requested: str | None) -> Path:
    if requested:
        root = Path(requested).expanduser().resolve()
        if root.name.startswith("haru-fork-smoke-") is False:
            raise SmokeError("--run-root basename must start with haru-fork-smoke-")
        root.mkdir(mode=0o700, parents=True, exist_ok=False)
    else:
        if not parent:
            raise SmokeError("--fixture-parent is required when --run-root is omitted")
        base = Path(parent).expanduser().resolve(strict=True)
        if not base.is_dir():
            raise SmokeError("--fixture-parent must be an existing directory")
        root = Path(tempfile.mkdtemp(prefix="haru-fork-smoke-", dir=base))
    root.chmod(0o700)
    return root


def _mcp_call(endpoint: str, token: str, name: str, arguments: dict[str, Any], timeout: float = 10) -> dict[str, Any]:
    parsed = urlsplit(endpoint)
    if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"} or parsed.path != "/mcp":
        raise SmokeError("fixture MCP endpoint must be loopback HTTP /mcp")
    payload = {"jsonrpc": "2.0", "id": "haru-fork-smoke", "method": "tools/call",
               "params": {"name": name, "arguments": arguments}}
    request = Request(endpoint, data=json.dumps(payload).encode(), method="POST", headers={
        "Authorization": "Bearer " + token, "Content-Type": "application/json", "Accept": "application/json",
    })
    try:
        with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=timeout) as response:
            raw = response.read(MAX_BODY + 1)
    except (HTTPError, URLError, OSError) as exc:
        raise SmokeError("fixture MCP request failed") from exc
    if len(raw) > MAX_BODY:
        raise SmokeError("fixture MCP response exceeded bound")
    try:
        envelope = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SmokeError("fixture MCP response was not JSON") from exc
    result = envelope.get("result") if isinstance(envelope, dict) else None
    if not isinstance(result, dict) or result.get("isError"):
        raise SmokeError("fixture MCP tool failed")
    structured = result.get("structuredContent")
    if isinstance(structured, dict):
        return structured
    content = result.get("content")
    if isinstance(content, list) and len(content) == 1 and isinstance(content[0], dict):
        text = content[0].get("text")
        if isinstance(text, str):
            try:
                parsed_text = json.loads(text)
            except json.JSONDecodeError as exc:
                raise SmokeError("fixture MCP content was not JSON") from exc
            if isinstance(parsed_text, dict):
                return parsed_text
    raise SmokeError("fixture MCP result shape is unsupported")


def _wait_mcp(endpoint: str, owner_token: str, deadline: float) -> None:
    while time.monotonic() < deadline:
        try:
            _mcp_call(endpoint, owner_token, "list_workspaces", {}, timeout=2)
            return
        except SmokeError:
            time.sleep(0.2)
    raise SmokeError("isolated HARU broker did not become ready")


def _spawn(command: list[str], env: dict[str, str], log: Path) -> subprocess.Popen[str]:
    with log.open("w", encoding="utf-8") as output:
        log.chmod(0o600)
        return subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT, text=True,
                                env=env, start_new_session=True)


def _pid_is_our_process(pid: int, expected_root: Path) -> bool:
    if pid <= 1:
        return False
    try:
        result = subprocess.run(["ps", "-p", str(pid), "-o", "command="], text=True,
                                capture_output=True, timeout=3, check=False)
    except (OSError, subprocess.SubprocessError):
        return False
    return result.returncode == 0 and str(expected_root) in result.stdout


def _stop_owned_process(pid: int, root: Path) -> None:
    if not _pid_is_our_process(pid, root):
        return
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except OSError as exc:
        raise SmokeError("could not stop an owned fixture process") from exc
    until = time.monotonic() + 5
    while time.monotonic() < until and _pid_is_our_process(pid, root):
        time.sleep(0.1)
    if _pid_is_our_process(pid, root):
        try:
            os.killpg(pid, signal.SIGKILL)
        except ProcessLookupError:
            return


def _herdr_workspaces() -> list[dict[str, Any]]:
    result = subprocess.run(["herdr", "workspace", "list"], capture_output=True, text=True, timeout=10, check=False)
    if result.returncode:
        raise SmokeError("could not inspect Herdr workspaces")
    try:
        payload = json.loads(result.stdout)
        items = payload["result"]["workspaces"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise SmokeError("Herdr workspace listing is malformed") from exc
    if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
        raise SmokeError("Herdr workspace listing is malformed")
    return items


def prepare(args: argparse.Namespace) -> int:
    source = _source_root(args.haru_source)
    socket_path = _validate_herdr_socket(args.herdr_socket)
    root = _run_root(args.fixture_parent, args.run_root)
    port = int(args.haru_port) if args.haru_port else _free_loopback_port()
    if not 1024 <= port <= 65535:
        raise SmokeError("--haru-port must be 1024..65535")
    marker = "HARU_FORK_SMOKE_" + secrets.token_hex(12)
    fixture = root / "fixture"
    fixture.mkdir(mode=0o700)
    _private(fixture / "facts.txt", marker + "\n")
    owner_token = secrets.token_urlsafe(32)
    worker_token = secrets.token_urlsafe(32)
    _private(root / "owner-token", owner_token + "\n")
    _private(root / "worker-token", worker_token + "\n")
    endpoint = f"http://127.0.0.1:{port}/mcp"
    config = {
        "usage": {"max_age_seconds": 300, "reserve_percent": 5, "refresh_codex": True},
        "local": {"max_concurrent": 1, "codex_model": "gpt-6-astra", "codex_effort": "high", "claude_model": None},
        "bridge": {"tokens": {"haru": "HARU_FORK_SMOKE_OWNER_TOKEN", "mac-worker": "HARU_FORK_SMOKE_WORKER_TOKEN"},
                   "allow_remote": False, "callback_hosts": []},
        "dispatch": {"enabled": True, "workspaces": ["fixture"], "submitters": ["haru"], "workers": ["mac-worker"],
                     "max_pending": 1, "lease_seconds": 90},
        "session_dispatch": {"enabled": True, "origins": {"fixture-source": {
            "principal": "haru", "adapter": "controlled-fixture", "session_id": "fixture-conversation",
        }}},
    }
    _private(root / "config.json", json.dumps(config, indent=2) + "\n")
    _private(root / "workspaces.json", json.dumps({"fixture": str(fixture)}, indent=2) + "\n")
    env = {**os.environ, "HARU_FORK_SMOKE_OWNER_TOKEN": owner_token,
           "HARU_FORK_SMOKE_WORKER_TOKEN": worker_token, "HERDR_ENV": "1"}
    binary = source / "bin" / "haru-dispatch"
    broker = _spawn([str(binary), "--state-dir", str(root / "state"), "--config", str(root / "config.json"),
                     "bridge", "serve", "--host", "127.0.0.1", "--port", str(port)], env, root / "broker.log")
    state = {"version": 1, "root": str(root), "state": str(root / "state"), "source": str(source), "endpoint": endpoint,
             "fixture": str(fixture), "marker": marker, "owner_token_file": str(root / "owner-token"),
             "worker_token_file": str(root / "worker-token"), "config": str(root / "config.json"),
             "workspaces": str(root / "workspaces.json"), "broker_pid": broker.pid,
             "herdr_socket": str(socket_path), "created_at": time.time()}
    try:
        _wait_mcp(endpoint, owner_token, time.monotonic() + 15)
        worker = _spawn([str(binary), "--state-dir", str(root / "state"), "--config", str(root / "config.json"),
                         "session-worker", "--endpoint", endpoint, "--token-file", str(root / "worker-token"),
                         "--workspaces", str(root / "workspaces.json"), "--herdr-socket", str(socket_path),
                         "--interval", "1", "--cleanup-grace-seconds", str(args.cleanup_grace_seconds)], env,
                        root / "worker.log")
        state["worker_pid"] = worker.pid
        _private(root / STATE_NAME, json.dumps(state, indent=2) + "\n")
    except BaseException:
        _stop_owned_process(broker.pid, root)
        raise
    print(json.dumps({
        "run_root": str(root), "haru_endpoint": endpoint, "workspace_alias": "fixture",
        "owner_token_file": str(root / "owner-token"),
        "integration": {"provider": "local_subscription", "base_url": endpoint,
                        "api_key_file": str(root / "owner-token")},
        "next": "Configure this endpoint and token only in an isolated native HarnessRouter instance, then run verify.",
    }, ensure_ascii=False))
    return 0


def _gateway_call(base: str, token: str, method: str, path: str, body: dict[str, Any] | None = None,
                  extra_headers: dict[str, str] | None = None) -> dict[str, Any]:
    parsed = urlsplit(base)
    if parsed.scheme not in {"http", "https"} or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise SmokeError("--gateway-url must be a plain HTTP(S) origin")
    url = base.rstrip("/") + path
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json", "Accept": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    request = Request(url, data=data, method=method, headers=headers)
    try:
        with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=30) as response:
            raw = response.read(MAX_BODY + 1)
    except HTTPError as exc:
        raw = exc.read(4096)
        raise SmokeError(f"HarnessRouter API rejected {method} {path} (HTTP {exc.code})") from None
    except (URLError, OSError) as exc:
        raise SmokeError(f"HarnessRouter API request failed: {method} {path}") from exc
    if len(raw) > MAX_BODY:
        raise SmokeError("HarnessRouter API response exceeded bound")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SmokeError("HarnessRouter API response was not JSON") from exc
    if not isinstance(value, dict):
        raise SmokeError("HarnessRouter API response was not an object")
    return value


def _state(root: str) -> tuple[Path, dict[str, Any]]:
    path = Path(root).expanduser().resolve(strict=True)
    if not path.name.startswith("haru-fork-smoke-"):
        raise SmokeError("--run-root is not a HARU fork smoke fixture")
    return path, _read_private_json(path / STATE_NAME)


def _request_intent_path(root: Path) -> Path:
    return root / "response-request.json"


def _save_request_intent(root: Path, value: dict[str, Any]) -> None:
    _private(_request_intent_path(root), json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def _load_request_intent(root: Path) -> dict[str, Any] | None:
    path = _request_intent_path(root)
    return _read_private_json(path) if path.exists() else None


def _ack_controlled_return(source: Path, run: dict[str, Any], task_id: str, owner_token: str) -> bool:
    """ACK only as the explicitly configured fixture source, never as Grok."""
    sys.path.insert(0, str(source))
    try:
        from haru_dispatch.config import load_config
        from haru_dispatch.session_api import SessionDispatchAPI
    except ImportError as exc:
        raise SmokeError("could not load the isolated HARU source adapter") from exc
    cfg = load_config(Path(run["state"]), Path(run["config"]))
    endpoint = str(run["endpoint"])
    messages = _mcp_call(endpoint, owner_token, "list_messages", {"limit": 100}).get("messages", [])
    if not isinstance(messages, list):
        raise SmokeError("fixture source mailbox result is malformed")
    message = next((item for item in messages if isinstance(item, dict) and item.get("task_id") == task_id and
                    item.get("kind") in {"result", "failed"}), None)
    if message is None:
        return False
    adapter = SessionDispatchAPI(Path(run["state"]), "fixture-delivery-adapter", cfg,
                                 trusted_delivery_adapter=True)
    delivery = adapter.delivery_context(str(message.get("id") or ""))
    _mcp_call(endpoint, owner_token, "ack_task_return", {
        "task_id": task_id, "message_id": str(message["id"]), "ack_capability": delivery["ack_capability"],
    })
    return True


def verify(args: argparse.Namespace) -> int:
    root, run = _state(args.run_root)
    _validate_herdr_socket(str(run["herdr_socket"]))
    source = _source_root(str(run["source"]))
    for key in ("broker_pid", "worker_pid"):
        if not isinstance(run.get(key), int) or not _pid_is_our_process(int(run[key]), root):
            raise SmokeError(f"isolated HARU {key.removesuffix('_pid')} is not running; inspect fixture logs before any retry")
    owner_token = _read_secret_file(str(run["owner_token_file"]), "fixture owner token")
    gateway_token = _read_secret_file(args.gateway_api_key_file, "gateway API key")
    _wait_mcp(str(run["endpoint"]), owner_token, time.monotonic() + 5)
    baseline = _herdr_workspaces()
    baseline_ids = {str(item.get("workspace_id") or "") for item in baseline}
    task_before = _mcp_call(str(run["endpoint"]), owner_token, "list_tasks", {}).get("tasks", [])
    if task_before:
        raise SmokeError("fixture broker is not empty; refuse to mix a prior run into this receipt")
    intent = _load_request_intent(root)
    if intent and args.create_harness:
        raise SmokeError("a saved request cannot be combined with --create-harness")
    harness_id = str(intent.get("harness_id") or "") if intent else args.harness_id
    if args.create_harness:
        created = _gateway_call(args.gateway_url, gateway_token, "POST", "/v1/harnesses", {
            "name": "HARU isolated smoke", "base": "herdr", "default_model": args.model,
            "env": {"HARU_WORKSPACE": "fixture"},
            "system_prompt": "Use only the configured HARU local-subscription backend.",
            "source": "haru-live-smoke",
        })
        harness_id = str(created.get("id") or "")
    if not harness_id:
        raise SmokeError("provide --harness-id or use --create-harness")
    if intent:
        if str(intent.get("gateway_url") or "") != args.gateway_url.rstrip("/") or str(intent.get("harness_id") or "") != harness_id:
            raise SmokeError("saved public request belongs to a different gateway or harness")
        request_id = str(intent.get("idempotency_key") or "")
        request_body = intent.get("body")
        if not request_id or not isinstance(request_body, dict):
            raise SmokeError("saved public request intent is malformed")
        response_id = str(intent.get("response_id") or "")
        if response_id:
            response = _gateway_call(args.gateway_url, gateway_token, "GET", f"/v1/responses/{response_id}")
        elif args.resume_request:
            response = _gateway_call(args.gateway_url, gateway_token, "POST", "/v1/responses", request_body,
                                     {"Idempotency-Key": request_id})
            response_id = str(response.get("id") or "")
            if not response_id:
                raise SmokeError("idempotent public request replay did not return a response id")
            intent["response_id"] = response_id
            intent["submitted_at"] = time.time()
            _save_request_intent(root, intent)
        else:
            raise SmokeError("a prior public request has unknown response id; inspect it or retry only with --resume-request")
    else:
        request_id = "haru-fork-smoke-" + secrets.token_hex(12)
        prompt = ("Read facts.txt in the registered fixture workspace and return its exact contents. "
                  "Do not modify files, invoke extra tools, or start agents. Include evidence that facts.txt was read.")
        request_body = {"model": args.model, "input": prompt, "background": True,
                        "metadata": {"harness_id": harness_id}}
        intent = {"version": 1, "gateway_url": args.gateway_url.rstrip("/"), "harness_id": harness_id,
                  "idempotency_key": request_id, "body": request_body, "response_id": "", "created_at": time.time()}
        # Persist before transmission. A transport failure then has exactly one permitted retry:
        # the same body and Idempotency-Key through --resume-request.
        _save_request_intent(root, intent)
        response = _gateway_call(args.gateway_url, gateway_token, "POST", "/v1/responses", request_body,
                                 {"Idempotency-Key": request_id})
        response_id = str(response.get("id") or "")
        if not response_id:
            raise SmokeError("HarnessRouter did not return a response id")
        intent["response_id"] = response_id
        intent["submitted_at"] = time.time()
        _save_request_intent(root, intent)
    observed_workspace = ""
    task: dict[str, Any] | None = None
    source_ack = False
    public: dict[str, Any] = response
    deadline = time.monotonic() + args.deadline
    while time.monotonic() < deadline:
        public = _gateway_call(args.gateway_url, gateway_token, "GET", f"/v1/responses/{response_id}")
        tasks = _mcp_call(str(run["endpoint"]), owner_token, "list_tasks", {}).get("tasks", [])
        if isinstance(tasks, list) and len(tasks) == 1 and isinstance(tasks[0], dict):
            task = _mcp_call(str(run["endpoint"]), owner_token, "get_task", {"task_id": tasks[0]["id"]}).get("task")
            if isinstance(task, dict):
                expected_label = "dispatch-" + str(task.get("id") or "")[:8]
                for workspace in _herdr_workspaces():
                    if workspace.get("label") == expected_label:
                        observed_workspace = str(workspace.get("workspace_id") or "")
                if task.get("execution_state") in {"succeeded", "failed", "blocked", "cancelled", "uncertain"} and not source_ack:
                    source_ack = _ack_controlled_return(source, run, str(task["id"]), owner_token)
                resource = task.get("resource") if isinstance(task.get("resource"), dict) else {}
                if task.get("execution_state") == "succeeded" and task.get("return_state") == "handled" and resource.get("state") == "closed":
                    break
        if str(public.get("status") or "") in {"failed", "cancelled"}:
            break
        time.sleep(1)
    current_ids = {str(item.get("workspace_id") or "") for item in _herdr_workspaces()}
    text = json.dumps(public, ensure_ascii=False)
    resource = task.get("resource") if isinstance(task, dict) and isinstance(task.get("resource"), dict) else {}
    passed = bool(task and task.get("execution_state") == "succeeded" and task.get("return_state") == "handled" and
                  resource.get("state") == "closed" and run["marker"] in str(task.get("result") or "") and
                  run["marker"] in text and observed_workspace and observed_workspace not in current_ids and
                  baseline_ids <= current_ids and source_ack)
    receipt = {
        "passed": passed, "response_id": response_id, "harness_id": harness_id, "model": args.model,
        "task_id": task.get("id") if isinstance(task, dict) else None,
        "execution_state": task.get("execution_state") if isinstance(task, dict) else None,
        "return_state": task.get("return_state") if isinstance(task, dict) else None,
        "resource_state": resource.get("state"), "controlled_source_ack": source_ack,
        "grok_original_conversation_verified": False, "observed_workspace": observed_workspace,
        "preexisting_workspace_count": len(baseline_ids), "preexisting_workspaces_preserved": baseline_ids <= current_ids,
        "response_status": public.get("status"), "completed_at": time.time(),
    }
    _private(root / "verification-receipt.json", json.dumps(receipt, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"receipt": str(root / "verification-receipt.json"), **receipt}, ensure_ascii=False))
    if not passed:
        raise SmokeError("live proof did not meet all gates; inspect receipt/logs and do not replay the request")
    return 0


def cleanup(args: argparse.Namespace) -> int:
    root, run = _state(args.run_root)
    for key in ("worker_pid", "broker_pid"):
        if isinstance(run.get(key), int):
            _stop_owned_process(int(run[key]), root)
    print(json.dumps({"stopped": [key for key in ("worker_pid", "broker_pid") if key in run],
                      "run_root": str(root), "files_preserved": True}))
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    prep = commands.add_parser("prepare", help="start an isolated loopback HARU fixture")
    prep.add_argument("--haru-source", required=True)
    prep.add_argument("--herdr-socket", required=True)
    prep.add_argument("--fixture-parent")
    prep.add_argument("--run-root")
    prep.add_argument("--haru-port", type=int)
    prep.add_argument("--cleanup-grace-seconds", type=int, default=5)
    verify_parser = commands.add_parser("verify", help="call an already configured isolated HarnessRouter API")
    verify_parser.add_argument("--run-root", required=True)
    verify_parser.add_argument("--gateway-url", required=True)
    verify_parser.add_argument("--gateway-api-key-file", required=True)
    verify_parser.add_argument("--harness-id")
    verify_parser.add_argument("--create-harness", action="store_true")
    verify_parser.add_argument("--resume-request", action="store_true",
                               help="retry only the saved, previously uncertain public request with its original Idempotency-Key")
    verify_parser.add_argument("--model", choices=("haru-auto", "haru-codex", "haru-claude"), default="haru-codex")
    verify_parser.add_argument("--deadline", type=int, default=240)
    clean = commands.add_parser("cleanup", help="stop only broker/worker processes owned by this fixture")
    clean.add_argument("--run-root", required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    if args.command == "prepare":
        return prepare(args)
    if args.command == "verify":
        if not 30 <= args.deadline <= 1800:
            raise SmokeError("--deadline must be 30..1800 seconds")
        return verify(args)
    return cleanup(args)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SmokeError as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        raise SystemExit(1)
