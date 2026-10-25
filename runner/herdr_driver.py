"""Safe remote driver for HARU Dispatch's Herdr-backed local subscriptions.

This module deliberately contains no Codex, Claude, or host-shell invocation.
It is a small MCP client that submits a constrained, read-only task to a
separately authenticated HARU host.  The host retains subscription OAuth,
quota admission, project allowlists, Herdr ownership, and source delivery.

Runner integration writes a private job file with exactly ``cwd``, ``prompt``,
``agent_doc``, ``model``, ``workspace``, ``request_id``,
``resume_session_id``, ``timeout_seconds``, and ``base_url``.  The bearer token
is supplied only in ``HR_HERDR_TOKEN``.  Output is Qoder-compatible NDJSON:
``__hr_init``, honest polling ``notice`` events, optional ``text``, then one
``__hr_result`` whose ``remote`` field reports host execution/return/resource
facts.  The driver never ACKs source delivery and never deletes HARU records.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import sys
import tempfile
import time
from typing import Any, Callable, Mapping
from urllib import error, parse, request

import fcntl


STATE_VERSION = 1
STATE_DIR = ".harness/herdr"
MAX_BODY = 512 * 1024
MAX_RESULT = 48_000
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_MODELS = {"haru-auto": None, "haru-codex": "codex", "haru-claude": "claude"}
_ACTIVE = {"queued", "running", "waiting_return", "session_running", "session_result_ready"}
_JOB_FIELDS = {"cwd", "prompt", "agent_doc", "model", "workspace", "request_id",
               "resume_session_id", "timeout_seconds", "base_url"}


class DriverError(ValueError):
    """A safe, non-secret error that may be shown to a task caller."""


class TransportError(RuntimeError):
    """The host may have received an idempotent request but did not answer."""


def _emit(method: str, payload: Mapping[str, Any]) -> None:
    sys.stdout.write(json.dumps({"m": method, "p": dict(payload)}, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def _sha(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def validate_endpoint(value: Any) -> str:
    if not isinstance(value, str) or len(value) > 2048:
        raise DriverError("HARU endpoint is invalid")
    parsed = parse.urlsplit(value)
    local_http = parsed.scheme == "http" and parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    if (parsed.scheme != "https" and not local_http or not parsed.hostname or parsed.username or
            parsed.password or parsed.query or parsed.fragment or parsed.path != "/mcp"):
        raise DriverError("HARU endpoint must be HTTPS /mcp (or localhost HTTP /mcp)")
    return value


def _text(value: Any, label: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip() or "\x00" in value or len(value.encode()) > maximum:
        raise DriverError(f"invalid {label}")
    return value


def _state_directory(cwd: Path) -> Path:
    """Create a private path under the runner workspace without following links."""
    root = cwd.resolve(strict=True)
    current = root
    for segment in (".harness", "herdr"):
        current = current / segment
        if current.is_symlink():
            raise DriverError("HARU task state path may not be a symlink")
        current.mkdir(mode=0o700, exist_ok=True)
        if current.is_symlink() or not current.is_dir() or current.stat().st_mode & 0o077:
            raise DriverError("HARU task state directory is not private")
    try:
        current.resolve(strict=True).relative_to(root)
    except ValueError as exc:
        raise DriverError("HARU task state escapes the workspace") from exc
    return current


def _state_path(cwd: Path, request_id: str) -> Path:
    return _state_directory(cwd) / f"{request_id}.json"


def _write_private(path: Path, value: Mapping[str, Any]) -> None:
    if path.is_symlink() or path.exists() and not path.is_file():
        raise DriverError("HARU task state is invalid")
    fd, temporary_name = tempfile.mkstemp(prefix=".state-", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            json.dump(value, output, ensure_ascii=False, sort_keys=True)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
    finally:
        if temporary.exists():
            temporary.unlink()


def _read_private(path: Path) -> dict[str, Any] | None:
    try:
        if path.is_symlink():
            raise DriverError("HARU task state is invalid")
        if not path.exists():
            return None
        if not path.is_file():
            raise DriverError("HARU task state is invalid")
        if path.stat().st_mode & 0o077:
            raise DriverError("HARU task state is not private")
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        raise DriverError("HARU task state is unreadable") from exc
    if not isinstance(value, dict):
        raise DriverError("HARU task state is invalid")
    return value


class _StateLock:
    """One active driver owns one idempotency record; a second driver must not poll or cancel it."""

    def __init__(self, directory: Path, request_id: str):
        locks = directory / ".locks"
        if locks.exists() and locks.is_symlink():
            raise DriverError("HARU task lock path may not be a symlink")
        locks.mkdir(mode=0o700, exist_ok=True)
        if locks.is_symlink() or not locks.is_dir() or locks.stat().st_mode & 0o077:
            raise DriverError("HARU task lock directory is not private")
        self.path = locks / f"{request_id}.lock"
        flags = os.O_RDWR | os.O_CREAT
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            self.fd = os.open(self.path, flags, 0o600)
            info = os.fstat(self.fd)
            if not stat.S_ISREG(info.st_mode):
                raise DriverError("HARU task lock is invalid")
            os.fchmod(self.fd, 0o600)
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            try:
                os.close(self.fd)
            except AttributeError:
                pass
            raise DriverError("HARU task is already active") from exc
        except OSError as exc:
            try:
                os.close(self.fd)
            except AttributeError:
                pass
            raise DriverError("HARU task lock is unavailable") from exc

    def close(self) -> None:
        if getattr(self, "fd", None) is not None:
            os.close(self.fd)
            self.fd = None


def _content_result(reply: Any) -> dict[str, Any]:
    if not isinstance(reply, dict) or isinstance(reply.get("error"), dict):
        raise DriverError("HARU returned an invalid MCP response")
    result = reply.get("result")
    if not isinstance(result, dict) or result.get("isError") is True:
        raise DriverError("HARU rejected the request")
    structured = result.get("structuredContent")
    if isinstance(structured, dict):
        return structured
    content = result.get("content")
    if (isinstance(content, list) and len(content) == 1 and isinstance(content[0], dict) and
            isinstance(content[0].get("text"), str)):
        try:
            parsed = json.loads(content[0]["text"])
        except json.JSONDecodeError as exc:
            raise DriverError("HARU returned an invalid tool response") from exc
        if isinstance(parsed, dict):
            return parsed
    raise DriverError("HARU returned an unsupported tool response")


class MCPClient:
    """A bounded JSON-RPC client which never follows an authenticated redirect."""

    def __init__(self, endpoint: str, token: str, timeout: float = 15, opener=None):
        self.endpoint = validate_endpoint(endpoint)
        self.token = _text(token, "HARU access token", 4096)
        self.timeout = max(1.0, min(float(timeout), 60.0))
        self.opener = opener or request.build_opener(request.ProxyHandler({}), _NoRedirect())

    def call(self, name: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(name, str) or not isinstance(arguments, Mapping):
            raise DriverError("invalid MCP request")
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                           "params": {"name": name, "arguments": dict(arguments)}}).encode()
        req = request.Request(self.endpoint, body, {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "HarnessRouter-Herdr-Driver/1",
        }, method="POST")
        try:
            with self.opener.open(req, timeout=self.timeout) as response:
                if response.status != 200:
                    raise TransportError("HARU MCP request failed")
                raw = response.read(MAX_BODY + 1)
        except (error.URLError, error.HTTPError, OSError) as exc:
            raise TransportError("HARU MCP did not answer") from exc
        if len(raw) > MAX_BODY:
            raise DriverError("HARU MCP response is oversized")
        try:
            return _content_result(json.loads(raw))
        except json.JSONDecodeError as exc:
            raise DriverError("HARU MCP returned invalid JSON") from exc


class _NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


def _unsupported(job: Mapping[str, Any]) -> None:
    for key in ("files", "mcp_servers", "skills", "plugins", "tools_disabled", "image_auth", "environment"):
        if job.get(key):
            raise DriverError(f"Herdr backend does not support {key.replace('_', ' ')}")


def _job(job: Mapping[str, Any], environ: Mapping[str, str]) -> tuple[dict[str, Any], Path, str, str]:
    if not isinstance(job, Mapping):
        raise DriverError("invalid Herdr job")
    if set(job) - _JOB_FIELDS:
        raise DriverError("Herdr job contains unsupported fields")
    _unsupported(job)
    cwd = Path(_text(job.get("cwd"), "working directory", 4096)).resolve(strict=True)
    request_id = _text(job.get("request_id"), "request id", 128)
    if not _ID.fullmatch(request_id):
        raise DriverError("invalid request id")
    workspace = _text(job.get("workspace"), "HARU workspace alias", 128)
    if workspace != environ.get("HARU_WORKSPACE") or not _ID.fullmatch(workspace):
        raise DriverError("HARU workspace must match the configured alias")
    model = job.get("model", "haru-auto")
    if model not in _MODELS:
        raise DriverError("invalid local subscription model")
    prompt = _text(job.get("prompt"), "prompt", 16 * 1024)
    agent_doc = job.get("agent_doc") or ""
    if not isinstance(agent_doc, str) or len(agent_doc.encode()) > 64 * 1024:
        raise DriverError("invalid agent instructions")
    endpoint = validate_endpoint(job.get("base_url"))
    token = _text(environ.get("HR_HERDR_TOKEN"), "HARU access token", 4096)
    timeout = job.get("timeout_seconds", 600)
    if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or not 1 <= timeout <= 21600:
        raise DriverError("invalid timeout")
    resume = job.get("resume_session_id") or ""
    if resume and (not isinstance(resume, str) or not resume.startswith("haru:") or not _ID.fullmatch(resume[5:])):
        raise DriverError("invalid HARU continuation id")
    task = prompt if not agent_doc else agent_doc.strip() + "\n\nTask:\n" + prompt
    payload: dict[str, Any] = {"task": task, "workspace": workspace, "request_id": request_id,
                               "mode": "analysis", "result_contract": "evidence_required"}
    provider = _MODELS[model]
    if provider:
        payload["requested_provider"] = provider
    if resume:
        payload["continue_task_id"] = resume[5:]
    return payload, cwd, endpoint, token


def _facts(task: Mapping[str, Any]) -> dict[str, Any]:
    resource = task.get("resource") if isinstance(task.get("resource"), Mapping) else {}
    delivery = task.get("return_delivery") if isinstance(task.get("return_delivery"), Mapping) else {}
    return {"execution_state": task.get("execution_state"), "return_state": task.get("return_state"),
            "resource_state": resource.get("state"), "delivery_stage": delivery.get("stage"),
            "selected_provider": task.get("selected_provider")}


def _safe_output(value: Any, token: str, maximum: int = MAX_RESULT) -> str:
    """A compromised or misconfigured host response must not reflect its bearer token."""
    text = str(value or "")
    if token:
        text = text.replace(token, "[redacted]")
    return text[:maximum]


def _redact(value: Any, token: str) -> Any:
    """Redact the bearer from every event field, including untrusted host facts."""
    if isinstance(value, str):
        return value.replace(token, "[redacted]") if token else value
    if isinstance(value, dict):
        return {str(key): _redact(item, token) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact(item, token) for item in value]
    if isinstance(value, tuple):
        return [_redact(item, token) for item in value]
    return value


class Turn:
    def __init__(self, job: Mapping[str, Any], *, environ=None, client_factory=MCPClient,
                 emit=_emit, now=time.monotonic, sleep=time.sleep):
        self.environ = dict(os.environ if environ is None else environ)
        self.payload, self.cwd, self.endpoint, self.token = _job(job, self.environ)
        self.timeout = float(job.get("timeout_seconds", 600))
        self.emit = lambda method, payload: emit(method, _redact(payload, self.token))
        self.now, self.sleep = now, sleep
        self.client = client_factory(self.endpoint, self.token, timeout=min(self.timeout, 60))
        self.state_path = _state_path(self.cwd, self.payload["request_id"])
        self.lock = _StateLock(self.state_path.parent, self.payload["request_id"])
        try:
            self.state = self._state()
        except BaseException:
            self.lock.close()
            raise
        self.cancelled = False

    def close(self) -> None:
        self.lock.close()

    def _state(self) -> dict[str, Any]:
        expected = {"version": STATE_VERSION, "request_id": self.payload["request_id"],
                    "endpoint": self.endpoint, "auth_sha256": _sha(self.token),
                    "payload": self.payload, "payload_sha256": _sha(self.payload), "task_id": None}
        previous = _read_private(self.state_path)
        if previous is None:
            _write_private(self.state_path, expected)  # durable submission intent before network I/O
            return expected
        if any(previous.get(key) != expected[key] for key in ("version", "request_id", "endpoint", "auth_sha256", "payload_sha256")):
            raise DriverError("HARU task state does not match this authenticated request")
        if previous.get("payload") != self.payload or previous.get("task_id") not in {None, ""} and not _ID.fullmatch(str(previous.get("task_id"))):
            raise DriverError("HARU task state is invalid")
        return previous

    def _save(self) -> None:
        _write_private(self.state_path, self.state)

    def _submit(self) -> Mapping[str, Any]:
        self._validate_continuation()
        # Persist intent before the first POST.  A signal or broken response at
        # this point has an unknown host outcome and must never be called
        # "before submission" or replayed under a new request id.
        self.state["submission_attempted"] = True
        self._save()
        for attempt in range(2):
            try:
                response = self.client.call("submit_session_task", self.payload)
            except TransportError:
                if attempt:
                    raise
                continue
            task = response.get("task") if isinstance(response, Mapping) else None
            if not isinstance(task, Mapping) or not _ID.fullmatch(str(task.get("id", ""))):
                raise DriverError("HARU accepted an invalid task")
            self.state["task_id"] = task["id"]
            self._save()
            return task
        raise TransportError("HARU MCP did not answer")

    def _validate_continuation(self) -> None:
        """Only a state record from this connection can authorize a native resume."""
        predecessor = self.payload.get("continue_task_id")
        if not predecessor:
            return
        owns_predecessor = False
        directory = self.state_path.parent
        if directory.is_dir():
            for candidate in directory.glob("*.json"):
                value = _read_private(candidate)
                if (value and value.get("task_id") == predecessor and value.get("endpoint") == self.endpoint and
                        value.get("auth_sha256") == _sha(self.token)):
                    owns_predecessor = True
                    break
        if not owns_predecessor:
            raise DriverError("HARU continuation is not owned by this authenticated connection")
        response = self.client.call("get_task", {"task_id": predecessor})
        task = response.get("task") if isinstance(response, Mapping) else None
        resource = task.get("resource") if isinstance(task, Mapping) and isinstance(task.get("resource"), Mapping) else {}
        if (not isinstance(task, Mapping) or task.get("execution_state") != "succeeded" or
                task.get("return_state") != "handled" or resource.get("state") in {"closed", "cleanup_uncertain", "retained"}):
            raise DriverError("HARU continuation is no longer eligible")

    def _task(self) -> Mapping[str, Any]:
        task_id = self.state.get("task_id")
        if not task_id:
            return self._submit()
        response = self.client.call("get_task", {"task_id": task_id})
        task = response.get("task") if isinstance(response, Mapping) else None
        if not isinstance(task, Mapping) or task.get("id") != task_id:
            raise DriverError("HARU returned an invalid task state")
        return task

    def cancel(self) -> str:
        task_id = self.state.get("task_id")
        if not task_id:
            return ("remote_stop_unknown" if self.state.get("submission_attempted")
                    else "stop_requested_before_host_submission")
        try:
            # Do not direct a mutation at a merely remembered id without first
            # proving the host still returns that exact task to this principal.
            self._task()
            self.client.call("cancel_session_task", {"task_id": task_id})
        except (DriverError, TransportError):
            return "remote_stop_unknown"
        return "remote_stop_unknown"

    def run(self) -> int:
        try:
            task = self._task()
            task_id = str(task["id"])
            self.emit("__hr_init", {"session_id": "haru:" + task_id})
            deadline = self.now() + self.timeout
            last = None
            while True:
                state = (task.get("execution_state"), task.get("return_state"), _facts(task).get("resource_state"))
                if state != last:
                    self.emit("notice", {"text": "HARU task state: " + "/".join(str(v or "-") for v in state)})
                    last = state
                    outcome = self._outcome(task, self.token)
                if outcome is not None:
                    status, reason = outcome
                    final = _safe_output(task.get("result"), self.token)
                    if final:
                        self.emit("text", {"text": final})
                    self.emit("__hr_result", {"status": status, "reason": reason, "final": final,
                                                "session_id": "haru:" + task_id, "remote": _facts(task)})
                    return 0
                if self.now() >= deadline:
                    reason = self.cancel()
                    self.emit("__hr_result", {"status": "incomplete", "reason": reason, "final": "",
                                                "session_id": "haru:" + task_id, "remote": _facts(task)})
                    return 0
                self.sleep(1)
                task = self._task()
        except TransportError:
            # The task id, if persisted, is evidence that this driver must not
            # rerun work. The host may still be working after a polling failure.
            self.emit("__hr_result", {"status": "incomplete", "reason": "remote_outcome_unknown", "final": "",
                                        "session_id": "haru:" + str(self.state.get("task_id") or ""),
                                        "remote": {"execution_state": "unknown"}})
            return 0
        except DriverError as exc:
            if self.state.get("submission_attempted") or self.state.get("task_id"):
                self.emit("__hr_result", {"status": "incomplete", "reason": "remote_outcome_unknown", "final": "",
                                            "session_id": "haru:" + str(self.state.get("task_id") or ""),
                                            "remote": {"execution_state": "unknown"}})
            else:
                self.emit("__hr_result", {"status": "failed", "reason": str(exc), "final": "", "session_id": ""})
            return 0
        finally:
            self.close()

    @staticmethod
    def _outcome(task: Mapping[str, Any], token: str) -> tuple[str, str] | None:
        execution = task.get("execution_state")
        if execution == "succeeded":
            if isinstance(task.get("result"), str) and task["result"].strip():
                return "completed", ""
            return "failed", "HARU task succeeded without a result"
        if execution in {"failed", "blocked"}:
            return "failed", _safe_output(task.get("result") or "HARU task failed", token, 1000)
        if execution == "waiting_input":
            return "incomplete", "HARU task needs an answer; this backend cannot fabricate one"
        if execution in {"uncertain", "cancelled"}:
            return "incomplete", "HARU task outcome is not confirmed"
        return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("job")
    args = parser.parse_args(argv)
    try:
        raw = json.loads(Path(args.job).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _emit("__hr_result", {"status": "failed", "reason": "Herdr driver job is unreadable", "final": "", "session_id": ""})
        return 0
    turn: Turn | None = None
    try:
        turn = Turn(raw)
        def on_term(_signum, _frame):  # noqa: ANN001
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, on_term)
        return turn.run()
    except KeyboardInterrupt:
        reason = turn.cancel() if turn else "remote_stop_unknown"
        _emit("__hr_result", {"status": "incomplete", "reason": reason, "final": "", "session_id": ""})
        return 0
    except (DriverError, TransportError) as exc:
        _emit("__hr_result", {"status": "failed", "reason": str(exc), "final": "", "session_id": ""})
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
