"""Atomic relocation of terminal native-worker mappings and immutable receipts."""

from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile
from typing import Callable

from .filesystem import durable_tree, sync_directory


LOCATORS = ("prompt_path", "schema_path", "report_path", "provider_report_path", "process_path")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")


class WorkerMigrationError(RuntimeError):
    """A failed migration invariant; no private receipt content is included."""


def _private_tree(path: Path) -> dict[str, str]:
    if path.is_symlink() or not path.is_dir():
        raise WorkerMigrationError("invalid worker state directory")
    result = {}
    for directory, folders, files in os.walk(path, followlinks=False):
        for name in [*folders, *files]:
            item = Path(directory) / name
            mode = item.lstat().st_mode
            if stat.S_ISLNK(mode) or not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                raise WorkerMigrationError("worker state contains an unsafe filesystem entry")
            if stat.S_ISREG(mode):
                result[item.relative_to(path).as_posix()] = hashlib.sha256(item.read_bytes()).hexdigest()
    return result


def _json_object(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError, UnicodeError) as exc:
        raise WorkerMigrationError("worker receipt is unreadable") from exc
    if not isinstance(value, dict):
        raise WorkerMigrationError("invalid worker receipt")
    return value


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _checked_mapping(root: Path, path: Path, pid_alive: Callable[[int], bool]) -> tuple[dict, dict[str, Path]]:
    mapping = _json_object(path)
    task_id = mapping.get("id")
    if (not isinstance(task_id, str) or not _IDENTIFIER.fullmatch(task_id) or ".." in task_id
            or path.name != task_id + ".json"):
        raise WorkerMigrationError("invalid worker mapping identity")
    if (mapping.get("launch_state") != "finished" or mapping.get("broker_finished") is not True
            or mapping.get("pending_events", []) != []):
        raise WorkerMigrationError("native worker is not quiescent")
    locators = {}
    for field in LOCATORS:
        value = mapping.get(field)
        if value is None:
            continue
        if not isinstance(value, str):
            raise WorkerMigrationError("invalid worker locator")
        candidate = Path(value)
        try:
            relative = candidate.relative_to(root)
        except ValueError as exc:
            raise WorkerMigrationError("worker locator is outside its state root") from exc
        if (not candidate.is_absolute() or ".." in relative.parts or len(relative.parts) < 3
                or relative.parts[:2] != ("session-worker-jobs", task_id)
                or candidate.is_symlink() or not candidate.is_file()):
            raise WorkerMigrationError("worker locator does not reference a private artifact")
        locators[field] = relative
    if locators and set(locators) != set(LOCATORS):
        raise WorkerMigrationError("worker artifact set is incomplete")
    if mapping.get("provider_command_submitted") and not locators:
        raise WorkerMigrationError("submitted worker command has no artifacts")

    if locators:
        process = _json_object(root / locators["process_path"])
        report_path = root / locators["report_path"]
        receipt = _json_object(report_path)
        for record in (process, receipt):
            if (record.get("task_id") != task_id or record.get("attempt") != mapping.get("attempt")
                    or record.get("nonce") != mapping.get("execution_nonce")):
                raise WorkerMigrationError("worker receipt identity mismatch")
        if process.get("finished_at") is None:
            raise WorkerMigrationError("native process has no terminal receipt")
        for field in ("wrapper_pid", "provider_pid"):
            pid = process.get(field)
            if type(pid) is int and pid > 1 and pid_alive(pid):
                raise WorkerMigrationError("a recorded native process is still present")
        for field in ("accepted_receipt_sha256", "rejected_receipt_sha256"):
            expected = mapping.get(field)
            if expected is not None and expected != hashlib.sha256(report_path.read_bytes()).hexdigest():
                raise WorkerMigrationError("worker receipt digest mismatch")
        if receipt.get("cwd") != mapping.get("cwd") or receipt.get("provider") != mapping.get("provider"):
            raise WorkerMigrationError("worker receipt execution binding mismatch")
    return mapping, locators


def inspect_worker_state(source: str | os.PathLike, *, pid_alive: Callable[[int], bool] = _pid_alive) -> dict:
    """Read-only plan used before the operator fences the old claim loop."""
    root = Path(source).expanduser().absolute()
    _private_tree(root)
    jobs = root / "session-worker-jobs"
    if not jobs.is_dir():
        raise WorkerMigrationError("native worker mappings are missing")
    mappings = []
    for path in sorted(jobs.glob("*.json")):
        mapping, locators = _checked_mapping(root, path, pid_alive)
        mappings.append({"task_id": mapping["id"], "locator_fields": sorted(locators)})
    return {"session_mappings": mappings, "mapping_count": len(mappings),
            "legacy_state_excluded": ["mcp-worker-jobs", "tasks.sqlite3", "tasks", "locks"],
            "provider_processes_present": False}


def migrate_worker_state(source: str | os.PathLike, destination: str | os.PathLike, *,
                         quiesced: bool = False, pid_alive: Callable[[int], bool] = _pid_alive) -> dict:
    """Copy terminal state and rewrite only the five mapping file locators.

    The caller must first disable the old worker's claim/restart paths. Immutable
    execution receipts keep their original bytes, identities and digest values.
    """
    if quiesced is not True:
        raise WorkerMigrationError("old worker fencing is required")
    root = Path(source).expanduser().absolute()
    target = Path(destination).expanduser().absolute()
    resolved_root, resolved_target = root.resolve(), target.resolve()
    if (resolved_target == resolved_root or resolved_root in resolved_target.parents
            or resolved_target in resolved_root.parents):
        raise WorkerMigrationError("worker destination overlaps its source")
    if target.exists() or target.is_symlink():
        raise WorkerMigrationError("worker destination must not exist")
    original = _private_tree(root)
    report = inspect_worker_state(root, pid_alive=pid_alive)
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".worker-import-", dir=target.parent))
    stage.chmod(0o700)
    try:
        stage_jobs = stage / "session-worker-jobs"
        stage_jobs.mkdir(mode=0o700)
        artifact_hashes = {}
        for path in sorted((root / "session-worker-jobs").glob("*.json")):
            mapping, locators = _checked_mapping(root, path, pid_alive)
            task_dir = root / "session-worker-jobs" / mapping["id"]
            if task_dir.exists():
                hashes = _private_tree(task_dir)
                shutil.copytree(task_dir, stage_jobs / mapping["id"])
                if hashes != _private_tree(stage_jobs / mapping["id"]):
                    raise WorkerMigrationError("copied native artifacts changed")
                artifact_hashes.update({mapping["id"] + "/" + key: value for key, value in hashes.items()})
            updated = copy.deepcopy(mapping)
            for field, relative in locators.items():
                updated[field] = str(target / relative)
            copied = stage_jobs / path.name
            copied.write_text(json.dumps(updated, sort_keys=True, indent=2) + "\n")
            copied.chmod(0o600)
            # Removing the rewritten fields must leave exactly the old mapping.
            preserved = {key: value for key, value in updated.items() if key not in locators}
            if preserved != {key: value for key, value in mapping.items() if key not in locators}:
                raise WorkerMigrationError("worker ownership fields changed")
        contexts = root / "session-native-contexts"
        if contexts.is_dir():
            output = stage / "session-native-contexts"
            output.mkdir(mode=0o700)
            for path in contexts.glob("*.json"):
                shutil.copy2(path, output / path.name)
                if path.read_bytes() != (output / path.name).read_bytes():
                    raise WorkerMigrationError("native context changed")
        for name in ("codex.json", "claude.json", "grok.json", ".claude-statusline-receipt.json"):
            path = root / name
            if path.exists():
                shutil.copy2(path, stage / name)
                (stage / name).chmod(0o600)
        if original != _private_tree(root):
            raise WorkerMigrationError("source worker state changed during migration")
        report["artifact_count"] = len(artifact_hashes)
        report["artifact_hashes_preserved"] = True
        report["source_unchanged"] = True
        report["locator_fields"] = list(LOCATORS)
        (stage / ".worker-migration.json").write_text(json.dumps(report, indent=2) + "\n")
        (stage / ".worker-migration.json").chmod(0o600)
        durable_tree(stage)
        if target.exists() or target.is_symlink():
            raise WorkerMigrationError("worker destination changed during migration")
        os.replace(stage, target)
        sync_directory(target.parent)
        return report
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise
