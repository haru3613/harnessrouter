"""Read-only validation of migrated Dispatch state and compatibility artifacts."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from pathlib import Path
import sqlite3
import stat
from typing import Any


_DATABASES = ("jobs.sqlite3", "bridge.sqlite3", "oauth.sqlite3")
_HEX = frozenset("0123456789abcdef")


class StateValidationError(RuntimeError):
    """The imported state is unsafe to start.

    Messages deliberately identify only the failed invariant. They never contain
    migrated identifiers, secret names, secret values, or database contents.
    """


def _regular_file(path: Path, label: str) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as exc:
        raise StateValidationError(f"missing {label}") from exc
    if not stat.S_ISREG(mode) or path.is_symlink():
        raise StateValidationError(f"invalid {label}")


def _hex_digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(c not in _HEX for c in value):
        raise StateValidationError(f"invalid {label}")
    return value


def _revision(value: Any) -> str:
    if not isinstance(value, str) or len(value) != 40 or any(c not in _HEX for c in value):
        raise StateValidationError("invalid source revision")
    return value


def _string_list(value: Any, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise StateValidationError(f"invalid {label}")
    if len(set(value)) != len(value):
        raise StateValidationError(f"invalid {label}")
    return tuple(value)


def _load_manifest(path: Path) -> dict[str, Any]:
    _regular_file(path, "migration manifest")
    try:
        if path.stat().st_size > 1024 * 1024:
            raise StateValidationError("invalid migration manifest")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise StateValidationError("invalid migration manifest") from exc
    if not isinstance(value, dict):
        raise StateValidationError("invalid migration manifest")
    required = {
        "schema_version", "source_revision", "artifact_tree_sha256", "salt",
        "original_job_ids", "original_client_ids", "session_secret_digests",
        "required_databases",
    }
    if set(value) != required or value["schema_version"] != 1:
        raise StateValidationError("unsupported migration manifest")
    _revision(value["source_revision"])
    _hex_digest(value["artifact_tree_sha256"], "artifact digest")
    _hex_digest(value["salt"], "manifest salt")
    _string_list(value["original_job_ids"], "job baseline")
    _string_list(value["original_client_ids"], "client baseline")
    digests = value["session_secret_digests"]
    if not isinstance(digests, dict) or any(not isinstance(name, str) or not name for name in digests):
        raise StateValidationError("invalid secret baseline")
    for digest in digests.values():
        _hex_digest(digest, "secret digest")
    if value["required_databases"] != list(_DATABASES):
        raise StateValidationError("invalid database manifest")
    return value


def artifact_tree_sha256(root: str | os.PathLike[str]) -> str:
    """Hash the pinned compatibility tree without following links or pycache."""
    root_path = Path(root)
    try:
        mode = root_path.lstat().st_mode
    except OSError as exc:
        raise StateValidationError("missing compatibility artifact") from exc
    if not stat.S_ISDIR(mode) or root_path.is_symlink():
        raise StateValidationError("invalid compatibility artifact")

    required = (
        root_path / "DISPATCH_SOURCE_REVISION",
        root_path / "bin" / "haru-dispatch",
        root_path / "haru_dispatch" / "__init__.py",
    )
    for path in required:
        _regular_file(path, "compatibility artifact")

    files: list[tuple[str, Path]] = []
    try:
        for path in root_path.rglob("*"):
            relative = path.relative_to(root_path)
            if "__pycache__" in relative.parts:
                continue
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                raise StateValidationError("invalid compatibility artifact")
            if stat.S_ISREG(mode):
                files.append((relative.as_posix(), path))
            elif not stat.S_ISDIR(mode):
                raise StateValidationError("invalid compatibility artifact")
    except OSError as exc:
        raise StateValidationError("unreadable compatibility artifact") from exc

    tree = hashlib.sha256()
    for relative, path in sorted(files):
        try:
            file_digest = hashlib.sha256(path.read_bytes()).digest()
        except OSError as exc:
            raise StateValidationError("unreadable compatibility artifact") from exc
        tree.update(relative.encode("utf-8"))
        tree.update(b"\0")
        tree.update(file_digest)
    return tree.hexdigest()


def _validate_sqlite_sidecars(database: Path) -> None:
    """Reject redirected/special sidecars; SQLite owns their live format."""
    for suffix, label in (("-wal", "database WAL"), ("-shm", "database SHM")):
        sidecar = Path(str(database) + suffix)
        if sidecar.exists() or sidecar.is_symlink():
            _regular_file(sidecar, label)


def _open_database(path: Path) -> sqlite3.Connection:
    _regular_file(path, "state database")
    _validate_sqlite_sidecars(path)
    try:
        # mode=ro prevents schema/data writes while still letting SQLite recover
        # committed rows from an existing WAL. It may create ephemeral SHM
        # metadata, but never initializes a database or compatibility key.
        connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        return connection
    except (OSError, sqlite3.Error) as exc:
        raise StateValidationError("unreadable state database") from exc


def _check_database(connection: sqlite3.Connection) -> None:
    try:
        integrity = connection.execute("PRAGMA integrity_check").fetchall()
        if not integrity or any(row[0] != "ok" for row in integrity):
            raise StateValidationError("database integrity check failed")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise StateValidationError("database foreign key check failed")
    except sqlite3.Error as exc:
        raise StateValidationError("database integrity check failed") from exc


def _secret_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise StateValidationError("invalid session secret")


def _validate_rows(
    jobs: sqlite3.Connection,
    bridge: sqlite3.Connection,
    oauth: sqlite3.Connection,
    manifest: dict[str, Any],
) -> tuple[int, int, int, int]:
    try:
        job_ids = {row[0] for row in jobs.execute("SELECT id FROM jobs")}
        client_ids = {row[0] for row in oauth.execute("SELECT client_id FROM clients")}
        if not set(manifest["original_job_ids"]).issubset(job_ids):
            raise StateValidationError("job baseline mismatch")
        if not set(manifest["original_client_ids"]).issubset(client_ids):
            raise StateValidationError("client baseline mismatch")

        expected_secrets = manifest["session_secret_digests"]
        secret_rows = jobs.execute("SELECT name, value FROM session_secrets").fetchall()
        if len(secret_rows) != len(expected_secrets):
            raise StateValidationError("session secret baseline mismatch")
        salt = bytes.fromhex(manifest["salt"])
        seen: set[str] = set()
        for row in secret_rows:
            name = row["name"]
            if not isinstance(name, str) or name in seen or name not in expected_secrets:
                raise StateValidationError("session secret baseline mismatch")
            seen.add(name)
            actual = hashlib.sha256(salt + _secret_bytes(row["value"])).hexdigest()
            if not hmac.compare_digest(actual, expected_secrets[name]):
                raise StateValidationError("session secret baseline mismatch")

        published = jobs.execute(
            "SELECT task_id, message_id FROM session_events WHERE published_at IS NOT NULL"
        ).fetchall()
        for event in published:
            message = bridge.execute(
                "SELECT task_id FROM messages WHERE id=?", (event["message_id"],)
            ).fetchone()
            if message is None or message["task_id"] != event["task_id"]:
                raise StateValidationError("published event linkage mismatch")

        handles = jobs.execute(
            "SELECT task_id, event_id, message_id FROM session_delivery_handles"
        ).fetchall()
        for handle in handles:
            event = jobs.execute(
                "SELECT message_id FROM session_events WHERE task_id=? AND event_id=?",
                (handle["task_id"], handle["event_id"]),
            ).fetchone()
            if event is None or event["message_id"] != handle["message_id"]:
                raise StateValidationError("delivery handle linkage mismatch")
    except sqlite3.Error as exc:
        raise StateValidationError("state schema validation failed") from exc
    return len(job_ids), len(client_ids), len(secret_rows), len(published)


def validate_state(
    state_dir: str | os.PathLike[str],
    manifest_path: str | os.PathLike[str] | None = None,
    compat_root: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    """Validate migrated state without modifying it or exposing secret material."""
    state_root = Path(state_dir)
    try:
        state_mode = state_root.lstat().st_mode
    except OSError as exc:
        raise StateValidationError("missing state directory") from exc
    if not stat.S_ISDIR(state_mode) or state_root.is_symlink():
        raise StateValidationError("invalid state directory")
    try:
        database_names = {path.name for path in state_root.iterdir() if path.name.endswith(".sqlite3")}
    except OSError as exc:
        raise StateValidationError("unreadable state directory") from exc
    if database_names != set(_DATABASES):
        raise StateValidationError("invalid state databases")
    manifest_file = Path(manifest_path) if manifest_path is not None else state_root / ".migration-complete.json"
    manifest = _load_manifest(manifest_file)

    connections: list[sqlite3.Connection] = []
    try:
        for name in _DATABASES:
            connection = _open_database(state_root / name)
            connections.append(connection)
            _check_database(connection)
        counts = _validate_rows(connections[0], connections[1], connections[2], manifest)
    finally:
        for connection in connections:
            connection.close()

    artifact_digest = None
    if compat_root is not None:
        compat = Path(compat_root)
        revision_file = compat / "DISPATCH_SOURCE_REVISION"
        _regular_file(revision_file, "source revision")
        try:
            revision = revision_file.read_text(encoding="ascii").strip()
        except (OSError, UnicodeError) as exc:
            raise StateValidationError("invalid source revision") from exc
        if not hmac.compare_digest(revision, manifest["source_revision"]):
            raise StateValidationError("source revision mismatch")
        artifact_digest = artifact_tree_sha256(compat)
        if not hmac.compare_digest(artifact_digest, manifest["artifact_tree_sha256"]):
            raise StateValidationError("compatibility artifact mismatch")

    return {
        "schema_version": 1,
        "source_revision": manifest["source_revision"],
        "artifact_tree_sha256": artifact_digest,
        "databases": list(_DATABASES),
        "job_count": counts[0],
        "client_count": counts[1],
        "session_secret_count": counts[2],
        "published_event_count": counts[3],
    }
