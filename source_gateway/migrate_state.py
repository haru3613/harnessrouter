"""Offline, fail-closed cloning of the three Dispatch broker databases."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import sqlite3
import stat
import tempfile
from typing import Any

from .validation import StateValidationError, _validate_wal, artifact_tree_sha256, validate_state


_DATABASES = ("jobs.sqlite3", "bridge.sqlite3", "oauth.sqlite3")
_EXCLUDED_FILES = {".migration-complete.json", ".migration-report.json"}
_HEX = frozenset("0123456789abcdef")
_PINNED_SOURCE_REVISION = "d6c41b8156756bcc59ddace624648a8fdf185e9a"
_PINNED_ARTIFACT_TREE_SHA256 = "aba1003003e6e5851e2620751fbadd74943b5adebbecd3e540d3ad772eb50d13"


class MigrationError(RuntimeError):
    """Broker state could not be cloned without weakening its invariants."""


def _mode(path: Path, label: str) -> int:
    try:
        return path.lstat().st_mode
    except OSError as exc:
        raise MigrationError(f"missing {label}") from exc


def _require_directory(path: Path, label: str) -> os.stat_result:
    mode = _mode(path, label)
    if not stat.S_ISDIR(mode) or path.is_symlink():
        raise MigrationError(f"invalid {label}")
    try:
        return path.stat(follow_symlinks=False)
    except OSError as exc:
        raise MigrationError(f"invalid {label}") from exc


def _require_regular(path: Path, label: str) -> os.stat_result:
    mode = _mode(path, label)
    if not stat.S_ISREG(mode) or path.is_symlink():
        raise MigrationError(f"invalid {label}")
    try:
        return path.stat(follow_symlinks=False)
    except OSError as exc:
        raise MigrationError(f"invalid {label}") from exc


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _value_bytes(value: Any) -> bytes:
    if value is None:
        payload = b""
        marker = b"n"
    elif isinstance(value, bool):
        payload = b"1" if value else b"0"
        marker = b"b"
    elif isinstance(value, int):
        payload = str(value).encode("ascii")
        marker = b"i"
    elif isinstance(value, float):
        payload = value.hex().encode("ascii")
        marker = b"f"
    elif isinstance(value, str):
        payload = value.encode("utf-8")
        marker = b"s"
    elif isinstance(value, bytes):
        payload = value
        marker = b"x"
    else:
        raise MigrationError("unsupported database value")
    return marker + len(payload).to_bytes(8, "big") + payload


def _row_digest(row: tuple[Any, ...]) -> bytes:
    digest = hashlib.sha256()
    for value in row:
        digest.update(_value_bytes(value))
    return digest.digest()


def _digest_set(values: list[bytes]) -> str:
    digest = hashlib.sha256()
    for value in sorted(values):
        digest.update(value)
    return digest.hexdigest()


def _connect_readonly(path: Path) -> sqlite3.Connection:
    _require_regular(path, "source database")
    try:
        _validate_wal(path)
        connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        return connection
    except (sqlite3.Error, StateValidationError) as exc:
        raise MigrationError("unreadable source database") from exc


def _check_database(connection: sqlite3.Connection) -> None:
    try:
        integrity = connection.execute("PRAGMA integrity_check").fetchall()
        if not integrity or any(row[0] != "ok" for row in integrity):
            raise MigrationError("database integrity check failed")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise MigrationError("database foreign key check failed")
    except sqlite3.Error as exc:
        raise MigrationError("database integrity check failed") from exc


def _database_inventory(path: Path) -> dict[str, dict[str, Any]]:
    connection = _connect_readonly(path)
    try:
        _check_database(connection)
        tables = connection.execute(
            "SELECT name, sql FROM sqlite_schema WHERE type='table' ORDER BY name"
        ).fetchall()
        inventory: dict[str, dict[str, Any]] = {}
        for table in tables:
            name = table["name"]
            if not isinstance(name, str):
                raise MigrationError("invalid database schema")
            quoted = _quote_identifier(name)
            columns = connection.execute(f"PRAGMA table_info({quoted})").fetchall()
            if not columns:
                raise MigrationError("invalid database schema")
            pk_indices = [
                index for _, index in sorted(
                    (int(column["pk"]), position)
                    for position, column in enumerate(columns)
                    if int(column["pk"]) > 0
                )
            ]
            row_hashes: list[bytes] = []
            pk_hashes: list[bytes] = []
            count = 0
            for raw_row in connection.execute(f"SELECT * FROM {quoted}"):
                row = tuple(raw_row)
                row_hashes.append(_row_digest(row))
                if pk_indices:
                    pk_hashes.append(_row_digest(tuple(row[index] for index in pk_indices)))
                count += 1
            schema = table["sql"] if isinstance(table["sql"], str) else ""
            inventory[name] = {
                "rows": count,
                "schema_sha256": hashlib.sha256(schema.encode("utf-8")).hexdigest(),
                "primary_key_columns": len(pk_indices),
                "primary_key_set_sha256": _digest_set(pk_hashes),
                "row_set_sha256": _digest_set(row_hashes),
            }
        return inventory
    except sqlite3.Error as exc:
        raise MigrationError("database inventory failed") from exc
    finally:
        connection.close()


def _backup_database(source: Path, destination: Path) -> None:
    source_connection = _connect_readonly(source)
    destination_connection: sqlite3.Connection | None = None
    try:
        destination_connection = sqlite3.connect(destination)
        source_connection.backup(destination_connection)
        destination_connection.commit()
        checkpoint = destination_connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if checkpoint is not None and checkpoint[0] != 0:
            raise MigrationError("destination checkpoint failed")
    except sqlite3.Error as exc:
        raise MigrationError("database backup failed") from exc
    finally:
        if destination_connection is not None:
            destination_connection.close()
        source_connection.close()
    try:
        os.chmod(destination, stat.S_IMODE(source.stat(follow_symlinks=False).st_mode))
        for suffix in ("-wal", "-shm", "-journal"):
            sidecar = Path(str(destination) + suffix)
            if sidecar.exists():
                sidecar.unlink()
    except OSError as exc:
        raise MigrationError("destination checkpoint failed") from exc


def _excluded(relative: Path) -> bool:
    if relative.as_posix() == "runtime/delivery-health.json":
        return True
    name = relative.name
    return (
        name in _EXCLUDED_FILES
        or name.endswith(".lock")
        or name.endswith(".sqlite3-wal")
        or name.endswith(".sqlite3-shm")
        or name.endswith(".sqlite3-journal")
        or (len(relative.parts) == 1 and name in _DATABASES)
    )


def _scan_extra(root: Path) -> tuple[list[tuple[Path, int]], list[tuple[Path, int, str]]]:
    directories: list[tuple[Path, int]] = []
    files: list[tuple[Path, int, str]] = []

    def visit(directory: Path) -> None:
        try:
            entries = sorted(directory.iterdir(), key=lambda item: item.name)
        except OSError as exc:
            raise MigrationError("unreadable source state") from exc
        for path in entries:
            relative = path.relative_to(root)
            mode = _mode(path, "source state entry")
            if stat.S_ISLNK(mode):
                raise MigrationError("invalid source state entry")
            if stat.S_ISDIR(mode):
                directories.append((relative, stat.S_IMODE(mode)))
                visit(path)
            elif stat.S_ISREG(mode):
                if _excluded(relative):
                    continue
                try:
                    digest = hashlib.sha256(path.read_bytes()).hexdigest()
                except OSError as exc:
                    raise MigrationError("unreadable source state") from exc
                files.append((relative, stat.S_IMODE(mode), digest))
            else:
                raise MigrationError("invalid source state entry")
    visit(root)
    return directories, files


def _copy_extra_tree(source: Path, destination: Path) -> tuple[int, str]:
    before_directories, before_files = _scan_extra(source)
    for relative, mode in before_directories:
        target = destination / relative
        target.mkdir(parents=True, exist_ok=True)
        os.chmod(target, mode)
    for relative, mode, _ in before_files:
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / relative, target, follow_symlinks=False)
        os.chmod(target, mode)

    after_directories, after_files = _scan_extra(source)
    if before_directories != after_directories or before_files != after_files:
        raise MigrationError("source state changed during migration")
    for relative, _, digest in before_files:
        try:
            copied_digest = hashlib.sha256((destination / relative).read_bytes()).hexdigest()
        except OSError as exc:
            raise MigrationError("copied state verification failed") from exc
        if copied_digest != digest:
            raise MigrationError("copied state verification failed")

    tree = hashlib.sha256()
    for relative, mode, digest in before_files:
        tree.update(relative.as_posix().encode("utf-8"))
        tree.update(b"\0")
        tree.update(mode.to_bytes(4, "big"))
        tree.update(bytes.fromhex(digest))
    return len(before_files), tree.hexdigest()


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(path.name + ".tmp")
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
    try:
        with temporary.open("x", encoding="utf-8") as target:
            os.chmod(temporary, 0o600)
            target.write(payload)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
    except OSError as exc:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise MigrationError("migration metadata write failed") from exc


def _column_values(path: Path, table: str, column: str) -> list[str]:
    connection = _connect_readonly(path)
    try:
        rows = connection.execute(
            f"SELECT {_quote_identifier(column)} FROM {_quote_identifier(table)}"
        ).fetchall()
    except sqlite3.Error as exc:
        raise MigrationError("required baseline read failed") from exc
    finally:
        connection.close()
    values = [row[0] for row in rows]
    if any(not isinstance(value, str) or not value for value in values) or len(set(values)) != len(values):
        raise MigrationError("invalid baseline identifiers")
    return sorted(values)


def _secret_digests(path: Path, salt: bytes) -> dict[str, str]:
    connection = _connect_readonly(path)
    try:
        rows = connection.execute("SELECT name, value FROM session_secrets").fetchall()
    except sqlite3.Error as exc:
        raise MigrationError("session secret read failed") from exc
    finally:
        connection.close()
    result: dict[str, str] = {}
    for row in rows:
        name, value = row[0], row[1]
        if not isinstance(name, str) or not name or name in result:
            raise MigrationError("invalid session secrets")
        if isinstance(value, str):
            raw = value.encode("utf-8")
        elif isinstance(value, bytes):
            raw = value
        else:
            raise MigrationError("invalid session secrets")
        result[name] = hashlib.sha256(salt + raw).hexdigest()
    return result


def _prepare_destination(destination: Path) -> tuple[bool, tuple[int, int] | None]:
    try:
        mode = destination.lstat().st_mode
    except FileNotFoundError:
        return False, None
    except OSError as exc:
        raise MigrationError("invalid destination") from exc
    if not stat.S_ISDIR(mode) or destination.is_symlink():
        raise MigrationError("invalid destination")
    try:
        if any(destination.iterdir()):
            raise MigrationError("destination is not empty")
        status = destination.stat(follow_symlinks=False)
    except OSError as exc:
        raise MigrationError("invalid destination") from exc
    return True, (status.st_dev, status.st_ino)


def _destination_unchanged(destination: Path, existed: bool, identity: tuple[int, int] | None) -> None:
    if not existed:
        if destination.exists() or destination.is_symlink():
            raise MigrationError("destination changed during migration")
        return
    status = _require_directory(destination, "destination")
    if (status.st_dev, status.st_ino) != identity:
        raise MigrationError("destination changed during migration")
    try:
        if any(destination.iterdir()):
            raise MigrationError("destination changed during migration")
    except OSError as exc:
        raise MigrationError("destination changed during migration") from exc


def clone_broker_state(
    source: str | os.PathLike[str],
    destination: str | os.PathLike[str],
    compat_root: str | os.PathLike[str],
    source_revision: str,
    quiesced: bool,
) -> dict[str, Any]:
    """Clone quiesced broker state and atomically publish a validated result."""
    if quiesced is not True:
        raise MigrationError("source must be quiesced")
    if (
        not isinstance(source_revision, str)
        or len(source_revision) != 40
        or any(character not in _HEX for character in source_revision)
    ):
        raise MigrationError("invalid source revision")
    if source_revision != _PINNED_SOURCE_REVISION:
        raise MigrationError("source revision mismatch")

    source_path = Path(source)
    destination_path = Path(destination)
    source_status = _require_directory(source_path, "source state")
    if destination_path.absolute().is_relative_to(source_path.absolute()):
        raise MigrationError("invalid destination")
    existed, destination_identity = _prepare_destination(destination_path)
    try:
        database_names = {path.name for path in source_path.iterdir() if path.name.endswith(".sqlite3")}
    except OSError as exc:
        raise MigrationError("unreadable source state") from exc
    if database_names != set(_DATABASES):
        raise MigrationError("invalid source databases")
    for name in _DATABASES:
        _require_regular(source_path / name, "source database")

    compat_path = Path(compat_root)
    revision_path = compat_path / "DISPATCH_SOURCE_REVISION"
    _require_regular(revision_path, "source revision")
    try:
        artifact_revision = revision_path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError) as exc:
        raise MigrationError("invalid source revision") from exc
    if artifact_revision != source_revision:
        raise MigrationError("source revision mismatch")
    try:
        artifact_digest = artifact_tree_sha256(compat_path)
    except StateValidationError as exc:
        raise MigrationError("invalid compatibility artifact") from exc
    if artifact_digest != _PINNED_ARTIFACT_TREE_SHA256:
        raise MigrationError("compatibility artifact mismatch")

    parent = destination_path.parent
    _require_directory(parent, "destination parent")
    staging = Path(tempfile.mkdtemp(prefix=f".{destination_path.name}.staging-", dir=parent))
    candidate_manifest = staging.with_name(staging.name + ".manifest-candidate")
    promoted = False
    try:
        os.chmod(staging, stat.S_IMODE(source_status.st_mode))
        before = {name: _database_inventory(source_path / name) for name in _DATABASES}
        for name in _DATABASES:
            _backup_database(source_path / name, staging / name)
        extra_file_count, extra_tree_digest = _copy_extra_tree(source_path, staging)
        after = {name: _database_inventory(source_path / name) for name in _DATABASES}
        copied = {name: _database_inventory(staging / name) for name in _DATABASES}
        if before != after or before != copied:
            raise MigrationError("source state changed during migration")

        salt = secrets.token_bytes(32)
        manifest = {
            "schema_version": 1,
            "source_revision": source_revision,
            "artifact_tree_sha256": artifact_digest,
            "salt": salt.hex(),
            "original_job_ids": _column_values(staging / "jobs.sqlite3", "jobs", "id"),
            "original_client_ids": _column_values(staging / "oauth.sqlite3", "clients", "client_id"),
            "session_secret_digests": _secret_digests(staging / "jobs.sqlite3", salt),
            "required_databases": list(_DATABASES),
        }
        report = {
            "schema_version": 1,
            "source_revision": source_revision,
            "artifact_tree_sha256": artifact_digest,
            "databases": copied,
            "extra_file_count": extra_file_count,
            "extra_tree_sha256": extra_tree_digest,
        }
        _atomic_json(candidate_manifest, manifest)
        _atomic_json(staging / ".migration-report.json", report)
        try:
            validate_state(staging, manifest_path=candidate_manifest, compat_root=compat_path)
        except StateValidationError as exc:
            raise MigrationError("copied state validation failed") from exc
        if artifact_tree_sha256(compat_path) != artifact_digest:
            raise MigrationError("compatibility artifact changed during migration")

        # This is deliberately the final file published into the staged state.
        _atomic_json(staging / ".migration-complete.json", manifest)
        candidate_manifest.unlink()
        directory_fd = os.open(staging, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

        _destination_unchanged(destination_path, existed, destination_identity)
        os.replace(staging, destination_path)
        promoted = True
        parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
        return report
    except MigrationError:
        raise
    except (OSError, sqlite3.Error, StateValidationError) as exc:
        raise MigrationError("broker state migration failed") from exc
    finally:
        try:
            candidate_manifest.unlink(missing_ok=True)
        except OSError:
            pass
        if not promoted:
            shutil.rmtree(staging, ignore_errors=True)
