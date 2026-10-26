import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat

import pytest

import source_gateway.migrate_state as migration
from source_gateway.migrate_state import MigrationError, clone_broker_state
from source_gateway.validation import artifact_tree_sha256, validate_state
from test_source_gateway_validation import REVISION, SECRET, build_compat, build_state


def _clone(source: Path, destination: Path, compat: Path, *, quiesced=True):
    migration._PINNED_ARTIFACT_TREE_SHA256 = artifact_tree_sha256(compat)
    return clone_broker_state(source, destination, compat, REVISION, quiesced)


def test_clone_copies_committed_wal_state_keys_handles_and_extra_files(tmp_path):
    compat = build_compat(tmp_path)
    source = build_state(tmp_path, compat)
    config = source / "config.json"
    config.write_text('{"safe":"configuration"}\n')
    config.chmod(0o640)
    (source / "bridge.delivery.lock").write_text("stale lock")
    runtime_dir = source / "runtime"
    runtime_dir.mkdir()
    (runtime_dir / "delivery-health.json").write_text('{"at":1}')
    (source / ".migration-report.json").write_text('{"old":true}')

    jobs = sqlite3.connect(source / "jobs.sqlite3")
    bridge = sqlite3.connect(source / "bridge.sqlite3")
    oauth = sqlite3.connect(source / "oauth.sqlite3")
    connections = (jobs, bridge, oauth)
    try:
        for connection in connections:
            assert connection.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
            connection.execute("PRAGMA wal_autocheckpoint=0")
        jobs.execute("INSERT INTO jobs VALUES(?)", ("job-from-wal",))
        jobs.execute(
            "INSERT INTO session_events VALUES(?,?,?,?)",
            ("job-from-wal", "event-from-wal", "message-from-wal", 999.0),
        )
        jobs.execute(
            "INSERT INTO session_delivery_handles VALUES(?,?,?,?)",
            ("wal-handle-hash", "job-from-wal", "event-from-wal", "message-from-wal"),
        )
        bridge.execute(
            "INSERT INTO messages VALUES(?,?)", ("message-from-wal", "job-from-wal")
        )
        oauth.execute("INSERT INTO clients VALUES(?)", ("client-from-wal",))
        for connection in connections:
            connection.commit()

        destination = tmp_path / "migrated"
        report = _clone(source, destination, compat)
    finally:
        for connection in connections:
            connection.close()

    validation = validate_state(destination, compat_root=compat)
    assert validation["job_count"] == 3
    assert validation["client_count"] == 3
    assert validation["published_event_count"] == 2
    assert report["databases"]["jobs.sqlite3"]["jobs"]["rows"] == 3

    connection = sqlite3.connect(destination / "jobs.sqlite3")
    try:
        assert connection.execute(
            "SELECT task_id,event_id,message_id FROM session_delivery_handles "
            "WHERE handle_hash='wal-handle-hash'"
        ).fetchone() == ("job-from-wal", "event-from-wal", "message-from-wal")
        assert connection.execute(
            "SELECT value FROM session_secrets WHERE name='delivery'"
        ).fetchone()[0] == SECRET
    finally:
        connection.close()

    assert config.read_bytes() == (destination / "config.json").read_bytes()
    assert stat.S_IMODE((destination / "config.json").stat().st_mode) == 0o640
    assert not (destination / "bridge.delivery.lock").exists()
    assert not (destination / "runtime" / "delivery-health.json").exists()
    assert (destination / ".migration-report.json").exists()
    manifest = json.loads((destination / ".migration-complete.json").read_text())
    assert set(manifest) == {
        "schema_version", "source_revision", "artifact_tree_sha256", "salt",
        "original_job_ids", "original_client_ids", "session_secret_digests",
        "required_databases",
    }
    expected = hashlib.sha256(bytes.fromhex(manifest["salt"]) + SECRET).hexdigest()
    assert manifest["session_secret_digests"] == {"delivery": expected}
    metadata = (destination / ".migration-complete.json").read_text() + (
        destination / ".migration-report.json"
    ).read_text()
    assert SECRET.decode() not in metadata


def test_clone_can_atomically_replace_an_existing_empty_destination(tmp_path):
    compat = build_compat(tmp_path)
    source = build_state(tmp_path, compat)
    destination = tmp_path / "migrated"
    destination.mkdir()

    _clone(source, destination, compat)

    assert (destination / ".migration-complete.json").is_file()
    validate_state(destination, compat_root=compat)


@pytest.mark.parametrize("failure", ["missing", "corrupt", "crosslink"])
def test_invalid_source_fails_without_publishing_destination(tmp_path, failure):
    compat = build_compat(tmp_path)
    source = build_state(tmp_path, compat)
    if failure == "missing":
        (source / "bridge.sqlite3").unlink()
    elif failure == "corrupt":
        (source / "oauth.sqlite3").write_bytes(b"not a database")
    else:
        connection = sqlite3.connect(source / "jobs.sqlite3")
        connection.execute(
            "UPDATE session_events SET message_id='missing-message' WHERE event_id='event-1'"
        )
        connection.commit()
        connection.close()
    destination = tmp_path / "migrated"

    with pytest.raises(MigrationError):
        _clone(source, destination, compat)

    assert not destination.exists()
    assert not list(tmp_path.glob(".migrated.staging-*"))
    assert not list(tmp_path.glob(".migrated.staging-*.manifest-candidate"))


def test_non_quiesced_source_is_rejected_before_destination_creation(tmp_path):
    compat = build_compat(tmp_path)
    source = build_state(tmp_path, compat)
    destination = tmp_path / "migrated"

    with pytest.raises(MigrationError, match="quiesced"):
        _clone(source, destination, compat, quiesced=False)

    assert not destination.exists()


def test_nonempty_and_symlink_destinations_are_refused(tmp_path):
    compat = build_compat(tmp_path)
    source = build_state(tmp_path, compat)
    nonempty = tmp_path / "nonempty"
    nonempty.mkdir()
    sentinel = nonempty / "keep"
    sentinel.write_text("untouched")
    with pytest.raises(MigrationError, match="not empty"):
        _clone(source, nonempty, compat)
    assert sentinel.read_text() == "untouched"

    real = tmp_path / "real"
    real.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(real, target_is_directory=True)
    with pytest.raises(MigrationError, match="invalid destination"):
        _clone(source, linked, compat)
    assert not any(real.iterdir())

    source_alias = tmp_path / "source-alias"
    source_alias.symlink_to(source, target_is_directory=True)
    with pytest.raises(MigrationError, match="invalid destination"):
        _clone(source, source_alias / "nested-destination", compat)


def test_source_database_symlink_and_extra_database_are_refused(tmp_path):
    compat = build_compat(tmp_path)
    source = build_state(tmp_path, compat)
    real_jobs = source / "jobs.real"
    (source / "jobs.sqlite3").rename(real_jobs)
    (source / "jobs.sqlite3").symlink_to(real_jobs)
    with pytest.raises(MigrationError, match="invalid source database"):
        _clone(source, tmp_path / "linked-db-destination", compat)

    (source / "jobs.sqlite3").unlink()
    real_jobs.rename(source / "jobs.sqlite3")
    sqlite3.connect(source / "extra.sqlite3").close()
    with pytest.raises(MigrationError, match="invalid source databases"):
        _clone(source, tmp_path / "extra-db-destination", compat)


def test_concurrent_database_mutation_is_detected_and_not_promoted(tmp_path, monkeypatch):
    compat = build_compat(tmp_path)
    source = build_state(tmp_path, compat)
    destination = tmp_path / "migrated"
    original_backup = migration._backup_database
    mutated = False

    def backup_then_mutate(source_database, destination_database):
        nonlocal mutated
        original_backup(source_database, destination_database)
        if not mutated:
            mutated = True
            connection = sqlite3.connect(source / "jobs.sqlite3")
            connection.execute("INSERT INTO jobs VALUES(?)", ("concurrent-job",))
            connection.commit()
            connection.close()

    monkeypatch.setattr(migration, "_backup_database", backup_then_mutate)

    with pytest.raises(MigrationError, match="changed during migration"):
        _clone(source, destination, compat)

    assert not destination.exists()


def test_wrong_revision_and_artifact_drift_fail_closed(tmp_path):
    compat = build_compat(tmp_path)
    source = build_state(tmp_path, compat)
    migration._PINNED_ARTIFACT_TREE_SHA256 = artifact_tree_sha256(compat)
    with pytest.raises(MigrationError, match="source revision mismatch"):
        clone_broker_state(source, tmp_path / "wrong-revision", compat, "a" * 40, True)

    (compat / "haru_dispatch" / "runtime.py").write_text("changed = True\n")
    destination = tmp_path / "changed-artifact"
    with pytest.raises(MigrationError, match="compatibility artifact mismatch"):
        clone_broker_state(source, destination, compat, REVISION, True)
    assert not destination.exists()


def test_corrupt_source_wal_is_not_ignored_by_backup(tmp_path):
    compat = build_compat(tmp_path)
    original = build_state(tmp_path, compat)
    writer = sqlite3.connect(original / "jobs.sqlite3")
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("INSERT INTO jobs VALUES(?)", ("committed-only-in-wal",))
        writer.commit()
        source = tmp_path / "corrupt-source"
        shutil.copytree(original, source)
    finally:
        writer.close()

    wal = source / "jobs.sqlite3-wal"
    data = bytearray(wal.read_bytes())
    data[24:32] = b"X" * 8
    wal.write_bytes(data)
    destination = tmp_path / "migrated"

    with pytest.raises(MigrationError, match="unreadable source database"):
        _clone(source, destination, compat)

    assert not destination.exists()
