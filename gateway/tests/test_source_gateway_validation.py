import hashlib
import json
from pathlib import Path
import shutil
import sqlite3

import pytest

from source_gateway.validation import StateValidationError, artifact_tree_sha256, validate_state


REVISION = "d6c41b8156756bcc59ddace624648a8fdf185e9a"
SALT = "11" * 32
SECRET = b"preserved-private-key"


def _database(path: Path, script: str, rows: list[tuple[str, tuple]]) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(script)
        for statement, values in rows:
            connection.execute(statement, values)
        connection.commit()
    finally:
        connection.close()


def build_compat(root: Path, runtime_source: str = "def main():\n    return None\n") -> Path:
    compat = root / "compat"
    (compat / "haru_dispatch").mkdir(parents=True)
    (compat / "bin").mkdir()
    (compat / "DISPATCH_SOURCE_REVISION").write_text(REVISION + "\n")
    (compat / "bin" / "haru-dispatch").write_text("#!/bin/sh\n")
    (compat / "haru_dispatch" / "__init__.py").write_text("")
    (compat / "haru_dispatch" / "runtime.py").write_text(runtime_source)
    return compat


def build_state(root: Path, compat: Path | None = None) -> Path:
    state = root / "state"
    state.mkdir()
    _database(
        state / "jobs.sqlite3",
        """
        CREATE TABLE jobs(id TEXT PRIMARY KEY);
        CREATE TABLE session_secrets(name TEXT PRIMARY KEY, value BLOB NOT NULL);
        CREATE TABLE session_events(
          task_id TEXT NOT NULL, event_id TEXT NOT NULL, message_id TEXT NOT NULL,
          published_at REAL, PRIMARY KEY(task_id, event_id));
        CREATE TABLE session_delivery_handles(
          handle_hash TEXT PRIMARY KEY, task_id TEXT NOT NULL, event_id TEXT NOT NULL,
          message_id TEXT NOT NULL);
        """,
        [
            ("INSERT INTO jobs VALUES(?)", ("job-original",)),
            ("INSERT INTO jobs VALUES(?)", ("job-new",)),
            ("INSERT INTO session_secrets VALUES(?,?)", ("delivery", SECRET)),
            (
                "INSERT INTO session_events VALUES(?,?,?,?)",
                ("job-original", "event-1", "message-1", 123.0),
            ),
            (
                "INSERT INTO session_delivery_handles VALUES(?,?,?,?)",
                ("digest-only", "job-original", "event-1", "message-1"),
            ),
        ],
    )
    _database(
        state / "bridge.sqlite3",
        "CREATE TABLE messages(id TEXT PRIMARY KEY, task_id TEXT);",
        [("INSERT INTO messages VALUES(?,?)", ("message-1", "job-original"))],
    )
    _database(
        state / "oauth.sqlite3",
        "CREATE TABLE clients(client_id TEXT PRIMARY KEY);",
        [
            ("INSERT INTO clients VALUES(?)", ("client-original",)),
            ("INSERT INTO clients VALUES(?)", ("client-new",)),
        ],
    )
    digest = hashlib.sha256(bytes.fromhex(SALT) + SECRET).hexdigest()
    manifest = {
        "schema_version": 1,
        "source_revision": REVISION,
        "artifact_tree_sha256": artifact_tree_sha256(compat) if compat else "22" * 32,
        "salt": SALT,
        "original_job_ids": ["job-original"],
        "original_client_ids": ["client-original"],
        "session_secret_digests": {"delivery": digest},
        "required_databases": ["jobs.sqlite3", "bridge.sqlite3", "oauth.sqlite3"],
    }
    (state / ".migration-complete.json").write_text(json.dumps(manifest))
    return state


def test_valid_state_preserves_baselines_and_cross_store_links(tmp_path):
    compat = build_compat(tmp_path)
    state = build_state(tmp_path, compat)

    report = validate_state(state, compat_root=compat)

    assert report == {
        "schema_version": 1,
        "source_revision": REVISION,
        "artifact_tree_sha256": artifact_tree_sha256(compat),
        "databases": ["jobs.sqlite3", "bridge.sqlite3", "oauth.sqlite3"],
        "job_count": 2,
        "client_count": 2,
        "session_secret_count": 1,
        "published_event_count": 1,
    }
    assert SECRET.decode() not in json.dumps(report)


def test_valid_state_reads_committed_wal_rows_before_and_after_checkpoint(tmp_path):
    compat = build_compat(tmp_path)
    state = build_state(tmp_path, compat)
    jobs = sqlite3.connect(state / "jobs.sqlite3")
    bridge = sqlite3.connect(state / "bridge.sqlite3")
    oauth = sqlite3.connect(state / "oauth.sqlite3")
    connections = (jobs, bridge, oauth)
    try:
        for connection in connections:
            assert connection.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
            connection.execute("PRAGMA wal_autocheckpoint=0")
        jobs.execute("INSERT INTO jobs VALUES(?)", ("job-from-wal",))
        jobs.execute(
            "INSERT INTO session_events VALUES(?,?,?,?)",
            ("job-from-wal", "event-from-wal", "message-from-wal", 456.0),
        )
        bridge.execute(
            "INSERT INTO messages VALUES(?,?)", ("message-from-wal", "job-from-wal")
        )
        oauth.execute("INSERT INTO clients VALUES(?)", ("client-from-wal",))
        for connection in connections:
            connection.commit()

        report = validate_state(state, compat_root=compat)
        assert report["job_count"] == 3
        assert report["client_count"] == 3
        assert report["published_event_count"] == 2
    finally:
        for connection in connections:
            connection.close()

    # Closing the writers checkpoints the WAL; restart validation must see the
    # same committed state from the main database files.
    report = validate_state(state, compat_root=compat)
    assert report["job_count"] == 3
    assert report["client_count"] == 3
    assert report["published_event_count"] == 2


def test_corrupt_wal_is_rejected_instead_of_silently_reading_stale_main_db(tmp_path):
    compat = build_compat(tmp_path)
    state = build_state(tmp_path, compat)
    writer = sqlite3.connect(state / "jobs.sqlite3")
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("INSERT INTO jobs VALUES(?)", ("committed-only-in-wal",))
        writer.commit()
        recovered = tmp_path / "recovered"
        shutil.copytree(state, recovered)
    finally:
        writer.close()

    wal = recovered / "jobs.sqlite3-wal"
    data = bytearray(wal.read_bytes())
    data[:32] = b"X" * 32
    wal.write_bytes(data)

    with pytest.raises(StateValidationError, match="database WAL validation failed"):
        validate_state(recovered, compat_root=compat)


@pytest.mark.parametrize("missing", ["jobs.sqlite3", "bridge.sqlite3", "oauth.sqlite3"])
def test_missing_database_fails_without_initializing_it(tmp_path, missing):
    state = build_state(tmp_path)
    path = state / missing
    path.unlink()
    before = sorted(item.name for item in state.iterdir())

    with pytest.raises(StateValidationError):
        validate_state(state)

    assert not path.exists()
    assert sorted(item.name for item in state.iterdir()) == before


def test_corrupt_database_fails_without_creating_sidecars(tmp_path):
    state = build_state(tmp_path)
    (state / "bridge.sqlite3").write_bytes(b"not sqlite")

    with pytest.raises(StateValidationError):
        validate_state(state)

    assert sorted(item.name for item in state.iterdir()) == [
        ".migration-complete.json", "bridge.sqlite3", "jobs.sqlite3", "oauth.sqlite3"
    ]


def test_altered_session_key_fails_closed_and_does_not_leak_value(tmp_path):
    state = build_state(tmp_path)
    connection = sqlite3.connect(state / "jobs.sqlite3")
    connection.execute("UPDATE session_secrets SET value=?", (b"altered-token-value",))
    connection.commit()
    connection.close()

    with pytest.raises(StateValidationError) as caught:
        validate_state(state)

    assert "altered-token-value" not in str(caught.value)


@pytest.mark.parametrize(
    ("statement", "expected"),
    [
        ("DELETE FROM jobs WHERE id='job-original'", "job baseline mismatch"),
        (
            "UPDATE session_events SET message_id='missing' WHERE event_id='event-1'",
            "published event linkage mismatch",
        ),
        (
            "UPDATE session_delivery_handles SET event_id='missing'",
            "delivery handle linkage mismatch",
        ),
    ],
)
def test_history_and_cross_store_mismatches_fail(tmp_path, statement, expected):
    state = build_state(tmp_path)
    connection = sqlite3.connect(state / "jobs.sqlite3")
    connection.execute(statement)
    connection.commit()
    connection.close()

    with pytest.raises(StateValidationError, match=expected):
        validate_state(state)


def test_client_baseline_mismatch_fails(tmp_path):
    state = build_state(tmp_path)
    connection = sqlite3.connect(state / "oauth.sqlite3")
    connection.execute("DELETE FROM clients WHERE client_id='client-original'")
    connection.commit()
    connection.close()

    with pytest.raises(StateValidationError, match="client baseline mismatch"):
        validate_state(state)


def test_bad_artifact_and_revision_fail(tmp_path):
    compat = build_compat(tmp_path)
    state = build_state(tmp_path, compat)
    (compat / "haru_dispatch" / "runtime.py").write_text("changed = True\n")
    with pytest.raises(StateValidationError, match="compatibility artifact mismatch"):
        validate_state(state, compat_root=compat)

    (compat / "DISPATCH_SOURCE_REVISION").write_text("wrong\n")
    with pytest.raises(StateValidationError, match="source revision mismatch"):
        validate_state(state, compat_root=compat)


def test_symlinked_database_is_rejected(tmp_path):
    state = build_state(tmp_path)
    original = state / "jobs.real"
    (state / "jobs.sqlite3").rename(original)
    (state / "jobs.sqlite3").symlink_to(original)

    with pytest.raises(StateValidationError, match="invalid state database"):
        validate_state(state)


def test_unmanifested_database_is_rejected(tmp_path):
    state = build_state(tmp_path)
    sqlite3.connect(state / "unexpected.sqlite3").close()

    with pytest.raises(StateValidationError, match="invalid state databases"):
        validate_state(state)
