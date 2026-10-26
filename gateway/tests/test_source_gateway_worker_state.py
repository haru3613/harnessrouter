import hashlib
import json
from pathlib import Path
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from source_gateway.worker_state import WorkerMigrationError, inspect_worker_state, migrate_worker_state


def fixture(tmp_path):
    root = tmp_path / "old"
    task = root / "session-worker-jobs/task-1"
    task.mkdir(parents=True)
    receipt = {"task_id": "task-1", "attempt": 2, "nonce": "opaque-execution-nonce",
               "provider": "codex", "cwd": "/unchanged/project", "result": "private result"}
    (task / "report.json").write_text(json.dumps(receipt))
    (task / "process.json").write_text(json.dumps({**receipt, "wrapper_pid": 123,
                                                   "provider_pid": 124, "finished_at": 10}))
    for name in ("prompt.txt", "schema.json", "provider.json"):
        (task / name).write_text("immutable artifact\n")
    mapping = {"id": "task-1", "attempt": 2, "execution_nonce": receipt["nonce"],
               "launch_state": "finished", "broker_finished": True, "pending_events": [],
               "provider_command_submitted": True, "provider_command_submission_intent": True,
               "cwd": receipt["cwd"], "provider": "codex", "provider_session_id": "native-1",
               "lease_id": "lease-1", "origin_binding": {"origin": "haru-main"},
               "pane_id": "pane-1", "owned_by_dispatch": True,
               "continuation_predecessor_id": "previous-1", "context_generation": 4,
               "accepted_receipt_sha256": hashlib.sha256((task / "report.json").read_bytes()).hexdigest(),
               "prompt_path": str(task / "prompt.txt"), "schema_path": str(task / "schema.json"),
               "report_path": str(task / "report.json"), "provider_report_path": str(task / "provider.json"),
               "process_path": str(task / "process.json")}
    (root / "session-worker-jobs/task-1.json").write_text(json.dumps(mapping))
    (root / "locks").mkdir()
    (root / "locks/old.lock").write_text("old lock")
    (root / "tasks.sqlite3").write_bytes(b"legacy store archive only")
    (root / "mcp-worker-jobs").mkdir()
    (root / "mcp-worker-jobs/old.json").write_text('{}')
    return root, mapping


def test_terminal_migration_preserves_receipts_and_native_ownership(tmp_path):
    root, original = fixture(tmp_path)
    target = tmp_path / "new"
    before = {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob('*') if p.is_file()}
    report = migrate_worker_state(root, target, quiesced=True, pid_alive=lambda _: False)
    updated = json.loads((target / "session-worker-jobs/task-1.json").read_text())
    for field in report['locator_fields']:
        assert updated[field] == str(target / Path(original[field]).relative_to(root))
        assert Path(updated[field]).read_bytes() == Path(original[field]).read_bytes()
        updated[field] = original[field]
    assert updated == original
    assert report['artifact_hashes_preserved'] and report['source_unchanged']
    assert not (target / "tasks.sqlite3").exists()
    assert not (target / "mcp-worker-jobs").exists()
    assert not (target / "locks").exists()
    assert before == {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob('*') if p.is_file()}


@pytest.mark.parametrize('change', [
    {'launch_state': 'waiting_input'}, {'broker_finished': False},
    {'pending_events': [{'kind': 'result'}]}, {'accepted_receipt_sha256': 'wrong'},
    {'prompt_path': '/outside/secret'}, {'execution_nonce': 'wrong'},
])
def test_nonquiescent_or_untrusted_state_never_creates_destination(tmp_path, change):
    root, mapping = fixture(tmp_path)
    mapping.update(change)
    (root / "session-worker-jobs/task-1.json").write_text(json.dumps(mapping))
    target = tmp_path / "new"
    with pytest.raises(WorkerMigrationError):
        migrate_worker_state(root, target, quiesced=True, pid_alive=lambda _: False)
    assert not target.exists()


def test_live_process_and_unfenced_claim_loop_are_refused(tmp_path):
    root, _ = fixture(tmp_path)
    with pytest.raises(WorkerMigrationError, match='still present'):
        inspect_worker_state(root, pid_alive=lambda _: True)
    with pytest.raises(WorkerMigrationError, match='fencing'):
        migrate_worker_state(root, tmp_path / 'new')


def test_symlink_missing_artifact_and_existing_target_are_refused(tmp_path):
    root, mapping = fixture(tmp_path)
    prompt = Path(mapping['prompt_path'])
    prompt.unlink()
    prompt.symlink_to(root / 'tasks.sqlite3')
    with pytest.raises(WorkerMigrationError):
        migrate_worker_state(root, tmp_path / 'new', quiesced=True, pid_alive=lambda _: False)
    prompt.unlink()
    with pytest.raises(WorkerMigrationError):
        migrate_worker_state(root, tmp_path / 'new', quiesced=True, pid_alive=lambda _: False)
    (tmp_path / 'existing').mkdir()
    with pytest.raises(WorkerMigrationError, match='must not exist'):
        migrate_worker_state(root, tmp_path / 'existing', quiesced=True, pid_alive=lambda _: False)


def test_destination_parent_alias_cannot_write_into_source(tmp_path):
    root, _ = fixture(tmp_path)
    alias = tmp_path / 'alias'
    alias.symlink_to(root, target_is_directory=True)
    with pytest.raises(WorkerMigrationError, match='overlaps'):
        migrate_worker_state(root, alias / 'new', quiesced=True, pid_alive=lambda _: False)
    assert not (root / 'new').exists()


def test_worker_promotion_flushes_bytes_then_rename_and_parent(tmp_path):
    root, _ = fixture(tmp_path)
    target = tmp_path / 'new'
    from source_gateway import worker_state
    events = []
    rename = worker_state.os.replace
    with patch.object(worker_state, 'durable_tree', side_effect=lambda p: events.append('tree')), \
            patch.object(worker_state.os, 'replace', side_effect=lambda a, b: (events.append('rename'), rename(a, b))[1]), \
            patch.object(worker_state, 'sync_directory', side_effect=lambda p: events.append('parent')):
        migrate_worker_state(root, target, quiesced=True, pid_alive=lambda _: False)
    assert events == ['tree', 'rename', 'parent']


def test_target_appearing_before_promotion_is_preserved(tmp_path):
    root, _ = fixture(tmp_path)
    target = tmp_path / 'new'
    from source_gateway import worker_state
    def other_writer(_):
        target.mkdir()
        (target / 'owner.txt').write_text('other writer')
    with patch.object(worker_state, 'durable_tree', side_effect=other_writer), \
            pytest.raises(WorkerMigrationError, match='changed during'):
        migrate_worker_state(root, target, quiesced=True, pid_alive=lambda _: False)
    assert (target / 'owner.txt').read_text() == 'other writer'


def test_claude_stdout_receipt_does_not_require_a_codex_output_file(tmp_path):
    root, mapping = fixture(tmp_path)
    mapping['provider'] = 'claude'
    report = Path(mapping['report_path'])
    value = json.loads(report.read_text())
    value['provider'] = 'claude'
    report.write_text(json.dumps(value))
    mapping['accepted_receipt_sha256'] = hashlib.sha256(report.read_bytes()).hexdigest()
    Path(mapping['provider_report_path']).unlink()
    (root / 'session-worker-jobs/task-1.json').write_text(json.dumps(mapping))
    before = report.read_bytes()
    target = tmp_path / 'new'
    result = migrate_worker_state(root, target, quiesced=True, pid_alive=lambda _: False)
    copied = json.loads((target / 'session-worker-jobs/task-1.json').read_text())
    assert Path(copied['report_path']).read_bytes() == before
    assert copied['accepted_receipt_sha256'] == mapping['accepted_receipt_sha256']
    assert not Path(copied['provider_report_path']).exists()
    assert result['session_mappings'][0]['missing_optional_artifacts'] == ['provider_report_path']


def test_missing_claude_output_still_requires_an_accepted_host_receipt(tmp_path):
    root, mapping = fixture(tmp_path)
    mapping['provider'] = 'claude'
    mapping.pop('accepted_receipt_sha256')
    Path(mapping['provider_report_path']).unlink()
    (root / 'session-worker-jobs/task-1.json').write_text(json.dumps(mapping))
    with pytest.raises(WorkerMigrationError):
        migrate_worker_state(root, tmp_path / 'new', quiesced=True, pid_alive=lambda _: False)
