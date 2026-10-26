import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from source_gateway import runtime
from source_gateway.validation import StateValidationError
from test_source_gateway_validation import REVISION, build_compat, build_state


def _forget_fake_package():
    for name in list(sys.modules):
        if name == "haru_dispatch" or name.startswith("haru_dispatch."):
            del sys.modules[name]


@pytest.mark.parametrize(
    ("command", "delegated"),
    [
        ("run", ["haru-dispatch-runtime", "combined"]),
        ("health", ["haru-dispatch-runtime", "health", "combined"]),
    ],
)
def test_wrapper_preflights_then_delegates_combined(tmp_path, monkeypatch, command, delegated):
    marker = tmp_path / "calls.json"
    compat = build_compat(
        tmp_path,
        """
import json
import os
import sys

def main():
    with open(os.environ["FAKE_RUNTIME_MARKER"], "w") as target:
        json.dump(sys.argv, target)
""",
    )
    state = build_state(tmp_path, compat)
    monkeypatch.setenv("HARU_DISPATCH_STATE_DIR", str(state))
    monkeypatch.setenv("HR_DISPATCH_COMPAT_ROOT", str(compat))
    monkeypatch.setenv("HR_DISPATCH_SOURCE_REVISION", REVISION)
    monkeypatch.setenv("FAKE_RUNTIME_MARKER", str(marker))
    _forget_fake_package()

    assert runtime.main([command]) == 0

    assert json.loads(marker.read_text()) == delegated


def test_invalid_state_is_rejected_before_compatibility_import(tmp_path, monkeypatch):
    import_marker = tmp_path / "imported"
    compat = build_compat(
        tmp_path,
        f'from pathlib import Path\nPath({str(import_marker)!r}).write_text("imported")\n'
        "def main():\n    raise AssertionError('must not run')\n",
    )
    state = build_state(tmp_path, compat)
    (state / "jobs.sqlite3").unlink()
    monkeypatch.setenv("HARU_DISPATCH_STATE_DIR", str(state))
    monkeypatch.setenv("HR_DISPATCH_COMPAT_ROOT", str(compat))
    monkeypatch.setenv("HR_DISPATCH_SOURCE_REVISION", REVISION)
    _forget_fake_package()

    with pytest.raises(StateValidationError):
        runtime.main(["run"])

    assert not import_marker.exists()


def test_environment_revision_is_required_before_import(tmp_path, monkeypatch):
    import_marker = tmp_path / "imported"
    compat = build_compat(
        tmp_path,
        f'from pathlib import Path\nPath({str(import_marker)!r}).write_text("imported")\n'
        "def main():\n    return None\n",
    )
    state = build_state(tmp_path, compat)
    monkeypatch.setenv("HARU_DISPATCH_STATE_DIR", str(state))
    monkeypatch.setenv("HR_DISPATCH_COMPAT_ROOT", str(compat))
    monkeypatch.delenv("HR_DISPATCH_SOURCE_REVISION", raising=False)
    _forget_fake_package()

    with pytest.raises(StateValidationError, match="source revision mismatch"):
        runtime.main(["run"])

    assert not import_marker.exists()


def test_unknown_wrapper_command_never_preflights_or_imports(monkeypatch):
    called = False

    def unexpected():
        nonlocal called
        called = True

    monkeypatch.setattr(runtime, "_preflight", unexpected)
    with pytest.raises(StateValidationError, match="usage"):
        runtime.main(["serve"])
    assert called is False


def test_cli_hides_compatibility_exception_details(tmp_path):
    secret = "token-value-must-not-be-logged"
    compat = build_compat(
        tmp_path,
        "import os\ndef main():\n    raise RuntimeError(os.environ['SENSITIVE_VALUE'])\n",
    )
    state = build_state(tmp_path, compat)
    environment = os.environ.copy()
    environment.update({
        "HARU_DISPATCH_STATE_DIR": str(state),
        "HR_DISPATCH_COMPAT_ROOT": str(compat),
        "HR_DISPATCH_SOURCE_REVISION": REVISION,
        "SENSITIVE_VALUE": secret,
    })

    completed = subprocess.run(
        [sys.executable, "-m", "source_gateway.runtime", "run"],
        cwd=Path(__file__).parents[2],
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 1
    assert completed.stderr.strip() == "source gateway failed"
    assert secret not in completed.stdout + completed.stderr
