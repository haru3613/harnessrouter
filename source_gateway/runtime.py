"""Entrypoint for the isolated private Dispatch compatibility process."""

from __future__ import annotations

import importlib
import os
from pathlib import Path
import sys

from .validation import StateValidationError, validate_state


def _preflight() -> Path:
    # Use the compatibility package's own state variable so the validated path
    # and the path it later opens can never diverge.
    state_dir = Path(os.environ.get("HARU_DISPATCH_STATE_DIR", "/state")).expanduser()
    compat_root = Path(os.environ.get("HR_DISPATCH_COMPAT_ROOT", "/app/compat"))
    report = validate_state(state_dir, compat_root=compat_root)
    os.environ["HARU_DISPATCH_STATE_DIR"] = str(state_dir.absolute())
    expected = os.environ.get("HR_DISPATCH_SOURCE_REVISION")
    if not expected or expected != report["source_revision"]:
        raise StateValidationError("source revision mismatch")
    return compat_root


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if args not in (["run"], ["health"]):
        raise StateValidationError("usage: source_gateway.runtime {run|health}")

    # Validation deliberately precedes both sys.path mutation and compatibility
    # import: constructors in that package initialize missing state.
    compat_root = _preflight()
    if any(name == "haru_dispatch" or name.startswith("haru_dispatch.") for name in sys.modules):
        raise StateValidationError("compatibility package already loaded")
    sys.path.insert(0, str(compat_root))
    try:
        runtime = importlib.import_module("haru_dispatch.runtime")
        delegated = ["haru-dispatch-runtime", "combined"]
        if args[0] == "health":
            delegated = ["haru-dispatch-runtime", "health", "combined"]
        previous = sys.argv
        try:
            sys.argv = delegated
            result = runtime.main()
        finally:
            sys.argv = previous
    finally:
        try:
            sys.path.remove(str(compat_root))
        except ValueError:
            pass
    return 0 if result is None else int(result)


if __name__ == "__main__":
    try:
        exit_code = main()
    except KeyboardInterrupt:
        exit_code = 130
    except BaseException:
        # Compatibility errors may include request URLs or credentials. Keep
        # container output generic; operators inspect non-secret health state.
        print("source gateway failed", file=sys.stderr)
        exit_code = 1
    raise SystemExit(exit_code)
