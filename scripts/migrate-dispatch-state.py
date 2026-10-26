#!/usr/bin/env python3
"""Offline broker/host-state migration; never stops services or prints secrets."""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    inspect = commands.add_parser("inspect-worker", help="read-only native-worker quiescence check")
    inspect.add_argument("--source", required=True)
    worker = commands.add_parser("worker", help="relocate fenced terminal native-worker state")
    broker = commands.add_parser("broker", help="clone fenced broker databases and identity")
    for command in (worker, broker):
        command.add_argument("--source", required=True)
        command.add_argument("--destination", required=True)
        command.add_argument("--quiesced", action="store_true", help="operator has fenced all old writers")
    broker.add_argument("--compat-root", required=True)
    broker.add_argument("--source-revision", required=True)
    args = parser.parse_args()
    if args.command == "broker":
        from source_gateway.migrate_state import clone_broker_state
        result = clone_broker_state(args.source, args.destination, args.compat_root,
                                    args.source_revision, quiesced=args.quiesced)
    else:
        from source_gateway.worker_state import inspect_worker_state, migrate_worker_state
        result = (inspect_worker_state(args.source) if args.command == "inspect-worker" else
                  migrate_worker_state(args.source, args.destination, quiesced=args.quiesced))
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # State may contain execution or authentication material; fail without
        # serializing a database value or raw exception from a copied artifact.
        print(json.dumps({"error": "dispatch migration failed", "type": type(exc).__name__}), file=sys.stderr)
        raise SystemExit(1)
