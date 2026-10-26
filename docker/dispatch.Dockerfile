# syntax=docker/dockerfile:1.7
FROM python:3.12-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1

ARG HR_DISPATCH_SOURCE_REVISION
ARG HR_DISPATCH_ARTIFACT_SHA256

RUN groupadd --gid 10001 dispatch \
    && useradd --uid 501 --gid 10001 --no-create-home dispatch \
    && mkdir -p /app/compat /app/source_gateway /state \
    && chown -R 501:10001 /app /state

WORKDIR /app

# dispatch_source is an explicit private named build context. Do not replace it
# with a path in this public repository or publish the resulting private layer.
COPY --from=dispatch_source --chown=501:10001 . /app/compat
COPY --chown=501:10001 source_gateway /app/source_gateway

RUN python3 - "$HR_DISPATCH_SOURCE_REVISION" "$HR_DISPATCH_ARTIFACT_SHA256" <<'PY'
from hashlib import sha256
from pathlib import Path
import sys

root = Path("/app/compat")
expected_revision, expected_tree_hash = sys.argv[1:]
if not expected_revision:
    raise SystemExit("HR_DISPATCH_SOURCE_REVISION is required")
if not expected_tree_hash:
    raise SystemExit("HR_DISPATCH_ARTIFACT_SHA256 is required")

revision_file = root / "DISPATCH_SOURCE_REVISION"
actual_revision = revision_file.read_text(encoding="utf-8").strip()
if actual_revision != expected_revision:
    raise SystemExit("private dispatch artifact revision does not match the pinned revision")

tree_hash = sha256()
files = sorted(
    (path for path in root.rglob("*") if path.is_file() and "__pycache__" not in path.parts),
    key=lambda path: path.relative_to(root).as_posix(),
)
for path in files:
    relative_path = path.relative_to(root).as_posix().encode("utf-8")
    tree_hash.update(relative_path)
    tree_hash.update(b"\0")
    tree_hash.update(sha256(path.read_bytes()).digest())

if tree_hash.hexdigest() != expected_tree_hash.lower():
    raise SystemExit("private dispatch artifact tree hash does not match the pinned hash")
PY

LABEL org.opencontainers.image.revision="${HR_DISPATCH_SOURCE_REVISION}" \
      ai.harnessrouter.dispatch-source.sha256="${HR_DISPATCH_ARTIFACT_SHA256}"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app:/app/compat \
    HARU_DISPATCH_STATE_DIR=/state \
    HARU_RUNTIME_CONFIG=/run/config/config.json \
    HR_DISPATCH_COMPAT_ROOT=/app/compat \
    HR_DISPATCH_SOURCE_REVISION=${HR_DISPATCH_SOURCE_REVISION}

USER 501:10001

HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
    CMD ["python", "-m", "source_gateway.runtime", "health"]

ENTRYPOINT ["python", "-m", "source_gateway.runtime"]
CMD ["run"]
