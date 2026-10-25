# HARU fork implementation — 2026-10-26

User authorization: fork HarnessRouter and implement the researched Herdr/subscription
integration using the implement skill. Baseline upstream edca78b. This first vertical
slice preserves the existing HARU Mac worker and production endpoint, and is verified
against an isolated fixture deployment before considering a cutover.

## Contract

- Add a `herdr` remote backend, provider `local_subscription` only.
- Logical model selectors: `haru-auto`, `haru-codex`, `haru-claude`; actual native model
  selection remains with the host's configured subscription policy.
- Connection `api_key` means a scoped Dispatch source access token (label it accordingly),
  not a model API credential. `base_url` is an explicitly configured MCP `/mcp` endpoint.
- The harness environment must set `HARU_WORKSPACE` to a registered workspace alias.
  Never accept host filesystem paths or grant writes. Submit mode=analysis and default
  result_contract=evidence_required; host owns quota/login/project admission.
- Driver uses the existing MCP submit_session_task/get_task/cancel_session_task tools.
  It persists request/task correlation before submission, retries only the exact
  idempotent request, and recovers the same task rather than rerunning work.
- Secrets are not in argv, job JSON, stdout, logs, checkpoint files, or prompts. No
  subscription OAuth credentials leave Mac. No API-key provider fallback is permitted.
- Progress is task-state progress plus final result, not invented token streaming.
  Completion, original-source ACK and Herdr resource cleanup remain separate facts.
- A cancellation request is not proof all descendants stopped. Propagate stop-unknown
  explicitly; stopping the container driver never implies stopping the Mac process.
- Reusing a native context requires an explicit, eligible host continuation. Unsupported
  resume, file uploads, tools/skills/plugins/MCP injection, or environment mounts must
  fail before remote submission; do not silently ignore or pretend they work.
- Remote deletion must not delete project data or task audit records. Only verified
  owned pane cleanup occurs via the existing host lifecycle.

## Implementation seams

1. New stdlib-only runner/herdr_driver.py + focused transport/lifecycle tests.
2. Runner registry/builder/event adapter/availability/remote cancellation wiring.
3. Gateway provider/catalog/admission/connection checks, using existing Console forms.
4. Minimal isolated compose profile (no starter kits/bundled skills/provider API keys),
   operator setup and live smoke harness/receipts.

## Acceptance

This is an experimental internal adapter, not an upstream verified harness base.
The upstream five-scenario/model and custom-harness release matrix is deferred:
v1 deliberately refuses artifact transfer, custom tools and skills. Catalog
`ready` only means the adapter is implemented/selectable; its visible label must
say experimental. No broader protocol/conformance or production cutover claim
is permitted until those capability-specific gates are independently passed.

- Test-first at new transport and routing seams; frequent focused tests/type checks.
- Real Responses API -> Runner driver -> isolated HARU broker -> Mac Herdr pane ->
  subscription-backed Codex -> stored result. Verify existing production untouched,
  proper failure/retry behavior, source ACK boundary and eventual pane cleanup.
- Claude must be tested separately or reported blocked by quota/auth, never assumed.
- Full repository suite at the end, code review, fix findings, commit to feature branch.
  No automatic merge/deploy to existing production or paid provider configuration.
