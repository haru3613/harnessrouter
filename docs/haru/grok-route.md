# Grok -> HarnessRouter activation gate (2026-10-26)

User authorized connecting Grok to the merged HarnessRouter fork. Preserve the
existing public MCP URL, OAuth, Grok source identity, original task ID and callback
ACK. Do not proxy/replace the whole public endpoint and do not enable task writes.

## Route

1. The existing authenticated Grok submission creates exactly one HARU task with
   its original request fingerprint and origin. Eligible new analysis tasks enter
   `session_waiting_route`, which the existing host worker cannot claim.
2. A durable route outbox submits a HarnessRouter response using Idempotency-Key
   `haru-route-<task_id>`. Router outage leaves the task waiting; no direct fallback.
3. HarnessRouter's trusted adopt-existing-task path forwards a bounded activation
   capability to its Herdr driver via environment only (never prompt/argv/job/
   response/trace/checkpoint). The driver never calls submit_session_task in this mode.
4. The restricted router principal activates that same task atomically, binding the
   HarnessRouter response ID. Then the existing worker may claim session_queued.
5. Driver polls the existing task. Waiting for Grok input remains nonterminal;
   original Grok replies can resume the same host task. Genuine observation timeout
   or uncertain stop stays explicit. No source ACK is minted by HarnessRouter.
6. Existing delivery wakes Grok and validates its original capability ACK. Existing
   task state, original source, provider selection and task body are preserved.

## Broker configuration and authority

New optional `router` config, disabled by default: enabled, origins (origin aliases),
principals (restricted router token identities), gateway_url (explicit local/HTTPS
base), api_key_env (environment variable name, not secret), harnesses (workspace alias
-> HarnessRouter harness ID), timeout_seconds (outbox claim lease; HTTP calls remain capped at 10 seconds), retry_seconds. Harness observation limits are configured separately on each harness.
Role identities must be disjoint from submitters/workers. Public router tools are only
`get_route_task`, `activate_route_task`, `cancel_route_task`. No mailbox, worker claim,
result reporting, source ACK or arbitrary task submission permission.

A route row binds task_id, request fingerprint, router principal, gateway URL,
harness_id, selector, a private nonce/capability and response_id/state. Capability
verification is constant-time and task/fingerprint/router-bound; never returned by
source task reads. Terminal source cancellation prevents later activation. Repeated
activation with the same response is idempotent; a competing response is refused.

Freeze routing intent for new tasks, including an unresolved workspace clarification.
Stage only before the first execution; replies to execution-time questions must not
create another route. Source cancellation before activation completes without launching
work and releases any continuation reservation. Old tasks stay untouched.

## HarnessRouter transport

The route pump uses a dedicated workspace-scoped HarnessRouter API key. Adopt headers:
`X-Haru-Route-Task`, `X-Haru-Route-Fingerprint`, `X-Haru-Route-Capability`.
Gateway accepts these only when bearer SHA256 matches operator configuration
`HR_HARU_ROUTE_API_KEY_SHA256`, and only for the Herdr backend in the authorized
workspace. Public metadata is never trusted as an adoption grant.

The Gateway passes a signed internal `haru_route` descriptor to Runner, binding
{task_id, fingerprint, harness_id, response_id, capability_sha256}; signature uses the
existing internal service key. Raw capability travels separately in a secret field,
never in the persistable descriptor. Runner verifies signature and capability hash,
stores only non-secret identifiers in the job, and supplies the capability in a
private env var. Driver uses the configured
restricted router MCP token and calls:
- get_route_task({task_id})
- activate_route_task({task_id, fingerprint, capability, response_id})
- cancel_route_task({task_id, fingerprint, capability, response_id})

Provider selector must match the originally requested provider or auto. Host task data
wins; no rewriting/downgrading original capabilities/result contract. Driver may not
silently submit a new task or change source identity. Headers/capabilities must not leak
into stored public metadata, logs, prompts or error envelopes. Restarts/retries reuse
correlation; a definitive pre-activation route failure is visible, never direct fallback.

## Verification and rollout

Use test-first at staging/activation, outbox retry, secret boundary and adoption seams.
Test owner/foreign/router permissions, duplicate request and activation, cancelled
staging race, waiting input, unsupported writes, route outage and pre-existing jobs.
Test the exact two-repo revision combination in isolation first. Run affected suites,
independent review, then deploy additive changes with DB/config backup and rollback.
Only enable Grok routing after HarnessRouter health and dedicated credentials/harnesses
are ready. Keep the old source URL, tunnels, data volumes and unrelated jobs unchanged.
Disabling the flag affects new submissions only; pending routed tasks require explicit
inspection/activation/cancellation and never silently fall back.

Final proof: a real Grok-origin task has one HARU ID, one HarnessRouter response, one
native execution, a visible result in HarnessRouter, original-source handling/ACK and
safe pane cleanup. Desktop tool availability is a separate UI proof boundary.


Adoption also requires `HR_HARU_ROUTE_INTEGRATION` and
`HR_HARU_ROUTE_MCP_ENDPOINT` to match the named local-subscription connection
exactly, in both Gateway and Runner. Global model maps and fallback chains are
not candidates for adoption. Cancellation before activation or before a worker
claim terminates the source task and emits its original return event; active
work receives a cancellation request, with the host retaining responsibility
for reporting whether the process stopped.
