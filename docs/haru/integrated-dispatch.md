# Integrated Dispatch ownership

The operator selected full replacement of the old Dispatch deployment. The new
HarnessRouter deployment owns MCP/OAuth ingress, durable broker state, delivery,
the native host-worker installation and the fixed tunnel. Existing source URLs,
identities, tokens, task IDs, return capabilities and read-only limits survive.

## Runtime design

- A small `source-gateway` service is owned and released by the HarnessRouter
  Compose project. It bundles the pinned compatibility protocol, its MCP/OAuth
  HTTP listener and one combined delivery/route loop. The Gateway/Runner/UI
  container mounts none of its source credentials or broker volume. This keeps
  source authority isolated from the root Runner while retiring the old project.
- The compatibility package comes from a pinned private source artifact through
  an explicit Docker named build context. Its code and credentials are not
  committed to this public fork. The image records its source revision and the
  artifact hash; no private layer is pushed to a public registry or cache.
- A HarnessRouter-owned volume holds the imported broker SQLite databases and
  capability/signing keys. A migration-complete manifest is checked read-only
  before any compatibility constructor; missing or inconsistent state fails
  closed rather than creating a new identity. Secret values are supplied only
  to source-gateway and the tunnel credential only to the new tunnel service.
- Source-gateway remains a single non-root process, read-only outside its state
  volume, with private secret mounts, no added capabilities, and a bounded
  memory limit. Its health check verifies authenticated MCP and fresh delivery
  heartbeat. A failed loop stops that source process for Compose to restart;
  active HarnessRouter turns are not terminated by a source delivery failure.
- The route pump resolves the HarnessRouter service on the new project network.
  The Router integration retains the pinned public HTTPS MCP endpoint. The
  restricted router identity still has only get/activate/cancel route tools.
- The fixed tunnel becomes a service in the HarnessRouter Compose project and
  resolves its existing upstream alias to source-gateway. It keeps the same
  tunnel ID and credential, copied to a HarnessRouter-owned private path.
- The host session worker uses the same pinned artifact under HarnessRouter-owned
  paths and a new launch-agent label. An atomic migration rewrites only known
  state-root locator fields, copies referenced artifacts, and preserves hashed
  execution receipts byte-for-byte. It never replays a submitted command.

## Cutover and rollback

Prepare and test the image before changing live state. Record existing jobs,
OAuth/client/token rows, key digests, queued work, active native processes and
unhandled returns. Take private DB/config/worker/tunnel backups. Do not print
secret values or export the private source artifact to the public repository.

Stop the old tunnel first. Boot out and persistently disable the old session
worker and its automatic restart paths; prove zero active owned provider
processes/descendants and `delivery_attempts.in_flight=1` rows. Stop the old
broker/pump and disable container restart policies before cloning. Use offline SQLite backup/checkpoint semantics
for all three broker databases, including WAL-resident commits and session keys;
check integrity/foreign keys, baseline primary-key sets and cross-store event
references, plus private salted key comparisons. Verify worker locators resolve
to copied files while execution receipts keep their original hashes. Persist and
validate the migration manifest before startup. Keep the original volumes
as rollback material. An already-submitted native command must never replay.
Pending source returns keep their event IDs, handles and source capability keys.

Start the integrated service, verify local role/OAuth checks and internal route
pinning, then start the HarnessRouter-owned host worker and tunnel. Verify the
public endpoint using the same clients. Only after native and source return
acceptance remove the already-disabled old definitions and containers. Before
removing the old network, require zero `waiting_workspace`, `pending`,
`submitting`, `retry` or `accepted` route rows bound to its frozen destination,
recreate HarnessRouter without the external network and inspect membership. Preserve original volumes/backups; do not run a volume-pruning down.

With the new tunnel off, complete local image, role, OAuth, route and worker
checks. Before enabling the new connector, prove the old named-tunnel connector
is stopped and no old connector process/container remains. Record the rollback
boundary immediately before starting the sole new connector: existing clients
can submit or ACK before an operator observes it. Before that boundary, rollback
can restore the stopped stack and matching snapshot under the same fence.
After connector start, recovery is forward repair only; do not restore the old state snapshot over accepted tasks. Every
pre-traffic rollback restores matching code, config and Compose commands together.

## Acceptance

Unit/HTTP checks cover disabled defaults, startup failure, source-gateway listener and
pump lifetime, health, private credentials, role restrictions and imported state.
Run the exact integrated image with a fixture before live cutover. Test preserved
OAuth identity/token behavior, source task IDs, historical jobs and an existing
unhandled return. Verify one new source task, one response and one native attempt,
then original-source handling/ACK and owned-pane cleanup. A controlled-source ACK
does not substitute for a real Grok source ACK or visible App inspection.

Finish with no running or stopped containers belonging to the old Compose
project, no old worker launch agent running, no old runtime/config/token paths
in active launch arguments or mounts, and measured memory receipts for the new
steady state. Retained archives and historical project workspace paths are data,
not runtime dependencies.


The source service starts with a 256 MiB limit. Lower it only after measured peak
and steady-state receipts. Its ingress network is shared with the tunnel only;
its route network is shared with HarnessRouter only. The tunnel never joins the
internal route network.
