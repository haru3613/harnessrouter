# Grok route rollout

The public MCP, original Grok owner and source-return capabilities remain on
HARU Dispatch. HarnessRouter authorizes the same staged task; it never creates a
replacement source task or acknowledges Grok delivery.

## Paired deployment

- Deploy the broker gate/runtime/pump version with `router.enabled=false` first.
  Add the dedicated router token via the optional `compose.router.yml` overlay.
- Keep router principals outside source submitters/workers. Their MCP surface
  contains exactly get/activate/cancel route tasks, with no mailbox or event API.
- Start HarnessRouter with `docker-compose.haru.yml` and `docker-compose.grok.yml`.
  Its only published port is localhost Console port 3100. Private networking joins
  the existing Dispatch network; no public proxy or Tunnel change is needed.
- The bootstrap tool creates a dedicated Grok workspace, registered-project
  harnesses and workspace-scoped API key. It writes private runtime files and a
  disabled broker proposal; it does not enable production routing.
- Pin the route key SHA, workspace and harness IDs in the Gateway environment.
  Configure its local-subscription connection with only the restricted Router
  token. Grok/OAuth credentials stay in the original broker.
- Update the Mac worker immutable source to the compatible broker revision before
  adding new configuration keys. Preserve its existing workspaces and state.
- Verify the paired code in isolation and check deployed image revisions, role
  restrictions, public authenticated MCP and source callback health. Enable routing
  for the selected origin only after these checks.

## Rollback

Disable routing for **new** submissions, then inspect all waiting/submitting/
accepted routed tasks. Drain or cancel each by its original source identity;
never bulk-activate or fall back directly to a model. Activated tasks keep their
source identity and can finish through existing workers. Keep the new compatible
broker/pump and Router running until pending rows are zero. Do not restore an old
DB snapshot over tasks accepted after the backup.

## Acceptance boundary

`scripts/haru-route-integration.py --haru-source /PATH/TO/PAIRED/HARU` uses real
HTTP services and a fake host worker to verify one task ID, activation before claim,
waiting-input continuation, source-owned fixture ACK, revoked route-key rejection
and absence of private route material in workspace artifacts. It launches no model.

The final live check must originate from the existing Grok/HARU conversation,
execute one harmless registered fixture and return through that conversation's
original callback/ACK. A controlled fixture ACK or direct operator submission is
not a claim of a visible Grok App reply. If the app-control transport is unavailable,
record the server receipts and ask for the smallest remaining source-side check.
