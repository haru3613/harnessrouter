# HARU fork: experimental Herdr + local subscriptions

This fork adds an opt-in `herdr` execution backend to HarnessRouter. It delegates
read-only work to an existing HARU Dispatch host, where Codex/Claude subscriptions,
quota admission, registered projects and Herdr pane ownership remain authoritative.
It does not configure a paid model API or replace an existing Dispatch deployment.

This is an experimental internal adapter. It has not passed the upstream full
five-scenario/model and custom-harness support matrix. Its visible experimental
label is intentional; file/skill/tool scenarios are explicitly unsupported.

## What works in this first slice

- The Console lists **HARU Herdr subscription** and a **HARU local subscription**
  connection. A connection must be explicitly configured before a task can run.
- `haru-auto`, `haru-codex`, `haru-claude` select the host's routing policy/provider;
  these are not model API names. The host selects its configured native model.
- A task passes only an allowlisted `HARU_WORKSPACE` alias and analysis instructions.
  The driver reports task-state progress, final output and a correlated native ID.
  These are polling updates, not live token streaming from the provider.
- Retries reuse a durable request/task binding. Ambiguous transport does not create
  a replacement task or claim failure of the remote work.
- A native continuation is accepted only for the same authenticated connection and
  an eligible host context. A closed context is refused; it is not silently reset.
- Source ACK and pane cleanup remain separate host facts. The driver never ACKs
  a Grok conversation or treats a cancelled client as proof the Mac stopped.

File transfer, container environments, custom skills/plugins/MCP/tool switches and
answering a host question from this Console are not implemented in v1. Unsupported
inputs fail before dispatch. A host question produces an incomplete result and must
be handled through the configured original source; the host retains its pane.
No remote task audit records or host project data are deleted by this fork.

## Minimal isolated Docker instance

Create a private `.env.haru` (ignored by Git) with your chosen values:

```dotenv
HARU_CONSOLE_USER=haru
HARU_CONSOLE_PASSWORD=replace-with-a-strong-private-password
HARU_SECRET_KEY=replace-with-a-random-local-encryption-secret
HARU_CONSOLE_PORT=3100
```

```sh
chmod 600 .env.haru
docker compose --env-file .env.haru -f docker-compose.haru.yml up -d --build
```

Open `http://127.0.0.1:3100`. This profile uses its own volume, installs no local
agent CLIs, and omits Starter Kits, bundled skills, document preview and media tools.
It does not mount the Mac's HOME, repository checkouts or Docker socket.

In the Console's provider connections, add **HARU local subscription**:

1. **Dispatch MCP endpoint**: the intended host broker's exact HTTPS `/mcp` URL.
2. **Dispatch access token**: a source-scoped token for that broker. This is not
   an OpenAI/Anthropic key; subscription OAuth stays on the Mac.
3. Create a harness with base **HARU Herdr subscription** and your chosen selector.
4. In its environment settings set `HARU_WORKSPACE` to a registered alias.

A container's `127.0.0.1` is itself, not the Mac. The driver permits HTTP only for
loopback testing in the same network namespace. Use a dedicated HTTPS route for
Docker-to-host operation. Never publish the unauthenticated native development
fixture or silently redirect the existing production endpoint.

Use a dedicated source principal/origin for this Console. Do not reuse a Grok
conversation's token as if it created a new source; HARU binds returns to the
configured authenticated origin. Before enabling a real source, verify its result
handling and ACK independently. Gateway runs this connection in the explicit
self-hosted owner-trust mode; broker-trust mode refuses it.

## API example

After creating a Console harness and an instance API key:

```json
{
  "model": "haru-codex",
  "input": "Read the registered project README and summarize its entry points with evidence.",
  "background": true,
  "metadata": {"harness_id": "YOUR_HERDR_HARNESS_ID"}
}
```

POST this to `/api/harness/v1/responses` with the instance API bearer and a unique
`Idempotency-Key` header. Poll the response ID or use streaming. Provider credentials
must never be sent in the prompt, metadata or files.

## Verification and boundaries

See [the isolated live verification procedure](live-verification.md) and
[the implementation contract](implementation-plan.md). The live harness uses a
fresh HARU fixture and preserves existing Herdr workspaces. Its controlled source
ACK is not a claim that Grok App visibly replied.

Cancellation has two separate meanings: the local request is cancelled, while the
host process stop can remain unverified. Herdr cancel responses expose
`host_stop_status=unverified`; stored response metadata records the same boundary.
Do not enable project writes based on a stopped driver or disappeared pane.

Core upstream remains Apache-2.0. Preserve LICENSE/NOTICE and modification notices
when redistributing. Starter Kits and native agent CLIs have their own terms.
