# HARU Herdr backend live verification

This is the only planned live test for the first fork slice. It proves a new,
isolated flow; it does not migrate or reconfigure the existing Dispatch service,
its Cloudflare endpoint, its production data, or a CardDex checkout.

```text
HarnessRouter Responses API
  -> runner herdr driver
  -> temporary HARU MCP broker + session worker on this Mac
  -> new owned Herdr pane + existing Codex/Claude subscription CLI
  -> stored response + controlled fixture-source ACK + pane resource cleanup
```

The test reports each boundary independently. A successful HarnessRouter response
does not establish a real Grok conversation wake. The fixture source is a local,
controlled adapter and its ACK is recorded as such.

## Preconditions

- The fork includes the `herdr` base and `local_subscription` provider, and its
  focused gateway/runner tests pass.
- Run from an authorized Herdr pane (`HERDR_ENV=1`), with an existing Herdr socket.
- Use the immutable HARU release/source being tested, e.g.
  `/Users/harvey/Library/Application Support/haru-dispatch/releases/f215a2b03ddf8230106a714d6723c4d05ca07dbc`.
- Start a separate local HarnessRouter instance with an empty data volume. Do not
  point its `HARNESS_WORKSPACE`, data volume, or its container mounts at a CardDex
  repository or the existing HARU state directory. Start with `WITH_STARTER_KITS=0`
  and `WITH_BUILTIN_SKILLS=0`; do not configure a model-provider API key.
- For this first proof run Gateway and Runner on the Mac host, or use a dedicated
  private HTTPS endpoint to the fixture broker. A Docker container cannot reach the
  host's `127.0.0.1` endpoint. The integration validator intentionally refuses a
  normal remote `http://` URL.

## 1. Create the isolated HARU fixture

Choose an empty parent directory, not an existing project. The command creates
a new `haru-fork-smoke-*` directory, a single `fixture/facts.txt`, unique tokens,
a loopback broker, and a session worker. It uses a fresh state directory and
does not read the deployed HARU configuration or token files.

```sh
python3 scripts/haru-live-smoke.py prepare \
  --haru-source /ABSOLUTE/PINNED/HARU-RELEASE \
  --herdr-socket /Users/harvey/.config/herdr/herdr.sock \
  --fixture-parent /ABSOLUTE/EMPTY-SMOKE-PARENT
```

Save the emitted `run_root`. The JSON also names the loopback MCP endpoint and
the mode-0600 token file; it intentionally never prints the token value.

## 2. Configure the isolated HarnessRouter instance

In the isolated Console, add exactly one integration:

| Field | Value |
| --- | --- |
| Provider | `HARU local subscription` |
| Dispatch endpoint | `haru_endpoint` from prepare output |
| Dispatch access token | contents of the emitted `owner_token_file` |

Create a custom harness with base **HARU Herdr subscription**, model
`haru-codex`, and environment variable `HARU_WORKSPACE=fixture`. Do not add
files, environments, MCP servers, skills, plugins, tools, or a provider API key.
These are deliberately unsupported by v1 and should be refused before submission.

Create a HarnessRouter API key for that isolated workspace, save it to a
mode-0600 file, and provide its path to the next command. This key is only the
fork API credential; it is not a model-provider credential.

## 3. Run the public API proof

Use the harness ID created above. `--create-harness` is available only after
the integration already exists; it cannot create the integration because that
route has separate Console administrator authentication.

```sh
python3 scripts/haru-live-smoke.py verify \
  --run-root /ABSOLUTE/PARENT/haru-fork-smoke-... \
  --gateway-url http://127.0.0.1:3000/api/harness \
  --gateway-api-key-file /ABSOLUTE/isolated-hr-api-key \
  --harness-id chrn_... \
  --model haru-codex
```

The script makes one idempotent `POST /v1/responses` then polls the same
response and the isolated HARU task. It refuses to submit if that fixture broker
already has a task. It writes `verification-receipt.json` without tokens.

Before transmission it writes a private `response-request.json` containing the
exact body, idempotency key, intended harness and eventual response id. If the
network fails before an id is returned, do not run a new verification command
that creates another task. Inspect the saved intent, then retry that exact request:

```sh
python3 scripts/haru-live-smoke.py verify \
  --run-root /ABSOLUTE/PARENT/haru-fork-smoke-... \
  --gateway-url http://127.0.0.1:3000/api/harness \
  --gateway-api-key-file /ABSOLUTE/isolated-hr-api-key \
  --resume-request
```

The replay sends the original `Idempotency-Key` HTTP header and does not generate
another prompt or id. Once a response id is saved, `verify` only reads that
response and never posts a replacement.

Pass requires all of the following:

1. The API response and HARU task contain the random fixture marker.
2. The HARU task is `succeeded`.
3. The controlled fixture source reads and acknowledges the result; HARU reports
   `return_state=handled`.
4. A new matching `dispatch-*` Herdr workspace appeared, then its resource became
   `closed` through normal worker cleanup.
5. Every workspace present before the request remains present afterward.

The receipt explicitly sets `grok_original_conversation_verified=false`. Do not
turn that into a Grok acceptance claim.

## Failure and cleanup

On failure, preserve the fixture directory, `broker.log`, `worker.log`, task
database and receipt. Do not submit a second request with a new id while the
first task's outcome is unresolved. Inspect the original response and task first.

This only stops the broker and worker processes whose command line contains the
exact fixture root; it never deletes the fixture directory, a Herdr workspace,
or a task record:

```sh
python3 scripts/haru-live-smoke.py cleanup \
  --run-root /ABSOLUTE/PARENT/haru-fork-smoke-...
```

If cancellation is included in a later test, preserve the current HARU meaning:
the request may be accepted while the host reports `stop_unknown`; no result or
workspace cleanup should be treated as proof every detached child process stopped.
