# bee-work — a multi-agent cowork framework

Several AI agents sharing one task board and one information layer,
working under explicit roles and claim/review rules, instead of relying
on chat alone — like bees in a hive: one board, many workers, every task
claimed, heartbeated and reviewed.

- **agent-bus/** — the bus core (Go, MCP server). A shared task board:
  `post_task`, `claim_next_task` (lease + heartbeat), `heartbeat_task`,
  `complete_task`, `get_artifact`, `ping_presence`, `bus_state`.
  SQLite storage. Presence is tracked in three tiers (available /
  recently active / registered), claims carry a 900-second lease, and
  empty completions are rejected server-side — a completion must carry
  a real summary and, where the task names one, a full artifact.
- **front-door/** — the gate (Go). Sits in front of the bus: per-client
  default-deny tool allowlists, static-token clients, secret-path
  clients, and a full OAuth 2.0 door (dynamic client registration,
  authorization code + S256 PKCE, rotating refresh tokens) so
  off-the-shelf AI apps can connect as commanders. One static owner
  passcode — held and entered only by the owner — approves every
  consent bind, for every agent. Audit log covers /mcp and OAuth
  traffic; secrets are never logged. The gate embeds a small PNG icon
  (`front-door/favicon.png`) at build time; drop in any PNG under that
  name before building.
- **deepseek-worker/** — the worker (Python). A resident process that
  claims `deepseek`-role tasks from the bus, answers them through a
  configured LLM provider, and completes them with the full answer as
  the artifact. If the model returns an empty answer (a reasoning model
  can burn its whole budget thinking), it retries once at a larger
  token budget, then fails the task honestly instead of completing it
  empty. All secrets are read at runtime — the bus token from a local
  token file, the provider key from a credentials file by reference
  name; neither file is ever committed.
- **chat-gateway/** — the chat front end (Go). A small web chat that
  turns each user message into a bus task for a role (`/claude`,
  `/code`, `/research` prefixes, DeepSeek by default), polls the board
  through one shared hub goroutine, and returns the worker's answer
  when the task lands. Access is gated by a single web code from the
  environment; bus credentials likewise come from the environment.
  v1 is web-only: the Telegram slot is an empty placeholder in the
  code (no bot token), and the `/claude` role slot stays empty until
  a Claude worker process exists — routing to it simply reports that
  no worker claimed the task.

## Governance (mode 乙+)

Every task has a coordinator and a specialty registry. The specialty
owner has a 12-hour priority claim window; urgent tasks skip the
window. Claims carry a lease and heartbeat; expired leases are
reclaimed.

## Iron rule

Private deployment data — memory, resources, credentials, tokens,
passcode hashes, client registries, databases, audit logs — NEVER
enters this repository. Only the general structure lives here. State
files (`clients.json`, `oauth.json`, `gate.env`, `*.token`, `*.url`,
`bus.db`) exist only on the deployment hosts.

## Deployment shape (private instance)

- Bus: systemd `agent-bus-mcp` on the host, loopback :8484.
- Gate: systemd `front-door.service`, loopback :8585, published through
  a tunnel at the owner's domain; the public base URL is configuration,
  not code.
- A bus-to-hub wake watcher rings a chat room for each new task, so
  posting a task actually wakes its target agent.

## Status

1. Bus presence registry (agents table, three tiers, ping/last-seen) —
   live.
2. Acceptance discipline — server-side empty-completion rejection with
   per-agent strikes; honest failures are exempt — live.
3. ChatGPT at the front door — the OAuth flow (discovery → registration
   → consent → token) is verified end to end; ChatGPT connects with a
   user-defined OAuth client whose per-app redirect URI is registered
   on the gate.
4. This repository is the single canonical source for all four pieces.
5. Every participant gets a claim/wake path — no task sits unanswered.
