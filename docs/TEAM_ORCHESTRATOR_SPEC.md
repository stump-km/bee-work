# Team Orchestrator: Unified Participant Interface

**Status:** Proposed engineering specification  
**Scope:** bee-work runtime architecture and the first implementation contract  
**Primary user experience:** one ChatGPT conversation window for the owner; many independent participants behind it.

## 1. Product requirement

The owner must be able to issue a natural-language instruction from one connected ChatGPT MCP endpoint and have bee-work coordinate the whole participant team. The owner must not need to know which connector, role prefix, worker endpoint, or low-level bus tool to call.

The system must support four distinct interaction patterns:

1. **Direct assignment:** send work to one named participant.
2. **Role assignment:** send work to one or more participants selected by role.
3. **Broadcast:** create a separate delivery for every eligible participant in the selected audience. Broadcast is not a shared queue item that agents race to claim.
4. **Delegated collaboration:** appoint a coordinator participant, allow it to create or assign child work within an explicitly granted scope, and collect the child results under one parent request.

A fifth operation, **team status**, gives the owner a single view of participants, assignments, progress, failures, and returned artifacts.

## 2. User-facing interaction contract

ChatGPT should see a small, stable MCP surface rather than the internal bus API:

- `team_command`: submit an instruction with an optional audience, dispatch mode, coordinator, and response policy. Natural-language interpretation may happen in ChatGPT; the tool must receive a validated, explicit dispatch request.
- `team_status`: inspect the team roster and a parent request's child-task progress.
- `team_result`: retrieve a completed participant result or the consolidated parent result.
- `team_cancel`: request cancellation of a parent request and its still-pending children, subject to ownership and authorization rules.

The existing low-level tools remain available to worker identities and operational administrators, but the ordinary owner-facing ChatGPT client should not need to call them individually.

A typical interaction:

> Owner: Ask everyone to independently inspect the new architecture and report the three highest-risk design issues. Then have the architect reconcile disagreements.

The runtime creates a parent request, snapshots the eligible audience, creates one child task per participant, and records each delivery separately. After independent results arrive, the coordinator phase can be created with those results as explicit input. The owner sees one parent request with participant-level status and a final synthesis.

## 3. Architectural placement

Add a **Team Orchestrator** as a control-plane layer between the owner-facing MCP facade and the existing Agent Bus.

- **ChatGPT / owner client:** the single conversational command surface.
- **Front Door:** authenticates the owner and enforces client-level tool permissions. It remains the security boundary; it must not become a second task database.
- **Team Orchestrator:** validates audience selection, snapshots recipients, creates parent/child relationships, applies dispatch policy, aggregates results, and exposes team-level status.
- **Agent Bus:** remains the source of truth for participant presence, task claims/leases, task state, and artifacts.
- **Participants / workers:** remain independent processes. They claim only tasks addressed to their participant identity or eligible role and return their own outputs.
- **Wake path:** the existing bus-to-hub watcher (or a replacement event delivery mechanism) wakes each addressed participant. Creating a task is not sufficient unless its recipient has a working wake/claim path.

The first implementation can place the orchestration handlers inside the Agent Bus process to avoid introducing another service and another deployment failure domain. Keep the API and internal package boundaries clean so the orchestrator can be extracted later if scale or deployment isolation requires it.

## 4. Core domain model

### 4.1 Team request

A `team_request` is the owner-visible unit of work. It has a stable ID, owner/session binding, original instruction, dispatch mode, requested audience, audience snapshot, optional coordinator, lifecycle status, timestamps, and a result policy.

Suggested states: `accepted`, `dispatching`, `running`, `aggregating`, `completed`, `partial`, `failed`, `cancel_requested`, `cancelled`.

### 4.2 Participant delivery

A `team_delivery` is one independently claimable child task addressed to one participant. It records the parent request ID, participant ID, role snapshot, child task ID, delivery state, and result reference. Each participant receives its own delivery record and task ID.

This is the critical broadcast invariant: **one recipient, one delivery, one independently attributable result**. Never implement broadcast by placing one task in a role queue and allowing every participant to race for it.

### 4.3 Participant identity and role

The participant's stable identity is `agent_id`; role is a capability/classification, not identity. A participant can change roles without changing its identity. Audience resolution must use the registry snapshot at dispatch time and record exactly who was selected.

### 4.4 Parent-child relation

The parent request groups child tasks but does not replace their task lifecycle. Existing lease, heartbeat, completion validation, and artifact behavior remain in force for each child. Parent status is derived from child states and orchestration phase, not manually guessed by the UI.

## 5. Broadcast and routing semantics

### Direct participant
Resolve exactly one registered participant. Reject unknown or ambiguous IDs. Create one delivery addressed to that participant.

### Role audience
Resolve all eligible participants for the requested role, subject to presence policy. If multiple participants share a role, the default behavior should be fan-out to all, not first-worker-wins. A separate future policy may support one-of-N load balancing.

### All participants
Snapshot every registered participant matching the selected presence policy. The default audience should be participants recently active/available; registered-but-stale participants should be visible but excluded unless the owner explicitly includes them. Return the exact recipient list before or with dispatch confirmation.

### Coordinator delegation
A coordinator is not implicitly granted unrestricted authority. The parent request stores its delegation scope: allowed roles/participants, maximum child count, whether it may delegate recursively, and whether external side effects are prohibited. The initial MVP should disable recursive delegation and allow only child tasks within the original audience/scope.

### Empty audience
Do not silently report success. Return `no_eligible_participants` with the roster and selection explanation. The owner may choose to include registered-but-offline participants, but those deliveries must be visibly marked as waiting for an offline worker.

## 6. Atomicity and task lifecycle

Creating a parent request and its initial delivery rows must be one database transaction. A partial fan-out must not appear as a successful full broadcast.

For each child task:
- it is claimable only by its addressed participant (or by a narrowly defined eligible role if the delivery explicitly uses role routing);
- only the current lease holder may heartbeat or complete it;
- completion must store a non-empty summary and the artifact where required;
- retries must preserve attempt history and must not allow an expired worker to overwrite a newer attempt;
- a delivery is complete only when the child task is terminal and its result reference is validated.

The parent state is computed as follows:
- `running`: at least one delivery remains non-terminal and no aggregation phase is active;
- `completed`: all required deliveries completed and required aggregation completed;
- `partial`: at least one delivery succeeded but one or more required deliveries failed, expired, or remained unavailable at the policy deadline;
- `failed`: no usable delivery succeeded, or the request could not be dispatched;
- `cancelled`: cancellation completed for all cancellable children. Already completed work remains recorded.

The exact deadline and quorum policies should be configurable per request; do not silently invent a quorum.

## 7. Result aggregation

The runtime must distinguish three things:

1. **Participant result:** the original output from one participant, preserved without rewriting.
2. **Coordinator synthesis:** a separate task that receives selected participant results and produces a synthesis.
3. **Owner-facing summary:** a compact status/result view with links or keys to full artifacts.

Do not overwrite participant outputs with the coordinator's summary. Preserve disagreements and missing responses. A synthesis must identify which participants contributed and which did not.

## 8. Security and authority boundaries

- The owner-facing client may create team requests, inspect its own requests, retrieve authorized results, and request cancellation.
- Participants may claim only tasks addressed to their identity or permitted role, heartbeat only their own claims, and complete only their own claims.
- A coordinator may delegate only within the parent request's recorded scope.
- A participant must not gain authority merely because it appears in the roster or was included in a broadcast.
- Session/owner isolation applies to parent requests, child tasks, and result retrieval.
- Do not expose provider credentials, worker secrets, private memory, or deployment state in task payloads or artifacts unless explicitly authorized for that task.
- Audit the owner command, audience snapshot, dispatch decisions, delegation events, child completion, aggregation, and cancellation.
- Tool allowlists in Front Door must be default-deny. The owner client and worker clients must have different tool profiles.

## 9. Presence is not reachability

Presence tiers describe recent observation, not guaranteed execution capacity. A participant can be marked available but fail to wake or claim a task. Therefore each delivery must expose distinct states for `queued`, `wake_pending`, `claimed`, `running`, `completed`, `failed`, and `unreachable` (or equivalent derived states).

The wake mechanism must be verified end to end for every participant type. A task that was inserted but never reached a worker is not a successful dispatch.

## 10. MVP implementation sequence

### M0 — Data model and invariants
Add parent request and per-recipient delivery records, participant-targeted tasks, migrations for existing SQLite databases, and indexes. Preserve compatibility with existing role-queue tasks.

### M1 — Correct recipient-aware claiming
Extend claim selection so a participant can claim tasks explicitly addressed to its `agent_id`, while legacy role-queue tasks retain their current behavior. Add tests proving that participant A cannot claim participant B's delivery and that a broadcast creates N independently claimable deliveries for N recipients.

### M2 — Team command and status tools
Expose the owner-facing team-level MCP tools through Front Door with strict owner-only permissions. Resolve and snapshot the audience, create the parent and all child records transactionally, and return the recipient list plus parent ID.

### M3 — Wake and result collection
Ensure every child delivery triggers the existing wake path, track wake/claim deadlines, derive parent progress, and return participant results/artifact references.

### M4 — Coordinator phase
Add bounded coordinator delegation and synthesis as a separate child phase. Preserve original outputs and attribution.

### M5 — Operational controls
Add cancellation, retry policy, stale delivery handling, audit views, and failure-injection tests.

## 11. Acceptance tests

1. One ChatGPT MCP connection can inspect the roster and submit a team command without switching connectors or manually invoking role-specific tools.
2. A broadcast to N eligible participants creates exactly N distinct deliveries and N independently attributable child tasks.
3. No participant can claim another participant's targeted task.
4. A role fan-out reaches every eligible participant of that role; it does not collapse to a single role queue consumer.
5. A stale/offline participant is not silently counted as having received or completed work.
6. The owner can see all child statuses and retrieve each participant's original artifact from the same owner-facing interface.
7. A coordinator can delegate only within the recorded scope; attempts outside it are rejected.
8. A partial failure is reported as partial, not success.
9. Parent and child records are isolated by owner/session; another owner cannot inspect or retrieve them.
10. Existing single-role task workers continue to function during migration.

## 12. Explicit non-goals

This layer does not merge participant identities, share private model memory automatically, guarantee that every participant is always online, or make every participant equally authoritative. It does not turn a broadcast into a consensus protocol. It does not infer a final answer when required participants have not responded. It does not replace ChatGPT's natural-language reasoning; it provides a reliable execution and coordination substrate.

## 13. First engineering decision

Implement M0 and M1 before building a sophisticated conversational UI. The most dangerous failure would be a polished `team_command` that claims to broadcast but actually places one shared role task in a queue, allowing only one worker to claim it. Correct recipient identity, durable parent-child tracking, and end-to-end wake delivery are prerequisites for the unified window experience.
