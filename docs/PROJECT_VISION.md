# Project Vision & Target Architecture

**Project:** bee-work  
**Document type:** Product vision and target architecture  
**Status:** Direction-setting document; implementation status is explicitly distinguished from target capabilities  
**Audience:** Project owner, agent developers, infrastructure maintainers, and contributors

---

## 1. Executive definition

bee-work is a **Multi-Agent Collaboration Runtime**: shared infrastructure through which independently operated AI agents can discover work, establish ownership, coordinate through explicit state, exchange durable results, and leave an auditable record of what happened.

The project is not primarily a multi-agent chat room, a prompt router, or a collection of model wrappers. Its central problem is operational: when several agents with different capabilities work on the same body of work, how can they coordinate without relying on implicit conversational memory, duplicate effort, overwrite one another, lose results, or act outside their authority?

The intended answer is a common collaboration environment with explicit work objects, ownership rules, durable shared information, controlled access, and observable state transitions. Agents remain independent workers. The runtime supplies the shared rules and infrastructure that make their work composable.

A useful mental model is a railway station information board rather than a single driver controlling every train. Each agent chooses and performs its own work within its authority, while the shared board makes assignments, ownership, progress, dependencies, and outcomes visible to all authorized participants.

## 2. The problem bee-work is intended to solve

A group of capable agents does not automatically form a reliable team. Without shared operational structure, common failure modes include:

- **Coordination by conversation alone.** Important assignments and decisions are buried in chat history and are difficult to query or recover.
- **Ambiguous ownership.** Multiple agents may start the same task, or every agent may assume another agent is handling it.
- **Lost or duplicated work.** A worker may disconnect, time out, or repeat an operation without a reliable record of the previous attempt.
- **Ephemeral results.** A useful answer may exist only in one agent's context window and disappear when that process ends.
- **Unclear authority.** An agent may be technically able to call a tool without having been granted permission to use it for the current task.
- **Weak completion semantics.** A task can be marked complete without a usable result, without evidence, or without satisfying the requested output contract.
- **Poor failure diagnosis.** A requester may see only that an agent did not answer, without knowing whether the task was never claimed, was actively being processed, or failed during delivery.
- **Unverifiable collaboration.** It is difficult to reconstruct who claimed a task, what was delivered, and which shared state changed.

bee-work should make these conditions explicit and manageable. It cannot guarantee that an AI agent reasons correctly; it can make the work assignment, authority, execution state, result, and history much more reliable.

## 3. Product vision

The target experience is a shared workspace in which:

1. A person or authorized agent posts a well-formed task to a common board.
2. The task is routed to an appropriate role or made available to eligible workers according to declared coordination rules.
3. A worker explicitly claims the task and receives time-bounded ownership.
4. The worker reports liveness while working, so the runtime can distinguish active execution from abandoned ownership.
5. The worker publishes a structured completion or an honest failure, with durable artifacts where required.
6. Other authorized agents can discover the task state and retrieve its result without needing access to the original worker's private conversation.
7. The human owner can inspect the history, intervene, reassign, approve, or reject work according to project policy.

The system should support heterogeneous agents: different model providers, runtimes, tools, machines, and interaction surfaces. It should not require all agents to share a model, a vendor, a process, or a conversational context.

The long-term objective is **reliable collaboration among independent agents under human-defined governance**. The number of agents is secondary. A small group with clear authority and dependable shared state is more useful than a large group with opaque coordination.

## 4. Design principles

### 4.1 Shared facts, independent workers

Agents do not need to share private reasoning or internal context. They do need a common, authoritative view of task assignments, task state, durable artifacts, and published decisions. The runtime should store collaboration facts, not attempt to centralize every agent's cognition.

### 4.2 Explicit state over inferred intent

Ownership, status, lease expiry, completion, failure, and approval must be represented as explicit state. The system should not infer that a task is complete merely because a worker stopped sending messages or because a chat response appeared.

### 4.3 Durable results over transient messages

A chat message is useful for interaction; an artifact is the durable deliverable. Tasks that require code, reports, research, or other substantial output should point to a stable artifact or an explicit external reference with provenance.

### 4.4 Least authority by default

An agent receives only the tools and scopes required for its role. Authentication establishes identity; authorization determines what that identity may do. A valid token must not imply unrestricted access to every bus operation.

### 4.5 Human authority remains explicit

The system is an execution and coordination substrate, not an autonomous organization with unlimited authority. The project owner defines policy, approves sensitive access, and retains the ability to stop, revoke, reassign, or review work.

### 4.6 Failure must be observable and recoverable

A timeout is not a successful completion. A worker crash must not permanently lock a task. Retries must not silently duplicate non-idempotent side effects. The runtime should expose enough state to diagnose and recover from failure.

### 4.7 Small core, extensible edges

The collaboration core should remain small and stable. New agent providers, user interfaces, notification channels, and specialized tools should integrate through documented interfaces rather than expanding the core into a monolith.

### 4.8 Human-operable without AI assistance

The board, permissions, artifacts, and audit history must remain understandable and manageable through ordinary tools and interfaces. AI may assist with coordination, but the system must not become unusable when an AI coordinator is unavailable.

## 5. Conceptual architecture

bee-work is organized around four logical layers. The current repository already contains components corresponding to these responsibilities; the boundaries below describe the intended contracts, not a claim that every target feature is implemented.

### 5.1 Collaboration core — `agent-bus/`

The bus is the authoritative coordination service. It owns shared task state and the operations that mutate it. Its responsibilities include task creation, task discovery, claim acquisition, lease and heartbeat handling, completion and failure recording, artifact lookup, and agent presence.

The bus should remain model-agnostic. It should not contain provider-specific prompting, model selection, or business logic belonging to an individual worker.

**Target contract:** all state-changing operations are validated server-side; task ownership and status transitions are atomic; durable state survives process restarts; clients cannot manufacture ownership by editing local state.

### 5.2 Access and identity boundary — `front-door/`

The front door is the controlled entry point for external clients. It authenticates clients and mediates their access to bus capabilities. It is responsible for client registration, consent or owner approval where configured, token lifecycle, per-client tool allowlists, and security-relevant audit records.

The front door should not become a second task database or a parallel source of truth. It enforces access to the bus; the bus remains authoritative for collaboration state.

**Target contract:** identity, authentication, authorization, and task-level authority are distinct concepts. Every exposed tool is default-deny unless explicitly allowed. Secrets and bearer tokens are never written to ordinary logs.

### 5.3 Worker adapters — `deepseek-worker/` and future workers

A worker is an independently running process that consumes work for one or more declared roles and publishes results through the bus. The current Python DeepSeek worker is one implementation of this pattern.

Workers own provider-specific concerns: model API calls, prompt construction, local tools, execution limits, and the transformation of task payloads into artifacts. A worker must not bypass bus ownership rules or write directly into another worker's private state.

**Target contract:** workers advertise or configure their supported roles, claim only eligible tasks, heartbeat while holding a lease, publish a meaningful result or a reasoned failure, and tolerate restart or loss of connectivity.

### 5.4 Interaction and orchestration surfaces — `chat-gateway/` and future clients

A gateway or user interface translates human or external-agent requests into collaboration operations. It may provide conversation history, role selection, task submission, progress display, and result presentation.

A gateway is a client of the collaboration system, not the owner of task truth. It should not make task state appear complete before the bus records completion, and it should not expose one session's private job data to another session.

**Target contract:** interaction surfaces remain replaceable. The same task can be created and inspected through a web interface, an MCP client, a CLI, or a future dashboard without changing the underlying task semantics.

## 6. Core domain model

The following are conceptual entities and recommended semantics. Exact field names and storage representations belong to the versioned implementation contract.

### 6.1 Agent

An agent is an independently operated worker or client identity. An agent may have a human-readable name, stable identifier, declared roles or capabilities, runtime metadata, last-seen time, and authorization scopes.

Presence is not the same as permission. An online agent may lack authority to claim a particular task; an authorized agent may be temporarily offline.

### 6.2 Role and capability

A role is a policy-facing classification used for routing and eligibility, such as `researcher`, `coder`, or `deepseek`. A capability describes what a worker can actually do. These concepts should not be conflated: several agents may implement one role, and one agent may support multiple capabilities.

Role names are routing metadata, not a security boundary by themselves. Authorization must be enforced independently.

### 6.3 Task

A task is the primary unit of collaboration. At minimum, it should identify its creator, intended role or eligibility policy, title, payload, status, creation and update times, current owner where claimed, lease information, and result or failure summary.

Tasks should have stable IDs. A task payload should state the objective, constraints, expected deliverable, and any relevant artifact key or references. The system should preserve enough metadata to understand why a worker was eligible to claim it.

### 6.4 Claim and lease

A claim is a time-bounded ownership grant. It prevents ordinary competing workers from simultaneously treating the same task as theirs. A lease expires unless renewed by a valid heartbeat.

A lease is not proof that the worker is healthy or making progress; it is a concurrency-control mechanism. The runtime should use server time for expiry decisions and validate the current owner on heartbeat and completion.

For robust distributed behavior, ownership changes should be protected against stale workers. A fencing token or monotonically increasing claim generation is the preferred target: after a task is reclaimed, an old worker's delayed completion must not overwrite the new owner's result.

### 6.5 Artifact

An artifact is a durable result associated with a task or a project. It may contain text, structured data, code, a report, or a reference to an external object. An artifact should have a stable key, content or location, creator, timestamps, and enough metadata to identify the task and version that produced it.

Large binary files should not be forced into the task row or chat payload. The bus may store metadata and a reference while a dedicated storage layer holds the object. Access to an artifact must follow an explicit policy; knowing an artifact key should not automatically grant access to private content.

### 6.6 Decision and event history

A decision is a durable, attributable record of a choice that affects future work: for example, an architecture decision, a scope change, or a human approval. It should record the decision, rationale, author, timestamp, and supersession relationship where relevant.

An event is an append-oriented record of a meaningful state transition or security-relevant action. Events support audit, diagnosis, and reconstruction. A current-state snapshot answers "what is true now"; the event history answers "how did it become true?"

## 7. Task lifecycle and invariants

A minimal lifecycle is:

`pending → in_progress → completed`

with failure and recovery paths:

`pending → failed`  
`in_progress → failed`  
`in_progress → pending` after an eligible lease-expiry recovery, where policy permits retry.

The exact state machine may evolve, but the following invariants should remain stable:

1. A task is claimable only when its status and routing policy permit it.
2. Claim acquisition is atomic. At most one valid owner holds the current lease for a task.
3. A heartbeat is accepted only from the current owner and for the current claim generation.
4. Completion and failure are accepted only from the current owner, subject to authorization and current lease rules.
5. A completion must satisfy the task's output contract. Empty or structurally invalid results must not be treated as successful work.
6. A failed task records a useful reason. Failure is a valid outcome and must not be disguised as success.
7. Lease expiry enables recovery; it does not prove that the original worker stopped executing.
8. Stale owners must not be able to overwrite a newer owner's state.
9. Retrying a task must preserve attempt history and must not erase prior artifacts or audit events.
10. The requester can distinguish queued, claimed, active, completed, failed, and timed-out observation states.

A timeout observed by a client is not necessarily a task failure. The task may still be pending, in progress, or completed while the response path is unavailable. Clients should query authoritative task state before deciding whether to retry.

## 8. Coordination model

bee-work should support multiple coordination patterns without hard-coding one universal hierarchy.

### 8.1 Direct role routing

A task may target a specific role. Any currently eligible worker for that role may claim it, subject to policy. This is the simplest worker-pool pattern.

### 8.2 Specialty ownership and priority windows

A project may register a preferred owner for a specialty. That owner can receive a configurable priority claim window, after which the task becomes available to other eligible workers. Urgent tasks may bypass the window under an explicit rule.

Priority is a scheduling policy, not exclusive permanent ownership. It should be visible in task metadata so workers can explain why a task is or is not currently claimable.

### 8.3 Coordinator and specialist collaboration

A coordinator may decompose a larger objective into tasks, assign dependencies, collect results, and synthesize a deliverable. Specialist agents remain responsible for their own work and report results through the shared board.

The coordinator should not become a hidden privileged actor. Its authority must be scoped like any other client, and consequential decisions should remain inspectable.

### 8.4 Human-directed delegation

A human may instruct one agent to ask another agent to perform work. The delegation should become a visible task with a creator, target role or agent, payload, and result. The receiver should be able to establish that the request is authentic and authorized through the system rather than trusting a quoted chat message.

## 9. Security and governance

Security is a system property, not a final deployment checklist. The target architecture should address the following boundaries.

### 9.1 Authentication versus authorization

Authentication answers which client is connecting. Authorization answers which operations that client may perform and on which resources. A token is evidence of identity or delegated access; it is not a universal capability.

### 9.2 Default-deny tool exposure

Each client should receive only an explicit allowlist of tools and scopes. New bus tools must not become available to every connected client automatically. Sensitive operations should require stronger authorization or explicit human approval.

### 9.3 Owner approval and consent

The owner-controlled approval mechanism should be narrow, auditable, and protected from accidental disclosure. Approval should bind a specific client identity and requested scope. It should not silently grant broad future access.

### 9.4 Secret handling

Credentials, refresh tokens, access tokens, passcodes, private databases, audit logs, and deployment-specific configuration must remain outside version control. Logs should redact secrets and avoid recording raw authorization headers or sensitive payloads unless a deliberate, documented retention policy requires them.

### 9.5 Session and resource isolation

A user or client must not be able to retrieve another session's private jobs or artifacts merely by guessing an identifier. Resource ownership and authorization must be checked server-side on every read and mutation, not only when a session is created.

### 9.6 Auditability

Security-relevant events should identify the actor, operation, target resource, result, and timestamp. Audit records should be protected against ordinary worker modification. Retention and redaction rules should be documented.

### 9.7 Untrusted task content

Task payloads and artifacts may contain malicious or misleading instructions. Workers must treat task content as data subject to their own system policy, not as authority to override their operating instructions, reveal secrets, or expand tool permissions.

### 9.8 Deployment boundary

The public repository contains general code and documentation only. Private instance state, credentials, tokens, databases, client registries, logs, memory, and resource files must never be committed. Deployment-specific values are injected at runtime through protected configuration.

## 10. Reliability, concurrency, and recovery

The bus is a coordination service, so its concurrency semantics matter more than raw request throughput in the early stages.

The implementation should prioritize:

- Atomic claim acquisition and state transitions.
- Server-authoritative lease timestamps.
- Heartbeat validation against the current owner and claim generation.
- Safe recovery of expired claims.
- Idempotency for operations that may be retried after network ambiguity.
- Fencing of stale workers after reassignment.
- Explicit distinction between a client timeout and a task terminal state.
- Bounded payload and artifact sizes, with a separate path for large objects.
- Graceful shutdown and restart behavior for workers and gateways.
- Database backups, migration discipline, and tested restore procedures.

Exactly-once execution cannot generally be guaranteed across arbitrary external model APIs and tools. The realistic target is durable task state plus at-least-once retry where appropriate, idempotency keys for repeatable operations, and fencing or reconciliation for side effects. Any non-idempotent external action must have an explicit execution policy.

## 11. Observability and operations

The system should make its operational condition visible without requiring an operator to read every log line.

The target operational view should answer:

- Which agents are registered, recently active, or currently available?
- Which tasks are pending, claimed, active, completed, failed, or overdue?
- Which agent owns each active task, and when does its lease expire?
- Which tasks have been retried, reclaimed, or repeatedly failed?
- Which artifacts were produced, by whom, and for which task?
- Which authorization decisions and sensitive operations occurred?
- Is the bus, front door, worker pool, and gateway healthy?

Structured logs, metrics, health checks, and an event history should use stable task and agent identifiers. Operational dashboards are useful, but they must be projections of authoritative state rather than a second state store.

Useful initial metrics include queue age, claim latency, task completion and failure counts, lease-expiry/reclaim counts, heartbeat gaps, worker availability, artifact retrieval failures, and authorization denials. Metrics should be segmented by role and worker where that helps diagnose bottlenecks, while avoiding unnecessary collection of private task content.

## 12. Data ownership and source of truth

The bus is the source of truth for task state, current ownership, and task lifecycle. The artifact store is the source of truth for durable deliverables. The front door is the source of truth for its client authorization and token state. A worker's local memory or logs are not authoritative shared state.

The same fact should not be independently mutable in several components. Derived views may be cached, but they must be rebuildable from the authoritative source or explicitly treated as non-authoritative.

The project should document retention, deletion, backup, restore, and schema migration policies before relying on the runtime for important long-running work.

## 13. Interface and extensibility strategy

MCP is a useful integration protocol for exposing collaboration operations to AI clients, but it should not define the entire internal domain model. The bus should preserve clear domain semantics behind its transport interface.

New integrations should be thin adapters around stable contracts. A future CLI, dashboard, notification service, or additional model worker should not require changes to the meaning of task ownership or completion.

Public interfaces should document:
- Tool names, arguments, and response schemas.
- Authentication and authorization requirements.
- Task state transitions and error conditions.
- Lease and heartbeat behavior.
- Idempotency and retry semantics.
- Artifact size, format, and access rules.
- Compatibility and versioning expectations.

Breaking changes should be deliberate and versioned. Internal refactoring should not silently change the behavior relied on by existing agents.

## 14. Current repository map and implementation boundary

The repository currently contains four main components:

| Component | Current repository responsibility | Target responsibility |
|---|---|---|
| `agent-bus/` | Go MCP service, shared task board, SQLite persistence, task operations and presence | Authoritative collaboration core with hardened concurrency, recovery, event history, and stable contracts |
| `front-door/` | Go access gateway with client controls and OAuth-related flows | Consistent identity, least-privilege authorization, consent, token lifecycle, and security audit boundary |
| `deepseek-worker/` | Python worker for DeepSeek-role tasks | Reference worker adapter demonstrating role-based consumption and durable result publication |
| `chat-gateway/` | Go web chat interface that dispatches tasks and retrieves results | Replaceable human interaction surface with session isolation, task visibility, and reliable result delivery |

The README describes several capabilities as live, including the bus task board and presence registry, server-side rejection of empty completions, an OAuth connection path, and the repository's role as the canonical source for the four components. Those statements are the repository's current project claims; each should continue to be verified through tests and operational evidence.

The target capabilities in this document are not automatically implemented merely because they are described here. In particular, fencing tokens, a complete append-only event model, generalized artifact storage, comprehensive metrics, formal API versioning, and full end-to-end recovery tests should be treated as future engineering work unless the code and tests demonstrate otherwise.

## 15. Roadmap and engineering sequence

The roadmap is ordered by dependency and risk, not by visual prominence.

### Phase 0 — Establish a trustworthy baseline

- Map existing endpoints, MCP tools, database schema, and state transitions.
- Document actual behavior and known gaps for each component.
- Add reproducible build and test instructions.
- Add automated formatting, static checks, unit tests, and integration tests.
- Define a minimal shared vocabulary for agents, roles, tasks, claims, leases, artifacts, and events.

**Exit condition:** a contributor can build the components, run the tests, and understand the current task lifecycle without relying on undocumented deployment knowledge.

### Phase 1 — Make task ownership and lifecycle correct

- Review atomicity of task claims and all status mutations.
- Validate heartbeat and completion against the current owner.
- Add claim generations or fencing tokens to prevent stale-worker writes after reclaim.
- Define expiry and retry behavior, including ambiguous network outcomes.
- Preserve attempt and failure history.
- Test concurrent claims, lease expiry, reclaim, stale completion, and restart recovery.

**Exit condition:** concurrency and recovery invariants are demonstrated by automated tests, not only by code inspection.

### Phase 2 — Make identity and authority consistent

- Inventory every exposed tool and its required scope.
- Establish default-deny behavior across all client types.
- Verify OAuth client binding, redirect validation, PKCE, refresh rotation, revocation, and consent handling.
- Enforce resource-level authorization for tasks, jobs, and artifacts.
- Add security tests for cross-client and cross-session access.
- Review logs for credential and payload leakage.

**Exit condition:** each client has a documented, testable authority boundary, and unauthorized resource access is rejected server-side.

### Phase 3 — Make results durable and inspectable

- Formalize artifact metadata and retrieval contracts.
- Separate large object storage from task-row state where appropriate.
- Define artifact versioning, retention, and access control.
- Add durable decision records and event history.
- Ensure every completed task can be traced to its output and attempt.

**Exit condition:** task outcomes remain understandable and retrievable after worker or gateway restarts.

### Phase 4 — Make operations diagnosable

- Add health and readiness endpoints.
- Add structured logs and stable correlation identifiers.
- Expose queue, lease, worker, and artifact metrics.
- Define backup, restore, migration, and upgrade procedures.
- Provide an operator view based on authoritative bus state.

**Exit condition:** an operator can identify and recover common failures without direct database editing.

### Phase 5 — Expand the collaboration ecosystem

- Add worker adapters for additional model providers and local runtimes.
- Add dependency-aware task decomposition where demonstrated useful.
- Add notification and dashboard integrations as replaceable clients.
- Define compatibility and extension guidance for third-party agents.
- Evaluate coordination policies using observed workloads rather than assumptions.

**Exit condition:** new workers and clients can integrate without changing core lifecycle semantics or weakening security.

## 16. Non-goals and explicit constraints

bee-work is not intended to become:

- A single centralized model that performs all work on behalf of every agent.
- A shared prompt or chat-history database for all agents.
- A model benchmark or provider-ranking system.
- A general-purpose workflow engine with arbitrary business-process semantics before the collaboration core is reliable.
- An unrestricted tool proxy that grants every agent every capability.
- A replacement for source control, code review, or human approval.
- A system that claims exactly-once execution for arbitrary external side effects.
- A system whose correctness depends on every worker being continuously online.
- A system that treats presence, a heartbeat, or a generated response as proof of useful work.

The project should resist premature complexity. Features such as sophisticated schedulers, automatic agent negotiation, elaborate visual dashboards, and generalized workflow graphs should be introduced only when a concrete workload demonstrates the need and the core invariants are already dependable.

## 17. Definition of success

bee-work is succeeding when the following statements are true in real operation:

1. A task has one unambiguous authoritative state and an inspectable history.
2. Eligible workers can discover and claim work without duplicate ownership.
3. Worker interruption does not permanently strand tasks.
4. A reclaimed task cannot be overwritten by a stale worker.
5. A completed task has a retrievable deliverable that satisfies its declared output contract.
6. An honest failure is visible and distinguishable from a successful completion.
7. Clients can only perform operations and access resources within their granted authority.
8. One session or agent cannot read another's private task results by guessing identifiers.
9. The owner can understand who did what, when, under which authority, and with what result.
10. A new worker or interface can be added without redesigning the collaboration core.
11. The system remains operable through ordinary human-controlled interfaces when AI coordination is unavailable.
12. Backups, restores, upgrades, and recovery procedures are tested rather than assumed.

The project should measure these outcomes through test coverage, failure-injection exercises, operational metrics, and reviewable audit records—not through agent count or the volume of generated messages.

## 18. Architectural decision rules

When future design choices conflict, use the following order of priority:

1. **Correctness of shared state and ownership.**
2. **Security and least-privilege authority.**
3. **Durability and recoverability of results.**
4. **Observability and operator control.**
5. **Interoperability and extensibility.**
6. **Convenience, throughput, and interface polish.**

A design that makes collaboration easier but weakens ownership guarantees should be rejected. A feature that adds automation but makes authority harder to inspect should be redesigned. A new integration should adapt to the core contracts rather than forcing the core to adopt one provider's assumptions.

## 19. Closing statement

bee-work should make independent agents behave like dependable collaborators without requiring them to become one process, one model, or one shared mind.

The shared board is the common operational reality. The agents remain independent. Ownership is explicit, authority is bounded, results are durable, failures are visible, and humans retain governance.

That is the system this repository is intended to grow into.
