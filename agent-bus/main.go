package main

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	_ "modernc.org/sqlite"
)

type Task struct {
	ID           string `json:"id"`
	SessionID    string `json:"session_id,omitempty"`
	Title        string `json:"title"`
	TargetRole   string `json:"target_role"`
	Payload      string `json:"payload"`
	CreatedBy    string `json:"created_by"`
	ClaimedBy    string `json:"claimed_by,omitempty"`
	Status       string `json:"status"` // pending | in_progress | completed | failed
	Summary      string `json:"summary,omitempty"`
	ArtifactKey  string `json:"artifact_key,omitempty"`
	LeaseExpires string `json:"lease_expires,omitempty"`
	UpdatedAt    string `json:"updated_at"`
}

type BusStore struct {
	db          *sql.DB
	mu          sync.Mutex
	leaseTTL    time.Duration
	presenceTTL time.Duration
}

func NewBusStore(dbPath string, leaseTTL time.Duration) (*BusStore, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)

	schema := `
	CREATE TABLE IF NOT EXISTS tasks (
		seq INTEGER PRIMARY KEY AUTOINCREMENT,
		id TEXT NOT NULL UNIQUE,
		session_id TEXT NOT NULL DEFAULT '',
		title TEXT NOT NULL,
		target_role TEXT NOT NULL,
		payload TEXT NOT NULL,
		created_by TEXT NOT NULL,
		claimed_by TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL,
		summary TEXT NOT NULL DEFAULT '',
		artifact_key TEXT NOT NULL DEFAULT '',
		lease_expires TEXT NOT NULL DEFAULT '',
		updated_at TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS artifacts (
		artifact_key TEXT PRIMARY KEY,
		content TEXT NOT NULL,
		updated_at TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS agents (
		agent_id TEXT PRIMARY KEY,
		role TEXT NOT NULL DEFAULT '',
		note TEXT NOT NULL DEFAULT '',
		last_seen TEXT NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_tasks_claim ON tasks(status, target_role, seq);
	`
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, err
	}

	// Migration (wave-1, gap 4): agents.empty_strikes counts server-side
	// rejected empty completions per agent. Older DBs lack the column.
	hasStrikes := false
	if prows, perr := db.Query(`PRAGMA table_info(agents)`); perr == nil {
		for prows.Next() {
			var cid int
			var cname, ctype string
			var notnull, pk int
			var dflt sql.NullString
			if serr := prows.Scan(&cid, &cname, &ctype, &notnull, &dflt, &pk); serr == nil && cname == "empty_strikes" {
				hasStrikes = true
			}
		}
		_ = prows.Close()
	}
	if !hasStrikes {
		if _, err := db.Exec(`ALTER TABLE agents ADD COLUMN empty_strikes INTEGER NOT NULL DEFAULT 0`); err != nil {
			_ = db.Close()
			return nil, err
		}
	}

	return &BusStore{
		db:          db,
		leaseTTL:    leaseTTL,
		presenceTTL: 2 * time.Minute,
	}, nil
}

func (s *BusStore) Close() error {
	return s.db.Close()
}

func formatUTC(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func (s *BusStore) reclaimExpiredLeasesLocked(ctx context.Context, now time.Time) error {
	nowStr := formatUTC(now)
	_, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET status = 'pending',
		    claimed_by = '',
		    lease_expires = '',
		    updated_at = ?
		WHERE status = 'in_progress'
		  AND lease_expires != ''
		  AND lease_expires < ?
	`, nowStr, nowStr)
	return err
}

// --- Presence Registry ---

// AgentPresence is one row of the presence registry as exposed in the
// state snapshot: who exists, what role they last claimed, and whether
// they have been seen within the presence TTL.
type AgentPresence struct {
	AgentID string `json:"agent_id"`
	Role    string `json:"role,omitempty"`
	Note    string `json:"note,omitempty"`
	LastSeen string `json:"last_seen"`
	Alive   bool   `json:"alive"`
	// Tier is the three-level presence derived per revamp plan S1:
	// "available" (seen within the presence TTL AND holding no
	// in_progress task with a live lease), "recently_active" (seen
	// within 30 minutes), otherwise "registered".
	Tier string `json:"tier"`
	// EmptyStrikes counts server-side rejected empty completions
	// (wave-1, gap 4). Surfaced only; enforcement is the watcher's job.
	EmptyStrikes int `json:"empty_strikes"`
}

// touchPresenceLocked upserts the caller's presence row. Callers must hold
// s.mu. An empty role or note preserves the previously recorded value, so
// task tools can refresh last_seen without knowing the registered role.
func (s *BusStore) touchPresenceLocked(ctx context.Context, agentID, role, note string, now time.Time) error {
	if strings.TrimSpace(agentID) == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO agents (agent_id, role, note, last_seen) VALUES (?, ?, ?, ?)
		ON CONFLICT(agent_id) DO UPDATE SET
			role = CASE WHEN excluded.role != '' THEN excluded.role ELSE agents.role END,
			note = CASE WHEN excluded.note != '' THEN excluded.note ELSE agents.note END,
			last_seen = excluded.last_seen
	`, agentID, role, note, formatUTC(now))
	return err
}

// listPresenceLocked returns every registered agent with a computed alive
// flag (seen within the presence TTL), a derived three-tier presence
// (revamp plan S1) and the empty-completion strike count. An agent counts
// as busy — and therefore not "available" — while it holds an in_progress
// task whose lease has not expired. Callers must hold s.mu.
func (s *BusStore) listPresenceLocked(ctx context.Context, now time.Time) ([]AgentPresence, error) {
	busy := make(map[string]bool)
	brows, berr := s.db.QueryContext(ctx, `
		SELECT DISTINCT claimed_by FROM tasks
		WHERE status = 'in_progress' AND claimed_by != '' AND lease_expires != '' AND lease_expires > ?
	`, formatUTC(now))
	if berr != nil {
		return nil, berr
	}
	for brows.Next() {
		var id string
		if err := brows.Scan(&id); err != nil {
			_ = brows.Close()
			return nil, err
		}
		busy[id] = true
	}
	if err := brows.Err(); err != nil {
		_ = brows.Close()
		return nil, err
	}
	_ = brows.Close()

	rows, err := s.db.QueryContext(ctx, `SELECT agent_id, role, note, last_seen, empty_strikes FROM agents ORDER BY agent_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]AgentPresence, 0)
	for rows.Next() {
		var a AgentPresence
		if err := rows.Scan(&a.AgentID, &a.Role, &a.Note, &a.LastSeen, &a.EmptyStrikes); err != nil {
			return nil, err
		}
		a.Tier = "registered"
		if seen, perr := time.Parse(time.RFC3339Nano, a.LastSeen); perr == nil {
			age := now.Sub(seen)
			a.Alive = age <= s.presenceTTL
			switch {
			case age <= s.presenceTTL && !busy[a.AgentID]:
				a.Tier = "available"
			case age <= 30*time.Minute:
				a.Tier = "recently_active"
			}
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// --- Tool I/O Schemas ---

type PostTaskInput struct {
	SessionID   string `json:"session_id,omitempty" jsonschema:"可选：关联的业务/对话会话 ID"`
	Title       string `json:"title" jsonschema:"任务标题"`
	TargetRole  string `json:"target_role" jsonschema:"负责该任务的角色，例如 researcher、coder、reviewer"`
	Payload     string `json:"payload" jsonschema:"任务详细指令或上下文"`
	ArtifactKey string `json:"artifact_key,omitempty" jsonschema:"可选：任务完成后写入共享产物的键名"`
	CreatedBy   string `json:"created_by,omitempty" jsonschema:"派发者标识，默认 orchestrator"`
}

type PostTaskOutput struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	TargetRole string `json:"target_role"`
}

type ClaimTaskInput struct {
	Role    string `json:"role" jsonschema:"当前 Agent 承担的角色"`
	AgentID string `json:"agent_id" jsonschema:"当前 Agent 唯一实例 ID"`
}

type ClaimTaskOutput struct {
	Found   bool   `json:"found"`
	Task    *Task  `json:"task,omitempty"`
	Message string `json:"message,omitempty"`
}

type HeartbeatTaskInput struct {
	TaskID  string `json:"task_id" jsonschema:"正在执行的任务 ID"`
	AgentID string `json:"agent_id" jsonschema:"当前认领该任务的 Agent ID"`
}

type HeartbeatTaskOutput struct {
	TaskID       string `json:"task_id"`
	LeaseExpires string `json:"lease_expires"`
}

type CompleteTaskInput struct {
	TaskID      string `json:"task_id" jsonschema:"要更新的任务 ID"`
	AgentID     string `json:"agent_id" jsonschema:"执行该任务的 Agent ID"`
	Summary     string `json:"summary" jsonschema:"简短执行摘要（建议控制在几百字内，避免撑爆下游上下文）"`
	FullContent string `json:"full_content,omitempty" jsonschema:"可选：完整输出/报告/代码正文，存入共享产物供按需读取"`
	ArtifactKey string `json:"artifact_key,omitempty" jsonschema:"可选：存入共享产物的键名（若未填则沿用任务预设键名）"`
	Failed      bool   `json:"failed,omitempty" jsonschema:"若任务失败设为 true，否则视为 completed"`
}

type CompleteTaskOutput struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	ArtifactKey string `json:"artifact_key,omitempty"`
}

type GetArtifactInput struct {
	ArtifactKey string `json:"artifact_key" jsonschema:"要读取的共享产物键名"`
}

type GetArtifactOutput struct {
	Found       bool   `json:"found"`
	ArtifactKey string `json:"artifact_key"`
	Content     string `json:"content,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
}

type PingPresenceInput struct {
	AgentID string `json:"agent_id" jsonschema:"当前 Agent 唯一实例 ID"`
	Role    string `json:"role,omitempty" jsonschema:"可选：当前 Agent 承担的角色；为空则保留已登记的角色"`
	Note    string `json:"note,omitempty" jsonschema:"可选：状态备注，如 idle、busy、offline；为空则保留已有备注"`
}

type PingPresenceOutput struct {
	AgentID  string `json:"agent_id"`
	LastSeen string `json:"last_seen"`
}

// SlimTask is a task as exposed in the bus_state snapshot: NO payload
// (full task text is fetched via claim/get flows, not the board), and
// the summary truncated to at most 200 characters.
type SlimTask struct {
	ID           string `json:"id"`
	SessionID    string `json:"session_id,omitempty"`
	Title        string `json:"title"`
	TargetRole   string `json:"target_role"`
	CreatedBy    string `json:"created_by"`
	ClaimedBy    string `json:"claimed_by,omitempty"`
	Status       string `json:"status"`
	Summary      string `json:"summary,omitempty"`
	ArtifactKey  string `json:"artifact_key,omitempty"`
	LeaseExpires string `json:"lease_expires,omitempty"`
	UpdatedAt    string `json:"updated_at"`
}

// BusStateSnapshot is the slim board snapshot shared by the bus://state
// resource and the bus_state tool.
type BusStateSnapshot struct {
	Tasks        []SlimTask      `json:"tasks"`
	ArtifactKeys []string        `json:"artifact_keys"`
	Agents       []AgentPresence `json:"agents"`
}

type BusStateInput struct{}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// snapshotLocked builds the slim state snapshot. Callers must hold s.mu.
func (s *BusStore) snapshotLocked(ctx context.Context) (BusStateSnapshot, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, session_id, title, target_role, created_by, claimed_by, status, summary, artifact_key, lease_expires, updated_at
		FROM tasks
		ORDER BY seq ASC
	`)
	if err != nil {
		return BusStateSnapshot{}, err
	}
	tasks := make([]SlimTask, 0)
	for rows.Next() {
		var t SlimTask
		if err := rows.Scan(
			&t.ID, &t.SessionID, &t.Title, &t.TargetRole,
			&t.CreatedBy, &t.ClaimedBy, &t.Status, &t.Summary,
			&t.ArtifactKey, &t.LeaseExpires, &t.UpdatedAt,
		); err != nil {
			_ = rows.Close()
			return BusStateSnapshot{}, err
		}
		t.Summary = truncateRunes(t.Summary, 200)
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return BusStateSnapshot{}, err
	}
	_ = rows.Close()

	artRows, err := s.db.QueryContext(ctx, `SELECT artifact_key FROM artifacts ORDER BY artifact_key ASC`)
	if err != nil {
		return BusStateSnapshot{}, err
	}
	artifactKeys := make([]string, 0)
	for artRows.Next() {
		var k string
		if err := artRows.Scan(&k); err != nil {
			_ = artRows.Close()
			return BusStateSnapshot{}, err
		}
		artifactKeys = append(artifactKeys, k)
	}
	if err := artRows.Err(); err != nil {
		_ = artRows.Close()
		return BusStateSnapshot{}, err
	}
	_ = artRows.Close()

	agents, err := s.listPresenceLocked(ctx, time.Now().UTC())
	if err != nil {
		return BusStateSnapshot{}, err
	}
	if agents == nil {
		agents = []AgentPresence{}
	}

	return BusStateSnapshot{
		Tasks:        tasks,
		ArtifactKeys: artifactKeys,
		Agents:       agents,
	}, nil
}

func buildServer(store *BusStore) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "multi-agent-bus",
		Version: "0.3.0",
	}, nil)

	// 1. Dispatch task (monotonic ID via AUTOINCREMENT + temporary placeholder)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "post_task",
		Description: "向共享总线派发一个交给特定角色 Agent 处理的任务",
	}, func(ctx context.Context, req *mcp.CallToolRequest, in PostTaskInput) (*mcp.CallToolResult, PostTaskOutput, error) {
		title := strings.TrimSpace(in.Title)
		role := strings.TrimSpace(in.TargetRole)
		if title == "" {
			return nil, PostTaskOutput{}, errors.New("title is required")
		}
		if role == "" {
			return nil, PostTaskOutput{}, errors.New("target_role is required")
		}

		store.mu.Lock()
		defer store.mu.Unlock()

		creator := strings.TrimSpace(in.CreatedBy)
		if creator == "" {
			creator = "orchestrator"
		}
		nowStr := formatUTC(time.Now())
		tempID := fmt.Sprintf("tmp-%d", time.Now().UnixNano())

		tx, err := store.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, PostTaskOutput{}, err
		}
		defer func() { _ = tx.Rollback() }()

		res, err := tx.ExecContext(ctx, `
			INSERT INTO tasks (id, session_id, title, target_role, payload, created_by, status, artifact_key, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
		`, tempID, strings.TrimSpace(in.SessionID), title, role, in.Payload, creator, strings.TrimSpace(in.ArtifactKey), nowStr)
		if err != nil {
			return nil, PostTaskOutput{}, err
		}

		seq, err := res.LastInsertId()
		if err != nil {
			return nil, PostTaskOutput{}, err
		}
		id := fmt.Sprintf("task-%04d", seq)

		if _, err := tx.ExecContext(ctx, `UPDATE tasks SET id = ? WHERE seq = ?`, id, seq); err != nil {
			return nil, PostTaskOutput{}, err
		}

		if err := tx.Commit(); err != nil {
			return nil, PostTaskOutput{}, err
		}

		return nil, PostTaskOutput{
			ID:         id,
			Status:     "pending",
			TargetRole: role,
		}, nil
	})

	// 2. Atomically claim next pending task (also reclaims expired leases)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "claim_next_task",
		Description: "按角色原子认领最早一条 pending 任务（会自动回收超时未续约的卡死任务）",
	}, func(ctx context.Context, req *mcp.CallToolRequest, in ClaimTaskInput) (*mcp.CallToolResult, ClaimTaskOutput, error) {
		role := strings.TrimSpace(in.Role)
		agentID := strings.TrimSpace(in.AgentID)
		if role == "" || agentID == "" {
			return nil, ClaimTaskOutput{}, errors.New("role and agent_id are required")
		}

		store.mu.Lock()
		defer store.mu.Unlock()

		now := time.Now().UTC()
		if err := store.reclaimExpiredLeasesLocked(ctx, now); err != nil {
			return nil, ClaimTaskOutput{}, err
		}

		var t Task
		err := store.db.QueryRowContext(ctx, `
			SELECT id, session_id, title, target_role, payload, created_by, claimed_by, status, summary, artifact_key, lease_expires, updated_at
			FROM tasks
			WHERE status = 'pending' AND target_role = ?
			ORDER BY seq ASC
			LIMIT 1
		`, role).Scan(
			&t.ID, &t.SessionID, &t.Title, &t.TargetRole, &t.Payload,
			&t.CreatedBy, &t.ClaimedBy, &t.Status, &t.Summary,
			&t.ArtifactKey, &t.LeaseExpires, &t.UpdatedAt,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ClaimTaskOutput{
				Found:   false,
				Message: fmt.Sprintf("no pending task for role %q", role),
			}, nil
		}
		if err != nil {
			return nil, ClaimTaskOutput{}, err
		}

		leaseExp := formatUTC(now.Add(store.leaseTTL))
		nowStr := formatUTC(now)

		res, err := store.db.ExecContext(ctx, `
			UPDATE tasks
			SET status = 'in_progress',
			    claimed_by = ?,
			    lease_expires = ?,
			    updated_at = ?
			WHERE id = ? AND status = 'pending'
		`, agentID, leaseExp, nowStr, t.ID)
		if err != nil {
			return nil, ClaimTaskOutput{}, err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return nil, ClaimTaskOutput{}, err
		}
		if rows == 0 {
			return nil, ClaimTaskOutput{
				Found:   false,
				Message: "task was claimed concurrently, please retry",
			}, nil
		}

		t.Status = "in_progress"
		t.ClaimedBy = agentID
		t.LeaseExpires = leaseExp
		t.UpdatedAt = nowStr

		// Claiming is proof of life: refresh the claimer's presence row.
		_ = store.touchPresenceLocked(ctx, agentID, role, "", now)

		return nil, ClaimTaskOutput{
			Found: true,
			Task:  &t,
		}, nil
	})

	// 3. Extend lease for long-running tasks
	mcp.AddTool(server, &mcp.Tool{
		Name:        "heartbeat_task",
		Description: "为长时间运行的 in_progress 任务续期租约，防止超时被其他 Agent 回收",
	}, func(ctx context.Context, req *mcp.CallToolRequest, in HeartbeatTaskInput) (*mcp.CallToolResult, HeartbeatTaskOutput, error) {
		taskID := strings.TrimSpace(in.TaskID)
		agentID := strings.TrimSpace(in.AgentID)
		if taskID == "" || agentID == "" {
			return nil, HeartbeatTaskOutput{}, errors.New("task_id and agent_id are required")
		}

		store.mu.Lock()
		defer store.mu.Unlock()

		now := time.Now().UTC()
		leaseExp := formatUTC(now.Add(store.leaseTTL))
		nowStr := formatUTC(now)

		res, err := store.db.ExecContext(ctx, `
			UPDATE tasks
			SET lease_expires = ?, updated_at = ?
			WHERE id = ? AND status = 'in_progress' AND claimed_by = ?
		`, leaseExp, nowStr, taskID, agentID)
		if err != nil {
			return nil, HeartbeatTaskOutput{}, err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return nil, HeartbeatTaskOutput{}, err
		}
		if rows == 0 {
			return nil, HeartbeatTaskOutput{}, fmt.Errorf("active task %q not owned by %q", taskID, agentID)
		}

		// A heartbeat renews presence as well as the lease.
		_ = store.touchPresenceLocked(ctx, agentID, "", "", now)

		return nil, HeartbeatTaskOutput{
			TaskID:       taskID,
			LeaseExpires: leaseExp,
		}, nil
	})

	// 4. Complete task + store optional artifact
	mcp.AddTool(server, &mcp.Tool{
		Name:        "complete_task",
		Description: "提交任务短摘要；如有长正文/大产物，请传入 full_content 与 artifact_key 供下游按需读取。注意：空交会被服务端拒绝（摘要为空、或有 artifact_key 却无正文、或无产物且摘要不足 20 字），任务保持 in_progress 并给提交者记一次空交 strike；如实失败请用 failed=true",
	}, func(ctx context.Context, req *mcp.CallToolRequest, in CompleteTaskInput) (*mcp.CallToolResult, CompleteTaskOutput, error) {
		taskID := strings.TrimSpace(in.TaskID)
		agentID := strings.TrimSpace(in.AgentID)
		if taskID == "" || agentID == "" {
			return nil, CompleteTaskOutput{}, errors.New("task_id and agent_id are required")
		}

		store.mu.Lock()
		defer store.mu.Unlock()

		var existingArtifactKey, statusNow, claimedNow string
		err := store.db.QueryRowContext(ctx, `
			SELECT artifact_key, status, claimed_by FROM tasks WHERE id = ?
		`, taskID).Scan(&existingArtifactKey, &statusNow, &claimedNow)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, CompleteTaskOutput{}, fmt.Errorf("task %q not found", taskID)
		}
		if err != nil {
			return nil, CompleteTaskOutput{}, err
		}

		if statusNow == "in_progress" && claimedNow != "" && claimedNow != agentID {
			return nil, CompleteTaskOutput{}, fmt.Errorf("task %q is currently claimed by %q", taskID, claimedNow)
		}

		// Gap 4 (wave-1, plan A1): reject empty completions server-side.
		// A non-failed completion must carry a real summary, and when an
		// artifact key applies (given or preset on the task) it must carry
		// real content; with no artifact at all the summary must be at
		// least 20 characters. A rejection leaves the task untouched
		// (still in_progress under its lease) and records one
		// empty-completion strike against the agent. Honest failures
		// (failed=true) are exempt — a failure report is not a fake done.
		if !in.Failed {
			summaryTrim := strings.TrimSpace(in.Summary)
			effKey := strings.TrimSpace(in.ArtifactKey)
			if effKey == "" {
				effKey = strings.TrimSpace(existingArtifactKey)
			}
			rejectReason := ""
			switch {
			case summaryTrim == "":
				rejectReason = "summary is empty"
			case effKey != "" && strings.TrimSpace(in.FullContent) == "":
				rejectReason = fmt.Sprintf("artifact_key %q applies but full_content is empty", effKey)
			case effKey == "" && len([]rune(summaryTrim)) < 20:
				rejectReason = "no artifact and summary shorter than 20 characters"
			}
			if rejectReason != "" {
				nowT := time.Now().UTC()
				_, _ = store.db.ExecContext(ctx, `
					INSERT INTO agents (agent_id, role, note, last_seen, empty_strikes) VALUES (?, '', '', ?, 1)
					ON CONFLICT(agent_id) DO UPDATE SET
						empty_strikes = agents.empty_strikes + 1,
						last_seen = excluded.last_seen
				`, agentID, formatUTC(nowT))
				return nil, CompleteTaskOutput{}, fmt.Errorf("completion rejected (empty completion: %s) — task %s stays in_progress; resubmit with a real summary and/or artifact content", rejectReason, taskID)
			}
		}

		status := "completed"
		if in.Failed {
			status = "failed"
		}

		artKey := strings.TrimSpace(in.ArtifactKey)
		if artKey == "" {
			artKey = existingArtifactKey
		}

		nowStr := formatUTC(time.Now())

		tx, err := store.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, CompleteTaskOutput{}, err
		}
		defer func() { _ = tx.Rollback() }()

		_, err = tx.ExecContext(ctx, `
			UPDATE tasks
			SET status = ?,
			    claimed_by = ?,
			    summary = ?,
			    artifact_key = ?,
			    lease_expires = '',
			    updated_at = ?
			WHERE id = ?
		`, status, agentID, in.Summary, artKey, nowStr, taskID)
		if err != nil {
			return nil, CompleteTaskOutput{}, err
		}

		if artKey != "" && in.FullContent != "" {
			_, err = tx.ExecContext(ctx, `
				INSERT INTO artifacts (artifact_key, content, updated_at)
				VALUES (?, ?, ?)
				ON CONFLICT(artifact_key) DO UPDATE SET
					content = excluded.content,
					updated_at = excluded.updated_at
			`, artKey, in.FullContent, nowStr)
			if err != nil {
				return nil, CompleteTaskOutput{}, err
			}
		}

		if err := tx.Commit(); err != nil {
			return nil, CompleteTaskOutput{}, err
		}

		// Completing a task is also proof of life.
		_ = store.touchPresenceLocked(ctx, agentID, "", "", time.Now().UTC())

		return nil, CompleteTaskOutput{
			ID:          taskID,
			Status:      status,
			ArtifactKey: artKey,
		}, nil
	})

	// 5. Fetch full artifact on demand
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_artifact",
		Description: "按 artifact_key 读取上游 Agent 写入的完整共享产物内容",
	}, func(ctx context.Context, req *mcp.CallToolRequest, in GetArtifactInput) (*mcp.CallToolResult, GetArtifactOutput, error) {
		key := strings.TrimSpace(in.ArtifactKey)
		if key == "" {
			return nil, GetArtifactOutput{}, errors.New("artifact_key is required")
		}

		store.mu.Lock()
		defer store.mu.Unlock()

		var content, updatedAt string
		err := store.db.QueryRowContext(ctx, `
			SELECT content, updated_at FROM artifacts WHERE artifact_key = ?
		`, key).Scan(&content, &updatedAt)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, GetArtifactOutput{
				Found:       false,
				ArtifactKey: key,
			}, nil
		}
		if err != nil {
			return nil, GetArtifactOutput{}, err
		}

		return nil, GetArtifactOutput{
			Found:       true,
			ArtifactKey: key,
			Content:     content,
			UpdatedAt:   updatedAt,
		}, nil
	})

	// 6. Presence ping (register / refresh last_seen for an agent)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "ping_presence",
		Description: "登记或刷新当前 Agent 的在线状态（claim / heartbeat / complete 也会自动刷新；空闲时定期 ping 即可保持 alive）",
	}, func(ctx context.Context, req *mcp.CallToolRequest, in PingPresenceInput) (*mcp.CallToolResult, PingPresenceOutput, error) {
		agentID := strings.TrimSpace(in.AgentID)
		if agentID == "" {
			return nil, PingPresenceOutput{}, errors.New("agent_id is required")
		}

		store.mu.Lock()
		defer store.mu.Unlock()

		now := time.Now().UTC()
		if err := store.touchPresenceLocked(ctx, agentID, strings.TrimSpace(in.Role), strings.TrimSpace(in.Note), now); err != nil {
			return nil, PingPresenceOutput{}, err
		}

		return nil, PingPresenceOutput{
			AgentID:  agentID,
			LastSeen: formatUTC(now),
		}, nil
	})

	// 7. Slim snapshot as a tool (same snapshot as the resource below;
	// direct-bus clients get bus_state without needing the gate facade)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "bus_state",
		Description: "读取共享总线的任务概况快照（精简版：任务不含 payload、摘要截断，含产物键与在线状态）",
	}, func(ctx context.Context, req *mcp.CallToolRequest, in BusStateInput) (*mcp.CallToolResult, BusStateSnapshot, error) {
		store.mu.Lock()
		defer store.mu.Unlock()
		snap, err := store.snapshotLocked(ctx)
		if err != nil {
			return nil, BusStateSnapshot{}, err
		}
		return nil, snap, nil
	})

	// 8. Snapshot resource (slim tasks + artifact keys + presence)
	server.AddResource(&mcp.Resource{
		URI:         "bus://state",
		Name:        "agent-bus-state",
		Description: "当前任务队列与可用共享产物键列表（完整产物请调 get_artifact）",
		MIMEType:    "application/json",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		store.mu.Lock()
		defer store.mu.Unlock()

		snap, err := store.snapshotLocked(ctx)
		if err != nil {
			return nil, err
		}

		raw, err := json.Marshal(snap)
		if err != nil {
			return nil, err
		}

		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{
				{
					URI:      req.Params.URI,
					MIMEType: "application/json",
					Text:     string(raw),
				},
			},
		}, nil
	})

	return server
}

func bearerAuthMiddleware(token string, next http.Handler) http.Handler {
	token = strings.TrimSpace(token)
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		got := strings.TrimSpace(strings.TrimPrefix(auth, prefix))
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	httpAddr := flag.String("http", "", "若指定监听地址（如 :8080），则以 Streamable HTTP 启动；留空则使用 stdio")
	dbPath := flag.String("db", "./agent_bus.db", "SQLite 数据库文件路径")
	leaseSec := flag.Int("lease-sec", 900, "任务认领租约超时秒数（超时未续约或完成会自动回退为 pending）")
	authToken := flag.String("token", os.Getenv("MCP_BUS_TOKEN"), "可选：HTTP Bearer Token（也可通过 MCP_BUS_TOKEN 环境变量传入）")
	flag.Parse()

	if *leaseSec <= 0 {
		log.Fatalf("lease-sec must be positive, got %d", *leaseSec)
	}

	store, err := NewBusStore(*dbPath, time.Duration(*leaseSec)*time.Second)
	if err != nil {
		log.Fatalf("init store error: %v", err)
	}
	defer func() { _ = store.Close() }()

	if *httpAddr != "" {
		mcpHandler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
			return buildServer(store)
		}, nil)

		mux := http.NewServeMux()
		mux.Handle("/mcp", bearerAuthMiddleware(*authToken, mcpHandler))

		srv := &http.Server{
			Addr:              *httpAddr,
			Handler:           mux,
			ReadHeaderTimeout: 15 * time.Second,
		}

		log.Printf("MCP Multi-Agent Bus listening on http://localhost%s/mcp (db=%s)", *httpAddr, *dbPath)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server error: %v", err)
		}
		return
	}

	server := buildServer(store)
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		log.Fatalf("stdio server error: %v", err)
	}
}
